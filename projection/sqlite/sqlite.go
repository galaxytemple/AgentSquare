package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// SchemaSQL defines the SQLite read-model projection tables.
// Note (Spec C1 & C2): SQLite/Postgres are NEVER used as the source-of-truth ledger;
// they are rebuildable read-model projections derived by replaying ledger.Log.
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS agentsquare_checkpoint (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	last_seq INTEGER NOT NULL,
	last_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agentsquare_agents (
	agent_id TEXT PRIMARY KEY,
	pubkey TEXT NOT NULL,
	display_name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS agentsquare_balances (
	agent_id TEXT NOT NULL,
	asset TEXT NOT NULL,
	available INTEGER NOT NULL DEFAULT 0,
	locked INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (agent_id, asset)
);

CREATE TABLE IF NOT EXISTS agentsquare_fills (
	seq INTEGER PRIMARY KEY,
	round INTEGER NOT NULL,
	asset TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	side TEXT NOT NULL,
	filled_qty INTEGER NOT NULL,
	clearing_price INTEGER NOT NULL,
	cash_delta INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agentsquare_clears (
	round INTEGER NOT NULL,
	asset TEXT NOT NULL,
	clearing_price INTEGER NOT NULL,
	volume INTEGER NOT NULL,
	seed_sig TEXT NOT NULL,
	PRIMARY KEY (round, asset)
);
`

// Projection implements ledger.Projection for SQLite or any database/sql driver (Spec C3 & C4).
// If db is nil, it maintains an idempotent in-memory SQL-equivalent read model so it can run without external drivers.
type Projection struct {
	mu       sync.RWMutex
	db       *sql.DB
	lastSeq  uint64
	lastHash string
	fills    []protocol.Fill
	clears   []ledger.ClearBody
}

// New creates a new SQLite/SQL Projection. Pass a *sql.DB (e.g. modernc.org/sqlite) or nil for embedded memory projection.
func New(ctx context.Context, db *sql.DB) (*Projection, error) {
	p := &Projection{
		db:       db,
		lastHash: ledger.ZeroHash,
	}
	if db != nil {
		if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
			return nil, fmt.Errorf("sqlite schema init: %w", err)
		}
		var seq uint64
		var hash string
		err := db.QueryRowContext(ctx, `SELECT last_seq, last_hash FROM agentsquare_checkpoint WHERE id = 1`).Scan(&seq, &hash)
		if err == nil {
			p.lastSeq = seq
			p.lastHash = hash
		}
	}
	return p, nil
}

// Apply idempotently applies a ledger record to the projection (Spec C4).
func (p *Projection) Apply(ctx context.Context, rec ledger.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Idempotency per seq (Spec C4)
	if rec.Seq <= p.lastSeq {
		return nil
	}

	switch rec.Type {
	case ledger.RecClear:
		var b ledger.ClearBody
		if err := json.Unmarshal(rec.Body, &b); err == nil {
			p.clears = append(p.clears, b)
			if p.db != nil {
				if _, err := p.db.ExecContext(ctx,
					`INSERT OR REPLACE INTO agentsquare_clears (round, asset, clearing_price, volume, seed_sig) VALUES (?, ?, ?, ?, ?)`,
					b.Round, b.Asset, b.ClearingPrice, b.Volume, b.SeedSig); err != nil {
					return err
				}
			}
		}
	case ledger.RecFill:
		var f protocol.Fill
		if err := json.Unmarshal(rec.Body, &f); err == nil {
			p.fills = append(p.fills, f)
			if p.db != nil {
				if _, err := p.db.ExecContext(ctx,
					`INSERT OR REPLACE INTO agentsquare_fills (seq, round, asset, agent_id, side, filled_qty, clearing_price, cash_delta) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					rec.Seq, f.Round, f.Asset, f.AgentID, string(f.Side), f.FilledQty, f.ClearingPrice, f.CashDelta); err != nil {
					return err
				}
			}
		}
	case ledger.RecAgentAdmitted:
		var b ledger.AgentAdmittedBody
		if err := json.Unmarshal(rec.Body, &b); err == nil && p.db != nil {
			if _, err := p.db.ExecContext(ctx,
				`INSERT OR REPLACE INTO agentsquare_agents (agent_id, pubkey, display_name) VALUES (?, ?, ?)`,
				b.AgentID, b.PubKey, b.DisplayName); err != nil {
				return err
			}
		}
	case ledger.RecBalanceCredit:
		var b ledger.BalanceChangeBody
		if err := json.Unmarshal(rec.Body, &b); err == nil && p.db != nil {
			if _, err := p.db.ExecContext(ctx,
				`INSERT INTO agentsquare_balances (agent_id, asset, available, locked) VALUES (?, ?, ?, 0)
				 ON CONFLICT(agent_id, asset) DO UPDATE SET available = available + excluded.available`,
				b.AgentID, b.Asset, b.Qty); err != nil {
				return err
			}
		}
	case ledger.RecBalanceDebit:
		var b ledger.BalanceChangeBody
		if err := json.Unmarshal(rec.Body, &b); err == nil && p.db != nil {
			if _, err := p.db.ExecContext(ctx,
				`INSERT INTO agentsquare_balances (agent_id, asset, available, locked) VALUES (?, ?, 0, 0)
				 ON CONFLICT(agent_id, asset) DO UPDATE SET available = available - ?`,
				b.AgentID, b.Asset, b.Qty); err != nil {
				return err
			}
		}
	}

	if p.db != nil {
		if _, err := p.db.ExecContext(ctx,
			`INSERT OR REPLACE INTO agentsquare_checkpoint (id, last_seq, last_hash) VALUES (1, ?, ?)`,
			rec.Seq, rec.Hash); err != nil {
			return err
		}
	}
	p.lastSeq = rec.Seq
	p.lastHash = rec.Hash
	return nil
}

func (p *Projection) Checkpoint(_ context.Context) (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastSeq, nil
}

// Reset clears the projection state so it can be rebuilt from seq 0 (Spec B4: agentsquare projection rebuild --from-seq 0).
func (p *Projection) Reset(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSeq = 0
	p.lastHash = ledger.ZeroHash
	p.fills = nil
	p.clears = nil
	if p.db != nil {
		for _, tbl := range []string{"agentsquare_checkpoint", "agentsquare_agents", "agentsquare_balances", "agentsquare_fills", "agentsquare_clears"} {
			if _, err := p.db.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
				return err
			}
		}
	}
	return nil
}

// Rebuild replays records from lg starting at fromSeq into proj (Spec B4).
func Rebuild(ctx context.Context, lg ledger.Log, proj ledger.Projection, fromSeq uint64) (uint64, error) {
	if fromSeq == 0 {
		if resetter, ok := proj.(interface{ Reset(context.Context) error }); ok {
			if err := resetter.Reset(ctx); err != nil {
				return 0, err
			}
		}
		fromSeq = 1
	}
	var count uint64
	for rec, err := range lg.Read(ctx, fromSeq) {
		if err != nil {
			return count, err
		}
		if err := proj.Apply(ctx, rec); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
