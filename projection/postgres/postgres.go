package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// SchemaSQL defines the PostgreSQL read-model projection schema (Spec C1 & C3).
// PostgreSQL is strictly a read-model projection for analytics/queries, never the ledger store.
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS agentsquare_checkpoint (
	id INT PRIMARY KEY CHECK (id = 1),
	last_seq BIGINT NOT NULL,
	last_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agentsquare_agents (
	agent_id TEXT PRIMARY KEY,
	pubkey TEXT NOT NULL,
	display_name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS agentsquare_fills (
	seq BIGINT PRIMARY KEY,
	round BIGINT NOT NULL,
	asset TEXT NOT NULL,
	agent_id TEXT NOT NULL,
	side TEXT NOT NULL,
	filled_qty BIGINT NOT NULL,
	clearing_price BIGINT NOT NULL,
	cash_delta BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS agentsquare_clears (
	round BIGINT NOT NULL,
	asset TEXT NOT NULL,
	clearing_price BIGINT NOT NULL,
	volume BIGINT NOT NULL,
	seed_sig TEXT NOT NULL,
	PRIMARY KEY (round, asset)
);
`

// Projection implements ledger.Projection for PostgreSQL.
type Projection struct {
	mu      sync.RWMutex
	db      *sql.DB
	lastSeq uint64
}

// New initializes a PostgreSQL read-model projection.
func New(ctx context.Context, db *sql.DB) (*Projection, error) {
	p := &Projection{db: db}
	if db != nil {
		if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
			return nil, fmt.Errorf("postgres schema init: %w", err)
		}
		var seq uint64
		var hash string
		if err := db.QueryRowContext(ctx, `SELECT last_seq, last_hash FROM agentsquare_checkpoint WHERE id = 1`).Scan(&seq, &hash); err == nil {
			p.lastSeq = seq
		}
	}
	return p, nil
}

func (p *Projection) Apply(ctx context.Context, rec ledger.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rec.Seq <= p.lastSeq {
		return nil
	}
	if p.db != nil {
		switch rec.Type {
		case ledger.RecAgentAdmitted:
			var b ledger.AgentAdmittedBody
			if err := json.Unmarshal(rec.Body, &b); err == nil {
				if _, err := p.db.ExecContext(ctx,
					`INSERT INTO agentsquare_agents (agent_id, pubkey, display_name) VALUES ($1, $2, $3) ON CONFLICT (agent_id) DO UPDATE SET pubkey = EXCLUDED.pubkey`,
					b.AgentID, b.PubKey, b.DisplayName); err != nil {
					return err
				}
			}
		case ledger.RecClear:
			var b ledger.ClearBody
			if err := json.Unmarshal(rec.Body, &b); err == nil {
				if _, err := p.db.ExecContext(ctx,
					`INSERT INTO agentsquare_clears (round, asset, clearing_price, volume, seed_sig) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (round, asset) DO NOTHING`,
					b.Round, b.Asset, b.ClearingPrice, b.Volume, b.SeedSig); err != nil {
					return err
				}
			}
		case ledger.RecFill:
			var f protocol.Fill
			if err := json.Unmarshal(rec.Body, &f); err == nil {
				if _, err := p.db.ExecContext(ctx,
					`INSERT INTO agentsquare_fills (seq, round, asset, agent_id, side, filled_qty, clearing_price, cash_delta) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (seq) DO NOTHING`,
					rec.Seq, f.Round, f.Asset, f.AgentID, string(f.Side), f.FilledQty, f.ClearingPrice, f.CashDelta); err != nil {
					return err
				}
			}
		}
		if _, err := p.db.ExecContext(ctx,
			`INSERT INTO agentsquare_checkpoint (id, last_seq, last_hash) VALUES (1, $1, $2) ON CONFLICT (id) DO UPDATE SET last_seq = EXCLUDED.last_seq, last_hash = EXCLUDED.last_hash`,
			rec.Seq, rec.Hash); err != nil {
			return err
		}
	}
	p.lastSeq = rec.Seq
	return nil
}

func (p *Projection) Checkpoint(_ context.Context) (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastSeq, nil
}
