package raft_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/ledger/logtest"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/raft"
	"github.com/galaxytemple/AgentSquare/sim"
)

func TestSingleVoterRaftLogConformance(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		return raft.NewNode(raft.Config{
			NodeID:    "node-1",
			Bootstrap: true,
		})
	})
}

type simGame struct {
	m manifest.Manifest
}

func (g *simGame) Manifest() manifest.Manifest         { return g.m }
func (g *simGame) ReferencePrice(_ string, _ uint64) int64 { return 10 }
func (g *simGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 100, Reason: "deck"},
		{Asset: "copper", Qty: 10, Reason: "deck"},
	}, nil
}
func (g *simGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error { return nil }

// TestDeterministicSimLeaderFailoverAndPartition (Spec C6):
// - 3-node Raft cluster (node-1, node-2, node-3) with shared game key.
// - Orders are accepted on leader (node-1) and replicated to followers.
// - Inject network partition isolating old leader (node-1) before clearing; old leader's clearing attempt fails with ErrNoQuorum (no split-brain clearing).
// - Elect node-2 as new leader from replicated log; node-2 clears the round deterministically.
// - Heal partition and verify all 3 nodes converge to the exact same state_hash with zero double clearing or lost orders.
func TestDeterministicSimLeaderFailoverAndPartition(t *testing.T) {
	ctx := context.Background()
	cluster := raft.NewSimCluster("node-1", "node-2", "node-3")

	srvSeed := make([]byte, 32)
	srvSeed[0] = 77
	srvKey, _ := protocol.NewSignerFromSeed(srvSeed)

	t0 := time.Date(2026, 11, 9, 14, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &simGame{
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "raft-sim-game",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			FeeBps:            0,
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey:      srvKey.PublicKeyString(),
			AdminPubKeys:      []string{srvKey.PublicKeyString()},
		},
	}

	// Start Engine 1 on Leader node-1
	eng1, err := engine.New(gm, engine.WithLog(cluster.Node("node-1")), engine.WithSigner(srvKey), engine.WithClock(clk))
	if err != nil {
		t.Fatalf("eng1 init: %v", err)
	}

	agA, _ := protocol.NewSignerFromSeed(bytesRepeat(11, 32))
	agB, _ := protocol.NewSignerFromSeed(bytesRepeat(22, 32))
	ts := t0.Unix()

	for _, ag := range []*protocol.MemorySigner{agA, agB} {
		raw := signedEnv(ag, "raft-sim-game", protocol.TypeAgentRegister, "reg", ts, protocol.RegisterPayload{PubKey: ag.PublicKeyString()})
		if _, rej := eng1.SubmitRawEnvelope(ctx, raw); rej != nil {
			t.Fatalf("register failed: %+v", rej)
		}
	}

	// Submit buy and sell orders on node-1
	buyRaw := signedEnv(agA, "raft-sim-game", protocol.TypePlaceOrder, "o-buy", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5,
	})
	sellRaw := signedEnv(agB, "raft-sim-game", protocol.TypePlaceOrder, "o-sell", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 5,
	})
	if _, rej := eng1.SubmitRawEnvelope(ctx, buyRaw); rej != nil {
		t.Fatalf("buy failed: %+v", rej)
	}
	if _, rej := eng1.SubmitRawEnvelope(ctx, sellRaw); rej != nil {
		t.Fatalf("sell failed: %+v", rej)
	}

	// Inject partition isolating node-1 right before clearing!
	cluster.IsolateNode("node-1", true)

	// Partitioned old leader node-1 tries to clear -> MUST fail with ErrNoQuorum (no double clearing!)
	if _, err := eng1.SealAndClearRound(ctx); err == nil {
		t.Fatalf("expected partitioned old leader to fail clearing without quorum")
	}

	// Elect node-2 as new Leader among {node-2, node-3} majority quorum
	if err := cluster.ElectLeader("node-2"); err != nil {
		t.Fatalf("ElectLeader(node-2): %v", err)
	}

	// Boot Engine 2 on new leader node-2 (replays replicated log automatically!)
	eng2, err := engine.New(gm, engine.WithLog(cluster.Node("node-2")), engine.WithSigner(srvKey), engine.WithClock(clk))
	if err != nil {
		t.Fatalf("eng2 init on failover leader: %v", err)
	}

	// New leader node-2 clears the round
	fills, err := eng2.SealAndClearRound(ctx)
	if err != nil {
		t.Fatalf("eng2 SealAndClearRound failed: %v", err)
	}
	if len(fills) != 2 {
		t.Fatalf("expected 2 fills on new leader, got %d", len(fills))
	}

	// Heal partition and sync node-1 from new leader node-2
	cluster.IsolateNode("node-1", false)
	_ = cluster.ElectLeader("node-2")

	// Verify all 3 nodes replay identically to the exact same state_hash
	var refHash string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		st, err := ledger.ReplayAndVerify(&gm.m, cluster.Node(id).Read(ctx, 1))
		if err != nil {
			t.Fatalf("ReplayAndVerify on %s failed: %v", id, err)
		}
		h, _ := st.StateHash()
		if refHash == "" {
			refHash = h
		} else if h != refHash {
			t.Fatalf("state_hash divergence on %s: %s vs %s", id, h, refHash)
		}
	}

	// Test HTTPTransport replication between Leader and Follower over HTTP
	follower := raft.NewNode(raft.Config{NodeID: "http-follower", Peers: []string{"http-leader"}})
	mux := http.NewServeMux()
	mux.Handle("/internal/raft/append", follower.Handler())
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	httpTr := raft.NewHTTPTransport(map[string]string{"http-follower": httpSrv.URL})
	leader := raft.NewNode(raft.Config{
		NodeID:    "http-leader",
		Peers:     []string{"http-follower"},
		Transport: httpTr,
		Bootstrap: true,
	})
	rec1 := ledger.Record{Seq: 1, PrevHash: ledger.ZeroHash, GameID: "g1", Type: ledger.RecGenesis, Hash: "sha256:1111"}
	if _, err := leader.Append(ctx, rec1); err != nil {
		t.Fatalf("HTTPTransport Append failed: %v", err)
	}
	fSeq, fHash, _ := follower.Head(ctx)
	if fSeq != 1 || fHash != "sha256:1111" {
		t.Fatalf("expected follower head (1, sha256:1111), got (%d, %s)", fSeq, fHash)
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func signedEnv(s protocol.Signer, gameID, msgType, nonce string, ts int64, payload any) []byte {
	rawP, _ := json.Marshal(payload)
	env := protocol.Envelope{
		Version:   "1.0",
		GameID:    gameID,
		From:      s.AgentID(),
		To:        "server",
		Type:      msgType,
		Timestamp: ts,
		Nonce:     nonce,
		CorrID:    "c-" + nonce,
		Payload:   rawP,
	}
	_ = protocol.SignEnvelope(&env, s)
	b, _ := json.Marshal(env)
	return b
}
