package conformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/projection/sqlite"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/raft"
	"github.com/galaxytemple/AgentSquare/sim"
	"github.com/galaxytemple/AgentSquare/storage/streamlog"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

type e2eMarketGame struct {
	m manifest.Manifest
}

func (g *e2eMarketGame) Manifest() manifest.Manifest           { return g.m }
func (g *e2eMarketGame) ReferencePrice(_ string, _ uint64) int64 { return 100 }
func (g *e2eMarketGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 10000, Reason: "initial_grant"},
		{Asset: "copper", Qty: 100, Reason: "initial_grant"},
	}, nil
}
func (g *e2eMarketGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error { return nil }

// inMemoryAppendDB simulates an external append-specialized stream database (e.g. NATS JetStream / Kafka)
// enforcing strict optimistic concurrency on expectedLastSeq.
type inMemoryAppendDB struct {
	mu      sync.Mutex
	entries [][]byte
}

func (db *inMemoryAppendDB) PublishBatch(_ context.Context, expectedLastSeq uint64, payloads [][]byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if uint64(len(db.entries)) != expectedLastSeq {
		return fmt.Errorf("append-db optimistic lock failed: tail=%d, expected=%d", len(db.entries), expectedLastSeq)
	}
	for _, p := range payloads {
		cp := make([]byte, len(p))
		copy(cp, p)
		db.entries = append(db.entries, cp)
	}
	return nil
}

func (db *inMemoryAppendDB) FetchFrom(_ context.Context, fromSeq uint64) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if fromSeq <= 1 {
		fromSeq = 1
	}
	if int(fromSeq-1) >= len(db.entries) {
		return nil, nil
	}
	out := make([][]byte, len(db.entries)-int(fromSeq-1))
	for i := range out {
		src := db.entries[int(fromSeq-1)+i]
		cp := make([]byte, len(src))
		copy(cp, src)
		out[i] = cp
	}
	return out, nil
}

// TestE2EMultiNodeRaftAndAppendDBWithVirtualAgents spins up:
// 1) A 3-node Raft cluster (node-1, node-2, node-3) + SQLite read-model Projection + HTTP Server
// 2) 5 virtual agents trading across 3 rounds over HTTP (including mid-round Raft leader failover from node-1 to node-2)
// 3) Verifies fills, portfolios, historical prices (candles, tape, stats, VWAP), talk-vs-trade, and full ledger replay convergence
// 4) Repeats the multi-agent trading & historical query scenario against the external Append-Specialized Stream DB (streamlog).
func TestE2EMultiNodeRaftAndAppendDBWithVirtualAgents(t *testing.T) {
	ctx := context.Background()

	srvSeed := make([]byte, 32)
	srvSeed[0] = 0x77
	srvKey, _ := protocol.NewSignerFromSeed(srvSeed)

	gm := &e2eMarketGame{
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "e2e-city-market",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			FeeBps:            0,
			Board: &manifest.BoardConfig{
				Enabled:                  true,
				MaxPostsPerAgentPerRound: 5,
				MaxBodyChars:             280,
				MinIntervalSec:           0,
				RequireParticipant:       true,
				AllowedContent:           "text/plain",
			},
			Admission:    "open",
			Ledger:       manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey: srvKey.PublicKeyString(),
			AdminPubKeys: []string{srvKey.PublicKeyString()},
		},
	}

	// =========================================================================
	// Part 1: 3-Node Raft Cluster + SQLite Read-Model Projection + 5 Agents E2E
	// =========================================================================
	cluster := raft.NewSimCluster("node-1", "node-2", "node-3")
	sqlProj, err := sqlite.New(ctx, nil)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}

	eng1, err := engine.New(gm,
		engine.WithLog(cluster.Node("node-1")),
		engine.WithProjection(sqlProj),
		engine.WithSigner(srvKey),
	)
	if err != nil {
		t.Fatalf("eng1 init: %v", err)
	}
	srv1 := httptest.NewServer(aghttp.NewServer(eng1, aghttp.WithSimTrigger(true)))
	defer srv1.Close()

	// Create 5 virtual agents
	type virtualAgent struct {
		name   string
		signer *protocol.MemorySigner
		cli    *client.Client
	}
	names := []string{"Alice_Buyer", "Bob_Buyer", "Charlie_Buyer", "Dave_Seller", "Eve_Manipulator"}
	agents := make([]*virtualAgent, len(names))
	for i, name := range names {
		seed := make([]byte, 32)
		seed[0] = byte(100 + i)
		s, _ := protocol.NewSignerFromSeed(seed)
		va := &virtualAgent{
			name:   name,
			signer: s,
			cli:    client.New(srv1.URL, gm.m.GameID, s),
		}
		if _, rej, err := va.cli.Register(ctx, name, ""); err != nil || rej != nil {
			t.Fatalf("register %s failed: err=%v rej=%+v", name, err, rej)
		}
		agents[i] = va
	}

	// --- ROUND 1 (Trading at $90) ---
	// Alice, Bob, Charlie each bid $90 for 5 copper (total demand 15)
	// Dave offers 10 copper at min $80 (total supply 10) -> should clear 10 units @ $90 (3+3+4 lottery!)
	for _, buyer := range agents[:3] {
		if _, rej, err := buyer.cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 90, 5); err != nil || rej != nil {
			t.Fatalf("Round 1 buy (%s) failed: err=%v rej=%+v", buyer.name, err, rej)
		}
	}
	if _, rej, err := agents[3].cli.PlaceOrder(ctx, "copper", protocol.SideSell, 80, 10); err != nil || rej != nil {
		t.Fatalf("Round 1 sell (Dave) failed: err=%v rej=%+v", err, rej)
	}
	// Eve posts a bearish statement on the board, then buys 2 copper @ $95 (strictly better price -> filled in full!)
	if _, rej, err := agents[4].cli.PostBoard(ctx, "copper", "bear", "Copper is crashing, everyone sell!", ""); err != nil || rej != nil {
		t.Fatalf("Eve board post failed: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := agents[4].cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 95, 2); err != nil || rej != nil {
		t.Fatalf("Eve buy order failed: err=%v rej=%+v", err, rej)
	}

	// Check pre-clear indicative price over HTTP before Round 1 clears
	indR1, err := agents[0].cli.GetIndicative(ctx, "copper")
	if err != nil || indR1.IndicativeVolume != 10 {
		t.Fatalf("Round 1 indicative mismatch: %+v err=%v", indR1, err)
	}

	// Trigger Round 1 clearing on Leader Node-1
	r1Fills, err := eng1.SealAndClearRound(ctx)
	if err != nil {
		t.Fatalf("Round 1 clear failed: %v", err)
	}
	t.Logf("[E2E Round 1] Cleared %d fills on Leader node-1", len(r1Fills))

	// --- ROUND 2 (Orders placed on node-1, then node-1 crashes -> node-2 elected Leader & clears at $105) ---
	// Alice and Bob bid $105 for 4 copper each; Dave sells 8 copper at $100
	if _, rej, err := agents[0].cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 105, 4); err != nil || rej != nil {
		t.Fatalf("Round 2 Alice buy: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := agents[1].cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 105, 4); err != nil || rej != nil {
		t.Fatalf("Round 2 Bob buy: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := agents[3].cli.PlaceOrder(ctx, "copper", protocol.SideSell, 100, 8); err != nil || rej != nil {
		t.Fatalf("Round 2 Dave sell: err=%v rej=%+v", err, rej)
	}

	// Simulate Leader node-1 crash / isolation and elect node-2 as new Leader!
	cluster.IsolateNode("node-1", true)
	if err := cluster.ElectLeader("node-2"); err != nil {
		t.Fatalf("ElectLeader(node-2): %v", err)
	}
	eng2, err := engine.New(gm,
		engine.WithLog(cluster.Node("node-2")),
		engine.WithProjection(sqlProj),
		engine.WithSigner(srvKey),
	)
	if err != nil {
		t.Fatalf("eng2 failover init: %v", err)
	}
	srv2 := httptest.NewServer(aghttp.NewServer(eng2, aghttp.WithSimTrigger(true)))
	defer srv2.Close()

	// Point all virtual agents to the new Leader HTTP server (srv2)
	for _, va := range agents {
		va.cli = client.New(srv2.URL, gm.m.GameID, va.signer)
	}

	// New Leader node-2 clears Round 2 using the replicated orders from before node-1 died!
	r2Fills, err := eng2.SealAndClearRound(ctx)
	if err != nil {
		t.Fatalf("Round 2 clear on failover leader node-2 failed: %v", err)
	}
	t.Logf("[E2E Round 2] Failover Leader node-2 cleared %d fills", len(r2Fills))

	// --- ROUND 3 (Trading on node-2 at $115) ---
	if _, rej, err := agents[2].cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 115, 6); err != nil || rej != nil {
		t.Fatalf("Round 3 Charlie buy: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := agents[3].cli.PlaceOrder(ctx, "copper", protocol.SideSell, 115, 6); err != nil || rej != nil {
		t.Fatalf("Round 3 Dave sell: err=%v rej=%+v", err, rej)
	}
	r3Fills, err := eng2.SealAndClearRound(ctx)
	if err != nil {
		t.Fatalf("Round 3 clear failed: %v", err)
	}
	t.Logf("[E2E Round 3] Cleared %d fills", len(r3Fills))

	// --- VERIFY HISTORICAL PRICES, CANDLES, TAPE, STATS, PORTFOLIOS, TALK-VS-TRADE ---
	candles, err := agents[0].cli.GetCandles(ctx, "copper", 10)
	if err != nil || len(candles) != 3 {
		t.Fatalf("expected 3 historical candles, got %+v err=%v", candles, err)
	}
	t.Logf("[E2E Historical Candles] R1=$%d (vol=%d), R2=$%d (vol=%d), R3=$%d (vol=%d)",
		candles[0].Close, candles[0].Volume,
		candles[1].Close, candles[1].Volume,
		candles[2].Close, candles[2].Volume)

	if candles[0].Volume != 10 || candles[1].Volume != 8 || candles[2].Volume != 6 || candles[2].Close != 115 {
		t.Fatalf("unexpected candle history: %+v", candles)
	}

	tape, err := agents[0].cli.GetTape(ctx, "copper", 10)
	if err != nil || len(tape) != 3 {
		t.Fatalf("expected 3 rounds on public tape, got %d", len(tape))
	}

	stats, err := agents[0].cli.GetStats(ctx, "copper", 10)
	if err != nil || stats.TotalVolume != 24 || stats.LastPrice != 115 || stats.High != 115 || stats.VWAP <= 0 {
		t.Fatalf("unexpected market stats: %+v err=%v", stats, err)
	}
	t.Logf("[E2E Market Stats] LastPrice=$%d, VWAP=$%d, High=$%d, Low=$%d, TotalVolume=%d, UnfilledRatio=%d bps",
		stats.LastPrice, stats.VWAP, stats.High, stats.Low, stats.TotalVolume, stats.UnfilledRatio)

	// Verify Seller Dave's portfolio reflects all 24 copper sold across the 3 rounds (100 - 24 = 76 copper remaining)
	davePF, err := agents[3].cli.GetPortfolio(ctx, "")
	if err != nil || davePF.Available["copper"] != 76 {
		t.Fatalf("expected Dave to have 76 copper after selling 24, got %+v err=%v", davePF, err)
	}

	// Verify Manipulator Eve's Talk-vs-Trade flags her Round 1 contrarian trade (posted bear + bought 2 copper)
	eveTVT, err := agents[4].cli.TalkVsTrade(ctx, agents[4].signer.AgentID(), "copper", 2)
	if err != nil || eveTVT.ContrarianCount != 1 || eveTVT.ContrarianRatioBps != 10000 {
		t.Fatalf("expected Eve's contrarian ratio to be 10000 bps (100%%), got %+v err=%v", eveTVT, err)
	}

	// Heal node-1 partition and verify all 3 Raft nodes + HTTP downloaded ledger converge to identical state_hash
	cluster.IsolateNode("node-1", false)
	_ = cluster.ElectLeader("node-2")

	resp, err := http.Get(srv2.URL + "/v1/ledger")
	if err != nil {
		t.Fatalf("GET /v1/ledger: %v", err)
	}
	rawLedger, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	lines := strings.Split(strings.TrimSpace(string(rawLedger)), "\n")
	var httpRecs []ledger.Record
	for _, line := range lines {
		var r ledger.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("unmarshal ledger line: %v", err)
		}
		httpRecs = append(httpRecs, r)
	}

	httpState := ledger.NewState(&gm.m)
	for _, r := range httpRecs {
		if err := httpState.Apply(r); err != nil {
			t.Fatalf("http ledger replay Apply failed: %v", err)
		}
	}
	httpHash, _ := httpState.StateHash()

	for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
		st, err := ledger.ReplayAndVerify(&gm.m, cluster.Node(nodeID).Read(ctx, 1))
		if err != nil {
			t.Fatalf("ReplayAndVerify(%s) failed: %v", nodeID, err)
		}
		nh, _ := st.StateHash()
		if nh != httpHash {
			t.Fatalf("state_hash mismatch on %s: %s vs http %s", nodeID, nh, httpHash)
		}
	}
	t.Logf("[E2E Raft Convergence] All 3 Raft nodes & HTTP ledger verified at state_hash=%s (%d records)", httpHash, len(httpRecs))

	// =========================================================================
	// Part 2: External Append-Specialized Stream DB (streamlog) E2E Verification
	// =========================================================================
	appendDB := &inMemoryAppendDB{}
	sLog, err := streamlog.New(ctx, appendDB)
	if err != nil {
		t.Fatalf("streamlog.New: %v", err)
	}
	engStream, err := engine.New(gm, engine.WithLog(sLog), engine.WithSigner(srvKey))
	if err != nil {
		t.Fatalf("engStream init: %v", err)
	}
	srvStream := httptest.NewServer(aghttp.NewServer(engStream, aghttp.WithSimTrigger(true)))
	defer srvStream.Close()

	// Replay the exact same virtual agent trading scenario against the Append-Specialized DB server
	for _, va := range agents[:4] {
		va.cli = client.New(srvStream.URL, gm.m.GameID, va.signer)
		if _, rej, err := va.cli.Register(ctx, va.name, ""); err != nil || rej != nil {
			t.Fatalf("streamlog register %s: err=%v rej=%+v", va.name, err, rej)
		}
	}
	_, _, _ = agents[0].cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 95, 5)
	_, _, _ = agents[3].cli.PlaceOrder(ctx, "copper", protocol.SideSell, 85, 5)
	if _, err := http.Post(srvStream.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil)); err != nil {
		t.Fatalf("streamlog clear 1: %v", err)
	}
	streamCandles, err := agents[0].cli.GetCandles(ctx, "copper", 5)
	if err != nil || len(streamCandles) != 1 || streamCandles[0].Volume != 5 {
		t.Fatalf("streamlog candles mismatch: %+v err=%v", streamCandles, err)
	}
	t.Logf("[E2E Append-Stream DB] Verified round clearing & historical candles on streamlog (price=$%d, vol=%d)",
		streamCandles[0].Close, streamCandles[0].Volume)
}

func TestE2EScheduledCutoffAndDynamicIntervalChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvKey, _ := protocol.GenerateKeyPair(nil)
	adminKey, _ := protocol.GenerateKeyPair(nil)

	// Start Round 1 at 14:00:00Z with 1-hour interval ("PT1H") -> Cutoff is 15:00:00Z
	t0 := time.Date(2026, 11, 10, 14, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &e2eMarketGame{
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "e2e-schedule-cutoff",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round: manifest.RoundConfig{
				Interval:  "PT1H",
				Anchor:    "2026-11-10T14:00:00Z",
				CarryOver: false,
			},
			PriceBand:    manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:   manifest.MarketDataConfig{PreClear: "indicative"},
			Admission:    "open",
			Ledger:       manifest.LedgerConfig{Public: "full"},
			ServerPubKey: srvKey.PublicKeyString(),
			AdminPubKeys: []string{adminKey.PublicKeyString()},
		},
	}

	eng, err := engine.New(gm,
		engine.WithSigner(srvKey),
		engine.WithClock(clk),
		engine.WithStrictCutoff(true),
		engine.WithTimestampTolerance(0), // allow simulated time steps with client.Client
	)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	// Start background scheduler polling every 5ms
	eng.StartScheduler(ctx, 5*time.Millisecond)

	srv := httptest.NewServer(aghttp.NewServer(eng))
	defer srv.Close()

	aliceKey, _ := protocol.GenerateKeyPair(nil)
	bobKey, _ := protocol.GenerateKeyPair(nil)
	charlieKey, _ := protocol.GenerateKeyPair(nil)

	alice := client.New(srv.URL, gm.m.GameID, aliceKey)
	bob := client.New(srv.URL, gm.m.GameID, bobKey)
	charlie := client.New(srv.URL, gm.m.GameID, charlieKey)
	adminCli := client.New(srv.URL, gm.m.GameID, adminKey)

	_, _, _ = alice.Register(ctx, "Alice", "")
	_, _, _ = bob.Register(ctx, "Bob", "")
	_, _, _ = charlie.Register(ctx, "Charlie", "")

	// 1. At 14:59:59Z (1 second before the 15:00:00Z hourly cutoff), Alice and Bob submit orders
	clk.Set(time.Date(2026, 11, 10, 14, 59, 59, 0, time.UTC))
	if rec, rej, err := alice.PlaceOrder(ctx, "copper", protocol.SideBuy, 95, 5); err != nil || rej != nil || rec.Round != 1 {
		t.Fatalf("expected Alice order accepted in Round 1 at 14:59:59Z, got rec=%+v rej=%+v err=%v", rec, rej, err)
	}
	if rec, rej, err := bob.PlaceOrder(ctx, "copper", protocol.SideSell, 85, 5); err != nil || rej != nil || rec.Round != 1 {
		t.Fatalf("expected Bob order accepted in Round 1 at 14:59:59Z, got rec=%+v rej=%+v err=%v", rec, rej, err)
	}

	// Admin schedules a dynamic interval change: starting from Round 2, settle EVERY MINUTE ("PT1M") instead of "PT1H"
	rawUpdatePayload, _ := json.Marshal(protocol.ManifestUpdatePayload{
		NewHash:        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		EffectiveRound: 2,
		Interval:       "PT1M",
	})
	updateEnv := protocol.Envelope{
		Version:   "1.0",
		GameID:    gm.m.GameID,
		From:      adminKey.AgentID(),
		To:        "server",
		Type:      protocol.TypeManifestUpdate,
		Timestamp: clk.Now().Unix(),
		Nonce:     "admin-upd-1",
		CorrID:    "corr-upd-1",
		Payload:   rawUpdatePayload,
	}
	_ = protocol.SignEnvelope(&updateEnv, adminKey)
	rawUpdateBytes, _ := json.Marshal(updateEnv)
	if rec, rej := eng.SubmitRawEnvelope(ctx, rawUpdateBytes); rej != nil || rec == nil {
		t.Fatalf("manifest.update failed: rej=%+v", rej)
	}
	_ = adminCli

	// 2. Verify that if an order arrives right at 15:00:00Z BEFORE Tick runs, StrictCutoff rejects it with ROUND_SEALED
	engNoAuto, _ := engine.New(gm, engine.WithSigner(srvKey), engine.WithClock(clk), engine.WithStrictCutoff(true), engine.WithTimestampTolerance(0))
	rawReg, _ := json.Marshal(protocol.RegisterPayload{PubKey: charlieKey.PublicKeyString(), DisplayName: "Charlie"})
	regEnv := protocol.Envelope{Version: "1.0", GameID: gm.m.GameID, From: charlieKey.AgentID(), To: "server", Type: protocol.TypeAgentRegister, Timestamp: clk.Now().Unix(), Nonce: "n1", CorrID: "c1", Payload: rawReg}
	_ = protocol.SignEnvelope(&regEnv, charlieKey)
	bReg, _ := json.Marshal(regEnv)
	_, _ = engNoAuto.SubmitRawEnvelope(ctx, bReg)

	// Advance clock to exact cutoff 15:00:00Z
	clk.Set(time.Date(2026, 11, 10, 15, 0, 0, 0, time.UTC))

	// Charlie's late order at 15:00:00Z on un-ticked engine is strictly rejected with ROUND_SEALED!
	rawLate, _ := json.Marshal(protocol.PlaceOrderPayload{Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 5})
	lateEnv := protocol.Envelope{Version: "1.0", GameID: gm.m.GameID, From: charlieKey.AgentID(), To: "server", Type: protocol.TypePlaceOrder, Timestamp: clk.Now().Unix(), Nonce: "n2", CorrID: "c2", Payload: rawLate}
	_ = protocol.SignEnvelope(&lateEnv, charlieKey)
	bLate, _ := json.Marshal(lateEnv)
	if _, rej := engNoAuto.SubmitRawEnvelope(ctx, bLate); rej == nil || rej.Code != protocol.ErrCodeRoundSealed {
		t.Fatalf("expected late order at exact cutoff 15:00:00Z to be rejected with ROUND_SEALED, got %+v", rej)
	}

	// 3. Wait briefly for background scheduler on `eng` to detect 15:00:00Z and auto-settle Round 1
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		r, _, _, _ := eng.CurrentRoundBounds()
		if r == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	r, start2, end2, err := eng.CurrentRoundBounds()
	if err != nil || r != 2 {
		t.Fatalf("expected background scheduler to advance to Round 2 at 15:00:00Z, got round=%d err=%v", r, err)
	}
	// Verify that Round 2 automatically switched to 1-minute interval ("PT1M"): [15:00:00Z, 15:01:00Z)
	expectedEnd2 := time.Date(2026, 11, 10, 15, 1, 0, 0, time.UTC)
	if !end2.Equal(expectedEnd2) || eng.Manifest().Round.Interval != "PT1M" {
		t.Fatalf("expected Round 2 bounds [15:00:00Z, 15:01:00Z) with PT1M, got [%s, %s) interval=%s",
			start2.Format(time.RFC3339), end2.Format(time.RFC3339), eng.Manifest().Round.Interval)
	}
	t.Logf("[E2E Scheduled Cutoff] Round 1 auto-settled at 15:00:00Z; Round 2 dynamically switched to PT1M [%s, %s)",
		start2.Format(time.RFC3339), end2.Format(time.RFC3339))

	// 4. Submit Round 2 orders at 15:00:30Z, then advance clock by 1 minute to 15:01:00Z
	clk.Set(time.Date(2026, 11, 10, 15, 0, 30, 0, time.UTC))
	_, _, _ = alice.PlaceOrder(ctx, "copper", protocol.SideBuy, 105, 4)
	_, _, _ = bob.PlaceOrder(ctx, "copper", protocol.SideSell, 95, 4)

	clk.Set(time.Date(2026, 11, 10, 15, 1, 0, 0, time.UTC))
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		r, _, _, _ = eng.CurrentRoundBounds()
		if r == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if r != 3 {
		t.Fatalf("expected Round 2 to auto-settle after 1 minute at 15:01:00Z, got round=%d", r)
	}

	// 5. Dynamically change Round 3 interval on the fly to 10 seconds ("PT10S") via SetRoundInterval
	if err := eng.SetRoundInterval("PT10S"); err != nil {
		t.Fatalf("SetRoundInterval(PT10S): %v", err)
	}
	_, start3, end3, _ := eng.CurrentRoundBounds()
	expectedEnd3 := time.Date(2026, 11, 10, 15, 1, 10, 0, time.UTC)
	if !end3.Equal(expectedEnd3) {
		t.Fatalf("expected Round 3 cutoff at 15:01:10Z, got %s", end3.Format(time.RFC3339))
	}
	t.Logf("[E2E Dynamic Interval] Round 3 reconfigured on-the-fly to PT10S [%s, %s)",
		start3.Format(time.RFC3339), end3.Format(time.RFC3339))

	// Advance 10 seconds to 15:01:10Z and verify Round 3 auto-settles
	clk.Set(expectedEnd3)
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		r, _, _, _ = eng.CurrentRoundBounds()
		if r == 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if r != 4 {
		t.Fatalf("expected Round 3 to auto-settle after 10 seconds at 15:01:10Z, got round=%d", r)
	}

	candles, err := alice.GetCandles(ctx, "copper", 10)
	if err != nil || len(candles) != 3 || candles[0].Volume != 5 || candles[1].Volume != 4 || candles[2].Volume != 0 {
		t.Fatalf("expected 3 candles (R1 vol=5, R2 vol=4, R3 vol=0), got %+v err=%v", candles, err)
	}
	t.Logf("[E2E Scheduled Settlement Complete] Verified PT1H -> PT1M -> PT10S auto-clearing across 3 rounds")
}

func TestE2EOrderPersistenceTTLCancelAndDefaultExpiry(t *testing.T) {
	ctx := context.Background()
	srvKey, _ := protocol.GenerateKeyPair(nil)

	gm := &e2eMarketGame{
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "e2e-order-ttl-cancel",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round: manifest.RoundConfig{
				Interval:  "PT1H",
				Anchor:    "2026-11-10T00:00:00Z",
				CarryOver: false, // Default: auto-expire unfilled orders at round settlement
			},
			PriceBand:    manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:   manifest.MarketDataConfig{PreClear: "indicative"},
			Admission:    "open",
			Ledger:       manifest.LedgerConfig{Public: "full"},
			ServerPubKey: srvKey.PublicKeyString(),
		},
	}

	eng, err := engine.New(gm, engine.WithSigner(srvKey))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
	defer srv.Close()

	aliceKey, _ := protocol.GenerateKeyPair(nil)
	bobKey, _ := protocol.GenerateKeyPair(nil)
	charlieKey, _ := protocol.GenerateKeyPair(nil)
	daveKey, _ := protocol.GenerateKeyPair(nil)

	alice := client.New(srv.URL, gm.m.GameID, aliceKey)
	bob := client.New(srv.URL, gm.m.GameID, bobKey)
	charlie := client.New(srv.URL, gm.m.GameID, charlieKey)
	dave := client.New(srv.URL, gm.m.GameID, daveKey)

	_, _, _ = alice.Register(ctx, "Alice", "")
	_, _, _ = bob.Register(ctx, "Bob", "")
	_, _, _ = charlie.Register(ctx, "Charlie", "")
	_, _, _ = dave.Register(ctx, "Dave", "")

	// =========================================================================
	// Round 1:
	// - Alice submits DEFAULT order (1-round auto-expire): Buy 5 @ $90 ($450 locked)
	// - Bob submits GTC ("사질때까지 냅둬", CarryOver=true): Buy 6 @ $95 ($570 locked)
	// - Charlie submits 2-round TTL ("2회차까지 유지", TTLRounds=2): Buy 5 @ $88 ($440 locked)
	// No seller in Round 1 -> 0 fills!
	// =========================================================================
	if _, rej, err := alice.PlaceOrder(ctx, "copper", protocol.SideBuy, 90, 5); err != nil || rej != nil {
		t.Fatalf("Alice default order failed: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := bob.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 95, 6, client.OrderOptions{CarryOver: true}); err != nil || rej != nil {
		t.Fatalf("Bob GTC order failed: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := charlie.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 88, 5, client.OrderOptions{TTLRounds: 2}); err != nil || rej != nil {
		t.Fatalf("Charlie TTL=2 order failed: err=%v rej=%+v", err, rej)
	}

	// Settle Round 1 (no sellers -> all 3 orders fail to match in Round 1)
	_, _ = http.Post(srv.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil))

	// 1) Alice's default order MUST be automatically expired and $450 refunded
	pAlice, _ := alice.GetPortfolio(ctx, "")
	if len(pAlice.OpenOrders) != 0 || pAlice.Available["usd"] != 10000 || pAlice.Locked["usd"] != 0 {
		t.Fatalf("expected Alice default order to auto-expire after Round 1, got %+v", pAlice)
	}

	// 2) Bob's GTC (CarryOver=true) order MUST remain active into Round 2 (6 units, $570 locked)
	pBob, _ := bob.GetPortfolio(ctx, "")
	if len(pBob.OpenOrders) != 1 || pBob.OpenOrders[0].Qty != 6 || pBob.Locked["usd"] != 570 {
		t.Fatalf("expected Bob GTC order to carry over into Round 2, got %+v", pBob)
	}

	// 3) Charlie's TTLRounds=2 order MUST remain active into Round 2 (1 of 2 rounds elapsed)
	pCharlie, _ := charlie.GetPortfolio(ctx, "")
	if len(pCharlie.OpenOrders) != 1 || pCharlie.OpenOrders[0].Qty != 5 || pCharlie.Locked["usd"] != 440 {
		t.Fatalf("expected Charlie TTL=2 order to survive into Round 2, got %+v", pCharlie)
	}
	t.Logf("[After Round 1] Alice(default)=expired, Bob(GTC)=active(qty=6), Charlie(TTL=2)=active(qty=5)")

	// =========================================================================
	// Round 2:
	// - Dave sells 2 units @ $92 -> matches 2 units of Bob's carried-over $95 bid (Charlie's $88 bid is too low)
	// - At end of Round 2:
	//   * Bob's remaining 4 units STILL carry over into Round 3!
	//   * Charlie's TTLRounds=2 order has now completed 2 rounds without filling -> MUST auto-expire!
	// =========================================================================
	_, _, _ = dave.PlaceOrder(ctx, "copper", protocol.SideSell, 92, 2)
	_, _ = http.Post(srv.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil))

	pBobR2, _ := bob.GetPortfolio(ctx, "")
	if len(pBobR2.OpenOrders) != 1 || pBobR2.OpenOrders[0].Qty != 4 || pBobR2.Available["copper"] != 102 {
		t.Fatalf("expected Bob to fill 2 and keep 4 units open into Round 3, got %+v", pBobR2)
	}

	pCharlieR2, _ := charlie.GetPortfolio(ctx, "")
	if len(pCharlieR2.OpenOrders) != 0 || pCharlieR2.Available["usd"] != 10000 || pCharlieR2.Locked["usd"] != 0 {
		t.Fatalf("expected Charlie's TTL=2 order to auto-expire after Round 2 and refund $440, got %+v", pCharlieR2)
	}
	t.Logf("[After Round 2] Bob(GTC)=filled 2 & carried over 4, Charlie(TTL=2)=auto-expired after 2nd round")

	// =========================================================================
	// Round 3:
	// - Test Order Cancellation (cancel_order):
	//   * Alice places a new order and cancels it before settlement -> immediate refund
	//   * Bob cancels his remaining carried-over 4-unit GTC order -> immediate refund of remaining $380 escrow!
	// =========================================================================
	aliceRec, rej, err := alice.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 90, 3, client.OrderOptions{TTLRounds: 5})
	if err != nil || rej != nil {
		t.Fatalf("Alice Round 3 order failed: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := alice.CancelOrder(ctx, aliceRec.OrderID); err != nil || rej != nil {
		t.Fatalf("Alice cancel_order failed: err=%v rej=%+v", err, rej)
	}
	pAliceAfterCancel, _ := alice.GetPortfolio(ctx, "")
	if len(pAliceAfterCancel.OpenOrders) != 0 || pAliceAfterCancel.Available["usd"] != 10000 {
		t.Fatalf("expected Alice's cancelled order to refund immediately, got %+v", pAliceAfterCancel)
	}

	bobCarryOrderID := pBobR2.OpenOrders[0].OrderID
	if _, rej, err := bob.CancelOrder(ctx, bobCarryOrderID); err != nil || rej != nil {
		t.Fatalf("Bob cancel carried-over GTC order failed: err=%v rej=%+v", err, rej)
	}
	pBobAfterCancel, _ := bob.GetPortfolio(ctx, "")
	if len(pBobAfterCancel.OpenOrders) != 0 || pBobAfterCancel.Locked["usd"] != 0 {
		t.Fatalf("expected Bob's cancelled GTC order to release all locked USD, got %+v", pBobAfterCancel)
	}
	t.Logf("[Round 3 Cancellation] Both new and carried-over orders cancelled with immediate escrow release")

	// Verify full ledger replay from genesis matches live engine state_hash
	replayed, err := ledger.ReplayAndVerify(&gm.m, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify failed: %v", err)
	}
	hLive, _ := eng.StateHash()
	hReplayed, _ := replayed.StateHash()
	if hLive != hReplayed {
		t.Fatalf("state_hash mismatch: live=%s vs replayed=%s", hLive, hReplayed)
	}
}

type dynamicRefMarketGame struct {
	m      manifest.Manifest
	mu     sync.RWMutex
	refMap map[string]int64
}

func (g *dynamicRefMarketGame) Manifest() manifest.Manifest { return g.m }
func (g *dynamicRefMarketGame) ReferencePrice(asset string, _ uint64) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if p, ok := g.refMap[asset]; ok {
		return p
	}
	return 100
}
func (g *dynamicRefMarketGame) SetRefPrice(asset string, price int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.refMap[asset] = price
}
func (g *dynamicRefMarketGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 10000, Reason: "initial_grant"},
		{Asset: "copper", Qty: 100, Reason: "initial_grant"},
		{Asset: "iron", Qty: 100, Reason: "initial_grant"},
	}, nil
}
func (g *dynamicRefMarketGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error {
	return nil
}

func TestE2EHighRiskExchangeEdgeCases(t *testing.T) {
	ctx := context.Background()
	srvKey, _ := protocol.GenerateKeyPair(nil)

	gm := &dynamicRefMarketGame{
		refMap: map[string]int64{"copper": 100, "iron": 200},
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "e2e-high-risk-edge-cases",
			Assets: []manifest.Asset{
				{ID: "copper", QtyUnit: 1},
				{ID: "iron", QtyUnit: 1},
			},
			SettlementUnit: manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round: manifest.RoundConfig{
				Interval:  "PT1H",
				Anchor:    "2026-11-10T00:00:00Z",
				CarryOver: false,
			},
			PriceBand:    manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:   manifest.MarketDataConfig{PreClear: "indicative"},
			Admission:    "open",
			Ledger:       manifest.LedgerConfig{Public: "full"},
			ServerPubKey: srvKey.PublicKeyString(),
			AdminPubKeys: []string{srvKey.PublicKeyString()},
		},
	}

	eng, err := engine.New(gm, engine.WithSigner(srvKey))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
	defer srv.Close()

	aliceKey, _ := protocol.GenerateKeyPair(nil)
	bobKey, _ := protocol.GenerateKeyPair(nil)
	charlieKey, _ := protocol.GenerateKeyPair(nil)
	daveKey, _ := protocol.GenerateKeyPair(nil)

	alice := client.New(srv.URL, gm.m.GameID, aliceKey)
	bob := client.New(srv.URL, gm.m.GameID, bobKey)
	charlie := client.New(srv.URL, gm.m.GameID, charlieKey)
	dave := client.New(srv.URL, gm.m.GameID, daveKey)

	_, _, _ = alice.Register(ctx, "Alice", "")
	_, _, _ = bob.Register(ctx, "Bob", "")
	_, _, _ = charlie.Register(ctx, "Charlie", "")
	_, _, _ = dave.Register(ctx, "Dave", "")

	// =========================================================================
	// Edge Case 1 & 2 (Round 1):
	// - Alice places a GTC buy order at $140 (valid in Round 1 when ref=$100, band=[$50, $150])
	// - Bob places TWO orders at the exact same price ($90):
	//   1) GTC order (CarryOver=true) for 5 units (submitted FIRST, smaller OrderID!)
	//   2) Default 1-round order (CarryOver=false) for 5 units (submitted SECOND, larger OrderID!)
	// - Dave sells only 5 units @ $90 -> Bob gets a partial fill of 5 units out of his 10 units.
	//   * Crucial invariant: The engine MUST consume Bob's expiring 1-round order FIRST so that
	//     Bob's GTC order (CarryOver=true) is preserved into Round 2!
	// - Charlie places a TTLRounds=2 buy order at $85 for 4 units.
	// =========================================================================
	_, _, _ = alice.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 140, 2, client.OrderOptions{CarryOver: true})
	bobGTC, _, _ := bob.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 90, 5, client.OrderOptions{CarryOver: true})
	_, _, _ = bob.PlaceOrder(ctx, "copper", protocol.SideBuy, 90, 5) // 1-round default order at same price!
	charlieTTL, _, _ := charlie.PlaceOrderWithOptions(ctx, "copper", protocol.SideBuy, 85, 4, client.OrderOptions{TTLRounds: 2})

	// Dave sells 7 units @ $90 -> fills Alice's 2 units (@$140) + 5 of Bob's 10 units (@$90)
	// Wait: let's also have Alice place a GTC sell order at $145 (which won't fill in R1 and will fall outside R2's band!)
	_, _, _ = alice.PlaceOrderWithOptions(ctx, "copper", protocol.SideSell, 145, 3, client.OrderOptions{CarryOver: true})
	_, _, _ = dave.PlaceOrder(ctx, "copper", protocol.SideSell, 90, 7)

	_, _ = http.Post(srv.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil))

	// Check Bob after Round 1: 5 units filled, and his GTC order (bobGTC.OrderID) MUST still be open with 5 units!
	pBobR1, _ := bob.GetPortfolio(ctx, "")
	if len(pBobR1.OpenOrders) != 1 || pBobR1.OpenOrders[0].OrderID != bobGTC.OrderID || pBobR1.OpenOrders[0].Qty != 5 {
		t.Fatalf("expected Bob's expiring 1-round order to be consumed first and GTC order %s (qty=5) to survive, got %+v",
			bobGTC.OrderID, pBobR1.OpenOrders)
	}
	t.Logf("[Edge Case 1 Verified] Equal-price fill consumed 1-round order first, preserving GTC order %s", bobGTC.OrderID)

	// =========================================================================
	// Edge Case 3 (Round 2):
	// - Cross-Round Self-Cross Prevention:
	//   Bob has a carried-over Buy @ $90 from Round 1. If Bob tries to Sell @ $90 or $85 in Round 2 -> SELF_CROSS!
	// - Replacing a Carried-Over TTL Order Must NOT Reset Its Remaining TTL:
	//   Charlie replaces his carried-over TTL=2 order from $85 to $86 in Round 2.
	//   Since 1 round already elapsed, its remaining TTL in Round 2 is 1 (so it MUST expire after Round 2!).
	// - Dynamic Reference Price Shift Auto-Expires Out-of-Band Carried-Over Orders:
	//   Copper reference price drops from $100 to $80 in Round 2 -> new band is [$40, $120].
	//   Alice's carried-over Sell @ $145 is now > $120, so at Round 2 seal it MUST auto-expire and refund 3 copper!
	// =========================================================================
	if _, rej, _ := bob.PlaceOrder(ctx, "copper", protocol.SideSell, 88, 1); rej == nil || rej.Code != protocol.ErrCodeSelfCross {
		t.Fatalf("expected SELF_CROSS against carried-over buy order @ $90, got %+v", rej)
	}
	t.Logf("[Edge Case 2 Verified] Cross-round SELF_CROSS against carried-over order blocked")

	if _, rej, err := charlie.ReplaceOrder(ctx, charlieTTL.OrderID, 86, 4); err != nil || rej != nil {
		t.Fatalf("Charlie ReplaceOrder on carried-over TTL order failed: err=%v rej=%+v", err, rej)
	}

	// Shift copper reference price to $80 (band [$40, $120]) before Round 2 clears
	gm.SetRefPrice("copper", 80)

	// Settle Round 2 (no sellers in Round 2)
	_, _ = http.Post(srv.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil))

	// Verify Alice's carried-over Sell 3 @ $145 was auto-expired by the new [$40, $120] price band and 3 copper unlocked!
	pAliceR2, _ := alice.GetPortfolio(ctx, "")
	if len(pAliceR2.OpenOrders) != 0 || pAliceR2.Locked["copper"] != 0 || pAliceR2.Available["copper"] != 102 {
		t.Fatalf("expected Alice's out-of-band carried-over order ($145 > $120) to auto-expire and unlock 3 copper, got %+v", pAliceR2)
	}
	t.Logf("[Edge Case 3 Verified] Carried-over order outside new reference price band [$40, $120] auto-expired & refunded")

	// Verify Charlie's replaced TTL=2 order expired at the end of Round 2 (did NOT reset TTL to 2!)
	pCharlieR2, _ := charlie.GetPortfolio(ctx, "")
	if len(pCharlieR2.OpenOrders) != 0 || pCharlieR2.Locked["usd"] != 0 || pCharlieR2.Available["usd"] != 10000 {
		t.Fatalf("expected Charlie's replaced TTL=2 order to expire after Round 2 without resetting TTL, got %+v", pCharlieR2)
	}
	t.Logf("[Edge Case 4 Verified] ReplaceOrder on carried-over TTL order preserved remaining TTL without reset")

	// =========================================================================
	// Edge Case 5 (Round 3):
	// - Multi-Asset Trading ("copper" + "iron") with Per-Asset Circuit Breaker ("scope": "asset:copper")
	//   * Bob still has his carried-over copper Buy 5 @ $90 ($450 locked)
	//   * Alice & Dave place "iron" orders @ $200 (5 units)
	//   * Admin halts ONLY "copper" ("asset:copper") -> Bob's copper escrow is released,
	//     while "iron" orders stay open and clear normally in Round 3!
	// =========================================================================
	_, _, _ = alice.PlaceOrder(ctx, "iron", protocol.SideBuy, 200, 5)
	_, _, _ = dave.PlaceOrder(ctx, "iron", protocol.SideSell, 190, 5)

	haltCopperPayload, _ := json.Marshal(protocol.AdminHaltPayload{
		Scope:  "asset:copper",
		Reason: "copper oracle maintenance",
	})
	haltEnv := protocol.Envelope{
		Version:   "1.0",
		GameID:    gm.m.GameID,
		From:      srvKey.AgentID(),
		To:        "server",
		Type:      protocol.TypeAdminHalt,
		Timestamp: time.Now().UTC().Unix(),
		Nonce:     "halt-copper-1",
		CorrID:    "c-halt-copper-1",
		Payload:   haltCopperPayload,
	}
	_ = protocol.SignEnvelope(&haltEnv, srvKey)
	haltBytes, _ := json.Marshal(haltEnv)
	if _, rej := eng.SubmitRawEnvelope(ctx, haltBytes); rej != nil {
		t.Fatalf("admin.halt asset:copper failed: %+v", rej)
	}

	// Bob's carried-over copper order must be immediately released, while iron orders clear in Round 3!
	pBobAfterCopperHalt, _ := bob.GetPortfolio(ctx, "")
	if len(pBobAfterCopperHalt.OpenOrders) != 0 || pBobAfterCopperHalt.Locked["usd"] != 0 {
		t.Fatalf("expected asset:copper halt to release Bob's copper order, got %+v", pBobAfterCopperHalt)
	}

	_, _ = http.Post(srv.URL+"/v1/sim/clear", "application/json", bytes.NewReader(nil))

	ironCandles, err := alice.GetCandles(ctx, "iron", 5)
	if err != nil || len(ironCandles) == 0 || ironCandles[len(ironCandles)-1].Volume != 5 {
		t.Fatalf("expected iron to clear 5 units while copper was paused, got %+v err=%v", ironCandles, err)
	}
	t.Logf("[Edge Case 5 Verified] Per-asset halt ('asset:copper') released copper escrow while 'iron' cleared 5 units normally")

	// Final invariant check: full ledger replay & DoctorCheck must match live engine state_hash
	replayed, err := ledger.ReplayAndVerify(&gm.m, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify failed: %v", err)
	}
	if err := replayed.DoctorCheck(); err != nil {
		t.Fatalf("DoctorCheck failed: %v", err)
	}
	hLive, _ := eng.StateHash()
	hReplayed, _ := replayed.StateHash()
	if hLive != hReplayed {
		t.Fatalf("state_hash mismatch: live=%s vs replayed=%s", hLive, hReplayed)
	}
}
