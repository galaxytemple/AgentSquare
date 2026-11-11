package engine

import (
	"context"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/board"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
)

func TestEdgeCasesOverflowKeyRotateCarryOverAdminAndBoard(t *testing.T) {
	ctx := context.Background()
	srvSeed := make([]byte, 32)
	srvSeed[0] = 1
	srvKey, _ := protocol.NewSignerFromSeed(srvSeed)

	// Start 10 minutes before round 1 cutoff (00:50:00Z, cutoff at 01:00:00Z)
	t0 := time.Date(2026, 11, 9, 0, 50, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "edge-case-game",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: true},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "depth"},
			FeeBps:            0,
			Board: &manifest.BoardConfig{
				Enabled:                  true,
				MaxPostsPerAgentPerRound: 2,
				MaxBodyChars:             100,
				MinIntervalSec:           10,
				RequireParticipant:       true,
				BlackoutBeforeCutoffSec:  300,
				AllowedContent:           "text/plain",
			},
			Admission:    "open",
			Ledger:       manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey: srvKey.PublicKeyString(),
			AdminPubKeys: []string{srvKey.PublicKeyString()},
		},
	}

	eng, err := New(gm, WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	seedA := make([]byte, 32)
	seedA[0] = 10
	agA, _ := protocol.NewSignerFromSeed(seedA)

	seedB := make([]byte, 32)
	seedB[0] = 20
	agB, _ := protocol.NewSignerFromSeed(seedB)

	ts := clk.Now().Unix()

	// Register A and B
	for _, ag := range []*protocol.MemorySigner{agA, agB} {
		raw := makeSignedEnv(t, ag, "edge-case-game", protocol.TypeAgentRegister, "reg", "c-reg", ts, protocol.RegisterPayload{
			PubKey: ag.PublicKeyString(),
		})
		if _, rej := eng.SubmitRawEnvelope(ctx, raw); rej != nil {
			t.Fatalf("register failed: %+v", rej)
		}
	}

	// 1. Edge Case: Integer multiplication overflow (limit_price * qty > 2^53 - 1)
	// Both 100000000 and 100000000 are < 2^53-1 individually, but product 10^16 > 9.007*10^15 (2^53-1)
	overflowRaw := makeSignedEnv(t, agA, "edge-case-game", protocol.TypePlaceOrder, "n-ovf", "c-ovf", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100000000, Qty: 100000000,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, overflowRaw); rej == nil || rej.Code != protocol.ErrCodeOutOfBand {
		t.Fatalf("expected OUT_OF_BAND for limit_price*qty > 2^53-1, got %+v", rej)
	}

	// 2. Edge Case: Key Rotation (key.rotate)
	newSeedA := make([]byte, 32)
	newSeedA[0] = 101
	newAgA, _ := protocol.NewSignerFromSeed(newSeedA)
	rotProof, _ := newAgA.Sign([]byte("rotate|" + agA.AgentID() + "|" + newAgA.PublicKeyString()))

	rotEnv := protocol.Envelope{
		Version:   "1.0",
		GameID:    "edge-case-game",
		From:      agA.AgentID(),
		To:        "server",
		Type:      protocol.TypeKeyRotate,
		Timestamp: ts,
		Nonce:     "n-rot",
		CorrID:    "c-rot",
	}
	rotEnv.Payload, _ = protocol.MarshalCanonical(protocol.KeyRotatePayload{
		OldPubKey: agA.PublicKeyString(),
		NewPubKey: newAgA.PublicKeyString(),
		NewSig:    rotProof,
	})
	_ = protocol.SignEnvelope(&rotEnv, agA)
	rotBytes, _ := protocol.MarshalCanonical(rotEnv)
	if _, rej := eng.SubmitRawEnvelope(ctx, rotBytes); rej != nil {
		t.Fatalf("key.rotate failed: %+v", rej)
	}

	// Signing with old key must now fail with INVALID_SIGNATURE
	oldKeyOrder := makeSignedEnv(t, agA, "edge-case-game", protocol.TypePlaceOrder, "n-old", "c-old", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 1,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, oldKeyOrder); rej == nil || rej.Code != protocol.ErrCodeInvalidSignature {
		t.Fatalf("expected old key after rotation to fail with INVALID_SIGNATURE, got %+v", rej)
	}

	// Signing with new key (keeping original AgentID!) must succeed
	newKeyEnv := protocol.Envelope{
		Version:   "1.0",
		GameID:    "edge-case-game",
		From:      agA.AgentID(),
		To:        "server",
		Type:      protocol.TypePlaceOrder,
		Timestamp: ts,
		Nonce:     "n-newkey-buy",
		CorrID:    "c-newkey-buy",
	}
	newKeyEnv.Payload, _ = protocol.MarshalCanonical(protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 10, Qty: 6,
	})
	_ = protocol.SignEnvelope(&newKeyEnv, newAgA)
	newKeyBytes, _ := protocol.MarshalCanonical(newKeyEnv)
	if _, rej := eng.SubmitRawEnvelope(ctx, newKeyBytes); rej != nil {
		t.Fatalf("order signed with rotated new key failed: %+v", rej)
	}

	// 3. Edge Case: Board validation (URL/markdown rejection, rate limit, blackout window, retract)
	urlPost := makeSignedEnv(t, agB, "edge-case-game", protocol.TypeBoardPost, "bp-url", "c-bp", ts, protocol.BoardPostPayload{
		Asset: "copper", Stance: "bull", Body: "Check https://malicious.example now!",
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, urlPost); rej == nil || rej.Code != protocol.ErrCodeBoardTooLong {
		t.Fatalf("expected URL in board post to be rejected, got %+v", rej)
	}

	validPost := makeSignedEnv(t, agB, "edge-case-game", protocol.TypeBoardPost, "bp-1", "c-bp1", ts, protocol.BoardPostPayload{
		Asset: "copper", Stance: "bull", Body: "Copper demand looks strong",
	})
	postRec, rej := eng.SubmitRawEnvelope(ctx, validPost)
	if rej != nil {
		t.Fatalf("valid board post failed: %+v", rej)
	}

	// Immediate second post within MinIntervalSec (10s) must fail with BOARD_RATE_LIMIT
	fastPost := makeSignedEnv(t, agB, "edge-case-game", protocol.TypeBoardPost, "bp-2", "c-bp2", ts, protocol.BoardPostPayload{
		Asset: "copper", Stance: "bull", Body: "Posting too fast",
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, fastPost); rej == nil || rej.Code != protocol.ErrCodeBoardRateLimit {
		t.Fatalf("expected BOARD_RATE_LIMIT on min_interval_sec, got %+v", rej)
	}

	// Retract post
	retractRaw := makeSignedEnv(t, agB, "edge-case-game", protocol.TypeBoardRetract, "bp-ret", "c-ret", ts, protocol.BoardRetractPayload{
		PostID: postRec.PostID,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, retractRaw); rej != nil {
		t.Fatalf("board.retract failed: %+v", rej)
	}
	posts := eng.BoardPosts("copper", 0, 10)
	if len(posts) != 1 || posts[0].UntrustedText != "[retracted]" {
		t.Fatalf("expected retracted post text to be masked, got %+v", posts)
	}

	// Advance clock to 00:58:00Z (2 minutes before cutoff < BlackoutBeforeCutoffSec 300s)
	clk.Set(time.Date(2026, 11, 9, 0, 58, 0, 0, time.UTC))
	tsBlackout := clk.Now().Unix()
	blackoutPost := makeSignedEnv(t, agB, "edge-case-game", protocol.TypeBoardPost, "bp-blk", "c-blk", tsBlackout, protocol.BoardPostPayload{
		Asset: "copper", Stance: "bear", Body: "Late post inside blackout window",
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, blackoutPost); rej == nil || rej.Code != protocol.ErrCodeBoardBlackout {
		t.Fatalf("expected BOARD_BLACKOUT, got %+v", rej)
	}

	// 4. Edge Case: CarryOver: true with partial fill
	// A bought 6 @ $10; now B sells only 2 @ $8 -> 2 units clear, and A's remaining 4 units carry over to Round 2!
	sellPartial := makeSignedEnv(t, agB, "edge-case-game", protocol.TypePlaceOrder, "s-part", "c-sp", tsBlackout, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 2,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, sellPartial); rej != nil {
		t.Fatalf("sellPartial failed: %+v", rej)
	}

	// Check Depth book (since pre_clear == "depth")
	depth, err := eng.Depth("copper")
	if err != nil || len(depth.Bids) != 1 || len(depth.Asks) != 1 {
		t.Fatalf("unexpected depth book: %+v err=%v", depth, err)
	}

	fills1, err := eng.SealAndClearRound(ctx)
	if err != nil || len(fills1) != 2 {
		t.Fatalf("SealAndClearRound 1: fills=%+v err=%v", fills1, err)
	}
	// Verify A's remaining 4 units carried over into Round 2
	snapAfterR1 := eng.StateSnapshot()
	if len(snapAfterR1.OpenOrders) != 1 {
		t.Fatalf("expected 1 carried-over open order after Round 1, got %d", len(snapAfterR1.OpenOrders))
	}

	// 5. Edge Case: Snapshot Checkpoint & Custom Game Balance Batch & Ext Record
	if err := eng.CreditOrDebitBatch(ctx, []struct {
		AgentID string
		Asset   string
		Delta   int64
		Reason  string
	}{
		{AgentID: agB.AgentID(), Asset: "usd", Delta: -10, Reason: "shop_purchase"},
		{AgentID: agB.AgentID(), Asset: "copper", Delta: 1, Reason: "shop_purchase"},
	}); err != nil {
		t.Fatalf("CreditOrDebitBatch: %v", err)
	}
	if _, err := eng.AppendExt(ctx, "city.score", map[string]any{"agent_id": agB.AgentID(), "score": 1200}); err != nil {
		t.Fatalf("AppendExt: %v", err)
	}
	if _, err := eng.AppendCheckpoint(ctx); err != nil {
		t.Fatalf("AppendCheckpoint: %v", err)
	}

	// 6. Edge Case: Admin Halt voids round and releases carried-over escrow locks
	haltRaw := makeSignedEnv(t, srvKey, "edge-case-game", protocol.TypeAdminHalt, "adm-h1", "c-h1", tsBlackout, protocol.AdminHaltPayload{
		Scope: "all", Reason: "maintenance",
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, haltRaw); rej != nil {
		t.Fatalf("admin.halt failed: %+v", rej)
	}
	snapHalted := eng.StateSnapshot()
	if !snapHalted.HaltedGlobal || len(snapHalted.OpenOrders) != 0 {
		t.Fatalf("expected global halt to void round and release open orders, got %+v", snapHalted)
	}

	// Order or cancel during halt must return HALTED
	duringHalt := makeSignedEnv(t, agB, "edge-case-game", protocol.TypePlaceOrder, "s-halt", "c-sh", tsBlackout, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 1,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, duringHalt); rej == nil || rej.Code != protocol.ErrCodeHalted {
		t.Fatalf("expected HALTED rejection during halt, got %+v", rej)
	}

	// Admin Resume
	resumeRaw := makeSignedEnv(t, srvKey, "edge-case-game", protocol.TypeAdminResume, "adm-r1", "c-r1", tsBlackout, protocol.AdminResumePayload{
		Scope: "all", Reason: "done",
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, resumeRaw); rej != nil {
		t.Fatalf("admin.resume failed: %+v", rej)
	}

	// Final full replay & doctor check from genesis
	replayed, err := ledger.ReplayAndVerify(&gm.manifest, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify after all edge cases failed: %v", err)
	}
	h1, _ := eng.StateHash()
	h2, _ := replayed.StateHash()
	if h1 != h2 {
		t.Fatalf("state_hash mismatch after edge cases: %s vs %s", h1, h2)
	}
}

func TestUnitEngineCarriedOverAggregationWithTTLAndImmediateExpiry(t *testing.T) {
	ctx := context.Background()
	srvKey, _ := protocol.GenerateKeyPair(nil)
	t0 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "unit-engine-ttl-agg",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-10T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "full"},
			ServerPubKey:      srvKey.PublicKeyString(),
		},
	}

	eng, err := New(gm, WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	agA, _ := protocol.GenerateKeyPair(nil)
	agB, _ := protocol.GenerateKeyPair(nil)
	agS, _ := protocol.GenerateKeyPair(nil)

	ts := clk.Now().Unix()
	for _, ag := range []*protocol.MemorySigner{agA, agB, agS} {
		raw := makeSignedEnv(t, ag, gm.manifest.GameID, protocol.TypeAgentRegister, "reg", "c-reg", ts, protocol.RegisterPayload{
			PubKey: ag.PublicKeyString(),
		})
		if _, rej := eng.SubmitRawEnvelope(ctx, raw); rej != nil {
			t.Fatalf("register failed: %+v", rej)
		}
	}

	// =========================================================================
	// Round 1:
	// - agA places:
	//   * Buy 2 @ $9 (CarryOver=true, GTC)
	//   * Buy 2 @ $9 (TTLRounds=2, survives R1 -> expires after R2)
	//   * Buy 2 @ $9 (CarryOver=false, immediate expiry after R1!)
	// - agB places:
	//   * Buy 3 @ $9 (TTLRounds=3, survives R1 & R2 -> expires after R3)
	// - agS sells only 2 units @ $9 in Round 1!
	//
	// In Round 1 Clearing:
	// - Total demand at $9: agA has 2+2+2 = 6 units, agB has 3 units (total 9 units).
	// - Supply at $9: agS has 2 units -> Round-Robin gives 1 unit to agA and 1 unit to agB!
	// - For agA (filled 1 unit out of 6):
	//   * The 1 filled unit MUST be consumed from agA's immediate-expiry order (CarryOver=false)!
	//   * Then the remaining 1 unfilled unit of agA's immediate-expiry order expires at Round 1 settle!
	//   * Both agA's GTC order (2 units) and TTL=2 order (2 units) survive 100% intact (4 units total) into Round 2!
	// - For agB (filled 1 unit out of 3):
	//   * 2 units remain on agB's TTL=3 order and carry over into Round 2!
	// =========================================================================
	recAGTC, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "a-gtc", "c1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 2, CarryOver: true,
	}))
	if rej != nil {
		t.Fatalf("a-gtc failed: %+v", rej)
	}
	recATTL2, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "a-ttl2", "c2", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 2, TTLRounds: 2,
	}))
	if rej != nil {
		t.Fatalf("a-ttl2 failed: %+v", rej)
	}
	_, rej = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "a-exp1", "c3", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 2, CarryOver: false,
	}))
	if rej != nil {
		t.Fatalf("a-exp1 failed: %+v", rej)
	}
	recBTTL3, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agB, gm.manifest.GameID, protocol.TypePlaceOrder, "b-ttl3", "c4", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 3, TTLRounds: 3,
	}))
	if rej != nil {
		t.Fatalf("b-ttl3 failed: %+v", rej)
	}
	_, rej = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agS, gm.manifest.GameID, protocol.TypePlaceOrder, "s-r1", "c5", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 9, Qty: 2,
	}))
	if rej != nil {
		t.Fatalf("s-r1 failed: %+v", rej)
	}

	fills1, err := eng.SealAndClearRound(ctx)
	if err != nil || len(fills1) != 3 { // agA(1), agB(1), agS(2)
		t.Fatalf("Round 1 clear: fills=%+v err=%v", fills1, err)
	}

	snap1 := eng.StateSnapshot()
	if snap1.OpenOrders[recAGTC.OrderID] == nil || snap1.OpenOrders[recAGTC.OrderID].Qty != 2 {
		t.Fatalf("expected agA GTC order to survive R1 with qty=2, got %+v", snap1.OpenOrders[recAGTC.OrderID])
	}
	if snap1.OpenOrders[recATTL2.OrderID] == nil || snap1.OpenOrders[recATTL2.OrderID].Qty != 2 {
		t.Fatalf("expected agA TTL=2 order to survive R1 with qty=2, got %+v", snap1.OpenOrders[recATTL2.OrderID])
	}
	if snap1.OpenOrders[recBTTL3.OrderID] == nil || snap1.OpenOrders[recBTTL3.OrderID].Qty != 2 {
		t.Fatalf("expected agB TTL=3 order to survive R1 with qty=2, got %+v", snap1.OpenOrders[recBTTL3.OrderID])
	}
	if len(snap1.OpenOrders) != 3 {
		t.Fatalf("expected exactly 3 open orders after R1, got %d", len(snap1.OpenOrders))
	}

	// =========================================================================
	// Round 2:
	// - agA adds a NEW 1-round order: Buy 2 @ $9 (CarryOver=false)
	//   Now agA has:
	//   * recATTL2 (from R1, TTL=2 -> remLife=1 in R2, qty=2)
	//   * new R2 order (CarryOver=false -> remLife=1 in R2, qty=2)
	//   * recAGTC (from R1, CarryOver=true -> remLife=∞, qty=2)
	//   Total agA demand at $9 = 6 units; agB has 2 units (from recBTTL3).
	// - agS sells 2 units @ $9 -> agA gets 1 unit, agB gets 1 unit.
	// - For agA:
	//   * 1 unit is consumed from recATTL2 (leaving 1 unit on recATTL2 and 2 units on the new R2 order).
	//   * At Round 2 settle, BOTH recATTL2 (2nd round reached!) and the new R2 immediate-expiry order EXPIRE!
	//   * ONLY recAGTC (qty=2) survives into Round 3 for agA!
	// - For agB:
	//   * 1 unit is consumed from recBTTL3, leaving 1 unit on recBTTL3 (which survives into Round 3 since 2 < 3!).
	// =========================================================================
	_, rej = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "a-exp2", "c6", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 2, CarryOver: false,
	}))
	if rej != nil {
		t.Fatalf("a-exp2 failed: %+v", rej)
	}
	_, rej = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agS, gm.manifest.GameID, protocol.TypePlaceOrder, "s-r2", "c7", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 9, Qty: 2,
	}))
	if rej != nil {
		t.Fatalf("s-r2 failed: %+v", rej)
	}

	fills2, err := eng.SealAndClearRound(ctx)
	if err != nil || len(fills2) != 3 {
		t.Fatalf("Round 2 clear: fills=%+v err=%v", fills2, err)
	}

	snap2 := eng.StateSnapshot()
	if len(snap2.OpenOrders) != 2 {
		t.Fatalf("expected 2 open orders after R2 (recAGTC and recBTTL3), got %d: %+v", len(snap2.OpenOrders), snap2.OpenOrders)
	}
	if snap2.OpenOrders[recAGTC.OrderID] == nil || snap2.OpenOrders[recAGTC.OrderID].Qty != 2 {
		t.Fatalf("expected agA GTC order to still have qty=2 after R2, got %+v", snap2.OpenOrders[recAGTC.OrderID])
	}
	if snap2.OpenOrders[recBTTL3.OrderID] == nil || snap2.OpenOrders[recBTTL3.OrderID].Qty != 1 {
		t.Fatalf("expected agB TTL=3 order to have qty=1 after R2, got %+v", snap2.OpenOrders[recBTTL3.OrderID])
	}

	// =========================================================================
	// Round 3: No sellers -> Round 3 settles with 0 fills.
	// - agB's recBTTL3 has now completed its 3rd round (roundsElapsed = 3 == TTLRounds) -> MUST expire!
	// - ONLY agA's recAGTC (qty=2) remains open entering Round 4!
	// =========================================================================
	if _, err := eng.SealAndClearRound(ctx); err != nil {
		t.Fatalf("Round 3 clear: %v", err)
	}
	snap3 := eng.StateSnapshot()
	if len(snap3.OpenOrders) != 1 || snap3.OpenOrders[recAGTC.OrderID] == nil || snap3.OpenOrders[recAGTC.OrderID].Qty != 2 {
		t.Fatalf("expected only agA GTC order (qty=2) to survive after Round 3, got %+v", snap3.OpenOrders)
	}

	// Verify deterministic ledger replay & DoctorCheck
	replayed, err := ledger.ReplayAndVerify(&gm.manifest, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify failed: %v", err)
	}
	if err := replayed.DoctorCheck(); err != nil {
		t.Fatalf("DoctorCheck failed: %v", err)
	}
	hLive, _ := eng.StateHash()
	hRep, _ := replayed.StateHash()
	if hLive != hRep {
		t.Fatalf("state_hash mismatch: %s vs %s", hLive, hRep)
	}
}

func TestUnitMarketOrdersEscrowCheckPriorityAndRefund(t *testing.T) {
	ctx := context.Background()
	srvKey, _ := protocol.GenerateKeyPair(nil)
	clk := sim.NewFakeClock(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC))

	// refPrice=10, LowerPct=50, UpperPct=150 -> Band is [$5, $15]
	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "unit-market-order",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-10T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "full"},
			ServerPubKey:      srvKey.PublicKeyString(),
		},
	}

	eng, err := New(gm, WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	mktBuyer, _ := protocol.GenerateKeyPair(nil)
	limBuyer, _ := protocol.GenerateKeyPair(nil)
	mktSeller, _ := protocol.GenerateKeyPair(nil)
	limSeller, _ := protocol.GenerateKeyPair(nil)

	ts := clk.Now().Unix()
	for _, ag := range []*protocol.MemorySigner{mktBuyer, limBuyer, mktSeller, limSeller} {
		raw := makeSignedEnv(t, ag, gm.manifest.GameID, protocol.TypeAgentRegister, "reg", "c-reg", ts, protocol.RegisterPayload{
			PubKey: ag.PublicKeyString(),
		})
		if _, rej := eng.SubmitRawEnvelope(ctx, raw); rej != nil {
			t.Fatalf("register failed: %+v", rej)
		}
	}

	// 1. mktBuyer has $100 USD. Band max is $15.
	//    Trying to Market Buy 7 units requires 7 * $15 = $105 > $100 -> MUST reject with INSUFFICIENT_BALANCE!
	_, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, mktBuyer, gm.manifest.GameID, protocol.TypePlaceOrder, "mb-too-big", "c1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, Qty: 7, Market: true,
	}))
	if rej == nil || rej.Code != protocol.ErrCodeInsufficientBalance {
		t.Fatalf("expected Market Buy 7 (requiring $105 > $100) to fail with INSUFFICIENT_BALANCE, got %+v", rej)
	}

	// 2. Market Buy 4 units requires 4 * $15 = $60 <= $100 -> ACCEPTED ($60 locked, $40 available)
	_, rej = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, mktBuyer, gm.manifest.GameID, protocol.TypePlaceOrder, "mb-ok", "c2", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, Qty: 4, Market: true,
	}))
	if rej != nil {
		t.Fatalf("expected Market Buy 4 ($60 escrow) to succeed, got %+v", rej)
	}
	snapPre := eng.StateSnapshot()
	if snapPre.Agents[mktBuyer.AgentID()].Locked["usd"] != 60 || snapPre.Agents[mktBuyer.AgentID()].Available["usd"] != 40 {
		t.Fatalf("expected mktBuyer locked=$60 avail=$40, got %+v", snapPre.Agents[mktBuyer.AgentID()])
	}

	// 3. limBuyer bids $9 for 4 units; mktSeller Market Sells 1 unit (pegged to $5); limSeller sells 5 units @ $8.
	//    Total supply = 6 units (1@$5 + 5@$8); Total demand at $9 = 8 units (4@$15 + 4@$9).
	//    ClearingPrice = $9, Volume = 6:
	//    - mktBuyer gets 100% priority (4/4 filled at $9 -> pays $36, gets $24 excess escrow refunded immediately -> avail=$64!)
	//    - limBuyer gets remaining 2/4 units at $9
	//    - mktSeller gets 100% priority (1/1 filled at $9 -> receives $9 instead of $5!)
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, limBuyer, gm.manifest.GameID, protocol.TypePlaceOrder, "lb-1", "c3", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 4,
	}))
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, mktSeller, gm.manifest.GameID, protocol.TypePlaceOrder, "ms-1", "c4", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, Qty: 1, Market: true,
	}))
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, limSeller, gm.manifest.GameID, protocol.TypePlaceOrder, "ls-1", "c5", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 5,
	}))

	fills, err := eng.SealAndClearRound(ctx)
	if err != nil || len(fills) != 4 {
		t.Fatalf("SealAndClearRound failed: fills=%+v err=%v", fills, err)
	}

	snapPost := eng.StateSnapshot()
	// mktBuyer: started with $100, bought 4 copper @ $9 ($36 paid), $24 refunded -> Available USD = $64, Copper = 14!
	if snapPost.Agents[mktBuyer.AgentID()].Available["usd"] != 64 || snapPost.Agents[mktBuyer.AgentID()].Available["copper"] != 14 {
		t.Fatalf("expected mktBuyer avail_usd=64 copper=14 after $24 refund, got %+v", snapPost.Agents[mktBuyer.AgentID()])
	}
	// mktSeller: sold 1 copper at clearing price $9 -> Available USD = $109, Copper = 9!
	if snapPost.Agents[mktSeller.AgentID()].Available["usd"] != 109 || snapPost.Agents[mktSeller.AgentID()].Available["copper"] != 9 {
		t.Fatalf("expected mktSeller avail_usd=109 copper=9, got %+v", snapPost.Agents[mktSeller.AgentID()])
	}
}

type noopAdmitGame struct {
	testGame
}

func (g *noopAdmitGame) Admit(_ context.Context, _ AdmissionRequest) ([]BalanceChange, error) {
	// Return empty so Engine falls back to manifest.InitialBalances
	return nil, nil
}

func TestUnitInitialBalancesAndWeeklyAdminReset(t *testing.T) {
	ctx := context.Background()
	srvKey, _ := protocol.GenerateKeyPair(nil)
	t0 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &noopAdmitGame{
		testGame: testGame{
			refPrice: 10,
			manifest: manifest.Manifest{
				Protocol:          protocol.ProtocolID,
				SupportedVersions: []string{"1.0"},
				GameID:            "weekly-season-game",
				Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
				SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
				InitialBalances:   map[string]int64{"usd": 500, "copper": 20},
				Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-10T00:00:00Z", CarryOver: false},
				PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
				MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
				Admission:         "open",
				Ledger:            manifest.LedgerConfig{Public: "full"},
				ServerPubKey:      srvKey.PublicKeyString(),
				AdminPubKeys:      []string{srvKey.PublicKeyString()},
			},
		},
	}

	eng, err := New(gm, WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	agA, _ := protocol.GenerateKeyPair(nil)
	agB, _ := protocol.GenerateKeyPair(nil)
	ts := clk.Now().Unix()

	// 1. Register agA and agB -> automatically receive manifest.InitialBalances (usd=500, copper=20)
	for _, ag := range []*protocol.MemorySigner{agA, agB} {
		raw := makeSignedEnv(t, ag, gm.manifest.GameID, protocol.TypeAgentRegister, "reg", "c-reg", ts, protocol.RegisterPayload{
			PubKey: ag.PublicKeyString(),
		})
		if _, rej := eng.SubmitRawEnvelope(ctx, raw); rej != nil {
			t.Fatalf("register failed: %+v", rej)
		}
	}
	snapInit := eng.StateSnapshot()
	if snapInit.Agents[agA.AgentID()].Available["usd"] != 500 || snapInit.Agents[agA.AgentID()].Available["copper"] != 20 {
		t.Fatalf("expected agA initial grant usd=500 copper=20, got %+v", snapInit.Agents[agA.AgentID()].Available)
	}

	// 2. Trade & place a GTC order so balances change and escrow is locked
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "b1", "cb1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 10, Qty: 10, CarryOver: true,
	}))
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agB, gm.manifest.GameID, protocol.TypePlaceOrder, "s1", "cs1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 10, Qty: 4,
	}))
	if _, err := eng.SealAndClearRound(ctx); err != nil {
		t.Fatalf("SealAndClearRound: %v", err)
	}
	snapMid := eng.StateSnapshot()
	// agA bought 4 @ $10 and still has 6 @ $10 GTC locked ($60 locked)
	if len(snapMid.OpenOrders) != 1 || snapMid.Agents[agA.AgentID()].Locked["usd"] != 60 {
		t.Fatalf("expected 1 carried-over GTC order and $60 locked, got orders=%d locked=%d", len(snapMid.OpenOrders), snapMid.Agents[agA.AgentID()].Locked["usd"])
	}

	// 3. Non-admin agent attempting admin.reset MUST be rejected
	hackRaw := makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypeAdminReset, "hack-1", "ch1", ts, protocol.AdminResetPayload{
		SeasonID: "hack", Reason: "steal", InitialBalances: map[string]int64{"usd": 1000000},
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, hackRaw); rej == nil {
		t.Fatalf("expected non-admin admin.reset to be rejected")
	}

	// 4. Admin executes weekly reset (falls back to manifest.InitialBalances when InitialBalances omitted)
	resetRaw := makeSignedEnv(t, srvKey, gm.manifest.GameID, protocol.TypeAdminReset, "adm-rst-1", "cr1", ts, protocol.AdminResetPayload{
		SeasonID:     "2026-W46",
		Reason:       "weekly economy reset",
		ClearHistory: true,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, resetRaw); rej != nil {
		t.Fatalf("admin.reset failed: %+v", rej)
	}

	snapAfter := eng.StateSnapshot()
	if len(snapAfter.OpenOrders) != 0 {
		t.Fatalf("expected all open/GTC orders wiped after admin.reset, got %d", len(snapAfter.OpenOrders))
	}
	for _, id := range []string{agA.AgentID(), agB.AgentID()} {
		ag := snapAfter.Agents[id]
		if len(ag.Locked) != 0 || ag.Available["usd"] != 500 || ag.Available["copper"] != 20 {
			t.Fatalf("expected agent %s reset to locked=0, avail=(500 usd, 20 copper), got avail=%+v locked=%+v", id, ag.Available, ag.Locked)
		}
	}

	// 5. Verify full deterministic replay from genesis matches engine state hash
	replayed, err := ledger.ReplayAndVerify(&gm.manifest, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify after admin.reset failed: %v", err)
	}
	h1, _ := eng.StateHash()
	h2, _ := replayed.StateHash()
	if h1 != h2 {
		t.Fatalf("StateHash mismatch after admin.reset: %s vs %s", h1, h2)
	}

	// 6. Verify Engine read helpers, SetRoundInterval, and Tick
	if err := eng.SetRoundInterval("PT1M"); err != nil {
		t.Fatalf("SetRoundInterval(PT1M) failed: %v", err)
	}
	_, startB, endB, _ := eng.CurrentRoundBounds()
	if endB.Sub(startB) != time.Minute {
		t.Fatalf("expected 1m round bounds after SetRoundInterval, got %v", endB.Sub(startB))
	}
	clk.Advance(endB.Sub(clk.Now()))
	settled, _, err := eng.Tick(ctx)
	if err != nil || !settled {
		t.Fatalf("expected Tick to settle round after 61s, settled=%v err=%v", settled, err)
	}
	_ = eng.Manifest()
	_ = eng.Tape("copper", 5)
	_ = eng.Candles("copper", 5)
	_ = eng.Stats("copper", 5)
	_, _ = eng.RoundAnatomy(1, "copper")
	_ = eng.TalkVsTrade(agA.AgentID(), "copper", 3)
}

type noopProj struct {
	seq uint64
}

func (p *noopProj) Apply(_ context.Context, rec ledger.Record) error {
	p.seq = rec.Seq
	return nil
}
func (p *noopProj) Checkpoint(_ context.Context) (uint64, error) { return p.seq, nil }

func TestUnitEngineOptionsReplaceModerateManifestUpdateAndLogReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvKey, _ := protocol.GenerateKeyPair(nil)
	t0 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "unit-engine-full-cov",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-10T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			Board:             &manifest.BoardConfig{Enabled: true, MaxPostsPerAgentPerRound: 5, MaxBodyChars: 280},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "full"},
			ServerPubKey:      srvKey.PublicKeyString(),
			AdminPubKeys:      []string{srvKey.PublicKeyString()},
		},
	}

	proj := &noopProj{}
	eng, err := New(
		gm,
		WithSigner(srvKey),
		WithClock(clk),
		WithProjection(proj),
		WithBoard(board.NewMemoryStore(), board.DefaultLimits()),
		WithTimestampTolerance(30*time.Second),
		WithStrictCutoff(true),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ch, unsub := eng.Subscribe()
	defer unsub()

	agA, _ := protocol.GenerateKeyPair(nil)
	ts := clk.Now().Unix()
	_, _ = eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypeAgentRegister, "r1", "cr1", ts, protocol.RegisterPayload{
		PubKey: agA.PublicKeyString(),
	}))

	// Place order with TTL=2, then replace_order
	ordRec, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypePlaceOrder, "o1", "co1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 8, Qty: 2, TTLRounds: 2,
	}))
	if rej != nil || ordRec == nil {
		t.Fatalf("place_order failed: %+v", rej)
	}
	select {
	case ev := <-ch:
		if ev.Type == "" {
			t.Fatalf("expected non-empty event type")
		}
	default:
		t.Fatalf("expected indicative_update event on subscriber channel")
	}

	repRec, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypeReplaceOrder, "rep1", "crep1", ts, protocol.ReplaceOrderPayload{
		OrderID: ordRec.OrderID, LimitPrice: 9, Qty: 3,
	}))
	if rej != nil || repRec == nil {
		t.Fatalf("replace_order failed: %+v", rej)
	}

	// Board post + admin.moderate + manifest.update (with dynamic interval PT10S for round 2)
	postRec, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, agA, gm.manifest.GameID, protocol.TypeBoardPost, "bp1", "cbp1", ts, protocol.BoardPostPayload{
		Asset: "copper", Stance: "bull", Body: "copper up",
	}))
	if rej != nil || postRec == nil {
		t.Fatalf("board.post failed: %+v", rej)
	}
	if _, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, srvKey, gm.manifest.GameID, protocol.TypeAdminModerate, "mod1", "cmod1", ts, protocol.AdminModeratePayload{
		PostID: postRec.PostID, Reason: "spam",
	})); rej != nil {
		t.Fatalf("admin.moderate failed: %+v", rej)
	}
	if _, rej := eng.SubmitRawEnvelope(ctx, makeSignedEnv(t, srvKey, gm.manifest.GameID, protocol.TypeManifestUpdate, "mu1", "cmu1", ts, protocol.ManifestUpdatePayload{
		NewHash: "sha256:0000000000000000000000000000000000000000000000000000000000000001", EffectiveRound: 2, Interval: "PT10S",
	})); rej != nil {
		t.Fatalf("manifest.update failed: %+v", rej)
	}

	// StartScheduler and advance fake clock past Round 1 cutoff (1h)
	schedCtx, schedCancel := context.WithCancel(ctx)
	eng.StartScheduler(schedCtx, 5*time.Millisecond)
	clk.Advance(61 * time.Minute)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if eng.StateSnapshot().CurrentRound >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	schedCancel()
	time.Sleep(15 * time.Millisecond)
	if eng.StateSnapshot().CurrentRound < 2 {
		t.Fatalf("expected StartScheduler to settle Round 1 and advance to Round 2")
	}

	// Re-open a second Engine from eng.Log() via WithLog to test the full startup replay loop in engine.New
	eng2, err := New(
		gm,
		WithLog(eng.Log()),
		WithSigner(srvKey),
		WithClock(clk),
		WithBoard(board.NewMemoryStore(), board.DefaultLimits()),
	)
	if err != nil {
		t.Fatalf("New from existing log failed: %v", err)
	}
	h1, _ := eng.StateHash()
	h2, _ := eng2.StateHash()
	if h1 != h2 {
		t.Fatalf("reopened engine StateHash mismatch: %s vs %s", h1, h2)
	}
	// Verify that moderated post remains hidden after engine restart
	postsAfterRestart := eng2.BoardPosts("copper", 0, 10)
	if len(postsAfterRestart) != 1 || postsAfterRestart[0].UntrustedText != "[retracted]" {
		t.Fatalf("expected moderated post to remain hidden after restart, got %+v", postsAfterRestart)
	}
	// Verify that dynamic round interval update from manifest.update (PT10S at Round 2) was replayed on eng2
	if eng2.Manifest().Round.Interval != "PT10S" {
		t.Fatalf("expected replayed engine manifest interval PT10S, got %s", eng2.Manifest().Round.Interval)
	}

	// Verify AppendCheckpoint saves snapshot to log and fast-bootstraps a 3rd engine
	if _, err := eng2.AppendCheckpoint(ctx); err != nil {
		t.Fatalf("AppendCheckpoint failed: %v", err)
	}
	snap, err := eng2.Log().Snapshot(ctx)
	if err != nil || snap.Seq == 0 || snap.State == nil {
		t.Fatalf("expected non-empty saved Snapshot after AppendCheckpoint, got %+v err=%v", snap, err)
	}
	eng3, err := New(gm, WithLog(eng2.Log()), WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("New from checkpointed log failed: %v", err)
	}
	h2After, _ := eng2.StateHash()
	h3, _ := eng3.StateHash()
	if h2After != h3 {
		t.Fatalf("checkpoint fast-bootstrap StateHash mismatch: %s vs %s", h2After, h3)
	}
}
