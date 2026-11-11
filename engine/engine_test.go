package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
)

type testGame struct {
	manifest manifest.Manifest
	refPrice int64
}

func (g *testGame) Manifest() manifest.Manifest { return g.manifest }
func (g *testGame) ReferencePrice(_ string, _ uint64) int64 {
	return g.refPrice
}
func (g *testGame) Admit(_ context.Context, _ AdmissionRequest) ([]BalanceChange, error) {
	return []BalanceChange{
		{Asset: "usd", Qty: 100, Reason: "deck_grant"},
		{Asset: "copper", Qty: 10, Reason: "deck_grant"},
	}, nil
}
func (g *testGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error {
	return nil
}

func makeSignedEnv(t *testing.T, signer protocol.Signer, gameID, msgType, nonce, corrID string, ts int64, payload any) []byte {
	t.Helper()
	rawP, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := protocol.Envelope{
		Version:   "1.0",
		GameID:    gameID,
		From:      signer.AgentID(),
		To:        "server",
		Type:      msgType,
		Timestamp: ts,
		Nonce:     nonce,
		CorrID:    corrID,
		Payload:   rawP,
	}
	if err := protocol.SignEnvelope(&env, signer); err != nil {
		t.Fatalf("SignEnvelope: %v", err)
	}
	b, _ := json.Marshal(env)
	return b
}

func TestEngineEndToEndLifecycle(t *testing.T) {
	ctx := context.Background()
	srvSeed := make([]byte, 32)
	srvSeed[0] = 1
	srvKey, _ := protocol.NewSignerFromSeed(srvSeed)

	t0 := time.Date(2026, 11, 9, 14, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "city-2026w46",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			FeeBps:            0,
			Board:             &manifest.BoardConfig{Enabled: true, MaxPostsPerAgentPerRound: 3, MaxBodyChars: 280, MinIntervalSec: 0, RequireParticipant: true},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey:      srvKey.PublicKeyString(),
			AdminPubKeys:      []string{srvKey.PublicKeyString()},
		},
	}

	eng, err := New(gm, WithSigner(srvKey), WithClock(clk))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	// Create 2 agents: Buyer (agA) and Seller (agB)
	seedA := make([]byte, 32)
	seedA[0] = 10
	agA, _ := protocol.NewSignerFromSeed(seedA)

	seedB := make([]byte, 32)
	seedB[0] = 20
	agB, _ := protocol.NewSignerFromSeed(seedB)

	ts := t0.Unix()

	// Register A and B
	for i, ag := range []*protocol.MemorySigner{agA, agB} {
		raw := makeSignedEnv(t, ag, "city-2026w46", protocol.TypeAgentRegister, "reg-1", "c-reg", ts, protocol.RegisterPayload{
			PubKey:      ag.PublicKeyString(),
			DisplayName: "agent",
		})
		rec, rej := eng.SubmitRawEnvelope(ctx, raw)
		if rej != nil || rec == nil {
			t.Fatalf("register agent %d failed: %+v", i, rej)
		}
		// Idempotent resend of the exact same envelope MUST return the exact same receipt
		rec2, rej2 := eng.SubmitRawEnvelope(ctx, raw)
		if rej2 != nil || rec2.ServerSig != rec.ServerSig {
			t.Fatalf("idempotent resend failed: rec2=%+v rej2=%+v", rec2, rej2)
		}
	}

	// Replayed nonce with different content must return REPLAYED_NONCE
	replayRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypePlaceOrder, "reg-1", "c-replay", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 1,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, replayRaw); rej == nil || rej.Code != protocol.ErrCodeReplayedNonce {
		t.Fatalf("expected REPLAYED_NONCE, got %+v", rej)
	}

	// Out-of-band order ($12 when band is [5, 10]) must return OUT_OF_BAND
	oobRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypePlaceOrder, "n-oob", "c-oob", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 12, Qty: 1,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, oobRaw); rej == nil || rej.Code != protocol.ErrCodeOutOfBand {
		t.Fatalf("expected OUT_OF_BAND, got %+v", rej)
	}

	// Valid buy order by A: buy 5 copper @ max $9 (locks $45)
	buyRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypePlaceOrder, "n-buy1", "c-buy1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5,
	})
	buyRec, rej := eng.SubmitRawEnvelope(ctx, buyRaw)
	if rej != nil {
		t.Fatalf("buy order rejected: %+v", rej)
	}
	if err := protocol.VerifyReceipt(buyRec, srvKey.PublicKeyString()); err != nil {
		t.Fatalf("VerifyReceipt: %v", err)
	}

	// Self-cross check: A tries to sell copper @ min $8 (crosses A's own buy @ $9) -> SELF_CROSS
	crossRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypePlaceOrder, "n-cross", "c-cross", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 2,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, crossRaw); rej == nil || rej.Code != protocol.ErrCodeSelfCross {
		t.Fatalf("expected SELF_CROSS, got %+v", rej)
	}

	// Non-crossing opposite order by A: sell @ min $10 (buy max $9 < sell min $10) -> allowed!
	nonCrossRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypePlaceOrder, "n-nc", "c-nc", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 10, Qty: 2,
	})
	ncRec, rej := eng.SubmitRawEnvelope(ctx, nonCrossRaw)
	if rej != nil {
		t.Fatalf("expected non-crossing order to succeed, got %+v", rej)
	}
	// Cancel A's sell @ $10
	cancelRaw := makeSignedEnv(t, agA, "city-2026w46", protocol.TypeCancelOrder, "n-can", "c-can", ts, protocol.CancelOrderPayload{
		OrderID: ncRec.OrderID,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, cancelRaw); rej != nil {
		t.Fatalf("cancel failed: %+v", rej)
	}

	// Seller B places sell 5 copper @ min $7
	sellRaw := makeSignedEnv(t, agB, "city-2026w46", protocol.TypePlaceOrder, "n-sell1", "c-sell1", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideSell, LimitPrice: 7, Qty: 5,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, sellRaw); rej != nil {
		t.Fatalf("sell order rejected: %+v", rej)
	}

	// Check indicative price before clearing
	ind, err := eng.Indicative("copper")
	if err != nil || ind.IndicativeVolume != 5 {
		t.Fatalf("unexpected indicative quote: %+v err=%v", ind, err)
	}

	// Seal and clear the round
	fills, err := eng.SealAndClearRound(ctx)
	if err != nil {
		t.Fatalf("SealAndClearRound: %v", err)
	}
	if len(fills) != 2 {
		t.Fatalf("expected 2 fills, got %d: %+v", len(fills), fills)
	}

	// Replay the entire ledger from scratch and verify state_hash matches live engine state_hash
	replayed, err := ledger.ReplayAndVerify(&gm.manifest, eng.Log().Read(ctx, 1))
	if err != nil {
		t.Fatalf("ReplayAndVerify failed: %v", err)
	}
	liveHash, _ := eng.StateHash()
	replayHash, _ := replayed.StateHash()
	if liveHash != replayHash {
		t.Fatalf("state_hash mismatch: live=%s replay=%s", liveHash, replayHash)
	}
}
