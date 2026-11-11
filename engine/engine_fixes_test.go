package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

func setupFixesTestEngine(t *testing.T) (*Engine, protocol.Signer, sim.Clock) {
	srvSeed := make([]byte, 32)
	srvSeed[0] = 99
	srvKey, err := protocol.NewSignerFromSeed(srvSeed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	t0 := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &testGame{
		refPrice: 10,
		manifest: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "fixes-game",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "depth"},
			FeeBps:            0,
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey:      srvKey.PublicKeyString(),
			AdminPubKeys:      []string{srvKey.PublicKeyString()},
			InitialBalances:   map[string]int64{"usd": 100, "copper": 5},
		},
	}
	lg := memlog.New()
	eng, err := New(gm, WithSigner(srvKey), WithClock(clk), WithLog(lg))
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}
	return eng, srvKey, clk
}

func TestFixIssue1CreditOrDebitBatchPreValidation(t *testing.T) {
	eng, _, _ := setupFixesTestEngine(t)
	ctx := context.Background()

	// Register agent A
	aKey, _ := protocol.GenerateKeyPair(nil)
	raw := makeSignedEnv(t, aKey, eng.manifest.GameID, protocol.TypeAgentRegister, "reg-1", "c-reg", eng.clock.Now().Unix(), protocol.RegisterPayload{
		PubKey: aKey.PublicKeyString(),
	})
	rec, rej := eng.SubmitRawEnvelope(ctx, raw)
	if rej != nil || rec == nil {
		t.Fatalf("Register failed: %v", rej)
	}

	headSeqBefore, _, _ := eng.log.Head(ctx)

	// Attempt overdraft debit
	err := eng.CreditOrDebitBatch(ctx, []struct {
		AgentID string
		Asset   string
		Delta   int64
		Reason  string
	}{
		{AgentID: aKey.AgentID(), Asset: "usd", Delta: -999999, Reason: "overdraft_test"},
	})
	if err == nil {
		t.Fatalf("expected error from overdraft, got nil")
	}

	headSeqAfter, _, _ := eng.log.Head(ctx)
	if headSeqAfter != headSeqBefore {
		t.Fatalf("log corrupted: headSeq advanced from %d to %d despite validation failure", headSeqBefore, headSeqAfter)
	}

	// Reboot must succeed cleanly without errors
	eng2, err := New(eng.game, WithSigner(eng.signer), WithClock(eng.clock), WithLog(eng.log))
	if err != nil {
		t.Fatalf("engine reboot failed after rejected debit batch: %v", err)
	}
	if eng2.state.LastSeq != headSeqBefore {
		t.Fatalf("rebooted state LastSeq mismatch: got %d, want %d", eng2.state.LastSeq, headSeqBefore)
	}
}

func TestFixIssue2TickAndClearConcurrency(t *testing.T) {
	eng, _, clk := setupFixesTestEngine(t)
	ctx := context.Background()

	// Advance clock past round 1 cutoff (cutoff is at 01:00:00Z)
	fakeClk := clk.(*sim.FakeClock)
	fakeClk.Advance(1 * time.Hour)

	var wg sync.WaitGroup
	clearedCount := 0
	var mu sync.Mutex

	// Launch multiple concurrent Tick and SealAndClearRound calls
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cleared, _, err := eng.Tick(ctx)
			if err == nil && cleared {
				mu.Lock()
				clearedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Only 1 Tick should have succeeded because round 2's cutoff has NOT arrived!
	if clearedCount != 1 {
		t.Fatalf("expected exactly 1 Tick to clear round 1, but got %d (possible double clearing)", clearedCount)
	}
	if eng.state.CurrentRound != 2 {
		t.Fatalf("expected current round to be 2, got %d", eng.state.CurrentRound)
	}
}

func TestFixIssue4MarketDataTrackerGTCStats(t *testing.T) {
	eng, _, _ := setupFixesTestEngine(t)
	ctx := context.Background()

	// Register agent A
	aKey, _ := protocol.GenerateKeyPair(nil)
	raw := makeSignedEnv(t, aKey, eng.manifest.GameID, protocol.TypeAgentRegister, "reg-1", "c-reg", eng.clock.Now().Unix(), protocol.RegisterPayload{
		PubKey: aKey.PublicKeyString(),
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, raw); rej != nil {
		t.Fatalf("Register failed: %v", rej)
	}

	// Place order with CarryOver: true
	placeRaw := makeSignedEnv(t, aKey, eng.manifest.GameID, protocol.TypePlaceOrder, "ord-1", "c-ord", eng.clock.Now().Unix(), protocol.PlaceOrderPayload{
		Asset:      "copper",
		Side:       protocol.SideBuy,
		LimitPrice: 10,
		Qty:        5,
		CarryOver:  true,
	})
	receipt, rej := eng.SubmitRawEnvelope(ctx, placeRaw)
	if rej != nil || receipt == nil {
		t.Fatalf("PlaceOrder failed: %v", rej)
	}

	// Advance and clear round 1
	if _, err := eng.SealAndClearRound(ctx); err != nil {
		t.Fatalf("SealAndClearRound round 1: %v", err)
	}

	// Cancel the carried-over order in round 2
	cancelRaw := makeSignedEnv(t, aKey, eng.manifest.GameID, protocol.TypeCancelOrder, "c-1", "c-canc", eng.clock.Now().Unix(), protocol.CancelOrderPayload{
		OrderID: receipt.OrderID,
	})
	if _, rej := eng.SubmitRawEnvelope(ctx, cancelRaw); rej != nil {
		t.Fatalf("CancelOrder failed: %v", rej)
	}

	// Clear round 2
	if _, err := eng.SealAndClearRound(ctx); err != nil {
		t.Fatalf("SealAndClearRound round 2: %v", err)
	}

	// The order was cancelled, so submitted quantity should be decremented and unfilled ratio should be 0
	st2 := eng.Stats("copper", 2)
	if st2.UnfilledRatio != 0 {
		t.Fatalf("expected UnfilledRatio 0 after cancelling carried-over order, got %d bps", st2.UnfilledRatio)
	}
}

func TestFixIssue5AdminKeyRotateSnapshotRestore(t *testing.T) {
	eng, srvKey, _ := setupFixesTestEngine(t)
	ctx := context.Background()

	// Rotate admin key
	newAdminSigner, _ := protocol.GenerateKeyPair(nil)
	msg := []byte("rotate|" + srvKey.AgentID() + "|" + newAdminSigner.PublicKeyString())
	sig, _ := newAdminSigner.Sign(msg)

	rotateEnv := makeSignedEnv(t, srvKey, eng.manifest.GameID, protocol.TypeKeyRotate, "rot-1", "c-rot", eng.clock.Now().Unix(), protocol.KeyRotatePayload{
		OldPubKey: srvKey.PublicKeyString(),
		NewPubKey: newAdminSigner.PublicKeyString(),
		NewSig:    sig,
	})
	rec, rej := eng.SubmitRawEnvelope(ctx, rotateEnv)
	if rej != nil || rec == nil {
		t.Fatalf("Admin KeyRotate failed: %v", rej)
	}

	// Create snapshot checkpoint
	if _, err := eng.AppendCheckpoint(ctx); err != nil {
		t.Fatalf("AppendCheckpoint: %v", err)
	}

	// Reboot new engine instance from the log with snapshot
	eng2, err := New(eng.game, WithSigner(eng.signer), WithClock(eng.clock), WithLog(eng.log))
	if err != nil {
		t.Fatalf("New eng2: %v", err)
	}

	// Admin key MUST be the rotated key
	if len(eng2.state.AdminPubKeys) == 0 || eng2.state.AdminPubKeys[0] != newAdminSigner.PublicKeyString() {
		t.Fatalf("Admin key rotate lost after snapshot restore: got %v, want %s",
			eng2.state.AdminPubKeys, newAdminSigner.PublicKeyString())
	}
}

func TestFixIssue6AdminModerateEmptyPostID(t *testing.T) {
	eng, srvKey, _ := setupFixesTestEngine(t)
	ctx := context.Background()

	// Admin attempts to moderate with empty post_id
	modEnv := makeSignedEnv(t, srvKey, eng.manifest.GameID, protocol.TypeAdminModerate, "mod-1", "c-mod", eng.clock.Now().Unix(), protocol.AdminModeratePayload{
		PostID: "",
		Reason: "spam",
	})
	rec, rej := eng.SubmitRawEnvelope(ctx, modEnv)
	if rec != nil || rej == nil {
		t.Fatalf("expected rejection for empty post_id, got receipt: %+v", rec)
	}
	if !strings.Contains(rej.Reason, "empty") {
		t.Fatalf("unexpected rejection reason: %s", rej.Reason)
	}
}
