package ledger_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/ledger/logtest"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/storage/filelog"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

func TestMemLogConformance(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		return memlog.New()
	})
}

func TestFileLogConformance(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		p := filepath.Join(t.TempDir(), "test-ledger.jsonl")
		lg, err := filelog.Open(p)
		if err != nil {
			t.Fatalf("filelog.Open: %v", err)
		}
		t.Cleanup(func() { _ = lg.Close() })
		return lg
	})
}

func TestLedgerReplayVerificationAndTamperDetection(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	for _, vec := range vecs {
		lg := memlog.New()
		if _, err := lg.Append(ctx, vec.ExpectedLedgerAfter...); err != nil {
			t.Fatalf("%s Append: %v", vec.Name, err)
		}
		st, err := ledger.ReplayAndVerify(&vec.Manifest, lg.Read(ctx, 1))
		if err != nil {
			t.Fatalf("%s ReplayAndVerify failed: %v", vec.Name, err)
		}
		h, err := st.StateHash()
		if err != nil || h != vec.ExpectedStateHash {
			t.Fatalf("%s state_hash mismatch: got %s, want %s", vec.Name, h, vec.ExpectedStateHash)
		}
	}

	// Tampering with a record's hash or body must fail ReplayAndVerify
	tampered := make([]ledger.Record, len(vecs[0].ExpectedLedgerAfter))
	copy(tampered, vecs[0].ExpectedLedgerAfter)
	tampered[1].Body = []byte(`{"agent_id":"ag_tampered","pubkey":"ed25519:00"}`)

	st := ledger.NewState(&vecs[0].Manifest)
	if err := st.Apply(tampered[0]); err != nil {
		t.Fatalf("Apply genesis: %v", err)
	}
	if err := st.Apply(tampered[1]); err == nil {
		t.Fatalf("expected tampered record to fail signature/hash verification")
	}
}

func TestUnitCarriedOverOrderAggregationWithTTLAndImmediateExpiry(t *testing.T) {
	srvKey, _ := protocol.GenerateKeyPair(nil)
	m := manifest.Manifest{
		Protocol:          protocol.ProtocolID,
		SupportedVersions: []string{"1.0"},
		GameID:            "unit-ttl-agg",
		Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
		SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
		Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-10T00:00:00Z", CarryOver: false},
		PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 150},
		MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
		Admission:         "open",
		Ledger:            manifest.LedgerConfig{Public: "full"},
		ServerPubKey:      srvKey.PublicKeyString(),
	}

	st := ledger.NewState(&m)
	seq := uint64(0)
	prevHash := ledger.ZeroHash

	applyRec := func(recType string, body any) {
		t.Helper()
		seq++
		rawBody, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s: %v", recType, err)
		}
		rec := ledger.Record{
			Seq:      seq,
			PrevHash: prevHash,
			GameID:   m.GameID,
			Type:     recType,
			Body:     rawBody,
		}
		if err := ledger.SignAndHashRecord(&rec, srvKey); err != nil {
			t.Fatalf("sign %s: %v", recType, err)
		}
		if err := st.Apply(rec); err != nil {
			t.Fatalf("apply seq %d (%s): %v", seq, recType, err)
		}
		prevHash = rec.Hash
	}

	mh, _ := m.Hash()
	applyRec(ledger.RecGenesis, ledger.GenesisBody{ManifestHash: mh})

	keyA, _ := protocol.GenerateKeyPair(nil)
	keyS, _ := protocol.GenerateKeyPair(nil)
	idA := keyA.AgentID()
	idS := keyS.AgentID()

	// Admit Buyer A ($10,000) and Seller S (100 copper)
	applyRec(ledger.RecAgentAdmitted, ledger.AgentAdmittedBody{AgentID: idA, PubKey: keyA.PublicKeyString()})
	applyRec(ledger.RecBalanceCredit, ledger.BalanceChangeBody{AgentID: idA, Asset: "usd", Qty: 10000, Reason: "init"})
	applyRec(ledger.RecAgentAdmitted, ledger.AgentAdmittedBody{AgentID: idS, PubKey: keyS.PublicKeyString()})
	applyRec(ledger.RecBalanceCredit, ledger.BalanceChangeBody{AgentID: idS, Asset: "copper", Qty: 100, Reason: "init"})

	// =========================================================================
	// Round 1: Buyer A places 3 orders that will carry over into Round 2:
	// - ord_1_high: Buy 3 @ $100, TTLRounds=2 ($300 locked; expires at end of R2 if unfilled)
	// - ord_1_gtc:  Buy 4 @ $90,  CarryOver=true ($360 locked; GTC infinite life)
	// - ord_1_ttl2: Buy 4 @ $90,  TTLRounds=2 ($360 locked; in R2 its remaining life is 1 round!)
	// - ord_1_exp:  Buy 5 @ $90,  CarryOver=false ($450 locked; default 1-round -> expires at end of R1!)
	// =========================================================================
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_1_high", Round: 1, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 100, Qty: 3, TTLRounds: 2, EnvelopeSig: "ed25519:00",
	})
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_1_gtc", Round: 1, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 90, Qty: 4, CarryOver: true, EnvelopeSig: "ed25519:00",
	})
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_1_ttl2", Round: 1, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 90, Qty: 4, TTLRounds: 2, EnvelopeSig: "ed25519:00",
	})
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_1_exp", Round: 1, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 90, Qty: 5, CarryOver: false, EnvelopeSig: "ed25519:00",
	})

	// Seal & Settle Round 1 with 0 sellers (volume=0)
	_, seedSig1, _ := protocol.DeriveLotterySeed(srvKey, m.GameID, 1)
	applyRec(ledger.RecRoundSealed, ledger.RoundSealedBody{Round: 1, CutoffTime: 100, RefPrices: map[string]int64{"copper": 100}})
	applyRec(ledger.RecClear, ledger.ClearBody{Round: 1, Asset: "copper", ClearingPrice: 0, Volume: 0, ReferencePrice: 100, SeedSig: seedSig1})
	applyRec(ledger.RecSettle, ledger.SettleBody{Round: 1, CarryOver: false})

	// Verify after Round 1:
	// - ord_1_exp (5 @ $90) MUST be auto-expired and $450 unlocked
	// - ord_1_high (3 @ $100), ord_1_gtc (4 @ $90), ord_1_ttl2 (4 @ $90) MUST remain open ($1,020 locked)
	if _, exists := st.OpenOrders["ord_1_exp"]; exists {
		t.Fatalf("expected default 1-round order ord_1_exp to expire at end of Round 1")
	}
	if len(st.OpenOrders) != 3 || st.Agents[idA].Locked["usd"] != 1020 || st.Agents[idA].Available["usd"] != 8980 {
		t.Fatalf("unexpected state after Round 1: open=%d locked=%d avail=%d",
			len(st.OpenOrders), st.Agents[idA].Locked["usd"], st.Agents[idA].Available["usd"])
	}
	if err := st.DoctorCheck(); err != nil {
		t.Fatalf("DoctorCheck after R1 failed: %v", err)
	}

	// =========================================================================
	// Round 2: Buyer A places 2 MORE orders at $90 in Round 2:
	// - ord_2_ttl3: Buy 4 @ $90, TTLRounds=3 ($360 locked; remaining life in R2 = 3 rounds)
	// - ord_2_exp:  Buy 4 @ $90, CarryOver=false ($360 locked; remaining life in R2 = 1 round, expires after R2!)
	// =========================================================================
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_2_ttl3", Round: 2, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 90, Qty: 4, TTLRounds: 3, EnvelopeSig: "ed25519:00",
	})
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_2_exp", Round: 2, AgentID: idA, Asset: "copper", Side: protocol.SideBuy,
		LimitPrice: 90, Qty: 4, CarryOver: false, EnvelopeSig: "ed25519:00",
	})
	applyRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID: "ord_2_sell", Round: 2, AgentID: idS, Asset: "copper", Side: protocol.SideSell,
		LimitPrice: 90, Qty: 9, CarryOver: false, EnvelopeSig: "ed25519:00",
	})

	_, seedSig2, _ := protocol.DeriveLotterySeed(srvKey, m.GameID, 2)
	applyRec(ledger.RecRoundSealed, ledger.RoundSealedBody{Round: 2, CutoffTime: 200, RefPrices: map[string]int64{"copper": 100}})
	applyRec(ledger.RecClear, ledger.ClearBody{Round: 2, Asset: "copper", ClearingPrice: 90, Volume: 9, ReferencePrice: 100, SeedSig: seedSig2})
	applyRec(ledger.RecFill, protocol.Fill{GameID: m.GameID, Round: 2, Asset: "copper", AgentID: idA, Side: protocol.SideBuy, FilledQty: 9, ClearingPrice: 90, CashDelta: 810})
	applyRec(ledger.RecFill, protocol.Fill{GameID: m.GameID, Round: 2, Asset: "copper", AgentID: idS, Side: protocol.SideSell, FilledQty: 9, ClearingPrice: 90, CashDelta: 810})
	applyRec(ledger.RecSettle, ledger.SettleBody{Round: 2, CarryOver: false})

	// Check post-Round 2 invariants!
	if len(st.OpenOrders) != 2 {
		t.Fatalf("expected exactly 2 open orders (ord_1_gtc and ord_2_ttl3) after Round 2, got %d: %+v", len(st.OpenOrders), st.OpenOrders)
	}
	if o1 := st.OpenOrders["ord_1_gtc"]; o1 == nil || o1.Qty != 4 || o1.LockedAmt != 360 {
		t.Fatalf("expected ord_1_gtc to survive untouched with qty=4 locked=360, got %+v", o1)
	}
	if o2 := st.OpenOrders["ord_2_ttl3"]; o2 == nil || o2.Qty != 4 || o2.LockedAmt != 360 {
		t.Fatalf("expected ord_2_ttl3 to survive untouched with qty=4 locked=360, got %+v", o2)
	}
	// Buyer A started with $10,000, paid $810 for 9 copper, and has $720 locked in 2 surviving orders -> Available MUST be $8,470!
	if st.Agents[idA].Available["copper"] != 9 || st.Agents[idA].Locked["usd"] != 720 || st.Agents[idA].Available["usd"] != 8470 {
		t.Fatalf("unexpected Buyer A balances after Round 2: copper=%d locked_usd=%d avail_usd=%d",
			st.Agents[idA].Available["copper"], st.Agents[idA].Locked["usd"], st.Agents[idA].Available["usd"])
	}
	if err := st.DoctorCheck(); err != nil {
		t.Fatalf("DoctorCheck after R2 failed: %v", err)
	}
}
