package manifest

import (
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/protocol"
)

func TestManifestValidationAndBounds(t *testing.T) {
	seed := make([]byte, 32)
	seed[0] = 1
	signer, _ := protocol.NewSignerFromSeed(seed)

	m := &Manifest{
		Protocol:          protocol.ProtocolID,
		SupportedVersions: []string{"1.0"},
		GameID:            "city-2026w46",
		Assets:            []Asset{{ID: "copper", QtyUnit: 1}},
		SettlementUnit:    SettlementUnit{ID: "usd", Tick: 1},
		Round:             RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
		PriceBand:         PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
		MarketData:        MarketDataConfig{PreClear: "indicative"},
		FeeBps:            0,
		Admission:         "open",
		Ledger:            LedgerConfig{Public: "after-clear"},
		ServerPubKey:      signer.PublicKeyString(),
		AdminPubKeys:      []string{signer.PublicKeyString()},
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	minP, maxP := m.PriceBounds(10)
	if minP != 5 || maxP != 10 {
		t.Fatalf("PriceBounds(10) = [%d, %d], want [5, 10]", minP, maxP)
	}

	t0 := time.Date(2026, 11, 9, 2, 30, 0, 0, time.UTC)
	r, start, end, err := m.RoundForTime(t0)
	if err != nil {
		t.Fatalf("RoundForTime: %v", err)
	}
	if r != 3 {
		t.Fatalf("got round %d, want 3", r)
	}
	if !start.Equal(time.Date(2026, 11, 9, 2, 0, 0, 0, time.UTC)) || !end.Equal(time.Date(2026, 11, 9, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected bounds: %v to %v", start, end)
	}

	if !m.HasAsset("copper") || m.HasAsset("gold") {
		t.Fatalf("unexpected HasAsset result")
	}

	bStart, bEnd, err := m.RoundBounds(3)
	if err != nil || !bStart.Equal(start) || !bEnd.Equal(end) {
		t.Fatalf("unexpected RoundBounds(3): %v, %v, %v", bStart, bEnd, err)
	}
	if _, _, err := m.RoundBounds(0); err == nil {
		t.Fatalf("expected RoundBounds(0) to fail")
	}

	h, err := m.Hash()
	if err != nil || h == "" {
		t.Fatalf("Hash failed: %v", err)
	}

	for _, d := range []string{"PT15M", "PT30S", "PT1H30M"} {
		if _, err := ParseISO8601Duration(d); err != nil {
			t.Fatalf("expected duration %s to parse, got %v", d, err)
		}
	}
	for _, bad := range []string{"1H", "PT", "PT-5S", "PT10X", "PT10"} {
		if _, err := ParseISO8601Duration(bad); err == nil {
			t.Fatalf("expected invalid duration %s to fail", bad)
		}
	}

	if _, err := ParseAndValidate([]byte(`{"protocol":"agentsquare/1.0","critical":["unknown_field"]}`)); err == nil {
		t.Fatalf("expected unknown critical field in manifest to fail")
	}
}
