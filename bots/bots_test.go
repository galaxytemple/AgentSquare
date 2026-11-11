package bots_test

import (
	"context"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/bots"
	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
)

type botGame struct {
	m manifest.Manifest
}

func (g *botGame) Manifest() manifest.Manifest           { return g.m }
func (g *botGame) ReferencePrice(_ string, _ uint64) int64 { return 10 }
func (g *botGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 500, Reason: "deck"},
		{Asset: "copper", Qty: 50, Reason: "deck"},
	}, nil
}
func (g *botGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error { return nil }

func TestFourReferenceBotsAndManipulatorTalkVsTrade(t *testing.T) {
	ctx := context.Background()
	srvSeed := make([]byte, 32)
	srvSeed[0] = 1
	srvKey, _ := protocol.NewSignerFromSeed(srvSeed)

	t0 := time.Date(2026, 11, 9, 14, 0, 0, 0, time.UTC)
	clk := sim.NewFakeClock(t0)

	gm := &botGame{
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            "bots-sim",
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			FeeBps:            0,
			Board:             &manifest.BoardConfig{Enabled: true, MaxPostsPerAgentPerRound: 5, MaxBodyChars: 280, MinIntervalSec: 0, RequireParticipant: true},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey:      srvKey.PublicKeyString(),
			AdminPubKeys:      []string{srvKey.PublicKeyString()},
		},
	}

	eng, err := engine.New(gm, engine.WithSigner(srvKey), engine.WithClock(clk))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	mkKey := func(b byte) *protocol.MemorySigner {
		s := make([]byte, 32)
		s[0] = b
		k, _ := protocol.NewSignerFromSeed(s)
		return k
	}

	bList := []bots.Bot{
		bots.NewRandomBot("random", mkKey(10), 42),
		bots.NewMarketMakerBot("mm", mkKey(11), 1, 5),
		bots.NewTrendFollowerBot("trend", mkKey(12), 2),
		bots.NewManipulatorBot("manip", mkKey(13), 3),
	}

	ts := t0.Unix()
	for _, b := range bList {
		if err := bots.RegisterBot(ctx, eng, b, ts); err != nil {
			t.Fatalf("RegisterBot(%s): %v", b.Name(), err)
		}
	}

	// Run 3 rounds of bot activity
	for r := 0; r < 3; r++ {
		for _, b := range bList {
			if err := b.Step(ctx, eng, "copper", ts); err != nil {
				t.Fatalf("Step(%s): %v", b.Name(), err)
			}
		}
		if _, err := eng.SealAndClearRound(ctx); err != nil {
			t.Fatalf("SealAndClearRound: %v", err)
		}
	}

	// ManipulatorBot posted "bear" and bought copper -> TalkVsTrade should flag contrarian behavior
	manipID := bList[3].Signer().AgentID()
	rep := eng.TalkVsTrade(manipID, "copper", 2)
	if rep.TotalStancePosts != 3 || rep.ContrarianCount == 0 {
		t.Fatalf("expected manipulator contrarian posts to be detected, got %+v", rep)
	}
}
