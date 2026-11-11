// Package main demonstrates embedding AgentSquare into a minimal 1-asset, 1-minute-round game in <100 lines (Spec C3).
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

type MinimalCopperGame struct {
	serverPubKey string
}

func (g *MinimalCopperGame) Manifest() manifest.Manifest {
	return manifest.Manifest{
		Protocol:          protocol.ProtocolID,
		SupportedVersions: []string{"1.0"},
		GameID:            "minimal-copper-demo",
		Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
		SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
		Round:             manifest.RoundConfig{Interval: "PT1M", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
		PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
		MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
		FeeBps:            0,
		Admission:         "open",
		Ledger:            manifest.LedgerConfig{Public: "after-clear"},
		ServerPubKey:      g.serverPubKey,
		AdminPubKeys:      []string{g.serverPubKey},
	}
}

func (g *MinimalCopperGame) ReferencePrice(_ string, _ uint64) int64 { return 10 }

func (g *MinimalCopperGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 100, Reason: "starter_deck"},
		{Asset: "copper", Qty: 10, Reason: "starter_deck"},
	}, nil
}

func (g *MinimalCopperGame) OnSettled(_ context.Context, round uint64, fills []protocol.Fill) error {
	fmt.Printf("[minimal-game] round %d settled with %d fills\n", round, len(fills))
	return nil
}

func main() {
	signer, err := protocol.GenerateKeyPair(nil)
	if err != nil {
		log.Fatal(err)
	}
	game := &MinimalCopperGame{serverPubKey: signer.PublicKeyString()}
	eng, err := engine.New(game, engine.WithSigner(signer))
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for range time.Tick(time.Minute) {
			_, _ = eng.SealAndClearRound(context.Background())
		}
	}()

	fmt.Println("Minimal Copper Game listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", aghttp.NewServer(eng, aghttp.WithSimTrigger(true))))
}
