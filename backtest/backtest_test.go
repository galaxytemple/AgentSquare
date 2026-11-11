package backtest_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/galaxytemple/AgentSquare/backtest"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/marketdata"
	"github.com/galaxytemple/AgentSquare/protocol"
)

type simpleBuyerStrategy struct{}

func (simpleBuyerStrategy) OnRoundOpen(_ uint64, _ string, _ []marketdata.Candle, availableCash, _ int64) []backtest.StrategyOrder {
	if availableCash >= 18 {
		return []backtest.StrategyOrder{
			{Side: protocol.SideBuy, LimitPrice: 9, Qty: 2},
		}
	}
	return nil
}

func TestBacktestAgainstHistoricalLedger(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	eng, _, err := conformance.NewVectorEngine(ctx, vecs[0])
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	// Replay vec[0] inputs into eng
	for _, in := range vecs[0].Inputs {
		if in.Action == "envelope" {
			eng.SubmitRawEnvelope(ctx, in.Envelope)
		} else if in.Action == "clear_round" {
			_, _ = eng.SealAndClearRound(ctx)
		}
	}

	rep, err := backtest.Run("copper", 100, 0, eng.Log().Read(ctx, 1), simpleBuyerStrategy{})
	if err != nil {
		t.Fatalf("backtest.Run: %v", err)
	}
	if len(rep.Rounds) == 0 || rep.FinalAsset <= 0 {
		t.Fatalf("expected backtest strategy to participate and acquire copper, got %+v", rep)
	}
	_ = filepath.Clean(".")
}
