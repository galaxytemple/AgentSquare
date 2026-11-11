package marketdata

import (
	"encoding/json"
	"testing"

	"github.com/galaxytemple/AgentSquare/clearing"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
)

func TestComputeIndicativeAndDepth(t *testing.T) {
	orders := []clearing.Order{
		{OrderID: "o1", AgentID: "ag_1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5},
		{OrderID: "o2", AgentID: "ag_2", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 8, Qty: 3},
		{OrderID: "o3", AgentID: "ag_3", Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 6},
	}

	ind := ComputeIndicative("g", 1, "copper", orders, 9, 10, 5, 10)
	if ind.IndicativePrice != 9 && ind.IndicativePrice != 8 {
		t.Fatalf("unexpected indicative price: %+v", ind)
	}
	if ind.IndicativeVolume != 6 {
		t.Fatalf("expected indicative volume 6, got %d", ind.IndicativeVolume)
	}

	depth := ComputeDepth(1, "copper", orders)
	if len(depth.Bids) != 2 || depth.Bids[0].Price != 9 || depth.Bids[0].Qty != 5 {
		t.Fatalf("unexpected bids depth: %+v", depth.Bids)
	}
	if len(depth.Asks) != 1 || depth.Asks[0].Price != 8 || depth.Asks[0].Qty != 6 {
		t.Fatalf("unexpected asks depth: %+v", depth.Asks)
	}
}

func TestTrackerApplyTapeCandlesAndStats(t *testing.T) {
	tr := NewTracker()

	// Empty stats
	emptySt := tr.Stats("copper", 5)
	if emptySt.Rounds != 0 || emptySt.TotalVolume != 0 {
		t.Fatalf("expected empty stats, got %+v", emptySt)
	}

	mustRec := func(recType string, body any) ledger.Record {
		b, _ := json.Marshal(body)
		return ledger.Record{Type: recType, Body: b}
	}

	// Round 1: 10买 + 6卖 = 16 submitted, clear 6 @ $8 (executed sides = 12, unfilled = 4 -> 2500 bps)
	tr.Apply(mustRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{Round: 1, Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 10}))
	tr.Apply(mustRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{Round: 1, Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 6}))
	tr.Apply(mustRec(ledger.RecClear, ledger.ClearBody{Round: 1, Asset: "copper", ClearingPrice: 8, Volume: 6}))
	tr.Apply(mustRec(ledger.RecFill, protocol.Fill{Round: 1, Asset: "copper", AgentID: "ag_b", Side: protocol.SideBuy, FilledQty: 6, ClearingPrice: 8}))
	tr.Apply(mustRec(ledger.RecFill, protocol.Fill{Round: 1, Asset: "copper", AgentID: "ag_s", Side: protocol.SideSell, FilledQty: 6, ClearingPrice: 8}))
	tr.Apply(mustRec(ledger.RecSettle, ledger.SettleBody{Round: 1}))

	// Round 2: 4买 + 4卖 = 8 submitted, clear 4 @ $10
	tr.Apply(mustRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{Round: 2, Asset: "copper", Side: protocol.SideBuy, LimitPrice: 10, Qty: 4}))
	tr.Apply(mustRec(ledger.RecOrderAccepted, ledger.OrderAcceptedBody{Round: 2, Asset: "copper", Side: protocol.SideSell, LimitPrice: 10, Qty: 4}))
	tr.Apply(mustRec(ledger.RecClear, ledger.ClearBody{Round: 2, Asset: "copper", ClearingPrice: 10, Volume: 4}))
	tr.Apply(mustRec(ledger.RecFill, protocol.Fill{Round: 2, Asset: "copper", AgentID: "ag_b", Side: protocol.SideBuy, FilledQty: 4, ClearingPrice: 10}))
	tr.Apply(mustRec(ledger.RecFill, protocol.Fill{Round: 2, Asset: "copper", AgentID: "ag_s", Side: protocol.SideSell, FilledQty: 4, ClearingPrice: 10}))
	tr.Apply(mustRec(ledger.RecSettle, ledger.SettleBody{Round: 2}))

	tapeAll := tr.Tape("copper", 0)
	if len(tapeAll) != 2 {
		t.Fatalf("expected 2 tape entries, got %d", len(tapeAll))
	}
	tapeLast1 := tr.Tape("copper", 1)
	if len(tapeLast1) != 1 || tapeLast1[0].Round != 2 {
		t.Fatalf("expected last 1 tape entry to be round 2, got %+v", tapeLast1)
	}

	candles := tr.Candles("copper", 5)
	if len(candles) != 2 || candles[0].Close != 8 || candles[1].Close != 10 {
		t.Fatalf("unexpected candles: %+v", candles)
	}

	st := tr.Stats("copper", 5)
	// TotalVolume = 6 + 4 = 10; sumPV = 48 + 40 = 88 -> VWAP = 88/10 = 8; High = 10, Low = 8, Last = 10
	// Submitted = 16 + 8 = 24; ExecutedSides = 12 + 8 = 20; Unfilled = 4/24 = 1666 bps
	if st.TotalVolume != 10 || st.VWAP != 8 || st.High != 10 || st.Low != 8 || st.LastPrice != 10 || st.UnfilledRatio != 1666 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}
