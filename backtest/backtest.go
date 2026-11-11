package backtest

import (
	"encoding/json"
	"iter"

	"github.com/galaxytemple/AgentSquare/clearing"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/marketdata"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Strategy defines a trading strategy evaluated against a historical ledger's rounds (Spec B5).
type Strategy interface {
	OnRoundOpen(round uint64, asset string, history []marketdata.Candle, availableCash, availableAsset int64) []StrategyOrder
}

// StrategyOrder is a virtual limit order submitted by a backtest Strategy.
type StrategyOrder struct {
	Side       protocol.Side `json:"side"`
	LimitPrice int64         `json:"limit_price"`
	Qty        int64         `json:"qty"`
}

// RoundResult records the backtest performance after a single round.
type RoundResult struct {
	Round         uint64 `json:"round"`
	ClearingPrice int64  `json:"clearing_price"`
	FilledBuyQty  int64  `json:"filled_buy_qty"`
	FilledSellQty int64  `json:"filled_sell_qty"`
	EndingCash    int64  `json:"ending_cash"`
	EndingAsset   int64  `json:"ending_asset"`
	MarkToMarket  int64  `json:"mark_to_market"`
}

// Report summarizes a backtest run over a historical ledger (Spec B5).
type Report struct {
	Asset        string        `json:"asset"`
	InitialCash  int64         `json:"initial_cash"`
	InitialAsset int64         `json:"initial_asset"`
	FinalCash    int64         `json:"final_cash"`
	FinalAsset   int64         `json:"final_asset"`
	FinalValue   int64         `json:"final_value"`
	NetPnL       int64         `json:"net_pnl"`
	Rounds       []RoundResult `json:"rounds"`
}

const backtestAgentID = "ag_backtest_strategy"

// Run replays a historical ledger stream, injecting the strategy's orders into each round's open order book
// and executing the exact call-auction clearing engine to measure counterfactual fills and P&L.
func Run(asset string, initialCash, initialAsset int64, records iter.Seq2[ledger.Record, error], strat Strategy) (*Report, error) {
	cash := initialCash
	holdings := initialAsset

	openOrders := make(map[string]clearing.Order)
	var history []marketdata.Candle
	var roundResults []RoundResult
	var lastPrice int64

	for rec, err := range records {
		if err != nil {
			return nil, err
		}
		switch rec.Type {
		case ledger.RecOrderAccepted:
			var b ledger.OrderAcceptedBody
			if err := json.Unmarshal(rec.Body, &b); err == nil && b.Asset == asset {
				openOrders[b.OrderID] = clearing.Order{
					OrderID:    b.OrderID,
					AgentID:    b.AgentID,
					Asset:      b.Asset,
					Side:       b.Side,
					LimitPrice: b.LimitPrice,
					Qty:        b.Qty,
				}
			}
		case ledger.RecOrderCancelled:
			var b ledger.OrderCancelledBody
			if err := json.Unmarshal(rec.Body, &b); err == nil {
				delete(openOrders, b.OrderID)
			}
		case ledger.RecOrderReplaced:
			var b ledger.OrderReplacedBody
			if err := json.Unmarshal(rec.Body, &b); err == nil {
				delete(openOrders, b.OldOrderID)
				if b.Asset == asset {
					openOrders[b.NewOrderID] = clearing.Order{
						OrderID:    b.NewOrderID,
						AgentID:    b.AgentID,
						Asset:      b.Asset,
						Side:       b.Side,
						LimitPrice: b.LimitPrice,
						Qty:        b.Qty,
					}
				}
			}
		case ledger.RecClear:
			var b ledger.ClearBody
			if err := json.Unmarshal(rec.Body, &b); err != nil || b.Asset != asset {
				continue
			}

			// Collect historical round orders + strategy orders
			orders := make([]clearing.Order, 0, len(openOrders)+2)
			for _, o := range openOrders {
				orders = append(orders, o)
			}

			stratOrders := strat.OnRoundOpen(b.Round, asset, history, cash, holdings)
			lockedCash := int64(0)
			lockedAsset := int64(0)
			for idx, so := range stratOrders {
				if so.Qty <= 0 || so.LimitPrice <= 0 {
					continue
				}
				if so.Side == protocol.SideBuy {
					cost := so.LimitPrice * so.Qty
					if cash-lockedCash < cost {
						continue
					}
					lockedCash += cost
				} else {
					if holdings-lockedAsset < so.Qty {
						continue
					}
					lockedAsset += so.Qty
				}
				orders = append(orders, clearing.Order{
					OrderID:    "bt_ord_" + string(rune('0'+idx)),
					AgentID:    backtestAgentID,
					Asset:      asset,
					Side:       so.Side,
					LimitPrice: so.LimitPrice,
					Qty:        so.Qty,
				})
			}

			res := clearing.Clear(clearing.ClearingInput{
				GameID:            rec.GameID,
				Round:             b.Round,
				Asset:             asset,
				Orders:            orders,
				PrevClearingPrice: b.PrevPrice,
				ReferencePrice:    b.ReferencePrice,
				SeedSig:           b.SeedSig,
			})

			var filledBuy, filledSell int64
			for _, f := range res.Fills {
				if f.AgentID != backtestAgentID {
					continue
				}
				if f.Side == protocol.SideBuy {
					filledBuy += f.FilledQty
					cash -= f.CashDelta
					holdings += f.FilledQty
				} else {
					filledSell += f.FilledQty
					cash += f.CashDelta
					holdings -= f.FilledQty
				}
			}

			if res.ClearingPrice > 0 {
				lastPrice = res.ClearingPrice
			}
			mtm := cash + holdings*lastPrice
			roundResults = append(roundResults, RoundResult{
				Round:         b.Round,
				ClearingPrice: res.ClearingPrice,
				FilledBuyQty:  filledBuy,
				FilledSellQty: filledSell,
				EndingCash:    cash,
				EndingAsset:   holdings,
				MarkToMarket:  mtm,
			})
			history = append(history, marketdata.Candle{
				Round:  b.Round,
				Asset:  asset,
				Open:   res.ClearingPrice,
				High:   res.ClearingPrice,
				Low:    res.ClearingPrice,
				Close:  res.ClearingPrice,
				Volume: res.Volume,
			})
			openOrders = make(map[string]clearing.Order)
		}
	}

	initialVal := initialCash + initialAsset*lastPrice
	finalVal := cash + holdings*lastPrice
	return &Report{
		Asset:        asset,
		InitialCash:  initialCash,
		InitialAsset: initialAsset,
		FinalCash:    cash,
		FinalAsset:   holdings,
		FinalValue:   finalVal,
		NetPnL:       finalVal - initialVal,
		Rounds:       roundResults,
	}, nil
}
