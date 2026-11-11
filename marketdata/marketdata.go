package marketdata

import (
	"encoding/json"
	"sort"

	"github.com/galaxytemple/AgentSquare/clearing"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// TapeEntry represents a post-clear fill entry on the public tape (Spec A11).
type TapeEntry struct {
	Round         uint64        `json:"round"`
	Asset         string        `json:"asset"`
	ClearingPrice int64         `json:"clearing_price"`
	Volume        int64         `json:"volume"`
	Fills         []protocol.Fill `json:"fills"`
}

// Candle represents per-round OHLCV data (Spec A11).
// In batch call auctions, a single clearing price per round means Open=High=Low=Close=ClearingPrice.
type Candle struct {
	Round  uint64 `json:"round"`
	Asset  string `json:"asset"`
	Open   int64  `json:"open"`
	High   int64  `json:"high"`
	Low    int64  `json:"low"`
	Close  int64  `json:"close"`
	Volume int64  `json:"volume"`
}

// Stats represents aggregated market statistics over N rounds (Spec A11).
type Stats struct {
	Asset         string `json:"asset"`
	Rounds        int    `json:"rounds"`
	LastPrice     int64  `json:"last_price"`
	VWAP          int64  `json:"vwap"` // Integer volume-weighted average price
	High          int64  `json:"high"`
	Low           int64  `json:"low"`
	TotalVolume   int64  `json:"total_volume"`
	UnfilledRatio int64  `json:"unfilled_ratio_bps"` // In basis points (0..10000)
}

// IndicativeQuote represents pre-clear indicative price and volume (Spec A11).
type IndicativeQuote struct {
	Asset            string `json:"asset"`
	Round            uint64 `json:"round"`
	IndicativePrice  int64  `json:"indicative_price"`
	IndicativeVolume int64  `json:"indicative_volume"`
}

// DepthLevel represents aggregated quantity at a single price level without order owners (Spec A11).
type DepthLevel struct {
	Price int64 `json:"price"`
	Qty   int64 `json:"qty"`
}

// DepthBook represents aggregated bid/ask depth when pre_clear == "depth" (Spec A11).
type DepthBook struct {
	Asset string       `json:"asset"`
	Round uint64       `json:"round"`
	Bids  []DepthLevel `json:"bids"` // Sorted highest price first
	Asks  []DepthLevel `json:"asks"` // Sorted lowest price first
}

type trackedOrder struct {
	round     uint64
	asset     string
	qty       int64
	carryOver bool
	ttlRounds uint64
}

// Tracker maintains deterministic market data projections from ledger records.
type Tracker struct {
	tapeByAsset      map[string][]TapeEntry
	submittedByRound map[uint64]map[string]int64 // round -> asset -> total submitted qty
	openOrders       map[string]trackedOrder
	pendingClear     map[string]*TapeEntry
}

// NewTracker creates a new market data Tracker.
func NewTracker() *Tracker {
	return &Tracker{
		tapeByAsset:      make(map[string][]TapeEntry),
		submittedByRound: make(map[uint64]map[string]int64),
		openOrders:       make(map[string]trackedOrder),
		pendingClear:     make(map[string]*TapeEntry),
	}
}

// Apply updates the market data projection from a ledger record.
func (t *Tracker) Apply(rec ledger.Record) {
	switch rec.Type {
	case ledger.RecOrderAccepted:
		var b ledger.OrderAcceptedBody
		if err := json.Unmarshal(rec.Body, &b); err == nil {
			if t.submittedByRound[b.Round] == nil {
				t.submittedByRound[b.Round] = make(map[string]int64)
			}
			t.submittedByRound[b.Round][b.Asset] += b.Qty
			t.openOrders[b.OrderID] = trackedOrder{
				round:     b.Round,
				asset:     b.Asset,
				qty:       b.Qty,
				carryOver: b.CarryOver,
				ttlRounds: b.TTLRounds,
			}
		}
	case ledger.RecOrderCancelled:
		var b ledger.OrderCancelledBody
		if err := json.Unmarshal(rec.Body, &b); err == nil {
			if ord, ok := t.openOrders[b.OrderID]; ok {
				if t.submittedByRound[ord.round] != nil {
					t.submittedByRound[ord.round][ord.asset] -= ord.qty
					if t.submittedByRound[ord.round][ord.asset] < 0 {
						t.submittedByRound[ord.round][ord.asset] = 0
					}
				}
				delete(t.openOrders, b.OrderID)
			}
		}
	case ledger.RecOrderReplaced:
		var b ledger.OrderReplacedBody
		if err := json.Unmarshal(rec.Body, &b); err == nil {
			oldCarryOver := false
			oldTTL := uint64(0)
			if ord, ok := t.openOrders[b.OldOrderID]; ok {
				oldCarryOver = ord.carryOver
				oldTTL = ord.ttlRounds
				if t.submittedByRound[ord.round] != nil {
					t.submittedByRound[ord.round][ord.asset] -= ord.qty
					if t.submittedByRound[ord.round][ord.asset] < 0 {
						t.submittedByRound[ord.round][ord.asset] = 0
					}
				}
				delete(t.openOrders, b.OldOrderID)
			}
			if t.submittedByRound[b.Round] == nil {
				t.submittedByRound[b.Round] = make(map[string]int64)
			}
			t.submittedByRound[b.Round][b.Asset] += b.Qty
			t.openOrders[b.NewOrderID] = trackedOrder{
				round:     b.Round,
				asset:     b.Asset,
				qty:       b.Qty,
				carryOver: oldCarryOver,
				ttlRounds: oldTTL,
			}
		}
	case ledger.RecClear:
		var b ledger.ClearBody
		if err := json.Unmarshal(rec.Body, &b); err == nil {
			t.pendingClear[b.Asset] = &TapeEntry{
				Round:         b.Round,
				Asset:         b.Asset,
				ClearingPrice: b.ClearingPrice,
				Volume:        b.Volume,
				Fills:         []protocol.Fill{},
			}
		}
	case ledger.RecFill:
		var f protocol.Fill
		if err := json.Unmarshal(rec.Body, &f); err == nil {
			if entry, ok := t.pendingClear[f.Asset]; ok {
				entry.Fills = append(entry.Fills, f)
			}
		}
	case ledger.RecSettle:
		var b ledger.SettleBody
		_ = json.Unmarshal(rec.Body, &b)
		assets := make([]string, 0, len(t.pendingClear))
		for a := range t.pendingClear {
			assets = append(assets, a)
		}
		sort.Strings(assets)
		for _, a := range assets {
			t.tapeByAsset[a] = append(t.tapeByAsset[a], *t.pendingClear[a])
		}
		t.pendingClear = make(map[string]*TapeEntry)
		for id, ord := range t.openOrders {
			keep := false
			if ord.ttlRounds > 0 {
				roundsElapsed := uint64(1)
				if b.Round >= ord.round {
					roundsElapsed = b.Round - ord.round + 1
				}
				keep = roundsElapsed < ord.ttlRounds
			} else {
				keep = ord.carryOver || b.CarryOver
			}
			if !keep {
				delete(t.openOrders, id)
			}
		}
		// Evict submittedByRound entries older than 512 rounds to bound memory
		if b.Round > 512 {
			cutoff := b.Round - 512
			for r := range t.submittedByRound {
				if r < cutoff {
					delete(t.submittedByRound, r)
				}
			}
		}
	}
}

// Tape returns the last maxRounds tape entries for asset.
func (t *Tracker) Tape(asset string, maxRounds int) []TapeEntry {
	all := t.tapeByAsset[asset]
	if maxRounds <= 0 || maxRounds >= len(all) {
		out := make([]TapeEntry, len(all))
		copy(out, all)
		return out
	}
	out := make([]TapeEntry, maxRounds)
	copy(out, all[len(all)-maxRounds:])
	return out
}

// Candles returns the last maxRounds OHLCV candles for asset.
func (t *Tracker) Candles(asset string, maxRounds int) []Candle {
	entries := t.Tape(asset, maxRounds)
	candles := make([]Candle, 0, len(entries))
	for _, e := range entries {
		candles = append(candles, Candle{
			Round:  e.Round,
			Asset:  e.Asset,
			Open:   e.ClearingPrice,
			High:   e.ClearingPrice,
			Low:    e.ClearingPrice,
			Close:  e.ClearingPrice,
			Volume: e.Volume,
		})
	}
	return candles
}

// Stats computes market statistics over the last maxRounds cleared rounds for asset.
func (t *Tracker) Stats(asset string, maxRounds int) Stats {
	entries := t.Tape(asset, maxRounds)
	st := Stats{
		Asset:  asset,
		Rounds: len(entries),
	}
	if len(entries) == 0 {
		return st
	}
	var sumPV int64
	var totalSubmitted int64
	var totalExecutedSides int64

	for _, e := range entries {
		if e.ClearingPrice > 0 {
			st.LastPrice = e.ClearingPrice
			if st.High == 0 || e.ClearingPrice > st.High {
				st.High = e.ClearingPrice
			}
			if st.Low == 0 || e.ClearingPrice < st.Low {
				st.Low = e.ClearingPrice
			}
			sumPV += e.ClearingPrice * e.Volume
			st.TotalVolume += e.Volume
		}
		if t.submittedByRound[e.Round] != nil {
			totalSubmitted += t.submittedByRound[e.Round][asset]
		}
		totalExecutedSides += e.Volume * 2
	}
	if st.TotalVolume > 0 {
		st.VWAP = sumPV / st.TotalVolume
	}
	if totalSubmitted > 0 && totalSubmitted >= totalExecutedSides {
		unfilled := totalSubmitted - totalExecutedSides
		st.UnfilledRatio = (unfilled * 10000) / totalSubmitted
	}
	return st
}

// ComputeIndicative computes the indicative clearing price and volume for an open round without revealing owners.
func ComputeIndicative(gameID string, round uint64, asset string, orders []clearing.Order, prevPrice, refPrice, bandMin, bandMax int64) IndicativeQuote {
	res := clearing.Clear(clearing.ClearingInput{
		GameID:            gameID,
		Round:             round,
		Asset:             asset,
		Orders:            orders,
		PrevClearingPrice: prevPrice,
		ReferencePrice:    refPrice,
		BandMin:           bandMin,
		BandMax:           bandMax,
	})
	return IndicativeQuote{
		Asset:            asset,
		Round:            round,
		IndicativePrice:  res.ClearingPrice,
		IndicativeVolume: res.Volume,
	}
}

// ComputeDepth computes aggregated bid/ask quantities per price level without revealing order owners.
func ComputeDepth(round uint64, asset string, orders []clearing.Order) DepthBook {
	bidsMap := make(map[int64]int64)
	asksMap := make(map[int64]int64)
	for _, o := range orders {
		if o.Asset != asset || o.Qty <= 0 {
			continue
		}
		if o.Side == protocol.SideBuy {
			bidsMap[o.LimitPrice] += o.Qty
		} else {
			asksMap[o.LimitPrice] += o.Qty
		}
	}
	bids := make([]DepthLevel, 0, len(bidsMap))
	for p, q := range bidsMap {
		bids = append(bids, DepthLevel{Price: p, Qty: q})
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })

	asks := make([]DepthLevel, 0, len(asksMap))
	for p, q := range asksMap {
		asks = append(asks, DepthLevel{Price: p, Qty: q})
	}
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })

	return DepthBook{
		Asset: asset,
		Round: round,
		Bids:  bids,
		Asks:  asks,
	}
}
