package clearing

import (
	"sort"

	"github.com/galaxytemple/AgentSquare/protocol"
)

// Order represents an accepted open order entering batch call-auction clearing.
type Order struct {
	OrderID    string        `json:"order_id"`
	AgentID    string        `json:"agent_id"`
	Asset      string        `json:"asset"`
	Side       protocol.Side `json:"side"`
	LimitPrice int64         `json:"limit_price"`
	Qty        int64         `json:"qty"`
}

// AggregatedBucket represents merged quantity for a single (AgentID, Asset, Side, LimitPrice).
// Spec A9.2: The allocation unit is the agent, not the individual order.
type AggregatedBucket struct {
	AgentID    string        `json:"agent_id"`
	Asset      string        `json:"asset"`
	Side       protocol.Side `json:"side"`
	LimitPrice int64         `json:"limit_price"`
	Qty        int64         `json:"qty"`
}

// PriceEvaluation captures supply/demand/volume at a candidate price for Round Anatomy inspection.
type PriceEvaluation struct {
	Price  int64 `json:"price"`
	Demand int64 `json:"demand"`
	Supply int64 `json:"supply"`
	Volume int64 `json:"volume"`
}

// LotteryStep captures deterministic lottery details for verification and the viewer's Round Anatomy view.
type LotteryStep struct {
	Side              protocol.Side `json:"side"`
	Domain            string        `json:"domain"`
	RemainingToRation int64         `json:"remaining_to_ration"`
	CandidatesSorted  []string      `json:"candidates_sorted"`
	CandidatesShuffled []string     `json:"candidates_shuffled"`
	Winners           []string      `json:"winners"`
	PRNGCounterUsed   uint64        `json:"prng_counter_used"`
}

// ClearingInput encapsulates all pure inputs required to clear one asset for one round.
type ClearingInput struct {
	GameID            string   `json:"game_id"`
	Round             uint64   `json:"round"`
	Asset             string   `json:"asset"`
	Orders            []Order  `json:"orders"`
	PrevClearingPrice int64    `json:"prev_clearing_price"` // 0 if no previous clearing price
	ReferencePrice    int64    `json:"reference_price"`
	BandMin           int64    `json:"band_min"`
	BandMax           int64    `json:"band_max"`
	Seed              [32]byte `json:"-"`
	SeedSig           string   `json:"seed_sig"`
}

// ClearingResult contains the deterministic output of Clear().
type ClearingResult struct {
	GameID             string             `json:"game_id"`
	Round              uint64             `json:"round"`
	Asset              string             `json:"asset"`
	ClearingPrice      int64              `json:"clearing_price"`
	Volume             int64              `json:"volume"`
	TotalDemandAtClear int64              `json:"total_demand_at_clear"`
	TotalSupplyAtClear int64              `json:"total_supply_at_clear"`
	SeedSig            string             `json:"seed_sig"`
	Fills              []protocol.Fill    `json:"fills"`
	AggregatedBuckets  []AggregatedBucket `json:"aggregated_buckets"`
	PriceCurve         []PriceEvaluation  `json:"price_curve,omitempty"`
	Lotteries          []LotteryStep      `json:"lotteries,omitempty"`
}

type aggKey struct {
	agentID string
	side    protocol.Side
	price   int64
}

// Clear executes the normative batch call-auction clearing algorithm (Spec A9).
// It is a pure function with zero I/O, and arrival order of input.Orders has zero effect on the output.
func Clear(in ClearingInput) ClearingResult {
	res := ClearingResult{
		GameID:            in.GameID,
		Round:             in.Round,
		Asset:             in.Asset,
		SeedSig:           in.SeedSig,
		Fills:             []protocol.Fill{},
		AggregatedBuckets: []AggregatedBucket{},
	}

	// Step 1: Filter orders for this asset with positive qty and aggregate per (agent, side, price).
	bucketMap := make(map[aggKey]int64)
	var minPrice int64 = -1
	var maxPrice int64 = -1

	for _, o := range in.Orders {
		if o.Asset != in.Asset || o.Qty <= 0 {
			continue
		}
		k := aggKey{agentID: o.AgentID, side: o.Side, price: o.LimitPrice}
		bucketMap[k] += o.Qty
		if minPrice == -1 || o.LimitPrice < minPrice {
			minPrice = o.LimitPrice
		}
		if maxPrice == -1 || o.LimitPrice > maxPrice {
			maxPrice = o.LimitPrice
		}
	}

	if len(bucketMap) == 0 {
		return res
	}

	// Sort aggregated buckets deterministically by (side, price, agent_id)
	buckets := make([]AggregatedBucket, 0, len(bucketMap))
	for k, qty := range bucketMap {
		buckets = append(buckets, AggregatedBucket{
			AgentID:    k.agentID,
			Asset:      in.Asset,
			Side:       k.side,
			LimitPrice: k.price,
			Qty:        qty,
		})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Side != buckets[j].Side {
			return buckets[i].Side < buckets[j].Side
		}
		if buckets[i].LimitPrice != buckets[j].LimitPrice {
			return buckets[i].LimitPrice < buckets[j].LimitPrice
		}
		return buckets[i].AgentID < buckets[j].AgentID
	})
	res.AggregatedBuckets = buckets

	// Determine candidate price range
	lowP := minPrice
	highP := maxPrice
	if in.BandMin > 0 && in.BandMin > lowP {
		lowP = in.BandMin
	}
	if in.BandMax > 0 && in.BandMax < highP {
		highP = in.BandMax
	}
	if lowP > highP {
		return res
	}

	// Step 2 & 3: Evaluate executable volume across candidate integer prices and select uniform clearing price.
	var bestVolume int64 = 0
	var bestStrictOverflow int64 = -1
	var bestImbalance int64 = -1
	var bestPrice int64 = 0
	var bestDemand int64 = 0
	var bestSupply int64 = 0

	// If the price range is small enough (<= 10000 ticks), evaluate every integer tick in [lowP, highP].
	// If the price range is huge (e.g. near 2^53 boundary test vectors), evaluate all distinct order prices,
	// midpoints, prevPrice, and refPrice within [lowP, highP] so clearing remains O(U log U) and fast.
	candidatePrices := buildCandidatePrices(buckets, lowP, highP, in.PrevClearingPrice, in.ReferencePrice)

	curve := make([]PriceEvaluation, 0, len(candidatePrices))
	for _, p := range candidatePrices {
		var demand, supply int64
		var strictBuy, strictSell int64
		for _, b := range buckets {
			if b.Side == protocol.SideBuy && b.LimitPrice >= p {
				demand += b.Qty
				if b.LimitPrice > p {
					strictBuy += b.Qty
				}
			} else if b.Side == protocol.SideSell && b.LimitPrice <= p {
				supply += b.Qty
				if b.LimitPrice < p {
					strictSell += b.Qty
				}
			}
		}
		vol := demand
		if supply < vol {
			vol = supply
		}
		curve = append(curve, PriceEvaluation{
			Price:  p,
			Demand: demand,
			Supply: supply,
			Volume: vol,
		})
		if vol == 0 {
			continue
		}
		var strictOverflow int64
		if strictBuy > vol {
			strictOverflow += strictBuy - vol
		}
		if strictSell > vol {
			strictOverflow += strictSell - vol
		}
		imbalance := absDiff(demand, supply)

		isBetter := false
		if vol > bestVolume {
			isBetter = true
		} else if vol == bestVolume {
			if strictOverflow != bestStrictOverflow {
				isBetter = strictOverflow < bestStrictOverflow
			} else if imbalance != bestImbalance {
				isBetter = imbalance < bestImbalance
			} else {
				isBetter = isBetterTieBreak(p, bestPrice, in.PrevClearingPrice, in.ReferencePrice)
			}
		}
		if isBetter {
			bestVolume = vol
			bestStrictOverflow = strictOverflow
			bestImbalance = imbalance
			bestPrice = p
			bestDemand = demand
			bestSupply = supply
		}
	}
	res.PriceCurve = curve

	if bestVolume == 0 {
		return res
	}

	res.ClearingPrice = bestPrice
	res.Volume = bestVolume
	res.TotalDemandAtClear = bestDemand
	res.TotalSupplyAtClear = bestSupply

	// Step 4 & 5: Allocate on Buy side and Sell side independently.
	buyAlloc, buyLot := allocateSide(buckets, protocol.SideBuy, bestPrice, bestVolume, in.Seed, "buy")
	if buyLot != nil {
		res.Lotteries = append(res.Lotteries, *buyLot)
	}
	sellAlloc, sellLot := allocateSide(buckets, protocol.SideSell, bestPrice, bestVolume, in.Seed, "sell")
	if sellLot != nil {
		res.Lotteries = append(res.Lotteries, *sellLot)
	}

	// Build deterministic Fill records sorted by (side, agent_id)
	buyAgents := make([]string, 0, len(buyAlloc))
	for ag, qty := range buyAlloc {
		if qty > 0 {
			buyAgents = append(buyAgents, ag)
		}
	}
	sort.Strings(buyAgents)
	for _, ag := range buyAgents {
		qty := buyAlloc[ag]
		res.Fills = append(res.Fills, protocol.Fill{
			GameID:        in.GameID,
			Round:         in.Round,
			Asset:         in.Asset,
			AgentID:       ag,
			Side:          protocol.SideBuy,
			FilledQty:     qty,
			ClearingPrice: bestPrice,
			CashDelta:     qty * bestPrice,
		})
	}

	sellAgents := make([]string, 0, len(sellAlloc))
	for ag, qty := range sellAlloc {
		if qty > 0 {
			sellAgents = append(sellAgents, ag)
		}
	}
	sort.Strings(sellAgents)
	for _, ag := range sellAgents {
		qty := sellAlloc[ag]
		res.Fills = append(res.Fills, protocol.Fill{
			GameID:        in.GameID,
			Round:         in.Round,
			Asset:         in.Asset,
			AgentID:       ag,
			Side:          protocol.SideSell,
			FilledQty:     qty,
			ClearingPrice: bestPrice,
			CashDelta:     qty * bestPrice,
		})
	}

	return res
}

func buildCandidatePrices(buckets []AggregatedBucket, minP, maxP, prevP, refP int64) []int64 {
	if maxP-minP <= 10000 {
		out := make([]int64, 0, maxP-minP+1)
		for p := minP; p <= maxP; p++ {
			out = append(out, p)
		}
		return out
	}
	// For large numeric intervals (e.g. near 2^53), demand and supply are step functions
	// that only change at submitted limit prices. Within any constant-volume interval [L, R],
	// the tie-breaker optimal integer is one of: L, R, prevP (clamped to [L, R]), or refP (clamped to [L, R]).
	priceSet := make(map[int64]bool)
	var distinct []int64
	for _, b := range buckets {
		if b.LimitPrice < minP || b.LimitPrice > maxP {
			continue
		}
		if !priceSet[b.LimitPrice] {
			priceSet[b.LimitPrice] = true
			distinct = append(distinct, b.LimitPrice)
		}
	}
	if !priceSet[minP] {
		priceSet[minP] = true
		distinct = append(distinct, minP)
	}
	if !priceSet[maxP] {
		priceSet[maxP] = true
		distinct = append(distinct, maxP)
	}
	sort.Slice(distinct, func(i, j int) bool { return distinct[i] < distinct[j] })

	addCandidate := func(p int64) {
		if p >= minP && p <= maxP {
			priceSet[p] = true
		}
	}
	addCandidate(prevP)
	addCandidate(refP)
	for i := 0; i+1 < len(distinct); i++ {
		l, r := distinct[i], distinct[i+1]
		addCandidate(l + 1)
		addCandidate(r - 1)
		if prevP >= l && prevP <= r {
			addCandidate(prevP)
		}
		if refP >= l && refP <= r {
			addCandidate(refP)
		}
	}
	out := make([]int64, 0, len(priceSet))
	for p := range priceSet {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// isBetterTieBreak returns true if candidate price p beats currentBest at the same volume (Spec A9.3).
// 1. Closest to prevClearingPrice (if prevClearingPrice > 0)
// 2. Closest to referencePrice (if referencePrice > 0)
// 3. Lower price wins
func isBetterTieBreak(p, currentBest, prevPrice, refPrice int64) bool {
	if currentBest == 0 {
		return true
	}
	if prevPrice > 0 {
		dNew := absDiff(p, prevPrice)
		dCur := absDiff(currentBest, prevPrice)
		if dNew != dCur {
			return dNew < dCur
		}
	}
	if refPrice > 0 {
		dNew := absDiff(p, refPrice)
		dCur := absDiff(currentBest, refPrice)
		if dNew != dCur {
			return dNew < dCur
		}
	}
	return p < currentBest
}

func absDiff(a, b int64) int64 {
	if a >= b {
		return a - b
	}
	return b - a
}

// allocateSide implements Spec A9.4 & A9.5:
// - Strictly better price levels are filled in full in price priority order (highest-first for buy, lowest-first for sell).
// - At the marginal price level (normally clearingPrice) where remaining < levelSupply, active agents receive
//   1 unit per round-robin pass until remaining < len(active), then active agents are sorted by agent_id,
//   shuffled with PRNG(seed, domain), and the first `remaining` get +1.
func allocateSide(
	buckets []AggregatedBucket,
	side protocol.Side,
	clearingPrice int64,
	targetVolume int64,
	seed [32]byte,
	domain string,
) (map[string]int64, *LotteryStep) {
	alloc := make(map[string]int64)
	remaining := targetVolume
	if remaining <= 0 {
		return alloc, nil
	}

	// Group eligible buckets by price level
	byPrice := make(map[int64]map[string]int64)
	var levels []int64
	for _, b := range buckets {
		if b.Side != side || b.Qty <= 0 {
			continue
		}
		if side == protocol.SideBuy && b.LimitPrice < clearingPrice {
			continue
		}
		if side == protocol.SideSell && b.LimitPrice > clearingPrice {
			continue
		}
		if byPrice[b.LimitPrice] == nil {
			byPrice[b.LimitPrice] = make(map[string]int64)
			levels = append(levels, b.LimitPrice)
		}
		byPrice[b.LimitPrice][b.AgentID] += b.Qty
	}

	// Sort price levels: highest-first for Buy, lowest-first for Sell
	sort.Slice(levels, func(i, j int) bool {
		if side == protocol.SideBuy {
			return levels[i] > levels[j]
		}
		return levels[i] < levels[j]
	})

	var lotStep *LotteryStep

	for _, p := range levels {
		if remaining <= 0 {
			break
		}
		atLevelCap := byPrice[p]
		var levelTotal int64
		for _, q := range atLevelCap {
			levelTotal += q
		}

		// If remaining covers the entire price level, fill everyone at this price in full
		if remaining >= levelTotal {
			for ag, q := range atLevelCap {
				alloc[ag] += q
			}
			remaining -= levelTotal
			continue
		}

		// Otherwise ration `remaining` at this marginal price level via Round-Robin + Uniform Lottery
		active := make([]string, 0, len(atLevelCap))
		for ag, q := range atLevelCap {
			if q > 0 {
				active = append(active, ag)
			}
		}
		sort.Strings(active)
		atLevelAllocated := make(map[string]int64, len(active))

		for remaining > 0 && len(active) > 0 {
			nActive := int64(len(active))
			if remaining >= nActive {
				maxBatch := remaining / nActive
				minCapRem := maxBatch
				for _, ag := range active {
					capRem := atLevelCap[ag] - atLevelAllocated[ag]
					if capRem < minCapRem {
						minCapRem = capRem
					}
				}
				step := minCapRem
				if step < 1 {
					step = 1
				}
				nextActive := active[:0]
				for _, ag := range active {
					atLevelAllocated[ag] += step
					alloc[ag] += step
					remaining -= step
					if atLevelAllocated[ag] < atLevelCap[ag] {
						nextActive = append(nextActive, ag)
					}
				}
				active = nextActive
			} else {
				sort.Strings(active)
				sortedCopy := make([]string, len(active))
				copy(sortedCopy, active)

				shuffled := make([]string, len(active))
				copy(shuffled, active)
				prng := protocol.NewPRNG(seed, domain)
				prng.ShuffleStrings(shuffled)

				winners := make([]string, int(remaining))
				for i := int64(0); i < remaining; i++ {
					w := shuffled[i]
					atLevelAllocated[w]++
					alloc[w]++
					winners[i] = w
				}
				lotStep = &LotteryStep{
					Side:               side,
					Domain:             domain,
					RemainingToRation:  remaining,
					CandidatesSorted:   sortedCopy,
					CandidatesShuffled: shuffled,
					Winners:            winners,
					PRNGCounterUsed:    prng.Counter(),
				}
				remaining = 0
			}
		}
	}

	return alloc, lotStep
}
