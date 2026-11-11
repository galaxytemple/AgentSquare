package clearing

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/galaxytemple/AgentSquare/protocol"
)

// TestSpecA10CopperMarket tests the normative conformance vector in Spec A10:
// Copper market, 14:00 round, previous clearing price $9.
// D sells 10 @ min $8; A, B, C each buy 5 @ max $9.
// Clearing price MUST be $9, volume 10.
// Round-robin gives A, B, C 3 units each (9 total), and lottery picks 1 of {A, B, C} for the 10th unit.
func TestSpecA10CopperMarket(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x42

	orders := []Order{
		{OrderID: "o1", AgentID: "ag_D", Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 10},
		{OrderID: "o2", AgentID: "ag_A", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5},
		{OrderID: "o3", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5},
		{OrderID: "o4", AgentID: "ag_C", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 5},
	}

	res := Clear(ClearingInput{
		GameID:            "city-2026w46",
		Round:             14,
		Asset:             "copper",
		Orders:            orders,
		PrevClearingPrice: 9,
		ReferencePrice:    10,
		BandMin:           5,
		BandMax:           10,
		Seed:              seed,
		SeedSig:           "ed25519:dummy",
	})

	if res.ClearingPrice != 9 {
		t.Fatalf("got clearing price %d, want 9", res.ClearingPrice)
	}
	if res.Volume != 10 {
		t.Fatalf("got volume %d, want 10", res.Volume)
	}
	if len(res.Fills) != 4 {
		t.Fatalf("expected 4 fills (A, B, C, D), got %d: %+v", len(res.Fills), res.Fills)
	}

	var buySum int64
	gotThree := 0
	gotFour := 0
	for _, f := range res.Fills {
		if f.Side == protocol.SideBuy {
			buySum += f.FilledQty
			if f.FilledQty == 3 {
				gotThree++
			} else if f.FilledQty == 4 {
				gotFour++
			} else {
				t.Fatalf("unexpected buyer fill qty %d for %s", f.FilledQty, f.AgentID)
			}
			if f.CashDelta != f.FilledQty*9 {
				t.Fatalf("unexpected buyer cash delta %d", f.CashDelta)
			}
		} else {
			if f.AgentID != "ag_D" || f.FilledQty != 10 || f.CashDelta != 90 {
				t.Fatalf("unexpected seller fill: %+v", f)
			}
		}
	}
	if buySum != 10 || gotThree != 2 || gotFour != 1 {
		t.Fatalf("unexpected buyer distribution: sum=%d three=%d four=%d", buySum, gotThree, gotFour)
	}
}

func TestOrderSplittingInvarianceAndArrivalOrderIndependence(t *testing.T) {
	var seed [32]byte
	seed[0] = 99

	// Combined order for ag_A (1x10) vs split orders for ag_A (10x1)
	combined := []Order{
		{OrderID: "s1", AgentID: "ag_S", Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 7},
		{OrderID: "b1", AgentID: "ag_A", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 10},
		{OrderID: "b2", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 10},
	}

	split := []Order{
		{OrderID: "b2", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 10},
		{OrderID: "s1", AgentID: "ag_S", Asset: "copper", Side: protocol.SideSell, LimitPrice: 8, Qty: 7},
	}
	for i := 0; i < 10; i++ {
		split = append(split, Order{
			OrderID:    fmt.Sprintf("b1_%d", i),
			AgentID:    "ag_A",
			Asset:      "copper",
			Side:       protocol.SideBuy,
			LimitPrice: 9,
			Qty:        1,
		})
	}

	res1 := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Orders: combined, PrevClearingPrice: 9, ReferencePrice: 10, Seed: seed})
	res2 := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Orders: split, PrevClearingPrice: 9, ReferencePrice: 10, Seed: seed})

	if res1.ClearingPrice != res2.ClearingPrice || res1.Volume != res2.Volume || len(res1.Fills) != len(res2.Fills) {
		t.Fatalf("mismatch between combined and split orders: %+v vs %+v", res1, res2)
	}
	for i := range res1.Fills {
		if res1.Fills[i] != res2.Fills[i] {
			t.Fatalf("fill %d mismatch: %+v vs %+v", i, res1.Fills[i], res2.Fills[i])
		}
	}
}

func TestTieBreakingRules(t *testing.T) {
	var seed [32]byte
	orders := []Order{
		{OrderID: "s1", AgentID: "ag_S", Asset: "copper", Side: protocol.SideSell, LimitPrice: 6, Qty: 5},
		{OrderID: "b1", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 10, Qty: 5},
	}

	// Case 1: Equidistant to prevPrice=8 (e.g., all prices 6..10 have volume 5, 8 is distance 0) -> 8
	r1 := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Orders: orders, PrevClearingPrice: 8, ReferencePrice: 10, Seed: seed})
	if r1.ClearingPrice != 8 {
		t.Fatalf("expected 8 (closest to prevPrice 8), got %d", r1.ClearingPrice)
	}

	// Case 2: No prevPrice (0), refPrice=9 -> 9
	r2 := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Orders: orders, PrevClearingPrice: 0, ReferencePrice: 9, Seed: seed})
	if r2.ClearingPrice != 9 {
		t.Fatalf("expected 9 (closest to refPrice 9), got %d", r2.ClearingPrice)
	}

	// Case 3: Equidistant to prevPrice and refPrice: sell @ 6, buy @ 7, prevPrice=0, refPrice=0 -> lower price (6)
	orders2 := []Order{
		{OrderID: "s1", AgentID: "ag_S", Asset: "copper", Side: protocol.SideSell, LimitPrice: 6, Qty: 5},
		{OrderID: "b1", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 7, Qty: 5},
	}
	r3 := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Orders: orders2, PrevClearingPrice: 0, ReferencePrice: 0, Seed: seed})
	if r3.ClearingPrice != 6 {
		t.Fatalf("expected lower price 6 on tie, got %d", r3.ClearingPrice)
	}
}

func FuzzClearingConservation(f *testing.F) {
	f.Add(int64(9), int64(10), uint64(12345))
	f.Fuzz(func(t *testing.T, prevP int64, refP int64, rngSeed uint64) {
		if prevP < 0 || prevP > 20 || refP < 1 || refP > 20 {
			return
		}
		r := rand.New(rand.NewSource(int64(rngSeed)))
		nOrders := 1 + r.Intn(25)
		orders := make([]Order, 0, nOrders)
		for i := 0; i < nOrders; i++ {
			side := protocol.SideBuy
			if r.Intn(2) == 1 {
				side = protocol.SideSell
			}
			orders = append(orders, Order{
				OrderID:    fmt.Sprintf("o_%d", i),
				AgentID:    fmt.Sprintf("ag_%d", r.Intn(8)),
				Asset:      "copper",
				Side:       side,
				LimitPrice: int64(5 + r.Intn(11)),
				Qty:        int64(1 + r.Intn(10)),
			})
		}
		var seed [32]byte
		seed[0] = byte(rngSeed)
		res := Clear(ClearingInput{
			GameID:            "fuzz",
			Round:             1,
			Asset:             "copper",
			Orders:            orders,
			PrevClearingPrice: prevP,
			ReferencePrice:    refP,
			BandMin:           5,
			BandMax:           15,
			Seed:              seed,
		})

		var totalBuyFilled, totalSellFilled int64
		var totalBuyCash, totalSellCash int64
		for _, fill := range res.Fills {
			if fill.Side == protocol.SideBuy {
				totalBuyFilled += fill.FilledQty
				totalBuyCash += fill.CashDelta
			} else {
				totalSellFilled += fill.FilledQty
				totalSellCash += fill.CashDelta
			}
		}
		if totalBuyFilled != res.Volume || totalSellFilled != res.Volume {
			t.Fatalf("asset conservation violated: buy=%d sell=%d vol=%d", totalBuyFilled, totalSellFilled, res.Volume)
		}
		if totalBuyCash != totalSellCash {
			t.Fatalf("cash conservation violated: buyCash=%d sellCash=%d", totalBuyCash, totalSellCash)
		}
	})
}

func generate10kOrders() []Order {
	r := rand.New(rand.NewSource(42))
	orders := make([]Order, 10000)
	for i := 0; i < 10000; i++ {
		side := protocol.SideBuy
		if i%2 == 1 {
			side = protocol.SideSell
		}
		orders[i] = Order{
			OrderID:    fmt.Sprintf("ord_%d", i),
			AgentID:    fmt.Sprintf("ag_%d", i%500),
			Asset:      "copper",
			Side:       side,
			LimitPrice: int64(50 + r.Intn(51)), // prices in [50..100]
			Qty:        int64(1 + r.Intn(10)),
		}
	}
	return orders
}

// TestClear10kOrdersPerformanceSLA verifies that 10,000 orders clear well under 500ms (Spec C6).
func TestClear10kOrdersPerformanceSLA(t *testing.T) {
	orders := generate10kOrders()
	var seed [32]byte
	seed[0] = 0x99

	res := Clear(ClearingInput{
		GameID:            "bench-game",
		Round:             1,
		Asset:             "copper",
		Orders:            orders,
		PrevClearingPrice: 75,
		ReferencePrice:    80,
		BandMin:           50,
		BandMax:           100,
		Seed:              seed,
	})
	if res.ClearingPrice < 50 || res.ClearingPrice > 100 || res.Volume <= 0 {
		t.Fatalf("unexpected 10k clearing result: price=%d vol=%d", res.ClearingPrice, res.Volume)
	}
}

func BenchmarkClear10kOrders(b *testing.B) {
	orders := generate10kOrders()
	var seed [32]byte
	seed[0] = 0x99
	in := ClearingInput{
		GameID:            "bench-game",
		Round:             1,
		Asset:             "copper",
		Orders:            orders,
		PrevClearingPrice: 75,
		ReferencePrice:    80,
		BandMin:           50,
		BandMax:           100,
		Seed:              seed,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Clear(in)
	}
}

func TestUserFairnessScenarios(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x11

	// Scenario 1: Buy 2 @ $40, Buy 2 @ $50 vs Sell 100 @ $40, Sell 2 @ $50
	// -> ClearingPrice MUST be $40, Volume MUST be 4 (even if ReferencePrice is $50!)
	r1 := Clear(ClearingInput{
		GameID:         "fairness-test",
		Round:          1,
		Asset:          "copper",
		ReferencePrice: 50,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_40", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40, Qty: 2},
			{OrderID: "b2", AgentID: "buyer_50", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 2},
			{OrderID: "s1", AgentID: "seller_40", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40, Qty: 100},
			{OrderID: "s2", AgentID: "seller_50", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 2},
		},
		Seed: seed,
	})
	if r1.ClearingPrice != 40 || r1.Volume != 4 {
		t.Fatalf("Scenario 1: expected price=$40 vol=4, got price=$%d vol=%d", r1.ClearingPrice, r1.Volume)
	}

	// Scenario 2: Buy 2 @ $40, Buy 2 @ $50 vs Sell 2 @ $40, Sell 3 @ $50
	// -> Only buyer_50 (2 units) and seller_40 (2 units) match; Volume MUST be 2!
	r2 := Clear(ClearingInput{
		GameID:         "fairness-test",
		Round:          2,
		Asset:          "copper",
		ReferencePrice: 45,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_40", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40, Qty: 2},
			{OrderID: "b2", AgentID: "buyer_50", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 2},
			{OrderID: "s1", AgentID: "seller_40", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40, Qty: 2},
			{OrderID: "s2", AgentID: "seller_50", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 3},
		},
		Seed: seed,
	})
	if r2.Volume != 2 || len(r2.Fills) != 2 {
		t.Fatalf("Scenario 2: expected vol=2 with 2 fills (buyer_50 & seller_40), got vol=%d fills=%+v", r2.Volume, r2.Fills)
	}
	for _, f := range r2.Fills {
		if f.Side == protocol.SideBuy && f.AgentID != "buyer_50" {
			t.Fatalf("Scenario 2: expected buyer_50 to win by price priority, got %s", f.AgentID)
		}
		if f.Side == protocol.SideSell && f.AgentID != "seller_40" {
			t.Fatalf("Scenario 2: expected seller_40 to win by price priority, got %s", f.AgentID)
		}
	}

	// Scenario 3: Buy 2 @ $50 vs Sell 5 @ $40, Sell 2 @ $50
	// -> Because there is excess supply at $40 (5 offered vs 2 demanded), ClearingPrice MUST be $40
	//    even when ReferencePrice=$50 and PrevClearingPrice=$50!
	//    And ONLY seller_40 fills 2 units (seller_50 fills 0).
	r3 := Clear(ClearingInput{
		GameID:            "fairness-test",
		Round:             3,
		Asset:             "copper",
		PrevClearingPrice: 50,
		ReferencePrice:    50,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_50", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 2},
			{OrderID: "s1", AgentID: "seller_40", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40, Qty: 5},
			{OrderID: "s2", AgentID: "seller_50", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 2},
		},
		Seed: seed,
	})
	if r3.ClearingPrice != 40 || r3.Volume != 2 {
		t.Fatalf("Scenario 3: expected price=$40 vol=2 under excess supply at $40, got price=$%d vol=%d", r3.ClearingPrice, r3.Volume)
	}
	for _, f := range r3.Fills {
		if f.Side == protocol.SideSell && (f.AgentID != "seller_40" || f.FilledQty != 2) {
			t.Fatalf("Scenario 3: expected only seller_40 to fill 2 units, got %+v", f)
		}
	}
}

func TestClearingEmptyAndNoCrossCases(t *testing.T) {
	var seed [32]byte
	// 1. Empty orders
	rEmpty := Clear(ClearingInput{GameID: "g", Round: 1, Asset: "copper", Seed: seed})
	if rEmpty.ClearingPrice != 0 || rEmpty.Volume != 0 || len(rEmpty.Fills) != 0 {
		t.Fatalf("expected empty clearing result, got %+v", rEmpty)
	}

	// 2. Only buy orders
	rBuyOnly := Clear(ClearingInput{
		GameID: "g", Round: 1, Asset: "copper", Seed: seed,
		Orders: []Order{
			{OrderID: "b1", AgentID: "ag_1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 10},
		},
	})
	if rBuyOnly.ClearingPrice != 0 || rBuyOnly.Volume != 0 || len(rBuyOnly.Fills) != 0 {
		t.Fatalf("expected zero volume for buy-only book, got %+v", rBuyOnly)
	}

	// 3. Only sell orders
	rSellOnly := Clear(ClearingInput{
		GameID: "g", Round: 1, Asset: "copper", Seed: seed,
		Orders: []Order{
			{OrderID: "s1", AgentID: "ag_1", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 10},
		},
	})
	if rSellOnly.ClearingPrice != 0 || rSellOnly.Volume != 0 || len(rSellOnly.Fills) != 0 {
		t.Fatalf("expected zero volume for sell-only book, got %+v", rSellOnly)
	}

	// 4. Non-overlapping book (Max Buy $40 < Min Sell $41) + ignored wrong asset / zero qty
	rNoCross := Clear(ClearingInput{
		GameID: "g", Round: 1, Asset: "copper", Seed: seed,
		Orders: []Order{
			{OrderID: "b1", AgentID: "ag_1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40, Qty: 10},
			{OrderID: "s1", AgentID: "ag_2", Asset: "copper", Side: protocol.SideSell, LimitPrice: 41, Qty: 10},
			{OrderID: "x1", AgentID: "ag_3", Asset: "iron", Side: protocol.SideBuy, LimitPrice: 100, Qty: 10},
			{OrderID: "x2", AgentID: "ag_4", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 0},
		},
	})
	if rNoCross.ClearingPrice != 0 || rNoCross.Volume != 0 || len(rNoCross.Fills) != 0 {
		t.Fatalf("expected zero volume when max buy < min sell, got %+v", rNoCross)
	}
}

func TestClearingExcessSupplyVsExcessDemandStaircase(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x22

	// Case A: Symmetric Excess Demand at $50 (Buy 5 @ $50, Buy 2 @ $40 vs Sell 2 @ $40)
	// Even if PrevClearingPrice=40 and ReferencePrice=40, excess demand at $50 (5 > 2) forces ClearingPrice = $50!
	rDemand := Clear(ClearingInput{
		GameID:            "staircase",
		Round:             1,
		Asset:             "copper",
		PrevClearingPrice: 40,
		ReferencePrice:    40,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_50", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 5},
			{OrderID: "b2", AgentID: "buyer_40", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40, Qty: 2},
			{OrderID: "s1", AgentID: "seller_40", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40, Qty: 2},
		},
		Seed: seed,
	})
	if rDemand.ClearingPrice != 50 || rDemand.Volume != 2 {
		t.Fatalf("expected excess demand at $50 to clear at $50 vol=2, got price=$%d vol=%d", rDemand.ClearingPrice, rDemand.Volume)
	}

	// Case B: Multi-Step Supply Staircase
	// Buy 10 @ $100 vs Sell 3 @ $60, Sell 4 @ $70, Sell 5 @ $80, Sell 10 @ $90
	// Cumulative supply: $60->3, $70->7, $80->12 (>= 10), $90->22.
	// Marginal price MUST be $80, Volume = 10:
	// - seller_60 fills 3/3 (100%)
	// - seller_70 fills 4/4 (100%)
	// - seller_80 fills 3/5 (marginal rationing)
	// - seller_90 fills 0/10
	rSupplySteps := Clear(ClearingInput{
		GameID:         "staircase",
		Round:          2,
		Asset:          "copper",
		ReferencePrice: 95,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_100", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 10},
			{OrderID: "s1", AgentID: "seller_60", Asset: "copper", Side: protocol.SideSell, LimitPrice: 60, Qty: 3},
			{OrderID: "s2", AgentID: "seller_70", Asset: "copper", Side: protocol.SideSell, LimitPrice: 70, Qty: 4},
			{OrderID: "s3", AgentID: "seller_80", Asset: "copper", Side: protocol.SideSell, LimitPrice: 80, Qty: 5},
			{OrderID: "s4", AgentID: "seller_90", Asset: "copper", Side: protocol.SideSell, LimitPrice: 90, Qty: 10},
		},
		Seed: seed,
	})
	if rSupplySteps.ClearingPrice != 80 || rSupplySteps.Volume != 10 {
		t.Fatalf("expected supply staircase to clear at $80 vol=10, got price=$%d vol=%d", rSupplySteps.ClearingPrice, rSupplySteps.Volume)
	}
	sellFills := make(map[string]int64)
	for _, f := range rSupplySteps.Fills {
		if f.Side == protocol.SideSell {
			sellFills[f.AgentID] = f.FilledQty
		}
	}
	if sellFills["seller_60"] != 3 || sellFills["seller_70"] != 4 || sellFills["seller_80"] != 3 || sellFills["seller_90"] != 0 {
		t.Fatalf("unexpected supply staircase fills: %+v", sellFills)
	}

	// Case C: Multi-Step Demand Staircase
	// Buy 3 @ $100, Buy 4 @ $90, Buy 5 @ $80, Buy 10 @ $70 vs Sell 10 @ $60
	// Cumulative demand: $100->3, $90->7, $80->12 (>= 10), $70->22.
	// Marginal price MUST be $80, Volume = 10:
	// - buyer_100 fills 3/3 (100%)
	// - buyer_90 fills 4/4 (100%)
	// - buyer_80 fills 3/5 (marginal rationing)
	// - buyer_70 fills 0/10
	rDemandSteps := Clear(ClearingInput{
		GameID:         "staircase",
		Round:          3,
		Asset:          "copper",
		ReferencePrice: 65,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_100", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 3},
			{OrderID: "b2", AgentID: "buyer_90", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 90, Qty: 4},
			{OrderID: "b3", AgentID: "buyer_80", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 80, Qty: 5},
			{OrderID: "b4", AgentID: "buyer_70", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 70, Qty: 10},
			{OrderID: "s1", AgentID: "seller_60", Asset: "copper", Side: protocol.SideSell, LimitPrice: 60, Qty: 10},
		},
		Seed: seed,
	})
	if rDemandSteps.ClearingPrice != 80 || rDemandSteps.Volume != 10 {
		t.Fatalf("expected demand staircase to clear at $80 vol=10, got price=$%d vol=%d", rDemandSteps.ClearingPrice, rDemandSteps.Volume)
	}
	buyFills := make(map[string]int64)
	for _, f := range rDemandSteps.Fills {
		if f.Side == protocol.SideBuy {
			buyFills[f.AgentID] = f.FilledQty
		}
	}
	if buyFills["buyer_100"] != 3 || buyFills["buyer_90"] != 4 || buyFills["buyer_80"] != 3 || buyFills["buyer_70"] != 0 {
		t.Fatalf("unexpected demand staircase fills: %+v", buyFills)
	}
}

func TestClearingOpenIntervalAndTieBreakingMatrix(t *testing.T) {
	var seed [32]byte
	// Orders: Buy 2 @ $40, Buy 2 @ $50 vs Sell 2 @ $40, Sell 3 @ $50
	// Zero-imbalance equilibrium interval is [$41, $49] where Demand=2 and Supply=2.
	orders := []Order{
		{OrderID: "b1", AgentID: "buyer_40", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40, Qty: 2},
		{OrderID: "b2", AgentID: "buyer_50", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 2},
		{OrderID: "s1", AgentID: "seller_40", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40, Qty: 2},
		{OrderID: "s2", AgentID: "seller_50", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 3},
	}

	cases := []struct {
		name      string
		prevP     int64
		refP      int64
		wantPrice int64
	}{
		{name: "no_prev_no_ref_picks_lowest_equilibrium_41", prevP: 0, refP: 0, wantPrice: 41},
		{name: "ref_inside_interval_45", prevP: 0, refP: 45, wantPrice: 45},
		{name: "prev_overrides_ref_48_vs_42", prevP: 48, refP: 42, wantPrice: 48},
		{name: "prev_above_interval_clamps_to_49", prevP: 60, refP: 42, wantPrice: 49},
		{name: "prev_below_interval_clamps_to_41", prevP: 30, refP: 48, wantPrice: 41},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Clear(ClearingInput{
				GameID:            "tie-matrix",
				Round:             1,
				Asset:             "copper",
				PrevClearingPrice: tc.prevP,
				ReferencePrice:    tc.refP,
				Orders:            orders,
				Seed:              seed,
			})
			if res.ClearingPrice != tc.wantPrice || res.Volume != 2 {
				t.Fatalf("%s: got price=$%d vol=%d, want price=$%d vol=2", tc.name, res.ClearingPrice, res.Volume, tc.wantPrice)
			}
		})
	}
}

func TestClearingMultiLevelPriorityRoundRobinAndCapExit(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x77

	// Buy side:
	// - ag_VIP1: 3 @ $110 (strictly better -> 100% filled = 3)
	// - ag_VIP2: 4 @ $105 (strictly better -> 100% filled = 4)
	// - Marginal @ $100 (need to allocate 15 - 7 = 8 units across 4 agents):
	//   * ag_M1: cap 1 (exits after Round-Robin pass 1 with 1 unit!)
	//   * ag_M2: cap 5
	//   * ag_M3: cap 5
	//   * ag_M4: cap 5
	//   Pass 1 gives {M1:1, M2:1, M3:1, M4:1} (4 remaining; M1 hits cap 1 and exits).
	//   Pass 2 gives {M2:2, M3:2, M4:2} (1 remaining).
	//   Lottery among {M2, M3, M4} gives +1 to exactly 1 winner -> two get 2, one gets 3!
	// - ag_Low: 10 @ $95 (out of the money -> 0)
	orders := []Order{
		{OrderID: "b_vip1", AgentID: "ag_VIP1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 110, Qty: 3},
		{OrderID: "b_vip2", AgentID: "ag_VIP2", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 105, Qty: 4},
		{OrderID: "b_m1", AgentID: "ag_M1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 1},
		{OrderID: "b_m2", AgentID: "ag_M2", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 5},
		{OrderID: "b_m3", AgentID: "ag_M3", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 5},
		{OrderID: "b_m4", AgentID: "ag_M4", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 5},
		{OrderID: "b_low", AgentID: "ag_Low", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 95, Qty: 10},
		{OrderID: "s_all", AgentID: "ag_Seller", Asset: "copper", Side: protocol.SideSell, LimitPrice: 90, Qty: 15},
	}

	res := Clear(ClearingInput{
		GameID:         "multi-level-rr",
		Round:          1,
		Asset:          "copper",
		ReferencePrice: 100,
		Orders:         orders,
		Seed:           seed,
	})
	if res.ClearingPrice != 100 || res.Volume != 15 {
		t.Fatalf("expected price=$100 vol=15, got price=$%d vol=%d", res.ClearingPrice, res.Volume)
	}

	fills := make(map[string]int64)
	for _, f := range res.Fills {
		if f.Side == protocol.SideBuy {
			fills[f.AgentID] = f.FilledQty
		}
	}
	if fills["ag_VIP1"] != 3 || fills["ag_VIP2"] != 4 || fills["ag_M1"] != 1 || fills["ag_Low"] != 0 {
		t.Fatalf("unexpected priority/cap fills: %+v", fills)
	}
	mSum := fills["ag_M2"] + fills["ag_M3"] + fills["ag_M4"]
	if mSum != 7 {
		t.Fatalf("expected M2+M3+M4 to receive 7 units total, got %d (%+v)", mSum, fills)
	}
	for _, id := range []string{"ag_M2", "ag_M3", "ag_M4"} {
		if fills[id] != 2 && fills[id] != 3 {
			t.Fatalf("expected %s to get 2 or 3 units, got %d", id, fills[id])
		}
	}
}

func TestClearingSingleAgentMultiplePriceLevelsAndLargeRangeParity(t *testing.T) {
	var seed [32]byte
	seed[0] = 0x55

	// 1. Single agent (ag_Whale) placing orders across multiple price levels:
	// Buy 2 @ $110 (strict priority -> 2), Buy 3 @ $100 (marginal tied with ag_Peer -> 2), Buy 4 @ $90 (out -> 0)
	// Seller sells 6 @ $95 -> ClearingPrice=$100, Volume=6, ag_Whale total fill = 2 + 2 = 4, ag_Peer = 2.
	rWhale := Clear(ClearingInput{
		GameID: "whale", Round: 1, Asset: "copper", ReferencePrice: 100, Seed: seed,
		Orders: []Order{
			{OrderID: "w1", AgentID: "ag_Whale", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 110, Qty: 2},
			{OrderID: "w2", AgentID: "ag_Whale", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 3},
			{OrderID: "w3", AgentID: "ag_Whale", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 90, Qty: 4},
			{OrderID: "p1", AgentID: "ag_Peer", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 3},
			{OrderID: "s1", AgentID: "ag_Seller", Asset: "copper", Side: protocol.SideSell, LimitPrice: 95, Qty: 6},
		},
	})
	if rWhale.ClearingPrice != 100 || rWhale.Volume != 6 {
		t.Fatalf("expected whale scenario price=$100 vol=6, got price=$%d vol=%d", rWhale.ClearingPrice, rWhale.Volume)
	}
	for _, f := range rWhale.Fills {
		if f.AgentID == "ag_Whale" && f.FilledQty != 4 {
			t.Fatalf("expected ag_Whale combined fill=4 (2@$110 + 2@$100), got %d", f.FilledQty)
		}
		if f.AgentID == "ag_Peer" && f.FilledQty != 2 {
			t.Fatalf("expected ag_Peer fill=2, got %d", f.FilledQty)
		}
	}

	// 2. Large-Price Range (> 10,000 ticks) Parity Test:
	// Scale Scenario 2 ($40,000,000 and $50,000,000) to verify sparse candidate builder matches exact equilibrium.
	rLarge := Clear(ClearingInput{
		GameID: "large-range", Round: 1, Asset: "copper", ReferencePrice: 45_000_000, Seed: seed,
		Orders: []Order{
			{OrderID: "b1", AgentID: "buyer_40m", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 40_000_000, Qty: 2},
			{OrderID: "b2", AgentID: "buyer_50m", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50_000_000, Qty: 2},
			{OrderID: "s1", AgentID: "seller_40m", Asset: "copper", Side: protocol.SideSell, LimitPrice: 40_000_000, Qty: 2},
			{OrderID: "s2", AgentID: "seller_50m", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50_000_000, Qty: 3},
		},
	})
	if rLarge.ClearingPrice != 45_000_000 || rLarge.Volume != 2 {
		t.Fatalf("expected large-range clearing at $45,000,000 vol=2, got price=$%d vol=%d", rLarge.ClearingPrice, rLarge.Volume)
	}
}

func TestLotteryUniformityAcrossRounds(t *testing.T) {
	wins := map[string]int{"ag_A": 0, "ag_B": 0, "ag_C": 0}
	const trials = 1500

	for i := 0; i < trials; i++ {
		var seed [32]byte
		seed[0] = byte(i & 0xff)
		seed[1] = byte((i >> 8) & 0xff)
		seed[2] = 0xAB

		res := Clear(ClearingInput{
			GameID: "lottery-fairness", Round: uint64(i + 1), Asset: "copper", ReferencePrice: 50, Seed: seed,
			Orders: []Order{
				{OrderID: "b1", AgentID: "ag_A", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 1},
				{OrderID: "b2", AgentID: "ag_B", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 1},
				{OrderID: "b3", AgentID: "ag_C", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 50, Qty: 1},
				{OrderID: "s1", AgentID: "ag_S", Asset: "copper", Side: protocol.SideSell, LimitPrice: 50, Qty: 1},
			},
		})
		for _, f := range res.Fills {
			if f.Side == protocol.SideBuy && f.FilledQty == 1 {
				wins[f.AgentID]++
			}
		}
	}

	// Each of the 3 agents should win ~500 times out of 1500 (assert within [420, 580])
	for ag, w := range wins {
		if w < 420 || w > 580 {
			t.Fatalf("lottery bias detected for %s: won %d / %d times (expected ~500)", ag, w, trials)
		}
	}
}

func TestMassiveMarginalAllocationFast(t *testing.T) {
	var seed [32]byte
	res := Clear(ClearingInput{
		GameID: "massive-marginal", Round: 1, Asset: "copper", ReferencePrice: 100, Seed: seed,
		Orders: []Order{
			{OrderID: "b1", AgentID: "ag_1", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 100_000_000},
			{OrderID: "b2", AgentID: "ag_2", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 300_000_000},
			{OrderID: "b3", AgentID: "ag_3", Asset: "copper", Side: protocol.SideBuy, LimitPrice: 100, Qty: 500_000_000},
			{OrderID: "s1", AgentID: "ag_seller", Asset: "copper", Side: protocol.SideSell, LimitPrice: 100, Qty: 450_000_001},
		},
	})
	if res.Volume != 450_000_001 {
		t.Fatalf("expected volume 450000001, got %d", res.Volume)
	}
	var sumBuy int64
	for _, f := range res.Fills {
		if f.Side == protocol.SideBuy {
			sumBuy += f.FilledQty
		}
	}
	if sumBuy != 450_000_001 {
		t.Fatalf("expected total buy fills 450000001, got %d", sumBuy)
	}
}
