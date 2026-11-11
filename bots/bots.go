package bots

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Bot defines a reference autonomous trading agent (Spec B5).
type Bot interface {
	Name() string
	Signer() protocol.Signer
	Step(ctx context.Context, eng *engine.Engine, asset string, timestamp int64) error
}

func submitEnv(ctx context.Context, eng *engine.Engine, signer protocol.Signer, msgType, nonce string, ts int64, payload any) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	rawP, _ := json.Marshal(payload)
	env := protocol.Envelope{
		Version:   "1.0",
		GameID:    eng.Manifest().GameID,
		From:      signer.AgentID(),
		To:        "server",
		Type:      msgType,
		Timestamp: ts,
		Nonce:     nonce,
		CorrID:    "c-" + nonce,
		Payload:   rawP,
	}
	_ = protocol.SignEnvelope(&env, signer)
	b, _ := json.Marshal(env)
	return eng.SubmitRawEnvelope(ctx, b)
}

// RegisterBot registers a bot with the engine if not already admitted.
func RegisterBot(ctx context.Context, eng *engine.Engine, b Bot, ts int64) error {
	snap := eng.StateSnapshot()
	if _, ok := snap.Agents[b.Signer().AgentID()]; ok {
		return nil
	}
	_, rej := submitEnv(ctx, eng, b.Signer(), protocol.TypeAgentRegister, "reg-"+b.Name(), ts, protocol.RegisterPayload{
		PubKey:      b.Signer().PublicKeyString(),
		DisplayName: b.Name(),
	})
	if rej != nil {
		return fmt.Errorf("register bot %s failed: %s (%s)", b.Name(), rej.Code, rej.Reason)
	}
	return nil
}

// 1. RandomBot places random limit buy or sell orders within the allowed price band (Spec B5).
type RandomBot struct {
	name   string
	signer protocol.Signer
	rng    *rand.Rand
	seq    uint64
}

func NewRandomBot(name string, signer protocol.Signer, seed int64) *RandomBot {
	return &RandomBot{
		name:   name,
		signer: signer,
		rng:    rand.New(rand.NewSource(seed)),
	}
}

func (b *RandomBot) Name() string             { return b.name }
func (b *RandomBot) Signer() protocol.Signer  { return b.signer }
func (b *RandomBot) Step(ctx context.Context, eng *engine.Engine, asset string, ts int64) error {
	m := eng.Manifest()
	st := eng.Stats(asset, 5)
	ref := st.LastPrice
	if ref <= 0 {
		ref = 10
	}
	minP, maxP := m.PriceBounds(ref)
	price := minP
	if maxP > minP {
		price = minP + int64(b.rng.Intn(int(maxP-minP+1)))
	}
	side := protocol.SideBuy
	if b.rng.Intn(2) == 1 {
		side = protocol.SideSell
	}
	qty := int64(1 + b.rng.Intn(3))
	b.seq++
	nonce := fmt.Sprintf("rnd_%d", b.seq)
	submitEnv(ctx, eng, b.signer, protocol.TypePlaceOrder, nonce, ts, protocol.PlaceOrderPayload{
		Asset:      asset,
		Side:       side,
		LimitPrice: price,
		Qty:        qty,
	})
	return nil
}

// 2. MarketMakerBot posts both a bid below mid/last price and an ask above mid/last price (non-crossing, Spec B5).
type MarketMakerBot struct {
	name   string
	signer protocol.Signer
	spread int64
	qty    int64
	seq    uint64
}

func NewMarketMakerBot(name string, signer protocol.Signer, spread, qty int64) *MarketMakerBot {
	if spread < 1 {
		spread = 1
	}
	if qty < 1 {
		qty = 2
	}
	return &MarketMakerBot{name: name, signer: signer, spread: spread, qty: qty}
}

func (b *MarketMakerBot) Name() string            { return b.name }
func (b *MarketMakerBot) Signer() protocol.Signer { return b.signer }
func (b *MarketMakerBot) Step(ctx context.Context, eng *engine.Engine, asset string, ts int64) error {
	m := eng.Manifest()
	st := eng.Stats(asset, 5)
	mid := st.LastPrice
	if mid <= 0 {
		mid = 8
	}
	minP, maxP := m.PriceBounds(10)
	bid := mid - b.spread
	if bid < minP {
		bid = minP
	}
	ask := mid + b.spread
	if ask > maxP {
		ask = maxP
	}
	if bid >= ask {
		bid = ask - 1
	}
	if bid >= minP {
		b.seq++
		submitEnv(ctx, eng, b.signer, protocol.TypePlaceOrder, fmt.Sprintf("mm_bid_%d", b.seq), ts, protocol.PlaceOrderPayload{
			Asset: asset, Side: protocol.SideBuy, LimitPrice: bid, Qty: b.qty,
		})
	}
	if ask <= maxP && ask > bid {
		b.seq++
		submitEnv(ctx, eng, b.signer, protocol.TypePlaceOrder, fmt.Sprintf("mm_ask_%d", b.seq), ts, protocol.PlaceOrderPayload{
			Asset: asset, Side: protocol.SideSell, LimitPrice: ask, Qty: b.qty,
		})
	}
	return nil
}

// 3. TrendFollowerBot buys when recent clearing price >= VWAP and sells when recent clearing price < VWAP (Spec B5).
type TrendFollowerBot struct {
	name   string
	signer protocol.Signer
	qty    int64
	seq    uint64
}

func NewTrendFollowerBot(name string, signer protocol.Signer, qty int64) *TrendFollowerBot {
	if qty <= 0 {
		qty = 2
	}
	return &TrendFollowerBot{name: name, signer: signer, qty: qty}
}

func (b *TrendFollowerBot) Name() string            { return b.name }
func (b *TrendFollowerBot) Signer() protocol.Signer { return b.signer }
func (b *TrendFollowerBot) Step(ctx context.Context, eng *engine.Engine, asset string, ts int64) error {
	st := eng.Stats(asset, 5)
	side := protocol.SideBuy
	price := int64(9)
	if st.LastPrice > 0 && st.VWAP > 0 {
		if st.LastPrice >= st.VWAP {
			side = protocol.SideBuy
			price = st.LastPrice
		} else {
			side = protocol.SideSell
			price = st.LastPrice
		}
	}
	b.seq++
	submitEnv(ctx, eng, b.signer, protocol.TypePlaceOrder, fmt.Sprintf("tf_%d", b.seq), ts, protocol.PlaceOrderPayload{
		Asset: asset, Side: side, LimitPrice: price, Qty: b.qty,
	})
	return nil
}

// 4. ManipulatorBot posts a bearish or bullish statement on the public board and then trades in the opposite direction (Spec B5).
type ManipulatorBot struct {
	name   string
	signer protocol.Signer
	qty    int64
	seq    uint64
}

func NewManipulatorBot(name string, signer protocol.Signer, qty int64) *ManipulatorBot {
	if qty <= 0 {
		qty = 3
	}
	return &ManipulatorBot{name: name, signer: signer, qty: qty}
}

func (b *ManipulatorBot) Name() string            { return b.name }
func (b *ManipulatorBot) Signer() protocol.Signer { return b.signer }
func (b *ManipulatorBot) Step(ctx context.Context, eng *engine.Engine, asset string, ts int64) error {
	b.seq++
	// Post bearish talk on the board
	submitEnv(ctx, eng, b.signer, protocol.TypeBoardPost, fmt.Sprintf("manip_post_%d", b.seq), ts, protocol.BoardPostPayload{
		Asset:  asset,
		Stance: "bear",
		Body:   fmt.Sprintf("%s is completely overpriced right now, expecting a drop!", asset),
	})
	// Then place a buy order in the opposite direction
	submitEnv(ctx, eng, b.signer, protocol.TypePlaceOrder, fmt.Sprintf("manip_order_%d", b.seq), ts, protocol.PlaceOrderPayload{
		Asset:      asset,
		Side:       protocol.SideBuy,
		LimitPrice: 9,
		Qty:        b.qty,
	})
	return nil
}
