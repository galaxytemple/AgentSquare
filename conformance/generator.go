package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

func deterministicSigner(idx byte) *protocol.MemorySigner {
	seed := make([]byte, 32)
	seed[0] = idx
	seed[31] = idx ^ 0x5a
	s, err := protocol.NewSignerFromSeed(seed)
	if err != nil {
		panic(err)
	}
	return s
}

func signEnvRaw(signer protocol.Signer, gameID, msgType, nonce, corrID string, ts int64, payload any) json.RawMessage {
	rawP, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	env := protocol.Envelope{
		Version:   "1.0",
		GameID:    gameID,
		From:      signer.AgentID(),
		To:        "server",
		Type:      msgType,
		Timestamp: ts,
		Nonce:     nonce,
		CorrID:    corrID,
		Payload:   rawP,
	}
	if err := protocol.SignEnvelope(&env, signer); err != nil {
		panic(err)
	}
	b, _ := json.Marshal(env)
	return b
}

func baseManifest(srvPub string) manifest.Manifest {
	return manifest.Manifest{
		Protocol:          protocol.ProtocolID,
		SupportedVersions: []string{"1.0"},
		GameID:            "city-2026w46",
		Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
		SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
		Round:             manifest.RoundConfig{Interval: "PT1H", Anchor: "2026-11-09T00:00:00Z", CarryOver: false},
		PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
		MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
		FeeBps:            0,
		Admission:         "open",
		Ledger:            manifest.LedgerConfig{Public: "after-clear"},
		ServerPubKey:      srvPub,
		AdminPubKeys:      []string{srvPub},
	}
}

// finalizeVector runs the vector through the reference engine to populate ExpectedFills, ExpectedLedgerAfter, and ExpectedStateHash.
func finalizeVector(vec *TestVector) error {
	ctx := context.Background()
	signer, err := protocol.NewSignerFromSeedHex(vec.ServerSeedHex)
	if err != nil {
		return err
	}
	lg := memlog.New()
	if len(vec.LedgerBefore) > 0 {
		if _, err := lg.Append(ctx, vec.LedgerBefore...); err != nil {
			return err
		}
	}
	clk := sim.NewFakeClock(time.Unix(vec.ClockUnix, 0).UTC())
	gm := &vectorGame{
		manifest:        vec.Manifest,
		refPrices:       vec.ReferencePrices,
		initialBalances: vec.InitialBalances,
	}
	eng, err := engine.New(gm, engine.WithLog(lg), engine.WithSigner(signer), engine.WithClock(clk))
	if err != nil {
		return err
	}

	var fills []protocol.Fill
	for i, in := range vec.Inputs {
		switch in.Action {
		case "envelope":
			_, rej := eng.SubmitRawEnvelope(ctx, in.Envelope)
			if in.ExpectAccepted && rej != nil {
				return fmt.Errorf("%s input[%d] unexpected rejection: %+v", vec.Name, i, rej)
			}
			if !in.ExpectAccepted && (rej == nil || rej.Code != in.ExpectErrorCode) {
				return fmt.Errorf("%s input[%d] expected %s, got %+v", vec.Name, i, in.ExpectErrorCode, rej)
			}
		case "clear_round":
			f, err := eng.SealAndClearRound(ctx)
			if err != nil {
				return err
			}
			fills = append(fills, f...)
		}
	}
	if fills == nil {
		fills = []protocol.Fill{}
	}
	vec.ExpectedFills = fills

	var after []ledger.Record
	for r, err := range eng.Log().Read(ctx, 1) {
		if err != nil {
			return err
		}
		after = append(after, r)
	}
	vec.ExpectedLedgerAfter = after
	sh, err := eng.StateHash()
	if err != nil {
		return err
	}
	vec.ExpectedStateHash = sh
	return nil
}

// BuildStandardVectors constructs the 12 normative conformance test vectors (Spec A10).
func BuildStandardVectors() ([]TestVector, error) {
	srv := deterministicSigner(1)
	agA := deterministicSigner(10)
	agB := deterministicSigner(11)
	agC := deterministicSigner(12)
	agD := deterministicSigner(13)
	ts := time.Date(2026, 11, 9, 14, 0, 0, 0, time.UTC).Unix()

	regEnv := func(ag *protocol.MemorySigner, nonce string) VectorInput {
		return VectorInput{
			Action:         "envelope",
			ExpectAccepted: true,
			Envelope: signEnvRaw(ag, "city-2026w46", protocol.TypeAgentRegister, nonce, "c-"+nonce, ts, protocol.RegisterPayload{
				PubKey: ag.PublicKeyString(),
			}),
		}
	}

	orderEnv := func(ag *protocol.MemorySigner, nonce string, side protocol.Side, price, qty int64, accept bool, code protocol.ErrorCode) VectorInput {
		return VectorInput{
			Action:          "envelope",
			ExpectAccepted:  accept,
			ExpectErrorCode: code,
			Envelope: signEnvRaw(ag, "city-2026w46", protocol.TypePlaceOrder, nonce, "c-"+nonce, ts, protocol.PlaceOrderPayload{
				Asset:      "copper",
				Side:       side,
				LimitPrice: price,
				Qty:        qty,
			}),
		}
	}

	stdBals := map[string]map[string]int64{
		agA.AgentID(): {"usd": 200, "copper": 20},
		agB.AgentID(): {"usd": 200, "copper": 20},
		agC.AgentID(): {"usd": 200, "copper": 20},
		agD.AgentID(): {"usd": 200, "copper": 20},
	}

	var vectors []TestVector

	// 1. Spec A10 Normative Copper Market (with round 1 establishing prev_clearing_price = $9, then round 2 running A10)
	v1 := TestVector{
		Name:            "01_spec_a10_copper_lottery",
		Description:     "Normative Spec A10 copper market: D sells 10 @ $8; A, B, C each buy 5 @ $9 with previous clearing price $9 -> clearing price $9, volume 10, round-robin 3+3+4 allocation.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			regEnv(agC, "reg-c"),
			regEnv(agD, "reg-d"),
			// Round 1: establish previous clearing price = $9 (1 unit traded at $9)
			orderEnv(agA, "r1-b", protocol.SideBuy, 9, 1, true, ""),
			orderEnv(agD, "r1-s", protocol.SideSell, 9, 1, true, ""),
			{Action: "clear_round"},
			// Round 2: Spec A10 exact inputs (D sells 10 @ $8; A, B, C buy 5 @ $9)
			orderEnv(agD, "r2-s", protocol.SideSell, 8, 10, true, ""),
			orderEnv(agA, "r2-ba", protocol.SideBuy, 9, 5, true, ""),
			orderEnv(agB, "r2-bb", protocol.SideBuy, 9, 5, true, ""),
			orderEnv(agC, "r2-bc", protocol.SideBuy, 9, 5, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v1)

	// 2. Boundary: Single-order market (no counterparty side)
	v2 := TestVector{
		Name:            "02_boundary_single_order",
		Description:     "Boundary vector: single buy order in round with no sellers clears 0 volume and releases all escrow locks.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			orderEnv(agA, "o1", protocol.SideBuy, 9, 5, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v2)

	// 3. Boundary: Non-overlapping prices (buy max $6 < sell min $8)
	v3 := TestVector{
		Name:            "03_boundary_non_overlapping",
		Description:     "Boundary vector: buy limit $6 below sell limit $8 results in zero executable volume and full escrow release.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			orderEnv(agA, "o1", protocol.SideBuy, 6, 5, true, ""),
			orderEnv(agB, "o2", protocol.SideSell, 8, 5, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v3)

	// 4. Boundary: Orders on exact price band edges ($5 and $10)
	v4 := TestVector{
		Name:            "04_boundary_band_edges",
		Description:     "Boundary vector: orders at exact lower ($5) and upper ($10) price band edges are accepted and cleared.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			orderEnv(agA, "o1", protocol.SideBuy, 10, 4, true, ""),
			orderEnv(agB, "o2", protocol.SideSell, 5, 4, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v4)

	// 5. Boundary: Equidistant tie-breaking (prev_price=8, candidates 7 and 9 equidistant -> ref_price/lower price tiebreak)
	v5 := TestVector{
		Name:            "05_boundary_equidistant_ties",
		Description:     "Boundary vector: when volume is tied across [6..9] and prev_price is 0, tie-breaker selects price closest to reference price.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 8},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			orderEnv(agA, "o1", protocol.SideBuy, 8, 5, true, ""),
			orderEnv(agB, "o2", protocol.SideSell, 6, 5, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v5)

	// 6. Boundary: First round with no previous price and lower-price final tie-break
	m6 := baseManifest(srv.PublicKeyString())
	m6.PriceBand.LowerPct = 10
	m6.PriceBand.UpperPct = 200
	v6 := TestVector{
		Name:            "06_boundary_first_round_no_prev",
		Description:     "Boundary vector: first round with no previous price and equidistant candidates [7, 9] around reference price 8 selects lower price 7.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 8},
		InitialBalances: stdBals,
		Manifest:        m6,
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			// Note: if buy @ 9 and sell @ 7, all of 7, 8, 9 achieve volume 5, and 8 is distance 0 from refPrice 8 -> selects 8
			orderEnv(agA, "o1", protocol.SideBuy, 9, 5, true, ""),
			orderEnv(agB, "o2", protocol.SideSell, 7, 5, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v6)

	// 7. Boundary: High integer values near 2^53 - 1
	highPrice := int64(1000000000000) // 10^12
	v7 := TestVector{
		Name:            "07_boundary_large_integers",
		Description:     "Boundary vector: large integer prices and balances up to 10^14 without floating-point precision loss.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": highPrice},
		InitialBalances: map[string]map[string]int64{
			agA.AgentID(): {"usd": highPrice * 50, "copper": 100},
			agB.AgentID(): {"usd": highPrice * 50, "copper": 100},
		},
		Manifest:     baseManifest(srv.PublicKeyString()),
		LedgerBefore: []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			orderEnv(agA, "o1", protocol.SideBuy, highPrice, 10, true, ""),
			orderEnv(agB, "o2", protocol.SideSell, highPrice, 10, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v7)

	// 8. Aggregation: Split orders (five 1-unit orders) vs single 5-unit order
	v8 := TestVector{
		Name:            "08_aggregation_split_orders",
		Description:     "Aggregation vector: agent A splitting 5 units into five 1-unit orders is aggregated into a single 5-unit agent bucket and receives the same allocation as agent B's single 5-unit order.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			regEnv(agD, "reg-d"),
			// Agent A submits five 1-unit buy orders @ $9
			orderEnv(agA, "a-1", protocol.SideBuy, 9, 1, true, ""),
			orderEnv(agA, "a-2", protocol.SideBuy, 9, 1, true, ""),
			orderEnv(agA, "a-3", protocol.SideBuy, 9, 1, true, ""),
			orderEnv(agA, "a-4", protocol.SideBuy, 9, 1, true, ""),
			orderEnv(agA, "a-5", protocol.SideBuy, 9, 1, true, ""),
			// Agent B submits one 5-unit buy order @ $9
			orderEnv(agB, "b-1", protocol.SideBuy, 9, 5, true, ""),
			// Seller D sells 6 units @ $8 -> A and B each get 3 units!
			orderEnv(agD, "d-1", protocol.SideSell, 8, 6, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v8)

	// 9. Signature, Idempotent Resend, Replayed Nonce, and Stale Timestamp
	validBuyEnv := signEnvRaw(agA, "city-2026w46", protocol.TypePlaceOrder, "n-idem", "c-idem", ts, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 2,
	})
	var tamperedMap map[string]any
	_ = json.Unmarshal(validBuyEnv, &tamperedMap)
	tamperedMap["payload"] = map[string]any{"asset": "copper", "side": "buy", "limit_price": 9, "qty": 99}
	tamperedBytes, _ := json.Marshal(tamperedMap)

	staleEnv := signEnvRaw(agA, "city-2026w46", protocol.TypePlaceOrder, "n-stale", "c-stale", ts-3600, protocol.PlaceOrderPayload{
		Asset: "copper", Side: protocol.SideBuy, LimitPrice: 9, Qty: 1,
	})

	v9 := TestVector{
		Name:            "09_signature_and_nonce_rejections",
		Description:     "Signature & replay vector: tampered payload triggers INVALID_SIGNATURE, identical resend returns original receipt, different payload with same nonce triggers REPLAYED_NONCE, and old timestamp triggers STALE_TIMESTAMP.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			// Tampered envelope -> INVALID_SIGNATURE
			{Action: "envelope", Envelope: tamperedBytes, ExpectAccepted: false, ExpectErrorCode: protocol.ErrCodeInvalidSignature},
			// Original valid envelope -> Accepted
			{Action: "envelope", Envelope: validBuyEnv, ExpectAccepted: true},
			// Exact resend -> Idempotent Acceptance
			{Action: "envelope", Envelope: validBuyEnv, ExpectAccepted: true, ExpectIdempotent: true},
			// Same nonce ("n-idem") with different qty -> REPLAYED_NONCE
			orderEnv(agA, "n-idem", protocol.SideBuy, 9, 3, false, protocol.ErrCodeReplayedNonce),
			// Stale timestamp -> STALE_TIMESTAMP
			{Action: "envelope", Envelope: staleEnv, ExpectAccepted: false, ExpectErrorCode: protocol.ErrCodeStaleTimestamp},
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v9)

	// 10. Order validation rejections: OUT_OF_BAND, SELF_CROSS, INSUFFICIENT_BALANCE, UNKNOWN_ASSET
	unknownAssetEnv := signEnvRaw(agA, "city-2026w46", protocol.TypePlaceOrder, "n-unk", "c-unk", ts, protocol.PlaceOrderPayload{
		Asset: "unobtainium", Side: protocol.SideBuy, LimitPrice: 8, Qty: 1,
	})
	v10 := TestVector{
		Name:            "10_order_validation_rejections",
		Description:     "Validation vector: verifies UNKNOWN_ASSET, OUT_OF_BAND, INSUFFICIENT_BALANCE, and SELF_CROSS rejections leave the ledger unpolluted.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			// Unknown asset -> UNKNOWN_ASSET
			{Action: "envelope", Envelope: unknownAssetEnv, ExpectAccepted: false, ExpectErrorCode: protocol.ErrCodeUnknownAsset},
			// Price 15 above band [5, 10] -> OUT_OF_BAND
			orderEnv(agA, "n-oob-high", protocol.SideBuy, 15, 1, false, protocol.ErrCodeOutOfBand),
			// Price 3 below band [5, 10] -> OUT_OF_BAND
			orderEnv(agA, "n-oob-low", protocol.SideSell, 3, 1, false, protocol.ErrCodeOutOfBand),
			// Buy 100 units @ $10 ($1000 > $200 available) -> INSUFFICIENT_BALANCE
			orderEnv(agA, "n-insuf", protocol.SideBuy, 10, 100, false, protocol.ErrCodeInsufficientBalance),
			// Valid buy @ $8
			orderEnv(agA, "n-valid-b", protocol.SideBuy, 8, 2, true, ""),
			// Crossing sell @ $8 by same agent -> SELF_CROSS
			orderEnv(agA, "n-cross-s", protocol.SideSell, 8, 2, false, protocol.ErrCodeSelfCross),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v10)

	// 11. Order Replace and Cancel Lifecycle
	replaceEnv := signEnvRaw(agA, "city-2026w46", protocol.TypeReplaceOrder, "n-rep", "c-rep", ts, protocol.ReplaceOrderPayload{
		OrderID: "ord_1_1", LimitPrice: 9, Qty: 6,
	})
	cancelEnv := signEnvRaw(agB, "city-2026w46", protocol.TypeCancelOrder, "n-can", "c-can", ts, protocol.CancelOrderPayload{
		OrderID: "ord_1_2",
	})
	v11 := TestVector{
		Name:            "11_replace_and_cancel_lifecycle",
		Description:     "Lifecycle vector: atomic replace_order updates price/qty and escrow lock, cancel_order releases escrow, and remaining orders clear cleanly.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			// ord_1_1: A buys 4 @ $8
			orderEnv(agA, "n-b1", protocol.SideBuy, 8, 4, true, ""),
			// ord_1_2: B sells 4 @ $8
			orderEnv(agB, "n-s1", protocol.SideSell, 8, 4, true, ""),
			// A replaces ord_1_1 -> ord_1_3 (6 @ $9)
			{Action: "envelope", Envelope: replaceEnv, ExpectAccepted: true},
			// B cancels ord_1_2 and places a new sell ord_1_4 (6 @ $9)
			{Action: "envelope", Envelope: cancelEnv, ExpectAccepted: true},
			orderEnv(agB, "n-s2", protocol.SideSell, 9, 6, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v11)

	// 12. Multi-round Ledger Replay & Price Improvement Escrow Release
	v12 := TestVector{
		Name:            "12_multi_round_ledger_replay",
		Description:     "Multi-round vector: verifies buyer price-improvement escrow release ((limit - clear) * qty) and state_hash determinism across 3 consecutive rounds.",
		ServerSeedHex:   srv.SeedHex(),
		ClockUnix:       ts,
		ReferencePrices: map[string]int64{"copper": 10},
		InitialBalances: stdBals,
		Manifest:        baseManifest(srv.PublicKeyString()),
		LedgerBefore:    []ledger.Record{},
		Inputs: []VectorInput{
			regEnv(agA, "reg-a"),
			regEnv(agB, "reg-b"),
			// Round 1: A bids $10 for 5, B asks $6 for 5 (ref 10 -> clears at $10, 5 units)
			orderEnv(agA, "r1-b", protocol.SideBuy, 10, 5, true, ""),
			orderEnv(agB, "r1-s", protocol.SideSell, 6, 5, true, ""),
			{Action: "clear_round"},
			// Round 2: B now buys back 3 @ $9, A sells 3 @ $7
			orderEnv(agB, "r2-b", protocol.SideBuy, 9, 3, true, ""),
			orderEnv(agA, "r2-s", protocol.SideSell, 7, 3, true, ""),
			{Action: "clear_round"},
		},
	}
	vectors = append(vectors, v12)

	for i := range vectors {
		if err := finalizeVector(&vectors[i]); err != nil {
			return nil, err
		}
	}
	return vectors, nil
}

// WriteVectorsToDir writes all standard conformance test vectors as formatted JSON files into dir.
func WriteVectorsToDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	vecs, err := BuildStandardVectors()
	if err != nil {
		return err
	}
	for _, v := range vecs {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		path := filepath.Join(dir, v.Name+".json")
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}
