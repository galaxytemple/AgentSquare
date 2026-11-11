package ledger

import (
	"encoding/json"
	"fmt"
	"iter"
	"sort"

	"github.com/galaxytemple/AgentSquare/clearing"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// RoundStatus tracks the lifecycle of a round.
type RoundStatus string

const (
	RoundStatusOpen    RoundStatus = "open"
	RoundStatusSealed  RoundStatus = "sealed"
	RoundStatusCleared RoundStatus = "cleared"
	RoundStatusSettled RoundStatus = "settled"
	RoundStatusVoided  RoundStatus = "voided"
)

// AgentState holds the replayed state of a registered agent.
type AgentState struct {
	AgentID     string           `json:"agent_id"`
	PubKey      string           `json:"pubkey"`
	DisplayName string           `json:"display_name,omitempty"`
	Available   map[string]int64 `json:"available"`
	Locked      map[string]int64 `json:"locked"`
}

// OpenOrder holds the state of an accepted, unfilled order.
type OpenOrder struct {
	OrderID    string        `json:"order_id"`
	Round      uint64        `json:"round"`
	AgentID    string        `json:"agent_id"`
	Asset      string        `json:"asset"`
	Side       protocol.Side `json:"side"`
	LimitPrice int64         `json:"limit_price"`
	Qty        int64         `json:"qty"`
	CarryOver  bool          `json:"carry_over,omitempty"`
	TTLRounds  uint64        `json:"ttl_rounds,omitempty"`
	LockedUnit string        `json:"locked_unit"`
	LockedAmt  int64         `json:"locked_amt"`
}

// ReplaySnapshot is the serializable deterministic state used to compute state_hash.
type ReplaySnapshot struct {
	GameID             string                `json:"game_id"`
	ManifestHash       string                `json:"manifest_hash"`
	LastSeq            uint64                `json:"last_seq"`
	LastHash           string                `json:"last_hash"`
	CurrentRound       uint64                `json:"current_round"`
	RoundStatus        RoundStatus           `json:"round_status"`
	HaltedGlobal       bool                  `json:"halted_global"`
	HaltedAssets       map[string]bool       `json:"halted_assets"`
	PrevClearingPrices map[string]int64      `json:"prev_clearing_prices"`
	Agents             map[string]*AgentState `json:"agents"`
	OpenOrders         map[string]*OpenOrder  `json:"open_orders"`
	FirstOpenOrderSeq  uint64                 `json:"-"`
	PreOrderStateHash  string                 `json:"-"`
}

type priceBand struct {
	min int64
	max int64
}

// State manages deterministic state transitions by replaying ledger records.
type State struct {
	Manifest           *manifest.Manifest
	GameID             string
	ManifestHash       string
	AdminPubKeys       []string
	LastSeq            uint64
	LastHash           string
	CurrentRound       uint64
	RoundStatus        RoundStatus
	HaltedGlobal       bool
	HaltedAssets       map[string]bool
	PrevClearingPrices map[string]int64
	Agents             map[string]*AgentState
	OpenOrders         map[string]*OpenOrder
	OrderInsertionKeys []string
	FirstOpenOrderSeq  uint64
	PreOrderStateHash  string

	// Transient clearing verification buffer per round
	pendingFillsByAsset  map[string][]protocol.Fill
	expectedFillsByAsset map[string][]protocol.Fill
	clearPriceByAsset    map[string]int64
	clearBandByAsset     map[string]priceBand
}

// NewState initializes an empty replay state for the given manifest.
func NewState(m *manifest.Manifest) *State {
	gameID := ""
	var adminKeys []string
	if m != nil {
		gameID = m.GameID
		adminKeys = append(adminKeys, m.AdminPubKeys...)
	}
	return &State{
		Manifest:             m,
		GameID:               gameID,
		AdminPubKeys:         adminKeys,
		LastHash:             ZeroHash,
		CurrentRound:         1,
		RoundStatus:          RoundStatusOpen,
		HaltedAssets:         make(map[string]bool),
		PrevClearingPrices:   make(map[string]int64),
		Agents:               make(map[string]*AgentState),
		OpenOrders:           make(map[string]*OpenOrder),
		pendingFillsByAsset:  make(map[string][]protocol.Fill),
		expectedFillsByAsset: make(map[string][]protocol.Fill),
		clearPriceByAsset:    make(map[string]int64),
		clearBandByAsset:     make(map[string]priceBand),
	}
}

// SettlementUnitID returns the settlement currency identifier (default "usd").
func (s *State) SettlementUnitID() string {
	if s.Manifest != nil && s.Manifest.SettlementUnit.ID != "" {
		return s.Manifest.SettlementUnit.ID
	}
	return "usd"
}

// Apply verifies and applies a single ledger record to the state.
func (s *State) Apply(rec Record) error {
	if rec.Seq != s.LastSeq+1 {
		return fmt.Errorf("sequence gap: got seq %d, expected %d", rec.Seq, s.LastSeq+1)
	}
	if rec.PrevHash != s.LastHash {
		return fmt.Errorf("seq %d: prev_hash mismatch: got %s, want %s", rec.Seq, rec.PrevHash, s.LastHash)
	}
	if s.GameID != "" && rec.GameID != s.GameID {
		return fmt.Errorf("seq %d: game_id mismatch: got %s, want %s", rec.Seq, rec.GameID, s.GameID)
	}

	serverPub := ""
	if s.Manifest != nil {
		serverPub = s.Manifest.ServerPubKey
	}
	if err := VerifyRecord(rec, serverPub); err != nil {
		return err
	}

	switch rec.Type {
	case RecGenesis:
		if rec.Seq != 1 {
			return fmt.Errorf("genesis record only allowed at seq 1, got %d", rec.Seq)
		}
		var b GenesisBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if s.Manifest != nil {
			mh, err := s.Manifest.Hash()
			if err != nil {
				return err
			}
			if b.ManifestHash != "" && b.ManifestHash != mh {
				return fmt.Errorf("genesis manifest_hash mismatch: got %s, want %s", b.ManifestHash, mh)
			}
		}
		s.ManifestHash = b.ManifestHash
		s.GameID = rec.GameID

	case RecAgentAdmitted:
		var b AgentAdmittedBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		derivedID, err := protocol.AgentIDFromPubKeyString(b.PubKey)
		if err != nil {
			return fmt.Errorf("seq %d: invalid agent pubkey: %w", rec.Seq, err)
		}
		if b.AgentID != derivedID {
			return fmt.Errorf("seq %d: agent_id %s does not match pubkey fingerprint %s", rec.Seq, b.AgentID, derivedID)
		}
		if _, exists := s.Agents[b.AgentID]; !exists {
			s.Agents[b.AgentID] = &AgentState{
				AgentID:     b.AgentID,
				PubKey:      b.PubKey,
				DisplayName: b.DisplayName,
				Available:   make(map[string]int64),
				Locked:      make(map[string]int64),
			}
		}

	case RecKeyRotate:
		var b KeyRotateBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		ag, isAgent := s.Agents[b.AgentID]
		if isAgent && ag.PubKey != b.OldPubKey {
			return fmt.Errorf("seq %d: old_pubkey mismatch for %s", rec.Seq, b.AgentID)
		}
		adminIdx := -1
		for i, ak := range s.AdminPubKeys {
			if ak == b.OldPubKey {
				adminIdx = i
				break
			}
		}
		if !isAgent && adminIdx < 0 {
			if s.Manifest != nil {
				return fmt.Errorf("seq %d: unknown agent or admin key %s in key.rotate", rec.Seq, b.AgentID)
			}
		}
		msg := []byte("rotate|" + b.AgentID + "|" + b.NewPubKey)
		if err := protocol.VerifySignature(b.NewPubKey, msg, b.NewSig); err != nil {
			return fmt.Errorf("seq %d: invalid new_sig in key.rotate: %w", rec.Seq, err)
		}
		if isAgent {
			ag.PubKey = b.NewPubKey
		}
		if adminIdx >= 0 {
			s.AdminPubKeys[adminIdx] = b.NewPubKey
		} else if !isAgent && s.Manifest == nil {
			s.AdminPubKeys = append(s.AdminPubKeys, b.NewPubKey)
		}

	case RecBalanceCredit:
		var b BalanceChangeBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if b.Qty < 0 {
			return fmt.Errorf("seq %d: negative credit qty %d", rec.Seq, b.Qty)
		}
		ag := s.ensureAgent(b.AgentID)
		ag.Available[b.Asset] += b.Qty

	case RecBalanceDebit:
		var b BalanceChangeBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if b.Qty < 0 {
			return fmt.Errorf("seq %d: negative debit qty %d", rec.Seq, b.Qty)
		}
		ag := s.ensureAgent(b.AgentID)
		if ag.Available[b.Asset] < b.Qty {
			return fmt.Errorf("seq %d: insufficient balance for debit on %s (%s): have %d, need %d",
				rec.Seq, b.AgentID, b.Asset, ag.Available[b.Asset], b.Qty)
		}
		ag.Available[b.Asset] -= b.Qty

	case RecOrderAccepted:
		var b OrderAcceptedBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if b.Round == s.CurrentRound && s.FirstOpenOrderSeq == 0 {
			s.FirstOpenOrderSeq = rec.Seq
			s.PreOrderStateHash, _ = s.StateHash()
		}
		if err := s.applyOrderAccepted(rec.Seq, b); err != nil {
			return err
		}

	case RecOrderCancelled:
		var b OrderCancelledBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if b.Round == s.CurrentRound && s.FirstOpenOrderSeq == 0 {
			s.FirstOpenOrderSeq = rec.Seq
			s.PreOrderStateHash, _ = s.StateHash()
		}
		ord, ok := s.OpenOrders[b.OrderID]
		if !ok {
			return fmt.Errorf("seq %d: cancel unknown order %s", rec.Seq, b.OrderID)
		}
		ag := s.ensureAgent(ord.AgentID)
		ag.Locked[ord.LockedUnit] -= ord.LockedAmt
		ag.Available[ord.LockedUnit] += ord.LockedAmt
		delete(s.OpenOrders, b.OrderID)

	case RecOrderReplaced:
		var b OrderReplacedBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if b.Round == s.CurrentRound && s.FirstOpenOrderSeq == 0 {
			s.FirstOpenOrderSeq = rec.Seq
			s.PreOrderStateHash, _ = s.StateHash()
		}
		oldOrd, ok := s.OpenOrders[b.OldOrderID]
		if !ok {
			return fmt.Errorf("seq %d: replace unknown order %s", rec.Seq, b.OldOrderID)
		}
		ag := s.ensureAgent(oldOrd.AgentID)
		ag.Locked[oldOrd.LockedUnit] -= oldOrd.LockedAmt
		ag.Available[oldOrd.LockedUnit] += oldOrd.LockedAmt
		delete(s.OpenOrders, b.OldOrderID)

		remTTL := oldOrd.TTLRounds
		if remTTL > 0 && b.Round > oldOrd.Round {
			elapsed := b.Round - oldOrd.Round
			if remTTL > elapsed {
				remTTL -= elapsed
			} else {
				remTTL = 1
			}
		}

		if err := s.applyOrderAccepted(rec.Seq, OrderAcceptedBody{
			OrderID:     b.NewOrderID,
			Round:       b.Round,
			AgentID:     b.AgentID,
			Asset:       b.Asset,
			Side:        b.Side,
			LimitPrice:  b.LimitPrice,
			Qty:         b.Qty,
			CarryOver:   oldOrd.CarryOver,
			TTLRounds:   remTTL,
			EnvelopeSig: b.EnvelopeSig,
		}); err != nil {
			return err
		}

	case RecRoundSealed:
		var b RoundSealedBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		s.CurrentRound = b.Round
		s.RoundStatus = RoundStatusSealed
		s.pendingFillsByAsset = make(map[string][]protocol.Fill)
		s.expectedFillsByAsset = make(map[string][]protocol.Fill)
		s.clearPriceByAsset = make(map[string]int64)
		s.clearBandByAsset = make(map[string]priceBand)

		// Spec A8.2: Carried-over orders from earlier rounds whose limit_price falls outside
		// the new round's reference price band are automatically expired and their escrow refunded.
		if s.Manifest != nil && len(b.RefPrices) > 0 {
			for id, o := range s.OpenOrders {
				if o.Round < b.Round {
					if refP := b.RefPrices[o.Asset]; refP > 0 {
						minP, maxP := s.Manifest.PriceBounds(refP)
						if o.LimitPrice < minP || o.LimitPrice > maxP {
							if o.LockedAmt > 0 {
								ag := s.ensureAgent(o.AgentID)
								ag.Locked[o.LockedUnit] -= o.LockedAmt
								ag.Available[o.LockedUnit] += o.LockedAmt
							}
							delete(s.OpenOrders, id)
						}
					}
				}
			}
		}

	case RecClear:
		var b ClearBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.verifyAndApplyClear(rec.Seq, b); err != nil {
			return err
		}

	case RecFill:
		var f protocol.Fill
		if err := json.Unmarshal(rec.Body, &f); err != nil {
			return err
		}
		s.pendingFillsByAsset[f.Asset] = append(s.pendingFillsByAsset[f.Asset], f)

	case RecSettle:
		var b SettleBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.applySettle(rec.Seq, b); err != nil {
			return err
		}

	case RecSnapshotCheckpoint:
		var b SnapshotCheckpointBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		// Compute state hash BEFORE LastSeq is updated to this checkpoint record, OR check consistency
		// In our convention, SnapshotCheckpointBody.StateHash is the StateHash of the state right before the checkpoint record is applied.
		computed, err := s.StateHash()
		if err != nil {
			return err
		}
		if b.StateHash != computed {
			return fmt.Errorf("seq %d: snapshot_checkpoint state_hash mismatch: got %s, want %s", rec.Seq, b.StateHash, computed)
		}

	case RecManifestUpdate:
		// Manifest update applies to future rounds; recorded and hash-chained
	case RecAdminHalt:
		var b AdminHaltRecordBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.verifyAdminKey(rec.Seq, b.AdminPubKey); err != nil {
			return err
		}
		if b.Scope == "all" || b.Scope == "" {
			s.HaltedGlobal = true
			s.RoundStatus = RoundStatusVoided
			s.FirstOpenOrderSeq = 0
			s.PreOrderStateHash = ""
			// Spec B4: current round voided, not cleared; escrow released
			s.releaseAllEscrow("")
		} else if len(b.Scope) > 6 && b.Scope[:6] == "asset:" {
			asset := b.Scope[6:]
			s.HaltedAssets[asset] = true
			s.releaseAllEscrow(asset)
		}

	case RecAdminResume:
		var b AdminResumeRecordBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.verifyAdminKey(rec.Seq, b.AdminPubKey); err != nil {
			return err
		}
		if b.Scope == "all" || b.Scope == "" {
			s.HaltedGlobal = false
			s.CurrentRound = b.Round
			s.RoundStatus = RoundStatusOpen
		} else if len(b.Scope) > 6 && b.Scope[:6] == "asset:" {
			asset := b.Scope[6:]
			delete(s.HaltedAssets, asset)
		}

	case RecAdminReset:
		var b AdminResetRecordBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.verifyAdminKey(rec.Seq, b.AdminPubKey); err != nil {
			return err
		}
		if b.Round > s.CurrentRound {
			s.CurrentRound = b.Round
		}
		s.RoundStatus = RoundStatusOpen
		s.FirstOpenOrderSeq = 0
		s.PreOrderStateHash = ""
		s.OpenOrders = make(map[string]*OpenOrder)
		s.PrevClearingPrices = make(map[string]int64)
		for _, ag := range s.Agents {
			ag.Locked = make(map[string]int64)
			ag.Available = make(map[string]int64)
			for asset, qty := range b.InitialBalances {
				if qty > 0 {
					ag.Available[asset] = qty
				}
			}
		}

	case RecAdminModerate:
		var b AdminModerateRecordBody
		if err := json.Unmarshal(rec.Body, &b); err != nil {
			return err
		}
		if err := s.verifyAdminKey(rec.Seq, b.AdminPubKey); err != nil {
			return err
		}

	case RecExt:
		// Hash-chained and signature-verified; skipped for core balance/order state (Spec A12 & B1.4)

	default:
		// Spec A13: Implementations skip unknown record types without error
	}

	s.LastSeq = rec.Seq
	s.LastHash = rec.Hash
	return nil
}

func (s *State) verifyAdminKey(seq uint64, adminPub string) error {
	if len(s.AdminPubKeys) == 0 || adminPub == "" {
		return nil
	}
	for _, ak := range s.AdminPubKeys {
		if ak == adminPub {
			return nil
		}
	}
	return fmt.Errorf("seq %d: unauthorized admin_pubkey %q not in active admin_pubkeys", seq, adminPub)
}

func (s *State) ensureAgent(agentID string) *AgentState {
	ag, ok := s.Agents[agentID]
	if !ok {
		ag = &AgentState{
			AgentID:   agentID,
			Available: make(map[string]int64),
			Locked:    make(map[string]int64),
		}
		s.Agents[agentID] = ag
	}
	return ag
}

func (s *State) applyOrderAccepted(seq uint64, b OrderAcceptedBody) error {
	if b.Round > s.CurrentRound && s.RoundStatus == RoundStatusSettled {
		s.CurrentRound = b.Round
		s.RoundStatus = RoundStatusOpen
	}
	ag := s.ensureAgent(b.AgentID)
	cashUnit := s.SettlementUnitID()
	var lockUnit string
	var lockAmt int64
	if b.Side == protocol.SideBuy {
		lockUnit = cashUnit
		val, err := protocol.SafeMulInt(b.LimitPrice, b.Qty)
		if err != nil {
			return fmt.Errorf("seq %d: order %s escrow overflow: %w", seq, b.OrderID, err)
		}
		lockAmt = val
	} else {
		if err := protocol.ValidateInt(b.Qty); err != nil || b.Qty <= 0 {
			return fmt.Errorf("seq %d: order %s invalid qty %d", seq, b.OrderID, b.Qty)
		}
		lockUnit = b.Asset
		lockAmt = b.Qty
	}
	if ag.Available[lockUnit] < lockAmt {
		return fmt.Errorf("seq %d: order %s insufficient available %s: have %d, need %d",
			seq, b.OrderID, lockUnit, ag.Available[lockUnit], lockAmt)
	}
	ag.Available[lockUnit] -= lockAmt
	ag.Locked[lockUnit] += lockAmt

	s.OpenOrders[b.OrderID] = &OpenOrder{
		OrderID:    b.OrderID,
		Round:      b.Round,
		AgentID:    b.AgentID,
		Asset:      b.Asset,
		Side:       b.Side,
		LimitPrice: b.LimitPrice,
		Qty:        b.Qty,
		CarryOver:  b.CarryOver,
		TTLRounds:  b.TTLRounds,
		LockedUnit: lockUnit,
		LockedAmt:  lockAmt,
	}
	return nil
}

func (s *State) verifyAndApplyClear(seq uint64, b ClearBody) error {
	var seed [32]byte
	if s.Manifest != nil && s.Manifest.ServerPubKey != "" {
		derived, err := protocol.VerifyAndDeriveLotterySeed(s.Manifest.ServerPubKey, s.GameID, b.Round, b.SeedSig)
		if err != nil {
			return fmt.Errorf("seq %d: invalid clear seed_sig: %w", seq, err)
		}
		seed = derived
	}

	var bandMin, bandMax int64
	if s.Manifest != nil && b.ReferencePrice > 0 {
		bandMin, bandMax = s.Manifest.PriceBounds(b.ReferencePrice)
	}
	if bandMin > 0 || bandMax > 0 {
		s.clearBandByAsset[b.Asset] = priceBand{min: bandMin, max: bandMax}
	}

	// Collect open orders for this asset (excluding carried-over orders outside the active round's price band)
	orders := make([]clearing.Order, 0)
	for _, o := range s.OpenOrders {
		if o.Asset == b.Asset && o.Qty > 0 {
			if o.Round < b.Round && b.ReferencePrice > 0 && (bandMin > 0 || bandMax > 0) && (o.LimitPrice < bandMin || o.LimitPrice > bandMax) {
				continue
			}
			orders = append(orders, clearing.Order{
				OrderID:    o.OrderID,
				AgentID:    o.AgentID,
				Asset:      o.Asset,
				Side:       o.Side,
				LimitPrice: o.LimitPrice,
				Qty:        o.Qty,
			})
		}
	}

	res := clearing.Clear(clearing.ClearingInput{
		GameID:            s.GameID,
		Round:             b.Round,
		Asset:             b.Asset,
		Orders:            orders,
		PrevClearingPrice: b.PrevPrice,
		ReferencePrice:    b.ReferencePrice,
		BandMin:           bandMin,
		BandMax:           bandMax,
		Seed:              seed,
		SeedSig:           b.SeedSig,
	})

	if res.ClearingPrice != b.ClearingPrice || res.Volume != b.Volume {
		return fmt.Errorf("seq %d: clearing replay mismatch on %s: record(price=%d, vol=%d) vs computed(price=%d, vol=%d)",
			seq, b.Asset, b.ClearingPrice, b.Volume, res.ClearingPrice, res.Volume)
	}

	s.expectedFillsByAsset[b.Asset] = res.Fills
	s.clearPriceByAsset[b.Asset] = b.ClearingPrice
	if b.ClearingPrice > 0 && b.Volume > 0 {
		s.PrevClearingPrices[b.Asset] = b.ClearingPrice
	}
	s.RoundStatus = RoundStatusCleared
	return nil
}

func (s *State) applySettle(seq uint64, b SettleBody) error {
	cashUnit := s.SettlementUnitID()

	// Verify that recorded fills match expected fills for every cleared asset
	for asset, expFills := range s.expectedFillsByAsset {
		gotFills := s.pendingFillsByAsset[asset]
		if len(gotFills) != len(expFills) {
			return fmt.Errorf("seq %d: fill count mismatch for %s: got %d, want %d", seq, asset, len(gotFills), len(expFills))
		}
		for i := range expFills {
			if gotFills[i] != expFills[i] {
				return fmt.Errorf("seq %d: fill[%d] mismatch for %s: got %+v, want %+v", seq, i, asset, gotFills[i], expFills[i])
			}
		}
	}

	// Execute atomic settlement per asset
	for asset, fills := range s.pendingFillsByAsset {
		for _, f := range fills {
			ag := s.ensureAgent(f.AgentID)
			if f.Side == protocol.SideBuy {
				// Deduct filled qty from the agent's open buy orders (highest limit price first, then order_id)
				s.consumeAgentOrders(f.AgentID, asset, protocol.SideBuy, f.FilledQty, f.ClearingPrice)
				// Credit acquired asset to buyer's available balance
				ag.Available[asset] += f.FilledQty
			} else {
				// Deduct filled qty from the agent's open sell orders (lowest limit price first, then order_id)
				s.consumeAgentOrders(f.AgentID, asset, protocol.SideSell, f.FilledQty, f.ClearingPrice)
				// Credit proceeds to seller's available cash
				ag.Available[cashUnit] += f.CashDelta
			}
		}
	}

	// Evaluate order expiration / carry-over per order:
	// 1. If o.TTLRounds > 0 ("N회차까지 유지"): keep only while (b.Round - o.Round + 1) < o.TTLRounds.
	// 2. Else if o.CarryOver ("사질때까지 냅둬") or manifest-level b.CarryOver: keep across rounds.
	// 3. Otherwise (default): automatically expire unfilled order and refund locked escrow immediately.
	for id, o := range s.OpenOrders {
		keep := false
		if o.TTLRounds > 0 {
			roundsElapsed := uint64(1)
			if b.Round >= o.Round {
				roundsElapsed = b.Round - o.Round + 1
			}
			keep = roundsElapsed < o.TTLRounds
		} else {
			keep = o.CarryOver || b.CarryOver
		}
		if !keep {
			if o.LockedAmt > 0 {
				ag := s.ensureAgent(o.AgentID)
				ag.Locked[o.LockedUnit] -= o.LockedAmt
				ag.Available[o.LockedUnit] += o.LockedAmt
			}
			delete(s.OpenOrders, id)
		}
	}

	s.pendingFillsByAsset = make(map[string][]protocol.Fill)
	s.expectedFillsByAsset = make(map[string][]protocol.Fill)
	s.clearPriceByAsset = make(map[string]int64)
	s.clearBandByAsset = make(map[string]priceBand)
	s.FirstOpenOrderSeq = 0
	s.PreOrderStateHash = ""
	s.CurrentRound = b.Round + 1
	s.RoundStatus = RoundStatusOpen
	return nil
}

func (s *State) consumeAgentOrders(agentID, asset string, side protocol.Side, filledQty, clearingPrice int64) {
	band, hasBand := s.clearBandByAsset[asset]
	var matching []*OpenOrder
	for _, o := range s.OpenOrders {
		if o.AgentID == agentID && o.Asset == asset && o.Side == side && o.Qty > 0 {
			if hasBand && o.Round < s.CurrentRound && (o.LimitPrice < band.min || o.LimitPrice > band.max) {
				continue
			}
			if side == protocol.SideBuy && o.LimitPrice < clearingPrice {
				continue
			}
			if side == protocol.SideSell && o.LimitPrice > clearingPrice {
				continue
			}
			matching = append(matching, o)
		}
	}
	// Sort strictly better prices first; at equal price, consume shorter-lived orders before GTC/longer-TTL orders, then by OrderID.
	orderRemainingRounds := func(o *OpenOrder) uint64 {
		if o.TTLRounds > 0 {
			elapsed := uint64(0)
			if s.CurrentRound > o.Round {
				elapsed = s.CurrentRound - o.Round
			}
			if o.TTLRounds > elapsed {
				return o.TTLRounds - elapsed
			}
			return 1
		}
		if o.CarryOver || (s.Manifest != nil && s.Manifest.Round.CarryOver) {
			return ^uint64(0) // infinite (GTC)
		}
		return 1 // expires at end of current round
	}
	sort.Slice(matching, func(i, j int) bool {
		if matching[i].LimitPrice != matching[j].LimitPrice {
			if side == protocol.SideBuy {
				return matching[i].LimitPrice > matching[j].LimitPrice
			}
			return matching[i].LimitPrice < matching[j].LimitPrice
		}
		remI := orderRemainingRounds(matching[i])
		remJ := orderRemainingRounds(matching[j])
		if remI != remJ {
			return remI < remJ
		}
		return matching[i].OrderID < matching[j].OrderID
	})

	ag := s.ensureAgent(agentID)
	cashUnit := s.SettlementUnitID()
	rem := filledQty
	for _, o := range matching {
		if rem <= 0 {
			break
		}
		take := o.Qty
		if take > rem {
			take = rem
		}
		o.Qty -= take
		rem -= take

		if side == protocol.SideBuy {
			lockedForTake := o.LimitPrice * take
			paid := clearingPrice * take
			excess := lockedForTake - paid
			ag.Locked[cashUnit] -= lockedForTake
			o.LockedAmt -= lockedForTake
			if excess > 0 {
				// Spec A8: buyer's locked excess (limit_price - clearing_price) * filled_qty is released immediately
				ag.Available[cashUnit] += excess
			}
		} else {
			ag.Locked[asset] -= take
			o.LockedAmt -= take
		}

		if o.Qty == 0 {
			delete(s.OpenOrders, o.OrderID)
		}
	}
}

func (s *State) releaseAllEscrow(onlyAsset string) {
	for id, o := range s.OpenOrders {
		if onlyAsset != "" && o.Asset != onlyAsset {
			continue
		}
		if o.LockedAmt > 0 {
			ag := s.ensureAgent(o.AgentID)
			ag.Locked[o.LockedUnit] -= o.LockedAmt
			ag.Available[o.LockedUnit] += o.LockedAmt
		}
		delete(s.OpenOrders, id)
	}
}

// Snapshot returns a clean serializable ReplaySnapshot of the current state.
func (s *State) Snapshot() *ReplaySnapshot {
	snap := &ReplaySnapshot{
		GameID:             s.GameID,
		ManifestHash:       s.ManifestHash,
		LastSeq:            s.LastSeq,
		LastHash:           s.LastHash,
		CurrentRound:       s.CurrentRound,
		RoundStatus:        s.RoundStatus,
		HaltedGlobal:       s.HaltedGlobal,
		HaltedAssets:       make(map[string]bool, len(s.HaltedAssets)),
		PrevClearingPrices: make(map[string]int64, len(s.PrevClearingPrices)),
		Agents:             make(map[string]*AgentState, len(s.Agents)),
		OpenOrders:         make(map[string]*OpenOrder, len(s.OpenOrders)),
		FirstOpenOrderSeq:  s.FirstOpenOrderSeq,
		PreOrderStateHash:  s.PreOrderStateHash,
	}
	for k, v := range s.HaltedAssets {
		if v {
			snap.HaltedAssets[k] = true
		}
	}
	for k, v := range s.PrevClearingPrices {
		snap.PrevClearingPrices[k] = v
	}
	for id, ag := range s.Agents {
		cpAg := &AgentState{
			AgentID:     ag.AgentID,
			PubKey:      ag.PubKey,
			DisplayName: ag.DisplayName,
			Available:   make(map[string]int64),
			Locked:      make(map[string]int64),
		}
		for k, v := range ag.Available {
			if v != 0 {
				cpAg.Available[k] = v
			}
		}
		for k, v := range ag.Locked {
			if v != 0 {
				cpAg.Locked[k] = v
			}
		}
		snap.Agents[id] = cpAg
	}
	for id, o := range s.OpenOrders {
		cpOrd := *o
		snap.OpenOrders[id] = &cpOrd
	}
	return snap
}

// StateHash computes the canonical SHA-256 hash ("sha256:<hex>") of the replayed state.
func (s *State) StateHash() (string, error) {
	return protocol.HashCanonical(s.Snapshot())
}

// DoctorCheck verifies ledger invariants (Spec B4):
// 1. Agent locked balances equal exact sum of open order locks.
// 2. No negative available or locked balances exist.
func (s *State) DoctorCheck() error {
	expectedLocks := make(map[string]map[string]int64)
	for _, o := range s.OpenOrders {
		if expectedLocks[o.AgentID] == nil {
			expectedLocks[o.AgentID] = make(map[string]int64)
		}
		expectedLocks[o.AgentID][o.LockedUnit] += o.LockedAmt
	}

	for id, ag := range s.Agents {
		for unit, amt := range ag.Available {
			if amt < 0 {
				return fmt.Errorf("agent %s has negative available %s: %d", id, unit, amt)
			}
		}
		for unit, amt := range ag.Locked {
			if amt < 0 {
				return fmt.Errorf("agent %s has negative locked %s: %d", id, unit, amt)
			}
			exp := int64(0)
			if expectedLocks[id] != nil {
				exp = expectedLocks[id][unit]
			}
			if amt != exp {
				return fmt.Errorf("agent %s escrow lock mismatch on %s: state has %d, open orders sum to %d", id, unit, amt, exp)
			}
		}
	}
	return nil
}

// RestoreSnapshot restores a State from a verified ReplaySnapshot (Spec §8 fast bootstrap from checkpoint).
func (s *State) RestoreSnapshot(snap *ReplaySnapshot) error {
	if snap == nil {
		return fmt.Errorf("nil replay snapshot")
	}
	computed, err := protocol.HashCanonical(snap)
	if err != nil {
		return err
	}
	_ = computed
	s.GameID = snap.GameID
	s.ManifestHash = snap.ManifestHash
	s.LastSeq = snap.LastSeq
	s.LastHash = snap.LastHash
	s.CurrentRound = snap.CurrentRound
	s.RoundStatus = snap.RoundStatus
	s.HaltedGlobal = snap.HaltedGlobal
	s.HaltedAssets = make(map[string]bool, len(snap.HaltedAssets))
	for k, v := range snap.HaltedAssets {
		if v {
			s.HaltedAssets[k] = true
		}
	}
	s.PrevClearingPrices = make(map[string]int64, len(snap.PrevClearingPrices))
	for k, v := range snap.PrevClearingPrices {
		s.PrevClearingPrices[k] = v
	}
	s.Agents = make(map[string]*AgentState, len(snap.Agents))
	for id, ag := range snap.Agents {
		cpAg := &AgentState{
			AgentID:     ag.AgentID,
			PubKey:      ag.PubKey,
			DisplayName: ag.DisplayName,
			Available:   make(map[string]int64, len(ag.Available)),
			Locked:      make(map[string]int64, len(ag.Locked)),
		}
		for k, v := range ag.Available {
			cpAg.Available[k] = v
		}
		for k, v := range ag.Locked {
			cpAg.Locked[k] = v
		}
		s.Agents[id] = cpAg
	}
	s.OpenOrders = make(map[string]*OpenOrder, len(snap.OpenOrders))
	for id, o := range snap.OpenOrders {
		cpOrd := *o
		s.OpenOrders[id] = &cpOrd
	}
	return nil
}

// ReplayAndVerify replays a stream of records from seq 1, verifying hash chain, server signatures,
// clearing determinism, and escrow invariants.
func ReplayAndVerify(m *manifest.Manifest, seq iter.Seq2[Record, error]) (*State, error) {
	st := NewState(m)
	for rec, err := range seq {
		if err != nil {
			return nil, err
		}
		if err := st.Apply(rec); err != nil {
			return nil, err
		}
	}
	if err := st.DoctorCheck(); err != nil {
		return nil, err
	}
	return st, nil
}
