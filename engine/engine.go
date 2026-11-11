package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/galaxytemple/AgentSquare/board"
	"github.com/galaxytemple/AgentSquare/clearing"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/marketdata"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

// AdmissionRequest is passed to Game.Admit when an agent registers.
type AdmissionRequest struct {
	AgentID     string `json:"agent_id"`
	PubKey      string `json:"pubkey"`
	DisplayName string `json:"display_name,omitempty"`
	InviteCode  string `json:"invite_code,omitempty"`
}

// BalanceChange specifies an initial or game-triggered asset/currency credit or debit.
type BalanceChange struct {
	Asset  string `json:"asset"`
	Qty    int64  `json:"qty"`
	Reason string `json:"reason"`
}

// Game is the host game hook interface (Spec C4).
type Game interface {
	Manifest() manifest.Manifest
	ReferencePrice(asset string, round uint64) int64
	Admit(ctx context.Context, req AdmissionRequest) (initial []BalanceChange, err error)
	OnSettled(ctx context.Context, round uint64, fills []protocol.Fill) error
}

// StreamEvent represents a real-time SSE/WebSocket market event (Spec A11).
type StreamEvent struct {
	Type    string `json:"type"` // "round_opened" | "indicative_update" | "round_cleared"
	Round   uint64 `json:"round"`
	Asset   string `json:"asset,omitempty"`
	Payload any    `json:"payload"`
}

type nonceRecord struct {
	round       uint64
	contentHash string
	receipt     *protocol.AcceptanceReceipt
}

// Engine coordinates order intake, escrow locks, batch call-auction clearing, ledger persistence, and projections.
type Engine struct {
	clearMu            sync.Mutex
	mu                 sync.RWMutex
	game               Game
	manifest           manifest.Manifest
	log                ledger.Log
	projections        []ledger.Projection
	signer             protocol.Signer
	clock              sim.Clock
	timestampTolerance time.Duration
	state              *ledger.State
	market             *marketdata.Tracker
	boardStore         board.Store
	boardLimits        board.Limits

	nonces            map[string]nonceRecord // key: from + "|" + nonce
	orderSeq          uint64
	postSeq           uint64
	postsCountByRound map[uint64]map[string]int64
	lastPostTime      map[string]int64
	lastAnatomy       map[uint64]map[string]clearing.ClearingResult
	strictCutoff      bool
	pendingIntervals  map[uint64]string

	subsMu      sync.Mutex
	subscribers map[chan StreamEvent]struct{}
}

// Option configures an Engine.
type Option func(*Engine)

func WithLog(l ledger.Log) Option {
	return func(e *Engine) { e.log = l }
}

func WithProjection(p ledger.Projection) Option {
	return func(e *Engine) { e.projections = append(e.projections, p) }
}

func WithSigner(s protocol.Signer) Option {
	return func(e *Engine) { e.signer = s }
}

func WithBoard(store board.Store, limits board.Limits) Option {
	return func(e *Engine) {
		e.boardStore = store
		e.boardLimits = limits
	}
}

func WithClock(c sim.Clock) Option {
	return func(e *Engine) { e.clock = c }
}

func WithTimestampTolerance(d time.Duration) Option {
	return func(e *Engine) { e.timestampTolerance = d }
}

// WithStrictCutoff enforces that orders/cancellations/replacements arriving at or after the current
// round's scheduled cutoff timestamp (RoundBounds end) are immediately rejected with ROUND_SEALED,
// even before SealAndClearRound has been invoked.
func WithStrictCutoff(strict bool) Option {
	return func(e *Engine) { e.strictCutoff = strict }
}

// New constructs and initializes an Engine for game, replaying any existing records in Log or creating genesis.
func New(game Game, opts ...Option) (*Engine, error) {
	m := game.Manifest()
	e := &Engine{
		game:               game,
		manifest:           m,
		clock:              sim.RealClock{},
		timestampTolerance: 30 * time.Second,
		market:             marketdata.NewTracker(),
		nonces:             make(map[string]nonceRecord),
		postsCountByRound:  make(map[uint64]map[string]int64),
		lastPostTime:       make(map[string]int64),
		lastAnatomy:        make(map[uint64]map[string]clearing.ClearingResult),
		pendingIntervals:   make(map[uint64]string),
		subscribers:        make(map[chan StreamEvent]struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.log == nil {
		e.log = memlog.New()
	}
	if e.signer == nil {
		return nil, fmt.Errorf("engine requires a server Signer (WithSigner)")
	}
	if e.manifest.ServerPubKey == "" {
		e.manifest.ServerPubKey = e.signer.PublicKeyString()
	}
	if err := e.manifest.Validate(); err != nil {
		return nil, fmt.Errorf("invalid game manifest: %w", err)
	}
	if e.boardStore == nil && e.manifest.Board != nil && e.manifest.Board.Enabled {
		e.boardStore = board.NewMemoryStore()
		e.boardLimits = board.Limits{
			Enabled:                  e.manifest.Board.Enabled,
			MaxPostsPerAgentPerRound: e.manifest.Board.MaxPostsPerAgentPerRound,
			MaxBodyChars:             e.manifest.Board.MaxBodyChars,
			MinIntervalSec:           e.manifest.Board.MinIntervalSec,
			RequireParticipant:       e.manifest.Board.RequireParticipant,
			BlackoutBeforeCutoffSec:  e.manifest.Board.BlackoutBeforeCutoffSec,
			VisibilityDelaySec:       e.manifest.Board.VisibilityDelaySec,
		}
	}

	e.state = ledger.NewState(&e.manifest)

	ctx := context.Background()
	headSeq, _, err := e.log.Head(ctx)
	if err != nil {
		return nil, err
	}
	if headSeq == 0 {
		// Initialize genesis record
		mh, err := e.manifest.Hash()
		if err != nil {
			return nil, err
		}
		rec, err := e.buildRecord(1, ledger.ZeroHash, ledger.RecGenesis, ledger.GenesisBody{
			ManifestHash: mh,
		})
		if err != nil {
			return nil, err
		}
		if err := e.appendAndApply(ctx, rec); err != nil {
			return nil, err
		}
		e.manifest.Ledger.GenesisHash = rec.Hash
	} else {
		var snapRestoredSeq uint64
		if snap, sErr := e.log.Snapshot(ctx); sErr == nil && snap.Seq > 0 && snap.State != nil {
			if computed, hErr := protocol.HashCanonical(snap.State); hErr == nil && computed == snap.StateHash {
				if err := e.state.RestoreSnapshot(snap.State); err == nil {
					snapRestoredSeq = snap.Seq
				}
			}
		}
		for rec, err := range e.log.Read(ctx, 1) {
			if err != nil {
				return nil, err
			}
			if rec.Seq > snapRestoredSeq {
				if err := e.state.Apply(rec); err != nil {
					return nil, err
				}
			}
			e.market.Apply(rec)
			if rec.Type == ledger.RecKeyRotate && rec.Seq <= snapRestoredSeq {
				var b ledger.KeyRotateBody
				if err := json.Unmarshal(rec.Body, &b); err == nil {
					for i, ak := range e.state.AdminPubKeys {
						if ak == b.OldPubKey {
							e.state.AdminPubKeys[i] = b.NewPubKey
							e.manifest.AdminPubKeys = append([]string(nil), e.state.AdminPubKeys...)
							break
						}
					}
				}
			}
			if rec.Type == ledger.RecAdminReset {
				var b ledger.AdminResetRecordBody
				if err := json.Unmarshal(rec.Body, &b); err == nil && b.ClearHistory {
					e.market = marketdata.NewTracker()
					if e.boardStore != nil {
						e.boardStore = board.NewMemoryStore()
					}
					e.postsCountByRound = make(map[uint64]map[string]int64)
				}
			}
			if rec.Seq == 1 {
				e.manifest.Ledger.GenesisHash = rec.Hash
			}
			if rec.Type == ledger.RecOrderAccepted || rec.Type == ledger.RecOrderReplaced {
				e.orderSeq++
			}
			if rec.Type == ledger.RecAdminModerate && e.boardStore != nil {
				var mod ledger.AdminModerateRecordBody
				if err := json.Unmarshal(rec.Body, &mod); err == nil && mod.PostID != "" {
					e.boardStore.Hide(mod.PostID, true)
				}
			}
			if rec.Type == ledger.RecManifestUpdate {
				var mu protocol.ManifestUpdatePayload
				if err := json.Unmarshal(rec.Body, &mu); err == nil && mu.Interval != "" && mu.EffectiveRound > 0 {
					e.pendingIntervals[mu.EffectiveRound] = mu.Interval
				}
			}
			if rec.Type == ledger.RecSettle {
				var sb ledger.SettleBody
				if err := json.Unmarshal(rec.Body, &sb); err == nil {
					nextRound := sb.Round + 1
					if newInterval, ok := e.pendingIntervals[nextRound]; ok {
						delete(e.pendingIntervals, nextRound)
						if _, roundEnd, bErr := e.manifest.RoundBounds(sb.Round); bErr == nil {
							_ = e.reanchorRoundLocked(nextRound, roundEnd, newInterval)
						}
					}
				}
			}
			if rec.Type == ledger.RecExt && e.boardStore != nil {
				var ext ledger.ExtBody
				if err := json.Unmarshal(rec.Body, &ext); err == nil && ext.Namespace == "board" {
					var rawMap map[string]any
					if err := json.Unmarshal(ext.Body, &rawMap); err == nil && rawMap["action"] == "retract" {
						if pid, ok := rawMap["post_id"].(string); ok && pid != "" {
							e.boardStore.Hide(pid, false)
						}
					} else {
						var anchor board.AnchorRecordBody
						if err := json.Unmarshal(ext.Body, &anchor); err == nil && anchor.PostID != "" {
							e.postSeq++
							_ = e.boardStore.Put(board.Post{
								PostID:      anchor.PostID,
								Round:       anchor.Round,
								Seq:         rec.Seq,
								Author:      anchor.Author,
								Asset:       anchor.Asset,
								Stance:      anchor.Stance,
								ContentHash: anchor.ContentHash,
							})
							if e.postsCountByRound[anchor.Round] == nil {
								e.postsCountByRound[anchor.Round] = make(map[string]int64)
							}
							e.postsCountByRound[anchor.Round][anchor.Author]++
						}
					}
				}
			}
		}
	}

	return e, nil
}

// Manifest returns the active game manifest (including populated genesis_hash).
func (e *Engine) Manifest() manifest.Manifest {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.manifest
}

// Log returns the underlying ledger.Log.
func (e *Engine) Log() ledger.Log {
	return e.log
}

// StateSnapshot returns a thread-safe snapshot of the current ledger state.
func (e *Engine) StateSnapshot() *ledger.ReplaySnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state.Snapshot()
}

// StateHash returns the canonical state_hash of the current replayed state.
func (e *Engine) StateHash() (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state.StateHash()
}

// Subscribe creates a channel receiving real-time market stream events.
func (e *Engine) Subscribe() (ch chan StreamEvent, cancel func()) {
	ch = make(chan StreamEvent, 64)
	e.subsMu.Lock()
	e.subscribers[ch] = struct{}{}
	e.subsMu.Unlock()
	return ch, func() {
		e.subsMu.Lock()
		delete(e.subscribers, ch)
		close(ch)
		e.subsMu.Unlock()
	}
}

func (e *Engine) publishEvent(ev StreamEvent) {
	e.subsMu.Lock()
	defer e.subsMu.Unlock()
	for ch := range e.subscribers {
		select {
		case ch <- ev:
		default:
		}
	}
}

// SubmitRawEnvelope processes a raw JSON signed envelope and returns either an *AcceptanceReceipt or a *SignedRejection.
func (e *Engine) SubmitRawEnvelope(ctx context.Context, raw []byte) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	e.mu.Lock()
	defer e.mu.Unlock()

	nowUnix := e.clock.Now().Unix()

	// Peek envelope fields for sender identification and corr_id
	var peek struct {
		GameID    string          `json:"game_id"`
		From      string          `json:"from"`
		Type      string          `json:"type"`
		Timestamp int64           `json:"timestamp"`
		Nonce     string          `json:"nonce"`
		CorrID    string          `json:"corr_id"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &peek); err != nil {
		return nil, e.reject("", protocol.ErrCodeInvalidSignature, "malformed JSON envelope", "send a valid JSON object matching envelope.schema.json", nowUnix)
	}

	if peek.GameID != "" && peek.GameID != e.manifest.GameID {
		return nil, e.reject(peek.CorrID, protocol.ErrCodeInvalidSignature,
			fmt.Sprintf("wrong game_id %q (expected %q)", peek.GameID, e.manifest.GameID),
			"set game_id to match /.well-known/agentsquare.json", nowUnix)
	}

	// Determine the expected signer public key
	pubKeyStr, err := e.resolveSenderPubKey(peek.Type, peek.From, peek.Payload)
	if err != nil {
		return nil, e.reject(peek.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "register the agent first or sign with the registered Ed25519 key", nowUnix)
	}

	env, contentHash, errCode, err := protocol.VerifyRawEnvelope(raw, pubKeyStr, knownPayloadFieldsForType(peek.Type))
	if err != nil {
		if errCode == "" {
			errCode = protocol.ErrCodeInvalidSignature
		}
		hint := "sign the RFC 8785 JCS canonical envelope with your Ed25519 private key"
		if errCode == protocol.ErrCodeUnsupportedVersion {
			hint = "use version 1.0 and remove unrecognized critical fields"
		}
		return nil, e.reject(peek.CorrID, errCode, err.Error(), hint, nowUnix)
	}

	// Canonicalize env.From to the registered agent_id if sender signed with a rotated key fingerprint
	if env.Type != protocol.TypeAgentRegister {
		if canonicalID := e.canonicalAgentID(env.From); canonicalID != "" {
			env.From = canonicalID
		}
	}

	// Check timestamp freshness tolerance (Spec A6)
	if e.timestampTolerance > 0 {
		tolSec := int64(e.timestampTolerance.Seconds())
		diff := env.Timestamp - nowUnix
		if diff < -tolSec || diff > tolSec {
			return nil, e.reject(env.CorrID, protocol.ErrCodeStaleTimestamp,
				fmt.Sprintf("envelope timestamp %d outside ±%ds window of server time %d", env.Timestamp, tolSec, nowUnix),
				"synchronize clock via NTP and set timestamp to current UTC unix seconds", nowUnix)
		}
	}

	// Check idempotent resend vs replayed nonce (Spec A6)
	nonceKey := env.From + "|" + env.Nonce
	if existing, exists := e.nonces[nonceKey]; exists {
		if existing.contentHash == contentHash {
			// Idempotent resend: return original signed receipt!
			return existing.receipt, nil
		}
		return nil, e.reject(env.CorrID, protocol.ErrCodeReplayedNonce,
			fmt.Sprintf("nonce %q already used by %s with different payload", env.Nonce, env.From),
			"generate a fresh unique nonce for each new request", nowUnix)
	}

	var receipt *protocol.AcceptanceReceipt
	var rej *protocol.SignedRejection

	switch env.Type {
	case protocol.TypeAgentRegister:
		receipt, rej = e.handleRegister(ctx, env, contentHash, nowUnix)
	case protocol.TypeKeyRotate:
		receipt, rej = e.handleKeyRotate(ctx, env, contentHash, nowUnix)
	case protocol.TypePlaceOrder:
		receipt, rej = e.handlePlaceOrder(ctx, env, contentHash, nowUnix)
	case protocol.TypeCancelOrder:
		receipt, rej = e.handleCancelOrder(ctx, env, contentHash, nowUnix)
	case protocol.TypeReplaceOrder:
		receipt, rej = e.handleReplaceOrder(ctx, env, contentHash, nowUnix)
	case protocol.TypeBoardPost:
		receipt, rej = e.handleBoardPost(ctx, env, contentHash, nowUnix)
	case protocol.TypeBoardRetract:
		receipt, rej = e.handleBoardRetract(ctx, env, contentHash, nowUnix)
	case protocol.TypeAdminHalt:
		receipt, rej = e.handleAdminHalt(ctx, env, contentHash, nowUnix)
	case protocol.TypeAdminResume:
		receipt, rej = e.handleAdminResume(ctx, env, contentHash, nowUnix)
	case protocol.TypeAdminModerate:
		receipt, rej = e.handleAdminModerate(ctx, env, contentHash, nowUnix)
	case protocol.TypeAdminReset:
		receipt, rej = e.handleAdminReset(ctx, env, contentHash, nowUnix)
	case protocol.TypeManifestUpdate:
		receipt, rej = e.handleManifestUpdate(ctx, env, contentHash, nowUnix)
	default:
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnsupportedVersion,
			fmt.Sprintf("unsupported message type: %s", env.Type),
			"use a supported primitive type", nowUnix)
	}

	if rej != nil {
		return nil, rej
	}
	e.nonces[nonceKey] = nonceRecord{
		round:       e.state.CurrentRound,
		contentHash: contentHash,
		receipt:     receipt,
	}
	return receipt, nil
}

func knownPayloadFieldsForType(msgType string) map[string]bool {
	switch msgType {
	case protocol.TypeAgentRegister:
		return map[string]bool{"pubkey": true, "display_name": true, "invite_code": true}
	case protocol.TypeKeyRotate:
		return map[string]bool{"old_pubkey": true, "new_pubkey": true, "new_sig": true, "agent_id": true}
	case protocol.TypePlaceOrder:
		return map[string]bool{"asset": true, "side": true, "limit_price": true, "qty": true, "market": true, "carry_over": true, "ttl_rounds": true}
	case protocol.TypeCancelOrder:
		return map[string]bool{"order_id": true}
	case protocol.TypeReplaceOrder:
		return map[string]bool{"order_id": true, "limit_price": true, "qty": true}
	case protocol.TypeBoardPost:
		return map[string]bool{"asset": true, "stance": true, "body": true, "reply_to": true}
	case protocol.TypeBoardRetract:
		return map[string]bool{"post_id": true}
	case protocol.TypeAdminHalt, protocol.TypeAdminResume:
		return map[string]bool{"scope": true, "reason": true}
	case protocol.TypeAdminModerate:
		return map[string]bool{"post_id": true, "reason": true}
	case protocol.TypeAdminReset:
		return map[string]bool{"season_id": true, "reason": true, "initial_balances": true, "clear_history": true}
	case protocol.TypeManifestUpdate:
		return map[string]bool{"new_hash": true, "effective_round": true, "interval": true}
	default:
		return nil
	}
}

func (e *Engine) canonicalAgentID(from string) string {
	if ag, ok := e.state.Agents[from]; ok {
		return ag.AgentID
	}
	for id, ag := range e.state.Agents {
		if ag.PubKey == "" {
			continue
		}
		if ag.PubKey == from {
			return id
		}
		if derived, err := protocol.AgentIDFromPubKeyString(ag.PubKey); err == nil && derived == from {
			return id
		}
	}
	return ""
}

func (e *Engine) resolveSenderPubKey(msgType, from string, rawPayload json.RawMessage) (string, error) {
	if msgType == protocol.TypeAgentRegister {
		var p protocol.RegisterPayload
		if err := json.Unmarshal(rawPayload, &p); err != nil {
			return "", fmt.Errorf("invalid agent.register payload")
		}
		derivedID, err := protocol.AgentIDFromPubKeyString(p.PubKey)
		if err != nil {
			return "", err
		}
		if from != derivedID {
			return "", fmt.Errorf("envelope from %q does not match pubkey fingerprint %q", from, derivedID)
		}
		return p.PubKey, nil
	}
	if strings.HasPrefix(msgType, "admin.") || msgType == protocol.TypeManifestUpdate {
		for _, ak := range e.state.AdminPubKeys {
			id, err := protocol.AgentIDFromPubKeyString(ak)
			if err == nil && (from == id || from == ak) {
				return ak, nil
			}
		}
		return "", fmt.Errorf("sender %q is not an authorized admin key", from)
	}
	if canonicalID := e.canonicalAgentID(from); canonicalID != "" {
		ag := e.state.Agents[canonicalID]
		if ag != nil && ag.PubKey != "" {
			return ag.PubKey, nil
		}
	}
	if msgType == protocol.TypeKeyRotate {
		for _, ak := range e.state.AdminPubKeys {
			id, err := protocol.AgentIDFromPubKeyString(ak)
			if err == nil && (from == id || from == ak) {
				return ak, nil
			}
		}
	}
	return "", fmt.Errorf("unknown or unregistered agent %q", from)
}

func (e *Engine) handleRegister(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.RegisterPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid register payload", "check payload fields", nowUnix)
	}
	if _, exists := e.state.Agents[env.From]; exists {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "agent already registered", "use existing agent identity", nowUnix)
	}
	initial, err := e.game.Admit(ctx, AdmissionRequest{
		AgentID:     env.From,
		PubKey:      p.PubKey,
		DisplayName: p.DisplayName,
		InviteCode:  p.InviteCode,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, err.Error(), "provide valid admission credentials", nowUnix)
	}
	if len(initial) == 0 && len(e.manifest.InitialBalances) > 0 {
		assets := make([]string, 0, len(e.manifest.InitialBalances))
		for asset := range e.manifest.InitialBalances {
			assets = append(assets, asset)
		}
		sort.Strings(assets)
		for _, asset := range assets {
			if qty := e.manifest.InitialBalances[asset]; qty > 0 {
				initial = append(initial, BalanceChange{
					Asset:  asset,
					Qty:    qty,
					Reason: "initial_grant",
				})
			}
		}
	}

	seq, prevHash := e.state.LastSeq+1, e.state.LastHash
	var recs []ledger.Record

	admRec, err := e.buildRecord(seq, prevHash, ledger.RecAgentAdmitted, ledger.AgentAdmittedBody{
		AgentID:     env.From,
		PubKey:      p.PubKey,
		DisplayName: p.DisplayName,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "internal signing error", nowUnix)
	}
	recs = append(recs, admRec)
	prevHash = admRec.Hash
	seq++

	for _, chg := range initial {
		if chg.Qty <= 0 {
			continue
		}
		cRec, err := e.buildRecord(seq, prevHash, ledger.RecBalanceCredit, ledger.BalanceChangeBody{
			AgentID: env.From,
			Asset:   chg.Asset,
			Qty:     chg.Qty,
			Reason:  chg.Reason,
		})
		if err != nil {
			return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "internal signing error", nowUnix)
		}
		recs = append(recs, cRec)
		prevHash = cRec.Hash
		seq++
	}

	if err := e.appendAndApply(ctx, recs...); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry request", nowUnix)
	}

	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, recs[len(recs)-1].Seq, contentHash), nil
}

func (e *Engine) handleKeyRotate(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.KeyRotatePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid key.rotate payload", "provide old_pubkey, new_pubkey, new_sig", nowUnix)
	}
	ag, isAgent := e.state.Agents[env.From]
	if isAgent && ag.PubKey != p.OldPubKey {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "old_pubkey does not match current registered key", "sign with current registered key", nowUnix)
	}
	isAdmin := false
	for _, ak := range e.state.AdminPubKeys {
		if ak == p.OldPubKey {
			isAdmin = true
			break
		}
	}
	if !isAgent && !isAdmin {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "old_pubkey does not match current registered agent or admin key", "sign with current registered key", nowUnix)
	}
	if _, err := protocol.ParsePubKey(p.NewPubKey); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid new_pubkey format", "use ed25519:<64-hex>", nowUnix)
	}
	msg := []byte("rotate|" + env.From + "|" + p.NewPubKey)
	if err := protocol.VerifySignature(p.NewPubKey, msg, p.NewSig); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid new_sig proof of possession", "sign 'rotate|<agent_id>|<new_pubkey>' with new_pubkey", nowUnix)
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecKeyRotate, ledger.KeyRotateBody{
		AgentID:   env.From,
		OldPubKey: p.OldPubKey,
		NewPubKey: p.NewPubKey,
		NewSig:    p.NewSig,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if isAdmin {
		e.manifest.AdminPubKeys = append([]string(nil), e.state.AdminPubKeys...)
	}
	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handlePlaceOrder(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.PlaceOrderPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid place_order payload", "provide asset, side, limit_price, qty", nowUnix)
	}
	if e.state.HaltedGlobal || e.state.HaltedAssets[p.Asset] {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "market is currently halted", "wait for admin.resume", nowUnix)
	}
	if e.state.RoundStatus != ledger.RoundStatusOpen {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "current round is sealed", "submit order after the next round opens", nowUnix)
	}
	if e.strictCutoff && e.isPastCurrentCutoffLocked() {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "round cutoff time reached", "submit order after the next round opens", nowUnix)
	}
	if !e.manifest.HasAsset(p.Asset) {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnknownAsset, fmt.Sprintf("unknown asset %q", p.Asset), "check manifest.assets for valid asset IDs", nowUnix)
	}
	if p.Side != protocol.SideBuy && p.Side != protocol.SideSell {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "side must be 'buy' or 'sell'", "set side to 'buy' or 'sell'", nowUnix)
	}
	refPrice := e.game.ReferencePrice(p.Asset, e.state.CurrentRound)
	if p.Market {
		if refPrice <= 0 {
			return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "market order requires a positive reference price band", "specify an explicit limit_price", nowUnix)
		}
		minP, maxP := e.manifest.PriceBounds(refPrice)
		if p.Side == protocol.SideBuy {
			p.LimitPrice = maxP
		} else {
			p.LimitPrice = minP
		}
	}
	if p.Qty <= 0 || p.LimitPrice <= 0 {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "limit_price and qty must be positive integers", "use positive integer price and quantity", nowUnix)
	}
	totalVal, err := protocol.SafeMulInt(p.LimitPrice, p.Qty)
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "limit_price * qty exceeds maximum safe integer (2^53-1)", "reduce order quantity or limit_price", nowUnix)
	}

	// Price band check (Spec A8.2)
	if refPrice > 0 {
		minP, maxP := e.manifest.PriceBounds(refPrice)
		if p.LimitPrice < minP || p.LimitPrice > maxP {
			return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand,
				fmt.Sprintf("limit_price %d outside price band [%d, %d] (reference=%d)", p.LimitPrice, minP, maxP, refPrice),
				fmt.Sprintf("submit a limit_price between %d and %d", minP, maxP), nowUnix)
		}
	}

	// Self-cross check (Spec A8.3: buy limit >= sell limit on same asset in same round -> SELF_CROSS)
	if e.wouldSelfCross(env.From, p.Asset, p.Side, p.LimitPrice, "") {
		return nil, e.reject(env.CorrID, protocol.ErrCodeSelfCross,
			"order would cross your own open order on the opposite side",
			"ensure your buy limit_price is strictly less than your sell limit_price, or cancel the opposite order first", nowUnix)
	}

	// Escrow balance check (Spec A8.1)
	ag := e.state.Agents[env.From]
	cashUnit := e.state.SettlementUnitID()
	if p.Side == protocol.SideBuy {
		need := totalVal
		if ag == nil || ag.Available[cashUnit] < need {
			have := int64(0)
			if ag != nil {
				have = ag.Available[cashUnit]
			}
			return nil, e.reject(env.CorrID, protocol.ErrCodeInsufficientBalance,
				fmt.Sprintf("insufficient %s balance: available %d, required escrow %d", cashUnit, have, need),
				"reduce order quantity/limit_price or cancel existing open buy orders", nowUnix)
		}
	} else {
		if ag == nil || ag.Available[p.Asset] < p.Qty {
			have := int64(0)
			if ag != nil {
				have = ag.Available[p.Asset]
			}
			return nil, e.reject(env.CorrID, protocol.ErrCodeInsufficientBalance,
				fmt.Sprintf("insufficient %s balance: available %d, required escrow %d", p.Asset, have, p.Qty),
				"reduce sell quantity or cancel existing open sell orders", nowUnix)
		}
	}

	var orderID string
	for {
		e.orderSeq++
		orderID = fmt.Sprintf("ord_%d_%d", e.state.CurrentRound, e.orderSeq)
		if _, exists := e.state.OpenOrders[orderID]; !exists {
			break
		}
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecOrderAccepted, ledger.OrderAcceptedBody{
		OrderID:     orderID,
		Round:       e.state.CurrentRound,
		AgentID:     env.From,
		Asset:       p.Asset,
		Side:        p.Side,
		LimitPrice:  p.LimitPrice,
		Qty:         p.Qty,
		CarryOver:   p.CarryOver,
		TTLRounds:   p.TTLRounds,
		EnvelopeSig: env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}

	e.publishIndicativeUpdate(p.Asset)
	return e.makeReceipt(env.CorrID, orderID, "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleCancelOrder(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.CancelOrderPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid cancel_order payload", "provide order_id", nowUnix)
	}
	if e.state.HaltedGlobal {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "market is halted", "wait for admin.resume", nowUnix)
	}
	if e.state.RoundStatus != ledger.RoundStatusOpen {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "cannot cancel order after round is sealed", "wait for round settlement", nowUnix)
	}
	if e.strictCutoff && e.isPastCurrentCutoffLocked() {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "cannot cancel order at or after round cutoff time", "wait for round settlement", nowUnix)
	}
	ord, ok := e.state.OpenOrders[p.OrderID]
	if !ok {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOrderNotFound, fmt.Sprintf("open order %q not found", p.OrderID), "check get_portfolio for open order IDs", nowUnix)
	}
	if ord.AgentID != env.From {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "cannot cancel another agent's order", "specify an order_id owned by your agent", nowUnix)
	}
	if e.state.HaltedAssets[ord.Asset] {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "market is halted", "wait for admin.resume", nowUnix)
	}

	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecOrderCancelled, ledger.OrderCancelledBody{
		OrderID:     p.OrderID,
		Round:       e.state.CurrentRound,
		AgentID:     env.From,
		EnvelopeSig: env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}

	e.publishIndicativeUpdate(ord.Asset)
	return e.makeReceipt(env.CorrID, p.OrderID, "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleReplaceOrder(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.ReplaceOrderPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid replace_order payload", "provide order_id, limit_price, qty", nowUnix)
	}
	if e.state.HaltedGlobal {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "market is halted", "wait for admin.resume", nowUnix)
	}
	if e.state.RoundStatus != ledger.RoundStatusOpen {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "cannot replace order after round is sealed", "wait for next round", nowUnix)
	}
	if e.strictCutoff && e.isPastCurrentCutoffLocked() {
		return nil, e.reject(env.CorrID, protocol.ErrCodeRoundSealed, "cannot replace order at or after round cutoff time", "wait for next round", nowUnix)
	}
	oldOrd, ok := e.state.OpenOrders[p.OrderID]
	if !ok {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOrderNotFound, fmt.Sprintf("open order %q not found", p.OrderID), "check get_portfolio for open order IDs", nowUnix)
	}
	if oldOrd.AgentID != env.From {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "cannot replace another agent's order", "specify an order_id owned by your agent", nowUnix)
	}
	if e.state.HaltedAssets[oldOrd.Asset] {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "market is halted", "wait for admin.resume", nowUnix)
	}
	if p.Qty <= 0 || p.LimitPrice <= 0 {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "limit_price and qty must be positive integers", "use positive integer price and quantity", nowUnix)
	}
	totalVal, err := protocol.SafeMulInt(p.LimitPrice, p.Qty)
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "limit_price * qty exceeds maximum safe integer (2^53-1)", "reduce replacement qty or limit_price", nowUnix)
	}

	refPrice := e.game.ReferencePrice(oldOrd.Asset, e.state.CurrentRound)
	if refPrice > 0 {
		minP, maxP := e.manifest.PriceBounds(refPrice)
		if p.LimitPrice < minP || p.LimitPrice > maxP {
			return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand,
				fmt.Sprintf("limit_price %d outside price band [%d, %d]", p.LimitPrice, minP, maxP),
				fmt.Sprintf("submit a limit_price between %d and %d", minP, maxP), nowUnix)
		}
	}

	if e.wouldSelfCross(env.From, oldOrd.Asset, oldOrd.Side, p.LimitPrice, oldOrd.OrderID) {
		return nil, e.reject(env.CorrID, protocol.ErrCodeSelfCross,
			"replaced order would cross your own open order on the opposite side",
			"adjust limit_price so buy < sell", nowUnix)
	}

	ag := e.state.Agents[env.From]
	availPlusOld := ag.Available[oldOrd.LockedUnit] + oldOrd.LockedAmt
	var newLock int64
	if oldOrd.Side == protocol.SideBuy {
		newLock = totalVal
	} else {
		newLock = p.Qty
	}
	if availPlusOld < newLock {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInsufficientBalance,
			fmt.Sprintf("insufficient %s balance for replace: available+released=%d, required=%d", oldOrd.LockedUnit, availPlusOld, newLock),
			"reduce replacement qty or limit_price", nowUnix)
	}

	var newOrderID string
	for {
		e.orderSeq++
		newOrderID = fmt.Sprintf("ord_%d_%d", e.state.CurrentRound, e.orderSeq)
		if _, exists := e.state.OpenOrders[newOrderID]; !exists {
			break
		}
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecOrderReplaced, ledger.OrderReplacedBody{
		OldOrderID:  oldOrd.OrderID,
		NewOrderID:  newOrderID,
		Round:       e.state.CurrentRound,
		AgentID:     env.From,
		Asset:       oldOrd.Asset,
		Side:        oldOrd.Side,
		LimitPrice:  p.LimitPrice,
		Qty:         p.Qty,
		EnvelopeSig: env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}

	e.publishIndicativeUpdate(oldOrd.Asset)
	return e.makeReceipt(env.CorrID, newOrderID, "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) wouldSelfCross(agentID, asset string, side protocol.Side, limitPrice int64, ignoreOrderID string) bool {
	for _, o := range e.state.OpenOrders {
		if o.OrderID == ignoreOrderID || o.AgentID != agentID || o.Asset != asset || o.Qty <= 0 {
			continue
		}
		if side == protocol.SideBuy && o.Side == protocol.SideSell {
			if limitPrice >= o.LimitPrice {
				return true
			}
		} else if side == protocol.SideSell && o.Side == protocol.SideBuy {
			if o.LimitPrice >= limitPrice {
				return true
			}
		}
	}
	return false
}

func (e *Engine) handleBoardPost(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	if e.boardStore == nil || !e.boardLimits.Enabled {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardDisabled, "board extension is disabled", "check manifest.board.enabled", nowUnix)
	}
	if e.state.HaltedGlobal {
		return nil, e.reject(env.CorrID, protocol.ErrCodeHalted, "game is halted", "wait for admin.resume", nowUnix)
	}
	if e.state.RoundStatus != ledger.RoundStatusOpen {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardBlackout, "round is sealed", "wait for next round", nowUnix)
	}

	var p protocol.BoardPostPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid board.post payload", "provide asset and body", nowUnix)
	}
	if p.Asset != "general" && !e.manifest.HasAsset(p.Asset) {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnknownAsset, fmt.Sprintf("unknown board asset %q", p.Asset), "use a manifest asset ID or 'general'", nowUnix)
	}
	if e.boardLimits.RequireParticipant {
		if _, ok := e.state.Agents[env.From]; !ok {
			return nil, e.reject(env.CorrID, protocol.ErrCodeBoardNotParticipant, "only admitted participants may post", "register first", nowUnix)
		}
	}

	// Check blackout window before round cutoff
	if e.boardLimits.BlackoutBeforeCutoffSec > 0 {
		_, end, err := e.manifest.RoundBounds(e.state.CurrentRound)
		if err == nil {
			secsLeft := int64(end.Sub(e.clock.Now()).Seconds())
			if secsLeft >= 0 && secsLeft < e.boardLimits.BlackoutBeforeCutoffSec {
				return nil, e.reject(env.CorrID, protocol.ErrCodeBoardBlackout,
					fmt.Sprintf("board is in pre-cutoff blackout (%ds remaining)", secsLeft),
					"wait until the next round opens to post", nowUnix)
			}
		}
	}

	// Rate limit checks
	roundPosts := e.postsCountByRound[e.state.CurrentRound]
	if roundPosts == nil {
		roundPosts = make(map[string]int64)
		e.postsCountByRound[e.state.CurrentRound] = roundPosts
	}
	if e.boardLimits.MaxPostsPerAgentPerRound > 0 && roundPosts[env.From] >= e.boardLimits.MaxPostsPerAgentPerRound {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardRateLimit,
			fmt.Sprintf("exceeded max_posts_per_agent_per_round (%d)", e.boardLimits.MaxPostsPerAgentPerRound),
			"wait for the next round", nowUnix)
	}
	if e.boardLimits.MinIntervalSec > 0 {
		if last, ok := e.lastPostTime[env.From]; ok && (nowUnix-last) < e.boardLimits.MinIntervalSec {
			return nil, e.reject(env.CorrID, protocol.ErrCodeBoardRateLimit,
				fmt.Sprintf("minimum interval between posts is %ds", e.boardLimits.MinIntervalSec),
				"wait before posting again", nowUnix)
		}
	}

	cleanBody := board.SanitizeBody(p.Body)
	maxChars := e.boardLimits.MaxBodyChars
	if maxChars <= 0 {
		maxChars = 280
	}
	if cleanBody == "" || board.CharCount(cleanBody) > maxChars {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardTooLong,
			fmt.Sprintf("post body length must be 1..%d chars", maxChars),
			"shorten post body", nowUnix)
	}
	if err := board.ValidatePlainText(cleanBody); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardTooLong,
			err.Error(),
			"remove URLs, markdown links, and images from board post", nowUnix)
	}

	payloadHash, err := protocol.HashCanonical(protocol.BoardPostPayload{
		Asset:   p.Asset,
		Stance:  p.Stance,
		Body:    cleanBody,
		ReplyTo: p.ReplyTo,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}

	e.postSeq++
	postID := fmt.Sprintf("post_%d_%d", e.state.CurrentRound, e.postSeq)
	anchorBytes, _ := json.Marshal(board.AnchorRecordBody{
		PostID:      postID,
		Author:      env.From,
		Asset:       p.Asset,
		Stance:      p.Stance,
		Round:       e.state.CurrentRound,
		ContentHash: payloadHash,
	})
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecExt, ledger.ExtBody{
		Namespace: "board",
		Body:      anchorBytes,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}

	_ = e.boardStore.Put(board.Post{
		PostID:        postID,
		Round:         e.state.CurrentRound,
		Seq:           rec.Seq,
		Author:        env.From,
		Asset:         p.Asset,
		Stance:        p.Stance,
		UntrustedText: cleanBody,
		ReplyTo:       p.ReplyTo,
		Timestamp:     nowUnix,
		ContentHash:   payloadHash,
	})
	roundPosts[env.From]++
	e.lastPostTime[env.From] = nowUnix

	return e.makeReceipt(env.CorrID, "", postID, e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleBoardRetract(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	if e.boardStore == nil || !e.boardLimits.Enabled {
		return nil, e.reject(env.CorrID, protocol.ErrCodeBoardDisabled, "board disabled", "enable board", nowUnix)
	}
	var p protocol.BoardRetractPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid board.retract payload", "provide post_id", nowUnix)
	}
	post, ok := e.boardStore.Get(p.PostID)
	if !ok || post.Author != env.From {
		return nil, e.reject(env.CorrID, protocol.ErrCodeUnauthorized, "post not found or not authored by you", "provide your own post_id", nowUnix)
	}
	e.boardStore.Hide(p.PostID, false)

	retractBytes, _ := json.Marshal(map[string]any{"action": "retract", "post_id": p.PostID, "author": env.From})
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecExt, ledger.ExtBody{
		Namespace: "board",
		Body:      retractBytes,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	return e.makeReceipt(env.CorrID, "", p.PostID, e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleAdminHalt(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.AdminHaltPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid admin.halt payload", "provide scope and reason", nowUnix)
	}
	adminPub, _ := e.resolveSenderPubKey(env.Type, env.From, env.Payload)
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecAdminHalt, ledger.AdminHaltRecordBody{
		Round:       e.state.CurrentRound,
		Scope:       p.Scope,
		Reason:      p.Reason,
		AdminPubKey: adminPub,
		AdminSig:    env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleAdminResume(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.AdminResumePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid admin.resume payload", "provide scope", nowUnix)
	}
	adminPub, _ := e.resolveSenderPubKey(env.Type, env.From, env.Payload)
	nextRound := e.state.CurrentRound
	if e.state.RoundStatus == ledger.RoundStatusVoided || e.state.RoundStatus == ledger.RoundStatusSettled {
		nextRound++
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecAdminResume, ledger.AdminResumeRecordBody{
		Round:       nextRound,
		Scope:       p.Scope,
		Reason:      p.Reason,
		AdminPubKey: adminPub,
		AdminSig:    env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleAdminModerate(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.AdminModeratePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid admin.moderate payload", "provide post_id and reason", nowUnix)
	}
	if strings.TrimSpace(p.PostID) == "" {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "post_id must not be empty", "provide a valid post_id to moderate", nowUnix)
	}
	adminPub, _ := e.resolveSenderPubKey(env.Type, env.From, env.Payload)
	if e.boardStore != nil {
		e.boardStore.Hide(p.PostID, true)
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecAdminModerate, ledger.AdminModerateRecordBody{
		PostID:      p.PostID,
		Reason:      p.Reason,
		AdminPubKey: adminPub,
		AdminSig:    env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	return e.makeReceipt(env.CorrID, "", p.PostID, e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleAdminReset(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.AdminResetPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid admin.reset payload", "provide season_id, reason, and optional initial_balances", nowUnix)
	}
	adminPub, _ := e.resolveSenderPubKey(env.Type, env.From, env.Payload)

	initBalances := make(map[string]int64)
	if len(p.InitialBalances) > 0 {
		for asset, qty := range p.InitialBalances {
			if qty > 0 {
				initBalances[asset] = qty
			}
		}
	} else if len(e.manifest.InitialBalances) > 0 {
		for asset, qty := range e.manifest.InitialBalances {
			if qty > 0 {
				initBalances[asset] = qty
			}
		}
	} else if defaultGrants, err := e.game.Admit(ctx, AdmissionRequest{}); err == nil {
		for _, chg := range defaultGrants {
			if chg.Qty > 0 {
				initBalances[chg.Asset] += chg.Qty
			}
		}
	}

	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecAdminReset, ledger.AdminResetRecordBody{
		Round:           e.state.CurrentRound,
		SeasonID:        p.SeasonID,
		Reason:          p.Reason,
		InitialBalances: initBalances,
		ClearHistory:    p.ClearHistory,
		AdminPubKey:     adminPub,
		AdminSig:        env.Signature,
	})
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if p.ClearHistory {
		e.market = marketdata.NewTracker()
		if e.boardStore != nil {
			e.boardStore = board.NewMemoryStore()
		}
		e.postsCountByRound = make(map[uint64]map[string]int64)
		e.lastPostTime = make(map[string]int64)
		e.nonces = make(map[string]nonceRecord)
	}
	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

func (e *Engine) handleManifestUpdate(ctx context.Context, env *protocol.Envelope, contentHash string, nowUnix int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection) {
	var p protocol.ManifestUpdatePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, "invalid manifest.update payload", "provide new_hash and effective_round", nowUnix)
	}
	if p.EffectiveRound <= e.state.CurrentRound {
		return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, "manifest.update effective_round must be a future round", "set effective_round > current_round", nowUnix)
	}
	if p.Interval != "" {
		if _, err := manifest.ParseISO8601Duration(p.Interval); err != nil {
			return nil, e.reject(env.CorrID, protocol.ErrCodeOutOfBand, fmt.Sprintf("invalid round interval %q: %v", p.Interval, err), "use ISO 8601 duration such as PT1H, PT1M, PT10S", nowUnix)
		}
		e.pendingIntervals[p.EffectiveRound] = p.Interval
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecManifestUpdate, p)
	if err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return nil, e.reject(env.CorrID, protocol.ErrCodeInvalidSignature, err.Error(), "retry", nowUnix)
	}
	return e.makeReceipt(env.CorrID, "", "", e.state.CurrentRound, rec.Seq, contentHash), nil
}

// SealAndClearRound executes the batch call-auction clearing lifecycle for the current open round:
// Open -> Sealed (round_sealed) -> Cleared (clear + fill per asset) -> Settled (settle) -> Next Round Open.
func (e *Engine) SealAndClearRound(ctx context.Context) ([]protocol.Fill, error) {
	e.clearMu.Lock()
	defer e.clearMu.Unlock()
	return e.sealAndClearRoundWithClearMuHeld(ctx)
}

func (e *Engine) sealAndClearRoundWithClearMuHeld(ctx context.Context) ([]protocol.Fill, error) {
	e.mu.Lock()
	if e.state.HaltedGlobal {
		e.mu.Unlock()
		return nil, fmt.Errorf("cannot clear while globally halted")
	}
	if e.state.RoundStatus != ledger.RoundStatusOpen {
		e.mu.Unlock()
		return nil, fmt.Errorf("current round is not open (status=%s)", e.state.RoundStatus)
	}
	round := e.state.CurrentRound
	nowUnix := e.clock.Now().Unix()

	refPrices := make(map[string]int64, len(e.manifest.Assets))
	for _, a := range e.manifest.Assets {
		refPrices[a.ID] = e.game.ReferencePrice(a.ID, round)
	}

	seq, prevHash := e.state.LastSeq+1, e.state.LastHash
	var batch []ledger.Record

	// 1. round_sealed record
	sealRec, err := e.buildRecord(seq, prevHash, ledger.RecRoundSealed, ledger.RoundSealedBody{
		Round:      round,
		CutoffTime: nowUnix,
		RefPrices:  refPrices,
	})
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	batch = append(batch, sealRec)
	prevHash = sealRec.Hash
	seq++

	// Derive round lottery seed once (Spec A9.6)
	seed, seedSig, err := protocol.DeriveLotterySeed(e.signer, e.manifest.GameID, round)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}

	if e.lastAnatomy[round] == nil {
		e.lastAnatomy[round] = make(map[string]clearing.ClearingResult)
	}

	// Sort assets by ID for deterministic ledger record ordering
	assets := make([]string, 0, len(e.manifest.Assets))
	for _, a := range e.manifest.Assets {
		assets = append(assets, a.ID)
	}
	sort.Strings(assets)

	var allFills []protocol.Fill

	// 2. Clear each asset independently
	for _, assetID := range assets {
		if e.state.HaltedAssets[assetID] {
			continue
		}
		refP := refPrices[assetID]
		bandMin, bandMax := e.manifest.PriceBounds(refP)
		prevP := e.state.PrevClearingPrices[assetID]

		var orders []clearing.Order
		for _, o := range e.state.OpenOrders {
			if o.Asset == assetID && o.Qty > 0 {
				if o.Round < round && refP > 0 && (o.LimitPrice < bandMin || o.LimitPrice > bandMax) {
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
			GameID:            e.manifest.GameID,
			Round:             round,
			Asset:             assetID,
			Orders:            orders,
			PrevClearingPrice: prevP,
			ReferencePrice:    refP,
			BandMin:           bandMin,
			BandMax:           bandMax,
			Seed:              seed,
			SeedSig:           seedSig,
		})
		e.lastAnatomy[round][assetID] = res

		clearRec, err := e.buildRecord(seq, prevHash, ledger.RecClear, ledger.ClearBody{
			Round:          round,
			Asset:          assetID,
			ClearingPrice:  res.ClearingPrice,
			Volume:         res.Volume,
			PrevPrice:      prevP,
			ReferencePrice: refP,
			SeedSig:        seedSig,
		})
		if err != nil {
			e.mu.Unlock()
			return nil, err
		}
		batch = append(batch, clearRec)
		prevHash = clearRec.Hash
		seq++

		for _, f := range res.Fills {
			fillRec, err := e.buildRecord(seq, prevHash, ledger.RecFill, f)
			if err != nil {
				e.mu.Unlock()
				return nil, err
			}
			batch = append(batch, fillRec)
			prevHash = fillRec.Hash
			seq++
			allFills = append(allFills, f)
		}
	}

	// 3. Settle record
	settleRec, err := e.buildRecord(seq, prevHash, ledger.RecSettle, ledger.SettleBody{
		Round:     round,
		CarryOver: e.manifest.Round.CarryOver,
	})
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	batch = append(batch, settleRec)

	// Append atomic clearing + settlement batch (Settle record advances state to round+1 Open)
	_, roundEnd, roundBoundsErr := e.manifest.RoundBounds(round)
	if err := e.appendAndApply(ctx, batch...); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	nextRound := round + 1
	if newInterval, ok := e.pendingIntervals[nextRound]; ok {
		delete(e.pendingIntervals, nextRound)
		if roundBoundsErr == nil {
			_ = e.reanchorRoundLocked(nextRound, roundEnd, newInterval)
		}
	}
	// Evict stale per-round caches (Anatomy, Board post counts, Nonces) older than 128 rounds
	if round > 128 {
		cutoff := round - 128
		for r := range e.lastAnatomy {
			if r < cutoff {
				delete(e.lastAnatomy, r)
			}
		}
		for r := range e.postsCountByRound {
			if r < cutoff {
				delete(e.postsCountByRound, r)
			}
		}
		for k, nr := range e.nonces {
			if nr.round > 0 && nr.round < cutoff {
				delete(e.nonces, k)
			}
		}
	}
	e.mu.Unlock()

	// Notify game hook
	if err := e.game.OnSettled(ctx, round, allFills); err != nil {
		return allFills, err
	}

	e.publishEvent(StreamEvent{
		Type:    "round_cleared",
		Round:   round,
		Payload: allFills,
	})
	e.publishEvent(StreamEvent{
		Type:    "round_opened",
		Round:   round + 1,
		Payload: map[string]any{"round": round + 1},
	})

	return allFills, nil
}

func (e *Engine) isPastCurrentCutoffLocked() bool {
	_, end, err := e.manifest.RoundBounds(e.state.CurrentRound)
	if err != nil {
		return false
	}
	return !e.clock.Now().Before(end)
}

func (e *Engine) reanchorRoundLocked(targetRound uint64, targetRoundStart time.Time, newInterval string) error {
	dur, err := manifest.ParseISO8601Duration(newInterval)
	if err != nil {
		return err
	}
	newAnchor := targetRoundStart.UTC().Add(-time.Duration(targetRound-1) * dur)
	e.manifest.Round.Anchor = newAnchor.Format(time.RFC3339)
	e.manifest.Round.Interval = newInterval
	return nil
}

// CurrentRoundBounds returns the active round number and its scheduled [start, end) UTC timestamps.
// The end timestamp is the exact scheduled clearing cutoff ("정시") for the active round.
func (e *Engine) CurrentRoundBounds() (round uint64, start, end time.Time, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r := e.state.CurrentRound
	s, endT, err := e.manifest.RoundBounds(r)
	return r, s, endT, err
}

// SetRoundInterval dynamically changes the round settlement interval (e.g. "PT1H" -> "PT1M" or "PT10S")
// starting from the current open round while preserving the current round's start timestamp.
func (e *Engine) SetRoundInterval(newInterval string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	start, _, err := e.manifest.RoundBounds(e.state.CurrentRound)
	if err != nil {
		return err
	}
	return e.reanchorRoundLocked(e.state.CurrentRound, start, newInterval)
}

// Tick checks whether the engine's clock has reached or passed the current round's scheduled cutoff time (end).
// If so, it automatically executes SealAndClearRound(ctx) and returns (true, fills, nil).
func (e *Engine) Tick(ctx context.Context) (bool, []protocol.Fill, error) {
	e.clearMu.Lock()
	defer e.clearMu.Unlock()

	e.mu.RLock()
	halted := e.state.HaltedGlobal
	status := e.state.RoundStatus
	past := e.isPastCurrentCutoffLocked()
	e.mu.RUnlock()

	if halted || status != ledger.RoundStatusOpen || !past {
		return false, nil, nil
	}
	fills, err := e.sealAndClearRoundWithClearMuHeld(ctx)
	if err != nil {
		return false, nil, err
	}
	return true, fills, nil
}

// StartScheduler starts a background loop that polls the engine clock every checkInterval
// and automatically triggers SealAndClearRound when the current round's scheduled cutoff time arrives.
func (e *Engine) StartScheduler(ctx context.Context, checkInterval time.Duration) {
	if checkInterval <= 0 {
		checkInterval = 100 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _, _ = e.Tick(ctx)
			}
		}
	}()
}

// CreditOrDebitBatch appends an atomic batch of balance changes (e.g. shop purchase = debit cash + credit material, Spec A12).
func (e *Engine) CreditOrDebitBatch(ctx context.Context, changes []struct {
	AgentID string
	Asset   string
	Delta   int64 // positive = credit, negative = debit
	Reason  string
}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Simulate and pre-validate balance changes to prevent log corruption on debit overdraft
	simAvail := make(map[string]map[string]int64)
	for _, c := range changes {
		if c.Delta == 0 {
			continue
		}
		if simAvail[c.AgentID] == nil {
			simAvail[c.AgentID] = make(map[string]int64)
		}
		if _, ok := simAvail[c.AgentID][c.Asset]; !ok {
			avail := int64(0)
			if ag := e.state.Agents[c.AgentID]; ag != nil {
				avail = ag.Available[c.Asset]
			}
			simAvail[c.AgentID][c.Asset] = avail
		}
		simAvail[c.AgentID][c.Asset] += c.Delta
		if simAvail[c.AgentID][c.Asset] < 0 {
			return fmt.Errorf("insufficient balance for debit on %s (%s): required %d", c.AgentID, c.Asset, -c.Delta)
		}
	}

	seq, prevHash := e.state.LastSeq+1, e.state.LastHash
	var recs []ledger.Record
	for _, c := range changes {
		if c.Delta == 0 {
			continue
		}
		recType := ledger.RecBalanceCredit
		qty := c.Delta
		if c.Delta < 0 {
			recType = ledger.RecBalanceDebit
			qty = -c.Delta
		}
		r, err := e.buildRecord(seq, prevHash, recType, ledger.BalanceChangeBody{
			AgentID: c.AgentID,
			Asset:   c.Asset,
			Qty:     qty,
			Reason:  c.Reason,
		})
		if err != nil {
			return err
		}
		recs = append(recs, r)
		prevHash = r.Hash
		seq++
	}
	return e.appendAndApply(ctx, recs...)
}

// AppendExt appends a custom game extension record `ext{namespace, body}` to the ledger (Spec A12).
func (e *Engine) AppendExt(ctx context.Context, namespace string, body any) (ledger.Record, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rawBody, err := json.Marshal(body)
	if err != nil {
		return ledger.Record{}, err
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecExt, ledger.ExtBody{
		Namespace: namespace,
		Body:      rawBody,
	})
	if err != nil {
		return ledger.Record{}, err
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return ledger.Record{}, err
	}
	return rec, nil
}

// AppendCheckpoint appends a `snapshot_checkpoint` record containing the current state_hash (Spec A12).
func (e *Engine) AppendCheckpoint(ctx context.Context) (ledger.Record, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sh, err := e.state.StateHash()
	if err != nil {
		return ledger.Record{}, err
	}
	rec, err := e.buildRecord(e.state.LastSeq+1, e.state.LastHash, ledger.RecSnapshotCheckpoint, ledger.SnapshotCheckpointBody{
		Round:     e.state.CurrentRound,
		StateHash: sh,
	})
	if err != nil {
		return ledger.Record{}, err
	}
	if err := e.appendAndApply(ctx, rec); err != nil {
		return ledger.Record{}, err
	}
	if saver, ok := e.log.(interface{ SaveSnapshot(ledger.Snapshot) }); ok {
		postHash, _ := e.state.StateHash()
		saver.SaveSnapshot(ledger.Snapshot{
			Seq:       rec.Seq,
			Hash:      rec.Hash,
			StateHash: postHash,
			State:     e.state.Snapshot(),
		})
	}
	return rec, nil
}

// Market data accessors (Spec A11)

func (e *Engine) Tape(asset string, rounds int) []marketdata.TapeEntry {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.market.Tape(asset, rounds)
}

func (e *Engine) Candles(asset string, rounds int) []marketdata.Candle {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.market.Candles(asset, rounds)
}

func (e *Engine) Stats(asset string, rounds int) marketdata.Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.market.Stats(asset, rounds)
}

func (e *Engine) Indicative(asset string) (marketdata.IndicativeQuote, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.manifest.MarketData.PreClear == "none" {
		return marketdata.IndicativeQuote{}, fmt.Errorf("pre-clear market data is disabled")
	}
	return e.computeIndicativeLocked(asset), nil
}

func (e *Engine) computeIndicativeLocked(asset string) marketdata.IndicativeQuote {
	round := e.state.CurrentRound
	refP := e.game.ReferencePrice(asset, round)
	minP, maxP := e.manifest.PriceBounds(refP)
	prevP := e.state.PrevClearingPrices[asset]

	var orders []clearing.Order
	for _, o := range e.state.OpenOrders {
		if o.Asset == asset && o.Qty > 0 {
			if o.Round < round && refP > 0 && (o.LimitPrice < minP || o.LimitPrice > maxP) {
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
	return marketdata.ComputeIndicative(e.manifest.GameID, round, asset, orders, prevP, refP, minP, maxP)
}

func (e *Engine) Depth(asset string) (marketdata.DepthBook, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.manifest.MarketData.PreClear != "depth" {
		return marketdata.DepthBook{}, fmt.Errorf("depth book only available when pre_clear is 'depth'")
	}
	round := e.state.CurrentRound
	refP := e.game.ReferencePrice(asset, round)
	minP, maxP := e.manifest.PriceBounds(refP)

	var orders []clearing.Order
	for _, o := range e.state.OpenOrders {
		if o.Asset == asset && o.Qty > 0 {
			if o.Round < round && refP > 0 && (o.LimitPrice < minP || o.LimitPrice > maxP) {
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
	return marketdata.ComputeDepth(round, asset, orders), nil
}

func (e *Engine) RoundAnatomy(round uint64, asset string) (clearing.ClearingResult, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if m, ok := e.lastAnatomy[round]; ok {
		res, exists := m[asset]
		return res, exists
	}
	return clearing.ClearingResult{}, false
}

func (e *Engine) BoardPosts(asset string, sinceSeq uint64, limit int) []board.Post {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.boardStore == nil {
		return nil
	}
	posts := e.boardStore.List(asset, sinceSeq, limit)
	if e.boardLimits.VisibilityDelaySec <= 0 {
		return posts
	}
	nowUnix := e.clock.Now().Unix()
	visible := make([]board.Post, 0, len(posts))
	for _, p := range posts {
		if p.Timestamp == 0 || nowUnix-p.Timestamp >= e.boardLimits.VisibilityDelaySec {
			visible = append(visible, p)
		}
	}
	return visible
}

func (e *Engine) TalkVsTrade(agentID, asset string, rounds uint64) board.TalkVsTradeReport {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var posts []board.Post
	if e.boardStore != nil {
		posts = e.boardStore.List(asset, 0, -1)
	}
	tape := e.market.Tape(asset, 0)
	return board.ComputeTalkVsTrade(agentID, asset, rounds, posts, tape)
}

func (e *Engine) publishIndicativeUpdate(asset string) {
	if e.manifest.MarketData.PreClear == "none" {
		return
	}
	ind := e.computeIndicativeLocked(asset)
	e.publishEvent(StreamEvent{
		Type:    "indicative_update",
		Round:   e.state.CurrentRound,
		Asset:   asset,
		Payload: ind,
	})
}

func (e *Engine) buildRecord(seq uint64, prevHash, recType string, body any) (ledger.Record, error) {
	rawBody, err := json.Marshal(body)
	if err != nil {
		return ledger.Record{}, err
	}
	rec := ledger.Record{
		Seq:      seq,
		PrevHash: prevHash,
		GameID:   e.manifest.GameID,
		Type:     recType,
		Body:     rawBody,
	}
	if err := ledger.SignAndHashRecord(&rec, e.signer); err != nil {
		return ledger.Record{}, err
	}
	return rec, nil
}

func (e *Engine) appendAndApply(ctx context.Context, recs ...ledger.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if _, err := e.log.Append(ctx, recs...); err != nil {
		return err
	}
	var projErr error
	for _, r := range recs {
		if err := e.state.Apply(r); err != nil {
			return err
		}
		e.market.Apply(r)
		for _, proj := range e.projections {
			if err := proj.Apply(ctx, r); err != nil && projErr == nil {
				projErr = err
			}
		}
	}
	return projErr
}

func (e *Engine) makeReceipt(corrID, orderID, postID string, round, seq uint64, contentHash string) *protocol.AcceptanceReceipt {
	r := &protocol.AcceptanceReceipt{
		GameID:      e.manifest.GameID,
		CorrID:      corrID,
		OrderID:     orderID,
		PostID:      postID,
		Round:       round,
		Seq:         seq,
		ContentHash: contentHash,
	}
	_ = protocol.SignReceipt(r, e.signer)
	return r
}

func (e *Engine) reject(corrID string, code protocol.ErrorCode, reason, fixHint string, ts int64) *protocol.SignedRejection {
	rej, _ := protocol.NewSignedRejection(e.manifest.GameID, corrID, code, reason, fixHint, ts, e.signer)
	return rej
}
