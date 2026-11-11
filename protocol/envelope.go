package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ProtocolID     = "agentsquare/1.0"
	CurrentVersion = "1.0"
)

// ErrorCode enumerates normative protocol rejection codes (Spec A6 & B1.3).
type ErrorCode string

const (
	ErrCodeInvalidSignature    ErrorCode = "INVALID_SIGNATURE"
	ErrCodeReplayedNonce       ErrorCode = "REPLAYED_NONCE"
	ErrCodeStaleTimestamp      ErrorCode = "STALE_TIMESTAMP"
	ErrCodeUnsupportedVersion  ErrorCode = "UNSUPPORTED_VERSION"
	ErrCodeUnknownAsset        ErrorCode = "UNKNOWN_ASSET"
	ErrCodeOutOfBand           ErrorCode = "OUT_OF_BAND"
	ErrCodeInsufficientBalance ErrorCode = "INSUFFICIENT_BALANCE"
	ErrCodeSelfCross           ErrorCode = "SELF_CROSS"
	ErrCodeRoundSealed         ErrorCode = "ROUND_SEALED"
	ErrCodeOrderNotFound       ErrorCode = "ORDER_NOT_FOUND"
	ErrCodeUnauthorized        ErrorCode = "UNAUTHORIZED"
	ErrCodeHalted              ErrorCode = "HALTED"
	ErrCodeBoardRateLimit      ErrorCode = "BOARD_RATE_LIMIT"
	ErrCodeBoardTooLong        ErrorCode = "BOARD_TOO_LONG"
	ErrCodeBoardBlackout       ErrorCode = "BOARD_BLACKOUT"
	ErrCodeBoardNotParticipant ErrorCode = "BOARD_NOT_PARTICIPANT"
	ErrCodeBoardDisabled       ErrorCode = "BOARD_DISABLED"
)

// Message types
const (
	TypeAgentRegister  = "agent.register"
	TypeKeyRotate      = "key.rotate"
	TypePlaceOrder     = "place_order"
	TypeCancelOrder    = "cancel_order"
	TypeReplaceOrder   = "replace_order"
	TypeFill           = "fill"
	TypeSettle         = "settle"
	TypeQuote          = "quote"
	TypeBoardPost      = "board.post"
	TypeBoardRetract   = "board.retract"
	TypeAdminHalt      = "admin.halt"
	TypeAdminResume    = "admin.resume"
	TypeAdminModerate  = "admin.moderate"
	TypeAdminReset     = "admin.reset"
	TypeManifestUpdate = "manifest.update"
)

// Side represents order direction ("buy" or "sell").
type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// Envelope is the standard signed message wrapper (Spec A6).
type Envelope struct {
	Version   string          `json:"version"`
	GameID    string          `json:"game_id"`
	From      string          `json:"from"`
	To        string          `json:"to"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Nonce     string          `json:"nonce"`
	CorrID    string          `json:"corr_id"`
	Critical  []string        `json:"critical,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Signature string          `json:"signature"`
}

// KnownTopLevelFields lists the standard envelope fields recognized by v1.0.
var KnownTopLevelFields = map[string]bool{
	"version":   true,
	"game_id":   true,
	"from":      true,
	"to":        true,
	"type":      true,
	"timestamp": true,
	"nonce":     true,
	"corr_id":   true,
	"critical":  true,
	"payload":   true,
	"signature": true,
}

// SignEnvelope signs an Envelope in-place over its RFC 8785 JCS representation (excluding "signature").
func SignEnvelope(env *Envelope, signer Signer) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	sig, _, err := SignRawEnvelopeJSON(raw, signer)
	if err != nil {
		return err
	}
	env.Signature = sig
	return nil
}

// SignRawEnvelopeJSON signs a raw JSON envelope (preserving any extra unknown fields) and returns
// the signature string and the full signed JSON bytes.
func SignRawEnvelopeJSON(raw []byte, signer Signer) (string, []byte, error) {
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return "", nil, err
	}
	delete(m, "signature")
	canon, err := MarshalCanonical(m)
	if err != nil {
		return "", nil, err
	}
	sig, err := signer.Sign(canon)
	if err != nil {
		return "", nil, err
	}
	m["signature"] = sig
	signedBytes, err := MarshalCanonical(m)
	if err != nil {
		return "", nil, err
	}
	return sig, signedBytes, nil
}

// VerifyRawEnvelope parses and verifies a raw JSON envelope against pubKeyStr.
// Spec A6 & A13: Signature covers the JCS of the whole envelope (without "signature"), INCLUDING unknown fields.
// Returns the parsed Envelope, the canonical content hash (for idempotent resend checks), and any error code.
func VerifyRawEnvelope(raw []byte, pubKeyStr string, knownPayloadFields map[string]bool) (*Envelope, string, ErrorCode, error) {
	// First canonicalize the entire raw JSON to enforce integer rules and valid syntax.
	if _, err := CanonicalizeJSON(raw); err != nil {
		return nil, "", ErrCodeInvalidSignature, err
	}

	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, "", ErrCodeInvalidSignature, err
	}

	sigVal, ok := m["signature"].(string)
	if !ok || sigVal == "" {
		return nil, "", ErrCodeInvalidSignature, fmt.Errorf("missing signature field")
	}

	// Compute JCS over envelope without "signature" (preserving all unknown fields!)
	delete(m, "signature")
	unsignedCanon, err := MarshalCanonical(m)
	if err != nil {
		return nil, "", ErrCodeInvalidSignature, err
	}

	if pubKeyStr != "" {
		if err := VerifySignature(pubKeyStr, unsignedCanon, sigVal); err != nil {
			return nil, "", ErrCodeInvalidSignature, err
		}
	}

	contentHash, err := HashCanonicalBytes(unsignedCanon)
	if err != nil {
		return nil, "", ErrCodeInvalidSignature, err
	}

	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, "", ErrCodeInvalidSignature, err
	}

	// Version check: major version must be "1"
	parts := strings.Split(env.Version, ".")
	if len(parts) != 2 || parts[0] != "1" {
		return &env, contentHash, ErrCodeUnsupportedVersion, fmt.Errorf("unsupported protocol version: %s", env.Version)
	}

	// Check critical fields (Spec A13: unknown critical -> reject with UNSUPPORTED_VERSION)
	if len(env.Critical) > 0 {
		payloadMap, _ := m["payload"].(map[string]any)
		for _, crit := range env.Critical {
			if KnownTopLevelFields[crit] {
				continue
			}
			if knownPayloadFields != nil && knownPayloadFields[crit] {
				continue
			}
			// Check if it exists in top-level or payload as an unknown critical field, or required extension
			if _, inTop := m[crit]; inTop {
				return &env, contentHash, ErrCodeUnsupportedVersion, fmt.Errorf("unknown critical field: %s", crit)
			}
			if payloadMap != nil {
				if _, inPayload := payloadMap[crit]; inPayload {
					return &env, contentHash, ErrCodeUnsupportedVersion, fmt.Errorf("unknown critical payload field: %s", crit)
				}
			}
			return &env, contentHash, ErrCodeUnsupportedVersion, fmt.Errorf("unrecognized critical requirement: %s", crit)
		}
	}

	return &env, contentHash, "", nil
}

// RegisterPayload is the payload for "agent.register".
type RegisterPayload struct {
	PubKey      string `json:"pubkey"`
	DisplayName string `json:"display_name,omitempty"`
	InviteCode  string `json:"invite_code,omitempty"`
}

// KeyRotatePayload is the payload for "key.rotate".
type KeyRotatePayload struct {
	OldPubKey string `json:"old_pubkey"`
	NewPubKey string `json:"new_pubkey"`
	NewSig    string `json:"new_sig"` // Signature by NewPubKey over "rotate|" + AgentID + "|" + NewPubKey
}

// PlaceOrderPayload is the payload for "place_order" (Spec A8: no counterparty field).
// By default (CarryOver=false, TTLRounds=0), any unfilled quantity is automatically cancelled and refunded at the end of the current round's settlement.
// Setting CarryOver=true keeps the unfilled order active across future rounds until filled or cancelled ("사질때까지 유지").
// Setting TTLRounds=N (N >= 1) keeps the unfilled order active for up to N settlement rounds ("N회차까지 유지") before automatic expiration.
type PlaceOrderPayload struct {
	Asset      string `json:"asset"`
	Side       Side   `json:"side"`
	LimitPrice int64  `json:"limit_price,omitempty"`
	Qty        int64  `json:"qty"`
	Market     bool   `json:"market,omitempty"`
	CarryOver  bool   `json:"carry_over,omitempty"`
	TTLRounds  uint64 `json:"ttl_rounds,omitempty"`
}

// CancelOrderPayload is the payload for "cancel_order".
type CancelOrderPayload struct {
	OrderID string `json:"order_id"`
}

// ReplaceOrderPayload is the payload for "replace_order".
type ReplaceOrderPayload struct {
	OrderID    string `json:"order_id"`
	LimitPrice int64  `json:"limit_price"`
	Qty        int64  `json:"qty"`
}

// BoardPostPayload is the payload for "board.post" (Spec B1.2).
type BoardPostPayload struct {
	Asset   string `json:"asset"`
	Stance  string `json:"stance,omitempty"`
	Body    string `json:"body"`
	ReplyTo string `json:"reply_to,omitempty"`
}

// BoardRetractPayload is the payload for "board.retract".
type BoardRetractPayload struct {
	PostID string `json:"post_id"`
}

// AdminHaltPayload is the payload for "admin.halt" (Spec B4).
type AdminHaltPayload struct {
	Scope  string `json:"scope"` // "all" or "asset:<id>"
	Reason string `json:"reason"`
}

// AdminResumePayload is the payload for "admin.resume".
type AdminResumePayload struct {
	Scope  string `json:"scope"`
	Reason string `json:"reason,omitempty"`
}

// AdminModeratePayload is the payload for "admin.moderate".
type AdminModeratePayload struct {
	PostID string `json:"post_id"`
	Reason string `json:"reason"`
}

// AdminResetPayload is the payload for "admin.reset" (weekly/seasonal full balance & orderbook reset).
// Only an authorized admin_pubkey may submit this action.
type AdminResetPayload struct {
	SeasonID        string           `json:"season_id,omitempty"`
	Reason          string           `json:"reason"`
	InitialBalances map[string]int64 `json:"initial_balances,omitempty"`
	ClearHistory    bool             `json:"clear_history,omitempty"`
}

// ManifestUpdatePayload is the payload for "manifest.update".
type ManifestUpdatePayload struct {
	NewHash        string `json:"new_hash"`
	EffectiveRound uint64 `json:"effective_round"`
	Interval       string `json:"interval,omitempty"`
}

// Fill represents an agent's clearing allocation in a round (Spec A8 & A9).
type Fill struct {
	GameID        string `json:"game_id"`
	Round         uint64 `json:"round"`
	Asset         string `json:"asset"`
	AgentID       string `json:"agent_id"`
	Side          Side   `json:"side"`
	FilledQty     int64  `json:"filled_qty"`
	ClearingPrice int64  `json:"clearing_price"`
	CashDelta     int64  `json:"cash_delta"` // Magnitude of cash paid (buy) or received (sell)
}

// AcceptanceReceipt is returned by the server when an order or post is accepted (Spec A8 & B1.2).
type AcceptanceReceipt struct {
	GameID      string `json:"game_id"`
	CorrID      string `json:"corr_id"`
	OrderID     string `json:"order_id,omitempty"`
	PostID      string `json:"post_id,omitempty"`
	Round       uint64 `json:"round"`
	Seq         uint64 `json:"seq"`
	ContentHash string `json:"content_hash"`
	ServerSig   string `json:"server_sig"`
}

// SignReceipt signs the receipt with the server key.
func SignReceipt(r *AcceptanceReceipt, signer Signer) error {
	cp := *r
	cp.ServerSig = ""
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	delete(m, "server_sig")
	canon, err := MarshalCanonical(m)
	if err != nil {
		return err
	}
	sig, err := signer.Sign(canon)
	if err != nil {
		return err
	}
	r.ServerSig = sig
	return nil
}

// VerifyReceipt verifies the server signature on an AcceptanceReceipt.
func VerifyReceipt(r *AcceptanceReceipt, serverPubKey string) error {
	cp := *r
	sig := cp.ServerSig
	cp.ServerSig = ""
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	delete(m, "server_sig")
	canon, err := MarshalCanonical(m)
	if err != nil {
		return err
	}
	return VerifySignature(serverPubKey, canon, sig)
}

// SignedRejection is the server-signed error response returned when a request is rejected (Spec A6).
type SignedRejection struct {
	GameID    string    `json:"game_id"`
	CorrID    string    `json:"corr_id"`
	Code      ErrorCode `json:"code"`
	Reason    string    `json:"reason"`
	FixHint   string    `json:"fix_hint"`
	Timestamp int64     `json:"timestamp"`
	ServerSig string    `json:"server_sig"`
}

// NewSignedRejection constructs and signs a rejection response.
func NewSignedRejection(gameID, corrID string, code ErrorCode, reason, fixHint string, timestamp int64, signer Signer) (*SignedRejection, error) {
	rej := &SignedRejection{
		GameID:    gameID,
		CorrID:    corrID,
		Code:      code,
		Reason:    reason,
		FixHint:   fixHint,
		Timestamp: timestamp,
	}
	if signer != nil {
		if err := SignRejection(rej, signer); err != nil {
			return nil, err
		}
	}
	return rej, nil
}

// SignRejection signs a SignedRejection using the server's key.
func SignRejection(rej *SignedRejection, signer Signer) error {
	m := map[string]any{
		"game_id":   rej.GameID,
		"corr_id":   rej.CorrID,
		"code":      string(rej.Code),
		"reason":    rej.Reason,
		"fix_hint":  rej.FixHint,
		"timestamp": rej.Timestamp,
	}
	canon, err := MarshalCanonical(m)
	if err != nil {
		return err
	}
	sig, err := signer.Sign(canon)
	if err != nil {
		return err
	}
	rej.ServerSig = sig
	return nil
}

// VerifyRejection verifies the server signature on a SignedRejection.
func VerifyRejection(rej *SignedRejection, serverPubKey string) error {
	m := map[string]any{
		"game_id":   rej.GameID,
		"corr_id":   rej.CorrID,
		"code":      string(rej.Code),
		"reason":    rej.Reason,
		"fix_hint":  rej.FixHint,
		"timestamp": rej.Timestamp,
	}
	canon, err := MarshalCanonical(m)
	if err != nil {
		return err
	}
	return VerifySignature(serverPubKey, canon, rej.ServerSig)
}
