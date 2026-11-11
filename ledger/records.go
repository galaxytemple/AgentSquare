package ledger

import (
	"encoding/json"
	"fmt"

	"github.com/galaxytemple/AgentSquare/protocol"
)

const (
	ZeroHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

// Record types (Spec A12)
const (
	RecGenesis            = "genesis"
	RecAgentAdmitted      = "agent.admitted"
	RecKeyRotate          = "key.rotate"
	RecBalanceCredit      = "balance.credit"
	RecBalanceDebit       = "balance.debit"
	RecOrderAccepted      = "order_accepted"
	RecOrderCancelled     = "order_cancelled"
	RecOrderReplaced      = "order_replaced"
	RecRoundSealed        = "round_sealed"
	RecClear              = "clear"
	RecFill               = "fill"
	RecSettle             = "settle"
	RecSnapshotCheckpoint = "snapshot_checkpoint"
	RecManifestUpdate     = "manifest.update"
	RecAdminHalt          = "admin.halt"
	RecAdminResume        = "admin.resume"
	RecAdminModerate      = "admin.moderate"
	RecAdminReset         = "admin.reset"
	RecExt                = "ext"
)

// Record is a single hash-chained append-only ledger entry (Spec A12).
type Record struct {
	Seq       uint64          `json:"seq"`
	PrevHash  string          `json:"prev_hash"`
	GameID    string          `json:"game_id"`
	Type      string          `json:"type"`
	Body      json.RawMessage `json:"body"`
	ServerSig string          `json:"server_sig"`
	Hash      string          `json:"hash"`
}

// SignAndHashRecord populates ServerSig and Hash on rec.
// ServerSig signs JCS(record without "hash" and without "server_sig").
// Hash is SHA-256(JCS(record without "hash")).
func SignAndHashRecord(rec *Record, signer protocol.Signer) error {
	var bodyAny any
	if err := json.Unmarshal(rec.Body, &bodyAny); err != nil {
		return fmt.Errorf("invalid record body JSON: %w", err)
	}
	unsignedMap := map[string]any{
		"seq":       rec.Seq,
		"prev_hash": rec.PrevHash,
		"game_id":   rec.GameID,
		"type":      rec.Type,
		"body":      bodyAny,
	}
	unsignedCanon, err := protocol.MarshalCanonical(unsignedMap)
	if err != nil {
		return err
	}
	sig, err := signer.Sign(unsignedCanon)
	if err != nil {
		return err
	}
	rec.ServerSig = sig

	signedMap := map[string]any{
		"seq":        rec.Seq,
		"prev_hash":  rec.PrevHash,
		"game_id":    rec.GameID,
		"type":       rec.Type,
		"body":       bodyAny,
		"server_sig": rec.ServerSig,
	}
	h, err := protocol.HashCanonical(signedMap)
	if err != nil {
		return err
	}
	rec.Hash = h
	return nil
}

// VerifyRecord checks the record's ServerSig and Hash against serverPubKey.
func VerifyRecord(rec Record, serverPubKey string) error {
	var bodyAny any
	if err := json.Unmarshal(rec.Body, &bodyAny); err != nil {
		return fmt.Errorf("seq %d: invalid body JSON: %w", rec.Seq, err)
	}
	unsignedMap := map[string]any{
		"seq":       rec.Seq,
		"prev_hash": rec.PrevHash,
		"game_id":   rec.GameID,
		"type":      rec.Type,
		"body":      bodyAny,
	}
	unsignedCanon, err := protocol.MarshalCanonical(unsignedMap)
	if err != nil {
		return fmt.Errorf("seq %d: canonicalize unsigned failed: %w", rec.Seq, err)
	}
	if serverPubKey != "" {
		if err := protocol.VerifySignature(serverPubKey, unsignedCanon, rec.ServerSig); err != nil {
			return fmt.Errorf("seq %d: invalid server signature: %w", rec.Seq, err)
		}
	}

	signedMap := map[string]any{
		"seq":        rec.Seq,
		"prev_hash":  rec.PrevHash,
		"game_id":    rec.GameID,
		"type":       rec.Type,
		"body":       bodyAny,
		"server_sig": rec.ServerSig,
	}
	expectedHash, err := protocol.HashCanonical(signedMap)
	if err != nil {
		return fmt.Errorf("seq %d: canonicalize signed failed: %w", rec.Seq, err)
	}
	if rec.Hash != expectedHash {
		return fmt.Errorf("seq %d: hash mismatch: got %s, want %s", rec.Seq, rec.Hash, expectedHash)
	}
	return nil
}

// GenesisBody is the body of a "genesis" record.
type GenesisBody struct {
	ManifestHash string `json:"manifest_hash"`
	PrevHeadHash string `json:"prev_head_hash,omitempty"` // For multi-season archival chaining (Spec A12)
}

// AgentAdmittedBody is the body of an "agent.admitted" record.
type AgentAdmittedBody struct {
	AgentID     string `json:"agent_id"`
	PubKey      string `json:"pubkey"`
	DisplayName string `json:"display_name,omitempty"`
}

// KeyRotateBody is the body of a "key.rotate" record.
type KeyRotateBody struct {
	AgentID   string `json:"agent_id"`
	OldPubKey string `json:"old_pubkey"`
	NewPubKey string `json:"new_pubkey"`
	NewSig    string `json:"new_sig"`
}

// BalanceChangeBody is the body of "balance.credit" and "balance.debit" records.
type BalanceChangeBody struct {
	AgentID string `json:"agent_id"`
	Asset   string `json:"asset"`
	Qty     int64  `json:"qty"`
	Reason  string `json:"reason"`
}

// OrderAcceptedBody is the body of an "order_accepted" record.
type OrderAcceptedBody struct {
	OrderID     string        `json:"order_id"`
	Round       uint64        `json:"round"`
	AgentID     string        `json:"agent_id"`
	Asset       string        `json:"asset"`
	Side        protocol.Side `json:"side"`
	LimitPrice  int64         `json:"limit_price"`
	Qty         int64         `json:"qty"`
	CarryOver   bool          `json:"carry_over,omitempty"`
	TTLRounds   uint64        `json:"ttl_rounds,omitempty"`
	EnvelopeSig string        `json:"envelope_sig"`
}

// OrderCancelledBody is the body of an "order_cancelled" record.
type OrderCancelledBody struct {
	OrderID     string `json:"order_id"`
	Round       uint64 `json:"round"`
	AgentID     string `json:"agent_id"`
	EnvelopeSig string `json:"envelope_sig"`
}

// OrderReplacedBody is the body of an "order_replaced" record.
type OrderReplacedBody struct {
	OldOrderID  string        `json:"old_order_id"`
	NewOrderID  string        `json:"new_order_id"`
	Round       uint64        `json:"round"`
	AgentID     string        `json:"agent_id"`
	Asset       string        `json:"asset"`
	Side        protocol.Side `json:"side"`
	LimitPrice  int64         `json:"limit_price"`
	Qty         int64         `json:"qty"`
	EnvelopeSig string        `json:"envelope_sig"`
}

// RoundSealedBody is the body of a "round_sealed" record.
type RoundSealedBody struct {
	Round      uint64           `json:"round"`
	CutoffTime int64            `json:"cutoff_time"`
	RefPrices  map[string]int64 `json:"ref_prices,omitempty"`
}

// ClearBody is the body of a "clear" record for a specific asset in a round.
type ClearBody struct {
	Round          uint64 `json:"round"`
	Asset          string `json:"asset"`
	ClearingPrice  int64  `json:"clearing_price"`
	Volume         int64  `json:"volume"`
	PrevPrice      int64  `json:"prev_price"`
	ReferencePrice int64  `json:"reference_price"`
	SeedSig        string `json:"seed_sig"`
}

// SettleBody is the body of a "settle" record completing a round.
type SettleBody struct {
	Round     uint64 `json:"round"`
	CarryOver bool   `json:"carry_over"`
}

// SnapshotCheckpointBody is the body of a "snapshot_checkpoint" record.
type SnapshotCheckpointBody struct {
	Round     uint64 `json:"round"`
	StateHash string `json:"state_hash"`
}

// AdminHaltRecordBody is the body of an "admin.halt" record.
type AdminHaltRecordBody struct {
	Round       uint64 `json:"round"`
	Scope       string `json:"scope"`
	Reason      string `json:"reason"`
	AdminPubKey string `json:"admin_pubkey"`
	AdminSig    string `json:"admin_sig"`
}

// AdminResumeRecordBody is the body of an "admin.resume" record.
type AdminResumeRecordBody struct {
	Round       uint64 `json:"round"`
	Scope       string `json:"scope"`
	Reason      string `json:"reason,omitempty"`
	AdminPubKey string `json:"admin_pubkey"`
	AdminSig    string `json:"admin_sig"`
}

// AdminModerateRecordBody is the body of an "admin.moderate" record.
type AdminModerateRecordBody struct {
	PostID      string `json:"post_id"`
	Reason      string `json:"reason"`
	AdminPubKey string `json:"admin_pubkey"`
	AdminSig    string `json:"admin_sig"`
}

// AdminResetRecordBody is the body of an "admin.reset" record (weekly/seasonal economy reset).
type AdminResetRecordBody struct {
	Round           uint64           `json:"round"`
	SeasonID        string           `json:"season_id,omitempty"`
	Reason          string           `json:"reason"`
	InitialBalances map[string]int64 `json:"initial_balances"`
	ClearHistory    bool             `json:"clear_history,omitempty"`
	AdminPubKey     string           `json:"admin_pubkey"`
	AdminSig        string           `json:"admin_sig"`
}

// ExtBody is the body of an "ext" record (Spec A12: uninterpreted by core protocol).
type ExtBody struct {
	Namespace string          `json:"namespace"`
	Body      json.RawMessage `json:"body"`
}
