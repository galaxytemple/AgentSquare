package admin

import (
	"context"

	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Client provides signed administrative interventions (Spec B4).
// Every intervention appends a signed ledger record (`admin.halt`, `admin.resume`, `admin.moderate`, `manifest.update`).
type Client struct {
	cli *client.Client
}

// NewClient wraps a client.Client configured with an admin Signer.
func NewClient(baseURL, gameID string, adminSigner protocol.Signer) *Client {
	return &Client{
		cli: client.New(baseURL, gameID, adminSigner),
	}
}

// Halt globally halts the market, voiding the current open round and releasing all escrow locks (Spec B4).
func (a *Client) Halt(ctx context.Context, reason string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.SendEnvelope(ctx, protocol.TypeAdminHalt, protocol.AdminHaltPayload{
		Scope:  "all",
		Reason: reason,
	})
}

// PauseAsset halts trading on a single asset market (`scope: "asset:<id>"`, Spec B4).
func (a *Client) PauseAsset(ctx context.Context, asset, reason string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.SendEnvelope(ctx, protocol.TypeAdminHalt, protocol.AdminHaltPayload{
		Scope:  "asset:" + asset,
		Reason: reason,
	})
}

// Resume resumes trading from the next round (Spec B4).
func (a *Client) Resume(ctx context.Context, scope, reason string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	if scope == "" {
		scope = "all"
	}
	return a.cli.SendEnvelope(ctx, protocol.TypeAdminResume, protocol.AdminResumePayload{
		Scope:  scope,
		Reason: reason,
	})
}

// HidePost moderates and hides a board post while preserving its ledger hash (Spec B4).
func (a *Client) HidePost(ctx context.Context, postID, reason string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.SendEnvelope(ctx, protocol.TypeAdminModerate, protocol.AdminModeratePayload{
		PostID: postID,
		Reason: reason,
	})
}

// ManifestUpdate schedules a manifest hash update effective at a future round (Spec B4).
func (a *Client) ManifestUpdate(ctx context.Context, newHash string, effectiveRound uint64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.SendEnvelope(ctx, protocol.TypeManifestUpdate, protocol.ManifestUpdatePayload{
		NewHash:        newHash,
		EffectiveRound: effectiveRound,
	})
}

// Reset executes an admin-signed weekly/seasonal economy reset, wiping all open/carried-over orders,
// releasing all escrow locks, and resetting every registered agent's balances to the starting balances.
func (a *Client) Reset(ctx context.Context, seasonID, reason string, initialBalances map[string]int64, clearHistory bool) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.SendEnvelope(ctx, protocol.TypeAdminReset, protocol.AdminResetPayload{
		SeasonID:        seasonID,
		Reason:          reason,
		InitialBalances: initialBalances,
		ClearHistory:    clearHistory,
	})
}

// RotateKey rotates the current admin Ed25519 key to newAdminSigner using a dual-signature proof of possession (key.rotate).
// Once accepted, the old admin key is immediately revoked in the engine's AdminPubKeys and newAdminSigner becomes active.
func (a *Client) RotateKey(ctx context.Context, newAdminSigner protocol.Signer) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return a.cli.RotateKey(ctx, newAdminSigner)
}
