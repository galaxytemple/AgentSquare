package admin_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/admin"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

func TestAdminClientInterventions(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	eng, srvSigner, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng))
	defer srv.Close()

	adm := admin.NewClient(srv.URL, v.Manifest.GameID, srvSigner)

	if rec, rej, err := adm.PauseAsset(ctx, "copper", "volatility check"); err != nil || rej != nil || rec == nil {
		t.Fatalf("PauseAsset failed: err=%v rej=%+v", err, rej)
	}
	if rec, rej, err := adm.Halt(ctx, "scheduled maintenance"); err != nil || rej != nil || rec == nil {
		t.Fatalf("Halt failed: err=%v rej=%+v", err, rej)
	}
	if rec, rej, err := adm.Resume(ctx, "all", "maintenance complete"); err != nil || rej != nil || rec == nil {
		t.Fatalf("Resume failed: err=%v rej=%+v", err, rej)
	}
	if rec, rej, err := adm.HidePost(ctx, "post_1_1", "spam"); err != nil || rej != nil || rec == nil {
		t.Fatalf("HidePost failed: err=%v rej=%+v", err, rej)
	}
	if rec, rej, err := adm.ManifestUpdate(ctx, "sha256:0000000000000000000000000000000000000000000000000000000000000001", 10); err != nil || rej != nil || rec == nil {
		t.Fatalf("ManifestUpdate failed: err=%v rej=%+v", err, rej)
	}
	if rec, rej, err := adm.Reset(ctx, "season-2026-w46", "weekly reset", map[string]int64{"usd": 1000, "copper": 20}, true); err != nil || rej != nil || rec == nil {
		t.Fatalf("Reset failed: err=%v rej=%+v", err, rej)
	}

	// Non-admin key attempting admin.halt or admin.reset must be rejected
	nonAdmin, _ := protocol.GenerateKeyPair(nil)
	badAdm := admin.NewClient(srv.URL, v.Manifest.GameID, nonAdmin)
	if _, rej, _ := badAdm.Halt(ctx, "unauthorized"); rej == nil {
		t.Fatalf("expected non-admin halt to be rejected")
	}
	if _, rej, _ := badAdm.Reset(ctx, "hack-season", "unauthorized reset", map[string]int64{"usd": 999999}, false); rej == nil {
		t.Fatalf("expected non-admin reset to be rejected")
	}

	// Admin Key Rotation: rotate from srvSigner -> newAdminKey
	newAdminKey, _ := protocol.GenerateKeyPair(nil)
	oldAdmCopy := admin.NewClient(srv.URL, v.Manifest.GameID, srvSigner)
	if rec, rej, err := adm.RotateKey(ctx, newAdminKey); err != nil || rej != nil || rec == nil {
		t.Fatalf("Admin RotateKey failed: err=%v rej=%+v", err, rej)
	}

	// Old admin key must now be immediately rejected!
	if _, rej, _ := oldAdmCopy.Reset(ctx, "old-key-season", "try with revoked admin key", nil, false); rej == nil {
		t.Fatalf("expected revoked old admin key to be rejected after RotateKey")
	}

	// New rotated admin key succeeds on admin.reset!
	if rec, rej, err := adm.Reset(ctx, "season-2026-w47", "weekly reset with rotated admin key", map[string]int64{"usd": 1000, "copper": 20}, false); err != nil || rej != nil || rec == nil {
		t.Fatalf("Reset with rotated admin key failed: err=%v rej=%+v", err, rej)
	}
}
