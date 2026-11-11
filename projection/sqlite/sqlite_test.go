package sqlite_test

import (
	"context"
	"testing"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/projection/sqlite"
)

func TestSQLiteProjectionIdempotencyAndRebuild(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	eng, _, err := conformance.NewVectorEngine(ctx, vecs[0])
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	for _, in := range vecs[0].Inputs {
		if in.Action == "envelope" {
			eng.SubmitRawEnvelope(ctx, in.Envelope)
		} else if in.Action == "clear_round" {
			_, _ = eng.SealAndClearRound(ctx)
		}
	}

	proj, err := sqlite.New(ctx, nil)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}

	// Rebuild from seq 0
	n, err := sqlite.Rebuild(ctx, eng.Log(), proj, 0)
	if err != nil || n == 0 {
		t.Fatalf("Rebuild failed: n=%d err=%v", n, err)
	}
	cp, err := proj.Checkpoint(ctx)
	if err != nil || cp != n {
		t.Fatalf("Checkpoint = %d, want %d", cp, n)
	}

	// Re-applying already applied records must be idempotent (no-op)
	for rec, err := range eng.Log().Read(ctx, 1) {
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if err := proj.Apply(ctx, rec); err != nil {
			t.Fatalf("idempotent Apply failed: %v", err)
		}
	}
}
