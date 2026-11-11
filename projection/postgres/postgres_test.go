package postgres_test

import (
	"context"
	"testing"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/projection/postgres"
)

func TestPostgresProjectionIdempotency(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	proj, err := postgres.New(ctx, nil)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	for _, rec := range vecs[0].ExpectedLedgerAfter {
		if err := proj.Apply(ctx, rec); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		// Duplicate apply for idempotency check
		if err := proj.Apply(ctx, rec); err != nil {
			t.Fatalf("duplicate Apply: %v", err)
		}
	}
	cp, err := proj.Checkpoint(ctx)
	if err != nil || cp != uint64(len(vecs[0].ExpectedLedgerAfter)) {
		t.Fatalf("Checkpoint = %d, want %d", cp, len(vecs[0].ExpectedLedgerAfter))
	}
}
