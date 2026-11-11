package ledger

import (
	"context"
	"iter"
)

// Snapshot represents a consistent point-in-time state checkpoint of the ledger.
type Snapshot struct {
	Seq       uint64          `json:"seq"`
	Hash      string          `json:"hash"`
	StateHash string          `json:"state_hash"`
	State     *ReplaySnapshot `json:"state"`
}

// Log is the append-only source-of-truth storage interface (Spec C4).
type Log interface {
	// Append atomically appends one or more records to the log and returns the new head hash.
	Append(ctx context.Context, recs ...Record) (head string, err error)
	// Read streams records starting at fromSeq (inclusive; 1-indexed). If fromSeq <= 1, starts from the beginning.
	Read(ctx context.Context, fromSeq uint64) iter.Seq2[Record, error]
	// Head returns the current head sequence number (0 if empty) and hash (ZeroHash if empty).
	Head(ctx context.Context) (seq uint64, hash string, err error)
	// Snapshot returns the latest saved snapshot, if any.
	Snapshot(ctx context.Context) (Snapshot, error)
}

// Projection is a rebuildable read-model consumer that applies ledger records idempotently (Spec C4).
type Projection interface {
	Apply(ctx context.Context, rec Record) error
	Checkpoint(ctx context.Context) (seq uint64, err error)
}
