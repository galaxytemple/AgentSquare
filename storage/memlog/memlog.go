package memlog

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/galaxytemple/AgentSquare/ledger"
)

// Log is a thread-safe in-memory implementation of ledger.Log.
type Log struct {
	mu       sync.RWMutex
	records  []ledger.Record
	snapshot ledger.Snapshot
}

// New creates a new empty in-memory Log.
func New() *Log {
	return &Log{}
}

func (l *Log) Append(_ context.Context, recs ...ledger.Record) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedSeq := uint64(len(l.records)) + 1
	expectedPrev := ledger.ZeroHash
	if len(l.records) > 0 {
		expectedPrev = l.records[len(l.records)-1].Hash
	}

	for _, r := range recs {
		if r.Seq != expectedSeq {
			return "", fmt.Errorf("memlog: sequence mismatch: got %d, want %d", r.Seq, expectedSeq)
		}
		if r.PrevHash != expectedPrev {
			return "", fmt.Errorf("memlog: prev_hash mismatch at seq %d: got %s, want %s", r.Seq, r.PrevHash, expectedPrev)
		}
		expectedSeq++
		expectedPrev = r.Hash
	}

	l.records = append(l.records, recs...)
	if len(l.records) == 0 {
		return ledger.ZeroHash, nil
	}
	return l.records[len(l.records)-1].Hash, nil
}

func (l *Log) Read(_ context.Context, fromSeq uint64) iter.Seq2[ledger.Record, error] {
	l.mu.RLock()
	if fromSeq <= 1 {
		fromSeq = 1
	}
	var slice []ledger.Record
	if int(fromSeq-1) < len(l.records) {
		slice = make([]ledger.Record, len(l.records)-int(fromSeq-1))
		copy(slice, l.records[fromSeq-1:])
	}
	l.mu.RUnlock()

	return func(yield func(ledger.Record, error) bool) {
		for _, rec := range slice {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func (l *Log) Head(_ context.Context) (uint64, string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.records) == 0 {
		return 0, ledger.ZeroHash, nil
	}
	last := l.records[len(l.records)-1]
	return last.Seq, last.Hash, nil
}

func (l *Log) Snapshot(_ context.Context) (ledger.Snapshot, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snapshot, nil
}

func (l *Log) SaveSnapshot(snap ledger.Snapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapshot = snap
}
