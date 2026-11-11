package streamlog

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"sync"

	"github.com/galaxytemple/AgentSquare/ledger"
)

// AppendStream is the minimal provider contract for external append-specialized log databases
// such as NATS JetStream (with optimistic expected-last-sequence header) or Apache Kafka / Redpanda.
// Adopters already running JetStream or Kafka implement these 3 methods and wrap with New(provider).
type AppendStream interface {
	// PublishBatch atomically appends serialized record payloads only if the stream's current tail sequence equals expectedLastSeq.
	PublishBatch(ctx context.Context, expectedLastSeq uint64, payloads [][]byte) error
	// FetchFrom reads serialized record payloads starting at 1-indexed sequence fromSeq.
	FetchFrom(ctx context.Context, fromSeq uint64) ([][]byte, error)
}

// Log adapts any external AppendStream (NATS JetStream, Kafka/Redpanda) to ledger.Log (Spec C1).
type Log struct {
	mu       sync.RWMutex
	stream   AppendStream
	headSeq  uint64
	headHash string
	snapshot ledger.Snapshot
}

// New creates a ledger.Log backed by an external AppendStream provider, syncing its current head on startup.
func New(ctx context.Context, stream AppendStream) (*Log, error) {
	l := &Log{
		stream:   stream,
		headHash: ledger.ZeroHash,
	}
	raws, err := stream.FetchFrom(ctx, 1)
	if err != nil {
		return nil, err
	}
	expectedSeq := uint64(1)
	expectedPrev := ledger.ZeroHash
	for _, raw := range raws {
		var rec ledger.Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("streamlog: invalid record JSON at seq %d: %w", expectedSeq, err)
		}
		if rec.Seq != expectedSeq || rec.PrevHash != expectedPrev {
			return nil, fmt.Errorf("streamlog: broken chain at seq %d", expectedSeq)
		}
		l.headSeq = rec.Seq
		l.headHash = rec.Hash
		expectedSeq++
		expectedPrev = rec.Hash
	}
	return l, nil
}

func (l *Log) Append(ctx context.Context, recs ...ledger.Record) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedSeq := l.headSeq + 1
	expectedPrev := l.headHash
	payloads := make([][]byte, 0, len(recs))

	for _, r := range recs {
		if r.Seq != expectedSeq {
			return "", fmt.Errorf("streamlog: sequence mismatch: got %d, want %d", r.Seq, expectedSeq)
		}
		if r.PrevHash != expectedPrev {
			return "", fmt.Errorf("streamlog: prev_hash mismatch at seq %d: got %s, want %s", r.Seq, r.PrevHash, expectedPrev)
		}
		b, err := json.Marshal(r)
		if err != nil {
			return "", err
		}
		payloads = append(payloads, b)
		expectedSeq++
		expectedPrev = r.Hash
	}

	if err := l.stream.PublishBatch(ctx, l.headSeq, payloads); err != nil {
		return "", err
	}
	if len(recs) > 0 {
		last := recs[len(recs)-1]
		l.headSeq = last.Seq
		l.headHash = last.Hash
	}
	return l.headHash, nil
}

func (l *Log) Read(ctx context.Context, fromSeq uint64) iter.Seq2[ledger.Record, error] {
	if fromSeq <= 1 {
		fromSeq = 1
	}
	raws, err := l.stream.FetchFrom(ctx, fromSeq)
	return func(yield func(ledger.Record, error) bool) {
		if err != nil {
			yield(ledger.Record{}, err)
			return
		}
		for _, raw := range raws {
			var rec ledger.Record
			if uErr := json.Unmarshal(raw, &rec); uErr != nil {
				yield(ledger.Record{}, uErr)
				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func (l *Log) Head(_ context.Context) (uint64, string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.headSeq, l.headHash, nil
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
