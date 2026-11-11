package streamlog_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/ledger/logtest"
	"github.com/galaxytemple/AgentSquare/storage/streamlog"
)

type fakeJetStream struct {
	mu   sync.Mutex
	msgs [][]byte
}

func (f *fakeJetStream) PublishBatch(_ context.Context, expectedLastSeq uint64, payloads [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if uint64(len(f.msgs)) != expectedLastSeq {
		return fmt.Errorf("optimistic concurrency mismatch: have %d, expected %d", len(f.msgs), expectedLastSeq)
	}
	f.msgs = append(f.msgs, payloads...)
	return nil
}

func (f *fakeJetStream) FetchFrom(_ context.Context, fromSeq uint64) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if fromSeq <= 1 {
		fromSeq = 1
	}
	if int(fromSeq-1) >= len(f.msgs) {
		return nil, nil
	}
	out := make([][]byte, len(f.msgs)-int(fromSeq-1))
	copy(out, f.msgs[fromSeq-1:])
	return out, nil
}

func TestStreamLogConformance(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		lg, err := streamlog.New(context.Background(), &fakeJetStream{})
		if err != nil {
			t.Fatalf("streamlog.New: %v", err)
		}
		return lg
	})
}

func TestStreamLogStartupSyncAndSnapshot(t *testing.T) {
	ctx := context.Background()
	js := &fakeJetStream{}
	lg, err := streamlog.New(ctx, js)
	if err != nil {
		t.Fatalf("streamlog.New: %v", err)
	}
	r1 := ledger.Record{Seq: 1, PrevHash: ledger.ZeroHash, Hash: "sha256:0001", Type: ledger.RecGenesis}
	r2 := ledger.Record{Seq: 2, PrevHash: "sha256:0001", Hash: "sha256:0002", Type: ledger.RecRoundSealed}
	if _, err := lg.Append(ctx, r1, r2); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := lg.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Re-open on existing stream (tests startup sync loop in streamlog.New)
	lg2, err := streamlog.New(ctx, js)
	if err != nil {
		t.Fatalf("streamlog.New reopen: %v", err)
	}
	seq, h, err := lg2.Head(ctx)
	if err != nil || seq != 2 || h != "sha256:0002" {
		t.Fatalf("unexpected reopened head: seq=%d hash=%s err=%v", seq, h, err)
	}

	// Sequence mismatch and prev_hash mismatch errors
	if _, err := lg2.Append(ctx, ledger.Record{Seq: 5, PrevHash: "sha256:0002"}); err == nil {
		t.Fatalf("expected sequence mismatch error")
	}
	if _, err := lg2.Append(ctx, ledger.Record{Seq: 3, PrevHash: "sha256:wrong"}); err == nil {
		t.Fatalf("expected prev_hash mismatch error")
	}
}
