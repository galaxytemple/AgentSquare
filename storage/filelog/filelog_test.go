package filelog_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/ledger/logtest"
	"github.com/galaxytemple/AgentSquare/storage/filelog"
)

func TestFileLogPersistenceReopenAndCorruptDetection(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		p := filepath.Join(t.TempDir(), "log.jsonl")
		lg, err := filelog.Open(p)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = lg.Close() })
		return lg
	})

	ctx := context.Background()
	vecs, _ := conformance.BuildStandardVectors()
	p := filepath.Join(t.TempDir(), "persist.jsonl")

	lg1, err := filelog.Open(p)
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	if _, err := lg1.Append(ctx, vecs[0].ExpectedLedgerAfter...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lg1.Bytes() <= 0 {
		t.Fatalf("expected non-zero file size")
	}
	_ = lg1.Close()

	// Reopen and verify all records load cleanly
	lg2, err := filelog.Open(p)
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	seq, head, err := lg2.Head(ctx)
	if err != nil || seq != uint64(len(vecs[0].ExpectedLedgerAfter)) || head == ledger.ZeroHash {
		t.Fatalf("unexpected head after reopen: seq=%d head=%s err=%v", seq, head, err)
	}
	_ = lg2.Close()

	// Append corrupt JSON line and verify Open rejects it
	f, _ := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	_, _ = f.WriteString("{corrupt_json\n")
	_ = f.Close()

	if _, err := filelog.Open(p); err == nil {
		t.Fatalf("expected filelog.Open to fail on corrupt trailing JSON line")
	}
}
