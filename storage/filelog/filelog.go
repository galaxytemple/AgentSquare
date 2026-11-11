package filelog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"sync"

	"github.com/galaxytemple/AgentSquare/ledger"
)

// Log is a durable JSON-Lines (JSONL) file-backed implementation of ledger.Log.
type Log struct {
	mu       sync.RWMutex
	path     string
	file     *os.File
	records  []ledger.Record
	snapshot ledger.Snapshot
}

// Open opens or creates a JSONL ledger file at path, verifying contiguous sequence and hash chain on load.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		path: path,
		file: f,
	}
	if err := l.loadExisting(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

func (l *Log) loadExisting() error {
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(l.file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	expectedSeq := uint64(1)
	expectedPrev := ledger.ZeroHash

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec ledger.Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("filelog: corrupt JSON at seq %d: %w", expectedSeq, err)
		}
		if rec.Seq != expectedSeq {
			return fmt.Errorf("filelog: sequence mismatch: got %d, want %d", rec.Seq, expectedSeq)
		}
		if rec.PrevHash != expectedPrev {
			return fmt.Errorf("filelog: prev_hash mismatch at seq %d: got %s, want %s", rec.Seq, rec.PrevHash, expectedPrev)
		}
		l.records = append(l.records, rec)
		expectedSeq++
		expectedPrev = rec.Hash
	}
	return scanner.Err()
}

// Close closes the underlying file descriptor.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

func (l *Log) Append(_ context.Context, recs ...ledger.Record) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedSeq := uint64(len(l.records)) + 1
	expectedPrev := ledger.ZeroHash
	if len(l.records) > 0 {
		expectedPrev = l.records[len(l.records)-1].Hash
	}

	var buf bytes.Buffer
	for _, r := range recs {
		if r.Seq != expectedSeq {
			return "", fmt.Errorf("filelog: sequence mismatch: got %d, want %d", r.Seq, expectedSeq)
		}
		if r.PrevHash != expectedPrev {
			return "", fmt.Errorf("filelog: prev_hash mismatch at seq %d: got %s, want %s", r.Seq, r.PrevHash, expectedPrev)
		}
		b, err := json.Marshal(r)
		if err != nil {
			return "", err
		}
		buf.Write(b)
		buf.WriteByte('\n')
		expectedSeq++
		expectedPrev = r.Hash
	}

	if buf.Len() > 0 {
		if _, err := l.file.Write(buf.Bytes()); err != nil {
			return "", err
		}
		if err := l.file.Sync(); err != nil {
			return "", err
		}
		l.records = append(l.records, recs...)
	}

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

// Bytes returns the approximate total byte size of the ledger file.
func (l *Log) Bytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.file == nil {
		return 0
	}
	info, err := l.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
