package logtest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Run validates a ledger.Log implementation against the protocol storage contract (Spec C4).
func Run(t *testing.T, newLog func(t *testing.T) ledger.Log) {
	t.Helper()
	ctx := context.Background()
	lg := newLog(t)

	// 1. Empty head must be (0, ZeroHash)
	seq, head, err := lg.Head(ctx)
	if err != nil {
		t.Fatalf("Head on empty log failed: %v", err)
	}
	if seq != 0 || head != ledger.ZeroHash {
		t.Fatalf("expected (0, ZeroHash) on empty log, got (%d, %s)", seq, head)
	}

	seed := make([]byte, 32)
	seed[0] = 55
	signer, _ := protocol.NewSignerFromSeed(seed)

	// 2. Append genesis record
	b1, _ := json.Marshal(ledger.GenesisBody{ManifestHash: "sha256:1111111111111111111111111111111111111111111111111111111111111111"})
	r1 := ledger.Record{
		Seq:      1,
		PrevHash: ledger.ZeroHash,
		GameID:   "test-game",
		Type:     ledger.RecGenesis,
		Body:     b1,
	}
	if err := ledger.SignAndHashRecord(&r1, signer); err != nil {
		t.Fatalf("SignAndHashRecord r1: %v", err)
	}

	h1, err := lg.Append(ctx, r1)
	if err != nil {
		t.Fatalf("Append r1: %v", err)
	}
	if h1 != r1.Hash {
		t.Fatalf("got head %s, want %s", h1, r1.Hash)
	}

	// 3. Batch append r2 and r3 atomically
	b2, _ := json.Marshal(ledger.BalanceChangeBody{AgentID: "ag_1", Asset: "usd", Qty: 100, Reason: "init"})
	r2 := ledger.Record{
		Seq:      2,
		PrevHash: r1.Hash,
		GameID:   "test-game",
		Type:     ledger.RecBalanceCredit,
		Body:     b2,
	}
	_ = ledger.SignAndHashRecord(&r2, signer)

	b3, _ := json.Marshal(ledger.BalanceChangeBody{AgentID: "ag_1", Asset: "copper", Qty: 10, Reason: "init"})
	r3 := ledger.Record{
		Seq:      3,
		PrevHash: r2.Hash,
		GameID:   "test-game",
		Type:     ledger.RecBalanceCredit,
		Body:     b3,
	}
	_ = ledger.SignAndHashRecord(&r3, signer)

	h3, err := lg.Append(ctx, r2, r3)
	if err != nil {
		t.Fatalf("Append batch r2,r3: %v", err)
	}
	if h3 != r3.Hash {
		t.Fatalf("got head %s, want %s", h3, r3.Hash)
	}

	seq, head, err = lg.Head(ctx)
	if err != nil || seq != 3 || head != r3.Hash {
		t.Fatalf("Head after batch = (%d, %s, %v), want (3, %s, nil)", seq, head, err, r3.Hash)
	}

	// 4. Read from seq 1 and from seq 2
	var readAll []ledger.Record
	for rec, err := range lg.Read(ctx, 1) {
		if err != nil {
			t.Fatalf("Read(1): %v", err)
		}
		readAll = append(readAll, rec)
	}
	if len(readAll) != 3 {
		t.Fatalf("expected 3 records from Read(1), got %d", len(readAll))
	}

	var readFrom2 []ledger.Record
	for rec, err := range lg.Read(ctx, 2) {
		if err != nil {
			t.Fatalf("Read(2): %v", err)
		}
		readFrom2 = append(readFrom2, rec)
	}
	if len(readFrom2) != 2 || readFrom2[0].Seq != 2 || readFrom2[1].Seq != 3 {
		t.Fatalf("unexpected records from Read(2): %+v", readFrom2)
	}
}
