package memlog_test

import (
	"context"
	"testing"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/ledger/logtest"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

func TestMemLogContractAndSnapshot(t *testing.T) {
	logtest.Run(t, func(t *testing.T) ledger.Log {
		return memlog.New()
	})

	lg := memlog.New()
	lg.SaveSnapshot(ledger.Snapshot{Seq: 5, Hash: "sha256:abc", StateHash: "sha256:def"})
	snap, err := lg.Snapshot(context.Background())
	if err != nil || snap.Seq != 5 {
		t.Fatalf("unexpected snapshot: %+v err=%v", snap, err)
	}
}
