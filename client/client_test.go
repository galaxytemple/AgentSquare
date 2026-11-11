package client_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

func TestClientSDKEndToEnd(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	v.Manifest.Board = &manifest.BoardConfig{
		Enabled:                  true,
		MaxPostsPerAgentPerRound: 5,
		MaxBodyChars:             280,
		RequireParticipant:       true,
		AllowedContent:           "text/plain",
	}
	eng, _, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
	defer srv.Close()

	agA, _ := protocol.GenerateKeyPair(nil)
	agB, _ := protocol.GenerateKeyPair(nil)

	cliA := client.New(srv.URL, "", agA)
	cliB := client.New(srv.URL, "", agB)

	m, err := cliA.GetManifest(ctx)
	if err != nil || m.GameID != v.Manifest.GameID {
		t.Fatalf("GetManifest: %v", err)
	}
	rs, err := cliA.GetRound(ctx)
	if err != nil || rs.Round != 1 {
		t.Fatalf("GetRound: %+v err=%v", rs, err)
	}

	if _, rej, err := cliA.Register(ctx, "Alice", ""); err != nil || rej != nil {
		t.Fatalf("Register A: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := cliB.Register(ctx, "Bob", ""); err != nil || rej != nil {
		t.Fatalf("Register B: err=%v rej=%+v", err, rej)
	}

	// Credit copper to Bob so he can sell
	_ = eng.CreditOrDebitBatch(ctx, []struct {
		AgentID string
		Asset   string
		Delta   int64
		Reason  string
	}{
		{AgentID: agB.AgentID(), Asset: "copper", Delta: 10, Reason: "grant"},
	})

	ordRec, rej, err := cliA.PlaceOrder(ctx, "copper", protocol.SideBuy, 8, 2)
	if err != nil || rej != nil {
		t.Fatalf("PlaceOrder A: err=%v rej=%+v", err, rej)
	}
	repRec, rej, err := cliA.ReplaceOrder(ctx, ordRec.OrderID, 9, 3)
	if err != nil || rej != nil {
		t.Fatalf("ReplaceOrder A: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := cliB.PlaceOrder(ctx, "copper", protocol.SideSell, 8, 3); err != nil || rej != nil {
		t.Fatalf("PlaceOrder B: err=%v rej=%+v", err, rej)
	}

	pf, err := cliA.GetPortfolio(ctx, "")
	if err != nil || len(pf.OpenOrders) != 1 || pf.OpenOrders[0].OrderID != repRec.OrderID {
		t.Fatalf("GetPortfolio A: %+v err=%v", pf, err)
	}

	ind, err := cliA.GetIndicative(ctx, "copper")
	if err != nil || ind.IndicativeVolume != 3 {
		t.Fatalf("GetIndicative: %+v err=%v", ind, err)
	}

	if _, rej, err := cliA.PostBoard(ctx, "copper", "bull", "Copper looking strong", ""); err != nil || rej != nil {
		t.Fatalf("PostBoard: err=%v rej=%+v", err, rej)
	}
	posts, err := cliA.ReadBoard(ctx, "copper", 0, 10)
	if err != nil || len(posts) != 1 {
		t.Fatalf("ReadBoard: %+v err=%v", posts, err)
	}

	// Clear round
	_, _ = eng.SealAndClearRound(ctx)

	tape, err := cliA.GetTape(ctx, "copper", 5)
	if err != nil || len(tape) != 1 {
		t.Fatalf("GetTape: %+v err=%v", tape, err)
	}
	candles, err := cliA.GetCandles(ctx, "copper", 5)
	if err != nil || len(candles) != 1 {
		t.Fatalf("GetCandles: %+v err=%v", candles, err)
	}
	st, err := cliA.GetStats(ctx, "copper", 5)
	if err != nil || st.TotalVolume != 3 {
		t.Fatalf("GetStats: %+v err=%v", st, err)
	}
	tvt, err := cliA.TalkVsTrade(ctx, agA.AgentID(), "copper", 2)
	if err != nil || tvt.TotalStancePosts != 1 {
		t.Fatalf("TalkVsTrade: %+v err=%v", tvt, err)
	}

	// PlaceMarketOrder, CancelOrder, and RotateKey
	mktRec, rej, err := cliA.PlaceMarketOrder(ctx, "copper", protocol.SideBuy, 1)
	if err != nil || rej != nil || mktRec == nil {
		t.Fatalf("PlaceMarketOrder A: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := cliA.CancelOrder(ctx, mktRec.OrderID); err != nil || rej != nil {
		t.Fatalf("CancelOrder A: err=%v rej=%+v", err, rej)
	}
	newAgA, _ := protocol.GenerateKeyPair(nil)
	if _, rej, err := cliA.RotateKey(ctx, newAgA); err != nil || rej != nil {
		t.Fatalf("RotateKey A: err=%v rej=%+v", err, rej)
	}
	// Verify that agent A can still place orders and query portfolio after key rotation
	if postRotRec, rej, err := cliA.PlaceOrder(ctx, "copper", protocol.SideBuy, 8, 1); err != nil || rej != nil || postRotRec == nil {
		t.Fatalf("PlaceOrder after RotateKey failed: err=%v rej=%+v", err, rej)
	}
	if pfAfter, err := cliA.GetPortfolio(ctx, ""); err != nil || pfAfter.AgentID != agA.AgentID() {
		t.Fatalf("GetPortfolio after RotateKey failed: pf=%+v err=%v", pfAfter, err)
	}
}
