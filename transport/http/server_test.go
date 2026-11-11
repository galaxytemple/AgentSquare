package http_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

func TestHTTPServerEndpointsPrivacyAndSSEStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vecs, _ := conformance.BuildStandardVectors()
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	v.Manifest.MarketData.PreClear = "depth"
	v.Manifest.Ledger.Public = "after-clear"

	eng, _, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
	defer srv.Close()

	// 1. Healthz, Readyz, OpenAPI, Metrics, CORS OPTIONS
	for _, path := range []string{"/healthz", "/readyz", "/openapi.json", "/metrics"} {
		r, err := http.Get(srv.URL + path)
		if err != nil || r.StatusCode != http.StatusOK {
			t.Fatalf("GET %s failed: status=%v err=%v", path, r, err)
		}
		_ = r.Body.Close()
	}

	reqOpt, _ := http.NewRequest(http.MethodOptions, srv.URL+"/v1/envelope", nil)
	respOpt, err := http.DefaultClient.Do(reqOpt)
	if err != nil || respOpt.StatusCode != http.StatusNoContent {
		t.Fatalf("OPTIONS failed: %v", err)
	}
	_ = respOpt.Body.Close()

	// 2. Connect SSE Stream (/v1/market/stream)
	sseReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/market/stream", nil)
	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil || sseResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/market/stream failed: %v", err)
	}
	defer func() { _ = sseResp.Body.Close() }()

	// 3. Register agent and submit an order in the open round
	ag, _ := protocol.GenerateKeyPair(nil)
	cli := client.New(srv.URL, v.Manifest.GameID, ag)
	if _, rej, err := cli.Register(ctx, "Trader1", ""); err != nil || rej != nil {
		t.Fatalf("Register: err=%v rej=%+v", err, rej)
	}
	if _, rej, err := cli.PlaceOrder(ctx, "copper", protocol.SideBuy, 9, 2); err != nil || rej != nil {
		t.Fatalf("PlaceOrder: err=%v rej=%+v", err, rej)
	}

	// Read first SSE event (indicative_update)
	reader := bufio.NewReader(sseResp.Body)
	line1, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line1, "event: indicative_update") {
		t.Fatalf("unexpected SSE event line: %q err=%v", line1, err)
	}

	// 4. Verify depth endpoint (/v1/market/book/depth)
	depthResp, err := http.Get(srv.URL + "/v1/market/book/depth?asset=copper")
	if err != nil || depthResp.StatusCode != http.StatusOK {
		t.Fatalf("GET depth failed: %v", err)
	}
	_ = depthResp.Body.Close()

	// 5. Verify Spec A12 Privacy: while round 1 is still OPEN and ledger.public == "after-clear",
	// GET /v1/ledger MUST NOT expose the open round's order_accepted record!
	ledResp, err := http.Get(srv.URL + "/v1/ledger")
	if err != nil {
		t.Fatalf("GET /v1/ledger: %v", err)
	}
	ledBytes, _ := io.ReadAll(ledResp.Body)
	_ = ledResp.Body.Close()
	if strings.Contains(string(ledBytes), ledger.RecOrderAccepted) {
		t.Fatalf("privacy violation: /v1/ledger exposed open-round order_accepted before clearing:\n%s", string(ledBytes))
	}

	// 6. Now clear the round via /v1/sim/clear; afterward GET /v1/ledger MUST expose the cleared round's records!
	clearResp, err := http.Post(srv.URL+"/v1/sim/clear", "application/json", nil)
	if err != nil || clearResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/sim/clear failed: %v", err)
	}
	_ = clearResp.Body.Close()

	ledAfterResp, err := http.Get(srv.URL + "/v1/ledger")
	if err != nil {
		t.Fatalf("GET /v1/ledger after clear: %v", err)
	}
	ledAfterBytes, _ := io.ReadAll(ledAfterResp.Body)
	_ = ledAfterResp.Body.Close()
	if !strings.Contains(string(ledAfterBytes), ledger.RecOrderAccepted) {
		t.Fatalf("expected /v1/ledger to expose round 1 orders after round cleared")
	}

	// 7. Verify GET /v1/fills
	fillsResp, err := http.Get(srv.URL + "/v1/fills?since_seq=0")
	if err != nil || fillsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/fills failed: %v", err)
	}
	var fillsObj map[string]any
	_ = json.NewDecoder(fillsResp.Body).Decode(&fillsObj)
	_ = fillsResp.Body.Close()

	// 8. Exercise remaining GET endpoints: manifest, round, portfolio, indicative, tape, candles, stats, board posts, talk-vs-trade
	for _, ep := range []string{
		"/.well-known/agentsquare.json",
		"/v1/round",
		"/v1/agents/" + ag.AgentID() + "/portfolio",
		"/v1/market/book/indicative?asset=copper",
		"/v1/market/tape?asset=copper&rounds=5",
		"/v1/market/candles?asset=copper&rounds=5",
		"/v1/market/stats?asset=copper&rounds=5",
		"/v1/board/posts?asset=copper",
		"/v1/board/agents/" + ag.AgentID() + "/talk-vs-trade?asset=copper&rounds=3",
	} {
		resp, gErr := http.Get(srv.URL + ep)
		if gErr != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s failed: status=%v err=%v", ep, resp, gErr)
		}
		_ = resp.Body.Close()
	}
}
