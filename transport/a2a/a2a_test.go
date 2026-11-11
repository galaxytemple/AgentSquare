package a2a_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/transport/a2a"
)

func TestA2AAgentCardAndTaskExecution(t *testing.T) {
	ctx := context.Background()
	vecs, _ := conformance.BuildStandardVectors()
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	eng, _, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}

	h := a2a.NewHandler(eng)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// 1. Check /.well-known/agent.json
	resp, err := http.Get(srv.URL + "/.well-known/agent.json")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET agent.json failed: %v", err)
	}
	var card a2a.AgentCard
	_ = json.NewDecoder(resp.Body).Decode(&card)
	_ = resp.Body.Close()
	if len(card.SupportedProtocols) == 0 || card.SupportedProtocols[0] != protocol.ProtocolID {
		t.Fatalf("unexpected AgentCard: %+v", card)
	}

	// 2. Submit signed registration envelope via POST /a2a
	ag, _ := protocol.GenerateKeyPair(nil)
	rawP, _ := json.Marshal(protocol.RegisterPayload{PubKey: ag.PublicKeyString()})
	env := protocol.Envelope{
		Version:   "1.0",
		GameID:    v.Manifest.GameID,
		From:      ag.AgentID(),
		To:        "server",
		Type:      protocol.TypeAgentRegister,
		Timestamp: time.Now().UTC().Unix(),
		Nonce:     "a2a-1",
		CorrID:    "c-a2a-1",
		Payload:   rawP,
	}
	_ = protocol.SignEnvelope(&env, ag)
	envBytes, _ := json.Marshal(env)

	rpcBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tasks/send",
		"params": map[string]any{
			"id":       "task-1",
			"envelope": json.RawMessage(envBytes),
		},
	})
	r2, err := http.Post(srv.URL+"/a2a", "application/json", bytes.NewReader(rpcBody))
	if err != nil || r2.StatusCode != http.StatusOK {
		t.Fatalf("POST /a2a failed: %v", err)
	}
	var out struct {
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	_ = json.NewDecoder(r2.Body).Decode(&out)
	_ = r2.Body.Close()
	if out.Result.Status != "completed" {
		t.Fatalf("expected A2A task status completed, got %+v", out)
	}
}
