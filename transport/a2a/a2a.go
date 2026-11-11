package a2a

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/galaxytemple/AgentSquare/engine"
)

// AgentCard represents the Google A2A discovery document at /.well-known/agent.json (Spec 0.4 & C3).
// Tagline: "A2A carries the message; this protocol defines the market it trades in, and the ledger that proves it."
type AgentCard struct {
	Name               string   `json:"name"`
	Description        string   `json:"description"`
	URL                string   `json:"url"`
	Version            string   `json:"version"`
	Capabilities       map[string]bool `json:"capabilities"`
	SupportedProtocols []string `json:"supported_protocols"`
}

// Handler exposes an A2A-compatible transport endpoint carrying signed AgentSquare envelopes (Spec C3).
type Handler struct {
	eng *engine.Engine
	mux *http.ServeMux
}

// NewHandler creates a new A2A HTTP handler bound to eng.
func NewHandler(eng *engine.Engine) *Handler {
	h := &Handler{
		eng: eng,
		mux: http.NewServeMux(),
	}
	h.mux.HandleFunc("GET /.well-known/agent.json", h.handleAgentCard)
	h.mux.HandleFunc("POST /a2a", h.handleTask)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) handleAgentCard(w http.ResponseWriter, _ *http.Request) {
	m := h.eng.Manifest()
	card := AgentCard{
		Name:               "AgentSquare Market Host (" + m.GameID + ")",
		Description:        "A2A carries the message; AgentSquare defines the batch call-auction market and verifiable ledger.",
		URL:                "/a2a",
		Version:            "1.0.0",
		Capabilities:       map[string]bool{"streaming": true, "pushNotifications": false},
		SupportedProtocols: []string{m.Protocol},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(card)
}

func (h *Handler) handleTask(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JSONRPC string `json:"jsonrpc"`
		ID      any    `json:"id"`
		Method  string `json:"method"`
		Params  struct {
			TaskID   string          `json:"id"`
			Envelope json.RawMessage `json:"envelope"`
		} `json:"params"`
	}
	const maxA2ABytes = 64 * 1024
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxA2ABytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	if len(rawBody) > maxA2ABytes {
		http.Error(w, "A2A payload exceeds maximum allowed size (64 KB)", http.StatusRequestEntityTooLarge)
		return
	}
	if err := json.Unmarshal(rawBody, &req); err != nil {
		http.Error(w, "invalid A2A JSON-RPC payload", http.StatusBadRequest)
		return
	}

	receipt, rej := h.eng.SubmitRawEnvelope(r.Context(), req.Params.Envelope)
	w.Header().Set("Content-Type", "application/json")
	if rej != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"id":        req.Params.TaskID,
				"status":    "failed",
				"rejection": rej,
			},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result": map[string]any{
			"id":      req.Params.TaskID,
			"status":  "completed",
			"receipt": receipt,
		},
	})
}
