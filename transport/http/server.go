package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/obs"
	"github.com/galaxytemple/AgentSquare/transport/a2a"
)

// Server wraps an Engine and exposes the standard HTTP + SSE + Prometheus + A2A transport bindings (Spec C3 & A11).
type Server struct {
	eng          *engine.Engine
	metrics      *obs.Metrics
	allowSimStep bool
	mux          *http.ServeMux
}

// Option configures the HTTP Server.
type Option func(*Server)

// WithSimTrigger enables POST /v1/sim/clear for conformance testing and local simulation.
func WithSimTrigger(enabled bool) Option {
	return func(s *Server) { s.allowSimStep = enabled }
}

// WithMetrics attaches a Prometheus Metrics collector to the HTTP Server.
func WithMetrics(m *obs.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// NewServer creates an HTTP handler bound to eng.
func NewServer(eng *engine.Engine, opts ...Option) *Server {
	s := &Server{
		eng:     eng,
		metrics: obs.NewMetrics(),
		mux:     http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerRoutes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers for browser ledger viewer (Spec B3)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET "+manifest.WellKnownPath, s.handleManifest)
	s.mux.HandleFunc("GET /.well-known/roundabout.json", s.handleManifest)
	s.mux.HandleFunc("GET /openapi.json", s.handleOpenAPI)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	// A2A (Agent-to-Agent) discovery and task endpoint
	a2aHandler := a2a.NewHandler(s.eng)
	s.mux.Handle("GET /.well-known/agent.json", a2aHandler)
	s.mux.Handle("POST /a2a", a2aHandler)

	s.mux.HandleFunc("POST /v1/envelope", s.handleEnvelope)
	s.mux.HandleFunc("GET /v1/round", s.handleRound)
	s.mux.HandleFunc("GET /v1/agents/{id}/portfolio", s.handlePortfolio)
	s.mux.HandleFunc("GET /v1/fills", s.handleFills)

	// Market data (Spec A11)
	s.mux.HandleFunc("GET /v1/market/tape", s.handleTape)
	s.mux.HandleFunc("GET /v1/market/candles", s.handleCandles)
	s.mux.HandleFunc("GET /v1/market/stats", s.handleStats)
	s.mux.HandleFunc("GET /v1/market/book/indicative", s.handleIndicative)
	s.mux.HandleFunc("GET /v1/market/book/depth", s.handleDepth)
	s.mux.HandleFunc("GET /v1/market/stream", s.handleStream)

	// Board extension (Spec B1)
	s.mux.HandleFunc("GET /v1/board/posts", s.handleBoardPosts)
	s.mux.HandleFunc("GET /v1/board/agents/{id}/talk-vs-trade", s.handleTalkVsTrade)

	// Public verifiable ledger (Spec A12)
	s.mux.HandleFunc("GET /v1/ledger", s.handleLedger)

	// Simulation / Conformance round clearing trigger
	s.mux.HandleFunc("POST /v1/sim/clear", s.handleSimClear)
}

func (s *Server) handleManifest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.eng.Manifest())
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	snap := s.eng.StateSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ready",
		"game_id":       snap.GameID,
		"current_round": snap.CurrentRound,
		"last_seq":      snap.LastSeq,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metrics != nil {
		snap := s.eng.StateSnapshot()
		var bytesCount int64
		if sizer, ok := s.eng.Log().(interface{ Bytes() int64 }); ok {
			bytesCount = sizer.Bytes()
		}
		s.metrics.SetLedgerHead(snap.LastSeq, bytesCount)
		lockedTotals := make(map[string]int64)
		for _, ag := range snap.Agents {
			for unit, amt := range ag.Locked {
				lockedTotals[unit] += amt
			}
		}
		for unit, amt := range lockedTotals {
			s.metrics.SetEscrowLocked(unit, amt)
		}
	}
	s.metrics.Handler().ServeHTTP(w, r)
}

func (s *Server) handleEnvelope(w http.ResponseWriter, r *http.Request) {
	// Size limit before signature verification (Spec A14 DoS mitigation: max 64 KB)
	const maxEnvelopeBytes = 64 * 1024
	body, err := io.ReadAll(io.LimitReader(r.Body, maxEnvelopeBytes+1))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if len(body) > maxEnvelopeBytes {
		http.Error(w, "envelope exceeds maximum allowed size (64 KB)", http.StatusRequestEntityTooLarge)
		return
	}
	receipt, rej := s.eng.SubmitRawEnvelope(r.Context(), body)
	if rej != nil {
		if s.metrics != nil {
			s.metrics.IncOrder(string(rej.Code))
		}
		writeJSON(w, http.StatusUnprocessableEntity, rej)
		return
	}
	if s.metrics != nil && receipt != nil {
		if receipt.OrderID != "" {
			s.metrics.IncOrder("accepted")
		} else if receipt.PostID != "" {
			s.metrics.IncBoardPost("accepted")
		}
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) handleRound(w http.ResponseWriter, _ *http.Request) {
	snap := s.eng.StateSnapshot()
	m := s.eng.Manifest()
	start, end, _ := m.RoundBounds(snap.CurrentRound)
	writeJSON(w, http.StatusOK, map[string]any{
		"game_id":       snap.GameID,
		"round":         snap.CurrentRound,
		"status":        snap.RoundStatus,
		"halted":        snap.HaltedGlobal,
		"round_start":   start.Unix(),
		"round_cutoff":  end.Unix(),
		"last_seq":      snap.LastSeq,
		"last_hash":     snap.LastHash,
	})
}

func (s *Server) handlePortfolio(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	snap := s.eng.StateSnapshot()
	ag, ok := snap.Agents[agentID]
	if !ok {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}
	var orders []*ledger.OpenOrder
	for _, o := range snap.OpenOrders {
		if o.AgentID == agentID {
			orders = append(orders, o)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id":    ag.AgentID,
		"available":   ag.Available,
		"locked":      ag.Locked,
		"open_orders": orders,
	})
}

func (s *Server) handleFills(w http.ResponseWriter, r *http.Request) {
	sinceSeq, _ := strconv.ParseUint(r.URL.Query().Get("since_seq"), 10, 64)
	agentID := r.URL.Query().Get("agent_id")
	var fills []map[string]any
	for rec, err := range s.eng.Log().Read(r.Context(), sinceSeq+1) {
		if err != nil {
			break
		}
		if rec.Type == ledger.RecFill {
			var f map[string]any
			if err := json.Unmarshal(rec.Body, &f); err == nil {
				if agentID == "" || f["agent_id"] == agentID {
					f["seq"] = rec.Seq
					fills = append(fills, f)
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"fills": fills})
}

func (s *Server) handleTape(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	rounds, _ := strconv.Atoi(r.URL.Query().Get("rounds"))
	writeJSON(w, http.StatusOK, map[string]any{
		"asset": asset,
		"tape":  s.eng.Tape(asset, rounds),
	})
}

func (s *Server) handleCandles(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	rounds, _ := strconv.Atoi(r.URL.Query().Get("rounds"))
	if rounds <= 0 || rounds > 168 {
		rounds = 168
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"asset":   asset,
		"candles": s.eng.Candles(asset, rounds),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	rounds, _ := strconv.Atoi(r.URL.Query().Get("rounds"))
	writeJSON(w, http.StatusOK, s.eng.Stats(asset, rounds))
}

func (s *Server) handleIndicative(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	q, err := s.eng.Indicative(asset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (s *Server) handleDepth(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	d, err := s.eng.Depth(asset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, cancel := s.eng.Subscribe()
	defer cancel()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, string(b))
			flusher.Flush()
		}
	}
}

func (s *Server) handleBoardPosts(w http.ResponseWriter, r *http.Request) {
	asset := r.URL.Query().Get("asset")
	sinceSeq, _ := strconv.ParseUint(r.URL.Query().Get("since_seq"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, http.StatusOK, map[string]any{
		"posts": s.eng.BoardPosts(asset, sinceSeq, limit),
	})
}

func (s *Server) handleTalkVsTrade(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	asset := r.URL.Query().Get("asset")
	rounds, _ := strconv.ParseUint(r.URL.Query().Get("rounds"), 10, 64)
	writeJSON(w, http.StatusOK, s.eng.TalkVsTrade(agentID, asset, rounds))
}

func (s *Server) handleLedger(w http.ResponseWriter, r *http.Request) {
	fromSeq, _ := strconv.ParseUint(r.URL.Query().Get("from_seq"), 10, 64)
	if fromSeq <= 1 {
		fromSeq = 1
	}
	m := s.eng.Manifest()
	snap := s.eng.StateSnapshot()

	// Spec A12: Order contents within an open round are not publicly readable when ledger.public == "after-clear".
	// Use cached FirstOpenOrderSeq in O(1) without scanning from genesis.
	var maxPublicSeq uint64 = snap.LastSeq
	publicStateHash := ""
	if m.Ledger.Public == "after-clear" && snap.RoundStatus == ledger.RoundStatusOpen && snap.FirstOpenOrderSeq > 0 {
		maxPublicSeq = snap.FirstOpenOrderSeq - 1
		publicStateHash = snap.PreOrderStateHash
	}

	if strings.Contains(r.Header.Get("Accept"), "application/json") && r.URL.Query().Get("format") == "array" {
		var recs []ledger.Record
		for rec, err := range s.eng.Log().Read(r.Context(), fromSeq) {
			if err != nil || rec.Seq > maxPublicSeq {
				break
			}
			recs = append(recs, rec)
		}
		if publicStateHash == "" {
			publicStateHash, _ = s.eng.StateHash()
		}
		writeJSON(w, http.StatusOK, map[string]any{"records": recs, "state_hash": publicStateHash})
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	for rec, err := range s.eng.Log().Read(r.Context(), fromSeq) {
		if err != nil || rec.Seq > maxPublicSeq {
			break
		}
		b, _ := json.Marshal(rec)
		_, _ = w.Write(b)
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *Server) handleSimClear(w http.ResponseWriter, r *http.Request) {
	if !s.allowSimStep {
		http.Error(w, "simulation clear endpoint is disabled", http.StatusForbidden)
		return
	}
	t0 := time.Now()
	fills, err := s.eng.SealAndClearRound(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if s.metrics != nil {
		s.metrics.ObserveClearDuration(time.Since(t0).Seconds())
	}
	sh, _ := s.eng.StateHash()
	writeJSON(w, http.StatusOK, map[string]any{
		"fills":      fills,
		"state_hash": sh,
	})
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	spec := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":   "AgentSquare Protocol HTTP Binding",
			"version": "1.0.0",
		},
		"paths": map[string]any{
			manifest.WellKnownPath:                   map[string]any{"get": map[string]any{"summary": "Get game manifest"}},
			"/v1/envelope":                           map[string]any{"post": map[string]any{"summary": "Submit signed protocol envelope"}},
			"/v1/round":                              map[string]any{"get": map[string]any{"summary": "Get current round state"}},
			"/v1/agents/{id}/portfolio":              map[string]any{"get": map[string]any{"summary": "Get agent balances and open orders"}},
			"/v1/fills":                              map[string]any{"get": map[string]any{"summary": "Pull fills since sequence"}},
			"/v1/market/tape":                        map[string]any{"get": map[string]any{"summary": "Get post-clear public tape"}},
			"/v1/market/candles":                     map[string]any{"get": map[string]any{"summary": "Get per-round OHLCV candles"}},
			"/v1/market/stats":                       map[string]any{"get": map[string]any{"summary": "Get market statistics"}},
			"/v1/market/book/indicative":             map[string]any{"get": map[string]any{"summary": "Get indicative clearing price and volume"}},
			"/v1/market/book/depth":                  map[string]any{"get": map[string]any{"summary": "Get aggregated price level depth"}},
			"/v1/market/stream":                      map[string]any{"get": map[string]any{"summary": "SSE real-time market stream"}},
			"/v1/board/posts":                        map[string]any{"get": map[string]any{"summary": "List public board posts"}},
			"/v1/board/agents/{id}/talk-vs-trade":    map[string]any{"get": map[string]any{"summary": "Get agent talk-vs-trade transparency report"}},
			"/v1/ledger":                             map[string]any{"get": map[string]any{"summary": "Stream append-only hash-chained ledger records"}},
		},
	}
	writeJSON(w, http.StatusOK, spec)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
