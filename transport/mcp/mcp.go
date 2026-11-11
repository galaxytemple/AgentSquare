package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/galaxytemple/AgentSquare/board"
	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Policy defines local signing proxy guardrails loaded from ~/.agentsquare/policy.toml (Spec B2).
// Serves as the last line of defense against prompt-injected trading instructions.
type Policy struct {
	MaxOrderValue     int64    `json:"max_order_value"`
	MaxOrdersPerRound int64    `json:"max_orders_per_round"`
	AllowedAssets     []string `json:"allowed_assets"`
	ConfirmWrites     bool     `json:"confirm_writes"`
}

// ParsePolicyTOML parses a simple TOML policy file into a Policy struct.
func ParsePolicyTOML(content string) (Policy, error) {
	var p Policy
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" || strings.HasPrefix(line, "[") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "max_order_value":
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return p, err
			}
			p.MaxOrderValue = v
		case "max_orders_per_round":
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return p, err
			}
			p.MaxOrdersPerRound = v
		case "confirm_writes":
			p.ConfirmWrites = val == "true"
		case "allowed_assets":
			val = strings.Trim(val, "[] ")
			if val != "" {
				for _, item := range strings.Split(val, ",") {
					item = strings.Trim(strings.TrimSpace(item), `"'`)
					if item != "" {
						p.AllowedAssets = append(p.AllowedAssets, item)
					}
				}
			}
		}
	}
	return p, nil
}

// CheckOrder validates an order against the local Policy before signing.
func (p Policy) CheckOrder(asset string, limitPrice, qty, ordersInCurrentRound int64) error {
	if limitPrice < 0 || qty < 0 {
		return fmt.Errorf("policy violation: negative price or quantity")
	}
	orderVal, err := protocol.SafeMulInt(limitPrice, qty)
	if err != nil {
		return fmt.Errorf("policy violation: order value overflow: %w", err)
	}
	if p.MaxOrderValue > 0 && orderVal > p.MaxOrderValue {
		return fmt.Errorf("policy violation: order value %d exceeds max_order_value %d", orderVal, p.MaxOrderValue)
	}
	if p.MaxOrdersPerRound > 0 && ordersInCurrentRound >= p.MaxOrdersPerRound {
		return fmt.Errorf("policy violation: round order count %d reached max_orders_per_round %d", ordersInCurrentRound, p.MaxOrdersPerRound)
	}
	if len(p.AllowedAssets) > 0 && asset != "" {
		allowed := false
		for _, a := range p.AllowedAssets {
			if a == asset {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("policy violation: asset %q is not in allowed_assets %v", asset, p.AllowedAssets)
		}
	}
	return nil
}

// GameTool represents a custom game-specific tool registered with a "game." prefix (Spec B2).
type GameTool struct {
	Name        string // Will be prefixed with "game." if not already
	Description string
	Handler     func(ctx context.Context, args map[string]any) (any, error)
}

// Proxy implements the Model Context Protocol (MCP) server over stdio JSON-RPC 2.0 (Spec B2).
// - Form A (Local signing proxy): signer != nil -> exposes both read and write tools, signs locally.
// - Form B (Remote read-only MCP): signer == nil -> exposes only read tools; private keys never touch the server.
type Proxy struct {
	mu            sync.Mutex
	cli           *client.Client
	readOnly      bool
	policy        Policy
	gameTools     map[string]GameTool
	ordersByRound map[uint64]int64
}

// Option configures a Proxy.
type Option func(*Proxy)

func WithPolicy(p Policy) Option {
	return func(pr *Proxy) { pr.policy = p }
}

func WithReadOnly(ro bool) Option {
	return func(pr *Proxy) { pr.readOnly = ro }
}

// WithGameTools registers game-specific tools under the "game." prefix (e.g. "game.submit_design", Spec B2).
func WithGameTools(tools ...GameTool) Option {
	return func(pr *Proxy) {
		for _, gt := range tools {
			if !strings.HasPrefix(gt.Name, "game.") {
				gt.Name = "game." + gt.Name
			}
			pr.gameTools[gt.Name] = gt
		}
	}
}

// NewProxy creates a new MCP Proxy backed by an AgentSquare HTTP client.
func NewProxy(cli *client.Client, opts ...Option) *Proxy {
	pr := &Proxy{
		cli:           cli,
		gameTools:     make(map[string]GameTool),
		ordersByRound: make(map[uint64]int64),
	}
	for _, opt := range opts {
		opt(pr)
	}
	return pr
}

// JSON-RPC 2.0 wire types
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ServeStdio processes line-delimited JSON-RPC 2.0 MCP messages from in and writes responses to out.
func (p *Proxy) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	enc := json.NewEncoder(out)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			_ = enc.Encode(rpcResponse{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: -32700, Message: "parse error"},
			})
			continue
		}
		resp := p.HandleRPC(ctx, req)
		if req.ID != nil {
			_ = enc.Encode(resp)
		}
	}
	return scanner.Err()
}

// HandleRPC processes a single MCP JSON-RPC 2.0 request.
func (p *Proxy) HandleRPC(ctx context.Context, req rpcRequest) rpcResponse {
	switch req.Method {
	case "initialize":
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"serverInfo": map[string]any{
					"name":    "agentsquare-mcp",
					"version": "1.0.0",
				},
				"capabilities": map[string]any{
					"tools":     map[string]any{},
					"resources": map[string]any{},
					"prompts":   map[string]any{},
				},
			},
		}

	case "tools/list":
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{"tools": p.listTools()},
		}

	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: -32602, Message: err.Error()}}
		}
		res, isErr := p.CallTool(ctx, params.Name, params.Arguments)
		textBytes, _ := json.Marshal(res)
		// Spec B2: Responses <= 8 KB with UTF-8 & JSON syntax-preserving truncation safeguard
		if len(textBytes) > 8192 {
			preview := truncateUTF8(string(textBytes), 7900)
			textBytes, _ = json.Marshal(map[string]any{
				"truncated": true,
				"preview":   preview,
			})
		}
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"isError": isErr,
				"content": []map[string]any{
					{"type": "text", "text": string(textBytes)},
				},
			},
		}

	case "resources/list":
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"resources": []map[string]any{
					{"uri": "agentsquare://manifest", "name": "Game Manifest", "mimeType": "application/json"},
					{"uri": "agentsquare://candles/copper", "name": "Copper OHLCV Candles", "mimeType": "application/json"},
				},
			},
		}

	case "resources/read":
		var params struct {
			URI string `json:"uri"`
		}
		_ = json.Unmarshal(req.Params, &params)
		res, err := p.readResource(ctx, params.URI)
		if err != nil {
			return rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: -32602, Message: err.Error()}}
		}
		b, _ := json.Marshal(res)
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"contents": []map[string]any{
					{"uri": params.URI, "mimeType": "application/json", "text": string(b)},
				},
			},
		}

	case "prompts/list":
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"prompts": []map[string]any{
					{"name": "trading_turn", "description": "Round state + portfolio + market + safe board summary for an agent trading turn"},
				},
			},
		}

	case "prompts/get":
		promptText, _ := p.buildTradingTurnPrompt(ctx)
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"messages": []map[string]any{
					{
						"role":    "user",
						"content": map[string]any{"type": "text", "text": promptText},
					},
				},
			},
		}

	default:
		return rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32601, Message: "method not found: " + req.Method},
		}
	}
}

func (p *Proxy) listTools() []map[string]any {
	readTools := []map[string]any{
		{"name": "get_manifest", "description": "Read the game manifest (/.well-known/agentsquare.json)"},
		{"name": "get_round", "description": "Read current round number, status, and cutoff timestamp"},
		{"name": "get_portfolio", "description": "Read agent available/locked balances and open orders"},
		{"name": "get_market", "description": "Read indicative clearing price/volume and market stats for an asset"},
		{"name": "get_candles", "description": "Read OHLCV candles for an asset (up to 168 rounds)"},
		{"name": "get_tape", "description": "Read post-clear public tape for an asset"},
		{"name": "read_board", "description": "Read public board posts wrapped with untrusted-text prompt injection protection"},
		{"name": "talk_vs_trade", "description": "Read talk-vs-trade transparency report for an agent"},
	}
	if p.readOnly {
		return readTools
	}
	writeTools := []map[string]any{
		{"name": "place_order", "description": "Submit a signed buy or sell limit/market order"},
		{"name": "cancel_order", "description": "Cancel an open order before round cutoff"},
		{"name": "replace_order", "description": "Atomically replace an open order's limit_price and qty"},
		{"name": "post_board", "description": "Publish a non-binding public statement on the board"},
		{"name": "retract_board", "description": "Retract an authored public board post"},
	}
	all := append(readTools, writeTools...)
	for _, gt := range p.gameTools {
		all = append(all, map[string]any{
			"name":        gt.Name,
			"description": gt.Description,
		})
	}
	return all
}

// CallTool executes one of the 12 normative MCP tools or a registered game.* tool (Spec B2).
func (p *Proxy) CallTool(ctx context.Context, name string, args map[string]any) (res any, isError bool) {
	switch name {
	case "get_manifest":
		m, err := p.cli.GetManifest(ctx)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return m, false

	case "get_round":
		r, err := p.cli.GetRound(ctx)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return r, false

	case "get_portfolio":
		agentID, _ := args["agent_id"].(string)
		pf, err := p.cli.GetPortfolio(ctx, agentID)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return pf, false

	case "get_market":
		asset, _ := args["asset"].(string)
		ind, _ := p.cli.GetIndicative(ctx, asset)
		st, err := p.cli.GetStats(ctx, asset, 24)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		// Spec B2: LLM-friendly integers with display values
		return map[string]any{
			"asset": asset,
			"indicative": map[string]any{
				"price":   ind.IndicativePrice,
				"display": fmt.Sprintf("$%d", ind.IndicativePrice),
				"volume":  ind.IndicativeVolume,
			},
			"stats": st,
		}, false

	case "get_candles":
		asset, _ := args["asset"].(string)
		rounds := toInt(args["rounds"], 24)
		if rounds > 168 {
			rounds = 168
		}
		c, err := p.cli.GetCandles(ctx, asset, rounds)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return map[string]any{"asset": asset, "candles": c}, false

	case "get_tape":
		asset, _ := args["asset"].(string)
		rounds := toInt(args["rounds"], 10)
		t, err := p.cli.GetTape(ctx, asset, rounds)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return map[string]any{"asset": asset, "tape": t}, false

	case "read_board":
		asset, _ := args["asset"].(string)
		since := uint64(toInt(args["since"], 0))
		limit := toInt(args["limit"], 50)
		if limit > 50 {
			limit = 50
		}
		posts, err := p.cli.ReadBoard(ctx, asset, since, limit)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return map[string]any{
			"posts":          posts,
			"llm_safe_block": board.FormatForLLM(posts),
		}, false

	case "talk_vs_trade":
		agentID, _ := args["agent_id"].(string)
		asset, _ := args["asset"].(string)
		rep, err := p.cli.TalkVsTrade(ctx, agentID, asset, 5)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		return rep, false

	case "place_order":
		if p.readOnly {
			return map[string]any{"code": "UNAUTHORIZED", "reason": "remote MCP is read-only", "fix_hint": "use Form A local signing proxy"}, true
		}
		if p.policy.ConfirmWrites && !toBool(args["confirmed"]) {
			return map[string]any{
				"code":     "CONFIRMATION_REQUIRED",
				"reason":   "policy.confirm_writes is enabled",
				"fix_hint": "re-invoke place_order with confirmed=true after user confirmation",
			}, true
		}
		asset, _ := args["asset"].(string)
		side := protocol.Side(fmt.Sprint(args["side"]))
		limitPrice := int64(toInt(args["limit_price"], 0))
		qty := int64(toInt(args["qty"], 0))
		opts := client.OrderOptions{
			Market:    toBool(args["market"]),
			CarryOver: toBool(args["carry_over"]),
			TTLRounds: uint64(toInt(args["ttl_rounds"], 0)),
		}

		rs, _ := p.cli.GetRound(ctx)
		var roundNum uint64 = 1
		if rs != nil {
			roundNum = rs.Round
		}

		p.mu.Lock()
		countInRound := p.ordersByRound[roundNum]
		if err := p.policy.CheckOrder(asset, limitPrice, qty, countInRound); err != nil {
			p.mu.Unlock()
			return map[string]any{
				"code":     "POLICY_BLOCKED",
				"reason":   err.Error(),
				"fix_hint": "adjust order within ~/.agentsquare/policy.toml limits",
			}, true
		}
		p.mu.Unlock()

		rec, rej, err := p.cli.PlaceOrderWithOptions(ctx, asset, side, limitPrice, qty, opts)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		if rej != nil {
			return rej, true
		}
		p.mu.Lock()
		p.ordersByRound[roundNum]++
		p.mu.Unlock()
		return rec, false

	case "cancel_order":
		if p.readOnly {
			return map[string]any{"code": "UNAUTHORIZED", "reason": "remote MCP is read-only"}, true
		}
		if p.policy.ConfirmWrites && !toBool(args["confirmed"]) {
			return map[string]any{
				"code":     "CONFIRMATION_REQUIRED",
				"reason":   "policy.confirm_writes is enabled",
				"fix_hint": "re-invoke cancel_order with confirmed=true after user confirmation",
			}, true
		}
		orderID, _ := args["order_id"].(string)
		rec, rej, err := p.cli.CancelOrder(ctx, orderID)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		if rej != nil {
			return rej, true
		}
		return rec, false

	case "replace_order":
		if p.readOnly {
			return map[string]any{"code": "UNAUTHORIZED", "reason": "remote MCP is read-only"}, true
		}
		if p.policy.ConfirmWrites && !toBool(args["confirmed"]) {
			return map[string]any{
				"code":     "CONFIRMATION_REQUIRED",
				"reason":   "policy.confirm_writes is enabled",
				"fix_hint": "re-invoke replace_order with confirmed=true after user confirmation",
			}, true
		}
		orderID, _ := args["order_id"].(string)
		asset, _ := args["asset"].(string)
		limitPrice := int64(toInt(args["limit_price"], 0))
		qty := int64(toInt(args["qty"], 0))

		if asset == "" && len(p.policy.AllowedAssets) > 0 {
			if pf, err := p.cli.GetPortfolio(ctx, ""); err == nil && pf != nil {
				for _, o := range pf.OpenOrders {
					if o.OrderID == orderID {
						asset = o.Asset
						break
					}
				}
			}
		}

		rs, _ := p.cli.GetRound(ctx)
		var roundNum uint64 = 1
		if rs != nil {
			roundNum = rs.Round
		}

		p.mu.Lock()
		countInRound := p.ordersByRound[roundNum]
		if err := p.policy.CheckOrder(asset, limitPrice, qty, countInRound); err != nil {
			p.mu.Unlock()
			return map[string]any{
				"code":     "POLICY_BLOCKED",
				"reason":   err.Error(),
				"fix_hint": "adjust replacement order within ~/.agentsquare/policy.toml limits",
			}, true
		}
		p.mu.Unlock()

		rec, rej, err := p.cli.ReplaceOrder(ctx, orderID, limitPrice, qty)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		if rej != nil {
			return rej, true
		}
		p.mu.Lock()
		p.ordersByRound[roundNum]++
		p.mu.Unlock()
		return rec, false

	case "post_board":
		if p.readOnly {
			return map[string]any{"code": "UNAUTHORIZED", "reason": "remote MCP is read-only"}, true
		}
		if p.policy.ConfirmWrites && !toBool(args["confirmed"]) {
			return map[string]any{
				"code":     "CONFIRMATION_REQUIRED",
				"reason":   "policy.confirm_writes is enabled",
				"fix_hint": "re-invoke post_board with confirmed=true after user confirmation",
			}, true
		}
		asset, _ := args["asset"].(string)
		body, _ := args["body"].(string)
		stance, _ := args["stance"].(string)
		replyTo, _ := args["reply_to"].(string)
		rec, rej, err := p.cli.PostBoard(ctx, asset, stance, body, replyTo)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		if rej != nil {
			return rej, true
		}
		return rec, false

	case "retract_board":
		if p.readOnly {
			return map[string]any{"code": "UNAUTHORIZED", "reason": "remote MCP is read-only"}, true
		}
		if p.policy.ConfirmWrites && !toBool(args["confirmed"]) {
			return map[string]any{
				"code":     "CONFIRMATION_REQUIRED",
				"reason":   "policy.confirm_writes is enabled",
				"fix_hint": "re-invoke retract_board with confirmed=true after user confirmation",
			}, true
		}
		postID, _ := args["post_id"].(string)
		rec, rej, err := p.cli.RetractBoard(ctx, postID)
		if err != nil {
			return map[string]any{"error": err.Error()}, true
		}
		if rej != nil {
			return rej, true
		}
		return rec, false

	default:
		if gt, ok := p.gameTools[name]; ok {
			out, err := gt.Handler(ctx, args)
			if err != nil {
				return map[string]any{"error": err.Error()}, true
			}
			return out, false
		}
		return map[string]any{"error": "unknown tool: " + name}, true
	}
}

func (p *Proxy) readResource(ctx context.Context, uri string) (any, error) {
	if uri == "agentsquare://manifest" || uri == "roundabout://manifest" {
		return p.cli.GetManifest(ctx)
	}
	if strings.HasPrefix(uri, "agentsquare://candles/") || strings.HasPrefix(uri, "roundabout://candles/") {
		parts := strings.Split(uri, "/")
		asset := parts[len(parts)-1]
		return p.cli.GetCandles(ctx, asset, 24)
	}
	return nil, fmt.Errorf("unknown resource URI: %s", uri)
}

func (p *Proxy) buildTradingTurnPrompt(ctx context.Context) (string, error) {
	rs, _ := p.cli.GetRound(ctx)
	pf, _ := p.cli.GetPortfolio(ctx, "")
	posts, _ := p.cli.ReadBoard(ctx, "", 0, 10)
	roundB, _ := json.Marshal(rs)
	pfB, _ := json.Marshal(pf)
	return fmt.Sprintf("Round State: %s\nPortfolio: %s\n%s",
		string(roundB), string(pfB), board.FormatForLLM(posts)), nil
}

func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	b := s[:maxBytes]
	for len(b) > 0 && !utf8.ValidString(b) {
		b = b[:len(b)-1]
	}
	return b
}

func toBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val == "true" || val == "1"
	}
	return false
}

func toInt(v any, def int) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return def
}
