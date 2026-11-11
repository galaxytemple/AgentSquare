package mcp_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
	"github.com/galaxytemple/AgentSquare/transport/mcp"
)

func TestMCPProxyToolsAndPolicyGuard(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	v.Manifest.Board = &manifest.BoardConfig{Enabled: true, MaxPostsPerAgentPerRound: 5, MaxBodyChars: 280}
	eng, _, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
	defer srv.Close()

	seed := make([]byte, 32)
	seed[0] = 88
	agKey, _ := protocol.NewSignerFromSeed(seed)

	cli := client.New(srv.URL, vecs[0].Manifest.GameID, agKey)
	if _, rej, err := cli.Register(ctx, "mcp-agent", ""); err != nil || rej != nil {
		t.Fatalf("register failed: err=%v rej=%+v", err, rej)
	}

	pol, err := mcp.ParsePolicyTOML(`
max_order_value = 30
max_orders_per_round = 2
allowed_assets = ["copper"]
confirm_writes = false
`)
	if err != nil {
		t.Fatalf("ParsePolicyTOML: %v", err)
	}

	proxy := mcp.NewProxy(cli, mcp.WithPolicy(pol), mcp.WithGameTools(mcp.GameTool{
		Name:        "submit_design",
		Description: "Submit city design",
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]any{"score": 950}, nil
		},
	}))

	// 1. Read tool: get_round
	if _, isErr := proxy.CallTool(ctx, "get_round", nil); isErr {
		t.Fatalf("get_round failed")
	}

	// 2. Write tool blocked by policy (order value 9*5 = 45 > max_order_value 30)
	if res, isErr := proxy.CallTool(ctx, "place_order", map[string]any{
		"asset": "copper", "side": "buy", "limit_price": 9, "qty": 5,
	}); !isErr {
		t.Fatalf("expected policy to block order > 30, got %+v", res)
	}

	// 3. Valid order within policy (9*2 = 18 <= 30)
	OrderRes, isErr := proxy.CallTool(ctx, "place_order", map[string]any{
		"asset": "copper", "side": "buy", "limit_price": 9, "qty": 2,
	})
	if isErr {
		t.Fatalf("expected valid order within policy to succeed, got %+v", OrderRes)
	}

	// 4. Read tools: get_manifest, get_portfolio, get_market, get_candles, get_tape, read_board, talk_vs_trade
	for _, toolName := range []string{"get_manifest", "get_portfolio", "get_market", "get_candles", "get_tape", "read_board", "talk_vs_trade"} {
		if res, errFlag := proxy.CallTool(ctx, toolName, map[string]any{"asset": "copper", "agent_id": agKey.AgentID(), "rounds": 5}); errFlag {
			t.Fatalf("read tool %s failed: %+v", toolName, res)
		}
	}

	// 5. Write tools: post_board, retract_board, replace_order, cancel_order
	postRes, errFlag := proxy.CallTool(ctx, "post_board", map[string]any{
		"asset": "copper", "stance": "bull", "body": "looking strong",
	})
	if errFlag {
		t.Fatalf("post_board failed: %+v", postRes)
	}
	if pRec, ok := postRes.(*protocol.AcceptanceReceipt); ok && pRec != nil {
		if retRes, rErr := proxy.CallTool(ctx, "retract_board", map[string]any{"post_id": pRec.PostID}); rErr {
			t.Fatalf("retract_board failed: %+v", retRes)
		}
	}

	// Test policy.confirm_writes guardrail
	confirmProxy := mcp.NewProxy(cli, mcp.WithPolicy(mcp.Policy{ConfirmWrites: true}))
	if res, isErr := confirmProxy.CallTool(ctx, "place_order", map[string]any{
		"asset": "copper", "side": "buy", "limit_price": 8, "qty": 1,
	}); !isErr {
		t.Fatalf("expected ConfirmWrites to block unconfirmed place_order, got %+v", res)
	}
	if res, isErr := confirmProxy.CallTool(ctx, "place_order", map[string]any{
		"asset": "copper", "side": "buy", "limit_price": 8, "qty": 1, "confirmed": true,
	}); isErr {
		t.Fatalf("expected confirmed place_order to succeed, got %+v", res)
	}
	if rec, ok := OrderRes.(*protocol.AcceptanceReceipt); ok && rec != nil {
		// Policy must also block replace_order if value exceeds max_order_value (50) or overflows int64
		if _, errFlag := proxy.CallTool(ctx, "replace_order", map[string]any{
			"order_id": rec.OrderID, "limit_price": 10, "qty": 10,
		}); !errFlag {
			t.Fatalf("expected policy to block replace_order with value 100 > max_order_value 50")
		}
		if err := pol.CheckOrder("copper", 1<<30, 1<<30, 0); err == nil {
			t.Fatalf("expected CheckOrder to reject multiplication exceeding MaxSafeInt / max_order_value")
		}

		repRes, errFlag := proxy.CallTool(ctx, "replace_order", map[string]any{
			"order_id": rec.OrderID, "limit_price": 8, "qty": 2,
		})
		if errFlag {
			t.Fatalf("replace_order failed: %+v", repRes)
		}
		if repRec, ok := repRes.(*protocol.AcceptanceReceipt); ok && repRec != nil {
			if cRes, cErr := proxy.CallTool(ctx, "cancel_order", map[string]any{"order_id": repRec.OrderID}); cErr {
				t.Fatalf("cancel_order failed: %+v", cRes)
			}
		}
	}

	// 6. Custom game.submit_design tool & ServeStdio JSON-RPC (initialize, tools/list, tools/call, resources/*, prompts/*)
	if res, isErr := proxy.CallTool(ctx, "game.submit_design", nil); isErr {
		t.Fatalf("game.submit_design failed: %+v", res)
	}

	var outBuf bytes.Buffer
	rpcIn := strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize"}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/list"}` + "\n" +
			`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_round","arguments":{}}}` + "\n" +
			`{"jsonrpc":"2.0","id":4,"method":"resources/list"}` + "\n" +
			`{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"agentsquare://manifest"}}` + "\n" +
			`{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"agentsquare://candles/copper"}}` + "\n" +
			`{"jsonrpc":"2.0","id":7,"method":"prompts/list"}` + "\n" +
			`{"jsonrpc":"2.0","id":8,"method":"prompts/get","params":{"name":"trading_turn"}}` + "\n",
	)
	if err := proxy.ServeStdio(ctx, rpcIn, &outBuf); err != nil {
		t.Fatalf("ServeStdio failed: %v", err)
	}
	if !strings.Contains(outBuf.String(), "protocolVersion") || !strings.Contains(outBuf.String(), "place_order") {
		t.Fatalf("unexpected ServeStdio output: %s", outBuf.String())
	}
}
