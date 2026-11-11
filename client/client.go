package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/galaxytemple/AgentSquare/board"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/marketdata"
	"github.com/galaxytemple/AgentSquare/protocol"
)

// Client is the official Go SDK client for speaking the AgentSquare protocol over HTTP (Spec B5 & C5).
type Client struct {
	baseURL    string
	gameID     string
	agentID    string
	signer     protocol.Signer
	httpClient *http.Client
	nonceSeq   atomic.Uint64
}

// New creates a new AgentSquare Client.
func New(baseURL, gameID string, signer protocol.Signer) *Client {
	agID := ""
	if signer != nil {
		agID = signer.AgentID()
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		gameID:     gameID,
		agentID:    agID,
		signer:     signer,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// AgentID returns the client's stable agent identity (preserved across key rotations).
func (c *Client) AgentID() string {
	if c.agentID != "" {
		return c.agentID
	}
	if c.signer != nil {
		return c.signer.AgentID()
	}
	return ""
}

func (c *Client) nextID(prefix string) string {
	n := c.nonceSeq.Add(1)
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), n)
}

// GetManifest fetches /.well-known/agentsquare.json and updates the client's gameID if empty.
func (c *Client) GetManifest(ctx context.Context) (*manifest.Manifest, error) {
	var m manifest.Manifest
	if err := c.getJSON(ctx, manifest.WellKnownPath, &m); err != nil {
		return nil, err
	}
	if c.gameID == "" {
		c.gameID = m.GameID
	}
	return &m, nil
}

// RoundState represents the response of GET /v1/round.
type RoundState struct {
	GameID      string             `json:"game_id"`
	Round       uint64             `json:"round"`
	Status      ledger.RoundStatus `json:"status"`
	Halted      bool               `json:"halted"`
	RoundStart  int64              `json:"round_start"`
	RoundCutoff int64              `json:"round_cutoff"`
	LastSeq     uint64             `json:"last_seq"`
	LastHash    string             `json:"last_hash"`
}

func (c *Client) GetRound(ctx context.Context) (*RoundState, error) {
	var rs RoundState
	if err := c.getJSON(ctx, "/v1/round", &rs); err != nil {
		return nil, err
	}
	return &rs, nil
}

// Portfolio represents the response of GET /v1/agents/{id}/portfolio.
type Portfolio struct {
	AgentID    string              `json:"agent_id"`
	Available  map[string]int64    `json:"available"`
	Locked     map[string]int64    `json:"locked"`
	OpenOrders []*ledger.OpenOrder `json:"open_orders"`
}

func (c *Client) GetPortfolio(ctx context.Context, agentID string) (*Portfolio, error) {
	if agentID == "" {
		agentID = c.AgentID()
	}
	var p Portfolio
	if err := c.getJSON(ctx, "/v1/agents/"+url.PathEscape(agentID)+"/portfolio", &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (c *Client) GetIndicative(ctx context.Context, asset string) (*marketdata.IndicativeQuote, error) {
	var q marketdata.IndicativeQuote
	if err := c.getJSON(ctx, "/v1/market/book/indicative?asset="+url.QueryEscape(asset), &q); err != nil {
		return nil, err
	}
	return &q, nil
}

func (c *Client) GetCandles(ctx context.Context, asset string, rounds int) ([]marketdata.Candle, error) {
	var res struct {
		Candles []marketdata.Candle `json:"candles"`
	}
	path := fmt.Sprintf("/v1/market/candles?asset=%s&rounds=%d", url.QueryEscape(asset), rounds)
	if err := c.getJSON(ctx, path, &res); err != nil {
		return nil, err
	}
	return res.Candles, nil
}

func (c *Client) GetTape(ctx context.Context, asset string, rounds int) ([]marketdata.TapeEntry, error) {
	var res struct {
		Tape []marketdata.TapeEntry `json:"tape"`
	}
	path := fmt.Sprintf("/v1/market/tape?asset=%s&rounds=%d", url.QueryEscape(asset), rounds)
	if err := c.getJSON(ctx, path, &res); err != nil {
		return nil, err
	}
	return res.Tape, nil
}

func (c *Client) GetStats(ctx context.Context, asset string, rounds int) (*marketdata.Stats, error) {
	var st marketdata.Stats
	path := fmt.Sprintf("/v1/market/stats?asset=%s&rounds=%d", url.QueryEscape(asset), rounds)
	if err := c.getJSON(ctx, path, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (c *Client) ReadBoard(ctx context.Context, asset string, sinceSeq uint64, limit int) ([]board.Post, error) {
	var res struct {
		Posts []board.Post `json:"posts"`
	}
	path := fmt.Sprintf("/v1/board/posts?asset=%s&since_seq=%d&limit=%d",
		url.QueryEscape(asset), sinceSeq, limit)
	if err := c.getJSON(ctx, path, &res); err != nil {
		return nil, err
	}
	return res.Posts, nil
}

func (c *Client) TalkVsTrade(ctx context.Context, agentID, asset string, rounds uint64) (*board.TalkVsTradeReport, error) {
	var rep board.TalkVsTradeReport
	path := fmt.Sprintf("/v1/board/agents/%s/talk-vs-trade?asset=%s&rounds=%d",
		url.PathEscape(agentID), url.QueryEscape(asset), rounds)
	if err := c.getJSON(ctx, path, &rep); err != nil {
		return nil, err
	}
	return &rep, nil
}

// Register submits an agent.register envelope.
func (c *Client) Register(ctx context.Context, displayName, inviteCode string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypeAgentRegister, protocol.RegisterPayload{
		PubKey:      c.signer.PublicKeyString(),
		DisplayName: displayName,
		InviteCode:  inviteCode,
	})
}

// RotateKey submits a dual-signed key.rotate envelope (signed by the current key and proof-of-possession signed by newSigner),
// and updates the client's active signer to newSigner upon acceptance.
func (c *Client) RotateKey(ctx context.Context, newSigner protocol.Signer) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	if c.signer == nil || newSigner == nil {
		return nil, nil, fmt.Errorf("both current signer and newSigner are required for key.rotate")
	}
	msg := []byte("rotate|" + c.signer.AgentID() + "|" + newSigner.PublicKeyString())
	newSig, err := newSigner.Sign(msg)
	if err != nil {
		return nil, nil, err
	}
	rec, rej, err := c.SendEnvelope(ctx, protocol.TypeKeyRotate, protocol.KeyRotatePayload{
		OldPubKey: c.signer.PublicKeyString(),
		NewPubKey: newSigner.PublicKeyString(),
		NewSig:    newSig,
	})
	if err == nil && rej == nil && rec != nil {
		c.signer = newSigner
	}
	return rec, rej, err
}

// OrderOptions configures order persistence across settlement rounds and market-order pricing.
type OrderOptions struct {
	// Market submits a market order pegged to the round's price band ceiling (for buy) or floor (for sell).
	Market bool
	// CarryOver keeps an unfilled/partially-filled order active across future rounds until filled or cancelled ("사질때까지 유지").
	CarryOver bool
	// TTLRounds keeps an unfilled/partially-filled order active for up to N settlement rounds ("N회차까지 유지") before automatic expiration.
	TTLRounds uint64
}

// PlaceOrder submits a signed place_order envelope with default 1-round expiration (auto-cancel if unfilled at settlement).
func (c *Client) PlaceOrder(ctx context.Context, asset string, side protocol.Side, limitPrice, qty int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.PlaceOrderWithOptions(ctx, asset, side, limitPrice, qty, OrderOptions{})
}

// PlaceMarketOrder submits a market order (Market=true) that locks the round's maximum band price (for buy) or minimum band price (for sell),
// receives top price priority in clearing, and settles at the uniform clearing price with immediate refund of any excess escrow.
func (c *Client) PlaceMarketOrder(ctx context.Context, asset string, side protocol.Side, qty int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.PlaceOrderWithOptions(ctx, asset, side, 0, qty, OrderOptions{Market: true})
}

// PlaceOrderWithOptions submits a signed place_order envelope with custom market, carry-over, or N-round TTL options.
func (c *Client) PlaceOrderWithOptions(ctx context.Context, asset string, side protocol.Side, limitPrice, qty int64, opts OrderOptions) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypePlaceOrder, protocol.PlaceOrderPayload{
		Asset:      asset,
		Side:       side,
		LimitPrice: limitPrice,
		Qty:        qty,
		Market:     opts.Market,
		CarryOver:  opts.CarryOver,
		TTLRounds:  opts.TTLRounds,
	})
}

// CancelOrder submits a signed cancel_order envelope.
func (c *Client) CancelOrder(ctx context.Context, orderID string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypeCancelOrder, protocol.CancelOrderPayload{
		OrderID: orderID,
	})
}

// ReplaceOrder submits a signed replace_order envelope.
func (c *Client) ReplaceOrder(ctx context.Context, orderID string, limitPrice, qty int64) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypeReplaceOrder, protocol.ReplaceOrderPayload{
		OrderID:    orderID,
		LimitPrice: limitPrice,
		Qty:        qty,
	})
}

// PostBoard submits a signed board.post envelope.
func (c *Client) PostBoard(ctx context.Context, asset, stance, body, replyTo string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypeBoardPost, protocol.BoardPostPayload{
		Asset:   asset,
		Stance:  stance,
		Body:    body,
		ReplyTo: replyTo,
	})
}

// RetractBoard submits a signed board.retract envelope.
func (c *Client) RetractBoard(ctx context.Context, postID string) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	return c.SendEnvelope(ctx, protocol.TypeBoardRetract, protocol.BoardRetractPayload{
		PostID: postID,
	})
}

// SendEnvelope constructs, signs, and POSTs a protocol Envelope to /v1/envelope.
func (c *Client) SendEnvelope(ctx context.Context, msgType string, payload any) (*protocol.AcceptanceReceipt, *protocol.SignedRejection, error) {
	if c.signer == nil {
		return nil, nil, fmt.Errorf("client has no Signer configured for write operation %s", msgType)
	}
	if c.gameID == "" {
		if _, err := c.GetManifest(ctx); err != nil {
			return nil, nil, err
		}
	}
	rawP, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	env := protocol.Envelope{
		Version:   protocol.CurrentVersion,
		GameID:    c.gameID,
		From:      c.signer.AgentID(),
		To:        "server",
		Type:      msgType,
		Timestamp: time.Now().UTC().Unix(),
		Nonce:     c.nextID("n"),
		CorrID:    c.nextID("c"),
		Payload:   rawP,
	}
	if err := protocol.SignEnvelope(&env, c.signer); err != nil {
		return nil, nil, err
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/envelope", bytes.NewReader(envBytes))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode == http.StatusOK {
		var rec protocol.AcceptanceReceipt
		if err := json.Unmarshal(respBody, &rec); err != nil {
			return nil, nil, err
		}
		return &rec, nil, nil
	}

	var rej protocol.SignedRejection
	if err := json.Unmarshal(respBody, &rej); err == nil && rej.Code != "" {
		return nil, &rej, nil
	}
	return nil, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strconv.Quote(string(respBody)))
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s returned %d: %s", path, resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
