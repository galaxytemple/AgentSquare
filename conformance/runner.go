package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/sim"
	"github.com/galaxytemple/AgentSquare/storage/memlog"
)

// VectorInput represents a single step in a conformance test vector.
type VectorInput struct {
	Action           string             `json:"action"`                      // "envelope" | "clear_round"
	Envelope         json.RawMessage    `json:"envelope,omitempty"`          // Signed raw JSON envelope
	ExpectAccepted   bool               `json:"expect_accepted,omitempty"`   // True if envelope should be accepted
	ExpectErrorCode  protocol.ErrorCode `json:"expect_error_code,omitempty"` // Expected rejection code if !ExpectAccepted
	ExpectIdempotent bool               `json:"expect_idempotent,omitempty"` // True if resend of previous (from, nonce)
}

// TestVector is the language-neutral conformance test vector format (Spec A10).
type TestVector struct {
	Name                string               `json:"name"`
	Description         string               `json:"description"`
	ServerSeedHex       string               `json:"server_seed_hex"`
	ClockUnix           int64                `json:"clock_unix"`
	ReferencePrices     map[string]int64     `json:"reference_prices"`
	InitialBalances     map[string]map[string]int64 `json:"initial_balances,omitempty"`
	Manifest            manifest.Manifest    `json:"manifest"`
	LedgerBefore        []ledger.Record      `json:"ledger_before"`
	Inputs              []VectorInput        `json:"inputs"`
	ExpectedFills       []protocol.Fill      `json:"expected_fills"`
	ExpectedLedgerAfter []ledger.Record      `json:"expected_ledger_after"`
	ExpectedStateHash   string               `json:"expected_state_hash"`
}

type vectorGame struct {
	manifest        manifest.Manifest
	refPrices       map[string]int64
	initialBalances map[string]map[string]int64
}

func (g *vectorGame) Manifest() manifest.Manifest { return g.manifest }
func (g *vectorGame) ReferencePrice(asset string, _ uint64) int64 {
	if p, ok := g.refPrices[asset]; ok {
		return p
	}
	return 10
}
func (g *vectorGame) Admit(_ context.Context, req engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	var out []engine.BalanceChange
	if g.initialBalances != nil {
		if bals, ok := g.initialBalances[req.AgentID]; ok {
			keys := make([]string, 0, len(bals))
			for k := range bals {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				out = append(out, engine.BalanceChange{
					Asset:  k,
					Qty:    bals[k],
					Reason: "deck_grant",
				})
			}
			return out, nil
		}
	}
	return []engine.BalanceChange{
		{Asset: g.manifest.SettlementUnit.ID, Qty: 100, Reason: "deck_grant"},
	}, nil
}
func (g *vectorGame) OnSettled(_ context.Context, _ uint64, _ []protocol.Fill) error {
	return nil
}

// NewVectorEngine creates an initialized Engine configured for a specific TestVector.
func NewVectorEngine(ctx context.Context, vec TestVector) (*engine.Engine, protocol.Signer, error) {
	signer, err := protocol.NewSignerFromSeedHex(vec.ServerSeedHex)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: invalid server_seed_hex: %w", vec.Name, err)
	}

	lg := memlog.New()
	if len(vec.LedgerBefore) > 0 {
		if _, err := lg.Append(ctx, vec.LedgerBefore...); err != nil {
			return nil, nil, fmt.Errorf("%s: preload ledger_before failed: %w", vec.Name, err)
		}
	}

	clk := sim.NewFakeClock(time.Unix(vec.ClockUnix, 0).UTC())
	gm := &vectorGame{
		manifest:        vec.Manifest,
		refPrices:       vec.ReferencePrices,
		initialBalances: vec.InitialBalances,
	}

	eng, err := engine.New(gm, engine.WithLog(lg), engine.WithSigner(signer), engine.WithClock(clk))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: engine.New failed: %w", vec.Name, err)
	}
	return eng, signer, nil
}

// RunVectorInProcess executes a single TestVector against the in-process Go Engine and verifies all expectations.
func RunVectorInProcess(ctx context.Context, vec TestVector) error {
	eng, signer, err := NewVectorEngine(ctx, vec)
	if err != nil {
		return err
	}

	var allFills []protocol.Fill
	var lastReceiptSig string

	for i, in := range vec.Inputs {
		switch in.Action {
		case "envelope":
			rec, rej := eng.SubmitRawEnvelope(ctx, in.Envelope)
			if in.ExpectAccepted {
				if rej != nil {
					return fmt.Errorf("%s: input[%d] expected acceptance, got rejection %s (%s)", vec.Name, i, rej.Code, rej.Reason)
				}
				if err := protocol.VerifyReceipt(rec, signer.PublicKeyString()); err != nil {
					return fmt.Errorf("%s: input[%d] invalid server receipt signature: %w", vec.Name, i, err)
				}
				if in.ExpectIdempotent && lastReceiptSig != "" && rec.ServerSig != lastReceiptSig {
					return fmt.Errorf("%s: input[%d] idempotent resend returned different receipt sig", vec.Name, i)
				}
				lastReceiptSig = rec.ServerSig
			} else {
				if rej == nil {
					return fmt.Errorf("%s: input[%d] expected rejection %s, but was accepted", vec.Name, i, in.ExpectErrorCode)
				}
				if rej.Code != in.ExpectErrorCode {
					return fmt.Errorf("%s: input[%d] got error code %s, want %s (reason: %s)", vec.Name, i, rej.Code, in.ExpectErrorCode, rej.Reason)
				}
				if err := protocol.VerifyRejection(rej, signer.PublicKeyString()); err != nil {
					return fmt.Errorf("%s: input[%d] invalid server rejection signature: %w", vec.Name, i, err)
				}
			}
		case "clear_round":
			fills, err := eng.SealAndClearRound(ctx)
			if err != nil {
				return fmt.Errorf("%s: input[%d] clear_round failed: %w", vec.Name, i, err)
			}
			allFills = append(allFills, fills...)
		default:
			return fmt.Errorf("%s: input[%d] unknown action %q", vec.Name, i, in.Action)
		}
	}

	if allFills == nil {
		allFills = []protocol.Fill{}
	}
	expectedFills := vec.ExpectedFills
	if expectedFills == nil {
		expectedFills = []protocol.Fill{}
	}
	if len(allFills) != len(expectedFills) {
		return fmt.Errorf("%s: fill count mismatch: got %d (%+v), want %d (%+v)", vec.Name, len(allFills), allFills, len(expectedFills), expectedFills)
	}
	for i := range expectedFills {
		if allFills[i] != expectedFills[i] {
			return fmt.Errorf("%s: fill[%d] mismatch: got %+v, want %+v", vec.Name, i, allFills[i], expectedFills[i])
		}
	}

	// Verify full ledger replay from genesis reproduces ExpectedStateHash
	replayed, err := ledger.ReplayAndVerify(&vec.Manifest, eng.Log().Read(ctx, 1))
	if err != nil {
		return fmt.Errorf("%s: ledger ReplayAndVerify failed: %w", vec.Name, err)
	}
	gotHash, err := replayed.StateHash()
	if err != nil {
		return err
	}
	if gotHash != vec.ExpectedStateHash {
		return fmt.Errorf("%s: state_hash mismatch: got %s, want %s", vec.Name, gotHash, vec.ExpectedStateHash)
	}

	var gotRecords []ledger.Record
	for r, err := range eng.Log().Read(ctx, 1) {
		if err != nil {
			return err
		}
		gotRecords = append(gotRecords, r)
	}
	if len(gotRecords) != len(vec.ExpectedLedgerAfter) {
		return fmt.Errorf("%s: ledger record count mismatch: got %d, want %d", vec.Name, len(gotRecords), len(vec.ExpectedLedgerAfter))
	}
	for i := range gotRecords {
		if gotRecords[i].Hash != vec.ExpectedLedgerAfter[i].Hash {
			return fmt.Errorf("%s: ledger record[%d] hash mismatch: got %s, want %s",
				vec.Name, i, gotRecords[i].Hash, vec.ExpectedLedgerAfter[i].Hash)
		}
	}

	return nil
}

// RunDirInProcess loads and executes all *.json test vectors in dir.
func RunDirInProcess(ctx context.Context, dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			return count, err
		}
		var vec TestVector
		if err := json.Unmarshal(raw, &vec); err != nil {
			return count, fmt.Errorf("parse %s: %w", e.Name(), err)
		}
		if err := RunVectorInProcess(ctx, vec); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// RunVectorOverHTTP executes a TestVector against a running HTTP server endpoint (Spec C3).
func RunVectorOverHTTP(ctx context.Context, baseURL string, vec TestVector) error {
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL = strings.TrimRight(baseURL, "/")

	// 1. Check manifest endpoint
	resp, err := client.Get(baseURL + manifest.WellKnownPath)
	if err != nil {
		return fmt.Errorf("GET manifest failed: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET manifest status = %d", resp.StatusCode)
	}

	// 2. Execute inputs over HTTP
	var allFills []protocol.Fill
	for i, in := range vec.Inputs {
		switch in.Action {
		case "envelope":
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/envelope", bytes.NewReader(in.Envelope))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			r, err := client.Do(req)
			if err != nil {
				return err
			}
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()

			if in.ExpectAccepted {
				if r.StatusCode != http.StatusOK {
					return fmt.Errorf("%s http input[%d]: expected 200 OK, got %d: %s", vec.Name, i, r.StatusCode, string(body))
				}
			} else {
				if r.StatusCode == http.StatusOK {
					return fmt.Errorf("%s http input[%d]: expected rejection %s, got 200 OK", vec.Name, i, in.ExpectErrorCode)
				}
				var rej protocol.SignedRejection
				if err := json.Unmarshal(body, &rej); err != nil {
					return fmt.Errorf("%s http input[%d]: unmarshal rejection: %w", vec.Name, i, err)
				}
				if rej.Code != in.ExpectErrorCode {
					return fmt.Errorf("%s http input[%d]: got code %s, want %s", vec.Name, i, rej.Code, in.ExpectErrorCode)
				}
			}
		case "clear_round":
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/sim/clear", nil)
			if err != nil {
				return err
			}
			r, err := client.Do(req)
			if err != nil {
				return err
			}
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if r.StatusCode != http.StatusOK {
				return fmt.Errorf("%s http clear_round[%d]: status %d: %s", vec.Name, i, r.StatusCode, string(body))
			}
			var out struct {
				Fills []protocol.Fill `json:"fills"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				return err
			}
			allFills = append(allFills, out.Fills...)
		}
	}

	if len(allFills) != len(vec.ExpectedFills) {
		return fmt.Errorf("%s http: fill count got %d, want %d", vec.Name, len(allFills), len(vec.ExpectedFills))
	}
	return nil
}
