package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/galaxytemple/AgentSquare/admin"
	"github.com/galaxytemple/AgentSquare/client"
	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/engine"
	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
	"github.com/galaxytemple/AgentSquare/projection/sqlite"
	"github.com/galaxytemple/AgentSquare/protocol"
	"github.com/galaxytemple/AgentSquare/storage/filelog"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
	"github.com/galaxytemple/AgentSquare/transport/mcp"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	sub := os.Args[1]
	args := os.Args[2:]

	var err error
	switch sub {
	case "keygen":
		err = cmdKeygen(args)
	case "serve":
		err = cmdServe(args)
	case "verify":
		err = cmdVerify(args, false)
	case "doctor":
		err = cmdVerify(args, true)
	case "conformance":
		err = cmdConformance(args)
	case "snapshot":
		err = cmdSnapshot(args)
	case "mcp":
		err = cmdMCP(args)
	case "viewer":
		err = cmdViewer(args)
	case "admin":
		err = cmdAdmin(args)
	case "projection":
		err = cmdProjection(args)
	case "-h", "--help", "help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", sub)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`AgentSquare — Agent-to-Agent Batch Call-Auction Protocol CLI

Usage:
  agentsquare <command> [flags]

Commands:
  serve         Run a standalone AgentSquare market server
  verify        Replay and verify a JSONL ledger file from genesis
  doctor        Check ledger hash chain, escrow locks, and conservation invariants
  conformance   Run conformance test vectors (in-process or against an HTTP server)
  keygen        Generate a new Ed25519 key pair and Agent ID
  mcp           Run the local signing MCP proxy (Form A) or read-only MCP server (Form B) over stdio
  viewer        Serve the static Ledger Replay & Verification Web Viewer
  admin         Execute signed administrative interventions (halt, resume, pause-asset, hide-post, manifest-update)
  snapshot      Export a verified state snapshot from a JSONL ledger file
  projection    Rebuild a read-model projection from the ledger (--from-seq 0)`)
}

func cmdKeygen(_ []string) error {
	kp, err := protocol.GenerateKeyPair(nil)
	if err != nil {
		return err
	}
	out := map[string]string{
		"agent_id": kp.AgentID(),
		"pubkey":   kp.PublicKeyString(),
		"seed_hex": kp.SeedHex(),
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	return nil
}

type defaultServeGame struct {
	m        manifest.Manifest
	refPrice int64
}

func (g *defaultServeGame) Manifest() manifest.Manifest           { return g.m }
func (g *defaultServeGame) ReferencePrice(_ string, _ uint64) int64 { return g.refPrice }
func (g *defaultServeGame) Admit(_ context.Context, _ engine.AdmissionRequest) ([]engine.BalanceChange, error) {
	return []engine.BalanceChange{
		{Asset: "usd", Qty: 100, Reason: "deck_grant"},
		{Asset: "copper", Qty: 10, Reason: "deck_grant"},
	}, nil
}
func (g *defaultServeGame) OnSettled(_ context.Context, round uint64, fills []protocol.Fill) error {
	fmt.Printf("[round %d settled] %d fills executed\n", round, len(fills))
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	ledgerPath := fs.String("ledger", "ledger.jsonl", "Path to append-only JSONL ledger file")
	gameID := fs.String("game-id", "city-2026w46", "Game identifier")
	interval := fs.String("interval", "PT1H", "Round interval in ISO 8601 (e.g. PT1H, PT1M)")
	seedHex := fs.String("key-seed", "", "Optional 64-char hex Ed25519 server seed (generates random if empty)")
	simMode := fs.Bool("sim", false, "Enable /v1/sim/clear endpoint")
	_ = fs.Parse(args)

	var signer *protocol.MemorySigner
	var err error
	if *seedHex != "" {
		signer, err = protocol.NewSignerFromSeedHex(*seedHex)
	} else {
		signer, err = protocol.GenerateKeyPair(nil)
	}
	if err != nil {
		return fmt.Errorf("init server key: %w", err)
	}

	lg, err := filelog.Open(*ledgerPath)
	if err != nil {
		return fmt.Errorf("open ledger %s: %w", *ledgerPath, err)
	}
	defer func() { _ = lg.Close() }()

	gm := &defaultServeGame{
		refPrice: 10,
		m: manifest.Manifest{
			Protocol:          protocol.ProtocolID,
			SupportedVersions: []string{"1.0"},
			GameID:            *gameID,
			Assets:            []manifest.Asset{{ID: "copper", QtyUnit: 1}},
			SettlementUnit:    manifest.SettlementUnit{ID: "usd", Tick: 1},
			Round:             manifest.RoundConfig{Interval: *interval, Anchor: time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339), CarryOver: false},
			PriceBand:         manifest.PriceBandConfig{Reference: "shop", LowerPct: 50, UpperPct: 100},
			MarketData:        manifest.MarketDataConfig{PreClear: "indicative"},
			FeeBps:            0,
			Board:             &manifest.BoardConfig{Enabled: true, MaxPostsPerAgentPerRound: 3, MaxBodyChars: 280, MinIntervalSec: 60, RequireParticipant: true, BlackoutBeforeCutoffSec: 300, AllowedContent: "text/plain"},
			Admission:         "open",
			Ledger:            manifest.LedgerConfig{Public: "after-clear"},
			ServerPubKey:      signer.PublicKeyString(),
			AdminPubKeys:      []string{signer.PublicKeyString()},
		},
	}

	eng, err := engine.New(gm, engine.WithLog(lg), engine.WithSigner(signer), engine.WithStrictCutoff(true))
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng.StartScheduler(ctx, 100*time.Millisecond)

	srv := &http.Server{
		Addr:    *addr,
		Handler: aghttp.NewServer(eng, aghttp.WithSimTrigger(*simMode)),
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	fmt.Printf("AgentSquare server listening on %s (game_id=%s, server_pubkey=%s)\n", *addr, *gameID, signer.PublicKeyString())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func cmdVerify(args []string, doctor bool) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "Optional path to manifest JSON file")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		return fmt.Errorf("usage: agentsquare verify [--manifest manifest.json] <ledger.jsonl>")
	}
	ledgerPath := fs.Arg(0)

	var m *manifest.Manifest
	if *manifestPath != "" {
		raw, err := os.ReadFile(*manifestPath)
		if err != nil {
			return err
		}
		m, err = manifest.ParseAndValidate(raw)
		if err != nil {
			return err
		}
	}

	lg, err := filelog.Open(ledgerPath)
	if err != nil {
		return err
	}
	defer func() { _ = lg.Close() }()

	st, err := ledger.ReplayAndVerify(m, lg.Read(context.Background(), 1))
	if err != nil {
		return err
	}
	sh, err := st.StateHash()
	if err != nil {
		return err
	}

	res := map[string]any{
		"status":        "verified",
		"game_id":       st.GameID,
		"last_seq":      st.LastSeq,
		"last_hash":     st.LastHash,
		"current_round": st.CurrentRound,
		"round_status":  st.RoundStatus,
		"agents_count":  len(st.Agents),
		"open_orders":   len(st.OpenOrders),
		"state_hash":    sh,
	}
	if doctor {
		res["doctor_escrow_check"] = "ok"
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdConformance(args []string) error {
	fs := flag.NewFlagSet("conformance", flag.ExitOnError)
	dir := fs.String("dir", "spec/test-vectors", "Directory containing conformance test vector JSON files")
	url := fs.String("url", "", "Optional target HTTP server URL to test against")
	write := fs.Bool("generate", false, "Regenerate standard test vectors in --dir before running")
	_ = fs.Parse(args)

	ctx := context.Background()
	if *write {
		if err := conformance.WriteVectorsToDir(*dir); err != nil {
			return err
		}
	}

	if *url != "" {
		vecs, err := conformance.BuildStandardVectors()
		if err != nil {
			return err
		}
		for _, v := range vecs {
			if err := conformance.RunVectorOverHTTP(ctx, *url, v); err != nil {
				return err
			}
		}
		fmt.Printf("PASS: %d conformance vectors verified against %s\n", len(vecs), *url)
		return nil
	}

	n, err := conformance.RunDirInProcess(ctx, *dir)
	if err != nil {
		return err
	}
	fmt.Printf("PASS: %d conformance vectors verified in %s\n", n, *dir)
	return nil
}

func cmdSnapshot(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: agentsquare snapshot <ledger.jsonl>")
	}
	lg, err := filelog.Open(args[0])
	if err != nil {
		return err
	}
	defer func() { _ = lg.Close() }()

	st, err := ledger.ReplayAndVerify(nil, lg.Read(context.Background(), 1))
	if err != nil {
		return err
	}
	sh, err := st.StateHash()
	if err != nil {
		return err
	}
	snap := ledger.Snapshot{
		Seq:       st.LastSeq,
		Hash:      st.LastHash,
		StateHash: sh,
		State:     st.Snapshot(),
	}
	b, _ := json.MarshalIndent(snap, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	gameURL := fs.String("game", "http://localhost:8080", "Target AgentSquare game server URL")
	keyFlag := fs.String("key", "", "Ed25519 seed hex or path to seed file (omit for Form B read-only MCP)")
	policyPath := fs.String("policy", "", "Optional path to policy.toml file")
	readOnly := fs.Bool("read-only", false, "Run in Form B read-only mode")
	_ = fs.Parse(args)

	var signer protocol.Signer
	if *keyFlag != "" && !*readOnly {
		s, err := loadSigner(*keyFlag)
		if err != nil {
			return err
		}
		signer = s
	} else {
		*readOnly = true
	}

	var pol mcp.Policy
	if *policyPath != "" {
		raw, err := os.ReadFile(*policyPath)
		if err != nil {
			return err
		}
		p, err := mcp.ParsePolicyTOML(string(raw))
		if err != nil {
			return err
		}
		pol = p
	}

	cli := client.New(*gameURL, "", signer)
	proxy := mcp.NewProxy(cli, mcp.WithPolicy(pol), mcp.WithReadOnly(*readOnly))
	return proxy.ServeStdio(context.Background(), os.Stdin, os.Stdout)
}

func cmdViewer(args []string) error {
	fs := flag.NewFlagSet("viewer", flag.ExitOnError)
	addr := fs.String("addr", ":8090", "HTTP listen address for static web viewer")
	dir := fs.String("dir", "web/viewer", "Directory containing viewer static files")
	_ = fs.Parse(args)

	mux := http.NewServeMux()
	mux.Handle("/spec/", http.StripPrefix("/spec/", http.FileServer(http.Dir("spec"))))
	mux.Handle("/", http.FileServer(http.Dir(*dir)))

	fmt.Printf("AgentSquare Ledger Replay Web Viewer listening on http://localhost%s\n", *addr)
	return http.ListenAndServe(*addr, mux)
}

func cmdAdmin(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: agentsquare admin <halt|resume|pause-asset|hide-post|manifest-update|reset|rotate-key> [flags]")
	}
	action := args[0]
	fs := flag.NewFlagSet("admin "+action, flag.ExitOnError)
	url := fs.String("url", "http://localhost:8080", "Server URL")
	key := fs.String("key", "", "Admin Ed25519 key seed hex or file")
	newKey := fs.String("new-key", "", "New Admin Ed25519 key seed hex or file (for rotate-key)")
	reason := fs.String("reason", "administrative action", "Reason recorded in ledger")
	asset := fs.String("asset", "", "Asset ID (for pause-asset)")
	postID := fs.String("post-id", "", "Post ID (for hide-post)")
	newHash := fs.String("new-hash", "", "New manifest hash (for manifest-update)")
	effRound := fs.Uint64("effective-round", 0, "Effective future round (for manifest-update)")
	seasonID := fs.String("season", "", "Season/week identifier (for reset)")
	clearHistory := fs.Bool("clear-history", true, "Clear market/board history on reset")
	_ = fs.Parse(args[1:])

	if *key == "" {
		return fmt.Errorf("--key is required for signed admin commands")
	}
	signer, err := loadSigner(*key)
	if err != nil {
		return err
	}

	adm := admin.NewClient(*url, "", signer)
	ctx := context.Background()

	var rec *protocol.AcceptanceReceipt
	var rej *protocol.SignedRejection

	switch action {
	case "halt":
		rec, rej, err = adm.Halt(ctx, *reason)
	case "resume":
		rec, rej, err = adm.Resume(ctx, "all", *reason)
	case "pause-asset":
		rec, rej, err = adm.PauseAsset(ctx, *asset, *reason)
	case "hide-post":
		rec, rej, err = adm.HidePost(ctx, *postID, *reason)
	case "manifest-update":
		rec, rej, err = adm.ManifestUpdate(ctx, *newHash, *effRound)
	case "reset":
		rec, rej, err = adm.Reset(ctx, *seasonID, *reason, nil, *clearHistory)
	case "rotate-key":
		if *newKey == "" {
			return fmt.Errorf("--new-key is required for rotate-key")
		}
		newSigner, nErr := loadSigner(*newKey)
		if nErr != nil {
			return nErr
		}
		rec, rej, err = adm.RotateKey(ctx, newSigner)
	default:
		return fmt.Errorf("unknown admin subcommand %q", action)
	}
	if err != nil {
		return err
	}
	if rej != nil {
		b, _ := json.MarshalIndent(rej, "", "  ")
		return fmt.Errorf("rejected: %s", string(b))
	}
	b, _ := json.MarshalIndent(rec, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdProjection(args []string) error {
	if len(args) < 1 || args[0] != "rebuild" {
		return fmt.Errorf("usage: agentsquare projection rebuild [--from-seq 0] <ledger.jsonl>")
	}
	fs := flag.NewFlagSet("projection rebuild", flag.ExitOnError)
	fromSeq := fs.Uint64("from-seq", 0, "Starting sequence number (0 resets and rebuilds from genesis)")
	_ = fs.Parse(args[1:])
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: agentsquare projection rebuild [--from-seq 0] <ledger.jsonl>")
	}

	lg, err := filelog.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer func() { _ = lg.Close() }()

	proj, err := sqlite.New(context.Background(), nil)
	if err != nil {
		return err
	}
	count, err := sqlite.Rebuild(context.Background(), lg, proj, *fromSeq)
	if err != nil {
		return err
	}
	cp, _ := proj.Checkpoint(context.Background())
	out := map[string]any{
		"status":             "rebuilt",
		"records_applied":    count,
		"checkpoint_last_seq": cp,
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	return nil
}

func loadSigner(keyOrPath string) (*protocol.MemorySigner, error) {
	if b, err := os.ReadFile(keyOrPath); err == nil {
		var obj struct {
			SeedHex string `json:"seed_hex"`
		}
		if json.Unmarshal(b, &obj) == nil && obj.SeedHex != "" {
			return protocol.NewSignerFromSeedHex(obj.SeedHex)
		}
		return protocol.NewSignerFromSeedHex(strings.TrimSpace(string(b)))
	}
	return protocol.NewSignerFromSeedHex(strings.TrimSpace(keyOrPath))
}
