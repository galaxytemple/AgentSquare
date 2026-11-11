package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/conformance"
	"github.com/galaxytemple/AgentSquare/protocol"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

func TestCLISubcommands(t *testing.T) {
	// 1. keygen & usage
	printUsage()
	if err := cmdKeygen(nil); err != nil {
		t.Fatalf("cmdKeygen failed: %v", err)
	}

	// 2. conformance (in-process)
	if err := cmdConformance([]string{"--dir", "../../spec/test-vectors"}); err != nil {
		t.Fatalf("cmdConformance failed: %v", err)
	}

	// 3. Spin up an HTTP test server and test admin CLI subcommands (halt, resume, pause-asset, hide-post, manifest-update, reset, rotate-key)
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}
	v := vecs[0]
	v.ClockUnix = time.Now().UTC().Unix()
	eng, srvSigner, err := conformance.NewVectorEngine(ctx, v)
	if err != nil {
		t.Fatalf("NewVectorEngine: %v", err)
	}
	srv := httptest.NewServer(aghttp.NewServer(eng))
	defer srv.Close()

	seedHex := srvSigner.(*protocol.MemorySigner).SeedHex()
	for _, tc := range [][]string{
		{"pause-asset", "--url", srv.URL, "--key", seedHex, "--asset", "copper", "--reason", "check"},
		{"halt", "--url", srv.URL, "--key", seedHex, "--reason", "maint"},
		{"resume", "--url", srv.URL, "--key", seedHex, "--reason", "done"},
		{"hide-post", "--url", srv.URL, "--key", seedHex, "--post-id", "post_1_1", "--reason", "spam"},
		{"manifest-update", "--url", srv.URL, "--key", seedHex, "--new-hash", "sha256:0000000000000000000000000000000000000000000000000000000000000001", "--effective-round", "10"},
		{"reset", "--url", srv.URL, "--key", seedHex, "--season", "2026-W47", "--reason", "weekly"},
	} {
		if err := cmdAdmin(tc); err != nil {
			t.Fatalf("cmdAdmin(%v) failed: %v", tc[0], err)
		}
	}

	// Admin rotate-key via CLI, followed by reset signed by the newly rotated admin key
	newAdmin, _ := protocol.GenerateKeyPair(nil)
	if err := cmdAdmin([]string{"rotate-key", "--url", srv.URL, "--key", seedHex, "--new-key", newAdmin.SeedHex()}); err != nil {
		t.Fatalf("cmdAdmin(rotate-key) failed: %v", err)
	}
	if err := cmdAdmin([]string{"reset", "--url", srv.URL, "--key", newAdmin.SeedHex(), "--season", "2026-W48", "--reason", "post-rotation weekly reset"}); err != nil {
		t.Fatalf("cmdAdmin(reset with rotated key) failed: %v", err)
	}

	// 4. Write ledger & genesis manifest to temp dir and test verify, doctor, snapshot, and projection rebuild
	tmpDir := t.TempDir()
	ledgerFile := filepath.Join(tmpDir, "ledger.jsonl")
	manifestFile := filepath.Join(tmpDir, "manifest.json")

	genesisManifest := v.Manifest
	genesisManifest.Ledger.GenesisHash = eng.Manifest().Ledger.GenesisHash
	mBytes, _ := json.Marshal(genesisManifest)
	_ = os.WriteFile(manifestFile, mBytes, 0600)

	lf, err := os.Create(ledgerFile)
	if err != nil {
		t.Fatalf("Create ledgerFile: %v", err)
	}
	for rec, rErr := range eng.Log().Read(ctx, 1) {
		if rErr != nil {
			t.Fatalf("Read log: %v", rErr)
		}
		b, _ := json.Marshal(rec)
		_, _ = lf.Write(append(b, '\n'))
	}
	_ = lf.Close()

	if err := cmdVerify([]string{"--manifest", manifestFile, ledgerFile}, false); err != nil {
		t.Fatalf("cmdVerify failed: %v", err)
	}
	if err := cmdVerify([]string{"--manifest", manifestFile, ledgerFile}, true); err != nil {
		t.Fatalf("cmdVerify(doctor) failed: %v", err)
	}
	if err := cmdSnapshot([]string{ledgerFile}); err != nil {
		t.Fatalf("cmdSnapshot failed: %v", err)
	}
	if err := cmdProjection([]string{"rebuild", "--from-seq", "0", ledgerFile}); err != nil {
		t.Fatalf("cmdProjection failed: %v", err)
	}
}
