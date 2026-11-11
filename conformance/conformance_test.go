package conformance_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/galaxytemple/AgentSquare/conformance"
	aghttp "github.com/galaxytemple/AgentSquare/transport/http"
)

func TestGenerateAndRunAllConformanceVectors(t *testing.T) {
	ctx := context.Background()
	vecDir := filepath.Join("..", "spec", "test-vectors")

	if err := conformance.WriteVectorsToDir(vecDir); err != nil {
		t.Fatalf("WriteVectorsToDir failed: %v", err)
	}

	count, err := conformance.RunDirInProcess(ctx, vecDir)
	if err != nil {
		t.Fatalf("RunDirInProcess failed: %v", err)
	}
	if count < 10 {
		t.Fatalf("expected at least 10 test vectors, ran %d", count)
	}
	t.Logf("Successfully verified %d conformance test vectors in-process", count)
}

func TestConformanceOverHTTP(t *testing.T) {
	ctx := context.Background()
	vecs, err := conformance.BuildStandardVectors()
	if err != nil {
		t.Fatalf("BuildStandardVectors: %v", err)
	}

	for _, vec := range vecs {
		t.Run(vec.Name, func(t *testing.T) {
			eng, _, err := conformance.NewVectorEngine(ctx, vec)
			if err != nil {
				t.Fatalf("NewVectorEngine: %v", err)
			}
			srv := httptest.NewServer(aghttp.NewServer(eng, aghttp.WithSimTrigger(true)))
			defer srv.Close()

			if err := conformance.RunVectorOverHTTP(ctx, srv.URL, vec); err != nil {
				t.Fatalf("RunVectorOverHTTP failed: %v", err)
			}
		})
	}
}
