package obs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/galaxytemple/AgentSquare/obs"
)

func TestPrometheusMetricsHandler(t *testing.T) {
	m := obs.NewMetrics()
	m.ObserveClearDuration(0.0042)
	m.SetRoundLag(1.5)
	m.IncOrder("accepted")
	m.IncOrder("OUT_OF_BAND")
	m.IncBoardPost("accepted")
	m.SetLedgerHead(42, 16384)
	m.SetRaftState(2)
	m.IncRaftLeaderChanges()
	m.SetRaftCommitLag(0)
	m.SetEscrowLocked("usd", 450)
	m.SetProjectionLag(2)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	expectedMetrics := []string{
		"agentsquare_round_clear_duration_seconds 0.004200",
		"agentsquare_round_lag_seconds 1.500",
		`agentsquare_orders_total{result="accepted"} 1`,
		`agentsquare_orders_total{result="OUT_OF_BAND"} 1`,
		`agentsquare_board_posts_total{result="accepted"} 1`,
		"agentsquare_ledger_head_seq 42",
		"agentsquare_ledger_bytes 16384",
		"agentsquare_raft_state 2",
		"agentsquare_raft_leader_changes_total 1",
		`agentsquare_escrow_locked{asset="usd"} 450`,
		"agentsquare_projection_lag_records 2",
	}
	for _, exp := range expectedMetrics {
		if !strings.Contains(body, exp) {
			t.Fatalf("missing metric %q in:\n%s", exp, body)
		}
	}

	if l := obs.NewLogger(); l == nil {
		t.Fatalf("expected non-nil slog.Logger")
	}
}
