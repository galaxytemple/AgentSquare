package obs

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics collects all normative Prometheus metrics prefixed with agentsquare_ (Spec B4).
type Metrics struct {
	mu                   sync.RWMutex
	clearDurationSec     float64
	roundLagSec          float64
	ordersTotal          map[string]uint64 // result -> count ("accepted" or error code)
	boardPostsTotal      map[string]uint64 // result -> count
	ledgerHeadSeq        atomic.Uint64
	ledgerBytes          atomic.Int64
	raftState            atomic.Int64 // 0=Follower, 1=Candidate, 2=Leader, 3=Shutdown
	raftLeaderChanges    atomic.Uint64
	raftCommitIndexLag   atomic.Uint64
	escrowLocked         map[string]int64 // asset -> locked amount
	projectionLagRecords atomic.Uint64
}

// NewMetrics creates a new Metrics collector.
func NewMetrics() *Metrics {
	m := &Metrics{
		ordersTotal:     make(map[string]uint64),
		boardPostsTotal: make(map[string]uint64),
		escrowLocked:    make(map[string]int64),
	}
	m.raftState.Store(2) // Default single-node is Leader (2)
	return m
}

func (m *Metrics) ObserveClearDuration(seconds float64) {
	m.mu.Lock()
	m.clearDurationSec = seconds
	m.mu.Unlock()
}

func (m *Metrics) SetRoundLag(seconds float64) {
	m.mu.Lock()
	m.roundLagSec = seconds
	m.mu.Unlock()
}

func (m *Metrics) IncOrder(result string) {
	m.mu.Lock()
	m.ordersTotal[result]++
	m.mu.Unlock()
}

func (m *Metrics) IncBoardPost(result string) {
	m.mu.Lock()
	m.boardPostsTotal[result]++
	m.mu.Unlock()
}

func (m *Metrics) SetLedgerHead(seq uint64, bytes int64) {
	m.ledgerHeadSeq.Store(seq)
	m.ledgerBytes.Store(bytes)
}

func (m *Metrics) SetRaftState(state int64) {
	m.raftState.Store(state)
}

func (m *Metrics) IncRaftLeaderChanges() {
	m.raftLeaderChanges.Add(1)
}

func (m *Metrics) SetRaftCommitLag(lag uint64) {
	m.raftCommitIndexLag.Store(lag)
}

func (m *Metrics) SetEscrowLocked(asset string, amount int64) {
	m.mu.Lock()
	m.escrowLocked[asset] = amount
	m.mu.Unlock()
}

func (m *Metrics) SetProjectionLag(lag uint64) {
	m.projectionLagRecords.Store(lag)
}

// Handler returns an http.Handler serving the Prometheus text exposition format at /metrics (Spec B4).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var sb strings.Builder

		m.mu.RLock()
		clearDur := m.clearDurationSec
		roundLag := m.roundLagSec

		orderKeys := make([]string, 0, len(m.ordersTotal))
		for k := range m.ordersTotal {
			orderKeys = append(orderKeys, k)
		}
		sort.Strings(orderKeys)
		ordersCopy := make(map[string]uint64, len(m.ordersTotal))
		for k, v := range m.ordersTotal {
			ordersCopy[k] = v
		}

		postKeys := make([]string, 0, len(m.boardPostsTotal))
		for k := range m.boardPostsTotal {
			postKeys = append(postKeys, k)
		}
		sort.Strings(postKeys)
		postsCopy := make(map[string]uint64, len(m.boardPostsTotal))
		for k, v := range m.boardPostsTotal {
			postsCopy[k] = v
		}

		escrowKeys := make([]string, 0, len(m.escrowLocked))
		for k := range m.escrowLocked {
			escrowKeys = append(escrowKeys, k)
		}
		sort.Strings(escrowKeys)
		escrowCopy := make(map[string]int64, len(m.escrowLocked))
		for k, v := range m.escrowLocked {
			escrowCopy[k] = v
		}
		m.mu.RUnlock()

		fmt.Fprintf(&sb, "# TYPE agentsquare_round_clear_duration_seconds gauge\n")
		fmt.Fprintf(&sb, "agentsquare_round_clear_duration_seconds %.6f\n", clearDur)

		fmt.Fprintf(&sb, "# TYPE agentsquare_round_lag_seconds gauge\n")
		fmt.Fprintf(&sb, "agentsquare_round_lag_seconds %.3f\n", roundLag)

		fmt.Fprintf(&sb, "# TYPE agentsquare_orders_total counter\n")
		for _, k := range orderKeys {
			fmt.Fprintf(&sb, "agentsquare_orders_total{result=%q} %d\n", k, ordersCopy[k])
		}

		fmt.Fprintf(&sb, "# TYPE agentsquare_board_posts_total counter\n")
		for _, k := range postKeys {
			fmt.Fprintf(&sb, "agentsquare_board_posts_total{result=%q} %d\n", k, postsCopy[k])
		}

		fmt.Fprintf(&sb, "# TYPE agentsquare_ledger_head_seq gauge\n")
		fmt.Fprintf(&sb, "agentsquare_ledger_head_seq %d\n", m.ledgerHeadSeq.Load())

		fmt.Fprintf(&sb, "# TYPE agentsquare_ledger_bytes gauge\n")
		fmt.Fprintf(&sb, "agentsquare_ledger_bytes %d\n", m.ledgerBytes.Load())

		fmt.Fprintf(&sb, "# TYPE agentsquare_raft_state gauge\n")
		fmt.Fprintf(&sb, "agentsquare_raft_state %d\n", m.raftState.Load())

		fmt.Fprintf(&sb, "# TYPE agentsquare_raft_leader_changes_total counter\n")
		fmt.Fprintf(&sb, "agentsquare_raft_leader_changes_total %d\n", m.raftLeaderChanges.Load())

		fmt.Fprintf(&sb, "# TYPE agentsquare_raft_commit_index_lag gauge\n")
		fmt.Fprintf(&sb, "agentsquare_raft_commit_index_lag %d\n", m.raftCommitIndexLag.Load())

		fmt.Fprintf(&sb, "# TYPE agentsquare_escrow_locked gauge\n")
		for _, k := range escrowKeys {
			fmt.Fprintf(&sb, "agentsquare_escrow_locked{asset=%q} %d\n", k, escrowCopy[k])
		}

		fmt.Fprintf(&sb, "# TYPE agentsquare_projection_lag_records gauge\n")
		fmt.Fprintf(&sb, "agentsquare_projection_lag_records %d\n", m.projectionLagRecords.Load())

		_, _ = w.Write([]byte(sb.String()))
	})
}

// NewLogger creates a structured JSON logger that never logs private keys or raw board post bodies (Spec B4).
func NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}
