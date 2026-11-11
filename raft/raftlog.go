package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/galaxytemple/AgentSquare/ledger"
)

// Role represents the Raft consensus state of a node.
type Role int64

const (
	RoleFollower  Role = 0
	RoleCandidate Role = 1
	RoleLeader    Role = 2
	RoleShutdown  Role = 3
)

var (
	ErrNotLeader   = errors.New("raft: node is not the current leader")
	ErrNoQuorum    = errors.New("raft: cluster quorum unavailable (network partition)")
	ErrNodeStopped = errors.New("raft: node is shut down")
)

// Transport abstracts network communication between Raft nodes (supports both TCP/HTTP and deterministic simulation networks).
type Transport interface {
	Replicate(ctx context.Context, fromID, toID string, term uint64, recs []ledger.Record) error
}

// Node implements ledger.Log backed by Raft consensus replication (Spec C1 & C4).
// - In 1-voter dev mode (peers empty), the single node is elected Leader immediately using the exact same code path.
// - In multi-node mode, the Leader replicates atomic record batches to a majority quorum before committing.
// - Only the Leader appends and runs hourly clearing (Spec C1: no Redis or external lock required).
type Node struct {
	appendMu    sync.Mutex
	mu          sync.RWMutex
	id          string
	peers       []string // peer node IDs (excluding self)
	transport   Transport
	role        Role
	term        uint64
	leaderID    string
	records     []ledger.Record
	commitIndex uint64
	snapshot    ledger.Snapshot
	partitioned map[string]bool // peerID -> unreachable
}

// Config configures a Raft Node.
type Config struct {
	NodeID    string
	Peers     []string
	Transport Transport
	Bootstrap bool // If true (or single-voter dev mode), starts as Leader for Term 1
}

// NewNode creates a Raft-replicated ledger.Log node.
func NewNode(cfg Config) *Node {
	n := &Node{
		id:          cfg.NodeID,
		peers:       cfg.Peers,
		transport:   cfg.Transport,
		role:        RoleFollower,
		term:        1,
		partitioned: make(map[string]bool),
	}
	// Single-voter dev mode or bootstrap leader
	if len(cfg.Peers) == 0 || cfg.Bootstrap {
		n.role = RoleLeader
		n.leaderID = cfg.NodeID
	}
	return n
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == RoleLeader
}

func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

// PromoteToLeader transitions this node to Leader for a new term (used on election win).
func (n *Node) PromoteToLeader() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.term++
	n.role = RoleLeader
	n.leaderID = n.id
	return n.term
}

// StepDown transitions this node to Follower.
func (n *Node) StepDown(newTerm uint64, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if newTerm > n.term {
		n.term = newTerm
	}
	n.role = RoleFollower
	n.leaderID = leaderID
}

// Shutdown stops the Raft node.
func (n *Node) Shutdown() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = RoleShutdown
}

// Append replicates and commits an atomic batch of records to the Raft log (Leader only, Spec C4).
func (n *Node) Append(ctx context.Context, recs ...ledger.Record) (string, error) {
	n.appendMu.Lock()
	defer n.appendMu.Unlock()

	n.mu.Lock()
	if n.role == RoleShutdown {
		n.mu.Unlock()
		return "", ErrNodeStopped
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return "", ErrNotLeader
	}

	expectedSeq := uint64(len(n.records)) + 1
	expectedPrev := ledger.ZeroHash
	if len(n.records) > 0 {
		expectedPrev = n.records[len(n.records)-1].Hash
	}

	for _, r := range recs {
		if r.Seq != expectedSeq {
			n.mu.Unlock()
			return "", fmt.Errorf("raft: sequence mismatch: got %d, want %d", r.Seq, expectedSeq)
		}
		if r.PrevHash != expectedPrev {
			n.mu.Unlock()
			return "", fmt.Errorf("raft: prev_hash mismatch at seq %d: got %s, want %s", r.Seq, r.PrevHash, expectedPrev)
		}
		expectedSeq++
		expectedPrev = r.Hash
	}

	term := n.term
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	tr := n.transport
	n.mu.Unlock()

	// Replicate to peers if in a multi-node cluster
	totalVoters := 1 + len(peers)
	quorum := totalVoters/2 + 1
	acks := 1 // Leader votes for itself

	if len(peers) > 0 && tr != nil {
		for _, peerID := range peers {
			if err := tr.Replicate(ctx, n.id, peerID, term, recs); err == nil {
				acks++
			}
		}
	}

	if acks < quorum {
		return "", fmt.Errorf("%w: got %d/%d acks, need %d", ErrNoQuorum, acks, totalVoters, quorum)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader || n.term != term {
		return "", ErrNotLeader
	}
	n.records = append(n.records, recs...)
	n.commitIndex = uint64(len(n.records))
	if len(n.records) == 0 {
		return ledger.ZeroHash, nil
	}
	return n.records[len(n.records)-1].Hash, nil
}

// ReceiveAppendEntries is called on a Follower when the Leader replicates records.
func (n *Node) ReceiveAppendEntries(fromLeader string, term uint64, recs []ledger.Record) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == RoleShutdown {
		return ErrNodeStopped
	}
	if term < n.term {
		return fmt.Errorf("stale term %d < %d", term, n.term)
	}
	n.term = term
	n.role = RoleFollower
	n.leaderID = fromLeader

	for _, r := range recs {
		if r.Seq <= uint64(len(n.records)) {
			// If an existing entry at r.Seq conflicts with the new leader's entry, overwrite and truncate tail (Raft §5.3)
			if n.records[r.Seq-1].Hash != r.Hash {
				n.records[r.Seq-1] = r
				n.records = n.records[:r.Seq]
			}
			continue
		}
		expectedSeq := uint64(len(n.records)) + 1
		expectedPrev := ledger.ZeroHash
		if len(n.records) > 0 {
			expectedPrev = n.records[len(n.records)-1].Hash
		}
		if r.Seq != expectedSeq || r.PrevHash != expectedPrev {
			return fmt.Errorf("follower log gap at seq %d", r.Seq)
		}
		n.records = append(n.records, r)
	}
	n.commitIndex = uint64(len(n.records))
	return nil
}

func (n *Node) Read(_ context.Context, fromSeq uint64) iter.Seq2[ledger.Record, error] {
	n.mu.RLock()
	if fromSeq <= 1 {
		fromSeq = 1
	}
	var slice []ledger.Record
	if int(fromSeq-1) < len(n.records) {
		slice = make([]ledger.Record, len(n.records)-int(fromSeq-1))
		copy(slice, n.records[fromSeq-1:])
	}
	n.mu.RUnlock()

	return func(yield func(ledger.Record, error) bool) {
		for _, rec := range slice {
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func (n *Node) Head(_ context.Context) (uint64, string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.records) == 0 {
		return 0, ledger.ZeroHash, nil
	}
	last := n.records[len(n.records)-1]
	return last.Seq, last.Hash, nil
}

func (n *Node) Snapshot(_ context.Context) (ledger.Snapshot, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.snapshot, nil
}

func (n *Node) SaveSnapshot(snap ledger.Snapshot) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshot = snap
}

// SimCluster is a deterministic in-memory multi-node Raft cluster with partition & leader-failure injection (Spec C6).
type SimCluster struct {
	mu         sync.RWMutex
	nodes      map[string]*Node
	partitions map[string]map[string]bool // from -> to -> blocked
}

func NewSimCluster(nodeIDs ...string) *SimCluster {
	c := &SimCluster{
		nodes:      make(map[string]*Node, len(nodeIDs)),
		partitions: make(map[string]map[string]bool),
	}
	for i, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		c.nodes[id] = NewNode(Config{
			NodeID:    id,
			Peers:     peers,
			Transport: c,
			Bootstrap: i == 0,
		})
	}
	return c
}

func (c *SimCluster) Node(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

func (c *SimCluster) Leader() *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var best *Node
	for _, n := range c.nodes {
		if n.IsLeader() {
			if best == nil || n.Term() > best.Term() {
				best = n
			}
		}
	}
	return best
}

// SetPartition blocks or unblocks network traffic between nodeA and nodeB.
func (c *SimCluster) SetPartition(nodeA, nodeB string, blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions[nodeA] == nil {
		c.partitions[nodeA] = make(map[string]bool)
	}
	if c.partitions[nodeB] == nil {
		c.partitions[nodeB] = make(map[string]bool)
	}
	c.partitions[nodeA][nodeB] = blocked
	c.partitions[nodeB][nodeA] = blocked
}

//isolateNode cuts a node off from all peers.
func (c *SimCluster) IsolateNode(nodeID string, isolated bool) {
	c.mu.RLock()
	var others []string
	for id := range c.nodes {
		if id != nodeID {
			others = append(others, id)
		}
	}
	c.mu.RUnlock()
	for _, other := range others {
		c.SetPartition(nodeID, other, isolated)
	}
}

// ElectLeader promotes newLeaderID and steps down any reachable peers with older terms, syncing log up to quorum head.
func (c *SimCluster) ElectLeader(newLeaderID string) error {
	c.mu.RLock()
	cand, ok := c.nodes[newLeaderID]
	if !ok {
		c.mu.RUnlock()
		return fmt.Errorf("unknown node %s", newLeaderID)
	}
	c.mu.RUnlock()

	newTerm := cand.PromoteToLeader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Catch up reachable followers with candidate's committed log
	var allRecs []ledger.Record
	for r, err := range cand.Read(ctx, 1) {
		if err == nil {
			allRecs = append(allRecs, r)
		}
	}
	for _, peerID := range cand.peers {
		_ = c.Replicate(ctx, newLeaderID, peerID, newTerm, allRecs)
	}
	return nil
}

func (c *SimCluster) Replicate(_ context.Context, fromID, toID string, term uint64, recs []ledger.Record) error {
	c.mu.RLock()
	if c.partitions[fromID] != nil && c.partitions[fromID][toID] {
		c.mu.RUnlock()
		return fmt.Errorf("network partition between %s and %s", fromID, toID)
	}
	target, ok := c.nodes[toID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown peer %s", toID)
	}
	return target.ReceiveAppendEntries(fromID, term, recs)
}

// HTTPTransport implements Transport over standard HTTP for multi-process Raft clusters.
type HTTPTransport struct {
	peerURLs   map[string]string // peerID -> base URL (e.g. "http://node2:8080")
	httpClient *http.Client
}

// NewHTTPTransport creates a network Transport mapping peer node IDs to their HTTP base URLs.
func NewHTTPTransport(peerURLs map[string]string) *HTTPTransport {
	clean := make(map[string]string, len(peerURLs))
	for id, u := range peerURLs {
		clean[id] = strings.TrimRight(u, "/")
	}
	return &HTTPTransport{
		peerURLs:   clean,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

type appendEntriesRequest struct {
	FromLeader string          `json:"from_leader"`
	Term       uint64          `json:"term"`
	Records    []ledger.Record `json:"records"`
}

// Replicate sends an AppendEntries RPC batch over HTTP POST to /internal/raft/append on toID.
func (t *HTTPTransport) Replicate(ctx context.Context, fromID, toID string, term uint64, recs []ledger.Record) error {
	baseURL, ok := t.peerURLs[toID]
	if !ok || baseURL == "" {
		return fmt.Errorf("raft http: unknown peer URL for %s", toID)
	}
	body, err := json.Marshal(appendEntriesRequest{
		FromLeader: fromID,
		Term:       term,
		Records:    recs,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/internal/raft/append", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft http: peer %s returned status %d", toID, resp.StatusCode)
	}
	return nil
}

// Handler returns an http.Handler for /internal/raft/append that receives replicated entries on this Node.
func (n *Node) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req appendEntriesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := n.ReceiveAppendEntries(req.FromLeader, req.Term, req.Records); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
