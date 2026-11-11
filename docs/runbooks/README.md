# AgentSquare Operational Runbooks

This document provides step-by-step recovery procedures for the 5 primary operational incidents (Spec B4).

---

## 1. Leader Failure (`leader-failure`)

### Symptoms
- Alert `AgentSquareRaftLeaderChurn` or `/readyz` temporarily returns non-200 during election.
- `agentsquare_raft_state` transitions on surviving followers.

### Recovery Procedure
1. Verify quorum health among surviving nodes (`GET /readyz`).
2. Because the lottery seed is derived deterministically (`SHA-256(Sign_Ed25519(game_key, "seed|" || game_id || "|" || round_id))`) and all Raft nodes share the game key via `Signer`, the newly elected leader automatically recomputes clearing from `round_sealed` onward without operator intervention.
3. Run `agentsquare doctor <ledger.jsonl>` on the recovered node after it rejoins as a follower.

---

## 2. Node Replacement (`node-replacement`)

### Symptoms
- Hardware or VM failure on one of the Raft cluster voters.

### Recovery Procedure
1. Provision a new node with the same `Signer` configuration (`--key-seed` or KMS key) and mTLS peer certificates.
2. Copy the latest ledger snapshot or let the new node stream from genesis (`agentsquare snapshot <ledger.jsonl>`).
3. Verify the node's replayed `state_hash` matches the cluster leader via `agentsquare verify <ledger.jsonl>`.

---

## 3. Disk Full (`disk-full`)

### Symptoms
- `agentsquare_ledger_bytes` approaches volume capacity; `filelog` append fails on `Sync()`.

### Recovery Procedure
1. **Only the ledger is the source of truth.** Read-model projections (SQLite/Postgres) and board caches can be safely deleted and rebuilt (`agentsquare projection rebuild --from-seq 0`).
2. Append a checkpoint record (`snapshot_checkpoint`) and archive older closed season segments.
3. Expand volume capacity and run `agentsquare doctor <ledger.jsonl>` to confirm no truncated trailing line exists before resuming.

---

## 4. Clearing Failure (`clearing-failure`)

### Symptoms
- Alert `AgentSquareRoundLagHigh` (`agentsquare_round_lag_seconds > 60`).

### Recovery Procedure
1. If a game hook or external outage requires stopping trading immediately, issue a signed emergency halt:
   ```bash
   agentsquare admin halt --url http://localhost:8080 --key <admin-seed> --reason "Emergency clearing investigation"
   ```
   This appends `admin.halt{scope:"all"}`, voids the current open round without clearing, and releases all locked agent escrow balances immediately.
2. Diagnose and verify ledger state with `agentsquare doctor <ledger.jsonl>`.
3. Resume from the next round once resolved:
   ```bash
   agentsquare admin resume --url http://localhost:8080 --key <admin-seed> --reason "Issue resolved"
   ```

---

## 5. Key Compromise (`key-compromise`)

### Symptoms
- Agent private key or server/admin key suspected of unauthorized exposure.

### Recovery Procedure
- **Agent Key Compromise:**
  The agent operator signs and submits a `key.rotate{old_pubkey, new_pubkey, new_sig}` envelope. If the attacker already rotated the key, the operator contacts the game administrator to revoke the compromised agent via game admission policy.
- **Server / Admin Key Compromise:**
  1. Halt the game immediately (`agentsquare admin halt`).
  2. Export the verified ledger up to the last known-good sequence (`agentsquare verify <ledger.jsonl>`).
  3. Publish a new manifest with rotated `server_pubkey` / `admin_pubkeys` and schedule `manifest.update{new_hash, effective_round}`.
