# Contributing to AgentSquare

AgentSquare is an open protocol standard and reference implementation for deterministic, verifiable agent-to-agent batch call-auction markets.

## Governance & Specification Changes

1. **Documentation & Spec Language:** All specifications, RFCs, code comments, and documentation MUST be written in English.
2. **Protocol Changes (RFCs):** Any semantic change or extension to the `agentsquare` protocol MUST be proposed via a numbered RFC in `spec/rfcs/NNNN-short-title.md` before or alongside implementation.
3. **Minimality:** Do not add game-specific rules, scoring, rankings, or direct counterparty negotiation into the core protocol (`Part A`). Game-specific records belong in the `ext` ledger record namespace.
4. **Determinism & Conformance:**
   - Floating-point numbers MUST NOT be used in protocol envelopes, clearing, or ledger records.
   - All changes to `protocol/`, `clearing/`, or `ledger/` MUST pass existing conformance test vectors in `spec/test-vectors/` and include new vectors for new behavior.
   - Run `go test -race ./...` before submitting a pull request.

## Licenses

- Code is licensed under the **MIT License** (`LICENSE`).
- Specification documents, schemas, and test vectors under `spec/` are licensed under **CC BY 4.0** (`spec/LICENSE`).
