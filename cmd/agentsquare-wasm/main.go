//go:build js && wasm

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"syscall/js"

	"github.com/galaxytemple/AgentSquare/ledger"
	"github.com/galaxytemple/AgentSquare/manifest"
)

func verifyLedgerJSONL(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return `{"valid":false,"error":"missing ledger JSONL argument"}`
	}
	jsonlText := args[0].String()
	var m *manifest.Manifest
	if len(args) >= 2 && args[1].String() != "" {
		parsed, err := manifest.ParseAndValidate([]byte(args[1].String()))
		if err == nil {
			m = parsed
		}
	}

	lines := strings.Split(strings.TrimSpace(jsonlText), "\n")
	var recs []ledger.Record
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var r ledger.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			out, _ := json.Marshal(map[string]any{
				"valid":          false,
				"first_mismatch": i + 1,
				"error":          err.Error(),
			})
			return string(out)
		}
		recs = append(recs, r)
	}

	st := ledger.NewState(m)
	for _, r := range recs {
		if err := st.Apply(r); err != nil {
			out, _ := json.Marshal(map[string]any{
				"valid":          false,
				"first_mismatch": r.Seq,
				"error":          err.Error(),
			})
			return string(out)
		}
	}
	if err := st.DoctorCheck(); err != nil {
		out, _ := json.Marshal(map[string]any{
			"valid": false,
			"error": err.Error(),
		})
		return string(out)
	}
	sh, _ := st.StateHash()
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(map[string]any{
		"valid":        true,
		"records":      len(recs),
		"last_seq":     st.LastSeq,
		"last_hash":    st.LastHash,
		"state_hash":   sh,
		"snapshot":     st.Snapshot(),
		"wasm_runtime": "go-agentsquare-core",
	})
	return buf.String()
}

func main() {
	js.Global().Set("agentSquareVerifyLedger", js.FuncOf(verifyLedgerJSONL))
	select {}
}
