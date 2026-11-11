package manifest

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/galaxytemple/AgentSquare/protocol"
)

const WellKnownPath = "/.well-known/agentsquare.json"

type Asset struct {
	ID      string `json:"id"`
	QtyUnit int64  `json:"qty_unit"`
}

type SettlementUnit struct {
	ID   string `json:"id"`
	Tick int64  `json:"tick"`
}

type RoundConfig struct {
	Interval  string `json:"interval"` // ISO 8601 duration, e.g. "PT1H", "PT1M", "PT30S"
	Anchor    string `json:"anchor"`   // RFC 3339 UTC timestamp
	CarryOver bool   `json:"carry_over"`
}

type PriceBandConfig struct {
	Reference string `json:"reference"`
	LowerPct  int64  `json:"lower_pct"`
	UpperPct  int64  `json:"upper_pct"`
}

type MarketDataConfig struct {
	PreClear string `json:"pre_clear"` // "none" | "indicative" | "depth"
}

type BoardConfig struct {
	Enabled                 bool   `json:"enabled"`
	MaxPostsPerAgentPerRound int64 `json:"max_posts_per_agent_per_round"`
	MaxBodyChars            int64  `json:"max_body_chars"`
	MinIntervalSec          int64  `json:"min_interval_sec"`
	RequireParticipant      bool   `json:"require_participant"`
	BlackoutBeforeCutoffSec int64  `json:"blackout_before_cutoff_sec"`
	VisibilityDelaySec      int64  `json:"visibility_delay_sec"`
	AllowedContent          string `json:"allowed_content"`
}

type LedgerConfig struct {
	GenesisHash string `json:"genesis_hash"`
	Public      string `json:"public"` // "after-clear" | "always"
}

type Binding struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

// Manifest represents the game discovery & parameter document (Spec A7).
type Manifest struct {
	Protocol          string           `json:"protocol"`
	SupportedVersions []string         `json:"supported_versions"`
	GameID            string           `json:"game_id"`
	Assets            []Asset          `json:"assets"`
	SettlementUnit    SettlementUnit   `json:"settlement_unit"`
	Round             RoundConfig      `json:"round"`
	PriceBand         PriceBandConfig  `json:"price_band"`
	MarketData        MarketDataConfig `json:"market_data"`
	FeeBps            int64            `json:"fee_bps"`
	Board             *BoardConfig     `json:"board,omitempty"`
	Admission         string           `json:"admission"`
	Ledger            LedgerConfig     `json:"ledger"`
	ServerPubKey      string           `json:"server_pubkey"`
	AdminPubKeys      []string         `json:"admin_pubkeys"`
	InitialBalances   map[string]int64 `json:"initial_balances,omitempty"`
	Critical          []string         `json:"critical,omitempty"`
	Bindings          []Binding        `json:"bindings,omitempty"`
}

var knownManifestFields = map[string]bool{
	"protocol":           true,
	"supported_versions": true,
	"game_id":            true,
	"assets":             true,
	"settlement_unit":    true,
	"round":              true,
	"price_band":         true,
	"market_data":        true,
	"fee_bps":            true,
	"board":              true,
	"admission":          true,
	"ledger":             true,
	"server_pubkey":      true,
	"admin_pubkeys":      true,
	"initial_balances":   true,
	"critical":           true,
	"bindings":           true,
}

// ParseAndValidate parses raw manifest JSON and validates all normative constraints.
func ParseAndValidate(raw []byte) (*Manifest, error) {
	if _, err := protocol.CanonicalizeJSON(raw); err != nil {
		return nil, err
	}
	var rawMap map[string]any
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	for _, crit := range m.Critical {
		if !knownManifestFields[crit] {
			return nil, fmt.Errorf("unknown critical manifest field: %s", crit)
		}
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// Validate checks semantic validity of a Manifest struct.
func (m *Manifest) Validate() error {
	if m.Protocol != protocol.ProtocolID && m.Protocol != "roundabout/1.0" {
		return fmt.Errorf("unsupported protocol: %s", m.Protocol)
	}
	if m.GameID == "" {
		return fmt.Errorf("game_id is required")
	}
	if len(m.Assets) == 0 {
		return fmt.Errorf("at least one asset is required")
	}
	seenAssets := make(map[string]bool)
	for _, a := range m.Assets {
		if a.ID == "" || a.QtyUnit <= 0 {
			return fmt.Errorf("invalid asset definition: %+v", a)
		}
		if seenAssets[a.ID] {
			return fmt.Errorf("duplicate asset id: %s", a.ID)
		}
		seenAssets[a.ID] = true
	}
	if m.SettlementUnit.ID == "" || m.SettlementUnit.Tick <= 0 {
		return fmt.Errorf("invalid settlement_unit")
	}
	if _, err := m.IntervalDuration(); err != nil {
		return fmt.Errorf("invalid round.interval: %w", err)
	}
	if _, err := m.AnchorTime(); err != nil {
		return fmt.Errorf("invalid round.anchor: %w", err)
	}
	if m.PriceBand.LowerPct <= 0 || m.PriceBand.UpperPct < m.PriceBand.LowerPct {
		return fmt.Errorf("invalid price_band percentages: lower=%d upper=%d", m.PriceBand.LowerPct, m.PriceBand.UpperPct)
	}
	switch m.MarketData.PreClear {
	case "", "indicative", "none", "depth":
	default:
		return fmt.Errorf("invalid market_data.pre_clear: %s", m.MarketData.PreClear)
	}
	if m.FeeBps != 0 {
		return fmt.Errorf("fee_bps must be 0 in v1")
	}
	if _, err := protocol.ParsePubKey(m.ServerPubKey); err != nil {
		return fmt.Errorf("invalid server_pubkey: %w", err)
	}
	for _, ak := range m.AdminPubKeys {
		if _, err := protocol.ParsePubKey(ak); err != nil {
			return fmt.Errorf("invalid admin_pubkey %s: %w", ak, err)
		}
	}
	return nil
}

// Hash computes the canonical SHA-256 hash ("sha256:<hex>") of the manifest.
// Notice that ledger.genesis_hash is excluded (set to "") when hashing the manifest for genesis anchoring.
func (m *Manifest) Hash() (string, error) {
	cp := *m
	cp.Ledger.GenesisHash = ""
	return protocol.HashCanonical(cp)
}

// HasAsset checks whether assetID is declared in the manifest.
func (m *Manifest) HasAsset(assetID string) bool {
	for _, a := range m.Assets {
		if a.ID == assetID {
			return true
		}
	}
	return false
}

// PriceBounds computes the inclusive [minPrice, maxPrice] allowed for a given referencePrice.
// minPrice = ceil(refPrice * lower_pct / 100), maxPrice = floor(refPrice * upper_pct / 100).
func (m *Manifest) PriceBounds(refPrice int64) (minPrice, maxPrice int64) {
	minPrice = (refPrice*m.PriceBand.LowerPct + 99) / 100
	if minPrice < 1 {
		minPrice = 1
	}
	maxPrice = (refPrice * m.PriceBand.UpperPct) / 100
	return minPrice, maxPrice
}

// AnchorTime parses the round.anchor UTC timestamp.
func (m *Manifest) AnchorTime() (time.Time, error) {
	return time.Parse(time.RFC3339, m.Round.Anchor)
}

// IntervalDuration parses the ISO 8601 duration in round.interval (e.g., "PT1H", "PT1M", "PT30S").
func (m *Manifest) IntervalDuration() (time.Duration, error) {
	return ParseISO8601Duration(m.Round.Interval)
}

// RoundForTime returns the 1-indexed round number and its [start, end) UTC boundaries for time t.
func (m *Manifest) RoundForTime(t time.Time) (round uint64, start, end time.Time, err error) {
	anchor, err := m.AnchorTime()
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	interval, err := m.IntervalDuration()
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	if t.Before(anchor) {
		return 1, anchor, anchor.Add(interval), nil
	}
	elapsed := t.Sub(anchor)
	idx := uint64(elapsed / interval)
	round = idx + 1
	start = anchor.Add(time.Duration(idx) * interval)
	end = start.Add(interval)
	return round, start, end, nil
}

// RoundBounds returns the [start, end) UTC timestamps for a given 1-indexed round number.
func (m *Manifest) RoundBounds(round uint64) (start, end time.Time, err error) {
	if round == 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("round must be >= 1")
	}
	anchor, err := m.AnchorTime()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	interval, err := m.IntervalDuration()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start = anchor.Add(time.Duration(round-1) * interval)
	end = start.Add(interval)
	return start, end, nil
}

// ParseISO8601Duration parses simple ISO 8601 durations such as "PT1H", "PT15M", "PT30S", "PT1H30M".
func ParseISO8601Duration(s string) (time.Duration, error) {
	if !strings.HasPrefix(s, "PT") {
		return 0, fmt.Errorf("duration must start with PT: %s", s)
	}
	rest := strings.TrimPrefix(s, "PT")
	if rest == "" {
		return 0, fmt.Errorf("empty PT duration")
	}
	var total time.Duration
	numStart := 0
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if c >= '0' && c <= '9' {
			continue
		}
		valStr := rest[numStart:i]
		if valStr == "" {
			return 0, fmt.Errorf("invalid duration syntax: %s", s)
		}
		v, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("invalid duration number in %s", s)
		}
		switch c {
		case 'H':
			total += time.Duration(v) * time.Hour
		case 'M':
			total += time.Duration(v) * time.Minute
		case 'S':
			total += time.Duration(v) * time.Second
		default:
			return 0, fmt.Errorf("unsupported duration unit %q in %s", c, s)
		}
		numStart = i + 1
	}
	if numStart != len(rest) || total <= 0 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}
	return total, nil
}
