package protocol

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	PubKeyPrefix = "ed25519:"
	SigPrefix    = "ed25519:"
	HashPrefix   = "sha256:"
	AgentPrefix  = "ag_"
)

var (
	ErrInvalidPubKey = errors.New("invalid ed25519 public key")
	ErrInvalidSig    = errors.New("invalid ed25519 signature")
)

// Signer abstracts holding a private key (in-memory, file, env, or KMS).
type Signer interface {
	PublicKey() ed25519.PublicKey
	PublicKeyString() string
	AgentID() string
	Sign(message []byte) (string, error)
	SignRaw(message []byte) ([]byte, error)
}

// MemorySigner is an in-memory Ed25519 Signer.
type MemorySigner struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
}

// GenerateKeyPair creates a new random Ed25519 key pair.
func GenerateKeyPair(rng io.Reader) (*MemorySigner, error) {
	if rng == nil {
		rng = rand.Reader
	}
	pub, priv, err := ed25519.GenerateKey(rng)
	if err != nil {
		return nil, err
	}
	return &MemorySigner{priv: priv, pub: pub}, nil
}

// NewSignerFromSeed creates a deterministic Ed25519 signer from a 32-byte seed.
func NewSignerFromSeed(seed []byte) (*MemorySigner, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	return &MemorySigner{priv: priv, pub: pub}, nil
}

// NewSignerFromSeedHex creates a deterministic Ed25519 signer from a 64-char hex seed.
func NewSignerFromSeedHex(seedHex string) (*MemorySigner, error) {
	seedHex = strings.TrimPrefix(seedHex, "ed25519:")
	b, err := hex.DecodeString(seedHex)
	if err != nil {
		return nil, err
	}
	return NewSignerFromSeed(b)
}

func (s *MemorySigner) PublicKey() ed25519.PublicKey {
	return s.pub
}

func (s *MemorySigner) PublicKeyString() string {
	return FormatPubKey(s.pub)
}

func (s *MemorySigner) AgentID() string {
	return AgentIDFromPubKey(s.pub)
}

func (s *MemorySigner) SeedHex() string {
	return hex.EncodeToString(s.priv.Seed())
}

func (s *MemorySigner) SignRaw(message []byte) ([]byte, error) {
	return ed25519.Sign(s.priv, message), nil
}

func (s *MemorySigner) Sign(message []byte) (string, error) {
	sig := ed25519.Sign(s.priv, message)
	return SigPrefix + hex.EncodeToString(sig), nil
}

// FormatPubKey formats a 32-byte Ed25519 public key as "ed25519:<hex>".
func FormatPubKey(pub ed25519.PublicKey) string {
	return PubKeyPrefix + hex.EncodeToString(pub)
}

// ParsePubKey parses an "ed25519:<hex>" string into an ed25519.PublicKey.
func ParsePubKey(s string) (ed25519.PublicKey, error) {
	if !strings.HasPrefix(s, PubKeyPrefix) {
		return nil, fmt.Errorf("%w: missing %s prefix", ErrInvalidPubKey, PubKeyPrefix)
	}
	b, err := hex.DecodeString(strings.TrimPrefix(s, PubKeyPrefix))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: invalid hex or length", ErrInvalidPubKey)
	}
	return ed25519.PublicKey(b), nil
}

// ParseSignature parses an "ed25519:<hex>" signature string.
func ParseSignature(s string) ([]byte, error) {
	if !strings.HasPrefix(s, SigPrefix) {
		return nil, fmt.Errorf("%w: missing %s prefix", ErrInvalidSig, SigPrefix)
	}
	b, err := hex.DecodeString(strings.TrimPrefix(s, SigPrefix))
	if err != nil || len(b) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: invalid hex or length", ErrInvalidSig)
	}
	return b, nil
}

// AgentIDFromPubKey computes the canonical agent ID ("ag_" + first 16 bytes of SHA-256 of raw public key in hex).
func AgentIDFromPubKey(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return AgentPrefix + hex.EncodeToString(sum[:16])
}

// AgentIDFromPubKeyString parses a public key string and returns its canonical agent ID.
func AgentIDFromPubKeyString(pubStr string) (string, error) {
	pub, err := ParsePubKey(pubStr)
	if err != nil {
		return "", err
	}
	return AgentIDFromPubKey(pub), nil
}

// VerifySignature verifies an "ed25519:<hex>" signature over message using an "ed25519:<hex>" public key.
func VerifySignature(pubStr string, message []byte, sigStr string) error {
	pub, err := ParsePubKey(pubStr)
	if err != nil {
		return err
	}
	sig, err := ParseSignature(sigStr)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, message, sig) {
		return ErrInvalidSig
	}
	return nil
}

// LotterySeedMessage constructs the canonical message signed by the game key to derive a round's lottery seed.
func LotterySeedMessage(gameID string, roundID uint64) []byte {
	return []byte("seed|" + gameID + "|" + strconv.FormatUint(roundID, 10))
}

// DeriveLotterySeed computes the 32-byte lottery seed and the server's Ed25519 signature string.
// Spec A9.6: seed = SHA-256(Sign_ed25519(game_key, "seed|" || game_id || "|" || round_id))
func DeriveLotterySeed(signer Signer, gameID string, roundID uint64) (seed [32]byte, sigStr string, err error) {
	msg := LotterySeedMessage(gameID, roundID)
	rawSig, err := signer.SignRaw(msg)
	if err != nil {
		return seed, "", err
	}
	seed = sha256.Sum256(rawSig)
	sigStr = SigPrefix + hex.EncodeToString(rawSig)
	return seed, sigStr, nil
}

// VerifyAndDeriveLotterySeed verifies the published seed signature against serverPubKey and returns the 32-byte seed.
func VerifyAndDeriveLotterySeed(serverPubKey string, gameID string, roundID uint64, sigStr string) ([32]byte, error) {
	var zero [32]byte
	msg := LotterySeedMessage(gameID, roundID)
	if err := VerifySignature(serverPubKey, msg, sigStr); err != nil {
		return zero, err
	}
	rawSig, err := ParseSignature(sigStr)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(rawSig), nil
}

// PRNG implements the normative deterministic pseudorandom number generator (Spec A4).
// Draws use HMAC-SHA256(seed, domain || uint64_be(counter)) -> uint64_be(block[0:8]) -> rejection sampling.
type PRNG struct {
	seed    [32]byte
	domain  string
	counter uint64
}

// NewPRNG creates a deterministic PRNG for a given 32-byte seed and domain ("buy" or "sell").
func NewPRNG(seed [32]byte, domain string) *PRNG {
	return &PRNG{
		seed:    seed,
		domain:  domain,
		counter: 0,
	}
}

// NextUint64 returns the next deterministic 64-bit unsigned integer and increments the counter.
func (p *PRNG) NextUint64() uint64 {
	mac := hmac.New(sha256.New, p.seed[:])
	mac.Write([]byte(p.domain))
	var ctrBuf [8]byte
	binary.BigEndian.PutUint64(ctrBuf[:], p.counter)
	mac.Write(ctrBuf[:])
	p.counter++
	sum := mac.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// IntN returns a uniform random integer in [0, n) without modulo bias using rejection sampling.
func (p *PRNG) IntN(n int) int {
	if n <= 1 {
		return 0
	}
	un := uint64(n)
	// threshold = (2^64 - un) % un == (-un) % un in uint64 arithmetic
	threshold := (0 - un) % un
	for {
		v := p.NextUint64()
		if v >= threshold {
			return int(v % un)
		}
	}
}

// Counter returns how many HMAC blocks have been consumed.
func (p *PRNG) Counter() uint64 {
	return p.counter
}

// ShuffleStrings performs an in-place Fisher-Yates shuffle on slice a using p.
func (p *PRNG) ShuffleStrings(a []string) {
	for i := len(a) - 1; i >= 1; i-- {
		j := p.IntN(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}
