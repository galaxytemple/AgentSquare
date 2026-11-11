package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

const (
	// MaxSafeInt is 2^53 - 1 (9007199254740991). All protocol integers MUST satisfy 0 <= x <= MaxSafeInt.
	MaxSafeInt int64 = (1 << 53) - 1
)

var (
	ErrFloatNotAllowed = errors.New("floating-point numbers are not allowed in protocol JSON")
	ErrIntOutOfRange   = errors.New("integer out of allowed range [0, 2^53-1]")
	ErrInvalidJSON     = errors.New("invalid JSON input")
)

// ValidateInt checks that an integer satisfies 0 <= x < 2^53.
func ValidateInt(x int64) error {
	if x < 0 || x > MaxSafeInt {
		return fmt.Errorf("%w: %d", ErrIntOutOfRange, x)
	}
	return nil
}

// SafeMulInt multiplies two non-negative integers and returns ErrIntOutOfRange if a*b > MaxSafeInt (2^53 - 1).
func SafeMulInt(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > MaxSafeInt || b > MaxSafeInt {
		return 0, ErrIntOutOfRange
	}
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a > MaxSafeInt/b {
		return 0, fmt.Errorf("%w: %d * %d exceeds 2^53-1", ErrIntOutOfRange, a, b)
	}
	return a * b, nil
}

// CanonicalizeJSON converts arbitrary JSON bytes into RFC 8785 JSON Canonicalization Scheme (JCS) bytes,
// while enforcing the protocol determinism rule that all numbers MUST be non-negative integers in [0, 2^53-1].
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var val any
	if err := dec.Decode(&val); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	// Ensure no trailing tokens
	if dec.More() {
		return nil, fmt.Errorf("%w: unexpected trailing data", ErrInvalidJSON)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return nil, fmt.Errorf("%w: multiple JSON values", ErrInvalidJSON)
	}

	var buf bytes.Buffer
	if err := writeCanonical(&buf, val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalCanonical marshals a Go value to JSON and then returns its RFC 8785 JCS canonical form.
func MarshalCanonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalizeJSON(raw)
}

// HashCanonical computes "sha256:<hex>" over the RFC 8785 JCS representation of v.
func HashCanonical(v any) (string, error) {
	canon, err := MarshalCanonical(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// HashCanonicalBytes computes "sha256:<hex>" over the RFC 8785 JCS representation of raw JSON bytes.
func HashCanonicalBytes(raw []byte) (string, error) {
	canon, err := CanonicalizeJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if val {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		s := val.String()
		if strings.ContainsAny(s, ".eE") {
			return fmt.Errorf("%w: %s", ErrFloatNotAllowed, s)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrIntOutOfRange, s)
		}
		if err := ValidateInt(n); err != nil {
			return err
		}
		buf.WriteString(strconv.FormatInt(n, 10))
	case string:
		writeCanonicalString(buf, val)
	case []any:
		buf.WriteByte('[')
		for i, elem := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, elem); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		// RFC 8785 JCS sorts object properties lexicographically by UTF-16 code units.
		sort.Slice(keys, func(i, j int) bool {
			return compareUTF16(keys[i], keys[j]) < 0
		})
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalString(buf, k)
			buf.WriteByte(':')
			if err := writeCanonical(buf, val[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JSON type %T", v)
	}
	return nil
}

func compareUTF16(a, b string) int {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	minLen := len(ua)
	if len(ub) < minLen {
		minLen = len(ub)
	}
	for i := 0; i < minLen; i++ {
		if ua[i] < ub[i] {
			return -1
		}
		if ua[i] > ub[i] {
			return 1
		}
	}
	if len(ua) < len(ub) {
		return -1
	}
	if len(ua) > len(ub) {
		return 1
	}
	return 0
}

func writeCanonicalString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if r < 0x20 {
				buf.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				buf.WriteRune(r)
			}
		}
	}
	buf.WriteByte('"')
}
