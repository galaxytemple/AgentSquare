package protocol

import (
	"encoding/json"
	"testing"
)

func TestJCSAndIntegerDeterminism(t *testing.T) {
	// Keys should be sorted in UTF-16 order and whitespace stripped
	raw := []byte(`{ "z": 10, "a": [3, 2, 1], "b": {"y": true, "x": null} }`)
	canon, err := CanonicalizeJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `{"a":[3,2,1],"b":{"x":null,"y":true},"z":10}`
	if string(canon) != expected {
		t.Fatalf("got %s, want %s", string(canon), expected)
	}

	// Floating point must be rejected
	if _, err := CanonicalizeJSON([]byte(`{"price": 9.5}`)); err == nil {
		t.Fatalf("expected error for floating point number")
	}

	// Negative integers must be rejected
	if _, err := CanonicalizeJSON([]byte(`{"price": -1}`)); err == nil {
		t.Fatalf("expected error for negative integer")
	}

	// 2^53 must be rejected, 2^53 - 1 must pass
	if _, err := CanonicalizeJSON([]byte(`{"val": 9007199254740991}`)); err != nil {
		t.Fatalf("expected 2^53-1 to pass: %v", err)
	}
	if _, err := CanonicalizeJSON([]byte(`{"val": 9007199254740992}`)); err == nil {
		t.Fatalf("expected 2^53 to fail")
	}
}

func TestEnvelopeSigningWithUnknownAndCriticalFields(t *testing.T) {
	seed := make([]byte, 32)
	seed[0] = 42
	signer, err := NewSignerFromSeed(seed)
	if err != nil {
		t.Fatalf("NewSignerFromSeed: %v", err)
	}

	// Envelope with an unknown field "custom_ext": "hello"
	rawUnsigned := []byte(`{
		"version": "1.0",
		"game_id": "test-game",
		"from": "` + signer.AgentID() + `",
		"to": "server",
		"type": "place_order",
		"timestamp": 1700000000,
		"nonce": "n1",
		"corr_id": "c1",
		"custom_ext": "hello",
		"payload": {"asset": "copper", "side": "buy", "limit_price": 9, "qty": 5}
	}`)

	_, signedBytes, err := SignRawEnvelopeJSON(rawUnsigned, signer)
	if err != nil {
		t.Fatalf("SignRawEnvelopeJSON: %v", err)
	}

	// Verification must succeed when unknown field is not critical
	env, hash1, code, err := VerifyRawEnvelope(signedBytes, signer.PublicKeyString(), nil)
	if err != nil || code != "" {
		t.Fatalf("VerifyRawEnvelope failed: code=%s err=%v", code, err)
	}
	if env.GameID != "test-game" || hash1 == "" {
		t.Fatalf("unexpected envelope fields: %+v", env)
	}

	// Tampering with the unknown field must invalidate the signature
	var tampered map[string]any
	_ = json.Unmarshal(signedBytes, &tampered)
	tampered["custom_ext"] = "tampered"
	tamperedBytes, _ := json.Marshal(tampered)
	if _, _, code, err := VerifyRawEnvelope(tamperedBytes, signer.PublicKeyString(), nil); err == nil || code != ErrCodeInvalidSignature {
		t.Fatalf("expected INVALID_SIGNATURE on tampered unknown field, got code=%s err=%v", code, err)
	}

	// Adding an unknown field in "critical" must fail with UNSUPPORTED_VERSION
	rawCritical := []byte(`{
		"version": "1.0",
		"game_id": "test-game",
		"from": "` + signer.AgentID() + `",
		"to": "server",
		"type": "place_order",
		"timestamp": 1700000000,
		"nonce": "n2",
		"corr_id": "c2",
		"critical": ["custom_ext"],
		"custom_ext": "hello",
		"payload": {"asset": "copper", "side": "buy", "limit_price": 9, "qty": 5}
	}`)
	_, signedCrit, err := SignRawEnvelopeJSON(rawCritical, signer)
	if err != nil {
		t.Fatalf("SignRawEnvelopeJSON: %v", err)
	}
	if _, _, code, err := VerifyRawEnvelope(signedCrit, signer.PublicKeyString(), nil); err == nil || code != ErrCodeUnsupportedVersion {
		t.Fatalf("expected UNSUPPORTED_VERSION for unknown critical field, got code=%s err=%v", code, err)
	}
}

func TestDeterministicPRNGAndLotterySeed(t *testing.T) {
	seed := make([]byte, 32)
	seed[0] = 7
	serverKey, _ := NewSignerFromSeed(seed)

	lotSeed1, sig1, err := DeriveLotterySeed(serverKey, "city-2026w46", 14)
	if err != nil {
		t.Fatalf("DeriveLotterySeed: %v", err)
	}
	lotSeed2, err := VerifyAndDeriveLotterySeed(serverKey.PublicKeyString(), "city-2026w46", 14, sig1)
	if err != nil {
		t.Fatalf("VerifyAndDeriveLotterySeed: %v", err)
	}
	if lotSeed1 != lotSeed2 {
		t.Fatalf("lottery seed mismatch")
	}

	p1 := NewPRNG(lotSeed1, "buy")
	p2 := NewPRNG(lotSeed2, "buy")
	a1 := []string{"ag_a", "ag_b", "ag_c", "ag_d"}
	a2 := []string{"ag_a", "ag_b", "ag_c", "ag_d"}
	p1.ShuffleStrings(a1)
	p2.ShuffleStrings(a2)
	for i := range a1 {
		if a1[i] != a2[i] {
			t.Fatalf("shuffle not deterministic at index %d: %v vs %v", i, a1, a2)
		}
	}
	if p1.Counter() == 0 {
		t.Fatalf("expected non-zero PRNG counter")
	}
}

func TestCryptoReceiptsRejectionsAndSafeMul(t *testing.T) {
	kp, err := GenerateKeyPair(nil)
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	if len(kp.PublicKey()) != 32 || len(kp.SeedHex()) != 64 {
		t.Fatalf("unexpected key/seed lengths")
	}
	kp2, err := NewSignerFromSeedHex(kp.SeedHex())
	if err != nil || kp2.AgentID() != kp.AgentID() {
		t.Fatalf("NewSignerFromSeedHex mismatch: %v", err)
	}
	derived, err := AgentIDFromPubKeyString(kp.PublicKeyString())
	if err != nil || derived != kp.AgentID() {
		t.Fatalf("AgentIDFromPubKeyString mismatch: %v", err)
	}

	env := &Envelope{
		Version:   CurrentVersion,
		GameID:    "g1",
		From:      kp.AgentID(),
		To:        "server",
		Type:      TypePlaceOrder,
		Timestamp: 1700000000,
		Nonce:     "n1",
		CorrID:    "c1",
		Payload:   json.RawMessage(`{"asset":"copper","side":"buy","limit_price":10,"qty":2}`),
	}
	if err := SignEnvelope(env, kp); err != nil || env.Signature == "" {
		t.Fatalf("SignEnvelope failed: %v", err)
	}

	rec := &AcceptanceReceipt{
		GameID:      "g1",
		CorrID:      "c1",
		OrderID:     "ord_1_1",
		Round:       1,
		Seq:         2,
		ContentHash: "sha256:0001",
	}
	if err := SignReceipt(rec, kp); err != nil {
		t.Fatalf("SignReceipt failed: %v", err)
	}
	if err := VerifyReceipt(rec, kp.PublicKeyString()); err != nil {
		t.Fatalf("VerifyReceipt failed: %v", err)
	}

	rej, err := NewSignedRejection("g1", "c1", ErrCodeOutOfBand, "bad price", "fix price", 1700000000, kp)
	if err != nil {
		t.Fatalf("NewSignedRejection failed: %v", err)
	}
	if err := VerifyRejection(rej, kp.PublicKeyString()); err != nil {
		t.Fatalf("VerifyRejection failed: %v", err)
	}

	if v, err := SafeMulInt(1000, 2000); err != nil || v != 2000000 {
		t.Fatalf("SafeMulInt(1000, 2000) = %d, %v", v, err)
	}
	if _, err := SafeMulInt(MaxSafeInt, 2); err == nil {
		t.Fatalf("expected SafeMulInt overflow error")
	}
	h, err := HashCanonical(map[string]any{"msg": "line1\nline2\t\"quote\"\b\f\r\\\x01"})
	if err != nil || h == "" {
		t.Fatalf("HashCanonical control-char escaping failed: %v", err)
	}
}
