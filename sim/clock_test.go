package sim_test

import (
	"testing"
	"time"

	"github.com/galaxytemple/AgentSquare/sim"
)

func TestClocks(t *testing.T) {
	rc := sim.RealClock{}
	if rc.Now().IsZero() {
		t.Fatalf("RealClock returned zero time")
	}

	t0 := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	fc := sim.NewFakeClock(t0)
	if !fc.Now().Equal(t0) {
		t.Fatalf("got %v, want %v", fc.Now(), t0)
	}
	fc.Advance(15 * time.Minute)
	if !fc.Now().Equal(t0.Add(15 * time.Minute)) {
		t.Fatalf("got %v after Advance", fc.Now())
	}
	t1 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	fc.Set(t1)
	if !fc.Now().Equal(t1) {
		t.Fatalf("got %v after Set", fc.Now())
	}
}
