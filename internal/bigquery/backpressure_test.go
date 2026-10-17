// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

func TestBackpressureControllerAIMD(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     1024 * 1024,
		MaxRequestBytes:     8 * 1024 * 1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 8,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Second,
		LatencyThreshold:    2 * time.Second,
		Now:                 func() time.Time { return now },
	})

	if got := ctrl.EffectiveRequestBytes(); got != 8*1024*1024 {
		t.Fatalf("initial EffectiveRequestBytes() = %d, want %d", got, 8*1024*1024)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 8 {
		t.Fatalf("initial EffectiveInflightRequests() = %d, want 8", got)
	}
	if got := ctrl.Level(); got != 0 {
		t.Fatalf("initial Level() = %v, want 0", got)
	}

	// Clean ack at ceiling does nothing.
	if ctrl.OnCleanAck(100 * time.Millisecond) {
		t.Fatal("OnCleanAck at ceiling returned true, want false")
	}

	// First throttle steps down by 0.5x.
	if !ctrl.OnThrottle() {
		t.Fatal("first OnThrottle() = false, want true")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 4*1024*1024 {
		t.Fatalf("EffectiveRequestBytes() after 1st throttle = %d, want %d", got, 4*1024*1024)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 4 {
		t.Fatalf("EffectiveInflightRequests() after 1st throttle = %d, want 4", got)
	}
	if got := ctrl.Level(); got <= 0 || got >= 1 {
		t.Fatalf("Level() after 1st throttle = %v, want in (0, 1)", got)
	}

	// Burst throttle within 250ms cooldown does not step down again.
	now = now.Add(50 * time.Millisecond)
	if ctrl.OnThrottle() {
		t.Fatal("OnThrottle() inside cooldown = true, want false")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 4*1024*1024 {
		t.Fatalf("EffectiveRequestBytes() after cooldown throttle = %d, want %d", got, 4*1024*1024)
	}

	// Throttle after cooldown steps down to 2 MiB / 2 inflight.
	now = now.Add(250 * time.Millisecond)
	if !ctrl.OnThrottle() {
		t.Fatal("OnThrottle() after cooldown = false, want true")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 2*1024*1024 {
		t.Fatalf("EffectiveRequestBytes() after 2nd step = %d, want %d", got, 2*1024*1024)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() after 2nd step = %d, want 2", got)
	}

	// Third step reaches floor (1 MiB / 1 inflight) and Level() == 1.0.
	now = now.Add(250 * time.Millisecond)
	if !ctrl.OnThrottle() {
		t.Fatal("OnThrottle() to floor = false, want true")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 1024*1024 {
		t.Fatalf("EffectiveRequestBytes() at floor = %d, want %d", got, 1024*1024)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 1 {
		t.Fatalf("EffectiveInflightRequests() at floor = %d, want 1", got)
	}
	if got := ctrl.Level(); got != 1.0 {
		t.Fatalf("Level() at floor = %v, want 1.0", got)
	}

	// Further throttle at floor cannot go below floor and returns false.
	now = now.Add(250 * time.Millisecond)
	if ctrl.OnThrottle() {
		t.Fatal("OnThrottle() already at floor = true, want false")
	}

	// Clean ack before RecoveryInterval (10s) does not recover.
	now = now.Add(5 * time.Second)
	if ctrl.OnCleanAck(100 * time.Millisecond) {
		t.Fatal("OnCleanAck() before recovery_interval = true, want false")
	}

	// Clean ack after RecoveryInterval steps up additively (+10% of span, +1 inflight).
	now = now.Add(6 * time.Second)
	if !ctrl.OnCleanAck(100 * time.Millisecond) {
		t.Fatal("OnCleanAck() after recovery_interval = false, want true")
	}
	wantStep := (8*1024*1024 - 1024*1024) / 10
	if got, want := ctrl.EffectiveRequestBytes(), 1024*1024+wantStep; got != want {
		t.Fatalf("EffectiveRequestBytes() after 1st recovery = %d, want %d", got, want)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 2 {
		t.Fatalf("EffectiveInflightRequests() after 1st recovery = %d, want 2", got)
	}

	// Ten total recovery intervals return the controller to ceiling and Level() == 0.
	for i := 0; i < 10; i++ {
		now = now.Add(10 * time.Second)
		ctrl.OnCleanAck(100 * time.Millisecond)
	}
	if got := ctrl.EffectiveRequestBytes(); got != 8*1024*1024 {
		t.Fatalf("EffectiveRequestBytes() after full recovery = %d, want %d", got, 8*1024*1024)
	}
	if got := ctrl.EffectiveInflightRequests(); got != 8 {
		t.Fatalf("EffectiveInflightRequests() after full recovery = %d, want 8", got)
	}
	if got := ctrl.Level(); got != 0 {
		t.Fatalf("Level() after full recovery = %v, want 0", got)
	}
}

func TestBackpressureControllerSlowAck(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     1000,
		MaxRequestBytes:     4000,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      0.5,
		RecoveryInterval:    8 * time.Second,
		LatencyThreshold:    500 * time.Millisecond,
		Now:                 func() time.Time { return now },
	})

	if ctrl.OnSlowAck(500 * time.Millisecond) {
		t.Fatal("OnSlowAck at exact threshold = true, want false")
	}
	if !ctrl.OnSlowAck(501 * time.Millisecond) {
		t.Fatal("OnSlowAck above threshold = false, want true")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 2000 {
		t.Fatalf("EffectiveRequestBytes() = %d, want 2000", got)
	}

	// Slow ack passed to OnCleanAck never triggers recovery.
	now = now.Add(10 * time.Second)
	if ctrl.OnCleanAck(600 * time.Millisecond) {
		t.Fatal("OnCleanAck with slow duration = true, want false")
	}

	// Disabled LatencyThreshold ignores slow acks.
	disabled := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     1000,
		MaxRequestBytes:     4000,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		LatencyThreshold:    0,
		Now:                 func() time.Time { return now },
	})
	if disabled.OnSlowAck(10 * time.Second) {
		t.Fatal("OnSlowAck with LatencyThreshold=0 returned true, want false")
	}
}

func TestBackpressureControllerBoundsProperty(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	const (
		minB = 64 * 1024
		maxB = 2 * 1024 * 1024
		minI = 2
		maxI = 12
	)
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     minB,
		MaxRequestBytes:     maxB,
		MinInflightRequests: minI,
		MaxInflightRequests: maxI,
		DecreaseFactor:      0.65,
		RecoveryInterval:    4 * time.Second,
		LatencyThreshold:    time.Second,
		Now:                 func() time.Time { return now },
	})

	rng := rand.New(rand.NewPCG(42, 99))
	for range 2000 {
		now = now.Add(time.Duration(rng.IntN(3000)) * time.Millisecond)
		switch rng.IntN(3) {
		case 0:
			ctrl.OnThrottle()
		case 1:
			ctrl.OnSlowAck(time.Duration(rng.IntN(2500)) * time.Millisecond)
		default:
			ctrl.OnCleanAck(time.Duration(rng.IntN(900)) * time.Millisecond)
		}
		if got := ctrl.EffectiveRequestBytes(); got < minB || got > maxB {
			t.Fatalf("EffectiveRequestBytes() = %d outside [%d, %d]", got, minB, maxB)
		}
		if got := ctrl.EffectiveInflightRequests(); got < minI || got > maxI {
			t.Fatalf("EffectiveInflightRequests() = %d outside [%d, %d]", got, minI, maxI)
		}
		if lvl := ctrl.Level(); lvl < 0 || lvl > 1 {
			t.Fatalf("Level() = %v outside [0, 1]", lvl)
		}
	}
}

func TestBackpressureControllerConcurrentRace(t *testing.T) {
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     1024,
		MaxRequestBytes:     64 * 1024,
		MinInflightRequests: 1,
		MaxInflightRequests: 8,
		DecreaseFactor:      0.5,
		RecoveryInterval:    10 * time.Millisecond,
		LatencyThreshold:    5 * time.Millisecond,
	})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				_ = ctrl.EffectiveRequestBytes()
				_ = ctrl.EffectiveInflightRequests()
				_ = ctrl.Level()
				switch i % 3 {
				case 0:
					ctrl.OnThrottle()
				case 1:
					ctrl.OnSlowAck(8 * time.Millisecond)
				default:
					ctrl.OnCleanAck(time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	if got := ctrl.EffectiveRequestBytes(); got < 1024 || got > 64*1024 {
		t.Fatalf("EffectiveRequestBytes() after concurrent test = %d, want in [1024, 65536]", got)
	}
}

func TestBackpressureControllerCooldownDoesNotDelayRecoveryAndNaNFallback(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	ctrl := NewBackpressureController(BackpressureOptions{
		MinRequestBytes:     1000,
		MaxRequestBytes:     4000,
		MinInflightRequests: 1,
		MaxInflightRequests: 4,
		DecreaseFactor:      math.NaN(),
		RecoveryInterval:    10 * time.Second,
		Now:                 func() time.Time { return now },
	})

	if got := ctrl.MinRequestBytes(); got != 1000 {
		t.Fatalf("MinRequestBytes() = %d, want 1000", got)
	}

	// NaN DecreaseFactor falls back to 0.5x default.
	if !ctrl.OnThrottle() {
		t.Fatal("OnThrottle() = false, want true")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 2000 {
		t.Fatalf("EffectiveRequestBytes() after NaN-fallback throttle = %d, want 2000", got)
	}

	// A second throttle inside the 250ms cooldown is absorbed and must not
	// push back the recovery clock.
	now = now.Add(100 * time.Millisecond)
	if ctrl.OnThrottle() {
		t.Fatal("OnThrottle() inside cooldown = true, want false")
	}

	// Exactly 10s after the real decrease (9.9s after the absorbed throttle),
	// a clean ack must recover.
	now = now.Add(9900 * time.Millisecond)
	if !ctrl.OnCleanAck(50 * time.Millisecond) {
		t.Fatal("OnCleanAck() 10s after real decrease = false, want true (cooldown throttle must not reset lastEvent)")
	}
	if got := ctrl.EffectiveRequestBytes(); got != 2300 {
		t.Fatalf("EffectiveRequestBytes() after recovery = %d, want 2300", got)
	}
}
