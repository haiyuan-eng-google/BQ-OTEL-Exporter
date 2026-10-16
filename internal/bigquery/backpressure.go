// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigquery

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBackpressureMinBytes        = 1024 * 1024
	defaultBackpressureMaxBytes        = 8 * 1024 * 1024
	defaultBackpressureMinInflight     = 1
	defaultBackpressureMaxInflight     = 8
	defaultBackpressureDecreaseFactor  = 0.5
	defaultBackpressureRecovery        = 10 * time.Second
	maxBackpressureDecreaseCooldownCap = 250 * time.Millisecond
)

// BackpressureOptions configures an AIMD BackpressureController.
type BackpressureOptions struct {
	MinRequestBytes     int
	MaxRequestBytes     int
	MinInflightRequests int
	MaxInflightRequests int
	DecreaseFactor      float64
	RecoveryInterval    time.Duration
	LatencyThreshold    time.Duration
	Now                 func() time.Time
}

// BackpressureController adjusts the effective per-request byte threshold and
// in-flight request limit using additive-increase / multiplicative-decrease
// (AIMD) control.
//
// It never sleeps, drops, or retries rows: callers read the current atomic
// limits before splitting and dispatching sub-requests, while exporterhelper
// remains the sole owner of retry scheduling.
type BackpressureController struct {
	opts     BackpressureOptions
	cooldown time.Duration
	now      func() time.Time

	effectiveBytes    atomic.Int64
	effectiveInflight atomic.Int64
	ackLatencyNanos   atomic.Int64

	mu           sync.Mutex
	lastDecrease time.Time
	lastEvent    time.Time
}

// NewBackpressureController returns an AIMD controller initialized at its
// configured ceiling (Level() == 0).
func NewBackpressureController(opts BackpressureOptions) *BackpressureController {
	if opts.MaxRequestBytes <= 0 {
		opts.MaxRequestBytes = defaultBackpressureMaxBytes
	}
	if opts.MinRequestBytes <= 0 {
		opts.MinRequestBytes = min(defaultBackpressureMinBytes, opts.MaxRequestBytes)
	}
	if opts.MinRequestBytes > opts.MaxRequestBytes {
		opts.MinRequestBytes = opts.MaxRequestBytes
	}
	if opts.MaxInflightRequests < 1 {
		opts.MaxInflightRequests = defaultBackpressureMaxInflight
	}
	if opts.MinInflightRequests < 1 {
		opts.MinInflightRequests = defaultBackpressureMinInflight
	}
	if opts.MinInflightRequests > opts.MaxInflightRequests {
		opts.MinInflightRequests = opts.MaxInflightRequests
	}
	if math.IsNaN(opts.DecreaseFactor) || opts.DecreaseFactor <= 0 || opts.DecreaseFactor >= 1.0 {
		opts.DecreaseFactor = defaultBackpressureDecreaseFactor
	}
	if opts.RecoveryInterval <= 0 {
		opts.RecoveryInterval = defaultBackpressureRecovery
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	cooldown := min(opts.RecoveryInterval/4, maxBackpressureDecreaseCooldownCap)

	c := &BackpressureController{
		opts:     opts,
		cooldown: cooldown,
		now:      nowFn,
	}
	c.effectiveBytes.Store(int64(opts.MaxRequestBytes))
	c.effectiveInflight.Store(int64(opts.MaxInflightRequests))
	return c
}

// EffectiveRequestBytes returns the current request split threshold in bytes.
func (c *BackpressureController) EffectiveRequestBytes() int {
	return int(c.effectiveBytes.Load())
}

// MinRequestBytes returns the configured floor for the request split threshold
// in bytes.
func (c *BackpressureController) MinRequestBytes() int {
	return c.opts.MinRequestBytes
}

// EffectiveInflightRequests returns the current concurrent dispatch limit.
func (c *BackpressureController) EffectiveInflightRequests() int {
	return int(c.effectiveInflight.Load())
}

// LatencyThreshold returns the configured slow-acknowledgement threshold, or 0
// when latency-based step-down is disabled.
func (c *BackpressureController) LatencyThreshold() time.Duration {
	return c.opts.LatencyThreshold
}

// MaxAckLatency returns the controller's recent acknowledgement latency
// estimate, or 0 before any server acknowledgement has been observed.
func (c *BackpressureController) MaxAckLatency() time.Duration {
	return time.Duration(c.ackLatencyNanos.Load())
}

// ObserveAckLatency updates the controller's recent server acknowledgement
// latency estimate. Spikes raise the estimate immediately; faster acks decay
// it gradually so a single old outlier cannot starve later deadline budgets.
func (c *BackpressureController) ObserveAckLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	ns := int64(d)
	for {
		cur := c.ackLatencyNanos.Load()
		var next int64
		if cur <= 0 || ns >= cur {
			next = ns
		} else {
			next = cur - (cur-ns)/4
		}
		if c.ackLatencyNanos.CompareAndSwap(cur, next) {
			return
		}
	}
}

// Level returns the current backpressure level in [0.0, 1.0], where 0 means
// the controller is at its configured ceiling and 1 means either effective
// request bytes or effective in-flight requests has reached its configured
// floor.
func (c *BackpressureController) Level() float64 {
	var bytesPressure float64
	if span := c.opts.MaxRequestBytes - c.opts.MinRequestBytes; span > 0 {
		cur := int(c.effectiveBytes.Load())
		bytesPressure = float64(c.opts.MaxRequestBytes-cur) / float64(span)
	}
	var inflightPressure float64
	if span := c.opts.MaxInflightRequests - c.opts.MinInflightRequests; span > 0 {
		cur := int(c.effectiveInflight.Load())
		inflightPressure = float64(c.opts.MaxInflightRequests-cur) / float64(span)
	}
	lvl := max(bytesPressure, inflightPressure)
	if lvl < 0 {
		return 0
	}
	if lvl > 1 {
		return 1
	}
	return lvl
}

// OnThrottle records a congestion or quota signal and applies a multiplicative
// decrease if outside the cooldown window. It returns true when the effective
// byte threshold or in-flight limit stepped down.
func (c *BackpressureController) OnThrottle() bool {
	return c.decrease(c.now())
}

// OnSlowAck records an AppendResult acknowledgement duration and applies a
// multiplicative decrease when LatencyThreshold is enabled and exceeded. It
// returns true when the effective byte threshold or in-flight limit stepped
// down.
func (c *BackpressureController) OnSlowAck(d time.Duration) bool {
	if c.opts.LatencyThreshold <= 0 || d <= c.opts.LatencyThreshold {
		return false
	}
	return c.decrease(c.now())
}

// OnCleanAck records a healthy AppendResult acknowledgement and applies one
// additive recovery step when RecoveryInterval has elapsed since the previous
// congestion or recovery event. It returns true when the effective byte
// threshold or in-flight limit stepped up.
func (c *BackpressureController) OnCleanAck(d time.Duration) bool {
	c.ObserveAckLatency(d)
	if c.opts.LatencyThreshold > 0 && d > c.opts.LatencyThreshold {
		return false
	}
	if c.effectiveBytes.Load() >= int64(c.opts.MaxRequestBytes) && c.effectiveInflight.Load() >= int64(c.opts.MaxInflightRequests) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	curBytes := c.effectiveBytes.Load()
	curInflight := c.effectiveInflight.Load()
	maxBytes := int64(c.opts.MaxRequestBytes)
	maxInflight := int64(c.opts.MaxInflightRequests)
	if curBytes >= maxBytes && curInflight >= maxInflight {
		return false
	}

	now := c.now()
	if !c.lastEvent.IsZero() && now.Sub(c.lastEvent) < c.opts.RecoveryInterval {
		return false
	}

	stepBytes := max(int64(1), int64(c.opts.MaxRequestBytes-c.opts.MinRequestBytes)/10)
	nextBytes := min(curBytes+stepBytes, maxBytes)
	nextInflight := min(curInflight+1, maxInflight)

	c.effectiveBytes.Store(nextBytes)
	c.effectiveInflight.Store(nextInflight)
	c.lastEvent = now
	return nextBytes != curBytes || nextInflight != curInflight
}

func (c *BackpressureController) decrease(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.lastDecrease.IsZero() && now.Sub(c.lastDecrease) < c.cooldown {
		return false
	}
	c.lastDecrease = now
	c.lastEvent = now

	curBytes := c.effectiveBytes.Load()
	curInflight := c.effectiveInflight.Load()

	nextBytes := max(
		int64(math.Floor(float64(curBytes)*c.opts.DecreaseFactor)),
		int64(c.opts.MinRequestBytes),
	)
	nextInflight := max(
		int64(math.Floor(float64(curInflight)*c.opts.DecreaseFactor)),
		int64(c.opts.MinInflightRequests),
	)

	if nextBytes == curBytes && nextInflight == curInflight {
		return false
	}
	c.effectiveBytes.Store(nextBytes)
	c.effectiveInflight.Store(nextInflight)
	return true
}
