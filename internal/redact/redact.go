// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

// Package redact scrubs sensitive attribute keys and string values from
// OTLP telemetry before the exporter offloads, size-checks, fingerprints,
// or serializes it.
//
// The ordering matters more than the matching. GCS object keys and the
// record_fingerprint / point_fingerprint columns are content-addressed, so a
// secret that is only masked at JSON-encoding time would still be uploaded in
// cleartext and would still leak through its digest. Callers therefore run a
// Redactor as step 0 of every transform and feed the redacted copy to every
// later stage.
//
// Redaction never mutates the batch it is given. Traces, Logs, and Metrics
// scan first and return the input untouched when nothing matches; only when
// a key or value matches is the batch cloned, once, and redacted in place.
// That keeps the no-match hot path allocation-free, and a matching batch
// costs one clone rather than one per record. Resource and
// InstrumentationScope attributes are shared by every record in their group,
// so they are scanned and masked once per group, not once per record.
package redact // import "github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/redact"

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Match types reported to OnRedact. They are the only two values the
// redacted_values metric's match_type label ever takes.
const (
	MatchKey   = "key"
	MatchValue = "value"
)

// DefaultMaskReplacement replaces a blocked key's value or a matched value
// substring when the configuration names no replacement.
const DefaultMaskReplacement = "[REDACTED]"

// DefaultBlockedKeys returns the key names and globs that are blocked unless
// the configuration disables them. Matching is case-insensitive; a name
// containing '*' is a glob.
//
// The list is the exact names credentials travel under (headers, query
// parameters, SDK option names) plus globs for the words that name a secret:
// "secret", "password", "passwd", and "private_key" anywhere in a key, and
// "token" at the end of one. Masking too much costs a column of
// "[REDACTED]"; masking too little leaks, so these globs are deliberately
// broad.
//
// Token is the one word that is not matched in the middle of a key. A
// "*token*" glob would also mask gen_ai.usage.input_tokens,
// gen_ai.usage.output_tokens, and gen_ai.token.type, which are counts and
// enums rather than credentials. The GenAI views read the counts with
// LAX_INT64, so a masked count turns those view columns to NULL. Keys are
// compared lower-cased, so "*token" still covers access_token,
// session_token, x-auth-token, and camelCase forms such as accessToken. A
// key that puts the word first or in the middle, such as token_value or a
// plural "tokens", is not matched; operators add those to blocked_keys.
func DefaultBlockedKeys() []string {
	return []string{
		// Exact header, parameter, and field names.
		"authorization",
		"proxy-authorization",
		"cookie",
		"set-cookie",
		"x-api-key",
		"api_key",
		"api-key",
		"apikey",
		"client_secret",
		"access_token",
		"refresh_token",
		"id_token",
		"token",
		"password",
		"passwd",
		"secret",
		"private_key",
		"credentials",
		// Broad globs: these words name a secret wherever they appear.
		"*secret*",
		"*password*",
		"*passwd*",
		"*private_key*",
		// End-anchored on purpose; see above.
		"*token",
		// Suffix forms of the exact names above, including the semconv
		// http.request.header.* shapes such as ...header.authorization.
		"*_api_key",
		"*-api-key",
		"*.api_key",
		"*apikey",
		"*.authorization",
		"*-authorization",
		"*cookie",
	}
}

// Config builds a Redactor.
type Config struct {
	// BlockedKeys are attribute keys whose entire value is replaced by
	// MaskReplacement. A key is matched case-insensitively; a key containing
	// '*' is a glob where '*' matches any run of characters.
	BlockedKeys []string
	// BlockedValuePatterns are RE2 regular expressions. Every match inside a
	// string value is replaced by MaskReplacement.
	BlockedValuePatterns []string
	// MaskReplacement is the replacement text. Empty means
	// DefaultMaskReplacement.
	MaskReplacement string
	// RedactLogBody applies value patterns (and, for map bodies, key
	// matching) to plog.LogRecord.Body as well as to attributes.
	RedactLogBody bool
	// OnRedact, when set, is called once per redacted batch and match type
	// with the number of keys or values replaced. It never receives key names
	// or values.
	OnRedact func(matchType string, n int)
}

// Redactor applies a compiled Config. The zero value and a nil *Redactor are
// both no-ops, so callers can hold one unconditionally.
type Redactor struct {
	exact map[string]struct{}
	// Globs are split by shape so a single-star suffix or prefix form, such
	// as "*token" or "x-*", is a HasSuffix / HasPrefix instead of a wildcard
	// walk.
	suffixes []string
	prefixes []string
	globs    []string
	// valuePattern is every configured value pattern folded into one
	// alternation, so each string value is scanned once however many
	// patterns are configured. Nil when there are none.
	valuePattern *regexp.Regexp
	mask         string
	logBody      bool
	onRedact     func(matchType string, n int)
}

// CompilePatterns compiles RE2 value patterns, reporting the first invalid
// one by position so a configuration error names the offending entry.
func CompilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for i, p := range patterns {
		if strings.TrimSpace(p) == "" {
			return nil, fmt.Errorf("blocked_value_patterns[%d] is empty", i)
		}
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("blocked_value_patterns[%d] %q is not a valid RE2 regular expression: %w", i, p, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// New compiles cfg. It fails only on an invalid value pattern or an empty
// blocked key, both of which Config validation is expected to have rejected
// already.
func New(cfg Config) (*Redactor, error) {
	// Compile individually first so an invalid entry is reported by position,
	// then fold them into one alternation for execution.
	if _, err := CompilePatterns(cfg.BlockedValuePatterns); err != nil {
		return nil, err
	}
	r := &Redactor{
		exact:    make(map[string]struct{}, len(cfg.BlockedKeys)),
		mask:     cfg.MaskReplacement,
		logBody:  cfg.RedactLogBody,
		onRedact: cfg.OnRedact,
	}
	if len(cfg.BlockedValuePatterns) > 0 {
		parts := make([]string, 0, len(cfg.BlockedValuePatterns))
		for _, p := range cfg.BlockedValuePatterns {
			parts = append(parts, "(?:"+p+")")
		}
		combined, err := regexp.Compile(strings.Join(parts, "|"))
		if err != nil {
			return nil, fmt.Errorf("blocked_value_patterns cannot be combined: %w", err)
		}
		r.valuePattern = combined
	}
	if r.mask == "" {
		r.mask = DefaultMaskReplacement
	}
	for i, k := range cfg.BlockedKeys {
		k = normalizeKey(k)
		if k == "" {
			return nil, fmt.Errorf("blocked_keys[%d] is empty", i)
		}
		stars := strings.Count(k, "*")
		switch {
		case stars == 0:
			r.exact[k] = struct{}{}
		case stars == 1 && strings.HasPrefix(k, "*"):
			r.suffixes = append(r.suffixes, k[1:])
		case stars == 1 && strings.HasSuffix(k, "*"):
			r.prefixes = append(r.prefixes, k[:len(k)-1])
		default:
			r.globs = append(r.globs, k)
		}
	}
	if len(r.exact) == 0 && len(r.suffixes) == 0 && len(r.prefixes) == 0 && len(r.globs) == 0 && r.valuePattern == nil {
		return nil, errors.New("redaction has no blocked keys and no value patterns")
	}
	return r, nil
}

func normalizeKey(k string) string {
	return strings.ToLower(strings.TrimSpace(k))
}

// KeyBlocked reports whether an attribute key would have its value masked.
func (r *Redactor) KeyBlocked(key string) bool {
	if r == nil {
		return false
	}
	k := normalizeKey(key)
	if _, ok := r.exact[k]; ok {
		return true
	}
	for _, s := range r.suffixes {
		if strings.HasSuffix(k, s) {
			return true
		}
	}
	for _, p := range r.prefixes {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	for _, g := range r.globs {
		if wildcardMatch(g, k) {
			return true
		}
	}
	return false
}

// wildcardMatch reports whether s matches pattern, where '*' matches any
// (possibly empty) run of bytes and every other byte matches itself. Both
// arguments are expected to be lower-cased already.
func wildcardMatch(pattern, s string) bool {
	p, i := 0, 0
	star, mark := -1, 0
	for i < len(s) {
		switch {
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, i
			p++
		case p < len(pattern) && pattern[p] == s[i]:
			p++
			i++
		case star >= 0:
			p = star + 1
			mark++
			i = mark
		default:
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// counts accumulates replacements made by one apply pass.
type counts struct {
	keys, values int
}

func (r *Redactor) report(c counts) {
	if r == nil || r.onRedact == nil {
		return
	}
	if c.keys > 0 {
		r.onRedact(MatchKey, c.keys)
	}
	if c.values > 0 {
		r.onRedact(MatchValue, c.values)
	}
}

// Traces returns td unchanged when nothing in it matches, and otherwise a
// redacted deep copy. The input is never mutated.
func (r *Redactor) Traces(td ptrace.Traces) ptrace.Traces {
	if r == nil || !r.walkTraces(td, false, nil) {
		return td
	}
	out := ptrace.NewTraces()
	td.CopyTo(out)
	var c counts
	r.walkTraces(out, true, &c)
	r.report(c)
	return out
}

// Logs returns ld unchanged when nothing in it matches, and otherwise a
// redacted deep copy. The input is never mutated.
func (r *Redactor) Logs(ld plog.Logs) plog.Logs {
	if r == nil || !r.walkLogs(ld, false, nil) {
		return ld
	}
	out := plog.NewLogs()
	ld.CopyTo(out)
	var c counts
	r.walkLogs(out, true, &c)
	r.report(c)
	return out
}

// Metrics returns md unchanged when nothing in it matches, and otherwise a
// redacted deep copy. The input is never mutated.
func (r *Redactor) Metrics(md pmetric.Metrics) pmetric.Metrics {
	if r == nil || !r.walkMetrics(md, false, nil) {
		return md
	}
	out := pmetric.NewMetrics()
	md.CopyTo(out)
	var c counts
	r.walkMetrics(out, true, &c)
	r.report(c)
	return out
}

// RedactTraces redacts td in place and returns how many keys and values were
// replaced. It does not call OnRedact: it is meant for copies the caller has
// already made, such as the single-record payload of a quarantine row, whose
// matches were counted when the batch went through Traces.
func (r *Redactor) RedactTraces(td ptrace.Traces) (keys, values int) {
	if r == nil {
		return 0, 0
	}
	var c counts
	r.walkTraces(td, true, &c)
	return c.keys, c.values
}

// RedactLogs is RedactTraces for logs.
func (r *Redactor) RedactLogs(ld plog.Logs) (keys, values int) {
	if r == nil {
		return 0, 0
	}
	var c counts
	r.walkLogs(ld, true, &c)
	return c.keys, c.values
}

// RedactMetrics is RedactTraces for metrics.
func (r *Redactor) RedactMetrics(md pmetric.Metrics) (keys, values int) {
	if r == nil {
		return 0, 0
	}
	var c counts
	r.walkMetrics(md, true, &c)
	return c.keys, c.values
}

// RedactMap redacts one attribute map in place and returns how many keys and
// values were replaced. It does not call OnRedact.
func (r *Redactor) RedactMap(m pcommon.Map) (keys, values int) {
	if r == nil {
		return 0, 0
	}
	var c counts
	r.walkMap(m, true, &c)
	return c.keys, c.values
}

// The walk functions share one traversal for the dry-run scan and the apply
// pass. With apply false they return at the first match and never touch c;
// with apply true they visit everything, replace, and count into c.

func (r *Redactor) walkTraces(td ptrace.Traces, apply bool, c *counts) (hit bool) {
	for _, rs := range td.ResourceSpans().All() {
		if r.walkMap(rs.Resource().Attributes(), apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
		for _, ss := range rs.ScopeSpans().All() {
			if r.walkMap(ss.Scope().Attributes(), apply, c) {
				if !apply {
					return true
				}
				hit = true
			}
			for _, span := range ss.Spans().All() {
				if r.walkSpan(span, apply, c) {
					if !apply {
						return true
					}
					hit = true
				}
			}
		}
	}
	return hit
}

func (r *Redactor) walkSpan(span ptrace.Span, apply bool, c *counts) (hit bool) {
	if r.walkMap(span.Attributes(), apply, c) {
		if !apply {
			return true
		}
		hit = true
	}
	for _, ev := range span.Events().All() {
		if r.walkMap(ev.Attributes(), apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
	}
	for _, ln := range span.Links().All() {
		if r.walkMap(ln.Attributes(), apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
	}
	return hit
}

func (r *Redactor) walkLogs(ld plog.Logs, apply bool, c *counts) (hit bool) {
	for _, rl := range ld.ResourceLogs().All() {
		if r.walkMap(rl.Resource().Attributes(), apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
		for _, sl := range rl.ScopeLogs().All() {
			if r.walkMap(sl.Scope().Attributes(), apply, c) {
				if !apply {
					return true
				}
				hit = true
			}
			for _, lr := range sl.LogRecords().All() {
				if r.walkMap(lr.Attributes(), apply, c) {
					if !apply {
						return true
					}
					hit = true
				}
				if r.logBody && r.walkValue(lr.Body(), apply, c) {
					if !apply {
						return true
					}
					hit = true
				}
			}
		}
	}
	return hit
}

func (r *Redactor) walkMetrics(md pmetric.Metrics, apply bool, c *counts) (hit bool) {
	for _, rm := range md.ResourceMetrics().All() {
		if r.walkMap(rm.Resource().Attributes(), apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
		for _, sm := range rm.ScopeMetrics().All() {
			if r.walkMap(sm.Scope().Attributes(), apply, c) {
				if !apply {
					return true
				}
				hit = true
			}
			for _, m := range sm.Metrics().All() {
				if r.walkMetric(m, apply, c) {
					if !apply {
						return true
					}
					hit = true
				}
			}
		}
	}
	return hit
}

func (r *Redactor) walkMetric(m pmetric.Metric, apply bool, c *counts) (hit bool) {
	if r.walkMap(m.Metadata(), apply, c) {
		if !apply {
			return true
		}
		hit = true
	}
	point := func(attrs pcommon.Map, exemplars pmetric.ExemplarSlice) bool {
		if r.walkMap(attrs, apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
		for _, ex := range exemplars.All() {
			if r.walkMap(ex.FilteredAttributes(), apply, c) {
				if !apply {
					return true
				}
				hit = true
			}
		}
		return false
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for _, dp := range m.Gauge().DataPoints().All() {
			if point(dp.Attributes(), dp.Exemplars()) {
				return true
			}
		}
	case pmetric.MetricTypeSum:
		for _, dp := range m.Sum().DataPoints().All() {
			if point(dp.Attributes(), dp.Exemplars()) {
				return true
			}
		}
	case pmetric.MetricTypeHistogram:
		for _, dp := range m.Histogram().DataPoints().All() {
			if point(dp.Attributes(), dp.Exemplars()) {
				return true
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		for _, dp := range m.ExponentialHistogram().DataPoints().All() {
			if point(dp.Attributes(), dp.Exemplars()) {
				return true
			}
		}
	case pmetric.MetricTypeSummary:
		for _, dp := range m.Summary().DataPoints().All() {
			if point(dp.Attributes(), pmetric.NewExemplarSlice()) {
				return true
			}
		}
	}
	return hit
}

// walkMap matches keys first: a blocked key has its whole value replaced,
// whatever its type, so a nested map under a blocked key is not descended
// into. Other values are scanned for value patterns.
func (r *Redactor) walkMap(m pcommon.Map, apply bool, c *counts) (hit bool) {
	for k, v := range m.All() {
		if r.KeyBlocked(k) {
			if !apply {
				return true
			}
			v.SetStr(r.mask)
			c.keys++
			hit = true
			continue
		}
		if r.walkValue(v, apply, c) {
			if !apply {
				return true
			}
			hit = true
		}
	}
	return hit
}

func (r *Redactor) walkValue(v pcommon.Value, apply bool, c *counts) (hit bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if r.valuePattern == nil {
			return false
		}
		s := v.Str()
		if !apply {
			return r.valuePattern.MatchString(s)
		}
		// Literal replacement: a mask such as "$1" must not expand.
		out := r.valuePattern.ReplaceAllLiteralString(s, r.mask)
		if out == s {
			return false
		}
		v.SetStr(out)
		c.values++
		return true
	case pcommon.ValueTypeMap:
		return r.walkMap(v.Map(), apply, c)
	case pcommon.ValueTypeSlice:
		for _, el := range v.Slice().All() {
			if r.walkValue(el, apply, c) {
				if !apply {
					return true
				}
				hit = true
			}
		}
		return hit
	}
	// Ints, doubles, bools, bytes, and empty values are never scanned: byte
	// payloads are opaque and the others cannot carry a secret string.
	return false
}
