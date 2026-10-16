// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

// This file checks the GenAI companion views (v_gen_ai_spans, v_gen_ai_logs,
// v_gen_ai_metrics, v_gen_ai_metric_exemplars, v_gen_ai_model_costs and
// v_gen_ai_sessions) against the rows the exporters really write, using the
// golden fixtures under testdata/otlp.
//
// The tests cannot run SQL: no BigQuery is available to them. Instead a
// reference model, written in Go, evaluates each view over the rows that the
// exporters sent to the BigQuery fake, and the tests assert exact numbers on
// the model's output. The model is a hand translation of the view SQL, so a
// second test (TestViewModelMatchesViewSQL) renders every clause the model
// translates back into SQL and requires the view definitions to contain it: a
// change to either side fails a test until the other follows.
//
// What this proves is that the views, as written, express the intended rules
// (each model call is counted once, a session is a service and a UTC date, a
// log joins only a span near it, and so on) and that the rows the exporters
// write carry what those rules read. It does not prove that BigQuery executes
// the SQL as the model does, and it does not model the percentile columns of
// v_gen_ai_model_costs, which are approximate by definition.

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/testfakes/bqfake"
)

// The views are rendered over fixed names: only the shape of the SQL matters.
const (
	confProject = "p"
	confDataset = "d"
	confSpans   = "otel_spans"
	confLogs    = "otel_logs"
	confMetrics = "otel_metrics"
)

// A dedup view keeps, from each group of rows that agree on the partition
// columns, the one row that comes first when the group is ordered by the newest
// columns, each descending.
type dedupSpec struct {
	partition []string
	newest    []string
}

var (
	spansDedup = dedupSpec{
		partition: []string{"start_time", "service_name", "trace_id", "span_id", "name"},
		newest:    []string{"end_time_unix_nano", "receive_time_unix_nano"},
	}
	logsDedup = dedupSpec{
		partition: []string{"timestamp", "time_unix_nano", "service_name", "trace_id", "span_id", "severity_number", "log_name", "insert_id"},
		newest:    []string{"receive_time_unix_nano"},
	}
	metricsDedup = dedupSpec{
		partition: []string{"timestamp", "time_unix_nano", "service_name", "metric_name", "metric_type", "point_fingerprint"},
		newest:    []string{"receive_time_unix_nano"},
	}
)

const (
	// v_gen_ai_logs enriches a log record with the span it belongs to only when
	// the span started from logJoinBefore before to logJoinAfter after the
	// record's timestamp, ends included.
	logJoinBefore = 24 * time.Hour
	logJoinAfter  = time.Hour

	// v_gen_ai_metric_exemplars enriches an exemplar with the span it points to
	// only when the span started from exemplarJoinBefore before to
	// exemplarJoinAfter after the metric point's timestamp, ends included.
	exemplarJoinBefore = 24 * time.Hour
	exemplarJoinAfter  = time.Hour

	// v_gen_ai_metrics keeps the metrics whose name matches this LIKE pattern,
	// and v_gen_ai_logs the log records whose event name does.
	genAIMetricNamePattern = "gen_ai.%"
	genAILogEventPattern   = "gen_ai.%"
)

// The operation names that make a span an inference span, and the ones that
// make it an agent or tool span, as the rollup views read them.
var (
	inferenceOps = []string{"chat", "generate_content", "text_completion", "embeddings"}
	agentToolOps = []string{"invoke_agent", "create_agent", "execute_tool"}
)

// attrStep is one attribute lookup inside an attrChain, either from the row's
// attributes column or from its resource.attributes column.
type attrStep struct {
	resource bool
	key      string
}

// attrChain is one projection of a view out of the plain-JSON attributes
// column (and, when resourceKeys or steps is non-empty, resource.attributes):
// the first non-NULL of steps (when set) or of keys followed by resourceKeys,
// then of the fallback column.
type attrChain struct {
	alias        string
	keys         []string
	resourceKeys []string   // keys read from resource.attributes after attributes
	steps        []attrStep // interleaved attributes / resource.attributes order when set
	asInt        bool       // LAX_INT64(attributes['key']) rather than JSON_VALUE(attributes, '$."key"')
	asFloat      bool       // LAX_FLOAT64(attributes['key']) rather than JSON_VALUE(attributes, '$."key"')
	fallback     string     // a column of the row that ends the COALESCE
}

// sql renders the select item the view is expected to hold for the chain over
// the given attribute sources: a table alias prefix such as "l." and "s.", or
// "" for a view with one source. Sources are tried in order, and within a
// source either steps are tried in order, or keys are tried in order followed
// by resourceKeys on resource.attributes.
func (c attrChain) sql(srcs ...string) string {
	var els []string
	if len(c.steps) > 0 {
		for _, src := range srcs {
			for _, st := range c.steps {
				prefix := src
				if st.resource {
					prefix += "resource."
				}
				els = append(els, fmt.Sprintf("JSON_VALUE(%sattributes, '$.\"%s\"')", prefix, st.key))
			}
		}
	} else {
		for _, src := range srcs {
			for _, k := range c.keys {
				switch {
				case c.asInt:
					els = append(els, fmt.Sprintf("LAX_INT64(%sattributes['%s'])", src, k))
				case c.asFloat:
					els = append(els, fmt.Sprintf("LAX_FLOAT64(%sattributes['%s'])", src, k))
				default:
					els = append(els, fmt.Sprintf("JSON_VALUE(%sattributes, '$.\"%s\"')", src, k))
				}
			}
		}
		for _, src := range srcs {
			for _, k := range c.resourceKeys {
				els = append(els, fmt.Sprintf("JSON_VALUE(%sresource.attributes, '$.\"%s\"')", src, k))
			}
		}
	}
	if c.fallback != "" {
		els = append(els, c.fallback)
	}
	expr := els[0]
	if len(els) > 1 {
		expr = "COALESCE(" + strings.Join(els, ", ") + ")"
	}
	return expr + " AS " + c.alias
}

// eval is the chain's value for a row whose attributes, in source order, are
// srcs: a string, an int64, a float64, or nil for NULL.
func (c attrChain) eval(row map[string]any, srcs ...attrs) any {
	if len(c.steps) > 0 {
		var resAttrs attrs
		var resParsed bool
		for _, src := range srcs {
			for _, st := range c.steps {
				bag := src
				if st.resource {
					if !resParsed {
						resAttrs = parseResourceAttrs(row)
						resParsed = true
					}
					bag = resAttrs
				}
				if v, ok := bag.jsonValue(st.key); ok {
					return v
				}
			}
		}
	}
	for _, src := range srcs {
		for _, k := range c.keys {
			switch {
			case c.asInt:
				if n, ok := src.laxInt64(k); ok {
					return n
				}
			case c.asFloat:
				if f, ok := src.laxFloat64(k); ok {
					return f
				}
			default:
				if v, ok := src.jsonValue(k); ok {
					return v
				}
			}
		}
	}
	if len(c.resourceKeys) > 0 {
		resAttrs := parseResourceAttrs(row)
		for _, k := range c.resourceKeys {
			if v, ok := resAttrs.jsonValue(k); ok {
				return v
			}
		}
	}
	if c.fallback != "" {
		if v, ok := row[c.fallback]; ok {
			return v
		}
	}
	return nil
}

// spanChains are the columns of v_gen_ai_spans that the other views, the
// rollups above all, read out of a span's attributes.
var spanChains = []attrChain{
	{alias: "gen_ai_operation_name", keys: []string{"gen_ai.operation.name"}},
	{alias: "gen_ai_system", keys: []string{"gen_ai.system", "gen_ai.provider.name"}},
	{alias: "gen_ai_request_model", keys: []string{"gen_ai.request.model"}},
	{alias: "gen_ai_response_model", keys: []string{"gen_ai.response.model"}},
	{alias: "gen_ai_agent_name", keys: []string{"gen_ai.agent.name", "gcp.vertex.agent.name"}, fallback: "service_name"},
	{alias: "gen_ai_conversation_id", keys: []string{"gen_ai.conversation.id", "gcp.vertex.agent.session_id"}},
	{alias: "gen_ai_usage_input_tokens", keys: []string{"gen_ai.usage.input_tokens"}, asInt: true},
	{alias: "gen_ai_usage_output_tokens", keys: []string{"gen_ai.usage.output_tokens"}, asInt: true},
	{alias: "gen_ai_tool_name", keys: []string{"gen_ai.tool.name"}},
	{alias: "adk_session_id", keys: []string{"gcp.vertex.agent.session_id", "gen_ai.conversation.id"}},
	{alias: "adk_invocation_id", keys: []string{"gcp.vertex.agent.invocation_id"}},
	{alias: "adk_user_id", keys: []string{"user.id", "gen_ai.user.id"}},
	{alias: "gen_ai_provider_name", keys: []string{"gen_ai.provider.name"}},
	{alias: "gen_ai_usage_cache_read_input_tokens", keys: []string{"gen_ai.usage.cache_read.input_tokens", "gen_ai.usage.cache_read_input_tokens"}, asInt: true},
	{alias: "gen_ai_usage_cache_creation_input_tokens", keys: []string{"gen_ai.usage.cache_creation.input_tokens", "gen_ai.usage.cache_creation_input_tokens"}, asInt: true},
	{alias: "gen_ai_usage_reasoning_tokens", keys: []string{"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.reasoning_tokens"}, asInt: true},
	{alias: "error_type", keys: []string{"error.type"}},
	{alias: "gen_ai_main_agent_id", keys: []string{"gen_ai.main_agent.id"}, resourceKeys: []string{"gen_ai.main_agent.id"}},
	{alias: "gen_ai_main_agent_name", keys: []string{"gen_ai.main_agent.name"}, resourceKeys: []string{"gen_ai.main_agent.name"}},
	{alias: "gen_ai_main_agent_description", keys: []string{"gen_ai.main_agent.description"}, resourceKeys: []string{"gen_ai.main_agent.description"}},
	{alias: "gen_ai_agent_version", keys: []string{"gen_ai.agent.version"}, resourceKeys: []string{"gen_ai.agent.version"}},
	{alias: "gen_ai_response_id", keys: []string{"gen_ai.response.id"}},
	{alias: "gen_ai_response_time_to_first_chunk", keys: []string{"gen_ai.response.time_to_first_chunk"}, asFloat: true},
}

// logChains are the columns of v_gen_ai_logs enriched from the record's own
// attributes first and the span's second. Unlike the span view, the agent name
// has no service_name fallback.
var logChains = []attrChain{
	{alias: "gen_ai_operation_name", keys: []string{"gen_ai.operation.name"}},
	{alias: "gen_ai_system", keys: []string{"gen_ai.system", "gen_ai.provider.name"}},
	{alias: "gen_ai_agent_name", keys: []string{"gen_ai.agent.name", "gcp.vertex.agent.name"}},
	{alias: "adk_session_id", keys: []string{"gcp.vertex.agent.session_id", "gen_ai.conversation.id"}},
	{alias: "adk_invocation_id", keys: []string{"gcp.vertex.agent.invocation_id"}},
	{alias: "adk_event_id", keys: []string{"gcp.vertex.agent.event_id"}},
	{alias: "adk_user_id", keys: []string{"user.id", "gen_ai.user.id"}},
}

// logOnlyErrorChain reads error.type from the log record's own attributes
// without falling back to the joined span's attributes.
var logOnlyErrorChain = attrChain{alias: "error_type", keys: []string{"error.type"}}

// metricChains are the GenAI attributes v_gen_ai_metrics promotes.
var metricChains = []attrChain{
	{alias: "gen_ai_operation_name", keys: []string{"gen_ai.operation.name"}},
	{alias: "gen_ai_system", keys: []string{"gen_ai.system", "gen_ai.provider.name"}},
	{alias: "gen_ai_provider_name", keys: []string{"gen_ai.provider.name"}},
	{alias: "gen_ai_request_model", keys: []string{"gen_ai.request.model"}},
	{alias: "gen_ai_response_model", keys: []string{"gen_ai.response.model"}},
	{alias: "gen_ai_token_type", keys: []string{"gen_ai.token.type"}},
	{alias: "error_type", keys: []string{"error.type"}},
	{alias: "gen_ai_token_modality", keys: []string{"gen_ai.token.modality"}},
	{alias: "gen_ai_agent_name", keys: []string{"gen_ai.agent.name", "gen_ai.main_agent.name"}, resourceKeys: []string{"gen_ai.main_agent.name"}},
}

// exemplarSpanChains are the columns of v_gen_ai_metric_exemplars read from
// the joined span's attributes and resource.attributes.
var exemplarSpanChains = []attrChain{
	{
		alias: "span_gen_ai_agent_name",
		steps: []attrStep{
			{key: "gen_ai.agent.name"},
			{key: "gcp.vertex.agent.name"},
			{resource: true, key: "gen_ai.agent.name"},
			{key: "gen_ai.main_agent.name"},
			{resource: true, key: "gen_ai.main_agent.name"},
		},
	},
	{alias: "span_gen_ai_tool_name", keys: []string{"gen_ai.tool.name"}},
	{alias: "adk_session_id", keys: []string{"gcp.vertex.agent.session_id", "gen_ai.conversation.id"}},
	{alias: "span_error_type", keys: []string{"error.type"}},
}

// attrs is the parsed plain-JSON attributes column of one row.
type attrs map[string]any

// parseAttrs parses the attributes column of a decoded row. Numbers stay
// json.Number so that a count above 2^53 is not rounded.
func parseAttrs(t testing.TB, row map[string]any) attrs {
	t.Helper()
	raw, ok := row["attributes"].(string)
	if !ok {
		return attrs{}
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var a attrs
	if err := dec.Decode(&a); err != nil {
		t.Fatalf("attributes column is not a JSON object (%v): %s", err, raw)
	}
	return a
}

// parseResourceAttrs parses resource.attributes of a decoded row, returning an
// empty map when the row or its resource struct lacks attributes.
func parseResourceAttrs(row map[string]any) attrs {
	res, ok := row["resource"].(map[string]any)
	if !ok {
		return attrs{}
	}
	raw, ok := res["attributes"].(string)
	if !ok || raw == "" {
		return attrs{}
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var a attrs
	if err := dec.Decode(&a); err != nil {
		return attrs{}
	}
	return a
}

// jsonValue is JSON_VALUE(attributes, '$."key"'): a scalar as text, and NULL
// for a missing key, a JSON null, an array or an object.
func (a attrs) jsonValue(key string) (string, bool) {
	switch v := a[key].(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case bool:
		return strconv.FormatBool(v), true
	}
	return "", false
}

// laxInt64 is LAX_INT64(attributes['key']) for the whole numbers the tests feed
// it, written as a JSON number or as a string. How BigQuery treats a count with
// a fractional part is not modeled: it is NULL here.
func (a attrs) laxInt64(key string) (int64, bool) {
	var text string
	switch v := a[key].(type) {
	case json.Number:
		text = v.String()
	case string:
		text = v
	default:
		return 0, false
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil && f == math.Trunc(f) && math.Abs(f) < 9.2e18 {
		return int64(f), true
	}
	return 0, false
}

// laxFloat64 is LAX_FLOAT64(attributes['key']) for numeric or string attributes.
func (a attrs) laxFloat64(key string) (float64, bool) {
	switch v := a[key].(type) {
	case json.Number:
		f, err := v.Float64()
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	case float64:
		return v, !math.IsNaN(v) && !math.IsInf(v, 0)
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	default:
		return 0, false
	}
}

// colString reads a STRING column of a decoded row; ok is false for NULL.
func colString(row map[string]any, col string) (string, bool) {
	v, ok := row[col].(string)
	return v, ok
}

// colInt reads an INT64 column, or a TIMESTAMP one in microseconds.
func colInt(row map[string]any, col string) (int64, bool) {
	v, ok := row[col].(int64)
	return v, ok
}

// sqlLike reports whether s matches the SQL LIKE pattern: % stands for any run
// of characters, _ for any one character, and every other character for itself.
func sqlLike(pattern, s string) bool {
	var re strings.Builder
	re.WriteString("(?s)^")
	for _, r := range pattern {
		switch r {
		case '%':
			re.WriteString(".*")
		case '_':
			re.WriteString(".")
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString("$")
	return regexp.MustCompile(re.String()).MatchString(s)
}

// logJoinsSpan reports whether the ON clause of v_gen_ai_logs holds for a log
// row and a span row of the dedup views: equal trace and span IDs (a NULL
// never equals anything), service names that are equal or both NULL, and a
// span that started within the bounded window around the record's timestamp.
func logJoinsSpan(l, s map[string]any) bool {
	for _, col := range []string{"trace_id", "span_id"} {
		lv, lok := colString(l, col)
		sv, sok := colString(s, col)
		if !lok || !sok || lv != sv {
			return false
		}
	}
	lsvc, lok := colString(l, "service_name")
	ssvc, sok := colString(s, "service_name")
	if lok != sok || lsvc != ssvc {
		return false
	}
	logAt, lok := colInt(l, "timestamp")
	spanStart, sok := colInt(s, "start_time")
	if !lok || !sok {
		return false
	}
	return spanStart >= logAt-logJoinBefore.Microseconds() && spanStart <= logAt+logJoinAfter.Microseconds()
}

// exemplarJoinsSpan reports whether the ON clause of v_gen_ai_metric_exemplars
// holds for a metric row m, an unnested exemplar ex, and a span row s of the
// dedup view: equal trace and span IDs, service names on m and s that are
// equal or both NULL, and a span that started within the bounded window around
// the metric point's timestamp.
func exemplarJoinsSpan(m, ex, s map[string]any) bool {
	for _, col := range []string{"trace_id", "span_id"} {
		ev, eok := colString(ex, col)
		sv, sok := colString(s, col)
		if !eok || !sok || ev != sv {
			return false
		}
	}
	msvc, mok := colString(m, "service_name")
	ssvc, sok := colString(s, "service_name")
	if mok != sok || msvc != ssvc {
		return false
	}
	metricAt, mok := colInt(m, "timestamp")
	spanStart, sok := colInt(s, "start_time")
	if !mok || !sok {
		return false
	}
	return spanStart >= metricAt-exemplarJoinBefore.Microseconds() && spanStart <= metricAt+exemplarJoinAfter.Microseconds()
}

// sql is the window that the dedup view is expected to hold.
func (d dedupSpec) sql() string {
	order := make([]string, len(d.newest))
	for i, col := range d.newest {
		order[i] = col + " DESC"
	}
	return "PARTITION BY " + strings.Join(d.partition, ", ") + " ORDER BY " + strings.Join(order, ", ")
}

// newer reports whether row a comes before row b in the view's ORDER BY.
func (d dedupSpec) newer(a, b map[string]any) bool {
	for _, col := range d.newest {
		av, _ := colInt(a, col)
		bv, _ := colInt(b, col)
		if av != bv {
			return av > bv
		}
	}
	return false
}

// apply is the dedup view over the rows of its table: it returns the survivors
// in the order their groups first appeared.
func (d dedupSpec) apply(rows []map[string]any) []map[string]any {
	newest := make(map[string]map[string]any)
	var order []string
	for _, r := range rows {
		var key strings.Builder
		for _, col := range d.partition {
			fmt.Fprintf(&key, "%T:%v|", r[col], r[col])
		}
		k := key.String()
		cur, seen := newest[k]
		if !seen {
			order = append(order, k)
		}
		if !seen || d.newer(r, cur) {
			newest[k] = r
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		out = append(out, newest[k])
	}
	return out
}

// genAISpansView is v_gen_ai_spans over the rows of the span table.
func genAISpansView(t testing.TB, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, s := range spansDedup.apply(rawSpans) {
		a := parseAttrs(t, s)
		row := make(map[string]any)
		for _, c := range spanChains {
			if v := c.eval(s, a); v != nil {
				row[c.alias] = v
			}
		}
		_, hasOp := row["gen_ai_operation_name"]
		_, hasSystem := row["gen_ai_system"]
		_, hasInvocation := row["adk_invocation_id"]
		_, hasMainAgentName := a.jsonValue("gen_ai.main_agent.name")
		_, hasMainAgentID := a.jsonValue("gen_ai.main_agent.id")
		if !hasOp && !hasSystem && !hasInvocation && !hasMainAgentName && !hasMainAgentID {
			continue
		}
		for _, col := range []string{"trace_id", "span_id", "parent_span_id", "service_name", "start_time", "end_time", "duration_nano"} {
			if v, ok := s[col]; ok {
				row[col] = v
			}
		}
		if st, ok := s["status"].(map[string]any); ok {
			if code, ok := st["code"]; ok {
				row["status_code"] = code
			}
		}
		if ns, ok := colInt(s, "duration_nano"); ok {
			row["duration_ms"] = math.Round(float64(ns)/1e6*1000) / 1000
		}
		out = append(out, row)
	}
	return out
}

// rollupWindow is the window that v_gen_ai_model_costs and v_gen_ai_sessions
// run over their one read of v_gen_ai_spans to find the token parents. A span
// is a token parent when a span that reports input or output tokens, in the
// same partition of the window, names it as its parent. keys are the PARTITION
// BY columns.
type rollupWindow struct {
	keys []string
}

var (
	// costsWindow partitions by usage hour, service and trace.
	costsWindow = rollupWindow{keys: []string{"usage_hour", "service_name", "trace_id"}}
	// sessionsWindow partitions by service, UTC date, session and trace. A span
	// that has no session id falls in a partition whose session is NULL, and no
	// span that has a session id is in that partition, so such a span neither
	// reports tokens to its parent nor hides the parent. The view drops those
	// spans before the window runs so that the window reads fewer rows. That
	// changes no row, so the model needs no filter of its own: the SQL text
	// checks pin it.
	sessionsWindow = rollupWindow{keys: []string{"service_name", "session_start_date", "session_id", "trace_id"}}
)

// sessionID is COALESCE(adk_session_id, gen_ai_conversation_id).
func sessionID(s map[string]any) (string, bool) {
	if id, ok := colString(s, "adk_session_id"); ok {
		return id, true
	}
	return colString(s, "gen_ai_conversation_id")
}

// windowKey is the value of one PARTITION BY column for a span, NULL as nil.
func windowKey(s map[string]any, key string) any {
	switch key {
	case "usage_hour":
		start, _ := colInt(s, "start_time")
		return start - start%time.Hour.Microseconds()
	case "session_start_date":
		start, _ := colInt(s, "start_time")
		return utcDate(start)
	case "session_id":
		id, ok := sessionID(s)
		return nilUnless(ok, id)
	}
	return s[key]
}

// partition renders the PARTITION BY values of a span as one key. A NULL is a
// value of its own, as it is in a window.
func (w rollupWindow) partition(s map[string]any) string {
	vals := make([]any, len(w.keys))
	for i, key := range w.keys {
		vals[i] = windowKey(s, key)
	}
	return groupKey(vals...)
}

// tokenParentKey is a span ID within one partition of a rollup window.
type tokenParentKey struct{ partition, span string }

// tokenParents is the ARRAY_AGG window: for each partition, the parent_span_id
// of every span that has a parent and reports input or output tokens.
func (w rollupWindow) tokenParents(spans []map[string]any) map[tokenParentKey]bool {
	parents := make(map[tokenParentKey]bool)
	for _, s := range spans {
		parent, hasParent := colString(s, "parent_span_id")
		_, hasIn := colInt(s, "gen_ai_usage_input_tokens")
		_, hasOut := colInt(s, "gen_ai_usage_output_tokens")
		if hasParent && (hasIn || hasOut) {
			parents[tokenParentKey{w.partition(s), parent}] = true
		}
	}
	return parents
}

// tokenLeaf is the token leaf test: no span that reports tokens names the span
// as its parent from within the span's own partition.
func (w rollupWindow) tokenLeaf(s map[string]any, parents map[tokenParentKey]bool) bool {
	span, _ := colString(s, "span_id")
	return !parents[tokenParentKey{w.partition(s), span}]
}

// isLLMCall is the predicate the rollup views share: a span is one LLM call
// when it is a token leaf, it is an inference span (an inference operation or
// a request model), and it is not an agent or tool span unless it reports
// tokens itself.
func isLLMCall(s map[string]any, tokenLeaf bool) bool {
	if !tokenLeaf {
		return false
	}
	op, hasOp := colString(s, "gen_ai_operation_name")
	_, hasModel := colString(s, "gen_ai_request_model")
	if (!hasOp || !slices.Contains(inferenceOps, op)) && !hasModel {
		return false
	}
	if !slices.Contains(agentToolOps, op) {
		return true
	}
	_, hasIn := colInt(s, "gen_ai_usage_input_tokens")
	_, hasOut := colInt(s, "gen_ai_usage_output_tokens")
	return hasIn || hasOut
}

// intOrZero is COALESCE(column, 0).
func intOrZero(s map[string]any, col string) int64 {
	v, _ := colInt(s, col)
	return v
}

// groupKey renders values, NULL included, as one grouping key.
func groupKey(vals ...any) string {
	var b strings.Builder
	for _, v := range vals {
		fmt.Fprintf(&b, "%T:%v|", v, v)
	}
	return b.String()
}

// topErrorTypes models the rollup views' outer ARRAY(SELECT AS STRUCT x.value, x.count FROM UNNEST(raw_top_error_types) AS x WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5)
// over raw_top_error_types = APPROX_TOP_COUNT(error_type, 6): an array of
// {"value": string, "count": int64} entries for non-NULL error_type values,
// ordered by count descending and then value ascending. BigQuery rejects
// UNNEST(APPROX_TOP_COUNT(...)) inside the aggregating SELECT and counts NULL
// as a bucket, so the SQL computes 6 buckets in the aggregating CTE and drops
// the NULL bucket in the outer SELECT.
func topErrorTypes(counts map[string]int64, limit int) []map[string]any {
	if len(counts) == 0 {
		return []map[string]any{}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]map[string]any, len(keys))
	for i, k := range keys {
		out[i] = map[string]any{"value": k, "count": counts[k]}
	}
	return out
}

// modelCostsView is v_gen_ai_model_costs over the rows of v_gen_ai_spans,
// without its percentile columns.
func modelCostsView(spans []map[string]any) []map[string]any {
	parents := costsWindow.tokenParents(spans)
	groups := make(map[string]map[string]any)
	errCounts := make(map[string]map[string]int64)
	var order []string
	for _, s := range spans {
		if !isLLMCall(s, costsWindow.tokenLeaf(s, parents)) {
			continue
		}
		start, _ := colInt(s, "start_time")
		hour := start - start%time.Hour.Microseconds()
		provider, hasProvider := colString(s, "gen_ai_provider_name")
		if !hasProvider {
			provider, hasProvider = colString(s, "gen_ai_system")
		}
		model, hasModel := colString(s, "gen_ai_response_model")
		if !hasModel {
			model, hasModel = colString(s, "gen_ai_request_model")
		}
		key := groupKey(hour, s["service_name"], s["gen_ai_agent_name"], nilUnless(hasProvider, provider), nilUnless(hasModel, model))
		g, ok := groups[key]
		if !ok {
			g = map[string]any{"usage_hour": hour}
			for _, col := range []string{"service_name", "gen_ai_agent_name"} {
				if v, ok := s[col]; ok {
					g[col] = v
				}
			}
			if hasProvider {
				g["provider"] = provider
			}
			if hasModel {
				g["model"] = model
			}
			for _, col := range []string{"call_count", "error_count", "total_input_tokens", "total_output_tokens",
				"total_cache_read_input_tokens", "total_cache_creation_input_tokens", "total_reasoning_tokens",
				"typed_error_count"} {
				g[col] = int64(0)
			}
			groups[key] = g
			errCounts[key] = make(map[string]int64)
			order = append(order, key)
		}
		g["call_count"] = g["call_count"].(int64) + 1
		if code, _ := colInt(s, "status_code"); code == 2 {
			g["error_count"] = g["error_count"].(int64) + 1
		}
		if et, ok := colString(s, "error_type"); ok {
			g["typed_error_count"] = g["typed_error_count"].(int64) + 1
			errCounts[key][et]++
		}
		for col, src := range map[string]string{
			"total_input_tokens":                "gen_ai_usage_input_tokens",
			"total_output_tokens":               "gen_ai_usage_output_tokens",
			"total_cache_read_input_tokens":     "gen_ai_usage_cache_read_input_tokens",
			"total_cache_creation_input_tokens": "gen_ai_usage_cache_creation_input_tokens",
			"total_reasoning_tokens":            "gen_ai_usage_reasoning_tokens",
		} {
			g[col] = g[col].(int64) + intOrZero(s, src)
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		g := groups[k]
		g["top_error_types"] = topErrorTypes(errCounts[k], 5)
		out = append(out, g)
	}
	return out
}

func nilUnless(ok bool, v string) any {
	if !ok {
		return nil
	}
	return v
}

// utcDate is DATE(timestamp) for a timestamp in microseconds, as YYYY-MM-DD.
func utcDate(micros int64) string {
	return time.UnixMicro(micros).UTC().Format("2006-01-02")
}

// sessionsView is v_gen_ai_sessions over the rows of v_gen_ai_spans. ANY_VALUE
// may return any value of its group, so user_id and agent_name come back as
// the sorted set of values ANY_VALUE may pick, in user_id_candidates and
// agent_name_candidates.
func sessionsView(spans []map[string]any) []map[string]any {
	parents := sessionsWindow.tokenParents(spans)
	type acc struct {
		row                   map[string]any
		traces, invocations   map[string]bool
		users, agents         map[string]bool
		minStart, maxEnd      int64
		haveStart, haveEnd    bool
		toolCalls, errorCount int64
		typedErrorCount       int64
		errorTypes            map[string]int64
	}
	groups := make(map[string]*acc)
	var order []string
	for _, s := range spans {
		session, hasSession := sessionID(s)
		if !hasSession {
			continue
		}
		start, _ := colInt(s, "start_time")
		date := utcDate(start)
		key := groupKey(s["service_name"], date, session)
		g, ok := groups[key]
		if !ok {
			g = &acc{
				row:         map[string]any{"session_id": session, "session_start_date": date},
				traces:      make(map[string]bool),
				invocations: make(map[string]bool),
				users:       make(map[string]bool),
				agents:      make(map[string]bool),
				errorTypes:  make(map[string]int64),
			}
			if v, ok := s["service_name"]; ok {
				g.row["service_name"] = v
			}
			for _, col := range []string{"total_input_tokens", "total_output_tokens", "total_cache_read_input_tokens",
				"total_reasoning_tokens", "total_cache_creation_input_tokens", "llm_call_count", "agent_invocation_count"} {
				g.row[col] = int64(0)
			}
			groups[key] = g
			order = append(order, key)
		}
		if trace, ok := colString(s, "trace_id"); ok {
			g.traces[trace] = true
		}
		if inv, ok := colString(s, "adk_invocation_id"); ok {
			g.invocations[inv] = true
		}
		if u, ok := colString(s, "adk_user_id"); ok {
			g.users[u] = true
		}
		if a, ok := colString(s, "gen_ai_agent_name"); ok {
			g.agents[a] = true
		}
		if !g.haveStart || start < g.minStart {
			g.minStart, g.haveStart = start, true
		}
		if end, ok := colInt(s, "end_time"); ok && (!g.haveEnd || end > g.maxEnd) {
			g.maxEnd, g.haveEnd = end, true
		}
		op, hasOp := colString(s, "gen_ai_operation_name")
		_, hasTool := colString(s, "gen_ai_tool_name")
		if (hasOp && op == "execute_tool") || hasTool {
			g.toolCalls++
		}
		if code, _ := colInt(s, "status_code"); code == 2 {
			g.errorCount++
		}
		if et, ok := colString(s, "error_type"); ok {
			g.typedErrorCount++
			g.errorTypes[et]++
		}
		leaf := sessionsWindow.tokenLeaf(s, parents)
		if leaf { // is_token_leaf
			for col, src := range map[string]string{
				"total_input_tokens":                "gen_ai_usage_input_tokens",
				"total_output_tokens":               "gen_ai_usage_output_tokens",
				"total_cache_read_input_tokens":     "gen_ai_usage_cache_read_input_tokens",
				"total_reasoning_tokens":            "gen_ai_usage_reasoning_tokens",
				"total_cache_creation_input_tokens": "gen_ai_usage_cache_creation_input_tokens",
			} {
				g.row[col] = g.row[col].(int64) + intOrZero(s, src)
			}
		}
		if isLLMCall(s, leaf) {
			g.row["llm_call_count"] = g.row["llm_call_count"].(int64) + 1
		}
		if hasOp && op == "invoke_agent" {
			g.row["agent_invocation_count"] = g.row["agent_invocation_count"].(int64) + 1
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		g := groups[k]
		g.row["session_start_time"] = g.minStart
		g.row["session_end_time"] = g.maxEnd
		g.row["session_duration_ms"] = (g.maxEnd - g.minStart) / 1000
		g.row["turn_count"] = int64(len(g.traces))
		g.row["invocation_count"] = int64(len(g.invocations))
		g.row["tool_call_count"] = g.toolCalls
		g.row["error_count"] = g.errorCount
		g.row["typed_error_count"] = g.typedErrorCount
		g.row["top_error_types"] = topErrorTypes(g.errorTypes, 5)
		g.row["user_id_candidates"] = sortedKeys(g.users)
		g.row["agent_name_candidates"] = sortedKeys(g.agents)
		out = append(out, g.row)
	}
	return out
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// genAILogsView is v_gen_ai_logs over the rows of the log and span tables, as
// far as the tests read it. A log record left-joins every span of the dedup
// view that satisfies logJoinsSpan, or none.
func genAILogsView(t testing.TB, rawLogs, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	spans := spansDedup.apply(rawSpans)
	var out []map[string]any
	for _, l := range logsDedup.apply(rawLogs) {
		la := parseAttrs(t, l)
		event, hasEvent := colString(l, "event_name")
		if !hasEvent || event == "" {
			event, hasEvent = la.jsonValue("event.name")
		}
		_, hasSystem := la.jsonValue("gen_ai.system")
		_, hasProvider := la.jsonValue("gen_ai.provider.name")
		_, hasInvocation := la.jsonValue("gcp.vertex.agent.invocation_id")
		if (!hasEvent || !sqlLike(genAILogEventPattern, event)) && !hasSystem && !hasProvider && !hasInvocation {
			continue
		}

		matches := []map[string]any{nil}
		if joined := slices.DeleteFunc(slices.Clone(spans), func(s map[string]any) bool { return !logJoinsSpan(l, s) }); len(joined) > 0 {
			matches = joined
		}
		for _, s := range matches {
			sources := []attrs{la}
			if s != nil {
				sources = append(sources, parseAttrs(t, s))
			}
			row := make(map[string]any)
			for _, col := range []string{"timestamp", "trace_id", "span_id", "service_name", "attributes"} {
				if v, ok := l[col]; ok {
					row[col] = v
				}
			}
			if hasEvent {
				row["event_name"] = event
			}
			for _, c := range logChains {
				if v := c.eval(l, sources...); v != nil {
					row[c.alias] = v
				}
			}
			if v := logOnlyErrorChain.eval(l, la); v != nil {
				row[logOnlyErrorChain.alias] = v
			}
			out = append(out, row)
		}
	}
	return out
}

// inferenceUsageDimension maps a genai-dev gen_ai.client.inference.usage.* metric
// name to its token dimension label, or falls back to gen_ai.token.type.
func inferenceUsageDimension(metricName string, a attrs) (string, bool) {
	switch metricName {
	case "gen_ai.client.inference.usage.input_tokens":
		return "input", true
	case "gen_ai.client.inference.usage.output_tokens":
		return "output", true
	case "gen_ai.client.inference.usage.cache_read.input_tokens":
		return "cache_read_input", true
	case "gen_ai.client.inference.usage.cache_write.input_tokens":
		return "cache_write_input", true
	case "gen_ai.client.inference.usage.reasoning.output_tokens":
		return "reasoning_output", true
	default:
		return a.jsonValue("gen_ai.token.type")
	}
}

// genAIMetricsView is v_gen_ai_metrics over the rows of the metrics table. The
// point values and the raw columns pass through, so the model keeps every
// column of the dedup row and adds the promoted ones.
func genAIMetricsView(t testing.TB, rawMetrics []map[string]any) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range metricsDedup.apply(rawMetrics) {
		name, _ := colString(m, "metric_name")
		if !sqlLike(genAIMetricNamePattern, name) {
			continue
		}
		a := parseAttrs(t, m)
		row := make(map[string]any, len(m))
		for k, v := range m {
			row[k] = v
		}
		for _, c := range metricChains {
			if v := c.eval(m, a); v != nil {
				row[c.alias] = v
			}
		}
		if dim, ok := inferenceUsageDimension(name, a); ok {
			row["gen_ai_inference_usage_dimension"] = dim
		}
		out = append(out, row)
	}
	return out
}

// compareExemplarSpanCandidate orders the rows of one exemplar slot as the
// QUALIFY window of v_gen_ai_metric_exemplars does: by the joined span's
// end_time DESC, start_time DESC, receive_time DESC, then name ASC. A nil span
// stands for the row of an exemplar the LEFT JOIN matched no span for, whose
// span columns are NULL; BigQuery sorts NULL last in a descending key and
// first in an ascending one.
func compareExemplarSpanCandidate(a, b map[string]any) int {
	for _, col := range []string{"end_time", "start_time", "receive_time"} {
		av, aok := colInt(a, col)
		bv, bok := colInt(b, col)
		switch {
		case aok && bok:
			if c := cmp.Compare(bv, av); c != 0 {
				return c
			}
		case aok:
			return -1
		case bok:
			return 1
		}
	}
	an, aok := colString(a, "name")
	bn, bok := colString(b, "name")
	switch {
	case aok && bok:
		return cmp.Compare(an, bn)
	case aok:
		return 1
	case bok:
		return -1
	}
	return 0
}

// exemplarSlotPartition is the PARTITION BY list of the QUALIFY window of
// v_gen_ai_metric_exemplars: the identity of a point in the metric dedup view,
// which v_gen_ai_metrics passes through, then the exemplar's array offset.
// Each partition is one exemplar slot of one deduplicated point.
func exemplarSlotPartition() []string {
	cols := make([]string, 0, len(metricsDedup.partition)+1)
	for _, col := range metricsDedup.partition {
		cols = append(cols, "m."+col)
	}
	return append(cols, "exemplar_ordinal")
}

// genAIMetricExemplarsView is v_gen_ai_metric_exemplars over the rows of the
// metrics and spans tables. It unnests v_gen_ai_metrics.exemplars with the
// array offset, drops exemplars without a non-empty trace_id and span_id, and
// left-joins otel_spans_dedup within the 25-hour window around m.timestamp,
// which gives an exemplar one row per matching span, or one row with NULL span
// columns. It then evaluates the QUALIFY window over all of those rows as
// BigQuery does: the rows are grouped by the values of the window's PARTITION
// BY list (exemplarSlotPartition), not per metric row, and each group keeps its
// first row in compareExemplarSpanCandidate order. Groups come out in the order
// they first appear.
func genAIMetricExemplarsView(t testing.TB, rawMetrics, rawSpans []map[string]any) []map[string]any {
	t.Helper()
	spans := spansDedup.apply(rawSpans)
	type candidate struct{ m, ex, s map[string]any }
	best := make(map[string]candidate)
	var order []string
	for _, m := range genAIMetricsView(t, rawMetrics) {
		exemplars, _ := m["exemplars"].([]map[string]any)
		for ordinal, ex := range exemplars {
			traceID, tok := colString(ex, "trace_id")
			spanID, sok := colString(ex, "span_id")
			if !tok || !sok || traceID == "" || spanID == "" {
				continue
			}
			joined := slices.DeleteFunc(slices.Clone(spans), func(s map[string]any) bool { return !exemplarJoinsSpan(m, ex, s) })
			if len(joined) == 0 {
				joined = []map[string]any{nil}
			}
			partition := make([]any, 0, len(metricsDedup.partition)+1)
			for _, col := range metricsDedup.partition {
				partition = append(partition, m[col])
			}
			key := groupKey(append(partition, int64(ordinal))...)
			for _, s := range joined {
				cur, seen := best[key]
				if !seen {
					order = append(order, key)
				}
				if !seen || compareExemplarSpanCandidate(s, cur.s) < 0 {
					best[key] = candidate{m: m, ex: ex, s: s}
				}
			}
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, key := range order {
		c := best[key]
		out = append(out, exemplarRow(t, c.m, c.ex, c.s))
	}
	return out
}

// exemplarRow is the projection of v_gen_ai_metric_exemplars for a row of
// v_gen_ai_metrics m, one of its exemplars ex, and the span row s it joined,
// nil for none.
func exemplarRow(t testing.TB, m, ex, s map[string]any) map[string]any {
	t.Helper()
	row := make(map[string]any)
	if v, ok := m["timestamp"]; ok {
		row["metric_timestamp"] = v
	}
	for _, col := range []string{
		"service_name", "metric_name", "metric_unit", "metric_type",
		"gen_ai_operation_name", "gen_ai_system", "gen_ai_request_model",
		"gen_ai_response_model", "gen_ai_agent_name", "gen_ai_token_type",
		"gen_ai_token_modality",
	} {
		if v, ok := m[col]; ok {
			row[col] = v
		}
	}
	if v, ok := m["error_type"]; ok {
		row["metric_error_type"] = v
	}
	for src, dst := range map[string]string{
		"time":                "exemplar_timestamp",
		"value_double":        "exemplar_value_double",
		"value_int64":         "exemplar_value_int64",
		"trace_id":            "trace_id",
		"span_id":             "span_id",
		"filtered_attributes": "exemplar_filtered_attributes",
	} {
		if v, ok := ex[src]; ok {
			row[dst] = v
		}
	}
	if s == nil {
		return row
	}
	for src, dst := range map[string]string{
		"start_time":    "span_start_time",
		"end_time":      "span_end_time",
		"duration_nano": "span_duration_nano",
		"name":          "span_name",
	} {
		if v, ok := s[src]; ok {
			row[dst] = v
		}
	}
	if ns, ok := colInt(s, "duration_nano"); ok {
		row["span_duration_ms"] = math.Round(float64(ns)/1e6*1000) / 1000
	}
	if st, ok := s["status"].(map[string]any); ok {
		if code, ok := st["code"]; ok {
			row["span_status_code"] = code
		}
		if msg, ok := st["message"]; ok {
			row["span_status_message"] = msg
		}
	}
	sa := parseAttrs(t, s)
	for _, c := range exemplarSpanChains {
		if v := c.eval(s, sa); v != nil {
			row[c.alias] = v
		}
	}
	return row
}

// rowsWhere returns the rows whose column equals want.
func rowsWhere(rows []map[string]any, col string, want any) []map[string]any {
	var out []map[string]any
	for _, r := range rows {
		if got, ok := r[col]; ok && reflect.DeepEqual(got, want) {
			out = append(out, r)
		}
	}
	return out
}

// onlyRow returns the one row whose column equals want, or fails.
func onlyRow(t testing.TB, label string, rows []map[string]any, col string, want any) map[string]any {
	t.Helper()
	got := rowsWhere(rows, col, want)
	if len(got) != 1 {
		t.Fatalf("%s: %d rows with %s = %v, want exactly 1 (of %d rows)", label, len(got), col, want, len(rows))
	}
	return got[0]
}

// requireCols checks the named columns of a row: a nil want means NULL.
func requireCols(t testing.TB, label string, row map[string]any, want map[string]any) {
	t.Helper()
	cols := make([]string, 0, len(want))
	for col := range want {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	for _, col := range cols {
		w := want[col]
		got, present := row[col]
		switch {
		case w == nil && present:
			t.Errorf("%s: %s = %v (%T), want NULL", label, col, got, got)
		case w != nil && (!present || !reflect.DeepEqual(got, w)):
			t.Errorf("%s: %s = %v (%T), want %v (%T)", label, col, got, got, w, w)
		}
	}
}

func sumCol(rows []map[string]any, col string) int64 {
	var total int64
	for _, r := range rows {
		total += intOrZero(r, col)
	}
	return total
}

// squash removes all whitespace, so that SQL can be compared regardless of
// its layout.
func squash(s string) string { return strings.Join(strings.Fields(s), "") }

// requireSQL fails for every fragment that the SQL does not contain, compared
// without whitespace.
func requireSQL(t *testing.T, view, sql string, fragments ...string) {
	t.Helper()
	flat := squash(sql)
	for _, f := range fragments {
		if !strings.Contains(flat, squash(f)) {
			t.Errorf("%s does not contain %q (whitespace ignored)", view, f)
		}
	}
}

func quoteList(vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = "'" + v + "'"
	}
	return strings.Join(quoted, ", ")
}

// The rollup views' shared SQL, rendered from the model's own tables.

// tokenParentsAggSQL is the window aggregate that collects the parent of every
// span that reports tokens. It keeps the NULLs: BigQuery's analytic ARRAY_AGG
// takes neither IGNORE NULLS nor DISTINCT.
func tokenParentsAggSQL() string {
	return "ARRAY_AGG( IF( s.parent_span_id IS NOT NULL" +
		" AND (s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL)," +
		" s.parent_span_id, NULL ) )"
}

// sql renders the aggregate over the partitions of the window.
func (w rollupWindow) sql() string {
	keys := make([]string, len(w.keys))
	for i, key := range w.keys {
		keys[i] = "s." + key
	}
	return tokenParentsAggSQL() + " OVER ( PARTITION BY " + strings.Join(keys, ", ") + " ) AS token_parent_span_ids"
}

// tokenLeafSQL is true for a span that no token-reporting span of its
// partition names as its parent.
func tokenLeafSQL() string {
	return "NOT EXISTS ( SELECT 1 FROM UNNEST(s.token_parent_span_ids) AS token_parent_span_id" +
		" WHERE token_parent_span_id = s.span_id )"
}

func llmCallPredicateSQL() string {
	return tokenLeafSQL() + " AND ( s.gen_ai_operation_name IN (" + quoteList(inferenceOps) + ") OR s.gen_ai_request_model IS NOT NULL )" +
		" AND ( COALESCE(s.gen_ai_operation_name, '') NOT IN (" + quoteList(agentToolOps) + ")" +
		" OR s.gen_ai_usage_input_tokens IS NOT NULL OR s.gen_ai_usage_output_tokens IS NOT NULL )"
}

func TestViewModelMatchesViewSQL(t *testing.T) {
	render := fmt.Sprintf

	requireSQL(t, "otel_spans_dedup", render(schema.SpansDedupViewQuery, confProject, confDataset, confSpans), spansDedup.sql())
	requireSQL(t, "otel_logs_dedup", render(schema.LogsDedupViewQuery, confProject, confDataset, confLogs), logsDedup.sql())
	requireSQL(t, "otel_metrics_dedup", render(schema.MetricsDedupViewQuery, confProject, confDataset, confMetrics), metricsDedup.sql())

	spanFragments := []string{
		"ROUND(duration_nano / 1e6, 3) AS duration_ms",
		"status.code AS status_code",
		"FROM `" + confProject + "." + confDataset + "." + confSpans + "_dedup`",
		"WHERE JSON_VALUE(attributes, '$.\"gen_ai.operation.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.system\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.provider.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gcp.vertex.agent.invocation_id\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.main_agent.name\"') IS NOT NULL" +
			" OR JSON_VALUE(attributes, '$.\"gen_ai.main_agent.id\"') IS NOT NULL",
	}
	for _, c := range spanChains {
		spanFragments = append(spanFragments, c.sql(""))
	}
	requireSQL(t, "v_gen_ai_spans", render(schema.GenAISpansViewQuery, confProject, confDataset, confSpans), spanFragments...)

	eventName := "COALESCE(NULLIF(l.event_name, ''), JSON_VALUE(l.attributes, '$.\"event.name\"'))"
	logFragments := []string{
		eventName + " AS event_name",
		"FROM `" + confProject + "." + confDataset + "." + confLogs + "_dedup` l" +
			" LEFT JOIN `" + confProject + "." + confDataset + "." + confSpans + "_dedup` s" +
			" ON l.trace_id = s.trace_id AND l.span_id = s.span_id" +
			" AND l.service_name IS NOT DISTINCT FROM s.service_name" +
			fmt.Sprintf(" AND s.start_time BETWEEN TIMESTAMP_SUB(l.timestamp, INTERVAL %d HOUR) AND TIMESTAMP_ADD(l.timestamp, INTERVAL %d HOUR)",
				int(logJoinBefore.Hours()), int(logJoinAfter.Hours())),
		"WHERE " + eventName + " LIKE '" + genAILogEventPattern + "'" +
			" OR JSON_VALUE(l.attributes, '$.\"gen_ai.system\"') IS NOT NULL" +
			" OR JSON_VALUE(l.attributes, '$.\"gen_ai.provider.name\"') IS NOT NULL" +
			" OR JSON_VALUE(l.attributes, '$.\"gcp.vertex.agent.invocation_id\"') IS NOT NULL",
		logOnlyErrorChain.sql("l."),
	}
	for _, c := range logChains {
		logFragments = append(logFragments, c.sql("l.", "s."))
	}
	requireSQL(t, "v_gen_ai_logs", render(schema.GenAILogsViewQuery, confProject, confDataset, confLogs, confSpans), logFragments...)

	metricFragments := []string{
		"FROM `" + confProject + "." + confDataset + "." + confMetrics + "_dedup`",
		"WHERE metric_name LIKE '" + genAIMetricNamePattern + "'",
		"CASE" +
			" WHEN metric_name = 'gen_ai.client.inference.usage.input_tokens' THEN 'input'" +
			" WHEN metric_name = 'gen_ai.client.inference.usage.output_tokens' THEN 'output'" +
			" WHEN metric_name = 'gen_ai.client.inference.usage.cache_read.input_tokens' THEN 'cache_read_input'" +
			" WHEN metric_name = 'gen_ai.client.inference.usage.cache_write.input_tokens' THEN 'cache_write_input'" +
			" WHEN metric_name = 'gen_ai.client.inference.usage.reasoning.output_tokens' THEN 'reasoning_output'" +
			" ELSE JSON_VALUE(attributes, '$.\"gen_ai.token.type\"') END AS gen_ai_inference_usage_dimension",
	}
	for _, c := range metricChains {
		metricFragments = append(metricFragments, c.sql(""))
	}
	requireSQL(t, "v_gen_ai_metrics", render(schema.GenAIMetricsViewQuery, confProject, confDataset, confMetrics), metricFragments...)

	exemplarFragments := []string{
		"m.timestamp AS metric_timestamp",
		"m.service_name, m.metric_name, m.metric_unit, m.metric_type",
		"m.gen_ai_operation_name, m.gen_ai_system, m.gen_ai_request_model, m.gen_ai_response_model, m.gen_ai_agent_name, m.gen_ai_token_type, m.gen_ai_token_modality",
		"m.error_type AS metric_error_type",
		"ex.time AS exemplar_timestamp",
		"ex.value_double AS exemplar_value_double",
		"ex.value_int64 AS exemplar_value_int64",
		"ex.trace_id AS trace_id",
		"ex.span_id AS span_id",
		"ex.filtered_attributes AS exemplar_filtered_attributes",
		"s.start_time AS span_start_time",
		"s.end_time AS span_end_time",
		"s.duration_nano AS span_duration_nano",
		"ROUND(s.duration_nano / 1e6, 3) AS span_duration_ms",
		"s.name AS span_name",
		"s.status.code AS span_status_code",
		"s.status.message AS span_status_message",
		"FROM `" + confProject + "." + confDataset + ".v_gen_ai_metrics` m, UNNEST(m.exemplars) AS ex WITH OFFSET AS exemplar_ordinal" +
			" LEFT JOIN `" + confProject + "." + confDataset + "." + confSpans + "_dedup` s" +
			" ON s.trace_id = ex.trace_id AND s.span_id = ex.span_id" +
			" AND s.service_name IS NOT DISTINCT FROM m.service_name" +
			fmt.Sprintf(" AND s.start_time BETWEEN TIMESTAMP_SUB(m.timestamp, INTERVAL %d HOUR) AND TIMESTAMP_ADD(m.timestamp, INTERVAL %d HOUR)",
				int(exemplarJoinBefore.Hours()), int(exemplarJoinAfter.Hours())),
		"WHERE ex.trace_id IS NOT NULL AND ex.span_id IS NOT NULL AND ex.trace_id != '' AND ex.span_id != ''",
		"QUALIFY ROW_NUMBER() OVER (" +
			" PARTITION BY " + strings.Join(exemplarSlotPartition(), ", ") +
			" ORDER BY s.end_time DESC, s.start_time DESC, s.receive_time DESC, s.name ASC" +
			" ) = 1",
	}
	for _, c := range exemplarSpanChains {
		exemplarFragments = append(exemplarFragments, c.sql("s."))
	}
	requireSQL(t, "v_gen_ai_metric_exemplars", render(schema.GenAIMetricExemplarsViewQuery, confProject, confDataset, confSpans), exemplarFragments...)

	// Both rollups read v_gen_ai_spans once, and a window over that read finds the
	// token parents; the window's PARTITION BY list is the matching rule.
	spansRead := "FROM `" + confProject + "." + confDataset + ".v_gen_ai_spans` AS s"
	requireSQL(t, "v_gen_ai_model_costs", render(schema.GenAIModelCostsViewQuery, confProject, confDataset),
		"WITH cost_spans AS ( SELECT TIMESTAMP_TRUNC(s.start_time, HOUR) AS usage_hour, s.service_name, s.trace_id, s.span_id, s.parent_span_id,",
		"s.gen_ai_usage_reasoning_tokens, s.error_type "+spansRead+" ), cost_windows AS ( SELECT s.*, "+costsWindow.sql()+" FROM cost_spans AS s )",
		"FROM cost_windows AS s WHERE "+llmCallPredicateSQL(),
		"SELECT s.usage_hour, s.service_name, s.gen_ai_agent_name,"+
			" COALESCE(s.gen_ai_provider_name, s.gen_ai_system) AS provider,"+
			" COALESCE(s.gen_ai_response_model, s.gen_ai_request_model) AS model,",
		"GROUP BY 1, 2, 3, 4, 5",
		"COUNT(*) AS call_count",
		"COUNTIF(s.status_code = 2) AS error_count",
		"SUM(COALESCE(s.gen_ai_usage_input_tokens, 0)) AS total_input_tokens",
		"SUM(COALESCE(s.gen_ai_usage_output_tokens, 0)) AS total_output_tokens",
		"SUM(COALESCE(s.gen_ai_usage_cache_read_input_tokens, 0)) AS total_cache_read_input_tokens",
		"SUM(COALESCE(s.gen_ai_usage_cache_creation_input_tokens, 0)) AS total_cache_creation_input_tokens",
		"SUM(COALESCE(s.gen_ai_usage_reasoning_tokens, 0)) AS total_reasoning_tokens",
		"COUNTIF(s.error_type IS NOT NULL) AS typed_error_count",
		"APPROX_TOP_COUNT(s.error_type, 6) AS raw_top_error_types",
		"SELECT * EXCEPT (raw_top_error_types), ARRAY( SELECT AS STRUCT x.value, x.count FROM UNNEST(raw_top_error_types) AS x WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5 ) AS top_error_types FROM cost_agg",
	)

	requireSQL(t, "v_gen_ai_sessions", render(schema.GenAISessionsViewQuery, confProject, confDataset),
		"WITH session_spans AS (",
		"DATE(s.start_time) AS session_start_date",
		"COALESCE(s.adk_session_id, s.gen_ai_conversation_id) AS session_id",
		"s.gen_ai_usage_reasoning_tokens, s.error_type "+spansRead+" WHERE COALESCE(s.adk_session_id, s.gen_ai_conversation_id) IS NOT NULL ),",
		"session_windows AS ( SELECT s.*, "+sessionsWindow.sql()+" FROM session_spans AS s ),",
		"session_calls AS ( SELECT s.*,",
		tokenLeafSQL()+" AS is_token_leaf",
		"COALESCE("+llmCallPredicateSQL()+", FALSE) AS is_llm_call",
		"FROM session_windows AS s )",
		"FROM session_calls GROUP BY service_name, session_start_date, session_id",
		"MIN(start_time) AS session_start_time",
		"MAX(end_time) AS session_end_time",
		"TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), MILLISECOND) AS session_duration_ms",
		"COUNT(DISTINCT trace_id) AS turn_count",
		"COUNT(DISTINCT adk_invocation_id) AS invocation_count",
		"COUNTIF(gen_ai_operation_name = 'execute_tool' OR gen_ai_tool_name IS NOT NULL) AS tool_call_count",
		"COUNTIF(status_code = 2) AS error_count",
		"SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_input_tokens, 0), 0)) AS total_input_tokens",
		"SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_output_tokens, 0), 0)) AS total_output_tokens",
		"SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_read_input_tokens, 0), 0)) AS total_cache_read_input_tokens",
		"SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_reasoning_tokens, 0), 0)) AS total_reasoning_tokens",
		"SUM(IF(is_token_leaf, COALESCE(gen_ai_usage_cache_creation_input_tokens, 0), 0)) AS total_cache_creation_input_tokens",
		"COUNTIF(is_llm_call) AS llm_call_count",
		"COUNTIF(gen_ai_operation_name = 'invoke_agent') AS agent_invocation_count",
		"COUNTIF(error_type IS NOT NULL) AS typed_error_count",
		"APPROX_TOP_COUNT(error_type, 6) AS raw_top_error_types",
		"SELECT * EXCEPT (raw_top_error_types), ARRAY( SELECT AS STRUCT x.value, x.count FROM UNNEST(raw_top_error_types) AS x WHERE x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5 ) AS top_error_types FROM session_agg",
		"GROUP BY service_name, session_start_date, session_id",
	)
}

// The rest of the file loads the golden fixtures, pushes them through the real
// exporters into the BigQuery fake, and evaluates the model over the rows.

// conformanceRig is a BigQuery fake with one exporter per signal started over
// it, as one collector would run them.
type conformanceRig struct {
	srv     *bqfake.Server
	traces  *tracesExporter
	logs    *logsExporter
	metrics *metricsExporter
}

func newConformanceRig(t *testing.T) *conformanceRig {
	t.Helper()
	srv := bqfake.NewServer(t)
	wireFakeHooks(t, srv)
	cfg := provisioningConfig()
	cfg.Project, cfg.Dataset = "conformance-proj", "otel_conformance"

	rig := &conformanceRig{srv: srv}
	var err error
	if rig.traces, err = newTracesExporter(e2eSettings(), cfg); err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if rig.logs, err = newLogsExporter(e2eSettings(), cfg); err != nil {
		t.Fatalf("newLogsExporter: %v", err)
	}
	if rig.metrics, err = newMetricsExporter(e2eSettings(), cfg); err != nil {
		t.Fatalf("newMetricsExporter: %v", err)
	}
	startE2EExporter(t, rig.traces)
	startE2EExporter(t, rig.logs)
	startE2EExporter(t, rig.metrics)
	return rig
}

func (r *conformanceRig) pushTraces(t *testing.T, td ptrace.Traces) {
	t.Helper()
	if err := r.traces.pushTraceData(context.Background(), td); err != nil {
		t.Fatalf("pushTraceData: %v", err)
	}
}

func (r *conformanceRig) pushLogs(t *testing.T, ld plog.Logs) {
	t.Helper()
	if err := r.logs.pushLogData(context.Background(), ld); err != nil {
		t.Fatalf("pushLogData: %v", err)
	}
}

func (r *conformanceRig) pushMetrics(t *testing.T, md pmetric.Metrics) {
	t.Helper()
	if err := r.metrics.pushMetricsData(context.Background(), md); err != nil {
		t.Fatalf("pushMetricsData: %v", err)
	}
}

func readFixture(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "otlp", rel))
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	return raw
}

func loadTraces(t *testing.T, rel string) ptrace.Traces {
	t.Helper()
	var u ptrace.JSONUnmarshaler
	td, err := u.UnmarshalTraces(readFixture(t, rel))
	if err != nil {
		t.Fatalf("UnmarshalTraces(%s): %v", rel, err)
	}
	return td
}

func loadLogs(t *testing.T, rel string) plog.Logs {
	t.Helper()
	var u plog.JSONUnmarshaler
	ld, err := u.UnmarshalLogs(readFixture(t, rel))
	if err != nil {
		t.Fatalf("UnmarshalLogs(%s): %v", rel, err)
	}
	return ld
}

func loadMetrics(t *testing.T, rel string) pmetric.Metrics {
	t.Helper()
	var u pmetric.JSONUnmarshaler
	md, err := u.UnmarshalMetrics(readFixture(t, rel))
	if err != nil {
		t.Fatalf("UnmarshalMetrics(%s): %v", rel, err)
	}
	return md
}

// pushFrameworkFixtures sends every signal each framework of the manifest
// lists.
func pushFrameworkFixtures(t *testing.T, rig *conformanceRig) sdkManifest {
	t.Helper()
	m := loadSDKManifest(t)
	for _, fw := range m.Frameworks {
		if slices.Contains(fw.Signals, "traces") && fw.TracesFile != "" {
			rig.pushTraces(t, loadTraces(t, fw.TracesFile))
		}
		if slices.Contains(fw.Signals, "logs") && fw.LogsFile != "" {
			rig.pushLogs(t, loadLogs(t, fw.LogsFile))
		}
		if slices.Contains(fw.Signals, "metrics") && fw.MetricsFile != "" {
			rig.pushMetrics(t, loadMetrics(t, fw.MetricsFile))
		}
	}
	return m
}

// frameworkGolden is what the views are expected to say about one framework's
// fixtures: the same spans that sdk_fixtures_test.go counts, read through the
// views. The numbers are written out here, not computed from the fixtures.
type frameworkGolden struct {
	id, service string
	// system is the provider value, named by gen_ai.provider.name when
	// viaProviderKey is set and by gen_ai.system otherwise.
	system         string
	viaProviderKey bool
	op             string
	requestModel   string
	responseModel  string // empty when the fixtures carry none

	// noSpanView marks CrewAI, whose spans carry OpenInference attributes and
	// so are not in v_gen_ai_spans at all.
	noSpanView bool

	agent, session   string // the agent name of the token-bearing span, and the session
	input, output    int64
	durationMS       int64
	tools            int64
	invocations      int64 // distinct gcp.vertex.agent.invocation_id values
	agentInvocations int64 // invoke_agent spans
	userIDs          []string
	agentNames       []string // the values ANY_VALUE(agent_name) may pick
	viewLogs         int      // the framework's log rows that v_gen_ai_logs holds
}

func (g frameworkGolden) model() string {
	if g.responseModel != "" {
		return g.responseModel
	}
	return g.requestModel
}

// Every one of these frameworks puts its token counts on the one span the
// views read, and for four of them that span is an invoke_agent span: a leaf
// that reports tokens still counts as one LLM call.
var frameworkGoldens = []frameworkGolden{
	{
		id: "anthropic_agent_sdk", service: "anthropic-agent-cli", system: "anthropic", op: "invoke_agent",
		requestModel: "anthropic-v4-default", responseModel: "anthropic-v4-default-20260514",
		agent: "code-assistant", session: "sess-anth-001", input: 1420, output: 380, durationMS: 1250,
		agentInvocations: 1, agentNames: []string{"code-assistant"},
	},
	{
		id: "google_adk", service: "adk-weather-agent", system: "gcp.vertex.agent", op: "invoke_agent",
		requestModel: "gemini-2.5-pro", responseModel: "gemini-2.5-pro-001",
		agent: "weather_coordinator", session: "sess-adk-1001", input: 612, output: 198, durationMS: 1500,
		tools: 1, invocations: 1, agentInvocations: 1, userIDs: []string{"user-adk-07"}, viewLogs: 2,
		// The tool span has no agent name, so it falls back to the service name.
		agentNames: []string{"adk-weather-agent", "weather_coordinator"},
	},
	{
		id: "langgraph", service: "langgraph-research-graph", system: "langsmith", op: "invoke_agent",
		requestModel: "gpt-4o",
		agent:        "research_supervisor", session: "thread-lg-404", input: 840, output: 310, durationMS: 1800,
		agentInvocations: 1, agentNames: []string{"research_supervisor"},
	},
	{
		id: "openai_agents", service: "openai-triage-agent", system: "openai", viaProviderKey: true, op: "chat",
		requestModel: "gpt-4o", responseModel: "gpt-4o-2024-08-06",
		agent: "billing_triage", session: "conv-oai-88", input: 512, output: 128, durationMS: 900,
		agentNames: []string{"billing_triage"}, viewLogs: 1,
	},
	{
		id: "crewai", service: "crewai-analyst-crew", system: "openai", op: "invoke_agent",
		requestModel: "gpt-4o-mini", noSpanView: true, input: 920, output: 410, durationMS: 2200,
	},
	{
		id: "aws_bedrock", service: "bedrock-converse-worker", system: "aws.bedrock", op: "chat",
		requestModel: "amazon.nova-pro-v1:0", responseModel: "amazon.nova-pro-v1:0",
		agent: "bedrock_router", session: "conv-bdrk-77", input: 350, output: 140, durationMS: 1100,
		agentNames: []string{"bedrock_router"}, viewLogs: 1,
	},
	{
		id: "microsoft_agent_framework", service: "maf-orchestrator", system: "microsoft.agent_framework",
		viaProviderKey: true, op: "invoke_agent", requestModel: "gpt-4o",
		agent: "support_orchestrator", session: "conv-maf-303", input: 480, output: 210, durationMS: 1400,
		agentInvocations: 1, agentNames: []string{"support_orchestrator"}, viewLogs: 1,
	},
}

// hourMicros is 2024-08-12T00:00:00Z, the hour all of the fixtures' spans start in.
const hourMicros = int64(1723420800) * 1_000_000

func TestFrameworkGoldensCoverTheManifest(t *testing.T) {
	m := loadSDKManifest(t)
	var fromManifest, fromGoldens []string
	for _, fw := range m.Frameworks {
		fromManifest = append(fromManifest, fw.ID)
	}
	for _, g := range frameworkGoldens {
		fromGoldens = append(fromGoldens, g.id)
	}
	sort.Strings(fromManifest)
	sort.Strings(fromGoldens)
	if !slices.Equal(fromManifest, fromGoldens) {
		t.Fatalf("frameworks in testdata/otlp/manifest.yaml = %v, but the golden table covers %v", fromManifest, fromGoldens)
	}
}

// A framework's fixtures, read through the views, say what the table above says.
func TestRollupViewsOverFrameworkFixtures(t *testing.T) {
	rig := newConformanceRig(t)
	pushFrameworkFixtures(t, rig)

	spans := genAISpansView(t, rig.srv.Rows(confSpans))
	costs := modelCostsView(spans)
	sessions := sessionsView(spans)

	wantRollupRows := 0
	for _, g := range frameworkGoldens {
		if g.noSpanView {
			if rows := rowsWhere(spans, "service_name", g.service); len(rows) != 0 {
				t.Errorf("%s: %d span rows in v_gen_ai_spans, want 0: its spans carry no gen_ai attributes", g.id, len(rows))
			}
			continue
		}
		wantRollupRows++

		cost := onlyRow(t, g.id+" costs", costs, "service_name", g.service)
		requireCols(t, g.id+" costs", cost, map[string]any{
			"usage_hour":        hourMicros,
			"gen_ai_agent_name": g.agent,
			"provider":          g.system,
			"model":             g.model(),
			"call_count":        int64(1),
			"error_count":       int64(0),
			// Each token count appears once, whatever other spans the trace holds.
			"total_input_tokens":                g.input,
			"total_output_tokens":               g.output,
			"total_cache_read_input_tokens":     int64(0),
			"total_cache_creation_input_tokens": int64(0),
			"total_reasoning_tokens":            int64(0),
		})

		session := onlyRow(t, g.id+" sessions", sessions, "service_name", g.service)
		requireCols(t, g.id+" sessions", session, map[string]any{
			"session_id":                        g.session,
			"session_start_date":                "2024-08-12",
			"session_duration_ms":               g.durationMS,
			"turn_count":                        int64(1),
			"invocation_count":                  g.invocations,
			"tool_call_count":                   g.tools,
			"error_count":                       int64(0),
			"total_input_tokens":                g.input,
			"total_output_tokens":               g.output,
			"total_cache_read_input_tokens":     int64(0),
			"total_reasoning_tokens":            int64(0),
			"total_cache_creation_input_tokens": int64(0),
			"llm_call_count":                    int64(1),
			"agent_invocation_count":            g.agentInvocations,
			"agent_name_candidates":             g.agentNames,
			"user_id_candidates":                nonNilStrings(g.userIDs),
		})
	}
	if len(costs) != wantRollupRows || len(sessions) != wantRollupRows {
		t.Errorf("v_gen_ai_model_costs has %d rows and v_gen_ai_sessions %d, want %d each", len(costs), len(sessions), wantRollupRows)
	}

	// The two rollups agree: every LLM call is counted once in each, and so is
	// every token.
	if got, want := sumCol(costs, "call_count"), sumCol(sessions, "llm_call_count"); got != want {
		t.Errorf("model costs count %d LLM calls, sessions %d", got, want)
	}
	for costCol, sessionCol := range map[string]string{
		"total_input_tokens":  "total_input_tokens",
		"total_output_tokens": "total_output_tokens",
	} {
		if got, want := sumCol(costs, costCol), sumCol(sessions, sessionCol); got != want {
			t.Errorf("model costs sum %s = %d, sessions sum %s = %d", costCol, got, sessionCol, want)
		}
	}
}

// nonNilStrings turns a nil slice into the empty slice the model reports for
// a group in which ANY_VALUE has nothing to pick.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// An agent span that reports the sum of its model calls must not be counted
// with them. The hierarchy fixture holds two turns of one session: in turn
// one the agent span reports the tokens of its two chat children, and in turn
// two it reports none and its one chat child fails.
func TestRollupViewsCountEachModelCallOnce(t *testing.T) {
	rig := newConformanceRig(t)
	td := loadTraces(t, "hierarchy/traces.json")
	// A collector retries a batch it could not confirm, so the same spans reach
	// the table twice; the dedup views make them one.
	rig.pushTraces(t, td)
	rig.pushTraces(t, td)

	raw := rig.srv.Rows(confSpans)
	if len(raw) != 2*td.SpanCount() {
		t.Fatalf("span table holds %d rows after two pushes of %d spans, want %d", len(raw), td.SpanCount(), 2*td.SpanCount())
	}
	spans := genAISpansView(t, raw)
	if len(spans) != td.SpanCount() {
		t.Fatalf("v_gen_ai_spans holds %d rows, want the %d distinct spans", len(spans), td.SpanCount())
	}

	// The fixture is built so that counting every span double counts: the agent
	// span of turn one reports exactly what its children report between them.
	naiveInput, naiveOutput := sumCol(spans, "gen_ai_usage_input_tokens"), sumCol(spans, "gen_ai_usage_output_tokens")
	if naiveInput != 1840 || naiveOutput != 500 {
		t.Fatalf("every span's tokens add up to %d in and %d out, want 1840 and 500: the fixture no longer double counts", naiveInput, naiveOutput)
	}
	naiveCalls := 0
	for _, s := range spans {
		if isLLMCall(s, true) {
			naiveCalls++
		}
	}
	if naiveCalls != 4 {
		t.Fatalf("without the token parent exclusion %d spans count as LLM calls, want 4", naiveCalls)
	}

	costs := modelCostsView(spans)
	if len(costs) != 2 {
		t.Fatalf("v_gen_ai_model_costs has %d rows, want 2 (one per model): %v", len(costs), costs)
	}
	requireCols(t, "costs for the model that answered", onlyRow(t, "costs", costs, "model", "gpt-4o-2024-08-06"), map[string]any{
		"usage_hour":                        hourMicros,
		"service_name":                      "hierarchy-trip-agent",
		"gen_ai_agent_name":                 "travel_planner",
		"provider":                          "openai",
		"call_count":                        int64(2), // not 3: the agent span is its children
		"error_count":                       int64(0),
		"total_input_tokens":                int64(920),
		"total_output_tokens":               int64(250),
		"total_cache_read_input_tokens":     int64(128),
		"total_cache_creation_input_tokens": int64(0),
		"total_reasoning_tokens":            int64(40),
	})
	requireCols(t, "costs for the model that failed", onlyRow(t, "costs", costs, "model", "gpt-4o-mini"), map[string]any{
		"call_count":                        int64(1), // a failed call is a call
		"error_count":                       int64(1),
		"total_input_tokens":                int64(0),
		"total_output_tokens":               int64(0),
		"total_cache_read_input_tokens":     int64(0),
		"total_cache_creation_input_tokens": int64(0),
		"total_reasoning_tokens":            int64(0),
	})

	sessions := sessionsView(spans)
	if len(sessions) != 1 {
		t.Fatalf("v_gen_ai_sessions has %d rows, want 1: %v", len(sessions), sessions)
	}
	requireCols(t, "session", sessions[0], map[string]any{
		"session_id":                        "conv-hier-501",
		"service_name":                      "hierarchy-trip-agent",
		"session_start_date":                "2024-08-12",
		"session_start_time":                int64(1723420870) * 1_000_000,
		"session_end_time":                  int64(1723420901)*1_000_000 + 500_000,
		"session_duration_ms":               int64(31500),
		"turn_count":                        int64(2),
		"invocation_count":                  int64(0),
		"tool_call_count":                   int64(1),
		"error_count":                       int64(1),
		"total_input_tokens":                int64(920),
		"total_output_tokens":               int64(250),
		"total_cache_read_input_tokens":     int64(128),
		"total_reasoning_tokens":            int64(40),
		"total_cache_creation_input_tokens": int64(0),
		"llm_call_count":                    int64(3),
		"agent_invocation_count":            int64(2),
		"user_id_candidates":                []string{"user-hier-42"},
		"agent_name_candidates":             []string{"travel_planner"},
	})

	if got, want := sumCol(costs, "call_count"), sumCol(sessions, "llm_call_count"); got != want {
		t.Errorf("model costs count %d LLM calls, sessions %d", got, want)
	}
	if got, want := sumCol(costs, "total_input_tokens"), sumCol(sessions, "total_input_tokens"); got != want {
		t.Errorf("model costs sum %d input tokens, sessions %d", got, want)
	}
}

// A parent that reports the sum of its children is left out of a rollup only
// when it and the children share a partition of the rollup's window: one
// service and trace in one usage hour for v_gen_ai_model_costs, and one
// service, session, UTC date and trace for v_gen_ai_sessions. The cases below
// pin what each view does inside a partition, at the edges of one, and for a
// child that carries no session id.
//
// A pair that straddles an hour, a date or a service is not matched, so both
// of its spans count and the rollup holds their tokens twice. That is a limit
// of the matching, pinned here so that a change to it is deliberate.
func TestRollupViewsMatchTokenParentsWithinTheirWindow(t *testing.T) {
	rig := newConformanceRig(t)
	at := func(day, hour, minute, second int) time.Time {
		return time.Date(2024, 8, day, hour, minute, second, 0, time.UTC)
	}
	// span is one model-naming span: a parent of 0 makes it a root, and an
	// empty session leaves it without a session id.
	span := func(trace, id, parent int, op, agent, session string, start time.Time, in, out int64) specSpan {
		attrs := map[string]any{
			"gen_ai.operation.name":      op,
			"gen_ai.provider.name":       "openai",
			"gen_ai.request.model":       "m",
			"gen_ai.agent.name":          agent,
			"gen_ai.usage.input_tokens":  in,
			"gen_ai.usage.output_tokens": out,
		}
		if session != "" {
			attrs["gen_ai.conversation.id"] = session
		}
		sp := specSpan{
			trace: traceHex(trace), id: spanHex(id), name: op + " m",
			start: start, end: start.Add(time.Second), attrs: attrs,
		}
		if parent != 0 {
			sp.parent = spanHex(parent)
		}
		return sp
	}
	rig.pushTraces(t, buildTraces(t, "window-agent",
		// An agent span reports the sum of its two chat calls.
		span(1, 1, 0, "invoke_agent", "hier", "conv-hier", at(12, 10, 0, 0), 100, 50),
		span(1, 2, 1, "chat", "hier", "conv-hier", at(12, 10, 0, 1), 40, 20),
		span(1, 3, 1, "chat", "hier", "conv-hier", at(12, 10, 0, 2), 60, 30),
		// The chat child carries no session id.
		span(2, 4, 0, "invoke_agent", "nosession_child", "conv-nosession", at(12, 10, 0, 0), 100, 50),
		span(2, 5, 4, "chat", "nosession_child", "", at(12, 10, 0, 1), 100, 50),
		// The child starts in the next hour of the same UTC date.
		span(3, 6, 0, "invoke_agent", "crosshour", "conv-crosshour", at(12, 10, 59, 59), 80, 40),
		span(3, 7, 6, "chat", "crosshour", "conv-crosshour", at(12, 11, 0, 1), 80, 40),
		// The child starts after midnight UTC.
		span(4, 8, 0, "invoke_agent", "crossdate", "conv-crossdate", at(12, 23, 59, 59), 80, 40),
		span(4, 9, 8, "chat", "crossdate", "conv-crossdate", at(13, 0, 0, 1), 80, 40),
		// The child is pushed below, under another service name.
		span(5, 10, 0, "invoke_agent", "crossservice", "conv-crossservice", at(12, 10, 0, 0), 80, 40),
		// A chat call with no parent and no child.
		span(7, 14, 0, "chat", "loner", "conv-loner", at(12, 10, 0, 0), 7, 3),
	))
	rig.pushTraces(t, buildTraces(t, "window-agent-b",
		span(5, 11, 10, "chat", "crossservice", "conv-crossservice", at(12, 10, 0, 1), 80, 40),
	))
	// Neither span has a service name, which is a value of its own.
	rig.pushTraces(t, buildTraces(t, "",
		span(6, 12, 0, "invoke_agent", "noservice", "conv-noservice", at(12, 10, 0, 0), 80, 40),
		span(6, 13, 12, "chat", "noservice", "conv-noservice", at(12, 10, 0, 1), 80, 40),
	))

	spans := genAISpansView(t, rig.srv.Rows(confSpans))
	if len(spans) != 14 {
		t.Fatalf("v_gen_ai_spans holds %d rows, want 14", len(spans))
	}
	costs := modelCostsView(spans)
	sessions := sessionsView(spans)

	// rollup is what a case adds up to in one view: rows and the sums of their
	// call counts and tokens.
	type rollup struct {
		rows           int
		calls, in, out int64
	}
	for _, tc := range []struct {
		name, agent, session string
		costs, sessions      rollup
	}{
		{
			name: "parent and children in one partition", agent: "hier", session: "conv-hier",
			costs: rollup{1, 2, 100, 50}, sessions: rollup{1, 2, 100, 50},
		},
		{
			// The child has no session id, so the sessions view does not pair it
			// with its parent, which has one: the parent keeps its tokens and
			// the totals are not 0. v_gen_ai_model_costs pairs them and leaves
			// the parent out.
			name: "child without a session id", agent: "nosession_child", session: "conv-nosession",
			costs: rollup{1, 1, 100, 50}, sessions: rollup{1, 1, 100, 50},
		},
		{
			// The hours differ, so the model costs count both spans. The UTC date
			// is the same, so the sessions view matches them.
			name: "child in the next hour", agent: "crosshour", session: "conv-crosshour",
			costs: rollup{2, 2, 160, 80}, sessions: rollup{1, 1, 80, 40},
		},
		{
			name: "child after midnight UTC", agent: "crossdate", session: "conv-crossdate",
			costs: rollup{2, 2, 160, 80}, sessions: rollup{2, 2, 160, 80},
		},
		{
			name: "child under another service name", agent: "crossservice", session: "conv-crossservice",
			costs: rollup{2, 2, 160, 80}, sessions: rollup{2, 2, 160, 80},
		},
		{
			name: "no service name on either span", agent: "noservice", session: "conv-noservice",
			costs: rollup{1, 1, 80, 40}, sessions: rollup{1, 1, 80, 40},
		},
		{
			name: "no parent and no child", agent: "loner", session: "conv-loner",
			costs: rollup{1, 1, 7, 3}, sessions: rollup{1, 1, 7, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			costRows := rowsWhere(costs, "gen_ai_agent_name", tc.agent)
			if got := (rollup{len(costRows), sumCol(costRows, "call_count"), sumCol(costRows, "total_input_tokens"), sumCol(costRows, "total_output_tokens")}); got != tc.costs {
				t.Errorf("v_gen_ai_model_costs for agent %q holds %+v, want %+v: %v", tc.agent, got, tc.costs, costRows)
			}
			sessionRows := rowsWhere(sessions, "session_id", tc.session)
			if got := (rollup{len(sessionRows), sumCol(sessionRows, "llm_call_count"), sumCol(sessionRows, "total_input_tokens"), sumCol(sessionRows, "total_output_tokens")}); got != tc.sessions {
				t.Errorf("v_gen_ai_sessions for session %q holds %+v, want %+v: %v", tc.session, got, tc.sessions, sessionRows)
			}
		})
	}
	if len(costs) != 10 || len(sessions) != 9 {
		t.Errorf("v_gen_ai_model_costs has %d rows and v_gen_ai_sessions %d, want 10 and 9", len(costs), len(sessions))
	}

	// At midnight the parent stays with its own date and the child with its
	// own, and across an hour the one session row holds only the child's tokens.
	requireCols(t, "session of the date before midnight", onlyRow(t, "sessions", rowsWhere(sessions, "session_id", "conv-crossdate"), "session_start_date", "2024-08-12"), map[string]any{
		"total_input_tokens": int64(80), "llm_call_count": int64(1), "agent_invocation_count": int64(1),
	})
	requireCols(t, "session of the date after midnight", onlyRow(t, "sessions", rowsWhere(sessions, "session_id", "conv-crossdate"), "session_start_date", "2024-08-13"), map[string]any{
		"total_input_tokens": int64(80), "llm_call_count": int64(1), "agent_invocation_count": int64(0),
	})
	requireCols(t, "session across the hour", onlyRow(t, "sessions", sessions, "session_id", "conv-crosshour"), map[string]any{
		"total_input_tokens": int64(80), "llm_call_count": int64(1), "agent_invocation_count": int64(1),
	})
}

// frameworkEntry is the manifest entry of a framework.
func frameworkEntry(t testing.TB, m sdkManifest, id string) sdkManifestEntry {
	t.Helper()
	for _, fw := range m.Frameworks {
		if fw.ID == id {
			return fw
		}
	}
	t.Fatalf("framework %q is not in the manifest", id)
	return sdkManifestEntry{}
}

// The fixtures cannot hold every case the views must get right, so the tests
// below build the few spans, log records and metrics that can, and push them
// through the same exporters.

// specSpan is one span of a hand-built trace.
type specSpan struct {
	trace, id, parent string // hex IDs; parent is empty for a root span
	name              string
	start, end        time.Time
	failed            bool
	attrs             map[string]any
}

// specLog is one log record of a hand-built batch.
type specLog struct {
	trace, span string
	at          time.Time
	event       string
	attrs       map[string]any
}

// traceHex and spanHex give the n-th trace and span ID of a hand-built test.
func traceHex(n int) string { return fmt.Sprintf("%032x", n) }
func spanHex(n int) string  { return fmt.Sprintf("%016x", n) }

func mustTraceID(t testing.TB, h string) pcommon.TraceID {
	t.Helper()
	var id pcommon.TraceID
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != len(id) {
		t.Fatalf("%q is not a %d-byte hex trace ID", h, len(id))
	}
	copy(id[:], b)
	return id
}

func mustSpanID(t testing.TB, h string) pcommon.SpanID {
	t.Helper()
	var id pcommon.SpanID
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != len(id) {
		t.Fatalf("%q is not a %d-byte hex span ID", h, len(id))
	}
	copy(id[:], b)
	return id
}

func putAttrs(t testing.TB, dst pcommon.Map, src map[string]any) {
	t.Helper()
	if err := dst.FromRaw(src); err != nil {
		t.Fatalf("attributes %v: %v", src, err)
	}
}

// buildTraces is one resource, named service unless that is empty, holding the
// spans.
func buildTraces(t testing.TB, service string, spans ...specSpan) ptrace.Traces {
	t.Helper()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	if service != "" {
		rs.Resource().Attributes().PutStr("service.name", service)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("synthetic.view.conformance")
	for _, sp := range spans {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(mustTraceID(t, sp.trace))
		s.SetSpanID(mustSpanID(t, sp.id))
		if sp.parent != "" {
			s.SetParentSpanID(mustSpanID(t, sp.parent))
		}
		s.SetName(sp.name)
		s.SetKind(ptrace.SpanKindClient)
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(sp.start))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(sp.end))
		if sp.failed {
			s.Status().SetCode(ptrace.StatusCodeError)
		}
		putAttrs(t, s.Attributes(), sp.attrs)
	}
	return td
}

// buildLogs is one resource, named service unless that is empty, holding the
// log records.
func buildLogs(t testing.TB, service string, recs ...specLog) plog.Logs {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	if service != "" {
		rl.Resource().Attributes().PutStr("service.name", service)
	}
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("synthetic.view.conformance")
	for _, rec := range recs {
		lr := sl.LogRecords().AppendEmpty()
		if rec.trace != "" {
			lr.SetTraceID(mustTraceID(t, rec.trace))
		}
		if rec.span != "" {
			lr.SetSpanID(mustSpanID(t, rec.span))
		}
		lr.SetTimestamp(pcommon.NewTimestampFromTime(rec.at))
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		if rec.event != "" {
			lr.SetEventName(rec.event)
		}
		putAttrs(t, lr.Attributes(), rec.attrs)
	}
	return ld
}

// buildGaugeMetrics is one gauge point per name, in a resource named service.
func buildGaugeMetrics(service string, at time.Time, names ...string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("synthetic.view.conformance")
	for _, name := range names {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(at))
		dp.SetDoubleValue(1)
	}
	return md
}

// Each chain of the span view picks the first key it finds, and the keys that
// follow are only fallbacks. This builds spans that carry every key of a chain,
// and spans that carry only the last, so that a reordered chain changes a value.
func TestViewModelPromotionAndCoalesceOrder(t *testing.T) {
	rig := newConformanceRig(t)
	at := time.Date(2024, 8, 12, 1, 0, 0, 0, time.UTC)
	span := func(n int, attrs map[string]any) specSpan {
		return specSpan{
			trace: traceHex(n), id: spanHex(n), name: fmt.Sprintf("span %d", n),
			start: at, end: at.Add(time.Second), attrs: attrs,
		}
	}
	rig.pushTraces(t, buildTraces(t, "promo-agent",
		// Span 1 carries both keys of every chain.
		span(1, map[string]any{
			"gen_ai.operation.name":                    "chat",
			"gen_ai.system":                            "openai",
			"gen_ai.provider.name":                     "azure.ai.openai",
			"gen_ai.request.model":                     "req-model",
			"gen_ai.agent.name":                        "named_agent",
			"gcp.vertex.agent.name":                    "vertex_agent",
			"gen_ai.conversation.id":                   "conv-id",
			"gcp.vertex.agent.session_id":              "vertex-session",
			"user.id":                                  "user-a",
			"gen_ai.user.id":                           "user-b",
			"gen_ai.usage.input_tokens":                int64(10),
			"gen_ai.usage.output_tokens":               "7", // the column stores an int attribute as text too, so this reads as an int
			"gen_ai.usage.reasoning.output_tokens":     int64(5),
			"gen_ai.usage.reasoning_tokens":            int64(99),
			"gen_ai.usage.cache_read.input_tokens":     int64(3),
			"gen_ai.usage.cache_read_input_tokens":     int64(77),
			"gen_ai.usage.cache_creation.input_tokens": int64(2),
			"gen_ai.usage.cache_creation_input_tokens": int64(88),
			"error.type":                               "timeout",
		}),
		// Span 2 carries only the other key of each chain.
		span(2, map[string]any{
			"gen_ai.operation.name":                    "chat",
			"gen_ai.provider.name":                     "anthropic",
			"gen_ai.request.model":                     "req-model-2",
			"gen_ai.response.model":                    "resp-model-2",
			"gcp.vertex.agent.name":                    "vertex_only",
			"gen_ai.conversation.id":                   "conv-only",
			"gen_ai.user.id":                           "user-c",
			"gen_ai.usage.reasoning_tokens":            int64(11),
			"gen_ai.usage.cache_read_input_tokens":     int64(4),
			"gen_ai.usage.cache_creation_input_tokens": int64(6),
		}),
		// Span 3 names no agent, so the span view falls back to the service name.
		span(3, map[string]any{
			"gen_ai.operation.name":     "chat",
			"gen_ai.provider.name":      "openai",
			"gen_ai.request.model":      "req-model-3",
			"gen_ai.usage.input_tokens": "12",
		}),
		// Span 4 is an ADK span with an invocation and nothing else.
		span(4, map[string]any{"gcp.vertex.agent.invocation_id": "inv-4"}),
		// Spans 5 and 6 are not GenAI spans: a request model alone does not make one.
		span(5, map[string]any{"gen_ai.request.model": "lonely-model"}),
		span(6, map[string]any{"http.request.method": "GET"}),
	))

	spans := genAISpansView(t, rig.srv.Rows(confSpans))
	if len(spans) != 4 {
		t.Fatalf("v_gen_ai_spans has %d rows, want 4 (spans 1 to 4): %v", len(spans), spans)
	}
	for _, n := range []int{5, 6} {
		if rows := rowsWhere(spans, "span_id", spanHex(n)); len(rows) != 0 {
			t.Errorf("span %d is in v_gen_ai_spans, but it names no operation, system, provider or invocation", n)
		}
	}

	requireCols(t, "span 1", onlyRow(t, "spans", spans, "span_id", spanHex(1)), map[string]any{
		"gen_ai_operation_name":                    "chat",
		"gen_ai_system":                            "openai",          // gen_ai.system before gen_ai.provider.name
		"gen_ai_provider_name":                     "azure.ai.openai", // the provider column reads the provider key alone
		"gen_ai_request_model":                     "req-model",
		"gen_ai_response_model":                    nil,
		"gen_ai_agent_name":                        "named_agent",
		"gen_ai_conversation_id":                   "conv-id",        // the portable column prefers the conversation ID
		"adk_session_id":                           "vertex-session", // the ADK column prefers the vertex session ID
		"adk_user_id":                              "user-a",
		"gen_ai_usage_input_tokens":                int64(10),
		"gen_ai_usage_output_tokens":               int64(7),
		"gen_ai_usage_reasoning_tokens":            int64(5), // the current key, not the older alias
		"gen_ai_usage_cache_read_input_tokens":     int64(3),
		"gen_ai_usage_cache_creation_input_tokens": int64(2),
		"error_type":                               "timeout",
	})
	requireCols(t, "span 2", onlyRow(t, "spans", spans, "span_id", spanHex(2)), map[string]any{
		"gen_ai_system":                            "anthropic", // the provider key is the fallback for the system
		"gen_ai_provider_name":                     "anthropic",
		"gen_ai_response_model":                    "resp-model-2",
		"gen_ai_agent_name":                        "vertex_only",
		"gen_ai_conversation_id":                   "conv-only",
		"adk_session_id":                           "conv-only", // the conversation ID is the fallback for the ADK column
		"adk_user_id":                              "user-c",
		"gen_ai_usage_input_tokens":                nil,
		"gen_ai_usage_reasoning_tokens":            int64(11), // the older alias is still read
		"gen_ai_usage_cache_read_input_tokens":     int64(4),
		"gen_ai_usage_cache_creation_input_tokens": int64(6),
		"error_type":                               nil,
	})
	requireCols(t, "span 3", onlyRow(t, "spans", spans, "span_id", spanHex(3)), map[string]any{
		"gen_ai_agent_name":          "promo-agent",
		"gen_ai_usage_input_tokens":  int64(12),
		"gen_ai_usage_output_tokens": nil,
	})
	requireCols(t, "span 4", onlyRow(t, "spans", spans, "span_id", spanHex(4)), map[string]any{
		"adk_invocation_id":     "inv-4",
		"gen_ai_operation_name": nil,
		"gen_ai_system":         nil,
	})

	// The rollups read the same chains: the provider before the system, and the
	// model the provider answered with before the model asked for.
	costs := modelCostsView(spans)
	if len(costs) != 3 {
		t.Fatalf("v_gen_ai_model_costs has %d rows, want 3 (spans 1 to 3; span 4 is not a model call): %v", len(costs), costs)
	}
	requireCols(t, "costs of span 1", onlyRow(t, "costs", costs, "gen_ai_agent_name", "named_agent"), map[string]any{
		"provider": "azure.ai.openai", "model": "req-model",
		"total_input_tokens": int64(10), "total_output_tokens": int64(7),
		"total_reasoning_tokens": int64(5), "total_cache_read_input_tokens": int64(3), "total_cache_creation_input_tokens": int64(2),
	})
	requireCols(t, "costs of span 2", onlyRow(t, "costs", costs, "gen_ai_agent_name", "vertex_only"), map[string]any{
		"provider": "anthropic", "model": "resp-model-2",
		"total_input_tokens": int64(0), "total_output_tokens": int64(0),
		"total_reasoning_tokens": int64(11), "total_cache_read_input_tokens": int64(4), "total_cache_creation_input_tokens": int64(6),
	})
	requireCols(t, "costs of span 3", onlyRow(t, "costs", costs, "gen_ai_agent_name", "promo-agent"), map[string]any{
		"provider": "openai", "model": "req-model-3", "total_input_tokens": int64(12),
	})
}

// A session is a service, a session ID and a UTC date, so a session that runs
// across midnight is two rows, and the model costs split at the hour.
func TestSessionsViewGroupsByUTCDate(t *testing.T) {
	rig := newConformanceRig(t)
	chat := func(n int, start time.Time, length time.Duration, in, out int64) specSpan {
		return specSpan{
			trace: traceHex(n), id: spanHex(n), name: "chat gpt-4o", start: start, end: start.Add(length),
			attrs: map[string]any{
				"gen_ai.operation.name":      "chat",
				"gen_ai.provider.name":       "openai",
				"gen_ai.request.model":       "gpt-4o",
				"gen_ai.agent.name":          "night_bot",
				"gen_ai.conversation.id":     "conv-midnight-1",
				"gen_ai.usage.input_tokens":  in,
				"gen_ai.usage.output_tokens": out,
			},
		}
	}
	rig.pushTraces(t, buildTraces(t, "midnight-agent",
		chat(1, time.Date(2024, 8, 12, 23, 59, 30, 0, time.UTC), 10*time.Second, 100, 10),
		chat(2, time.Date(2024, 8, 13, 0, 0, 30, 0, time.UTC), 10*time.Second, 300, 30),
		chat(3, time.Date(2024, 8, 12, 23, 59, 50, 0, time.UTC), 5*time.Second, 200, 20),
	))
	spans := genAISpansView(t, rig.srv.Rows(confSpans))

	sessions := sessionsView(spans)
	if len(sessions) != 2 {
		t.Fatalf("v_gen_ai_sessions has %d rows, want 2 (one per UTC date): %v", len(sessions), sessions)
	}
	requireCols(t, "session of 2024-08-12", onlyRow(t, "sessions", sessions, "session_start_date", "2024-08-12"), map[string]any{
		"session_id":          "conv-midnight-1",
		"service_name":        "midnight-agent",
		"session_start_time":  time.Date(2024, 8, 12, 23, 59, 30, 0, time.UTC).UnixMicro(),
		"session_end_time":    time.Date(2024, 8, 12, 23, 59, 55, 0, time.UTC).UnixMicro(),
		"session_duration_ms": int64(25000),
		"turn_count":          int64(2),
		"llm_call_count":      int64(2),
		"total_input_tokens":  int64(300),
		"total_output_tokens": int64(30),
	})
	requireCols(t, "session of 2024-08-13", onlyRow(t, "sessions", sessions, "session_start_date", "2024-08-13"), map[string]any{
		"session_id":          "conv-midnight-1",
		"service_name":        "midnight-agent",
		"session_start_time":  time.Date(2024, 8, 13, 0, 0, 30, 0, time.UTC).UnixMicro(),
		"session_end_time":    time.Date(2024, 8, 13, 0, 0, 40, 0, time.UTC).UnixMicro(),
		"session_duration_ms": int64(10000),
		"turn_count":          int64(1),
		"llm_call_count":      int64(1),
		"total_input_tokens":  int64(300),
		"total_output_tokens": int64(30),
	})

	costs := modelCostsView(spans)
	if len(costs) != 2 {
		t.Fatalf("v_gen_ai_model_costs has %d rows, want 2 (one per hour): %v", len(costs), costs)
	}
	requireCols(t, "costs of the hour before midnight", onlyRow(t, "costs", costs, "usage_hour", time.Date(2024, 8, 12, 23, 0, 0, 0, time.UTC).UnixMicro()), map[string]any{
		"call_count": int64(2), "total_input_tokens": int64(300), "total_output_tokens": int64(30),
	})
	requireCols(t, "costs of the hour after midnight", onlyRow(t, "costs", costs, "usage_hour", time.Date(2024, 8, 13, 0, 0, 0, 0, time.UTC).UnixMicro()), map[string]any{
		"call_count": int64(1), "total_input_tokens": int64(300), "total_output_tokens": int64(30),
	})
	for _, col := range []string{"total_input_tokens", "total_output_tokens"} {
		if got, want := sumCol(costs, col), sumCol(sessions, col); got != want {
			t.Errorf("model costs sum %s = %d, sessions %d", col, got, want)
		}
	}
}

// A log record is enriched with its span only when the span started inside a
// window around the record, and only when both name the same service, a
// missing service name being a name of its own. The bounds are included, and a
// record that matches no span stays in the view, unenriched.
func TestLogsViewJoinIsBoundedAndNullSafe(t *testing.T) {
	rig := newConformanceRig(t)
	t0 := time.Date(2024, 8, 12, 6, 0, 0, 0, time.UTC)
	span := func(n int, attrs map[string]any) specSpan {
		return specSpan{
			trace: traceHex(n), id: spanHex(n), name: "chat gpt-4o",
			start: t0, end: t0.Add(time.Second), attrs: attrs,
		}
	}
	rig.pushTraces(t, buildTraces(t, "join-agent",
		span(1, map[string]any{
			"gen_ai.operation.name":  "chat",
			"gen_ai.provider.name":   "openai",
			"gen_ai.request.model":   "gpt-4o",
			"gen_ai.conversation.id": "conv-join-1",
			"gen_ai.agent.name":      "join_bot",
			"user.id":                "user-join",
		}),
		// Span 2 names no agent. The logs view does not fall back to the service
		// name for one, as the span view does.
		span(2, map[string]any{
			"gen_ai.operation.name":  "chat",
			"gen_ai.provider.name":   "openai",
			"gen_ai.request.model":   "gpt-4o",
			"gen_ai.conversation.id": "conv-join-2",
		}),
	))
	rig.pushTraces(t, buildTraces(t, "", span(3, map[string]any{
		"gen_ai.operation.name":  "chat",
		"gen_ai.provider.name":   "anthropic",
		"gen_ai.request.model":   "example-model",
		"gen_ai.conversation.id": "conv-null-1",
		"gen_ai.agent.name":      "null_bot",
	})))

	record := func(name string, trace, spanID int, at time.Time, extra map[string]any) specLog {
		attrs := map[string]any{"case": name}
		for k, v := range extra {
			attrs[k] = v
		}
		return specLog{trace: traceHex(trace), span: spanHex(spanID), at: at, event: "gen_ai.choice", attrs: attrs}
	}
	rig.pushLogs(t, buildLogs(t, "join-agent",
		record("inside", 1, 1, t0.Add(time.Second), nil),
		record("edge_24h_after", 1, 1, t0.Add(24*time.Hour), nil),
		record("edge_1h_before", 1, 1, t0.Add(-time.Hour), nil),
		record("too_late", 1, 1, t0.Add(24*time.Hour+time.Second), nil),
		record("too_early", 1, 1, t0.Add(-time.Hour-time.Second), nil),
		record("log_wins", 1, 1, t0.Add(2*time.Second), map[string]any{
			"gen_ai.agent.name": "log_agent",
			"gen_ai.system":     "log_system",
		}),
		record("no_agent_name", 2, 2, t0.Add(time.Second), nil),
	))
	// The same IDs as span 1, from another service and from none.
	rig.pushLogs(t, buildLogs(t, "other-agent", record("other_service", 1, 1, t0.Add(time.Second), nil)))
	rig.pushLogs(t, buildLogs(t, "",
		record("no_service", 1, 1, t0.Add(time.Second), nil),
		record("null_service", 3, 3, t0.Add(time.Second), nil),
	))

	type enrichment struct{ op, agent, system, session, user any }
	span1 := enrichment{op: "chat", agent: "join_bot", system: "openai", session: "conv-join-1", user: "user-join"}
	want := map[string]enrichment{
		"inside":         span1,
		"edge_24h_after": span1,
		"edge_1h_before": span1,
		"too_late":       {},
		"too_early":      {},
		"other_service":  {},
		"no_service":     {},
		// The record's own attributes come before the span's.
		"log_wins":      {op: "chat", agent: "log_agent", system: "log_system", session: "conv-join-1", user: "user-join"},
		"no_agent_name": {op: "chat", system: "openai", session: "conv-join-2"},
		"null_service":  {op: "chat", agent: "null_bot", system: "anthropic", session: "conv-null-1"},
	}

	rows := genAILogsView(t, rig.srv.Rows(confLogs), rig.srv.Rows(confSpans))
	if len(rows) != len(want) {
		t.Fatalf("v_gen_ai_logs has %d rows, want %d: every record stays, joined or not", len(rows), len(want))
	}
	for name, e := range want {
		var match []map[string]any
		for _, r := range rows {
			if c, _ := parseAttrs(t, r).jsonValue("case"); c == name {
				match = append(match, r)
			}
		}
		if len(match) != 1 {
			t.Errorf("%s: %d rows in v_gen_ai_logs, want 1", name, len(match))
			continue
		}
		requireCols(t, name, match[0], map[string]any{
			"event_name":            "gen_ai.choice",
			"gen_ai_operation_name": e.op,
			"gen_ai_agent_name":     e.agent,
			"gen_ai_system":         e.system,
			"adk_session_id":        e.session,
			"adk_user_id":           e.user,
		})
	}
}

// Every GenAI log record of a framework is enriched with the operation, the
// agent and the session of the span it belongs to; the one record that has no
// GenAI attribute is not in the view at all.
func TestLogsViewOverFrameworkFixtures(t *testing.T) {
	rig := newConformanceRig(t)
	m := pushFrameworkFixtures(t, rig)
	rawLogs := rig.srv.Rows(confLogs)
	logs := genAILogsView(t, rawLogs, rig.srv.Rows(confSpans))

	totalInView := 0
	for _, g := range frameworkGoldens {
		entry := frameworkEntry(t, m, g.id)
		raw := rowsWhere(rawLogs, "service_name", g.service)
		rows := rowsWhere(logs, "service_name", g.service)
		totalInView += len(rows)

		switch {
		case !slices.Contains(entry.Signals, "logs"):
			if len(raw) != 0 {
				t.Errorf("%s: %d log rows written, but its manifest entry lists no logs", g.id, len(raw))
			}
		case entry.LogsOutsideGenAIView:
			if len(raw) == 0 {
				t.Errorf("%s: its log events carry no gen_ai attribute, yet none was written", g.id)
			}
		case len(rows) != len(raw):
			t.Errorf("%s: %d of its %d log rows are in v_gen_ai_logs, want all", g.id, len(rows), len(raw))
		}
		if len(rows) != g.viewLogs {
			t.Errorf("%s: %d rows in v_gen_ai_logs, want %d", g.id, len(rows), g.viewLogs)
		}

		var user any
		if len(g.userIDs) == 1 {
			user = g.userIDs[0]
		}
		for _, row := range rows {
			requireCols(t, g.id+" log", row, map[string]any{
				"gen_ai_operation_name": g.op,
				"gen_ai_agent_name":     g.agent,
				"adk_session_id":        g.session,
				"gen_ai_system":         g.system,
				"adk_user_id":           user,
			})
		}
	}
	if totalInView != len(logs) {
		t.Errorf("v_gen_ai_logs has %d rows, the frameworks account for %d", len(logs), totalInView)
	}
}

// Only the metrics named gen_ai.<something> are in v_gen_ai_metrics, each once
// however many times the batch arrived, and each carries the attributes that
// the view promotes.
func TestMetricsViewKeepsOnlyGenAIMetrics(t *testing.T) {
	rig := newConformanceRig(t)
	m := loadSDKManifest(t)
	const pointsPerFramework = 3 // input tokens, output tokens, duration

	// A collector that cannot confirm a batch sends it again.
	for range 2 {
		for _, fw := range m.Frameworks {
			rig.pushMetrics(t, loadMetrics(t, fw.MetricsFile))
		}
	}
	decoys := []string{"http.client.request.duration", "my.gen_ai.custom", "gen_ai"}
	rig.pushMetrics(t, buildGaugeMetrics("decoy-service", time.Date(2024, 8, 12, 0, 0, 5, 0, time.UTC), decoys...))

	raw := rig.srv.Rows(confMetrics)
	if want := 2*pointsPerFramework*len(m.Frameworks) + len(decoys); len(raw) != want {
		t.Fatalf("otel_metrics holds %d rows, want %d", len(raw), want)
	}
	if got, want := len(metricsDedup.apply(raw)), pointsPerFramework*len(m.Frameworks)+len(decoys); got != want {
		t.Fatalf("otel_metrics_dedup holds %d rows, want %d", got, want)
	}
	view := genAIMetricsView(t, raw)
	if want := pointsPerFramework * len(m.Frameworks); len(view) != want {
		t.Fatalf("v_gen_ai_metrics holds %d rows, want %d", len(view), want)
	}
	if rows := rowsWhere(view, "service_name", "decoy-service"); len(rows) != 0 {
		t.Errorf("v_gen_ai_metrics holds %d rows of metrics that are not gen_ai.<name>", len(rows))
	}

	for _, g := range frameworkGoldens {
		rows := rowsWhere(view, "service_name", g.service)
		tokens := rowsWhere(rows, "metric_name", "gen_ai.client.token.usage")
		durations := rowsWhere(rows, "metric_name", "gen_ai.client.operation.duration")
		if len(rows) != pointsPerFramework || len(tokens) != 2 || len(durations) != 1 {
			t.Errorf("%s: %d rows in v_gen_ai_metrics (%d token, %d duration), want 3 (2, 1)", g.id, len(rows), len(tokens), len(durations))
			continue
		}

		// The promoted columns every point of the framework shares.
		common := map[string]any{
			"gen_ai_system":         g.system,
			"gen_ai_operation_name": g.op,
			"gen_ai_request_model":  g.requestModel,
			"gen_ai_provider_name":  nil,
			"gen_ai_response_model": nil,
			"error_type":            nil,
		}
		if g.viaProviderKey {
			common["gen_ai_provider_name"] = g.system
		}
		if g.responseModel != "" {
			common["gen_ai_response_model"] = g.responseModel
		}
		with := func(extra map[string]any) map[string]any {
			out := make(map[string]any, len(common)+len(extra))
			for k, v := range common {
				out[k] = v
			}
			for k, v := range extra {
				out[k] = v
			}
			return out
		}

		for tokenType, count := range map[string]int64{"input": g.input, "output": g.output} {
			label := g.id + " " + tokenType + " token point"
			row := onlyRow(t, label, tokens, "gen_ai_token_type", tokenType)
			requireCols(t, label, row, with(map[string]any{"sum": float64(count)}))
			if exemplars, _ := row["exemplars"].([]map[string]any); len(exemplars) != 1 {
				t.Errorf("%s: %d exemplars, want 1", label, len(exemplars))
			}
		}
		requireCols(t, g.id+" duration point", durations[0], with(map[string]any{"gen_ai_token_type": nil}))
		if got, want := durations[0]["sum"], float64(g.durationMS)/1000; math.Abs(asFloat(got)-want) > 1e-9 {
			t.Errorf("%s duration point sums to %v seconds, want %v", g.id, got, want)
		}
	}
}

// Every exemplar across all seven framework metric fixtures joins to its span
// in otel_spans_dedup, and corrupting a span_id breaks the join for that row.
func TestFrameworkFixturesMetricExemplarsView(t *testing.T) {
	rig := newConformanceRig(t)
	pushFrameworkFixtures(t, rig)

	const pointsPerFramework = 3
	rawMetrics := rig.srv.Rows(confMetrics)
	rawSpans := rig.srv.Rows(confSpans)
	view := genAIMetricExemplarsView(t, rawMetrics, rawSpans)
	if want := pointsPerFramework * len(frameworkGoldens); len(view) != want {
		t.Fatalf("v_gen_ai_metric_exemplars holds %d rows, want %d", len(view), want)
	}

	for _, g := range frameworkGoldens {
		rows := rowsWhere(view, "service_name", g.service)
		if len(rows) != pointsPerFramework {
			t.Errorf("%s: %d rows in v_gen_ai_metric_exemplars, want %d", g.id, len(rows), pointsPerFramework)
			continue
		}
		for _, row := range rows {
			if row["span_name"] == nil || row["span_start_time"] == nil {
				t.Errorf("%s: exemplar row did not join to its span: %v", g.id, row)
				continue
			}
			if row["gen_ai_operation_name"] != g.op {
				t.Errorf("%s: gen_ai_operation_name = %v, want %q", g.id, row["gen_ai_operation_name"], g.op)
			}
			if got, want := asFloat(row["span_duration_ms"]), float64(g.durationMS); math.Abs(got-want) > 1e-9 {
				t.Errorf("%s: span_duration_ms = %v, want %v", g.id, got, want)
			}
			if g.noSpanView {
				// CrewAI's span carries OpenInference attributes rather than gen_ai.*,
				// so it joins in otel_spans_dedup with NULL promoted gen_ai_* span columns.
				requireCols(t, g.id+" exemplar span", row, map[string]any{
					"span_name":              "Crew.kickoff",
					"span_gen_ai_agent_name": nil,
					"adk_session_id":         nil,
				})
			} else {
				requireCols(t, g.id+" exemplar span", row, map[string]any{
					"span_gen_ai_agent_name": g.agent,
					"adk_session_id":         g.session,
				})
			}
		}
	}

	// Mutation check: corrupting span_id on one exemplar leaves 21 rows in the
	// left join, with span_name == nil on the mutated exemplar and non-nil on
	// the remaining 20.
	mutatedMetrics := slices.Clone(rawMetrics)
	firstMetric := make(map[string]any, len(mutatedMetrics[0]))
	for k, v := range mutatedMetrics[0] {
		firstMetric[k] = v
	}
	origEx, _ := firstMetric["exemplars"].([]map[string]any)
	clonedEx := slices.Clone(origEx)
	badEx := make(map[string]any, len(clonedEx[0]))
	for k, v := range clonedEx[0] {
		badEx[k] = v
	}
	badEx["span_id"] = "ffffffffffffffff"
	clonedEx[0] = badEx
	firstMetric["exemplars"] = clonedEx
	mutatedMetrics[0] = firstMetric

	mutatedView := genAIMetricExemplarsView(t, mutatedMetrics, rawSpans)
	if len(mutatedView) != len(view) {
		t.Fatalf("mutated v_gen_ai_metric_exemplars holds %d rows, want %d", len(mutatedView), len(view))
	}
	nilSpanNames := 0
	for _, row := range mutatedView {
		if row["span_name"] == nil {
			nilSpanNames++
		}
	}
	if nilSpanNames != 1 {
		t.Errorf("corrupting one exemplar span_id yielded %d rows with span_name == nil, want 1", nilSpanNames)
	}
}

func asFloat(v any) float64 {
	f, _ := v.(float64)
	return f
}

// An LLM call is an inference span, or any span that names a request model;
// but an agent or tool span is one only when it reports tokens itself, since
// otherwise it merely wraps the calls beneath it.
func TestLLMCallPredicate(t *testing.T) {
	rig := newConformanceRig(t)
	at := time.Date(2024, 8, 12, 2, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		attrs map[string]any
		call  bool
	}{
		{"chat with tokens", map[string]any{
			"gen_ai.operation.name": "chat", "gen_ai.request.model": "m",
			"gen_ai.usage.input_tokens": int64(5), "gen_ai.usage.output_tokens": int64(2),
		}, true},
		{"chat that failed before reporting tokens", map[string]any{
			"gen_ai.operation.name": "chat", "gen_ai.request.model": "m",
		}, true},
		{"generate_content without a model", map[string]any{"gen_ai.operation.name": "generate_content"}, true},
		{"text_completion without a model", map[string]any{"gen_ai.operation.name": "text_completion"}, true},
		{"embeddings without a model", map[string]any{"gen_ai.operation.name": "embeddings"}, true},
		{"other operation that names a request model", map[string]any{
			"gen_ai.operation.name": "rerank", "gen_ai.request.model": "m",
		}, true},
		{"agent span that names a model and reports no tokens", map[string]any{
			"gen_ai.operation.name": "invoke_agent", "gen_ai.request.model": "m",
		}, false},
		{"agent span that reports tokens", map[string]any{
			"gen_ai.operation.name": "invoke_agent", "gen_ai.request.model": "m",
			"gen_ai.usage.output_tokens": int64(9),
		}, true},
		{"agent span that reports tokens but names no model", map[string]any{
			"gen_ai.operation.name": "invoke_agent", "gen_ai.usage.output_tokens": int64(9),
		}, false},
		{"create_agent that names a model", map[string]any{
			"gen_ai.operation.name": "create_agent", "gen_ai.request.model": "m",
		}, false},
		{"tool span", map[string]any{
			"gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": "lookup",
		}, false},
		{"other operation without a model", map[string]any{"gen_ai.operation.name": "plan"}, false},
	}

	var specs []specSpan
	wantCalls := int64(0)
	for i, c := range cases {
		attrs := map[string]any{
			"gen_ai.provider.name":   "openai",
			"gen_ai.conversation.id": "conv-predicate",
		}
		for k, v := range c.attrs {
			attrs[k] = v
		}
		specs = append(specs, specSpan{
			trace: traceHex(i + 1), id: spanHex(i + 1), name: c.name,
			start: at, end: at.Add(time.Second), attrs: attrs,
		})
		if c.call {
			wantCalls++
		}
	}
	rig.pushTraces(t, buildTraces(t, "predicate-agent", specs...))

	spans := genAISpansView(t, rig.srv.Rows(confSpans))
	if len(spans) != len(cases) {
		t.Fatalf("v_gen_ai_spans has %d rows, want %d", len(spans), len(cases))
	}
	parents := costsWindow.tokenParents(spans)
	for i, c := range cases {
		row := onlyRow(t, c.name, spans, "span_id", spanHex(i+1))
		if got := isLLMCall(row, costsWindow.tokenLeaf(row, parents)); got != c.call {
			t.Errorf("%s: counted as an LLM call = %v, want %v", c.name, got, c.call)
		}
	}

	costs := modelCostsView(spans)
	if got := sumCol(costs, "call_count"); got != wantCalls {
		t.Errorf("v_gen_ai_model_costs counts %d calls, want %d", got, wantCalls)
	}
	sessions := sessionsView(spans)
	if len(sessions) != 1 {
		t.Fatalf("v_gen_ai_sessions has %d rows, want 1: %v", len(sessions), sessions)
	}
	requireCols(t, "session", sessions[0], map[string]any{
		"llm_call_count":         wantCalls,
		"agent_invocation_count": int64(3), // every invoke_agent span, whether or not it is a call
		"tool_call_count":        int64(1),
	})
}

type specExemplar struct {
	trace, span string
	at          time.Time
	valDouble   *float64
	valInt      *int64
	attrs       map[string]any
}

type specMetricPoint struct {
	name      string
	unit      string
	at        time.Time
	valDouble *float64
	valInt    *int64
	attrs     map[string]any
	exemplars []specExemplar
}

func buildExemplarMetrics(t testing.TB, service string, points ...specMetricPoint) pmetric.Metrics {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	if service != "" {
		rm.Resource().Attributes().PutStr("service.name", service)
	}
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("synthetic.view.conformance")
	for _, pt := range points {
		m := sm.Metrics().AppendEmpty()
		m.SetName(pt.name)
		m.SetUnit(pt.unit)
		var exList pmetric.ExemplarSlice
		if pt.valInt != nil {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			dp := sum.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(pt.at))
			dp.SetIntValue(*pt.valInt)
			putAttrs(t, dp.Attributes(), pt.attrs)
			exList = dp.Exemplars()
		} else {
			hist := m.SetEmptyHistogram()
			hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			dp := hist.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(pt.at))
			dp.SetCount(1)
			if pt.valDouble != nil {
				dp.SetSum(*pt.valDouble)
			}
			putAttrs(t, dp.Attributes(), pt.attrs)
			exList = dp.Exemplars()
		}
		for _, ex := range pt.exemplars {
			e := exList.AppendEmpty()
			e.SetTimestamp(pcommon.NewTimestampFromTime(ex.at))
			if ex.trace != "" {
				e.SetTraceID(mustTraceID(t, ex.trace))
			}
			if ex.span != "" {
				e.SetSpanID(mustSpanID(t, ex.span))
			}
			if ex.valDouble != nil {
				e.SetDoubleValue(*ex.valDouble)
			}
			if ex.valInt != nil {
				e.SetIntValue(*ex.valInt)
			}
			putAttrs(t, e.FilteredAttributes(), ex.attrs)
		}
	}
	return md
}

// v_gen_ai_metric_exemplars keeps only exemplars with non-empty trace_id and
// span_id, and enriches them with their span only when the span started within
// [m.timestamp - 24h, m.timestamp + 1h] under the same service_name.
func TestMetricExemplarsViewJoinAndFiltering(t *testing.T) {
	rig := newConformanceRig(t)
	t0 := time.Date(2024, 8, 12, 12, 0, 0, 0, time.UTC)
	valD := func(v float64) *float64 { return &v }
	valI := func(v int64) *int64 { return &v }

	rig.pushTraces(t, buildTraces(t, "exemplar-agent",
		// Span 1: inside window, failed tool call with attributes.
		specSpan{
			trace: traceHex(1), id: spanHex(1), name: "execute_tool search",
			start: t0.Add(-10 * time.Minute), end: t0.Add(-10*time.Minute + 250*time.Millisecond),
			failed: true,
			attrs: map[string]any{
				"gen_ai.operation.name":  "execute_tool",
				"gen_ai.agent.name":      "tool_bot",
				"gen_ai.tool.name":       "search",
				"gen_ai.conversation.id": "conv-ex-1",
				"error.type":             "ToolTimeout",
			},
		},
		// Span 2: exactly 24h before m.timestamp (inclusive lower boundary).
		specSpan{
			trace: traceHex(2), id: spanHex(2), name: "chat lower_edge",
			start: t0.Add(-24 * time.Hour), end: t0.Add(-24*time.Hour + time.Second),
			attrs: map[string]any{
				"gen_ai.operation.name":  "chat",
				"gen_ai.main_agent.name": "main_bot",
				"gen_ai.conversation.id": "conv-edge-low",
			},
		},
		// Span 3: exactly 1h after m.timestamp (inclusive upper boundary).
		// Carries gcp.vertex.agent.name and both session ID keys to verify
		// gcp.vertex.agent.session_id takes precedence over gen_ai.conversation.id.
		specSpan{
			trace: traceHex(3), id: spanHex(3), name: "chat upper_edge",
			start: t0.Add(time.Hour), end: t0.Add(time.Hour + time.Second),
			attrs: map[string]any{
				"gen_ai.operation.name":       "chat",
				"gcp.vertex.agent.name":       "vertex_bot",
				"gcp.vertex.agent.session_id": "adk-1",
				"gen_ai.conversation.id":      "conv-1",
			},
		},
		// Span 4: 24h + 1s before m.timestamp (outside lower boundary).
		specSpan{
			trace: traceHex(4), id: spanHex(4), name: "chat too_old",
			start: t0.Add(-24*time.Hour - time.Second), end: t0.Add(-24 * time.Hour),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "old_bot"},
		},
		// Span 5: 1h + 1s after m.timestamp (outside upper boundary).
		specSpan{
			trace: traceHex(5), id: spanHex(5), name: "chat too_new",
			start: t0.Add(time.Hour + time.Second), end: t0.Add(time.Hour + 2*time.Second),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "new_bot"},
		},
		// Span 8: competing span rows with the same (trace_id, span_id, service_name)
		// and start_time, but different name and end_time. Both survive otel_spans_dedup
		// because name is in the span dedup partition key; QUALIFY picks the later end_time.
		specSpan{
			trace: traceHex(8), id: spanHex(8), name: "chat draft",
			start: t0.Add(-2 * time.Minute), end: t0.Add(-2*time.Minute + 100*time.Millisecond),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "draft_bot"},
		},
		specSpan{
			trace: traceHex(8), id: spanHex(8), name: "chat final",
			start: t0.Add(-2 * time.Minute), end: t0.Add(-2*time.Minute + 500*time.Millisecond),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "final_bot"},
		},
		// Span 9: competing span rows with the same (trace_id, span_id, service_name)
		// and name, but different start_time inside the window. Both survive
		// otel_spans_dedup because start_time is in the span dedup partition key;
		// QUALIFY picks the later end_time/start_time.
		specSpan{
			trace: traceHex(9), id: spanHex(9), name: "chat retry",
			start: t0.Add(-3 * time.Minute), end: t0.Add(-3*time.Minute + 200*time.Millisecond),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "early_retry_bot"},
		},
		specSpan{
			trace: traceHex(9), id: spanHex(9), name: "chat retry",
			start: t0.Add(-1 * time.Minute), end: t0.Add(-1*time.Minute + 600*time.Millisecond),
			attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "late_retry_bot"},
		},
	))
	// Span 6: different service_name ("other-agent").
	rig.pushTraces(t, buildTraces(t, "other-agent", specSpan{
		trace: traceHex(6), id: spanHex(6), name: "chat wrong_service",
		start: t0, end: t0.Add(time.Second),
		attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "other_bot"},
	}))
	// Span 7: both metric and span have NULL service_name (IS NOT DISTINCT FROM).
	rig.pushTraces(t, buildTraces(t, "", specSpan{
		trace: traceHex(7), id: spanHex(7), name: "chat null_service",
		start: t0, end: t0.Add(time.Second),
		attrs: map[string]any{"gen_ai.operation.name": "chat", "gen_ai.agent.name": "null_svc_bot"},
	}))

	rig.pushMetrics(t, buildExemplarMetrics(t, "exemplar-agent",
		specMetricPoint{
			name: "gen_ai.client.operation.duration", unit: "s", at: t0, valDouble: valD(0.25),
			attrs: map[string]any{
				"gen_ai.system":         "gcp.vertex.agent",
				"gen_ai.operation.name": "execute_tool",
				"gen_ai.request.model":  "gemini-2.5-pro",
				"gen_ai.agent.name":     "tool_bot",
				"error.type":            "ToolTimeout",
			},
			exemplars: []specExemplar{
				{trace: traceHex(1), span: spanHex(1), at: t0.Add(-5 * time.Minute), valDouble: valD(0.25), attrs: map[string]any{"case": "matched_inside"}},
				{trace: traceHex(2), span: spanHex(2), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "edge_24h_before"}},
				{trace: traceHex(3), span: spanHex(3), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "edge_1h_after"}},
				{trace: traceHex(4), span: spanHex(4), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "too_old"}},
				{trace: traceHex(5), span: spanHex(5), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "too_new"}},
				{trace: traceHex(6), span: spanHex(6), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "service_mismatch"}},
				{trace: traceHex(99), span: spanHex(99), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "missing_span"}},
				{trace: traceHex(8), span: spanHex(8), at: t0, valDouble: valD(0.5), attrs: map[string]any{"case": "competing_name"}},
				{trace: traceHex(9), span: spanHex(9), at: t0, valDouble: valD(0.6), attrs: map[string]any{"case": "competing_start_time"}},
				// Two identical exemplars in the same metric point's exemplars array
				// occupy distinct exemplar_ordinal slots, so both survive QUALIFY.
				{trace: traceHex(1), span: spanHex(1), at: t0, valDouble: valD(0.25), attrs: map[string]any{"case": "duplicate_exemplar_slot"}},
				{trace: traceHex(1), span: spanHex(1), at: t0, valDouble: valD(0.25), attrs: map[string]any{"case": "duplicate_exemplar_slot"}},
				// Excluded by WHERE: empty trace_id or empty span_id.
				{trace: "", span: spanHex(1), at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "empty_trace"}},
				{trace: traceHex(1), span: "", at: t0, valDouble: valD(1.0), attrs: map[string]any{"case": "empty_span"}},
			},
		},
		// Non-gen_ai metric: excluded by v_gen_ai_metrics before unnesting.
		specMetricPoint{
			name: "http.server.request.duration", unit: "s", at: t0, valDouble: valD(0.1),
			exemplars: []specExemplar{
				{trace: traceHex(1), span: spanHex(1), at: t0, valDouble: valD(0.1), attrs: map[string]any{"case": "non_gen_ai_metric"}},
			},
		},
	))
	rig.pushMetrics(t, buildExemplarMetrics(t, "",
		specMetricPoint{
			name: "gen_ai.client.inference.usage.input_tokens", unit: "{token}", at: t0, valInt: valI(42),
			attrs: map[string]any{
				"gen_ai.provider.name":  "gcp.vertex.agent",
				"gen_ai.operation.name": "chat",
				"gen_ai.token.modality": "text",
			},
			exemplars: []specExemplar{
				{trace: traceHex(7), span: spanHex(7), at: t0, valInt: valI(42), attrs: map[string]any{"case": "null_service_match"}},
				{trace: traceHex(1), span: spanHex(1), at: t0, valInt: valI(42), attrs: map[string]any{"case": "null_vs_named_service"}},
			},
		},
	))

	rows := genAIMetricExemplarsView(t, rig.srv.Rows(confMetrics), rig.srv.Rows(confSpans))
	if len(rows) != 13 {
		t.Fatalf("v_gen_ai_metric_exemplars has %d rows, want 13 (empty IDs, non-gen_ai metrics, and duplicate span join rows excluded)", len(rows))
	}

	byCase := make(map[string]map[string]any, len(rows))
	caseCounts := make(map[string]int, len(rows))
	for _, r := range rows {
		raw, _ := r["exemplar_filtered_attributes"].(string)
		var m map[string]string
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("unmarshal exemplar_filtered_attributes %q: %v", raw, err)
		}
		byCase[m["case"]] = r
		caseCounts[m["case"]]++
	}
	for tc, wantCount := range map[string]int{
		"competing_name":          1,
		"competing_start_time":    1,
		"duplicate_exemplar_slot": 2,
	} {
		if got := caseCounts[tc]; got != wantCount {
			t.Errorf("case %q emitted %d rows, want %d", tc, got, wantCount)
		}
	}

	requireCols(t, "matched_inside", byCase["matched_inside"], map[string]any{
		"metric_timestamp":       t0.UnixMicro(),
		"service_name":           "exemplar-agent",
		"metric_name":            "gen_ai.client.operation.duration",
		"metric_unit":            "s",
		"metric_type":            "HISTOGRAM",
		"gen_ai_operation_name":  "execute_tool",
		"gen_ai_system":          "gcp.vertex.agent",
		"gen_ai_request_model":   "gemini-2.5-pro",
		"gen_ai_agent_name":      "tool_bot",
		"metric_error_type":      "ToolTimeout",
		"exemplar_timestamp":     t0.Add(-5 * time.Minute).UnixMicro(),
		"exemplar_value_double":  0.25,
		"exemplar_value_int64":   nil,
		"trace_id":               traceHex(1),
		"span_id":                spanHex(1),
		"span_start_time":        t0.Add(-10 * time.Minute).UnixMicro(),
		"span_end_time":          t0.Add(-10*time.Minute + 250*time.Millisecond).UnixMicro(),
		"span_duration_nano":     int64(250_000_000),
		"span_duration_ms":       250.0,
		"span_name":              "execute_tool search",
		"span_status_code":       int64(2),
		"span_gen_ai_agent_name": "tool_bot",
		"span_gen_ai_tool_name":  "search",
		"adk_session_id":         "conv-ex-1",
		"span_error_type":        "ToolTimeout",
	})
	requireCols(t, "edge_24h_before", byCase["edge_24h_before"], map[string]any{
		"span_name":              "chat lower_edge",
		"span_gen_ai_agent_name": "main_bot", // falls back to gen_ai.main_agent.name
		"adk_session_id":         "conv-edge-low",
	})
	requireCols(t, "edge_1h_after", byCase["edge_1h_after"], map[string]any{
		"span_name":              "chat upper_edge",
		"span_gen_ai_agent_name": "vertex_bot", // falls back to gcp.vertex.agent.name
		"adk_session_id":         "adk-1",      // gcp.vertex.agent.session_id wins over gen_ai.conversation.id
	})
	requireCols(t, "null_service_match", byCase["null_service_match"], map[string]any{
		"service_name":           nil,
		"metric_type":            "SUM",
		"gen_ai_token_modality":  "text",
		"exemplar_value_int64":   int64(42),
		"exemplar_value_double":  nil,
		"span_name":              "chat null_service",
		"span_gen_ai_agent_name": "null_svc_bot",
	})
	requireCols(t, "competing_name", byCase["competing_name"], map[string]any{
		"span_name":              "chat final",
		"span_duration_ms":       500.0,
		"span_gen_ai_agent_name": "final_bot",
	})
	requireCols(t, "competing_start_time", byCase["competing_start_time"], map[string]any{
		"span_start_time":        t0.Add(-1 * time.Minute).UnixMicro(),
		"span_duration_ms":       600.0,
		"span_gen_ai_agent_name": "late_retry_bot",
	})

	for _, unmatched := range []string{"too_old", "too_new", "service_mismatch", "missing_span", "null_vs_named_service"} {
		requireCols(t, unmatched, byCase[unmatched], map[string]any{
			"span_start_time":        nil,
			"span_end_time":          nil,
			"span_duration_nano":     nil,
			"span_duration_ms":       nil,
			"span_name":              nil,
			"span_status_code":       nil,
			"span_status_message":    nil,
			"span_gen_ai_agent_name": nil,
			"span_gen_ai_tool_name":  nil,
			"adk_session_id":         nil,
			"span_error_type":        nil,
		})
	}
	for _, excluded := range []string{"empty_trace", "empty_span", "non_gen_ai_metric"} {
		if _, found := byCase[excluded]; found {
			t.Errorf("case %q should have been filtered out of v_gen_ai_metric_exemplars", excluded)
		}
	}
}

// Two points of one metric written in one batch can agree on their
// microsecond timestamp, service, name, type, attributes, resource, scope,
// start time and receive time, and still be distinct points of the metric
// dedup view, told apart by time_unix_nano and point_fingerprint. The exemplar
// window ranks span rows per point and exemplar offset, so the exemplar of
// each point keeps its own row, whether or not its span is found.
func TestMetricExemplarsViewKeepsDistinctPointsThatShareAMicrosecond(t *testing.T) {
	t0 := time.Date(2024, 8, 12, 12, 0, 0, 0, time.UTC)
	points := []struct {
		at          time.Time
		value       int64
		trace, span string
		spanName    string
	}{
		{t0.Add(123 * time.Nanosecond), 7, traceHex(41), spanHex(41), "chat first_point"},
		{t0.Add(124 * time.Nanosecond), 9, traceHex(42), spanHex(42), "chat second_point"},
	}
	metrics := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "identity-agent")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("synthetic.view.conformance")
		m := sm.Metrics().AppendEmpty()
		m.SetName("gen_ai.client.token.usage")
		m.SetUnit("{token}")
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, p := range points {
			dp := sum.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(p.at))
			dp.SetIntValue(p.value)
			dp.Attributes().PutStr("gen_ai.token.type", "input")
			ex := dp.Exemplars().AppendEmpty()
			ex.SetTimestamp(pcommon.NewTimestampFromTime(p.at))
			ex.SetIntValue(p.value)
			ex.SetTraceID(mustTraceID(t, p.trace))
			ex.SetSpanID(mustSpanID(t, p.span))
		}
		return md
	}

	for _, tc := range []struct {
		name      string
		withSpans bool
	}{
		{"unmatched exemplars", false},
		{"matched exemplars", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newConformanceRig(t)
			if tc.withSpans {
				var spans []specSpan
				for _, p := range points {
					spans = append(spans, specSpan{
						trace: p.trace, id: p.span, name: p.spanName,
						start: t0.Add(-time.Minute), end: t0.Add(-time.Minute + time.Second),
						attrs: map[string]any{"gen_ai.operation.name": "chat"},
					})
				}
				rig.pushTraces(t, buildTraces(t, "identity-agent", spans...))
			}
			rig.pushMetrics(t, metrics())
			rawMetrics := rig.srv.Rows(confMetrics)

			metricRows := genAIMetricsView(t, rawMetrics)
			if len(metricRows) != len(points) {
				t.Fatalf("v_gen_ai_metrics has %d rows, want %d", len(metricRows), len(points))
			}
			first, second := metricRows[0], metricRows[1]
			for _, col := range []string{
				"timestamp", "start_time", "receive_time", "service_name", "metric_name", "metric_type",
				"attributes", "resource", "instrumentation_scope",
			} {
				if !reflect.DeepEqual(first[col], second[col]) {
					t.Fatalf("v_gen_ai_metrics %s = %v and %v, want the two points to agree on it", col, first[col], second[col])
				}
			}
			if got := first["timestamp"]; got != t0.UnixMicro() {
				t.Fatalf("v_gen_ai_metrics timestamp = %v, want %d", got, t0.UnixMicro())
			}
			for _, col := range []string{"time_unix_nano", "point_fingerprint"} {
				if first[col] == nil || reflect.DeepEqual(first[col], second[col]) {
					t.Fatalf("v_gen_ai_metrics %s = %v and %v, want two distinct values", col, first[col], second[col])
				}
			}

			rows := genAIMetricExemplarsView(t, rawMetrics, rig.srv.Rows(confSpans))
			if len(rows) != len(points) {
				t.Fatalf("v_gen_ai_metric_exemplars has %d rows, want %d, one for the exemplar of each point", len(rows), len(points))
			}
			for _, p := range points {
				want := map[string]any{
					"metric_timestamp":     t0.UnixMicro(),
					"trace_id":             p.trace,
					"exemplar_value_int64": p.value,
					"span_name":            nil,
					"span_start_time":      nil,
				}
				if tc.withSpans {
					want["span_name"] = p.spanName
					want["span_start_time"] = t0.Add(-time.Minute).UnixMicro()
				}
				requireCols(t, p.span, onlyRow(t, p.span, rows, "span_id", p.span), want)
			}
		})
	}
}

// In the QUALIFY window of v_gen_ai_metric_exemplars, BigQuery sorts NULL last
// in the descending span keys and first in the ascending name, so the model
// orders the rows of a slot the same way: a row with a joined span ranks
// before a row whose span columns are NULL.
func TestCompareExemplarSpanCandidateOrdersNullsAsBigQuery(t *testing.T) {
	span := func(end int64, name any) map[string]any {
		s := map[string]any{"end_time": end, "start_time": int64(2), "receive_time": int64(1)}
		if name != nil {
			s["name"] = name
		}
		return s
	}
	for _, tc := range []struct {
		name string
		a, b map[string]any
		want int
	}{
		{"joined span before unmatched row", span(3, "chat"), nil, -1},
		{"unmatched row after joined span", nil, span(3, "chat"), 1},
		{"unmatched rows tie", nil, nil, 0},
		{"later end_time first", span(3, "chat b"), span(2, "chat a"), -1},
		{"name ascending on a tie", span(3, "chat a"), span(3, "chat b"), -1},
		{"NULL name first on a tie", span(3, nil), span(3, "chat a"), -1},
	} {
		if got := compareExemplarSpanCandidate(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: compareExemplarSpanCandidate = %d, want %d", tc.name, got, tc.want)
		}
	}
}
