// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

// The tests in this file hold the Dataform package in deploy/dataform to the
// views it reads, to the dashboards that can read its tables instead of the
// views and to its README. Nothing here runs Dataform or reaches BigQuery:
// every check runs on the files of the repository.
//
//   - A .sqlx file is split into its config block, its pre_operations and
//     post_operations blocks and its SQL body. The placeholders the package
//     uses (self(), ref() and when(incremental(), ...)) are resolved for an
//     incremental run and for a full refresh, so the statements of both runs
//     can be read with the GoogleSQL reader of dashboards_conformance_test.go.
//   - Every mart keeps the columns of its view in the view's order, the staging
//     table and the trajectory mart keep the query of the view they stand in
//     for, and every identifier is resolved against the columns it reads.
//   - The refresh model that the README documents (one window of
//     INTERVAL n HOUR in every action, a DELETE and an INSERT in one
//     transaction, the Unix epoch as the window of a full refresh) is checked
//     in every action, so that a mart cannot drift from the others.
//
// Every rule is a pure function from the files to a list of problems. The tests
// run the rules on the repository and then on copies with one defect each, and
// require the rule that is meant to catch the defect to report it.

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	dfxDir  = "deploy/dataform/"
	dfxDefs = dfxDir + "definitions/"

	dfxTag       = "otel_marts"
	dfxRunWindow = "stg_run_window"
	dfxStaging   = "stg_spans_dedup"
	dfxTraj      = "m_gen_ai_agent_trajectory"
	dfxTrajView  = "v_gen_ai_agent_trajectory"
	dfxSpansView = "v_gen_ai_spans"

	// dfxSharedWindowExpr is the incremental window_start expression that every
	// staging table, mart and assertion reads from stg_run_window.
	dfxSharedWindowExpr = "( SELECT RW . WINDOW_START FROM ref." + dfxRunWindow + " AS RW )"

	dfxProjectPlaceholder = "YOUR_PROJECT_ID"
	dfxDatasetPlaceholder = "YOUR_DATASET_ID"

	dfxVarExpr = "dataform.projectConfig.vars."

	// dfxSQLXLicense opens every .sqlx file; dfxYAMLLicense opens the settings.
	dfxSQLXLicense = "/*\n * Copyright 2026 Google LLC\n * SPDX-License-Identifier: Apache-2.0\n *"
	dfxYAMLLicense = "# Copyright 2026 Google LLC\n# SPDX-License-Identifier: Apache-2.0\n"

	// dfxEpochWindow is the window_start of a first run or a full refresh, as
	// dashJoin writes it.
	dfxEpochWindow = "TIMESTAMP ( '1970-01-01 00:00:00+00' )"

	// dfxContextWhere is what the spans WITH table of the trajectory mart reads
	// and the DELETE of the mart removes, without the table alias: the spans at
	// or after context_start of the traces that have a span in the window.
	dfxContextWhere = "START_TIME >= CONTEXT_START AND EXISTS ( SELECT 1 FROM TOUCHED_TRACES AS TOUCHED WHERE " +
		"TOUCHED . TRACE_ID = TRACE_ID AND TOUCHED . SERVICE_NAME IS NOT DISTINCT FROM SERVICE_NAME )"

	// dfxTouchedWhere is the WHERE clause of touched_traces in an incremental
	// run of m_gen_ai_agent_trajectory, without the s. alias: spans in the
	// current window plus any staged span in the 24-hour context window not yet
	// present in the trajectory mart.
	dfxTouchedWhere = "START_TIME >= WINDOW_START OR ( START_TIME >= CONTEXT_START AND NOT EXISTS ( SELECT 1 FROM SELF AS M WHERE M . START_TIME >= CONTEXT_START AND M . TRACE_ID = TRACE_ID AND M . SERVICE_NAME IS NOT DISTINCT FROM SERVICE_NAME AND M . SPAN_ID = SPAN_ID ) )"

	// dfxPrevWindowInc is the incremental default expression for prev_window_start
	// in stg_run_window.
	dfxPrevWindowInc = "( SELECT MIN ( IF ( IS_COMMITTED , TIMESTAMP ( '9999-12-31 23:59:59+00' ) , WINDOW_START ) ) FROM SELF )"

	// dfxPrevWindowFull is the full-refresh default expression for prev_window_start
	// in stg_run_window: the Unix epoch so an interrupted full refresh pins the
	// watermark at 1970-01-01 00:00:00+00 for subsequent incremental retries.
	dfxPrevWindowFull = dfxEpochWindow
)

var (
	dfxCoreVersion = regexp.MustCompile(`^3\.\d+\.\d+$`)
	dfxDatasetName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

	// dfxClockRE matches the run_ts item of the clock WITH table in stg_run_window, as dashJoin writes it.
	dfxClockRE = regexp.MustCompile(`^COALESCE \( CAST \( CASE WHEN "[^"]*" = '' THEN NULL WHEN REGEXP_CONTAINS \( "[^"]*" , r'\^\\d\{4\}-\\d\{2\}-\\d\{2\}T\\d\{2\}:\\d\{2\}:\\d\{2\}\(\\\.\\d\{1,6\}\)\?\(Z\|\\\+00:00\)\$' \) THEN "[^"]*" ELSE CONCAT \( 'INVALID_ISO8601_UTC_RUN_TIMESTAMP:' , "[^"]*" \) END AS TIMESTAMP \) , CURRENT_TIMESTAMP \( \) \) AS RUN_TS$`)

	// dfxWindowRE matches the incremental window_start item of stg_run_window, as dashJoin writes it.
	dfxWindowRE = regexp.MustCompile(`^LEAST \( TIMESTAMP_TRUNC \( TIMESTAMP_SUB \( RUN_TS , INTERVAL (\d+) HOUR \) , HOUR \) , COALESCE \( PREV_WINDOW_START , TIMESTAMP \( '9999-12-31 23:59:59\+00' \) \) \) AS WINDOW_START$`)

	// dfxRunTimestampISO8601UTC matches strict ISO-8601 UTC timestamps accepted by stg_run_window.sqlx.
	dfxRunTimestampISO8601UTC = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|\+00:00)$`)

	// dfxIntervalRE finds the window that an action takes from the clock.
	dfxIntervalRE = regexp.MustCompile(`(?:CURRENT_TIMESTAMP \( \)|RUN_TS) , INTERVAL (\d+) (\w+) \)`)

	// dfxHoursRE finds the hours of an interval of the trajectory view.
	dfxHoursRE = regexp.MustCompile(`INTERVAL (\d+) HOUR`)

	dfxRefRE = regexp.MustCompile(`\$\{\s*ref\(\s*"([^"]*)"\s*\)\s*\}`)
	dfxVarRE = regexp.MustCompile(`dataform\.projectConfig\.vars\.([A-Za-z_][A-Za-z0-9_]*)`)
)

// dfxProtected are the columns of v_gen_ai_spans that no table of the package
// may hold: the free text of a span status, which sits behind a policy tag with
// enable_column_level_security, and the raw bags, which a typed table has no
// use for. A table that Dataform creates carries no policy tags.
var dfxProtected = []string{"status_message", "payload_refs", "attributes", "resource", "instrumentation_scope", "events", "links"}

// dfxActionPlan is the contract of one action of the package.
type dfxActionPlan struct {
	file        string   // under definitions/
	name        string   // the name of the action
	typ         string   // declaration, incremental or assertion
	reads       []string // the actions it reads through ref()
	deps        []string // the dependencies its config declares
	partitionBy string
	clusterBy   []string
	view        string // the view of the exporter whose columns a mart keeps
	prune       string // the predicate the DELETE and the body share, as dashJoin writes it
}

var dfxPlan = []dfxActionPlan{
	{file: "sources/otel_sources.sqlx", name: dfxSpansView, typ: "declaration"},
	{file: "sources/v_gen_ai_model_costs.sqlx", name: "v_gen_ai_model_costs", typ: "declaration"},
	{file: "sources/v_gen_ai_sessions.sqlx", name: "v_gen_ai_sessions", typ: "declaration"},
	{file: "sources/v_mcp_tool_usage.sqlx", name: "v_mcp_tool_usage", typ: "declaration"},
	{file: "staging/stg_run_window.sqlx", name: dfxRunWindow, typ: "incremental", prune: "TRUE"},
	{
		file: "staging/stg_spans_dedup.sqlx", name: dfxStaging, typ: "incremental",
		reads: []string{dfxRunWindow, dfxSpansView}, deps: []string{dfxRunWindow},
		partitionBy: "DATE(start_time)", clusterBy: []string{"service_name", "trace_id"}, prune: "START_TIME >= WINDOW_START",
	},
	{
		file: "marts/m_gen_ai_model_costs_hourly.sqlx", name: "m_gen_ai_model_costs_hourly", typ: "incremental",
		reads: []string{dfxRunWindow, "v_gen_ai_model_costs"}, deps: []string{dfxRunWindow},
		partitionBy: "DATE(usage_hour)", clusterBy: []string{"service_name", "model"},
		view: "v_gen_ai_model_costs", prune: "USAGE_HOUR >= WINDOW_START",
	},
	{
		file: "marts/m_mcp_tool_usage_hourly.sqlx", name: "m_mcp_tool_usage_hourly", typ: "incremental",
		reads: []string{dfxRunWindow, "v_mcp_tool_usage"}, deps: []string{dfxRunWindow},
		partitionBy: "DATE(usage_hour)", clusterBy: []string{"service_name", "mcp_method_name"},
		view: "v_mcp_tool_usage", prune: "USAGE_HOUR >= WINDOW_START",
	},
	{
		file: "marts/m_gen_ai_sessions_daily.sqlx", name: "m_gen_ai_sessions_daily", typ: "incremental",
		reads: []string{dfxRunWindow, "v_gen_ai_sessions"}, deps: []string{dfxRunWindow},
		partitionBy: "session_start_date", clusterBy: []string{"service_name", "agent_name"},
		view: "v_gen_ai_sessions", prune: "SESSION_START_DATE >= DATE ( WINDOW_START )",
	},
	{
		file: "marts/m_gen_ai_agent_trajectory.sqlx", name: dfxTraj, typ: "incremental",
		reads:       []string{dfxRunWindow, dfxStaging},
		deps:        []string{dfxRunWindow, dfxStaging, "m_gen_ai_model_costs_hourly", "m_mcp_tool_usage_hourly", "m_gen_ai_sessions_daily"},
		partitionBy: "DATE(start_time)", clusterBy: []string{"service_name", "trace_id"}, view: dfxTrajView,
	},
	{
		file: "assertions/assert_no_orphan_tokens.sqlx", name: "assert_no_orphan_tokens", typ: "assertion",
		reads: []string{dfxRunWindow, dfxStaging, dfxTraj}, deps: []string{dfxRunWindow},
	},
	{
		file: "assertions/assert_no_unhandled_cycles.sqlx", name: "assert_no_unhandled_cycles", typ: "assertion",
		reads: []string{dfxRunWindow, dfxStaging, dfxTraj}, deps: []string{dfxRunWindow},
	},
}

// dfxSameSet says whether two lists hold the same names, in any order.
func dfxSameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}

// dfxIdentifiers returns the column names an expression reads, in upper case
// where they are keywords and as written otherwise.
func dfxIdentifiers(expr string) []string {
	toks, err := dashTokenize(expr)
	if err != nil {
		return nil
	}
	var names []string
	for i, t := range toks {
		if t.kind != dashWord || dashSQLKeywords[t.up] || i+1 < len(toks) && toks[i+1].is("(") {
			continue
		}
		names = append(names, t.text)
	}
	return names
}

// dfxMentions says whether the tokens name the column.
func dfxMentions(toks []dashTok, col string) bool {
	return slices.ContainsFunc(toks, func(t dashTok) bool {
		return t.kind == dashWord && strings.EqualFold(t.text, col)
	})
}

// ---------------------------------------------------------------------------
// The files of the package and its settings.
// ---------------------------------------------------------------------------

// dfxValidateFiles checks which files the package holds and what each one
// opens with.
func dfxValidateFiles(files dashFileSet) []string {
	var p dashProblems
	want := map[string]bool{
		dfxDir + "README.md":              true,
		dfxDir + ".gitignore":             true,
		dfxDir + "workflow_settings.yaml": true,
	}
	for _, e := range dfxPlan {
		want[dfxDefs+e.file] = true
	}
	for _, name := range files.sortedPaths(dfxDir, "") {
		if want[name] {
			continue
		}
		hint := ""
		if strings.HasSuffix(name, "/dataform.json") {
			hint = ": Dataform core 3 rejects a project that has both workflow_settings.yaml and dataform.json"
		}
		p.addf("%s is not a file of the package%s", name, hint)
	}
	for _, name := range dashSortedKeys(want) {
		text, ok := files[name]
		if !ok {
			p.addf("%s is missing", name)
			continue
		}
		switch {
		case strings.HasSuffix(name, ".sqlx") && !strings.HasPrefix(text, dfxSQLXLicense),
			strings.HasSuffix(name, ".yaml") && !strings.HasPrefix(text, dfxYAMLLicense):
			p.addf("%s does not open with the license header", name)
		case strings.HasSuffix(name, ".gitignore") && !slices.Contains(strings.Fields(text), ".df-credentials.json"):
			p.addf("%s does not list .df-credentials.json, the file dataform init-creds writes", name)
		}
		if i := dashNonASCII(text); i >= 0 {
			p.addf("%s: byte %d is not ASCII", name, i)
		}
		if strings.Contains(text, "\r") {
			p.addf("%s has a carriage return", name)
		}
	}
	return p
}

// dfxSettings is workflow_settings.yaml. Dataform core 3 reads it instead of
// dataform.json.
type dfxSettings struct {
	DataformCoreVersion     string            `yaml:"dataformCoreVersion"`
	DefaultProject          string            `yaml:"defaultProject"`
	DefaultLocation         string            `yaml:"defaultLocation"`
	DefaultDataset          string            `yaml:"defaultDataset"`
	DefaultAssertionDataset string            `yaml:"defaultAssertionDataset"`
	Vars                    map[string]string `yaml:"vars"`
}

// dfxDecodeSettings decodes workflow_settings.yaml and rejects a key it does
// not know.
func dfxDecodeSettings(files dashFileSet) (*dfxSettings, error) {
	text, ok := files[dfxDir+"workflow_settings.yaml"]
	if !ok {
		return nil, errors.New("workflow_settings.yaml is missing")
	}
	dec := yaml.NewDecoder(strings.NewReader(text))
	dec.KnownFields(true)
	var s dfxSettings
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("workflow_settings.yaml: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow_settings.yaml holds more than one document")
	}
	return &s, nil
}

// dfxValidateSettings checks workflow_settings.yaml: the pinned core version,
// the placeholders and the variables the declarations read.
func dfxValidateSettings(files dashFileSet) []string {
	s, err := dfxDecodeSettings(files)
	if err != nil {
		return []string{err.Error()}
	}
	var p dashProblems
	if !dfxCoreVersion.MatchString(s.DataformCoreVersion) {
		p.addf("workflow_settings.yaml: dataformCoreVersion is %q, want a pinned 3.x.y release", s.DataformCoreVersion)
	}
	for _, f := range []struct{ name, got, want string }{
		{"defaultProject", s.DefaultProject, dfxProjectPlaceholder},
		{"vars.source_project", s.Vars["source_project"], dfxProjectPlaceholder},
		{"vars.source_dataset", s.Vars["source_dataset"], dfxDatasetPlaceholder},
		{"vars.run_timestamp", s.Vars["run_timestamp"], ""},
	} {
		if f.got != f.want {
			p.addf("workflow_settings.yaml: %s is %q, want the placeholder %s", f.name, f.got, f.want)
		}
	}
	if s.DefaultLocation == "" {
		p.addf("workflow_settings.yaml: defaultLocation is empty; the marts and the exporter dataset must share a location")
	}
	for _, f := range []struct{ name, got string }{
		{"defaultDataset", s.DefaultDataset},
		{"defaultAssertionDataset", s.DefaultAssertionDataset},
	} {
		if !dfxDatasetName.MatchString(f.got) {
			p.addf("workflow_settings.yaml: %s is %q, want a BigQuery dataset ID", f.name, f.got)
		}
	}
	if s.DefaultDataset == s.DefaultAssertionDataset {
		p.addf("workflow_settings.yaml: defaultDataset and defaultAssertionDataset are both %q; the assertion views need a dataset of their own", s.DefaultDataset)
	}
	read := map[string]bool{}
	for _, name := range files.sortedPaths(dfxDefs, ".sqlx") {
		for _, m := range dfxVarRE.FindAllStringSubmatch(files[name], -1) {
			read[m[1]] = true
		}
	}
	for _, name := range dashSortedKeys(read) {
		if _, ok := s.Vars[name]; !ok {
			p.addf("workflow_settings.yaml: the definitions read the variable %s, which vars does not set", name)
		}
	}
	for _, name := range dashSortedKeys(s.Vars) {
		if !read[name] {
			p.addf("workflow_settings.yaml: vars sets %s, which no definition reads", name)
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// A reader for .sqlx files.
//
// Dataform splits a .sqlx file into a config block, optional pre_operations
// and post_operations blocks and the SQL of the action. The blocks hold
// JavaScript expressions (${...}) that Dataform evaluates when it compiles the
// project. The package uses three of them: self(), ref("name") and
// when(incremental(), `a`, `b`). The reader resolves those for an incremental
// run and for a full refresh and rejects every other expression, so a
// definition cannot hide behavior from the rules in a placeholder that the
// reader does not understand.
// ---------------------------------------------------------------------------

// dfxExpr is a bare JavaScript expression of a config block, such as
// dataform.projectConfig.vars.source_project.
type dfxExpr string

// dfxReader reads the members of a config block: a JavaScript object literal
// with unquoted keys, double-quoted strings, lists, nested objects and bare
// expressions.
type dfxReader struct {
	src string
	pos int
}

func (r *dfxReader) done() bool { return r.pos >= len(r.src) }

// skip moves past white space and comments.
func (r *dfxReader) skip() {
	for r.pos < len(r.src) {
		rest := r.src[r.pos:]
		switch {
		case strings.IndexByte(" \t\r\n", rest[0]) >= 0:
			r.pos++
		case strings.HasPrefix(rest, "//"):
			if j := strings.IndexByte(rest, '\n'); j >= 0 {
				r.pos += j
			} else {
				r.pos = len(r.src)
			}
		case strings.HasPrefix(rest, "/*"):
			if j := strings.Index(rest[2:], "*/"); j >= 0 {
				r.pos += j + 4
			} else {
				r.pos = len(r.src)
			}
		default:
			return
		}
	}
}

// eat consumes c when it is the next byte.
func (r *dfxReader) eat(c byte) bool {
	r.skip()
	if r.pos < len(r.src) && r.src[r.pos] == c {
		r.pos++
		return true
	}
	return false
}

// name reads an identifier, or a dotted path of identifiers.
func (r *dfxReader) name(dotted bool) string {
	r.skip()
	start := r.pos
	for r.pos < len(r.src) {
		c := r.src[r.pos]
		inside := r.pos > start
		if !dashIsWordStart(c) && (!inside || (!dashIsDigit(c) && (!dotted || c != '.'))) {
			break
		}
		r.pos++
	}
	return r.src[start:r.pos]
}

// value reads a string, a list, an object or an expression.
func (r *dfxReader) value() (any, error) {
	r.skip()
	if r.done() {
		return nil, errors.New("a value is missing")
	}
	switch r.src[r.pos] {
	case '"':
		end, err := dashScanString(r.src, r.pos)
		if err != nil {
			return nil, err
		}
		s, err := strconv.Unquote(r.src[r.pos:end])
		if err != nil {
			return nil, fmt.Errorf("string at byte %d: %w", r.pos, err)
		}
		r.pos = end
		return s, nil
	case '[':
		r.pos++
		var list []any
		for !r.eat(']') {
			v, err := r.value()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
			if !r.eat(',') && (r.pos >= len(r.src) || r.src[r.pos] != ']') {
				return nil, fmt.Errorf("expected ',' or ']' at byte %d", r.pos)
			}
		}
		return list, nil
	case '{':
		r.pos++
		return r.object('}')
	}
	if n := r.name(true); n != "" {
		return dfxExpr(n), nil
	}
	return nil, fmt.Errorf("unexpected %q at byte %d", r.src[r.pos], r.pos)
}

// object reads members up to the closing byte, or to the end of the text when
// closing is 0. A repeated key and a missing comma are errors, as they are to
// the JavaScript engine.
func (r *dfxReader) object(closing byte) (map[string]any, error) {
	obj := map[string]any{}
	for {
		r.skip()
		switch {
		case r.done() && closing == 0:
			return obj, nil
		case r.done():
			return nil, errors.New("an object is not closed")
		case closing != 0 && r.src[r.pos] == closing:
			r.pos++
			return obj, nil
		}
		key := r.name(false)
		if key == "" {
			return nil, fmt.Errorf("expected a key at byte %d", r.pos)
		}
		if !r.eat(':') {
			return nil, fmt.Errorf("key %s: expected ':' at byte %d", key, r.pos)
		}
		v, err := r.value()
		if err != nil {
			return nil, fmt.Errorf("key %s: %w", key, err)
		}
		if _, dup := obj[key]; dup {
			return nil, fmt.Errorf("key %s appears twice", key)
		}
		obj[key] = v
		r.skip()
		if !r.eat(',') && !r.done() && r.src[r.pos] != closing {
			return nil, fmt.Errorf("expected ',' after key %s at byte %d", key, r.pos)
		}
	}
}

// dfxMatchBrace returns the index of the brace that closes the one at
// src[open], or -1. It steps over SQL strings and comments, JavaScript
// comments and template literals, and over the ${...} expressions inside the
// literals, so a brace in any of them does not count.
func dfxMatchBrace(src string, open int) int {
	stack := []byte{'{'}
	for i := open + 1; i < len(src); {
		c := src[i]
		if stack[len(stack)-1] == '`' {
			switch {
			case c == '\\':
				i += 2
			case c == '`':
				stack = stack[:len(stack)-1]
				i++
			case c == '$' && i+1 < len(src) && src[i+1] == '{':
				stack = append(stack, '{')
				i += 2
			default:
				i++
			}
			continue
		}
		switch {
		case strings.HasPrefix(src[i:], "/*"):
			j := strings.Index(src[i+2:], "*/")
			if j < 0 {
				return -1
			}
			i += j + 4
		case strings.HasPrefix(src[i:], "--"), strings.HasPrefix(src[i:], "//"):
			j := strings.IndexByte(src[i:], '\n')
			if j < 0 {
				return -1
			}
			i += j
		case c == '\'' || c == '"':
			end, err := dashScanString(src, i)
			if err != nil {
				return -1
			}
			i = end
		case c == '`':
			stack = append(stack, '`')
			i++
		case c == '{':
			stack = append(stack, '{')
			i++
		case c == '}':
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return i
			}
			i++
		default:
			i++
		}
	}
	return -1
}

// dfxTemplate returns the index after the template literal that opens at s[i].
func dfxTemplate(s string, i int) (int, error) {
	for j := i + 1; j < len(s); {
		switch {
		case s[j] == '\\':
			j += 2
		case s[j] == '`':
			return j + 1, nil
		case s[j] == '$' && j+1 < len(s) && s[j+1] == '{':
			end := dfxMatchBrace(s, j+1)
			if end < 0 {
				return 0, fmt.Errorf("unterminated ${ at byte %d", j)
			}
			j = end + 1
		default:
			j++
		}
	}
	return 0, fmt.Errorf("unterminated template literal at byte %d", i)
}

// dfxSnippet shortens text for a message.
func dfxSnippet(s string) string {
	s = dashSquash(s)
	if len(s) > 48 {
		s = s[:48] + "..."
	}
	return s
}

var dfxRefCall = regexp.MustCompile(`^ref\(\s*"([A-Za-z0-9_]+)"\s*\)$`)

// dfxExpand resolves the ${...} expressions of a text for an incremental run or
// for a full refresh. self() becomes the bare word SELF and ref("x") becomes
// the quoted name `ref.x`, so that the SQL reader can tell the references apart
// from the tables of the exporter dataset.
func dfxExpand(text string, incremental bool) (string, error) {
	return dfxExpandWithVars(text, incremental, nil)
}

func dfxExpandWithVars(text string, incremental bool, vars map[string]string) (string, error) {
	var out strings.Builder
	for {
		i := strings.Index(text, "${")
		if i < 0 {
			out.WriteString(text)
			return out.String(), nil
		}
		end := dfxMatchBrace(text, i+1)
		if end < 0 {
			return "", fmt.Errorf("unterminated ${ near %q", dfxSnippet(text[i:]))
		}
		rep, err := dfxEvalWithVars(strings.TrimSpace(text[i+2:end]), incremental, vars)
		if err != nil {
			return "", err
		}
		out.WriteString(text[:i])
		out.WriteString(rep)
		text = text[end+1:]
	}
}

// dfxEval evaluates one ${...} expression.
func dfxEvalWithVars(expr string, incremental bool, vars map[string]string) (string, error) {
	if m := dfxRefCall.FindStringSubmatch(expr); m != nil {
		return "`ref." + m[1] + "`", nil
	}
	switch {
	case expr == "self()":
		return "SELF", nil
	case expr == "JSON.stringify("+dfxVarExpr+"run_timestamp)",
		expr == "JSON.stringify("+dfxVarExpr+`run_timestamp ?? "")`:
		return strconv.Quote(vars["run_timestamp"]), nil
	case expr == dfxVarExpr+"run_timestamp":
		return "", fmt.Errorf("run_timestamp must be escaped with JSON.stringify(%srun_timestamp)", dfxVarExpr)
	case strings.HasPrefix(expr, "when(") && strings.HasSuffix(expr, ")"):
		return dfxWhenWithVars(expr[len("when("):len(expr)-1], incremental, vars)
	}
	return "", fmt.Errorf("the expression ${%s} is not one the package uses (self(), ref(\"name\"), JSON.stringify(%srun_timestamp) and when(incremental(), ...))", dfxSnippet(expr), dfxVarExpr)
}

// dfxWhenWithVars evaluates the arguments of when(incremental(), `a`[, `b`]): a for an
// incremental run, b, or nothing, for a full refresh.
func dfxWhenWithVars(args string, incremental bool, vars map[string]string) (string, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(args), "incremental()")
	if !ok {
		return "", errors.New("when() must test incremental()")
	}
	var branches []string
	for {
		rest = strings.TrimSpace(rest)
		if rest == "" {
			break
		}
		if rest, ok = strings.CutPrefix(rest, ","); !ok {
			return "", errors.New("when() arguments must be separated by commas")
		}
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, "`") {
			return "", errors.New("when() takes template literals")
		}
		end, err := dfxTemplate(rest, 0)
		if err != nil {
			return "", err
		}
		branches = append(branches, rest[1:end-1])
		rest = rest[end:]
	}
	switch {
	case len(branches) < 1 || len(branches) > 2:
		return "", fmt.Errorf("when() takes one or two template literals, got %d", len(branches))
	case incremental:
		return dfxExpandWithVars(branches[0], incremental, vars)
	case len(branches) == 2:
		return dfxExpandWithVars(branches[1], incremental, vars)
	}
	return "", nil
}

// dfxSplit splits the text of an operations block at the lines that hold only
// ---, the separator Dataform uses, and drops the empty statements.
func dfxSplit(text string) []string {
	var parts, cur []string
	flush := func() {
		if s := strings.TrimSpace(strings.Join(cur, "\n")); s != "" {
			parts = append(parts, s)
		}
		cur = nil
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "---" {
			flush()
			continue
		}
		cur = append(cur, line)
	}
	flush()
	return parts
}

// dfxStmt is one statement of an operations block. The positions of its tokens
// are byte offsets into text.
type dfxStmt struct {
	text string
	toks []dashTok
	kind string
}

// dfxStmtKinds names the statements that the package's operations consist of,
// by their first words as dashJoin writes them.
var dfxStmtKinds = []struct{ prefix, kind string }{
	{"DECLARE WINDOW_START TIMESTAMP DEFAULT", "declare window_start"},
	{"DECLARE PREV_WINDOW_START TIMESTAMP DEFAULT", "declare prev_window_start"},
	{"DECLARE CONTEXT_START TIMESTAMP DEFAULT", "declare context_start"},
	{"CREATE TEMP TABLE TOUCHED_TRACES AS", "temp table touched_traces"},
	{"CREATE TEMP TABLE TRAJECTORY_ROWS AS", "temp table trajectory_rows"},
	{"CREATE OR REPLACE TEMP TABLE _BQ_OTEL_ORPHAN_TOKEN_VIOLATIONS AS", "temp table orphan_token_violations"},
	{"BEGIN TRANSACTION", "begin transaction"},
	{"COMMIT TRANSACTION", "commit transaction"},
	{"DELETE FROM SELF", "delete"},
	{"UPDATE ref." + dfxRunWindow + " SET IS_COMMITTED = TRUE WHERE TRUE", "commit run_window"},
}

var dfxTopTempTablePrefixRE = regexp.MustCompile(`^CREATE (OR REPLACE )?(TEMP|TEMPORARY) TABLE [A-Z0-9_]+ AS$`)

// dfxCheckRecursivePlacement verifies that WITH RECURSIVE only appears at the
// top level of a query (depth == 0, index 0) or immediately after a top-level
// CREATE [OR REPLACE] TEMP TABLE <name> AS statement, never inside
// ASSERT NOT EXISTS (...) or any parenthesized subquery expression.
func dfxCheckRecursivePlacement(label string, toks []dashTok) []string {
	var p dashProblems
	for i := 0; i+1 < len(toks); i++ {
		if toks[i].up != "WITH" || toks[i+1].up != "RECURSIVE" {
			continue
		}
		if toks[i].depth > 0 {
			p.addf("%s: WITH RECURSIVE is not allowed inside a parenthesized subquery or ASSERT NOT EXISTS (...) expression (depth %d); materialize a top-level TEMP TABLE first", label, toks[i].depth)
			continue
		}
		if i > 0 {
			prefix := dashJoin(toks[:i])
			if !dfxTopTempTablePrefixRE.MatchString(prefix) {
				p.addf("%s: WITH RECURSIVE must be at the start of a query or immediately follow CREATE [OR REPLACE] TEMP TABLE <name> AS, got prefix %q", label, prefix)
			}
		}
	}
	return p
}

func dfxClassify(toks []dashTok) string {
	joined := dashJoin(toks)
	for _, k := range dfxStmtKinds {
		if joined == k.prefix || strings.HasPrefix(joined, k.prefix+" ") {
			return k.kind
		}
	}
	if strings.HasPrefix(joined, "ASSERT NOT EXISTS ( ") && strings.HasSuffix(joined, " )") {
		switch {
		case strings.Contains(joined, "'span_in_trajectory_more_than_once'"):
			return "assert no_unhandled_cycles"
		case strings.Contains(joined, "_BQ_OTEL_ORPHAN_TOKEN_VIOLATIONS"), strings.Contains(joined, "TOKEN_PARENT_COVERAGE"):
			return "assert no_orphan_tokens"
		}
	}
	return "unrecognized"
}

// dfxBlock resolves and splits the text of an operations block.
func dfxBlock(text string, incremental bool) ([]dfxStmt, error) {
	expanded, err := dfxExpand(text, incremental)
	if err != nil {
		return nil, err
	}
	var stmts []dfxStmt
	for n, part := range dfxSplit(expanded) {
		toks, err := dashTokenize(part)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", n+1, err)
		}
		if len(toks) == 0 { // nothing but a comment
			continue
		}
		stmts = append(stmts, dfxStmt{text: part, toks: toks, kind: dfxClassify(toks)})
	}
	return stmts, nil
}

// dfxStmtQuery parses the query that follows the first skip tokens of a
// statement, such as the SELECT of CREATE TEMP TABLE t AS SELECT ...
func dfxStmtQuery(st *dfxStmt, skip int) (*dashQuery, error) {
	if len(st.toks) <= skip {
		return nil, errors.New("the statement has no query")
	}
	return dashParseSQL(st.text[st.toks[skip].pos:])
}

// dfxConfig is the config block of an action, as far as the package uses it.
type dfxConfig struct {
	typ, name, description string
	database, schema       string // the bare expressions of a declaration
	tags, deps             []string
	partitionBy            string
	clusterBy              []string
}

// dfxConfigKeys are the config keys the package sets, by type of action. Every
// other key changes how Dataform runs an action in a way this package and its
// README do not describe.
var dfxConfigKeys = map[string][]string{
	"declaration": {"type", "database", "schema", "name", "description"},
	"table":       {"type", "tags", "description"},
	"incremental": {"type", "tags", "dependencies", "description", "bigquery"},
	"assertion":   {"type", "tags", "dependencies", "description"},
}

// dfxFields reads typed values out of a decoded config object and collects the
// problems.
type dfxFields struct {
	obj   map[string]any
	probs *[]string
}

func (f dfxFields) str(key string) string {
	v, ok := f.obj[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		*f.probs = append(*f.probs, fmt.Sprintf("config key %s is not a string", key))
	}
	return s
}

func (f dfxFields) expr(key string) string {
	v, ok := f.obj[key]
	if !ok {
		return ""
	}
	e, ok := v.(dfxExpr)
	if !ok {
		*f.probs = append(*f.probs, fmt.Sprintf("config key %s has to be an expression such as %ssource_project, not a literal", key, dfxVarExpr))
	}
	return string(e)
}

func (f dfxFields) list(key string) []string {
	v, ok := f.obj[key]
	if !ok {
		return nil
	}
	items, ok := v.([]any)
	var out []string
	for _, it := range items {
		s, isString := it.(string)
		ok = ok && isString
		out = append(out, s)
	}
	if !ok {
		*f.probs = append(*f.probs, fmt.Sprintf("config key %s is not a list of strings", key))
	}
	return out
}

// dfxBuildConfig reads the typed config of an action out of its decoded block.
func dfxBuildConfig(obj map[string]any) (dfxConfig, []string) {
	var cfg dfxConfig
	var probs []string
	typ, ok := obj["type"].(string)
	if !ok {
		return cfg, []string{"the config block has no type, or its type is not a string"}
	}
	allowed, known := dfxConfigKeys[typ]
	if !known {
		return cfg, []string{fmt.Sprintf("the config type %q is not one the package uses (declaration, incremental, assertion)", typ)}
	}
	for _, key := range dashSortedKeys(obj) {
		if !slices.Contains(allowed, key) {
			probs = append(probs, fmt.Sprintf("config key %s is not one the package sets on a %s action (%s)", key, typ, strings.Join(allowed, ", ")))
		}
	}
	f := dfxFields{obj: obj, probs: &probs}
	cfg.typ = typ
	cfg.name = f.str("name")
	cfg.description = f.str("description")
	cfg.database = f.expr("database")
	cfg.schema = f.expr("schema")
	cfg.tags = f.list("tags")
	cfg.deps = f.list("dependencies")
	if bq, present := obj["bigquery"]; present {
		inner, isObject := bq.(map[string]any)
		if !isObject {
			probs = append(probs, "config key bigquery is not an object")
		}
		for _, key := range dashSortedKeys(inner) {
			if key != "partitionBy" && key != "clusterBy" {
				probs = append(probs, fmt.Sprintf("config key bigquery.%s is not one the package sets (partitionBy, clusterBy)", key))
			}
		}
		g := dfxFields{obj: inner, probs: &probs}
		cfg.partitionBy = g.str("partitionBy")
		cfg.clusterBy = g.list("clusterBy")
	}
	return cfg, probs
}

// dfxAction is one .sqlx file: its blocks and its query as written.
type dfxAction struct {
	file     string // path relative to the repository root
	name     string
	cfg      dfxConfig
	pre      string // the text of the pre_operations block
	post     string // the text of the post_operations block
	body     string // the SQL, without the blocks
	problems []string
}

var dfxBlockRE = regexp.MustCompile(`(?m)^(config|pre_operations|post_operations)\s*\{`)

// dfxParseAction splits a .sqlx file into its blocks and its query.
func dfxParseAction(file, src string) *dfxAction {
	a := &dfxAction{file: file, name: strings.TrimSuffix(path.Base(file), ".sqlx")}
	rest := src
	if strings.HasPrefix(rest, "/*") {
		if end := strings.Index(rest, "*/"); end >= 0 {
			rest = rest[end+2:]
		}
	}
	var (
		body       strings.Builder
		at         int
		configText string
		haveConfig bool
		seen       = map[string]bool{}
	)
	for _, m := range dfxBlockRE.FindAllStringSubmatchIndex(rest, -1) {
		if m[0] < at {
			continue // inside the block that was just read
		}
		kind := rest[m[2]:m[3]]
		open := m[1] - 1
		end := dfxMatchBrace(rest, open)
		if end < 0 {
			a.problems = append(a.problems, fmt.Sprintf("the %s block is not closed", kind))
			break
		}
		if seen[kind] {
			a.problems = append(a.problems, fmt.Sprintf("the %s block appears twice", kind))
		}
		seen[kind] = true
		switch kind {
		case "config":
			configText, haveConfig = rest[open+1:end], true
		case "pre_operations":
			a.pre = rest[open+1 : end]
		case "post_operations":
			a.post = rest[open+1 : end]
		}
		body.WriteString(rest[at:m[0]])
		at = end + 1
	}
	body.WriteString(rest[at:])
	a.body = strings.TrimSpace(body.String())
	if !haveConfig {
		a.problems = append(a.problems, "there is no config block")
		return a
	}
	obj, err := (&dfxReader{src: configText}).object(0)
	if err != nil {
		a.problems = append(a.problems, fmt.Sprintf("the config block: %v", err))
		return a
	}
	cfg, probs := dfxBuildConfig(obj)
	a.cfg = cfg
	a.problems = append(a.problems, probs...)
	if cfg.name != "" {
		a.name = cfg.name
	}
	return a
}

// dfxRun is an action as one kind of run executes it.
type dfxRun struct {
	pre, post []dfxStmt
	body      string
}

// find returns the first statement of pre_operations or post_operations of the given kind.
func (r *dfxRun) find(kind string) *dfxStmt {
	for i := range r.pre {
		if r.pre[i].kind == kind {
			return &r.pre[i]
		}
	}
	for i := range r.post {
		if r.post[i].kind == kind {
			return &r.post[i]
		}
	}
	return nil
}

func dfxKinds(stmts []dfxStmt) []string {
	var kinds []string
	for _, st := range stmts {
		kinds = append(kinds, st.kind)
	}
	return kinds
}

// dfxRender resolves an action for an incremental run or for a full refresh.
func dfxRender(a *dfxAction, incremental bool) (*dfxRun, error) {
	return dfxRenderWithVars(a, incremental, nil)
}

func dfxRenderWithVars(a *dfxAction, incremental bool, vars map[string]string) (*dfxRun, error) {
	var (
		run dfxRun
		err error
	)
	if run.pre, err = dfxBlock(a.pre, incremental); err != nil {
		return nil, fmt.Errorf("pre_operations: %w", err)
	}
	if run.post, err = dfxBlock(a.post, incremental); err != nil {
		return nil, fmt.Errorf("post_operations: %w", err)
	}
	if run.body, err = dfxExpandWithVars(a.body, incremental, vars); err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	run.body = strings.TrimSpace(run.body)
	return &run, nil
}

// dfxWindowExpr returns the expression a DECLARE window_start or DECLARE
// context_start statement assigns, as dashJoin writes it.
func dfxWindowExpr(st *dfxStmt) string { return dashJoin(st.toks[4:]) }

// dfxPackage is every action of definitions/.
type dfxPackage struct {
	byFile map[string]*dfxAction
	byName map[string]*dfxAction
	probs  []string
}

// dfxLoad reads every .sqlx file under definitions/.
func dfxLoad(files dashFileSet) *dfxPackage {
	pkg := &dfxPackage{byFile: map[string]*dfxAction{}, byName: map[string]*dfxAction{}}
	for _, file := range files.sortedPaths(dfxDefs, ".sqlx") {
		a := dfxParseAction(file, files[file])
		pkg.byFile[file] = a
		if prev, dup := pkg.byName[a.name]; dup {
			pkg.probs = append(pkg.probs, fmt.Sprintf("%s and %s both define the action %s", prev.file, file, a.name))
			continue
		}
		pkg.byName[a.name] = a
	}
	return pkg
}

// dfxMartColumns returns the columns the incremental run of a mart inserts.
func dfxMartColumns(files dashFileSet, mart string) ([]string, error) {
	a := dfxLoad(files).byName[mart]
	if a == nil {
		return nil, fmt.Errorf("the package has no action %s", mart)
	}
	run, err := dfxRender(a, true)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", mart, err)
	}
	q, err := dashParseSQL(run.body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", mart, err)
	}
	return q.columns(0)
}

// ---------------------------------------------------------------------------
// Helpers for comparing queries.
// ---------------------------------------------------------------------------

// dfxDiffColumns says how a column list differs from the one it should be, or
// returns "" when they are equal.
func dfxDiffColumns(got, want []string) string {
	if slices.Equal(got, want) {
		return ""
	}
	var parts []string
	for _, c := range want {
		if !slices.Contains(got, c) {
			parts = append(parts, "lacks "+c)
		}
	}
	for _, c := range got {
		if !slices.Contains(want, c) {
			parts = append(parts, "has the extra column "+c)
		}
	}
	if len(parts) == 0 {
		for i := 0; i < min(len(got), len(want)); i++ {
			if got[i] != want[i] {
				parts = append(parts, fmt.Sprintf("column %d is %s, want %s", i+1, got[i], want[i]))
				break
			}
		}
	}
	if len(got) != len(want) {
		parts = append(parts, fmt.Sprintf("%d columns, want %d", len(got), len(want)))
	}
	return strings.Join(parts, "; ")
}

// dfxDiffSeq returns the first difference between two lists of entries, or "".
func dfxDiffSeq(got, want []string) string {
	for i := 0; i < min(len(got), len(want)); i++ {
		if got[i] != want[i] {
			return fmt.Sprintf("entry %d is %q, want %q", i+1, got[i], want[i])
		}
	}
	switch {
	case len(got) > len(want):
		return fmt.Sprintf("has the extra entry %q", got[len(want)])
	case len(got) < len(want):
		return fmt.Sprintf("lacks the entry %q", want[len(got)])
	}
	return ""
}

// dfxClauseOrder lists the clauses of a SELECT in the order they are written.
var dfxClauseOrder = []string{"FROM", "WHERE", "GROUP BY", "HAVING", "QUALIFY", "ORDER BY", "LIMIT"}

// dfxItemSigs returns the select items of a branch, as dashJoin writes them,
// without the status_message projections that the package leaves out.
func dfxItemSigs(b *dashBranch) []string {
	var items []string
	for _, it := range b.items {
		if it.name != "status_message" {
			items = append(items, dashJoin(it.toks))
		}
	}
	return items
}

// dfxBranchSig describes a SELECT by its items and its clauses, apart from the
// status_message projections.
func dfxBranchSig(b *dashBranch) []string {
	var sig []string
	if len(b.toks) > 1 && b.toks[1].up == "DISTINCT" {
		sig = append(sig, "DISTINCT")
	}
	for _, it := range dfxItemSigs(b) {
		sig = append(sig, "item "+it)
	}
	for _, name := range dfxClauseOrder {
		if toks, ok := b.clauses[name]; ok {
			sig = append(sig, name+" "+dashJoin(toks))
		}
	}
	return sig
}

// dfxQuerySig describes the SELECT branches of a query expression.
func dfxQuerySig(q *dashQuery) []string {
	var sig []string
	for i, b := range q.branches {
		sig = append(sig, fmt.Sprintf("branch %d", i+1))
		sig = append(sig, dfxBranchSig(b)...)
	}
	return sig
}

// dfxJoinWithout writes tokens like dashJoin, leaving out the qualifier alias
// and its dot, so that s.start_time and start_time compare equal.
func dfxJoinWithout(toks []dashTok, alias string) string {
	kept := make([]dashTok, 0, len(toks))
	for i := 0; i < len(toks); i++ {
		if toks[i].kind == dashWord && strings.EqualFold(toks[i].text, alias) && i+1 < len(toks) && toks[i+1].is(".") {
			i++
			continue
		}
		kept = append(kept, toks[i])
	}
	return dashJoin(kept)
}

// dfxCTE returns the WITH table of a query by name, or nil.
func dfxCTE(q *dashQuery, name string) *dashQuery {
	for _, c := range q.ctes {
		if c.name == name {
			return c.query
		}
	}
	return nil
}

func dfxCTENames(q *dashQuery) []string {
	names := make([]string, len(q.ctes))
	for i, c := range q.ctes {
		names[i] = c.name
	}
	return names
}

// dfxTrajectoryView parses the query of v_gen_ai_agent_trajectory, which the
// staging table and the trajectory mart take their definitions from.
func dfxTrajectoryView() (*dashQuery, error) {
	sql, ok := dashViewSQLs()[dfxTrajView]
	if !ok {
		return nil, fmt.Errorf("no view query is known for %s", dfxTrajView)
	}
	q, err := dashParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", dfxTrajView, err)
	}
	return q, nil
}

// dfxReachHours returns the longest interval of the trajectory view in hours: how
// far its recursion reaches from a span to a parent or to a child.
func dfxReachHours() (int, error) {
	sql, ok := dashViewSQLs()[dfxTrajView]
	if !ok {
		return 0, fmt.Errorf("no view query is known for %s", dfxTrajView)
	}
	reach := 0
	for _, m := range dfxHoursRE.FindAllStringSubmatch(sql, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, err
		}
		reach = max(reach, n)
	}
	if reach == 0 {
		return 0, fmt.Errorf("%s has no INTERVAL n HOUR", dfxTrajView)
	}
	return reach, nil
}

// dfxLookback returns the number of hours of the window of stg_run_window:
// the number every other action has to share.
func dfxLookback(pkg *dfxPackage) (int, error) {
	a := pkg.byName[dfxRunWindow]
	if a == nil {
		return 0, fmt.Errorf("the package has no action %s", dfxRunWindow)
	}
	run, err := dfxRender(a, true)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", dfxRunWindow, err)
	}
	q, err := dashParseSQL(run.body)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", dfxRunWindow, err)
	}
	clock := dfxCTE(q, "clock")
	if len(q.ctes) != 1 || clock == nil || len(clock.branches) != 1 || len(clock.branches[0].items) != 1 {
		return 0, fmt.Errorf("%s: expected 1 WITH table clock with 1 item (run_ts)", dfxRunWindow)
	}
	clockExpr := dashJoin(clock.branches[0].items[0].toks)
	if !dfxClockRE.MatchString(clockExpr) {
		return 0, fmt.Errorf("%s: clock run_ts item is %q, want strict ISO-8601 UTC REGEXP_CONTAINS + CAST(... AS TIMESTAMP) (must use CAST, not SAFE_CAST, so an invalid vars.run_timestamp fails fast)", dfxRunWindow, clockExpr)
	}
	if len(q.branches) != 1 || len(q.branches[0].items) != 3 || dashJoin(q.branches[0].clauses["FROM"]) != "CLOCK" {
		return 0, fmt.Errorf("%s: expected 1 SELECT with 3 items (run_ts, window_start, is_committed) FROM clock", dfxRunWindow)
	}
	if got := dashJoin(q.branches[0].items[0].toks); got != "RUN_TS" {
		return 0, fmt.Errorf("%s: run_ts item is %q, want RUN_TS", dfxRunWindow, got)
	}
	if got := dashJoin(q.branches[0].items[2].toks); got != "FALSE AS IS_COMMITTED" {
		return 0, fmt.Errorf("%s: is_committed item is %q, want FALSE AS IS_COMMITTED", dfxRunWindow, got)
	}
	expr := dashJoin(q.branches[0].items[1].toks)
	m := dfxWindowRE.FindStringSubmatch(expr)
	if m == nil {
		return 0, fmt.Errorf("%s: window_start item is %q, want LEAST ( TIMESTAMP_TRUNC ( TIMESTAMP_SUB ( RUN_TS , INTERVAL n HOUR ) , HOUR ) , COALESCE ( PREV_WINDOW_START , TIMESTAMP ( '9999-12-31 23:59:59+00' ) ) ) AS WINDOW_START", dfxRunWindow, expr)
	}
	hours, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, err
	}
	fullRun, err := dfxRender(a, false)
	if err != nil {
		return 0, fmt.Errorf("%s full refresh: %w", dfxRunWindow, err)
	}
	fullQ, err := dashParseSQL(fullRun.body)
	if err != nil {
		return 0, fmt.Errorf("%s full refresh: %w", dfxRunWindow, err)
	}
	wantFull := fmt.Sprintf("LEAST ( TIMESTAMP_TRUNC ( TIMESTAMP_SUB ( RUN_TS , INTERVAL %d HOUR ) , HOUR ) , %s ) AS WINDOW_START", hours, dfxEpochWindow)
	if len(fullQ.branches) != 1 || len(fullQ.branches[0].items) != 3 || dashJoin(fullQ.branches[0].items[1].toks) != wantFull {
		return 0, fmt.Errorf("%s: full-refresh window_start item must be %q", dfxRunWindow, wantFull)
	}
	return hours, nil
}

// dfxWindowText writes the incremental window_start of n hours as dashJoin
// writes it.
func dfxWindowText(n int) string {
	return fmt.Sprintf("TIMESTAMP_TRUNC ( TIMESTAMP_SUB ( CURRENT_TIMESTAMP ( ) , INTERVAL %d HOUR ) , HOUR )", n)
}

// dfxRefs returns the actions an action reads through ref(), sorted.
func dfxRefs(a *dfxAction) []string {
	seen := map[string]bool{}
	for _, text := range []string{a.pre, a.post, a.body} {
		for _, m := range dfxRefRE.FindAllStringSubmatch(text, -1) {
			seen[m[1]] = true
		}
	}
	return dashSortedKeys(seen)
}

// dfxCycle returns a dependency cycle of the graph as a path that ends where it
// starts, or nil.
func dfxCycle(edges map[string][]string) []string {
	const (
		unseen = iota
		open
		closed
	)
	var (
		state = map[string]int{}
		stack []string
		found []string
		visit func(node string) bool
	)
	visit = func(node string) bool {
		state[node] = open
		stack = append(stack, node)
		for _, next := range edges[node] {
			switch state[next] {
			case open:
				found = append(slices.Clone(stack[slices.Index(stack, next):]), next)
				return true
			case unseen:
				if visit(next) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[node] = closed
		return false
	}
	for _, node := range dashSortedKeys(edges) {
		if state[node] == unseen && visit(node) {
			return found
		}
	}
	return nil
}

// dfxSource is a table that a SELECT reads: a WITH table, a table of the
// package (as `ref.name`) or a temporary table.
type dfxSource struct {
	name, alias string
	top         bool // read by the FROM clause of the SELECT itself, not by a subquery
}

// dfxFromEnd are the words that end the FROM clause of a SELECT.
var dfxFromEnd = map[string]bool{
	"WHERE": true, "GROUP": true, "ORDER": true, "HAVING": true, "QUALIFY": true,
	"LIMIT": true, "UNION": true, "INTERSECT": true, "EXCEPT": true, "WINDOW": true,
}

// dfxInSubquery says whether the token at toks[k] sits in a bracket that opens
// with SELECT.
func dfxInSubquery(toks []dashTok, k int) bool {
	d := toks[k].depth
	for j := k - 1; j >= 0; j-- {
		if toks[j].depth == d-1 {
			return j+1 < len(toks) && (toks[j+1].up == "SELECT" || toks[j+1].up == "WITH")
		}
	}
	return false
}

// dfxFromList reads the tables of one FROM clause: the first one and the ones
// after JOIN and after a comma.
func dfxFromList(toks []dashTok, from int) []dfxSource {
	d := toks[from].depth
	var out []dfxSource
	expect := true
	for p := from + 1; p < len(toks) && toks[p].depth >= d; p++ {
		t := toks[p]
		if t.depth > d {
			continue
		}
		switch {
		case t.kind == dashWord && dfxFromEnd[t.up]:
			return out
		case t.kind == dashWord && t.up == "JOIN", t.is(","):
			expect = true
		case expect:
			expect = false
			if t.kind != dashIdent && (t.kind != dashWord || dashSQLKeywords[t.up]) {
				continue // UNNEST(...), a subquery or a keyword
			}
			src := dfxSource{name: t.text}
			if p+2 < len(toks) && toks[p+1].up == "AS" && toks[p+1].kind == dashWord {
				src.alias = toks[p+2].text
			} else if p+1 < len(toks) && toks[p+1].kind == dashWord && !dashSQLKeywords[toks[p+1].up] &&
				(p+2 >= len(toks) || !toks[p+2].is("(")) {
				src.alias = toks[p+1].text
			}
			out = append(out, src)
		}
	}
	return out
}

// dfxFromSources returns the tables read by the FROM clause of a SELECT and by
// the subqueries inside it.
func dfxFromSources(toks []dashTok) []dfxSource {
	if len(toks) == 0 {
		return nil
	}
	base := toks[0].depth
	var out []dfxSource
	for k, t := range toks {
		if t.kind != dashWord || t.up != "FROM" || k == 0 || toks[k-1].up == "DISTINCT" {
			continue
		}
		if t.depth != base && !dfxInSubquery(toks, k) {
			continue
		}
		for _, src := range dfxFromList(toks, k) {
			src.top = t.depth == base
			out = append(out, src)
		}
	}
	return out
}

// dfxSourceColumns returns the columns of a table that a SELECT reads.
func dfxSourceColumns(owner *dashQuery, name string, tables map[string][]string) ([]string, bool) {
	if cols, ok := tables[name]; ok {
		return cols, true
	}
	if q := owner.lookup(name); q != nil {
		if cols, err := q.columns(0); err == nil {
			return cols, true
		}
	}
	return nil, false
}

// dfxResolveBranch resolves the identifiers of one SELECT against the tables it
// reads.
func dfxResolveBranch(b *dashBranch, tables map[string][]string, vars []string) []string {
	var probs []string
	src := dashSource{cols: dashSet(vars), quals: map[string]map[string]bool{}}
	for name := range tables {
		src.cols[name] = true
	}
	for _, s := range dfxFromSources(b.toks) {
		cols, ok := dfxSourceColumns(b.owner, s.name, tables)
		if !ok {
			probs = append(probs, fmt.Sprintf("FROM %s: not a WITH table, a table of the package or a temporary table of the action", s.name))
			continue
		}
		alias := s.alias
		if alias == "" {
			alias = s.name
		}
		src.quals[alias] = dashSet(cols)
		src.cols[alias], src.cols[s.name] = true, true
		if s.top {
			for _, c := range cols {
				src.cols[c] = true
			}
		}
	}
	return append(probs, dashCheckBranch(b, src)...)
}

// dfxResolve resolves the identifiers of every WITH table and of the final
// SELECT of a query against the tables they read. tables maps the name of a
// table that is not a WITH table to its columns; vars are the script variables
// in scope.
func dfxResolve(q *dashQuery, tables map[string][]string, vars ...string) []string {
	var p dashProblems
	var walk func(q *dashQuery, where string)
	walk = func(q *dashQuery, where string) {
		for _, c := range q.ctes {
			walk(c.query, where+"WITH table "+c.name+": ")
		}
		for i, b := range q.branches {
			label := where
			if label == "" {
				label = "the final SELECT: "
			}
			if len(q.branches) > 1 {
				label += fmt.Sprintf("branch %d: ", i+1)
			}
			for _, msg := range dfxResolveBranch(b, tables, vars) {
				p.addf("%s%s", label, msg)
			}
		}
	}
	walk(q, "")
	return p
}

// ---------------------------------------------------------------------------
// The actions: their config and their dependency graph.
// ---------------------------------------------------------------------------

// dfxCheckConfig checks the config block of an action against the plan.
func dfxCheckConfig(a *dfxAction, e dfxActionPlan) []string {
	var p dashProblems
	cfg := a.cfg
	if a.name != e.name {
		p.addf("%s defines the action %s, want %s", a.file, a.name, e.name)
	}
	if cfg.typ != e.typ {
		p.addf("%s: type is %q, want %q", a.name, cfg.typ, e.typ)
	}
	if cfg.description == "" {
		p.addf("%s: the config has no description", a.name)
	}
	if e.typ == "declaration" {
		for _, f := range []struct{ key, got, want string }{
			{"database", cfg.database, dfxVarExpr + "source_project"},
			{"schema", cfg.schema, dfxVarExpr + "source_dataset"},
		} {
			if f.got != f.want {
				p.addf("%s: %s is %q, want the expression %s so that the source is set in workflow_settings.yaml", a.name, f.key, f.got, f.want)
			}
		}
		if len(cfg.tags) != 0 || len(cfg.deps) != 0 {
			p.addf("%s: a declaration has no tags and no dependencies", a.name)
		}
		if a.body != "" || strings.TrimSpace(a.pre+a.post) != "" {
			p.addf("%s: a declaration holds no SQL", a.name)
		}
		return p
	}
	if !slices.Equal(cfg.tags, []string{dfxTag}) {
		p.addf("%s: tags are %v, want [%s]: dataform run --tags %s selects the package by that tag", a.name, cfg.tags, dfxTag, dfxTag)
	}
	if !dfxSameSet(cfg.deps, e.deps) {
		p.addf("%s: dependencies are %v, want %v", a.name, cfg.deps, e.deps)
	}
	if e.typ == "incremental" && e.name != dfxRunWindow {
		if cfg.partitionBy != e.partitionBy {
			p.addf("%s: partitionBy is %q, want %q", a.name, cfg.partitionBy, e.partitionBy)
		}
		if !slices.Equal(cfg.clusterBy, e.clusterBy) {
			p.addf("%s: clusterBy is %v, want %v", a.name, cfg.clusterBy, e.clusterBy)
		}
	}
	if a.body == "" {
		p.addf("%s: the action has no query", a.name)
	}
	return p
}

// dfxCheckGraph checks what every action reads through ref(), that the
// dependencies exist and that the graph has no cycle.
func dfxCheckGraph(pkg *dfxPackage) []string {
	var p dashProblems
	edges := map[string][]string{}
	read := map[string]bool{}
	for _, e := range dfxPlan {
		a := pkg.byFile[dfxDefs+e.file]
		if a == nil {
			continue
		}
		refs := dfxRefs(a)
		if !dfxSameSet(refs, e.reads) {
			p.addf("%s: reads %v through ref(), want %v", a.name, refs, e.reads)
		}
		for _, name := range refs {
			switch target := pkg.byName[name]; {
			case target == nil:
				p.addf("%s: ref(%q) names no action of the package", a.name, name)
			case target.cfg.typ == "assertion":
				p.addf("%s: ref(%q) names an assertion, which no action may read", a.name, name)
			default:
				read[name] = true
			}
		}
		for _, dep := range a.cfg.deps {
			if pkg.byName[dep] == nil {
				p.addf("%s: the dependency %q names no action of the package", a.name, dep)
			}
		}
		edges[a.name] = slices.Concat(refs, a.cfg.deps)
	}
	for _, e := range dfxPlan {
		if e.typ == "declaration" && !read[e.name] {
			p.addf("the declaration %s is never read", e.name)
		}
	}
	if cycle := dfxCycle(edges); cycle != nil {
		p.addf("dependency cycle: %s", strings.Join(cycle, " -> "))
	}
	return p
}

// dfxValidateActions checks the config blocks and the dependency graph.
func dfxValidateActions(files dashFileSet) []string {
	pkg := dfxLoad(files)
	p := dashProblems(slices.Clone(pkg.probs))
	for _, e := range dfxPlan {
		a := pkg.byFile[dfxDefs+e.file]
		if a == nil {
			continue // dfxValidateFiles reports the missing file
		}
		for _, msg := range a.problems {
			p.addf("%s: %s", a.file, msg)
		}
		p = append(p, dfxCheckConfig(a, e)...)
	}
	return append(p, dfxCheckGraph(pkg)...)
}

// ---------------------------------------------------------------------------
// The refresh model.
// ---------------------------------------------------------------------------

// dfxCheckDelete checks the DELETE of an incremental run: what it removes is
// what the query of the same run inserts again.
func dfxCheckDelete(a *dfxAction, e dfxActionPlan, st *dfxStmt) []string {
	toks := st.toks // DELETE FROM SELF [AS alias] WHERE predicate
	i, alias := 3, ""
	if i+1 < len(toks) && toks[i].up == "AS" {
		alias = toks[i+1].text
		i += 2
	}
	if i >= len(toks) || toks[i].up != "WHERE" {
		return []string{a.name + ": the DELETE has no WHERE clause, so it would empty the table"}
	}
	var p dashProblems
	pred := toks[i+1:]
	wantAlias, wantPred, got := "", e.prune, dashJoin(pred)
	if e.name == dfxTraj {
		wantAlias, wantPred, got = "mart", dfxContextWhere, dfxJoinWithout(pred, "mart")
	}
	if alias != wantAlias {
		p.addf("%s: the DELETE names the mart %q, want %q", a.name, alias, wantAlias)
	}
	if got != wantPred {
		p.addf("%s: the DELETE removes the rows WHERE %s, want WHERE %s", a.name, got, wantPred)
	}
	return p
}

// dfxCheckRun checks the operations of an action for an incremental run or for
// a full refresh: which statements run, in which order, and the window they
// share.
func dfxCheckRun(a *dfxAction, e dfxActionPlan, run *dfxRun, incremental bool, reach int) []string {
	var p dashProblems
	mode := "a full refresh"
	wantPre := []string{"declare window_start"}
	if e.name == dfxRunWindow {
		wantPre = []string{"declare prev_window_start"}
	}
	var wantPost []string
	if e.name == dfxTraj {
		wantPre = append(wantPre, "declare context_start", "temp table touched_traces", "temp table trajectory_rows")
	}
	if incremental {
		mode = "an incremental run"
		wantPre = append(wantPre, "begin transaction", "delete")
		wantPost = []string{"commit transaction"}
	}
	if e.name == dfxTraj {
		wantPost = append(wantPost, "assert no_unhandled_cycles", "temp table orphan_token_violations", "assert no_orphan_tokens", "commit run_window")
	}
	for _, st := range slices.Concat(run.pre, run.post) {
		p = append(p, dfxCheckRecursivePlacement(fmt.Sprintf("%s: in %s, %s", a.name, mode, st.kind), st.toks)...)
	}
	if got := dfxKinds(run.pre); !slices.Equal(got, wantPre) {
		p.addf("%s: in %s, pre_operations run %v, want %v", a.name, mode, got, wantPre)
	}
	if got := dfxKinds(run.post); !slices.Equal(got, wantPost) {
		p.addf("%s: in %s, post_operations run %v, want %v", a.name, mode, got, wantPost)
	}
	if st := run.find("declare prev_window_start"); st != nil {
		got := dfxWindowExpr(st)
		switch {
		case !incremental && got != dfxPrevWindowFull:
			p.addf("%s: in %s, prev_window_start is %s, want %s", a.name, mode, got, dfxPrevWindowFull)
		case incremental && got != dfxPrevWindowInc:
			p.addf("%s: in %s, prev_window_start is %s, want %s", a.name, mode, got, dfxPrevWindowInc)
		}
	}
	if st := run.find("declare window_start"); st != nil {
		got := dfxWindowExpr(st)
		switch {
		case !incremental && got != dfxEpochWindow:
			p.addf("%s: in %s, window_start is %s, want the Unix epoch %s", a.name, mode, got, dfxEpochWindow)
		case incremental && got != dfxSharedWindowExpr:
			p.addf("%s: in %s, window_start is %s, want %s: every action shares one window from %s", a.name, mode, got, dfxSharedWindowExpr, dfxRunWindow)
		}
	}
	if st := run.find("declare context_start"); st != nil && reach > 0 {
		if got, want := dfxWindowExpr(st), fmt.Sprintf("TIMESTAMP_SUB ( WINDOW_START , INTERVAL %d HOUR )", reach); got != want {
			p.addf("%s: in %s, context_start is %s, want %s, the reach of the view's recursion", a.name, mode, got, want)
		}
	}
	if st := run.find("delete"); st != nil {
		p = append(p, dfxCheckDelete(a, e, st)...)
	}
	return p
}

// dfxCheckClock checks that stg_run_window takes the shared lookback in hours
// from the clock and that no downstream action bypasses stg_run_window by
// calling CURRENT_TIMESTAMP() directly.
func dfxCheckClock(a *dfxAction, run *dfxRun, lookback int) []string {
	var p dashProblems
	var parts []string
	for _, st := range slices.Concat(run.pre, run.post) {
		parts = append(parts, dashJoin(st.toks))
	}
	toks, err := dashTokenize(run.body)
	if err != nil {
		return []string{fmt.Sprintf("%s: the query: %v", a.name, err)}
	}
	parts = append(parts, dashJoin(toks))
	joined := strings.Join(parts, " ; ")
	matches := dfxIntervalRE.FindAllStringSubmatch(joined, -1)
	wantHours := lookback
	if wantHours == 0 {
		wantHours = 3
	}
	if a.name == dfxRunWindow && len(matches) == 0 {
		p.addf("%s: never takes an interval from the clock, so it has no INTERVAL %d HOUR window", a.name, wantHours)
	}
	for _, m := range matches {
		if m[2] != "HOUR" || m[1] != strconv.Itoa(wantHours) {
			p.addf("%s: takes INTERVAL %s %s from the clock, want INTERVAL %d HOUR in every action", a.name, m[1], m[2], wantHours)
		} else if a.name != dfxRunWindow {
			p.addf("%s: calls CURRENT_TIMESTAMP() directly instead of reading %s", a.name, dfxRunWindow)
		}
	}
	if a.name != dfxRunWindow && !strings.Contains(joined, "ref."+dfxRunWindow) {
		p.addf("%s: does not read window_start from %s", a.name, dfxRunWindow)
	}
	return p
}

// dfxValidateRefresh checks that every incremental action replaces the same
// window in one transaction and that a full refresh rebuilds all of it.
func dfxValidateRefresh(files dashFileSet) []string {
	pkg := dfxLoad(files)
	var p dashProblems
	lookback, err := dfxLookback(pkg)
	if err != nil {
		p.addf("%v", err)
	}
	reach, err := dfxReachHours()
	if err != nil {
		p.addf("%v", err)
	}
	for _, e := range dfxPlan {
		a := pkg.byFile[dfxDefs+e.file]
		if a == nil || e.typ == "declaration" {
			continue
		}
		inc, err := dfxRender(a, true)
		if err != nil {
			p.addf("%s: an incremental run: %v", a.name, err)
			continue
		}
		p = append(p, dfxCheckClock(a, inc, lookback)...)
		if e.typ != "incremental" {
			continue
		}
		p = append(p, dfxCheckRun(a, e, inc, true, reach)...)
		full, err := dfxRender(a, false)
		if err != nil {
			p.addf("%s: a full refresh: %v", a.name, err)
			continue
		}
		p = append(p, dfxCheckRun(a, e, full, false, reach)...)
	}
	return p
}

// ---------------------------------------------------------------------------
// The marts: their columns, their queries and what they must not hold.
// ---------------------------------------------------------------------------

// dfxPlainItems returns the output columns of a SELECT whose items have to be
// plain columns, and reports the items that are not.
func dfxPlainItems(a *dfxAction, b *dashBranch) ([]string, []string) {
	var cols, probs []string
	for n, it := range b.items {
		switch {
		case it.star:
			probs = append(probs, fmt.Sprintf("%s: select item %d is a star; list the columns so that the schema of the table is explicit", a.name, n+1))
		case len(it.toks) != 1 || it.toks[0].kind != dashWord:
			probs = append(probs, fmt.Sprintf("%s: select item %d is not a plain column", a.name, n+1))
		default:
			cols = append(cols, it.name)
		}
	}
	return cols, probs
}

// dfxCheckProtected checks that the table of an action holds none of the
// protected columns and that no statement of the action reads status_message.
func dfxCheckProtected(a *dfxAction, run *dfxRun, q *dashQuery) []string {
	cols, err := q.columns(0)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", a.name, err)}
	}
	var p dashProblems
	for _, c := range cols {
		if slices.Contains(dfxProtected, c) {
			p.addf("%s outputs %s, which no table of the package may hold: status_message sits behind a policy tag under enable_column_level_security and a table that Dataform creates has none, and the raw bags are left to the views", a.name, c)
		}
	}
	for _, st := range slices.Concat(run.pre, run.post) {
		if dfxMentions(st.toks, "status_message") {
			p.addf("%s: the %s statement mentions status_message, which the package never reads", a.name, st.kind)
		}
	}
	if toks, err := dashTokenize(run.body); err == nil && dfxMentions(toks, "status_message") {
		p.addf("%s: the query mentions status_message, which the package never reads", a.name)
	}
	return p
}

// dfxCheckKept checks that the partitioning and clustering columns of a table
// are columns that the query outputs.
func dfxCheckKept(a *dfxAction, q *dashQuery) []string {
	cols, err := q.columns(0)
	if err != nil {
		return nil // dfxCheckProtected reports it
	}
	var p dashProblems
	for _, c := range dfxIdentifiers(a.cfg.partitionBy) {
		if !slices.Contains(cols, c) {
			p.addf("%s: partitionBy %q uses %s, which the query does not output", a.name, a.cfg.partitionBy, c)
		}
	}
	for _, c := range a.cfg.clusterBy {
		if !slices.Contains(cols, c) {
			p.addf("%s: clusterBy has %s, which the query does not output", a.name, c)
		}
	}
	if len(a.cfg.clusterBy) > 4 {
		p.addf("%s: clusterBy has %d columns; BigQuery clusters by at most 4", a.name, len(a.cfg.clusterBy))
	}
	return p
}

// dfxCheckAggregate checks one of the three marts that keep the rows of a view.
func dfxCheckAggregate(a *dfxAction, e dfxActionPlan, q *dashQuery) []string {
	if len(q.ctes) != 0 || len(q.branches) != 1 {
		return []string{a.name + ": the query is not a single SELECT"}
	}
	b := q.branches[0]
	cols, probs := dfxPlainItems(a, b)
	p := dashProblems(probs)
	want, err := dashViewColumns(e.view)
	if err != nil {
		return append(p, err.Error())
	}
	if d := dfxDiffColumns(cols, want); d != "" {
		p.addf("%s: the columns differ from those of %s, in its order: %s", a.name, e.view, d)
	}
	if got, wantFrom := dashJoin(b.clauses["FROM"]), "ref."+e.view; got != wantFrom {
		p.addf("%s: FROM is %q, want %q", a.name, got, wantFrom)
	}
	if got := dashJoin(b.clauses["WHERE"]); got != e.prune {
		p.addf("%s: WHERE is %q, want %q, the predicate of the DELETE", a.name, got, e.prune)
	}
	for _, name := range []string{"GROUP BY", "HAVING", "QUALIFY", "ORDER BY", "LIMIT"} {
		if _, ok := b.clauses[name]; ok {
			p.addf("%s: the query has a %s clause; a row of a mart is a row of its view", a.name, name)
		}
	}
	for _, msg := range dashCheckBranch(b, dashSource{cols: dashSet(want, "window_start")}) {
		p.addf("%s: %s", a.name, msg)
	}
	return p
}

// dfxCheckStaging checks the staging table against the spans WITH table of the
// trajectory view, whose canonical rows it holds.
func dfxCheckStaging(a *dfxAction, e dfxActionPlan, q, view *dashQuery, spansCols []string) []string {
	if len(q.ctes) != 0 || len(q.branches) != 1 {
		return []string{a.name + ": the query is not a single SELECT; the time predicate has to sit in the SELECT that holds the ROW_NUMBER window"}
	}
	b := q.branches[0]
	vs := dfxCTE(view, "spans")
	if vs == nil || len(vs.branches) != 1 {
		return []string{dfxTrajView + " has no spans WITH table of one SELECT to stage"}
	}
	vb := vs.branches[0]
	var p dashProblems
	if d := dfxDiffSeq(dfxItemSigs(b), dfxItemSigs(vb)); d != "" {
		p.addf("%s: the select list differs from the spans WITH table of %s, apart from status_message: %s", a.name, dfxTrajView, d)
	}
	if got, want := dashJoin(b.clauses["QUALIFY"]), dashJoin(vb.clauses["QUALIFY"]); got != want {
		p.addf("%s: QUALIFY is %q, want the deduplication of %s: %q", a.name, got, dfxTrajView, want)
	}
	if got := dashJoin(b.clauses["WHERE"]); got != e.prune {
		p.addf("%s: WHERE is %q, want %q", a.name, got, e.prune)
	}
	if got, want := dashJoin(b.clauses["FROM"]), "ref."+dfxSpansView; got != want {
		p.addf("%s: FROM is %q, want %q", a.name, got, want)
	}
	for _, name := range []string{"GROUP BY", "HAVING", "ORDER BY", "LIMIT"} {
		if _, ok := b.clauses[name]; ok {
			p.addf("%s: the query has a %s clause; it stages one canonical row per span", a.name, name)
		}
	}
	for _, msg := range dashCheckBranch(b, dashSource{cols: dashSet(spansCols, "window_start")}) {
		p.addf("%s: %s", a.name, msg)
	}
	return p
}

// dfxCheckTouched checks the temporary table that lists the traces with a span
// in the window or a pending staged span in the 24-hour context window not yet
// present in the trajectory mart.
func dfxCheckTouched(a *dfxAction, q *dashQuery, stagingCols []string) []string {
	if len(q.ctes) != 0 || len(q.branches) != 1 {
		return []string{a.name + ": touched_traces is not a single SELECT"}
	}
	b := q.branches[0]
	var p dashProblems
	if len(b.toks) < 2 || b.toks[1].up != "DISTINCT" {
		p.addf("%s: touched_traces is not SELECT DISTINCT, so a trace is listed once per span", a.name)
	}
	if cols, err := b.columns(0); err != nil {
		p.addf("%s: touched_traces: %v", a.name, err)
	} else if d := dfxDiffColumns(cols, []string{"trace_id", "service_name"}); d != "" {
		p.addf("%s: touched_traces outputs %v, want trace_id and service_name: %s", a.name, cols, d)
	}
	if got, want := dashJoin(b.clauses["FROM"]), "ref."+dfxStaging+" AS S"; got != want {
		p.addf("%s: touched_traces reads %q, want %q", a.name, got, want)
	}
	if got := dfxJoinWithout(b.clauses["WHERE"], "s"); got != dfxTouchedWhere {
		p.addf("%s: the WHERE of touched_traces is %q, want %q", a.name, got, dfxTouchedWhere)
	}
	tables := map[string][]string{"ref." + dfxStaging: stagingCols, "SELF": stagingCols}
	for _, msg := range dfxResolveBranch(b, tables, []string{"window_start", "context_start"}) {
		p.addf("%s: touched_traces: %s", a.name, msg)
	}
	return p
}

// dfxCheckSpans checks the spans WITH table of the trajectory mart against the
// one of the view: the staged columns, without status_message, of the spans
// that belong to a trace touched by the run.
func dfxCheckSpans(a *dfxAction, got, want *dashQuery) []string {
	if len(got.branches) != 1 || len(want.branches) != 1 {
		return []string{a.name + ": the spans WITH table is not a single SELECT"}
	}
	gb, wb := got.branches[0], want.branches[0]
	var p dashProblems
	var gotNames, wantNames []string
	for _, it := range gb.items {
		gotNames = append(gotNames, it.name)
		if !strings.EqualFold(dfxJoinWithout(it.toks, "s"), it.name) {
			p.addf("%s: the spans item %q is not the staged column %s", a.name, dfxJoinWithout(it.toks, "s"), it.name)
		}
	}
	for _, it := range wb.items {
		if it.name != "status_message" {
			wantNames = append(wantNames, it.name)
		}
	}
	if d := dfxDiffColumns(gotNames, wantNames); d != "" {
		p.addf("%s: the spans WITH table has to hold the columns of the spans WITH table of %s without status_message, but it: %s", a.name, dfxTrajView, d)
	}
	if g, w := dfxJoinWithout(gb.clauses["QUALIFY"], "s"), dashJoin(wb.clauses["QUALIFY"]); g != w {
		p.addf("%s: the QUALIFY of the spans WITH table is %q, want the view's %q", a.name, g, w)
	}
	if g, w := dashJoin(gb.clauses["FROM"]), "ref."+dfxStaging+" AS S"; g != w {
		p.addf("%s: the spans WITH table reads %q, want %q", a.name, g, w)
	}
	if g := dfxJoinWithout(gb.clauses["WHERE"], "s"); g != dfxContextWhere {
		p.addf("%s: the WHERE of the spans WITH table is %q, want %q, which the DELETE shares", a.name, g, dfxContextWhere)
	}
	for _, name := range []string{"GROUP BY", "HAVING", "ORDER BY", "LIMIT"} {
		if _, ok := gb.clauses[name]; ok {
			p.addf("%s: the spans WITH table has a %s clause", a.name, name)
		}
	}
	return p
}

// dfxCompareTrajectory compares the recursive query of the trajectory mart
// with the query of the view: the same WITH tables, and apart from the spans
// WITH table the same text, without the status_message projections.
func dfxCompareTrajectory(a *dfxAction, rows, view *dashQuery) []string {
	var p dashProblems
	want := dfxCTENames(view)
	if got := dfxCTENames(rows); !slices.Equal(got, want) {
		p.addf("%s: the WITH tables of trajectory_rows are %v, want those of %s: %v", a.name, got, dfxTrajView, want)
		return p
	}
	for _, name := range want {
		got, w := dfxCTE(rows, name), dfxCTE(view, name)
		if name == "spans" {
			p = append(p, dfxCheckSpans(a, got, w)...)
			continue
		}
		if d := dfxDiffSeq(dfxQuerySig(got), dfxQuerySig(w)); d != "" {
			p.addf("%s: the WITH table %s differs from the one of %s, apart from status_message: %s", a.name, name, dfxTrajView, d)
		}
	}
	if d := dfxDiffSeq(dfxQuerySig(rows), dfxQuerySig(view)); d != "" {
		p.addf("%s: the final SELECT of trajectory_rows differs from the one of %s, apart from status_message: %s", a.name, dfxTrajView, d)
	}
	return p
}

// dfxCheckTrajectoryBody checks the query of the trajectory mart, which only
// reads the temporary table that the pre_operations fill.
func dfxCheckTrajectoryBody(a *dfxAction, body *dashQuery, rowCols []string) []string {
	if len(body.ctes) != 0 || len(body.branches) != 1 {
		return []string{a.name + ": the query is not a single SELECT"}
	}
	b := body.branches[0]
	cols, probs := dfxPlainItems(a, b)
	p := dashProblems(probs)
	viewCols, err := dashViewColumns(dfxTrajView)
	if err != nil {
		return append(p, err.Error())
	}
	want := slices.DeleteFunc(slices.Clone(viewCols), func(c string) bool { return c == "status_message" })
	if d := dfxDiffColumns(cols, want); d != "" {
		p.addf("%s: the columns differ from those of %s without status_message, in its order: %s", a.name, dfxTrajView, d)
	}
	if got := dashJoin(b.clauses["FROM"]); got != "TRAJECTORY_ROWS" {
		p.addf("%s: FROM is %q, want TRAJECTORY_ROWS, the temporary table that pre_operations fill", a.name, got)
	}
	for _, name := range []string{"WHERE", "GROUP BY", "HAVING", "QUALIFY", "ORDER BY", "LIMIT"} {
		if _, ok := b.clauses[name]; ok {
			p.addf("%s: the query has a %s clause; it has to insert the rows of trajectory_rows as they are", a.name, name)
		}
	}
	if rowCols != nil {
		for _, msg := range dashCheckBranch(b, dashSource{cols: dashSet(rowCols)}) {
			p.addf("%s: %s", a.name, msg)
		}
	}
	return p
}

var (
	dfxSelfWordRE         = regexp.MustCompile(`\bSELF\b`)
	dfxContextStartWordRE = regexp.MustCompile(`\bcontext_start\b`)
)

// dfxAssertNotExistsSQL extracts and normalizes the inner query of an
// ASSERT NOT EXISTS ( <query> ) statement from m_gen_ai_agent_trajectory's
// post_operations so it can be compared or evaluated against the standalone
// assertion queries. It rejects WITH RECURSIVE inside ASSERT NOT EXISTS (...).
func dfxAssertNotExistsSQL(st *dfxStmt) (string, error) {
	if st == nil {
		return "", errors.New("missing ASSERT NOT EXISTS statement")
	}
	if probs := dfxCheckRecursivePlacement("ASSERT NOT EXISTS", st.toks); len(probs) > 0 {
		return "", errors.New(probs[0])
	}
	if len(st.toks) < 5 || st.toks[0].up != "ASSERT" || st.toks[1].up != "NOT" || st.toks[2].up != "EXISTS" ||
		!st.toks[3].is("(") || !st.toks[len(st.toks)-1].is(")") {
		return "", errors.New("expected ASSERT NOT EXISTS ( <query> )")
	}
	inner := st.text[st.toks[3].pos+1 : st.toks[len(st.toks)-1].pos]
	normalized := dfxSelfWordRE.ReplaceAllString(inner, "`ref."+dfxTraj+"`")
	normalized = dfxContextStartWordRE.ReplaceAllString(normalized,
		"TIMESTAMP_SUB((SELECT rw.window_start FROM `ref."+dfxRunWindow+"` AS rw), INTERVAL 24 HOUR)")
	return normalized, nil
}

// dfxOrphanTokenTempTableSQL validates the outer wrapper of the
// CREATE OR REPLACE TEMP TABLE _bq_otel_orphan_token_violations AS WITH RECURSIVE ...
// statement in m_gen_ai_agent_trajectory's post_operations and returns its
// normalized query text for parity and semantic evaluation.
func dfxOrphanTokenTempTableSQL(st *dfxStmt) (string, error) {
	if st == nil {
		return "", errors.New("missing temp table orphan_token_violations statement")
	}
	if probs := dfxCheckRecursivePlacement("temp table orphan_token_violations", st.toks); len(probs) > 0 {
		return "", errors.New(probs[0])
	}
	const wantPrefix = "CREATE OR REPLACE TEMP TABLE _BQ_OTEL_ORPHAN_TOKEN_VIOLATIONS AS"
	if len(st.toks) <= 8 || dashJoin(st.toks[:7]) != wantPrefix || st.toks[7].up != "WITH" || st.toks[8].up != "RECURSIVE" {
		return "", fmt.Errorf("expected %s WITH RECURSIVE ..., got %q", wantPrefix, dashJoin(st.toks))
	}
	inner := st.text[st.toks[7].pos:]
	normalized := dfxSelfWordRE.ReplaceAllString(inner, "`ref."+dfxTraj+"`")
	normalized = dfxContextStartWordRE.ReplaceAllString(normalized,
		"TIMESTAMP_SUB((SELECT rw.window_start FROM `ref."+dfxRunWindow+"` AS rw), INTERVAL 24 HOUR)")
	return normalized, nil
}

// dfxCheckOrphanTokenAssertStmt validates the complete ASSERT NOT EXISTS
// wrapper that checks _bq_otel_orphan_token_violations in post_operations.
func dfxCheckOrphanTokenAssertStmt(st *dfxStmt) error {
	if st == nil {
		return errors.New("missing assert no_orphan_tokens statement")
	}
	if probs := dfxCheckRecursivePlacement("assert no_orphan_tokens", st.toks); len(probs) > 0 {
		return errors.New(probs[0])
	}
	const wantStmt = "ASSERT NOT EXISTS ( SELECT 1 FROM _BQ_OTEL_ORPHAN_TOKEN_VIOLATIONS )"
	if got := dashJoin(st.toks); got != wantStmt {
		return fmt.Errorf("got %q, want %q", got, wantStmt)
	}
	innerSQL, err := dfxAssertNotExistsSQL(st)
	if err != nil {
		return err
	}
	q, err := dashParseSQL(innerSQL)
	if err != nil {
		return err
	}
	if len(q.ctes) != 0 || len(q.branches) != 1 || dashJoin(q.branches[0].clauses["FROM"]) != "_BQ_OTEL_ORPHAN_TOKEN_VIOLATIONS" || len(q.branches[0].clauses["WHERE"]) != 0 {
		return fmt.Errorf("inner query must be unconditional SELECT 1 FROM _bq_otel_orphan_token_violations")
	}
	return nil
}

// dfxCheckTrajectory checks the trajectory mart: the statements that run the
// recursive query, the query that inserts its rows, and the post_operations
// temp-table + ASSERT NOT EXISTS gates that guard updating stg_run_window.is_committed.
func dfxCheckTrajectory(pkg *dfxPackage, a *dfxAction, run *dfxRun, body, view *dashQuery, stagingCols []string) []string {
	var p dashProblems
	for _, st := range slices.Concat(run.pre, run.post) {
		for _, msg := range dfxCheckRecursivePlacement(fmt.Sprintf("%s: %s", a.name, st.kind), st.toks) {
			p.addf("%s", msg)
		}
	}
	if st := run.find("temp table touched_traces"); st != nil {
		q, err := dfxStmtQuery(st, 5)
		if err != nil {
			p.addf("%s: touched_traces: %v", a.name, err)
		} else {
			p = append(p, dfxCheckTouched(a, q, stagingCols)...)
		}
	}
	var rowCols []string
	if st := run.find("temp table trajectory_rows"); st != nil {
		rows, err := dfxStmtQuery(st, 5)
		if err != nil {
			p.addf("%s: trajectory_rows: %v", a.name, err)
		} else {
			p = append(p, dfxCompareTrajectory(a, rows, view)...)
			tables := map[string][]string{"ref." + dfxStaging: stagingCols, "touched_traces": {"trace_id", "service_name"}}
			for _, msg := range dfxResolve(rows, tables, "window_start", "context_start") {
				p.addf("%s: trajectory_rows: %s", a.name, msg)
			}
			if rowCols, err = rows.columns(0); err != nil {
				p.addf("%s: trajectory_rows: %v", a.name, err)
			}
		}
	}
	if st := run.find("assert no_orphan_tokens"); st != nil {
		if err := dfxCheckOrphanTokenAssertStmt(st); err != nil {
			p.addf("%s: assert no_orphan_tokens: %v", a.name, err)
		}
	}
	for _, spec := range []struct {
		stmtKind   string
		reportKind string
		assertName string
		extract    func(*dfxStmt) (string, error)
	}{
		{"assert no_unhandled_cycles", "assert no_unhandled_cycles", "assert_no_unhandled_cycles", dfxAssertNotExistsSQL},
		{"temp table orphan_token_violations", "assert no_orphan_tokens", "assert_no_orphan_tokens", dfxOrphanTokenTempTableSQL},
	} {
		st := run.find(spec.stmtKind)
		if st == nil {
			continue // dfxCheckRun reports missing post_operations statement
		}
		normSQL, err := spec.extract(st)
		if err != nil {
			p.addf("%s: %s: %v", a.name, spec.reportKind, err)
			continue
		}
		gotQ, err := dashParseSQL(normSQL)
		if err != nil {
			p.addf("%s: %s: %v", a.name, spec.reportKind, err)
			continue
		}
		assertAction := pkg.byName[spec.assertName]
		if assertAction == nil {
			continue
		}
		assertRun, err := dfxRender(assertAction, true)
		if err != nil {
			continue
		}
		wantQ, err := dashParseSQL(assertRun.body)
		if err != nil {
			continue
		}
		if !slices.Equal(dfxCTENames(gotQ), dfxCTENames(wantQ)) {
			p.addf("%s: %s WITH tables are %v, want %s's %v", a.name, spec.reportKind, dfxCTENames(gotQ), spec.assertName, dfxCTENames(wantQ))
			continue
		}
		for _, cteName := range dfxCTENames(wantQ) {
			if d := dfxDiffSeq(dfxQuerySig(dfxCTE(gotQ, cteName)), dfxQuerySig(dfxCTE(wantQ, cteName))); d != "" {
				p.addf("%s: %s WITH table %s differs from %s: %s", a.name, spec.reportKind, cteName, spec.assertName, d)
			}
		}
		if d := dfxDiffSeq(dfxQuerySig(gotQ), dfxQuerySig(wantQ)); d != "" {
			p.addf("%s: %s final SELECT differs from %s: %s", a.name, spec.reportKind, spec.assertName, d)
		}
	}
	return append(p, dfxCheckTrajectoryBody(a, body, rowCols)...)
}

// dfxValidateMarts checks the staging table, the three aggregate marts and the
// trajectory mart against the views they stand in for.
func dfxValidateMarts(files dashFileSet) []string {
	pkg := dfxLoad(files)
	view, err := dfxTrajectoryView()
	if err != nil {
		return []string{err.Error()}
	}
	spansCols, err := dashViewColumns(dfxSpansView)
	if err != nil {
		return []string{err.Error()}
	}
	var p dashProblems
	stagingCols, err := dfxMartColumns(files, dfxStaging)
	if err != nil {
		p.addf("%v", err)
	}
	for _, e := range dfxPlan {
		a := pkg.byFile[dfxDefs+e.file]
		if a == nil || e.typ != "incremental" || e.name == dfxRunWindow {
			continue
		}
		run, err := dfxRender(a, true)
		if err != nil {
			p.addf("%s: %v", a.name, err)
			continue
		}
		q, err := dashParseSQL(run.body)
		if err != nil {
			p.addf("%s: the query: %v", a.name, err)
			continue
		}
		p = append(p, dfxCheckProtected(a, run, q)...)
		p = append(p, dfxCheckKept(a, q)...)
		switch e.name {
		case dfxStaging:
			p = append(p, dfxCheckStaging(a, e, q, view, spansCols)...)
		case dfxTraj:
			p = append(p, dfxCheckTrajectory(pkg, a, run, q, view, stagingCols)...)
		default:
			p = append(p, dfxCheckAggregate(a, e, q)...)
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// The assertions.
// ---------------------------------------------------------------------------

// dfxAssertionRule is the shape of an assertion: its WITH tables, the columns
// of every branch of its final SELECT and, per WITH table, what its WHERE
// clause has to contain. %W stands for the window of the run and %R for the
// window moved back by the reach of the trajectory view.
type dfxAssertionRule struct {
	ctes     []string
	cols     []string
	where    map[string]string
	wantRefs int
}

var dfxAssertionRules = map[string]dfxAssertionRule{
	"assert_no_orphan_tokens": {
		ctes: []string{"staged", "trajectory_context", "emitted_ancestors", "token_parent_coverage", "emitted", "row_violations", "double_counted", "missing"},
		cols: []string{"trace_id", "service_name", "span_id", "violation"},
		where: map[string]string{
			"staged":             "START_TIME >= %R",
			"trajectory_context": "M . START_TIME >= %R AND S . START_TIME >= %R",
			"emitted":            "M . START_TIME >= %R",
			"missing":            "M . START_TIME >= %R",
		},
		wantRefs: 4,
	},
	"assert_no_unhandled_cycles": {
		ctes: []string{"staged", "emitted"},
		cols: []string{"trace_id", "service_name", "span_id", "emitted_rows", "violation"},
		where: map[string]string{
			"staged":  "START_TIME >= %R",
			"emitted": "START_TIME >= %R",
		},
		wantRefs: 2,
	},
}

// dfxViolationRE matches the string literal that names a broken rule.
var dfxViolationRE = regexp.MustCompile(`^'([a-z_]+)'$`)

// dfxViolations returns the rule names an assertion query returns in its
// violation column: its lower-case string literals.
func dfxViolations(toks []dashTok) []string {
	seen := map[string]bool{}
	for _, t := range toks {
		if m := dfxViolationRE.FindStringSubmatch(t.text); m != nil && t.kind == dashString {
			seen[m[1]] = true
		}
	}
	return dashSortedKeys(seen)
}

// dfxCountRefs counts the reads of the staging table and trajectory mart in a query.
func dfxCountRefs(toks []dashTok) int {
	n := 0
	for _, t := range toks {
		if t.kind == dashIdent && strings.HasPrefix(t.text, "ref.") && t.text != "ref."+dfxRunWindow {
			n++
		}
	}
	return n
}

// dfxCheckAssertion checks one assertion query against its rule.
func dfxCheckAssertion(a *dfxAction, rule dfxAssertionRule, run *dfxRun, tables map[string][]string, lookback, reach int) []string {
	q, err := dashParseSQL(run.body)
	if err != nil {
		return []string{fmt.Sprintf("%s: the query: %v", a.name, err)}
	}
	toks, err := dashTokenize(run.body)
	if err != nil {
		return []string{fmt.Sprintf("%s: the query: %v", a.name, err)}
	}
	var p dashProblems
	for _, msg := range dfxCheckRecursivePlacement(a.name, toks) {
		p.addf("%s", msg)
	}
	if got := dfxCTENames(q); !slices.Equal(got, rule.ctes) {
		p.addf("%s: the WITH tables are %v, want %v", a.name, got, rule.ctes)
	}
	for _, msg := range dfxResolve(q, tables) {
		p.addf("%s: %s", a.name, msg)
	}
	for i, b := range q.branches {
		cols, err := b.columns(0)
		switch {
		case err != nil:
			p.addf("%s: branch %d of the final SELECT: %v", a.name, i+1, err)
		case !slices.Equal(cols, rule.cols):
			p.addf("%s: branch %d of the final SELECT outputs %v, want %v", a.name, i+1, cols, rule.cols)
		}
	}
	if lookback <= 0 || reach <= 0 {
		return p
	}
	window := dfxSharedWindowExpr
	repl := strings.NewReplacer("%W", window, "%R", fmt.Sprintf("TIMESTAMP_SUB ( %s , INTERVAL %d HOUR )", window, reach))
	for _, name := range dashSortedKeys(rule.where) {
		cte := dfxCTE(q, name)
		if cte == nil || len(cte.branches) != 1 {
			continue // the check of the WITH tables reports it
		}
		got, want := dashJoin(cte.branches[0].clauses["WHERE"]), repl.Replace(rule.where[name])
		if !strings.Contains(got, want) {
			p.addf("%s: the WHERE of the WITH table %s is %q, want it to contain %q", a.name, name, got, want)
		}
	}
	if n := dfxCountRefs(toks); n != rule.wantRefs {
		p.addf("%s: reads the staging table and the trajectory mart %d times, want %d, each read with its own window predicate", a.name, n, rule.wantRefs)
	}
	if a.name == "assert_no_orphan_tokens" {
		if tc := dfxCTE(q, "trajectory_context"); tc != nil && len(tc.branches) == 1 {
			wantTCFrom := "ref.m_gen_ai_agent_trajectory AS M JOIN ref.stg_spans_dedup AS S ON M . TRACE_ID = S . TRACE_ID AND M . SERVICE_NAME IS NOT DISTINCT FROM S . SERVICE_NAME AND M . SPAN_ID = S . SPAN_ID"
			if gotTCFrom := dashJoin(tc.branches[0].clauses["FROM"]); gotTCFrom != wantTCFrom {
				p.addf("%s: trajectory_context FROM is %q, want %q", a.name, gotTCFrom, wantTCFrom)
			}
		}
		if ea := dfxCTE(q, "emitted_ancestors"); ea != nil && len(ea.branches) == 2 {
			wantSeed := []string{
				"C . TRACE_ID",
				"C . SERVICE_NAME",
				"C . SPAN_ID AS DESCENDANT_SPAN_ID",
				"C . START_TIME AS DESCENDANT_START_TIME",
				"C . RAW_INPUT_TOKENS IS NOT NULL AS HAS_INPUT",
				"C . RAW_OUTPUT_TOKENS IS NOT NULL AS HAS_OUTPUT",
				"C . RAW_REASONING_TOKENS IS NOT NULL AS HAS_REASONING",
				"P . SPAN_ID AS ANCESTOR_SPAN_ID",
				"P . PARENT_SPAN_ID AS ANCESTOR_PARENT_SPAN_ID",
				"P . START_TIME AS ANCESTOR_START_TIME",
				"1 AS HOP",
				"[ C . SPAN_ID , P . SPAN_ID ] AS PATH_SPAN_IDS",
			}
			if d := dfxDiffSeq(dfxItemSigs(ea.branches[0]), wantSeed); d != "" {
				p.addf("%s: emitted_ancestors seed select items: %s", a.name, d)
			}
			wantSeedFrom := "TRAJECTORY_CONTEXT AS C JOIN TRAJECTORY_CONTEXT AS P ON P . TRACE_ID = C . TRACE_ID AND P . SPAN_ID = C . PARENT_SPAN_ID AND P . SERVICE_NAME IS NOT DISTINCT FROM C . SERVICE_NAME"
			if gotSeedFrom := dashJoin(ea.branches[0].clauses["FROM"]); gotSeedFrom != wantSeedFrom {
				p.addf("%s: emitted_ancestors seed FROM is %q, want %q", a.name, gotSeedFrom, wantSeedFrom)
			}
			wantSeedWhere := "P . SPAN_ID != C . SPAN_ID"
			if gotSeedWhere := dashJoin(ea.branches[0].clauses["WHERE"]); gotSeedWhere != wantSeedWhere {
				p.addf("%s: emitted_ancestors seed WHERE is %q, want %q", a.name, gotSeedWhere, wantSeedWhere)
			}
			wantRec := []string{
				"EA . TRACE_ID",
				"EA . SERVICE_NAME",
				"EA . DESCENDANT_SPAN_ID",
				"EA . DESCENDANT_START_TIME",
				"EA . HAS_INPUT",
				"EA . HAS_OUTPUT",
				"EA . HAS_REASONING",
				"P . SPAN_ID AS ANCESTOR_SPAN_ID",
				"P . PARENT_SPAN_ID AS ANCESTOR_PARENT_SPAN_ID",
				"P . START_TIME AS ANCESTOR_START_TIME",
				"EA . HOP + 1 AS HOP",
				"ARRAY_CONCAT ( EA . PATH_SPAN_IDS , [ P . SPAN_ID ] ) AS PATH_SPAN_IDS",
			}
			if d := dfxDiffSeq(dfxItemSigs(ea.branches[1]), wantRec); d != "" {
				p.addf("%s: emitted_ancestors recursive select items: %s", a.name, d)
			}
			wantRecFrom := "EMITTED_ANCESTORS AS EA JOIN TRAJECTORY_CONTEXT AS P ON P . TRACE_ID = EA . TRACE_ID AND P . SPAN_ID = EA . ANCESTOR_PARENT_SPAN_ID AND P . SERVICE_NAME IS NOT DISTINCT FROM EA . SERVICE_NAME"
			if gotRecFrom := dashJoin(ea.branches[1].clauses["FROM"]); gotRecFrom != wantRecFrom {
				p.addf("%s: emitted_ancestors recursive FROM is %q, want %q", a.name, gotRecFrom, wantRecFrom)
			}
			wantRecWhere := "EA . HOP < 32 AND P . SPAN_ID NOT IN UNNEST ( EA . PATH_SPAN_IDS )"
			if gotRecWhere := dashJoin(ea.branches[1].clauses["WHERE"]); gotRecWhere != wantRecWhere {
				p.addf("%s: emitted_ancestors recursive WHERE is %q, want %q", a.name, gotRecWhere, wantRecWhere)
			}
		}
		if cov := dfxCTE(q, "token_parent_coverage"); cov != nil && len(cov.branches) == 1 {
			wantItems := []string{
				"EA . TRACE_ID",
				"EA . SERVICE_NAME",
				"EA . ANCESTOR_SPAN_ID AS SPAN_ID",
				"LOGICAL_OR ( EA . HAS_INPUT ) AS HAS_INPUT",
				"LOGICAL_OR ( EA . HAS_OUTPUT ) AS HAS_OUTPUT",
				"LOGICAL_OR ( EA . HAS_REASONING ) AS HAS_REASONING",
			}
			if d := dfxDiffSeq(dfxItemSigs(cov.branches[0]), wantItems); d != "" {
				p.addf("%s: token_parent_coverage select items: %s", a.name, d)
			}
		}
		if rv := dfxCTE(q, "row_violations"); rv != nil && len(rv.branches) == 1 {
			joined := dashJoin(rv.branches[0].toks)
			if !strings.Contains(joined, "WHEN NOT E . IS_TOKEN_LEAF AND ( E . EXPECTED_IS_TOKEN_LEAF OR") {
				p.addf("%s: row_violations must check E . EXPECTED_IS_TOKEN_LEAF when NOT E . IS_TOKEN_LEAF", a.name)
			}
		}
		if dc := dfxCTE(q, "double_counted"); dc != nil && len(dc.branches) == 1 {
			joined := dashJoin(dc.branches[0].clauses["WHERE"])
			if !strings.Contains(joined, "( E . IS_TOKEN_LEAF AND NOT E . EXPECTED_IS_TOKEN_LEAF )") {
				p.addf("%s: double_counted WHERE must check ( E . IS_TOKEN_LEAF AND NOT E . EXPECTED_IS_TOKEN_LEAF )", a.name)
			}
		}
		if missing := dfxCTE(q, "missing"); missing != nil && len(missing.branches) == 1 {
			wantMissingWhere := repl.Replace("( S . RAW_INPUT_TOKENS IS NOT NULL OR S . RAW_OUTPUT_TOKENS IS NOT NULL OR S . RAW_REASONING_TOKENS IS NOT NULL ) AND NOT EXISTS ( SELECT 1 FROM ref.m_gen_ai_agent_trajectory AS M WHERE M . START_TIME >= %R AND M . TRACE_ID = S . TRACE_ID AND M . SERVICE_NAME IS NOT DISTINCT FROM S . SERVICE_NAME AND M . SPAN_ID = S . SPAN_ID )")
			if gotMissingWhere := dashJoin(missing.branches[0].clauses["WHERE"]); gotMissingWhere != wantMissingWhere {
				p.addf("%s: the WHERE of the WITH table missing is %q, want %q", a.name, gotMissingWhere, wantMissingWhere)
			}
		}
		wantFinalSig := []string{
			"branch 1",
			"item TRACE_ID", "item SERVICE_NAME", "item SPAN_ID", "item VIOLATION",
			"FROM ROW_VIOLATIONS",
			"WHERE VIOLATION IS NOT NULL",
			"branch 2",
			"item TRACE_ID", "item SERVICE_NAME", "item SPAN_ID", "item VIOLATION",
			"FROM DOUBLE_COUNTED",
			"branch 3",
			"item TRACE_ID", "item SERVICE_NAME", "item SPAN_ID", "item VIOLATION",
			"FROM MISSING",
		}
		if d := dfxDiffSeq(dfxQuerySig(q), wantFinalSig); d != "" {
			p.addf("%s: final UNION ALL branches: %s", a.name, d)
		}
	}
	wantWindowDoc := fmt.Sprintf("%d-hour context window anchored at %s.window_start", reach, dfxRunWindow)
	if !strings.Contains(a.cfg.description, wantWindowDoc) {
		p.addf("%s: description %q must document the %s", a.name, a.cfg.description, wantWindowDoc)
	}
	return p
}

// dfxValidateAssertions checks that the assertions read the window the
// incremental run refreshes and only columns that the tables they read hold.
func dfxValidateAssertions(files dashFileSet) []string {
	pkg := dfxLoad(files)
	var p dashProblems
	lookback, err := dfxLookback(pkg)
	if err != nil {
		p.addf("%v", err)
	}
	reach, err := dfxReachHours()
	if err != nil {
		p.addf("%v", err)
	}
	tables := map[string][]string{
		"ref." + dfxRunWindow: {"run_ts", "window_start", "is_committed"},
	}
	for _, name := range []string{dfxStaging, dfxTraj} {
		cols, err := dfxMartColumns(files, name)
		if err != nil {
			p.addf("%v", err)
		}
		tables["ref."+name] = cols
	}
	for _, e := range dfxPlan {
		a := pkg.byFile[dfxDefs+e.file]
		if a == nil || e.typ != "assertion" {
			continue
		}
		if raw := files[dfxDefs+e.file]; strings.Contains(raw, fmt.Sprintf("last %d hours", lookback)) {
			p.addf("%s: file text still mentions \"last %d hours\" instead of the %d-hour context window anchored at %s.window_start", a.name, lookback, reach, dfxRunWindow)
		}
		run, err := dfxRender(a, true)
		if err != nil {
			p.addf("%s: %v", a.name, err)
			continue
		}
		p = append(p, dfxCheckAssertion(a, dfxAssertionRules[e.name], run, tables, lookback, reach)...)
	}
	return p
}

// ---------------------------------------------------------------------------
// The README.
// ---------------------------------------------------------------------------

const (
	dfxDoc       = dfxDir + "README.md"
	dfxVariables = "deploy/terraform/variables.tf"
)

var (
	dfxBacktick = regexp.MustCompile("`([^`]+)`")
	dfxDepthRE  = regexp.MustCompile(`\bdepth\s*<\s*(\d+)`)
)

// dfxNumberWords are the counts the README spells out as words.
var dfxNumberWords = [...]string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}

// dfxWord spells a small count the way the README does.
func dfxWord(n int) string {
	if n >= 0 && n < len(dfxNumberWords) {
		return dfxNumberWords[n]
	}
	return strconv.Itoa(n)
}

// dfxCode returns the backticked words of a table cell.
func dfxCode(cell string) []string {
	var words []string
	for _, m := range dfxBacktick.FindAllStringSubmatch(cell, -1) {
		words = append(words, m[1])
	}
	return words
}

// dfxTable returns the rows of the Markdown table that follows the header
// line, without the header and its separator, as trimmed cells. It returns nil
// when the document has no such table.
func dfxTable(text, header string) [][]string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if line != header {
			continue
		}
		var rows [][]string
		for _, row := range lines[min(i+2, len(lines)):] {
			if !strings.HasPrefix(row, "|") {
				break
			}
			cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(row, "|"), "|"), "|")
			for k := range cells {
				cells[k] = strings.TrimSpace(cells[k])
			}
			rows = append(rows, cells)
		}
		return rows
	}
	return nil
}

// dfxDepthLimit returns how many levels the recursion of the trajectory view
// descends: a span deeper than that is not emitted.
func dfxDepthLimit() (int, error) {
	sql, ok := dashViewSQLs()[dfxTrajView]
	if !ok {
		return 0, fmt.Errorf("no view query is known for %s", dfxTrajView)
	}
	m := dfxDepthRE.FindStringSubmatch(sql)
	if m == nil {
		return 0, fmt.Errorf("%s has no depth < n bound", dfxTrajView)
	}
	return strconv.Atoi(m[1])
}

// dfxCheckActionTable compares the table of the actions with their config
// blocks: the type, the actions read, the partitioning and the clustering.
func dfxCheckActionTable(p *dashProblems, pkg *dfxPackage, text string) {
	const header = "| Action | Type | Reads | Partition by | Cluster by |"
	rows := dfxTable(text, header)
	if rows == nil {
		p.addf("%s has no table with the header %q", dfxDoc, header)
		return
	}
	var declared []string
	for _, name := range dashSortedKeys(pkg.byName) {
		if pkg.byName[name].cfg.typ == "declaration" {
			declared = append(declared, name)
		}
	}
	var (
		seen      = map[string]bool{}
		sawSource bool
	)
	for _, cells := range rows {
		if len(cells) != 5 {
			p.addf("%s: a row of the action table has %d cells, want 5: %s", dfxDoc, len(cells), strings.Join(cells, " | "))
			continue
		}
		names := dfxCode(cells[0])
		if cells[1] == "declaration" {
			sawSource = true
			if !dfxSameSet(names, declared) {
				p.addf("%s: the action table lists the declarations %v, want %v", dfxDoc, names, declared)
			}
			continue
		}
		if len(names) != 1 {
			p.addf("%s: the action table has a row for %q, want the name of one action", dfxDoc, cells[0])
			continue
		}
		a := pkg.byName[names[0]]
		if a == nil || a.cfg.typ == "declaration" {
			p.addf("%s: the action table lists %s, which is not an incremental table or an assertion of the package", dfxDoc, names[0])
			continue
		}
		seen[a.name] = true
		if cells[1] != a.cfg.typ {
			p.addf("%s: the action table gives %s the type %q, want %q", dfxDoc, a.name, cells[1], a.cfg.typ)
		}
		if got, want := dfxCode(cells[2]), dfxRefs(a); !dfxSameSet(got, want) {
			p.addf("%s: the action table says %s reads %v, want %v", dfxDoc, a.name, got, want)
		}
		wantPartition := ""
		if a.cfg.partitionBy != "" {
			wantPartition = "`" + a.cfg.partitionBy + "`"
		}
		if cells[3] != wantPartition {
			p.addf("%s: the action table partitions %s by %q, want %q", dfxDoc, a.name, cells[3], wantPartition)
		}
		if got := dfxCode(cells[4]); !slices.Equal(got, a.cfg.clusterBy) {
			p.addf("%s: the action table clusters %s by %v, want %v", dfxDoc, a.name, got, a.cfg.clusterBy)
		}
	}
	if !sawSource {
		p.addf("%s: the action table has no row for the declarations", dfxDoc)
	}
	for _, name := range dashSortedKeys(pkg.byName) {
		if pkg.byName[name].cfg.typ != "declaration" && !seen[name] {
			p.addf("%s: the action table has no row for %s", dfxDoc, name)
		}
	}
}

// dfxCheckViewTable compares the table that maps a view to its mart with the
// plan.
func dfxCheckViewTable(p *dashProblems, text string) {
	const header = "| View | Mart |"
	rows := dfxTable(text, header)
	if rows == nil {
		p.addf("%s has no table with the header %q", dfxDoc, header)
		return
	}
	got := map[string]string{}
	for _, cells := range rows {
		if len(cells) != 2 || len(dfxCode(cells[0])) != 1 || len(dfxCode(cells[1])) != 1 {
			p.addf("%s: a row of the table of views and marts is not `view` | `mart`: %s", dfxDoc, strings.Join(cells, " | "))
			continue
		}
		got[dfxCode(cells[0])[0]] = dfxCode(cells[1])[0]
	}
	want := map[string]string{}
	for _, e := range dfxPlan {
		if e.view != "" {
			want[e.view] = e.name
		}
	}
	for _, view := range dashSortedKeys(want) {
		if got[view] != want[view] {
			p.addf("%s: the table of views and marts maps %s to %q, want %s", dfxDoc, view, got[view], want[view])
		}
	}
	for _, view := range dashSortedKeys(got) {
		if _, ok := want[view]; !ok {
			p.addf("%s: the table of views and marts lists %s, which no mart of the package replaces", dfxDoc, view)
		}
	}
}

// dfxCheckAssertionTable compares the rules the README says each assertion
// returns with the rule names the assertion query returns.
func dfxCheckAssertionTable(p *dashProblems, pkg *dfxPackage, text string) {
	const header = "| Assertion | A row is returned when |"
	rows := dfxTable(text, header)
	if rows == nil {
		p.addf("%s has no table with the header %q", dfxDoc, header)
		return
	}
	documented := map[string][]string{}
	for _, cells := range rows {
		names := dfxCode(cells[0])
		if len(cells) != 2 || len(names) != 1 {
			p.addf("%s: a row of the table of assertions is not `assertion` | description: %s", dfxDoc, strings.Join(cells, " | "))
			continue
		}
		var rules []string
		for _, m := range dashBacktickName.FindAllStringSubmatch(cells[1], -1) {
			if m[1] != "violation" {
				rules = append(rules, m[1])
			}
		}
		documented[names[0]] = rules
	}
	for _, e := range dfxPlan {
		if e.typ != "assertion" {
			continue
		}
		rules, ok := documented[e.name]
		a := pkg.byName[e.name]
		if !ok {
			p.addf("%s: the table of assertions has no row for %s", dfxDoc, e.name)
			continue
		}
		if a == nil {
			continue // the rules of the actions report it
		}
		run, err := dfxRender(a, true)
		if err != nil {
			continue
		}
		toks, err := dashTokenize(run.body)
		if err != nil {
			continue
		}
		if want := dfxViolations(toks); !dfxSameSet(rules, want) {
			p.addf("%s: the table of assertions says %s returns the rules %v, want %v, the rules its query returns", dfxDoc, e.name, rules, want)
		}
		delete(documented, e.name)
	}
	for _, name := range dashSortedKeys(documented) {
		p.addf("%s: the table of assertions has a row for %s, which is not an assertion of the package", dfxDoc, name)
	}
}

// dfxCheckWindowSnippet compares the window_start of the README with the one
// the actions declare.
func dfxCheckWindowSnippet(p *dashProblems, text string, lookback int) {
	var snippet string
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, "window_start = "); ok {
			snippet = rest
			break
		}
	}
	if snippet == "" {
		p.addf("%s has no line `window_start = ...` that shows the window of a run", dfxDoc)
		return
	}
	toks, err := dashTokenize(snippet)
	if err != nil {
		p.addf("%s: the window_start snippet: %v", dfxDoc, err)
		return
	}
	if got, want := dashJoin(toks), dfxWindowText(lookback); got != want {
		p.addf("%s: the window_start snippet is %q, want %q, the window of the actions", dfxDoc, got, want)
	}
}

// dfxLookbackNeedles are the phrases of the README that spell out the lookback
// of the actions and the reach of the trajectory view.
func dfxLookbackNeedles(pkg *dfxPackage, lookback, reach int) []string {
	var marts, aggregates, assertions int
	for _, a := range pkg.byName {
		switch {
		case a.cfg.typ == "assertion":
			assertions++
		case a.cfg.typ == "incremental" && a.name != dfxStaging && a.name != dfxRunWindow:
			marts++
			if a.name != dfxTraj {
				aggregates++
			}
		}
	}
	return []string{
		fmt.Sprintf("`INTERVAL %d HOUR` in `%s` (which `%s`, the %s marts and the %s assertions read)", lookback, dfxRunWindow, dfxStaging, dfxWord(marts), dfxWord(assertions)),
		fmt.Sprintf("so `T - %dh < window_start <= T - %dh`: the lookback `L` is %d to %d hours", lookback+1, lookback, lookback, lookback+1),
		fmt.Sprintf("Do not read the latest %d to %d hours as final", lookback, lookback+1),
		fmt.Sprintf("with `T < H + %dh`, that is, until %d hours after the hour ends", lookback+1, lookback),
		fmt.Sprintf("less than `%dh - g` after the end of its hour: %d hours for hourly runs", lookback, lookback-1),
		fmt.Sprintf("plus `g` stays below %d hours", lookback),
		fmt.Sprintf("Run at least every %d hours", lookback),
		fmt.Sprintf("`%s` and the %s aggregate marts", dfxStaging, dfxWord(aggregates)),
		fmt.Sprintf("over the spans at or after `window_start - %dh`", reach),
		fmt.Sprintf("a parent up to %d hours before it and to children up to %d hours after it", reach, reach),
		fmt.Sprintf("the first event of the trace is at or after `window_start - %dh`", reach),
	}
}

// dfxValidateDocs checks the README of the package against the package: the
// tables against the config blocks, the numbers of the refresh model against
// the window the actions declare and the settings against
// workflow_settings.yaml.
func dfxValidateDocs(files dashFileSet) []string {
	text, ok := files[dfxDoc]
	if !ok {
		return []string{dfxDoc + " is missing"}
	}
	var p dashProblems
	pkg := dfxLoad(files)
	needles := []string{
		"All actions except the declarations carry the tag `" + dfxTag + "`.",
		"The definitions are in `definitions/`: `sources/` (the declarations), `staging/`, `marts/` and `assertions/`.",
		"npm install -g @dataform/cli", "dataform init-creds bigquery", "`.df-credentials.json`",
		"`.gitignore` in this directory lists it", "dataform compile", "dataform run --tags " + dfxTag, "--full-refresh",
		"## Refresh model and freshness", "### Lookback completeness bound", "### What the refresh does not promise",
		"## Pointing the dashboards at the marts", "## Assertions", "## Datasets that require a partition filter",
		"## Access and column-level security", "## Parity with the views", "## Full refresh and retention",
		"## Dataform core versions", "at least once", "It is not exactly-once processing",
		"never stages or materializes `status_message`", "a conformance test fails when they differ",
		"`v_gen_ai_tool_usage` and `v_gen_ai_metric_exemplars` have no mart.", "`ANY_VALUE`", "`protected: true`",
	}
	if s, err := dfxDecodeSettings(files); err == nil { // the rules of the settings report an error
		needles = append(needles,
			"pins `dataformCoreVersion: "+s.DataformCoreVersion+"`",
			"`defaultProject`", "`defaultLocation`", "`defaultDataset`", "`defaultAssertionDataset`",
			"`my-project."+s.DefaultDataset+".m_gen_ai_model_costs_hourly`",
			"`my-project."+s.DefaultAssertionDataset+".assert_no_orphan_tokens`")
		for _, key := range dashSortedKeys(s.Vars) {
			needles = append(needles, "`vars."+key+"`")
		}
	}
	for _, name := range []string{"project_id", "dataset_id", "require_partition_filter", "allow_unprunable_views", "enable_column_level_security", "spans_partition_expiration_days"} {
		needles = append(needles, "`"+name+"`")
		if !strings.Contains(files[dfxVariables], `variable "`+name+`"`) {
			p.addf("%s names the Terraform variable %s, which %s does not declare", dfxDoc, name, dfxVariables)
		}
	}
	lookback, lerr := dfxLookback(pkg)
	reach, rerr := dfxReachHours()
	depth, derr := dfxDepthLimit()
	if lerr == nil && rerr == nil {
		needles = append(needles, dfxLookbackNeedles(pkg, lookback, reach)...)
	}
	if derr == nil {
		needles = append(needles, fmt.Sprintf("deeper than %d levels", depth))
	}
	dashNeedles(&p, dfxDoc, text, needles...)
	if lerr == nil {
		dfxCheckWindowSnippet(&p, text, lookback)
	}
	dfxCheckActionTable(&p, pkg, text)
	dfxCheckViewTable(&p, text)
	dfxCheckAssertionTable(&p, pkg, text)
	return p
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

// dfxAreas are the groups of rules. Each is a pure function of the files.
var dfxAreas = []struct {
	name string
	run  func(dashFileSet) []string
}{
	{"files", dfxValidateFiles},
	{"settings", dfxValidateSettings},
	{"actions", dfxValidateActions},
	{"refresh", dfxValidateRefresh},
	{"marts", dfxValidateMarts},
	{"assertions", dfxValidateAssertions},
	{"docs", dfxValidateDocs},
}

// TestDataformConformance holds the Dataform package to the views it reads, to
// the refresh model it documents and to its README.
func TestDataformConformance(t *testing.T) {
	files := dashLoadFileSet(t)
	for _, area := range dfxAreas {
		t.Run(area.name, func(t *testing.T) {
			for _, problem := range area.run(files) {
				t.Error(problem)
			}
		})
	}
}

// dfxMutation breaks the package in one place and names the problem that the
// rules of its area have to report for it.
type dfxMutation struct {
	area string
	name string
	file string
	// edit changes the content of the file; a nil edit removes the file. A
	// mutation with create adds a file that the package does not have, and its
	// edit receives the empty string.
	edit   func(content string) (string, error)
	create bool
	want   string // a piece of the problem
}

// TestDataformConformanceDetectsBrokenAssets runs the rules on copies of the
// package that each carry one defect. A rule that cannot fail would let a
// broken mart through, so every defect has to produce its own problem and the
// unchanged files must not produce it.
func TestDataformConformanceDetectsBrokenAssets(t *testing.T) {
	files := dashLoadFileSet(t)
	runners := map[string]func(dashFileSet) []string{}
	baseline := map[string][]string{}
	for _, area := range dfxAreas {
		runners[area.name] = area.run
		baseline[area.name] = area.run(files)
	}
	perArea := map[string]int{}
	seen := map[string]bool{}
	for _, m := range dfxMutations {
		name := m.area + "/" + m.name
		perArea[m.area]++
		if seen[name] {
			t.Fatalf("the defect %q is listed twice", name)
		}
		seen[name] = true
		t.Run(name, func(t *testing.T) {
			run := runners[m.area]
			if run == nil {
				t.Fatalf("no rules are named %q", m.area)
			}
			if containsText(baseline[m.area], m.want) {
				t.Fatalf("the unchanged files already report %q", m.want)
			}
			broken := files.clone()
			content, present := files[m.file]
			switch {
			case m.create && (present || m.edit == nil):
				t.Fatalf("%s: a defect that adds a file needs a file that is not there and an edit", m.file)
			case !m.create && !present:
				t.Fatalf("%s is not one of the files the rules read", m.file)
			case m.edit == nil:
				delete(broken, m.file)
			default:
				edited, err := m.edit(content)
				if err != nil {
					t.Fatalf("editing %s: %v", m.file, err)
				}
				if edited == content {
					t.Fatalf("the edit left %s as it was", m.file)
				}
				broken[m.file] = edited
			}
			if problems := run(broken); !containsText(problems, m.want) {
				t.Errorf("the defect was not reported as %q; the problems are:\n  %s", m.want, strings.Join(problems, "\n  "))
			}
		})
	}
	for _, area := range dfxAreas {
		if perArea[area.name] < 5 {
			t.Errorf("only %d defects are tried on the %s rules", perArea[area.name], area.name)
		}
	}
}

// dfxDropLine removes the first line that contains marker.
func dfxDropLine(marker string) func(string) (string, error) {
	return func(content string) (string, error) {
		lines := strings.Split(content, "\n")
		for i, line := range lines {
			if strings.Contains(line, marker) {
				return strings.Join(slices.Delete(lines, i, i+1), "\n"), nil
			}
		}
		return "", fmt.Errorf("no line contains %q", marker)
	}
}

// dfxFixedWindow is the window_start of an incremental run in the actions.
const dfxFixedWindow = "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), HOUR)"

// The files that the defects change.
const (
	dfxFSettings  = dfxDir + "workflow_settings.yaml"
	dfxFIgnore    = dfxDir + ".gitignore"
	dfxFSources   = dfxDefs + "sources/otel_sources.sqlx"
	dfxFRunWindow = dfxDefs + "staging/stg_run_window.sqlx"
	dfxFStaging   = dfxDefs + "staging/stg_spans_dedup.sqlx"
	dfxFCosts     = dfxDefs + "marts/m_gen_ai_model_costs_hourly.sqlx"
	dfxFTools     = dfxDefs + "marts/m_mcp_tool_usage_hourly.sqlx"
	dfxFSessions  = dfxDefs + "marts/m_gen_ai_sessions_daily.sqlx"
	dfxFTraj      = dfxDefs + "marts/m_gen_ai_agent_trajectory.sqlx"
	dfxFOrphans   = dfxDefs + "assertions/assert_no_orphan_tokens.sqlx"
	dfxFCycles    = dfxDefs + "assertions/assert_no_unhandled_cycles.sqlx"
	dfxFTerra     = dfxVariables
)

// dfxMutations are the defects that the rules have to catch. The text each one
// has to produce is a piece of the problem, so that a defect that is only
// caught by another rule of the area does not pass.
var dfxMutations = []dfxMutation{
	// The files of the package.
	{area: "files", name: "dataform_json_next_to_the_settings", file: dfxDir + "dataform.json", create: true,
		edit: dashAppend("{\n  \"defaultDatabase\": \"YOUR_PROJECT_ID\"\n}\n"),
		want: "dataform.json is not a file of the package: Dataform core 3 rejects"},
	{area: "files", name: "extra_definition", file: dfxDefs + "marts/m_extra.sqlx", create: true,
		edit: dashAppend("SELECT 1\n"), want: "m_extra.sqlx is not a file of the package"},
	{area: "files", name: "mart_missing", file: dfxFTools, want: "m_mcp_tool_usage_hourly.sqlx is missing"},
	{area: "files", name: "credentials_not_ignored", file: dfxFIgnore,
		edit: dashReplaceFirst(".df-credentials.json", "node_modules/"), want: "does not list .df-credentials.json"},
	{area: "files", name: "license_holder_changed", file: dfxFStaging,
		edit: dashReplaceFirst("Copyright 2026 Google LLC", "Copyright 2026 Example Inc"), want: "stg_spans_dedup.sqlx does not open with the license header"},
	{area: "files", name: "settings_license_removed", file: dfxFSettings,
		edit: dashReplaceFirst("# SPDX-License-Identifier: Apache-2.0\n", ""), want: "workflow_settings.yaml does not open with the license header"},
	{area: "files", name: "non_ascii_comment", file: dfxFCosts,
		edit: dashReplaceFirst("Hourly model cost mart", "Hourly model cost mart \u00e9"), want: "m_gen_ai_model_costs_hourly.sqlx: byte"},
	{area: "files", name: "carriage_return", file: dfxFStaging,
		edit: dashReplaceFirst("config {\n", "config {\r\n"), want: "stg_spans_dedup.sqlx has a carriage return"},

	// workflow_settings.yaml.
	{area: "settings", name: "core_version_2x", file: dfxFSettings,
		edit: dashReplaceFirst("dataformCoreVersion: 3.1.2", "dataformCoreVersion: 2.9.0"), want: `dataformCoreVersion is "2.9.0", want a pinned 3.x.y release`},
	{area: "settings", name: "core_version_floating", file: dfxFSettings,
		edit: dashReplaceFirst("dataformCoreVersion: 3.1.2", "dataformCoreVersion: latest"), want: `dataformCoreVersion is "latest"`},
	{area: "settings", name: "key_of_dataform_json", file: dfxFSettings,
		edit: dashAppend("defaultDatabase: YOUR_PROJECT_ID\n"), want: "field defaultDatabase not found"},
	{area: "settings", name: "second_document", file: dfxFSettings,
		edit: dashAppend("---\ndefaultProject: other\n"), want: "holds more than one document"},
	{area: "settings", name: "location_empty", file: dfxFSettings,
		edit: dashReplaceFirst("defaultLocation: US", "defaultLocation: \"\""), want: "defaultLocation is empty"},
	{area: "settings", name: "assertion_dataset_shared", file: dfxFSettings,
		edit: dashReplaceFirst("defaultAssertionDataset: otel_marts_assertions", "defaultAssertionDataset: otel_marts"), want: "defaultDataset and defaultAssertionDataset are both"},
	{area: "settings", name: "dataset_not_an_id", file: dfxFSettings,
		edit: dashReplaceFirst("defaultDataset: otel_marts", "defaultDataset: otel-marts"), want: `defaultDataset is "otel-marts", want a BigQuery dataset ID`},
	{area: "settings", name: "project_filled_in", file: dfxFSettings,
		edit: dashReplaceFirst("defaultProject: YOUR_PROJECT_ID", "defaultProject: my-project"), want: `defaultProject is "my-project", want the placeholder YOUR_PROJECT_ID`},
	{area: "settings", name: "variable_renamed", file: dfxFSettings,
		edit: dashReplaceFirst("source_dataset: YOUR_DATASET_ID", "source_dtaset: YOUR_DATASET_ID"), want: "the definitions read the variable source_dataset, which vars does not set"},
	{area: "settings", name: "variable_unused", file: dfxFSettings,
		edit: dashAppend("  spare: x\n"), want: "vars sets spare, which no definition reads"},

	// The config blocks and the dependency graph.
	{area: "actions", name: "tag_changed", file: dfxFCosts,
		edit: dashReplaceFirst("tags: [\"otel_marts\"]", "tags: [\"other\"]"), want: "m_gen_ai_model_costs_hourly: tags are [other], want [otel_marts]"},
	{area: "actions", name: "partition_column_changed", file: dfxFCosts,
		edit: dashReplaceFirst("partitionBy: \"DATE(usage_hour)\"", "partitionBy: \"usage_hour\""), want: `partitionBy is "usage_hour", want "DATE(usage_hour)"`},
	{area: "actions", name: "cluster_columns_swapped", file: dfxFCosts,
		edit: dashReplaceFirst("clusterBy: [\"service_name\", \"model\"]", "clusterBy: [\"model\", \"service_name\"]"), want: "clusterBy is [model service_name], want [service_name model]"},
	{area: "actions", name: "unknown_config_key", file: dfxFCosts,
		edit: dashReplaceFirst("type: \"incremental\",", "type: \"incremental\",\n  uniqueKey: [\"usage_hour\"],"), want: "config key uniqueKey is not one the package sets on a incremental action"},
	{area: "actions", name: "unknown_config_type", file: dfxFCosts,
		edit: dashReplaceFirst("type: \"incremental\"", "type: \"view\""), want: `the config type "view" is not one the package uses`},
	{area: "actions", name: "description_missing", file: dfxFCosts,
		edit: dfxDropLine("description: \"Hourly GenAI call"), want: "m_gen_ai_model_costs_hourly: the config has no description"},
	{area: "actions", name: "config_block_missing", file: dfxFStaging,
		edit: dashReplaceFirst("\nconfig {", "\nconfg {"), want: "stg_spans_dedup.sqlx: there is no config block"},
	{area: "actions", name: "name_taken_twice", file: dfxFSessions,
		edit: dashReplaceFirst("type: \"incremental\",", "type: \"incremental\",\n  name: \"m_gen_ai_model_costs_hourly\","), want: "both define the action m_gen_ai_model_costs_hourly"},
	{area: "actions", name: "declaration_schema_is_a_literal", file: dfxFSources,
		edit: dashReplaceFirst("schema: dataform.projectConfig.vars.source_dataset,", "schema: \"otel\","), want: "config key schema has to be an expression such as"},
	{area: "actions", name: "ref_misspelled", file: dfxFTraj,
		edit: dashReplaceFirst("FROM ${ref(\"stg_spans_dedup\")} AS s", "FROM ${ref(\"stg_spans_dedupe\")} AS s"), want: `ref("stg_spans_dedupe") names no action of the package`},
	{area: "actions", name: "trajectory_reads_the_view", file: dfxFTraj,
		edit: dashReplaceAll("${ref(\"stg_spans_dedup\")}", "${ref(\"v_gen_ai_spans\")}"), want: "m_gen_ai_agent_trajectory: reads [stg_run_window v_gen_ai_spans] through ref(), want [stg_run_window stg_spans_dedup]"},
	{area: "actions", name: "trajectory_loses_its_dependency", file: dfxFTraj,
		edit: dashReplaceFirst("  dependencies: [\"stg_run_window\", \"stg_spans_dedup\", \"m_gen_ai_model_costs_hourly\", \"m_mcp_tool_usage_hourly\", \"m_gen_ai_sessions_daily\"],\n", ""), want: "m_gen_ai_agent_trajectory: dependencies are [], want [stg_run_window stg_spans_dedup m_gen_ai_model_costs_hourly m_mcp_tool_usage_hourly m_gen_ai_sessions_daily]"},
	{area: "actions", name: "staging_reads_the_trajectory", file: dfxFStaging,
		edit: dashReplaceFirst("${ref(\"v_gen_ai_spans\")}", "${ref(\"m_gen_ai_agent_trajectory\")}"), want: "dependency cycle: m_gen_ai_agent_trajectory -> stg_spans_dedup -> m_gen_ai_agent_trajectory"},
	{area: "actions", name: "assertion_read_by_assertion", file: dfxFCycles,
		edit: dashReplaceFirst("${ref(\"m_gen_ai_agent_trajectory\")}", "${ref(\"assert_no_orphan_tokens\")}"), want: `ref("assert_no_orphan_tokens") names an assertion, which no action may read`},
	{area: "actions", name: "declaration_never_read", file: dfxFTools,
		edit: dashReplaceFirst("${ref(\"v_mcp_tool_usage\")}", "${ref(\"v_gen_ai_sessions\")}"), want: "the declaration v_mcp_tool_usage is never read"},

	// The refresh model: one window, one transaction, an epoch for a full refresh.
	{area: "refresh", name: "window_of_one_mart_shorter", file: dfxFCosts,
		edit: dashReplaceFirst("(SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw)", "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), HOUR)"), want: "m_gen_ai_model_costs_hourly: in an incremental run, window_start is"},
	{area: "refresh", name: "window_of_the_trajectory_longer", file: dfxFTraj,
		edit: dashReplaceFirst("(SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw)", "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR), HOUR)"), want: "m_gen_ai_agent_trajectory: in an incremental run, window_start is"},
	{area: "refresh", name: "window_of_the_staging_table_longer", file: dfxFStaging,
		edit: dashReplaceFirst("(SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw)", "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR), HOUR)"), want: "stg_spans_dedup: takes INTERVAL 4 HOUR from the clock, want INTERVAL 3 HOUR in every action"},
	{area: "refresh", name: "window_in_days", file: dfxFRunWindow,
		edit: dashReplaceAll("INTERVAL 3 HOUR", "INTERVAL 3 DAY"), want: "takes INTERVAL 3 DAY from the clock, want INTERVAL 3 HOUR in every action"},
	{area: "refresh", name: "assertion_window_longer", file: dfxFOrphans,
		edit: dashReplaceFirst("(SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw)", "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR), HOUR)"), want: "assert_no_orphan_tokens: takes INTERVAL 4 HOUR from the clock, want INTERVAL 3 HOUR in every action"},
	{area: "refresh", name: "mart_calls_clock_directly", file: dfxFSessions,
		edit: dashReplaceFirst("(SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw)", "TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), HOUR)"), want: "m_gen_ai_sessions_daily: calls CURRENT_TIMESTAMP() directly instead of reading stg_run_window"},
	{area: "refresh", name: "full_refresh_starts_late", file: dfxFStaging,
		edit: dashReplaceFirst("TIMESTAMP('1970-01-01 00:00:00+00')", "TIMESTAMP('2020-01-01 00:00:00+00')"), want: "in a full refresh, window_start is"},
	{area: "refresh", name: "commit_missing", file: dfxFTools,
		edit: dashReplaceFirst("${when(incremental(), `COMMIT TRANSACTION`)}", ""), want: "m_mcp_tool_usage_hourly: in an incremental run, post_operations run [], want [commit transaction]"},
	{area: "refresh", name: "trajectory_commit_run_window_missing", file: dfxFTraj,
		edit: dashReplaceFirst("  ---\n  UPDATE ${ref(\"stg_run_window\")} SET is_committed = TRUE WHERE TRUE\n", ""), want: "m_gen_ai_agent_trajectory: in an incremental run, post_operations run [commit transaction assert no_unhandled_cycles temp table orphan_token_violations assert no_orphan_tokens], want [commit transaction assert no_unhandled_cycles temp table orphan_token_violations assert no_orphan_tokens commit run_window]"},
	{area: "refresh", name: "run_window_full_refresh_prev_not_epoch", file: dfxFRunWindow,
		edit: dashReplaceFirst("`TIMESTAMP('1970-01-01 00:00:00+00')`", "`TIMESTAMP('9999-12-31 23:59:59+00')`"), want: "stg_run_window: in a full refresh, prev_window_start is TIMESTAMP ( '9999-12-31 23:59:59+00' ), want TIMESTAMP ( '1970-01-01 00:00:00+00' )"},
	{area: "refresh", name: "run_window_prev_cutoff_missing", file: dfxFRunWindow,
		edit: dashReplaceFirst("COALESCE(prev_window_start, TIMESTAMP('9999-12-31 23:59:59+00'))", "TIMESTAMP('9999-12-31 23:59:59+00')"), want: "stg_run_window: window_start item is"},
	{area: "refresh", name: "begin_missing", file: dfxFTools,
		edit: dashReplaceFirst("  ${when(incremental(), `BEGIN TRANSACTION`)}\n  ---\n", ""), want: "pre_operations run [declare window_start delete], want [declare window_start begin transaction delete]"},
	{area: "refresh", name: "delete_missing", file: dfxFTools,
		edit: dashReplaceFirst("  ---\n  ${when(incremental(), `DELETE FROM ${self()} WHERE usage_hour >= window_start`)}\n", ""), want: "pre_operations run [declare window_start begin transaction], want [declare window_start begin transaction delete]"},
	{area: "refresh", name: "delete_predicate_strict", file: dfxFCosts,
		edit: dashReplaceFirst("WHERE usage_hour >= window_start`)}", "WHERE usage_hour > window_start`)}"), want: "the DELETE removes the rows WHERE USAGE_HOUR > WINDOW_START, want WHERE USAGE_HOUR >= WINDOW_START"},
	{area: "refresh", name: "delete_without_where", file: dfxFCosts,
		edit: dashReplaceFirst("DELETE FROM ${self()} WHERE usage_hour >= window_start`)}", "DELETE FROM ${self()}`)}"), want: "the DELETE has no WHERE clause, so it would empty the table"},
	{area: "refresh", name: "delete_runs_in_a_full_refresh", file: dfxFCosts,
		edit: dashReplaceFirst("${when(incremental(), `DELETE FROM ${self()} WHERE usage_hour >= window_start`)}", "DELETE FROM ${self()} WHERE usage_hour >= window_start"), want: "in a full refresh, pre_operations run [declare window_start delete], want [declare window_start]"},
	{area: "refresh", name: "context_shorter_than_the_reach", file: dfxFTraj,
		edit: dashReplaceFirst("TIMESTAMP_SUB(window_start, INTERVAL 24 HOUR)", "TIMESTAMP_SUB(window_start, INTERVAL 12 HOUR)"), want: "context_start is TIMESTAMP_SUB ( WINDOW_START , INTERVAL 12 HOUR )"},
	{area: "refresh", name: "trajectory_delete_ignores_the_context", file: dfxFTraj,
		edit: dashReplaceFirst("WHERE mart.start_time >= context_start", "WHERE mart.start_time >= window_start"), want: "the DELETE removes the rows WHERE START_TIME >= WINDOW_START AND EXISTS"},
	{area: "refresh", name: "trajectory_delete_unaliased", file: dfxFTraj,
		edit: dashReplaceFirst("DELETE FROM ${self()} AS mart", "DELETE FROM ${self()}"), want: `the DELETE names the mart "", want "mart"`},
	{area: "refresh", name: "unsupported_expression", file: dfxFCosts,
		edit: dashReplaceFirst("DELETE FROM ${self()} WHERE", "DELETE FROM ${self() + \"x\"} WHERE"), want: `m_gen_ai_model_costs_hourly: an incremental run: pre_operations: the expression ${self() + "x"} is not one the package uses`},
	{area: "refresh", name: "run_window_uses_safe_cast", file: dfxFRunWindow,
		edit: dashReplaceFirst("        CAST(\n", "        SAFE_CAST(\n"), want: "must use CAST, not SAFE_CAST"},
	{area: "refresh", name: "run_window_unquoted_var", file: dfxFRunWindow,
		edit: dashReplaceFirst("${JSON.stringify(dataform.projectConfig.vars.run_timestamp ?? \"\")}", "'${dataform.projectConfig.vars.run_timestamp}'"), want: "run_timestamp must be escaped with JSON.stringify(dataform.projectConfig.vars.run_timestamp)"},

	// The columns and the queries of the tables.
	{area: "marts", name: "staging_holds_status_message", file: dfxFStaging,
		edit: dashReplaceFirst("  error_type,\n  COALESCE(", "  error_type,\n  status_message,\n  COALESCE("), want: "stg_spans_dedup outputs status_message, which no table of the package may hold"},
	{area: "marts", name: "staging_holds_the_attribute_bag", file: dfxFStaging,
		edit: dashReplaceFirst("  error_type,\n  COALESCE(", "  error_type,\n  attributes,\n  COALESCE("), want: "stg_spans_dedup outputs attributes, which no table of the package may hold"},
	{area: "marts", name: "aggregate_reads_status_message", file: dfxFCosts,
		edit: dashReplaceFirst("WHERE usage_hour >= window_start\n\npost_operations", "WHERE usage_hour >= window_start AND status_message IS NULL\n\npost_operations"), want: "m_gen_ai_model_costs_hourly: the query mentions status_message, which the package never reads"},
	{area: "marts", name: "aggregate_column_dropped", file: dfxFCosts,
		edit: dashReplaceFirst("  model,\n", ""), want: "the columns differ from those of v_gen_ai_model_costs, in its order: lacks model"},
	{area: "marts", name: "aggregate_column_added", file: dfxFCosts,
		edit: dashReplaceFirst("SELECT\n  usage_hour,", "SELECT\n  spare_column,\n  usage_hour,"), want: "has the extra column spare_column"},
	{area: "marts", name: "aggregate_columns_reordered", file: dfxFCosts,
		edit: dashReplaceFirst("  usage_hour,\n  service_name,\n", "  service_name,\n  usage_hour,\n"), want: "column 1 is service_name, want usage_hour"},
	{area: "marts", name: "aggregate_selects_a_star", file: dfxFCosts,
		edit: dashReplaceFirst("SELECT\n  usage_hour,\n  service_name,", "SELECT\n  *,\n  service_name,"), want: "select item 1 is a star; list the columns so that the schema of the table is explicit"},
	{area: "marts", name: "aggregate_groups_rows", file: dfxFCosts,
		edit: dashReplaceFirst("WHERE usage_hour >= window_start\n\npost_operations", "WHERE usage_hour >= window_start\nGROUP BY usage_hour\n\npost_operations"), want: "the query has a GROUP BY clause; a row of a mart is a row of its view"},
	{area: "marts", name: "aggregate_where_differs_from_delete", file: dfxFCosts,
		edit: dashReplaceFirst("WHERE usage_hour >= window_start\n\npost_operations", "WHERE usage_hour > window_start\n\npost_operations"), want: `WHERE is "USAGE_HOUR > WINDOW_START"`},
	{area: "marts", name: "aggregate_reads_another_view", file: dfxFSessions,
		edit: dashReplaceFirst("FROM ${ref(\"v_gen_ai_sessions\")}", "FROM ${ref(\"v_mcp_tool_usage\")}"), want: `FROM is "ref.v_mcp_tool_usage", want "ref.v_gen_ai_sessions"`},
	{area: "marts", name: "partition_column_not_output", file: dfxFSessions,
		edit: dashReplaceFirst("partitionBy: \"session_start_date\"", "partitionBy: \"session_day\""), want: "uses session_day, which the query does not output"},
	{area: "marts", name: "cluster_column_not_output", file: dfxFCosts,
		edit: dashReplaceFirst("clusterBy: [\"service_name\", \"model\"]", "clusterBy: [\"service_name\", \"model_name\"]"), want: "clusterBy has model_name, which the query does not output"},
	{area: "marts", name: "staging_dedup_order_changed", file: dfxFStaging,
		edit: dashReplaceFirst("ORDER BY start_time ASC, end_time DESC", "ORDER BY start_time DESC, end_time DESC"), want: "stg_spans_dedup: QUALIFY is"},
	{area: "marts", name: "staging_filters_on_end_time", file: dfxFStaging,
		edit: dashReplaceFirst("WHERE start_time >= window_start\nQUALIFY", "WHERE end_time >= window_start\nQUALIFY"), want: `stg_spans_dedup: WHERE is "END_TIME >= WINDOW_START"`},
	{area: "marts", name: "staging_reads_another_view", file: dfxFStaging,
		edit: dashReplaceFirst("${ref(\"v_gen_ai_spans\")}", "${ref(\"v_gen_ai_model_costs\")}"), want: `FROM is "ref.v_gen_ai_model_costs", want "ref.v_gen_ai_spans"`},
	{area: "marts", name: "staging_loses_a_column", file: dfxFStaging,
		edit: dashReplaceFirst("  span_kind,\n", ""), want: "the select list differs from the spans WITH table of v_gen_ai_agent_trajectory"},
	{area: "marts", name: "staging_groups_rows", file: dfxFStaging,
		edit: dashReplaceFirst("WHERE start_time >= window_start\nQUALIFY", "WHERE start_time >= window_start\nGROUP BY trace_id\nQUALIFY"), want: "the query has a GROUP BY clause; it stages one canonical row per span"},
	{area: "marts", name: "trajectory_holds_status_message", file: dfxFTraj,
		edit: dashReplaceFirst("  error_type,\n  depth,\n", "  error_type,\n  status_message,\n  depth,\n"), want: "m_gen_ai_agent_trajectory outputs status_message, which no table of the package may hold"},
	{area: "marts", name: "trajectory_reads_status_message", file: dfxFTraj,
		edit: dashReplaceFirst("        s.error_type,\n", "        s.error_type,\n        s.status_message,\n"), want: "the temp table trajectory_rows statement mentions status_message, which the package never reads"},
	{area: "marts", name: "trajectory_loses_its_last_column", file: dfxFTraj,
		edit: dashReplaceFirst("  is_leaf,\n  is_token_leaf\nFROM trajectory_rows", "  is_leaf\nFROM trajectory_rows"), want: "lacks is_token_leaf"},
	{area: "marts", name: "trajectory_depth_limit_changed", file: dfxFTraj,
		edit: dashReplaceFirst("WHERE t.depth < 16", "WHERE t.depth < 15"), want: "the WITH table trajectory differs from the one of v_gen_ai_agent_trajectory, apart from status_message"},
	{area: "marts", name: "trajectory_spans_ignore_the_context", file: dfxFTraj,
		edit: dashReplaceFirst("WHERE s.start_time >= context_start\n        AND EXISTS", "WHERE s.start_time >= window_start\n        AND EXISTS"), want: "the WHERE of the spans WITH table is"},
	{area: "marts", name: "touched_traces_not_distinct", file: dfxFTraj,
		edit: dashReplaceFirst("  SELECT DISTINCT\n    s.trace_id,\n    s.service_name\n", "  SELECT\n    s.trace_id,\n    s.service_name\n"), want: "touched_traces is not SELECT DISTINCT"},
	{area: "marts", name: "touched_traces_window_changed", file: dfxFTraj,
		edit: dashReplaceFirst("  WHERE s.start_time >= window_start\n", "  WHERE s.start_time >= context_start\n"), want: "the WHERE of touched_traces is"},
	{area: "marts", name: "trajectory_query_filters_rows", file: dfxFTraj,
		edit: dashReplaceFirst("FROM trajectory_rows\n", "FROM trajectory_rows\nWHERE depth < 3\n"), want: "the query has a WHERE clause; it has to insert the rows of trajectory_rows as they are"},
	{area: "marts", name: "trajectory_query_reads_another_table", file: dfxFTraj,
		edit: dashReplaceFirst("FROM trajectory_rows\n", "FROM touched_traces\n"), want: `FROM is "TOUCHED_TRACES", want TRAJECTORY_ROWS`},
	{area: "marts", name: "trajectory_reads_a_column_the_staging_table_lacks", file: dfxFTraj,
		edit: dashReplaceFirst("        s.agent_path_name\n", "        s.agent_path_label\n"), want: "agent_path_label"},
	{area: "marts", name: "trajectory_post_assert_cycles_drifted", file: dfxFTraj,
		edit: dashReplaceFirst("WHERE emitted.emitted_rows IS NULL\n      OR emitted.emitted_rows != 1", "WHERE emitted.emitted_rows IS NULL"), want: "m_gen_ai_agent_trajectory: assert no_unhandled_cycles final SELECT differs from assert_no_unhandled_cycles"},
	{area: "marts", name: "trajectory_post_assert_orphans_drifted", file: dfxFTraj,
		edit: dashReplaceFirst("  SELECT trace_id, service_name, span_id, violation\n  FROM missing", "  SELECT trace_id, service_name, span_id, violation\n  FROM missing\n  WHERE FALSE"), want: "m_gen_ai_agent_trajectory: assert no_orphan_tokens final SELECT differs from assert_no_orphan_tokens"},
	{area: "marts", name: "trajectory_post_assert_recursive_in_subquery", file: dfxFTraj,
		edit: dashReplaceFirst("    SELECT 1 FROM _bq_otel_orphan_token_violations", "    WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 2) SELECT 1 FROM r"), want: "WITH RECURSIVE is not allowed inside a parenthesized subquery or ASSERT NOT EXISTS"},

	// The assertions.
	{area: "assertions", name: "orphans_emitted_ancestors_has_input_false", file: dfxFOrphans,
		edit: dashReplaceFirst("c.raw_input_tokens IS NOT NULL AS has_input", "FALSE AS has_input"), want: "assert_no_orphan_tokens: emitted_ancestors seed select items"},
	{area: "assertions", name: "orphans_emitted_ancestors_has_input_swapped", file: dfxFOrphans,
		edit: dashReplaceFirst("c.raw_input_tokens IS NOT NULL AS has_input", "c.raw_output_tokens IS NOT NULL AS has_input"), want: "assert_no_orphan_tokens: emitted_ancestors seed select items"},
	{area: "assertions", name: "orphans_trajectory_context_join_broken", file: dfxFOrphans,
		edit: dashReplaceFirst("ON m.trace_id = s.trace_id\n      AND m.service_name IS NOT DISTINCT FROM s.service_name\n      AND m.span_id = s.span_id", "ON m.trace_id = s.trace_id\n      AND m.service_name IS NOT DISTINCT FROM s.service_name\n      AND m.span_id = m.parent_span_id"), want: "assert_no_orphan_tokens: trajectory_context FROM is"},
	{area: "assertions", name: "orphans_seed_join_self_span", file: dfxFOrphans,
		edit: dashReplaceFirst("AND p.span_id = c.parent_span_id", "AND p.span_id = c.span_id"), want: "assert_no_orphan_tokens: emitted_ancestors seed FROM is"},
	{area: "assertions", name: "orphans_seed_path_array_broken", file: dfxFOrphans,
		edit: dashReplaceFirst("[c.span_id, p.span_id] AS path_span_ids", "[c.span_id, c.span_id] AS path_span_ids"), want: "assert_no_orphan_tokens: emitted_ancestors seed select items"},
	{area: "assertions", name: "orphans_recursive_join_broken", file: dfxFOrphans,
		edit: dashReplaceFirst("AND p.span_id = ea.ancestor_parent_span_id", "AND p.span_id = ea.ancestor_span_id"), want: "assert_no_orphan_tokens: emitted_ancestors recursive FROM is"},
	{area: "assertions", name: "orphans_recursive_path_array_broken", file: dfxFOrphans,
		edit: dashReplaceFirst("ARRAY_CONCAT(ea.path_span_ids, [p.span_id]) AS path_span_ids", "[p.span_id] AS path_span_ids"), want: "assert_no_orphan_tokens: emitted_ancestors recursive select items"},
	{area: "assertions", name: "orphans_recursive_where_and_false", file: dfxFOrphans,
		edit: dashReplaceFirst("WHERE ea.hop < 32", "WHERE ea.hop < 32 AND FALSE"), want: "assert_no_orphan_tokens: emitted_ancestors recursive WHERE is"},
	{area: "assertions", name: "orphans_recursive_cycle_guard_removed", file: dfxFOrphans,
		edit: dashReplaceFirst("    WHERE ea.hop < 32\n      AND p.span_id NOT IN UNNEST(ea.path_span_ids)", "    WHERE ea.hop < 32"), want: "assert_no_orphan_tokens: emitted_ancestors recursive WHERE is"},
	{area: "assertions", name: "orphans_missing_not_exists_and_false", file: dfxFOrphans,
		edit: dashReplaceFirst("          AND m.span_id = s.span_id\n      )", "          AND m.span_id = s.span_id\n          AND FALSE\n      )"), want: "assert_no_orphan_tokens: the WHERE of the WITH table missing is"},
	{area: "assertions", name: "orphans_missing_branch_where_false", file: dfxFOrphans,
		edit: dashReplaceFirst("SELECT trace_id, service_name, span_id, violation\nFROM missing", "SELECT trace_id, service_name, span_id, violation\nFROM missing\nWHERE FALSE"), want: "assert_no_orphan_tokens: final UNION ALL branches"},
	{area: "assertions", name: "orphans_description_claims_3h_window", file: dfxFOrphans,
		edit: dashReplaceFirst("description: \"Fails when the token columns of the trajectory mart break the is_token_leaf sum invariants in the 24-hour context window anchored at stg_run_window.window_start.\"", "description: \"Fails when the token columns of the trajectory mart break the is_token_leaf sum invariants in the last 3 hours.\""), want: "assert_no_orphan_tokens: description"},
	{area: "assertions", name: "cycles_comment_claims_3h_window", file: dfxFCycles,
		edit: dashReplaceFirst("in the 24-hour context window anchored at\n * stg_run_window.window_start", "in the last 3 hours"), want: "assert_no_unhandled_cycles: file text still mentions \"last 3 hours\""},
	{area: "assertions", name: "orphans_staged_window_longer", file: dfxFOrphans,
		edit: dashReplaceFirst("    WHERE start_time >= TIMESTAMP_SUB(\n      (SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw),\n      INTERVAL 24 HOUR\n    )", "    WHERE start_time >= TIMESTAMP_SUB((SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw), INTERVAL 1 HOUR)"), want: "assert_no_orphan_tokens: the WHERE of the WITH table staged is"},
	{area: "assertions", name: "orphans_staged_window_removed", file: dfxFOrphans,
		edit: dashReplaceFirst("    WHERE start_time >= TIMESTAMP_SUB(\n      (SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw),\n      INTERVAL 24 HOUR\n    )\n", ""), want: "assert_no_orphan_tokens: the WHERE of the WITH table staged is"},
	{area: "assertions", name: "orphans_reach_shorter", file: dfxFOrphans,
		edit: dashReplaceAll("INTERVAL 24 HOUR", "INTERVAL 12 HOUR"), want: "assert_no_orphan_tokens: the WHERE of the WITH table missing is"},
	{area: "assertions", name: "orphans_reads_a_column_the_mart_lacks", file: dfxFOrphans,
		edit: dashReplaceFirst("      m.is_token_leaf,\n", "      m.is_token_leafs,\n"), want: "is_token_leafs"},
	{area: "assertions", name: "orphans_violation_renamed", file: dfxFOrphans,
		edit: dashReplaceFirst("SELECT trace_id, service_name, span_id, violation\nFROM row_violations", "SELECT trace_id, service_name, span_id, violation AS rule\nFROM row_violations"), want: "branch 1 of the final SELECT outputs"},
	{area: "assertions", name: "orphans_with_table_renamed", file: dfxFOrphans,
		edit: dashReplaceFirst("  double_counted AS (", "  doubled AS ("), want: "assert_no_orphan_tokens: the WITH tables are [staged trajectory_context emitted_ancestors token_parent_coverage emitted row_violations doubled missing]"},
	{area: "assertions", name: "orphans_reads_the_staging_table_again", file: dfxFOrphans,
		edit: dashReplaceFirst("    WHERE start_time >= TIMESTAMP_SUB(\n      (SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw),\n      INTERVAL 24 HOUR\n    )\n", "    WHERE start_time >= TIMESTAMP_SUB(\n      (SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw),\n      INTERVAL 24 HOUR\n    )\n      AND span_id IN (SELECT span_id FROM ${ref(\"stg_spans_dedup\")})\n"), want: "reads the staging table and the trajectory mart 5 times, want 4"},
	{area: "assertions", name: "orphans_drops_expected_is_token_leaf", file: dfxFOrphans,
		edit: dashReplaceFirst("            e.expected_is_token_leaf\n", "            FALSE\n"), want: "row_violations must check E . EXPECTED_IS_TOKEN_LEAF"},
	{area: "assertions", name: "orphans_swaps_coverage_family", file: dfxFOrphans,
		edit: dashReplaceFirst("LOGICAL_OR(ea.has_input) AS has_input", "LOGICAL_OR(ea.has_output) AS has_input"), want: "token_parent_coverage select items"},
	{area: "assertions", name: "cycles_loses_a_column", file: dfxFCycles,
		edit: dashReplaceFirst("  COALESCE(emitted.emitted_rows, 0) AS emitted_rows,\n", ""), want: "branch 1 of the final SELECT outputs [trace_id service_name span_id violation], want [trace_id service_name span_id emitted_rows violation]"},
	{area: "assertions", name: "cycles_reach_shorter", file: dfxFCycles,
		edit: dashReplaceAll("INTERVAL 24 HOUR", "INTERVAL 12 HOUR"), want: "assert_no_unhandled_cycles: the WHERE of the WITH table emitted is"},
	{area: "assertions", name: "cycles_staged_window_longer", file: dfxFCycles,
		edit: dashReplaceFirst("  WHERE start_time >= TIMESTAMP_SUB(\n    (SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw),\n    INTERVAL 24 HOUR\n  )\n),", "  WHERE start_time >= TIMESTAMP_SUB((SELECT rw.window_start FROM ${ref(\"stg_run_window\")} AS rw), INTERVAL 3 HOUR)\n),"), want: "assert_no_unhandled_cycles: the WHERE of the WITH table staged is"},

	// The README.
	{area: "docs", name: "partition_column_documented_wrongly", file: dfxDoc,
		edit: dashEditLine("| `m_gen_ai_model_costs_hourly` | incremental", "`DATE(usage_hour)`", "`usage_hour`"), want: "the action table partitions m_gen_ai_model_costs_hourly by"},
	{area: "docs", name: "cluster_order_documented_wrongly", file: dfxDoc,
		edit: dashEditLine("| `stg_spans_dedup` | incremental", "`service_name`, `trace_id`", "`trace_id`, `service_name`"), want: "the action table clusters stg_spans_dedup by [trace_id service_name], want [service_name trace_id]"},
	{area: "docs", name: "read_documented_wrongly", file: dfxDoc,
		edit: dashEditLine("| `m_gen_ai_agent_trajectory` | incremental", "`stg_spans_dedup`", "`v_gen_ai_spans`"), want: "the action table says m_gen_ai_agent_trajectory reads [stg_run_window v_gen_ai_spans], want [stg_run_window stg_spans_dedup]"},
	{area: "docs", name: "mart_row_missing", file: dfxDoc,
		edit: dfxDropLine("| `m_mcp_tool_usage_hourly` | incremental"), want: "the action table has no row for m_mcp_tool_usage_hourly"},
	{area: "docs", name: "type_documented_wrongly", file: dfxDoc,
		edit: dashEditLine("| `assert_no_orphan_tokens` | assertion", "| assertion |", "| incremental |"), want: `the action table gives assert_no_orphan_tokens the type "incremental", want "assertion"`},
	{area: "docs", name: "declaration_row_incomplete", file: dfxDoc,
		edit: dashEditLine("| declaration |", "`v_mcp_tool_usage`", "`v_other`"), want: "the action table lists the declarations"},
	{area: "docs", name: "action_table_header_changed", file: dfxDoc,
		edit: dashReplaceFirst("| Action | Type | Reads | Partition by | Cluster by |", "| Action | Type | Reads | Partitioned | Cluster by |"), want: "has no table with the header"},
	{area: "docs", name: "view_mapped_to_another_mart", file: dfxDoc,
		edit: dashEditLine("| `v_gen_ai_sessions` | `m_gen_ai_sessions_daily` |", "`m_gen_ai_sessions_daily`", "`m_mcp_tool_usage_hourly`"), want: `the table of views and marts maps v_gen_ai_sessions to "m_mcp_tool_usage_hourly", want m_gen_ai_sessions_daily`},
	{area: "docs", name: "violation_not_documented", file: dfxDoc,
		edit: dashReplaceFirst("`rollup_token_changed` (a span that is not a token leaf has an uncovered count or lost/changed a count, or a partial leaf nulled an uncovered count), ", ""), want: "the table of assertions says assert_no_orphan_tokens returns the rules"},
	{area: "docs", name: "assertion_row_missing", file: dfxDoc,
		edit: dfxDropLine("| `assert_no_unhandled_cycles` | a staged span appears"), want: "the table of assertions has no row for assert_no_unhandled_cycles"},
	{area: "docs", name: "window_snippet_changed", file: dfxDoc,
		edit: dashReplaceFirst("window_start = "+dfxFixedWindow, "window_start = "+strings.Replace(dfxFixedWindow, "INTERVAL 3 HOUR", "INTERVAL 2 HOUR", 1)), want: "the window_start snippet is"},
	{area: "docs", name: "lookback_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("the lookback `L` is 3 to 4 hours", "the lookback `L` is 2 to 3 hours"), want: "the lookback `L` is 3 to 4 hours"},
	{area: "docs", name: "run_frequency_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("Run at least every 3 hours", "Run at least every 6 hours"), want: "Run at least every 3 hours"},
	{area: "docs", name: "reach_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("over the spans at or after `window_start - 24h`", "over the spans at or after `window_start - 12h`"), want: "over the spans at or after `window_start - 24h`"},
	{area: "docs", name: "depth_limit_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("deeper than 16 levels", "deeper than 12 levels"), want: "deeper than 16 levels"},
	{area: "docs", name: "mart_count_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("the four marts and the two\nassertions read)", "the five marts and the two\nassertions read)"), want: "the four marts and the two assertions read)"},
	{area: "docs", name: "core_version_documented_wrongly", file: dfxDoc,
		edit: dashReplaceFirst("pins `dataformCoreVersion: 3.1.2`", "pins `dataformCoreVersion: 3.0.0`"), want: "pins `dataformCoreVersion: 3.1.2`"},
	{area: "docs", name: "heading_renamed", file: dfxDoc,
		edit: dashReplaceFirst("## Assertions", "## Checks"), want: `lacks "## Assertions"`},
	{area: "docs", name: "exactly_once_claimed", file: dfxDoc,
		edit: dashReplaceFirst("It is not\nexactly-once processing", "It is\nexactly-once processing"), want: "It is not exactly-once processing"},
	{area: "docs", name: "status_message_promise_removed", file: dfxDoc,
		edit: dashReplaceFirst("never stages or materializes `status_message`", "never stages the status"), want: "never stages or materializes"},
	{area: "docs", name: "terraform_variable_unknown", file: dfxFTerra,
		edit: dashReplaceFirst("variable \"allow_unprunable_views\"", "variable \"allow_unprunable\""), want: "names the Terraform variable allow_unprunable_views, which deploy/terraform/variables.tf does not declare"},
}

// ---------------------------------------------------------------------------
// The readers under the rules.
// ---------------------------------------------------------------------------

// TestDfxReaders pins down what the readers of the package accept and what
// they refuse: the rules above are only as good as the readers under them.
func TestDfxReaders(t *testing.T) {
	t.Run("config members", func(t *testing.T) {
		src := "type: \"incremental\", // a line comment\n" +
			"tags: [\"a\", \"b\",],\n" +
			"/* a block comment */ description: \"a \\\"quoted\\\" word\",\n" +
			"database: dataform.projectConfig.vars.source_project,\n" +
			"bigquery: { partitionBy: \"DATE(x)\", clusterBy: [\"a\"] }\n"
		got, err := (&dfxReader{src: src}).object(0)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{
			"type":        "incremental",
			"tags":        []any{"a", "b"},
			"description": `a "quoted" word`,
			"database":    dfxExpr("dataform.projectConfig.vars.source_project"),
			"bigquery":    map[string]any{"partitionBy": "DATE(x)", "clusterBy": []any{"a"}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("the members are %#v, want %#v", got, want)
		}
	})

	t.Run("refused config", func(t *testing.T) {
		for _, tc := range []struct{ name, src, want string }{
			{"a missing comma", `a: "x" b: "y"`, "expected ',' after key a"},
			{"a repeated key", `a: "x", a: "y"`, "key a appears twice"},
			{"an object that is not closed", `a: { b: "x"`, "an object is not closed"},
			{"a missing value", `a:`, "a value is missing"},
			{"a missing colon", `a "x"`, "key a: expected ':'"},
			{"a missing key", `: "x"`, "expected a key"},
			{"a number", `a: 5`, "unexpected '5'"},
			{"a list without a comma", `a: ["x" "y"]`, "expected ',' or ']'"},
			{"a string that is not closed", `a: "x`, "unterminated string"},
			{"an escape the reader does not know", `a: "x\qy"`, "string at byte"},
		} {
			if _, err := (&dfxReader{src: tc.src}).object(0); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: %q gave the error %v, want one with %q", tc.name, tc.src, err, tc.want)
			}
		}
	})

	t.Run("matching braces", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			src  string
			open int
			want int // -1 when the brace is never closed
		}{
			{"nested braces", "{ a { b } c }", 0, 12},
			{"an inner brace", "{ a { b } c }", 4, 8},
			{"a brace in an SQL string", "{ x = '}' }", 0, 10},
			{"a brace in a double-quoted string", "{ x = \"}\" }", 0, 10},
			{"a brace in a template literal after an expression", "{ `a ${ b } }` }", 0, 15},
			{"an escaped backtick in a template literal", "{ `\\`}` }", 0, 8},
			{"a brace in an SQL comment", "{ -- }\n }", 0, 8},
			{"a brace in a line comment", "{ // }\n }", 0, 8},
			{"a brace in a block comment", "{ /* } */ }", 0, 10},
			{"a brace that is never closed", "{ a { b }", 0, -1},
			{"a block comment that is never closed", "{ /* }", 0, -1},
			{"a string that is never closed", "{ 'a }", 0, -1},
		} {
			if got := dfxMatchBrace(tc.src, tc.open); got != tc.want {
				t.Errorf("%s: %q closes at %d, want %d", tc.name, tc.src, got, tc.want)
			}
		}
	})

	t.Run("placeholders", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			text        string
			incremental bool
			want        string
		}{
			{"self and ref", "a ${self()} b ${ ref(\"x\") }", true, "a SELF b `ref.x`"},
			{"no placeholder", "SELECT 1", false, "SELECT 1"},
			{"when in an incremental run", "${when(incremental(), `A`)}", true, "A"},
			{"when in a full refresh", "${when(incremental(), `A`)}", false, ""},
			{"when with an else, incremental", "${when(incremental(), `A`, `B`)}", true, "A"},
			{"when with an else, full refresh", "${when(incremental(), `A`, `B`)}", false, "B"},
			{"a placeholder inside when", "${when(incremental(), `DELETE FROM ${self()} WHERE x`)}", true, "DELETE FROM SELF WHERE x"},
			{"a brace inside a template literal", "${when(incremental(), `a } b`)}", true, "a } b"},
		} {
			got, err := dfxExpand(tc.text, tc.incremental)
			if err != nil || got != tc.want {
				t.Errorf("%s: %q expands to %q (%v), want %q", tc.name, tc.text, got, err, tc.want)
			}
		}
		for _, tc := range []struct{ name, text, want string }{
			{"a test that is not incremental()", "${when(foo, `a`)}", "when() must test incremental()"},
			{"arguments without a comma", "${when(incremental() `a`)}", "when() arguments must be separated by commas"},
			{"an argument that is not a template literal", "${when(incremental(), \"a\")}", "when() takes template literals"},
			{"no branch", "${when(incremental())}", "when() takes one or two template literals, got 0"},
			{"three branches", "${when(incremental(), `a`, `b`, `c`)}", "when() takes one or two template literals, got 3"},
			{"a template literal that is not closed", "${when(incremental(), `a)}", "unterminated ${"},
			{"an expression the package does not use", "${unknown()}", "is not one the package uses"},
			{"a placeholder that is not closed", "${self()", "unterminated ${"},
		} {
			if _, err := dfxExpand(tc.text, true); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: %q gave the error %v, want one with %q", tc.name, tc.text, err, tc.want)
			}
		}
	})

	t.Run("operations", func(t *testing.T) {
		if got, want := dfxSplit("a\n---\n\n---\n  b\nb2\n  ---  \nc\n"), []string{"a", "b\nb2", "c"}; !slices.Equal(got, want) {
			t.Errorf("the statements are %q, want %q", got, want)
		}
		if got := dfxSplit(""); len(got) != 0 {
			t.Errorf("an empty block holds the statements %q", got)
		}
		text := "DECLARE window_start TIMESTAMP DEFAULT ${when(incremental(), `TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)`, `TIMESTAMP('1970-01-01 00:00:00+00')`)}\n" +
			"---\n${when(incremental(), `BEGIN TRANSACTION`)}\n---\n-- only a comment\n---\n" +
			"${when(incremental(), `DELETE FROM ${self()} WHERE x >= window_start`)}\n---\nSELECT 1\n"
		for _, tc := range []struct {
			incremental bool
			want        []string
		}{
			{true, []string{"declare window_start", "begin transaction", "delete", "unrecognized"}},
			{false, []string{"declare window_start", "unrecognized"}},
		} {
			stmts, err := dfxBlock(text, tc.incremental)
			if err != nil {
				t.Fatal(err)
			}
			if got := dfxKinds(stmts); !slices.Equal(got, tc.want) {
				t.Errorf("incremental is %v: the statements are %v, want %v", tc.incremental, got, tc.want)
			}
		}
		if _, err := dfxBlock("SELECT 'a", true); err == nil || !strings.Contains(err.Error(), "statement 1") {
			t.Errorf("a statement with a string that is not closed gave the error %v, want one that names statement 1", err)
		}
		for _, tc := range []struct{ stmt, want string }{
			{"declare window_start timestamp default x", "declare window_start"},
			{"DECLARE window_start DATE DEFAULT x", "unrecognized"},
			{"DECLARE context_start TIMESTAMP DEFAULT x", "declare context_start"},
			{"CREATE TEMP TABLE touched_traces AS SELECT 1", "temp table touched_traces"},
			{"CREATE TEMP TABLE trajectory_rows AS SELECT 1", "temp table trajectory_rows"},
			{"CREATE TEMP TABLE other AS SELECT 1", "unrecognized"},
			{"BEGIN TRANSACTION", "begin transaction"},
			{"BEGIN TRANSACTIONS", "unrecognized"},
			{"COMMIT TRANSACTION", "commit transaction"},
			{"DELETE FROM SELF WHERE x", "delete"},
			{"DELETE FROM t WHERE x", "unrecognized"},
		} {
			toks, err := dashTokenize(tc.stmt)
			if err != nil {
				t.Fatal(err)
			}
			if got := dfxClassify(toks); got != tc.want {
				t.Errorf("%q is a %q statement, want %q", tc.stmt, got, tc.want)
			}
		}
	})

	t.Run("action files", func(t *testing.T) {
		src := "/*\n * Copyright 2026 Google LLC\n */\n" +
			"config {\n  type: \"incremental\",\n  tags: [\"otel_marts\"],\n  description: \"d\",\n  bigquery: { partitionBy: \"DATE(x)\" }\n}\n\n" +
			"pre_operations {\n  DECLARE window_start TIMESTAMP DEFAULT ${when(incremental(), `TIMESTAMP('2020-01-01')`)}\n}\n\n" +
			"post_operations {\n  ${when(incremental(), `COMMIT TRANSACTION`)}\n}\n\n" +
			"SELECT x FROM ${ref(\"v\")}\n"
		a := dfxParseAction("deploy/dataform/definitions/marts/m_x.sqlx", src)
		if len(a.problems) != 0 {
			t.Fatalf("problems: %v", a.problems)
		}
		if a.name != "m_x" || a.cfg.typ != "incremental" || !slices.Equal(a.cfg.tags, []string{"otel_marts"}) || a.cfg.partitionBy != "DATE(x)" {
			t.Errorf("the action is %+v", a.cfg)
		}
		if got, want := strings.TrimSpace(a.pre), "DECLARE window_start TIMESTAMP DEFAULT ${when(incremental(), `TIMESTAMP('2020-01-01')`)}"; got != want {
			t.Errorf("pre_operations is %q, want %q", got, want)
		}
		if got, want := strings.TrimSpace(a.post), "${when(incremental(), `COMMIT TRANSACTION`)}"; got != want {
			t.Errorf("post_operations is %q, want %q", got, want)
		}
		if want := "SELECT x FROM ${ref(\"v\")}"; a.body != want {
			t.Errorf("the body is %q, want %q", a.body, want)
		}
		renamed := dfxParseAction("deploy/dataform/definitions/sources/file.sqlx", "config {\n  type: \"declaration\",\n  name: \"other\"\n}\n")
		if renamed.name != "other" {
			t.Errorf("the config names the action %q, want other", renamed.name)
		}

		for _, tc := range []struct{ name, src, want string }{
			{"no config block", "SELECT 1\n", "there is no config block"},
			{"a config block twice", "config {\n  type: \"assertion\"\n}\nconfig {\n  type: \"assertion\"\n}\nSELECT 1\n", "the config block appears twice"},
			{"a config block that is not closed", "config {\n  type: \"assertion\"\nSELECT 1\n", "the config block is not closed"},
			{"a pre_operations block that is not closed", "config {\n  type: \"assertion\"\n}\npre_operations {\n  SELECT 1\n", "the pre_operations block is not closed"},
			{"a config block that cannot be read", "config {\n  type: \"assertion\" tags: []\n}\nSELECT 1\n", "the config block: expected ',' after key type"},
			{"a config without a type", "config {\n  description: \"d\"\n}\nSELECT 1\n", "the config block has no type"},
			{"a type the package does not use", "config {\n  type: \"view\"\n}\nSELECT 1\n", `the config type "view" is not one the package uses`},
			{"a key the type does not take", "config {\n  type: \"assertion\",\n  partitionBy: \"x\"\n}\nSELECT 1\n", "config key partitionBy is not one the package sets on a assertion action"},
			{"a tag that is not a list", "config {\n  type: \"assertion\",\n  tags: \"otel_marts\"\n}\nSELECT 1\n", "config key tags is not a list of strings"},
			{"a description that is not a string", "config {\n  type: \"assertion\",\n  description: [\"x\"]\n}\nSELECT 1\n", "config key description is not a string"},
			{"a literal for an expression", "config {\n  type: \"declaration\",\n  database: \"p\"\n}\n", "config key database has to be an expression"},
			{"a bigquery key that is not an object", "config {\n  type: \"incremental\",\n  bigquery: \"x\"\n}\nSELECT 1\n", "config key bigquery is not an object"},
			{"a bigquery key the package does not set", "config {\n  type: \"incremental\",\n  bigquery: { updatePartitionFilter: \"x\" }\n}\nSELECT 1\n", "config key bigquery.updatePartitionFilter is not one the package sets"},
		} {
			if got := dfxParseAction("m.sqlx", tc.src).problems; !containsText(got, tc.want) {
				t.Errorf("%s: the problems are %q, want one with %q", tc.name, got, tc.want)
			}
		}
	})

	t.Run("dependency cycles", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			edges map[string][]string
			want  []string
		}{
			{"a graph without a cycle", map[string][]string{"a": {"b", "c"}, "b": {"c"}, "c": nil}, nil},
			{"a cycle through three actions", map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"a"}}, []string{"a", "b", "c", "a"}},
			{"an action that reads itself", map[string][]string{"a": {"a"}}, []string{"a", "a"}},
			{"a cycle that does not hold the first action", map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"b"}}, []string{"b", "c", "b"}},
		} {
			if got := dfxCycle(tc.edges); !slices.Equal(got, tc.want) {
				t.Errorf("%s: the cycle is %v, want %v", tc.name, got, tc.want)
			}
		}
	})

	t.Run("differences", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			got, want []string
			diff      string
		}{
			{"equal lists", []string{"a", "b"}, []string{"a", "b"}, ""},
			{"a changed order", []string{"a", "b"}, []string{"b", "a"}, "column 1 is a, want b"},
			{"a lacking column", []string{"a"}, []string{"a", "b"}, "lacks b; 1 columns, want 2"},
			{"an extra column", []string{"a", "b", "x"}, []string{"a", "b"}, "has the extra column x; 3 columns, want 2"},
			{"a renamed column", []string{"a", "x"}, []string{"a", "b"}, "lacks b; has the extra column x"},
		} {
			if got := dfxDiffColumns(tc.got, tc.want); got != tc.diff {
				t.Errorf("%s: dfxDiffColumns says %q, want %q", tc.name, got, tc.diff)
			}
		}
		for _, tc := range []struct {
			name      string
			got, want []string
			diff      string
		}{
			{"equal lists", []string{"a", "b"}, []string{"a", "b"}, ""},
			{"a changed entry", []string{"a", "x"}, []string{"a", "b"}, `entry 2 is "x", want "b"`},
			{"an extra entry", []string{"a", "b", "c"}, []string{"a", "b"}, `has the extra entry "c"`},
			{"a lacking entry", []string{"a"}, []string{"a", "b"}, `lacks the entry "b"`},
		} {
			if got := dfxDiffSeq(tc.got, tc.want); got != tc.diff {
				t.Errorf("%s: dfxDiffSeq says %q, want %q", tc.name, got, tc.diff)
			}
		}
	})

	t.Run("README tables", func(t *testing.T) {
		text := "intro\n\n| A | B |\n|---|---|\n| `x` | `y`, `z` |\n|  p  | q |\n\nafter\n| A | B |\n"
		want := [][]string{{"`x`", "`y`, `z`"}, {"p", "q"}}
		if got := dfxTable(text, "| A | B |"); !reflect.DeepEqual(got, want) {
			t.Errorf("the rows are %q, want %q", got, want)
		}
		if got := dfxTable(text, "| C |"); got != nil {
			t.Errorf("a table that is not there has the rows %q", got)
		}
		if got := dfxCode("`y`, `z` and `w`"); !slices.Equal(got, []string{"y", "z", "w"}) {
			t.Errorf("the code words are %q", got)
		}
		for n, want := range map[int]string{0: "zero", 4: "four", 10: "ten", 11: "11", -1: "-1"} {
			if got := dfxWord(n); got != want {
				t.Errorf("dfxWord(%d) is %q, want %q", n, got, want)
			}
		}
	})

	t.Run("identifiers resolve against the tables a query reads", func(t *testing.T) {
		tables := map[string][]string{"ref.t": {"a", "b"}}
		for _, tc := range []struct {
			name string
			sql  string
			vars []string
			want string // "" for no problem
		}{
			{"the columns of a table of the package", "SELECT a, b FROM `ref.t` WHERE a > 1", nil, ""},
			{"a qualified column", "SELECT m.a FROM `ref.t` AS m", nil, ""},
			{"a script variable", "SELECT a FROM `ref.t` WHERE a >= window_start", []string{"window_start"}, ""},
			{"a WITH table", "WITH s AS (SELECT a FROM `ref.t`) SELECT a FROM s", nil, ""},
			{"a column the table lacks", "SELECT a, zz FROM `ref.t`", nil, "the final SELECT: select item zz: unknown identifier zz"},
			{"a script variable that is not declared", "SELECT a FROM `ref.t` WHERE a >= window_start", nil, "unknown identifier window_start"},
			{"a table that is not known", "SELECT a FROM mystery", nil, "FROM mystery: not a WITH table, a table of the package or a temporary table of the action"},
			{"a column a WITH table lacks", "WITH s AS (SELECT a FROM `ref.t`) SELECT b FROM s", nil, "unknown identifier b"},
			{"a branch of a UNION", "SELECT a FROM `ref.t` UNION ALL SELECT zz FROM `ref.t`", nil, "the final SELECT: branch 2: select item zz: unknown identifier zz"},
			{"a column of a WITH table", "WITH s AS (SELECT zz FROM `ref.t`) SELECT 1 AS one FROM s", nil, "WITH table s: select item zz: unknown identifier zz"},
		} {
			problems := dfxResolve(mustParse(t, tc.sql), tables, tc.vars...)
			switch {
			case tc.want == "" && len(problems) != 0:
				t.Errorf("%s: unexpected problems %q", tc.name, problems)
			case tc.want != "" && !containsText(problems, tc.want):
				t.Errorf("%s: the problems are %q, want one with %q", tc.name, problems, tc.want)
			}
		}
	})
}

// TestDfxPlanAgreesWithTheDashboards ties the plan of this file to the one of
// the dashboards: both name the same marts for the same views, and every
// declaration stands for a view of the exporter.
func TestDfxPlanAgreesWithTheDashboards(t *testing.T) {
	views := dashViewSQLs()
	marts := 0
	for _, e := range dfxPlan {
		if e.view != "" {
			marts++
			if got := dashMartForView[e.view]; got != e.name {
				t.Errorf("the plan keeps the columns of %s in %s, the dashboards name %q", e.view, e.name, got)
			}
			if _, ok := views[e.view]; !ok {
				t.Errorf("%s keeps the columns of %s, which is not a view of the exporter", e.name, e.view)
			}
		}
		if _, ok := views[e.name]; ok != (e.typ == "declaration") {
			t.Errorf("%s is a %s action, and a view of the exporter is %v", e.name, e.typ, ok)
		}
	}
	if marts != len(dashMartForView) {
		t.Errorf("the plan keeps the columns of %d views, the dashboards name %d marts", marts, len(dashMartForView))
	}
}

// TestDataformSharedRunWindowAcrossHourBoundary verifies that because
// stg_run_window captures window_start once at the beginning of a Dataform
// invocation (or from vars.run_timestamp), a run that starts at 12:59:58Z and
// crosses 13:00:00Z before m_gen_ai_agent_trajectory and the assertions execute
// uses the exact same 09:00:00Z cutoff in every action instead of drifting to
// 10:00:00Z and leaving spans in [09:00:00Z, 10:00:00Z) unmaterialized in the
// trajectory mart.
func TestDataformSharedRunWindowAcrossHourBoundary(t *testing.T) {
	files := dashLoadFileSet(t)
	pkg := dfxLoad(files)
	lookback, err := dfxLookback(pkg)
	if err != nil {
		t.Fatalf("dfxLookback: %v", err)
	}
	if lookback != 3 {
		t.Fatalf("lookback = %d, want 3", lookback)
	}

	// Simulate DAG start just before an hour boundary (12:59:58Z) and downstream
	// execution after the hour boundary (13:01:05Z), with a staged span at
	// 09:30:00Z.
	dagStart := time.Date(2026, 10, 25, 12, 59, 58, 0, time.UTC)
	downstreamStart := time.Date(2026, 10, 25, 13, 1, 5, 0, time.UTC)
	spanStart := time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC)

	sharedWindowStart := dagStart.Add(-time.Duration(lookback) * time.Hour).Truncate(time.Hour)
	driftedWindowStart := downstreamStart.Add(-time.Duration(lookback) * time.Hour).Truncate(time.Hour)

	if want := time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC); !sharedWindowStart.Equal(want) {
		t.Fatalf("sharedWindowStart = %v, want %v", sharedWindowStart, want)
	}
	if want := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC); !driftedWindowStart.Equal(want) {
		t.Fatalf("driftedWindowStart = %v, want %v", driftedWindowStart, want)
	}

	// Under the old per-action CURRENT_TIMESTAMP() design, stg_spans_dedup staged
	// the 09:30:00Z span (>= 09:00:00Z) while m_gen_ai_agent_trajectory's
	// touched_traces scanned >= 10:00:00Z and missed it.
	if spanStart.Compare(sharedWindowStart) < 0 || spanStart.Compare(driftedWindowStart) >= 0 {
		t.Fatalf("spanStart %v must fall in [sharedWindowStart %v, driftedWindowStart %v)", spanStart, sharedWindowStart, driftedWindowStart)
	}

	// Verify that every downstream incremental action and assertion in the
	// package depends on stg_run_window and reads its window_start rather than
	// calling CURRENT_TIMESTAMP().
	for _, e := range dfxPlan {
		if (e.typ != "incremental" && e.typ != "assertion") || e.name == dfxRunWindow {
			continue
		}
		a := pkg.byName[e.name]
		if a == nil {
			t.Fatalf("missing action %s", e.name)
		}
		if !slices.Contains(a.cfg.deps, dfxRunWindow) {
			t.Errorf("%s: dependencies %v do not include %s", e.name, a.cfg.deps, dfxRunWindow)
		}
		if !slices.Contains(dfxRefs(a), dfxRunWindow) {
			t.Errorf("%s: refs %v do not include %s", e.name, dfxRefs(a), dfxRunWindow)
		}
		inc, err := dfxRender(a, true)
		if err != nil {
			t.Fatalf("%s: %v", e.name, err)
		}
		for _, st := range slices.Concat(inc.pre, inc.post) {
			if strings.Contains(dashJoin(st.toks), "CURRENT_TIMESTAMP") {
				t.Errorf("%s: operation %q calls CURRENT_TIMESTAMP() directly", e.name, st.kind)
			}
		}
		if strings.Contains(strings.ToUpper(inc.body), "CURRENT_TIMESTAMP") {
			t.Errorf("%s: body calls CURRENT_TIMESTAMP() directly", e.name)
		}
	}

	// Verify stg_run_window evaluation under empty default, omitted key, valid
	// ISO-8601 UTC overrides, non-UTC or date-only strings (rejected by
	// REGEXP_CONTAINS), invalid calendar dates (rejected by CAST), and
	// quote/backslash injection strings (escaped by JSON.stringify).
	rwAction := pkg.byName[dfxRunWindow]
	if rwAction == nil {
		t.Fatalf("missing action %s", dfxRunWindow)
	}
	evalRunWindow := func(vars map[string]string, now time.Time, incremental bool, prevWindow *time.Time, prevCommitted bool) (time.Time, time.Time, bool, error) {
		run, err := dfxRenderWithVars(rwAction, incremental, vars)
		if err != nil {
			return time.Time{}, time.Time{}, false, err
		}
		q, err := dashParseSQL(run.body)
		if err != nil {
			return time.Time{}, time.Time{}, false, err
		}
		clock := dfxCTE(q, "clock")
		if clock == nil || len(clock.branches) != 1 || len(clock.branches[0].items) != 1 {
			return time.Time{}, time.Time{}, false, errors.New("missing clock CTE")
		}
		var lit string
		for _, tok := range clock.branches[0].items[0].toks {
			if tok.kind == dashString && strings.HasPrefix(tok.text, "\"") {
				unq, err := strconv.Unquote(tok.text)
				if err != nil {
					return time.Time{}, time.Time{}, false, err
				}
				lit = unq
				break
			}
		}
		runTS := now
		if lit != "" {
			castInput := "INVALID_ISO8601_UTC_RUN_TIMESTAMP:" + lit
			if dfxRunTimestampISO8601UTC.MatchString(lit) {
				castInput = lit
			}
			parsed, err := time.Parse(time.RFC3339Nano, castInput)
			if err != nil {
				return time.Time{}, time.Time{}, false, fmt.Errorf("CAST(%q AS TIMESTAMP): %w", castInput, err)
			}
			runTS = parsed.UTC()
		}
		candidateWin := runTS.Add(-time.Duration(lookback) * time.Hour).Truncate(time.Hour)
		if !incremental {
			epoch := time.Unix(0, 0).UTC()
			if epoch.Before(candidateWin) {
				return runTS, epoch, false, nil
			}
			return runTS, candidateWin, false, nil
		}
		effectiveWin := candidateWin
		if prevWindow != nil && !prevCommitted && prevWindow.Before(effectiveWin) {
			effectiveWin = *prevWindow
		}
		return runTS, effectiveWin, false, nil
	}

	// Empty default and omitted key both fall back to CURRENT_TIMESTAMP().
	for _, vars := range []map[string]string{{"run_timestamp": ""}, nil, {}} {
		gotTS, gotWin, committed, err := evalRunWindow(vars, dagStart, true, nil, true)
		if err != nil || !gotTS.Equal(dagStart) || !gotWin.Equal(sharedWindowStart) || committed {
			t.Errorf("evalRunWindow(%v) = (%v, %v, %v, %v), want (%v, %v, false, nil)", vars, gotTS, gotWin, committed, err, dagStart, sharedWindowStart)
		}
	}

	// Valid ISO-8601 UTC strings (Z, +00:00, midnight, microsecond boundary).
	for _, tc := range []struct {
		raw     string
		wantTS  time.Time
		wantWin time.Time
	}{
		{
			raw:     "2026-04-20T12:34:56Z",
			wantTS:  time.Date(2026, 4, 20, 12, 34, 56, 0, time.UTC),
			wantWin: time.Date(2026, 4, 20, 9, 0, 0, 0, time.UTC),
		},
		{
			raw:     "2026-04-20T00:00:00Z",
			wantTS:  time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC),
			wantWin: time.Date(2026, 4, 19, 21, 0, 0, 0, time.UTC),
		},
		{
			raw:     "2026-04-20T23:59:59.999999Z",
			wantTS:  time.Date(2026, 4, 20, 23, 59, 59, 999999000, time.UTC),
			wantWin: time.Date(2026, 4, 20, 20, 0, 0, 0, time.UTC),
		},
		{
			raw:     "2026-04-20T12:34:56+00:00",
			wantTS:  time.Date(2026, 4, 20, 12, 34, 56, 0, time.UTC),
			wantWin: time.Date(2026, 4, 20, 9, 0, 0, 0, time.UTC),
		},
	} {
		gotTS, gotWin, _, err := evalRunWindow(map[string]string{"run_timestamp": tc.raw}, dagStart, true, nil, true)
		if err != nil || !gotTS.Equal(tc.wantTS) || !gotWin.Equal(tc.wantWin) {
			t.Errorf("evalRunWindow(%q) = (%v, %v, %v), want (%v, %v, nil)", tc.raw, gotTS, gotWin, err, tc.wantTS, tc.wantWin)
		}
	}

	// Non-UTC offsets, date-only strings, invalid dates, and injection strings must fail CAST.
	for _, bad := range []string{
		"2026-04-20T12:34:56+05:00",
		"2026-04-20",
		"2026-13-45T00:00:00Z",
		"not-a-timestamp",
		"2026-04-20' OR '1'='1",
		`2026-04-20" OR "1"="1`,
		`2026-04-20\`,
	} {
		if _, _, _, err := evalRunWindow(map[string]string{"run_timestamp": bad}, dagStart, true, nil, true); err == nil {
			t.Errorf("evalRunWindow(%q) succeeded, want CAST failure", bad)
		}
	}

	orphansAction := pkg.byName["assert_no_orphan_tokens"]
	orphansRun, err := dfxRender(orphansAction, true)
	if err != nil {
		t.Fatalf("render assert_no_orphan_tokens: %v", err)
	}
	cyclesAction := pkg.byName["assert_no_unhandled_cycles"]
	cyclesRun, err := dfxRender(cyclesAction, true)
	if err != nil {
		t.Fatalf("render assert_no_unhandled_cycles: %v", err)
	}
	trajAction := pkg.byName[dfxTraj]
	trajRun, err := dfxRender(trajAction, true)
	if err != nil {
		t.Fatalf("render %s: %v", dfxTraj, err)
	}
	postCyclesSQL, err := dfxAssertNotExistsSQL(trajRun.find("assert no_unhandled_cycles"))
	if err != nil {
		t.Fatalf("extract post_operations assert no_unhandled_cycles: %v", err)
	}
	postOrphansSQL, err := dfxOrphanTokenTempTableSQL(trajRun.find("temp table orphan_token_violations"))
	if err != nil {
		t.Fatalf("extract post_operations temp table orphan_token_violations: %v", err)
	}
	if err := dfxCheckOrphanTokenAssertStmt(trajRun.find("assert no_orphan_tokens")); err != nil {
		t.Fatalf("post_operations assert no_orphan_tokens: %v", err)
	}

	// evalTrajectoryPostOps executes a slice of m_gen_ai_agent_trajectory
	// post_operations statements in order, validating top-level statement
	// wrappers and GoogleSQL WITH RECURSIVE placement rules and aborting before
	// UPDATE stg_run_window SET is_committed = TRUE whenever any ASSERT fails.
	evalTrajectoryPostOps := func(stmts []dfxStmt, next []dfxSpanRow, win time.Time) (committed bool, cycViol, orphViol map[string][]string, err error) {
		var tempTableViolations map[string][]string
		hasTempTable := false
		assertCyclesRan := false
		assertOrphansRan := false
		for i, st := range stmts {
			for _, msg := range dfxCheckRecursivePlacement(st.kind, st.toks) {
				return false, nil, nil, fmt.Errorf("post_operations statement %d (%s): %s", i+1, st.kind, msg)
			}
			switch st.kind {
			case "commit transaction":
				if dashJoin(st.toks) != "COMMIT TRANSACTION" {
					return false, nil, nil, fmt.Errorf("unexpected commit transaction statement %q", dashJoin(st.toks))
				}
			case "assert no_unhandled_cycles":
				sqlText, err := dfxAssertNotExistsSQL(&st)
				if err != nil {
					return false, nil, nil, fmt.Errorf("assert no_unhandled_cycles wrapper: %w", err)
				}
				cycViol, err = dfxEvalUnhandledCyclesAssertion(sqlText, next, win)
				if err != nil {
					return false, nil, nil, fmt.Errorf("evaluating assert no_unhandled_cycles: %w", err)
				}
				assertCyclesRan = true
				if len(cycViol) > 0 {
					return false, cycViol, orphViol, nil
				}
			case "temp table orphan_token_violations":
				sqlText, err := dfxOrphanTokenTempTableSQL(&st)
				if err != nil {
					return false, nil, nil, fmt.Errorf("temp table orphan_token_violations wrapper: %w", err)
				}
				tempTableViolations, err = dfxEvalOrphanTokensAssertion(sqlText, next, win)
				if err != nil {
					return false, nil, nil, fmt.Errorf("evaluating temp table orphan_token_violations: %w", err)
				}
				hasTempTable = true
			case "assert no_orphan_tokens":
				if !hasTempTable {
					return false, nil, nil, errors.New("assert no_orphan_tokens executed before temp table orphan_token_violations")
				}
				if err := dfxCheckOrphanTokenAssertStmt(&st); err != nil {
					return false, nil, nil, err
				}
				orphViol = tempTableViolations
				assertOrphansRan = true
				if len(orphViol) > 0 {
					return false, cycViol, orphViol, nil
				}
			case "commit run_window":
				if !assertCyclesRan || !assertOrphansRan {
					return false, nil, nil, errors.New("commit run_window executed before both post_operations ASSERT gates succeeded")
				}
				committed = true
			default:
				return false, nil, nil, fmt.Errorf("unexpected post_operations statement kind %q", st.kind)
			}
		}
		return committed, cycViol, orphViol, nil
	}

	// runDAGStep executes one Dataform invocation in true DAG order:
	//   1. stg_run_window (computes window_start, writes is_committed = FALSE)
	//   2. stg_spans_dedup (stages rows with start_time >= window_start)
	//   3. m_gen_ai_agent_trajectory body + COMMIT TRANSACTION (materializes
	//      reachable non-cycle spans in touched_traces; if failMartBeforePost is
	//      true, aborts before post_operations)
	//   4. m_gen_ai_agent_trajectory post_operations: runs COMMIT TRANSACTION,
	//      ASSERT NOT EXISTS (cycles), CREATE OR REPLACE TEMP TABLE
	//      _bq_otel_orphan_token_violations AS WITH RECURSIVE ..., ASSERT NOT
	//      EXISTS (SELECT 1 FROM _bq_otel_orphan_token_violations), and finally
	//      UPDATE stg_run_window SET is_committed = TRUE WHERE TRUE.
	//   5. downstream assertions (assert_no_unhandled_cycles, assert_no_orphan_tokens)
	type dagStepResult struct {
		windowStart      time.Time
		stagedCount      int
		materializedCnt  int
		violatingSpanCnt int
		committed        bool
		cycleViolations  map[string][]string
		orphanViolations map[string][]string
	}
	runDAGStep := func(now time.Time, incremental bool, prevWin *time.Time, prevCommitted bool, rows []dfxSpanRow, failMartBeforePost bool) ([]dfxSpanRow, dagStepResult) {
		_, win, committed, err := evalRunWindow(nil, now, incremental, prevWin, prevCommitted)
		if err != nil {
			t.Fatalf("evalRunWindow at %v: %v", now, err)
		}
		next := slices.Clone(rows)
		contextStart := win.Add(-24 * time.Hour)

		if failMartBeforePost {
			stagedCnt, matCnt := 0, 0
			for _, r := range next {
				if !r.startTime.Before(contextStart) {
					stagedCnt++
					if r.inMart {
						matCnt++
					}
				}
			}
			return next, dagStepResult{
				windowStart:     win,
				stagedCount:     stagedCnt,
				materializedCnt: matCnt,
				committed:       committed,
			}
		}

		// Determine touched traces: s.start_time >= window_start OR (s.start_time >= context_start AND NOT inMart)
		touched := map[string]bool{}
		for _, r := range next {
			if !r.startTime.Before(win) || (!r.startTime.Before(contextStart) && !r.inMart) {
				touched[r.traceID+"|"+r.serviceName] = true
			}
		}

		// Build lookup of staged spans in context window for touched traces.
		inContextByKey := map[string]dfxSpanRow{}
		for _, r := range next {
			if !r.startTime.Before(contextStart) && touched[r.traceID+"|"+r.serviceName] {
				inContextByKey[r.traceID+"|"+r.serviceName+"|"+r.spanID] = r
			}
		}

		// A span in a touched trace is materialized by the recursive trajectory
		// query iff its parent chain reaches a root or orphan root within < 16
		// hops without repeating a span_id (so self-cycles and mutual cycles are
		// never emitted).
		reachesRoot := func(r dfxSpanRow) bool {
			visited := map[string]bool{r.spanID: true}
			cur := r
			for depth := 0; depth < 16; depth++ {
				if cur.parentSpanID == "" {
					return true
				}
				p, ok := inContextByKey[cur.traceID+"|"+cur.serviceName+"|"+cur.parentSpanID]
				if !ok {
					return true // orphan root
				}
				if visited[p.spanID] {
					return false // parent cycle
				}
				visited[p.spanID] = true
				cur = p
			}
			return false
		}

		for i := range next {
			s := &next[i]
			if !s.startTime.Before(contextStart) && touched[s.traceID+"|"+s.serviceName] {
				if reachesRoot(*s) {
					s.inMart = true
					s.isTokenLeaf = s.rawInput != nil || s.rawOutput != nil || s.rawReasoning != nil
					s.inputTokens = s.rawInput
					s.outputTokens = s.rawOutput
					s.reasoningTokens = s.rawReasoning
				} else {
					s.inMart = false
				}
			}
		}

		postCommitted, _, _, err := evalTrajectoryPostOps(trajRun.post, next, win)
		if err != nil {
			t.Fatalf("evalTrajectoryPostOps: %v", err)
		}

		cycViol, err := dfxEvalUnhandledCyclesAssertion(postCyclesSQL, next, win)
		if err != nil {
			t.Fatalf("dfxEvalUnhandledCyclesAssertion (post_operations): %v", err)
		}
		standaloneCycViol, err := dfxEvalUnhandledCyclesAssertion(cyclesRun.body, next, win)
		if err != nil {
			t.Fatalf("dfxEvalUnhandledCyclesAssertion (standalone): %v", err)
		}
		if !reflect.DeepEqual(cycViol, standaloneCycViol) {
			t.Fatalf("post_operations cycle assertion %v != standalone %v", cycViol, standaloneCycViol)
		}

		orphViol, err := dfxEvalOrphanTokensAssertion(postOrphansSQL, next, win)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion (post_operations): %v", err)
		}
		standaloneOrphViol, err := dfxEvalOrphanTokensAssertion(orphansRun.body, next, win)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion (standalone): %v", err)
		}
		if !reflect.DeepEqual(orphViol, standaloneOrphViol) {
			t.Fatalf("post_operations orphan assertion %v != standalone %v", orphViol, standaloneOrphViol)
		}

		violatingSpans := map[string]bool{}
		for spanID := range cycViol {
			violatingSpans[spanID] = true
		}
		for spanID := range orphViol {
			violatingSpans[spanID] = true
		}
		wantCommitted := len(violatingSpans) == 0
		if postCommitted != wantCommitted {
			t.Fatalf("evalTrajectoryPostOps committed = %v, want %v (violations=%v)", postCommitted, wantCommitted, violatingSpans)
		}
		committed = postCommitted

		stagedCnt, matCnt := 0, 0
		for _, r := range next {
			if !r.startTime.Before(contextStart) {
				stagedCnt++
				if r.inMart {
					matCnt++
				}
			}
		}
		return next, dagStepResult{
			windowStart:      win,
			stagedCount:      stagedCnt,
			materializedCnt:  matCnt,
			violatingSpanCnt: len(violatingSpans),
			committed:        committed,
			cycleViolations:  cycViol,
			orphanViolations: orphViol,
		}
	}

	// Timeline 1: Mart fails before post_operations at 12:00Z, recovers at 13:00Z, advances at 14:00Z.
	want0900 := time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)
	want1000 := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	want1100 := time.Date(2026, 10, 25, 11, 0, 0, 0, time.UTC)
	stagedAfterR1 := []dfxSpanRow{
		{traceID: "t_0900", serviceName: "svc", spanID: "s_0900", startTime: time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC), inMart: false, rawInput: dfxInt64(10)},
		{traceID: "t_0930", serviceName: "svc", spanID: "s_0930", startTime: time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC), inMart: false, rawInput: dfxInt64(20)},
		{traceID: "t_1015", serviceName: "svc", spanID: "s_1015", startTime: time.Date(2026, 10, 25, 10, 15, 0, 0, time.UTC), inMart: false, rawInput: dfxInt64(30)},
	}
	state1, res1 := runDAGStep(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC), true, nil, true, stagedAfterR1, true)
	if !res1.windowStart.Equal(want0900) || res1.committed {
		t.Fatalf("Timeline 1 Run 1 = (%v, %v), want (%v, false)", res1.windowStart, res1.committed, want0900)
	}

	// Verify that if [09:00Z, 10:00Z) spans were left unmaterialized at a 10:00Z cutoff, assertions report them.
	unrecovered := slices.Clone(stagedAfterR1)
	unrecovered[2].inMart = true
	unrecovered[2].isTokenLeaf = true
	unrecovered[2].inputTokens = dfxInt64(30)
	unrecViolations, err := dfxEvalOrphanTokensAssertion(orphansRun.body, unrecovered, want1000)
	if err != nil {
		t.Fatalf("dfxEvalOrphanTokensAssertion on unrecovered state: %v", err)
	}
	if !slices.Equal(unrecViolations["s_0900"], []string{"span_missing_from_trajectory"}) ||
		!slices.Equal(unrecViolations["s_0930"], []string{"span_missing_from_trajectory"}) {
		t.Errorf("expected unmaterialized [09:00Z, 10:00Z) spans to fail assert_no_orphan_tokens at 10:00Z cutoff, got %v", unrecViolations)
	}

	state2, res2 := runDAGStep(time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC), true, &res1.windowStart, res1.committed, state1, false)
	if !res2.windowStart.Equal(want0900) || !res2.committed || res2.stagedCount != 3 || res2.materializedCnt != 3 || res2.violatingSpanCnt != 0 {
		t.Fatalf("Timeline 1 Run 2 = %+v, want window=09:00Z committed=true 3/3/0", res2)
	}
	_, res3 := runDAGStep(time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC), true, &res2.windowStart, res2.committed, state2, false)
	if !res3.windowStart.Equal(want1100) || !res3.committed {
		t.Errorf("Timeline 1 Run 3 = (%v, %v), want (%v, true)", res3.windowStart, res3.committed, want1100)
	}

	// Timeline 2 (P1-1): staging-success / trajectory-success / assertion-failure /
	// next-invocation and >24h-persistence for two cycle spans (09:00Z, 09:30Z)
	// plus one isolated root (10:15Z).
	t.Run("P1-1 two-cycle-spans plus isolated root stays pinned across hourly and >24h retries until repaired", func(t *testing.T) {
		cycleSpans := []dfxSpanRow{
			{traceID: "t_cyc", serviceName: "svc", spanID: "s_cyc_0900", parentSpanID: "s_cyc_0930", startTime: time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)},
			{traceID: "t_cyc", serviceName: "svc", spanID: "s_cyc_0930", parentSpanID: "s_cyc_0900", startTime: time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC)},
			{traceID: "t_ok", serviceName: "svc", spanID: "s_ok_1015", parentSpanID: "", startTime: time.Date(2026, 10, 25, 10, 15, 0, 0, time.UTC), rawInput: dfxInt64(15)},
		}

		// Run at 2026-10-25 12:00Z: 3 staged / 1 materialized / 2 violations, is_committed = false.
		curState, step12 := runDAGStep(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC), true, nil, true, cycleSpans, false)
		if !step12.windowStart.Equal(want0900) || step12.committed || step12.stagedCount != 3 || step12.materializedCnt != 1 || step12.violatingSpanCnt != 2 {
			t.Fatalf("2026-10-25 12:00Z = %+v, want window=09:00Z committed=false 3 staged / 1 materialized / 2 violations", step12)
		}

		// Next invocation at 2026-10-25 13:00Z: still pinned at 09:00Z with 3 staged / 1 materialized / 2 violations.
		_, step13 := runDAGStep(time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC), true, &step12.windowStart, step12.committed, curState, false)
		if !step13.windowStart.Equal(want0900) || step13.committed || step13.stagedCount != 3 || step13.materializedCnt != 1 || step13.violatingSpanCnt != 2 {
			t.Fatalf("2026-10-25 13:00Z = %+v, want window=09:00Z committed=false 3 staged / 1 materialized / 2 violations", step13)
		}

		// Direct >24h retry at 2026-10-26 13:00Z: still pinned at 2026-10-25 09:00Z with 3 staged / 1 materialized / 2 violations.
		_, stepDirect24h := runDAGStep(time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC), true, &step12.windowStart, step12.committed, curState, false)
		if !stepDirect24h.windowStart.Equal(want0900) || stepDirect24h.committed || stepDirect24h.stagedCount != 3 || stepDirect24h.materializedCnt != 1 || stepDirect24h.violatingSpanCnt != 2 {
			t.Fatalf("direct >24h retry at 2026-10-26 13:00Z = %+v, want window=09:00Z committed=false 3 staged / 1 materialized / 2 violations", stepDirect24h)
		}

		// 24 consecutive hourly invocations through 2026-10-26 13:00Z: every hour remains pinned at 09:00Z with 3 staged / 1 materialized / 2 violations.
		prevWin, prevComm := step12.windowStart, step12.committed
		for h := 1; h <= 25; h++ {
			runNow := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC).Add(time.Duration(h) * time.Hour)
			var stepH dagStepResult
			curState, stepH = runDAGStep(runNow, true, &prevWin, prevComm, curState, false)
			if !stepH.windowStart.Equal(want0900) || stepH.committed || stepH.stagedCount != 3 || stepH.materializedCnt != 1 || stepH.violatingSpanCnt != 2 {
				t.Fatalf("hourly retry +%dh (%v) = %+v, want window=09:00Z committed=false 3/1/2", h, runNow, stepH)
			}
			prevWin, prevComm = stepH.windowStart, stepH.committed
		}

		// Upstream repair before 2026-10-26 14:00Z breaks the cycle (s_cyc_0900 becomes root):
		// 2026-10-26 14:00Z retains window_start = 2026-10-25 09:00Z, materializes all 3 spans (3/3/0), and commits.
		curState[0].parentSpanID = ""
		curState, stepRepair := runDAGStep(time.Date(2026, 10, 26, 14, 0, 0, 0, time.UTC), true, &prevWin, prevComm, curState, false)
		if !stepRepair.windowStart.Equal(want0900) || !stepRepair.committed || stepRepair.stagedCount != 3 || stepRepair.materializedCnt != 3 || stepRepair.violatingSpanCnt != 0 {
			t.Fatalf("repair run at 2026-10-26 14:00Z = %+v, want window=09:00Z committed=true 3/3/0", stepRepair)
		}

		// Next run at 2026-10-26 15:00Z advances window_start to 2026-10-26 12:00Z.
		wantNextDay1200 := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
		_, stepAdvance := runDAGStep(time.Date(2026, 10, 26, 15, 0, 0, 0, time.UTC), true, &stepRepair.windowStart, stepRepair.committed, curState, false)
		if !stepAdvance.windowStart.Equal(wantNextDay1200) || !stepAdvance.committed {
			t.Fatalf("advance run at 2026-10-26 15:00Z = (%v, %v), want (%v, true)", stepAdvance.windowStart, stepAdvance.committed, wantNextDay1200)
		}
	})

	// Timeline 3 (P1-1): single token-bearing parent-cycle span at 09:30Z stays
	// at 1 staged / 0 materialized / 1 violation across 2026-10-25 13:00Z and 2026-10-26 13:00Z.
	t.Run("P1-1 single token-bearing parent-cycle span stays at 1 staged / 0 materialized / 1 violation", func(t *testing.T) {
		selfCycle := []dfxSpanRow{
			{traceID: "t_self", serviceName: "svc", spanID: "s_self_0930", parentSpanID: "s_self_0930", startTime: time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC), rawInput: dfxInt64(42)},
		}
		curState, s12 := runDAGStep(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC), true, nil, true, selfCycle, false)
		if !s12.windowStart.Equal(want0900) || s12.committed || s12.stagedCount != 1 || s12.materializedCnt != 0 || s12.violatingSpanCnt != 1 ||
			!slices.Equal(s12.orphanViolations["s_self_0930"], []string{"span_missing_from_trajectory"}) {
			t.Fatalf("2026-10-25 12:00Z = %+v, want 1 staged / 0 materialized / 1 violation", s12)
		}
		curState, s13 := runDAGStep(time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC), true, &s12.windowStart, s12.committed, curState, false)
		if !s13.windowStart.Equal(want0900) || s13.committed || s13.stagedCount != 1 || s13.materializedCnt != 0 || s13.violatingSpanCnt != 1 {
			t.Fatalf("2026-10-25 13:00Z = %+v, want 1 staged / 0 materialized / 1 violation", s13)
		}
		_, sNextDay := runDAGStep(time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC), true, &s13.windowStart, s13.committed, curState, false)
		if !sNextDay.windowStart.Equal(want0900) || sNextDay.committed || sNextDay.stagedCount != 1 || sNextDay.materializedCnt != 0 || sNextDay.violatingSpanCnt != 1 {
			t.Fatalf("2026-10-26 13:00Z = %+v, want 1 staged / 0 materialized / 1 violation", sNextDay)
		}
	})

	// Timeline 4 (P1-2): failed full refresh followed by normal incremental retry
	// with staged rows older than 24 hours.
	t.Run("P1-2 failed full refresh pins epoch window_start so normal incremental retry recovers >24h rows", func(t *testing.T) {
		epoch := time.Unix(0, 0).UTC()
		backfillSpans := []dfxSpanRow{
			{traceID: "t_old", serviceName: "svc", spanID: "s_old_48h", parentSpanID: "", startTime: time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC), rawInput: dfxInt64(100)},
			{traceID: "t_recent", serviceName: "svc", spanID: "s_recent", parentSpanID: "", startTime: time.Date(2026, 10, 25, 10, 15, 0, 0, time.UTC), rawInput: dfxInt64(50)},
		}

		// Full refresh at 2026-10-25 12:00Z fails after stg_spans_dedup commits:
		// stg_run_window holds window_start = 1970-01-01 00:00:00Z, is_committed = FALSE.
		curState, fullStep := runDAGStep(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC), false, nil, true, backfillSpans, true)
		if !fullStep.windowStart.Equal(epoch) || fullStep.committed || fullStep.stagedCount != 2 || fullStep.materializedCnt != 0 {
			t.Fatalf("failed full refresh = %+v, want window=1970-01-01T00:00:00Z committed=false 2 staged / 0 materialized", fullStep)
		}

		// Subsequent normal incremental retry at 2026-10-25 13:00Z retains
		// window_start = 1970-01-01 00:00:00Z via LEAST(...) and materializes
		// both the >24h-old span (2026-10-23 09:00Z) and the recent span:
		// 2 staged / 2 materialized / 0 violations, is_committed = TRUE.
		curState, incRetry := runDAGStep(time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC), true, &fullStep.windowStart, fullStep.committed, curState, false)
		if !incRetry.windowStart.Equal(epoch) || !incRetry.committed || incRetry.stagedCount != 2 || incRetry.materializedCnt != 2 || incRetry.violatingSpanCnt != 0 {
			t.Fatalf("normal incremental retry after failed full refresh = %+v, want window=1970-01-01T00:00:00Z committed=true 2/2/0", incRetry)
		}

		// Next normal incremental run at 2026-10-25 14:00Z sees is_committed = TRUE
		// and advances window_start to 2026-10-25 11:00Z.
		_, nextInc := runDAGStep(time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC), true, &incRetry.windowStart, incRetry.committed, curState, false)
		if !nextInc.windowStart.Equal(want1100) || !nextInc.committed {
			t.Fatalf("next incremental run at 14:00Z = (%v, %v), want (%v, true)", nextInc.windowStart, nextInc.committed, want1100)
		}
	})

	// Timeline 5: post_operations statement-level validation for GoogleSQL
	// WITH RECURSIVE placement and orphan-token gate ordering before UPDATE.
	t.Run("post_operations temp table + ASSERT NOT EXISTS ordering and WITH RECURSIVE placement", func(t *testing.T) {
		cleanRow := []dfxSpanRow{
			{
				traceID: "t_clean", serviceName: "svc", spanID: "s_clean", parentSpanID: "",
				startTime: time.Date(2026, 10, 25, 10, 15, 0, 0, time.UTC),
				inMart:    true, isTokenLeaf: true, inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
			},
		}
		committed, cycViol, orphViol, err := evalTrajectoryPostOps(trajRun.post, cleanRow, want0900)
		if err != nil || !committed || len(cycViol) != 0 || len(orphViol) != 0 {
			t.Fatalf("clean post_operations = (committed=%v, cyc=%v, orph=%v, err=%v), want (true, empty, empty, nil)", committed, cycViol, orphViol, err)
		}

		// Emitted span whose rollup token was silently dropped passes cycle gate
		// but fails the orphan-token temp table + ASSERT gate before UPDATE.
		orphanDroppedRow := []dfxSpanRow{
			{
				traceID: "t_drop", serviceName: "svc", spanID: "s_drop", parentSpanID: "",
				startTime: time.Date(2026, 10, 25, 10, 15, 0, 0, time.UTC),
				inMart:    true, isTokenLeaf: false, inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
			},
		}
		committed, cycViol, orphViol, err = evalTrajectoryPostOps(trajRun.post, orphanDroppedRow, want0900)
		if err != nil || committed || len(cycViol) != 0 || !slices.Equal(orphViol["s_drop"], []string{"rollup_token_changed"}) {
			t.Fatalf("orphan-token violation post_operations = (committed=%v, cyc=%v, orph=%v, err=%v), want committed=false orph[s_drop]=[rollup_token_changed]", committed, cycViol, orphViol, err)
		}

		// Putting WITH RECURSIVE inside ASSERT NOT EXISTS (...) is rejected by the statement validator.
		badToks, err := dashTokenize("ASSERT NOT EXISTS ( WITH RECURSIVE r AS ( SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 2 ) SELECT 1 FROM r )")
		if err != nil {
			t.Fatalf("dashTokenize: %v", err)
		}
		badStmts := slices.Clone(trajRun.post)
		badStmts[3] = dfxStmt{kind: "assert no_orphan_tokens", toks: badToks}
		if _, _, _, err := evalTrajectoryPostOps(badStmts, cleanRow, want0900); err == nil || !strings.Contains(err.Error(), "WITH RECURSIVE is not allowed inside a parenthesized subquery or ASSERT NOT EXISTS") {
			t.Fatalf("expected WITH RECURSIVE inside ASSERT NOT EXISTS to fail statement validator, got %v", err)
		}

		// Reordering UPDATE before ASSERT NOT EXISTS is rejected.
		reordered := []dfxStmt{trajRun.post[0], trajRun.post[1], trajRun.post[4], trajRun.post[2], trajRun.post[3]}
		if _, _, _, err := evalTrajectoryPostOps(reordered, cleanRow, want0900); err == nil {
			t.Fatal("expected UPDATE before assert no_orphan_tokens to fail")
		}
	})
}

// dfxSpanRow models a staged/emitted trajectory span row for testing the
// relational semantics of assert_no_orphan_tokens.sqlx.
type dfxSpanRow struct {
	traceID         string
	serviceName     string
	spanID          string
	parentSpanID    string
	startTime       time.Time
	inMart          bool
	isTokenLeaf     bool
	inputTokens     *int64
	outputTokens    *int64
	reasoningTokens *int64
	rawInput        *int64
	rawOutput       *int64
	rawReasoning    *int64
}

func dfxInt64(v int64) *int64 { return &v }

// dfxEvalBoolToks evaluates a boolean SQL expression from assert_no_orphan_tokens.sqlx
// (staged, trajectory_context, emitted_ancestors, token_parent_coverage, emitted,
// row_violations, double_counted, missing) directly from its parsed dashTok slice
// against the given identifier environment.
func dfxEvalBoolToks(toks []dashTok, env map[string]any) (bool, error) {
	pos := 0
	var parseOr func() (bool, error)
	var parseAnd func() (bool, error)
	var parseNot func() (bool, error)
	var parsePrimary func() (bool, error)
	var parseVal func() (any, error)

	findMatchingClose := func(openIdx int) (int, error) {
		if closeIdx := dashClosing(toks, openIdx); closeIdx >= 0 {
			return closeIdx, nil
		}
		return -1, fmt.Errorf("unclosed '(' at token %d", openIdx)
	}

	isNilVal := func(v any) bool {
		if v == nil {
			return true
		}
		if p, ok := v.(*int64); ok {
			return p == nil
		}
		return false
	}

	asInt64 := func(v any) (int64, bool) {
		switch x := v.(type) {
		case int:
			return int64(x), true
		case int64:
			return x, true
		case *int64:
			if x != nil {
				return *x, true
			}
		}
		return 0, false
	}

	equalVal := func(a, b any) bool {
		if isNilVal(a) || isNilVal(b) {
			return isNilVal(a) && isNilVal(b)
		}
		if ia, ok := asInt64(a); ok {
			if ib, ok := asInt64(b); ok {
				return ia == ib
			}
		}
		if ta, ok := a.(time.Time); ok {
			if tb, ok := b.(time.Time); ok {
				return ta.Equal(tb)
			}
		}
		if ba, ok := a.(bool); ok {
			if bb, ok := b.(bool); ok {
				return ba == bb
			}
		}
		return reflect.DeepEqual(a, b)
	}

	compareVal := func(a, b any, op string) (bool, error) {
		if isNilVal(a) || isNilVal(b) {
			return false, nil
		}
		if op == "=" {
			return equalVal(a, b), nil
		}
		if op == "!=" || op == "<>" {
			return !equalVal(a, b), nil
		}
		if ia, ok := asInt64(a); ok {
			if ib, ok := asInt64(b); ok {
				switch op {
				case "<":
					return ia < ib, nil
				case "<=":
					return ia <= ib, nil
				case ">":
					return ia > ib, nil
				case ">=":
					return ia >= ib, nil
				}
			}
		}
		if ta, ok := a.(time.Time); ok {
			if tb, ok := b.(time.Time); ok {
				switch op {
				case "<":
					return ta.Before(tb), nil
				case "<=":
					return !ta.After(tb), nil
				case ">":
					return ta.After(tb), nil
				case ">=":
					return !ta.Before(tb), nil
				}
			}
		}
		if sa, ok := a.(string); ok {
			if sb, ok := b.(string); ok {
				switch op {
				case "<":
					return sa < sb, nil
				case "<=":
					return sa <= sb, nil
				case ">":
					return sa > sb, nil
				case ">=":
					return sa >= sb, nil
				}
			}
		}
		return false, fmt.Errorf("unsupported comparison %T %s %T", a, op, b)
	}

	parseVal = func() (any, error) {
		if pos >= len(toks) {
			return nil, errors.New("unexpected end of expression")
		}
		t := toks[pos]
		if t.is("(") {
			closeIdx, err := findMatchingClose(pos)
			if err != nil {
				return nil, err
			}
			inner := toks[pos+1 : closeIdx]
			if len(inner) > 0 && inner[0].up == "SELECT" {
				if v, ok := env[dashJoin(toks[pos:closeIdx+1])]; ok {
					pos = closeIdx + 1
					return v, nil
				}
			}
			pos = closeIdx + 1
			return dfxEvalBoolToks(inner, env)
		}
		if t.up == "TRUE" {
			pos++
			return true, nil
		}
		if t.up == "FALSE" {
			pos++
			return false, nil
		}
		if t.up == "NULL" {
			pos++
			return (*int64)(nil), nil
		}
		if t.kind == dashNumber {
			n, err := strconv.ParseInt(t.text, 10, 64)
			if err != nil {
				return nil, err
			}
			pos++
			return n, nil
		}
		if t.kind == dashString {
			pos++
			if len(t.text) >= 2 && (t.text[0] == '\'' || t.text[0] == '"') {
				return t.text[1 : len(t.text)-1], nil
			}
			return t.text, nil
		}
		if t.up == "EXISTS" && pos+1 < len(toks) && toks[pos+1].is("(") {
			closeIdx, err := findMatchingClose(pos + 1)
			if err != nil {
				return nil, err
			}
			subToks := toks[pos+2 : closeIdx]
			pos = closeIdx + 1
			subQ, err := dashParseSQL(dashJoin(subToks))
			if err != nil {
				return nil, fmt.Errorf("invalid EXISTS subquery: %w", err)
			}
			if len(subQ.branches) != 1 {
				return nil, errors.New("invalid EXISTS subquery: want 1 branch")
			}
			sb := subQ.branches[0]
			fromToks := sb.clauses["FROM"]
			if len(fromToks) < 3 || fromToks[len(fromToks)-2].up != "AS" {
				return nil, fmt.Errorf("EXISTS subquery FROM %q must end with AS <alias>", dashJoin(fromToks))
			}
			alias := strings.ToLower(fromToks[len(fromToks)-1].text)
			subWhere := sb.clauses["WHERE"]
			rowsVal, ok := env["__mart_rows__"]
			if !ok {
				return nil, errors.New("missing __mart_rows__ in environment for EXISTS subquery")
			}
			martRows, ok := rowsVal.([]dfxSpanRow)
			if !ok {
				return nil, errors.New("__mart_rows__ has unexpected type")
			}
			for _, mr := range martRows {
				subEnv := maps.Clone(env)
				subEnv[alias+".trace_id"] = mr.traceID
				subEnv[alias+".service_name"] = mr.serviceName
				subEnv[alias+".span_id"] = mr.spanID
				subEnv[alias+".parent_span_id"] = mr.parentSpanID
				subEnv[alias+".start_time"] = mr.startTime
				subEnv[alias+".is_token_leaf"] = mr.isTokenLeaf
				subEnv[alias+".input_tokens"] = mr.inputTokens
				subEnv[alias+".output_tokens"] = mr.outputTokens
				subEnv[alias+".reasoning_tokens"] = mr.reasoningTokens
				subEnv[alias+".raw_input_tokens"] = mr.rawInput
				subEnv[alias+".raw_output_tokens"] = mr.rawOutput
				subEnv[alias+".raw_reasoning_tokens"] = mr.rawReasoning
				matched, err := dfxEvalBoolToks(subWhere, subEnv)
				if err != nil {
					return nil, err
				}
				if matched {
					return true, nil
				}
			}
			return false, nil
		}
		if t.up == "TIMESTAMP_SUB" && pos+1 < len(toks) && toks[pos+1].is("(") {
			closeIdx, err := findMatchingClose(pos + 1)
			if err != nil {
				return nil, err
			}
			argDepth := toks[pos+1].depth + 1
			commaIdx := -1
			for i := pos + 2; i < closeIdx; i++ {
				if toks[i].depth == argDepth && toks[i].is(",") {
					commaIdx = i
					break
				}
			}
			if commaIdx < 0 || commaIdx+4 != closeIdx || toks[commaIdx+1].up != "INTERVAL" || toks[commaIdx+3].up != "HOUR" {
				return nil, fmt.Errorf("unsupported TIMESTAMP_SUB call %q", dashJoin(toks[pos:closeIdx+1]))
			}
			hrs, err := strconv.Atoi(toks[commaIdx+2].text)
			if err != nil {
				return nil, err
			}
			baseToks := toks[pos+2 : commaIdx]
			var baseTime time.Time
			if v, ok := env[dashJoin(baseToks)]; ok {
				if bt, ok := v.(time.Time); ok {
					baseTime = bt
				}
			} else if len(baseToks) == 1 {
				if v, ok := env[strings.ToLower(baseToks[0].text)]; ok {
					if bt, ok := v.(time.Time); ok {
						baseTime = bt
					}
				}
			}
			if baseTime.IsZero() {
				return nil, fmt.Errorf("unknown base timestamp %q in TIMESTAMP_SUB", dashJoin(baseToks))
			}
			pos = closeIdx + 1
			return baseTime.Add(-time.Duration(hrs) * time.Hour), nil
		}
		if t.up == "COALESCE" && pos+1 < len(toks) && toks[pos+1].is("(") {
			closeIdx, err := findMatchingClose(pos + 1)
			if err != nil {
				return nil, err
			}
			argDepth := toks[pos+1].depth + 1
			argStart := pos + 2
			var result any
			foundNonNil := false
			for i := pos + 2; i <= closeIdx; i++ {
				if i == closeIdx || (toks[i].depth == argDepth && toks[i].is(",")) {
					subPos := pos
					toksSlice := toks[argStart:i]
					pos = 0
					savedToks := toks
					toks = toksSlice
					val, err := parseVal()
					if err == nil && pos != len(toks) {
						pos = 0
						val, err = parseOr()
					}
					toks = savedToks
					pos = subPos
					if err != nil {
						return nil, err
					}
					if !foundNonNil && !isNilVal(val) {
						result = val
						foundNonNil = true
					}
					argStart = i + 1
				}
			}
			pos = closeIdx + 1
			return result, nil
		}
		if t.kind == dashWord || t.kind == dashIdent {
			key := strings.ToLower(t.text)
			pos++
			if pos+1 < len(toks) && toks[pos].is(".") && (toks[pos+1].kind == dashWord || toks[pos+1].kind == dashIdent) {
				key = key + "." + strings.ToLower(toks[pos+1].text)
				pos += 2
			}
			val, ok := env[key]
			if !ok {
				return nil, fmt.Errorf("unknown identifier %q in AST evaluator", key)
			}
			return val, nil
		}
		return nil, fmt.Errorf("unexpected token %q", t.text)
	}

	parsePrimary = func() (bool, error) {
		val, err := parseVal()
		if err != nil {
			return false, err
		}
		if pos < len(toks) && (toks[pos].up == "NOT" || toks[pos].up == "IN") {
			notIn := false
			savePos := pos
			if toks[pos].up == "NOT" {
				notIn = true
				pos++
			}
			if pos+2 < len(toks) && toks[pos].up == "IN" && toks[pos+1].up == "UNNEST" && toks[pos+2].is("(") {
				closeIdx, err := findMatchingClose(pos + 2)
				if err != nil {
					return false, err
				}
				arrKey := strings.ToLower(strings.ReplaceAll(dashJoin(toks[pos+3:closeIdx]), " ", ""))
				pos = closeIdx + 1
				arrVal, ok := env[arrKey]
				if !ok {
					return false, fmt.Errorf("unknown array identifier %q in UNNEST", arrKey)
				}
				strSlice, ok := arrVal.([]string)
				if !ok {
					return false, fmt.Errorf("UNNEST(%s) is %T, want []string", arrKey, arrVal)
				}
				needle, ok := val.(string)
				if !ok {
					return false, fmt.Errorf("LHS of IN UNNEST is %T, want string", val)
				}
				found := slices.Contains(strSlice, needle)
				if notIn {
					return !found, nil
				}
				return found, nil
			}
			pos = savePos
		}
		if pos < len(toks) && toks[pos].up == "IS" {
			pos++
			notFlag := false
			if pos < len(toks) && toks[pos].up == "NOT" {
				notFlag = true
				pos++
			}
			if pos < len(toks) && toks[pos].up == "NULL" {
				pos++
				if notFlag {
					return !isNilVal(val), nil
				}
				return isNilVal(val), nil
			}
			if pos+1 < len(toks) && toks[pos].up == "DISTINCT" && toks[pos+1].up == "FROM" {
				pos += 2
				rhs, err := parseVal()
				if err != nil {
					return false, err
				}
				eq := equalVal(val, rhs)
				if notFlag {
					return eq, nil
				}
				return !eq, nil
			}
			return false, fmt.Errorf("unsupported IS predicate at token %d", pos)
		}
		if pos < len(toks) {
			op := toks[pos].text
			if op == "=" || op == "!=" || op == "<>" || op == "<" || op == "<=" || op == ">" || op == ">=" {
				pos++
				rhs, err := parseVal()
				if err != nil {
					return false, err
				}
				return compareVal(val, rhs, op)
			}
		}
		b, ok := val.(bool)
		if !ok {
			return false, fmt.Errorf("expected boolean value, got %T (%v)", val, val)
		}
		return b, nil
	}

	parseNot = func() (bool, error) {
		if pos < len(toks) && toks[pos].up == "NOT" {
			pos++
			v, err := parseNot()
			return !v, err
		}
		return parsePrimary()
	}

	parseAnd = func() (bool, error) {
		lhs, err := parseNot()
		if err != nil {
			return false, err
		}
		for pos < len(toks) && toks[pos].up == "AND" {
			pos++
			rhs, err := parseNot()
			if err != nil {
				return false, err
			}
			lhs = lhs && rhs
		}
		return lhs, nil
	}

	parseOr = func() (bool, error) {
		lhs, err := parseAnd()
		if err != nil {
			return false, err
		}
		for pos < len(toks) && toks[pos].up == "OR" {
			pos++
			rhs, err := parseAnd()
			if err != nil {
				return false, err
			}
			lhs = lhs || rhs
		}
		return lhs, nil
	}

	res, err := parseOr()
	if err != nil {
		return false, err
	}
	if pos != len(toks) {
		return false, fmt.Errorf("trailing token %q at %d", toks[pos].text, pos)
	}
	return res, nil
}

// dfxParseCaseBranches extracts the WHEN <cond> THEN '<literal>' branches of a
// CASE ... END expression.
type dfxCaseWhen struct {
	cond      []dashTok
	violation string
}

func dfxParseCaseBranches(toks []dashTok) ([]dfxCaseWhen, error) {
	if len(toks) < 2 || toks[0].up != "CASE" || toks[len(toks)-1].up != "END" {
		return nil, errors.New("expected CASE ... END expression")
	}
	base := toks[0].depth
	var branches []dfxCaseWhen
	i := 1
	for i < len(toks)-1 {
		if toks[i].depth == base && toks[i].up == "ELSE" {
			break
		}
		if toks[i].depth != base || toks[i].up != "WHEN" {
			return nil, fmt.Errorf("expected WHEN at token %d (%q)", i, toks[i].text)
		}
		thenIdx := -1
		for j := i + 1; j < len(toks)-1; j++ {
			if toks[j].depth == base && toks[j].up == "THEN" {
				thenIdx = j
				break
			}
		}
		if thenIdx < 0 || thenIdx+1 >= len(toks)-1 {
			return nil, errors.New("missing THEN in CASE expression")
		}
		litTok := toks[thenIdx+1]
		m := dfxViolationRE.FindStringSubmatch(litTok.text)
		if m == nil {
			return nil, fmt.Errorf("expected violation string literal after THEN, got %q", litTok.text)
		}
		branches = append(branches, dfxCaseWhen{
			cond:      toks[i+1 : thenIdx],
			violation: m[1],
		})
		i = thenIdx + 2
	}
	if len(branches) == 0 {
		return nil, errors.New("CASE expression has no WHEN branches")
	}
	return branches, nil
}

var dfxHopLimitRE = regexp.MustCompile(`\bEA \. HOP < (\d+)\b`)

// dfxEvalUnhandledCyclesAssertion parses sqlText (the rendered SQL of
// assert_no_unhandled_cycles.sqlx or the first ASSERT NOT EXISTS query in
// m_gen_ai_agent_trajectory's post_operations) and evaluates it against rows
// in the 24-hour context window anchored at windowStart.
func dfxEvalUnhandledCyclesAssertion(sqlText string, rows []dfxSpanRow, windowStart time.Time) (map[string][]string, error) {
	q, err := dashParseSQL(sqlText)
	if err != nil {
		return nil, fmt.Errorf("parsing unhandled cycles assertion SQL: %w", err)
	}
	if dfxCTE(q, "staged") == nil || dfxCTE(q, "emitted") == nil || len(q.branches) != 1 {
		return nil, errors.New("expected staged and emitted CTEs and 1 final SELECT branch")
	}
	b := q.branches[0]
	if gotWhere := dashJoin(b.clauses["WHERE"]); gotWhere != "EMITTED . EMITTED_ROWS IS NULL OR EMITTED . EMITTED_ROWS != 1" {
		return nil, fmt.Errorf("unexpected WHERE clause %q", gotWhere)
	}
	missingViolation := "span_missing_from_trajectory"
	duplicateViolation := "span_in_trajectory_more_than_once"
	for _, it := range b.items {
		if strings.EqualFold(it.name, "violation") {
			lits := dfxViolations(it.body)
			if len(lits) != 2 {
				return nil, fmt.Errorf("expected 2 violation literals in IF(...), got %v", lits)
			}
		}
	}
	contextStart := windowStart.Add(-24 * time.Hour)
	type spanKey struct{ traceID, serviceName, spanID string }
	var stagedKeys []spanKey
	seenStaged := map[spanKey]bool{}
	emittedCounts := map[spanKey]int{}
	for _, r := range rows {
		if r.startTime.Before(contextStart) {
			continue
		}
		k := spanKey{r.traceID, r.serviceName, r.spanID}
		if !seenStaged[k] {
			seenStaged[k] = true
			stagedKeys = append(stagedKeys, k)
		}
		if r.inMart {
			emittedCounts[k]++
		}
	}
	violations := map[string][]string{}
	for _, k := range stagedKeys {
		switch cnt := emittedCounts[k]; {
		case cnt == 0:
			violations[k.spanID] = append(violations[k.spanID], missingViolation)
		case cnt != 1:
			violations[k.spanID] = append(violations[k.spanID], duplicateViolation)
		}
	}
	return violations, nil
}

// dfxExtractJoinOn extracts the ON <expr> tokens of a JOIN or LEFT JOIN from a
// FROM clause token slice.
func dfxExtractJoinOn(fromToks []dashTok) ([]dashTok, error) {
	if len(fromToks) == 0 {
		return nil, errors.New("empty FROM clause")
	}
	base := fromToks[0].depth
	for i, t := range fromToks {
		if t.depth == base && t.up == "ON" && i+1 < len(fromToks) {
			return fromToks[i+1:], nil
		}
	}
	return nil, fmt.Errorf("FROM clause %q has no top-level ON predicate", dashJoin(fromToks))
}

// dfxEvalStringArrayToks evaluates a targeted GoogleSQL string-array expression
// subset: array literals ([a, b, ...]), ARRAY_CONCAT(arr1, arr2), and
// environment array identifiers (such as ea.path_span_ids).
func dfxEvalStringArrayToks(toks []dashTok, env map[string]any) ([]string, error) {
	if len(toks) == 0 {
		return nil, errors.New("empty string-array expression")
	}
	// Array literal: [ expr, expr, ... ]
	if toks[0].is("[") && toks[len(toks)-1].is("]") {
		inner := toks[1 : len(toks)-1]
		if len(inner) == 0 {
			return nil, nil
		}
		var out []string
		start := 0
		bracketDepth := 0
		for i, t := range inner {
			switch t.text {
			case "[", "(":
				bracketDepth++
			case "]", ")":
				bracketDepth--
			case ",":
				if bracketDepth == 0 {
					elem := strings.ToLower(strings.ReplaceAll(dashJoin(inner[start:i]), " ", ""))
					v, ok := env[elem]
					if !ok {
						return nil, fmt.Errorf("unknown array element identifier %q", elem)
					}
					s, ok := v.(string)
					if !ok {
						return nil, fmt.Errorf("array element %q is %T, want string", elem, v)
					}
					out = append(out, s)
					start = i + 1
				}
			}
		}
		elem := strings.ToLower(strings.ReplaceAll(dashJoin(inner[start:]), " ", ""))
		v, ok := env[elem]
		if !ok {
			return nil, fmt.Errorf("unknown array element identifier %q", elem)
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("array element %q is %T, want string", elem, v)
		}
		out = append(out, s)
		return out, nil
	}
	// ARRAY_CONCAT(arg1, arg2, ...)
	if len(toks) >= 4 && toks[0].up == "ARRAY_CONCAT" && toks[1].is("(") && toks[len(toks)-1].is(")") {
		inner := toks[2 : len(toks)-1]
		var out []string
		start := 0
		bracketDepth := 0
		for i, t := range inner {
			switch t.text {
			case "[", "(":
				bracketDepth++
			case "]", ")":
				bracketDepth--
			case ",":
				if bracketDepth == 0 {
					part, err := dfxEvalStringArrayToks(inner[start:i], env)
					if err != nil {
						return nil, err
					}
					out = append(out, part...)
					start = i + 1
				}
			}
		}
		part, err := dfxEvalStringArrayToks(inner[start:], env)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
		return out, nil
	}
	// Qualified identifier: e.g. ea.path_span_ids
	key := strings.ToLower(strings.ReplaceAll(dashJoin(toks), " ", ""))
	v, ok := env[key]
	if !ok {
		return nil, fmt.Errorf("unsupported string-array expression %q", dashJoin(toks))
	}
	arr, ok := v.([]string)
	if !ok {
		return nil, fmt.Errorf("identifier %q is %T, want []string", key, v)
	}
	return slices.Clone(arr), nil
}

// dfxEvalOrphanTokensAssertion parses sqlText (the rendered SQL of
// assert_no_orphan_tokens.sqlx or the temp table query in
// m_gen_ai_agent_trajectory's post_operations) and evaluates its 32-hop
// recursive emitted_ancestors -> token_parent_coverage -> emitted ->
// (row_violations UNION ALL double_counted UNION ALL missing) AST on a set of
// spans. Because the evaluator executes the parsed SQL AST of staged,
// trajectory_context, emitted_ancestors (including JOIN ON, WHERE, and
// path_span_ids cycle guard), token_parent_coverage, emitted, row_violations,
// double_counted, missing (including the NOT EXISTS subquery), and the final
// UNION ALL branches, mutating any of those SQL expressions changes the
// evaluated result.
func dfxEvalOrphanTokensAssertion(sqlText string, rows []dfxSpanRow, windowStart time.Time) (map[string][]string, error) {
	q, err := dashParseSQL(sqlText)
	if err != nil {
		return nil, fmt.Errorf("parsing assertion SQL: %w", err)
	}

	contextStart := windowStart.Add(-24 * time.Hour)
	baseEnv := map[string]any{
		"window_start":                       windowStart,
		"context_start":                      contextStart,
		dfxSharedWindowExpr:                  windowStart,
		strings.ToUpper(dfxSharedWindowExpr): windowStart,
		"( SELECT RW . WINDOW_START FROM REF . STG_RUN_WINDOW AS RW )": windowStart,
	}
	bindRow := func(env map[string]any, prefix string, r dfxSpanRow) {
		env[prefix+"trace_id"] = r.traceID
		env[prefix+"service_name"] = r.serviceName
		env[prefix+"span_id"] = r.spanID
		env[prefix+"parent_span_id"] = r.parentSpanID
		env[prefix+"start_time"] = r.startTime
		env[prefix+"is_token_leaf"] = r.isTokenLeaf
		env[prefix+"input_tokens"] = r.inputTokens
		env[prefix+"output_tokens"] = r.outputTokens
		env[prefix+"reasoning_tokens"] = r.reasoningTokens
		env[prefix+"raw_input_tokens"] = r.rawInput
		env[prefix+"raw_output_tokens"] = r.rawOutput
		env[prefix+"raw_reasoning_tokens"] = r.rawReasoning
	}

	// Parse staged CTE and evaluate its WHERE predicate on rows.
	stagedCTE := dfxCTE(q, "staged")
	if stagedCTE == nil || len(stagedCTE.branches) != 1 {
		return nil, errors.New("missing staged CTE")
	}
	stagedWhere := stagedCTE.branches[0].clauses["WHERE"]
	if len(stagedWhere) == 0 {
		return nil, errors.New("staged CTE missing WHERE clause")
	}
	var staged []dfxSpanRow
	for _, r := range rows {
		env := maps.Clone(baseEnv)
		bindRow(env, "", r)
		bindRow(env, "s.", r)
		keep, err := dfxEvalBoolToks(stagedWhere, env)
		if err != nil {
			return nil, fmt.Errorf("evaluating staged WHERE: %w", err)
		}
		if keep {
			staged = append(staged, r)
		}
	}

	// Parse trajectory_context CTE and evaluate its JOIN ON and WHERE predicates.
	tcCTE := dfxCTE(q, "trajectory_context")
	if tcCTE == nil || len(tcCTE.branches) != 1 {
		return nil, errors.New("missing trajectory_context CTE")
	}
	tcJoinOn, err := dfxExtractJoinOn(tcCTE.branches[0].clauses["FROM"])
	if err != nil {
		return nil, fmt.Errorf("trajectory_context FROM: %w", err)
	}
	tcWhere := tcCTE.branches[0].clauses["WHERE"]
	if len(tcWhere) == 0 {
		return nil, errors.New("trajectory_context missing WHERE clause")
	}

	var martRows []dfxSpanRow
	for _, r := range rows {
		if r.inMart {
			martRows = append(martRows, r)
		}
	}

	var context []dfxSpanRow
	for _, m := range martRows {
		for _, s := range staged {
			jEnv := maps.Clone(baseEnv)
			bindRow(jEnv, "m.", m)
			bindRow(jEnv, "s.", s)
			jKeep, err := dfxEvalBoolToks(tcJoinOn, jEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating trajectory_context JOIN ON: %w", err)
			}
			if !jKeep {
				continue
			}
			wKeep, err := dfxEvalBoolToks(tcWhere, jEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating trajectory_context WHERE: %w", err)
			}
			if wKeep {
				merged := m
				merged.rawInput = s.rawInput
				merged.rawOutput = s.rawOutput
				merged.rawReasoning = s.rawReasoning
				context = append(context, merged)
			}
		}
	}

	// Parse emitted_ancestors seed and recursive branches from the SQL AST.
	eaCTE := dfxCTE(q, "emitted_ancestors")
	if eaCTE == nil || len(eaCTE.branches) != 2 {
		return nil, errors.New("missing emitted_ancestors recursive CTE with 2 branches")
	}
	eaSeedJoinOn, err := dfxExtractJoinOn(eaCTE.branches[0].clauses["FROM"])
	if err != nil {
		return nil, fmt.Errorf("emitted_ancestors seed FROM: %w", err)
	}
	eaSeedWhere := eaCTE.branches[0].clauses["WHERE"]
	if len(eaSeedWhere) == 0 {
		return nil, errors.New("emitted_ancestors seed missing WHERE clause")
	}
	eaRecJoinOn, err := dfxExtractJoinOn(eaCTE.branches[1].clauses["FROM"])
	if err != nil {
		return nil, fmt.Errorf("emitted_ancestors recursive FROM: %w", err)
	}
	eaRecWhere := eaCTE.branches[1].clauses["WHERE"]
	if len(eaRecWhere) == 0 {
		return nil, errors.New("emitted_ancestors recursive missing WHERE clause")
	}
	eaSeedExprs := map[string][]dashTok{}
	for _, it := range eaCTE.branches[0].items {
		eaSeedExprs[strings.ToLower(it.name)] = it.body
	}
	eaRecExprs := map[string][]dashTok{}
	for _, it := range eaCTE.branches[1].items {
		eaRecExprs[strings.ToLower(it.name)] = it.body
	}
	for _, need := range []string{"has_input", "has_output", "has_reasoning", "path_span_ids"} {
		if len(eaSeedExprs[need]) == 0 {
			return nil, fmt.Errorf("emitted_ancestors seed branch missing item %s", need)
		}
		if len(eaRecExprs[need]) == 0 {
			return nil, fmt.Errorf("emitted_ancestors recursive branch missing item %s", need)
		}
	}
	hopMatch := dfxHopLimitRE.FindStringSubmatch(dashJoin(eaRecWhere))
	if hopMatch == nil {
		return nil, fmt.Errorf("emitted_ancestors recursive WHERE %q missing EA . HOP < N", dashJoin(eaRecWhere))
	}
	maxHops, err := strconv.Atoi(hopMatch[1])
	if err != nil {
		return nil, err
	}
	// Verify that seed/recursive path_span_ids expressions and the recursive WHERE
	// predicate evaluate to [c.span_id, p.span_id] and ARRAY_CONCAT(ea.path_span_ids, [p.span_id])
	// and reject extending a path onto any span_id already present in ea.path_span_ids.
	seedProbePath, err := dfxEvalStringArrayToks(eaSeedExprs["path_span_ids"], map[string]any{
		"c.span_id": "probe_desc",
		"p.span_id": "probe_anc",
	})
	if err != nil {
		return nil, fmt.Errorf("evaluating emitted_ancestors seed path_span_ids: %w", err)
	}
	if !slices.Equal(seedProbePath, []string{"probe_desc", "probe_anc"}) {
		return nil, fmt.Errorf("emitted_ancestors seed path_span_ids evaluated to %v, want [probe_desc probe_anc]", seedProbePath)
	}
	recProbePath, err := dfxEvalStringArrayToks(eaRecExprs["path_span_ids"], map[string]any{
		"ea.path_span_ids": seedProbePath,
		"p.span_id":        "probe_gp",
	})
	if err != nil {
		return nil, fmt.Errorf("evaluating emitted_ancestors recursive path_span_ids: %w", err)
	}
	if !slices.Equal(recProbePath, []string{"probe_desc", "probe_anc", "probe_gp"}) {
		return nil, fmt.Errorf("emitted_ancestors recursive path_span_ids evaluated to %v, want [probe_desc probe_anc probe_gp]", recProbePath)
	}
	for _, cycleSpan := range []string{"probe_desc", "probe_anc", "probe_gp"} {
		cycleProbeEnv := map[string]any{
			"ea.hop":           int64(2),
			"ea.path_span_ids": recProbePath,
			"p.span_id":        cycleSpan,
		}
		if cycleProbeAllowed, err := dfxEvalBoolToks(eaRecWhere, cycleProbeEnv); err != nil {
			return nil, fmt.Errorf("evaluating emitted_ancestors recursive WHERE on cycle probe: %w", err)
		} else if cycleProbeAllowed {
			return nil, fmt.Errorf("emitted_ancestors recursive WHERE %q does not reject p.span_id %q already in ea.path_span_ids %v", dashJoin(eaRecWhere), cycleSpan, recProbePath)
		}
	}

	// Parse token_parent_coverage LOGICAL_OR(ea.<col>) mappings from the SQL AST.
	covCTE := dfxCTE(q, "token_parent_coverage")
	if covCTE == nil || len(covCTE.branches) != 1 {
		return nil, errors.New("missing token_parent_coverage CTE")
	}
	covSourceField := map[string]string{}
	for _, it := range covCTE.branches[0].items {
		joined := dashJoin(it.body)
		for _, srcField := range []string{"HAS_INPUT", "HAS_OUTPUT", "HAS_REASONING"} {
			if joined == "LOGICAL_OR ( EA . "+srcField+" )" {
				covSourceField[strings.ToLower(it.name)] = strings.ToLower(srcField)
			}
		}
	}
	for _, need := range []string{"has_input", "has_output", "has_reasoning"} {
		if covSourceField[need] == "" {
			return nil, fmt.Errorf("token_parent_coverage missing LOGICAL_OR mapping for %s", need)
		}
	}

	// Parse emitted CTE items (cov_input, cov_output, cov_reasoning, expected_is_token_leaf).
	emittedCTE := dfxCTE(q, "emitted")
	if emittedCTE == nil || len(emittedCTE.branches) != 1 {
		return nil, errors.New("missing emitted CTE")
	}
	emittedExprs := map[string][]dashTok{}
	for _, it := range emittedCTE.branches[0].items {
		emittedExprs[strings.ToLower(it.name)] = it.body
	}
	for _, need := range []string{"cov_input", "cov_output", "cov_reasoning", "expected_is_token_leaf"} {
		if len(emittedExprs[need]) == 0 {
			return nil, fmt.Errorf("emitted CTE missing item %s", need)
		}
	}

	// Parse row_violations CASE branches from the SQL AST.
	rvCTE := dfxCTE(q, "row_violations")
	if rvCTE == nil || len(rvCTE.branches) != 1 {
		return nil, errors.New("missing row_violations CTE")
	}
	var rvCaseToks []dashTok
	for _, it := range rvCTE.branches[0].items {
		if strings.EqualFold(it.name, "violation") {
			rvCaseToks = it.body
			break
		}
	}
	rvBranches, err := dfxParseCaseBranches(rvCaseToks)
	if err != nil {
		return nil, fmt.Errorf("row_violations CASE: %w", err)
	}

	// Parse double_counted WHERE clause and violation string literal from the SQL AST.
	dcCTE := dfxCTE(q, "double_counted")
	if dcCTE == nil || len(dcCTE.branches) != 1 {
		return nil, errors.New("missing double_counted CTE")
	}
	dcWhere := dcCTE.branches[0].clauses["WHERE"]
	if len(dcWhere) == 0 {
		return nil, errors.New("double_counted missing WHERE clause")
	}
	var dcViolation string
	for _, it := range dcCTE.branches[0].items {
		if strings.EqualFold(it.name, "violation") && len(it.body) == 1 {
			if m := dfxViolationRE.FindStringSubmatch(it.body[0].text); m != nil {
				dcViolation = m[1]
			}
		}
	}
	if dcViolation == "" {
		return nil, errors.New("double_counted missing violation string literal")
	}

	// Parse missing CTE WHERE clause and violation string literal from the SQL AST.
	missingCTE := dfxCTE(q, "missing")
	if missingCTE == nil || len(missingCTE.branches) != 1 {
		return nil, errors.New("missing 'missing' CTE")
	}
	var missingViolation string
	for _, it := range missingCTE.branches[0].items {
		if strings.EqualFold(it.name, "violation") && len(it.body) == 1 {
			if m := dfxViolationRE.FindStringSubmatch(it.body[0].text); m != nil {
				missingViolation = m[1]
			}
		}
	}
	if missingViolation == "" {
		return nil, errors.New("missing CTE has no violation string literal")
	}
	missingWhere := missingCTE.branches[0].clauses["WHERE"]
	if len(missingWhere) == 0 {
		return nil, errors.New("missing CTE has no WHERE clause")
	}

	// 3. emitted_ancestors: evaluated from the parsed seed and recursive
	// JOIN ON, WHERE, and SELECT ASTs.
	type ancEdge struct {
		traceID             string
		serviceName         string
		descendantSpanID    string
		descendantStartTime time.Time
		hasInput            bool
		hasOutput           bool
		hasReasoning        bool
		ancestorSpanID      string
		ancestorParentID    string
		ancestorStartTime   time.Time
		hop                 int64
		pathSpanIDs         []string
	}
	var edges []ancEdge
	var frontier []ancEdge
	for _, c := range context {
		for _, p := range context {
			seedEnv := maps.Clone(baseEnv)
			bindRow(seedEnv, "c.", c)
			bindRow(seedEnv, "p.", p)
			joined, err := dfxEvalBoolToks(eaSeedJoinOn, seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed JOIN ON: %w", err)
			}
			if !joined {
				continue
			}
			keep, err := dfxEvalBoolToks(eaSeedWhere, seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed WHERE: %w", err)
			}
			if !keep {
				continue
			}
			curIn, err := dfxEvalBoolToks(eaSeedExprs["has_input"], seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed has_input: %w", err)
			}
			curOut, err := dfxEvalBoolToks(eaSeedExprs["has_output"], seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed has_output: %w", err)
			}
			curReas, err := dfxEvalBoolToks(eaSeedExprs["has_reasoning"], seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed has_reasoning: %w", err)
			}
			seedPath, err := dfxEvalStringArrayToks(eaSeedExprs["path_span_ids"], seedEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating emitted_ancestors seed path_span_ids: %w", err)
			}
			edge := ancEdge{
				traceID:             c.traceID,
				serviceName:         c.serviceName,
				descendantSpanID:    c.spanID,
				descendantStartTime: c.startTime,
				hasInput:            curIn,
				hasOutput:           curOut,
				hasReasoning:        curReas,
				ancestorSpanID:      p.spanID,
				ancestorParentID:    p.parentSpanID,
				ancestorStartTime:   p.startTime,
				hop:                 1,
				pathSpanIDs:         seedPath,
			}
			edges = append(edges, edge)
			frontier = append(frontier, edge)
		}
	}
	for step := 0; len(frontier) > 0 && step <= maxHops+2; step++ {
		var nextFrontier []ancEdge
		for _, ea := range frontier {
			for _, p := range context {
				recEnv := maps.Clone(baseEnv)
				bindRow(recEnv, "p.", p)
				recEnv["ea.trace_id"] = ea.traceID
				recEnv["ea.service_name"] = ea.serviceName
				recEnv["ea.descendant_span_id"] = ea.descendantSpanID
				recEnv["ea.descendant_start_time"] = ea.descendantStartTime
				recEnv["ea.has_input"] = ea.hasInput
				recEnv["ea.has_output"] = ea.hasOutput
				recEnv["ea.has_reasoning"] = ea.hasReasoning
				recEnv["ea.ancestor_span_id"] = ea.ancestorSpanID
				recEnv["ea.ancestor_parent_span_id"] = ea.ancestorParentID
				recEnv["ea.ancestor_start_time"] = ea.ancestorStartTime
				recEnv["ea.hop"] = ea.hop
				recEnv["ea.path_span_ids"] = ea.pathSpanIDs

				joined, err := dfxEvalBoolToks(eaRecJoinOn, recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive JOIN ON: %w", err)
				}
				if !joined {
					continue
				}
				keep, err := dfxEvalBoolToks(eaRecWhere, recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive WHERE: %w", err)
				}
				if !keep {
					continue
				}
				curIn, err := dfxEvalBoolToks(eaRecExprs["has_input"], recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive has_input: %w", err)
				}
				curOut, err := dfxEvalBoolToks(eaRecExprs["has_output"], recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive has_output: %w", err)
				}
				curReas, err := dfxEvalBoolToks(eaRecExprs["has_reasoning"], recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive has_reasoning: %w", err)
				}
				nextPath, err := dfxEvalStringArrayToks(eaRecExprs["path_span_ids"], recEnv)
				if err != nil {
					return nil, fmt.Errorf("evaluating emitted_ancestors recursive path_span_ids: %w", err)
				}
				edge := ancEdge{
					traceID:             ea.traceID,
					serviceName:         ea.serviceName,
					descendantSpanID:    ea.descendantSpanID,
					descendantStartTime: ea.descendantStartTime,
					hasInput:            curIn,
					hasOutput:           curOut,
					hasReasoning:        curReas,
					ancestorSpanID:      p.spanID,
					ancestorParentID:    p.parentSpanID,
					ancestorStartTime:   p.startTime,
					hop:                 ea.hop + 1,
					pathSpanIDs:         nextPath,
				}
				edges = append(edges, edge)
				nextFrontier = append(nextFrontier, edge)
			}
		}
		frontier = nextFrontier
	}

	// 4. token_parent_coverage with reverse_path cycle tie-breaker, using the
	// column mapping parsed from token_parent_coverage's SQL AST.
	type covFlags struct {
		hasInput, hasOutput, hasReasoning bool
	}
	edgeFlag := func(ea ancEdge, field string) bool {
		switch field {
		case "has_input":
			return ea.hasInput
		case "has_output":
			return ea.hasOutput
		case "has_reasoning":
			return ea.hasReasoning
		default:
			return false
		}
	}
	coverage := map[string]covFlags{}
	for _, ea := range edges {
		if !ea.hasInput && !ea.hasOutput && !ea.hasReasoning {
			continue
		}
		hasReverse := false
		for _, rev := range edges {
			if rev.traceID == ea.traceID &&
				rev.serviceName == ea.serviceName &&
				rev.descendantSpanID == ea.ancestorSpanID &&
				rev.ancestorSpanID == ea.descendantSpanID {
				hasReverse = true
				break
			}
		}
		if hasReverse {
			later := ea.descendantStartTime.After(ea.ancestorStartTime) ||
				(ea.descendantStartTime.Equal(ea.ancestorStartTime) && ea.descendantSpanID > ea.ancestorSpanID)
			if !later {
				continue
			}
		}
		k := ea.traceID + "|" + ea.serviceName + "|" + ea.ancestorSpanID
		cf := coverage[k]
		cf.hasInput = cf.hasInput || edgeFlag(ea, covSourceField["has_input"])
		cf.hasOutput = cf.hasOutput || edgeFlag(ea, covSourceField["has_output"])
		cf.hasReasoning = cf.hasReasoning || edgeFlag(ea, covSourceField["has_reasoning"])
		coverage[k] = cf
	}

	type candRow struct {
		spanID    string
		violation string // empty string represents SQL NULL
	}
	cteRows := map[string][]candRow{
		"row_violations": nil,
		"double_counted": nil,
		"missing":        nil,
	}

	// 5. emitted (start_time >= contextStart) -> row_violations and double_counted,
	// evaluated directly from the parsed SQL AST.
	for _, m := range context {
		if m.startTime.Before(contextStart) {
			continue
		}
		cov, hasCovRow := coverage[m.traceID+"|"+m.serviceName+"|"+m.spanID]
		var covInVal, covOutVal, covReasVal any
		if hasCovRow {
			covInVal = cov.hasInput
			covOutVal = cov.hasOutput
			covReasVal = cov.hasReasoning
		}
		emittedEnv := map[string]any{
			"m.raw_input_tokens":     m.rawInput,
			"m.raw_output_tokens":    m.rawOutput,
			"m.raw_reasoning_tokens": m.rawReasoning,
			"cov.has_input":          covInVal,
			"cov.has_output":         covOutVal,
			"cov.has_reasoning":      covReasVal,
		}
		covInput, err := dfxEvalBoolToks(emittedExprs["cov_input"], emittedEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating emitted.cov_input: %w", err)
		}
		covOutput, err := dfxEvalBoolToks(emittedExprs["cov_output"], emittedEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating emitted.cov_output: %w", err)
		}
		covReasoning, err := dfxEvalBoolToks(emittedExprs["cov_reasoning"], emittedEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating emitted.cov_reasoning: %w", err)
		}
		expectedLeaf, err := dfxEvalBoolToks(emittedExprs["expected_is_token_leaf"], emittedEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating emitted.expected_is_token_leaf: %w", err)
		}

		rowEnv := map[string]any{
			"e.is_token_leaf":          m.isTokenLeaf,
			"e.input_tokens":           m.inputTokens,
			"e.output_tokens":          m.outputTokens,
			"e.reasoning_tokens":       m.reasoningTokens,
			"e.raw_input_tokens":       m.rawInput,
			"e.raw_output_tokens":      m.rawOutput,
			"e.raw_reasoning_tokens":   m.rawReasoning,
			"e.cov_input":              covInput,
			"e.cov_output":             covOutput,
			"e.cov_reasoning":          covReasoning,
			"e.expected_is_token_leaf": expectedLeaf,
		}

		rvMatchedViolation := ""
		for _, br := range rvBranches {
			matched, err := dfxEvalBoolToks(br.cond, rowEnv)
			if err != nil {
				return nil, fmt.Errorf("evaluating row_violations WHEN for %s: %w", br.violation, err)
			}
			if matched {
				rvMatchedViolation = br.violation
				break
			}
		}
		cteRows["row_violations"] = append(cteRows["row_violations"], candRow{
			spanID:    m.spanID,
			violation: rvMatchedViolation,
		})

		dcMatched, err := dfxEvalBoolToks(dcWhere, rowEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating double_counted WHERE: %w", err)
		}
		if dcMatched {
			cteRows["double_counted"] = append(cteRows["double_counted"], candRow{
				spanID:    m.spanID,
				violation: dcViolation,
			})
		}
	}

	// 6. missing CTE evaluated from its parsed WHERE predicate (including the
	// NOT EXISTS subquery over the trajectory mart) and violation literal.
	for _, s := range staged {
		missingEnv := maps.Clone(baseEnv)
		bindRow(missingEnv, "s.", s)
		missingEnv["__mart_rows__"] = martRows
		matched, err := dfxEvalBoolToks(missingWhere, missingEnv)
		if err != nil {
			return nil, fmt.Errorf("evaluating missing WHERE: %w", err)
		}
		if matched {
			cteRows["missing"] = append(cteRows["missing"], candRow{
				spanID:    s.spanID,
				violation: missingViolation,
			})
		}
	}

	// 7. Final UNION ALL branches (q.branches): evaluate each branch's FROM and optional WHERE.
	if len(q.branches) == 0 {
		return nil, errors.New("assertion query has no final SELECT branches")
	}
	violations := map[string][]string{}
	addViolation := func(spanID, v string) {
		if !slices.Contains(violations[spanID], v) {
			violations[spanID] = append(violations[spanID], v)
		}
	}
	for i, b := range q.branches {
		fromName := strings.ToLower(dashJoin(b.clauses["FROM"]))
		cands, ok := cteRows[fromName]
		if !ok {
			return nil, fmt.Errorf("final SELECT branch %d reads unsupported FROM source %q", i+1, fromName)
		}
		whereToks, hasWhere := b.clauses["WHERE"]
		for _, cand := range cands {
			if hasWhere {
				var violVal any = (*int64)(nil)
				if cand.violation != "" {
					violVal = dfxInt64(1)
				}
				keep, err := dfxEvalBoolToks(whereToks, map[string]any{"violation": violVal})
				if err != nil {
					return nil, fmt.Errorf("evaluating final SELECT branch %d WHERE: %w", i+1, err)
				}
				if !keep {
					continue
				}
			}
			if cand.violation != "" {
				addViolation(cand.spanID, cand.violation)
			}
		}
	}
	return violations, nil
}

// TestDataformOrphanTokensAssertionSemantics verifies that
// assert_no_orphan_tokens.sqlx uses a 32-hop WITH RECURSIVE ancestor walk and
// catches both silent token drops (a token-bearing span with no covering
// descendants marked is_token_leaf = false) and multi-hop double-counting
// across tokenless intermediate spans, and that mutating the SQL in
// assert_no_orphan_tokens.sqlx causes the AST-driven evaluator to fail.
func TestDataformOrphanTokensAssertionSemantics(t *testing.T) {
	files := dashLoadFileSet(t)
	pkg := dfxLoad(files)
	a := pkg.byName["assert_no_orphan_tokens"]
	if a == nil {
		t.Fatal("missing assert_no_orphan_tokens")
	}
	run, err := dfxRender(a, true)
	if err != nil {
		t.Fatalf("dfxRender: %v", err)
	}
	trajAction := pkg.byName[dfxTraj]
	if trajAction == nil {
		t.Fatalf("missing %s", dfxTraj)
	}
	trajRun, err := dfxRender(trajAction, true)
	if err != nil {
		t.Fatalf("dfxRender(%s): %v", dfxTraj, err)
	}
	postOrphansSQL, err := dfxOrphanTokenTempTableSQL(trajRun.find("temp table orphan_token_violations"))
	if err != nil {
		t.Fatalf("dfxOrphanTokenTempTableSQL: %v", err)
	}
	upper := strings.ToUpper(run.body)
	for _, required := range []string{
		"WITH RECURSIVE",
		"EMITTED_ANCESTORS AS",
		"TOKEN_PARENT_COVERAGE AS",
		"EA.HOP < 32",
		"EXPECTED_IS_TOKEN_LEAF",
	} {
		if !strings.Contains(upper, required) {
			t.Errorf("assert_no_orphan_tokens.sqlx missing %q", required)
		}
	}

	windowStart := time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC)
	ts := windowStart.Add(30 * time.Minute)

	validTreeRows := []dfxSpanRow{
		{
			traceID: "t1", serviceName: "svc", spanID: "root", parentSpanID: "",
			startTime: ts, inMart: true, isTokenLeaf: false,
			inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
		},
		{
			traceID: "t1", serviceName: "svc", spanID: "mid", parentSpanID: "root",
			startTime: ts, inMart: true, isTokenLeaf: false,
		},
		{
			traceID: "t1", serviceName: "svc", spanID: "leaf", parentSpanID: "mid",
			startTime: ts, inMart: true, isTokenLeaf: true,
			inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
		},
	}

	rootDroppedRows := []dfxSpanRow{
		{
			traceID: "t2", serviceName: "svc", spanID: "root_dropped", parentSpanID: "",
			startTime: ts, inMart: true, isTokenLeaf: false,
			inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
		},
	}

	t.Run("valid rollup and multi-hop leaf tree produces zero violations", func(t *testing.T) {
		got, err := dfxEvalOrphanTokensAssertion(run.body, validTreeRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("unexpected violations on valid tree: %v", got)
		}
	})

	t.Run("root span with tokens and no descendants marked is_token_leaf=false triggers rollup_token_changed", func(t *testing.T) {
		got, err := dfxEvalOrphanTokensAssertion(run.body, rootDroppedRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion: %v", err)
		}
		if !slices.Equal(got["root_dropped"], []string{"rollup_token_changed"}) {
			t.Errorf("got violations %v, want map[root_dropped:[rollup_token_changed]]", got)
		}
	})

	t.Run("multi-hop root -> tokenless intermediate -> token-bearing grandchild both is_token_leaf=true triggers token_double_counted_with_child", func(t *testing.T) {
		rows := []dfxSpanRow{
			{
				traceID: "t3", serviceName: "svc", spanID: "root_double", parentSpanID: "",
				startTime: ts, inMart: true, isTokenLeaf: true,
				inputTokens: dfxInt64(10), rawInput: dfxInt64(10),
			},
			{
				traceID: "t3", serviceName: "svc", spanID: "tokenless_mid", parentSpanID: "root_double",
				startTime: ts, inMart: true, isTokenLeaf: false,
			},
			{
				traceID: "t3", serviceName: "svc", spanID: "grandchild", parentSpanID: "tokenless_mid",
				startTime: ts, inMart: true, isTokenLeaf: true,
				inputTokens: dfxInt64(5), rawInput: dfxInt64(5),
			},
		}
		got, err := dfxEvalOrphanTokensAssertion(run.body, rows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion: %v", err)
		}
		if !slices.Equal(got["root_double"], []string{"token_double_counted_with_child"}) {
			t.Errorf("got violations %v, want map[root_double:[token_double_counted_with_child]]", got)
		}
	})

	t.Run("partial leaf that keeps a covered token family triggers token_double_counted_with_child", func(t *testing.T) {
		rows := []dfxSpanRow{
			{
				traceID: "t4", serviceName: "svc", spanID: "partial_parent", parentSpanID: "",
				startTime: ts, inMart: true, isTokenLeaf: true,
				inputTokens: dfxInt64(10), reasoningTokens: dfxInt64(3),
				rawInput: dfxInt64(10), rawReasoning: dfxInt64(3),
			},
			{
				traceID: "t4", serviceName: "svc", spanID: "input_child", parentSpanID: "partial_parent",
				startTime: ts, inMart: true, isTokenLeaf: true,
				inputTokens: dfxInt64(5), rawInput: dfxInt64(5),
			},
		}
		got, err := dfxEvalOrphanTokensAssertion(run.body, rows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion: %v", err)
		}
		if !slices.Equal(got["partial_parent"], []string{"token_double_counted_with_child"}) {
			t.Errorf("got violations %v, want map[partial_parent:[token_double_counted_with_child]]", got)
		}
	})

	t.Run("staged token span absent from trajectory triggers span_missing_from_trajectory", func(t *testing.T) {
		rows := []dfxSpanRow{
			{
				traceID: "t5", serviceName: "svc", spanID: "orphan_cycle_span", parentSpanID: "other",
				startTime: ts, inMart: false, rawInput: dfxInt64(7),
			},
		}
		got, err := dfxEvalOrphanTokensAssertion(run.body, rows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion: %v", err)
		}
		if !slices.Equal(got["orphan_cycle_span"], []string{"span_missing_from_trajectory"}) {
			t.Errorf("got violations %v, want map[orphan_cycle_span:[span_missing_from_trajectory]]", got)
		}
	})

	t.Run("SQL mutation replacing e.expected_is_token_leaf with FALSE in row_violations is caught by AST evaluator", func(t *testing.T) {
		mutatedSQL := strings.Replace(run.body, "e.expected_is_token_leaf", "FALSE", 1)
		if mutatedSQL == run.body {
			t.Fatal("failed to mutate e.expected_is_token_leaf in rendered SQL")
		}
		got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, rootDroppedRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
		}
		if slices.Equal(got["root_dropped"], []string{"rollup_token_changed"}) {
			t.Errorf("mutating e.expected_is_token_leaf to FALSE still reported rollup_token_changed; evaluator is not bound to row_violations SQL AST")
		}
	})

	t.Run("SQL mutation replacing LOGICAL_OR(ea.has_input) with LOGICAL_OR(ea.has_output) in token_parent_coverage is caught by AST evaluator", func(t *testing.T) {
		mutatedSQL := strings.Replace(run.body, "LOGICAL_OR(ea.has_input) AS has_input", "LOGICAL_OR(ea.has_output) AS has_input", 1)
		if mutatedSQL == run.body {
			t.Fatal("failed to mutate LOGICAL_OR(ea.has_input) in rendered SQL")
		}
		got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, validTreeRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
		}
		if !slices.Equal(got["root"], []string{"rollup_token_changed"}) {
			t.Errorf("mutating LOGICAL_OR(ea.has_input) to LOGICAL_OR(ea.has_output) gave %v on valid tree, want root:[rollup_token_changed]", got)
		}
	})

	t.Run("SQL mutation replacing c.raw_input_tokens IS NOT NULL AS has_input with FALSE AS has_input in emitted_ancestors is caught by AST evaluator", func(t *testing.T) {
		mutatedSQL := strings.Replace(run.body, "c.raw_input_tokens IS NOT NULL AS has_input", "FALSE AS has_input", 1)
		if mutatedSQL == run.body {
			t.Fatal("failed to mutate c.raw_input_tokens IS NOT NULL AS has_input in rendered SQL")
		}
		got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, validTreeRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
		}
		if !slices.Equal(got["root"], []string{"rollup_token_changed"}) {
			t.Errorf("mutating c.raw_input_tokens IS NOT NULL AS has_input to FALSE AS has_input gave %v on valid tree, want root:[rollup_token_changed]", got)
		}
	})

	t.Run("SQL mutation replacing c.raw_input_tokens IS NOT NULL AS has_input with c.raw_output_tokens IS NOT NULL AS has_input in emitted_ancestors is caught by AST evaluator", func(t *testing.T) {
		mutatedSQL := strings.Replace(run.body, "c.raw_input_tokens IS NOT NULL AS has_input", "c.raw_output_tokens IS NOT NULL AS has_input", 1)
		if mutatedSQL == run.body {
			t.Fatal("failed to mutate c.raw_input_tokens IS NOT NULL AS has_input in rendered SQL")
		}
		got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, validTreeRows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
		}
		if !slices.Equal(got["root"], []string{"rollup_token_changed"}) {
			t.Errorf("mutating c.raw_input_tokens IS NOT NULL AS has_input to c.raw_output_tokens IS NOT NULL AS has_input gave %v on valid tree, want root:[rollup_token_changed]", got)
		}
	})

	t.Run("SQL mutation appending WHERE FALSE to missing branch of final UNION ALL is caught by AST evaluator", func(t *testing.T) {
		mutatedSQL := strings.Replace(run.body, "FROM missing", "FROM missing\nWHERE FALSE", 1)
		if mutatedSQL == run.body {
			t.Fatal("failed to mutate FROM missing in rendered SQL")
		}
		rows := []dfxSpanRow{
			{
				traceID: "t5", serviceName: "svc", spanID: "orphan_cycle_span", parentSpanID: "other",
				startTime: ts, inMart: false, rawInput: dfxInt64(7),
			},
		}
		got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, rows, windowStart)
		if err != nil {
			t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
		}
		if slices.Equal(got["orphan_cycle_span"], []string{"span_missing_from_trajectory"}) {
			t.Errorf("appending WHERE FALSE to FROM missing still reported span_missing_from_trajectory; evaluator is not bound to final UNION ALL AST")
		}
	})

	t.Run("SQL mutation changing seed join p.span_id = c.parent_span_id to p.span_id = c.span_id is caught by AST evaluator on both copies", func(t *testing.T) {
		for _, sqlSrc := range []string{run.body, postOrphansSQL} {
			mutatedSQL := strings.Replace(sqlSrc, "AND p.span_id = c.parent_span_id", "AND p.span_id = c.span_id", 1)
			if mutatedSQL == sqlSrc {
				t.Fatal("failed to mutate seed join ON in rendered SQL")
			}
			got, err := dfxEvalOrphanTokensAssertion(mutatedSQL, validTreeRows, windowStart)
			if err != nil {
				t.Fatalf("dfxEvalOrphanTokensAssertion on mutated SQL: %v", err)
			}
			if !slices.Equal(got["root"], []string{"rollup_token_changed"}) {
				t.Errorf("mutating seed join ON to p.span_id = c.span_id gave %v on validTreeRows, want root:[rollup_token_changed]", got)
			}
		}
	})

	t.Run("SQL mutation breaking recursive join p.span_id = ea.ancestor_parent_span_id or adding AND FALSE to recursive WHERE is caught by AST evaluator on both copies", func(t *testing.T) {
		for _, sqlSrc := range []string{run.body, postOrphansSQL} {
			mutJoin := strings.Replace(sqlSrc, "AND p.span_id = ea.ancestor_parent_span_id", "AND p.span_id = ea.ancestor_span_id", 1)
			if mutJoin == sqlSrc {
				t.Fatal("failed to mutate recursive join ON in rendered SQL")
			}
			gotJoin, err := dfxEvalOrphanTokensAssertion(mutJoin, validTreeRows, windowStart)
			if err != nil {
				t.Fatalf("dfxEvalOrphanTokensAssertion on mutated recursive join SQL: %v", err)
			}
			if !slices.Equal(gotJoin["root"], []string{"rollup_token_changed"}) {
				t.Errorf("mutating recursive join ON gave %v on validTreeRows, want root:[rollup_token_changed]", gotJoin)
			}

			mutWhere := strings.Replace(sqlSrc, "WHERE ea.hop < 32", "WHERE ea.hop < 32 AND FALSE", 1)
			if mutWhere == sqlSrc {
				t.Fatal("failed to mutate recursive WHERE in rendered SQL")
			}
			gotWhere, err := dfxEvalOrphanTokensAssertion(mutWhere, validTreeRows, windowStart)
			if err != nil {
				t.Fatalf("dfxEvalOrphanTokensAssertion on mutated recursive WHERE SQL: %v", err)
			}
			if !slices.Equal(gotWhere["root"], []string{"rollup_token_changed"}) {
				t.Errorf("mutating recursive WHERE to AND FALSE gave %v on validTreeRows, want root:[rollup_token_changed]", gotWhere)
			}
		}
	})

	t.Run("SQL mutation removing p.span_id NOT IN UNNEST(ea.path_span_ids) from recursive WHERE is caught by AST evaluator on both copies", func(t *testing.T) {
		for _, sqlSrc := range []string{run.body, postOrphansSQL} {
			mutatedSQL := strings.Replace(sqlSrc, "AND p.span_id NOT IN UNNEST(ea.path_span_ids)", "", 1)
			if mutatedSQL == sqlSrc {
				t.Fatal("failed to remove path_span_ids cycle guard in rendered SQL")
			}
			if _, err := dfxEvalOrphanTokensAssertion(mutatedSQL, validTreeRows, windowStart); err == nil {
				t.Errorf("removing p.span_id NOT IN UNNEST(ea.path_span_ids) succeeded, want error from cycle guard check")
			}
		}
	})

	t.Run("SQL mutation appending AND FALSE to missing NOT EXISTS subquery or outer WHERE is caught by AST evaluator on both copies", func(t *testing.T) {
		for _, sqlSrc := range []string{run.body, postOrphansSQL} {
			idx := strings.LastIndex(sqlSrc, "AND m.span_id = s.span_id")
			if idx < 0 {
				t.Fatal("failed to locate missing NOT EXISTS span_id predicate in rendered SQL")
			}
			mutInner := sqlSrc[:idx] + "AND m.span_id = s.span_id AND FALSE" + sqlSrc[idx+len("AND m.span_id = s.span_id"):]
			gotInner, err := dfxEvalOrphanTokensAssertion(mutInner, validTreeRows, windowStart)
			if err != nil {
				t.Fatalf("dfxEvalOrphanTokensAssertion on inner NOT EXISTS mutation: %v", err)
			}
			if !slices.Contains(gotInner["root"], "span_missing_from_trajectory") ||
				!slices.Contains(gotInner["leaf"], "span_missing_from_trajectory") {
				t.Errorf("appending AND FALSE inside NOT EXISTS gave %v on validTreeRows, want span_missing_from_trajectory on emitted token spans", gotInner)
			}
		}
	})

	t.Run("SQL mutation breaking trajectory_context join or emitted_ancestors path_span_ids array is caught by AST evaluator on both copies", func(t *testing.T) {
		for _, sqlSrc := range []string{run.body, postOrphansSQL} {
			mutTC := strings.Replace(sqlSrc, "AND m.span_id = s.span_id", "AND m.span_id = s.parent_span_id", 1)
			if mutTC == sqlSrc {
				t.Fatal("failed to mutate trajectory_context JOIN ON in rendered SQL")
			}
			gotTC, err := dfxEvalOrphanTokensAssertion(mutTC, validTreeRows, windowStart)
			if err != nil {
				t.Fatalf("dfxEvalOrphanTokensAssertion on trajectory_context mutation: %v", err)
			}
			if len(gotTC) == 0 {
				t.Errorf("mutating trajectory_context JOIN ON gave 0 violations on validTreeRows, want violations")
			}

			mutSeedPath := strings.Replace(sqlSrc, "[c.span_id, p.span_id] AS path_span_ids", "[c.span_id, c.span_id] AS path_span_ids", 1)
			if mutSeedPath == sqlSrc {
				t.Fatal("failed to mutate seed path_span_ids in rendered SQL")
			}
			if _, err := dfxEvalOrphanTokensAssertion(mutSeedPath, validTreeRows, windowStart); err == nil {
				t.Errorf("mutating seed path_span_ids succeeded, want error")
			}

			mutRecPath := strings.Replace(sqlSrc, "ARRAY_CONCAT(ea.path_span_ids, [p.span_id]) AS path_span_ids", "[p.span_id] AS path_span_ids", 1)
			if mutRecPath == sqlSrc {
				t.Fatal("failed to mutate recursive path_span_ids in rendered SQL")
			}
			if _, err := dfxEvalOrphanTokensAssertion(mutRecPath, validTreeRows, windowStart); err == nil {
				t.Errorf("mutating recursive path_span_ids succeeded, want error")
			}
		}
	})
}

// TestDataformTargetResolutionAndVarsOverride verifies that Dataform resolves
// destination action targets from top-level workflow_settings.yaml fields
// (defaultProject, defaultLocation, defaultDataset, defaultAssertionDataset)
// while --vars only overrides vars.* (source_project, source_dataset,
// run_timestamp), and that deploy/dataform/README.md documents editing
// workflow_settings.yaml before compiling or running.
func TestDataformTargetResolutionAndVarsOverride(t *testing.T) {
	files := dashLoadFileSet(t)
	s, err := dfxDecodeSettings(files)
	if err != nil {
		t.Fatalf("dfxDecodeSettings: %v", err)
	}
	pkg := dfxLoad(files)

	type resolvedTarget struct {
		project, location, dataset, name string
	}
	resolve := func(cfg dfxSettings, cliVars map[string]string, a *dfxAction) resolvedTarget {
		vars := map[string]string{}
		for k, v := range cfg.Vars {
			vars[k] = v
		}
		for k, v := range cliVars {
			vars[k] = v
		}
		switch a.cfg.typ {
		case "declaration":
			return resolvedTarget{
				project:  vars[strings.TrimPrefix(a.cfg.database, dfxVarExpr)],
				location: cfg.DefaultLocation,
				dataset:  vars[strings.TrimPrefix(a.cfg.schema, dfxVarExpr)],
				name:     a.name,
			}
		case "assertion":
			return resolvedTarget{
				project:  cfg.DefaultProject,
				location: cfg.DefaultLocation,
				dataset:  cfg.DefaultAssertionDataset,
				name:     a.name,
			}
		default:
			return resolvedTarget{
				project:  cfg.DefaultProject,
				location: cfg.DefaultLocation,
				dataset:  cfg.DefaultDataset,
				name:     a.name,
			}
		}
	}

	// Passing only --vars leaves defaultProject at YOUR_PROJECT_ID for marts/tables/assertions.
	cliOnly := resolve(*s, map[string]string{"source_project": "my-project", "source_dataset": "otel_export"}, pkg.byName[dfxStaging])
	if cliOnly.project != dfxProjectPlaceholder {
		t.Fatalf("with unedited workflow_settings.yaml, destination project = %q, want %q", cliOnly.project, dfxProjectPlaceholder)
	}

	// Setting defaultProject and defaultLocation in workflow_settings.yaml resolves all actions cleanly.
	configured := *s
	configured.DefaultProject = "my-project"
	configured.DefaultLocation = "US"
	configured.Vars = map[string]string{
		"source_project": "my-project",
		"source_dataset": "otel_export",
		"run_timestamp":  "",
	}
	for _, e := range dfxPlan {
		got := resolve(configured, nil, pkg.byName[e.name])
		if got.project != "my-project" || got.location != "US" {
			t.Errorf("%s resolved to %+v, want project=my-project location=US", e.name, got)
		}
	}

	readme := dashSquash(files[dfxDoc])
	for _, needle := range []string{
		"Always set `defaultProject` and `defaultLocation` in [`workflow_settings.yaml`](workflow_settings.yaml) before running",
		"`--vars` only overrides entries under `vars:`, so leaving `defaultProject` at `YOUR_PROJECT_ID` would still target `YOUR_PROJECT_ID` for the destination tables and assertion views",
	} {
		if !strings.Contains(readme, needle) {
			t.Errorf("%s missing setup guidance %q", dfxDoc, needle)
		}
	}
}
