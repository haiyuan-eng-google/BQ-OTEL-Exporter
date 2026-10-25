// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

// The tests in this file hold the dashboards in dashboards/ to the schema they
// read. Nothing here reaches BigQuery, Looker Studio or Grafana: every check
// runs on the files of the repository.
//
//   - The Looker Studio queries, the Grafana panel queries and the Dataform
//     marts (dataform_conformance_test.go) are parsed with a small GoogleSQL
//     reader and every identifier they use is resolved against the columns of
//     the view the exporter creates (internal/schema), so a renamed or dropped
//     view column fails here and not in a dashboard.
//   - report_spec.yaml is decoded strictly and cross-checked against the
//     queries, the Terraform that binds the data sources and the acceptance
//     list of each page.
//   - The Grafana dashboards are checked against the datasource plugin
//     contract (inputs, panel layout, query format and macros) and the
//     collector dashboard against metadata.yaml, which declares the metrics.
//
// Every rule is a pure function from the files to a list of problems. The
// tests run the rules on the repository and then on copies with one defect
// each, and require the rule that is meant to catch the defect to report it.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// dashFileSet maps a slash-separated path relative to the repository root to
// the content of the file.
type dashFileSet map[string]string

// dashLoadFileSet reads the files the dashboard rules look at: the dashboards,
// the Dataform package, the Continuous Queries templates, the Terraform module
// and metadata.yaml. Local artifacts that are not part of the repository
// (Dataform credentials, node_modules and the package files of a local npm
// install) are skipped.
func dashLoadFileSet(t *testing.T) dashFileSet {
	t.Helper()
	files := dashFileSet{}
	for _, root := range []string{"dashboards", "deploy/dataform", "deploy/continuous-queries"} {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if name == "node_modules" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasPrefix(name, ".") && name != ".gitignore" || name == "package.json" || name == "package-lock.json" {
				return nil
			}
			raw, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(p)] = string(raw)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}
	for _, p := range []string{"README.md", "deploy/terraform/main.tf", "deploy/terraform/outputs.tf", "deploy/terraform/variables.tf", "metadata.yaml"} {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("reading %s: %v", p, err)
		}
		files[p] = string(raw)
	}
	return files
}

// clone returns a copy that a test can change without touching the original.
func (f dashFileSet) clone() dashFileSet {
	c := make(dashFileSet, len(f))
	for k, v := range f {
		c[k] = v
	}
	return c
}

// sortedPaths returns the paths of the set that have the given prefix and
// suffix, in order.
func (f dashFileSet) sortedPaths(prefix, suffix string) []string {
	var paths []string
	for p := range f {
		if strings.HasPrefix(p, prefix) && strings.HasSuffix(p, suffix) {
			paths = append(paths, p)
		}
	}
	slices.Sort(paths)
	return paths
}

// ---------------------------------------------------------------------------
// A GoogleSQL reader.
//
// It knows as much of the language as the dashboards and the Dataform marts
// use: WITH [RECURSIVE] tables, SELECT branches joined by UNION or INTERSECT or
// EXCEPT, select lists with aliases and stars, and the FROM / WHERE / GROUP BY
// / HAVING / QUALIFY / ORDER BY / LIMIT clauses. It splits a query into those
// parts and leaves function arguments and subqueries inside the tokens of the
// item or clause that holds them. It rejects what it does not know (triple
// quoted strings, unbalanced brackets) instead of guessing.
// ---------------------------------------------------------------------------

type dashTokKind int

const (
	dashWord   dashTokKind = iota // keyword, function name or bare identifier
	dashNumber                    // 12, 0.5
	dashString                    // 'x', "x", r'x', b'x'; text keeps the quotes
	dashIdent                     // `x`; text is without the backticks
	dashParam                     // @name; text is without the @
	dashPunct                     // operator, comma, dot or bracket
)

// dashTok is a token of a statement. depth is the bracket nesting of the
// token; both brackets of a pair carry the depth outside the pair.
type dashTok struct {
	kind  dashTokKind
	text  string
	up    string // text in upper case
	depth int
	pos   int
}

func (t dashTok) is(punct string) bool { return t.kind == dashPunct && t.text == punct }

func dashIsWordStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func dashIsDigit(c byte) bool { return c >= '0' && c <= '9' }

// dashStringPrefixes are the letters that may precede a quote to make a raw or
// a bytes literal.
var dashStringPrefixes = map[string]bool{"R": true, "B": true, "RB": true, "BR": true}

var dashTwoCharPuncts = []string{">=", "<=", "!=", "<>", "||", "=>", "<<", ">>"}

// dashScanString returns the index after the string literal that opens at src[i].
// A backslash protects the next byte in raw and in ordinary literals alike.
func dashScanString(src string, i int) (int, error) {
	q := src[i]
	if strings.HasPrefix(src[i:], strings.Repeat(string(q), 3)) {
		return 0, fmt.Errorf("triple-quoted string at byte %d is not supported", i)
	}
	for j := i + 1; j < len(src); j++ {
		switch src[j] {
		case '\\':
			j++
		case q:
			return j + 1, nil
		case '\n':
			return 0, fmt.Errorf("line break in string at byte %d", i)
		}
	}
	return 0, fmt.Errorf("unterminated string at byte %d", i)
}

// dashTokenize splits a statement into tokens and drops its comments.
func dashTokenize(src string) ([]dashTok, error) {
	var (
		toks  []dashTok
		stack []byte
	)
	add := func(kind dashTokKind, text string, pos int) {
		toks = append(toks, dashTok{kind: kind, text: text, up: strings.ToUpper(text), depth: len(stack), pos: pos})
	}
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.HasPrefix(src[i:], "--"):
			if j := strings.IndexByte(src[i:], '\n'); j >= 0 {
				i += j
			} else {
				i = len(src)
			}
		case strings.HasPrefix(src[i:], "/*"):
			j := strings.Index(src[i+2:], "*/")
			if j < 0 {
				return nil, fmt.Errorf("unterminated comment at byte %d", i)
			}
			i += j + 4
		case c == '\'' || c == '"':
			j, err := dashScanString(src, i)
			if err != nil {
				return nil, err
			}
			add(dashString, src[i:j], i)
			i = j
		case c == '`':
			j := strings.IndexByte(src[i+1:], '`')
			if j < 0 {
				return nil, fmt.Errorf("unterminated identifier at byte %d", i)
			}
			add(dashIdent, src[i+1:i+1+j], i)
			i += j + 2
		case c == '@':
			j := i + 1
			for j < len(src) && (dashIsWordStart(src[j]) || dashIsDigit(src[j])) {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("bare @ at byte %d", i)
			}
			add(dashParam, src[i+1:j], i)
			i = j
		case dashIsDigit(c):
			j := i
			for j < len(src) && dashIsDigit(src[j]) {
				j++
			}
			if j+1 < len(src) && src[j] == '.' && dashIsDigit(src[j+1]) {
				j++
				for j < len(src) && dashIsDigit(src[j]) {
					j++
				}
			}
			add(dashNumber, src[i:j], i)
			i = j
		case dashIsWordStart(c):
			j := i
			for j < len(src) && (dashIsWordStart(src[j]) || dashIsDigit(src[j])) {
				j++
			}
			if j < len(src) && (src[j] == '\'' || src[j] == '"') && dashStringPrefixes[strings.ToUpper(src[i:j])] {
				k, err := dashScanString(src, j)
				if err != nil {
					return nil, err
				}
				add(dashString, src[i:k], i)
				i = k
				continue
			}
			add(dashWord, src[i:j], i)
			i = j
		case i+1 < len(src) && slices.Contains(dashTwoCharPuncts, src[i:i+2]):
			add(dashPunct, src[i:i+2], i)
			i += 2
		case c == '(' || c == '[':
			add(dashPunct, string(c), i)
			stack = append(stack, c)
			i++
		case c == ')' || c == ']':
			want := byte('(')
			if c == ']' {
				want = '['
			}
			if len(stack) == 0 || stack[len(stack)-1] != want {
				return nil, fmt.Errorf("unbalanced %q at byte %d", c, i)
			}
			stack = stack[:len(stack)-1]
			add(dashPunct, string(c), i)
			i++
		case strings.IndexByte(",.=<>+-*/%|&^~;:?!", c) >= 0:
			add(dashPunct, string(c), i)
			i++
		default:
			return nil, fmt.Errorf("unexpected character %q at byte %d", c, i)
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("%d bracket(s) left open", len(stack))
	}
	return toks, nil
}

// dashQuery is a query expression: optional WITH tables and one or more SELECT
// branches. parent is the query whose WITH tables are visible to this one.
type dashQuery struct {
	parent   *dashQuery
	ctes     []dashCTE
	branches []*dashBranch
}

type dashCTE struct {
	name  string
	query *dashQuery
}

// dashBranch is one SELECT with the clauses at its own nesting level.
type dashBranch struct {
	owner   *dashQuery
	toks    []dashTok
	items   []dashItem
	clauses map[string][]dashTok // keyed FROM, WHERE, GROUP BY, HAVING, QUALIFY, ORDER BY, LIMIT; tokens after the keyword
}

// dashItem is one entry of a select list.
type dashItem struct {
	toks   []dashTok
	body   []dashTok // toks without the final AS alias
	name   string    // output column name; "" when the item names none
	star   bool
	except []string // columns a star leaves out
}

// dashClosing returns the index of the bracket that closes the one at toks[open].
func dashClosing(toks []dashTok, open int) int {
	for k := open + 1; k < len(toks); k++ {
		if toks[k].depth == toks[open].depth && (toks[k].is(")") || toks[k].is("]")) {
			return k
		}
	}
	return -1
}

// dashParseSQL parses one statement made of a single query expression.
func dashParseSQL(src string) (*dashQuery, error) {
	toks, err := dashTokenize(src)
	if err != nil {
		return nil, err
	}
	for len(toks) > 0 && toks[len(toks)-1].is(";") {
		toks = toks[:len(toks)-1]
	}
	return dashParseQuery(toks, nil)
}

func dashParseQuery(toks []dashTok, parent *dashQuery) (*dashQuery, error) {
	if len(toks) == 0 {
		return nil, errors.New("empty query")
	}
	q := &dashQuery{parent: parent}
	base := toks[0].depth
	i := 0
	if toks[0].up == "WITH" {
		i = 1
		if i < len(toks) && toks[i].up == "RECURSIVE" {
			i++
		}
		for {
			if i+2 >= len(toks) || toks[i].kind != dashWord && toks[i].kind != dashIdent || toks[i+1].up != "AS" || !toks[i+2].is("(") {
				return nil, errors.New("malformed WITH table")
			}
			end := dashClosing(toks, i+2)
			if end < 0 {
				return nil, fmt.Errorf("WITH table %s is not closed", toks[i].text)
			}
			body, err := dashParseQuery(toks[i+3:end], q)
			if err != nil {
				return nil, fmt.Errorf("WITH table %s: %w", toks[i].text, err)
			}
			q.ctes = append(q.ctes, dashCTE{name: toks[i].text, query: body})
			i = end + 1
			if i < len(toks) && toks[i].is(",") && toks[i].depth == base {
				i++
				continue
			}
			break
		}
	}
	start := i
	for k := i; k < len(toks); k++ {
		t := toks[k]
		if t.depth != base || t.kind != dashWord {
			continue
		}
		followedByModifier := k+1 < len(toks) && (toks[k+1].up == "ALL" || toks[k+1].up == "DISTINCT")
		switch {
		case t.up == "UNION" || t.up == "INTERSECT":
			if !followedByModifier {
				return nil, fmt.Errorf("%s needs ALL or DISTINCT", t.up)
			}
		case t.up == "EXCEPT" && followedByModifier:
		default:
			continue
		}
		b, err := dashParseBranch(toks[start:k], q)
		if err != nil {
			return nil, err
		}
		q.branches = append(q.branches, b)
		start = k + 2
		k++
	}
	b, err := dashParseBranch(toks[start:], q)
	if err != nil {
		return nil, err
	}
	q.branches = append(q.branches, b)
	return q, nil
}

func dashParseBranch(toks []dashTok, owner *dashQuery) (*dashBranch, error) {
	if len(toks) == 0 || toks[0].up != "SELECT" {
		return nil, errors.New("branch does not start with SELECT")
	}
	base := toks[0].depth
	b := &dashBranch{owner: owner, toks: toks, clauses: map[string][]dashTok{}}
	type mark struct {
		name       string
		kw, starts int
	}
	var marks []mark
	for k := 1; k < len(toks); k++ {
		t := toks[k]
		if t.depth != base || t.kind != dashWord {
			continue
		}
		switch t.up {
		case "FROM":
			if toks[k-1].up != "DISTINCT" { // IS DISTINCT FROM
				marks = append(marks, mark{"FROM", k, k + 1})
			}
		case "WHERE", "HAVING", "QUALIFY", "LIMIT":
			marks = append(marks, mark{t.up, k, k + 1})
		case "GROUP", "ORDER":
			if k+1 < len(toks) && toks[k+1].up == "BY" {
				marks = append(marks, mark{t.up + " BY", k, k + 2})
			}
		}
	}
	itemsEnd := len(toks)
	if len(marks) > 0 {
		itemsEnd = marks[0].kw
	}
	first := 1
	if first < itemsEnd && (toks[first].up == "DISTINCT" || toks[first].up == "ALL") {
		first++
	}
	if first+1 < itemsEnd && toks[first].up == "AS" && (toks[first+1].up == "STRUCT" || toks[first+1].up == "VALUE") {
		first += 2
	}
	itemStart := first
	for k := first; k <= itemsEnd; k++ {
		if k < itemsEnd && (toks[k].depth != base || !toks[k].is(",")) {
			continue
		}
		if k == itemStart {
			return nil, errors.New("empty select item")
		}
		it, err := dashParseItem(toks[itemStart:k], base)
		if err != nil {
			return nil, err
		}
		b.items = append(b.items, it)
		itemStart = k + 1
	}
	for n, m := range marks {
		end := len(toks)
		if n+1 < len(marks) {
			end = marks[n+1].kw
		}
		if _, dup := b.clauses[m.name]; dup {
			return nil, fmt.Errorf("%s appears twice in one SELECT", m.name)
		}
		b.clauses[m.name] = toks[m.starts:end]
	}
	return b, nil
}

func dashParseItem(toks []dashTok, base int) (dashItem, error) {
	it := dashItem{toks: toks, body: toks}
	n := len(toks)
	if n >= 3 && toks[n-2].up == "AS" && toks[n-2].depth == base && toks[n-1].depth == base &&
		(toks[n-1].kind == dashWord || toks[n-1].kind == dashIdent) {
		it.name = toks[n-1].text
		it.body = toks[:n-2]
		return it, nil
	}
	star := -1
	switch {
	case toks[0].is("*"):
		star = 0
	case n >= 3 && toks[1].is(".") && toks[2].is("*"):
		star = 2
	}
	if star >= 0 {
		it.star = true
		rest := toks[star+1:]
		if len(rest) == 0 {
			return it, nil
		}
		if len(rest) < 3 || rest[0].up != "EXCEPT" || !rest[1].is("(") || !rest[len(rest)-1].is(")") {
			return it, errors.New("unsupported star modifier")
		}
		for _, t := range rest[2 : len(rest)-1] {
			if t.kind == dashWord || t.kind == dashIdent {
				it.except = append(it.except, t.text)
			}
		}
		return it, nil
	}
	last := toks[n-1]
	if last.kind == dashWord || last.kind == dashIdent {
		if n == 1 && !dashSQLKeywords[last.up] || n >= 3 && toks[n-2].is(".") && toks[n-3].kind == dashWord {
			it.name = last.text
		}
	}
	return it, nil
}

// lookup returns the WITH table that the name refers to from q, or nil.
func (q *dashQuery) lookup(name string) *dashQuery {
	for p := q; p != nil; p = p.parent {
		for _, c := range p.ctes {
			if c.name == name {
				return c.query
			}
		}
	}
	return nil
}

// columns returns the output columns of the query: those of its first branch.
func (q *dashQuery) columns(depth int) ([]string, error) {
	return q.branches[0].columns(depth)
}

// columns returns the output column names of the branch. A star is expanded
// when the branch reads a WITH table.
func (b *dashBranch) columns(depth int) ([]string, error) {
	if depth > 32 {
		return nil, errors.New("WITH tables nest too deeply")
	}
	var cols []string
	for _, it := range b.items {
		if !it.star {
			cols = append(cols, it.name)
			continue
		}
		from := b.clauses["FROM"]
		if len(from) == 0 || from[0].kind != dashWord && from[0].kind != dashIdent {
			return nil, errors.New("cannot expand a star without a named table")
		}
		src := b.owner.lookup(from[0].text)
		if src == nil {
			return nil, fmt.Errorf("cannot expand a star over %s: it is not a WITH table", from[0].text)
		}
		srcCols, err := src.columns(depth + 1)
		if err != nil {
			return nil, err
		}
		for _, c := range srcCols {
			if !slices.Contains(it.except, c) {
				cols = append(cols, c)
			}
		}
	}
	return cols, nil
}

// fromTables returns the quoted table names of the FROM clause.
func (b *dashBranch) fromTables() []string {
	var tables []string
	for _, t := range b.clauses["FROM"] {
		if t.kind == dashIdent {
			tables = append(tables, t.text)
		}
	}
	return tables
}

// dashSQLKeywords are the words that are neither column names nor function
// calls, in upper case. A word that is not in this list and is not followed by
// an opening bracket has to be a column, an alias or a variable.
var dashSQLKeywords = func() map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(`
		SELECT FROM WHERE GROUP ORDER BY HAVING QUALIFY LIMIT UNION INTERSECT EXCEPT ALL DISTINCT
		AS AND OR NOT NULL IS IN TRUE FALSE CASE WHEN THEN ELSE END BETWEEN LIKE EXISTS
		OVER PARTITION ASC DESC ROWS RANGE UNBOUNDED PRECEDING FOLLOWING CURRENT ROW NULLS FIRST LAST
		INTERVAL YEAR MONTH WEEK DAY HOUR MINUTE SECOND MILLISECOND MICROSECOND
		STRING INT64 FLOAT64 BOOL BYTES NUMERIC BIGNUMERIC TIMESTAMP DATE DATETIME TIME JSON ARRAY STRUCT
		JOIN LEFT RIGHT INNER OUTER CROSS FULL ON USING UNNEST WITH RECURSIVE DECLARE DEFAULT
		OFFSET ORDINAL IGNORE RESPECT`) {
		m[w] = true
	}
	return m
}()

// dashAllowedSQLFuncs are the GoogleSQL functions the dashboards, views and
// Dataform package may call. Any unknown function call is rejected.
var dashAllowedSQLFuncs = func() map[string]bool {
	m := map[string]bool{}
	for _, fn := range strings.Fields(`
		SUM AVG MIN MAX COUNT ANY_VALUE LOGICAL_OR ARRAY_AGG STRING_AGG APPROX_QUANTILES ROW_NUMBER
		COALESCE IF IFNULL NULLIF CAST SAFE_CAST SAFE_DIVIDE ROUND
		DATE TIMESTAMP TIMESTAMP_TRUNC TIMESTAMP_SUB TIMESTAMP_ADD TIMESTAMP_DIFF TIMESTAMP_MILLIS UNIX_MICROS
		PARSE_DATE DATE_ADD DATE_SUB CURRENT_TIMESTAMP
		CONCAT LOWER UPPER TRIM SUBSTR LENGTH TO_JSON_STRING JSON_VALUE JSON_QUERY JSON_EXTRACT_SCALAR
		REGEXP_CONTAINS REGEXP_EXTRACT
		ARRAY STRUCT UNNEST ARRAY_LENGTH ARRAY_TO_STRING ARRAY_CONCAT GENERATE_ARRAY SAFE_OFFSET SAFE_ORDINAL`) {
		m[fn] = true
	}
	return m
}()

// dashSource says what the identifiers of one SELECT may name.
type dashSource struct {
	// cols are the unqualified names the sources of the FROM clause provide,
	// and the script variables in scope.
	cols map[string]bool
	// quals maps a FROM alias to its columns, for the qualified references
	// that a column list has to resolve.
	quals map[string]map[string]bool
}

func dashSet(names []string, more ...string) map[string]bool {
	m := make(map[string]bool, len(names)+len(more))
	for _, n := range names {
		m[n] = true
	}
	for _, n := range more {
		m[n] = true
	}
	return m
}

// dashDefinedAliases returns the names the tokens define with AS.
func dashDefinedAliases(toks []dashTok) map[string]bool {
	m := map[string]bool{}
	for i := 1; i < len(toks); i++ {
		if toks[i-1].up == "AS" && (toks[i].kind == dashWord || toks[i].kind == dashIdent) {
			m[toks[i].text] = true
		}
	}
	return m
}

// dashCheckBranch resolves every identifier of the branch and returns what it
// cannot resolve. A select item may use the columns of the source and the
// aliases of the FROM clause and of its own body, but not the aliases of other
// items. WHERE adds nothing to that, since BigQuery does not accept a select
// alias there; GROUP BY, HAVING, QUALIFY and ORDER BY may also use the names
// the select list outputs.
func dashCheckBranch(b *dashBranch, src dashSource) []string {
	var problems []string
	fromAliases := dashDefinedAliases(b.clauses["FROM"])
	outputs := map[string]bool{}
	for _, it := range b.items {
		if it.name != "" {
			outputs[it.name] = true
		}
	}
	check := func(where string, toks []dashTok, tableRefs bool, extra ...map[string]bool) {
		inner := dashDefinedAliases(toks)
		for i, t := range toks {
			if t.kind != dashWord || dashSQLKeywords[t.up] {
				continue
			}
			if i+1 < len(toks) && toks[i+1].is("(") {
				if !dashAllowedSQLFuncs[t.up] {
					problems = append(problems, fmt.Sprintf("%s: unknown function %s", where, t.text))
				}
				continue
			}
			if i > 0 && toks[i-1].up == "AS" {
				continue // an alias or a type name
			}
			if i > 0 && toks[i-1].is(".") {
				if i >= 2 && toks[i-2].kind == dashWord {
					if cols, ok := src.quals[toks[i-2].text]; ok && !cols[t.text] {
						problems = append(problems, fmt.Sprintf("%s: %s.%s is not a column of the table %s stands for", where, toks[i-2].text, t.text, toks[i-2].text))
					}
				}
				continue
			}
			if tableRefs && b.isTableRef(toks, i) {
				continue
			}
			if src.cols[t.text] || fromAliases[t.text] || inner[t.text] {
				continue
			}
			known := false
			for _, m := range extra {
				known = known || m[t.text]
			}
			if !known {
				problems = append(problems, fmt.Sprintf("%s: unknown identifier %s", where, t.text))
			}
		}
	}
	for n, it := range b.items {
		name := it.name
		if name == "" {
			name = fmt.Sprintf("#%d", n+1)
		}
		check("select item "+name, it.body, false)
	}
	for _, name := range []string{"FROM", "WHERE", "GROUP BY", "HAVING", "QUALIFY", "ORDER BY"} {
		toks, ok := b.clauses[name]
		if !ok {
			continue
		}
		switch name {
		case "FROM":
			check(name, toks, true)
		case "WHERE":
			check(name, toks, false)
		default:
			check(name, toks, false, outputs)
		}
	}
	return problems
}

// isTableRef says whether the word at toks[i] of the FROM clause names a table:
// the first thing after FROM, after JOIN or after a comma at the top level.
func (b *dashBranch) isTableRef(toks []dashTok, i int) bool {
	if i+1 < len(toks) && toks[i+1].is(".") {
		return false
	}
	return i == 0 || toks[i-1].up == "JOIN" || toks[i-1].is(",") && toks[i-1].depth == toks[0].depth
}

// ---------------------------------------------------------------------------
// The columns of the views.
// ---------------------------------------------------------------------------

// dashViewSQLs returns the query of every companion view the dashboards and the
// Dataform sources read, as the module creates it, keyed by view name.
func dashViewSQLs() map[string]string {
	return map[string]string{
		"v_gen_ai_model_costs":      fmt.Sprintf(schema.GenAIModelCostsViewQuery, "p", "d"),
		"v_gen_ai_sessions":         fmt.Sprintf(schema.GenAISessionsViewQuery, "p", "d"),
		"v_gen_ai_tool_usage":       fmt.Sprintf(schema.GenAIToolUsageViewQuery, "p", "d"),
		"v_mcp_tool_usage":          fmt.Sprintf(schema.MCPToolUsageViewQuery, "p", "d"),
		"v_gen_ai_agent_trajectory": fmt.Sprintf(schema.GenAIAgentTrajectoryViewQuery, "p", "d"),
		"v_gen_ai_metric_exemplars": fmt.Sprintf(schema.GenAIMetricExemplarsViewQuery, "p", "d", "otel_spans"),
		"v_gen_ai_spans":            fmt.Sprintf(schema.GenAISpansViewQuery, "p", "d", "otel_spans"),
	}
}

// dashViewColumnCache holds the parsed columns of every companion view. The
// views do not change while the tests run and the mutation tests ask for the
// columns of the largest ones many times.
var dashViewColumnCache = sync.OnceValue(func() map[string]dashViewColumnsResult {
	cached := map[string]dashViewColumnsResult{}
	for view, sql := range dashViewSQLs() {
		var res dashViewColumnsResult
		q, err := dashParseSQL(sql)
		if err != nil {
			res.err = fmt.Errorf("parsing %s: %w", view, err)
		} else if res.cols, err = q.columns(0); err != nil {
			res.err = fmt.Errorf("columns of %s: %w", view, err)
		}
		cached[view] = res
	}
	return cached
})

type dashViewColumnsResult struct {
	cols []string
	err  error
}

// dashViewColumns returns the output columns of a companion view.
func dashViewColumns(view string) ([]string, error) {
	res, ok := dashViewColumnCache()[view]
	if !ok {
		return nil, fmt.Errorf("no view query is known for %s", view)
	}
	return slices.Clone(res.cols), res.err
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

// dashProblems collects what a rule finds wrong.
type dashProblems []string

func (p *dashProblems) addf(format string, args ...any) {
	*p = append(*p, fmt.Sprintf(format, args...))
}

// dashNonASCII returns the index of the first byte of text that is not ASCII,
// or -1.
func dashNonASCII(text string) int {
	for i := 0; i < len(text); i++ {
		if text[i] >= 0x80 {
			return i
		}
	}
	return -1
}

func dashSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// dashJoin renders tokens as upper-case words separated by single spaces, so
// that a clause can be compared with the form it is meant to have.
func dashJoin(toks []dashTok) string {
	parts := make([]string, len(toks))
	for i, t := range toks {
		switch t.kind {
		case dashWord:
			parts[i] = t.up
		case dashParam:
			parts[i] = "@" + t.text
		default:
			parts[i] = t.text
		}
	}
	return strings.Join(parts, " ")
}

func dashHasWord(toks []dashTok, word string) bool {
	for _, t := range toks {
		if t.kind == dashWord && t.up == word {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// The Looker Studio package.
// ---------------------------------------------------------------------------

const (
	dashLookerDir = "dashboards/looker_studio/"

	// dashLookerLicense opens every query file.
	dashLookerLicense = "/*\n * Copyright 2026 Google LLC\n * SPDX-License-Identifier: Apache-2.0\n *"
)

// dashSpec is report_spec.yaml. It is decoded with KnownFields, so a key that
// these structs do not name is an error.
type dashSpec struct {
	SpecVersion int `yaml:"spec_version"`
	Report      struct {
		Name      string `yaml:"name"`
		PageCount int    `yaml:"page_count"`
	} `yaml:"report"`
	Placeholders struct {
		Project string `yaml:"project"`
		Dataset string `yaml:"dataset"`
	} `yaml:"placeholders"`
	DateRange struct {
		StartParameter string `yaml:"start_parameter"`
		EndParameter   string `yaml:"end_parameter"`
		Format         string `yaml:"format"`
		Inclusive      bool   `yaml:"inclusive"`
		Default        string `yaml:"default"`
	} `yaml:"date_range"`
	ReportParameters []struct {
		Name        string `yaml:"name"`
		Type        string `yaml:"type"`
		Default     any    `yaml:"default"`
		Description string `yaml:"description"`
	} `yaml:"report_parameters"`
	DataSources map[string]dashSpecSource `yaml:"data_sources"`
	Pages       []dashSpecPage            `yaml:"pages"`
}

type dashSpecSource struct {
	Name             string            `yaml:"name"`
	Query            string            `yaml:"query"`
	Views            []string          `yaml:"views"`
	Marts            map[string]string `yaml:"marts"`
	TerraformBinding string            `yaml:"terraform_binding"`
	DateField        string            `yaml:"date_field"`
	FieldTypes       map[string]string `yaml:"field_types"`
	CalculatedFields []dashSpecCalc    `yaml:"calculated_fields"`
}

type dashSpecCalc struct {
	Name       string   `yaml:"name"`
	Formula    string   `yaml:"formula"`
	Type       string   `yaml:"type"`
	Uses       []string `yaml:"uses"`
	Parameters []string `yaml:"parameters"`
}

type dashSpecPage struct {
	ID         string              `yaml:"id"`
	Title      string              `yaml:"title"`
	DataSource string              `yaml:"data_source"`
	Controls   []dashSpecControl   `yaml:"controls"`
	Components []dashSpecComponent `yaml:"components"`
}

type dashSpecControl struct {
	ID    string `yaml:"id"`
	Type  string `yaml:"type"`
	Field string `yaml:"field"`
}

type dashSpecComponent struct {
	ID         string           `yaml:"id"`
	Type       string           `yaml:"type"`
	Title      string           `yaml:"title"`
	Note       string           `yaml:"note"`
	Dimensions []string         `yaml:"dimensions"`
	Metrics    []dashSpecMetric `yaml:"metrics"`
	Sort       *struct {
		Field     string `yaml:"field"`
		Direction string `yaml:"direction"`
	} `yaml:"sort"`
	Limit     *int             `yaml:"limit"`
	Filters   []dashSpecFilter `yaml:"filters"`
	Breakdown string           `yaml:"breakdown"`
}

type dashSpecMetric struct {
	Field       string `yaml:"field"`
	Aggregation string `yaml:"aggregation"`
	Role        string `yaml:"role"`
}

type dashSpecFilter struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"`
	Value    any    `yaml:"value"`
}

func dashDecodeSpec(files dashFileSet) (*dashSpec, error) {
	raw, ok := files[dashLookerDir+"report_spec.yaml"]
	if !ok {
		return nil, errors.New("report_spec.yaml is missing")
	}
	dec := yaml.NewDecoder(strings.NewReader(raw))
	dec.KnownFields(true)
	var spec dashSpec
	if err := dec.Decode(&spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// dashAlertColumnPattern matches one column of the otel_alerts schema in main.tf.
var dashAlertColumnPattern = regexp.MustCompile(`\{ name = "([a-z_]+)", type = "([A-Z0-9]+)", mode = "[A-Z]+" \}`)

// dashAlertColumns returns the columns main.tf gives the otel_alerts table.
func dashAlertColumns(files dashFileSet) []string {
	text := files["deploy/terraform/main.tf"]
	start := strings.Index(text, "resource \"google_bigquery_table\" \"otel_alerts\" {\n")
	if start < 0 {
		return nil
	}
	block := text[start:]
	if end := strings.Index(block, "\n}\n"); end >= 0 {
		block = block[:end]
	}
	var cols []string
	for _, m := range dashAlertColumnPattern.FindAllStringSubmatch(block, -1) {
		cols = append(cols, m[1])
	}
	return cols
}

// dashSourceColumnTypes returns the GoogleSQL column types of a companion view
// or of otel_alerts so Looker Studio select items and metric aggregations can
// be type-checked.
func dashSourceColumnTypes(files dashFileSet, view string) map[string]string {
	types := map[string]string{
		// Timestamps and dates
		"usage_hour":         "TIMESTAMP",
		"session_start_date": "DATE",
		"first_turn_time":    "TIMESTAMP",
		"last_turn_time":     "TIMESTAMP",
		"start_time":         "TIMESTAMP",
		"end_time":           "TIMESTAMP",
		"root_start_time":    "TIMESTAMP",
		"metric_time":        "TIMESTAMP",
		"span_start_time":    "TIMESTAMP",
		"span_end_time":      "TIMESTAMP",
		"event_time":         "TIMESTAMP",
		"publish_time":       "TIMESTAMP",
		// Strings
		"service_name":           "STRING",
		"service_version":        "STRING",
		"deployment_environment": "STRING",
		"provider":               "STRING",
		"operation_name":         "STRING",
		"request_model":          "STRING",
		"response_model":         "STRING",
		"model":                  "STRING",
		"agent_name":             "STRING",
		"agent_id":               "STRING",
		"session_id":             "STRING",
		"user_id":                "STRING",
		"first_trace_id":         "STRING",
		"last_trace_id":          "STRING",
		"mcp_method_name":        "STRING",
		"tool_name":              "STRING",
		"tool_type":              "STRING",
		"server_address":         "STRING",
		"network_transport":      "STRING",
		"mcp_protocol_version":   "STRING",
		"trace_id":               "STRING",
		"span_id":                "STRING",
		"parent_span_id":         "STRING",
		"root_span_id":           "STRING",
		"span_name":              "STRING",
		"step_kind":              "STRING",
		"status_message":         "STRING",
		"error_type":             "STRING",
		"gen_ai_system":          "STRING",
		"gen_ai_operation_name":  "STRING",
		"gen_ai_request_model":   "STRING",
		"gen_ai_response_model":  "STRING",
		"gen_ai_agent_id":        "STRING",
		"gen_ai_agent_name":      "STRING",
		"gen_ai_conversation_id": "STRING",
		"gen_ai_tool_name":       "STRING",
		"gen_ai_tool_type":       "STRING",
		"gen_ai_tool_call_id":    "STRING",
		"gen_ai_user_id":         "STRING",
		"gen_ai_Workflow_name":   "STRING",
		"gen_ai_output_type":     "STRING",
		"gen_ai_data_source_id":  "STRING",
		"agent_path":             "STRING",
		"span_path":              "STRING",
		"metric_name":            "STRING",
		"metric_unit":            "STRING",
		"metric_type":            "STRING",
		"span_status_message":    "STRING",
		"span_error_type":        "STRING",
		"alert_id":               "STRING",
		"subscription_name":      "STRING",
		"source":                 "STRING",
		"trace_url":              "STRING",
		"alert_kind":             "STRING",
		"window_start":           "STRING",
		"window_end":             "STRING",
		"last_error_type":        "STRING",
		// Booleans
		"is_orphan":     "BOOL",
		"is_leaf":       "BOOL",
		"is_token_leaf": "BOOL",
		// Integers
		"span_kind":                         "INT64",
		"status_code":                       "INT64",
		"span_status_code":                  "INT64",
		"call_count":                        "INT64",
		"error_count":                       "INT64",
		"total_input_tokens":                "INT64",
		"total_output_tokens":               "INT64",
		"total_reasoning_tokens":            "INT64",
		"total_cache_read_input_tokens":     "INT64",
		"total_cache_creation_input_tokens": "INT64",
		"total_tokens":                      "INT64",
		"turn_count":                        "INT64",
		"llm_call_count":                    "INT64",
		"tool_call_count":                   "INT64",
		"agent_Error_count":                 "INT64",
		"distinct_models_used":              "INT64",
		"distinct_tools_used":               "INT64",
		"gen_ai_tagged_call_count":          "INT64",
		"mcp_only_call_count":               "INT64",
		"server_port":                       "INT64",
		"depth":                             "INT64",
		"gen_ai_usage_input_tokens":         "INT64",
		"gen_ai_usage_output_tokens":        "INT64",
		"gen_ai_usage_reasoning_tokens":     "INT64",
		"gen_ai_usage_cache_read_tokens":    "INT64",
		"gen_ai_usage_cache_create_tokens":  "INT64",
		"leaf_input_tokens":                 "INT64",
		"leaf_output_tokens":                "INT64",
		"leaf_reasoning_tokens":             "INT64",
		// Floats
		"avg_duration_ms":       "FLOAT64",
		"p50_duration_ms":       "FLOAT64",
		"p95_duration_ms":       "FLOAT64",
		"p99_duration_ms":       "FLOAT64",
		"max_duration_ms":       "FLOAT64",
		"session_duration_ms":   "FLOAT64",
		"total_llm_duration_ms": "FLOAT64",
		"duration_ms":           "FLOAT64",
		"exemplar_value":        "FLOAT64",
		"span_duration_ms":      "FLOAT64",
		"error_rate":            "FLOAT64",
		"p95_s":                 "FLOAT64",
	}
	if view == "otel_alerts" {
		text := files["deploy/terraform/main.tf"]
		if start := strings.Index(text, "resource \"google_bigquery_table\" \"otel_alerts\" {\n"); start >= 0 {
			block := text[start:]
			if end := strings.Index(block, "\n}\n"); end >= 0 {
				block = block[:end]
			}
			for _, m := range dashAlertColumnPattern.FindAllStringSubmatch(block, -1) {
				types[m[1]] = m[2]
			}
		}
	}
	return types
}

// dashInferExprType infers the GoogleSQL scalar type of a SELECT item body.
func dashInferExprType(toks []dashTok, colTypes map[string]string) string {
	if len(toks) == 0 {
		return ""
	}
	if len(toks) == 1 {
		t := toks[0]
		switch {
		case t.kind == dashString:
			return "STRING"
		case t.kind == dashNumber:
			if strings.Contains(t.text, ".") {
				return "FLOAT64"
			}
			return "INT64"
		case t.up == "TRUE" || t.up == "FALSE":
			return "BOOL"
		case t.kind == dashWord:
			return colTypes[strings.ToLower(t.text)]
		}
	}
	if (toks[0].up == "CAST" || toks[0].up == "SAFE_CAST") && len(toks) >= 5 && toks[1].is("(") && toks[len(toks)-1].is(")") {
		innerDepth := toks[1].depth + 1
		for i := len(toks) - 2; i >= 2; i-- {
			if toks[i].depth == innerDepth && toks[i-1].depth == innerDepth && toks[i-1].up == "AS" {
				return toks[i].up
			}
		}
	}
	if (toks[0].up == "COALESCE" || toks[0].up == "IFNULL" || toks[0].up == "NULLIF") && len(toks) >= 4 && toks[1].is("(") {
		innerDepth := toks[1].depth + 1
		start := 2
		for i := 2; i < len(toks); i++ {
			if (toks[i].depth == innerDepth && toks[i].is(",")) || i == len(toks)-1 {
				if typ := dashInferExprType(toks[start:i], colTypes); typ != "" {
					return typ
				}
				start = i + 1
			}
		}
	}
	if toks[0].up == "IF" && len(toks) >= 6 && toks[1].is("(") {
		innerDepth := toks[1].depth + 1
		var commas []int
		for i := 2; i < len(toks)-1; i++ {
			if toks[i].depth == innerDepth && toks[i].is(",") {
				commas = append(commas, i)
			}
		}
		if len(commas) == 2 {
			if typ := dashInferExprType(toks[commas[0]+1:commas[1]], colTypes); typ != "" {
				return typ
			}
			return dashInferExprType(toks[commas[1]+1:len(toks)-1], colTypes)
		}
	}
	switch toks[0].up {
	case "CONCAT", "JSON_VALUE", "JSON_EXTRACT_SCALAR", "TO_JSON_STRING", "LOWER", "UPPER", "TRIM", "ARRAY_TO_STRING":
		return "STRING"
	case "COUNT", "ARRAY_LENGTH":
		return "INT64"
	case "AVG", "SAFE_DIVIDE", "ROUND":
		return "FLOAT64"
	case "LOGICAL_OR", "REGEXP_CONTAINS":
		return "BOOL"
	}
	return ""
}

var dashPlaceholderPattern = regexp.MustCompile(`\$\{[a-z_]+\}`)

// dashAlertPayload reads the Continuous Queries templates and returns the keys
// of the alert JSON they publish and the alert kinds they use. A window alert
// documented in the README adds error_spans.
func dashAlertPayload(files dashFileSet) (keys, kinds map[string]bool, problems []string) {
	keys, kinds = map[string]bool{}, map[string]bool{}
	paths := files.sortedPaths("deploy/continuous-queries/", ".sql")
	if len(paths) != 3 {
		problems = append(problems, fmt.Sprintf("deploy/continuous-queries has %d templates, want 3", len(paths)))
	}
	for _, path := range paths {
		toks, err := dashTokenize(dashPlaceholderPattern.ReplaceAllString(files[path], "0"))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		found := false
		for i := 0; i+1 < len(toks) && !found; i++ {
			if toks[i].up != "STRUCT" || !toks[i+1].is("(") {
				continue
			}
			end := dashClosing(toks, i+1)
			if end < 0 {
				continue
			}
			found = true
			inner, depth, start := toks[i+2:end], toks[i+1].depth+1, 0
			for k := 0; k <= len(inner); k++ {
				if k < len(inner) && (inner[k].depth != depth || !inner[k].is(",")) {
					continue
				}
				if k > start {
					if it, err := dashParseItem(inner[start:k], depth); err == nil && it.name != "" {
						keys[it.name] = true
						if it.name == "alert_kind" {
							for _, t := range it.body {
								if t.kind == dashString {
									kinds[strings.Trim(t.text, "'")] = true
								}
							}
						}
					}
				}
				start = k + 1
			}
		}
		if !found {
			problems = append(problems, fmt.Sprintf("%s: no STRUCT of alert fields", path))
		}
	}
	if strings.Contains(files["deploy/continuous-queries/README.md"], "COUNT(*) AS error_spans") {
		keys["error_spans"] = true
	} else {
		problems = append(problems, "deploy/continuous-queries/README.md no longer documents error_spans; drop it from the alert keys the dashboards read")
	}
	return keys, kinds, problems
}

// dashSourceColumns returns the columns of a view or of the otel_alerts table.
func dashSourceColumns(files dashFileSet, view string) ([]string, error) {
	if view == "otel_alerts" {
		cols := dashAlertColumns(files)
		if len(cols) == 0 {
			return nil, errors.New("main.tf declares no otel_alerts columns")
		}
		return cols, nil
	}
	return dashViewColumns(view)
}

// dashLookerQuery is a parsed data source query.
type dashLookerQuery struct {
	path    string
	toks    []dashTok
	query   *dashQuery
	outputs []string
}

// dashLooker runs the rules on the Looker Studio package.
type dashLooker struct {
	files   dashFileSet
	spec    *dashSpec
	queries map[string]*dashLookerQuery
	probs   dashProblems
}

// dashValidateLooker runs every rule on the Looker Studio package: the query
// files, the report specification, and the Terraform that binds the data
// sources to a deployment.
func dashValidateLooker(files dashFileSet) []string {
	spec, err := dashDecodeSpec(files)
	if err != nil {
		return []string{"report_spec.yaml: " + err.Error()}
	}
	l := &dashLooker{files: files, spec: spec, queries: map[string]*dashLookerQuery{}}
	l.parseQueries()
	l.checkQueries()
	l.checkSpec()
	l.checkTerraform()
	return l.probs
}

func (l *dashLooker) parseQueries() {
	used := map[string]bool{}
	for _, id := range dashSortedKeys(l.spec.DataSources) {
		src := l.spec.DataSources[id]
		path := dashLookerDir + src.Query
		used[path] = true
		sql, ok := l.files[path]
		if !ok {
			l.probs.addf("%s: query file %s does not exist", id, path)
			continue
		}
		toks, err := dashTokenize(sql)
		if err != nil {
			l.probs.addf("%s: %v", path, err)
			continue
		}
		q, err := dashParseSQL(sql)
		if err != nil {
			l.probs.addf("%s: %v", path, err)
			continue
		}
		outputs, err := q.columns(0)
		if err != nil {
			l.probs.addf("%s: %v", path, err)
			continue
		}
		l.queries[id] = &dashLookerQuery{path: path, toks: toks, query: q, outputs: outputs}
	}
	for _, path := range l.files.sortedPaths(dashLookerDir+"queries/", ".sql") {
		if !used[path] {
			l.probs.addf("%s: no data source of report_spec.yaml uses this query", path)
		}
	}
}

// dashCheckDateRange checks that the WHERE clause is the two predicates on the
// date field that apply the date range parameters, and nothing else: it is what
// lets BigQuery prune partitions and keeps both ends of the range inclusive.
func dashCheckDateRange(b *dashBranch, dateField string, dateType bool) []string {
	var p dashProblems
	where, ok := b.clauses["WHERE"]
	if !ok {
		return []string{"has no WHERE clause on " + dateField}
	}
	base := b.toks[0].depth
	var conjuncts [][]dashTok
	start := 0
	for k := 0; k <= len(where); k++ {
		if k == len(where) || where[k].depth == base && where[k].up == "AND" {
			conjuncts = append(conjuncts, where[start:k])
			start = k + 1
		}
	}
	if len(conjuncts) != 2 {
		return []string{fmt.Sprintf("WHERE has %d predicates, want the two date range predicates on %s", len(conjuncts), dateField)}
	}
	for n, c := range conjuncts {
		param, ops := "DS_START_DATE", []string{">="}
		if n == 1 {
			param, ops = "DS_END_DATE", []string{"<", "<="}
		}
		if len(c) < 4 || c[0].kind != dashWord || c[0].text != dateField || c[1].kind != dashPunct || !slices.Contains(ops, c[1].text) {
			p.addf("predicate %d of WHERE is not `%s %s ...`", n+1, dateField, strings.Join(ops, "` or `"))
			continue
		}
		rest := c[2:]
		if dateType && rest[0].up != "PARSE_DATE" {
			p.addf("predicate %d compares the DATE column %s with something that is not PARSE_DATE(...)", n+1, dateField)
		}
		if !dateType && (rest[0].up != "TIMESTAMP" || !rest[1].is("(")) {
			p.addf("predicate %d compares the TIMESTAMP column %s with something that is not TIMESTAMP(...)", n+1, dateField)
		}
		hasParam := false
		for _, t := range rest {
			if t.kind == dashParam {
				hasParam = hasParam || t.text == param
				if t.text != param {
					p.addf("predicate %d uses @%s, want @%s", n+1, t.text, param)
				}
			}
		}
		if !hasParam {
			p.addf("predicate %d does not use @%s", n+1, param)
		}
		if !slices.ContainsFunc(rest, func(t dashTok) bool { return t.kind == dashString && t.text == "'%Y%m%d'" }) {
			p.addf("predicate %d does not parse the parameter with '%%Y%%m%%d'", n+1)
		}
		if n == 1 {
			addDay := dashHasWord(rest, "DATE_ADD") && strings.Contains(dashJoin(rest), "INTERVAL 1 DAY")
			switch c[1].text {
			case "<":
				if dateType || !addDay {
					p.addf("an exclusive end needs DATE_ADD(..., INTERVAL 1 DAY) over a TIMESTAMP column, so that the last day is included")
				}
			case "<=":
				if !dateType || dashHasWord(rest, "DATE_ADD") {
					p.addf("an inclusive end is for a DATE column and takes no DATE_ADD")
				}
			}
		}
	}
	return p
}

var (
	dashJSONPathKey  = regexp.MustCompile(`^'\$\.([A-Za-z_][A-Za-z0-9_]*)'$`)
	dashBannedWords  = []string{"status_message", "span_status_message"}
	dashDateTypeCols = map[string]bool{"session_start_date": true}
)

func (l *dashLooker) checkQueries() {
	keys, _, problems := dashAlertPayload(l.files)
	l.probs = append(l.probs, problems...)
	traceBase := "'https://console.cloud.google.com/traces/explorer;traceId='"
	traceProject := "'?project=" + l.spec.Placeholders.Project + "'"
	for _, id := range dashSortedKeys(l.spec.DataSources) {
		src, q := l.spec.DataSources[id], l.queries[id]
		if q == nil {
			continue
		}
		path := q.path
		sql := l.files[path]
		if !strings.HasPrefix(sql, dashLookerLicense) {
			l.probs.addf("%s: does not open with the license header", path)
		}
		if i := dashNonASCII(sql); i >= 0 {
			l.probs.addf("%s: byte %d is not ASCII", path, i)
		}
		for _, t := range q.toks {
			switch {
			case t.kind == dashParam && t.text != l.spec.DateRange.StartParameter && t.text != l.spec.DateRange.EndParameter:
				l.probs.addf("%s: @%s is not a date range parameter", path, t.text)
			case t.kind == dashWord && slices.Contains(dashBannedWords, strings.ToLower(t.text)):
				l.probs.addf("%s: reads %s, the free text of a span status, which no dashboard shows", path, t.text)
			case t.kind == dashString:
				if m := dashJSONPathKey.FindStringSubmatch(t.text); m != nil && !keys[m[1]] {
					l.probs.addf("%s: reads the alert payload key %s, which no Continuous Queries template publishes", path, m[1])
				}
			}
		}
		if len(q.query.branches) != len(src.Views) {
			l.probs.addf("%s: has %d SELECT branches for the %d views of %s", path, len(q.query.branches), len(src.Views), id)
			continue
		}
		var first []string
		if !slices.Contains(q.outputs, src.DateField) {
			l.probs.addf("%s: outputs no %s, the date field of %s", path, src.DateField, id)
		}
		for i, b := range q.query.branches {
			view := src.Views[i]
			label := fmt.Sprintf("%s branch %d (%s)", path, i+1, view)
			want := l.spec.Placeholders.Project + "." + l.spec.Placeholders.Dataset + "." + view
			if tables := b.fromTables(); len(tables) != 1 || tables[0] != want {
				l.probs.addf("%s: reads %v, want only `%s`", label, tables, want)
			}
			cols, err := b.columns(0)
			if err != nil {
				l.probs.addf("%s: %v", label, err)
				continue
			}
			seen := map[string]bool{}
			for _, c := range cols {
				switch {
				case c == "":
					l.probs.addf("%s: a select item has no name; add an alias", label)
				case seen[c]:
					l.probs.addf("%s: outputs %s twice", label, c)
				}
				seen[c] = true
			}
			if i == 0 {
				first = cols
			} else if !slices.Equal(first, cols) {
				l.probs.addf("%s: outputs %v, but the first branch outputs %v", label, cols, first)
			}
			viewCols, err := dashSourceColumns(l.files, view)
			if err != nil {
				l.probs.addf("%s: %v", label, err)
				continue
			}
			for _, msg := range dashCheckBranch(b, dashSource{cols: dashSet(viewCols)}) {
				l.probs.addf("%s: %s", label, msg)
			}
			colTypes := dashSourceColumnTypes(l.files, view)
			for _, it := range b.items {
				if it.name == "" {
					continue
				}
				gotType := dashInferExprType(it.body, colTypes)
				wantType := colTypes[it.name]
				if gotType != "" && wantType != "" && gotType != wantType {
					l.probs.addf("%s: select item %s has type %s, want %s", label, it.name, gotType, wantType)
				}
			}
			if mart, ok := src.Marts[view]; ok {
				martCols, err := dfxMartColumns(l.files, mart)
				if err != nil {
					l.probs.addf("%s: mart %s: %v", label, mart, err)
				} else {
					for _, msg := range dashCheckBranch(b, dashSource{cols: dashSet(martCols)}) {
						l.probs.addf("%s: mart %s lacks what the query reads, so the FROM clause cannot point at it: %s", label, mart, msg)
					}
				}
			}
			for _, msg := range dashCheckDateRange(b, src.DateField, dashDateTypeCols[src.DateField]) {
				l.probs.addf("%s: %s", label, msg)
			}
			for _, it := range b.items {
				if it.name != "trace_url" {
					continue
				}
				if !slices.ContainsFunc(it.body, func(t dashTok) bool { return t.kind == dashString && t.text == traceBase }) ||
					!slices.ContainsFunc(it.body, func(t dashTok) bool { return t.kind == dashString && t.text == traceProject }) {
					l.probs.addf("%s: trace_url is not the Cloud Trace explorer link for trace_id in %s", label, l.spec.Placeholders.Project)
				}
			}
			if view == "otel_alerts" {
				// dashJoin upper-cases words, and column names are not case sensitive.
				if got, want := dashJoin(b.clauses["QUALIFY"]), "ROW_NUMBER ( ) OVER ( PARTITION BY ALERT_ID ORDER BY EVENT_TIME ASC ) = 1"; got != want {
					l.probs.addf("%s: QUALIFY is %q, want %q: Pub/Sub delivers at least once, so an alert can arrive twice", label, got, want)
				}
			}
		}
	}
}

// dashDataSourcePlan is the contract of the six data sources: the Linking API
// alias, the query file, the views it reads, the page that uses it and when
// Terraform binds it to a deployment.
var dashDataSourcePlan = map[string]struct {
	query   string
	views   []string
	page    string
	binding string
}{
	"ds0": {"queries/executive_summary.sql", []string{"v_gen_ai_model_costs"}, "executive_genai_finops", "always"},
	"ds1": {"queries/agent_sessions.sql", []string{"v_gen_ai_sessions"}, "agent_sessions", "always"},
	"ds2": {"queries/mcp_tool_usage.sql", []string{"v_mcp_tool_usage", "v_gen_ai_tool_usage"}, "tool_and_mcp", "always"},
	"ds3": {"queries/agent_trajectory.sql", []string{"v_gen_ai_agent_trajectory"}, "trajectory", "partition_filter_off"},
	"ds4": {"queries/security_sla_alerts.sql", []string{"otel_alerts"}, "security_sla_alerts", "enable_continuous_queries"},
	"ds5": {"queries/metric_exemplars.sql", []string{"v_gen_ai_metric_exemplars"}, "metric_exemplars", "partition_filter_off"},
}

// dashMartForView names the Dataform mart that keeps the columns of a view.
var dashMartForView = map[string]string{
	"v_gen_ai_model_costs":      "m_gen_ai_model_costs_hourly",
	"v_gen_ai_sessions":         "m_gen_ai_sessions_daily",
	"v_mcp_tool_usage":          "m_mcp_tool_usage_hourly",
	"v_gen_ai_agent_trajectory": "m_gen_ai_agent_trajectory",
}

// dashAcceptance lists what each page has to show: the metrics, dimensions,
// filters and chart roles that the acceptance criteria of its page name.
// Entries are "metric field:AGGREGATION", "dim field", "filter field OPERATOR
// value" and "role chart-type role field".
var dashAcceptance = map[string][]string{
	"executive_genai_finops": {
		"metric total_input_tokens:SUM", "metric total_output_tokens:SUM", "metric total_reasoning_tokens:SUM",
		"metric total_cache_read_input_tokens:SUM", "metric total_cache_creation_input_tokens:SUM",
		"metric call_count:SUM", "metric error_rate:NONE",
		"metric p50_duration_ms:MAX", "metric p95_duration_ms:MAX", "metric p99_duration_ms:MAX",
		"dim model", "dim provider",
	},
	"agent_sessions": {
		"metric user_id:COUNT_DISTINCT", "metric turn_count:AVG", "dim turn_count_bucket",
		"role scatter_chart x session_duration_ms", "role scatter_chart y total_tokens",
		"filter error_count GREATER_THAN 0", "dim top_error_types",
	},
	"tool_and_mcp": {
		"dim mcp_method_name", "dim server_address", "dim top_error_types",
		"metric gen_ai_tagged_call_count:SUM", "metric mcp_only_call_count:SUM", "metric p95_duration_ms:MAX",
		"filter source EQUALS mcp", "filter source EQUALS gen_ai",
	},
	"trajectory": {
		"dim depth", "dim agent_path", "dim is_token_leaf", "filter is_orphan IS_TRUE <nil>",
		"metric leaf_input_tokens:SUM", "metric leaf_output_tokens:SUM",
		"role bar_chart all_reported gen_ai_usage_input_tokens", "role bar_chart counted_once leaf_input_tokens",
	},
	"security_sla_alerts": {
		"filter alert_kind EQUALS TOOL_ERROR", "filter alert_kind EQUALS TOKEN_BUDGET_BURN",
		"filter alert_kind EQUALS AGENT_LATENCY_HIGH", "dim trace_link",
	},
	"metric_exemplars": {
		"dim trace_link", "dim span_id", "metric trace_id:COUNT_DISTINCT", "metric exemplar_value:MAX",
	},
}

var (
	dashControlTypes   = []string{"date_range", "dropdown", "input_box"}
	dashComponentTypes = []string{"scorecard", "time_series", "bar_chart", "stacked_bar_chart", "table", "scatter_chart", "pie_chart"}
	dashAggregations   = []string{"SUM", "AVG", "MAX", "MIN", "COUNT", "COUNT_DISTINCT", "NONE"}
	dashFilterOps      = []string{"EQUALS", "GREATER_THAN", "IS_TRUE"}
	dashPercentiles    = []string{"p50_duration_ms", "p95_duration_ms", "p99_duration_ms"}
	dashFormulaWords   = []string{"SUM", "AVG", "MAX", "MIN", "COUNT", "COUNT_DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "HYPERLINK"}
	dashAggregateCall  = regexp.MustCompile(`\b(SUM|AVG|MAX|MIN|COUNT|COUNT_DISTINCT)\(`)
	dashFormulaString  = regexp.MustCompile(`"[^"]*"`)
	dashFormulaWord    = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	dashUpperWord      = regexp.MustCompile(`^[A-Z][A-Z_]*$`)
)

// dashCheckFormula checks one calculated field: the columns and parameters its
// formula reads are exactly those it declares, and they exist.
func dashCheckFormula(c dashSpecCalc, outputs, calcs, params map[string]bool) []string {
	var p dashProblems
	text := dashFormulaString.ReplaceAllString(c.Formula, `""`)
	read := map[string]bool{}
	for _, w := range dashFormulaWord.FindAllString(text, -1) {
		if dashUpperWord.MatchString(w) {
			if !slices.Contains(dashFormulaWords, w) {
				p.addf("formula uses %s, which is not a function or keyword the report formulas use", w)
			}
			continue
		}
		read[w] = true
		if !slices.Contains(c.Uses, w) && !slices.Contains(c.Parameters, w) && !calcs[w] {
			p.addf("formula reads %s, which is neither listed in uses or parameters nor another calculated field", w)
		}
	}
	for _, u := range c.Uses {
		if !read[u] {
			p.addf("uses lists %s, which the formula does not read", u)
		}
		if !outputs[u] {
			p.addf("uses lists %s, which the query does not output", u)
		}
	}
	for _, u := range c.Parameters {
		if !read[u] {
			p.addf("parameters lists %s, which the formula does not read", u)
		}
		if !params[u] {
			p.addf("parameters lists %s, which is not a report parameter", u)
		}
	}
	return p
}

func (l *dashLooker) checkSpec() {
	s := l.spec
	if s.SpecVersion != 1 {
		l.probs.addf("spec_version is %d, want 1", s.SpecVersion)
	}
	if s.Report.PageCount != 6 || len(s.Pages) != 6 {
		l.probs.addf("the report has %d pages and says page_count %d, want 6", len(s.Pages), s.Report.PageCount)
	}
	if got, want := dashSortedKeys(s.DataSources), dashSortedKeys(dashDataSourcePlan); !slices.Equal(got, want) {
		l.probs.addf("data sources are %v, want %v", got, want)
	}
	if s.Placeholders.Project != "YOUR_PROJECT_ID" || s.Placeholders.Dataset != "YOUR_DATASET_ID" {
		l.probs.addf("placeholders are %q and %q, want YOUR_PROJECT_ID and YOUR_DATASET_ID", s.Placeholders.Project, s.Placeholders.Dataset)
	}
	if dr := s.DateRange; dr.StartParameter != "DS_START_DATE" || dr.EndParameter != "DS_END_DATE" || dr.Format != "YYYYMMDD" || !dr.Inclusive {
		l.probs.addf("date_range is %+v, want DS_START_DATE and DS_END_DATE in YYYYMMDD with both ends inclusive", dr)
	}
	params := map[string]bool{}
	for _, rp := range s.ReportParameters {
		if params[rp.Name] {
			l.probs.addf("report parameter %s is declared twice", rp.Name)
		}
		params[rp.Name] = true
		if rp.Type != "NUMBER" {
			l.probs.addf("report parameter %s has type %q, want NUMBER", rp.Name, rp.Type)
		}
		if rp.Default != 0 {
			l.probs.addf("report parameter %s defaults to %v: the template ships no prices", rp.Name, rp.Default)
		}
	}
	_, kinds, _ := dashAlertPayload(l.files)
	fields := map[string]map[string]string{} // data source -> field -> "column", "agg" or "row"
	for _, id := range dashSortedKeys(s.DataSources) {
		src := s.DataSources[id]
		plan, ok := dashDataSourcePlan[id]
		if !ok {
			continue
		}
		if src.Query != plan.query {
			l.probs.addf("%s reads %s, want %s", id, src.Query, plan.query)
		}
		if !slices.Equal(src.Views, plan.views) {
			l.probs.addf("%s reads views %v, want %v", id, src.Views, plan.views)
		}
		if src.TerraformBinding != plan.binding {
			l.probs.addf("%s has terraform_binding %q, want %q", id, src.TerraformBinding, plan.binding)
		}
		for view, mart := range src.Marts {
			if !slices.Contains(src.Views, view) {
				l.probs.addf("%s names a mart for %s, which it does not read", id, view)
			}
			if mart != dashMartForView[view] {
				l.probs.addf("%s names the mart %s for %s, want %s", id, mart, view, dashMartForView[view])
			}
			// What the mart reads is the business of the Dataform rules; here
			// the mart only has to exist and to keep the columns the query
			// reads, which checkQueries resolves against dfxMartColumns.
			if _, ok := l.files["deploy/dataform/definitions/marts/"+mart+".sqlx"]; !ok {
				l.probs.addf("%s: the mart %s has no definition in deploy/dataform", id, mart)
			}
		}
		for _, view := range src.Views {
			if _, has := src.Marts[view]; !has && dashMartForView[view] != "" {
				l.probs.addf("%s: view %s has a Dataform mart (%s) that the spec does not name", id, view, dashMartForView[view])
			}
		}
		q := l.queries[id]
		if q == nil {
			continue
		}
		outputs := dashSet(q.outputs)
		kind := map[string]string{}
		for _, c := range q.outputs {
			kind[c] = "column"
		}
		calcs := map[string]bool{}
		for _, c := range src.CalculatedFields {
			if calcs[c.Name] || outputs[c.Name] {
				l.probs.addf("%s: calculated field %s repeats another field", id, c.Name)
			}
			calcs[c.Name] = true
		}
		for _, c := range src.CalculatedFields {
			if !slices.Contains([]string{"NUMBER", "PERCENT", "TEXT"}, c.Type) {
				l.probs.addf("%s: calculated field %s has type %q", id, c.Name, c.Type)
			}
			for _, msg := range dashCheckFormula(c, outputs, calcs, params) {
				l.probs.addf("%s: calculated field %s: %s", id, c.Name, msg)
			}
			kind[c.Name] = "row"
			if dashAggregateCall.MatchString(c.Formula) {
				kind[c.Name] = "agg"
			}
		}
		for f, typ := range src.FieldTypes {
			if kind[f] == "" {
				l.probs.addf("%s: field_types names %s, which is not a field", id, f)
			}
			if typ != "PERCENT" && typ != "URL" {
				l.probs.addf("%s: field %s has type %q", id, f, typ)
			}
		}
		fields[id] = kind
	}
	if len(s.Pages) == len(dashDataSourcePlan) {
		for i, page := range s.Pages {
			l.checkPage(i, page, fields, kinds)
		}
	}
}

func (l *dashLooker) checkPage(index int, page dashSpecPage, fields map[string]map[string]string, kinds map[string]bool) {
	id := fmt.Sprintf("ds%d", index)
	plan := dashDataSourcePlan[id]
	prefix := fmt.Sprintf("p%d_", index+1)
	if page.ID != plan.page || page.DataSource != id {
		l.probs.addf("page %d is %q on %s, want %q on %s", index+1, page.ID, page.DataSource, plan.page, id)
	}
	if strings.TrimSpace(page.Title) == "" {
		l.probs.addf("page %s has no title", page.ID)
	}
	kind := fields[id]
	if kind == nil {
		return // the data source is already reported
	}
	dateField := l.spec.DataSources[id].DateField
	seen := map[string]bool{}
	hasDateRange := false
	for _, c := range page.Controls {
		if !strings.HasPrefix(c.ID, prefix) || seen[c.ID] {
			l.probs.addf("page %s: control id %q is not unique or does not start with %s", page.ID, c.ID, prefix)
		}
		seen[c.ID] = true
		switch c.Type {
		case "date_range":
			hasDateRange = true
			if c.Field != "" {
				l.probs.addf("page %s: the date range control %s names a field", page.ID, c.ID)
			}
		case "dropdown", "input_box":
			if kind[c.Field] != "column" {
				l.probs.addf("page %s: control %s filters %q, which is not a column of the data source", page.ID, c.ID, c.Field)
			}
		default:
			l.probs.addf("page %s: control %s has type %q, want one of %v", page.ID, c.ID, c.Type, dashControlTypes)
		}
	}
	if !hasDateRange {
		l.probs.addf("page %s has no date range control, so the query parameters get no value", page.ID)
	}
	for _, c := range page.Components {
		l.checkComponent(page.ID, prefix, dateField, c, kind, kinds, seen)
	}
	uses := map[string]bool{}
	for _, c := range page.Components {
		for _, d := range c.Dimensions {
			uses["dim "+d] = true
		}
		for _, m := range c.Metrics {
			uses["metric "+m.Field+":"+m.Aggregation] = true
			if m.Role != "" {
				uses["role "+c.Type+" "+m.Role+" "+m.Field] = true
			}
		}
		for _, f := range c.Filters {
			uses[fmt.Sprintf("filter %s %s %v", f.Field, f.Operator, f.Value)] = true
		}
	}
	for _, need := range dashAcceptance[page.ID] {
		if !uses[need] {
			l.probs.addf("page %s does not use %q, which its acceptance criteria name", page.ID, need)
		}
	}
	if page.ID == "security_sla_alerts" {
		for _, k := range []string{"TOOL_ERROR", "TOKEN_BUDGET_BURN", "AGENT_LATENCY_HIGH"} {
			if !kinds[k] {
				l.probs.addf("the Continuous Queries templates publish no %s alert", k)
			}
		}
		if len(kinds) != 3 {
			l.probs.addf("the Continuous Queries templates publish alert kinds %v, want three that page %s covers", dashSortedKeys(kinds), page.ID)
		}
	}
}

func (l *dashLooker) checkComponent(page, prefix, dateField string, c dashSpecComponent, kind map[string]string, kinds, seen map[string]bool) {
	where := fmt.Sprintf("page %s, component %s", page, c.ID)
	if !strings.HasPrefix(c.ID, prefix) || seen[c.ID] {
		l.probs.addf("%s: the id is not unique or does not start with %s", where, prefix)
	}
	seen[c.ID] = true
	if !slices.Contains(dashComponentTypes, c.Type) {
		l.probs.addf("%s: type %q is not one of %v", where, c.Type, dashComponentTypes)
	}
	if strings.TrimSpace(c.Title) == "" {
		l.probs.addf("%s: has no title", where)
	}
	if len(c.Metrics) == 0 {
		l.probs.addf("%s: has no metric", where)
	}
	named := map[string]bool{}
	use := func(role, field string) {
		if kind[field] == "" {
			l.probs.addf("%s: %s %q is neither a column of the query nor a calculated field", where, role, field)
		}
		named[field] = true
	}
	for _, d := range c.Dimensions {
		use("dimension", d)
	}
	roles := map[string]string{}
	for _, m := range c.Metrics {
		use("metric", m.Field)
		if !slices.Contains(dashAggregations, m.Aggregation) {
			l.probs.addf("%s: metric %s has aggregation %q", where, m.Field, m.Aggregation)
		}
		switch k := kind[m.Field]; {
		case k == "agg" && m.Aggregation != "NONE":
			l.probs.addf("%s: %s already aggregates, so its aggregation is NONE, not %s", where, m.Field, m.Aggregation)
		case k != "agg" && m.Aggregation == "NONE":
			l.probs.addf("%s: %s does not aggregate, so NONE would leave it unaggregated", where, m.Field)
		}
		if slices.Contains(dashPercentiles, m.Field) && m.Aggregation != "MAX" {
			l.probs.addf("%s: %s is a percentile per hour and cannot be summed or averaged; use MAX", where, m.Field)
		}
		if m.Role != "" {
			roles[m.Role] = m.Field
		}
	}
	switch c.Type {
	case "scorecard":
		if len(c.Metrics) != 1 || len(c.Dimensions) != 0 {
			l.probs.addf("%s: a scorecard has one metric and no dimension", where)
		}
	case "time_series":
		if len(c.Dimensions) == 0 || c.Dimensions[0] != dateField {
			l.probs.addf("%s: a time series runs over the date field %s", where, dateField)
		}
	case "scatter_chart":
		if roles["x"] == "" || roles["y"] == "" {
			l.probs.addf("%s: a scatter chart needs metrics with the roles x and y", where)
		}
	}
	for role := range roles {
		switch {
		case slices.Contains([]string{"x", "y", "size"}, role) && c.Type != "scatter_chart":
			l.probs.addf("%s: the role %s belongs to a scatter chart", where, role)
		case slices.Contains([]string{"all_reported", "counted_once"}, role) && c.ID != "p4_token_integrity":
			l.probs.addf("%s: the role %s belongs to p4_token_integrity", where, role)
		case !slices.Contains([]string{"x", "y", "size", "all_reported", "counted_once"}, role):
			l.probs.addf("%s: unknown role %q", where, role)
		}
	}
	if c.ID == "p4_token_integrity" && (roles["all_reported"] == "" || roles["counted_once"] == "") {
		l.probs.addf("%s: the token integrity chart compares every reported count with the count of token leaves", where)
	}
	if c.Sort != nil {
		if !named[c.Sort.Field] {
			l.probs.addf("%s: sorts by %s, which the component does not show", where, c.Sort.Field)
		}
		if c.Sort.Direction != "ASC" && c.Sort.Direction != "DESC" {
			l.probs.addf("%s: sort direction %q", where, c.Sort.Direction)
		}
	}
	if c.Limit != nil && *c.Limit <= 0 {
		l.probs.addf("%s: limit %d", where, *c.Limit)
	}
	for _, f := range c.Filters {
		use("filter", f.Field)
		switch {
		case !slices.Contains(dashFilterOps, f.Operator):
			l.probs.addf("%s: filter operator %q is not one of %v", where, f.Operator, dashFilterOps)
		case f.Operator == "IS_TRUE" && f.Value != nil, f.Operator != "IS_TRUE" && f.Value == nil:
			l.probs.addf("%s: the filter %s %s has the wrong value %v", where, f.Field, f.Operator, f.Value)
		}
		if f.Field == "source" && f.Value != "mcp" && f.Value != "gen_ai" {
			l.probs.addf("%s: source is %v, but the query labels its rows mcp and gen_ai", where, f.Value)
		}
		if v, ok := f.Value.(string); ok && f.Field == "alert_kind" && !kinds[v] {
			l.probs.addf("%s: alert_kind %s is not published by any Continuous Queries template", where, v)
		}
	}
	if c.Breakdown != "" {
		use("breakdown", c.Breakdown)
		if c.Type != "time_series" && c.Type != "stacked_bar_chart" {
			l.probs.addf("%s: only time series and stacked bar charts break down", where)
		}
	}
}

var (
	dashTFTablesBlock = regexp.MustCompile(`(?s)looker_studio_data_source_tables = \{\n(.*?)\n  \}`)
	dashTFTableLine   = regexp.MustCompile(`(?m)^\s+(ds\d+)\s*=\s*\[(.*)\]$`)
	dashTFCountLine   = regexp.MustCompile(`(?m)^  count = (.+)$`)
)

// dashTFBlock returns the body of the top-level Terraform block that header
// opens, or "".
func dashTFBlock(text, header string) string {
	m := regexp.MustCompile(`(?s)(?:^|\n)` + regexp.QuoteMeta(header) + ` \{\n(.*?)\n\}`).FindStringSubmatch(text)
	if m == nil {
		return ""
	}
	return m[1]
}

// dashTFLookerNeedles are the pieces of the looker_studio_create_url output
// and its locals that make the URL a working Linking API URL.
var dashTFLookerNeedles = []string{
	`"${var.looker_studio_template_project_id}.${var.looker_studio_template_dataset_id}."`,
	`"${var.project_id}.${google_bigquery_dataset.otel.dataset_id}."`,
	`"project=${var.looker_studio_template_project_id}"`,
	`"project=${var.project_id}"`,
	`urlencode(piece)`,
	`"ds.${alias}.sqlReplace=${local.looker_studio_sql_replace}"`,
	`"ds.${alias}.billingProjectId=${urlencode(var.project_id)}"`,
	`"ds.${alias}.keepDatasourceName=true"`,
	`"ds.${alias}.refreshFields=false"`,
	`"https://lookerstudio.google.com/reporting/create?c.reportId=%s&r.reportName=%s&%s"`,
	`var.looker_studio_template_report_id != "" ? format(`,
	`var.looker_studio_template_project_id != "" && var.looker_studio_template_dataset_id != ""`,
}

func (l *dashLooker) checkTerraform() {
	outputs, mainTF, vars := l.files["deploy/terraform/outputs.tf"], l.files["deploy/terraform/main.tf"], l.files["deploy/terraform/variables.tf"]
	block := dashTFTablesBlock.FindStringSubmatch(outputs)
	if block == nil {
		l.probs.addf("outputs.tf has no looker_studio_data_source_tables local")
		return
	}
	bound := map[string][]string{}
	for _, m := range dashTFTableLine.FindAllStringSubmatch(block[1], -1) {
		bound[m[1]] = strings.Split(m[2], ", ")
	}
	if got, want := dashSortedKeys(bound), dashSortedKeys(dashDataSourcePlan); !slices.Equal(got, want) {
		l.probs.addf("outputs.tf binds the data sources %v, want %v", got, want)
	}
	for _, id := range dashSortedKeys(l.spec.DataSources) {
		src := l.spec.DataSources[id]
		var want []string
		wantCount := ""
		for _, v := range src.Views {
			switch src.TerraformBinding {
			case "always":
				want = append(want, "google_bigquery_table."+v+".table_id")
			case "partition_filter_off":
				want = append(want, "var.require_partition_filter ? null : one(google_bigquery_table."+v+"[*].table_id)")
				wantCount = "(!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0"
			case "enable_continuous_queries":
				want = append(want, "one(google_bigquery_table."+v+"[*].table_id)")
				wantCount = "var.enable_continuous_queries ? 1 : 0"
			}
		}
		if !slices.Equal(bound[id], want) {
			l.probs.addf("outputs.tf binds %s to %v, want %v (terraform_binding %s)", id, bound[id], want, src.TerraformBinding)
		}
		for _, v := range src.Views {
			res := dashTFBlock(mainTF, `resource "google_bigquery_table" "`+v+`"`)
			if res == "" {
				l.probs.addf("main.tf has no table or view %s", v)
				continue
			}
			got := ""
			if m := dashTFCountLine.FindStringSubmatch(res); m != nil {
				got = m[1]
			}
			if got != wantCount {
				l.probs.addf("main.tf creates %s with count %q, but the %s binding of %s needs %q", v, got, src.TerraformBinding, id, wantCount)
			}
		}
	}
	for _, name := range []string{"looker_studio_template_report_id", "looker_studio_template_project_id", "looker_studio_template_dataset_id"} {
		body := dashTFBlock(vars, `variable "`+name+`"`)
		switch {
		case body == "":
			l.probs.addf("variables.tf declares no %s", name)
		case !strings.Contains(body, "\n  default     = \"\"\n"), !strings.Contains(body, "\n  validation {\n"):
			l.probs.addf("variable %s needs the default \"\" (the output is off by default) and a validation block", name)
		}
	}
	out := dashTFBlock(outputs, `output "looker_studio_create_url"`)
	switch {
	case out == "":
		l.probs.addf("outputs.tf declares no looker_studio_create_url")
	case !strings.Contains(out, "precondition {"):
		l.probs.addf("looker_studio_create_url has no precondition for the template project and dataset")
	}
	for _, needle := range dashTFLookerNeedles {
		if !strings.Contains(outputs, needle) {
			l.probs.addf("outputs.tf lacks %s", needle)
		}
	}
}

// ---------------------------------------------------------------------------
// The Grafana dashboards.
// ---------------------------------------------------------------------------

const (
	dashGrafanaBQ   = "dashboards/grafana/genai-and-mcp-overview.json"
	dashGrafanaProm = "dashboards/grafana/collector-self-telemetry.json"
)

type dashGrafanaDS struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type dashGrafanaTarget struct {
	Datasource   *dashGrafanaDS `json:"datasource"`
	EditorMode   string         `json:"editorMode"`
	Format       *int           `json:"format"`
	RawQuery     bool           `json:"rawQuery"`
	RawSQL       string         `json:"rawSql"`
	Expr         string         `json:"expr"`
	LegendFormat string         `json:"legendFormat"`
	Range        bool           `json:"range"`
	RefID        string         `json:"refId"`
}

type dashGrafanaPanel struct {
	ID         int            `json:"id"`
	Type       string         `json:"type"`
	Title      string         `json:"title"`
	Collapsed  bool           `json:"collapsed"`
	Datasource *dashGrafanaDS `json:"datasource"`
	GridPos    struct {
		H int `json:"h"`
		W int `json:"w"`
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"gridPos"`
	Targets     []dashGrafanaTarget `json:"targets"`
	FieldConfig struct {
		Overrides []struct {
			Matcher struct {
				ID      string `json:"id"`
				Options string `json:"options"`
			} `json:"matcher"`
			Properties []struct {
				ID    string          `json:"id"`
				Value json.RawMessage `json:"value"`
			} `json:"properties"`
		} `json:"overrides"`
	} `json:"fieldConfig"`
}

type dashGrafanaVar struct {
	Type             string          `json:"type"`
	Name             string          `json:"name"`
	Hide             int             `json:"hide"`
	Query            json.RawMessage `json:"query"`
	Definition       string          `json:"definition"`
	Datasource       *dashGrafanaDS  `json:"datasource"`
	Multi            bool            `json:"multi"`
	IncludeAll       bool            `json:"includeAll"`
	AllValue         string          `json:"allValue"`
	AllowCustomValue *bool           `json:"allowCustomValue"`
}

type dashGrafana struct {
	UID           string `json:"uid"`
	Title         string `json:"title"`
	SchemaVersion int    `json:"schemaVersion"`
	Inputs        []struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		PluginID string `json:"pluginId"`
		Value    string `json:"value"`
	} `json:"__inputs"`
	Requires []struct {
		Type    string `json:"type"`
		ID      string `json:"id"`
		Version string `json:"version"`
	} `json:"__requires"`
	Templating struct {
		List []dashGrafanaVar `json:"list"`
	} `json:"templating"`
	Panels []dashGrafanaPanel `json:"panels"`
}

// dashDecodeGrafana reads a dashboard model. Keys the structs above do not name
// are ignored: Grafana adds many of its own.
func dashDecodeGrafana(files dashFileSet, path string) (*dashGrafana, dashProblems) {
	raw, ok := files[path]
	if !ok {
		return nil, dashProblems{path + " is missing"}
	}
	var p dashProblems
	if i := dashNonASCII(raw); i >= 0 {
		p.addf("%s: byte %d is not ASCII", path, i)
	}
	var g dashGrafana
	if err := json.Unmarshal([]byte(raw), &g); err != nil {
		p.addf("%s: %v", path, err)
		return nil, p
	}
	return &g, p
}

// dashCheckGrafanaCommon checks what both dashboards share: the identity, the
// inputs that the import form fills, the panel layout, the data source of every
// panel and target, and the plugin list. It returns the title of the row each
// panel sits under, keyed by panel id.
func dashCheckGrafanaCommon(path string, g *dashGrafana, uid, title, plugin, input string, wantInputs []string) (map[int]string, dashProblems) {
	var p dashProblems
	if g.UID != uid || g.Title != title {
		p.addf("%s: uid and title are %q and %q, want %q and %q", path, g.UID, g.Title, uid, title)
	}
	if g.SchemaVersion != 39 {
		p.addf("%s: schemaVersion is %d, want 39 (the dashboard format of Grafana 10.4)", path, g.SchemaVersion)
	}
	var inputs []string
	for _, in := range g.Inputs {
		inputs = append(inputs, in.Name)
		if in.Name == input && (in.Type != "datasource" || in.PluginID != plugin) {
			p.addf("%s: input %s is %q of plugin %q, want a datasource of plugin %q", path, in.Name, in.Type, in.PluginID, plugin)
		}
	}
	if !slices.Equal(inputs, wantInputs) {
		p.addf("%s: __inputs are %v, want %v", path, inputs, wantInputs)
	}
	requires, panelTypes := map[string]bool{}, map[string]bool{}
	for _, r := range g.Requires {
		requires[r.Type+" "+r.ID] = true
		if r.Type == "grafana" && !strings.HasPrefix(r.Version, "10.4") {
			p.addf("%s: __requires Grafana %s, want 10.4", path, r.Version)
		}
	}
	if !requires["grafana grafana"] || !requires["datasource "+plugin] {
		p.addf("%s: __requires lacks Grafana or the %s data source", path, plugin)
	}

	rows := map[int]string{}
	ids := map[int]bool{}
	var order []dashGrafanaPanel
	for _, pn := range g.Panels {
		if ids[pn.ID] {
			p.addf("%s: panel id %d is used twice", path, pn.ID)
		}
		ids[pn.ID] = true
		if strings.TrimSpace(pn.Title) == "" {
			p.addf("%s: panel %d has no title", path, pn.ID)
		}
		gp := pn.GridPos
		if gp.X < 0 || gp.Y < 0 || gp.W < 1 || gp.H < 1 || gp.X+gp.W > 24 {
			p.addf("%s: panel %q has the grid position %+v, outside the 24 columns", path, pn.Title, gp)
		}
		order = append(order, pn)
		if pn.Type == "row" {
			if pn.Collapsed || gp.W != 24 {
				p.addf("%s: row %q has to be open and 24 columns wide", path, pn.Title)
			}
			continue
		}
		panelTypes[pn.Type] = true
		want := dashGrafanaDS{Type: plugin, UID: "${" + input + "}"}
		if pn.Datasource == nil || *pn.Datasource != want {
			p.addf("%s: panel %q has the data source %+v, want %+v", path, pn.Title, pn.Datasource, want)
		}
		if len(pn.Targets) == 0 {
			p.addf("%s: panel %q has no query", path, pn.Title)
		}
		refs := map[string]bool{}
		for _, t := range pn.Targets {
			if t.Datasource == nil || *t.Datasource != want {
				p.addf("%s: a query of panel %q has the data source %+v, want %+v", path, pn.Title, t.Datasource, want)
			}
			if t.RefID == "" || refs[t.RefID] {
				p.addf("%s: panel %q has a query without a refId or with a repeated one", path, pn.Title)
			}
			refs[t.RefID] = true
		}
	}
	for _, t := range dashSortedKeys(panelTypes) {
		if !requires["panel "+t] {
			p.addf("%s: __requires lacks the %s panel the dashboard uses", path, t)
		}
	}
	for key := range requires {
		if typ, id, _ := strings.Cut(key, " "); typ == "panel" && !panelTypes[id] {
			p.addf("%s: __requires lists the %s panel, which no panel uses", path, id)
		}
	}

	slices.SortFunc(order, func(a, b dashGrafanaPanel) int {
		if a.GridPos.Y != b.GridPos.Y {
			return a.GridPos.Y - b.GridPos.Y
		}
		return a.GridPos.X - b.GridPos.X
	})
	row := ""
	for i, a := range order {
		if a.Type == "row" {
			row = a.Title
		} else {
			rows[a.ID] = row
			if row == "" {
				p.addf("%s: panel %q is above the first row", path, a.Title)
			}
		}
		for _, b := range order[i+1:] {
			if a.GridPos.X < b.GridPos.X+b.GridPos.W && b.GridPos.X < a.GridPos.X+a.GridPos.W &&
				a.GridPos.Y < b.GridPos.Y+b.GridPos.H && b.GridPos.Y < a.GridPos.Y+a.GridPos.H {
				p.addf("%s: panels %q and %q overlap", path, a.Title, b.Title)
			}
		}
	}
	return rows, p
}

// The BigQuery dashboard expands Grafana macros before BigQuery sees the query.
// The tests expand them the same way, with fixed values, and parse the result.
const (
	dashGrafanaFrom = "1700000000000"
	dashGrafanaTo   = "1700086400000"
)

var (
	dashGrafanaMacros = strings.NewReplacer(
		"${VAR_PROJECT}", "p",
		"${VAR_DATASET}", "d",
		"${service:json}", `["svc-a","svc-b"]`,
		"${__from}", dashGrafanaFrom,
		"${__to}", dashGrafanaTo,
	)
	dashTimeFilter = regexp.MustCompile(`\$__timeFilter\(([A-Za-z_][A-Za-z0-9_]*)\)`)
	dashRowView    = regexp.MustCompile(`\((\w+)\)$`)

	// dashServiceGuard is the literal that keeps a service name that holds a
	// quote, a backslash, a backtick or a control character out of the list that
	// Grafana pastes into the SQL.
	dashServiceGuard = "r'[\\x00-\\x1f\\'\"\\\\`]'"
)

// dashExpandGrafanaSQL replaces the Grafana macros of a query and refuses any
// variable or macro the tests do not know.
func dashExpandGrafanaSQL(sql string) (string, error) {
	sql = dashGrafanaMacros.Replace(sql)
	sql = dashTimeFilter.ReplaceAllString(sql, "$1 BETWEEN TIMESTAMP_MILLIS("+dashGrafanaFrom+") AND TIMESTAMP_MILLIS("+dashGrafanaTo+")")
	if i := strings.Index(sql, "${"); i >= 0 {
		return "", fmt.Errorf("uses the variable %s, which the dashboard does not define", sql[i:min(i+24, len(sql))])
	}
	if i := strings.Index(sql, "$__"); i >= 0 {
		return "", fmt.Errorf("uses the macro %s, which the tests do not expand", sql[i:min(i+24, len(sql))])
	}
	return sql, nil
}

// dashGrafanaTime says which column of each source a time range applies to, and
// whether it is a DATE.
var dashGrafanaTime = map[string]struct {
	col  string
	date bool
}{
	"v_gen_ai_model_costs": {"usage_hour", false},
	"v_mcp_tool_usage":     {"usage_hour", false},
	"v_gen_ai_sessions":    {"session_start_date", true},
	"otel_alerts":          {"event_time", false},
}

// dashGrafanaTimePredicate is the time predicate of a source in the form the
// WHERE clause is joined by dashJoin.
func dashGrafanaTimePredicate(source string) string {
	t := dashGrafanaTime[source]
	col := strings.ToUpper(t.col)
	if t.date {
		return fmt.Sprintf("%[1]s >= DATE ( TIMESTAMP_MILLIS ( %[2]s ) ) AND %[1]s <= DATE ( TIMESTAMP_MILLIS ( %[3]s ) )", col, dashGrafanaFrom, dashGrafanaTo)
	}
	return fmt.Sprintf("%s BETWEEN TIMESTAMP_MILLIS ( %s ) AND TIMESTAMP_MILLIS ( %s )", col, dashGrafanaFrom, dashGrafanaTo)
}

const (
	dashServicePredicate = "( '__all' IN UNNEST ( ARRAY < STRING > [ \"svc-a\" , \"svc-b\" ] ) OR COALESCE ( SERVICE_NAME , '(none)' ) IN UNNEST ( ARRAY < STRING > [ \"svc-a\" , \"svc-b\" ] ) )"
	dashAlertDedup       = "ROW_NUMBER ( ) OVER ( PARTITION BY ALERT_ID ORDER BY EVENT_TIME ASC ) = 1"
)

// dashCheckGrafanaBranch checks one SELECT of a BigQuery panel or of the
// service variable against the source it reads.
func dashCheckGrafanaBranch(files dashFileSet, b *dashBranch, source string, serviceVar bool) []string {
	var p dashProblems
	if got, want := b.fromTables(), "p.d."+source; len(got) != 1 || got[0] != want {
		p.addf("reads %v, want only `%s`", got, want)
	}
	cols, err := dashSourceColumns(files, source)
	if err != nil {
		return append(p, err.Error())
	}
	p = append(p, dashCheckBranch(b, dashSource{cols: dashSet(cols)})...)
	where := dashJoin(b.clauses["WHERE"])
	timePred := dashGrafanaTimePredicate(source)
	switch {
	case serviceVar:
		guard := "NOT REGEXP_CONTAINS ( COALESCE ( SERVICE_NAME , '' ) , " + dashServiceGuard + " )"
		if !strings.HasPrefix(where, timePred+" AND ") || !strings.Contains(where, guard) {
			p.addf("WHERE is %q, want the time predicate on %s and then the guard that drops service names with a quote, a backslash, a backtick or a control character", where, dashGrafanaTime[source].col)
		}
	case !strings.HasPrefix(where, timePred+" AND "+dashServicePredicate):
		p.addf("WHERE is %q, want it to open with the time predicate on %s (so BigQuery prunes partitions) and the service filter", where, dashGrafanaTime[source].col)
	}
	for i, t := range b.toks {
		if t.kind == dashWord && slices.Contains(dashBannedWords, strings.ToLower(t.text)) {
			p.addf("reads %s, the free text of a span status", t.text)
		}
		if (t.up == "AVG" || t.up == "SUM") && i+2 < len(b.toks) && b.toks[i+1].is("(") && slices.Contains(dashPercentiles, strings.ToLower(b.toks[i+2].text)) {
			p.addf("%s of %s: percentiles cannot be averaged or summed; use MAX", t.up, b.toks[i+2].text)
		}
	}
	joined := dashJoin(b.toks)
	switch source {
	case "otel_alerts":
		dedup := dashJoin(b.clauses["QUALIFY"]) == dashAlertDedup
		distinct := strings.Contains(joined, "COUNT ( DISTINCT ALERT_ID )")
		bare := strings.Contains(joined, "COUNT ( * )") || strings.Contains(joined, "COUNT ( ALERT_ID )")
		if !dedup && (!distinct || bare) {
			p.addf("alerts are delivered at least once: keep the first row of each alert_id (QUALIFY %s) or count DISTINCT alert_id", dashAlertDedup)
		}
	case "v_gen_ai_sessions":
		if strings.Contains(joined, "COUNT ( * )") || strings.Contains(joined, "COUNT ( SESSION_ID )") {
			p.addf("a session that crosses midnight UTC has a row on each date: count DISTINCT session_id")
		}
	}
	return p
}

// dashCheckGrafanaPayload checks the alert payload keys a query reads.
func dashCheckGrafanaPayload(files dashFileSet, toks []dashTok) []string {
	keys, _, _ := dashAlertPayload(files)
	var p dashProblems
	for _, t := range toks {
		if m := dashJSONPathKey.FindStringSubmatch(t.text); t.kind == dashString && m != nil && !keys[m[1]] {
			p.addf("reads the alert payload key %s, which no Continuous Queries template publishes", m[1])
		}
	}
	return p
}

// dashValidateGrafanaBQ runs every rule on the BigQuery dashboard.
func dashValidateGrafanaBQ(files dashFileSet) []string {
	g, p := dashDecodeGrafana(files, dashGrafanaBQ)
	if g == nil {
		return p
	}
	rows, common := dashCheckGrafanaCommon(dashGrafanaBQ, g, "bq-otel-genai-mcp-overview", "GenAI, MCP & Trajectory Analytics",
		"grafana-bigquery-datasource", "DS_BIGQUERY", []string{"DS_BIGQUERY", "VAR_PROJECT", "VAR_DATASET"})
	p = append(p, common...)
	for _, in := range g.Inputs {
		if (in.Name == "VAR_PROJECT" && in.Value != "YOUR_PROJECT_ID") || (in.Name == "VAR_DATASET" && in.Value != "YOUR_DATASET_ID") {
			p.addf("%s: input %s defaults to %q, want the placeholder", dashGrafanaBQ, in.Name, in.Value)
		}
	}
	vars := map[string]dashGrafanaVar{}
	for _, v := range g.Templating.List {
		vars[v.Name] = v
	}
	if got := dashSortedKeys(vars); !slices.Equal(got, []string{"service"}) {
		p.addf("%s: the variables are %v, want only service (project and dataset are __inputs constants so ?var-project= and ?var-dataset= URL parameters cannot override them)", dashGrafanaBQ, got)
	}
	p = append(p, dashCheckServiceVar(files, vars["service"])...)

	views := map[string]bool{}
	for _, pn := range g.Panels {
		if pn.Type != "row" {
			continue
		}
		m := dashRowView.FindStringSubmatch(pn.Title)
		if m == nil || dashGrafanaTime[m[1]].col == "" {
			p.addf("%s: row %q does not end with the source it reads in brackets, one of %v", dashGrafanaBQ, pn.Title, dashSortedKeys(dashGrafanaTime))
			continue
		}
		views[m[1]] = true
	}
	if got := dashSortedKeys(views); !slices.Equal(got, dashSortedKeys(dashGrafanaTime)) {
		p.addf("%s: the rows read %v, want %v", dashGrafanaBQ, got, dashSortedKeys(dashGrafanaTime))
	}
	for _, pn := range g.Panels {
		if pn.Type == "row" {
			continue
		}
		label := fmt.Sprintf("%s: panel %q", dashGrafanaBQ, pn.Title)
		if !slices.Contains([]string{"stat", "timeseries", "barchart", "table"}, pn.Type) {
			p.addf("%s: type %q is not one of the panel types the dashboard declares", label, pn.Type)
		}
		if len(pn.Targets) != 1 {
			continue
		}
		t := pn.Targets[0]
		if t.RefID != "A" || !t.RawQuery || t.EditorMode != "code" {
			p.addf("%s: the query has to be raw SQL in the code editor with refId A", label)
		}
		wantFormat := 1
		if pn.Type == "timeseries" {
			wantFormat = 0
		}
		if t.Format == nil || *t.Format != wantFormat {
			p.addf("%s: a %s panel needs the query format %d (0 is time series, 1 is table)", label, pn.Type, wantFormat)
		}
		source := dashRowView.FindStringSubmatch(rows[pn.ID])
		if source == nil {
			continue
		}
		sql, err := dashExpandGrafanaSQL(t.RawSQL)
		if err != nil {
			p.addf("%s: %v", label, err)
			continue
		}
		q, err := dashParseSQL(sql)
		if err != nil {
			p.addf("%s: %v", label, err)
			continue
		}
		toks, _ := dashTokenize(sql)
		for _, msg := range dashCheckGrafanaPayload(files, toks) {
			p.addf("%s: %s", label, msg)
		}
		for _, b := range q.branches {
			for _, msg := range dashCheckGrafanaBranch(files, b, source[1], false) {
				p.addf("%s: %s", label, msg)
			}
			if pn.Type == "timeseries" && (len(b.items) == 0 || b.items[0].name != "time") {
				p.addf("%s: the first column of a time series has to be named time", label)
			}
		}
		if pn.Type == "table" && source[1] == "otel_alerts" {
			p = append(p, dashCheckTraceLink(label, pn)...)
		}
	}
	return p
}

// dashCheckTraceLink checks that the trace_id column of a table opens Cloud
// Trace Explorer in the project of the dashboard.
func dashCheckTraceLink(label string, pn dashGrafanaPanel) []string {
	const want = "https://console.cloud.google.com/traces/explorer;traceId=${__value.raw}?project=${VAR_PROJECT}"
	for _, o := range pn.FieldConfig.Overrides {
		if o.Matcher.ID != "byName" || o.Matcher.Options != "trace_id" {
			continue
		}
		for _, prop := range o.Properties {
			var links []struct {
				URL string `json:"url"`
			}
			if prop.ID == "links" && json.Unmarshal(prop.Value, &links) == nil && len(links) == 1 && links[0].URL == want {
				return nil
			}
		}
	}
	return []string{label + ": the trace_id column does not link to " + want}
}

// dashCheckServiceVar checks the query that lists the services.
func dashCheckServiceVar(files dashFileSet, v dashGrafanaVar) []string {
	const label = dashGrafanaBQ + ": variable service"
	var p dashProblems
	if v.Type != "query" || !v.Multi || !v.IncludeAll || v.AllValue != "__all" || v.AllowCustomValue == nil || *v.AllowCustomValue {
		p.addf("%s has to be a multi-value query variable with All (allValue __all) and without custom values", label)
	}
	if v.Datasource == nil || *v.Datasource != (dashGrafanaDS{Type: "grafana-bigquery-datasource", UID: "${DS_BIGQUERY}"}) {
		p.addf("%s has the data source %+v", label, v.Datasource)
	}
	var query struct {
		RawSQL     string `json:"rawSql"`
		RefID      string `json:"refId"`
		RawQuery   bool   `json:"rawQuery"`
		EditorMode string `json:"editorMode"`
	}
	if err := json.Unmarshal(v.Query, &query); err != nil || query.RawSQL == "" {
		return append(p, label+" has no query object with rawSql")
	}
	if query.RawSQL != v.Definition {
		p.addf("%s: definition differs from the query it shows", label)
	}
	if query.RefID != "tempvar" || !query.RawQuery || query.EditorMode != "code" {
		p.addf("%s: the query has to be raw SQL in the code editor with refId tempvar", label)
	}
	sql, err := dashExpandGrafanaSQL(query.RawSQL)
	if err != nil {
		return append(p, fmt.Sprintf("%s: %v", label, err))
	}
	q, err := dashParseSQL(sql)
	if err != nil {
		return append(p, fmt.Sprintf("%s: %v", label, err))
	}
	want := []string{"v_gen_ai_model_costs", "v_mcp_tool_usage", "v_gen_ai_sessions"}
	if len(q.branches) != len(want) {
		return append(p, fmt.Sprintf("%s has %d SELECT branches, want one for each of %v", label, len(q.branches), want))
	}
	for i, b := range q.branches {
		for _, msg := range dashCheckGrafanaBranch(files, b, want[i], true) {
			p.addf("%s, branch %d (%s): %s", label, i+1, want[i], msg)
		}
	}
	return p
}

// ---------------------------------------------------------------------------
// The collector dashboard.
//
// It queries the metrics that metadata.yaml declares through a Prometheus data
// source. Prometheus renames an OpenTelemetry metric when it ingests it: a
// counter gets _total, a unit gets a suffix and a histogram gets _bucket. The
// selectors of the dashboard match the name with and without each suffix, and
// the rules below derive the pattern a metric needs from its declaration.
// ---------------------------------------------------------------------------

const dashPromPrefix = "otelcol_exporter_bigquery_"

// dashMetric is a metric that metadata.yaml declares.
type dashMetric struct {
	Unit string `yaml:"unit"`
	Sum  *struct {
		Monotonic bool `yaml:"monotonic"`
	} `yaml:"sum"`
	Gauge      *struct{} `yaml:"gauge"`
	Histogram  *struct{} `yaml:"histogram"`
	Attributes []string  `yaml:"attributes"`
}

type dashMetadata struct {
	Telemetry struct {
		Metrics map[string]dashMetric `yaml:"metrics"`
	} `yaml:"telemetry"`
	Attributes map[string]struct {
		Enum []string `yaml:"enum"`
	} `yaml:"attributes"`
}

func dashDecodeMetadata(files dashFileSet) (*dashMetadata, error) {
	var md dashMetadata
	if err := yaml.Unmarshal([]byte(files["metadata.yaml"]), &md); err != nil {
		return nil, fmt.Errorf("metadata.yaml: %w", err)
	}
	if len(md.Telemetry.Metrics) == 0 {
		return nil, errors.New("metadata.yaml declares no metric")
	}
	return &md, nil
}

// kind is counter, updown, gauge or histogram.
func (m dashMetric) kind() string {
	switch {
	case m.Histogram != nil:
		return "histogram"
	case m.Gauge != nil:
		return "gauge"
	case m.Sum != nil && m.Sum.Monotonic:
		return "counter"
	case m.Sum != nil:
		return "updown"
	}
	return "unknown"
}

// rated says whether a query has to take the rate of the metric: a counter and
// the buckets of a histogram only ever grow.
func (m dashMetric) rated() bool { return m.kind() == "counter" || m.kind() == "histogram" }

// promPattern returns the name pattern that matches the metric however
// Prometheus spells it.
func (m dashMetric) promPattern(name string) string {
	pattern := name
	switch {
	case m.Unit == "By":
		pattern += "(_bytes)?"
	case m.Unit == "s":
		pattern += "(_seconds)?"
	case m.Unit == "1" && m.Gauge != nil:
		pattern += "(_ratio)?"
	}
	if m.kind() == "counter" {
		pattern += "(_total)?"
	}
	if m.Histogram != nil {
		pattern += "_bucket"
	}
	return pattern
}

var (
	dashPromSelector    = regexp.MustCompile(`\{__name__=~"([^"]*)"((?:, *[a-z_]+(?:=|=~|!=|!~)"[^"]*")*)\}`)
	dashPromMatcher     = regexp.MustCompile(`, *([a-z_]+)(=|=~|!=|!~)"([^"]*)"`)
	dashPromGroup       = regexp.MustCompile(`\(_[a-z]+\)\?`)
	dashPromBy          = regexp.MustCompile(`\bby \(([a-z_, ]*)\)`)
	dashPromLegend      = regexp.MustCompile(`\{\{([a-z_]+)\}\}`)
	dashPromShape       = regexp.MustCompile(`^histogram_quantile\((0\.[0-9]+), sum by \(le\) \(rate\(SEL\[\$__rate_interval\]\)\)\)$`)
	dashPromWord        = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	dashPromLabelValues = regexp.MustCompile(`^label_values\(\{__name__=~"([^"]*)"\}, instance\)$`)

	// dashPromRequired are the metrics the acceptance criteria name.
	dashPromRequired = []string{
		"acknowledged_rows", "append_result_wait", "inflight_requests", "backpressure_events",
		"backpressure_level", "effective_request_bytes", "quarantined_rows", "redacted_values", "rejected_rows",
	}
)

// dashPromMetric resolves the name pattern of a selector to the metric it
// stands for. problem is empty when the metric is declared and the pattern is
// the one the declaration needs.
func dashPromMetric(md *dashMetadata, pattern string) (name string, m dashMetric, problem string) {
	name = strings.TrimSuffix(dashPromGroup.ReplaceAllString(pattern, ""), "_bucket")
	m, ok := md.Telemetry.Metrics[name]
	switch want := m.promPattern(name); {
	case !ok:
		problem = fmt.Sprintf("reads %s, which metadata.yaml does not declare", name)
	case pattern != want:
		problem = fmt.Sprintf("selects %q for %s, want %q: Prometheus may add _total, the unit and _bucket to the name", pattern, name, want)
	}
	return name, m, problem
}

// dashPromRead is a metric a query reads and the labels it groups by.
type dashPromRead struct {
	metric string
	by     []string
}

// dashCheckPromExpr checks one query of the collector dashboard.
func dashCheckPromExpr(md *dashMetadata, expr, legend string) ([]dashPromRead, []string) {
	var p dashProblems
	locs := dashPromSelector.FindAllStringSubmatchIndex(expr, -1)
	if len(locs) == 0 {
		return nil, []string{`reads no metric: the query is not built from selectors like {__name__=~"...", instance=~"$instance"}`}
	}
	shape := dashPromSelector.ReplaceAllString(expr, "SEL")
	var by []string
	if m := dashPromBy.FindStringSubmatch(shape); m != nil {
		for _, l := range strings.Split(m[1], ",") {
			if l = strings.TrimSpace(l); l != "" {
				by = append(by, l)
			}
		}
	}
	allowed := map[string]bool{"instance": true}
	var reads []dashPromRead
	histogram := false
	for _, loc := range locs {
		pattern, matchers := expr[loc[2]:loc[3]], expr[loc[4]:loc[5]]
		name, metric, problem := dashPromMetric(md, pattern)
		if problem != "" {
			p.addf("%s", problem)
		}
		if _, declared := md.Telemetry.Metrics[name]; !declared {
			continue
		}
		reads = append(reads, dashPromRead{metric: name, by: by})
		for _, a := range metric.Attributes {
			allowed[a] = true
		}
		hasInstance := false
		for _, mm := range dashPromMatcher.FindAllStringSubmatch(matchers, -1) {
			label, op, value := mm[1], mm[2], mm[3]
			if label == "instance" {
				hasInstance = op == "=~" && value == "$instance"
				continue
			}
			if !slices.Contains(metric.Attributes, label) {
				p.addf("%s is filtered on %s, which is not one of its attributes %v", name, label, metric.Attributes)
				continue
			}
			values := []string{value}
			if op == "=~" || op == "!~" {
				values = strings.Split(value, "|")
			}
			for _, v := range values {
				if enum := md.Attributes[label].Enum; len(enum) > 0 && !slices.Contains(enum, v) {
					p.addf("%s is filtered on %s %s %q, which metadata.yaml does not list for that attribute", name, label, op, v)
				}
			}
		}
		if !hasInstance {
			p.addf(`%s is not filtered on instance=~"$instance"`, name)
		}
		wrapped := strings.HasSuffix(expr[:loc[0]], "rate(") && strings.HasPrefix(expr[loc[1]:], "[$__rate_interval])")
		switch {
		case metric.rated() && !wrapped:
			p.addf("%s is a %s and has to be read as rate(...[$__rate_interval])", name, metric.kind())
		case !metric.rated() && wrapped:
			p.addf("%s is a %s, whose rate() is meaningless: read the metric as it is", name, metric.kind())
		}
		histogram = histogram || metric.Histogram != nil
	}
	switch m := dashPromShape.FindStringSubmatch(shape); {
	case histogram && m == nil:
		p.addf("a histogram is read as histogram_quantile(<q>, sum by (le) (rate(<buckets>[$__rate_interval]))), not as %q", shape)
	case histogram:
		q, _ := strconv.ParseFloat(m[1], 64)
		if want := fmt.Sprintf("p%d", int(math.Round(q*100))); legend != want {
			p.addf("the legend of the quantile %s is %q, want %q", m[1], legend, want)
		}
	case strings.Contains(shape, "histogram_quantile"):
		p.addf("histogram_quantile is applied to something that is not a histogram")
	}
	allowed["le"] = histogram
	for _, l := range by {
		if !allowed[l] {
			p.addf("groups by %s, which none of the metrics it reads has as an attribute", l)
		}
	}
	for _, m := range dashPromLegend.FindAllStringSubmatch(legend, -1) {
		if !slices.Contains(by, m[1]) {
			p.addf("the legend %q uses {{%s}}, which the query does not group by", legend, m[1])
		}
	}
	residual := strings.NewReplacer("SEL", "", "[$__rate_interval]", "").Replace(dashPromBy.ReplaceAllString(shape, ""))
	for _, w := range dashPromWord.FindAllString(residual, -1) {
		if !slices.Contains([]string{"sum", "max", "rate", "histogram_quantile"}, w) {
			p.addf("uses %s, which the collector dashboard does not use", w)
		}
	}
	return reads, p
}

// dashCheckPromVar checks the instance variable.
func dashCheckPromVar(md *dashMetadata, g *dashGrafana) []string {
	const label = dashGrafanaProm + ": variable instance"
	var v *dashGrafanaVar
	for i := range g.Templating.List {
		if g.Templating.List[i].Name == "instance" {
			v = &g.Templating.List[i]
		}
	}
	if v == nil {
		return []string{label + " is missing"}
	}
	var p dashProblems
	if v.Type != "query" || !v.Multi || !v.IncludeAll || v.AllValue != ".*" {
		p.addf(`%s has to be a multi-value query variable whose All value is .*, so that instance=~"$instance" matches every instance`, label)
	}
	if v.Datasource == nil || *v.Datasource != (dashGrafanaDS{Type: "prometheus", UID: "${DS_PROMETHEUS}"}) {
		p.addf("%s has the data source %+v", label, v.Datasource)
	}
	var query string
	if err := json.Unmarshal(v.Query, &query); err != nil {
		return append(p, label+" has no query string")
	}
	if query != v.Definition {
		p.addf("%s: definition differs from the query it shows", label)
	}
	m := dashPromLabelValues.FindStringSubmatch(query)
	if m == nil {
		return append(p, fmt.Sprintf(`%s: the query %q is not label_values({__name__=~"..."}, instance)`, label, query))
	}
	if _, _, problem := dashPromMetric(md, m[1]); problem != "" {
		p.addf("%s: %s", label, problem)
	}
	return p
}

// dashPromMetricsRead returns the metrics the panels of the dashboard read.
func dashPromMetricsRead(g *dashGrafana) map[string]bool {
	read := map[string]bool{}
	for _, pn := range g.Panels {
		for _, t := range pn.Targets {
			for _, m := range dashPromSelector.FindAllStringSubmatch(t.Expr, -1) {
				read[strings.TrimSuffix(dashPromGroup.ReplaceAllString(m[1], ""), "_bucket")] = true
			}
		}
	}
	return read
}

// dashValidateGrafanaProm runs every rule on the collector dashboard.
func dashValidateGrafanaProm(files dashFileSet) []string {
	g, p := dashDecodeGrafana(files, dashGrafanaProm)
	if g == nil {
		return p
	}
	md, err := dashDecodeMetadata(files)
	if err != nil {
		return append(p, err.Error())
	}
	_, common := dashCheckGrafanaCommon(dashGrafanaProm, g, "bq-otel-collector-self-telemetry",
		"Collector BigQuery Exporter Self-Telemetry", "prometheus", "DS_PROMETHEUS", []string{"DS_PROMETHEUS"})
	p = append(p, common...)
	p = append(p, dashCheckPromVar(md, g)...)
	reads := map[string][][]string{}
	for _, pn := range g.Panels {
		if pn.Type == "row" {
			continue
		}
		label := fmt.Sprintf("%s: panel %q", dashGrafanaProm, pn.Title)
		if pn.Type != "stat" && pn.Type != "timeseries" {
			p.addf("%s: type %q is not one of the panel types the dashboard declares", label, pn.Type)
		}
		for _, t := range pn.Targets {
			if t.EditorMode != "code" || !t.Range || t.Expr == "" {
				p.addf("%s: query %s has to be a PromQL expression in the code editor with Range on", label, t.RefID)
			}
			rs, problems := dashCheckPromExpr(md, t.Expr, t.LegendFormat)
			for _, msg := range problems {
				p.addf("%s: query %s: %s", label, t.RefID, msg)
			}
			for _, r := range rs {
				reads[r.metric] = append(reads[r.metric], r.by)
			}
		}
	}
	for _, name := range dashPromRequired {
		if len(reads[dashPromPrefix+name]) == 0 {
			p.addf("%s: no panel reads %s, which the acceptance criteria name", dashGrafanaProm, name)
		}
	}
	for _, fullName := range dashSortedKeys(md.Telemetry.Metrics) {
		if len(reads[fullName]) == 0 {
			p.addf("%s: no panel reads %s, which metadata.yaml declares", dashGrafanaProm, strings.TrimPrefix(fullName, dashPromPrefix))
		}
	}
	if !slices.ContainsFunc(reads[dashPromPrefix+"backpressure_events"], func(by []string) bool { return slices.Contains(by, "trigger") }) {
		p.addf("%s: no panel splits backpressure_events by trigger", dashGrafanaProm)
	}
	return p
}

// ---------------------------------------------------------------------------
// The documents.
// ---------------------------------------------------------------------------

// dashDocFiles are the READMEs that describe what this package ships.
var dashDocFiles = []string{"dashboards/README.md", dashLookerDir + "README.md", "deploy/dataform/README.md"}

var (
	dashMDLink         = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)
	dashMDHeading      = regexp.MustCompile(`^#{1,6} +(.+?) *$`)
	dashBacktickName   = regexp.MustCompile("`([a-z_]+)`")
	dashExactlyOnceRE  = regexp.MustCompile(`(?i)exactly[- ]once`)
	dashNotExactlyOnce = regexp.MustCompile(`(?i)not exactly[- ]once`)
)

// dashSquash collapses the white space of a document, so that a phrase that a
// line break splits is still one phrase.
func dashSquash(text string) string { return strings.Join(strings.Fields(text), " ") }

// dashSlug returns the anchor GitHub gives a heading.
func dashSlug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('-')
		}
	}
	return b.String()
}

// dashProse returns the lines of a Markdown document that are not in a fenced
// code block, keyed by their line number.
func dashProse(text string) map[int]string {
	prose := map[int]string{}
	fenced := false
	for n, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "```"):
			fenced = !fenced
		case !fenced:
			prose[n+1] = line
		}
	}
	return prose
}

// dashAnchors returns the anchors of the headings of a Markdown document.
func dashAnchors(text string) map[string]bool {
	anchors := map[string]bool{}
	for _, line := range dashProse(text) {
		if m := dashMDHeading.FindStringSubmatch(line); m != nil {
			anchors[dashSlug(m[1])] = true
		}
	}
	return anchors
}

// dashCheckLinks checks that every relative link of a document points at a file
// or directory of the repository and that every anchor names a heading.
func dashCheckLinks(files dashFileSet, doc string) []string {
	var p dashProblems
	prose := dashProse(files[doc])
	for _, n := range slices.Sorted(maps.Keys(prose)) {
		for _, m := range dashMDLink.FindAllStringSubmatch(prose[n], -1) {
			target := m[1]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			file, anchor, _ := strings.Cut(target, "#")
			resolved := doc
			if file != "" {
				resolved = path.Join(path.Dir(doc), file)
			}
			_, isFile := files[resolved]
			isDir := slices.ContainsFunc(dashSortedKeys(files), func(f string) bool { return strings.HasPrefix(f, resolved+"/") })
			switch {
			case !isFile && !isDir:
				p.addf("%s:%d: the link %s points at %s, which does not exist", doc, n, target, resolved)
			case anchor != "" && (!isFile || !strings.HasSuffix(resolved, ".md")):
				p.addf("%s:%d: the link %s has an anchor, but %s is not a Markdown file", doc, n, target, resolved)
			case anchor != "" && !dashAnchors(files[resolved])[anchor]:
				p.addf("%s:%d: the link %s points at the anchor #%s, which no heading of %s has", doc, n, target, anchor, resolved)
			}
		}
	}
	return p
}

// dashNeedles reports every phrase that the document does not contain.
func dashNeedles(p *dashProblems, doc, text string, needles ...string) {
	squashed := dashSquash(text)
	for _, n := range needles {
		if !strings.Contains(squashed, n) {
			p.addf("%s lacks %q", doc, n)
		}
	}
}

// dashCheckGrafanaDoc checks the Grafana part of dashboards/README.md against
// the dashboards and metadata.yaml.
func dashCheckGrafanaDoc(files dashFileSet) []string {
	const doc = "dashboards/README.md"
	var p dashProblems
	needles := []string{
		"Grafana 10.4", "grafana-bigquery-datasource", "`roles/bigquery.dataViewer`", "`roles/bigquery.jobUser`",
		"`__inputs`", "`DS_BIGQUERY`", "`DS_PROMETHEUS`", "`VAR_PROJECT`", "`VAR_DATASET`", "at least once",
	}
	for _, source := range dashSortedKeys(dashGrafanaTime) {
		needles = append(needles, "`"+source+"`")
	}
	g, _ := dashDecodeGrafana(files, dashGrafanaProm)
	if g == nil {
		return nil // the dashboard rules report it
	}
	needles = append(needles, fmt.Sprintf("`schemaVersion` %d", g.SchemaVersion))
	dashNeedles(&p, doc, files[doc], needles...)

	lines := strings.Split(files[doc], "\n")
	var documented map[string]bool
	for i, line := range lines {
		if !strings.HasPrefix(line, "| Metric, without the `"+dashPromPrefix+"` prefix |") {
			continue
		}
		documented = map[string]bool{}
		for _, row := range lines[i+2:] {
			if !strings.HasPrefix(row, "|") {
				break
			}
			cell, _, _ := strings.Cut(strings.TrimPrefix(row, "|"), "|")
			for _, m := range dashBacktickName.FindAllStringSubmatch(cell, -1) {
				documented[dashPromPrefix+m[1]] = true
			}
		}
	}
	md, err := dashDecodeMetadata(files)
	switch {
	case documented == nil:
		p.addf("%s has no table of the collector metrics", doc)
	case err != nil:
		p.addf("%v", err)
	default:
		read := dashPromMetricsRead(g)
		for _, name := range dashSortedKeys(documented) {
			if _, ok := md.Telemetry.Metrics[name]; !ok {
				p.addf("%s documents %s, which metadata.yaml does not declare", doc, name)
			}
			if !read[name] {
				p.addf("%s documents %s, which no panel of the collector dashboard reads", doc, name)
			}
		}
		for _, name := range dashSortedKeys(read) {
			if !documented[name] {
				p.addf("%s does not document %s, which a panel reads", doc, name)
			}
		}
	}
	return p
}

// dashCheckLookerDoc checks the Looker Studio README against the report
// specification: one row per data source in the Pages table, and the pieces of
// the setup that the Terraform module and the sharing rules need.
func dashCheckLookerDoc(files dashFileSet) []string {
	const doc = dashLookerDir + "README.md"
	spec, err := dashDecodeSpec(files)
	if err != nil {
		return nil // the Looker Studio rules report it
	}
	var p dashProblems
	lines := strings.Split(files[doc], "\n")
	for _, id := range dashSortedKeys(spec.DataSources) {
		src := spec.DataSources[id]
		row := ""
		for _, line := range lines {
			if strings.Contains(line, "| `"+id+"` |") {
				row = line
			}
		}
		if row == "" {
			p.addf("%s: the Pages table has no row for %s", doc, id)
			continue
		}
		want := []string{"`" + path.Base(src.Query) + "`"}
		for _, v := range src.Views {
			want = append(want, "`"+v+"`")
			if mart := src.Marts[v]; mart != "" {
				want = append(want, "`"+mart+"`")
			}
		}
		if len(src.Marts) == 0 {
			want = append(want, "| none |")
		}
		for _, page := range spec.Pages {
			if page.DataSource == id {
				want = append(want, page.Title)
			}
		}
		switch src.TerraformBinding {
		case "always":
			want = append(want, "| always |")
		case "partition_filter_off":
			want = append(want, "`require_partition_filter` is `false`")
		case "enable_continuous_queries":
			want = append(want, "`enable_continuous_queries` is `true`")
		}
		for _, w := range want {
			if !strings.Contains(row, w) {
				p.addf("%s: the Pages row of %s lacks %q", doc, id, w)
			}
		}
	}
	dashNeedles(&p, doc, files[doc],
		"`looker_studio_create_url`", "looker_studio_template_report_id", "looker_studio_template_project_id", "looker_studio_template_dataset_id",
		"Owner's", "Viewer's", "Service account", "`roles/bigquery.dataViewer`", "`roles/bigquery.jobUser`",
		"`DS_START_DATE`", "`DS_END_DATE`", "`YYYYMMDD`", "at least once")
	return p
}

// dashCheckExactlyOnce rejects every claim of exactly-once delivery: the
// exporter delivers at least once, and the views and marts deduplicate.
func dashCheckExactlyOnce(files dashFileSet) []string {
	var p dashProblems
	for _, name := range dashSortedKeys(files) {
		if !strings.HasPrefix(name, "dashboards/") && !strings.HasPrefix(name, "deploy/dataform/") {
			continue
		}
		text := dashNotExactlyOnce.ReplaceAllString(dashSquash(files[name]), "")
		if m := dashExactlyOnceRE.FindStringIndex(text); m != nil {
			p.addf("%s claims exactly-once processing, but delivery is at least once", name)
		}
	}
	return p
}

// dashValidateDocs runs every rule on the READMEs.
func dashValidateDocs(files dashFileSet) []string {
	var p dashProblems
	for _, doc := range dashDocFiles {
		if _, ok := files[doc]; !ok {
			p.addf("%s is missing", doc)
			continue
		}
		if i := dashNonASCII(files[doc]); i >= 0 {
			p.addf("%s: byte %d is not ASCII", doc, i)
		}
		p = append(p, dashCheckLinks(files, doc)...)
	}
	p = append(p, dashCheckGrafanaDoc(files)...)
	p = append(p, dashCheckLookerDoc(files)...)
	p = append(p, dashCheckExactlyOnce(files)...)
	return p
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

// dashAreas are the groups of rules. Each is a pure function of the files.
var dashAreas = []struct {
	name string
	run  func(dashFileSet) []string
}{
	{"looker_studio", dashValidateLooker},
	{"grafana_bigquery", dashValidateGrafanaBQ},
	{"grafana_prometheus", dashValidateGrafanaProm},
	{"docs", dashValidateDocs},
}

// TestDashboardsConformance holds the dashboards, the report specification and
// their READMEs to the views, the Terraform module and metadata.yaml.
func TestDashboardsConformance(t *testing.T) {
	files := dashLoadFileSet(t)
	for _, area := range dashAreas {
		t.Run(area.name, func(t *testing.T) {
			for _, problem := range area.run(files) {
				t.Error(problem)
			}
		})
	}
}

// dashMutation breaks one file and names the problem that the rules of its
// area have to report for it.
type dashMutation struct {
	area string
	name string
	file string
	edit func(content string) (string, error)
	want string // a piece of the problem
}

// TestDashboardsConformanceDetectsBrokenAssets runs the rules on copies of the
// files that each carry one defect. A rule that cannot fail would let a broken
// dashboard through, so every defect has to produce its own problem and the
// unchanged files must not produce it.
func TestDashboardsConformanceDetectsBrokenAssets(t *testing.T) {
	files := dashLoadFileSet(t)
	runners := map[string]func(dashFileSet) []string{}
	baseline := map[string][]string{}
	for _, area := range dashAreas {
		runners[area.name] = area.run
		baseline[area.name] = area.run(files)
	}
	perArea := map[string]int{}
	seen := map[string]bool{}
	for _, m := range dashMutations {
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
			content, ok := files[m.file]
			if !ok {
				t.Fatalf("%s is not one of the files the rules read", m.file)
			}
			edited, err := m.edit(content)
			if err != nil {
				t.Fatalf("editing %s: %v", m.file, err)
			}
			if edited == content {
				t.Fatalf("the edit left %s as it was", m.file)
			}
			broken := files.clone()
			broken[m.file] = edited
			if problems := run(broken); !containsText(problems, m.want) {
				t.Errorf("the defect was not reported as %q; the problems are:\n  %s", m.want, strings.Join(problems, "\n  "))
			}
		})
	}
	for _, area := range dashAreas {
		if perArea[area.name] < 5 {
			t.Errorf("only %d defects are tried on the %s rules", perArea[area.name], area.name)
		}
	}
}

func containsText(lines []string, want string) bool {
	return slices.ContainsFunc(lines, func(line string) bool { return strings.Contains(line, want) })
}

// dashReplaceFirst edits a file by replacing the first occurrence of from.
func dashReplaceFirst(from, to string) func(string) (string, error) {
	return func(content string) (string, error) {
		if !strings.Contains(content, from) {
			return "", fmt.Errorf("the file does not contain %q", from)
		}
		return strings.Replace(content, from, to, 1), nil
	}
}

// dashReplaceAll edits a file by replacing every occurrence of from.
func dashReplaceAll(from, to string) func(string) (string, error) {
	return func(content string) (string, error) {
		if !strings.Contains(content, from) {
			return "", fmt.Errorf("the file does not contain %q", from)
		}
		return strings.ReplaceAll(content, from, to), nil
	}
}

// dashReplaceAfter replaces the first occurrence of from that follows the first
// occurrence of anchor.
func dashReplaceAfter(anchor, from, to string) func(string) (string, error) {
	return func(content string) (string, error) {
		i := strings.Index(content, anchor)
		if i < 0 {
			return "", fmt.Errorf("the file does not contain %q", anchor)
		}
		j := strings.Index(content[i:], from)
		if j < 0 {
			return "", fmt.Errorf("%q does not follow %q", from, anchor)
		}
		at := i + j
		return content[:at] + to + content[at+len(from):], nil
	}
}

// dashEditLine replaces from with to on the first line that contains marker.
func dashEditLine(marker, from, to string) func(string) (string, error) {
	return func(content string) (string, error) {
		lines := strings.Split(content, "\n")
		for i, line := range lines {
			if strings.Contains(line, marker) {
				if !strings.Contains(line, from) {
					return "", fmt.Errorf("the line %q does not contain %q", line, from)
				}
				lines[i] = strings.Replace(line, from, to, 1)
				return strings.Join(lines, "\n"), nil
			}
		}
		return "", fmt.Errorf("no line contains %q", marker)
	}
}

// dashAppend adds text to the end of a file.
func dashAppend(text string) func(string) (string, error) {
	return func(content string) (string, error) { return content + text, nil }
}

// dashDropComponent removes a component of a page from report_spec.yaml.
func dashDropComponent(id string) func(string) (string, error) {
	return func(content string) (string, error) {
		start := strings.Index(content, "      - id: "+id+"\n")
		if start < 0 {
			return "", fmt.Errorf("the file has no component %s", id)
		}
		end := len(content)
		for _, next := range []string{"\n      - id: ", "\n\n"} {
			if i := strings.Index(content[start+1:], next); i >= 0 && start+1+i+1 < end {
				end = start + 1 + i + 1
			}
		}
		return content[:start] + content[end:], nil
	}
}

// dashEditJSON edits a JSON file as a document and writes it back.
func dashEditJSON(edit func(doc map[string]any) error) func(string) (string, error) {
	return func(content string) (string, error) {
		var doc map[string]any
		if err := json.Unmarshal([]byte(content), &doc); err != nil {
			return "", err
		}
		if err := edit(doc); err != nil {
			return "", err
		}
		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(doc); err != nil {
			return "", err
		}
		return out.String(), nil
	}
}

func dashFindPanel(doc map[string]any, title string) (map[string]any, error) {
	panels, _ := doc["panels"].([]any)
	for _, p := range panels {
		if panel, ok := p.(map[string]any); ok && panel["title"] == title {
			return panel, nil
		}
	}
	return nil, fmt.Errorf("no panel is titled %q", title)
}

func dashEditPanel(title string, edit func(panel map[string]any) error) func(string) (string, error) {
	return dashEditJSON(func(doc map[string]any) error {
		panel, err := dashFindPanel(doc, title)
		if err != nil {
			return err
		}
		return edit(panel)
	})
}

// dashFirstTarget returns the first query of a panel.
func dashFirstTarget(panel map[string]any) (map[string]any, error) {
	targets, _ := panel["targets"].([]any)
	if len(targets) == 0 {
		return nil, fmt.Errorf("panel %q has no query", panel["title"])
	}
	target, _ := targets[0].(map[string]any)
	if target == nil {
		return nil, fmt.Errorf("panel %q has a query that is not an object", panel["title"])
	}
	return target, nil
}

// dashEditTarget replaces from with to in a text field of the first query of a
// panel.
func dashEditTarget(title, key, from, to string) func(string) (string, error) {
	return dashEditPanel(title, func(panel map[string]any) error {
		target, err := dashFirstTarget(panel)
		if err != nil {
			return err
		}
		text, _ := target[key].(string)
		if !strings.Contains(text, from) {
			return fmt.Errorf("the %s of panel %q does not contain %q", key, title, from)
		}
		target[key] = strings.Replace(text, from, to, 1)
		return nil
	})
}

// dashSetTarget sets a field of the first query of a panel.
func dashSetTarget(title, key string, value any) func(string) (string, error) {
	return dashEditPanel(title, func(panel map[string]any) error {
		target, err := dashFirstTarget(panel)
		if err != nil {
			return err
		}
		target[key] = value
		return nil
	})
}

// dashEditServiceVar edits the query of the service variable, and the copy of
// it that Grafana keeps in definition.
func dashEditServiceVar(from, to string) func(string) (string, error) {
	return dashEditJSON(func(doc map[string]any) error {
		templating, _ := doc["templating"].(map[string]any)
		list, _ := templating["list"].([]any)
		for _, item := range list {
			v, _ := item.(map[string]any)
			if v == nil || v["name"] != "service" {
				continue
			}
			query, _ := v["query"].(map[string]any)
			sql, _ := query["rawSql"].(string)
			if query == nil || !strings.Contains(sql, from) {
				return fmt.Errorf("the service variable does not contain %q", from)
			}
			query["rawSql"] = strings.Replace(sql, from, to, 1)
			v["definition"] = query["rawSql"]
			return nil
		}
		return errors.New("the dashboard has no service variable")
	})
}

// dashDropListItem removes the entry of a top-level list of a dashboard (such
// as __inputs or __requires) whose field has the given value.
func dashDropListItem(list, field, value string) func(string) (string, error) {
	return dashEditJSON(func(doc map[string]any) error {
		items, _ := doc[list].([]any)
		var kept []any
		for _, it := range items {
			if item, _ := it.(map[string]any); item == nil || item[field] != value {
				kept = append(kept, it)
			}
		}
		if len(kept) == len(items) {
			return fmt.Errorf("%s has no entry whose %s is %q", list, field, value)
		}
		doc[list] = kept
		return nil
	})
}

// dashSpecFile is the report specification, the file most defects below break.
const dashSpecFile = dashLookerDir + "report_spec.yaml"

var dashMutations = []dashMutation{
	// --- Looker Studio: queries, specification and Terraform ----------------
	{"looker_studio", "a query reads a column that the view lacks", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  total_input_tokens,\n", "  total_input_tokenz,\n"), "unknown identifier total_input_tokenz"},
	{"looker_studio", "a query calls an unknown function", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  total_input_tokens,\n", "  DOES_NOT_EXIST(total_input_tokens) AS total_input_tokens,\n"), "unknown function DOES_NOT_EXIST"},
	{"looker_studio", "a query casts a numeric metric column to STRING", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  total_input_tokens,\n", "  CAST(total_input_tokens AS STRING) AS total_input_tokens,\n"), "select item total_input_tokens has type STRING, want INT64"},
	{"looker_studio", "a query reads the free text of a span status", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  call_count,\n", "  call_count,\n  status_message,\n"), "the free text of a span status"},
	{"looker_studio", "the range end drops the last day", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))", "TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_END_DATE))"),
		"an exclusive end needs DATE_ADD"},
	{"looker_studio", "a query reads another view", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("YOUR_DATASET_ID.v_gen_ai_model_costs`", "YOUR_DATASET_ID.v_gen_ai_sessions`"),
		"want only `YOUR_PROJECT_ID.YOUR_DATASET_ID.v_gen_ai_model_costs`"},
	{"looker_studio", "a query loses the license header", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("Copyright 2026 Google LLC", "Copyright 2025 Google LLC"), "does not open with the license header"},
	{"looker_studio", "a query holds a character that is not ASCII", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("SELECT\n  usage_hour,", "SELECT\n  usage_hour, /* \u00e9 */"), "is not ASCII"},
	{"looker_studio", "the alert query reads a payload key that no template publishes", dashLookerDir + "queries/security_sla_alerts.sql",
		dashReplaceFirst("'$.agent_name'", "'$.agent_nam'"), "reads the alert payload key agent_nam"},
	{"looker_studio", "the alert query stops deduplicating alerts", dashLookerDir + "queries/security_sla_alerts.sql",
		dashReplaceFirst("QUALIFY ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY event_time ASC) = 1\n", ""), "Pub/Sub delivers at least once"},
	{"looker_studio", "the trace link names another project", dashLookerDir + "queries/security_sla_alerts.sql",
		dashReplaceFirst("'?project=YOUR_PROJECT_ID'", "'?project=OTHER'"), "trace_url is not the Cloud Trace explorer link"},
	{"looker_studio", "a formula reads a name that is not declared", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("* input_price_per_million_tokens", "* input_price_per_million_tokenz"), "formula reads input_price_per_million_tokenz"},
	{"looker_studio", "a page shows a field that the query does not output", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("{field: total_input_tokens, aggregation: SUM}]", "{field: total_input_tokenz, aggregation: SUM}]"),
		`metric "total_input_tokenz" is neither a column of the query nor a calculated field`},
	{"looker_studio", "a percentile is averaged", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("{field: p95_duration_ms, aggregation: MAX}", "{field: p95_duration_ms, aggregation: AVG}"),
		"is a percentile per hour and cannot be summed or averaged; use MAX"},
	{"looker_studio", "a calculated rate is aggregated twice", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("{field: error_rate, aggregation: NONE}", "{field: error_rate, aggregation: SUM}"), "already aggregates, so its aggregation is NONE"},
	{"looker_studio", "a page filters on an alert kind that no template publishes", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("value: TOKEN_BUDGET_BURN}]", "value: TOKEN_BUDGET_BURNZ}]"), "alert_kind TOKEN_BUDGET_BURNZ is not published by any Continuous Queries template"},
	{"looker_studio", "a page loses the chart that its acceptance criteria name", dashLookerDir + "report_spec.yaml",
		dashDropComponent("p4_token_integrity"), `page trajectory does not use "role bar_chart all_reported gen_ai_usage_input_tokens"`},
	{"looker_studio", "the specification binds a data source to another condition", dashLookerDir + "report_spec.yaml",
		dashReplaceFirst("terraform_binding: partition_filter_off", "terraform_binding: always"),
		`has terraform_binding "always", want "partition_filter_off"`},
	{"looker_studio", "Terraform binds a data source without the partition filter condition", "deploy/terraform/outputs.tf",
		dashReplaceFirst("[var.require_partition_filter ? null : one(google_bigquery_table.v_gen_ai_agent_trajectory[*].table_id)]",
			"[one(google_bigquery_table.v_gen_ai_agent_trajectory[*].table_id)]"), "outputs.tf binds ds3 to"},
	{"looker_studio", "Terraform creates a view that the data source needs under another condition", "deploy/terraform/main.tf",
		dashReplaceAfter(`resource "google_bigquery_table" "v_gen_ai_agent_trajectory" {`,
			"count = (!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0", "count = 1"),
		`main.tf creates v_gen_ai_agent_trajectory with count "1"`},
	{"looker_studio", "Terraform stops keeping the template's data source names", "deploy/terraform/outputs.tf",
		dashReplaceFirst("keepDatasourceName=true", "keepDatasourceName=false"), "keepDatasourceName=true"},
	{"looker_studio", "a template variable loses its empty default", "deploy/terraform/variables.tf",
		dashReplaceAfter(`variable "looker_studio_template_project_id" {`, "\n  default     = \"\"\n", "\n  default     = \"sandbox\"\n"),
		"variable looker_studio_template_project_id needs the default"},

	// --- Looker Studio: the query files --------------------------------------
	{"looker_studio", "a query reads a parameter that is not a date range parameter", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("PARSE_DATE('%Y%m%d', @DS_START_DATE)", "PARSE_DATE('%Y%m%d', @DS_BEGIN_DATE)"), "@DS_BEGIN_DATE is not a date range parameter"},
	{"looker_studio", "the range start reads another parameter", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("PARSE_DATE('%Y%m%d', @DS_START_DATE)", "PARSE_DATE('%Y%m%d', @DS_BEGIN_DATE)"), "predicate 1 uses @DS_BEGIN_DATE, want @DS_START_DATE"},
	{"looker_studio", "the range start ignores its parameter", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))", "TIMESTAMP(PARSE_DATE('%Y%m%d', '20260101'))"), "predicate 1 does not use @DS_START_DATE"},
	{"looker_studio", "a query loses its date range", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("WHERE usage_hour >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))\n  AND usage_hour < TIMESTAMP(DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY))", ""),
		"has no WHERE clause on usage_hour"},
	{"looker_studio", "a query adds a third predicate", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("@DS_END_DATE), INTERVAL 1 DAY))", "@DS_END_DATE), INTERVAL 1 DAY))\n  AND service_name IS NOT NULL"),
		"WHERE has 3 predicates, want the two date range predicates on usage_hour"},
	{"looker_studio", "a query filters another column on the range start", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("WHERE usage_hour >= TIMESTAMP(", "WHERE service_name >= TIMESTAMP("), "predicate 1 of WHERE is not `usage_hour >= ...`"},
	{"looker_studio", "a DATE column is compared with a timestamp expression", dashLookerDir + "queries/agent_sessions.sql",
		dashReplaceFirst("WHERE session_start_date >= PARSE_DATE('%Y%m%d', @DS_START_DATE)", "WHERE session_start_date >= DATE(PARSE_DATE('%Y%m%d', @DS_START_DATE))"),
		"compares the DATE column session_start_date with something that is not PARSE_DATE(...)"},
	{"looker_studio", "a TIMESTAMP column is compared with a date", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("WHERE usage_hour >= TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))", "WHERE usage_hour >= PARSE_DATE('%Y%m%d', @DS_START_DATE)"),
		"compares the TIMESTAMP column usage_hour with something that is not TIMESTAMP(...)"},
	{"looker_studio", "the range start parses another date format", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("TIMESTAMP(PARSE_DATE('%Y%m%d', @DS_START_DATE))", "TIMESTAMP(PARSE_DATE('%Y-%m-%d', @DS_START_DATE))"),
		"predicate 1 does not parse the parameter with '%Y%m%d'"},
	{"looker_studio", "an inclusive end adds a day", dashLookerDir + "queries/agent_sessions.sql",
		dashReplaceFirst("AND session_start_date <= PARSE_DATE('%Y%m%d', @DS_END_DATE)", "AND session_start_date <= DATE_ADD(PARSE_DATE('%Y%m%d', @DS_END_DATE), INTERVAL 1 DAY)"),
		"an inclusive end is for a DATE column and takes no DATE_ADD"},
	{"looker_studio", "a query no longer outputs its date field", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("SELECT\n  usage_hour,\n", "SELECT\n  usage_hour AS usage_time,\n"), "outputs no usage_hour, the date field of ds0"},
	{"looker_studio", "a select item has no name", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  call_count,\n  error_count,\n", "  call_count + 1,\n  error_count,\n"), "a select item has no name; add an alias"},
	{"looker_studio", "a query outputs a column twice", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("  call_count,\n  error_count,\n", "  call_count,\n  call_count,\n  error_count,\n"), "outputs call_count twice"},
	{"looker_studio", "the branches of a union output different columns", dashLookerDir + "queries/mcp_tool_usage.sql",
		dashReplaceFirst("  CAST(NULL AS FLOAT64) AS avg_duration_ms,\n", ""), "but the first branch outputs"},
	{"looker_studio", "a query holds a string that does not end", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("SELECT\n  usage_hour,", "SELECT\n  'usage_hour,"), "line break in string"},
	{"looker_studio", "a query has an empty select item", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("SELECT\n  usage_hour,", "SELECT\n  usage_hour,,"), "empty select item"},
	{"looker_studio", "a query selects a star", dashLookerDir + "queries/executive_summary.sql",
		dashReplaceFirst("SELECT\n  usage_hour,\n", "SELECT\n  *,\n  usage_hour,\n"), "it is not a WITH table"},

	// --- Looker Studio: the Continuous Queries templates the alerts read ------
	{"looker_studio", "a Continuous Queries template publishes no alert fields", "deploy/continuous-queries/token_budget_burn.sql",
		dashReplaceAll("STRUCT(", "STRUCTX("), "token_budget_burn.sql: no STRUCT of alert fields"},
	{"looker_studio", "a Continuous Queries template is not SQL", "deploy/continuous-queries/tool_error_rate_alert.sql",
		dashAppend("\n'unterminated\n"), "tool_error_rate_alert.sql: line break in string"},
	{"looker_studio", "the Continuous Queries README stops documenting the error spans", "deploy/continuous-queries/README.md",
		dashReplaceAll("COUNT(*) AS error_spans", "COUNT(*) AS spans_in_error"), "no longer documents error_spans"},

	// --- Looker Studio: the report specification -----------------------------
	{"looker_studio", "the specification changes its version", dashSpecFile,
		dashReplaceFirst("spec_version: 1\n", "spec_version: 2\n"), "spec_version is 2, want 1"},
	{"looker_studio", "the report says another page count", dashSpecFile,
		dashReplaceFirst("  page_count: 6\n", "  page_count: 7\n"), "says page_count 7, want 6"},
	{"looker_studio", "a data source is renamed", dashSpecFile,
		dashReplaceFirst("  ds5:\n    name: Metric exemplars\n", "  ds6:\n    name: Metric exemplars\n"), "data sources are [ds0 ds1 ds2 ds3 ds4 ds6], want [ds0 ds1 ds2 ds3 ds4 ds5]"},
	{"looker_studio", "the placeholders name a real project", dashSpecFile,
		dashReplaceFirst("  project: YOUR_PROJECT_ID\n", "  project: my-project\n"), `placeholders are "my-project" and "YOUR_DATASET_ID"`},
	{"looker_studio", "the date range stops being inclusive", dashSpecFile,
		dashReplaceFirst("  inclusive: true\n", "  inclusive: false\n"), "in YYYYMMDD with both ends inclusive"},
	{"looker_studio", "two report parameters share a name", dashSpecFile,
		dashReplaceFirst("  - name: output_price_per_million_tokens\n", "  - name: input_price_per_million_tokens\n"), "report parameter input_price_per_million_tokens is declared twice"},
	{"looker_studio", "a report parameter is not a number", dashSpecFile,
		dashReplaceFirst("    type: NUMBER\n    default: 0\n    description: Price of one million input", "    type: TEXT\n    default: 0\n    description: Price of one million input"),
		`report parameter input_price_per_million_tokens has type "TEXT", want NUMBER`},
	{"looker_studio", "a report parameter ships a price", dashSpecFile,
		dashReplaceFirst("    default: 0\n    description: Price of one million input", "    default: 3\n    description: Price of one million input"),
		"report parameter input_price_per_million_tokens defaults to 3: the template ships no prices"},
	{"looker_studio", "a data source reads another query file", dashSpecFile,
		dashReplaceFirst("    query: queries/agent_sessions.sql\n", "    query: queries/sessions.sql\n"), "ds1 reads queries/sessions.sql, want queries/agent_sessions.sql"},
	{"looker_studio", "a data source names a query file that does not exist", dashSpecFile,
		dashReplaceFirst("    query: queries/agent_sessions.sql\n", "    query: queries/sessions.sql\n"), "ds1: query file dashboards/looker_studio/queries/sessions.sql does not exist"},
	{"looker_studio", "a query file belongs to no data source", dashSpecFile,
		dashReplaceFirst("    query: queries/agent_sessions.sql\n", "    query: queries/sessions.sql\n"), "agent_sessions.sql: no data source of report_spec.yaml uses this query"},
	{"looker_studio", "a data source reads its views in another order", dashSpecFile,
		dashReplaceFirst("    views: [v_mcp_tool_usage, v_gen_ai_tool_usage]\n", "    views: [v_gen_ai_tool_usage, v_mcp_tool_usage]\n"),
		"ds2 reads views [v_gen_ai_tool_usage v_mcp_tool_usage], want [v_mcp_tool_usage v_gen_ai_tool_usage]"},
	{"looker_studio", "a data source drops a view and the query keeps its branch", dashSpecFile,
		dashReplaceFirst("    views: [v_mcp_tool_usage, v_gen_ai_tool_usage]\n", "    views: [v_mcp_tool_usage]\n"), "has 2 SELECT branches for the 1 views of ds2"},
	{"looker_studio", "a mart is named for a view the data source does not read", dashSpecFile,
		dashReplaceFirst("    marts: {}\n", "    marts:\n      v_mcp_tool_usage: m_mcp_tool_usage_hourly\n"), "ds4 names a mart for v_mcp_tool_usage, which it does not read"},
	{"looker_studio", "a data source names another mart", dashSpecFile,
		dashReplaceFirst("      v_gen_ai_sessions: m_gen_ai_sessions_daily\n", "      v_gen_ai_sessions: m_gen_ai_sessions_hourly\n"),
		"ds1 names the mart m_gen_ai_sessions_hourly for v_gen_ai_sessions, want m_gen_ai_sessions_daily"},
	{"looker_studio", "a data source names a mart that Dataform does not define", dashSpecFile,
		dashReplaceFirst("      v_gen_ai_sessions: m_gen_ai_sessions_daily\n", "      v_gen_ai_sessions: m_gen_ai_sessions_hourly\n"),
		"ds1: the mart m_gen_ai_sessions_hourly has no definition in deploy/dataform"},
	{"looker_studio", "the specification stops naming the mart of a view", dashSpecFile,
		dashReplaceFirst("    marts:\n      v_gen_ai_model_costs: m_gen_ai_model_costs_hourly\n", "    marts: {}\n"),
		"ds0: view v_gen_ai_model_costs has a Dataform mart (m_gen_ai_model_costs_hourly) that the spec does not name"},
	{"looker_studio", "a calculated field repeats a column", dashSpecFile,
		dashReplaceFirst("      - name: estimated_cost\n", "      - name: call_count\n"), "ds0: calculated field call_count repeats another field"},
	{"looker_studio", "a calculated field has a type that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("        formula: SUM(error_count) / SUM(call_count)\n        type: PERCENT\n", "        formula: SUM(error_count) / SUM(call_count)\n        type: RATIO\n"),
		`ds0: calculated field error_rate has type "RATIO"`},
	{"looker_studio", "a formula uses a function the report does not use", dashSpecFile,
		dashReplaceFirst("        formula: SUM(error_count) / SUM(call_count)\n", "        formula: SUMX(error_count) / SUM(call_count)\n"),
		"formula uses SUMX, which is not a function or keyword the report formulas use"},
	{"looker_studio", "uses lists a column that the formula does not read", dashSpecFile,
		dashReplaceFirst("        uses: [total_input_tokens, total_output_tokens]\n        parameters:", "        uses: [total_input_tokens, total_output_tokens, call_count]\n        parameters:"),
		"uses lists call_count, which the formula does not read"},
	{"looker_studio", "parameters lists a name that is not a report parameter", dashSpecFile,
		dashReplaceFirst("  - name: output_price_per_million_tokens\n", "  - name: output_price_per_million_token\n"),
		"parameters lists output_price_per_million_tokens, which is not a report parameter"},
	{"looker_studio", "parameters lists a parameter that the formula does not read", dashSpecFile,
		dashReplaceFirst("* output_price_per_million_tokens) / 1000000", "* 2) / 1000000"),
		"parameters lists output_price_per_million_tokens, which the formula does not read"},
	{"looker_studio", "field_types names something that is not a field", dashSpecFile,
		dashReplaceFirst("    field_types:\n      error_rate: PERCENT\n", "    field_types:\n      error_rates: PERCENT\n"), "field_types names error_rates, which is not a field"},
	{"looker_studio", "field_types gives a field a type that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("    field_types:\n      trace_url: URL\n", "    field_types:\n      trace_url: LINK\n"), `field trace_url has type "LINK"`},

	// --- Looker Studio: pages, controls and components -----------------------
	{"looker_studio", "a page swaps its id", dashSpecFile,
		dashReplaceFirst("  - id: agent_sessions\n    title:", "  - id: agent_session\n    title:"), `page 2 is "agent_session" on ds1, want "agent_sessions" on ds1`},
	{"looker_studio", "a page loses its title", dashSpecFile,
		dashReplaceFirst("    title: Agent Sessions and Multi-Turn Engagement\n", "    title: \"\"\n"), "page agent_sessions has no title"},
	{"looker_studio", "a control reuses the id of another page", dashSpecFile,
		dashReplaceFirst("      - {id: p2_service, type: dropdown, field: service_name}\n", "      - {id: p1_service, type: dropdown, field: service_name}\n"),
		`control id "p1_service" is not unique or does not start with p2_`},
	{"looker_studio", "the date range control names a field", dashSpecFile,
		dashReplaceFirst("      - {id: p2_date_range, type: date_range}\n", "      - {id: p2_date_range, type: date_range, field: session_start_date}\n"),
		"the date range control p2_date_range names a field"},
	{"looker_studio", "a dropdown filters a field that is not a column", dashSpecFile,
		dashReplaceFirst("      - {id: p2_agent, type: dropdown, field: agent_name}\n", "      - {id: p2_agent, type: dropdown, field: agent_nam}\n"),
		`control p2_agent filters "agent_nam", which is not a column of the data source`},
	{"looker_studio", "a control has a type that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("      - {id: p2_agent, type: dropdown, field: agent_name}\n", "      - {id: p2_agent, type: slider, field: agent_name}\n"), `control p2_agent has type "slider"`},
	{"looker_studio", "a page loses its date range control", dashSpecFile,
		dashReplaceFirst("      - {id: p2_date_range, type: date_range}\n", ""), "page agent_sessions has no date range control, so the query parameters get no value"},
	{"looker_studio", "a component reuses an id", dashSpecFile,
		dashReplaceFirst("      - id: p2_active_users\n", "      - id: p2_sessions\n"), "component p2_sessions: the id is not unique or does not start with p2_"},
	{"looker_studio", "a component has a type that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("      - id: p2_sessions\n        type: scorecard\n", "      - id: p2_sessions\n        type: gauge\n"), `component p2_sessions: type "gauge" is not one of`},
	{"looker_studio", "a component loses its title", dashSpecFile,
		dashReplaceFirst("        title: Sessions\n", "        title: \"\"\n"), "component p2_sessions: has no title"},
	{"looker_studio", "a component loses its metrics", dashSpecFile,
		dashReplaceAfter("        title: Sessions\n", "        metrics: [{field: session_id, aggregation: COUNT_DISTINCT}]\n", "        metrics: []\n"), "component p2_sessions: has no metric"},
	{"looker_studio", "a metric has an aggregation that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("        metrics: [{field: call_count, aggregation: SUM}]\n", "        metrics: [{field: call_count, aggregation: MEDIAN}]\n"),
		`metric call_count has aggregation "MEDIAN"`},
	{"looker_studio", "a column is shown with no aggregation", dashSpecFile,
		dashReplaceFirst("        metrics: [{field: call_count, aggregation: SUM}]\n", "        metrics: [{field: call_count, aggregation: NONE}]\n"),
		"call_count does not aggregate, so NONE would leave it unaggregated"},
	{"looker_studio", "a scorecard gets a dimension", dashSpecFile,
		dashReplaceFirst("        title: LLM calls\n", "        title: LLM calls\n        dimensions: [model]\n"), "component p1_llm_calls: a scorecard has one metric and no dimension"},
	{"looker_studio", "a time series leaves the date field", dashSpecFile,
		dashReplaceFirst("        title: Token burn by hour\n        dimensions: [usage_hour]\n", "        title: Token burn by hour\n        dimensions: [model]\n"),
		"a time series runs over the date field usage_hour"},
	{"looker_studio", "a scatter chart loses its x axis", dashSpecFile,
		dashReplaceFirst("{field: session_duration_ms, aggregation: SUM, role: x}", "{field: session_duration_ms, aggregation: SUM}"), "a scatter chart needs metrics with the roles x and y"},
	{"looker_studio", "a bar chart gets a scatter role", dashSpecFile,
		dashReplaceAfter("        title: LLM calls by provider\n", "{field: call_count, aggregation: SUM}", "{field: call_count, aggregation: SUM, role: x}"),
		"component p1_calls_by_provider: the role x belongs to a scatter chart"},
	{"looker_studio", "a chart gets a token integrity role", dashSpecFile,
		dashReplaceAfter("        title: LLM calls by provider\n", "{field: call_count, aggregation: SUM}", "{field: call_count, aggregation: SUM, role: all_reported}"),
		"the role all_reported belongs to p4_token_integrity"},
	{"looker_studio", "a metric has a role that does not exist", dashSpecFile,
		dashReplaceAfter("        title: LLM calls by provider\n", "{field: call_count, aggregation: SUM}", "{field: call_count, aggregation: SUM, role: median}"), `unknown role "median"`},
	{"looker_studio", "the token integrity chart loses a role", dashSpecFile,
		dashReplaceFirst(", role: counted_once}", "}"), "the token integrity chart compares every reported count with the count of token leaves"},
	{"looker_studio", "a component sorts by a field it does not show", dashSpecFile,
		dashReplaceFirst("sort: {field: total_input_tokens, direction: DESC}", "sort: {field: call_count, direction: DESC}"),
		"component p1_tokens_by_model: sorts by call_count, which the component does not show"},
	{"looker_studio", "a component sorts in an unknown direction", dashSpecFile,
		dashReplaceFirst("sort: {field: total_input_tokens, direction: DESC}", "sort: {field: total_input_tokens, direction: DOWN}"), `sort direction "DOWN"`},
	{"looker_studio", "a component limits its rows to nothing", dashSpecFile,
		dashReplaceFirst("        limit: 15\n", "        limit: 0\n"), "component p1_tokens_by_model: limit 0"},
	{"looker_studio", "a filter has an operator that Looker Studio lacks", dashSpecFile,
		dashReplaceFirst("operator: GREATER_THAN, value: 0}", "operator: MORE_THAN, value: 0}"), `filter operator "MORE_THAN" is not one of`},
	{"looker_studio", "a comparison filter has no value", dashSpecFile,
		dashReplaceFirst("{field: error_count, operator: GREATER_THAN, value: 0}", "{field: error_count, operator: GREATER_THAN}"),
		"the filter error_count GREATER_THAN has the wrong value <nil>"},
	{"looker_studio", "a source filter names a source the query does not label", dashSpecFile,
		dashReplaceFirst("{field: source, operator: EQUALS, value: mcp}", "{field: source, operator: EQUALS, value: tool}"),
		"source is tool, but the query labels its rows mcp and gen_ai"},
	{"looker_studio", "a table breaks down", dashSpecFile,
		dashReplaceFirst("        title: Model ledger\n", "        title: Model ledger\n        breakdown: model\n"), "only time series and stacked bar charts break down"},

	// --- Looker Studio: the Terraform that binds the data sources -------------
	{"looker_studio", "outputs.tf loses the table of data sources", "deploy/terraform/outputs.tf",
		dashReplaceFirst("  looker_studio_data_source_tables = {\n", "  looker_studio_data_source_table = {\n"), "outputs.tf has no looker_studio_data_source_tables local"},
	{"looker_studio", "outputs.tf binds another set of data sources", "deploy/terraform/outputs.tf",
		dashReplaceFirst("    ds5 = [", "    ds6 = ["), "outputs.tf binds the data sources [ds0 ds1 ds2 ds3 ds4 ds6], want [ds0 ds1 ds2 ds3 ds4 ds5]"},
	{"looker_studio", "main.tf loses a view that a data source reads", "deploy/terraform/main.tf",
		dashReplaceFirst(`resource "google_bigquery_table" "v_gen_ai_metric_exemplars" {`, `resource "google_bigquery_table" "v_gen_ai_metric_exemplar" {`),
		"main.tf has no table or view v_gen_ai_metric_exemplars"},
	{"looker_studio", "variables.tf loses a template variable", "deploy/terraform/variables.tf",
		dashReplaceFirst(`variable "looker_studio_template_report_id" {`, `variable "looker_studio_template_report_ident" {`), "variables.tf declares no looker_studio_template_report_id"},
	{"looker_studio", "outputs.tf loses the create URL", "deploy/terraform/outputs.tf",
		dashReplaceFirst(`output "looker_studio_create_url" {`, `output "looker_studio_create_urls" {`), "outputs.tf declares no looker_studio_create_url"},
	{"looker_studio", "the create URL loses its precondition", "deploy/terraform/outputs.tf",
		dashReplaceAfter(`output "looker_studio_create_url" {`, "precondition {", "postcondition {"),
		"looker_studio_create_url has no precondition for the template project and dataset"},

	// --- Grafana, BigQuery: what both dashboards share, on this one -----------
	{"grafana_bigquery", "a panel reads a column that the view lacks", dashGrafanaBQ,
		dashEditTarget("Error rate", "rawSql", "SUM(error_count)", "SUM(error_countz)"), "unknown identifier error_countz"},
	{"grafana_bigquery", "a panel loses the time predicate", dashGrafanaBQ,
		dashEditTarget("LLM calls", "rawSql", "$__timeFilter(usage_hour)", "usage_hour IS NOT NULL"), "want it to open with the time predicate on usage_hour"},
	{"grafana_bigquery", "a panel reads another view", dashGrafanaBQ,
		dashEditTarget("LLM calls", "rawSql", "v_gen_ai_model_costs", "v_mcp_tool_usage"), "want only `p.d.v_gen_ai_model_costs`"},
	{"grafana_bigquery", "a panel uses a variable that is not defined", dashGrafanaBQ,
		dashEditTarget("LLM calls", "rawSql", "${VAR_PROJECT}", "${VAR_PROJEKT}"), "which the dashboard does not define"},
	{"grafana_bigquery", "the dashboard exposes project as a template variable", dashGrafanaBQ,
		dashEditJSON(func(doc map[string]any) error {
			templating, _ := doc["templating"].(map[string]any)
			list, _ := templating["list"].([]any)
			templating["list"] = append(list, map[string]any{"name": "project", "type": "constant", "hide": 2, "query": "${VAR_PROJECT}"})
			return nil
		}), "want only service"},
	{"grafana_bigquery", "the service variable loses its __all sentinel", dashGrafanaBQ,
		dashEditJSON(func(doc map[string]any) error {
			templating, _ := doc["templating"].(map[string]any)
			list, _ := templating["list"].([]any)
			for _, item := range list {
				if v, _ := item.(map[string]any); v != nil && v["name"] == "service" {
					v["allValue"] = ""
					return nil
				}
			}
			return errors.New("no service variable")
		}), "allValue __all"},
	{"grafana_bigquery", "a panel holds a character that is not ASCII", dashGrafanaBQ,
		dashEditTarget("LLM calls", "rawSql", "SELECT ", "SELECT /* \u00e9 */ "), "is not ASCII"},
	{"grafana_bigquery", "a time series names its first column something else", dashGrafanaBQ,
		dashEditTarget("Token burn by hour", "rawSql", "usage_hour AS time", "usage_hour AS ts"), "the first column of a time series has to be named time"},
	{"grafana_bigquery", "a stat panel asks for a time series", dashGrafanaBQ,
		dashSetTarget("LLM calls", "format", 0), "needs the query format 1"},
	{"grafana_bigquery", "a panel has a data source that is not an input", dashGrafanaBQ,
		dashEditPanel("LLM calls", func(panel map[string]any) error {
			panel["datasource"] = map[string]any{"type": "grafana-bigquery-datasource", "uid": "ds-prod"}
			return nil
		}), `panel "LLM calls" has the data source`},
	{"grafana_bigquery", "two panels overlap", dashGrafanaBQ,
		dashEditPanel("Error rate", func(panel map[string]any) error {
			panel["gridPos"] = map[string]any{"h": 4, "w": 4, "x": 0, "y": 1}
			return nil
		}), `panels "LLM calls" and "Error rate" overlap`},
	{"grafana_bigquery", "the plugin list forgets a panel type", dashGrafanaBQ,
		dashDropListItem("__requires", "id", "barchart"), "__requires lacks the barchart panel"},
	{"grafana_bigquery", "the service list stops dropping names that break the SQL", dashGrafanaBQ,
		dashEditServiceVar("REGEXP_CONTAINS(COALESCE(service_name, ''), "+dashServiceGuard+")", "FALSE"),
		"want the time predicate on usage_hour and then the guard"},
	{"grafana_bigquery", "a panel reads the free text of a span status", dashGrafanaBQ,
		dashEditTarget("Session error ledger", "rawSql", "session_id,", "session_id, status_message,"), "the free text of a span status"},
	{"grafana_bigquery", "a percentile is averaged", dashGrafanaBQ,
		dashEditTarget("Worst p95 latency by model and hour", "rawSql", "MAX(p95_duration_ms)", "AVG(p95_duration_ms)"),
		"percentiles cannot be averaged or summed"},
	{"grafana_bigquery", "alerts are counted without deduplication", dashGrafanaBQ,
		dashEditTarget("Alerts", "rawSql", "COUNT(DISTINCT alert_id)", "COUNT(*)"), "alerts are delivered at least once"},
	{"grafana_bigquery", "the alert log reads a payload key that no template publishes", dashGrafanaBQ,
		dashEditTarget("Alert log", "rawSql", "'$.agent_name'", "'$.agent_nam'"), "reads the alert payload key agent_nam"},
	{"grafana_bigquery", "the alert log stops linking to Cloud Trace", dashGrafanaBQ,
		dashEditPanel("Alert log", func(panel map[string]any) error {
			config, _ := panel["fieldConfig"].(map[string]any)
			overrides, _ := config["overrides"].([]any)
			for _, o := range overrides {
				override, _ := o.(map[string]any)
				matcher, _ := override["matcher"].(map[string]any)
				if matcher != nil && matcher["options"] == "trace_id" {
					matcher["options"] = "trace_idx"
					return nil
				}
			}
			return errors.New("the alert log has no override for trace_id")
		}), "does not link to"},
	{"grafana_bigquery", "sessions are counted per row", dashGrafanaBQ,
		dashEditTarget("Sessions and active users", "rawSql", "COUNT(DISTINCT session_id)", "COUNT(session_id)"),
		"a session that crosses midnight UTC has a row on each date"},

	// --- Grafana, Prometheus: the collector dashboard against metadata.yaml --
	{"grafana_prometheus", "a counter is read without rate", dashGrafanaProm,
		dashSetTarget("Rejected rows/s", "expr", `sum({__name__=~"otelcol_exporter_bigquery_rejected_rows(_total)?", instance=~"$instance"})`),
		"rejected_rows is a counter and has to be read as rate(...[$__rate_interval])"},
	{"grafana_prometheus", "a gauge is read with rate", dashGrafanaProm,
		dashSetTarget("In-flight requests", "expr", `sum by (instance) (rate({__name__=~"otelcol_exporter_bigquery_inflight_requests", instance=~"$instance"}[$__rate_interval]))`),
		"whose rate() is meaningless"},
	{"grafana_prometheus", "a query reads a metric that metadata.yaml does not declare", dashGrafanaProm,
		dashEditTarget("Acknowledged rows/s", "expr", "acknowledged_rows(_total)?", "acknowledged_row(_total)?"),
		"acknowledged_row, which metadata.yaml does not declare"},
	{"grafana_prometheus", "a query does not match the name Prometheus gives a metric", dashGrafanaProm,
		dashEditTarget("Effective request bytes", "expr", "effective_request_bytes(_bytes)?", "effective_request_bytes"),
		"Prometheus may add _total, the unit and _bucket to the name"},
	{"grafana_prometheus", "a query ignores the instance variable", dashGrafanaProm,
		dashEditTarget("Rejected rows/s", "expr", `, instance=~"$instance"`, ""), `is not filtered on instance=~"$instance"`},
	{"grafana_prometheus", "a query groups by a label that the metric lacks", dashGrafanaProm,
		dashEditTarget("Acknowledged rows per second", "expr", "sum by (instance)", "sum by (trigger)"),
		"groups by trigger, which none of the metrics it reads has as an attribute"},
	{"grafana_prometheus", "a query filters on a value outside the enum", dashGrafanaProm,
		dashEditTarget("Backpressure events per second by trigger", "expr", `instance=~"$instance"}`, `instance=~"$instance", trigger="slowdown"}`),
		"which metadata.yaml does not list for that attribute"},
	{"grafana_prometheus", "a legend uses a label that the query dropped", dashGrafanaProm,
		dashSetTarget("Acknowledged rows per second", "legendFormat", "{{trigger}}"), "uses {{trigger}}, which the query does not group by"},
	{"grafana_prometheus", "a histogram quantile gets the wrong legend", dashGrafanaProm,
		dashSetTarget("Append result wait", "legendFormat", "p90"), `the legend of the quantile 0.5 is "p90", want "p50"`},
	{"grafana_prometheus", "the backpressure panel stops splitting by trigger", dashGrafanaProm,
		dashEditTarget("Backpressure events per second by trigger", "expr", "sum by (trigger)", "sum by (instance)"),
		"no panel splits backpressure_events by trigger"},
	{"grafana_prometheus", "no panel shows the backpressure level", dashGrafanaProm,
		dashEditJSON(func(doc map[string]any) error {
			panels, _ := doc["panels"].([]any)
			var kept []any
			for _, p := range panels {
				if panel, _ := p.(map[string]any); panel == nil || panel["title"] != "Backpressure level" {
					kept = append(kept, p)
				}
			}
			if len(kept) == len(panels) {
				return errors.New("the dashboard has no Backpressure level panel")
			}
			doc["panels"] = kept
			return nil
		}), "no panel reads backpressure_level"},
	{"grafana_prometheus", "a declared metric outside the headline nine is dropped from the dashboard", dashGrafanaProm,
		dashEditJSON(func(doc map[string]any) error {
			panels, _ := doc["panels"].([]any)
			var kept []any
			for _, p := range panels {
				if panel, _ := p.(map[string]any); panel == nil || panel["title"] != "Offload errors per second by class" {
					kept = append(kept, p)
				}
			}
			if len(kept) == len(panels) {
				return errors.New("the dashboard has no Offload errors per second by class panel")
			}
			doc["panels"] = kept
			return nil
		}), "no panel reads offload_errors, which metadata.yaml declares"},
	{"grafana_prometheus", "a panel has a data source that is not the input", dashGrafanaProm,
		dashEditPanel("Acknowledged rows/s", func(panel map[string]any) error {
			panel["datasource"] = map[string]any{"type": "prometheus", "uid": "prom-prod"}
			return nil
		}), `panel "Acknowledged rows/s" has the data source`},
	{"grafana_prometheus", "the instance variable stops matching every instance", dashGrafanaProm,
		dashEditJSON(func(doc map[string]any) error {
			templating, _ := doc["templating"].(map[string]any)
			list, _ := templating["list"].([]any)
			for _, item := range list {
				if v, _ := item.(map[string]any); v != nil && v["name"] == "instance" {
					v["allValue"] = ".+"
					return nil
				}
			}
			return errors.New("the dashboard has no instance variable")
		}), "whose All value is .*"},

	// --- The READMEs against the files they describe -------------------------
	{"docs", "the Looker Studio README loses a required IAM role", dashLookerDir + "README.md",
		dashReplaceAll("roles/bigquery.dataViewer", "roles/bigquery.viewer"), "`roles/bigquery.dataViewer`"},
	{"docs", "the Looker Studio README links to a heading that is gone", dashLookerDir + "README.md",
		dashReplaceFirst("(#build-the-template)", "(#build-the-templat)"), "points at the anchor #build-the-templat"},
	{"docs", "a Pages row loses its Terraform condition", dashLookerDir + "README.md",
		dashEditLine("| `ds3` |", "`require_partition_filter` is `false`", "`require_partition_filter` is `true`"), "the Pages row of ds3 lacks"},
	{"docs", "a Pages row names another query", dashLookerDir + "README.md",
		dashEditLine("| `ds1` |", "`agent_sessions.sql`", "`sessions.sql`"), "the Pages row of ds1 lacks"},
	{"docs", "a link points at a file that does not exist", "dashboards/README.md",
		dashReplaceFirst("(../metadata.yaml)", "(../metadata.yml)"), "metadata.yml, which does not exist"},
	{"docs", "a link points at a heading that is gone", "dashboards/README.md",
		dashReplaceFirst("#pointing-the-dashboards-at-the-marts", "#pointing-the-dashboards-at-the-martz"), "points at the anchor #pointing-the-dashboards-at-the-martz"},
	{"docs", "the metric table names a metric that metadata.yaml lacks", "dashboards/README.md",
		dashReplaceFirst("| `acknowledged_rows` |", "| `acknowledged_rowz` |"),
		"documents otelcol_exporter_bigquery_acknowledged_rowz, which metadata.yaml does not declare"},
	{"docs", "the metric table forgets a metric that a panel reads", "dashboards/README.md",
		dashReplaceFirst("| `acknowledged_rows` | Acknowledged rows per second |\n", ""),
		"does not document otelcol_exporter_bigquery_acknowledged_rows, which a panel reads"},
	{"docs", "a README claims exactly-once delivery", "dashboards/README.md",
		dashAppend("\nThe exporter delivers each row exactly-once.\n"), "claims exactly-once processing"},
	{"docs", "a README holds a character that is not ASCII", "dashboards/README.md",
		dashAppend("\ncaf\u00e9\n"), "is not ASCII"},
}

// TestDashSQLParser pins down what the GoogleSQL reader accepts and what it
// refuses: the rules above are only as good as the reader under them.
func TestDashSQLParser(t *testing.T) {
	t.Run("tokens", func(t *testing.T) {
		toks, err := dashTokenize("SELECT r'a`b\"c', `p.d.t`, @DS_START_DATE, 1.5 -- tail\n, /* block */ x")
		if err != nil {
			t.Fatal(err)
		}
		type token struct {
			kind dashTokKind
			text string
		}
		want := []token{
			{dashWord, "SELECT"}, {dashString, "r'a`b\"c'"}, {dashPunct, ","}, {dashIdent, "p.d.t"}, {dashPunct, ","},
			{dashParam, "DS_START_DATE"}, {dashPunct, ","}, {dashNumber, "1.5"}, {dashPunct, ","}, {dashWord, "x"},
		}
		if len(toks) != len(want) {
			t.Fatalf("got %d tokens %+v, want %d", len(toks), toks, len(want))
		}
		for i, w := range want {
			if toks[i].kind != w.kind || toks[i].text != w.text {
				t.Errorf("token %d is %v %q, want %v %q", i, toks[i].kind, toks[i].text, w.kind, w.text)
			}
		}
	})

	t.Run("bracket depth", func(t *testing.T) {
		toks, err := dashTokenize("f(a, [b, c])")
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for _, tok := range toks {
			got = append(got, tok.depth)
		}
		if want := []int{0, 0, 1, 1, 1, 2, 2, 2, 1, 0}; !slices.Equal(got, want) {
			t.Errorf("depths are %v, want %v", got, want)
		}
	})

	t.Run("refused input", func(t *testing.T) {
		for name, src := range map[string]string{
			"unbalanced close":        "SELECT 1)",
			"unclosed bracket":        "SELECT (1",
			"crossed brackets":        "SELECT (1]",
			"triple quote":            "SELECT '''a'''",
			"unterminated string":     "SELECT 'a",
			"line break in a string":  "SELECT 'a\nb'",
			"unterminated comment":    "SELECT 1 /* x",
			"unterminated identifier": "SELECT `a",
			"bare at sign":            "SELECT @ ",
			"unexpected character":    "SELECT $a",
		} {
			if _, err := dashTokenize(src); err == nil {
				t.Errorf("%s: %q was accepted", name, src)
			}
		}
		for name, src := range map[string]string{
			"UNION without a modifier":  "SELECT 1 AS x UNION SELECT 2 AS x",
			"INTERSECT without ALL":     "SELECT 1 AS x INTERSECT SELECT 2 AS x",
			"no SELECT":                 "FROM t",
			"empty select item":         "SELECT a, , b FROM t",
			"WHERE twice":               "SELECT a FROM t WHERE a > 1 WHERE b > 1",
			"malformed WITH table":      "WITH a (SELECT 1 AS x) SELECT x FROM a",
			"unsupported star modifier": "SELECT * REPLACE (1 AS x) FROM t",
		} {
			if _, err := dashParseSQL(src); err == nil {
				t.Errorf("%s: %q was accepted", name, src)
			}
		}
	})

	t.Run("WITH tables and a star", func(t *testing.T) {
		q, err := dashParseSQL("WITH a AS (SELECT 1 AS x, 2 AS y), b AS (SELECT * EXCEPT (y) FROM a) SELECT * FROM b;")
		if err != nil {
			t.Fatal(err)
		}
		if len(q.ctes) != 2 || q.ctes[0].name != "a" || q.ctes[1].name != "b" {
			t.Fatalf("WITH tables are %+v, want a and b", q.ctes)
		}
		cols, err := q.columns(0)
		if err != nil || !slices.Equal(cols, []string{"x"}) {
			t.Errorf("columns are %v (%v), want [x]", cols, err)
		}
		if _, err := mustParse(t, "SELECT * FROM t").columns(0); err == nil {
			t.Error("a star over a table that is not a WITH table was expanded")
		}
	})

	t.Run("select items", func(t *testing.T) {
		q := mustParse(t, "SELECT a, t.b, c AS d, COUNT(*), `e`, f.* FROM t")
		var names []string
		var stars []bool
		for _, it := range q.branches[0].items {
			names = append(names, it.name)
			stars = append(stars, it.star)
		}
		if want := []string{"a", "b", "d", "", "e", ""}; !slices.Equal(names, want) {
			t.Errorf("item names are %q, want %q", names, want)
		}
		if want := []bool{false, false, false, false, false, true}; !slices.Equal(stars, want) {
			t.Errorf("stars are %v, want %v", stars, want)
		}
	})

	t.Run("set operations", func(t *testing.T) {
		for src, branches := range map[string]int{
			"SELECT 1 AS x UNION ALL SELECT 2 AS x":                      2,
			"SELECT 1 AS x UNION DISTINCT SELECT 2 AS x":                 2,
			"SELECT 1 AS x EXCEPT DISTINCT SELECT 2 AS x":                2,
			"SELECT * EXCEPT (y) FROM t":                                 1,
			"SELECT a IS DISTINCT FROM b AS c FROM t":                    1,
			"SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3":   3,
			"SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS u": 1,
		} {
			if got := len(mustParse(t, src).branches); got != branches {
				t.Errorf("%q has %d branches, want %d", src, got, branches)
			}
		}
		q := mustParse(t, "SELECT a IS DISTINCT FROM b AS c FROM t")
		if got := q.branches[0].fromTables(); len(got) != 0 || len(q.branches[0].clauses["FROM"]) != 1 {
			t.Errorf("IS DISTINCT FROM was taken for a FROM clause: %v", q.branches[0].clauses["FROM"])
		}
	})

	t.Run("clauses", func(t *testing.T) {
		q := mustParse(t, "SELECT a FROM t WHERE a > 1 GROUP BY a HAVING COUNT(*) > 1 "+
			"QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY a) = 1 ORDER BY a LIMIT 3")
		var got []string
		for name := range q.branches[0].clauses {
			got = append(got, name)
		}
		slices.Sort(got)
		if want := []string{"FROM", "GROUP BY", "HAVING", "LIMIT", "ORDER BY", "QUALIFY", "WHERE"}; !slices.Equal(got, want) {
			t.Errorf("clauses are %q, want %q", got, want)
		}
		if got := dashJoin(q.branches[0].clauses["QUALIFY"]); got != "ROW_NUMBER ( ) OVER ( PARTITION BY A ORDER BY A ) = 1" {
			t.Errorf("QUALIFY is %q", got)
		}
	})

	t.Run("identifier checks", func(t *testing.T) {
		check := func(sql string, quals map[string]map[string]bool, cols ...string) []string {
			t.Helper()
			return dashCheckBranch(mustParse(t, sql).branches[0], dashSource{cols: dashSet(cols), quals: quals})
		}
		for _, tc := range []struct {
			sql  string
			cols []string
			want string // "" for no problem
		}{
			{"SELECT a, b FROM t WHERE a > 1 GROUP BY a", []string{"a", "b"}, ""},
			{"SELECT COUNT(a) AS n FROM t", []string{"a"}, ""},
			{"SELECT a AS x FROM t GROUP BY x", []string{"a"}, ""},
			{"SELECT a, zz FROM t", []string{"a"}, "unknown identifier zz"},
			{"SELECT a AS x FROM t WHERE x > 1", []string{"a"}, "WHERE: unknown identifier x"},
			{"SELECT a AS x, x + 1 AS y FROM t", []string{"a"}, "select item y: unknown identifier x"},
			{"SELECT CAST(a AS STRING) AS s FROM t", []string{"a"}, ""},
			{"SELECT a FROM t WHERE a IS DISTINCT FROM @p", []string{"a"}, ""},
		} {
			problems := check(tc.sql, nil, tc.cols...)
			switch {
			case tc.want == "" && len(problems) != 0:
				t.Errorf("%q: unexpected problems %v", tc.sql, problems)
			case tc.want != "" && !containsText(problems, tc.want):
				t.Errorf("%q: problems %v, want one with %q", tc.sql, problems, tc.want)
			}
		}
		quals := map[string]map[string]bool{"t": {"a": true}}
		if problems := check("SELECT t.a, t.nope FROM x AS t", quals, "a"); !containsText(problems, "t.nope is not a column") || containsText(problems, "t.a ") {
			t.Errorf("qualified columns: problems %v, want only t.nope", problems)
		}
	})
}

func mustParse(t *testing.T, sql string) *dashQuery {
	t.Helper()
	q, err := dashParseSQL(sql)
	if err != nil {
		t.Fatalf("parsing %q: %v", sql, err)
	}
	return q
}

// TestGrafanaBQVariableExpansionAndAlertOnlyServices verifies:
//  1. ?var-project= and ?var-dataset= URL parameters cannot override the table
//     reference because project and dataset are not template variables in
//     templating.list (panels use import-time __inputs constants ${VAR_PROJECT}
//     and ${VAR_DATASET});
//  2. Adversarial ?var-service= payloads expanded via ${service:json}
//     (json.Marshal) inside ARRAY<STRING>${service:json} always produce a
//     single inert GoogleSQL string literal inside the array and cannot inject
//     SQL tokens or break out of UNNEST;
//  3. Selecting "All" (allValue: "__all" -> ["__all"]) leaves every panel
//     unfiltered via ('__all' IN UNNEST(ARRAY<STRING>${service:json}) OR ...),
//     so alert-only services in otel_alerts (such as an AGENT_LATENCY_HIGH
//     alert from a service with zero LLM/MCP/session rows in the three views
//     that populate the service dropdown) are shown under "All".
func TestGrafanaBQVariableExpansionAndAlertOnlyServices(t *testing.T) {
	files := dashLoadFileSet(t)
	g, probs := dashDecodeGrafana(files, dashGrafanaBQ)
	if g == nil || len(probs) != 0 {
		t.Fatalf("dashDecodeGrafana: %v", probs)
	}

	// 1. Verify templating.list contains only "service", so ?var-project= and
	// ?var-dataset= have no template variable to bind to.
	for _, v := range g.Templating.List {
		if v.Name != "service" {
			t.Fatalf("unexpected template variable %q in %s", v.Name, dashGrafanaBQ)
		}
		if v.AllValue != "__all" {
			t.Fatalf("service.allValue = %q, want \"__all\"", v.AllValue)
		}
	}

	// 2. Simulate Grafana's ${service:json} interpolation for adversarial
	// ?var-service= URL values and verify every panel's expanded SQL tokenizes
	// and parses with zero extra clauses and a single string literal in UNNEST.
	adversarialServices := []string{
		`x\']) OR TRUE -- `,
		`svc' OR 1=1 --`,
		`svc" ) UNION ALL SELECT 1 --`,
		"svc\n--",
		"svc`",
		`svc\"`,
	}
	for _, adv := range adversarialServices {
		jsonBytes, err := json.Marshal([]string{adv})
		if err != nil {
			t.Fatalf("json.Marshal(%q): %v", adv, err)
		}
		repl := strings.NewReplacer(
			"${VAR_PROJECT}", "my-proj",
			"${VAR_DATASET}", "my_ds",
			"${service:json}", string(jsonBytes),
			"${__from}", dashGrafanaFrom,
			"${__to}", dashGrafanaTo,
		)
		for _, pn := range g.Panels {
			if pn.Type == "row" || len(pn.Targets) == 0 {
				continue
			}
			expanded := repl.Replace(pn.Targets[0].RawSQL)
			expanded = dashTimeFilter.ReplaceAllString(expanded, "$1 BETWEEN TIMESTAMP_MILLIS("+dashGrafanaFrom+") AND TIMESTAMP_MILLIS("+dashGrafanaTo+")")
			q, err := dashParseSQL(expanded)
			if err != nil {
				t.Fatalf("panel %q failed to parse with adversarial service %q: %v", pn.Title, adv, err)
			}
			if len(q.branches) == 0 {
				t.Fatalf("panel %q has 0 branches", pn.Title)
			}
			// Verify the WHERE clause still has exactly 2 top-level AND conjuncts
			// (time filter + parenthesized service filter), proving no SQL
			// operator was injected at the WHERE level.
			b := q.branches[0]
			where := b.clauses["WHERE"]
			base := b.toks[0].depth
			topAnds := 0
			for _, tok := range where {
				if tok.depth == base && tok.up == "AND" {
					topAnds++
				}
			}
			if topAnds < 1 {
				t.Fatalf("panel %q WHERE lost its top-level AND structure for %q", pn.Title, adv)
			}
		}
	}

	// 3. Simulate selecting "All" (["__all"]) vs selecting specific dropdown
	// options when an alert-only service ("alert-only-agent") exists in
	// otel_alerts but has 0 rows in v_gen_ai_model_costs, v_mcp_tool_usage, and
	// v_gen_ai_sessions.
	evalServiceFilter := func(selected []string, rowService string) bool {
		if rowService == "" {
			rowService = "(none)"
		}
		return slices.Contains(selected, "__all") || slices.Contains(selected, rowService)
	}
	dropdownOptions := []string{"chat-svc", "mcp-svc"} // alert-only-agent is absent from the 3 views
	if !evalServiceFilter([]string{"__all"}, "alert-only-agent") {
		t.Errorf("selecting All ([\"__all\"]) filtered out alert-only-agent")
	}
	if !evalServiceFilter([]string{"__all"}, "") {
		t.Errorf("selecting All ([\"__all\"]) filtered out NULL service_name")
	}
	if evalServiceFilter(dropdownOptions, "alert-only-agent") {
		t.Errorf("expanding All to dropdown options %v unexpectedly matched alert-only-agent", dropdownOptions)
	}
}
