// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package contribcheck

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/confmap"
	"go.yaml.in/yaml/v3"

	bqotel "github.com/haiyuan-eng-google/BQ-OTEL-Exporter"
)

// docsDir is docs/upstream, relative to this package.
const docsDir = "../../docs/upstream"

// stagingDocs maps each draft that stages files to the pull requests it
// stages.
var stagingDocs = map[string][]int{
	"contrib-pr-staging.md":         {1, 2},
	"contrib-alpha-and-releases.md": {3},
}

// stagedPaths lists the files that each staged pull request adds or
// replaces, relative to ComponentDir, sorted.
var stagedPaths = map[int][]string{
	1: {"Makefile", "README.md", "config.go", "config_test.go", "doc.go", "factory.go", "factory_test.go", "go.mod", "metadata.yaml", "testdata/config.yaml"},
	2: {"README.md", "factory.go", "factory_test.go", "go.mod", "metadata.yaml"},
	3: {"README.md", "metadata.yaml"},
}

// expects holds what the metadata.yaml of each staged tree declares.
var expects = []struct {
	pr   int
	want Expect
}{
	{1, Expect{Stability: "development", SkipShutdown: true}},
	{2, Expect{Stability: "development"}},
	{3, Expect{Stability: "alpha", Distributions: []string{"contrib"}}},
}

func readDoc(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(docsDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// stagedFiles returns the files staged in docs/upstream. It fails the test
// when a draft stages a pull request that belongs to the other draft.
func stagedFiles(t *testing.T) []File {
	t.Helper()
	var all []File
	for _, name := range slices.Sorted(maps.Keys(stagingDocs)) {
		files, err := Extract(readDoc(t, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, f := range files {
			if !slices.Contains(stagingDocs[name], f.PR) {
				t.Fatalf("%s:%d stages pull request %d; want only pull requests %v", name, f.Line, f.PR, stagingDocs[name])
			}
		}
		all = append(all, files...)
	}
	return all
}

func stagedTree(t *testing.T, files []File, pr int) map[string][]byte {
	t.Helper()
	tree, err := Tree(files, pr)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// writeStaged writes tree into a fresh directory named after the Go package,
// as mdatagen requires, and returns the directory.
func writeStaged(t *testing.T, tree map[string][]byte) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), packageName)
	if err := WriteTree(dir, tree); err != nil {
		t.Fatal(err)
	}
	return dir
}

// mdBlock is a fenced code block of a Markdown document.
type mdBlock struct {
	info   string // the info string of the opening fence, such as "yaml"
	body   string
	line   int  // the line of the opening fence, starting at 1
	staged bool // whether a contribcheck marker directly precedes the block
}

// split separates md into the lines outside fenced code blocks and the
// fenced code blocks that are not nested in another block.
func split(t *testing.T, md []byte) ([]string, []mdBlock) {
	t.Helper()
	lines := strings.Split(string(md), "\n")
	var prose []string
	var blocks []mdBlock
	for i := 0; i < len(lines); i++ {
		f, ok := openingFence(lines[i], 3)
		if !ok {
			prose = append(prose, lines[i])
			continue
		}
		end := closingFence(lines, i+1, f)
		if end < 0 {
			t.Fatalf("line %d: code fence is never closed", i+1)
		}
		blocks = append(blocks, mdBlock{
			info:   strings.TrimSpace(strings.TrimLeft(strings.TrimLeft(lines[i], " "), string(f.char))),
			body:   strings.Join(lines[i+1:end], "\n") + "\n",
			line:   i + 1,
			staged: i > 0 && strings.HasPrefix(lines[i-1], markerPrefix),
		})
		i = end
	}
	return prose, blocks
}

// formatFiles renders files for test failure messages.
func formatFiles(files []File) string {
	var b strings.Builder
	for _, f := range files {
		fmt.Fprintf(&b, "{PR:%d Path:%q Body:%q Line:%d}\n", f.PR, f.Path, f.Body, f.Line)
	}
	return b.String()
}

func replaceOnce(t *testing.T, b []byte, from, to string) []byte {
	t.Helper()
	if n := bytes.Count(b, []byte(from)); n != 1 {
		t.Fatalf("%q occurs %d times; want once", from, n)
	}
	return bytes.Replace(b, []byte(from), []byte(to), 1)
}

func TestExtract(t *testing.T) {
	lines := []string{
		"# Draft",
		"",
		"```text",
		"<!-- contribcheck:file pr=9 path=exporter/bigqueryexporter/shown.go -->",
		"```",
		"   ~~~~",
		"<!-- contribcheck shown in an indented block -->",
		"   ~~~~",
		"<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/doc.go -->",
		"```go",
		"package bigqueryexporter",
		"```",
		"",
		"<!-- contribcheck:file pr=2 path=exporter/bigqueryexporter/README.md -->",
		"````markdown",
		"# README",
		"",
		"```yaml",
		"a: b",
		"```",
		"````",
		"<!-- contribcheck:file  path=exporter/bigqueryexporter/testdata/config.yaml pr=12 -->",
		"~~~yaml",
		"bigquery:",
		"```",
		"~~~",
	}
	want := []File{
		{PR: 1, Path: "exporter/bigqueryexporter/doc.go", Body: []byte("package bigqueryexporter\n"), Line: 9},
		{PR: 2, Path: "exporter/bigqueryexporter/README.md", Body: []byte("# README\n\n```yaml\na: b\n```\n"), Line: 14},
		{PR: 12, Path: "exporter/bigqueryexporter/testdata/config.yaml", Body: []byte("bigquery:\n```\n"), Line: 22},
	}
	for _, eol := range []string{"\n", "\r\n"} {
		got, err := Extract([]byte(strings.Join(lines, eol) + eol))
		if err != nil {
			t.Fatalf("line ending %q: %v", eol, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("line ending %q: Extract =\n%s\nwant\n%s", eol, formatFiles(got), formatFiles(want))
		}
	}
}

func TestExtractErrors(t *testing.T) {
	const (
		marker = "<!-- contribcheck:file pr=1 path=exporter/bigqueryexporter/a.go -->"
		staged = marker + "\n```go\npackage a\n```\n"
	)
	edit := func(from, to string) string { return strings.Replace(staged, from, to, 1) }
	tests := []struct {
		name, md, want string
	}{
		{"malformed marker", edit("contribcheck:file", "contribcheck:files"), "line 1: malformed marker"},
		{"text after the marker", edit("-->", "--> trailing"), "line 1: malformed marker"},
		{"marker on the last line", marker, "line 1: marker is not followed by a code fence"},
		{"text after the marker line", marker + "\ntext\n", "line 1: marker is not followed by a code fence in column 0"},
		{"blank line after the marker", marker + "\n\n```go\npackage a\n```\n", "line 1: marker is not followed by a code fence in column 0"},
		{"indented fence", marker + "\n ```go\npackage a\n ```\n", "line 1: marker is not followed by a code fence in column 0"},
		{"unclosed staged block", marker + "\n```go\npackage a\n", "line 2: code fence is never closed"},
		{"unclosed block", "```text\nnot staged\n", "line 1: code fence is never closed"},
		{"empty file", marker + "\n```go\n```\n", "line 1: staged file exporter/bigqueryexporter/a.go is empty"},
		{"file staged twice", staged + "\n" + staged, "line 6: pull request 1 stages exporter/bigqueryexporter/a.go again; the first block is at line 1"},
		{"pr zero", edit("pr=1", "pr=0"), `line 1: marker pr "0" is not a positive integer`},
		{"pr with a leading zero", edit("pr=1", "pr=01"), `line 1: marker pr "01" is not a positive integer`},
		{"negative pr", edit("pr=1", "pr=-1"), `line 1: marker pr "-1" is not a positive integer`},
		{"pr that is not a number", edit("pr=1", "pr=x"), `line 1: marker pr "x" is not a positive integer`},
		{"no pr", edit("pr=1 ", ""), "line 1: marker has no pr attribute"},
		{"no path", edit(" path=exporter/bigqueryexporter/a.go", ""), "line 1: marker has no path attribute"},
		{"repeated attribute", edit("pr=1", "pr=1 pr=2"), `line 1: marker attribute "pr" is repeated`},
		{"unknown attribute", edit("pr=1", "pr=1 lang=go"), `line 1: unknown marker attribute "lang"`},
		{"attribute without a value", edit("pr=1", "pr=1 go"), `line 1: marker attribute "go" is not key=value`},
		{"empty attribute value", edit("pr=1", "pr="), `line 1: marker attribute "pr=" is not key=value`},
		{"path that is not clean", edit("bigqueryexporter/a.go", "bigqueryexporter/../a.go"), "is not a clean path under exporter/bigqueryexporter/"},
		{"path outside the component", edit("exporter/bigqueryexporter/a.go", "receiver/otlpreceiver/a.go"), "is not a clean path under exporter/bigqueryexporter/"},
		{"absolute path", edit("path=exporter", "path=/exporter"), "is not a clean path under exporter/bigqueryexporter/"},
		{"component directory as the path", edit("exporter/bigqueryexporter/a.go", "exporter/bigqueryexporter"), "is not a clean path under exporter/bigqueryexporter/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files, err := Extract([]byte(tt.md))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Extract = %s, %v; want an error containing %q", formatFiles(files), err, tt.want)
			}
		})
	}
}

func TestTree(t *testing.T) {
	file := func(pr int, name, body string) File {
		return File{PR: pr, Path: ComponentDir + "/" + name, Body: []byte(body)}
	}
	// Out of order on purpose: a later pull request replaces a file of an
	// earlier one wherever it is listed.
	files := []File{
		file(2, "factory.go", "factory 2"),
		file(1, "factory.go", "factory 1"),
		file(1, "doc.go", "doc 1"),
		file(3, "metadata.yaml", "metadata 3"),
	}
	tests := []struct {
		pr   int
		want map[string]string
	}{
		{1, map[string]string{"doc.go": "doc 1", "factory.go": "factory 1"}},
		{2, map[string]string{"doc.go": "doc 1", "factory.go": "factory 2"}},
		{3, map[string]string{"doc.go": "doc 1", "factory.go": "factory 2", "metadata.yaml": "metadata 3"}},
	}
	for _, tt := range tests {
		tree, err := Tree(files, tt.pr)
		if err != nil {
			t.Fatalf("Tree(%d): %v", tt.pr, err)
		}
		got := make(map[string]string, len(tree))
		for name, body := range tree {
			got[name] = string(body)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Tree(%d) = %v; want %v", tt.pr, got, tt.want)
		}
	}
	if _, err := Tree(files, 4); err == nil || !strings.Contains(err.Error(), "no files are staged for pull request 4") {
		t.Errorf("Tree(4) error = %v; want no files are staged", err)
	}
}

func TestWriteTree(t *testing.T) {
	dir := t.TempDir()
	if err := WriteTree(dir, map[string][]byte{"testdata/config.yaml": []byte("bigquery:\n")}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "testdata", "config.yaml")); err != nil || string(got) != "bigquery:\n" {
		t.Errorf("testdata/config.yaml = %q, %v; want %q", got, err, "bigquery:\n")
	}
	for _, rel := range []string{"../escape.go", "/absolute.go", ""} {
		if err := WriteTree(dir, map[string][]byte{rel: []byte("x")}); err == nil || !strings.Contains(err.Error(), "is not local") {
			t.Errorf("WriteTree(%q) error = %v; want is not local", rel, err)
		}
	}
}

func TestCheckTreeOnMissingDirectory(t *testing.T) {
	if _, err := CheckTree(filepath.Join(t.TempDir(), "missing"), Expect{}); err == nil {
		t.Error("CheckTree on a missing directory returned no error")
	}
}

func TestProblemString(t *testing.T) {
	p := Problem{Check: "go.mod", Path: "go.mod", Msg: `go "1.26.5"; want "1.26.0"`}
	if got, want := p.String(), `go.mod: go.mod: go "1.26.5"; want "1.26.0"`; got != want {
		t.Errorf("String() = %q; want %q", got, want)
	}
}

func TestUpstreamDocs(t *testing.T) {
	entries, err := os.ReadDir(docsDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	// A new draft must be added to stagingDocs or to the drafts below.
	drafts := []string{"new-component-issue-draft.md", "outreach-and-governance.md"}
	if want := slices.Sorted(slices.Values(slices.Concat(slices.Collect(maps.Keys(stagingDocs)), drafts))); !slices.Equal(names, want) {
		t.Fatalf("docs/upstream holds %v; want %v", names, want)
	}
	for _, name := range drafts {
		if files, err := Extract(readDoc(t, name)); err != nil || len(files) != 0 {
			t.Errorf("%s: Extract = %d files, %v; want no staged files", name, len(files), err)
		}
	}
}

func TestStagedTrees(t *testing.T) {
	files := stagedFiles(t)
	got := make(map[int][]string)
	for _, f := range files {
		got[f.PR] = append(got[f.PR], strings.TrimPrefix(f.Path, ComponentDir+"/"))
	}
	for _, paths := range got {
		slices.Sort(paths)
	}
	if !reflect.DeepEqual(got, stagedPaths) {
		t.Fatalf("staged files = %v; want %v", got, stagedPaths)
	}
	for _, e := range expects {
		t.Run("pr"+strconv.Itoa(e.pr), func(t *testing.T) {
			problems, err := CheckTree(writeStaged(t, stagedTree(t, files, e.pr)), e.want)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range problems {
				t.Error(p)
			}
		})
	}
}

func TestPendingCodeowners(t *testing.T) {
	files := stagedFiles(t)
	// The handles of the sponsor and the third codeowner are not confirmed
	// yet. When they are, replace the placeholders in docs/upstream,
	// including outreach-and-governance.md, and empty this list.
	want := []string{"<codeowner-3>", "<sponsor>"}
	for _, e := range expects {
		meta := stagedTree(t, files, e.pr)["metadata.yaml"]
		got, err := PendingCodeowners(meta)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("pull request %d: PendingCodeowners = %q; want %q", e.pr, got, want)
		}
		confirmed := strings.NewReplacer("<sponsor>", "example-sponsor", "<codeowner-3>", "example-owner").Replace(string(meta))
		if got, err := PendingCodeowners([]byte(confirmed)); err != nil || len(got) != 0 {
			t.Errorf("pull request %d with confirmed handles: PendingCodeowners = %q, %v; want none", e.pr, got, err)
		}
	}
	if _, err := PendingCodeowners([]byte("unknown_key: true\n")); err == nil {
		t.Error("PendingCodeowners accepted a key that contribcheck does not model")
	}
}

func TestCheckPromotion(t *testing.T) {
	files := stagedFiles(t)
	before := stagedTree(t, files, 2)["metadata.yaml"]
	after := stagedTree(t, files, 3)["metadata.yaml"]
	if problems := CheckPromotion(before, after); len(problems) != 0 {
		t.Fatalf("CheckPromotion on the staged trees = %v; want none", problems)
	}
	const otherKeys = "changes keys other than status.stability and status.distributions"
	tests := []struct {
		name          string
		before, after []byte
		want          []string // prefixes of the messages
	}{
		{"display name changes", before, replaceOnce(t, after, "display_name: BigQuery Exporter\n", "display_name: BigQuery\n"), []string{otherKeys}},
		{"codeowners change", before, replaceOnce(t, after, "<codeowner-3>]", "example-owner]"), []string{otherKeys}},
		{"compared with pull request 1", stagedTree(t, files, 1)["metadata.yaml"], after, []string{otherKeys}},
		{"stays in development", before, replaceOnce(t, after, "    alpha: [traces, logs, metrics]\n", "    development: [traces, logs, metrics]\n"), []string{"after: status.stability level development; want alpha"}},
		{"one signal promoted", before, replaceOnce(t, after, "    alpha: [traces, logs, metrics]\n", "    alpha: [traces]\n    development: [logs, metrics]\n"), []string{"after: status.stability declares 2 levels"}},
		{"no contrib distribution", before, replaceOnce(t, after, "  distributions: [contrib]\n", "  distributions: []\n"), []string{"after: status.distributions []; want [contrib]"}},
		{"already promoted", after, after, []string{"before: status.distributions [contrib]; want []", "before: status.stability level alpha; want development"}},
		{"before does not decode", []byte("unknown_key: true\n"), after, []string{"before: decoding metadata.yaml"}},
		{"after does not decode", before, []byte("unknown_key: true\n"), []string{"after: decoding metadata.yaml"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := CheckPromotion(tt.before, tt.after)
			ok := len(problems) == len(tt.want)
			for i := 0; ok && i < len(problems); i++ {
				p := problems[i]
				ok = p.Check == "promotion" && p.Path == "metadata.yaml" && strings.HasPrefix(p.Msg, tt.want[i])
			}
			if !ok {
				t.Errorf("CheckPromotion = %v; want messages starting with %q", problems, tt.want)
			}
		})
	}
}

// mutation changes a staged tree.
type mutation func(t *testing.T, tree map[string][]byte)

func edit(file, from, to string) mutation {
	return func(t *testing.T, tree map[string][]byte) {
		t.Helper()
		tree[file] = replaceOnce(t, tree[file], from, to)
	}
}

func appendTo(file, text string) mutation {
	return func(_ *testing.T, tree map[string][]byte) {
		tree[file] = append(slices.Clone(tree[file]), text...)
	}
}

func add(file, body string) mutation {
	return func(_ *testing.T, tree map[string][]byte) {
		tree[file] = []byte(body)
	}
}

func remove(file string) mutation {
	return func(_ *testing.T, tree map[string][]byte) {
		delete(tree, file)
	}
}

// TestCheckTreeMutations breaks the pull request 1 tree one way at a time
// and checks that exactly the expected checks fail.
func TestCheckTreeMutations(t *testing.T) {
	const generated = "// Code generated by mdatagen. DO NOT EDIT.\n\n"
	header := headerCopyright + "\n" + headerSPDX + "\n\n"
	importComment := ` // import "` + ModulePath + `"`
	tests := []struct {
		name   string
		mutate mutation
		want   []string // "path: check" of each problem
	}{
		{"unchanged", nil, nil},
		{"missing file", remove("config_test.go"), []string{"config_test.go: files"}},
		{"license header", edit("config.go", headerCopyright+"\n", "// Copyright 2026 Example\n"), []string{"config.go: header"}},
		{"generated file without a license header", add("internal/metadata/generated_status.go", generated+"package metadata\n"), nil},
		{"syntax error", appendTo("config_test.go", "func {\n"), []string{"config_test.go: parse"}},
		{"package name", edit("config_test.go", "\npackage bigqueryexporter\n", "\npackage bqexporter\n"), []string{"config_test.go: package"}},
		{"external test package", edit("config_test.go", "\npackage bigqueryexporter\n", "\npackage bigqueryexporter_test\n"), nil},
		{"generated file in the wrong package", add("internal/metadata/generated_status.go", generated+"package status\n"), []string{"internal/metadata/generated_status.go: package"}},
		{"import comment", edit("config.go", importComment, ""), []string{"config.go: import comment"}},
		{"import comment of a subpackage", add("internal/metadata/doc.go", header+"package metadata"+` // import "`+ModulePath+`/internal/metadata"`+"\n"), nil},
		{"subpackage without an import comment", add("internal/metadata/doc.go", header+"package metadata\n"), []string{"internal/metadata/doc.go: import comment"}},
		{"second go:generate directive", appendTo("factory.go", "\n//go:generate make other\n"), []string{"doc.go: go:generate", "factory.go: go:generate"}},
		{"no go:generate directive", edit("doc.go", goGenerate+"\n", ""), []string{"doc.go: go:generate"}},
		{"other go:generate directive", edit("doc.go", goGenerate, "//go:generate mdatagen metadata.yaml"), []string{"doc.go: go:generate"}},
		{"generated file with a directive and an exported function", add("generated_extra.go", generated+"package bigqueryexporter\n\n//go:generate make other\n\nfunc Extra() {}\n"), nil},
		{"exported function", appendTo("factory.go", "\nfunc Extra() {}\n"), []string{"factory.go: api"}},
		{"NewFactory signature", edit("factory.go", "func NewFactory() exporter.Factory {", "func NewFactory() any {"), []string{"factory.go: api"}},
		{"no NewFactory", edit("factory.go", "func NewFactory() exporter.Factory {", "func newFactory() exporter.Factory {"), []string{".: api"}},
		{"module path", edit("go.mod", "module "+ModulePath+"\n", "module example.com/bigqueryexporter\n"), []string{"go.mod: go.mod"}},
		{"go directive", edit("go.mod", "\ngo 1.26.0\n", "\ngo 1.26.5\n"), []string{"go.mod: go.mod"}},
		{"toolchain directive", edit("go.mod", "\ngo 1.26.0\n", "\ngo 1.26.0\n\ntoolchain go1.27.3\n"), []string{"go.mod: go.mod"}},
		{"library version", edit("go.mod", LibraryPath+" "+LibraryVersion, LibraryPath+" v0.0.9"), []string{"go.mod: go.mod"}},
		{"replace directive", appendTo("go.mod", "\nreplace "+LibraryPath+" => ../../..\n"), []string{"go.mod: go.mod"}},
		{"unclosed require block", edit("go.mod", "\n)\n", "\n"), []string{"go.mod: go.mod"}},
		{"Makefile", appendTo("Makefile", "\nlint:\n"), []string{"Makefile: Makefile"}},
		{"README without the status section", edit("README.md", statusBegin+"\n", ""), []string{"README.md: readme"}},
		{"README with an unterminated status section", edit("README.md", statusEnd+"\n", ""), []string{"README.md: readme"}},
		{"unknown metadata key", appendTo("metadata.yaml", "unknown_key: true\n"), []string{"metadata.yaml: metadata"}},
		{"component type", edit("metadata.yaml", "type: bigquery\n", "type: big_query\n"), []string{"metadata.yaml: metadata"}},
		{"stability level", edit("metadata.yaml", "    development: [traces, logs, metrics]\n", "    alpha: [traces, logs, metrics]\n"), []string{"metadata.yaml: metadata"}},
		{"signals", edit("metadata.yaml", "    development: [traces, logs, metrics]\n", "    development: [traces, logs]\n"), []string{"metadata.yaml: metadata"}},
		{"distributions", edit("metadata.yaml", "  distributions: []\n", "  distributions: [contrib]\n"), []string{"metadata.yaml: metadata"}},
		{"two codeowners", edit("metadata.yaml", ", <codeowner-3>]", "]"), []string{"metadata.yaml: metadata"}},
		{"codeowner that is not a handle", edit("metadata.yaml", "<codeowner-3>]", "not_a_handle]"), []string{"metadata.yaml: metadata"}},
		{"repeated codeowner", edit("metadata.yaml", "<codeowner-3>]", "<sponsor>]"), []string{"metadata.yaml: metadata", "metadata.yaml: metadata"}},
		{"sending queue support", edit("metadata.yaml", "  support: has_overrides\n", "  support: unsupported\n"), []string{"metadata.yaml: metadata"}},
		{"sending queue overrides", edit("metadata.yaml", "    batch:\n      enabled: false\n", ""), []string{"metadata.yaml: metadata"}},
		{"skip_shutdown", edit("metadata.yaml", "  skip_shutdown: true\n", "  skip_shutdown: false\n"), []string{"metadata.yaml: metadata"}},
		{"test configuration", edit("metadata.yaml", "    project: my-project\n", "    project: \"\"\n"), []string{"metadata.yaml: metadata"}},
		{"goleak ignore list", edit("metadata.yaml", opencensusWorker, "example.com/worker.run"), []string{"metadata.yaml: metadata"}},
		{"testdata without the default component ID", edit("testdata/config.yaml", "bigquery:\n  project: my-project\n  dataset: otel\n", ""), []string{"testdata/config.yaml: testdata"}},
		{"testdata that does not parse", appendTo("testdata/config.yaml", "bigquery: [\n"), []string{"testdata/config.yaml: testdata"}},
	}
	files := stagedFiles(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := stagedTree(t, files, 1)
			if tt.mutate != nil {
				tt.mutate(t, tree)
			}
			problems, err := CheckTree(writeStaged(t, tree), expects[0].want)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, p := range problems {
				got = append(got, p.Path+": "+p.Check)
			}
			slices.Sort(got)
			if !slices.Equal(got, slices.Sorted(slices.Values(tt.want))) {
				t.Errorf("CheckTree = %q; want problems %q", problems, tt.want)
			}
		})
	}
}

// issueGates are the gates that item 5.6 of the v0.1.0 milestone issue
// names, in its order.
var issueGates = []string{
	"checkdoc", "checkmetadata", "checkapi", "goporto", "crosslink", "tidylist", "gotidy", "genotelcontribcol",
	"generate", "multimod-verify", "generate-gh-issue-templates", "gengithub", "genlabels", "gencodecov", "addlicense",
}

func TestGates(t *testing.T) {
	sourceRE := regexp.MustCompile(`^Makefile(\.Common)?:[1-9][0-9]*$`)
	var targets, static []string
	for i, g := range Gates() {
		targets = append(targets, g.Target)
		if g.InIssue != (i < len(issueGates)) {
			t.Errorf("%s: InIssue = %t", g.Target, g.InIssue)
		}
		switch g.Status {
		case StaticPrecondition:
			static = append(static, g.Target)
		case NeedsCheckout:
		default:
			t.Errorf("%s: status %q", g.Target, g.Status)
		}
		if !sourceRE.MatchString(g.Source) {
			t.Errorf("%s: source %q is not a Makefile line", g.Target, g.Source)
		}
		if g.Recipe == "" {
			t.Errorf("%s: no recipe excerpt", g.Target)
		}
	}
	if want := slices.Concat(issueGates, []string{"genoteltestbedcol"}); !slices.Equal(targets, want) {
		t.Errorf("gate targets = %q; want %q", targets, want)
	}
	slices.Sort(static)
	if want := []string{"addlicense", "checkapi", "checkdoc", "checkmetadata", "generate", "goporto", "gotidy"}; !slices.Equal(static, want) {
		t.Errorf("gates with a static precondition = %q; want %q", static, want)
	}
}

// TestGateTable checks that the gate table of contrib-pr-staging.md lists
// Gates in order, with the same sources, recipe excerpts, and statuses.
func TestGateTable(t *testing.T) {
	code := func(cell string) string {
		return strings.TrimSuffix(strings.TrimPrefix(cell, "`"), "`")
	}
	var rows []Gate
	for _, line := range strings.Split(string(readDoc(t, "contrib-pr-staging.md")), "\n") {
		if !strings.HasPrefix(line, "| `make ") {
			continue
		}
		cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "| "), " |"), " | ")
		if len(cells) != 5 {
			t.Fatalf("gate row has %d cells; want 5: %s", len(cells), line)
		}
		rows = append(rows, Gate{
			Target: strings.TrimPrefix(code(cells[0]), "make "),
			Source: code(cells[1]),
			Recipe: code(cells[2]),
			Status: Status(cells[3]),
		})
	}
	var want []Gate
	for _, g := range Gates() {
		g.InIssue = false
		want = append(want, g)
	}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("gate table rows =\n%+v\nwant\n%+v", rows, want)
	}
}

// TestUpstreamDiffs checks that the drafts stage the changes to contrib files
// outside the component and to the releases manifest.
func TestUpstreamDiffs(t *testing.T) {
	tests := []struct{ doc, line string }{
		{"contrib-pr-staging.md", "+      - " + ModulePath},
		{"contrib-alpha-and-releases.md", "+    alpha: [traces, logs, metrics]"},
		{"contrib-alpha-and-releases.md", "+  distributions: [contrib]"},
		{"contrib-alpha-and-releases.md", "+  - gomod: " + ModulePath + " v0.159.0"},
		{"contrib-alpha-and-releases.md", "+  - gomod: " + ModulePath + " <first-contrib-release-with-alpha>"},
	}
	for _, tt := range tests {
		_, blocks := split(t, readDoc(t, tt.doc))
		found := false
		for _, b := range blocks {
			found = found || (b.info == "diff" && slices.Contains(strings.Split(b.body, "\n"), tt.line))
		}
		if !found {
			t.Errorf("%s has no diff block with the line %q", tt.doc, tt.line)
		}
	}
}

func TestChangelogEntries(t *testing.T) {
	type entry struct {
		ChangeType string   `yaml:"change_type"`
		Component  string   `yaml:"component"`
		Note       string   `yaml:"note"`
		Issues     []any    `yaml:"issues"`
		Subtext    string   `yaml:"subtext"`
		ChangeLogs []string `yaml:"change_logs"`
	}
	var types []string
	for _, name := range slices.Sorted(maps.Keys(stagingDocs)) {
		_, blocks := split(t, readDoc(t, name))
		for _, b := range blocks {
			if b.info != "yaml" || b.staged || !strings.Contains(b.body, "change_type:") {
				continue
			}
			where := fmt.Sprintf("%s:%d", name, b.line)
			var e entry
			dec := yaml.NewDecoder(strings.NewReader(b.body))
			dec.KnownFields(true)
			if err := dec.Decode(&e); err != nil {
				t.Errorf("%s: %v", where, err)
				continue
			}
			types = append(types, e.ChangeType)
			if e.Component != "exporter/bigquery" || e.Note == "" || !slices.Equal(e.ChangeLogs, []string{"user"}) {
				t.Errorf("%s: component %q, note %q, change_logs %q; want exporter/bigquery, a note, and [user]", where, e.Component, e.Note, e.ChangeLogs)
			}
			if len(e.Issues) == 0 {
				t.Errorf("%s: no issues", where)
			}
			for _, issue := range e.Issues {
				if n, ok := issue.(int); ok && n > 0 || issue == "<donation-issue>" {
					continue
				}
				t.Errorf("%s: issue %v is neither a number nor <donation-issue>", where, issue)
			}
		}
	}
	// contrib-alpha-and-releases.md sorts before contrib-pr-staging.md.
	if want := []string{"enhancement", "new_component", "enhancement"}; !slices.Equal(types, want) {
		t.Errorf("change types = %q; want %q", types, want)
	}
}

func TestIssueDraft(t *testing.T) {
	doc := readDoc(t, "new-component-issue-draft.md")
	prose, _ := split(t, doc)
	// The field labels of .github/ISSUE_TEMPLATE/new_component.yaml at the
	// pinned commit, in form order.
	labels := []string{
		"Building and distributing outside this repository",
		"Existing component implementation",
		"Components covering similar use cases",
		"The purpose and use-cases of the component",
		"Example configuration for the component",
		"Telemetry data types supported",
		"Code Owners",
		"Sponsor (optional)",
		"Additional context",
		"Tip",
	}
	var headings []string
	sections := make(map[string]string)
	inBody, current := false, ""
	for _, line := range prose {
		if strings.HasPrefix(line, "## ") {
			inBody = line == "## Body"
			continue
		}
		if !inBody {
			continue
		}
		if label, ok := strings.CutPrefix(line, "### "); ok {
			headings = append(headings, label)
			current = label
			continue
		}
		sections[current] += line + "\n"
	}
	if !slices.Equal(headings, labels) {
		t.Errorf("body headings = %q; want the form labels %q", headings, labels)
	}
	for _, s := range []string{
		"https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/releases/tag/v0.1.0",
		"https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/blob/v0.1.0/docs/benchmarks/v0.1.0-performance-envelope.md",
		"exporter/googlecloudexporter",
		"`" + ComponentDir + "`",
		"`" + ModulePath + "`",
	} {
		if !bytes.Contains(doc, []byte(s)) {
			t.Errorf("the issue draft does not mention %s", s)
		}
	}
	for _, owner := range []string{"@haiyuan-eng-google", "@&lt;sponsor&gt;", "@&lt;codeowner-3&gt;"} {
		if !strings.Contains(sections["Code Owners"], owner) {
			t.Errorf("the Code Owners section does not list %s", owner)
		}
	}
}

var hunkRE = regexp.MustCompile(`^@@ -([1-9][0-9]*)(?:,([0-9]+))? \+[0-9]+(?:,([0-9]+))? @@`)

// applyDiff applies a unified diff of one file to old. It fails the test when
// a context or removed line does not match old, which is when `git apply
// --check` reports that the patch does not apply.
func applyDiff(t *testing.T, old []byte, diff string) []byte {
	t.Helper()
	count := func(s string) int {
		if s == "" {
			return 1
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	src := strings.Split(string(old), "\n")
	lines := strings.Split(strings.TrimSuffix(diff, "\n"), "\n")
	if len(lines) < 3 || !strings.HasPrefix(lines[0], "--- a/") || !strings.HasPrefix(lines[1], "+++ b/") {
		t.Fatalf("diff does not start with --- a/ and +++ b/ lines:\n%s", diff)
	}
	var out []string
	next := 0 // the index in src of the next line to copy
	for i := 2; i < len(lines); {
		m := hunkRE.FindStringSubmatch(lines[i])
		if m == nil {
			t.Fatalf("diff line %q is not a hunk header", lines[i])
		}
		start := count(m[1]) - 1
		if start < next || start > len(src) {
			t.Fatalf("hunk %q starts outside the file", m[0])
		}
		out = append(out, src[next:start]...)
		next = start
		oldLines, newLines := 0, 0
		for i++; i < len(lines) && !strings.HasPrefix(lines[i], "@@"); i++ {
			op, text := byte(' '), "" // an empty line is an empty context line
			if lines[i] != "" {
				op, text = lines[i][0], lines[i][1:]
			}
			switch op {
			case ' ', '-':
				if next >= len(src) || src[next] != text {
					t.Fatalf("the diff expects line %d to be %q; the file does not match", next+1, text)
				}
				if op == ' ' {
					out = append(out, text)
					newLines++
				}
				oldLines++
				next++
			case '+':
				out = append(out, text)
				newLines++
			default:
				t.Fatalf("unexpected diff line %q", lines[i])
			}
		}
		if oldLines != count(m[2]) || newLines != count(m[3]) {
			t.Fatalf("hunk %q has %d old and %d new lines", m[0], oldLines, newLines)
		}
	}
	return []byte(strings.Join(append(out, src[next:]...), "\n"))
}

// TestGovernanceDiffsApply checks that the codeowner diffs staged in
// outreach-and-governance.md still apply to this repository and list the
// same placeholders as the staged contrib metadata.yaml. Once the change
// lands, the diffs no longer apply and the page has to record it.
func TestGovernanceDiffsApply(t *testing.T) {
	_, blocks := split(t, readDoc(t, "outreach-and-governance.md"))
	applied := make(map[string][]byte)
	for _, b := range blocks {
		if b.info != "diff" {
			continue
		}
		first, _, _ := strings.Cut(b.body, "\n")
		name, ok := strings.CutPrefix(first, "--- a/")
		if !ok {
			t.Fatalf("diff at line %d does not start with a --- a/ line", b.line)
		}
		if _, dup := applied[name]; dup {
			t.Fatalf("diff at line %d changes %s again", b.line, name)
		}
		old, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		applied[name] = applyDiff(t, old, b.body)
	}
	if got := slices.Sorted(maps.Keys(applied)); !slices.Equal(got, []string{"CODEOWNERS", "metadata.yaml"}) {
		t.Fatalf("the diffs change %q; want CODEOWNERS and metadata.yaml", got)
	}

	var meta struct {
		Status struct {
			Codeowners struct {
				Active []string `yaml:"active"`
			} `yaml:"codeowners"`
		} `yaml:"status"`
	}
	if err := yaml.Unmarshal(applied["metadata.yaml"], &meta); err != nil {
		t.Fatal(err)
	}
	active := meta.Status.Codeowners.Active
	var owners []string
	for _, line := range strings.Split(string(applied["CODEOWNERS"]), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == "*" {
			for _, f := range fields[1:] {
				owners = append(owners, strings.TrimPrefix(f, "@"))
			}
		}
	}
	if !slices.Contains(active, "haiyuan-eng-google") ||
		!slices.Equal(slices.Sorted(slices.Values(owners)), slices.Sorted(slices.Values(active))) {
		t.Errorf("after the diffs, CODEOWNERS lists %q and metadata.yaml lists %q; want the same set, with haiyuan-eng-google", owners, active)
	}
	var placeholders []string
	for _, h := range active {
		if placeholderRE.MatchString(h) {
			placeholders = append(placeholders, h)
		}
	}
	slices.Sort(placeholders)
	pending, err := PendingCodeowners(stagedTree(t, stagedFiles(t), 3)["metadata.yaml"])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(placeholders, pending) {
		t.Errorf("the diffs add the placeholders %q; the staged contrib metadata.yaml has %q", placeholders, pending)
	}
}

// TestExampleConfigsDecode checks every exporters.bigquery example in the
// staged READMEs and in the issue draft against this library's
// configuration, which the component's Config aliases.
func TestExampleConfigsDecode(t *testing.T) {
	sources := map[string][]byte{"new-component-issue-draft.md": readDoc(t, "new-component-issue-draft.md")}
	for _, f := range stagedFiles(t) {
		if f.Path == ComponentDir+"/README.md" {
			sources[fmt.Sprintf("pull request %d README.md", f.PR)] = f.Body
		}
	}
	if len(sources) != 4 {
		t.Fatalf("found %d sources; want the issue draft and three READMEs", len(sources))
	}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		_, blocks := split(t, sources[name])
		examples := 0
		for _, b := range blocks {
			if b.info != "yaml" {
				continue
			}
			where := fmt.Sprintf("%s, block at line %d", name, b.line)
			var conf map[string]any
			if err := yaml.Unmarshal([]byte(b.body), &conf); err != nil {
				t.Errorf("%s: %v", where, err)
				continue
			}
			exporters, _ := conf["exporters"].(map[string]any)
			section, ok := exporters["bigquery"].(map[string]any)
			if !ok {
				continue
			}
			examples++
			cfg := bqotel.NewFactory().CreateDefaultConfig()
			if err := confmap.NewFromStringMap(section).Unmarshal(cfg); err != nil {
				t.Errorf("%s: the bigquery section does not decode: %v", where, err)
				continue
			}
			if err := confmap.Validate(cfg); err != nil {
				t.Errorf("%s: the bigquery configuration is invalid: %v", where, err)
			}
		}
		if examples == 0 {
			t.Errorf("%s has no exporters.bigquery example", name)
		}
	}
}

// TestMdatagen runs mdatagen on each staged tree when CONTRIBCHECK_MDATAGEN
// names a binary.
func TestMdatagen(t *testing.T) {
	bin := os.Getenv("CONTRIBCHECK_MDATAGEN")
	if bin == "" {
		t.Skip("set CONTRIBCHECK_MDATAGEN to an mdatagen binary to run it on the staged trees")
	}
	levels := map[string]string{"development": "Development", "alpha": "Alpha"}
	files := stagedFiles(t)
	for _, e := range expects {
		t.Run("pr"+strconv.Itoa(e.pr), func(t *testing.T) {
			tree := stagedTree(t, files, e.pr)
			dir := writeStaged(t, tree)
			cmd := exec.CommandContext(t.Context(), bin, "metadata.yaml")
			cmd.Dir = dir
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("mdatagen metadata.yaml: %v\n%s", err, out)
			}
			readme, err := os.ReadFile(filepath.Join(dir, "README.md"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readme, tree["README.md"]) {
				t.Errorf("mdatagen rewrote the staged README.md:\n%s", readme)
			}
			status, err := os.ReadFile(filepath.Join(dir, "internal", "metadata", "generated_status.go"))
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				`Type\s+= component\.MustNewType\("bigquery"\)`,
				`ScopeName\s+= "` + regexp.QuoteMeta(ModulePath) + `"`,
				`TracesStability\s+= component\.StabilityLevel` + levels[e.want.Stability],
				`LogsStability\s+= component\.StabilityLevel` + levels[e.want.Stability],
				`MetricsStability\s+= component\.StabilityLevel` + levels[e.want.Stability],
			} {
				if !regexp.MustCompile(`(?m)^\s*` + want + `$`).Match(status) {
					t.Errorf("generated_status.go has no line matching %s:\n%s", want, status)
				}
			}
			problems, err := CheckTree(dir, e.want)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range problems {
				t.Errorf("after mdatagen: %s", p)
			}
		})
	}
}
