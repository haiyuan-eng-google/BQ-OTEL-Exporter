// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const secret = "sk-live-0123456789abcdef"

func mustNew(t *testing.T, cfg Config) *Redactor {
	t.Helper()
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func defaultRedactor(t *testing.T, patterns ...string) *Redactor {
	t.Helper()
	return mustNew(t, Config{
		BlockedKeys:          DefaultBlockedKeys(),
		BlockedValuePatterns: patterns,
		RedactLogBody:        true,
	})
}

func TestNewRejectsInvalidInputs(t *testing.T) {
	cases := map[string]Config{
		"bad regex":     {BlockedKeys: []string{"a"}, BlockedValuePatterns: []string{"("}},
		"empty pattern": {BlockedKeys: []string{"a"}, BlockedValuePatterns: []string{"  "}},
		"empty key":     {BlockedKeys: []string{"ok", " "}},
		"nothing":       {},
	}
	for name, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestCompilePatternsNamesTheOffendingEntry(t *testing.T) {
	_, err := CompilePatterns([]string{"ok", "(unclosed"})
	if err == nil || !strings.Contains(err.Error(), "blocked_value_patterns[1]") {
		t.Fatalf("error = %v, want position of the invalid pattern", err)
	}
}

func TestKeyBlockedExactAndGlobCaseInsensitive(t *testing.T) {
	r := mustNew(t, Config{BlockedKeys: []string{"Authorization", "*Token*", "x-*-key", "*_secret"}})
	yes := []string{"authorization", "AUTHORIZATION", " authorization ", "access_token", "TOKEN", "refresh-token-v2", "x-api-key", "x--key", "client_secret"}
	no := []string{"author", "authorization_header", "x-key", "secret_client", "toke", "", "service.name"}
	for _, k := range yes {
		if !r.KeyBlocked(k) {
			t.Errorf("KeyBlocked(%q) = false, want true", k)
		}
	}
	for _, k := range no {
		if r.KeyBlocked(k) {
			t.Errorf("KeyBlocked(%q) = true, want false", k)
		}
	}
	var nilR *Redactor
	if nilR.KeyBlocked("authorization") {
		t.Error("nil Redactor must block nothing")
	}
}

func TestWildcardMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"**", "x", true},
		{"a*", "abc", true},
		{"a*", "ba", false},
		{"*c", "abc", true},
		{"*c", "abcd", false},
		{"a*c", "ac", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbbd", false},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "aXcYb", false},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"", "", true},
		{"", "a", false},
	}
	for _, tc := range cases {
		if got := wildcardMatch(tc.pattern, tc.s); got != tc.want {
			t.Errorf("wildcardMatch(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

func TestRedactMapKeysValuesAndNesting(t *testing.T) {
	r := defaultRedactor(t, `sk-live-[0-9a-f]+`, `(?i)bearer\s+\S+`)
	m := pcommon.NewMap()
	m.PutStr("service.name", "checkout")
	m.PutStr("Authorization", "Bearer abc.def")
	m.PutInt("api_key", 42) // non-string value under a blocked key is replaced too
	m.PutStr("note", "token is "+secret+" ok")
	nested := m.PutEmptyMap("http")
	nested.PutStr("cookie", "session=1")
	nested.PutStr("header", "Bearer zzz")
	deep := nested.PutEmptyMap("client_secret")
	deep.PutStr("inner", "should vanish with the parent")
	sl := m.PutEmptySlice("list")
	sl.AppendEmpty().SetStr("plain")
	sl.AppendEmpty().SetStr(secret)
	sl.AppendEmpty().SetInt(7)
	m.PutDouble("price", 1.5)
	m.PutBool("ok", true)
	m.PutEmptyBytes("blob").FromRaw([]byte(secret))

	keys, values := r.RedactMap(m)
	if keys != 4 {
		t.Errorf("keys = %d, want 4 (Authorization, api_key, http.cookie, http.client_secret)", keys)
	}
	if values != 3 {
		t.Errorf("values = %d, want 3 (note, http.header, list[1])", values)
	}

	get := func(path ...string) pcommon.Value {
		t.Helper()
		cur := m
		for i, p := range path {
			v, ok := cur.Get(p)
			if !ok {
				t.Fatalf("missing %v", path[:i+1])
			}
			if i == len(path)-1 {
				return v
			}
			cur = v.Map()
		}
		return pcommon.NewValueEmpty()
	}
	if got := get("service.name").Str(); got != "checkout" {
		t.Errorf("service.name = %q, must be untouched", got)
	}
	if got := get("Authorization").Str(); got != DefaultMaskReplacement {
		t.Errorf("Authorization = %q", got)
	}
	if v := get("api_key"); v.Type() != pcommon.ValueTypeStr || v.Str() != DefaultMaskReplacement {
		t.Errorf("api_key = %v (%s), want masked string", v.AsString(), v.Type())
	}
	if got := get("note").Str(); got != "token is "+DefaultMaskReplacement+" ok" {
		t.Errorf("note = %q", got)
	}
	if got := get("http", "cookie").Str(); got != DefaultMaskReplacement {
		t.Errorf("http.cookie = %q", got)
	}
	if got := get("http", "header").Str(); got != DefaultMaskReplacement {
		t.Errorf("http.header = %q", got)
	}
	if v := get("http", "client_secret"); v.Type() != pcommon.ValueTypeStr || v.Str() != DefaultMaskReplacement {
		t.Errorf("http.client_secret = %v, want the whole nested map replaced", v.AsString())
	}
	list := get("list").Slice()
	if list.At(0).Str() != "plain" || list.At(1).Str() != DefaultMaskReplacement || list.At(2).Int() != 7 {
		t.Errorf("list = %v", list.AsRaw())
	}
	if !bytes.Equal(get("blob").Bytes().AsRaw(), []byte(secret)) {
		t.Error("bytes values must not be scanned or modified")
	}
	if get("price").Double() != 1.5 || !get("ok").Bool() {
		t.Error("scalar non-string values must be untouched")
	}
}

func TestMaskReplacementIsLiteralNotTemplate(t *testing.T) {
	r := mustNew(t, Config{BlockedValuePatterns: []string{`(sk-\w+)`}, MaskReplacement: "$1<hidden>"})
	m := pcommon.NewMap()
	m.PutStr("k", "x sk-abc y")
	r.RedactMap(m)
	v, _ := m.Get("k")
	if v.Str() != "x $1<hidden> y" {
		t.Fatalf("got %q, want the mask inserted literally", v.Str())
	}
}

func TestEmptyMaskFallsBackToDefault(t *testing.T) {
	r := mustNew(t, Config{BlockedKeys: []string{"password"}})
	m := pcommon.NewMap()
	m.PutStr("password", "hunter2")
	r.RedactMap(m)
	v, _ := m.Get("password")
	if v.Str() != DefaultMaskReplacement {
		t.Fatalf("got %q", v.Str())
	}
}

func buildTraces(withSecret bool) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	if withSecret {
		rs.Resource().Attributes().PutStr("cloud.account.password", "p")
	}
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("scope")
	ss.Scope().Attributes().PutStr("scope.note", "k")
	for i := 0; i < 3; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("span")
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(i + 1)}))
		sp.Attributes().PutStr("gen_ai.prompt", "hello")
		if withSecret {
			sp.Attributes().PutStr("gen_ai.prompt.session_token", "x")
			sp.Events().AppendEmpty().Attributes().PutStr("value", "Bearer "+secret)
			sp.Links().AppendEmpty().Attributes().PutStr("authorization", "x")
		}
	}
	return td
}

func marshalTraces(t *testing.T, td ptrace.Traces) []byte {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTracesCopyOnWriteNeverMutatesInputAndReturnsSameWhenClean(t *testing.T) {
	var gotKeys, gotValues int
	r := mustNew(t, Config{
		BlockedKeys:          DefaultBlockedKeys(),
		BlockedValuePatterns: []string{`(?i)bearer\s+\S+`},
		OnRedact: func(mt string, n int) {
			switch mt {
			case MatchKey:
				gotKeys += n
			case MatchValue:
				gotValues += n
			default:
				t.Errorf("unexpected match type %q", mt)
			}
		},
	})

	// Clean batch: the same pdata object comes back, no clone, no callback.
	clean := buildTraces(false)
	cleanBefore := marshalTraces(t, clean)
	out := r.Traces(clean)
	if out != clean {
		t.Error("clean batch must be returned as-is (no clone)")
	}
	if gotKeys+gotValues != 0 {
		t.Errorf("OnRedact fired on a clean batch: keys=%d values=%d", gotKeys, gotValues)
	}
	if after := marshalTraces(t, clean); !bytes.Equal(cleanBefore, after) {
		t.Fatal("clean input was mutated")
	}

	in := buildTraces(true)
	before := marshalTraces(t, in)
	gotKeys, gotValues = 0, 0
	out = r.Traces(in)
	if out == in {
		t.Fatal("matching batch must be cloned, not returned as-is")
	}
	if after := marshalTraces(t, in); !bytes.Equal(before, after) {
		t.Fatal("input traces were mutated")
	}
	js := string(marshalTraces(t, out))
	for _, leak := range []string{secret, `"p"`, `"x"`} {
		if strings.Contains(js, leak) {
			t.Errorf("redacted output still contains %q:\n%s", leak, js)
		}
	}
	if !strings.Contains(js, "hello") {
		t.Error("non-sensitive attribute was dropped")
	}
	// resource password (1) + per span: session_token key + link authorization key (2*3) = 7 keys
	if gotKeys != 7 {
		t.Errorf("OnRedact keys = %d, want 7", gotKeys)
	}
	// per span: one event value containing a bearer token = 3 values
	if gotValues != 3 {
		t.Errorf("OnRedact values = %d, want 3", gotValues)
	}
}

func TestTracesScanCoversScopeOnly(t *testing.T) {
	r := mustNew(t, Config{BlockedKeys: []string{"scope.note"}})
	in := buildTraces(false)
	out := r.Traces(in)
	if out == in {
		t.Fatal("a scope-level match must trigger the clone")
	}
	v, _ := out.ResourceSpans().At(0).ScopeSpans().At(0).Scope().Attributes().Get("scope.note")
	if v.Str() != DefaultMaskReplacement {
		t.Fatalf("scope attribute = %q", v.Str())
	}
	orig, _ := in.ResourceSpans().At(0).ScopeSpans().At(0).Scope().Attributes().Get("scope.note")
	if orig.Str() != "k" {
		t.Fatal("input scope attribute was mutated")
	}
}

func TestLogsRedactBodyOnlyWhenEnabled(t *testing.T) {
	build := func() plog.Logs {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "svc")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Attributes().PutStr("user", "u1")
		body := lr.Body().SetEmptyMap()
		body.PutStr("message", "token "+secret)
		body.PutStr("password", "pw")
		return ld
	}
	marshal := func(ld plog.Logs) string {
		b, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	withBody := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys(), BlockedValuePatterns: []string{`sk-live-\w+`}, RedactLogBody: true})
	in := build()
	before := marshal(in)
	out := withBody.Logs(in)
	if out == in {
		t.Fatal("body match must clone")
	}
	if marshal(in) != before {
		t.Fatal("input logs mutated")
	}
	js := marshal(out)
	if strings.Contains(js, secret) || strings.Contains(js, `"pw"`) {
		t.Fatalf("body not redacted: %s", js)
	}

	withoutBody := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys(), BlockedValuePatterns: []string{`sk-live-\w+`}, RedactLogBody: false})
	in2 := build()
	if out2 := withoutBody.Logs(in2); out2 != in2 {
		t.Fatal("with redact_log_body off and clean attributes, the batch must be returned as-is")
	}
}

func buildMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	g := sm.Metrics().AppendEmpty()
	g.SetName("gauge")
	g.Metadata().PutStr("x-api-key", "meta")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("access_token", "t")
	ex := dp.Exemplars().AppendEmpty()
	ex.FilteredAttributes().PutStr("note", "Bearer "+secret)
	h := sm.Metrics().AppendEmpty()
	h.SetName("hist")
	hp := h.SetEmptyHistogram().DataPoints().AppendEmpty()
	hp.Attributes().PutStr("client_secret", "c")
	hp.Exemplars().AppendEmpty().FilteredAttributes().PutStr("password", "p")
	e := sm.Metrics().AppendEmpty()
	e.SetName("exp")
	ep := e.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	ep.Attributes().PutStr("id_token", "i")
	s := sm.Metrics().AppendEmpty()
	s.SetName("sum")
	s.SetEmptySum().DataPoints().AppendEmpty().Attributes().PutStr("refresh_token", "r")
	su := sm.Metrics().AppendEmpty()
	su.SetName("summary")
	su.SetEmptySummary().DataPoints().AppendEmpty().Attributes().PutStr("private_key", "k")
	return md
}

func TestMetricsRedactMetadataPointsAndExemplarsAcrossAllTypes(t *testing.T) {
	r := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys(), BlockedValuePatterns: []string{`(?i)bearer\s+\S+`}})
	in := buildMetrics()
	marshal := func(md pmetric.Metrics) string {
		b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	before := marshal(in)
	out := r.Metrics(in)
	if out == in {
		t.Fatal("expected clone")
	}
	if marshal(in) != before {
		t.Fatal("input metrics mutated")
	}
	js := marshal(out)
	for _, leak := range []string{secret, `"meta"`, `"t"`, `"c"`, `"p"`, `"i"`, `"r"`, `"k"`} {
		if strings.Contains(js, leak) {
			t.Errorf("leak %q in %s", leak, js)
		}
	}
	keys, values := r.RedactMetrics(in) // in-place variant on the original now
	if keys != 7 || values != 1 {
		t.Errorf("RedactMetrics = (%d keys, %d values), want (7, 1)", keys, values)
	}
	if marshal(in) != js {
		t.Error("in-place redaction must produce the same result as the copy-on-write path")
	}
}

func TestNilRedactorIsANoOp(t *testing.T) {
	var r *Redactor
	td := buildTraces(true)
	if r.Traces(td) != td {
		t.Error("nil.Traces must return input")
	}
	ld := plog.NewLogs()
	if r.Logs(ld) != ld {
		t.Error("nil.Logs must return input")
	}
	md := buildMetrics()
	if r.Metrics(md) != md {
		t.Error("nil.Metrics must return input")
	}
	if k, v := r.RedactTraces(td); k != 0 || v != 0 {
		t.Error("nil.RedactTraces must count nothing")
	}
	if k, v := r.RedactLogs(ld); k != 0 || v != 0 {
		t.Error("nil.RedactLogs must count nothing")
	}
	if k, v := r.RedactMap(pcommon.NewMap()); k != 0 || v != 0 {
		t.Error("nil.RedactMap must count nothing")
	}
}

func TestOnRedactNeverFiresFromInPlaceVariants(t *testing.T) {
	fired := 0
	r := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys(), OnRedact: func(string, int) { fired++ }})
	r.RedactTraces(buildTraces(true))
	r.RedactLogs(plog.NewLogs())
	r.RedactMetrics(buildMetrics())
	m := pcommon.NewMap()
	m.PutStr("password", "x")
	r.RedactMap(m)
	if fired != 0 {
		t.Fatalf("OnRedact fired %d times from in-place variants; want 0", fired)
	}
	r.Traces(buildTraces(true))
	if fired == 0 {
		t.Fatal("OnRedact must fire from the copy-on-write path")
	}
}

// TestDefaultBlockedKeysSpareGenAISemconv pins the boundary of the default
// list: credential-shaped keys are masked, including camelCase and mid-key
// forms of secret, password, and private_key, while the GenAI usage counters
// and enums that the GenAI views read are not.
func TestDefaultBlockedKeysSpareGenAISemconv(t *testing.T) {
	r := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys()})
	blocked := []string{
		"authorization", "Proxy-Authorization", "http.request.header.authorization",
		"cookie", "set-cookie", "http.response.header.set-cookie",
		"x-api-key", "http.request.header.x-api-key", "api_key", "apikey", "openai_api_key", "OPENAI_APIKEY", "llm.api_key",
		"secret", "client_secret", "webhook-secret", "app.secret", "secret_key", "aws_secret_access_key", "secret_question",
		"db.secret.value", "oauthClientSecret", "secrets_manager.version",
		"access_token", "refresh_token", "id_token", "token", "session_token", "x-auth-token", "auth.token", "csrf_token",
		"accessToken", "authToken", "k8s.serviceaccount.token",
		"password", "passwd", "db.password", "smtp_password", "password_hash", "ldap_passwd", "db.passwd", "userPassword", "passwordless",
		"private_key", "ssh.private_key", "tls_private_key", "private_key_pem", "my_private_key_id", "credentials",
	}
	spared := []string{
		"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "gen_ai.usage.cache_read.input_tokens",
		"gen_ai.usage.cache_creation_input_tokens", "gen_ai.usage.reasoning_tokens", "gen_ai.usage.prompt_tokens",
		"gen_ai.token.type", "llm.token_count.prompt", "llm.token_count.completion", "llm.usage.total_tokens",
		"gen_ai.request.max_tokens", "tokenizer", "token_endpoint", "tokens_per_second",
		"service.name", "gen_ai.system", "gen_ai.request.model", "gen_ai.conversation.id", "session.id", "user.id",
		"author", "authorization_scope", "cookies_enabled",
	}
	for _, k := range blocked {
		if !r.KeyBlocked(k) {
			t.Errorf("default list must block %q", k)
		}
	}
	for _, k := range spared {
		if r.KeyBlocked(k) {
			t.Errorf("default list must not block %q", k)
		}
	}
}

// TestBroadTokenGlobIsOptIn documents the one place the default list is
// narrower than a plain "*token*": a key with the word in the middle or a
// plural is left alone by default, and an operator who wants the broader form
// adds it to blocked_keys, accepting that the GenAI usage counters go with it.
func TestBroadTokenGlobIsOptIn(t *testing.T) {
	def := mustNew(t, Config{BlockedKeys: DefaultBlockedKeys()})
	broad := mustNew(t, Config{BlockedKeys: append(DefaultBlockedKeys(), "*token*")})
	for _, k := range []string{"auth.tokens", "token_value", "gen_ai.usage.input_tokens"} {
		if def.KeyBlocked(k) {
			t.Errorf("default list must not block %q", k)
		}
		if !broad.KeyBlocked(k) {
			t.Errorf("adding *token* to blocked_keys must block %q", k)
		}
	}
}
