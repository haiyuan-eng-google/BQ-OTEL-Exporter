// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"gopkg.in/yaml.v3"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
)

// The deploy templates (example config, Cloud Run manifest, Compose overlay,
// Helm chart) and testdata/config.yaml are checked here by decoding them the
// way the Collector does and running the exporter's own Validate, not by
// searching their text. A template that a collector would refuse to start with
// fails these tests.

// exporterConfigKeys returns the dotted mapstructure path of every option of
// Config. It is derived by reflection, so an option cannot be added without
// the fixtures and templates being revisited. Structs declared in this package
// are walked; the Collector helper types (sending_queue, retry_on_failure),
// maps, slices and scalars are leaves, and a squashed embedded struct
// contributes its keys at the level of its parent.
func exporterConfigKeys() []string {
	rt := reflect.TypeFor[Config]()
	pkgPath := rt.PkgPath()
	var keys []string
	var walk func(rt reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for i := range rt.NumField() {
			field := rt.Field(i)
			name, opts, _ := strings.Cut(field.Tag.Get("mapstructure"), ",")
			if name == "-" {
				continue
			}
			if slices.Contains(strings.Split(opts, ","), "squash") {
				walk(field.Type, prefix)
				continue
			}
			if name == "" {
				continue
			}
			path := prefix + name
			if field.Type.Kind() == reflect.Struct && field.Type.PkgPath() == pkgPath {
				walk(field.Type, path+".")
				continue
			}
			keys = append(keys, path)
		}
	}
	walk(rt, "")
	sort.Strings(keys)
	return keys
}

// presentExporterKeys returns the exporter options a decoded YAML section sets,
// using the dotted paths of exporterConfigKeys. A key that is itself an option
// (a leaf such as labels) is reported without descending into it.
func presentExporterKeys(section map[string]any, options map[string]bool, prefix string) []string {
	var present []string
	for key, value := range section {
		path := prefix + key
		if options[path] {
			present = append(present, path)
			continue
		}
		if nested, ok := value.(map[string]any); ok {
			present = append(present, presentExporterKeys(nested, options, path+".")...)
		}
	}
	sort.Strings(present)
	return present
}

func exporterKeySet() map[string]bool {
	set := map[string]bool{}
	for _, key := range exporterConfigKeys() {
		set[key] = true
	}
	return set
}

// deployEnvironment holds the values the deploy templates read from the
// environment (the example config, Compose, Cloud Run and the Helm ConfigMap
// all take the destination from BQ_PROJECT, BQ_DATASET and BQ_LOCATION).
var deployEnvironment = strings.NewReplacer(
	"${env:BQ_PROJECT}", "my-project",
	"${env:BQ_DATASET}", "otel",
	"${env:BQ_LOCATION}", "US",
)

// expandDeployEnv resolves the environment references a template contains and
// fails when it contains one this test does not provide.
func expandDeployEnv(t *testing.T, name, text string) string {
	t.Helper()
	out := deployEnvironment.Replace(text)
	if i := strings.Index(out, "${"); i >= 0 {
		t.Fatalf("%s references an unexpected placeholder near %q", name, out[i:min(len(out), i+40)])
	}
	return out
}

func parseYAMLMap(t *testing.T, name, text string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("%s is not valid YAML: %v", name, err)
	}
	if doc == nil {
		t.Fatalf("%s is empty", name)
	}
	return doc
}

// yamlPath returns the value at the given key path of a decoded YAML document,
// or nil when any step is missing.
func yamlPath(doc any, keys ...string) any {
	for _, key := range keys {
		m, ok := doc.(map[string]any)
		if !ok {
			return nil
		}
		doc = m[key]
	}
	return doc
}

// exampleCollectorConfig returns the bundled example Collector configuration
// with its environment references resolved.
func exampleCollectorConfig(t *testing.T) map[string]any {
	t.Helper()
	const path = "example/otel-collector-config.yml"
	return parseYAMLMap(t, path, expandDeployEnv(t, path, readRepoText(t, path)))
}

// exporterBlock returns the exporters::<id> section of a Collector
// configuration.
func exporterBlock(t *testing.T, conf *confmap.Conf, id string) map[string]any {
	t.Helper()
	sub, err := conf.Sub("exporters::" + id)
	if err != nil {
		t.Fatalf("reading exporters::%s: %v", id, err)
	}
	block := sub.ToStringMap()
	if len(block) == 0 {
		t.Fatalf("the configuration has no exporters::%s section", id)
	}
	return block
}

// decodeExporterConfig decodes an exporter section onto the factory defaults
// the way the Collector does, where an unknown or mistyped key is an error, and
// then runs Validate. It is the check that a collector started with the
// configuration would accept it.
func decodeExporterConfig(t *testing.T, name string, section map[string]any) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(section).Unmarshal(cfg); err != nil {
		t.Fatalf("%s: the bigquery exporter section does not decode: %v", name, err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("%s: the bigquery exporter configuration is invalid: %v", name, err)
	}
	return cfg
}

// applyCollectorYAML merges one `--config=yaml:<document>` argument over conf.
// Keys may be written as `a::b::c`, which the Collector expands to nested maps.
func applyCollectorYAML(t *testing.T, conf *confmap.Conf, document string) {
	t.Helper()
	var overlay map[string]any
	if err := yaml.Unmarshal([]byte(document), &overlay); err != nil {
		t.Fatalf("overlay %q is not valid YAML: %v", document, err)
	}
	if err := conf.Merge(confmap.NewFromStringMap(overlay)); err != nil {
		t.Fatalf("merging overlay %q: %v", document, err)
	}
}

// applyCollectorSet merges one `--set=a.b.c=value` argument over conf. The
// Collector turns it into the `yaml:` document `a::b::c: value`, so the value
// is parsed as YAML and replaces whatever the configuration had.
func applyCollectorSet(t *testing.T, conf *confmap.Conf, arg string) {
	t.Helper()
	spec, ok := strings.CutPrefix(arg, "--set=")
	if !ok {
		t.Fatalf("%q is not a --set argument", arg)
	}
	key, value, ok := strings.Cut(spec, "=")
	if !ok {
		t.Fatalf("--set argument %q has no value", arg)
	}
	applyCollectorYAML(t, conf, strings.ReplaceAll(key, ".", "::")+": "+value)
}

func TestExporterConfigKeysAreEnumeratedByReflection(t *testing.T) {
	keys := exporterConfigKeys()
	for _, want := range []string{
		"timeout",
		"sending_queue",
		"retry_on_failure",
		"project",
		"dataset",
		"location",
		"kms_key_name",
		"labels",
		"traces.max_staleness",
		"logs.insert_id_attribute",
		"logs.log_name_attribute",
		"metrics.partition_expiration_days",
		"credentials.json",
		"endpoint.without_authentication",
		"auto_create.search_indexes",
		"write.keepalive.permit_without_stream",
		"limits.max_attribute_key_bytes",
		"quarantine.max_staleness",
		"offload.cache_size",
		"redaction.redact_log_body",
	} {
		if !slices.Contains(keys, want) {
			t.Errorf("exporterConfigKeys() has no %q; the reflection walk is not reaching it", want)
		}
	}
	for _, key := range keys {
		if strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") || strings.Contains(key, "..") {
			t.Errorf("malformed option path %q", key)
		}
	}
	if len(keys) < 80 {
		t.Errorf("exporterConfigKeys() found %d options, want at least 80: %v", len(keys), keys)
	}
}

// testdata/config.yaml is the fixture of every option the exporter accepts. No
// single section can set all of them (the three credential sources and the
// loopback emulator endpoint exclude one another), so the check is the union of
// the exporters::bigquery* sections: each must decode strictly and validate,
// and together they must set every option that reflection finds on Config.
func TestTestdataConfigSetsEveryExporterOption(t *testing.T) {
	doc := parseYAMLMap(t, "testdata/config.yaml", readRepoText(t, "testdata/config.yaml"))
	exporters, ok := doc["exporters"].(map[string]any)
	if !ok {
		t.Fatal("testdata/config.yaml has no exporters section")
	}
	options := exporterKeySet()
	set := map[string]bool{}
	var ids []string
	for id := range exporters {
		if id == "bigquery" || strings.HasPrefix(id, "bigquery/") {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) < 2 {
		t.Fatalf("testdata/config.yaml defines exporter sections %v, want a minimal and a full one at least", ids)
	}
	for _, id := range ids {
		section, ok := exporters[id].(map[string]any)
		if !ok {
			t.Fatalf("exporters::%s is not a mapping", id)
		}
		t.Run(id, func(t *testing.T) { decodeExporterConfig(t, id, section) })
		for _, key := range presentExporterKeys(section, options, "") {
			set[key] = true
		}
	}
	for _, key := range exporterConfigKeys() {
		if !set[key] {
			t.Errorf("no exporters::bigquery* section of testdata/config.yaml sets %q", key)
		}
	}
}

func TestExampleCollectorConfigDecodesAndWiresEverySignal(t *testing.T) {
	doc := exampleCollectorConfig(t)
	exporters, _ := doc["exporters"].(map[string]any)
	var bigquery []string
	for id := range exporters {
		if id == "bigquery" || strings.HasPrefix(id, "bigquery/") {
			bigquery = append(bigquery, id)
		}
	}
	if !slices.Equal(bigquery, []string{"bigquery"}) {
		t.Fatalf("the example configures the bigquery exporters %v, want exactly bigquery", bigquery)
	}
	cfg := decodeExporterConfig(t, "example config", exporters["bigquery"].(map[string]any))

	// auto_create is what the image does by default, and the views and indexes
	// stay opt-in, so a Cloud Run service that turns table creation off does
	// not inherit an option that requires it.
	if !cfg.AutoCreate.Dataset || !cfg.AutoCreate.Tables {
		t.Errorf("example auto_create = %+v, want dataset and tables creation on", cfg.AutoCreate)
	}
	if cfg.AutoCreate.CompanionViews || cfg.AutoCreate.SearchIndexes {
		t.Errorf("example auto_create = %+v, want companion_views and search_indexes off (documented as opt-in)", cfg.AutoCreate)
	}
	for _, signal := range []string{"traces", "logs", "metrics"} {
		pipeline := yamlPath(doc, "service", "pipelines", signal)
		if pipeline == nil {
			t.Errorf("the example has no %s pipeline", signal)
			continue
		}
		if got := yamlPath(pipeline, "exporters"); !reflect.DeepEqual(got, []any{"bigquery"}) {
			t.Errorf("%s pipeline exporters = %v, want [bigquery]", signal, got)
		}
		if got := yamlPath(pipeline, "receivers"); !reflect.DeepEqual(got, []any{"otlp"}) {
			t.Errorf("%s pipeline receivers = %v, want [otlp]", signal, got)
		}
	}
	if cfg.Metrics.Table != "otel_metrics" || cfg.Traces.Table != "otel_spans" || cfg.Logs.Table != "otel_logs" {
		t.Errorf("example tables = %q, %q, %q", cfg.Traces.Table, cfg.Logs.Table, cfg.Metrics.Table)
	}
}

// The Compose smoke test starts the example with the loopback endpoint and
// auto_create overlays layered on by `--config=yaml:` arguments.
func TestComposeSmokeOverlayDecodesAndValidates(t *testing.T) {
	var compose struct {
		Services map[string]struct {
			Command []string `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, ".github/compose-smoke.yml")), &compose); err != nil {
		t.Fatal(err)
	}
	command := compose.Services["otelcollector"].Command
	if !slices.Contains(command, "--config=/etc/otel/config.yml") {
		t.Fatalf("compose-smoke command = %v, want the bundled config first", command)
	}
	conf := confmap.NewFromStringMap(exampleCollectorConfig(t))
	overlays := 0
	for _, arg := range command {
		if document, ok := strings.CutPrefix(arg, "--config=yaml:"); ok {
			applyCollectorYAML(t, conf, document)
			overlays++
		}
	}
	if overlays == 0 {
		t.Fatalf("compose-smoke command = %v, want yaml: overlays", command)
	}
	cfg := decodeExporterConfig(t, "example config with the compose-smoke overlays", exporterBlock(t, conf, "bigquery"))
	if !cfg.Endpoint.Insecure || !cfg.Endpoint.WithoutAuthentication || cfg.Endpoint.URL == "" {
		t.Errorf("compose-smoke endpoint = %+v, want an insecure unauthenticated loopback endpoint", cfg.Endpoint)
	}
	if cfg.AutoCreate != (AutoCreateConfig{}) {
		t.Errorf("compose-smoke auto_create = %+v, want everything off", cfg.AutoCreate)
	}
}

func cloudRunArgs(t *testing.T) []string {
	t.Helper()
	var svc struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Args []string `yaml:"args"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, "deploy/cloudrun/service.yaml")), &svc); err != nil {
		t.Fatal(err)
	}
	containers := svc.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("Cloud Run service has %d containers, want 1", len(containers))
	}
	return containers[0].Args
}

// The Cloud Run container starts the bundled config and turns off whatever
// would create resources, because the service account may only append rows.
// Every --set merges over the bundled config, so the bundled options that
// depend on the ones turned off must not leave the result invalid.
func TestCloudRunTemplateValid(t *testing.T) {
	args := cloudRunArgs(t)
	if !slices.Contains(args, "--config=/etc/otel/config.yml") {
		t.Fatalf("Cloud Run args = %v, want --config=/etc/otel/config.yml", args)
	}
	conf := confmap.NewFromStringMap(exampleCollectorConfig(t))
	for _, arg := range args {
		if strings.HasPrefix(arg, "--set=") {
			applyCollectorSet(t, conf, arg)
		}
	}
	cfg := decodeExporterConfig(t, "bundled config with the Cloud Run args", exporterBlock(t, conf, "bigquery"))
	if cfg.AutoCreate != (AutoCreateConfig{}) {
		t.Errorf("Cloud Run auto_create = %+v, want every option off: the service account only appends rows", cfg.AutoCreate)
	}
}

// The header of the manifest lists --set overrides to add for the enterprise
// options. Applied together on top of the live args they must give a valid
// configuration, so an option cannot be documented that the manifest's own
// auto_create settings forbid.
func TestCloudRunDocumentedOverridesValidate(t *testing.T) {
	var documented []string
	for _, line := range strings.Split(readRepoText(t, "deploy/cloudrun/service.yaml"), "\n") {
		comment, ok := strings.CutPrefix(strings.TrimSpace(line), "#")
		if !ok {
			continue
		}
		if arg := strings.TrimSpace(comment); strings.HasPrefix(arg, "--set=") {
			documented = append(documented, strings.TrimSpace(strings.TrimSuffix(arg, `\`)))
		}
	}
	if len(documented) == 0 {
		t.Fatal("deploy/cloudrun/service.yaml documents no --set overrides in its header")
	}
	conf := confmap.NewFromStringMap(exampleCollectorConfig(t))
	for _, arg := range append(cloudRunArgs(t), documented...) {
		if strings.HasPrefix(arg, "--set=") {
			applyCollectorSet(t, conf, arg)
		}
	}
	cfg := decodeExporterConfig(t, "bundled config with the Cloud Run args and every documented override", exporterBlock(t, conf, "bigquery"))
	if !cfg.Offload.Enabled || cfg.Offload.Bucket == "" || !cfg.Quarantine.Enabled || !cfg.Redaction.Enabled || cfg.KMSKeyName == "" {
		t.Errorf("the documented overrides did not enable offload, quarantine, redaction and CMEK: %+v", cfg)
	}
}

// helmFuncs are the template functions the chart's ConfigMap uses, with the
// semantics Helm gives them (toYaml from sigs.k8s.io/yaml, nindent and quote
// from sprig).
var helmFuncs = template.FuncMap{
	"toYaml": func(v any) (string, error) {
		var buf bytes.Buffer
		enc := yaml.NewEncoder(&buf)
		enc.SetIndent(2)
		if err := enc.Encode(v); err != nil {
			return "", err
		}
		if err := enc.Close(); err != nil {
			return "", err
		}
		return strings.TrimSuffix(buf.String(), "\n"), nil
	},
	"nindent": func(spaces int, text string) string {
		pad := strings.Repeat(" ", spaces)
		return "\n" + pad + strings.ReplaceAll(text, "\n", "\n"+pad)
	},
	"quote": func(v any) string {
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%q", fmt.Sprint(v))
	},
}

const helmChartDir = "deploy/helm/bigquery-otel-collector"

// helmValues returns the chart's values.yaml merged with overrides. Helm reads
// values files through JSON, so every number there is a float64, while --set
// produces int64 and strings; overrides use those types.
func helmValues(t *testing.T, overrides map[string]any) map[string]any {
	t.Helper()
	values := parseYAMLMap(t, helmChartDir+"/values.yaml", readRepoText(t, helmChartDir+"/values.yaml"))
	var floats func(v any) any
	floats = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				x[k] = floats(e)
			}
		case []any:
			for i, e := range x {
				x[i] = floats(e)
			}
		case int:
			return float64(x)
		}
		return v
	}
	floats(values)
	var merge func(dst, src map[string]any)
	merge = func(dst, src map[string]any) {
		for k, v := range src {
			if sub, ok := v.(map[string]any); ok {
				if existing, ok := dst[k].(map[string]any); ok {
					merge(existing, sub)
					continue
				}
			}
			dst[k] = v
		}
	}
	merge(values, overrides)
	return values
}

// renderHelmConfigMap renders the chart's ConfigMap template with the given
// value overrides and returns the rendered manifest.
func renderHelmConfigMap(t *testing.T, overrides map[string]any) string {
	t.Helper()
	tmpl, err := template.New("configmap.yaml").Funcs(helmFuncs).
		Parse(readRepoText(t, helmChartDir+"/templates/configmap.yaml"))
	if err != nil {
		t.Fatalf("parsing the chart's ConfigMap template: %v", err)
	}
	var out bytes.Buffer
	data := map[string]any{
		"Release": map[string]any{"Name": "bq-otel", "Namespace": "observability"},
		"Values":  helmValues(t, overrides),
	}
	if err := tmpl.Execute(&out, data); err != nil {
		t.Fatalf("rendering the chart's ConfigMap template: %v", err)
	}
	return out.String()
}

// helmCollectorConfig renders the chart and returns the Collector
// configuration its ConfigMap embeds.
func helmCollectorConfig(t *testing.T, overrides map[string]any) map[string]any {
	t.Helper()
	manifest := renderHelmConfigMap(t, overrides)
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal([]byte(manifest), &cm); err != nil {
		t.Fatalf("the rendered ConfigMap is not valid YAML: %v\n%s", err, manifest)
	}
	raw, ok := cm.Data["config.yml"]
	if !ok {
		t.Fatalf("the rendered ConfigMap has no config.yml:\n%s", manifest)
	}
	return parseYAMLMap(t, "the rendered config.yml", expandDeployEnv(t, "the rendered config.yml", raw))
}

// helmEnterpriseValues turns on every optional section of the chart, with
// values written the way --set and --set-string deliver them: counts as int64,
// a numeric label value as int64 and an all-digit bucket name as a string.
func helmEnterpriseValues(maskReplacement string) map[string]any {
	const key = "projects/my-project/locations/us/keyRings/otel/cryptoKeys/"
	signal := func(name string) map[string]any {
		return map[string]any{
			"partitionExpirationDays": int64(90),
			"requirePartitionFilter":  true,
			"kmsKeyName":              key + name,
			"labels":                  map[string]any{"signal": name},
			"maxStaleness":            "15m",
		}
	}
	return map[string]any{
		"bigquery": map[string]any{
			"autoCreateDataset":        true,
			"autoCreateTables":         true,
			"autoCreateCompanionViews": true,
			"autoCreateSearchIndexes":  true,
			"kmsKeyName":               key + "default",
			"labels":                   map[string]any{"env": "prod", "cost-center": int64(1234)},
			"traces":                   signal("traces"),
			"logs":                     signal("logs"),
			"metrics":                  signal("metrics"),
		},
		"offload": map[string]any{
			"enabled":        true,
			"bucket":         "123456",
			"prefix":         "otel",
			"authorizer":     "my-project.us.otel-gcs",
			"minBytes":       int64(1048576),
			"maxObjectBytes": int64(67108864),
			"maxConcurrency": int64(4),
			"timeout":        "30s",
			"cacheSize":      int64(2048),
		},
		"quarantine": map[string]any{
			"enabled":                 true,
			"table":                   "otel_quarantine",
			"metadataOnly":            true,
			"maxRawPayloadBytes":      int64(1048576),
			"timeout":                 "5s",
			"partitionExpirationDays": int64(14),
			"requirePartitionFilter":  true,
			"kmsKeyName":              key + "quarantine",
			"labels":                  map[string]any{"signal": "quarantine"},
			"maxStaleness":            "30m",
		},
		"redaction": map[string]any{
			"enabled":              true,
			"blockedKeys":          []any{"x-custom-secret", "*.ssn"},
			"disableDefaultKeys":   true,
			"blockedValuePatterns": []any{`Bearer [A-Za-z0-9._-]+`, `\b\d{3}-\d{2}-\d{4}\b`},
			"maskReplacement":      maskReplacement,
			"redactLogBody":        true,
		},
	}
}

func TestHelmChartStructure(t *testing.T) {
	var chart struct {
		APIVersion  string `yaml:"apiVersion"`
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
		Type        string `yaml:"type"`
		Version     string `yaml:"version"`
		AppVersion  string `yaml:"appVersion"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, helmChartDir+"/Chart.yaml")), &chart); err != nil {
		t.Fatal(err)
	}
	if chart.APIVersion != "v2" || chart.Name != "bigquery-otel-collector" || chart.Type != "application" ||
		chart.Version == "" || chart.AppVersion == "" {
		t.Errorf("Chart.yaml = %+v, want a v2 application chart named bigquery-otel-collector with a version", chart)
	}
	for _, signal := range []string{"traces", "logs", "metrics"} {
		if !strings.Contains(chart.Description, signal) {
			t.Errorf("Chart.yaml description %q does not mention %s; the chart ships all three pipelines", chart.Description, signal)
		}
	}

	// A template that reads a value values.yaml does not define renders
	// "<no value>" rather than failing, so scan the default render for it.
	for name, overrides := range map[string]map[string]any{
		"defaults":   nil,
		"enterprise": helmEnterpriseValues("[REDACTED]"),
	} {
		if manifest := renderHelmConfigMap(t, overrides); strings.Contains(manifest, "<no value>") {
			t.Errorf("%s render has an undefined value:\n%s", name, manifest)
		}
	}
}

func TestHelmConfigMapDecodesAndValidates(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		doc := helmCollectorConfig(t, nil)
		conf := confmap.NewFromStringMap(doc)
		cfg := decodeExporterConfig(t, "default values", exporterBlock(t, conf, "bigquery"))
		if cfg.Metrics.Table != "otel_metrics" {
			t.Errorf("metrics.table = %q, want otel_metrics", cfg.Metrics.Table)
		}
		for _, signal := range []string{"traces", "logs", "metrics"} {
			if got := yamlPath(doc, "service", "pipelines", signal, "exporters"); !reflect.DeepEqual(got, []any{"bigquery"}) {
				t.Errorf("%s pipeline exporters = %v, want [bigquery]", signal, got)
			}
		}
	})

	// Values a YAML parser reads as something other than a string, a list or a
	// mapping when written bare (a flow sequence, an alias, a tag, null, a
	// boolean, a number) must still reach the exporter as the string the
	// operator wrote.
	for _, mask := range []string{
		"[REDACTED]", "***", "*", "{redacted}", "&anchor", "!tag", "#hash", "@at", "%pct",
		"- dash", "key: value", "null", "true", "123", `"quoted"`, `back\slash`,
	} {
		t.Run("enterprise/mask="+mask, func(t *testing.T) {
			conf := confmap.NewFromStringMap(helmCollectorConfig(t, helmEnterpriseValues(mask)))
			cfg := decodeExporterConfig(t, "enterprise values", exporterBlock(t, conf, "bigquery"))
			if cfg.Redaction.MaskReplacement != mask {
				t.Errorf("redaction.mask_replacement = %q, want %q", cfg.Redaction.MaskReplacement, mask)
			}
			if cfg.Offload.Bucket != "123456" {
				t.Errorf("offload.bucket = %q, want the all-digit bucket name as a string", cfg.Offload.Bucket)
			}
			if got := cfg.Labels["cost-center"]; got != "1234" {
				t.Errorf("labels[cost-center] = %q, want %q: a numeric label value must stay a string", got, "1234")
			}
			if cfg.Traces.MaxStaleness != "15m" || cfg.Quarantine.MaxStaleness != "30m" {
				t.Errorf("max_staleness = %q (traces) and %q (quarantine), want 15m and 30m",
					cfg.Traces.MaxStaleness, cfg.Quarantine.MaxStaleness)
			}
			if cfg.KMSKeyName == "" || cfg.Logs.KMSKeyName == "" || cfg.Quarantine.KMSKeyName == "" {
				t.Errorf("kms_key_name did not reach the exporter: %q, %q, %q",
					cfg.KMSKeyName, cfg.Logs.KMSKeyName, cfg.Quarantine.KMSKeyName)
			}
			if !cfg.AutoCreate.CompanionViews || !cfg.AutoCreate.SearchIndexes {
				t.Errorf("auto_create = %+v, want companion_views and search_indexes on", cfg.AutoCreate)
			}
			if !reflect.DeepEqual(cfg.Redaction.BlockedKeys, []string{"x-custom-secret", "*.ssn"}) ||
				len(cfg.Redaction.BlockedValuePatterns) != 2 {
				t.Errorf("redaction lists = %v and %v", cfg.Redaction.BlockedKeys, cfg.Redaction.BlockedValuePatterns)
			}
		})
	}
}

// An offload.cacheSize of 0 disables the cross-batch cache, whose exporter
// default is 4096, so it has to reach the exporter rather than be dropped as
// false; an explicit quarantine.partitionExpirationDays of 0 renders as
// written too. A values file delivers numbers as float64 and --set as int64.
// An unset value is still left to the exporter default.
func TestHelmConfigMapRendersZeroCacheSizeAndPartitionExpiration(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		cacheSize, expiration any
		// Rendered values, nil when the key is absent.
		renderedCacheSize, renderedExpiration any
		// Decoded onto the factory defaults.
		wantCacheSize, wantExpiration int
	}{
		{"zero from --set", int64(0), int64(0), 0, 0, 0, 0},
		{"zero from a values file", float64(0), float64(0), 0, 0, 0, 0},
		{"positive", int64(2048), int64(14), 2048, 14, 2048, 14},
		{"unset", nil, nil, nil, nil, defaultOffloadCacheSize, defaultQuarantinePartitionExpiryDays},
		{"empty", "", "", nil, nil, defaultOffloadCacheSize, defaultQuarantinePartitionExpiryDays},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := helmCollectorConfig(t, map[string]any{
				"offload":    map[string]any{"enabled": true, "bucket": "test-offload-bucket", "cacheSize": tc.cacheSize},
				"quarantine": map[string]any{"enabled": true, "partitionExpirationDays": tc.expiration},
			})
			if got := yamlPath(doc, "exporters", "bigquery", "offload", "cache_size"); got != tc.renderedCacheSize {
				t.Errorf("rendered offload.cache_size = %#v, want %#v", got, tc.renderedCacheSize)
			}
			if got := yamlPath(doc, "exporters", "bigquery", "quarantine", "partition_expiration_days"); got != tc.renderedExpiration {
				t.Errorf("rendered quarantine.partition_expiration_days = %#v, want %#v", got, tc.renderedExpiration)
			}
			cfg := decodeExporterConfig(t, tc.name, exporterBlock(t, confmap.NewFromStringMap(doc), "bigquery"))
			if cfg.Offload.CacheSize != tc.wantCacheSize || cfg.Quarantine.PartitionExpirationDays != tc.wantExpiration {
				t.Errorf("offload.cache_size = %d and quarantine.partition_expiration_days = %d, want %d and %d",
					cfg.Offload.CacheSize, cfg.Quarantine.PartitionExpirationDays, tc.wantCacheSize, tc.wantExpiration)
			}
		})
	}
}

// helmUnexposedExporterKeys lists the exporter options the chart does not
// surface as values, with the reason. Every other option must reach the
// exporter's section of the rendered ConfigMap, and an option listed here must
// not have been exposed since: a new Config field forces a decision.
var helmUnexposedExporterKeys = map[string]string{
	"timeout": "the exporter default applies to the whole send",

	"credentials.file":                        "the pod authenticates with Workload Identity Federation, not a key file",
	"credentials.json":                        "the pod authenticates with Workload Identity Federation, not inline keys",
	"credentials.impersonate_service_account": "the pod authenticates as its own Workload Identity service account",
	"credentials.impersonate_delegates":       "the pod authenticates as its own Workload Identity service account",
	"credentials.quota_project":               "the default quota project of the service account applies",
	"credentials.universe_domain":             "the chart targets the public googleapis.com universe",

	"endpoint.url":                    "custom and emulator endpoints are test configuration, not a deployment option",
	"endpoint.admin_url":              "custom and emulator endpoints are test configuration, not a deployment option",
	"endpoint.insecure":               "custom and emulator endpoints are test configuration, not a deployment option",
	"endpoint.without_authentication": "custom and emulator endpoints are test configuration, not a deployment option",

	"write.max_request_bytes":               "Storage Write API tuning keeps the exporter defaults",
	"write.max_row_bytes":                   "Storage Write API tuning keeps the exporter defaults",
	"write.max_inflight_requests":           "Storage Write API tuning keeps the exporter defaults",
	"write.max_inflight_bytes":              "Storage Write API tuning keeps the exporter defaults",
	"write.enable_multiplexing":             "Storage Write API tuning keeps the exporter defaults",
	"write.connection_pool_size":            "Storage Write API tuning keeps the exporter defaults",
	"write.compression":                     "Storage Write API tuning keeps the exporter defaults",
	"write.keepalive.time":                  "Storage Write API tuning keeps the exporter defaults",
	"write.keepalive.timeout":               "Storage Write API tuning keeps the exporter defaults",
	"write.keepalive.permit_without_stream": "Storage Write API tuning keeps the exporter defaults",

	"limits.max_attribute_count":     "structural limits keep the exporter defaults",
	"limits.max_event_count":         "structural limits keep the exporter defaults",
	"limits.max_link_count":          "structural limits keep the exporter defaults",
	"limits.max_nesting_depth":       "structural limits keep the exporter defaults",
	"limits.max_collection_size":     "structural limits keep the exporter defaults",
	"limits.max_attribute_key_bytes": "structural limits keep the exporter defaults",
	"limits.max_value_bytes":         "structural limits keep the exporter defaults",

	"logs.log_name_attribute":  "the exporter falls back to well-known attributes, then the service name",
	"logs.insert_id_attribute": "the exporter falls back to well-known attributes, then the record fingerprint",
}

func TestHelmChartExposesExporterOptions(t *testing.T) {
	conf := confmap.NewFromStringMap(helmCollectorConfig(t, helmEnterpriseValues("[REDACTED]")))
	section := exporterBlock(t, conf, "bigquery")
	exposed := map[string]bool{}
	for _, key := range presentExporterKeys(section, exporterKeySet(), "") {
		exposed[key] = true
	}
	for _, key := range exporterConfigKeys() {
		reason, unexposed := helmUnexposedExporterKeys[key]
		switch {
		case !exposed[key] && !unexposed:
			t.Errorf("the chart renders no %q: expose it as a value or list it in helmUnexposedExporterKeys with the reason", key)
		case exposed[key] && unexposed:
			t.Errorf("the chart now renders %q but helmUnexposedExporterKeys still lists it (%s)", key, reason)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(helmUnexposedExporterKeys)) {
		if !slices.Contains(exporterConfigKeys(), key) {
			t.Errorf("helmUnexposedExporterKeys lists %q, which is not an exporter option", key)
		}
	}
}

// readmeByteCount renders a byte count the way the README's configuration
// table does: the exact count, then the size in the largest binary unit that
// divides it.
func readmeByteCount(n int) string {
	for _, unit := range []struct {
		name string
		size int
	}{{"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if n >= unit.size && n%unit.size == 0 {
			return fmt.Sprintf("`%d` (%d %s)", n, n/unit.size, unit.name)
		}
	}
	return fmt.Sprintf("`%d`", n)
}

// The README's configuration table gives the default of every offload
// option. Each Default cell is rebuilt here from createDefaultConfig, so a
// default cannot change without the table following it, and an offload
// option cannot be added without its row being checked.
func TestReadmeOffloadDefaultsMatchFactory(t *testing.T) {
	offload := createDefaultConfig().(*Config).Offload
	literal := func(value string) string {
		if value == "" {
			return "—"
		}
		return "`" + value + "`"
	}
	cells := map[string]string{
		"offload.enabled":          literal(strconv.FormatBool(offload.Enabled)),
		"offload.bucket":           literal(offload.Bucket),
		"offload.prefix":           literal(offload.Prefix),
		"offload.authorizer":       literal(offload.Authorizer),
		"offload.min_bytes":        readmeByteCount(offload.MinBytes),
		"offload.max_object_bytes": readmeByteCount(offload.MaxObjectBytes),
		"offload.max_concurrency":  literal(strconv.Itoa(offload.MaxConcurrency)),
		"offload.timeout":          literal(offload.Timeout.String()),
		"offload.cache_size":       literal(strconv.Itoa(offload.CacheSize)),
	}
	readme := readRepoText(t, "README.md")
	for _, key := range exporterConfigKeys() {
		if !strings.HasPrefix(key, "offload.") {
			continue
		}
		want, ok := cells[key]
		if !ok {
			t.Errorf("the README default of %s is not checked; add it here", key)
			continue
		}
		if got := firstCapture("(?m)^\\| `"+regexp.QuoteMeta(key)+"` +\\| (.*?) +\\|", readme); got != want {
			t.Errorf("the README gives the %s default as %q, but the exporter's default is %q", key, got, want)
		}
	}
}

// The Terraform module provisions the tables the collector would otherwise
// create, so each retention variable defaults to what the exporter applies
// when it creates that table, and a number of days becomes the partition
// expiration the exporter sets for that many days. One partition filter
// variable covers all four tables, so it defaults to what the exporter
// applies to each of them.
func TestTerraformRetentionMatchesExporter(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	vars := readRepoText(t, "deploy/terraform/variables.tf")
	defaultOf := func(variable string) string {
		t.Helper()
		block := terraformBlock(vars, `variable "`+variable+`"`)
		if block == "" {
			t.Fatalf("variables.tf declares no %s", variable)
		}
		return firstCapture(`(?m)^  default\s+=\s+(.+)$`, block)
	}
	for table, days := range map[string]int{
		"otel_spans":      cfg.Traces.PartitionExpirationDays,
		"otel_logs":       cfg.Logs.PartitionExpirationDays,
		"otel_metrics":    cfg.Metrics.PartitionExpirationDays,
		"otel_quarantine": cfg.Quarantine.PartitionExpirationDays,
	} {
		variable := terraformExpirationVariables[table]
		if got := defaultOf(variable); got != strconv.Itoa(days) {
			t.Errorf("variables.tf %s defaults to %s, but the exporter creates %s with %d", variable, got, table, days)
		}
	}
	for table, required := range map[string]bool{
		"otel_spans":      cfg.Traces.RequirePartitionFilter,
		"otel_logs":       cfg.Logs.RequirePartitionFilter,
		"otel_metrics":    cfg.Metrics.RequirePartitionFilter,
		"otel_quarantine": cfg.Quarantine.RequirePartitionFilter,
	} {
		if got := defaultOf("require_partition_filter"); got != strconv.FormatBool(required) {
			t.Errorf("variables.tf require_partition_filter defaults to %s, but the exporter creates %s with %t", got, table, required)
		}
	}

	// exporterExpiration is the partition expiration the exporter gives a
	// table it creates with partition_expiration_days set to days.
	exporterExpiration := func(table string, days int) time.Duration {
		t.Helper()
		opts := schema.DestinationOptions{PartitionExpirationDays: days}
		if table == "otel_quarantine" {
			md, err := schema.QuarantineDestinationMetadataWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			return md.TimePartitioning.Expiration
		}
		in := schema.SpansTableSchema()
		switch table {
		case "otel_logs":
			in = schema.LogsTableSchema()
		case "otel_metrics":
			in = schema.MetricsTableSchema()
		}
		md, _, err := schema.DestinationMetadataWithOptions(in, "my-project", "otel", table, opts)
		if err != nil {
			t.Fatal(err)
		}
		return md.TimePartitioning.Expiration
	}
	mainTF := readRepoText(t, "deploy/terraform/main.tf")
	for table, variable := range terraformExpirationVariables {
		perDay, err := strconv.ParseInt(firstCapture(`var\.`+variable+` \* ([0-9]+) : null`, mainTF), 10, 64)
		if err != nil {
			t.Fatalf("main.tf does not convert %s to milliseconds: %v", variable, err)
		}
		for _, days := range []int{1, 30, 400} {
			if got, want := time.Duration(int64(days)*perDay)*time.Millisecond, exporterExpiration(table, days); got != want {
				t.Errorf("%s = %d expires %s partitions after %v, but the exporter expires them after %v",
					variable, days, table, got, want)
			}
		}
	}
}
