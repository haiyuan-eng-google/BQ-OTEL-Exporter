// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"gopkg.in/yaml.v3"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/quarantinereplay"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/transform"
)

func TestGoReleaserConfigUsesValidSchemaAndSignsManifests(t *testing.T) {
	raw, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("reading .goreleaser.yaml: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshaling .goreleaser.yaml: %v", err)
	}

	if _, bad := doc["docker_image_files"]; bad {
		t.Error(".goreleaser.yaml must not contain unknown top-level key docker_image_files")
	}

	manifests, ok := doc["docker_manifests"].([]any)
	if !ok || len(manifests) < 2 {
		t.Fatalf("docker_manifests = %#v, want at least 2 manifest entries", doc["docker_manifests"])
	}
	for i, item := range manifests {
		m, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("docker_manifests[%d] = %T, want map", i, item)
		}
		if _, bad := m["docker_manifests_templates"]; bad {
			t.Errorf("docker_manifests[%d] must not use invalid key docker_manifests_templates", i)
		}
		nameTpl, _ := m["name_template"].(string)
		if nameTpl == "" {
			t.Errorf("docker_manifests[%d] missing name_template", i)
		}
		imgTpls, ok := m["image_templates"].([]any)
		if !ok || len(imgTpls) != 2 {
			t.Errorf("docker_manifests[%d].image_templates = %#v, want amd64 and arm64 arch images", i, m["image_templates"])
		}
	}

	dockerSigns, ok := doc["docker_signs"].([]any)
	if !ok || len(dockerSigns) == 0 {
		t.Fatalf("docker_signs = %#v, want container manifest signing configured", doc["docker_signs"])
	}
	ds0, _ := dockerSigns[0].(map[string]any)
	if ds0["artifacts"] != "manifests" || ds0["cmd"] != "cosign" {
		t.Errorf("docker_signs[0] = %#v, want cosign signing manifests", ds0)
	}

	dockers, ok := doc["dockers"].([]any)
	if !ok || len(dockers) != 2 {
		t.Fatalf("dockers = %#v, want 2 linux arch entries", doc["dockers"])
	}
	for i, item := range dockers {
		d, _ := item.(map[string]any)
		extra, _ := d["extra_files"].([]any)
		if len(extra) == 0 || extra[0] != "example/otel-collector-config.yml" {
			t.Errorf("dockers[%d].extra_files = %#v, want example/otel-collector-config.yml", i, extra)
		}
	}
}

func TestMetadataYAMLDeclaresGoleakIgnoreMatchingGeneratedPackageTest(t *testing.T) {
	var meta struct {
		Tests struct {
			SkipLifecycle bool `yaml:"skip_lifecycle"`
			Goleak        struct {
				Ignore struct {
					Top []string `yaml:"top"`
				} `yaml:"ignore"`
			} `yaml:"goleak"`
		} `yaml:"tests"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, "metadata.yaml")), &meta); err != nil {
		t.Fatalf("unmarshaling metadata.yaml: %v", err)
	}
	const wantOpenCensusTop = "go.opencensus.io/stats/view.(*worker).start"
	if !slices.Contains(meta.Tests.Goleak.Ignore.Top, wantOpenCensusTop) {
		t.Errorf("metadata.yaml tests.goleak.ignore.top = %v, want %q", meta.Tests.Goleak.Ignore.Top, wantOpenCensusTop)
	}

	genTest := readRepoText(t, "generated_package_test.go")
	if !strings.Contains(genTest, `goleak.IgnoreTopFunction("`+wantOpenCensusTop+`")`) {
		t.Errorf("generated_package_test.go missing IgnoreTopFunction(%q)", wantOpenCensusTop)
	}
	if !strings.Contains(genTest, "integrationGoleakOptions()...") {
		t.Error("generated_package_test.go must chain integrationGoleakOptions() for build-tag-isolated unit and live integration runs")
	}
}

func TestOpenTelemetryRegistryEntryAndReleaseDocs(t *testing.T) {
	registry := registryEntry(t)
	if err := registrySchema(t).Validate(registry); err != nil {
		t.Fatalf("registry entry fails destination schema: %v", err)
	}
	if pkg, ok := registry["package"].(map[string]any); ok && pkg["version"] != nil {
		t.Error("registry candidate must omit package.version until the release is published")
	}
	var entry struct {
		Title        string   `yaml:"title"`
		RegistryType string   `yaml:"registryType"`
		Language     string   `yaml:"language"`
		Tags         []string `yaml:"tags"`
		License      string   `yaml:"license"`
		Description  string   `yaml:"description"`
		Authors      []struct {
			Name string `yaml:"name"`
			URL  string `yaml:"url"`
		} `yaml:"authors"`
		URLs struct {
			Repo string `yaml:"repo"`
		} `yaml:"urls"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, "docs/otel-registry-entry.yaml")), &entry); err != nil {
		t.Fatalf("unmarshaling docs/otel-registry-entry.yaml: %v", err)
	}
	if entry.Title == "" {
		t.Error("docs/otel-registry-entry.yaml missing title")
	}
	if entry.RegistryType != "exporter" {
		t.Errorf("registryType = %q, want %q", entry.RegistryType, "exporter")
	}
	if entry.Language != "collector" {
		t.Errorf("language = %q, want %q", entry.Language, "collector")
	}
	if entry.License != "Apache-2.0" {
		t.Errorf("license = %q, want %q", entry.License, "Apache-2.0")
	}
	if strings.TrimSpace(entry.Description) == "" {
		t.Error("docs/otel-registry-entry.yaml missing description")
	}
	if len(entry.Authors) == 0 || entry.Authors[0].Name == "" {
		t.Errorf("authors = %#v, want at least one named author", entry.Authors)
	}
	if entry.URLs.Repo != "https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter" {
		t.Errorf("urls.repo = %q, want https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter", entry.URLs.Repo)
	}
	for _, wantTag := range []string{"go", "exporter", "collector", "bigquery", "google-cloud", "genai", "mcp"} {
		if !slices.Contains(entry.Tags, wantTag) {
			t.Errorf("tags %v missing %q", entry.Tags, wantTag)
		}
	}

	provenance := readRepoText(t, "docs/provenance.md")
	for _, want := range []string{
		"Apache-2.0",
		"Copyright 2026 Google LLC",
		"CONTRIBUTING.md",
		"v0alpha1",
		"v0alpha2",
		"v0.1.0",
	} {
		if !strings.Contains(provenance, want) {
			t.Errorf("docs/provenance.md missing %q", want)
		}
	}

	changelog := readRepoText(t, "CHANGELOG.md")
	for _, want := range []string{
		"v0.1.0",
		"v0alpha2",
		"v0alpha1",
		"CreateTracesExporter",
		"CreateLogsExporter",
		"CreateMetricsExporter",
		"[1, 16]",
		"otel_sensitivity",
		"sensitive_payload",
		"sensitive_payload_mask",
	} {
		if !strings.Contains(changelog, want) {
			t.Errorf("CHANGELOG.md missing %q", want)
		}
	}

	knownCompanionViews := map[string]bool{
		schema.SpansAsAllSpansViewName:      true,
		schema.GenAISpansViewName:           true,
		schema.GenAILogsViewName:            true,
		schema.GenAIMetricsViewName:         true,
		schema.GenAIMetricExemplarsViewName: true,
		schema.GenAIModelCostsViewName:      true,
		schema.GenAISessionsViewName:        true,
		schema.GenAIToolUsageViewName:       true,
		schema.GenAIAgentTrajectoryViewName: true,
		schema.MCPSpansViewName:             true,
		schema.MCPToolUsageViewName:         true,
	}
	viewIdentRe := regexp.MustCompile("`((?:v_gen_ai_|v_mcp_)[a-z0-9_]+)`")
	for _, docPath := range []string{"CHANGELOG.md", "docs/provenance.md"} {
		docText := readRepoText(t, docPath)
		for _, bad := range []string{
			"[1, 32]",
			"v_gen_ai_conversation_turns",
			"high_sensitivity_payload",
			"medium_sensitivity_attributes",
			"low_sensitivity_identity",
			"#34809",
		} {
			if strings.Contains(docText, bad) {
				t.Errorf("%s must not contain unverified or drifted identifier %q", docPath, bad)
			}
		}
		for _, m := range viewIdentRe.FindAllStringSubmatch(docText, -1) {
			if !knownCompanionViews[m[1]] {
				t.Errorf("%s references unknown companion view %q", docPath, m[1])
			}
		}
	}
}

func TestCloudRunServiceAndDockerfilesSatisfyRuntimeContract(t *testing.T) {
	raw, err := os.ReadFile("deploy/cloudrun/service.yaml")
	if err != nil {
		t.Fatalf("reading deploy/cloudrun/service.yaml: %v", err)
	}
	var svc map[string]any
	if err := yaml.Unmarshal(raw, &svc); err != nil {
		t.Fatalf("unmarshaling deploy/cloudrun/service.yaml: %v", err)
	}
	if svc["apiVersion"] != "serving.knative.dev/v1" {
		t.Errorf("apiVersion = %v, want serving.knative.dev/v1", svc["apiVersion"])
	}
	spec, _ := svc["spec"].(map[string]any)
	tpl, _ := spec["template"].(map[string]any)
	tplMeta, _ := tpl["metadata"].(map[string]any)
	ann, _ := tplMeta["annotations"].(map[string]any)
	if ann["autoscaling.knative.dev/minScale"] != "1" || ann["autoscaling.knative.dev/maxScale"] != "10" {
		t.Errorf("template annotations = %#v, want Knative minScale/maxScale annotations", ann)
	}
	tplSpec, _ := tpl["spec"].(map[string]any)
	if _, bad := tplSpec["serviceAccount"]; bad {
		t.Error("Knative service spec must use serviceAccountName, not serviceAccount")
	}
	if _, bad := tplSpec["maxScale"]; bad {
		t.Error("Knative service spec must not place maxScale under template.spec")
	}
	if tplSpec["serviceAccountName"] == nil {
		t.Error("Knative service spec missing serviceAccountName")
	}
	containers, _ := tplSpec["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	c0, _ := containers[0].(map[string]any)
	ports, _ := c0["ports"].([]any)
	if len(ports) != 1 {
		t.Fatalf("Cloud Run container ports = %d, want exactly 1 ingress port", len(ports))
	}
	p0, _ := ports[0].(map[string]any)
	if p0["name"] != "http1" || p0["containerPort"] != 4318 {
		t.Errorf("Cloud Run port[0] = %#v, want http1 on 4318", p0)
	}

	dfRaw, err := os.ReadFile("Dockerfile.goreleaser")
	if err != nil {
		t.Fatalf("reading Dockerfile.goreleaser: %v", err)
	}
	dfText := string(dfRaw)
	if !strings.Contains(dfText, "COPY example/otel-collector-config.yml /etc/otel/config.yml") {
		t.Error("Dockerfile.goreleaser must copy example/otel-collector-config.yml to /etc/otel/config.yml")
	}
	if !strings.Contains(dfText, "COPY healthcheck /healthcheck") {
		t.Error("Dockerfile.goreleaser must copy /healthcheck into the runtime image")
	}
}

func TestHelmDeploymentAndDockerComposeHealthcheck(t *testing.T) {
	helmRaw, err := os.ReadFile("deploy/helm/bigquery-otel-collector/templates/deployment.yaml")
	if err != nil {
		t.Fatalf("reading Helm deployment.yaml: %v", err)
	}
	helmText := string(helmRaw)
	if strings.Contains(helmText, "emptyDir") {
		t.Error("Helm deployment.yaml must not back persistentQueue with emptyDir")
	}
	if !strings.Contains(helmText, "kind: StatefulSet") || !strings.Contains(helmText, "volumeClaimTemplates:") {
		t.Error("Helm deployment.yaml must use StatefulSet with volumeClaimTemplates when persistentQueue.enabled is true")
	}

	dcRaw, err := os.ReadFile("example/docker-compose.yml")
	if err != nil {
		t.Fatalf("reading example/docker-compose.yml: %v", err)
	}
	dcText := string(dcRaw)
	if strings.Contains(dcText, "CMD-SHELL") {
		t.Error("example/docker-compose.yml must not use CMD-SHELL on a distroless runtime image")
	}
	if !strings.Contains(dcText, `"/healthcheck"`) {
		t.Error("example/docker-compose.yml must invoke /healthcheck via exec-form CMD")
	}

	cmRaw, err := os.ReadFile("deploy/helm/bigquery-otel-collector/templates/configmap.yaml")
	if err != nil {
		t.Fatalf("reading Helm configmap.yaml: %v", err)
	}
	cmText := string(cmRaw)
	if strings.Contains(cmText, "compaction/directory") || strings.Contains(cmText, "/compaction") {
		t.Error("Helm configmap.yaml must not specify non-existent /compaction subdirectory")
	}

	tfRaw, err := os.ReadFile("deploy/terraform/main.tf")
	if err != nil {
		t.Fatalf("reading deploy/terraform/main.tf: %v", err)
	}
	tfText := string(tfRaw)
	if !strings.Contains(tfText, "bigquery.tables.createIndex") {
		t.Error("Terraform main.tf must include bigquery.tables.createIndex for search indexes")
	}
	if !strings.Contains(tfText, "otelCollectorWriter") {
		t.Error("Terraform main.tf must define otelCollectorWriter write-only role")
	}
}

// Release binaries embed the standard library they were built with, so the
// release workflow builds with the same patched Go release example/Dockerfile
// pins, not the older go.mod minimum. GoReleaser's -X flags for main.version
// and friends were no-ops on OCB's generated main.go; the version comes from
// the dist.version the workflow stamps.
func TestReleaseBuildUsesPinnedPatchedGoAndStampedVersion(t *testing.T) {
	wf, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	df, err := os.ReadFile("example/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wf), "go-version-file") {
		t.Error("release.yml must pin go-version instead of resolving the go.mod minimum")
	}
	got := regexp.MustCompile(`go-version: "?([0-9.]+)"?`).FindStringSubmatch(string(wf))
	want := regexp.MustCompile(`golang:([0-9.]+)-`).FindStringSubmatch(string(df))
	if got == nil || want == nil || got[1] != want[1] {
		t.Errorf("release.yml go-version = %v, example/Dockerfile builder = %v, want the same patched release", got, want)
	}

	// CI tests the toolchain that ships, so every setup-go step in ci.yml, and
	// each of the four jobs that run Go code in particular, pins that release.
	ciGo := workflowSetupGoVersions(t, ".github/workflows/ci.yml")
	for _, job := range []string{"build", "lint", "vulncheck", "release-stamping"} {
		if len(ciGo[job]) == 0 {
			t.Errorf("ci.yml job %q has no actions/setup-go step", job)
		}
	}
	for job, versions := range ciGo {
		for _, version := range versions {
			if want == nil || version != want[1] {
				t.Errorf("ci.yml job %q sets up Go %q, example/Dockerfile builder = %v, want the same patched release", job, version, want)
			}
		}
	}
	// The toolchain line in go.mod names that release too, so local builds and
	// the workflows that set up Go from go-version-file: go.mod use it. The go
	// line stays the minimum for modules that require this one, which ignore
	// the toolchain line.
	if toolchain := firstCapture(`(?m)^toolchain go(\S+)$`, readRepoText(t, "go.mod")); want == nil || toolchain != want[1] {
		t.Errorf("go.mod toolchain = %q, example/Dockerfile builder = %v, want the same patched release", toolchain, want)
	}
	if strings.Contains(string(wf), "/usr/local/bin") {
		t.Error("release.yml must not install tools into the root-owned /usr/local/bin")
	}

	gr, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(gr), "-X main.") {
		t.Error(".goreleaser.yaml sets -X main.* flags, which OCB's generated main.go ignores")
	}
	if !strings.Contains(string(wf), "s/version: 0.1.0-dev/version: ${VERSION}/g") {
		t.Error("release.yml must stamp the release version into the OCB manifest's dist.version")
	}
}

// Both runtime images ship the static /healthcheck probe, and declare it as an
// exec-form HEALTHCHECK, since distroless has no shell for the shell form.
func TestRuntimeImagesDeclareExecFormHealthcheck(t *testing.T) {
	for _, path := range []string{"Dockerfile.goreleaser", "example/Dockerfile"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if !regexp.MustCompile(`(?m)^HEALTHCHECK .*CMD \["/healthcheck"\]$`).MatchString(text) {
			t.Errorf("%s has no exec-form HEALTHCHECK running /healthcheck", path)
		}
	}
}

// Cloud Run cannot pull from GHCR, and the service account Terraform creates
// may only read metadata and append rows, so the service must name a
// supported registry and switch off the image's default auto_create.
func TestCloudRunServiceUsesSupportedRegistryAndPreProvisionedTables(t *testing.T) {
	raw, err := os.ReadFile("deploy/cloudrun/service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var svc struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Image string   `yaml:"image"`
						Args  []string `yaml:"args"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &svc); err != nil {
		t.Fatal(err)
	}
	containers := svc.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(containers))
	}
	c := containers[0]
	if !regexp.MustCompile(`^([a-z0-9-]+-docker\.pkg\.dev|(?:[a-z]+\.)?gcr\.io|docker\.io)/`).MatchString(c.Image) ||
		strings.HasPrefix(c.Image, "ghcr.io/") {
		t.Errorf("image %q is not on Artifact Registry, Container Registry, or Docker Hub", c.Image)
	}
	for _, want := range []string{
		"--set=exporters.bigquery.auto_create.dataset=false",
		"--set=exporters.bigquery.auto_create.tables=false",
	} {
		found := false
		for _, a := range c.Args {
			found = found || a == want
		}
		if !found {
			t.Errorf("args %v missing %q", c.Args, want)
		}
	}
}

// filestorage compacts in compaction.directory, which defaults to its own
// /var/lib/otelcol/file_storage rather than the configured directory, and
// Validate refuses a compaction directory that does not exist. The image has
// no such directory and a read-only root, so every on_start compaction must
// point at the persistent queue mount.
func TestHelmPersistentQueueCompactsOnTheQueueVolume(t *testing.T) {
	raw, err := os.ReadFile("deploy/helm/bigquery-otel-collector/templates/configmap.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	starts := strings.Count(text, "on_start: true")
	dirs := strings.Count(text, "          directory: {{ .Values.persistentQueue.mountPath }}")
	if starts == 0 || dirs != starts {
		t.Errorf("configmap has %d on_start compactions and %d compaction directories on the queue mount, want them equal and non-zero", starts, dirs)
	}
}

// Terraform provisions every table and view, so by default the collector's
// service account gets only the data path on the dataset. The auto_create
// permissions are granted only when grant_auto_create is set, and the only
// project-scope grants are the opt-in jobs roles (collector search indexes or
// Continuous Queries). CMEK uses the BigQuery default service account, not
// data.google_project.number.
func TestTerraformGrantsWritePathOnDatasetOnly(t *testing.T) {
	raw, err := os.ReadFile("deploy/terraform/main.tf")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `resource "google_bigquery_dataset_iam_member" "otel_collector_writer"`) {
		t.Error("main.tf must bind the writer role on the dataset")
	}
	for _, block := range regexp.MustCompile(`(?s)resource "google_project_iam_member" "[^"]+" \{.*?\n\}`).FindAllString(text, -1) {
		collectorJobs := strings.Contains(block, "count   = var.grant_auto_create ? 1 : 0") &&
			strings.Contains(block, "otel_collector_jobs")
		continuousQueryJobs := strings.Contains(block, "count   = var.enable_continuous_queries ? 1 : 0") &&
			strings.Contains(block, "continuous_queries_jobs") &&
			strings.Contains(block, `role    = "roles/bigquery.jobUser"`) &&
			strings.Contains(block, "google_service_account.continuous_queries[0].email")
		if !collectorJobs && !continuousQueryJobs {
			t.Errorf("project-scope binding must be an opt-in jobs role only:\n%s", block)
		}
	}
	roles := regexp.MustCompile(`(?s)resource "google_project_iam_custom_role" "([^"]+)" \{(.*?)\n\}`).FindAllStringSubmatch(text, -1)
	if len(roles) == 0 {
		t.Fatal("main.tf defines no custom roles")
	}
	for _, r := range roles {
		name, body := r[1], r[2]
		optIn := strings.Contains(body, "count       = var.grant_auto_create ? 1 : 0")
		for _, perm := range []string{"bigquery.tables.create", "bigquery.tables.update\"", "bigquery.tables.createIndex", "bigquery.jobs.create", "bigquery.datasets.create"} {
			if strings.Contains(body, perm) && !optIn {
				t.Errorf("role %s grants %s without grant_auto_create", name, strings.Trim(perm, `"`))
			}
		}
	}
	if !strings.Contains(text, "roles/cloudkms.cryptoKeyEncrypterDecrypter") {
		t.Error("main.tf must use roles/cloudkms.cryptoKeyEncrypterDecrypter for CMEK")
	}
	if !strings.Contains(text, "google_bigquery_default_service_account") {
		t.Error("main.tf must use google_bigquery_default_service_account for CMEK, not data.google_project.number")
	}
	if strings.Contains(text, "data.google_project.number") {
		t.Error("main.tf must not reference data.google_project.number; use google_bigquery_default_service_account instead")
	}
}

// TestTerraformViewQueriesMatchSchemaContract asserts that the dedup view SQL
// in main.tf matches the schema contract: otel_metrics_dedup must partition by
// metric_type, and gen_ai views must use LIKE 'gen_ai.%' (single %, not %%).
func TestTerraformViewQueriesMatchSchemaContract(t *testing.T) {
	raw, err := os.ReadFile("deploy/terraform/main.tf")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)

	// otel_metrics_dedup must partition by metric_type.
	metricsDedupSection := regexp.MustCompile(`(?s)resource "google_bigquery_table" "otel_metrics_dedup" \{.*?PARTITION BY(.*?)ORDER BY`).FindStringSubmatch(text)
	if metricsDedupSection == nil {
		t.Fatal("could not find otel_metrics_dedup PARTITION BY clause")
	}
	if !strings.Contains(metricsDedupSection[1], "metric_type") {
		t.Error("otel_metrics_dedup PARTITION BY must include metric_type")
	}

	// gen_ai views must use LIKE 'gen_ai.%' (single %), not 'gen_ai.%%'.
	for _, view := range []string{"v_gen_ai_logs", "v_gen_ai_metrics"} {
		section := regexp.MustCompile(`(?s)resource "google_bigquery_table" "` + view + `" \{.*?\n\}`).FindString(text)
		if section == "" {
			t.Fatalf("could not find %s view in main.tf", view)
		}
		if strings.Contains(section, "gen_ai.%%") {
			t.Errorf("%s must use LIKE 'gen_ai.%%' (single percent), not 'gen_ai.%%%%'", view)
		}
		if !strings.Contains(section, "gen_ai.%") {
			t.Errorf("%s must contain LIKE 'gen_ai.%%'", view)
		}
	}

	// v_gen_ai_spans must not have backslash-escaped error.type.
	spansSection := regexp.MustCompile(`(?s)resource "google_bigquery_table" "v_gen_ai_spans" \{.*?\n\}`).FindString(text)
	if spansSection == "" {
		t.Fatal("could not find v_gen_ai_spans view in main.tf")
	}
	if strings.Contains(spansSection, "\\") {
		t.Error("v_gen_ai_spans must not backslash-escape JSON paths in heredocs")
	}
	if !strings.Contains(spansSection, "$.\"error.type\"") {
		t.Error("v_gen_ai_spans must reference error.type with $.\"error.type\" (no backslash escaping in heredoc)")
	}
}

// terraformBlock returns the body of the top-level Terraform block that header
// opens (for example `resource "google_bigquery_table" "otel_spans"`), or ""
// when there is none. Nested blocks are indented, so the block ends at the
// first closing brace in the first column.
func terraformBlock(text, header string) string {
	return firstCapture(`(?s)(?:^|\n)`+regexp.QuoteMeta(header)+` \{\n(.*?)\n\}`, text)
}

// terraformTables are the partitioned tables main.tf provisions, which take
// max_staleness and the CMEK key; the views take neither.
var terraformTables = []string{"otel_spans", "otel_logs", "otel_metrics", "otel_quarantine"}

// max_staleness is an argument of the table itself: inside time_partitioning,
// terraform validate rejects it ("An argument named "max_staleness" is not
// expected here"). Every table takes the converted local, and no view sets it.
func TestTerraformSetsMaxStalenessOnTablesNotPartitioning(t *testing.T) {
	text := readRepoText(t, "deploy/terraform/main.tf")
	if strings.Contains(text, "max_staleness = var.max_staleness") {
		t.Error("main.tf passes var.max_staleness to a resource unconverted")
	}
	for _, table := range terraformTables {
		body := terraformBlock(text, `resource "google_bigquery_table" "`+table+`"`)
		if body == "" {
			t.Fatalf("main.tf has no %s table", table)
		}
		partitioning := firstCapture(`(?s)\n  time_partitioning \{\n(.*?)\n  \}`, body)
		if partitioning == "" {
			t.Errorf("%s has no time_partitioning block", table)
		} else if strings.Contains(partitioning, "max_staleness") {
			t.Errorf("%s sets max_staleness inside time_partitioning, where the provider rejects it:\n%s", table, partitioning)
		}
		if !strings.Contains(body, "\n  max_staleness = local.max_staleness\n") {
			t.Errorf("%s does not set max_staleness = local.max_staleness on the table", table)
		}
	}
	views := 0
	for _, m := range regexp.MustCompile(`(?s)(?:^|\n)resource "google_bigquery_table" "([^"]+)" \{\n(.*?)\n\}`).FindAllStringSubmatch(text, -1) {
		if !strings.Contains(m[2], "\n  view {") {
			continue
		}
		views++
		if strings.Contains(m[2], "max_staleness") {
			t.Errorf("view %s sets max_staleness", m[1])
		}
	}
	if views == 0 {
		t.Error("main.tf defines no views")
	}
}

// The provider passes max_staleness to the API unchanged, and the API takes
// only a SQL interval string, so main.tf converts the Go duration the exporter
// options take: timeadd adds it to an epoch, and formatdate reads back the day
// of the month, hour, minute and second. This replays that conversion with
// main.tf's epoch and checks it against the interval the exporter writes for
// the same duration (bq.IntervalValueFromDuration), up to the 168h bound.
func TestTerraformMaxStalenessConvertsToTheExporterInterval(t *testing.T) {
	locals := terraformBlock(readRepoText(t, "deploy/terraform/main.tf"), "locals")
	for _, want := range []string{
		`timeadd(local.max_staleness_epoch, var.max_staleness)`,
		`var.max_staleness == "" ? null :`,
		`startswith(var.max_staleness, "0-0 ") ? var.max_staleness :`,
		`format("0-0 0 %d:%s:%s",`,
		`(tonumber(formatdate("D", local.max_staleness_end)) - 1) * 24 + tonumber(formatdate("h", local.max_staleness_end))`,
		`formatdate("m", local.max_staleness_end)`,
		`formatdate("s", local.max_staleness_end)`,
	} {
		if !strings.Contains(locals, want) {
			t.Errorf("main.tf locals do not contain %s; update the conversion this test replays", want)
		}
	}
	epoch, err := time.Parse(time.RFC3339, firstCapture(`max_staleness_epoch\s*=\s*"([^"]+)"`, locals))
	if err != nil {
		t.Fatalf("main.tf max_staleness_epoch: %v", err)
	}
	// The day of the month counts the elapsed days only while the sum stays
	// in the epoch's month.
	if epoch.Day() != 1 || epoch.Hour() != 0 || epoch.Minute() != 0 || epoch.Second() != 0 ||
		epoch.Add(maxTableStaleness).Month() != epoch.Month() {
		t.Fatalf("max_staleness_epoch %v must be midnight on the first of a month that holds %v", epoch, maxTableStaleness)
	}
	for _, raw := range []string{"1s", "30s", "15m", "90m", "1h30m", "2h45m", "1.5h", "25h", "48h", "100h59m59s", "168h", "1500ms500ms"} {
		d, err := parseMaxStaleness("max_staleness", raw)
		if err != nil {
			t.Errorf("the exporter rejects %q: %v", raw, err)
			continue
		}
		end := epoch.Add(d)
		got := fmt.Sprintf("0-0 0 %d:%d:%d", (end.Day()-1)*24+end.Hour(), end.Minute(), end.Second())
		if want := bq.IntervalValueFromDuration(d).String(); got != want {
			t.Errorf("max_staleness %q converts to %q, but the exporter writes %q", raw, got, want)
		}
	}
}

// terraformValidationPatterns returns the patterns the named variable's
// validation rules in variables.tf pass to regex(), in order. Terraform's
// regex() is RE2, as Go's regexp is, so the patterns are evaluated directly.
func terraformValidationPatterns(t *testing.T, variable string) []*regexp.Regexp {
	t.Helper()
	block := terraformBlock(readRepoText(t, "deploy/terraform/variables.tf"), `variable "`+variable+`"`)
	var patterns []*regexp.Regexp
	for _, m := range regexp.MustCompile(`regex\(("(?:[^"\\]|\\.)*")`).FindAllStringSubmatch(block, -1) {
		// The patterns use only escapes HCL and Go strings share.
		pattern, err := strconv.Unquote(m[1])
		if err != nil {
			t.Fatalf("variables.tf %s pattern %s: %v", variable, m[1], err)
		}
		patterns = append(patterns, regexp.MustCompile(pattern))
	}
	return patterns
}

// The location rule must accept every BigQuery location, including regions
// whose first label is longer than ten letters.
func TestTerraformLocationValidationAcceptsBigQueryLocations(t *testing.T) {
	patterns := terraformValidationPatterns(t, "location")
	if len(patterns) != 1 {
		t.Fatalf("location has %d regex validation rules, want 1", len(patterns))
	}
	for _, location := range []string{
		"US", "EU", "us-central1", "europe-west4", "asia-northeast1", "me-central2", "australia-southeast1",
		"africa-south1", "northamerica-northeast1", "northamerica-northeast2", "northamerica-south1",
		"southamerica-east1", "southamerica-west1",
	} {
		if !patterns[0].MatchString(location) {
			t.Errorf("location validation rejects the BigQuery location %q", location)
		}
	}
	for _, location := range []string{"", "u", "us_central1", "1us", "us-", "-us", "us central1"} {
		if patterns[0].MatchString(location) {
			t.Errorf("location validation accepts %q", location)
		}
	}
}

// variables.tf takes max_staleness as the exporter options do, as a Go
// duration (compound ones included), or as a SQL interval string. A well
// formed duration then meets the range rule, which timeadd and timecmp
// evaluate in Terraform against the exporter's own bounds, in whole seconds,
// because the conversion in main.tf drops fractions of a second.
func TestTerraformMaxStalenessValidationMatchesExporter(t *testing.T) {
	patterns := terraformValidationPatterns(t, "max_staleness")
	if len(patterns) != 2 {
		t.Fatalf("max_staleness has %d regex validation rules, want the format rule and the range rule", len(patterns))
	}
	format, duration := patterns[0], patterns[1]
	for _, raw := range []string{"15m", "1h30m", "2h45m", "30s", "1m30s", "1.5h", "168h", "1500ms500ms", "60000000µs"} {
		if _, err := parseMaxStaleness("max_staleness", raw); err != nil {
			t.Fatalf("the exporter rejects %q: %v", raw, err)
		}
		if !format.MatchString(raw) || !duration.MatchString(raw) {
			t.Errorf("max_staleness validation does not take %q as a duration, though the exporter accepts it", raw)
		}
	}
	for _, raw := range []string{"0-0 0 0:15:0", "0-0 1 2:3:4"} {
		if !format.MatchString(raw) || duration.MatchString(raw) {
			t.Errorf("max_staleness validation does not take %q as a SQL interval", raw)
		}
	}
	for _, raw := range []string{"15", "1d", "abc", "15 m", "-1h", "0-0 0 0:15", "1-0 0 0:0:0"} {
		if format.MatchString(raw) {
			t.Errorf("max_staleness validation accepts %q", raw)
		}
	}
	// Durations the exporter rejects are well formed, so the range rule sees them.
	for _, raw := range []string{"0s", "500ms", "168h1s", "800h"} {
		if _, err := parseMaxStaleness("max_staleness", raw); err == nil {
			t.Errorf("the exporter accepts %q", raw)
		}
		if !duration.MatchString(raw) {
			t.Errorf("the max_staleness range rule does not check %q", raw)
		}
	}

	const epoch = "2000-01-01T00:00:00Z"
	start, err := time.Parse(time.RFC3339, epoch)
	if err != nil {
		t.Fatal(err)
	}
	block := terraformBlock(readRepoText(t, "deploy/terraform/variables.tf"), `variable "max_staleness"`)
	for _, rule := range []string{
		fmt.Sprintf(`timecmp(timeadd(%q, var.max_staleness), %q) >= 0`, epoch, start.Add(minTableStaleness).Format(time.RFC3339)),
		fmt.Sprintf(`timecmp(timeadd(%q, var.max_staleness), %q) <= 0`, epoch, start.Add(maxTableStaleness).Format(time.RFC3339)),
		fmt.Sprintf(`timeadd(%q, var.max_staleness) == timeadd(%q, "${var.max_staleness}999999999ns")`, epoch, epoch),
	} {
		if !strings.Contains(block, rule) {
			t.Errorf("variables.tf max_staleness has no range rule %s", rule)
		}
	}
}

// A first apply with a new key has to grant the BigQuery service agent on the
// key before BigQuery creates anything encrypted with it, and no argument of
// the dataset or the tables refers to the grant, so each of them depends on
// it. Without CMEK the grant has no instances and orders nothing.
func TestTerraformEncryptedResourcesDependOnKeyGrant(t *testing.T) {
	text := readRepoText(t, "deploy/terraform/main.tf")
	grant := terraformBlock(text, `resource "google_kms_crypto_key_iam_member" "bq_encryptor"`)
	if !strings.Contains(grant, `count         = var.kms_key_name == "" ? 0 : 1`) {
		t.Fatalf("main.tf must grant the BigQuery service agent on the key exactly when kms_key_name is set:\n%s", grant)
	}
	dependsOnGrant := regexp.MustCompile(`(?m)^  depends_on = \[google_kms_crypto_key_iam_member\.bq_encryptor\]$`)
	var encrypted []string
	for _, m := range regexp.MustCompile(`(?s)(?:^|\n)resource "([^"]+)" "([^"]+)" \{\n(.*?)\n\}`).FindAllStringSubmatch(text, -1) {
		if !strings.Contains(m[3], "kms_key_name = var.kms_key_name") {
			continue
		}
		encrypted = append(encrypted, m[2])
		if !dependsOnGrant.MatchString(m[3]) {
			t.Errorf("%s.%s is encrypted with var.kms_key_name but does not depend on the key grant", m[1], m[2])
		}
	}
	if want := append([]string{"otel"}, terraformTables...); !slices.Equal(encrypted, want) {
		t.Errorf("resources encrypted with var.kms_key_name = %v, want %v", encrypted, want)
	}
}

// terraformExpirationVariables maps each table main.tf provisions to the
// variable holding its partition expiration in days.
var terraformExpirationVariables = map[string]string{
	"otel_spans":      "spans_partition_expiration_days",
	"otel_logs":       "logs_partition_expiration_days",
	"otel_metrics":    "metrics_partition_expiration_days",
	"otel_quarantine": "quarantine_partition_expiration_days",
}

// Each table takes its partition expiration from its own variable, converted
// from days to the milliseconds the provider takes and left unset at 0, and
// the partition filter requirement from require_partition_filter, set on the
// table because the time_partitioning argument of that name is deprecated.
// An expiration variable takes what the exporter's partition_expiration_days
// options take: a whole number of days, 0 or more.
func TestTerraformTablesTakeRetentionAndPartitionFilterVariables(t *testing.T) {
	text := readRepoText(t, "deploy/terraform/main.tf")
	if regexp.MustCompile(`expiration_ms\s*=\s*[0-9]`).MatchString(text) {
		t.Error("main.tf hard-codes a partition expiration instead of reading it from a variable")
	}
	vars := readRepoText(t, "deploy/terraform/variables.tf")
	for _, table := range terraformTables {
		variable := terraformExpirationVariables[table]
		body := terraformBlock(text, `resource "google_bigquery_table" "`+table+`"`)
		partitioning := firstCapture(`(?s)\n  time_partitioning \{\n(.*?)\n  \}`, body)
		expiration := fmt.Sprintf("expiration_ms = var.%[1]s > 0 ? var.%[1]s * %[2]d : null", variable, (24 * time.Hour).Milliseconds())
		if !strings.Contains(partitioning, expiration) {
			t.Errorf("%s time_partitioning does not set %s:\n%s", table, expiration, partitioning)
		}
		if strings.Contains(partitioning, "require_partition_filter") {
			t.Errorf("%s sets the deprecated time_partitioning.require_partition_filter", table)
		}
		if !strings.Contains(body, "\n  require_partition_filter = var.require_partition_filter\n") {
			t.Errorf("%s does not set require_partition_filter = var.require_partition_filter on the table", table)
		}
		block := terraformBlock(vars, `variable "`+variable+`"`)
		for _, want := range []string{
			"type        = number",
			fmt.Sprintf("condition     = var.%[1]s >= 0 && floor(var.%[1]s) == var.%[1]s", variable),
		} {
			if !strings.Contains(block, want) {
				t.Errorf("variables.tf %s does not contain %s:\n%s", variable, want, block)
			}
		}
	}
	if block := terraformBlock(vars, `variable "require_partition_filter"`); !strings.Contains(block, "type        = bool") {
		t.Errorf("variables.tf require_partition_filter is not a bool:\n%s", block)
	}
}

// require_partition_filter applies to otel_spans too, and BigQuery rejects
// every query over v_gen_ai_logs, v_gen_ai_metric_exemplars, and
// v_gen_ai_agent_trajectory while otel_spans requires a partition filter,
// because each joins spans on a window that depends on each row. As the
// collector does, the module creates those three views only while
// require_partition_filter is off or allow_unprunable_views (default false) is
// set, and gates no other view. The outputs stay valid without the views, and
// the v_gen_ai_logs of an earlier release moves to instance 0 instead of being
// replaced.
func TestTerraformCrossSignalViewsFollowAllowUnprunableViews(t *testing.T) {
	variable := terraformBlock(readRepoText(t, "deploy/terraform/variables.tf"), `variable "allow_unprunable_views"`)
	for _, want := range []string{"type        = bool", "default     = false"} {
		if !strings.Contains(variable, want) {
			t.Errorf("variables.tf allow_unprunable_views does not contain %s:\n%s", want, variable)
		}
	}

	mainTF := readRepoText(t, "deploy/terraform/main.tf")
	const gate = "\n  count = (!var.require_partition_filter || var.allow_unprunable_views) ? 1 : 0\n"
	gated := []string{schema.GenAILogsViewName, schema.GenAIMetricExemplarsViewName, schema.GenAIAgentTrajectoryViewName}
	for _, view := range gated {
		if body := terraformBlock(mainTF, `resource "google_bigquery_table" "`+view+`"`); !strings.Contains("\n"+body, gate) {
			t.Errorf("main.tf %s is not gated by%s", view, gate)
		}
	}
	countArgument := regexp.MustCompile(`(?m)^  count\s+=`)
	for _, view := range append(slices.Clone(schema.SpanCompanionViewOrder), schema.GenAIMetricsViewName) {
		if slices.Contains(gated, view) {
			continue
		}
		body := terraformBlock(mainTF, `resource "google_bigquery_table" "`+view+`"`)
		if body == "" {
			t.Fatalf("main.tf has no %s view", view)
		}
		if countArgument.MatchString(body) || strings.Contains(body, "allow_unprunable_views") {
			t.Errorf("main.tf gates the view %s, which does not join otel_spans on a row-dependent window", view)
		}
	}
	if n := strings.Count(mainTF, "var.allow_unprunable_views"); n != len(gated) {
		t.Errorf("main.tf reads var.allow_unprunable_views %d times, want %d, in the gates of %v", n, len(gated), gated)
	}
	if moved := "\nmoved {\n  from = google_bigquery_table.v_gen_ai_logs\n  to   = google_bigquery_table.v_gen_ai_logs[0]\n}\n"; !strings.Contains(mainTF, moved) {
		t.Errorf("main.tf does not move the earlier google_bigquery_table.v_gen_ai_logs to instance 0:%s", moved)
	}

	outputs := readRepoText(t, "deploy/terraform/outputs.tf")
	for _, view := range []string{schema.GenAIMetricExemplarsViewName, schema.GenAIAgentTrajectoryViewName} {
		if got, want := terraformAttribute(terraformBlock(outputs, `output "`+view+`_table_id"`), "value"),
			"one(google_bigquery_table."+view+"[*].id)"; got != want {
			t.Errorf("outputs.tf %s_table_id has the value %q, want %q", view, got, want)
		}
	}
	unindexed := regexp.MustCompile(`google_bigquery_table\.(v_gen_ai_logs|v_gen_ai_metric_exemplars|v_gen_ai_agent_trajectory)\.`)
	for _, file := range []string{"main.tf", "outputs.tf", "governance.tf"} {
		if m := unindexed.FindString(readRepoText(t, "deploy/terraform/"+file)); m != "" {
			t.Errorf("deploy/terraform/%s reads %s without an instance key, which is invalid once the view has count", file, m)
		}
	}
}

// The prose gives the column count of the tables, and each count is taken
// here from the exporter's schema, so a column cannot be added without the
// prose following it.
func TestDocsStateTableColumnCounts(t *testing.T) {
	columns := map[string]int{
		"otel_spans":      len(schema.SpansTableSchema().GetFields()),
		"otel_logs":       len(schema.LogsTableSchema().GetFields()),
		"otel_metrics":    len(schema.MetricsTableSchema().GetFields()),
		"otel_quarantine": len(schema.QuarantineTableSchema().GetFields()),
	}
	for _, doc := range []struct {
		path string
		// pattern takes the table name, and its group captures the count.
		pattern string
		tables  []string
	}{
		{"deploy/terraform/main.tf", `(?m)^# %s: ([0-9]+)-column\b`, terraformTables},
		{"docs/schema-rationale.md", "(?m)^\\| `%s` is a ([0-9]+)-column\\b", []string{"otel_metrics", "otel_quarantine"}},
		{"README.md", "`%s` stores [^.]*? ([0-9]+)-column\\b", []string{"otel_metrics"}},
	} {
		text := readRepoText(t, doc.path)
		for _, table := range doc.tables {
			got := firstCapture(fmt.Sprintf(doc.pattern, table), text)
			if want := strconv.Itoa(columns[table]); got != want {
				t.Errorf("%s gives %s %q columns, but its schema has %s", doc.path, table, got, want)
			}
		}
	}
}

// kmsKeyLocation is the Cloud KMS location of the keys that can encrypt a
// BigQuery dataset in datasetLocation: us for the US multi-region, europe for
// the EU multi-region, and the dataset's own region otherwise.
func kmsKeyLocation(datasetLocation string) string {
	if location := strings.ToLower(datasetLocation); location != "eu" {
		return location
	}
	return "europe"
}

// A CMEK key has to be in the location of the dataset it encrypts, so every
// example command in deploy/README.md that sets kms_key_name names a key in
// the location of the dataset that command creates: the location it sets, or
// the module default. The CMEK section gives the rule for each kind of
// location.
func TestDeployReadmeCMEKExamplesUseKeysInTheDatasetLocation(t *testing.T) {
	readme := readRepoText(t, "deploy/README.md")
	defaultLocation := firstCapture(`(?m)^  default\s+=\s+"([^"]+)"$`,
		terraformBlock(readRepoText(t, "deploy/terraform/variables.tf"), `variable "location"`))
	if defaultLocation == "" {
		t.Fatal("variables.tf location has no default")
	}
	examples := 0
	for _, m := range regexp.MustCompile("(?s)```bash\n(.*?)```").FindAllStringSubmatch(readme, -1) {
		keyLocation := firstCapture(`kms_key_name=projects/[^/]+/locations/([^/]+)/`, m[1])
		if keyLocation == "" {
			continue
		}
		examples++
		location := firstCapture(`-var="location=([^"]+)"`, m[1])
		if location == "" {
			location = defaultLocation
		}
		if want := kmsKeyLocation(location); keyLocation != want {
			t.Errorf("deploy/README.md encrypts a dataset in %s with a key in locations/%s, but it needs a key in locations/%s:\n%s",
				location, keyLocation, want, m[1])
		}
	}
	if examples == 0 {
		t.Error("deploy/README.md has no example that sets kms_key_name")
	}
	section := firstCapture(`(?s)\n### CMEK Encryption\n(.*?)\n### `, readme)
	for _, location := range []string{"US", "EU", "us-central1"} {
		for _, want := range []string{"`" + location + "`", "`locations/" + kmsKeyLocation(location) + "`"} {
			if !strings.Contains(section, want) {
				t.Errorf("the CMEK section does not give the key location for a dataset in %s: it has no %s", location, want)
			}
		}
	}
}

// The Max Staleness section documents the values the module takes: each
// duration it shows is one the exporter and the variable validation accept,
// its interval example has the form the validation takes as an interval, the
// range it gives is the exporter's, and the conversion it shows is the
// interval the exporter writes. Neither it nor the variable describes the
// option as keeping recent partitions out of queries any more.
func TestDeployReadmeDocumentsMaxStalenessValues(t *testing.T) {
	section := firstCapture(`(?s)\n### Max Staleness\n(.*?)\n### `, readRepoText(t, "deploy/README.md"))
	if section == "" {
		t.Fatal("deploy/README.md has no Max Staleness section")
	}
	patterns := terraformValidationPatterns(t, "max_staleness")
	if len(patterns) != 2 {
		t.Fatalf("max_staleness has %d regex validation rules, want the format rule and the range rule", len(patterns))
	}
	format, duration := patterns[0], patterns[1]
	// Code spans are read from the prose, outside the example command, whose
	// fences would pair backticks wrongly; the command's value is read apart.
	prose := regexp.MustCompile("(?s)```.*?```").ReplaceAllString(section, "")
	values := regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(prose, -1)
	values = append(values, regexp.MustCompile(`max_staleness=([^"]+)"`).FindAllStringSubmatch(section, -1)...)
	durations := 0
	for _, m := range values {
		if value := m[1]; duration.MatchString(value) {
			durations++
			if _, err := parseMaxStaleness("max_staleness", value); err != nil || !format.MatchString(value) {
				t.Errorf("the Max Staleness section shows the duration %q, which the module or the exporter rejects: %v", value, err)
			}
		}
	}
	if durations == 0 {
		t.Error("the Max Staleness section shows no duration")
	}
	if interval := firstCapture("interval string \\(for example `([^`]+)`\\)", prose); !format.MatchString(interval) || duration.MatchString(interval) {
		t.Errorf("the Max Staleness section gives %q as its SQL interval example, which the module does not take as an interval", interval)
	}

	bounds := regexp.MustCompile("whole seconds from `([^`]+)` to `([^`]+)`").FindStringSubmatch(prose)
	if bounds == nil {
		t.Fatal("the Max Staleness section does not give the range of a duration")
	}
	for i, want := range []time.Duration{minTableStaleness, maxTableStaleness} {
		if got, err := time.ParseDuration(bounds[i+1]); err != nil || got != want {
			t.Errorf("the Max Staleness section bounds a duration at %q, but the exporter's bound is %v", bounds[i+1], want)
		}
	}

	conversion := regexp.MustCompile("`([^`]+)` is sent as `([^`]+)`").FindStringSubmatch(prose)
	if conversion == nil {
		t.Fatal("the Max Staleness section shows no conversion of a duration")
	}
	d, err := parseMaxStaleness("max_staleness", conversion[1])
	if err != nil {
		t.Fatal(err)
	}
	if want := bq.IntervalValueFromDuration(d).String(); conversion[2] != want {
		t.Errorf("the Max Staleness section converts %s to %q, but the exporter writes %q", conversion[1], conversion[2], want)
	}

	variable := terraformBlock(readRepoText(t, "deploy/terraform/variables.tf"), `variable "max_staleness"`)
	for name, text := range map[string]string{"deploy/README.md": section, "variables.tf": variable} {
		if strings.Contains(strings.ToLower(text), "partitions newer than") {
			t.Errorf("%s still says max_staleness keeps partitions newer than its value out of queries", name)
		}
	}
}

// deploy/README.md lists every retention and partition filter variable, and
// allow_unprunable_views, with the default variables.tf declares, and says
// that the collector's own options for a table apply only to tables the
// collector creates, naming every per-signal option other than the table name.
func TestDeployReadmeDocumentsTerraformGovernance(t *testing.T) {
	readme := readRepoText(t, "deploy/README.md")
	vars := readRepoText(t, "deploy/terraform/variables.tf")
	for _, variable := range append(slices.Sorted(maps.Values(terraformExpirationVariables)), "require_partition_filter", "allow_unprunable_views") {
		want := firstCapture(`(?m)^  default\s+=\s+(.+)$`, terraformBlock(vars, `variable "`+variable+`"`))
		got := firstCapture("(?m)^\\| `"+variable+"` \\| `([^`]*)` \\|", readme)
		if want == "" || got != want {
			t.Errorf("deploy/README.md gives %s the default %q, but variables.tf declares %q", variable, got, want)
		}
	}
	intro := firstCapture(`(?s)\n## Enterprise Governance\n(.*?)\n### `, readme)
	for _, key := range exporterConfigKeys() {
		if option, ok := strings.CutPrefix(key, "traces."); ok && option != "table" && !strings.Contains(intro, "`"+option+"`") {
			t.Errorf("the Enterprise Governance introduction does not say that the collector's %s applies only to tables it creates", option)
		}
	}
	if !strings.Contains(intro, "`auto_create.tables`") {
		t.Error("the Enterprise Governance introduction does not name auto_create.tables")
	}
}

// readRepoText returns the contents of a repository file as a string.
func readRepoText(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(raw)
}

// firstCapture returns the first capture group of the first match of pattern in
// text, or "" when pattern does not match.
func firstCapture(pattern, text string) string {
	m := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if m == nil {
		return ""
	}
	return m[1]
}

// collectorVersionsFromGoMod reads the Collector release this module builds
// against from go.mod: the v0.x version shared by the Collector component and
// contrib modules (read from componentstatus) and the v1.x version shared by the
// stable modules (read from confmap). Every pin that has to follow a Collector
// bump is compared with these two values instead of repeating a literal that
// must be edited by hand with each release.
func collectorVersionsFromGoMod(t *testing.T, path string) (collectorVersion, confmapVersion string) {
	t.Helper()
	goMod := readRepoText(t, path)
	required := func(module string) string {
		t.Helper()
		version := firstCapture(`(?m)^\s*(?:require\s+)?`+regexp.QuoteMeta(module)+` (v\d+\.\d+\.\d+)`, goMod)
		if version == "" {
			t.Fatalf("%s has no requirement for %s", path, module)
		}
		return version
	}
	return required("go.opentelemetry.io/collector/component/componentstatus"),
		required("go.opentelemetry.io/collector/confmap")
}

// builderModule is one gomod entry of an OCB manifest.
type builderModule struct {
	Gomod string `yaml:"gomod"`
}

// builderManifestModules returns every gomod entry of the OCB manifest at path,
// across all of its component sections and its telemetry section.
func builderManifestModules(t *testing.T, path string) []string {
	t.Helper()
	var manifest struct {
		Receivers  []builderModule `yaml:"receivers"`
		Processors []builderModule `yaml:"processors"`
		Exporters  []builderModule `yaml:"exporters"`
		Connectors []builderModule `yaml:"connectors"`
		Extensions []builderModule `yaml:"extensions"`
		Providers  []builderModule `yaml:"providers"`
		Telemetry  builderModule   `yaml:"telemetry"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, path)), &manifest); err != nil {
		t.Fatalf("unmarshaling %s: %v", path, err)
	}
	var gomods []string
	for _, section := range [][]builderModule{
		manifest.Receivers, manifest.Processors, manifest.Exporters, manifest.Connectors,
		manifest.Extensions, manifest.Providers, {manifest.Telemetry},
	} {
		for _, module := range section {
			if module.Gomod != "" {
				gomods = append(gomods, module.Gomod)
			}
		}
	}
	return gomods
}

// workflowSetupGoVersions returns, per job, the go-version of every
// actions/setup-go step in the workflow at path. A step that sets no go-version,
// for example one that resolves the go.mod minimum, is reported as "(none)".
func workflowSetupGoVersions(t *testing.T, path string) map[string][]string {
	t.Helper()
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string         `yaml:"uses"`
				With map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, path)), &workflow); err != nil {
		t.Fatalf("unmarshaling %s: %v", path, err)
	}
	versions := map[string][]string{}
	for name, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if !strings.HasPrefix(step.Uses, "actions/setup-go@") {
				continue
			}
			version := "(none)"
			if v, ok := step.With["go-version"]; ok {
				version = fmt.Sprint(v)
			}
			versions[name] = append(versions[name], version)
		}
	}
	return versions
}

// The Collector release is pinned in several places that no tool bumps
// together: the Makefile, both OCB manifests, the example Dockerfile, and the CI
// and release workflows. go.mod is the source of truth. A pin that lags it
// builds the custom distribution from a different Collector than the one the
// exporter's own tests ran against, and leaves the stock image that CI compares
// the example against on another release than the builder.
func TestCollectorPinsStayInLockstepWithGoMod(t *testing.T) {
	collectorVersion, confmapVersion := collectorVersionsFromGoMod(t, "go.mod")

	if got := firstCapture(`(?m)^OCB_VERSION \?= (\S+)$`, readRepoText(t, "Makefile")); got != collectorVersion {
		t.Errorf("Makefile OCB_VERSION = %q, want %q from go.mod", got, collectorVersion)
	}

	builderPin := regexp.MustCompile(`go\.opentelemetry\.io/collector/cmd/builder@(v\d+\.\d+\.\d+)`)
	for _, path := range []string{"example/Dockerfile", ".github/workflows/ci.yml", ".github/workflows/release.yml"} {
		pins := builderPin.FindAllStringSubmatch(readRepoText(t, path), -1)
		if len(pins) == 0 {
			t.Errorf("%s does not pin the OpenTelemetry Collector builder", path)
		}
		for _, pin := range pins {
			if pin[1] != collectorVersion {
				t.Errorf("%s pins the builder at %s, want %s from go.mod", path, pin[1], collectorVersion)
			}
		}
	}

	for _, path := range []string{"example/otelcol-builder.yaml", "example/otelcol-builder-release.yaml"} {
		var collectorModules, providers int
		var service bool
		for _, gomod := range builderManifestModules(t, path) {
			fields := strings.Fields(gomod)
			if len(fields) != 2 {
				t.Errorf("%s: gomod %q is not \"<module> <version>\"", path, gomod)
				continue
			}
			module, version := fields[0], fields[1]
			var want string
			switch {
			case strings.HasPrefix(module, "go.opentelemetry.io/collector/confmap/provider/"):
				want = confmapVersion
				providers++
			case strings.HasPrefix(module, "go.opentelemetry.io/collector/"),
				strings.HasPrefix(module, "github.com/open-telemetry/opentelemetry-collector-contrib/"):
				want = collectorVersion
				collectorModules++
			default:
				// The exporter itself, pinned to the checkout being built.
				continue
			}
			service = service || module == "go.opentelemetry.io/collector/service"
			if version != want {
				t.Errorf("%s pins %s at %s, want %s from go.mod", path, module, version, want)
			}
		}
		if collectorModules == 0 || providers == 0 || !service {
			t.Errorf("%s: checked %d Collector modules and %d confmap providers, service pinned = %v; want all three present", path, collectorModules, providers, service)
		}
	}

	stock := firstCapture(`(?m)^\s*STOCK_IMAGE: otel/opentelemetry-collector-contrib:(\d+\.\d+\.\d+)@sha256:[0-9a-f]{64}$`, readRepoText(t, ".github/workflows/ci.yml"))
	if stock == "" {
		t.Error("ci.yml STOCK_IMAGE is not an otel/opentelemetry-collector-contrib image pinned by version and sha256 digest")
	} else if "v"+stock != collectorVersion {
		t.Errorf("ci.yml STOCK_IMAGE is v%s, want %s from go.mod", stock, collectorVersion)
	}
}

// runtimeImage returns the image the last stage of a Dockerfile runs on, which
// is the image that ships. Stage flags such as --platform and an AS alias are
// not part of it.
func runtimeImage(t *testing.T, path string) string {
	t.Helper()
	froms := regexp.MustCompile(`(?m)^FROM\s+(?:--\S+\s+)*(\S+)(?:\s+AS\s+\S+)?\s*$`).FindAllStringSubmatch(readRepoText(t, path), -1)
	if len(froms) == 0 {
		t.Fatalf("%s has no FROM instruction", path)
	}
	return froms[len(froms)-1][1]
}

// Dockerfile.goreleaser builds the released image and example/Dockerfile builds
// the Compose and CI image. Both run on the same distroless base, pinned by tag
// and digest, so the image that is tested is the image that ships and a
// Dependabot bump of one cannot silently leave the other behind.
func TestRuntimeImagesShareBaseImageAndDigest(t *testing.T) {
	pinned := regexp.MustCompile(`^gcr\.io/distroless/[^:@\s]+:[^:@\s]+@sha256:[0-9a-f]{64}$`)
	images := map[string]string{}
	for _, path := range []string{"Dockerfile.goreleaser", "example/Dockerfile"} {
		images[path] = runtimeImage(t, path)
		if !pinned.MatchString(images[path]) {
			t.Errorf("%s runs on %q, want a gcr.io/distroless image pinned by tag and sha256 digest", path, images[path])
		}
	}
	if images["Dockerfile.goreleaser"] != images["example/Dockerfile"] {
		t.Errorf("runtime images differ: Dockerfile.goreleaser runs on %q, example/Dockerfile on %q; raise both pins together",
			images["Dockerfile.goreleaser"], images["example/Dockerfile"])
	}
}

// Both runtime images publish the OTLP, self-telemetry and health-check ports
// the example configuration serves, so a container started from either behaves
// the same.
func TestRuntimeImagesExposeSamePorts(t *testing.T) {
	exposeLine := regexp.MustCompile(`(?m)^EXPOSE\s+(.+?)\s*$`)
	ports := map[string]string{}
	for _, path := range []string{"Dockerfile.goreleaser", "example/Dockerfile"} {
		var all []string
		for _, line := range exposeLine.FindAllStringSubmatch(readRepoText(t, path), -1) {
			all = append(all, strings.Fields(line[1])...)
		}
		if len(all) == 0 {
			t.Errorf("%s exposes no ports", path)
		}
		ports[path] = strings.Join(all, " ")
	}
	if ports["Dockerfile.goreleaser"] != ports["example/Dockerfile"] {
		t.Errorf("exposed ports differ: Dockerfile.goreleaser %q, example/Dockerfile %q",
			ports["Dockerfile.goreleaser"], ports["example/Dockerfile"])
	}
}

// TestDependabotCombinesDockerEntries verifies that dependabot.yml uses a
// single docker entry with directories instead of separate entries, and
// that the docker entry groups updates by dependency-name.
func TestDependabotCombinesDockerEntries(t *testing.T) {
	var doc struct {
		Updates []struct {
			PackageEcosystem string   `yaml:"package-ecosystem"`
			Directories      []string `yaml:"directories"`
			Groups           map[string]struct {
				GroupBy string `yaml:"group-by"`
			} `yaml:"groups"`
		} `yaml:"updates"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, ".github/dependabot.yml")), &doc); err != nil {
		t.Fatalf("parsing .github/dependabot.yml: %v", err)
	}
	var dockerCount int
	for _, u := range doc.Updates {
		if u.PackageEcosystem == "docker" {
			dockerCount++
			if len(u.Directories) != 2 {
				t.Errorf("docker entry has %d directories, want 2", len(u.Directories))
			}
			want := map[string]bool{"/": false, "/example": false}
			for _, d := range u.Directories {
				want[d] = true
			}
			for d, found := range want {
				if !found {
					t.Errorf("docker entry is missing directory %q", d)
				}
			}
			if g, ok := u.Groups["docker"]; !ok {
				t.Error("docker entry must have a 'docker' group with group-by")
			} else if g.GroupBy != "dependency-name" {
				t.Errorf("docker group group-by = %q, want %q", g.GroupBy, "dependency-name")
			}
		}
	}
	if dockerCount != 1 {
		t.Errorf(".github/dependabot.yml has %d docker entries, want 1 (combined with directories)", dockerCount)
	}
}

// TestBenchFuzzWorkflowListsEveryFuzzTarget keeps the bounded fuzzing job in
// .github/workflows/bench-fuzz.yml in step with the fuzz targets in the code.
// `go test -fuzz` takes one target per run, so the job lists its targets one by
// one: a target missing from that list is never fuzzed in CI, and an entry whose
// target was renamed or removed fuzzes nothing. The job runs `go test` without
// build tags, so a target in a file with a build constraint would not even be
// compiled, and its run would pass having fuzzed nothing. Each target also
// needs a committed seed corpus under <pkg>/testdata/fuzz/<target>/, which
// plain `go test` replays on every run, and the job has to upload that
// directory when fuzzing fails, because the failing input it writes there
// exists only on the runner.
func TestBenchFuzzWorkflowListsEveryFuzzTarget(t *testing.T) {
	const path = ".github/workflows/bench-fuzz.yml"
	var workflow struct {
		Jobs struct {
			Fuzz struct {
				Strategy struct {
					Matrix struct {
						Include []struct {
							Pkg    string `yaml:"pkg"`
							Target string `yaml:"target"`
						} `yaml:"include"`
					} `yaml:"matrix"`
				} `yaml:"strategy"`
				Steps []struct {
					If   string         `yaml:"if"`
					Uses string         `yaml:"uses"`
					With map[string]any `yaml:"with"`
				} `yaml:"steps"`
			} `yaml:"fuzz"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readRepoText(t, path)), &workflow); err != nil {
		t.Fatalf("unmarshaling %s: %v", path, err)
	}

	declared := moduleFuzzTargets(t)
	if len(declared) == 0 {
		t.Fatal("found no fuzz targets in the module's test files")
	}

	listed := map[fuzzTarget]bool{}
	for _, entry := range workflow.Jobs.Fuzz.Strategy.Matrix.Include {
		ft := fuzzTarget{pkg: entry.Pkg, name: entry.Target}
		if listed[ft] {
			t.Errorf("%s lists fuzz target %s more than once", path, ft)
		}
		listed[ft] = true
		if _, ok := declared[ft]; !ok {
			t.Errorf("%s lists fuzz target %s, which no test file declares", path, ft)
		}
	}

	byName := func(a, b fuzzTarget) int { return strings.Compare(a.String(), b.String()) }
	for _, ft := range slices.SortedFunc(maps.Keys(declared), byName) {
		if !listed[ft] {
			t.Errorf("fuzz target %s (%s) is missing from jobs.fuzz.strategy.matrix.include in %s", ft, declared[ft], path)
		}
		if line, err := goBuildConstraint(declared[ft], nil); err != nil {
			t.Errorf("reading the build constraints of %s: %v", declared[ft], err)
		} else if line != "" {
			t.Errorf("fuzz target %s is declared in %s, which has the build constraint %q: the untagged go test -fuzz in %s would compile nothing to fuzz and still pass",
				ft, declared[ft], line, path)
		}
		corpus := filepath.Join(ft.pkg, "testdata", "fuzz", ft.name)
		entries, err := os.ReadDir(corpus)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("reading the seed corpus of fuzz target %s: %v", ft, err)
			continue
		}
		if !slices.ContainsFunc(entries, func(e fs.DirEntry) bool { return e.Type().IsRegular() }) {
			t.Errorf("fuzz target %s has no seed corpus file in %s", ft, corpus)
		}
	}

	uploads := false
	for _, step := range workflow.Jobs.Fuzz.Steps {
		if !strings.HasPrefix(step.Uses, "actions/upload-artifact@") {
			continue
		}
		uploads = true
		if cond := strings.TrimSpace(step.If); cond != "failure()" && cond != "${{ failure() }}" {
			t.Errorf("%s uploads the fuzz corpus with if: %q, want failure()", path, step.If)
		}
		if upload := fmt.Sprint(step.With["path"]); !strings.Contains(upload, "testdata/fuzz/") {
			t.Errorf("%s uploads path %q, want the fuzz target's testdata/fuzz directory", path, upload)
		}
	}
	if !uploads {
		t.Errorf("jobs.fuzz in %s has no actions/upload-artifact step for the failing input", path)
	}
}

// fuzzTarget is a fuzz target as the bench-fuzz workflow names it: the package
// argument `go test` takes and the name of the target function.
type fuzzTarget struct {
	pkg, name string
}

func (ft fuzzTarget) String() string { return ft.pkg + " " + ft.name }

// moduleFuzzTargets returns every fuzz target declared in the module's test
// files, mapped to the file that declares it. Like the go command matching
// ./..., it skips testdata and vendor directories, nested modules, and
// directories and files whose names start with "." or "_".
func moduleFuzzTargets(t *testing.T) map[fuzzTarget]string {
	t.Helper()
	targets := map[fuzzTarget]string{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}
		name := d.Name()
		ignored := strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
		if d.IsDir() {
			if ignored || name == "testdata" || name == "vendor" {
				return filepath.SkipDir
			}
			if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if ignored || !strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		pkg := "."
		if dir := filepath.Dir(path); dir != "." {
			pkg = "./" + filepath.ToSlash(dir)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && isFuzzTarget(fn) {
				targets[fuzzTarget{pkg: pkg, name: fn.Name.Name}] = path
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("collecting fuzz targets: %v", err)
	}
	return targets
}

// isFuzzTarget reports whether `go test` runs fn as a fuzz target, by the rule
// the go command applies: fn is named Fuzz, or Fuzz followed by anything but a
// lower-case letter, has no receiver, type parameters or results, and takes a
// single *testing.F. Like the go command, it accepts F under any import name.
func isFuzzTarget(fn *ast.FuncDecl) bool {
	suffix, ok := strings.CutPrefix(fn.Name.Name, "Fuzz")
	if !ok || fn.Recv != nil || fn.Type.TypeParams.NumFields() > 0 || fn.Type.Results.NumFields() > 0 {
		return false
	}
	if r, _ := utf8.DecodeRuneInString(suffix); unicode.IsLower(r) {
		return false
	}
	params := fn.Type.Params.List
	if len(params) != 1 || len(params[0].Names) > 1 {
		return false
	}
	ptr, ok := params[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := ptr.X.(type) {
	case *ast.Ident:
		return x.Name == "F"
	case *ast.SelectorExpr:
		return x.Sel.Name == "F"
	default:
		return false
	}
}

// goBuildConstraint returns the first //go:build or // +build line among the
// line comments above the package clause of a Go file, where the go command
// reads build constraints, or "" if there is none. filename and src are as
// for parser.ParseFile: a nil src reads filename.
func goBuildConstraint(filename string, src any) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filename, src, parser.PackageClauseOnly|parser.ParseComments)
	if err != nil {
		return "", err
	}
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			break
		}
		for _, c := range group.List {
			if constraint.IsGoBuild(c.Text) || constraint.IsPlusBuild(c.Text) {
				return c.Text, nil
			}
		}
	}
	return "", nil
}

func TestGoBuildConstraint(t *testing.T) {
	const header = "// Copyright 2026 Google LLC\n// SPDX-License-Identifier: Apache-2.0\n\n"
	for _, tc := range []struct {
		name, src, want string
	}{
		{name: "no constraint", src: header + "package p\n"},
		{name: "go:build after the license header", src: header + "//go:build integration\n\npackage p\n", want: "//go:build integration"},
		{name: "+build line", src: "// +build linux\n\npackage p\n", want: "// +build linux"},
		{name: "inside a block comment", src: "/*\n//go:build integration\n*/\n\npackage p\n"},
		{name: "below the package clause", src: "package p\n\n//go:build integration\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := goBuildConstraint("p_test.go", tc.src)
			if err != nil {
				t.Fatalf("goBuildConstraint: %v", err)
			}
			if got != tc.want {
				t.Errorf("goBuildConstraint = %q, want %q", got, tc.want)
			}
		})
	}
}

// stripSQLComments removes /* ... */ block comments and -- ... line comments
// so tests can inspect the active SQL statement of a template.
func stripSQLComments(sql string) string {
	noBlocks := regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(sql, "")
	return regexp.MustCompile(`(?m)--.*$`).ReplaceAllString(noBlocks, "")
}

// hasBalancedParens reports whether every '(' and ')' outside SQL single-quoted
// and backtick-quoted literals in sql is balanced and never dips below depth 0.
func hasBalancedParens(sql string) bool {
	depth := 0
	inSingle, inBacktick := false, false
	for _, r := range sql {
		switch {
		case r == '\'' && !inBacktick:
			inSingle = !inSingle
		case r == '`' && !inSingle:
			inBacktick = !inBacktick
		case inSingle || inBacktick:
		case r == '(':
			depth++
		case r == ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return !inSingle && !inBacktick && depth == 0
}

// extractWhereClause returns the full text of the last WHERE clause in
// activeSQL (up to the closing ')' that encloses the SELECT query or a
// top-level ';') together with its depth-0 projection (with the contents of
// every parenthesized group omitted).
func extractWhereClause(activeSQL string) (whereClause, topLevel string) {
	i := strings.LastIndex(activeSQL, "WHERE")
	if i < 0 {
		return "", ""
	}
	var full, top strings.Builder
	depth := 0
	inSingle, inBacktick := false, false
	for _, r := range activeSQL[i:] {
		switch {
		case r == '\'' && !inBacktick:
			inSingle = !inSingle
			full.WriteRune(r)
			if depth == 0 {
				top.WriteRune(r)
			}
		case r == '`' && !inSingle:
			inBacktick = !inBacktick
			full.WriteRune(r)
			if depth == 0 {
				top.WriteRune(r)
			}
		case inSingle || inBacktick:
			full.WriteRune(r)
			if depth == 0 {
				top.WriteRune(r)
			}
		case r == '(':
			depth++
			full.WriteRune(r)
		case r == ')':
			depth--
			if depth < 0 {
				return full.String(), top.String()
			}
			full.WriteRune(r)
		case r == ';' && depth == 0:
			return full.String(), top.String()
		default:
			full.WriteRune(r)
			if depth == 0 {
				top.WriteRune(r)
			}
		}
	}
	return full.String(), top.String()
}

// parenDepthAtSubstring returns the parenthesis nesting depth at the start of
// the first occurrence of sub in sql (ignoring parentheses inside single-quoted
// or backtick-quoted SQL literals), or (-1, false) if sub is not present.
func parenDepthAtSubstring(sql, sub string) (int, bool) {
	idx := strings.Index(sql, sub)
	if idx < 0 {
		return -1, false
	}
	depth := 0
	inSingle, inBacktick := false, false
	for _, r := range sql[:idx] {
		switch {
		case r == '\'' && !inBacktick:
			inSingle = !inSingle
		case r == '`' && !inSingle:
			inBacktick = !inBacktick
		case inSingle || inBacktick:
		case r == '(':
			depth++
		case r == ')':
			depth--
		}
	}
	return depth, true
}

// BigQuery Continuous Queries cannot read views built on QUALIFY or window
// functions, so every SQL template in deploy/continuous-queries/ reads the
// base spans table through APPENDS(TABLE ...) and exports to Cloud Pub/Sub.
// Templates start with a block comment (never --) so bq never mistakes leading
// comment text for a CLI flag, include a start_time partition filter, omit
// policy-tagged columns (status.message), and use only placeholders passed by
// templatefile in deploy/terraform/outputs.tf.
func TestContinuousQueryTemplatesReadBaseTablesViaAppends(t *testing.T) {
	const dir = "deploy/continuous-queries"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	wantTemplates := []string{
		"agent_latency_p95.sql",
		"token_budget_burn.sql",
		"tool_error_rate_alert.sql",
	}
	var gotTemplates []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			gotTemplates = append(gotTemplates, e.Name())
		}
	}
	slices.Sort(gotTemplates)
	if !slices.Equal(gotTemplates, wantTemplates) {
		t.Fatalf("SQL templates in %s = %v, want %v", dir, gotTemplates, wantTemplates)
	}

	outputs := readRepoText(t, "deploy/terraform/outputs.tf")
	renderedBlock := terraformBlock(outputs, `output "continuous_queries_rendered_sql"`)
	templateVarsSection := firstCapture(`(?s)templatefile\([^\n]+,\s*\{(.*?)\n\s*\}\)`, renderedBlock)
	if templateVarsSection == "" {
		t.Fatalf("could not parse templatefile vars map in continuous_queries_rendered_sql:\n%s", renderedBlock)
	}
	templatefileKeys := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\s+([a-z0-9_]+)\s+=`).FindAllStringSubmatch(templateVarsSection, -1) {
		templatefileKeys[m[1]] = true
	}

	specificChecks := map[string][]string{
		"tool_error_rate_alert.sql": {
			"'TOOL_ERROR' AS alert_kind",
			"status.code = 2",
			`$."error.type"`,
			`$."gen_ai.operation.name"') = 'execute_tool'`,
			`$."gen_ai.tool.name"`,
			`$."mcp.method.name"`,
		},
		"token_budget_burn.sql": {
			"'TOKEN_BUDGET_BURN' AS alert_kind",
			"> ${max_turn_input_tokens}",
			"> ${max_turn_output_tokens}",
		},
		"agent_latency_p95.sql": {
			"'AGENT_LATENCY_HIGH' AS alert_kind",
			"duration_nano / 1e6 > ${max_turn_duration_ms}",
		},
	}
	// Each per-span template builds the documented alert_id,
	// <ALERT_KIND>:<trace_id>:<span_id>, which consumers deduplicate on.
	alertKinds := map[string]string{
		"tool_error_rate_alert.sql": "TOOL_ERROR",
		"token_budget_burn.sql":     "TOKEN_BUDGET_BURN",
		"agent_latency_p95.sql":     "AGENT_LATENCY_HIGH",
	}
	// The non-lossy start_time predicate must stay a top-level AND conjunct of
	// WHERE (parenthesis depth 0) with no relative time filters or extra
	// start_time comparisons.
	const lookback = "AND start_time >= TIMESTAMP('1970-01-01 00:00:00+00')"
	topLevelOr := regexp.MustCompile(`(?i)\bOR\b`)
	partitionBoundRE := regexp.MustCompile(`start_time\s*>=\s*TIMESTAMP\('([^']+)'\)`)
	checkNonLossyWhere := func(label, sqlText string) {
		t.Helper()
		if !hasBalancedParens(sqlText) {
			t.Errorf("%s has unbalanced parentheses", label)
		}
		whereClause, top := extractWhereClause(sqlText)
		if whereClause == "" {
			t.Fatalf("%s has no WHERE clause", label)
		}
		if depth, ok := parenDepthAtSubstring(whereClause, lookback); !ok || depth != 0 {
			t.Errorf("%s: %q must appear in WHERE at parenthesis depth 0, got ok=%v depth=%d (WHERE: %q)", label, lookback, ok, depth, whereClause)
		}
		if !strings.Contains(top, "AND start_time >= TIMESTAMP") || topLevelOr.MatchString(top) {
			t.Errorf("%s: the start_time partition predicate is not a top-level AND conjunct of WHERE (top level: %q)", label, top)
		}
		if gotCount := strings.Count(whereClause, "start_time"); gotCount != 1 {
			t.Errorf("%s WHERE clause references start_time %d times, want 1 (WHERE: %q)", label, gotCount, whereClause)
		}
		upperWhere := strings.ToUpper(whereClause)
		for _, bannedWhere := range []string{"CURRENT_TIMESTAMP", "TIMESTAMP_SUB", "TIMESTAMP_DIFF", "INTERVAL"} {
			if strings.Contains(upperWhere, bannedWhere) {
				t.Errorf("%s WHERE clause contains relative time filter keyword %q (WHERE: %q)", label, bannedWhere, whereClause)
			}
		}
	}

	placeholderRE := regexp.MustCompile(`\$\{([a-zA-Z0-9_]+)\}`)
	readme := readRepoText(t, filepath.Join(dir, "README.md"))
	for _, docPath := range []string{filepath.Join(dir, "README.md"), "deploy/README.md", "README.md"} {
		docText := readRepoText(t, docPath)
		if strings.Contains(strings.ToLower(docText), "partition pruning succeeds") {
			t.Errorf("%s still claims the epoch start_time bound makes partition pruning succeed", docPath)
		}
	}
	appendsBlocks := 0
	for _, m := range regexp.MustCompile("(?s)```sql\\n(.*?)```").FindAllStringSubmatch(readme, -1) {
		if strings.Contains(m[1], "APPENDS(") {
			appendsBlocks++
			checkNonLossyWhere(dir+"/README.md SQL block", stripSQLComments(m[1]))
		}
	}
	if appendsBlocks < 2 {
		t.Errorf("%s/README.md has %d APPENDS SQL blocks, want at least 2 (INSERT INTO and TUMBLE)", dir, appendsBlocks)
	}

	parsedBounds := map[string]time.Time{}
	for _, name := range wantTemplates {
		path := filepath.Join(dir, name)
		rawSQL := readRepoText(t, path)
		trimmedRaw := strings.TrimSpace(rawSQL)
		if !strings.HasPrefix(trimmedRaw, "/*") || strings.HasPrefix(trimmedRaw, "--") {
			t.Errorf("%s must start with a /* ... */ block comment, not --, so bq CLI flag parsing cannot fail", path)
		}

		activeSQL := stripSQLComments(rawSQL)
		checkNonLossyWhere(path, activeSQL)
		for _, want := range []string{
			"EXPORT DATA",
			"CLOUD_PUBSUB",
			"https://pubsub.googleapis.com/projects/${project_id}/topics/${pubsub_topic}",
			"APPENDS(",
			"TABLE `${project_id}.${dataset_id}.${spans_table}`",
			lookback,
			`JSON_VALUE(attributes, '$."gen_ai.agent.name"')`,
			`JSON_VALUE(attributes, '$."gcp.vertex.agent.name"')`,
			`JSON_VALUE(attributes, '$."gen_ai.main_agent.name"')`,
		} {
			if !strings.Contains(activeSQL, want) {
				t.Errorf("%s active SQL is missing %q", path, want)
			}
		}
		for _, want := range specificChecks[name] {
			if !strings.Contains(activeSQL, want) {
				t.Errorf("%s active SQL is missing template predicate %q", path, want)
			}
		}
		if want := "CONCAT('" + alertKinds[name] + ":', trace_id, ':', span_id) AS alert_id"; !strings.Contains(activeSQL, want) {
			t.Errorf("%s active SQL does not build the documented alert_id %q", path, want)
		}
		if m := partitionBoundRE.FindStringSubmatch(activeSQL); len(m) == 2 {
			ts, err := time.Parse("2006-01-02 15:04:05-07", m[1])
			if err != nil {
				t.Errorf("%s: parsing partition bound %q: %v", path, m[1], err)
			} else {
				parsedBounds[name] = ts
			}
		} else {
			t.Errorf("%s: could not parse start_time TIMESTAMP bound from active SQL", path)
		}
		for _, banned := range []string{"_dedup", "v_gen_ai_", "v_mcp_", "duration_ns", "status.message"} {
			if strings.Contains(activeSQL, banned) {
				t.Errorf("%s active SQL contains banned reference %q", path, banned)
			}
		}
		upperSQL := strings.ToUpper(activeSQL)
		for _, bannedKW := range []string{"QUALIFY", "OVER (", "OVER(", "DISTINCT"} {
			if strings.Contains(upperSQL, bannedKW) {
				t.Errorf("%s active SQL contains unsupported Continuous Query keyword %q", path, bannedKW)
			}
		}
		for _, m := range placeholderRE.FindAllStringSubmatch(rawSQL, -1) {
			if !templatefileKeys[m[1]] {
				t.Errorf("%s references placeholder ${%s}, which is not passed by templatefile in deploy/terraform/outputs.tf", path, m[1])
			}
		}
		if !strings.Contains(readme, name) {
			t.Errorf("%s/README.md does not document %s", dir, name)
		}
	}
	for _, want := range []string{
		"APPENDS",
		"CONTINUOUS",
		"--continuous=true",
		"--connection_property=",
		"service_account=",
		"| bq query",
		"INSERT INTO",
		"TUMBLE",
		// A window alert has no span, so its alert_id encodes the typed tuple
		// with TO_JSON_STRING(STRUCT(...)) to preserve JSON string escaping and
		// NULL vs empty tool names across GROUP BY groups.
		"CONCAT('TOOL_ERROR_WINDOW:', TO_JSON_STRING(STRUCT(service_name, tool_name, window_start)))",
		"require_partition_filter",
		"enable_column_level_security",
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("%s/README.md is missing %q", dir, want)
		}
	}

	t.Run("recent_append_old_event_spans", func(t *testing.T) {
		now := time.Date(2026, 10, 16, 20, 0, 0, 0, time.UTC)
		makeQualifyingTraces := func(age time.Duration, svcName, toolName string, isAlert bool) ptrace.Traces {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", svcName)
			sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			sp.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
			sp.SetName("execute_tool")
			start := now.Add(-age)
			dur := 5 * time.Millisecond
			if isAlert {
				dur = 45 * time.Second
				sp.Status().SetCode(ptrace.StatusCodeError)
				sp.Attributes().PutInt("gen_ai.usage.input_tokens", 150000)
			} else {
				sp.Status().SetCode(ptrace.StatusCodeOk)
				sp.Attributes().PutInt("gen_ai.usage.input_tokens", 100)
			}
			sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
			sp.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(dur)))
			sp.Attributes().PutStr("gen_ai.operation.name", "execute_tool")
			sp.Attributes().PutStr("gen_ai.tool.name", toolName)
			return td
		}

		// Build a 10-day-old span replayed from otel_quarantine (30-day default retention).
		var marshaler ptrace.JSONMarshaler
		rawPayload, err := marshaler.MarshalTraces(makeQualifyingTraces(10*24*time.Hour, "api:worker", "lookup", true))
		if err != nil {
			t.Fatalf("MarshalTraces: %v", err)
		}
		qLine, err := json.Marshal(quarantinereplay.Record{
			IngestTime: now.Add(-10 * 24 * time.Hour).Format(time.RFC3339),
			Signal:     "spans",
			ErrorCode:  "schema_mismatch",
			RawPayload: string(rawPayload),
		})
		if err != nil {
			t.Fatalf("Marshal quarantine Record: %v", err)
		}
		memSink := quarantinereplay.NewMemorySink()
		sum, err := quarantinereplay.Replay(context.Background(), bytes.NewReader(append(qLine, '\n')), memSink, quarantinereplay.Options{})
		if err != nil || sum.ReplayedSpans != 1 || len(memSink.Traces) != 1 {
			t.Fatalf("quarantinereplay.Replay = (%+v, %v), traces=%d; want 1 replayed span", sum, err, len(memSink.Traces))
		}

		opts := transform.Options{
			Limits: transform.DefaultLimits(),
			Now:    func() time.Time { return now },
		}
		eightDayRows, rej := transform.SpanRows(makeQualifyingTraces(8*24*time.Hour, "api", "worker:lookup", true), opts)
		if len(rej) != 0 || len(eightDayRows) != 1 {
			t.Fatalf("8-day SpanRows = (%d rows, %d rejects), want (1, 0)", len(eightDayRows), len(rej))
		}
		tenDayReplayRows, rej := transform.SpanRows(memSink.Traces[0], opts)
		if len(rej) != 0 || len(tenDayReplayRows) != 1 {
			t.Fatalf("10-day replayed SpanRows = (%d rows, %d rejects), want (1, 0)", len(tenDayReplayRows), len(rej))
		}
		thirtyDayRows, rej := transform.SpanRows(makeQualifyingTraces(30*24*time.Hour, "agent-svc", "search", true), opts)
		if len(rej) != 0 || len(thirtyDayRows) != 1 {
			t.Fatalf("30-day SpanRows = (%d rows, %d rejects), want (1, 0)", len(thirtyDayRows), len(rej))
		}
		controlRows, rej := transform.SpanRows(makeQualifyingTraces(1*time.Hour, "agent-svc", "search", false), opts)
		if len(rej) != 0 || len(controlRows) != 1 {
			t.Fatalf("control SpanRows = (%d rows, %d rejects), want (1, 0)", len(controlRows), len(rej))
		}

		matchesTemplate := func(tmpl string, row transform.Row) bool {
			bound, hasBound := parsedBounds[tmpl]
			if !hasBound {
				t.Fatalf("no parsed start_time TIMESTAMP bound for %s", tmpl)
			}
			startMicros, ok := row["start_time"].(int64)
			if !ok {
				t.Fatalf("row start_time = %#v, want int64 micros", row["start_time"])
			}
			startTime := time.UnixMicro(startMicros).UTC()
			if startTime.Before(bound) {
				return false
			}
			var attrs map[string]any
			if rawAttrs, _ := row["attributes"].(string); rawAttrs != "" {
				if err := json.Unmarshal([]byte(rawAttrs), &attrs); err != nil {
					t.Fatalf("unmarshaling row attributes: %v", err)
				}
			}
			statusMap, _ := row["status"].(transform.Row)
			statusCode, _ := statusMap["code"].(int64)
			durNano, _ := row["duration_nano"].(int64)

			switch tmpl {
			case "tool_error_rate_alert.sql":
				isErr := statusCode == 2 || attrs["error.type"] != nil
				isToolOrMCP := attrs["gen_ai.operation.name"] == "execute_tool" || attrs["gen_ai.tool.name"] != nil || attrs["mcp.method.name"] != nil
				return isErr && isToolOrMCP
			case "token_budget_burn.sql":
				laxInt64 := func(v any) int64 {
					switch x := v.(type) {
					case float64:
						return int64(x)
					case string:
						n, _ := strconv.ParseInt(x, 10, 64)
						return n
					default:
						return 0
					}
				}
				return laxInt64(attrs["gen_ai.usage.input_tokens"]) > 100000 ||
					laxInt64(attrs["gen_ai.usage.output_tokens"]) > 32000
			case "agent_latency_p95.sql":
				op, _ := attrs["gen_ai.operation.name"].(string)
				validOp := op == "invoke_agent" || op == "chat" || op == "generate_content" || op == "execute_tool"
				return float64(durNano)/1e6 > 30000 && validOp
			default:
				t.Fatalf("unknown template %s", tmpl)
				return false
			}
		}

		for _, tc := range []struct {
			label string
			row   transform.Row
			want  bool
		}{
			{"8_day_old_late_batch", eightDayRows[0], true},
			{"10_day_old_quarantine_replay", tenDayReplayRows[0], true},
			{"30_day_old_quarantine_replay", thirtyDayRows[0], true},
			{"healthy_control", controlRows[0], false},
		} {
			if gotReceive, _ := tc.row["receive_time"].(int64); gotReceive != now.UnixMicro() {
				t.Errorf("%s receive_time = %d, want %d (current append time)", tc.label, gotReceive, now.UnixMicro())
			}
			for _, tmpl := range wantTemplates {
				if got := matchesTemplate(tmpl, tc.row); got != tc.want {
					t.Errorf("%s against %s = %v, want %v", tc.label, tmpl, got, tc.want)
				}
			}
		}
	})

	t.Run("window_identity_collision_free", func(t *testing.T) {
		var tumbleSQL string
		for _, m := range regexp.MustCompile("(?s)```sql\n(.*?)```").FindAllStringSubmatch(readme, -1) {
			if strings.Contains(m[1], "FROM TUMBLE(") {
				tumbleSQL = m[1]
				break
			}
		}
		if tumbleSQL == "" {
			t.Fatal("deploy/continuous-queries/README.md has no TUMBLE SQL block")
		}
		if !regexp.MustCompile(`(?i)\bFROM\s+TUMBLE\(\s*TABLE\s+\w+`).MatchString(tumbleSQL) {
			t.Errorf("TUMBLE SQL block must pass TABLE <cte> to TUMBLE:\n%s", tumbleSQL)
		}
		alertIDMatch := regexp.MustCompile(`CONCAT\('([^']+)',\s*TO_JSON_STRING\(STRUCT\(([^)]+)\)\)\)\s+AS\s+alert_id`).FindStringSubmatch(tumbleSQL)
		if len(alertIDMatch) != 3 {
			t.Fatalf("could not parse CONCAT('<prefix>', TO_JSON_STRING(STRUCT(...))) AS alert_id from TUMBLE SQL:\n%s", tumbleSQL)
		}
		prefix := alertIDMatch[1]
		if prefix != "TOOL_ERROR_WINDOW:" {
			t.Errorf("TUMBLE alert_id prefix = %q, want %q", prefix, "TOOL_ERROR_WINDOW:")
		}
		var structFields []string
		for _, f := range strings.Split(alertIDMatch[2], ",") {
			if col := strings.TrimSpace(f); col != "" {
				structFields = append(structFields, col)
			}
		}
		groupByClause := firstCapture(`(?i)\bGROUP\s+BY\s+([^\n;]+)`, tumbleSQL)
		if groupByClause == "" {
			t.Fatalf("could not parse GROUP BY clause from TUMBLE SQL:\n%s", tumbleSQL)
		}
		groupCols := map[string]bool{}
		for _, g := range strings.Split(groupByClause, ",") {
			if col := strings.TrimSpace(g); col != "" {
				groupCols[col] = true
			}
		}
		for _, wantCol := range []string{"service_name", "tool_name", "window_start"} {
			if !slices.Contains(structFields, wantCol) {
				t.Errorf("TUMBLE alert_id STRUCT fields %v missing required group column %q", structFields, wantCol)
			}
		}
		for _, col := range structFields {
			if !groupCols[col] {
				t.Errorf("TUMBLE alert_id STRUCT field %q is not in GROUP BY (%s)", col, groupByClause)
			}
		}

		strPtr := func(s string) *string { return &s }
		windowAlertID := func(serviceName, toolName *string, windowStart string) string {
			colValues := map[string]any{
				"service_name": serviceName,
				"tool_name":    toolName,
				"window_start": windowStart,
			}
			var buf bytes.Buffer
			buf.WriteByte('{')
			for i, col := range structFields {
				val, ok := colValues[col]
				if !ok {
					t.Fatalf("unknown column %q in TUMBLE alert_id STRUCT", col)
				}
				if i > 0 {
					buf.WriteByte(',')
				}
				keyBytes, err := json.Marshal(col)
				if err != nil {
					t.Fatalf("json.Marshal key %q: %v", col, err)
				}
				valBytes, err := json.Marshal(val)
				if err != nil {
					t.Fatalf("json.Marshal val for %q: %v", col, err)
				}
				buf.Write(keyBytes)
				buf.WriteByte(':')
				buf.Write(valBytes)
			}
			buf.WriteByte('}')
			return prefix + buf.String()
		}

		w1 := "2026-10-16 20:00:00+00"
		w2 := "2026-10-16 20:05:00+00"
		groups := []struct {
			label   string
			service *string
			tool    *string
			window  string
		}{
			{"colon_in_service", strPtr("api:worker"), strPtr("lookup"), w1},
			{"colon_in_tool", strPtr("api"), strPtr("worker:lookup"), w1},
			{"null_tool", strPtr("api"), nil, w1},
			{"empty_tool", strPtr("api"), strPtr(""), w1},
			{"unknown_tool", strPtr("api"), strPtr("unknown"), w1},
			{"null_service", nil, strPtr("lookup"), w1},
			{"empty_service", strPtr(""), strPtr("lookup"), w1},
			{"second_tool", strPtr("api"), strPtr("search"), w1},
			{"next_window", strPtr("api"), strPtr("lookup"), w2},
		}

		seen := map[string]string{}
		for _, g := range groups {
			key := windowAlertID(g.service, g.tool, g.window)
			if !strings.HasPrefix(key, "TOOL_ERROR_WINDOW:{") {
				t.Errorf("%s produced malformed alert_id %q", g.label, key)
			}
			if prev, exists := seen[key]; exists {
				t.Errorf("window alert_id collision between %s and %s: %q", prev, g.label, key)
			}
			seen[key] = g.label
		}
	})
}

type cqNameSpec struct {
	rawLocals       string
	nonAlnumRE      *regexp.Regexp
	normReplace     string
	normTrim        string
	fallbackSlug    string
	hashStart       int
	hashLen         int
	resCleanRE      *regexp.Regexp
	resSuffixRE     *regexp.Regexp
	resPrefix       string
	resFastMaxLen   int
	resSlowSlugCap  int
	topicCleanRE    *regexp.Regexp
	topicSuffixRE   *regexp.Regexp
	topicPrefix     string
	topicFastMaxLen int
	topicSlowCap    int
}

func parseContinuousQueryNameSpec(t *testing.T, mainTF string) cqNameSpec {
	t.Helper()
	var cqLocals string
	for _, m := range regexp.MustCompile(`(?s)(?:^|\n)locals \{\n(.*?)\n\}`).FindAllStringSubmatch(mainTF, -1) {
		if strings.Contains(m[1], "continuous_query_reservation_name") || strings.Contains(m[1], "otel_alerts_topic_name") {
			cqLocals = m[1]
			break
		}
	}
	if cqLocals == "" {
		t.Fatal("main.tf has no locals block defining continuous_query_reservation_name and otel_alerts_topic_name")
	}

	normMatch := regexp.MustCompile(`cq_dataset_norm\s*=\s*trim\(replace\(lower\(var\.dataset_id\),\s*"/([^"]+)/",\s*"([^"]+)"\),\s*"([^"]+)"\)`).FindStringSubmatch(cqLocals)
	if len(normMatch) != 4 {
		t.Fatalf("could not parse cq_dataset_norm from main.tf locals:\n%s", cqLocals)
	}
	slugFallback := firstCapture(`cq_dataset_slug\s*=\s*local\.cq_dataset_norm\s*!=\s*""\s*\?\s*local\.cq_dataset_norm\s*:\s*"([^"]+)"`, cqLocals)
	if slugFallback == "" {
		t.Fatalf("could not parse cq_dataset_slug from main.tf locals:\n%s", cqLocals)
	}
	hashMatch := regexp.MustCompile(`cq_dataset_hash\s*=\s*substr\(sha256\(var\.dataset_id\),\s*([0-9]+),\s*([0-9]+)\)`).FindStringSubmatch(cqLocals)
	if len(hashMatch) != 3 {
		t.Fatalf("could not parse cq_dataset_hash from main.tf locals:\n%s", cqLocals)
	}
	hashStart, _ := strconv.Atoi(hashMatch[1])
	hashLen, _ := strconv.Atoi(hashMatch[2])

	resMatch := regexp.MustCompile(`(?s)continuous_query_reservation_name\s*=\s*\(\s*` +
		`can\(regex\("([^"]+)",\s*var\.dataset_id\)\)\s*&&\s*!can\(regex\("([^"]+)",\s*var\.dataset_id\)\)\s*&&\s*` +
		`length\("([a-z0-9-]+)\$\{replace\(var\.dataset_id,\s*"_",\s*"-"\)\}"\)\s*<=\s*([0-9]+)\s*` +
		`\?\s*"([a-z0-9-]+)\$\{replace\(var\.dataset_id,\s*"_",\s*"-"\)\}"\s*` +
		`:\s*"([a-z0-9-]+)\$\{trim\(substr\(local\.cq_dataset_slug,\s*0,\s*min\(length\(local\.cq_dataset_slug\),\s*([0-9]+)\)\),\s*"-"\)\}-\$\{local\.cq_dataset_hash\}"\s*\)`).FindStringSubmatch(cqLocals)
	if len(resMatch) != 8 {
		t.Fatalf("could not parse continuous_query_reservation_name from main.tf locals:\n%s", cqLocals)
	}
	if resMatch[3] != resMatch[5] || resMatch[3] != resMatch[6] {
		t.Fatalf("continuous_query_reservation_name prefixes disagree: %q vs %q vs %q", resMatch[3], resMatch[5], resMatch[6])
	}
	resFastMaxLen, _ := strconv.Atoi(resMatch[4])
	resSlowSlugCap, _ := strconv.Atoi(resMatch[7])

	topicMatch := regexp.MustCompile(`(?s)otel_alerts_topic_name\s*=\s*\(\s*` +
		`can\(regex\("([^"]+)",\s*var\.dataset_id\)\)\s*&&\s*!can\(regex\("([^"]+)",\s*var\.dataset_id\)\)\s*&&\s*` +
		`length\("([a-z0-9-]+)\$\{var\.dataset_id\}"\)\s*<=\s*([0-9]+)\s*` +
		`\?\s*"([a-z0-9-]+)\$\{var\.dataset_id\}"\s*` +
		`:\s*"([a-z0-9-]+)\$\{trim\(substr\(local\.cq_dataset_slug,\s*0,\s*min\(length\(local\.cq_dataset_slug\),\s*([0-9]+)\)\),\s*"-"\)\}-\$\{local\.cq_dataset_hash\}"\s*\)`).FindStringSubmatch(cqLocals)
	if len(topicMatch) != 8 {
		t.Fatalf("could not parse otel_alerts_topic_name from main.tf locals:\n%s", cqLocals)
	}
	if topicMatch[3] != topicMatch[5] || topicMatch[3] != topicMatch[6] {
		t.Fatalf("otel_alerts_topic_name prefixes disagree: %q vs %q vs %q", topicMatch[3], topicMatch[5], topicMatch[6])
	}
	topicFastMaxLen, _ := strconv.Atoi(topicMatch[4])
	topicSlowCap, _ := strconv.Atoi(topicMatch[7])

	return cqNameSpec{
		rawLocals:       cqLocals,
		nonAlnumRE:      regexp.MustCompile(normMatch[1]),
		normReplace:     normMatch[2],
		normTrim:        normMatch[3],
		fallbackSlug:    slugFallback,
		hashStart:       hashStart,
		hashLen:         hashLen,
		resCleanRE:      regexp.MustCompile(resMatch[1]),
		resSuffixRE:     regexp.MustCompile(resMatch[2]),
		resPrefix:       resMatch[3],
		resFastMaxLen:   resFastMaxLen,
		resSlowSlugCap:  resSlowSlugCap,
		topicCleanRE:    regexp.MustCompile(topicMatch[1]),
		topicSuffixRE:   regexp.MustCompile(topicMatch[2]),
		topicPrefix:     topicMatch[3],
		topicFastMaxLen: topicFastMaxLen,
		topicSlowCap:    topicSlowCap,
	}
}

func (s cqNameSpec) derive(datasetID string) (reservation, topic string) {
	norm := strings.Trim(s.nonAlnumRE.ReplaceAllString(strings.ToLower(datasetID), s.normReplace), s.normTrim)
	slug := norm
	if slug == "" {
		slug = s.fallbackSlug
	}
	sum := sha256.Sum256([]byte(datasetID))
	fullHash := hex.EncodeToString(sum[:])
	hash := fullHash[s.hashStart : s.hashStart+s.hashLen]

	fastRes := s.resPrefix + strings.ReplaceAll(datasetID, "_", "-")
	if s.resCleanRE.MatchString(datasetID) && !s.resSuffixRE.MatchString(datasetID) && len(fastRes) <= s.resFastMaxLen {
		reservation = fastRes
	} else {
		prefix := slug
		if len(prefix) > s.resSlowSlugCap {
			prefix = prefix[:s.resSlowSlugCap]
		}
		prefix = strings.Trim(prefix, "-")
		reservation = s.resPrefix + prefix + "-" + hash
	}

	fastTopic := s.topicPrefix + datasetID
	if s.topicCleanRE.MatchString(datasetID) && !s.topicSuffixRE.MatchString(datasetID) && len(fastTopic) <= s.topicFastMaxLen {
		topic = fastTopic
	} else {
		prefix := slug
		if len(prefix) > s.topicSlowCap {
			prefix = prefix[:s.topicSlowCap]
		}
		prefix = strings.Trim(prefix, "-")
		topic = s.topicPrefix + prefix + "-" + hash
	}
	return reservation, topic
}

// Continuous Queries Terraform resources are opt-in: enable_continuous_queries
// defaults to false, every Continuous Query resource in main.tf is gated on
// var.enable_continuous_queries, outputs.tf renders all three SQL templates
// through templatefile and pipes SQL on stdin to bq query, and deploy/README.md
// and README.md document the setup.
func TestTerraformContinuousQueriesAreOptIn(t *testing.T) {
	const gate = "var.enable_continuous_queries"
	vars := readRepoText(t, "deploy/terraform/variables.tf")
	wantVars := []struct{ name, typ, def string }{
		{"enable_continuous_queries", "bool", "false"},
		{"continuous_query_slot_capacity", "number", "100"},
		{"continuous_query_edition", "string", `"ENTERPRISE"`},
		{"continuous_query_max_turn_input_tokens", "number", "100000"},
		{"continuous_query_max_turn_output_tokens", "number", "32000"},
		{"continuous_query_max_turn_duration_ms", "number", "30000"},
		{"alerts_deletion_protection", "bool", "false"},
		{"alerts_partition_expiration_days", "number", "0"},
	}
	for _, v := range wantVars {
		block := terraformBlock(vars, `variable "`+v.name+`"`)
		if block == "" {
			t.Errorf("variables.tf declares no %s", v.name)
			continue
		}
		if got := terraformAttribute(block, "type"); got != v.typ {
			t.Errorf("variables.tf gives %s the type %q, want %q", v.name, got, v.typ)
		}
		if got := terraformAttribute(block, "default"); got != v.def {
			t.Errorf("variables.tf gives %s the default %q, want %q", v.name, got, v.def)
		}
	}
	editions := firstCapture(`(?m)^\s+condition\s+=\s+contains\(\[([^\]]*)\], var\.continuous_query_edition\)$`,
		terraformBlock(vars, `variable "continuous_query_edition"`))
	if !strings.Contains(editions, `"ENTERPRISE"`) || !strings.Contains(editions, `"ENTERPRISE_PLUS"`) {
		t.Errorf("continuous_query_edition validation = %q, want ENTERPRISE and ENTERPRISE_PLUS", editions)
	}
	slotBlock := terraformBlock(vars, `variable "continuous_query_slot_capacity"`)
	for _, want := range []string{
		"var.continuous_query_slot_capacity >= 50",
		"var.continuous_query_slot_capacity <= 500",
		"var.continuous_query_slot_capacity % 50 == 0",
	} {
		if !strings.Contains(slotBlock, want) {
			t.Errorf("continuous_query_slot_capacity validation missing %q:\n%s", want, slotBlock)
		}
	}
	saBlock := terraformBlock(vars, `variable "service_account_id"`)
	if !strings.Contains(saBlock, "length(var.service_account_id) <= 30") {
		t.Errorf("service_account_id validation must allow up to 30 chars by default:\n%s", saBlock)
	}

	mainTF := readRepoText(t, "deploy/terraform/main.tf")
	cqSA := terraformBlock(mainTF, `resource "google_service_account" "continuous_queries"`)
	if !strings.Contains(cqSA, "condition     = length(var.service_account_id) <= 27") {
		t.Errorf("google_service_account.continuous_queries missing length(var.service_account_id) <= 27 precondition:\n%s", cqSA)
	}
	gated := gate + " ? 1 : 0"
	resources := []string{
		`resource "google_bigquery_table" "otel_alerts"`,
		`resource "google_pubsub_topic" "otel_alerts"`,
		`resource "google_service_account" "continuous_queries"`,
		`resource "google_bigquery_dataset_iam_member" "continuous_queries_viewer"`,
		`resource "google_bigquery_table_iam_member" "continuous_queries_alerts_writer"`,
		`resource "google_pubsub_topic_iam_member" "continuous_queries_publisher"`,
		`resource "google_pubsub_topic_iam_member" "continuous_queries_pubsub_viewer"`,
		`resource "google_project_iam_member" "continuous_queries_jobs"`,
		`resource "google_bigquery_reservation" "continuous_queries"`,
		`resource "google_bigquery_reservation_assignment" "continuous_queries"`,
	}
	for _, header := range resources {
		body := terraformBlock(mainTF, header)
		if body == "" {
			t.Errorf("main.tf is missing %s", header)
			continue
		}
		if got := terraformAttribute(body, "count"); got != gated {
			t.Errorf("main.tf %s sets count = %q, want %q", header, got, gated)
		}
	}

	alertsTable := terraformBlock(mainTF, `resource "google_bigquery_table" "otel_alerts"`)
	if got := terraformAttribute(alertsTable, "deletion_protection"); got != "var.alerts_deletion_protection" {
		t.Errorf("otel_alerts sets deletion_protection = %q, want var.alerts_deletion_protection", got)
	}
	if got := terraformAttribute(alertsTable, "require_partition_filter"); got != "var.require_partition_filter" {
		t.Errorf("otel_alerts sets require_partition_filter = %q, want var.require_partition_filter", got)
	}

	alertsTopic := terraformBlock(mainTF, `resource "google_pubsub_topic" "otel_alerts"`)
	if got := terraformAttribute(alertsTopic, "name"); got != "local.otel_alerts_topic_name" {
		t.Errorf("google_pubsub_topic.otel_alerts sets name = %q, want local.otel_alerts_topic_name", got)
	}
	if !strings.Contains(alertsTopic, `!can(regex("^(?i)goog", local.otel_alerts_topic_name))`) {
		t.Errorf("google_pubsub_topic.otel_alerts missing Pub/Sub topic name precondition:\n%s", alertsTopic)
	}

	reservation := terraformBlock(mainTF, `resource "google_bigquery_reservation" "continuous_queries"`)
	if got := terraformAttribute(reservation, "name"); got != "local.continuous_query_reservation_name" {
		t.Errorf("continuous_queries reservation sets name = %q, want local.continuous_query_reservation_name", got)
	}
	if got := terraformAttribute(reservation, "edition"); got != "var.continuous_query_edition" {
		t.Errorf("continuous_queries reservation sets edition = %q, want var.continuous_query_edition", got)
	}
	if got := terraformAttribute(reservation, "slot_capacity"); got != "var.continuous_query_slot_capacity" {
		t.Errorf("continuous_queries reservation sets slot_capacity = %q, want var.continuous_query_slot_capacity", got)
	}
	if !strings.Contains(reservation, "condition     = !var.enable_column_level_security") {
		t.Errorf("continuous_queries reservation missing !var.enable_column_level_security precondition:\n%s", reservation)
	}
	if !strings.Contains(reservation, `can(regex("^[a-z][a-z0-9-]{0,62}[a-z0-9]$", local.continuous_query_reservation_name))`) {
		t.Errorf("continuous_queries reservation missing reservation name precondition:\n%s", reservation)
	}

	assignment := terraformBlock(mainTF, `resource "google_bigquery_reservation_assignment" "continuous_queries"`)
	if got := terraformAttribute(assignment, "job_type"); got != `"CONTINUOUS"` {
		t.Errorf("continuous_queries reservation assignment sets job_type = %q, want \"CONTINUOUS\"", got)
	}

	wantRoles := map[string]string{
		`resource "google_bigquery_dataset_iam_member" "continuous_queries_viewer"`:      `"roles/bigquery.dataViewer"`,
		`resource "google_bigquery_table_iam_member" "continuous_queries_alerts_writer"`: `"roles/bigquery.dataEditor"`,
		`resource "google_pubsub_topic_iam_member" "continuous_queries_publisher"`:       `"roles/pubsub.publisher"`,
		`resource "google_pubsub_topic_iam_member" "continuous_queries_pubsub_viewer"`:   `"roles/pubsub.viewer"`,
		`resource "google_project_iam_member" "continuous_queries_jobs"`:                 `"roles/bigquery.jobUser"`,
	}
	for header, wantRole := range wantRoles {
		if got := terraformAttribute(terraformBlock(mainTF, header), "role"); got != wantRole {
			t.Errorf("%s sets role = %q, want %s", header, got, wantRole)
		}
	}

	outputs := readRepoText(t, "deploy/terraform/outputs.tf")
	for _, outName := range []string{
		"otel_alerts_table_id",
		"otel_alerts_pubsub_topic_id",
		"continuous_queries_service_account_email",
		"continuous_queries_rendered_sql",
		"continuous_queries_bq_cli_commands",
	} {
		block := terraformBlock(outputs, `output "`+outName+`"`)
		if block == "" {
			t.Errorf("outputs.tf is missing %s", outName)
			continue
		}
		if !strings.Contains(block, gate+" ?") || !strings.Contains(block, ": null") {
			t.Errorf("outputs.tf %s is not gated on %s with null fallback:\n%s", outName, gate, block)
		}
	}
	renderedBlock := terraformBlock(outputs, `output "continuous_queries_rendered_sql"`)
	for _, name := range []string{"tool_error_rate_alert", "token_budget_burn", "agent_latency_p95"} {
		if !strings.Contains(renderedBlock, `"`+name+`"`) {
			t.Errorf("continuous_queries_rendered_sql does not include %s", name)
		}
	}
	if !strings.Contains(renderedBlock, `templatefile("${path.module}/../continuous-queries/${name}.sql"`) {
		t.Errorf("continuous_queries_rendered_sql does not render templates with templatefile:\n%s", renderedBlock)
	}
	cliBlock := terraformBlock(outputs, `output "continuous_queries_bq_cli_commands"`)
	if !strings.Contains(cliBlock, "| bq query") || strings.Contains(cliBlock, `"$(terraform`) {
		t.Errorf("continuous_queries_bq_cli_commands must pipe SQL on stdin to bq query:\n%s", cliBlock)
	}

	deploySection := firstCapture(`(?s)\n### Real-Time Alerting \(Continuous Queries\)\n(.*?)(?:\n##+ |\z)`, readRepoText(t, "deploy/README.md"))
	if deploySection == "" {
		t.Fatal("deploy/README.md has no Real-Time Alerting (Continuous Queries) section")
	}
	for _, v := range wantVars {
		want := strings.Trim(v.def, `"`)
		if got := firstCapture("(?m)^\\| `"+v.name+"` \\| `([^`]*)` \\|", deploySection); got != want {
			t.Errorf("deploy/README.md gives %s the default %q, but variables.tf declares %q", v.name, got, want)
		}
	}
	if !strings.Contains(readRepoText(t, "README.md"), "deploy/continuous-queries/") {
		t.Error("README.md does not link deploy/continuous-queries/")
	}

	t.Run("dataset_id_derived_names", func(t *testing.T) {
		spec := parseContinuousQueryNameSpec(t, mainTF)

		resPrecondRE := firstCapture(`can\(regex\("([^"]+)",\s*local\.continuous_query_reservation_name\)\)`, reservation)
		topicPrecondRE := firstCapture(`can\(regex\("([^"]+)",\s*local\.otel_alerts_topic_name\)\)`, alertsTopic)
		topicNoGoogRE := firstCapture(`!can\(regex\("([^"]+)",\s*local\.otel_alerts_topic_name\)\)`, alertsTopic)
		if resPrecondRE == "" || topicPrecondRE == "" || topicNoGoogRE == "" {
			t.Fatalf("missing precondition regexes in main.tf (res=%q, topic=%q, nogoog=%q)", resPrecondRE, topicPrecondRE, topicNoGoogRE)
		}
		resRE := regexp.MustCompile(resPrecondRE)
		topicRE := regexp.MustCompile(topicPrecondRE)
		googRE := regexp.MustCompile(topicNoGoogRE)

		datasets := []string{
			"otel",
			"OTel_Prod",
			"otel_prod",
			"2026_otel",
			"_otel",
			"otel_",
			"123otel",
			"google_otel",
			"goog_otel",
			"GOOG_OTEL",
			"___",
			"a_b",
			"a__b",
			"otel_deadbeef",
			"otel_telemetry_production_us_central1_agents_v2",
			strings.Repeat("a", 60),
			strings.Repeat("B", 1024),
		}

		tfBin, tfErr := exec.LookPath("terraform")
		var tfDir string
		if tfErr == nil {
			tfDir = t.TempDir()
			hclFile := "variable \"dataset_id\" {\n  type = string\n}\n\nlocals {\n" + spec.rawLocals + "\n}\n"
			if err := os.WriteFile(filepath.Join(tfDir, "main.tf"), []byte(hclFile), 0o600); err != nil {
				t.Fatalf("writing temp main.tf for terraform console: %v", err)
			}
		}

		seenRes := map[string]string{}
		seenTopic := map[string]string{}
		for _, ds := range datasets {
			res, topic := spec.derive(ds)
			if !resRE.MatchString(res) || len(res) > 64 {
				t.Errorf("dataset_id %q produced invalid reservation name %q (len=%d)", ds, res, len(res))
			}
			if !topicRE.MatchString(topic) || googRE.MatchString(topic) || len(topic) > 255 {
				t.Errorf("dataset_id %q produced invalid topic name %q (len=%d)", ds, topic, len(topic))
			}
			if ds == "otel" && (res != "otel-cq-otel" || topic != "otel-alerts-otel") {
				t.Errorf("dataset_id %q = (%q, %q), want (\"otel-cq-otel\", \"otel-alerts-otel\")", ds, res, topic)
			}
			if tfDir != "" {
				cmd := exec.Command(tfBin, "-chdir="+tfDir, "console", "-var=dataset_id="+ds)
				cmd.Env = append(os.Environ(), "CHECKPOINT_DISABLE=1")
				cmd.Stdin = strings.NewReader("jsonencode([local.continuous_query_reservation_name, local.otel_alerts_topic_name])\n")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("terraform console for dataset_id %q failed: %v\n%s", ds, err, out)
				}
				var encoded string
				if err := json.Unmarshal(bytes.TrimSpace(out), &encoded); err != nil {
					t.Fatalf("unquoting terraform console output %q: %v", out, err)
				}
				var hclPair [2]string
				if err := json.Unmarshal([]byte(encoded), &hclPair); err != nil {
					t.Fatalf("unmarshaling terraform console JSON %q: %v", encoded, err)
				}
				if hclPair[0] != res || hclPair[1] != topic {
					t.Errorf("terraform console for %q = (%q, %q), parsed spec derived (%q, %q)", ds, hclPair[0], hclPair[1], res, topic)
				}
			}
			if prev, exists := seenRes[res]; exists {
				t.Errorf("reservation name collision between %q and %q: %q", prev, ds, res)
			}
			seenRes[res] = ds
			if prev, exists := seenTopic[topic]; exists {
				t.Errorf("topic name collision between %q and %q: %q", prev, ds, topic)
			}
			seenTopic[topic] = ds
		}
	})
}
