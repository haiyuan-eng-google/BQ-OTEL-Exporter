// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"gopkg.in/yaml.v3"

	"github.com/haiyuan-eng-google/BQ-OTEL-Exporter/internal/schema"
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

func TestReleaseWorkflowInstallsPrerequisitesAndResolvesManifestDigest(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("reading .github/workflows/release.yml: %v", err)
	}
	text := string(raw)

	collectorVersion, _ := collectorVersionsFromGoMod(t, "go.mod")
	for _, needle := range []string{
		"anchore/sbom-action/download-syft",
		"go.opentelemetry.io/collector/cmd/builder@" + collectorVersion,
		"ocb version",
		"syft version",
		"id: manifest_digest",
		"docker buildx imagetools inspect",
		"steps.manifest_digest.outputs.digest",
	} {
		if !strings.Contains(text, needle) {
			t.Errorf(".github/workflows/release.yml missing %q", needle)
		}
	}
	if strings.Contains(text, "steps.build.outputs.digest") {
		t.Error(".github/workflows/release.yml must not reference nonexistent steps.build.outputs.digest")
	}

	ocbRaw, err := os.ReadFile("example/otelcol-builder-release.yaml")
	if err != nil {
		t.Fatalf("reading example/otelcol-builder-release.yaml: %v", err)
	}
	if !strings.Contains(string(ocbRaw), "replaces:") {
		t.Error("example/otelcol-builder-release.yaml must include a replaces entry for the checked-out commit")
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
// project-scope grant is the jobs.create needed for search indexes. CMEK
// uses the BigQuery default service account, not data.google_project.number.
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
		if !strings.Contains(block, "count   = var.grant_auto_create ? 1 : 0") ||
			!strings.Contains(block, "otel_collector_jobs") {
			t.Errorf("project-scope binding must be the opt-in jobs role only:\n%s", block)
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

// deploy/README.md lists every retention and partition filter variable with
// the default variables.tf declares, and says that the collector's own
// options for a table apply only to tables the collector creates, naming
// every per-signal option other than the table name.
func TestDeployReadmeDocumentsTerraformGovernance(t *testing.T) {
	readme := readRepoText(t, "deploy/README.md")
	vars := readRepoText(t, "deploy/terraform/variables.tf")
	for _, variable := range append(slices.Sorted(maps.Values(terraformExpirationVariables)), "require_partition_filter") {
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
