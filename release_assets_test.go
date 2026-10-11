// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

package bigqueryexporter

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

	for _, needle := range []string{
		"anchore/sbom-action/download-syft",
		"go.opentelemetry.io/collector/cmd/builder@v0.158.0",
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
