# Security Policy

## Supported versions

This component is **pre-alpha** and has no tagged releases yet. Security fixes
are made on the `main` branch only; there are no maintained back-branches.

## Reporting a vulnerability

Please **do not open a public issue** for a suspected vulnerability.

Report it privately through GitHub's private vulnerability reporting: open the
repository's **Security** tab and choose **Report a vulnerability**. If that
option is not available, open a public issue that says only that you have a
security report and asks for a private channel; do not include any details in
that issue.

Please include:

- what you found and the impact you expect;
- the commit or version you tested;
- the smallest configuration and input that reproduces it;
- whether it is already public.

You can expect an acknowledgement within a few working days. This is a small,
volunteer-maintained project, so fix timelines depend on severity and on how
much of the fix needs to be coordinated with upstream projects. We will keep
you informed and credit you in the fix unless you prefer otherwise.

## What is in scope

The exporter handles credentials and writes data into a customer's BigQuery
project, so the following are in scope:

- credential handling: reading or logging of credential file contents, token
  exposure, and unintended use of ambient credentials;
- any path by which telemetry content (attribute keys or values, log bodies,
  trace or span IDs) reaches the exporter's own logs or metric labels;
- the `endpoint.insecure` and `endpoint.without_authentication` safeguards,
  which must stay limited to loopback endpoints;
- generated SQL and DDL (deduplication views, search indexes, table creation),
  including identifier handling and injection;
- unbounded resource consumption from crafted OTLP input that bypasses the
  structural `limits.*` settings;
- vulnerabilities in dependencies that the exporter's code can actually reach.

## What is out of scope

- Findings that require an already-compromised Collector host, credential file
  or BigQuery project.
- Issues in the OpenTelemetry Collector, the Google Cloud client libraries or
  BigQuery itself. Please report those upstream; tell us if the exporter makes
  them worse.
- Denial of service that needs more traffic than the Collector's own receivers
  and memory limiter would accept.

## How the project checks itself

Continuous integration runs `govulncheck ./...` and `golangci-lint`, and
Dependabot proposes dependency updates weekly. These reduce risk but do not
replace a report from a person who has found something we missed.
