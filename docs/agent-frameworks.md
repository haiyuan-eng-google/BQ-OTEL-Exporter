# Agent framework OTLP setup

This guide records which Python agent frameworks can send useful OTLP traces
and logs to this exporter without application source edits. It is deliberately
not a blanket “zero-code” claim: activation, transport, signals, content
defaults, and semantic conventions differ materially by framework.

The matrix was source-reviewed and then executed on 2026-08-12. Claude Code,
ADK, and LangGraph used real framework/model paths; OpenAI, CrewAI, Bedrock,
and Microsoft Agent Framework used local model-protocol mocks, which verifies
instrumentation, transport, content controls, and payload shape but not the
vendor model API. The complete evidence and exact scenarios are in
[issue #1](https://github.com/haiyuan-eng-google/BQ-OTEL-Exporter/issues/1#issuecomment-5277226467).

The versions below are verification pins, not an evergreen compatibility
promise. Re-run the matrix before changing them: instrumentation releases can
silently change signal placement, semantic-convention keys, or content
capture.

## Tiers

- **T1** — automatic activation once required dependencies exist: environment
  variables and an unmodified framework command.
- **T2** — instrumentation/exporter dependencies plus the
  `opentelemetry-instrument` launch wrapper; no source edit.
- **T3** — application source change required, usually one setup call.
- **T4** — no supported OTLP path for that uninstrumented SDK/API.

## Compatibility matrix

| Framework (verified version) | Tier | Collector transport | Signals that matter here | Content default and notable behavior |
| --- | --- | --- | --- | --- |
| Claude Code / Agent SDK 2.1.229 | T1; traces are beta behind a second flag | gRPC 4317 and HTTP 4318 | Spans plus log events; token counts are on spans and cost is in log events | Content off. Raw `anthropic` emits nothing by itself. |
| Google ADK 2.6.3 | T1 for `adk web` / `api_server`; T3 for programmatic `Runner`; no built-in `adk run` path | HTTP 4318 only | Spans and `gen_ai.*.message` log events | Content on; explicitly disable it for privacy. The HTTP exporter is not a base dependency. |
| LangGraph 1.2.x / LangSmith 0.10.18 | T1 in LangSmith OTEL-only mode | HTTP 4318 only; endpoint must include `/v1/traces` | Spans only | Content on; hide inputs and outputs. No LangSmith API key is needed in OTEL-only mode. |
| OpenAI SDK 2.54.0 / genai-openai 1.0b0 | T2; bare `openai` is T4 | gRPC or HTTP | Chat spans and content log events | Content off. The opt-in is an enum, not a boolean. |
| OpenAI Agents SDK 0.20.0 / bridge 1.0b0 | T2 with dual-export caveat; OTLP-only is T3 | gRPC or HTTP | Workflow/agent spans; model spans with OpenAI co-instrumentation | The wrapper also exported to OpenAI’s backend in the live check. Disabling that egress requires one code call. |
| CrewAI 1.15.15 / OpenInference 1.1.12 | T2 | gRPC or HTTP | OpenInference chain, agent, and LLM spans | Content on. `OPENINFERENCE_HIDE_INPUTS` did not redact the `crew_tasks` task-description attribute. Built-in CrewAI telemetry is separate and not your OTLP route. |
| AWS Bedrock / botocore instrumentation 0.65b0 | T2; Strands is T3; classic Bedrock `InvokeAgent` is T4 | gRPC or HTTP | Model span plus `gen_ai.*.message` log events | Content off. Converse is the best-covered API; classic Bedrock Agent traces are proprietary JSON. |
| Microsoft Agent Framework 1.11.0–1.13.0 | T2 wrapper or T3 `configure_otel_providers()`; environment variables alone do not activate export | gRPC or HTTP | Agent/model spans and logs | Content off. The T3 helper ignored `OTEL_METRICS_EXPORTER=none` in the verified version; use per-signal endpoints. |

The Collector example enables both OTLP receivers and both supported pipelines.
Keep the logs pipeline enabled: Bedrock/OpenAI/ADK content events and Claude
Code cost events do not all live on spans.

## Privacy baseline

Prompts, completions, tool arguments, task descriptions, user identity, and
cost data can become BigQuery rows. Start from content-off settings below,
restrict dataset access, and opt into content only after reviewing retention
and deletion policy.

Do not treat framework redaction flags as a universal guarantee. The executed
CrewAI case leaked task descriptions in `crew_tasks` even with input hiding
enabled. If you need a stronger boundary, add a Collector redaction/transform
processor and test the serialized OTLP payload before enabling the BigQuery
exporter.

The optional file exporter in the example is diagnostic only. It can contain
the complete OTLP payload. Its default `append: false` truncates the file on
collector restart; `append: true` preserves earlier runs but cannot be combined
with its rotation feature.

## Setup recipes

All commands assume the example Collector is listening locally. Use either
gRPC `http://localhost:4317` or HTTP/protobuf endpoints beneath
`http://localhost:4318` as specified for each framework.

### Claude Code / Agent SDK (T1)

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_EXPORT_INTERVAL=1000
export OTEL_LOGS_EXPORT_INTERVAL=1000
claude
```

Prompt and tool content remain redacted unless `OTEL_LOG_USER_PROMPTS=1` or
`OTEL_LOG_TOOL_DETAILS=1` is set. Claude’s event identity is the `event.name`
log attribute (for example `api_request` for the `claude_code.api_request`
cost event). The exporter copies it into `otel_logs.event_name` when a record
has no OTLP event name. These events carry no `gen_ai.*` attribute, so
`v_gen_ai_logs` does not select them: query cost from `otel_logs_dedup`, for
example `WHERE event_name = 'api_request'`.

Official reference: [Claude Code monitoring](https://code.claude.com/docs/en/monitoring-usage).

### Google ADK serve modes (T1)

```bash
pip install google-adk==2.6.3 opentelemetry-exporter-otlp-proto-http

export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://localhost:4318/v1/logs
export ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS=false
adk web path/to/agents_dir
# or: adk api_server
```

Do not point this path at 4317; the execution check produced export errors and
no arrivals. A programmatic `Runner` needs one setup call:

```python
from google.adk.telemetry.setup import maybe_set_otel_providers

maybe_set_otel_providers()
```

Official reference: [ADK OTLP logging](https://adk.dev/observability/logging/).

### LangGraph through LangSmith OTEL-only mode (T1)

```bash
pip install 'langsmith[otel]==0.10.18'

export LANGSMITH_TRACING=true
export LANGSMITH_TRACING_MODE=otel
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
export LANGSMITH_HIDE_INPUTS=true
export LANGSMITH_HIDE_OUTPUTS=true
python app.py
```

The endpoint is used verbatim: omitting `/v1/traces` returned HTTP 404 in the
execution pass. Constructor data may still appear under
`langsmith.metadata.*`, so inspect representative payloads even with input and
output hiding enabled.

Official reference: [LangSmith tracing modes](https://docs.langchain.com/langsmith/log-traces-to-project).

### OpenAI SDK (T2)

```bash
pip install openai \
  opentelemetry-distro \
  opentelemetry-exporter-otlp-proto-grpc \
  opentelemetry-instrumentation-genai-openai==1.0b0

export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
opentelemetry-instrument python main.py
```

Content is off by default. To enable it, use one of `span_only`, `event_only`,
`span_and_event`, or `no_content` (case-insensitive):

```bash
export OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=span_and_event
```

The older boolean value `true` is invalid for this verified experimental path;
it warns and falls back to no content.

Official reference: [OpenAI instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation-genai/openai.html).

#### OpenAI Agents SDK egress caveat

Add `openai-agents` and
`opentelemetry-instrumentation-genai-openai-agents==1.0b0` to the T2 install.
The launch wrapper was observed exporting to both OTLP and OpenAI’s hosted
backend. OTLP-only operation requires a source edit:

```python
from opentelemetry.instrumentation.openai_agents import OpenAIAgentsInstrumentor

OpenAIAgentsInstrumentor().instrument(disable_openai_trace_export=True)
```

### CrewAI through OpenInference (T2)

```bash
pip install crewai==1.15.15 \
  openinference-instrumentation-crewai==1.1.12 \
  openinference-instrumentation-openai \
  opentelemetry-distro \
  opentelemetry-exporter-otlp

export CREWAI_DISABLE_TELEMETRY=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=none
export OTEL_METRICS_EXPORTER=none
export OPENINFERENCE_HIDE_INPUTS=true
export OPENINFERENCE_HIDE_OUTPUTS=true
opentelemetry-instrument python main.py
```

Use the OpenAI companion instrumentor for CrewAI’s OpenAI provider. The
LiteLLM companion alone produced no LLM spans in the verified setup. Pin
CrewAI: resolving 1.9.3 satisfied a loose install but fell below the
instrumentor’s supported floor and silently no-op’d. CrewAI 1.10+ also lacked
the required wheels on the Intel macOS test host, so the verified run used a
Linux container.

Official reference: [OpenInference instrumentations](https://github.com/Arize-ai/openinference#instrumentation).

### AWS Bedrock through botocore (T2)

```bash
pip install boto3 \
  opentelemetry-distro \
  opentelemetry-exporter-otlp \
  opentelemetry-instrumentation-botocore==0.65b0

export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
opentelemetry-instrument python app.py
```

Prompt/completion content is off by default and, when enabled, arrives as
`gen_ai.*.message` log events. Keep the logs pipeline. AgentCore can redirect
an already-instrumented agent with environment variables; Strands needs a code
setup call. Classic Bedrock Agents `InvokeAgent` emits proprietary trace JSON,
not OTLP.

Official reference: [botocore instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/_modules/opentelemetry/instrumentation/botocore.html).

### Microsoft Agent Framework (T2 or T3)

Zero-source-edit wrapper:

```bash
pip install agent-framework \
  opentelemetry-distro \
  opentelemetry-exporter-otlp-proto-grpc

export OTEL_SERVICE_NAME=my-agent-app
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_METRICS_EXPORTER=none
opentelemetry-instrument python app.py
```

Environment variables alone do not activate export. The programmatic option is
one line, called once during startup:

```python
from agent_framework.observability import configure_otel_providers

configure_otel_providers()
```

In the verified versions, that helper still configured metrics despite
`OTEL_METRICS_EXPORTER=none`. Avoid a failing metrics exporter by omitting the
generic endpoint and setting only the signals this component accepts:

```bash
unset OTEL_EXPORTER_OTLP_ENDPOINT
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://localhost:4317
```

Content remains off unless `ENABLE_SENSITIVE_DATA=true` is set.

Official reference: [Microsoft Agent Framework observability](https://learn.microsoft.com/en-us/python/api/agent-framework-core/agent_framework.observability?view=agent-framework-python-latest).

## Querying mixed instrumentation

Do not assume all producers use the same semantic-convention generation.
Depending on the instrumentor, provider identity can be `gen_ai.system` or
`gen_ai.provider.name`, and session identity can arrive under
`gen_ai.conversation.id` or `gcp.vertex.agent.session_id`; OpenInference spans
may contain only `llm.*` and `openinference.*`.

In `v0alpha2`, attribute bags (`attributes`, `resource.attributes`,
`instrumentation_scope.attributes`, `events.attributes`, `links.attributes`)
and `otel_logs.body` are stored as plain JSON alongside an `attributes_schema`
JSON sidecar that records the original OTLP type of each key. A string
attribute is therefore a plain JSON string (`"openai"`), queried directly via
`JSON_VALUE(attributes, '$."gen_ai.system"')`, while numeric attributes such as
token counts can be read with `LAX_INT64(attributes['gen_ai.usage.input_tokens'])`.

When `auto_create.companion_views` is enabled (or when you apply the companion
view DDL in [`internal/schema`](../internal/schema/schema.go)), `v_gen_ai_spans`
and `v_gen_ai_logs` coalesce the common `gen_ai.*` and `gcp.vertex.agent.*`
attribute variants into typed top-level columns over `otel_spans_dedup` and
`otel_logs_dedup` while keeping the raw transport tables schema-pure.
`v_gen_ai_metrics` does the same for the `gen_ai.*` metric points over
`otel_metrics_dedup`. For reasoning tokens, `v_gen_ai_spans` reads
`gen_ai.usage.reasoning.output_tokens` first and the earlier
`gen_ai.usage.reasoning_tokens` spelling second. Against OpenTelemetry Semantic Conventions `1.40.0+1.41.0+genai-dev` (`1.40.0`
GenAI spans, logs, client metrics, `gen_ai.agent.version`, and MCP conventions;
`1.41.0` `gen_ai.response.time_to_first_chunk` and
`gen_ai.usage.reasoning.output_tokens`; and `genai-dev` `gen_ai.main_agent.*`
and `gen_ai.client.inference.usage.*`, plus parent-agent correlation via
`gen_ai.agent.parent.id`), `v_gen_ai_spans` also
promotes `gen_ai.main_agent.{id,name,description}` and `gen_ai.agent.version`
(coalescing span `attributes` first and `resource.attributes` second),
`gen_ai.response.id`, and `gen_ai.response.time_to_first_chunk` (and admits
spans carrying `gen_ai.main_agent.name` or `gen_ai.main_agent.id` on span
`attributes`, without admitting unrelated spans that only inherit
`resource.attributes`); `v_gen_ai_logs` promotes `error_type`
from the log record's own `error.type` attribute; `v_gen_ai_metrics` promotes
`gen_ai_token_modality`, `gen_ai_agent_name` (from `gen_ai.agent.name` then
`gen_ai.main_agent.name` on point `attributes`, then `gen_ai.main_agent.name` on
`resource.attributes`), and `gen_ai_inference_usage_dimension` (`input`,
`output`, `cache_read_input`, `cache_write_input`, or `reasoning_output` for
`gen_ai.client.inference.usage.*` metrics, falling back to `gen_ai.token.type`);
and `v_gen_ai_model_costs`, `v_gen_ai_sessions`, and `v_gen_ai_tool_usage` expose
`typed_error_count` and `top_error_types` (top 5 non-null `error_type` entries via `APPROX_TOP_COUNT(error_type, 6)` in the aggregating CTE and `UNNEST(raw_top_error_types)` filtered to `x.value IS NOT NULL ORDER BY x.count DESC, x.value ASC LIMIT 5` in the outer `SELECT * EXCEPT (raw_top_error_types)`).

Some frameworks report the same tokens on an agent span and again on the model
call spans beneath it. `v_gen_ai_model_costs` and `v_gen_ai_sessions` leave out
a span that has a direct child reporting tokens, so each model call is counted
once; the [README](../README.md) states the rule and its limits.

## Synthetic scenario fixtures

Beyond the single-framework captures in `testdata/otlp/`, the manifest lists
synthetic multi-span scenario fixtures (`kind: scenario`, `provenance: synthetic`)
that exercise delegation hierarchies, parallel tool calls, orphan spans, error
types, and Model Context Protocol (MCP) telemetry against OpenTelemetry
Semantic Conventions `1.40.0+1.41.0+genai-dev` (`1.40.0` GenAI baseline,
`gen_ai.agent.version`, and `mcp.*`; `1.41.0` `gen_ai.response.time_to_first_chunk`
and `gen_ai.usage.reasoning.output_tokens`; and `genai-dev` `gen_ai.main_agent.*`
and `gen_ai.client.inference.usage.*`, plus parent-agent correlation via
`gen_ai.agent.parent.id`).

| Scenario ID | Semconv target | Signals | Fixture files | Coverage |
| --- | --- | --- | --- | --- |
| `multi_agent` | `1.40.0+1.41.0+genai-dev` | Traces, logs, metrics | `testdata/otlp/multi_agent/{traces,logs,metrics}.json` | Two-level `invoke_agent` delegation in a three-level span tree (`planner` -> `researcher`), `gen_ai.main_agent.*` on `resource.attributes`, `gen_ai.agent.parent.id`, parallel `execute_tool` spans, failed `chat` and tool spans with `error.type`, orphan child span, both reasoning-token spellings, legacy `gen_ai.client.token.usage` + `gen_ai.client.operation.duration` delta histograms, and `gen_ai.client.inference.usage.*` monotonic delta `Sum` counters with span exemplars. |
| `mcp` | `1.40.0+1.41.0+genai-dev` | Traces | `testdata/otlp/mcp/traces.json` | MCP session lifecycle (`initialize`, `tools/list`, `tools/call`, `resources/read`), dual-tagged GenAI + MCP tool call vs. pure MCP protocol spans, `SERVER` span kind, and failed `tools/call` carrying `gen_ai.tool.name` + JSON-RPC `error.type = "-32603"`. |
