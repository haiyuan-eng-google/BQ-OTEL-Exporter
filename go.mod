module github.com/haiyuan-eng-google/BQ-OTEL-Exporter

go 1.26.5

toolchain go1.27.3

require (
	cloud.google.com/go/auth v0.24.1-0.20261001053825-dbc26066f70a
	cloud.google.com/go/bigquery v1.85.0
	github.com/googleapis/gax-go/v2 v2.27.0
	go.opentelemetry.io/collector/component v1.69.0
	go.opentelemetry.io/collector/component/componentstatus v0.163.0
	go.opentelemetry.io/collector/component/componenttest v0.163.0
	go.opentelemetry.io/collector/config/configopaque v1.69.0
	go.opentelemetry.io/collector/config/configoptional v1.69.0
	go.opentelemetry.io/collector/config/configretry v1.69.0
	go.opentelemetry.io/collector/confmap v1.69.0
	go.opentelemetry.io/collector/consumer/consumererror v0.163.0
	go.opentelemetry.io/collector/exporter v1.69.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.163.0
	go.opentelemetry.io/collector/exporter/exportertest v0.163.0
	go.opentelemetry.io/collector/pdata v1.69.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/metric v1.47.0
	go.opentelemetry.io/otel/sdk/metric v1.47.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.28.0
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/oauth2 v0.37.0
	google.golang.org/api v0.307.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261019180522-ce908d29299b
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.0 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	cloud.google.com/go/iam v1.14.0 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/cenkalti/backoff/v7 v7.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.23 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.8 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/client v1.69.0 // indirect
	go.opentelemetry.io/collector/consumer v1.69.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.163.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.163.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.163.0 // indirect
	go.opentelemetry.io/collector/extension v1.69.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.163.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.69.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.163.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.163.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.163.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.69.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.163.0 // indirect
	go.opentelemetry.io/collector/receiver v1.69.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.163.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.163.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/sdk v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20240719175910-8a7402abbf56 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/telemetry v0.0.0-20260924152758-ed294f943157 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto v0.0.0-20261012220534-4341edf3f941 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261012220534-4341edf3f941 // indirect
)
