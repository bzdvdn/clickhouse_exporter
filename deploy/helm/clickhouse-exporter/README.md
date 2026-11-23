# clickhouse-exporter Helm chart

Deploys the exporter as a single-replica Deployment with a Service and an
optional Prometheus Operator `ServiceMonitor`.

The chart does **not** deploy ClickHouse, Prometheus or Grafana.

## Install

### Chart-managed DSN Secret

```bash
helm upgrade --install clickhouse-exporter ./deploy/helm/clickhouse-exporter \
  --namespace monitoring --create-namespace \
  --set image.repository=registry.example.com/clickhouse-exporter \
  --set image.tag=0.1.0 \
  --set clickhouse.dsn='clickhouse://exporter:secret@ch-1:9000/default' \
  --set config.labels.cluster=prod \
  --set config.labels.node=ch-1
```

### Existing Secret (recommended)

```bash
kubectl -n monitoring create secret generic clickhouse-exporter-dsn \
  --from-literal=dsn='clickhouse://exporter:secret@ch-1:9000/default'

helm upgrade --install clickhouse-exporter ./deploy/helm/clickhouse-exporter \
  --namespace monitoring --create-namespace \
  --set image.repository=registry.example.com/clickhouse-exporter \
  --set image.tag=0.1.0 \
  --set clickhouse.existingSecret=clickhouse-exporter-dsn \
  --set config.labels.cluster=prod \
  --set config.labels.node=ch-1
```

The DSN is always injected as `CLICKHOUSE_DSN` from a Secret and is never
rendered into the ConfigMap.

## One release per ClickHouse node

The exporter is stateless, so run one release per node with its own DSN and
`config.labels.node`, or run one release with a remote DSN. For ClickHouse in
Kubernetes, a sidecar or a separate Deployment next to each pod both work; the
chart itself is a Deployment.

## ServiceMonitor

```bash
helm upgrade --install clickhouse-exporter ./deploy/helm/clickhouse-exporter \
  ... \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.interval=30s
```

Requires the Prometheus Operator CRDs.

## Important values

| Value | Default | Meaning |
|---|---|---|
| `image.repository` | `ghcr.io/your-org/clickhouse-exporter` | Set to your registry |
| `image.tag` | chart appVersion | Image tag |
| `clickhouse.existingSecret` | `""` | Secret with the DSN |
| `clickhouse.secretKey` | `dsn` | Key inside that Secret |
| `clickhouse.dsn` | `""` | Used only without `existingSecret` |
| `config.labels` | `{}` | Added to every series, e.g. `cluster`, `node`; `exporter` defaults to `clickhouse-exporter` |
| `config.metric_prefix` | `chx_` | Prefix for every metric name |
| `config.workload.top_n` | `200` | Cardinality bound for users/tables |
| `config.tables.top_n` | `200` | Cardinality bound for table series |
| `serviceMonitor.enabled` | `false` | Create a ServiceMonitor |
| `resources` | 50m/64Mi requests | Exporter footprint |

The full `config` value is rendered verbatim into the ConfigMap. See
[`values.yaml`](values.yaml) for all collectors and windows.

## Upgrade and rollback

```bash
helm upgrade clickhouse-exporter ./deploy/helm/clickhouse-exporter -n monitoring
helm rollback clickhouse-exporter -n monitoring
```

The Deployment gets `checksum/config` and `checksum/secret` annotations, so a
chart-managed config or DSN change triggers a rollout. An external Secret
(`clickhouse.existingSecret`) is not checksummed: restart the Deployment after
rotating it:

```bash
kubectl -n monitoring rollout restart deployment/clickhouse-exporter
```

## Verify

```bash
kubectl -n monitoring port-forward svc/clickhouse-exporter 9116:9116
curl -s localhost:9116/readyz
curl -s localhost:9116/metrics | grep clickhouse_user
```
