# ClickHouse Exporter

Prometheus exporter for ClickHouse that answers three questions:

1. **Who loads the database** — CPU, memory, read/write bytes, rows and errors per user and query kind.
2. **What exactly they load** — per-user write attribution to tables, table sizes, parts, merges, mutations.
3. **What happened in the past** — Prometheus keeps aggregates and history for every exported metric.

It is a small Go binary with background collectors. A Prometheus scrape never runs SQL: it reads metric vectors that the collectors updated on their own schedule. One slow `system.parts` query therefore cannot break the whole scrape.

## What is inside

| Collector | Default interval | Source |
|---|---|---|
| `node` | 15s | `system.metrics`, `system.asynchronous_metrics`, `system.disks`, `system.errors`, `system.dictionaries` |
| `query_load` | 15s | `system.events` |
| `workload` | 30s | `system.query_log`, `system.processes`, `system.part_log` |
| `tables` | 60s | `system.parts`, `system.detached_parts`, `system.mutations`, `system.merges`, `system.part_log` |
| `replication` | 30s | `system.replicas`, `system.replication_queue`, `system.distributed_ddl_queue` |

All metrics carry the labels from the config (`cluster`, `node`); Prometheus adds `instance` and `job`.

## Data flow

Everything goes through the exporter:

```text
exporter → Prometheus → Grafana
```

Grafana never connects to ClickHouse and does not need a ClickHouse DSN. The
exporter is the only component that holds the ClickHouse DSN, and it should use a
read-only user:

```sql
GRANT SELECT ON system.* TO exporter;
GRANT SELECT ON prod.* TO exporter; -- every monitored database
```

## Quick start

```bash
make demo
```

- Grafana: <http://localhost:13000> (`admin` / `admin`)
- Prometheus: <http://localhost:9090>
- Exporter: <http://localhost:9116/metrics>

The demo starts ClickHouse, creates an `exporter` user, generates a small `demo.events` table, and provisions one Prometheus-only dashboard with four expanded rows:

- **Workload** — who and which tables load the database.
- **Databases** — per-database size, read/write load and active users.
- **User Deep Dive** — one user (or All), by query kind.
- **Node, Tables & Replication** — parts, merges, mutations, replication, disks.

Grafana has only the Prometheus datasource; there are no SQL panels and no
ClickHouse credentials in Grafana.

Stop and remove volumes with `make down`.

## Separating from other exporters

By default every metric name is prefixed:

```text
chx_clickhouse_user_cpu_seconds
chx_clickhouse_table_disk_bytes
chx_clickhouse_exporter_collector_success
```

Change or disable it with `metric_prefix` (default `chx_`; empty string means
no prefix). If you change it, update the dashboard queries and alert rules to
match.

Every series also carries a constant label:

```text
exporter="clickhouse-exporter"
```

Override it with `labels.exporter` if you run several exporter variants.

The native ClickHouse Prometheus endpoint uses different metric names
(`ClickHouseMetrics_*`, `ClickHouseProfileEvents_*`, ...) and no `exporter`
label, so it does not mix with these panels. The provisioned dashboard has an
**Exporter** variable and filters with `exporter=~".+"` by default, which
excludes unlabeled series. The alert rules use the same matcher.

Prometheus `job` and `instance` labels still identify the scrape target; keep
one `job` per exporter deployment if you need scrape-level isolation in
addition to the `exporter` label.

## Production setup

### 1. Create a read-only user

```sql
CREATE USER exporter IDENTIFIED WITH sha256_password BY 'change-me' HOST ANY;

-- System tables: query_log, processes, metrics, events, parts, ...
GRANT SELECT ON system.* TO exporter;

-- Business tables. This is required: system.parts, system.tables,
-- system.mutations, system.merges and system.replicas are filtered by
-- table-level access. With only system.* the exporter sees system tables
-- but no production parts.
GRANT SELECT ON prod.* TO exporter;
-- GRANT SELECT ON default.* TO exporter;
-- ... repeat for every monitored database, or use GRANT SELECT ON *.*.

ALTER USER exporter SETTINGS
    readonly = 2,
    log_queries = 0,
    log_query_threads = 0,
    max_execution_time = 5,
    max_threads = 2;
```

`log_queries = 0` keeps the exporter's own queries out of `system.query_log`, so it does not pollute attribution. `readonly = 2` lets the exporter send per-query limits (`max_execution_time`, `max_threads`). The exporter also parses the username from the DSN and automatically excludes it from workload metrics.

For a cluster, create the user on every node, or use `ON CLUSTER` with an admin user.

### 2. Configure and run

```bash
cp deploy/exporter/config.example.yaml exporter.yaml
# edit dsn, labels, top_n
./clickhouse-exporter --config exporter.yaml
```

Or with Docker:

```bash
docker build -t clickhouse-exporter .
docker run -d --name clickhouse-exporter -p 9116:9116 \
  -e CLICKHOUSE_DSN='clickhouse://exporter:secret@ch-1:9000/default' \
  clickhouse-exporter
```

Endpoints:

- `/metrics` — Prometheus exposition
- `/healthz` — process is alive
- `/readyz` — every collector has succeeded recently

Flags: `--config`, `--listen-address`, `--clickhouse.dsn`, `--log.level`, `--version`.
Environment: `CLICKHOUSE_DSN`, `CLICKHOUSE_EXPORTER_LISTEN`, `CLICKHOUSE_EXPORTER_LOG_LEVEL`.

### 3. Prometheus

```yaml
scrape_configs:
  - job_name: clickhouse-exporter
    static_configs:
      - targets: ["ch-1:9116", "ch-2:9116", "ch-3:9116"]
```

Use one exporter per node. Alert rules are in [`deploy/prometheus/rules/clickhouse.yml`](deploy/prometheus/rules/clickhouse.yml).

### 4. Kubernetes / Helm

```bash
kubectl -n monitoring create secret generic clickhouse-exporter-dsn \
  --from-literal=dsn='clickhouse://exporter:secret@ch-1:9000/default'

helm upgrade --install clickhouse-exporter deploy/helm/clickhouse-exporter \
  --namespace monitoring --create-namespace \
  --set image.repository=ghcr.io/bzdvdn/clickhouse_exporter \
  --set image.tag=0.1.1 \
  --set clickhouse.existingSecret=clickhouse-exporter-dsn \
  --set config.labels.cluster=prod \
  --set config.labels.node=ch-1 \
  --set serviceMonitor.enabled=true
```

The chart creates a Deployment, Service, ConfigMap, optional Secret and
ServiceMonitor. See
[`deploy/helm/clickhouse-exporter/README.md`](deploy/helm/clickhouse-exporter/README.md).
Run one release per ClickHouse node (or one release with a remote DSN), and
restart the Deployment after rotating an external DSN Secret.

### Argo CD

The chart is a standard Helm 3 chart and works with Argo CD. A ready
`Application` is in
[`deploy/argocd/application.yaml`](deploy/argocd/application.yaml); it uses the
Git path and `clickhouse.existingSecret`.

- Create the DSN Secret before syncing. The chart deliberately fails if neither
  `clickhouse.existingSecret` nor `clickhouse.dsn` is set.
- Enable `serviceMonitor.enabled` only when the Prometheus Operator CRD is
  installed; otherwise Argo CD will fail on the unknown resource.
- For the OCI chart, register the registry and use `ghcr.io/bzdvdn/charts` with
  chart `clickhouse-exporter`:

  ```bash
  argocd repo add ghcr.io/bzdvdn/charts --type helm --enable-oci
  ```

  The published chart and image are public, so no registry credentials are
  needed.

### 5. Grafana

Provision one datasource:

- Prometheus (required)

Grafana does not need a ClickHouse datasource, a ClickHouse plugin or a
ClickHouse credential. There are two dashboard files:

- [`deploy/grafana/dashboards/clickhouse.json`](deploy/grafana/dashboards/clickhouse.json)
  — used by the compose provisioning. It has a top-level **Prometheus**
  datasource variable (`${datasource}`), so you can switch datasources from the
  dashboard.
- [`deploy/grafana/import/clickhouse.json`](deploy/grafana/import/clickhouse.json)
  — for manual import. It uses Grafana `__inputs` and `${DS_PROMETHEUS}`, so the
  import wizard asks you to select a **Prometheus** datasource and substitutes it
  into every panel.

For manual import into production use the import file. For provisioning, either
create a Prometheus datasource with UID `prometheus` or change the variable's
default value to your UID.

## Compatibility

Tested on ClickHouse 24.8 LTS. Expected to work on 23.x+, where `system.query_log`
has `query_kind`, `normalized_query_hash` and `tables`, and `system.part_log`
has `query_id`. Missing system tables or columns degrade gracefully: the
collector either skips the series or reports `chx_clickhouse_exporter_source_available=0`.

On ClickHouse Cloud some `system.*` tables and Keeper are restricted; verify
access before relying on replication and `system.query_log` panels.

## How workload attribution works

### Per user

Every refresh reads a window from `system.query_log` (default 5m) and aggregates:

- `user`, `query_kind`
- `count()`
- CPU: `ProfileEvents['OSCPUVirtualTimeMicroseconds']`
- memory: `sum(memory_usage)` — sum of per-query memory usage over the window, not current RAM
- `read_bytes`, `written_bytes`, `read_rows`, `written_rows`
- errors: `countIf(exception_code != 0)`, including `ExceptionBeforeStart` (syntax errors, quota and access errors)

Metrics are gauges over the window, not counters: re-reading `query_log` cannot inflate a counter, and late rows do not break monotonicity. In Grafana, divide by the window to get per-second values, e.g. `sum by (user)(chx_clickhouse_user_cpu_seconds) / 300` gives average CPU cores over 5 minutes. The provisioned dashboard assumes the default 5-minute window for workload and tables; change the `/ 300` divisors if you change `workload.window` or `tables.window`.

`chx_clickhouse_user_memory_bytes` is a sum over completed queries, not instantaneous memory. It can exceed the server's RAM because memory usage from different queries does not overlap in time. For current values use `chx_clickhouse_user_running_memory_bytes` (from `system.processes`) and `chx_clickhouse_memory_tracking_bytes` for the server. The dashboard has separate panels for memory pressure (sum over the window) and current memory (running queries).

### Per user and database

Two additional views answer "which database is loaded and who loads it"
without querying ClickHouse from Grafana:

- `chx_clickhouse_user_database_read_bytes_touched{user,database}` and
  `_read_queries_touched` come from `system.query_log ARRAY JOIN databases`.
  This is **approximate**: `read_bytes` belongs to the whole query, so a query
  reading three databases is counted in all three. Treat it as "hotness", not
  an exact byte split.
- `chx_clickhouse_user_database_running_queries{user,database}`,
  `_running_memory_bytes` and `_running_oldest_query_seconds` come from
  `system.processes` filtered by `current_database`. These are exact current
  values.

Per-database write load is exact through `chx_clickhouse_user_table_written_*`,
which already has `user`, `database` and `table` labels.

### Per user and table

`system.query_log.tables` is not enough for writes: for `INSERT INTO target SELECT ... FROM source` it contains both source and target, so summing `written_bytes` by `tables` double-counts. The exporter instead reads `system.part_log` (`event_type = 'NewPart'`) for the physical target table and compressed size, and joins `query_id` to `system.query_log` for the user:

```sql
SELECT if(q.user = '', 'unknown', q.user) AS user,
       p.database, p.table,
       count() AS parts,
       sum(p.rows) AS written_rows,
       sum(p.size_in_bytes) AS written_bytes
FROM system.part_log AS p
ANY LEFT JOIN (
    SELECT query_id, user
    FROM system.query_log
    WHERE type IN (2, 4) AND is_initial_query
) AS q ON p.query_id = q.query_id
WHERE p.event_type = 'NewPart'
GROUP BY user, database, table;
```

The part_log window ends a few seconds in the past because `query_log` is flushed asynchronously. Inserts that cannot be matched (for example some async inserts) are reported as `user="unknown"` rather than hidden.

### Query text and fingerprints

Prometheus labels must stay bounded, so query text, query hashes and per-query
detail are intentionally not exported. The dashboard is Prometheus-only and has
no SQL panels. If you need per-query detail, query `system.query_log` directly
with a read-only user or wait for the exporter JSON API; Grafana does not get a
ClickHouse DSN either way.

The exporter never exports query text, exception messages, client hostnames or IP addresses.

## Metric reference

All names below use the default `chx_` prefix. With `metric_prefix: ""` they are
exposed without it. The exporter also adds `exporter="clickhouse-exporter"` to
every series.

### Node

| Metric | Labels |
|---|---|
| `chx_clickhouse_server_info` | `version` |
| `chx_clickhouse_uptime_seconds`, `chx_clickhouse_memory_tracking_bytes`, `chx_clickhouse_memory_limit_bytes` | |
| `chx_clickhouse_running_queries`, `chx_clickhouse_running_merges`, `chx_clickhouse_running_mutations` | |
| `chx_clickhouse_connections` | `protocol` |
| `chx_clickhouse_background_tasks` | `pool` |
| `chx_clickhouse_open_files` | `direction` |
| `chx_clickhouse_disk_free_bytes`, `_total_bytes`, `_unreserved_bytes`, `_readonly`, `_broken` | `disk` |
| `chx_clickhouse_error_events` | `code`, `name` |
| `chx_clickhouse_max_part_count_for_partition`, `chx_clickhouse_replicas_max_absolute_delay_seconds` | |
| `chx_clickhouse_os_memory_free_bytes`, `chx_clickhouse_os_memory_total_bytes` | |
| `chx_clickhouse_load_average` | `window` |
| `chx_clickhouse_databases`, `chx_clickhouse_tables`, `chx_clickhouse_merge_tree_bytes` | |
| `chx_clickhouse_dictionary_loaded`, `chx_clickhouse_dictionary_last_success_age_seconds` | `database`, `name` |

### Global query load

| Metric | Labels |
|---|---|
| `chx_clickhouse_queries_total` | `type=all|select` |
| `chx_clickhouse_query_duration_seconds_total` | `type=all|select|other` |
| `chx_clickhouse_inserted_rows_total`, `chx_clickhouse_inserted_bytes_total` | |
| `chx_clickhouse_selected_rows_total`, `chx_clickhouse_selected_bytes_total` | |
| `chx_clickhouse_cpu_seconds_total` | |
| `chx_clickhouse_disk_read_elapsed_seconds_total`, `chx_clickhouse_disk_write_elapsed_seconds_total` | |
| `chx_clickhouse_network_receive_elapsed_seconds_total`, `chx_clickhouse_network_send_elapsed_seconds_total` | |
| `chx_clickhouse_query_memory_limit_exceeded_total` (if the event exists) | |
| `chx_clickhouse_delayed_inserts_total`, `chx_clickhouse_rejected_inserts_total` (if the events exist) | |

### Workload

| Metric | Labels |
|---|---|
| `chx_clickhouse_user_queries` | `user`, `query_kind` |
| `chx_clickhouse_user_cpu_seconds` | `user`, `query_kind` |
| `chx_clickhouse_user_memory_bytes` | `user`, `query_kind` |
| `chx_clickhouse_user_read_bytes`, `chx_clickhouse_user_written_bytes` | `user`, `query_kind` |
| `chx_clickhouse_user_read_rows`, `chx_clickhouse_user_written_rows` | `user`, `query_kind` |
| `chx_clickhouse_user_errors` | `user`, `query_kind` |
| `chx_clickhouse_user_query_duration_seconds` | `user`, `query_kind`, `percentile` |
| `chx_clickhouse_user_running_queries`, `_memory_bytes`, `_oldest_running_query_seconds` | `user` |
| `chx_clickhouse_user_database_read_bytes_touched`, `_read_queries_touched` | `user`, `database` |
| `chx_clickhouse_user_database_running_queries`, `_running_memory_bytes`, `_running_oldest_query_seconds` | `user`, `database` |
| `chx_clickhouse_user_table_parts`, `_written_rows`, `_written_bytes` | `user`, `database`, `table` |

Window sizes are exposed as `chx_clickhouse_exporter_workload_window_seconds` and `chx_clickhouse_exporter_latency_window_seconds`.

`chx_clickhouse_user_memory_bytes` is a sum over the workload window, not current RAM; `chx_clickhouse_user_running_memory_bytes` is the current value from `system.processes`.

### Tables

| Metric | Labels |
|---|---|
| `chx_clickhouse_table_parts`, `_rows`, `_disk_bytes`, `_uncompressed_bytes`, `_last_modification_seconds` | `database`, `table` |
| `chx_clickhouse_table_detached_parts` | `database`, `table` |
| `chx_clickhouse_table_mutations_pending`, `_mutation_oldest_seconds` | `database`, `table` |
| `chx_clickhouse_table_merges_running`, `_merge_progress`, `_merge_max_elapsed_seconds`, `_merge_memory_bytes` | `database`, `table` |
| `chx_clickhouse_table_new_parts`, `_written_rows`, `_written_bytes` | `database`, `table` |

Top-N is controlled by `tables.top_n` (default 200). The rest is aggregated into `database="__other__", table="__other__"`.

### Replication

| Metric | Labels |
|---|---|
| `chx_clickhouse_replica_readonly`, `_absolute_delay_seconds`, `_queue_size`, `_inserts_in_queue`, `_merges_in_queue`, `_future_parts`, `_active_ratio` | `database`, `table` |
| `chx_clickhouse_replication_queue_tasks`, `_oldest_seconds` | `database`, `table` |
| `chx_clickhouse_ddl_queue_tasks` | `status` |
| `chx_clickhouse_ddl_queue_oldest_seconds` | |

### Exporter self-monitoring

| Metric | Labels |
|---|---|
| `chx_clickhouse_exporter_collector_success` | `collector` |
| `chx_clickhouse_exporter_collector_duration_seconds` | `collector` |
| `chx_clickhouse_exporter_last_success_timestamp_seconds` | `collector` |
| `chx_clickhouse_exporter_sql_errors_total` | `collector` |
| `chx_clickhouse_exporter_series` | `collector` |
| `chx_clickhouse_exporter_source_available` | `source` |
| `chx_clickhouse_exporter_build_info` | `version`, `go_version` |

`chx_clickhouse_exporter_source_available` turns 0 when `system.query_log` or `system.part_log` is missing or disabled.

## Retrospective: what to configure

Prometheus keeps the aggregates. ClickHouse keeps the detail:

- **Prometheus retention** — 30–90 days for 15–60s resolution. For longer/cheaper retention use VictoriaMetrics or Thanos.
- **`query_log` TTL** — default is usually 30 days. Increase if you need per-query history:

  ```sql
  SHOW CREATE TABLE system.query_log;
  ALTER TABLE system.query_log MODIFY TTL event_date + INTERVAL 90 DAY;
  ```

- **`part_log` TTL** — 7–30 days is usually enough for write/merge history.
- **`flush_interval_milliseconds`** — default ~7500 ms. Lower it to 1000–2000 for fresher attribution, at the cost of more small parts.

If `query_log` retention is too short for your needs, the next step is an aggregate table (per minute, user, query kind, table) written by the exporter or a cron job with an idempotent `ReplacingMergeTree` upsert. It is not part of v1.

## Cardinality

Top-N limits are not cosmetic. A cluster with 5000 tables and 10 table-level metrics produces 50k series per node before users and query kinds. Keep:

- `tables.top_n` 200–500
- `workload.top_n` 200
- `replication.top_n` 500

Use `chx_clickhouse_exporter_series{collector="tables"}` to see the real number. A reasonable budget is under ~50k series per exporter.

## Security and privacy

- Never put `query`, `exception`, `client_hostname` or IP addresses into labels.
- Use a dedicated read-only user; grant `SELECT` only on the databases you monitor.
- Put the DSN password in an environment variable or a secret file, not in the repository.
- For TLS use `?secure=true` in the DSN.
- Grafana never connects to ClickHouse, so it does not hold a ClickHouse credential. Only the exporter has the DSN.

## Limitations

- `system.parts` and friends are filtered by table-level grants. Grant `SELECT` on monitored databases.
- `system.query_log` and `system.part_log` are created lazily and can be disabled. The exporter detects absence and exposes it via `chx_clickhouse_exporter_source_available`.
- `query_log` is flushed asynchronously; very recent queries (last seconds) may be missing.
- Distributed queries are counted once via `is_initial_query`, but every node reports its own local view. Aggregate with `sum by (cluster, user)` across instances.
- Async inserts may not join back to `query_log` by `query_id`; those rows are labelled `user="unknown"`.
- `chx_clickhouse_user_database_read_bytes_touched` is approximate: a query reading several databases contributes its full `read_bytes` to each. Use it for hotness, not exact attribution.
- Host CPU/iowait/steal and network are not ClickHouse metrics. Use `node_exporter` for the host.
- The exporter intentionally does not export query text, column-level sizes or all 500+ ProfileEvents.

## Development

```bash
make test
make vet
make lint
make vuln
make build
```

Integration check against a live server:

```bash
make integration DSN='clickhouse://exporter@127.0.0.1:9000/default'
```

The project is built and scanned with Go 1.26.11; `govulncheck` must report no
vulnerabilities before a release.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on push and pull
request:

- **Test and lint** — build, `go vet`, race tests, staticcheck, golangci-lint and
  govulncheck.
- **Integration with ClickHouse** — a ClickHouse 24.8 service, the integration
  collector test and an exporter smoke test that checks `/readyz` and the
  `chx_` metric prefix.
- **Manifests and image** — Helm lint/template, dashboard JSON validation,
  `docker compose config`, `promtool check rules` and `docker build`.

Locally the same checks are available as `make ci` (without the ClickHouse
service) and `make integration`.

## Releases

Push a semver tag:

```bash
git tag v0.1.1
git push origin v0.1.1
```

[`.github/workflows/release.yml`](.github/workflows/release.yml) publishes:

- multi-arch image `ghcr.io/bzdvdn/clickhouse_exporter:0.1.1` and `:latest`
  (`linux/amd64`, `linux/arm64`);
- Helm chart `oci://ghcr.io/bzdvdn/charts/clickhouse-exporter:0.1.1`;
- a GitHub release with the packaged chart attached.

Install the published chart:

```bash
helm install clickhouse-exporter \
  oci://ghcr.io/bzdvdn/charts/clickhouse-exporter \
  --version 0.1.1 \
  --set clickhouse.existingSecret=clickhouse-exporter-dsn \
  --set config.labels.cluster=prod
```

The release workflow rewrites the chart's default image repository to the actual
GHCR repository before packaging.

### Make the GHCR packages public

GHCR creates new packages as **private**, even when the repository is public.
Package visibility is a registry setting, not a workflow flag. After the first
release, set it once for each package:

1. Open the repository on GitHub → **Packages**.
2. Select the image package (`clickhouse_exporter`) → **Package settings**.
3. Under **Danger Zone** → **Change visibility** → **Public**.
4. Repeat for the chart package (`charts/clickhouse-exporter`).

Once public, future versions inherit the visibility. Public container packages
allow anonymous access; private packages require `imagePullSecrets` for pods and
`helm registry login` for the chart.

For an organization, an owner can also set the organization's default package
visibility under **Organization settings → Packages**, so new packages are
created public from the start.

Verify without credentials:

```bash
docker pull ghcr.io/bzdvdn/clickhouse_exporter:0.1.1
helm show chart oci://ghcr.io/bzdvdn/charts/clickhouse-exporter --version 0.1.1
```

The channel is tags only: a normal push to `main` runs CI but does not publish.

## Roadmap

- Optional long-term rollup table for >90 days of per-user/per-table history.
- Helm chart and Kubernetes ServiceMonitor/PodMonitor examples.
- Kafka consumer lag and async insert queue metrics.
- Configurable `log_comment` attribution for multi-tenant writers.
- Exporter JSON API for query-level detail, so the data stays behind the exporter.
- JSON `/api/v1/snapshot` for embedding into a custom UI.
