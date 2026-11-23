# Import-friendly dashboard

Use `clickhouse.json` from this directory when importing the dashboard manually
into another Grafana.

It declares a Prometheus datasource input (`__inputs` / `${DS_PROMETHEUS}`), so
Grafana shows a datasource picker during import and substitutes the selected UID
into every panel and variable query.

The provisioning variant lives in `../dashboards/clickhouse.json` and uses a
dashboard datasource variable instead.
