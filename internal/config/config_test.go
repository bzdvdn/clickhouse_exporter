package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultIsValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config is invalid: %v", err)
	}
	if cfg.MetricPrefix != "chx_" {
		t.Fatalf("unexpected metric prefix: %q", cfg.MetricPrefix)
	}
	if cfg.Workload.Window.Std() != 5*time.Minute {
		t.Fatalf("unexpected workload window: %s", cfg.Workload.Window.Std())
	}
}

func TestLoadOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `
listen_address: ":9999"
clickhouse:
  dsn: "clickhouse://u:p@example:9000/db"
  timeout: 3s
labels:
  cluster: prod
  node: ch-1
collectors:
  tables:
    interval: 2m
workload:
  window: 10m
  latency_window: 30m
  top_n: 50
  exclude_users: [exporter, default]
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.ListenAddress != ":9999" {
		t.Fatalf("listen address = %q", cfg.ListenAddress)
	}
	if cfg.ClickHouse.Timeout.Std() != 3*time.Second {
		t.Fatalf("timeout = %s", cfg.ClickHouse.Timeout.Std())
	}
	if cfg.Collectors.Tables.Interval.Std() != 2*time.Minute {
		t.Fatalf("tables interval = %s", cfg.Collectors.Tables.Interval.Std())
	}
	if !cfg.Collectors.Tables.Enabled {
		t.Fatalf("tables collector should stay enabled by default")
	}
	if cfg.Collectors.Node.Interval.Std() != 15*time.Second {
		t.Fatalf("node interval default lost: %s", cfg.Collectors.Node.Interval.Std())
	}
	if cfg.Workload.Window.Std() != 10*time.Minute {
		t.Fatalf("workload window = %s", cfg.Workload.Window.Std())
	}
	if got := cfg.Labels["cluster"]; got != "prod" {
		t.Fatalf("cluster label = %q", got)
	}
	if len(cfg.Workload.ExcludeUsers) != 2 {
		t.Fatalf("exclude users = %#v", cfg.Workload.ExcludeUsers)
	}
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("CLICKHOUSE_DSN", "clickhouse://env@localhost:9000/default")
	t.Setenv("CLICKHOUSE_EXPORTER_LISTEN", ":1234")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClickHouse.DSN != "clickhouse://env@localhost:9000/default" {
		t.Fatalf("dsn = %q", cfg.ClickHouse.DSN)
	}
	if cfg.ListenAddress != ":1234" {
		t.Fatalf("listen = %q", cfg.ListenAddress)
	}
}

func TestValidation(t *testing.T) {
	cfg := Default()
	cfg.Workload.LatencyWindow = Duration(time.Minute)
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected latency_window validation error")
	}

	cfg = Default()
	cfg.Collectors.Node.Interval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected interval validation error")
	}

	cfg = Default()
	cfg.ClickHouse.DSN = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected dsn validation error")
	}

	cfg = Default()
	cfg.Labels = map[string]string{"bad-name": "x"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid label name error")
	}

	cfg = Default()
	cfg.MetricPrefix = "bad-prefix-"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid metric prefix error")
	}
}
