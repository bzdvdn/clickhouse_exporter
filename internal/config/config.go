// Package config loads and validates the exporter configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from YAML strings such as "30s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config is the root configuration.
type Config struct {
	ListenAddress string            `yaml:"listen_address"`
	LogLevel      string            `yaml:"log_level"`
	MetricPrefix  string            `yaml:"metric_prefix"`
	ClickHouse    ClickHouse        `yaml:"clickhouse"`
	Labels        map[string]string `yaml:"labels"`
	Collectors    Collectors        `yaml:"collectors"`
	Workload      Workload          `yaml:"workload"`
	Tables        Tables            `yaml:"tables"`
	Replication   Replication       `yaml:"replication"`
}

// ClickHouse holds connection settings.
type ClickHouse struct {
	DSN      string         `yaml:"dsn"`
	Timeout  Duration       `yaml:"timeout"`
	Settings map[string]any `yaml:"settings"`
}

// Collector toggles and schedules one collector.
type Collector struct {
	Enabled  bool     `yaml:"enabled"`
	Interval Duration `yaml:"interval"`
}

// Collectors groups per-collector settings.
type Collectors struct {
	Node        Collector `yaml:"node"`
	QueryLoad   Collector `yaml:"query_load"`
	Workload    Collector `yaml:"workload"`
	Tables      Collector `yaml:"tables"`
	Replication Collector `yaml:"replication"`
}

// Workload configures per-user attribution.
type Workload struct {
	// Window is the time range aggregated from system.query_log on every refresh.
	Window Duration `yaml:"window"`
	// LatencyWindow is the range used for duration quantiles.
	LatencyWindow Duration `yaml:"latency_window"`
	// TopN bounds the number of user/table series.
	TopN int `yaml:"top_n"`
	// ExcludeUsers is removed from workload metrics (the exporter itself, for example).
	ExcludeUsers []string `yaml:"exclude_users"`
}

// Tables configures table-level series.
type Tables struct {
	// Window is the range aggregated from system.part_log on every refresh.
	Window           Duration `yaml:"window"`
	TopN             int      `yaml:"top_n"`
	ExcludeDatabases []string `yaml:"exclude_databases"`
	ExcludeTables    []string `yaml:"exclude_tables"`
}

// Replication configures replication series.
type Replication struct {
	TopN int `yaml:"top_n"`
}

// Default returns a configuration that is safe to run against a local server.
func Default() *Config {
	return &Config{
		ListenAddress: ":9116",
		LogLevel:      "info",
		MetricPrefix:  "chx_",
		ClickHouse: ClickHouse{
			DSN:     "clickhouse://default@127.0.0.1:9000/default",
			Timeout: Duration(5 * time.Second),
			Settings: map[string]any{
				"max_execution_time":   4,
				"max_threads":          2,
				"max_result_rows":      20000,
				"result_overflow_mode": "break",
				"log_queries":          0,
				"log_query_threads":    0,
			},
		},
		Labels: map[string]string{},
		Collectors: Collectors{
			Node:        Collector{Enabled: true, Interval: Duration(15 * time.Second)},
			QueryLoad:   Collector{Enabled: true, Interval: Duration(15 * time.Second)},
			Workload:    Collector{Enabled: true, Interval: Duration(30 * time.Second)},
			Tables:      Collector{Enabled: true, Interval: Duration(60 * time.Second)},
			Replication: Collector{Enabled: true, Interval: Duration(30 * time.Second)},
		},
		Workload: Workload{
			Window:        Duration(5 * time.Minute),
			LatencyWindow: Duration(15 * time.Minute),
			TopN:          200,
		},
		Tables: Tables{
			Window: Duration(5 * time.Minute),
			TopN:   200,
			ExcludeDatabases: []string{
				"system",
				"INFORMATION_SCHEMA",
				"information_schema",
			},
		},
		Replication: Replication{TopN: 500},
	}
}

// Load reads path (if non-empty) over the defaults and applies environment overrides.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // path is the operator-supplied --config flag
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	cfg.applyEnv()
	return cfg, nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("CLICKHOUSE_DSN"); v != "" {
		c.ClickHouse.DSN = v
	}
	if v := os.Getenv("CLICKHOUSE_EXPORTER_LISTEN"); v != "" {
		c.ListenAddress = v
	}
	if v := os.Getenv("CLICKHOUSE_EXPORTER_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
}

// Validate checks required fields and sane intervals.
func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		return fmt.Errorf("listen_address must not be empty")
	}
	if c.ClickHouse.DSN == "" {
		return fmt.Errorf("clickhouse.dsn must not be empty")
	}
	if c.ClickHouse.Timeout <= 0 {
		return fmt.Errorf("clickhouse.timeout must be positive")
	}
	if c.MetricPrefix != "" && !model.LegacyValidation.IsValidMetricName(c.MetricPrefix+"x") {
		return fmt.Errorf("metric_prefix %q is not a valid Prometheus metric name prefix", c.MetricPrefix)
	}
	for name, col := range map[string]Collector{
		"node":        c.Collectors.Node,
		"query_load":  c.Collectors.QueryLoad,
		"workload":    c.Collectors.Workload,
		"tables":      c.Collectors.Tables,
		"replication": c.Collectors.Replication,
	} {
		if col.Enabled && col.Interval <= 0 {
			return fmt.Errorf("collectors.%s.interval must be positive", name)
		}
	}
	if c.Workload.Window <= 0 {
		return fmt.Errorf("workload.window must be positive")
	}
	if c.Workload.LatencyWindow < c.Workload.Window {
		return fmt.Errorf("workload.latency_window must be >= workload.window")
	}
	if c.Workload.TopN <= 0 {
		return fmt.Errorf("workload.top_n must be positive")
	}
	if c.Tables.TopN <= 0 {
		return fmt.Errorf("tables.top_n must be positive")
	}
	if c.Tables.Window <= 0 {
		return fmt.Errorf("tables.window must be positive")
	}
	if c.Replication.TopN <= 0 {
		return fmt.Errorf("replication.top_n must be positive")
	}
	for k := range c.Labels {
		if k == "" {
			return fmt.Errorf("labels must not contain an empty key")
		}
		if !model.LegacyValidation.IsValidLabelName(k) {
			return fmt.Errorf("labels: %q is not a valid Prometheus label name", k)
		}
	}
	return nil
}
