package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/collector"
	"clickhouse-exporter/internal/config"
	"clickhouse-exporter/internal/exporter"
	"clickhouse-exporter/internal/httpserver"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(findConfig(os.Args[1:]))
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("clickhouse-exporter", flag.ContinueOnError)
	fs.String("config", findConfig(os.Args[1:]), "path to YAML config (already loaded before flags)")
	fs.StringVar(&cfg.ListenAddress, "listen-address", cfg.ListenAddress, "address to listen on (default :9116)")
	fs.StringVar(&cfg.ClickHouse.DSN, "clickhouse.dsn", cfg.ClickHouse.DSN, "ClickHouse DSN")
	fs.StringVar(&cfg.LogLevel, "log.level", cfg.LogLevel, "log level: debug, info, warn, error")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *showVersion {
		fmt.Println(version)
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	client, err := ch.Open(ctx, cfg.ClickHouse)
	if err != nil {
		stop()
		return err
	}
	defer client.Close()

	labels := prometheus.Labels{}
	for k, v := range cfg.Labels {
		labels[k] = v
	}
	if labels["exporter"] == "" {
		labels["exporter"] = "clickhouse-exporter"
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// Every exporter metric goes through this registerer, so metric_prefix is
	// applied consistently to collectors and self-metrics.
	metricsReg := prometheus.WrapRegistererWithPrefix(cfg.MetricPrefix, reg)

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "clickhouse_exporter_build_info",
		Help:        "Exporter build information.",
		ConstLabels: labels,
	}, []string{"version", "go_version"})
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)
	metricsReg.MustRegister(buildInfo)

	windowGauges := []struct {
		name  string
		help  string
		value float64
	}{
		{"clickhouse_exporter_workload_window_seconds", "Window used for per-user workload metrics.", cfg.Workload.Window.Std().Seconds()},
		{"clickhouse_exporter_latency_window_seconds", "Window used for per-user latency quantiles.", cfg.Workload.LatencyWindow.Std().Seconds()},
		{"clickhouse_exporter_tables_window_seconds", "Window used for table write metrics.", cfg.Tables.Window.Std().Seconds()},
	}
	for _, w := range windowGauges {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: w.name, Help: w.help, ConstLabels: labels})
		g.Set(w.value)
		metricsReg.MustRegister(g)
	}

	opts := collector.Options{
		Labels:  labels,
		Logger:  logger,
		Sources: collector.NewSourceAvailability(metricsReg, labels),
	}

	// Do not attribute the exporter's own SELECTs to a workload user.
	if user := dsnUser(cfg.ClickHouse.DSN); user != "" {
		cfg.Workload.ExcludeUsers = appendUnique(cfg.Workload.ExcludeUsers, user)
	}

	var collectorsList []collector.Collector
	if cfg.Collectors.Node.Enabled {
		collectorsList = append(collectorsList, collector.NewNode(metricsReg, client, cfg.Collectors.Node, opts))
	}
	if cfg.Collectors.QueryLoad.Enabled {
		collectorsList = append(collectorsList, collector.NewQueryLoad(metricsReg, client, cfg.Collectors.QueryLoad, opts))
	}
	if cfg.Collectors.Workload.Enabled {
		collectorsList = append(collectorsList, collector.NewWorkload(metricsReg, client, cfg.Collectors.Workload, cfg.Workload, opts))
	}
	if cfg.Collectors.Tables.Enabled {
		collectorsList = append(collectorsList, collector.NewTables(metricsReg, client, cfg.Collectors.Tables, cfg.Tables, opts))
	}
	if cfg.Collectors.Replication.Enabled {
		collectorsList = append(collectorsList, collector.NewReplication(metricsReg, client, cfg.Collectors.Replication, cfg.Replication, opts))
	}
	if len(collectorsList) == 0 {
		logger.Warn("all collectors are disabled")
	}

	manager := exporter.NewManager(metricsReg, collectorsList, logger, labels)
	managerDone := make(chan struct{})
	go func() {
		manager.Run(ctx)
		close(managerDone)
	}()

	srv := httpserver.New(cfg.ListenAddress, reg, manager.Ready, logger)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	logger.Info("clickhouse-exporter started",
		"version", version,
		"listen", cfg.ListenAddress,
		"clickhouse", redactDSN(cfg.ClickHouse.DSN),
		"collectors", collectorNames(collectorsList),
	)

	select {
	case err := <-serveErr:
		stop()
		select {
		case <-managerDone:
		case <-time.After(5 * time.Second):
			logger.Warn("collectors did not stop in time")
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			cancel()
			return err
		}
		cancel()
	}

	select {
	case <-managerDone:
	case <-time.After(5 * time.Second):
		logger.Warn("collectors did not stop in time")
	}
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

// findConfig extracts --config before flags are parsed, because flags are bound
// to values loaded from that file.
func findConfig(args []string) string {
	for i, arg := range args {
		for _, prefix := range []string{"--config=", "-config="} {
			if strings.HasPrefix(arg, prefix) {
				return strings.TrimPrefix(arg, prefix)
			}
		}
		if (arg == "--config" || arg == "-config") && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
		}
	}
	q := u.Query()
	for _, key := range []string{"password", "token", "access_token", "secret"} {
		if q.Has(key) {
			q.Set(key, "xxxxx")
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func collectorNames(cs []collector.Collector) []string {
	names := make([]string, len(cs))
	for i, c := range cs {
		names[i] = c.Name()
	}
	return names
}

func dsnUser(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return ""
	}
	return u.User.Username()
}

func appendUnique(values []string, v string) []string {
	for _, existing := range values {
		if existing == v {
			return values
		}
	}
	return append(values, v)
}
