package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// Node collects server-wide state from system.metrics, system.asynchronous_metrics,
// system.disks, system.errors and system.dictionaries.
type Node struct {
	ch     *ch.Client
	logger *slog.Logger
	every  time.Duration

	info           *prometheus.GaugeVec
	uptime         prometheus.Gauge
	memoryTracking prometheus.Gauge
	memoryMerges   prometheus.Gauge
	memoryLimit    prometheus.Gauge

	runningQueries   prometheus.Gauge
	runningMerges    prometheus.Gauge
	runningMutations prometheus.Gauge
	readonlyReplicas prometheus.Gauge
	replicatedChecks prometheus.Gauge

	connections     *prometheus.GaugeVec
	backgroundTasks *prometheus.GaugeVec
	openFiles       *prometheus.GaugeVec

	diskFree       *prometheus.GaugeVec
	diskTotal      *prometheus.GaugeVec
	diskUnreserved *prometheus.GaugeVec
	diskReadonly   *prometheus.GaugeVec
	diskBroken     *prometheus.GaugeVec

	errors *prometheus.GaugeVec

	maxPartCount      prometheus.Gauge
	replicasMaxDelay  prometheus.Gauge
	osMemoryFree      prometheus.Gauge
	osMemoryTotal     prometheus.Gauge
	loadAverage       *prometheus.GaugeVec
	databases         prometheus.Gauge
	tables            prometheus.Gauge
	keeperAlive       prometheus.Gauge
	keeperOutstanding prometheus.Gauge
	mergeTreeBytes    prometheus.Gauge

	dictionaryLoaded *prometheus.GaugeVec
	dictionaryAge    *prometheus.GaugeVec
}

// NewNode registers and returns the node collector.
func NewNode(reg prometheus.Registerer, client *ch.Client, cfg config.Collector, opts Options) *Node {
	l := opts.Labels
	n := &Node{
		ch:     client,
		logger: opts.logger(),
		every:  cfg.Interval.Std(),

		info:           gauge("clickhouse_server_info", "ClickHouse server version.", l, "version"),
		uptime:         simpleGauge("clickhouse_uptime_seconds", "Server uptime in seconds.", l),
		memoryTracking: simpleGauge("clickhouse_memory_tracking_bytes", "Memory tracked by ClickHouse.", l),
		memoryMerges:   simpleGauge("clickhouse_memory_tracking_merges_bytes", "Memory tracked by merges and mutations.", l),
		memoryLimit:    simpleGauge("clickhouse_memory_limit_bytes", "Effective server memory limit (0 if unknown).", l),

		runningQueries:   simpleGauge("clickhouse_running_queries", "Queries currently running.", l),
		runningMerges:    simpleGauge("clickhouse_running_merges", "Merges currently running.", l),
		runningMutations: simpleGauge("clickhouse_running_mutations", "Mutations currently running.", l),
		readonlyReplicas: simpleGauge("clickhouse_readonly_replicas", "Replicas in read-only state.", l),
		replicatedChecks: simpleGauge("clickhouse_replicated_checks", "Replicated tables waiting for a check.", l),

		connections:     gauge("clickhouse_connections", "Open client connections by protocol.", l, "protocol"),
		backgroundTasks: gauge("clickhouse_background_tasks", "Background pool tasks by pool.", l, "pool"),
		openFiles:       gauge("clickhouse_open_files", "Open files by direction.", l, "direction"),

		diskFree:       gauge("clickhouse_disk_free_bytes", "Free disk space.", l, "disk"),
		diskTotal:      gauge("clickhouse_disk_total_bytes", "Total disk space.", l, "disk"),
		diskUnreserved: gauge("clickhouse_disk_unreserved_bytes", "Unreserved disk space.", l, "disk"),
		diskReadonly:   gauge("clickhouse_disk_readonly", "Disk is read-only (1/0).", l, "disk"),
		diskBroken:     gauge("clickhouse_disk_broken", "Disk is broken (1/0).", l, "disk"),

		errors: gauge("clickhouse_error_events", "Errors since server start by code.", l, "code", "name"),

		maxPartCount:      simpleGauge("clickhouse_max_part_count_for_partition", "Max active parts in a partition.", l),
		replicasMaxDelay:  simpleGauge("clickhouse_replicas_max_absolute_delay_seconds", "Max replication delay across tables.", l),
		osMemoryFree:      simpleGauge("clickhouse_os_memory_free_bytes", "OS memory available without cached pages.", l),
		osMemoryTotal:     simpleGauge("clickhouse_os_memory_total_bytes", "OS total memory.", l),
		loadAverage:       gauge("clickhouse_load_average", "OS load average.", l, "window"),
		databases:         simpleGauge("clickhouse_databases", "Number of databases.", l),
		tables:            simpleGauge("clickhouse_tables", "Number of tables.", l),
		keeperAlive:       simpleGauge("clickhouse_keeper_alive_connections", "Alive Keeper connections.", l),
		keeperOutstanding: simpleGauge("clickhouse_keeper_outstanding_requests", "Outstanding Keeper requests.", l),
		mergeTreeBytes:    simpleGauge("clickhouse_merge_tree_bytes", "Total bytes of MergeTree tables.", l),

		dictionaryLoaded: gauge("clickhouse_dictionary_loaded", "Dictionary is loaded (1/0).", l, "database", "name"),
		dictionaryAge:    gauge("clickhouse_dictionary_last_success_age_seconds", "Seconds since the last successful dictionary load.", l, "database", "name"),
	}

	reg.MustRegister(
		n.info, n.uptime, n.memoryTracking, n.memoryMerges, n.memoryLimit,
		n.runningQueries, n.runningMerges, n.runningMutations,
		n.readonlyReplicas, n.replicatedChecks,
		n.connections, n.backgroundTasks, n.openFiles,
		n.diskFree, n.diskTotal, n.diskUnreserved, n.diskReadonly, n.diskBroken,
		n.errors,
		n.maxPartCount, n.replicasMaxDelay, n.osMemoryFree, n.osMemoryTotal,
		n.loadAverage, n.databases, n.tables, n.keeperAlive, n.keeperOutstanding,
		n.mergeTreeBytes, n.dictionaryLoaded, n.dictionaryAge,
	)
	return n
}

func (n *Node) Name() string            { return "node" }
func (n *Node) Interval() time.Duration { return n.every }

func (n *Node) Refresh(ctx context.Context) (int, error) {
	var errs []error
	series := 0

	var info []struct {
		Uptime  uint64 `ch:"uptime"`
		Version string `ch:"version"`
	}
	if err := n.ch.Select(ctx, &info, "SELECT toUInt64(uptime()) AS uptime, version() AS version"); err != nil {
		errs = append(errs, fmt.Errorf("server info: %w", err))
	} else if len(info) > 0 {
		n.uptime.Set(float64(info[0].Uptime))
		n.info.Reset()
		n.info.WithLabelValues(info[0].Version).Set(1)
		series += 2
	}

	var metrics []struct {
		Metric string `ch:"metric"`
		Value  int64  `ch:"value"`
	}
	if err := n.ch.Select(ctx, &metrics, "SELECT metric, value FROM system.metrics"); err != nil {
		errs = append(errs, fmt.Errorf("system.metrics: %w", err))
	} else {
		values := make(map[string]float64, len(metrics))
		for _, m := range metrics {
			values[m.Metric] = float64(m.Value)
		}
		setIf := func(g prometheus.Gauge, name string) {
			if v, ok := values[name]; ok {
				g.Set(v)
				series++
			}
		}
		setIf(n.memoryTracking, "MemoryTracking")
		setIf(n.memoryMerges, "MemoryTrackingForMerges")
		setIf(n.runningQueries, "Query")
		setIf(n.runningMerges, "Merge")
		setIf(n.runningMutations, "PartMutation")
		setIf(n.readonlyReplicas, "ReadonlyReplica")
		setIf(n.replicatedChecks, "ReplicatedChecks")

		n.connections.Reset()
		for metric, protocol := range map[string]string{
			"TCPConnection":         "tcp",
			"HTTPConnection":        "http",
			"InterserverConnection": "interserver",
			"MySQLConnection":       "mysql",
			"PostgreSQLConnection":  "postgresql",
		} {
			if v, ok := values[metric]; ok {
				n.connections.WithLabelValues(protocol).Set(v)
				series++
			}
		}

		n.backgroundTasks.Reset()
		for metric, pool := range map[string]string{
			"BackgroundPoolTask":                      "pool",
			"BackgroundCommonPoolTask":                "common",
			"BackgroundSchedulePoolTask":              "schedule",
			"BackgroundFetchesPoolTask":               "fetches",
			"BackgroundMovePoolTask":                  "move",
			"BackgroundMergesAndMutationsPoolTask":    "merges_mutations",
			"BackgroundMessageBrokerSchedulePoolTask": "message_broker",
			"BackgroundDistributedSchedulePoolTask":   "distributed",
			"BackgroundBufferFlushSchedulePoolTask":   "buffer_flush",
			"BackgroundTTLDeleteSchedulePoolTask":     "ttl_delete",
		} {
			if v, ok := values[metric]; ok {
				n.backgroundTasks.WithLabelValues(pool).Set(v)
				series++
			}
		}

		n.openFiles.Reset()
		if v, ok := values["OpenFileForRead"]; ok {
			n.openFiles.WithLabelValues("read").Set(v)
			series++
		}
		if v, ok := values["OpenFileForWrite"]; ok {
			n.openFiles.WithLabelValues("write").Set(v)
			series++
		}
	}

	var async []asyncMetric
	if err := n.ch.Select(ctx, &async, "SELECT metric, value FROM system.asynchronous_metrics"); err != nil {
		errs = append(errs, fmt.Errorf("system.asynchronous_metrics: %w", err))
	} else {
		values := make(map[string]float64, len(async))
		for _, m := range async {
			values[m.Metric] = m.Value
		}
		setIf := func(g prometheus.Gauge, name string) {
			if v, ok := values[name]; ok {
				g.Set(v)
				series++
			}
		}
		setIf(n.maxPartCount, "MaxPartCountForPartition")
		setIf(n.replicasMaxDelay, "ReplicasMaxAbsoluteDelay")
		setIf(n.osMemoryFree, "OSMemoryFreeWithoutCached")
		setIf(n.osMemoryTotal, "OSMemoryTotal")
		setIf(n.databases, "NumberOfDatabases")
		setIf(n.tables, "NumberOfTables")
		setIf(n.keeperAlive, "KeeperAliveConnections")
		setIf(n.keeperOutstanding, "KeeperOutstandingRequests")
		setIf(n.mergeTreeBytes, "TotalBytesOfMergeTreeTables")

		n.loadAverage.Reset()
		for metric, window := range map[string]string{
			"LoadAverage1":  "1m",
			"LoadAverage5":  "5m",
			"LoadAverage15": "15m",
		} {
			if v, ok := values[metric]; ok {
				n.loadAverage.WithLabelValues(window).Set(v)
				series++
			}
		}
	}

	var disks []struct {
		Name       string `ch:"name"`
		FreeSpace  uint64 `ch:"free_space"`
		TotalSpace uint64 `ch:"total_space"`
		Unreserved uint64 `ch:"unreserved_space"`
		IsReadOnly uint8  `ch:"is_read_only"`
		IsBroken   uint8  `ch:"is_broken"`
	}
	if err := n.ch.Select(ctx, &disks, "SELECT name, free_space, total_space, unreserved_space, is_read_only, is_broken FROM system.disks"); err != nil {
		errs = append(errs, fmt.Errorf("system.disks: %w", err))
	} else {
		n.diskFree.Reset()
		n.diskTotal.Reset()
		n.diskUnreserved.Reset()
		n.diskReadonly.Reset()
		n.diskBroken.Reset()
		for _, d := range disks {
			n.diskFree.WithLabelValues(d.Name).Set(float64(d.FreeSpace))
			n.diskTotal.WithLabelValues(d.Name).Set(float64(d.TotalSpace))
			n.diskUnreserved.WithLabelValues(d.Name).Set(float64(d.Unreserved))
			n.diskReadonly.WithLabelValues(d.Name).Set(float64(d.IsReadOnly))
			n.diskBroken.WithLabelValues(d.Name).Set(float64(d.IsBroken))
			series += 5
		}
	}

	var errorsRows []struct {
		Name  string `ch:"name"`
		Code  int32  `ch:"code"`
		Value uint64 `ch:"value"`
	}
	if err := n.ch.Select(ctx, &errorsRows, "SELECT name, code, value FROM system.errors WHERE value > 0"); err != nil {
		errs = append(errs, fmt.Errorf("system.errors: %w", err))
	} else {
		n.errors.Reset()
		for _, e := range errorsRows {
			n.errors.WithLabelValues(strconv.FormatInt(int64(e.Code), 10), e.Name).Set(float64(e.Value))
			series++
		}
	}

	// Optional: dictionaries. Some servers have none or restrict the table.
	var dicts []struct {
		Database   string `ch:"database"`
		Name       string `ch:"name"`
		Status     string `ch:"status"`
		AgeSeconds int64  `ch:"age_seconds"`
	}
	dictQuery := `SELECT database, name, status,
		toInt64(if(last_successful_update_time > toDateTime(0), dateDiff('second', last_successful_update_time, now()), 0)) AS age_seconds
		FROM system.dictionaries`
	if err := n.ch.Select(ctx, &dicts, dictQuery); err != nil {
		n.logger.Debug("dictionaries unavailable", "error", err)
	} else {
		n.dictionaryLoaded.Reset()
		n.dictionaryAge.Reset()
		for _, d := range dicts {
			loaded := 0.0
			if strings.EqualFold(d.Status, "LOADED") {
				loaded = 1
			}
			n.dictionaryLoaded.WithLabelValues(d.Database, d.Name).Set(loaded)
			n.dictionaryAge.WithLabelValues(d.Database, d.Name).Set(float64(d.AgeSeconds))
			series += 2
		}
	}

	// Optional: effective memory limit.
	var settings []struct {
		Name  string `ch:"name"`
		Value string `ch:"value"`
	}
	limit := 0.0
	if err := n.ch.Select(ctx, &settings, `SELECT name, value FROM system.server_settings
		WHERE name IN ('max_server_memory_usage', 'max_server_memory_usage_to_ram_ratio')`); err != nil {
		n.logger.Debug("server settings unavailable", "error", err)
	} else {
		var maxUsage, ratio float64
		for _, s := range settings {
			switch s.Name {
			case "max_server_memory_usage":
				maxUsage = parseFloat(s.Value)
			case "max_server_memory_usage_to_ram_ratio":
				ratio = parseFloat(s.Value)
			}
		}
		if maxUsage > 0 {
			limit = maxUsage
		} else if ratio > 0 {
			if v, ok := asyncValue(async, "OSMemoryTotal"); ok {
				limit = ratio * v
			}
		}
	}
	n.memoryLimit.Set(limit)
	series++

	return series, joinErrors(errs)
}

type asyncMetric struct {
	Metric string  `ch:"metric"`
	Value  float64 `ch:"value"`
}

func asyncValue(rows []asyncMetric, name string) (float64, bool) {
	for _, r := range rows {
		if r.Metric == name {
			return r.Value, true
		}
	}
	return 0, false
}

func parseFloat(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return v
}
