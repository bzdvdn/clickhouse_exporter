package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// Replication collects system.replicas, the replication queue and distributed DDL.
type Replication struct {
	ch     *ch.Client
	logger *slog.Logger
	every  time.Duration
	topN   int

	readonly      *prometheus.GaugeVec
	absoluteDelay *prometheus.GaugeVec
	queueSize     *prometheus.GaugeVec
	insertsQueue  *prometheus.GaugeVec
	mergesQueue   *prometheus.GaugeVec
	futureParts   *prometheus.GaugeVec
	activeRatio   *prometheus.GaugeVec

	queueTasks  *prometheus.GaugeVec
	queueOldest *prometheus.GaugeVec

	ddlTasks  *prometheus.GaugeVec
	ddlOldest prometheus.Gauge
}

// NewReplication registers and returns the replication collector.
func NewReplication(reg prometheus.Registerer, client *ch.Client, cfg config.Collector, rcfg config.Replication, opts Options) *Replication {
	l := opts.Labels
	r := &Replication{
		ch:     client,
		logger: opts.logger(),
		every:  cfg.Interval.Std(),
		topN:   rcfg.TopN,

		readonly:      gauge("clickhouse_replica_readonly", "Replica is read-only (1/0).", l, "database", "table"),
		absoluteDelay: gauge("clickhouse_replica_absolute_delay_seconds", "Absolute replication delay.", l, "database", "table"),
		queueSize:     gauge("clickhouse_replica_queue_size", "Replication queue size.", l, "database", "table"),
		insertsQueue:  gauge("clickhouse_replica_inserts_in_queue", "Inserts in the replication queue.", l, "database", "table"),
		mergesQueue:   gauge("clickhouse_replica_merges_in_queue", "Merges in the replication queue.", l, "database", "table"),
		futureParts:   gauge("clickhouse_replica_future_parts", "Future parts waiting for replication.", l, "database", "table"),
		activeRatio:   gauge("clickhouse_replica_active_ratio", "Active replicas divided by total replicas.", l, "database", "table"),

		queueTasks:  gauge("clickhouse_replication_queue_tasks", "Tasks in the replication queue.", l, "database", "table"),
		queueOldest: gauge("clickhouse_replication_queue_oldest_seconds", "Age of the oldest replication queue task.", l, "database", "table"),

		ddlTasks:  gauge("clickhouse_ddl_queue_tasks", "Tasks in the distributed DDL queue by status.", l, "status"),
		ddlOldest: simpleGauge("clickhouse_ddl_queue_oldest_seconds", "Age of the oldest distributed DDL task.", l),
	}
	reg.MustRegister(
		r.readonly, r.absoluteDelay, r.queueSize, r.insertsQueue, r.mergesQueue,
		r.futureParts, r.activeRatio, r.queueTasks, r.queueOldest,
		r.ddlTasks, r.ddlOldest,
	)
	return r
}

func (r *Replication) Name() string            { return "replication" }
func (r *Replication) Interval() time.Duration { return r.every }

func (r *Replication) Refresh(ctx context.Context) (int, error) {
	var errs []error
	series := 0

	if n, err := r.refreshReplicas(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	if n, err := r.refreshQueue(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	if n, err := r.refreshDDL(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	return series, joinErrors(errs)
}

func (r *Replication) refreshReplicas(ctx context.Context) (int, error) {
	const query = `
SELECT
    database,
    table,
    toUInt64(is_readonly)       AS is_readonly,
    toUInt64(absolute_delay)    AS absolute_delay,
    toUInt64(queue_size)        AS queue_size,
    toUInt64(inserts_in_queue)  AS inserts_in_queue,
    toUInt64(merges_in_queue)   AS merges_in_queue,
    toUInt64(future_parts)      AS future_parts,
    toUInt64(total_replicas)    AS total_replicas,
    toUInt64(active_replicas)   AS active_replicas
FROM system.replicas`

	var rows []struct {
		Database       string `ch:"database"`
		Table          string `ch:"table"`
		IsReadonly     uint64 `ch:"is_readonly"`
		AbsoluteDelay  uint64 `ch:"absolute_delay"`
		QueueSize      uint64 `ch:"queue_size"`
		InsertsInQueue uint64 `ch:"inserts_in_queue"`
		MergesInQueue  uint64 `ch:"merges_in_queue"`
		FutureParts    uint64 `ch:"future_parts"`
		TotalReplicas  uint64 `ch:"total_replicas"`
		ActiveReplicas uint64 `ch:"active_replicas"`
	}
	if err := r.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("system.replicas: %w", err)
	}

	r.readonly.Reset()
	r.absoluteDelay.Reset()
	r.queueSize.Reset()
	r.insertsQueue.Reset()
	r.mergesQueue.Reset()
	r.futureParts.Reset()
	r.activeRatio.Reset()
	for _, row := range rows {
		r.readonly.WithLabelValues(row.Database, row.Table).Set(float64(row.IsReadonly))
		r.absoluteDelay.WithLabelValues(row.Database, row.Table).Set(float64(row.AbsoluteDelay))
		r.queueSize.WithLabelValues(row.Database, row.Table).Set(float64(row.QueueSize))
		r.insertsQueue.WithLabelValues(row.Database, row.Table).Set(float64(row.InsertsInQueue))
		r.mergesQueue.WithLabelValues(row.Database, row.Table).Set(float64(row.MergesInQueue))
		r.futureParts.WithLabelValues(row.Database, row.Table).Set(float64(row.FutureParts))
		ratio := 0.0
		if row.TotalReplicas > 0 {
			ratio = float64(row.ActiveReplicas) / float64(row.TotalReplicas)
		}
		r.activeRatio.WithLabelValues(row.Database, row.Table).Set(ratio)
	}
	return len(rows) * 7, nil
}

func (r *Replication) refreshQueue(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    database,
    table,
    count()                                      AS tasks,
    toInt64(dateDiff('second', min(create_time), now())) AS oldest_seconds
FROM system.replication_queue
GROUP BY database, table
ORDER BY tasks DESC
LIMIT %d`, r.topN)

	var rows []struct {
		Database      string `ch:"database"`
		Table         string `ch:"table"`
		Tasks         uint64 `ch:"tasks"`
		OldestSeconds int64  `ch:"oldest_seconds"`
	}
	if err := r.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("system.replication_queue: %w", err)
	}
	r.queueTasks.Reset()
	r.queueOldest.Reset()
	for _, row := range rows {
		r.queueTasks.WithLabelValues(row.Database, row.Table).Set(float64(row.Tasks))
		r.queueOldest.WithLabelValues(row.Database, row.Table).Set(float64(row.OldestSeconds))
	}
	return len(rows) * 2, nil
}

func (r *Replication) refreshDDL(ctx context.Context) (int, error) {
	const query = `
SELECT
    status,
    count()                                                AS tasks,
    toInt64(dateDiff('second', min(query_create_time), now())) AS oldest_seconds
FROM system.distributed_ddl_queue
GROUP BY status
ORDER BY tasks DESC`

	var rows []struct {
		Status        string `ch:"status"`
		Tasks         uint64 `ch:"tasks"`
		OldestSeconds int64  `ch:"oldest_seconds"`
	}
	if err := r.ch.Select(ctx, &rows, query); err != nil {
		// distributed_ddl_queue requires Keeper/ZooKeeper and is optional on a
		// standalone server, so a failure here must not fail the collector.
		r.ddlTasks.Reset()
		r.ddlOldest.Set(0)
		r.logger.Debug("distributed DDL queue unavailable", "error", err)
		return 0, nil
	}
	r.ddlTasks.Reset()
	maxOldest := int64(0)
	for _, row := range rows {
		r.ddlTasks.WithLabelValues(row.Status).Set(float64(row.Tasks))
		if row.OldestSeconds > maxOldest {
			maxOldest = row.OldestSeconds
		}
	}
	r.ddlOldest.Set(float64(maxOldest))
	return len(rows) + 1, nil
}
