package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// QueryLoad converts cumulative system.events counters into Prometheus counters.
type QueryLoad struct {
	ch      *ch.Client
	every   time.Duration
	tracker *deltaTracker

	queries         *prometheus.CounterVec
	queryDuration   *prometheus.CounterVec
	insertedRows    prometheus.Counter
	insertedBytes   prometheus.Counter
	selectedRows    prometheus.Counter
	selectedBytes   prometheus.Counter
	cpuSeconds      prometheus.Counter
	diskRead        prometheus.Counter
	diskWrite       prometheus.Counter
	networkReceive  prometheus.Counter
	networkSend     prometheus.Counter
	memoryLimitHit  prometheus.Counter
	delayedInserts  prometheus.Counter
	rejectedInserts prometheus.Counter
}

// NewQueryLoad registers and returns the global query-load collector.
func NewQueryLoad(reg prometheus.Registerer, client *ch.Client, cfg config.Collector, opts Options) *QueryLoad {
	l := opts.Labels
	q := &QueryLoad{
		ch:      client,
		every:   cfg.Interval.Std(),
		tracker: newDeltaTracker(),

		queries:         counter("clickhouse_queries_total", "Queries since server start.", l, "type"),
		queryDuration:   counter("clickhouse_query_duration_seconds_total", "Cumulative query execution time.", l, "type"),
		insertedRows:    simpleCounter("clickhouse_inserted_rows_total", "Rows inserted since server start.", l),
		insertedBytes:   simpleCounter("clickhouse_inserted_bytes_total", "Bytes inserted since server start.", l),
		selectedRows:    simpleCounter("clickhouse_selected_rows_total", "Rows selected since server start.", l),
		selectedBytes:   simpleCounter("clickhouse_selected_bytes_total", "Bytes selected since server start.", l),
		cpuSeconds:      simpleCounter("clickhouse_cpu_seconds_total", "CPU time used by queries.", l),
		diskRead:        simpleCounter("clickhouse_disk_read_elapsed_seconds_total", "Time spent reading from disk.", l),
		diskWrite:       simpleCounter("clickhouse_disk_write_elapsed_seconds_total", "Time spent writing to disk.", l),
		networkReceive:  simpleCounter("clickhouse_network_receive_elapsed_seconds_total", "Time spent receiving network data.", l),
		networkSend:     simpleCounter("clickhouse_network_send_elapsed_seconds_total", "Time spent sending network data.", l),
		memoryLimitHit:  simpleCounter("clickhouse_query_memory_limit_exceeded_total", "Queries that hit the memory limit.", l),
		delayedInserts:  simpleCounter("clickhouse_delayed_inserts_total", "Inserts delayed by too many parts.", l),
		rejectedInserts: simpleCounter("clickhouse_rejected_inserts_total", "Inserts rejected by too many parts.", l),
	}
	reg.MustRegister(
		q.queries, q.queryDuration,
		q.insertedRows, q.insertedBytes, q.selectedRows, q.selectedBytes,
		q.cpuSeconds, q.diskRead, q.diskWrite, q.networkReceive, q.networkSend,
		q.memoryLimitHit, q.delayedInserts, q.rejectedInserts,
	)
	return q
}

func (q *QueryLoad) Name() string            { return "query_load" }
func (q *QueryLoad) Interval() time.Duration { return q.every }

func (q *QueryLoad) Refresh(ctx context.Context) (int, error) {
	var rows []struct {
		Name  string `ch:"name"`
		Value uint64 `ch:"value"`
	}
	if err := q.ch.Select(ctx, &rows, "SELECT name, value FROM system.events"); err != nil {
		return 0, fmt.Errorf("system.events: %w", err)
	}
	events := make(map[string]float64, len(rows))
	for _, r := range rows {
		events[r.Name] = float64(r.Value)
	}

	series := 0
	add := func(key string, c prometheus.Counter, scale float64) {
		raw, ok := events[key]
		if !ok {
			return
		}
		d := q.tracker.delta(key, raw*scale)
		if d > 0 {
			c.Add(d)
		}
		series++
	}

	addType := func(key, typ string) {
		raw, ok := events[key]
		if !ok {
			return
		}
		d := q.tracker.delta(key, raw)
		if d > 0 {
			q.queries.WithLabelValues(typ).Add(d)
		}
		series++
	}
	addType("Query", "all")
	addType("SelectQuery", "select")

	duration := func(key, typ string) {
		raw, ok := events[key]
		if !ok {
			return
		}
		d := q.tracker.delta(key, raw/1e6)
		if d > 0 {
			q.queryDuration.WithLabelValues(typ).Add(d)
		}
		series++
	}
	duration("QueryTimeMicroseconds", "all")
	duration("SelectQueryTimeMicroseconds", "select")
	duration("OtherQueryTimeMicroseconds", "other")

	add("InsertedRows", q.insertedRows, 1)
	add("InsertedBytes", q.insertedBytes, 1)
	add("SelectedRows", q.selectedRows, 1)
	add("SelectedBytes", q.selectedBytes, 1)
	add("OSCPUVirtualTimeMicroseconds", q.cpuSeconds, 1e-6)
	add("DiskReadElapsedMicroseconds", q.diskRead, 1e-6)
	add("DiskWriteElapsedMicroseconds", q.diskWrite, 1e-6)
	add("NetworkReceiveElapsedMicroseconds", q.networkReceive, 1e-6)
	add("NetworkSendElapsedMicroseconds", q.networkSend, 1e-6)
	add("QueryMemoryLimitExceeded", q.memoryLimitHit, 1)
	add("DelayedInserts", q.delayedInserts, 1)
	add("RejectedInserts", q.rejectedInserts, 1)

	return series, nil
}
