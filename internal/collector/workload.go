package collector

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// Workload attributes query load to users and tables.
type Workload struct {
	ch            *ch.Client
	every         time.Duration
	window        time.Duration
	latencyWindow time.Duration
	topN          int
	excludeUsers  []string

	userQueries      *prometheus.GaugeVec
	userCPU          *prometheus.GaugeVec
	userMemory       *prometheus.GaugeVec
	userReadBytes    *prometheus.GaugeVec
	userWrittenBytes *prometheus.GaugeVec
	userReadRows     *prometheus.GaugeVec
	userWrittenRows  *prometheus.GaugeVec
	userErrors       *prometheus.GaugeVec
	userDuration     *prometheus.GaugeVec

	runningQueries *prometheus.GaugeVec
	runningMemory  *prometheus.GaugeVec
	runningOldest  *prometheus.GaugeVec

	userDatabaseReadBytes   *prometheus.GaugeVec
	userDatabaseReadQueries *prometheus.GaugeVec
	userDatabaseRunning     *prometheus.GaugeVec
	userDatabaseRunningMem  *prometheus.GaugeVec
	userDatabaseRunningOld  *prometheus.GaugeVec

	userTableParts *prometheus.GaugeVec
	userTableRows  *prometheus.GaugeVec
	userTableBytes *prometheus.GaugeVec
	sources        *prometheus.GaugeVec
}

// NewWorkload registers and returns the workload collector.
func NewWorkload(reg prometheus.Registerer, client *ch.Client, cfg config.Collector, wcfg config.Workload, opts Options) *Workload {
	l := opts.Labels
	w := &Workload{
		ch:            client,
		every:         cfg.Interval.Std(),
		window:        wcfg.Window.Std(),
		latencyWindow: wcfg.LatencyWindow.Std(),
		topN:          wcfg.TopN,
		excludeUsers:  wcfg.ExcludeUsers,

		userQueries:      gauge("clickhouse_user_queries", "Queries over the workload window.", l, "user", "query_kind"),
		userCPU:          gauge("clickhouse_user_cpu_seconds", "CPU seconds over the workload window.", l, "user", "query_kind"),
		userMemory:       gauge("clickhouse_user_memory_bytes", "Sum of per-query memory peaks over the workload window.", l, "user", "query_kind"),
		userReadBytes:    gauge("clickhouse_user_read_bytes", "Bytes read over the workload window.", l, "user", "query_kind"),
		userWrittenBytes: gauge("clickhouse_user_written_bytes", "Bytes written over the workload window.", l, "user", "query_kind"),
		userReadRows:     gauge("clickhouse_user_read_rows", "Rows read over the workload window.", l, "user", "query_kind"),
		userWrittenRows:  gauge("clickhouse_user_written_rows", "Rows written over the workload window.", l, "user", "query_kind"),
		userErrors:       gauge("clickhouse_user_errors", "Failed queries over the workload window.", l, "user", "query_kind"),
		userDuration:     gauge("clickhouse_user_query_duration_seconds", "Query duration percentiles over the latency window.", l, "user", "query_kind", "percentile"),

		runningQueries: gauge("clickhouse_user_running_queries", "Queries currently running.", l, "user"),
		runningMemory:  gauge("clickhouse_user_running_memory_bytes", "Memory used by running queries.", l, "user"),
		runningOldest:  gauge("clickhouse_user_oldest_running_query_seconds", "Elapsed time of the oldest running query.", l, "user"),

		userDatabaseReadBytes:   gauge("clickhouse_user_database_read_bytes_touched", "Bytes read by queries touching this database; a query reading several databases counts in each.", l, "user", "database"),
		userDatabaseReadQueries: gauge("clickhouse_user_database_read_queries_touched", "Queries touching this database; a query reading several databases counts in each.", l, "user", "database"),
		userDatabaseRunning:     gauge("clickhouse_user_database_running_queries", "Queries currently running in this database.", l, "user", "database"),
		userDatabaseRunningMem:  gauge("clickhouse_user_database_running_memory_bytes", "Memory used by running queries in this database.", l, "user", "database"),
		userDatabaseRunningOld:  gauge("clickhouse_user_database_running_oldest_query_seconds", "Elapsed time of the oldest running query in this database.", l, "user", "database"),

		userTableParts: gauge("clickhouse_user_table_parts", "New parts written over the workload window.", l, "user", "database", "table"),
		userTableRows:  gauge("clickhouse_user_table_written_rows", "Rows written per table over the workload window.", l, "user", "database", "table"),
		userTableBytes: gauge("clickhouse_user_table_written_bytes", "Compressed bytes written per table over the workload window.", l, "user", "database", "table"),
		sources:        opts.Sources,
	}
	reg.MustRegister(
		w.userQueries, w.userCPU, w.userMemory, w.userReadBytes, w.userWrittenBytes,
		w.userReadRows, w.userWrittenRows, w.userErrors, w.userDuration,
		w.runningQueries, w.runningMemory, w.runningOldest,
		w.userDatabaseReadBytes, w.userDatabaseReadQueries,
		w.userDatabaseRunning, w.userDatabaseRunningMem, w.userDatabaseRunningOld,
		w.userTableParts, w.userTableRows, w.userTableBytes,
	)
	return w
}

func (w *Workload) Name() string            { return "workload" }
func (w *Workload) Interval() time.Duration { return w.every }

func (w *Workload) Refresh(ctx context.Context) (int, error) {
	var errs []error
	series := 0

	hasQueryLog, err := w.ch.TableExists(ctx, "system", "query_log")
	if err != nil {
		errs = append(errs, fmt.Errorf("check system.query_log: %w", err))
		hasQueryLog = false
	}
	w.sourceSet("query_log", hasQueryLog)

	if hasQueryLog {
		if n, err := w.refreshUsers(ctx); err != nil {
			errs = append(errs, err)
		} else {
			series += n
		}
		if n, err := w.refreshLatency(ctx); err != nil {
			errs = append(errs, err)
		} else {
			series += n
		}
		if n, err := w.refreshUserDatabaseReads(ctx); err != nil {
			errs = append(errs, err)
		} else {
			series += n
		}
	}

	if n, err := w.refreshRunning(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}

	hasPartLog, err := w.ch.TableExists(ctx, "system", "part_log")
	if err != nil {
		errs = append(errs, fmt.Errorf("check system.part_log: %w", err))
		hasPartLog = false
	}
	w.sourceSet("part_log", hasPartLog)

	if hasQueryLog && hasPartLog {
		if n, err := w.refreshUserTables(ctx); err != nil {
			errs = append(errs, err)
		} else {
			series += n
		}
	}

	return series, joinErrors(errs)
}

func (w *Workload) refreshUsers(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    user,
    query_kind,
    count()                                                  AS queries,
    sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 AS cpu_seconds,
    sum(memory_usage)                                        AS memory_bytes,
    sum(read_bytes)                                          AS read_bytes,
    sum(written_bytes)                                       AS written_bytes,
    sum(read_rows)                                           AS read_rows,
    sum(written_rows)                                        AS written_rows,
    countIf(exception_code != 0)                             AS errors
FROM system.query_log
WHERE event_date >= today() - 1
  AND event_time >= now() - INTERVAL %d SECOND
  AND type IN (2, 3, 4)
  AND is_initial_query%s
GROUP BY user, query_kind
ORDER BY cpu_seconds DESC
LIMIT %d`, int64(w.window.Seconds()), notIn("user", w.excludeUsers), w.topN)

	var rows []struct {
		User         string  `ch:"user"`
		QueryKind    string  `ch:"query_kind"`
		Queries      uint64  `ch:"queries"`
		CPUSeconds   float64 `ch:"cpu_seconds"`
		MemoryBytes  uint64  `ch:"memory_bytes"`
		ReadBytes    uint64  `ch:"read_bytes"`
		WrittenBytes uint64  `ch:"written_bytes"`
		ReadRows     uint64  `ch:"read_rows"`
		WrittenRows  uint64  `ch:"written_rows"`
		Errors       uint64  `ch:"errors"`
	}
	if err := w.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("workload by user: %w", err)
	}

	w.userQueries.Reset()
	w.userCPU.Reset()
	w.userMemory.Reset()
	w.userReadBytes.Reset()
	w.userWrittenBytes.Reset()
	w.userReadRows.Reset()
	w.userWrittenRows.Reset()
	w.userErrors.Reset()

	for _, r := range rows {
		w.userQueries.WithLabelValues(r.User, r.QueryKind).Set(float64(r.Queries))
		w.userCPU.WithLabelValues(r.User, r.QueryKind).Set(r.CPUSeconds)
		w.userMemory.WithLabelValues(r.User, r.QueryKind).Set(float64(r.MemoryBytes))
		w.userReadBytes.WithLabelValues(r.User, r.QueryKind).Set(float64(r.ReadBytes))
		w.userWrittenBytes.WithLabelValues(r.User, r.QueryKind).Set(float64(r.WrittenBytes))
		w.userReadRows.WithLabelValues(r.User, r.QueryKind).Set(float64(r.ReadRows))
		w.userWrittenRows.WithLabelValues(r.User, r.QueryKind).Set(float64(r.WrittenRows))
		w.userErrors.WithLabelValues(r.User, r.QueryKind).Set(float64(r.Errors))
	}
	return len(rows) * 8, nil
}

func (w *Workload) refreshLatency(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    user,
    query_kind,
    quantiles(0.5, 0.95, 0.99)(toFloat64(query_duration_ms) / 1000) AS q
FROM system.query_log
WHERE event_date >= today() - 1
  AND event_time >= now() - INTERVAL %d SECOND
  AND type IN (2, 4)
  AND is_initial_query%s
GROUP BY user, query_kind
ORDER BY count() DESC
LIMIT %d`, int64(w.latencyWindow.Seconds()), notIn("user", w.excludeUsers), w.topN)

	var rows []struct {
		User      string    `ch:"user"`
		QueryKind string    `ch:"query_kind"`
		Q         []float64 `ch:"q"`
	}
	if err := w.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("latency by user: %w", err)
	}

	w.userDuration.Reset()
	percentiles := []string{"0.5", "0.95", "0.99"}
	for _, r := range rows {
		for i, q := range r.Q {
			if i >= len(percentiles) {
				break
			}
			w.userDuration.WithLabelValues(r.User, r.QueryKind, percentiles[i]).Set(q)
		}
	}
	return len(rows) * len(percentiles), nil
}

func (w *Workload) refreshUserDatabaseReads(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    user,
    database,
    count()         AS queries,
    sum(read_bytes) AS read_bytes
FROM system.query_log
ARRAY JOIN databases AS database
WHERE event_date >= today() - 1
  AND event_time >= now() - INTERVAL %d SECOND
  AND type IN (2, 4)
  AND is_initial_query
  AND database != ''%s
GROUP BY user, database
ORDER BY read_bytes DESC
LIMIT %d`, int64(w.window.Seconds()), notIn("user", w.excludeUsers), w.topN)

	var rows []struct {
		User      string `ch:"user"`
		Database  string `ch:"database"`
		Queries   uint64 `ch:"queries"`
		ReadBytes uint64 `ch:"read_bytes"`
	}
	if err := w.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("workload by database: %w", err)
	}

	w.userDatabaseReadBytes.Reset()
	w.userDatabaseReadQueries.Reset()
	for _, r := range rows {
		w.userDatabaseReadBytes.WithLabelValues(r.User, r.Database).Set(float64(r.ReadBytes))
		w.userDatabaseReadQueries.WithLabelValues(r.User, r.Database).Set(float64(r.Queries))
	}
	return len(rows) * 2, nil
}

type runningAgg struct {
	queries uint64
	memory  int64
	oldest  float64
}

type userDBKey struct {
	user     string
	database string
}

func (w *Workload) refreshRunning(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    user,
    if(current_database = '', 'unknown', current_database) AS database,
    memory_usage,
    elapsed
FROM system.processes
WHERE 1 = 1%s
LIMIT 10000`, notIn("user", w.excludeUsers))

	var rows []struct {
		User     string  `ch:"user"`
		Database string  `ch:"database"`
		Memory   int64   `ch:"memory_usage"`
		Elapsed  float64 `ch:"elapsed"`
	}
	if err := w.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("running queries: %w", err)
	}

	byUser := make(map[string]*runningAgg, len(rows))
	byUserDB := make(map[userDBKey]*runningAgg, len(rows))
	for _, r := range rows {
		a := byUser[r.User]
		if a == nil {
			a = &runningAgg{}
			byUser[r.User] = a
		}
		a.queries++
		a.memory += r.Memory
		if r.Elapsed > a.oldest {
			a.oldest = r.Elapsed
		}

		key := userDBKey{user: r.User, database: r.Database}
		b := byUserDB[key]
		if b == nil {
			b = &runningAgg{}
			byUserDB[key] = b
		}
		b.queries++
		b.memory += r.Memory
		if r.Elapsed > b.oldest {
			b.oldest = r.Elapsed
		}
	}

	type userEntry struct {
		user string
		agg  runningAgg
	}
	users := make([]userEntry, 0, len(byUser))
	for user, agg := range byUser {
		users = append(users, userEntry{user: user, agg: *agg})
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].agg.queries != users[j].agg.queries {
			return users[i].agg.queries > users[j].agg.queries
		}
		return users[i].agg.memory > users[j].agg.memory
	})
	if len(users) > w.topN {
		users = users[:w.topN]
	}

	type dbEntry struct {
		key userDBKey
		agg runningAgg
	}
	dbs := make([]dbEntry, 0, len(byUserDB))
	for key, agg := range byUserDB {
		dbs = append(dbs, dbEntry{key: key, agg: *agg})
	}
	sort.Slice(dbs, func(i, j int) bool {
		if dbs[i].agg.queries != dbs[j].agg.queries {
			return dbs[i].agg.queries > dbs[j].agg.queries
		}
		return dbs[i].agg.memory > dbs[j].agg.memory
	})
	if len(dbs) > w.topN {
		dbs = dbs[:w.topN]
	}

	w.runningQueries.Reset()
	w.runningMemory.Reset()
	w.runningOldest.Reset()
	for _, e := range users {
		w.runningQueries.WithLabelValues(e.user).Set(float64(e.agg.queries))
		w.runningMemory.WithLabelValues(e.user).Set(float64(e.agg.memory))
		w.runningOldest.WithLabelValues(e.user).Set(e.agg.oldest)
	}

	w.userDatabaseRunning.Reset()
	w.userDatabaseRunningMem.Reset()
	w.userDatabaseRunningOld.Reset()
	for _, e := range dbs {
		w.userDatabaseRunning.WithLabelValues(e.key.user, e.key.database).Set(float64(e.agg.queries))
		w.userDatabaseRunningMem.WithLabelValues(e.key.user, e.key.database).Set(float64(e.agg.memory))
		w.userDatabaseRunningOld.WithLabelValues(e.key.user, e.key.database).Set(e.agg.oldest)
	}

	return len(users)*3 + len(dbs)*3, nil
}

func (w *Workload) refreshUserTables(ctx context.Context) (int, error) {
	// part_log gives the physical target table and compressed size; query_log
	// supplies the user. The part_log window ends slightly in the past because
	// query_log is flushed asynchronously.
	query := fmt.Sprintf(`
SELECT
    if(q.user = '', 'unknown', q.user) AS user,
    p.database                          AS database,
    p.table                             AS table,
    count()                             AS parts,
    sum(p.rows)                         AS written_rows,
    sum(p.size_in_bytes)                AS written_bytes
FROM system.part_log AS p
ANY LEFT JOIN (
    SELECT query_id, user
    FROM system.query_log
    WHERE event_date >= today() - 1
      AND event_time >= now() - INTERVAL %d SECOND
      AND type IN (2, 4)
      AND is_initial_query
      AND query_kind = 'Insert'%s
) AS q ON p.query_id = q.query_id
WHERE p.event_type = 'NewPart'
  AND p.event_date >= today() - 1
  AND p.event_time >= now() - INTERVAL %d SECOND
  AND p.event_time < now() - INTERVAL 15 SECOND
GROUP BY user, database, table
ORDER BY written_bytes DESC
LIMIT %d
SETTINGS join_use_nulls = 0`,
		int64((w.window + 5*time.Minute).Seconds()), notIn("user", w.excludeUsers),
		int64(w.window.Seconds()), w.topN)

	var rows []struct {
		User         string `ch:"user"`
		Database     string `ch:"database"`
		Table        string `ch:"table"`
		Parts        uint64 `ch:"parts"`
		WrittenRows  uint64 `ch:"written_rows"`
		WrittenBytes uint64 `ch:"written_bytes"`
	}
	if err := w.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("workload by user and table: %w", err)
	}

	w.userTableParts.Reset()
	w.userTableRows.Reset()
	w.userTableBytes.Reset()
	for _, r := range rows {
		w.userTableParts.WithLabelValues(r.User, r.Database, r.Table).Set(float64(r.Parts))
		w.userTableRows.WithLabelValues(r.User, r.Database, r.Table).Set(float64(r.WrittenRows))
		w.userTableBytes.WithLabelValues(r.User, r.Database, r.Table).Set(float64(r.WrittenBytes))
	}
	return len(rows) * 3, nil
}

func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func (w *Workload) sourceSet(source string, available bool) {
	if w.sources != nil {
		w.sources.WithLabelValues(source).Set(boolFloat(available))
	}
}
