package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// Tables collects table sizes, parts, merges, mutations and write activity.
type Tables struct {
	ch            *ch.Client
	every         time.Duration
	window        time.Duration
	topN          int
	excludeDBs    []string
	excludeTables []string
	sources       *prometheus.GaugeVec

	parts        *prometheus.GaugeVec
	rows         *prometheus.GaugeVec
	diskBytes    *prometheus.GaugeVec
	uncompressed *prometheus.GaugeVec
	lastMod      *prometheus.GaugeVec
	detached     *prometheus.GaugeVec

	mutations      *prometheus.GaugeVec
	mutationOldest *prometheus.GaugeVec

	merges        *prometheus.GaugeVec
	mergeProgress *prometheus.GaugeVec
	mergeElapsed  *prometheus.GaugeVec
	mergeMemory   *prometheus.GaugeVec

	newParts     *prometheus.GaugeVec
	writtenRows  *prometheus.GaugeVec
	writtenBytes *prometheus.GaugeVec
}

// NewTables registers and returns the table collector.
func NewTables(reg prometheus.Registerer, client *ch.Client, cfg config.Collector, tcfg config.Tables, opts Options) *Tables {
	l := opts.Labels
	t := &Tables{
		ch:            client,
		every:         cfg.Interval.Std(),
		window:        tcfg.Window.Std(),
		topN:          tcfg.TopN,
		excludeDBs:    tcfg.ExcludeDatabases,
		excludeTables: tcfg.ExcludeTables,
		sources:       opts.Sources,

		parts:        gauge("clickhouse_table_parts", "Active parts per table.", l, "database", "table"),
		rows:         gauge("clickhouse_table_rows", "Rows in active parts per table.", l, "database", "table"),
		diskBytes:    gauge("clickhouse_table_disk_bytes", "Compressed bytes on disk per table.", l, "database", "table"),
		uncompressed: gauge("clickhouse_table_uncompressed_bytes", "Uncompressed bytes per table.", l, "database", "table"),
		lastMod:      gauge("clickhouse_table_last_modification_seconds", "Seconds since the last part modification.", l, "database", "table"),
		detached:     gauge("clickhouse_table_detached_parts", "Detached parts per table.", l, "database", "table"),

		mutations:      gauge("clickhouse_table_mutations_pending", "Unfinished mutations per table.", l, "database", "table"),
		mutationOldest: gauge("clickhouse_table_mutation_oldest_seconds", "Age of the oldest unfinished mutation.", l, "database", "table"),

		merges:        gauge("clickhouse_table_merges_running", "Merges currently running per table.", l, "database", "table"),
		mergeProgress: gauge("clickhouse_table_merge_progress", "Sum of merge progress per table.", l, "database", "table"),
		mergeElapsed:  gauge("clickhouse_table_merge_max_elapsed_seconds", "Longest running merge per table.", l, "database", "table"),
		mergeMemory:   gauge("clickhouse_table_merge_memory_bytes", "Memory used by merges per table.", l, "database", "table"),

		newParts:     gauge("clickhouse_table_new_parts", "New parts created over the tables window.", l, "database", "table"),
		writtenRows:  gauge("clickhouse_table_written_rows", "Rows written over the tables window.", l, "database", "table"),
		writtenBytes: gauge("clickhouse_table_written_bytes", "Compressed bytes written over the tables window.", l, "database", "table"),
	}
	reg.MustRegister(
		t.parts, t.rows, t.diskBytes, t.uncompressed, t.lastMod, t.detached,
		t.mutations, t.mutationOldest,
		t.merges, t.mergeProgress, t.mergeElapsed, t.mergeMemory,
		t.newParts, t.writtenRows, t.writtenBytes,
	)
	return t
}

func (t *Tables) Name() string            { return "tables" }
func (t *Tables) Interval() time.Duration { return t.every }

func (t *Tables) Refresh(ctx context.Context) (int, error) {
	var errs []error
	series := 0

	if n, err := t.refreshParts(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	if n, err := t.refreshDetached(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	if n, err := t.refreshMutations(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}
	if n, err := t.refreshMerges(ctx); err != nil {
		errs = append(errs, err)
	} else {
		series += n
	}

	hasPartLog, err := t.ch.TableExists(ctx, "system", "part_log")
	if err != nil {
		errs = append(errs, fmt.Errorf("check system.part_log: %w", err))
		hasPartLog = false
	}
	if t.sources != nil {
		t.sources.WithLabelValues("part_log").Set(boolFloat(hasPartLog))
	}
	if hasPartLog {
		if n, err := t.refreshWrites(ctx); err != nil {
			errs = append(errs, err)
		} else {
			series += n
		}
	}

	return series, joinErrors(errs)
}

func (t *Tables) where() string {
	return notIn("database", t.excludeDBs) + notIn("table", t.excludeTables)
}

type tableRow struct {
	Database                string `ch:"database"`
	Table                   string `ch:"table"`
	Parts                   uint64 `ch:"parts"`
	Rows                    uint64 `ch:"rows"`
	DiskBytes               uint64 `ch:"disk_bytes"`
	UncompressedBytes       uint64 `ch:"uncompressed_bytes"`
	LastModificationSeconds int64  `ch:"last_modification_seconds"`
}

func (t *Tables) refreshParts(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    database,
    table,
    count()                                            AS parts,
    sum(rows)                                          AS rows,
    sum(bytes_on_disk)                                 AS disk_bytes,
    sum(data_uncompressed_bytes)                       AS uncompressed_bytes,
    dateDiff('second', max(modification_time), now())  AS last_modification_seconds
FROM system.parts
WHERE active%s
GROUP BY database, table
ORDER BY disk_bytes DESC
LIMIT 20000`, t.where())

	var rows []tableRow
	if err := t.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("table parts: %w", err)
	}
	kept, other := reduceTableRows(rows, t.topN)

	t.parts.Reset()
	t.rows.Reset()
	t.diskBytes.Reset()
	t.uncompressed.Reset()
	t.lastMod.Reset()
	for _, r := range kept {
		t.setTableRow(r)
	}
	if other != nil {
		t.setTableRow(*other)
	}
	return (len(kept) + boolInt(other != nil)) * 5, nil
}

func (t *Tables) setTableRow(r tableRow) {
	t.parts.WithLabelValues(r.Database, r.Table).Set(float64(r.Parts))
	t.rows.WithLabelValues(r.Database, r.Table).Set(float64(r.Rows))
	t.diskBytes.WithLabelValues(r.Database, r.Table).Set(float64(r.DiskBytes))
	t.uncompressed.WithLabelValues(r.Database, r.Table).Set(float64(r.UncompressedBytes))
	t.lastMod.WithLabelValues(r.Database, r.Table).Set(float64(r.LastModificationSeconds))
}

func reduceTableRows(rows []tableRow, n int) ([]tableRow, *tableRow) {
	if len(rows) <= n {
		return rows, nil
	}
	kept := rows[:n]
	other := tableRow{Database: "__other__", Table: "__other__"}
	for _, r := range rows[n:] {
		other.Parts += r.Parts
		other.Rows += r.Rows
		other.DiskBytes += r.DiskBytes
		other.UncompressedBytes += r.UncompressedBytes
		if r.LastModificationSeconds > other.LastModificationSeconds {
			other.LastModificationSeconds = r.LastModificationSeconds
		}
	}
	return kept, &other
}

func (t *Tables) refreshDetached(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT database, table, count() AS parts
FROM system.detached_parts
WHERE 1 = 1%s
GROUP BY database, table
ORDER BY parts DESC
LIMIT %d`, t.where(), t.topN)

	var rows []struct {
		Database string `ch:"database"`
		Table    string `ch:"table"`
		Parts    uint64 `ch:"parts"`
	}
	if err := t.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("detached parts: %w", err)
	}
	t.detached.Reset()
	for _, r := range rows {
		t.detached.WithLabelValues(r.Database, r.Table).Set(float64(r.Parts))
	}
	return len(rows), nil
}

func (t *Tables) refreshMutations(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    database,
    table,
    count()                                       AS pending,
    dateDiff('second', min(create_time), now())   AS oldest_seconds
FROM system.mutations
WHERE is_done = 0%s
GROUP BY database, table
ORDER BY pending DESC
LIMIT %d`, t.where(), t.topN)

	var rows []struct {
		Database      string `ch:"database"`
		Table         string `ch:"table"`
		Pending       uint64 `ch:"pending"`
		OldestSeconds int64  `ch:"oldest_seconds"`
	}
	if err := t.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("mutations: %w", err)
	}
	t.mutations.Reset()
	t.mutationOldest.Reset()
	for _, r := range rows {
		t.mutations.WithLabelValues(r.Database, r.Table).Set(float64(r.Pending))
		t.mutationOldest.WithLabelValues(r.Database, r.Table).Set(float64(r.OldestSeconds))
	}
	return len(rows) * 2, nil
}

func (t *Tables) refreshMerges(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    database,
    table,
    count()                        AS merges,
    sum(progress)                  AS progress,
    max(elapsed)                   AS elapsed,
    toUInt64(sum(memory_usage))    AS memory_bytes
FROM system.merges
WHERE 1 = 1%s
GROUP BY database, table
ORDER BY merges DESC
LIMIT %d`, t.where(), t.topN)

	var rows []struct {
		Database    string  `ch:"database"`
		Table       string  `ch:"table"`
		Merges      uint64  `ch:"merges"`
		Progress    float64 `ch:"progress"`
		Elapsed     float64 `ch:"elapsed"`
		MemoryBytes uint64  `ch:"memory_bytes"`
	}
	if err := t.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("merges: %w", err)
	}
	t.merges.Reset()
	t.mergeProgress.Reset()
	t.mergeElapsed.Reset()
	t.mergeMemory.Reset()
	for _, r := range rows {
		t.merges.WithLabelValues(r.Database, r.Table).Set(float64(r.Merges))
		t.mergeProgress.WithLabelValues(r.Database, r.Table).Set(r.Progress)
		t.mergeElapsed.WithLabelValues(r.Database, r.Table).Set(r.Elapsed)
		t.mergeMemory.WithLabelValues(r.Database, r.Table).Set(float64(r.MemoryBytes))
	}
	return len(rows) * 4, nil
}

type writeRow struct {
	Database     string `ch:"database"`
	Table        string `ch:"table"`
	Parts        uint64 `ch:"parts"`
	WrittenRows  uint64 `ch:"written_rows"`
	WrittenBytes uint64 `ch:"written_bytes"`
}

func (t *Tables) refreshWrites(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`
SELECT
    database,
    table,
    count()               AS parts,
    sum(rows)             AS written_rows,
    sum(size_in_bytes)    AS written_bytes
FROM system.part_log
WHERE event_type = 'NewPart'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL %d SECOND%s
GROUP BY database, table
ORDER BY written_bytes DESC
LIMIT 20000`, int64(t.window.Seconds()), t.where())

	var rows []writeRow
	if err := t.ch.Select(ctx, &rows, query); err != nil {
		return 0, fmt.Errorf("part_log writes: %w", err)
	}
	kept, other := reduceWriteRows(rows, t.topN)

	t.newParts.Reset()
	t.writtenRows.Reset()
	t.writtenBytes.Reset()
	for _, r := range kept {
		t.setWriteRow(r)
	}
	if other != nil {
		t.setWriteRow(*other)
	}
	return (len(kept) + boolInt(other != nil)) * 3, nil
}

func (t *Tables) setWriteRow(r writeRow) {
	t.newParts.WithLabelValues(r.Database, r.Table).Set(float64(r.Parts))
	t.writtenRows.WithLabelValues(r.Database, r.Table).Set(float64(r.WrittenRows))
	t.writtenBytes.WithLabelValues(r.Database, r.Table).Set(float64(r.WrittenBytes))
}

func reduceWriteRows(rows []writeRow, n int) ([]writeRow, *writeRow) {
	if len(rows) <= n {
		return rows, nil
	}
	kept := rows[:n]
	other := writeRow{Database: "__other__", Table: "__other__"}
	for _, r := range rows[n:] {
		other.Parts += r.Parts
		other.WrittenRows += r.WrittenRows
		other.WrittenBytes += r.WrittenBytes
	}
	return kept, &other
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
