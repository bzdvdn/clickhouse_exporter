// Package collector contains the metric collectors.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
)

// Collector refreshes a set of metric vectors.
type Collector interface {
	Name() string
	Interval() time.Duration
	// Refresh queries ClickHouse and updates metrics. It returns the number of
	// series written and an error. Metrics from successful sub-queries are kept
	// even when another sub-query fails.
	Refresh(ctx context.Context) (int, error)
}

// Options are shared collector dependencies.
type Options struct {
	Labels  prometheus.Labels
	Logger  *slog.Logger
	Sources *prometheus.GaugeVec
}

func (o Options) logger() *slog.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return slog.Default()
}

// NewSourceAvailability registers the shared source-availability metric.
func NewSourceAvailability(reg prometheus.Registerer, labels prometheus.Labels) *prometheus.GaugeVec {
	v := gauge("clickhouse_exporter_source_available", "Whether a source system table is available (1/0).", labels, "source")
	reg.MustRegister(v)
	return v
}

func gauge(name, help string, constLabels prometheus.Labels, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	}, labels)
}

func counter(name, help string, constLabels prometheus.Labels, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	}, labels)
}

func simpleGauge(name, help string, constLabels prometheus.Labels) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	})
}

func simpleCounter(name, help string, constLabels prometheus.Labels) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Name:        name,
		Help:        help,
		ConstLabels: constLabels,
	})
}

// notIn renders " AND column NOT IN (...)" or an empty string.
func notIn(column string, values []string) string {
	if len(values) == 0 {
		return ""
	}
	return " AND " + column + " NOT IN (" + ch.QuoteList(values) + ")"
}

// deltaTracker turns a cumulative value into a Prometheus delta and handles
// server restarts (a smaller value means the counter reset).
type deltaTracker struct {
	last map[string]float64
}

func newDeltaTracker() *deltaTracker {
	return &deltaTracker{last: map[string]float64{}}
}

func (d *deltaTracker) delta(key string, current float64) float64 {
	prev, ok := d.last[key]
	d.last[key] = current
	if !ok {
		// First observation: export the full value, as a Counter would.
		return current
	}
	if current < prev {
		return current
	}
	return current - prev
}

func joinErrors(errs []error) error {
	return errors.Join(errs...)
}
