// Package exporter wires collectors into a scrape-safe runtime.
package exporter

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/collector"
)

// Manager runs collectors in the background so a slow query never blocks a scrape.
type Manager struct {
	collectors []collector.Collector
	logger     *slog.Logger

	success     *prometheus.GaugeVec
	duration    *prometheus.GaugeVec
	lastSuccess *prometheus.GaugeVec
	errors      *prometheus.CounterVec
	series      *prometheus.GaugeVec

	mu   sync.Mutex
	last map[string]time.Time
}

// NewManager registers self-metrics and returns a manager.
func NewManager(reg prometheus.Registerer, collectors []collector.Collector, logger *slog.Logger, labels prometheus.Labels) *Manager {
	m := &Manager{
		collectors: collectors,
		logger:     logger,
		success: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "clickhouse_exporter_collector_success",
			Help:        "Last collector run succeeded (1/0).",
			ConstLabels: labels,
		}, []string{"collector"}),
		duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "clickhouse_exporter_collector_duration_seconds",
			Help:        "Duration of the last collector run.",
			ConstLabels: labels,
		}, []string{"collector"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "clickhouse_exporter_last_success_timestamp_seconds",
			Help:        "Unix timestamp of the last successful collector run.",
			ConstLabels: labels,
		}, []string{"collector"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "clickhouse_exporter_sql_errors_total",
			Help:        "Collector run failures.",
			ConstLabels: labels,
		}, []string{"collector"}),
		series: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "clickhouse_exporter_series",
			Help:        "Series written by the last collector run.",
			ConstLabels: labels,
		}, []string{"collector"}),
		last: make(map[string]time.Time, len(collectors)),
	}
	reg.MustRegister(m.success, m.duration, m.lastSuccess, m.errors, m.series)
	return m
}

// Run starts one goroutine per collector and blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range m.collectors {
		wg.Add(1)
		go func(c collector.Collector) {
			defer wg.Done()
			m.loop(ctx, c)
		}(c)
	}
	wg.Wait()
}

func (m *Manager) loop(ctx context.Context, c collector.Collector) {
	interval := c.Interval()
	jitter := time.Duration(rand.Int64N(int64(min(interval, 5*time.Second)))) //nolint:gosec // jitter is not security-sensitive
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}
	m.refresh(ctx, c)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refresh(ctx, c)
		}
	}
}

func (m *Manager) refresh(ctx context.Context, c collector.Collector) {
	name := c.Name()
	start := time.Now()
	n, err := c.Refresh(ctx)
	elapsed := time.Since(start)

	m.duration.WithLabelValues(name).Set(elapsed.Seconds())
	m.series.WithLabelValues(name).Set(float64(n))

	if err != nil {
		m.success.WithLabelValues(name).Set(0)
		m.errors.WithLabelValues(name).Inc()
		m.logger.Error("collector failed",
			"collector", name,
			"duration", elapsed.Round(time.Millisecond),
			"error", err,
		)
		return
	}
	m.success.WithLabelValues(name).Set(1)
	m.lastSuccess.WithLabelValues(name).Set(float64(time.Now().Unix()))
	m.mu.Lock()
	m.last[name] = time.Now()
	m.mu.Unlock()
	m.logger.Debug("collector finished",
		"collector", name,
		"duration", elapsed.Round(time.Millisecond),
		"series", n,
	)
}

// Ready reports whether every collector has succeeded recently enough.
func (m *Manager) Ready() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, c := range m.collectors {
		last, ok := m.last[c.Name()]
		if !ok {
			return false
		}
		if now.Sub(last) > max(3*c.Interval(), 2*time.Minute) {
			return false
		}
	}
	return true
}
