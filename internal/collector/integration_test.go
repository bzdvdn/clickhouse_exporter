//go:build integration

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"clickhouse-exporter/internal/ch"
	"clickhouse-exporter/internal/config"
)

// TestCollectorsAgainstClickHouse runs every collector against a real server.
//
//	CLICKHOUSE_DSN='clickhouse://exporter@127.0.0.1:9000/default' \
//	  go test -tags=integration ./internal/collector
func TestCollectorsAgainstClickHouse(t *testing.T) {
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("CLICKHOUSE_DSN is not set")
	}
	ctx := context.Background()
	cfg := config.Default()
	cfg.ClickHouse.DSN = dsn
	cfg.ClickHouse.Timeout = config.Duration(10 * time.Second)

	client, err := ch.Open(ctx, cfg.ClickHouse)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer client.Close()

	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"cluster": "test", "node": "test"}
	opts := Options{
		Labels:  labels,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Sources: NewSourceAvailability(reg, labels),
	}

	collectors := []Collector{
		NewNode(reg, client, cfg.Collectors.Node, opts),
		NewQueryLoad(reg, client, cfg.Collectors.QueryLoad, opts),
		NewWorkload(reg, client, cfg.Collectors.Workload, cfg.Workload, opts),
		NewTables(reg, client, cfg.Collectors.Tables, cfg.Tables, opts),
		NewReplication(reg, client, cfg.Collectors.Replication, cfg.Replication, opts),
	}
	for _, c := range collectors {
		series, err := c.Refresh(ctx)
		if err != nil {
			t.Errorf("%s: %v", c.Name(), err)
			continue
		}
		t.Logf("%s: %d series", c.Name(), series)
	}
}
