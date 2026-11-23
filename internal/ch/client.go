// Package ch wraps the ClickHouse driver with timeouts and small helpers.
package ch

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"clickhouse-exporter/internal/config"
)

// Client is a thin, read-only wrapper around clickhouse.Conn.
type Client struct {
	conn    clickhouse.Conn
	timeout time.Duration
}

// Open connects to ClickHouse and verifies the connection.
func Open(ctx context.Context, cfg config.ClickHouse) (*Client, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	opts.DialTimeout = cfg.Timeout.Std()
	opts.ReadTimeout = cfg.Timeout.Std() + time.Second
	opts.MaxOpenConns = 8
	opts.MaxIdleConns = 4
	opts.ConnMaxLifetime = time.Hour
	if opts.Settings == nil {
		opts.Settings = map[string]any{}
	}
	for k, v := range cfg.Settings {
		opts.Settings[k] = v
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.Timeout.Std())
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Client{conn: conn, timeout: cfg.Timeout.Std()}, nil
}

// Select runs a query and scans the result into dest.
func (c *Client) Select(ctx context.Context, dest any, query string) error {
	qctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.conn.Select(qctx, dest, query)
}

// Ping checks that the server is reachable.
func (c *Client) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.conn.Ping(pingCtx)
}

// Close releases the connection pool.
func (c *Client) Close() error { return c.conn.Close() }

// TableExists reports whether database.table exists. ClickHouse creates
// system.query_log / system.part_log lazily, so this is checked at runtime.
func (c *Client) TableExists(ctx context.Context, database, table string) (bool, error) {
	var rows []struct {
		N uint64 `ch:"n"`
	}
	query := fmt.Sprintf(
		"SELECT count() AS n FROM system.tables WHERE database = %s AND name = %s",
		Quote(database), Quote(table),
	)
	if err := c.Select(ctx, &rows, query); err != nil {
		return false, err
	}
	return len(rows) == 1 && rows[0].N > 0, nil
}

// Quote renders a string literal for ClickHouse SQL.
func Quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// QuoteList renders a comma-separated list of SQL string literals.
func QuoteList(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = Quote(v)
	}
	return strings.Join(quoted, ", ")
}
