package sqlitecompat

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/lib/pq"
)

// NewSQLiteCompatiblePostgresConnector returns a native database/sql connector
// which applies the same bounded SQLite ABI translation as the Pulp host. It
// exists for native integration and migration tooling; production cells still
// receive database authority only through the scoped Pulp host capability.
func NewPostgresConnector(dsn string) (driver.Connector, error) {
	base, err := pq.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("storage.postgres: parse DSN: %w", err)
	}
	return sqliteCompatConnector{base: base}, nil
}

type sqliteCompatConnector struct{ base driver.Connector }

func (c sqliteCompatConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &sqliteCompatConn{Conn: conn}, nil
}

func (c sqliteCompatConnector) Driver() driver.Driver { return c.base.Driver() }

type sqliteCompatConn struct{ driver.Conn }

func (c *sqliteCompatConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	exec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rewritten, err := Rewrite(query, len(args))
	if err != nil {
		return nil, err
	}
	normalizeNamedValues(args)
	return exec.ExecContext(ctx, rewritten, args)
}

func (c *sqliteCompatConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rewritten, err := Rewrite(query, len(args))
	if err != nil {
		return nil, err
	}
	normalizeNamedValues(args)
	return queryer.QueryContext(ctx, rewritten, args)
}

func normalizeNamedValues(args []driver.NamedValue) {
	for i := range args {
		args[i].Value = NormalizeValue(args[i].Value)
	}
}

// NormalizeValue preserves SQLite's integer representation of booleans when
// calls cross the PostgreSQL wire protocol.
func NormalizeValue(value any) any {
	if boolean, ok := value.(bool); ok {
		if boolean {
			return int64(1)
		}
		return int64(0)
	}
	return value
}

func (c *sqliteCompatConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	return beginner.BeginTx(ctx, opts)
}

func (c *sqliteCompatConn) Ping(ctx context.Context) error {
	pinger, ok := c.Conn.(driver.Pinger)
	if !ok {
		return nil
	}
	return pinger.Ping(ctx)
}

func (c *sqliteCompatConn) ResetSession(ctx context.Context) error {
	resetter, ok := c.Conn.(driver.SessionResetter)
	if !ok {
		return nil
	}
	return resetter.ResetSession(ctx)
}

func (c *sqliteCompatConn) IsValid() bool {
	validator, ok := c.Conn.(driver.Validator)
	return !ok || validator.IsValid()
}
