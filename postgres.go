// Package postgresext provides the storage.sqlite capability for Pulp
// cells, backed by a shared Postgres connection via lib/pq.
//
// This is a drop-in replacement for Pulp-ext-sqlite. It registers the
// same host import names (sqlite_exec, sqlite_query) so existing cell
// WASM binaries work without recompilation. The cell-side code
// switches to pgdialect to emit Postgres-flavoured SQL; the host
// simply executes whatever SQL it receives.
//
// Deployment:
//
//	import _ "github.com/BananaLabs-OSS/Pulp-ext-postgres"
//
// Host imports exposed:
//
//	sqlite_exec(query_ptr, query_len, params_ptr, params_len, res_ptr_out, res_len_out) -> error_code
//	sqlite_query(query_ptr, query_len, params_ptr, params_len, rows_ptr_out, rows_len_out) -> error_code
//
// All cells share a single Postgres connection pool. The connection
// string is read from the DATABASE_URL environment variable.
package postgresext

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
	_ "github.com/lib/pq"
)

var (
	sharedDB *sql.DB
	logger   *slog.Logger
)

func init() {
	ext.Register(ext.Capability{
		Name:     "storage.sqlite", // same ABI surface as ext-sqlite
		Setup:    setup,
		Teardown: teardown,
		Register: bindActive,
		Stub:     bindStub,
	})
}

// setup opens a shared Postgres connection pool from DATABASE_URL.
// Unlike ext-sqlite there is no per-cell database — all cells share
// one connection pool with different tables.
func setup(env ext.SetupEnv) error {
	logger = env.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("storage.postgres: DATABASE_URL not set")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("storage.postgres: open: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(20 * time.Second)

	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("storage.postgres: ping: %w", err)
	}

	sharedDB = db
	// Log a prefix of the DSN so the full password is not leaked.
	prefix := dsn
	if len(prefix) > 30 {
		prefix = prefix[:30] + "..."
	}
	logger.Info("storage.postgres ready", "dsn_prefix", prefix)
	return nil
}

func teardown(_ context.Context) error {
	if sharedDB != nil {
		return sharedDB.Close()
	}
	return nil
}

// ---- types ----------------------------------------------------------------

type ExecResult struct {
	RowsAffected int64  `msgpack:"rows_affected"`
	LastInsertID int64  `msgpack:"last_insert_id"`
	Error        string `msgpack:"error,omitempty"`
}

type QueryResult struct {
	Columns []string `msgpack:"columns"`
	Rows    [][]any  `msgpack:"rows"`
	Error   string   `msgpack:"error,omitempty"`
}

// ---- binding --------------------------------------------------------------

func bindActive(b wazero.HostModuleBuilder, cell ext.Cell) error {
	if sharedDB == nil {
		return fmt.Errorf("storage.postgres: setup not called")
	}
	exec := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
		return pgExec(ctx, m, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut)
	}
	query := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
		return pgQuery(ctx, m, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut)
	}
	b.NewFunctionBuilder().WithFunc(exec).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(query).Export("sqlite_query")
	logger.Info("storage.postgres bound", "cell", cell.Name())
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop6 := func(_ context.Context, _ api.Module, _, _, _, _, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_query")
	return nil
}

// ---- handlers -------------------------------------------------------------

func pgExec(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
	if qLen == 0 {
		return 1
	}
	q, ok := m.Memory().Read(qPtr, qLen)
	if !ok {
		return 2
	}
	args, code := decodeArgs(m, pPtr, pLen)
	if code != 0 {
		return code
	}

	res, err := sharedDB.ExecContext(ctx, string(q), args...)
	if err != nil {
		encoded, mErr := msgpack.Marshal(ExecResult{Error: err.Error()})
		if mErr != nil {
			return 5
		}
		_ = writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
		return pgErrorCode(err)
	}
	var out ExecResult
	if ra, raErr := res.RowsAffected(); raErr != nil {
		logger.Warn("postgres: RowsAffected failed", "err", raErr)
	} else {
		out.RowsAffected = ra
	}
	if lid, lidErr := res.LastInsertId(); lidErr != nil {
		// Postgres does not support LastInsertId via the database/sql
		// interface — callers should use RETURNING instead. Silently
		// swallow rather than warn on every INSERT.
	} else {
		out.LastInsertID = lid
	}
	encoded, err := msgpack.Marshal(out)
	if err != nil {
		return 5
	}
	return writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
}

func pgQuery(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
	if qLen == 0 {
		return 1
	}
	q, ok := m.Memory().Read(qPtr, qLen)
	if !ok {
		return 2
	}
	args, code := decodeArgs(m, pPtr, pLen)
	if code != 0 {
		return code
	}

	rows, err := sharedDB.QueryContext(ctx, string(q), args...)
	if err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	result := QueryResult{Columns: cols}
	for rows.Next() {
		values := make([]any, len(cols))
		scan := make([]any, len(cols))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return 5
	}
	return writeResponse(ctx, m, encoded, rowsPtrOut, rowsLenOut)
}

func writeQueryError(ctx context.Context, m api.Module, err error, ptrOut, lenOut uint32) uint32 {
	encoded, mErr := msgpack.Marshal(QueryResult{Error: err.Error()})
	if mErr != nil {
		return 5
	}
	_ = writeResponse(ctx, m, encoded, ptrOut, lenOut)
	return pgErrorCode(err)
}

// pgErrorCode maps Postgres errors to the same coarse host codes as
// ext-sqlite so cells can branch on busy vs constraint vs generic
// without parsing the message.
// 5 = generic, 12 = busy/locked, 13 = constraint violation, 14 = readonly.
func pgErrorCode(err error) uint32 {
	if err == nil {
		return 0
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "deadlock"),
		strings.Contains(msg, "could not serialize"):
		return 12
	case strings.Contains(msg, "unique_violation"),
		strings.Contains(msg, "foreign_key_violation"),
		strings.Contains(msg, "not-null_violation"),
		strings.Contains(msg, "check_violation"),
		strings.Contains(msg, "duplicate key"):
		return 13
	case strings.Contains(msg, "read_only"),
		strings.Contains(msg, "insufficient_privilege"):
		return 14
	default:
		return 5
	}
}

// ---- helpers --------------------------------------------------------------

func decodeArgs(m api.Module, ptr, ln uint32) ([]any, uint32) {
	if ln == 0 {
		return nil, 0
	}
	data, ok := m.Memory().Read(ptr, ln)
	if !ok {
		return nil, 2
	}
	var args []any
	if err := msgpack.Unmarshal(data, &args); err != nil {
		return nil, 3
	}
	return args, 0
}

func writeResponse(ctx context.Context, m api.Module, data []byte, ptrOut, lenOut uint32) uint32 {
	allocFn := m.ExportedFunction("pulp_alloc")
	if allocFn == nil {
		return 7
	}
	var ptr uint32
	if len(data) > 0 {
		results, err := allocFn.Call(ctx, uint64(len(data)))
		if err != nil || len(results) == 0 {
			return 7
		}
		ptr = uint32(results[0])
		if ptr == 0 {
			return 7
		}
		if !m.Memory().Write(ptr, data) {
			return 8
		}
	}
	if !m.Memory().WriteUint32Le(ptrOut, ptr) {
		return 8
	}
	if !m.Memory().WriteUint32Le(lenOut, uint32(len(data))) {
		return 8
	}
	return 0
}
