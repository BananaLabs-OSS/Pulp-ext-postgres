// Package postgresext provides the storage.sqlite capability for Pulp
// cells, backed by per-cell-scoped Postgres connections via lib/pq.
//
// This is a drop-in replacement for Pulp-ext-sqlite. It registers the
// same host import names (sqlite_exec, sqlite_query) so existing cell
// WASM binaries work without recompilation. The cell-side code
// switches to pgdialect to emit Postgres-flavoured SQL; the host
// simply executes whatever SQL it receives.
//
// Isolation: ext-sqlite gives each cell its own physical database file,
// so a cell physically cannot touch another cell's data. This extension
// reproduces that isolation on a shared Postgres server by giving each
// declaring cell its own Postgres schema and pinning that cell's
// connection pool to it via search_path. A cell's unqualified
// CREATE/SELECT/UPDATE/DELETE therefore resolves into its own private
// schema; cell A cannot see, list, or scan cell B's tables.
//
// The host never parses or rewrites the cell's SQL — scoping is done
// purely at the connection level (search_path), so no key-prefix /
// cell_id column threading is required and existing cell SQL is
// unchanged.
//
// Shared-schema mode: some deployments intentionally share tables
// across cells (e.g. one cell writes a table another cell reads). Set
// STORAGE_POSTGRES_SHARED_SCHEMA to a schema name (e.g. "public") to
// place ALL cells in that one schema instead of per-cell schemas. This
// is an explicit opt-out of isolation; leave it unset for the safe
// per-cell-isolated default.
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
// All cells share a single Postgres server (one DATABASE_URL) but get
// separate connection pools pinned to separate schemas. The connection
// string is read from the DATABASE_URL environment variable.
//
// Note: LastInsertID is always 0 on the Postgres backend (Postgres has
// no last-insert-id via database/sql) — cells must use RETURNING. This
// is a behavioural difference from the sqlite backend.
package postgresext

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
	_ "github.com/lib/pq"
)

// manager owns per-cell *sql.DB handles. Setup runs once (cell name is
// empty there — see Pulp run.Main) so the pools are opened lazily on
// first Register with the manifest's cell name baked in. Each cell's
// pool is pinned to that cell's schema via search_path.
type pgManager struct {
	mu           sync.RWMutex
	dbs          map[string]*sql.DB
	dsn          string
	sharedSchema string // STORAGE_POSTGRES_SHARED_SCHEMA; "" => per-cell isolation
	logger       *slog.Logger
}

var manager = &pgManager{dbs: map[string]*sql.DB{}}

func init() {
	ext.Register(ext.Capability{
		Name:         "storage.sqlite", // same ABI surface as ext-sqlite
		Setup:        setup,
		Teardown:     teardown,
		Register:     bindActive,
		Stub:         bindStub,
		TeardownCell: teardownCell,
	})
}

// setup captures the DSN and logger. It does NOT open a pool — Pulp
// calls Setup once with an empty CellName, so a pool opened here could
// not be pinned to a cell's schema. Pools are opened lazily from
// Register() once the cell identity is known.
func setup(env ext.SetupEnv) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.logger = env.Logger
	if manager.logger == nil {
		manager.logger = slog.Default()
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("storage.postgres: DATABASE_URL not set")
	}
	manager.dsn = dsn
	manager.sharedSchema = sanitizeSchema(os.Getenv("STORAGE_POSTGRES_SHARED_SCHEMA"))

	// Log only host/dbname — never substring the raw DSN, which can
	// expose username/password.
	if manager.sharedSchema != "" {
		manager.logger.Info("storage.postgres setup", "endpoint", dsnEndpoint(dsn), "mode", "shared-schema", "schema", manager.sharedSchema)
	} else {
		manager.logger.Info("storage.postgres setup", "endpoint", dsnEndpoint(dsn), "mode", "per-cell-isolated")
	}
	return nil
}

// teardown closes every open per-cell pool. Safe to call more than once
// — closed handles are removed from the map.
func teardown(_ context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var first error
	for name, db := range manager.dbs {
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", name, err)
		}
		delete(manager.dbs, name)
	}
	return first
}

// teardownCell closes just one cell's pool during a per-cell
// control-socket shutdown, leaving other cells untouched. The cell's
// schema and data are left intact in Postgres (a stopped cell may be
// restarted); only the cached connection pool is released.
func teardownCell(_ context.Context, cellID string) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	db, ok := manager.dbs[cellID]
	if !ok {
		return nil
	}
	delete(manager.dbs, cellID)
	if err := db.Close(); err != nil {
		return fmt.Errorf("close %s: %w", cellID, err)
	}
	if manager.logger != nil {
		manager.logger.Info("storage.postgres teardown cell", "cell", cellID)
	}
	return nil
}

// schemaFor resolves which Postgres schema a cell's pool is pinned to:
// the shared schema if configured, else a per-cell schema derived from
// the cell name.
func (m *pgManager) schemaFor(cellID string) string {
	if m.sharedSchema != "" {
		return m.sharedSchema
	}
	s := sanitizeSchema(cellID)
	if s == "" {
		s = "cell"
	}
	return "cell_" + s
}

// openForCell opens a connection pool pinned to the cell's schema via
// search_path, creating the schema if it does not exist, and caches the
// handle. Idempotent — returns the cached *sql.DB on subsequent calls.
func (m *pgManager) openForCell(cellID string) (*sql.DB, error) {
	m.mu.RLock()
	if db, ok := m.dbs[cellID]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the write lock — another caller may have raced us.
	if db, ok := m.dbs[cellID]; ok {
		return db, nil
	}
	if m.dsn == "" {
		return nil, fmt.Errorf("storage.postgres: setup not called before register")
	}

	schema := m.schemaFor(cellID)

	// Create the schema on a short-lived pool that is NOT pinned to it
	// (the schema may not exist yet, which would make a pinned
	// connection's search_path point at nothing).
	bootstrap, err := sql.Open("postgres", m.dsn)
	if err != nil {
		return nil, fmt.Errorf("open bootstrap: %w", err)
	}
	if _, err := bootstrap.Exec(`CREATE SCHEMA IF NOT EXISTS "` + schema + `"`); err != nil {
		bootstrap.Close()
		return nil, fmt.Errorf("create schema %q: %w", schema, err)
	}
	bootstrap.Close()

	// Pin the cell's pool to its schema. search_path is set per
	// connection via the libpq `options` parameter, so every pooled
	// connection resolves unqualified names into this cell's schema.
	dsn, err := dsnWithSearchPath(m.dsn, schema)
	if err != nil {
		return nil, fmt.Errorf("build dsn: %w", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	// Bound connection lifetime so connections recycled / killed
	// server-side by a managed pooler (Crunchy Bridge / pgbouncer,
	// failover, idle reaper) are not handed out dead.
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	m.dbs[cellID] = db
	if m.logger != nil {
		m.logger.Info("storage.postgres ready", "cell", cellID, "schema", schema)
	}
	return db, nil
}

func (m *pgManager) get(cellID string) (*sql.DB, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbs[cellID]
	return db, ok
}

// sanitizeSchema lowercases s and strips anything outside [a-z0-9_], so
// a cell name can never break out of its quoted schema identifier or
// inject SQL into the CREATE SCHEMA / search_path.
func sanitizeSchema(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

// dsnWithSearchPath returns dsn with libpq `options=-c search_path=<schema>`
// appended, handling both URL-form (postgres://…) and keyword/value-form
// DSNs. The schema is already sanitized to [a-z0-9_] by the caller.
func dsnWithSearchPath(dsn, schema string) (string, error) {
	opt := "-c search_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		q := u.Query()
		// Preserve any existing options the operator set.
		if existing := q.Get("options"); existing != "" {
			q.Set("options", existing+" "+opt)
		} else {
			q.Set("options", opt)
		}
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	// Keyword/value form. libpq treats `options` as a single value;
	// quote it so the embedded space is not parsed as a new keyword.
	return dsn + " options='" + opt + "'", nil
}

// dsnEndpoint returns a non-credential host[:port]/dbname marker for
// logging. Never logs userinfo. Falls back to a fixed marker if the DSN
// is not URL-form.
func dsnEndpoint(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			db := strings.TrimPrefix(u.Path, "/")
			if db == "" {
				return u.Host
			}
			return u.Host + "/" + db
		}
	}
	return "(non-url dsn)"
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
	cellID := cell.Name()
	// Open eagerly so a misconfigured DSN / unreachable server fails at
	// cell load, not on the first query, and the cell's schema is
	// created up front. Errors here abort cell registration.
	if _, err := manager.openForCell(cellID); err != nil {
		return fmt.Errorf("storage.postgres: open for cell %q: %w", cellID, err)
	}
	exec := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
		return pgExec(ctx, m, cellID, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut)
	}
	query := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
		return pgQuery(ctx, m, cellID, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut)
	}
	b.NewFunctionBuilder().WithFunc(exec).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(query).Export("sqlite_query")
	manager.logger.Info("storage.postgres bound", "cell", cellID)
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop6 := func(_ context.Context, _ api.Module, _, _, _, _, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_query")
	return nil
}

// ---- handlers -------------------------------------------------------------

func pgExec(ctx context.Context, m api.Module, cellID string, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
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
	db, ok := manager.get(cellID)
	if !ok {
		return 9
	}

	res, err := db.ExecContext(ctx, string(q), args...)
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
		manager.logger.Warn("postgres: RowsAffected failed", "err", raErr)
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

func pgQuery(ctx context.Context, m api.Module, cellID string, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
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
	db, ok := manager.get(cellID)
	if !ok {
		return 9
	}

	rows, err := db.QueryContext(ctx, string(q), args...)
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
