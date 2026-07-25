// Package postgresext provides the storage.sqlite capability for Pulp
// cells, backed by per-cell-scoped Postgres connections via lib/pq.
//
// This is a drop-in replacement for Pulp-ext-sqlite. It registers the
// same host import names (sqlite_exec, sqlite_query) so existing cell
// WASM binaries work without recompilation. The host has a deliberately
// bounded SQL compatibility adapter for the SQLite-storage ABI: it rewrites
// positional ? binds, BLOB, INTEGER PRIMARY KEY AUTOINCREMENT, and X'hex'
// literals to their Postgres equivalents. Native Postgres $n SQL passes
// through unchanged. Ambiguous or unsupported SQLite-only syntax fails
// closed rather than being guessed or silently corrupted.
//
// Shared schema (DEFAULT): the platform's first-party cells
// intentionally share tables — e.g. the Evolution engine WRITES
// game_visibility/tier/server and the Sessions gene READS them via
// unqualified queries against the SAME table. To preserve that design
// (and the existing production data, which lives in `public`), all
// cells share one schema by default. The shared schema is `public`
// unless STORAGE_POSTGRES_SHARED_SCHEMA names a different one. This is
// the behaviour the pre-isolation pool had, so it is a no-migration,
// prod-preserving default.
//
// Per-cell isolation (OPT-IN): ext-sqlite gives each cell its own
// physical database file, so a cell physically cannot touch another
// cell's data. This extension can reproduce that isolation on a shared
// Postgres server by giving each declaring cell its own Postgres schema
// and pinning that cell's connection pool to it via search_path — a
// cell's unqualified CREATE/SELECT/UPDATE/DELETE then resolves into its
// own private schema and cell A cannot see, list, or scan cell B's
// tables. This is for a FUTURE untrusted-cell scenario and is NOT safe
// for the current first-party shared-table deployment, so it is opt-in:
// set STORAGE_POSTGRES_ISOLATE=true to enable per-cell schemas.
//
// Schema scoping is done purely at the connection level (search_path), so no
// key-prefix / cell_id column threading is required. SQL beyond the documented
// adapter boundary must use portable SQL or native Postgres syntax.
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
// All cells share a single Postgres server (one DATABASE_URL). By
// default they also share one schema (public); with
// STORAGE_POSTGRES_ISOLATE=true each cell instead gets a pool pinned to
// its own schema. The connection string is read from the DATABASE_URL
// environment variable.
//
// Note: LastInsertID is always 0 on the Postgres backend (Postgres has
// no last-insert-id via database/sql) — cells must use RETURNING. This
// is a behavioural difference from the sqlite backend.
package postgresext

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	_ "github.com/lib/pq"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// manager owns per-cell *sql.DB handles. Setup runs once (cell name is
// empty there — see Pulp run.Main) so the pools are opened lazily on
// first Register with the manifest's cell name baked in. Each cell's
// pool is pinned to that cell's schema via search_path.
type pgManager struct {
	mu     sync.RWMutex
	dbs    map[ext.ResourceKey]*sql.DB
	setups map[pgApplicationKey]pgSetup

	// Legacy single-application configuration. Explicit application setups
	// never mutate or read another application's setup entry.
	dsn          string
	isolate      bool
	sharedSchema string
	logger       *slog.Logger
}

type pgApplicationKey struct {
	applicationID string
	instanceID    string
}

type pgSetup struct {
	dsn              string
	isolate          bool
	sharedSchema     string
	storageNamespace string
	logger           *slog.Logger
}

// defaultSharedSchema is the schema all cells share when isolation is
// not opted into. This preserves the pre-isolation production layout.
const defaultSharedSchema = "public"

func newPGManager() *pgManager {
	return &pgManager{
		dbs:    map[ext.ResourceKey]*sql.DB{},
		setups: map[pgApplicationKey]pgSetup{},
	}
}

var manager = newPGManager()

// ErrConnectionUnavailable means the requested scoped pool has not been
// registered, or has already been released by its scope teardown. It does not
// reveal whether another scope currently owns a pool.
var ErrConnectionUnavailable = errors.New("storage.postgres: scoped connection unavailable")

// ExistingConnection returns the already-open Postgres pool owned by exactly
// scope. It never reads configuration, opens a connection, or creates a
// schema. Callers must not close the returned pool: its lifecycle remains
// owned by this extension and the pool is released by the matching scope
// teardown.
//
// A scope can resolve only its exact resource key. In particular, an
// application, instance, cell, or cell-instance mismatch cannot fall back to
// another scope's pool.
func ExistingConnection(scope ext.Scope) (*sql.DB, error) {
	return manager.existingConnection(scope)
}

func init() {
	ext.Register(ext.Capability{
		Name:          "storage.sqlite", // same ABI surface as ext-sqlite
		Setup:         setup,
		Teardown:      teardown,
		TeardownScope: teardownScope,
		Register:      bindActive,
		Stub:          bindStub,
		TeardownCell:  teardownCell,
	})
}

// setup captures the DSN and logger. It does NOT open a pool — Pulp
// calls Setup once with an empty CellName, so a pool opened here could
// not be pinned to a cell's schema. Pools are opened lazily from
// Register() once the cell identity is known.
func setup(env ext.SetupEnv) error { return manager.setup(env) }

func (m *pgManager) setup(env ext.SetupEnv) error {
	scope := env.EffectiveScope()
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("storage.postgres: invalid setup scope: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMapsLocked()

	logger := env.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("storage.postgres: DATABASE_URL not set")
	}

	// Per-cell isolation is OPT-IN. The default preserves the first-party
	// shared-table deployment (Evolution writes / Sessions-Gene reads the
	// same tables) and the existing prod data in `public`.
	isolate := isTrue(os.Getenv("STORAGE_POSTGRES_ISOLATE"))

	// In shared mode, all cells live in one schema: STORAGE_POSTGRES_SHARED_SCHEMA
	// if set, else `public`. This value is unused when isolation is on.
	shared := sanitizeSchema(os.Getenv("STORAGE_POSTGRES_SHARED_SCHEMA"))
	if shared == "" {
		shared = defaultSharedSchema
	}
	setup := pgSetup{
		dsn:              dsn,
		isolate:          isolate,
		sharedSchema:     shared,
		storageNamespace: storageNamespaceFromRoot(scope, env.StorageRoot),
		logger:           logger,
	}
	owner := pgApplicationScopeKey(scope)
	if existing, ok := m.setups[owner]; ok {
		if existing.dsn != setup.dsn || existing.isolate != setup.isolate || existing.sharedSchema != setup.sharedSchema || existing.storageNamespace != setup.storageNamespace {
			return fmt.Errorf("storage.postgres: application %s/%s setup already owns its database configuration; refusing replacement", owner.applicationID, owner.instanceID)
		}
		return nil
	}
	m.setups[owner] = setup
	if scope.IsLegacy() {
		m.dsn, m.isolate, m.sharedSchema, m.logger = setup.dsn, setup.isolate, setup.sharedSchema, setup.logger
	}
	if m.logger == nil {
		m.logger = logger
	}

	// Log only host/dbname — never substring the raw DSN, which can
	// expose username/password.
	if isolate {
		logger.Info("storage.postgres setup", "endpoint", dsnEndpoint(dsn), "mode", "per-cell-isolated", "application", owner.applicationID, "instance", owner.instanceID)
	} else {
		logger.Info("storage.postgres setup", "endpoint", dsnEndpoint(dsn), "mode", "shared-schema", "schema", shared, "application", owner.applicationID, "instance", owner.instanceID)
	}
	return nil
}

// teardown closes every open per-cell pool. Safe to call more than once
// — closed handles are removed from the map.
func teardown(_ context.Context) error {
	return manager.teardown()
}

// teardown retains the single-app cleanup behavior. Scoped application pools
// are released only by TeardownScope so one application cannot stop another.
func (m *pgManager) teardown() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for name, db := range m.dbs {
		if !name.Scope().IsLegacy() {
			continue
		}
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", name, err)
		}
		delete(m.dbs, name)
	}
	delete(m.setups, pgApplicationScopeKey(ext.LegacyScope("host")))
	m.dsn = ""
	return first
}

func teardownScope(_ context.Context, scope ext.Scope) error { return manager.teardownScope(scope) }

func (m *pgManager) teardownScope(scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("storage.postgres: invalid teardown scope: %w", err)
	}
	owner := pgApplicationScopeKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for key, db := range m.dbs {
		if pgApplicationScopeKey(key.Scope()) != owner {
			continue
		}
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", key, err)
		}
		delete(m.dbs, key)
	}
	delete(m.setups, owner)
	return first
}

// teardownCell closes just one cell's pool during a per-cell
// control-socket shutdown, leaving other cells untouched. The cell's
// schema and data are left intact in Postgres (a stopped cell may be
// restarted); only the cached connection pool is released.
func teardownCell(_ context.Context, cellID string) error {
	return manager.closeCellTarget(cellID)
}

// schemaFor resolves which Postgres schema a cell's pool is pinned to.
// Default (isolate=false): the shared schema, so all cells resolve
// unqualified names into the same schema and first-party cells can read
// each other's tables. Opt-in (isolate=true): a per-cell schema derived
// from the cell name, giving each cell a private namespace.
func (m *pgManager) schemaFor(cellID string) string {
	if !m.isolate {
		return m.sharedSchema
	}
	s := sanitizeSchema(cellID)
	if s == "" {
		s = "cell"
	}
	return "cell_" + s
}

func pgApplicationScopeKey(scope ext.Scope) pgApplicationKey {
	return pgApplicationKey{applicationID: scope.ApplicationID(), instanceID: scope.ApplicationInstanceID()}
}

func (m *pgManager) ensureMapsLocked() {
	if m.dbs == nil {
		m.dbs = make(map[ext.ResourceKey]*sql.DB)
	}
	if m.setups == nil {
		m.setups = make(map[pgApplicationKey]pgSetup)
	}
}

func storageNamespaceFromRoot(scope ext.Scope, storageRoot string) string {
	if scope.IsLegacy() || storageRoot == "" {
		return ""
	}
	// Pulp's multi-app runtime supplies <root>/<storage_namespace>/<instance>.
	// The namespace is used only as an additional schema-name component; the
	// application and instance identity remain mandatory, so an unusual direct
	// caller cannot make two applications alias by choosing the same path.
	if filepath.Base(filepath.Clean(storageRoot)) != scope.ApplicationInstanceID() {
		return ""
	}
	return sanitizeSchema(filepath.Base(filepath.Dir(filepath.Clean(storageRoot))))
}

func (m *pgManager) setupForScopeLocked(scope ext.Scope) pgSetup {
	if setup, ok := m.setups[pgApplicationScopeKey(scope)]; ok {
		return setup
	}
	return pgSetup{dsn: m.dsn, isolate: m.isolate, sharedSchema: m.sharedSchema, logger: m.logger}
}

func (m *pgManager) loggerForScope(scope ext.Scope) *slog.Logger {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.setupForScopeLocked(scope).logger
}

func schemaName(prefix string, parts ...string) string {
	var raw strings.Builder
	for _, part := range parts {
		raw.WriteString(part)
		raw.WriteByte(0)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(raw.String())))[:12]
	readable := sanitizeSchema(strings.Join(parts, "_"))
	if readable == "" {
		readable = "scope"
	}
	maxReadable := 63 - len(prefix) - len(digest) - 2
	if len(readable) > maxReadable {
		readable = readable[:maxReadable]
	}
	return prefix + "_" + readable + "_" + digest
}

// schemaForScope resolves a scope's database schema. Shared mode deliberately
// preserves the configured shared schema for both legacy and explicit Pulp
// scopes so existing shared-table deployments continue to work while physical
// schema isolation remains opt-in. Isolation gives each explicit cell instance
// its own schema.
func (m *pgManager) schemaForScope(scope ext.Scope) string {
	m.mu.RLock()
	setup := m.setupForScopeLocked(scope)
	m.mu.RUnlock()
	if !setup.isolate {
		return setup.sharedSchema
	}
	if scope.IsLegacy() {
		return legacyCellSchema(scope.CellID())
	}
	parts := []string{scope.ApplicationID(), scope.ApplicationInstanceID(), setup.storageNamespace}
	parts = append(parts, scope.CellID(), scope.CellInstanceID())
	return schemaName("cell", parts...)
}

func legacyCellSchema(cellID string) string {
	s := sanitizeSchema(cellID)
	if s == "" {
		s = "cell"
	}
	return "cell_" + s
}

// openForCell opens a connection pool pinned to the cell's schema via
// search_path, creating the schema if it does not exist, and caches the
// handle. Idempotent — returns the cached *sql.DB on subsequent calls.
// openLegacyForCell retains the former implementation as a narrow legacy
// helper while openForCell below routes normal callers through full scopes.
func (m *pgManager) openLegacyForCell(cellID string) (*sql.DB, error) {
	key, err := pgKey(ext.LegacyScope(cellID))
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	if db, ok := m.dbs[key]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the write lock — another caller may have raced us.
	if db, ok := m.dbs[key]; ok {
		return db, nil
	}
	if m.dsn == "" {
		return nil, fmt.Errorf("storage.postgres: setup not called before register")
	}

	schema := m.schemaFor(cellID)

	// Create the schema unless it is `public`, which always exists and
	// whose creation a least-privilege managed-Postgres role may not be
	// allowed to run (CREATE SCHEMA needs CREATE on the database). The
	// shared-schema default targets `public`, so the common path issues
	// no DDL and imposes no new privilege requirement. Per-cell schemas
	// (and any non-public shared schema) are created on a short-lived
	// pool that is NOT pinned to them — a pinned connection's search_path
	// would otherwise point at a not-yet-existing schema.
	if schema != "public" {
		bootstrap, err := sql.Open("postgres", m.dsn)
		if err != nil {
			return nil, fmt.Errorf("open bootstrap: %w", err)
		}
		if _, err := bootstrap.Exec(`CREATE SCHEMA IF NOT EXISTS "` + schema + `"`); err != nil {
			bootstrap.Close()
			return nil, fmt.Errorf("create schema %q: %w", schema, err)
		}
		bootstrap.Close()
	}

	// Pin the cell's pool to its schema. For the default `public` schema we
	// DO NOT set search_path: public is already first on Postgres's default
	// search_path, so pinning it is redundant — AND a managed pooler
	// (Crunchy Bridge / pgbouncer) REJECTS the libpq startup `options`
	// parameter ("unsupported startup parameter in options: search_path"),
	// which crash-loops every shared-mode cell behind the pooler. Only a
	// non-public schema (per-cell isolation, or a custom shared schema)
	// needs the explicit pin. (Isolation behind pgbouncer would still need a
	// post-connect `SET search_path`; it's opt-in and unused in shared prod.)
	dsn, err := dsnForSchema(m.dsn, schema)
	if err != nil {
		return nil, fmt.Errorf("build dsn: %w", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	// storage.sqlite exposes transaction control as separate host calls.
	// One connection is required per cell scope; otherwise BEGIN/COMMIT/
	// ROLLBACK can land on different pooled sessions.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	// Bound connection lifetime so connections recycled / killed
	// server-side by a managed pooler (Crunchy Bridge / pgbouncer,
	// failover, idle reaper) are not handed out dead.
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	m.dbs[key] = db
	if m.logger != nil {
		m.logger.Info("storage.postgres ready", "cell", cellID, "schema", schema)
	}
	return db, nil
}

func (m *pgManager) get(cellID string) (*sql.DB, bool) {
	key, err := pgKey(ext.LegacyScope(cellID))
	if err != nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbs[key]
	return db, ok
}

const (
	postgresResourceType = "storage.postgres"
	postgresResourceID   = "database"
)

func pgKey(scope ext.Scope) (ext.ResourceKey, error) {
	if err := scope.Validate(); err != nil {
		return ext.ResourceKey{}, fmt.Errorf("storage.postgres: invalid scope: %w", err)
	}
	return scope.ResourceKey(postgresResourceType, postgresResourceID)
}

func schemaForSetup(scope ext.Scope, setup pgSetup) string {
	if !setup.isolate {
		return setup.sharedSchema
	}
	if scope.IsLegacy() {
		return legacyCellSchema(scope.CellID())
	}
	parts := []string{scope.ApplicationID(), scope.ApplicationInstanceID(), setup.storageNamespace}
	return schemaName("cell", append(parts, scope.CellID(), scope.CellInstanceID())...)
}

func (m *pgManager) openForCell(cellID string) (*sql.DB, error) {
	return m.openForScope(ext.LegacyScope(cellID))
}

func (m *pgManager) openForScope(scope ext.Scope) (*sql.DB, error) {
	if scope.IsLegacy() {
		return m.openLegacyForCell(scope.CellID())
	}
	key, err := pgKey(scope)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	if db, ok := m.dbs[key]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMapsLocked()
	if db, ok := m.dbs[key]; ok {
		return db, nil
	}
	setup := m.setupForScopeLocked(scope)
	if setup.dsn == "" {
		return nil, fmt.Errorf("storage.postgres: setup not called before register")
	}
	schema := schemaForSetup(scope, setup)
	if schema == "" {
		return nil, fmt.Errorf("storage.postgres: empty schema for scope %q", scope.RoutingID())
	}
	if schema != "public" {
		bootstrap, err := sql.Open("postgres", setup.dsn)
		if err != nil {
			return nil, fmt.Errorf("open bootstrap: %w", err)
		}
		if _, err := bootstrap.Exec(`CREATE SCHEMA IF NOT EXISTS "` + schema + `"`); err != nil {
			_ = bootstrap.Close()
			return nil, fmt.Errorf("create schema %q: %w", schema, err)
		}
		_ = bootstrap.Close()
	}
	dsn, err := dsnForSchema(setup.dsn, schema)
	if err != nil {
		return nil, fmt.Errorf("build dsn: %w", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	// Host-level transaction calls require one connection for this scope.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	m.dbs[key] = db
	if setup.logger != nil {
		setup.logger.Info("storage.postgres ready", "scope", key, "schema", schema)
	}
	return db, nil
}

func (m *pgManager) getForScope(scope ext.Scope) (*sql.DB, bool) {
	key, err := pgKey(scope)
	if err != nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbs[key]
	return db, ok
}

func (m *pgManager) existingConnection(scope ext.Scope) (*sql.DB, error) {
	key, err := pgKey(scope)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	db, ok := m.dbs[key]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrConnectionUnavailable
	}
	return db, nil
}

func (m *pgManager) closeCellTarget(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, db := range m.dbs {
		if !key.Scope().IsLegacy() && key.Scope().RoutingID() == target {
			return m.closeKeyLocked(key, db)
		}
	}
	key, err := pgKey(ext.LegacyScope(target))
	if err != nil {
		return err
	}
	if db, ok := m.dbs[key]; ok {
		return m.closeKeyLocked(key, db)
	}
	return nil
}

func (m *pgManager) closeKeyLocked(key ext.ResourceKey, db *sql.DB) error {
	delete(m.dbs, key)
	if err := db.Close(); err != nil {
		return fmt.Errorf("close %s: %w", key, err)
	}
	if logger := m.setupForScopeLocked(key.Scope()).logger; logger != nil {
		logger.Info("storage.postgres teardown cell", "scope", key)
	}
	return nil
}

// isTrue reports whether an env-var value means "enabled". Accepts the
// common truthy spellings so deploys can use 1/true/yes/on.
func isTrue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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

// dsnForSchema returns a DSN suitable for the requested schema. `public` is
// already first on PostgreSQL's default search_path, so it intentionally keeps
// the original DSN: managed poolers reject libpq startup `options` parameters.
func dsnForSchema(dsn, schema string) (string, error) {
	if schema == defaultSharedSchema {
		return dsn, nil
	}
	return dsnWithSearchPath(dsn, schema)
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
	// If the DSN already contains an `options=` keyword, appending a
	// second one would be silently ignored by libpq (first wins). Detect
	// and return an error so misconfigured DSNs are loud rather than
	// silently broken.
	if strings.Contains(dsn, "options=") {
		return "", fmt.Errorf("storage.postgres: keyword/value DSN already contains 'options='; cannot append search_path — use a URL-form DSN instead")
	}
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
	scope, err := ext.ValidatedScopeOf(cell)
	if err != nil {
		return fmt.Errorf("storage.postgres: resolve cell scope: %w", err)
	}
	// Open eagerly so a misconfigured DSN / unreachable server fails at
	// cell load, not on the first query, and the cell's schema is
	// created up front. Errors here abort cell registration.
	if _, err := manager.openForScope(scope); err != nil {
		return fmt.Errorf("storage.postgres: open for scope %q: %w", scope.RoutingID(), err)
	}
	exec := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
		return pgExec(ctx, m, scope, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut)
	}
	query := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
		return pgQuery(ctx, m, scope, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut)
	}
	b.NewFunctionBuilder().WithFunc(exec).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(query).Export("sqlite_query")
	if logger := manager.loggerForScope(scope); logger != nil {
		logger.Info("storage.postgres bound", "scope", scope.RoutingID())
	}
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop6 := func(_ context.Context, _ api.Module, _, _, _, _, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_query")
	return nil
}

// ---- handlers -------------------------------------------------------------

func pgExec(ctx context.Context, m api.Module, scope ext.Scope, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
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
	db, ok := manager.getForScope(scope)
	if !ok {
		return 9
	}

	statement, err := rewriteSQLiteSQL(string(q), len(args))
	if err != nil {
		encoded, mErr := msgpack.Marshal(ExecResult{Error: err.Error()})
		if mErr != nil {
			return 5
		}
		_ = writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
		return 5
	}

	res, err := db.ExecContext(ctx, statement, args...)
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
		if logger := manager.loggerForScope(scope); logger != nil {
			logger.Warn("postgres: RowsAffected failed", "err", raErr)
		}
	} else {
		out.RowsAffected = ra
	}
	// Postgres has no LastInsertId; out.LastInsertID stays 0 by design.
	// Callers that need the inserted ID should use a RETURNING clause instead.
	encoded, err := msgpack.Marshal(out)
	if err != nil {
		return 5
	}
	return writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
}

func pgQuery(ctx context.Context, m api.Module, scope ext.Scope, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
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
	db, ok := manager.getForScope(scope)
	if !ok {
		return 9
	}

	statement, err := rewriteSQLiteSQL(string(q), len(args))
	if err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}

	rows, err := db.QueryContext(ctx, statement, args...)
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
	case strings.Contains(msg, "duplicate key"),
		strings.Contains(msg, "foreign key constraint"),
		strings.Contains(msg, "not-null constraint"),
		strings.Contains(msg, "check constraint"):
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
