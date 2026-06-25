package postgresext

// Real-Postgres integration tests. These spin up an actual PostgreSQL
// server in-process via embedded-postgres (a vanilla upstream PG binary,
// no managed-vendor lock-in) and drive ext-postgres's connection manager
// against it. They cover what the hermetic schemaFor/isTrue unit tests
// structurally cannot: that the search_path-based multi-tenancy actually
// isolates (and, in shared mode, actually shares) DATA at the SQL level on
// a real engine, plus the constraint-error mapping and boolean round-trip
// that depend on Postgres wire behaviour rather than string logic.
//
// If the embedded server cannot start (no network to fetch the binary on a
// cold cache, a busy port, a restricted CI sandbox), every test Skips with
// a loud reason — it is never silently faked green. On a developer box with
// network it runs for real (verified against PostgreSQL 18.x).

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

// testDSN is the connection string to the shared embedded server, or ""
// if it could not be started (in which case the integration tests skip).
var testDSN string

func TestMain(m *testing.M) {
	runtimeDir, err := os.MkdirTemp("", "extpg-rt-")
	if err != nil {
		// Can't even make a temp dir — run tests (the real-PG ones skip).
		os.Exit(m.Run())
	}
	defer os.RemoveAll(runtimeDir)

	const port = 54330
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(port).
		RuntimePath(runtimeDir).
		Logger(io.Discard))

	if startErr := pg.Start(); startErr != nil {
		// Leave testDSN empty so integration tests skip with a clear reason.
		fmt.Fprintf(os.Stderr, "embedded postgres unavailable, real-PG tests will skip: %v\n", startErr)
		os.Exit(m.Run())
	}
	testDSN = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)

	code := m.Run()
	_ = pg.Stop()
	os.Exit(code)
}

// requirePG skips the calling test when the embedded server is unavailable.
func requirePG(t *testing.T) {
	t.Helper()
	if testDSN == "" {
		t.Skip("real-PG integration skipped: embedded postgres did not start (see TestMain log)")
	}
}

// newRealManager builds a pgManager pointed at the embedded server in the
// requested mode, with a discarding logger, and registers cleanup of every
// pool it opens.
func newRealManager(t *testing.T, isolate bool) *pgManager {
	t.Helper()
	m := &pgManager{
		dbs:          map[string]*sql.DB{},
		dsn:          testDSN,
		isolate:      isolate,
		sharedSchema: defaultSharedSchema,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	t.Cleanup(func() {
		for _, db := range m.dbs {
			_ = db.Close()
		}
	})
	return m
}

// TestRealPG_PerCellIsolation proves the OPT-IN isolation actually keeps a
// cell's table invisible to another cell on a real server: with
// STORAGE_POSTGRES_ISOLATE=true each cell's pool is search_path-pinned to
// its own schema, so cell A's unqualified CREATE lands in cell_a and cell
// B literally cannot resolve it.
func TestRealPG_PerCellIsolation(t *testing.T) {
	requirePG(t)
	m := newRealManager(t, true)

	evo, err := m.openForCell("evolution")
	if err != nil {
		t.Fatalf("openForCell(evolution): %v", err)
	}
	sess, err := m.openForCell("sessions")
	if err != nil {
		t.Fatalf("openForCell(sessions): %v", err)
	}

	// Schemas must be the per-cell ones and must exist on the server.
	if got := m.schemaFor("evolution"); got != "cell_evolution" {
		t.Fatalf("schemaFor(evolution) = %q, want cell_evolution", got)
	}
	for _, sc := range []string{"cell_evolution", "cell_sessions"} {
		var exists bool
		if err := evo.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`, sc,
		).Scan(&exists); err != nil {
			t.Fatalf("check schema %s: %v", sc, err)
		}
		if !exists {
			t.Fatalf("isolation: schema %s was not created on the server", sc)
		}
	}

	// Evolution creates an unqualified table; it lands in cell_evolution.
	if _, err := evo.Exec(`CREATE TABLE secrets (k text primary key, v text)`); err != nil {
		t.Fatalf("evolution CREATE TABLE: %v", err)
	}
	if _, err := evo.Exec(`INSERT INTO secrets (k, v) VALUES ('api', 'evo-only')`); err != nil {
		t.Fatalf("evolution INSERT: %v", err)
	}

	// Evolution can read its own row.
	var v string
	if err := evo.QueryRow(`SELECT v FROM secrets WHERE k = 'api'`).Scan(&v); err != nil {
		t.Fatalf("evolution SELECT own row: %v", err)
	}
	if v != "evo-only" {
		t.Fatalf("evolution read back %q, want evo-only", v)
	}

	// Sessions, pinned to cell_sessions, must NOT resolve evolution's table.
	var sink string
	err = sess.QueryRow(`SELECT v FROM secrets WHERE k = 'api'`).Scan(&sink)
	if err == nil {
		t.Fatal("ISOLATION BREACH: sessions read evolution's table across schemas")
	}
	// The error must be "relation does not exist", not a connection failure.
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("sessions query: want undefined-table error, got: %v", err)
	}
}

// TestRealPG_SharedSchemaReadThrough is the positive regression for the
// production default: with isolation OFF, Evolution and Sessions-Gene share
// `public`, so the gene's unqualified read sees the engine's write. This is
// the game_visibility contract that the hermetic schemaFor test only asserts
// at the naming level — here it is proven against real data.
func TestRealPG_SharedSchemaReadThrough(t *testing.T) {
	requirePG(t)
	m := newRealManager(t, false)

	evo, err := m.openForCell("evolution")
	if err != nil {
		t.Fatalf("openForCell(evolution): %v", err)
	}
	sess, err := m.openForCell("sessions")
	if err != nil {
		t.Fatalf("openForCell(sessions): %v", err)
	}

	if got := m.schemaFor("evolution"); got != "public" {
		t.Fatalf("shared mode schemaFor(evolution) = %q, want public", got)
	}

	// Use a uniquely named table so this test is independent of the others.
	t.Cleanup(func() { _, _ = evo.Exec(`DROP TABLE IF EXISTS game_visibility_rt`) })
	if _, err := evo.Exec(`CREATE TABLE IF NOT EXISTS game_visibility_rt (game text primary key, visible boolean)`); err != nil {
		t.Fatalf("evolution CREATE shared table: %v", err)
	}
	if _, err := evo.Exec(`INSERT INTO game_visibility_rt (game, visible) VALUES ('minecraft', true)`); err != nil {
		t.Fatalf("evolution INSERT shared row: %v", err)
	}

	// Sessions reads the engine's write through the shared schema.
	var visible bool
	if err := sess.QueryRow(`SELECT visible FROM game_visibility_rt WHERE game = 'minecraft'`).Scan(&visible); err != nil {
		t.Fatalf("sessions read shared table (the Evolution↔gene contract): %v", err)
	}
	if !visible {
		t.Fatal("sessions read visible=false, want true (shared write not observed)")
	}
}

// TestRealPG_ConstraintErrorCode proves pgErrorCode classifies a real
// Postgres unique violation as a constraint error (13), matching ext-sqlite,
// so cells can branch on duplicate-key without parsing the message. This
// only works against a real engine — the wire error text is what's mapped.
func TestRealPG_ConstraintErrorCode(t *testing.T) {
	requirePG(t)
	m := newRealManager(t, false)

	db, err := m.openForCell("evolution")
	if err != nil {
		t.Fatalf("openForCell: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS uniq_probe`) })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS uniq_probe (id int primary key)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO uniq_probe (id) VALUES (1)`); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, dupErr := db.Exec(`INSERT INTO uniq_probe (id) VALUES (1)`)
	if dupErr == nil {
		t.Fatal("duplicate insert unexpectedly succeeded")
	}
	if code := pgErrorCode(dupErr); code != 13 {
		t.Fatalf("pgErrorCode(unique violation) = %d, want 13 (constraint); err=%v", code, dupErr)
	}
}

// TestRealPG_BooleanRoundTrip guards the dialect boolean behaviour the cells
// rely on (e.g. destroy_email_sent = false predicates): a Go bool written
// through the pool reads back as the same bool on real Postgres, and a
// WHERE <bool> = false filter selects the right rows.
func TestRealPG_BooleanRoundTrip(t *testing.T) {
	requirePG(t)
	m := newRealManager(t, false)

	db, err := m.openForCell("evolution")
	if err != nil {
		t.Fatalf("openForCell: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE IF EXISTS bool_probe`) })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS bool_probe (id int primary key, sent boolean not null)`); err != nil {
		t.Fatalf("create bool table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO bool_probe (id, sent) VALUES (1, true), (2, false)`); err != nil {
		t.Fatalf("insert bools: %v", err)
	}

	var sent bool
	if err := db.QueryRow(`SELECT sent FROM bool_probe WHERE id = 1`).Scan(&sent); err != nil {
		t.Fatalf("read bool: %v", err)
	}
	if !sent {
		t.Fatal("round-trip: id=1 sent read false, want true")
	}

	// `WHERE sent = false` must select exactly the unsent row (id=2).
	var id int
	if err := db.QueryRow(`SELECT id FROM bool_probe WHERE sent = false`).Scan(&id); err != nil {
		t.Fatalf("bool predicate query: %v", err)
	}
	if id != 2 {
		t.Fatalf("WHERE sent=false selected id=%d, want 2", id)
	}
}

