package postgresext

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

type countingConnector struct{ connects atomic.Int32 }

func (c *countingConnector) Connect(context.Context) (driver.Conn, error) {
	c.connects.Add(1)
	return nil, errors.New("unexpected database connection")
}

func (*countingConnector) Driver() driver.Driver { return countingDriver{} }

type countingDriver struct{}

func (countingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected database connection")
}

func testScope(t *testing.T, app, instance, cell, cellInstance string) ext.Scope {
	t.Helper()
	scope, err := ext.NewScope(app, instance, cell, cellInstance)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func testSetup(namespace string, isolate bool) pgSetup {
	return pgSetup{
		dsn:              "postgres://invalid.invalid/test?sslmode=disable",
		isolate:          isolate,
		sharedSchema:     defaultSharedSchema,
		storageNamespace: namespace,
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestScopedSchemaOwnership is hermetic: it proves the decision before any
// network connection is attempted. Shared mode keeps Evolution's cooperating
// cells together, while same-named cells in another app/instance have a
// separate schema. Isolation adds the complete cell placement to that key.
func TestScopedSchemaOwnership(t *testing.T) {
	m := newPGManager()
	evolution := testScope(t, "evolution", "blue", "sessions", "primary")
	evolutionOtherCell := testScope(t, "evolution", "blue", "commerce", "primary")
	sessions := testScope(t, "sessions", "blue", "sessions", "primary")
	green := testScope(t, "evolution", "green", "sessions", "primary")
	m.setups[pgApplicationScopeKey(evolution)] = testSetup("evolution-store", false)
	m.setups[pgApplicationScopeKey(sessions)] = testSetup("sessions-store", false)
	m.setups[pgApplicationScopeKey(green)] = testSetup("evolution-store", false)

	if got, want := m.schemaForScope(evolutionOtherCell), m.schemaForScope(evolution); got != want {
		t.Fatalf("shared app cells chose different schemas: %q != %q", got, want)
	}
	for _, other := range []ext.Scope{sessions, green} {
		if got, want := m.schemaForScope(other), m.schemaForScope(evolution); got == want {
			t.Fatalf("application isolation breach: %q and %q both use %q", evolution.RoutingID(), other.RoutingID(), got)
		}
	}

	m.setups[pgApplicationScopeKey(evolution)] = testSetup("evolution-store", true)
	cellTwo := testScope(t, "evolution", "blue", "sessions", "secondary")
	if got, want := m.schemaForScope(cellTwo), m.schemaForScope(evolution); got == want {
		t.Fatalf("isolated cell instances chose shared schema %q", got)
	}
}

// TestTeardownScopeLeavesOtherApplicationAlive proves that a host stopping
// Evolution does not close Sessions' independently owned pool. sql.Open does
// not connect, so this test never reaches a real DSN or database.
func TestTeardownScopeLeavesOtherApplicationAlive(t *testing.T) {
	m := newPGManager()
	a := testScope(t, "evolution", "blue", "storage", "primary")
	b := testScope(t, "sessions", "blue", "storage", "primary")
	ka, err := pgKey(a)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := pgKey(b)
	if err != nil {
		t.Fatal(err)
	}
	da, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("postgres", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m.dbs[ka], m.dbs[kb] = da, db
	m.setups[pgApplicationScopeKey(a)] = testSetup("evolution-store", false)
	m.setups[pgApplicationScopeKey(b)] = testSetup("sessions-store", false)

	if err := m.teardownScope(a); err != nil {
		t.Fatalf("teardown evolution: %v", err)
	}
	if _, ok := m.getForScope(a); ok {
		t.Fatal("evolution pool survived its scoped teardown")
	}
	if got, ok := m.getForScope(b); !ok || got != db {
		t.Fatal("sessions pool was changed by evolution teardown")
	}
	if _, err := da.Conn(t.Context()); err == nil {
		t.Fatal("closed evolution pool accepted a connection")
	}
}

// TestScopedLifecycleRace exercises concurrent readers and independent app
// teardown. It is intentionally connection-free; `go test -race` verifies
// manager ownership maps and setup state remain synchronized.
func TestScopedLifecycleRace(t *testing.T) {
	m := newPGManager()
	a := testScope(t, "evolution", "blue", "storage", "primary")
	b := testScope(t, "sessions", "blue", "storage", "primary")
	ka, _ := pgKey(a)
	kb, _ := pgKey(b)
	da, _ := sql.Open("postgres", "")
	db, _ := sql.Open("postgres", "")
	t.Cleanup(func() { _ = da.Close(); _ = db.Close() })
	m.dbs[ka], m.dbs[kb] = da, db
	m.setups[pgApplicationScopeKey(a)] = testSetup("evolution-store", false)
	m.setups[pgApplicationScopeKey(b)] = testSetup("sessions-store", false)

	var readers sync.WaitGroup
	for range 32 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 200 {
				_, _ = m.getForScope(a)
				_, _ = m.getForScope(b)
				_ = m.schemaForScope(a)
				_ = m.schemaForScope(b)
			}
		}()
	}
	if err := m.teardownScope(a); err != nil {
		t.Fatalf("teardown evolution: %v", err)
	}
	readers.Wait()
	if _, ok := m.getForScope(b); !ok {
		t.Fatal("sessions pool was removed during concurrent evolution teardown")
	}
}

// TestExistingConnectionIsExactAndConnectionFree proves the archive-facing
// accessor can borrow only its own already-managed pool. It must not turn a
// missed scope into a new sql.Open/Ping path, which would bypass manager
// ownership and create a second pool.
func TestExistingConnectionIsExactAndConnectionFree(t *testing.T) {
	m := newPGManager()
	owner := testScope(t, "evolution", "blue", "archive", "primary")
	other := testScope(t, "sessions", "blue", "archive", "primary")
	key, err := pgKey(owner)
	if err != nil {
		t.Fatal(err)
	}
	connector := &countingConnector{}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	m.dbs[key] = db

	previous := manager
	manager = m
	t.Cleanup(func() { manager = previous })

	got, err := ExistingConnection(owner)
	if err != nil {
		t.Fatalf("resolve owning scope: %v", err)
	}
	if got != db {
		t.Fatal("resolve owning scope returned a different pool")
	}
	if got, err := ExistingConnection(other); !errors.Is(err, ErrConnectionUnavailable) || got != nil {
		t.Fatalf("cross-scope resolve = (%p, %v), want (nil, ErrConnectionUnavailable)", got, err)
	}
	if calls := connector.connects.Load(); calls != 0 {
		t.Fatalf("existing connection accessor opened %d database connections", calls)
	}
}
