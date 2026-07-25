package postgresext

import (
	"strings"
	"testing"
)

// TestSchemaForSharedDefault is the regression guard for the
// Evolution↔Sessions-Gene shared-table read. With isolation NOT opted
// into (the default), every cell must resolve to the same schema so a
// cell's unqualified read sees another cell's writes.
func TestSchemaForSharedDefault(t *testing.T) {
	m := &pgManager{isolate: false, sharedSchema: defaultSharedSchema}

	evo := m.schemaFor("evolution")
	sess := m.schemaFor("sessions")

	if evo != "public" || sess != "public" {
		t.Fatalf("shared default: want both in public, got evolution=%q sessions=%q", evo, sess)
	}
	if evo != sess {
		t.Fatalf("shared default: cells must share a schema so Sessions-Gene reads Evolution's game_visibility; got %q vs %q", evo, sess)
	}
}

// TestSchemaForSharedCustom verifies a non-public shared schema still
// places all cells together.
func TestSchemaForSharedCustom(t *testing.T) {
	m := &pgManager{isolate: false, sharedSchema: "app"}
	if got := m.schemaFor("evolution"); got != "app" {
		t.Fatalf("custom shared: want app, got %q", got)
	}
	if got := m.schemaFor("sessions"); got != "app" {
		t.Fatalf("custom shared: want app, got %q", got)
	}
}

// TestSchemaForIsolateOptIn verifies the isolation capability is still
// available and gives each cell its own private schema.
func TestSchemaForIsolateOptIn(t *testing.T) {
	m := &pgManager{isolate: true, sharedSchema: defaultSharedSchema}

	evo := m.schemaFor("evolution")
	sess := m.schemaFor("sessions")

	if evo != "cell_evolution" {
		t.Fatalf("isolate: want cell_evolution, got %q", evo)
	}
	if sess != "cell_sessions" {
		t.Fatalf("isolate: want cell_sessions, got %q", sess)
	}
	if evo == sess {
		t.Fatalf("isolate: schemas must be distinct, both = %q", evo)
	}
}

func TestSchemaForIsolateEmptyName(t *testing.T) {
	m := &pgManager{isolate: true}
	if got := m.schemaFor(""); got != "cell_cell" {
		t.Fatalf("isolate empty cell name: want cell_cell, got %q", got)
	}
}

func TestIsTrue(t *testing.T) {
	on := []string{"1", "true", "TRUE", "Yes", " on "}
	for _, s := range on {
		if !isTrue(s) {
			t.Errorf("isTrue(%q) = false, want true", s)
		}
	}
	off := []string{"", "0", "false", "no", "off", "public"}
	for _, s := range off {
		if isTrue(s) {
			t.Errorf("isTrue(%q) = true, want false", s)
		}
	}
}

func TestDSNForSchemaEnablesOneExchangeParameterizedQueries(t *testing.T) {
	tests := []struct {
		name   string
		dsn    string
		schema string
		want   string
	}{
		{
			name:   "url shared schema",
			dsn:    "postgres://user:password@example.test:5432/app?sslmode=require",
			schema: "public",
			want:   "binary_parameters=yes",
		},
		{
			name:   "url custom schema preserves search path",
			dsn:    "postgres://user:password@example.test/app",
			schema: "app",
			want:   "binary_parameters=yes",
		},
		{
			name:   "keyword DSN",
			dsn:    "host=example.test dbname=app sslmode=require",
			schema: "public",
			want:   "binary_parameters=yes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dsnForSchema(tt.dsn, tt.schema)
			if err != nil {
				t.Fatalf("dsnForSchema: %v", err)
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("dsnForSchema() = %q, want %q", got, tt.want)
			}
			if tt.schema != "public" && !strings.Contains(got, "search_path") {
				t.Fatalf("dsnForSchema(%q) lost schema path: %q", tt.schema, got)
			}
		})
	}
}
