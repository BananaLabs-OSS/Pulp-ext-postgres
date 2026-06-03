package postgresext

import "testing"

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
