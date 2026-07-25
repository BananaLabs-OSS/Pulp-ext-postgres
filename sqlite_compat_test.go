package postgresext

import (
	"strings"
	"testing"
)

func TestRewriteSQLiteSQL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		args int
		want string
	}{
		{
			name: "sqlite placeholders",
			in:   "SELECT * FROM orders WHERE id = ? AND state = ?",
			args: 2,
			want: "SELECT * FROM orders WHERE id = $1 AND state = $2",
		},
		{
			name: "native postgres parameters untouched",
			in:   "SELECT $2, $1, '?' AS literal, \"?\" AS identifier -- ?\n",
			args: 2,
			want: "SELECT $2, $1, '?' AS literal, \"?\" AS identifier -- ?\n",
		},
		{
			name: "opaque quoted and comment content",
			in:   "SELECT '?' AS s, \"BLOB?\" FROM t /* ? BLOB X'AB' */ WHERE id = ?",
			args: 1,
			want: "SELECT '?' AS s, \"BLOB?\" FROM t /* ? BLOB X'AB' */ WHERE id = $1",
		},
		{
			name: "dollar quoted postgres string",
			in:   "SELECT $tag$? BLOB X'AB'$tag$, ?",
			args: 1,
			want: "SELECT $tag$? BLOB X'AB'$tag$, $1",
		},
		{
			name: "sqlite ddl and byte literals",
			in:   "CREATE TABLE commerce (id INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB DEFAULT X'', seed BLOB DEFAULT X'cAFE')",
			args: 0,
			want: "CREATE TABLE commerce (id BIGSERIAL PRIMARY KEY , payload BYTEA DEFAULT decode('', 'hex'), seed BYTEA DEFAULT decode('cAFE', 'hex'))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rewriteSQLiteSQL(tt.in, tt.args)
			if err != nil {
				t.Fatalf("rewriteSQLiteSQL() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("rewriteSQLiteSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRewriteSQLiteSQLRejectsUnsafeOrAmbiguousSyntax(t *testing.T) {
	tests := []struct {
		name string
		in   string
		args int
		want string
	}{
		{"mixed parameters", "SELECT $1, ?", 1, "mixed SQLite"},
		{"wrong placeholder count", "SELECT ?, ?", 1, "placeholders"},
		{"numbered sqlite parameter", "SELECT ?1", 1, "numbered ?NNN"},
		{"json operator", "SELECT payload ?| array['a']", 0, "JSON ? operator"},
		{"insert or ignore", "INSERT OR IGNORE INTO identities(id) VALUES (?)", 1, "INSERT OR IGNORE"},
		{"invalid hex", "SELECT X'0'", 0, "invalid SQLite X'hex'"},
		{"pragma", "PRAGMA foreign_keys = ON", 0, "SQLite-only PRAGMA"},
		{"stray autoincrement", "CREATE TABLE t (id INT AUTOINCREMENT)", 0, "AUTOINCREMENT"},
		{"backticks", "SELECT `id` FROM t", 0, "backtick"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rewriteSQLiteSQL(tt.in, tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("rewriteSQLiteSQL() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
