# Pulp-ext-postgres

Postgres storage capability for Pulp cells. Drop-in replacement for [Pulp-ext-sqlite](https://github.com/BananaLabs-OSS/Pulp-ext-sqlite) backed by a shared Postgres connection via [lib/pq](https://github.com/lib/pq).

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-postgres"
```

Requires the `DATABASE_URL` environment variable set to a valid Postgres connection string.

## Capability

- `storage.sqlite` — registers the same `sqlite_exec` and `sqlite_query` host imports as ext-sqlite so existing cell WASM binaries work without recompilation. The capability name is an ABI compatibility surface, not a backend indicator. Cell-side code switches to Postgres dialect (`pgdialect.New()`) to emit `$1` params, `RETURNING`, etc. The host executes whatever SQL it receives.

## Schema scoping

### Shared schema (default)

By default **all cells share one Postgres schema** (`public`), exactly as the pre-isolation pool did. This is the safe, no-migration default because the platform's first-party cells intentionally share tables: the Evolution engine *writes* `game_visibility`/`tier`/`server` and the Sessions gene *reads* them via unqualified queries against the **same** table. Isolating those cells into separate schemas would make the gene's reads resolve into an empty schema (blank game names, `price = 0`, wrong capacity), so shared is the correct default for this deployment.

No configuration is required. Optionally point the shared schema somewhere other than `public`:

```
STORAGE_POSTGRES_SHARED_SCHEMA=myschema
```

When the shared schema is `public` (the default) the host issues **no** DDL on cell load, so the connection role needs no `CREATE` privilege. A non-`public` shared schema is auto-created (`CREATE SCHEMA IF NOT EXISTS`) and so requires `CREATE` on the database.

### Per-cell isolation (opt-in)

ext-sqlite gives each cell its own database file, so a cell physically cannot touch another cell's data. This extension can reproduce that isolation on a shared Postgres server by giving each declaring cell its own schema (`cell_<sanitized-cell-name>`) and pinning that cell's pool to it via `search_path`, so a cell's unqualified `CREATE`/`SELECT`/`UPDATE`/`DELETE`/`DROP` resolves only into its own private schema and cell A cannot see, list, or scan cell B's tables.

This is intended for a **future untrusted-cell scenario** — it is **not** safe for the current first-party shared-table design (it breaks the Evolution↔Sessions-Gene read). Enable it explicitly:

```
STORAGE_POSTGRES_ISOLATE=true
```

Per-cell schemas are auto-created on first cell load, so the connection role needs `CREATE` on the database. To carry forward existing `public` tables into a cell's private schema, move them with `ALTER TABLE public.<t> SET SCHEMA cell_<cell>;` per cell, or repopulate.

The host never parses or rewrites cell SQL in either mode — scoping is purely at the connection level (`search_path`).

## Notes

- `LastInsertID` is always `0` on the Postgres backend (Postgres has no last-insert-id via `database/sql`). Cells must use `RETURNING` — a behavioural difference from the sqlite backend.
