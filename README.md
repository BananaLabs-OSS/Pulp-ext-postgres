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

## Per-cell isolation

ext-sqlite gives each cell its own database file, so a cell physically cannot touch another cell's data. This extension reproduces that isolation on a shared Postgres server: each declaring cell gets its own Postgres schema (`cell_<sanitized-cell-name>`), and that cell's connection pool is pinned to it via `search_path`. A cell's unqualified `CREATE`/`SELECT`/`UPDATE`/`DELETE`/`DROP` resolves into its own private schema, so cell A cannot see, list, or scan cell B's tables. The host never parses or rewrites cell SQL — scoping is purely at the connection level.

The schema is created automatically (`CREATE SCHEMA IF NOT EXISTS`) on first cell load. The connection string user therefore needs `CREATE` on the database.

### Shared-schema mode (opt-out of isolation)

Some deployments intentionally share tables across cells — e.g. the Evolution engine writes `game_visibility` and the Sessions gene reads it from the same table. To place **all** cells in one schema instead of per-cell schemas, set:

```
STORAGE_POSTGRES_SHARED_SCHEMA=public
```

This is an explicit opt-out of isolation. Leave it unset for the safe per-cell-isolated default.

### Migrating existing data

Data written before this change lives in whatever schema the pre-isolation pool used (typically `public`). After upgrading:

- **Shared deployments** (Evolution + Sessions-Gene today): set `STORAGE_POSTGRES_SHARED_SCHEMA=public` to keep using the existing tables unchanged — no data migration needed.
- **New isolation**: a cell's first run starts with an empty private schema. To carry forward existing tables, move them with `ALTER TABLE public.<t> SET SCHEMA cell_<cell>;` per cell, or repopulate.

## Notes

- `LastInsertID` is always `0` on the Postgres backend (Postgres has no last-insert-id via `database/sql`). Cells must use `RETURNING` — a behavioural difference from the sqlite backend.
