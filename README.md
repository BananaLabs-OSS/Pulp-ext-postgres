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
