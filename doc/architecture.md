# Architecture Notes

## Runtime Layers

- `geocompare/interfaces`: CLI and GUI frontends.
- `geocompare/services`: service layer used by interfaces.
- `geocompare/engine`: orchestration and domain workflows.
- `geocompare/repository`: persistence and query acceleration.
- `geocompare/database`: ingest/build pipeline from Census source files.

## Persistence

- SQLite is the only runtime data backend (`bin/default.sqlite`).
- `SQLiteRepository` stores:
  - the full serialized data product payload (`data_products`)
  - query-optimized profile/geovector tables (`demographic_profiles`, `geovectors`)

## Schema Versioning and Migrations

- `schema_version` tracks one integer version row (`id=1`).
- `CURRENT_SCHEMA_VERSION` in `SQLiteRepository` is the source of truth.
- Startup flow:
  1. Create `schema_version` if missing.
  2. If missing row, initialize to current version.
  3. If older than current, apply step-based migrations in order.
  4. If newer than supported, fail fast with an explicit error.

This keeps upgrades deterministic and allows future non-breaking schema evolution.
