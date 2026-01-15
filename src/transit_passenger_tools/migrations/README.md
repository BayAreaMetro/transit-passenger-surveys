# Database Migrations

This directory contains SQL migration files for the transit passenger survey database.

## Naming Convention

Migration files must follow the pattern: `NNN_description.sql`

- `NNN`: Three-digit sequential number (001, 002, 003, etc.)
- `description`: Brief description using underscores (e.g., `add_index`, `alter_weights_table`)
- Extension: `.sql`

## Examples

```
001_initial_tables.sql
002_add_station_index.sql
003_alter_weights_precision.sql
```

## How Migrations Work

1. Migrations run in alphanumeric order
2. Each migration is recorded in the `schema_migrations` table
3. Already-applied migrations are automatically skipped
4. Run migrations with: `db.run_migrations()`

## Creating a New Migration

1. Find the highest numbered migration file
2. Create a new file with the next number
3. Write your SQL (CREATE, ALTER, INSERT, etc.)
4. Run `initialize_duckdb.py` or call `db.run_migrations()` directly

## Notes

- Pure SQL files - no templating or variables
- Migrations are idempotent when possible (use `IF NOT EXISTS`, `OR REPLACE`, etc.)
- Views are managed separately in `db.create_views()` (not in migrations)
