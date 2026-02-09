# Seeder

A PostgreSQL database seeder that introspects your schema, respects foreign keys, and inserts realistic mock data (up to 50 records per table).

## Setup

```bash
npm install
```

## Usage

```bash
npm run dev -- --connection "postgres://user:pass@localhost:5432/dbname"
```

Multiple schemas:

```bash
npm run dev -- --connection "postgres://user:pass@localhost:5432/dbname" --schemas "public,analytics"
```

Or with a config file:

```bash
npm run dev -- --config seeder.config.json
```

Preview counts without inserting:

```bash
npm run dev -- --connection "postgres://user:pass@localhost:5432/dbname" --dry-run
```

## Config

Create `seeder.config.json` at the project root:

```json
{
  "connectionString": "postgres://user:pass@localhost:5432/dbname",
  "schema": "public",
  "schemas": ["public", "analytics"],
  "maxRecords": 50,
  "seed": 1337,
  "dryRun": false,
  "includeTables": [],
  "excludeTables": ["schema_migrations"],
  "overrides": {
    "public.users.email": { "faker": "internet.email" },
    "public.users.status": { "values": ["active", "pending", "disabled"] }
  }
}
```

Notes:

- Existing data is preserved; the seeder only inserts enough rows to reach `maxRecords` per table.
- Foreign keys are enforced and parent tables are seeded first.
- Cyclic foreign key dependencies will error with a helpful message.
- Use `--dry-run` or `"dryRun": true` to preview counts without inserting data. Only works for single table insertions.
- `includeTables` and `excludeTables` accept bare table names or `schema.table`. If its empty, it will try to update all tables.
- `overrides` keys accept `schema.table.column`, `table.column`, or `column`.
- Unique constraints (single and multi-column) are honored by regenerating values on collisions when all columns are inserted.
