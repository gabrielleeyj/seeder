import type { Client } from "pg";
import { faker } from "@faker-js/faker";
import { generateValue } from "./generate.js";
import { introspectTables, loadEnums } from "./introspect.js";
import type { ColumnInfo, Config, ForeignKeyInfo, TableInfo } from "./types.js";
import { quoteIdent, topologicalSort } from "./utils.js";

const DEFAULT_POOL_LIMIT = 5000;

type SeedSummary = {
  schema?: string;
  table: string;
  inserted: number;
  existing: number;
};

type UniqueConstraintSet = {
  name: string;
  columns: string[];
  indices: number[];
  set: Set<string>;
  pairQueue?: Array<[unknown, unknown]>;
};

export async function runSeeder(
  client: Client,
  config: Config,
): Promise<SeedSummary[]> {
  const schemas =
    config.schemas && config.schemas.length
      ? config.schemas
      : [config.schema ?? "public"];
  const summary: SeedSummary[] = [];

  const include = config.includeTables?.length
    ? new Set(config.includeTables)
    : null;
  const exclude = config.excludeTables?.length
    ? new Set(config.excludeTables)
    : null;

  const fkPoolCache = new Map<string, Array<Array<unknown>>>();

  for (const schema of schemas) {
    const enums = await loadEnums(client, schema);
    const allTables = await introspectTables(client, schema);
    const tables = allTables.filter((table) => {
      if (include && !matchesTableFilter(include, table.schema, table.name))
        return false;
      if (exclude && matchesTableFilter(exclude, table.schema, table.name))
        return false;
      return true;
    });

    const tableNameSet = new Set(tables.map((table) => table.name));
    const edges = new Map<string, Set<string>>();
    for (const table of tables) {
      for (const fk of table.fks) {
        if (fk.refSchema !== schema) continue;
        if (!tableNameSet.has(fk.refTable)) continue;
        const from = fk.refTable;
        const to = table.name;
        const set = edges.get(from) ?? new Set<string>();
        set.add(to);
        edges.set(from, set);
      }
    }

    const orderedNames = topologicalSort([...tableNameSet], edges);
    const orderedTables = orderedNames
      .map((name) => tables.find((table) => table.name === name))
      .filter((table): table is TableInfo => Boolean(table));

    for (const table of orderedTables) {
      console.log("seeding table", table.name);
      const stats = await seedTable(client, table, config, enums, fkPoolCache);
      summary.push({ ...stats, schema: table.schema });
    }
  }

  return summary;
}

async function seedTable(
  client: Client,
  table: TableInfo,
  config: Config,
  enums: Map<string, string[]>,
  fkPoolCache: Map<string, Array<Array<unknown>>>,
): Promise<SeedSummary> {
  const schemaName = quoteIdent(table.schema);
  const tableName = quoteIdent(table.name);
  const fullName = `${schemaName}.${tableName}`;

  const countResult = await client.query(
    `select count(*)::int as count from ${fullName}`,
  );
  const existing = countResult.rows[0]?.count ?? 0;
  const target = Math.max(0, config.maxRecords - existing);

  if (target === 0) {
    return { table: table.name, inserted: 0, existing };
  }

  const fkPools = await loadForeignKeyPools(
    client,
    table,
    fkPoolCache,
    config.maxRecords,
  );
  validateForeignKeyPools(table, fkPools);
  const fkColumns = new Set(table.fks.flatMap((fk) => fk.columns));

  const insertColumns = table.columns.filter((column) => {
    if (column.isIdentity || column.isGenerated) return false;
    if (column.columnDefault && !fkColumns.has(column.name)) return false;
    return true;
  });
  const uniqueConstraints = buildUniqueConstraintSets(table, insertColumns);
  await loadUniqueConstraintValues(client, table, uniqueConstraints);
  buildUniquePairQueues(table, fkPools, uniqueConstraints, target);

  if (insertColumns.length === 0) {
    if (!config.dryRun) {
      for (let i = 0; i < target; i += 1) {
        await client.query(`insert into ${fullName} default values`);
      }
    }
    return { table: table.name, inserted: target, existing };
  }

  if (!config.dryRun) {
    const batchSize = 100;
    for (let i = 0; i < target; i += batchSize) {
      const batchCount = Math.min(batchSize, target - i);
      const values: unknown[] = [];
      const rowsPlaceholders: string[] = [];

      for (let rowIndex = 0; rowIndex < batchCount; rowIndex += 1) {
        const rowValues = buildRowValues(
          table,
          insertColumns,
          fkPools,
          enums,
          config.overrides,
          uniqueConstraints,
        );
        const placeholders: string[] = [];
        for (let colIndex = 0; colIndex < insertColumns.length; colIndex += 1) {
          values.push(rowValues[colIndex]);
          placeholders.push(`$${values.length}`);
        }
        rowsPlaceholders.push(`(${placeholders.join(", ")})`);
      }

      const columnsSql = insertColumns
        .map((column) => quoteIdent(column.name))
        .join(", ");
      const sql = `insert into ${fullName} (${columnsSql}) values ${rowsPlaceholders.join(", ")}`;
      await client.query(sql, values);
    }
  }

  return { table: table.name, inserted: target, existing };
}

function matchesTableFilter(
  filter: Set<string>,
  schema: string,
  table: string,
): boolean {
  if (filter.has(table)) return true;
  return filter.has(`${schema}.${table}`);
}

function buildRowValues(
  table: TableInfo,
  insertColumns: TableInfo["columns"],
  fkPools: Map<string, Array<Array<unknown>>>,
  enums: Map<string, string[]>,
  overrides: Config["overrides"],
  uniqueConstraints: UniqueConstraintSet[],
): unknown[] {
  const fkMap = new Map<string, ForeignKeyInfo>();
  for (const fk of table.fks) {
    for (const column of fk.columns) {
      fkMap.set(column, fk);
    }
  }

  const attempts = uniqueConstraints.length > 0 ? 25 : 1;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const values: unknown[] = [];
    const presetValues = new Map<number, unknown>();
    for (const constraint of uniqueConstraints) {
      if (constraint.pairQueue && constraint.pairQueue.length > 0) {
        const pair = constraint.pairQueue.shift();
        if (pair) {
          presetValues.set(constraint.indices[0], pair[0]);
          presetValues.set(constraint.indices[1], pair[1]);
        }
      }
    }
    const fkAssignments = new Map<ForeignKeyInfo, Array<unknown> | null>();
    for (const fk of table.fks) {
      const poolKey = foreignKeyPoolKey(fk);
      const pool = fkPools.get(poolKey) ?? [];
      if (pool.length === 0) {
        fkAssignments.set(fk, null);
      } else {
        fkAssignments.set(fk, faker.helpers.arrayElement(pool));
      }
    }

    for (let colIndex = 0; colIndex < insertColumns.length; colIndex += 1) {
      const column = insertColumns[colIndex];
      if (presetValues.has(colIndex)) {
        values.push(presetValues.get(colIndex));
        continue;
      }
      const fk = fkMap.get(column.name);
      if (fk) {
        const assigned = fkAssignments.get(fk);
        if (!assigned) {
          if (column.isNullable) {
            values.push(null);
            continue;
          }
          throw new Error(
            `Foreign key pool empty for ${table.name}.${column.name}. ` +
              `Ensure parent table ${fk.refTable} has rows or relax nullability.`,
          );
        }
        const position = fk.columns.indexOf(column.name);
        values.push(assigned[position]);
        continue;
      }

      if (column.isNullable && faker.number.int({ min: 1, max: 100 }) <= 15) {
        values.push(null);
        continue;
      }

      const override = resolveOverride(
        overrides,
        table.schema,
        table.name,
        column.name,
      );
      values.push(generateValue(column, enums, override));
    }

    if (uniqueConstraints.length === 0) {
      return values;
    }

    const pending: Array<{ constraint: UniqueConstraintSet; key: string }> = [];
    let conflict = false;
    for (const constraint of uniqueConstraints) {
      const key = buildUniqueKeyFromRow(values, constraint.indices);
      if (key === null) continue;
      if (constraint.set.has(key)) {
        conflict = true;
        break;
      }
      pending.push({ constraint, key });
    }
    if (!conflict) {
      for (const item of pending) {
        item.constraint.set.add(item.key);
      }
      return values;
    }
  }

  const constraintNames = uniqueConstraints
    .map((constraint) => constraint.name)
    .join(", ");
  throw new Error(
    `Unable to generate unique values for ${table.schema}.${table.name}. Constraints: ${constraintNames}`,
  );
}

async function loadForeignKeyPools(
  client: Client,
  table: TableInfo,
  cache: Map<string, Array<Array<unknown>>>,
  maxRecords: number,
): Promise<Map<string, Array<Array<unknown>>>> {
  const pools = new Map<string, Array<Array<unknown>>>();
  const limit = Math.min(DEFAULT_POOL_LIMIT, Math.max(50, maxRecords * 5));

  for (const fk of table.fks) {
    const key = foreignKeyPoolKey(fk);
    if (cache.has(key)) {
      pools.set(key, cache.get(key) ?? []);
      continue;
    }

    const columnsSql = fk.refColumns.map(quoteIdent).join(", ");
    const sql = `select ${columnsSql} from ${quoteIdent(fk.refSchema)}.${quoteIdent(
      fk.refTable,
    )} order by ${columnsSql} limit ${limit}`;

    const result = await client.query(sql);
    const pool = result.rows.map((row) => fk.refColumns.map((col) => row[col]));
    cache.set(key, pool);
    pools.set(key, pool);
  }

  return pools;
}

function foreignKeyPoolKey(fk: ForeignKeyInfo): string {
  return `${fk.refSchema}.${fk.refTable}:${fk.refColumns.join(",")}`;
}

function resolveOverride(
  overrides: Config["overrides"],
  schema: string,
  table: string,
  column: string,
): Config["overrides"][string] | undefined {
  if (!overrides) return undefined;
  const fullKey = `${schema}.${table}.${column}`;
  const tableKey = `${table}.${column}`;
  return overrides[fullKey] ?? overrides[tableKey] ?? overrides[column];
}

function buildUniqueConstraintSets(
  table: TableInfo,
  insertColumnNames: String[],
): UniqueConstraintSet[] {
  const indexMap = new Map(
    insertColumnNames.map((name, index) => [name, index]),
  );
  const sets: UniqueConstraintSet[] = [];
  for (const constraint of table.uniqueConstraints) {
    if (constraint.columns.length === 0) continue;
    const indices: number[] = [];
    let missing = false;
    for (const column of constraint.columns) {
      const index = indexMap.get(column);
      if (index === undefined) {
        missing = true;
        break;
      }
      indices.push(index);
    }
    if (missing) continue;
    sets.push({
      name: constraint.name,
      columns: constraint.columns,
      indices,
      set: new Set(),
    });
  }
  return sets;
}

async function loadUniqueConstraintValues(
  client: Client,
  table: TableInfo,
  constraints: UniqueConstraintSet[],
): Promise<void> {
  for (const constraint of constraints) {
    const columnsSql = constraint.columns.map(quoteIdent).join(", ");
    const sql = `select ${columnsSql} from ${quoteIdent(
      table.schema,
    )}.${quoteIdent(table.name)}`;
    const result = await client.query(sql);
    for (const row of result.rows) {
      const key = buildUniqueKeyFromColumns(row, constraint.columns);
      if (key !== null) {
        constraint.set.add(key);
      }
    }
  }
}

function buildUniquePairQueues(
  table: TableInfo,
  fkPools: Map<string, Array<Array<unknown>>>,
  constraints: UniqueConstraintSet[],
  target: number,
): void {
  const fkColumnValues = new Map<string, unknown[]>();
  for (const fk of table.fks) {
    if (fk.columns.length !== 1) continue;
    const key = foreignKeyPoolKey(fk);
    const pool = fkPools.get(key) ?? [];
    const values = Array.from(
      new Set(pool.map((row) => row[0]).filter((value) => value !== null)),
    );
    fkColumnValues.set(fk.columns[0], values);
  }

  const maxPairs = Math.max(200, target * 5);
  for (const constraint of constraints) {
    if (constraint.columns.length !== 2) continue;
    const [colA, colB] = constraint.columns;
    const valuesA = fkColumnValues.get(colA);
    const valuesB = fkColumnValues.get(colB);
    if (!valuesA || !valuesB) continue;

    const total = valuesA.length * valuesB.length;
    if (total === 0 || total > maxPairs) continue;

    const pairs: Array<[unknown, unknown]> = [];
    for (const a of valuesA) {
      for (const b of valuesB) {
        const key = buildUniqueKeyFromRow([a, b], [0, 1]);
        if (key === null) continue;
        if (constraint.set.has(key)) continue;
        pairs.push([a, b]);
      }
    }
    shuffleInPlace(pairs);
    constraint.pairQueue = pairs;
  }
}

function buildUniqueKeyFromColumns(
  row: Record<string, unknown>,
  columns: string[],
): string | null {
  const keys: string[] = [];
  for (const column of columns) {
    const key = valueToKey(row[column]);
    if (key === null) return null;
    keys.push(key);
  }
  return JSON.stringify(keys);
}

function buildUniqueKeyFromRow(
  values: unknown[],
  indices: number[],
): string | null {
  const keys: string[] = [];
  for (const index of indices) {
    const key = valueToKey(values[index]);
    if (key === null) return null;
    keys.push(key);
  }
  return JSON.stringify(keys);
}

function shuffleInPlace<T>(values: T[]): void {
  for (let i = values.length - 1; i > 0; i -= 1) {
    const j = faker.number.int({ min: 0, max: i });
    const temp = values[i];
    values[i] = values[j];
    values[j] = temp;
  }
}

function valueToKey(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  if (value instanceof Date) return value.toISOString();
  if (Buffer.isBuffer(value)) return value.toString("hex");
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function validateForeignKeyPools(
  table: TableInfo,
  fkPools: Map<string, Array<Array<unknown>>>,
): void {
  const columnMap = new Map(
    table.columns.map((column) => [column.name, column]),
  );
  for (const fk of table.fks) {
    const key = foreignKeyPoolKey(fk);
    const pool = fkPools.get(key) ?? [];
    if (pool.length > 0) continue;
    const requiresValue = fk.columns.some(
      (columnName) => !columnMap.get(columnName)?.isNullable,
    );
    if (requiresValue) {
      throw new Error(
        `Foreign key pool empty for ${table.schema}.${table.name} -> ${fk.refSchema}.${fk.refTable}. ` +
          `Seed parent table first or allow nulls on ${fk.columns.join(", ")}.`,
      );
    }
  }
}
