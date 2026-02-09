import type { Client } from 'pg';
import type { ColumnInfo, EnumMap, ForeignKeyInfo, TableInfo, UniqueConstraintInfo } from './types.js';

const SYSTEM_SCHEMAS = new Set(['pg_catalog', 'information_schema']);

export async function loadEnums(client: Client, schema: string): Promise<EnumMap> {
  const result = await client.query(
    `
    select n.nspname as schema,
           t.typname as name,
           e.enumlabel as label
    from pg_type t
      join pg_enum e on t.oid = e.enumtypid
      join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = $1
    order by t.typname, e.enumsortorder;
  `,
    [schema]
  );

  const map: EnumMap = new Map();
  for (const row of result.rows) {
    const scopedKey = `${row.schema}.${row.name}`;
    const bareKey = `${row.name}`;
    const scopedValues = map.get(scopedKey) ?? [];
    scopedValues.push(row.label);
    map.set(scopedKey, scopedValues);

    const bareValues = map.get(bareKey) ?? [];
    bareValues.push(row.label);
    map.set(bareKey, bareValues);
  }

  return map;
}

export async function introspectTables(client: Client, schema: string): Promise<TableInfo[]> {
  if (SYSTEM_SCHEMAS.has(schema)) {
    throw new Error(`Refusing to introspect system schema: ${schema}`);
  }

  const tablesResult = await client.query(
    `
    select table_schema, table_name
    from information_schema.tables
    where table_type = 'BASE TABLE'
      and table_schema = $1
    order by table_name;
  `,
    [schema]
  );

  const tableNames = tablesResult.rows.map((row) => row.table_name as string);
  if (!tableNames.length) return [];

  const columnsResult = await client.query(
    `
    select table_schema,
           table_name,
           column_name,
           data_type,
           udt_name,
           is_nullable,
           column_default,
           is_identity,
           is_generated,
           character_maximum_length,
           numeric_precision,
           numeric_scale
    from information_schema.columns
    where table_schema = $1
    order by table_name, ordinal_position;
  `,
    [schema]
  );

  const pkResult = await client.query(
    `
    select tc.table_name, kcu.column_name, kcu.ordinal_position
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
    where tc.constraint_type = 'PRIMARY KEY'
      and tc.table_schema = $1
    order by tc.table_name, kcu.ordinal_position;
  `,
    [schema]
  );

  const fkResult = await client.query(
    `
    select
      tc.table_name as table_name,
      kcu.column_name as column_name,
      kcu.ordinal_position as ordinal_position,
      ccu.table_schema as foreign_table_schema,
      ccu.table_name as foreign_table_name,
      ccu.column_name as foreign_column_name,
      tc.constraint_name as constraint_name
    from information_schema.table_constraints tc
      join information_schema.key_column_usage kcu
        on tc.constraint_name = kcu.constraint_name
       and tc.table_schema = kcu.table_schema
      join information_schema.constraint_column_usage ccu
        on ccu.constraint_name = tc.constraint_name
       and ccu.table_schema = tc.table_schema
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema = $1
    order by tc.table_name, tc.constraint_name, kcu.ordinal_position;
  `,
    [schema]
  );

  const uniqueResult = await client.query(
    `
    select tc.table_name as table_name,
           tc.constraint_name as constraint_name,
           kcu.column_name as column_name,
           kcu.ordinal_position as ordinal_position
    from information_schema.table_constraints tc
      join information_schema.key_column_usage kcu
        on tc.constraint_name = kcu.constraint_name
       and tc.table_schema = kcu.table_schema
    where tc.constraint_type = 'UNIQUE'
      and tc.table_schema = $1
    order by tc.table_name, tc.constraint_name, kcu.ordinal_position;
  `,
    [schema]
  );

  const columnsByTable = new Map<string, ColumnInfo[]>();
  for (const row of columnsResult.rows) {
    const key = row.table_name as string;
    const column: ColumnInfo = {
      name: row.column_name,
      dataType: row.data_type,
      udtName: row.udt_name,
      isNullable: row.is_nullable === 'YES',
      columnDefault: row.column_default,
      isIdentity: row.is_identity === 'YES',
      isGenerated: row.is_generated && row.is_generated !== 'NEVER',
      maxLength: row.character_maximum_length,
      numericPrecision: row.numeric_precision,
      numericScale: row.numeric_scale
    };
    const list = columnsByTable.get(key) ?? [];
    list.push(column);
    columnsByTable.set(key, list);
  }

  const pkByTable = new Map<string, string[]>();
  for (const row of pkResult.rows) {
    const key = row.table_name as string;
    const list = pkByTable.get(key) ?? [];
    list.push(row.column_name);
    pkByTable.set(key, list);
  }

  const fkByTable = new Map<string, ForeignKeyInfo[]>();
  const fkKeyMap = new Map<string, ForeignKeyInfo>();

  for (const row of fkResult.rows) {
    const tableName = row.table_name as string;
    const constraintName = row.constraint_name as string;
    const mapKey = `${tableName}:${constraintName}`;
    let fk = fkKeyMap.get(mapKey);
    if (!fk) {
      fk = {
        columns: [],
        refSchema: row.foreign_table_schema as string,
        refTable: row.foreign_table_name as string,
        refColumns: []
      };
      fkKeyMap.set(mapKey, fk);
      const list = fkByTable.get(tableName) ?? [];
      list.push(fk);
      fkByTable.set(tableName, list);
    }
    fk.columns.push(row.column_name);
    fk.refColumns.push(row.foreign_column_name);
  }

  const uniqueByTable = new Map<string, UniqueConstraintInfo[]>();
  const uniqueKeyMap = new Map<string, UniqueConstraintInfo>();

  for (const row of uniqueResult.rows) {
    const tableName = row.table_name as string;
    const constraintName = row.constraint_name as string;
    const mapKey = `${tableName}:${constraintName}`;
    let unique = uniqueKeyMap.get(mapKey);
    if (!unique) {
      unique = {
        name: constraintName,
        columns: []
      };
      uniqueKeyMap.set(mapKey, unique);
      const list = uniqueByTable.get(tableName) ?? [];
      list.push(unique);
      uniqueByTable.set(tableName, list);
    }
    unique.columns.push(row.column_name);
  }

  const tables: TableInfo[] = [];
  for (const name of tableNames) {
    const columns = columnsByTable.get(name) ?? [];
    const pkColumns = pkByTable.get(name) ?? [];
    const fks = fkByTable.get(name) ?? [];
    const uniqueConstraints = uniqueByTable.get(name) ?? [];
    tables.push({
      schema,
      name,
      columns,
      pkColumns,
      fks,
      uniqueConstraints
    });
  }

  return tables;
}
