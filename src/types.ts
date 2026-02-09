export type ColumnInfo = {
  name: string;
  dataType: string;
  udtName: string;
  isNullable: boolean;
  columnDefault: string | null;
  isIdentity: boolean;
  isGenerated: boolean;
  maxLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
};

export type ForeignKeyInfo = {
  columns: string[];
  refSchema: string;
  refTable: string;
  refColumns: string[];
};

export type UniqueConstraintInfo = {
  name: string;
  columns: string[];
};

export type TableInfo = {
  schema: string;
  name: string;
  columns: ColumnInfo[];
  pkColumns: string[];
  fks: ForeignKeyInfo[];
  uniqueConstraints: UniqueConstraintInfo[];
};

export type Config = {
  connectionString?: string;
  connection?: {
    host?: string;
    port?: number;
    user?: string;
    password?: string;
    database?: string;
  };
  schema?: string;
  schemas?: string[];
  maxRecords: number;
  seed: number;
  includeTables?: string[];
  excludeTables?: string[];
  dryRun?: boolean;
  overrides?: Record<
    string,
    {
      faker?: string;
      values?: unknown[];
    }
  >;
};

export type EnumMap = Map<string, string[]>;
