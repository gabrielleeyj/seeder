#!/usr/bin/env node
import fs from 'node:fs';
import { Command } from 'commander';
import { Client } from 'pg';
import { runSeeder } from './index.js';
import type { Config } from './types.js';
import { parseList, resolveConfigPath, configFileExists } from './utils.js';
import { seedFaker } from './generate.js';

const program = new Command();

program
  .name('seeder')
  .description('Seed a PostgreSQL database with realistic mock data')
  .option('-c, --config <path>', 'Path to seeder.config.json')
  .option('--schema <schema>', 'Schema to seed (default: public)')
  .option('--schemas <schemas>', 'Comma-separated list of schemas to seed')
  .option('--max-records <number>', 'Max records per table (default: 50)')
  .option('--seed <number>', 'Seed for deterministic randomness (default: 1337)')
  .option('--include <tables>', 'Comma-separated list of tables to include')
  .option('--exclude <tables>', 'Comma-separated list of tables to exclude')
  .option('--dry-run', 'Preview insert counts without writing')
  .option('--connection <connectionString>', 'Postgres connection string')
  .parse(process.argv);

const options = program.opts();

const configPath = resolveConfigPath(options.config);
let fileConfig: Partial<Config> = {};
if (options.config) {
  if (!configFileExists(configPath)) {
    console.error(`Config file not found: ${configPath}`);
    process.exit(1);
  }
}

if (configFileExists(configPath)) {
  const raw = fs.readFileSync(configPath, 'utf8');
  fileConfig = JSON.parse(raw) as Partial<Config>;
}

const cliConfig: Partial<Config> = stripUndefined({
  schema: options.schema,
  schemas: parseList(options.schemas),
  maxRecords: options.maxRecords ? Number(options.maxRecords) : undefined,
  seed: options.seed ? Number(options.seed) : undefined,
  includeTables: parseList(options.include),
  excludeTables: parseList(options.exclude),
  dryRun: options.dryRun ?? undefined,
  connectionString: options.connection
});

const config: Config = {
  schema: 'public',
  maxRecords: 50,
  seed: 1337,
  ...fileConfig,
  ...cliConfig
};

if (Number.isNaN(config.maxRecords)) {
  console.error('Invalid --max-records value');
  process.exit(1);
}

if (config.maxRecords > 50) {
  console.warn('Max records capped at 50 by default.');
  config.maxRecords = 50;
}

if (!config.schema) {
  config.schema = 'public';
}

if (!config.schemas || config.schemas.length === 0) {
  config.schemas = [config.schema];
}

if (Number.isNaN(config.seed)) {
  console.error('Invalid --seed value');
  process.exit(1);
}

seedFaker(config.seed);

const connectionString =
  config.connectionString ??
  process.env.DATABASE_URL ??
  process.env.PG_CONNECTION_STRING;

const client = new Client(
  connectionString
    ? { connectionString }
    : {
        host: config.connection?.host,
        port: config.connection?.port,
        user: config.connection?.user,
        password: config.connection?.password,
        database: config.connection?.database
      }
);

async function main() {
  await client.connect();
  try {
    await client.query('BEGIN');
    const summary = await runSeeder(client, config);
    if (config.dryRun) {
      await client.query('ROLLBACK');
    } else {
      await client.query('COMMIT');
    }

    console.log('\nSeed summary');
    for (const row of summary) {
      const total = row.existing + row.inserted;
      const prefix = row.schema ? `${row.schema}.` : '';
      const note = config.dryRun ? ' (dry-run)' : '';
      console.log(`- ${prefix}${row.table}: +${row.inserted} (total ${total})${note}`);
    }
  } catch (error) {
    await enrichAndLogError(error, client);
    await client.query('ROLLBACK');
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();

function stripUndefined<T extends Record<string, unknown>>(input: T): Partial<T> {
  const entries = Object.entries(input).filter(([, value]) => value !== undefined);
  return Object.fromEntries(entries);
}

async function enrichAndLogError(error: unknown, client: Client): Promise<void> {
  const err = error as { code?: string; constraint?: string; message?: string };
  if (err?.code === '23505' && err.constraint) {
    try {
      const result = await client.query(
        `select conrelid::regclass::text as table_name
         from pg_constraint
         where conname = $1`,
        [err.constraint]
      );
      const table = result.rows[0]?.table_name;
      if (table) {
        console.error(`Unique constraint violation on ${table} (${err.constraint}).`);
        return;
      }
    } catch {
      // fall through to generic error output
    }
  }

  console.error(err?.message ?? error);
}
