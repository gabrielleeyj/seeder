import { faker } from '@faker-js/faker';
import type { ColumnInfo, EnumMap } from './types.js';

export type OverrideSpec = {
  faker?: string;
  values?: unknown[];
};

const NAME_HINTS: Array<[RegExp, () => string]> = [
  [/email/, () => faker.internet.email()],
  [/first_name|firstname|given_name/, () => faker.person.firstName()],
  [/last_name|lastname|family_name|surname/, () => faker.person.lastName()],
  [/full_name|name/, () => faker.person.fullName()],
  [/username|user_name|login/, () => faker.internet.userName()],
  [/phone|phone_number|mobile/, () => faker.phone.number()],
  [/address/, () => faker.location.streetAddress()],
  [/city/, () => faker.location.city()],
  [/state|province|region/, () => faker.location.state()],
  [/zip|postal/, () => faker.location.zipCode()],
  [/country/, () => faker.location.country()],
  [/company|org|organization/, () => faker.company.name()],
  [/title/, () => faker.lorem.sentence({ min: 3, max: 6 })],
  [/description|bio|notes|summary/, () => faker.lorem.paragraph()],
  [/url|website/, () => faker.internet.url()],
  [/slug/, () => faker.helpers.slugify(faker.lorem.words(3)).toLowerCase()]
];

export function seedFaker(seed: number): void {
  faker.seed(seed);
}

function randomFromArray<T>(values: T[]): T {
  return values[faker.number.int({ min: 0, max: values.length - 1 })];
}

function generateFromHints(columnName: string): string | undefined {
  const normalized = columnName.toLowerCase();
  for (const [pattern, producer] of NAME_HINTS) {
    if (pattern.test(normalized)) {
      return producer();
    }
  }
  return undefined;
}

function clampString(value: string, maxLength: number | null): string {
  if (!maxLength) return value;
  if (value.length <= maxLength) return value;
  return value.slice(0, Math.max(1, maxLength));
}

function formatTime(date: Date): string {
  const hh = String(date.getHours()).padStart(2, '0');
  const mm = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function udtToDataType(udtName: string): string {
  switch (udtName) {
    case 'int2':
      return 'smallint';
    case 'int4':
      return 'integer';
    case 'int8':
      return 'bigint';
    case 'varchar':
      return 'character varying';
    case 'text':
      return 'text';
    case 'bool':
      return 'boolean';
    case 'float4':
      return 'real';
    case 'float8':
      return 'double precision';
    case 'numeric':
      return 'numeric';
    case 'date':
      return 'date';
    case 'timestamp':
      return 'timestamp';
    case 'timestamptz':
      return 'timestamp with time zone';
    case 'uuid':
      return 'uuid';
    case 'json':
      return 'json';
    case 'jsonb':
      return 'jsonb';
    case 'inet':
      return 'inet';
    default:
      return udtName;
  }
}

export function generateValue(
  column: ColumnInfo,
  enums: EnumMap,
  override?: OverrideSpec
): unknown {
  if (override?.values && override.values.length) {
    return randomFromArray(override.values);
  }

  if (override?.faker) {
    const result = resolveFakerPath(override.faker);
    if (typeof result === 'function') {
      return result();
    }
    throw new Error(`Invalid faker override: ${override.faker}`);
  }

  const dataType = column.dataType.toLowerCase();
  const udtName = column.udtName.toLowerCase();

  if (udtName.startsWith('_')) {
    const baseName = udtName.slice(1);
    const values = Array.from({ length: faker.number.int({ min: 1, max: 3 }) }, () =>
      generateValue(
        {
          ...column,
          dataType: udtToDataType(baseName),
          udtName: baseName
        },
        enums
      )
    );
    return values;
  }

  if (enums.size) {
    const possible = enums.get(column.udtName) ?? enums.get(`public.${column.udtName}`);
    if (possible && possible.length) {
      return randomFromArray(possible);
    }
  }

  if (dataType.includes('inet') || dataType.includes('cidr')) {
    return faker.internet.ip();
  }

  const hint = generateFromHints(column.name);
  if (hint) {
    return clampString(hint, column.maxLength);
  }

  if (dataType.includes('uuid')) {
    return faker.string.uuid();
  }

  if (dataType.includes('boolean')) {
    return faker.datatype.boolean();
  }

  if (dataType.includes('integer') || dataType.includes('bigint') || dataType.includes('smallint')) {
    return faker.number.int({ min: 1, max: 10000 });
  }

  if (dataType.includes('numeric') || dataType.includes('decimal')) {
    const precision = column.numericPrecision ?? 8;
    const scale = column.numericScale ?? 2;
    const max = Math.pow(10, Math.max(1, precision - scale)) - 1;
    return faker.number.float({ min: 0, max, fractionDigits: scale });
  }

  if (dataType.includes('real') || dataType.includes('double')) {
    return faker.number.float({ min: 0, max: 10000, fractionDigits: 4 });
  }

  if (dataType.includes('timestamp')) {
    return faker.date.past({ years: 5 }).toISOString();
  }

  if (dataType.includes('date')) {
    const date = faker.date.past({ years: 5 });
    return date.toISOString().slice(0, 10);
  }

  if (dataType.includes('time')) {
    return formatTime(faker.date.recent());
  }

  if (dataType.includes('json')) {
    return {
      id: faker.string.uuid(),
      label: faker.lorem.word(),
      createdAt: faker.date.recent().toISOString()
    };
  }

  if (dataType.includes('bytea')) {
    return Buffer.from(faker.string.alphanumeric(16));
  }

  if (dataType.includes('character') || dataType.includes('text')) {
    return clampString(faker.lorem.words({ min: 1, max: 4 }), column.maxLength);
  }

  return clampString(faker.lorem.word(), column.maxLength);
}

function resolveFakerPath(path: string): unknown {
  const segments = path.split('.').filter(Boolean);
  let current: unknown = faker as unknown;
  for (const segment of segments) {
    if (!current || typeof current !== 'object' || !(segment in (current as object))) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}
