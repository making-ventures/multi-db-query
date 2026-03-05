import type { ColumnType, IntrospectOptions, IntrospectResult } from '@mkven/multi-db-query'
import { resolveApiNameMapper } from '@mkven/multi-db-query'
import { Pool } from 'pg'
import type { PostgresExecutorConfig } from './index.js'

// ── PG-specific options ────────────────────────────────────────

export interface IntrospectPostgresOptions extends IntrospectOptions {
  /** Connection config (same as `createPostgresExecutor`). */
  connection: PostgresExecutorConfig
}

// ── Type mapping ───────────────────────────────────────────────

const PG_TYPE_MAP: Record<string, ColumnType> = {
  // Strings
  text: 'string',
  'character varying': 'string',
  varchar: 'string',
  char: 'string',
  character: 'string',
  citext: 'string',
  name: 'string',
  // Integers
  integer: 'int',
  int: 'int',
  int4: 'int',
  smallint: 'int',
  int2: 'int',
  bigint: 'int',
  int8: 'int',
  serial: 'int',
  bigserial: 'int',
  smallserial: 'int',
  // Decimals
  numeric: 'decimal',
  decimal: 'decimal',
  real: 'decimal',
  float4: 'decimal',
  'double precision': 'decimal',
  float8: 'decimal',
  money: 'decimal',
  // Boolean
  boolean: 'boolean',
  bool: 'boolean',
  // UUID
  uuid: 'uuid',
  // Date
  date: 'date',
  // Timestamp
  'timestamp without time zone': 'timestamp',
  'timestamp with time zone': 'timestamp',
  timestamp: 'timestamp',
  timestamptz: 'timestamp',
}

const PG_ARRAY_TYPE_MAP: Record<string, ColumnType> = {
  'text[]': 'string[]',
  'character varying[]': 'string[]',
  'varchar[]': 'string[]',
  'integer[]': 'int[]',
  'int[]': 'int[]',
  'bigint[]': 'int[]',
  'smallint[]': 'int[]',
  'numeric[]': 'decimal[]',
  'decimal[]': 'decimal[]',
  'real[]': 'decimal[]',
  'double precision[]': 'decimal[]',
  'boolean[]': 'boolean[]',
  'bool[]': 'boolean[]',
  'uuid[]': 'uuid[]',
  'date[]': 'date[]',
  'timestamp without time zone[]': 'timestamp[]',
  'timestamp with time zone[]': 'timestamp[]',
  'timestamp[]': 'timestamp[]',
  'timestamptz[]': 'timestamp[]',
}

function mapPgType(pgType: string, isArray: boolean): ColumnType | undefined {
  if (isArray) {
    // Try direct match first, then strip ARRAY suffix
    return PG_ARRAY_TYPE_MAP[pgType] ?? PG_ARRAY_TYPE_MAP[`${pgType}[]`]
  }
  return PG_TYPE_MAP[pgType]
}

// ── SQL queries ────────────────────────────────────────────────

function columnsQuery(schemas: string[]): { sql: string; params: string[] } {
  return {
    sql: `
      SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        c.data_type,
        c.udt_name,
        c.is_nullable,
        CASE WHEN c.data_type = 'ARRAY' THEN TRUE ELSE FALSE END AS is_array
      FROM information_schema.columns c
      JOIN information_schema.tables t
        ON t.table_schema = c.table_schema AND t.table_name = c.table_name
      WHERE t.table_type = 'BASE TABLE'
        AND c.table_schema = ANY($1)
      ORDER BY c.table_schema, c.table_name, c.ordinal_position
    `,
    params: schemas,
  }
}

function primaryKeysQuery(schemas: string[]): { sql: string; params: string[] } {
  return {
    sql: `
      SELECT
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = ANY($1)
      ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
    `,
    params: schemas,
  }
}

function foreignKeysQuery(schemas: string[]): { sql: string; params: string[] } {
  return {
    sql: `
      SELECT
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name,
        ccu.table_name AS referenced_table,
        ccu.column_name AS referenced_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        AND tc.constraint_schema = ccu.constraint_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = ANY($1)
      ORDER BY kcu.table_schema, kcu.table_name
    `,
    params: schemas,
  }
}

// ── Row types ──────────────────────────────────────────────────

interface ColumnRow {
  table_schema: string
  table_name: string
  column_name: string
  data_type: string
  udt_name: string
  is_nullable: string
  is_array: boolean
}

interface PkRow {
  table_schema: string
  table_name: string
  column_name: string
}

interface FkRow {
  table_schema: string
  table_name: string
  column_name: string
  referenced_table: string
  referenced_column: string
}

// ── introspectPostgres ─────────────────────────────────────────

/**
 * Discover tables, columns, primary keys and foreign-key relations
 * from a live PostgreSQL database.
 *
 * ```ts
 * const result = await introspectPostgres({
 *   connection: { connectionString: 'postgresql://...' },
 *   schemas: ['public'],
 *   exclude: ['schema_migrations'],
 * })
 * ```
 */
export async function introspectPostgres(options: IntrospectPostgresOptions): Promise<IntrospectResult> {
  const schemas = options.schemas ?? ['public']
  const exclude = new Set(options.exclude ?? [])
  const mapName = resolveApiNameMapper(options.apiNameMapper)

  const pool = new Pool({
    connectionString: options.connection.connectionString,
    host: options.connection.host,
    port: options.connection.port,
    database: options.connection.database,
    user: options.connection.user,
    password: options.connection.password,
    ssl: options.connection.ssl,
    max: 2,
  })

  try {
    // Run all three queries in parallel
    const [colResult, pkResult, fkResult] = await Promise.all([
      pool.query(columnsQuery(schemas).sql, [schemas]),
      pool.query(primaryKeysQuery(schemas).sql, [schemas]),
      pool.query(foreignKeysQuery(schemas).sql, [schemas]),
    ])

    const colRows = colResult.rows as unknown as ColumnRow[]
    const pkRows = pkResult.rows as unknown as PkRow[]
    const fkRows = fkResult.rows as unknown as FkRow[]

    // Index primary keys by schema.table → column[]
    const pkIndex = new Map<string, string[]>()
    for (const row of pkRows) {
      const key = `${row.table_schema}.${row.table_name}`
      const list = pkIndex.get(key) ?? []
      list.push(row.column_name)
      pkIndex.set(key, list)
    }

    // Index foreign keys by schema.table → fk[]
    const fkIndex = new Map<string, FkRow[]>()
    for (const row of fkRows) {
      const key = `${row.table_schema}.${row.table_name}`
      const list = fkIndex.get(key) ?? []
      list.push(row)
      fkIndex.set(key, list)
    }

    // Build table map from column rows
    const tableMap = new Map<string, { schema: string; table: string; columns: ColumnRow[] }>()
    for (const row of colRows) {
      const key = `${row.table_schema}.${row.table_name}`
      if (exclude.has(row.table_name) || exclude.has(key)) continue

      const entry = tableMap.get(key) ?? { schema: row.table_schema, table: row.table_name, columns: [] }
      if (!tableMap.has(key)) tableMap.set(key, entry)
      entry.columns.push(row)
    }

    // Map to IntrospectResult
    const tables: IntrospectResult['tables'] = []

    // Build a lookup from physicalName → apiName for FK resolution
    const tableApiNames = new Map<string, string>()
    for (const [, entry] of tableMap) {
      tableApiNames.set(entry.table, mapName(entry.table))
    }

    for (const [key, entry] of tableMap) {
      const physicalName = `${entry.schema}.${entry.table}`
      const tableApiName = mapName(entry.table)

      const columns = []
      const skippedColumns: string[] = []

      for (const col of entry.columns) {
        const pgType = col.is_array ? col.udt_name.replace(/^_/, '') : col.data_type.toLowerCase()
        const colType = mapPgType(pgType, col.is_array)

        if (colType === undefined) {
          skippedColumns.push(col.column_name)
          continue
        }

        columns.push({
          apiName: mapName(col.column_name),
          physicalName: col.column_name,
          type: colType,
          nullable: col.is_nullable === 'YES',
        })
      }

      const primaryKey = (pkIndex.get(key) ?? []).map(mapName)
      const fks = fkIndex.get(key) ?? []
      const relations = fks
        .filter((fk) => tableApiNames.has(fk.referenced_table))
        .map((fk) => ({
          column: mapName(fk.column_name),
          references: {
            table: tableApiNames.get(fk.referenced_table)!,
            column: mapName(fk.referenced_column),
          },
          type: 'many-to-one' as const,
        }))

      tables.push({
        id: tableApiName,
        apiName: tableApiName,
        physicalName,
        columns,
        primaryKey,
        relations,
      })
    }

    return { tables }
  } finally {
    await pool.end()
  }
}
