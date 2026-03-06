import type { ColumnType, MetadataConfig, RelationMeta, TableMeta } from '@mkven/multi-db-validation'
import type { MetadataProvider } from './types/providers.js'

// ── Shared Introspection Types ─────────────────────────────────

/**
 * Controls how raw database identifiers are mapped to API names.
 *
 * Built-in options:
 * - `'camelCase'` — `user_name` → `userName` (default)
 * - `'preserve'`  — keep the original name unchanged
 * - A custom function for full control
 */
export type ApiNameMapper = 'camelCase' | 'preserve' | ((raw: string) => string)

/** Options shared by all database-specific introspectors. */
export interface IntrospectOptions {
  /**
   * Schemas to include (default: engine-specific, e.g. `['public']` for PG).
   * Only tables from these schemas are discovered.
   */
  schemas?: string[] | undefined
  /**
   * Tables to skip (matched against `schema.table` or just `table`).
   * Useful for excluding migration tables, temporary tables, etc.
   */
  exclude?: string[] | undefined
  /**
   * How to derive `apiName` from physical column / table names.
   * @default 'camelCase'
   */
  apiNameMapper?: ApiNameMapper | undefined
}

/**
 * Result of introspecting a single database.
 *
 * Contains the discovered tables ready to merge into `MetadataConfig`.
 * The consumer decides how to compose multi-DB configs or pass them
 * to `createSingleDb`.
 */
export interface IntrospectResult {
  tables: Omit<TableMeta, 'database'>[]
  /**
   * Columns that were discovered but skipped because their database
   * type has no matching `ColumnType` (e.g. `json`, `bytea`, `interval`).
   *
   * Each entry is `"schema.table.column (pgType)"`.
   * Empty when every column was mapped successfully.
   */
  skippedColumns: string[]
}

// ── Name mapping helpers ───────────────────────────────────────

/** Convert `snake_case` (or any `_`-separated) identifier to `camelCase`. */
export function snakeToCamel(name: string): string {
  if (!name.includes('_')) return name
  const parts = name.split(/_+/).filter(Boolean)
  return parts
    .map((s, i) => (i === 0 ? s.toLowerCase() : s.charAt(0).toUpperCase() + s.slice(1).toLowerCase()))
    .join('')
}

/** Resolve an `ApiNameMapper` option into a concrete mapping function. */
export function resolveApiNameMapper(mapper: ApiNameMapper | undefined): (raw: string) => string {
  if (mapper === undefined || mapper === 'camelCase') return snakeToCamel
  if (mapper === 'preserve') return (x) => x
  return mapper
}

// ── MetadataProvider factory ───────────────────────────────────

/**
 * Build a `MetadataConfig` from introspection results.
 *
 * Stamps every table with the given `databaseId` and wraps the
 * config in a `MetadataProvider` for direct use with `createMultiDb`
 * or `createSingleDb`.
 */
export function introspectionMetadataProvider(
  databaseId: string,
  engine: MetadataConfig['databases'][number]['engine'],
  result: IntrospectResult,
): MetadataProvider {
  const tables: TableMeta[] = result.tables.map((t) => ({ ...t, database: databaseId }))

  const config: MetadataConfig = {
    databases: [{ id: databaseId, engine }],
    tables,
    caches: [],
    externalSyncs: [],
  }

  return { load: () => Promise.resolve(config) }
}

// Re-export types that executor packages need
export type { ColumnType, RelationMeta }
