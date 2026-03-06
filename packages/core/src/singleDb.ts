import type { DatabaseEngine, RoleMeta, TableMeta } from '@mkven/multi-db-validation'
import { staticMetadata, staticRoles } from './metadata/providers.js'
import type { MultiDb } from './pipeline.js'
import { createMultiDb } from './pipeline.js'
import type { DbExecutor } from './types/interfaces.js'

// ── Types ──────────────────────────────────────────────────────

/**
 * Table definition for single-database setup.
 *
 * Same shape as `TableMeta` but without `database` — it is inferred
 * from the single database you provide. Also matches
 * `IntrospectResult['tables'][number]`, so introspection results
 * can be passed directly.
 */
export type SingleDbTable = Omit<TableMeta, 'database'>

export interface CreateSingleDbOptions {
  /** Logical database identifier (e.g. `'main'`). */
  databaseId?: string | undefined
  engine: DatabaseEngine
  tables: SingleDbTable[]
  roles: RoleMeta[]
  executor: DbExecutor
  /** @default true */
  validateConnection?: boolean | undefined
}

// ── createSingleDb ─────────────────────────────────────────────

/**
 * Convenience wrapper around `createMultiDb` for single-database setups.
 *
 * Eliminates boilerplate: no provider wiring, no executor map, no
 * empty `caches`/`externalSyncs` arrays.
 *
 * ```ts
 * const db = await createSingleDb({
 *   engine: 'postgres',
 *   tables: [{ id: 'users', apiName: 'users', physicalName: 'public.users', ... }],
 *   roles: [{ id: 'admin', tables: '*' }],
 *   executor: createPostgresExecutor({ connectionString: '...' }),
 * })
 * ```
 */
export async function createSingleDb(options: CreateSingleDbOptions): Promise<MultiDb> {
  const dbId = options.databaseId ?? 'default'

  const tables = options.tables.map((t) => ({ ...t, database: dbId }))

  return createMultiDb({
    metadataProvider: staticMetadata({
      databases: [{ id: dbId, engine: options.engine }],
      tables,
      caches: [],
      externalSyncs: [],
    }),
    roleProvider: staticRoles(options.roles),
    executors: { [dbId]: options.executor },
    validateConnections: options.validateConnection,
  })
}
