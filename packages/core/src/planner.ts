import type { CachedTableMeta, DatabaseEngine, QueryDefinition, TableMeta } from '@mkven/multi-db-validation'
import { PlannerError } from '@mkven/multi-db-validation'
import type { RegistrySnapshot } from './metadataRegistry.js'

// --- Types ---

export type DialectName = 'postgres' | 'clickhouse' | 'trino'

export interface PlannerOptions {
  trinoEnabled?: boolean | undefined
}

export type QueryPlan = CachePlan | DirectPlan | MaterializedPlan | TrinoPlan

export interface CachePlan {
  strategy: 'cache'
  cacheId: string
  tableId: string
  fallbackDatabase: string
  fallbackDialect: DialectName
}

export interface DirectPlan {
  strategy: 'direct'
  database: string
  dialect: DialectName
}

export interface MaterializedPlan {
  strategy: 'materialized'
  database: string
  dialect: DialectName
  tableOverrides: ReadonlyMap<string, string>
}

export interface TrinoPlan {
  strategy: 'trino'
  catalogs: ReadonlyMap<string, string>
}

// --- Main ---

export function planQuery(
  query: QueryDefinition,
  snapshot: RegistrySnapshot,
  options?: PlannerOptions | undefined,
): QueryPlan {
  const tables = collectTables(query, snapshot)
  const opts = options ?? {}

  // P0 — Cache
  const cachePlan = tryCache(query, tables, snapshot)
  if (cachePlan !== undefined) return cachePlan

  // P1 — Direct (single database)
  const directPlan = tryDirect(tables, snapshot)
  if (directPlan !== undefined) return directPlan

  // P2 — Materialized replica
  const matPlan = tryMaterialized(query, tables, snapshot)
  if (matPlan !== undefined) return matPlan

  // P3 — Trino cross-database
  const trinoPlan = tryTrino(tables, snapshot, opts)
  if (trinoPlan !== undefined) return trinoPlan

  // P4 — Error
  return throwPlannerError(query, tables, snapshot, opts)
}

// --- Table collection ---

function collectTables(query: QueryDefinition, snapshot: RegistrySnapshot): TableMeta[] {
  const tables: TableMeta[] = []
  const seen = new Set<string>()

  const from = snapshot.index.getTable(query.from)
  if (from !== undefined) {
    tables.push(from)
    seen.add(from.id)
  }

  if (query.joins !== undefined) {
    for (const join of query.joins) {
      const t = snapshot.index.getTable(join.table)
      if (t !== undefined && !seen.has(t.id)) {
        tables.push(t)
        seen.add(t.id)
      }
    }
  }

  return tables
}

// --- P0: Cache ---

function tryCache(query: QueryDefinition, tables: TableMeta[], snapshot: RegistrySnapshot): CachePlan | undefined {
  // Only for byIds, no filters, no joins
  if (query.byIds === undefined || query.byIds.length === 0) return undefined
  if (query.filters !== undefined && query.filters.length > 0) return undefined
  if (query.joins !== undefined && query.joins.length > 0) return undefined
  if (tables.length !== 1) return undefined

  const table = tables[0]
  if (table === undefined) return undefined
  if (table.primaryKey.length !== 1) return undefined

  // Check cache
  const caches = snapshot.cachesByTable.get(table.id)
  if (caches === undefined || caches.length === 0) return undefined

  const cached = caches[0]
  if (cached === undefined) return undefined

  // Column subset check
  if (!cacheHasColumns(cached, query, table)) return undefined

  // Find cache ID
  const cacheId = findCacheId(table.id, snapshot)
  if (cacheId === undefined) return undefined

  // Fallback DB (for partial cache misses at runtime)
  const db = snapshot.config.databases.find((d) => d.id === table.database)
  if (db === undefined) return undefined

  return {
    strategy: 'cache',
    cacheId,
    tableId: table.id,
    fallbackDatabase: db.id,
    fallbackDialect: dialectForEngine(db.engine),
  }
}

function cacheHasColumns(cached: CachedTableMeta, query: QueryDefinition, table: TableMeta): boolean {
  // Cache has all columns → always viable
  if (cached.columns === undefined) return true

  const cachedSet = new Set(cached.columns)

  if (query.columns === undefined) {
    // User wants all columns → cache must have all table columns
    return table.columns.every((c) => cachedSet.has(c.apiName))
  }

  // User wants specific columns → cache must have those
  return query.columns.every((c) => cachedSet.has(c))
}

function findCacheId(tableId: string, snapshot: RegistrySnapshot): string | undefined {
  for (const cache of snapshot.config.caches) {
    if (cache.tables.some((ct) => ct.tableId === tableId)) {
      return cache.id
    }
  }
  return undefined
}

// --- P1: Direct ---

function tryDirect(tables: TableMeta[], snapshot: RegistrySnapshot): DirectPlan | undefined {
  if (tables.length === 0) return undefined

  const first = tables[0]
  if (first === undefined) return undefined

  const dbId = first.database
  if (!tables.every((t) => t.database === dbId)) return undefined

  const db = snapshot.config.databases.find((d) => d.id === dbId)
  if (db === undefined) return undefined

  return {
    strategy: 'direct',
    database: dbId,
    dialect: dialectForEngine(db.engine),
  }
}

// --- P2: Materialized ---

interface CandidateDb {
  database: string
  overrides: Map<string, string>
  originalCount: number
  worstLag: string
}

function tryMaterialized(
  query: QueryDefinition,
  tables: TableMeta[],
  snapshot: RegistrySnapshot,
): MaterializedPlan | undefined {
  if (tables.length <= 1) return undefined

  const databases = new Set(tables.map((t) => t.database))
  if (databases.size <= 1) return undefined

  // Find all candidate databases
  const candidates: CandidateDb[] = []

  // Check each database that has at least one involved table
  for (const dbId of databases) {
    const result = evaluateCandidate(dbId, tables, snapshot)
    if (result !== undefined) candidates.push(result)
  }

  // Also check databases not directly involved but reachable via syncs
  for (const db of snapshot.config.databases) {
    if (!databases.has(db.id)) {
      const result = evaluateCandidate(db.id, tables, snapshot)
      if (result !== undefined) candidates.push(result)
    }
  }

  if (candidates.length === 0) return undefined

  // Freshness filter
  const valid = candidates.filter((c) => {
    if (c.overrides.size === 0) return true
    return isFreshEnough(query.freshness, c.worstLag)
  })

  if (valid.length === 0) return undefined

  // Prefer most originals
  valid.sort((a, b) => b.originalCount - a.originalCount)

  const best = valid[0]
  if (best === undefined) return undefined

  const db = snapshot.config.databases.find((d) => d.id === best.database)
  if (db === undefined) return undefined

  return {
    strategy: 'materialized',
    database: best.database,
    dialect: dialectForEngine(db.engine),
    tableOverrides: best.overrides,
  }
}

function evaluateCandidate(
  targetDbId: string,
  tables: TableMeta[],
  snapshot: RegistrySnapshot,
): CandidateDb | undefined {
  const overrides = new Map<string, string>()
  let originalCount = 0
  let worstLag = 'seconds'

  for (const table of tables) {
    if (table.database === targetDbId) {
      originalCount++
      continue
    }

    // Check for a sync to this target DB
    const syncs = snapshot.syncsByTable.get(table.id)
    if (syncs === undefined) return undefined

    const sync = syncs.find((s) => s.targetDatabase === targetDbId)
    if (sync === undefined) return undefined

    overrides.set(table.id, sync.targetPhysicalName)

    if (lagLevel(sync.estimatedLag) > lagLevel(worstLag)) {
      worstLag = sync.estimatedLag
    }
  }

  return { database: targetDbId, overrides, originalCount, worstLag }
}

// --- P3: Trino ---

function tryTrino(tables: TableMeta[], snapshot: RegistrySnapshot, opts: PlannerOptions): TrinoPlan | undefined {
  if (opts.trinoEnabled !== true) return undefined

  const catalogs = new Map<string, string>()

  for (const table of tables) {
    if (catalogs.has(table.database)) continue
    const db = snapshot.config.databases.find((d) => d.id === table.database)
    if (db === undefined) return undefined
    if (db.trinoCatalog === undefined) return undefined
    catalogs.set(table.database, db.trinoCatalog)
  }

  return { strategy: 'trino', catalogs }
}

// --- P4: Error ---

function throwPlannerError(
  query: QueryDefinition,
  tables: TableMeta[],
  snapshot: RegistrySnapshot,
  opts: PlannerOptions,
): never {
  const fromTable = query.from
  const databases = new Set(tables.map((t) => t.database))

  // Check freshness: if materialized candidates exist but are blocked by freshness
  if (databases.size > 1) {
    const anyCandidateIgnoringFreshness = findAnyCandidateIgnoringFreshness(tables, snapshot)
    if (anyCandidateIgnoringFreshness !== undefined) {
      throw new PlannerError('FRESHNESS_UNMET', fromTable, {
        code: 'FRESHNESS_UNMET',
        requiredFreshness: query.freshness ?? 'realtime',
        availableLag: anyCandidateIgnoringFreshness.worstLag,
      })
    }
  }

  // Check Trino disabled
  if (databases.size > 1 && opts.trinoEnabled !== true) {
    throw new PlannerError('TRINO_DISABLED', fromTable, { code: 'TRINO_DISABLED' })
  }

  // Check missing catalogs
  if (opts.trinoEnabled === true) {
    const missing: string[] = []
    for (const dbId of databases) {
      const db = snapshot.config.databases.find((d) => d.id === dbId)
      if (db !== undefined && db.trinoCatalog === undefined) {
        missing.push(dbId)
      }
    }
    if (missing.length > 0) {
      throw new PlannerError('NO_CATALOG', fromTable, { code: 'NO_CATALOG', databases: missing })
    }
  }

  // Default: unreachable tables
  const firstDb = tables[0]?.database
  const unreachable = tables.filter((t) => t.database !== firstDb).map((t) => t.id)
  throw new PlannerError('UNREACHABLE_TABLES', fromTable, {
    code: 'UNREACHABLE_TABLES',
    tables: unreachable.length > 0 ? unreachable : tables.map((t) => t.id),
  })
}

function findAnyCandidateIgnoringFreshness(tables: TableMeta[], snapshot: RegistrySnapshot): CandidateDb | undefined {
  const databases = new Set(tables.map((t) => t.database))

  for (const dbId of databases) {
    const result = evaluateCandidate(dbId, tables, snapshot)
    if (result !== undefined && result.overrides.size > 0) return result
  }

  for (const db of snapshot.config.databases) {
    if (!databases.has(db.id)) {
      const result = evaluateCandidate(db.id, tables, snapshot)
      if (result !== undefined && result.overrides.size > 0) return result
    }
  }

  return undefined
}

// --- Helpers ---

function dialectForEngine(engine: DatabaseEngine): DialectName {
  switch (engine) {
    case 'postgres':
      return 'postgres'
    case 'clickhouse':
      return 'clickhouse'
    case 'iceberg':
      return 'trino'
  }
}

const lagLevels: Record<string, number> = {
  seconds: 1,
  minutes: 2,
  hours: 3,
}

function lagLevel(lag: string): number {
  return lagLevels[lag] ?? 0
}

function isFreshEnough(required: string | undefined, worstLag: string): boolean {
  if (required === undefined) return true
  if (required === 'realtime') return false
  return lagLevel(worstLag) <= lagLevel(required)
}
