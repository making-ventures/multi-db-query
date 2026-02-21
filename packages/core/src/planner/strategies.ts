import type {
  CachedTableMeta,
  DatabaseEngine,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
  TableMeta,
} from '@mkven/multi-db-validation'
import { PlannerError } from '@mkven/multi-db-validation'
import type { RegistrySnapshot } from '../metadata/registry.js'
import { evaluateCandidate, findAnyCandidateIgnoringFreshness, isFreshEnough } from './graph.js'
import type { CachePlan, DialectName, DirectPlan, MaterializedPlan, PlannerOptions, TrinoPlan } from './planner.js'

// --- Table collection ---

export function collectTables(query: QueryDefinition, snapshot: RegistrySnapshot): TableMeta[] {
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
      // Also collect EXISTS tables from join filters
      if (join.filters !== undefined) {
        collectExistsTables(join.filters, snapshot, tables, seen)
      }
    }
  }

  // Collect tables from top-level EXISTS filters
  if (query.filters !== undefined) {
    collectExistsTables(query.filters, snapshot, tables, seen)
  }

  return tables
}

type FilterEntry = QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter

function collectExistsTables(
  filters: readonly FilterEntry[],
  snapshot: RegistrySnapshot,
  tables: TableMeta[],
  seen: Set<string>,
): void {
  for (const filter of filters) {
    if ('logic' in filter && 'conditions' in filter) {
      collectExistsTables((filter as QueryFilterGroup).conditions, snapshot, tables, seen)
    } else if (!('operator' in filter) && !('logic' in filter) && !('column' in filter) && 'table' in filter) {
      const ef = filter as QueryExistsFilter
      const t = snapshot.index.getTable(ef.table)
      if (t !== undefined && !seen.has(t.id)) {
        tables.push(t)
        seen.add(t.id)
      }
      if (ef.filters !== undefined) {
        collectExistsTables(ef.filters, snapshot, tables, seen)
      }
    }
  }
}

// --- P0: Cache ---

export function tryCache(
  query: QueryDefinition,
  tables: TableMeta[],
  snapshot: RegistrySnapshot,
): CachePlan | undefined {
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

export function tryDirect(tables: TableMeta[], snapshot: RegistrySnapshot): DirectPlan | undefined {
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

interface CandidateDbLocal {
  database: string
  overrides: Map<string, string>
  originalCount: number
  worstLag: string
}

export function tryMaterialized(
  query: QueryDefinition,
  tables: TableMeta[],
  snapshot: RegistrySnapshot,
): MaterializedPlan | undefined {
  if (tables.length <= 1) return undefined

  const databases = new Set(tables.map((t) => t.database))
  if (databases.size <= 1) return undefined

  // Find all candidate databases
  const candidates: CandidateDbLocal[] = []

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

// --- P3: Trino ---

export function tryTrino(tables: TableMeta[], snapshot: RegistrySnapshot, opts: PlannerOptions): TrinoPlan | undefined {
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

export function throwPlannerError(
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

// --- Helpers ---

export function dialectForEngine(engine: DatabaseEngine): DialectName {
  switch (engine) {
    case 'postgres':
      return 'postgres'
    case 'clickhouse':
      return 'clickhouse'
    case 'iceberg':
      return 'trino'
  }
}
