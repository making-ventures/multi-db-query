import type {
  ColumnType,
  DebugLogEntry,
  ExecutionContext,
  HealthCheckResult,
  QueryAggregation,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
  QueryResult,
  QueryResultMeta,
  TableMeta,
} from '@mkven/multi-db-validation'
import { ConnectionError, ExecutionError, validateQuery } from '@mkven/multi-db-validation'

import type { EffectiveColumn } from './access/access.js'
import { maskRows } from './access/access.js'
import { debugEntry, withDebugLog } from './debug/logger.js'
import { generateSql } from './generator/generator.js'
import type { RegistrySnapshot } from './metadata/registry.js'
import { MetadataRegistry } from './metadata/registry.js'
import type { CachePlan, DialectName, DirectPlan, MaterializedPlan, QueryPlan, TrinoPlan } from './planner/planner.js'
import { planQuery } from './planner/planner.js'
import type { ResolveResult } from './resolution/resolver.js'
import { resolveNames } from './resolution/resolver.js'
import type { CacheProvider, DbExecutor } from './types/interfaces.js'
import type { ColumnMapping, CorrelatedSubquery, SqlParts, WhereNode } from './types/ir.js'
import type { MetadataProvider, RoleProvider } from './types/providers.js'

// ── Public Types ───────────────────────────────────────────────

export interface CreateMultiDbOptions {
  readonly metadataProvider: MetadataProvider
  readonly roleProvider: RoleProvider
  readonly executors?: Record<string, DbExecutor> | undefined
  readonly cacheProviders?: Record<string, CacheProvider> | undefined
  readonly validateConnections?: boolean | undefined
}

export interface MultiDb {
  query<T = unknown>(input: { definition: QueryDefinition; context: ExecutionContext }): Promise<QueryResult<T>>
  reloadMetadata(): Promise<void>
  reloadRoles(): Promise<void>
  healthCheck(): Promise<HealthCheckResult>
  close(): Promise<void>
}

// ── Dialect Singletons ─────────────────────────────────────────

// ── createMultiDb ──────────────────────────────────────────────

export async function createMultiDb(options: CreateMultiDbOptions): Promise<MultiDb> {
  const executors = options.executors ?? {}
  const cacheProviders = options.cacheProviders ?? {}

  // Steps 1–4: load providers → validate → index → build graph
  const registry = await MetadataRegistry.create(options.metadataProvider, options.roleProvider)

  // Step 5: ping all executors + cache providers
  if (options.validateConnections !== false) {
    await pingAllOrThrow(executors, cacheProviders, registry.getSnapshot())
  }

  let closed = false

  return {
    async query<T = unknown>(input: { definition: QueryDefinition; context: ExecutionContext }) {
      if (closed) {
        throw new ExecutionError({ code: 'EXECUTOR_MISSING', database: 'closed' })
      }
      return runQuery(input.definition, input.context, registry, executors, cacheProviders) as Promise<QueryResult<T>>
    },

    async reloadMetadata() {
      await registry.reloadMetadata(options.metadataProvider)
    },

    async reloadRoles() {
      await registry.reloadRoles(options.roleProvider)
    },

    async healthCheck() {
      return measureHealth(executors, cacheProviders)
    },

    async close() {
      closed = true
      await closeAll(executors, cacheProviders, registry.getSnapshot())
    },
  }
}

// ── Query Pipeline ─────────────────────────────────────────────

async function runQuery(
  definition: QueryDefinition,
  context: ExecutionContext,
  registry: MetadataRegistry,
  executors: Record<string, DbExecutor>,
  cacheProviders: Record<string, CacheProvider>,
): Promise<QueryResult> {
  const log: DebugLogEntry[] = []
  const debug = definition.debug === true
  const snapshot = registry.getSnapshot()

  // 1. Validate
  const t0 = Date.now()
  const vErr = validateQuery(definition, context, snapshot.index, snapshot.roles)
  if (vErr !== null) throw vErr
  if (debug) log.push(debugEntry('validation', 'Validated', Date.now() - t0))

  // 2. Access control timing (masking now derived from ColumnMapping in finishResult)
  const t1 = Date.now()
  if (debug) log.push(debugEntry('access-control', 'Access resolved', Date.now() - t1))

  // 3. Plan
  const t2 = Date.now()
  const plan = planQuery(definition, snapshot, { trinoEnabled: 'trino' in executors })
  const planningMs = Date.now() - t2
  if (debug) log.push(debugEntry('planning', `Strategy: ${plan.strategy}`, planningMs))

  // 4. Resolve names
  const t3 = Date.now()
  const resolved = resolveNames(definition, context, snapshot.index, snapshot.index.rolesById)
  if (plan.strategy === 'materialized') overrideTables(resolved.parts, plan, definition, snapshot)
  if (plan.strategy === 'trino') setCatalogs(resolved.parts, plan, definition, snapshot)
  // Iceberg P1: direct + trino dialect → set catalog from trinoCatalog
  if (plan.strategy === 'direct' && plan.dialect === 'trino') {
    setDirectTrinoCatalog(resolved.parts, plan, snapshot)
  }
  if (debug) log.push(debugEntry('name-resolution', 'Names resolved', Date.now() - t3))

  // 5. Generate SQL
  const t4 = Date.now()
  const { sql, params: genParams, dialectName } = generateSql(plan, resolved.parts, resolved.params)
  const gen = { sql, params: genParams }
  const generationMs = Date.now() - t4
  if (debug) log.push(debugEntry('sql-generation', `Generated (${dialectName})`, generationMs, { sql: gen.sql }))

  // 6a. SQL-only mode
  if (definition.executeMode === 'sql-only') {
    const meta = buildMeta(plan, resolved, dialectName, definition, snapshot, planningMs, generationMs)
    return withDebugLog({ kind: 'sql', sql: gen.sql, params: [...gen.params], meta }, debug, log)
  }

  // 6b. Cache path (P0)
  if (plan.strategy === 'cache') {
    return cachePath(
      plan,
      definition,
      context,
      snapshot,
      cacheProviders,
      executors,
      gen,
      resolved,
      dialectName,
      planningMs,
      generationMs,
      debug,
      log,
    )
  }

  // 6c. SQL execution (P1 / P2 / P3)
  // Iceberg databases have no standalone executor — always use 'trino' when dialect is trino
  const dbId = plan.strategy === 'trino' ? 'trino' : plan.dialect === 'trino' ? 'trino' : plan.database
  const executor = executors[dbId]
  if (executor === undefined) {
    throw new ExecutionError({ code: 'EXECUTOR_MISSING', database: dbId })
  }

  const t5 = Date.now()
  let rows: Record<string, unknown>[]
  try {
    rows = await executor.execute(gen.sql, gen.params)
  } catch (err) {
    throw toExecError(err, dbId, dialectName, gen)
  }
  const executionMs = Date.now() - t5
  if (debug) log.push(debugEntry('execution', `Executed (${rows.length} rows)`, executionMs))

  // 7. Mask and build result
  return finishResult(
    rows,
    resolved,
    definition,
    plan,
    dialectName,
    snapshot,
    planningMs,
    generationMs,
    executionMs,
    debug,
    log,
  )
}

// ── Cache Path (P0) ────────────────────────────────────────────

async function cachePath(
  plan: CachePlan,
  definition: QueryDefinition,
  context: ExecutionContext,
  snapshot: RegistrySnapshot,
  cacheProviders: Record<string, CacheProvider>,
  executors: Record<string, DbExecutor>,
  gen: { sql: string; params: unknown[] },
  resolved: ResolveResult,
  dialectName: DialectName,
  planningMs: number,
  generationMs: number,
  debug: boolean,
  log: DebugLogEntry[],
): Promise<QueryResult> {
  const provider = cacheProviders[plan.cacheId]
  if (provider === undefined) {
    throw new ExecutionError({ code: 'CACHE_PROVIDER_MISSING', cacheId: plan.cacheId })
  }

  // Build cache keys
  const byIds = definition.byIds ?? []
  const cachedTables = snapshot.cachesByTable.get(plan.tableId)
  const ctm = cachedTables?.[0]
  const keyPattern = ctm?.keyPattern ?? `${plan.tableId}:{id}`
  const keys = byIds.map((id) => keyPattern.replace('{id}', String(id)))

  // Lookup
  const tc = Date.now()
  const cached = await provider.getMany(keys)
  if (debug) log.push(debugEntry('cache', `Lookup (${keys.length} keys)`, Date.now() - tc))

  // Check hits
  const hits: Record<string, unknown>[] = []
  const missingIds: (string | number)[] = []
  for (let i = 0; i < byIds.length; i++) {
    const key = keys[i]
    if (key === undefined) continue
    const val = cached.get(key)
    if (val !== null && val !== undefined) {
      hits.push(val)
    } else {
      const id = byIds[i]
      if (id !== undefined) missingIds.push(id)
    }
  }

  // Full cache hit
  if (missingIds.length === 0 && hits.length > 0) {
    return finishResult(
      hits,
      resolved,
      definition,
      plan,
      dialectName,
      snapshot,
      planningMs,
      generationMs,
      undefined,
      debug,
      log,
    )
  }

  // Partial cache hit — query only missing IDs from fallback DB, merge
  const executor = executors[plan.fallbackDatabase]
  if (executor === undefined) {
    throw new ExecutionError({ code: 'EXECUTOR_MISSING', database: plan.fallbackDatabase })
  }

  const queryIds = missingIds.length > 0 && hits.length > 0 ? missingIds : byIds
  const missingDef = queryIds === byIds ? definition : { ...definition, byIds: queryIds }
  const missingResolved =
    queryIds === byIds ? resolved : resolveNames(missingDef, context, snapshot.index, snapshot.index.rolesById)
  const missingGen = queryIds === byIds ? gen : generateSql(plan, missingResolved.parts, missingResolved.params)

  const t5 = Date.now()
  let rows: Record<string, unknown>[]
  try {
    rows = await executor.execute(missingGen.sql, missingGen.params)
  } catch (err) {
    throw toExecError(err, plan.fallbackDatabase, dialectName, missingGen)
  }
  const executionMs = Date.now() - t5
  if (debug && hits.length > 0) {
    log.push(debugEntry('execution', `Partial cache: ${hits.length} cached, ${rows.length} from DB`, executionMs))
  } else if (debug) {
    log.push(debugEntry('execution', `Fallback executed (${rows.length} rows)`, executionMs))
  }

  // Merge cached hits + DB results
  const allRows = hits.length > 0 ? [...hits, ...rows] : rows

  return finishResult(
    allRows,
    resolved,
    definition,
    plan,
    dialectName,
    snapshot,
    planningMs,
    generationMs,
    executionMs,
    debug,
    log,
  )
}

// ── Result Building ────────────────────────────────────────────

function finishResult(
  rows: Record<string, unknown>[],
  resolved: ResolveResult,
  definition: QueryDefinition,
  plan: QueryPlan,
  dialectName: DialectName,
  snapshot: RegistrySnapshot,
  planningMs: number,
  generationMs: number,
  executionMs: number | undefined,
  debug: boolean,
  log: DebugLogEntry[],
): QueryResult {
  const aggAliases = new Set(definition.aggregations?.map((a) => a.alias) ?? [])

  // Remap rows: SQL alias keys (t0__physical_name) → apiName keys
  const remapped = remapRows(rows, resolved.columnMappings)

  // Build masking map from ColumnMapping (apiName-keyed, aligned with remapped rows)
  const colMaskingMap = buildColumnMaskingMap(resolved.columnMappings)
  const masked = maskRows(remapped, colMaskingMap, aggAliases)

  const meta = buildMeta(plan, resolved, dialectName, definition, snapshot, planningMs, generationMs, executionMs)

  if (resolved.mode === 'count') {
    return withDebugLog({ kind: 'count', count: extractCount(masked), meta }, debug, log)
  }
  return withDebugLog({ kind: 'data', data: masked, meta }, debug, log)
}

function extractCount(rows: Record<string, unknown>[]): number {
  const first = rows[0]
  if (first === undefined) return 0
  const vals = Object.values(first)
  const v = vals[0]
  if (typeof v === 'number') return v
  if (typeof v === 'bigint') return Number(v)
  if (typeof v === 'string') return Number.parseInt(v, 10) || 0
  return 0
}

// ── Row Remapping ──────────────────────────────────────────────

/**
 * Remap row keys from SQL alias convention ({tableAlias}__{physicalName})
 * to apiNames using ColumnMapping[].
 * Aggregation alias keys (already correct) pass through unchanged.
 */
function remapRows(rows: Record<string, unknown>[], columnMappings: ColumnMapping[]): Record<string, unknown>[] {
  if (columnMappings.length === 0) return rows

  // Build remap: SQL alias → apiName
  const remap = new Map<string, string>()
  for (const cm of columnMappings) {
    remap.set(`${cm.tableAlias}__${cm.physicalName}`, cm.apiName)
  }

  return rows.map((row) => {
    const result: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(row)) {
      result[remap.get(key) ?? key] = value
    }
    return result
  })
}

/**
 * Build masking map from ColumnMapping[] — keys are apiNames (collision-aware).
 */
function buildColumnMaskingMap(columnMappings: ColumnMapping[]): ReadonlyMap<string, EffectiveColumn> {
  const map = new Map<string, EffectiveColumn>()
  for (const cm of columnMappings) {
    map.set(cm.apiName, {
      apiName: cm.apiName,
      allowed: true,
      masked: cm.masked,
      maskingFn: cm.maskingFn,
    })
  }
  return map
}

// ── Metadata Building ──────────────────────────────────────────

function buildMeta(
  plan: QueryPlan,
  resolved: ResolveResult,
  dialectName: DialectName,
  definition: QueryDefinition,
  snapshot: RegistrySnapshot,
  planningMs: number,
  generationMs: number,
  executionMs?: number | undefined,
): QueryResultMeta {
  // Strategy mapping
  const strategy = strategyLabel(plan)
  const targetDatabase = targetDb(plan)

  // Tables used
  const apiNames = [definition.from, ...(definition.joins?.map((j) => j.table) ?? [])]
  const tablesUsed: QueryResultMeta['tablesUsed'] = []
  for (const apiName of apiNames) {
    const table = snapshot.index.tablesByApiName.get(apiName)
    if (table !== undefined) {
      let source: 'original' | 'materialized' | 'cache' = 'original'
      let physicalName = table.physicalName
      if (plan.strategy === 'cache' && apiName === definition.from) {
        source = 'cache'
      } else if (plan.strategy === 'materialized') {
        const ov = plan.tableOverrides.get(table.id)
        if (ov !== undefined) {
          source = 'materialized'
          physicalName = ov
        }
      }
      tablesUsed.push({ tableId: table.id, source, database: table.database, physicalName })
    }
  }

  // Columns from resolved mappings
  const columns: QueryResultMeta['columns'] = resolved.columnMappings.map((cm) => ({
    apiName: cm.apiName,
    type: cm.type,
    nullable: cm.nullable,
    fromTable: cm.tableApiName,
    masked: cm.masked,
  }))

  // Append aggregation alias entries (suppressed in count mode — concept: meta.columns is empty)
  if (definition.aggregations !== undefined && resolved.mode !== 'count') {
    for (const agg of definition.aggregations) {
      const aggType = inferAggType(agg, definition, snapshot)
      const fromTableApiName = agg.table ?? definition.from
      columns.push({
        apiName: agg.alias,
        type: aggType,
        nullable: agg.fn === 'count' ? false : inferAggNullable(agg, definition, snapshot),
        fromTable: fromTableApiName,
        masked: false,
      })
    }
  }

  // Timing
  const timing: QueryResultMeta['timing'] =
    executionMs !== undefined ? { planningMs, generationMs, executionMs } : { planningMs, generationMs }

  return { strategy, targetDatabase, dialect: dialectName, tablesUsed, columns, timing }
}

function strategyLabel(plan: QueryPlan): QueryResultMeta['strategy'] {
  switch (plan.strategy) {
    case 'direct':
      return 'direct'
    case 'cache':
      return 'cache'
    case 'materialized':
      return 'materialized'
    case 'trino':
      return 'trino-cross-db'
  }
}

function targetDb(plan: QueryPlan): string {
  switch (plan.strategy) {
    case 'direct':
    case 'materialized':
      return plan.database
    case 'cache':
      return plan.cacheId
    case 'trino':
      return 'trino'
  }
}

function inferAggType(agg: QueryAggregation, definition: QueryDefinition, snapshot: RegistrySnapshot): ColumnType {
  if (agg.fn === 'count') return 'int'
  if (agg.fn === 'avg') return 'decimal'
  // sum/min/max → source column type
  if (agg.column === '*') return 'int' // count(*) already handled; sum/min/max on * is unusual
  const tableApiName = agg.table ?? definition.from
  const table = snapshot.index.tablesByApiName.get(tableApiName)
  if (table === undefined) return 'decimal'
  const col = table.columns.find((c) => c.apiName === agg.column)
  return col?.type ?? 'decimal'
}

function inferAggNullable(agg: QueryAggregation, definition: QueryDefinition, snapshot: RegistrySnapshot): boolean {
  // count is always non-nullable (handled at call site)
  // sum/avg/min/max return NULL when source is nullable (all rows could be NULL)
  if (agg.column === '*') return false
  const tableApiName = agg.table ?? definition.from
  const table = snapshot.index.tablesByApiName.get(tableApiName)
  if (table === undefined) return true
  const col = table.columns.find((c) => c.apiName === agg.column)
  return col?.nullable ?? true
}

// ── Table Overrides (P2 Materialized) ──────────────────────────

function overrideTables(
  parts: SqlParts,
  plan: MaterializedPlan,
  definition: QueryDefinition,
  snapshot: RegistrySnapshot,
): void {
  const fromMeta = snapshot.index.tablesByApiName.get(definition.from)
  if (fromMeta !== undefined) {
    const ov = plan.tableOverrides.get(fromMeta.id)
    if (ov !== undefined) parts.from.physicalName = ov
  }

  if (definition.joins !== undefined) {
    for (let i = 0; i < parts.joins.length && i < definition.joins.length; i++) {
      const jd = definition.joins[i]
      const jc = parts.joins[i]
      if (jd !== undefined && jc !== undefined) {
        const jm = snapshot.index.tablesByApiName.get(jd.table)
        if (jm !== undefined) {
          const ov = plan.tableOverrides.get(jm.id)
          if (ov !== undefined) jc.table.physicalName = ov
        }
      }
    }
  }

  // Override EXISTS subquery tables
  const existsMap = collectExistsTableMap(definition, snapshot)
  walkWhereSubqueries(parts.where, (sub) => {
    const meta = existsMap.get(sub.from.physicalName)
    if (meta !== undefined) {
      const ov = plan.tableOverrides.get(meta.id)
      if (ov !== undefined) sub.from.physicalName = ov
    }
  })
}

// ── Catalog Qualifiers (P3 Trino) ──────────────────────────────

function setCatalogs(parts: SqlParts, plan: TrinoPlan, definition: QueryDefinition, snapshot: RegistrySnapshot): void {
  const fromMeta = snapshot.index.tablesByApiName.get(definition.from)
  if (fromMeta !== undefined) {
    const fc = plan.catalogs.get(fromMeta.database)
    if (fc !== undefined) parts.from.catalog = fc
  }

  if (definition.joins !== undefined) {
    for (let i = 0; i < parts.joins.length && i < definition.joins.length; i++) {
      const jd = definition.joins[i]
      const jc = parts.joins[i]
      if (jd !== undefined && jc !== undefined) {
        const jm = snapshot.index.tablesByApiName.get(jd.table)
        if (jm !== undefined) {
          const cat = plan.catalogs.get(jm.database)
          if (cat !== undefined) jc.table.catalog = cat
        }
      }
    }
  }

  // Set catalogs on EXISTS subquery tables
  const existsMap = collectExistsTableMap(definition, snapshot)
  walkWhereSubqueries(parts.where, (sub) => {
    const meta = existsMap.get(sub.from.physicalName)
    if (meta !== undefined) {
      const cat = plan.catalogs.get(meta.database)
      if (cat !== undefined) sub.from.catalog = cat
    }
  })
}

// ── Iceberg Direct Catalog (P1 with trino dialect) ─────────────

function setDirectTrinoCatalog(parts: SqlParts, plan: DirectPlan, snapshot: RegistrySnapshot): void {
  const db = snapshot.config.databases.find((d) => d.id === plan.database)
  if (db?.trinoCatalog !== undefined) {
    parts.from.catalog = db.trinoCatalog
  }
  // Also set catalogs on EXISTS subquery tables (all same database in P1 direct)
  walkWhereSubqueries(parts.where, (sub) => {
    if (db?.trinoCatalog !== undefined) {
      sub.from.catalog = db.trinoCatalog
    }
  })
}

// ── WHERE Tree Walk — collect EXISTS subquery table refs ────────

function collectExistsTableMap(definition: QueryDefinition, snapshot: RegistrySnapshot): Map<string, TableMeta> {
  const result = new Map<string, TableMeta>()
  type FilterEntry = QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter

  function walk(filters: readonly FilterEntry[]): void {
    for (const f of filters) {
      if ('logic' in f && 'conditions' in f) {
        walk((f as QueryFilterGroup).conditions)
      } else if (!('operator' in f) && !('logic' in f) && !('column' in f) && 'table' in f) {
        const ef = f as QueryExistsFilter
        const t = snapshot.index.tablesByApiName.get(ef.table)
        if (t !== undefined) result.set(t.physicalName, t)
        if (ef.filters !== undefined) walk(ef.filters)
      }
    }
  }

  if (definition.filters !== undefined) walk(definition.filters)
  if (definition.joins !== undefined) {
    for (const join of definition.joins) {
      if (join.filters !== undefined) walk(join.filters)
    }
  }
  return result
}

function walkWhereSubqueries(node: WhereNode | undefined, handler: (sub: CorrelatedSubquery) => void): void {
  if (node === undefined) return
  if ('logic' in node && 'conditions' in node) {
    const group = node as { conditions: WhereNode[] }
    for (const c of group.conditions) walkWhereSubqueries(c, handler)
  } else if ('subquery' in node) {
    const sub = (node as { subquery: CorrelatedSubquery }).subquery
    handler(sub)
    walkWhereSubqueries(sub.where, handler)
  }
}

// ── Ping / Health / Close ──────────────────────────────────────

function resolveEngineById(id: string, snapshot: RegistrySnapshot): 'postgres' | 'clickhouse' | 'trino' | undefined {
  const db = snapshot.config.databases.find((d) => d.id === id)
  if (!db) return undefined
  if (db.engine === 'iceberg') return 'trino'
  return db.engine
}

async function pingAllOrThrow(
  executors: Record<string, DbExecutor>,
  cacheProviders: Record<string, CacheProvider>,
  snapshot: RegistrySnapshot,
): Promise<void> {
  const unreachable: Array<{
    id: string
    type: 'executor' | 'cache'
    engine?: 'postgres' | 'clickhouse' | 'trino' | 'redis' | undefined
    cause?: Error | undefined
  }> = []

  for (const [id, ex] of Object.entries(executors)) {
    try {
      await ex.ping()
    } catch (err) {
      const engine = id === 'trino' ? 'trino' : resolveEngineById(id, snapshot)
      unreachable.push({ id, type: 'executor', engine, cause: err instanceof Error ? err : undefined })
    }
  }
  for (const [id, cp] of Object.entries(cacheProviders)) {
    try {
      await cp.ping()
    } catch (err) {
      unreachable.push({ id, type: 'cache', engine: 'redis', cause: err instanceof Error ? err : undefined })
    }
  }

  if (unreachable.length > 0) {
    throw new ConnectionError('CONNECTION_FAILED', `Unreachable: ${unreachable.map((u) => u.id).join(', ')}`, {
      unreachable,
    })
  }
}

async function measureHealth(
  executors: Record<string, DbExecutor>,
  cacheProviders: Record<string, CacheProvider>,
): Promise<HealthCheckResult> {
  const exResult: Record<string, { healthy: boolean; latencyMs: number; error?: string | undefined }> = {}
  const cpResult: Record<string, { healthy: boolean; latencyMs: number; error?: string | undefined }> = {}
  let healthy = true

  for (const [id, ex] of Object.entries(executors)) {
    const s = Date.now()
    try {
      await ex.ping()
      exResult[id] = { healthy: true, latencyMs: Date.now() - s }
    } catch (err) {
      healthy = false
      exResult[id] = {
        healthy: false,
        latencyMs: Date.now() - s,
        error: err instanceof Error ? err.message : String(err),
      }
    }
  }

  for (const [id, cp] of Object.entries(cacheProviders)) {
    const s = Date.now()
    try {
      await cp.ping()
      cpResult[id] = { healthy: true, latencyMs: Date.now() - s }
    } catch (err) {
      healthy = false
      cpResult[id] = {
        healthy: false,
        latencyMs: Date.now() - s,
        error: err instanceof Error ? err.message : String(err),
      }
    }
  }

  return { healthy, executors: exResult, cacheProviders: cpResult }
}

async function closeAll(
  executors: Record<string, DbExecutor>,
  cacheProviders: Record<string, CacheProvider>,
  snapshot: RegistrySnapshot,
): Promise<void> {
  const failures: Array<{
    id: string
    type: 'executor' | 'cache'
    engine?: 'postgres' | 'clickhouse' | 'trino' | 'redis' | undefined
    cause?: Error | undefined
  }> = []

  for (const [id, ex] of Object.entries(executors)) {
    try {
      await ex.close()
    } catch (err) {
      const engine = id === 'trino' ? 'trino' : resolveEngineById(id, snapshot)
      failures.push({ id, type: 'executor', engine, cause: err instanceof Error ? err : undefined })
    }
  }
  for (const [id, cp] of Object.entries(cacheProviders)) {
    try {
      await cp.close()
    } catch (err) {
      failures.push({ id, type: 'cache', engine: 'redis', cause: err instanceof Error ? err : undefined })
    }
  }

  if (failures.length > 0) {
    throw new ConnectionError('CONNECTION_FAILED', `Failed to close: ${failures.map((f) => f.id).join(', ')}`, {
      unreachable: failures,
    })
  }
}

// ── Error Helpers ──────────────────────────────────────────────

function toExecError(
  err: unknown,
  database: string,
  dialect: DialectName,
  gen: { sql: string; params: unknown[] },
): ExecutionError {
  const cause = err instanceof Error ? err : new Error(String(err))
  if (isTimeout(cause)) {
    return new ExecutionError({
      code: 'QUERY_TIMEOUT',
      database,
      dialect,
      sql: gen.sql,
      timeoutMs: extractTimeoutMs(cause),
    })
  }
  return new ExecutionError(
    { code: 'QUERY_FAILED', database, dialect, sql: gen.sql, params: [...gen.params], cause },
    cause,
  )
}

function isTimeout(err: Error): boolean {
  const m = err.message.toLowerCase()
  return m.includes('timeout') || m.includes('statement_timeout') || m.includes('max_execution_time')
}

function extractTimeoutMs(err: Error): number {
  if ('timeoutMs' in err && typeof (err as Record<string, unknown>).timeoutMs === 'number') {
    return (err as Record<string, unknown>).timeoutMs as number
  }
  return 0
}
