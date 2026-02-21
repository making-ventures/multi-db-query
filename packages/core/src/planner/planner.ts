import type { QueryDefinition } from '@mkven/multi-db-validation'
import type { RegistrySnapshot } from '../metadata/registry.js'
import { collectTables, throwPlannerError, tryCache, tryDirect, tryMaterialized, tryTrino } from './strategies.js'

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
