import type {
  WhereArrayCondition,
  WhereBetween,
  WhereColumnCondition,
  WhereCountedSubquery,
  WhereExists,
  WhereFunction,
  WhereGroup,
  WhereNode,
} from '../types/ir.js'

// ── WhereNode type guards (shared across all dialects) ─────────

export function isGroup(n: WhereNode): n is WhereGroup {
  return 'logic' in n && 'conditions' in n
}

export function isExists(n: WhereNode): n is WhereExists {
  return 'exists' in n && 'subquery' in n
}

export function isCounted(n: WhereNode): n is WhereCountedSubquery {
  return 'countParamIndex' in n && 'subquery' in n
}

export function isColCond(n: WhereNode): n is WhereColumnCondition {
  return 'leftColumn' in n && 'rightColumn' in n
}

export function isFn(n: WhereNode): n is WhereFunction {
  return 'fn' in n && 'fnParamIndex' in n
}

export function isBetween(n: WhereNode): n is WhereBetween {
  return 'fromParamIndex' in n && 'toParamIndex' in n && !('alias' in n)
}

export function isArrayCond(n: WhereNode): n is WhereArrayCondition {
  return 'elementType' in n
}

// ── Shared SQL helpers ─────────────────────────────────────────

export function escapeLike(value: string): string {
  return value.replace(/[%_\\]/g, '\\$&')
}

const VALID_AGG_FNS = new Set(['count', 'sum', 'avg', 'min', 'max'])

/** Whitelist aggregation function name; defaults to COUNT if invalid. */
export function safeAggFn(fn: string): string {
  const lower = fn.toLowerCase()
  return VALID_AGG_FNS.has(lower) ? lower.toUpperCase() : 'COUNT'
}

/** Escape a double-quoted SQL identifier (PG/Trino) by doubling internal double-quotes. */
export function escapeIdentDQ(value: string): string {
  return value.replace(/"/g, '""')
}

/** Escape a backtick-quoted SQL identifier (ClickHouse) by doubling internal backticks. */
export function escapeIdentBT(value: string): string {
  return value.replace(/`/g, '``')
}

const VALID_WHERE_FNS = new Set(['levenshtein', 'levenshtein_distance', 'editdistance'])

/** Whitelist where-clause function name; throws if invalid. */
export function safeWhereFn(fn: string): string {
  if (!VALID_WHERE_FNS.has(fn.toLowerCase())) {
    throw new Error(`Unsupported where function: ${fn}`)
  }
  return fn
}
