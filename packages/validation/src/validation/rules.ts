import type { ValidationErrorEntry } from '../errors.js'
import type { ExecutionContext } from '../types/context.js'
import type { RoleMeta, TableMeta } from '../types/metadata.js'
import type {
  FilterOperator,
  QueryColumnFilter,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
} from '../types/query.js'

// ── Enum Validation Constants ──────────────────────────────────

export const VALID_DIRECTIONS = new Set(['asc', 'desc'])
export const VALID_AGG_FNS = new Set(['count', 'sum', 'avg', 'min', 'max'])
export const VALID_FILTER_OPS = new Set<string>([
  '=',
  '!=',
  '>',
  '<',
  '>=',
  '<=',
  'in',
  'notIn',
  'like',
  'notLike',
  'ilike',
  'notIlike',
  'isNull',
  'isNotNull',
  'between',
  'notBetween',
  'contains',
  'icontains',
  'notContains',
  'notIcontains',
  'startsWith',
  'istartsWith',
  'endsWith',
  'iendsWith',
  'levenshteinLte',
  'arrayContains',
  'arrayContainsAll',
  'arrayContainsAny',
  'arrayIsEmpty',
  'arrayIsNotEmpty',
])
export const VALID_COLUMN_FILTER_OPS = new Set(['=', '!=', '>', '<', '>=', '<='])
export const VALID_EXISTS_COUNT_OPS = new Set(['=', '!=', '>', '<', '>=', '<='])
export const VALID_LOGIC_OPS = new Set(['and', 'or'])

// ── Operator / Type Constants ──────────────────────────────────

export const ORDERABLE_TYPES = new Set(['string', 'int', 'decimal', 'date', 'timestamp'])
export const COMPARISON_OPS = new Set<FilterOperator>(['>', '<', '>=', '<='])
export const IN_OPS = new Set<FilterOperator>(['in', 'notIn'])
export const BETWEEN_OPS = new Set<FilterOperator>(['between', 'notBetween'])
export const PATTERN_OPS = new Set<FilterOperator>([
  'like',
  'notLike',
  'ilike',
  'notIlike',
  'contains',
  'icontains',
  'notContains',
  'notIcontains',
  'startsWith',
  'istartsWith',
  'endsWith',
  'iendsWith',
])
export const NULL_OPS = new Set<FilterOperator>(['isNull', 'isNotNull'])
export const LEVENSHTEIN_OPS = new Set<FilterOperator>(['levenshteinLte'])
export const ARRAY_OPS = new Set<FilterOperator>([
  'arrayContains',
  'arrayContainsAll',
  'arrayContainsAny',
  'arrayIsEmpty',
  'arrayIsNotEmpty',
])
export const HAVING_ALLOWED_OPS = new Set<FilterOperator>([
  '=',
  '!=',
  '>',
  '<',
  '>=',
  '<=',
  'in',
  'notIn',
  'between',
  'notBetween',
  'isNull',
  'isNotNull',
])

// ── Type Helpers ───────────────────────────────────────────────

export function isArrayType(type: string): boolean {
  return type.endsWith('[]')
}

export function getElementType(type: string): string {
  return type.slice(0, -2)
}

export function isOperatorValidForType(op: FilterOperator, colType: string): boolean {
  if (isArrayType(colType)) {
    if (ARRAY_OPS.has(op)) return true
    if (NULL_OPS.has(op)) return true
    return false
  }

  if (ARRAY_OPS.has(op)) return false

  if (op === '=' || op === '!=') return true
  if (NULL_OPS.has(op)) return true

  if (COMPARISON_OPS.has(op) || BETWEEN_OPS.has(op)) {
    return ORDERABLE_TYPES.has(colType)
  }
  if (IN_OPS.has(op)) {
    return colType === 'string' || colType === 'int' || colType === 'decimal' || colType === 'uuid'
  }
  if (PATTERN_OPS.has(op) || LEVENSHTEIN_OPS.has(op)) {
    return colType === 'string'
  }

  return false
}

export function matchesColumnType(value: unknown, colType: string): boolean {
  const baseType = isArrayType(colType) ? getElementType(colType) : colType
  switch (baseType) {
    case 'string':
    case 'uuid':
      return typeof value === 'string'
    case 'int':
      return typeof value === 'number' && Number.isInteger(value)
    case 'decimal':
      return typeof value === 'number'
    case 'boolean':
      return typeof value === 'boolean'
    case 'date':
    case 'timestamp':
      return typeof value === 'string'
    default:
      return true
  }
}

// ── Access Control Helpers ─────────────────────────────────────

export interface EffectiveAccess {
  allowed: boolean
  allowedColumns: Set<string> | '*'
}

export function computeEffectiveAccess(
  tableId: string,
  roles: readonly RoleMeta[],
  scopeRoleIds: readonly string[],
): EffectiveAccess {
  let allowed = false
  let allowedColumns: Set<string> | '*' = new Set<string>()

  for (const roleId of scopeRoleIds) {
    const role = roles.find((r) => r.id === roleId)
    if (role === undefined) continue

    if (role.tables === '*') {
      allowed = true
      allowedColumns = '*'
      continue
    }

    const ta = role.tables.find((t) => t.tableId === tableId)
    if (ta === undefined) continue

    allowed = true
    if (ta.allowedColumns === '*') {
      allowedColumns = '*'
    } else if (allowedColumns !== '*') {
      for (const c of ta.allowedColumns) {
        allowedColumns.add(c)
      }
    }
  }

  return { allowed, allowedColumns }
}

export function mergeAccess(scopes: EffectiveAccess[]): EffectiveAccess {
  // All scopes must allow access (intersection = most restrictive)
  if (scopes.length === 0 || scopes.some((s) => !s.allowed)) {
    return { allowed: false, allowedColumns: new Set() }
  }
  const first = scopes[0]
  if (first === undefined || scopes.length === 1) {
    return first ?? { allowed: false, allowedColumns: new Set() }
  }

  // INTERSECTION between scopes
  let result: Set<string> | '*' = first.allowedColumns

  for (let i = 1; i < scopes.length; i++) {
    const item = scopes[i]
    if (item === undefined) continue
    const next = item.allowedColumns
    if (result === '*' && next === '*') {
      result = '*'
    } else if (result === '*') {
      result = new Set(next as Set<string>)
    } else if (next === '*') {
      // result stays as-is
    } else {
      const intersection = new Set<string>()
      for (const c of result) {
        if (next.has(c)) {
          intersection.add(c)
        }
      }
      result = intersection
    }
  }

  return { allowed: true, allowedColumns: result }
}

export function getEffectiveAccess(
  tableId: string,
  context: ExecutionContext,
  roles: readonly RoleMeta[],
): EffectiveAccess {
  const scopes: EffectiveAccess[] = []

  if (context.roles.user !== undefined) {
    scopes.push(computeEffectiveAccess(tableId, roles, context.roles.user))
  }
  if (context.roles.service !== undefined) {
    scopes.push(computeEffectiveAccess(tableId, roles, context.roles.service))
  }

  if (scopes.length === 0) {
    return { allowed: true, allowedColumns: '*' }
  }

  return mergeAccess(scopes)
}

export function isColumnAllowed(access: EffectiveAccess, column: string): boolean {
  if (access.allowedColumns === '*') return true
  return access.allowedColumns.has(column)
}

// ── Filter Type Guards ─────────────────────────────────────────

export function isFilterGroup(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryFilterGroup {
  return 'logic' in f && 'conditions' in f
}

export function isExistsFilter(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryExistsFilter {
  return 'table' in f && !('operator' in f) && !('logic' in f) && !('column' in f)
}

export function isColumnFilter(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryColumnFilter {
  return 'refColumn' in f
}

// ── Table Resolution Helpers ───────────────────────────────────

export function resolveTableForFilter(
  tableName: string | undefined,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  errors: ValidationErrorEntry[],
  filterIndex: number,
): TableMeta | undefined {
  if (tableName === undefined) return fromTable

  if (tableName === fromTable.apiName) return fromTable

  const joined = joinedTables.get(tableName)
  if (joined !== undefined) return joined

  errors.push({
    code: 'INVALID_FILTER',
    message: `Filter references table '${tableName}' which is not the from table or a joined table`,
    details: { table: tableName, filterIndex },
  })
  return undefined
}

export function resolveAggTable(
  tableName: string,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  errors: ValidationErrorEntry[],
  alias: string,
): TableMeta | undefined {
  if (tableName === fromTable.apiName) return fromTable
  const joined = joinedTables.get(tableName)
  if (joined !== undefined) return joined

  errors.push({
    code: 'INVALID_AGGREGATION',
    message: `Aggregation table '${tableName}' is not the from table or a joined table`,
    details: { table: tableName, alias },
  })
  return undefined
}

export function resolveTableForGroupBy(
  tableName: string,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  errors: ValidationErrorEntry[],
  column: string,
): TableMeta | undefined {
  if (tableName === fromTable.apiName) return fromTable
  const joined = joinedTables.get(tableName)
  if (joined !== undefined) return joined

  errors.push({
    code: 'INVALID_GROUP_BY',
    message: `GroupBy table '${tableName}' is not the from table or a joined table`,
    details: { table: tableName, column },
  })
  return undefined
}

export function resolveTableForOrderBy(
  tableName: string,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  errors: ValidationErrorEntry[],
  column: string,
): TableMeta | undefined {
  if (tableName === fromTable.apiName) return fromTable
  const joined = joinedTables.get(tableName)
  if (joined !== undefined) return joined

  errors.push({
    code: 'INVALID_ORDER_BY',
    message: `OrderBy table '${tableName}' is not the from table or a joined table`,
    details: { table: tableName, column },
  })
  return undefined
}
