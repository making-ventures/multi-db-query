import type { ValidationErrorEntry } from '../errors.js'
import { ValidationError } from '../errors.js'
import type { MetadataIndex } from '../metadataIndex.js'
import type { ExecutionContext } from '../types/context.js'
import type { ColumnMeta, RoleMeta, TableMeta } from '../types/metadata.js'
import type {
  FilterOperator,
  QueryAggregation,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
} from '../types/query.js'

// --- Operator / Type Compatibility ---

const ORDERABLE_TYPES = new Set(['string', 'int', 'decimal', 'date', 'timestamp'])
const COMPARISON_OPS = new Set<FilterOperator>(['>', '<', '>=', '<='])
const IN_OPS = new Set<FilterOperator>(['in', 'notIn'])
const BETWEEN_OPS = new Set<FilterOperator>(['between', 'notBetween'])
const PATTERN_OPS = new Set<FilterOperator>([
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
const NULL_OPS = new Set<FilterOperator>(['isNull', 'isNotNull'])
const LEVENSHTEIN_OPS = new Set<FilterOperator>(['levenshteinLte'])
const ARRAY_OPS = new Set<FilterOperator>([
  'arrayContains',
  'arrayContainsAll',
  'arrayContainsAny',
  'arrayIsEmpty',
  'arrayIsNotEmpty',
])
const HAVING_ALLOWED_OPS = new Set<FilterOperator>([
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

function isArrayType(type: string): boolean {
  return type.endsWith('[]')
}

function getElementType(type: string): string {
  return type.slice(0, -2)
}

function isOperatorValidForType(op: FilterOperator, colType: string): boolean {
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

// --- Access Control Helpers ---

interface EffectiveAccess {
  allowed: boolean
  allowedColumns: Set<string> | '*'
}

function computeEffectiveAccess(
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

function mergeAccess(scopes: EffectiveAccess[]): EffectiveAccess {
  const nonEmpty = scopes.filter((s) => s.allowed)
  if (nonEmpty.length === 0) {
    return { allowed: false, allowedColumns: new Set() }
  }
  const first = nonEmpty[0]
  if (first === undefined || nonEmpty.length === 1) {
    return first ?? { allowed: false, allowedColumns: new Set() }
  }

  // INTERSECTION between scopes
  let result: Set<string> | '*' = first.allowedColumns

  for (let i = 1; i < nonEmpty.length; i++) {
    const item = nonEmpty[i]
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

function getEffectiveAccess(tableId: string, context: ExecutionContext, roles: readonly RoleMeta[]): EffectiveAccess {
  const scopes: EffectiveAccess[] = []

  if (context.roles.user !== undefined && context.roles.user.length > 0) {
    scopes.push(computeEffectiveAccess(tableId, roles, context.roles.user))
  }
  if (context.roles.service !== undefined && context.roles.service.length > 0) {
    scopes.push(computeEffectiveAccess(tableId, roles, context.roles.service))
  }

  if (scopes.length === 0) {
    return { allowed: true, allowedColumns: '*' }
  }

  return mergeAccess(scopes)
}

function isColumnAllowed(access: EffectiveAccess, column: string): boolean {
  if (access.allowedColumns === '*') return true
  return access.allowedColumns.has(column)
}

// --- Table Qualifier Helpers ---

function resolveTableForFilter(
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

// --- Value Type Checking ---

function matchesColumnType(value: unknown, colType: string): boolean {
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

// --- Main Validation ---

export function validateQuery(
  definition: QueryDefinition,
  context: ExecutionContext,
  index: MetadataIndex,
  roles: readonly RoleMeta[],
): ValidationError | null {
  const errors: ValidationErrorEntry[] = []

  // Rule 13 — Role existence
  const allRoleIds: string[] = [...(context.roles.user ?? []), ...(context.roles.service ?? [])]
  for (const roleId of allRoleIds) {
    if (index.getRole(roleId) === undefined) {
      errors.push({
        code: 'UNKNOWN_ROLE',
        message: `Unknown role ID '${roleId}'`,
        details: { role: roleId },
      })
    }
  }

  // Rule 1 — Table existence
  const fromTable = index.getTable(definition.from)
  if (fromTable === undefined) {
    errors.push({
      code: 'UNKNOWN_TABLE',
      message: `Unknown table '${definition.from}'`,
      details: { table: definition.from },
    })
    return new ValidationError(definition.from, errors)
  }

  // Rule 3 — Role permission on from table
  const fromAccess = getEffectiveAccess(fromTable.id, context, roles)
  if (!fromAccess.allowed) {
    errors.push({
      code: 'ACCESS_DENIED',
      message: `Access denied for table '${definition.from}'`,
      details: { table: definition.from },
    })
  }

  // Build joined tables map
  const joinedTables = new Map<string, TableMeta>()
  const joinedAccessMap = new Map<string, EffectiveAccess>()

  // Rule 6 — Join validity
  if (definition.joins !== undefined) {
    for (const join of definition.joins) {
      const joinTable = index.getTable(join.table)
      if (joinTable === undefined) {
        errors.push({
          code: 'UNKNOWN_TABLE',
          message: `Joined table '${join.table}' does not exist`,
          details: { table: join.table },
        })
        continue
      }

      const hasRelation =
        fromTable.relations.some((r) => r.references.table === joinTable.id || r.references.table === join.table) ||
        joinTable.relations.some((r) => r.references.table === fromTable.id || r.references.table === definition.from)

      if (!hasRelation) {
        errors.push({
          code: 'INVALID_JOIN',
          message: `No relation between '${definition.from}' and '${join.table}'`,
          details: { table: join.table },
        })
      }

      joinedTables.set(join.table, joinTable)

      const jAccess = getEffectiveAccess(joinTable.id, context, roles)
      joinedAccessMap.set(join.table, jAccess)
      if (!jAccess.allowed) {
        errors.push({
          code: 'ACCESS_DENIED',
          message: `Access denied for joined table '${join.table}'`,
          details: { table: join.table },
        })
      }

      if (join.columns !== undefined) {
        for (const col of join.columns) {
          const colMeta = index.getColumn(joinTable.id, col)
          if (colMeta === undefined) {
            errors.push({
              code: 'UNKNOWN_COLUMN',
              message: `Column '${col}' does not exist in table '${join.table}'`,
              details: { table: join.table, column: col },
            })
          } else if (!isColumnAllowed(jAccess, col)) {
            errors.push({
              code: 'ACCESS_DENIED',
              message: `Column '${col}' in table '${join.table}' is not allowed`,
              details: { table: join.table, column: col },
            })
          }
        }
      }

      if (join.filters !== undefined) {
        validateFilters(
          join.filters,
          fromTable,
          joinedTables,
          index,
          context,
          roles,
          fromAccess,
          joinedAccessMap,
          errors,
        )
      }
    }
  }

  // Aggregation alias map
  const aggAliases = new Map<string, QueryAggregation>()

  // Rule 14 — Aggregation validity
  if (definition.aggregations !== undefined) {
    const seenAliases = new Set<string>()
    for (const agg of definition.aggregations) {
      if (seenAliases.has(agg.alias)) {
        errors.push({
          code: 'INVALID_AGGREGATION',
          message: `Duplicate aggregation alias '${agg.alias}'`,
          details: { alias: agg.alias },
        })
      } else {
        seenAliases.add(agg.alias)
      }

      const aggTable =
        agg.table !== undefined ? resolveAggTable(agg.table, fromTable, joinedTables, errors, agg.alias) : fromTable

      if (aggTable !== undefined && agg.column !== '*') {
        const colMeta = index.getColumn(aggTable.id, agg.column)
        if (colMeta === undefined) {
          errors.push({
            code: 'UNKNOWN_COLUMN',
            message: `Aggregation column '${agg.column}' does not exist in table '${aggTable.apiName}'`,
            details: { column: agg.column, table: aggTable.apiName, alias: agg.alias },
          })
        } else if (isArrayType(colMeta.type) && agg.fn !== 'count') {
          errors.push({
            code: 'INVALID_AGGREGATION',
            message: `${agg.fn} on array column '${agg.column}' is not valid (only count is allowed)`,
            details: { column: agg.column, alias: agg.alias, operator: agg.fn },
          })
        }
      }

      aggAliases.set(agg.alias, agg)
    }
  }

  // Rule 2 — Column existence on from table
  // Rule 4 — Column permission
  const resultColumns = new Set<string>()
  if (definition.columns !== undefined) {
    if (definition.columns.length === 0 && definition.aggregations === undefined) {
      errors.push({
        code: 'INVALID_AGGREGATION',
        message: 'Empty columns[] requires aggregations',
        details: {},
      })
    }
    for (const col of definition.columns) {
      const colMeta = index.getColumn(fromTable.id, col)
      if (colMeta === undefined) {
        errors.push({
          code: 'UNKNOWN_COLUMN',
          message: `Column '${col}' does not exist in table '${definition.from}'`,
          details: { table: definition.from, column: col },
        })
      } else {
        if (!isColumnAllowed(fromAccess, col)) {
          errors.push({
            code: 'ACCESS_DENIED',
            message: `Column '${col}' in table '${definition.from}' is not allowed`,
            details: { table: definition.from, column: col },
          })
        }
        resultColumns.add(col)
      }
    }
  }

  // Rule 14 — Alias/column collisions
  if (definition.aggregations !== undefined) {
    for (const agg of definition.aggregations) {
      if (resultColumns.has(agg.alias)) {
        errors.push({
          code: 'INVALID_AGGREGATION',
          message: `Aggregation alias '${agg.alias}' collides with column name in result set`,
          details: { alias: agg.alias },
        })
      }
    }
  }

  // Rule 5 — Filter validity
  if (definition.filters !== undefined) {
    validateFilters(
      definition.filters,
      fromTable,
      joinedTables,
      index,
      context,
      roles,
      fromAccess,
      joinedAccessMap,
      errors,
    )
  }

  // Rule 7 — Group By validity
  if (definition.groupBy !== undefined) {
    for (const gb of definition.groupBy) {
      const gbTable =
        gb.table !== undefined
          ? resolveTableForGroupBy(gb.table, fromTable, joinedTables, errors, gb.column)
          : fromTable

      if (gbTable !== undefined) {
        const colMeta = index.getColumn(gbTable.id, gb.column)
        if (colMeta === undefined) {
          errors.push({
            code: 'UNKNOWN_COLUMN',
            message: `GroupBy column '${gb.column}' does not exist in table '${gbTable.apiName}'`,
            details: { table: gbTable.apiName, column: gb.column },
          })
        } else if (isArrayType(colMeta.type)) {
          errors.push({
            code: 'INVALID_GROUP_BY',
            message: `Array column '${gb.column}' cannot be used in GROUP BY`,
            details: { column: gb.column, table: gbTable.apiName },
          })
        }
      }
    }
  }

  // Rule 7 — ungrouped column check
  if ((definition.groupBy !== undefined || definition.aggregations !== undefined) && definition.columns !== undefined) {
    const groupedCols = new Set(
      (definition.groupBy ?? [])
        .filter((gb) => gb.table === undefined || gb.table === fromTable.apiName)
        .map((gb) => gb.column),
    )

    for (const col of definition.columns) {
      if (!groupedCols.has(col) && !aggAliases.has(col)) {
        errors.push({
          code: 'INVALID_GROUP_BY',
          message: `Column '${col}' is in columns but not in groupBy and is not an aggregation alias`,
          details: { column: col },
        })
      }
    }
  }

  // Rule 8 — Having validity
  if (definition.having !== undefined) {
    validateHaving(definition.having, aggAliases, errors)
  }

  // Rule 9 — Order By validity
  if (definition.orderBy !== undefined) {
    for (const ob of definition.orderBy) {
      if (aggAliases.has(ob.column)) continue

      const obTable =
        ob.table !== undefined
          ? resolveTableForOrderBy(ob.table, fromTable, joinedTables, errors, ob.column)
          : fromTable

      if (obTable !== undefined) {
        const colMeta = index.getColumn(obTable.id, ob.column)
        if (colMeta === undefined) {
          errors.push({
            code: 'INVALID_ORDER_BY',
            message: `OrderBy column '${ob.column}' does not exist in table '${obTable.apiName}'`,
            details: { table: obTable.apiName, column: ob.column },
          })
        } else if (isArrayType(colMeta.type)) {
          errors.push({
            code: 'INVALID_ORDER_BY',
            message: `Array column '${ob.column}' cannot be used in ORDER BY`,
            details: { column: ob.column, table: obTable.apiName },
          })
        }
      }
    }
  }

  // Rule 10 — ByIds validity
  if (definition.byIds !== undefined) {
    if (definition.byIds.length === 0) {
      errors.push({
        code: 'INVALID_BY_IDS',
        message: 'byIds requires a non-empty array',
        details: {},
      })
    }
    if (fromTable.primaryKey.length !== 1) {
      errors.push({
        code: 'INVALID_BY_IDS',
        message: `byIds requires a single-column primary key, table '${definition.from}' has ${fromTable.primaryKey.length}`,
        details: { table: definition.from },
      })
    }
    if (definition.groupBy !== undefined) {
      errors.push({
        code: 'INVALID_BY_IDS',
        message: 'byIds cannot be combined with groupBy',
        details: {},
      })
    }
    if (definition.aggregations !== undefined) {
      errors.push({
        code: 'INVALID_BY_IDS',
        message: 'byIds cannot be combined with aggregations',
        details: {},
      })
    }
  }

  // Rule 11 — Limit/Offset validity
  if (definition.limit !== undefined) {
    if (!Number.isInteger(definition.limit) || definition.limit < 0) {
      errors.push({
        code: 'INVALID_LIMIT',
        message: `limit must be a non-negative integer, got ${definition.limit}`,
        details: { actual: String(definition.limit) },
      })
    }
  }
  if (definition.offset !== undefined) {
    if (!Number.isInteger(definition.offset) || definition.offset < 0) {
      errors.push({
        code: 'INVALID_LIMIT',
        message: `offset must be a non-negative integer, got ${definition.offset}`,
        details: { actual: String(definition.offset) },
      })
    }
    if (definition.limit === undefined) {
      errors.push({
        code: 'INVALID_LIMIT',
        message: 'offset requires limit',
        details: {},
      })
    }
  }

  if (errors.length === 0) return null
  return new ValidationError(definition.from, errors)
}

// --- Filter Validation ---

function validateFilters(
  filters: readonly (QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter)[],
  fromTable: TableMeta,
  joinedTables: Map<string, TableMeta>,
  index: MetadataIndex,
  context: ExecutionContext,
  roles: readonly RoleMeta[],
  fromAccess: EffectiveAccess,
  joinedAccessMap: Map<string, EffectiveAccess>,
  errors: ValidationErrorEntry[],
  existsParentTable?: TableMeta | undefined,
): void {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i]
    if (filter === undefined) continue
    validateSingleFilter(
      filter,
      i,
      fromTable,
      joinedTables,
      index,
      context,
      roles,
      fromAccess,
      joinedAccessMap,
      errors,
      existsParentTable,
    )
  }
}

function validateSingleFilter(
  filter: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
  filterIndex: number,
  fromTable: TableMeta,
  joinedTables: Map<string, TableMeta>,
  index: MetadataIndex,
  context: ExecutionContext,
  roles: readonly RoleMeta[],
  fromAccess: EffectiveAccess,
  joinedAccessMap: Map<string, EffectiveAccess>,
  errors: ValidationErrorEntry[],
  existsParentTable?: TableMeta | undefined,
): void {
  if (isFilterGroup(filter)) {
    for (const cond of filter.conditions) {
      validateSingleFilter(
        cond,
        filterIndex,
        fromTable,
        joinedTables,
        index,
        context,
        roles,
        fromAccess,
        joinedAccessMap,
        errors,
        existsParentTable,
      )
    }
    return
  }

  if (isExistsFilter(filter)) {
    validateExistsFilter(filter, filterIndex, fromTable, joinedTables, index, context, roles, errors, existsParentTable)
    return
  }

  if (isColumnFilter(filter)) {
    validateColumnFilter(filter, filterIndex, fromTable, joinedTables, index, fromAccess, joinedAccessMap, errors)
    return
  }

  validateRegularFilter(filter, filterIndex, fromTable, joinedTables, index, fromAccess, joinedAccessMap, errors)
}

function validateRegularFilter(
  filter: QueryFilter,
  filterIndex: number,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  index: MetadataIndex,
  fromAccess: EffectiveAccess,
  joinedAccessMap: ReadonlyMap<string, EffectiveAccess>,
  errors: ValidationErrorEntry[],
): void {
  const table = resolveTableForFilter(filter.table, fromTable, joinedTables, errors, filterIndex)
  if (table === undefined) return

  const colMeta = index.getColumn(table.id, filter.column)
  if (colMeta === undefined) {
    errors.push({
      code: 'UNKNOWN_COLUMN',
      message: `Filter column '${filter.column}' does not exist in table '${table.apiName}'`,
      details: { table: table.apiName, column: filter.column, filterIndex },
    })
    return
  }

  const access = table.apiName === fromTable.apiName ? fromAccess : joinedAccessMap.get(table.apiName)
  if (access !== undefined && !isColumnAllowed(access, filter.column)) {
    errors.push({
      code: 'ACCESS_DENIED',
      message: `Column '${filter.column}' in table '${table.apiName}' is not allowed`,
      details: { table: table.apiName, column: filter.column, filterIndex },
    })
  }

  if (!isOperatorValidForType(filter.operator, colMeta.type)) {
    errors.push({
      code: 'INVALID_FILTER',
      message: `Operator '${filter.operator}' is not valid for column '${filter.column}' of type '${colMeta.type}'`,
      details: { column: filter.column, operator: filter.operator, actual: colMeta.type, filterIndex },
    })
    return
  }

  if (NULL_OPS.has(filter.operator) && !colMeta.nullable) {
    errors.push({
      code: 'INVALID_FILTER',
      message: `Operator '${filter.operator}' requires column '${filter.column}' to be nullable`,
      details: { column: filter.column, operator: filter.operator, filterIndex },
    })
  }

  validateCompoundValue(filter, colMeta, filterIndex, errors)
}

function validateCompoundValue(
  filter: QueryFilter,
  colMeta: ColumnMeta,
  filterIndex: number,
  errors: ValidationErrorEntry[],
): void {
  const { operator, value, column } = filter

  if (BETWEEN_OPS.has(operator)) {
    if (value === undefined || value === null || typeof value !== 'object') {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' requires an object with 'from' and 'to'`,
        details: { column, operator, filterIndex },
      })
      return
    }
    const v = value as Record<string, unknown>
    if (v.from === undefined || v.from === null) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' requires non-null 'from' value`,
        details: { column, operator, filterIndex },
      })
    } else if (!matchesColumnType(v.from, colMeta.type)) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' 'from' value doesn't match column type '${colMeta.type}'`,
        details: { column, operator, filterIndex, expected: colMeta.type },
      })
    }
    if (v.to === undefined || v.to === null) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' requires non-null 'to' value`,
        details: { column, operator, filterIndex },
      })
    } else if (!matchesColumnType(v.to, colMeta.type)) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' 'to' value doesn't match column type '${colMeta.type}'`,
        details: { column, operator, filterIndex, expected: colMeta.type },
      })
    }
    return
  }

  if (IN_OPS.has(operator)) {
    if (!Array.isArray(value)) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' requires an array value`,
        details: { column, operator, filterIndex },
      })
      return
    }
    if (value.length === 0) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `Operator '${operator}' requires a non-empty array`,
        details: { column, operator, filterIndex },
      })
      return
    }
    for (const el of value) {
      if (el === null || el === undefined) {
        errors.push({
          code: 'INVALID_VALUE',
          message: `Operator '${operator}' array must not contain null elements`,
          details: { column, operator, filterIndex },
        })
        break
      }
    }
    for (const el of value) {
      if (el !== null && el !== undefined && !matchesColumnType(el, colMeta.type)) {
        errors.push({
          code: 'INVALID_VALUE',
          message: `Operator '${operator}' array element doesn't match column type '${colMeta.type}'`,
          details: { column, operator, filterIndex, expected: colMeta.type },
        })
        break
      }
    }
    return
  }

  if (operator === 'levenshteinLte') {
    if (value === undefined || value === null || typeof value !== 'object') {
      errors.push({
        code: 'INVALID_VALUE',
        message: 'levenshteinLte requires an object with text and maxDistance',
        details: { column, operator, filterIndex },
      })
      return
    }
    const v = value as Record<string, unknown>
    if (typeof v.maxDistance !== 'number' || !Number.isInteger(v.maxDistance) || v.maxDistance < 0) {
      errors.push({
        code: 'INVALID_VALUE',
        message: 'levenshteinLte maxDistance must be a non-negative integer',
        details: { column, operator, filterIndex },
      })
    }
    return
  }

  if (operator === 'arrayContains') {
    if (isArrayType(colMeta.type) && value !== undefined && !matchesColumnType(value, getElementType(colMeta.type))) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `arrayContains value must match element type '${getElementType(colMeta.type)}'`,
        details: { column, operator, filterIndex, expected: getElementType(colMeta.type) },
      })
    }
    return
  }

  if (operator === 'arrayContainsAll' || operator === 'arrayContainsAny') {
    if (!Array.isArray(value)) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `${operator} requires an array value`,
        details: { column, operator, filterIndex },
      })
      return
    }
    if (value.length === 0) {
      errors.push({
        code: 'INVALID_VALUE',
        message: `${operator} requires a non-empty array`,
        details: { column, operator, filterIndex },
      })
      return
    }
    for (const el of value) {
      if (el === null || el === undefined) {
        errors.push({
          code: 'INVALID_VALUE',
          message: `${operator} array must not contain null elements`,
          details: { column, operator, filterIndex },
        })
        break
      }
    }
    if (isArrayType(colMeta.type)) {
      const et = getElementType(colMeta.type)
      for (const el of value) {
        if (el !== null && el !== undefined && !matchesColumnType(el, et)) {
          errors.push({
            code: 'INVALID_VALUE',
            message: `${operator} array element doesn't match element type '${et}'`,
            details: { column, operator, filterIndex, expected: et },
          })
          break
        }
      }
    }
  }
}

function validateColumnFilter(
  filter: QueryColumnFilter,
  filterIndex: number,
  fromTable: TableMeta,
  joinedTables: ReadonlyMap<string, TableMeta>,
  index: MetadataIndex,
  fromAccess: EffectiveAccess,
  joinedAccessMap: ReadonlyMap<string, EffectiveAccess>,
  errors: ValidationErrorEntry[],
): void {
  const leftTable = resolveTableForFilter(filter.table, fromTable, joinedTables, errors, filterIndex)
  if (leftTable === undefined) return

  const rightTable = resolveTableForFilter(filter.refTable, fromTable, joinedTables, errors, filterIndex)
  if (rightTable === undefined) return

  const leftCol = index.getColumn(leftTable.id, filter.column)
  if (leftCol === undefined) {
    errors.push({
      code: 'UNKNOWN_COLUMN',
      message: `Filter column '${filter.column}' does not exist in table '${leftTable.apiName}'`,
      details: { table: leftTable.apiName, column: filter.column, filterIndex },
    })
  }

  const rightCol = index.getColumn(rightTable.id, filter.refColumn)
  if (rightCol === undefined) {
    errors.push({
      code: 'UNKNOWN_COLUMN',
      message: `Filter refColumn '${filter.refColumn}' does not exist in table '${rightTable.apiName}'`,
      details: {
        table: rightTable.apiName,
        column: filter.refColumn,
        refColumn: filter.refColumn,
        refTable: rightTable.apiName,
        filterIndex,
      },
    })
  }

  const leftAccess = leftTable.apiName === fromTable.apiName ? fromAccess : joinedAccessMap.get(leftTable.apiName)
  if (leftAccess !== undefined && !isColumnAllowed(leftAccess, filter.column)) {
    errors.push({
      code: 'ACCESS_DENIED',
      message: `Column '${filter.column}' is not allowed`,
      details: { column: filter.column, filterIndex },
    })
  }

  const rightAccess = rightTable.apiName === fromTable.apiName ? fromAccess : joinedAccessMap.get(rightTable.apiName)
  if (rightAccess !== undefined && !isColumnAllowed(rightAccess, filter.refColumn)) {
    errors.push({
      code: 'ACCESS_DENIED',
      message: `Column '${filter.refColumn}' is not allowed`,
      details: { column: filter.refColumn, refColumn: filter.refColumn, filterIndex },
    })
  }

  if (leftCol !== undefined && rightCol !== undefined) {
    if (isArrayType(leftCol.type) || isArrayType(rightCol.type)) {
      errors.push({
        code: 'INVALID_FILTER',
        message: 'QueryColumnFilter does not support array columns',
        details: { column: filter.column, refColumn: filter.refColumn, filterIndex },
      })
    } else if (leftCol.type !== rightCol.type) {
      const bothOrderable = ORDERABLE_TYPES.has(leftCol.type) && ORDERABLE_TYPES.has(rightCol.type)
      if (!bothOrderable) {
        errors.push({
          code: 'INVALID_FILTER',
          message: `Incompatible types: '${leftCol.type}' vs '${rightCol.type}'`,
          details: {
            column: filter.column,
            refColumn: filter.refColumn,
            expected: leftCol.type,
            actual: rightCol.type,
            filterIndex,
          },
        })
      }
    }
  }
}

function validateExistsFilter(
  filter: QueryExistsFilter,
  filterIndex: number,
  fromTable: TableMeta,
  _joinedTables: Map<string, TableMeta>,
  index: MetadataIndex,
  context: ExecutionContext,
  roles: readonly RoleMeta[],
  errors: ValidationErrorEntry[],
  existsParentTable?: TableMeta | undefined,
): void {
  const parentTable = existsParentTable ?? fromTable

  const existsTable = index.getTable(filter.table)
  if (existsTable === undefined) {
    errors.push({
      code: 'UNKNOWN_TABLE',
      message: `EXISTS references unknown table '${filter.table}'`,
      details: { table: filter.table, filterIndex },
    })
    return
  }

  const hasRelation =
    parentTable.relations.some((r) => r.references.table === existsTable.id || r.references.table === filter.table) ||
    existsTable.relations.some(
      (r) => r.references.table === parentTable.id || r.references.table === parentTable.apiName,
    )

  if (!hasRelation) {
    errors.push({
      code: 'INVALID_EXISTS',
      message: `No relation between '${parentTable.apiName}' and '${filter.table}'`,
      details: { table: filter.table, filterIndex },
    })
  }

  const existsAccess = getEffectiveAccess(existsTable.id, context, roles)
  if (!existsAccess.allowed) {
    errors.push({
      code: 'ACCESS_DENIED',
      message: `Access denied for EXISTS table '${filter.table}'`,
      details: { table: filter.table, filterIndex },
    })
  }

  if (filter.count !== undefined) {
    if (!Number.isInteger(filter.count.value) || filter.count.value < 0) {
      errors.push({
        code: 'INVALID_EXISTS',
        message: `EXISTS count value must be a non-negative integer, got ${filter.count.value}`,
        details: { table: filter.table, filterIndex },
      })
    }
  }

  if (filter.filters !== undefined) {
    const existsJoinedTables = new Map<string, TableMeta>()
    const existsJoinedAccess = new Map<string, EffectiveAccess>()

    for (const nested of filter.filters) {
      validateSingleFilter(
        nested,
        filterIndex,
        existsTable,
        existsJoinedTables,
        index,
        context,
        roles,
        existsAccess,
        existsJoinedAccess,
        errors,
        existsTable,
      )
    }
  }
}

// --- Having Validation ---

function validateHaving(
  having: readonly (QueryFilter | QueryFilterGroup)[],
  aggAliases: ReadonlyMap<string, QueryAggregation>,
  errors: ValidationErrorEntry[],
): void {
  for (let i = 0; i < having.length; i++) {
    const filter = having[i]
    if (filter === undefined) continue
    validateHavingFilter(filter, i, aggAliases, errors)
  }
}

function validateHavingFilter(
  filter: QueryFilter | QueryFilterGroup,
  filterIndex: number,
  aggAliases: ReadonlyMap<string, QueryAggregation>,
  errors: ValidationErrorEntry[],
): void {
  if (isFilterGroup(filter)) {
    for (const cond of filter.conditions) {
      if (isColumnFilter(cond)) {
        errors.push({
          code: 'INVALID_HAVING',
          message: 'QueryColumnFilter is not allowed in HAVING',
          details: { filterIndex },
        })
        continue
      }
      if (isExistsFilter(cond)) {
        errors.push({
          code: 'INVALID_HAVING',
          message: 'QueryExistsFilter is not allowed in HAVING',
          details: { filterIndex },
        })
        continue
      }
      validateHavingFilter(cond, filterIndex, aggAliases, errors)
    }
    return
  }

  const f = filter as QueryFilter

  if (f.table !== undefined) {
    errors.push({
      code: 'INVALID_HAVING',
      message: 'table qualifier is not allowed in HAVING filters',
      details: { table: f.table, filterIndex },
    })
  }

  if (!aggAliases.has(f.column)) {
    errors.push({
      code: 'INVALID_HAVING',
      message: `HAVING column '${f.column}' is not an aggregation alias`,
      details: { alias: f.column, filterIndex },
    })
  }

  if (!HAVING_ALLOWED_OPS.has(f.operator)) {
    errors.push({
      code: 'INVALID_HAVING',
      message: `Operator '${f.operator}' is not allowed in HAVING`,
      details: { operator: f.operator, filterIndex },
    })
  }
}

// --- Type Guards ---

function isFilterGroup(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryFilterGroup {
  return 'logic' in f && 'conditions' in f
}

function isExistsFilter(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryExistsFilter {
  return 'table' in f && !('operator' in f) && !('logic' in f) && !('column' in f)
}

function isColumnFilter(
  f: QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter,
): f is QueryColumnFilter {
  return 'refColumn' in f
}

// --- Table Resolution Helpers ---

function resolveAggTable(
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

function resolveTableForGroupBy(
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

function resolveTableForOrderBy(
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
