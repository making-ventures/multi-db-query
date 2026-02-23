import {
  escapeIdentBT,
  escapeLike,
  isArrayCond,
  isBetween,
  isColCond,
  isCounted,
  isExists,
  isFn,
  isGroup,
  safeAggFn,
  safeWhereFn,
} from '../generator/fragments.js'
import type {
  AggregationClause,
  ColumnRef,
  CorrelatedSubquery,
  HavingBetween,
  HavingGroup,
  HavingNode,
  JoinClause,
  OrderByClause,
  SqlParts,
  TableRef,
  WhereArrayCondition,
  WhereBetween,
  WhereColumnCondition,
  WhereCondition,
  WhereCountedSubquery,
  WhereExists,
  WhereFunction,
  WhereGroup,
  WhereNode,
} from '../types/ir.js'
import type { SqlDialect } from './dialect.js'

// --- ClickHouse Dialect ---

export class ClickHouseDialect implements SqlDialect {
  generate(parts: SqlParts, params: unknown[]): { sql: string; params: unknown[] } {
    const gen = new ChGenerator(params)
    const sql = gen.build(parts)
    return { sql, params: gen.outParams }
  }
}

// --- Internal generator ---

class ChGenerator {
  readonly outParams: unknown[] = []
  private paramCounter = 0
  private readonly input: unknown[]

  constructor(inputParams: unknown[]) {
    this.input = inputParams
  }

  build(parts: SqlParts): string {
    const clauses: string[] = []

    clauses.push(this.selectClause(parts))
    clauses.push(`FROM ${quoteTable(parts.from)}`)

    for (const j of parts.joins) {
      clauses.push(this.joinClause(j))
    }

    if (parts.where !== undefined) {
      clauses.push(`WHERE ${this.whereNode(parts.where)}`)
    }

    if (parts.groupBy.length > 0) {
      clauses.push(`GROUP BY ${parts.groupBy.map((c) => quoteCol(c)).join(', ')}`)
    }

    if (parts.having !== undefined) {
      clauses.push(`HAVING ${this.havingNode(parts.having)}`)
    }

    if (parts.orderBy.length > 0) {
      clauses.push(`ORDER BY ${parts.orderBy.map((o) => this.orderByClause(o)).join(', ')}`)
    }

    if (parts.limit !== undefined) {
      clauses.push(`LIMIT ${String(parts.limit)}`)
    }

    if (parts.offset !== undefined) {
      clauses.push(`OFFSET ${String(parts.offset)}`)
    }

    return clauses.join(' ')
  }

  // --- SELECT ---

  private selectClause(parts: SqlParts): string {
    if (parts.countMode === true) {
      return 'SELECT COUNT(*)'
    }

    const items: string[] = []
    for (const col of parts.select) {
      items.push(`${quoteCol(col)} AS \`${col.tableAlias}__${col.columnName}\``)
    }
    for (const a of parts.aggregations) {
      items.push(this.aggClause(a))
    }

    if (items.length === 0) {
      return 'SELECT *'
    }

    const distinct = parts.distinct === true ? 'DISTINCT ' : ''
    return `SELECT ${distinct}${items.join(', ')}`
  }

  // --- Aggregation ---

  private aggClause(a: AggregationClause): string {
    const fn = safeAggFn(a.fn)
    const col = a.column === '*' ? '*' : quoteCol(a.column)
    return `${fn}(${col}) AS \`${escapeIdentBT(a.alias)}\``
  }

  // --- JOIN ---

  private joinClause(j: JoinClause): string {
    const type = j.type === 'left' ? 'LEFT JOIN' : 'INNER JOIN'
    return `${type} ${quoteTable(j.table)} ON ${quoteCol(j.leftColumn)} = ${quoteCol(j.rightColumn)}`
  }

  // --- WHERE ---

  private whereNode(node: WhereNode): string {
    if (isGroup(node)) return this.whereGroup(node)
    if (isExists(node)) return this.whereExists(node)
    if (isCounted(node)) return this.whereCounted(node)
    if (isColCond(node)) return this.whereColCond(node)
    if (isFn(node)) return this.whereFn(node)
    if (isBetween(node)) return this.whereBetween(node)
    if (isArrayCond(node)) return this.whereArray(node)
    return this.whereCond(node)
  }

  // WhereCondition
  private whereCond(c: WhereCondition): string {
    const col = typeof c.column === 'string' ? `\`${escapeIdentBT(c.column)}\`` : quoteCol(c.column)
    const op = c.operator

    if (op === 'isNull') return `${col} IS NULL`
    if (op === 'isNotNull') return `${col} IS NOT NULL`

    if (c.paramIndex === undefined) return `${col} ${op}`

    // in / notIn — expand inline as IN tuple(v1, v2, ...)
    if (op === 'in') {
      return `${col} IN tuple(${this.expandTuple(c.paramIndex, c.columnType)})`
    }
    if (op === 'notIn') {
      return `${col} NOT IN tuple(${this.expandTuple(c.paramIndex, c.columnType)})`
    }

    // Pattern: raw
    if (op === 'like') return `${col} LIKE ${this.ref(c.paramIndex)}`
    if (op === 'notLike') return `${col} NOT LIKE ${this.ref(c.paramIndex)}`
    if (op === 'ilike') return `ilike(${col}, ${this.ref(c.paramIndex)})`
    if (op === 'notIlike') return `NOT ilike(${col}, ${this.ref(c.paramIndex)})`

    // ClickHouse built-in functions for startsWith/endsWith
    if (op === 'startsWith') return `startsWith(${col}, ${this.ref(c.paramIndex)})`
    if (op === 'endsWith') return `endsWith(${col}, ${this.ref(c.paramIndex)})`

    // Case-insensitive via ilike with wildcards
    if (op === 'istartsWith') return `ilike(${col}, ${this.pat(c.paramIndex, false, true)})`
    if (op === 'iendsWith') return `ilike(${col}, ${this.pat(c.paramIndex, true, false)})`

    // contains/icontains: LIKE/ilike with %wrapped%
    if (op === 'contains') return `${col} LIKE ${this.pat(c.paramIndex, true, true)}`
    if (op === 'icontains') return `ilike(${col}, ${this.pat(c.paramIndex, true, true)})`
    if (op === 'notContains') return `${col} NOT LIKE ${this.pat(c.paramIndex, true, true)}`
    if (op === 'notIcontains') return `NOT ilike(${col}, ${this.pat(c.paramIndex, true, true)})`

    // Standard comparison — use column type when available
    const chType = c.columnType !== undefined ? chColumnTypeMap(c.columnType) : undefined
    return `${col} ${op} ${this.ref(c.paramIndex, chType)}`
  }

  // WhereColumnCondition
  private whereColCond(c: WhereColumnCondition): string {
    return `${quoteCol(c.leftColumn)} ${c.operator} ${quoteCol(c.rightColumn)}`
  }

  // WhereBetween
  private whereBetween(c: WhereBetween): string {
    const col = quoteCol(c.column)
    // Timestamp columns: use parseDateTimeBestEffort to handle ISO 8601 strings with tz (e.g. trailing Z)
    if (c.columnType === 'timestamp') {
      const fromRef = `parseDateTimeBestEffort(${this.ref(c.fromParamIndex)})`
      const toRef = `parseDateTimeBestEffort(${this.ref(c.toParamIndex)})`
      if (c.not === true) return `NOT (${col} BETWEEN ${fromRef} AND ${toRef})`
      return `${col} BETWEEN ${fromRef} AND ${toRef}`
    }
    const chType = c.columnType !== undefined ? chColumnTypeMap(c.columnType) : undefined
    const fromRef = chType !== undefined ? this.refTyped(c.fromParamIndex, chType) : this.ref(c.fromParamIndex)
    const toRef = chType !== undefined ? this.refTyped(c.toParamIndex, chType) : this.ref(c.toParamIndex)
    if (c.not === true) {
      return `NOT (${col} BETWEEN ${fromRef} AND ${toRef})`
    }
    return `${col} BETWEEN ${fromRef} AND ${toRef}`
  }

  // WhereFunction — levenshtein → editDistance
  private whereFn(c: WhereFunction): string {
    const fn = safeWhereFn(c.fn) === 'levenshtein' ? 'editDistance' : safeWhereFn(c.fn)
    const col = quoteCol(c.column)
    return `${fn}(${col}, ${this.ref(c.fnParamIndex)}) ${c.operator} ${this.refTyped(c.compareParamIndex, 'UInt32')}`
  }

  // WhereArrayCondition
  private whereArray(c: WhereArrayCondition): string {
    const col = quoteCol(c.column)
    const op = c.operator

    if (op === 'isEmpty') return `empty(${col})`
    if (op === 'isNotEmpty') return `notEmpty(${col})`

    const idx = c.paramIndexes?.[0]
    if (idx === undefined) return `${col} IS NOT NULL`

    if (op === 'contains') return `has(${col}, ${this.refTyped(idx, chColumnTypeMap(c.elementType))})`
    if (op === 'containsAll')
      return `hasAll(${col}, [${this.expandArrayElements(c.paramIndexes ?? [], c.elementType)}])`
    // containsAny
    return `hasAny(${col}, [${this.expandArrayElements(c.paramIndexes ?? [], c.elementType)}])`
  }

  // WhereGroup
  private whereGroup(g: WhereGroup): string {
    const inner = g.conditions.map((c) => this.whereNode(c)).join(` ${g.logic === 'or' ? 'OR' : 'AND'} `)
    const wrapped = g.conditions.length > 1 ? `(${inner})` : inner
    return g.not === true ? `NOT ${wrapped}` : wrapped
  }

  // WhereExists
  private whereExists(e: WhereExists): string {
    const prefix = e.exists ? '' : 'NOT '
    return `${prefix}EXISTS (${this.subquery(e.subquery)})`
  }

  // WhereCountedSubquery
  private whereCounted(c: WhereCountedSubquery): string {
    // Correlated subqueries are decorrelated into INNER JOINs by CH, which drops 0-match rows.
    // For < and <= use NOT IN with inverted HAVING to include 0-count parents.
    // For >= and > use IN with direct HAVING (0-count parents correctly excluded).
    if (c.operator === '<' || c.operator === '<=') {
      return this.countedNotIn(c.subquery, c.operator, c.countParamIndex)
    }
    if (c.operator === '>=' || c.operator === '>') {
      return this.countedIn(c.subquery, c.operator, c.countParamIndex)
    }
    return `(${this.countSubquery(c.subquery)}) ${c.operator} ${this.refTyped(c.countParamIndex, 'UInt64')}`
  }

  /**
   * Rewrite `COUNT(*) < N` as `parent.id NOT IN (SELECT fk FROM child GROUP BY fk HAVING COUNT(*) >= N)`.
   * This avoids correlated subqueries and correctly includes parents with zero matching children.
   */
  private countedNotIn(sub: CorrelatedSubquery, operator: string, countParamIndex: number): string {
    const invertedOp = operator === '<' ? '>=' : '>'
    const fkCol = quoteCol(sub.join.rightColumn)
    const parentCol = quoteCol(sub.join.leftColumn)
    let inner = `SELECT ${fkCol} FROM ${quoteTable(sub.from)}`
    if (sub.where !== undefined) {
      inner += ` WHERE ${this.whereNode(sub.where)}`
    }
    inner += ` GROUP BY ${fkCol} HAVING COUNT(*) ${invertedOp} ${this.refTyped(countParamIndex, 'UInt64')}`
    return `${parentCol} NOT IN (${inner})`
  }

  /**
   * Rewrite `COUNT(*) >= N` as `parent.id IN (SELECT fk FROM child GROUP BY fk HAVING COUNT(*) >= N)`.
   * This avoids correlated subqueries; parents with zero children are correctly excluded.
   */
  private countedIn(sub: CorrelatedSubquery, operator: string, countParamIndex: number): string {
    const fkCol = quoteCol(sub.join.rightColumn)
    const parentCol = quoteCol(sub.join.leftColumn)
    let inner = `SELECT ${fkCol} FROM ${quoteTable(sub.from)}`
    if (sub.where !== undefined) {
      inner += ` WHERE ${this.whereNode(sub.where)}`
    }
    inner += ` GROUP BY ${fkCol} HAVING COUNT(*) ${operator} ${this.refTyped(countParamIndex, 'UInt64')}`
    return `${parentCol} IN (${inner})`
  }

  // --- Subquery ---

  private subquery(sub: CorrelatedSubquery): string {
    let sql = `SELECT 1 FROM ${quoteTable(sub.from)} WHERE ${quoteCol(sub.join.leftColumn)} = ${quoteCol(sub.join.rightColumn)}`
    if (sub.where !== undefined) {
      sql += ` AND ${this.whereNode(sub.where)}`
    }
    return sql
  }

  private countSubquery(sub: CorrelatedSubquery): string {
    let sql = `SELECT COUNT(*) FROM ${quoteTable(sub.from)} WHERE ${quoteCol(sub.join.leftColumn)} = ${quoteCol(sub.join.rightColumn)}`
    if (sub.where !== undefined) {
      sql += ` AND ${this.whereNode(sub.where)}`
    }
    return sql
  }

  // --- HAVING ---

  private havingNode(node: HavingNode): string {
    if ('logic' in node && 'conditions' in node) {
      const g = node as HavingGroup
      const inner = g.conditions.map((c) => this.havingNode(c)).join(` ${g.logic === 'or' ? 'OR' : 'AND'} `)
      const wrapped = g.conditions.length > 1 ? `(${inner})` : inner
      return g.not === true ? `NOT ${wrapped}` : wrapped
    }
    if ('alias' in node) {
      const b = node as HavingBetween
      if (b.not === true) {
        return `NOT (\`${escapeIdentBT(b.alias)}\` BETWEEN ${this.ref(b.fromParamIndex)} AND ${this.ref(b.toParamIndex)})`
      }
      return `\`${escapeIdentBT(b.alias)}\` BETWEEN ${this.ref(b.fromParamIndex)} AND ${this.ref(b.toParamIndex)}`
    }
    return this.whereCond(node as WhereCondition)
  }

  // --- ORDER BY ---

  private orderByClause(o: OrderByClause): string {
    const col = typeof o.column === 'string' ? `\`${escapeIdentBT(o.column)}\`` : quoteCol(o.column)
    const dir = o.direction.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
    return `${col} ${dir}`
  }

  // --- Param helpers ---

  private ref(idx: number, typeOverride?: string | undefined): string {
    const value = this.input[idx]
    this.outParams.push(value)
    const n = ++this.paramCounter
    const type = typeOverride ?? chValueType(value)
    return `{p${String(n)}:${type}}`
  }

  private refTyped(idx: number, type: string): string {
    const value = this.input[idx]
    this.outParams.push(value)
    const n = ++this.paramCounter
    return `{p${String(n)}:${type}}`
  }

  private pat(idx: number, pre: boolean, post: boolean): string {
    const raw = String(this.input[idx] ?? '')
    const escaped = escapeLike(raw)
    const value = `${pre ? '%' : ''}${escaped}${post ? '%' : ''}`
    this.outParams.push(value)
    const n = ++this.paramCounter
    return `{p${String(n)}:String}`
  }

  /** Expand array param inline as individual typed tuple values */
  private expandTuple(idx: number, elementType: string | undefined): string {
    const value = this.input[idx]
    if (!Array.isArray(value)) {
      this.outParams.push(value)
      const n = ++this.paramCounter
      const type = chColumnTypeMap(elementType)
      return `{p${String(n)}:${type}}`
    }
    const refs: string[] = []
    const type = chColumnTypeMap(elementType)
    for (const item of value) {
      this.outParams.push(item)
      const n = ++this.paramCounter
      refs.push(`{p${String(n)}:${type}}`)
    }
    return refs.join(', ')
  }

  /** Expand array param indexes as individual typed elements for hasAll/hasAny */
  private expandArrayElements(indexes: number[], elementType: string): string {
    // Indexes may be a single index pointing to an array value
    if (indexes.length === 1 && indexes[0] !== undefined) {
      const value = this.input[indexes[0]]
      if (Array.isArray(value)) {
        const type = chColumnTypeMap(elementType)
        const refs: string[] = []
        for (const item of value) {
          this.outParams.push(item)
          const n = ++this.paramCounter
          refs.push(`{p${String(n)}:${type}}`)
        }
        return refs.join(', ')
      }
    }
    const type = chColumnTypeMap(elementType)
    const refs: string[] = []
    for (const idx of indexes) {
      this.outParams.push(this.input[idx])
      const n = ++this.paramCounter
      refs.push(`{p${String(n)}:${type}}`)
    }
    return refs.join(', ')
  }
}

// --- Type guards for WhereNode ---

// --- Helpers ---

function quoteCol(col: ColumnRef): string {
  return `\`${escapeIdentBT(col.tableAlias)}\`.\`${escapeIdentBT(col.columnName)}\``
}

function quoteTable(ref: TableRef): string {
  const parts = ref.physicalName.split('.')
  const quoted = parts.map((p) => `\`${escapeIdentBT(p)}\``).join('.')
  return `${quoted} AS \`${escapeIdentBT(ref.alias)}\``
}

function chValueType(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'Int32' : 'Float64'
  }
  if (typeof value === 'boolean') return 'Bool'
  if (Array.isArray(value)) return 'Array(String)'
  return 'String'
}

/** Map ColumnType → ClickHouse type name */
function chColumnTypeMap(elementType: string | undefined): string {
  if (elementType === undefined) return 'String'
  const map: Record<string, string> = {
    uuid: 'UUID',
    string: 'String',
    int: 'Int32',
    decimal: 'Decimal',
    boolean: 'Bool',
    date: 'Date',
    timestamp: 'DateTime',
  }
  return map[elementType] ?? 'String'
}
