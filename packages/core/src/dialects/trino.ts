import {
  escapeIdentDQ,
  escapeLike,
  isArrayCond,
  isBetween,
  isColCond,
  isCounted,
  isExists,
  isFn,
  isGroup,
  safeAggFn,
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

// --- Trino Dialect ---

export class TrinoDialect implements SqlDialect {
  generate(parts: SqlParts, params: unknown[]): { sql: string; params: unknown[] } {
    const gen = new TrinoGenerator(params)
    const sql = gen.build(parts)
    return { sql, params: gen.outParams }
  }
}

// --- Internal generator ---

class TrinoGenerator {
  readonly outParams: unknown[] = []
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
      items.push(`${quoteCol(col)} AS "${col.tableAlias}__${col.columnName}"`)
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
    return `${fn}(${col}) AS "${escapeIdentDQ(a.alias)}"`
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
    const col = typeof c.column === 'string' ? `"${c.column}"` : quoteCol(c.column)
    const op = c.operator

    if (op === 'isNull') return `${col} IS NULL`
    if (op === 'isNotNull') return `${col} IS NOT NULL`

    if (c.paramIndex === undefined) return `${col} ${op}`

    // in — expand array to IN (?, ?, ...)
    if (op === 'in') {
      return `${col} IN (${this.expandArray(c.paramIndex)})`
    }
    if (op === 'notIn') {
      return `${col} NOT IN (${this.expandArray(c.paramIndex)})`
    }

    // Pattern: raw
    if (op === 'like') return `${col} LIKE ${this.ref(c.paramIndex)}`
    if (op === 'notLike') return `${col} NOT LIKE ${this.ref(c.paramIndex)}`

    // Case-insensitive via lower()
    if (op === 'ilike') return `lower(${col}) LIKE lower(${this.ref(c.paramIndex)})`
    if (op === 'notIlike') return `NOT (lower(${col}) LIKE lower(${this.ref(c.paramIndex)}))`

    // Pattern: wrapped (case-sensitive)
    if (op === 'startsWith') return `${col} LIKE ${this.pat(c.paramIndex, false, true)} ESCAPE '\\'`
    if (op === 'endsWith') return `${col} LIKE ${this.pat(c.paramIndex, true, false)} ESCAPE '\\'`
    if (op === 'contains') return `${col} LIKE ${this.pat(c.paramIndex, true, true)} ESCAPE '\\'`
    if (op === 'notContains') return `${col} NOT LIKE ${this.pat(c.paramIndex, true, true)} ESCAPE '\\'`

    // Pattern: wrapped (case-insensitive)
    if (op === 'istartsWith') return `lower(${col}) LIKE lower(${this.pat(c.paramIndex, false, true)}) ESCAPE '\\'`
    if (op === 'iendsWith') return `lower(${col}) LIKE lower(${this.pat(c.paramIndex, true, false)}) ESCAPE '\\'`
    if (op === 'icontains') return `lower(${col}) LIKE lower(${this.pat(c.paramIndex, true, true)}) ESCAPE '\\'`
    if (op === 'notIcontains')
      return `NOT (lower(${col}) LIKE lower(${this.pat(c.paramIndex, true, true)}) ESCAPE '\\')`

    // Standard comparison
    return `${col} ${op} ${this.ref(c.paramIndex)}`
  }

  // WhereColumnCondition
  private whereColCond(c: WhereColumnCondition): string {
    return `${quoteCol(c.leftColumn)} ${c.operator} ${quoteCol(c.rightColumn)}`
  }

  // WhereBetween
  private whereBetween(c: WhereBetween): string {
    const not = c.not === true ? 'NOT ' : ''
    return `${quoteCol(c.column)} ${not}BETWEEN ${this.ref(c.fromParamIndex)} AND ${this.ref(c.toParamIndex)}`
  }

  // WhereFunction — levenshtein → levenshtein_distance
  private whereFn(c: WhereFunction): string {
    const fn = c.fn === 'levenshtein' ? 'levenshtein_distance' : c.fn
    const col = quoteCol(c.column)
    return `${fn}(${col}, ${this.ref(c.fnParamIndex)}) ${c.operator} ${this.ref(c.compareParamIndex)}`
  }

  // WhereArrayCondition
  private whereArray(c: WhereArrayCondition): string {
    const col = quoteCol(c.column)
    const op = c.operator

    if (op === 'isEmpty') return `cardinality(${col}) = 0`
    if (op === 'isNotEmpty') return `cardinality(${col}) > 0`

    const idx = c.paramIndexes?.[0]
    if (idx === undefined) return `${col} IS NOT NULL`

    if (op === 'contains') return `contains(${col}, ${this.ref(idx)})`
    if (op === 'containsAll') return `cardinality(array_except(ARRAY[${this.expandArray(idx)}], ${col})) = 0`
    // containsAny
    return `arrays_overlap(${col}, ARRAY[${this.expandArray(idx)}])`
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
    return `(${this.countSubquery(c.subquery, c.operator, c.countParamIndex)}) ${c.operator} ${this.ref(c.countParamIndex)}`
  }

  // --- Subquery ---

  private subquery(sub: CorrelatedSubquery): string {
    let sql = `SELECT 1 FROM ${quoteTable(sub.from)} WHERE ${quoteCol(sub.join.leftColumn)} = ${quoteCol(sub.join.rightColumn)}`
    if (sub.where !== undefined) {
      sql += ` AND ${this.whereNode(sub.where)}`
    }
    return sql
  }

  private countSubquery(
    sub: CorrelatedSubquery,
    operator?: string | undefined,
    countParamIndex?: number | undefined,
  ): string {
    const limit = this.countLimit(operator, countParamIndex)
    if (limit !== undefined) {
      let inner = `SELECT 1 FROM ${quoteTable(sub.from)} WHERE ${quoteCol(sub.join.leftColumn)} = ${quoteCol(sub.join.rightColumn)}`
      if (sub.where !== undefined) {
        inner += ` AND ${this.whereNode(sub.where)}`
      }
      inner += ` LIMIT ${String(limit)}`
      return `SELECT COUNT(*) FROM (${inner}) AS "_c"`
    }
    let sql = `SELECT COUNT(*) FROM ${quoteTable(sub.from)} WHERE ${quoteCol(sub.join.leftColumn)} = ${quoteCol(sub.join.rightColumn)}`
    if (sub.where !== undefined) {
      sql += ` AND ${this.whereNode(sub.where)}`
    }
    return sql
  }

  private countLimit(operator: string | undefined, countParamIndex: number | undefined): number | undefined {
    if (operator === undefined || countParamIndex === undefined) return undefined
    if (operator !== '>=' && operator !== '>') return undefined
    const value = this.input[countParamIndex]
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) return undefined
    return operator === '>=' ? value : value + 1
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
      const not = b.not === true ? 'NOT ' : ''
      return `"${escapeIdentDQ(b.alias)}" ${not}BETWEEN ${this.ref(b.fromParamIndex)} AND ${this.ref(b.toParamIndex)}`
    }
    return this.whereCond(node as WhereCondition)
  }

  // --- ORDER BY ---

  private orderByClause(o: OrderByClause): string {
    const col = typeof o.column === 'string' ? `"${o.column}"` : quoteCol(o.column)
    const dir = o.direction.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
    return `${col} ${dir}`
  }

  // --- Param helpers ---

  private ref(idx: number): string {
    this.outParams.push(this.input[idx])
    return '?'
  }

  private pat(idx: number, pre: boolean, post: boolean): string {
    const raw = String(this.input[idx] ?? '')
    const escaped = escapeLike(raw)
    const value = `${pre ? '%' : ''}${escaped}${post ? '%' : ''}`
    this.outParams.push(value)
    return '?'
  }

  private expandArray(idx: number): string {
    const value = this.input[idx]
    if (!Array.isArray(value)) {
      this.outParams.push(value)
      return '?'
    }
    const placeholders: string[] = []
    for (const item of value) {
      this.outParams.push(item)
      placeholders.push('?')
    }
    return placeholders.join(', ')
  }
}

// --- Helpers ---

function quoteCol(col: ColumnRef): string {
  return `"${col.tableAlias}"."${col.columnName}"`
}

function quoteTable(ref: TableRef): string {
  const segments: string[] = []
  if (ref.catalog !== undefined) {
    segments.push(`"${ref.catalog}"`)
  }
  const parts = ref.physicalName.split('.')
  for (const p of parts) {
    segments.push(`"${p}"`)
  }
  return `${segments.join('.')} AS "${ref.alias}"`
}
