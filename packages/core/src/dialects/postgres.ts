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

// --- Postgres Dialect ---

export class PostgresDialect implements SqlDialect {
  generate(parts: SqlParts, params: unknown[]): { sql: string; params: unknown[] } {
    const gen = new PgGenerator(params)
    const sql = gen.build(parts)
    return { sql, params: gen.outParams }
  }
}

// --- Internal generator ---

class PgGenerator {
  readonly outParams: unknown[] = []
  private readonly input: unknown[]
  private aggregations: AggregationClause[] = []

  constructor(inputParams: unknown[]) {
    this.input = inputParams
  }

  build(parts: SqlParts): string {
    this.aggregations = parts.aggregations
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
    const col = typeof c.column === 'string' ? `"${escapeIdentDQ(c.column)}"` : quoteCol(c.column)
    const op = c.operator

    if (op === 'isNull') return `${col} IS NULL`
    if (op === 'isNotNull') return `${col} IS NOT NULL`

    if (c.paramIndex === undefined) return `${col} ${op}`

    // in / notIn
    if (op === 'in') {
      const cast = pgCast(c.columnType)
      return `${col} = ANY(${this.ref(c.paramIndex)}::${cast})`
    }
    if (op === 'notIn') {
      const cast = pgCast(c.columnType)
      return `${col} <> ALL(${this.ref(c.paramIndex)}::${cast})`
    }

    // Pattern: raw
    if (op === 'like') return `${col} LIKE ${this.ref(c.paramIndex)}`
    if (op === 'notLike') return `${col} NOT LIKE ${this.ref(c.paramIndex)}`
    if (op === 'ilike') return `${col} ILIKE ${this.ref(c.paramIndex)}`
    if (op === 'notIlike') return `${col} NOT ILIKE ${this.ref(c.paramIndex)}`

    // Pattern: wrapped
    if (op === 'startsWith') return `${col} LIKE ${this.pat(c.paramIndex, false, true)}`
    if (op === 'endsWith') return `${col} LIKE ${this.pat(c.paramIndex, true, false)}`
    if (op === 'istartsWith') return `${col} ILIKE ${this.pat(c.paramIndex, false, true)}`
    if (op === 'iendsWith') return `${col} ILIKE ${this.pat(c.paramIndex, true, false)}`
    if (op === 'contains') return `${col} LIKE ${this.pat(c.paramIndex, true, true)}`
    if (op === 'icontains') return `${col} ILIKE ${this.pat(c.paramIndex, true, true)}`
    if (op === 'notContains') return `${col} NOT LIKE ${this.pat(c.paramIndex, true, true)}`
    if (op === 'notIcontains') return `${col} NOT ILIKE ${this.pat(c.paramIndex, true, true)}`

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

  // WhereFunction
  private whereFn(c: WhereFunction): string {
    return `${safeWhereFn(c.fn)}(${quoteCol(c.column)}, ${this.ref(c.fnParamIndex)}) ${c.operator} ${this.ref(c.compareParamIndex)}`
  }

  // WhereArrayCondition
  private whereArray(c: WhereArrayCondition): string {
    const col = quoteCol(c.column)
    const cast = pgCast(c.elementType)
    const op = c.operator

    if (op === 'isEmpty') return `cardinality(${col}) = 0`
    if (op === 'isNotEmpty') return `cardinality(${col}) > 0`

    const idx = c.paramIndexes?.[0]
    if (idx === undefined) return `${col} IS NOT NULL`

    if (op === 'contains') return `${this.ref(idx)}::${pgScalarCast(c.elementType)} = ANY(${col})`
    if (op === 'containsAll') return `${col} @> ${this.ref(idx)}::${cast}`
    // containsAny
    return `${col} && ${this.ref(idx)}::${cast}`
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
    // For >= / > operators, wrap in a limited inner query to short-circuit counting
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
    // Only optimize >= and > (threshold checks that don't need exact count)
    if (operator !== '>=' && operator !== '>') return undefined
    const value = this.input[countParamIndex]
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) return undefined
    return operator === '>=' ? value : value + 1
  }

  // --- HAVING ---

  /** Resolve an aggregation alias to its expression (e.g. "totalAmt" → SUM("t0"."amount")). */
  private resolveAlias(alias: string): string {
    const agg = this.aggregations.find((a) => a.alias === alias)
    if (agg) {
      const fn = safeAggFn(agg.fn)
      const col = agg.column === '*' ? '*' : quoteCol(agg.column)
      return `${fn}(${col})`
    }
    return `"${escapeIdentDQ(alias)}"`
  }

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
      return `${this.resolveAlias(b.alias)} ${not}BETWEEN ${this.ref(b.fromParamIndex)} AND ${this.ref(b.toParamIndex)}`
    }
    // WhereCondition — resolve alias if column is a string
    const c = node as WhereCondition
    const col = typeof c.column === 'string' ? this.resolveAlias(c.column) : quoteCol(c.column)
    const op = c.operator
    if (op === 'isNull') return `${col} IS NULL`
    if (op === 'isNotNull') return `${col} IS NOT NULL`
    if (c.paramIndex === undefined) return `${col} ${op}`
    return `${col} ${op} ${this.ref(c.paramIndex)}`
  }

  // --- ORDER BY ---

  private orderByClause(o: OrderByClause): string {
    const col = typeof o.column === 'string' ? `"${escapeIdentDQ(o.column)}"` : quoteCol(o.column)
    const dir = o.direction.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
    return `${col} ${dir}`
  }

  // --- Param helpers ---

  private ref(idx: number): string {
    this.outParams.push(this.input[idx])
    return `$${String(this.outParams.length)}`
  }

  private pat(idx: number, pre: boolean, post: boolean): string {
    const raw = String(this.input[idx] ?? '')
    const escaped = escapeLike(raw)
    const value = `${pre ? '%' : ''}${escaped}${post ? '%' : ''}`
    this.outParams.push(value)
    return `$${String(this.outParams.length)}`
  }
}

// --- Helpers ---

function quoteCol(col: ColumnRef): string {
  return `"${escapeIdentDQ(col.tableAlias)}"."${escapeIdentDQ(col.columnName)}"`
}

function quoteTable(ref: TableRef): string {
  const parts = ref.physicalName.split('.')
  const quoted = parts.map((p) => `"${escapeIdentDQ(p)}"`).join('.')
  return `${quoted} AS "${escapeIdentDQ(ref.alias)}"`
}

function pgCast(elementType: string | undefined): string {
  if (elementType === undefined) return 'text[]'
  const map: Record<string, string> = {
    uuid: 'uuid[]',
    string: 'text[]',
    int: 'integer[]',
    decimal: 'numeric[]',
    boolean: 'bool[]',
    date: 'date[]',
    timestamp: 'timestamp[]',
  }
  return map[elementType] ?? 'text[]'
}

function pgScalarCast(elementType: string): string {
  const map: Record<string, string> = {
    uuid: 'uuid',
    string: 'text',
    int: 'integer',
    decimal: 'numeric',
    boolean: 'bool',
    date: 'date',
    timestamp: 'timestamp',
  }
  return map[elementType] ?? 'text'
}
