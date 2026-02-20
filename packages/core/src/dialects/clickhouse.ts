import type {
  AggregationClause,
  ColumnRef,
  CorrelatedSubquery,
  HavingBetween,
  HavingGroup,
  HavingNode,
  JoinClause,
  OrderByClause,
  SqlDialect,
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
      items.push(quoteCol(col))
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
    const fn = a.fn === 'avg' ? 'avg' : a.fn.toUpperCase()
    const col = a.column === '*' ? '*' : quoteCol(a.column)
    return `${fn === 'avg' ? 'avg' : fn}(${col}) AS \`${a.alias}\``
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
    const col = typeof c.column === 'string' ? `\`${c.column}\`` : quoteCol(c.column)
    const op = c.operator

    if (op === 'isNull') return `${col} IS NULL`
    if (op === 'isNotNull') return `${col} IS NOT NULL`

    if (c.paramIndex === undefined) {
      return c.literal !== undefined ? `${col} ${op} ${c.literal}` : `${col} ${op}`
    }

    // in / notIn — use Array param
    if (op === 'in') {
      const chType = chArrayType(c.columnType)
      return `${col} IN (${this.ref(c.paramIndex, chType)})`
    }
    if (op === 'notIn') {
      const chType = chArrayType(c.columnType)
      return `${col} NOT IN (${this.ref(c.paramIndex, chType)})`
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

    // Standard comparison
    return `${col} ${op} ${this.ref(c.paramIndex)}`
  }

  // WhereColumnCondition
  private whereColCond(c: WhereColumnCondition): string {
    return `${quoteCol(c.leftColumn)} ${c.operator} ${quoteCol(c.rightColumn)}`
  }

  // WhereBetween
  private whereBetween(c: WhereBetween): string {
    const col = quoteCol(c.column)
    if (c.not === true) {
      return `NOT (${col} BETWEEN ${this.ref(c.fromParamIndex)} AND ${this.ref(c.toParamIndex)})`
    }
    return `${col} BETWEEN ${this.ref(c.fromParamIndex)} AND ${this.ref(c.toParamIndex)}`
  }

  // WhereFunction — levenshtein → editDistance
  private whereFn(c: WhereFunction): string {
    const fn = c.fn === 'levenshtein' ? 'editDistance' : c.fn
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

    if (op === 'contains') return `has(${col}, ${this.ref(idx)})`
    if (op === 'containsAll') return `hasAll(${col}, ${this.ref(idx, chArrayType(c.elementType))})`
    // containsAny
    return `hasAny(${col}, ${this.ref(idx, chArrayType(c.elementType))})`
  }

  // WhereGroup
  private whereGroup(g: WhereGroup): string {
    const inner = g.conditions.map((c) => this.whereNode(c)).join(` ${g.logic.toUpperCase()} `)
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
      return `SELECT COUNT(*) FROM (${inner}) AS \`_c\``
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
      const inner = g.conditions.map((c) => this.havingNode(c)).join(` ${g.logic.toUpperCase()} `)
      const wrapped = g.conditions.length > 1 ? `(${inner})` : inner
      return g.not === true ? `NOT ${wrapped}` : wrapped
    }
    if ('alias' in node) {
      const b = node as HavingBetween
      const not = b.not === true ? 'NOT ' : ''
      return `\`${b.alias}\` ${not}BETWEEN ${this.ref(b.fromParamIndex)} AND ${this.ref(b.toParamIndex)}`
    }
    return this.whereCond(node as WhereCondition)
  }

  // --- ORDER BY ---

  private orderByClause(o: OrderByClause): string {
    const col = typeof o.column === 'string' ? `\`${o.column}\`` : quoteCol(o.column)
    return `${col} ${o.direction.toUpperCase()}`
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
}

// --- Type guards for WhereNode ---

function isGroup(n: WhereNode): n is WhereGroup {
  return 'logic' in n && 'conditions' in n
}

function isExists(n: WhereNode): n is WhereExists {
  return 'exists' in n && 'subquery' in n
}

function isCounted(n: WhereNode): n is WhereCountedSubquery {
  return 'countParamIndex' in n && 'subquery' in n
}

function isColCond(n: WhereNode): n is WhereColumnCondition {
  return 'leftColumn' in n && 'rightColumn' in n
}

function isFn(n: WhereNode): n is WhereFunction {
  return 'fn' in n && 'fnParamIndex' in n
}

function isBetween(n: WhereNode): n is WhereBetween {
  return 'fromParamIndex' in n && 'toParamIndex' in n && !('alias' in n)
}

function isArrayCond(n: WhereNode): n is WhereArrayCondition {
  return 'elementType' in n
}

// --- Helpers ---

function quoteCol(col: ColumnRef): string {
  return `\`${col.tableAlias}\`.\`${col.columnName}\``
}

function quoteTable(ref: TableRef): string {
  const parts = ref.physicalName.split('.')
  const quoted = parts.map((p) => `\`${p}\``).join('.')
  return `${quoted} AS \`${ref.alias}\``
}

function escapeLike(value: string): string {
  return value.replace(/[%_\\]/g, '\\$&')
}

function chValueType(value: unknown): string {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'Int32' : 'Float64'
  }
  if (typeof value === 'boolean') return 'Bool'
  if (Array.isArray(value)) return 'Array(String)'
  return 'String'
}

function chArrayType(elementType: string | undefined): string {
  if (elementType === undefined) return 'Array(String)'
  const map: Record<string, string> = {
    uuid: 'Array(UUID)',
    string: 'Array(String)',
    int: 'Array(Int32)',
    decimal: 'Array(Decimal)',
    boolean: 'Array(Bool)',
    date: 'Array(Date)',
    datetime: 'Array(DateTime)',
  }
  return map[elementType] ?? 'Array(String)'
}
