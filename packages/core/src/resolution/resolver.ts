import type {
  ExecutionContext,
  MetadataIndex,
  QueryAggregation,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
  RoleMeta,
  TableMeta,
} from '@mkven/multi-db-validation'
import { ExecutionError, PlannerError } from '@mkven/multi-db-validation'
import type { EffectiveTableAccess } from '../access/access.js'
import { resolveTableAccess } from '../access/access.js'
import type {
  AggregationClause,
  ColumnMapping,
  ColumnRef,
  CorrelatedSubquery,
  HavingNode,
  JoinClause,
  OrderByClause,
  SqlParts,
  TableRef,
  WhereNode,
} from '../types/ir.js'

// --- Result ---

export interface ResolveResult {
  parts: SqlParts
  params: unknown[]
  columnMappings: ColumnMapping[]
  mode: 'data' | 'count'
}

// --- Type union for filter entries ---

type FilterEntry = QueryFilter | QueryColumnFilter | QueryFilterGroup | QueryExistsFilter

// --- Type guards ---

function isFilterGroup(f: FilterEntry): f is QueryFilterGroup {
  return 'logic' in f && 'conditions' in f
}

function isExistsFilter(f: FilterEntry): f is QueryExistsFilter {
  return 'table' in f && !('operator' in f) && !('logic' in f) && !('column' in f)
}

function isColumnFilter(f: FilterEntry): f is QueryColumnFilter {
  return 'refColumn' in f
}

// --- Name Resolver ---

/**
 * Translate a validated QueryDefinition into dialect-agnostic SqlParts + ColumnMapping[].
 *
 * Prerequisites: the query must be validated via validateQuery() before calling this.
 */
export function resolveNames(
  query: QueryDefinition,
  context: ExecutionContext,
  index: MetadataIndex,
  rolesById: ReadonlyMap<string, RoleMeta>,
): ResolveResult {
  const ctx = new ResolutionContext(query, context, index, rolesById)
  return ctx.resolve()
}

// --- Internal context ---

class ResolutionContext {
  private readonly query: QueryDefinition
  private readonly context: ExecutionContext
  private readonly index: MetadataIndex
  private readonly rolesById: ReadonlyMap<string, RoleMeta>
  private readonly fromTable: TableMeta
  private readonly fromAccess: EffectiveTableAccess

  // Table aliasing
  private aliasCounter = 0
  private readonly tableAliases = new Map<string, string>() // tableId → alias
  private readonly tableRefs = new Map<string, TableRef>() // alias → TableRef

  // Parameters
  private readonly params: unknown[] = []

  constructor(
    query: QueryDefinition,
    context: ExecutionContext,
    index: MetadataIndex,
    rolesById: ReadonlyMap<string, RoleMeta>,
  ) {
    this.query = query
    this.context = context
    this.index = index
    this.rolesById = rolesById

    const table = index.getTable(query.from)
    if (table === undefined) {
      throw new PlannerError('UNREACHABLE_TABLES', query.from, {
        code: 'UNREACHABLE_TABLES',
        tables: [query.from],
      })
    }
    this.fromTable = table
    this.fromAccess = resolveTableAccess(table, context, rolesById)

    // Register from table
    this.registerTable(table)
  }

  resolve(): ResolveResult {
    const isCountMode = this.query.executeMode === 'count'

    // Resolve joins first (needed for column resolution)
    const joins = this.resolveJoins()

    // Resolve select columns
    const { select, columnMappings } = isCountMode ? { select: [], columnMappings: [] } : this.resolveSelect()

    // Resolve aggregations
    const aggregations = isCountMode ? [] : this.resolveAggregations()

    // Resolve WHERE
    const where = this.resolveWhere()

    // Resolve GROUP BY — suppressed in count mode
    const groupBy = isCountMode ? [] : this.resolveGroupBy()

    // Resolve HAVING — suppressed in count mode
    const having = isCountMode ? undefined : this.resolveHaving()

    // Resolve ORDER BY — suppressed in count mode
    const orderBy = isCountMode ? [] : this.resolveOrderBy(aggregations)

    const fromAlias = this.tableAliases.get(this.fromTable.id)
    if (fromAlias === undefined) {
      throw new ExecutionError({
        code: 'QUERY_FAILED',
        database: '',
        dialect: 'postgres',
        sql: '',
        params: [],
        cause: new Error('From table alias not found'),
      })
    }
    const fromRef = this.tableRefs.get(fromAlias)
    if (fromRef === undefined) {
      throw new ExecutionError({
        code: 'QUERY_FAILED',
        database: '',
        dialect: 'postgres',
        sql: '',
        params: [],
        cause: new Error('From table ref not found'),
      })
    }

    const parts: SqlParts = {
      select,
      distinct: isCountMode ? undefined : this.query.distinct,
      from: fromRef,
      joins,
      where,
      groupBy,
      having,
      aggregations,
      orderBy,
      limit: isCountMode ? undefined : this.query.limit,
      offset: isCountMode ? undefined : this.query.offset,
      countMode: isCountMode,
    }

    return {
      parts,
      params: this.params,
      columnMappings,
      mode: isCountMode ? 'count' : 'data',
    }
  }

  // --- Table registration ---

  private registerTable(table: TableMeta): string {
    const existing = this.tableAliases.get(table.id)
    if (existing !== undefined) {
      return existing
    }
    const alias = `t${this.aliasCounter++}`
    this.tableAliases.set(table.id, alias)
    this.tableRefs.set(alias, {
      physicalName: table.physicalName,
      alias,
    })
    return alias
  }

  private getAlias(tableId: string): string {
    const alias = this.tableAliases.get(tableId)
    if (alias === undefined) {
      throw new ExecutionError({
        code: 'QUERY_FAILED',
        database: '',
        dialect: 'postgres',
        sql: '',
        params: [],
        cause: new Error(`No alias for table: ${tableId}`),
      })
    }
    return alias
  }

  // --- Column resolution ---

  private resolveColumnRef(apiName: string, tableId: string): ColumnRef {
    const col = this.index.getColumn(tableId, apiName)
    if (col === undefined) {
      throw new ExecutionError({
        code: 'QUERY_FAILED',
        database: '',
        dialect: 'postgres',
        sql: '',
        params: [],
        cause: new Error(`Column not found: ${apiName} in table ${tableId}`),
      })
    }
    return {
      tableAlias: this.getAlias(tableId),
      columnName: col.physicalName,
    }
  }

  // --- SELECT ---

  private resolveSelect(): { select: ColumnRef[]; columnMappings: ColumnMapping[] } {
    const select: ColumnRef[] = []
    const columnMappings: ColumnMapping[] = []

    const hasAggregations = this.query.aggregations !== undefined && this.query.aggregations.length > 0
    const requestedColumns = this.query.columns

    if (requestedColumns !== undefined && requestedColumns.length === 0 && hasAggregations) {
      // Aggregation-only query — no select columns
      return { select, columnMappings }
    }

    if (requestedColumns === undefined && hasAggregations) {
      // columns: undefined with aggregations → default to groupBy columns only (zero if no groupBy)
      if (this.query.groupBy !== undefined) {
        for (const gb of this.query.groupBy) {
          const tableId = this.resolveQualifiedTableId(gb.table)
          this.addSelectColumn(select, columnMappings, gb.column, tableId)
        }
      }
      return { select, columnMappings }
    }

    // Build candidate columns for collision detection (see QUERY.md § Column disambiguation)
    const candidates: Array<{ apiName: string; tableId: string; tableApiName: string }> = []

    // From-table candidates
    if (requestedColumns !== undefined) {
      for (const colName of requestedColumns) {
        candidates.push({ apiName: colName, tableId: this.fromTable.id, tableApiName: this.fromTable.apiName })
      }
    } else {
      for (const col of this.fromTable.columns) {
        const eff = this.fromAccess.columns.get(col.apiName)
        if (eff?.allowed) {
          candidates.push({ apiName: col.apiName, tableId: this.fromTable.id, tableApiName: this.fromTable.apiName })
        }
      }
    }

    // Join-table candidates
    if (this.query.joins !== undefined) {
      for (const join of this.query.joins) {
        const joinTable = this.index.getTable(join.table)
        if (joinTable === undefined) continue

        if (join.columns === undefined) {
          const joinAccess = resolveTableAccess(joinTable, this.context, this.rolesById)
          for (const col of joinTable.columns) {
            const eff = joinAccess.columns.get(col.apiName)
            if (eff?.allowed) {
              candidates.push({ apiName: col.apiName, tableId: joinTable.id, tableApiName: join.table })
            }
          }
        } else if (join.columns.length > 0) {
          for (const colName of join.columns) {
            candidates.push({ apiName: colName, tableId: joinTable.id, tableApiName: join.table })
          }
        }
        // [] = no columns (join for filter only)
      }
    }

    // Detect collisions: apiNames that appear in more than one table
    const apiNameTables = new Map<string, Set<string>>()
    for (const c of candidates) {
      let tables = apiNameTables.get(c.apiName)
      if (tables === undefined) {
        tables = new Set()
        apiNameTables.set(c.apiName, tables)
      }
      tables.add(c.tableId)
    }
    const colliding = new Set<string>()
    for (const [apiName, tables] of apiNameTables) {
      if (tables.size > 1) colliding.add(apiName)
    }

    // Build select + mappings with collision-aware naming
    for (const c of candidates) {
      const qualify = colliding.has(c.apiName) ? c.tableApiName : undefined
      this.addSelectColumn(select, columnMappings, c.apiName, c.tableId, qualify)
    }

    return { select, columnMappings }
  }

  private addSelectColumn(
    select: ColumnRef[],
    mappings: ColumnMapping[],
    apiName: string,
    tableId: string,
    qualifiedTableApiName?: string | undefined,
  ): void {
    const col = this.index.getColumn(tableId, apiName)
    if (col === undefined) return

    const table = this.index.getTableById(tableId)
    if (table === undefined) return

    const access = resolveTableAccess(table, this.context, this.rolesById)
    const effCol = access.columns.get(apiName)

    select.push(this.resolveColumnRef(apiName, tableId))

    const resultApiName = qualifiedTableApiName !== undefined ? `${qualifiedTableApiName}.${apiName}` : apiName

    mappings.push({
      physicalName: col.physicalName,
      apiName: resultApiName,
      tableAlias: this.getAlias(tableId),
      tableApiName: table.apiName,
      masked: effCol?.masked === true,
      nullable: col.nullable,
      type: col.type,
      maskingFn: effCol?.maskingFn ?? col.maskingFn,
    })
  }

  // --- JOINS ---

  private resolveJoins(): JoinClause[] {
    if (this.query.joins === undefined) return []

    const result: JoinClause[] = []

    for (const join of this.query.joins) {
      const joinClause = this.resolveJoin(join)
      if (joinClause !== undefined) {
        result.push(joinClause)
      }
    }

    return result
  }

  private resolveJoin(join: { table: string; type?: 'inner' | 'left' | undefined }): JoinClause | undefined {
    const joinTable = this.index.getTable(join.table)
    if (joinTable === undefined) return undefined

    const joinAlias = this.registerTable(joinTable)

    // Find relation: from→join or join→from
    const fromRel = this.fromTable.relations.find((r) => r.references.table === joinTable.id)
    const joinRel = joinTable.relations.find((r) => r.references.table === this.fromTable.id)

    if (fromRel !== undefined) {
      const leftCol = this.fromTable.columns.find((c) => c.apiName === fromRel.column)
      const rightCol = joinTable.columns.find((c) => c.apiName === fromRel.references.column)
      if (leftCol === undefined || rightCol === undefined) return undefined

      return {
        type: join.type ?? 'left',
        table: { physicalName: joinTable.physicalName, alias: joinAlias },
        leftColumn: { tableAlias: this.getAlias(this.fromTable.id), columnName: leftCol.physicalName },
        rightColumn: { tableAlias: joinAlias, columnName: rightCol.physicalName },
      }
    }

    if (joinRel !== undefined) {
      const rightCol = joinTable.columns.find((c) => c.apiName === joinRel.column)
      const leftCol = this.fromTable.columns.find((c) => c.apiName === joinRel.references.column)
      if (leftCol === undefined || rightCol === undefined) return undefined

      return {
        type: join.type ?? 'left',
        table: { physicalName: joinTable.physicalName, alias: joinAlias },
        leftColumn: { tableAlias: this.getAlias(this.fromTable.id), columnName: leftCol.physicalName },
        rightColumn: { tableAlias: joinAlias, columnName: rightCol.physicalName },
      }
    }

    // Transitive: find relation through an already-joined table
    const transitive = this.findTransitiveRelation(joinTable)
    if (transitive !== undefined) {
      return {
        type: join.type ?? 'left',
        table: { physicalName: joinTable.physicalName, alias: joinAlias },
        leftColumn: transitive.leftColumn,
        rightColumn: { tableAlias: joinAlias, columnName: transitive.rightPhysical },
      }
    }

    return undefined
  }

  private findTransitiveRelation(joinTable: TableMeta): { leftColumn: ColumnRef; rightPhysical: string } | undefined {
    for (const [tableId, alias] of this.tableAliases) {
      if (tableId === joinTable.id) continue
      const intermediary = this.index.getTable(tableId)
      if (intermediary === undefined) continue

      // intermediary → joinTable
      const intRel = intermediary.relations.find((r) => r.references.table === joinTable.id)
      if (intRel !== undefined) {
        const leftCol = intermediary.columns.find((c) => c.apiName === intRel.column)
        const rightCol = joinTable.columns.find((c) => c.apiName === intRel.references.column)
        if (leftCol !== undefined && rightCol !== undefined) {
          return {
            leftColumn: { tableAlias: alias, columnName: leftCol.physicalName },
            rightPhysical: rightCol.physicalName,
          }
        }
      }

      // joinTable → intermediary
      const joinRel = joinTable.relations.find((r) => r.references.table === intermediary.id)
      if (joinRel !== undefined) {
        const rightCol = joinTable.columns.find((c) => c.apiName === joinRel.column)
        const leftCol = intermediary.columns.find((c) => c.apiName === joinRel.references.column)
        if (leftCol !== undefined && rightCol !== undefined) {
          return {
            leftColumn: { tableAlias: alias, columnName: leftCol.physicalName },
            rightPhysical: rightCol.physicalName,
          }
        }
      }
    }
    return undefined
  }

  // --- WHERE ---

  private resolveWhere(): WhereNode | undefined {
    const conditions: WhereNode[] = []

    // byIds → in operator on PK
    if (this.query.byIds !== undefined && this.query.byIds.length > 0) {
      const pkColName = this.fromTable.primaryKey[0]
      if (pkColName !== undefined) {
        const col = this.fromTable.columns.find((c) => c.apiName === pkColName)
        if (col !== undefined) {
          const paramIdx = this.addParam(this.query.byIds)
          conditions.push({
            column: this.resolveColumnRef(pkColName, this.fromTable.id),
            operator: 'in',
            paramIndex: paramIdx,
            columnType: col.type.replace('[]', ''),
          })
        }
      }
    }

    // Regular filters
    if (this.query.filters !== undefined) {
      for (const filter of this.query.filters) {
        const node = this.resolveFilterEntry(filter, this.fromTable)
        if (node !== undefined) {
          conditions.push(node)
        }
      }
    }

    // Join-scoped filters (see QUERY.md § QueryJoin.filters — placed in WHERE, context is joined table)
    if (this.query.joins !== undefined) {
      for (const join of this.query.joins) {
        if (join.filters !== undefined) {
          const joinTable = this.index.getTable(join.table)
          if (joinTable === undefined) continue
          for (const filter of join.filters) {
            const node = this.resolveFilterEntry(filter, joinTable)
            if (node !== undefined) {
              conditions.push(node)
            }
          }
        }
      }
    }

    if (conditions.length === 0) return undefined
    if (conditions.length === 1) {
      const first = conditions[0]
      if (first !== undefined) return first
    }
    return { logic: 'and', conditions }
  }

  private resolveFilterEntry(filter: FilterEntry, contextTable: TableMeta): WhereNode | undefined {
    if (isFilterGroup(filter)) {
      return this.resolveFilterGroup(filter, contextTable)
    }
    if (isExistsFilter(filter)) {
      return this.resolveExistsFilter(filter, contextTable)
    }
    if (isColumnFilter(filter)) {
      return this.resolveColumnFilter(filter, contextTable)
    }
    return this.resolveValueFilter(filter, contextTable)
  }

  private resolveFilterGroup(group: QueryFilterGroup, contextTable: TableMeta): WhereNode | undefined {
    const resolved = group.conditions
      .map((c) => this.resolveFilterEntry(c, contextTable))
      .filter((n): n is WhereNode => n !== undefined)
    if (resolved.length === 0) return undefined
    return {
      logic: group.logic,
      not: group.not,
      conditions: resolved,
    }
  }

  private resolveValueFilter(filter: QueryFilter, contextTable: TableMeta): WhereNode | undefined {
    const tableId = filter.table !== undefined ? this.resolveQualifiedTableId(filter.table) : contextTable.id
    const col = this.index.getColumn(tableId, filter.column)
    if (col === undefined) return undefined

    const colRef = this.resolveColumnRef(filter.column, tableId)
    const op = filter.operator

    // Null checks
    if (op === 'isNull' || op === 'isNotNull') {
      return { column: colRef, operator: op }
    }

    // Between
    if (op === 'between' || op === 'notBetween') {
      const val = filter.value as { from: unknown; to: unknown }
      return {
        column: colRef,
        not: op === 'notBetween',
        columnType: col.type,
        fromParamIndex: this.addParam(val.from),
        toParamIndex: this.addParam(val.to),
      }
    }

    // Levenshtein
    if (op === 'levenshteinLte') {
      const val = filter.value as { text: string; maxDistance: number }
      return {
        fn: 'levenshtein',
        column: colRef,
        fnParamIndex: this.addParam(val.text),
        operator: '<=',
        compareParamIndex: this.addParam(val.maxDistance),
      }
    }

    // Array operators
    if (op === 'arrayContains' || op === 'arrayContainsAll' || op === 'arrayContainsAny') {
      const opMap = {
        arrayContains: 'contains',
        arrayContainsAll: 'containsAll',
        arrayContainsAny: 'containsAny',
      } as const
      const baseType = col.type.replace('[]', '')
      return {
        column: colRef,
        operator: opMap[op],
        paramIndexes: [this.addParam(filter.value)],
        elementType: baseType,
      }
    }

    if (op === 'arrayIsEmpty' || op === 'arrayIsNotEmpty') {
      const opMap = {
        arrayIsEmpty: 'isEmpty',
        arrayIsNotEmpty: 'isNotEmpty',
      } as const
      const baseType = col.type.replace('[]', '')
      return {
        column: colRef,
        operator: opMap[op],
        elementType: baseType,
      }
    }

    // Regular comparison / in / notIn / like / etc.
    return {
      column: colRef,
      operator: op,
      paramIndex: this.addParam(filter.value),
      columnType: col.type.replace('[]', ''),
    }
  }

  private resolveColumnFilter(filter: QueryColumnFilter, contextTable: TableMeta): WhereNode | undefined {
    const leftTableId = filter.table !== undefined ? this.resolveQualifiedTableId(filter.table) : contextTable.id
    const rightTableId = filter.refTable !== undefined ? this.resolveQualifiedTableId(filter.refTable) : contextTable.id

    return {
      leftColumn: this.resolveColumnRef(filter.column, leftTableId),
      operator: filter.operator,
      rightColumn: this.resolveColumnRef(filter.refColumn, rightTableId),
    }
  }

  private resolveExistsFilter(filter: QueryExistsFilter, parentTable: TableMeta): WhereNode | undefined {
    const existsTable = this.index.getTable(filter.table)
    if (existsTable === undefined) return undefined

    // Find relation
    const fromRel = parentTable.relations.find((r) => r.references.table === existsTable.id)
    const toRel = existsTable.relations.find((r) => r.references.table === parentTable.id)

    if (fromRel === undefined && toRel === undefined) return undefined

    // Create correlated subquery
    const subAlias = `s${this.aliasCounter++}`
    const subRef: TableRef = { physicalName: existsTable.physicalName, alias: subAlias }

    let joinLeft: ColumnRef
    let joinRight: ColumnRef

    if (fromRel !== undefined) {
      const parentCol = parentTable.columns.find((c) => c.apiName === fromRel.column)
      const existsCol = existsTable.columns.find((c) => c.apiName === fromRel.references.column)
      if (parentCol === undefined || existsCol === undefined) return undefined
      joinLeft = { tableAlias: this.getAlias(parentTable.id), columnName: parentCol.physicalName }
      joinRight = { tableAlias: subAlias, columnName: existsCol.physicalName }
    } else if (toRel !== undefined) {
      const existsCol = existsTable.columns.find((c) => c.apiName === toRel.column)
      const parentCol = parentTable.columns.find((c) => c.apiName === toRel.references.column)
      if (existsCol === undefined || parentCol === undefined) return undefined
      joinLeft = { tableAlias: this.getAlias(parentTable.id), columnName: parentCol.physicalName }
      joinRight = { tableAlias: subAlias, columnName: existsCol.physicalName }
    } else {
      return undefined
    }

    // Resolve sub-filters
    let subWhere: WhereNode | undefined
    if (filter.filters !== undefined && filter.filters.length > 0) {
      // Temporarily register the subquery alias so nested filters can resolve columns
      const prevAlias = this.tableAliases.get(existsTable.id)
      this.tableAliases.set(existsTable.id, subAlias)

      const subConditions: WhereNode[] = []
      for (const f of filter.filters) {
        const node = this.resolveFilterEntry(f, existsTable)
        if (node !== undefined) {
          subConditions.push(node)
        }
      }

      // Restore previous alias (or remove if none existed)
      if (prevAlias !== undefined) {
        this.tableAliases.set(existsTable.id, prevAlias)
      } else {
        this.tableAliases.delete(existsTable.id)
      }

      if (subConditions.length === 1) {
        subWhere = subConditions[0]
      } else if (subConditions.length > 0) {
        subWhere = { logic: 'and', conditions: subConditions }
      }
    }

    const subquery: CorrelatedSubquery = {
      from: subRef,
      join: { leftColumn: joinLeft, rightColumn: joinRight },
      where: subWhere,
    }

    if (filter.count !== undefined) {
      return {
        subquery,
        operator: filter.count.operator,
        countParamIndex: this.addParam(filter.count.value),
      }
    }

    return {
      exists: filter.exists !== false,
      subquery,
    }
  }

  // --- GROUP BY ---

  private resolveGroupBy(): ColumnRef[] {
    if (this.query.groupBy === undefined) return []
    return this.query.groupBy.map((gb) => {
      const tableId = this.resolveQualifiedTableId(gb.table)
      return this.resolveColumnRef(gb.column, tableId)
    })
  }

  // --- HAVING ---

  private resolveHaving(): HavingNode | undefined {
    if (this.query.having === undefined || this.query.having.length === 0) return undefined

    const conditions: HavingNode[] = []
    for (const h of this.query.having) {
      const node = this.resolveHavingEntry(h)
      if (node !== undefined) {
        conditions.push(node)
      }
    }

    if (conditions.length === 0) return undefined
    if (conditions.length === 1) {
      const first = conditions[0]
      if (first !== undefined) return first
    }
    return { logic: 'and', conditions }
  }

  private resolveHavingEntry(entry: QueryFilter | QueryFilterGroup): HavingNode | undefined {
    if (isFilterGroup(entry)) {
      const resolved = entry.conditions
        .map((c) => {
          // In having, only plain filters and groups are allowed
          if (isFilterGroup(c)) {
            return this.resolveHavingEntry(c as QueryFilterGroup)
          }
          if ('column' in c && 'operator' in c) {
            return this.resolveHavingEntry(c as QueryFilter)
          }
          return undefined
        })
        .filter((n): n is HavingNode => n !== undefined)
      if (resolved.length === 0) return undefined
      return { logic: entry.logic, not: entry.not, conditions: resolved }
    }

    // Simple having = alias-based condition
    const h = entry as QueryFilter
    if (h.operator === 'between' || h.operator === 'notBetween') {
      const val = h.value as { from: unknown; to: unknown }
      return {
        alias: h.column,
        not: h.operator === 'notBetween',
        fromParamIndex: this.addParam(val.from),
        toParamIndex: this.addParam(val.to),
      }
    }

    return {
      column: h.column, // bare alias string
      operator: h.operator,
      paramIndex: h.value !== undefined ? this.addParam(h.value) : undefined,
    }
  }

  // --- AGGREGATIONS ---

  private resolveAggregations(): AggregationClause[] {
    if (this.query.aggregations === undefined) return []

    return this.query.aggregations.map((agg) => {
      if (agg.column === '*') {
        return { fn: agg.fn, column: '*' as const, alias: agg.alias }
      }
      const tableId = this.resolveAggTableId(agg)
      return {
        fn: agg.fn,
        column: this.resolveColumnRef(agg.column, tableId),
        alias: agg.alias,
      }
    })
  }

  private resolveAggTableId(agg: QueryAggregation): string {
    if (agg.table !== undefined) {
      return this.resolveQualifiedTableId(agg.table)
    }
    return this.fromTable.id
  }

  // --- ORDER BY ---

  private resolveOrderBy(aggregations: AggregationClause[]): OrderByClause[] {
    if (this.query.orderBy === undefined) return []

    const aggAliases = new Set(aggregations.map((a) => a.alias))

    return this.query.orderBy.map((ob) => {
      // Check if ordering by aggregation alias
      if (aggAliases.has(ob.column)) {
        return { column: ob.column, direction: ob.direction }
      }

      const tableId = this.resolveQualifiedTableId(ob.table)
      return {
        column: this.resolveColumnRef(ob.column, tableId),
        direction: ob.direction,
      }
    })
  }

  // --- Helpers ---

  private resolveQualifiedTableId(tableApiName: string | undefined): string {
    if (tableApiName === undefined) {
      return this.fromTable.id
    }
    const table = this.index.getTable(tableApiName)
    if (table === undefined) {
      return this.fromTable.id
    }
    return table.id
  }

  private addParam(value: unknown): number {
    this.params.push(value)
    return this.params.length - 1
  }
}
