import type { ColumnType } from '@mkven/multi-db-validation'

// --- Column Reference ---

export interface ColumnRef {
  tableAlias: string
  columnName: string
}

// --- Table Reference ---

export interface TableRef {
  physicalName: string
  alias: string
  catalog?: string | undefined
}

// --- SqlParts (dialect-agnostic IR) ---

export interface SqlParts {
  select: ColumnRef[]
  distinct?: boolean | undefined
  from: TableRef
  joins: JoinClause[]
  where?: WhereNode | undefined
  groupBy: ColumnRef[]
  having?: HavingNode | undefined
  aggregations: AggregationClause[]
  orderBy: OrderByClause[]
  limit?: number | undefined
  offset?: number | undefined
  countMode?: boolean | undefined
}

// --- WHERE tree ---

export type WhereNode =
  | WhereCondition
  | WhereColumnCondition
  | WhereBetween
  | WhereFunction
  | WhereArrayCondition
  | WhereGroup
  | WhereExists
  | WhereCountedSubquery

export interface WhereCondition {
  column: ColumnRef | string
  operator: string
  paramIndex?: number | undefined
  literal?: string | undefined
  columnType?: string | undefined
}

export interface WhereColumnCondition {
  leftColumn: ColumnRef
  operator: string
  rightColumn: ColumnRef
}

export interface WhereBetween {
  column: ColumnRef
  not?: boolean | undefined
  fromParamIndex: number
  toParamIndex: number
}

export interface WhereFunction {
  fn: string
  column: ColumnRef
  fnParamIndex: number
  operator: string
  compareParamIndex: number
}

export interface WhereArrayCondition {
  column: ColumnRef
  operator: 'contains' | 'containsAll' | 'containsAny' | 'isEmpty' | 'isNotEmpty'
  paramIndexes?: number[] | undefined
  elementType: string
}

export interface WhereGroup {
  logic: 'and' | 'or'
  not?: boolean | undefined
  conditions: WhereNode[]
}

// --- Correlated Subquery (shared) ---

export interface CorrelatedSubquery {
  from: TableRef
  join: { leftColumn: ColumnRef; rightColumn: ColumnRef }
  where?: WhereNode | undefined
}

export interface WhereExists {
  exists: boolean
  subquery: CorrelatedSubquery
}

export interface WhereCountedSubquery {
  subquery: CorrelatedSubquery
  operator: string
  countParamIndex: number
}

// --- HAVING tree ---

export type HavingNode = WhereCondition | HavingBetween | HavingGroup

export interface HavingBetween {
  alias: string
  not?: boolean | undefined
  fromParamIndex: number
  toParamIndex: number
}

export interface HavingGroup {
  logic: 'and' | 'or'
  not?: boolean | undefined
  conditions: HavingNode[]
}

// --- JOIN ---

export interface JoinClause {
  type: 'inner' | 'left'
  table: TableRef
  leftColumn: ColumnRef
  rightColumn: ColumnRef
}

// --- ORDER BY ---

export interface OrderByClause {
  column: ColumnRef | string
  direction: 'asc' | 'desc'
}

// --- Aggregation ---

export interface AggregationClause {
  fn: 'count' | 'sum' | 'avg' | 'min' | 'max'
  column: ColumnRef | '*'
  alias: string
}

// --- Column Mapping (apiName ↔ physicalName) ---

export interface ColumnMapping {
  physicalName: string
  apiName: string
  tableAlias: string
  masked: boolean
  type: ColumnType
  maskingFn?: 'email' | 'phone' | 'name' | 'uuid' | 'number' | 'date' | 'full' | undefined
}

// --- SQL Dialect ---

export interface SqlDialect {
  generate(parts: SqlParts, params: unknown[]): { sql: string; params: unknown[] }
}
