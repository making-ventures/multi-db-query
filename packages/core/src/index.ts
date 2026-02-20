// Re-export all types from validation package
export type {
  ArrayColumnType,
  CachedTableMeta,
  CacheMeta,
  ColumnFilterOperator,
  ColumnMeta,
  ColumnType,
  CountResult,
  DatabaseEngine,
  DatabaseMeta,
  DataResult,
  DebugLogEntry,
  ExecutionContext,
  ExternalSync,
  FilterOperator,
  HealthCheckResult,
  MetadataConfig,
  QueryAggregation,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
  QueryGroupBy,
  QueryJoin,
  QueryOrderBy,
  QueryResult,
  QueryResultMeta,
  RelationMeta,
  RoleMeta,
  ScalarColumnType,
  SqlResult,
  TableMeta,
  TableRoleAccess,
} from '@mkven/multi-db-validation'
// Re-export validation functions and classes
export {
  ConfigError,
  ConnectionError,
  ExecutionError,
  MetadataIndex,
  MultiDbError,
  PlannerError,
  ProviderError,
  ValidationError,
  validateConfig,
  validateQuery,
} from '@mkven/multi-db-validation'
// Access Control & Masking
export type { EffectiveColumn, EffectiveTableAccess, MaskingFn } from './accessControl.js'
export {
  applyMask,
  computeAllowedColumns,
  maskRows,
  resolveTableAccess,
} from './accessControl.js'
// Dialects
export { ClickHouseDialect } from './dialects/clickhouse.js'
export { PostgresDialect } from './dialects/postgres.js'
export { TrinoDialect } from './dialects/trino.js'
// Metadata Registry
export type { ConnectivityEdge, RegistrySnapshot } from './metadataRegistry.js'
export { MetadataRegistry } from './metadataRegistry.js'
// Name Resolution
export type { ResolveResult } from './nameResolver.js'
export { resolveNames } from './nameResolver.js'
// Query Planner
export type {
  CachePlan,
  DialectName,
  DirectPlan,
  MaterializedPlan,
  PlannerOptions,
  QueryPlan,
  TrinoPlan,
} from './planner.js'
export { planQuery } from './planner.js'
// Static provider helpers
export { staticMetadata, staticRoles } from './staticProviders.js'
// Public interfaces
export type { CacheProvider, DbExecutor } from './types/interfaces.js'
// IR types (internal)
export type {
  AggregationClause,
  ColumnMapping,
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
} from './types/ir.js'
// Providers
export type { MetadataProvider, RoleProvider } from './types/providers.js'
