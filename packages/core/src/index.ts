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
export type { EffectiveColumn, EffectiveTableAccess, MaskingFn } from './access/access.js'
export {
  computeAllowedColumns,
  maskRows,
  resolveTableAccess,
} from './access/access.js'
// Debug
export { debugEntry, withDebugLog } from './debug/logger.js'
// Dialects
export { ClickHouseDialect } from './dialects/clickhouse.js'
export type { SqlDialect } from './dialects/dialect.js'
export { PostgresDialect } from './dialects/postgres.js'
export { TrinoDialect } from './dialects/trino.js'
export {
  escapeLike,
  isArrayCond,
  isBetween,
  isColCond,
  isCounted,
  isExists,
  isFn,
  isGroup,
} from './generator/fragments.js'
// Generator
export { generateSql } from './generator/generator.js'
export { applyMask } from './masking/masking.js'
// Static provider helpers
export { staticMetadata, staticRoles } from './metadata/providers.js'
// Metadata Registry
export type { ConnectivityEdge, RegistrySnapshot } from './metadata/registry.js'
export { MetadataRegistry } from './metadata/registry.js'
// Pipeline
export type { CreateMultiDbOptions, MultiDb } from './pipeline.js'
export { createMultiDb } from './pipeline.js'
// Query Planner
export type {
  CachePlan,
  DialectName,
  DirectPlan,
  MaterializedPlan,
  PlannerOptions,
  QueryPlan,
  TrinoPlan,
} from './planner/planner.js'
export { planQuery } from './planner/planner.js'
// Name Resolution
export type { ResolveResult } from './resolution/resolver.js'
export { resolveNames } from './resolution/resolver.js'
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
