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
// Metadata Registry
export type { ConnectivityEdge, RegistrySnapshot } from './metadataRegistry.js'
export { MetadataRegistry } from './metadataRegistry.js'
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
