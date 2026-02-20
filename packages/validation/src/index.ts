// Config validation
export { validateApiName, validateConfig } from './configValidation.js'

// Errors
export type {
  ConfigErrorEntry,
  ConnectionErrorDetails,
  ExecutionErrorDetails,
  PlannerErrorDetails,
  UnreachableProvider,
  ValidationErrorEntry,
} from './errors.js'
export {
  ConfigError,
  ConnectionError,
  ExecutionError,
  MultiDbError,
  PlannerError,
  ProviderError,
  ValidationError,
} from './errors.js'

// Metadata Index
export { MetadataIndex } from './metadataIndex.js'

// Query validation
export { validateQuery } from './queryValidation.js'

// Types — context
export type { ExecutionContext } from './types/context.js'
export type {
  ArrayColumnType,
  CachedTableMeta,
  CacheMeta,
  ColumnMeta,
  ColumnType,
  DatabaseEngine,
  DatabaseMeta,
  ExternalSync,
  MetadataConfig,
  RelationMeta,
  RoleMeta,
  ScalarColumnType,
  TableMeta,
  TableRoleAccess,
} from './types/metadata.js'
// Types — query
export type {
  ColumnFilterOperator,
  FilterOperator,
  QueryAggregation,
  QueryColumnFilter,
  QueryDefinition,
  QueryExistsFilter,
  QueryFilter,
  QueryFilterGroup,
  QueryGroupBy,
  QueryJoin,
  QueryOrderBy,
} from './types/query.js'
// Types — result
export type {
  CountResult,
  DataResult,
  DebugLogEntry,
  HealthCheckResult,
  QueryResult,
  QueryResultMeta,
  SqlResult,
} from './types/result.js'
