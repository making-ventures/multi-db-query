// Re-export types from validation package
export type {
  CountResult,
  DataResult,
  ExecutionContext,
  HealthCheckResult,
  QueryDefinition,
  QueryResult,
  QueryResultMeta,
  SqlResult,
} from '@mkven/multi-db-validation'
// Client
export type { MultiDbClient, MultiDbClientConfig } from './client.js'
export { createMultiDbClient } from './client.js'
// Contract testing
export type { QueryContract } from './contract/queryContract.js'
export { describeQueryContract } from './contract/queryContract.js'
// Error deserialization
export { deserializeError } from './errors.js'
