import type {
  ConfigErrorEntry,
  ConnectionErrorDetails,
  ExecutionErrorDetails,
  PlannerErrorDetails,
  ValidationErrorEntry,
} from '@mkven/multi-db-validation'
import {
  ConfigError,
  ConnectionError,
  ExecutionError,
  MultiDbError,
  PlannerError,
  ProviderError,
  ValidationError,
} from '@mkven/multi-db-validation'

const PLANNER_CODES = new Set(['UNREACHABLE_TABLES', 'TRINO_DISABLED', 'NO_CATALOG', 'FRESHNESS_UNMET'])
const EXECUTION_CODES = new Set(['EXECUTOR_MISSING', 'CACHE_PROVIDER_MISSING', 'QUERY_FAILED', 'QUERY_TIMEOUT'])
const CONNECTION_CODES = new Set(['CONNECTION_FAILED', 'NETWORK_ERROR', 'REQUEST_TIMEOUT'])
const PROVIDER_CODES = new Set(['METADATA_LOAD_FAILED', 'ROLE_LOAD_FAILED'])

/**
 * Reconstruct a typed error from a JSON body returned by the server.
 * Maps the `code` field to the correct error class.
 */
export function deserializeError(body: Record<string, unknown>): Error {
  const code = typeof body.code === 'string' ? body.code : ''
  const message = typeof body.message === 'string' ? body.message : 'Unknown error'

  if (code === 'VALIDATION_FAILED') {
    return new ValidationError(
      typeof body.fromTable === 'string' ? body.fromTable : 'unknown',
      Array.isArray(body.errors) ? (body.errors as ValidationErrorEntry[]) : [],
    )
  }

  if (code === 'CONFIG_INVALID') {
    return new ConfigError(Array.isArray(body.errors) ? (body.errors as ConfigErrorEntry[]) : [])
  }

  if (PLANNER_CODES.has(code)) {
    return new PlannerError(
      code as 'UNREACHABLE_TABLES' | 'TRINO_DISABLED' | 'NO_CATALOG' | 'FRESHNESS_UNMET',
      typeof body.fromTable === 'string' ? body.fromTable : 'unknown',
      (body.details ?? { code }) as PlannerErrorDetails,
      message,
    )
  }

  if (EXECUTION_CODES.has(code)) {
    return new ExecutionError((body.details ?? { code }) as ExecutionErrorDetails)
  }

  if (CONNECTION_CODES.has(code)) {
    return new ConnectionError(
      code as 'CONNECTION_FAILED' | 'NETWORK_ERROR' | 'REQUEST_TIMEOUT',
      message,
      (body.details ?? {}) as ConnectionErrorDetails,
    )
  }

  if (PROVIDER_CODES.has(code)) {
    return new ProviderError(code as 'METADATA_LOAD_FAILED' | 'ROLE_LOAD_FAILED', message)
  }

  return new Error(message)
}

export { MultiDbError }
