// --- Base Error ---

export class MultiDbError extends Error {
  readonly code: string

  constructor(code: string, message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'MultiDbError'
    this.code = code
  }

  toJSON(): Record<string, unknown> {
    const json: Record<string, unknown> = {
      code: this.code,
      message: this.message,
    }
    if (this.cause !== undefined) {
      json.cause = serializeError(this.cause)
    }
    return json
  }
}

// --- Config Error ---

export interface ConfigErrorEntry {
  code:
    | 'INVALID_API_NAME'
    | 'DUPLICATE_API_NAME'
    | 'INVALID_REFERENCE'
    | 'INVALID_RELATION'
    | 'INVALID_SYNC'
    | 'INVALID_CACHE'
  message: string
  details: {
    entity?: string | undefined
    field?: string | undefined
    expected?: string | undefined
    actual?: string | undefined
    database?: string | undefined
    cacheId?: string | undefined
  }
}

export class ConfigError extends MultiDbError {
  declare readonly code: 'CONFIG_INVALID'
  readonly errors: readonly ConfigErrorEntry[]

  constructor(errors: readonly ConfigErrorEntry[]) {
    super('CONFIG_INVALID', `Config invalid: ${errors.length} error${errors.length === 1 ? '' : 's'}`)
    this.name = 'ConfigError'
    this.errors = errors
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      errors: this.errors,
    }
  }
}

// --- Connection Error ---

export interface UnreachableProvider {
  id: string
  type: 'executor' | 'cache'
  engine?: 'postgres' | 'clickhouse' | 'trino' | 'redis' | undefined
  cause?: Error | undefined
}

export type ConnectionErrorDetails =
  | { unreachable: readonly UnreachableProvider[] }
  | { url?: string | undefined; timeoutMs?: number | undefined }

export class ConnectionError extends MultiDbError {
  declare readonly code: 'CONNECTION_FAILED' | 'NETWORK_ERROR' | 'REQUEST_TIMEOUT'
  readonly details: ConnectionErrorDetails

  constructor(
    code: 'CONNECTION_FAILED' | 'NETWORK_ERROR' | 'REQUEST_TIMEOUT',
    message: string,
    details: ConnectionErrorDetails,
  ) {
    super(code, message)
    this.name = 'ConnectionError'
    this.details = details
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      details: serializeConnectionDetails(this.details),
    }
  }
}

// --- Validation Error ---

export interface ValidationErrorEntry {
  code:
    | 'UNKNOWN_TABLE'
    | 'UNKNOWN_COLUMN'
    | 'UNKNOWN_ROLE'
    | 'ACCESS_DENIED'
    | 'INVALID_FILTER'
    | 'INVALID_VALUE'
    | 'INVALID_JOIN'
    | 'INVALID_GROUP_BY'
    | 'INVALID_HAVING'
    | 'INVALID_ORDER_BY'
    | 'INVALID_BY_IDS'
    | 'INVALID_LIMIT'
    | 'INVALID_EXISTS'
    | 'INVALID_AGGREGATION'
  message: string
  details: {
    expected?: string | undefined
    actual?: string | undefined
    table?: string | undefined
    column?: string | undefined
    role?: string | undefined
    alias?: string | undefined
    operator?: string | undefined
    refColumn?: string | undefined
    refTable?: string | undefined
    filterIndex?: number | undefined
    fn?: string | undefined
    direction?: string | undefined
    logic?: string | undefined
  }
}

export class ValidationError extends MultiDbError {
  declare readonly code: 'VALIDATION_FAILED'
  readonly fromTable: string
  readonly errors: readonly ValidationErrorEntry[]

  constructor(fromTable: string, errors: readonly ValidationErrorEntry[]) {
    super('VALIDATION_FAILED', `Validation failed: ${errors.length} error${errors.length === 1 ? '' : 's'}`)
    this.name = 'ValidationError'
    this.fromTable = fromTable
    this.errors = errors
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      fromTable: this.fromTable,
      errors: this.errors,
    }
  }
}

// --- Planner Error ---

export type PlannerErrorDetails =
  | { code: 'UNREACHABLE_TABLES'; tables: readonly string[] }
  | { code: 'TRINO_DISABLED' }
  | { code: 'NO_CATALOG'; databases: readonly string[] }
  | { code: 'FRESHNESS_UNMET'; requiredFreshness: string; availableLag: string }

export class PlannerError extends MultiDbError {
  declare readonly code: 'UNREACHABLE_TABLES' | 'TRINO_DISABLED' | 'NO_CATALOG' | 'FRESHNESS_UNMET'
  readonly fromTable: string
  readonly details: PlannerErrorDetails

  constructor(
    code: 'UNREACHABLE_TABLES' | 'TRINO_DISABLED' | 'NO_CATALOG' | 'FRESHNESS_UNMET',
    fromTable: string,
    details: PlannerErrorDetails,
    message?: string | undefined,
  ) {
    super(code, message ?? defaultPlannerMessage(code, details))
    this.name = 'PlannerError'
    this.fromTable = fromTable
    this.details = details
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      fromTable: this.fromTable,
      details: this.details,
    }
  }
}

// --- Execution Error ---

export type ExecutionErrorDetails =
  | { code: 'EXECUTOR_MISSING'; database: string }
  | { code: 'CACHE_PROVIDER_MISSING'; cacheId: string }
  | {
      code: 'QUERY_FAILED'
      database: string
      dialect: 'postgres' | 'clickhouse' | 'trino'
      sql: string
      params: unknown[]
      cause?: Error | undefined
    }
  | {
      code: 'QUERY_TIMEOUT'
      database: string
      dialect: 'postgres' | 'clickhouse' | 'trino'
      sql: string
      timeoutMs: number
    }

export class ExecutionError extends MultiDbError {
  declare readonly code: 'EXECUTOR_MISSING' | 'CACHE_PROVIDER_MISSING' | 'QUERY_FAILED' | 'QUERY_TIMEOUT'
  readonly details: ExecutionErrorDetails

  constructor(details: ExecutionErrorDetails, cause?: Error | undefined) {
    super(details.code, defaultExecutionMessage(details), cause ? { cause } : undefined)
    this.name = 'ExecutionError'
    this.details = details
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      details: serializeExecutionDetails(this.details),
    }
  }
}

// --- Provider Error ---

export class ProviderError extends MultiDbError {
  declare readonly code: 'METADATA_LOAD_FAILED' | 'ROLE_LOAD_FAILED'
  readonly details: { provider: 'metadata' | 'role' }

  constructor(code: 'METADATA_LOAD_FAILED' | 'ROLE_LOAD_FAILED', message: string, cause?: Error | undefined) {
    super(code, message, cause ? { cause } : undefined)
    this.name = 'ProviderError'
    this.details = { provider: code === 'METADATA_LOAD_FAILED' ? 'metadata' : 'role' }
  }

  override toJSON(): Record<string, unknown> {
    return {
      ...super.toJSON(),
      details: this.details,
    }
  }
}

// --- Helpers ---

function serializeError(err: unknown): Record<string, unknown> | unknown {
  if (err instanceof MultiDbError) {
    return err.toJSON()
  }
  if (err instanceof Error) {
    const json: Record<string, unknown> = {
      message: err.message,
      name: err.name,
    }
    if (err.cause !== undefined) {
      json.cause = serializeError(err.cause)
    }
    return json
  }
  return err
}

function serializeConnectionDetails(details: ConnectionErrorDetails): unknown {
  if ('unreachable' in details) {
    return {
      unreachable: details.unreachable.map((u) => ({
        id: u.id,
        type: u.type,
        ...(u.engine !== undefined ? { engine: u.engine } : {}),
        ...(u.cause !== undefined ? { cause: serializeError(u.cause) } : {}),
      })),
    }
  }
  return details
}

function serializeExecutionDetails(details: ExecutionErrorDetails): unknown {
  if (details.code === 'QUERY_FAILED' && details.cause !== undefined) {
    const { cause, ...rest } = details
    return { ...rest, cause: serializeError(cause) }
  }
  return details
}

function defaultPlannerMessage(code: PlannerError['code'], details: PlannerErrorDetails): string {
  switch (code) {
    case 'UNREACHABLE_TABLES': {
      const d = details as { code: 'UNREACHABLE_TABLES'; tables: readonly string[] }
      return `Unreachable tables: ${d.tables.join(', ')}`
    }
    case 'TRINO_DISABLED':
      return 'Trino is disabled'
    case 'NO_CATALOG': {
      const d = details as { code: 'NO_CATALOG'; databases: readonly string[] }
      return `No Trino catalog for databases: ${d.databases.join(', ')}`
    }
    case 'FRESHNESS_UNMET': {
      const d = details as { code: 'FRESHNESS_UNMET'; requiredFreshness: string; availableLag: string }
      return `Freshness unmet: required ${d.requiredFreshness}, available lag ${d.availableLag}`
    }
  }
}

function defaultExecutionMessage(details: ExecutionErrorDetails): string {
  switch (details.code) {
    case 'EXECUTOR_MISSING':
      return `Executor missing for database: ${details.database}`
    case 'CACHE_PROVIDER_MISSING':
      return `Cache provider missing: ${details.cacheId}`
    case 'QUERY_FAILED':
      return `Query failed on ${details.dialect} database: ${details.database}`
    case 'QUERY_TIMEOUT':
      return `Query timeout on ${details.dialect} database: ${details.database} (${details.timeoutMs}ms)`
  }
}
