// --- DbExecutor (implemented by executor packages) ---

/**
 * Database executor interface.
 *
 * Error contract:
 * - `execute()` must throw `ExecutionError` (code: `'QUERY_FAILED'`) on any failure.
 * - `ping()` must throw `ConnectionError` (code: `'CONNECTION_FAILED'`) on any failure.
 * - `close()` should attempt cleanup; failures may propagate as raw errors.
 */
export interface DbExecutor {
  execute(sql: string, params: unknown[]): Promise<Record<string, unknown>[]>
  ping(): Promise<void>
  close(): Promise<void>
}

// --- CacheProvider (implemented by cache packages) ---

export interface CacheProvider {
  getMany(keys: string[]): Promise<Map<string, Record<string, unknown> | null>>
  ping(): Promise<void>
  close(): Promise<void>
}
