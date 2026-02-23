import type { DbExecutor } from '@mkven/multi-db-query'
import { ConnectionError, ExecutionError } from '@mkven/multi-db-query'
import { Trino } from 'trino-client'

export interface TrinoExecutorConfig {
  readonly server: string
  readonly catalog?: string | undefined
  readonly schema?: string | undefined
  readonly user?: string | undefined
  readonly source?: string | undefined
  readonly timeoutMs?: number | undefined
}

/**
 * Inline a parameter value into Trino SQL.
 * Strings are escaped by doubling single-quotes (Trino's default SQL mode
 * does not use C-style backslash escapes).
 */
function escapeTrinoValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE'
  if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`
  if (Array.isArray(value)) {
    return `ARRAY[${value.map((v) => escapeTrinoValue(v)).join(', ')}]`
  }
  throw new Error(`Unsupported Trino parameter type: ${typeof value}`)
}

function rowToObject(columns: string[], row: unknown[]): Record<string, unknown> {
  const obj: Record<string, unknown> = {}
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i]
    if (col !== undefined) {
      obj[col] = row[i]
    }
  }
  return obj
}

export function createTrinoExecutor(config: TrinoExecutorConfig): DbExecutor {
  const options: Record<string, unknown> = { server: config.server }
  if (config.catalog !== undefined) options.catalog = config.catalog
  if (config.schema !== undefined) options.schema = config.schema
  if (config.source !== undefined) options.source = config.source
  if (config.user !== undefined) options.extraHeaders = { 'X-Trino-User': config.user }

  const trino = Trino.create(options as import('trino-client').ConnectionOptions)

  function inlineParams(sql: string, params: unknown[]): string {
    let idx = 0
    return sql.replace(/\?/g, () => {
      const value = params[idx]
      idx++
      return escapeTrinoValue(value)
    })
  }

  function throwTrinoError(sql: string, message: string): never {
    const cause = new Error(message)
    throw new ExecutionError(
      { code: 'QUERY_FAILED', database: 'trino', dialect: 'trino', sql, params: [], cause },
      cause,
    )
  }

  async function submitAndCollect(sql: string): Promise<Record<string, unknown>[]> {
    const iter = await trino.query(sql)
    const columns: string[] = []
    const allRows: Record<string, unknown>[] = []
    let queryId: string | undefined
    let timer: ReturnType<typeof setTimeout> | undefined

    try {
      if (config.timeoutMs !== undefined) {
        timer = setTimeout(() => {
          if (queryId !== undefined) trino.cancel(queryId).catch(() => {})
        }, config.timeoutMs)
      }

      for await (const result of iter) {
        queryId = result.id

        if (result.error !== undefined) throwTrinoError(sql, result.error.message)

        if (result.columns !== undefined && columns.length === 0) {
          for (const col of result.columns) {
            columns.push(col.name)
          }
        }

        if (result.data !== undefined) {
          for (const row of result.data) {
            allRows.push(rowToObject(columns, row))
          }
        }
      }

      return allRows
    } finally {
      if (timer !== undefined) clearTimeout(timer)
    }
  }

  return {
    async execute(sql: string, params: unknown[]): Promise<Record<string, unknown>[]> {
      try {
        const finalSql = params.length > 0 ? inlineParams(sql, params) : sql
        return await submitAndCollect(finalSql)
      } catch (err) {
        if (err instanceof ExecutionError) throw err
        const cause = err instanceof Error ? err : new Error(String(err))
        throw new ExecutionError(
          { code: 'QUERY_FAILED', database: 'trino', dialect: 'trino', sql, params: [...params], cause },
          cause,
        )
      }
    },

    async ping(): Promise<void> {
      try {
        await submitAndCollect('SELECT 1')
      } catch (err) {
        if (err instanceof ExecutionError || err instanceof ConnectionError) throw err
        throw new ConnectionError('CONNECTION_FAILED', 'Trino ping failed', {})
      }
    },

    async close(): Promise<void> {
      // trino-client is HTTP-based — no persistent connection to close
    },
  }
}

export type { DbExecutor } from '@mkven/multi-db-query'
