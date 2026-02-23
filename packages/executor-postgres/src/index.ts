import type { DbExecutor } from '@mkven/multi-db-query'
import { ConnectionError, ExecutionError } from '@mkven/multi-db-query'
import { Pool, types } from 'pg'

// Parse NUMERIC/DECIMAL and INT8 as JavaScript numbers instead of strings
types.setTypeParser(1700, parseFloat) // numeric / decimal
types.setTypeParser(20, Number) // int8 / bigint

export interface PostgresExecutorConfig {
  readonly connectionString?: string | undefined
  readonly host?: string | undefined
  readonly port?: number | undefined
  readonly database?: string | undefined
  readonly user?: string | undefined
  readonly password?: string | undefined
  readonly ssl?: boolean | Record<string, unknown> | undefined
  readonly max?: number | undefined
  readonly timeoutMs?: number | undefined
}

export function createPostgresExecutor(config: PostgresExecutorConfig): DbExecutor {
  const pool = new Pool({
    connectionString: config.connectionString,
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    ssl: config.ssl,
    max: config.max,
    statement_timeout: config.timeoutMs,
  })

  return {
    async execute(sql: string, params: unknown[]): Promise<Record<string, unknown>[]> {
      try {
        const result = await pool.query(sql, params)
        return result.rows
      } catch (err) {
        const cause = err instanceof Error ? err : new Error(String(err))
        throw new ExecutionError(
          { code: 'QUERY_FAILED', database: 'postgres', dialect: 'postgres', sql, params: [...params], cause },
          cause,
        )
      }
    },

    async ping(): Promise<void> {
      try {
        await pool.query('SELECT 1')
      } catch (_err) {
        throw new ConnectionError('CONNECTION_FAILED', 'PostgreSQL ping failed', {
          url: config.connectionString,
        })
      }
    },

    async close(): Promise<void> {
      await pool.end()
    },
  }
}

export type { DbExecutor } from '@mkven/multi-db-query'
