import type { DbExecutor } from '@mkven/multi-db-query'
import { Pool } from 'pg'

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
      const result = await pool.query(sql, params)
      return result.rows
    },

    async ping(): Promise<void> {
      await pool.query('SELECT 1')
    },

    async close(): Promise<void> {
      await pool.end()
    },
  }
}

export type { DbExecutor } from '@mkven/multi-db-query'
