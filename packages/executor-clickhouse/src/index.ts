import { createClient } from '@clickhouse/client'
import type { DbExecutor } from '@mkven/multi-db-query'

export interface ClickHouseExecutorConfig {
  readonly url?: string | undefined
  readonly username?: string | undefined
  readonly password?: string | undefined
  readonly database?: string | undefined
  readonly timeoutMs?: number | undefined
}

export function createClickHouseExecutor(config: ClickHouseExecutorConfig): DbExecutor {
  const settings: Record<string, number | string | boolean> = {}
  if (config.timeoutMs !== undefined) {
    settings.max_execution_time = Math.ceil(config.timeoutMs / 1000)
  }

  const client = createClient({
    url: config.url,
    username: config.username,
    password: config.password,
    database: config.database,
    clickhouse_settings: settings,
  })

  return {
    async execute(sql: string, params: unknown[]): Promise<Record<string, unknown>[]> {
      const queryParams: Record<string, unknown> = {}
      for (let i = 0; i < params.length; i++) {
        queryParams[`p${String(i + 1)}`] = params[i]
      }

      const result = await client.query({
        query: sql,
        query_params: queryParams,
        format: 'JSONEachRow',
      })

      return result.json()
    },

    async ping(): Promise<void> {
      const result = await client.ping()
      if (!result.success) {
        throw new Error('ClickHouse ping failed')
      }
    },

    async close(): Promise<void> {
      await client.close()
    },
  }
}

export type { DbExecutor } from '@mkven/multi-db-query'
