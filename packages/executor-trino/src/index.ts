import type { DbExecutor } from '@mkven/multi-db-query'
import { ExecutionError } from '@mkven/multi-db-query'

export interface TrinoExecutorConfig {
  readonly server: string
  readonly catalog?: string | undefined
  readonly schema?: string | undefined
  readonly user?: string | undefined
  readonly source?: string | undefined
  readonly timeoutMs?: number | undefined
}

interface TrinoResponseColumn {
  name: string
  type: string
}

interface TrinoResponseError {
  message: string
  errorCode: number
  errorName: string
}

interface TrinoResponse {
  id: string
  nextUri?: string
  columns?: TrinoResponseColumn[]
  data?: unknown[][]
  error?: TrinoResponseError
}

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
  const { server, catalog, schema, user, source, timeoutMs } = config

  function buildHeaders(): Record<string, string> {
    const h: Record<string, string> = { 'Content-Type': 'text/plain' }
    if (user !== undefined) h['X-Trino-User'] = user
    if (catalog !== undefined) h['X-Trino-Catalog'] = catalog
    if (schema !== undefined) h['X-Trino-Schema'] = schema
    if (source !== undefined) h['X-Trino-Source'] = source
    return h
  }

  function inlineParams(sql: string, params: unknown[]): string {
    let idx = 0
    return sql.replace(/\?/g, () => {
      const value = params[idx]
      idx++
      return escapeTrinoValue(value)
    })
  }

  async function submitAndCollect(sql: string): Promise<Record<string, unknown>[]> {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => controller.abort(), timeoutMs)
    }

    try {
      const res = await fetch(`${server}/v1/statement`, {
        method: 'POST',
        headers: buildHeaders(),
        body: sql,
        signal: controller.signal,
      })

      let body = (await res.json()) as TrinoResponse

      if (body.error !== undefined) {
        throw new ExecutionError(
          {
            code: 'QUERY_FAILED',
            database: 'trino',
            dialect: 'trino',
            sql,
            params: [],
            cause: new Error(body.error.message),
          },
          new Error(body.error.message),
        )
      }

      const columns: string[] = body.columns?.map((c) => c.name) ?? []
      const allRows: Record<string, unknown>[] = []

      if (body.data !== undefined) {
        for (const row of body.data) {
          allRows.push(rowToObject(columns, row))
        }
      }

      while (body.nextUri !== undefined) {
        const nextRes = await fetch(body.nextUri, { signal: controller.signal })
        body = (await nextRes.json()) as TrinoResponse

        if (body.error !== undefined) {
          throw new ExecutionError(
            {
              code: 'QUERY_FAILED',
              database: 'trino',
              dialect: 'trino',
              sql,
              params: [],
              cause: new Error(body.error.message),
            },
            new Error(body.error.message),
          )
        }

        if (body.columns !== undefined && columns.length === 0) {
          for (const col of body.columns) {
            columns.push(col.name)
          }
        }

        if (body.data !== undefined) {
          for (const row of body.data) {
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
      const finalSql = params.length > 0 ? inlineParams(sql, params) : sql
      return submitAndCollect(finalSql)
    },

    async ping(): Promise<void> {
      await submitAndCollect('SELECT 1')
    },

    async close(): Promise<void> {
      // No persistent connection to close — Trino uses stateless REST API
    },
  }
}

export type { DbExecutor } from '@mkven/multi-db-query'
