import type { HealthCheckResult, QueryResult } from '@mkven/multi-db-validation'
import { ConnectionError, ExecutionError, PlannerError, ValidationError } from '@mkven/multi-db-validation'
import { describe, expect, it } from 'vitest'
import { createMultiDbClient } from '../src/client.js'

// ── Mock fetch helper ──────────────────────────────────────────

type FetchFn = (
  url: string,
  init?: Record<string, unknown>,
) => Promise<{
  ok: boolean
  status: number
  json(): Promise<unknown>
}>

function mockFetch(
  handler: (url: string, init?: Record<string, unknown>) => { status: number; body: unknown },
): FetchFn {
  return async (url, init) => {
    const result = handler(url, init)
    return {
      ok: result.status >= 200 && result.status < 300,
      status: result.status,
      json: async () => result.body,
    }
  }
}

// ── Tests ──────────────────────────────────────────────────────

describe('HTTP Client — query', () => {
  it('#208: successful query returns DataResult', async () => {
    const responseBody: QueryResult = {
      kind: 'data',
      data: [{ id: 1, name: 'Order 1' }],
      meta: {
        strategy: 'direct',
        targetDatabase: 'pg-main',
        dialect: 'postgres',
        tablesUsed: [],
        columns: [],
        timing: { planningMs: 1, generationMs: 1, executionMs: 2 },
      },
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 200, body: responseBody })) as typeof globalThis.fetch,
    })

    const result = await client.query({
      definition: { from: 'orders' },
      context: { roles: { user: ['admin'] } },
    })

    expect(result.kind).toBe('data')
    if (result.kind === 'data') {
      expect(result.data).toHaveLength(1)
    }
  })

  it('#209: SQL-only mode returns SqlResult', async () => {
    const responseBody: QueryResult = {
      kind: 'sql',
      sql: 'SELECT * FROM orders',
      params: [],
      meta: {
        strategy: 'direct',
        targetDatabase: 'pg-main',
        dialect: 'postgres',
        tablesUsed: [],
        columns: [],
        timing: { planningMs: 1, generationMs: 1 },
      },
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 200, body: responseBody })) as typeof globalThis.fetch,
    })

    const result = await client.query({
      definition: { from: 'orders', executeMode: 'sql-only' },
      context: { roles: { user: ['admin'] } },
    })

    expect(result.kind).toBe('sql')
  })

  it('#210: count mode returns CountResult', async () => {
    const responseBody: QueryResult = {
      kind: 'count',
      count: 42,
      meta: {
        strategy: 'direct',
        targetDatabase: 'pg-main',
        dialect: 'postgres',
        tablesUsed: [],
        columns: [],
        timing: { planningMs: 1, generationMs: 1, executionMs: 2 },
      },
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 200, body: responseBody })) as typeof globalThis.fetch,
    })

    const result = await client.query({
      definition: { from: 'orders', executeMode: 'count' },
      context: { roles: { user: ['admin'] } },
    })

    expect(result.kind).toBe('count')
    if (result.kind === 'count') {
      expect(result.count).toBe(42)
    }
  })
})

describe('HTTP Client — error deserialization', () => {
  it('#211: ValidationError deserialization', async () => {
    const errorBody = {
      code: 'VALIDATION_FAILED',
      message: 'Validation failed: 1 error',
      fromTable: 'orders',
      errors: [{ code: 'UNKNOWN_TABLE', message: 'Table not found', details: {} }],
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 400, body: errorBody })) as typeof globalThis.fetch,
    })

    await expect(
      client.query({
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(ValidationError)
  })

  it('#212: ExecutionError deserialization', async () => {
    const errorBody = {
      code: 'QUERY_FAILED',
      message: 'Query failed',
      details: {
        code: 'QUERY_FAILED',
        database: 'pg-main',
        dialect: 'postgres',
        sql: 'SELECT * FROM orders',
        params: [],
      },
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 500, body: errorBody })) as typeof globalThis.fetch,
    })

    await expect(
      client.query({
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(ExecutionError)
  })

  it('#226: PlannerError deserialization', async () => {
    const errorBody = {
      code: 'UNREACHABLE_TABLES',
      message: 'Tables unreachable',
      fromTable: 'orders',
      details: { code: 'UNREACHABLE_TABLES', tables: ['events'] },
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 422, body: errorBody })) as typeof globalThis.fetch,
    })

    await expect(
      client.query({
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(PlannerError)
  })
})

describe('HTTP Client — network', () => {
  it('#213: ConnectionError on network failure', async () => {
    const failFetch = async () => {
      throw new TypeError('fetch failed')
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: failFetch as typeof globalThis.fetch,
    })

    await expect(
      client.query({
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(ConnectionError)
  })

  it('#214: request timeout', async () => {
    const slowFetch = async (_url: string, init?: Record<string, unknown>) => {
      // Wait until abort signal fires
      const signal = init?.signal as AbortSignal | undefined
      await new Promise((resolve, reject) => {
        if (signal?.aborted) {
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))
          return
        }
        const timer = setTimeout(resolve, 5000)
        if (signal !== undefined) {
          signal.addEventListener('abort', () => {
            clearTimeout(timer)
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }))
          })
        }
      })
      return { ok: true, status: 200, json: async () => ({}) }
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: slowFetch as typeof globalThis.fetch,
      timeout: 50,
    })

    await expect(
      client.query({
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(ConnectionError)
  })

  it('#215: custom headers sent', async () => {
    let capturedHeaders: Record<string, string> = {}

    const captureFetch = mockFetch((_url, init) => {
      capturedHeaders = (init?.headers as Record<string, string>) ?? {}
      return {
        status: 200,
        body: {
          kind: 'data',
          data: [],
          meta: {
            strategy: 'direct',
            targetDatabase: 'pg-main',
            tablesUsed: [],
            columns: [],
            timing: { planningMs: 0, generationMs: 0 },
          },
        },
      }
    })

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: captureFetch as typeof globalThis.fetch,
      headers: { Authorization: 'Bearer test-token' },
    })

    await client.query({
      definition: { from: 'orders' },
      context: { roles: { user: ['admin'] } },
    })

    expect(capturedHeaders.Authorization).toBe('Bearer test-token')
  })

  it('#218: custom fetch injection', async () => {
    let fetchCalled = false

    const injectedFetch = mockFetch(() => {
      fetchCalled = true
      return {
        status: 200,
        body: {
          kind: 'data',
          data: [],
          meta: {
            strategy: 'direct',
            targetDatabase: 'pg-main',
            tablesUsed: [],
            columns: [],
            timing: { planningMs: 0, generationMs: 0 },
          },
        },
      }
    })

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: injectedFetch as typeof globalThis.fetch,
    })

    await client.query({
      definition: { from: 'orders' },
      context: { roles: { user: ['admin'] } },
    })

    expect(fetchCalled).toBe(true)
  })
})

describe('HTTP Client — health check', () => {
  it('#217: health check returns typed result', async () => {
    const healthBody: HealthCheckResult = {
      healthy: true,
      executors: { 'pg-main': { healthy: true, latencyMs: 5 } },
      cacheProviders: {},
    }

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: mockFetch(() => ({ status: 200, body: healthBody })) as typeof globalThis.fetch,
    })

    const health = await client.healthCheck()
    expect(health.healthy).toBe(true)
    expect(health.executors['pg-main']?.healthy).toBe(true)
  })
})

describe('HTTP Client — local validation', () => {
  it('#216: local validation before send — fails without network', async () => {
    let fetchCalled = false
    const trackedFetch = mockFetch(() => {
      fetchCalled = true
      return { status: 200, body: {} }
    })

    const client = createMultiDbClient({
      baseUrl: 'http://localhost:3000',
      fetch: trackedFetch as typeof globalThis.fetch,
      validateBeforeSend: true,
      metadata: {
        databases: [{ id: 'pg-main', engine: 'postgres' }],
        tables: [
          {
            id: 'orders',
            apiName: 'orders',
            database: 'pg-main',
            physicalName: 'public.orders',
            columns: [{ apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false }],
            primaryKey: ['id'],
            relations: [],
          },
        ],
        caches: [],
        externalSyncs: [],
      },
      roles: [{ id: 'admin', tables: '*' as const }],
    })

    // Query a non-existent table — should fail locally
    await expect(
      client.query({
        definition: { from: 'nonExistentTable' },
        context: { roles: { user: ['admin'] } },
      }),
    ).rejects.toThrow(ValidationError)

    // Fetch should NOT have been called
    expect(fetchCalled).toBe(false)
  })
})
