import { describe, expect, it } from 'vitest'
import type { CacheProvider, DbExecutor, ExecutionContext, MetadataConfig, RoleMeta } from '../src/index.js'
import { ConfigError, ConnectionError, ExecutionError, staticMetadata, staticRoles } from '../src/index.js'
import { createMultiDb } from '../src/pipeline.js'

// ── Mock helpers ───────────────────────────────────────────────

function mockExecutor(rows: Record<string, unknown>[] = []): DbExecutor {
  return {
    execute: async () => rows,
    ping: async () => {},
    close: async () => {},
  }
}

function mockCache(data: Map<string, Record<string, unknown> | null> = new Map()): CacheProvider {
  return {
    getMany: async (keys) => {
      const result = new Map<string, Record<string, unknown> | null>()
      for (const key of keys) {
        result.set(key, data.get(key) ?? null)
      }
      return result
    },
    ping: async () => {},
    close: async () => {},
  }
}

// ── Fixtures ───────────────────────────────────────────────────

const config: MetadataConfig = {
  databases: [
    { id: 'pg-main', engine: 'postgres' },
    { id: 'ch-analytics', engine: 'clickhouse' },
  ],
  tables: [
    {
      id: 'orders',
      apiName: 'orders',
      database: 'pg-main',
      physicalName: 'public.orders',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
        { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false },
        { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
      ],
      primaryKey: ['id'],
      relations: [],
    },
    {
      id: 'users',
      apiName: 'users',
      database: 'pg-main',
      physicalName: 'public.users',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
        { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
        { apiName: 'email', physicalName: 'email', type: 'string', nullable: false, maskingFn: 'email' },
      ],
      primaryKey: ['id'],
      relations: [],
    },
    {
      id: 'events',
      apiName: 'events',
      database: 'ch-analytics',
      physicalName: 'default.events',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
        { apiName: 'type', physicalName: 'type', type: 'string', nullable: false },
      ],
      primaryKey: ['id'],
      relations: [],
    },
  ],
  caches: [],
  externalSyncs: [],
}

const configWithCache: MetadataConfig = {
  ...config,
  caches: [{ id: 'redis-main', engine: 'redis', tables: [{ tableId: 'users', keyPattern: 'user:{id}' }] }],
}

const roles: RoleMeta[] = [
  { id: 'admin', tables: '*' },
  {
    id: 'tenant-user',
    tables: [
      { tableId: 'users', allowedColumns: '*', maskedColumns: ['email'] },
      { tableId: 'orders', allowedColumns: '*' },
    ],
  },
]

const adminCtx: ExecutionContext = { roles: { user: ['admin'] } }
const tenantCtx: ExecutionContext = { roles: { user: ['tenant-user'] } }

// ── Tests ──────────────────────────────────────────────────────

describe('Pipeline — createMultiDb init', () => {
  it('#53: connection failed — executor ping fails at init', async () => {
    const badExecutor: DbExecutor = {
      execute: async () => [],
      ping: async () => {
        throw new Error('ECONNREFUSED')
      },
      close: async () => {},
    }

    await expect(
      createMultiDb({
        metadataProvider: staticMetadata(config),
        roleProvider: staticRoles(roles),
        executors: { 'pg-main': badExecutor },
      }),
    ).rejects.toThrow(ConnectionError)
  })

  it('#63: lazy connections — validateConnections: false', async () => {
    const badExecutor: DbExecutor = {
      execute: async () => [],
      ping: async () => {
        throw new Error('ECONNREFUSED')
      },
      close: async () => {},
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': badExecutor },
      validateConnections: false,
    })

    // Init succeeds but health check detects issue
    const health = await db.healthCheck()
    expect(health.healthy).toBe(false)
    expect(health.executors['pg-main']?.healthy).toBe(false)
  })
})

describe('Pipeline — query modes', () => {
  it('#31: SQL-only mode', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor() },
    })

    const result = await db.query({
      definition: { from: 'orders', executeMode: 'sql-only' },
      context: adminCtx,
    })

    expect(result.kind).toBe('sql')
    if (result.kind === 'sql') {
      expect(result.sql).toContain('SELECT')
      expect(result.params).toBeDefined()
      expect(result.meta.strategy).toBe('direct')
      expect(result.meta.dialect).toBe('postgres')
    }
  })

  it('#14e: count mode', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ count: 42 }]) },
    })

    const result = await db.query({
      definition: { from: 'orders', executeMode: 'count' },
      context: adminCtx,
    })

    expect(result.kind).toBe('count')
    if (result.kind === 'count') {
      expect(result.count).toBe(42)
    }
  })

  it('#152: byIds + count mode', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ count: 3 }]) },
    })

    const result = await db.query({
      definition: { from: 'orders', byIds: [1, 2, 3], executeMode: 'count' },
      context: adminCtx,
    })

    expect(result.kind).toBe('count')
    if (result.kind === 'count') {
      expect(result.count).toBe(3)
    }
  })

  it('#76: count + groupBy ignored', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ count: 10 }]) },
    })

    const result = await db.query({
      definition: {
        from: 'orders',
        executeMode: 'count',
        groupBy: [{ column: 'status' }],
        aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      },
      context: adminCtx,
    })

    expect(result.kind).toBe('count')
    if (result.kind === 'count') {
      expect(result.count).toBe(10)
    }
  })
})

describe('Pipeline — debug', () => {
  it('#39: debug mode emits log entries', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: {
        'pg-main': mockExecutor([{ id: 1, total: 100, status: 'paid' }]),
      },
    })

    const result = await db.query({
      definition: { from: 'orders', debug: true },
      context: adminCtx,
    })

    expect(result.debugLog).toBeDefined()
    expect(result.debugLog?.length).toBeGreaterThanOrEqual(4)

    const phases = result.debugLog?.map((e) => e.phase) ?? []
    expect(phases).toContain('validation')
    expect(phases).toContain('planning')
    expect(phases).toContain('sql-generation')
    expect(phases).toContain('execution')
  })
})

describe('Pipeline — cache', () => {
  it('#35: masking on cached results', async () => {
    const cacheData = new Map<string, Record<string, unknown> | null>([
      ['user:1', { id: '1', name: 'Alice', email: 'alice@example.com' }],
      ['user:2', { id: '2', name: 'Bob', email: 'bob@example.com' }],
    ])

    const db = await createMultiDb({
      metadataProvider: staticMetadata(configWithCache),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor() },
      cacheProviders: { 'redis-main': mockCache(cacheData) },
    })

    const result = await db.query({
      definition: { from: 'users', byIds: ['1', '2'] },
      context: tenantCtx,
    })

    expect(result.kind).toBe('data')
    if (result.kind === 'data') {
      expect(result.data).toHaveLength(2)
      expect(result.meta.strategy).toBe('cache')
      // Email should be masked for tenant-user role
      for (const row of result.data) {
        const r = row as Record<string, unknown>
        expect(r.email).not.toBe('alice@example.com')
        expect(r.email).not.toBe('bob@example.com')
      }
    }
  })

  it('#48: cache provider missing', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(configWithCache),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor() },
      // No cache providers!
    })

    await expect(
      db.query({
        definition: { from: 'users', byIds: ['1'] },
        context: adminCtx,
      }),
    ).rejects.toThrow(ExecutionError)
  })

  it('#10: partial cache hit — cached + direct merge', async () => {
    // IDs 1,2 in cache; ID 3 miss → query DB for ID 3 only, merge results
    const cacheData = new Map<string, Record<string, unknown> | null>([
      ['user:1', { id: '1', name: 'Alice', email: 'alice@test.com' }],
      ['user:2', { id: '2', name: 'Bob', email: 'bob@test.com' }],
    ])

    const db = await createMultiDb({
      metadataProvider: staticMetadata(configWithCache),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ id: '3', name: 'Charlie', email: 'charlie@test.com' }]) },
      cacheProviders: { 'redis-main': mockCache(cacheData) },
    })

    const result = await db.query({
      definition: { from: 'users', byIds: ['1', '2', '3'] },
      context: adminCtx,
    })

    expect(result.kind).toBe('data')
    if (result.kind === 'data') {
      expect(result.data).toHaveLength(3)
      expect(result.meta.strategy).toBe('cache')
      // Verify all 3 rows present (2 from cache + 1 from DB)
      const names = result.data.map((r) => (r as Record<string, unknown>).name)
      expect(names).toContain('Alice')
      expect(names).toContain('Bob')
      expect(names).toContain('Charlie')
    }
  })
})

describe('Pipeline — error paths', () => {
  it('#44: executor missing', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor() },
      // No ch-analytics executor!
    })

    await expect(
      db.query({
        definition: { from: 'events' },
        context: adminCtx,
      }),
    ).rejects.toThrow(ExecutionError)
  })

  it('#58: query execution fails', async () => {
    const failingEx: DbExecutor = {
      execute: async () => {
        throw new Error('connection reset')
      },
      ping: async () => {},
      close: async () => {},
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': failingEx },
    })

    try {
      await db.query({
        definition: { from: 'orders' },
        context: adminCtx,
      })
      expect.fail('Should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ExecutionError)
      const ee = err as ExecutionError
      expect(ee.details.code).toBe('QUERY_FAILED')
      if (ee.details.code === 'QUERY_FAILED') {
        expect(ee.details.sql).toContain('SELECT')
        expect(ee.details.dialect).toBe('postgres')
      }
    }
  })

  it('#131: query timeout', async () => {
    const timeoutEx: DbExecutor = {
      execute: async () => {
        throw Object.assign(new Error('statement_timeout exceeded'), { timeoutMs: 100 })
      },
      ping: async () => {},
      close: async () => {},
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': timeoutEx },
    })

    try {
      await db.query({
        definition: { from: 'orders' },
        context: adminCtx,
      })
      expect.fail('Should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ExecutionError)
      const ee = err as ExecutionError
      expect(ee.details.code).toBe('QUERY_TIMEOUT')
      if (ee.details.code === 'QUERY_TIMEOUT') {
        expect(ee.details.timeoutMs).toBe(100)
      }
    }
  })

  it('#172: executor timeout enforcement', async () => {
    const timeoutEx: DbExecutor = {
      execute: async () => {
        throw Object.assign(new Error('timeout after 100ms'), { timeoutMs: 100 })
      },
      ping: async () => {},
      close: async () => {},
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': timeoutEx },
    })

    try {
      await db.query({
        definition: { from: 'orders' },
        context: adminCtx,
      })
      expect.fail('Should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ExecutionError)
      const ee = err as ExecutionError
      expect(ee.details.code).toBe('QUERY_TIMEOUT')
    }
  })
})

describe('Pipeline — health check', () => {
  it('#60: health check returns per-provider status', async () => {
    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor() },
      cacheProviders: { 'redis-main': mockCache() },
    })

    const health = await db.healthCheck()
    expect(health.healthy).toBe(true)
    expect(health.executors['pg-main']?.healthy).toBe(true)
    expect(health.executors['pg-main']?.latencyMs).toBeGreaterThanOrEqual(0)
    expect(health.cacheProviders['redis-main']?.healthy).toBe(true)
  })
})

describe('Pipeline — close lifecycle', () => {
  it('#105: close then query throws EXECUTOR_MISSING', async () => {
    let closeCalled = false
    const ex: DbExecutor = {
      execute: async () => [],
      ping: async () => {},
      close: async () => {
        closeCalled = true
      },
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': ex },
    })

    await db.close()
    expect(closeCalled).toBe(true)

    await expect(
      db.query({
        definition: { from: 'orders' },
        context: adminCtx,
      }),
    ).rejects.toThrow(ExecutionError)
  })

  it('#170: close partial failure', async () => {
    const goodEx: DbExecutor = {
      execute: async () => [],
      ping: async () => {},
      close: async () => {},
    }
    const badEx: DbExecutor = {
      execute: async () => [],
      ping: async () => {},
      close: async () => {
        throw new Error('close failed')
      },
    }

    const twoDbConfig: MetadataConfig = {
      ...config,
      databases: [...config.databases, { id: 'pg-tenant', engine: 'postgres' }],
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(twoDbConfig),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': goodEx, 'pg-tenant': badEx },
    })

    try {
      await db.close()
      expect.fail('Should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConnectionError)
      // Subsequent queries should throw
      await expect(db.query({ definition: { from: 'orders' }, context: adminCtx })).rejects.toThrow(ExecutionError)
    }
  })
})

describe('Pipeline — reload', () => {
  it('#61: hot-reload metadata — new table visible', async () => {
    let currentConfig = config
    const provider = {
      load: async () => currentConfig,
    }

    const db = await createMultiDb({
      metadataProvider: provider,
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ id: 1 }]) },
    })

    // Add a new table
    currentConfig = {
      ...config,
      tables: [
        ...config.tables,
        {
          id: 'products',
          apiName: 'products',
          database: 'pg-main',
          physicalName: 'public.products',
          columns: [{ apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false }],
          primaryKey: ['id'],
          relations: [],
        },
      ],
    }

    await db.reloadMetadata()

    // Query the new table
    const result = await db.query({
      definition: { from: 'products' },
      context: adminCtx,
    })
    expect(result.kind).toBe('data')
  })

  it('#228: reload with invalid config — old config preserved', async () => {
    let currentConfig = config
    const provider = {
      load: async () => currentConfig,
    }

    const db = await createMultiDb({
      metadataProvider: provider,
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': mockExecutor([{ id: 1 }]) },
    })

    // Set invalid config (duplicate apiNames)
    currentConfig = {
      ...config,
      tables: [
        ...config.tables,
        {
          id: 'orders-dup',
          apiName: 'orders', // duplicate!
          database: 'pg-main',
          physicalName: 'public.orders2',
          columns: [{ apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false }],
          primaryKey: ['id'],
          relations: [],
        },
      ],
    }

    await expect(db.reloadMetadata()).rejects.toThrow(ConfigError)

    // Old config still works
    const result = await db.query({
      definition: { from: 'orders' },
      context: adminCtx,
    })
    expect(result.kind).toBe('data')
  })

  it('#171: snapshot isolation during reload', async () => {
    let queryCount = 0
    const slowExecutor: DbExecutor = {
      execute: async () => {
        queryCount++
        // Simulate slow query
        await new Promise((r) => setTimeout(r, 50))
        return [{ id: queryCount }]
      },
      ping: async () => {},
      close: async () => {},
    }

    let currentConfig = config
    const provider = { load: async () => currentConfig }

    const db = await createMultiDb({
      metadataProvider: provider,
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': slowExecutor },
    })

    // Start a query (uses current snapshot)
    const queryPromise = db.query({
      definition: { from: 'orders' },
      context: adminCtx,
    })

    // Reload metadata while query is in-flight
    currentConfig = {
      ...config,
      tables: [
        ...config.tables,
        {
          id: 'products',
          apiName: 'products',
          database: 'pg-main',
          physicalName: 'public.products',
          columns: [{ apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false }],
          primaryKey: ['id'],
          relations: [],
        },
      ],
    }
    await db.reloadMetadata()

    // Original query should still succeed with old snapshot
    const result = await queryPromise
    expect(result.kind).toBe('data')
  })
})

describe('Pipeline — error serialization', () => {
  it('#132: error toJSON() serialization', async () => {
    const failingEx: DbExecutor = {
      execute: async () => {
        throw new Error('connection reset')
      },
      ping: async () => {},
      close: async () => {},
    }

    const db = await createMultiDb({
      metadataProvider: staticMetadata(config),
      roleProvider: staticRoles(roles),
      executors: { 'pg-main': failingEx },
    })

    try {
      await db.query({
        definition: { from: 'orders' },
        context: adminCtx,
      })
      expect.fail('Should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ExecutionError)
      const ee = err as ExecutionError
      const json = ee.toJSON()
      expect(json.code).toBe('QUERY_FAILED')
      expect(json.message).toBeDefined()
      // JSON.stringify should not throw
      const str = JSON.stringify(json)
      expect(str).toContain('QUERY_FAILED')
    }
  })
})
