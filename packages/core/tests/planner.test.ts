import type {
  CachedTableMeta,
  CacheMeta,
  DatabaseMeta,
  ExternalSync,
  MetadataConfig,
  RoleMeta,
  TableMeta,
} from '@mkven/multi-db-validation'
import { MetadataIndex, PlannerError } from '@mkven/multi-db-validation'
import { describe, expect, it } from 'vitest'
import type { RegistrySnapshot } from '../src/metadataRegistry.js'
import { planQuery } from '../src/planner.js'

// --- Fixtures ---

const pgMain: DatabaseMeta = { id: 'pg-main', engine: 'postgres', trinoCatalog: 'pg_main' }
const pgSecondary: DatabaseMeta = { id: 'pg-secondary', engine: 'postgres', trinoCatalog: 'pg_secondary' }
const chAnalytics: DatabaseMeta = { id: 'ch-analytics', engine: 'clickhouse', trinoCatalog: 'ch_analytics' }
const icebergArchive: DatabaseMeta = { id: 'iceberg-archive', engine: 'iceberg', trinoCatalog: 'iceberg_archive' }
const pgNoCatalog: DatabaseMeta = { id: 'pg-no-catalog', engine: 'postgres' }

const usersTable: TableMeta = {
  id: 'users',
  database: 'pg-main',
  physicalName: 'public.users',
  apiName: 'users',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'email', physicalName: 'email', type: 'string', nullable: false },
  ],
  relations: [],
}

const ordersTable: TableMeta = {
  id: 'orders',
  database: 'pg-main',
  physicalName: 'public.orders',
  apiName: 'orders',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'amount', physicalName: 'amount', type: 'decimal', nullable: false },
  ],
  relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const }],
}

const productsTable: TableMeta = {
  id: 'products',
  database: 'pg-main',
  physicalName: 'public.products',
  apiName: 'products',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'price', physicalName: 'price', type: 'decimal', nullable: false },
  ],
  relations: [],
}

const tenantsTable: TableMeta = {
  id: 'tenants',
  database: 'pg-secondary',
  physicalName: 'public.tenants',
  apiName: 'tenants',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
  ],
  relations: [],
}

const invoicesTable: TableMeta = {
  id: 'invoices',
  database: 'pg-secondary',
  physicalName: 'public.invoices',
  apiName: 'invoices',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false },
  ],
  relations: [],
}

const eventsTable: TableMeta = {
  id: 'events',
  database: 'ch-analytics',
  physicalName: 'default.events',
  apiName: 'events',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'type', physicalName: 'type', type: 'string', nullable: false },
  ],
  relations: [],
}

const archiveTable: TableMeta = {
  id: 'ordersArchive',
  database: 'iceberg-archive',
  physicalName: 'archive.orders',
  apiName: 'ordersArchive',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'amount', physicalName: 'amount', type: 'decimal', nullable: false },
  ],
  relations: [],
}

const metricsTable: TableMeta = {
  id: 'metrics',
  database: 'pg-no-catalog',
  physicalName: 'public.metrics',
  apiName: 'metrics',
  primaryKey: ['id'],
  columns: [{ apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false }],
  relations: [],
}

// Syncs
const tenantSync: ExternalSync = {
  sourceTable: 'tenants',
  targetDatabase: 'pg-main',
  targetPhysicalName: 'replicas.tenants',
  method: 'debezium',
  estimatedLag: 'seconds',
}

const ordersSyncToCh: ExternalSync = {
  sourceTable: 'orders',
  targetDatabase: 'ch-analytics',
  targetPhysicalName: 'replicas.orders',
  method: 'debezium',
  estimatedLag: 'minutes',
}

const eventsSyncToPg: ExternalSync = {
  sourceTable: 'events',
  targetDatabase: 'pg-main',
  targetPhysicalName: 'replicas.events',
  method: 'debezium',
  estimatedLag: 'hours',
}

// Cache
const usersCache: CachedTableMeta = { tableId: 'users', keyPattern: 'user:{id}' }
const usersCacheSubset: CachedTableMeta = { tableId: 'users', keyPattern: 'user:{id}', columns: ['id', 'name'] }
const productsCacheSubset: CachedTableMeta = {
  tableId: 'products',
  keyPattern: 'product:{id}',
  columns: ['id', 'name'],
}

const redisCache: CacheMeta = { id: 'redis-main', engine: 'redis', tables: [usersCache] }
const redisCacheSubset: CacheMeta = { id: 'redis-main', engine: 'redis', tables: [usersCacheSubset] }
const redisCacheProducts: CacheMeta = { id: 'redis-main', engine: 'redis', tables: [productsCacheSubset] }

const adminRole: RoleMeta = { id: 'admin', tables: '*' }

// --- Snapshot builder ---

function snap(overrides: {
  databases?: DatabaseMeta[]
  tables?: TableMeta[]
  syncs?: ExternalSync[]
  caches?: CacheMeta[]
}): RegistrySnapshot {
  const databases = overrides.databases ?? [pgMain, pgSecondary, chAnalytics, icebergArchive]
  const tables = overrides.tables ?? [
    usersTable,
    ordersTable,
    productsTable,
    tenantsTable,
    invoicesTable,
    eventsTable,
    archiveTable,
  ]
  const syncs = overrides.syncs ?? []
  const caches = overrides.caches ?? []

  const config: MetadataConfig = {
    databases,
    tables,
    caches,
    externalSyncs: syncs,
  }
  const roles = [adminRole]
  const index = new MetadataIndex(config, roles)

  // Build indexes
  const syncsByTable = new Map<string, ExternalSync[]>()
  for (const sync of syncs) {
    const existing = syncsByTable.get(sync.sourceTable)
    if (existing !== undefined) {
      existing.push(sync)
    } else {
      syncsByTable.set(sync.sourceTable, [sync])
    }
  }

  const cachesByTable = new Map<string, CachedTableMeta[]>()
  for (const cache of caches) {
    for (const ct of cache.tables) {
      const existing = cachesByTable.get(ct.tableId)
      if (existing !== undefined) {
        existing.push(ct)
      } else {
        cachesByTable.set(ct.tableId, [ct])
      }
    }
  }

  const connectivityGraph = syncs
    .map((sync) => {
      const table = tables.find((t) => t.id === sync.sourceTable)
      if (table === undefined) return undefined
      return { sourceDatabase: table.database, targetDatabase: sync.targetDatabase, method: 'debezium' as const }
    })
    .filter((e): e is NonNullable<typeof e> => e !== undefined)

  return { index, config, roles, syncsByTable, cachesByTable, connectivityGraph }
}

// --- Tests ---

describe('Planner — P1: Direct (single database)', () => {
  it('#1: single PG table → direct pg-main', () => {
    const s = snap({})
    const plan = planQuery({ from: 'orders' }, s)
    expect(plan.strategy).toBe('direct')
    if (plan.strategy === 'direct') {
      expect(plan.database).toBe('pg-main')
      expect(plan.dialect).toBe('postgres')
    }
  })

  it('#2: join within same PG → direct pg-main', () => {
    const s = snap({})
    const plan = planQuery({ from: 'orders', joins: [{ table: 'products' }] }, s)
    expect(plan.strategy).toBe('direct')
    if (plan.strategy === 'direct') {
      expect(plan.database).toBe('pg-main')
    }
  })

  it('#64: multi-table join (3 tables) same DB → direct', () => {
    const s = snap({})
    const plan = planQuery({ from: 'orders', joins: [{ table: 'users' }, { table: 'products' }] }, s)
    expect(plan.strategy).toBe('direct')
    if (plan.strategy === 'direct') {
      expect(plan.database).toBe('pg-main')
    }
  })

  it('#79: single Iceberg table → direct via trino dialect', () => {
    const s = snap({})
    const plan = planQuery({ from: 'ordersArchive' }, s)
    expect(plan.strategy).toBe('direct')
    if (plan.strategy === 'direct') {
      expect(plan.database).toBe('iceberg-archive')
      expect(plan.dialect).toBe('trino')
    }
  })
})

describe('Planner — P0: Cache', () => {
  it('#8: byIds with cache → cache strategy', () => {
    const s = snap({ caches: [redisCache] })
    const plan = planQuery({ from: 'users', byIds: ['id1', 'id2', 'id3'] }, s)
    expect(plan.strategy).toBe('cache')
    if (plan.strategy === 'cache') {
      expect(plan.cacheId).toBe('redis-main')
      expect(plan.tableId).toBe('users')
      expect(plan.fallbackDatabase).toBe('pg-main')
      expect(plan.fallbackDialect).toBe('postgres')
    }
  })

  it('#9: byIds with no cache → direct', () => {
    const s = snap({ caches: [] })
    const plan = planQuery({ from: 'orders', byIds: ['id1'] }, s)
    expect(plan.strategy).toBe('direct')
  })

  it('#33: byIds + filters → skip cache → direct', () => {
    const s = snap({ caches: [redisCache] })
    const plan = planQuery(
      {
        from: 'users',
        byIds: ['id1'],
        filters: [{ column: 'name', operator: '=', value: 'Alice' }],
      },
      s,
    )
    expect(plan.strategy).toBe('direct')
  })

  it('#103: cache column subset — requested includes price not in cache → skip cache', () => {
    const s = snap({ caches: [redisCacheProducts] })
    const plan = planQuery(
      {
        from: 'products',
        byIds: ['id1'],
        columns: ['id', 'name', 'price'],
      },
      s,
    )
    // productsCacheSubset has only ['id', 'name'], not 'price'
    expect(plan.strategy).toBe('direct')
  })

  it('#130: byIds + cache has subset columns — requested columns ⊆ cached → cache', () => {
    const s = snap({ caches: [redisCacheSubset] })
    const plan = planQuery(
      {
        from: 'users',
        byIds: ['id1'],
        columns: ['id', 'name'],
      },
      s,
    )
    expect(plan.strategy).toBe('cache')
  })
})

describe('Planner — P2: Materialized replica', () => {
  it('#3: cross-PG, debezium available → materialized pg-main', () => {
    const s = snap({ syncs: [tenantSync] })
    const plan = planQuery({ from: 'orders', joins: [{ table: 'tenants' }] }, s)
    expect(plan.strategy).toBe('materialized')
    if (plan.strategy === 'materialized') {
      expect(plan.database).toBe('pg-main')
      expect(plan.dialect).toBe('postgres')
      expect(plan.tableOverrides.get('tenants')).toBe('replicas.tenants')
    }
  })

  it('#5: PG + CH, debezium available → materialized ch-analytics', () => {
    const s = snap({ syncs: [ordersSyncToCh] })
    const plan = planQuery({ from: 'events', joins: [{ table: 'orders' }] }, s)
    expect(plan.strategy).toBe('materialized')
    if (plan.strategy === 'materialized') {
      expect(plan.database).toBe('ch-analytics')
      expect(plan.dialect).toBe('clickhouse')
      expect(plan.tableOverrides.get('orders')).toBe('replicas.orders')
    }
  })

  it('#12: freshness=hours, lag=minutes → materialized OK', () => {
    const s = snap({ syncs: [ordersSyncToCh] })
    const plan = planQuery({ from: 'events', joins: [{ table: 'orders' }], freshness: 'hours' }, s)
    expect(plan.strategy).toBe('materialized')
  })

  it('prefers DB with most originals', () => {
    // Both pg-main and ch-analytics can serve orders+events
    // pg-main: orders original, events replicated from ch → 1 original
    // ch-analytics: events original, orders replicated from pg → 1 original
    // Tie-break: first found (pg-main listed first in databases)
    const s = snap({ syncs: [ordersSyncToCh, eventsSyncToPg] })
    const plan = planQuery({ from: 'orders', joins: [{ table: 'events' }] }, s)
    expect(plan.strategy).toBe('materialized')
  })
})

describe('Planner — P3: Trino cross-database', () => {
  it('#4: cross-PG, no debezium → trino', () => {
    const s = snap({ syncs: [] })
    const plan = planQuery({ from: 'orders', joins: [{ table: 'invoices' }] }, s, { trinoEnabled: true })
    expect(plan.strategy).toBe('trino')
    if (plan.strategy === 'trino') {
      expect(plan.catalogs.get('pg-main')).toBe('pg_main')
      expect(plan.catalogs.get('pg-secondary')).toBe('pg_secondary')
    }
  })

  it('#6: PG + CH, no debezium → trino', () => {
    const s = snap({ syncs: [] })
    const plan = planQuery({ from: 'users', joins: [{ table: 'events' }] }, s, { trinoEnabled: true })
    expect(plan.strategy).toBe('trino')
    if (plan.strategy === 'trino') {
      expect(plan.catalogs.get('pg-main')).toBe('pg_main')
      expect(plan.catalogs.get('ch-analytics')).toBe('ch_analytics')
    }
  })

  it('#11: freshness=realtime → skip materialized → trino', () => {
    const s = snap({ syncs: [ordersSyncToCh] })
    const plan = planQuery({ from: 'events', joins: [{ table: 'orders' }], freshness: 'realtime' }, s, {
      trinoEnabled: true,
    })
    expect(plan.strategy).toBe('trino')
  })
})

describe('Planner — P4: Error', () => {
  it('#19: trino disabled → TRINO_DISABLED', () => {
    const s = snap({ syncs: [] })
    expect(() => {
      planQuery({ from: 'orders', joins: [{ table: 'invoices' }] }, s)
    }).toThrow(PlannerError)

    try {
      planQuery({ from: 'orders', joins: [{ table: 'invoices' }] }, s)
    } catch (e) {
      expect(e).toBeInstanceOf(PlannerError)
      expect((e as PlannerError).code).toBe('TRINO_DISABLED')
    }
  })

  it('#56: trino enabled but DB missing trinoCatalog → NO_CATALOG', () => {
    const s = snap({
      databases: [pgMain, pgNoCatalog],
      tables: [usersTable, metricsTable],
    })
    expect(() => {
      planQuery({ from: 'users', joins: [{ table: 'metrics' }] }, s, { trinoEnabled: true })
    }).toThrow(PlannerError)

    try {
      planQuery({ from: 'users', joins: [{ table: 'metrics' }] }, s, { trinoEnabled: true })
    } catch (e) {
      expect((e as PlannerError).code).toBe('NO_CATALOG')
    }
  })

  it('#57: freshness unmet → FRESHNESS_UNMET', () => {
    // orders+events cross-DB, sync exists but freshness=seconds, lag=minutes
    const s = snap({ syncs: [ordersSyncToCh] })
    expect(() => {
      planQuery({ from: 'events', joins: [{ table: 'orders' }], freshness: 'seconds' }, s)
    }).toThrow(PlannerError)

    try {
      planQuery({ from: 'events', joins: [{ table: 'orders' }], freshness: 'seconds' }, s)
    } catch (e) {
      expect((e as PlannerError).code).toBe('FRESHNESS_UNMET')
    }
  })

  it('#59: unreachable tables → UNREACHABLE_TABLES', () => {
    // metrics in pg-no-catalog, tenants in pg-secondary, no syncs, no trino catalog
    const s = snap({
      databases: [pgNoCatalog, pgSecondary],
      tables: [metricsTable, tenantsTable],
      syncs: [],
    })
    expect(() => {
      planQuery({ from: 'metrics', joins: [{ table: 'tenants' }] }, s, { trinoEnabled: true })
    }).toThrow(PlannerError)

    try {
      planQuery({ from: 'metrics', joins: [{ table: 'tenants' }] }, s, { trinoEnabled: true })
    } catch (e) {
      expect((e as PlannerError).code).toBe('NO_CATALOG')
    }
  })
})
