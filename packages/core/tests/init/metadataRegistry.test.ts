import type { MetadataConfig, RoleMeta, TableMeta } from '@mkven/multi-db-validation'
import { describe, expect, it } from 'vitest'
import { staticMetadata, staticRoles } from '../../src/metadata/providers.js'
import { MetadataRegistry } from '../../src/metadata/registry.js'
import type { MetadataProvider, RoleProvider } from '../../src/types/providers.js'

// --- Fixtures ---

const pgMain = { id: 'pg-main', engine: 'postgres' as const, host: 'localhost', port: 5432 }
const chAnalytics = { id: 'ch-analytics', engine: 'clickhouse' as const, host: 'localhost', port: 8123 }

const usersTable: TableMeta = {
  id: 'users',
  database: 'pg-main',
  physicalName: 'public.users',
  apiName: 'users',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'firstName', physicalName: 'first_name', type: 'string', nullable: false },
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
    { apiName: 'customerId', physicalName: 'customer_id', type: 'uuid', nullable: false },
    { apiName: 'total', physicalName: 'total_amount', type: 'decimal', nullable: false },
  ],
  relations: [{ column: 'customerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
}

const userSync = {
  sourceTable: 'users',
  targetDatabase: 'ch-analytics',
  targetPhysicalName: 'users_replicated',
  method: 'debezium' as const,
  estimatedLag: 'seconds' as const,
}

const validConfig = (): MetadataConfig => ({
  databases: [pgMain, chAnalytics],
  tables: [usersTable, ordersTable],
  caches: [],
  externalSyncs: [userSync],
})

const adminRole: RoleMeta = { id: 'admin', tables: '*' }
const roles = (): RoleMeta[] => [adminRole]

// --- Tests ---

describe('staticMetadata & staticRoles', () => {
  it('staticMetadata returns same config on load', async () => {
    const config = validConfig()
    const provider = staticMetadata(config)
    const loaded = await provider.load()
    expect(loaded).toBe(config)
  })

  it('staticRoles returns same roles on load', async () => {
    const r = roles()
    const provider = staticRoles(r)
    const loaded = await provider.load()
    expect(loaded).toBe(r)
  })
})

describe('MetadataRegistry.create', () => {
  it('loads and indexes metadata successfully', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))
    const snapshot = registry.getSnapshot()

    expect(snapshot.index.getTable('users')).toBeDefined()
    expect(snapshot.index.getTable('orders')).toBeDefined()
    expect(snapshot.roles).toHaveLength(1)
    expect(snapshot.config.databases).toHaveLength(2)
  })

  it('builds syncs-by-table index', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))
    const snapshot = registry.getSnapshot()

    const usersSyncs = snapshot.syncsByTable.get('users')
    expect(usersSyncs).toBeDefined()
    expect(usersSyncs).toHaveLength(1)
    expect(usersSyncs?.[0]?.targetDatabase).toBe('ch-analytics')
  })

  it('builds connectivity graph', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))
    const snapshot = registry.getSnapshot()

    expect(snapshot.connectivityGraph).toHaveLength(1)
    expect(snapshot.connectivityGraph[0]?.sourceDatabase).toBe('pg-main')
    expect(snapshot.connectivityGraph[0]?.targetDatabase).toBe('ch-analytics')
    expect(snapshot.connectivityGraph[0]?.method).toBe('debezium')
  })

  it('#54 throws ProviderError when metadata provider fails', async () => {
    const failingProvider: MetadataProvider = {
      load: () => Promise.reject(new Error('connection refused')),
    }

    try {
      await MetadataRegistry.create(failingProvider, staticRoles(roles()))
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeDefined()
      expect((err as { code: string }).code).toBe('METADATA_LOAD_FAILED')
      expect((err as { name: string }).name).toBe('ProviderError')
    }
  })

  it('#55 throws ProviderError when role provider fails', async () => {
    const failingProvider: RoleProvider = {
      load: () => Promise.reject(new Error('auth service down')),
    }

    try {
      await MetadataRegistry.create(staticMetadata(validConfig()), failingProvider)
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeDefined()
      expect((err as { code: string }).code).toBe('ROLE_LOAD_FAILED')
      expect((err as { name: string }).name).toBe('ProviderError')
    }
  })

  it('throws ConfigError on invalid config', async () => {
    const badConfig: MetadataConfig = {
      databases: [pgMain],
      tables: [
        {
          ...usersTable,
          apiName: '123-invalid',
        },
      ],
      caches: [],
      externalSyncs: [],
    }

    try {
      await MetadataRegistry.create(staticMetadata(badConfig), staticRoles(roles()))
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeDefined()
      expect((err as { name: string }).name).toBe('ConfigError')
    }
  })
})

describe('MetadataRegistry.reload', () => {
  it('#62 reloadRoles preserves old config on failure', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))

    const snapshotBefore = registry.getSnapshot()
    expect(snapshotBefore.roles).toHaveLength(1)

    const failingRoleProvider: RoleProvider = {
      load: () => Promise.reject(new Error('role service crashed')),
    }

    try {
      await registry.reloadRoles(failingRoleProvider)
      expect.fail('should have thrown')
    } catch {
      // Expected
    }

    // Old snapshot should be preserved
    const snapshotAfter = registry.getSnapshot()
    expect(snapshotAfter).toBe(snapshotBefore)
    expect(snapshotAfter.roles).toHaveLength(1)
  })

  it('reloadRoles updates roles on success', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))

    expect(registry.getSnapshot().roles).toHaveLength(1)

    const newRoles: RoleMeta[] = [
      { id: 'admin', tables: '*' },
      { id: 'viewer', tables: [{ tableId: 'users', allowedColumns: ['id', 'name'] }] },
    ]

    await registry.reloadRoles(staticRoles(newRoles))
    expect(registry.getSnapshot().roles).toHaveLength(2)
  })

  it('reloadMetadata rebuilds indexes', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))

    expect(registry.getSnapshot().index.getTable('users')).toBeDefined()

    // Reload with config that has different tables
    const newConfig = validConfig()
    newConfig.tables[0] = { ...usersTable, apiName: 'customers' }

    await registry.reloadMetadata(staticMetadata(newConfig))
    expect(registry.getSnapshot().index.getTable('users')).toBeUndefined()
    expect(registry.getSnapshot().index.getTable('customers')).toBeDefined()
  })

  it('reloadMetadata preserves old config on failure', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))

    const snapshotBefore = registry.getSnapshot()

    const failingProvider: MetadataProvider = {
      load: () => Promise.reject(new Error('storage unavailable')),
    }

    try {
      await registry.reloadMetadata(failingProvider)
      expect.fail('should have thrown')
    } catch {
      // Expected
    }

    // Old snapshot preserved
    expect(registry.getSnapshot()).toBe(snapshotBefore)
  })

  it('snapshot isolation — getSnapshot before reload returns old data', async () => {
    const registry = await MetadataRegistry.create(staticMetadata(validConfig()), staticRoles(roles()))

    const oldSnapshot = registry.getSnapshot()

    const newRoles: RoleMeta[] = [
      { id: 'admin', tables: '*' },
      { id: 'viewer', tables: [{ tableId: 'users', allowedColumns: ['id'] }] },
    ]
    await registry.reloadRoles(staticRoles(newRoles))

    // Old snapshot is still the same
    expect(oldSnapshot.roles).toHaveLength(1)
    // New snapshot is different
    expect(registry.getSnapshot().roles).toHaveLength(2)
    // They are different references
    expect(registry.getSnapshot()).not.toBe(oldSnapshot)
  })
})
