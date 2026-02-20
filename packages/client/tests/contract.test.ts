import type { DbExecutor, MetadataConfig, RoleMeta } from '@mkven/multi-db'
import { createMultiDb, staticMetadata, staticRoles } from '@mkven/multi-db'
import { describeQueryContract } from '../src/contract.js'

// ── Fixtures ───────────────────────────────────────────────────

const config: MetadataConfig = {
  databases: [{ id: 'pg-main', engine: 'postgres' }],
  tables: [
    {
      id: 'orders',
      apiName: 'orders',
      database: 'pg-main',
      physicalName: 'public.orders',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
        { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false },
      ],
      primaryKey: ['id'],
      relations: [],
    },
  ],
  caches: [],
  externalSyncs: [],
}

const roles: RoleMeta[] = [{ id: 'admin', tables: '*' }]

function mockExecutor(): DbExecutor {
  return {
    execute: async () => [
      { id: 1, total: 100 },
      { id: 2, total: 200 },
    ],
    ping: async () => {},
    close: async () => {},
  }
}

// ── Contract tests ─────────────────────────────────────────────

describeQueryContract('in-process (direct)', async () => {
  const db = await createMultiDb({
    metadataProvider: staticMetadata(config),
    roleProvider: staticRoles(roles),
    executors: { 'pg-main': mockExecutor() },
  })
  return db
})
