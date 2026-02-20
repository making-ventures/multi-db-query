import type { DbExecutor, MetadataConfig, RoleMeta } from '@mkven/multi-db'
import { createMultiDb, staticMetadata, staticRoles } from '@mkven/multi-db'
import { describeQueryContract } from '../src/contract/queryContract.js'

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
        { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
        { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false },
        { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
        { apiName: 'internalNote', physicalName: 'internal_note', type: 'string', nullable: true, maskingFn: 'full' },
      ],
      primaryKey: ['id'],
      relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
    },
    {
      id: 'users',
      apiName: 'users',
      database: 'pg-main',
      physicalName: 'public.users',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
        { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
      ],
      primaryKey: ['id'],
      relations: [],
    },
  ],
  caches: [],
  externalSyncs: [],
}

const roles: RoleMeta[] = [
  { id: 'admin', tables: '*' },
  {
    id: 'restricted',
    tables: [{ tableId: 'orders', allowedColumns: ['id', 'total', 'status'] }],
  },
]

function mockExecutor(): DbExecutor {
  return {
    execute: async () => [
      { id: 1, total: 100, status: 'active', user_id: 'u1' },
      { id: 2, total: 200, status: 'paid', user_id: 'u2' },
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
