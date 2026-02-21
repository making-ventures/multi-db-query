import type { DbExecutor, MetadataConfig, RoleMeta } from '@mkven/multi-db-query'
import {
  createMultiDb,
  MetadataIndex,
  staticMetadata,
  staticRoles,
  validateConfig,
  validateQuery,
} from '@mkven/multi-db-query'
import type { ValidateResult } from '../../src/client.js'
import { describeQueryContract } from '../../src/contract/queryContract.js'
import { describeValidationContract } from '../../src/contract/validationContract.js'

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
        { apiName: 'customerId', physicalName: 'customer_id', type: 'uuid', nullable: false },
        { apiName: 'productId', physicalName: 'product_id', type: 'uuid', nullable: true },
        { apiName: 'total', physicalName: 'total_amount', type: 'decimal', nullable: false, maskingFn: 'number' },
        { apiName: 'status', physicalName: 'order_status', type: 'string', nullable: false },
        { apiName: 'internalNote', physicalName: 'internal_note', type: 'string', nullable: true, maskingFn: 'full' },
      ],
      primaryKey: ['id'],
      relations: [
        { column: 'customerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' },
        { column: 'productId', references: { table: 'products', column: 'id' }, type: 'many-to-one' },
      ],
    },
    {
      id: 'products',
      apiName: 'products',
      database: 'pg-main',
      physicalName: 'public.products',
      columns: [
        { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
        { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
        { apiName: 'category', physicalName: 'category', type: 'string', nullable: false },
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
        { apiName: 'firstName', physicalName: 'first_name', type: 'string', nullable: false },
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
    id: 'tenant-user',
    tables: [{ tableId: 'orders', allowedColumns: ['id', 'total', 'status'], maskedColumns: ['total'] }],
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

// ── Validation contract (in-process, zero I/O) ────────────────

describeValidationContract(
  'in-process (direct)',
  async () => {
    const index = new MetadataIndex(config, roles)
    return {
      async validateQuery(input) {
        const err = validateQuery(input.definition, input.context, index, roles)
        if (err !== null) throw err
        return { valid: true } satisfies ValidateResult
      },
      async validateConfig(input) {
        const err = validateConfig(input.metadata)
        if (err !== null) throw err
        return { valid: true } satisfies ValidateResult
      },
    }
  },
  config,
  roles,
)
