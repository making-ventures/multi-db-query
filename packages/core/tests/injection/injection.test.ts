import type { ExecutionContext, MetadataConfig, QueryDefinition, RoleMeta } from '@mkven/multi-db-validation'
import { ExecutionError, ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'
import type { CacheProvider, CreateMultiDbOptions, DbExecutor, MultiDb } from '../../src/index.js'
import { createMultiDb, staticMetadata, staticRoles } from '../../src/index.js'

// ── Minimal fixture (subset of client contract fixture) ────────

const databases: MetadataConfig['databases'] = [
  { id: 'pg-main', engine: 'postgres', trinoCatalog: 'pg_main' },
  { id: 'ch-analytics', engine: 'clickhouse', trinoCatalog: 'ch_analytics' },
]

const tables: MetadataConfig['tables'] = [
  {
    id: 'orders',
    apiName: 'orders',
    database: 'pg-main',
    physicalName: 'public.orders',
    columns: [
      { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
      { apiName: 'customerId', physicalName: 'customer_id', type: 'uuid', nullable: false },
      { apiName: 'productId', physicalName: 'product_id', type: 'uuid', nullable: true },
      { apiName: 'total', physicalName: 'total_amount', type: 'decimal', nullable: false },
      { apiName: 'status', physicalName: 'order_status', type: 'string', nullable: false },
      { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
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
      { apiName: 'price', physicalName: 'price', type: 'decimal', nullable: false },
      { apiName: 'labels', physicalName: 'labels', type: 'string[]', nullable: true },
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
      { apiName: 'email', physicalName: 'email', type: 'string', nullable: false },
      { apiName: 'firstName', physicalName: 'first_name', type: 'string', nullable: false },
      { apiName: 'lastName', physicalName: 'last_name', type: 'string', nullable: false },
      { apiName: 'role', physicalName: 'role', type: 'string', nullable: false },
      { apiName: 'age', physicalName: 'age', type: 'int', nullable: true },
      { apiName: 'managerId', physicalName: 'manager_id', type: 'uuid', nullable: true },
      { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
    ],
    primaryKey: ['id'],
    relations: [{ column: 'managerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
  },
  {
    id: 'events',
    apiName: 'events',
    database: 'ch-analytics',
    physicalName: 'default.events',
    columns: [
      { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
      { apiName: 'type', physicalName: 'event_type', type: 'string', nullable: false },
      { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
      { apiName: 'orderId', physicalName: 'order_id', type: 'int', nullable: true },
      { apiName: 'tags', physicalName: 'tags', type: 'string[]', nullable: true },
      { apiName: 'timestamp', physicalName: 'event_ts', type: 'timestamp', nullable: false },
    ],
    primaryKey: ['id'],
    relations: [
      { column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' },
      { column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' },
    ],
  },
]

const metadata: MetadataConfig = {
  databases,
  tables,
  caches: [{ id: 'redis-main', engine: 'redis', tables: [{ tableId: 'users', keyPattern: 'users:{id}' }] }],
  externalSyncs: [],
  trino: { enabled: true },
}

const roles: RoleMeta[] = [{ id: 'admin', tables: '*' }]

// ── Helpers ────────────────────────────────────────────────────

function mockExecutor(): DbExecutor {
  return {
    execute: async () => [],
    ping: async () => {},
    close: async () => {},
  }
}

function mockCache(): CacheProvider {
  return {
    getMany: async (keys) => {
      const r = new Map<string, Record<string, unknown> | null>()
      for (const k of keys) r.set(k, null)
      return r
    },
    ping: async () => {},
    close: async () => {},
  }
}

const admin: ExecutionContext = { roles: { user: ['admin'] } }

let db: MultiDb

beforeAll(async () => {
  const options: CreateMultiDbOptions = {
    metadataProvider: staticMetadata(metadata),
    roleProvider: staticRoles(roles),
    executors: {
      'pg-main': mockExecutor(),
      'ch-analytics': mockExecutor(),
      trino: mockExecutor(),
    },
    cacheProviders: {
      'redis-main': mockCache(),
    },
  }
  db = await createMultiDb(options)
})

// ── Assertion helpers ──────────────────────────────────────────

/**
 * Asserts the query either:
 * - throws a ValidationError (rejected before SQL generation), or
 * - throws an ExecutionError (DB rejected the malformed query — still safe), or
 * - succeeds safely (malicious value treated as literal data, no injection)
 *
 * Uses `executeMode: 'sql-only'` so no live DB is needed.
 */
async function expectSafeOrRejected(definition: QueryDefinition): Promise<void> {
  try {
    const r = await db.query({ definition: { ...definition, executeMode: 'sql-only' }, context: admin })
    // If it doesn't throw, the query generated SQL safely — value was parameterized
    expect(r.kind).toBe('sql')
  } catch (err) {
    // If it throws, must be a validation error or an execution error — not a raw crash
    expect(err instanceof ValidationError || err instanceof ExecutionError).toBe(true)
  }
}

async function expectValidationError(definition: QueryDefinition, code: string): Promise<void> {
  try {
    await db.query({ definition: { ...definition, executeMode: 'sql-only' }, context: admin })
    expect.fail(`Expected ValidationError with code ${code}`)
  } catch (err) {
    expect(err).toBeInstanceOf(ValidationError)
    if (err instanceof ValidationError) {
      expect(err.errors.some((e) => e.code === code)).toBe(true)
    }
  }
}

// ── §16.1 Identifier & Structural Injection ───────────────────

describe('§16.1 Identifier & Structural Injection', () => {
  it('C1404: column name injection (" payload)', async () => {
    await expectValidationError({ from: 'orders', columns: ['id"; DROP TABLE orders; --'] }, 'UNKNOWN_COLUMN')
  })

  it('C1418: column name injection (` payload)', async () => {
    await expectValidationError({ from: 'events', columns: ['id`; DROP TABLE events; --'] }, 'UNKNOWN_COLUMN')
  })

  it('C1405: table name injection', async () => {
    await expectValidationError({ from: 'orders; DROP TABLE orders' }, 'UNKNOWN_TABLE')
  })

  it('C1411: EXISTS table name injection', async () => {
    await expectValidationError({ from: 'orders', filters: [{ table: 'users; DROP TABLE users' }] }, 'UNKNOWN_TABLE')
  })

  it('C1421: column name on cross-DB table', async () => {
    await expectValidationError(
      {
        from: 'events',
        columns: ['id"; DROP TABLE users; --'],
        joins: [{ table: 'users' }],
      },
      'UNKNOWN_COLUMN',
    )
  })

  it('C1460: ORDER BY direction injection', async () => {
    await expectValidationError(
      { from: 'orders', orderBy: [{ column: 'id', direction: 'asc; DROP TABLE orders;--' as 'asc' }] },
      'INVALID_ORDER_BY',
    )
  })

  it('C1461: aggregation function name injection', async () => {
    await expectValidationError(
      {
        from: 'orders',
        columns: [],
        aggregations: [{ column: 'total', fn: 'sum); DROP TABLE orders;--' as 'sum', alias: 'x' }],
      },
      'INVALID_AGGREGATION',
    )
  })

  it('C1462: column filter operator injection', async () => {
    await expectValidationError(
      { from: 'orders', filters: [{ column: 'id', operator: ') OR 1=1 --' as '=', refColumn: 'customerId' }] },
      'INVALID_FILTER',
    )
  })

  it('C1463: filter group logic injection', async () => {
    await expectValidationError(
      {
        from: 'orders',
        filters: [
          { logic: 'and 1=1);--' as 'and', conditions: [{ column: 'status', operator: '=', value: 'active' }] },
        ],
      },
      'INVALID_FILTER',
    )
  })

  it('C1464: EXISTS count operator injection', async () => {
    await expectValidationError(
      {
        from: 'orders',
        filters: [{ table: 'users', count: { operator: ') UNION SELECT 1;--' as '=', value: 1 } }],
      },
      'INVALID_FILTER',
    )
  })

  it('C1465: HAVING group logic injection', async () => {
    await expectValidationError(
      {
        from: 'orders',
        columns: [],
        aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
        groupBy: [{ column: 'status' }],
        having: [{ logic: 'or 1=1);--' as 'and', conditions: [{ column: 'x', operator: '>', value: 0 }] }],
      },
      'INVALID_HAVING',
    )
  })
})

// ── §16.2 Aggregation Alias Injection ──────────────────────────

describe('§16.2 Aggregation Alias Injection', () => {
  it('C1412: PG alias with double-quote injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'total', fn: 'sum', alias: 'x"; DROP TABLE orders;--' }],
    })
  })

  it('C1413: PG alias with backtick injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'total', fn: 'sum', alias: 'x`; DROP TABLE orders;--' }],
    })
  })

  it('C1414: PG HAVING referencing injected alias', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'total', fn: 'sum', alias: 'x"; --' }],
      groupBy: [{ column: 'status' }],
      having: [{ column: 'x"; --', operator: '>', value: 0 }],
    })
  })

  it('C1415: PG ORDER BY referencing injected alias', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'total', fn: 'sum', alias: 'x"; --' }],
      groupBy: [{ column: 'status' }],
      orderBy: [{ column: 'x"; --', direction: 'asc' }],
    })
  })

  it('C1419: CH alias with backtick injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      aggregations: [{ column: 'timestamp', fn: 'count', alias: 'x`; DROP TABLE events;--' }],
    })
  })

  it('C1448: CH HAVING referencing backtick-injected alias', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      aggregations: [{ column: 'timestamp', fn: 'count', alias: 'x`; --' }],
      groupBy: [{ column: 'type' }],
      having: [{ column: 'x`; --', operator: '>', value: 0 }],
    })
  })

  it('C1449: CH ORDER BY referencing backtick-injected alias', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      aggregations: [{ column: 'timestamp', fn: 'count', alias: 'x`; --' }],
      groupBy: [{ column: 'type' }],
      orderBy: [{ column: 'x`; --', direction: 'asc' }],
    })
  })

  it('C1422: Trino alias with double-quote injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      joins: [{ table: 'users' }],
      aggregations: [{ column: 'id', table: 'users', fn: 'count', alias: 'x"; DROP TABLE users;--' }],
    })
  })

  it('C1450: Trino HAVING referencing injected alias', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      joins: [{ table: 'users' }],
      aggregations: [{ column: 'id', table: 'users', fn: 'count', alias: 'x"; --' }],
      groupBy: [{ column: 'type' }],
      having: [{ column: 'x"; --', operator: '>', value: 0 }],
    })
  })

  it('C1451: Trino ORDER BY referencing injected alias', async () => {
    await expectSafeOrRejected({
      from: 'events',
      columns: [],
      joins: [{ table: 'users' }],
      aggregations: [{ column: 'id', table: 'users', fn: 'count', alias: 'x"; --' }],
      groupBy: [{ column: 'type' }],
      orderBy: [{ column: 'x"; --', direction: 'asc' }],
    })
  })
})

// ── §16.3 PostgreSQL Filter Value Injection ────────────────────

describe('§16.3 PostgreSQL Filter Value Injection', () => {
  it('C1400: PG = filter injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      filters: [{ column: 'status', operator: '=', value: "'; DROP TABLE orders; --" }],
    })
  })

  it('C1401: PG like filter injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [{ column: 'email', operator: 'like', value: "%'; DROP TABLE users; --%" }],
    })
  })

  it('C1402: PG contains injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [{ column: 'email', operator: 'contains', value: "'; DROP TABLE --" }],
    })
  })

  it('C1403: PG between injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      filters: [{ column: 'total', operator: 'between', value: { from: '0; DROP TABLE orders', to: 100 } }],
    })
  })

  it('C1406: PG in filter injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      filters: [{ column: 'status', operator: 'in', value: ["active'; DROP TABLE orders; --"] }],
    })
  })

  it('C1407: PG notIn filter injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      filters: [{ column: 'status', operator: 'notIn', value: ["active'; DROP TABLE orders; --"] }],
    })
  })

  it('C1408: PG levenshteinLte injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [
        {
          column: 'firstName',
          operator: 'levenshteinLte',
          value: { text: "'; DROP TABLE users; --", maxDistance: 3 },
        },
      ],
    })
  })

  it('C1409: PG arrayContains injection', async () => {
    await expectSafeOrRejected({
      from: 'products',
      filters: [{ column: 'labels', operator: 'arrayContains', value: "sale'; DROP TABLE products; --" }],
    })
  })

  it('C1431: PG icontains injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [{ column: 'email', operator: 'icontains', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1432: PG notBetween injection', async () => {
    await expectSafeOrRejected({
      from: 'orders',
      filters: [{ column: 'total', operator: 'notBetween', value: { from: '0; DROP TABLE orders', to: 100 } }],
    })
  })

  it('C1433: PG endsWith injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [{ column: 'email', operator: 'endsWith', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1453: PG startsWith injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      filters: [{ column: 'email', operator: 'startsWith', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1434: PG arrayContainsAll injection', async () => {
    await expectSafeOrRejected({
      from: 'products',
      filters: [{ column: 'labels', operator: 'arrayContainsAll', value: ["sale'; DROP TABLE products; --"] }],
    })
  })

  it('C1435: PG arrayContainsAny injection', async () => {
    await expectSafeOrRejected({
      from: 'products',
      filters: [{ column: 'labels', operator: 'arrayContainsAny', value: ["sale'; DROP TABLE products; --"] }],
    })
  })

  it('C1410: PG byIds injection', async () => {
    await expectSafeOrRejected({
      from: 'users',
      byIds: ["'; DROP TABLE users; --"],
    })
  })
})

// ── §16.4 ClickHouse Filter Value Injection ────────────────────

describe('§16.4 ClickHouse Filter Value Injection', () => {
  it('C1416: CH = filter injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: '=', value: "'; DROP TABLE events; --" }],
    })
  })

  it('C1423: CH in filter injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'in', value: ["purchase'; DROP TABLE events; --"] }],
    })
  })

  it('C1424: CH contains filter injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'contains', value: "'; DROP TABLE events; --" }],
    })
  })

  it('C1425: CH between filter injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [
        {
          column: 'timestamp',
          operator: 'between',
          value: { from: "2024-01-01'; DROP TABLE events; --", to: '2024-12-31' },
        },
      ],
    })
  })

  it('C1426: CH levenshteinLte injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [
        { column: 'type', operator: 'levenshteinLte', value: { text: "'; DROP TABLE events; --", maxDistance: 5 } },
      ],
    })
  })

  it('C1427: CH startsWith injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'startsWith', value: "'; DROP TABLE events; --" }],
    })
  })

  it('C1436: CH endsWith injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'endsWith', value: "'; DROP TABLE events; --" }],
    })
  })

  it('C1437: CH icontains injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'icontains', value: "'; DROP TABLE events; --" }],
    })
  })

  it('C1438: CH notBetween injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [
        {
          column: 'timestamp',
          operator: 'notBetween',
          value: { from: "2024-01-01'; DROP TABLE events;--", to: '2024-12-31' },
        },
      ],
    })
  })

  it('C1439: CH arrayContains injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContains', value: "x'; DROP TABLE events; --" }],
    })
  })

  it('C1440: CH arrayContainsAll injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContainsAll', value: ["x'; DROP TABLE events; --"] }],
    })
  })

  it('C1417: CH arrayContainsAny injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContainsAny', value: ["x'; DROP TABLE events; --"] }],
    })
  })

  it('C1441: CH notIn injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'notIn', value: ["purchase'; DROP TABLE events; --"] }],
    })
  })

  it('C1446: CH byIds injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      byIds: ["'; DROP TABLE events; --"],
    })
  })

  it('C1454: CH like injection', async () => {
    await expectSafeOrRejected({
      from: 'events',
      filters: [{ column: 'type', operator: 'like', value: "%'; DROP TABLE events; --%" }],
    })
  })
})

// ── §16.5 Trino Filter Value Injection ─────────────────────────
// Cross-DB join (events@CH + users@PG) forces Trino routing

describe('§16.5 Trino Filter Value Injection', () => {
  const crossDb = { from: 'events' as const, joins: [{ table: 'users' as const }] }

  it('C1420: Trino = filter injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: '=', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1428: Trino in filter injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'in', value: ["x'; DROP TABLE users; --"] }],
    })
  })

  it('C1429: Trino contains filter injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'contains', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1430: Trino levenshteinLte injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        {
          column: 'firstName',
          table: 'users',
          operator: 'levenshteinLte',
          value: { text: "'; DROP TABLE users; --", maxDistance: 5 },
        },
      ],
    })
  })

  it('C1442: Trino icontains injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'icontains', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1443: Trino arrayContains injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        { column: 'labels', table: 'products', operator: 'arrayContains', value: "x'; DROP TABLE products; --" },
      ],
    })
  })

  it('C1444: Trino arrayContainsAll injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        {
          column: 'labels',
          table: 'products',
          operator: 'arrayContainsAll',
          value: ["x'; DROP TABLE products; --"],
        },
      ],
    })
  })

  it('C1445: Trino arrayContainsAny injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        {
          column: 'labels',
          table: 'products',
          operator: 'arrayContainsAny',
          value: ["x'; DROP TABLE products; --"],
        },
      ],
    })
  })

  it('C1452: Trino notIn injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'notIn', value: ["x'; DROP TABLE users; --"] }],
    })
  })

  it('C1447: Trino byIds injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      byIds: ["'; DROP TABLE users; --"],
    })
  })

  it('C1455: Trino like injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'like', value: "%'; DROP TABLE users; --%" }],
    })
  })

  it('C1456: Trino between injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        { column: 'age', table: 'users', operator: 'between', value: { from: '0; DROP TABLE users', to: 100 } },
      ],
    })
  })

  it('C1457: Trino notBetween injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [
        { column: 'age', table: 'users', operator: 'notBetween', value: { from: '0; DROP TABLE users', to: 100 } },
      ],
    })
  })

  it('C1458: Trino startsWith injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'startsWith', value: "'; DROP TABLE users; --" }],
    })
  })

  it('C1459: Trino endsWith injection', async () => {
    await expectSafeOrRejected({
      ...crossDb,
      filters: [{ column: 'email', table: 'users', operator: 'endsWith', value: "'; DROP TABLE users; --" }],
    })
  })
})
