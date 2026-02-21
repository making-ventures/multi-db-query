import type { ExecutionContext, MetadataConfig, QueryDefinition, RoleMeta, TableMeta } from '@mkven/multi-db-validation'
import { MetadataIndex } from '@mkven/multi-db-validation'
import { describe, expect, it } from 'vitest'
import { resolveNames } from '../../src/resolution/resolver.js'

// --- Fixtures ---

const pgMain = { id: 'pg-main', engine: 'postgres' as const }

const usersTable: TableMeta = {
  id: 'users',
  database: 'pg-main',
  physicalName: 'public.users',
  apiName: 'users',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'firstName', physicalName: 'first_name', type: 'string', nullable: false, maskingFn: 'name' as const },
    { apiName: 'email', physicalName: 'email', type: 'string', nullable: false, maskingFn: 'email' as const },
    { apiName: 'age', physicalName: 'age', type: 'int', nullable: true },
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
    { apiName: 'status', physicalName: 'order_status', type: 'string', nullable: false },
  ],
  relations: [{ column: 'customerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const }],
}

const eventsTable: TableMeta = {
  id: 'events',
  database: 'pg-main',
  physicalName: 'public.events',
  apiName: 'events',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'type', physicalName: 'event_type', type: 'string', nullable: false },
    { apiName: 'tags', physicalName: 'tags', type: 'string[]', nullable: true },
  ],
  relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const }],
}

const config: MetadataConfig = {
  databases: [pgMain],
  tables: [usersTable, ordersTable, eventsTable],
  caches: [],
  externalSyncs: [],
}

const adminRole: RoleMeta = { id: 'admin', tables: '*' }
const roles: RoleMeta[] = [adminRole]
const rolesById = new Map([['admin', adminRole]])
const index = new MetadataIndex(config, roles)
const adminCtx: ExecutionContext = { roles: { user: ['admin'] } }

// --- Tests ---

describe('Name Resolution — basic', () => {
  it('simple select all columns', () => {
    const q: QueryDefinition = { from: 'users' }
    const result = resolveNames(q, adminCtx, index, rolesById)

    expect(result.mode).toBe('data')
    expect(result.parts.from.physicalName).toBe('public.users')
    expect(result.parts.from.alias).toBe('t0')
    expect(result.parts.select).toHaveLength(4) // id, firstName, email, age
    expect(result.columnMappings).toHaveLength(4)
  })

  it('explicit columns', () => {
    const q: QueryDefinition = { from: 'users', columns: ['id', 'firstName'] }
    const result = resolveNames(q, adminCtx, index, rolesById)

    expect(result.parts.select).toHaveLength(2)
    expect(result.columnMappings[0]?.apiName).toBe('id')
    expect(result.columnMappings[0]?.physicalName).toBe('id')
    expect(result.columnMappings[1]?.apiName).toBe('firstName')
  })

  it('count mode', () => {
    const q: QueryDefinition = { from: 'users', executeMode: 'count' }
    const result = resolveNames(q, adminCtx, index, rolesById)

    expect(result.mode).toBe('count')
    expect(result.parts.select).toHaveLength(0)
    expect(result.columnMappings).toHaveLength(0)
  })

  it('count mode suppresses groupBy, having, limit, offset, distinct', () => {
    const q: QueryDefinition = {
      from: 'orders',
      executeMode: 'count',
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'cnt', operator: '>', value: 5 }],
      limit: 10,
      offset: 20,
      distinct: true,
    }
    const result = resolveNames(q, adminCtx, index, rolesById)

    expect(result.mode).toBe('count')
    expect(result.parts.countMode).toBe(true)
    expect(result.parts.groupBy).toHaveLength(0)
    expect(result.parts.having).toBeUndefined()
    expect(result.parts.aggregations).toHaveLength(0)
    expect(result.parts.orderBy).toHaveLength(0)
    expect(result.parts.limit).toBeUndefined()
    expect(result.parts.offset).toBeUndefined()
    expect(result.parts.distinct).toBeUndefined()
  })

  it('distinct', () => {
    const q: QueryDefinition = { from: 'users', distinct: true }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.distinct).toBe(true)
  })

  it('limit and offset', () => {
    const q: QueryDefinition = { from: 'users', limit: 10, offset: 20 }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.limit).toBe(10)
    expect(result.parts.offset).toBe(20)
  })
})

describe('Name Resolution — table aliasing', () => {
  it('from table gets t0', () => {
    const q: QueryDefinition = { from: 'users' }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.from.alias).toBe('t0')
  })

  it('joined table gets t1', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.joins).toHaveLength(1)
    expect(result.parts.joins[0]?.table.alias).toBe('t1')
  })
})

describe('Name Resolution — joins', () => {
  it('resolves join with FK from→join', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    const join = result.parts.joins[0]
    expect(join).toBeDefined()
    expect(join?.type).toBe('left')
    expect(join?.leftColumn.columnName).toBe('customer_id') // orders.customerId
    expect(join?.rightColumn.columnName).toBe('id') // users.id
  })

  it('left join type', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users', type: 'left' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.joins[0]?.type).toBe('left')
  })

  it('join columns get qualified apiName only on collision', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users', columns: ['firstName'] }],
      columns: ['id', 'total'],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // firstName doesn't collide — stays bare
    const joinMapping = result.columnMappings.find((m) => m.apiName === 'firstName')
    expect(joinMapping).toBeDefined()
    expect(joinMapping?.physicalName).toBe('first_name')
  })

  it('colliding apiNames get qualified on both sides', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users', columns: ['id', 'firstName'] }],
      columns: ['id', 'total'],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // 'id' collides between orders and users → both qualified
    const ordersId = result.columnMappings.find((m) => m.apiName === 'orders.id')
    const usersId = result.columnMappings.find((m) => m.apiName === 'users.id')
    expect(ordersId).toBeDefined()
    expect(usersId).toBeDefined()
    // 'total' and 'firstName' don't collide → bare
    expect(result.columnMappings.find((m) => m.apiName === 'total')).toBeDefined()
    expect(result.columnMappings.find((m) => m.apiName === 'firstName')).toBeDefined()
  })

  it('join with empty columns — join for filter only', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users', columns: [] }],
      columns: ['id'],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // Join exists but no columns from it
    expect(result.parts.joins).toHaveLength(1)
    expect(result.columnMappings).toHaveLength(1) // Only orders.id
  })

  it('join with columns: undefined — all allowed columns', () => {
    const q: QueryDefinition = {
      from: 'orders',
      joins: [{ table: 'users' }],
      columns: ['id'],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // undefined = all allowed columns from joined table (4 for admin: id, firstName, email, age)
    expect(result.parts.joins).toHaveLength(1)
    // 1 (orders.id) + 4 (users: id, firstName, email, age) = 5 mappings
    expect(result.columnMappings).toHaveLength(5)
    // Only 'id' collides — both qualified as orders.id and users.id
    expect(result.columnMappings.find((m) => m.apiName === 'orders.id')).toBeDefined()
    expect(result.columnMappings.find((m) => m.apiName === 'users.id')).toBeDefined()
    // Non-colliding user columns stay bare
    expect(result.columnMappings.find((m) => m.apiName === 'firstName')).toBeDefined()
    expect(result.columnMappings.find((m) => m.apiName === 'email')).toBeDefined()
    expect(result.columnMappings.find((m) => m.apiName === 'age')).toBeDefined()
  })
})

describe('Name Resolution — join filters', () => {
  it('#69 join-scoped filter resolves against joined table', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['id', 'total'],
      joins: [
        {
          table: 'users',
          columns: ['firstName'],
          filters: [{ column: 'age', operator: '>', value: 18 }],
        },
      ],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // Filter on users.age should produce WHERE condition using t1."age"
    expect(result.parts.where).toBeDefined()
    expect(result.params).toContain(18)
  })

  it('#147 multi-join with per-table filters', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['id'],
      joins: [
        { table: 'users', columns: [], filters: [{ column: 'age', operator: '>=', value: 21 }] },
        { table: 'events', columns: [], filters: [{ column: 'type', operator: '=', value: 'login' }] },
      ],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.where).toBeDefined()
    expect(result.params).toContain(21)
    expect(result.params).toContain('login')
  })
})

describe('Name Resolution — filters', () => {
  it('simple equality filter', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'firstName', operator: '=', value: 'Alice' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.where).toBeDefined()
    expect(result.params).toEqual(['Alice'])
  })

  it('isNull produces no param', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'isNull' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toHaveLength(0)
  })

  it('between produces two params', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'between', value: { from: 18, to: 65 } }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toEqual([18, 65])
  })

  it('levenshteinLte produces two params', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'firstName', operator: 'levenshteinLte', value: { text: 'test', maxDistance: 2 } }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toEqual(['test', 2])
  })

  it('filter group', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [
        {
          logic: 'or',
          conditions: [
            { column: 'firstName', operator: '=', value: 'Alice' },
            { column: 'firstName', operator: '=', value: 'Bob' },
          ],
        },
      ],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toEqual(['Alice', 'Bob'])
    const where = result.parts.where
    expect(where).toBeDefined()
    if (where !== undefined && 'logic' in where) {
      expect(where.logic).toBe('or')
    }
  })

  it('QueryColumnFilter', () => {
    const q: QueryDefinition = {
      from: 'orders',
      filters: [{ column: 'total', operator: '>', refColumn: 'total' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toHaveLength(0)
    const where = result.parts.where
    expect(where).toBeDefined()
    if (where !== undefined && 'leftColumn' in where) {
      expect(where.leftColumn.columnName).toBe('total_amount')
      expect(where.rightColumn.columnName).toBe('total_amount')
    }
  })

  it('EXISTS filter', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'orders', exists: true }],
    }
    // Need reverse relation: either users→orders or orders→users
    // orders has FK to users, so this should work
    const result = resolveNames(q, adminCtx, index, rolesById)
    const where = result.parts.where
    expect(where).toBeDefined()
    if (where !== undefined && 'exists' in where) {
      expect(where.exists).toBe(true)
    }
  })

  it('EXISTS filter (implicit exists: true)', () => {
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'orders' }],
    }
    // { table: 'orders' } without explicit exists — defaults to exists: true
    const result = resolveNames(q, adminCtx, index, rolesById)
    const where = result.parts.where
    expect(where).toBeDefined()
    if (where !== undefined && 'exists' in where) {
      expect(where.exists).toBe(true)
    }
  })

  it('array operator', () => {
    const q: QueryDefinition = {
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContains', value: 'test' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toEqual(['test'])
  })
})

describe('Name Resolution — byIds', () => {
  it('byIds produces in filter on PK', () => {
    const q: QueryDefinition = { from: 'users', byIds: ['id1', 'id2'] }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.params).toEqual([['id1', 'id2']])
    const where = result.parts.where
    expect(where).toBeDefined()
    if (where !== undefined && 'operator' in where) {
      expect(where.operator).toBe('in')
    }
  })
})

describe('Name Resolution — groupBy & aggregations', () => {
  it('groupBy resolves to physical names', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.groupBy).toHaveLength(1)
    expect(result.parts.groupBy[0]?.columnName).toBe('order_status')
    expect(result.parts.aggregations).toHaveLength(1)
    expect(result.parts.aggregations[0]?.alias).toBe('cnt')
  })

  it('aggregation-only: columns: [] plus aggregation', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.select).toHaveLength(0)
    expect(result.parts.aggregations).toHaveLength(1)
  })

  it('columns: undefined with aggregations defaults to groupBy columns', () => {
    const q: QueryDefinition = {
      from: 'orders',
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // Should include status but not all columns
    expect(result.parts.select).toHaveLength(1)
    expect(result.columnMappings[0]?.apiName).toBe('status')
  })

  it('columns: undefined with aggregations but no groupBy — zero select columns', () => {
    const q: QueryDefinition = {
      from: 'orders',
      aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    // No groupBy → zero regular columns, only the aggregation
    expect(result.parts.select).toHaveLength(0)
    expect(result.parts.aggregations).toHaveLength(1)
  })
})

describe('Name Resolution — having', () => {
  it('having resolves to alias-based condition', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'cnt', operator: '>', value: 5 }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.having).toBeDefined()
    expect(result.params).toContain(5)
  })
})

describe('Name Resolution — orderBy', () => {
  it('orderBy resolves to physical name', () => {
    const q: QueryDefinition = {
      from: 'users',
      orderBy: [{ column: 'firstName', direction: 'asc' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    expect(result.parts.orderBy).toHaveLength(1)
    const ob = result.parts.orderBy[0]
    expect(ob?.direction).toBe('asc')
  })

  it('orderBy by aggregation alias', () => {
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      orderBy: [{ column: 'cnt', direction: 'desc' }],
    }
    const result = resolveNames(q, adminCtx, index, rolesById)
    const ob = result.parts.orderBy[0]
    // Should be a string alias, not a ColumnRef
    expect(ob?.column).toBe('cnt')
    expect(ob?.direction).toBe('desc')
  })
})

describe('Name Resolution — column mapping', () => {
  it('masked column has masked flag', () => {
    const maskedRole: RoleMeta = {
      id: 'viewer',
      tables: [{ tableId: 'users', allowedColumns: ['id', 'email'], maskedColumns: ['email'] }],
    }
    const viewerRolesById = new Map([['viewer', maskedRole]])
    const viewerIndex = new MetadataIndex(config, [maskedRole])
    const ctx: ExecutionContext = { roles: { user: ['viewer'] } }

    const q: QueryDefinition = { from: 'users', columns: ['id', 'email'] }
    const result = resolveNames(q, ctx, viewerIndex, viewerRolesById)

    const emailMapping = result.columnMappings.find((m) => m.apiName === 'email')
    expect(emailMapping?.masked).toBe(true)
    expect(emailMapping?.maskingFn).toBe('email')

    const idMapping = result.columnMappings.find((m) => m.apiName === 'id')
    expect(idMapping?.masked).toBe(false)
  })

  it('maskingFn uses effective access default (full)', () => {
    // Column 'age' has no maskingFn in metadata, but role masks it → should default to 'full'
    const maskedRole: RoleMeta = {
      id: 'viewer',
      tables: [{ tableId: 'users', allowedColumns: ['id', 'age'], maskedColumns: ['age'] }],
    }
    const viewerRolesById = new Map([['viewer', maskedRole]])
    const viewerIndex = new MetadataIndex(config, [maskedRole])
    const ctx: ExecutionContext = { roles: { user: ['viewer'] } }

    const q: QueryDefinition = { from: 'users', columns: ['id', 'age'] }
    const result = resolveNames(q, ctx, viewerIndex, viewerRolesById)

    const ageMapping = result.columnMappings.find((m) => m.apiName === 'age')
    expect(ageMapping?.masked).toBe(true)
    // age has no maskingFn in metadata → effective access defaults to 'full'
    expect(ageMapping?.maskingFn).toBe('full')
  })
})
