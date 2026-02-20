import { describe, expect, it } from 'vitest'
import { MetadataIndex } from '../src/metadataIndex.js'
import type { ExecutionContext } from '../src/types/context.js'
import type { MetadataConfig, RoleMeta } from '../src/types/metadata.js'
import type { QueryDefinition } from '../src/types/query.js'
import { validateQuery } from '../src/validation/queryValidator.js'
import { eventsTable, ordersTable, usersTable, validConfig } from './fixtures/testConfig.js'

// --- Helpers ---

const adminRole: RoleMeta = {
  id: 'admin',
  tables: '*',
}

const viewerRole: RoleMeta = {
  id: 'viewer',
  tables: [
    { tableId: 'users', allowedColumns: ['id', 'name', 'email', 'age', 'createdAt'] },
    { tableId: 'orders', allowedColumns: ['id', 'userId', 'amount', 'status', 'createdAt'] },
    { tableId: 'events', allowedColumns: ['id', 'userId', 'eventType', 'payload', 'createdAt', 'tags'] },
  ],
}

const restrictedRole: RoleMeta = {
  id: 'restricted',
  tables: [{ tableId: 'users', allowedColumns: ['id', 'name'] }],
}

const allRoles: RoleMeta[] = [adminRole, viewerRole, restrictedRole]

const adminCtx: ExecutionContext = { roles: { user: ['admin'] } }
const restrictedCtx: ExecutionContext = { roles: { user: ['restricted'] } }

function buildIndex(config?: MetadataConfig, roles?: RoleMeta[]): MetadataIndex {
  return new MetadataIndex(config ?? validConfig(), roles ?? allRoles)
}

// --- Rule 1: Table existence ---

describe('Rule 1 — Table existence', () => {
  it('#15 unknown from table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'nonExistent' }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_TABLE')).toBe(true)
  })

  it('valid from table returns null', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users' }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 2: Column existence ---

describe('Rule 2 — Column existence', () => {
  it('#17 unknown column in columns', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', columns: ['id', 'nonExistent'] }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_COLUMN' && e.details.column === 'nonExistent')).toBe(true)
  })

  it('#18 unknown column in filter', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'badCol', operator: '=', value: 1 }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_COLUMN')).toBe(true)
  })
})

// --- Rule 3: Role permission (table-level) ---

describe('Rule 3 — Role permission', () => {
  it('#32 access denied for table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'orders' }
    const err = validateQuery(q, restrictedCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'ACCESS_DENIED' && e.details.table === 'orders')).toBe(true)
  })

  it('#34 admin has access', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'orders' }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 4: Column permission ---

describe('Rule 4 — Column permission', () => {
  it('#36 access denied for column', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', columns: ['id', 'email'] }
    const err = validateQuery(q, restrictedCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'ACCESS_DENIED' && e.details.column === 'email')).toBe(true)
  })
})

// --- Rule 5: Filter validity ---

describe('Rule 5 — Filter validity', () => {
  it('#37 invalid operator for type', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'like', value: '%test%' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER')).toBe(true)
  })

  it('#40 isNull on non-nullable column', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'isNull' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER' && e.details.operator === 'isNull')).toBe(true)
  })

  it('#41 isNull on nullable column is valid', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'isNull' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('#42 between with missing to', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'between', value: { from: 1 } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
  })

  it('#43 in with empty array', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'in', value: [] }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
  })

  it('#116 in with null element', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'in', value: ['a', null, 'b'] }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE' && e.message.includes('null'))).toBe(true)
  })

  it('#117 in element type mismatch', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'in', value: ['a', 'b'] }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
  })

  it('#118 between null bounds', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'between', value: { from: null, to: 10 } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE' && e.message.includes('from'))).toBe(true)
  })

  it('#119 between value type mismatch', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      filters: [{ column: 'amount', operator: 'between', value: { from: 'abc', to: 'xyz' } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE' && e.details.expected === 'decimal')).toBe(true)
  })

  it('#120 levenshteinLte negative maxDistance', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'levenshteinLte', value: { text: 'test', maxDistance: -1 } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
  })

  it('#139 comparison on uuid rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'id', operator: '>', value: 'abc' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER')).toBe(true)
  })

  it('#140 scalar operator on array column rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      filters: [{ column: 'tags', operator: '=', value: 'test' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER')).toBe(true)
  })

  it('#141 array operator on scalar column rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'arrayContains', value: 'test' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER')).toBe(true)
  })

  it('#143 arrayContains valid on array column', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContains', value: 'test' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('#145 filter.table references non-joined table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'status', table: 'orders', operator: '=', value: 'active' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER' && e.details.table === 'orders')).toBe(true)
  })

  it('#146 arrayContains wrong element type', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      filters: [{ column: 'tags', operator: 'arrayContains', value: 123 }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_VALUE')).toBe(true)
  })

  it('valid between filter', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'age', operator: 'between', value: { from: 18, to: 65 } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('valid in filter', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'name', operator: 'in', value: ['Alice', 'Bob'] }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 5: QueryColumnFilter ---

describe('Rule 5 — QueryColumnFilter', () => {
  it('#150 refColumn does not exist', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      joins: [{ table: 'orders' }],
      filters: [{ column: 'id', operator: '=', refColumn: 'nonExistent', refTable: 'orders' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_COLUMN' && e.details.refColumn === 'nonExistent')).toBe(true)
  })

  it('#151 QueryColumnFilter with array column rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      filters: [{ column: 'tags', operator: '=', refColumn: 'eventType' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER' && e.message.includes('array'))).toBe(true)
  })

  it('#153 incompatible types in QueryColumnFilter', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ column: 'id', operator: '=', refColumn: 'age' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_FILTER' && e.message.includes('Incompatible'))).toBe(true)
  })

  it('compatible orderable types are allowed', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      filters: [{ column: 'amount', operator: '>', refColumn: 'amount' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 5: Filter groups (recursive) ---

describe('Rule 5 — Filter groups', () => {
  it('#154 recursive filter group validation', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [
        {
          logic: 'or',
          conditions: [
            { column: 'name', operator: '=', value: 'Alice' },
            { column: 'nonExistent', operator: '=', value: 'test' },
          ],
        },
      ],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_COLUMN')).toBe(true)
  })
})

// --- Rule 6: Join validity ---

describe('Rule 6 — Join validity', () => {
  it('#46 join with no relation', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      joins: [{ table: 'metrics' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_JOIN')).toBe(true)
  })

  it('#47 valid join', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      joins: [{ table: 'orders' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('join with unknown table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      joins: [{ table: 'nonExistent' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_TABLE' && e.details.table === 'nonExistent')).toBe(true)
  })
})

// --- Rule 7: Group By validity ---

describe('Rule 7 — Group By validity', () => {
  it('#65 ungrouped column in columns', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status', 'userId'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmount' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_GROUP_BY' && e.details.column === 'userId')).toBe(true)
  })

  it('#167 array column in groupBy rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      columns: ['tags'],
      groupBy: [{ column: 'tags' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_GROUP_BY' && e.message.includes('Array'))).toBe(true)
  })

  it('#168 groupBy table qualifier references non-joined table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      columns: ['name'],
      groupBy: [{ column: 'status', table: 'orders' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_GROUP_BY')).toBe(true)
  })

  it('valid groupBy', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 8: Having validity ---

describe('Rule 8 — Having validity', () => {
  it('#78 having with non-alias column', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'status', operator: '=', value: 'active' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_HAVING')).toBe(true)
  })

  it('#169 having with table qualifier rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'cnt', table: 'orders', operator: '>', value: 5 }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_HAVING' && e.message.includes('table'))).toBe(true)
  })

  it('#173 pattern operator in HAVING rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'cnt', operator: 'like', value: '%5%' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_HAVING' && e.details.operator === 'like')).toBe(true)
  })

  it('#174 QueryColumnFilter in HAVING group rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [
        {
          logic: 'and',
          conditions: [{ column: 'cnt', operator: '=', refColumn: 'cnt' }],
        },
      ],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_HAVING' && e.message.includes('QueryColumnFilter'))).toBe(true)
  })

  it('#175 QueryExistsFilter in HAVING group rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [
        {
          logic: 'and',
          conditions: [{ table: 'users', exists: true }],
        },
      ],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_HAVING' && e.message.includes('QueryExistsFilter'))).toBe(true)
  })

  it('valid having', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      having: [{ column: 'cnt', operator: '>', value: 5 }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 9: Order By validity ---

describe('Rule 9 — Order By validity', () => {
  it('#82 orderBy unknown column', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      orderBy: [{ column: 'nonExistent', direction: 'asc' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_ORDER_BY')).toBe(true)
  })

  it('#176 orderBy array column rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      orderBy: [{ column: 'tags', direction: 'asc' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_ORDER_BY' && e.message.includes('Array'))).toBe(true)
  })

  it('#177 orderBy with agg alias is valid', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
      orderBy: [{ column: 'cnt', direction: 'desc' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('#178 orderBy.table references non-joined table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      orderBy: [{ column: 'status', table: 'orders', direction: 'asc' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_ORDER_BY')).toBe(true)
  })
})

// --- Rule 10: ByIds validity ---

describe('Rule 10 — ByIds validity', () => {
  it('#86 byIds empty array', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', byIds: [] }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_BY_IDS')).toBe(true)
  })

  it('#87 byIds with groupBy', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      byIds: ['id1'],
      groupBy: [{ column: 'name' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_BY_IDS' && e.message.includes('groupBy'))).toBe(true)
  })

  it('#88 byIds with aggregations', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      byIds: ['id1'],
      aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_BY_IDS' && e.message.includes('aggregation'))).toBe(true)
  })

  it('valid byIds', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', byIds: ['id1', 'id2'] }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 11: Limit/Offset validity ---

describe('Rule 11 — Limit/Offset validity', () => {
  it('#97 negative limit', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', limit: -1 }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_LIMIT')).toBe(true)
  })

  it('#98 offset without limit', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', offset: 10 }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_LIMIT' && e.message.includes('offset requires limit'))).toBe(
      true,
    )
  })

  it('valid limit + offset', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'users', limit: 10, offset: 20 }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Rule 12: Exists filter validity ---

describe('Rule 12 — Exists filter validity', () => {
  it('#107 EXISTS with no relation', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'metrics', exists: true }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_EXISTS')).toBe(true)
  })

  it('#109 valid EXISTS', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'orders', exists: true }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('#179 EXISTS count with negative value', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'orders', count: { operator: '>', value: -1 } }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_EXISTS')).toBe(true)
  })

  it('#180 EXISTS with unknown table', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'nonExistent', exists: true }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_TABLE')).toBe(true)
  })

  it('#187 EXISTS with denied access', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      filters: [{ table: 'orders', exists: true }],
    }
    const err = validateQuery(q, restrictedCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'ACCESS_DENIED')).toBe(true)
  })
})

// --- Rule 13: Role existence ---

describe('Rule 13 — Role existence', () => {
  it('#190 unknown role ID', () => {
    const idx = buildIndex()
    const ctx: ExecutionContext = { roles: { user: ['nonExistentRole'] } }
    const q: QueryDefinition = { from: 'users' }
    const err = validateQuery(q, ctx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'UNKNOWN_ROLE' && e.details.role === 'nonExistentRole')).toBe(true)
  })
})

// --- Rule 14: Aggregation validity ---

describe('Rule 14 — Aggregation validity', () => {
  it('#191 duplicate aggregation alias', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [
        { column: '*', fn: 'count', alias: 'cnt' },
        { column: 'amount', fn: 'sum', alias: 'cnt' },
      ],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_AGGREGATION' && e.message.includes('Duplicate'))).toBe(true)
  })

  it('#192 alias collides with column name', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: ['status'],
      groupBy: [{ column: 'status' }],
      aggregations: [{ column: '*', fn: 'count', alias: 'status' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_AGGREGATION' && e.message.includes('collides'))).toBe(true)
  })

  it('#195 empty columns without aggregations', () => {
    const idx = buildIndex()
    const q: QueryDefinition = { from: 'orders', columns: [] }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_AGGREGATION' && e.message.includes('Empty columns'))).toBe(true)
  })

  it('#198 aggregation table qualifier non-joined', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      columns: [],
      aggregations: [{ column: 'amount', table: 'orders', fn: 'sum', alias: 'total' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_AGGREGATION' && e.details.table === 'orders')).toBe(true)
  })

  it('#199 sum/avg on array column rejected', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      columns: [],
      aggregations: [{ column: 'tags', fn: 'sum', alias: 'tagSum' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_AGGREGATION' && e.message.includes('array'))).toBe(true)
  })

  it('count on array column is valid', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'events',
      columns: [],
      aggregations: [{ column: 'tags', fn: 'count', alias: 'tagCount' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('valid aggregation-only query', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'orders',
      columns: [],
      aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmount' }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})

// --- Multiple errors collected ---

describe('Multiple errors collected', () => {
  it('#229-232 collects multiple validation errors', () => {
    const idx = buildIndex()
    const q: QueryDefinition = {
      from: 'users',
      columns: ['id', 'badCol1', 'badCol2'],
      filters: [{ column: 'badFilter', operator: '=', value: 1 }],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).not.toBeNull()
    // Should have collected all errors, not just one
    expect(err?.errors.length).toBeGreaterThanOrEqual(3)
    expect(err?.fromTable).toBe('users')
  })
})

// --- Edge cases ---

describe('Edge cases', () => {
  it('query with no roles context — no restrictions', () => {
    const idx = buildIndex()
    const ctx: ExecutionContext = { roles: {} }
    const q: QueryDefinition = { from: 'users', columns: ['id', 'name', 'email'] }
    const err = validateQuery(q, ctx, idx, allRoles)
    expect(err).toBeNull()
  })

  it('#234-235 nested EXISTS validates against parent table', () => {
    // Add reverse relation to ordersTable for nested EXISTS
    const config = validConfig()
    const ordersWithInverse = {
      ...ordersTable,
      relations: [...ordersTable.relations],
    }
    config.tables = [usersTable, ordersWithInverse, eventsTable]

    const idx = buildIndex(config)
    // users -> orders (exists) is valid
    const q: QueryDefinition = {
      from: 'users',
      filters: [
        {
          table: 'orders',
          exists: true,
          filters: [{ column: 'amount', operator: '>', value: 100 }],
        },
      ],
    }
    const err = validateQuery(q, adminCtx, idx, allRoles)
    expect(err).toBeNull()
  })
})
