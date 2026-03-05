import { describe, expect, it } from 'vitest'
import { introspectionMetadataProvider, resolveApiNameMapper, snakeToCamel } from '../../src/introspection.js'

describe('snakeToCamel', () => {
  it('converts snake_case to camelCase', () => {
    expect(snakeToCamel('user_name')).toBe('userName')
    expect(snakeToCamel('created_at')).toBe('createdAt')
    expect(snakeToCamel('order_id')).toBe('orderId')
  })

  it('handles multiple underscores', () => {
    expect(snakeToCamel('first_last_name')).toBe('firstLastName')
    expect(snakeToCamel('a_b_c_d')).toBe('aBCD')
  })

  it('preserves already camelCase', () => {
    expect(snakeToCamel('userName')).toBe('userName')
    expect(snakeToCamel('id')).toBe('id')
    expect(snakeToCamel('createdAt')).toBe('createdAt')
  })

  it('handles leading underscores by stripping them', () => {
    expect(snakeToCamel('_private')).toBe('private')
    expect(snakeToCamel('__double')).toBe('double')
    expect(snakeToCamel('_some_field')).toBe('someField')
  })

  it('handles trailing underscores by stripping them', () => {
    expect(snakeToCamel('name_')).toBe('name')
    expect(snakeToCamel('name__')).toBe('name')
  })

  it('handles consecutive mid-word underscores', () => {
    expect(snakeToCamel('user__name')).toBe('userName')
    expect(snakeToCamel('a___b')).toBe('aB')
  })

  it('handles all underscores', () => {
    expect(snakeToCamel('_')).toBe('')
    expect(snakeToCamel('___')).toBe('')
  })

  it('handles empty string', () => {
    expect(snakeToCamel('')).toBe('')
  })

  it('lowercases UPPER_CASE identifiers', () => {
    expect(snakeToCamel('USER_NAME')).toBe('userName')
    expect(snakeToCamel('ORDER_ID')).toBe('orderId')
  })

  it('lowercases Mixed_Case identifiers', () => {
    expect(snakeToCamel('Mixed_Case')).toBe('mixedCase')
    expect(snakeToCamel('User_Name_ID')).toBe('userNameId')
  })

  it('handles single character input', () => {
    expect(snakeToCamel('x')).toBe('x')
    expect(snakeToCamel('X')).toBe('X')
  })

  it('handles numeric suffixes', () => {
    expect(snakeToCamel('col_1')).toBe('col1')
    expect(snakeToCamel('item_2_name')).toBe('item2Name')
  })
})

describe('resolveApiNameMapper', () => {
  it('defaults to snakeToCamel for undefined', () => {
    const mapper = resolveApiNameMapper(undefined)
    expect(mapper('user_name')).toBe('userName')
  })

  it('returns snakeToCamel for "camelCase"', () => {
    const mapper = resolveApiNameMapper('camelCase')
    expect(mapper('user_name')).toBe('userName')
  })

  it('returns identity for "preserve"', () => {
    const mapper = resolveApiNameMapper('preserve')
    expect(mapper('user_name')).toBe('user_name')
  })

  it('returns custom function as-is', () => {
    const custom = (s: string) => s.toUpperCase()
    const mapper = resolveApiNameMapper(custom)
    expect(mapper('user_name')).toBe('USER_NAME')
  })
})

describe('introspectionMetadataProvider', () => {
  it('stamps database on every table', async () => {
    const result = {
      tables: [
        { id: 'users', apiName: 'users', physicalName: 'public.users', columns: [], primaryKey: ['id'], relations: [] },
        {
          id: 'orders',
          apiName: 'orders',
          physicalName: 'public.orders',
          columns: [],
          primaryKey: ['id'],
          relations: [],
        },
      ],
    }

    const provider = introspectionMetadataProvider('mydb', 'postgres', result)
    const config = await provider.load()

    expect(config.tables).toHaveLength(2)
    expect(config.tables.every((t) => t.database === 'mydb')).toBe(true)
  })

  it('returns correct database entry', async () => {
    const provider = introspectionMetadataProvider('pg1', 'postgres', { tables: [] })
    const config = await provider.load()

    expect(config.databases).toEqual([{ id: 'pg1', engine: 'postgres' }])
  })

  it('handles empty tables', async () => {
    const provider = introspectionMetadataProvider('empty', 'clickhouse', { tables: [] })
    const config = await provider.load()

    expect(config.tables).toEqual([])
    expect(config.caches).toEqual([])
    expect(config.externalSyncs).toEqual([])
  })

  it('does not mutate the original result', async () => {
    const original = {
      tables: [
        { id: 'users', apiName: 'users', physicalName: 'public.users', columns: [], primaryKey: ['id'], relations: [] },
      ],
    }
    const tablesBefore = [...original.tables]

    introspectionMetadataProvider('db', 'postgres', original)

    expect(original.tables).toEqual(tablesBefore)
    expect(original.tables[0]).not.toHaveProperty('database')
  })

  it('preserves table fields when stamping database', async () => {
    const col = { apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false }
    const rel = { column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const }
    const result = {
      tables: [
        {
          id: 'orders',
          apiName: 'orders',
          physicalName: 'public.orders',
          columns: [col],
          primaryKey: ['id'],
          relations: [rel],
        },
      ],
    }

    const provider = introspectionMetadataProvider('db', 'postgres', result)
    const config = await provider.load()

    const t = config.tables[0]
    expect(t).toBeDefined()
    expect(t?.columns).toEqual([col])
    expect(t?.relations).toEqual([rel])
    expect(t?.primaryKey).toEqual(['id'])
    expect(t?.database).toBe('db')
  })
})
