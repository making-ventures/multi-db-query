import { introspectionMetadataProvider } from '@mkven/multi-db-query'
import { introspectPostgres } from '@mkven/multi-db-executor-postgres'
import { describe, expect, it } from 'vitest'

const PG_URL = process.env.PG_URL ?? 'postgresql://postgres:postgres@localhost:5432/multidb'

describe('introspectPostgres', () => {
  it('discovers tables from the public schema', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    expect(result.tables.length).toBeGreaterThan(0)

    // Should find the orders table
    const orders = result.tables.find((t) => t.apiName === 'orders')
    expect(orders).toBeDefined()
    expect(orders!.physicalName).toBe('public.orders')
    expect(orders!.columns.length).toBeGreaterThan(0)

    // Columns should be camelCased by default
    const idCol = orders!.columns.find((c) => c.apiName === 'id')
    expect(idCol).toBeDefined()
    expect(idCol!.physicalName).toBe('id')
  })

  it('discovers primary keys', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = result.tables.find((t) => t.apiName === 'orders')
    expect(orders).toBeDefined()
    expect(orders!.primaryKey).toContain('id')
  })

  it('returns empty relations when no FK constraints exist', async () => {
    // The test DB has no explicit FOREIGN KEY constraints
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = result.tables.find((t) => t.apiName === 'orders')
    expect(orders).toBeDefined()
    expect(orders!.relations).toEqual([])
  })

  it('maps PG types to ColumnType', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = result.tables.find((t) => t.apiName === 'orders')!
    const cols = new Map(orders.columns.map((c) => [c.apiName, c]))

    // int column
    expect(cols.get('id')!.type).toBe('int')
    // decimal column
    expect(cols.get('totalAmount')!.type).toBe('decimal')
    // string column
    expect(cols.get('orderStatus')!.type).toBe('string')
  })

  it('excludes specified tables', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
      exclude: ['orders'],
    })

    const orders = result.tables.find((t) => t.apiName === 'orders')
    expect(orders).toBeUndefined()
  })

  it('preserves names when apiNameMapper is "preserve"', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
      apiNameMapper: 'preserve',
    })

    const orders = result.tables.find((t) => t.apiName === 'orders')
    expect(orders).toBeDefined()

    // snake_case columns should not be converted
    const totalCol = orders!.columns.find((c) => c.apiName === 'total_amount')
    expect(totalCol).toBeDefined()
  })

  it('wraps result into MetadataProvider via introspectionMetadataProvider', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const provider = introspectionMetadataProvider('pg-main', 'postgres', result)
    const config = await provider.load()

    expect(config.databases).toEqual([{ id: 'pg-main', engine: 'postgres' }])
    expect(config.tables.length).toBe(result.tables.length)
    expect(config.tables.every((t) => t.database === 'pg-main')).toBe(true)
    expect(config.caches).toEqual([])
    expect(config.externalSyncs).toEqual([])
  })
})
