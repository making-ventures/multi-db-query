import { introspectPostgres } from '@mkven/multi-db-executor-postgres'
import type { IntrospectResult } from '@mkven/multi-db-query'
import { introspectionMetadataProvider } from '@mkven/multi-db-query'
import { describe, expect, it } from 'vitest'

const PG_URL = process.env.PG_URL ?? 'postgresql://postgres:postgres@localhost:5432/multidb'

type IntrospectedTable = IntrospectResult['tables'][number]

/** Find a table by apiName and fail the test if missing. */
function findTable(result: IntrospectResult, apiName: string): IntrospectedTable {
  const table = result.tables.find((t) => t.apiName === apiName)
  expect(table, `table "${apiName}" should exist`).toBeDefined()
  // biome-ignore lint/style/noNonNullAssertion: guarded by expect above
  return table!
}

/** Find a column by apiName and fail the test if missing. */
function findColumn(columns: IntrospectedTable['columns'], apiName: string): IntrospectedTable['columns'][number] {
  const col = columns.find((c) => c.apiName === apiName)
  expect(col, `column "${apiName}" should exist`).toBeDefined()
  // biome-ignore lint/style/noNonNullAssertion: guarded by expect above
  return col!
}

describe('introspectPostgres', () => {
  it('discovers tables from the public schema', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    expect(result.tables.length).toBeGreaterThan(0)

    const orders = findTable(result, 'orders')
    expect(orders.physicalName).toBe('public.orders')
    expect(orders.columns.length).toBeGreaterThan(0)

    const idCol = findColumn(orders.columns, 'id')
    expect(idCol.physicalName).toBe('id')
  })

  it('discovers primary keys', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = findTable(result, 'orders')
    expect(orders.primaryKey).toContain('id')
  })

  it('returns empty relations when no FK constraints exist', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = findTable(result, 'orders')
    expect(orders.relations).toEqual([])
  })

  it('maps PG types to ColumnType', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const orders = findTable(result, 'orders')
    const cols = new Map(orders.columns.map((c) => [c.apiName, c]))

    expect(cols.get('id')?.type).toBe('int')
    expect(cols.get('totalAmount')?.type).toBe('decimal')
    expect(cols.get('orderStatus')?.type).toBe('string')
    expect(cols.get('priorities')?.type).toBe('int[]')
  })

  it('maps array types correctly', async () => {
    const result = await introspectPostgres({
      connection: { connectionString: PG_URL },
      schemas: ['public'],
    })

    const products = findTable(result, 'products')
    const labelsCol = findColumn(products.columns, 'labels')
    expect(labelsCol.type).toBe('string[]')
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

    const orders = findTable(result, 'orders')
    const totalCol = findColumn(orders.columns, 'total_amount')
    expect(totalCol.physicalName).toBe('total_amount')
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
