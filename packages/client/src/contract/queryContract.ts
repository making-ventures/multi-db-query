import type { ExecutionContext, QueryDefinition, QueryResult } from '@mkven/multi-db-validation'
import { ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'

// ── QueryContract ──────────────────────────────────────────────

export interface QueryContract {
  query<T = unknown>(input: { definition: QueryDefinition; context: ExecutionContext }): Promise<QueryResult<T>>
}

// ── describeQueryContract ──────────────────────────────────────

/**
 * Parameterized contract test suite.
 * Verifies that any implementation of QueryContract behaves correctly.
 *
 * Usage:
 * ```ts
 * describeQueryContract('direct', async () => {
 *   const multiDb = await createMultiDb({ ... })
 *   return multiDb
 * })
 * ```
 */
export function describeQueryContract(name: string, factory: () => Promise<QueryContract>): void {
  describe(`QueryContract: ${name}`, () => {
    let engine: QueryContract

    beforeAll(async () => {
      engine = await factory()
    })

    it('#219: simple select returns data', async () => {
      const result = await engine.query({
        definition: { from: 'orders', columns: ['id', 'status'] },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
      if (result.kind === 'data') {
        expect(result.meta.columns).toHaveLength(2)
      }
    })

    it('#222: validation error on unknown table', async () => {
      await expect(
        engine.query({
          definition: { from: 'nonExistentTable' },
          context: { roles: { user: ['admin'] } },
        }),
      ).rejects.toThrow(ValidationError)
    })

    it('#224: count mode returns count', async () => {
      const result = await engine.query({
        definition: { from: 'orders', executeMode: 'count' },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('count')
      if (result.kind === 'count') {
        expect(typeof result.count).toBe('number')
      }
    })

    it('#225: SQL-only mode returns sql', async () => {
      const result = await engine.query({
        definition: { from: 'orders', executeMode: 'sql-only' },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('sql')
      if (result.kind === 'sql') {
        expect(result.sql).toContain('SELECT')
        expect(Array.isArray(result.params)).toBe(true)
      }
    })

    it('#220: filter + join', async () => {
      const result = await engine.query({
        definition: {
          from: 'orders',
          joins: [{ table: 'products' }],
          filters: [{ column: 'status', operator: '=', value: 'active' }],
        },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
      if (result.kind === 'data') {
        expect(result.meta.columns.length).toBeGreaterThan(0)
      }
    })

    it('#221: aggregation', async () => {
      const result = await engine.query({
        definition: {
          from: 'orders',
          columns: ['status'],
          groupBy: [{ column: 'status' }],
          aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
        },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
    })

    it('#223: access denied on restricted column', async () => {
      await expect(
        engine.query({
          definition: { from: 'orders', columns: ['id', 'internalNote'] },
          context: { roles: { user: ['tenant-user'] } },
        }),
      ).rejects.toThrow(ValidationError)
    })

    it('#236: debug mode includes debugLog', async () => {
      const result = await engine.query({
        definition: { from: 'orders', columns: ['id', 'status'], debug: true },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
      if (result.kind === 'data') {
        expect(Array.isArray(result.debugLog)).toBe(true)
        expect(result.debugLog!.length).toBeGreaterThan(0)
        for (const entry of result.debugLog!) {
          expect(typeof entry.timestamp).toBe('number')
          expect(typeof entry.phase).toBe('string')
          expect(typeof entry.message).toBe('string')
        }
      }
    })

    it('#237: masking reported in meta.columns', async () => {
      const result = await engine.query({
        definition: { from: 'orders', columns: ['id', 'total'] },
        context: { roles: { user: ['tenant-user'] } },
      })
      expect(result.kind).toBe('data')
      if (result.kind === 'data') {
        const totalCol = result.meta.columns.find((c) => c.apiName === 'total')
        expect(totalCol).toBeDefined()
        expect(totalCol!.masked).toBe(true)
        const idCol = result.meta.columns.find((c) => c.apiName === 'id')
        expect(idCol).toBeDefined()
        expect(idCol!.masked).toBe(false)
      }
    })

    it('#238: byIds returns data', async () => {
      const result = await engine.query({
        definition: { from: 'orders', byIds: [1, 2] },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
      if (result.kind === 'data') {
        expect(result.meta.columns.length).toBeGreaterThan(0)
      }
    })
  })
}
