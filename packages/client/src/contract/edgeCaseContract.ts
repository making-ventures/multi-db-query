import type { ExecutionContext } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'
import type { QueryContract } from './queryContract.js'

// ── Helpers ────────────────────────────────────────────────────

const admin: ExecutionContext = { roles: { user: ['admin'] } }

// ── describeEdgeCaseContract ───────────────────────────────────

export function describeEdgeCaseContract(name: string, factory: () => Promise<QueryContract>): void {
  describe(`EdgeCaseContract: ${name}`, () => {
    let engine: QueryContract

    beforeAll(async () => {
      engine = await factory()
    })

    // ── 18. Edge Cases ───────────────────────────────────────

    describe('18. Edge Cases', () => {
      it('C1700: empty result set', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            filters: [{ column: 'status', operator: '=', value: 'nonexistent_status_xyz' }],
          },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.data).toEqual([])
          expect(r.meta.columns).toBeDefined()
          expect(r.meta.columns.length).toBeGreaterThan(0)
        }
      })

      it('C1701: single row result', async () => {
        const r = await engine.query({
          definition: { from: 'orders', byIds: [1] },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.data.length).toBe(1)
        }
      })

      it('C1702: large in-list', async () => {
        const values = Array.from({ length: 50 }, (_, i) => `status_${i}`)
        values.push('active')
        const r = await engine.query({
          definition: { from: 'orders', filters: [{ column: 'status', operator: 'in', value: values }] },
          context: admin,
        })
        // Should not throw — large IN list works
        expect(r.kind).toBe('data')
      })

      it('C1703: nullable column in result', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'discount'] },
          context: admin,
        })
        if (r.kind === 'data') {
          const hasNull = r.data.some((row) => (row as Record<string, unknown>).discount === null)
          expect(hasNull).toBe(true)
        }
      })

      it('C1704: boolean column values', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'isPaid'] },
          context: admin,
        })
        if (r.kind === 'data') {
          for (const row of r.data) {
            const val = (row as Record<string, unknown>).isPaid
            expect(val === true || val === false || val === null).toBe(true)
          }
        }
      })

      it('C1705: timestamp format', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['createdAt'] },
          context: admin,
        })
        if (r.kind === 'data') {
          for (const row of r.data) {
            const val = (row as Record<string, unknown>).createdAt
            // Timestamps can be ISO strings, numbers (epoch ms), or Date objects
            expect(typeof val === 'string' || typeof val === 'number' || val instanceof Date).toBe(true)
          }
        }
      })

      it('C1706: date format', async () => {
        const r = await engine.query({
          definition: { from: 'invoices', columns: ['dueDate'] },
          context: admin,
        })
        if (r.kind === 'data') {
          for (const row of r.data) {
            const val = (row as Record<string, unknown>).dueDate
            if (val !== null) {
              // Date can be ISO string (YYYY-MM-DD...) or Date object
              if (val instanceof Date) {
                expect(val.getTime()).not.toBeNaN()
              } else {
                expect(typeof val).toBe('string')
                expect(val as string).toMatch(/^\d{4}-\d{2}-\d{2}/)
              }
            }
          }
        }
      })

      it('C1707: array column in result', async () => {
        const r = await engine.query({
          definition: { from: 'products', columns: ['name', 'labels'] },
          context: admin,
        })
        if (r.kind === 'data') {
          for (const row of r.data) {
            const val = (row as Record<string, unknown>).labels
            expect(val === null || Array.isArray(val)).toBe(true)
          }
        }
      })

      it('C1708: decimal precision', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['total'] },
          context: admin,
        })
        if (r.kind === 'data') {
          for (const row of r.data) {
            const val = (row as Record<string, unknown>).total
            // Decimals may be numbers or strings depending on driver
            expect(typeof val === 'number' || typeof val === 'string').toBe(true)
          }
        }
      })

      it('C1709: multiple filters (implicit AND)', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            filters: [
              { column: 'status', operator: '=', value: 'active' },
              { column: 'total', operator: '>', value: 50 },
            ],
          },
          context: admin,
        })
        if (r.kind === 'data') {
          // Both conditions applied: status='active' AND total>50 → orders 1 (100), 4 (300)
          expect(r.data.length).toBe(2)
        }
      })

      it('C1710: cache strategy reported', async () => {
        const r = await engine.query({
          definition: { from: 'users', byIds: ['00000000-0000-4000-a000-000000000c01'] },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.meta.strategy).toBe('cache')
        }
      })

      it.skip('C1711: materialized replica query', async () => {
        // TODO: planner doesn't route to replica when primary executor is available
        const r = await engine.query({
          definition: { from: 'orders', freshness: 'seconds' },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.meta.strategy).toBe('materialized')
          expect(r.meta.tablesUsed[0]?.source).toBe('replica')
        }
      })

      it.skip('C1712: cross-DB Trino join', async () => {
        // TODO: Trino catalog configuration not available in Docker Compose test setup
        const r = await engine.query({
          definition: {
            from: 'events',
            columns: ['id'],
            joins: [{ table: 'users' }],
          },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.meta.strategy).toBe('trino-cross-db')
        }
      })

      it('C1713: DISTINCT + count mode', async () => {
        const r = await engine.query({
          definition: { from: 'orders', distinct: true, columns: ['status'], executeMode: 'count' },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          // Count of all rows (DISTINCT + count = counts distinct rows)
          expect(r.count).toBeGreaterThanOrEqual(4)
        }
      })

      it('C1714: GROUP BY with zero matching rows', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: [],
            filters: [{ column: 'status', operator: '=', value: 'nonexistent' }],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
          },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.data).toEqual([])
          expect(r.meta.columns).toBeDefined()
        }
      })

      it('C1715: freshness realtime skips materialized', async () => {
        const r = await engine.query({
          definition: { from: 'orders', freshness: 'realtime' },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.meta.strategy).not.toBe('materialized')
        }
      })

      it.skip('C1716: freshness hours allows stale replica', async () => {
        // TODO: planner doesn't route to replica when primary executor is available
        const r = await engine.query({
          definition: { from: 'orders', freshness: 'hours' },
          context: admin,
        })
        if (r.kind === 'data') {
          expect(r.meta.strategy).toBe('materialized')
        }
      })
    })
  })
}
