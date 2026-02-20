import type { ExecutionContext, QueryDefinition, QueryResult } from '@mkven/multi-db-validation'
import { ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'

// ── QueryContract ──────────────────────────────────────────────

export interface QueryContract {
  query(input: { definition: QueryDefinition; context: ExecutionContext }): Promise<QueryResult>
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
        definition: { from: 'orders' },
        context: { roles: { user: ['admin'] } },
      })
      expect(result.kind).toBe('data')
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
  })
}
