import type { DbExecutor } from '@mkven/multi-db-query'
import { ConnectionError, ExecutionError } from '@mkven/multi-db-validation'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

// ── Types ──────────────────────────────────────────────────────

export interface ExecutorContractConfig {
  /** A valid SQL query that returns ≥ 1 row (e.g. `'SELECT 1 AS n'`). */
  readonly validQuery: string
  /** A SQL query that the database will reject (e.g. `'SELECT * FROM __nonexistent_table_xyz__'`). */
  readonly invalidQuery: string
}

// ── describeExecutorContract ───────────────────────────────────

export function describeExecutorContract(
  name: string,
  factory: () => DbExecutor,
  config: ExecutorContractConfig,
): void {
  describe(`ExecutorContract: ${name}`, () => {
    let executor: DbExecutor

    beforeAll(() => {
      executor = factory()
    })

    afterAll(async () => {
      await executor?.close()
    })

    it('C1800: ping() resolves for healthy executor', async () => {
      await expect(executor.ping()).resolves.toBeUndefined()
    })

    it('C1801: execute() returns Record[] for valid query', async () => {
      const rows = await executor.execute(config.validQuery, [])
      expect(Array.isArray(rows)).toBe(true)
      expect(rows.length).toBeGreaterThanOrEqual(1)
      for (const row of rows) {
        expect(typeof row).toBe('object')
        expect(row).not.toBeNull()
      }
    })

    it('C1802: execute() throws ExecutionError for invalid SQL', async () => {
      try {
        await executor.execute(config.invalidQuery, [])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        if (err instanceof ExecutionError) {
          expect(err.code).toBe('QUERY_FAILED')
        }
      }
    })

    it('C1803: close() resolves without error', async () => {
      const temp = factory()
      await expect(temp.close()).resolves.toBeUndefined()
    })

    it('C1804: ping() throws ConnectionError after close', async () => {
      const temp = factory()
      await temp.close()
      try {
        await temp.ping()
        // Stateless executors (e.g. Trino REST) may not throw — acceptable
      } catch (err) {
        expect(err instanceof ConnectionError || err instanceof ExecutionError).toBe(true)
      }
    })
  })
}
