import { ConnectionError, ExecutionError } from '@mkven/multi-db-query'
import type { QueryResult } from 'trino-client'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createTrinoExecutor } from '../src/index.js'

// ── Mock trino-client ──────────────────────────────────────────

const mockQuery = vi.fn()
const mockCancel = vi.fn()

vi.mock('trino-client', () => ({
  Trino: {
    create: () => ({ query: mockQuery, cancel: mockCancel }),
  },
}))

/** Create an async iterable from an array of QueryResult pages. */
function asyncIter(results: QueryResult[]): AsyncIterable<QueryResult> {
  return {
    [Symbol.asyncIterator]: async function* () {
      for (const r of results) yield r
    },
  }
}

function trinoOk(data: unknown[][], columns: string[]): QueryResult {
  return {
    id: 'q1',
    columns: columns.map((name) => ({ name, type: 'varchar' })),
    data,
  }
}

function trinoError(message: string): QueryResult {
  return {
    id: 'q1',
    error: {
      message,
      errorCode: 1,
      errorName: 'GENERIC_INTERNAL_ERROR',
      errorType: 'INTERNAL_ERROR',
      failureInfo: { type: 'error', message, suppressed: [], stack: [] },
    },
  }
}

// ── Tests ──────────────────────────────────────────────────────

describe('executor-trino', () => {
  const executor = createTrinoExecutor({ server: 'http://trino:8080' })

  afterEach(() => {
    vi.clearAllMocks()
  })

  // ── S1: escapeTrinoValue — unsupported types ──────────────

  describe('escapeTrinoValue rejects unsupported types', () => {
    it('throws ExecutionError on object param', async () => {
      mockQuery.mockResolvedValue(asyncIter([trinoOk([], ['id'])]))
      try {
        await executor.execute('SELECT ?', [{}])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
        expect((e.cause as Error)?.message).toContain('Unsupported Trino parameter type: object')
      }
    })

    it('throws ExecutionError on function param', async () => {
      mockQuery.mockResolvedValue(asyncIter([trinoOk([], ['id'])]))
      try {
        await executor.execute('SELECT ?', [() => {}])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
        expect((e.cause as Error)?.message).toContain('Unsupported Trino parameter type: function')
      }
    })

    it('throws ExecutionError on symbol param', async () => {
      mockQuery.mockResolvedValue(asyncIter([trinoOk([], ['id'])]))
      try {
        await executor.execute('SELECT ?', [Symbol('x')])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
        expect((e.cause as Error)?.message).toContain('Unsupported Trino parameter type: symbol')
      }
    })
  })

  // ── S2: ExecutionError on Trino error response ────────────

  describe('Trino error responses throw ExecutionError', () => {
    it('initial response error', async () => {
      mockQuery.mockResolvedValue(asyncIter([trinoError('Table not found')]))

      try {
        await executor.execute('SELECT * FROM bad_table', [])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
        expect(e.details).toMatchObject({ database: 'trino', dialect: 'trino' })
      }
    })

    it('polling response error', async () => {
      mockQuery.mockResolvedValue(
        asyncIter([
          { id: 'q1', columns: [{ name: 'id', type: 'varchar' }], data: [] },
          trinoError('Query exceeded max time'),
        ]),
      )

      try {
        await executor.execute('SELECT * FROM slow_table', [])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
      }
    })
  })

  // ── Happy path ────────────────────────────────────────────

  describe('happy path', () => {
    it('returns rows from successful query', async () => {
      mockQuery.mockResolvedValue(
        asyncIter([
          trinoOk(
            [
              ['1', 'active'],
              ['2', 'shipped'],
            ],
            ['id', 'status'],
          ),
        ]),
      )

      const rows = await executor.execute('SELECT id, status FROM orders', [])
      expect(rows).toEqual([
        { id: '1', status: 'active' },
        { id: '2', status: 'shipped' },
      ])
    })

    it('inlines params correctly', async () => {
      mockQuery.mockResolvedValue(asyncIter([trinoOk([[1]], ['cnt'])]))

      await executor.execute('SELECT * WHERE id = ? AND active = ? AND name = ?', [42, true, "O'Brien"])
      const sql = mockQuery.mock.calls[0]?.[0] as string
      expect(sql).toContain('42')
      expect(sql).toContain('TRUE')
      expect(sql).toContain("'O''Brien'")
    })
  })

  // ── Network errors ────────────────────────────────────────

  describe('network error wrapping', () => {
    it('execute() wraps network error in ExecutionError', async () => {
      mockQuery.mockRejectedValue(new Error('connect ECONNREFUSED'))

      try {
        await executor.execute('SELECT 1', [])
        expect.fail('Expected ExecutionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ExecutionError)
        const e = err as ExecutionError
        expect(e.code).toBe('QUERY_FAILED')
      }
    })

    it('ping() wraps network error in ConnectionError', async () => {
      mockQuery.mockRejectedValue(new Error('connect ECONNREFUSED'))

      try {
        await executor.ping()
        expect.fail('Expected ConnectionError')
      } catch (err) {
        expect(err).toBeInstanceOf(ConnectionError)
        const e = err as ConnectionError
        expect(e.code).toBe('CONNECTION_FAILED')
      }
    })
  })

  // ── close() ───────────────────────────────────────────────

  it('close() resolves without error (stateless)', async () => {
    await expect(executor.close()).resolves.toBeUndefined()
  })
})
