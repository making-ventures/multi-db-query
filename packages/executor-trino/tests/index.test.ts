import { ConnectionError, ExecutionError } from '@mkven/multi-db-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createTrinoExecutor } from '../src/index.js'

// ── Mock fetch ─────────────────────────────────────────────────

function trinoOk(data: unknown[][], columns: string[]): Response {
  return Response.json({
    id: 'q1',
    columns: columns.map((name) => ({ name, type: 'varchar' })),
    data,
  })
}

function trinoError(message: string): Response {
  return Response.json({
    id: 'q1',
    error: { message, errorCode: 1, errorName: 'GENERIC_INTERNAL_ERROR' },
  })
}

// ── Tests ──────────────────────────────────────────────────────

describe('executor-trino', () => {
  const executor = createTrinoExecutor({ server: 'http://trino:8080' })

  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn())
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // ── S1: escapeTrinoValue — unsupported types ──────────────

  describe('escapeTrinoValue rejects unsupported types', () => {
    it('throws ExecutionError on object param', async () => {
      vi.mocked(fetch).mockResolvedValue(trinoOk([], ['id']))
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
      vi.mocked(fetch).mockResolvedValue(trinoOk([], ['id']))
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
      vi.mocked(fetch).mockResolvedValue(trinoOk([], ['id']))
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
      vi.mocked(fetch).mockResolvedValue(trinoError('Table not found'))

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
      const firstResponse = Response.json({
        id: 'q1',
        nextUri: 'http://trino:8080/v1/statement/q1/1',
        columns: [{ name: 'id', type: 'varchar' }],
      })
      vi.mocked(fetch).mockResolvedValueOnce(firstResponse).mockResolvedValueOnce(trinoError('Query exceeded max time'))

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
      vi.mocked(fetch).mockResolvedValue(
        trinoOk(
          [
            ['1', 'active'],
            ['2', 'shipped'],
          ],
          ['id', 'status'],
        ),
      )

      const rows = await executor.execute('SELECT id, status FROM orders', [])
      expect(rows).toEqual([
        { id: '1', status: 'active' },
        { id: '2', status: 'shipped' },
      ])
    })

    it('inlines params correctly', async () => {
      vi.mocked(fetch).mockResolvedValue(trinoOk([[1]], ['cnt']))

      await executor.execute('SELECT * WHERE id = ? AND active = ? AND name = ?', [42, true, "O'Brien"])
      const body = vi.mocked(fetch).mock.calls[0]?.[1]?.body as string
      expect(body).toContain('42')
      expect(body).toContain('TRUE')
      expect(body).toContain("'O''Brien'")
    })
  })

  // ── Network errors ────────────────────────────────────────

  describe('network error wrapping', () => {
    it('execute() wraps network error in ExecutionError', async () => {
      vi.mocked(fetch).mockRejectedValue(new TypeError('fetch failed'))

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
      vi.mocked(fetch).mockRejectedValue(new TypeError('fetch failed'))

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
})
