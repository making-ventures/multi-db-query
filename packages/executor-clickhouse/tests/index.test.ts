import { ConnectionError, ExecutionError } from '@mkven/multi-db-query'
import { afterEach, describe, expect, it, vi } from 'vitest'

// ── Mock @clickhouse/client ────────────────────────────────────

const mockPing = vi.fn()
const mockQuery = vi.fn()
const mockClose = vi.fn()

vi.mock('@clickhouse/client', () => ({
  createClient: () => ({
    query: mockQuery,
    ping: mockPing,
    close: mockClose,
  }),
}))

// ── Tests ──────────────────────────────────────────────────────

describe('executor-clickhouse', () => {
  afterEach(() => {
    vi.clearAllMocks()
  })

  it('ping failure throws ConnectionError', async () => {
    const { createClickHouseExecutor } = await import('../src/index.js')
    const executor = createClickHouseExecutor({})

    mockPing.mockResolvedValue({ success: false })

    try {
      await executor.ping()
      expect.fail('Expected ConnectionError')
    } catch (err) {
      expect(err).toBeInstanceOf(ConnectionError)
      const e = err as ConnectionError
      expect(e.code).toBe('CONNECTION_FAILED')
      expect(e.message).toBe('ClickHouse ping failed')
    }
  })

  it('ping success does not throw', async () => {
    const { createClickHouseExecutor } = await import('../src/index.js')
    const executor = createClickHouseExecutor({})

    mockPing.mockResolvedValue({ success: true })

    await expect(executor.ping()).resolves.toBeUndefined()
  })

  it('ping network error throws ConnectionError', async () => {
    const { createClickHouseExecutor } = await import('../src/index.js')
    const executor = createClickHouseExecutor({})

    mockPing.mockRejectedValue(new Error('ECONNREFUSED'))

    try {
      await executor.ping()
      expect.fail('Expected ConnectionError')
    } catch (err) {
      expect(err).toBeInstanceOf(ConnectionError)
      const e = err as ConnectionError
      expect(e.code).toBe('CONNECTION_FAILED')
    }
  })

  it('execute error throws ExecutionError', async () => {
    const { createClickHouseExecutor } = await import('../src/index.js')
    const executor = createClickHouseExecutor({})

    mockPing.mockResolvedValue({ success: true })
    mockQuery.mockRejectedValue(new Error('Table __bad__ does not exist'))

    try {
      await executor.execute('SELECT * FROM __bad__', [])
      expect.fail('Expected ExecutionError')
    } catch (err) {
      expect(err).toBeInstanceOf(ExecutionError)
      const e = err as ExecutionError
      expect(e.code).toBe('QUERY_FAILED')
    }
  })
})
