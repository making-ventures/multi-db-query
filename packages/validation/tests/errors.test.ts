import { describe, expect, it } from 'vitest'
import {
  ConfigError,
  ConnectionError,
  ExecutionError,
  MultiDbError,
  PlannerError,
  ProviderError,
  ValidationError,
} from '../src/errors.js'

describe('MultiDbError', () => {
  it('has code and message', () => {
    const err = new MultiDbError('TEST_CODE', 'test message')
    expect(err.code).toBe('TEST_CODE')
    expect(err.message).toBe('test message')
    expect(err.name).toBe('MultiDbError')
    expect(err).toBeInstanceOf(Error)
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('toJSON() returns code and message', () => {
    const err = new MultiDbError('X', 'msg')
    expect(err.toJSON()).toEqual({ code: 'X', message: 'msg' })
  })

  it('toJSON() serializes cause chain', () => {
    const root = new Error('root cause')
    const mid = new MultiDbError('MID', 'mid', { cause: root })
    const top = new MultiDbError('TOP', 'top', { cause: mid })
    const json = top.toJSON()
    expect(json.cause).toEqual({
      code: 'MID',
      message: 'mid',
      cause: { message: 'root cause', name: 'Error' },
    })
  })

  it('toJSON() omits cause when undefined', () => {
    const err = new MultiDbError('X', 'msg')
    expect(err.toJSON()).not.toHaveProperty('cause')
  })
})

describe('ConfigError', () => {
  it('collects errors with summary message', () => {
    const err = new ConfigError([
      { code: 'INVALID_API_NAME', message: 'bad name: 123', details: { entity: 'users', field: 'apiName' } },
      { code: 'DUPLICATE_API_NAME', message: 'dup: users', details: { entity: 'users' } },
      { code: 'INVALID_REFERENCE', message: 'no such db', details: { database: 'analytics' } },
    ])
    expect(err.code).toBe('CONFIG_INVALID')
    expect(err.message).toBe('Config invalid: 3 errors')
    expect(err.errors).toHaveLength(3)
    expect(err.name).toBe('ConfigError')
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('singular message for 1 error', () => {
    const err = new ConfigError([{ code: 'INVALID_CACHE', message: 'bad cache', details: { cacheId: 'c1' } }])
    expect(err.message).toBe('Config invalid: 1 error')
  })

  it('toJSON() includes errors array', () => {
    const entries = [{ code: 'INVALID_SYNC' as const, message: 'bad sync', details: { database: 'db1' } }]
    const err = new ConfigError(entries)
    const json = err.toJSON()
    expect(json.code).toBe('CONFIG_INVALID')
    expect(json.errors).toEqual(entries)
  })

  it('survives JSON.stringify()', () => {
    const err = new ConfigError([
      { code: 'INVALID_RELATION', message: 'no such col', details: { entity: 'orders', field: 'userId' } },
    ])
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.code).toBe('CONFIG_INVALID')
    expect(parsed.errors).toHaveLength(1)
    expect(parsed.errors[0].code).toBe('INVALID_RELATION')
  })
})

describe('ConnectionError', () => {
  it('CONNECTION_FAILED with unreachable providers', () => {
    const cause = new Error('ECONNREFUSED')
    const err = new ConnectionError('CONNECTION_FAILED', 'Cannot connect', {
      unreachable: [
        { id: 'pg-main', type: 'executor', engine: 'postgres', cause },
        { id: 'redis-1', type: 'cache', engine: 'redis' },
      ],
    })
    expect(err.code).toBe('CONNECTION_FAILED')
    expect(err.name).toBe('ConnectionError')
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('REQUEST_TIMEOUT with url and timeoutMs', () => {
    const err = new ConnectionError('REQUEST_TIMEOUT', 'timed out', {
      url: 'http://localhost:8080',
      timeoutMs: 5000,
    })
    expect(err.code).toBe('REQUEST_TIMEOUT')
    expect(err.details).toEqual({ url: 'http://localhost:8080', timeoutMs: 5000 })
  })

  it('toJSON() serializes unreachable causes', () => {
    const cause = new Error('refused')
    const err = new ConnectionError('CONNECTION_FAILED', 'fail', {
      unreachable: [{ id: 'pg1', type: 'executor', engine: 'postgres', cause }],
    })
    const json = err.toJSON()
    const details = json.details as { unreachable: { cause: { message: string } }[] }
    expect(details.unreachable[0].cause.message).toBe('refused')
  })

  it('toJSON() omits engine/cause when undefined', () => {
    const err = new ConnectionError('NETWORK_ERROR', 'net err', {
      unreachable: [{ id: 'x', type: 'executor' }],
    })
    const json = err.toJSON()
    const details = json.details as { unreachable: Record<string, unknown>[] }
    expect(details.unreachable[0]).not.toHaveProperty('engine')
    expect(details.unreachable[0]).not.toHaveProperty('cause')
  })

  it('survives JSON.stringify()', () => {
    const err = new ConnectionError('CONNECTION_FAILED', 'down', {
      unreachable: [{ id: 'ch1', type: 'executor', engine: 'clickhouse', cause: new Error('timeout') }],
    })
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.details.unreachable[0].engine).toBe('clickhouse')
    expect(parsed.details.unreachable[0].cause.message).toBe('timeout')
  })
})

describe('ValidationError', () => {
  it('collects errors with fromTable and summary', () => {
    const err = new ValidationError('users', [
      { code: 'UNKNOWN_TABLE', message: 'no table: orders', details: { table: 'orders' } },
      { code: 'UNKNOWN_COLUMN', message: 'no col: age', details: { table: 'users', column: 'age' } },
      { code: 'ACCESS_DENIED', message: 'denied', details: { table: 'secrets', role: 'viewer' } },
    ])
    expect(err.code).toBe('VALIDATION_FAILED')
    expect(err.fromTable).toBe('users')
    expect(err.message).toBe('Validation failed: 3 errors')
    expect(err.errors).toHaveLength(3)
    expect(err.name).toBe('ValidationError')
  })

  it('singular error message', () => {
    const err = new ValidationError('t', [{ code: 'INVALID_FILTER', message: 'bad', details: { filterIndex: 0 } }])
    expect(err.message).toBe('Validation failed: 1 error')
  })

  it('toJSON() includes fromTable and errors', () => {
    const errors = [{ code: 'INVALID_JOIN' as const, message: 'bad join', details: { table: 'orders', column: 'id' } }]
    const err = new ValidationError('users', errors)
    const json = err.toJSON()
    expect(json.fromTable).toBe('users')
    expect(json.errors).toEqual(errors)
  })

  it('survives JSON.stringify()', () => {
    const err = new ValidationError('invoices', [
      { code: 'INVALID_VALUE', message: 'missing to', details: { filterIndex: 2, operator: 'between' } },
      { code: 'UNKNOWN_COLUMN', message: 'no col', details: { column: 'foo', refColumn: 'bar', refTable: 'baz' } },
    ])
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.fromTable).toBe('invoices')
    expect(parsed.errors).toHaveLength(2)
    expect(parsed.errors[0].details.filterIndex).toBe(2)
    expect(parsed.errors[1].details.refColumn).toBe('bar')
  })
})

describe('PlannerError', () => {
  it('UNREACHABLE_TABLES with auto message', () => {
    const err = new PlannerError('UNREACHABLE_TABLES', 'users', {
      code: 'UNREACHABLE_TABLES',
      tables: ['analytics', 'logs'],
    })
    expect(err.code).toBe('UNREACHABLE_TABLES')
    expect(err.fromTable).toBe('users')
    expect(err.message).toBe('Unreachable tables: analytics, logs')
    expect(err.name).toBe('PlannerError')
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('TRINO_DISABLED with auto message', () => {
    const err = new PlannerError('TRINO_DISABLED', 'orders', { code: 'TRINO_DISABLED' })
    expect(err.message).toBe('Trino is disabled')
  })

  it('NO_CATALOG with auto message', () => {
    const err = new PlannerError('NO_CATALOG', 'orders', {
      code: 'NO_CATALOG',
      databases: ['analytics'],
    })
    expect(err.message).toBe('No Trino catalog for databases: analytics')
  })

  it('FRESHNESS_UNMET with auto message', () => {
    const err = new PlannerError('FRESHNESS_UNMET', 'orders', {
      code: 'FRESHNESS_UNMET',
      requiredFreshness: '1m',
      availableLag: '5m',
    })
    expect(err.message).toBe('Freshness unmet: required 1m, available lag 5m')
  })

  it('custom message overrides auto', () => {
    const err = new PlannerError('TRINO_DISABLED', 'users', { code: 'TRINO_DISABLED' }, 'custom msg')
    expect(err.message).toBe('custom msg')
  })

  it('toJSON() includes fromTable and details', () => {
    const err = new PlannerError('UNREACHABLE_TABLES', 't1', {
      code: 'UNREACHABLE_TABLES',
      tables: ['t2'],
    })
    const json = err.toJSON()
    expect(json.fromTable).toBe('t1')
    expect(json.details).toEqual({ code: 'UNREACHABLE_TABLES', tables: ['t2'] })
  })

  it('survives JSON.stringify()', () => {
    const err = new PlannerError('FRESHNESS_UNMET', 'x', {
      code: 'FRESHNESS_UNMET',
      requiredFreshness: '30s',
      availableLag: '2m',
    })
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.details.requiredFreshness).toBe('30s')
  })
})

describe('ExecutionError', () => {
  it('EXECUTOR_MISSING', () => {
    const err = new ExecutionError({ code: 'EXECUTOR_MISSING', database: 'analytics' })
    expect(err.code).toBe('EXECUTOR_MISSING')
    expect(err.message).toBe('Executor missing for database: analytics')
    expect(err.name).toBe('ExecutionError')
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('CACHE_PROVIDER_MISSING', () => {
    const err = new ExecutionError({ code: 'CACHE_PROVIDER_MISSING', cacheId: 'redis-1' })
    expect(err.code).toBe('CACHE_PROVIDER_MISSING')
    expect(err.message).toBe('Cache provider missing: redis-1')
  })

  it('QUERY_FAILED with cause', () => {
    const dbErr = new Error('relation "foo" does not exist')
    const err = new ExecutionError(
      {
        code: 'QUERY_FAILED',
        database: 'main',
        dialect: 'postgres',
        sql: 'SELECT * FROM foo',
        params: [1, 'bar'],
        cause: dbErr,
      },
      dbErr,
    )
    expect(err.code).toBe('QUERY_FAILED')
    expect(err.cause).toBe(dbErr)
    expect(err.message).toBe('Query failed on postgres database: main')
  })

  it('QUERY_TIMEOUT', () => {
    const err = new ExecutionError({
      code: 'QUERY_TIMEOUT',
      database: 'ch-analytics',
      dialect: 'clickhouse',
      sql: 'SELECT count(*) FROM events',
      timeoutMs: 30000,
    })
    expect(err.code).toBe('QUERY_TIMEOUT')
    expect(err.message).toBe('Query timeout on clickhouse database: ch-analytics (30000ms)')
  })

  it('toJSON() serializes QUERY_FAILED cause', () => {
    const dbErr = new Error('syntax error')
    const err = new ExecutionError(
      {
        code: 'QUERY_FAILED',
        database: 'db1',
        dialect: 'trino',
        sql: 'BAD SQL',
        params: [],
        cause: dbErr,
      },
      dbErr,
    )
    const json = err.toJSON()
    const details = json.details as { cause: { message: string } }
    expect(details.cause.message).toBe('syntax error')
  })

  it('survives JSON.stringify()', () => {
    const err = new ExecutionError(
      {
        code: 'QUERY_FAILED',
        database: 'pg1',
        dialect: 'postgres',
        sql: 'SELECT 1',
        params: ['x'],
        cause: new Error('oom'),
      },
      new Error('oom'),
    )
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.code).toBe('QUERY_FAILED')
    expect(parsed.details.sql).toBe('SELECT 1')
    expect(parsed.details.cause.message).toBe('oom')
    // top-level cause also serialized
    expect(parsed.cause.message).toBe('oom')
  })
})

describe('ProviderError', () => {
  it('METADATA_LOAD_FAILED', () => {
    const cause = new Error('file not found')
    const err = new ProviderError('METADATA_LOAD_FAILED', 'cannot load metadata', cause)
    expect(err.code).toBe('METADATA_LOAD_FAILED')
    expect(err.details).toEqual({ provider: 'metadata' })
    expect(err.cause).toBe(cause)
    expect(err.name).toBe('ProviderError')
    expect(err).toBeInstanceOf(MultiDbError)
  })

  it('ROLE_LOAD_FAILED', () => {
    const err = new ProviderError('ROLE_LOAD_FAILED', 'DB down')
    expect(err.code).toBe('ROLE_LOAD_FAILED')
    expect(err.details).toEqual({ provider: 'role' })
    expect(err.cause).toBeUndefined()
  })

  it('toJSON() serializes cause', () => {
    const cause = new Error('timeout')
    const err = new ProviderError('METADATA_LOAD_FAILED', 'fail', cause)
    const json = err.toJSON()
    expect(json.details).toEqual({ provider: 'metadata' })
    expect((json.cause as { message: string }).message).toBe('timeout')
  })

  it('survives JSON.stringify()', () => {
    const err = new ProviderError('ROLE_LOAD_FAILED', 'role err', new Error('conn refused'))
    const parsed = JSON.parse(JSON.stringify(err.toJSON()))
    expect(parsed.code).toBe('ROLE_LOAD_FAILED')
    expect(parsed.details.provider).toBe('role')
    expect(parsed.cause.message).toBe('conn refused')
  })
})
