import { describe, expect, it } from 'vitest'
import type { TableMeta } from '../../src/types/metadata.js'
import { validateApiName, validateConfig } from '../../src/validation/configValidator.js'
import { eventsTable, ordersTable, usersTable, validConfig } from '../fixtures/testConfig.js'

// --- validateApiName ---

describe('validateApiName', () => {
  it('accepts valid camelCase names', () => {
    expect(validateApiName('users')).toBeNull()
    expect(validateApiName('orderItems')).toBeNull()
    expect(validateApiName('a')).toBeNull()
    expect(validateApiName('x1')).toBeNull()
  })

  it('rejects empty name', () => {
    expect(validateApiName('')).toContain('1–64 characters')
  })

  it('rejects name longer than 64 chars', () => {
    expect(validateApiName('a'.repeat(65))).toContain('1–64 characters')
  })

  it('rejects name starting with uppercase', () => {
    expect(validateApiName('Users')).toContain('^[a-z]')
  })

  it('rejects name with underscores', () => {
    expect(validateApiName('order_items')).toContain('^[a-z]')
  })

  it('rejects name starting with number', () => {
    expect(validateApiName('1user')).toContain('^[a-z]')
  })

  it('rejects reserved words', () => {
    expect(validateApiName('from')).toContain('reserved')
    expect(validateApiName('select')).toContain('reserved')
    expect(validateApiName('where')).toContain('reserved')
    expect(validateApiName('count')).toContain('reserved')
    expect(validateApiName('max')).toContain('reserved')
  })
})

// --- validateConfig ---

describe('validateConfig', () => {
  it('returns null for valid config', () => {
    expect(validateConfig(validConfig())).toBeNull()
  })

  // #49 — Invalid apiName format
  it('#49 invalid apiName format', () => {
    const config = validConfig()
    config.tables = [
      {
        ...usersTable,
        apiName: 'Order_Items',
      },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.code).toBe('CONFIG_INVALID')
    expect(err?.errors.some((e) => e.code === 'INVALID_API_NAME')).toBe(true)
  })

  // #50 — Duplicate apiName
  it('#50 duplicate table apiName', () => {
    const config = validConfig()
    const dup: TableMeta = {
      id: 'orders-dup',
      apiName: 'orders',
      database: 'pg-main',
      physicalName: 'orders_v2',
      columns: [{ apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false }],
      primaryKey: ['id'],
      relations: [],
    }
    config.tables = [...config.tables, dup]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'DUPLICATE_API_NAME')).toBe(true)
  })

  // #51 — Invalid DB reference
  it('#51 invalid database reference', () => {
    const config = validConfig()
    config.tables = [
      {
        ...usersTable,
        database: 'pg-other',
      },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_REFERENCE')).toBe(true)
    expect(err?.errors.some((e) => e.details.database === 'pg-other')).toBe(true)
  })

  // #52 — Invalid relation
  it('#52 invalid relation — non-existent table', () => {
    const config = validConfig()
    const badTable: TableMeta = {
      ...ordersTable,
      relations: [{ column: 'customerId', references: { table: 'invoiceLines', column: 'id' }, type: 'many-to-one' }],
    }
    config.tables = [usersTable, badTable, eventsTable]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_RELATION')).toBe(true)
  })

  it('#52 invalid relation — non-existent column', () => {
    const config = validConfig()
    const badTable: TableMeta = {
      ...ordersTable,
      relations: [{ column: 'customerId', references: { table: 'users', column: 'nonExistent' }, type: 'many-to-one' }],
    }
    config.tables = [usersTable, badTable, eventsTable]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_RELATION' && e.message.includes('nonExistent'))).toBe(true)
  })

  it('#52 invalid relation — source column does not exist', () => {
    const config = validConfig()
    const badTable: TableMeta = {
      ...ordersTable,
      relations: [{ column: 'noSuchCol', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
    }
    config.tables = [usersTable, badTable, eventsTable]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_RELATION' && e.message.includes('noSuchCol'))).toBe(true)
  })

  // #80 — Multiple config errors collected
  it('#80 multiple config errors collected', () => {
    const config = validConfig()
    // Invalid apiName + duplicate + broken reference
    config.tables = [
      { ...usersTable, apiName: 'Order_Bad' },
      { ...ordersTable, id: 'orders-dup', apiName: 'orders' },
      { ...ordersTable },
      { ...eventsTable, database: 'no-such-db' },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.length).toBeGreaterThanOrEqual(3)
    const codes = new Set(err?.errors.map((e) => e.code))
    expect(codes.has('INVALID_API_NAME')).toBe(true)
    expect(codes.has('DUPLICATE_API_NAME')).toBe(true)
    expect(codes.has('INVALID_REFERENCE')).toBe(true)
  })

  // #81 — Invalid sync reference
  it('#81 invalid sync — non-existent source table', () => {
    const config = validConfig()
    config.externalSyncs = [
      {
        sourceTable: 'nonExistent',
        targetDatabase: 'ch-analytics',
        targetPhysicalName: 'rep',
        method: 'debezium',
        estimatedLag: 'seconds',
      },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_SYNC')).toBe(true)
  })

  it('#81 invalid sync — non-existent target database', () => {
    const config = validConfig()
    config.externalSyncs = [
      {
        sourceTable: 'users',
        targetDatabase: 'no-such-db',
        targetPhysicalName: 'rep',
        method: 'debezium',
        estimatedLag: 'seconds',
      },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_SYNC' && e.details.database === 'no-such-db')).toBe(true)
  })

  // #89 — Invalid cache config
  it('#89 invalid cache — non-existent table', () => {
    const config = validConfig()
    config.caches = [{ id: 'c1', engine: 'redis', tables: [{ tableId: 'nonExistent', keyPattern: '{id}' }] }]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_CACHE' && e.details.cacheId === 'c1')).toBe(true)
  })

  it('#89 invalid cache — bad keyPattern placeholder', () => {
    const config = validConfig()
    config.caches = [{ id: 'c2', engine: 'redis', tables: [{ tableId: 'users', keyPattern: 'users:{badCol}' }] }]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_CACHE' && e.message.includes('badCol'))).toBe(true)
  })

  it('#89 invalid cache — non-existent cached column', () => {
    const config = validConfig()
    config.caches = [
      { id: 'c3', engine: 'redis', tables: [{ tableId: 'users', keyPattern: 'users:{id}', columns: ['id', 'noCol'] }] },
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'INVALID_CACHE' && e.message.includes('noCol'))).toBe(true)
  })

  // #96 — Duplicate column apiName
  it('#96 duplicate column apiName within table', () => {
    const config = validConfig()
    config.tables = [
      {
        ...usersTable,
        columns: [
          ...usersTable.columns,
          { apiName: 'firstName', physicalName: 'display_name', type: 'string', nullable: false },
        ],
      },
      ordersTable,
      eventsTable,
    ]
    const err = validateConfig(config)
    expect(err).not.toBeNull()
    expect(err?.errors.some((e) => e.code === 'DUPLICATE_API_NAME' && e.details.actual === 'firstName')).toBe(true)
  })
})
