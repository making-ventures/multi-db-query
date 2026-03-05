import { describe, expect, it } from 'vitest'
import type { DbExecutor, RoleMeta } from '../../src/index.js'
import { ExecutionError, ValidationError } from '../../src/index.js'
import type { SingleDbTable } from '../../src/singleDb.js'
import { createSingleDb } from '../../src/singleDb.js'

// ── Mock helpers ───────────────────────────────────────────────

function mockExecutor(rows: Record<string, unknown>[] = []): DbExecutor {
  return {
    execute: async () => rows,
    ping: async () => {},
    close: async () => {},
  }
}

// ── Fixtures ───────────────────────────────────────────────────

const tables: SingleDbTable[] = [
  {
    id: 'users',
    apiName: 'users',
    physicalName: 'public.users',
    columns: [
      { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
      { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
      { apiName: 'email', physicalName: 'email', type: 'string', nullable: false },
    ],
    primaryKey: ['id'],
    relations: [],
  },
  {
    id: 'orders',
    apiName: 'orders',
    physicalName: 'public.orders',
    columns: [
      { apiName: 'id', physicalName: 'id', type: 'int', nullable: false },
      { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false },
      { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
      { apiName: 'userId', physicalName: 'user_id', type: 'int', nullable: false },
    ],
    primaryKey: ['id'],
    relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
  },
]

const roles: RoleMeta[] = [
  { id: 'admin', tables: '*' },
  {
    id: 'viewer',
    tables: [{ tableId: 'users', allowedColumns: ['id', 'name'] }],
  },
]

const admin = { roles: { user: ['admin'] } }

// ── Tests ──────────────────────────────────────────────────────

describe('createSingleDb', () => {
  it('returns query results for a simple query', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor([{ id: 1, name: 'Alice' }]),
    })

    const result = await db.query({
      definition: { from: 'users' },
      context: admin,
    })

    expect(result.kind).toBe('data')
    if (result.kind === 'data') {
      expect(result.data).toEqual([{ id: 1, name: 'Alice' }])
    }
    await db.close()
  })

  it('validates unknown tables', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    await expect(db.query({ definition: { from: 'nonexistent' }, context: admin })).rejects.toThrow(ValidationError)

    await db.close()
  })

  it('validates unknown columns', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    await expect(db.query({ definition: { from: 'users', columns: ['bogus'] }, context: admin })).rejects.toThrow(
      ValidationError,
    )

    await db.close()
  })

  it('enforces role-based access', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    // viewer can see users but not orders
    await expect(
      db.query({ definition: { from: 'orders' }, context: { roles: { user: ['viewer'] } } }),
    ).rejects.toThrow(ValidationError)

    await db.close()
  })

  it('generates sql-only output', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    const result = await db.query({
      definition: { from: 'users', executeMode: 'sql-only' },
      context: admin,
    })

    expect(result.kind).toBe('sql')
    expect((result as { sql: string }).sql).toContain('SELECT')
    await db.close()
  })

  it('uses custom databaseId', async () => {
    const db = await createSingleDb({
      databaseId: 'my-pg',
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor([{ id: 1 }]),
    })

    const result = await db.query({
      definition: { from: 'users' },
      context: admin,
    })

    expect(result.kind).toBe('data')
    await db.close()
  })

  it('supports healthCheck', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    const health = await db.healthCheck()
    expect(health).toBeDefined()
    await db.close()
  })

  it('rejects queries after close', async () => {
    const db = await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor: mockExecutor(),
    })

    await db.close()

    await expect(db.query({ definition: { from: 'users' }, context: admin })).rejects.toThrow(ExecutionError)
  })

  it('skips connection validation when disabled', async () => {
    let pinged = false
    const executor: DbExecutor = {
      execute: async () => [],
      ping: async () => {
        pinged = true
      },
      close: async () => {},
    }

    await createSingleDb({
      engine: 'postgres',
      tables,
      roles,
      executor,
      validateConnection: false,
    })

    expect(pinged).toBe(false)
  })
})
