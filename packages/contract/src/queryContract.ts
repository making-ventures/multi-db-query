import type { ExecutionContext, QueryDefinition, QueryResult } from '@mkven/multi-db-validation'
import { ValidationError } from '@mkven/multi-db-validation'
import { beforeAll, describe, expect, it } from 'vitest'

// ── QueryContract ──────────────────────────────────────────────

export interface QueryContract {
  query<T = unknown>(input: { definition: QueryDefinition; context: ExecutionContext }): Promise<QueryResult<T>>
}

// ── Helpers ────────────────────────────────────────────────────

const admin: ExecutionContext = { roles: { user: ['admin'] } }
const tenantUser: ExecutionContext = { roles: { user: ['tenant-user'] } }
const analyst: ExecutionContext = { roles: { user: ['analyst'] } }
const noAccess: ExecutionContext = { roles: { user: ['no-access'] } }

function hasErrorCode(err: unknown, code: string): boolean {
  if (err instanceof ValidationError) {
    return err.errors.some((e) => e.code === code)
  }
  return false
}

async function expectValidationError(
  engine: QueryContract,
  definition: QueryDefinition,
  context: ExecutionContext,
  code: string,
): Promise<void> {
  try {
    await engine.query({ definition, context })
    expect.fail(`Expected ValidationError with code ${code}`)
  } catch (err) {
    expect(err).toBeInstanceOf(ValidationError)
    expect(hasErrorCode(err, code)).toBe(true)
  }
}

// ── describeQueryContract ──────────────────────────────────────

export function describeQueryContract(name: string, factory: () => Promise<QueryContract>): void {
  describe(`QueryContract: ${name}`, () => {
    let engine: QueryContract

    beforeAll(async () => {
      engine = await factory()
    })

    // ── 1.1 Data Mode ────────────────────────────────────────

    describe('1.1 Data Mode', () => {
      it('C001: default execute mode returns data', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(Array.isArray(r.data)).toBe(true)
          expect(r.data.length).toBeGreaterThanOrEqual(1)
          for (const row of r.data) {
            expect(row).toHaveProperty('id')
            expect(row).toHaveProperty('status')
          }
        }
      })

      it('C002: data result includes correct meta.columns', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns).toHaveLength(2)
          for (const c of r.meta.columns) {
            expect(c).toHaveProperty('apiName')
            expect(c).toHaveProperty('type')
            expect(c).toHaveProperty('nullable')
            expect(c).toHaveProperty('fromTable')
            expect(c.masked).toBe(false)
          }
        }
      })

      it('C003: data result includes meta.timing', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.timing.planningMs).toBeGreaterThanOrEqual(0)
          expect(r.meta.timing.generationMs).toBeGreaterThanOrEqual(0)
          expect(r.meta.timing.executionMs).toBeGreaterThanOrEqual(0)
        }
      })

      it('C004: data result includes meta.strategy', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(['direct', 'cache', 'materialized', 'trino-cross-db']).toContain(r.meta.strategy)
        }
      })

      it('C005: data result includes meta.tablesUsed', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.tablesUsed.length).toBeGreaterThanOrEqual(1)
          for (const t of r.meta.tablesUsed) {
            expect(t).toHaveProperty('tableId')
            expect(t).toHaveProperty('source')
            expect(t).toHaveProperty('database')
            expect(t).toHaveProperty('physicalName')
          }
        }
      })

      it('C006: omitting columns returns all allowed', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns.length).toBe(11) // orders has 11 columns
        }
      })

      it('C007: no debugLog when debug is omitted', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'status'] }, context: admin })
        expect(r.debugLog).toBeUndefined()
      })
    })

    // ── 1.2 SQL-Only Mode ────────────────────────────────────

    describe('1.2 SQL-Only Mode', () => {
      it('C010: sql-only returns SqlResult', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id'], executeMode: 'sql-only' },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.sql).toContain('SELECT')
          expect(Array.isArray(r.params)).toBe(true)
        }
      })

      it('C011: sql-only has no data field', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id'], executeMode: 'sql-only' },
          context: admin,
        })
        expect((r as unknown as Record<string, unknown>).data).toBeUndefined()
      })

      it('C012: sql-only includes meta.columns', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id'], executeMode: 'sql-only' },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.meta.columns).toHaveLength(1)
          expect(r.meta.columns[0]?.apiName).toBe('id')
        }
      })

      it('C013: sql-only has no executionMs', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id'], executeMode: 'sql-only' },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.meta.timing.executionMs).toBeUndefined()
          expect(r.meta.timing.planningMs).toBeGreaterThanOrEqual(0)
          expect(r.meta.timing.generationMs).toBeGreaterThanOrEqual(0)
        }
      })

      it('C014: sql-only with filters produces parameterized SQL', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: ['id'],
            filters: [{ column: 'status', operator: '=', value: 'active' }],
            executeMode: 'sql-only',
          },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.params.length).toBeGreaterThanOrEqual(1)
        }
      })

      it('C015: sql-only masking reported in meta', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total'], executeMode: 'sql-only' },
          context: tenantUser,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(true)
        }
      })

      it('C016: sql-only with join', async () => {
        const r = await engine.query({
          definition: { from: 'orders', joins: [{ table: 'products' }], columns: ['id'], executeMode: 'sql-only' },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.sql).toContain('JOIN')
          expect(r.meta.tablesUsed.length).toBe(2)
        }
      })
    })

    // ── 1.3 Count Mode ───────────────────────────────────────

    describe('1.3 Count Mode', () => {
      it('C020: count mode returns CountResult', async () => {
        const r = await engine.query({ definition: { from: 'orders', executeMode: 'count' }, context: admin })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(typeof r.count).toBe('number')
          expect(r.count).toBeGreaterThanOrEqual(5)
        }
      })

      it('C021: count mode has empty meta.columns', async () => {
        const r = await engine.query({ definition: { from: 'orders', executeMode: 'count' }, context: admin })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.meta.columns).toEqual([])
        }
      })

      it('C022: count with filter', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            filters: [{ column: 'status', operator: '=', value: 'active' }],
            executeMode: 'count',
          },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.count).toBeGreaterThanOrEqual(2)
        }
      })

      it('C023: count ignores groupBy/aggregations', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
            executeMode: 'count',
          },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.count).toBeGreaterThanOrEqual(5) // scalar count, not number of groups (4)
        }
      })

      it('C024: count ignores orderBy, limit, offset', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            orderBy: [{ column: 'id', direction: 'asc' }],
            limit: 2,
            offset: 1,
            executeMode: 'count',
          },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.count).toBeGreaterThanOrEqual(5)
        }
      })

      it('C025: count with join', async () => {
        const r = await engine.query({
          definition: { from: 'orders', joins: [{ table: 'products' }], executeMode: 'count' },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(typeof r.count).toBe('number')
        }
      })

      it('C026: count with restricted role', async () => {
        const r = await engine.query({
          definition: { from: 'orders', executeMode: 'count' },
          context: tenantUser,
        })
        expect(r.kind).toBe('count')
      })

      it('C027: count with zero matching rows', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            filters: [{ column: 'status', operator: '=', value: 'nonexistent' }],
            executeMode: 'count',
          },
          context: admin,
        })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.count).toBe(0)
        }
      })
    })

    // ── 2. Debug Mode ────────────────────────────────────────

    describe('2. Debug Mode', () => {
      it('C030: debug: true includes debugLog', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id'], debug: true }, context: admin })
        expect(Array.isArray(r.debugLog)).toBe(true)
        expect(r.debugLog?.length).toBeGreaterThan(0)
      })

      it('C031: debugLog entries have required fields', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id'], debug: true }, context: admin })
        for (const entry of r.debugLog ?? []) {
          expect(typeof entry.timestamp).toBe('number')
          expect(typeof entry.phase).toBe('string')
          expect(typeof entry.message).toBe('string')
        }
      })

      it('C032: debugLog covers pipeline phases', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id'], debug: true }, context: admin })
        const phases = r.debugLog?.map((e) => e.phase) ?? []
        expect(phases).toContain('validation')
        expect(phases).toContain('access-control')
        expect(phases).toContain('planning')
        expect(phases).toContain('name-resolution')
        expect(phases).toContain('sql-generation')
      })

      it('C033: debug works with sql-only', async () => {
        const r = await engine.query({
          definition: { from: 'orders', executeMode: 'sql-only', debug: true },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        expect(Array.isArray(r.debugLog)).toBe(true)
      })

      it('C034: debug works with count', async () => {
        const r = await engine.query({
          definition: { from: 'orders', executeMode: 'count', debug: true },
          context: admin,
        })
        expect(r.kind).toBe('count')
        expect(Array.isArray(r.debugLog)).toBe(true)
      })
    })

    // ── 10.1 Role-Based Permissions ──────────────────────────

    describe('10.1 Role-Based Permissions', () => {
      it('C700: admin sees all columns', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns.length).toBe(11)
        }
      })

      it('C701: restricted role sees subset', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total', 'status'] },
          context: tenantUser,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns.length).toBe(3)
        }
      })

      it('C702: omitting columns uses role-allowed set', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: tenantUser })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const names = r.meta.columns.map((c) => c.apiName).sort()
          expect(names).toEqual(['createdAt', 'id', 'status', 'total'])
        }
      })

      it('C703: access denied on table (negative)', async () => {
        await expectValidationError(engine, { from: 'events' }, tenantUser, 'ACCESS_DENIED')
      })

      it('C704: access denied on column (negative)', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', columns: ['id', 'internalNote'] },
          tenantUser,
          'ACCESS_DENIED',
        )
      })

      it('C705: no-access role (negative)', async () => {
        await expectValidationError(engine, { from: 'orders' }, noAccess, 'ACCESS_DENIED')
      })

      it('C706: empty roles array (negative)', async () => {
        await expectValidationError(engine, { from: 'orders' }, { roles: { user: [] } }, 'ACCESS_DENIED')
      })

      it('C707: access denied on joined table (negative)', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', joins: [{ table: 'events' }] },
          tenantUser,
          'ACCESS_DENIED',
        )
      })
    })

    // ── 10.2 Multi-Role (Union Within Scope) ─────────────────

    describe('10.2 Multi-Role Union', () => {
      it('C710: union of two user roles', async () => {
        const r = await engine.query({
          definition: { from: 'orders' },
          context: { roles: { user: ['tenant-user', 'admin'] } },
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns.length).toBe(11)
        }
      })

      it('C711: union adds permissions', async () => {
        const r = await engine.query({
          definition: { from: 'orders' },
          context: { roles: { user: ['tenant-user', 'viewer'] } },
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const names = r.meta.columns.map((c) => c.apiName).sort()
          expect(names).toEqual(['createdAt', 'id', 'quantity', 'status', 'total'])
        }
      })
    })

    // ── 10.3 Cross-Scope (Intersection) ──────────────────────

    describe('10.3 Cross-Scope Intersection', () => {
      it('C720: admin user + service restriction', async () => {
        const r = await engine.query({
          definition: { from: 'orders', joins: [{ table: 'users' }] },
          context: { roles: { user: ['admin'], service: ['orders-service'] } },
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const userCols = r.meta.columns
            .filter((c) => c.fromTable === 'users')
            .map((c) => c.apiName)
            .sort()
          // id is qualified as 'users.id' due to collision with orders.id
          expect(userCols).toEqual(['firstName', 'lastName', 'users.id'])
        }
      })

      it('C721: empty scope intersection (negative)', async () => {
        await expectValidationError(
          engine,
          { from: 'events' },
          { roles: { user: ['tenant-user'], service: ['orders-service'] } },
          'ACCESS_DENIED',
        )
      })

      it('C722: omitted scope = no restriction', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: { roles: { user: ['admin'] } } })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.columns.length).toBe(11)
        }
      })

      it('C723: one scope with zero roles (negative)', async () => {
        await expectValidationError(
          engine,
          { from: 'orders' },
          { roles: { user: [], service: ['orders-service'] } },
          'ACCESS_DENIED',
        )
      })

      it('C724: multi-scope service has no table access (negative)', async () => {
        // reporting-service has no users access → service scope denies users → ACCESS_DENIED
        await expectValidationError(
          engine,
          { from: 'users', columns: ['id'] },
          { roles: { user: ['tenant-user'], service: ['reporting-service'] } },
          'ACCESS_DENIED',
        )
      })

      it('C725: multi-scope wildcard ∩ specific restricts to specific', async () => {
        // admin (wildcard) intersected with viewer (users: [id, firstName]) → restricted
        // requesting 'email' which is outside viewer → ACCESS_DENIED
        await expectValidationError(
          engine,
          { from: 'users', columns: ['id', 'email'] },
          { roles: { user: ['admin'], service: ['viewer'] } },
          'ACCESS_DENIED',
        )
      })
    })

    // ── 11. Column Masking ───────────────────────────────────

    describe('11. Column Masking', () => {
      it('C800: masked column reported in meta', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'total'] }, context: tenantUser })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(true)
          const idCol = r.meta.columns.find((c) => c.apiName === 'id')
          expect(idCol?.masked).toBe(false)
        }
      })

      it('C801: admin sees unmasked', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'total'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(false)
        }
      })

      it('C802: masked value is obfuscated (number)', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['total'] }, context: tenantUser })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            expect((row as Record<string, unknown>).total).toBe(0)
          }
        }
      })

      it('C803: masked value (full)', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'internalNote'] },
          context: analyst,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const note = (row as Record<string, unknown>).internalNote
            if (note !== null) {
              expect(note).toBe('***')
            }
          }
        }
      })

      it('C804: masking on email column', async () => {
        const r = await engine.query({ definition: { from: 'users', columns: ['email'] }, context: tenantUser })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const email = (row as Record<string, unknown>).email as string
            expect(email).toContain('***')
          }
        }
      })

      it('C805: aggregation alias never masked', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
          },
          context: tenantUser,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const agg = r.meta.columns.find((c) => c.apiName === 'totalSum')
          expect(agg?.masked).toBe(false)
        }
      })

      it('C806: sql-only reports masking intent', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total'], executeMode: 'sql-only' },
          context: tenantUser,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(true)
        }
      })

      it('C807: multi-role masking (union unmasks)', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total'] },
          context: { roles: { user: ['tenant-user', 'admin'] } },
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(false)
        }
      })

      it('C808: cross-scope masking preserved', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total'] },
          context: { roles: { user: ['admin'], service: ['reporting-service'] } },
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.masked).toBe(true)
        }
      })

      it('C809: masked value (phone)', async () => {
        const r = await engine.query({ definition: { from: 'users', columns: ['id', 'phone'] }, context: analyst })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const phone = (row as Record<string, unknown>).phone
            if (phone !== null) {
              expect(typeof phone).toBe('string')
              expect(phone as string).toContain('***')
            }
          }
        }
      })

      it('C810: masked value (name)', async () => {
        const r = await engine.query({
          definition: { from: 'users', columns: ['id', 'firstName', 'lastName'] },
          context: analyst,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const fn = (row as Record<string, unknown>).firstName as string
            // name masking: first char + stars + last char (e.g. A***e, B*b)
            expect(fn).toContain('*')
          }
        }
      })

      it('C811: masked value (number on price)', async () => {
        const r = await engine.query({ definition: { from: 'products', columns: ['id', 'price'] }, context: analyst })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            expect((row as Record<string, unknown>).price).toBe(0)
          }
        }
      })

      it('C812: masked value (number on amount)', async () => {
        const r = await engine.query({ definition: { from: 'invoices', columns: ['id', 'amount'] }, context: analyst })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            expect((row as Record<string, unknown>).amount).toBe(0)
          }
        }
      })

      it('C813: multiple masking functions in one query', async () => {
        const r = await engine.query({
          definition: { from: 'users', columns: ['id', 'email', 'phone', 'firstName'] },
          context: analyst,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const emailCol = r.meta.columns.find((c) => c.apiName === 'email')
          const phoneCol = r.meta.columns.find((c) => c.apiName === 'phone')
          const fnCol = r.meta.columns.find((c) => c.apiName === 'firstName')
          expect(emailCol?.masked).toBe(false) // analyst has no email masking
          expect(phoneCol?.masked).toBe(true)
          expect(fnCol?.masked).toBe(true)
        }
      })

      it('C814: masked value (date)', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'createdAt'] }, context: analyst })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const ca = (row as Record<string, unknown>).createdAt as string
            expect(ca).toMatch(/-01-01/)
          }
        }
      })

      it('C815: masking on null value', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'internalNote'] },
          context: analyst,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const nullRows = r.data.filter((row) => {
            const id = (row as Record<string, unknown>).id
            return id === 2 || id === 4
          })
          for (const row of nullRows) {
            expect((row as Record<string, unknown>).internalNote).toBeNull()
          }
        }
      })

      it('C816: masked value (uuid)', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'customerId'] },
          context: analyst,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          for (const row of r.data) {
            const cid = (row as Record<string, unknown>).customerId as string
            expect(cid).toContain('****')
          }
        }
      })
    })

    // ── 12. Validation Errors ────────────────────────────────

    describe('12. Validation Errors', () => {
      // 12.1 Table & Column
      it('C900: unknown table', async () => {
        await expectValidationError(engine, { from: 'nonExistentTable' }, admin, 'UNKNOWN_TABLE')
      })

      it('C901: unknown column', async () => {
        await expectValidationError(engine, { from: 'orders', columns: ['nonexistent'] }, admin, 'UNKNOWN_COLUMN')
      })

      it('C902: unknown column in filter', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'nonexistent', operator: '=', value: 'x' }] },
          admin,
          'UNKNOWN_COLUMN',
        )
      })

      it('C903: unknown column on joined table', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            joins: [{ table: 'products' }],
            filters: [{ column: 'nonexistent', table: 'products', operator: '=', value: 'x' }],
          },
          admin,
          'UNKNOWN_COLUMN',
        )
      })

      // 12.2 Filter Validity
      it('C910: > on uuid column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'customerId', operator: '>', value: 'x' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C911: > on boolean column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'isPaid', operator: '>', value: true }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C912: in on boolean column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'isPaid', operator: 'in', value: [true, false] }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C913: in on date column', async () => {
        await expectValidationError(
          engine,
          { from: 'invoices', filters: [{ column: 'dueDate', operator: 'in', value: ['2024-01-01'] }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C914: in on timestamp column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'createdAt', operator: 'in', value: ['2024-01-01T00:00:00Z'] }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C915: notIn on date column', async () => {
        await expectValidationError(
          engine,
          { from: 'invoices', filters: [{ column: 'dueDate', operator: 'notIn', value: ['2024-01-01'] }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C916: notIn on boolean column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'isPaid', operator: 'notIn', value: [true] }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C917: like on int column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'quantity', operator: 'like', value: '%x%' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C918: contains on decimal column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'contains', value: '100' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C919: levenshteinLte on decimal column', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            filters: [{ column: 'total', operator: 'levenshteinLte', value: { text: '100', maxDistance: 2 } }],
          },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C920: between on boolean column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'isPaid', operator: 'between', value: { from: true, to: false } }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C921: between on uuid column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'customerId', operator: 'between', value: { from: 'a', to: 'z' } }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C922: notBetween on boolean column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'isPaid', operator: 'notBetween', value: { from: true, to: false } }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C923: notBetween on uuid column', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            filters: [{ column: 'customerId', operator: 'notBetween', value: { from: 'a', to: 'z' } }],
          },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C924: isNull on non-nullable column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'id', operator: 'isNull', value: null }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C925: isNotNull on non-nullable column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'id', operator: 'isNotNull', value: null }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C926: arrayContains on scalar column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'status', operator: 'arrayContains', value: 'active' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C927: scalar operator on array column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'priorities', operator: '=', value: 1 }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C928: filter table references non-joined table', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'name', table: 'products', operator: '=', value: 'x' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C929: filter on access-denied column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'internalNote', operator: '=', value: 'x' }] },
          tenantUser,
          'ACCESS_DENIED',
        )
      })

      // 12.3 Value Validity
      it('C930: between missing to', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'between', value: { from: 0 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C931: notBetween missing to', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'notBetween', value: { from: 0 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C932: levenshteinLte negative maxDistance', async () => {
        await expectValidationError(
          engine,
          {
            from: 'users',
            filters: [{ column: 'firstName', operator: 'levenshteinLte', value: { text: 'x', maxDistance: -1 } }],
          },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C933: levenshteinLte fractional maxDistance', async () => {
        await expectValidationError(
          engine,
          {
            from: 'users',
            filters: [{ column: 'firstName', operator: 'levenshteinLte', value: { text: 'x', maxDistance: 1.5 } }],
          },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C934: in with empty array', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'status', operator: 'in', value: [] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C935: in with type-mismatched elements', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'quantity', operator: 'in', value: ['abc'] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C936: in with null element', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'status', operator: 'in', value: [null] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C937: between with null from', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'between', value: { from: null, to: 100 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C938: between with null to', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'between', value: { from: 0, to: null } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C939: between with type-mismatched bounds', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'between', value: { from: 'abc', to: 100 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C940: arrayContains type mismatch', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'priorities', operator: 'arrayContains', value: 'notAnInt' }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C941: arrayContainsAll empty array', async () => {
        await expectValidationError(
          engine,
          { from: 'products', filters: [{ column: 'labels', operator: 'arrayContainsAll', value: [] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C942: arrayContainsAny type mismatch', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'priorities', operator: 'arrayContainsAny', value: ['notAnInt'] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C943: arrayContainsAll with null element', async () => {
        await expectValidationError(
          engine,
          { from: 'products', filters: [{ column: 'labels', operator: 'arrayContainsAll', value: [null] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C944: notIn with empty array', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'status', operator: 'notIn', value: [] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C945: notIn with type-mismatched elements', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'quantity', operator: 'notIn', value: ['abc'] }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C946: between missing from', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: 'between', value: { to: 100 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      it('C947: levenshteinLte missing text field', async () => {
        await expectValidationError(
          engine,
          { from: 'users', filters: [{ column: 'firstName', operator: 'levenshteinLte', value: { maxDistance: 2 } }] },
          admin,
          'INVALID_VALUE',
        )
      })

      // 12.4 Column Filter Validity
      it('C950: column filter type mismatch', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'total', operator: '>', refColumn: 'status' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C951: column filter on denied column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'id', operator: '>', refColumn: 'quantity' }] },
          tenantUser,
          'ACCESS_DENIED',
        )
      })

      it('C952: column filter non-existent refColumn', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'id', operator: '>', refColumn: 'nonexistent' }] },
          admin,
          'UNKNOWN_COLUMN',
        )
      })

      it('C953: column filter on array column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ column: 'priorities', operator: '>', refColumn: 'quantity' }] },
          admin,
          'INVALID_FILTER',
        )
      })

      it('C954: column filter same numeric family (int > decimal)', async () => {
        // int and decimal are in the same numeric family — should pass
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: ['id'],
            filters: [{ column: 'total', operator: '>', refColumn: 'quantity' }],
            executeMode: 'sql-only',
          },
          context: admin,
        })
        expect(r.kind).toBe('sql')
      })

      it('C955: column filter same temporal family (timestamp > date)', async () => {
        // timestamp and date both in temporal family — should pass
        const r = await engine.query({
          definition: {
            from: 'invoices',
            columns: ['id'],
            filters: [{ column: 'issuedAt', operator: '>', refColumn: 'dueDate' }],
            executeMode: 'sql-only',
          },
          context: admin,
        })
        expect(r.kind).toBe('sql')
      })

      // 12.5 Join Validity
      it('C960: join with no relation defined', async () => {
        await expectValidationError(engine, { from: 'products', joins: [{ table: 'invoices' }] }, admin, 'INVALID_JOIN')
      })

      it('C961: join to table with no role access', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', joins: [{ table: 'events' }] },
          tenantUser,
          'ACCESS_DENIED',
        )
      })

      it('C962: transitive join with no path (3rd table unrelated)', async () => {
        // products has no relation to orders or users — no transitive path
        await expectValidationError(
          engine,
          { from: 'users', joins: [{ table: 'orders' }, { table: 'products' }] },
          admin,
          'INVALID_JOIN',
        )
      })

      // 12.6 GroupBy Validity
      it('C970: column in SELECT not in groupBy', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['id', 'status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
          },
          admin,
          'INVALID_GROUP_BY',
        )
      })

      it('C971: array column in groupBy', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: [],
            groupBy: [{ column: 'priorities' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
          },
          admin,
          'INVALID_GROUP_BY',
        )
      })

      it('C972: groupBy table references non-joined table', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: [],
            groupBy: [{ column: 'name', table: 'products' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
          },
          admin,
          'INVALID_GROUP_BY',
        )
      })

      // 12.7 Having Validity
      it('C975: HAVING on non-existent alias', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
            having: [{ column: 'nonexistent', operator: '>', value: 0 }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C976: table qualifier in HAVING filter', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'totalSum' }],
            having: [{ column: 'totalSum', table: 'orders', operator: '>', value: 0 }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C977: QueryColumnFilter in HAVING group', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
            // @ts-expect-error intentional — QueryColumnFilter not allowed in having
            having: [{ column: 'x', operator: '>', refColumn: 'total' }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C978: EXISTS in HAVING', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
            // @ts-expect-error intentional — QueryExistsFilter not allowed in having
            having: [{ table: 'products', exists: true }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C979: contains operator in HAVING', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
            having: [{ column: 'x', operator: 'contains', value: 'abc' }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C980: levenshteinLte in HAVING', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
            having: [{ column: 'x', operator: 'levenshteinLte', value: { text: 'abc', maxDistance: 1 } }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C981: arrayContains in HAVING', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }],
            having: [{ column: 'x', operator: 'arrayContains', value: 1 }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C982: top-level QueryColumnFilter in HAVING (no group)', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [
              { column: '*', fn: 'count', alias: 'cnt' },
              { column: 'total', fn: 'sum', alias: 'totalSum' },
            ],
            // @ts-expect-error — refColumn not valid in having
            having: [{ column: 'cnt', operator: '>', refColumn: 'totalSum' }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      it('C983: top-level QueryExistsFilter in HAVING (no group)', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
            // @ts-expect-error — exists not valid in having
            having: [{ table: 'users', exists: true }],
          },
          admin,
          'INVALID_HAVING',
        )
      })

      // 12.8 OrderBy Validity
      it('C985: orderBy on non-joined table column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', orderBy: [{ column: 'name', table: 'products', direction: 'asc' }] },
          admin,
          'INVALID_ORDER_BY',
        )
      })

      it('C986: array column in orderBy', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', orderBy: [{ column: 'priorities', direction: 'asc' }] },
          admin,
          'INVALID_ORDER_BY',
        )
      })

      it('C987: orderBy table references non-joined table', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', orderBy: [{ column: 'name', table: 'samples', direction: 'asc' }] },
          admin,
          'INVALID_ORDER_BY',
        )
      })

      // 12.9 byIds Validity
      it('C990: empty byIds array', async () => {
        await expectValidationError(engine, { from: 'orders', byIds: [] }, admin, 'INVALID_BY_IDS')
      })

      it('C991: byIds + aggregations', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', byIds: [1], aggregations: [{ column: 'total', fn: 'sum', alias: 'x' }] },
          admin,
          'INVALID_BY_IDS',
        )
      })

      it('C992: byIds scalar on composite PK', async () => {
        await expectValidationError(engine, { from: 'orderItems', byIds: [1, 2] }, admin, 'INVALID_BY_IDS')
      })

      it('C994: byIds + groupBy', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', byIds: [1], groupBy: [{ column: 'status' }] },
          admin,
          'INVALID_BY_IDS',
        )
      })

      // 12.10 Limit/Offset Validity
      it('C995: negative limit', async () => {
        await expectValidationError(engine, { from: 'orders', limit: -1 }, admin, 'INVALID_LIMIT')
      })

      it('C996: offset without limit', async () => {
        await expectValidationError(engine, { from: 'orders', offset: 5 }, admin, 'INVALID_LIMIT')
      })

      it('C997: negative offset', async () => {
        await expectValidationError(engine, { from: 'orders', limit: 10, offset: -1 }, admin, 'INVALID_LIMIT')
      })

      it('C998: fractional limit', async () => {
        await expectValidationError(engine, { from: 'orders', limit: 2.5 }, admin, 'INVALID_LIMIT')
      })

      // 12.11 Aggregation Validity
      it('C1000: duplicate aggregation alias', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: [],
            aggregations: [
              { column: 'total', fn: 'sum', alias: 'x' },
              { column: 'quantity', fn: 'sum', alias: 'x' },
            ],
          },
          admin,
          'INVALID_AGGREGATION',
        )
      })

      it('C1001: alias collides with column apiName', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: ['status'],
            groupBy: [{ column: 'status' }],
            aggregations: [{ column: 'total', fn: 'sum', alias: 'status' }],
          },
          admin,
          'INVALID_AGGREGATION',
        )
      })

      it('C1002: empty columns [] without aggregations', async () => {
        await expectValidationError(engine, { from: 'orders', columns: [] }, admin, 'INVALID_AGGREGATION')
      })

      it('C1003: SUM on array column', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', columns: [], aggregations: [{ column: 'priorities', fn: 'sum', alias: 'x' }] },
          admin,
          'INVALID_AGGREGATION',
        )
      })

      it('C1004: aggregation table references non-joined table', async () => {
        await expectValidationError(
          engine,
          {
            from: 'orders',
            columns: [],
            aggregations: [{ column: 'price', table: 'products', fn: 'sum', alias: 'x' }],
          },
          admin,
          'INVALID_AGGREGATION',
        )
      })

      it('C1005: aggregation column does not exist', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', columns: [], aggregations: [{ column: 'nonexistent', fn: 'sum', alias: 'x' }] },
          admin,
          'UNKNOWN_COLUMN',
        )
      })

      // 12.12 EXISTS Validity
      it('C1010: EXISTS on unrelated table', async () => {
        await expectValidationError(
          engine,
          { from: 'products', filters: [{ table: 'invoices' }] },
          admin,
          'INVALID_EXISTS',
        )
      })

      it('C1011: counted EXISTS with negative count value', async () => {
        await expectValidationError(
          engine,
          { from: 'samples', filters: [{ table: 'sampleItems', count: { operator: '>=', value: -1 } }] },
          admin,
          'INVALID_EXISTS',
        )
      })

      it('C1012: counted EXISTS with fractional count value', async () => {
        await expectValidationError(
          engine,
          { from: 'samples', filters: [{ table: 'sampleItems', count: { operator: '>=', value: 1.5 } }] },
          admin,
          'INVALID_EXISTS',
        )
      })

      it('C1013: nested EXISTS invalid inner relation', async () => {
        await expectValidationError(
          engine,
          { from: 'orders', filters: [{ table: 'products', filters: [{ table: 'users' }] }] },
          admin,
          'INVALID_EXISTS',
        )
      })

      // 12.13 Role Validity
      it('C1020: unknown role ID', async () => {
        await expectValidationError(engine, { from: 'orders' }, { roles: { user: ['nonexistent'] } }, 'UNKNOWN_ROLE')
      })

      // 12.14 Multi-Error Collection
      it('C1030: multiple errors collected', async () => {
        try {
          await engine.query({
            definition: {
              from: 'nonExistentTable',
              columns: ['bad'],
              filters: [{ column: 'missing', operator: '=', value: 'x' }],
            },
            context: admin,
          })
          expect.fail('Expected ValidationError')
        } catch (err) {
          expect(err).toBeInstanceOf(ValidationError)
          if (err instanceof ValidationError) {
            expect(err.errors.length).toBeGreaterThanOrEqual(1)
          }
        }
      })
    })

    // ── 13. Query Result Meta Verification ───────────────────

    describe('13. Meta Verification', () => {
      it('C1100: meta.columns type correctness', async () => {
        const r = await engine.query({
          definition: { from: 'orders', columns: ['id', 'total', 'status'] },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const idCol = r.meta.columns.find((c) => c.apiName === 'id')
          expect(idCol?.type).toBe('int')
          const totalCol = r.meta.columns.find((c) => c.apiName === 'total')
          expect(totalCol?.type).toBe('decimal')
          const statusCol = r.meta.columns.find((c) => c.apiName === 'status')
          expect(statusCol?.type).toBe('string')
        }
      })

      it('C1101: meta.columns nullable correctness', async () => {
        const r = await engine.query({ definition: { from: 'orders', columns: ['id', 'productId'] }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const idCol = r.meta.columns.find((c) => c.apiName === 'id')
          expect(idCol?.nullable).toBe(false)
          const pidCol = r.meta.columns.find((c) => c.apiName === 'productId')
          expect(pidCol?.nullable).toBe(true)
        }
      })

      it('C1102: meta.columns fromTable', async () => {
        const r = await engine.query({
          definition: { from: 'orders', joins: [{ table: 'products' }], columns: ['id', 'status'] },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const ordersCols = r.meta.columns.filter((c) => c.fromTable === 'orders')
          expect(ordersCols.length).toBeGreaterThan(0)
          const productsCols = r.meta.columns.filter((c) => c.fromTable === 'products')
          expect(productsCols.length).toBeGreaterThan(0)
        }
      })

      it('C1103: meta.columns for aggregations', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: [],
            aggregations: [
              { column: 'total', fn: 'sum', alias: 'totalSum' },
              { column: '*', fn: 'count', alias: 'cnt' },
            ],
          },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const sumCol = r.meta.columns.find((c) => c.apiName === 'totalSum')
          expect(sumCol?.type).toBe('decimal')
          expect(sumCol?.fromTable).toBe('orders')
          expect(sumCol?.masked).toBe(false)
          const cntCol = r.meta.columns.find((c) => c.apiName === 'cnt')
          expect(cntCol?.type).toBe('int')
        }
      })

      it('C1104: AVG always returns decimal', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: [],
            aggregations: [{ column: 'quantity', fn: 'avg', alias: 'avgQty' }],
          },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const col = r.meta.columns.find((c) => c.apiName === 'avgQty')
          expect(col?.type).toBe('decimal')
        }
      })

      it('C1105: meta.tablesUsed for single table', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.tablesUsed).toHaveLength(1)
          expect(r.meta.tablesUsed[0]?.tableId).toBe('orders')
          expect(r.meta.tablesUsed[0]?.source).toBe('original')
          expect(r.meta.tablesUsed[0]?.database).toBe('pg-main')
        }
      })

      it('C1106: meta.tablesUsed for join', async () => {
        const r = await engine.query({
          definition: { from: 'orders', joins: [{ table: 'products' }] },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.tablesUsed).toHaveLength(2)
        }
      })

      it('C1107: meta.columns for count mode', async () => {
        const r = await engine.query({ definition: { from: 'orders', executeMode: 'count' }, context: admin })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(r.meta.columns).toEqual([])
        }
      })

      it('C1108: meta.dialect present (data mode)', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(['postgres', 'clickhouse', 'trino']).toContain(r.meta.dialect)
        }
      })

      it('C1109: meta.targetDatabase for direct query', async () => {
        const r = await engine.query({ definition: { from: 'orders' }, context: admin })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          expect(r.meta.targetDatabase).toBe('pg-main')
        }
      })

      it('C1110: meta.targetDatabase for cross-DB query (sql-only)', async () => {
        // events (ch-analytics) + users (pg-main), no sync → Trino cross-DB
        const r = await engine.query({
          definition: {
            from: 'events',
            columns: ['id'],
            joins: [{ table: 'users' }],
            executeMode: 'sql-only',
          },
          context: admin,
        })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(r.meta.targetDatabase).toBe('trino')
        }
      })

      it('C1111: meta.dialect in sql-only mode', async () => {
        const r = await engine.query({ definition: { from: 'orders', executeMode: 'sql-only' }, context: admin })
        expect(r.kind).toBe('sql')
        if (r.kind === 'sql') {
          expect(['postgres', 'clickhouse', 'trino']).toContain(r.meta.dialect)
        }
      })

      it('C1112: meta.dialect in count mode', async () => {
        const r = await engine.query({ definition: { from: 'orders', executeMode: 'count' }, context: admin })
        expect(r.kind).toBe('count')
        if (r.kind === 'count') {
          expect(['postgres', 'clickhouse', 'trino']).toContain(r.meta.dialect)
        }
      })

      it('C1113: aggregation nullable inference', async () => {
        const r = await engine.query({
          definition: {
            from: 'orders',
            columns: [],
            aggregations: [{ column: 'discount', fn: 'sum', alias: 'discountSum' }],
          },
          context: admin,
        })
        expect(r.kind).toBe('data')
        if (r.kind === 'data') {
          const col = r.meta.columns.find((c) => c.apiName === 'discountSum')
          expect(col?.nullable).toBe(true)
        }
      })
    })

    // ── 3-9. Parameterized Tests (pg/ch) ─────────────────────

    const dialectVariants = [
      { variant: 'pg', samples: 'samples', sampleItems: 'sampleItems', sampleDetails: 'sampleDetails' },
      { variant: 'ch', samples: 'chSamples', sampleItems: 'chSampleItems', sampleDetails: 'chSampleDetails' },
      { variant: 'trino', samples: 'chSamples', sampleItems: 'chSampleItems', sampleDetails: 'chSampleDetails' },
    ] as const

    describe.each(dialectVariants)('$variant dialect', ({ variant, samples, sampleItems, sampleDetails }) => {
      // ── 3.1 Comparison Operators ───────────────────────────

      describe('3.1 Comparison Operators', () => {
        it('C100: = filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'status', operator: '=', value: 'active' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C101: != filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'status', operator: '!=', value: 'cancelled' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C102: > filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: '>', value: 100 }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C103: < filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: '<', value: 200 }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C104: >= filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: '>=', value: 150 }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C105: <= filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: '<=', value: 100 }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C106: = on boolean column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'isActive', operator: '=', value: true }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C107: != on boolean column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'isActive', operator: '!=', value: true }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1) // only false (SQL != excludes NULL)
        })

        it('C108: = on uuid column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'externalId', operator: '=', value: '00000000-0000-4000-a000-000000000501' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })
      })

      // ── 3.2 Pattern Operators ──────────────────────────────

      describe('3.2 Pattern Operators', () => {
        it('C110: like filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'like', value: '%@test%' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(5)
        })

        it('C111: notLike filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'notLike', value: '%alpha%' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C112: ilike filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'ilike', value: '%TEST%' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(5)
        })

        it('C113: notIlike filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'notIlike', value: '%ALPHA%' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C114: contains filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'contains', value: 'alpha' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C115: icontains filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'icontains', value: 'ALPHA' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C116: notContains filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'notContains', value: 'alpha' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C117: notIcontains filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'notIcontains', value: 'ALPHA' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C118: startsWith filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'name', operator: 'startsWith', value: 'Al' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C119: istartsWith filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'name', operator: 'istartsWith', value: 'AL' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C120: endsWith filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'endsWith', value: '@test.com' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(5)
        })

        it('C121: iendsWith filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'email', operator: 'iendsWith', value: '@TEST.COM' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(5)
        })

        it('C122: contains with wildcard escaping', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'name', operator: 'contains', value: 'Al%ha' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(0)
        })

        it('C123: contains with underscore escaping', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'name', operator: 'contains', value: 'Al_ha' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(0)
        })
      })

      // ── 3.3 Range Operators ────────────────────────────────

      describe('3.3 Range Operators', () => {
        it('C130: between filter (decimal)', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'amount', operator: 'between', value: { from: 100, to: 200 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C131: notBetween filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'amount', operator: 'notBetween', value: { from: 100, to: 200 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C132: between on int', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'id', operator: 'between', value: { from: 2, to: 4 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C133: between on timestamp', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  column: 'createdAt',
                  operator: 'between',
                  value: { from: '2024-01-01T00:00:00Z', to: '2024-03-31T23:59:59Z' },
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C134: between on date', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'dueDate', operator: 'between', value: { from: '2024-02-01', to: '2024-05-01' } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C135: notBetween on int', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'id', operator: 'notBetween', value: { from: 2, to: 4 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })
      })

      // ── 3.4 Set Operators ──────────────────────────────────

      describe('3.4 Set Operators', () => {
        it('C140: in filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'status', operator: 'in', value: ['active', 'paid'] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C141: notIn filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'status', operator: 'notIn', value: ['cancelled'] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C142: in on int column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'id', operator: 'in', value: [1, 3, 5] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C143: in on uuid column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  column: 'externalId',
                  operator: 'in',
                  value: ['00000000-0000-4000-a000-000000000501', '00000000-0000-4000-a000-000000000502'],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C144: in on decimal column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: 'in', value: [100.0, 200.0] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })
      })

      // ── 3.5 Null Operators ─────────────────────────────────

      describe('3.5 Null Operators', () => {
        it('C150: isNull filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'discount', operator: 'isNull', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C151: isNotNull filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'discount', operator: 'isNotNull', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C152: isNull on array column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'tags', operator: 'isNull', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            // PG arrays can be NULL (Delta); CH arrays are never NULL (empty instead)
            expect(r.data.length).toBe(variant === 'pg' ? 1 : 0)
          }
        })

        it('C153: isNotNull on array column', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'tags', operator: 'isNotNull', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            // PG: 4 non-null; CH: 5 (arrays are never NULL)
            expect(r.data.length).toBe(variant === 'pg' ? 4 : 5)
          }
        })
      })

      // ── 3.6 Levenshtein ────────────────────────────────────

      describe('3.6 Levenshtein', () => {
        it('C160: levenshteinLte filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'name', operator: 'levenshteinLte', value: { text: 'Alphb', maxDistance: 2 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })
      })

      // ── 3.7 Array Operators ────────────────────────────────

      describe('3.7 Array Operators', () => {
        it('C170: arrayContains (int[])', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'scores', operator: 'arrayContains', value: 1 }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C171: arrayContainsAll (string[])', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'tags', operator: 'arrayContainsAll', value: ['fast', 'new'] }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })

        it('C172: arrayContainsAny (string[])', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'tags', operator: 'arrayContainsAny', value: ['slow', 'new'] }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C173: arrayIsEmpty', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'scores', operator: 'arrayIsEmpty', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            // PG: 1 (Delta has empty array; Gamma has NULL which is not empty)
            // CH: 2 (both Gamma and Delta have empty arrays — CH converts NULL to [])
            expect(r.data.length).toBe(variant === 'pg' ? 1 : 2)
          }
        })

        it('C174: arrayIsNotEmpty', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'scores', operator: 'arrayIsNotEmpty', value: null }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C175: arrayContainsAll single element', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'tags', operator: 'arrayContainsAll', value: ['fast'] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C176: arrayContains on string[]', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'tags', operator: 'arrayContains', value: 'fast' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })
      })

      // ── 3.8 Column-vs-Column Filters ───────────────────────

      describe('3.8 Column-vs-Column Filters', () => {
        it('C180: same-table column filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ column: 'amount', operator: '>', refColumn: 'discount' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C181: cross-table column filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems }],
              filters: [
                { column: 'amount', table: samples, operator: '>', refColumn: 'amount', refTable: sampleItems },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThan(0)
        })
      })

      // ── 3.9 Filter Groups ──────────────────────────────────

      describe('3.9 Filter Groups', () => {
        it('C190: OR filter group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  logic: 'or',
                  conditions: [
                    { column: 'status', operator: '=', value: 'active' },
                    { column: 'status', operator: '=', value: 'paid' },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })

        it('C191: AND filter group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  logic: 'and',
                  conditions: [
                    { column: 'status', operator: '=', value: 'active' },
                    { column: 'amount', operator: '>', value: 100 },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C192: NOT filter group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                { logic: 'and', not: true, conditions: [{ column: 'status', operator: '=', value: 'cancelled' }] },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C193: nested filter groups', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  logic: 'or',
                  conditions: [
                    { column: 'status', operator: '=', value: 'active' },
                    {
                      logic: 'and',
                      conditions: [
                        { column: 'amount', operator: '>', value: 100 },
                        { column: 'isActive', operator: '=', value: true },
                      ],
                    },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })

        it('C194: deeply nested (3 levels)', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  logic: 'or',
                  conditions: [
                    {
                      logic: 'and',
                      conditions: [
                        { column: 'status', operator: '=', value: 'active' },
                        { column: 'amount', operator: '>', value: 50 },
                      ],
                    },
                    {
                      logic: 'and',
                      conditions: [
                        { column: 'status', operator: '=', value: 'paid' },
                        { logic: 'and', not: true, conditions: [{ column: 'amount', operator: '<', value: 100 }] },
                      ],
                    },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3)
        })
      })

      // ── 3.10 Filter with Table Qualifier ───────────────────

      describe('3.10 Filter with Table Qualifier', () => {
        it('C195: top-level filter on joined column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems }],
              filters: [{ column: 'category', table: sampleItems, operator: '=', value: 'electronics' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBeGreaterThan(0)
          }
        })

        it('C196: explicit from-table reference', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ column: 'status', table: samples, operator: '=', value: 'active' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2)
        })
      })

      // ── 4. Joins ───────────────────────────────────────────

      describe('4. Joins', () => {
        it('C200: LEFT JOIN (default)', async () => {
          const r = await engine.query({
            definition: { from: samples, joins: [{ table: sampleItems }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBeGreaterThan(0)
            // Sample 4 has no items — should have null sampleItem keys
          }
        })

        it('C201: INNER JOIN', async () => {
          const r = await engine.query({
            definition: { from: samples, joins: [{ table: sampleItems, type: 'inner' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            // No sample 4 (has no items)
            const ids = r.data.map(
              (row) => (row as Record<string, unknown>).id ?? (row as Record<string, unknown>)[`${samples}.id`],
            )
            expect(ids).not.toContain(4)
          }
        })

        it('C202: multi-table join (3 tables)', async () => {
          const r = await engine.query({
            definition: { from: samples, joins: [{ table: sampleItems }, { table: sampleDetails }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.meta.tablesUsed.length).toBe(3)
          }
        })

        it('C203: join with column selection', async () => {
          const r = await engine.query({
            definition: { from: samples, joins: [{ table: sampleItems, columns: ['label'] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            const itemCols = r.meta.columns.filter((c) => c.fromTable === sampleItems)
            expect(itemCols.length).toBe(1)
            expect(itemCols[0]?.apiName).toBe('label')
          }
        })

        it('C204: join with columns: []', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              joins: [{ table: sampleItems, columns: [] }],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'id', fn: 'count', alias: 'cnt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            const itemCols = r.meta.columns.filter((c) => c.fromTable === sampleItems)
            expect(itemCols.length).toBe(0)
          }
        })

        it('C205: join-scoped filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems, filters: [{ column: 'category', operator: '=', value: 'electronics' }] }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBeGreaterThan(0)
          }
        })

        it('C206: column collision on join', async () => {
          const r = await engine.query({
            definition: { from: samples, joins: [{ table: sampleItems }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            // Both have id, category, amount, status — should be qualified
            const colNames = r.meta.columns.map((c) => c.apiName)
            const qualified = colNames.filter((n) => n.includes('.'))
            expect(qualified.length).toBeGreaterThan(0)
          }
        })

        it('C207: join filter at top level vs QueryJoin.filters', async () => {
          const rTop = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems }],
              filters: [{ column: 'category', table: sampleItems, operator: '=', value: 'electronics' }],
            },
            context: admin,
          })
          const rJoin = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems, filters: [{ column: 'category', operator: '=', value: 'electronics' }] }],
            },
            context: admin,
          })
          if (rTop.kind === 'data' && rJoin.kind === 'data') {
            expect(rTop.data.length).toBe(rJoin.data.length)
          }
        })
      })

      // ── 5. Aggregations ────────────────────────────────────

      describe('5. Aggregations', () => {
        it('C300: COUNT(*)', async () => {
          const r = await engine.query({
            definition: { from: samples, columns: [], aggregations: [{ column: '*', fn: 'count', alias: 'total' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBe(1)
            expect((r.data[0] as Record<string, unknown>).total).toBeGreaterThanOrEqual(5)
          }
        })

        it('C301: SUM', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBeGreaterThanOrEqual(1)
          }
        })

        it('C302: AVG', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'amount', fn: 'avg', alias: 'avgAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(typeof (r.data[0] as Record<string, unknown>).avgAmt).toBe('number')
          }
        })

        it('C303: MIN', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'createdAt', fn: 'min', alias: 'earliest' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect((r.data[0] as Record<string, unknown>).earliest).toBeDefined()
          }
        })

        it('C304: MAX', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'amount', fn: 'max', alias: 'maxAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect((r.data[0] as Record<string, unknown>).maxAmt).toBe(300)
          }
        })

        it('C305: COUNT(column)', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'discount', fn: 'count', alias: 'discountCount' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect((r.data[0] as Record<string, unknown>).discountCount).toBe(3)
          }
        })

        it('C306: multiple aggregations', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [
                { column: 'amount', fn: 'sum', alias: 'totalAmt' },
                { column: '*', fn: 'count', alias: 'cnt' },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            for (const row of r.data) {
              expect(row).toHaveProperty('totalAmt')
              expect(row).toHaveProperty('cnt')
            }
          }
        })

        it('C307: aggregation on joined column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              joins: [{ table: sampleItems, columns: [] }],
              aggregations: [{ column: 'amount', table: sampleItems, fn: 'sum', alias: 'totalItemAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect((r.data[0] as Record<string, unknown>).totalItemAmt).toBeDefined()
          }
        })

        it('C308: aggregation-only (columns: [])', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.meta.columns.length).toBe(1)
            expect(r.meta.columns[0]?.apiName).toBe('totalAmt')
          }
        })

        it('C309: columns undefined + aggregations + groupBy', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            const names = r.meta.columns.map((c) => c.apiName)
            expect(names).toContain('status')
            expect(names).toContain('totalAmt')
          }
        })

        it('C310: SUM on nullable column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              aggregations: [{ column: 'discount', fn: 'sum', alias: 'discountSum' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect((r.data[0] as Record<string, unknown>).discountSum).toBe(15)
          }
        })
      })

      // ── 6. GROUP BY & HAVING ───────────────────────────────

      describe('6. GROUP BY & HAVING', () => {
        it('C320: GROUP BY single column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4) // active, paid, cancelled, shipped
        })

        it('C321: GROUP BY with multi-column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status', 'isActive'],
              groupBy: [{ column: 'status' }, { column: 'isActive' }],
              aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThan(4)
        })

        it('C322: HAVING single condition', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
              having: [{ column: 'totalAmt', operator: '>', value: 100 }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3) // active(400), paid(200), shipped(150)
        })

        it('C324: HAVING with BETWEEN', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
              having: [{ column: 'totalAmt', operator: 'between', value: { from: 100, to: 300 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // paid(200), shipped(150)
        })

        it('C323: HAVING with OR group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [
                { column: 'amount', fn: 'sum', alias: 'totalAmt' },
                { column: 'amount', fn: 'avg', alias: 'avgAmt' },
              ],
              having: [
                {
                  logic: 'or',
                  conditions: [
                    { column: 'totalAmt', operator: '>', value: 250 },
                    { column: 'avgAmt', operator: '>', value: 150 },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // active(SUM 400>250), paid(AVG 200>150)
        })

        it('C325: HAVING with NOT BETWEEN', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
              having: [{ column: 'totalAmt', operator: 'notBetween', value: { from: 100, to: 300 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // active(400), cancelled(50)
        })

        it('C326: HAVING with IS NULL', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'discount', fn: 'sum', alias: 'discountSum' }],
              having: [{ column: 'discountSum', operator: 'isNull', value: null }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1) // paid (all discounts null → SUM is null)
        })

        it('C327: NOT in HAVING group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [
                { column: 'amount', fn: 'sum', alias: 'totalAmt' },
                { column: '*', fn: 'count', alias: 'cnt' },
              ],
              having: [
                {
                  logic: 'or',
                  not: true,
                  conditions: [
                    { column: 'totalAmt', operator: '>', value: 100 },
                    { column: 'cnt', operator: '>', value: 1 },
                  ],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1) // cancelled (SUM 50, COUNT 1)
        })

        it('C328: ORDER BY aggregation alias', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
              orderBy: [{ column: 'totalAmt', direction: 'desc' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data' && r.data.length >= 2) {
            const first = (r.data[0] as Record<string, unknown>).totalAmt as number
            const last = (r.data[r.data.length - 1] as Record<string, unknown>).totalAmt as number
            expect(first).toBeGreaterThanOrEqual(last)
          }
        })

        it('C329: GROUP BY joined column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: [],
              joins: [{ table: sampleItems, columns: [] }],
              groupBy: [{ column: 'category', table: sampleItems }],
              aggregations: [{ column: '*', fn: 'count', alias: 'cnt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThanOrEqual(1)
        })
      })

      // ── 7. ORDER BY, LIMIT, OFFSET, DISTINCT ──────────────

      describe('7. ORDER BY, LIMIT, OFFSET, DISTINCT', () => {
        it('C400: ORDER BY single column asc', async () => {
          const r = await engine.query({
            definition: { from: samples, orderBy: [{ column: 'amount', direction: 'asc' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data' && r.data.length >= 2) {
            const amounts = r.data.map((row) => (row as Record<string, unknown>).amount as number)
            for (let i = 1; i < amounts.length; i++) {
              const curr = amounts[i]
              const prev = amounts[i - 1]
              if (curr !== undefined && prev !== undefined) {
                expect(curr).toBeGreaterThanOrEqual(prev)
              }
            }
          }
        })

        it('C401: ORDER BY single column desc', async () => {
          const r = await engine.query({
            definition: { from: samples, orderBy: [{ column: 'amount', direction: 'desc' }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data' && r.data.length >= 2) {
            const amounts = r.data.map((row) => (row as Record<string, unknown>).amount as number)
            for (let i = 1; i < amounts.length; i++) {
              const curr = amounts[i]
              const prev = amounts[i - 1]
              if (curr !== undefined && prev !== undefined) {
                expect(curr).toBeLessThanOrEqual(prev)
              }
            }
          }
        })

        it('C402: ORDER BY multiple columns', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              orderBy: [
                { column: 'status', direction: 'asc' },
                { column: 'amount', direction: 'desc' },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBe(5)
            const rows = r.data as Record<string, unknown>[]
            // Verify primary sort: status ascending
            const statuses = rows.map((row) => row.status as string)
            const sortedStatuses = [...statuses].sort()
            expect(statuses).toEqual(sortedStatuses)
            // Verify secondary sort: within same status, amount descending
            for (let i = 1; i < rows.length; i++) {
              const curr = rows[i]
              const prev = rows[i - 1]
              if (curr !== undefined && prev !== undefined && curr.status === prev.status) {
                expect(curr.amount as number).toBeLessThanOrEqual(prev.amount as number)
              }
            }
          }
        })

        it('C403: ORDER BY joined column', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems }],
              orderBy: [{ column: 'category', table: sampleItems, direction: 'asc' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThan(0)
        })

        it('C404: LIMIT', async () => {
          const r = await engine.query({ definition: { from: samples, limit: 2 }, context: admin })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeLessThanOrEqual(2)
        })

        it('C405: LIMIT + OFFSET', async () => {
          const r = await engine.query({ definition: { from: samples, limit: 2, offset: 2 }, context: admin })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeLessThanOrEqual(2)
        })

        it('C406: DISTINCT', async () => {
          const r = await engine.query({
            definition: { from: samples, columns: ['status'], distinct: true },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            const statuses = new Set(r.data.map((row) => (row as Record<string, unknown>).status))
            expect(statuses.size).toBe(r.data.length)
          }
        })

        it('C407: DISTINCT + GROUP BY', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              columns: ['status'],
              distinct: true,
              groupBy: [{ column: 'status' }],
              aggregations: [{ column: 'amount', fn: 'sum', alias: 'totalAmt' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4)
        })
      })

      // ── 8. byIds ───────────────────────────────────────────

      describe('8. byIds', () => {
        it('C500: byIds returns matching rows', async () => {
          const r = await engine.query({ definition: { from: samples, byIds: [1, 2] }, context: admin })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.data.length).toBe(2)
          }
        })

        it('C501: byIds with non-existent IDs', async () => {
          const r = await engine.query({ definition: { from: samples, byIds: [1, 999] }, context: admin })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C502: byIds with count mode', async () => {
          const r = await engine.query({
            definition: { from: samples, byIds: [1, 2, 3], executeMode: 'count' },
            context: admin,
          })
          expect(r.kind).toBe('count')
          if (r.kind === 'count') expect(r.count).toBe(3)
        })

        it('C503: byIds with join', async () => {
          const r = await engine.query({
            definition: { from: samples, byIds: [1, 2], joins: [{ table: sampleItems }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThan(0)
        })

        it('C504: byIds with column selection', async () => {
          const r = await engine.query({
            definition: { from: samples, byIds: [1], columns: ['id', 'status'] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') {
            expect(r.meta.columns.length).toBe(2)
          }
        })

        it('C506: byIds with filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              byIds: [1, 2, 3],
              filters: [{ column: 'status', operator: '=', value: 'active' }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1)
        })

        it('C507: byIds with sql-only', async () => {
          const r = await engine.query({
            definition: { from: samples, byIds: [1, 2], executeMode: 'sql-only' },
            context: admin,
          })
          expect(r.kind).toBe('sql')
          if (r.kind === 'sql') {
            expect(r.sql).toContain('WHERE')
          }
        })

        it('C505: byIds rejects composite PK', async () => {
          await expectValidationError(engine, { from: 'orderItems', byIds: [1, 2] }, admin, 'INVALID_BY_IDS')
        })
      })

      // ── 9. EXISTS / NOT EXISTS ─────────────────────────────

      describe('9. EXISTS', () => {
        it('C600: EXISTS filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4) // ids 1,2,3,5
        })

        it('C601: NOT EXISTS filter', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, exists: false }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(1) // id 4
        })

        it('C602: EXISTS with subquery filter', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ table: sampleItems, filters: [{ column: 'status', operator: '=', value: 'paid' }] }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // ids 2, 5
        })

        it('C603: EXISTS inside OR group', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [
                {
                  logic: 'or',
                  conditions: [{ column: 'status', operator: '=', value: 'cancelled' }, { table: sampleItems }],
                },
              ],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4) // ids 1,2,3,5
        })

        it('C604: nested EXISTS', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, filters: [{ table: sampleDetails }] }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3) // ids 1, 2, 5
        })

        it('C605: counted EXISTS (>=)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '>=', value: 2 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // ids 1, 5
        })

        it('C606: counted EXISTS (=)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '=', value: 1 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // ids 2, 3
        })

        it('C607: counted EXISTS ignores exists field', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              filters: [{ table: sampleItems, exists: false, count: { operator: '>=', value: 1 } }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4) // ids 1,2,3,5 — count decides, not exists
        })

        it('C608: self-referencing EXISTS', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: samples }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3) // Beta, Gamma, Epsilon have valid managerId
        })

        it('C609: EXISTS with join', async () => {
          const r = await engine.query({
            definition: {
              from: samples,
              joins: [{ table: sampleItems }],
              filters: [{ table: samples }],
            },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBeGreaterThan(0) // samples that manage others, with items
        })

        it('C610: counted EXISTS (>)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '>', value: 1 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(2) // ids 1, 5
        })

        it('C611: counted EXISTS (<)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '<', value: 2 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3) // ids 2,3,4 (includes 0-item parent)
        })

        it('C612: counted EXISTS (!=)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '!=', value: 0 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(4) // ids 1,2,3,5
        })

        it('C613: counted EXISTS (<=)', async () => {
          const r = await engine.query({
            definition: { from: samples, filters: [{ table: sampleItems, count: { operator: '<=', value: 1 } }] },
            context: admin,
          })
          expect(r.kind).toBe('data')
          if (r.kind === 'data') expect(r.data.length).toBe(3) // ids 2,3,4 (includes 0-item parent)
        })
      })
    })
  })
}
