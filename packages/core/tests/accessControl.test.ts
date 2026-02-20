import type { ExecutionContext, RoleMeta, TableMeta } from '@mkven/multi-db-validation'
import { describe, expect, it } from 'vitest'
import { applyMask, computeAllowedColumns, maskRows, resolveTableAccess } from '../src/accessControl.js'

// --- Fixtures ---

const usersTable: TableMeta = {
  id: 'users',
  database: 'pg_main',
  physicalName: 'public.users',
  apiName: 'users',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'email', physicalName: 'email', type: 'string', nullable: false, maskingFn: 'email' },
    { apiName: 'phone', physicalName: 'phone', type: 'string', nullable: true, maskingFn: 'phone' },
    { apiName: 'age', physicalName: 'age', type: 'int', nullable: true },
  ],
  relations: [],
}

const ordersTable: TableMeta = {
  id: 'orders',
  database: 'pg_main',
  physicalName: 'public.orders',
  apiName: 'orders',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'total', physicalName: 'total', type: 'decimal', nullable: false, maskingFn: 'number' },
    { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
  ],
  relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
}

const eventsTable: TableMeta = {
  id: 'events',
  database: 'ch_analytics',
  physicalName: 'events',
  apiName: 'events',
  primaryKey: ['id'],
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'eventType', physicalName: 'event_type', type: 'string', nullable: false },
  ],
  relations: [],
}

// Roles
const adminRole: RoleMeta = { id: 'admin', tables: '*' }
const tenantUser: RoleMeta = {
  id: 'tenant-user',
  tables: [{ tableId: 'orders', allowedColumns: ['id', 'userId', 'status'], maskedColumns: ['total'] }],
}
const regionalManager: RoleMeta = {
  id: 'regional-manager',
  tables: [{ tableId: 'orders', allowedColumns: '*' }],
}
const analyticsReader: RoleMeta = {
  id: 'analytics-reader',
  tables: [{ tableId: 'events', allowedColumns: '*' }],
}
const ordersService: RoleMeta = {
  id: 'orders-service',
  tables: [{ tableId: 'orders', allowedColumns: '*' }],
}
const svcRoleMasking: RoleMeta = {
  id: 'svc-masking',
  tables: [{ tableId: 'orders', allowedColumns: '*', maskedColumns: ['total'] }],
}
const viewerRole: RoleMeta = {
  id: 'viewer',
  tables: [{ tableId: 'users', allowedColumns: ['id', 'name'] }],
}

const allRolesMap = new Map<string, RoleMeta>([
  ['admin', adminRole],
  ['tenant-user', tenantUser],
  ['regional-manager', regionalManager],
  ['analytics-reader', analyticsReader],
  ['orders-service', ordersService],
  ['svc-masking', svcRoleMasking],
  ['viewer', viewerRole],
])

// --- Tests ---

describe('Scope resolution', () => {
  it('#13 admin role — all columns visible', () => {
    const ctx: ExecutionContext = { roles: { user: ['admin'] } }
    const access = resolveTableAccess(usersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    expect(access.columns.size).toBe(5)
    for (const col of access.columns.values()) {
      expect(col.allowed).toBe(true)
      expect(col.masked).toBe(false)
    }
  })

  it('#14 tenant-user role — subset columns only', () => {
    const ctx: ExecutionContext = { roles: { user: ['tenant-user'] } }
    const access = resolveTableAccess(ordersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    expect(access.columns.get('id')?.allowed).toBe(true)
    expect(access.columns.get('userId')?.allowed).toBe(true)
    expect(access.columns.get('status')?.allowed).toBe(true)
    expect(access.columns.get('total')?.allowed).toBe(false)
  })

  it('#14b column masking — total masked', () => {
    // First give tenant-user access to total to test masking
    const tenantWithTotal: RoleMeta = {
      id: 'tenant-total',
      tables: [{ tableId: 'orders', allowedColumns: ['id', 'userId', 'status', 'total'], maskedColumns: ['total'] }],
    }
    const rolesMap = new Map(allRolesMap)
    rolesMap.set('tenant-total', tenantWithTotal)

    const ctx2: ExecutionContext = { roles: { user: ['tenant-total'] } }
    const access = resolveTableAccess(ordersTable, ctx2, rolesMap)
    expect(access.columns.get('total')?.allowed).toBe(true)
    expect(access.columns.get('total')?.masked).toBe(true)
    expect(access.columns.get('total')?.maskingFn).toBe('number')
  })

  it('#14c multi-role within scope — union', () => {
    const ctx: ExecutionContext = { roles: { user: ['tenant-user', 'regional-manager'] } }
    const access = resolveTableAccess(ordersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    // UNION: regional-manager has '*' → all columns
    for (const col of access.columns.values()) {
      expect(col.allowed).toBe(true)
    }
  })

  it('#14d cross-scope restriction — admin user + orders-service', () => {
    const ctx: ExecutionContext = { roles: { user: ['admin'], service: ['orders-service'] } }
    // Admin: all tables. orders-service: only orders
    const access = resolveTableAccess(usersTable, ctx, allRolesMap)
    // Service scope doesn't include users → table denied
    expect(access.allowed).toBe(false)
  })

  it('#14f omitted scope — no service restriction', () => {
    const ctx: ExecutionContext = { roles: { user: ['admin'] } }
    // Only user scope, no service scope → no service restriction
    const access = resolveTableAccess(ordersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    for (const col of access.columns.values()) {
      expect(col.allowed).toBe(true)
    }
  })

  it('#38 columns omitted — returns role-allowed columns', () => {
    const ctx: ExecutionContext = { roles: { user: ['tenant-user'] } }
    const allowed = computeAllowedColumns(ordersTable, ctx, allRolesMap)
    expect(allowed).toEqual(['id', 'userId', 'status'])
  })

  it('#95 empty scope intersection — ACCESS_DENIED', () => {
    const ctx: ExecutionContext = { roles: { user: ['analytics-reader'], service: ['orders-service'] } }
    // user scope: events only. service scope: orders only. Intersection: nothing matches events
    const access = resolveTableAccess(eventsTable, ctx, allRolesMap)
    expect(access.allowed).toBe(false)
  })

  it('#104 empty roles array — zero permissions', () => {
    const ctx: ExecutionContext = { roles: { user: [] } }
    const access = resolveTableAccess(ordersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(false)
  })

  it('#106 cross-scope masking — user unmasks, service masks → stays masked', () => {
    const ctx: ExecutionContext = { roles: { user: ['regional-manager'], service: ['svc-masking'] } }
    const access = resolveTableAccess(ordersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    // regional-manager unmasks total in user scope, svc-masking masks it in service scope
    // Intersection: masked (most restrictive)
    expect(access.columns.get('total')?.masked).toBe(true)
  })

  it('#16 column trimming on byIds', () => {
    const ctx: ExecutionContext = { roles: { user: ['viewer'] } }
    const allowed = computeAllowedColumns(usersTable, ctx, allRolesMap)
    expect(allowed).toEqual(['id', 'name'])
    // email, phone, age not in allowed columns for viewer
    expect(allowed).not.toContain('email')
    expect(allowed).not.toContain('phone')
    expect(allowed).not.toContain('age')
  })

  it('no scopes at all — unrestricted', () => {
    const ctx: ExecutionContext = { roles: {} }
    const access = resolveTableAccess(usersTable, ctx, allRolesMap)
    expect(access.allowed).toBe(true)
    for (const col of access.columns.values()) {
      expect(col.allowed).toBe(true)
      expect(col.masked).toBe(false)
    }
  })
})

describe('Masking functions', () => {
  it('email masking', () => {
    expect(applyMask('john@example.com', 'email')).toBe('j***@***.com')
  })

  it('phone masking', () => {
    expect(applyMask('+1234567890', 'phone')).toBe('+1***890')
  })

  it('name masking', () => {
    expect(applyMask('John Smith', 'name')).toBe('J********h')
  })

  it('uuid masking', () => {
    expect(applyMask('a1b2c3d4-e5f6-7890', 'uuid')).toBe('a1b2****')
  })

  it('number masking', () => {
    expect(applyMask(12345, 'number')).toBe(0)
  })

  it('date masking with string', () => {
    expect(applyMask('2025-03-15', 'date')).toBe('2025-01-01')
  })

  it('date masking with Date object', () => {
    expect(applyMask(new Date('2025-03-15'), 'date')).toBe('2025-01-01')
  })

  it('full masking', () => {
    expect(applyMask('anything', 'full')).toBe('***')
  })

  it('null values pass through', () => {
    expect(applyMask(null, 'email')).toBeNull()
    expect(applyMask(undefined, 'number')).toBeUndefined()
  })
})

describe('maskRows', () => {
  it('#233 aggregation aliases never masked', () => {
    const columns = new Map([
      ['status', { apiName: 'status', allowed: true, masked: false, maskingFn: undefined }],
      ['totalSum', { apiName: 'totalSum', allowed: true, masked: true, maskingFn: 'number' as const }],
    ])
    const aggAliases = new Set(['totalSum'])

    const rows = [{ status: 'active', totalSum: 5000 }]
    const result = maskRows(rows, columns, aggAliases)
    expect(result[0]?.totalSum).toBe(5000) // Not masked
    expect(result[0]?.status).toBe('active')
  })

  it('masks non-alias columns', () => {
    const columns = new Map([
      ['email', { apiName: 'email', allowed: true, masked: true, maskingFn: 'email' as const }],
      ['name', { apiName: 'name', allowed: true, masked: false, maskingFn: undefined }],
    ])
    const aggAliases = new Set<string>()

    const rows = [{ email: 'john@example.com', name: 'John' }]
    const result = maskRows(rows, columns, aggAliases)
    expect(result[0]?.email).toBe('j***@***.com')
    expect(result[0]?.name).toBe('John')
  })

  it('no masked columns — returns rows unchanged', () => {
    const columns = new Map([['id', { apiName: 'id', allowed: true, masked: false, maskingFn: undefined }]])
    const rows = [{ id: '123' }]
    const result = maskRows(rows, columns, new Set())
    expect(result).toBe(rows) // Same reference — no copy
  })
})
