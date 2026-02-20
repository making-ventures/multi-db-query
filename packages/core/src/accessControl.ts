import type { ExecutionContext, RoleMeta, TableMeta, TableRoleAccess } from '@mkven/multi-db-validation'

// --- Types ---

export type MaskingFn = 'email' | 'phone' | 'name' | 'uuid' | 'number' | 'date' | 'full'

/**
 * Effective column access for a single column.
 */
export interface EffectiveColumn {
  readonly apiName: string
  readonly allowed: boolean
  readonly masked: boolean
  readonly maskingFn: MaskingFn | undefined
}

/**
 * Effective table access after scope resolution.
 */
export interface EffectiveTableAccess {
  readonly tableId: string
  readonly allowed: boolean
  readonly columns: ReadonlyMap<string, EffectiveColumn>
}

// --- Scope resolution ---

/**
 * Resolve effective access for a table given the execution context and role definitions.
 *
 * Algorithm:
 * 1. Within a scope — UNION (most permissive)
 * 2. Between scopes — INTERSECTION (most restrictive)
 * 3. Omitted scope = no restriction from that scope
 * 4. Empty roles array = zero permissions
 * 5. tables: '*' = all tables, all columns, no masking
 */
export function resolveTableAccess(
  table: TableMeta,
  context: ExecutionContext,
  rolesById: ReadonlyMap<string, RoleMeta>,
): EffectiveTableAccess {
  const scopes = collectScopes(context)

  // No scopes at all → no restriction
  if (scopes.length === 0) {
    return unrestricted(table)
  }

  // Resolve each scope independently
  const scopeAccesses = scopes.map((roleIds) => resolveScopeAccess(table, roleIds, rolesById))

  // Intersect across scopes
  return intersectScopes(table, scopeAccesses)
}

/**
 * Compute effective allowed columns for a table (just the column names).
 * Convenience wrapper over resolveTableAccess.
 */
export function computeAllowedColumns(
  table: TableMeta,
  context: ExecutionContext,
  rolesById: ReadonlyMap<string, RoleMeta>,
): string[] {
  const access = resolveTableAccess(table, context, rolesById)
  if (!access.allowed) {
    return []
  }
  const result: string[] = []
  for (const [apiName, col] of access.columns) {
    if (col.allowed) {
      result.push(apiName)
    }
  }
  return result
}

// --- Masking functions ---

/**
 * Apply a masking function to a value.
 */
export function applyMask(value: unknown, fn: MaskingFn): unknown {
  if (value === null || value === undefined) {
    return value
  }

  switch (fn) {
    case 'email':
      return maskEmail(String(value))
    case 'phone':
      return maskPhone(String(value))
    case 'name':
      return maskName(String(value))
    case 'uuid':
      return maskUuid(String(value))
    case 'number':
      return 0
    case 'date':
      return maskDate(value)
    case 'full':
      return '***'
  }
}

/**
 * Apply masking to query result rows based on effective access.
 * Aggregation aliases are never masked.
 */
export function maskRows(
  rows: Record<string, unknown>[],
  columns: ReadonlyMap<string, EffectiveColumn>,
  aggregationAliases: ReadonlySet<string>,
): Record<string, unknown>[] {
  // Collect columns that need masking
  const maskedEntries: Array<{ apiName: string; fn: MaskingFn }> = []
  for (const [apiName, col] of columns) {
    if (col.masked && col.maskingFn !== undefined && !aggregationAliases.has(apiName)) {
      maskedEntries.push({ apiName, fn: col.maskingFn })
    }
  }

  if (maskedEntries.length === 0) {
    return rows
  }

  return rows.map((row) => {
    const masked = { ...row }
    for (const entry of maskedEntries) {
      if (entry.apiName in masked) {
        masked[entry.apiName] = applyMask(masked[entry.apiName], entry.fn)
      }
    }
    return masked
  })
}

// --- Internal helpers ---

interface ScopeAccess {
  allowed: boolean
  columns: Map<string, { allowed: boolean; masked: boolean; maskingFn: MaskingFn | undefined }>
}

function collectScopes(context: ExecutionContext): string[][] {
  const scopes: string[][] = []
  if (context.roles.user !== undefined) {
    scopes.push(context.roles.user)
  }
  if (context.roles.service !== undefined) {
    scopes.push(context.roles.service)
  }
  return scopes
}

/**
 * Resolve access for a single scope (UNION of role permissions within scope).
 */
function resolveScopeAccess(
  table: TableMeta,
  roleIds: string[],
  rolesById: ReadonlyMap<string, RoleMeta>,
): ScopeAccess {
  // Empty roles array → zero permissions
  if (roleIds.length === 0) {
    return zeroPerm(table)
  }

  let tableAllowed = false
  const colAccess = new Map<string, { allowed: boolean; masked: boolean; maskingFn: MaskingFn | undefined }>()

  // Initialize all columns as denied
  for (const col of table.columns) {
    colAccess.set(col.apiName, { allowed: false, masked: true, maskingFn: col.maskingFn ?? 'full' })
  }

  for (const roleId of roleIds) {
    const role = rolesById.get(roleId)
    if (role === undefined) {
      continue
    }

    // tables: '*' → all tables, all columns, no masking
    if (role.tables === '*') {
      tableAllowed = true
      for (const col of table.columns) {
        colAccess.set(col.apiName, { allowed: true, masked: false, maskingFn: col.maskingFn })
      }
      continue
    }

    // Find table access for this role
    const tableAccess = role.tables.find((t) => t.tableId === table.id)
    if (tableAccess === undefined) {
      continue
    }

    tableAllowed = true

    // UNION columns within scope
    applyTableAccess(table, tableAccess, colAccess)
  }

  return { allowed: tableAllowed, columns: colAccess }
}

/**
 * Apply a single role's table access (UNION semantics).
 */
function applyTableAccess(
  table: TableMeta,
  access: TableRoleAccess,
  colAccess: Map<string, { allowed: boolean; masked: boolean; maskingFn: MaskingFn | undefined }>,
): void {
  const maskedSet = new Set(access.maskedColumns ?? [])

  if (access.allowedColumns === '*') {
    // All columns allowed
    for (const col of table.columns) {
      const existing = colAccess.get(col.apiName)
      if (existing !== undefined) {
        const isMaskedByThisRole = maskedSet.has(col.apiName)
        // UNION: if any role grants access, it's allowed
        existing.allowed = true
        // UNION: if any role unmasks, the column becomes unmasked
        if (!isMaskedByThisRole) {
          existing.masked = false
        }
      }
    }
  } else {
    // Specific columns
    for (const colName of access.allowedColumns) {
      const existing = colAccess.get(colName)
      if (existing !== undefined) {
        const isMaskedByThisRole = maskedSet.has(colName)
        existing.allowed = true
        if (!isMaskedByThisRole) {
          existing.masked = false
        }
      }
    }
  }
}

/**
 * Intersect access across scopes (most restrictive).
 */
function intersectScopes(table: TableMeta, scopes: ScopeAccess[]): EffectiveTableAccess {
  // All scopes must allow the table
  const allowed = scopes.every((s) => s.allowed)
  if (!allowed) {
    return {
      tableId: table.id,
      allowed: false,
      columns: new Map(),
    }
  }

  const result = new Map<string, EffectiveColumn>()

  for (const col of table.columns) {
    // Column allowed = ALL scopes allow it
    const colAllowed = scopes.every((s) => s.columns.get(col.apiName)?.allowed === true)
    // Column masked = ANY scope masks it (intersection = most restrictive)
    const colMasked = colAllowed && scopes.some((s) => s.columns.get(col.apiName)?.masked === true)

    result.set(col.apiName, {
      apiName: col.apiName,
      allowed: colAllowed,
      masked: colMasked,
      maskingFn: colMasked ? (col.maskingFn ?? 'full') : col.maskingFn,
    })
  }

  return { tableId: table.id, allowed: true, columns: result }
}

function zeroPerm(table: TableMeta): ScopeAccess {
  const columns = new Map<string, { allowed: boolean; masked: boolean; maskingFn: MaskingFn | undefined }>()
  for (const col of table.columns) {
    columns.set(col.apiName, { allowed: false, masked: true, maskingFn: col.maskingFn ?? 'full' })
  }
  return { allowed: false, columns }
}

function unrestricted(table: TableMeta): EffectiveTableAccess {
  const columns = new Map<string, EffectiveColumn>()
  for (const col of table.columns) {
    columns.set(col.apiName, {
      apiName: col.apiName,
      allowed: true,
      masked: false,
      maskingFn: col.maskingFn,
    })
  }
  return { tableId: table.id, allowed: true, columns }
}

// --- Masking function implementations ---

function maskEmail(value: string): string {
  const atIdx = value.indexOf('@')
  if (atIdx <= 0) {
    return '***'
  }
  const domain = value.slice(atIdx + 1)
  const dotIdx = domain.lastIndexOf('.')
  if (dotIdx <= 0) {
    return `${value[0]}***@***`
  }
  const tld = domain.slice(dotIdx)
  return `${value[0]}***@***${tld}`
}

function maskPhone(value: string): string {
  // Show country code + last 3 digits
  const digits = value.replace(/\D/g, '')
  if (digits.length <= 3) {
    return '***'
  }
  const prefix = value.startsWith('+') ? '+' : ''
  const countryCode = digits.slice(0, 1)
  const last3 = digits.slice(-3)
  return `${prefix}${countryCode}***${last3}`
}

function maskName(value: string): string {
  if (value.length <= 2) {
    return '***'
  }
  const firstChar = value[0] ?? '*'
  const lastChar = value[value.length - 1] ?? '*'
  return `${firstChar}${'*'.repeat(value.length - 2)}${lastChar}`
}

function maskUuid(value: string): string {
  if (value.length <= 4) {
    return '****'
  }
  return `${value.slice(0, 4)}****`
}

function maskDate(value: unknown): string {
  // Try to parse as date and truncate to year
  if (value instanceof Date) {
    return `${value.getFullYear()}-01-01`
  }
  const str = String(value)
  const year = str.slice(0, 4)
  if (/^\d{4}$/.test(year)) {
    return `${year}-01-01`
  }
  return '***'
}
