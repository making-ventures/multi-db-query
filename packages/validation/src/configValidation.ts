import type { ConfigErrorEntry } from './errors.js'
import { ConfigError } from './errors.js'
import type { MetadataConfig, TableMeta } from './types/metadata.js'

// --- Reserved Words ---

const RESERVED_WORDS = new Set([
  'from',
  'select',
  'where',
  'having',
  'limit',
  'offset',
  'order',
  'group',
  'join',
  'distinct',
  'exists',
  'null',
  'true',
  'false',
  'and',
  'or',
  'not',
  'in',
  'like',
  'as',
  'on',
  'by',
  'asc',
  'desc',
  'count',
  'sum',
  'avg',
  'min',
  'max',
])

// --- apiName Validation ---

const API_NAME_REGEX = /^[a-z][a-zA-Z0-9]*$/

export function validateApiName(name: string): string | null {
  if (name.length === 0 || name.length > 64) {
    return `apiName must be 1–64 characters, got ${name.length}`
  }
  if (!API_NAME_REGEX.test(name)) {
    return `apiName must match ^[a-z][a-zA-Z0-9]*$, got '${name}'`
  }
  if (RESERVED_WORDS.has(name.toLowerCase())) {
    return `apiName '${name}' is a reserved word`
  }
  return null
}

// --- Config Validation ---

export function validateConfig(metadata: MetadataConfig): ConfigError | null {
  const errors: ConfigErrorEntry[] = []

  const databaseIds = new Set(metadata.databases.map((d: { id: string }) => d.id))
  const tableIds = new Set<string>()
  const tableApiNames = new Map<string, string>()

  // --- Table-level checks ---
  for (const table of metadata.tables) {
    // Table apiName format
    const nameErr = validateApiName(table.apiName)
    if (nameErr !== null) {
      errors.push({
        code: 'INVALID_API_NAME',
        message: `Table '${table.id}': ${nameErr}`,
        details: { entity: table.id, field: 'apiName', actual: table.apiName },
      })
    }

    // Table apiName global uniqueness
    const existing = tableApiNames.get(table.apiName)
    if (existing !== undefined) {
      errors.push({
        code: 'DUPLICATE_API_NAME',
        message: `Duplicate table apiName '${table.apiName}' (tables '${existing}' and '${table.id}')`,
        details: { entity: table.id, field: 'apiName', actual: table.apiName },
      })
    } else {
      tableApiNames.set(table.apiName, table.id)
    }

    tableIds.add(table.id)

    // Database reference
    if (!databaseIds.has(table.database)) {
      errors.push({
        code: 'INVALID_REFERENCE',
        message: `Table '${table.id}' references non-existent database '${table.database}'`,
        details: { entity: table.id, field: 'database', actual: table.database, database: table.database },
      })
    }

    // Column apiName checks
    const columnApiNames = new Set<string>()
    const columnApiNameSet = new Set<string>()

    for (const col of table.columns) {
      const colNameErr = validateApiName(col.apiName)
      if (colNameErr !== null) {
        errors.push({
          code: 'INVALID_API_NAME',
          message: `Column '${table.id}.${col.apiName}': ${colNameErr}`,
          details: { entity: `${table.id}.${col.apiName}`, field: 'apiName', actual: col.apiName },
        })
      }

      if (columnApiNames.has(col.apiName)) {
        errors.push({
          code: 'DUPLICATE_API_NAME',
          message: `Duplicate column apiName '${col.apiName}' in table '${table.id}'`,
          details: { entity: `${table.id}.${col.apiName}`, field: 'apiName', actual: col.apiName },
        })
      } else {
        columnApiNames.add(col.apiName)
      }

      columnApiNameSet.add(col.apiName)
    }

    // Relation checks
    for (const rel of table.relations) {
      // Check source column exists in this table
      if (!columnApiNameSet.has(rel.column)) {
        errors.push({
          code: 'INVALID_RELATION',
          message: `Relation in table '${table.id}': source column '${rel.column}' does not exist`,
          details: { entity: table.id, field: 'column', actual: rel.column },
        })
      }
    }
  }

  // Second pass: validate relation references (need all table IDs collected first)
  const tableById = new Map<string, TableMeta>(metadata.tables.map((t) => [t.id, t]))
  for (const table of metadata.tables) {
    for (const rel of table.relations) {
      const refTbl = tableById.get(rel.references.table)
      if (refTbl === undefined) {
        errors.push({
          code: 'INVALID_RELATION',
          message: `Relation in table '${table.id}': references non-existent table '${rel.references.table}'`,
          details: { entity: table.id, field: 'references.table', actual: rel.references.table },
        })
      } else {
        const refCol = refTbl.columns.find((c) => c.apiName === rel.references.column)
        if (refCol === undefined) {
          errors.push({
            code: 'INVALID_RELATION',
            message: `Relation in table '${table.id}': references non-existent column '${rel.references.column}' in table '${rel.references.table}'`,
            details: { entity: table.id, field: 'references.column', actual: rel.references.column },
          })
        }
      }
    }
  }

  // --- External Sync checks ---
  for (const sync of metadata.externalSyncs) {
    if (!tableIds.has(sync.sourceTable)) {
      errors.push({
        code: 'INVALID_SYNC',
        message: `ExternalSync references non-existent source table '${sync.sourceTable}'`,
        details: { entity: sync.sourceTable, field: 'sourceTable', actual: sync.sourceTable },
      })
    }
    if (!databaseIds.has(sync.targetDatabase)) {
      errors.push({
        code: 'INVALID_SYNC',
        message: `ExternalSync references non-existent target database '${sync.targetDatabase}'`,
        details: {
          entity: sync.sourceTable,
          field: 'targetDatabase',
          actual: sync.targetDatabase,
          database: sync.targetDatabase,
        },
      })
    }
  }

  // --- Cache checks ---
  for (const cache of metadata.caches) {
    for (const ct of cache.tables) {
      const cachedTable = tableById.get(ct.tableId)
      if (cachedTable === undefined) {
        errors.push({
          code: 'INVALID_CACHE',
          message: `Cache '${cache.id}' references non-existent table '${ct.tableId}'`,
          details: { entity: ct.tableId, cacheId: cache.id },
        })
        continue
      }

      // Validate keyPattern placeholders reference PK columns
      const placeholders = extractPlaceholders(ct.keyPattern)
      const pkSet = new Set(cachedTable.primaryKey)
      for (const ph of placeholders) {
        if (!pkSet.has(ph)) {
          errors.push({
            code: 'INVALID_CACHE',
            message: `Cache '${cache.id}' table '${ct.tableId}': keyPattern placeholder '{${ph}}' is not a PK column`,
            details: { entity: ct.tableId, cacheId: cache.id, field: 'keyPattern', actual: ph },
          })
        }
      }

      // Validate cached columns reference existing columns
      if (ct.columns !== undefined) {
        const colNames = new Set(cachedTable.columns.map((c) => c.apiName))
        for (const col of ct.columns) {
          if (!colNames.has(col)) {
            errors.push({
              code: 'INVALID_CACHE',
              message: `Cache '${cache.id}' table '${ct.tableId}': cached column '${col}' does not exist`,
              details: { entity: ct.tableId, cacheId: cache.id, field: 'columns', actual: col },
            })
          }
        }
      }
    }
  }

  if (errors.length === 0) {
    return null
  }

  return new ConfigError(errors)
}

// --- Helpers ---

function extractPlaceholders(pattern: string): string[] {
  const matches = pattern.matchAll(/\{([^}]+)\}/g)
  return [...matches].map((m) => m[1] as string)
}
