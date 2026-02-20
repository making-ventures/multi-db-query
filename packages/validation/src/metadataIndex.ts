import type { ColumnMeta, DatabaseMeta, MetadataConfig, RoleMeta, TableMeta } from './types/metadata.js'

/**
 * Pre-indexed metadata for O(1) lookups during query validation.
 */
export class MetadataIndex {
  readonly tablesByApiName: ReadonlyMap<string, TableMeta>
  readonly tablesById: ReadonlyMap<string, TableMeta>
  readonly columnsByTable: ReadonlyMap<string, ReadonlyMap<string, ColumnMeta>>
  readonly rolesById: ReadonlyMap<string, RoleMeta>
  readonly databasesById: ReadonlyMap<string, DatabaseMeta>

  constructor(config: MetadataConfig, roles: readonly RoleMeta[]) {
    const tablesByApiName = new Map<string, TableMeta>()
    const tablesById = new Map<string, TableMeta>()
    const columnsByTable = new Map<string, Map<string, ColumnMeta>>()

    for (const table of config.tables) {
      tablesByApiName.set(table.apiName, table)
      tablesById.set(table.id, table)
      const colMap = new Map<string, ColumnMeta>()
      for (const col of table.columns) {
        colMap.set(col.apiName, col)
      }
      columnsByTable.set(table.id, colMap)
    }

    this.tablesByApiName = tablesByApiName
    this.tablesById = tablesById
    this.columnsByTable = columnsByTable

    const dbs = new Map<string, DatabaseMeta>()
    for (const db of config.databases) {
      dbs.set(db.id, db)
    }
    this.databasesById = dbs

    const rolesMap = new Map<string, RoleMeta>()
    for (const role of roles) {
      rolesMap.set(role.id, role)
    }
    this.rolesById = rolesMap
  }

  getTable(apiName: string): TableMeta | undefined {
    return this.tablesByApiName.get(apiName)
  }

  getTableById(id: string): TableMeta | undefined {
    return this.tablesById.get(id)
  }

  getColumn(tableId: string, apiName: string): ColumnMeta | undefined {
    return this.columnsByTable.get(tableId)?.get(apiName)
  }

  getColumns(tableId: string): ReadonlyMap<string, ColumnMeta> | undefined {
    return this.columnsByTable.get(tableId)
  }

  getRole(id: string): RoleMeta | undefined {
    return this.rolesById.get(id)
  }
}
