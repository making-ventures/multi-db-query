import type {
  CachedTableMeta,
  DatabaseMeta,
  ExternalSync,
  MetadataConfig,
  RoleMeta,
  TableMeta,
} from '@mkven/multi-db-validation'
import { MetadataIndex, ProviderError, validateConfig } from '@mkven/multi-db-validation'
import type { MetadataProvider, RoleProvider } from '../types/providers.js'

/**
 * Database connectivity edge: two databases are connected if
 * an ExternalSync copies data from one to the other.
 */
export interface ConnectivityEdge {
  readonly sourceDatabase: string
  readonly targetDatabase: string
  readonly method: 'debezium'
}

/**
 * Snapshot of metadata and roles at a point in time.
 * Captured by queries for snapshot isolation.
 */
export interface RegistrySnapshot {
  readonly index: MetadataIndex
  readonly config: MetadataConfig
  readonly roles: readonly RoleMeta[]
  readonly syncsByTable: ReadonlyMap<string, readonly ExternalSync[]>
  readonly cachesByTable: ReadonlyMap<string, readonly CachedTableMeta[]>
  readonly connectivityGraph: readonly ConnectivityEdge[]
}

/**
 * In-memory metadata store with O(1) indexes.
 *
 * Features:
 * - Loads from providers → validates → throws on failure
 * - Builds indexes for O(1) lookups
 * - Atomic swap on reload — in-flight queries see old config
 * - Failed reloads preserve previous config
 */
export class MetadataRegistry {
  private snapshot: RegistrySnapshot

  private constructor(snapshot: RegistrySnapshot) {
    this.snapshot = snapshot
  }

  /**
   * Create a registry by loading from providers.
   * Validates config and throws on failure.
   */
  static async create(metadataProvider: MetadataProvider, roleProvider: RoleProvider): Promise<MetadataRegistry> {
    const snapshot = await MetadataRegistry.loadSnapshot(metadataProvider, roleProvider)
    return new MetadataRegistry(snapshot)
  }

  /**
   * Get the current snapshot for query processing.
   * Returns the snapshot that was active at the time of the call.
   */
  getSnapshot(): RegistrySnapshot {
    return this.snapshot
  }

  /**
   * Re-load metadata from provider, rebuild indexes.
   * On failure, old config is preserved and error is thrown.
   */
  async reloadMetadata(metadataProvider: MetadataProvider): Promise<void> {
    const config = await MetadataRegistry.loadMetadata(metadataProvider)
    MetadataRegistry.validateMetadata(config)

    const roles = [...this.snapshot.roles]
    const newSnapshot = MetadataRegistry.buildSnapshot(config, roles)
    this.snapshot = newSnapshot
  }

  /**
   * Re-load roles from provider, rebuild role index.
   * On failure, old config is preserved and error is thrown.
   */
  async reloadRoles(roleProvider: RoleProvider): Promise<void> {
    const roles = await MetadataRegistry.loadRoles(roleProvider)
    const config = this.snapshot.config
    const newSnapshot = MetadataRegistry.buildSnapshot(config, roles)
    this.snapshot = newSnapshot
  }

  // --- Internal helpers ---

  private static async loadSnapshot(
    metadataProvider: MetadataProvider,
    roleProvider: RoleProvider,
  ): Promise<RegistrySnapshot> {
    const config = await MetadataRegistry.loadMetadata(metadataProvider)
    MetadataRegistry.validateMetadata(config)
    const roles = await MetadataRegistry.loadRoles(roleProvider)
    return MetadataRegistry.buildSnapshot(config, roles)
  }

  private static async loadMetadata(provider: MetadataProvider): Promise<MetadataConfig> {
    try {
      return await provider.load()
    } catch (err) {
      throw new ProviderError(
        'METADATA_LOAD_FAILED',
        `Metadata provider failed to load: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err : undefined,
      )
    }
  }

  private static async loadRoles(provider: RoleProvider): Promise<RoleMeta[]> {
    try {
      return await provider.load()
    } catch (err) {
      throw new ProviderError(
        'ROLE_LOAD_FAILED',
        `Role provider failed to load: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err : undefined,
      )
    }
  }

  private static validateMetadata(config: MetadataConfig): void {
    const err = validateConfig(config)
    if (err !== null) {
      throw err
    }
  }

  private static buildSnapshot(config: MetadataConfig, roles: RoleMeta[]): RegistrySnapshot {
    const index = new MetadataIndex(config, roles)

    // Build syncs-by-table index
    const syncsByTable = new Map<string, ExternalSync[]>()
    for (const sync of config.externalSyncs) {
      const table = findTableByPhysicalContext(config, sync)
      if (table !== undefined) {
        const existing = syncsByTable.get(table.id)
        if (existing !== undefined) {
          existing.push(sync)
        } else {
          syncsByTable.set(table.id, [sync])
        }
      }
    }

    // Build caches-by-table index
    const cachesByTable = new Map<string, CachedTableMeta[]>()
    for (const cache of config.caches) {
      for (const ct of cache.tables) {
        const existing = cachesByTable.get(ct.tableId)
        if (existing !== undefined) {
          existing.push(ct)
        } else {
          cachesByTable.set(ct.tableId, [ct])
        }
      }
    }

    // Build connectivity graph from external syncs
    const connectivityGraph = buildConnectivityGraph(config)

    return {
      index,
      config,
      roles,
      syncsByTable,
      cachesByTable,
      connectivityGraph,
    }
  }
}

/**
 * Find the table that owns this sync's sourceTable.
 * sourceTable is a tableId reference.
 */
function findTableByPhysicalContext(config: MetadataConfig, sync: ExternalSync): TableMeta | undefined {
  return config.tables.find((t) => t.id === sync.sourceTable)
}

/**
 * Build connectivity graph: two databases are connected if
 * an ExternalSync copies data between them.
 */
function buildConnectivityGraph(config: MetadataConfig): ConnectivityEdge[] {
  const edges: ConnectivityEdge[] = []
  const tableById = new Map<string, TableMeta>()
  for (const t of config.tables) {
    tableById.set(t.id, t)
  }

  const dbByTableId = new Map<string, DatabaseMeta>()
  for (const db of config.databases) {
    for (const t of config.tables) {
      if (t.database === db.id) {
        dbByTableId.set(t.id, db)
      }
    }
  }

  for (const sync of config.externalSyncs) {
    const sourceDb = dbByTableId.get(sync.sourceTable)
    if (sourceDb !== undefined) {
      edges.push({
        sourceDatabase: sourceDb.id,
        targetDatabase: sync.targetDatabase,
        method: sync.method,
      })
    }
  }

  return edges
}
