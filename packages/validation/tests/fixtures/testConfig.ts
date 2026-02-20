import type { CacheMeta, DatabaseMeta, ExternalSync, MetadataConfig, TableMeta } from '../../src/types/metadata.js'

// --- Databases ---

export const pgMain: DatabaseMeta = {
  id: 'pg-main',
  engine: 'postgres',
}

export const chAnalytics: DatabaseMeta = {
  id: 'ch-analytics',
  engine: 'clickhouse',
}

export const icebergWarehouse: DatabaseMeta = {
  id: 'iceberg-wh',
  engine: 'iceberg',
  trinoCatalog: 'ice_catalog',
}

// --- Tables ---

export const usersTable: TableMeta = {
  id: 'users',
  apiName: 'users',
  database: 'pg-main',
  physicalName: 'users',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'email', physicalName: 'email', type: 'string', nullable: false, maskingFn: 'email' },
    { apiName: 'age', physicalName: 'age', type: 'int', nullable: true },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [],
}

export const ordersTable: TableMeta = {
  id: 'orders',
  apiName: 'orders',
  database: 'pg-main',
  physicalName: 'orders',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'amount', physicalName: 'amount', type: 'decimal', nullable: false },
    { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [{ column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' }],
}

export const eventsTable: TableMeta = {
  id: 'events',
  apiName: 'events',
  database: 'ch-analytics',
  physicalName: 'events',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'eventType', physicalName: 'event_type', type: 'string', nullable: false },
    { apiName: 'payload', physicalName: 'payload', type: 'string', nullable: true },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
    { apiName: 'tags', physicalName: 'tags', type: 'string[]', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [],
}

// --- Syncs ---

export const userSync: ExternalSync = {
  sourceTable: 'users',
  targetDatabase: 'ch-analytics',
  targetPhysicalName: 'users_replica',
  method: 'debezium',
  estimatedLag: 'seconds',
}

// --- Caches ---

export const userCache: CacheMeta = {
  id: 'redis-users',
  engine: 'redis',
  tables: [{ tableId: 'users', keyPattern: 'users:{id}' }],
}

// --- Valid Config ---

export function validConfig(): MetadataConfig {
  return {
    databases: [pgMain, chAnalytics, icebergWarehouse],
    tables: [usersTable, ordersTable, eventsTable],
    caches: [userCache],
    externalSyncs: [userSync],
    trino: { enabled: true },
  }
}
