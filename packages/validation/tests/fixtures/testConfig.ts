import type {
  CacheMeta,
  DatabaseMeta,
  ExternalSync,
  MetadataConfig,
  RoleMeta,
  TableMeta,
} from '../../src/types/metadata.js'

// --- Databases (4) ---

export const pgMain: DatabaseMeta = {
  id: 'pg-main',
  engine: 'postgres',
  trinoCatalog: 'pg_main',
}

export const pgTenant: DatabaseMeta = {
  id: 'pg-tenant',
  engine: 'postgres',
  trinoCatalog: 'pg_tenant',
}

export const chAnalytics: DatabaseMeta = {
  id: 'ch-analytics',
  engine: 'clickhouse',
  trinoCatalog: 'ch_analytics',
}

export const icebergWarehouse: DatabaseMeta = {
  id: 'iceberg-wh',
  engine: 'iceberg',
  trinoCatalog: 'iceberg_archive',
}

// --- Tables (8) ---

export const usersTable: TableMeta = {
  id: 'users',
  apiName: 'users',
  database: 'pg-main',
  physicalName: 'public.users',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'email', physicalName: 'email', type: 'string', nullable: false, maskingFn: 'email' },
    { apiName: 'phone', physicalName: 'phone', type: 'string', nullable: true, maskingFn: 'phone' },
    { apiName: 'age', physicalName: 'age', type: 'int', nullable: true },
    { apiName: 'tenantId', physicalName: 'tenant_id', type: 'uuid', nullable: false },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [{ column: 'tenantId', references: { table: 'tenants', column: 'id' }, type: 'many-to-one' }],
}

export const ordersTable: TableMeta = {
  id: 'orders',
  apiName: 'orders',
  database: 'pg-main',
  physicalName: 'public.orders',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'tenantId', physicalName: 'tenant_id', type: 'uuid', nullable: false },
    { apiName: 'productId', physicalName: 'product_id', type: 'uuid', nullable: true },
    { apiName: 'amount', physicalName: 'amount', type: 'decimal', nullable: false, maskingFn: 'number' },
    { apiName: 'discount', physicalName: 'discount', type: 'decimal', nullable: true },
    { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
    { apiName: 'internalNote', physicalName: 'internal_note', type: 'string', nullable: true, maskingFn: 'full' },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
    { apiName: 'quantity', physicalName: 'quantity', type: 'int', nullable: false },
    { apiName: 'isPaid', physicalName: 'is_paid', type: 'boolean', nullable: true },
    { apiName: 'priorities', physicalName: 'priorities', type: 'int[]', nullable: true },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' },
    { column: 'productId', references: { table: 'products', column: 'id' }, type: 'many-to-one' },
    { column: 'tenantId', references: { table: 'tenants', column: 'id' }, type: 'many-to-one' },
  ],
}

export const productsTable: TableMeta = {
  id: 'products',
  apiName: 'products',
  database: 'pg-main',
  physicalName: 'public.products',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'category', physicalName: 'category', type: 'string', nullable: false },
    { apiName: 'price', physicalName: 'price', type: 'decimal', nullable: false, maskingFn: 'number' },
    { apiName: 'labels', physicalName: 'labels', type: 'string[]', nullable: true },
    { apiName: 'tenantId', physicalName: 'tenant_id', type: 'uuid', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [{ column: 'tenantId', references: { table: 'tenants', column: 'id' }, type: 'many-to-one' }],
}

export const tenantsTable: TableMeta = {
  id: 'tenants',
  apiName: 'tenants',
  database: 'pg-tenant',
  physicalName: 'public.tenants',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string', nullable: false },
    { apiName: 'plan', physicalName: 'plan', type: 'string', nullable: false },
    { apiName: 'apiKey', physicalName: 'api_key', type: 'string', nullable: false, maskingFn: 'full' },
  ],
  primaryKey: ['id'],
  relations: [],
}

export const invoicesTable: TableMeta = {
  id: 'invoices',
  apiName: 'invoices',
  database: 'pg-tenant',
  physicalName: 'public.invoices',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'tenantId', physicalName: 'tenant_id', type: 'uuid', nullable: false },
    { apiName: 'orderId', physicalName: 'order_id', type: 'uuid', nullable: true },
    { apiName: 'amount', physicalName: 'amount', type: 'decimal', nullable: false, maskingFn: 'number' },
    { apiName: 'status', physicalName: 'status', type: 'string', nullable: false },
    { apiName: 'issuedAt', physicalName: 'issued_at', type: 'timestamp', nullable: false },
    { apiName: 'paidAt', physicalName: 'paid_at', type: 'timestamp', nullable: true },
    { apiName: 'dueDate', physicalName: 'due_date', type: 'date', nullable: true },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'tenantId', references: { table: 'tenants', column: 'id' }, type: 'many-to-one' },
    { column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' },
  ],
}

export const eventsTable: TableMeta = {
  id: 'events',
  apiName: 'events',
  database: 'ch-analytics',
  physicalName: 'default.events',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid', nullable: false },
    { apiName: 'orderId', physicalName: 'order_id', type: 'uuid', nullable: true },
    { apiName: 'eventType', physicalName: 'event_type', type: 'string', nullable: false },
    { apiName: 'payload', physicalName: 'payload', type: 'string', nullable: true, maskingFn: 'full' },
    { apiName: 'tags', physicalName: 'tags', type: 'string[]', nullable: false },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' },
    { column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' },
  ],
}

export const metricsTable: TableMeta = {
  id: 'metrics',
  apiName: 'metrics',
  database: 'ch-analytics',
  physicalName: 'default.metrics',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'name', physicalName: 'metric_name', type: 'string', nullable: false },
    { apiName: 'value', physicalName: 'value', type: 'decimal', nullable: false },
    { apiName: 'tags', physicalName: 'tags', type: 'string[]', nullable: true },
    { apiName: 'timestamp', physicalName: 'ts', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [],
}

export const ordersArchiveTable: TableMeta = {
  id: 'ordersArchive',
  apiName: 'ordersArchive',
  database: 'iceberg-wh',
  physicalName: 'warehouse.orders_archive',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid', nullable: false },
    { apiName: 'tenantId', physicalName: 'tenant_id', type: 'uuid', nullable: false },
    { apiName: 'customerId', physicalName: 'customer_id', type: 'uuid', nullable: false },
    { apiName: 'total', physicalName: 'total_amount', type: 'decimal', nullable: false },
    { apiName: 'status', physicalName: 'order_status', type: 'string', nullable: false },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp', nullable: false },
    { apiName: 'archivedAt', physicalName: 'archived_at', type: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'id', references: { table: 'orders', column: 'id' }, type: 'one-to-one' },
    { column: 'customerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' },
    { column: 'tenantId', references: { table: 'tenants', column: 'id' }, type: 'many-to-one' },
  ],
}

// --- External Syncs (3) ---

export const userSync: ExternalSync = {
  sourceTable: 'users',
  targetDatabase: 'ch-analytics',
  targetPhysicalName: 'default.users_replica',
  method: 'debezium',
  estimatedLag: 'seconds',
}

export const ordersSyncToCh: ExternalSync = {
  sourceTable: 'orders',
  targetDatabase: 'ch-analytics',
  targetPhysicalName: 'default.orders_replica',
  method: 'debezium',
  estimatedLag: 'seconds',
}

export const ordersSyncToIceberg: ExternalSync = {
  sourceTable: 'orders',
  targetDatabase: 'iceberg-wh',
  targetPhysicalName: 'warehouse.orders_current',
  method: 'debezium',
  estimatedLag: 'minutes',
}

export const tenantsSyncToPgMain: ExternalSync = {
  sourceTable: 'tenants',
  targetDatabase: 'pg-main',
  targetPhysicalName: 'replicas.tenants',
  method: 'debezium',
  estimatedLag: 'seconds',
}

// --- Caches (2) ---

export const userCache: CacheMeta = {
  id: 'redis-users',
  engine: 'redis',
  tables: [{ tableId: 'users', keyPattern: 'users:{id}' }],
}

export const productsCache: CacheMeta = {
  id: 'redis-products',
  engine: 'redis',
  tables: [{ tableId: 'products', keyPattern: 'products:{id}', columns: ['id', 'name', 'category'] }],
}

// --- Roles (7) ---

export const adminRole: RoleMeta = { id: 'admin', tables: '*' }

export const tenantUserRole: RoleMeta = {
  id: 'tenant-user',
  tables: [
    {
      tableId: 'orders',
      allowedColumns: ['id', 'amount', 'status', 'createdAt'],
      maskedColumns: ['amount'],
    },
    {
      tableId: 'users',
      allowedColumns: ['id', 'name', 'email'],
      maskedColumns: ['email'],
    },
    { tableId: 'products', allowedColumns: ['id', 'name', 'category', 'price'] },
  ],
}

export const regionalManagerRole: RoleMeta = {
  id: 'regional-manager',
  tables: [
    { tableId: 'orders', allowedColumns: '*' },
    { tableId: 'users', allowedColumns: '*', maskedColumns: ['phone', 'email'] },
    { tableId: 'products', allowedColumns: '*' },
  ],
}

export const analyticsReaderRole: RoleMeta = {
  id: 'analytics-reader',
  tables: [
    { tableId: 'events', allowedColumns: '*', maskedColumns: ['payload'] },
    { tableId: 'metrics', allowedColumns: '*' },
    { tableId: 'ordersArchive', allowedColumns: '*' },
  ],
}

export const noAccessRole: RoleMeta = { id: 'no-access', tables: [] }

export const ordersServiceRole: RoleMeta = {
  id: 'orders-service',
  tables: [
    { tableId: 'orders', allowedColumns: '*' },
    { tableId: 'products', allowedColumns: '*' },
    { tableId: 'users', allowedColumns: ['id', 'name'] },
  ],
}

export const fullServiceRole: RoleMeta = { id: 'full-service', tables: '*' }

export const allConceptRoles: RoleMeta[] = [
  adminRole,
  tenantUserRole,
  regionalManagerRole,
  analyticsReaderRole,
  noAccessRole,
  ordersServiceRole,
  fullServiceRole,
]

// --- Valid Config ---

export function validConfig(): MetadataConfig {
  return {
    databases: [pgMain, pgTenant, chAnalytics, icebergWarehouse],
    tables: [
      usersTable,
      ordersTable,
      productsTable,
      tenantsTable,
      invoicesTable,
      eventsTable,
      metricsTable,
      ordersArchiveTable,
    ],
    caches: [userCache, productsCache],
    externalSyncs: [userSync, ordersSyncToCh, ordersSyncToIceberg, tenantsSyncToPgMain],
    trino: { enabled: true },
  }
}
