import type { MetadataConfig, RoleMeta } from '@mkven/multi-db-validation'

// ── Databases ──────────────────────────────────────────────────

const databases: MetadataConfig['databases'] = [
  { id: 'pg-main', engine: 'postgres', trinoCatalog: 'pg_main' },
  { id: 'ch-analytics', engine: 'clickhouse', trinoCatalog: 'ch_analytics' },
]

// ── Tables ─────────────────────────────────────────────────────

const orders = {
  id: 'orders',
  apiName: 'orders',
  database: 'pg-main',
  physicalName: 'public.orders',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false },
    {
      apiName: 'customerId',
      physicalName: 'customer_id',
      type: 'uuid' as const,
      nullable: false,
      maskingFn: 'uuid' as const,
    },
    { apiName: 'productId', physicalName: 'product_id', type: 'uuid' as const, nullable: true },
    {
      apiName: 'total',
      physicalName: 'total_amount',
      type: 'decimal' as const,
      nullable: false,
      maskingFn: 'number' as const,
    },
    { apiName: 'discount', physicalName: 'discount', type: 'decimal' as const, nullable: true },
    { apiName: 'status', physicalName: 'order_status', type: 'string' as const, nullable: false },
    {
      apiName: 'internalNote',
      physicalName: 'internal_note',
      type: 'string' as const,
      nullable: true,
      maskingFn: 'full' as const,
    },
    {
      apiName: 'createdAt',
      physicalName: 'created_at',
      type: 'timestamp' as const,
      nullable: false,
      maskingFn: 'date' as const,
    },
    { apiName: 'quantity', physicalName: 'quantity', type: 'int' as const, nullable: false },
    { apiName: 'isPaid', physicalName: 'is_paid', type: 'boolean' as const, nullable: true },
    { apiName: 'priorities', physicalName: 'priorities', type: 'int[]' as const, nullable: true },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'customerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const },
    { column: 'productId', references: { table: 'products', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

const products = {
  id: 'products',
  apiName: 'products',
  database: 'pg-main',
  physicalName: 'public.products',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid' as const, nullable: false },
    { apiName: 'name', physicalName: 'name', type: 'string' as const, nullable: false },
    { apiName: 'category', physicalName: 'category', type: 'string' as const, nullable: false },
    {
      apiName: 'price',
      physicalName: 'price',
      type: 'decimal' as const,
      nullable: false,
      maskingFn: 'number' as const,
    },
    { apiName: 'labels', physicalName: 'labels', type: 'string[]' as const, nullable: true },
  ],
  primaryKey: ['id'],
  relations: [],
} satisfies MetadataConfig['tables'][number]

const users = {
  id: 'users',
  apiName: 'users',
  database: 'pg-main',
  physicalName: 'public.users',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid' as const, nullable: false },
    { apiName: 'email', physicalName: 'email', type: 'string' as const, nullable: false, maskingFn: 'email' as const },
    { apiName: 'phone', physicalName: 'phone', type: 'string' as const, nullable: true, maskingFn: 'phone' as const },
    {
      apiName: 'firstName',
      physicalName: 'first_name',
      type: 'string' as const,
      nullable: false,
      maskingFn: 'name' as const,
    },
    {
      apiName: 'lastName',
      physicalName: 'last_name',
      type: 'string' as const,
      nullable: false,
      maskingFn: 'name' as const,
    },
    { apiName: 'role', physicalName: 'role', type: 'string' as const, nullable: false },
    { apiName: 'age', physicalName: 'age', type: 'int' as const, nullable: true },
    { apiName: 'managerId', physicalName: 'manager_id', type: 'uuid' as const, nullable: true },
    { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp' as const, nullable: false },
  ],
  primaryKey: ['id'],
  relations: [{ column: 'managerId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const }],
} satisfies MetadataConfig['tables'][number]

const invoices = {
  id: 'invoices',
  apiName: 'invoices',
  database: 'pg-main',
  physicalName: 'public.invoices',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid' as const, nullable: false },
    { apiName: 'orderId', physicalName: 'order_id', type: 'int' as const, nullable: true },
    {
      apiName: 'amount',
      physicalName: 'amount',
      type: 'decimal' as const,
      nullable: false,
      maskingFn: 'number' as const,
    },
    { apiName: 'status', physicalName: 'status', type: 'string' as const, nullable: false },
    { apiName: 'issuedAt', physicalName: 'issued_at', type: 'timestamp' as const, nullable: false },
    { apiName: 'paidAt', physicalName: 'paid_at', type: 'timestamp' as const, nullable: true },
    { apiName: 'dueDate', physicalName: 'due_date', type: 'date' as const, nullable: true },
  ],
  primaryKey: ['id'],
  relations: [{ column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' as const }],
} satisfies MetadataConfig['tables'][number]

const events = {
  id: 'events',
  apiName: 'events',
  database: 'ch-analytics',
  physicalName: 'default.events',
  columns: [
    { apiName: 'id', physicalName: 'id', type: 'uuid' as const, nullable: false },
    { apiName: 'type', physicalName: 'event_type', type: 'string' as const, nullable: false },
    { apiName: 'userId', physicalName: 'user_id', type: 'uuid' as const, nullable: false },
    { apiName: 'orderId', physicalName: 'order_id', type: 'int' as const, nullable: true },
    {
      apiName: 'payload',
      physicalName: 'payload',
      type: 'string' as const,
      nullable: true,
      maskingFn: 'full' as const,
    },
    { apiName: 'tags', physicalName: 'tags', type: 'string[]' as const, nullable: true },
    { apiName: 'timestamp', physicalName: 'event_ts', type: 'timestamp' as const, nullable: false },
  ],
  primaryKey: ['id'],
  relations: [
    { column: 'userId', references: { table: 'users', column: 'id' }, type: 'many-to-one' as const },
    { column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

const orderItems = {
  id: 'orderItems',
  apiName: 'orderItems',
  database: 'pg-main',
  physicalName: 'public.order_items',
  columns: [
    { apiName: 'orderId', physicalName: 'order_id', type: 'int' as const, nullable: false },
    { apiName: 'productId', physicalName: 'product_id', type: 'uuid' as const, nullable: false },
    { apiName: 'quantity', physicalName: 'quantity', type: 'int' as const, nullable: false },
    { apiName: 'unitPrice', physicalName: 'unit_price', type: 'decimal' as const, nullable: false },
  ],
  primaryKey: ['orderId', 'productId'],
  relations: [
    { column: 'orderId', references: { table: 'orders', column: 'id' }, type: 'many-to-one' as const },
    { column: 'productId', references: { table: 'products', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

// ── Mirror tables for parameterized testing ────────────────────

const sampleColumns = [
  { apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false },
  { apiName: 'name', physicalName: 'name', type: 'string' as const, nullable: false },
  { apiName: 'email', physicalName: 'email', type: 'string' as const, nullable: false },
  { apiName: 'category', physicalName: 'category', type: 'string' as const, nullable: false },
  { apiName: 'amount', physicalName: 'amount', type: 'decimal' as const, nullable: false },
  { apiName: 'discount', physicalName: 'discount', type: 'decimal' as const, nullable: true },
  { apiName: 'status', physicalName: 'status', type: 'string' as const, nullable: false },
  { apiName: 'tags', physicalName: 'tags', type: 'string[]' as const, nullable: true },
  { apiName: 'scores', physicalName: 'scores', type: 'int[]' as const, nullable: true },
  { apiName: 'isActive', physicalName: 'is_active', type: 'boolean' as const, nullable: true },
  { apiName: 'note', physicalName: 'note', type: 'string' as const, nullable: true },
  { apiName: 'createdAt', physicalName: 'created_at', type: 'timestamp' as const, nullable: false },
  { apiName: 'dueDate', physicalName: 'due_date', type: 'date' as const, nullable: true },
  { apiName: 'externalId', physicalName: 'external_id', type: 'uuid' as const, nullable: false },
  { apiName: 'managerId', physicalName: 'manager_id', type: 'int' as const, nullable: true },
]

const sampleItemColumns = [
  { apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false },
  { apiName: 'sampleId', physicalName: 'sample_id', type: 'int' as const, nullable: false },
  { apiName: 'label', physicalName: 'label', type: 'string' as const, nullable: false },
  { apiName: 'category', physicalName: 'category', type: 'string' as const, nullable: false },
  { apiName: 'amount', physicalName: 'amount', type: 'decimal' as const, nullable: false },
  { apiName: 'quantity', physicalName: 'quantity', type: 'int' as const, nullable: false },
  { apiName: 'status', physicalName: 'status', type: 'string' as const, nullable: false },
]

const sampleDetailColumns = [
  { apiName: 'id', physicalName: 'id', type: 'int' as const, nullable: false },
  { apiName: 'sampleItemId', physicalName: 'sample_item_id', type: 'int' as const, nullable: false },
  { apiName: 'info', physicalName: 'info', type: 'string' as const, nullable: true },
]

const samples = {
  id: 'samples',
  apiName: 'samples',
  database: 'pg-main',
  physicalName: 'public.samples',
  columns: [...sampleColumns],
  primaryKey: ['id'],
  relations: [{ column: 'managerId', references: { table: 'samples', column: 'id' }, type: 'many-to-one' as const }],
} satisfies MetadataConfig['tables'][number]

const chSamples = {
  id: 'chSamples',
  apiName: 'chSamples',
  database: 'ch-analytics',
  physicalName: 'default.samples',
  columns: [...sampleColumns],
  primaryKey: ['id'],
  relations: [
    { column: 'id', references: { table: 'samples', column: 'id' }, type: 'one-to-one' as const },
    { column: 'managerId', references: { table: 'chSamples', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

const sampleItems = {
  id: 'sampleItems',
  apiName: 'sampleItems',
  database: 'pg-main',
  physicalName: 'public.sample_items',
  columns: [...sampleItemColumns],
  primaryKey: ['id'],
  relations: [{ column: 'sampleId', references: { table: 'samples', column: 'id' }, type: 'many-to-one' as const }],
} satisfies MetadataConfig['tables'][number]

const chSampleItems = {
  id: 'chSampleItems',
  apiName: 'chSampleItems',
  database: 'ch-analytics',
  physicalName: 'default.sample_items',
  columns: [...sampleItemColumns],
  primaryKey: ['id'],
  relations: [{ column: 'sampleId', references: { table: 'chSamples', column: 'id' }, type: 'many-to-one' as const }],
} satisfies MetadataConfig['tables'][number]

const sampleDetails = {
  id: 'sampleDetails',
  apiName: 'sampleDetails',
  database: 'pg-main',
  physicalName: 'public.sample_details',
  columns: [...sampleDetailColumns],
  primaryKey: ['id'],
  relations: [
    { column: 'sampleItemId', references: { table: 'sampleItems', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

const chSampleDetails = {
  id: 'chSampleDetails',
  apiName: 'chSampleDetails',
  database: 'ch-analytics',
  physicalName: 'default.sample_details',
  columns: [...sampleDetailColumns],
  primaryKey: ['id'],
  relations: [
    { column: 'sampleItemId', references: { table: 'chSampleItems', column: 'id' }, type: 'many-to-one' as const },
  ],
} satisfies MetadataConfig['tables'][number]

// ── Metadata Config ────────────────────────────────────────────

export const metadata: MetadataConfig = {
  databases,
  tables: [
    orders,
    products,
    users,
    invoices,
    events,
    orderItems,
    samples,
    chSamples,
    sampleItems,
    chSampleItems,
    sampleDetails,
    chSampleDetails,
  ],
  caches: [
    {
      id: 'redis-main',
      engine: 'redis',
      tables: [{ tableId: 'users', keyPattern: 'users:{id}' }],
    },
  ],
  externalSyncs: [
    {
      sourceTable: 'orders',
      targetDatabase: 'ch-analytics',
      targetPhysicalName: 'default.orders_replica',
      method: 'debezium',
      estimatedLag: 'seconds',
    },
  ],
  trino: { enabled: true },
}

// ── Roles ──────────────────────────────────────────────────────

export const roles: RoleMeta[] = [
  { id: 'admin', tables: '*' },
  {
    id: 'tenant-user',
    tables: [
      { tableId: 'orders', allowedColumns: ['id', 'total', 'status', 'createdAt'], maskedColumns: ['total'] },
      { tableId: 'users', allowedColumns: ['id', 'firstName', 'lastName', 'email'], maskedColumns: ['email'] },
      { tableId: 'products', allowedColumns: ['id', 'name', 'category', 'price'] },
    ],
  },
  {
    id: 'analyst',
    tables: [
      {
        tableId: 'orders',
        allowedColumns: ['id', 'total', 'status', 'internalNote', 'createdAt', 'customerId'],
        maskedColumns: ['internalNote', 'createdAt', 'customerId'],
      },
      {
        tableId: 'users',
        allowedColumns: ['id', 'firstName', 'lastName', 'email', 'phone'],
        maskedColumns: ['phone', 'firstName', 'lastName'],
      },
      { tableId: 'products', allowedColumns: ['id', 'name', 'category', 'price'], maskedColumns: ['price'] },
      { tableId: 'invoices', allowedColumns: ['id', 'orderId', 'amount', 'status'], maskedColumns: ['amount'] },
    ],
  },
  {
    id: 'viewer',
    tables: [
      { tableId: 'orders', allowedColumns: ['id', 'status', 'createdAt', 'quantity'] },
      { tableId: 'users', allowedColumns: ['id', 'firstName'] },
    ],
  },
  { id: 'no-access', tables: [] },
  {
    id: 'orders-service',
    tables: [
      { tableId: 'orders', allowedColumns: '*' },
      { tableId: 'products', allowedColumns: '*' },
      { tableId: 'users', allowedColumns: ['id', 'firstName', 'lastName'] },
    ],
  },
  {
    id: 'reporting-service',
    tables: [
      { tableId: 'orders', allowedColumns: ['id', 'total', 'status', 'createdAt'], maskedColumns: ['total'] },
      { tableId: 'products', allowedColumns: '*' },
    ],
  },
]

// ── Seed Data ──────────────────────────────────────────────────

export const seedOrders = [
  {
    id: 1,
    customer_id: '00000000-0000-4000-a000-000000000c01',
    product_id: '00000000-0000-4000-a000-0000000000a1',
    total_amount: 100.0,
    discount: 10.0,
    order_status: 'active',
    internal_note: 'internal-1',
    created_at: '2024-01-15T10:00:00Z',
    quantity: 2,
    is_paid: true,
    priorities: [1, 2],
  },
  {
    id: 2,
    customer_id: '00000000-0000-4000-a000-000000000c02',
    product_id: '00000000-0000-4000-a000-0000000000a2',
    total_amount: 200.0,
    discount: null,
    order_status: 'paid',
    internal_note: null,
    created_at: '2024-02-20T14:30:00Z',
    quantity: 5,
    is_paid: true,
    priorities: [3],
  },
  {
    id: 3,
    customer_id: '00000000-0000-4000-a000-000000000c01',
    product_id: '00000000-0000-4000-a000-0000000000a1',
    total_amount: 50.0,
    discount: 5.0,
    order_status: 'cancelled',
    internal_note: 'internal-3',
    created_at: '2024-03-10T08:15:00Z',
    quantity: 1,
    is_paid: false,
    priorities: null,
  },
  {
    id: 4,
    customer_id: '00000000-0000-4000-a000-000000000c03',
    product_id: null,
    total_amount: 300.0,
    discount: null,
    order_status: 'active',
    internal_note: null,
    created_at: '2024-04-05T16:45:00Z',
    quantity: 10,
    is_paid: null,
    priorities: [],
  },
  {
    id: 5,
    customer_id: '00000000-0000-4000-a000-000000000c02',
    product_id: '00000000-0000-4000-a000-0000000000a3',
    total_amount: 150.0,
    discount: 0.0,
    order_status: 'shipped',
    internal_note: 'internal-5',
    created_at: '2024-05-12T12:00:00Z',
    quantity: 3,
    is_paid: true,
    priorities: [1, 2, 3],
  },
]

export const seedProducts = [
  {
    id: '00000000-0000-4000-a000-0000000000a1',
    name: 'Widget A',
    category: 'electronics',
    price: 25.0,
    labels: ['sale', 'new'],
  },
  {
    id: '00000000-0000-4000-a000-0000000000a2',
    name: 'Widget B',
    category: 'clothing',
    price: 40.0,
    labels: ['clearance'],
  },
  { id: '00000000-0000-4000-a000-0000000000a3', name: 'Widget C', category: 'electronics', price: 15.0, labels: null },
]

export const seedUsers = [
  {
    id: '00000000-0000-4000-a000-000000000c01',
    email: 'alice@example.com',
    phone: '+1234567890',
    first_name: 'Alice',
    last_name: 'Smith',
    role: 'admin',
    age: 30,
    manager_id: null,
    created_at: '2023-01-01T00:00:00Z',
  },
  {
    id: '00000000-0000-4000-a000-000000000c02',
    email: 'bob@example.com',
    phone: null,
    first_name: 'Bob',
    last_name: 'Jones',
    role: 'viewer',
    age: 25,
    manager_id: '00000000-0000-4000-a000-000000000c01',
    created_at: '2023-06-15T00:00:00Z',
  },
  {
    id: '00000000-0000-4000-a000-000000000c03',
    email: 'carol@example.com',
    phone: '+9876543210',
    first_name: 'Carol',
    last_name: 'Williams',
    role: 'viewer',
    age: null,
    manager_id: '00000000-0000-4000-a000-000000000c01',
    created_at: '2024-01-01T00:00:00Z',
  },
]

export const seedInvoices = [
  {
    id: '00000000-0000-4000-a000-000000000b01',
    order_id: 1,
    amount: 100.0,
    status: 'paid',
    issued_at: '2024-01-20T00:00:00Z',
    paid_at: '2024-01-25T00:00:00Z',
    due_date: '2024-02-20',
  },
  {
    id: '00000000-0000-4000-a000-000000000b02',
    order_id: 2,
    amount: 200.0,
    status: 'pending',
    issued_at: '2024-02-25T00:00:00Z',
    paid_at: null,
    due_date: '2024-03-25',
  },
  {
    id: '00000000-0000-4000-a000-000000000b03',
    order_id: 1,
    amount: 50.0,
    status: 'paid',
    issued_at: '2024-01-22T00:00:00Z',
    paid_at: '2024-01-28T00:00:00Z',
    due_date: null,
  },
]

export const seedEvents = [
  {
    id: '00000000-0000-4000-a000-000000000e01',
    event_type: 'purchase',
    user_id: '00000000-0000-4000-a000-000000000c01',
    order_id: 1,
    payload: '{"action":"buy"}',
    tags: ['urgent', 'vip'],
    event_ts: '2024-01-15T10:05:00Z',
  },
  {
    id: '00000000-0000-4000-a000-000000000e02',
    event_type: 'view',
    user_id: '00000000-0000-4000-a000-000000000c02',
    order_id: null,
    payload: null,
    tags: null,
    event_ts: '2024-02-20T14:00:00Z',
  },
  {
    id: '00000000-0000-4000-a000-000000000e03',
    event_type: 'purchase',
    user_id: '00000000-0000-4000-a000-000000000c01',
    order_id: 3,
    payload: '{"action":"buy"}',
    tags: ['urgent'],
    event_ts: '2024-03-10T08:20:00Z',
  },
]

export const seedOrderItems = [
  { order_id: 1, product_id: '00000000-0000-4000-a000-0000000000a1', quantity: 2, unit_price: 25.0 },
  { order_id: 1, product_id: '00000000-0000-4000-a000-0000000000a2', quantity: 1, unit_price: 40.0 },
  { order_id: 2, product_id: '00000000-0000-4000-a000-0000000000a2', quantity: 5, unit_price: 40.0 },
  { order_id: 5, product_id: '00000000-0000-4000-a000-0000000000a3', quantity: 3, unit_price: 15.0 },
]

export const seedSamples = [
  {
    id: 1,
    name: 'Alpha',
    email: 'alpha@test.com',
    category: 'electronics',
    amount: 100.0,
    discount: 10.0,
    status: 'active',
    tags: ['fast', 'new'],
    scores: [1, 2],
    is_active: true,
    note: 'note-1',
    created_at: '2024-01-15T10:00:00Z',
    due_date: '2024-02-20',
    external_id: '00000000-0000-4000-a000-000000000501',
    manager_id: null,
  },
  {
    id: 2,
    name: 'Beta',
    email: 'beta@test.com',
    category: 'clothing',
    amount: 200.0,
    discount: null,
    status: 'paid',
    tags: ['slow'],
    scores: [3],
    is_active: true,
    note: null,
    created_at: '2024-02-20T14:30:00Z',
    due_date: '2024-03-25',
    external_id: '00000000-0000-4000-a000-000000000502',
    manager_id: 1,
  },
  {
    id: 3,
    name: 'Gamma',
    email: 'gamma@test.com',
    category: 'electronics',
    amount: 50.0,
    discount: 5.0,
    status: 'cancelled',
    tags: ['fast'],
    scores: null,
    is_active: false,
    note: 'note-3',
    created_at: '2024-03-10T08:15:00Z',
    due_date: null,
    external_id: '00000000-0000-4000-a000-000000000503',
    manager_id: 1,
  },
  {
    id: 4,
    name: 'Delta',
    email: 'delta@test.com',
    category: 'food',
    amount: 300.0,
    discount: null,
    status: 'active',
    tags: null,
    scores: [],
    is_active: null,
    note: null,
    created_at: '2024-04-05T16:45:00Z',
    due_date: '2024-05-01',
    external_id: '00000000-0000-4000-a000-000000000504',
    manager_id: null,
  },
  {
    id: 5,
    name: 'Epsilon',
    email: 'epsilon@test.com',
    category: 'electronics',
    amount: 150.0,
    discount: 0.0,
    status: 'shipped',
    tags: ['fast', 'slow', 'new'],
    scores: [1, 2, 3],
    is_active: true,
    note: 'note-5',
    created_at: '2024-05-12T12:00:00Z',
    due_date: '2024-06-15',
    external_id: '00000000-0000-4000-a000-000000000505',
    manager_id: 2,
  },
]

export const seedSampleItems = [
  { id: 1, sample_id: 1, label: 'item-A', category: 'electronics', amount: 25.0, quantity: 2, status: 'active' },
  { id: 2, sample_id: 1, label: 'item-B', category: 'clothing', amount: 120.0, quantity: 1, status: 'active' },
  { id: 3, sample_id: 2, label: 'item-C', category: 'clothing', amount: 40.0, quantity: 5, status: 'paid' },
  { id: 4, sample_id: 3, label: 'item-D', category: 'electronics', amount: 60.0, quantity: 3, status: 'cancelled' },
  { id: 5, sample_id: 5, label: 'item-E', category: 'food', amount: 10.0, quantity: 1, status: 'active' },
  { id: 6, sample_id: 5, label: 'item-F', category: 'electronics', amount: 20.0, quantity: 2, status: 'paid' },
]

export const seedSampleDetails = [
  { id: 1, sample_item_id: 1, info: 'detail-1' },
  { id: 2, sample_item_id: 2, info: null },
  { id: 3, sample_item_id: 3, info: 'detail-3' },
  { id: 4, sample_item_id: 5, info: 'detail-4' },
]
