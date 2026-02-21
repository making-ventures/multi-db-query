-- ClickHouse seed data for multi-db contract tests
-- Must match fixture.ts exactly
-- Database 'multidb' is created via CLICKHOUSE_DB env var

-- ── events ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS default.events (
  id        UUID         NOT NULL,
  event_type String      NOT NULL,
  user_id   UUID         NOT NULL,
  order_id  Nullable(Int32),
  payload   Nullable(String),
  tags      Array(String),
  event_ts  DateTime64(3) NOT NULL
) ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO default.events (id, event_type, user_id, order_id, payload, tags, event_ts) VALUES
  ('00000000-0000-4000-a000-000000000e01', 'purchase', '00000000-0000-4000-a000-000000000c01', 1,    '{"action":"buy"}', ['urgent', 'vip'], '2024-01-15 10:05:00'),
  ('00000000-0000-4000-a000-000000000e02', 'view',     '00000000-0000-4000-a000-000000000c02', NULL,  NULL,               [],                '2024-02-20 14:00:00'),
  ('00000000-0000-4000-a000-000000000e03', 'purchase', '00000000-0000-4000-a000-000000000c01', 3,    '{"action":"buy"}', ['urgent'],         '2024-03-10 08:20:00');

-- ── samples ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS default.samples (
  id          Int32          NOT NULL,
  name        String         NOT NULL,
  email       String         NOT NULL,
  category    String         NOT NULL,
  amount      Decimal(18, 2) NOT NULL,
  discount    Nullable(Decimal(18, 2)),
  status      String         NOT NULL,
  tags        Array(String),
  scores      Array(Int32),
  is_active   Nullable(Bool),
  note        Nullable(String),
  created_at  DateTime64(3)  NOT NULL,
  due_date    Nullable(Date),
  external_id UUID           NOT NULL,
  manager_id  Nullable(Int32)
) ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO default.samples (id, name, email, category, amount, discount, status, tags, scores, is_active, note, created_at, due_date, external_id, manager_id) VALUES
  (1, 'Alpha',   'alpha@test.com',   'electronics', 100.00, 10.00, 'active',    ['fast', 'new'],          [1, 2],    true,  'note-1', '2024-01-15 10:00:00', '2024-02-20', '00000000-0000-4000-a000-000000000501', NULL),
  (2, 'Beta',    'beta@test.com',    'clothing',    200.00, NULL,  'paid',       ['slow'],                 [3],       true,  NULL,     '2024-02-20 14:30:00', '2024-03-25', '00000000-0000-4000-a000-000000000502', 1),
  (3, 'Gamma',   'gamma@test.com',   'electronics', 50.00,  5.00,  'cancelled', ['fast'],                 [],        false, 'note-3', '2024-03-10 08:15:00', NULL,         '00000000-0000-4000-a000-000000000503', 1),
  (4, 'Delta',   'delta@test.com',   'food',        300.00, NULL,  'active',    [],                       [],        NULL,  NULL,     '2024-04-05 16:45:00', '2024-05-01', '00000000-0000-4000-a000-000000000504', NULL),
  (5, 'Epsilon', 'epsilon@test.com', 'electronics', 150.00, 0.00,  'shipped',   ['fast', 'slow', 'new'],  [1, 2, 3], true,  'note-5', '2024-05-12 12:00:00', '2024-06-15', '00000000-0000-4000-a000-000000000505', 2);

-- ── sample_items ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS default.sample_items (
  id        Int32          NOT NULL,
  sample_id Int32          NOT NULL,
  label     String         NOT NULL,
  category  String         NOT NULL,
  amount    Decimal(18, 2) NOT NULL,
  quantity  Int32          NOT NULL,
  status    String         NOT NULL
) ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO default.sample_items (id, sample_id, label, category, amount, quantity, status) VALUES
  (1, 1, 'item-A', 'electronics', 25.00,  2, 'active'),
  (2, 1, 'item-B', 'clothing',    120.00, 1, 'active'),
  (3, 2, 'item-C', 'clothing',    40.00,  5, 'paid'),
  (4, 3, 'item-D', 'electronics', 60.00,  3, 'cancelled'),
  (5, 5, 'item-E', 'food',        10.00,  1, 'active'),
  (6, 5, 'item-F', 'electronics', 20.00,  2, 'paid');

-- ── sample_details ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS default.sample_details (
  id             Int32 NOT NULL,
  sample_item_id Int32 NOT NULL,
  info           Nullable(String)
) ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO default.sample_details (id, sample_item_id, info) VALUES
  (1, 1, 'detail-1'),
  (2, 2, NULL),
  (3, 3, 'detail-3'),
  (4, 5, 'detail-4');

-- ── orders_replica ─────────────────────────────────────────────
-- External sync target (debezium replica of PG orders)

CREATE TABLE IF NOT EXISTS default.orders_replica (
  id            Int32          NOT NULL,
  customer_id   UUID           NOT NULL,
  product_id    Nullable(UUID),
  total_amount  Decimal(18, 2) NOT NULL,
  discount      Nullable(Decimal(18, 2)),
  order_status  String         NOT NULL,
  internal_note Nullable(String),
  created_at    DateTime64(3)  NOT NULL,
  quantity      Int32          NOT NULL,
  is_paid       Nullable(Bool),
  priorities    Array(Int32)
) ENGINE = MergeTree()
ORDER BY (id);
