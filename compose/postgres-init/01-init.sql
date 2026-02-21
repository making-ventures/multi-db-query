-- PostgreSQL seed data for multi-db contract tests
-- Must match fixture.ts exactly

-- Enable fuzzystrmatch extension (required for levenshtein function)
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- ── orders ─────────────────────────────────────────────────────

CREATE TABLE public.orders (
  id            INTEGER      PRIMARY KEY,
  customer_id   UUID         NOT NULL,
  product_id    UUID,
  total_amount  DECIMAL      NOT NULL,
  discount      DECIMAL,
  order_status  VARCHAR      NOT NULL,
  internal_note VARCHAR,
  created_at    TIMESTAMPTZ  NOT NULL,
  quantity      INTEGER      NOT NULL,
  is_paid       BOOLEAN,
  priorities    INTEGER[]
);

INSERT INTO public.orders (id, customer_id, product_id, total_amount, discount, order_status, internal_note, created_at, quantity, is_paid, priorities) VALUES
  (1, '00000000-0000-4000-a000-000000000c01', '00000000-0000-4000-a000-0000000000a1', 100.00, 10.00, 'active',    'internal-1', '2024-01-15T10:00:00Z', 2,  true,  ARRAY[1, 2]),
  (2, '00000000-0000-4000-a000-000000000c02', '00000000-0000-4000-a000-0000000000a2', 200.00, NULL,   'paid',      NULL,         '2024-02-20T14:30:00Z', 5,  true,  ARRAY[3]),
  (3, '00000000-0000-4000-a000-000000000c01', '00000000-0000-4000-a000-0000000000a1', 50.00,  5.00,   'cancelled', 'internal-3', '2024-03-10T08:15:00Z', 1,  false, NULL),
  (4, '00000000-0000-4000-a000-000000000c03', NULL,      300.00, NULL,   'active',    NULL,         '2024-04-05T16:45:00Z', 10, NULL,  ARRAY[]::INTEGER[]),
  (5, '00000000-0000-4000-a000-000000000c02', '00000000-0000-4000-a000-0000000000a3', 150.00, 0.00,   'shipped',   'internal-5', '2024-05-12T12:00:00Z', 3,  true,  ARRAY[1, 2, 3]);

-- ── products ───────────────────────────────────────────────────

CREATE TABLE public.products (
  id       UUID    PRIMARY KEY,
  name     VARCHAR NOT NULL,
  category VARCHAR NOT NULL,
  price    DECIMAL NOT NULL,
  labels   TEXT[]
);

INSERT INTO public.products (id, name, category, price, labels) VALUES
  ('00000000-0000-4000-a000-0000000000a1', 'Widget A', 'electronics', 25.00, ARRAY['sale', 'new']),
  ('00000000-0000-4000-a000-0000000000a2', 'Widget B', 'clothing',    40.00, ARRAY['clearance']),
  ('00000000-0000-4000-a000-0000000000a3', 'Widget C', 'electronics', 15.00, NULL);

-- ── users ──────────────────────────────────────────────────────

CREATE TABLE public.users (
  id         UUID        PRIMARY KEY,
  email      VARCHAR     NOT NULL,
  phone      VARCHAR,
  first_name VARCHAR     NOT NULL,
  last_name  VARCHAR     NOT NULL,
  role       VARCHAR     NOT NULL,
  age        INTEGER,
  manager_id UUID,
  created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO public.users (id, email, phone, first_name, last_name, role, age, manager_id, created_at) VALUES
  ('00000000-0000-4000-a000-000000000c01', 'alice@example.com', '+1234567890',  'Alice', 'Smith',    'admin',  30,   NULL,      '2023-01-01T00:00:00Z'),
  ('00000000-0000-4000-a000-000000000c02', 'bob@example.com',   NULL,           'Bob',   'Jones',    'viewer', 25,   '00000000-0000-4000-a000-000000000c01', '2023-06-15T00:00:00Z'),
  ('00000000-0000-4000-a000-000000000c03', 'carol@example.com', '+9876543210',  'Carol', 'Williams', 'viewer', NULL, '00000000-0000-4000-a000-000000000c01', '2024-01-01T00:00:00Z');

-- ── invoices ───────────────────────────────────────────────────

CREATE TABLE public.invoices (
  id        UUID        PRIMARY KEY,
  order_id  INTEGER,
  amount    DECIMAL     NOT NULL,
  status    VARCHAR     NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  paid_at   TIMESTAMPTZ,
  due_date  DATE
);

INSERT INTO public.invoices (id, order_id, amount, status, issued_at, paid_at, due_date) VALUES
  ('00000000-0000-4000-a000-000000000b01', 1, 100.00, 'paid',    '2024-01-20T00:00:00Z', '2024-01-25T00:00:00Z', '2024-02-20'),
  ('00000000-0000-4000-a000-000000000b02', 2, 200.00, 'pending', '2024-02-25T00:00:00Z', NULL,                   '2024-03-25'),
  ('00000000-0000-4000-a000-000000000b03', 1, 50.00,  'paid',    '2024-01-22T00:00:00Z', '2024-01-28T00:00:00Z', NULL);

-- ── order_items ────────────────────────────────────────────────

CREATE TABLE public.order_items (
  order_id   INTEGER NOT NULL,
  product_id UUID    NOT NULL,
  quantity   INTEGER NOT NULL,
  unit_price DECIMAL NOT NULL,
  PRIMARY KEY (order_id, product_id)
);

INSERT INTO public.order_items (order_id, product_id, quantity, unit_price) VALUES
  (1, '00000000-0000-4000-a000-0000000000a1', 2, 25.00),
  (1, '00000000-0000-4000-a000-0000000000a2', 1, 40.00),
  (2, '00000000-0000-4000-a000-0000000000a2', 5, 40.00),
  (5, '00000000-0000-4000-a000-0000000000a3', 3, 15.00);

-- ── samples ────────────────────────────────────────────────────

CREATE TABLE public.samples (
  id          INTEGER     PRIMARY KEY,
  name        VARCHAR     NOT NULL,
  email       VARCHAR     NOT NULL,
  category    VARCHAR     NOT NULL,
  amount      DECIMAL     NOT NULL,
  discount    DECIMAL,
  status      VARCHAR     NOT NULL,
  tags        TEXT[],
  scores      INTEGER[],
  is_active   BOOLEAN,
  note        VARCHAR,
  created_at  TIMESTAMPTZ NOT NULL,
  due_date    DATE,
  external_id UUID        NOT NULL,
  manager_id  INTEGER
);

INSERT INTO public.samples (id, name, email, category, amount, discount, status, tags, scores, is_active, note, created_at, due_date, external_id, manager_id) VALUES
  (1, 'Alpha',   'alpha@test.com',   'electronics', 100.00, 10.00, 'active',    ARRAY['fast', 'new'],          ARRAY[1, 2],    true,  'note-1', '2024-01-15T10:00:00Z', '2024-02-20', '00000000-0000-4000-a000-000000000501', NULL),
  (2, 'Beta',    'beta@test.com',    'clothing',    200.00, NULL,  'paid',       ARRAY['slow'],                 ARRAY[3],       true,  NULL,     '2024-02-20T14:30:00Z', '2024-03-25', '00000000-0000-4000-a000-000000000502', 1),
  (3, 'Gamma',   'gamma@test.com',   'electronics', 50.00,  5.00,  'cancelled', ARRAY['fast'],                 NULL,           false, 'note-3', '2024-03-10T08:15:00Z', NULL,         '00000000-0000-4000-a000-000000000503', 1),
  (4, 'Delta',   'delta@test.com',   'food',        300.00, NULL,  'active',    NULL,                          ARRAY[]::INTEGER[], NULL,  NULL,     '2024-04-05T16:45:00Z', '2024-05-01', '00000000-0000-4000-a000-000000000504', NULL),
  (5, 'Epsilon', 'epsilon@test.com', 'electronics', 150.00, 0.00,  'shipped',   ARRAY['fast', 'slow', 'new'],  ARRAY[1, 2, 3], true,  'note-5', '2024-05-12T12:00:00Z', '2024-06-15', '00000000-0000-4000-a000-000000000505', 2);

-- ── sample_items ───────────────────────────────────────────────

CREATE TABLE public.sample_items (
  id        INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL,
  label     VARCHAR NOT NULL,
  category  VARCHAR NOT NULL,
  amount    DECIMAL NOT NULL,
  quantity  INTEGER NOT NULL,
  status    VARCHAR NOT NULL
);

INSERT INTO public.sample_items (id, sample_id, label, category, amount, quantity, status) VALUES
  (1, 1, 'item-A', 'electronics', 25.00,  2, 'active'),
  (2, 1, 'item-B', 'clothing',    120.00, 1, 'active'),
  (3, 2, 'item-C', 'clothing',    40.00,  5, 'paid'),
  (4, 3, 'item-D', 'electronics', 60.00,  3, 'cancelled'),
  (5, 5, 'item-E', 'food',        10.00,  1, 'active'),
  (6, 5, 'item-F', 'electronics', 20.00,  2, 'paid');

-- ── sample_details ─────────────────────────────────────────────

CREATE TABLE public.sample_details (
  id             INTEGER PRIMARY KEY,
  sample_item_id INTEGER NOT NULL,
  info           VARCHAR
);

INSERT INTO public.sample_details (id, sample_item_id, info) VALUES
  (1, 1, 'detail-1'),
  (2, 2, NULL),
  (3, 3, 'detail-3'),
  (4, 5, 'detail-4');
