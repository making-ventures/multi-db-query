# Audit Fixes — CONTRACT_TESTS.md / Implementation / Design Doc

> **Status: RESOLVED** — All actionable items completed. 469 tests pass, 0 skipped.

> **Note:** CONTRACT_TESTS.md has moved to the [concept repo](https://github.com/making-ventures/concept-multi-db-query-engine/blob/main/CONTRACT_TESTS.md).

Comprehensive list of inconsistencies found and fixed across:

1. **CONTRACT_TESTS.md** — test design document (401 test IDs)
2. **queryContract.ts / validationContract.ts** — test implementation (237 test IDs)
3. **Design doc** — `concept-multi-db-query-engine/README.md` (system spec)
4. **Implementation** — types, validators, resolvers, generators

---

## 1. CONTRACT_TESTS.md Semantic Fixes ✅

Places where CONTRACT_TESTS.md contradicted the design doc — **all corrected**.

### 1.1 HAVING uses `alias` instead of `column`

Design doc and implementation use `{ column: 'totalAmt' }`, but CONTRACT_TESTS.md says `{ alias: 'totalAmt' }`.

| ID | Section | Fix |
|----|---------|-----|
| C322 | 6. GROUP BY & HAVING | `alias` → `column` |
| C1414 | 16.6 HAVING injection | `alias` → `column` |
| C1448 | 16.9 aggregation injection | `alias` → `column` |
| C1450 | 16.9 aggregation injection | `alias` → `column` |
| C1465 | 16.10 combined injection | `alias` → `column` |

### 1.2 EXISTS uses non-existent `on: { left, right }` notation

Design doc defines EXISTS via `filters: [{ kind: 'exists', ... }]` with `definition.filters`. CONTRACT_TESTS.md uses a non-existent `on: { left, right }` join-style syntax.

| ID | Section | Fix |
|----|---------|-----|
| C1411 | 16.5 EXISTS injection | Rewrite to use `definition.filters` with `QueryExistsFilter` |
| C1464 | 16.10 combined injection | Rewrite to use `definition.filters` with `QueryExistsFilter` |

### 1.3 byIds uses non-existent object form

Design doc defines `byIds` as `scalar[]` (e.g., `byIds: [1, 2, 3]`). CONTRACT_TESTS.md uses object form `[{ orderId: 1, productId: 'uuid-p1' }]`.

| ID | Section | Fix |
|----|---------|-----|
| C505 | 8. byIds | Rewrite — byIds cannot address composite PKs with object form. Change to scalar test or negative validation test for composite PK table. |
| C993 | 12.9 byIds validity | Rewrite — same object-form issue. Change to a proper scalar `byIds` validation scenario. |

### 1.4 EXISTS at definition level instead of `filters[]`

| ID | Section | Fix |
|----|---------|-----|
| C607 | 9. EXISTS | `exists: false, count: { ... }` at definition level → move into `filters: [{ kind: 'exists', ... }]` |

---

## 2. Missing Tests — Gaps in Implemented Sections ✅

These test IDs were defined in CONTRACT_TESTS.md but missing from implementation — **all added**.

| ID | Section | Description | Blocked? |
|----|---------|-------------|----------|
| C323 | 6. GROUP BY & HAVING | HAVING with OR group: `(SUM(amount) > 250 OR AVG(amount) > 150)` → 2 groups | No |
| C325 | 6. GROUP BY & HAVING | HAVING with NOT BETWEEN: `totalAmt notBetween { from: 100, to: 300 }` → 2 groups | No |
| C326 | 6. GROUP BY & HAVING | HAVING with IS NULL: `SUM(discount) isNull` → 1 group | No |
| C327 | 6. GROUP BY & HAVING | NOT in HAVING group: `NOT (SUM(amount) > 100 OR COUNT(*) > 1)` → 1 group | No |
| C403 | 7. ORDER BY | ORDER BY joined column: samples JOIN sampleItems, orderBy sampleItems.category | No |
| C407 | 7. ORDER BY | DISTINCT + GROUP BY interaction: distinct has no effect when GROUP BY present | No |
| C603 | 9. EXISTS | EXISTS inside OR group: `(status = 'cancelled' OR EXISTS sampleItems)` → 4 rows | No |
| C609 | 9. EXISTS | EXISTS with join: samples JOIN sampleItems WHERE EXISTS samples (managerId) | No |
| C1608 | 17. Validation | No DB connection — server returns `{ valid: true }` without executors | No (validationContract.ts) |

---

## 3. Missing Tests — Needed CONTRACT_TESTS.md Redesign First ✅

These required CONTRACT_TESTS.md fixes before implementation — **all resolved**.

| ID | Section | Issue |
|----|---------|-------|
| C505 | 8. byIds | Fixed to use scalar `byIds: [1, 2]` — implemented |
| C607 | 9. EXISTS | Fixed to use `filters: [{ table: sampleItems, exists: false, count: ... }]` — implemented |
| C993 | 12.9 byIds validity | Removed — redundant with C992 after fixing object-form byIds |

---

## 4. Skipped Tests ✅

All 8 were skipped without comments — **all investigated, fixed, and unskipped**.

| ID | Line | What It Tests | Action Needed |
|----|------|---------------|---------------|
| C723 | queryContract.ts ~L478 | One scope with zero roles → ACCESS_DENIED | Unskipped — works via implicit access denial |
| C947 | queryContract.ts ~L1073 | `levenshteinLte` missing `text` → INVALID_VALUE | Fixed validator (added `text` check), unskipped |
| C950 | queryContract.ts ~L1083 | Column filter type mismatch → INVALID_FILTER | Unskipped — validator handles this |
| C960 | queryContract.ts ~L1120 | Join with no relation → INVALID_JOIN | Fixed table pair (orders→products, invoices unrelated), unskipped |
| C1010 | queryContract.ts ~L1423 | EXISTS on unrelated table → INVALID_EXISTS | Fixed table pair (products→invoices, unrelated), unskipped |
| C1011 | queryContract.ts ~L1432 | Counted EXISTS negative count → INVALID_EXISTS | Unskipped — validator handles this |
| C1012 | queryContract.ts ~L1441 | Counted EXISTS fractional count → INVALID_EXISTS | Unskipped — validator handles this |
| C1013 | queryContract.ts ~L1450 | Nested EXISTS invalid inner relation → INVALID_EXISTS | Unskipped — validator handles this |

---

## 5. Unimplemented Sections — 110 Test IDs

Entire sections of CONTRACT_TESTS.md with no implementation yet. These are future work.

| Section | IDs | Count | Topic |
|---------|-----|-------|-------|
| 14 | C1200–C1208 | 9 | Error deserialization (HTTP → typed errors) |
| 15 | C1300–C1316 | 17 | Planner / execution / provider errors |
| 16 | C1400–C1470 | 71 | SQL injection prevention |
| 17 | C1500–C1509 | 10 | Health check endpoint |
| 18 | C1700–C1703 | 3 | Edge cases |

**Total: 110 test IDs across 5 sections.**

---

## 6. Minor Issues ✅

### 6.1 Design doc masking example typo — **Fixed**

In the design doc (`concept-multi-db-query-engine/README.md`), the `name` masking example shows:

```
"John Smith" → "J*********h"  (9 stars)
```

Should be 8 stars (`"J********h"`) since `"John Smith"` is 10 chars, minus first and last = 8.

### 6.2 Weak assertion in C1606 — **Fixed**

In `validationContract.ts`, test C1606 asserted `errors.length >= 1`. Tightened to `>= 2`.

### 6.3 Design doc role set divergence

The design doc has a different role set than fixture.ts / CONTRACT_TESTS.md. This is **expected** — the design doc describes a broader multi-tenant architecture.

| In fixture/CONTRACT_TESTS.md only | In design doc only |
|-----------------------------------|--------------------|
| analyst | regional-manager |
| viewer | analytics-reader |
| reporting-service | full-service |

Shared roles (7): admin, tenant-user, no-access, orders-service — identical definitions.

### 6.4 Design doc schema divergence

The design doc has additional databases (pg-tenant, iceberg-archive), tables (tenants, metrics, ordersArchive), and `tenantId` columns on all tables. This is **expected** — the contract tests simplified multi-tenancy. No action needed.

---

## Verified Consistent (No Issues)

These areas were checked and found fully consistent across all sources:

- Seed data (fixture.ts ↔ CONTRACT_TESTS.md) — all 9 tables, all rows, all values identical
- Row count assertions — 80+ assertions verified correct against seed data
- Error codes — all 34 codes consistent across design doc, types, throws, and CONTRACT_TESTS.md
- Filter type compatibility rules — every operator × type cell matches
- Masking function behavior — 7 functions match across all sources
- QueryResult / QueryResultMeta types — all fields match
- ColumnMeta, Timing, DebugLogEntry types — all match
- HealthCheckResult / ValidateResult types — all match
- Table schemas (fixture.ts ↔ CONTRACT_TESTS.md) — identical

---

## Execution Order

All items completed:

1. ✅ **Fixed CONTRACT_TESTS.md** (§1) — corrected 10 semantic mismatches, removed C993
2. ✅ **Fixed levenshteinLte validator** — added `text` property check in queryValidator.ts + unit test
3. ✅ **Implemented 10 missing tests** (§2, §3) — C323, C325, C326, C327, C403, C407, C505, C603, C607, C609 in queryContract.ts; C1608 in validationContract.ts
4. ✅ **Unskipped 8 tests** (§4) — C723, C947, C950, C960, C1010-C1013; fixed table pairs for C960 and C1010
5. ✅ **Fixed minor issues** (§6.1, §6.2) — design doc masking typo, C1606 assertion
6. ⏳ **110 unimplemented section tests** (§5) — future work
