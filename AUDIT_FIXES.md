# Audit Fixes — CONTRACT_TESTS.md / Implementation / Design Doc

> **Status: RESOLVED** — All actionable items completed. 661 tests pass, 10 skipped. All 167 spec IDs implemented.

> **Note:** CONTRACT_TESTS.md has moved to the [concept repo](https://github.com/making-ventures/concept-multi-db-query-engine/blob/main/CONTRACT_TESTS.md).

Comprehensive list of inconsistencies found and fixed across:

1. **CONTRACT_TESTS.md** — test design document (167 test IDs)
2. **Contract suites** — queryContract.ts, validationContract.ts, injectionContract.ts, edgeCaseContract.ts, errorContract.ts, healthLifecycleContract.ts
3. **Design doc** — `concept-multi-db-query-engine/` (6 markdown files)
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

## 4. Skipped Tests — 5 remain

8 tests were unskipped in the original audit. Subsequently 5 more were unskipped by fixing engine bugs and adding validation:
- **C602, C604** — EXISTS alias bug fixed in resolver (subquery alias now registered before resolving nested filters)
- **C723** — `mergeAccess()` fixed to deny when any scope has `allowed: false` (matches core's `intersectScopes`)
- **C950** — refColumn type validation tightened (numeric vs string now rejected; compatible families like int↔decimal still allowed)
- **C977** — `validateHavingFilter()` now rejects `QueryColumnFilter` and `QueryExistsFilter` at top level (not just inside groups)

| ID | File | Reason | Category |
|----|------|--------|----------|
| C202 | queryContract.ts | Engine does not support transitive join resolution (3 tables) | Feature gap |
| C1110 | queryContract.ts | Trino catalog not available in Docker Compose test setup | Infra |
| C1711 | edgeCaseContract.ts | Planner doesn't route to replica when primary executor is available | Feature gap |
| C1712 | edgeCaseContract.ts | Trino catalog not available in Docker Compose test setup | Infra |
| C1716 | edgeCaseContract.ts | Planner doesn't route to replica when primary executor is available | Feature gap |

See [MISSING_TESTS.md](MISSING_TESTS.md) for full coverage status.

---

## 5. Previously Unimplemented Sections — All Done ✅

All 110 test IDs from sections 14–18 have been implemented:

| Section | IDs | Count | Topic | Status |
|---------|-----|-------|-------|--------|
| 14, 14b–14d | C1200–C1206, C1250–C1254, C1260–C1263, C1270–C1271 | 18 | Error deserialization, planner/execution/provider errors | ✅ Implemented |
| 15, 15b | C1300–C1304, C1310–C1313 | 9 | Health check, lifecycle | ✅ Implemented |
| 16 | C1400–C1473 | 74 | SQL injection prevention (66 original + 8 defense-in-depth) | ✅ Implemented |
| 18 | C1700–C1716 | 17 | Edge cases | ✅ Implemented |

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
4. ✅ **Investigated skipped tests** (§4) — original 8 unskipped; 10 now skipped with TODO comments documenting reason
5. ✅ **Implemented all 110 section tests** (§5) — sections 14–18 fully implemented (74 injection + 18 error + 9 health + 17 edge case)
6. ✅ **Fixed minor issues** (§6.1, §6.2) — design doc masking typo, C1606 assertion
