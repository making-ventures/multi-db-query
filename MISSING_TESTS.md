# Missing Contract Tests

44 test IDs from [CONTRACT_TESTS.md](https://github.com/making-ventures/concept-multi-db-query-engine/blob/main/CONTRACT_TESTS.md) are not yet implemented.

Currently implemented: **356 / 400** test IDs (609 test executions including injection sql-only tests).

Additionally, sections 3–9 run only **2 dialect variants** (pg, ch) instead of the 3 required by the spec (pg, ch, **trino**). This means **113 parameterized test IDs** are missing their Trino variant.

---

## Infrastructure Required

| Component | Purpose | Tests blocked |
|---|---|---|
| **Trino** | Cross-DB query engine (joins pg-main + ch-analytics) | C1110, C1250-C1254, C1712 + all section 3–9 trino variants |
| **Redis** | Cache provider for `users` table `byIds` | C1304, C1710 |
| **Debezium sync** | Materialized replicas (`orders_replica` on ch-analytics) | C1711, C1715, C1716, C1253, C1254 |
| **HTTP server** (enhanced) | Error deserialization, provider errors, lifecycle | C1200-C1206, C1260-C1263, C1270-C1271, C1310-C1313 |

---

## §14. Error Deserialization (HTTP-specific) — 7 tests

| ID | Test | Assertions |
|---|---|---|
| C1200 | ValidationError via HTTP | client throws `ValidationError`; `instanceof ValidationError === true`; `code === 'VALIDATION_FAILED'`; `errors[]` present |
| C1201 | ValidationError preserves fromTable | `error.fromTable` matches query's `from` table |
| C1202 | PlannerError via HTTP | client throws `PlannerError`; `code` correct (e.g. `UNREACHABLE_TABLES`) |
| C1203 | ExecutionError via HTTP | client throws `ExecutionError`; `code` correct (e.g. `QUERY_FAILED`) |
| C1204 | ConnectionError on network failure | `ConnectionError` with `code: 'NETWORK_ERROR'` |
| C1205 | ConnectionError on timeout | `ConnectionError` with `code: 'REQUEST_TIMEOUT'` |
| C1206 | ProviderError via HTTP | client throws `ProviderError`; `code: 'METADATA_LOAD_FAILED'` or `'ROLE_LOAD_FAILED'` |

**Requires**: HTTP client tests with controlled error injection (server returning specific HTTP error codes).

---

## §14b. Planner Errors — 5 tests

| ID | Test | Assertions |
|---|---|---|
| C1250 | Cross-DB join with Trino disabled | `PlannerError` with `code: 'TRINO_DISABLED'`, HTTP 422 |
| C1251 | Cross-DB join, DB missing trinoCatalog | `PlannerError` with `code: 'NO_CATALOG'`, HTTP 422 |
| C1252 | Cross-DB tables, no sync, no trino | `PlannerError` with `code: 'UNREACHABLE_TABLES'`, HTTP 422 |
| C1253 | Freshness conflict with replica lag | `PlannerError` with `code: 'FRESHNESS_UNMET'`, HTTP 422 |
| C1254 | Freshness `'seconds'` accepts `'seconds'` lag | `meta.strategy === 'materialized'` |

**Requires**: Trino config (enabled/disabled), Debezium materialized replica setup.

---

## §14c. Execution Errors — 4 tests

| ID | Test | Assertions |
|---|---|---|
| C1260 | Missing executor | `ExecutionError` with `code: 'EXECUTOR_MISSING'`, HTTP 500 |
| C1261 | Missing cache provider | `ExecutionError` with `code: 'CACHE_PROVIDER_MISSING'`, HTTP 500 |
| C1262 | Query execution failure | `ExecutionError` with `code: 'QUERY_FAILED'`, HTTP 500 |
| C1263 | Query timeout | `ExecutionError` with `code: 'QUERY_TIMEOUT'`, HTTP 500 |

**Requires**: Ability to dynamically remove executors/cache providers and trigger DB errors at test time.

---

## §14d. Provider Errors — 2 tests

| ID | Test | Assertions |
|---|---|---|
| C1270 | Metadata provider failure | `ProviderError` with `code: 'METADATA_LOAD_FAILED'`, HTTP 503 |
| C1271 | Role provider failure | `ProviderError` with `code: 'ROLE_LOAD_FAILED'`, HTTP 503 |

**Requires**: Pluggable metadata/role providers that can be made to fail.

---

## §15. Health Check — 5 tests

| ID | Test | Assertions |
|---|---|---|
| C1300 | Healthy server | `GET /health` → `{ healthy: true, executors: {...}, cacheProviders: {...} }` |
| C1301 | Executor keys present | `executors` contains keys matching configured database IDs |
| C1302 | Each executor has required fields | `healthy: boolean`, `latencyMs: number` |
| C1303 | Unhealthy executor | stop one DB → `healthy: false`; failed executor has `healthy: false` and `error` |
| C1304 | Cache provider in health check | `cacheProviders` contains `redis-main` with `healthy: boolean`, `latencyMs: number` |

**Requires**: Health endpoint, Redis cache provider (C1304), ability to stop a DB (C1303).

---

## §15b. Lifecycle — 4 tests

| ID | Test | Assertions |
|---|---|---|
| C1310 | Reload metadata makes new table visible | add table, call `reloadMetadata()` → query succeeds |
| C1311 | Reload metadata failure preserves old config | broken provider → old queries still work |
| C1312 | Reload roles updates permissions | add role, call `reloadRoles()` → query with new role succeeds |
| C1313 | Close prevents further queries | `close()` then query → `ExecutionError` `EXECUTOR_MISSING` |

**Requires**: Dynamic metadata/role providers, lifecycle methods (`reloadMetadata`, `reloadRoles`, `close`).

---

## §18. Edge Cases — 17 tests

| ID | Test | Assertions |
|---|---|---|
| C1700 | Empty result set | `data` is empty array; `meta.columns` present |
| C1701 | Single row result | `data.length === 1` |
| C1702 | Large in-list | 50+ values; executes without error |
| C1703 | Nullable column in result | `discount` can be null |
| C1704 | Boolean column values | proper boolean (not 0/1) |
| C1705 | Timestamp format | ISO 8601 string |
| C1706 | Date format | ISO date (YYYY-MM-DD) |
| C1707 | Array column in result | `labels` is JSON array or null |
| C1708 | Decimal precision | preserved decimal precision |
| C1709 | Multiple filters (implicit AND) | both conditions applied |
| C1710 | Cache strategy reported | `meta.strategy === 'cache'` |
| C1711 | Materialized replica query | `meta.strategy === 'materialized'` |
| C1712 | Cross-DB Trino join | `meta.strategy === 'trino-cross-db'` |
| C1713 | DISTINCT + count mode | count = number of distinct statuses (4) |
| C1714 | GROUP BY with zero matching rows | empty array; meta.columns present |
| C1715 | Freshness `'realtime'` skips materialized | `meta.strategy !== 'materialized'` |
| C1716 | Freshness `'hours'` allows stale replica | `meta.strategy === 'materialized'` |

**Requires**: Redis (C1710), Debezium materialized replica (C1711, C1715, C1716), Trino (C1712). Tests C1700-C1709, C1713, C1714 need only PG/CH.

---

## Trino Dialect Parameterization — 113 missing variant runs

Sections 3–9 currently run each test ID × 2 variants (pg, ch). The spec requires × 3 (adding **trino** variant). This adds 113 additional test executions using `chSamples`/`chSampleItems`/`chSampleDetails` tables routed through Trino via transparent cross-DB join.

**Requires**: Live Trino instance connected to both pg-main and ch-analytics catalogs.

---

## Summary by Infrastructure Dependency

| Dependency | Test IDs | Count |
|---|---|---|
| **No new infra** (existing PG+CH) | C1700-C1709, C1713, C1714 | 12 |
| **Redis** | C1304, C1710 | 2 |
| **Debezium / materialized** | C1253, C1254, C1711, C1715, C1716 | 5 |
| **Trino** | C1250-C1252, C1712 | 4 |
| **Enhanced HTTP / lifecycle** | C1200-C1206, C1260-C1263, C1270-C1271, C1300-C1303, C1310-C1313 | 21 |
| **Trino dialect parameterization** | 113 test IDs × trino variant | 113 runs |
| **Total missing** | | **44 IDs + 113 trino variant runs** |

### Suggested implementation order

1. **§18. Edge Cases** C1700-C1709, C1713, C1714 (12 tests) — existing PG+CH
2. **§15. Health Check** C1300-C1303 (4 tests) — existing infra
3. **§14. Error Deserialization** (7 tests) — enhanced HTTP test harness
4. **§14c. Execution Errors** (4 tests) — error injection
5. **§15b. Lifecycle** (4 tests) — lifecycle methods
6. **§14d. Provider Errors** (2 tests) — pluggable providers
7. **Add Redis to compose** → C1304, C1710 (2 tests)
8. **Add Debezium to compose** → C1253, C1254, C1711, C1715, C1716 (5 tests)
9. **Add Trino to compose** → C1250-C1252, C1712, trino dialect variant (4 + 113 tests)
