# Test Coverage Status

All **415** contract test IDs from [CONTRACT_TESTS.md](https://github.com/making-ventures/concept-multi-db-query-engine/blob/main/CONTRACT_TESTS.md) are implemented.

Current test executions: **677** (includes masking, logger, and injection sql-only unit tests).

---

## Skipped Tests — 4

These tests are implemented but skipped (`it.skip`) due to missing features or infrastructure requirements.

### Feature Gaps (2)

| ID | File | Description |
|----|------|-------------|
| C1711 | edgeCaseContract.ts | Materialized replica query — planner doesn't route to replica |
| C1716 | edgeCaseContract.ts | Freshness hours allows stale replica — planner doesn't route to replica |

### Infrastructure (2)

| ID | File | Description |
|----|------|-------------|
| C1110 | queryContract.ts | meta.targetDatabase for cross-DB query — requires running integration tests |
| C1712 | edgeCaseContract.ts | Cross-DB Trino join — requires running integration tests |

> Trino and Redis are available in `compose/docker-compose.yml`. These tests can run via `pnpm test:integration`.

---

## Outstanding Gaps

### 1. Integration Tests Require Infrastructure

The following test IDs are **implemented** in `src/contract/` but only run when live infrastructure is available (`pnpm test:integration`).  They do **not** yet run in CI.

| Component | Purpose | Test IDs |
|---|---|---|
| **Trino** | Cross-DB query engine | C1110, C1250–C1252, C1712 |
| **Redis** | Cache provider | C1304, C1710 |
| **Debezium** | Materialized replicas | C1253, C1254, C1711, C1715, C1716 |
| **HTTP server** | Error deserialization, lifecycle | C1200–C1206, C1260–C1263, C1270–C1271, C1300–C1303, C1310–C1313 |

### 2. Trino Dialect Parameterization — 113 missing variant runs

Sections 3–9 run each test ID × 2 variants (pg, ch). The spec requires × 3 (adding **trino**). This adds **113** test executions using `chSamples`/`chSampleItems`/`chSampleDetails` tables routed through Trino.

**Requires**: Live Trino instance connected to both pg-main and ch-analytics catalogs.

---

## Suggested Implementation Order

1. **Run integration tests in CI** → Trino and Redis already in compose; enables C1110, C1250–C1252, C1304, C1710, C1712 + 113 trino dialect variant runs
2. **Add Debezium to compose** → C1253, C1254, C1711, C1715, C1716
3. **Enhanced HTTP test harness** → C1200–C1206, C1260–C1263, C1270–C1271
4. **Lifecycle tests** → C1300–C1303, C1310–C1313
