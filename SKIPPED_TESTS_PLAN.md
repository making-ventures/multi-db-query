# Skipped Tests — Implementation Plan

10 skipped tests → **6 fixed** (C202, C602, C604, C723, C950, C977), **4 remaining** (C1110, C1711, C1712, C1716).

## Phase 1 — C723: Multi-scope roles (bug fix, ~3 lines) ✅

**File:** `packages/validation/src/validation/rules.ts` (`mergeAccess()`)

**Bug:** `mergeAccess()` filters out denied scopes with `scopes.filter(s => s.allowed)`, silently ignoring `user: []`. Core's `intersectScopes()` correctly uses `scopes.every(s => s.allowed)`.

**Fix:** If any scope has `allowed: false`, return denied immediately.

## Phase 2 — C977: refColumn in HAVING (feature, ~6 lines) ✅

**File:** `packages/validation/src/validation/queryValidator.ts` (`validateHaving()`)

**Gap:** `QueryColumnFilter` (with `refColumn`) can be passed in HAVING but isn't rejected.

**Fix:** Add `isColumnFilter` check in `validateHaving()` loop — reject with `INVALID_HAVING`.

## Phase 3 — C950: refColumn type mismatch (feature, ~8 lines) ✅

**File:** `packages/validation/src/validation/queryValidator.ts` (`validateColumnFilter()`)

**Gap:** Numeric vs string refColumn comparison allowed when both types are "orderable".

**Fix:** Tighten type compatibility — numeric vs string should fail even if both orderable.

## Phase 4 — C602 + C604: EXISTS alias bug (bug fix, ~15 lines) ✅

**File:** `packages/core/src/resolution/resolver.ts` (`resolveExistsFilter()`)

**Bug:** EXISTS subquery table never registered in `tableAliases`, so nested filter resolution throws "No alias for table".

**Fix:** Temporarily register EXISTS table alias before resolving sub-filters, restore after.

## Phase 5 — C1110 + C1712: Integration infra (config, ~0 code)

**Files:** CI config, test runner

**Status:** Trino already in `compose/docker-compose.yml` with catalog configs. Just need to un-skip and run `pnpm test:integration`.

## Phase 6 — C202: 3-table join (feature, ~35 lines) ✅

**Files:** `packages/validation/src/validation/queryValidator.ts`, `packages/core/src/resolution/resolver.ts`

**Gap:** Join resolution only checks direct `fromTable` relations. Transitive joins (A→B→C) not supported.

**Fix:** Maintain set of "available" tables, check each join against any available table. Both validator and resolver need coordinated changes.

## Phase 7 — C1711 + C1716: Materialized replica routing (feature, ~40 lines)

**Files:** `packages/core/src/planner/strategies.ts`, `packages/core/src/planner/planner.ts`

**Gap:** Planner's `tryDirect()` succeeds for single-DB queries before `tryMaterialized()` is reached. No pathway for freshness-based replica routing.

**Fix:** Add materialized-single check between P1 and P2 when freshness tolerance allows. Requires Debezium in compose for integration testing.

---

## Summary

| Phase | IDs | Type | Effort | Deps | Status |
|-------|-----|------|--------|------|--------|
| 1 | C723 | Bug fix | ~3 LOC | None | ✅ |
| 2 | C977 | Feature | ~6 LOC | None | ✅ |
| 3 | C950 | Feature | ~8 LOC | None | ✅ |
| 4 | C602, C604 | Bug fix | ~15 LOC | None | ✅ |
| 5 | C1110, C1712 | Infra | ~0 LOC | Docker Compose | ⬜ |
| 6 | C202 | Feature | ~35 LOC | None | ✅ |
| 7 | C1711, C1716 | Feature | ~40 LOC | Debezium infra | ⬜ |
