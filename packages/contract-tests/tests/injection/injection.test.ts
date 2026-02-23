/**
 * Unit-mode wrapper for the canonical injection contract.
 *
 * Instead of duplicating the 74 injection tests, we reuse
 * `describeInjectionContract` from the contract package with a thin
 * `QueryContract` proxy that injects `executeMode: 'sql-only'` into
 * every query — no live databases required.
 */

import type { QueryContract } from '@mkven/multi-db-contract'
import { describeInjectionContract } from '@mkven/multi-db-contract'
import type { CacheProvider, CreateMultiDbOptions, DbExecutor } from '@mkven/multi-db-query'
import { createMultiDb, staticMetadata, staticRoles } from '@mkven/multi-db-query'
import { metadata, roles } from '../contract/fixture.js'

// ── Mock adapters ──────────────────────────────────────────────

function mockExecutor(): DbExecutor {
  return {
    execute: async () => [],
    ping: async () => {},
    close: async () => {},
  }
}

function mockCache(): CacheProvider {
  return {
    getMany: async (keys) => {
      const r = new Map<string, Record<string, unknown> | null>()
      for (const k of keys) r.set(k, null)
      return r
    },
    ping: async () => {},
    close: async () => {},
  }
}

// ── sql-only proxy ─────────────────────────────────────────────

/** Wraps MultiDb and injects `executeMode: 'sql-only'` into every query. */
function sqlOnlyProxy(db: { query: QueryContract['query'] }): QueryContract {
  return {
    async query(input) {
      return db.query({
        ...input,
        definition: { ...input.definition, executeMode: 'sql-only' },
      })
    },
  }
}

// ── Run the canonical 74 injection tests ───────────────────────

describeInjectionContract('unit (sql-only)', async () => {
  const options: CreateMultiDbOptions = {
    metadataProvider: staticMetadata(metadata),
    roleProvider: staticRoles(roles),
    executors: {
      'pg-main': mockExecutor(),
      'ch-analytics': mockExecutor(),
      trino: mockExecutor(),
    },
    cacheProviders: {
      'redis-main': mockCache(),
    },
  }
  const db = await createMultiDb(options)
  return sqlOnlyProxy(db)
})
