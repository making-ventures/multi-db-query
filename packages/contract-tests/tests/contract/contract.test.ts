import { createRedisCache } from '@mkven/multi-db-cache-redis'
import type { ValidateResult } from '@mkven/multi-db-client'
import { createMultiDbClient } from '@mkven/multi-db-client'
import {
  describeEdgeCaseContract,
  describeErrorContract,
  describeExecutorContract,
  describeHealthLifecycleContract,
  describeInjectionContract,
  describeQueryContract,
  describeValidationContract,
} from '@mkven/multi-db-contract'
import { createClickHouseExecutor } from '@mkven/multi-db-executor-clickhouse'
import { createPostgresExecutor } from '@mkven/multi-db-executor-postgres'
import { createTrinoExecutor } from '@mkven/multi-db-executor-trino'
import type { CreateMultiDbOptions } from '@mkven/multi-db-query'
import {
  createMultiDb,
  MetadataIndex,
  staticMetadata,
  staticRoles,
  validateConfig,
  validateQuery,
} from '@mkven/multi-db-query'
import { afterAll, beforeAll } from 'vitest'
import { createServer } from '../../src/server.js'
import { metadata, roles } from './fixture.js'

const PG_URL = process.env.PG_URL ?? 'postgresql://postgres:postgres@localhost:5432/multidb'
const CH_URL = process.env.CH_URL ?? 'http://localhost:8123'
const TRINO_URL = process.env.TRINO_URL ?? 'http://localhost:8080'
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'

// ── Shared options (built lazily in beforeAll) ─────────────────

let multiDbOptions: CreateMultiDbOptions
let serverUrl = ''
let stopServer: (() => Promise<void>) | undefined

beforeAll(async () => {
  multiDbOptions = {
    metadataProvider: staticMetadata(metadata),
    roleProvider: staticRoles(roles),
    executors: {
      'pg-main': createPostgresExecutor({ connectionString: PG_URL }),
      'ch-analytics': createClickHouseExecutor({
        url: CH_URL,
        username: 'default',
        password: 'clickhouse',
        database: 'multidb',
      }),
      trino: createTrinoExecutor({ server: TRINO_URL, user: 'trino' }),
    },
    cacheProviders: {
      'redis-main': createRedisCache({ url: REDIS_URL }),
    },
  }

  const server = await createServer({ port: 0, multiDbOptions })
  await server.start()
  serverUrl = server.url
  stopServer = server.stop
})

afterAll(async () => {
  await stopServer?.()
})

// ── Real DBs, in-process engine ────────────────────────────────

describeQueryContract('real-dbs (in-process)', async () => {
  return createMultiDb(multiDbOptions)
})

// ── HTTP client → in-process server → real DBs ────────────────

describeQueryContract('http-client (in-process server)', async () => {
  return createMultiDbClient({ baseUrl: serverUrl })
})

// ── Edge cases (in-process) ────────────────────────────────────

describeEdgeCaseContract('real-dbs (in-process)', async () => {
  return createMultiDb(multiDbOptions)
})

// ── Factory for fresh options (each call creates new connections) ───

function freshOptions(): CreateMultiDbOptions {
  return {
    metadataProvider: staticMetadata(metadata),
    roleProvider: staticRoles(roles),
    executors: {
      'pg-main': createPostgresExecutor({ connectionString: PG_URL }),
      'ch-analytics': createClickHouseExecutor({
        url: CH_URL,
        username: 'default',
        password: 'clickhouse',
        database: 'multidb',
      }),
      trino: createTrinoExecutor({ server: TRINO_URL, user: 'trino' }),
    },
    cacheProviders: {
      'redis-main': createRedisCache({ url: REDIS_URL }),
    },
  }
}

// ── Health check & lifecycle (in-process) ──────────────────────

describeHealthLifecycleContract('real-dbs (in-process)', freshOptions)

// ── Error contract (HTTP client) ───────────────────────────────

describeErrorContract('http-client', () => serverUrl, freshOptions)

// ── SQL injection (in-process) ─────────────────────────────────

describeInjectionContract('real-dbs (in-process)', async () => {
  return createMultiDb(multiDbOptions)
})

// ── Validation contract (in-process, zero I/O) ────────────────

describeValidationContract(
  'in-process (direct)',
  async () => {
    const index = new MetadataIndex(metadata, roles)
    return {
      async validateQuery(input) {
        const err = validateQuery(input.definition, input.context, index, roles)
        if (err !== null) throw err
        return { valid: true } satisfies ValidateResult
      },
      async validateConfig(input) {
        const err = validateConfig(input.metadata)
        if (err !== null) throw err
        return { valid: true } satisfies ValidateResult
      },
    }
  },
  metadata,
  roles,
)

// ── Executor contracts (direct DbExecutor interface) ──────────

describeExecutorContract('postgres', () => createPostgresExecutor({ connectionString: PG_URL }), {
  validQuery: 'SELECT 1 AS n',
  invalidQuery: 'SELECT * FROM __nonexistent_table_xyz__',
})

describeExecutorContract(
  'clickhouse',
  () =>
    createClickHouseExecutor({
      url: CH_URL,
      username: 'default',
      password: 'clickhouse',
      database: 'multidb',
    }),
  { validQuery: 'SELECT 1 AS n', invalidQuery: 'SELECT * FROM __nonexistent_table_xyz__' },
)

describeExecutorContract('trino', () => createTrinoExecutor({ server: TRINO_URL, user: 'trino' }), {
  validQuery: 'SELECT 1 AS n',
  invalidQuery: 'SELECT * FROM __nonexistent_catalog__.__bad_schema__.__bad_table__',
})
