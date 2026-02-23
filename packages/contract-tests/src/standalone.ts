import { createClickHouseExecutor } from '@mkven/multi-db-executor-clickhouse'
import { createPostgresExecutor } from '@mkven/multi-db-executor-postgres'
import { createTrinoExecutor } from '@mkven/multi-db-executor-trino'
import { staticMetadata, staticRoles } from '@mkven/multi-db-query'
import { metadata, roles } from '../tests/contract/fixture.js'
import { createServer } from './server.js'

const server = await createServer({
  port: Number(process.env.PORT ?? 3000),
  multiDbOptions: {
    metadataProvider: staticMetadata(metadata),
    roleProvider: staticRoles(roles),
    executors: {
      'pg-main': createPostgresExecutor({
        connectionString: process.env.PG_URL ?? 'postgresql://postgres:postgres@localhost:5432/multidb',
      }),
      'ch-analytics': createClickHouseExecutor({
        url: process.env.CH_URL ?? 'http://localhost:8123',
        username: 'default',
        password: 'clickhouse',
        database: 'multidb',
      }),
      trino: createTrinoExecutor({
        server: process.env.TRINO_URL ?? 'http://localhost:8080',
        user: 'trino',
      }),
    },
  },
})

await server.start()
console.log(`multi-db server listening on ${server.url}`)
