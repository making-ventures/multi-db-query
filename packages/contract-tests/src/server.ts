import { createServer as createHttpServer, type IncomingMessage, type ServerResponse } from 'node:http'
import type { CreateMultiDbOptions, MultiDb } from '@mkven/multi-db-query'
import { createMultiDb, MetadataIndex, validateConfig, validateQuery } from '@mkven/multi-db-query'
import {
  ConfigError,
  ConnectionError,
  ExecutionError,
  MultiDbError,
  PlannerError,
  ProviderError,
  ValidationError,
} from '@mkven/multi-db-validation'

// ── Types ──────────────────────────────────────────────────────

export interface ServerConfig {
  readonly port?: number | undefined
  readonly host?: string | undefined
  readonly multiDbOptions: CreateMultiDbOptions
}

class HttpError extends Error {
  readonly status: number
  readonly code: string
  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'HttpError'
    this.status = status
    this.code = code
  }
}

// ── Error mapping ──────────────────────────────────────────────

function errorToStatus(err: unknown): number {
  if (err instanceof HttpError) return err.status
  if (err instanceof ValidationError || err instanceof ConfigError) return 400
  if (err instanceof PlannerError) return 422
  if (err instanceof ExecutionError) return 500
  if (err instanceof ConnectionError || err instanceof ProviderError) return 503
  return 500
}

function errorToBody(err: unknown): object {
  if (err instanceof HttpError) return { error: err.code, message: err.message }
  if (err instanceof MultiDbError) return err.toJSON()
  const msg = err instanceof Error ? err.message : String(err)
  return { error: 'InternalError', message: msg }
}

// ── Helpers ────────────────────────────────────────────────────

function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(chunk))
    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf-8')
        resolve(raw.length > 0 ? JSON.parse(raw) : undefined)
      } catch {
        reject(new HttpError(400, 'INVALID_JSON', 'Request body is not valid JSON'))
      }
    })
    req.on('error', reject)
  })
}

function respond(res: ServerResponse, status: number, body: unknown): void {
  const json = JSON.stringify(body)
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(json) })
  res.end(json)
}

// ── Server factory ─────────────────────────────────────────────

export async function createServer(config: ServerConfig): Promise<{
  start(): Promise<void>
  stop(): Promise<void>
  url: string
}> {
  const port = config.port ?? 3000
  const host = config.host ?? '0.0.0.0'

  const multiDb: MultiDb = await createMultiDb(config.multiDbOptions)

  // Build index once for validate endpoints
  const meta = await config.multiDbOptions.metadataProvider.load()
  const rolesData = await config.multiDbOptions.roleProvider.load()
  const metadataIndex = new MetadataIndex(meta, rolesData)

  async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const method = req.method ?? 'GET'
    const url = (req.url ?? '/').split('?')[0]

    try {
      if (method === 'GET' && url === '/health') {
        const result = await multiDb.healthCheck()
        respond(res, 200, result)
      } else if (method === 'POST' && url === '/query') {
        const body = (await readBody(req)) as { definition: unknown; context: unknown }
        if (!body || typeof body !== 'object') {
          throw new HttpError(400, 'INVALID_BODY', 'Request body must be an object with definition and context')
        }
        const result = await multiDb.query(body as Parameters<MultiDb['query']>[0])
        respond(res, 200, result)
      } else if (method === 'POST' && url === '/validate/query') {
        const body = (await readBody(req)) as { definition: unknown; context: unknown }
        if (!body || typeof body !== 'object') {
          throw new HttpError(400, 'INVALID_BODY', 'Request body must be an object')
        }
        const err = validateQuery(
          (body as { definition: unknown }).definition as Parameters<typeof validateQuery>[0],
          (body as { context: unknown }).context as Parameters<typeof validateQuery>[1],
          metadataIndex,
          rolesData,
        )
        if (err !== null) throw err
        respond(res, 200, { valid: true })
      } else if (method === 'POST' && url === '/validate/config') {
        const body = (await readBody(req)) as { metadata: unknown }
        if (!body || typeof body !== 'object') {
          throw new HttpError(400, 'INVALID_BODY', 'Request body must be an object')
        }
        const err = validateConfig((body as { metadata: unknown }).metadata as Parameters<typeof validateConfig>[0])
        if (err !== null) throw err
        respond(res, 200, { valid: true })
      } else {
        respond(res, 404, { error: 'NotFound', message: `${method} ${url} not found` })
      }
    } catch (err) {
      const status = errorToStatus(err)
      respond(res, status, errorToBody(err))
    }
  }

  const server = createHttpServer((req: IncomingMessage, res: ServerResponse) => {
    handleRequest(req, res).catch((err: unknown) => {
      if (!res.headersSent) {
        respond(res, 500, { error: 'InternalError', message: err instanceof Error ? err.message : String(err) })
      }
    })
  })

  const displayHost = host === '0.0.0.0' ? 'localhost' : host

  const result = {
    url: `http://${displayHost}:${port}`,
    start() {
      return new Promise<void>((resolve, reject) => {
        server.on('error', reject)
        server.listen(port, host, () => {
          const addr = server.address()
          if (addr && typeof addr === 'object') {
            result.url = `http://${displayHost}:${addr.port}`
          }
          resolve()
        })
      })
    },
    async stop() {
      await multiDb.close().catch(() => {})
      return new Promise<void>((resolve, reject) => {
        server.close((err: Error | undefined) => (err ? reject(err) : resolve()))
      })
    },
  }

  return result
}
