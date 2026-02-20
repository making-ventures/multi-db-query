import type {
  ExecutionContext,
  HealthCheckResult,
  MetadataConfig,
  QueryDefinition,
  QueryResult,
  RoleMeta,
} from '@mkven/multi-db-validation'
import { ConnectionError, MetadataIndex, MultiDbError, validateQuery } from '@mkven/multi-db-validation'

import { deserializeError } from './errors.js'

// ── Types ──────────────────────────────────────────────────────

export interface MultiDbClientConfig {
  readonly baseUrl: string
  readonly headers?: Record<string, string> | undefined
  readonly fetch?: typeof globalThis.fetch | undefined
  readonly timeout?: number | undefined
  readonly validateBeforeSend?: boolean | undefined
  readonly metadata?: MetadataConfig | undefined
  readonly roles?: readonly RoleMeta[] | undefined
}

export interface MultiDbClient {
  query(input: { definition: QueryDefinition; context: ExecutionContext }): Promise<QueryResult>
  healthCheck(): Promise<HealthCheckResult>
}

// ── Factory ────────────────────────────────────────────────────

export function createMultiDbClient(config: MultiDbClientConfig): MultiDbClient {
  const { baseUrl, timeout = 30_000, validateBeforeSend } = config
  const customHeaders = config.headers ?? {}
  const fetchFn = config.fetch ?? globalThis.fetch

  // Pre-build a local index for optional client-side validation
  let localIndex: MetadataIndex | undefined
  let localRoles: readonly RoleMeta[] | undefined
  if (validateBeforeSend && config.metadata !== undefined && config.roles !== undefined) {
    localIndex = new MetadataIndex(config.metadata, config.roles)
    localRoles = config.roles
  }

  return {
    async query(input) {
      // Optional local validation — fail fast before network round-trip
      if (validateBeforeSend && localIndex !== undefined && localRoles !== undefined) {
        const err = validateQuery(input.definition, input.context, localIndex, localRoles)
        if (err !== null) throw err
      }

      const controller = new AbortController()
      const timer = timeout > 0 ? setTimeout(() => controller.abort(), timeout) : undefined

      try {
        const res = await fetchFn(`${baseUrl}/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...customHeaders },
          body: JSON.stringify(input),
          signal: controller.signal,
        })

        const body = (await res.json()) as Record<string, unknown>

        if (!res.ok) {
          throw deserializeError(body)
        }

        return body as unknown as QueryResult
      } catch (err) {
        if (err instanceof MultiDbError) throw err
        if (err instanceof Error && err.name === 'AbortError') {
          throw new ConnectionError('REQUEST_TIMEOUT', `Request timed out after ${timeout}ms`, {
            timeoutMs: timeout,
          })
        }
        throw new ConnectionError('NETWORK_ERROR', err instanceof Error ? err.message : String(err), {})
      } finally {
        if (timer !== undefined) clearTimeout(timer)
      }
    },

    async healthCheck() {
      const res = await fetchFn(`${baseUrl}/health`, {
        method: 'GET',
        headers: customHeaders,
      })
      return (await res.json()) as HealthCheckResult
    },
  }
}
