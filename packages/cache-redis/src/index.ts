import type { CacheProvider } from '@mkven/multi-db-query'
import Redis from 'ioredis'

export interface RedisCacheConfig {
  readonly url?: string | undefined
  readonly host?: string | undefined
  readonly port?: number | undefined
  readonly password?: string | undefined
  readonly db?: number | undefined
  readonly keyPrefix?: string | undefined
}

export function createRedisCache(config: RedisCacheConfig): CacheProvider {
  const redis =
    config.url !== undefined
      ? new Redis(config.url)
      : new Redis({
          host: config.host ?? 'localhost',
          port: config.port ?? 6379,
          password: config.password,
          db: config.db,
          keyPrefix: config.keyPrefix,
        })

  return {
    async getMany(keys: string[]): Promise<Map<string, Record<string, unknown> | null>> {
      const result = new Map<string, Record<string, unknown> | null>()
      if (keys.length === 0) return result

      const values = await redis.mget(...keys)
      for (let i = 0; i < keys.length; i++) {
        const key = keys[i]
        const val = values[i]
        if (key !== undefined) {
          if (val !== null && val !== undefined) {
            try {
              result.set(key, JSON.parse(val) as Record<string, unknown>)
            } catch {
              result.set(key, null)
            }
          } else {
            result.set(key, null)
          }
        }
      }
      return result
    },

    async ping(): Promise<void> {
      await redis.ping()
    },

    async close(): Promise<void> {
      await redis.quit()
    },
  }
}

export type { CacheProvider } from '@mkven/multi-db-query'
