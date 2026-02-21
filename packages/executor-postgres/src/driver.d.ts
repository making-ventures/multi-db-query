declare module 'pg' {
  export interface PoolConfig {
    connectionString?: string | undefined
    host?: string | undefined
    port?: number | undefined
    database?: string | undefined
    user?: string | undefined
    password?: string | undefined
    ssl?: boolean | Record<string, unknown> | undefined
    max?: number | undefined
    statement_timeout?: number | undefined
    connectionTimeoutMillis?: number | undefined
    idleTimeoutMillis?: number | undefined
  }

  export interface QueryResult {
    rows: Record<string, unknown>[]
    rowCount: number | null
  }

  export class Pool {
    constructor(config?: PoolConfig)
    query(text: string, values?: unknown[]): Promise<QueryResult>
    end(): Promise<void>
  }

  export namespace types {
    function setTypeParser(oid: number, parser: (value: string) => unknown): void
  }
}
