/* Minimal declarations for the Fetch API available in Node.js ≥ 18 */

interface MdFetchRequestInit {
  method?: string
  headers?: Record<string, string>
  body?: string
  signal?: AbortSignal
}

interface MdFetchResponse {
  readonly ok: boolean
  readonly status: number
  json(): Promise<unknown>
}

declare function fetch(input: string | URL, init?: MdFetchRequestInit): Promise<MdFetchResponse>

declare class AbortController {
  readonly signal: AbortSignal
  abort(reason?: unknown): void
}

declare interface AbortSignal {
  readonly aborted: boolean
}
