/**
 * The dashboard talks to its backend on the same origin: nginx proxies /api in
 * the container, and Vite's dev server proxies it in development.  So every call
 * here is a bare path, and nothing needs a compiled-in host.
 */

/** GET a JSON document, raising the backend's own message on failure. */
export async function getJson<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'GET' })
}

/** POST a JSON body and parse the JSON reply. */
export async function postJson<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, {
    method: 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
}

/**
 * POST a JSON body and read a newline-delimited JSON reply, calling `onLine` for
 * each object as it arrives.
 *
 * Used by the five-path comparison, whose slowest path takes minutes: the whole
 * body would arrive at the end, where each line arrives as its path answers.  A
 * failure before the stream opens carries the backend's own message, as a plain
 * POST does; a failure part-way through cannot, because the status line has
 * already been sent, so the last line the backend emits is what says whether the
 * run finished.
 */
export async function postNdjson(
  path: string,
  body: unknown,
  onLine: (line: Record<string, unknown>) => void,
  signal?: AbortSignal,
): Promise<void> {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  })
  if (!response.ok) {
    const payload = await response.json().catch(() => null)
    throw new Error(detailOf(payload) ?? `${response.status} ${response.statusText}`)
  }
  if (!response.body) throw new Error('The browser gave no response body to read')

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let held = ''
  for (;;) {
    const { done, value } = await reader.read()
    // A chunk can split a line anywhere, so anything after the last newline is
    // held over rather than parsed.
    held += decoder.decode(value ?? new Uint8Array(), { stream: !done })
    const lines = held.split('\n')
    held = lines.pop() ?? ''
    for (const line of lines) {
      if (line.trim()) onLine(JSON.parse(line) as Record<string, unknown>)
    }
    if (done) break
  }
  if (held.trim()) onLine(JSON.parse(held) as Record<string, unknown>)
}

async function request<T>(path: string, init: RequestInit): Promise<T> {
  const response = await fetch(path, init)
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    // FastAPI puts the reason in `detail`; fall back to the status when it has
    // not, so the UI never shows a bare "failed".
    throw new Error(detailOf(payload) ?? `${response.status} ${response.statusText}`)
  }
  return payload as T
}

function detailOf(payload: unknown): string | null {
  if (payload && typeof payload === 'object' && 'detail' in payload) {
    const detail = (payload as { detail: unknown }).detail
    if (typeof detail === 'string') return detail
    if (Array.isArray(detail) && detail.length) return JSON.stringify(detail[0])
  }
  return null
}

/**
 * Format a duration in whichever unit reads: the dashboard measures point reads
 * in single milliseconds and comparisons of the whole event history in minutes,
 * and "988200 ms" is not a figure anybody can read at a glance.  The decimal is
 * kept only below ten milliseconds, which is where the transactional path lives
 * and where a tenth still means something.
 */
export function formatMs(ms: number | null | undefined): string {
  if (ms == null) return '—'
  if (ms < 10_000) return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
  const seconds = Math.round(ms / 1000)
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
}

/**
 * A byte count in the unit a person would use.  Decimal rather than binary units,
 * because the figures beside it are throughputs in MB/s and mixing the two makes
 * the arithmetic on screen wrong by 7%.
 */
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null) return '—'
  if (bytes < 1_000) return `${bytes} B`
  if (bytes < 1_000_000) return `${(bytes / 1_000).toFixed(0)} kB`
  if (bytes < 1_000_000_000) return `${(bytes / 1_000_000).toFixed(0)} MB`
  return `${(bytes / 1_000_000_000).toFixed(2)} GB`
}

/** Compact a count: 1234 becomes 1.2k. */
export function formatCount(value: number | null | undefined): string {
  if (value == null) return '—'
  if (Math.abs(value) < 1000) return value.toLocaleString()
  if (Math.abs(value) < 1_000_000) return `${(value / 1000).toFixed(1)}k`
  return `${(value / 1_000_000).toFixed(1)}M`
}
