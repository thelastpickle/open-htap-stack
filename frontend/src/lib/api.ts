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

/** Format a millisecond duration the way every panel shows it. */
export function formatMs(ms: number | null | undefined): string {
  return ms == null ? '—' : `${ms.toFixed(ms < 10 ? 1 : 0)} ms`
}

/** Compact a count: 1234 becomes 1.2k. */
export function formatCount(value: number | null | undefined): string {
  if (value == null) return '—'
  if (Math.abs(value) < 1000) return value.toLocaleString()
  if (Math.abs(value) < 1_000_000) return `${(value / 1000).toFixed(1)}k`
  return `${(value / 1_000_000).toFixed(1)}M`
}
