import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import { formatCount, formatMs, getJson } from '../lib/api'

interface ServiceHealth {
  name: string
  status: string
  endpoint: string
}

interface PlatformHealth {
  services: ServiceHealth[]
  overall_health_score: number
  total_drones: number
}

interface Latency {
  cassandra_point_read_ms: number | null
  presto_scan_ms: number | null
  vector_search_ms: number | null
}

const SERVICE_ICONS: Record<string, string> = {
  Cassandra: 'database',
  Kafka: 'stream',
  Presto: 'analytics',
  Spark: 'bolt',
}

/** What each service does here, so the page explains the stack as it checks it. */
const SERVICE_ROLES: Record<string, string> = {
  Cassandra: 'Transactional store — every write lands here, and the live map reads it',
  Kafka: 'Event transport between the producer and the ingest sink',
  Presto: 'Analytical SQL over the same Cassandra rows, with no copy in between',
  Spark: 'Batch analytics and bulk reads, over the same rows again',
}

/**
 * The three queries the backend times, and the figure each one is worth paying
 * attention to.  The thresholds are what this demo on a laptop should manage,
 * not a production SLA.
 */
const LATENCY_TIERS = [
  {
    key: 'cassandra_point_read_ms' as const,
    label: 'Cassandra point read',
    detail: 'One partition of drone_latest_status',
    colour: 'var(--color-primary)',
    expectedUnder: 20,
  },
  {
    key: 'presto_scan_ms' as const,
    label: 'Presto aggregate',
    detail: 'count(*) over the same table',
    colour: 'var(--color-secondary)',
    expectedUnder: 1000,
  },
  {
    key: 'vector_search_ms' as const,
    label: 'Vector ANN search',
    detail: 'SAI index on payload_vector',
    colour: 'var(--color-accent)',
    expectedUnder: 100,
  },
]

export default function HealthPage() {
  const { data, isLoading } = useQuery<PlatformHealth>({
    queryKey: ['platform-health'],
    queryFn: () => getJson<PlatformHealth>('/api/platform/health'),
    refetchInterval: 10000,
  })

  const { data: latency } = useQuery<Latency>({
    queryKey: ['latency'],
    queryFn: () => getJson<Latency>('/api/demo/latency'),
    refetchInterval: 10000,
  })

  const services = data?.services ?? []
  const up = services.filter((s) => s.status === 'up').length
  const healthPercent = Math.round((data?.overall_health_score ?? 0) * 100)

  return (
    <section className="space-y-8">
      <div>
        <div className="mb-2 flex items-center gap-3">
          <span
            className={`h-2 w-2 rounded-full ${healthPercent === 100 ? 'bg-positive animate-pulse' : 'bg-secondary'}`}
          />
          <span className="font-label text-primary-dim text-[0.6875rem] font-bold uppercase tracking-widest">
            Infrastructure
          </span>
        </div>
        <h1 className="font-headline text-4xl font-black uppercase tracking-tighter">
          Platform health
        </h1>
        <p className="text-on-surface-variant mt-2 text-xs">
          Each service is probed by opening a TCP connection to the address this backend uses to
          reach it. That is reachability, not liveness: a service can accept connections while still
          starting up.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {[
          { label: 'Services reachable', value: `${up} / ${services.length}`, colour: 'var(--color-primary)' },
          { label: 'Health score', value: `${healthPercent}%`, colour: healthPercent === 100 ? 'var(--color-positive)' : 'var(--color-secondary)' },
          { label: 'Assets tracked', value: formatCount(data?.total_drones), colour: 'var(--color-secondary)' },
          { label: 'Point read', value: formatMs(latency?.cassandra_point_read_ms), colour: 'var(--color-primary)' },
        ].map((tile) => (
          <div key={tile.label} className="rounded-lg bg-surface-container p-5">
            <p className="text-on-surface-variant text-[0.6875rem] font-medium uppercase tracking-wider">
              {tile.label}
            </p>
            <p
              className="font-headline mt-2 text-2xl font-black tabular-nums"
              style={{ color: tile.colour }}
            >
              {tile.value}
            </p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
        {isLoading ? (
          <p className="text-on-surface-variant col-span-full py-12 text-center">Probing…</p>
        ) : (
          services.map((service) => {
            const isUp = service.status === 'up'
            return (
              <div
                key={service.name}
                className="rounded-lg border border-white/5 bg-surface-container p-6 transition-colors hover:bg-surface-container-high"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-center gap-3">
                    <div
                      className={`flex h-10 w-10 items-center justify-center rounded-lg ${isUp ? 'bg-positive/10' : 'bg-tertiary/10'}`}
                    >
                      <MaterialIcon
                        name={SERVICE_ICONS[service.name] ?? 'cloud'}
                        className={isUp ? 'text-positive' : 'text-tertiary'}
                      />
                    </div>
                    <div>
                      <h2 className="font-headline text-sm font-bold uppercase tracking-tight">
                        {service.name}
                      </h2>
                      <p className="text-on-surface-variant font-mono text-[10px]">
                        {service.endpoint}
                      </p>
                    </div>
                  </div>
                  <span
                    className={`rounded px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider ${
                      isUp ? 'text-positive bg-positive/10' : 'text-tertiary bg-tertiary/10'
                    }`}
                  >
                    {service.status}
                  </span>
                </div>
                <p className="text-on-surface-variant mt-4 text-xs leading-relaxed">
                  {SERVICE_ROLES[service.name] ?? 'Part of the stack'}
                </p>
              </div>
            )
          })
        )}
      </div>

      <div className="glass-panel rounded-xl p-8">
        <h2 className="font-headline text-lg font-bold uppercase tracking-wide">
          Latency by access path
        </h2>
        <p className="text-on-surface-variant mt-1 text-xs">
          One representative query per path, timed end to end from the backend. A dash means the path
          cannot answer yet — the vector search needs its embeddings built first, from the Explore
          page. The Spark paths are not probed here: each one starts a job, and the bulk reader takes
          a Cassandra snapshot, so they are measured on demand from Explore rather than every ten
          seconds.
        </p>
        <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-3">
          {LATENCY_TIERS.map((tier) => {
            const value = latency?.[tier.key]
            const withinExpectation = value != null && value < tier.expectedUnder
            return (
              <div key={tier.key} className="rounded-lg border border-white/5 bg-surface-container-low p-5">
                <p
                  className="text-[10px] font-black uppercase tracking-wider"
                  style={{ color: tier.colour }}
                >
                  {tier.label}
                </p>
                <p className="text-on-surface-variant mb-3 text-[9px]">{tier.detail}</p>
                <p
                  className="font-headline text-3xl font-black tabular-nums"
                  style={{ color: value != null ? tier.colour : 'var(--color-outline-variant)' }}
                >
                  {formatMs(value)}
                </p>
                {value != null && (
                  <p
                    className={`mt-1 text-[10px] font-bold ${withinExpectation ? 'text-positive' : 'text-secondary'}`}
                  >
                    {withinExpectation
                      ? `under ${tier.expectedUnder} ms, as expected`
                      : `over ${tier.expectedUnder} ms for this demo`}
                  </p>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </section>
  )
}
