import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import MaterialIcon from '../components/MaterialIcon'
import Toast, { useToast } from '../components/Toast'
import { formatCount, getJson, postJson } from '../lib/api'

interface Alert {
  alert_id: string
  alert_time: string
  entity_id: string
  alert_type: string
  severity: string
  message: string
  risk_score: number
}

interface KPIs {
  active_flying_drones: number
  grounded_drones: number
  total_drones: number
  near_zone_count: number
  predicted_breach_count: number
  total_events: number
  ingestion_rate_per_sec: number
  platform_health_score: number
  avg_speed_mps: number
  max_speed_mps: number
  avg_altitude_m: number
  max_altitude_m: number
  latest_alerts: Alert[]
}

interface IngestionHistory {
  hours: number
  buckets: { time: string; timestamp: string; count: number }[]
}

const HISTORY_WINDOWS = [8, 24] as const

// The ingestion bar is drawn against the highest rate the Settings page will let
// you ask for, so the bar means "how much of the demo's range is in use".
const RATE_SCALE_MAX = 5000

function KPICard({
  icon,
  label,
  value,
  unit,
  note,
  progress,
  colour = 'var(--color-primary)',
}: {
  icon: string
  label: string
  value: string
  unit?: string
  note?: string
  progress?: number
  colour?: string
}) {
  return (
    <div className="rounded-lg bg-surface-container p-6 transition-colors hover:bg-surface-container-high">
      <div className="mb-4 flex items-start justify-between">
        <MaterialIcon name={icon} style={{ color: colour }} className="text-primary-dim" />
        {note && (
          <span
            className="rounded px-2 py-0.5 text-[10px] font-bold"
            style={{ color: colour, background: `color-mix(in srgb, ${colour} 12%, transparent)` }}
          >
            {note}
          </span>
        )}
      </div>
      <p className="text-on-surface-variant text-[0.6875rem] font-medium uppercase tracking-wider">
        {label}
      </p>
      <div className="mt-2 flex items-baseline gap-2">
        <span className="font-headline text-3xl font-bold tabular-nums">{value}</span>
        {unit && <span className="text-on-surface-variant text-xs font-light">{unit}</span>}
      </div>
      {progress !== undefined && (
        <div className="mt-4 h-1 w-full overflow-hidden rounded-full bg-black/60">
          <div
            className="h-full transition-all duration-500"
            style={{ width: `${Math.min(Math.max(progress, 0), 100)}%`, background: colour }}
          />
        </div>
      )}
    </div>
  )
}

/** The ingestion chart's y-axis: the top of the range, its midpoint, and zero. */
function AxisLabels({ max }: { max: number }) {
  return (
    <div className="text-on-surface-variant/40 pointer-events-none absolute left-0 top-0 flex h-full flex-col justify-between text-[10px] font-bold tabular-nums">
      <span>{formatCount(max)}</span>
      <span>{formatCount(Math.round(max / 2))}</span>
      <span>0</span>
    </div>
  )
}

export default function OverviewPage() {
  const queryClient = useQueryClient()
  const { toast, show, dismiss } = useToast()
  const [historyHours, setHistoryHours] = useState<(typeof HISTORY_WINDOWS)[number]>(8)

  const { data: kpis, isLoading } = useQuery<KPIs>({
    queryKey: ['kpis'],
    queryFn: () => getJson<KPIs>('/api/overview/kpis'),
    refetchInterval: 3000,
  })

  const { data: history } = useQuery<IngestionHistory>({
    queryKey: ['ingestion-history', historyHours],
    queryFn: () => getJson<IngestionHistory>(`/api/overview/ingestion-history?hours=${historyHours}`),
    refetchInterval: 15000,
  })

  const resync = useMutation({
    mutationFn: () => postJson<{ message: string }>('/api/overview/resync'),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['kpis'] })
      show(data.message ?? 'Re-sync complete')
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  const buckets = history?.buckets ?? []
  const peakBucket = Math.max(...buckets.map((b) => b.count), 1)
  const labelEvery = Math.max(1, Math.floor(buckets.length / 8))

  const healthPercent = Math.round((kpis?.platform_health_score ?? 0) * 100)
  const flyingPercent = kpis?.total_drones
    ? Math.round((kpis.active_flying_drones / kpis.total_drones) * 100)
    : 0
  const threats = (kpis?.near_zone_count ?? 0) + (kpis?.predicted_breach_count ?? 0)

  return (
    <>
      {toast && <Toast toast={toast} onDismiss={dismiss} />}

      {/* Headline: every figure below comes from a query against the running stack. */}
      <section className="grid grid-cols-1 items-stretch gap-8 lg:grid-cols-3">
        <div className="glass-panel relative flex flex-col justify-between overflow-hidden rounded-xl p-10 lg:col-span-2">
          <div>
            <div className="mb-4 flex items-center gap-3">
              <span className="bg-primary h-2 w-2 animate-pulse rounded-full" />
              <span className="font-label text-primary-dim text-[0.6875rem] font-bold uppercase tracking-widest">
                Live fleet
              </span>
            </div>
            <h1 className="font-headline mb-6 text-4xl font-black uppercase tracking-tighter lg:text-5xl">
              {isLoading
                ? 'Connecting…'
                : `${formatCount(kpis?.active_flying_drones)} of ${formatCount(kpis?.total_drones)} airborne`}
            </h1>
            <p className="text-on-surface-variant mb-8 max-w-xl text-lg font-light leading-relaxed">
              {kpis && kpis.total_drones > 0 ? (
                <>
                  Ingesting {formatCount(Math.round(kpis.ingestion_rate_per_sec))} events per second
                  into Cassandra, {formatCount(kpis.total_events)} recorded so far. Mean speed{' '}
                  {kpis.avg_speed_mps.toFixed(1)} m/s, mean altitude{' '}
                  {kpis.avg_altitude_m.toFixed(0)} m.{' '}
                  {threats > 0
                    ? `${threats} asset${threats === 1 ? '' : 's'} near or predicted to enter restricted airspace.`
                    : 'No assets near restricted airspace.'}
                </>
              ) : (
                <>
                  No telemetry yet. The producer and the ingest sink populate Cassandra within a
                  minute of the stack starting; this page follows them.
                </>
              )}
            </p>
          </div>
          <div className="flex flex-wrap gap-4">
            <button
              onClick={() => resync.mutate()}
              disabled={resync.isPending}
              className="bg-primary hover:bg-primary-dim text-on-primary font-headline flex cursor-pointer items-center gap-2 px-8 py-3 font-bold tracking-wider transition-all active:scale-95 disabled:opacity-60"
            >
              {resync.isPending ? (
                <>
                  <MaterialIcon name="sync" className="animate-spin text-[16px]" /> Re-syncing…
                </>
              ) : (
                <>
                  <MaterialIcon name="sync" className="text-[16px]" /> Reconnect engines
                </>
              )}
            </button>
            <a
              href={`/api/overview/ingestion-history/csv?hours=${historyHours}`}
              className="border-primary/20 hover:border-primary/60 text-primary font-headline flex cursor-pointer items-center gap-2 border px-8 py-3 font-bold tracking-wider transition-all active:scale-95"
            >
              <MaterialIcon name="download" className="text-[16px]" /> Download {historyHours}h log
            </a>
          </div>
        </div>

        {/* Platform health: the share of the stack's services this backend can reach. */}
        <div className="glass-panel flex flex-col items-center justify-center rounded-xl p-10 text-center">
          <div className="relative flex h-48 w-48 items-center justify-center">
            <svg className="absolute inset-0 h-full w-full" viewBox="0 0 100 100">
              <circle cx="50" cy="50" r="44" fill="none" stroke="#20262f" strokeWidth="8" />
              <circle
                cx="50"
                cy="50"
                r="44"
                fill="none"
                stroke={healthPercent === 100 ? 'var(--color-positive)' : 'var(--color-secondary)'}
                strokeWidth="8"
                strokeDasharray={`${(healthPercent / 100) * 276.5} 276.5`}
                strokeLinecap="round"
                transform="rotate(-90 50 50)"
              />
            </svg>
            <div className="border-primary/10 absolute inset-3 flex flex-col items-center justify-center rounded-full border bg-background">
              <span className="font-headline text-5xl font-black tabular-nums">{healthPercent}</span>
              <span className="font-label text-on-surface-variant text-[0.6875rem] uppercase tracking-wider">
                percent
              </span>
            </div>
          </div>
          <div className="mt-8">
            <h2 className="font-headline text-xl font-bold uppercase tracking-tight">
              Services reachable
            </h2>
            <Link
              to="/health"
              className="text-on-surface-variant hover:text-primary mt-1 inline-flex items-center gap-1 text-xs uppercase tracking-wider transition-colors"
            >
              Per-service detail <MaterialIcon name="chevron_right" className="text-[14px]" />
            </Link>
          </div>
        </div>
      </section>

      <section className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-4">
        <div className="animate-stagger-in" style={{ '--stagger-delay': '0ms' } as React.CSSProperties}>
          <KPICard
            icon="database"
            label="Ingestion rate"
            value={formatCount(Math.round(kpis?.ingestion_rate_per_sec ?? 0))}
            unit="events/sec"
            note="measured"
            progress={((kpis?.ingestion_rate_per_sec ?? 0) / RATE_SCALE_MAX) * 100}
          />
        </div>
        <div className="animate-stagger-in" style={{ '--stagger-delay': '60ms' } as React.CSSProperties}>
          <KPICard
            icon="flight"
            label="Airborne"
            value={formatCount(kpis?.active_flying_drones)}
            unit={`of ${formatCount(kpis?.total_drones)}`}
            note={`${formatCount(kpis?.grounded_drones)} grounded`}
            progress={flyingPercent}
            colour="var(--color-primary)"
          />
        </div>
        <div className="animate-stagger-in" style={{ '--stagger-delay': '120ms' } as React.CSSProperties}>
          <KPICard
            icon="gpp_maybe"
            label="Zone exposure"
            value={formatCount(threats)}
            unit="assets"
            note={threats > 0 ? 'action needed' : 'clear'}
            progress={kpis?.total_drones ? (threats / kpis.total_drones) * 100 : 0}
            colour={threats > 0 ? 'var(--color-tertiary)' : 'var(--color-positive)'}
          />
        </div>
        <div className="animate-stagger-in" style={{ '--stagger-delay': '180ms' } as React.CSSProperties}>
          <KPICard
            icon="inventory_2"
            label="Events stored"
            value={formatCount(kpis?.total_events)}
            unit="rows"
            note="since reset"
          />
        </div>
      </section>

      <section className="grid grid-cols-1 gap-8 lg:grid-cols-3">
        <div className="glass-panel rounded-xl p-8 lg:col-span-2">
          <div className="mb-10 flex items-start justify-between">
            <div>
              <h2 className="font-headline text-lg font-bold uppercase tracking-wide">
                Ingestion volume
              </h2>
              <p className="text-on-surface-variant mt-1 text-xs font-medium uppercase tracking-wider">
                Counter table, 30-minute buckets · {historyHours}h window
              </p>
            </div>
            <div className="flex gap-2">
              {HISTORY_WINDOWS.map((hours) => (
                <button
                  key={hours}
                  onClick={() => setHistoryHours(hours)}
                  className={`rounded px-3 py-1 text-[10px] font-bold transition-colors ${
                    historyHours === hours
                      ? 'bg-primary text-on-primary'
                      : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
                  }`}
                >
                  {hours}h
                </button>
              ))}
            </div>
          </div>
          <div className="relative h-64 pl-10">
            <AxisLabels max={peakBucket} />
            {buckets.length === 0 ? (
              <p className="text-on-surface-variant/60 flex h-full items-center justify-center text-xs uppercase tracking-wider">
                No ingestion counted yet
              </p>
            ) : (
              <svg viewBox={`0 0 ${buckets.length} 100`} preserveAspectRatio="none" className="h-full w-full">
                <defs>
                  <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--color-primary)" stopOpacity="0.35" />
                    <stop offset="100%" stopColor="var(--color-primary)" stopOpacity="0.02" />
                  </linearGradient>
                </defs>
                {/* Area fill */}
                <polygon
                  points={`0,100 ${buckets.map((b, i) => `${i},${100 - (b.count / peakBucket) * 95}`).join(' ')} ${buckets.length - 1},100`}
                  fill="url(#areaGrad)"
                />
                {/* Line */}
                <polyline
                  points={buckets.map((b, i) => `${i},${100 - (b.count / peakBucket) * 95}`).join(' ')}
                  fill="none"
                  stroke="var(--color-primary)"
                  strokeWidth="0.8"
                  vectorEffect="non-scaling-stroke"
                  strokeLinejoin="round"
                />
                {/* Interactive hover bars (invisible but catch mouse events) */}
                {buckets.map((bucket, i) => (
                  <g key={bucket.timestamp}>
                    <rect
                      x={i}
                      y={0}
                      width={1}
                      height={100}
                      fill="transparent"
                      className="cursor-crosshair"
                    >
                      <title>{`${bucket.time}: ${bucket.count.toLocaleString()} events`}</title>
                    </rect>
                    {/* Dot on hover */}
                    <circle
                      cx={i + 0.5}
                      cy={100 - (bucket.count / peakBucket) * 95}
                      r="1.5"
                      fill="var(--color-primary)"
                      className="opacity-0 hover:opacity-100 transition-opacity"
                      style={{ pointerEvents: 'none' }}
                    />
                  </g>
                ))}
              </svg>
            )}
            {/* X-axis labels */}
            {buckets.length > 0 && (
              <div className="mt-2 flex justify-between">
                {buckets.filter((_, i) => i % labelEvery === 0).map((bucket) => (
                  <span key={bucket.timestamp} className="text-on-surface-variant/60 text-[9px] font-bold">
                    {bucket.time}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="glass-panel rounded-xl p-8">
          <h2 className="font-headline text-lg font-bold uppercase tracking-wide">Fleet envelope</h2>
          <p className="text-on-surface-variant mt-1 text-xs font-medium uppercase tracking-wider">
            Airborne assets only
          </p>
          <dl className="mt-8 space-y-6">
            {[
              { label: 'Mean speed', value: `${(kpis?.avg_speed_mps ?? 0).toFixed(1)} m/s` },
              { label: 'Peak speed', value: `${(kpis?.max_speed_mps ?? 0).toFixed(1)} m/s` },
              { label: 'Mean altitude', value: `${(kpis?.avg_altitude_m ?? 0).toFixed(0)} m` },
              { label: 'Peak altitude', value: `${(kpis?.max_altitude_m ?? 0).toFixed(0)} m` },
            ].map((row) => (
              <div key={row.label} className="flex items-baseline justify-between">
                <dt className="text-on-surface-variant text-[0.6875rem] uppercase tracking-wider">
                  {row.label}
                </dt>
                <dd className="font-headline text-xl font-bold tabular-nums">{row.value}</dd>
              </div>
            ))}
          </dl>
        </div>
      </section>

      {kpis?.latest_alerts && kpis.latest_alerts.length > 0 && (
        <section className="glass-panel overflow-hidden rounded-xl p-8">
          <div className="mb-8 flex items-center justify-between">
            <h2 className="font-headline text-lg font-bold uppercase tracking-wide">
              Latest alerts
            </h2>
            <Link
              to="/alerts"
              className="text-on-surface-variant hover:text-primary flex items-center gap-2 text-xs font-bold uppercase tracking-wider transition-colors"
            >
              All alerts <MaterialIcon name="chevron_right" className="text-[16px]" />
            </Link>
          </div>
          <ul className="space-y-4">
            {kpis.latest_alerts.slice(0, 3).map((alert) => (
              <li
                key={alert.alert_id}
                className={`flex items-center gap-6 rounded border-l-4 p-4 ${
                  alert.severity === 'critical'
                    ? 'border-tertiary bg-surface-container-high'
                    : 'border-secondary bg-surface-container'
                }`}
              >
                <div className="flex w-20 shrink-0 flex-col">
                  <span
                    className={`text-[10px] font-black uppercase tracking-wider ${
                      alert.severity === 'critical' ? 'text-tertiary' : 'text-secondary'
                    }`}
                  >
                    {alert.severity}
                  </span>
                  <span className="text-on-surface-variant font-label text-xs tabular-nums">
                    {alert.alert_time.slice(11, 19)}
                  </span>
                </div>
                <div className="min-w-0">
                  <p className="font-headline text-sm font-bold uppercase tracking-tight">
                    {alert.alert_type}
                  </p>
                  <p className="text-on-surface-variant mt-1 text-xs">{alert.message}</p>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}
    </>
  )
}
