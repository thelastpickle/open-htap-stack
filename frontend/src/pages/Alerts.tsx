import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import MaterialIcon from '../components/MaterialIcon'
import { getJson } from '../lib/api'

interface AlertRecord {
  alert_id: string
  alert_time: string
  entity_id: string
  alert_type: string
  severity: string
  zone_id?: string | null
  latitude: number
  longitude: number
  altitude_m: number
  message: string
  risk_score: number
}

interface AlertsResponse {
  alerts: AlertRecord[]
  total_count: number
}

const SEVERITIES = ['critical', 'high', 'warning'] as const

const SEVERITY_STYLES: Record<string, { border: string; text: string }> = {
  critical: { border: 'border-tertiary', text: 'text-tertiary' },
  high: { border: 'border-secondary', text: 'text-secondary' },
  warning: { border: 'border-secondary/60', text: 'text-secondary' },
}
const DEFAULT_SEVERITY_STYLE = { border: 'border-outline-variant', text: 'text-on-surface-variant' }

function RiskBar({ score }: { score: number }) {
  const percent = Math.round(score * 100)
  const colour =
    score > 0.7 ? 'var(--color-tertiary)' : score > 0.4 ? 'var(--color-secondary)' : 'var(--color-primary)'
  return (
    <div className="mt-1 flex items-center gap-2">
      <div className="h-1 flex-1 overflow-hidden rounded-full bg-black/40">
        <div className="h-full rounded-full" style={{ width: `${percent}%`, background: colour }} />
      </div>
      <span className="text-[9px] font-bold tabular-nums" style={{ color: colour }}>
        {percent}%
      </span>
    </div>
  )
}

export default function AlertsPage() {
  const navigate = useNavigate()
  const [severityFilter, setSeverityFilter] = useState<string>('')
  // Acknowledgement is a view-side annotation only: nothing is written back, and
  // it resets on reload.  Persisting it would need an alert-state table.
  const [acknowledged, setAcknowledged] = useState<Set<string>>(new Set())

  // Fetched unfiltered so the per-severity counts below always add up to the
  // total, and switching tabs needs no round trip.
  const { data, isLoading, isFetching } = useQuery<AlertsResponse>({
    queryKey: ['alerts'],
    queryFn: () => getJson<AlertsResponse>('/api/alerts?limit=200'),
    refetchInterval: 5000,
  })

  const alerts = data?.alerts ?? []
  const counts = useMemo(() => {
    const bySeverity: Record<string, number> = {}
    for (const alert of alerts) bySeverity[alert.severity] = (bySeverity[alert.severity] ?? 0) + 1
    return bySeverity
  }, [alerts])

  const visible = severityFilter ? alerts.filter((a) => a.severity === severityFilter) : alerts

  const showOnMap = (alert: AlertRecord) => {
    sessionStorage.setItem(
      'mapFlyTo',
      JSON.stringify({ lat: alert.latitude, lng: alert.longitude, entityId: alert.entity_id }),
    )
    navigate('/map')
  }

  return (
    <section className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="mb-2 flex items-center gap-3">
            <span className="bg-tertiary h-2 w-2 animate-pulse rounded-full" />
            <span className="font-label text-tertiary text-[0.6875rem] font-bold uppercase tracking-widest">
              Incident stream
            </span>
          </div>
          <h1 className="font-headline text-4xl font-black uppercase tracking-tighter">Alerts</h1>
          <p className="text-on-surface-variant mt-2 text-xs">
            Written by the ingest sink as assets approach the restricted zones, read back from
            alerts_by_bucket one hourly partition at a time.
          </p>
        </div>
        <div className="flex items-center gap-4">
          {isFetching && !isLoading && (
            <span className="text-primary/60 flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider">
              <MaterialIcon name="sync" className="animate-spin text-[14px]" /> Live
            </span>
          )}
          <div className="text-right">
            <p className="text-on-surface-variant text-[10px] uppercase tracking-wider">
              Last 6 hours
            </p>
            <p className="font-headline text-2xl font-black tabular-nums">{alerts.length}</p>
          </div>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        {[{ key: '', label: 'All' }, ...SEVERITIES.map((s) => ({ key: s, label: s }))].map((tab) => {
          const count = tab.key === '' ? alerts.length : (counts[tab.key] ?? 0)
          const isActive = severityFilter === tab.key
          return (
            <button
              key={tab.key || 'all'}
              onClick={() => setSeverityFilter(tab.key)}
              className={`flex items-center gap-2 rounded-lg px-4 py-2 text-xs font-black uppercase tracking-wider transition-colors ${
                isActive
                  ? 'bg-primary text-on-primary'
                  : 'bg-surface-container text-on-surface-variant hover:text-primary'
              }`}
            >
              {tab.label}
              <span
                className={`rounded px-1.5 py-0.5 text-[9px] tabular-nums ${isActive ? 'bg-black/20' : 'bg-white/10'}`}
              >
                {count}
              </span>
            </button>
          )
        })}
      </div>

      {isLoading ? (
        <div className="glass-panel flex flex-col items-center gap-4 rounded-xl p-20">
          <MaterialIcon name="sync" className="text-tertiary animate-spin text-4xl" />
          <p className="text-on-surface-variant text-[10px] font-black uppercase tracking-widest">
            Loading alerts…
          </p>
        </div>
      ) : visible.length === 0 ? (
        <div className="glass-panel rounded-xl p-20 text-center">
          <MaterialIcon name="check_circle" className="text-positive mb-4 text-6xl" />
          <p className="font-headline text-lg font-bold uppercase tracking-wide">No alerts</p>
          <p className="text-on-surface-variant mt-2 text-sm">
            {severityFilter
              ? `Nothing at ${severityFilter} severity in the last six hours.`
              : 'No asset has come within 500 m of a restricted zone in the last six hours.'}
          </p>
        </div>
      ) : (
        <ul className="space-y-3">
          {visible.map((alert) => {
            const style = SEVERITY_STYLES[alert.severity] ?? DEFAULT_SEVERITY_STYLE
            const isAcknowledged = acknowledged.has(alert.alert_id)
            return (
              <li
                key={alert.alert_id}
                className={`rounded-r-lg border-l-4 bg-surface-container p-5 transition-opacity ${style.border} ${
                  isAcknowledged ? 'opacity-40' : ''
                }`}
              >
                <div className="flex flex-wrap items-start justify-between gap-6">
                  <div className="flex min-w-0 flex-1 items-start gap-4">
                    <div className="flex w-16 shrink-0 flex-col items-center gap-1">
                      <span
                        className={`text-[9px] font-black uppercase tracking-wider ${style.text}`}
                      >
                        {alert.severity}
                      </span>
                      <span className="text-on-surface-variant text-[9px] tabular-nums">
                        {alert.alert_time.slice(11, 19)}
                      </span>
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-3">
                        <h2 className="font-headline text-sm font-bold uppercase tracking-tight">
                          {alert.alert_type}
                        </h2>
                        <span className="font-label text-on-surface-variant rounded bg-white/5 px-2 py-0.5 text-[10px]">
                          {alert.entity_id}
                        </span>
                        {alert.zone_id && (
                          <span className="font-label text-secondary bg-secondary/10 rounded px-2 py-0.5 text-[10px]">
                            {alert.zone_id}
                          </span>
                        )}
                        {isAcknowledged && (
                          <span className="font-label text-primary bg-primary/10 flex items-center gap-1 rounded px-2 py-0.5 text-[10px]">
                            <MaterialIcon name="check" className="text-[12px]" /> Acknowledged here
                          </span>
                        )}
                      </div>
                      <p className="text-on-surface-variant mt-1.5 text-xs leading-relaxed">
                        {alert.message}
                      </p>
                      <RiskBar score={alert.risk_score} />
                      {(alert.latitude !== 0 || alert.longitude !== 0) && (
                        <p className="text-on-surface-variant/50 mt-1 font-mono text-[9px]">
                          {alert.latitude.toFixed(4)}, {alert.longitude.toFixed(4)} ·{' '}
                          {alert.altitude_m.toFixed(0)} m
                        </p>
                      )}
                    </div>
                  </div>

                  <div className="flex shrink-0 flex-col gap-2">
                    {!isAcknowledged && (
                      <button
                        onClick={() =>
                          setAcknowledged((prev) => new Set(prev).add(alert.alert_id))
                        }
                        className="bg-tertiary/10 hover:bg-tertiary/25 text-tertiary rounded px-4 py-2 text-[10px] font-black uppercase tracking-wider transition-colors"
                      >
                        Acknowledge
                      </button>
                    )}
                    {(alert.latitude !== 0 || alert.longitude !== 0) && (
                      <button
                        onClick={() => showOnMap(alert)}
                        className="bg-surface-container-highest text-on-surface-variant hover:text-primary flex items-center gap-1 rounded px-4 py-2 text-[10px] font-black uppercase tracking-wider transition-colors"
                      >
                        <MaterialIcon name="map" className="text-[14px]" /> View on map
                      </button>
                    )}
                  </div>
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
