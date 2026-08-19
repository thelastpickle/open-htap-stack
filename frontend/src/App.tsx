import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from './components/MaterialIcon'
import AlertsPage from './pages/Alerts'
import ExplorePage from './pages/Explore'
import HealthPage from './pages/Health'
import MapPage from './pages/Map'
import OverviewPage from './pages/Overview'
import SettingsPage from './pages/Settings'

const NAV_ITEMS = [
  { path: '/', label: 'Overview', icon: 'dashboard' },
  { path: '/map', label: 'Map', icon: 'map' },
  { path: '/alerts', label: 'Alerts', icon: 'warning' },
  { path: '/explore', label: 'Explore', icon: 'search' },
  { path: '/health', label: 'Health', icon: 'monitor_heart' },
  { path: '/settings', label: 'Settings', icon: 'settings' },
]

/** The three tiers the stack runs, with the latency each is currently showing. */
const LATENCY_TIERS = [
  { key: 'cassandra_point_read_ms', label: 'Point read', engine: 'Cassandra', color: 'var(--color-primary)' },
  { key: 'presto_scan_ms', label: 'Scan', engine: 'Presto', color: 'var(--color-secondary)' },
  { key: 'vector_search_ms', label: 'Vector', engine: 'Cassandra SAI', color: 'var(--color-accent)' },
] as const

type Latency = Partial<Record<(typeof LATENCY_TIERS)[number]['key'], number | null>>

function Wordmark() {
  return (
    <div className="flex items-center gap-3">
      <svg viewBox="0 0 32 32" className="h-8 w-8 shrink-0" aria-hidden="true">
        <circle cx="16" cy="16" r="13" fill="none" stroke="currentColor" strokeWidth="1.5" opacity="0.35" />
        <circle cx="16" cy="16" r="8" fill="none" stroke="currentColor" strokeWidth="1.5" opacity="0.7" />
        <circle cx="16" cy="16" r="3.5" fill="currentColor" />
      </svg>
      <div>
        <p className="font-headline text-sm font-black tracking-widest uppercase leading-none">
          Mission Control
        </p>
        <p className="text-on-surface-variant text-[9px] font-medium tracking-widest uppercase mt-1">
          Open HTAP Stack
        </p>
      </div>
    </div>
  )
}

function Sidebar() {
  return (
    <aside className="fixed left-0 top-0 z-40 flex h-full w-64 flex-col border-r border-white/5 bg-surface-container-low/80 px-4 pb-8 backdrop-blur-xl">
      <div className="text-primary flex h-[76px] items-center border-b border-white/5">
        <Wordmark />
      </div>

      <nav className="mt-8 flex-1 space-y-1">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            className={({ isActive }) =>
              `flex items-center gap-4 px-4 py-3 transition-all duration-200 ease-out ${
                isActive
                  ? 'border-l-4 border-primary bg-primary/10 text-primary shadow-[inset_0_0_20px_rgba(153,247,255,0.05)]'
                  : 'text-on-surface-variant hover:bg-white/5 hover:text-on-surface hover:translate-x-1'
              }`
            }
          >
            {({ isActive }: { isActive: boolean }) => (
              <>
                <MaterialIcon name={item.icon} filled={isActive} />
                <span className="font-label text-[0.6875rem] font-medium uppercase tracking-wider">
                  {item.label}
                </span>
              </>
            )}
          </NavLink>
        ))}
      </nav>

      <div className="mt-auto flex flex-col items-center gap-4 border-t border-white/5 pt-6">
        <img src="/KTlogo-white.svg" alt="Kermit Technology" className="h-10 w-auto opacity-80" />
        <p className="text-on-surface-variant/60 text-[9px] leading-relaxed text-center">
          Cassandra for the live fleet, Presto for analytics over the same rows, and Spark for batch by
          two routes: through the request path, and straight off the SSTable files. One store, no ETL
          between them.
        </p>
      </div>
    </aside>
  )
}

/** Live latency per tier, measured by the backend against the running stack. */
function LatencyStrip() {
  const { data } = useQuery<Latency>({
    queryKey: ['latency'],
    queryFn: () => fetch('/api/demo/latency').then((r) => r.json()),
    refetchInterval: 8000,
  })

  return (
    <div className="flex items-center gap-1 rounded-full border border-white/5 bg-surface-container-low px-3 py-1.5">
      {LATENCY_TIERS.map((tier, index) => {
        const value = data?.[tier.key]
        const colour = value != null ? tier.color : 'var(--color-outline-variant)'
        return (
          <div key={tier.key} className="flex items-center gap-2">
            {index > 0 && <span className="mx-1 h-4 w-px bg-white/10" />}
            <div className="flex flex-col items-center" title={`${tier.label} — ${tier.engine}`}>
              <span
                className="text-[8px] font-black uppercase tracking-wider"
                style={{ color: colour }}
              >
                {tier.label}
              </span>
              <span
                className="font-headline text-[11px] font-black leading-none tabular-nums"
                style={{ color: colour }}
              >
                {value != null ? `${value.toFixed(0)}ms` : '—'}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function TopBar() {
  const { data } = useQuery<{ status: string }>({
    queryKey: ['api-health'],
    queryFn: () => fetch('/api/health').then((r) => r.json()),
    refetchInterval: 5000,
  })
  const online = data?.status === 'ok'

  return (
    <header className="fixed left-64 right-0 top-0 z-50 flex items-center justify-between border-b border-white/5 bg-background/70 px-8 py-4 backdrop-blur-2xl">
      <span className="font-headline text-primary text-xl font-bold uppercase tracking-widest">
        Drone Fleet Operations
      </span>
      <div className="flex items-center gap-4">
        <LatencyStrip />
        <div
          className="flex items-center gap-2 rounded-full bg-surface-container px-4 py-1.5"
          title={online ? 'The dashboard API is responding' : 'The dashboard API is not responding'}
        >
          <span
            className={`h-2 w-2 rounded-full ${online ? 'bg-positive animate-pulse' : 'bg-tertiary'}`}
          />
          <span className="text-on-surface-variant text-[10px] font-bold uppercase tracking-wider">
            {online ? 'API live' : 'API unreachable'}
          </span>
        </div>
      </div>
    </header>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-background text-on-background">
        <TopBar />
        <Sidebar />
        <main className="ml-64 px-8 pb-12 pt-24">
          <div className="mx-auto max-w-[1600px] space-y-8">
            <Routes>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/map" element={<MapPage />} />
              <Route path="/alerts" element={<AlertsPage />} />
              <Route path="/explore" element={<ExplorePage />} />
              <Route path="/health" element={<HealthPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Routes>
          </div>
        </main>
      </div>
    </BrowserRouter>
  )
}
