import { useCallback, useState } from 'react'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from './components/MaterialIcon'
import AlertsPage from './pages/Alerts'
import ExplorePage from './pages/Explore'
import HealthPage from './pages/Health'
import MapPage from './pages/Map'
import OverviewPage from './pages/Overview'
import SettingsPage from './pages/Settings'
import TransactionsPage from './pages/Transactions'

const NAV_ITEMS = [
  { path: '/', label: 'Overview', icon: 'dashboard' },
  { path: '/map', label: 'Map', icon: 'map' },
  { path: '/alerts', label: 'Alerts', icon: 'warning' },
  { path: '/explore', label: 'Explore', icon: 'search' },
  // Beside Explore, because the two are a pair: Explore reads the same rows five
  // ways, and this one writes them the ways none of those five can.
  { path: '/transactions', label: 'Transactions', icon: 'account_tree' },
  { path: '/health', label: 'Health', icon: 'monitor_heart' },
  { path: '/settings', label: 'Settings', icon: 'settings' },
]

/**
 * The sidebar's width, and the two offsets that have to match it: the top bar is
 * positioned beside it and the main column is indented past it.  Written as class
 * names rather than a number, because Tailwind reads the source for literals and
 * would not see a width built by a template.
 */
const SIDEBAR = {
  expanded: { aside: 'w-64', bar: 'left-64', main: 'ml-64' },
  collapsed: { aside: 'w-16', bar: 'left-16', main: 'ml-16' },
} as const

const NAV_COLLAPSED_KEY = 'mission-control.nav-collapsed'

/**
 * Whether the nav is collapsed, remembered across reloads: on a small screen, or
 * beside a wide comparison table, the collapsed nav is the state an operator wants
 * to keep rather than to re-choose on every page load.
 */
function useCollapsedNav(): [boolean, () => void] {
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(NAV_COLLAPSED_KEY) === '1',
  )
  const toggle = useCallback(() => {
    setCollapsed((wasCollapsed) => {
      localStorage.setItem(NAV_COLLAPSED_KEY, wasCollapsed ? '0' : '1')
      return !wasCollapsed
    })
  }, [])
  return [collapsed, toggle]
}

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

function Sidebar({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  return (
    <aside
      className={`fixed left-0 top-0 z-40 flex h-full flex-col border-r border-white/5 bg-surface-container-low/80 pb-8 backdrop-blur-xl transition-[width] duration-200 ease-out ${
        collapsed ? `${SIDEBAR.collapsed.aside} px-2` : `${SIDEBAR.expanded.aside} px-4`
      }`}
    >
      <div
        className={`text-primary flex h-[76px] items-center border-b border-white/5 ${
          collapsed ? 'justify-center' : 'justify-between'
        }`}
      >
        {!collapsed && <Wordmark />}
        <button
          type="button"
          onClick={onToggle}
          aria-expanded={!collapsed}
          aria-label={collapsed ? 'Expand the navigation' : 'Collapse the navigation'}
          title={collapsed ? 'Expand the navigation' : 'Collapse the navigation'}
          className="text-on-surface-variant hover:text-on-surface flex h-8 w-8 shrink-0 items-center justify-center rounded-full transition-colors duration-200 hover:bg-white/5"
        >
          <MaterialIcon name={collapsed ? 'chevron_right' : 'chevron_left'} className="text-[20px]" />
        </button>
      </div>

      <nav className="mt-8 flex-1 space-y-1">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === '/'}
            /* The label is the only affordance a nav item has, so a collapsed item
               carries it as a tooltip rather than dropping it. */
            title={collapsed ? item.label : undefined}
            className={({ isActive }) =>
              `flex items-center py-3 transition-all duration-200 ease-out ${
                collapsed ? 'justify-center px-0' : 'gap-4 px-4'
              } ${
                isActive
                  ? 'border-l-4 border-primary bg-primary/10 text-primary shadow-[inset_0_0_20px_rgba(153,247,255,0.05)]'
                  : 'text-on-surface-variant hover:bg-white/5 hover:text-on-surface hover:translate-x-1'
              }`
            }
          >
            {({ isActive }: { isActive: boolean }) => (
              <>
                <MaterialIcon name={item.icon} filled={isActive} />
                {!collapsed && (
                  <span className="font-label text-[0.6875rem] font-medium uppercase tracking-wider">
                    {item.label}
                  </span>
                )}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      <div className="mt-auto flex flex-col items-center gap-4 border-t border-white/5 pt-6">
        <img
          src="/KTlogo-white.svg"
          alt="Kermit Technology"
          className={`w-auto opacity-80 ${collapsed ? 'h-6' : 'h-10'}`}
        />
        {!collapsed && (
          <p className="text-on-surface-variant/60 text-[9px] leading-relaxed text-center">
            Cassandra for the live fleet, Presto for analytics over the same rows, and Spark for batch by
            two routes: through the request path, and straight off the SSTable files. One store, no ETL
            between them.
          </p>
        )}
      </div>
    </aside>
  )
}

/**
 * The projects this stack runs, each linked to its own home.  Named here rather
 * than in prose so the attribution below has something to attach to, and so a
 * reader can go and check any claim the dashboard makes about a path.
 */
const CREDITS = [
  { label: 'Apache Cassandra®', href: 'https://cassandra.apache.org/' },
  { label: 'Apache Kafka®', href: 'https://kafka.apache.org/' },
  { label: 'Apache Spark®', href: 'https://spark.apache.org/' },
  { label: 'Apache DataFusion™', href: 'https://datafusion.apache.org/' },
  { label: 'Presto', href: 'https://prestodb.io/' },
  { label: 'cqlite', href: 'https://github.com/pmcfadin/cqlite' },
]

/**
 * Trademark attribution, on every page because every page names at least one of
 * these projects.  Two distinctions are kept rather than blurred: Presto is a
 * Linux Foundation mark, not an Apache one, and cqlite carries the Apache licence
 * without being an Apache project, so neither may be presented as endorsing this
 * demonstration.
 */
function Footer() {
  return (
    <footer className="mx-auto mt-12 max-w-[1600px] border-t border-white/5 pt-6">
      <div className="text-on-surface-variant/70 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px]">
        <span className="font-bold uppercase tracking-wider">Built on</span>
        {CREDITS.map((credit) => (
          <a
            key={credit.href}
            href={credit.href}
            target="_blank"
            rel="noreferrer noopener"
            className="hover:text-primary transition-colors"
          >
            {credit.label}
          </a>
        ))}
      </div>
      <p className="text-on-surface-variant/50 mt-3 max-w-4xl text-[9px] leading-relaxed">
        Apache®, Apache Cassandra®, Apache Kafka®, Apache Spark® and Apache DataFusion™ are either
        registered trademarks or trademarks of the Apache Software Foundation in the United States
        and/or other countries. No endorsement by the Apache Software Foundation is implied by the
        use of these marks. Presto is a trademark of the Linux Foundation. cqlite is an independent
        project under the Apache License 2.0, and is not a project of the Apache Software
        Foundation. This dashboard is a demonstration, and speaks for none of them.
      </p>
    </footer>
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

function TopBar({ collapsed }: { collapsed: boolean }) {
  const { data } = useQuery<{ status: string }>({
    queryKey: ['api-health'],
    queryFn: () => fetch('/api/health').then((r) => r.json()),
    refetchInterval: 5000,
  })
  const online = data?.status === 'ok'

  return (
    <header
      className={`fixed right-0 top-0 z-50 flex items-center justify-between border-b border-white/5 bg-background/70 px-8 py-4 backdrop-blur-2xl transition-[left] duration-200 ease-out ${
        collapsed ? SIDEBAR.collapsed.bar : SIDEBAR.expanded.bar
      }`}
    >
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
  const [collapsed, toggleCollapsed] = useCollapsedNav()

  return (
    <BrowserRouter>
      <div className="min-h-screen bg-background text-on-background">
        <TopBar collapsed={collapsed} />
        <Sidebar collapsed={collapsed} onToggle={toggleCollapsed} />
        <main
          className={`px-8 pb-12 pt-24 transition-[margin] duration-200 ease-out ${
            collapsed ? SIDEBAR.collapsed.main : SIDEBAR.expanded.main
          }`}
        >
          <div className="mx-auto max-w-[1600px] space-y-8">
            <Routes>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/map" element={<MapPage />} />
              <Route path="/alerts" element={<AlertsPage />} />
              <Route path="/explore" element={<ExplorePage />} />
              <Route path="/transactions" element={<TransactionsPage />} />
              <Route path="/health" element={<HealthPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Routes>
          </div>
          <Footer />
        </main>
      </div>
    </BrowserRouter>
  )
}
