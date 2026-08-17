import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import Toast, { useToast, type ToastKind } from '../components/Toast'
import { formatCount, formatMs, getJson, postJson } from '../lib/api'

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

/** A query an engine is still working on, whoever submitted it. */
interface RunningQuery {
  engine: string
  id: string
  state: string
  running_s: number
  sql: string
  submitter: string
  tasks_done: number
  tasks_total: number
}

/** The comparison holding the one-at-a-time lock on Explore. */
interface ComparisonRun {
  running_for_s: number
  mode: string
  engines: string[]
  sql: string
  done: string[]
}

interface RunningWork {
  comparison: ComparisonRun | null
  queries: RunningQuery[]
  unreadable: Record<string, string>
}

interface OperationResult {
  ok: boolean
  actions: string[]
}

type ReconnectTarget = 'cassandra' | 'presto' | 'spark' | 'spark_bulk'

const SERVICE_ICONS: Record<string, string> = {
  Cassandra: 'database',
  Kafka: 'stream',
  Presto: 'analytics',
  Spark: 'bolt',
}

/**
 * The container each service runs in, so the page can show the one command it
 * cannot run itself.  The dashboard is a container beside the others with no
 * control over them, which is the right way round for something a browser can
 * reach; restarting a service is a command on the host.
 */
const SERVICE_CONTAINERS: Record<string, string> = {
  Cassandra: 'cassandra',
  Kafka: 'kafka',
  Presto: 'presto',
  Spark: 'spark',
}

/**
 * Which of the backend's clients belong to each service, for the reconnect
 * control.  Spark has two, because the connector and the bulk reader hold their
 * own sessions so they can run at once.  Kafka has none: nothing here queries it,
 * the health probe just opens a socket.
 */
const SERVICE_CLIENTS: Record<string, ReconnectTarget[]> = {
  Cassandra: ['cassandra'],
  Presto: ['presto'],
  Spark: ['spark', 'spark_bulk'],
  Kafka: [],
}

/** Colour per engine, matching the compare panel on Explore. */
const ENGINE_COLOURS: Record<string, string> = {
  cassandra: 'var(--color-primary)',
  presto: 'var(--color-secondary)',
  spark: 'var(--color-accent)',
  spark_bulk: 'var(--color-positive)',
}

/** How often the running-work view refreshes.  It is a live view, so: often. */
const RUNNING_POLL_MS = 5000

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

/** The one command the dashboard cannot run for you, ready to paste. */
function CopyableCommand({
  command,
  onResult,
}: {
  command: string
  onResult: (message: string, kind?: ToastKind) => void
}) {
  return (
    <button
      onClick={() => {
        if (!navigator.clipboard) {
          onResult('No clipboard here; select the command by hand', 'info')
          return
        }
        navigator.clipboard.writeText(command).then(
          () => onResult(`Copied: ${command}`),
          () => onResult('Could not copy; select the command by hand', 'info'),
        )
      }}
      title="Copy this command"
      className="text-on-surface-variant hover:text-primary flex w-full cursor-pointer items-center gap-2 rounded bg-black/20 px-2 py-1 font-mono text-[10px] transition-colors"
    >
      <MaterialIcon name="content_copy" className="text-[12px] shrink-0" />
      <span className="truncate">{command}</span>
    </button>
  )
}

/**
 * What the engines are working on, and the controls to stop it.
 *
 * The engines are asked directly rather than the dashboard listing what it
 * submitted, so a spark-sql session in the container or a presto-cli query shows
 * up here too, which is usually what you want to know when the dashboard has gone
 * slow for no reason of its own.
 */
function WorkInFlight({ onResult }: { onResult: (message: string, kind?: ToastKind) => void }) {
  const queryClient = useQueryClient()
  const { data, isLoading, error } = useQuery<RunningWork>({
    queryKey: ['running-work'],
    queryFn: () => getJson<RunningWork>('/api/platform/running'),
    refetchInterval: RUNNING_POLL_MS,
  })

  const report = (result: OperationResult) => {
    onResult(result.actions.join(' · ') || 'nothing to do', result.ok ? 'success' : 'info')
    queryClient.invalidateQueries({ queryKey: ['running-work'] })
  }
  const failed = (e: Error) => onResult(e.message, 'error')

  const stopComparison = useMutation({
    mutationFn: () => postJson<OperationResult>('/api/platform/running/cancel-comparison'),
    onSuccess: report,
    onError: failed,
  })
  const killQuery = useMutation({
    mutationFn: (query: RunningQuery) =>
      postJson<OperationResult>('/api/platform/running/kill', {
        engine: query.engine,
        id: query.id,
      }),
    onSuccess: report,
    onError: failed,
  })

  const comparison = data?.comparison
  const queries = data?.queries ?? []

  return (
    <div className="glass-panel rounded-xl p-8">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h2 className="font-headline text-lg font-bold uppercase tracking-wide">Work in flight</h2>
        <p className="text-on-surface-variant text-[10px] uppercase tracking-wider opacity-60">
          Asked of each engine, every {RUNNING_POLL_MS / 1000}s
        </p>
      </div>
      <p className="text-on-surface-variant mt-1 text-xs">
        Everything the engines are working on, whoever asked for it: a query from another browser tab,
        or a <span className="font-mono">presto-cli</span> session in a container, appears here as
        readily as the dashboard's own. Stopping a query is the engine's own cancel, so it stops
        server-side rather than only being abandoned.
      </p>

      {error && (
        <p className="text-tertiary mt-4 text-xs">Could not read what is running: {error.message}</p>
      )}

      {comparison && (
        <div className="border-secondary/30 bg-secondary/5 mt-6 rounded-lg border p-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="min-w-0">
              <p className="text-secondary text-[10px] font-black uppercase tracking-wider">
                A comparison holds the lock
              </p>
              <p className="text-on-surface-variant mt-1 text-[11px]">
                Running {formatMs(comparison.running_for_s * 1000)}, {comparison.mode === 'parallel' ? 'all at once' : 'one at a time'},{' '}
                {comparison.done.length} of {comparison.engines.length} paths answered
                {comparison.done.length > 0 && ` (${comparison.done.join(', ')})`}. Until it
                finishes, Explore refuses to start another.
              </p>
              <p className="text-on-surface-variant mt-2 truncate font-mono text-[10px] opacity-70" title={comparison.sql}>
                {comparison.sql}
              </p>
            </div>
            <button
              onClick={() => stopComparison.mutate()}
              disabled={stopComparison.isPending}
              className="text-tertiary border-tertiary/40 hover:bg-tertiary/10 shrink-0 cursor-pointer rounded border px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider transition-colors disabled:opacity-50"
            >
              {stopComparison.isPending ? 'Stopping…' : 'Stop it'}
            </button>
          </div>
        </div>
      )}

      <div className="mt-6 space-y-2">
        {isLoading && <p className="text-on-surface-variant text-xs">Asking the engines…</p>}
        {!isLoading && queries.length === 0 && (
          <p className="text-on-surface-variant text-xs">
            Nothing running on Presto or Spark.
          </p>
        )}
        {queries.map((query) => (
          <div
            key={`${query.engine}-${query.id}`}
            className="flex flex-wrap items-center gap-x-4 gap-y-2 rounded-lg border border-white/5 bg-surface-container-low px-4 py-3"
          >
            <span
              className="text-[10px] font-black uppercase tracking-wider"
              style={{ color: ENGINE_COLOURS[query.engine] ?? 'var(--color-on-surface-variant)' }}
            >
              {query.engine}
            </span>
            <span className="text-on-surface-variant font-mono text-[10px]">{query.id}</span>
            <span className="text-on-surface-variant text-[10px] uppercase tracking-wider opacity-70">
              {query.state}
            </span>
            <span className="text-on-surface tabular-nums text-[11px] font-bold">
              {formatMs(query.running_s * 1000)}
            </span>
            {query.tasks_total > 0 && (
              <span className="text-on-surface-variant tabular-nums text-[10px]">
                {query.tasks_done}/{query.tasks_total} tasks
              </span>
            )}
            <span
              className="text-on-surface-variant min-w-0 flex-1 truncate font-mono text-[10px] opacity-70"
              title={query.sql}
            >
              {query.sql}
            </span>
            <button
              onClick={() => killQuery.mutate(query)}
              disabled={killQuery.isPending}
              className="text-tertiary border-tertiary/40 hover:bg-tertiary/10 shrink-0 cursor-pointer rounded border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider transition-colors disabled:opacity-50"
            >
              Cancel
            </button>
          </div>
        ))}
      </div>

      {data?.unreadable && Object.keys(data.unreadable).length > 0 && (
        <div className="text-on-surface-variant mt-4 space-y-1 border-t border-white/5 pt-4 text-[10px] leading-relaxed opacity-70">
          {Object.entries(data.unreadable).map(([engine, reason]) => (
            <p key={engine}>
              <span className="uppercase tracking-wider">{engine}</span>: {reason}
            </p>
          ))}
        </div>
      )}
    </div>
  )
}

export default function HealthPage() {
  const { toast, show, dismiss } = useToast()
  const queryClient = useQueryClient()

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

  /**
   * Rebuild the backend's connections to one service.  Spark has two of them, so
   * this is per service rather than per client, which is how the page presents the
   * stack everywhere else.
   */
  const reconnect = useMutation({
    mutationFn: async (targets: ReconnectTarget[]) => {
      const results = await Promise.all(
        targets.map((target) => postJson<OperationResult>('/api/platform/reconnect', { target })),
      )
      return {
        ok: results.every((result) => result.ok),
        actions: results.flatMap((result) => result.actions),
      }
    },
    onSuccess: (result) => {
      show(result.actions.join(' · '), result.ok ? 'success' : 'info')
      queryClient.invalidateQueries({ queryKey: ['platform-health'] })
      queryClient.invalidateQueries({ queryKey: ['engines'] })
    },
    onError: (e: Error) => show(e.message, 'error'),
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
            const clients = SERVICE_CLIENTS[service.name] ?? []
            const container = SERVICE_CONTAINERS[service.name]
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

                <div className="mt-4 space-y-2 border-t border-white/5 pt-4">
                  {clients.length > 0 && (
                    <div className="flex items-center justify-between gap-3">
                      <span className="text-on-surface-variant text-[10px] leading-tight">
                        Rebuild this backend's{' '}
                        {clients.length > 1 ? `${clients.length} connections` : 'connection'} to it
                      </span>
                      <button
                        onClick={() => reconnect.mutate(clients)}
                        disabled={reconnect.isPending}
                        className="text-primary border-primary/40 hover:bg-primary/10 shrink-0 cursor-pointer rounded border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider transition-colors disabled:opacity-50"
                      >
                        {reconnect.isPending ? 'Working…' : 'Reconnect'}
                      </button>
                    </div>
                  )}
                  {container && (
                    <CopyableCommand command={`podman restart ${container}`} onResult={show} />
                  )}
                </div>
              </div>
            )
          })
        )}
      </div>

      <WorkInFlight onResult={show} />

      <div className="glass-panel rounded-xl p-8">
        <h2 className="font-headline text-lg font-bold uppercase tracking-wide">
          Restarting a service
        </h2>
        <p className="text-on-surface-variant mt-1 text-xs leading-relaxed">
          The dashboard cannot restart the stack, and is not given the means to: it is a container
          beside the others, reachable from a browser, so control over its neighbours is exactly what
          it should not have. Each card above therefore carries the command rather than a button.
          <span className="mt-2 block">
            Reconnecting is usually the one you want, and costs no downtime: it is what clears a
            session that has gone stale while the service itself is fine. Restart the backend instead
            when you want every connection rebuilt at once, or to release a comparison that will not
            stop.
          </span>
        </p>
        <div className="mt-4 grid grid-cols-1 gap-2 md:grid-cols-2">
          <CopyableCommand command="podman restart backend" onResult={show} />
          <CopyableCommand command="scripts/cleanup-data.sh" onResult={show} />
          <CopyableCommand command="podman exec cassandra nodetool clearsnapshot --all" onResult={show} />
          <CopyableCommand command="./stop-and-clean-data-and-schema.sh" onResult={show} />
        </div>
        <p className="text-on-surface-variant mt-3 text-[10px] leading-relaxed opacity-70">
          <span className="font-mono">cleanup-data.sh</span> truncates the generated tables and leaves
          the stack up;{' '}
          <span className="font-mono">clearsnapshot</span> reclaims snapshots an abandoned bulk read
          left pinning SSTables, which otherwise expire on their own;{' '}
          <span className="font-mono">stop-and-clean-data-and-schema.sh</span> is the full wipe,
          including the Kafka volume.
        </p>
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

      {toast && <Toast toast={toast} onDismiss={dismiss} />}
    </section>
  )
}
