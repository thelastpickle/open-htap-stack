import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import Toast, { useToast } from '../components/Toast'
import { formatMs, getJson, postJson } from '../lib/api'

interface QueryResult {
  columns: string[]
  rows: unknown[][]
  row_count: number
  query_time_ms: number
  sql?: string | null
}

interface EngineResult extends QueryResult {
  available: boolean
  error: string | null
}

type BenchmarkResponse = Record<Engine, EngineResult>

interface VectorHit {
  entity_id: string
  text_payload?: string | null
  similarity?: number | null
  is_flying?: boolean
  altitude_m?: number | null
  latitude?: number | null
  longitude?: number | null
}

interface VectorResponse {
  results: VectorHit[]
  query_time_ms: number
}

type Tab = 'sql' | 'ai' | 'compare'
type Engine = 'cassandra' | 'presto' | 'spark' | 'spark_bulk'

const TABS: { key: Tab; label: string; icon: string }[] = [
  { key: 'sql', label: 'SQL console', icon: 'terminal' },
  { key: 'ai', label: 'Vector search', icon: 'neurology' },
  { key: 'compare', label: 'Compare engines', icon: 'compare_arrows' },
]

/** What each engine is for, and how it reaches the data. */
const ENGINES: { key: Engine; label: string; role: string; colour: string }[] = [
  {
    key: 'cassandra',
    label: 'Cassandra',
    role: 'Transactional · CQL request path. A point read costs one partition; no joins, no ordering on arbitrary columns, no grouping.',
    colour: 'var(--color-primary)',
  },
  {
    key: 'presto',
    label: 'Presto',
    role: 'Analytical · full SQL, distributed scan over the same rows through the CQL request path.',
    colour: 'var(--color-secondary)',
  },
  {
    key: 'spark',
    label: 'Spark SQL',
    role: 'Batch · SparkSQL through the spark-cassandra-connector, also over the CQL request path.',
    colour: 'var(--color-accent)',
  },
  {
    key: 'spark_bulk',
    label: 'Spark bulk reader',
    role: 'Batch · SparkSQL reading SSTable files directly from a coordinated snapshot via the Sidecar. Never touches the request path, so it cannot contend with OLTP latency. Rows are consistent as of the snapshot.',
    colour: 'var(--color-positive)',
  },
]

/**
 * Two queries, because one query cannot show what the engines are for.  The
 * bounded read is where the transactional path wins; the fleet-wide aggregate is
 * where it cannot compete, and where reading SSTables directly pays off.
 */
const COMPARE_PRESETS = [
  {
    key: 'latest',
    label: 'Latest state',
    hint: 'One bounded read of the current fleet — the shape Cassandra is built for',
    sql: 'SELECT entity_id, speed_mps, altitude_m, risk_score\nFROM drone_latest_status\nWHERE is_flying = true\nLIMIT 10',
  },
  {
    key: 'aggregate',
    label: 'Fleet-wide aggregate',
    hint: 'Every event ever ingested, grouped — CQL cannot express this at all. Takes minutes.',
    sql:
      'SELECT event_type, count(*) AS event_count,\n' +
      '       min(temp_internal_c) AS coldest, max(temp_internal_c) AS hottest\n' +
      'FROM events\n' +
      'GROUP BY event_type\n' +
      'ORDER BY event_type\n' +
      'LIMIT 5',
  },
] as const

const DEFAULT_SQL = 'SELECT entity_id, speed_mps, altitude_m, risk_score\nFROM drone_latest_status\nLIMIT 10'

const VECTOR_EXAMPLES = [
  'sovereign wealth fund',
  'quantum computing breakthroughs',
  'Norse seafarers of the Viking age',
  'hydropower and renewable energy',
  'behavioural economics',
  'genome sequencing',
]

function ResultTable({ result }: { result: QueryResult }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-left">
        <thead>
          <tr className="border-b border-white/10 bg-black/20">
            {result.columns.map((column) => (
              <th
                key={column}
                className="text-primary px-6 py-4 text-[10px] font-black uppercase tracking-wider"
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {result.rows.map((row, rowIndex) => (
            <tr key={rowIndex} className="hover:bg-white/[0.04]">
              {row.map((value, columnIndex) => (
                <td
                  key={columnIndex}
                  className="max-w-md px-6 py-3 text-xs font-medium tabular-nums"
                >
                  {typeof value === 'boolean' ? (
                    <span className={value ? 'text-primary' : 'text-on-surface-variant'}>
                      {value ? 'yes' : 'no'}
                    </span>
                  ) : typeof value === 'number' ? (
                    Number.isInteger(value) ? value : value.toFixed(3)
                  ) : (
                    String(value ?? '—')
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EngineCard({
  label,
  role,
  colour,
  result,
}: {
  label: string
  role: string
  colour: string
  result: EngineResult | undefined
}) {
  if (!result) return null
  const succeeded = result.available && !result.error

  return (
    <div className="glass-panel overflow-hidden rounded-xl">
      <div className="flex items-start justify-between gap-4 border-b border-white/5 px-6 py-4">
        <div>
          <p className="text-[10px] font-black uppercase tracking-wider" style={{ color: colour }}>
            {label}
          </p>
          <p className="text-on-surface-variant mt-0.5 text-[9px] leading-relaxed">{role}</p>
        </div>
        {succeeded && (
          <div className="shrink-0 text-right">
            <p className="font-headline text-2xl font-black tabular-nums" style={{ color: colour }}>
              {formatMs(result.query_time_ms)}
            </p>
            <p className="text-on-surface-variant text-[9px]">{result.row_count} rows</p>
          </div>
        )}
      </div>

      {result.sql && (
        <pre className="text-on-surface-variant/70 overflow-x-auto border-b border-white/5 px-6 py-3 font-mono text-[10px] leading-relaxed">
          {result.sql}
        </pre>
      )}

      {!result.available ? (
        <div className="text-secondary flex items-start gap-3 p-6">
          <MaterialIcon name="info" className="shrink-0 text-[18px]" />
          <div>
            <p className="text-xs font-bold uppercase tracking-wide">Engine not reachable</p>
            <p className="text-on-surface-variant mt-1 text-[10px] leading-relaxed">
              {result.error ?? 'Start the service and run the comparison again.'}
            </p>
          </div>
        </div>
      ) : result.error ? (
        <div className="text-tertiary flex items-start gap-3 p-6">
          <MaterialIcon name="error" className="shrink-0 text-[18px]" />
          <p className="break-words font-mono text-[11px] leading-relaxed">{result.error}</p>
        </div>
      ) : result.rows.length === 0 ? (
        <p className="text-on-surface-variant p-6 text-[10px] italic">The query returned no rows.</p>
      ) : (
        <div className="max-h-64 overflow-y-auto">
          <ResultTable result={result} />
        </div>
      )}
    </div>
  )
}

function ComparePanel() {
  const [preset, setPreset] = useState<string>(COMPARE_PRESETS[0].key)
  const [sql, setSql] = useState<string>(COMPARE_PRESETS[0].sql)
  const [error, setError] = useState<string | null>(null)
  const [results, setResults] = useState<BenchmarkResponse | null>(null)

  const run = useMutation({
    mutationFn: () => postJson<BenchmarkResponse>('/api/query/benchmark', { sql, limit: 10 }),
    onSuccess: (data) => {
      setResults(data)
      setError(null)
    },
    onError: (e: Error) => {
      setError(e.message)
      setResults(null)
    },
  })

  const timings = ENGINES.map((engine) => {
    const result = results?.[engine.key]
    return {
      ...engine,
      ms: result && result.available && !result.error ? result.query_time_ms : null,
      failed: Boolean(result?.available && result.error),
    }
  })
  const slowest = Math.max(...timings.map((t) => t.ms ?? 0), 1)

  return (
    <div className="space-y-6">
      <div className="glass-panel overflow-hidden rounded-xl">
        <div className="flex flex-wrap items-center gap-4 border-b border-white/5 bg-surface-container-high px-6 py-3">
          <span className="text-primary text-[10px] font-black uppercase tracking-widest">
            One query, four access paths
          </span>
          <span className="text-on-surface-variant text-[10px] uppercase tracking-wider opacity-60">
            Each engine gets the statement in its own dialect; the rewrite is shown with its result
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-2 border-b border-white/5 px-6 py-3">
          {COMPARE_PRESETS.map((option) => (
            <button
              key={option.key}
              onClick={() => {
                setPreset(option.key)
                setSql(option.sql)
              }}
              title={option.hint}
              className={`rounded px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider transition-colors ${
                preset === option.key
                  ? 'bg-primary text-on-primary'
                  : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
              }`}
            >
              {option.label}
            </button>
          ))}
          <span className="text-on-surface-variant/60 ml-2 text-[10px]">
            {COMPARE_PRESETS.find((option) => option.key === preset)?.hint}
          </span>
        </div>

        <label htmlFor="compare-sql" className="sr-only">
          SQL to compare
        </label>
        <textarea
          id="compare-sql"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          className="text-primary h-40 w-full resize-none bg-transparent p-6 font-mono text-sm leading-relaxed focus:outline-none"
          spellCheck={false}
        />
      </div>

      <button
        onClick={() => run.mutate()}
        disabled={run.isPending}
        className="font-headline flex w-full cursor-pointer items-center justify-center gap-3 rounded border border-white/10 bg-surface-container px-8 py-3 font-bold tracking-wider transition-all hover:bg-surface-container-high active:scale-95 disabled:opacity-60"
      >
        <MaterialIcon
          name={run.isPending ? 'sync' : 'compare_arrows'}
          className={run.isPending ? 'animate-spin' : ''}
        />
        {run.isPending ? 'Running on all four paths…' : 'Run on all four paths'}
      </button>

      {error && (
        <div className="border-tertiary/30 bg-tertiary/10 text-tertiary flex items-start gap-3 rounded-lg border p-4">
          <MaterialIcon name="error" className="shrink-0 text-[18px]" />
          <p className="text-xs font-bold">{error}</p>
        </div>
      )}

      {results && (
        <>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
            {ENGINES.map((engine) => (
              <EngineCard
                key={engine.key}
                label={engine.label}
                role={engine.role}
                colour={engine.colour}
                result={results[engine.key]}
              />
            ))}
          </div>

          <div className="glass-panel space-y-4 rounded-xl border border-white/5 p-6">
            <p className="text-on-surface-variant text-[10px] font-bold uppercase tracking-wider">
              Time to answer
            </p>
            {timings.map((timing) => (
              <div key={timing.key} className="space-y-1">
                <div className="flex justify-between text-[10px] font-bold">
                  <span style={{ color: timing.colour }}>{timing.label}</span>
                  <span className="text-on-surface-variant tabular-nums">
                    {timing.ms != null
                      ? formatMs(timing.ms)
                      : timing.failed
                        ? 'cannot answer this query'
                        : 'unavailable'}
                  </span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-white/5">
                  <div
                    className="h-full rounded-full transition-all duration-700"
                    style={{
                      width: `${timing.ms != null ? Math.max(2, (timing.ms / slowest) * 100) : 0}%`,
                      background: timing.colour,
                    }}
                  />
                </div>
              </div>
            ))}
            <div className="text-on-surface-variant space-y-3 border-t border-white/5 pt-4 text-xs leading-relaxed">
              <p>
                All four paths read the same Cassandra rows, with nothing copied between them. Three
                of them go through the CQL request path and share it with the live ingest; the bulk
                reader goes to the SSTable files instead, from a coordinated snapshot taken through
                the Sidecar, so its scan cannot contend with OLTP latency. That property holds by
                construction, not by tuning, and it is what the timings here are really about.
              </p>
              <p>
                An engine reporting an error has not failed: it has told you the query is not for it.
                CQL groups only by primary-key columns, which is exactly why the analytical paths
                exist. Change the query and the ordering changes with it — a single-partition lookup
                and a fleet-wide aggregate do not favour the same engine, and no single number
                decides between them.
              </p>
              <p>
                On the aggregate, watch the counts rather than only the clock. The two paths that read
                through Cassandra see the table grow underneath them while they scan, so their totals
                differ; the bulk reader answers from one snapshot, so its groups are consistent with
                each other. That is point-in-time consistency, and it is the other half of what
                reading SSTables directly buys.
              </p>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

export default function ExplorePage() {
  const { toast, show, dismiss } = useToast()
  const [tab, setTab] = useState<Tab>('sql')
  const [engine, setEngine] = useState<Engine>('cassandra')
  const [sql, setSql] = useState(DEFAULT_SQL)
  const [question, setQuestion] = useState('hydropower and renewable energy')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [hits, setHits] = useState<VectorHit[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  const { data: engineStatus } = useQuery<{ engines: Record<string, boolean> }>({
    queryKey: ['engines'],
    queryFn: () => getJson<{ engines: Record<string, boolean> }>('/api/query/engines'),
    refetchInterval: 15000,
  })

  const runSql = useMutation({
    mutationFn: () => postJson<QueryResult>('/api/query/sql', { sql, limit: 10, engine }),
    onSuccess: (data) => {
      setResult(data)
      setError(null)
      show(`${data.row_count} rows in ${formatMs(data.query_time_ms)}`)
    },
    onError: (e: Error) => {
      setError(e.message)
      setResult(null)
    },
  })

  const search = useMutation({
    mutationFn: () => postJson<VectorResponse>('/api/vector/search', { query: question, limit: 6 }),
    onSuccess: (data) => {
      setHits(data.results)
      setError(null)
      show(`${data.results.length} matches in ${formatMs(data.query_time_ms)}`, 'info')
    },
    onError: (e: Error) => {
      setError(e.message)
      setHits(null)
    },
  })

  const buildIndex = useMutation({
    mutationFn: () => postJson<{ message: string; embedder: string }>('/api/vector/index-all'),
    onSuccess: (data) =>
      show(`${data.message} (${data.embedder} embedder)`, 'info'),
    onError: (e: Error) => show(e.message, 'error'),
  })

  const busy = runSql.isPending || search.isPending

  return (
    <section className="space-y-8">
      {toast && <Toast toast={toast} onDismiss={dismiss} />}

      <div>
        <div className="mb-2 flex items-center gap-3">
          <span className="bg-primary h-2 w-2 animate-pulse rounded-full" />
          <span className="font-label text-primary-dim text-[0.6875rem] font-bold uppercase tracking-widest">
            Query the live data
          </span>
        </div>
        <h1 className="font-headline text-4xl font-black uppercase tracking-tighter">Explore</h1>
      </div>

      <div className="flex w-fit flex-wrap gap-1 rounded-xl border border-white/5 bg-surface-container p-1">
        {TABS.map((item) => (
          <button
            key={item.key}
            onClick={() => {
              setTab(item.key)
              setError(null)
            }}
            className={`flex items-center gap-2 rounded-lg px-6 py-2 text-xs font-black uppercase tracking-wider transition-colors ${
              tab === item.key
                ? 'bg-surface-container-highest text-primary'
                : 'text-on-surface-variant hover:text-on-surface'
            }`}
          >
            <MaterialIcon name={item.icon} />
            {item.label}
          </button>
        ))}
      </div>

      {tab === 'compare' ? (
        <ComparePanel />
      ) : (
        <>
          <div className="grid grid-cols-1 gap-8 lg:grid-cols-4">
            <div className="space-y-4 lg:col-span-3">
              <div className="glass-panel overflow-hidden rounded-xl border border-white/5">
                <div className="flex flex-wrap items-center justify-between gap-4 border-b border-white/5 bg-surface-container-high px-6 py-3">
                  <div className="flex flex-wrap items-center gap-4">
                    <span className="text-primary text-[10px] font-black uppercase tracking-widest">
                      {tab === 'sql' ? 'SQL console' : 'Semantic search'}
                    </span>
                    {tab === 'sql' && (
                      <div className="flex rounded bg-black/40 p-0.5">
                        {ENGINES.map((option) => {
                          const reachable = engineStatus?.engines?.[option.key] !== false
                          return (
                            <button
                              key={option.key}
                              onClick={() => setEngine(option.key)}
                              title={
                                reachable
                                  ? option.role
                                  : `${option.label} is not currently reachable`
                              }
                              aria-pressed={engine === option.key}
                              className={`rounded px-3 py-1 text-[9px] font-black uppercase tracking-wider transition-colors ${
                                engine === option.key
                                  ? 'bg-primary text-on-primary'
                                  : 'text-on-surface-variant hover:text-primary'
                              } ${reachable ? '' : 'opacity-40'}`}
                            >
                              {option.label}
                            </button>
                          )
                        })}
                      </div>
                    )}
                  </div>
                  {tab === 'ai' && (
                    <button
                      onClick={() => buildIndex.mutate()}
                      disabled={buildIndex.isPending}
                      className="bg-secondary/10 border-secondary/30 text-secondary hover:bg-secondary/20 flex items-center gap-2 rounded border px-4 py-1.5 text-[10px] font-black uppercase tracking-wider transition-colors"
                    >
                      <MaterialIcon
                        name="refresh"
                        className={buildIndex.isPending ? 'animate-spin text-[14px]' : 'text-[14px]'}
                      />
                      Build embeddings
                    </button>
                  )}
                </div>

                {tab === 'sql' ? (
                  <>
                    <label htmlFor="console-sql" className="sr-only">
                      SQL to run
                    </label>
                    <textarea
                      id="console-sql"
                      value={sql}
                      onChange={(e) => setSql(e.target.value)}
                      className="text-primary h-60 w-full resize-none bg-transparent p-6 font-mono text-sm leading-relaxed focus:outline-none"
                      spellCheck={false}
                    />
                  </>
                ) : (
                  <div className="space-y-4 p-10 text-center">
                    <label htmlFor="semantic-query" className="sr-only">
                      What to search for
                    </label>
                    <input
                      id="semantic-query"
                      type="text"
                      value={question}
                      onChange={(e) => setQuestion(e.target.value)}
                      onKeyDown={(e) => e.key === 'Enter' && search.mutate()}
                      placeholder="Describe what you are looking for…"
                      className="focus:border-secondary/50 w-full rounded-full border border-white/10 bg-surface-container px-8 py-5 text-lg focus:outline-none"
                    />
                    <p className="text-on-surface-variant mx-auto max-w-lg text-[11px] leading-relaxed">
                      Every asset carries a snippet of prose on some unrelated subject. Both the
                      snippet and this query are embedded, and Cassandra’s SAI index returns the
                      nearest by cosine similarity. This is text similarity, not geography.
                    </p>
                    <div className="flex flex-wrap justify-center gap-2">
                      {VECTOR_EXAMPLES.map((example) => (
                        <button
                          key={example}
                          onClick={() => setQuestion(example)}
                          className="border-secondary/20 text-secondary/70 hover:border-secondary/60 hover:text-secondary rounded-full border px-3 py-1 text-[9px] font-bold uppercase tracking-wider transition-colors"
                        >
                          {example}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <button
                onClick={() => (tab === 'sql' ? runSql.mutate() : search.mutate())}
                disabled={busy}
                className={`font-headline text-on-primary flex cursor-pointer items-center gap-2 rounded px-8 py-2.5 font-bold tracking-wider transition-all active:scale-95 disabled:opacity-50 ${
                  tab === 'sql' ? 'bg-primary hover:bg-primary-dim' : 'bg-secondary hover:brightness-110'
                }`}
              >
                <MaterialIcon
                  name={busy ? 'sync' : 'play_arrow'}
                  className={busy ? 'animate-spin' : ''}
                />
                {busy ? 'Running…' : 'Run'}
              </button>
            </div>

            <aside className="space-y-6">
              <div className="glass-panel border-l-primary rounded-xl border-l-4 p-6">
                <h2 className="mb-4 text-sm font-black uppercase tracking-wider">
                  {tab === 'sql' ? 'Tables' : 'How it works'}
                </h2>
                {tab === 'sql' ? (
                  <div className="space-y-4 text-[10px] leading-relaxed">
                    <div>
                      <p className="text-primary font-bold">drone_latest_status</p>
                      <p className="text-on-surface-variant">
                        One row per asset: entity_id, speed_mps, altitude_m, risk_score, is_flying,
                        latitude, longitude, temp_internal_c
                      </p>
                    </div>
                    <div>
                      <p className="text-primary font-bold">drone_events_by_entity</p>
                      <p className="text-on-surface-variant">
                        Per-asset history, clustered by event_time descending
                      </p>
                    </div>
                    <div>
                      <p className="text-primary font-bold">events</p>
                      <p className="text-on-surface-variant">
                        Every event ingested, one row each — millions of them. The table to aim an
                        analytical path at.
                      </p>
                    </div>
                    <p className="text-on-surface-variant/70 border-t border-white/5 pt-3">
                      Write the query unqualified; the backend adds what each engine needs — the demo
                      schema for Presto, LIMIT and ALLOW FILTERING for CQL. Only SELECT is accepted.
                    </p>
                  </div>
                ) : (
                  <p className="text-on-surface-variant text-[10px] leading-relaxed">
                    Build embeddings once to populate payload_vector for every asset that has text.
                    Searching then embeds your phrase and asks Cassandra for the nearest neighbours,
                    scoring each with similarity_cosine. Without an embedding API key the backend
                    hashes tokens locally, so matching is lexical rather than semantic — but it is
                    real, ranked and reproducible.
                  </p>
                )}
              </div>
            </aside>
          </div>

          {error && (
            <div className="border-tertiary/30 bg-tertiary/10 text-tertiary flex items-start gap-3 rounded-lg border p-4">
              <MaterialIcon name="error" className="shrink-0 text-[18px]" />
              <p className="text-xs font-bold leading-relaxed">{error}</p>
            </div>
          )}

          <div className="glass-panel overflow-hidden rounded-xl border border-white/5">
            {tab === 'sql' &&
              (result ? (
                <>
                  {result.sql && (
                    <pre className="text-on-surface-variant/70 overflow-x-auto border-b border-white/5 px-6 py-3 font-mono text-[10px]">
                      {result.sql}
                    </pre>
                  )}
                  {result.rows.length > 0 ? (
                    <ResultTable result={result} />
                  ) : (
                    <p className="text-on-surface-variant p-12 text-center text-xs italic">
                      The query returned no rows.
                    </p>
                  )}
                </>
              ) : (
                !error && (
                  <p className="text-on-surface-variant/50 p-16 text-center text-sm italic">
                    Run a query to see results.
                  </p>
                )
              ))}

            {tab === 'ai' &&
              (hits && hits.length > 0 ? (
                <div className="grid grid-cols-1 gap-4 p-6 md:grid-cols-2">
                  {hits.map((hit) => (
                    <div
                      key={hit.entity_id}
                      className="border-secondary/20 space-y-2 rounded-lg border bg-surface-container-high p-4"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <span className="font-headline text-secondary text-sm font-bold uppercase">
                          {hit.entity_id}
                        </span>
                        {hit.similarity != null && (
                          <span className="bg-secondary/15 text-secondary rounded px-2 py-0.5 text-[10px] font-bold tabular-nums">
                            {(hit.similarity * 100).toFixed(1)}% similar
                          </span>
                        )}
                      </div>
                      {hit.text_payload && (
                        <p className="text-on-surface-variant text-[11px] leading-relaxed">
                          {hit.text_payload}
                        </p>
                      )}
                      <div className="flex flex-wrap gap-2 text-[9px] font-bold uppercase tracking-wider">
                        <span
                          className={`rounded px-2 py-0.5 ${hit.is_flying ? 'bg-primary/10 text-primary' : 'text-on-surface-variant bg-white/5'}`}
                        >
                          {hit.is_flying ? 'Flying' : 'Grounded'}
                        </span>
                        {hit.altitude_m != null && (
                          <span className="text-on-surface-variant rounded bg-white/5 px-2 py-0.5">
                            {hit.altitude_m.toFixed(0)} m
                          </span>
                        )}
                        {hit.latitude != null && hit.longitude != null && (
                          <span className="text-on-surface-variant rounded bg-white/5 px-2 py-0.5">
                            {hit.latitude.toFixed(3)}, {hit.longitude.toFixed(3)}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                !error && (
                  <p className="text-on-surface-variant/50 p-16 text-center text-sm italic">
                    {hits ? 'No matches for that phrase.' : 'Search to see the nearest snippets.'}
                  </p>
                )
              ))}
          </div>
        </>
      )}
    </section>
  )
}
