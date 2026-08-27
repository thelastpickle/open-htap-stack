import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import Toast, { useToast } from '../components/Toast'
import { formatBytes, formatMs, getJson, postJson, postNdjson } from '../lib/api'

interface QueryResult {
  columns: string[]
  rows: unknown[][]
  row_count: number
  query_time_ms: number
  sql?: string | null
}

/** What a single-partition read cost while this engine was working. */
interface OltpImpact {
  p50_ms: number
  p95_ms: number
  max_ms: number
  samples: number
  failures: number
}

interface EngineResult extends QueryResult {
  available: boolean
  error: string | null
  oltp?: OltpImpact | null
  /** Bulk reader only: the size of the snapshot it read, so growth is visible. */
  snapshot_bytes?: number | null
  /** Bulk reader only: what preparing that snapshot cost, in milliseconds. */
  snapshot_ms?: number | null
  /** Bulk reader only: whether it re-read the last snapshot instead of taking one. */
  snapshot_reused?: boolean
  /** Bulk reader only: the snapshot's age, which is the age of these rows. */
  snapshot_age_s?: number | null
  /** cqlite only: live SSTable files this statement merged. */
  sstable_files?: number | null
  /** cqlite only: their total size, so growth is visible as it is for a snapshot. */
  sstable_bytes?: number | null
  /** cqlite only: what listing the directory and opening those files cost. */
  reader_open_ms?: number | null
  /** cqlite only: seconds since the newest file was written, so the age of these rows. */
  data_age_s?: number | null
}

/** A path the request did not ask for is absent, so the keys are partial. */
type BenchmarkResponse = Partial<Record<Engine, EngineResult>> & {
  mode?: RunMode
  oltp_baseline?: OltpImpact | null
  /** Parallel runs only: one sample over the whole window, not per path. */
  oltp_combined?: OltpImpact | null
}

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

/**
 * The live embedder's state, as the backend reports it.  `pending` is what the last
 * pass deferred and `behind_s` is how long ago that pass ran, so the two together
 * say whether the index is following the writes or falling behind them.
 */
interface LiveEmbedding {
  enabled: boolean
  embedder: string
  interval_s: number
  embedded: number
  failed: number
  passes: number
  last_embedded: number
  last_pass_ms: number
  pending: number
  behind_s?: number | null
  tracked: number
  error?: string | null
}

type Tab = 'sql' | 'ai' | 'compare'
type Engine = 'cassandra' | 'presto' | 'spark' | 'spark_bulk' | 'cqlite'
type RunMode = 'sequential' | 'parallel'

/**
 * The two ways to run a comparison, and what each is for.  Running one at a time
 * is the only way a timing means what it looks like; running them together is how
 * to see what they cost each other.  Neither is the honest one — they answer
 * different questions, and the answers are not comparable, so the mode is shown
 * with the results.
 */
const RUN_MODES: { key: RunMode; label: string; hint: string }[] = [
  {
    key: 'sequential',
    label: 'One at a time',
    hint: 'Each path is timed alone, so its figure is its own cost and the point read beside it is the price that path alone charged',
  },
  {
    key: 'parallel',
    label: 'All at once',
    hint: 'The paths contend deliberately: every figure inflates, and the point read is one measurement for the whole window because the cost belongs to all of them',
  },
]

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
  {
    key: 'cqlite',
    label: 'cqlite SQL',
    role: 'Analytical · DataFusion over the live SSTable files, parsed by cqlite in this process. No snapshot, no Sidecar and no JVM, so it cannot contend with OLTP latency. Rows are as of the last flush.',
    colour: 'var(--color-pink)',
  },
]

/**
 * Which window to ask about, as the backend reports it.  Read from the stack rather
 * than computed here, because the answer depends on the data: a bucket exists only
 * because the sink wrote it, so the stack's clock, configuration and contents decide
 * which one is worth naming.  `closed` is false when nothing has closed since ingest
 * began and the window on offer is the one still filling — which changes what the
 * comparison can claim, so the page says so.
 *
 * `settled` is the stronger flag and the one the exactness claim rests on: the sink
 * files an event under the event's own timestamp, so a sink behind the topic keeps
 * writing into windows the clock has already passed.  A closed window is therefore
 * not necessarily a finished one, and only a finished one obliges the paths to agree.
 */
interface EventWindow {
  bucket_minutes: number
  shards: number
  current: string
  bucket: string
  closed: boolean
  settled: boolean
}

/** `0,1,2,…,n-1`, the shards of one window, for an IN list. */
function shardList(shards: number): string {
  return Array.from({ length: Math.max(1, shards) }, (_, i) => i).join(',')
}

/**
 * The order the paths are run in when they are run one at a time: quickest first,
 * so that the first box appears in milliseconds rather than after the slowest path
 * has finished.  It is fixed per question rather than computed, because the only
 * way to know a path's time is to have run it, and the point of the ordering is to
 * decide what to run first.
 *
 * Each ranking is a prediction and none of them is a measurement of the run on
 * screen: the bars underneath are, so a run that contradicts its order shows it.
 * Each was swept on this stack over repeated sequential runs of its own preset, and
 * two conditions decide what the figures mean.  A first run after a restart charges
 * Presto and the Thrift Server a warm-up that no later run pays, so the order
 * follows the warm run.  And the sink draining a Kafka backlog at 3,000 rows a
 * second takes the CPU that the JVM paths want, which is enough to reorder the two
 * slowest on one window; every figure below is from a drained sink.
 *
 * Cassandra is first in all four, because it either answers from one partition in
 * milliseconds or declines the grouping outright, and both take no time at all.
 * cqlite moves from second to fifth as the question grows, which is the shape of the
 * whole sweep: on `drone_latest_status` it opens one small file set and answers in
 * 54 to 153 ms, on one window it seeks 16 named partitions in 13.2 s, and on the
 * whole history it walks 957 MB of data files in 100.0 s.  The bulk reader moves the
 * other way, last on the two small questions because a snapshot costs 1.1 to 3.6 s
 * whatever it holds, and fourth on the history at 23.5 s.  Spark's own overhead puts
 * it behind Presto in all four; on the bounded read alone the two are close enough to
 * cross between runs, 184 to 324 ms for Presto against 244 to 480 ms for Spark, and
 * Presto keeps the earlier slot because it is the one that wins when both are cold.
 *
 * The two small questions rank strictly and repeatably.  The window does not: its
 * last three sit in one band and their order moves with the sink and with the size of
 * the window, which the preset's own comment records.  So read a contradiction there
 * as the ordering being wrong and not the run.
 */
const DEFAULT_RUN_ORDER: Engine[] = ['cassandra', 'cqlite', 'presto', 'spark', 'spark_bulk']

/**
 * Four queries of deliberately different size, because one query cannot show
 * what five access paths are for, and because the size is most of the answer.
 * The bounded read is where the transactional path wins; grouping the fleet is
 * the smallest question CQL cannot express at all; one window is the same grouping
 * bounded to the partitions that hold it, which is what the data model is shaped
 * for; and the full history is the one where reading SSTables directly pays off,
 * opt-in because on a single node it is measured in minutes rather than seconds.
 */
const COMPARE_PRESETS = [
  {
    key: 'latest',
    label: 'Latest state',
    cost: 'milliseconds',
    hint: 'One bounded read of the current fleet — the shape Cassandra is built for',
    // Warm: cassandra 8.4 ms, cqlite 74.1 ms, presto 184.1 ms, spark 253.4 ms,
    // the bulk reader 727.3 ms, most of the last being the snapshot.
    order: ['cassandra', 'cqlite', 'presto', 'spark', 'spark_bulk'],
    sql: 'SELECT entity_id, speed_mps, altitude_m, risk_score\nFROM drone_latest_status\nWHERE is_flying = true\nLIMIT 10',
  },
  {
    key: 'group',
    label: 'Group the fleet',
    cost: 'under a second',
    hint: 'The current fleet, grouped — the smallest question CQL cannot express at all',
    // Warm: cassandra declines in 4.7 ms, cqlite 68.7 ms, presto 264.4 ms,
    // spark 467.7 ms, the bulk reader 1.86 s.
    order: ['cassandra', 'cqlite', 'presto', 'spark', 'spark_bulk'],
    sql:
      'SELECT event_type, count(*) AS assets,\n' +
      '       min(temp_internal_c) AS coldest, max(temp_internal_c) AS hottest\n' +
      'FROM drone_latest_status\n' +
      'GROUP BY event_type\n' +
      'ORDER BY event_type\n' +
      'LIMIT 5',
  },
  {
    key: 'window',
    label: 'One window',
    cost: 'seconds',
    hint: 'The same grouping, bounded to the partitions holding one window — the question the data model was shaped for',
    // Two things about this question are settled, and one is not.  Cassandra declines
    // it in 3.0 to 13.2 ms, and Presto is decisively the quickest that can express it,
    // 2.20 to 4.06 s against 12 to 25 s for the other three.  cqlite seeks the 16
    // named partitions rather than walking the table, which is what moves it from
    // second on the two small questions to last on this one, 13.2 to 24.8 s.
    //
    // The order of the last three is *not* settled, and this ordering is the middle of
    // what was measured rather than a repeatable ranking.  Over four windows of 920 MB
    // to 1,161 MB: spark 7.23, 15.15, 15.84 and 16.80 s; the bulk reader 7.88, 9.65,
    // 11.85, 13.95 and 15.80 s plus a 1.1 to 3.6 s snapshot; cqlite 13.24, 13.26 and
    // 24.80 s.  Two conditions move them.  Draining a Kafka backlog at 3,000 rows a
    // second takes the CPU the two JVM paths want, and while it drained the bulk
    // reader read one window in 14.7 and 16.0 s against cqlite's 10.9 and 11.6 s,
    // which two later pairs on an idle sink reversed.  And the window itself grows,
    // so no two of these figures are over the same rows.
    order: ['cassandra', 'presto', 'spark', 'spark_bulk', 'cqlite'],
    // Built rather than fixed: the window has to be a bucket the sink actually
    // wrote, and which one that is changes every quarter of an hour.
    build: (window: EventWindow) =>
      'SELECT event_type, count(*) AS event_count,\n' +
      '       min(temp_internal_c) AS coldest, max(temp_internal_c) AS hottest\n' +
      'FROM events\n' +
      "WHERE event_bucket = '" + window.bucket + "'\n" +
      '  AND shard IN (' + shardList(window.shards) + ')\n' +
      'GROUP BY event_type\n' +
      'ORDER BY event_type\n' +
      'LIMIT 5',
  },
  {
    key: 'history',
    label: 'Every event ever ingested',
    cost: 'minutes',
    hint: 'The whole history, scanned on one node while it ingests — so it costs more every hour the demo runs',
    // On a drained sink over 957 MB of data files: cassandra declines in 3.6 ms,
    // presto 7.38 s, spark 10.38 s, the bulk reader 23.51 s plus a 2.45 s snapshot,
    // cqlite 100.0 s.  A run while the sink drained a backlog gave the same order at
    // 6.18, 11.01, 27.53 and 69.26 s over a third of the files, so the ranking is the
    // measurement here and the figures are not: the table grows under every run of it.
    order: ['cassandra', 'presto', 'spark', 'spark_bulk', 'cqlite'],
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

/**
 * Below this many samples the window was too short to describe with percentiles,
 * and quoting a p95 of one reading would be worse than quoting nothing.  A query
 * that finishes inside a few probe intervals has not had time to disturb
 * anything, which is itself the answer.
 */
const MIN_PROBE_SAMPLES = 4

/**
 * A path that cannot express a query says so at once, so a failure quicker than
 * this is a refusal rather than a breakage.  Anything slower ran and then broke,
 * which the comparison should report as what it is.
 */
const REFUSAL_MS = 1000

/**
 * Below this much data, a bulk read's clock is mostly the snapshot and the Spark
 * job starting, so the throughput it implies describes the overhead rather than the
 * mechanism.  The volume is still worth showing; the rate is not.
 */
const RATE_WORTH_QUOTING_BYTES = 10_000_000

/**
 * Whether a result's statement read everything available to it, judged by whether
 * it restricts anything.  A statement naming partitions reads only those, so
 * dividing the snapshot's size by its duration would describe a scan that never
 * happened.
 */
function scannedItAll(result: EngineResult): boolean {
  return !/\bWHERE\b/i.test(result.sql ?? '')
}

/**
 * The reference read is taken immediately before the run, which is not the same
 * as taking it on a quiet machine: the ingest never stops, the dashboard polls,
 * and a comparison finished moments ago may still be releasing Spark executors
 * and snapshots.  A tail this far above the median means the reference caught
 * some of that, and everything measured against it is correspondingly generous.
 */
const UNSETTLED_BASELINE_RATIO = 5

function baselineUnsettled(baseline?: OltpImpact | null): boolean {
  return Boolean(
    baseline?.samples &&
      baseline.samples >= MIN_PROBE_SAMPLES &&
      baseline.p50_ms > 0 &&
      baseline.p95_ms / baseline.p50_ms > UNSETTLED_BASELINE_RATIO,
  )
}

/**
 * The price this path charged the transactional one, next to the baseline it was
 * measured against.  A path that never touches the CQL request path should sit on
 * its baseline; one that scans through it should not.
 */
function OltpFooter({ oltp, baseline }: { oltp?: OltpImpact | null; baseline?: OltpImpact | null }) {
  if (!oltp?.samples) return null
  const worse = baseline?.p95_ms ? oltp.p95_ms / baseline.p95_ms : null

  if (oltp.samples < MIN_PROBE_SAMPLES) {
    return (
      <div className="text-on-surface-variant border-t border-white/5 px-6 py-3 text-[9px] leading-relaxed">
        <p className="font-bold uppercase tracking-wider">Point read while it ran</p>
        <p className="mt-1 opacity-60">
          Answered inside {oltp.samples} probe {oltp.samples === 1 ? 'interval' : 'intervals'} — too
          quick to have disturbed anything measurably.
        </p>
      </div>
    )
  }

  return (
    <div className="text-on-surface-variant border-t border-white/5 px-6 py-3 text-[9px] leading-relaxed">
      <p className="font-bold uppercase tracking-wider">Point read while it ran</p>
      <p className="mt-1 tabular-nums">
        p50 {oltp.p50_ms} ms · p95 {oltp.p95_ms} ms · max {oltp.max_ms} ms
        <span className="opacity-60"> over {oltp.samples} reads</span>
      </p>
      {baseline?.samples ? (
        <p className="mt-0.5 tabular-nums opacity-60">
          before this run p50 {baseline.p50_ms} ms · p95 {baseline.p95_ms} ms
          {worse ? ` — p95 ×${worse.toFixed(1)}` : ''}
        </p>
      ) : null}
      {oltp.failures > 0 && (
        <p className="text-tertiary mt-0.5 font-bold">
          {oltp.failures} point {oltp.failures === 1 ? 'read' : 'reads'} did not return
        </p>
      )}
    </div>
  )
}

/**
 * What the transactional path paid while several analytical paths ran at once.
 * Reported once rather than per path, because while they overlap the cost cannot
 * be attributed to any one of them without inventing an attribution.
 */
function CombinedOltp({
  oltp,
  baseline,
  paths,
}: {
  oltp?: OltpImpact | null
  baseline?: OltpImpact | null
  paths: number
}) {
  if (!oltp || oltp.samples < MIN_PROBE_SAMPLES) return null

  return (
    <div className="border-tertiary/30 bg-tertiary/5 rounded-lg border p-4">
      <p className="text-tertiary text-[10px] font-bold uppercase tracking-wider">
        Point read while all {paths} ran together
      </p>
      <p className="text-on-surface-variant mt-1 text-xs tabular-nums">
        p50 {oltp.p50_ms} ms · p95 {oltp.p95_ms} ms · max {oltp.max_ms} ms
        <span className="opacity-60"> over {oltp.samples} reads</span>
        {baseline?.samples ? (
          <span className="opacity-60">
            {' '}
            · before this run p50 {baseline.p50_ms} ms, p95 {baseline.p95_ms} ms
          </span>
        ) : null}
        {oltp.failures > 0 && (
          <span className="text-tertiary font-bold">
            {' '}
            · {oltp.failures} did not return
          </span>
        )}
      </p>
      <p className="text-on-surface-variant mt-2 text-[10px] leading-relaxed">
        One measurement for the whole window, not one per path: while they overlap, this cost
        belongs to all of them together and to none of them in particular. To find out what a
        single path charges, select that path on its own and run it.
      </p>
    </div>
  )
}

function EngineCard({
  label,
  role,
  colour,
  result,
  baseline,
}: {
  label: string
  role: string
  colour: string
  result: EngineResult | undefined
  baseline?: OltpImpact | null
}) {
  if (!result) return null
  const succeeded = result.available && !result.error

  return (
    <div className="glass-panel overflow-hidden rounded-xl">
      <div className="flex items-start justify-between gap-3 border-b border-white/5 px-6 py-4">
        {/* What the path is and how it reaches the data is a tooltip on the title
            rather than a paragraph under it.  With five boxes side by side the two
            things worth reading at a glance are the clock and the statement this
            path was actually given, and three lines of prose above them left
            neither any width.  The dotted underline is the cue that there is more
            to read; the same text is on the path's checkbox above. */}
        <p
          className="cursor-help text-[10px] font-black uppercase tracking-wider underline decoration-dotted decoration-1 underline-offset-4"
          style={{ color: colour }}
          title={role}
        >
          {label}
        </p>
        {succeeded && (
          <div className="shrink-0 text-right">
            <p className="font-headline text-2xl font-black tabular-nums" style={{ color: colour }}>
              {formatMs(result.query_time_ms)}
            </p>
            <p className="text-on-surface-variant text-[9px]">{result.row_count} rows</p>
            {/* Only the two paths that read files can say how much data was behind
                a read.  Worth the line: this table grows while the demo runs, so
                without the volume a bigger read is indistinguishable from a slower
                one. */}
            {result.snapshot_bytes != null && result.snapshot_bytes > 0 && (
              <>
                <p
                  className="text-on-surface-variant mt-0.5 text-[9px] tabular-nums"
                  title="The snapshot this read was taken over. A statement that names partitions reads only those, so the rate is quoted only when the query scans the lot."
                >
                  {formatBytes(result.snapshot_bytes)} snapshot
                  {/* The rate is a throughput only for a query that reads all of it,
                      and only once the data outweighs the fixed cost of taking the
                      snapshot and starting a job. */}
                  {scannedItAll(result) &&
                    result.snapshot_bytes >= RATE_WORTH_QUOTING_BYTES &&
                    result.query_time_ms > 0 &&
                    ` · ${Math.round(result.snapshot_bytes / 1_000 / result.query_time_ms)} MB/s`}
                </p>
                {/* What the snapshot cost, and how current it leaves the answer.  A
                    reused one is the whole point of the control: its age is the age
                    of these rows, while the other paths answered as of now. */}
                <p
                  className={`mt-0.5 text-[9px] tabular-nums ${
                    result.snapshot_reused ? 'text-secondary' : 'text-on-surface-variant/70'
                  }`}
                >
                  {result.snapshot_reused
                    ? `reused · ${formatMs((result.snapshot_age_s ?? 0) * 1000)} old`
                    : result.snapshot_ms != null
                      ? `taken in ${formatMs(result.snapshot_ms)}`
                      : ''}
                </p>
              </>
            )}
            {/* The same two figures for the path that takes no snapshot: the live
                files it opened, and how stale they leave the answer.  The rate is
                quoted on the same condition as the bulk reader's, and for the same
                reason: a statement that names partitions reads a fraction of what
                it opened. */}
            {result.sstable_files != null && result.sstable_files > 0 && (
              <>
                <p
                  className="text-on-surface-variant mt-0.5 text-[9px] tabular-nums"
                  title="The live SSTable files this read merged, and their total size.  There is no snapshot and no copy: these are the files Cassandra itself is writing, opened where they lie."
                >
                  {result.sstable_files} live files · {formatBytes(result.sstable_bytes ?? 0)}
                  {scannedItAll(result) &&
                    (result.sstable_bytes ?? 0) >= RATE_WORTH_QUOTING_BYTES &&
                    result.query_time_ms > 0 &&
                    ` · ${Math.round((result.sstable_bytes ?? 0) / 1_000 / result.query_time_ms)} MB/s`}
                </p>
                {/* Amber, like a reused snapshot, and for the same reason: this
                    answer is as of a moment that has passed.  Rows written since
                    the last flush are in a memtable and were not read. */}
                <p className="text-secondary mt-0.5 text-[9px] tabular-nums">
                  as of the last flush · {formatMs((result.data_age_s ?? 0) * 1000)} old
                  {result.reader_open_ms != null &&
                    ` · opened in ${formatMs(result.reader_open_ms)}`}
                </p>
              </>
            )}
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

      <OltpFooter oltp={result.oltp} baseline={baseline} />
    </div>
  )
}

/**
 * A path that has not answered yet, in the slot its result will fill.  It exists so
 * that a box arriving does not shift the four beside it, and so that a viewer can
 * see which path is working: on the whole history that is minutes of one path at a
 * time, and a grid that simply grew a column every few minutes said nothing about
 * what it was waiting for.
 */
function PendingCard({
  label,
  role,
  colour,
  running,
}: {
  label: string
  role: string
  colour: string
  running: boolean
}) {
  return (
    <div className="glass-panel overflow-hidden rounded-xl opacity-50">
      <div className="flex items-start justify-between gap-3 border-b border-white/5 px-6 py-4">
        <p
          className="cursor-help text-[10px] font-black uppercase tracking-wider underline decoration-dotted decoration-1 underline-offset-4"
          style={{ color: colour }}
          title={role}
        >
          {label}
        </p>
        <MaterialIcon
          name={running ? 'sync' : 'schedule'}
          className={`shrink-0 text-[16px] ${running ? 'animate-spin' : 'opacity-50'}`}
        />
      </div>
      <p className="text-on-surface-variant p-6 text-[10px] italic leading-relaxed">
        {running
          ? 'Running now, with nothing else the dashboard controls running beside it.'
          : 'Waiting for the paths above to finish, so that its figure is its own.'}
      </p>
    </div>
  )
}

function ComparePanel() {
  const [preset, setPreset] = useState<string>(COMPARE_PRESETS[0].key)
  const [sql, setSql] = useState<string>(COMPARE_PRESETS[0].sql ?? DEFAULT_SQL)

  // Which window can be named, refreshed often enough that the preset does not
  // offer a bucket that has since stopped being the last complete one.
  const { data: eventWindow } = useQuery<EventWindow>({
    queryKey: ['event-window'],
    queryFn: () => getJson<EventWindow>('/api/query/window'),
    refetchInterval: 60000,
  })
  const [chosen, setChosen] = useState<Engine[]>(ENGINES.map((engine) => engine.key))
  const [mode, setMode] = useState<RunMode>('sequential')
  // Off by default: taking a snapshot per read is what makes "the same rows at the
  // same moment" true, and that is the claim the comparison exists to make.
  const [reuseSnapshot, setReuseSnapshot] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [results, setResults] = useState<BenchmarkResponse | null>(null)
  const [pending, setPending] = useState(false)
  // The paths this run will ask, in the order it will ask them, and the order they
  // have answered in.  Both are needed: the first decides the slots and which one is
  // working, the second the order the answers are read in.
  const [runOrder, setRunOrder] = useState<Engine[]>([])
  const [answered, setAnswered] = useState<Engine[]>([])

  /** The quickest-first order for the question on offer, restricted to the paths chosen. */
  const orderFor = (presetKey: string, engines: Engine[]): Engine[] => {
    const option = COMPARE_PRESETS.find((candidate) => candidate.key === presetKey)
    const order: readonly Engine[] = option?.order ?? DEFAULT_RUN_ORDER
    // A hand-edited statement keeps whichever preset was last selected, so this is
    // an estimate for a question nobody has measured; the default covers a key that
    // carries no order at all.
    return order.filter((engine) => engines.includes(engine))
  }

  /**
   * Run the comparison.  One at a time it reads the streaming route, so each path
   * appears as it answers; all at once it reads the whole-body route, because paths
   * that overlap have no individual figure to report as each finishes.
   */
  const start = async () => {
    const order = orderFor(preset, chosen)
    setError(null)
    setAnswered([])
    setRunOrder(order)
    setPending(true)
    // Cleared to the mode about to run rather than to null, so the empty slots are
    // rendered from the first frame instead of after the first path answers.
    setResults({ mode })
    try {
      if (mode === 'parallel') {
        setResults(
          await postJson<BenchmarkResponse>('/api/query/benchmark', {
            sql,
            limit: 10,
            engines: chosen,
            mode,
            reuse_snapshot: reuseSnapshot,
          }),
        )
        setAnswered(order)
      } else {
        await postNdjson(
          '/api/query/benchmark/stream',
          { sql, limit: 10, engines: order, mode: 'sequential', reuse_snapshot: reuseSnapshot },
          (line) => {
            if (line.event === 'baseline') {
              setResults((current) => ({
                ...(current ?? {}),
                mode: 'sequential',
                oltp_baseline: line.oltp_baseline as OltpImpact | null,
              }))
            } else if (line.event === 'engine') {
              const engine = line.engine as Engine
              setResults((current) => ({
                ...(current ?? {}),
                mode: 'sequential',
                [engine]: line.result as EngineResult,
              }))
              setAnswered((current) => [...current, engine])
            }
          },
        )
      }
    } catch (e) {
      setError((e as Error).message)
      // A run that failed part-way keeps the paths that did answer: they were timed
      // alone and are as true as they were before the next path broke.
      setRunOrder([])
    } finally {
      setPending(false)
    }
  }

  /** Keep at least one path selected: a comparison of nothing is not a question. */
  const toggle = (engine: Engine) =>
    setChosen((current) =>
      current.includes(engine)
        ? current.length > 1
          ? current.filter((key) => key !== engine)
          : current
        : [...current, engine],
    )

  // Whether the query about to run is one of the minutes-long ones, which decides
  // whether running the paths together is worth warning about.
  const costsMinutes = COMPARE_PRESETS.find((option) => option.key === preset)?.cost === 'minutes'

  // Render the paths the answer actually covers, not the ones selected now: the
  // selection can be changed after a run, and the results would then be labelled
  // with paths they never included.
  //
  // In the order they answered, for a run made one at a time: that is the order they
  // were asked in, which is quickest first, and it is the order they appeared on
  // screen.  A parallel run keeps the fixed order instead, since nothing
  // distinguishes one arrival from another when they overlap.
  const ranInParallel = results?.mode === 'parallel'
  const shown = (
    ranInParallel
      ? ENGINES.filter((engine) => results?.[engine.key]).map((engine) => engine.key)
      : answered
  )
    .map((key) => ENGINES.find((engine) => engine.key === key))
    .filter((engine): engine is (typeof ENGINES)[number] => Boolean(engine && results?.[engine.key]))
  // The paths this run has still to reach, in the order it will reach them.  The
  // first of them is the one working now.
  const waiting = pending ? runOrder.filter((key) => !answered.includes(key)) : []
  // Slots rather than answers, so a box arriving does not reshuffle the ones beside
  // it while a run is in flight.
  const slots = shown.length + waiting.length
  const timings = shown.map((engine) => {
    const result = results?.[engine.key]
    const failed = Boolean(result?.available && result.error)
    return {
      ...engine,
      ms: result && result.available && !result.error ? result.query_time_ms : null,
      failed,
      // A path declining a query answers in milliseconds; a path that worked for
      // minutes and then broke is a different finding, and calling both of them
      // "cannot answer this query" would hide the one worth knowing about.
      failedAfterMs:
        failed && (result?.query_time_ms ?? 0) > REFUSAL_MS ? result!.query_time_ms : null,
    }
  })
  const slowest = Math.max(...timings.map((t) => t.ms ?? 0), 1)

  return (
    <div className="space-y-6">
      <div className="glass-panel overflow-hidden rounded-xl">
        <div className="flex flex-wrap items-center gap-4 border-b border-white/5 bg-surface-container-high px-6 py-3">
          <span className="text-primary text-[10px] font-black uppercase tracking-widest">
            One query, {chosen.length} access {chosen.length === 1 ? 'path' : 'paths'}
          </span>
          <span className="text-on-surface-variant text-[10px] uppercase tracking-wider opacity-60">
            Each in its own dialect; the rewrite is shown with its result
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-x-6 gap-y-3 border-b border-white/5 px-6 py-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-on-surface-variant text-[9px] font-bold uppercase tracking-wider">
              Compare
            </span>
            {ENGINES.map((engine) => {
              const on = chosen.includes(engine.key)
              return (
                <button
                  key={engine.key}
                  onClick={() => toggle(engine.key)}
                  title={
                    on && chosen.length === 1
                      ? 'At least one path has to stay selected'
                      : engine.role
                  }
                  aria-pressed={on}
                  className="flex cursor-pointer items-center gap-1.5 rounded px-2 py-1 text-[10px] font-bold uppercase tracking-wider transition-colors"
                  style={{
                    background: on ? 'color-mix(in srgb, var(--color-surface-container-highest) 100%, transparent)' : 'transparent',
                    color: on ? engine.colour : 'var(--color-on-surface-variant)',
                    opacity: on ? 1 : 0.5,
                  }}
                >
                  <MaterialIcon
                    name={on ? 'check_box' : 'check_box_outline_blank'}
                    className="text-[14px]"
                  />
                  {engine.label}
                </button>
              )
            })}
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <span className="text-on-surface-variant text-[9px] font-bold uppercase tracking-wider">
              Run
            </span>
            {RUN_MODES.map((option) => (
              <button
                key={option.key}
                onClick={() => setMode(option.key)}
                title={option.hint}
                aria-pressed={mode === option.key}
                className={`cursor-pointer rounded px-3 py-1 text-[10px] font-bold uppercase tracking-wider transition-colors ${
                  mode === option.key
                    ? 'bg-primary text-on-primary'
                    : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
                }`}
              >
                {option.label}
              </button>
            ))}
            <span className="text-on-surface-variant/60 text-[10px]">
              {mode === 'parallel'
                ? 'Contending on purpose: every figure inflates, and none is comparable with a run made one at a time'
                : 'Each path timed alone, so a figure is that path and nothing else; quickest first, and each box appears as its path answers'}
            </span>
          </div>

          {/* Only the bulk reader takes a snapshot, so the control is only shown
              when it is one of the paths being compared. */}
          {chosen.includes('spark_bulk') && (
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-on-surface-variant text-[9px] font-bold uppercase tracking-wider">
                Snapshot
              </span>
              {[
                { reuse: false, label: 'Fresh each run' },
                { reuse: true, label: 'Reuse the last' },
              ].map((option) => (
                <button
                  key={String(option.reuse)}
                  onClick={() => setReuseSnapshot(option.reuse)}
                  aria-pressed={reuseSnapshot === option.reuse}
                  title={
                    option.reuse
                      ? 'Read the snapshot the last bulk query took, skipping the hardlink pass over every SSTable. Faster, but the rows are as of that snapshot.'
                      : 'Take a coordinated snapshot for this read, so the bulk reader answers as of now like the other paths'
                  }
                  className={`cursor-pointer rounded px-3 py-1 text-[10px] font-bold uppercase tracking-wider transition-colors ${
                    reuseSnapshot === option.reuse
                      ? 'bg-primary text-on-primary'
                      : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
                  }`}
                >
                  {option.label}
                </button>
              ))}
              <span className="text-on-surface-variant/60 text-[10px]">
                {reuseSnapshot
                  ? 'Skips a fixed cost the bulk reader pays per read, and answers as of that snapshot rather than now'
                  : 'The bulk reader answers as of this moment, like the three paths that read through CQL'}
              </span>
            </div>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2 border-b border-white/5 px-6 py-3">
          {COMPARE_PRESETS.map((option) => {
            // A preset that names a window cannot be offered until the stack has
            // said which window that is.
            const statement = 'build' in option ? (eventWindow ? option.build(eventWindow) : null) : option.sql
            return (
            <button
              key={option.key}
              disabled={statement === null}
              onClick={() => {
                if (statement === null) return
                setPreset(option.key)
                setSql(statement)
              }}
              title={
                statement === null
                  ? 'Waiting for the stack to report which window is complete'
                  : option.hint
              }
              className={`rounded px-3 py-1.5 text-[10px] font-bold uppercase tracking-wider transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${
                preset === option.key
                  ? 'bg-primary text-on-primary'
                  : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
              }`}
            >
              {option.label}
              <span className="ml-1.5 font-normal normal-case opacity-60">{option.cost}</span>
            </button>
            )
          })}
          <span className="text-on-surface-variant/60 ml-2 text-[10px]">
            {COMPARE_PRESETS.find((option) => option.key === preset)?.hint}
            {preset === 'window' && eventWindow
              ? ' · ' +
                eventWindow.bucket +
                ', ' +
                eventWindow.bucket_minutes +
                ' minutes over ' +
                eventWindow.shards +
                ' shards' +
                (!eventWindow.closed
                  ? ' · still filling, because none has closed since ingest began, so the totals will differ by whatever arrives in between'
                  : eventWindow.settled
                    ? ' · closed, and the sink has moved past it, so the paths that can answer it must agree exactly'
                    : ' · closed, but the sink has not been shown to have moved past it, so the totals may differ by whatever it is still writing into it')
              : ''}
          </span>
        </div>

        <label htmlFor="compare-sql" className="sr-only">
          SQL to compare
        </label>
        <textarea
          id="compare-sql"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          className="text-primary h-40 w-full resize-none bg-surface-container-lowest/40 p-6 font-code text-sm leading-relaxed focus:outline-none"
          spellCheck={false}
        />
      </div>

      <button
        onClick={() => void start()}
        disabled={pending}
        className="font-headline flex w-full cursor-pointer items-center justify-center gap-3 rounded border border-white/10 bg-surface-container px-8 py-3 font-bold tracking-wider transition-all hover:bg-surface-container-high active:scale-95 disabled:opacity-60"
      >
        <MaterialIcon
          name={pending ? 'sync' : 'compare_arrows'}
          className={pending ? 'animate-spin' : ''}
        />
        {pending ? 'Running…' : 'Run'}
        <span className="font-normal opacity-70">
          {chosen.length === ENGINES.length
            ? 'all five paths'
            : `${chosen.length} ${chosen.length === 1 ? 'path' : 'paths'}`}
          {chosen.length > 1 && (mode === 'parallel' ? ' at once' : ' one at a time, quickest first')}
        </span>
      </button>

      {mode === 'parallel' && chosen.length > 1 && costsMinutes && (
        <div className="border-secondary/30 bg-secondary/5 text-on-surface-variant flex items-start gap-3 rounded-lg border p-4">
          <MaterialIcon name="schedule" className="text-secondary shrink-0 text-[18px]" />
          <p className="text-[11px] leading-relaxed">
            Measured here, all five over the whole history took 6 min 42 s, and every path that can
            express the question still answered: Presto in 58 s, Spark in 3 min 6 s, the bulk reader
            in 3 min 16 s, and cqlite in 6 min 42 s. Every one of those figures is inflated by the
            other four, which is the contention you asked to see; on a busier stack a path can be
            starved until its timeout instead, and the run then reports that rather than hiding it.
            The point read sampled across the window is the measurement that survives the crowding:
            p50 5.1 ms against 5.5 ms before the run, with one read at 1.9 s. For a figure per path,
            run them one at a time.
          </p>
        </div>
      )}

      {error && (
        <div className="border-tertiary/30 bg-tertiary/10 text-tertiary flex items-start gap-3 rounded-lg border p-4">
          <MaterialIcon name="error" className="shrink-0 text-[18px]" />
          <p className="text-xs font-bold">{error}</p>
        </div>
      )}

      {results && (
        <>
          <div
            className={`grid grid-cols-1 gap-4 md:grid-cols-2 ${
              slots >= 5
                ? 'xl:grid-cols-5'
                : slots === 4
                  ? 'xl:grid-cols-4'
                  : slots === 3
                    ? 'xl:grid-cols-3'
                    : ''
            }`}
          >
            {shown.map((engine) => (
              <EngineCard
                key={engine.key}
                label={engine.label}
                role={engine.role}
                colour={engine.colour}
                result={results[engine.key]}
                baseline={results.oltp_baseline}
              />
            ))}
            {waiting.map((key, index) => {
              const engine = ENGINES.find((candidate) => candidate.key === key)!
              return (
                <PendingCard
                  key={key}
                  label={engine.label}
                  role={engine.role}
                  colour={engine.colour}
                  running={index === 0}
                />
              )
            })}
          </div>

          <div className="glass-panel space-y-4 rounded-xl border border-white/5 p-6">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <p className="text-on-surface-variant text-[10px] font-bold uppercase tracking-wider">
                Time to answer
              </p>
              <p className="text-[10px] font-bold uppercase tracking-wider">
                <span style={{ color: ranInParallel ? 'var(--color-tertiary)' : 'var(--color-positive)' }}>
                  {ranInParallel ? 'run all at once' : 'run one at a time'}
                </span>
                <span className="text-on-surface-variant ml-2 font-normal normal-case opacity-60">
                  {ranInParallel
                    ? 'these figures include the paths obstructing each other'
                    : 'each figure is that path alone'}
                </span>
              </p>
            </div>

            {baselineUnsettled(results.oltp_baseline) && (
              <p className="text-secondary border-secondary/30 bg-secondary/5 rounded-lg border p-3 text-[10px] leading-relaxed">
                The reference read taken before this run was itself uneven — p95{' '}
                {results.oltp_baseline?.p95_ms} ms against a p50 of {results.oltp_baseline?.p50_ms} ms.
                The stack had not settled, most likely from a comparison that had just finished
                releasing its Spark executors and snapshots. Everything below is measured against
                that reference, so leave it a minute and run again for a cleaner one.
              </p>
            )}

            {ranInParallel && (
              <CombinedOltp
                oltp={results.oltp_combined}
                baseline={results.oltp_baseline}
                paths={shown.length}
              />
            )}
            {timings.map((timing) => (
              <div key={timing.key} className="space-y-1">
                <div className="flex justify-between text-[10px] font-bold">
                  <span style={{ color: timing.colour }}>{timing.label}</span>
                  <span className="text-on-surface-variant tabular-nums">
                    {timing.ms != null
                      ? formatMs(timing.ms)
                      : timing.failedAfterMs != null
                        ? `failed after ${formatMs(timing.failedAfterMs)}`
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
                These are not benchmark results. This is one Cassandra node on one machine, sharing
                its cores with Presto, Spark and a live ingest.{' '}
                {ranInParallel
                  ? 'These paths ran together, so each figure includes the others getting in its way; that is what the run was for, but it means no figure here is a measure of the path it sits under.'
                  : 'Each path was timed alone, so its figure is its own cost.'}{' '}
                Read the figures as a floor on what the mechanism costs here, not as a measure of any
                engine: given more nodes the Presto and Spark paths scale out, the transactional one
                does not change at all, and the in-process reader would want one of itself per node.
                Separating them is the whole point.
              </p>
              <p>
                A path reporting an error has usually not failed: it has told you the query is not for
                it. CQL groups only by primary-key columns, which is exactly why the analytical paths
                exist.{' '}
                {ranInParallel
                  ? 'The clock beside it says which kind of error it is: a path that declined the query answers in milliseconds, while one that ran for minutes and then gave up was starved of cores by the paths beside it — the contention, arriving as a failure rather than as a slower figure.'
                  : ''}{' '}
                Change the size of the question and the ordering changes with it — a single-partition
                lookup, a grouped read of the current fleet, and a scan of the whole history do not
                favour the same path, and no single number decides between them.
              </p>
              <p>
                The point read is sampled every 250&nbsp;ms while the query runs, against the same read
                taken just before the run started.{' '}
                {ranInParallel
                  ? 'Because these paths overlapped, it is reported once for the whole window rather than under each result.'
                  : 'Under each result it is the price that path charged the transactional one.'}{' '}
                Three of these paths go through the CQL request path and share it with the live
                ingest, so they show up there. The bulk reader reads SSTable files from a coordinated
                snapshot through the Sidecar instead: taking that snapshot is a brief hardlink pass on
                the node, which you may see as a single spike, and the scan that follows touches the
                request path not at all. The cqlite reader takes no snapshot: it opens the live files
                where they lie and parses them in this dashboard's own process, so it asks the node
                for nothing and there is no spike to see.
              </p>
              <p>
                Both run modes are legitimate and they answer different questions. One at a time
                tells you what a path costs; all at once tells you what the paths cost each other,
                which is the question worth asking of a stack that is meant to keep them apart.
                Timings from the two modes are not comparable, so the mode is stated above them.
              </p>
              <p>
                <strong className="font-bold">A snapshot is a fixed cost per bulk read.</strong>{' '}
                Taking one hardlinks every SSTable of the table, so it costs the same whether the query
                then reads all of it or one window — measured here, a quarter of a bounded read's total,
                and a larger share the more files the table has. Reusing the last one skips it: 0.84 s
                to 0.35 s on the bounded read above, 4.07 s to 3.04 s on one window. What it costs is
                currency. The rows are then as of when that snapshot was taken, not now, so the bulk
                reader is no longer answering the same instant as the paths that read through CQL —
                which is why it is
                off by default and why each result says whether its snapshot was reused and how old it
                was. A snapshot too near the end of its life to survive the read is refused and a fresh
                one taken, because Cassandra expires it on time regardless of who is reading.
              </p>
              <p>
                <strong className="font-bold">One window is the same question, bounded.</strong> Events
                are partitioned by a 15-minute window and a shard, so naming the window names the
                partitions that hold it: Cassandra reads them and needs no ALLOW FILTERING, Presto and
                the connector push the predicate down, the bulk reader turns it into a partition-key
                lookup instead of a scan, and the cqlite reader seeks to each of the sixteen
                partitions through the SSTable index rather than walking the files. Its cost is then
                proportional to the rows the window holds: measured here, one shard of a window took
                2.9 s and all sixteen took 47.1 s, on a table of 24 files and 2.5 GB.
                Compare it against the whole history above and
                the difference is the data model, not the engines. Cassandra still declines the
                grouping, because a bounded question is not the same as an expressible one. And a
                window the sink has finished writing cannot change, so the paths that can answer it
                agree on the totals exactly, where the unbounded presets see the table grow
                underneath them. The clock closing a window is not enough on its own: the sink files
                each event under the event's own timestamp, so a sink behind the topic keeps
                inserting into windows the clock has passed, and three paths reading one such window
                returned 80,810, 81,697 and 82,869 rows. The line above the statement says which
                state the window on offer is in, since for the first quarter of an hour after a wipe
                none has closed at all.
              </p>
              <p>
                Run the whole history twice an hour apart and the second run is slower, because the
                table it scans grew in between — at the default ingest rate, by tens of megabytes a
                minute. That is why the bulk reader reports the volume it streamed and the rate it
                managed: a scan that is bigger reads differently from one that is slower, and only the
                path that reads files can tell you which it was.
              </p>
              <p>
                On the grouped queries, watch the counts rather than only the clock. The two paths that
                read through Cassandra see the table grow underneath them while they scan, so their
                totals differ; the bulk reader answers from one snapshot, so its groups are consistent
                with each other. So are the cqlite reader's: it opens every generation before the scan
                begins and an SSTable never changes after it is written, which gives the same
                consistency without a snapshot, as of the last flush rather than as of now. That is
                point-in-time consistency, and it is the other half of what reading SSTables directly
                buys.
              </p>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

/**
 * What the live embedder has actually done, rather than that it is switched on.  A
 * toggle with no figures beside it cannot be checked: `pending` and `behind` are
 * the two that say whether the index is keeping up.
 */
function LiveEmbeddingFigures({ live }: { live: LiveEmbedding }) {
  const rows: [string, string][] = [
    ['Embedder', live.embedder === 'remote' ? 'Remote API' : 'Local hashing'],
    ['Pass interval', `${live.interval_s}s`],
    ['Assets tracked', live.tracked.toLocaleString()],
    ['Embedded', live.embedded.toLocaleString()],
    [
      'Last pass',
      live.passes === 0
        ? 'none yet'
        : `${live.last_embedded} in ${formatMs(live.last_pass_ms)}`,
    ],
    ['Waiting for the next pass', live.pending.toLocaleString()],
    ['Last pass ran', live.behind_s == null ? '—' : `${live.behind_s.toFixed(0)}s ago`],
  ]
  if (live.failed > 0) rows.push(['Failed writes', live.failed.toLocaleString()])

  return (
    <div className="space-y-1">
      {rows.map(([label, value]) => (
        <div key={label} className="flex items-baseline justify-between gap-2">
          <span className="text-on-surface-variant/70 text-[9px] uppercase tracking-wider">
            {label}
          </span>
          <span className="font-mono text-[10px] tabular-nums">{value}</span>
        </div>
      ))}
      {live.error && (
        <p className="text-tertiary mt-2 text-[9px] leading-relaxed">{live.error}</p>
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

  /* The live embedder, polled only while this tab is open: it is a live figure on
     one tab, and polling it behind the SQL console would buy nothing. */
  const { data: live, refetch: refetchLive } = useQuery<LiveEmbedding>({
    queryKey: ['vector-live'],
    queryFn: () => getJson<LiveEmbedding>('/api/vector/live'),
    enabled: tab === 'ai',
    refetchInterval: 5000,
  })

  const setLive = useMutation({
    mutationFn: (enabled: boolean) =>
      postJson<LiveEmbedding>('/api/vector/live', { enabled }),
    onSuccess: (data) => {
      refetchLive()
      show(
        data.enabled
          ? `Live embedding on; each pass runs every ${data.interval_s}s, behind the writes`
          : 'Live embedding off; embeddings stay as they are',
        'info',
      )
    },
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
                    <div className="flex flex-wrap items-center gap-3">
                      {/* On or off, not a third state: the loop runs for the
                          backend's lifetime and idles while this is off, so there
                          is nothing to start or stop beyond the flag. */}
                      <button
                        onClick={() => setLive.mutate(!live?.enabled)}
                        disabled={setLive.isPending || live == null}
                        aria-pressed={live?.enabled ?? false}
                        title={
                          'Embed each snippet as the sink writes it, in a loop behind the writes. ' +
                          'Nothing on the write path waits for it, so the point-read latency on the ' +
                          'Health page should not move.'
                        }
                        className={`flex items-center gap-2 rounded border px-4 py-1.5 text-[10px] font-black uppercase tracking-wider transition-colors disabled:opacity-50 ${
                          live?.enabled
                            ? 'border-positive/40 bg-positive/10 text-positive'
                            : 'text-on-surface-variant border-white/10 hover:text-on-surface'
                        }`}
                      >
                        <MaterialIcon
                          name={live?.enabled ? 'sync' : 'sync_disabled'}
                          className={
                            live?.enabled && (live?.last_embedded ?? 0) > 0
                              ? 'animate-spin text-[14px]'
                              : 'text-[14px]'
                          }
                        />
                        Live embedding {live?.enabled ? 'on' : 'off'}
                      </button>
                      <button
                        onClick={() => buildIndex.mutate()}
                        disabled={buildIndex.isPending}
                        title="Embed every asset's current snippet once, now"
                        className="bg-secondary/10 border-secondary/30 text-secondary hover:bg-secondary/20 flex items-center gap-2 rounded border px-4 py-1.5 text-[10px] font-black uppercase tracking-wider transition-colors"
                      >
                        <MaterialIcon
                          name="refresh"
                          className={
                            buildIndex.isPending ? 'animate-spin text-[14px]' : 'text-[14px]'
                          }
                        />
                        Build embeddings
                      </button>
                    </div>
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
                      className="text-primary h-60 w-full resize-none bg-surface-container-lowest/40 p-6 font-code text-sm leading-relaxed focus:outline-none"
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
                  <div className="space-y-4">
                    <p className="text-on-surface-variant text-[10px] leading-relaxed">
                      Build embeddings once to populate payload_vector for every asset that has text.
                      Searching then embeds your phrase and asks Cassandra for the nearest neighbours,
                      scoring each with similarity_cosine. Without an embedding API key the backend
                      hashes tokens locally, so matching is lexical rather than semantic — but it is
                      real, ranked and reproducible.
                    </p>
                    <div className="space-y-2 border-t border-white/5 pt-3">
                      <p className="text-secondary text-[10px] font-bold">Live embedding</p>
                      <p className="text-on-surface-variant text-[10px] leading-relaxed">
                        The producer rotates each asset's snippet every few seconds, so a one-off
                        build goes stale. Turned on, the backend re-embeds the snippets that changed,
                        in a loop of its own. It reads the snippets after the sink has written them
                        and writes the vectors separately, so no write waits for an embedding: the
                        index follows the data instead of standing in front of it.
                      </p>
                      {live && <LiveEmbeddingFigures live={live} />}
                    </div>
                  </div>
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
