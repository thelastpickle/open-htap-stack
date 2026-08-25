import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../../components/MaterialIcon'
import Panel from '../../components/Panel'
import { formatMs, getJson, postJson } from '../../lib/api'

/**
 * cassandra-sql: a Postgres dialect, with transactions, over Cassandra.
 *
 * Deliberately not part of the compare page.  cassandra-sql keeps SQL rows in its
 * own keyspaces under an encoding of its own, so it cannot read demo.events and a
 * timing beside the five paths would compare two different datasets.  What it
 * shows instead is SQL the other paths have no answer to: a join over four tables,
 * a transaction that commits, and one that rolls back.
 *
 * The panel is a write console, which is why it does not reuse Explore.tsx: that
 * page's backend rejects every write keyword.
 *
 * Every figure and every defect named in the copy below was measured on this
 * repository's own drone schema.  The page shipped first on GEICO's ecommerce
 * example, and each number taken there had to be re-measured rather than carried
 * over.
 */

interface Preset {
  id: string
  title: string
  description: string
  sql: string
}

interface StatementResult {
  sql: string
  columns: string[]
  rows: (string | null)[][]
  row_count: number
  duration_ms: number
  error: string | null
}

interface ConsoleResult {
  engine: string
  statements: StatementResult[]
  duration_ms: number
  error_count: number
}

interface Quirk {
  id: string
  title: string
  summary: string
  expected: string
  probe: StatementResult
  control: StatementResult
}

interface Status {
  engine: string
  connected: boolean
  host: string
  port: number
  database: string
  keyspaces: string[]
}

function ResultTable({ statement }: { statement: StatementResult }) {
  if (statement.error) {
    return (
      <p className="break-words rounded-lg bg-tertiary/10 p-3 font-mono text-xs text-tertiary">
        {statement.error}
      </p>
    )
  }
  if (statement.columns.length === 0) {
    return (
      <p className="text-xs text-on-surface-variant">
        No result set, which is what a DDL statement and a committed transaction return.
      </p>
    )
  }
  if (statement.rows.length === 0) {
    return (
      <p className="text-xs text-on-surface-variant">
        No rows.  The columns named are {statement.columns.join(', ')}.
      </p>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-xs">
        <thead className="text-[10px] uppercase tracking-wide text-on-surface-variant">
          <tr>
            {statement.columns.map((column) => (
              <th key={column} className="whitespace-nowrap pb-2 pr-4">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="font-mono text-on-surface">
          {statement.rows.map((row, index) => (
            <tr key={index} className="border-t border-outline-variant/40">
              {row.map((value, column) => (
                <td key={column} className="whitespace-nowrap py-1 pr-4">
                  {value ?? <span className="text-on-surface-variant">null</span>}
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
 * One measured defect beside the control that isolates it.
 *
 * Both halves are shown, and that is the whole design: a wrong answer on its own
 * could be this repository's SQL being wrong.  The control runs the same expression
 * over one table, is exact, and so puts the fault in the join.
 */
function QuirkCard({ quirk }: { quirk: Quirk }) {
  return (
    <li className="rounded-lg border-l-4 border-secondary bg-surface-variant/40 p-4">
      <h3 className="text-xs font-bold text-on-surface">{quirk.title}</h3>
      <p className="mt-1 text-xs text-on-surface-variant">{quirk.summary}</p>
      <p className="mt-2 text-xs text-primary">
        <span className="font-bold uppercase tracking-wide">A correct engine answers</span>{' '}
        {quirk.expected}
      </p>
      <div className="mt-3 grid gap-4 lg:grid-cols-2">
        <div>
          <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-secondary">
            The probe, over a join
          </div>
          <pre className="mb-2 overflow-x-auto rounded bg-black/40 p-2 font-mono text-[10px] leading-relaxed text-on-surface-variant">
            {quirk.probe.sql}
          </pre>
          <ResultTable statement={quirk.probe} />
        </div>
        <div>
          <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-primary">
            The control, over one table
          </div>
          <pre className="mb-2 overflow-x-auto rounded bg-black/40 p-2 font-mono text-[10px] leading-relaxed text-on-surface-variant">
            {quirk.control.sql}
          </pre>
          <ResultTable statement={quirk.control} />
        </div>
      </div>
    </li>
  )
}

export default function SqlPanel() {
  const [sql, setSql] = useState('')
  const [result, setResult] = useState<ConsoleResult | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [activePreset, setActivePreset] = useState<string | null>(null)
  const [showQuirks, setShowQuirks] = useState(false)

  const { data: status } = useQuery<Status>({
    queryKey: ['sql-console-status'],
    queryFn: () => getJson<Status>('/api/sql-console/status'),
    refetchInterval: 15_000,
  })

  const { data: presets } = useQuery<Preset[]>({
    queryKey: ['sql-console-presets'],
    queryFn: () => getJson<Preset[]>('/api/sql-console/presets'),
    staleTime: Infinity,
  })

  // The four defects are run against the live service rather than quoted, so a
  // release that fixes one shows it fixed.  Asked for only once the reader opens
  // the section: it is eight statements, and most visits will not want them.
  const { data: quirks, isFetching: quirksFetching } = useQuery<Quirk[]>({
    queryKey: ['sql-console-quirks'],
    queryFn: () => getJson<Quirk[]>('/api/sql-console/quirks'),
    enabled: showQuirks,
    staleTime: 60_000,
  })

  // Open on the first preset rather than on an empty box, so the page shows a
  // statement worth running before anything is clicked.
  useEffect(() => {
    const first = presets?.[0]
    if (first && !sql) {
      setSql(first.sql)
      setActivePreset(first.id)
    }
  }, [presets, sql])

  const call = async (fn: () => Promise<ConsoleResult>) => {
    setRunning(true)
    setError(null)
    try {
      setResult(await fn())
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setRunning(false)
    }
  }

  const run = () => call(() => postJson<ConsoleResult>('/api/sql-console/execute', { sql }))
  const createSchema = () => call(() => postJson<ConsoleResult>('/api/sql-console/schema'))
  const counts = () => call(() => getJson<ConsoleResult>('/api/sql-console/tables'))
  // Destructive, and separated from the other two for that reason: it drops all five
  // tables before recreating them.  Reset rather than Create is the button a second
  // visit wants, because UNIQUE is held and so the seed is refused on a replay.
  const reset = () => call(() => postJson<ConsoleResult>('/api/sql-console/reset'))

  const chosen = presets?.find((preset) => preset.id === activePreset)

  return (
    <div className="space-y-6">
      <p className="max-w-4xl text-sm text-on-surface-variant">
        GEICO's{' '}
        <a
          href="https://github.com/geico/cassandra-sql"
          className="text-primary hover:underline"
          target="_blank"
          rel="noreferrer"
        >
          cassandra-sql
        </a>{' '}
        speaks the Postgres wire protocol and plans SQL with Calcite over Cassandra used as an
        ordered key-value store.  It gives joins, subqueries and multi-statement transactions,
        none of which CQL has.
      </p>
      <p className="max-w-4xl text-sm text-on-surface-variant">
        Its five tables are its own: operators, drones, zones, flights, and the legs a flight
        flies through restricted airspace.  They carry the fleet's names and the same three Oslo
        zones the map draws, and there is no copy from{' '}
        <span className="font-mono">demo.events</span> into them: cassandra-sql cannot read the
        demo keyspace, so this is a sixth interface and not a sixth access path.  A timing beside
        the five would compare different data.
      </p>
      <p className="max-w-4xl text-sm text-on-surface-variant">
        It is a proof of concept by its own account, at "~40% (core features only)" SQL
        compliance, and the measured behaviours below say where it does and does not hold what it
        declares.  It is built from a pinned revision plus one patch, which gives a second
        hard-coded Cassandra session the contact point the first one already reads; the whole diff
        is in <span className="font-mono">accord-sql/patches/</span>.
      </p>

      <div className="flex flex-wrap items-center gap-3 rounded-xl border border-outline-variant bg-surface p-4 text-xs">
        <MaterialIcon
          name={status?.connected ? 'check_circle' : 'error'}
          className={`text-lg ${status?.connected ? 'text-primary' : 'text-tertiary'}`}
        />
        <span className="font-bold text-on-surface">
          {status?.connected ? 'reachable' : 'not reachable'}
        </span>
        <span className="font-mono text-on-surface-variant">
          {status?.host}:{status?.port}
        </span>
        <span className="text-on-surface-variant">
          keyspaces: {status?.keyspaces?.join(', ') ?? '—'}
        </span>
        <div className="ml-auto flex gap-2">
          <button
            type="button"
            onClick={createSchema}
            disabled={running}
            className="rounded-lg bg-surface-variant px-3 py-2 font-bold text-on-surface-variant transition hover:bg-surface-variant/70 disabled:opacity-40"
          >
            Create the schema
          </button>
          <button
            type="button"
            onClick={counts}
            disabled={running}
            className="rounded-lg bg-surface-variant px-3 py-2 font-bold text-on-surface-variant transition hover:bg-surface-variant/70 disabled:opacity-40"
          >
            Count the tables
          </button>
          <button
            type="button"
            onClick={reset}
            disabled={running}
            title="Drops all five tables, then creates and seeds them again"
            className="flex items-center gap-2 rounded-lg border border-tertiary px-3 py-2 font-bold text-tertiary transition hover:bg-tertiary/10 disabled:opacity-40"
          >
            <MaterialIcon name="delete_sweep" className="text-base" />
            Drop and reseed
          </button>
        </div>
      </div>

      <Panel
        title="Statements"
        subtitle="Each is written for this schema.  Two of them are written around a defect rather than into it, and say which in their description."
      >
        <div className="flex flex-wrap gap-2">
          {(presets ?? []).map((preset) => (
            <button
              key={preset.id}
              type="button"
              onClick={() => {
                setSql(preset.sql)
                setActivePreset(preset.id)
              }}
              className={`rounded-lg px-3 py-2 text-xs font-bold transition ${
                activePreset === preset.id
                  ? 'bg-primary text-on-primary'
                  : 'bg-surface-variant text-on-surface-variant hover:bg-surface-variant/70'
              }`}
            >
              {preset.title}
            </button>
          ))}
        </div>
        {chosen && <p className="mt-3 text-xs text-on-surface-variant">{chosen.description}</p>}
        <textarea
          value={sql}
          onChange={(event) => {
            setSql(event.target.value)
            setActivePreset(null)
          }}
          spellCheck={false}
          rows={12}
          className="mt-3 w-full rounded-lg border border-outline-variant bg-black/40 p-3 font-mono text-[11px] leading-relaxed text-on-surface outline-none focus:border-primary"
        />
        <div className="mt-3 flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={run}
            disabled={running || !sql.trim()}
            className="flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-xs font-bold text-on-primary transition hover:opacity-90 disabled:opacity-40"
          >
            <MaterialIcon
              name={running ? 'hourglass_top' : 'play_arrow'}
              className="text-base"
            />
            {running ? 'running…' : 'Run'}
          </button>
          <p className="text-[10px] text-on-surface-variant">
            Sent as one string, semicolons included, because cassandra-sql executes the whole
            string as one unit and returns only its last result set.  There are no parameters, and
            that is not a simplification: an integer bound as a parameter returns no rows here and
            raises nothing, so offering one would offer a silent wrong answer.
          </p>
        </div>
        {error && (
          <p className="mt-3 break-words rounded-lg bg-tertiary/10 p-3 text-xs text-tertiary">
            {error}
          </p>
        )}
      </Panel>

      {result && (
        <Panel
          title="Result"
          subtitle={`${formatMs(result.duration_ms)} for ${result.statements.length} ${
            result.statements.length === 1 ? 'statement' : 'statements'
          }${result.error_count > 0 ? `, ${result.error_count} of which raised` : ''}`}
        >
          <ol className="space-y-4">
            {result.statements.map((statement, index) => (
              <li key={index}>
                <div className="mb-2 flex flex-wrap items-baseline gap-x-3 text-[10px] uppercase tracking-wide text-on-surface-variant">
                  <span className="font-bold">{formatMs(statement.duration_ms)}</span>
                  <span>
                    {statement.row_count} {statement.row_count === 1 ? 'row' : 'rows'}
                  </span>
                  {statement.row_count > statement.rows.length && (
                    <span className="text-secondary">
                      showing the first {statement.rows.length}
                    </span>
                  )}
                </div>
                {result.statements.length > 1 && (
                  <pre className="mb-2 overflow-x-auto rounded bg-black/40 p-2 font-mono text-[10px] text-on-surface-variant">
                    {statement.sql}
                  </pre>
                )}
                <ResultTable statement={statement} />
              </li>
            ))}
          </ol>
        </Panel>
      )}

      <Panel
        title="Four join defects, run against the live service"
        subtitle="Each probe is a join whose answer is wrong, beside a control over one table whose answer is exact.  Run rather than quoted, so a release that fixes one of them shows it fixed here."
      >
        {!showQuirks ? (
          <button
            type="button"
            onClick={() => setShowQuirks(true)}
            className="flex items-center gap-2 rounded-lg bg-surface-variant px-4 py-2 text-xs font-bold text-on-surface-variant transition hover:bg-surface-variant/70"
          >
            <MaterialIcon name="bug_report" className="text-base" />
            Reproduce them
          </button>
        ) : quirksFetching && !quirks ? (
          <p className="text-xs text-on-surface-variant">running eight statements…</p>
        ) : (
          <>
            <ul className="space-y-4">
              {(quirks ?? []).map((quirk) => (
                <QuirkCard key={quirk.id} quirk={quirk} />
              ))}
            </ul>
            <p className="mt-4 text-xs text-on-surface-variant">
              All four need a join.  The same arithmetic over one table is exact, and{' '}
              <span className="font-mono">ORDER BY</span> on an ungrouped SELECT is exact, so this
              is the join planner rather than the storage engine underneath it.  Nothing in CI
              asserts these answers: an assertion on a defect fails on the release that fixes it.
            </p>
          </>
        )}
      </Panel>

      <Panel
        title="What it does not hold"
        subtitle="Measured against this service on the drone schema, on throwaway rows that were deleted afterwards.  Present because the demo's credibility rests on the awkward results being here."
      >
        <ul className="space-y-2 text-xs text-on-surface-variant">
          <li>
            <span className="font-bold text-on-surface">A duplicate PRIMARY KEY overwrites.</span>{' '}
            The same flight inserted twice with different totals leaves one row holding the second,
            with no constraint violation.  Cassandra's write path, showing through.
          </li>
          <li>
            <span className="font-bold text-on-surface">A FOREIGN KEY is accepted and not
            enforced.</span>{' '}
            All four declared here succeed, and a flight naming an operator and a drone that do not
            exist is then stored.
          </li>
          <li>
            <span className="font-bold text-on-surface">NOT NULL and ENUM are accepted and not
            enforced.</span>{' '}
            An operator with no name is stored, and a status column declared{' '}
            <span className="font-mono">flight_status</span> stores the string 'nonsense'.
          </li>
          <li>
            <span className="font-bold text-on-surface">Arithmetic promotes an integer column to a
            double.</span>{' '}
            A cycle count reads back "75" as inserted and "75.0" once an UPDATE has added to it,
            and "5" again once one has assigned a literal, so it is the arithmetic that promotes
            it.  DECIMAL is a double as well, so 4.20 reads back "4.2".  Every value arrives as
            text.
          </li>
          <li>
            <span className="font-bold text-on-surface">COUNT(*) over an empty table raises.</span>{' '}
            "Aggregation failed: Index 0 out of bounds for length 0", where zero was the answer.
            So a table with no rows yet counts as an error above rather than as 0, and a UNION ALL
            that includes an empty table fails whole.  Which is why counting the tables asks one
            table at a time.
          </li>
        </ul>
        <p className="mt-4 text-xs text-on-surface-variant">
          <span className="font-bold text-on-surface">UNIQUE is held</span>, and it is the one
          declared constraint of the four above that is: a second operator carrying an existing
          licence number is refused by name.  So the layer can hold a constraint over Cassandra,
          and the foreign key going unenforced is a gap in the prototype rather than something the
          storage engine forbids.  Holding it is also what makes the seed non-idempotent, and why
          the destructive Drop and reseed button exists beside Create the schema.
        </p>
        <p className="mt-2 text-xs text-on-surface-variant">
          What does hold is the part this panel exists to show.{' '}
          <span className="font-mono">BEGIN; INSERT …; ROLLBACK;</span> leaves no row behind, so
          the write is buffered until COMMIT rather than applied as it goes.
        </p>
      </Panel>
    </div>
  )
}
