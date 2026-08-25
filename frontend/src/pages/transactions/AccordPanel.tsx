import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../../components/MaterialIcon'
import Panel from '../../components/Panel'
import { formatMs, getJson, postJson } from '../../lib/api'

/**
 * Accord transactions, and the one thing the five read paths cannot do.
 *
 * Every other page here is about reading the same rows five ways.  This one is
 * about a write: a conditional write whose condition lives in other partitions,
 * which no batch and no lightweight transaction can express.  The scripted
 * sequence is the argument, so the panel shows each step's own CQL and the state
 * after it, and a refused step is presented as a success rather than an error:
 * refusing the replay is the whole point.
 *
 * Two demonstrations, because one of them cannot make the whole claim.  The
 * session sequence runs its steps one after another, so nothing it shows rules out
 * a counter read and written back outside consensus; the airspace clearance
 * semaphore overlaps its asks, and the number of winners has to be the zone's
 * capacity exactly.
 */

interface TransactionStep {
  action: string
  cql: string
  applied: boolean
  reason: string
  projection: Record<string, string | null>
  duration_ms: number
  timeline_rows: number
  state: Record<string, unknown>
  error: string | null
}

interface TimelineRow {
  seq: number
  event_id: string
  event_time: string
  event_type: string
  payload: string
}

interface OltpImpact {
  p50_ms?: number | null
  p95_ms?: number | null
  max_ms?: number | null
  samples?: number
  failures?: number
  entity_id?: string
}

interface DemoResult {
  user_id: string
  session_id: string
  steps: TransactionStep[]
  timeline: TimelineRow[]
  reference_ms: Record<string, number>
  repeats: number
  applied_p50_ms: number | null
  applied_max_ms: number | null
  oltp_probe: OltpImpact
  oltp_baseline: OltpImpact
}

interface SchemaStatus {
  keyspace: string
  tables: Record<string, string>
  ready: boolean
  note: string
}

interface ClearanceZone {
  zone_id: string
  zone_name: string
  severity: string
  capacity: number
  remaining: number
  holders: string[]
  consistent: boolean
}

interface ClearanceState {
  zones: ClearanceZone[]
  mismatched: string[]
}

interface ClearanceDemoResult {
  zone_id: string
  entity_ids: string[]
  steps: TransactionStep[]
  state: ClearanceState
  repeats: number
  grant_p50_ms: number | null
  grant_max_ms: number | null
  release_p50_ms: number | null
  release_max_ms: number | null
}

interface ContentionResult {
  zone_id: string
  capacity: number
  askers: number
  granted: number
  refused: number
  winners: string[]
  errors: string[]
  duration_ms: number
  zone: ClearanceZone | null
}

/**
 * How many applied transactions the p50 is taken over.  The three offered are not
 * arbitrary: the probe below reads four times a second, so only the longest of
 * them runs for long enough to say anything about the request path.
 */
const REPEAT_CHOICES = [
  { value: 20, label: '20', hint: 'one figure quickly' },
  { value: 100, label: '100', hint: 'a settled p50' },
  { value: 2000, label: '2000', hint: 'long enough for the probe' },
] as const

/**
 * How many drones ask for the same zone at once.  Each asker is a real fleet
 * asset rather than an invented identifier, and the answer must be the zone's
 * capacity at every one of the three.
 */
const ASKER_CHOICES = [8, 16, 32] as const

/**
 * The line under a step that says what it did or did not move.
 *
 * A clearance step has no timeline, so this reads the step's own ``state`` when it
 * carries one and the timeline row count when it does not.  Hard-coding the row
 * count here, as an earlier version did, reported "timeline: 0 rows" against every
 * clearance step, which was true and beside the point.
 */
function stateLine(step: TransactionStep): string {
  const state = step.state
  if (state && typeof state.capacity === 'number') {
    const holders = Array.isArray(state.holders) ? state.holders.length : 0
    return `${state.remaining} of ${state.capacity} slots left, ${holders} held`
  }
  return `timeline: ${step.timeline_rows} ${step.timeline_rows === 1 ? 'row' : 'rows'}`
}

function StepCard({ step, index }: { step: TransactionStep; index: number }) {
  const [showCql, setShowCql] = useState(false)
  // Three outcomes, not two.  An error is a failure; a step that did not apply is
  // not, and colouring the two alike would misreport the demo's own result.
  const outcome = step.error
    ? { icon: 'error', tone: 'text-tertiary', border: 'border-tertiary', label: 'failed' }
    : step.applied
      ? { icon: 'check_circle', tone: 'text-primary', border: 'border-primary', label: 'applied' }
      : { icon: 'block', tone: 'text-secondary', border: 'border-secondary', label: 'refused' }

  return (
    <li className={`rounded-lg border-l-4 ${outcome.border} bg-surface-variant/40 p-3`}>
      <div className="flex items-start gap-3">
        <MaterialIcon name={outcome.icon} className={`mt-0.5 shrink-0 text-lg ${outcome.tone}`} />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <span className="text-xs font-bold text-on-surface">
              {index + 1}. {step.action}
            </span>
            <span className={`text-[10px] font-bold uppercase tracking-wide ${outcome.tone}`}>
              {outcome.label}
            </span>
            <span className="text-[10px] tabular-nums text-on-surface-variant">
              {formatMs(step.duration_ms)}
            </span>
            <span className="text-[10px] tabular-nums text-on-surface-variant">
              {stateLine(step)}
            </span>
          </div>
          {step.reason && <p className="mt-1 text-xs text-secondary">{step.reason}</p>}
          {step.error && <p className="mt-1 break-words text-xs text-tertiary">{step.error}</p>}
          {Object.keys(step.projection).length > 0 && (
            <p className="mt-1 font-mono text-[10px] text-on-surface-variant">
              {Object.entries(step.projection)
                .map(([key, value]) => `${key} = ${value ?? 'null'}`)
                .join('   ')}
            </p>
          )}
          {step.cql && (
            <>
              <button
                type="button"
                onClick={() => setShowCql((shown) => !shown)}
                className="mt-1 text-[10px] font-bold uppercase tracking-wide text-primary hover:underline"
              >
                {showCql ? 'hide' : 'show'} the statement
              </button>
              {showCql && (
                <pre className="mt-2 overflow-x-auto rounded bg-black/40 p-2 font-mono text-[10px] leading-relaxed text-on-surface-variant">
                  {step.cql}
                </pre>
              )}
            </>
          )}
        </div>
      </div>
    </li>
  )
}

function Figure({ label, value, note }: { label: string; value: string; note?: string }) {
  return (
    <div className="rounded-lg bg-surface-variant/40 p-3">
      <div className="text-[10px] font-bold uppercase tracking-wide text-on-surface-variant">
        {label}
      </div>
      <div className="mt-1 text-lg font-bold tabular-nums text-on-surface">{value}</div>
      {note && <div className="text-[10px] text-on-surface-variant">{note}</div>}
    </div>
  )
}

/** One zone's ledger: slots left, who holds them, and whether the two add up. */
function ZoneCard({ zone }: { zone: ClearanceZone }) {
  const held = zone.capacity - zone.remaining
  return (
    <div className="rounded-lg bg-surface-variant/40 p-3">
      <div className="flex flex-wrap items-baseline gap-x-3">
        <span className="text-xs font-bold text-on-surface">{zone.zone_name}</span>
        <span className="text-[10px] uppercase tracking-wide text-on-surface-variant">
          {zone.severity}
        </span>
        <span
          className={`ml-auto text-[10px] font-bold uppercase tracking-wide ${
            zone.consistent ? 'text-primary' : 'text-tertiary'
          }`}
          title="capacity == remaining + holders"
        >
          {zone.consistent ? 'consistent' : 'inconsistent'}
        </span>
      </div>
      <div className="mt-2 flex items-baseline gap-2">
        <span className="text-lg font-bold tabular-nums text-on-surface">{zone.remaining}</span>
        <span className="text-[10px] text-on-surface-variant">
          of {zone.capacity} slots left, {held} held
        </span>
      </div>
      {/* One box per slot, filled for a held one.  A capacity of two or three reads
          faster as slots than as a fraction. */}
      <div className="mt-2 flex gap-1">
        {Array.from({ length: zone.capacity }, (_, slot) => (
          <span
            key={slot}
            className={`h-2 w-6 rounded-sm ${slot < held ? 'bg-secondary' : 'bg-outline-variant'}`}
          />
        ))}
      </div>
      <p className="mt-2 font-mono text-[10px] text-on-surface-variant">
        {zone.holders.length > 0 ? zone.holders.join('  ') : 'no holders'}
      </p>
    </div>
  )
}

/** The session projection: a stream of events turned into one ordered timeline. */
function SessionDemo() {
  const [repeats, setRepeats] = useState<number>(100)
  const [result, setResult] = useState<DemoResult | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const { data: schema } = useQuery<SchemaStatus>({
    queryKey: ['transaction-schema'],
    queryFn: () => getJson<SchemaStatus>('/api/transactions/session/schema'),
    refetchInterval: 30_000,
  })

  const run = async () => {
    setRunning(true)
    setError(null)
    try {
      setResult(
        await postJson<DemoResult>(
          `/api/transactions/session/demo?repeats=${repeats}&probe=true`,
        ),
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setRunning(false)
    }
  }

  const reference = result?.reference_ms ?? {}
  const probe = result?.oltp_probe ?? {}
  const baseline = result?.oltp_baseline ?? {}

  return (
    <div className="space-y-6">
      {schema && !schema.ready && (
        <div className="rounded-xl border border-tertiary bg-tertiary/10 p-4">
          <div className="flex items-center gap-2 text-sm font-bold text-tertiary">
            <MaterialIcon name="warning" className="text-lg" />
            These tables do not accept transactions
          </div>
          <p className="mt-2 text-xs text-on-surface-variant">{schema.note}</p>
          <ul className="mt-2 space-y-1">
            {Object.entries(schema.tables).map(([table, status]) => (
              <li key={table} className="font-mono text-[10px] text-on-surface-variant">
                {table}: {status}
              </li>
            ))}
          </ul>
        </div>
      )}

      <Panel
        title="Run the sequence"
        subtitle="Each run opens a session of its own, so nothing it does can collide with another run or with the sink."
      >
        <div className="flex flex-wrap items-end gap-4">
          <div>
            <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-on-surface-variant">
              Applied transactions timed
            </div>
            <div className="flex gap-2">
              {REPEAT_CHOICES.map((choice) => (
                <button
                  key={choice.value}
                  type="button"
                  onClick={() => setRepeats(choice.value)}
                  title={choice.hint}
                  className={`rounded-lg px-3 py-2 text-xs font-bold transition ${
                    repeats === choice.value
                      ? 'bg-primary text-on-primary'
                      : 'bg-surface-variant text-on-surface-variant hover:bg-surface-variant/70'
                  }`}
                >
                  {choice.label}
                </button>
              ))}
            </div>
          </div>
          <button
            type="button"
            onClick={run}
            disabled={running || schema?.ready === false}
            className="flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-xs font-bold text-on-primary transition hover:opacity-90 disabled:opacity-40"
          >
            <MaterialIcon name={running ? 'hourglass_top' : 'play_arrow'} className="text-base" />
            {running ? 'running…' : 'Run'}
          </button>
          <p className="text-[10px] text-on-surface-variant">
            The timed repeats go into a separate session, so the six steps below stay readable.
          </p>
        </div>
        {error && (
          <p className="mt-3 break-words rounded-lg bg-tertiary/10 p-3 text-xs text-tertiary">
            {error}
          </p>
        )}
      </Panel>

      {result && (
        <>
          <Panel
            title="The sequence"
            subtitle={`session ${result.user_id} / ${result.session_id}`}
          >
            <ol className="space-y-2">
              {result.steps.map((step, index) => (
                <StepCard key={index} step={step} index={index} />
              ))}
            </ol>
          </Panel>

          <Panel
            title="The projection"
            subtitle="What session_timeline holds after all six steps: one row per applied sequence number, and nothing from the two that were refused."
          >
            {result.timeline.length === 0 ? (
              <p className="text-xs text-on-surface-variant">No rows.</p>
            ) : (
              <table className="w-full text-left text-xs">
                <thead className="text-[10px] uppercase tracking-wide text-on-surface-variant">
                  <tr>
                    <th className="pb-2 pr-4">seq</th>
                    <th className="pb-2 pr-4">event_id</th>
                    <th className="pb-2 pr-4">event_time</th>
                    <th className="pb-2">event_type</th>
                  </tr>
                </thead>
                <tbody className="font-mono text-on-surface">
                  {result.timeline.map((row) => (
                    <tr key={row.seq} className="border-t border-outline-variant/40">
                      <td className="py-1 pr-4 tabular-nums">{row.seq}</td>
                      <td className="py-1 pr-4">{row.event_id}</td>
                      <td className="py-1 pr-4">{row.event_time}</td>
                      <td className="py-1">{row.event_type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Panel>

          <Panel
            title="What it costs"
            subtitle={`Over ${result.repeats} applied transactions, against the same row written two other ways into a table that is not transactional.  Every one of them writes at QUORUM, so what is compared is the write path and not the consistency level.`}
          >
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <Figure
                label="Transaction p50"
                value={formatMs(result.applied_p50_ms)}
                note={`max ${formatMs(result.applied_max_ms)}`}
              />
              <Figure
                label="Lightweight txn p50"
                value={formatMs(reference.lwt_if_not_exists_p50_ms)}
                note={`max ${formatMs(reference.lwt_if_not_exists_max_ms)}`}
              />
              <Figure
                label="Plain INSERT p50"
                value={formatMs(reference.plain_insert_p50_ms)}
                note={`max ${formatMs(reference.plain_insert_max_ms)}`}
              />
              <Figure
                label="Point read, idle → during"
                value={`${formatMs(baseline.p50_ms)} → ${formatMs(probe.p50_ms)}`}
                note={`${probe.samples ?? 0} samples, ${probe.failures ?? 0} failures`}
              />
            </div>
            <p className="mt-3 text-xs text-on-surface-variant">
              The point-read figures are the same probe the compare page uses: one asset read four
              times a second, over an idle window and then while the transactions run.  At 20 or
              100 repeats the run is over before the probe has taken three samples, so only the
              2000-repeat run says anything about the request path.
            </p>
          </Panel>
        </>
      )}
    </div>
  )
}

/** Airspace clearance: a semaphore across partitions, which is admission control. */
function ClearanceDemo() {
  const [result, setResult] = useState<ClearanceDemoResult | null>(null)
  const [contention, setContention] = useState<ContentionResult | null>(null)
  const [askers, setAskers] = useState<number>(16)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const { data: state, refetch } = useQuery<ClearanceState>({
    queryKey: ['clearance-state'],
    queryFn: () => getJson<ClearanceState>('/api/transactions/clearance/state'),
    refetchInterval: 15_000,
  })

  const call = async (run: () => Promise<void>) => {
    setBusy(true)
    setError(null)
    try {
      await run()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
      refetch()
    }
  }

  // The scripted sequence asks for no timed repeats.  The seven steps are the
  // argument and each of them reads the ledger afterwards, so a p50 taken here
  // would be a p50 over statements the reader is not looking at.
  const runDemo = () =>
    call(async () => {
      setContention(null)
      setResult(
        await postJson<ClearanceDemoResult>('/api/transactions/clearance/demo?repeats=100'),
      )
    })

  const runContention = () =>
    call(async () => {
      setResult(null)
      setContention(
        await postJson<ContentionResult>(
          `/api/transactions/clearance/contend?askers=${askers}`,
        ),
      )
    })

  const reset = () =>
    call(async () => {
      setResult(null)
      setContention(null)
      await postJson('/api/transactions/clearance/reset')
    })

  const zones = result?.state.zones ?? state?.zones ?? []
  const mismatched = result?.state.mismatched ?? state?.mismatched ?? []

  return (
    <div className="space-y-6">
      <Panel
        title="The ledger"
        subtitle="Three zones the map draws, each with a capacity the sink seeded.  A clearance is written into the zone's partition and the drone's at once, so the two sides must agree, and the panel says whether they do rather than assuming it."
      >
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {zones.map((zone) => (
            <ZoneCard key={zone.zone_id} zone={zone} />
          ))}
        </div>
        {mismatched.length > 0 && (
          <p className="mt-3 rounded-lg bg-tertiary/10 p-3 text-xs text-tertiary">
            These holders' own rows name a different zone, or none:{' '}
            <span className="font-mono">{mismatched.join(', ')}</span>
          </p>
        )}
        <p className="mt-3 text-xs text-on-surface-variant">
          The count runs down rather than up: a grant does <span className="font-mono">SET
          remaining -= 1</span> under a guard that <span className="font-mono">remaining &gt;
          0</span>.  Counting up would need the transaction to compare one LET reference to
          another, and Accord refuses that with a bare{' '}
          <span className="font-mono">IllegalArgumentException null</span>.
        </p>
      </Panel>

      <Panel
        title="Run it"
        subtitle="Seven steps, of which only three may change anything: grant, replay the grant, ask for a second zone while holding one, take the last slot, ask for a full zone, release, release again."
      >
        <div className="flex flex-wrap items-end gap-4">
          <button
            type="button"
            onClick={runDemo}
            disabled={busy}
            className="flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-xs font-bold text-on-primary transition hover:opacity-90 disabled:opacity-40"
          >
            <MaterialIcon name={busy ? 'hourglass_top' : 'play_arrow'} className="text-base" />
            {busy ? 'running…' : 'Run the seven steps'}
          </button>
          <div>
            <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-on-surface-variant">
              Drones asking at once
            </div>
            <div className="flex gap-2">
              {ASKER_CHOICES.map((choice) => (
                <button
                  key={choice}
                  type="button"
                  onClick={() => setAskers(choice)}
                  className={`rounded-lg px-3 py-2 text-xs font-bold transition ${
                    askers === choice
                      ? 'bg-primary text-on-primary'
                      : 'bg-surface-variant text-on-surface-variant hover:bg-surface-variant/70'
                  }`}
                >
                  {choice}
                </button>
              ))}
            </div>
          </div>
          <button
            type="button"
            onClick={runContention}
            disabled={busy}
            className="flex items-center gap-2 rounded-lg bg-secondary px-4 py-2 text-xs font-bold text-on-secondary transition hover:opacity-90 disabled:opacity-40"
          >
            <MaterialIcon name="groups" className="text-base" />
            Contend
          </button>
          <button
            type="button"
            onClick={reset}
            disabled={busy}
            className="rounded-lg bg-surface-variant px-3 py-2 text-xs font-bold text-on-surface-variant transition hover:bg-surface-variant/70 disabled:opacity-40"
          >
            Release every clearance
          </button>
        </div>
        {error && (
          <p className="mt-3 break-words rounded-lg bg-tertiary/10 p-3 text-xs text-tertiary">
            {error}
          </p>
        )}
      </Panel>

      {result && (
        <>
          <Panel title="The sequence" subtitle={`zone ${result.zone_id}`}>
            <ol className="space-y-2">
              {result.steps.map((step, index) => (
                <StepCard key={index} step={step} index={index} />
              ))}
            </ol>
          </Panel>

          <Panel
            title="What it costs"
            subtitle={`Over ${result.repeats} grant and release pairs on the quietest zone, so every one of them takes the applied path.  Two figures rather than one: a grant reads two partitions and writes three, a release reads one and writes three.`}
          >
            <div className="grid gap-3 sm:grid-cols-2">
              <Figure
                label="Grant p50"
                value={formatMs(result.grant_p50_ms)}
                note={`max ${formatMs(result.grant_max_ms)}`}
              />
              <Figure
                label="Release p50"
                value={formatMs(result.release_p50_ms)}
                note={`max ${formatMs(result.release_max_ms)}`}
              />
            </div>
          </Panel>
        </>
      )}

      {contention && (
        <Panel
          title="Under contention"
          subtitle="The claim the scripted steps cannot make on their own: they run one after another, so nothing in them rules out a counter read and written back outside consensus.  Here the asks overlap, and the number of winners has to be the capacity exactly."
        >
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Figure label="Asked" value={String(contention.askers)} note="all at once" />
            <Figure
              label="Granted"
              value={String(contention.granted)}
              note={`capacity ${contention.capacity}`}
            />
            <Figure label="Refused" value={String(contention.refused)} note="the expected outcome" />
            <Figure
              label="Errors"
              value={String(contention.errors.length)}
              note={formatMs(contention.duration_ms)}
            />
          </div>
          <p
            className={`mt-3 rounded-lg p-3 text-xs ${
              contention.granted === contention.capacity && contention.errors.length === 0
                ? 'bg-primary/10 text-primary'
                : 'bg-tertiary/10 text-tertiary'
            }`}
          >
            {contention.granted === contention.capacity && contention.errors.length === 0
              ? `Exactly ${contention.capacity} of ${contention.askers} got in, and the ledger still adds up.`
              : `${contention.granted} got in against a capacity of ${contention.capacity}, with ${contention.errors.length} errors.  That is the interesting result, and it is reported rather than hidden.`}
          </p>
          <p className="mt-2 font-mono text-[10px] text-on-surface-variant">
            winners: {contention.winners.join('  ') || 'none'}
          </p>
          {contention.errors.length > 0 && (
            <ul className="mt-2 space-y-1">
              {contention.errors.map((message, index) => (
                <li key={index} className="break-words font-mono text-[10px] text-tertiary">
                  {message}
                </li>
              ))}
            </ul>
          )}
          <p className="mt-3 text-xs text-on-surface-variant">
            The winners differ between runs, which is what shows the asks genuinely contended
            rather than being serialised by the client.
          </p>
        </Panel>
      )}
    </div>
  )
}

/** Which of the two Accord demonstrations is on screen. */
const DEMOS = [
  { key: 'session', label: 'Session projection', icon: 'timeline' },
  { key: 'clearance', label: 'Airspace clearance', icon: 'flight_takeoff' },
] as const

export default function AccordPanel() {
  const [demo, setDemo] = useState<(typeof DEMOS)[number]['key']>('session')

  return (
    <div className="space-y-6">
      <p className="max-w-4xl text-sm text-on-surface-variant">
        The other pages read the same rows five ways.  This one writes.  A transaction here reads
        several tables with different partition keys and writes some of them, applying only if
        every guard holds.  A batch cannot do that, because a batch is atomic but not conditional;
        a lightweight transaction cannot either, because it conditions on one partition.
      </p>
      <p className="max-w-4xl text-sm text-on-surface-variant">
        Two demonstrations.  The first turns a stream of events into one ordered projection, and
        refuses a replay and an out-of-order step.  The second admits drones into restricted
        airspace up to a capacity, which is the claim that needs concurrent askers to make.
      </p>

      <div className="flex w-fit flex-wrap gap-1 rounded-xl border border-white/5 bg-surface-container p-1">
        {DEMOS.map((item) => (
          <button
            key={item.key}
            type="button"
            onClick={() => setDemo(item.key)}
            className={`flex items-center gap-2 rounded-lg px-5 py-2 text-xs font-bold uppercase tracking-wide transition-colors ${
              demo === item.key
                ? 'bg-surface-container-highest text-primary'
                : 'text-on-surface-variant hover:text-on-surface'
            }`}
          >
            <MaterialIcon name={item.icon} className="text-base" />
            {item.label}
          </button>
        ))}
      </div>

      {demo === 'session' ? <SessionDemo /> : <ClearanceDemo />}
    </div>
  )
}
