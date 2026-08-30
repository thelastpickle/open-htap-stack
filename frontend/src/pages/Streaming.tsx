import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import Panel from '../components/Panel'
import RatePicker from '../components/RatePicker'
import { formatCount, formatMs, getJson } from '../lib/api'

/**
 * The Streaming page: the mutations Cassandra's commit log gave the Sidecar, as they
 * arrive on Kafka.
 *
 * It shows a window rather than a log.  The backend keeps a fixed number of records
 * and this page renders that window at whatever rate is chosen above it, so watching
 * the stream costs the same whether the page has been open for a minute or a day, and
 * no record here is one the dashboard is keeping.  What holds every mutation is the
 * topic, which is the point: the demo's other five paths read the rows, and this one
 * is told about the writes.
 */

interface CdcRecord {
  seq: number
  partition: number
  offset: number
  key: string
  keyspace: string
  table: string
  operation: string
  mutation_at_ms: number
  kafka_at_ms: number
  age_ms: number | null
  backfill: boolean
  partial: boolean
  columns: Record<string, unknown>
  update_fields: string[]
  schema_id: number | null
  decode_error: string | null
}

interface CdcStreamStatus {
  state: string
  topic: string
  bootstrap: string
  registry: string
  partitions: number[]
  buffer_size: number
  buffered: number
  consumed: number
  decode_failures: number
  rate_per_sec: number
  latency_p50_ms: number | null
  latency_max_ms: number | null
  schema_ids: number[]
  last_record_at_ms: number | null
  error: string | null
}

interface CdcStreamResponse {
  status: CdcStreamStatus
  records: CdcRecord[]
}

interface CdcSchemaView {
  subject: string
  schema_id: number | null
  version: number | null
  fields: { name?: string; type?: unknown }[]
  payload_fields: { name?: string; avro_type?: string | null; cql_type?: string | null }[]
  registry: string
  avro_schema: Record<string, unknown> | null
  error: string | null
}

interface TailState {
  label: string
  colour: string
  note: string
}

/** Shown until the backend has said what the tail is doing, and for any state added
 *  to it that this page has not been taught. */
const STARTING: TailState = {
  label: 'Starting',
  colour: 'text-on-surface-variant',
  note: 'The consumer has not attached yet.',
}

/** What each tail state means, in the operator's terms rather than the code's. */
const STATES: Record<string, TailState> = {
  tailing: {
    label: 'Tailing',
    colour: 'text-positive',
    note: 'The consumer is reading the topic.',
  },
  waiting_for_topic: {
    label: 'Waiting for the topic',
    colour: 'text-secondary',
    note: 'The Sidecar creates the topic when it publishes its first mutation.',
  },
  starting: STARTING,
  error: {
    label: 'Error',
    colour: 'text-tertiary',
    note: 'The consumer will attach again; the reason is below.',
  },
}

/** An operation's colour: a write, a change and a removal each read differently. */
const OPERATION_STYLES: Record<string, string> = {
  INSERT: 'text-primary bg-primary/10',
  UPDATE: 'text-secondary bg-secondary/10',
  DELETE: 'text-tertiary bg-tertiary/10',
  DELETE_RANGE: 'text-tertiary bg-tertiary/10',
  DELETE_PARTITION: 'text-tertiary bg-tertiary/10',
  COMPLEX_ELEMENT_DELETE: 'text-tertiary bg-tertiary/10',
}

/**
 * The refresh rates this page offers, fastest last.
 *
 * A tick reads a slice of the buffer the backend already holds: no Cassandra and no
 * Kafka call, so the cost is one response to serialise and one table to redraw.  That
 * is what lets the default sit at the fastest of these rather than the slowest.
 */
const RATES = [1000, 500, 250] as const

function clockOf(ms: number): string {
  if (!ms) return '—'
  return new Date(ms).toISOString().slice(11, 23)
}

/** One decoded column value, rendered without pretending a blob is text. */
function cellOf(value: unknown): string {
  if (value == null) return 'null'
  if (typeof value === 'object') {
    const base64 = (value as { base64?: string }).base64
    if (typeof base64 === 'string') return `${base64.slice(0, 24)}… (base64)`
    return JSON.stringify(value)
  }
  if (typeof value === 'number') return String(value)
  return String(value)
}

function Figure({
  label,
  value,
  note,
  colour,
}: {
  label: string
  value: string
  note?: string
  colour?: string
}) {
  return (
    <div className="rounded-lg border border-outline-variant bg-surface-container px-4 py-3">
      <p className="text-on-surface-variant text-[9px] font-bold uppercase tracking-wider">
        {label}
      </p>
      <p className={`font-headline mt-1 text-xl font-black tabular-nums ${colour ?? ''}`}>{value}</p>
      {note && <p className="text-on-surface-variant/70 mt-1 text-[9px]">{note}</p>}
    </div>
  )
}

/** The record itself: one line, expandable to the columns the mutation carried. */
function RecordRow({ record }: { record: CdcRecord }) {
  const [open, setOpen] = useState(false)
  const columns = Object.entries(record.columns)
  const style = OPERATION_STYLES[record.operation] ?? 'text-on-surface-variant bg-white/5'
  // The Avro record has a field per column of the table and leaves the untouched ones
  // null, so the envelope's updateFields is what says which columns the mutation wrote.
  // The summary line shows those; without them it shows whatever the record carried.
  const named = new Set(record.update_fields)
  const summary = named.size ? columns.filter(([name]) => named.has(name)) : columns

  return (
    <li className="border-b border-white/5 last:border-b-0">
      <button
        type="button"
        onClick={() => setOpen((wasOpen) => !wasOpen)}
        className="flex w-full items-center gap-3 px-3 py-2 text-left transition-colors hover:bg-white/5"
      >
        <MaterialIcon
          name={open ? 'expand_more' : 'chevron_right'}
          className="text-on-surface-variant shrink-0 text-[16px]"
        />
        <span className="text-on-surface-variant w-24 shrink-0 font-mono text-[10px] tabular-nums">
          {clockOf(record.mutation_at_ms)}
        </span>
        <span
          className={`w-28 shrink-0 rounded px-2 py-0.5 text-center text-[9px] font-black uppercase tracking-wider ${style}`}
        >
          {record.operation || '—'}
        </span>
        <span className="w-48 shrink-0 truncate font-mono text-[10px]">
          {record.keyspace}.{record.table}
        </span>
        <span className="text-on-surface-variant min-w-0 flex-1 truncate font-mono text-[10px]">
          {summary.length
            ? summary
                .slice(0, 4)
                .map(([name, value]) => `${name}=${cellOf(value)}`)
                .join('  ')
            : record.key}
        </span>
        {record.partial && (
          <span
            className="text-secondary bg-secondary/10 shrink-0 rounded px-1.5 py-0.5 text-[9px] font-bold uppercase"
            title="The Sidecar published this mutation before every replica's copy of it had been seen"
          >
            Partial
          </span>
        )}
        {record.backfill ? (
          <span
            className="text-on-surface-variant/70 w-20 shrink-0 text-right text-[9px] uppercase tracking-wider"
            title="Read to fill the buffer when the consumer attached, so its age would measure the backlog rather than the pipeline"
          >
            Backfill
          </span>
        ) : (
          <span className="w-20 shrink-0 text-right font-mono text-[10px] tabular-nums">
            {formatMs(record.age_ms)}
          </span>
        )}
      </button>

      {open && (
        <div className="bg-black/20 px-3 py-3 pl-10">
          {record.decode_error ? (
            <p className="text-tertiary font-mono text-[10px]">{record.decode_error}</p>
          ) : columns.length === 0 ? (
            <p className="text-on-surface-variant text-[10px]">
              No columns: a partition or range delete names what it removed and carries no values.
            </p>
          ) : (
            <dl className="grid grid-cols-[minmax(0,12rem)_1fr] gap-x-4 gap-y-1">
              {columns.map(([name, value]) => (
                <div key={name} className="contents">
                  <dt
                    className={`truncate font-mono text-[10px] ${
                      named.has(name) ? 'text-primary' : 'text-on-surface-variant/50'
                    }`}
                    title={
                      named.has(name)
                        ? 'The mutation named this column'
                        : 'Not named by the mutation; the record carries a null for it'
                    }
                  >
                    {name}
                  </dt>
                  <dd
                    className={`break-all font-mono text-[10px] ${
                      named.has(name) ? '' : 'text-on-surface-variant/50'
                    }`}
                  >
                    {cellOf(value)}
                  </dd>
                </div>
              ))}
            </dl>
          )}
          <p className="text-on-surface-variant/60 mt-3 font-mono text-[9px]">
            key {record.key || '—'} · partition {record.partition} · offset {record.offset} · schema{' '}
            {record.schema_id ?? '—'} · broker {clockOf(record.kafka_at_ms)}
          </p>
        </div>
      )}
    </li>
  )
}

/** The contract the records are written against, read from the registry by subject. */
function SchemaPanel() {
  const [open, setOpen] = useState(false)
  const { data } = useQuery<CdcSchemaView>({
    queryKey: ['cdc-schema'],
    queryFn: () => getJson<CdcSchemaView>('/api/streaming/cdc/schema'),
    // A schema changes when the table does, so this needs no live poll.
    refetchInterval: 60_000,
  })

  if (!data) return null

  return (
    <Panel
      title="The Avro schema"
      subtitle="Each record carries the id of its schema, not the schema; the registry holds the schema. This is what the topic's subject resolves to."
    >
      {data.error ? (
        <p className="text-on-surface-variant text-xs">{data.error}</p>
      ) : (
        <>
          <div className="flex flex-wrap items-center gap-x-6 gap-y-1 font-mono text-[10px]">
            <span>
              <span className="text-on-surface-variant">subject </span>
              {data.subject}
            </span>
            <span>
              <span className="text-on-surface-variant">id </span>
              {data.schema_id ?? '—'}
            </span>
            <span>
              <span className="text-on-surface-variant">version </span>
              {data.version ?? '—'}
            </span>
            <span>
              <span className="text-on-surface-variant">envelope fields </span>
              {data.fields.length - (data.payload_fields.length ? 1 : 0)}
            </span>
            <span>
              <span className="text-on-surface-variant">columns </span>
              {data.payload_fields.length}
            </span>
          </div>
          <button
            type="button"
            onClick={() => setOpen((wasOpen) => !wasOpen)}
            className="text-primary mt-3 flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider"
          >
            <MaterialIcon name={open ? 'expand_less' : 'expand_more'} className="text-[14px]" />
            {open ? 'Hide the fields' : 'Show the fields'}
          </button>
          {open && (
            <>
              <ul className="mt-3 grid gap-x-6 gap-y-1 sm:grid-cols-2 lg:grid-cols-3">
                {data.fields
                  .filter((field) => field.name !== 'payload')
                  .map((field, index) => (
                    <li key={`${field.name}-${index}`} className="font-mono text-[10px]">
                      <span className="text-secondary">{field.name}</span>{' '}
                      <span className="text-on-surface-variant">
                        {typeof field.type === 'string' ? field.type : JSON.stringify(field.type)}
                      </span>
                    </li>
                  ))}
              </ul>
              {/* The row itself is one nested record inside that envelope, and its fields
                  are the table's columns.  Shown apart, with the CQL type the publisher
                  converted from, because Avro's `long` says less than `timestamp` does. */}
              {data.payload_fields.length > 0 && (
                <>
                  <p className="text-on-surface-variant mt-4 text-[9px] font-bold uppercase tracking-wider">
                    The payload record: the table's own columns
                  </p>
                  <ul className="mt-2 grid gap-x-6 gap-y-1 sm:grid-cols-2 lg:grid-cols-3">
                    {data.payload_fields.map((field, index) => (
                      <li key={`${field.name}-${index}`} className="font-mono text-[10px]">
                        <span className="text-primary">{field.name}</span>{' '}
                        <span className="text-on-surface-variant">
                          {field.cql_type ?? field.avro_type ?? '—'}
                        </span>
                      </li>
                    ))}
                  </ul>
                </>
              )}
            </>
          )}
        </>
      )}
      <p className="text-on-surface-variant/70 mt-4 text-[10px]">
        Registry: <span className="font-mono">{data.registry}</span>. Apicurio Registry serves the
        Confluent-compatible endpoint the Sidecar's serializer writes to, so the wire format is
        Confluent's and the licence is Apache 2.0.
      </p>
    </Panel>
  )
}

export default function StreamingPage() {
  // Pausing stops this page refetching.  It does not stop the consumer, which keeps
  // reading the topic, so resuming shows the latest window and not the one that was
  // frozen: pausing is for reading a record, not for holding the stream.
  const [paused, setPaused] = useState(false)
  const [limit, setLimit] = useState(50)
  // The fastest of the offered rates, because a tick reads no table.
  const [rateMs, setRateMs] = useState<(typeof RATES)[number]>(250)

  const { data, isLoading } = useQuery<CdcStreamResponse>({
    queryKey: ['cdc-stream', limit],
    // The endpoint takes a `since` parameter and this page does not use it.  Asking for
    // only the records minted after the last one seen sounds like the way to make a
    // faster poll cheaper, and at the rate the demo publishes it saves nothing: the
    // buffer holds a fraction of a second of the topic, so at any of the rates offered
    // here the whole window is new anyway and `since` would return a full `limit` of
    // rows while adding a merge on this side.  The rate is not in the query key because
    // it does not change the response, where `limit` does.
    queryFn: () => getJson<CdcStreamResponse>(`/api/streaming/cdc?limit=${limit}`),
    refetchInterval: paused ? false : rateMs,
  })

  const status = data?.status
  const records = data?.records ?? []
  const state = STATES[status?.state ?? 'starting'] ?? STARTING

  return (
    <section className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="mb-2 flex items-center gap-3">
            <span
              className={`h-2 w-2 rounded-full ${
                status?.state === 'tailing' ? 'bg-positive animate-pulse' : 'bg-secondary'
              }`}
            />
            <span className="font-label text-secondary text-[0.6875rem] font-bold uppercase tracking-widest">
              Change data capture
            </span>
          </div>
          <h1 className="font-headline text-4xl font-black uppercase tracking-tighter">Streaming</h1>
          <p className="text-on-surface-variant mt-2 max-w-3xl text-xs leading-relaxed">
            Cassandra hard-links each commit log segment into <code>cdc_raw</code> as it discards it.
            The Sidecar reads those segments and publishes the mutations of{' '}
            <code>demo.drone_latest_status</code> to Kafka as Avro. Nothing here queries Cassandra:
            these are the writes, not a read of what they left behind.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => setPaused((wasPaused) => !wasPaused)}
            className={`flex items-center gap-2 rounded-lg px-4 py-2 text-[10px] font-black uppercase tracking-wider transition-colors ${
              paused
                ? 'bg-primary text-on-primary'
                : 'bg-surface-container text-on-surface-variant hover:text-primary'
            }`}
            title={
              paused
                ? 'Resume the view; the consumer never stopped'
                : 'Freeze the view so a record can be read; the consumer keeps reading'
            }
          >
            <MaterialIcon name={paused ? 'play_arrow' : 'pause'} className="text-[14px]" />
            {paused ? 'Resume' : 'Freeze view'}
          </button>
          <RatePicker
            value={rateMs}
            options={RATES}
            onChange={setRateMs}
            title="How often this page rereads the buffer; it reads no table, so a faster rate costs one response and one redraw"
          />
          <select
            value={limit}
            onChange={(event) => setLimit(Number(event.target.value))}
            className="rounded-lg border border-outline-variant bg-surface-container px-3 py-2 text-[10px] font-bold uppercase tracking-wider"
            title="How many of the buffered records to show"
          >
            {[25, 50, 100, 200].map((option) => (
              <option key={option} value={option}>
                {option} rows
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <Figure
          label="State"
          value={state.label}
          note={state.note}
          colour={state.colour}
        />
        <Figure
          label="Consume rate"
          value={status ? `${formatCount(Math.round(status.rate_per_sec))}/s` : '—'}
          note="This page's own tail, between two polls; the topic's end offsets are the publish rate"
        />
        <Figure
          label="Latency p50"
          value={formatMs(status?.latency_p50_ms)}
          note={`Write to Kafka append, so it measures the publisher; max ${formatMs(status?.latency_max_ms)}`}
        />
        <Figure
          label="Window"
          value={status ? `${status.buffered} / ${status.buffer_size}` : '—'}
          note={`Kept, of ${formatCount(status?.consumed)} consumed`}
        />
        <Figure
          label="Decode failures"
          value={formatCount(status?.decode_failures)}
          note="Kept in the window with the reason, not dropped"
          colour={status?.decode_failures ? 'text-tertiary' : undefined}
        />
      </div>

      {status?.error && (
        <div className="border-tertiary bg-tertiary/5 rounded-r-lg border-l-4 p-4">
          <p className="text-tertiary text-[10px] font-black uppercase tracking-wider">
            The consumer is not reading
          </p>
          <p className="text-on-surface-variant mt-1 font-mono text-[10px]">{status.error}</p>
        </div>
      )}

      <Panel
        title="Latest mutations"
        subtitle="Newest first. Select a row for the columns the mutation carried and where it sat on the topic. Age is the mutation's own write time against the clock here, so it is the whole path: Cassandra write, commit log discard, Sidecar publish, Kafka, decode."
      >
        {isLoading ? (
          <div className="flex items-center gap-3 py-10">
            <MaterialIcon name="sync" className="text-secondary animate-spin text-2xl" />
            <p className="text-on-surface-variant text-[10px] font-black uppercase tracking-widest">
              Attaching to the topic…
            </p>
          </div>
        ) : records.length === 0 ? (
          <div className="py-10 text-center">
            <MaterialIcon name="stream" className="text-on-surface-variant/40 mb-3 text-5xl" />
            <p className="font-headline text-sm font-bold uppercase tracking-wide">
              No mutations yet
            </p>
            <p className="text-on-surface-variant mx-auto mt-2 max-w-xl text-xs leading-relaxed">
              {state.note} The topic does not exist until the Sidecar creates it with its first
              batch, so a fresh stack publishes nothing for a while: measured at 95.7 s from{' '}
              <code>up -d</code> to the first record, most of it the node and the Sidecar starting.
            </p>
          </div>
        ) : (
          <>
            <div className="text-on-surface-variant flex items-center gap-3 px-3 pb-2 text-[9px] font-bold uppercase tracking-wider">
              <span className="w-4 shrink-0" />
              <span className="w-24 shrink-0">Write time</span>
              <span className="w-28 shrink-0 text-center">Operation</span>
              <span className="w-48 shrink-0">Table</span>
              <span className="min-w-0 flex-1">Columns</span>
              <span className="w-20 shrink-0 text-right">Age</span>
            </div>
            <ul className="rounded-lg border border-outline-variant bg-surface-container">
              {records.map((record) => (
                <RecordRow key={record.seq} record={record} />
              ))}
            </ul>
          </>
        )}
        {status && (
          <p className="text-on-surface-variant/60 mt-4 font-mono text-[9px]">
            topic {status.topic} · partitions {status.partitions.join(', ') || '—'} · broker{' '}
            {status.bootstrap} · schema ids {status.schema_ids.join(', ') || '—'}
          </p>
        )}
      </Panel>

      <SchemaPanel />
    </section>
  )
}
