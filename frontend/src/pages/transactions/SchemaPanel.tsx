import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../../components/MaterialIcon'
import Panel from '../../components/Panel'
import { getJson } from '../../lib/api'

/**
 * The two schemas this stack holds, each read from the engine that owns it.
 *
 * Nothing on the dashboard answered "what is the data model" before this.  The demo
 * keyspace was described in docs/DATA-MODEL.md and in a comment in the sink, and
 * neither is visible from a browser; cassandra-sql's own tables were described
 * nowhere.
 *
 * Two requests rather than one, because the two engines fail apart: Cassandra can be
 * up while cassandra-sql is down, and one call would blank the half that still
 * answers.  Both are read fresh on every visit and nothing is held here, because a
 * catalog on the SQL side goes stale: pg_tables still lists tables that were
 * dropped.
 */

interface SchemaColumn {
  name: string
  type: string
  kind: string
  position: number
  clustering_order: string
}

interface SchemaIndex {
  name: string
  table: string
  detail: string
  target: string
}

interface SchemaTable {
  name: string
  columns: SchemaColumn[]
  transactional_mode: string
  row_count: number | null
  create_statement: string
  note: string
}

interface SchemaView {
  engine: string
  keyspace: string
  tables: SchemaTable[]
  indexes: SchemaIndex[]
  storage_keyspaces: string[]
  warnings: string[]
  error: string | null
}

/** The key, written the way CQL writes it: ((partition), clustering). */
function keyOf(table: SchemaTable): string {
  const partition = table.columns.filter((column) => column.kind === 'partition_key')
  const clustering = table.columns.filter((column) => column.kind === 'clustering')
  if (partition.length === 0) return ''
  const head = `(${partition.map((column) => column.name).join(', ')})`
  const tail = clustering.map(
    (column) => `${column.name}${column.clustering_order === 'desc' ? ' desc' : ''}`,
  )
  return `PRIMARY KEY (${[head, ...tail].join(', ')})`
}

/** What a column's role is worth showing as, and in what colour. */
function kindTone(kind: string): string {
  if (kind === 'partition_key' || kind === 'primary key') return 'text-primary'
  if (kind === 'clustering') return 'text-secondary'
  if (kind === 'static') return 'text-tertiary'
  return 'text-on-surface-variant'
}

function ColumnList({ columns }: { columns: SchemaColumn[] }) {
  return (
    <table className="w-full text-left text-xs">
      <thead className="text-[10px] uppercase tracking-wide text-on-surface-variant">
        <tr>
          <th className="pb-1 pr-4">column</th>
          <th className="pb-1 pr-4">type</th>
          <th className="pb-1">role</th>
        </tr>
      </thead>
      <tbody className="font-mono text-on-surface">
        {columns.map((column) => (
          <tr key={column.name} className="border-t border-outline-variant/40">
            <td className="py-1 pr-4">{column.name}</td>
            <td className="py-1 pr-4 text-on-surface-variant">{column.type}</td>
            <td className={`py-1 ${kindTone(column.kind)}`}>
              {column.kind}
              {column.kind === 'clustering' && column.clustering_order !== 'none'
                ? ` ${column.clustering_order}`
                : ''}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function TableCard({ table, showMode }: { table: SchemaTable; showMode: boolean }) {
  const [open, setOpen] = useState(false)
  const accord = table.transactional_mode === 'full'

  return (
    <li className={`rounded-lg border-l-4 ${accord ? 'border-primary' : 'border-outline-variant'} bg-surface-variant/40 p-3`}>
      <button
        type="button"
        onClick={() => setOpen((wasOpen) => !wasOpen)}
        className="flex w-full items-baseline gap-x-3 gap-y-1 text-left"
      >
        <MaterialIcon
          name={open ? 'expand_more' : 'chevron_right'}
          className="shrink-0 text-base text-on-surface-variant"
        />
        <span className="font-mono text-xs font-bold text-on-surface">{table.name}</span>
        {showMode && (
          <span
            className={`text-[10px] font-bold uppercase tracking-wide ${
              accord ? 'text-primary' : 'text-on-surface-variant'
            }`}
            title="transactional_mode, parsed out of DESCRIBE TABLE"
          >
            accord {table.transactional_mode || 'unknown'}
          </span>
        )}
        <span className="text-[10px] tabular-nums text-on-surface-variant">
          {table.columns.length} columns
        </span>
        {table.row_count != null && (
          <span className="text-[10px] tabular-nums text-on-surface-variant">
            {table.row_count} {table.row_count === 1 ? 'row' : 'rows'}
          </span>
        )}
        <span className="ml-auto shrink-0 truncate font-mono text-[10px] text-on-surface-variant">
          {keyOf(table)}
        </span>
      </button>
      {table.note && <p className="mt-1 break-words text-[10px] text-tertiary">{table.note}</p>}
      {open && (
        <div className="mt-3 space-y-3">
          <ColumnList columns={table.columns} />
          {table.create_statement && (
            <pre className="overflow-x-auto rounded bg-black/40 p-2 font-mono text-[10px] leading-relaxed text-on-surface-variant">
              {table.create_statement.trim()}
            </pre>
          )}
        </div>
      )}
    </li>
  )
}

function IndexList({ indexes }: { indexes: SchemaIndex[] }) {
  if (indexes.length === 0) {
    return <p className="text-xs text-on-surface-variant">None reported.</p>
  }
  return (
    <ul className="space-y-1">
      {indexes.map((index) => (
        <li key={`${index.table}.${index.name}`} className="font-mono text-[10px] text-on-surface-variant">
          <span className="text-on-surface">{index.name}</span> on {index.table}
          {index.target && ` (${index.target})`} — {index.detail}
        </li>
      ))}
    </ul>
  )
}

function Warnings({ warnings, error }: { warnings: string[]; error: string | null }) {
  if (error) {
    return (
      <p className="break-words rounded-lg bg-tertiary/10 p-3 text-xs text-tertiary">{error}</p>
    )
  }
  if (warnings.length === 0) return null
  return (
    <ul className="space-y-2">
      {warnings.map((warning, index) => (
        <li key={index} className="text-xs text-on-surface-variant">
          <MaterialIcon name="info" className="mr-1 align-[-3px] text-sm text-secondary" />
          {warning}
        </li>
      ))}
    </ul>
  )
}

export default function SchemaPanel() {
  const { data: cql, isLoading: cqlLoading } = useQuery<SchemaView>({
    queryKey: ['schema-cql'],
    queryFn: () => getJson<SchemaView>('/api/schema/cql'),
    refetchInterval: 60_000,
  })

  const { data: sql, isLoading: sqlLoading } = useQuery<SchemaView>({
    queryKey: ['schema-sql'],
    queryFn: () => getJson<SchemaView>('/api/schema/sql'),
    refetchInterval: 60_000,
  })

  const accordTables = (cql?.tables ?? []).filter(
    (table) => table.transactional_mode === 'full',
  ).length

  return (
    <div className="space-y-6">
      <p className="max-w-4xl text-sm text-on-surface-variant">
        Two schemas, and they share no row.  Cassandra's{' '}
        <span className="font-mono">{cql?.keyspace ?? 'demo'}</span> keyspace holds everything the
        other pages read, and everything the Accord subtab writes.  cassandra-sql holds its own
        five tables under an ordered key-value encoding of its own, in three keyspaces of its own,
        and cannot read the first.
      </p>
      <p className="max-w-4xl text-sm text-on-surface-variant">
        Each side is read from the engine that owns it rather than from a document beside the code.
        On the CQL side that is one <span className="font-mono">DESCRIBE KEYSPACE</span> for the
        CREATE statements and <span className="font-mono">system_schema.columns</span> for the
        keys.  On the SQL side it is <span className="font-mono">pg_class</span> and{' '}
        <span className="font-mono">pg_attribute</span>, joined here rather than in SQL, because
        joining them in that engine would be using one of the defects the SQL subtab documents.
      </p>

      <Panel
        title={`Cassandra — the ${cql?.keyspace ?? 'demo'} keyspace`}
        subtitle={
          cql
            ? `${cql.tables.length} tables, of which ${accordTables} route every read and write through Accord.  Click a table for its columns and its whole CREATE statement.`
            : 'reading…'
        }
      >
        {cqlLoading && <p className="text-xs text-on-surface-variant">reading…</p>}
        <Warnings warnings={cql?.warnings ?? []} error={cql?.error ?? null} />
        {cql && cql.tables.length > 0 && (
          <ul className="mt-3 space-y-2">
            {cql.tables.map((table) => (
              <TableCard key={table.name} table={table} showMode />
            ))}
          </ul>
        )}
        {cql && (
          <div className="mt-4">
            <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-on-surface-variant">
              Indexes
            </div>
            <IndexList indexes={cql.indexes} />
            <p className="mt-2 text-xs text-on-surface-variant">
              One index, and it is a storage-attached index over a vector column, which is the
              only vector index Cassandra has.  It is also what fixes the partitioner: a
              storage-attached index refuses every partitioner but Murmur3.
            </p>
          </div>
        )}
      </Panel>

      <Panel
        title="cassandra-sql — its own tables"
        subtitle={
          sql
            ? `${sql.tables.length} tables, encoded into ${sql.storage_keyspaces.join(', ')}.  Every one of them is an Accord table: it is the engine that decides, not the table, so there is no per-table mode to show.`
            : 'reading…'
        }
      >
        {sqlLoading && <p className="text-xs text-on-surface-variant">reading…</p>}
        <Warnings warnings={sql?.warnings ?? []} error={sql?.error ?? null} />
        {sql && sql.tables.length > 0 && (
          <ul className="mt-3 space-y-2">
            {sql.tables.map((table) => (
              <TableCard key={table.name} table={table} showMode={false} />
            ))}
          </ul>
        )}
        {sql && (
          <div className="mt-4">
            <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-on-surface-variant">
              Indexes, as its catalog reports them
            </div>
            <IndexList indexes={sql.indexes} />
            <p className="mt-2 text-xs text-on-surface-variant">
              Two catalogs describe these and they disagree with each other, so what is shown is
              one of them filtered by the tables that exist.  The types are the engine's own
              resolution and not the declaration: a{' '}
              <span className="font-mono">DECIMAL(10,2)</span> column reads back as{' '}
              <span className="font-mono">text</span>, which is why every number on the SQL subtab
              is compared through a float.
            </p>
          </div>
        )}
      </Panel>
    </div>
  )
}
