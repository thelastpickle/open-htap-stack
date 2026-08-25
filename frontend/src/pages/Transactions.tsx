import { useSearchParams } from 'react-router-dom'
import MaterialIcon from '../components/MaterialIcon'
import AccordPanel from './transactions/AccordPanel'
import SqlPanel from './transactions/SqlPanel'

/**
 * Transactions: two subtabs over one keyspace pair.
 *
 * Accord and cassandra-sql were two pages, and neither filled one.  Each is an
 * interface that writes rather than reads, which is what separates both of them from
 * Explore; putting them together is what makes the difference between them legible,
 * since one is consensus in CQL and the other is a SQL dialect above the same
 * storage.
 *
 * The subtab lives in the query string rather than in component state, so a link to
 * one of them is a link a reader can send.  ``/transactions`` alone opens Accord.
 */

const TABS = [
  {
    key: 'accord',
    label: 'Accord',
    icon: 'account_tree',
    hint: 'Transactions across partitions, in CQL',
  },
  {
    key: 'sql',
    label: 'SQL',
    icon: 'terminal',
    hint: "Postgres-dialect SQL over Cassandra, on cassandra-sql's own tables",
  },
] as const

type TabKey = (typeof TABS)[number]['key']

function isTabKey(value: string | null): value is TabKey {
  return TABS.some((tab) => tab.key === value)
}

export default function TransactionsPage() {
  const [params, setParams] = useSearchParams()
  const requested = params.get('tab')
  const tab: TabKey = isTabKey(requested) ? requested : 'accord'

  // `replace` rather than a push, so switching subtab does not fill the reader's
  // back button with the same page three times.
  const select = (key: TabKey) => setParams({ tab: key }, { replace: true })

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-bold text-on-surface">Transactions</h1>
        <p className="mt-2 max-w-4xl text-sm text-on-surface-variant">
          Every other page here reads.  These two write: Accord for conditional writes whose
          conditions live in other partitions, and cassandra-sql for the joins and
          multi-statement transactions CQL has no answer to.
        </p>
      </header>

      <div className="flex w-fit flex-wrap gap-1 rounded-xl border border-white/5 bg-surface-container p-1">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            onClick={() => select(item.key)}
            title={item.hint}
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

      {tab === 'accord' && <AccordPanel />}
      {tab === 'sql' && <SqlPanel />}
    </div>
  )
}
