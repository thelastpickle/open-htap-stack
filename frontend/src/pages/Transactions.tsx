import AccordPanel from './transactions/AccordPanel'

/**
 * Transactions: the writes the other pages cannot make.
 *
 * Every other page here reads.  This one writes, and what it writes is a conditional
 * write whose condition lives in other partitions, which is the thing neither a CQL
 * batch nor a lightweight transaction can express.
 *
 * The body is a component of its own rather than this file, because a demonstration
 * with its own state and its own controls is long enough to read on its own.
 */

export default function TransactionsPage() {
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-bold text-on-surface">Transactions</h1>
        <p className="mt-2 max-w-4xl text-sm text-on-surface-variant">
          Accord conditions a write on rows in other partitions, and refuses it when they do
          not hold.  Both demonstrations below report the refusals as refusals, and prove they
          changed nothing by counting the rows afterwards.
        </p>
      </header>

      <AccordPanel />
    </div>
  )
}
