/**
 * A titled section, with room for the sentence that says why it is there.
 *
 * Extracted because the Transactions page and the SQL console had declared it
 * separately and identically, and the two are now subtabs of one page: two copies
 * of the same card would drift the moment one of them gained a border.
 */
export default function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle?: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-xl border border-outline-variant bg-surface p-5">
      <h2 className="text-sm font-bold uppercase tracking-wide text-on-surface">{title}</h2>
      {subtitle && <p className="mt-1 text-xs text-on-surface-variant">{subtitle}</p>}
      <div className="mt-4">{children}</div>
    </section>
  )
}
