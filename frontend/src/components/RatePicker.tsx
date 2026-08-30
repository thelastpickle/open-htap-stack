/**
 * How often a live page refetches, chosen by whoever is watching it.
 *
 * Shared rather than declared twice, because the two pages that offer it want the
 * same control and quite different rates: a tick of the CDC stream is a slice of a
 * buffer this process already holds, and a tick of the fleet map is a scan of
 * Cassandra on the request path.  So each caller passes its own options and its own
 * `title` saying what a tick costs there, and neither page hides the cost behind a
 * literal at the call site.
 *
 * A dropdown rather than a rate per button, because a button per rate grows the header
 * with the list: the map offers four and sits beside three filters and a theme toggle,
 * and the stream sits beside the freeze button and the row count, which is a dropdown
 * already.  The selected rate is what a watcher needs to see, and a dropdown shows it
 * in one control however many are on offer.
 *
 * Generic over the option type so a caller can hold its rate as the union of its own
 * options, as the Overview page holds its history window: dropping a rate from the
 * array then fails the build rather than leaving a state initialiser nothing selects.
 */
function formatRate(ms: number): string {
  return ms >= 1000 ? `${ms / 1000}s` : `${ms}ms`
}

export default function RatePicker<T extends number>({
  value,
  options,
  onChange,
  title,
}: {
  value: T
  options: readonly T[]
  onChange: (ms: T) => void
  title?: string
}) {
  return (
    // A label around the select rather than beside it, so "Every" names the dropdown
    // to a screen reader without either caller inventing an id for it.
    <label className="flex items-center gap-2" title={title}>
      <span className="text-on-surface-variant text-[10px] font-bold uppercase tracking-wider">
        Every
      </span>
      <select
        value={value}
        // The option values are numbers and a select reports strings, so the choice is
        // read back out of `options` rather than parsed: that keeps the callback on the
        // caller's own union type.
        onChange={(event) => {
          const chosen = options.find((option) => String(option) === event.target.value)
          if (chosen !== undefined) onChange(chosen)
        }}
        className="rounded border border-outline-variant bg-surface-container-highest text-on-surface-variant hover:text-primary px-2 py-1.5 text-[10px] font-bold uppercase tracking-wider transition-colors"
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {formatRate(option)}
          </option>
        ))}
      </select>
    </label>
  )
}
