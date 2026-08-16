import { useEffect, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import Toast, { useToast } from '../components/Toast'
import { getJson, postJson } from '../lib/api'

interface DemoSettings {
  drones_enabled: number
  events_per_sec: number
  outlier_percent: number
  paused: boolean
}

interface DemoSettingsResponse {
  settings: DemoSettings
  message: string
}

interface BreachScenario {
  entity_id: string
  message: string
}

/** Every control here changes what the data producer generates. */
const SLIDERS = [
  {
    key: 'drones_enabled' as const,
    label: 'Fleet size',
    hint: 'Assets emitting telemetry. Capped by the producer’s MAX_ENTITIES.',
    min: 10,
    max: 2000,
    step: 10,
    format: (v: number) => v.toLocaleString(),
  },
  {
    key: 'events_per_sec' as const,
    label: 'Events per second',
    hint: 'Total ingest rate across the fleet.',
    min: 50,
    max: 5000,
    step: 50,
    format: (v: number) => v.toLocaleString(),
  },
  {
    key: 'outlier_percent' as const,
    label: 'Overheating assets',
    hint: 'Share of the fleet running an anomalous internal temperature, for the outlier queries on Explore.',
    min: 0,
    max: 25,
    step: 0.5,
    format: (v: number) => `${v}%`,
  },
]

export default function SettingsPage() {
  const queryClient = useQueryClient()
  const { toast, show, dismiss } = useToast()
  const [form, setForm] = useState<DemoSettings | null>(null)

  // The form is loaded from the server once.  Re-syncing on every poll would
  // snatch a slider back while it was being dragged.
  const loaded = useRef(false)

  const { data } = useQuery<DemoSettingsResponse>({
    queryKey: ['demo-settings'],
    queryFn: () => getJson<DemoSettingsResponse>('/api/settings/demo'),
  })

  useEffect(() => {
    if (data?.settings && !loaded.current) {
      setForm(data.settings)
      loaded.current = true
    }
  }, [data])

  const save = useMutation({
    mutationFn: (settings: DemoSettings) =>
      postJson<DemoSettingsResponse>('/api/settings/demo', settings),
    onSuccess: (response) => {
      setForm(response.settings)
      show(response.message)
      queryClient.invalidateQueries({ queryKey: ['demo-settings'] })
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  const restoreDefaults = useMutation({
    mutationFn: () => getJson<DemoSettingsResponse>('/api/settings/demo/defaults'),
    onSuccess: (response) => {
      setForm(response.settings)
      show('Startup defaults loaded — apply them to take effect', 'info')
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  const togglePause = useMutation({
    mutationFn: () => postJson<DemoSettingsResponse>('/api/settings/demo/pause'),
    onSuccess: (response) => {
      setForm((previous) => (previous ? { ...previous, paused: response.settings.paused } : previous))
      show(response.message)
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  const clearFleetState = useMutation({
    mutationFn: () => postJson<{ success: boolean; message: string }>('/api/settings/demo/cleanup'),
    onSuccess: (response) => {
      show(response.message, response.success ? 'success' : 'error')
      queryClient.invalidateQueries({ queryKey: ['kpis'] })
      queryClient.invalidateQueries({ queryKey: ['map-live'] })
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  const triggerBreach = useMutation({
    mutationFn: () => postJson<BreachScenario>('/api/demo/trigger-breach-scenario'),
    onSuccess: (response) => {
      show(response.message, 'info')
      queryClient.invalidateQueries({ queryKey: ['kpis'] })
      queryClient.invalidateQueries({ queryKey: ['map-live'] })
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
    },
    onError: (e: Error) => show(e.message, 'error'),
  })

  return (
    <section>
      {toast && <Toast toast={toast} onDismiss={dismiss} />}

      <div className="mb-2 flex items-center gap-3">
        <span className="bg-primary h-2 w-2 animate-pulse rounded-full" />
        <span className="font-label text-primary-dim text-[0.6875rem] font-bold uppercase tracking-widest">
          Demo controls
        </span>
      </div>
      <h1 className="font-headline mb-2 text-4xl font-black uppercase tracking-tighter">Settings</h1>
      <p className="text-on-surface-variant mb-8 max-w-2xl text-xs leading-relaxed">
        These values live in the backend’s memory; the data producer polls them and adopts them
        within a few seconds. Restarting the backend restores whatever the compose file declared.
      </p>

      <div className="grid grid-cols-1 gap-8 lg:grid-cols-2">
        <div className="glass-panel rounded-xl p-8">
          <h2 className="font-headline mb-6 text-lg font-bold uppercase tracking-wide">
            Data generation
          </h2>
          {form === null ? (
            <p className="text-on-surface-variant text-xs">Loading current settings…</p>
          ) : (
            <div className="space-y-8">
              {SLIDERS.map((slider) => (
                <div key={slider.key}>
                  <div className="mb-2 flex items-baseline justify-between gap-4">
                    <label
                      htmlFor={slider.key}
                      className="text-on-surface-variant text-[0.6875rem] font-medium uppercase tracking-wider"
                    >
                      {slider.label}
                    </label>
                    <span className="text-primary text-xs font-bold tabular-nums">
                      {slider.format(form[slider.key])}
                    </span>
                  </div>
                  <input
                    id={slider.key}
                    type="range"
                    min={slider.min}
                    max={slider.max}
                    step={slider.step}
                    value={form[slider.key]}
                    onChange={(e) => setForm({ ...form, [slider.key]: Number(e.target.value) })}
                    className="accent-primary w-full"
                  />
                  <p className="text-on-surface-variant/60 mt-1 text-[10px] leading-relaxed">
                    {slider.hint}
                  </p>
                </div>
              ))}

              <button
                onClick={() => form && save.mutate(form)}
                disabled={save.isPending}
                className="bg-primary hover:bg-primary-dim text-on-primary font-headline flex w-full cursor-pointer items-center justify-center gap-2 rounded px-6 py-3 font-bold tracking-wider transition-all active:scale-95 disabled:opacity-60"
              >
                <MaterialIcon
                  name={save.isPending ? 'sync' : 'save'}
                  className={save.isPending ? 'animate-spin text-[16px]' : 'text-[16px]'}
                />
                {save.isPending ? 'Applying…' : 'Apply to the running stack'}
              </button>
              <button
                onClick={() => restoreDefaults.mutate()}
                disabled={restoreDefaults.isPending}
                className="bg-surface-container-highest border-primary/20 text-primary/70 hover:text-primary font-headline flex w-full cursor-pointer items-center justify-center gap-2 rounded border px-6 py-3 font-bold tracking-wider transition-all active:scale-95"
              >
                <MaterialIcon name="restart_alt" className="text-[16px]" /> Load startup defaults
              </button>
            </div>
          )}
        </div>

        <div className="glass-panel rounded-xl p-8">
          <h2 className="font-headline mb-6 text-lg font-bold uppercase tracking-wide">Actions</h2>
          <div className="space-y-4">
            <button
              onClick={() => togglePause.mutate()}
              disabled={togglePause.isPending || form === null}
              className={`font-headline flex w-full cursor-pointer items-center justify-center gap-3 rounded-lg border-2 px-6 py-5 text-base font-black uppercase tracking-wider transition-all active:scale-95 ${
                form?.paused
                  ? 'border-secondary text-secondary bg-secondary/10'
                  : 'border-primary/60 text-primary hover:border-primary bg-primary/5'
              }`}
            >
              <MaterialIcon
                name={form?.paused ? 'play_circle' : 'pause_circle'}
                className="text-[22px]"
              />
              {form?.paused ? 'Resume data generation' : 'Pause data generation'}
            </button>
            {form?.paused && (
              <p className="text-secondary/80 text-center text-xs font-medium">
                The producer is paused. Ingestion stops; stored data stays put.
              </p>
            )}

            <div className="rounded bg-surface-container p-4">
              <p className="font-headline text-sm font-bold uppercase tracking-tight">
                Zone breach scenario
              </p>
              <p className="text-on-surface-variant mt-1 text-xs leading-relaxed">
                Marks a real airborne asset as breaching and writes a matching alert to Cassandra.
                The map, the KPIs and the alert feed all pick it up through their ordinary queries.
              </p>
              <button
                onClick={() => triggerBreach.mutate()}
                disabled={triggerBreach.isPending}
                className="bg-tertiary/15 hover:bg-tertiary/25 border-tertiary/40 text-tertiary font-headline mt-4 flex w-full cursor-pointer items-center justify-center gap-2 rounded border px-6 py-3 font-bold tracking-wider transition-all active:scale-95 disabled:opacity-60"
              >
                <MaterialIcon
                  name={triggerBreach.isPending ? 'sync' : 'crisis_alert'}
                  className={triggerBreach.isPending ? 'animate-spin text-[16px]' : 'text-[16px]'}
                />
                {triggerBreach.isPending ? 'Injecting…' : 'Trigger breach scenario'}
              </button>
            </div>

            <div className="rounded bg-surface-container p-4">
              <p className="font-headline text-sm font-bold uppercase tracking-tight">
                Clear fleet state
              </p>
              <p className="text-on-surface-variant mt-1 text-xs leading-relaxed">
                Truncates drone_latest_status. Use it after reducing the fleet size: retired assets
                keep their last row otherwise, and the KPIs keep counting them. Event history and the
                zones are untouched.
              </p>
              <button
                onClick={() => clearFleetState.mutate()}
                disabled={clearFleetState.isPending}
                className="bg-tertiary/10 hover:bg-tertiary/20 border-tertiary/30 text-tertiary/80 hover:text-tertiary font-headline mt-4 flex w-full cursor-pointer items-center justify-center gap-2 rounded border px-6 py-3 font-bold tracking-wider transition-all active:scale-95 disabled:opacity-60"
              >
                <MaterialIcon
                  name={clearFleetState.isPending ? 'sync' : 'delete_sweep'}
                  className={clearFleetState.isPending ? 'animate-spin text-[16px]' : 'text-[16px]'}
                />
                {clearFleetState.isPending ? 'Clearing…' : 'Truncate drone_latest_status'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
