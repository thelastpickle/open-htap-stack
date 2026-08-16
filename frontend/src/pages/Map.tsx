import { useEffect, useMemo, useState } from 'react'
import { MapContainer, Marker, Polygon, Polyline, Popup, TileLayer, useMap } from 'react-leaflet'
import { Icon, type LatLngExpression, type LatLngTuple } from 'leaflet'
import { useQuery } from '@tanstack/react-query'
import MaterialIcon from '../components/MaterialIcon'
import { getJson } from '../lib/api'

interface DronePosition {
  entity_id: string
  event_time: string
  latitude: number
  longitude: number
  altitude_m: number
  speed_mps: number
  heading_deg: number
  is_flying: boolean
  temp_internal_c: number
  temp_external_c: number
  near_restricted_zone: boolean
  predicted_zone_breach: boolean
  risk_score: number
}

interface RestrictedZone {
  zone_id: string
  zone_name: string
  polygon_wkt: string
  severity: string
}

interface MapLive {
  drones: DronePosition[]
  zones: RestrictedZone[]
  timestamp: string
}

interface Trail {
  entity_id: string
  points: { event_time: string; latitude: number; longitude: number; altitude_m: number }[]
}

type MapTheme = 'dark' | 'light'

// Marker colours are chosen per basemap: what reads well on a dark tile set
// disappears on a light one.
const THEMES = {
  dark: {
    tile: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    flying: '#00e5ff',
    warning: '#ffb300',
    danger: '#ff5252',
    grounded: '#90a4ae',
    trail: '#00e5ff',
  },
  light: {
    tile: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    flying: '#0d47a1',
    warning: '#e65100',
    danger: '#b71c1c',
    grounded: '#546e7a',
    trail: '#0d47a1',
  },
} as const

const TILE_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

const ZONE_COLOURS: Record<string, string> = {
  critical: '#ff5252',
  high: '#ff8f00',
  warning: '#ffb300',
}
const ZONE_FALLBACK_COLOUR = '#40c4ff'

const DEFAULT_CENTRE: LatLngTuple = [59.91, 10.75]
const FILTERS = ['all', 'flying', 'at risk'] as const
type Filter = (typeof FILTERS)[number]

function droneIcon(colour: string, grounded = false): Icon {
  const size = grounded ? 20 : 30
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
         stroke="${colour}" stroke-width="${grounded ? 1.5 : 2.2}">
      <circle cx="12" cy="12" r="${grounded ? 3 : 3.5}" fill="${colour}" fill-opacity="${grounded ? 0.35 : 0.9}"/>
      <path d="M12 2v4M12 18v4M2 12h4M18 12h4M5.6 5.6l2.8 2.8M15.6 15.6l2.8 2.8M5.6 18.4l2.8-2.8M15.6 8.4l2.8-2.8"
            ${grounded ? 'stroke-dasharray="3 2"' : ''}/>
    </svg>`
  return new Icon({
    iconUrl: `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
  })
}

function parseWktPolygon(wkt: string): LatLngTuple[] {
  const match = wkt.match(/\(\((.+?)\)\)/)
  if (!match?.[1]) return []
  return match[1]
    .split(',')
    .map((pair) => pair.trim().split(/\s+/).map(Number))
    .filter((coords): coords is number[] => coords.length >= 2 && coords.every(Number.isFinite))
    .map(([lon, lat]) => [lat, lon] as LatLngTuple) // WKT is (x, y); Leaflet wants (lat, lng)
}

/** Fits the view to the fleet on first load, and flies to a requested position. */
function ViewController({
  bounds,
  flyTo,
}: {
  bounds: [LatLngTuple, LatLngTuple] | null
  flyTo: LatLngTuple | null
}) {
  const map = useMap()
  const [hasFitted, setHasFitted] = useState(false)

  useEffect(() => {
    // The container is sized by CSS after mount, so Leaflet needs telling.
    const timer = setTimeout(() => map.invalidateSize(), 100)
    return () => clearTimeout(timer)
  }, [map])

  useEffect(() => {
    // Fit once: refitting on every poll would fight the operator's own panning.
    if (bounds && !hasFitted) {
      map.fitBounds(bounds, { padding: [50, 50] })
      setHasFitted(true)
    }
  }, [map, bounds, hasFitted])

  useEffect(() => {
    if (flyTo) map.flyTo(flyTo, 14, { duration: 1.5 })
  }, [map, flyTo])

  return null
}

export default function MapPage() {
  const [filter, setFilter] = useState<Filter>('all')
  const [theme, setTheme] = useState<MapTheme>('dark')
  const [selected, setSelected] = useState<string | null>(null)
  const [flyTo, setFlyTo] = useState<LatLngTuple | null>(null)

  const palette = THEMES[theme]
  const icons = useMemo(
    () => ({
      flying: droneIcon(palette.flying),
      warning: droneIcon(palette.warning),
      danger: droneIcon(palette.danger),
      grounded: droneIcon(palette.grounded, true),
    }),
    [palette],
  )

  // The Alerts page asks for a position by leaving one behind in sessionStorage.
  useEffect(() => {
    const stored = sessionStorage.getItem('mapFlyTo')
    if (!stored) return
    sessionStorage.removeItem('mapFlyTo')
    try {
      const { lat, lng, entityId } = JSON.parse(stored)
      if (Number.isFinite(lat) && Number.isFinite(lng)) setFlyTo([lat, lng])
      if (typeof entityId === 'string') setSelected(entityId)
    } catch {
      // A malformed hint is not worth interrupting the map for.
    }
  }, [])

  const { data, isLoading } = useQuery<MapLive>({
    queryKey: ['map-live'],
    queryFn: () => getJson<MapLive>('/api/map/live?limit=2000'),
    refetchInterval: 5000,
  })

  // The trail is the asset's recorded history, read from Cassandra on demand,
  // so it is only fetched while an asset is selected.
  const { data: trail } = useQuery<Trail>({
    queryKey: ['trail', selected],
    queryFn: () => getJson<Trail>(`/api/map/drone/${selected}/trail?points=80`),
    enabled: selected !== null,
    // Slower than the fleet poll: a path is a history, and each fetch is a
    // range scan rather than the single bounded scan behind the live positions.
    refetchInterval: 10000,
  })

  const drones = data?.drones ?? []
  const zones = data?.zones ?? []

  const counts = useMemo(
    () => ({
      all: drones.length,
      flying: drones.filter((d) => d.is_flying).length,
      'at risk': drones.filter((d) => d.near_restricted_zone || d.predicted_zone_breach).length,
    }),
    [drones],
  )

  const visible = useMemo(() => {
    if (filter === 'flying') return drones.filter((d) => d.is_flying)
    if (filter === 'at risk')
      return drones.filter((d) => d.near_restricted_zone || d.predicted_zone_breach)
    return drones
  }, [drones, filter])

  const zonePolygons = useMemo(
    () => zones.map((zone) => ({ ...zone, positions: parseWktPolygon(zone.polygon_wkt) })),
    [zones],
  )

  // Bounds covering the fleet and the zones, so the first view frames both.
  const bounds = useMemo<[LatLngTuple, LatLngTuple] | null>(() => {
    const points: LatLngTuple[] = [
      ...drones.filter((d) => d.latitude && d.longitude).map((d) => [d.latitude, d.longitude] as LatLngTuple),
      ...zonePolygons.flatMap((z) => z.positions),
    ]
    if (points.length === 0) return null
    const lats = points.map((p) => p[0])
    const lngs = points.map((p) => p[1])
    return [
      [Math.min(...lats), Math.min(...lngs)],
      [Math.max(...lats), Math.max(...lngs)],
    ]
  }, [drones, zonePolygons])

  const trailPositions = useMemo<LatLngExpression[]>(
    () => (trail?.points ?? []).map((p) => [p.latitude, p.longitude] as LatLngTuple),
    [trail],
  )

  const iconFor = (drone: DronePosition) => {
    if (drone.predicted_zone_breach) return icons.danger
    if (drone.near_restricted_zone) return icons.warning
    return drone.is_flying ? icons.flying : icons.grounded
  }

  return (
    <section>
      <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div>
          <div className="mb-2 flex items-center gap-3">
            <span className="bg-primary h-2 w-2 animate-pulse rounded-full" />
            <span className="font-label text-primary-dim text-[0.6875rem] font-bold uppercase tracking-widest">
              Live operations
            </span>
          </div>
          <h1 className="font-headline text-4xl font-black uppercase tracking-tighter">Fleet map</h1>
        </div>
        <div className="flex flex-wrap gap-2">
          {FILTERS.map((option) => (
            <button
              key={option}
              onClick={() => setFilter(option)}
              className={`rounded px-4 py-1.5 text-xs font-bold uppercase tracking-wider transition-colors ${
                filter === option
                  ? option === 'at risk'
                    ? 'bg-tertiary text-white'
                    : 'bg-primary text-on-primary'
                  : 'bg-surface-container-highest text-on-surface-variant hover:text-primary'
              }`}
            >
              {option} ({counts[option]})
            </button>
          ))}
          <button
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            className="bg-surface-container-highest text-on-surface-variant hover:text-primary flex items-center gap-1.5 rounded px-3 py-1.5 text-xs font-bold uppercase tracking-wider transition-colors"
          >
            <MaterialIcon
              name={theme === 'dark' ? 'light_mode' : 'dark_mode'}
              className="text-[14px]"
            />
            {theme === 'dark' ? 'Light' : 'Dark'} basemap
          </button>
        </div>
      </div>

      {selected && (
        <div className="border-primary/20 mb-4 flex flex-wrap items-center gap-3 rounded-lg border bg-surface-container-high px-4 py-2">
          <span className="bg-primary h-2 w-2 animate-pulse rounded-full" />
          <span className="text-primary text-[10px] font-black uppercase tracking-wider">
            Flight path
          </span>
          <span className="text-on-surface-variant text-[10px] font-medium">
            {selected} · {trail?.points.length ?? 0} recorded positions from
            drone_events_by_entity
          </span>
          <button
            onClick={() => setSelected(null)}
            className="text-on-surface-variant hover:text-tertiary ml-auto text-[10px] font-black uppercase tracking-wider transition-colors"
          >
            Clear
          </button>
        </div>
      )}

      <div className="glass-panel relative overflow-hidden rounded-xl" style={{ height: '75vh' }}>
        {isLoading ? (
          <div className="text-on-surface-variant flex h-full w-full items-center justify-center">
            <div className="text-center">
              <MaterialIcon name="sync" className="text-primary mb-4 animate-spin text-6xl" />
              <p className="font-headline text-lg font-bold uppercase tracking-wide">
                Loading fleet…
              </p>
            </div>
          </div>
        ) : (
          <MapContainer
            center={DEFAULT_CENTRE}
            zoom={11}
            style={{ width: '100%', height: '100%', zIndex: 1 }}
            zoomControl={false}
          >
            <ViewController bounds={bounds} flyTo={flyTo} />
            <TileLayer attribution={TILE_ATTRIBUTION} url={palette.tile} />

            {zonePolygons.map(
              (zone) =>
                zone.positions.length > 0 && (
                  <Polygon
                    key={zone.zone_id}
                    positions={zone.positions}
                    pathOptions={{
                      color: ZONE_COLOURS[zone.severity] ?? ZONE_FALLBACK_COLOUR,
                      fillColor: ZONE_COLOURS[zone.severity] ?? ZONE_FALLBACK_COLOUR,
                      fillOpacity: 0.2,
                      weight: 2,
                      dashArray: '6, 5',
                    }}
                  >
                    <Popup>
                      <strong>{zone.zone_name}</strong>
                      <br />
                      {zone.severity} restricted zone
                    </Popup>
                  </Polygon>
                ),
            )}

            {trailPositions.length > 1 && (
              <Polyline
                positions={trailPositions}
                pathOptions={{ color: palette.trail, weight: 2, opacity: 0.8 }}
              />
            )}

            {visible.map((drone) => {
              if (!drone.latitude || !drone.longitude) return null
              const isSelected = selected === drone.entity_id
              return (
                <Marker
                  key={drone.entity_id}
                  position={[drone.latitude, drone.longitude]}
                  icon={iconFor(drone)}
                  eventHandlers={{
                    click: () => setSelected(isSelected ? null : drone.entity_id),
                  }}
                >
                  <Popup>
                    <div style={{ minWidth: 200 }}>
                      <strong>{drone.entity_id}</strong>
                      <table style={{ width: '100%', fontSize: 11, marginTop: 6 }}>
                        <tbody>
                          {[
                            ['Status', drone.is_flying ? 'Flying' : 'Grounded'],
                            ['Speed', `${drone.speed_mps.toFixed(1)} m/s`],
                            ['Altitude', `${drone.altitude_m.toFixed(0)} m`],
                            ['Heading', `${drone.heading_deg.toFixed(0)}°`],
                            ['Risk', `${(drone.risk_score * 100).toFixed(0)}%`],
                            ['Temp internal', `${drone.temp_internal_c.toFixed(1)} °C`],
                            ['Temp external', `${drone.temp_external_c.toFixed(1)} °C`],
                          ].map(([key, value]) => (
                            <tr key={key}>
                              <td style={{ color: '#666', paddingRight: 8 }}>{key}</td>
                              <td style={{ fontWeight: 600 }}>{value}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {drone.predicted_zone_breach ? (
                        <p style={{ color: '#c62828', fontWeight: 700, marginTop: 6 }}>
                          Predicted zone breach
                        </p>
                      ) : (
                        drone.near_restricted_zone && (
                          <p style={{ color: '#ef6c00', fontWeight: 700, marginTop: 6 }}>
                            Near a restricted zone
                          </p>
                        )
                      )}
                      <button
                        onClick={() => setSelected(isSelected ? null : drone.entity_id)}
                        style={{ marginTop: 8, width: '100%', cursor: 'pointer' }}
                      >
                        {isSelected ? 'Hide flight path' : 'Show flight path'}
                      </button>
                    </div>
                  </Popup>
                </Marker>
              )
            })}
          </MapContainer>
        )}
      </div>

      <div className="text-on-surface-variant mt-4 flex flex-wrap gap-6 text-xs font-bold uppercase tracking-wider">
        {[
          { colour: palette.flying, label: 'Flying' },
          { colour: palette.grounded, label: 'Grounded' },
          { colour: palette.warning, label: 'Near zone' },
          { colour: palette.danger, label: 'Predicted breach' },
        ].map(({ colour, label }) => (
          <span key={label} className="flex items-center gap-2">
            <span className="h-3 w-3 rounded-full" style={{ background: colour }} />
            {label}
          </span>
        ))}
        {Object.entries(ZONE_COLOURS).map(([severity, colour]) => (
          <span key={severity} className="flex items-center gap-2">
            <span
              className="h-3 w-3 rounded-full border-2 border-dashed"
              style={{ borderColor: colour }}
            />
            {severity} zone
          </span>
        ))}
      </div>
    </section>
  )
}
