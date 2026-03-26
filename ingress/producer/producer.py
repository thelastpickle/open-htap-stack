#!/usr/bin/env python3
"""
High-throughput Kafka event producer with a stateful "fleet" telemetry generator.

Key goals for PoC demos:
- Thousands of entities per process (low per-entity state, numpy-backed).
- Smooth, realistic-ish motion (parametric flight-path style + small OU noise).
- External temperature depends on latitude + diurnal/seasonality + altitude lapse rate.
- Internal temperature follows external temp with operational heat + rare anomaly spikes.
- Optional text payload sampled as 1–3 adjacent sentences from a large local text file.
- Schema is *not* drone-specific (generic IoT telemetry fields).
"""

import os
import time
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Sequence, Tuple

import numpy as np

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from cassandra.util import uuid_from_time

try:
    import orjson  # much faster than json
except Exception:  # pragma: no cover
    orjson = None  # type: ignore


# -----------------------------
# Helpers / env
# -----------------------------

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or v == "" else v


def try_create_topic(bootstrap: str, topic: str, partitions: int = 12, replication: int = 1) -> None:
    """
    Best-effort topic creation. Safe if broker auto-create is enabled too.
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="topic-bootstrap")
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)])
        admin.close()
        print(f"[producer] created topic={topic} partitions={partitions} rf={replication}")
    except TopicAlreadyExistsError:
        print(f"[producer] topic already exists: {topic}")
    except Exception as e:
        print(f"[producer] topic create skipped/failed (ok for demo): {e}")


# -----------------------------
# Text sampler (optional)
# -----------------------------

class TextSampler:
    """
    Memory-maps a text file and samples 1–3 adjacent sentences near a random offset.

    This is designed for *throughput*: it does small bounded scans around a random point
    rather than splitting the whole corpus into sentences.

    Tip for very high EPS:
      - enable text caching per entity (default enabled below)
      - refresh text only every 5–30 seconds per entity (configurable)
    """

    def __init__(self, path: str, seed: int = 0, max_scan_back: int = 2048, max_scan_fwd: int = 4096):
        import mmap
        self.path = path
        self._fh = open(path, "rb")
        self._mm = mmap.mmap(self._fh.fileno(), 0, access=mmap.ACCESS_READ)
        self._size = self._mm.size()
        self._rng = np.random.default_rng(seed)
        self._back = max_scan_back
        self._fwd = max_scan_fwd

    def close(self) -> None:
        try:
            self._mm.close()
        finally:
            self._fh.close()

    def _find_sentence_start(self, pos: int) -> int:
        """
        Scan backward to find a plausible sentence boundary.
        """
        start = max(0, pos - self._back)
        window = self._mm[start:pos]
        # Look for sentence terminators/newlines. If none, start at window start.
        for i in range(len(window) - 1, -1, -1):
            c = window[i]
            if c in (ord("."), ord("?"), ord("!"), ord("\n")):
                return start + i + 1
        return start

    def _take_sentences(self, pos: int, n_sent: int) -> str:
        """
        Scan forward until we have n_sent sentence terminators.
        """
        end = min(self._size, pos + self._fwd)
        window = self._mm[pos:end]
        found = 0
        cut = len(window)
        for i, c in enumerate(window):
            if c in (ord("."), ord("?"), ord("!")):
                found += 1
                if found >= n_sent:
                    cut = i + 1
                    break
        snippet = window[:cut]
        # Decode permissively; corpora like enwik8/enwik9 include markup and odd bytes.
        return snippet.decode("utf-8", errors="ignore").strip()

    def sample(self, n_sent: Optional[int] = None) -> str:
        if self._size < 32:
            return ""
        if n_sent is None:
            n_sent = int(self._rng.integers(1, 4))  # 1..3
        pos = int(self._rng.integers(0, self._size))
        start = self._find_sentence_start(pos)
        txt = self._take_sentences(start, n_sent)
        # Keep it clean-ish (optional): collapse whitespace
        return " ".join(txt.split())


# -----------------------------
# Event types for IoT telemetry
# -----------------------------

EVENT_TYPES = [
    "telemetry_update",
    "position_report",
    "temperature_reading",
    "status_check",
    "health_monitor",
    "sensor_data",
    "diagnostic_report",
    "performance_metric",
    "environmental_scan",
    "system_heartbeat",
    "operational_status",
    "maintenance_alert",
    "calibration_check",
    "power_status",
    "connectivity_test",
    "data_sync",
    "threshold_check",
    "routine_inspection",
    "compliance_report",
    "activity_log",
]


# -----------------------------
# Fleet model
# -----------------------------

@dataclass(frozen=True)
class FleetConfig:
    n_entities: int = 5000
    seed: int = 42

    # Path shape parameters
    scale_m_min: float = 500.0
    scale_m_max: float = 50_000.0
    speed_mps_min: float = 6.0
    speed_mps_max: float = 24.0
    drift_frac_max: float = 0.30     # fraction of speed used for linear drift (for "distance covering")

    # OU positional noise (meters)
    pos_noise_tau_s: float = 10.0
    pos_noise_sigma_m: float = 1.5

    # Altitude (meters)
    alt_base_min: float = 30.0
    alt_base_max: float = 200.0
    alt_amp_min: float = 0.0
    alt_amp_max: float = 80.0
    alt_period_min_s: float = 40.0
    alt_period_max_s: float = 180.0
    alt_noise_tau_s: float = 25.0
    alt_noise_sigma_m: float = 0.6

    # Temperature
    isa_lapse_c_per_km: float = 6.5
    internal_tau_s: float = 45.0
    internal_base_delta_c: float = 8.0
    internal_load_delta_c: float = 12.0
    internal_noise_sigma_c: float = 0.05

    # Rare internal anomaly
    anomaly_rate_per_s: float = 2e-6     # ~0.17/day per entity at 2e-6
    anomaly_dur_min_s: float = 10.0
    anomaly_dur_max_s: float = 60.0
    anomaly_delta_min_c: float = 15.0
    anomaly_delta_max_c: float = 40.0


class FleetState:
    """
    Low-state generator for many entities.

    Each entity has:
      - origin lat/lon
      - smooth parametric XY path (two harmonics + drift)
      - smooth altitude oscillation
      - OU noise for XY and altitude (small deviations)
      - internal temperature state + rare anomaly window
      - cached text payload (optional)
    """

    def __init__(self, cfg: FleetConfig):
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.t0 = time.time()
        n = cfg.n_entities

        self._initialize_geographic_origins(n)
        self._initialize_observer_assignments(n)
        self._initialize_motion_parameters(n)
        self._initialize_altitude_parameters(n)
        self._initialize_noise_states(n)
        self._initialize_tracking_states(n)
        self._initialize_temperature_states(n)
        self._initialize_anomaly_states(n)
        self._initialize_text_cache(n)

    def _initialize_geographic_origins(self, n: int) -> None:
        """Initialize entity origin coordinates and coordinate conversion factors."""
        # Origins: keep away from poles for numeric stability (demo convenience)
        self.lat0 = self.rng.uniform(-70.0, 70.0, n).astype(np.float64)
        self.lon0 = self.rng.uniform(-180.0, 180.0, n).astype(np.float64)
        
        # Coordinate conversion factors
        self.inv_m_per_deg_lat = 1.0 / 111_320.0
        self.inv_m_per_deg_lon = 1.0 / (111_320.0 * np.cos(np.deg2rad(self.lat0)).clip(0.2, None))

    def _initialize_observer_assignments(self, n: int) -> None:
        """Assign observers to entities with geographic affinity (80% regional, 20% random)."""
        # Observer assignment: 1:100 observer:entity ratio
        n_observers = max(1, n // 100)
        
        # Define geographic regions (simple lat/lon grid)
        n_regions = max(1, n_observers // 2)  # ~2 observers per region on average
        region_lat_bounds = np.linspace(-70.0, 70.0, int(np.sqrt(n_regions)) + 1)
        region_lon_bounds = np.linspace(-180.0, 180.0, int(np.sqrt(n_regions)) + 1)
        
        # Assign each entity to a region based on its origin
        entity_region_lat_idx = np.searchsorted(region_lat_bounds[:-1], self.lat0, side='right') - 1
        entity_region_lon_idx = np.searchsorted(region_lon_bounds[:-1], self.lon0, side='right') - 1
        entity_region_lat_idx = np.clip(entity_region_lat_idx, 0, len(region_lat_bounds) - 2)
        entity_region_lon_idx = np.clip(entity_region_lon_idx, 0, len(region_lon_bounds) - 2)
        
        # Create observer pool per region
        observers_per_region = {}
        for lat_idx in range(len(region_lat_bounds) - 1):
            for lon_idx in range(len(region_lon_bounds) - 1):
                region_key = (lat_idx, lon_idx)
                # Assign 1-3 observers per region
                n_obs_in_region = min(3, max(1, n_observers // n_regions))
                observers_per_region[region_key] = [
                    f"observer-{self.rng.integers(0, n_observers):04d}"
                    for _ in range(n_obs_in_region)
                ]
        
        # Assign observer_id to each entity (80% geographically bound, 20% random)
        self.observer_ids = []
        for i in range(n):
            if self.rng.random() < 0.8:  # 80% geographically bound
                region_key = (int(entity_region_lat_idx[i]), int(entity_region_lon_idx[i]))
                regional_observers = observers_per_region.get(region_key, [f"observer-{0:04d}"])
                observer_id = self.rng.choice(regional_observers)
            else:  # 20% random
                observer_id = f"observer-{self.rng.integers(0, n_observers):04d}"
            self.observer_ids.append(observer_id)

    def _initialize_motion_parameters(self, n: int) -> None:
        """Initialize parametric motion path parameters (harmonics, drift, phases)."""
        cfg = self.cfg
        
        # Path "size" and time parameters
        self.scale = self.rng.uniform(cfg.scale_m_min, cfg.scale_m_max, n).astype(np.float32)
        self.speed_ref = self.rng.uniform(cfg.speed_mps_min, cfg.speed_mps_max, n).astype(np.float32)

        # Angular frequencies chosen so typical speed ~ speed_ref
        w1 = (self.speed_ref / self.scale) * self.rng.uniform(0.6, 1.4, n).astype(np.float32)
        w2 = w1 * self.rng.uniform(1.4, 2.3, n).astype(np.float32)
        self.w1 = w1
        self.w2 = w2

        # Phase offsets for harmonic motion
        self.phi1 = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)
        self.phi2 = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)
        self.phi3 = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)
        self.phi4 = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)

        # Drift (to allow long-distance coverage)
        drift_mag = self.speed_ref * self.rng.uniform(0.0, cfg.drift_frac_max, n).astype(np.float32)
        drift_ang = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)
        self.drift_x = (drift_mag * np.cos(drift_ang)).astype(np.float32)
        self.drift_y = (drift_mag * np.sin(drift_ang)).astype(np.float32)

        # Time offsets so entities aren't synchronized
        self.t_offset = self.rng.uniform(0, 10_000.0, n).astype(np.float32)

    def _initialize_altitude_parameters(self, n: int) -> None:
        """Initialize altitude oscillation parameters."""
        cfg = self.cfg
        
        self.alt_base = self.rng.uniform(cfg.alt_base_min, cfg.alt_base_max, n).astype(np.float32)
        self.alt_amp = self.rng.uniform(cfg.alt_amp_min, cfg.alt_amp_max, n).astype(np.float32)
        alt_period = self.rng.uniform(cfg.alt_period_min_s, cfg.alt_period_max_s, n).astype(np.float32)
        self.w_alt = (2 * math.pi / alt_period).astype(np.float32)
        self.phi_alt = self.rng.uniform(0, 2 * math.pi, n).astype(np.float32)

    def _initialize_noise_states(self, n: int) -> None:
        """Initialize Ornstein-Uhlenbeck noise states for position and altitude."""
        self.noise_x = np.zeros(n, dtype=np.float32)
        self.noise_y = np.zeros(n, dtype=np.float32)
        self.noise_alt = np.zeros(n, dtype=np.float32)

    def _initialize_tracking_states(self, n: int) -> None:
        """Initialize previous position tracking for speed/load estimation."""
        self.last_t = np.full(n, self.t0, dtype=np.float64)
        self.x_prev = np.zeros(n, dtype=np.float32)
        self.y_prev = np.zeros(n, dtype=np.float32)
        self.alt_prev = self.alt_base.copy()

    def _initialize_temperature_states(self, n: int) -> None:
        """Initialize internal temperature state."""
        # Init roughly: ambient + operational delta, but we set properly on first step
        self.temp_in = np.full(n, 25.0, dtype=np.float32)

    def _initialize_anomaly_states(self, n: int) -> None:
        """Initialize anomaly window tracking."""
        self.anom_end = np.zeros(n, dtype=np.float64)
        self.anom_delta = np.zeros(n, dtype=np.float32)

    def _initialize_text_cache(self, n: int) -> None:
        """Initialize optional per-entity text payload cache."""
        self.text_cache: List[str] = [""] * n
        self.next_text_t = np.zeros(n, dtype=np.float64)

    def _ou_step(self, x: np.ndarray, dt: np.ndarray, tau: float, sigma: float) -> np.ndarray:
        """
        Vectorized Ornstein-Uhlenbeck update with exact discretization.
        """
        # avoid divide-by-zero
        tau = max(1e-6, float(tau))
        a = np.exp(-dt / tau).astype(np.float32)
        # stationary variance -> sigma^2; discrete innovation variance:
        # Var = sigma^2 * (1 - exp(-2 dt/tau))
        innov_std = (sigma * np.sqrt(1.0 - np.exp(-2.0 * dt / tau))).astype(np.float32)
        return (x * a) + (innov_std * self.rng.normal(0.0, 1.0, size=x.shape).astype(np.float32))

    def _external_temp_c(self, lat: np.ndarray, alt_m: np.ndarray, now_ts: float) -> np.ndarray:
        """
        Cheap but plausible external temperature model:
          - latitude baseline
          - seasonality based on day-of-year
          - diurnal cycle based on UTC hour
          - ISA lapse rate with altitude
        """
        dt_utc = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        doy = dt_utc.timetuple().tm_yday
        hour = dt_utc.hour + dt_utc.minute / 60.0

        lat_abs = np.abs(lat)
        # baseline by latitude (very rough climatology)
        t_lat = 30.0 - 0.7 * lat_abs  # 30C at equator-ish down to ~-19C at 70deg
        t_lat = np.clip(t_lat, -35.0, 38.0)

        # seasonality: peak around late June (doy~173) in NH; inverted in SH
        season = math.cos(2 * math.pi * (doy - 173) / 365.0)
        hemis = np.sign(lat)  # +NH, -SH
        t_season = 10.0 * season * hemis

        # diurnal: warmest mid-afternoon (~14:00)
        t_diurnal = 5.0 * math.sin(2 * math.pi * (hour - 14.0) / 24.0)

        t0 = t_lat + t_season + t_diurnal
        t0 = t0 + self.rng.normal(0.0, 0.5, size=t0.shape)  # weather-ish noise

        lapse = (self.cfg.isa_lapse_c_per_km * (alt_m / 1000.0)).astype(np.float32)
        t_ext = (t0 - lapse) + self.rng.normal(0.0, 0.3, size=t0.shape)

        return t_ext.astype(np.float32)

    def step(
        self,
        ids: np.ndarray,
        now_ts: float,
        text_sampler: Optional[TextSampler] = None,
        text_refresh_range_s: Tuple[float, float] = (5.0, 30.0),
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, Sequence[str]]:
        """
        Advance and return telemetry for the given entity indices.

        Returns: lat, lon, z_m, temp_ext_c, temp_int_c, text_payload
        """
        cfg = self.cfg

        # dt per entity (cap to keep simulation stable even if paused)
        dt = (now_ts - self.last_t[ids]).astype(np.float32)
        dt = np.clip(dt, 1e-3, 2.0)

        # "mission time"
        t = ((now_ts - self.t0) + self.t_offset[ids]).astype(np.float32)

        # OU noise updates
        self.noise_x[ids] = self._ou_step(self.noise_x[ids], dt, cfg.pos_noise_tau_s, cfg.pos_noise_sigma_m)
        self.noise_y[ids] = self._ou_step(self.noise_y[ids], dt, cfg.pos_noise_tau_s, cfg.pos_noise_sigma_m)
        self.noise_alt[ids] = self._ou_step(self.noise_alt[ids], dt, cfg.alt_noise_tau_s, cfg.alt_noise_sigma_m)

        # Nominal XY path (meters)
        scale = self.scale[ids]
        x = (
            self.drift_x[ids] * t
            + scale * (np.sin(self.w1[ids] * t + self.phi1[ids]) + 0.5 * np.sin(self.w2[ids] * t + self.phi2[ids]))
            + self.noise_x[ids]
        ).astype(np.float32)
        y = (
            self.drift_y[ids] * t
            + scale * (np.cos(self.w1[ids] * t + self.phi3[ids]) + 0.5 * np.cos(self.w2[ids] * t + self.phi4[ids]))
            + self.noise_y[ids]
        ).astype(np.float32)

        # Altitude (meters)
        alt = (self.alt_base[ids] + self.alt_amp[ids] * np.sin(self.w_alt[ids] * t + self.phi_alt[ids]) + self.noise_alt[ids]).astype(np.float32)
        alt = np.clip(alt, 5.0, 600.0)

        # Convert to lat/lon around per-entity origin
        lat = (self.lat0[ids] + (y.astype(np.float64) * self.inv_m_per_deg_lat)).astype(np.float64)
        lat = np.clip(lat, -85.0, 85.0)
        lon = (self.lon0[ids] + (x.astype(np.float64) * self.inv_m_per_deg_lon[ids])).astype(np.float64)

        # normalize lon to [-180, 180)
        lon = ((lon + 180.0) % 360.0) - 180.0

        # Speed + climb for "load" proxy
        dx = (x - self.x_prev[ids]).astype(np.float32)
        dy = (y - self.y_prev[ids]).astype(np.float32)
        da = (alt - self.alt_prev[ids]).astype(np.float32)

        speed = np.sqrt(dx * dx + dy * dy) / dt
        climb = da / dt

        self.x_prev[ids] = x
        self.y_prev[ids] = y
        self.alt_prev[ids] = alt
        self.last_t[ids] = now_ts

        load = 0.4 * (speed / np.maximum(self.speed_ref[ids], 1e-3)) + 0.6 * (np.abs(climb) / 3.0)
        load = np.clip(load, 0.0, 1.0).astype(np.float32)

        # External temp
        temp_ext = self._external_temp_c(lat.astype(np.float32), alt, now_ts)

        # Anomaly start (rare)
        # P(start in dt) ~ 1 - exp(-lambda dt)
        p_start = 1.0 - np.exp(-cfg.anomaly_rate_per_s * dt.astype(np.float64))
        can_start = self.anom_end[ids] <= now_ts
        u = self.rng.random(size=ids.shape[0])
        start_mask = (u < p_start) & can_start
        if np.any(start_mask):
            dur = self.rng.uniform(cfg.anomaly_dur_min_s, cfg.anomaly_dur_max_s, size=int(start_mask.sum()))
            delta = self.rng.uniform(cfg.anomaly_delta_min_c, cfg.anomaly_delta_max_c, size=int(start_mask.sum()))
            idxs = ids[start_mask]
            self.anom_end[idxs] = now_ts + dur
            self.anom_delta[idxs] = delta.astype(np.float32)

        anom_active = (self.anom_end[ids] > now_ts)
        anom_extra = (self.anom_delta[ids] * anom_active.astype(np.float32))

        # Internal temp dynamics (first-order system)
        # alpha = 1 - exp(-dt/tau)
        alpha = (1.0 - np.exp(-dt / cfg.internal_tau_s)).astype(np.float32)
        target = temp_ext + cfg.internal_base_delta_c + cfg.internal_load_delta_c * load + anom_extra
        self.temp_in[ids] = (
            self.temp_in[ids] + alpha * (target - self.temp_in[ids]) + self.rng.normal(0.0, cfg.internal_noise_sigma_c, size=ids.shape[0]).astype(np.float32)
        )

        # Text cache refresh (optional, and intentionally *not* per event)
        if text_sampler is not None:
            refresh_mask = now_ts >= self.next_text_t[ids]
            if np.any(refresh_mask):
                min_s, max_s = text_refresh_range_s
                idxs = ids[refresh_mask]
                for idx in idxs.tolist():
                    self.text_cache[idx] = text_sampler.sample()
                self.next_text_t[idxs] = now_ts + self.rng.uniform(min_s, max_s, size=idxs.shape[0])
            texts: Sequence[str] = [self.text_cache[i] for i in ids.tolist()]
        else:
            texts = [""] * ids.shape[0]

        return lat, lon, alt, temp_ext, self.temp_in[ids].copy(), texts


# -----------------------------
# Producer main
# -----------------------------

def _dumps(obj: dict) -> bytes:
    if orjson is not None:
        return orjson.dumps(obj)
    # fallback: stdlib json (slower)
    import json
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def main() -> None:
    bootstrap = env_str("KAFKA_BOOTSTRAP", "kafka:19092")
    topic = env_str("TOPIC", "demo-events")

    # mbp m3 handles up to ~11k/s
    eps = max(1, env_int("EVENTS_PER_SEC", 2000))
    n_entities = max(1, env_int("N_ENTITIES", 5000))

    # send loop cadence (lower => lower latency; higher => fewer wakeups)
    period_ms = max(5, env_int("BATCH_PERIOD_MS", 50))
    period_s = period_ms / 1000.0
    batch_n = max(1, int(eps * period_s))

    # Kafka tuning for throughput demos (adjust to your broker)
    linger_ms = env_int("KAFKA_LINGER_MS", 20)
    batch_size = env_int("KAFKA_BATCH_SIZE", 32768)
    acks = env_int("KAFKA_ACKS", 0)  # 0 for max throughput (lossy), 1/all for stronger guarantees
    compression = os.getenv("KAFKA_COMPRESSION")  # e.g. gzip, lz4, snappy, zstd (broker/client dependent)

    try_create_topic(bootstrap, topic, partitions=env_int("TOPIC_PARTITIONS", 12), replication=env_int("TOPIC_RF", 1))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=linger_ms,
        batch_size=batch_size,
        acks=acks,
        compression_type=compression,
        max_in_flight_requests_per_connection=5,
        retries=0,
    )

    # Text source (optional)
    text_file = os.getenv("TEXT_FILE", "")
    text_sampler: Optional[TextSampler] = None
    if text_file:
        text_sampler = TextSampler(text_file, seed=env_int("TEXT_SEED", 1))
        print(f"[producer] text_sampler enabled: {text_file}")

    text_refresh_min = env_float("TEXT_REFRESH_MIN_S", 5.0)
    text_refresh_max = env_float("TEXT_REFRESH_MAX_S", 30.0)

    fleet = FleetState(FleetConfig(n_entities=n_entities, seed=env_int("FLEET_SEED", 42)))

    # Precompute entity identifiers/keys (saves per-event string formatting at high EPS)
    entity_ids = [f"asset-{i:06d}" for i in range(n_entities)]
    entity_keys = [eid.encode("utf-8") for eid in entity_ids]

    # Round-robin entity selection for stable per-entity cadence
    ptr = 0

    sent = 0
    total = 0
    last_report = time.time()

    print(
        "[producer] started "
        f"bootstrap={bootstrap} topic={topic} eps={eps} n_entities={n_entities} "
        f"batch_period_ms={period_ms} batch_n={batch_n} acks={acks} compression={compression}"
    )

    try:
        while True:
            loop_start = time.time()
            now_ts = loop_start

            ids = (np.arange(ptr, ptr + batch_n, dtype=np.int64) % n_entities)
            ptr = int((ptr + batch_n) % n_entities)

            lat, lon, z_m, t_ext, t_in, texts = fleet.step(
                ids,
                now_ts=now_ts,
                text_sampler=text_sampler,
                text_refresh_range_s=(text_refresh_min, text_refresh_max),
            )

            # Emit events
            ts = datetime.now(timezone.utc)
            for k in range(ids.shape[0]):
                eid_idx = int(ids[k])
                entity_id = entity_ids[eid_idx]
                evt = {
                    "event_id": str(uuid_from_time(ts)),
                    "entity_id": entity_id,
                    "observer_id": fleet.observer_ids[eid_idx],
                    "event_type": EVENT_TYPES[eid_idx % len(EVENT_TYPES)],
                    "position": {"lat": float(lat[k]), "lon": float(lon[k])},
                    "z_m": float(z_m[k]),
                    "temp_external_c": float(t_ext[k]),
                    "temp_internal_c": float(t_in[k]),
                    "text": texts[k],
                }
                producer.send(topic, key=entity_keys[eid_idx], value=_dumps(evt))
                sent += 1

            # Periodic flush is usually unnecessary; producer batches automatically.
            # producer.poll(0) is not in kafka-python; send() triggers background I/O.

            # rate control
            elapsed = time.time() - loop_start
            if elapsed < period_s:
                time.sleep(period_s - elapsed)

            # report
            t = time.time()
            total += sent
            if t - last_report >= 5.0:
                print(f"[producer] sent_total={total} (~{sent/(t-last_report):.0f}/s)")
                sent = 0
                last_report = t

    finally:
        try:
            producer.flush(10)
        except Exception:
            pass
        try:
            producer.close()
        except Exception:
            pass
        if text_sampler is not None:
            text_sampler.close()


if __name__ == "__main__":
    main()
