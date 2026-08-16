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
import threading
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
        return self._extract_stable(pos, n_sent)

    def sample_stable(self, seed: int) -> str:
        """Sample a few sentences from a deterministic offset based on a seed."""
        if self._size < 32:
            return ""
        local_rng = np.random.default_rng(seed)
        n_sent = int(local_rng.integers(1, 4))
        pos = int(local_rng.integers(0, self._size))
        return self._extract_stable(pos, n_sent)

    def _extract_stable(self, pos: int, n_sent: int) -> str:
        start = self._find_sentence_start(pos)
        txt = self._take_sentences(start, n_sent)
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

    # Area the fleet operates in.  Defaults to greater Oslo, which is where the
    # demo's restricted zones are.
    center_lat: float = 59.91
    center_lon: float = 10.75
    lat_spread_deg: float = 0.06   # ~6.7 km north-south
    lon_spread_deg: float = 0.12   # ~8 km east-west at this latitude

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

    # Internal temperature anomalies.  Each lasts a few tens of seconds, and the
    # arrival rate is derived from the target fraction, so the share of the fleet
    # running hot at any moment tracks the demo's outlier setting.
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
        """Scatter entity origins over the configured area.

        The default is greater Oslo, which keeps the whole fleet on one map view
        and inside reach of the demo's restricted zones.  Override the centre and
        spread through the environment to move the fleet elsewhere.
        """
        cfg = self.cfg
        self.lat0 = self.rng.uniform(
            cfg.center_lat - cfg.lat_spread_deg, cfg.center_lat + cfg.lat_spread_deg, n
        ).astype(np.float64)
        self.lon0 = self.rng.uniform(
            cfg.center_lon - cfg.lon_spread_deg, cfg.center_lon + cfg.lon_spread_deg, n
        ).astype(np.float64)

        # Metres-per-degree conversion, per entity so it stays right as the fleet
        # spreads in latitude.
        self.inv_m_per_deg_lat = 1.0 / 111_320.0
        self.inv_m_per_deg_lon = 1.0 / (111_320.0 * np.cos(np.deg2rad(self.lat0)).clip(0.2, None))

    def _initialize_observer_assignments(self, n: int) -> None:
        """Assign one observer per 100 entities, in contiguous blocks.

        Entity origins are already clustered in one area, so the observer's job
        here is only to give queries a second grouping dimension alongside
        entity_id.  Contiguous blocks make that grouping predictable.
        """
        n_observers = max(1, n // 100)
        self.observer_ids = [f"observer-{min(i // 100, n_observers - 1):04d}" for i in range(n)]

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
        self.text_revision = np.zeros(n, dtype=np.int64)

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

    def _anomaly_rate_per_s(self, outlier_fraction: float) -> float:
        """Arrival rate that holds ``outlier_fraction`` of the fleet in anomaly.

        An entity is hot for a mean duration d, so with arrivals at rate r the
        long-run share hot is rd/(1+rd).  Solving for r gives the rate that lands
        on the requested fraction.
        """
        target = min(max(outlier_fraction, 0.0), 0.99)
        if target <= 0.0:
            return 0.0
        mean_duration_s = (self.cfg.anomaly_dur_min_s + self.cfg.anomaly_dur_max_s) / 2.0
        return target / (mean_duration_s * (1.0 - target))

    def step(
        self,
        ids: np.ndarray,
        now_ts: float,
        text_sampler: Optional[TextSampler] = None,
        text_refresh_range_s: Tuple[float, float] = (5.0, 30.0),
        outlier_fraction: float = 0.05,
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
        p_start = 1.0 - np.exp(-self._anomaly_rate_per_s(outlier_fraction) * dt.astype(np.float64))
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
                    # Seed on the entity *and* its refresh count, so a refresh
                    # actually yields new text.  Seeding on the entity alone gave
                    # every entity one fixed snippet for the process's lifetime.
                    self.text_revision[idx] += 1
                    self.text_cache[idx] = text_sampler.sample_stable(
                        idx * 1_000_003 + self.text_revision[idx]
                    )
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




# -----------------------------
# Live settings
# -----------------------------

class LiveSettings:
    """The demo controls, polled from the dashboard backend.

    The Settings page holds the authoritative values in the backend's memory;
    this thread copies them in.  A backend that is absent, slow or broken leaves
    the producer on the values it already had, so data generation never depends
    on the dashboard being up.
    """

    def __init__(self, events_per_sec: int, n_entities: int, outlier_percent: float):
        self._lock = threading.Lock()
        self.events_per_sec = events_per_sec
        self.n_entities = n_entities
        self.outlier_percent = outlier_percent
        self.paused = False

    def snapshot(self) -> Tuple[int, int, float, bool]:
        with self._lock:
            return (self.events_per_sec, self.n_entities, self.outlier_percent, self.paused)

    def apply(self, payload: dict) -> None:
        with self._lock:
            previous = (self.events_per_sec, self.n_entities, self.outlier_percent, self.paused)
            self.events_per_sec = max(1, int(payload.get("events_per_sec", self.events_per_sec)))
            self.n_entities = max(1, int(payload.get("drones_enabled", self.n_entities)))
            self.outlier_percent = float(payload.get("outlier_percent", self.outlier_percent))
            self.paused = bool(payload.get("paused", self.paused))
            current = (self.events_per_sec, self.n_entities, self.outlier_percent, self.paused)
        if current != previous:
            print(
                f"[producer] settings updated: eps={current[0]} n_entities={current[1]} "
                f"outlier_percent={current[2]} paused={current[3]}"
            )


def _poll_settings(url: str, live: LiveSettings, stop: threading.Event, interval_s: float) -> None:
    """Poll the settings endpoint until asked to stop."""
    import json as _json
    import urllib.error
    import urllib.request

    last_error = ""
    while not stop.wait(interval_s):
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = _json.loads(resp.read().decode("utf-8"))
            live.apply(body.get("settings", {}))
            last_error = ""
        except Exception as e:
            # Log a repeated failure once, so a backend that is simply not running
            # does not fill the log.
            message = f"{type(e).__name__}: {e}"
            if message != last_error:
                print(f"[producer] settings poll failed, keeping current values ({message})")
                last_error = message


# -----------------------------
# Producer main
# -----------------------------

def main() -> None:
    bootstrap = env_str("KAFKA_BOOTSTRAP", "kafka:19092")
    topic = env_str("TOPIC", "demo-events")

    # An Apple M3 laptop sustains roughly 11k events/s through this loop.
    live = LiveSettings(
        events_per_sec=max(1, env_int("EVENTS_PER_SEC", 2000)),
        n_entities=max(1, env_int("N_ENTITIES", 100)),
        outlier_percent=env_float("OUTLIER_PERCENT", 5.0),
    )

    # The fleet's arrays are allocated once, for the largest size the dashboard
    # may ask for, and live changes only vary how many of them are used.
    max_entities = max(live.n_entities, env_int("MAX_ENTITIES", 2000))
    fleet = FleetState(
        FleetConfig(
            n_entities=max_entities,
            seed=env_int("FLEET_SEED", 42),
            center_lat=env_float("FLEET_CENTER_LAT", 59.91),
            center_lon=env_float("FLEET_CENTER_LON", 10.75),
            lat_spread_deg=env_float("FLEET_LAT_SPREAD_DEG", 0.06),
            lon_spread_deg=env_float("FLEET_LON_SPREAD_DEG", 0.12),
        )
    )
    entity_ids = [f"asset-{i:06d}" for i in range(max_entities)]
    entity_keys = [eid.encode("utf-8") for eid in entity_ids]

    stop_polling = threading.Event()
    settings_url = env_str("SETTINGS_URL", "")
    if settings_url:
        threading.Thread(
            target=_poll_settings,
            args=(settings_url, live, stop_polling, env_float("SETTINGS_POLL_INTERVAL_S", 10.0)),
            daemon=True,
            name="settings-poller",
        ).start()
        print(f"[producer] polling {settings_url} for live settings")

    # Send-loop cadence: shorter means smoother motion and more wakeups.
    period_s = max(5, env_int("BATCH_PERIOD_MS", 50)) / 1000.0

    # Kafka tuning for throughput demos.  acks=0 is lossy by design: this is
    # generated telemetry, and the demo values sustained rate over durability.
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=env_int("KAFKA_LINGER_MS", 20),
        batch_size=env_int("KAFKA_BATCH_SIZE", 32768),
        acks=env_int("KAFKA_ACKS", 0),
        compression_type=os.getenv("KAFKA_COMPRESSION"),
        max_in_flight_requests_per_connection=5,
        retries=0,
    )
    try_create_topic(
        bootstrap, topic,
        partitions=env_int("TOPIC_PARTITIONS", 12),
        replication=env_int("TOPIC_RF", 1),
    )

    text_file = os.getenv("TEXT_FILE", "")
    text_sampler = TextSampler(text_file, seed=env_int("TEXT_SEED", 1)) if text_file else None
    if text_sampler is not None:
        print(f"[producer] text payloads sampled from {text_file}")
    text_refresh = (env_float("TEXT_REFRESH_MIN_S", 5.0), env_float("TEXT_REFRESH_MAX_S", 30.0))

    report_every_s = env_float("REPORT_EVERY_S", 5.0)
    ptr = 0                 # round-robin cursor, for an even per-entity cadence
    total_sent = 0
    window_sent = 0
    last_report = time.time()

    print(
        f"[producer] started bootstrap={bootstrap} topic={topic} "
        f"eps={live.events_per_sec} n_entities={live.n_entities} "
        f"max_entities={max_entities} batch_period_ms={int(period_s * 1000)}"
    )

    try:
        while True:
            loop_start = time.time()
            eps, n_entities, outlier_percent, paused = live.snapshot()

            if paused:
                time.sleep(period_s)
                continue

            # The fleet arrays are sized for max_entities, so a request for more
            # is capped rather than allowed to index past the end.
            n_entities = min(n_entities, max_entities)
            batch_n = max(1, int(eps * period_s))

            ids = (np.arange(ptr, ptr + batch_n, dtype=np.int64) % n_entities)
            ptr = int((ptr + batch_n) % n_entities)

            lat, lon, z_m, t_ext, t_in, texts = fleet.step(
                ids,
                now_ts=loop_start,
                text_sampler=text_sampler,
                text_refresh_range_s=text_refresh,
                outlier_fraction=outlier_percent / 100.0,
            )

            # Spread the batch's events across the window they represent rather
            # than stamping them all at the same instant.  The event_id is a
            # timeuuid, and the sink derives event_time from it, so identical
            # stamps would collapse an asset's history into one point and leave
            # the ordering within a batch undefined.
            stamp_step_s = period_s / ids.shape[0]
            for k in range(ids.shape[0]):
                index = int(ids[k])
                producer.send(
                    topic,
                    key=entity_keys[index],
                    value=_dumps({
                        "event_id": str(uuid_from_time(loop_start + k * stamp_step_s)),
                        "entity_id": entity_ids[index],
                        "observer_id": fleet.observer_ids[index],
                        "event_type": EVENT_TYPES[index % len(EVENT_TYPES)],
                        "position": {"lat": float(lat[k]), "lon": float(lon[k])},
                        "z_m": float(z_m[k]),
                        "temp_external_c": float(t_ext[k]),
                        "temp_internal_c": float(t_in[k]),
                        "text": texts[k],
                    }),
                )
            window_sent += ids.shape[0]

            elapsed = time.time() - loop_start
            if elapsed < period_s:
                time.sleep(period_s - elapsed)

            now = time.time()
            if now - last_report >= report_every_s:
                total_sent += window_sent
                print(f"[producer] sent_total={total_sent} (~{window_sent / (now - last_report):.0f}/s)")
                window_sent = 0
                last_report = now

    finally:
        stop_polling.set()
        try:
            producer.flush(10)
            producer.close()
        except Exception as e:
            print(f"[producer] shutdown error: {e}")
        if text_sampler is not None:
            text_sampler.close()


if __name__ == "__main__":
    main()
