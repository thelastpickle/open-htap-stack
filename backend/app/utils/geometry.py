"""Geometry helpers for the restricted-zone tools.

Zones are small (a few km across) so distances are computed on a local
equirectangular projection in metres, which is accurate to well under a metre at
this scale and far cheaper than a spherical solution.
"""
import math
from typing import List, Tuple

EARTH_RADIUS_M = 6_371_000.0
M_PER_DEG_LAT = 111_320.0

# A WKT ring is a list of (lon, lat) pairs — x, y order, as WKT specifies.
Ring = List[Tuple[float, float]]


def parse_wkt_polygon(wkt: str) -> Ring:
    """Parse ``POLYGON((lon lat, lon lat, ...))`` into a list of (lon, lat)."""
    text = wkt.strip()
    if not text.upper().startswith("POLYGON"):
        return []
    start, end = text.find("(("), text.rfind("))")
    if start == -1 or end == -1:
        return []

    ring: Ring = []
    for pair in text[start + 2 : end].split(","):
        parts = pair.split()
        if len(parts) >= 2:
            try:
                ring.append((float(parts[0]), float(parts[1])))
            except ValueError:
                continue
    return ring


def point_in_polygon(lat: float, lon: float, polygon: Ring) -> bool:
    """Ray-casting containment test against a (lon, lat) ring."""
    if len(polygon) < 3:
        return False

    inside = False
    j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        # Only edges straddling the ray's latitude can cross it, which also
        # guarantees yj != yi below.
        if (yi > lat) != (yj > lat):
            x_at_lat = xi + (xj - xi) * (lat - yi) / (yj - yi)
            if lon < x_at_lat:
                inside = not inside
        j = i
    return inside


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def distance_to_polygon_m(lat: float, lon: float, polygon: Ring) -> float:
    """Shortest distance in metres from a point to a polygon's boundary.

    Measured to the nearest point on each edge, not merely to the nearest
    vertex: a drone alongside a long edge is close to the zone even when it is
    far from either corner.  Returns 0.0 for a point inside the polygon.
    """
    if len(polygon) < 3:
        return float("inf")
    if point_in_polygon(lat, lon, polygon):
        return 0.0

    # Project to metres about the query point.
    m_per_deg_lon = M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.01)

    def to_xy(plon: float, plat: float) -> Tuple[float, float]:
        return ((plon - lon) * m_per_deg_lon, (plat - lat) * M_PER_DEG_LAT)

    nearest = float("inf")
    for i, vertex in enumerate(polygon):
        ax, ay = to_xy(*vertex)
        bx, by = to_xy(*polygon[(i + 1) % len(polygon)])
        # Distance from the origin to segment AB.
        dx, dy = bx - ax, by - ay
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq == 0.0:
            t = 0.0
        else:
            t = max(0.0, min(1.0, -(ax * dx + ay * dy) / seg_len_sq))
        px, py = ax + t * dx, ay + t * dy
        nearest = min(nearest, math.hypot(px, py))
    return nearest


def compute_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from point 1 to point 2, in degrees clockwise from north."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(rlat2)
    y = math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0
