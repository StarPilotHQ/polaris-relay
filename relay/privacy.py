"""Privacy zones and the segment filter. Fails closed: when in doubt, a segment stays on this machine."""
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from relay.qlogs import Scan

EARTH_M = 6_371_008.8
LATE_FIX_S = 30.0
ENDPOINT_MARGIN_M = 250.0


@dataclass(frozen=True)
class Circle:
  lat: float
  lng: float
  radius_m: float


@dataclass(frozen=True)
class Polygon:
  points: tuple[tuple[float, float], ...]  # (lat, lng)


Zone = Circle | Polygon


def distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
  p1, p2 = math.radians(lat1), math.radians(lat2)
  a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lng2 - lng1) / 2) ** 2
  return 2 * EARTH_M * math.asin(min(1.0, math.sqrt(a)))


def _local_xy(lat: float, lng: float, lat0: float, lng0: float) -> tuple[float, float]:
  """Equirectangular meters around (lat0, lng0): accurate to well under a meter at zone sizes."""
  return (math.radians(lng - lng0) * EARTH_M * math.cos(math.radians(lat0)), math.radians(lat - lat0) * EARTH_M)


def _segment_distance(p, a, b) -> float:
  (px, py), (ax, ay), (bx, by) = p, a, b
  dx, dy = bx - ax, by - ay
  t = 0.0 if dx == dy == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
  return math.hypot(px - ax - t * dx, py - ay - t * dy)


def near_zone(lat: float, lng: float, slack_m: float, zone: Zone) -> bool:
  """True when the point, or anywhere within `slack_m` of it (its accuracy circle), touches the zone."""
  if isinstance(zone, Circle):
    return distance_m(lat, lng, zone.lat, zone.lng) <= zone.radius_m + slack_m
  pts = [_local_xy(a, b, lat, lng) for a, b in zone.points]
  if len(pts) < 3:
    return False
  inside = False
  for (x1, y1), (x2, y2) in zip(pts, pts[1:] + pts[:1]):
    if (y1 > 0) != (y2 > 0) and 0 < (x2 - x1) * (0 - y1) / (y2 - y1) + x1:
      inside = not inside
  return inside or min(_segment_distance((0, 0), a, b) for a, b in zip(pts, pts[1:] + pts[:1])) <= slack_m


def in_any(lat: float, lng: float, slack_m: float, zones: Iterable[Zone]) -> bool:
  return any(near_zone(lat, lng, slack_m, z) for z in zones)


def excluded_segments(segments: list[int], scan_of: Callable[[int], Scan | None], zones: list[Zone], *,
                      start_point: list[float] | None = None, end_point: list[float] | None = None,
                      surround: int = 0) -> set[int]:
  """Segments that must never leave the machine:
  - any position whose circle (its accuracy, at most 1 km when unknown) touches a zone, fix or not;
  - no good GPS fix (under 50 m) at all, or a log that couldn't be read to the end (`scan_of` gives None);
  - the route's first segment when comma's start point is in a zone, its first good fix comes more than 30 s in, or
    any position comes within 250 m of a zone; the last segment likewise at its end. A drive usually starts and ends
    at a private place, often before GPS locks or after it drops (a garage);
  - then `surround` segments on each side of every one of those."""
  if not segments:
    return set()
  out: set[int] = set()
  first, last = min(segments), max(segments)
  for seg in segments:
    sc = scan_of(seg)
    if sc is None or not sc.has_good_fix or any(in_any(lat, lng, slack, zones) for lat, lng, slack in sc.points):
      out.add(seg)
      continue
    near_end = any(in_any(lat, lng, slack + ENDPOINT_MARGIN_M, zones) for lat, lng, slack in sc.points)
    if seg == first and (near_end or min(sc.good_fix_times) > LATE_FIX_S):
      out.add(seg)
    if seg == last and (near_end or sc.duration_s - max(sc.good_fix_times) > LATE_FIX_S):
      out.add(seg)
  if start_point and in_any(start_point[0], start_point[1], 0.0, zones):
    out.add(first)
  if end_point and in_any(end_point[0], end_point[1], 0.0, zones):
    out.add(last)
  if surround > 0:
    out |= {s for s in segments for e in out if abs(s - e) <= surround}
  return out
