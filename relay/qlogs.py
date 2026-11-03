"""Reading every position a qlog carries, with the site's schema bundle, for the privacy filter."""
import bz2
import functools
import json
import math
import multiprocessing
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import capnp
import zstandard

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
BZ2_MAGIC = b"BZh"
TRAVERSAL_LIMIT = 2**63 - 1
MAX_ACCURACY_M = 50.0  # a GPS fix this good counts as knowing where the segment was
MAX_SLACK_M = 1000.0  # the accuracy circle used for a position with no usable accuracy
READ_TIMEOUT_S = 600
WGS84_A, WGS84_E2 = 6378137.0, 6.69437999014e-3

# Positions of the car itself. Only the GPS messages decide whether a segment has a good fix; every position counts
# toward touching a zone, with its accuracy as the circle around it.
GPS_FIXES = ("gpsLocationExternal", "gpsLocation")
LAT_LNG = {  # event: path to a struct with latitude/longitude (and maybe horizontalAccuracy)
  "gpsLocationExternal": (), "gpsLocation": (), "starpilotCarState": ("gps",), "starpilotNavigation": ("phoneGps",),
  "mapdExtendedOut": ("position",),
}
LEGACY_LAT_LON = ("liveLocationDEPRECATED", "liveLocationKalmanDeprecatedDEPRECATED", "liveLocationTimingDEPRECATED",
                  "liveLocationCorrectedDEPRECATED", "locationDEPRECATED", "applanixLocationDEPRECATED")
KALMAN = {"liveLocationKalmanDEPRECATED": ("positionGeodetic", "positionECEF"),
          "gnssMeasurementsDEPRECATED": ("positionECEF", "kalmanPositionECEF")}
# Coordinate lists that aren't where the car was but can name a place (a navigation destination): any coordinate in a
# zone keeps the segment back, since the qlog would reveal it.
COORD_LISTS = {"navRoute": ("coordinates",), "mapRenderStateDEPRECATED": ("route",),
               "starpilotNavigation": ("navigation", "route"), "mapdExtendedOut": ("path",)}
ECEF_LISTS = {"gpsPlannerPointsDEPRECATED": ("curPos", "points"), "gpsPlannerPlanDEPRECATED": ("points",)}


@dataclass
class Scan:
  """What one qlog says about where the car was. `points` are (lat, lng, slack_m); times are seconds from the
  segment's first message."""
  points: list[tuple[float, float, float]] = field(default_factory=list)
  good_fix_times: list[float] = field(default_factory=list)
  duration_s: float = 0.0

  @property
  def has_good_fix(self) -> bool:
    return bool(self.good_fix_times)


@functools.cache
def _schema(schema_dir: str):
  capnp.remove_import_hook()
  return capnp.load(str(Path(schema_dir) / "log.capnp"), imports=[schema_dir, str(Path(schema_dir) / "include")])


def decompress(data: bytes) -> bytes:
  if data.startswith(ZSTD_MAGIC):  # every frame: stopping after the first would hide later GPS from the filter
    return zstandard.ZstdDecompressor().stream_reader(data, read_across_frames=True).read()
  if data.startswith(BZ2_MAGIC):
    return bz2.decompress(data)
  return data


def _slack(acc) -> float:
  try:
    acc = float(acc)
  except (TypeError, ValueError):
    return MAX_SLACK_M
  return acc if math.isfinite(acc) and 0 < acc <= MAX_SLACK_M else MAX_SLACK_M


def _valid(lat, lng) -> bool:
  return math.isfinite(lat) and math.isfinite(lng) and -90 <= lat <= 90 and -180 <= lng <= 180 and (lat, lng) != (0, 0)


def ecef_to_geodetic(x: float, y: float, z: float) -> tuple[float, float] | None:
  if not all(math.isfinite(v) for v in (x, y, z)) or math.hypot(x, y, z) < 6_000_000:
    return None  # zero or nonsense: not a position on the earth
  p = math.hypot(x, y)
  lat = math.atan2(z, p * (1 - WGS84_E2))
  for _ in range(5):
    n = WGS84_A / math.sqrt(1 - WGS84_E2 * math.sin(lat) ** 2)
    lat = math.atan2(z + WGS84_E2 * n * math.sin(lat), p)
  return math.degrees(lat), math.degrees(math.atan2(y, x))


def _get(obj, path: tuple[str, ...]):
  for name in path:
    try:
      obj = getattr(obj, name)
    except (AttributeError, capnp.KjException):
      return None
  return obj


def _std_slack(m) -> float:
  try:
    std = [float(v) for v in m.std][:3]
  except (AttributeError, TypeError, capnp.KjException):
    return MAX_SLACK_M
  return _slack(math.sqrt(sum(v * v for v in std))) if std else MAX_SLACK_M


def scan(data: bytes, schema_dir: Path) -> Scan | None:
  """Every position in one qlog. None when the log stops parsing partway: the unread part could be anywhere, so the
  segment must stay on this machine."""
  events = _schema(str(schema_dir)).Event.read_multiple_bytes(decompress(data),
                                                              traversal_limit_in_words=TRAVERSAL_LIMIT)
  out, t0 = Scan(), None
  while True:
    try:
      ev = next(events)
    except StopIteration:
      break
    except capnp.KjException:
      return None
    t0 = ev.logMonoTime if t0 is None else t0
    t = (ev.logMonoTime - t0) / 1e9
    out.duration_s = max(out.duration_s, t)
    try:
      name = ev.which()
    except (capnp.KjException, ValueError):
      continue
    msg = getattr(ev, name)
    if name in LAT_LNG and (g := _get(msg, LAT_LNG[name])) is not None:
      lat, lng = g.latitude, g.longitude
      if _valid(lat, lng):
        acc = getattr(g, "horizontalAccuracy", None)
        out.points.append((lat, lng, _slack(acc)))
        if name in GPS_FIXES and (g.hasFix or g.flags & 1) and acc is not None and 0 < acc < MAX_ACCURACY_M:
          out.good_fix_times.append(t)
    if name in LEGACY_LAT_LON and _valid(msg.lat, msg.lon):
      out.points.append((msg.lat, msg.lon, MAX_SLACK_M))
    for path in KALMAN.get(name, ()):
      m = _get(msg, (path,))
      vals = list(m.value) if m is not None else []
      if len(vals) < 3:
        continue
      pos = (vals[0], vals[1]) if path == "positionGeodetic" else ecef_to_geodetic(*vals[:3])
      if pos and _valid(*pos):
        out.points.append((pos[0], pos[1], _std_slack(m)))
    if name in COORD_LISTS:
      for c in _get(msg, COORD_LISTS[name]) or []:
        if _valid(c.latitude, c.longitude):
          out.points.append((c.latitude, c.longitude, 0.0))
    for path in ECEF_LISTS.get(name, ()):
      items = _get(msg, (path,))
      for p in ([items] if path == "curPos" else items or []):
        if p is not None and (pos := ecef_to_geodetic(p.x, p.y, p.z)) and _valid(*pos):
          out.points.append((pos[0], pos[1], 0.0))
  return out


def _child(paths: dict[str, str], schema_dir: str, out: str) -> None:
  result = {}
  for seg, path in paths.items():
    try:
      sc = scan(Path(path).read_bytes(), Path(schema_dir))
    except Exception:  # anything unreadable keeps the segment back
      sc = None
    result[seg] = None if sc is None else {"points": sc.points, "good": sc.good_fix_times, "duration": sc.duration_s}
  Path(out).write_text(json.dumps(result))


def read_scans(paths: dict[int, Path], schema_dir: Path, timeout_s: float = READ_TIMEOUT_S,
               target=None) -> dict[int, Scan | None]:
  """`scan` for each qlog file, in a fresh process that's killed past the timeout: pycapnp aborts the whole process
  when a second copy of a schema (a new bundle version) is loaded, and a malformed log can't hang or take the worker
  down. Raises RuntimeError when the child doesn't finish."""
  ctx = multiprocessing.get_context("spawn")
  fd, out = tempfile.mkstemp(prefix="scan-", suffix=".json", dir=str(next(iter(paths.values())).parent) if paths else None)
  os.close(fd)
  try:
    proc = ctx.Process(target=target or _child, args=({str(k): str(v) for k, v in paths.items()}, str(schema_dir), out),
                       daemon=True)
    proc.start()
    proc.join(timeout_s)
    if proc.is_alive():
      proc.kill()
      proc.join(5)
      raise RuntimeError("reading GPS from the drive's logs took too long")
    if proc.exitcode != 0:
      raise RuntimeError(f"couldn't read GPS from the drive's logs (exit {proc.exitcode})")
    raw = json.loads(Path(out).read_text() or "{}")
  finally:
    Path(out).unlink(missing_ok=True)
  return {int(seg): None if v is None else Scan([tuple(p) for p in v["points"]], v["good"], v["duration"])
          for seg, v in raw.items()}
