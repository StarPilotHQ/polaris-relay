import json
import math
import re
import time

import pytest
import zstandard

from relay import privacy, qlogs
from relay.db import Route, Zone
from relay.pipeline import State
from relay.qlogs import Scan, read_scans
from tests.conftest import AFTER_DRIVE, FAR, HOME, SCHEMA, FakeComma, FakeSite, onboard, qlog, run_until_settled


def drive(log_schema, home_segments=(), no_fix=(), n=8):
  """Segments far from home, except those passing home or with no fix at all."""
  out = {}
  for seg in range(n):
    if seg in no_fix:
      out[seg] = qlog(log_schema, [])
    elif seg in home_segments:
      out[seg] = qlog(log_schema, [FAR, (HOME[0] + 0.0005, HOME[1]), FAR])
    else:
      out[seg] = qlog(log_schema, [(FAR[0] + seg * 0.001, FAR[1])] * 3)
  return out


def outbound_segments(comma: FakeComma, site: FakeSite) -> set[int]:
  """Every segment whose bytes or files left the machine, or whose upload was requested from the device."""
  segs = set()
  for r in site.requests:
    if m := re.search(r"/(?:qlogs|files)/(\d+)", r.url.path):
      segs.add(int(m[1]))
  for p in comma.upload_paths:
    segs.add(int(re.search(r"--(\d+)/", p)[1]))
  return segs


def test_zone_segment_and_neighbors_never_leave(log_schema, clock, make_worker):
  comma = FakeComma(clock, drive(log_schema, home_segments=(3,)))
  site = FakeSite(rogue_suggestion=4)  # a site bug suggesting a range on a kept-private segment
  w = make_worker(comma, site)
  onboard(w, zones=[Zone(kind="circle", lat=HOME[0], lng=HOME[1], radius_m=200)], surround=1)
  run_until_settled(w, clock)
  with w.sessionmaker() as s:
    r = s.get(Route, comma.route_obj()["fullname"])
    assert r.state == State.SUBMITTED, r.note
    assert r.excluded == [2, 3, 4]
  private = {2, 3, 4}
  assert not outbound_segments(comma, site) & private
  assert set(site.qlogs) == {0, 1, 5, 6, 7}
  private_bytes = {comma.qlogs[s] for s in private}
  assert not any(r.content in private_bytes for r in site.requests if r.method == "PUT")
  assert "s-4" not in site.submits[0][1]["suggestions"]
  assert not any("dcamera" in p for p in comma.upload_paths)


def test_segment_without_fix_and_route_endpoints_in_zone_stay(log_schema, clock, make_worker):
  comma = FakeComma(clock, drive(log_schema, no_fix=(6,)), start_point=HOME, end_point=FAR)
  site = FakeSite()
  w = make_worker(comma, site)
  onboard(w, zones=[Zone(kind="polygon", points=[[HOME[0] - .01, HOME[1] - .01], [HOME[0] - .01, HOME[1] + .01],
                                                  [HOME[0] + .01, HOME[1] + .01], [HOME[0] + .01, HOME[1] - .01]])])
  run_until_settled(w, clock)
  assert outbound_segments(comma, site).isdisjoint({0, 6})
  with w.sessionmaker() as s:
    assert s.get(Route, comma.route_obj()["fullname"]).excluded == [0, 6]


def test_every_segment_private_sends_nothing(log_schema, clock, make_worker):
  comma = FakeComma(clock, drive(log_schema, home_segments=range(4), n=4))
  site = FakeSite()
  w = make_worker(comma, site)
  onboard(w, zones=[Zone(kind="circle", lat=HOME[0], lng=HOME[1], radius_m=200)])
  run_until_settled(w, clock)
  assert not [r for r in site.requests if r.url.path.startswith("/api/v1/routes")]
  with w.sessionmaker() as s:
    assert s.get(Route, comma.route_obj()["fullname"]).state == State.SKIPPED


def test_without_schema_nothing_is_sent(log_schema, clock, make_worker):
  comma = FakeComma(clock, drive(log_schema))
  site = FakeSite(fail={r"GET /schema": [(503, {})] * 50})
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  assert not [r for r in site.requests if r.method == "PUT" or r.url.path.endswith("/routes")]
  with w.sessionmaker() as s:
    assert "log format" in s.get(Route, comma.route_obj()["fullname"]).note


def scans_of(tmp_path, logs: dict[int, bytes], timeout_s: float = 60, target=None):
  paths = {}
  for seg, data in logs.items():
    paths[seg] = tmp_path / f"{seg}.qlog"
    paths[seg].write_bytes(data)
  return read_scans(paths, SCHEMA, timeout_s, target)


def built(log_schema, *fills) -> bytes:
  """A qlog of events made by `fills`: (seconds, union member, function filling it in)."""
  out = []
  for t, name, fill in fills:
    ev = log_schema.Event.new_message()
    ev.logMonoTime = int(t * 1e9)
    fill(ev.init(name))
    out.append(ev.to_bytes())
  return zstandard.ZstdCompressor().compress(b"".join(out))


def gps(lat, lng, acc=5.0, fix=True):
  def fill(g):
    g.latitude, g.longitude, g.horizontalAccuracy, g.hasFix, g.flags = lat, lng, acc, fix, int(fix)
  return fill


def to_ecef(lat, lng, alt=0.0):
  a, e2 = 6378137.0, 6.69437999014e-3
  p, l = math.radians(lat), math.radians(lng)
  n = a / math.sqrt(1 - e2 * math.sin(p) ** 2)
  return ((n + alt) * math.cos(p) * math.cos(l), (n + alt) * math.cos(p) * math.sin(l), (n * (1 - e2) + alt) * math.sin(p))


ZONE = privacy.Circle(HOME[0], HOME[1], 100)
GOOD = (0.0, "gpsLocationExternal", gps(*FAR))


def excluded(tmp_path, logs: dict[int, bytes], zones=(ZONE,), **kw) -> set[int]:
  scans = scans_of(tmp_path, logs)
  return privacy.excluded_segments(sorted(logs), lambda s: scans[s], list(zones), **kw)


def test_accuracy_circle_counts(log_schema, tmp_path):
  just_outside = (HOME[0] + 0.0012, HOME[1])  # about 133 m north
  assert not privacy.in_any(*just_outside, 10, [ZONE])
  assert privacy.in_any(*just_outside, 40, [ZONE])
  logs = {1: built(log_schema, GOOD, (1, "gpsLocationExternal", gps(*just_outside, acc=45)))}
  assert excluded(tmp_path, logs) == {1}


def test_rough_unreadable_or_missing_fixes_keep_a_segment_back(log_schema, tmp_path):
  logs = {1: qlog(log_schema, [FAR], accuracy=80), 2: qlog(log_schema, [FAR], external=False), 3: b"not a log",
          4: built(log_schema, (0, "gpsLocationExternal", gps(*HOME, acc=300)), GOOD),
          5: built(log_schema, GOOD, (1, "gpsLocation", gps(HOME[0], HOME[1] + 0.0005, acc=20, fix=False))),
          6: built(log_schema, GOOD, (1, "gpsLocationExternal", gps(HOME[0] + 0.004, HOME[1], acc=float("nan"))))}
  # No good fix (1), a log that stops parsing (3), a rough fix at home (4), a fix-less internal GPS point at home next
  # to good external fixes (5), and a point 450 m off with no accuracy (1 km circle) (6) all stay.
  assert excluded(tmp_path, logs, start_point=None) == {1, 3, 4, 5, 6}


def test_every_position_source_counts(log_schema, tmp_path):
  def kalman_geodetic(k):
    k.positionGeodetic.value = [HOME[0], HOME[1], 300.0]
    k.positionGeodetic.std = [5.0, 5.0, 5.0]

  def gnss_ecef(g):
    g.positionECEF.value = list(to_ecef(HOME[0], HOME[1] + 0.0005))

  def starpilot_gps(c):
    c.gps.latitude, c.gps.longitude, c.gps.horizontalAccuracy = HOME[0], HOME[1], 8.0

  def phone_gps(n):
    n.phoneGps.latitude, n.phoneGps.longitude = HOME

  def nav_destination(n):
    route = n.init("coordinates", 2)
    route[0].latitude, route[0].longitude = FAR
    route[1].latitude, route[1].longitude = HOME

  def mapd_position(m):
    m.position.latitude, m.position.longitude = HOME

  sources = [("liveLocationKalmanDEPRECATED", kalman_geodetic), ("gnssMeasurementsDEPRECATED", gnss_ecef),
             ("starpilotCarState", starpilot_gps), ("starpilotNavigation", phone_gps), ("navRoute", nav_destination),
             ("mapdExtendedOut", mapd_position)]
  logs = {seg: built(log_schema, GOOD, (1, name, fill)) for seg, (name, fill) in enumerate(sources, start=1)}
  logs[len(sources) + 1] = built(log_schema, GOOD)
  assert excluded(tmp_path, logs) == set(range(1, len(sources) + 1))


def test_ecef_round_trip():
  lat, lng = qlogs.ecef_to_geodetic(*to_ecef(45.123, -100.456, 400))
  assert abs(lat - 45.123) < 1e-6 and abs(lng + 100.456) < 1e-6
  assert qlogs.ecef_to_geodetic(0, 0, 0) is None


def test_route_ends_with_late_or_near_fixes_stay(log_schema, tmp_path):
  late = built(log_schema, (0, "carState", lambda c: None), (45, "gpsLocationExternal", gps(*FAR)), (59, "carState",
                                                                                                      lambda c: None))
  near = (HOME[0] + 0.0027, HOME[1])  # 300 m north: outside the 100 m zone, inside its 250 m end margin
  logs = {0: late, 1: built(log_schema, GOOD), 2: built(log_schema, GOOD, (1, "gpsLocationExternal", gps(*near))),
          3: built(log_schema, GOOD, (1, "gpsLocationExternal", gps(*near)))}
  # The first segment's first fix comes 45 s in; the last ends near home. Segment 2 is near home too, but mid-route.
  assert excluded(tmp_path, logs) == {0, 3}


def test_hung_reader_is_killed(tmp_path):
  started = time.monotonic()
  with pytest.raises(RuntimeError, match="too long"):
    scans_of(tmp_path, {0: b"x"}, timeout_s=1, target=_sleep_forever)
  assert time.monotonic() - started < 15


def _sleep_forever(*_):
  time.sleep(60)


@pytest.mark.parametrize("point,inside", [((45.0, -100.0), True), ((45.02, -100.0), False), ((45.0103, -100.0), True)])
def test_polygon_with_slack(point, inside):
  square = privacy.Polygon(((44.99, -100.01), (44.99, -99.99), (45.01, -99.99), (45.01, -100.01)))
  assert privacy.near_zone(*point, 50, square) is inside


def test_surround_expands():
  good = Scan([(*FAR, 5.0)], [0.0], 0.0)
  assert privacy.excluded_segments(list(range(6)), lambda s: None if s == 2 else good, [], surround=1) == {1, 2, 3}


def test_zone_added_later_stops_what_is_still_to_send(log_schema, clock, make_worker):
  comma = FakeComma(clock, drive(log_schema, home_segments=(1,), n=4), online=False, last_ping=AFTER_DRIVE)
  site = FakeSite()
  w = make_worker(comma, site)
  onboard(w)  # no zones yet: segment 1 (passing home) is suggested
  run_until_settled(w, clock, ticks=3)
  with w.sessionmaker() as s:
    assert s.get(Route, comma.route_obj()["fullname"]).state == State.WAITING_FILES
    s.add(Zone(kind="circle", lat=HOME[0], lng=HOME[1], radius_m=200))
    from relay.pipeline import filter_changed
    filter_changed(s)
    s.commit()
  comma.online, comma.last_ping, comma.uploaded = True, None, set()
  sent_before = len([r for r in site.requests if "/files/" in r.url.path])
  comma.upload_paths.clear()
  clock.advance(hours=7)  # past the re-request interval, so uploads are asked for again
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    r = s.get(Route, comma.route_obj()["fullname"])
    assert r.state == State.SUBMITTED and 1 in r.excluded
  assert sent_before == 0
  assert 1 not in {int(re.search(r"/files/(\d+)/", q.url.path)[1]) for q in site.requests if "/files/" in q.url.path}
  assert not any("--1/" in p for p in comma.upload_paths)
  assert "s-1" not in site.submits[0][1]["suggestions"]
  # The site was told to forget the segment it already had the qlog of.
  suggests = [json.loads(q.content) for q in site.requests if q.url.path.endswith("/suggest")]
  assert 1 not in suggests[0]["excluded"] and 1 in suggests[-1]["excluded"] and 1 not in suggests[-1]["segments"]
