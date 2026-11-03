import logging
import random
import threading
import time
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient

from relay import db, notify, pipeline, site as site_module
from relay.config import Config
from relay.db import Device, Route
from relay.pipeline import State
from relay.site import deprecation
from relay.web.app import create_app
from tests.conftest import DONGLE, FAR, ROUTE, FakeComma, FakeSite, onboard, qlog, run_until_settled

DEPRECATED = {"Deprecation": "@1798761600", "Link": '<https://site.test/docs/api>; rel="deprecation"; type="text/html"'}
SUNSET = "Sat, 01 May 2027 00:00:00 GMT"


def drive(log_schema, n=2):
  return {seg: qlog(log_schema, [(FAR[0] + seg * 0.001, FAR[1])] * 3) for seg in range(n)}


def route(w) -> Route:
  with w.sessionmaker() as s:
    return s.get(Route, ROUTE)


def until(w, clock, state, ticks=12, step_min=10) -> None:
  for _ in range(ticks):
    if (r := route(w)) is not None and r.state == state:
      return
    w.tick()
    clock.advance(minutes=step_min)
  raise AssertionError(f"never reached {state}: {route(w) and route(w).state}")


def set_device(w, **values) -> None:
  with w.sessionmaker() as s:
    d = s.get(Device, DONGLE)
    for k, v in values.items():
      setattr(d, k, v)
    s.commit()


def scopes_stale(w) -> None:
  """Have the next pass ask the site `GET /me`: the daemon only hears the headers on calls it makes."""
  with w.sessionmaker() as s:
    db.put(s, "token_info", {**db.get(s, "token_info"), "checked": 0})
    s.commit()


def web(w) -> TestClient:
  return TestClient(create_app(w.config, w.sessionmaker, w, comma_factory=w.comma_factory, site_factory=w.site_factory))


@pytest.fixture
def notes(monkeypatch):
  sent = []
  monkeypatch.setattr(notify, "send", lambda s, title, body: sent.append((title, body)) or 1)
  return sent


# ----- A: the API's deprecation headers -----

def test_deprecation_headers_are_read_defensively():
  full = deprecation({**{k.lower(): v for k, v in DEPRECATED.items()}, "sunset": SUNSET})
  assert full == {"since": 1798761600, "sunset": 1809129600, "link": "https://site.test/docs/api"}
  assert deprecation({"deprecation": "@1798761600", "sunset": "next tuesday", "link": "<javascript:x>; rel=deprecation"}) \
    == {"since": 1798761600, "sunset": None, "link": None}
  assert deprecation({"deprecation": "@1798761600", "link": '<https://site.test/v2>; rel="successor-version", '
                      '<https://site.test/docs/api>; rel="alternate deprecation"'})["link"] == "https://site.test/docs/api"
  for bad in ("true", "Sun, 01 Nov 2026 00:00:00 GMT", "@", "@12abc", ""):
    assert deprecation({"deprecation": bad}) is None


def test_deprecation_is_shown_and_told_once_and_nothing_stops(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(headers=dict(DEPRECATED))
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED  # the API works until its sunset
  told = [b for t, b in notes if "update" in t.lower()]
  assert len(told) == 1 and "deprecated since 2027-01-01" in told[0] and "stops" not in told[0]
  page = web(w).get("/").text
  assert "deprecated since 2027-01-01" in page and 'href="https://site.test/docs/api"' in page
  site.headers["Sunset"] = SUNSET  # an end date is set: tell again
  scopes_stale(w)
  w.tick()
  told = [b for t, b in notes if "update" in t.lower()]
  assert len(told) == 2 and "stops working on 2027-05-01" in told[1]
  scopes_stale(w)
  w.tick()
  assert len([b for t, b in notes if "update" in t.lower()]) == 2


def test_after_the_sunset_it_keeps_trying_and_says_so(log_schema, clock, make_worker, notes, monkeypatch):
  monkeypatch.setattr("relay.web.app.now_utc", clock)
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(headers={**DEPRECATED, "Sunset": SUNSET})
  w = make_worker(comma, site)
  onboard(w)
  scopes_stale(w)
  w.tick()
  clock.advance(days=200)
  with w.sessionmaker() as s:
    db.put(s, "onboarded_at", (clock() - timedelta(days=1)).timestamp())
    s.get(Device, DONGLE).enabled_at = clock() - timedelta(days=1)
    s.commit()
  page = web(w).get("/").text
  assert "stopped working on 2027-05-01" in page
  w.tick()
  assert any(q.url.path.endswith("/me") or "/routes" in q.url.path for q in site.requests[-3:])


def test_undeprecated_clears_and_foreign_links_are_ignored(log_schema, clock, make_worker, notes):
  comma = FakeComma(clock, drive(log_schema))
  site = FakeSite(headers={"Deprecation": "@1798761600", "Link": '<https://evil.example/x>; rel="deprecation"'})
  w = make_worker(comma, site)
  onboard(w)
  scopes_stale(w)
  w.tick()
  page = web(w).get("/").text
  assert "evil.example" not in page and 'href="https://site.test/docs/api"' in page
  site.headers = {}
  scopes_stale(w)
  w.tick()
  with w.sessionmaker() as s:
    assert db.get(s, "deprecation") is None


# ----- B: verification findings -----

def test_safety_net_ends_a_public_route_after_a_zone_change(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  with w.sessionmaker() as s:
    pipeline.filter_changed(s)
    s.commit()
  with w.lock:  # the pipeline is busy elsewhere
    w.run_safety_net()
  assert not comma.public


def test_a_public_copy_of_a_device_turned_off_ends(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  set_device(w, enabled=False)
  w.tick()
  r = route(w)
  assert r.state == State.FAILED and not comma.public and site.withdrawn == ["sub1"] and r.cap_hit


def test_a_site_copy_of_a_device_turned_off_still_ends(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING)
  set_device(w, paused_reason="verify again")
  clock.advance(days=3, hours=1)
  w.tick()
  assert route(w).state == State.SUBMITTED and "still checking" in route(w).note


def test_a_site_copy_ends_at_the_window_even_while_the_site_is_down(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING)
  site.fail = {r"GET /submissions/sub1": [(503, {"error": "internal_error"})] * 10_000}
  for _ in range(100):
    clock.advance(hours=1)
    w.tick()
  assert route(w).state == State.SUBMITTED
  polls = len([q for q in site.requests if q.url.path.endswith("/submissions/sub1")])
  w.tick()
  assert len([q for q in site.requests if q.url.path.endswith("/submissions/sub1")]) == polls


def test_a_submission_gone_from_the_site_isnt_a_failure(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING)
  site.fail = {r"GET /submissions/sub1": [(404, {"error": "not_found"})]}
  clock.advance(minutes=6)
  w.tick()
  r = route(w)
  assert r.state == State.SUBMITTED and "no longer on the site" in r.note
  assert not any("failed" in t for t, _ in notes)


def test_a_failing_pass_stops_the_heartbeat(make_worker, clock, log_schema, monkeypatch):
  w = make_worker(FakeComma(clock, drive(log_schema)), FakeSite())
  w.config.tick_s = 0

  def failing():
    w.stop.set()
    raise RuntimeError("discover failed")
  monkeypatch.setattr(w, "tick", failing)
  w.last_beat = 0.0
  w.run_forever()
  assert w.last_beat == 0.0


def test_one_device_failing_to_list_doesnt_stop_the_others(log_schema, clock, make_worker):
  other = "fedcba9876543210"
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  comma.devices_payload = [{"dongle_id": d, "alias": d, "last_athena_ping": int(clock().timestamp())}
                           for d in (other, DONGLE)]
  real = comma.handler
  comma.handler = lambda r: httpx.Response(403) if other in r.url.path else real(r)
  w = make_worker(comma, site)
  onboard(w)
  with w.sessionmaker() as s:
    s.add(Device(dongle_id=other, alias="Shared", enabled=True, enabled_at=clock() - timedelta(days=1)))
    s.commit()
  w.tick()
  assert route(w) is not None


def test_heartbeats_between_download_and_upload_and_between_qlogs(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema, n=3)), FakeSite()
  events = []
  real_c, real_s = comma.handler, site.handler

  def c(request):
    if request.url.host == "files.test" and "qlog" not in request.url.path:
      events.append("download")
    return real_c(request)

  def st(request):
    if "/files/" in request.url.path:
      events.append("upload")
    if "/qlogs/" in request.url.path:
      events.append("qlog")
    return real_s(request)
  comma.handler, site.handler = c, st
  w = make_worker(comma, site)
  monkeypatch.setattr(w, "beat", lambda: events.append("beat"))
  onboard(w)
  run_until_settled(w, clock)
  seq = "".join({"download": "d", "upload": "u", "qlog": "q", "beat": "b"}[e] for e in events)
  assert "du" not in seq and "qq" not in seq and "d" in seq and seq.count("q") == 3


def test_a_safety_net_failure_doesnt_stop_the_pipeline(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  monkeypatch.setattr(w, "safety_net", lambda *a: (_ for _ in ()).throw(RuntimeError("database is locked")))
  with w.sessionmaker() as s:
    s.add(Route(fullname=f"{DONGLE}|00000001--aaaaaaaaaa", dongle_id=DONGLE, state=State.FAILED, public_since=clock()))
    s.commit()
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED


def test_a_route_deleted_under_the_safety_net_is_skipped(log_schema, clock, make_worker, caplog):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  real_lock = w.public_lock

  class DeletingLock:  # the route goes between the safety net listing it and reading it
    def __enter__(self):
      with w.sessionmaker() as other:
        if (r := other.get(Route, ROUTE)) is not None:
          other.delete(r)
          other.commit()
      return real_lock.__enter__()

    def __exit__(self, *exc):
      return real_lock.__exit__(*exc)
  w.public_lock = DeletingLock()
  caplog.set_level(logging.WARNING)
  w.run_safety_net()  # used to raise on the missing row
  assert route(w) is None and not [rec for rec in caplog.records if rec.levelno >= logging.WARNING]


def test_make_public_rereads_the_route_and_the_device(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  with w.sessionmaker() as s:
    r = s.get(Route, ROUTE)
    w.make_private(s, w.comma_factory("t"), r)
    s.get(Device, DONGLE)  # this session now holds the device as enabled
    set_device(w, enabled=False)
    with pytest.raises(pipeline.Waiting):
      w.make_public(s, w.comma_factory("t"), r)
    set_device(w, enabled=True)
    with w.sessionmaker() as other:
      other.get(Route, ROUTE).cap_hit = True
      other.commit()
    with pytest.raises(pipeline.PublicSpent):
      w.make_public(s, w.comma_factory("t"), r)
  assert not comma.public


def test_safety_net_rereads_what_the_pipeline_changed(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  client = w.comma_factory("t")
  with w.sessionmaker() as s:
    held = (s.get(Route, ROUTE), s.get(Device, DONGLE))  # held, as the pipeline holds them: the session keeps them
    w.safety_net(s, client, True)
    assert comma.public
    set_device(w, enabled=False)
    w.safety_net(s, client, True)
    assert not comma.public and held
  set_device(w, enabled=True)
  with w.sessionmaker() as s:
    w.make_public(s, client, s.get(Route, ROUTE))
  assert comma.public
  with w.sessionmaker() as s:
    held = (s.get(Route, ROUTE), s.get(Device, DONGLE))
    with w.sessionmaker() as other:
      other.get(Route, ROUTE).cap_hit = True
      other.commit()
    w.safety_net(s, client, True)
    assert not comma.public and held


def test_a_link_too_slow_for_a_file_ends_the_route(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()

  def too_slow(path, deadline_s):
    raise site_module._TooSlow()
    yield b""
  monkeypatch.setattr(site_module, "_chunks", too_slow)
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=60, step_min=120)
  r = route(w)
  assert r.state == State.FAILED and "took too long" in r.note


# ----- concurrency and threads -----

@pytest.mark.parametrize("seed", range(4))
def test_safety_net_thread_alongside_the_pipeline_keeps_the_cap(log_schema, clock, make_worker, monkeypatch, caplog,
                                                                seed):
  rnd = random.Random(seed)
  monkeypatch.setattr(pipeline, "SAFETY_NET_EVERY_S", 0.002)
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  real_c, real_s, glock, timeline = comma.handler, site.handler, threading.Lock(), []

  def slow_c(request):
    time.sleep(rnd.random() * 0.002)
    with glock:
      response = real_c(request)
      if request.method == "PATCH":
        timeline.append((clock(), comma.public))
    return response

  def slow_s(request):
    time.sleep(rnd.random() * 0.002)
    with glock:
      return real_s(request)
  comma.handler, site.handler = slow_c, slow_s
  w = make_worker(comma, site)
  w.config.tick_s = 0.001
  onboard(w, transfer="public")
  caplog.set_level(logging.WARNING)
  threads = [threading.Thread(target=w.run_safety_net_forever, daemon=True),
             threading.Thread(target=w.run_forever, daemon=True)]
  for t in threads:
    t.start()
  try:
    for i in range(400):
      time.sleep(0.004)
      with glock:
        clock.advance(minutes=rnd.choice([1, 2, 3]))
      if rnd.random() < 0.05:
        with w.sessionmaker() as s:
          d = s.get(Device, DONGLE)
          d.paused_reason = None if d.paused_reason else "paused"
          s.commit()
      r = route(w)
      if r and r.state in State.FINAL and not comma.public and i > 50:
        break
  finally:
    w.stop.set()
    w.wake.set()
    for t in threads:
      t.join(10)
  total, since = timedelta(0), None
  for at, public in timeline:
    if public and since is None:
      since = at
    elif not public and since is not None:
      total, since = total + (at - since), None
  if since is not None:
    total += clock() - since
  errors = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
  assert total <= timedelta(hours=6, minutes=6), total
  assert not any("locked" in e or "pass failed" in e for e in errors), errors


def test_the_app_starts_the_safety_net_thread(tmp_path, clock):
  config = Config.from_env({"DATA_DIR": str(tmp_path), "POLARIS_HOST": "https://site.test", "ALLOWED_HOSTS": "testserver",
                            "INSECURE_ALLOW_NO_AUTH": "1"})
  from relay.db import make_sessionmaker
  app = create_app(config, make_sessionmaker(config.data_dir), start_worker=True)
  with TestClient(app):
    names = {t.name for t in threading.enumerate()}
    assert {"relay-worker", "relay-safety-net"} <= names
    assert app.state.worker.started


def test_each_route_in_a_pass_beats(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  newer = int((clock() - timedelta(minutes=1)).timestamp() * 1000)
  comma.extra_routes = [{"fullname": f"{DONGLE}|00000043--0b1c2d3e4f", "start_time_utc_millis": newer,
                         "end_time_utc_millis": newer + 60_000, "maxqlog": 3, "procqlog": 0, "segment_numbers": [0]}]
  w = make_worker(comma, site)
  onboard(w)
  w.tick()  # both routes found, both waiting for their logs
  beats = []
  monkeypatch.setattr(w, "beat", lambda: beats.append(1))
  clock.advance(minutes=6)
  w.tick()
  assert len(beats) == 2



def test_deprecation_edge_values_never_break_pages(log_schema, clock, make_worker, notes):
  assert deprecation({"deprecation": "@999999999999"}) is None  # past year 9999
  assert deprecation({"deprecation": "@1798761600", "sunset": "Fri, 01 Jan 10000 00:00:00 GMT"})["sunset"] is None
  comma = FakeComma(clock, drive(log_schema))
  site = FakeSite(headers={"Deprecation": "@999999999999"})
  w = make_worker(comma, site)
  onboard(w)
  scopes_stale(w)
  w.tick()
  assert web(w).get("/").status_code == 200


def test_a_proxy_error_page_doesnt_clear_the_notice_and_a_down_service_is_retried(log_schema, clock, make_worker,
                                                                                    monkeypatch):
  tries = []
  monkeypatch.setattr(notify, "send", lambda s, title, body: tries.append(title) or (0 if len(tries) == 1 else 1))
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(headers=dict(DEPRECATED))
  w = make_worker(comma, site)
  onboard(w)
  scopes_stale(w)
  w.tick()  # the service is down: nothing took it
  scopes_stale(w)
  w.tick()  # tried again, taken
  assert len([t for t in tries if "update" in t.lower()]) == 2
  real = site.handler
  site.handler = lambda request: httpx.Response(502, text="<html>bad gateway</html>",
                                                headers={"content-type": "text/html"})
  scopes_stale(w)
  w.tick()
  site.handler = real
  with w.sessionmaker() as s:
    assert db.get(s, "deprecation")["since"] == 1798761600


def test_a_zone_after_a_lost_public_submit_ends_the_copy(log_schema, clock, make_worker, notes):
  from tests.test_hardening import add_home_zone, drive as home_drive
  comma, site = FakeComma(clock, home_drive(log_schema, home=(1,), n=3)), FakeSite()
  real, lost = site.handler, []

  def lose_answer(request):
    response = real(request)
    if request.url.path.endswith("/submit") and not lost:
      lost.append(1)
      add_home_zone(w)  # the zone lands while the answer is lost
      raise httpx.ReadTimeout("lost")
    return response
  site.handler = lose_answer
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  for _ in range(10):
    w.tick()
    clock.advance(minutes=6)
  assert comma.public_changes.count(True) == 1 and not comma.public  # never public again for the repeat
  assert route(w).state == State.FAILED and site.withdrawn == ["sub1"]


def test_a_safety_net_notice_failing_doesnt_stop_the_other_routes(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  other = f"{DONGLE}|00000001--aaaaaaaaaa"
  with w.sessionmaker() as s:
    s.add(Route(fullname=other, dongle_id=DONGLE, state=State.FAILED, public_since=clock()))
    s.commit()
  real = w.make_private

  def fail_for_other(s, c, r):
    if r.fullname == other:
      raise RuntimeError("comma failed for this one")
    return real(s, c, r)
  monkeypatch.setattr(w, "make_private", fail_for_other)
  monkeypatch.setattr(notify, "event", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("database is locked")))
  set_device(w, enabled=False)
  w.run_safety_net()  # doesn't raise
  assert not comma.public


def test_a_withdraw_the_site_never_answers_ends_after_the_files_window(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC)
  site.fail = {r"POST /submissions/sub1/withdraw": [(503, {"error": "busy"})] * 10000,
               r"GET /submissions/sub1": [(503, {"error": "busy"})] * 10000}
  set_device(w, enabled=False)  # ends the copy: private, then withdraw, which never answers
  for _ in range(12):
    w.tick()
    clock.advance(hours=8)
  r = route(w)
  assert r.state == State.FAILED and "withdraw it on the site" in r.note and not comma.public
