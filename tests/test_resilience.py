import re
import sqlite3
import time
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient

from relay import db, notify, pipeline
from relay.comma import CommaClient, CommaError
from relay.db import Device, Notifier, Route
from relay.pipeline import State, describe, spoken
from relay.site import SiteClient, SiteError
from relay.web.app import create_app
from tests.conftest import (AFTER_DRIVE, DONGLE, FAR, ROUTE, ROUTE_NAME, T0, FakeComma, FakeSite, onboard, qlog,
                            run_until_settled)

ORIGIN = {"origin": "http://testserver"}


def drive(log_schema, n=2):
  return {seg: qlog(log_schema, [(FAR[0] + seg * 0.001, FAR[1])] * 3) for seg in range(n)}


def route(w) -> Route:
  with w.sessionmaker() as s:
    return s.get(Route, ROUTE)


def edit(w, **values) -> None:
  with w.sessionmaker() as s:
    r = s.get(Route, ROUTE)
    for k, v in values.items():
      setattr(r, k, v)
    s.commit()


def until(w, clock, state, ticks=12, step_min=31) -> None:
  for _ in range(ticks):
    if (r := route(w)) is not None and r.state == state:
      return
    w.tick()
    clock.advance(minutes=step_min)
  raise AssertionError(f"never reached {state}: {route(w) and route(w).state}")


@pytest.fixture
def notes(monkeypatch):
  sent = []
  monkeypatch.setattr(notify, "send", lambda s, title, body: sent.append((title, body)) or 1)
  return sent


def web(w) -> TestClient:
  return TestClient(create_app(w.config, w.sessionmaker, w, comma_factory=w.comma_factory, site_factory=w.site_factory))


def queue_reads(comma) -> int:
  return sum(1 for r in comma.requests if r.url.host == "athena.test" and b"listUploadQueue" in r.content)


def vanish_once(w, comma, clock) -> None:
  comma.queue.clear()  # a restart, or a cancel in comma connect
  clock.advance(minutes=6)
  w.tick()


# ----- review findings -----

def test_activity_renders_after_files_leave_the_queue(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  vanish_once(w, comma, clock)
  assert route(w).upload_vanished == 1 and all(isinstance(v, str) for v in route(w).timings.values())
  edit(w, timings={**route(w).timings, "upload_vanished": 2})  # what the bug stored: the page mustn't trip on it
  page = web(w).get("/")
  assert page.status_code == 200 and "left the upload queue" in page.text


def test_a_failure_while_ending_a_spent_public_route_stays_with_the_route(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC, step_min=10)
  w.tick()
  # Made private while paused, and first public more than 2 days ago: it can't be made public again.
  edit(w, public_since=None, first_public_at=clock() - timedelta(days=3))
  comma.public = False
  site.fail = {r"POST /submissions/sub1/withdraw": [(400, {"error": "bad_request"})]}
  w.tick()  # used to escape the pass
  assert route(w).attempts == 1 and "the site answered 400" in route(w).error


def test_files_the_device_no_longer_has_drop_their_moments(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), gone={(1, "cameras")}), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.SUBMITTED and r.upload_unavailable == ["1/cameras"] and r.upload_vanished == 0
  assert site.submits[0][1]["suggestions"] == ["s-0"]


def test_every_moment_needing_lost_files_ends_the_route(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), gone={(0, "logs"), (1, "logs")}), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.EXPIRED and "no longer has the files" in r.note and site.deleted == 1


def test_a_request_held_while_offline_isnt_read_as_vanished_on_reconnect(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False, online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  w.tick()
  clock.advance(minutes=30)
  comma.online, comma.last_ping = True, None  # back; athena hasn't delivered the held request yet
  w.tick()
  assert route(w).upload_vanished == 0 and queue_reads(comma) == 0


def test_no_queue_read_while_offline(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  reads = queue_reads(comma)
  comma.online, comma.last_ping = False, clock() - timedelta(hours=1)
  clock.advance(minutes=10)
  w.tick()
  r = route(w)
  assert r.upload_retry_at is None and queue_reads(comma) == reads and not r.device_online


def test_no_fallback_re_ask_while_offline(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  asked = len(comma.upload_paths)
  for _ in range(8):
    clock.advance(hours=1)
    w.tick()
  assert len(comma.upload_paths) == asked


def test_requesting_without_submit_records_and_drops_the_handle(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  edit(w, state=State.REQUESTING)
  with w.sessionmaker() as s:
    db.put(s, "token_info", {"scopes": ["record_drive"], "checked": clock().timestamp()})
    s.commit()
  w.tick()
  assert route(w).state == State.RECORDED and site.deleted == 1


def test_waits_reset_once_the_files_arrive(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  vanish_once(w, comma, clock)
  assert route(w).upload_wait_s == 600
  comma.uploaded |= {(seg, kind) for seg, kinds in route(w).needs.items() for kind in kinds for seg in [int(seg)]}
  clock.advance(minutes=11)
  w.tick()
  r = route(w)
  assert r.upload_wait_s == 0 and r.upload_retry_at is None and r.state == State.SITE_COPYING


def test_the_wait_doubles_up_to_twelve_hours_and_says_hours(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  for _ in range(9):
    vanish_once(w, comma, clock)
    clock.advance(seconds=route(w).upload_wait_s + 60)
    w.tick()  # asked again
  r = route(w)
  assert r.upload_wait_s == 12 * 3600
  vanish_once(w, comma, clock)
  assert "asking again in 12 hours" in route(w).note
  assert spoken(timedelta(minutes=80)) == "1.3 hours" and spoken(timedelta(minutes=1)) == "1 minute"


def test_a_queue_path_without_a_prefix_still_counts(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  real, reads = comma.handler, []

  def bare(request):
    if request.url.host == "athena.test" and b"listUploadQueue" in request.content:
      reads.append(1)
      return httpx.Response(200, json={"id": 0, "jsonrpc": "2.0", "result": [{"path": p} for p in comma.queue]})
    return real(request)
  comma.handler = bare
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  clock.advance(minutes=5)
  w.tick()
  assert reads and route(w).upload_vanished == 0


def test_a_newer_route_found_by_discovery_resets_the_wait(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  vanish_once(w, comma, clock)
  assert route(w).upload_wait_s == 600
  newer = int((clock() - timedelta(minutes=1)).timestamp() * 1000)
  comma.extra_routes = [{"fullname": f"{DONGLE}|00000043--0b1c2d3e4f", "start_time_utc_millis": newer,
                         "end_time_utc_millis": newer + 60_000, "maxqlog": 3, "procqlog": 0,
                         "segment_numbers": [0]}]  # still uploading, so it stays waiting
  asked = len(comma.upload_paths)
  clock.advance(minutes=6)
  w.tick()
  assert route(w).upload_wait_s == 0 and len(comma.upload_paths) > asked


def test_scope_refresh_survives_a_down_or_garbled_site(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(fail={r"GET /me": [(503, {})] * 3})
  w = make_worker(comma, site)
  onboard(w)
  with w.sessionmaker() as s:
    db.put(s, "token_info", {"scopes": ["submit", "record_drive"], "checked": 0})
    s.commit()
  run_until_settled(w, clock, ticks=3)
  real = site.handler
  site.handler = lambda r: httpx.Response(200, text="<html>") if r.url.path.endswith("/me") else real(r)
  run_until_settled(w, clock, ticks=6)
  assert route(w).state == State.SUBMITTED


def test_settings_show_the_saved_token(log_schema, clock, make_worker):
  w = make_worker(FakeComma(clock, drive(log_schema)), FakeSite())
  client = web(w)
  token = re.search(r'name="csrf" value="([^"]+)"', client.get("/setup").text)[1]
  client.post("/setup/token", data={"csrf": token, "token": "opu_abc"}, headers=ORIGIN)
  page = client.get("/setup").text
  assert 'Saved: "relay" for tester' in page and "submit, record_drive" in page


# ----- outages, unexpected answers, threads -----

def test_a_long_site_outage_waits_instead_of_failing(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  site.fail = {r"POST /routes": [(503, {"error": "internal_error"})] * 40}
  w = make_worker(comma, site)
  onboard(w)
  for _ in range(40):  # about 20 hours
    w.tick()
    clock.advance(minutes=31)
  r = route(w)
  assert r.state == State.SUGGESTING and r.attempts == 0 and r.note == "Waiting for the site to answer."
  site.fail = {}
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED


def test_comma_unreachable_waits_too(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  real, down = comma.handler, [True]

  def flaky(request):
    if down[0] and request.url.path.endswith("/files"):
      raise httpx.ConnectError("no route to host")
    return real(request)
  comma.handler = flaky
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=30)
  assert route(w).attempts == 0 and route(w).note == "Waiting for comma to answer."
  down[0] = False
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED


def test_site_storage_full_defers_without_an_attempt(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  site.fail = {r"PUT /routes/h1/qlogs/0": [(507, {"error": "storage_full", "retry_after": 1800})]}
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SUGGESTING, step_min=10)
  r = route(w)
  assert r.deferred and r.attempts == 0 and "short of space" in r.note
  assert r.next_attempt_at - clock() > timedelta(minutes=15)
  clock.advance(minutes=31)
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED


def test_site_internal_error_is_an_outage(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  site.fail = {r"POST /routes/h1/suggest": [(500, {"error": "internal_error"})]}
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SUGGESTING, step_min=10)
  assert route(w).attempts == 0 and route(w).outages == 1 and route(w).note == "Waiting for the site to answer."


def test_frozen_qlogs_end_the_route(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  site.fail = {r"PUT /routes/h1/qlogs/0": [(409, {"error": "qlogs_frozen"})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.FAILED and route(w).note == pipeline.TERMINAL["qlogs_frozen"]


def test_an_unexpected_answer_fails_an_attempt_not_the_pass(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  site.fail = {r"POST /routes": [(200, {"no": "id"})]}
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SUGGESTING, step_min=10)
  r = route(w)
  assert r.attempts == 1 and r.error == "an unexpected answer (KeyError)"


def test_the_safety_net_runs_while_the_pipeline_is_busy(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  until(w, clock, State.PUBLIC, step_min=10)
  clock.advance(hours=7)
  with w.lock:  # a long step holds the pipeline
    w.run_safety_net()
  assert not comma.public and route(w).cap_hit


def test_file_transfers_have_a_deadline(tmp_path):
  comma = CommaClient("t", "https://comma.test",
                      transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"x" * (3 << 20))))
  with pytest.raises(CommaError) as e:
    comma.download("https://files.test/a/b/0/fcamera.hevc", tmp_path / "f", deadline_s=-1)
  assert e.value.transient and not (tmp_path / "f").exists()
  path = tmp_path / "up"
  path.write_bytes(b"x" * (3 << 20))
  site = SiteClient("https://site.test", "opu_x", transport=httpx.MockTransport(
    lambda r: (r.read(), httpx.Response(200, json={}))[1]))
  with pytest.raises(SiteError) as e:
    site.put_file("h1", 0, "cameras", path, "0" * 64, "fcamera.hevc", deadline_s=-1)
  assert e.value.code == "timeout" and e.value.transient


def test_healthz_reports_a_stalled_worker(log_schema, clock, make_worker):
  w = make_worker(FakeComma(clock, drive(log_schema)), FakeSite())
  client = web(w)
  assert client.get("/healthz").status_code == 200
  w.started = True
  w.last_beat = time.monotonic() - (3 * w.config.tick_s + pipeline.STEP_BUDGET_S + 1)
  assert client.get("/healthz").status_code == 503
  w.beat()
  w.last_net = time.monotonic() - 3600
  assert client.get("/healthz").status_code == 503


def test_a_route_never_filtered_is_filtered_before_anything_is_asked(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  edit(w, filter_gen=None)
  qlog_reads = lambda: sum(1 for q in comma.requests if q.url.host == "files.test" and "qlog" in q.url.path)
  before = qlog_reads()
  w.tick()
  assert route(w).filter_gen is not None and qlog_reads() > before


def test_routes_of_a_disabled_device_still_expire(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  with w.sessionmaker() as s:
    s.get(Device, DONGLE).enabled = False
    s.commit()
  clock.advance(days=7)
  w.tick()
  assert route(w).state == State.EXPIRED and site.deleted == 1


def test_route_list_pages_back():
  pages = []

  def handler(request):
    end, limit = int(request.url.params["end"]), int(request.url.params["limit"])
    pages.append(end)
    t0 = int(T0.timestamp() * 1000)
    starts = [t for t in (t0 - k * 60_000 for k in range(10)) if t <= end][:limit]
    return httpx.Response(200, json=[{"fullname": f"{DONGLE}|r{t}", "start_time_utc_millis": t} for t in starts])
  comma = CommaClient("t", "https://comma.test", transport=httpx.MockTransport(handler))
  found = comma.routes(DONGLE, T0 - timedelta(days=3650), T0, limit=3)
  assert len(found) == 10 and len(pages) == 4


def test_no_write_lock_across_reading_the_queue(log_schema, clock, make_worker, config):
  comma, site = FakeComma(clock, drive(log_schema), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.WAITING_FILES)
  real, locked = comma.handler, []

  def check(request):
    if request.url.host == "athena.test" and b"listUploadQueue" in request.content:
      con = sqlite3.connect(config.data_dir / "relay.db", timeout=0)
      try:
        con.execute("BEGIN IMMEDIATE")
        con.rollback()
      except sqlite3.OperationalError:
        locked.append(1)
      finally:
        con.close()
    return real(request)
  comma.handler = check
  comma.qlogs[2] = comma.qlogs[1]  # more logs arrive: the route notes it, a write
  clock.advance(minutes=5)
  w.tick()
  assert queue_reads(comma) and route(w).grew_after_ready and not locked


def test_route_notifications_are_keyed_and_pruned(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True, copy_error="disk")
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING, step_min=10)
  run_until_settled(w, clock, ticks=3)
  with w.sessionmaker() as s:
    keys = [k for (k,) in s.execute(db.Setting.__table__.select().with_only_columns(db.Setting.key))]
  assert any(k.startswith("notified:copy_error:") for k in keys) and not any(ROUTE_NAME in k for k in keys)
  clock.advance(days=31)
  w.tick()
  with w.sessionmaker() as s:
    keys = [k for (k,) in s.execute(db.Setting.__table__.select().with_only_columns(db.Setting.key))]
  assert route(w) is None and not any(k.startswith("notified:copy_error:") for k in keys)


def test_names_and_errors_carry_no_route(log_schema, clock, make_worker):
  import hashlib
  w = make_worker(FakeComma(clock, drive(log_schema)), FakeSite())
  r = Route(fullname=ROUTE, dongle_id=DONGLE, state=State.SUGGESTING)
  assert w.work_dir(r).name != hashlib.sha256(ROUTE.encode()).hexdigest()[:16]
  message = describe(FileNotFoundError(2, "No such file", str(w.work_dir(r) / "qlogs" / "0")))
  assert w.work_dir(r).name not in message and "FileNotFoundError" in message


def test_relay_submit_waits_for_the_site_copy(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING, step_min=10)
  site.copy_error = "comma said no"
  w.tick()
  assert route(w).state == State.SITE_COPYING
  clock.advance(minutes=31)
  w.tick()
  r = route(w)
  assert r.state == State.SUBMITTED and "trouble with the files" in r.note
  body = next(b for t, b in notes if "couldn't take" in t)
  assert "https://site.test/status" in body and ROUTE_NAME not in body


def test_relay_copy_watch_gives_up_after_the_files_window(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite(hold_copy=True)
  w = make_worker(comma, site)
  onboard(w)
  until(w, clock, State.SITE_COPYING, step_min=10)
  clock.advance(days=3, hours=1)
  w.tick()
  assert route(w).state == State.SUBMITTED and "still checking" in route(w).note


def test_a_hung_notifier_is_left_behind_and_tried_again_later(log_schema, clock, make_worker, monkeypatch):
  monkeypatch.setattr(notify, "SEND_TIMEOUT_S", 0.3)
  monkeypatch.setattr(notify, "send_to", lambda n, title, body: time.sleep(5) if n.service == "ntfy" else True)
  w = make_worker(FakeComma(clock, drive(log_schema)), FakeSite())
  with w.sessionmaker() as s:
    s.add(Notifier(service="ntfy", fields={"topic": "t"}))
    s.commit()
    started = time.monotonic()
    assert notify.event(s, "comma_token", "title", "body") is False
    assert time.monotonic() - started < 2 and db.get(s, "notified:comma_token") is None  # not delivered: try again
    s.add(Notifier(service="discord", fields={}))
    s.commit()
    assert notify.event(s, "comma_token", "title", "body") is True  # one target took it while the other hung
    assert db.get(s, "notified:comma_token") is not None
