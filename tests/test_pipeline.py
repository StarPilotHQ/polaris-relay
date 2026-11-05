from datetime import timedelta

import httpx

from relay import db
from relay.db import Device, Route
from relay.pipeline import State
from tests.conftest import AFTER_DRIVE, DONGLE, FAR, ROUTE, FakeComma, FakeSite, onboard, qlog, run_until_settled


def plain_drive(log_schema, n=4):
  return {seg: qlog(log_schema, [(FAR[0] + seg * 0.001, FAR[1])] * 3) for seg in range(n)}


def route(w) -> Route:
  with w.sessionmaker() as s:
    return s.get(Route, ROUTE)


def test_auto_submit_relay_end_to_end(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.SUBMITTED and r.submitted == 4 and r.needs_input == 2
  assert site.drives == 1
  assert set(site.files) == {(s, k) for s in range(4) for k in ("rlog", "fcamera", "ecamera", "qcamera")}
  assert all(n == 1 for n in site.files.values())
  assert not comma.public_changes
  assert set(r.timings) >= {"pinged", "quiet", "settled", "contiguous", "ready"}
  assert not (w.config.data_dir / "work").exists() or not any((w.config.data_dir / "work").iterdir())


def test_qlogs_wait_until_quiet(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  w.tick()
  clock.advance(minutes=5)
  w.tick()
  assert route(w).state == State.WAITING_QLOGS
  assert not site.requests


def test_regrown_route_goes_back_to_waiting(log_schema, clock, make_worker, monkeypatch):
  comma, site = FakeComma(clock, plain_drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  w.tick()
  clock.advance(minutes=31)
  real = comma.handler

  def grown(request):
    if request.method == "GET" and request.url.path == f"/v1/route/{ROUTE}/":
      obj = comma.route_obj()
      obj["maxqlog"] = obj["procqlog"] = 4
      return httpx.Response(200, json=obj)
    return real(request)
  monkeypatch.setattr(comma, "handler", grown)
  w.tick()
  r = route(w)
  assert r.state == State.WAITING_QLOGS and r.grew_after_ready
  assert not [q for q in site.requests if q.method == "PUT"]


def test_resumes_after_restart_mid_transfer(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  site.fail = {r"PUT /routes/h1/files/1/fcamera": [(500, {"error": "boom"})] * 3}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  assert route(w).state == State.TRANSFERRING
  sent_before = dict(site.files)
  w2 = make_worker(comma, site)  # a restart: a fresh worker and database connection on the same volume
  run_until_settled(w2, clock, ticks=4)
  r = route(w2)
  assert r.state == State.SUBMITTED
  assert all(site.files[k] == 1 for k in sent_before)
  assert len(site.submits) == 1


def test_lost_submit_answer_retries_with_same_key(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  real = site.handler
  calls = []

  def flaky(request):
    if request.url.path.endswith("/submit") and not calls:
      calls.append(request.headers["idempotency-key"])
      raise httpx.ReadTimeout("lost")
    return real(request)
  site.handler = flaky
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=8)
  assert route(w).state == State.SUBMITTED
  assert site.submits[0][0] == calls[0]


def test_public_transfer_makes_private_after_copy(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4, step_min=31)
  assert route(w).state == State.PUBLIC and comma.public
  site.status = "ready_for_review"
  run_until_settled(w, clock, ticks=1)
  r = route(w)
  assert r.state == State.SUBMITTED and not comma.public and r.public_since is None
  assert comma.public_changes == [True, False]
  assert not site.files


def test_public_cap_makes_private_and_withdraws(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  assert route(w).state == State.PUBLIC
  clock.advance(hours=7)
  w.tick()
  r = route(w)
  assert r.state == State.FAILED and not comma.public and site.withdrawn == ["sub1"]


def test_public_route_made_private_after_crash(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    s.get(Device, DONGLE).enabled = False  # the user turned the device off while it was public
    s.commit()
  w2 = make_worker(comma, site)
  w2.tick()
  assert not comma.public and route(w2).public_since is None


def test_rate_limit_defers(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  site.fail = {r"POST /routes/h1/submit": [(429, {"error": "rate_limited", "limit": "ranges", "retry_after": 3600})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4, step_min=10)
  r = route(w)
  assert r.state == State.SUBMITTING and r.deferred and "daily range limit" in r.note
  clock.advance(hours=1)
  run_until_settled(w, clock, ticks=2)
  assert route(w).state == State.SUBMITTED


def test_unverified_device_pauses(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite(verified=False)
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    d = s.get(Device, DONGLE)
    assert d.paused_reason and "verified again" in d.paused_reason
  assert route(w).state == State.SUGGESTING


def test_rejected_access_token_pauses(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  site.fail = {r"POST /routes": [(401, {"error": "invalid_token"})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    assert db.get(s, "pause:site")


def test_suggest_only_requests_uploads_and_stops(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, mode="suggest")
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.SUGGESTED and r.link == "https://site.test/r"
  assert comma.upload_paths and not site.submits and not site.files


def test_offline_device_is_waiting_then_expires(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2), online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  r = route(w)
  assert r.state == State.WAITING_FILES and r.attempts == 0
  clock.advance(days=3, hours=1)
  w.tick()
  assert route(w).state == State.EXPIRED


def test_not_submittable_records_drive_only(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite(submittable=False)
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.RECORDED and site.drives == 1 and not comma.upload_paths


def test_routes_before_onboarding_are_ignored(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema, 2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  with w.sessionmaker() as s:
    db.put(s, "onboarded_at", (clock() + timedelta(minutes=1)).timestamp())
    s.get(Device, DONGLE).enabled_at = None
    s.commit()
  run_until_settled(w, clock, ticks=2)
  assert route(w) is None
