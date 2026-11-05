import html
import re
import sqlite3
from datetime import timedelta

import bcrypt
import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from relay import db, notify, pipeline
from relay.comma import CommaClient
from relay.config import Config
from relay.db import Device, Route, Zone, make_sessionmaker
from relay.pipeline import State, filter_changed
from relay.site import SiteClient
from relay.web.app import create_app, tile_url_ok
from tests.conftest import AFTER_DRIVE, DONGLE, FAR, HOME, ROUTE, FakeComma, FakeSite, onboard, qlog, run_until_settled

ORIGIN = {"origin": "http://testserver"}


def drive(log_schema, home=(), n=4):
  return {seg: qlog(log_schema, [FAR, (HOME[0] + 0.0005, HOME[1]), FAR] if seg in home else
                    [(FAR[0] + seg * 0.001, FAR[1])] * 3) for seg in range(n)}


def route(w) -> Route:
  with w.sessionmaker() as s:
    return s.get(Route, ROUTE)


def sent_file_segments(site: FakeSite) -> set[int]:
  return {int(re.search(r"/files/(\d+)/", r.url.path)[1]) for r in site.requests if "/files/" in r.url.path}


@pytest.fixture
def notes(monkeypatch):
  sent = []
  monkeypatch.setattr(notify, "send", lambda s, title, body: sent.append((title, body)) or 1)
  return sent


def add_home_zone(w) -> None:
  with w.sessionmaker() as s:
    s.add(Zone(kind="circle", lat=HOME[0], lng=HOME[1], radius_m=200))
    filter_changed(s)
    s.commit()


def test_zone_added_mid_transfer_stops_the_loop(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, home=(2,))), FakeSite()
  w = make_worker(comma, site)
  real = site.handler

  def hook(request):
    response = real(request)
    if request.url.path.endswith("/files/0/rlog"):
      add_home_zone(w)  # the user adds a zone while the first file is uploading
    return response
  site.handler = hook
  onboard(w)
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.SUBMITTED and 2 in r.excluded
  assert 2 not in sent_file_segments(site)
  assert [c["excluded"] for c in site.suggest_calls] == [[], [2]]
  assert "s-2" not in site.submits[0][1]["suggestions"]


def test_cap_applies_while_paused_and_is_never_reopened(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  assert route(w).state == State.PUBLIC and comma.public
  with w.sessionmaker() as s:
    db.put(s, "pause:site", "paused for the test")
    s.commit()
  clock.advance(hours=7)
  w.tick()
  r = route(w)
  assert not comma.public and r.cap_hit and r.public_seconds > 6 * 3600
  with w.sessionmaker() as s:
    db.delete(s, "pause:site")
    s.commit()
  w.tick()
  assert route(w).state == State.FAILED and site.withdrawn == ["sub1"]
  assert comma.public_changes == [True, False]


def test_public_time_adds_up_across_stretches(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4, step_min=31)
  assert route(w).state == State.PUBLIC
  clock.advance(hours=4)
  with w.sessionmaker() as s:  # paused: made private, but the copy isn't over
    db.put(s, "pause:site", "paused for the test")
    s.commit()
  w.tick()
  assert not comma.public
  with w.sessionmaker() as s:
    db.delete(s, "pause:site")
    s.commit()
  w.tick()
  assert comma.public  # made public again to finish the copy
  clock.advance(hours=2, minutes=30)
  w.tick()
  assert route(w).state == State.FAILED and not comma.public


def test_safety_net_survives_a_failing_route(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  real = comma.handler
  answers = [500, 404]

  def flaky(request):
    if request.method == "PATCH" and answers:
      return httpx.Response(answers.pop(0))
    return real(request)
  comma.handler = flaky
  with w.sessionmaker() as s:
    db.put(s, "pause:site", "paused for the test")
    s.commit()
  w.tick()
  assert route(w).public_since is not None and any("still be public" in t for t, _ in notes)
  w.tick()
  assert route(w).public_since is None  # comma's 404 means it's already gone, so nothing is public


def test_comma_refusal_counts_public_routes(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  comma.handler = lambda request: httpx.Response(401)
  w.tick()
  with w.sessionmaker() as s:
    assert db.get(s, "pause:comma")
  assert any("1 drive may still be public" in body for _, body in notes)


def test_forbidden_route_is_not_an_auth_failure(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  real = comma.handler
  comma.handler = lambda r: httpx.Response(403) if r.url.path.endswith("/files") else real(r)
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=3)
  with w.sessionmaker() as s:
    assert not db.get(s, "pause:comma")
  assert route(w).attempts > 0


def test_a_file_the_site_says_is_missing_is_sent_again(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  site.fail = {r"POST /routes/h1/submit": [(409, {"error": "files_missing", "missing": [{"segment": 0, "kind": "rlog"}]})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED and site.files[(0, "rlog")] == 2 and site.files[(0, "fcamera")] == 1


def test_a_file_kind_the_site_refuses_isnt_counted_as_sent(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  site.fail = {r"PUT /routes/h1/files/\d+/\w+": [(400, {"error": "kind_not_allowed", "message": "no"})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.FAILED and not site.submits and not route(w).relayed


def test_the_activity_list_says_which_file_is_moving(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  real, notes = site.handler, []

  def hook(request):
    if "/files/" in request.url.path:
      notes.append(route(w).note)
    return real(request)
  site.handler = hook
  onboard(w)
  run_until_settled(w, clock)
  assert notes[:2] == ["Uploading file 1 of 8.", "Uploading file 2 of 8."] and notes[-1] == "Uploading file 8 of 8."
  assert route(w).state == State.SUBMITTED and "file" not in route(w).note


def installed_before_the_file_names_fix(w, **fields):
  with w.sessionmaker() as s:
    s.delete(s.get(db.Setting, "repair:relayed_file_names"))
    r = s.get(Route, ROUTE)
    for name, value in fields.items():
      setattr(r, name, value)
    s.commit()


def test_updating_sends_the_files_of_a_drive_stuck_by_the_old_names(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30)
  for _ in range(20):
    w.tick()
    clock.advance(minutes=6)
    if route(w).state == State.READY:
      break
  sent = list(site.files)
  assert route(w).state == State.READY and sent
  site.files.clear()
  installed_before_the_file_names_fix(w, state=State.TRANSFERRING, regressions=4,
                                      next_attempt_at=clock() + timedelta(hours=1))
  w2 = make_worker(comma, site)
  assert route(w2).regressions == 0 and route(w2).next_attempt_at is None and not route(w2).relayed
  w2.tick()
  assert set(site.files) == set(sent) and all(n == 1 for n in site.files.values())


def test_updating_starts_over_a_drive_failed_by_the_old_names_once(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30)
  for _ in range(20):
    w.tick()
    clock.advance(minutes=6)
    if route(w).state == State.READY:
      break
  site.files.clear()
  failed = {"state": State.FAILED, "regressions": 5, "finished_at": clock(),
            "note": "The site kept finding files missing, so this drive was given up."}
  installed_before_the_file_names_fix(w, **failed)
  w2 = make_worker(comma, site)
  assert route(w2).state == State.SUGGESTING and route(w2).finished_at is None and route(w2).handle is None
  with w2.sessionmaker() as s:
    db.put(s, "submit_delay_min", 0)
    s.commit()
  run_until_settled(w2, clock)
  assert route(w2).state == State.SUBMITTED and len(site.submits) == 1 and site.files
  with w2.sessionmaker() as s:
    r = s.get(Route, ROUTE)
    r.state, r.note = failed["state"], failed["note"]
    s.commit()
  assert route(make_worker(comma, site)).state == State.FAILED


def test_files_missing_loop_backs_off_and_ends(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  site.fail = {r"POST /routes/h1/submit": [(409, {"error": "files_missing", "missing": []})] * 50}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=30, step_min=61)
  r = route(w)
  assert r.state == State.FAILED and r.regressions == pipeline.MAX_REGRESSIONS
  assert len([q for q in site.requests if q.url.path.endswith("/submit")]) == pipeline.MAX_REGRESSIONS


def test_token_without_submit_never_requests_uploads(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, scopes=("record_drive",))
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == State.RECORDED and "can't submit" in r.note
  assert not comma.upload_paths and not site.submits and site.drives == 1


@pytest.mark.parametrize("pattern,status,code,state", [
  (r"POST /routes/h1/suggest", 422, "route_unusable", State.FAILED),
  (r"POST /routes/h1/submit", 409, "idempotency_key_conflict", State.FAILED),
  (r"PUT /routes/h1/qlogs/0", 422, "qlog_mismatch", State.FAILED),
  (r"POST /routes/h1/submit", 403, "scope_missing", State.FAILED),
])
def test_terminal_site_errors_end_the_route(log_schema, clock, make_worker, pattern, status, code, state):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite(fail={pattern: [(status, {"error": code})] * 9})
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  r = route(w)
  assert r.state == state and r.note == pipeline.TERMINAL[code] and r.attempts == 0


@pytest.mark.parametrize("code", ["account_banned", "credit_choice_needed"])
def test_account_errors_pause_everything(log_schema, clock, make_worker, code):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  site.fail = {r"POST /routes/h1/submit": [(403, {"error": code})]}
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=5)
  with w.sessionmaker() as s:
    assert db.get(s, "pause:site") == pipeline.ACCOUNT[code]
  assert route(w).state == State.SUBMITTING


def test_copy_error_stops_waiting_out_the_cap(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  site.copy_error = "comma said no"
  w.tick()
  assert route(w).state == State.PUBLIC  # the site retries a passing comma error itself
  clock.advance(minutes=10)
  site.copy_error = ""
  w.tick()
  assert route(w).state == State.PUBLIC and "copy_error_since" not in (route(w).timings or {})
  site.copy_error = "comma said no"
  w.tick()
  clock.advance(minutes=31)
  w.tick()
  assert route(w).state == State.FAILED and not comma.public and site.withdrawn == ["sub1"]


def test_a_device_that_becomes_shared_is_dropped(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), owner=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  w.tick()
  with w.sessionmaker() as s:
    assert s.get(Device, DONGLE) is None and not s.scalars(select(Route)).all()
  assert not site.requests
  comma.requests.clear()
  clock.advance(minutes=10)
  w.tick()
  assert not [q for q in comma.requests if DONGLE in q.url.path]


def test_the_removed_owner_column_is_dropped_from_an_existing_database(tmp_path):
  make_sessionmaker(tmp_path)
  conn = sqlite3.connect(tmp_path / "relay.db")
  ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'devices'").fetchone()[0]
  conn.execute("DROP TABLE devices")
  conn.execute(ddl.replace("dongle_id VARCHAR(16) NOT NULL,", "dongle_id VARCHAR(16) NOT NULL, is_owner BOOLEAN NOT NULL,"))
  assert "is_owner" in [c[1] for c in conn.execute("PRAGMA table_info(devices)")]
  conn.commit()
  conn.close()
  with make_sessionmaker(tmp_path)() as s:
    s.add(Device(dongle_id=DONGLE))
    s.commit()
  conn = sqlite3.connect(tmp_path / "relay.db")
  assert "is_owner" not in [c[1] for c in conn.execute("PRAGMA table_info(devices)")]
  conn.close()


def test_expired_file_link_is_listed_again(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  real, expired = comma.handler, []

  def once(request):
    if request.url.host == "files.test" and "fcamera" in request.url.path and not expired:
      expired.append(1)
      return httpx.Response(403)
    return real(request)
  comma.handler = once
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock)
  assert route(w).state == State.SUBMITTED and route(w).attempts == 0 and expired


def test_lost_submit_then_zone_keeps_the_same_submit(log_schema, clock, make_worker, notes):
  comma, site = FakeComma(clock, drive(log_schema, home=(1,), n=3)), FakeSite()
  real, lost = site.handler, []

  def lose_answer(request):
    response = real(request)
    if request.url.path.endswith("/submit") and not lost:
      lost.append(1)
      add_home_zone(w)  # the zone arrives while the submit's answer is lost
      raise httpx.ReadTimeout("lost")
    return response
  site.handler = lose_answer
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=8)
  r = route(w)
  assert r.state == State.SUBMITTED and 1 in r.excluded
  assert site.submits[0] == site.submits[-1]  # same key and the same suggestions: the site returns the first
  assert any("before your new zone" in title for title, _ in notes)


def test_finished_without_submission_deletes_the_site_copy(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  clock.advance(days=3, hours=1)
  w.tick()
  assert route(w).state == State.EXPIRED and site.deleted == 1


def test_waiting_routes_poll_slowly(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  assert route(w).state == State.WAITING_FILES
  w.tick()
  listings = lambda: len([q for q in comma.requests if q.url.path.endswith("/files")])
  before = listings()
  clock.advance(minutes=1)
  w.tick()
  assert listings() == before
  clock.advance(minutes=5)
  w.tick()
  assert listings() == before + 1


def test_no_update_check_before_onboarding(config, monkeypatch):
  calls = []
  monkeypatch.setattr(pipeline.updates, "latest_tag", lambda *a, **k: calls.append(1))
  w = pipeline.Worker(config, make_sessionmaker(config.data_dir))
  w.tick()
  assert not calls


def test_tile_urls():
  assert tile_url_ok("https://{s}.tile.example.org/{z}/{x}/{y}.png") == "https://{s}.tile.example.org/{z}/{x}/{y}.png"
  assert tile_url_ok("https://bücher.example/{z}/{x}/{y}.png") == "https://xn--bcher-kva.example/{z}/{x}/{y}.png"
  for bad in ("http://tile.example/{z}/{x}/{y}", "https://user@evil.example/{z}/{x}/{y}", "https://a b/{z}/{x}/{y}",
              "https://evil.example; script-src *;/{z}/{x}/{y}", "https://tile.example/{z}/{x}"):
    assert tile_url_ok(bad) is None, bad


def client_for(tmp_path, clock, **env) -> tuple[TestClient, object, FakeSite]:
  comma, site = FakeComma(clock, {0: b""}), FakeSite()
  config = Config.from_env({"DATA_DIR": str(tmp_path), "POLARIS_HOST": "https://site.test",
                            "ALLOWED_HOSTS": "testserver", **env})
  sm = make_sessionmaker(config.data_dir)
  app = create_app(config, sm,
                   comma_factory=lambda t: CommaClient(t, "https://comma.test", transport=httpx.MockTransport(comma.handler)),
                   site_factory=lambda t: SiteClient("https://site.test", t, transport=httpx.MockTransport(site.handler)))
  return TestClient(app), sm, site


def csrf(page: str) -> str:
  return re.search(r'name="csrf" value="([^"]+)"', page)[1]


def test_bad_stored_tile_url_falls_back(tmp_path, clock):
  client, sm, _ = client_for(tmp_path, clock, INSECURE_ALLOW_NO_AUTH="1")
  with sm() as s:
    db.put(s, "tile_url", "https://evil.example; script-src *;/{z}/{x}/{y}")
    s.commit()
  page = client.get("/zones")
  assert page.status_code == 200 and "tile.openstreetmap.org" in page.headers["content-security-policy"]
  assert "evil" not in page.headers["content-security-policy"]


def test_enable_resets_the_start_and_resume_keeps_it(tmp_path, clock):
  client, sm, site = client_for(tmp_path, clock, INSECURE_ALLOW_NO_AUTH="1")
  token = csrf(client.get("/setup").text)
  post = lambda url, **data: html.unescape(client.post(url, data={"csrf": token, **data}, headers=ORIGIN).text)
  post("/setup/comma", token="e30.eyJleHAiOiA5OTk5OTk5OTk5fQ.sig")
  post("/setup/token", token="opu_abc")
  assert "from now on" in post(f"/devices/{DONGLE}/enable")
  with sm() as s:
    s.get(Device, DONGLE).enabled_at = s.get(Device, DONGLE).enabled_at - timedelta(days=2)
    first = s.get(Device, DONGLE).enabled_at
    s.get(Device, DONGLE).paused_reason = "verify again"
    s.commit()
  assert "resumed" in post(f"/devices/{DONGLE}/enable")
  with sm() as s:
    assert s.get(Device, DONGLE).enabled_at == first
  post(f"/devices/{DONGLE}/disable")
  post(f"/devices/{DONGLE}/enable")
  with sm() as s:
    assert s.get(Device, DONGLE).enabled_at > first


def test_site_link_only_to_the_site(tmp_path, clock):
  client, sm, _ = client_for(tmp_path, clock, INSECURE_ALLOW_NO_AUTH="1")
  with sm() as s:
    db.put(s, "onboarded_at", 1.0)
    s.add(Device(dongle_id=DONGLE, alias="Car"))
    s.add(Route(fullname=ROUTE, dongle_id=DONGLE, state=State.SUBMITTED, link="https://evil.example/r"))
    s.add(Route(fullname=ROUTE + "x", dongle_id=DONGLE, state=State.SUBMITTED, link="https://site.test/submit/route"))
    s.commit()
  page = client.get("/").text
  assert "evil.example" not in page and "https://site.test/submit/route" in page


def test_static_is_cacheable_pages_are_not(tmp_path, clock):
  client, _, _ = client_for(tmp_path, clock, INSECURE_ALLOW_NO_AUTH="1")
  assert client.get("/setup").headers["cache-control"] == "no-store"
  assert "no-store" not in client.get("/static/relay.css").headers.get("cache-control", "")


def test_password_change_ends_sessions(tmp_path, clock):
  first = bcrypt.hashpw(b"one", bcrypt.gensalt(4)).decode()
  client, _, _ = client_for(tmp_path, clock, AUTH_PASSWORD=f"admin:{first}")
  token = csrf(client.get("/login").text)
  client.post("/login", data={"csrf": token, "username": "admin", "password": "one"}, headers=ORIGIN)
  cookies = client.cookies
  assert client.get("/setup").status_code == 200
  second = bcrypt.hashpw(b"two", bcrypt.gensalt(4)).decode()
  client2, _, _ = client_for(tmp_path, clock, AUTH_PASSWORD=f"admin:{second}")
  client2.cookies = cookies
  assert client2.get("/setup", follow_redirects=False).headers["location"] == "/login"


def test_global_login_budget(tmp_path, clock):
  hashed = bcrypt.hashpw(b"right", bcrypt.gensalt(4)).decode()
  client, _, _ = client_for(tmp_path, clock, AUTH_PASSWORD=f"admin:{hashed}", TRUSTED_PROXIES="testclient")
  token = csrf(client.get("/login").text)
  for i in range(50):  # each from its own address, so no one address is locked
    client.post("/login", data={"csrf": token, "username": "admin", "password": "x"},
                headers={**ORIGIN, "x-forwarded-for": f"10.0.{i // 250}.{i % 250}"})
  page = client.post("/login", data={"csrf": token, "username": "admin", "password": "right"},
                     headers={**ORIGIN, "x-forwarded-for": "10.9.9.9"})
  assert "Too many attempts" in page.text


def test_secure_cookie_rule(tmp_path):
  base = {"DATA_DIR": str(tmp_path), "POLARIS_HOST": "https://site.test", "INSECURE_ALLOW_NO_AUTH": "1"}
  assert not Config.from_env({**base, "ALLOWED_HOSTS": "localhost:8080"}).secure_cookies
  assert Config.from_env({**base, "ALLOWED_HOSTS": "relay.example", "ALLOWED_ORIGINS":
                          "http://relay.example,https://relay.example"}).secure_cookies


def test_asking_the_device_again_isnt_a_regression(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), online=False, last_ping=AFTER_DRIVE), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  for _ in range(60):  # an offline device for 2.5 days: its request waits for it, nothing is asked again
    w.tick()
    clock.advance(hours=1)
  r = route(w)
  assert r.state in (State.WAITING_FILES, State.REQUESTING) and r.regressions == 0


def test_a_closed_submission_isnt_made_public_again(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  assert route(w).state == State.PUBLIC
  with w.sessionmaker() as s:  # made private while paused, and withdrawn on the site meanwhile
    r = s.get(Route, route(w).fullname)
    r.public_seconds, r.public_since = (r.public_seconds or 0) + 60, None
    s.commit()
  comma.public = False
  site.status = "withdrawn"
  changes = len(comma.public_changes)
  w.tick()
  assert route(w).state == State.SUBMITTED and True not in comma.public_changes[changes:]


def test_zones_changed_while_public_end_the_copy(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    pipeline.filter_changed(s)
    s.commit()
  w.tick()
  r = route(w)
  assert r.state == State.FAILED and r.cap_hit and not comma.public and site.withdrawn == ["sub1"]


def test_a_failed_withdraw_never_reopens_the_route(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  run_until_settled(w, clock, ticks=4)
  site.copy_error = "comma said no"
  site.fail = {r"POST /submissions/sub1/withdraw": [(503, {"error": "busy"})] * 3}
  w.tick()
  clock.advance(minutes=31)
  for _ in range(6):
    w.tick()
    clock.advance(minutes=20)
  assert comma.public_changes.count(True) == 1 and not comma.public


def queue_reads(comma: FakeComma) -> int:
  return sum(1 for r in comma.requests if r.url.host == "athena.test" and b"listUploadQueue" in r.content)


def until_waiting_on_device(w, comma, clock):
  for _ in range(12):
    if (r := route(w)) is not None and r.state == State.WAITING_FILES and comma.queue:
      return
    w.tick()
    clock.advance(minutes=31)
  raise AssertionError("never asked the device for files")


def test_files_that_leave_the_queue_are_asked_for_again_after_a_doubling_wait(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until_waiting_on_device(w, comma, clock)
  asked = len(comma.upload_paths)
  comma.queue.clear()  # a restart, or a cancel in comma connect
  clock.advance(minutes=5)
  w.tick()
  r = route(w)
  assert r.upload_wait_s == 600 and "asking again in 10 minutes" in r.note and len(comma.upload_paths) == asked
  clock.advance(minutes=6)
  w.tick()
  assert len(comma.upload_paths) == asked  # still waiting
  clock.advance(minutes=5)
  w.tick()
  assert len(comma.upload_paths) > asked and comma.queue  # asked again, queued again
  comma.queue.clear()  # gone again: the wait doubles
  clock.advance(minutes=5)
  w.tick()
  assert route(w).upload_wait_s == 1200
  comma.uploaded |= {(seg, kind) for seg, kinds in route(w).needs.items() for kind in kinds for seg in [int(seg)]}
  clock.advance(minutes=21)
  run_until_settled(w, clock, ticks=4, step_min=6)
  assert route(w).state == State.SUBMITTED


def test_the_device_coming_back_online_resets_the_wait(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until_waiting_on_device(w, comma, clock)
  for _ in range(2):  # gone twice: a 20-minute wait
    comma.queue.clear()
    clock.advance(minutes=5)
    w.tick()
    clock.advance(minutes=11 if route(w).upload_wait_s == 600 else 0)
    w.tick()
  assert route(w).upload_wait_s == 1200 and route(w).upload_retry_at is not None
  comma.last_ping = clock() - timedelta(hours=1)  # offline: its queue isn't read
  reads = queue_reads(comma)
  clock.advance(minutes=5)
  w.tick()
  assert queue_reads(comma) == reads and not route(w).device_online
  comma.last_ping = None  # back online: asked again now, and the next wait starts over
  asked = len(comma.upload_paths)
  clock.advance(minutes=5)
  w.tick()
  assert len(comma.upload_paths) > asked and route(w).upload_wait_s == 0


def test_a_newer_route_resets_the_wait(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=2), instant=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  until_waiting_on_device(w, comma, clock)
  comma.queue.clear()
  clock.advance(minutes=5)
  w.tick()
  with w.sessionmaker() as s:
    r, d = s.get(Route, ROUTE), s.get(Device, DONGLE)
    assert r.upload_wait_s == 600
    asked = len(comma.upload_paths)
    w.watch_uploads(w.comma_factory("t"), r, d, r.seen_newest + timedelta(minutes=30), [(0, "cameras")])
    assert r.upload_wait_s == 0 and r.state == State.REQUESTING and len(comma.upload_paths) == asked
