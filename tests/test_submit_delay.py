import html
import re
from datetime import timedelta

from relay import db
from relay.db import Route
from relay.pipeline import State
from tests.conftest import ROUTE, FakeComma, FakeSite, onboard
from tests.test_verification import drive, route, until, web

ORIGIN = {"origin": "http://testserver"}


def skip(w) -> str:
  client = web(w)
  page = client.get("/").text
  form = re.search(r'action="(/drives/[0-9a-f]+/skip)".*?name="csrf" value="([^"]+)"', page, re.S)
  return client.post(form[1], data={"csrf": form[2]}, headers=ORIGIN).text


def test_a_ready_drive_waits_out_the_delay_then_submits(log_schema, clock, make_worker, monkeypatch):
  monkeypatch.setattr("relay.web.app.now_utc", clock)
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=60)
  until(w, clock, State.READY, ticks=20)
  assert not site.submits and site.files
  page = web(w).get("/").text
  assert "Ready to submit" in page and "Submits automatically in 50 minutes" in page
  clock.advance(minutes=30)
  w.tick()
  assert route(w).state == State.READY and not site.submits
  clock.advance(minutes=21)
  w.tick()
  assert site.submits and route(w).state in (State.SITE_COPYING, State.SUBMITTED)


def test_no_delay_submits_at_once_and_a_shorter_delay_applies_to_waiting_drives(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=120)
  until(w, clock, State.READY, ticks=20)
  with w.sessionmaker() as s:
    db.put(s, "submit_delay_min", 0)
    s.commit()
  w.tick()
  assert site.submits


def test_coming_back_after_missing_files_doesnt_wait_again(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30)
  until(w, clock, State.READY, ticks=20)
  first = route(w).ready_at
  site.fail = {r"POST /routes/h1/submit": [(409, {"error": "files_missing", "missing": []})]}
  clock.advance(minutes=61)
  for _ in range(4):
    w.tick()
    clock.advance(minutes=6)
  assert route(w).ready_at == first and site.submits


def test_suggest_only_devices_never_reach_ready(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30, mode="suggest")
  until(w, clock, State.SUGGESTED, step_min=5)
  assert not site.submits


def test_skip_while_ready_submits_nothing_and_cleans_up(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30)
  until(w, clock, State.READY, ticks=20)
  assert "Skipped. Nothing will be submitted" in skip(w)
  w.tick()
  r = route(w)
  assert r.state == State.SKIPPED and "You skipped this drive" in r.note
  assert not site.submits and site.deleted == 1
  clock.advance(hours=2)
  w.tick()
  assert not site.submits
  assert "/skip" not in web(w).get("/").text


def test_skip_stops_a_transfer_part_way(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema, n=3)), FakeSite()
  real, sent = site.handler, []
  w = make_worker(comma, site)

  def skip_after_first_file(request):
    response = real(request)
    if "/files/" in request.url.path and not sent:
      sent.append(1)
      with w.sessionmaker() as s:
        s.get(Route, ROUTE).skip_requested = True
        s.commit()
    return response
  site.handler = skip_after_first_file
  onboard(w, submit_delay_min=30)
  for _ in range(20):
    w.tick()
    clock.advance(minutes=10)
  assert route(w).state == State.SKIPPED and len(site.files) == 1 and not site.submits


def test_skip_is_refused_once_a_submit_could_have_gone_out(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, submit_delay_min=30)
  until(w, clock, State.READY, ticks=20)
  client = web(w)
  page = client.get("/").text
  form = re.search(r'action="(/drives/[0-9a-f]+/skip)".*?name="csrf" value="([^"]+)"', page, re.S)
  with w.sessionmaker() as s:
    s.get(Route, ROUTE).idempotency_key = "sent"
    s.commit()
  assert "can't be skipped now" in html.unescape(client.post(form[1], data={"csrf": form[2]}, headers=ORIGIN).text)
  assert not route(w).skip_requested


def test_the_delay_is_an_advanced_setting(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  client = web(w)
  page = client.get("/settings").text
  assert 'name="submit_delay_min"' in page and "Skip it and submit it yourself" in page
  token = re.search(r'action="/settings/advanced".*?name="csrf" value="([^"]+)"', page, re.S)[1]
  client.post("/settings/advanced", data={"csrf": token, "submit_delay_min": "5000"}, headers=ORIGIN)
  with w.sessionmaker() as s:
    assert db.get(s, "submit_delay_min") == 1440
