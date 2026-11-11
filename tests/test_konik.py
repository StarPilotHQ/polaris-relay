import html

import httpx
import pytest
from fastapi.testclient import TestClient

from relay import db
from relay.comma import CommaClient
from relay.config import Config
from relay.db import Device, Route, make_sessionmaker
from relay.pipeline import State
from relay.site import SiteClient
from relay.web.app import create_app
from tests.conftest import DONGLE, FAR, ROUTE, FakeComma, FakeSite, onboard, qlog, run_until_settled

ORIGIN = {"origin": "http://testserver"}
TOKEN = "header.eyJleHAiOiA5OTk5OTk5OTk5fQ.sig"


def plain_drive(log_schema, n=4):
  return {seg: qlog(log_schema, [(FAR[0] + seg * 0.001, FAR[1])] * 3) for seg in range(n)}


def test_konik_endpoints_default():
  c = Config()
  assert c.endpoints("konik") == ("https://api.konik.ai", "https://api.konik.ai/ws")
  assert c.endpoints("comma") == ("https://api.commadotai.com", "https://athena.comma.ai")


def test_konik_drive_relays_without_procqlog(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik", transfer="public")  # public can't work on Konik: the site copies only from comma
  run_until_settled(w, clock)
  with w.sessionmaker() as s:
    r = s.get(Route, ROUTE)
  assert r.state == State.SUBMITTED and r.transfer == "relay" and "quiet" in r.timings
  assert site.opened and set(site.opened) == {f"konik:{ROUTE}"}
  assert site.transfer == "relay" and not comma.public_changes
  athena = [q for q in comma.requests if q.url.host == "athena.test"]
  assert athena and all(q.url.path == f"/ws/{DONGLE}" for q in athena)


def test_comma_route_still_waits_for_procqlog(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite()
  w = make_worker(comma, site)
  onboard(w)
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    assert s.get(Route, ROUTE).state == State.WAITING_QLOGS
  assert not site.opened


def test_konik_account_not_allowed_pauses(log_schema, clock, make_worker):
  comma = FakeComma(clock, plain_drive(log_schema), procqlog=False)
  site = FakeSite(konik=True, fail={r"POST /routes": [(403, {"error": "konik_api_not_allowed", "message": "no"})]})
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  run_until_settled(w, clock, ticks=4)
  with w.sessionmaker() as s:
    assert "operator" in db.get(s, "pause:site")


def web(tmp_path, comma, site, used):
  config = Config.from_env({"DATA_DIR": str(tmp_path / "data"), "POLARIS_HOST": "https://site.test",
                            "INSECURE_ALLOW_NO_AUTH": "1", "ALLOWED_HOSTS": "testserver"})
  sm = make_sessionmaker(config.data_dir)

  def comma_factory(t, cloud="comma"):
    used.append(cloud)
    return CommaClient(t, "https://comma.test", cloud=cloud, transport=httpx.MockTransport(comma.handler))
  app = create_app(config, sm, comma_factory=comma_factory, site_factory=lambda t: SiteClient(
    "https://site.test", t, transport=httpx.MockTransport(site.handler)))
  return TestClient(app), sm


def csrf(client) -> str:
  import re
  return re.search(r'name="csrf" value="([^"]+)"', client.get("/setup").text)[1]


def test_setup_saves_konik_and_verifies_with_provider(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(konik=True), []
  client, sm = web(tmp_path, comma, site, used)
  page = client.get("/setup").text
  assert "Konik Stable" in page
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN}, headers=ORIGIN)
  client.post("/setup/token", data={"csrf": token, "token": "opu_x"}, headers=ORIGIN)
  with sm() as s:
    assert db.get(s, "cloud") == "konik" and db.get(s, "transfer") == "relay"
  assert used == ["konik"]
  page = html.unescape(client.post(f"/devices/{DONGLE}/enable", data={"csrf": token}, headers=ORIGIN).text)
  with sm() as s:
    assert s.get(Device, DONGLE).enabled, page
  client.post("/settings/general", data={"csrf": token, "transfer": "public"}, headers=ORIGIN)
  with sm() as s:
    assert db.get(s, "transfer") == "relay"
  assert "Make the route public briefly" not in client.get("/setup").text


def test_switching_service_disables_devices(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    s.get(Device, DONGLE).enabled = True
    s.commit()
  client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    assert db.get(s, "cloud") == "konik" and not s.get(Device, DONGLE).enabled
  assert used == ["comma", "konik"]


@pytest.mark.parametrize("allowed", [False, True])
def test_cellular_uploads_follow_the_setting(allowed, log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  with w.sessionmaker() as s:
    db.put(s, "upload_cellular", allowed)
    s.commit()
  run_until_settled(w, clock)
  assert comma.cellular and set(comma.cellular) == {allowed}


def test_switching_is_refused_while_a_drive_is_public(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    s.get(Device, DONGLE).enabled = True
    s.add(Route(fullname=ROUTE, dongle_id=DONGLE, state=State.PUBLIC, public_since=clock()))
    s.commit()
  page = html.unescape(client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN},
                                   headers=ORIGIN).text)
  assert "still public on comma connect" in page
  with sm() as s:
    assert db.get(s, "cloud") == "comma" and s.get(Device, DONGLE).enabled


def test_a_route_made_public_while_devices_are_listed_stops_the_switch(tmp_path, clock):
  """The worker can make a route public while the switch waits on the service: the check comes after that call."""
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  real = comma.handler

  def handler(request):
    if request.url.path == "/v1/me/devices/":
      with sm() as s:
        s.add(Route(fullname=ROUTE, dongle_id=DONGLE, state=State.SUBMITTING, public_since=clock()))
        s.commit()
    return real(request)
  comma.handler = handler
  page = html.unescape(client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN},
                                   headers=ORIGIN).text)
  assert "still public" in page
  with sm() as s:
    assert db.get(s, "cloud") == "comma"


def test_unknown_service_is_refused(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "other", "token": TOKEN}, headers=ORIGIN)
  assert not used
  with sm() as s:
    assert db.get(s, "comma_token") is None


def test_only_the_current_services_devices_are_shown_or_changed(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(konik=True), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  comma.devices_payload = [{"dongle_id": "c" * 16, "alias": "Konik car", "is_owner": True}]
  client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN}, headers=ORIGIN)
  page = client.get("/setup").text
  assert "Konik car" in page and "Test car" not in page
  for action in ("enable", "disable", "options"):
    assert client.post(f"/devices/{DONGLE}/{action}", data={"csrf": token}, headers=ORIGIN).status_code == 404
  with sm() as s:
    assert s.get(Device, DONGLE).cloud == "comma" and s.get(Device, "c" * 16).cloud == "konik"


def test_a_device_listed_by_both_services_moves_and_is_disabled(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    s.get(Device, DONGLE).enabled = True
    s.commit()
  client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    d = s.get(Device, DONGLE)
    assert d.cloud == "konik" and not d.enabled


def foreign_route(clock, state=State.TRANSFERRING, **kw) -> Route:
  return Route(fullname=f"{DONGLE}|0000002a--0000000001", dongle_id=DONGLE, cloud="comma", state=state,
               start_time=clock(), **kw)


def test_a_route_from_the_old_service_is_ended_not_moved_on(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  with w.sessionmaker() as s:
    s.add(foreign_route(clock))
    s.commit()
  w.tick()
  with w.sessionmaker() as s:
    r = s.get(Route, f"{DONGLE}|0000002a--0000000001")
    assert r.state == State.SKIPPED and "switched services" in r.note
  assert not [q for q in comma.requests if "0000002a" in str(q.url)]


def test_a_route_from_the_old_service_waits_while_its_device_is_off(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  with w.sessionmaker() as s:
    s.get(Device, DONGLE).cloud = "comma"  # only comma lists it: switching back finds the drive where it was
    s.add(foreign_route(clock))
    s.commit()
  w.tick()
  with w.sessionmaker() as s:
    assert s.get(Route, f"{DONGLE}|0000002a--0000000001").state == State.TRANSFERRING


def test_the_safety_net_never_takes_another_services_not_found_as_private(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  with w.sessionmaker() as s:
    s.add(foreign_route(clock, State.PUBLIC, public_since=clock()))
    s.commit()
  w.run_safety_net()
  with w.sessionmaker() as s:
    assert s.get(Route, f"{DONGLE}|0000002a--0000000001").public_since is not None
  assert not comma.public_changes


def test_an_existing_database_gets_the_service_columns(tmp_path):
  import sqlite3
  make_sessionmaker(tmp_path)
  conn = sqlite3.connect(tmp_path / "relay.db")
  for table in ("devices", "routes"):
    conn.execute(f"ALTER TABLE {table} DROP COLUMN cloud")
  conn.execute("INSERT INTO devices (dongle_id, alias, enabled, mode, record_drives, last_athena_ping, "
               "submitted_before) VALUES ('d', '', 0, 'auto', 1, 0, 0)")
  conn.commit()
  conn.close()
  with make_sessionmaker(tmp_path)() as s:
    assert s.get(Device, "d").cloud == "comma"


def test_a_refusal_of_the_token_replaced_meanwhile_pauses_nothing(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w)
  with w.sessionmaker() as s:
    db.put(s, "comma_token", "header.eyJleHAiOiA5OTk5OTk5OTk4fQ.new")
    db.put(s, "cloud", "konik")
    s.commit()
    w.comma_refused(s, TOKEN)
    assert db.get(s, "pause:comma") is None
    w.comma_refused(s, "header.eyJleHAiOiA5OTk5OTk5OTk4fQ.new")
    assert db.get(s, "pause:comma").startswith("Konik refused")


def test_a_pass_still_on_the_old_service_never_makes_a_route_public(log_schema, clock, make_worker):
  """A switch commits while a long pass, holding the old client, reaches the make-public step."""
  comma, site = FakeComma(clock, plain_drive(log_schema)), FakeSite()
  w = make_worker(comma, site)
  onboard(w, transfer="public")
  with w.sessionmaker() as s:
    r = foreign_route(clock, State.SUBMITTING, transfer="public")
    s.add(r)
    s.commit()
    db.put(s, "cloud", "konik")
    s.get(Device, DONGLE).cloud = "konik"  # Konik lists the same dongle and the user enabled it there
    s.commit()
    client = w.comma_factory(TOKEN, "comma")
    with pytest.raises(Exception, match="switched services"):
      w.make_public(s, client, r)
    client.close()
    assert s.get(Route, r.fullname).public_since is None
  assert not comma.public_changes


def test_switching_to_a_public_routes_service_is_allowed(tmp_path, clock):
  comma, site, used = FakeComma(clock, {0: b""}), FakeSite(), []
  client, sm = web(tmp_path, comma, site, used)
  token = csrf(client)
  client.post("/setup/comma", data={"csrf": token, "cloud": "konik", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    s.add(foreign_route(clock, State.FAILED, public_since=clock()))
    s.commit()
  client.post("/setup/comma", data={"csrf": token, "cloud": "comma", "token": TOKEN}, headers=ORIGIN)
  with sm() as s:
    assert db.get(s, "cloud") == "comma"


def test_a_copy_on_the_old_service_is_ended_and_withdrawn(log_schema, clock, make_worker):
  comma, site = FakeComma(clock, plain_drive(log_schema), procqlog=False), FakeSite(konik=True)
  w = make_worker(comma, site)
  onboard(w, cloud="konik")
  with w.sessionmaker() as s:
    s.add(foreign_route(clock, State.PUBLIC, submission_id="sub1"))
    s.commit()
  w.tick()
  with w.sessionmaker() as s:
    r = s.get(Route, f"{DONGLE}|0000002a--0000000001")
    assert r.state == State.FAILED and "switched services" in r.note and "withdrawn" in r.note
  assert site.withdrawn == ["sub1"] and not comma.public_changes
