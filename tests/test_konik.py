import html

import httpx
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
  app = create_app(config, sm, comma_factory=comma_factory,
                   site_factory=lambda t: SiteClient("https://site.test", t, transport=httpx.MockTransport(site.handler)))
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
