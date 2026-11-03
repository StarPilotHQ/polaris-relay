import html
import re

import bcrypt
import httpx
import pytest
from fastapi.testclient import TestClient

from relay import cli, db
from relay.comma import CommaClient
from relay.config import Config, StartupError
from relay.db import Device, Zone, make_sessionmaker
from relay.site import RateLimited, SiteClient, TokenRejected
from relay.web.app import create_app
from tests.conftest import DONGLE, FakeComma, FakeSite

ORIGIN = {"origin": "http://testserver"}
JWT = "e30.eyJleHAiOiA5OTk5OTk5OTk5fQ.sig"


def env(tmp_path, **extra):
  return {"DATA_DIR": str(tmp_path), "POLARIS_HOST": "https://site.test", "ALLOWED_HOSTS": "testserver", **extra}


def test_refuses_to_start_without_auth(tmp_path):
  with pytest.raises(StartupError, match="AUTH_PASSWORD"):
    Config.from_env(env(tmp_path))


def test_refuses_without_site_or_hosts(tmp_path):
  with pytest.raises(StartupError, match="site address"):
    Config.from_env({**env(tmp_path, INSECURE_ALLOW_NO_AUTH="1"), "POLARIS_HOST": ""})
  with pytest.raises(StartupError, match="ALLOWED_HOSTS"):
    Config.from_env({**env(tmp_path, INSECURE_ALLOW_NO_AUTH="1"), "ALLOWED_HOSTS": ""})
  with pytest.raises(StartupError, match="bcrypt"):
    Config.from_env(env(tmp_path, AUTH_PASSWORD="admin:plaintext"))


def test_cli_exits_with_a_reason(tmp_path, monkeypatch, caplog):
  monkeypatch.setattr("os.environ", env(tmp_path))
  assert cli.main(["serve"]) == 2
  assert "not starting" in caplog.text


def test_wildcards_and_no_auth_warn(tmp_path):
  c = Config.from_env(env(tmp_path, INSECURE_ALLOW_NO_AUTH="1", ALLOWED_HOSTS="*", ALLOWED_ORIGINS="*"))
  text = " ".join(c.warnings)
  assert "INSECURE_ALLOW_NO_AUTH" in text and "ALLOWED_HOSTS is *" in text and "ALLOWED_ORIGINS is *" in text


@pytest.fixture
def fakes(clock):
  return FakeComma(clock, {0: b""}), FakeSite()


def make_client(tmp_path, fakes, **extra) -> tuple[TestClient, object]:
  comma, site = fakes
  config = Config.from_env(env(tmp_path, **extra))
  sm = make_sessionmaker(config.data_dir)
  app = create_app(config, sm,
                   comma_factory=lambda t: CommaClient(t, "https://comma.test", transport=httpx.MockTransport(comma.handler)),
                   site_factory=lambda t: SiteClient("https://site.test", t, transport=httpx.MockTransport(site.handler)))
  return TestClient(app), sm


def csrf(page: str) -> str:
  return re.search(r'name="csrf" value="([^"]+)"', page)[1]


def test_login_required_and_works(tmp_path, fakes):
  hashed = bcrypt.hashpw(b"hunter2", bcrypt.gensalt(4)).decode().replace("$2b$", "$2y$", 1)
  client, _ = make_client(tmp_path, fakes, AUTH_PASSWORD=f"admin:{hashed}")
  assert client.get("/setup", follow_redirects=False).headers["location"] == "/login"
  token = csrf(client.get("/login").text)
  bad = client.post("/login", data={"csrf": token, "username": "admin", "password": "nope"}, headers=ORIGIN)
  assert "don't match" in html.unescape(bad.text)
  client.post("/login", data={"csrf": token, "username": "admin", "password": "hunter2"}, headers=ORIGIN)
  assert client.get("/setup").status_code == 200


def test_login_locks_after_repeated_failures(tmp_path, fakes):
  hashed = bcrypt.hashpw(b"hunter2", bcrypt.gensalt(4)).decode()
  client, _ = make_client(tmp_path, fakes, AUTH_PASSWORD=f"admin:{hashed}")
  token = csrf(client.get("/login").text)
  for _ in range(5):
    client.post("/login", data={"csrf": token, "username": "admin", "password": "x"}, headers=ORIGIN)
  page = client.post("/login", data={"csrf": token, "username": "admin", "password": "hunter2"}, headers=ORIGIN)
  assert "Too many attempts" in page.text


def test_host_origin_and_csrf_guards(tmp_path, fakes):
  client, _ = make_client(tmp_path, fakes, INSECURE_ALLOW_NO_AUTH="1")
  assert client.get("/setup", headers={"host": "evil.test"}).status_code == 400
  token = csrf(client.get("/setup").text)
  assert client.post("/settings/general", data={"csrf": token}, headers={"origin": "https://evil.test"}).status_code == 403
  assert client.post("/settings/general", data={"csrf": token}).status_code == 403  # no Origin or Referer at all
  assert client.post("/settings/general", data={"csrf": "wrong"}, headers=ORIGIN).status_code == 403
  assert client.post("/settings/general", data={"csrf": token, "surround": "1"}, headers=ORIGIN,
                     follow_redirects=False).status_code == 303
  page = client.get("/setup")
  assert "Content-Security-Policy" in page.headers and "script-src 'self'" in page.headers["content-security-policy"]
  assert page.headers["referrer-policy"] == "same-origin"
  assert client.post("/settings/general", data={"csrf": token}, headers={"origin": "null"}).status_code == 403


def test_refusals_dont_say_why_but_the_log_does(tmp_path, fakes, caplog):
  client, _ = make_client(tmp_path, fakes, INSECURE_ALLOW_NO_AUTH="1")
  with caplog.at_level("WARNING", logger="relay"):
    for _ in range(3):
      refused = client.get("/setup", headers={"host": "192.168.1.5:8080"})
      assert refused.status_code == 400 and refused.text == "Bad Request"
    token = csrf(client.get("/setup").text)
    refused = client.post("/settings/general", data={"csrf": token}, headers={"origin": "https://evil.test"})
    assert refused.status_code == 403 and refused.text == "Forbidden"
  lines = [r.getMessage() for r in caplog.records]
  assert lines == ["refused a request: host not in ALLOWED_HOSTS ('192.168.1.5:8080')",
                   "refused a request: origin not in ALLOWED_ORIGINS ('https://evil.test')"]


def test_onboarding_flow(tmp_path, fakes):
  comma, site = fakes
  client, sm = make_client(tmp_path, fakes, INSECURE_ALLOW_NO_AUTH="1")
  token = csrf(client.get("/setup").text)
  post = lambda url, **data: html.unescape(client.post(url, data={"csrf": token, **data}, headers=ORIGIN).text)
  assert "doesn't look like a comma token" in post("/setup/comma", token="nope")
  assert "Found 1 device" in post("/setup/comma", token=JWT)
  assert "isn't an Operation Polaris access token" in post("/setup/token", token="opi_operator")
  assert "Access token saved" in post("/setup/token", token="opu_abc")
  site.verified = False
  assert "hasn't verified" in post(f"/devices/{DONGLE}/enable")
  site.verified = True
  assert "enabled" in post(f"/devices/{DONGLE}/enable")
  assert "isn't valid" in post("/zones", kind="circle", lat="45", lng="-100", radius_m="5")
  post("/zones", kind="circle", name="Home", lat="45", lng="-100", radius_m="300")
  post("/zones", kind="polygon", points="[[45,-100],[45.01,-100],[45.01,-99.99]]")
  post("/setup/finish")
  with sm() as s:
    assert db.get(s, "onboarded_at") and s.get(Device, DONGLE).enabled
    assert len(s.query(Zone).all()) == 2
    assert db.get(s, "access_token") == "opu_abc"
  assert client.get("/", follow_redirects=False).status_code == 200
  settings = client.get("/settings").text
  assert JWT not in settings and "opu_abc" not in settings  # pasted secrets are never shown again
  assert "Advanced" in settings


def test_advanced_settings_are_clamped(tmp_path, fakes):
  client, sm = make_client(tmp_path, fakes, INSECURE_ALLOW_NO_AUTH="1")
  token = csrf(client.get("/setup").text)
  client.post("/settings/advanced", data={"csrf": token, "public_cap_h": "1000", "quiet_min": "-5"}, headers=ORIGIN)
  with sm() as s:
    assert db.get(s, "public_cap_h") == 72 and db.get(s, "quiet_min") == 1 and db.get(s, "auto_withdraw") is False


def test_site_client_errors():
  def answer(status, body, headers=None):
    return SiteClient("https://site.test", "opu_x", transport=httpx.MockTransport(
      lambda r: httpx.Response(status, json=body, headers=headers or {})))
  with pytest.raises(RateLimited) as e:
    answer(429, {"error": "rate_limited", "limit": "calls"}, {"Retry-After": "90"}).me()
  assert e.value.retry_after == 90
  with pytest.raises(TokenRejected):
    answer(401, {"error": "invalid_token"}).me()
