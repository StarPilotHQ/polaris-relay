import hashlib
import io
import json
import re
import tarfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import capnp
import httpx
import pytest
import zstandard

from relay import db
from relay.config import Config
from relay.db import make_sessionmaker
from relay.pipeline import Worker

SCHEMA = Path(__file__).parent / "fixtures" / "schema"
DONGLE = "0123456789abcdef"
ROUTE_NAME = "00000042--0a1b2c3d4e"
ROUTE = f"{DONGLE}|{ROUTE_NAME}"
T0 = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
API_FILE_KINDS = ("rlog", "fcamera", "ecamera", "qcamera")
AFTER_DRIVE = T0 + timedelta(minutes=10)  # a device that pinged once after the drive, then went offline
HOME = (45.0, -100.0)
FAR = (45.2, -100.3)


@pytest.fixture(scope="session")
def log_schema():
  capnp.remove_import_hook()
  return capnp.load(str(SCHEMA / "log.capnp"), imports=[str(SCHEMA), str(SCHEMA / "include")])


def qlog(log_schema, points: list[tuple[float, float]], accuracy: float = 5.0, external: bool = True) -> bytes:
  """A synthetic qlog with one GPS fix per point."""
  out = []
  for i, (lat, lng) in enumerate(points):
    ev = log_schema.Event.new_message()
    ev.logMonoTime = int(i * 1e9)
    g = ev.init("gpsLocationExternal" if external else "gpsLocation")
    g.latitude, g.longitude, g.horizontalAccuracy, g.hasFix, g.flags = lat, lng, accuracy, True, 1
    g.unixTimestampMillis = int(T0.timestamp() * 1000) + i * 1000
    out.append(ev.to_bytes())
  return zstandard.ZstdCompressor().compress(b"".join(out))


def schema_bundle() -> bytes:
  buf = io.BytesIO()
  with tarfile.open(fileobj=buf, mode="w") as tar:
    for p in sorted(SCHEMA.rglob("*.capnp")):
      tar.add(p, arcname=str(p.relative_to(SCHEMA)))
  return zstandard.ZstdCompressor().compress(buf.getvalue())


@dataclass
class Clock:
  now: datetime = T0 + timedelta(minutes=10)

  def __call__(self) -> datetime:
    return self.now

  def advance(self, **kw) -> None:
    self.now += timedelta(**kw)


@pytest.fixture
def clock(monkeypatch):
  c = Clock()
  monkeypatch.setattr("relay.pipeline.now_utc", c)
  monkeypatch.setattr("relay.db.now_utc", c)
  monkeypatch.setattr("relay.notify.now_utc", c)
  return c


@dataclass
class FakeComma:
  """comma's API, athena and file storage for one device and one route."""
  clock: Clock
  qlogs: dict[int, bytes]
  start_point: tuple | None = FAR
  end_point: tuple | None = FAR
  online: bool = True
  public: bool = False
  uploaded: set = field(default_factory=set)  # (seg, kind) the device has uploaded
  requests: list = field(default_factory=list)
  upload_paths: list = field(default_factory=list)
  cellular: list = field(default_factory=list)
  public_changes: list = field(default_factory=list)
  devices_payload: list | None = None
  owner: bool = True
  instant: bool = True  # an online device uploads at once; otherwise requests wait in `queue`
  queue: list = field(default_factory=list)  # device upload queue: "<route>--<seg>/<file>"
  last_ping: datetime | None = None  # when it last pinged athena (default: now while online, a day ago offline)

  extra_routes: list = field(default_factory=list)  # more route objects in the device's route list
  gone: set = field(default_factory=set)  # (seg, kind) no longer on the device: athena reports them failed
  procqlog: bool = True  # Konik's route list leaves procqlog at -1

  def ping(self) -> datetime:
    return self.last_ping or (self.clock() if self.online else self.clock() - timedelta(days=1))

  @property
  def maxqlog(self) -> int:
    return max(self.qlogs)

  def route_obj(self) -> dict:
    return {"fullname": ROUTE, "start_time_utc_millis": int(T0.timestamp() * 1000),
            "end_time_utc_millis": int((T0 + timedelta(minutes=len(self.qlogs))).timestamp() * 1000),
            "maxqlog": self.maxqlog, "procqlog": self.maxqlog if self.procqlog else -1,
            "segment_numbers": sorted(self.qlogs),
            "start_lat": self.start_point[0] if self.start_point else 0,
            "start_lng": self.start_point[1] if self.start_point else 0,
            "end_lat": self.end_point[0] if self.end_point else 0, "end_lng": self.end_point[1] if self.end_point else 0,
            "is_public": self.public}

  def file_url(self, seg: int, name: str) -> str:
    return f"https://files.test/{DONGLE}/{ROUTE_NAME}/{seg}/{name}?sig=x"

  def files(self) -> dict:
    names = {"cameras": "fcamera.hevc", "ecameras": "ecamera.hevc", "logs": "rlog.zst", "qcameras": "qcamera.ts"}
    out = {k: [] for k in ("cameras", "dcameras", "ecameras", "logs", "qcameras", "qlogs")}
    out["qlogs"] = [self.file_url(s, "qlog.zst") for s in sorted(self.qlogs)]
    for seg, kind in sorted(self.uploaded):
      out[kind].append(self.file_url(seg, names[kind]))
    out["dcameras"] = [self.file_url(s, "dcamera.hevc") for s in sorted(self.qlogs)]
    return out

  def handler(self, request: httpx.Request) -> httpx.Response:
    self.requests.append(request)
    url, path = request.url, request.url.path
    if url.host == "files.test":
      m = re.search(r"/(\d+)/([a-z]+\.[a-z0-9.]+)$", path)
      seg, name = int(m[1]), m[2]
      if name.startswith("qlog"):
        return httpx.Response(200, content=self.qlogs[seg])
      if name.startswith("dcamera"):
        raise AssertionError("the driver camera was downloaded")
      return httpx.Response(200, content=f"{name}-{seg}".encode() * 100)
    if url.host == "athena.test":
      body = json.loads(request.content)
      if body["method"] == "listUploadQueue":
        if not self.online:
          return httpx.Response(404, json={"error": "Device not connected"})
        return httpx.Response(200, json={"id": 0, "jsonrpc": "2.0", "result": [
          {"path": f"/data/media/0/realdata/{fn}", "url": f"https://upload.test/{fn}", "id": str(i)}
          for i, fn in enumerate(self.queue)]})
      for item in body["params"]["files_data"]:
        self.upload_paths.append(item["fn"])
        self.cellular.append(item["allow_cellular"])
      if not self.online:
        return httpx.Response(200, json={"id": 0, "jsonrpc": "2.0", "result": "Device offline, message queued"})
      kinds = {"fcamera.hevc": "cameras", "ecamera.hevc": "ecameras", "rlog.zst": "logs", "qcamera.ts": "qcameras"}
      failed = []
      for item in body["params"]["files_data"]:
        seg_part, name = item["fn"].rsplit("/", 1)
        if name in kinds and (int(seg_part.rsplit("--", 1)[1]), kinds[name]) in self.gone:
          failed.append(item["fn"])
        elif name in kinds and not self.instant:
          if item["fn"] not in self.queue:
            self.queue.append(item["fn"])
        elif name in kinds:
          self.uploaded.add((int(seg_part.rsplit("--", 1)[1]), kinds[name]))
        else:
          failed.append(item["fn"])
      return httpx.Response(200, json={"id": 0, "jsonrpc": "2.0", "result": {"enqueued": 1, "items": [],
                                                                              "failed": failed}})
    if path == "/v1/me/devices/":
      return httpx.Response(200, json=self.devices_payload or [
        {"dongle_id": DONGLE, "alias": "Test car", "device_type": "four", "is_owner": self.owner,
         "last_athena_ping": int(self.ping().timestamp())}])
    if path == f"/v1/devices/{DONGLE}/routes_segments":
      return httpx.Response(200, json=[self.route_obj(), *self.extra_routes])
    if path.startswith("/v1/route/") and path.endswith("/files"):
      return httpx.Response(200, json=self.files())
    if path.startswith("/v1/route/") and request.method == "PATCH":
      self.public = json.loads(request.content)["is_public"]
      self.public_changes.append(self.public)
      return httpx.Response(200, json=self.route_obj())
    if path.startswith("/v1/route/"):
      return httpx.Response(200, json=self.route_obj())
    if path == f"/v1/{DONGLE}/upload_urls/":
      paths = json.loads(request.content)["paths"]
      return httpx.Response(200, json=[{"url": f"https://upload.test/{p}", "headers": {}} for p in paths])
    return httpx.Response(404)


@dataclass
class FakeSite:
  """The site's /api/v1, per its OpenAPI document (sp-intake's intake/web/openapi.yml)."""
  requests: list = field(default_factory=list)
  qlogs: dict = field(default_factory=dict)
  files: dict = field(default_factory=dict)
  submits: list = field(default_factory=list)
  status: str = "submitted"
  verified: bool = True
  submittable: bool = True
  rogue_suggestion: int | None = None  # also suggest a range on this segment (as a buggy site might)
  fail: dict = field(default_factory=dict)  # path regex -> list of (status, body) to answer first
  drives: int = 0
  withdrawn: list = field(default_factory=list)
  deleted: int = 0
  suggest_calls: list = field(default_factory=list)
  copy_error: str = ""
  headers: dict = field(default_factory=dict)  # added to every answer (the deprecation headers)
  hold_copy: bool = False  # a relay submission stays "submitted" (the site still copying) instead of moving on
  transfer: str = ""
  konik: bool = False  # the device is on Konik Stable: routes come as konik:<dongle>|<route>
  opened: list = field(default_factory=list)

  def handler(self, request: httpx.Request) -> httpx.Response:
    response = self.answer(request)
    response.headers.update(self.headers)
    return response

  def answer(self, request: httpx.Request) -> httpx.Response:
    self.requests.append(request)
    assert request.headers["user-agent"].startswith("polaris-relay/")
    path = request.url.path.removeprefix("/api/v1")
    for pattern, answers in self.fail.items():
      if re.fullmatch(pattern, f"{request.method} {path}") and answers:
        status, body = answers.pop(0)
        return httpx.Response(status, json=body, headers={"Retry-After": "120"} if status == 429 else {})
    if path == "/me":
      return httpx.Response(200, json={"handle": "tester", "token": {"name": "relay", "scopes": ["submit",
                                                                                                 "record_drive"]}})
    if path == "/devices/check":
      provider = json.loads(request.content).get("provider", "comma")
      if provider not in ("comma", "konik"):
        return httpx.Response(400, json={"error": "bad_provider", "message": "unknown provider"})
      return httpx.Response(200, json={"verified": self.verified and (provider == "konik") == self.konik})
    if path == "/schema":
      return httpx.Response(200, content=schema_bundle(), headers={"X-Schema-Version": "test1"})
    if path == "/routes":
      self.opened.append(json.loads(request.content)["route"])
      if self.opened[-1] != ("konik:" if self.konik else "") + ROUTE:
        return httpx.Response(400, json={"error": "bad_route", "message": "no such route"})
      if not self.verified:
        return httpx.Response(403, json={"error": "device_not_verified", "message": "not verified"})
      return httpx.Response(200, json={"id": "h1", "submittable": self.submittable, "link": "https://site.test/r"})
    if path == "/routes/h1" and request.method == "DELETE":
      if self.submits:
        return httpx.Response(409, json={"error": "already_submitted"})
      self.deleted += 1
      return httpx.Response(200, json={"deleted": True})
    if m := re.fullmatch(r"/routes/h1/qlogs/(\d+)", path):
      data = request.read()
      assert hashlib.sha256(data).hexdigest() == request.headers["x-sha256"]
      self.qlogs[int(m[1])] = data
      return httpx.Response(200, json={"stored": True})
    if path == "/routes/h1/suggest":
      body = json.loads(request.content)
      self.suggest_calls.append(body)
      if self.submits and self.suggest_calls[0]["excluded"] != body["excluded"]:
        return httpx.Response(409, json={"error": "already_submitted"})
      for seg in body["excluded"]:
        self.qlogs.pop(seg, None)
        for key in [k for k in self.files if k[0] == seg]:
          self.files.pop(key)
      segs = list(body["segments"])
      if self.rogue_suggestion is not None:
        segs.append(self.rogue_suggestion)
      sugg = [{"id": f"s-{s}", "detector": "lane_change", "scenario": "lane_change", "label": "x",
               "start_s": s * 60 + 10, "end_s": s * 60 + 40, "driver_action": "engaged_good", "fields": {},
               "tags": [], "needs_input": {"outcome": "guessed"} if s % 2 else {}, "segments": [s],
               "preview_segments": [s]} for s in segs]
      return httpx.Response(200, json={"detector_version": "abc", "submittable": self.submittable,
                                       "ranges_left_today": 90, "link": "https://site.test/r", "suggestions": sugg,
                                       "needs": {}})
    if m := re.fullmatch(r"/routes/h1/files/(\d+)/(\w+)", path):
      if m[2] not in API_FILE_KINDS:
        return httpx.Response(400, json={"error": "kind_not_allowed", "message": "Only rlog, fcamera, ecamera and qcamera"})
      data = request.read()
      assert hashlib.sha256(data).hexdigest() == request.headers["x-sha256"]
      self.files[(int(m[1]), m[2])] = self.files.get((int(m[1]), m[2]), 0) + 1
      return httpx.Response(200, json={"stored": True})
    if path == "/routes/h1/submit":
      body = json.loads(request.content)
      if body["transfer"] == "relay":
        segments = [int(s.removeprefix("s-")) for s in body["suggestions"]]
        missing = [{"segment": seg, "kind": kind} for seg in segments for kind in API_FILE_KINDS
                   if (seg, kind) not in self.files]
        if missing:
          return httpx.Response(409, json={"error": "files_missing", "message": "not there yet", "missing": missing})
      self.submits.append((request.headers.get("idempotency-key"), body))
      self.transfer = body["transfer"]
      return httpx.Response(201, json={"submission_id": "sub1", "status": "submitted",
                                       "ranges": len(json.loads(request.content)["suggestions"])})
    if path == "/routes/h1/drive":
      self.drives += 1
      return httpx.Response(200, json={"recorded": 1, "already_recorded": 0, "segments_left_out": 0, "models": []})
    if path == "/submissions/sub1":
      status = self.status
      if status == "submitted" and self.transfer == "relay" and not self.hold_copy and not self.copy_error:
        status = "ready_for_review"
      return httpx.Response(200, json={"submission_id": "sub1", "status": status, "label": status,
                                       "copy_error": self.copy_error})
    if path == "/submissions/sub1/withdraw":
      self.withdrawn.append("sub1")
      return httpx.Response(200, json={"status": "withdrawn"})
    return httpx.Response(404, json={"error": "not_found"})


@pytest.fixture
def data_dir(tmp_path):
  return tmp_path / "data"


@pytest.fixture
def config(data_dir):
  return Config.from_env({"DATA_DIR": str(data_dir), "POLARIS_HOST": "https://site.test",
                          "INSECURE_ALLOW_NO_AUTH": "1", "ALLOWED_HOSTS": "testserver",
                          "RELAY_COMMA_API": "https://comma.test", "RELAY_ATHENA_API": "https://athena.test",
                          "RELAY_KONIK_API": "https://comma.test", "RELAY_KONIK_ATHENA": "https://athena.test/ws"})


@pytest.fixture
def make_worker(config):
  from relay.comma import CommaClient
  from relay.site import SiteClient

  def make(comma: FakeComma, site: FakeSite) -> Worker:
    sm = make_sessionmaker(config.data_dir)
    return Worker(config, sm,
                  lambda t, cloud="comma": CommaClient(t, *config.endpoints(cloud), cloud=cloud,
                                                       transport=httpx.MockTransport(comma.handler)),
                  lambda t: SiteClient(config.polaris_host, t, transport=httpx.MockTransport(site.handler)))
  return make


def onboard(worker: Worker, *, zones=(), surround: int = 0, mode: str = "auto", transfer: str = "relay",
            record_drives: bool = True, scopes=("submit", "record_drive"), submit_delay_min: float = 0,
            cloud: str = "comma") -> None:
  from relay.db import Device
  with worker.sessionmaker() as s:
    db.put(s, "comma_token", "header.eyJleHAiOiA5OTk5OTk5OTk5fQ.sig")
    db.put(s, "access_token", "opu_test")
    db.put(s, "submit_delay_min", submit_delay_min)
    db.put(s, "token_info", {"handle": "tester", "name": "relay", "scopes": list(scopes), "checked": 4e9})
    db.put(s, "onboarded_at", (T0 - timedelta(days=1)).timestamp())
    db.put(s, "surround", surround)
    db.put(s, "transfer", transfer)
    db.put(s, "cloud", cloud)
    db.put(s, "update_check", False)
    s.add(Device(dongle_id=DONGLE, alias="Test car", enabled=True, mode=mode, record_drives=record_drives,
                 enabled_at=T0 - timedelta(days=1), cloud=cloud))
    for z in zones:
      s.add(z)
    s.commit()


def run_until_settled(worker: Worker, clock: Clock, ticks: int = 12, step_min: int = 31) -> None:
  for _ in range(ticks):
    worker.tick()
    clock.advance(minutes=step_min)
