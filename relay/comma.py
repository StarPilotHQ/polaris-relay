"""comma API client, authenticated with the user's own token (api.comma.ai; AGENTS.md has what its docs leave out)."""
import base64
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import httpx

# httpx logs every request URL at INFO; comma URLs carry the dongle ID and route name.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

KIND_FILES = {"cameras": ("fcamera.hevc",), "ecameras": ("ecamera.hevc",), "logs": ("rlog.zst", "rlog.bz2"),
              "qcameras": ("qcamera.ts",), "qlogs": ("qlog.zst", "qlog.bz2")}
NEVER_KINDS = ("dcameras",)
_SEG_IN_URL = re.compile(r"/(\d+)/([a-z]+\.[a-z0-9.]+)(?:\?|$)")
OFFLINE_QUEUED = "Device offline, message queued"
FILE_DEADLINE_S = 30 * 60  # one file download, wall clock: a stalled link mustn't hold the worker for hours
ROUTE_PAGES = 20
log = logging.getLogger("relay")


class CommaError(Exception):
  """`transient`: comma or the network didn't answer (unreachable, 5xx); waiting is the fix, not giving up."""

  def __init__(self, message: str = "", transient: bool = False):
    super().__init__(message)
    self.transient = transient


class CommaAuthError(CommaError):
  pass


class CommaNotFound(CommaError):
  pass


class LinkExpired(CommaError):
  pass


class TooSlow(CommaError):
  deadline = True


@dataclass
class RouteMeta:
  fullname: str
  start_time: datetime | None
  end_time: datetime | None
  maxqlog: int
  procqlog: int
  segment_numbers: list[int]
  start_point: list[float] | None
  end_point: list[float] | None


def token_expiry(token: str) -> datetime | None:
  """The `exp` claim of a comma JWT, read without verifying it (only shown to the user)."""
  try:
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return datetime.fromtimestamp(int(claims["exp"]), UTC)
  except (IndexError, KeyError, ValueError, TypeError):
    return None


def _time(value) -> datetime | None:
  if value in (None, "", 0):
    return None
  if isinstance(value, (int, float)):
    return datetime.fromtimestamp(value / 1000 if value > 1e11 else value, UTC)
  try:
    t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
  except ValueError:
    return None
  return t if t.tzinfo else t.replace(tzinfo=UTC)


def _point(lat, lng) -> list[float] | None:
  return [float(lat), float(lng)] if lat and lng else None


def route_meta(r: dict) -> RouteMeta:
  return RouteMeta(
    fullname=r["fullname"],
    start_time=_time(r.get("start_time_utc_millis") or r.get("start_time")),
    end_time=_time(r.get("end_time_utc_millis") or r.get("end_time")),
    maxqlog=int(r.get("maxqlog") if r.get("maxqlog") is not None else -1),
    procqlog=int(r.get("procqlog") if r.get("procqlog") is not None else -1),
    segment_numbers=sorted(int(s) for s in r.get("segment_numbers") or []),
    start_point=_point(r.get("start_lat"), r.get("start_lng")), end_point=_point(r.get("end_lat"), r.get("end_lng")))


def files_by_segment(files: dict) -> dict[str, dict[int, tuple[str, str]]]:
  """{kind: {segment: (file name, url)}} from a `/files` response; never dcameras."""
  out: dict[str, dict[int, tuple[str, str]]] = {}
  for kind, urls in files.items():
    if kind in NEVER_KINDS:
      continue
    for url in urls or []:
      if m := _SEG_IN_URL.search(url):
        out.setdefault(kind, {})[int(m[1])] = (m[2], url)
  return out


class CommaClient:
  def __init__(self, token: str, api: str = "https://api.commadotai.com", athena: str = "https://athena.comma.ai",
               transport: httpx.BaseTransport | None = None, timeout: float = 30.0):
    headers = {"Authorization": f"JWT {token}"}
    self.http = httpx.Client(base_url=api, headers=headers, transport=transport, timeout=timeout)
    self.athena = httpx.Client(base_url=athena, headers=headers, transport=transport, timeout=timeout)
    # Signed file URLs carry their own authorization; the JWT must not go to the storage host.
    self.files = httpx.Client(transport=transport, timeout=timeout, follow_redirects=True)

  def close(self) -> None:
    for c in (self.http, self.athena, self.files):
      c.close()

  def _call(self, client: httpx.Client, method: str, path: str, **kw):
    try:
      r = client.request(method, path, **kw)
    except httpx.HTTPError as e:
      raise CommaError(f"comma unreachable ({type(e).__name__})", transient=True) from None
    if r.status_code == 401 or (r.status_code == 403 and path == "/v1/me/devices/"):
      raise CommaAuthError("comma refused the token")
    if r.status_code == 403:
      raise CommaError("comma refused this for the route or device (not the owner?)")
    if r.status_code == 404:
      raise CommaNotFound("comma has no such route or device")
    if r.status_code >= 400:
      raise CommaError(f"comma answered {r.status_code}", transient=r.status_code >= 500)
    try:
      return r.json()
    except ValueError:
      raise CommaError("comma sent an unreadable answer") from None

  def devices(self) -> list[dict]:
    return self._call(self.http, "GET", "/v1/me/devices/")

  def routes(self, dongle_id: str, start: datetime, end: datetime, limit: int = 100) -> list[RouteMeta]:
    """Routes that started between `start` and `end`, paging back from `end` while a page comes back full."""
    out: dict[str, RouteMeta] = {}
    end_ms, start_ms = int(end.timestamp() * 1000), int(start.timestamp() * 1000)
    for _ in range(ROUTE_PAGES):
      data = self._call(self.http, "GET", f"/v1/devices/{dongle_id}/routes_segments",
                        params={"start": start_ms, "end": end_ms, "limit": limit})
      if not isinstance(data, list):
        raise CommaError("comma sent an unreadable route list")
      page = [route_meta(r) for r in data if isinstance(r, dict) and r.get("fullname")]
      out.update((m.fullname, m) for m in page)
      oldest = min((int(m.start_time.timestamp() * 1000) for m in page if m.start_time), default=None)
      if len(data) < limit or oldest is None or oldest - 1 >= end_ms:
        return list(out.values())
      end_ms = oldest - 1
    log.warning("comma's route list still had more after %d pages; older routes in the window were skipped", ROUTE_PAGES)
    return list(out.values())

  def route(self, fullname: str) -> RouteMeta:
    return route_meta(self._call(self.http, "GET", f"/v1/route/{quote(fullname, safe='')}/"))

  def route_files(self, fullname: str) -> dict[str, dict[int, tuple[str, str]]]:
    return files_by_segment(self._call(self.http, "GET", f"/v1/route/{quote(fullname, safe='')}/files"))

  def set_public(self, fullname: str, public: bool) -> None:
    data = self._call(self.http, "PATCH", f"/v1/route/{quote(fullname, safe='')}/", json={"is_public": public})
    if isinstance(data, dict) and data.get("is_public") not in (None, public):
      raise CommaError("comma didn't change the route's sharing")

  def request_uploads(self, dongle_id: str, paths: list[str], expiry_days: int = 7) -> dict:
    """Ask the device to upload these files, as Connect's upload button does. Returns athena's result, or
    `{"queued": True}` when the device is offline and the request waits for it."""
    if not paths:
      return {"enqueued": 0}
    if any(p.rsplit("/", 1)[-1].startswith("dcamera") for p in paths):
      raise ValueError("driver camera files are never requested")
    urls = self._call(self.http, "POST", f"/v1/{dongle_id}/upload_urls/",
                      json={"paths": paths, "expiry_days": expiry_days})
    if not isinstance(urls, list) or len(urls) != len(paths) or \
        not all(isinstance(u, dict) and isinstance(u.get("url"), str) for u in urls):
      raise CommaError("comma sent unreadable upload links")
    files = [{"fn": p, "url": u["url"], "headers": u.get("headers") or {}, "allow_cellular": False, "priority": 1}
             for p, u in zip(paths, urls)]
    body = {"id": 0, "jsonrpc": "2.0", "method": "uploadFilesToUrls", "params": {"files_data": files},
            "expiry": int(time.time()) + expiry_days * 86400}
    res = self._call(self.athena, "POST", f"/{dongle_id}", json=body)
    if not isinstance(res, dict) or "error" in res:
      raise CommaError("the device refused the upload request")
    result = res.get("result")
    if result == OFFLINE_QUEUED:
      return {"queued": True}
    return result if isinstance(result, dict) else {}

  def upload_queue(self, dongle_id: str) -> list[str] | None:
    """The device paths in its upload queue, the one uploading included, whoever queued them; None when that can't be
    known now (the device is offline). Never queued for later: an old answer would be worse than none."""
    body = {"id": 0, "jsonrpc": "2.0", "method": "listUploadQueue", "params": {}}
    try:
      res = self._call(self.athena, "POST", f"/{dongle_id}", json=body)
    except CommaAuthError:
      raise
    except CommaError:
      return None
    result = res.get("result") if isinstance(res, dict) else None
    if not isinstance(result, list):
      return None
    return [str(i.get("path") or "") for i in result if isinstance(i, dict)]

  def download(self, url: str, dest: Path, deadline_s: float = FILE_DEADLINE_S) -> tuple[str, int]:
    """Stream a signed file URL to `dest`; returns (sha256, size)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    h, size, until = hashlib.sha256(), 0, time.monotonic() + deadline_s
    try:
      with self.files.stream("GET", url) as r:
        if r.status_code in (401, 403):
          raise LinkExpired("the file link expired")
        if r.status_code != 200:
          raise CommaError(f"file download answered {r.status_code}", transient=r.status_code >= 500)
        with tmp.open("wb") as f:
          for chunk in r.iter_bytes(1 << 20):
            if time.monotonic() > until:
              raise TooSlow("a file download took too long", transient=True)
            f.write(chunk)
            h.update(chunk)
            size += len(chunk)
    except httpx.HTTPError as e:
      tmp.unlink(missing_ok=True)
      raise CommaError(f"file download failed ({type(e).__name__})", transient=True) from None
    except CommaError:
      tmp.unlink(missing_ok=True)
      raise
    tmp.replace(dest)
    return h.hexdigest(), size

