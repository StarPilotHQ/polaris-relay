"""Client for the Operation Polaris API (/api/v1, documented on the site at /docs/api)."""
import hashlib
import io
import re
import tarfile
import time
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
import zstandard

from relay import __version__

USER_AGENT = f"polaris-relay/{__version__}"
FILE_DEADLINE_S = 30 * 60  # one upload, wall clock
CHUNK = 1 << 20
API_KIND = {"logs": "rlog", "cameras": "fcamera", "ecameras": "ecamera", "qcameras": "qcamera"}
FROM_API = {v: k for k, v in API_KIND.items()}


class SiteError(Exception):
  def __init__(self, message: str, code: str = "", status: int = 0, body: dict | None = None):
    super().__init__(message)
    self.code, self.status, self.body = code, status, body or {}

  @property
  def transient(self) -> bool:
    """The site or the network didn't answer: waiting is the fix, not giving up on the route."""
    return self.code in ("unreachable", "timeout") or self.status >= 500

  @property
  def deadline(self) -> bool:
    return self.code == "timeout"


class _TooSlow(Exception):
  pass


def _chunks(path: Path, deadline_s: float):
  until = time.monotonic() + deadline_s
  with path.open("rb") as f:
    while chunk := f.read(CHUNK):
      if time.monotonic() > until:
        raise _TooSlow()
      yield chunk


class RateLimited(SiteError):
  @property
  def retry_after(self) -> int:
    try:
      return max(1, int(self.body.get("retry_after") or 60))
    except (TypeError, ValueError):
      return 60


class StorageFull(RateLimited):
  pass


class TokenRejected(SiteError):
  pass


class DeviceNotVerified(SiteError):
  pass


MAX_TIME = 253402300799  # 9999-12-31T23:59:59Z
_LINK = re.compile(r'<([^>]*)>\s*((?:;\s*[^;,]+)*)')


def deprecation(headers) -> dict | None:
  """RFC 9745 `Deprecation: @<seconds>`, with RFC 8594 `Sunset` (an HTTP date) and the `rel="deprecation"` link, from
  one response. None when the response doesn't say the API is deprecated, or says it in a form that can't be read."""
  raw = (headers.get("deprecation") or "").strip()
  if not re.fullmatch(r"@-?\d{1,12}", raw) or not 0 <= int(raw[1:]) <= MAX_TIME:  # past year 9999 can't be shown
    return None
  out = {"since": int(raw[1:]), "sunset": None, "link": None}
  try:
    sunset = parsedate_to_datetime(headers.get("sunset") or "")
    out["sunset"] = int((sunset if sunset.tzinfo else sunset.replace(tzinfo=UTC)).timestamp())
    if not 0 <= out["sunset"] <= MAX_TIME:
      out["sunset"] = None
  except (TypeError, ValueError, IndexError, OverflowError):
    pass
  for m in _LINK.finditer(headers.get("link") or ""):
    rel = re.search(r';\s*rel="?([^";]*)"?', m[2])
    if rel and "deprecation" in rel[1].lower().split() and m[1].startswith(("https://", "http://")):
      out["link"] = m[1]
      break
  return out


class SiteClient:
  def __init__(self, base: str, token: str, transport: httpx.BaseTransport | None = None, timeout: float = 120.0):
    self.http = httpx.Client(base_url=base.rstrip("/") + "/api/v1", transport=transport, timeout=timeout,
                             headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT})
    # What the last response said about deprecation: unset until one arrives, then None or the parsed headers.
    self.answered, self.deprecation = False, None

  def close(self) -> None:
    self.http.close()

  def _call(self, method: str, path: str, *, ok=(200, 201), **kw) -> httpx.Response:
    try:
      r = self.http.request(method, path, **kw)
    except httpx.HTTPError as e:
      raise SiteError(f"the site is unreachable ({type(e).__name__})", "unreachable") from None
    except _TooSlow:
      raise SiteError("an upload to the site took too long", "timeout") from None
    if r.status_code < 500 or "json" in r.headers.get("content-type", ""):  # the API's answer, not a proxy's error page
      self.answered, self.deprecation = True, deprecation(r.headers)
    if r.status_code in ok:
      return r
    try:
      body = r.json()
    except ValueError:
      body = {}
    body = body if isinstance(body, dict) else {}
    code, message = body.get("error", ""), body.get("message") or f"the site answered {r.status_code}"
    if r.status_code in (429, 507):
      if "retry_after" not in body and r.headers.get("retry-after", "").isdigit():
        body["retry_after"] = int(r.headers["retry-after"])
      if r.status_code == 507:
        raise StorageFull(message, code or "storage_full", 507, body)
      raise RateLimited(message, code or "rate_limited", 429, body)
    if r.status_code == 401:
      raise TokenRejected(message, code or "invalid_token", 401, body)
    if code == "device_not_verified":
      raise DeviceNotVerified(message, code, r.status_code, body)
    raise SiteError(message, code, r.status_code, body)

  @staticmethod
  def _json(r: httpx.Response) -> dict:
    try:
      body = r.json()
    except ValueError:
      raise SiteError("the site sent an unreadable answer", "unreadable", r.status_code) from None
    if not isinstance(body, dict):
      raise SiteError("the site sent an unreadable answer", "unreadable", r.status_code)
    return body

  def me(self) -> dict:
    return self._json(self._call("GET", "/me"))

  def device_verified(self, dongle_id: str, provider: str = "comma") -> bool:
    body = {"dongle_id": dongle_id, **({"provider": provider} if provider != "comma" else {})}
    return bool(self._json(self._call("POST", "/devices/check", json=body)).get("verified"))

  def schema_bundle(self) -> tuple[str, bytes]:
    r = self._call("GET", "/schema")
    version = r.headers.get("x-schema-version") or hashlib.sha256(r.content).hexdigest()[:12]
    return version, r.content

  def create_route(self, fullname: str, prefix: str = "") -> dict:
    return self._json(self._call("POST", "/routes", json={"route": prefix + fullname}))

  def put_qlog(self, handle: str, segment: int, path: Path, sha256: str, name: str,
               deadline_s: float = FILE_DEADLINE_S) -> None:
    self._call("PUT", f"/routes/{handle}/qlogs/{segment}", content=_chunks(path, deadline_s),
               headers={"X-Sha256": sha256, "X-File-Name": name, "Content-Type": "application/octet-stream",
                        "Content-Length": str(path.stat().st_size)})

  def delete_route(self, handle: str) -> None:
    self._call("DELETE", f"/routes/{handle}")

  def suggest(self, handle: str, segments: list[int], excluded: list[int], refresh: bool = False) -> dict:
    return self._json(self._call("POST", f"/routes/{handle}/suggest",
                                 json={"segments": segments, "excluded": excluded, "refresh": refresh}))

  def put_file(self, handle: str, segment: int, kind: str, path: Path, sha256: str, name: str,
               deadline_s: float = FILE_DEADLINE_S) -> None:
    self._call("PUT", f"/routes/{handle}/files/{segment}/{API_KIND[kind]}", content=_chunks(path, deadline_s),
               headers={"X-Sha256": sha256, "X-File-Name": name, "Content-Type": "application/octet-stream",
                        "Content-Length": str(path.stat().st_size)})

  def submit(self, handle: str, suggestions: list[str], transfer: str, idempotency_key: str) -> dict:
    return self._json(self._call("POST", f"/routes/{handle}/submit",
                                 json={"suggestions": suggestions, "transfer": transfer},
                                 headers={"Idempotency-Key": idempotency_key}))

  def record_drive(self, handle: str) -> dict:
    return self._json(self._call("POST", f"/routes/{handle}/drive"))

  def submission(self, submission_id: str) -> dict:
    return self._json(self._call("GET", f"/submissions/{submission_id}"))

  def withdraw(self, submission_id: str) -> dict:
    return self._json(self._call("POST", f"/submissions/{submission_id}/withdraw"))


def extract_schema(bundle: bytes, dest: Path) -> None:
  """Unpack a schema bundle (tar.zst of .capnp files) into `dest`, refusing anything that isn't a plain file inside it."""
  raw = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(bundle)).read()
  tmp = dest.with_name(dest.name + ".tmp")
  tmp.mkdir(parents=True, exist_ok=True)
  with tarfile.open(fileobj=io.BytesIO(raw)) as tar:
    for m in tar.getmembers():
      target = (tmp / m.name).resolve()
      if not m.isfile() or not m.name.endswith(".capnp") or tmp.resolve() not in target.parents:
        raise ValueError(f"unexpected entry in the schema bundle: {m.name!r}")
      target.parent.mkdir(parents=True, exist_ok=True)
      target.write_bytes(tar.extractfile(m).read())
  if not (tmp / "log.capnp").exists():
    raise ValueError("the schema bundle has no log.capnp")
  tmp.replace(dest)
