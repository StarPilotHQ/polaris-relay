"""The local web UI: sign-in, onboarding and settings, privacy zones, notifications and the activity list."""
import hashlib
import json
import logging
import re
import secrets
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import bcrypt
from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from relay import __version__, cluster, db, notify
from relay.comma import CLOUDS, CommaClient, CommaError, cloud, read_only, token_expiry
from relay.config import Config
from relay.db import Device, Notifier, Route, Zone, now_utc
from relay.pipeline import State, Worker, deprecation_parts, filter_changed, version_banner
from relay.site import SiteClient, SiteError

log = logging.getLogger("relay")
HERE = Path(__file__).parent
LOGIN_FAILS_BEFORE_LOCK = 5
LOGIN_LOCK_S = 60
LOGIN_GLOBAL_FAILS = 50  # per LOGIN_LOCK_S across every address: a spread-out guess still gets slowed down
LOGIN_TRACKED = 1000
QUIET_WARNING_S = 60
TILE_HOST = re.compile(r"^(\{s\}\.)?[A-Za-z0-9.-]+(:\d+)?$")


def tile_url_ok(url: str) -> str | None:
  """The tile URL with its host IDNA-encoded, or None when it isn't a plain https tile template."""
  if not isinstance(url, str) or not url.startswith("https://") or not all(p in url for p in ("{z}", "{x}", "{y}")):
    return None
  rest = url[len("https://"):]
  host, slash, path = rest.partition("/")
  if not slash or "@" in host:
    return None
  sub, name = ("{s}.", host[4:]) if host.startswith("{s}.") else ("", host)
  name, colon, port = name.partition(":")
  try:
    name = name.encode("idna").decode("ascii")
  except UnicodeError:
    return None
  host = sub + name + (colon + port)
  if not name or not TILE_HOST.match(host) or (port and not port.isdigit()):
    return None
  return f"https://{host}/{path}"


STEP_LABELS = {
  "contiguous": "All segments listed", "pinged": "Device online after the drive", "quiet": "Logs stopped arriving",
  "settled": "Drive settled", "ready": "Logs complete", "filtered": "Checked against privacy zones",
  "regrew": "More logs arrived", "grew_seen": "More logs arrived", "uploads_requested": "Files requested",
  "submitted": "Submitted", "copy_error_since": "The site had trouble with the files",
}
FALLBACK_FORMATS = {"date": "%Y-%m-%d", "datetime": "%Y-%m-%d %H:%M UTC", "time": "%H:%M UTC"}


def when(at: datetime | str | None, style: str = "datetime") -> Markup:
  """A <time> element; local-time.js rewrites its text in the browser's locale and time zone (UTC without JavaScript)."""
  if not at:
    return Markup("")
  at = datetime.fromisoformat(at) if isinstance(at, str) else at
  at = (at if at.tzinfo else at.replace(tzinfo=UTC)).astimezone(UTC)
  return Markup('<time datetime="{}" data-style="{}">{}</time>').format(
    at.isoformat(timespec="seconds"), style, at.strftime(FALLBACK_FORMATS[style]))


def create_app(config: Config, sessionmaker, worker: Worker | None = None, *, start_worker: bool = False,
               comma_factory=None, site_factory=None) -> FastAPI:
  @asynccontextmanager
  async def lifespan(_app):
    if start_worker:
      threading.Thread(target=worker.run_forever, name="relay-worker", daemon=True).start()
      threading.Thread(target=worker.run_safety_net_forever, name="relay-safety-net", daemon=True).start()
    yield
    worker.stop.set()
    worker.wake.set()

  app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
  templates = Jinja2Templates(directory=str(HERE / "templates"))
  templates.env.globals["when"] = when
  templates.env.globals["step_labels"] = STEP_LABELS
  comma_factory = comma_factory or (
    lambda token, cloud="comma": CommaClient(token, *config.endpoints(cloud), cloud=cloud))
  site_factory = site_factory or (lambda token: SiteClient(config.polaris_host, token))
  worker = worker or Worker(config, sessionmaker, comma_factory, site_factory)
  app.state.worker = worker
  failures: dict[str, tuple[int, float]] = {}
  global_fails: list[float] = []
  tiles: dict = {}
  session_tag = hashlib.sha256(config.auth_hash or b"open").hexdigest()[:16]

  static_dir = HERE / "static"
  asset_version = hashlib.sha256(b"".join(p.read_bytes() for p in sorted(static_dir.glob("*.*")))).hexdigest()[:8]

  warned: dict[str, float] = {}

  def warn_quietly(message: str, *args) -> None:
    now = time.monotonic()
    if now - warned.get(message, -QUIET_WARNING_S) >= QUIET_WARNING_S:
      warned[message] = now
      log.warning(message, *args)

  def refuse(status: int, reason: str, value: str | None) -> PlainTextResponse:
    warn_quietly(f"refused a request: {reason} (%r)", (value or "")[:100])
    return PlainTextResponse("Bad Request" if status == 400 else "Forbidden", status_code=status)

  @app.middleware("http")
  async def guard(request: Request, call_next):
    host = request.headers.get("host")
    if request.url.path != "/healthz" and not config.host_allowed(host):
      return refuse(400, "host not in ALLOWED_HOSTS", host)
    if request.method not in ("GET", "HEAD", "OPTIONS"):
      origin = request.headers.get("origin")
      if origin is None and (ref := request.headers.get("referer")):
        parts = urlsplit(ref)
        origin = f"{parts.scheme}://{parts.netloc}"
      if not config.origin_allowed(origin):
        return refuse(403, "origin not in ALLOWED_ORIGINS", origin)
    response = await call_next(request)
    tile_host = tile_url().split("/")[2].replace("{s}.", "*.")
    response.headers["Content-Security-Policy"] = (
      "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
      f"img-src 'self' data: https://{tile_host}; connect-src 'self'; "
      "frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    if not request.url.path.startswith("/static/"):
      response.headers["Cache-Control"] = "no-store"
    return response

  app.add_middleware(SessionMiddleware, secret_key=config.secret_key(), session_cookie="relay_session",
                     same_site="strict", https_only=config.secure_cookies, max_age=30 * 86400)
  app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

  def tile_url() -> str:
    if "url" not in tiles:
      with sessionmaker() as s:
        tiles["url"] = tile_url_ok(db.get(s, "tile_url")) or db.DEFAULTS["tile_url"]
    return tiles["url"]

  def client_ip(request: Request) -> str:
    ip = request.client.host if request.client else "?"
    if ip in config.trusted_proxies and (fwd := request.headers.get("x-forwarded-for")):
      return fwd.split(",")[-1].strip() or ip
    return ip

  def get_db():
    with sessionmaker() as s:
      yield s

  def signed_in(request: Request) -> bool:
    return config.insecure_no_auth or (request.session.get("user") == config.auth_user
                                       and request.session.get("tag") == session_tag)

  def require_user(request: Request):
    if not signed_in(request):
      raise HTTPException(status_code=303, headers={"Location": "/login"})

  def csrf_token(request: Request) -> str:
    if "csrf" not in request.session:
      request.session["csrf"] = secrets.token_urlsafe(32)
    return request.session["csrf"]

  def check_csrf(request: Request, csrf: str = Form("")):
    expected = request.session.get("csrf")
    if not expected or not secrets.compare_digest(expected, csrf):
      raise HTTPException(status_code=403, detail="The form expired. Reload the page and try again.")

  def flash(request: Request, message: str, kind: str = "info") -> None:
    request.session["flash"] = [*request.session.get("flash", []), [kind, message]]

  def back(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)

  def render(request: Request, name: str, s: Session | None = None, **ctx) -> HTMLResponse:
    banners = []
    if s is not None:
      for key in ("pause:comma", "pause:site"):
        if msg := db.get(s, key):
          banners.append(("error", msg))
      if dep := db.get(s, "deprecation"):
        link = dep.get("link") or ""
        link = link if link.startswith(config.polaris_host + "/") else config.site_url("/docs/api")
        text = Markup("").join(when(p, "date") if isinstance(p, datetime) else p for p in deprecation_parts(dep, now_utc()))
        banners.append(("warning", text, link))
      if (exp := token_expiry(db.get(s, "comma_token") or "")) and exp - now_utc() < timedelta(days=7):
        banners.append(("warning", Markup("Your comma token expires {}. Paste a new one in Setup.").format(when(exp))))
      if latest := version_banner(s):
        banners.append(("info", f"Version {latest} is available (you have {__version__})."))
    flashes = request.session.pop("flash", [])
    return templates.TemplateResponse(request, name, {
      "csrf": csrf_token(request), "banners": banners, "flashes": flashes, "auth": not config.insecure_no_auth,
      "version": __version__, "asset_version": asset_version, "labels": State.LABELS, **ctx})

  # ----- sign-in -----

  @app.get("/login", response_class=HTMLResponse)
  def login_page(request: Request):
    if signed_in(request):
      return back("/")
    return render(request, "login.html")

  @app.post("/login")
  def login(request: Request, username: str = Form(""), password: str = Form(""), _=Depends(check_csrf)):
    ip, now = client_ip(request), time.monotonic()
    count, until = failures.get(ip, (0, 0.0))
    global_fails[:] = [t for t in global_fails if now - t < LOGIN_LOCK_S]
    if until > now or len(global_fails) >= LOGIN_GLOBAL_FAILS:
      flash(request, "Too many attempts. Wait a minute and try again.", "error")
      return back("/login")
    user_ok = secrets.compare_digest(username.encode(), (config.auth_user or "").encode())
    password_ok = config.auth_hash is not None and bcrypt.checkpw(password.encode()[:72], config.auth_hash)
    if not (user_ok and password_ok):
      count += 1
      global_fails.append(now)
      if len(failures) >= LOGIN_TRACKED and ip not in failures:
        failures.pop(next(iter(failures)))
      failures[ip] = (count, now + LOGIN_LOCK_S if count >= LOGIN_FAILS_BEFORE_LOCK else 0.0)
      flash(request, "That user name and password don't match.", "error")
      return back("/login")
    failures.pop(ip, None)
    request.session.clear()
    request.session["user"], request.session["tag"] = config.auth_user, session_tag
    return back("/")

  @app.post("/logout")
  def logout(request: Request, _=Depends(check_csrf)):
    request.session.clear()
    return back("/login")

  @app.get("/healthz", response_class=PlainTextResponse)
  def healthz():
    if not worker.healthy():
      warn_quietly("unhealthy: the worker or the safety net has stopped making progress")
      return PlainTextResponse("unhealthy", status_code=503)
    return "ok"

  # ----- activity -----

  @app.get("/", response_class=HTMLResponse)
  def activity(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    if not db.get(s, "onboarded_at"):
      return back("/setup")
    routes = s.scalars(select(Route).order_by(Route.start_time.desc()).limit(200)).all()
    devices = {d.dongle_id: d for d in s.scalars(select(Device))}
    return render(request, "activity.html", s, routes=routes, devices=devices, final=State.FINAL,
                  site_prefix=config.polaris_host + "/", keys={r.fullname: worker.route_key(r) for r in routes},
                  skippable=worker.skippable, ready=State.READY, submit_at=lambda r: worker.submit_at(s, r),
                  now=now_utc())

  @app.post("/drives/{key}/skip")
  def skip_drive(request: Request, key: str, _=Depends(require_user), __=Depends(check_csrf),
                 s: Session = Depends(get_db)):
    """Mark a drive skipped; the worker stops it at its next step (it may be mid-transfer on its own thread)."""
    r = next((r for r in s.scalars(select(Route).where(Route.state.not_in(State.FINAL)))
              if worker.route_key(r) == key), None)
    if r is None or not worker.skippable(r):
      flash(request, "That drive can't be skipped now: it's finished, or a submit has already gone out.", "error")
      return back("/")
    r.skip_requested, r.note = True, "Skipping…"
    s.commit()
    worker.wake.set()
    flash(request, "Skipped. Nothing will be submitted for this drive.")
    return back("/")

  # ----- setup and settings -----

  def settings_ctx(s: Session) -> dict:
    keys = ("transfer", "surround", "tile_url", "update_check", "digest", "ping_after_end_min", "quiet_min",
            "settle_min", "files_wait_days", "public_cap_h", "auto_withdraw", "submit_delay_min")
    return {"devices": s.scalars(select(Device).order_by(Device.alias)).all(),
            "zone_count": len(s.scalars(select(Zone.id)).all()),
            "comma_set": bool(db.get(s, "comma_token")), "comma_expiry": token_expiry(db.get(s, "comma_token") or ""),
            "token_info": db.get(s, "token_info"), "onboarded": db.get(s, "onboarded_at"),
            "cloud": cloud(db.get(s, "cloud")), "clouds": CLOUDS.values(),
            **{k: db.get(s, k) for k in keys}}

  @app.get("/setup", response_class=HTMLResponse)
  def setup(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    if db.get(s, "onboarded_at"):
      return back("/settings")
    return render(request, "settings.html", s, onboarding=True, **settings_ctx(s))

  @app.get("/settings", response_class=HTMLResponse)
  def settings_page(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    if not db.get(s, "onboarded_at"):
      return back("/setup")
    return render(request, "settings.html", s, onboarding=False, **settings_ctx(s))

  def settings_url(s: Session) -> str:
    return "/settings" if db.get(s, "onboarded_at") else "/setup"

  def refresh_devices(s: Session, comma: CommaClient) -> int:
    owned = 0
    for dev in comma.devices():
      dongle = (dev.get("dongle_id") or "").lower()
      if len(dongle) != 16:
        continue
      d = s.get(Device, dongle)
      if read_only(dev):
        if d is not None:
          s.delete(d)
        continue
      d = d or Device(dongle_id=dongle, enabled=False)
      d.alias, d.last_athena_ping = dev.get("alias") or dev.get("device_type") or "", int(dev.get("last_athena_ping") or 0)
      s.add(d)
      owned += 1
    s.commit()
    return owned

  @app.post("/setup/comma")
  def set_comma(request: Request, token: str = Form(""), cloud_name: str = Form("comma", alias="cloud"),
                _=Depends(require_user), __=Depends(check_csrf), s: Session = Depends(get_db)):
    c = cloud(cloud_name)
    token = token.strip().removeprefix("JWT ").strip()
    exp = token_expiry(token)
    if exp is None:
      flash(request, f"That doesn't look like a {c.label} token. Copy the whole token from {c.token_from}.", "error")
      return back(settings_url(s))
    if exp < now_utc():
      flash(request, f"That token has already expired. Get a new one from {c.token_from}.", "error")
      return back(settings_url(s))
    switching = c.name != db.get(s, "cloud")
    if switching and s.scalar(select(Route.fullname).where(Route.public_since.is_not(None)).limit(1)):
      flash(request, f"A drive is still public on {cloud(db.get(s, 'cloud')).connect}. Wait until it's private "
                     "again before switching.", "error")
      return back(settings_url(s))
    comma = comma_factory(token, c.name)
    try:
      if switching:  # devices are verified with the site per service: each is enabled again on the new one
        for d in s.scalars(select(Device)).all():
          d.enabled = False
      n = refresh_devices(s, comma)
    except CommaError as e:
      s.rollback()
      flash(request, f"{c.label} didn't accept that token ({e}).", "error")
      return back(settings_url(s))
    finally:
      comma.close()
    db.put(s, "cloud", c.name)
    if not c.public:
      db.put(s, "transfer", "relay")
    db.put(s, "comma_token", token)
    db.delete(s, "pause:comma")
    notify.clear(s, "comma_token")
    notify.clear(s, "comma_token_expiring")
    s.commit()
    flash(request, f"{c.label} token saved. Found {n} device{'s' if n != 1 else ''}.")
    return back(settings_url(s))

  @app.post("/setup/token")
  def set_token(request: Request, token: str = Form(""), _=Depends(require_user), __=Depends(check_csrf),
                 s: Session = Depends(get_db)):
    token = token.strip()
    if not token.startswith("opu_"):
      flash(request, "That isn't an Operation Polaris access token. Create one under Profile → Access tokens on the site (it "
                     "starts with opu_).", "error")
      return back(settings_url(s))
    site = site_factory(token)
    try:
      me = site.me()
    except SiteError as e:
      flash(request, f"The site didn't accept that token ({e}).", "error")
      return back(settings_url(s))
    finally:
      site.close()
    if site.deprecation:
      db.put(s, "deprecation", site.deprecation)
    token_info = me.get("token") or {}
    db.put(s, "access_token", token)
    scopes = token_info.get("scopes") or []
    db.put(s, "token_info", {"handle": me.get("handle"), "name": token_info.get("name"), "scopes": scopes,
                              "checked": now_utc().timestamp()})
    db.delete(s, "pause:site")
    notify.clear(s, "access_token")
    s.commit()
    flash(request, "Access token saved." if "submit" in scopes else "Access token saved. It can't submit, so drives are "
          "only recorded and checked for moments; no files are requested from your devices.")
    return back(settings_url(s))

  @app.post("/devices/refresh")
  def devices_refresh(request: Request, _=Depends(require_user), __=Depends(check_csrf), s: Session = Depends(get_db)):
    token = db.get(s, "comma_token")
    if not token:
      flash(request, "Paste your comma or Konik token first.", "error")
      return back(settings_url(s))
    comma = comma_factory(token, db.get(s, "cloud"))
    try:
      refresh_devices(s, comma)
    except CommaError as e:
      flash(request, f"Couldn't list your devices ({e}).", "error")
    finally:
      comma.close()
    return back(settings_url(s))

  @app.post("/devices/{dongle_id}/enable")
  def device_enable(request: Request, dongle_id: str, _=Depends(require_user), __=Depends(check_csrf),
                    s: Session = Depends(get_db)):
    d = s.get(Device, dongle_id)
    token = db.get(s, "access_token")
    if d is None:
      raise HTTPException(404)
    if not token:
      flash(request, "Paste your access token first; the site has to confirm the device.", "error")
      return back(settings_url(s))
    site = site_factory(token)
    try:
      verified = site.device_verified(d.dongle_id, db.get(s, "cloud"))
    except SiteError as e:
      flash(request, f"Couldn't ask the site about this device ({e}).", "error")
      return back(settings_url(s))
    finally:
      site.close()
    if not verified:
      konik = " A Konik Stable device also needs an operator to allow your account on the site and through the API." \
        if db.get(s, "cloud") == "konik" else ""
      flash(request, f"The site hasn't verified {d.alias or 'this device'} on your account. Enter one of its routes on "
                     f"the site to run the device check, then enable it here.{konik}", "error")
      return back(settings_url(s))
    resuming = bool(d.paused_reason) and d.enabled
    d.paused_reason, d.verified_at = None, now_utc()
    if not resuming:  # a resume after a site pause picks up the drives made while it was paused (it stayed enabled)
      d.enabled, d.enabled_at = True, now_utc()
    notify.clear(s, f"device:{d.dongle_id}")
    s.commit()
    flash(request, f"{d.alias or 'Device'} resumed. Drives from the last few days that it missed are picked up." if resuming
          else f"{d.alias or 'Device'} enabled. Drives that start from now on are picked up; earlier ones aren't.")
    return back(settings_url(s))

  @app.post("/devices/{dongle_id}/disable")
  def device_disable(request: Request, dongle_id: str, _=Depends(require_user), __=Depends(check_csrf),
                     s: Session = Depends(get_db)):
    if (d := s.get(Device, dongle_id)) is None:
      raise HTTPException(404)
    d.enabled = False
    s.commit()
    flash(request, f"{d.alias or 'Device'} disabled. Drives already in progress stop where they are.")
    return back(settings_url(s))

  @app.post("/devices/{dongle_id}/options")
  def device_options(request: Request, dongle_id: str, mode: str = Form("auto"), record_drives: str = Form(""),
                     _=Depends(require_user), __=Depends(check_csrf), s: Session = Depends(get_db)):
    if (d := s.get(Device, dongle_id)) is None:
      raise HTTPException(404)
    d.mode = mode if mode in ("auto", "suggest") else "auto"
    d.record_drives = bool(record_drives)
    s.commit()
    flash(request, "Saved.")
    return back(settings_url(s))

  def _num(value: str, lo: float, hi: float, default):
    try:
      return min(hi, max(lo, float(value)))
    except (TypeError, ValueError):
      return default

  @app.post("/settings/general")
  def settings_general(request: Request, transfer: str = Form("relay"), surround: str = Form("0"),
                       update_check: str = Form(""), _=Depends(require_user), __=Depends(check_csrf),
                       s: Session = Depends(get_db)):
    public_ok = cloud(db.get(s, "cloud")).public
    db.put(s, "transfer", transfer if transfer in ("relay", "public") and public_ok else "relay")
    if int(_num(surround, 0, 5, 0)) != db.get(s, "surround"):
      db.put(s, "surround", int(_num(surround, 0, 5, 0)))
      filter_changed(s)
    db.put(s, "update_check", bool(update_check))
    s.commit()
    flash(request, "Saved.")
    return back(settings_url(s))

  @app.post("/settings/advanced")
  def settings_advanced(request: Request, ping_after_end_min: str = Form("3"), quiet_min: str = Form("10"),
                        settle_min: str = Form("30"), files_wait_days: str = Form("3"), public_cap_h: str = Form("6"),
                        auto_withdraw: str = Form(""), submit_delay_min: str = Form("30"), _=Depends(require_user),
                        __=Depends(check_csrf), s: Session = Depends(get_db)):
    db.put(s, "ping_after_end_min", _num(ping_after_end_min, 0, 120, 3))
    db.put(s, "quiet_min", _num(quiet_min, 1, 240, 10))
    db.put(s, "settle_min", _num(settle_min, 0, 720, 30))
    db.put(s, "files_wait_days", _num(files_wait_days, 0.5, 6, 3))
    db.put(s, "public_cap_h", _num(public_cap_h, 0.5, 72, 6))
    db.put(s, "auto_withdraw", bool(auto_withdraw))
    db.put(s, "submit_delay_min", _num(submit_delay_min, 0, 1440, 30))
    s.commit()
    flash(request, "Advanced settings saved.")
    return back(settings_url(s))

  @app.post("/setup/finish")
  def setup_finish(request: Request, _=Depends(require_user), __=Depends(check_csrf), s: Session = Depends(get_db)):
    if not db.get(s, "comma_token") or not db.get(s, "access_token"):
      flash(request, "Paste both tokens first.", "error")
      return back("/setup")
    db.put(s, "onboarded_at", now_utc().timestamp())
    s.commit()
    worker.wake.set()
    flash(request, "Setup complete. Drives that start from now on are picked up; earlier ones aren't.")
    return back("/")

  # ----- privacy zones -----

  def zone_dict(z: Zone) -> dict:
    return {"id": z.id, "name": z.name, "kind": z.kind, "lat": z.lat, "lng": z.lng, "radius_m": z.radius_m,
            "points": z.points}

  @app.get("/zones", response_class=HTMLResponse)
  def zones_page(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    zones = [zone_dict(z) for z in s.scalars(select(Zone).order_by(Zone.id))]
    return render(request, "zones.html", s, zones=zones, zones_json=json.dumps(zones), tile_url=tile_url(),
                  surround=db.get(s, "surround"), onboarded=db.get(s, "onboarded_at"))

  @app.post("/zones")
  def zone_add(request: Request, kind: str = Form(...), name: str = Form(""), lat: str = Form(""), lng: str = Form(""),
               radius_m: str = Form(""), points: str = Form(""), _=Depends(require_user), __=Depends(check_csrf),
               s: Session = Depends(get_db)):
    try:
      if kind == "circle":
        la, ln, rad = float(lat), float(lng), float(radius_m)
        if not (-90 <= la <= 90 and -180 <= ln <= 180 and 25 <= rad <= 50_000):
          raise ValueError
        s.add(Zone(kind="circle", name=name.strip()[:64], lat=la, lng=ln, radius_m=rad))
      elif kind == "polygon":
        pts = [[float(a), float(b)] for a, b in json.loads(points)]
        if len(pts) < 3 or len(pts) > 200 or not all(-90 <= a <= 90 and -180 <= b <= 180 for a, b in pts):
          raise ValueError
        s.add(Zone(kind="polygon", name=name.strip()[:64], points=pts))
      else:
        raise ValueError
    except (ValueError, TypeError, json.JSONDecodeError):
      flash(request, "That zone isn't valid. A circle needs a center and a radius of 25 m to 50 km; a polygon needs at "
                     "least 3 points.", "error")
      return back("/zones")
    filter_changed(s)
    s.commit()
    flash(request, "Zone added.")
    return back("/zones")

  @app.post("/zones/{zone_id}/delete")
  def zone_delete(request: Request, zone_id: int, _=Depends(require_user), __=Depends(check_csrf),
                  s: Session = Depends(get_db)):
    if (z := s.get(Zone, zone_id)) is not None:
      s.delete(z)
      filter_changed(s)
      s.commit()
      flash(request, "Zone removed.")
    return back("/zones")

  @app.post("/zones/tiles")
  def zone_tiles(request: Request, tile_url_field: str = Form("", alias="tile_url"), _=Depends(require_user), __=Depends(check_csrf),
                 s: Session = Depends(get_db)):
    url = tile_url_ok(tile_url_field.strip() or db.DEFAULTS["tile_url"])
    if url is None:
      flash(request, "The tile URL must be https, name a plain host, and contain {z}, {x} and {y}.", "error")
      return back("/zones")
    db.put(s, "tile_url", url)
    s.commit()
    tiles.pop("url", None)
    flash(request, "Map tiles saved.")
    return back("/zones")

  @app.get("/zones/proposals")
  def zone_proposals(_=Depends(require_user), s: Session = Depends(get_db)):
    token = db.get(s, "comma_token")
    if not token:
      return JSONResponse({"error": "Paste your comma or Konik token first."}, status_code=400)
    comma = comma_factory(token, db.get(s, "cloud"))
    points: list[tuple[float, float]] = []
    try:
      end = now_utc()
      for d in s.scalars(select(Device)).all():
        for m in comma.routes(d.dongle_id, end - timedelta(days=30), end, limit=200):
          points += [tuple(p) for p in (m.start_point, m.end_point) if p]
    except CommaError as e:
      return JSONResponse({"error": f"Couldn't read your recent drives ({e})."}, status_code=502)
    finally:
      comma.close()
    return {"proposals": [vars(p) for p in cluster.propose(points)]}

  # ----- notifications -----

  @app.get("/notifications", response_class=HTMLResponse)
  def notifications_page(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    targets = s.scalars(select(Notifier).order_by(Notifier.id)).all()
    return render(request, "notifications.html", s, targets=targets, services=notify.SERVICES,
                  secret_fields=notify.SECRET_FIELDS, digest=db.get(s, "digest"))

  @app.post("/notifications")
  async def notification_add(request: Request, _=Depends(require_user), s: Session = Depends(get_db)):
    form = await request.form()
    check_csrf(request, str(form.get("csrf", "")))
    service = str(form.get("service", ""))
    if service not in notify.SERVICES:
      raise HTTPException(400)
    fields = {k: str(form.get(k, "")).strip() for k, _l, _p in notify.SERVICES[service]["fields"]}
    try:
      notify.apprise_url(service, fields)
    except ValueError as e:
      flash(request, str(e), "error")
      return back("/notifications")
    s.add(Notifier(service=service, name=str(form.get("name", "")).strip()[:64] or notify.SERVICES[service]["label"],
                   fields=fields))
    s.commit()
    flash(request, "Notification target added. Send a test to check it.")
    return back("/notifications")

  @app.post("/notifications/{target_id}/test")
  def notification_test(request: Request, target_id: int, _=Depends(require_user), __=Depends(check_csrf),
                        s: Session = Depends(get_db)):
    if (n := s.get(Notifier, target_id)) is None:
      raise HTTPException(404)
    ok = notify.send_to(n, "Polaris Relay test", "Notifications from Polaris Relay will arrive here.")
    flash(request, "Test sent." if ok else "The test didn't go through. Check the details and try again.",
          "info" if ok else "error")
    return back("/notifications")

  @app.post("/notifications/{target_id}/delete")
  def notification_delete(request: Request, target_id: int, _=Depends(require_user), __=Depends(check_csrf),
                          s: Session = Depends(get_db)):
    if (n := s.get(Notifier, target_id)) is not None:
      s.delete(n)
      s.commit()
    return back("/notifications")

  @app.post("/notifications/digest")
  def notification_digest(request: Request, digest: str = Form(""), _=Depends(require_user), __=Depends(check_csrf),
                          s: Session = Depends(get_db)):
    db.put(s, "digest", bool(digest))
    s.commit()
    flash(request, "Saved.")
    return back("/notifications")

  @app.exception_handler(HTTPException)
  async def http_error(request: Request, exc: HTTPException):
    if exc.status_code == 303:
      return RedirectResponse(exc.headers["Location"], status_code=303)
    return PlainTextResponse(str(exc.detail), status_code=exc.status_code)

  return app
