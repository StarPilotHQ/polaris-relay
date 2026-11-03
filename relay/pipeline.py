"""The per-route pipeline: discover, wait for qlogs, filter, suggest, request uploads, transfer, submit, record.

State lives in SQLite (`db.Route`) and every step is safe to repeat, so a restart resumes where it stopped.
"""
import hashlib
import hmac
import logging
import secrets
import shutil
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import text, select
from sqlalchemy.orm import Session

from relay import __version__, db, notify, privacy, updates
from relay.comma import (KIND_FILES, NEVER_KINDS, CommaAuthError, CommaClient, CommaError, CommaNotFound, LinkExpired,
                         token_expiry)
from relay.config import Config
from relay.db import Device, Route, Zone, now_utc
from relay.qlogs import read_scans
from relay.site import DeviceNotVerified, RateLimited, SiteClient, SiteError, StorageFull, TokenRejected, extract_schema

log = logging.getLogger("relay")

BACKOFF_S = (60, 300, 900, 1800, 3600, 3 * 3600, 6 * 3600)
MAX_ATTEMPTS = 8
MAX_REGRESSIONS = 5
SLOW_FILE_GRACE = 3  # deadline timeouts on the same file waited out as outages before they use attempts
OUTAGE_BACKOFF_S = (60, 120, 300, 600, 1200, 1800)  # the site or comma not answering: wait, never give up for it
DISCOVER_DAYS = 3
COPY_ERROR_GRACE = timedelta(minutes=30)
ROUTE_MAX_AGE = timedelta(days=7)  # the site's submit window; nothing older can be submitted
RE_REQUEST = timedelta(hours=6)  # asked again this often only when the device's queue can't be read
ONLINE = timedelta(minutes=10)  # a device that pinged athena this recently is online
QUEUE_GRACE = timedelta(minutes=2)  # after asking, before a missing queue item counts as gone
UPLOAD_WAIT_FIRST = timedelta(minutes=10)
UPLOAD_WAIT_MAX = timedelta(hours=12)
POLL = timedelta(minutes=5)  # how often a route waiting on the device asks comma again
LISTING_MAX_AGE = 45 * 60  # comma's signed file URLs last about an hour
PUBLIC_MAX_DAYS = 2  # a route is never made public again this long after it first was
SAFETY_NET_EVERY_S = 60  # the safety net's own thread runs this often, whatever the pipeline is doing
STEP_BUDGET_S = 40 * 60  # the longest one step may go without a heartbeat: a file transfer, or reading GPS
ROUTE_KEY = b"relay-route:"
ROUTE_EVENTS = ("zoned_after_submit", "copy_error")  # notifications about one route, keyed by `route_key`
SCHEMA_REFRESH = timedelta(days=1)
UPDATE_EVERY = timedelta(days=1)
TOKEN_WARN = timedelta(days=7)
ACTIVITY_DAYS = 30
FILE_KINDS = ("logs", "cameras", "ecameras", "qcameras")
NOT_SUBMITTABLE = "The site can't take this route. It was either blocked, already submitted, or something else prevented it."
# Site errors no retry can fix, for this route: it ends as failed with this note.
TERMINAL = {
  "route_unusable": "The site can't use this drive (its recording time couldn't be confirmed, or it's too old).",
  "suggestion_blocked": "The site no longer accepts these suggestions.",
  "suggestions_unknown": "The site no longer knows these suggestions.",
  "nothing_to_submit": "Nothing was left to submit.",
  "idempotency_key_conflict": "The site saw a different submit for this drive.",
  "bad_transfer": "The site refused the transfer method.",
  "suggest_first": "The site lost this drive's suggestions.",
  "qlog_mismatch": "The site says this drive's logs belong to another device.",
  "segment_excluded": "The site already treats a segment as private that this drive tried to send.",
  "too_large": "A file was larger than the site accepts.",
  "too_many_ranges": "This drive has more suggestions than your daily range limit allows.",
  "qlogs_frozen": "The site already has different logs for this drive.",
  "bad_idempotency_key": "The site refused this drive's submit key.",
  "scope_missing": "Your access token can't submit. Create one with \"Submit drives\" under Profile → Access tokens on the site and "
                   "paste it in Setup.",
}
# Site errors about the whole account: everything stops until the user acts on the site.
ACCOUNT = {
  "account_banned": "The site says your account can't submit. Nothing is sent until that changes.",
  "credit_choice_needed": "Choose how you'd like to be credited on the site (your Profile page), then save your access "
                          "token again here.",
}


class State:
  WAITING_QLOGS = "waiting_qlogs"
  SUGGESTING = "suggesting"
  REQUESTING = "requesting_uploads"
  WAITING_FILES = "waiting_files"
  TRANSFERRING = "transferring"
  READY = "ready"  # everything is in place; auto-submit waits out the submit delay so the user can skip it
  SUBMITTING = "submitting"
  PUBLIC = "public_copying"
  SITE_COPYING = "site_copying"  # relay mode, submitted: the site moves the files into review
  SUBMITTED = "submitted"
  SUGGESTED = "suggested"
  RECORDED = "recorded"
  SKIPPED = "skipped"
  EXPIRED = "expired"
  FAILED = "failed"
  ORDER = (WAITING_QLOGS, SUGGESTING, REQUESTING, WAITING_FILES, TRANSFERRING, READY, SUBMITTING, PUBLIC, SITE_COPYING)
  FINAL = (SUBMITTED, SUGGESTED, RECORDED, SKIPPED, EXPIRED, FAILED)
  FILTERED = (REQUESTING, WAITING_FILES, TRANSFERRING, READY, SUBMITTING)  # after the filter, before anything is final
  POLLED = (WAITING_QLOGS, WAITING_FILES, SITE_COPYING)
  LABELS = {
    WAITING_QLOGS: "Waiting for the drive to finish uploading", SUGGESTING: "Finding moments",
    REQUESTING: "Asking the device for files", WAITING_FILES: "Waiting for the device to upload",
    TRANSFERRING: "Sending files", READY: "Ready to submit", SUBMITTING: "Submitting", PUBLIC: "Copying (route public)",
    SITE_COPYING: "Submitted; the site is checking the files",
    SUBMITTED: "Submitted", SUGGESTED: "Suggestions ready", RECORDED: "Drive recorded", SKIPPED: "Skipped",
    EXPIRED: "Expired", FAILED: "Failed",
  }


class Waiting(Exception):
  """Nothing to do yet; the message says why. Not a failure."""


class SkipRequested(Exception):
  """The user pressed Skip on this drive: stop working on it and submit nothing."""


class RefilterNeeded(Exception):
  """Zones or the surround setting changed since this route was filtered: nothing more leaves until it's redone."""


class AccountPaused(Exception):
  pass


class PublicSpent(Exception):
  pass


class Worker:
  def __init__(self, config: Config, sessionmaker, comma_factory=None, site_factory=None):
    self.config = config
    self.sessionmaker = sessionmaker
    self.comma_factory = comma_factory or (lambda token: CommaClient(token, config.comma_api, config.athena_api))
    self.site_factory = site_factory or (lambda token: SiteClient(config.polaris_host, token))
    self.lock = threading.Lock()
    # Held for every change to whether a route is public, by the worker and the safety net's thread alike, so
    # neither acts on the other's stale copy of the route.
    self.public_lock = threading.RLock()
    self.stop = threading.Event()
    self.wake = threading.Event()
    self.started = False
    self.last_beat = self.last_net = time.monotonic()

  # ----- loop -----

  def run_forever(self) -> None:
    self.started = True
    while not self.stop.is_set():
      try:
        self.tick()
        self.beat()  # only a pass that completed: one failing every time must show in /healthz
      except Exception:  # keep running; the next pass retries
        log.exception("worker pass failed")
      self.wake.wait(self.config.tick_s)
      self.wake.clear()

  def run_safety_net_forever(self) -> None:
    """Make public routes private on time even while the pipeline is busy with a long step."""
    self.started = True
    while not self.stop.is_set():
      try:
        self.run_safety_net()
      except Exception:
        log.exception("safety net pass failed")
      self.stop.wait(SAFETY_NET_EVERY_S)

  def beat(self) -> None:
    self.last_beat = time.monotonic()

  def healthy(self) -> bool:
    """False when the pipeline or the safety net has stopped making progress (or never ran, once started)."""
    if not self.started:
      return True
    now = time.monotonic()
    return now - self.last_beat < 3 * self.config.tick_s + STEP_BUDGET_S and \
      now - self.last_net < 3 * SAFETY_NET_EVERY_S + 300

  def run_safety_net(self) -> None:
    with self.sessionmaker() as s:
      token = db.get(s, "comma_token")
      public = s.scalar(select(Route.fullname).where(Route.public_since.is_not(None)).limit(1))
      if token and public and not db.get(s, "pause:comma"):
        can_advance = bool(db.get(s, "onboarded_at")) and not db.get(s, "pause:site")
        comma = self.comma_factory(token)
        try:
          self.safety_net(s, comma, can_advance)
        except CommaAuthError:
          s.rollback()
          self.comma_refused(s)
        finally:
          comma.close()
    self.last_net = time.monotonic()

  def tick(self) -> None:
    try:
      self.run_safety_net()
    except Exception:  # its own thread retries in a minute; the pipeline mustn't stop for it
      log.exception("safety net pass failed")
    with self.lock, self.sessionmaker() as s:
      self.housekeeping(s)
      if not db.get(s, "onboarded_at"):
        return
      self.check_updates(s)
      comma_token, access_token = db.get(s, "comma_token"), db.get(s, "access_token")
      if not comma_token or not access_token or db.get(s, "pause:comma"):
        return
      self.warn_token_expiry(s, comma_token)
      comma, site = self.comma_factory(comma_token), self.site_factory(access_token)
      try:
        if db.get(s, "pause:site"):
          return
        self.refresh_scopes(s, site)
        newest = self.discover(s, comma)
        due = s.scalars(select(Route).where(Route.state.not_in(State.FINAL)).order_by(Route.start_time)).all()
        for r in due:
          self.beat()
          d = s.get(Device, r.dongle_id)
          usable = d is not None and d.enabled and not d.paused_reason
          if not usable and r.state == State.PUBLIC:
            if not (r.next_attempt_at and r.next_attempt_at > now_utc()):
              self.stop_orphaned_copy(s, comma, site, r)
            continue
          if not usable and r.state != State.SITE_COPYING:  # that one only asks the site; the device doesn't matter
            self.expire_if_old(s, site, r)  # a device that's off, paused or gone still mustn't keep a route forever
            continue
          if r.next_attempt_at and r.next_attempt_at > now_utc():
            continue
          self.advance(s, comma, site, r, d, newest.get(r.dongle_id))
      except CommaAuthError:
        s.rollback()
        self.comma_refused(s)
      except TokenRejected:
        s.rollback()
        self.pause_site(s, "The site refused your access token. Create a new one under Profile → Access tokens and paste it "
                             "in Setup.")
      except AccountPaused as e:
        s.rollback()
        self.pause_site(s, str(e))
      finally:
        comma.close()
        site.close()
        self.note_deprecation(s, site)

  # ----- global conditions -----

  def comma_refused(self, s: Session) -> None:
    db.put(s, "pause:comma", "comma refused your token. Paste a new one from jwt.comma.ai in Setup.")
    s.commit()
    public = len(s.scalars(select(Route.fullname).where(Route.public_since.is_not(None))).all())
    body = "comma refused your token. Paste a new one in Setup."
    if public:  # without a token they can't be made private from here
      body += f" {public} drive{'s' if public != 1 else ''} may still be public: make {'them' if public != 1 else 'it'} " \
              "private in comma connect."
    notify.event(s, "comma_token", "Polaris Relay paused", body)

  def pause_site(self, s: Session, message: str) -> None:
    db.put(s, "pause:site", message)
    s.commit()
    notify.event(s, "access_token", "Polaris Relay paused", message)

  def refresh_scopes(self, s: Session, site: SiteClient) -> None:
    info = db.get(s, "token_info") or {}
    if "scopes" in info and now_utc().timestamp() - info.get("checked", 0) < 86400:
      return
    try:
      me = site.me()
    except TokenRejected:
      raise
    except (SiteError, ValueError):  # the site is down, busy or garbled: keep the scopes we had, ask again next pass
      return
    token = me.get("token") or {}
    db.put(s, "token_info", {"handle": me.get("handle"), "name": token.get("name"), "scopes": token.get("scopes") or [],
                              "checked": now_utc().timestamp()})
    s.commit()

  def scopes(self, s: Session) -> set[str]:
    return set((db.get(s, "token_info") or {}).get("scopes") or [])

  def note_deprecation(self, s: Session, site: SiteClient) -> None:
    """Record what the site's last answer said about this API version being deprecated, and tell the user once (again
    when the sunset date appears or changes). Nothing stops: the API works until its sunset, and after it the site
    decides."""
    if not site.answered:
      return
    try:
      s.rollback()
      seen = site.deprecation
      if seen is None:
        if db.get(s, "deprecation") is not None:
          db.delete(s, "deprecation")
          s.commit()
        return
      if seen != db.get(s, "deprecation"):
        db.put(s, "deprecation", seen)
        s.commit()
      # A no-op once a service took it; a service that was down is tried again on the next answer.
      notify.event(s, f"api_deprecated:{seen['since']}:{seen.get('sunset')}", "Polaris Relay needs an update soon",
                   deprecation_text(seen, now_utc()), once=True)
    except Exception:
      s.rollback()
      log.exception("couldn't record the API's deprecation")

  def warn_token_expiry(self, s: Session, token: str) -> None:
    exp = token_expiry(token)
    if exp and exp - now_utc() < TOKEN_WARN:
      left = max(0, (exp - now_utc()).days)
      notify.event(s, "comma_token_expiring", "comma token expiring",
                   f"Your comma token expires in {left} day{'s' if left != 1 else ''}. Paste a new one from "
                   "jwt.comma.ai in Setup.")

  # ----- public routes -----

  def cap_s(self, s: Session) -> float:
    return float(db.get(s, "public_cap_h")) * 3600

  @staticmethod
  def public_elapsed(r: Route) -> float:
    return r.public_seconds + ((now_utc() - r.public_since).total_seconds() if r.public_since else 0.0)

  def safety_net(self, s: Session, comma: CommaClient, can_advance: bool) -> None:
    """Make private every public route that's past the cap (marking it so it's never public again), or that this
    pass won't carry on copying: a restart mid-step, a disabled or paused device, a paused worker. One route failing
    doesn't stop the others."""
    cap = self.cap_s(s)
    s.commit()
    for fullname in s.scalars(select(Route.fullname).where(Route.public_since.is_not(None))).all():
      try:
        with self.public_lock:
          s.commit()  # a new snapshot, so the refreshes below see what the pipeline committed while this waited
          r = s.get(Route, fullname)
          if r is None:
            continue
          s.refresh(r)
          if r.public_since is None:
            continue
          d = s.get(Device, r.dongle_id)
          if d is not None:
            s.refresh(d)
          copying = can_advance and r.state in (State.SUBMITTING, State.PUBLIC) and d is not None and d.enabled \
            and not d.paused_reason and r.filter_gen == self.generation()
          if self.public_elapsed(r) > cap:
            r.cap_hit = True
          if r.cap_hit or not copying:
            self.make_private(s, comma, r)
      except CommaAuthError:
        raise
      except Exception as e:  # a route deleted meanwhile, a locked database, comma down: the others still get their turn
        s.rollback()
        log.warning("couldn't make a route private again (%s)", "comma didn't answer" if getattr(e, "transient", False)
                    else type(e).__name__)
        try:
          notify.event(s, "make_private_failed", "Polaris Relay: a drive may still be public",
                       "A drive couldn't be made private again; it's retried every minute. Check it in comma connect.")
        except Exception:
          s.rollback()

  def make_private(self, s: Session, comma: CommaClient, r: Route) -> None:
    with self.public_lock:
      try:
        comma.set_public(r.fullname, False)
      except CommaNotFound:
        pass  # gone from comma: nothing is public
      if r.public_since:
        r.public_seconds += (now_utc() - r.public_since).total_seconds()
      r.public_since = None
      s.commit()

  def make_public(self, s: Session, comma: CommaClient, r: Route) -> None:
    with self.public_lock:
      s.commit()
      s.refresh(r)  # the safety net's thread may have capped it since this session read it
      d = s.get(Device, r.dongle_id)
      if d is not None:
        s.refresh(d)
      if d is None or not d.enabled or d.paused_reason:  # turned off since the pass began: the safety net would undo it
        raise Waiting("The device was turned off or paused, so the route isn't made public.")
      first = r.first_public_at
      if r.cap_hit or self.public_elapsed(r) >= self.cap_s(s) or \
          (first and now_utc() - first > timedelta(days=PUBLIC_MAX_DAYS)):
        raise PublicSpent()
      r.public_since, r.first_public_at = now_utc(), first or now_utc()
      s.commit()  # written before the route is made public, so a crash still makes it private again
      comma.set_public(r.fullname, True)

  # ----- discovery -----

  def discover(self, s: Session, comma: CommaClient) -> dict[str, datetime]:
    """Refresh devices and add new routes of enabled devices (forward-only: started after their enabling). All comma
    calls happen before any write, so the database isn't locked across them."""
    onboarded = datetime.fromtimestamp(db.get(s, "onboarded_at"), now_utc().tzinfo)
    now = now_utc()
    watch = {d.dongle_id: max(onboarded, d.enabled_at or onboarded, now - timedelta(days=DISCOVER_DAYS))
             for d in s.scalars(select(Device).where(Device.enabled.is_(True), Device.paused_reason.is_(None)))}
    s.commit()
    devices = comma.devices()
    found = {}
    for dongle, since in watch.items():
      try:
        found[dongle] = comma.routes(dongle, since - timedelta(hours=1), now)
      except CommaAuthError:
        raise
      except CommaError as e:  # one device's trouble (shared with someone else, say) mustn't stop the others
        log.warning("couldn't list a device's routes (%s)", "comma didn't answer" if e.transient else type(e).__name__)
    for dev in devices:
      if (d := s.get(Device, dev.get("dongle_id", ""))) is not None:
        d.last_athena_ping = int(dev.get("last_athena_ping") or 0)
        d.alias = dev.get("alias") or d.alias
        d.is_owner = dev.get("is_owner", True) is not False
    newest: dict[str, datetime] = {}
    for dongle, metas in found.items():
      for m in metas:
        if m.start_time is None or not m.fullname.lower().startswith(dongle.lower() + "|"):
          continue
        newest[dongle] = max(newest.get(dongle, m.start_time), m.start_time)
        if m.start_time < watch[dongle]:
          continue
        r = s.get(Route, m.fullname)
        if r is None:
          r = Route(fullname=m.fullname, dongle_id=dongle, state=State.WAITING_QLOGS, maxqlog_changed_at=now)
          s.add(r)
        if r.state != State.WAITING_QLOGS:
          continue
        if m.maxqlog != r.maxqlog:
          r.maxqlog_changed_at = now
        r.start_time, r.end_time, r.maxqlog, r.procqlog = m.start_time, m.end_time, m.maxqlog, m.procqlog
        r.segment_numbers = m.segment_numbers
    s.commit()
    return newest

  # ----- one route -----

  def advance(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device,
              newest: datetime | None) -> None:
    try:
      self.run_steps(s, comma, site, r, d, newest)
    except (CommaAuthError, TokenRejected, AccountPaused):
      raise
    except SkipRequested:
      s.rollback()
      self.skip(s, comma, site, r)
    except Exception as e:
      self.route_error(s, comma, site, r, d, e)

  def run_steps(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device,
                newest: datetime | None) -> None:
    steps = {State.WAITING_QLOGS: lambda: self.wait_qlogs(s, comma, r, d, newest),
             State.SUGGESTING: lambda: self.suggesting(s, comma, site, r, d),
             State.REQUESTING: lambda: self.requesting(s, comma, site, r),
             State.WAITING_FILES: lambda: self.waiting_files(s, comma, site, r, d, newest),
             State.TRANSFERRING: lambda: self.transferring(s, comma, site, r),
             State.READY: lambda: self.ready(s, r),
             State.SUBMITTING: lambda: self.submitting(s, comma, site, r, d),
             State.PUBLIC: lambda: self.public_copying(s, comma, site, r),
             State.SITE_COPYING: lambda: self.site_copying(s, site, r)}
    if self.expire_if_old(s, site, r):
      return
    for _ in range(len(steps) + 2):
      if self.skip_wanted(r):
        raise SkipRequested()
      if self.filter_stale(r) and not self.refilter(s, comma, site, r):
        return
      before = r.state
      if r.state not in steps:
        return
      if not steps[r.state]():
        if r.state in State.POLLED:
          r.next_attempt_at = now_utc() + POLL
        r.outages = 0
        s.commit()
        return
      if before in (State.TRANSFERRING, State.SUBMITTING) and r.state in State.ORDER and \
          State.ORDER.index(r.state) < State.ORDER.index(before):
        if not self.regressed(s, r, site):
          return
      else:
        r.attempts, r.error, r.deferred, r.outages = 0, "", False, 0
      r.updated_at = now_utc()
      s.commit()
      if r.state == before:
        return

  def route_error(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device, e: Exception) -> None:
    """Sort a step's failure: waiting (not a failure), the site's or comma's outage (backed off, never given up on),
    something no retry fixes (the route ends), or a failed attempt. Whatever happens here mustn't escape the pass."""
    s.rollback()
    try:
      if isinstance(e, Waiting):
        r.note = str(e)
      elif isinstance(e, RefilterNeeded):
        r.note = "Your privacy zones changed; checking this drive again before anything else is sent."
      elif isinstance(e, PublicSpent):
        why = "This drive was already public as long as it may be, so it isn't made public again"
        if r.submission_id:
          return self.end_public(s, comma, site, r, why)
        return self.finish(s, r, State.FAILED, why + ".", site)
      elif isinstance(e, RateLimited):
        r.deferred = True
        r.next_attempt_at = now_utc() + timedelta(seconds=e.retry_after)
        r.note = "The site is short of space; trying again later." if isinstance(e, StorageFull) else \
          {"ranges": "Waiting for your daily range limit to reset.",
           "bytes": "Waiting for your daily upload allowance to reset."}.get(e.body.get("limit"),
                                                                            "Waiting for the site's limit to reset.")
      elif isinstance(e, DeviceNotVerified):
        if d is not None:
          return self.pause_device(s, d)
        r.note = "The site needs the device verified again."
      elif getattr(e, "deadline", False) and self.too_slow(r, getattr(e, "file", "")):
        return self.failed_attempt(s, r, describe(e), site)
      elif isinstance(e, (SiteError, CommaError, ConnectionError, TimeoutError)) and self.transient(e):
        return self.outage(s, r, "comma" if isinstance(e, CommaError) else "the site" if isinstance(e, SiteError)
                           else "the network")
      elif isinstance(e, SiteError) and e.code in ACCOUNT:
        raise AccountPaused(ACCOUNT[e.code]) from None
      elif isinstance(e, SiteError) and e.code == "route_expired":
        return self.finish(s, r, State.EXPIRED, "Too old to submit now.", site)
      elif isinstance(e, SiteError) and e.code in TERMINAL:
        return self.finish(s, r, State.FAILED, TERMINAL[e.code], site)
      else:
        if not isinstance(e, (SiteError, CommaError, OSError, ValueError, RuntimeError)):
          log.exception("unexpected error in a route step")  # the traceback, not the route
        return self.failed_attempt(s, r, describe(e), site)
      s.commit()
    except (CommaAuthError, TokenRejected, AccountPaused):
      raise
    except Exception as again:
      s.rollback()
      if isinstance(again, (SiteError, CommaError, ConnectionError, TimeoutError)) and self.transient(again):
        self.outage(s, r, "the site" if isinstance(again, SiteError) else "comma")
      else:
        self.failed_attempt(s, r, describe(again))

  @staticmethod
  def too_slow(r: Route, file: str) -> bool:
    """A file that keeps running past its deadline is a link too slow for it, not an outage: after a few, each one uses
    an attempt, so the route ends instead of downloading it again every half hour for a week."""
    r.deadline_hits = r.deadline_hits + 1 if file and file == r.deadline_file else 1
    r.deadline_file = file
    return r.deadline_hits > SLOW_FILE_GRACE

  @staticmethod
  def transient(e: Exception) -> bool:
    return isinstance(e, (ConnectionError, TimeoutError)) or bool(getattr(e, "transient", False))

  def outage(self, s: Session, r: Route, who: str) -> None:
    r.outages += 1
    r.next_attempt_at = now_utc() + timedelta(seconds=OUTAGE_BACKOFF_S[min(r.outages - 1, len(OUTAGE_BACKOFF_S) - 1)])
    r.note = f"Waiting for {who} to answer."
    s.commit()

  def stop_orphaned_copy(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> None:
    """A public copy whose device was turned off, paused or removed: the safety net has made the route private, so
    the copy can't finish. End it now (withdrawing per the setting) rather than leave it open."""
    try:
      self.end_public(s, comma, site, r, "The device was turned off or paused while the site was copying, so the route "
                                         "was made private again")
    except (CommaAuthError, TokenRejected, AccountPaused):
      raise
    except Exception as e:
      self.route_error(s, comma, site, r, None, e)

  def expire_if_old(self, s: Session, site: SiteClient, r: Route) -> bool:
    if r.start_time and now_utc() - r.start_time > ROUTE_MAX_AGE and r.state not in (State.PUBLIC, State.SITE_COPYING):
      self.finish(s, r, State.EXPIRED, "Too old to submit now.", site)
      return True
    return False

  def regressed(self, s: Session, r: Route, site: SiteClient) -> bool:
    """A step went backward (the site found files missing). Counted for the route's life and backed off on, so a
    loop slows down and ends."""
    r.regressions += 1
    if r.regressions >= MAX_REGRESSIONS:
      self.finish(s, r, State.FAILED, "The site kept finding files missing, so this drive was given up.", site)
      return False
    r.next_attempt_at = now_utc() + timedelta(seconds=BACKOFF_S[min(r.regressions, len(BACKOFF_S) - 1)])
    s.commit()
    return False

  def failed_attempt(self, s: Session, r: Route, message: str, site: SiteClient | None = None) -> None:
    r.attempts += 1
    r.error = message[:500]
    if r.attempts >= MAX_ATTEMPTS:
      self.finish(s, r, State.FAILED, f"Gave up after {r.attempts} tries: {r.error}", site)
      notify.event(s, "repeated_failures", "Polaris Relay: a drive failed",
                   "A drive failed after repeated tries. Open the activity list for details.")
      return
    r.next_attempt_at = now_utc() + timedelta(seconds=BACKOFF_S[min(r.attempts - 1, len(BACKOFF_S) - 1)])
    s.commit()
    log.warning("route step failed (attempt %d): %s", r.attempts, r.error)


  def pause_device(self, s: Session, d: Device) -> None:
    d.paused_reason = ("The site needs this device verified again. Enter one of its routes on the site to run the "
                       "device check, then resume it here.")
    s.commit()
    notify.event(s, f"device:{d.dongle_id}", "Polaris Relay paused a device",
                 f"{d.alias or 'A device'} needs verifying on the site again. It's paused until you resume it.")

  def finish(self, s: Session, r: Route, state: str, note: str, site: SiteClient | None = None) -> None:
    r.state, r.note, r.finished_at, r.next_attempt_at = state, note, now_utc(), None
    s.commit()
    shutil.rmtree(self.work_dir(r), ignore_errors=True)
    if site is not None and r.handle and not r.submission_id and state != State.SUBMITTED:
      try:  # nothing was submitted: the site needn't keep this drive's logs
        site.delete_route(r.handle)
      except SiteError:
        pass

  def route_key(self, r: Route) -> str:
    """A name for the route that's safe to put in a folder name, a setting key or a log line: keyed with the data
    volume's secret, so it can't be matched back to a route name without it."""
    return hmac.new(self.config.secret_key().encode(), ROUTE_KEY + r.fullname.encode(), hashlib.sha256).hexdigest()[:20]

  def work_dir(self, r: Route) -> Path:
    return self.config.data_dir / "work" / self.route_key(r)

  def generation(self) -> int:
    """The current `filter_generation`, from a fresh read (a long-lived session would see its own old snapshot)."""
    with self.sessionmaker() as fresh:
      return int(db.get(fresh, "filter_generation") or 0)

  def skip_wanted(self, r: Route) -> bool:
    """Whether the user pressed Skip, from a fresh read: the web page sets it in its own session."""
    with self.sessionmaker() as fresh:
      return bool(fresh.scalar(select(Route.skip_requested).where(Route.fullname == r.fullname)))

  def skip(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> None:
    if r.public_since:
      self.make_private(s, comma, r)
    self.finish(s, r, State.SKIPPED, "You skipped this drive, so nothing was submitted." +
                (" The drive was recorded." if r.drive_recorded else ""), site)

  @staticmethod
  def skippable(r: Route) -> bool:
    """Skip is offered until a submit could have gone out (its key is saved just before sending)."""
    return r.state not in State.FINAL and r.state not in (State.PUBLIC, State.SITE_COPYING) and not r.idempotency_key

  def prepared(self, s: Session, r: Route) -> bool:
    """Everything the submit needs is in place: wait out the submit delay in READY, or submit now when it's 0. A drive
    that comes back here (the site found a file missing) doesn't wait again."""
    r.ready_at = r.ready_at or now_utc()
    r.state = State.READY if self.submit_at(s, r) > now_utc() else State.SUBMITTING
    return True

  def submit_at(self, s: Session, r: Route) -> datetime:
    return r.ready_at + timedelta(minutes=float(db.get(s, "submit_delay_min")))

  def ready(self, s: Session, r: Route) -> bool:
    at = self.submit_at(s, r)  # read each time, so a changed delay applies to drives already waiting
    if now_utc() >= at:
      r.state = State.SUBMITTING
      return True
    r.next_attempt_at = min(at, now_utc() + POLL)
    return False

  def filter_stale(self, r: Route) -> bool:
    return r.state in State.FILTERED and (r.filter_gen is None or r.filter_gen != self.generation())

  def guard(self, r: Route, segment: int) -> None:
    """The second wall around privacy zones, before anything of a segment is sent or requested: never an excluded
    segment, and nothing at all once the zones changed since this route was filtered."""
    if int(segment) in set(r.excluded or []):
      raise ValueError(f"refused to send excluded segment {segment}")
    if r.filter_gen is None or r.filter_gen != self.generation():  # never filtered (or under old zones): fail closed
      raise RefilterNeeded()
    if self.skip_wanted(r):
      raise SkipRequested()

  # ----- filtering -----

  def zone_snapshot(self, s: Session) -> tuple[int, list[privacy.Zone], int]:
    """The generation, zones and surround setting from one read, so a change between them can't be missed."""
    s.commit()
    return int(db.get(s, "filter_generation") or 0), load_zones(s), int(db.get(s, "surround"))

  def fetch_qlogs(self, comma: CommaClient, r: Route, qlogs: dict, segs: list[int]) -> dict[int, tuple[str, Path, str]]:
    out = {}
    for seg in segs:
      name, url = qlogs[seg]
      dest = self.work_dir(r) / "qlogs" / str(seg) / name
      sha, _ = tagged(f"qlog/{seg}", comma.download, url, dest)
      out[seg] = (name, dest, sha)
      self.beat()
    return out

  def excluded_for(self, schema: Path, meta, files: dict[int, tuple[str, Path, str]], zones: list[privacy.Zone],
                   surround: int) -> set[int]:
    scans = read_scans({seg: path for seg, (_, path, _) in files.items()}, schema)
    self.beat()
    return privacy.excluded_segments(sorted(files), lambda seg: scans.get(seg), zones, start_point=meta.start_point,
                                     end_point=meta.end_point, surround=surround)

  def refilter(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> bool:
    """Filter again under the current zones before anything else leaves. Exclusions only grow; the site deletes what
    it has of newly excluded segments, and suggestions touching one are dropped."""
    schema = self.schema_dir(s, site)
    gen, zones, surround = self.zone_snapshot(s)
    meta = comma.route(r.fullname)
    qlogs = comma.route_files(r.fullname).get("qlogs", {})
    segs = sorted(set(range(meta.maxqlog + 1)) | set(r.excluded or []))
    if any(seg not in qlogs for seg in segs):
      raise CommaError("the drive's logs are no longer listed, so it can't be checked against the new zones")
    files = self.fetch_qlogs(comma, r, qlogs, segs)
    try:
      before = set(r.excluded or [])
      excluded = before | self.excluded_for(schema, meta, files, zones, surround)
    finally:
      shutil.rmtree(self.work_dir(r) / "qlogs", ignore_errors=True)
    new = excluded - before
    if r.handle and new:
      sent = sorted(set(segs) - excluded)
      try:
        if sent:
          site.suggest(r.handle, sent, sorted(excluded))
        else:
          site.delete_route(r.handle)
      except SiteError as e:
        if e.code != "already_submitted":
          raise
        r.excluded, r.filter_gen = sorted(excluded), gen
        s.commit()
        if r.transfer == "public":
          # Nothing is copied while the route is private: end the copy rather than make the route public again for the
          # repeat of the lost submit answer. The repeat with the same key only fetches the submission it made.
          if not r.submission_id:
            res = site.submit(r.handle, r.suggestion_ids, "public", r.idempotency_key)
            r.submission_id = str(res.get("submission_id") or "")
            s.commit()
          self.end_public(s, comma, site, r, "Your privacy zones changed before the site copied this drive, so the "
                                             "route was made private again")
          return False
        # Relay: submitted before the zone existed, and what went out can't be taken back. Keep the suggestions so the
        # repeat of the lost submit answer gets the same submission, and say so.
        notify.event(s, f"zoned_after_submit:{self.route_key(r)}", "Polaris Relay: sent before your new zone",
                     "A drive was submitted before you added a privacy zone that covers part of it. That part went to "
                     "the site before the zone existed; you can withdraw the submission on the site.")
        return True
    r.excluded, r.filter_gen = sorted(excluded), gen
    r.relayed = [k for k in (r.relayed or []) if int(k.split("/")[0]) not in excluded]
    r.timings = {**(r.timings or {}), "filtered": now_utc().isoformat()}
    self.set_picks(r, [p for p in (r.picks or []) if not (set(p["segments"]) | set(p["preview_segments"])) & excluded])
    s.commit()
    if not r.picks:
      self.finish(s, r, State.RECORDED if r.drive_recorded else State.SKIPPED,
                  "Your privacy zones now cover every suggested moment, so nothing more was sent.", site)
      return False
    return True

  @staticmethod
  def set_picks(r: Route, picks: list[dict]) -> None:
    r.picks = picks
    r.suggestion_ids = [p["id"] for p in picks]
    r.suggestions, r.needs_input = len(picks), sum(1 for p in picks if p.get("needs_input"))
    r.needs = needs_for(picks)

  # ----- steps: each returns True when the route moved on -----

  def wait_qlogs(self, s: Session, comma: CommaClient, r: Route, d: Device, newest: datetime | None) -> bool:
    now, t = now_utc(), dict(r.timings or {})
    ping_after = timedelta(minutes=float(db.get(s, "ping_after_end_min")))
    conds = {
      "pinged": bool(r.end_time) and d.last_athena_ping >= (r.end_time + ping_after).timestamp(),
      "quiet": r.maxqlog >= 0 and r.procqlog == r.maxqlog and r.maxqlog_changed_at is not None
               and now - r.maxqlog_changed_at >= timedelta(minutes=float(db.get(s, "quiet_min"))),
      "settled": (newest is not None and r.start_time is not None and newest > r.start_time)
                 or (bool(r.end_time) and now - r.end_time >= timedelta(minutes=float(db.get(s, "settle_min")))),
      "contiguous": r.maxqlog >= 0 and list(r.segment_numbers or []) == list(range(r.maxqlog + 1)),
    }
    for name, ok in conds.items():
      if ok and name not in t:
        t[name] = now.isoformat()
    waiting_for = [n for n, ok in conds.items() if not ok]
    if waiting_for:
      r.timings = t
      r.note = {"pinged": "Waiting for the device to come online after the drive.",
                "quiet": "Waiting for the drive's logs to finish uploading.",
                "settled": "Waiting for the drive to settle.",
                "contiguous": "Waiting for missing segments to upload."}[waiting_for[0]]
      return False
    qlogs = comma.route_files(r.fullname).get("qlogs", {})
    if any(seg not in qlogs for seg in range(r.maxqlog + 1)):
      r.timings, r.note = t, "Waiting for missing logs to upload."
      return False
    t["ready"] = now.isoformat()
    r.timings, r.state, r.note = t, State.SUGGESTING, ""
    return True

  def schema_dir(self, s: Session, site: SiteClient) -> Path:
    """The site's schema bundle, cached in the data volume. Without one, nothing is filtered or sent."""
    base = self.config.data_dir / "schema"
    version, checked = db.get(s, "schema_version"), db.get(s, "schema_checked_at") or 0
    if version and (base / version / "log.capnp").exists() and now_utc().timestamp() - checked < SCHEMA_REFRESH.total_seconds():
      return base / version
    try:
      new, bundle = site.schema_bundle()
      if not (base / new / "log.capnp").exists():
        shutil.rmtree(base / new, ignore_errors=True)
        extract_schema(bundle, base / new)
      db.put(s, "schema_version", new)
      db.put(s, "schema_checked_at", now_utc().timestamp())
      s.commit()
      return base / new
    except (TokenRejected, RateLimited):
      raise
    except (SiteError, ValueError, OSError) as e:
      if version and (base / version / "log.capnp").exists():
        return base / version
      raise Waiting("Waiting for the site's log format, needed to read GPS before anything is sent.") from e

  def suggesting(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device) -> bool:
    schema = self.schema_dir(s, site)
    meta = comma.route(r.fullname)
    qlogs = comma.route_files(r.fullname).get("qlogs", {})
    segs = list(range(meta.maxqlog + 1)) if meta.maxqlog >= 0 else []
    if meta.maxqlog != r.maxqlog or not segs or any(seg not in qlogs for seg in segs):
      self.note_growth(r, meta.maxqlog)
      r.maxqlog, r.procqlog, r.maxqlog_changed_at = meta.maxqlog, meta.procqlog, now_utc()
      r.state, r.timings = State.WAITING_QLOGS, {**(r.timings or {}), "regrew": now_utc().isoformat()}
      return True
    gen, zones, surround = self.zone_snapshot(s)
    files = self.fetch_qlogs(comma, r, qlogs, segs)
    excluded = self.excluded_for(schema, meta, files, zones, surround)
    r.excluded, r.filter_gen = sorted(excluded), gen
    r.timings = {**(r.timings or {}), "filtered": now_utc().isoformat()}
    for seg in excluded:
      files.pop(seg)
    s.commit()
    if not files:
      self.finish(s, r, State.SKIPPED, "Every segment was in or near a privacy zone, so nothing was sent.", site)
      return False
    created = site.create_route(r.fullname)
    r.handle, r.submittable, r.link = created["id"], bool(created.get("submittable")), created.get("link") or r.link
    d.verified_at = now_utc()
    s.commit()
    if not r.submittable and not d.record_drives:
      self.finish(s, r, State.SKIPPED, NOT_SUBMITTABLE, site)
      return False
    try:
      for seg, (name, path, sha) in files.items():
        self.guard(r, seg)
        tagged(f"qlog/{seg}", site.put_qlog, r.handle, seg, path, sha, name)
        self.beat()
      res = site.suggest(r.handle, sorted(files), r.excluded)
    except SiteError as e:
      if e.code != "already_submitted":
        raise
      self.finish(s, r, State.SKIPPED, "This drive was already submitted.")
      return False
    finally:
      shutil.rmtree(self.work_dir(r) / "qlogs", ignore_errors=True)
    suggestions = [x for x in res.get("suggestions", []) if not (set(x.get("segments", [])) |
                   set(x.get("preview_segments", []))) & excluded]
    self.set_picks(r, [{"id": x["id"], "segments": [int(v) for v in x.get("segments", [])],
                        "preview_segments": [int(v) for v in x.get("preview_segments", [])],
                        "needs_input": bool(x.get("needs_input"))} for x in suggestions])
    r.link = res.get("link") or r.link
    r.submittable = bool(res.get("submittable", r.submittable))
    s.commit()
    scopes = self.scopes(s)
    if d.record_drives and not r.drive_recorded and "record_drive" in scopes:
      self.record_drive(s, site, r)
    if not r.submittable or not r.suggestion_ids:
      self.finish(s, r, State.RECORDED if r.drive_recorded else State.SKIPPED,
                  "No moments to suggest in this drive." if r.submittable else NOT_SUBMITTABLE, site)
      return False
    if "submit" not in scopes:
      n = r.suggestions
      self.finish(s, r, State.RECORDED if r.drive_recorded else State.SKIPPED,
                  f"{n} moment{'s' if n != 1 else ''} found, but your access token can't submit, so no files were "
                  "requested from the device.", site)
      return False
    r.state, r.note = State.REQUESTING, ""
    return True

  def record_drive(self, s: Session, site: SiteClient, r: Route) -> None:
    try:
      site.record_drive(r.handle)
    except SiteError as e:
      if e.code != "scope_missing":
        raise
      r.note = "Drive not recorded: the access token can't record drives."
      return
    r.drive_recorded = True
    s.commit()

  @staticmethod
  def note_growth(r: Route, maxqlog: int) -> bool:
    if maxqlog > r.maxqlog >= 0 and not r.grew_after_ready:
      r.grew_after_ready = True
      r.timings = {**(r.timings or {}), "grew_seen": now_utc().isoformat()}
      return True
    return False

  def requesting(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> bool:
    if "submit" not in self.scopes(s):
      self.finish(s, r, State.RECORDED if r.drive_recorded else State.SKIPPED, TERMINAL["scope_missing"], site)
      return False
    files = comma.route_files(r.fullname)
    if self.note_growth(r, max(files.get("qlogs", {}), default=-1)):
      s.commit()
    name = r.fullname.split("|", 1)[1]
    paths, wanted, asked = [], 0, {}
    for seg, kind in needed(r):
      self.guard(r, seg)
      if seg not in files.get(kind, {}) and f"{seg}/{kind}" not in (r.upload_unavailable or []):
        wanted += 1
        asked[(seg, kind)] = [f"{name}--{seg}/{f}" for f in KIND_FILES[kind]]
        paths += asked[(seg, kind)]
    result = comma.request_uploads(r.dongle_id, paths) if paths else {}
    failed = set(result.get("failed") or []) if isinstance(result.get("failed"), list) else set()
    # A kind is gone from the device when every file name it could have was refused (it has rlog.zst or rlog.bz2).
    gone = [f"{seg}/{kind}" for (seg, kind), candidates in asked.items() if set(candidates) <= failed]
    r.uploads_requested_at = r.uploads_requested_at or now_utc()
    r.timings = {**(r.timings or {}), "uploads_requested": now_utc().isoformat()}
    if gone:
      r.upload_unavailable = sorted(set(r.upload_unavailable or []) | set(gone))
      lost = {int(k.split("/")[0]) for k in r.upload_unavailable}
      self.set_picks(r, [p for p in r.picks or [] if not (set(p["segments"]) | set(p["preview_segments"])) & lost])
      if not r.picks:
        self.finish(s, r, State.EXPIRED, "The device no longer has the files these moments need, so nothing was "
                                         "submitted.", site)
        return False
      wanted -= len(gone)
    r.note = "The device is offline; it uploads when it's back online." if result.get("queued") else \
      f"Waiting for the device to upload {wanted} file(s)." if wanted else ""
    if gone:
      r.note += f" The device no longer has the files for {len(gone)} of them; the moments needing them were dropped."
    r.state = State.WAITING_FILES
    return True

  def watch_uploads(self, comma: CommaClient, r: Route, d: Device, newest: datetime | None,
                    missing: list[tuple[int, str]]) -> bool:
    """While files are missing: ask again for any that left the device's queue without uploading, after a wait that
    doubles each time (a device restarting for an update needs a few minutes; someone who cancelled in Connect may want
    their own uploads first). A newer route or the device coming back online resets the wait. The queue is read
    before anything about the route changes, so no write is pending across that call."""
    now = now_utc()
    online = now.timestamp() - d.last_athena_ping < ONLINE.total_seconds()
    came_online = online and not r.device_online
    new_route = newest is not None and r.seen_newest is not None and newest > r.seen_newest
    last = datetime.fromisoformat((r.timings or {}).get("uploads_requested", now.isoformat()))
    # A request held by athena while the device was offline is delivered when it reconnects: give it time to land.
    grace_from = max(last, now if came_online else (r.online_since or last))
    retry_due = r.upload_retry_at is not None and not (came_online or new_route)
    queue = comma.upload_queue(r.dongle_id) if online and not retry_due and now - grace_from >= QUEUE_GRACE else None
    r.device_online = online
    if came_online:
      r.online_since = now
    r.seen_newest = max(newest, r.seen_newest) if newest and r.seen_newest else newest or r.seen_newest
    if (came_online or new_route) and r.upload_wait_s:
      r.upload_wait_s = 0
      r.upload_retry_at = now if r.upload_retry_at else None
    if r.upload_retry_at is not None:
      if now >= r.upload_retry_at:
        r.upload_retry_at, r.state = None, State.REQUESTING
        return True
      return False
    if queue is None:
      if online and now - last > RE_REQUEST:  # an offline device gets the last request when it's back
        r.state = State.REQUESTING
        return True
      r.note = (f"Waiting for the device to upload {len(missing)} file(s)." if online else
                "The device is offline; it uploads when it's back online.")
      return False
    name = r.fullname.split("|", 1)[1]
    queued = lambda rel: any(p == rel or p.endswith("/" + rel) for p in queue)
    gone = [(seg, kind) for seg, kind in missing if not any(queued(f"{name}--{seg}/{f}") for f in KIND_FILES[kind])]
    if not gone:
      r.note = f"Waiting for the device to upload {len(missing)} file(s)."
      return False
    wait = min(timedelta(seconds=r.upload_wait_s * 2) if r.upload_wait_s else UPLOAD_WAIT_FIRST, UPLOAD_WAIT_MAX)
    r.upload_wait_s, r.upload_retry_at = int(wait.total_seconds()), now + wait
    r.upload_vanished += 1
    r.note = (f"{len(gone)} file(s) left the device's upload queue without uploading (it restarted, or they were "
              f"cancelled in comma connect); asking again in {spoken(wait)}.")
    return False

  def waiting_files(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device,
                    newest: datetime | None = None) -> bool:
    files = comma.route_files(r.fullname)
    if self.note_growth(r, max(files.get("qlogs", {}), default=-1)):
      s.commit()
    unavailable = set(r.upload_unavailable or [])
    missing = [(seg, kind) for seg, kind in needed(r)
               if seg not in files.get(kind, {}) and f"{seg}/{kind}" not in unavailable]
    now = now_utc()
    if not missing:
      r.upload_wait_s, r.upload_retry_at = 0, None
    if missing:
      if r.uploads_requested_at and now - r.uploads_requested_at > timedelta(days=float(db.get(s, "files_wait_days"))):
        self.finish(s, r, State.EXPIRED, "The device didn't upload the needed files in time.", site)
        return False
      return self.watch_uploads(comma, r, d, newest, missing)
    r.files_ready_at = now
    if d.mode == "suggest":
      n = r.suggestions
      self.finish(s, r, State.SUGGESTED, f"{n} suggestion{'s' if n != 1 else ''} ready. To submit them, make the route "
                  "public in comma connect and open it on the site. Making it public shares every segment, privacy-zone "
                  "segments included, and the site's picker doesn't know about your zones.", site)
      return False
    r.transfer = db.get(s, "transfer")
    if r.transfer == "public" and not d.is_owner:
      r.transfer = "relay"
      r.note = "Only the device's owner can make its routes public, so this drive is relayed."
    if r.transfer == "relay":
      r.state = State.TRANSFERRING
      return True
    return self.prepared(s, r)

  def transferring(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> bool:
    files, listed = comma.route_files(r.fullname), time.monotonic()
    for seg, kind in needed(r):
      key = f"{seg}/{kind}"
      if key in (r.relayed or []):
        continue
      self.guard(r, seg)
      if time.monotonic() - listed > LISTING_MAX_AGE:
        files, listed = comma.route_files(r.fullname), time.monotonic()
      if seg not in files.get(kind, {}):
        r.state = State.WAITING_FILES
        return True
      dest = self.work_dir(r) / "files" / str(seg) / files[kind][seg][0]
      try:
        try:
          sha, _ = tagged(key, comma.download, files[kind][seg][1], dest)
        except LinkExpired:
          files, listed = comma.route_files(r.fullname), time.monotonic()
          if seg not in files.get(kind, {}):
            r.state = State.WAITING_FILES
            return True
          sha, _ = tagged(key, comma.download, files[kind][seg][1], dest)
        self.beat()
        self.guard(r, seg)
        tagged(key, site.put_file, r.handle, seg, kind, dest, sha, files[kind][seg][0])
      except SiteError as e:
        if e.code == "already_submitted":  # a submit went through and its answer was lost: repeat it
          r.state = State.SUBMITTING
          return True
        if e.code not in ("segment_not_needed", "kind_not_allowed"):
          raise
      finally:
        dest.unlink(missing_ok=True)
      r.relayed = [*(r.relayed or []), key]
      r.note = f"Sent {len(r.relayed)} of {len(needed(r))} files."
      s.commit()
      self.beat()
    r.note = ""
    return self.prepared(s, r)

  def submitting(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, d: Device) -> bool:
    if not r.idempotency_key:
      r.idempotency_key = secrets.token_urlsafe(24)
      s.commit()  # saved before sending, so a retry after a lost answer repeats the same submit
    public = r.transfer == "public"
    if public and r.public_since is None:
      self.make_public(s, comma, r)
    try:
      res = site.submit(r.handle, r.suggestion_ids, r.transfer or "relay", r.idempotency_key)
    except SiteError as e:
      if public and not isinstance(e, (TokenRejected, RateLimited)):
        self.make_private(s, comma, r)
      if e.code == "files_missing":
        if public:
          r.state = State.WAITING_FILES
        else:
          gone = {f"{m.get('segment')}/{m.get('kind')}" for m in e.body.get("missing", [])}
          r.relayed = [k for k in (r.relayed or []) if k not in gone] if gone else []
          r.state = State.TRANSFERRING
        return True
      if e.code == "route_not_submittable":
        self.finish(s, r, State.SKIPPED, NOT_SUBMITTABLE, site)
        return False
      if isinstance(e, RateLimited) and public:
        self.make_private(s, comma, r)
      raise
    r.submission_id = str(res.get("submission_id") or "")
    r.submitted = int(res.get("ranges") or len(r.suggestion_ids))
    d.submitted_before = True
    if public:
      r.state, r.note = State.PUBLIC, "The site is copying the files; the route is public until it finishes."
    else:
      r.state, r.note = State.SITE_COPYING, "Submitted; the site is checking the files."
    r.timings = {**(r.timings or {}), "submitted": now_utc().isoformat()}
    return True

  def site_copying(self, s: Session, site: SiteClient, r: Route) -> bool:
    """Relay mode, after submitting: wait for the site to move the files into review, so a copy that fails there is
    told to the user rather than shown as done. Polled cheaply, and given up on after the files window."""
    timings = dict(r.timings or {})
    submitted = datetime.fromisoformat(timings.get("submitted", now_utc().isoformat()))
    if now_utc() - submitted > timedelta(days=float(db.get(s, "files_wait_days"))):
      self.finish(s, r, State.SUBMITTED, submitted_note(r) + " The site was still checking the files when this stopped "
                                                             "watching.")
      return False
    try:
      sub = site.submission(r.submission_id)
    except SiteError as e:
      if e.status != 404:
        raise
      self.finish(s, r, State.SUBMITTED, submitted_note(r) + " The submission is no longer on the site.")
      return False
    status = sub.get("status", "")
    if status and status != "submitted":
      note = submitted_note(r) if status not in ("withdrawn", "rejected", "expired") else f"Submission {status}."
      self.finish(s, r, State.SUBMITTED, note)
      return False
    if sub.get("copy_error"):
      since = datetime.fromisoformat(timings.setdefault("copy_error_since", now_utc().isoformat()))
      r.timings = timings
      if now_utc() - since >= COPY_ERROR_GRACE:
        self.finish(s, r, State.SUBMITTED, submitted_note(r) + " The site had trouble with the files; check the "
                                                               "submission on the site.")
        notify.event(s, f"copy_error:{self.route_key(r)}", "Polaris Relay: the site couldn't take a drive's files",
                     f"A submitted drive's files didn't make it into review. See your submissions: "
                     f"{self.config.site_url('/status')}")
        return False
    elif timings.pop("copy_error_since", None):
      r.timings = timings
    return False

  def public_copying(self, s: Session, comma: CommaClient, site: SiteClient, r: Route) -> bool:
    if r.cap_hit or self.public_elapsed(r) > self.cap_s(s):
      self.end_public(s, comma, site, r, "The site didn't finish copying in time, so the route was made private again")
      return False
    if r.filter_gen is not None and r.filter_gen != self.generation():
      # The site may be copying a segment a new zone covers: stop it rather than finish the copy.
      self.end_public(s, comma, site, r, "Your privacy zones changed while the site was copying, so the route was made "
                                         "private again")
      return False
    try:
      sub = site.submission(r.submission_id)  # before anything is made public again: it may be done or closed
    except SiteError as e:
      if e.status != 404:
        raise
      if r.public_since:
        self.make_private(s, comma, r)
      self.finish(s, r, State.SUBMITTED, submitted_note(r) + " The submission is no longer on the site.")
      return False
    status = sub.get("status", "")
    if status == "submitted" and r.public_since is None:  # made private while paused; the copy needs it public
      self.make_public(s, comma, r)
    timings = dict(r.timings or {})
    if sub.get("copy_error") and status == "submitted":
      # The site retries a passing comma error by itself; only one that lasts means the copy won't finish.
      since = datetime.fromisoformat(timings.setdefault("copy_error_since", now_utc().isoformat()))
      r.timings = timings
      s.commit()
      if now_utc() - since >= COPY_ERROR_GRACE:
        self.end_public(s, comma, site, r, "The site couldn't copy the files, so the route was made private again")
        return False
    elif timings.pop("copy_error_since", None):
      r.timings = timings
      s.commit()
    if status and status != "submitted":
      self.make_private(s, comma, r)
      note = submitted_note(r) if status not in ("withdrawn", "rejected", "expired") else f"Submission {status}."
      self.finish(s, r, State.SUBMITTED, note)
    return False

  def end_public(self, s: Session, comma: CommaClient, site: SiteClient, r: Route, why: str) -> None:
    r.cap_hit = True  # never public again, even if the withdraw below fails and this runs once more
    s.commit()
    if r.public_since:
      self.make_private(s, comma, r)
    withdraw = bool(db.get(s, "auto_withdraw")) and bool(r.submission_id)
    if withdraw:
      try:
        site.withdraw(r.submission_id)
      except SiteError as e:
        started = datetime.fromisoformat((r.timings or {}).get("submitted") or (r.first_public_at or now_utc()).isoformat())
        if e.code == "not_pending":
          pass
        elif now_utc() - started > timedelta(days=float(db.get(s, "files_wait_days"))):
          why += ", but the site didn't answer to withdraw the submission; withdraw it on the site"  # already private
          withdraw = False
        else:
          raise
    self.finish(s, r, State.FAILED, why + (" and the submission withdrawn." if withdraw else "."))
    notify.event(s, "repeated_failures", "Polaris Relay: a drive failed",
                 "A public-route copy didn't finish. Open the activity list for details.")

  # ----- housekeeping -----

  def housekeeping(self, s: Session) -> None:
    cutoff = now_utc() - timedelta(days=ACTIVITY_DAYS)
    for r in s.scalars(select(Route).where(Route.finished_at.is_not(None), Route.finished_at < cutoff)).all():
      for event in ROUTE_EVENTS:
        notify.clear(s, f"{event}:{self.route_key(r)}")
      s.delete(r)
    s.commit()
    work = self.config.data_dir / "work"
    if work.exists():
      live = {self.work_dir(r).name for r in s.scalars(select(Route).where(Route.state.not_in(State.FINAL)))}
      for p in work.iterdir():
        if p.name not in live:
          shutil.rmtree(p, ignore_errors=True)
    self.digest(s)

  def check_updates(self, s: Session) -> None:
    if not db.get(s, "update_check"):
      return
    last = db.get(s, "update_checked_at") or 0
    if now_utc().timestamp() - last < UPDATE_EVERY.total_seconds():
      return
    db.put(s, "update_checked_at", now_utc().timestamp())
    s.commit()
    latest = updates.latest_tag(self.config.registry, self.config.image)
    if latest:
      db.put(s, "latest_version", latest)
      s.commit()

  def digest(self, s: Session) -> None:
    if not db.get(s, "digest"):
      return
    last = db.get(s, "digest_at") or 0
    now = now_utc()
    if now.timestamp() - last < 86400:
      return
    db.put(s, "digest_at", now.timestamp())
    s.commit()
    if not last:
      return
    since = datetime.fromtimestamp(last, now.tzinfo)
    done = s.scalars(select(Route).where(Route.finished_at >= since)).all()
    counts: dict[str, int] = {}
    for r in done:
      counts[State.LABELS[r.state]] = counts.get(State.LABELS[r.state], 0) + 1
    body = ", ".join(f"{v} {k.lower()}" for k, v in counts.items()) or "No drives finished."
    notify.send(s, "Polaris Relay daily summary", body)


def filter_changed(s: Session) -> None:
  """Call in the same transaction as any change to zones or the surround setting: routes filtered under an older
  generation stop sending until they're filtered again."""
  # Take SQLite's write lock before reading, so two saves at once can't both write the same generation.
  s.execute(text("UPDATE settings SET value = value WHERE key = 'filter_generation'"))
  db.put(s, "filter_generation", int(db.get(s, "filter_generation") or 0) + 1)


def needs_for(suggestions: list[dict]) -> dict[str, list[str]]:
  """Files the chosen suggestions need: full-res for every kept segment, preview for the seconds each selects."""
  out: dict[int, set[str]] = {}
  for x in suggestions:
    for seg in x.get("segments", []):
      out.setdefault(int(seg), set()).update(("logs", "cameras", "ecameras"))
    for seg in x.get("preview_segments", []):
      out.setdefault(int(seg), set()).add("qcameras")
  return {str(k): sorted(v, key=FILE_KINDS.index) for k, v in sorted(out.items())}


def needed(r: Route) -> list[tuple[int, str]]:
  return [(int(seg), kind) for seg, kinds in sorted((r.needs or {}).items(), key=lambda kv: int(kv[0]))
          for kind in kinds if kind in FILE_KINDS and kind not in NEVER_KINDS]


def submitted_note(r: Route) -> str:
  note = f"Submitted {r.submitted} range{'s' if r.submitted != 1 else ''}."
  if r.needs_input:
    note += f" {r.needs_input} need a reviewer's input."
  return note


def load_zones(s: Session) -> list[privacy.Zone]:
  zones: list[privacy.Zone] = []
  for z in s.scalars(select(Zone)):
    if z.kind == "circle" and z.lat is not None and z.lng is not None and z.radius_m:
      zones.append(privacy.Circle(z.lat, z.lng, z.radius_m))
    elif z.kind == "polygon" and z.points and len(z.points) >= 3:
      zones.append(privacy.Polygon(tuple((float(a), float(b)) for a, b in z.points)))
  return zones


def version_banner(s: Session) -> str | None:
  latest = db.get(s, "latest_version")
  return latest if db.get(s, "update_check") and updates.newer_than(latest, __version__) else None


def tagged(file: str, call, *args):
  """Run a file transfer, naming the file on a deadline error so repeats on the same file can be told apart."""
  try:
    return call(*args)
  except (CommaError, SiteError) as e:
    if getattr(e, "deadline", False):
      e.file = file
    raise


def describe(e: Exception) -> str:
  """What a failure says in the activity list and the log. Local file errors and unexpected ones give only their type:
  their text can carry paths and data from comma or the site."""
  if isinstance(e, (SiteError, CommaError)):
    return str(e) or type(e).__name__
  if isinstance(e, OSError):
    return f"a file error on this machine ({type(e).__name__})"
  if isinstance(e, (ValueError, RuntimeError)) and str(e).startswith(("refused to send", "couldn't read", "reading GPS")):
    return str(e)
  return f"an unexpected answer ({type(e).__name__})"


def spoken(wait: timedelta) -> str:
  minutes = int(wait.total_seconds() // 60)
  if minutes < 60:
    return f"{minutes} minute{'s' if minutes != 1 else ''}"
  hours = round(minutes / 60, 1)
  return f"{hours:g} hour{'s' if hours != 1 else ''}"


def deprecation_text(seen: dict, now: datetime) -> str:
  """The banner and notification: since when the API version is deprecated and, once known, when it stops."""
  since = datetime.fromtimestamp(seen["since"], now.tzinfo).strftime("%Y-%m-%d")
  text = f"The site's API version Polaris Relay uses is deprecated since {since}"
  if seen.get("sunset"):
    sunset = datetime.fromtimestamp(seen["sunset"], now.tzinfo)
    text += (f", and stopped working on {sunset:%Y-%m-%d} (it's still tried; the site decides)"
             if sunset <= now else f", and stops working on {sunset:%Y-%m-%d}")
  return text + ". Update Polaris Relay."
