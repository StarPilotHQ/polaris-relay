"""The daemon's SQLite database: settings, devices, privacy zones, notification targets and one row per route.

Metadata only: route names, states, times, counts, errors and links. No GPS tracks; zones are the user's settings.
"""
import json
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import JSON, Boolean, Float, Integer, String, Text, create_engine, event, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.types import DateTime, TypeDecorator


def now_utc() -> datetime:
  return datetime.now(UTC)


class UTCDateTime(TypeDecorator):
  impl = DateTime
  cache_ok = True

  def process_bind_param(self, value, dialect):
    return value.astimezone(UTC).replace(tzinfo=None) if value is not None else None

  def process_result_value(self, value, dialect):
    return value.replace(tzinfo=UTC) if value is not None else None


class Base(DeclarativeBase):
  type_annotation_map = {datetime: UTCDateTime}


class Setting(Base):
  __tablename__ = "settings"
  key: Mapped[str] = mapped_column(String(64), primary_key=True)
  value: Mapped[str] = mapped_column(Text)


class Device(Base):
  __tablename__ = "devices"
  dongle_id: Mapped[str] = mapped_column(String(16), primary_key=True)
  alias: Mapped[str] = mapped_column(String(128), default="")
  enabled: Mapped[bool] = mapped_column(Boolean, default=False)
  mode: Mapped[str] = mapped_column(String(16), default="auto")  # auto (auto-submit) | suggest (suggest-only)
  record_drives: Mapped[bool] = mapped_column(Boolean, default=True)
  last_athena_ping: Mapped[int] = mapped_column(Integer, default=0)
  verified_at: Mapped[datetime | None]
  # Set when the site stops treating the device as verified; the device does nothing until enabled again.
  paused_reason: Mapped[str | None] = mapped_column(Text)
  # It submitted through the site before, so a later "not verified" most likely means a revoke or a lapse.
  submitted_before: Mapped[bool] = mapped_column(Boolean, default=False)
  enabled_at: Mapped[datetime | None]


class Zone(Base):
  __tablename__ = "zones"
  id: Mapped[int] = mapped_column(primary_key=True)
  name: Mapped[str] = mapped_column(String(64), default="")
  kind: Mapped[str] = mapped_column(String(16))  # circle | polygon
  lat: Mapped[float | None] = mapped_column(Float)
  lng: Mapped[float | None] = mapped_column(Float)
  radius_m: Mapped[float | None] = mapped_column(Float)
  points: Mapped[list | None] = mapped_column(JSON)  # [[lat, lng], ...] for polygons
  created_at: Mapped[datetime] = mapped_column(default=now_utc)


class Notifier(Base):
  __tablename__ = "notifiers"
  id: Mapped[int] = mapped_column(primary_key=True)
  service: Mapped[str] = mapped_column(String(16))  # discord | ntfy | telegram | email | apprise
  name: Mapped[str] = mapped_column(String(64), default="")
  fields: Mapped[dict] = mapped_column(JSON, default=dict)
  enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class Route(Base):
  __tablename__ = "routes"
  fullname: Mapped[str] = mapped_column(String(64), primary_key=True)  # <dongle>|<name>
  dongle_id: Mapped[str] = mapped_column(String(16), index=True)
  state: Mapped[str] = mapped_column(String(24), index=True)
  start_time: Mapped[datetime | None]
  end_time: Mapped[datetime | None]
  discovered_at: Mapped[datetime] = mapped_column(default=now_utc)
  updated_at: Mapped[datetime] = mapped_column(default=now_utc)
  finished_at: Mapped[datetime | None]
  # What comma's route list says, refreshed while waiting for qlogs.
  maxqlog: Mapped[int] = mapped_column(Integer, default=-1)
  procqlog: Mapped[int] = mapped_column(Integer, default=-1)
  maxqlog_changed_at: Mapped[datetime | None]
  segment_numbers: Mapped[list] = mapped_column(JSON, default=list)
  # When each qlog-completeness condition first held, for tuning the thresholds on real drives.
  timings: Mapped[dict] = mapped_column(JSON, default=dict)
  grew_after_ready: Mapped[bool] = mapped_column(Boolean, default=False)
  excluded: Mapped[list] = mapped_column(JSON, default=list)
  # The `filter_generation` setting the exclusions were worked out under; a newer one stops everything until refiltered.
  filter_gen: Mapped[int | None] = mapped_column(Integer)
  handle: Mapped[str | None] = mapped_column(String(128))
  submittable: Mapped[bool | None] = mapped_column(Boolean)
  link: Mapped[str | None] = mapped_column(Text)
  suggestion_ids: Mapped[list] = mapped_column(JSON, default=list)
  # Per suggestion: {"id", "segments", "preview_segments", "needs_input"}, to drop the ones a later zone covers.
  picks: Mapped[list] = mapped_column(JSON, default=list)
  suggestions: Mapped[int] = mapped_column(Integer, default=0)
  needs_input: Mapped[int] = mapped_column(Integer, default=0)
  submitted: Mapped[int] = mapped_column(Integer, default=0)
  needs: Mapped[dict] = mapped_column(JSON, default=dict)  # {"<seg>": [kinds]} from suggest
  relayed: Mapped[list] = mapped_column(JSON, default=list)  # ["<seg>/<kind>", ...] uploaded to the site
  uploads_requested_at: Mapped[datetime | None]
  # Files that left the device's upload queue without uploading (a restart, a cancel in Connect) are asked for again
  # after a wait that doubles each time; a newer route or the device coming back online resets it.
  upload_wait_s: Mapped[int] = mapped_column(Integer, default=0)
  upload_retry_at: Mapped[datetime | None]
  upload_vanished: Mapped[int] = mapped_column(Integer, default=0)  # how many times files left the queue unuploaded
  # "<seg>/<kind>" the device said it doesn't have (athena's `failed`): never asked for again.
  upload_unavailable: Mapped[list] = mapped_column(JSON, default=list)
  device_online: Mapped[bool] = mapped_column(Boolean, default=False)
  online_since: Mapped[datetime | None]  # when the device was last seen coming online
  seen_newest: Mapped[datetime | None]
  files_ready_at: Mapped[datetime | None]
  ready_at: Mapped[datetime | None]  # first ready to submit: the submit delay counts from here
  skip_requested: Mapped[bool] = mapped_column(Boolean, default=False)  # set by the web page, acted on by the worker
  idempotency_key: Mapped[str | None] = mapped_column(String(64))
  submission_id: Mapped[str | None] = mapped_column(String(64))
  transfer: Mapped[str | None] = mapped_column(String(16))
  public_since: Mapped[datetime | None]  # written before the route is made public, so a restart makes it private
  public_seconds: Mapped[float] = mapped_column(Float, default=0.0)  # total time public, over every stretch
  first_public_at: Mapped[datetime | None]
  cap_hit: Mapped[bool] = mapped_column(Boolean, default=False)  # made private for the cap; never public again
  drive_recorded: Mapped[bool] = mapped_column(Boolean, default=False)
  attempts: Mapped[int] = mapped_column(Integer, default=0)
  # Steps that went backward (files the site found missing): not reset by moving on, so a loop backs off and ends.
  regressions: Mapped[int] = mapped_column(Integer, default=0)
  # Passes the site or comma didn't answer in a row: backed off on, never a reason to give up.
  outages: Mapped[int] = mapped_column(Integer, default=0)
  # The file that last ran past its deadline, and how many times in a row: a link too slow for it ends the route.
  deadline_file: Mapped[str] = mapped_column(String(32), default="")
  deadline_hits: Mapped[int] = mapped_column(Integer, default=0)
  next_attempt_at: Mapped[datetime | None]
  deferred: Mapped[bool] = mapped_column(Boolean, default=False)  # waiting on the site's rate limit
  note: Mapped[str] = mapped_column(Text, default="")
  error: Mapped[str] = mapped_column(Text, default="")


ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = ()
REMOVED_COLUMNS: tuple[tuple[str, str], ...] = (("devices", "is_owner"),)
# Run once per database, by name. 0.1.0 to 0.1.2 sent files under names the site refuses and counted them as sent, so no
# relayed file ever arrived: open drives send them again, and drives failed by that are started over (a failure deletes
# them on the site). Their backoff and regressions came from the bug, so both start clean.
REPAIRS: dict[str, tuple[str, ...]] = {
  "relayed_file_names": (
    "UPDATE routes SET state = 'transferring', relayed = '[]', regressions = 0, attempts = 0, next_attempt_at = NULL, "
    "note = '', error = '' WHERE transfer = 'relay' AND submission_id IS NULL "
    "AND state IN ('transferring', 'ready', 'submitting')",
    "UPDATE routes SET state = 'suggesting', finished_at = NULL, handle = NULL, relayed = '[]', ready_at = NULL, "
    "idempotency_key = NULL, regressions = 0, attempts = 0, next_attempt_at = NULL, note = '', error = '' "
    "WHERE transfer = 'relay' AND submission_id IS NULL AND state = 'failed' "
    "AND note LIKE 'The site kept finding files missing%'",
  ),
}


def make_sessionmaker(data_dir: Path):
  data_dir.mkdir(parents=True, exist_ok=True)
  path = data_dir / "relay.db"
  engine = create_engine(f"sqlite:///{path}", hide_parameters=True)

  @event.listens_for(engine, "connect")
  def _pragmas(conn, _):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
  Base.metadata.create_all(engine)
  with engine.begin() as conn:
    for table, column, ddl in ADDED_COLUMNS:
      if column not in {c["name"] for c in inspect(conn).get_columns(table)}:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
    for table, column in REMOVED_COLUMNS:
      if column in {c["name"] for c in inspect(conn).get_columns(table)}:
        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {column}"))
    for name, statements in REPAIRS.items():
      key = {"key": f"repair:{name}"}
      if conn.execute(text("SELECT 1 FROM settings WHERE key = :key"), key).first() is None:
        for statement in statements:
          conn.execute(text(statement))
        conn.execute(text("INSERT INTO settings (key, value) VALUES (:key, 'true')"), key)
  for p in (path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
    if p.exists():
      p.chmod(0o600)
  return sessionmaker(engine, expire_on_commit=False)


DEFAULTS = {
  "cloud": "comma",
  "transfer": "relay",
  "surround": 0,
  "tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
  "update_check": True,
  "upload_cellular": False,  # let the device upload requested files over LTE, not only Wi-Fi
  "filter_generation": 0,
  "digest": False,
  # Advanced: qlog completeness (minutes) and the public-route safety net.
  "ping_after_end_min": 3,
  "quiet_min": 10,
  "settle_min": 30,
  "files_wait_days": 3,
  "public_cap_h": 6,
  "submit_delay_min": 30,
  "auto_withdraw": True,
}


def get(db: Session, key: str, default=None):
  row = db.get(Setting, key)
  if row is None:
    return DEFAULTS.get(key, default)
  return json.loads(row.value)


def put(db: Session, key: str, value) -> None:
  row = db.get(Setting, key)
  if row is None:
    db.add(Setting(key=key, value=json.dumps(value)))
  else:
    row.value = json.dumps(value)


def delete(db: Session, key: str) -> None:
  row = db.get(Setting, key)
  if row is not None:
    db.delete(row)
