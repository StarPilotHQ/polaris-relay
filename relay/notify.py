"""Notifications: friendly per-service forms that become Apprise URLs. Nothing is sent per route."""
import logging
import threading
import time
from datetime import timedelta
from urllib.parse import quote, urlsplit

import apprise
from sqlalchemy import select
from sqlalchemy.orm import Session

from relay import db
from relay.db import Notifier, now_utc

log = logging.getLogger("relay")
logging.getLogger("apprise").setLevel(logging.WARNING)

SERVICES = {
  "discord": {"label": "Discord webhook", "fields": [("webhook", "Webhook URL", "https://discord.com/api/webhooks/...")]},
  "ntfy": {"label": "ntfy", "fields": [("server", "Server", "https://ntfy.sh"), ("topic", "Topic", "my-relay")]},
  "telegram": {"label": "Telegram", "fields": [("bot_token", "Bot token", "123456:ABC..."), ("chat_id", "Chat ID", "")]},
  "email": {"label": "Email", "fields": [("host", "SMTP server", "smtp.example.com"), ("port", "Port", "587"),
                                         ("user", "User", ""), ("password", "Password", ""),
                                         ("to", "Send to", "you@example.com")]},
  "apprise": {"label": "Other (Apprise URL)", "fields": [("url", "Apprise URL", "pover://user@token")]},
}
SECRET_FIELDS = {"webhook", "bot_token", "password", "url"}
# Events notified at most once per this interval each, so a stuck condition doesn't repeat every minute.
REPEAT = timedelta(hours=24)
# Each target is sent from its own thread, waited on together for at most this long: Apprise has no overall timeout,
# and one hung service mustn't hold up the others or the pipeline. A thread still hanging is left behind (daemon).
SEND_TIMEOUT_S = 20.0


def apprise_url(service: str, f: dict) -> str:
  if service == "discord":
    parts = urlsplit(f.get("webhook", ""))
    bits = [p for p in parts.path.split("/") if p]
    if parts.hostname not in ("discord.com", "discordapp.com", "ptb.discord.com", "canary.discord.com") or \
        len(bits) < 4 or bits[:2] != ["api", "webhooks"]:
      raise ValueError("That doesn't look like a Discord webhook URL.")
    return f"discord://{bits[2]}/{bits[3]}"
  if service == "ntfy":
    server = urlsplit(f.get("server") or "https://ntfy.sh")
    topic = (f.get("topic") or "").strip()
    if not topic:
      raise ValueError("Give the ntfy topic.")
    scheme = "ntfys" if server.scheme == "https" else "ntfy"
    return f"{scheme}://{server.netloc}/{quote(topic)}"
  if service == "telegram":
    if not f.get("bot_token") or not f.get("chat_id"):
      raise ValueError("Give the bot token and the chat ID.")
    return f"tgram://{f['bot_token']}/{f['chat_id']}"
  if service == "email":
    if not f.get("host") or not f.get("to"):
      raise ValueError("Give the SMTP server and the address to send to.")
    auth = f"{quote(f.get('user', ''))}:{quote(f.get('password', ''))}@" if f.get("user") else ""
    port = f.get("port") or "587"
    return f"mailtos://{auth}{f['host']}:{port}?to={quote(f['to'])}"
  if service == "apprise":
    if "://" not in (f.get("url") or ""):
      raise ValueError("Give an Apprise URL.")
    return f["url"]
  raise ValueError("Unknown service.")


def send_to(notifier: Notifier, title: str, body: str) -> bool:
  a = apprise.Apprise()
  try:
    if not a.add(apprise_url(notifier.service, notifier.fields)):
      return False
  except ValueError:
    return False
  return bool(a.notify(title=title, body=body))


def send(session: Session, title: str, body: str) -> int | None:
  """Send to every enabled target at once; returns how many took it, or None when there are no targets."""
  targets = [(n.service, dict(n.fields or {})) for n in session.scalars(select(Notifier).where(Notifier.enabled.is_(True)))]
  if not targets:
    return None
  done: list[bool] = []

  def one(service: str, fields: dict) -> None:
    try:
      if send_to(Notifier(service=service, fields=fields), title, body):
        done.append(True)
    except Exception:  # a broken target mustn't stop the others
      log.warning("a %s notification failed", service)
  threads = [threading.Thread(target=one, args=t, name="relay-notify", daemon=True) for t in targets]
  for t in threads:
    t.start()
  until = time.monotonic() + SEND_TIMEOUT_S
  for t in threads:
    t.join(max(0.0, until - time.monotonic()))
  if any(t.is_alive() for t in threads):
    log.warning("a notification service didn't answer in %d s", SEND_TIMEOUT_S)
  return len(done)


def event(session: Session, key: str, title: str, body: str, once: bool = False) -> bool:
  """Notify about `key` at most once a day (or, with `once`, ever). The key is only set once a target took the
  message, so a service that's down is tried again next time; with no targets there's nothing to retry."""
  last = db.get(session, f"notified:{key}")
  now = now_utc()
  if last and (once or now.timestamp() - float(last) < REPEAT.total_seconds()):
    return False
  session.commit()  # nothing pending (no write lock held) while the services take their time
  sent = send(session, title, body)
  if sent is None or sent > 0:
    db.put(session, f"notified:{key}", now.timestamp())
    session.commit()
  return bool(sent)


def clear(session: Session, key: str) -> None:
  db.delete(session, f"notified:{key}")
