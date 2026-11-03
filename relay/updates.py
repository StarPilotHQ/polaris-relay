"""Daily check for a newer image on GHCR (anonymous). Opt-out; auto-updating is left to the user (e.g. Watchtower)."""
import re

import httpx

from relay import __version__

SEMVER = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


def parse(v: str) -> tuple[int, int, int] | None:
  m = SEMVER.match(v.strip())
  return (int(m[1]), int(m[2]), int(m[3])) if m else None


def newer_than(latest: str | None, current: str = __version__) -> bool:
  a, b = parse(latest or ""), parse(current)
  return a is not None and b is not None and a > b


def latest_tag(registry: str, image: str, transport: httpx.BaseTransport | None = None) -> str | None:
  """Highest semver tag of the image; None when the registry can't be read."""
  with httpx.Client(transport=transport, timeout=20.0, headers={"User-Agent": f"polaris-relay/{__version__}"}) as c:
    try:
      tok = c.get(f"{registry}/token", params={"scope": f"repository:{image}:pull"}).json().get("token")
      r = c.get(f"{registry}/v2/{image}/tags/list", params={"n": 1000}, headers={"Authorization": f"Bearer {tok}"})
      tags = (r.json().get("tags") or []) if r.status_code == 200 else []
    except (httpx.HTTPError, ValueError):
      return None
  versions = [t for t in tags if parse(t)]
  return max(versions, key=parse) if versions else None
