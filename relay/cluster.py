"""Proposed privacy zones: places the account's recent drives often start or end, from comma's route list only."""
from dataclasses import dataclass

from relay.privacy import distance_m

JOIN_M = 250.0
MIN_RADIUS_M = 200.0
MARGIN_M = 100.0


@dataclass
class Proposal:
  lat: float
  lng: float
  radius_m: float
  visits: int


def propose(points: list[tuple[float, float]], min_visits: int = 2, limit: int = 8) -> list[Proposal]:
  clusters: list[list[tuple[float, float]]] = []
  for p in points:
    best = None
    for c in clusters:
      clat, clng = sum(q[0] for q in c) / len(c), sum(q[1] for q in c) / len(c)
      d = distance_m(p[0], p[1], clat, clng)
      if d <= JOIN_M and (best is None or d < best[0]):
        best = (d, c)
    if best:
      best[1].append(p)
    else:
      clusters.append([p])
  out = []
  for c in clusters:
    if len(c) < min_visits:
      continue
    lat, lng = sum(q[0] for q in c) / len(c), sum(q[1] for q in c) / len(c)
    spread = max(distance_m(lat, lng, q[0], q[1]) for q in c)
    out.append(Proposal(round(lat, 5), round(lng, 5), round(max(MIN_RADIUS_M, spread + MARGIN_M)), len(c)))
  return sorted(out, key=lambda p: -p.visits)[:limit]
