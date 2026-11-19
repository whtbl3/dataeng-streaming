"""ETA heuristic v1 (spec 006, resolved question 3). Pure functions, no I/O.

From the van's position through its remaining stops in stop order, up to the order's stop:
- the leg the van is driving now: straight-line distance ÷ its mean moving speed over the last
  10 min (current traffic), or the typical speed if it has not moved yet;
- every later leg: straight-line distance ÷ the typical fleet speed (25 km/h), because a jam on
  the current leg says little about the roads after it;
- plus a fixed dwell (4 min) per stop before the order's own stop.
No road network: v1 is a distance/speed heuristic (intent: ML ETA is out of scope). Spike S1 of
spec 006: using the recent speed for every leg gave 33 % precision; this gives 82-86 %.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime, timedelta

EARTH_KM = 6371.0088
TYPICAL_SPEED_KMH = 25.0  # later legs, and the current leg until the van has moved
DWELL = timedelta(minutes=4)  # per stop before the order's own stop


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(math.sqrt(a))


def route_eta(
    now: datetime,
    position: tuple[float, float],
    current_speed_kmh: float | None,
    stops: Sequence[tuple[float, float]],
    typical_speed_kmh: float = TYPICAL_SPEED_KMH,
    dwell: timedelta = DWELL,
) -> tuple[datetime, float]:
    """ETA at the last of `stops` (the order's stop), and the distance to it in km."""
    current = current_speed_kmh if current_speed_kmh and current_speed_kmh > 0 else None
    hours, km, here = 0.0, 0.0, position
    for i, stop in enumerate(stops):
        leg = haversine_km(*here, *stop)
        speed = current if i == 0 and current else typical_speed_kmh
        hours += leg / speed
        km += leg
        here = stop
    return now + timedelta(hours=hours) + dwell * max(len(stops) - 1, 0), km
