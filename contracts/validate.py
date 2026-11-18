"""Business rules for GPS pings (spec 003 AC4). Each break has its own reason code.

Late and duplicate pings are *valid*; feature 4 handles them. "In the future" compares the
event time with the Kafka record time (system time), never with the processing clock.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timedelta

from contracts.bounds import HCMC_BBOX, MAX_SPEED_KMH

FUTURE_TOLERANCE = timedelta(minutes=5)


def _num(v: object) -> float | None:
    if isinstance(v, int | float) and not isinstance(v, bool) and not math.isnan(v):
        return float(v)
    return None


def gps_violation(ping: Mapping[str, object], ingest_ts: datetime) -> tuple[str, str] | None:
    """Return (reason, detail) for the first broken rule, or None when the ping is valid."""
    vid = ping.get("vehicle_id")
    if not isinstance(vid, str) or not vid.strip():
        return "missing_vehicle_id", f"vehicle_id={vid!r}"
    lat, lon = _num(ping.get("lat")), _num(ping.get("lon"))
    if lat is None or lon is None or not HCMC_BBOX.contains(lat, lon):
        return "out_of_bbox", f"lat={ping.get('lat')!r} lon={ping.get('lon')!r}"
    speed = _num(ping.get("speed_kmh"))
    if speed is None or not 0.0 <= speed <= MAX_SPEED_KMH:
        return "speed_out_of_range", f"speed_kmh={ping.get('speed_kmh')!r} (0-{MAX_SPEED_KMH:g})"
    ts = ping.get("event_ts")
    if not isinstance(ts, datetime):
        return "event_ts_in_future", f"event_ts={ts!r} is not a timestamp"
    if ts > ingest_ts + FUTURE_TOLERANCE:
        return (
            "event_ts_in_future",
            f"event_ts {ts.isoformat()} > ingest_ts {ingest_ts.isoformat()} + 5 min",
        )
    return None
