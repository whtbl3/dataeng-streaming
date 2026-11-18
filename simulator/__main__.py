"""CLI: `python -m simulator --vans 20 --interval 5 --minutes 2 [--realtime]`.

Fast mode (default) emits `minutes` worth of history ending now, as fast as possible.
Realtime mode starts now and sends each ping at its event time (Ctrl-C to stop).
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

from simulator.fleet import generate_pings
from simulator.producer import ProducerLike, make_producer, produce_all

REALTIME_FOREVER_STEPS = 10**9


def steps_for(*, minutes: float, interval_s: int) -> int:
    return max(1, math.floor(minutes * 60 / interval_s))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="simulator", description=__doc__)
    p.add_argument("--vans", type=int, default=int(os.environ.get("SIM_VANS", 20)))
    p.add_argument("--interval", type=int, default=int(os.environ.get("SIM_PING_SECONDS", 5)))
    p.add_argument("--minutes", type=float, default=None, help="history/run length")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--prefix", default="VAN", help="vehicle id prefix (tests use a unique one)")
    p.add_argument("--realtime", action="store_true")
    p.add_argument("--topic", default=os.environ.get("GPS_TOPIC", "gps.pings.v1"))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    return p


def main(
    argv: Sequence[str] | None = None,
    producer_factory: Callable[[str], ProducerLike] = make_producer,
) -> int:
    args = build_parser().parse_args(argv)
    now = datetime.now(UTC).replace(microsecond=0)
    if args.realtime:
        steps = (
            steps_for(minutes=args.minutes, interval_s=args.interval)
            if args.minutes
            else REALTIME_FOREVER_STEPS
        )
        end_ts = now + timedelta(seconds=args.interval * (steps - 1))
    else:
        steps = steps_for(minutes=args.minutes or 2, interval_s=args.interval)
        end_ts = now

    pings = generate_pings(
        seed=args.seed,
        vans=args.vans,
        interval_s=args.interval,
        steps=steps,
        end_ts=end_ts,
        prefix=args.prefix,
    )
    producer = producer_factory(args.bootstrap)
    try:
        sent = produce_all(producer, args.topic, pings, realtime=args.realtime)
    except KeyboardInterrupt:
        producer.flush(10)
        print("stopped", file=sys.stderr)
        return 0
    print(f"produced={sent} topic={args.topic} vans={args.vans} prefix={args.prefix}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
