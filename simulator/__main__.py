"""Simulator CLI.

  python -m simulator day [--vans 20 --orders-per-day 500] [--history-hours 2 | --realtime]
  python -m simulator seed-sql > infra/local/postgres/init/002_seed.sql

`day` runs the coupled delivery-day simulation: order events go to Postgres (CDC source),
GPS pings go to Kafka. History mode emits [now - H, now] as fast as possible (order events
before the window are applied first, without pings, so the database is consistent).
Realtime mode catches up the orders to now, then sends each event at its time (Ctrl-C stops).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

from simulator.day import local_dates, merged_stream, simulate_day
from simulator.orders_db import ConnectionLike, OrdersWriter, connect
from simulator.producer import Corrupter, GpsEncoder, ProducerLike, make_producer, send
from simulator.world import seed_sql

DEFAULT_DSN = "postgresql://ct:ct-local@localhost:15432/control_tower"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="simulator", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("seed-sql", help="print idempotent seed SQL for depots and vans")

    d = sub.add_parser("day", help="run the coupled order + GPS simulation")
    d.add_argument("--vans", type=int, default=int(os.environ.get("SIM_VANS", 20)))
    d.add_argument("--orders-per-day", type=int, default=None, help="default: 25 per van")
    d.add_argument("--interval", type=int, default=int(os.environ.get("SIM_PING_SECONDS", 5)))
    d.add_argument("--seed", type=int, default=42)
    d.add_argument("--late-rate", type=float, default=0.10)
    d.add_argument("--fail-rate", type=float, default=0.02)
    d.add_argument("--prefix", default="ORD", help="order id prefix (tests use a unique one)")
    d.add_argument("--vehicle-prefix", default=None, help="rename GPS vehicle ids (tests only)")
    mode = d.add_mutually_exclusive_group()
    mode.add_argument("--history-hours", type=float, default=2.0)
    mode.add_argument("--realtime", action="store_true")
    d.add_argument("--no-db", action="store_true", help="do not write orders")
    d.add_argument("--no-gps", action="store_true", help="do not send GPS pings")
    d.add_argument("--topic", default=os.environ.get("GPS_TOPIC", "gps.pings.v1"))
    d.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    d.add_argument(
        "--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")
    )
    d.add_argument("--schema-version", default=None, help="e.g. v1; default: newest in schemas/")
    d.add_argument(
        "--invalid-rate", type=float, default=0.0, help="share of pings sent invalid (DLQ tests)"
    )
    d.add_argument("--dsn", default=os.environ.get("ORDERS_DSN", DEFAULT_DSN))
    return p


def _floor(ts: datetime, interval_s: int) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % interval_s, UTC)


def run_day(
    args: argparse.Namespace,
    producer_factory: Callable[[str], ProducerLike],
    db_factory: Callable[[str], ConnectionLike],
    now: datetime | None = None,
    encoder_factory: Callable[[argparse.Namespace], GpsEncoder] | None = None,
) -> tuple[int, int, Corrupter | None]:
    now = _floor(now or datetime.now(UTC), args.interval)
    if args.realtime:
        start, end = now, now + timedelta(days=7)
    else:
        start, end = now - timedelta(hours=args.history_hours), now

    orders_per_day = args.orders_per_day if args.orders_per_day is not None else 25 * args.vans
    plans = [
        simulate_day(
            seed=args.seed,
            local_date=d,
            vans=args.vans,
            orders_per_day=orders_per_day,
            late_rate=args.late_rate,
            fail_rate=args.fail_rate,
            prefix=args.prefix,
            interval_s=args.interval,
        )
        for d in local_dates(start, end)
    ]
    writer = None if args.no_db else OrdersWriter(db_factory(args.dsn))
    producer = None if args.no_gps else producer_factory(args.bootstrap)
    make_encoder = encoder_factory or (
        lambda a: GpsEncoder.from_registry(a.registry, a.schema_version)
    )
    encoder = make_encoder(args) if producer else None
    corrupter = Corrupter(args.invalid_rate, args.seed) if args.invalid_rate > 0 else None

    if writer:  # catch-up: order history before the window, without pings
        for plan in plans:
            for event in plan.events:
                if event.ts < start:
                    writer.apply(event)

    pings = 0
    stream = merged_stream(plans, start, end, gps=producer is not None, orders=writer is not None)
    try:
        for ts, kind, item in stream:
            if args.realtime:
                delay = (ts - datetime.now(UTC)).total_seconds()
                if delay > 0:
                    if producer:
                        producer.flush()
                    time.sleep(delay)
            if kind == "order":
                writer.apply(item)
            else:
                if args.vehicle_prefix:
                    item = type(item)(
                        f"{args.vehicle_prefix}-{item.vehicle_id.split('-')[-1]}",
                        *(
                            getattr(item, f)
                            for f in ("event_ts", "lat", "lon", "speed_kmh", "heading_deg")
                        ),
                    )
                send(producer, args.topic, item, encoder, corrupter)
                pings += 1
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)
    finally:
        if producer:
            remaining = producer.flush(30)
            if remaining:
                raise RuntimeError(f"{remaining} pings were not delivered")
    return (writer.changes if writer else 0), pings, corrupter


def main(
    argv: Sequence[str] | None = None,
    producer_factory: Callable[[str], ProducerLike] = make_producer,
    db_factory: Callable[[str], ConnectionLike] = connect,
    now: datetime | None = None,
    encoder_factory: Callable[[argparse.Namespace], GpsEncoder] | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "seed-sql":
        print(seed_sql(), end="")
        return 0
    changes, pings, corrupter = run_day(args, producer_factory, db_factory, now, encoder_factory)
    line = f"order_changes={changes} pings={pings} vans={args.vans} prefix={args.prefix}"
    if corrupter:
        line += " invalid=" + ",".join(f"{k}:{n}" for k, n in sorted(corrupter.counts.items()))
    print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
