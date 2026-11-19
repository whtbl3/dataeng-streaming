"""Kafka producing for simulator pings: registry-framed Avro (spec 003).

The schema comes from `schemas/gps.pings.v1-value/` (the contract in git). The producer only
looks up its id; it never registers a schema itself, so an unregistered contract fails fast.
`Corrupter` turns a seeded share of pings into labelled invalid events for DLQ tests (AC7).
"""

from __future__ import annotations

import io
import json
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from fastavro import parse_schema, schemaless_writer

from contracts.decode import GPS_SUBJECT, SCHEMAS_DIR
from contracts.registry import Registry
from contracts.wire import frame
from simulator.fleet import Ping

# Reason codes the DLQ must report for each injected kind (see contracts.validate/decode).
KINDS = (
    "missing_vehicle_id",
    "out_of_bbox",
    "speed_out_of_range",
    "event_ts_in_future",
    "not_decodable",
)
LOCAL_UTC_OFFSET = timedelta(hours=7)


class ProducerLike(Protocol):
    def produce(
        self, topic: str, key: bytes, value: bytes, timestamp: int | None = None
    ) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float | None = None) -> int: ...


def latest_version(subject: str = GPS_SUBJECT) -> str:
    files = sorted((SCHEMAS_DIR / subject).glob("v*.avsc"), key=lambda p: int(p.stem[1:]))
    return files[-1].stem


def battery_pct(ping: Ping) -> int:
    """Deterministic battery level for the v2 demo field: full at 06:00 local, -1 % per 10 min."""
    local = ping.event_ts + LOCAL_UTC_OFFSET
    minutes = (local.hour - 6) * 60 + local.minute
    return int(min(100, max(5, 100 - minutes // 10)))


class GpsEncoder:
    """Encodes pings with one registered schema version (key = vehicle id)."""

    def __init__(self, schema: dict, schema_id: int) -> None:
        self.schema_id = schema_id
        self._parsed = parse_schema(schema)
        self._fields = {f["name"] for f in schema["fields"]}

    @classmethod
    def from_registry(cls, registry_url: str, version: str | None = None) -> GpsEncoder:
        version = version or latest_version()
        schema = json.loads(Path(SCHEMAS_DIR / GPS_SUBJECT / f"{version}.avsc").read_text())
        return cls(schema, Registry(registry_url).id_for(GPS_SUBJECT, schema))

    def record(self, ping: Ping) -> dict[str, object]:
        rec: dict[str, object] = {
            "vehicle_id": ping.vehicle_id,
            "event_ts": ping.event_ts,
            "lat": ping.lat,
            "lon": ping.lon,
            "speed_kmh": ping.speed_kmh,
            "heading_deg": ping.heading_deg,
        }
        if "battery_pct" in self._fields:
            rec["battery_pct"] = battery_pct(ping)
        return rec

    def encode_record(self, record: dict[str, object]) -> bytes:
        buf = io.BytesIO()
        schemaless_writer(buf, self._parsed, record)
        return frame(self.schema_id, buf.getvalue())

    def encode(self, ping: Ping) -> tuple[bytes, bytes]:
        """Key by vehicle so each van's pings stay ordered within one partition."""
        return ping.vehicle_id.encode(), self.encode_record(self.record(ping))


class Corrupter:
    """Turns about `rate` of the pings into invalid events, one labelled kind each."""

    def __init__(
        self, rate: float, seed: int = 42, clock: Callable[[], datetime] | None = None
    ) -> None:
        self.rate = rate
        self._rng = random.Random(seed)
        self._clock = clock or (lambda: datetime.now(UTC))
        self.counts: Counter[str] = Counter()

    def encode(self, ping: Ping, encoder: GpsEncoder) -> tuple[bytes, bytes]:
        key = ping.vehicle_id.encode()
        if self.rate <= 0 or self._rng.random() >= self.rate:
            return encoder.encode(ping)
        kind = KINDS[self._rng.randrange(len(KINDS))]
        self.counts[kind] += 1
        if kind == "not_decodable":  # e.g. a producer that still sends the old JSON
            return key, json.dumps(ping.to_dict(), separators=(",", ":")).encode()
        rec = encoder.record(ping)
        if kind == "missing_vehicle_id":
            rec["vehicle_id"] = ""
        elif kind == "out_of_bbox":
            rec["lat"] = 21.0285  # Hanoi
        elif kind == "speed_out_of_range":
            rec["speed_kmh"] = 150.0
        else:  # event_ts_in_future: relative to the wall clock, so it fires in history mode too
            rec["event_ts"] = self._clock() + timedelta(minutes=10)
        return key, encoder.encode_record(rec)


# Batched and compressed (spec 005 AC2): wait up to 20 ms to fill a batch of up to 512 KiB, then
# compress the whole batch. zstd shrinks a batch of Avro pings to about a third of its size.
PRODUCER_CONFIG = {
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
    "batch.size": 512 * 1024,
    "compression.type": "zstd",
}


def make_producer(bootstrap: str) -> ProducerLike:
    from confluent_kafka import Producer  # imported lazily so unit tests need no broker

    return Producer({"bootstrap.servers": bootstrap, **PRODUCER_CONFIG})


def _ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def send_raw(
    producer: ProducerLike, topic: str, key: bytes, value: bytes, timestamp_ms: int | None = None
) -> None:
    """Produce one message. `timestamp_ms` sets the Kafka record time (= bronze `ingest_ts`)."""
    kwargs = {} if timestamp_ms is None else {"timestamp": timestamp_ms}
    while True:
        try:
            producer.produce(topic, key=key, value=value, **kwargs)
            break
        except BufferError:  # local queue full: let deliveries drain, then retry
            producer.poll(0.5)
    producer.poll(0)


def send(
    producer: ProducerLike,
    topic: str,
    ping: Ping,
    encoder: GpsEncoder,
    corrupter: Corrupter | None = None,
    sent_at: datetime | None = None,
) -> None:
    """Send one ping; the record time is when the device sent it (default: its event time)."""
    key, value = corrupter.encode(ping, encoder) if corrupter else encoder.encode(ping)
    send_raw(producer, topic, key, value, _ms(sent_at or ping.event_ts))


def produce_all(
    producer: ProducerLike,
    topic: str,
    pings: Iterable[Ping],
    encoder: GpsEncoder,
    *,
    realtime: bool = False,
    corrupter: Corrupter | None = None,
) -> int:
    """Send pings; in realtime mode wait until each ping's event time. Returns count sent."""
    sent = 0
    for ping in pings:
        if realtime:
            delay = (ping.event_ts - datetime.now(UTC)).total_seconds()
            if delay > 0:
                producer.flush()
                time.sleep(delay)
        send(producer, topic, ping, encoder, corrupter)
        sent += 1
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages were not delivered")
    return sent
