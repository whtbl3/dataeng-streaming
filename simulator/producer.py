"""Kafka producing for simulator pings (JSON until feature 3 switches to Avro)."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Protocol

from simulator.fleet import Ping


class ProducerLike(Protocol):
    def produce(self, topic: str, key: bytes, value: bytes) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float | None = None) -> int: ...


def encode(ping: Ping) -> tuple[bytes, bytes]:
    """Key by vehicle so each van's pings stay ordered within one partition."""
    value = json.dumps(ping.to_dict(), separators=(",", ":")).encode()
    return ping.vehicle_id.encode(), value


def make_producer(bootstrap: str) -> ProducerLike:
    from confluent_kafka import Producer  # imported lazily so unit tests need no broker

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


def produce_all(
    producer: ProducerLike,
    topic: str,
    pings: Iterable[Ping],
    *,
    realtime: bool = False,
) -> int:
    """Send pings; in realtime mode wait until each ping's event time. Returns count sent."""
    sent = 0
    for ping in pings:
        if realtime:
            delay = (ping.event_ts - datetime.now(UTC)).total_seconds()
            if delay > 0:
                producer.flush()
                time.sleep(delay)
        key, value = encode(ping)
        while True:
            try:
                producer.produce(topic, key=key, value=value)
                break
            except BufferError:  # local queue full: let deliveries drain, then retry
                producer.poll(0.5)
        producer.poll(0)
        sent += 1
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages were not delivered")
    return sent
