"""Producer encoding and CLI wiring, tested with a fake Kafka producer (no Docker)."""

import json
from datetime import UTC, datetime

from simulator.__main__ import main, steps_for
from simulator.fleet import Ping, generate_pings
from simulator.producer import encode, produce_all

PING = Ping(
    "VAN-0001", datetime(2026, 11, 18, 3, 0, 5, 123000, tzinfo=UTC), 10.7, 106.6, 32.5, 90.0
)


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flushed = True
        return 0


def test_encode_keys_by_vehicle_and_serialises_json_with_utc_millis():
    key, value = encode(PING)
    assert key == b"VAN-0001"
    payload = json.loads(value)
    assert payload == {
        "vehicle_id": "VAN-0001",
        "event_ts": "2026-11-18T03:00:05.123Z",
        "lat": 10.7,
        "lon": 106.6,
        "speed_kmh": 32.5,
        "heading_deg": 90.0,
    }


def test_produce_all_sends_every_ping_and_flushes():
    fake = FakeProducer()
    end = datetime(2026, 11, 18, tzinfo=UTC)
    pings = generate_pings(seed=1, vans=3, interval_s=5, steps=4, end_ts=end)
    sent = produce_all(fake, "gps.pings.v1", pings)
    assert sent == 12
    assert fake.flushed
    assert {t for t, _, _ in fake.messages} == {"gps.pings.v1"}


def test_steps_for_minutes():
    assert steps_for(minutes=2, interval_s=5) == 24
    assert steps_for(minutes=0.05, interval_s=5) == 1  # at least one step


def test_cli_fast_mode_prints_count(capsys):
    fake = FakeProducer()
    code = main(
        ["--vans", "2", "--interval", "5", "--minutes", "1", "--seed", "3"],
        producer_factory=lambda _bootstrap: fake,
    )
    assert code == 0
    assert len(fake.messages) == 2 * 12
    assert "produced=24" in capsys.readouterr().out
