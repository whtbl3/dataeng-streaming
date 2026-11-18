"""Confluent wire format: magic byte 0, 4-byte big-endian schema id, Avro body."""

import pytest

from contracts.wire import WireError, frame, unframe


def test_frame_then_unframe_round_trips():
    raw = frame(7, b"\x02ab")
    assert raw == b"\x00\x00\x00\x00\x07\x02ab"
    assert unframe(raw) == (7, b"\x02ab")


@pytest.mark.parametrize(
    "raw, msg",
    [
        (None, "empty"),
        (b"", "empty"),
        (b"\x00\x00\x01", "shorter than 5 bytes"),
        (b'{"vehicle_id":"VAN-0001"}', "magic byte 0x7b"),
    ],
)
def test_unframe_rejects_non_registry_bytes(raw, msg):
    with pytest.raises(WireError, match=msg):
        unframe(raw)
