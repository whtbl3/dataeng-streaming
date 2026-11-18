"""Confluent wire format, used by the Redpanda Schema Registry serdes and the Avro converter.

Layout: one magic byte (0), the schema id as a 4-byte big-endian int, then the Avro body.
"""

from __future__ import annotations

MAGIC = 0
HEADER_LEN = 5


class WireError(ValueError):
    """The bytes are not a registry-framed message."""


def frame(schema_id: int, body: bytes) -> bytes:
    return bytes([MAGIC]) + schema_id.to_bytes(4, "big") + body


def unframe(raw: bytes | None) -> tuple[int, bytes]:
    if not raw:
        raise WireError("empty message")
    if len(raw) < HEADER_LEN:
        raise WireError(f"message is {len(raw)} bytes, shorter than 5 bytes")
    if raw[0] != MAGIC:
        raise WireError(f"magic byte 0x{raw[0]:02x}, expected 0x00")
    return int.from_bytes(raw[1:5], "big"), raw[5:]
