"""Apply simulated order events to the order database, one transaction per event.

Writes are idempotent and enforce the lifecycle: an update only applies when the order is
in the expected previous status, so re-running a simulation produces no extra changes
(and no extra CDC events). `apply` returns the number of rows actually changed.
"""

from __future__ import annotations

from typing import Any, Protocol

from simulator.day import OrderEvent

INSERT_CREATED = """
INSERT INTO orders (order_id, depot_id, dest_lat, dest_lon, promised_start, promised_end,
                    status, created_at, updated_at)
VALUES (%(order_id)s, %(depot_id)s, %(dest_lat)s, %(dest_lon)s, %(promised_start)s,
        %(promised_end)s, 'created', %(ts)s, %(ts)s)
ON CONFLICT (order_id) DO NOTHING
"""

UPDATES = {
    "assigned": (
        "created",
        "UPDATE orders SET status = 'assigned', van_id = %(van_id)s, stop_seq = %(stop_seq)s, "
        "assigned_at = %(ts)s, updated_at = %(ts)s "
        "WHERE order_id = %(order_id)s AND status = %(prev)s",
    ),
    "out_for_delivery": (
        "assigned",
        "UPDATE orders SET status = 'out_for_delivery', out_for_delivery_at = %(ts)s, "
        "updated_at = %(ts)s WHERE order_id = %(order_id)s AND status = %(prev)s",
    ),
    "delivered": (
        "out_for_delivery",
        "UPDATE orders SET status = 'delivered', delivered_at = %(ts)s, updated_at = %(ts)s "
        "WHERE order_id = %(order_id)s AND status = %(prev)s",
    ),
    "failed": (
        "out_for_delivery",
        "UPDATE orders SET status = 'failed', failed_at = %(ts)s, "
        "failure_reason = %(reason)s, updated_at = %(ts)s "
        "WHERE order_id = %(order_id)s AND status = %(prev)s",
    ),
}


class ConnectionLike(Protocol):
    def execute(self, query: str, params: dict[str, Any]) -> Any: ...


def statement_for(event: OrderEvent) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {"order_id": event.order_id, "ts": event.ts, **event.fields}
    if event.kind == "created":
        return INSERT_CREATED, params
    prev, sql = UPDATES[event.kind]
    return sql, {**params, "prev": prev}


class OrdersWriter:
    def __init__(self, conn: ConnectionLike) -> None:
        self.conn = conn  # autocommit: each statement is its own transaction
        self.changes = 0

    def apply(self, event: OrderEvent) -> int:
        sql, params = statement_for(event)
        changed = self.conn.execute(sql, params).rowcount
        self.changes += changed
        return changed


def connect(dsn: str) -> ConnectionLike:
    import psycopg  # lazy: unit tests need no database

    return psycopg.connect(dsn, autocommit=True)
