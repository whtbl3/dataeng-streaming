"""Spec 007 acceptance tests: control tower history in bronze and gold built by dbt (`make up`)."""

from __future__ import annotations

import time

import pytest
from conftest import query, wait_for
from test_control_tower import ReservedVan

pytestmark = pytest.mark.e2e

BRONZE_TIMEOUT_S = 150  # control tower checkpoint (30 s) commits the Iceberg sinks


def test_alert_events_and_eta_marks_reach_bronze():
    """AC1: every alert raise/resolve and the T30/T15 ETA marks land in bronze, once each."""
    van = ReservedVan()
    try:
        # deadline in 30 min + 20 s: the first ping is before T-30, the second after it
        oid = van.order(north_km=12, promised_in_min=30 + 20 / 60)
        time.sleep(3)
        van.ping(van.place(0), 30.0)
        time.sleep(25)
        van.ping(van.place(0.1), 30.0)  # 12 km at 30 km/h = 24 min: on time, no alert yet
        marks, _ = wait_for(
            lambda: query(
                "SELECT mark, eta, promised_end FROM lakehouse.bronze.eta_marks WHERE order_id = ?",
                (oid,),
            ),
            lambda rows: len(rows) == 1,
            BRONZE_TIMEOUT_S,
        )
        assert marks[0][0] == "T30"

        late = van.order(north_km=12, promised_in_min=5)  # 24 min away: at risk
        time.sleep(3)
        van.ping(van.place(0.2), 30.0)
        time.sleep(3)
        van.close(late)
        events, _ = wait_for(
            lambda: query(
                "SELECT event, type, alert_id FROM lakehouse.bronze.alert_events "
                "WHERE order_id = ? ORDER BY event",
                (late,),
            ),
            lambda rows: [r[0] for r in rows] == ["raised", "resolved"],
            BRONZE_TIMEOUT_S,
        )
        assert {r[1] for r in events} == {"SLA_RISK"} and len({r[2] for r in events}) == 1
    finally:
        van.teardown()
