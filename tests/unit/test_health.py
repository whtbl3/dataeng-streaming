"""The NFR-10 health rules behind `make health` (spec 008 AC3, AC4)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

_PATH = Path(__file__).resolve().parents[2] / "scripts/health.py"
_spec = importlib.util.spec_from_file_location("health", _PATH)
h = importlib.util.module_from_spec(_spec)
sys.modules["health"] = h
_spec.loader.exec_module(h)

NOW = datetime(2026, 11, 20, 3, 0, tzinfo=UTC)


def test_lag_alerts_only_when_it_grows_past_the_minimum():
    assert not h.lag_alert(0, 0)
    assert not h.lag_alert(500, 1500)  # growing, but within one checkpoint's worth
    assert not h.lag_alert(5000, 3000)  # catching up
    assert h.lag_alert(3000, 5000)
    assert not h.lag_alert(None, 5000)  # one sample only (--quick): growth unknown


def test_checkpoint_alerts_when_the_latest_attempt_failed():
    assert not h.checkpoint_alert(None, NOW)
    assert not h.checkpoint_alert(NOW - timedelta(minutes=5), NOW)  # failed, then recovered
    assert h.checkpoint_alert(NOW, NOW - timedelta(minutes=1))
    assert h.checkpoint_alert(NOW, None)


def test_slot_alerts_over_500_mb():
    assert not h.slot_alert(500 * 1024 * 1024)
    assert h.slot_alert(500 * 1024 * 1024 + 1)
    assert not h.slot_alert(None)  # no slot: the CDC profile is off


def test_bronze_is_stuck_only_when_kafka_is_fresh_and_bronze_is_not():
    fresh, stale = NOW - timedelta(seconds=30), NOW - timedelta(minutes=3)
    assert h.bronze_stuck(fresh, stale, NOW)
    assert not h.bronze_stuck(fresh, fresh, NOW)
    assert not h.bronze_stuck(stale, stale, NOW)  # nothing sent: idle, not stuck
    assert not h.bronze_stuck(None, None, NOW)
    assert h.bronze_stuck(fresh, None, NOW)


def test_report_and_exit_code():
    checks = [h.Check("lag flink-bronze-gps", False, "120 → 80"), h.Check("slot", True, "600 MB")]
    lines = h.report(checks)
    assert lines == ["OK     lag flink-bronze-gps  120 → 80", "ALERT  slot  600 MB"]
    assert h.exit_code(checks) == 1
    assert h.exit_code(checks[:1]) == 0
