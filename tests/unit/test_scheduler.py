"""Gold scheduler: which dbt runs, and which days to close when (spec 007, ADR 0006)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

_PATH = Path(__file__).resolve().parents[2] / "dbt/scheduler.py"
_spec = importlib.util.spec_from_file_location("gold_scheduler", _PATH)
s = importlib.util.module_from_spec(_spec)
sys.modules["gold_scheduler"] = s
_spec.loader.exec_module(s)

HCM = ZoneInfo("Asia/Ho_Chi_Minh")


def local(y, m, d, hh, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=HCM)


def test_before_01_00_yesterday_stays_open():
    days = [date(2026, 11, 16), date(2026, 11, 17), date(2026, 11, 18)]
    assert s.days_to_close(local(2026, 11, 19, 0, 59), days, closed=set()) == [
        date(2026, 11, 16),
        date(2026, 11, 17),
    ]


def test_after_01_00_every_past_day_closes_oldest_first_once():
    days = [date(2026, 11, 18), date(2026, 11, 16), date(2026, 11, 19)]
    closed = {date(2026, 11, 16)}
    assert s.days_to_close(local(2026, 11, 19, 1, 0), days, closed) == [date(2026, 11, 18)]


def test_today_never_closes():
    assert s.days_to_close(local(2026, 11, 19, 23, 0), [date(2026, 11, 19)], set()) == []


def test_close_time_is_configurable():
    assert time(1, 0) == s.CLOSE_AT


def test_dbt_commands():
    assert s.dbt_command("intraday") == [
        "dbt", "build", "--vars", "{mode: intraday}", "--exclude", "test_type:unit",
    ]  # fmt: skip
    assert s.dbt_command("close", day=date(2026, 11, 18)) == [
        "dbt", "build", "--vars", "{mode: close, day: '2026-11-18'}",
        "--exclude", "test_type:unit",
    ]  # fmt: skip
    assert s.dbt_command("close", day=date(2026, 11, 18), restatement=True) == [
        "dbt", "build", "--vars", "{mode: close, day: '2026-11-18', restatement: true}",
        "--exclude", "test_type:unit",
    ]  # fmt: skip
