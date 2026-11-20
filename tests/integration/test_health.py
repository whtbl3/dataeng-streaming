"""Spec 008 light checks: every designed panel query runs, and `make health` reports (`make up`)."""

from __future__ import annotations

import re
import subprocess

import pytest
from conftest import ROOT, pg_query, query

pytestmark = pytest.mark.e2e

PANEL = re.compile(r"^-- panel: (\S+) \((postgres|trino)\)$", re.M)


def panels() -> list[tuple[str, str, str]]:
    text = (ROOT / "docs/design/dashboards.sql").read_text()
    marks = list(PANEL.finditer(text))
    out = []
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        sql = text[m.end() : end].strip().rstrip(";")
        out.append((m.group(1), m.group(2), sql))
    return out


@pytest.mark.parametrize(("name", "source", "sql"), panels(), ids=lambda v: v if "." in v else "")
def test_every_dashboard_query_runs(name, source, sql):
    """AC2."""
    rows = pg_query(sql) if source == "postgres" else query(sql)
    assert isinstance(rows, list), name


def test_every_designed_panel_has_a_query():
    """AC1: each SQL panel named in the design exists in the query file, and the reverse."""
    doc = (ROOT / "docs/design/dashboards.md").read_text()
    named = set(re.findall(r"`((?:dispatch|kpi|pipeline)\.[a-z_]+)`", doc))
    assert named == {name for name, _, _ in panels()}


def test_health_reports_every_check():
    """AC3 (light): one run, every NFR-10 check present with OK or ALERT."""
    out = subprocess.run(
        ["uv", "run", "python", "scripts/health.py", "--quick"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    ).stdout  # fmt: skip
    lines = out.splitlines()
    assert all(line.split()[0] in ("OK", "ALERT") for line in lines), out
    for name in ("lag flink-bronze-gps", "checkpoint bronze_gps", "replication slot",
                 "bronze receives rows"):  # fmt: skip
        assert any(name in line for line in lines), name
