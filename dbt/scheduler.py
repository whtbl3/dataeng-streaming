"""Gold scheduler (spec 007, ADR 0006, ADR 0014). Stdlib only, runs dbt as a subprocess.

- Every 5 min: the intraday build (provisional rows, the last 2 h of commits, open days only).
- After 01:00 Asia/Ho_Chi_Minh: close every past day that has facts and is not closed yet,
  oldest first. A stack that was down at 01:00 catches up on its next loop.
Each build also runs the data tests; a failing test fails the build (AC11).

  python scheduler.py                       # loop (the `dbt` service)
  python scheduler.py --once                # one intraday build      (make gold)
  python scheduler.py --close 2026-11-18    # close a day             (make close DAY=…)
  python scheduler.py --reclose 2026-11-18  # logged restatement      (make reclose DAY=…)
  python scheduler.py --test                # data tests on gold      (make gold-test)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time as clock
import urllib.request
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
LOCAL_TZ = ZoneInfo(os.environ.get("GOLD_LOCAL_TZ", "Asia/Ho_Chi_Minh"))
CLOSE_AT = time.fromisoformat(os.environ.get("GOLD_CLOSE_AT", "01:00"))
INTERVAL_S = int(os.environ.get("GOLD_INTERVAL_MIN", "5")) * 60
TRINO_HOST = os.environ.get("DBT_TRINO_HOST", "localhost")
TRINO = f"http://{TRINO_HOST}:{os.environ.get('DBT_TRINO_PORT', '8080')}"


def days_to_close(now_local: datetime, days_with_facts, closed: set[date]) -> list[date]:
    """Past days to close now: before CLOSE_AT, yesterday is still open (late events land)."""
    today = now_local.date()
    last = today.toordinal() - (1 if now_local.time() >= CLOSE_AT else 2)
    return sorted(d for d in set(days_with_facts) if d.toordinal() <= last and d not in closed)


def dbt_command(mode: str, day: date | None = None, restatement: bool = False) -> list[str]:
    vars_ = f"mode: {mode}"
    if day is not None:
        vars_ += f", day: '{day.isoformat()}'"
    if restatement:
        vars_ += ", restatement: true"
    return ["dbt", "build", "--vars", "{" + vars_ + "}", "--exclude", "test_type:unit"]


def _profiles() -> None:
    """Copy the committed example to a temp profiles dir (no profiles.yml in the repo)."""
    target = Path(os.environ.setdefault("DBT_PROFILES_DIR", "/tmp/dbt-profiles"))
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy(HERE / "profiles.yml.example", target / "profiles.yml")


def run_dbt(cmd: list[str]) -> int:
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=HERE, check=False).returncode


def _query(sql: str) -> list[list]:
    headers = {"X-Trino-User": "dbt-scheduler", "X-Trino-Catalog": "lakehouse"}
    req = urllib.request.Request(f"{TRINO}/v1/statement", data=sql.encode(), headers=headers)
    rows: list[list] = []
    while True:
        with urllib.request.urlopen(req, timeout=60) as r:
            reply = json.load(r)
        if "error" in reply:
            raise RuntimeError(reply["error"].get("message", "query failed"))
        rows += reply.get("data", [])
        if "nextUri" not in reply:
            return rows
        req = urllib.request.Request(reply["nextUri"], headers=headers)
        clock.sleep(0.05)


def close_due(now_local: datetime) -> None:
    try:
        days = [date.fromisoformat(r[0]) for r in _query(
            "SELECT DISTINCT CAST(local_date AS varchar) FROM gold.fct_delivery"
        )]  # fmt: skip
        closed = {date.fromisoformat(r[0]) for r in _query(
            "SELECT CAST(local_date AS varchar) FROM gold.closed_days"
        )}  # fmt: skip
    except Exception as exc:  # noqa: BLE001 - gold not built yet; the next loop tries again
        print(f"cannot list days to close: {exc}", file=sys.stderr, flush=True)
        return
    for day in days_to_close(now_local, days, closed):
        run_dbt(dbt_command("close", day=day))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true", help="one intraday build")
    group.add_argument("--close", type=date.fromisoformat, metavar="DAY")
    group.add_argument("--reclose", type=date.fromisoformat, metavar="DAY")
    group.add_argument("--test", action="store_true", help="data tests and source freshness")
    args = parser.parse_args(argv)
    _profiles()
    if args.once:
        return run_dbt(dbt_command("intraday"))
    if args.close:
        return run_dbt(dbt_command("close", day=args.close))
    if args.reclose:
        return run_dbt(dbt_command("close", day=args.reclose, restatement=True))
    if args.test:
        tests = run_dbt(["dbt", "test", "--exclude", "test_type:unit"])
        return tests or run_dbt(["dbt", "source", "freshness"])
    while True:
        started = clock.monotonic()
        run_dbt(dbt_command("intraday"))
        close_due(datetime.now(LOCAL_TZ))
        clock.sleep(max(0, INTERVAL_S - (clock.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
