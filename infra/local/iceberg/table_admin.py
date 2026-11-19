"""Iceberg table settings that Trino cannot set, through the REST catalog API. Stdlib only.

Trino creates the tables (only it can declare partition transforms), but it cannot set:
- identifier fields, the key Flink's upsert sink uses (spec 004, ADR 0011);
- `write.*` properties: hash write distribution (one writer per partition, so one file per
  partition per commit) and metadata cleanup (old metadata.json files deleted, 100 kept), spec 005.

  python table_admin.py http://iceberg-rest:8181 apply             # every Flink-written table
  python table_admin.py http://iceberg-rest:8181 has-rows silver.gps_pings   # exit 0 if it has rows
"""

from __future__ import annotations

import json
import sys
import urllib.request
from collections.abc import Callable

Call = Callable[[str, str, dict | None], dict]

FLINK_TABLES = [
    "bronze.gps_pings",
    "bronze.dlq_events",
    "bronze.depots_cdc",
    "bronze.vans_cdc",
    "bronze.orders_cdc",
    "bronze.alert_events",
    "bronze.eta_marks",
    "silver.gps_pings",
    "silver.orders",
    "silver.order_status_history",
]
TABLE_PROPERTIES = {
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}
# Tables Flink writes with upserts, and their keys (ADR 0011).
UPSERT_KEYS = {
    "silver.gps_pings": ["vehicle_id", "event_ts"],
    "silver.orders": ["order_id", "created_at"],
    "silver.order_status_history": ["order_id", "status", "status_ts"],
}


def http_call(method: str, url: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, method=method, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def set_identifier_fields(
    base: str, namespace: str, table: str, columns: list[str], call: Call = http_call
) -> bool:
    """Make `columns` the identifier fields; returns False if they already are."""
    url = f"{base}/v1/namespaces/{namespace}/tables/{table}"
    meta = call("GET", url, None)["metadata"]
    schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    by_name = {f["name"]: f for f in schema["fields"]}
    missing = [c for c in columns if c not in by_name]
    if missing:
        raise ValueError(f"{namespace}.{table}: no column(s) {missing}")
    optional = [c for c in columns if not by_name[c]["required"]]
    if optional:
        raise ValueError(f"{namespace}.{table}: key column(s) {optional} must be NOT NULL")
    ids = sorted(by_name[c]["id"] for c in columns)
    if sorted(schema.get("identifier-field-ids", [])) == ids:
        return False
    new_schema = {k: v for k, v in schema.items() if k != "schema-id"}
    new_schema.update({"schema-id": -1, "identifier-field-ids": ids})
    call(
        "POST",
        url,
        {
            "requirements": [
                {"type": "assert-current-schema-id", "current-schema-id": meta["current-schema-id"]}
            ],
            "updates": [
                {
                    "action": "add-schema",
                    "schema": new_schema,
                    "last-column-id": meta["last-column-id"],
                },
                {"action": "set-current-schema", "schema-id": -1},
            ],
        },
    )
    return True


def set_properties(
    base: str, namespace: str, table: str, properties: dict[str, str], call: Call = http_call
) -> bool:
    """Set the properties that differ; returns False if all already have these values."""
    url = f"{base}/v1/namespaces/{namespace}/tables/{table}"
    current = call("GET", url, None)["metadata"].get("properties", {})
    changes = {k: v for k, v in properties.items() if current.get(k) != v}
    if not changes:
        return False
    call(
        "POST",
        url,
        {"requirements": [], "updates": [{"action": "set-properties", "updates": changes}]},
    )
    return True


def has_rows(base: str, namespace: str, table: str, call: Call = http_call) -> bool:
    meta = call("GET", f"{base}/v1/namespaces/{namespace}/tables/{table}", None)["metadata"]
    current = meta.get("current-snapshot-id", -1)
    snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == current), None)
    return bool(snap) and int(snap.get("summary", {}).get("total-records", "0")) > 0


def apply(base: str, call: Call = http_call) -> None:
    for name in FLINK_TABLES:
        namespace, table = name.split(".")
        changed = set_properties(base, namespace, table, TABLE_PROPERTIES, call)
        print(f"{name}: write properties {'set' if changed else 'already set'}")
        if name in UPSERT_KEYS:
            changed = set_identifier_fields(base, namespace, table, UPSERT_KEYS[name], call)
            print(f"{name}: identifier fields {'set' if changed else 'already set'}")


def main(argv: list[str]) -> int:
    base, command, *args = argv
    if command == "apply":
        apply(base)
        return 0
    if command == "has-rows":
        namespace, table = args[0].split(".")
        return 0 if has_rows(base, namespace, table) else 1
    raise SystemExit(f"unknown command {command!r}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
