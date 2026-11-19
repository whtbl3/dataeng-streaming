"""Set Iceberg identifier fields (the upsert key) through the REST catalog API (spec 004).

Flink's Iceberg upsert sink takes its key from the table's identifier fields, and Trino, which
creates the tables (it can declare partition transforms), cannot set them. Stdlib only, idempotent:

  python set_identifier_fields.py http://iceberg-rest:8181 silver.gps_pings=vehicle_id,event_ts ...
"""

from __future__ import annotations

import json
import sys
import urllib.request
from collections.abc import Callable

Call = Callable[[str, str, dict | None], dict]


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


def main(argv: list[str]) -> int:
    base, *specs = argv
    for spec in specs:
        name, cols = spec.split("=")
        namespace, table = name.split(".")
        columns = cols.split(",")
        changed = set_identifier_fields(base, namespace, table, columns)
        print(f"{name}: identifier fields {columns} {'set' if changed else 'already set'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
