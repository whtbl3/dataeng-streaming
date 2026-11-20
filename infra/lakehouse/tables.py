"""Every bronze, silver and ops Iceberg table, defined once (spec 009, ADR 0015).

The same definitions create the tables in whichever catalog pyiceberg is configured for: the
Iceberg REST catalog locally, the AWS Glue Data Catalog on AWS. Settings come from the
environment (`PYICEBERG_CATALOG__LAKEHOUSE__*`), so nothing here knows which one it talks to.

A table that exists is checked, never changed: a different schema, partitioning, sort order or
key fails loudly (an evolution is a deliberate step, as in spec 005). Missing `write.*`
properties are added, since they only steer writers.

Gold is not here: dbt creates it (spec 007).

  python tables.py create         # create what is missing, check the rest
  python tables.py check          # check only (exit 1 on a difference)
  python tables.py drop silver    # drop one namespace's tables (`make silver-rebuild`)
"""

from __future__ import annotations

import contextlib
import os
import sys
from dataclasses import dataclass, field

# ---- the definitions ------------------------------------------------------------------------
# Column types: string, int, long, double, boolean, binary, timestamptz, or a struct as a tuple
# of (name, type) pairs. A trailing "!" marks a required (NOT NULL) column.
TS = "timestamptz"
VAN = (("van_id", "string"), ("depot_id", "string"), ("plate", "string"),
       ("capacity", "int"), ("created_at", TS), ("updated_at", TS))  # fmt: skip
DEPOT = (("depot_id", "string"), ("name", "string"), ("lat", "double"), ("lon", "double"),
         ("created_at", TS), ("updated_at", TS))  # fmt: skip
ORDER = (("order_id", "string"), ("depot_id", "string"), ("van_id", "string"),
         ("stop_seq", "int"), ("dest_lat", "double"), ("dest_lon", "double"),
         ("promised_start", TS), ("promised_end", TS), ("status", "string"),
         ("failure_reason", "string"), ("created_at", TS), ("assigned_at", TS),
         ("out_for_delivery_at", TS), ("delivered_at", TS), ("failed_at", TS),
         ("updated_at", TS))  # fmt: skip
CDC_TAIL = (("snapshot", "string"), ("source_lsn", "long"), ("source_txid", "long"),
            ("commit_ts", TS), ("capture_ts", TS), ("kafka_partition", "int"),
            ("kafka_offset", "long"), ("ingest_ts", TS))  # fmt: skip
# hash write distribution and metadata cleanup for every table Flink writes (spec 005)
FLINK_WRITE = {
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class Table:
    name: str  # namespace.table
    columns: tuple
    partitions: tuple = ()  # (transform, column): day, month, identity
    sort: tuple = ()  # columns, ascending
    key: tuple = ()  # Iceberg identifier fields: the upsert key (ADR 0011)
    properties: dict = field(default_factory=dict)


def _cdc(name: str, image: tuple, partitions: tuple = ()) -> Table:
    return Table(name, (("op", "string"), ("before", image), ("after", image), *CDC_TAIL),
                 partitions, properties=FLINK_WRITE)  # fmt: skip


TABLES = (
    Table(
        "bronze.gps_pings",
        (("vehicle_id", "string"), ("event_ts", TS), ("lat", "double"), ("lon", "double"),
         ("speed_kmh", "double"), ("heading_deg", "double"), ("ingest_ts", TS),
         ("kafka_partition", "int"), ("kafka_offset", "long")),
        partitions=(("day", "event_ts"),), sort=("vehicle_id", "event_ts"), properties=FLINK_WRITE,
    ),
    _cdc("bronze.depots_cdc", DEPOT),
    _cdc("bronze.vans_cdc", VAN),
    _cdc("bronze.orders_cdc", ORDER, (("day", "commit_ts"),)),
    Table(
        "bronze.dlq_events",
        (("source_topic", "string"), ("kafka_partition", "int"), ("kafka_offset", "long"),
         ("ingest_ts", TS), ("reason", "string"), ("detail", "string"), ("schema_id", "int"),
         ("raw_value", "binary")),
        partitions=(("day", "ingest_ts"),), properties=FLINK_WRITE,
    ),
    Table(
        "bronze.alert_events",
        (("alert_id", "string"), ("event", "string"), ("type", "string"), ("van_id", "string"),
         ("order_id", "string"), ("raised_at", TS), ("resolved_at", TS), ("lat", "double"),
         ("lon", "double"), ("detail", "string"), ("event_ts", TS)),
        partitions=(("day", "event_ts"),), properties=FLINK_WRITE,
    ),
    Table(
        "bronze.eta_marks",
        (("order_id", "string"), ("van_id", "string"), ("mark", "string"), ("eta", TS),
         ("promised_end", TS), ("event_ts", TS)),
        partitions=(("day", "event_ts"),), properties=FLINK_WRITE,
    ),
    Table(
        "silver.gps_pings",
        (("vehicle_id", "string!"), ("event_ts", TS + "!"), ("lat", "double"), ("lon", "double"),
         ("speed_kmh", "double"), ("heading_deg", "double"), ("ingest_ts", TS),
         ("lateness_s", "long"), ("is_late", "boolean")),
        partitions=(("day", "event_ts"),), sort=("vehicle_id", "event_ts"),
        key=("vehicle_id", "event_ts"), properties=FLINK_WRITE,
    ),
    Table(
        "silver.orders",
        (("order_id", "string!"), ("created_at", TS + "!"),
         *(c for c in ORDER if c[0] not in ("order_id", "created_at")),
         ("last_lsn", "long"), ("last_commit_ts", TS)),
        partitions=(("month", "created_at"),), key=("order_id", "created_at"),
        properties=FLINK_WRITE,
    ),
    Table(
        "silver.order_status_history",
        (("order_id", "string!"), ("status", "string!"), ("status_ts", TS + "!"),
         ("source_lsn", "long"), ("commit_ts", TS)),
        partitions=(("day", "status_ts"),), key=("order_id", "status", "status_ts"),
        properties=FLINK_WRITE,
    ),
    Table(
        "ops.maintenance_runs",
        (("task", "string"), ("table_name", "string"), ("started_at", TS), ("finished_at", TS),
         ("status", "string"), ("detail", "string")),
        partitions=(("month", "started_at"),),
    ),
)  # fmt: skip

BY_NAME = {t.name: t for t in TABLES}


# ---- definition → pyiceberg objects ---------------------------------------------------------
def _type(spec, ids):
    from pyiceberg import types as T

    if isinstance(spec, tuple):
        return T.StructType(*[_field(n, s, ids) for n, s in spec])
    simple = {"string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
              "double": T.DoubleType(), "boolean": T.BooleanType(), "binary": T.BinaryType(),
              TS: T.TimestamptzType()}  # fmt: skip
    return simple[spec]


def _field(name, spec, ids):
    from pyiceberg.types import NestedField

    required = isinstance(spec, str) and spec.endswith("!")
    spec = spec.rstrip("!") if isinstance(spec, str) else spec
    field_id = next(ids)
    return NestedField(field_id, name, _type(spec, ids), required=required)


def schema(table: Table):
    from itertools import count

    from pyiceberg.schema import Schema

    ids = count(1)
    fields = [_field(n, s, ids) for n, s in table.columns]
    by_name = {f.name: f.field_id for f in fields}
    return Schema(*fields, identifier_field_ids=[by_name[k] for k in table.key])


def partition_spec(table: Table, sch):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform

    transforms = {"day": DayTransform(), "month": MonthTransform(), "identity": IdentityTransform()}
    return PartitionSpec(*[
        PartitionField(sch.find_field(col).field_id, 1000 + i, transforms[t], f"{col}_{t}")
        for i, (t, col) in enumerate(table.partitions)
    ])  # fmt: skip


def sort_order(table: Table, sch):
    from pyiceberg.table.sorting import SortField, SortOrder
    from pyiceberg.transforms import IdentityTransform

    if not table.sort:
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

        return UNSORTED_SORT_ORDER
    return SortOrder(
        *[SortField(sch.find_field(c).field_id, IdentityTransform()) for c in table.sort]
    )


# ---- comparing an existing table with its definition ----------------------------------------
def describe_type(t) -> object:
    from pyiceberg import types as T

    if isinstance(t, T.StructType):
        return tuple((f.name, describe_type(f.field_type), f.required) for f in t.fields)
    return str(t)


def shape(sch, spec, order) -> dict:
    """What must match, by name (field ids may differ between catalogs)."""
    names = {f.field_id: f.name for f in sch.fields}
    return {
        "columns": [(f.name, describe_type(f.field_type), f.required) for f in sch.fields],
        "partitions": [(str(p.transform), names[p.source_id]) for p in spec.fields],
        "sort": [names[s.source_id] for s in order.fields],
        "key": sorted(names[i] for i in sch.identifier_field_ids),
    }


def differences(table: Table, existing) -> list[str]:
    want_schema = schema(table)
    want = shape(want_schema, partition_spec(table, want_schema), sort_order(table, want_schema))
    have = shape(existing.schema(), existing.spec(), existing.sort_order())
    return [f"{k}: want {want[k]}, have {have[k]}" for k in want if want[k] != have[k]]


# ---- catalog actions ------------------------------------------------------------------------
def catalog(wait_s: int = 90):
    """The configured catalog, waiting for it to answer (the local REST catalog starts slowly)."""
    import time

    from pyiceberg.catalog import load_catalog

    deadline = time.monotonic() + wait_s
    while True:
        try:
            cat = load_catalog("lakehouse")
            cat.list_namespaces()
            return cat
        except Exception as exc:  # noqa: BLE001 - not up yet; give up after wait_s
            if time.monotonic() > deadline:
                raise
            print(f"waiting for the catalog: {type(exc).__name__}", flush=True)
            time.sleep(3)


def create_missing(cat=None) -> list[str]:
    cat = cat or catalog()
    problems: list[str] = []
    for ns in sorted({t.name.split(".")[0] for t in TABLES}):
        cat.create_namespace_if_not_exists(ns)
    for table in TABLES:
        if not cat.table_exists(table.name):
            sch = schema(table)
            cat.create_table(
                table.name, sch, partition_spec=partition_spec(table, sch),
                sort_order=sort_order(table, sch),
                properties={"format-version": "2", **table.properties},
            )  # fmt: skip
            print(f"{table.name}: created")
            continue
        existing = cat.load_table(table.name)
        diff = differences(table, existing)
        if diff:
            problems += [f"{table.name}: {d}" for d in diff]
            continue
        missing = {k: v for k, v in table.properties.items() if existing.properties.get(k) != v}
        if missing:
            with existing.transaction() as tx:
                tx.set_properties(missing)
        print(f"{table.name}: ok" + (f", set {sorted(missing)}" if missing else ""))
    enable_optimizers()
    return problems


def check(cat=None) -> list[str]:
    cat = cat or catalog()
    return [
        f"{t.name}: {d}" if cat.table_exists(t.name) else f"{t.name}: missing"
        for t in TABLES
        for d in (differences(t, cat.load_table(t.name)) if cat.table_exists(t.name) else [""])
    ]


def drop_namespace(namespace: str, cat=None) -> None:
    cat = cat or catalog()
    for table in TABLES:
        if table.name.startswith(namespace + ".") and cat.table_exists(table.name):
            cat.purge_table(table.name)
            print(f"{table.name}: dropped")


def enable_optimizers() -> None:
    """Glue table optimizers (compaction, snapshot retention, orphan deletion) when a role is
    configured (AWS); locally the maintenance service does the same work (ADR 0012)."""
    role = os.environ.get("GLUE_OPTIMIZER_ROLE_ARN")
    if not role:
        return
    import boto3

    glue = boto3.client("glue")
    account = boto3.client("sts").get_caller_identity()["Account"]
    for table in TABLES:
        ns, name = table.name.split(".")
        for kind in ("compaction", "retention", "orphan_file_deletion"):
            with contextlib.suppress(glue.exceptions.AlreadyExistsException):
                glue.create_table_optimizer(
                    CatalogId=account, DatabaseName=ns, TableName=name, Type=kind,
                    TableOptimizerConfiguration={"roleArn": role, "enabled": True},
                )  # fmt: skip
        print(f"{table.name}: Glue optimizers on")


def main(argv: list[str]) -> int:
    command, *args = argv or ["create"]
    if command == "create":
        problems = create_missing()
    elif command == "check":
        problems = [p for p in check() if not p.endswith(": ")]
    elif command == "drop":
        drop_namespace(args[0])
        problems = []
    else:
        raise SystemExit(f"unknown command {command!r}")
    for p in problems:
        print(p, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
