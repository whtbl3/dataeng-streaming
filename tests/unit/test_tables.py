"""One table definition for both catalogs (spec 009 AC3); keys and write settings (004, 005)."""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
from pathlib import Path

_PATH = Path(__file__).resolve().parents[2] / "infra/lakehouse/tables.py"
_spec = importlib.util.spec_from_file_location("tables", _PATH)
t = importlib.util.module_from_spec(_spec)
sys.modules["tables"] = t
_spec.loader.exec_module(t)


def test_every_bronze_silver_and_ops_table_is_defined_once():
    names = [table.name for table in t.TABLES]
    assert len(names) == len(set(names))
    assert set(names) == {
        "bronze.gps_pings", "bronze.depots_cdc", "bronze.vans_cdc", "bronze.orders_cdc",
        "bronze.dlq_events", "bronze.alert_events", "bronze.eta_marks",
        "silver.gps_pings", "silver.orders", "silver.order_status_history",
        "ops.maintenance_runs",
    }  # fmt: skip


def test_upsert_keys_are_required_identifier_fields():
    """ADR 0011: Flink upserts on the identifier fields; Iceberg wants them required."""
    keys = {table.name: table.key for table in t.TABLES if table.key}
    assert keys == {
        "silver.gps_pings": ("vehicle_id", "event_ts"),
        "silver.orders": ("order_id", "created_at"),
        "silver.order_status_history": ("order_id", "status", "status_ts"),
    }
    for name in keys:
        sch = t.schema(t.BY_NAME[name])
        ids = set(sch.identifier_field_ids)
        assert ids and all(sch.find_field(i).required for i in ids)


def test_partitions_and_sort_order():
    """GPS by day, sorted by van (spec 005); orders by month of creation (spec 004)."""
    gps = t.BY_NAME["silver.gps_pings"]
    sch = t.schema(gps)
    shape = t.shape(sch, t.partition_spec(gps, sch), t.sort_order(gps, sch))
    assert shape["partitions"] == [("day", "event_ts")]
    assert shape["sort"] == ["vehicle_id", "event_ts"]
    orders = t.BY_NAME["silver.orders"]
    sch = t.schema(orders)
    assert t.shape(sch, t.partition_spec(orders, sch), t.sort_order(orders, sch))["partitions"] == [
        ("month", "created_at")
    ]


def test_flink_written_tables_get_hash_writes_and_metadata_cleanup():
    """Spec 005 AC11, AC12."""
    for table in t.TABLES:
        if table.name != "ops.maintenance_runs":
            assert table.properties == t.FLINK_WRITE, table.name
    assert t.FLINK_WRITE["write.distribution-mode"] == "hash"


def test_cdc_images_are_nested_rows():
    sch = t.schema(t.BY_NAME["bronze.orders_cdc"])
    after = t.describe_type(sch.find_field("after").field_type)
    assert ("delivered_at", "timestamptz", False) in after


def test_a_changed_definition_is_reported_not_applied():
    """An existing table that differs fails loudly (no silent evolution)."""
    gps = t.BY_NAME["silver.gps_pings"]
    sch = t.schema(gps)

    class Existing:
        def schema(self):
            return sch

        def spec(self):
            return t.partition_spec(gps, sch)

        def sort_order(self):
            return t.sort_order(gps, sch)

    assert t.differences(gps, Existing()) == []
    monthly = dataclasses.replace(gps, partitions=(("month", "event_ts"),))
    assert t.differences(monthly, Existing()) == [
        "partitions: want [('month', 'event_ts')], have [('day', 'event_ts')]"
    ]
