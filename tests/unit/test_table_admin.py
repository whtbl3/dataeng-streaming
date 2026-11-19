"""Table settings Trino cannot set, through the Iceberg REST catalog (fake catalog).

Spec 004: the silver upsert key (identifier fields). Spec 005: write distribution and metadata
cleanup properties, and whether a table already has rows.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "infra/local/iceberg/table_admin.py"
_spec = importlib.util.spec_from_file_location("table_admin", _PATH)
sif = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sif)


class FakeCatalog:
    def __init__(self, required=True, ids=None, properties=None, snapshot=None):
        self.posts: list[dict] = []
        self.meta = {
            "properties": properties or {},
            "current-snapshot-id": snapshot["snapshot-id"] if snapshot else -1,
            "snapshots": [snapshot] if snapshot else [],
            "current-schema-id": 0,
            "last-column-id": 3,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    **({"identifier-field-ids": ids} if ids else {}),
                    "fields": [
                        {"id": 1, "name": "vehicle_id", "required": required, "type": "string"},
                        {"id": 2, "name": "event_ts", "required": required, "type": "timestamptz"},
                        {"id": 3, "name": "lat", "required": False, "type": "double"},
                    ],
                }
            ],
        }

    def __call__(self, method, url, body):
        assert url == "http://cat/v1/namespaces/silver/tables/gps_pings"
        if method == "GET":
            return {"metadata": self.meta}
        self.posts.append(body)
        return {}


def test_adds_a_schema_with_the_key_and_makes_it_current():
    cat = FakeCatalog()
    assert sif.set_identifier_fields(
        "http://cat", "silver", "gps_pings", ["event_ts", "vehicle_id"], cat
    )
    (body,) = cat.posts
    assert body["requirements"] == [{"type": "assert-current-schema-id", "current-schema-id": 0}]
    add, current = body["updates"]
    assert add["action"] == "add-schema" and add["last-column-id"] == 3
    assert add["schema"]["identifier-field-ids"] == [1, 2]
    assert add["schema"]["schema-id"] == -1
    assert add["schema"]["fields"] == cat.meta["schemas"][0]["fields"]
    assert current == {"action": "set-current-schema", "schema-id": -1}


def test_is_idempotent():
    cat = FakeCatalog(ids=[2, 1])
    assert not sif.set_identifier_fields(
        "http://cat", "silver", "gps_pings", ["vehicle_id", "event_ts"], cat
    )
    assert cat.posts == []


def test_rejects_unknown_or_nullable_key_columns():
    with pytest.raises(ValueError, match="no column"):
        sif.set_identifier_fields("http://cat", "silver", "gps_pings", ["nope"], FakeCatalog())
    with pytest.raises(ValueError, match="NOT NULL"):
        sif.set_identifier_fields(
            "http://cat", "silver", "gps_pings", ["vehicle_id"], FakeCatalog(required=False)
        )


def test_sets_only_the_properties_that_differ():
    cat = FakeCatalog(properties={"write.distribution-mode": "hash", "owner": "trino"})
    want = {"write.distribution-mode": "hash", "write.metadata.previous-versions-max": "100"}
    assert sif.set_properties("http://cat", "silver", "gps_pings", want, cat)
    (body,) = cat.posts
    assert body == {
        "requirements": [],
        "updates": [
            {"action": "set-properties", "updates": {"write.metadata.previous-versions-max": "100"}}
        ],
    }


def test_properties_already_set_post_nothing():
    cat = FakeCatalog(properties={"write.distribution-mode": "hash"})
    assert not sif.set_properties(
        "http://cat", "silver", "gps_pings", {"write.distribution-mode": "hash"}, cat
    )
    assert cat.posts == []


def test_every_flink_written_table_gets_hash_distribution_and_metadata_cleanup():
    """AC11 (one writer per partition) and AC12 (old metadata files deleted, at most 100 kept)."""
    assert "silver.gps_pings" in sif.FLINK_TABLES and "bronze.gps_pings" in sif.FLINK_TABLES
    assert sif.TABLE_PROPERTIES == {
        "write.distribution-mode": "hash",
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "100",
    }
    assert sif.UPSERT_KEYS == {
        "silver.gps_pings": ["vehicle_id", "event_ts"],
        "silver.orders": ["order_id", "created_at"],
        "silver.order_status_history": ["order_id", "status", "status_ts"],
    }


@pytest.mark.parametrize(
    ("snapshot", "expected"),
    [
        (None, False),
        ({"snapshot-id": 7, "summary": {"total-records": "0"}}, False),
        ({"snapshot-id": 7, "summary": {"total-records": "12"}}, True),
    ],
)
def test_has_rows(snapshot, expected):
    cat = FakeCatalog(snapshot=snapshot)
    assert sif.has_rows("http://cat", "silver", "gps_pings", cat) is expected
