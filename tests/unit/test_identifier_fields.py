"""Spec 004: the silver upsert key is set as Iceberg identifier fields (fake REST catalog)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "infra/local/iceberg/set_identifier_fields.py"
_spec = importlib.util.spec_from_file_location("set_identifier_fields", _PATH)
sif = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sif)


class FakeCatalog:
    def __init__(self, required=True, ids=None):
        self.posts: list[dict] = []
        self.meta = {
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
