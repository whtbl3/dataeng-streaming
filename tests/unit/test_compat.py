"""Offline FULL_TRANSITIVE compatibility check (spec 003 AC5)."""

import copy
import json
from pathlib import Path

import pytest

from contracts.compat import check_dir, full_errors

MICROS = {"type": "long", "logicalType": "timestamp-micros"}
V1 = json.loads(Path("schemas/gps.pings.v1-value/v1.avsc").read_text())


def changed(fn):
    s = copy.deepcopy(V1)
    fn(s["fields"])
    return s


def test_adding_an_optional_field_with_default_is_compatible():
    s = changed(lambda f: f.append({"name": "note", "type": ["null", "string"], "default": None}))
    assert full_errors(V1, s) == []


@pytest.mark.parametrize(
    "fn, words",
    [
        (lambda f: f.pop(2), "lat"),  # remove a required field
        (lambda f: f[2].update(name="latitude"), "lat"),  # rename
        (lambda f: f[0].update(type="int"), "vehicle_id"),  # type change
        (lambda f: f.append({"name": "battery", "type": "int"}), "battery"),  # new, no default
        (lambda f: f[1].update(type=MICROS), "event_ts"),  # same bytes, other meaning
    ],
)
def test_breaking_changes_are_rejected_with_the_field_name(fn, words):
    errors = full_errors(V1, changed(fn))
    assert errors and any(words in e for e in errors)


def test_type_promotion_is_one_way():
    widen = changed(lambda f: f.append({"name": "n", "type": "int", "default": 0}))
    wider = changed(lambda f: f.append({"name": "n", "type": "long", "default": 0}))
    assert full_errors(widen, wider)  # long cannot be read as int (forward fails)


def test_repo_schemas_are_full_transitive(tmp_path):
    assert check_dir(Path("schemas")) == []


def test_check_dir_reports_a_breaking_new_version(tmp_path):
    subject = tmp_path / "gps.pings.v1-value"
    subject.mkdir()
    (subject / "v1.avsc").write_text(json.dumps(V1))
    (subject / "v2.avsc").write_text(json.dumps(changed(lambda f: f.pop(2))))
    errors = check_dir(tmp_path)
    assert len(errors) >= 1 and "v2" in errors[0] and "v1" in errors[0]
