"""Offline Avro compatibility check, FULL_TRANSITIVE (ADR 0008, spec 003 AC5).

FULL = a new reader can read old data (BACKWARD) *and* an old reader can read new data
(FORWARD). TRANSITIVE = against every earlier version, not only the previous one. The registry
enforces the same mode at runtime; this check catches a bad change in CI, before merge.

Implements the Avro schema-resolution rules for what the contracts use: records, primitives
(with promotion), unions, enums, arrays, maps, fixed and logical types (a logical type change is
treated as breaking, because the bytes would be read with a different meaning).

CLI: `python -m contracts.compat schemas/`, one folder per subject holding v1.avsc, v2.avsc, ...
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
PROMOTIONS = {
    "int": {"long", "float", "double"},
    "long": {"float", "double"},
    "float": {"double"},
    "string": {"bytes"},
    "bytes": {"string"},
}

Schema = Any


def _norm(s: Schema, names: dict[str, dict]) -> Schema:
    """Turn "int" into {"type": "int"} and resolve references to named types."""
    if isinstance(s, str):
        return {"type": s} if s in PRIMITIVES else names.get(s, {"type": s})
    if isinstance(s, list):
        return s
    if isinstance(s, dict) and s.get("type") in ("record", "enum", "fixed") and "name" in s:
        names.setdefault(_fullname(s), s)
        names.setdefault(s["name"], s)
    return s


def _fullname(s: dict) -> str:
    ns = s.get("namespace")
    return f"{ns}.{s['name']}" if ns and "." not in s["name"] else s["name"]


def _kind(s: Schema) -> str:
    return "union" if isinstance(s, list) else s["type"]


def can_read(
    reader: Schema, writer: Schema, path: str = "", names: dict | None = None
) -> list[str]:
    """Errors that stop `reader` from reading data written with `writer` (empty = compatible)."""
    names = {} if names is None else names
    r, w = _norm(reader, names), _norm(writer, names)
    where = path or "<root>"

    if _kind(w) == "union":  # every writer branch must be readable
        return [e for branch in w for e in can_read(r, branch, path, names)]
    if _kind(r) == "union":  # some reader branch must read the writer type
        for branch in r:
            if not can_read(branch, w, path, names):
                return []
        return [f"{where}: no branch of reader union {_short(r)} can read writer type {_short(w)}"]

    rk, wk = _kind(r), _kind(w)
    if rk != wk and wk not in PRIMITIVES | {"record", "enum", "array", "map", "fixed"}:
        return [f"{where}: unknown type {wk!r}"]
    if r.get("logicalType") != w.get("logicalType"):
        return [f"{where}: logical type changed {w.get('logicalType')} -> {r.get('logicalType')}"]
    if rk in PRIMITIVES or wk in PRIMITIVES:
        if rk == wk or rk in PROMOTIONS.get(wk, set()):
            return []
        return [f"{where}: type {wk} cannot be read as {rk}"]
    if rk != wk:
        return [f"{where}: type {wk} cannot be read as {rk}"]

    if rk == "record":
        if r["name"] != w["name"] and w["name"] not in r.get("aliases", []):
            return [f"{where}: record name {w['name']} != {r['name']}"]
        wf = {f["name"]: f for f in w["fields"]}
        for alias_src in w["fields"]:
            for a in alias_src.get("aliases", []):
                wf.setdefault(a, alias_src)
        errors: list[str] = []
        for f in r["fields"]:
            fpath = f"{path}.{f['name']}" if path else f["name"]
            src = wf.get(f["name"]) or next((wf[a] for a in f.get("aliases", []) if a in wf), None)
            if src is None:
                if "default" not in f:
                    errors.append(f"{fpath}: field is missing in the writer and has no default")
                continue
            errors += can_read(f["type"], src["type"], fpath, names)
        return errors
    if rk == "enum":
        missing = set(w["symbols"]) - set(r["symbols"])
        if missing and "default" not in r:
            return [f"{where}: enum symbols {sorted(missing)} unknown to the reader"]
        return []
    if rk == "array":
        return can_read(r["items"], w["items"], f"{path}[]", names)
    if rk == "map":
        return can_read(r["values"], w["values"], f"{path}{{}}", names)
    if rk == "fixed":
        if r["size"] != w["size"] or r["name"] != w["name"]:
            return [f"{where}: fixed {w['name']}[{w['size']}] != {r['name']}[{r['size']}]"]
        return []
    return [f"{where}: unsupported type {rk!r}"]


def _short(s: Schema) -> str:
    return json.dumps(s, separators=(",", ":"))[:80]


def full_errors(old: Schema, new: Schema) -> list[str]:
    """FULL compatibility between two versions: both read directions must work."""
    back = [f"new reader on old data (BACKWARD): {e}" for e in can_read(new, old)]
    fwd = [f"old reader on new data (FORWARD): {e}" for e in can_read(old, new)]
    return back + fwd


def _version(p: Path) -> int:
    m = re.fullmatch(r"v(\d+)\.avsc", p.name)
    if not m:
        raise ValueError(f"{p}: schema files must be named v<N>.avsc")
    return int(m.group(1))


def check_dir(root: Path) -> list[str]:
    """Check every subject folder under `root`: each version against all earlier ones."""
    from fastavro import parse_schema  # also proves each file is a valid Avro schema

    errors: list[str] = []
    for subject in sorted(p for p in root.iterdir() if p.is_dir()):
        files = sorted(subject.glob("*.avsc"), key=_version)
        versions = [(f.stem, json.loads(f.read_text())) for f in files]
        for name, schema in versions:
            try:
                parse_schema(schema, _write_hint=False)
            except Exception as exc:  # noqa: BLE001 - report any parse failure the same way
                errors.append(f"{subject.name}/{name}: not a valid Avro schema: {exc}")
        for i, (new_name, new) in enumerate(versions):
            for old_name, old in versions[:i]:
                errors += [
                    f"{subject.name} {new_name} vs {old_name}: {e}" for e in full_errors(old, new)
                ]
    return errors


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    root = Path(args[0] if args else "schemas")
    errors = check_dir(root)
    for e in errors:
        print(f"INCOMPATIBLE {e}", file=sys.stderr)
    if errors:
        print(
            "Schema change is not FULL_TRANSITIVE compatible. Only add or remove fields that "
            "have a default; for a breaking change create a new subject/topic (.v2).",
            file=sys.stderr,
        )
        return 1
    subjects = sorted(p.name for p in root.iterdir() if p.is_dir())
    print(f"schemas: {len(subjects)} subject(s) FULL_TRANSITIVE compatible: {', '.join(subjects)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
