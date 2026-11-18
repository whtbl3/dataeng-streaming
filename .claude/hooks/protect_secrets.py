#!/usr/bin/env python3
"""PreToolUse hook: block edits to secret files.

Exit 2 = block the action; stderr is shown to Claude.
Exit 0 = allow.
Add your project's own secret paths to BLOCKED_NAMES / BLOCKED_SUFFIXES / BLOCKED_DIRS.
"""

import json
import sys
from pathlib import PurePath

ALLOWED_NAMES = {".env.example", ".env.sample", ".env.template", "terraform.tfvars.example"}
# dbt/profiles.yml is committed and may only use env_var(); see .claude/rules/dbt.md
BLOCKED_NAMES = {".env", "secrets.toml", "credentials.json"}
BLOCKED_PREFIXES = (".env.",)
BLOCKED_SUFFIXES = (".pem", ".p8", ".key", ".pfx", ".p12", ".tfvars", ".tfstate", ".tfstate.backup")
BLOCKED_DIRS = {"secrets", ".ssh", ".aws"}


def is_blocked(path_str: str) -> bool:
    path = PurePath(path_str)
    name = path.name
    if name in ALLOWED_NAMES:
        return False
    if name in BLOCKED_NAMES or name.startswith(BLOCKED_PREFIXES):
        return True
    if name.endswith(BLOCKED_SUFFIXES):
        return True
    return any(part in BLOCKED_DIRS for part in path.parts)


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    tool_input = data.get("tool_input") or {}
    path = tool_input.get("file_path") or tool_input.get("path") or ""
    if path and is_blocked(path):
        print(
            f"Blocked: {path} holds secrets. Edit the matching .example file instead, "
            "or ask the user to change it by hand.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
