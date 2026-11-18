#!/usr/bin/env python3
"""PostToolUse hook: format and lint ONLY the file Claude just edited.

Fast and file-scoped on purpose; the full test suite belongs in `make check` / CI.
Exit 2 when lint errors remain, so Claude sees stderr and fixes them right away.
Tools that are not installed are skipped silently. Add your stack below.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60)


def ruff_cmd() -> list[str] | None:
    """Prefer the project's locked ruff (.venv), then a global one."""
    venv_ruff = Path(os.environ.get("CLAUDE_PROJECT_DIR", ".")) / ".venv" / "bin" / "ruff"
    if venv_ruff.exists():
        return [str(venv_ruff)]
    return ["ruff"] if shutil.which("ruff") else None


def handle_python(path: str) -> str | None:
    ruff = ruff_cmd()
    if ruff is None:
        return None
    run([*ruff, "format", path])
    result = run([*ruff, "check", "--fix", path])
    return None if result.returncode == 0 else result.stdout + result.stderr


def handle_terraform(path: str) -> str | None:
    if not shutil.which("terraform"):
        return None
    result = run(["terraform", "fmt", path])
    return None if result.returncode == 0 else result.stdout + result.stderr


def handle_js(path: str) -> str | None:
    if shutil.which("npx") and Path("node_modules/.bin/prettier").exists():
        run(["npx", "--no-install", "prettier", "--write", path])
    if shutil.which("npx") and Path("node_modules/.bin/eslint").exists():
        result = run(["npx", "--no-install", "eslint", "--fix", path])
        return None if result.returncode == 0 else result.stdout + result.stderr
    return None


HANDLERS = {
    ".py": handle_python,
    ".tf": handle_terraform,
    ".js": handle_js,
    ".jsx": handle_js,
    ".ts": handle_js,
    ".tsx": handle_js,
}


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    path = (data.get("tool_input") or {}).get("file_path") or ""
    handler = HANDLERS.get(Path(path).suffix)
    if not path or handler is None or not Path(path).exists():
        return 0
    try:
        errors = handler(path)
    except (subprocess.TimeoutExpired, OSError):
        return 0
    if errors:
        print(f"Lint errors remain in {path}; fix them now:\n{errors}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
