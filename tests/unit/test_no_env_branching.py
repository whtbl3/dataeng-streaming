"""Spec 009 AC1 (ADR 0005, 0015): local and AWS differ only in configuration.

The jobs, table definitions, dbt models and scripts must not look at which environment they run
in. Configuration lives in the env files (infra/local/lakehouse.env, infra/aws/lakehouse.env),
the dbt profile and Terraform; engine details stay in the adapter macros.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CODE = [
    *ROOT.glob("flink/**/*.py"), *ROOT.glob("flink/jobs/*.sql"), *ROOT.glob("contracts/*.py"),
    *ROOT.glob("simulator/*.py"), *ROOT.glob("dbt/models/**/*.sql"), *ROOT.glob("dbt/tests/*.sql"),
    ROOT / "dbt/scheduler.py", ROOT / "infra/lakehouse/tables.py",
    ROOT / "infra/local/maintenance/maintain.py", ROOT / "infra/local/flink/submit.sh",
]  # fmt: skip
# the switch variable itself, or a comparison with an environment name or an engine
SWITCH = re.compile(r"(?<![A-Z_])LAKEHOUSE(?![A-Z_])")
BRANCHING = re.compile(
    r"\bif\b[^\n]*['\"](aws|local)['\"]|==\s*['\"](aws|local)['\"]|target\.type\s*==|"
    r"adapter\.type\(\)|\bathena\b",
    re.IGNORECASE,
)


def _without_block_comments(text: str) -> str:
    """Blank out Jinja and SQL block comments, keeping line numbers."""
    return re.sub(r"\{#.*?#\}|/\*.*?\*/", lambda m: "\n" * m.group().count("\n"), text, flags=re.S)


def test_code_does_not_branch_on_the_environment():
    hits = [
        f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
        for path in CODE
        for n, line in enumerate(_without_block_comments(path.read_text()).splitlines(), start=1)
        if (SWITCH.search(line) or BRANCHING.search(line))
        and not line.lstrip().startswith(("#", "--", "{#"))
    ]
    assert hits == []


def test_only_config_files_name_the_environments():
    local = (ROOT / "infra/local/lakehouse.env").read_text()
    aws = (ROOT / "infra/aws/lakehouse.env.example").read_text()
    keys = lambda text: {l.split("=", 1)[0] for l in text.splitlines() if "=" in l and l[0] != "#"}  # noqa: E731, E741
    # both files set the same switches (the AWS one adds the optimizer role and Athena settings)
    assert keys(local) - {"PYICEBERG_CATALOG__LAKEHOUSE__URI"} <= keys(aws) | {
        "PYICEBERG_CATALOG__LAKEHOUSE__S3__ENDPOINT",
        "PYICEBERG_CATALOG__LAKEHOUSE__S3__ACCESS_KEY_ID",
        "PYICEBERG_CATALOG__LAKEHOUSE__S3__SECRET_ACCESS_KEY",
        "PYICEBERG_CATALOG__LAKEHOUSE__S3__PATH_STYLE_ACCESS",
    }


def test_aws_settings_hold_no_credentials():
    """AC8: the AWS env example names no access key or secret; they come from the login."""
    aws = (ROOT / "infra/aws/lakehouse.env.example").read_text()
    assert not re.search(r"(ACCESS_KEY|SECRET|SESSION_TOKEN)\w*\s*=", aws)
    assert "secret-access-key" not in aws and "access-key-id" not in aws
