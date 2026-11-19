"""The read-only schema check command, against Postgres."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from agentos.schema_version import EXPECTED_VERSIONS
from tests.pg.conftest import Databases


@pytest.mark.parametrize(
    ("break_it", "exit_code", "printed", "not_printed"),
    [
        ("SELECT 1", 0, f"schema current (latest {EXPECTED_VERSIONS[-1]})", "behind"),
        ("DELETE FROM schema_migrations WHERE version = '016'", 1, "016", "schema current"),
    ],
    ids=["current", "one migration missing"],
)
def test_schema_command_reports_current_or_names_the_missing_migration(
    databases: Databases, break_it: str, exit_code: int, printed: str, not_printed: str
):
    """Fails if the command reports current while a migration is missing."""
    database = databases.create(init=True)
    with database.owner() as conn:
        conn.execute(break_it)
    result = subprocess.run(
        [sys.executable, "-m", "agentos.schema_version"],
        env={**os.environ, "AGENTOS_DATABASE_URL": database.app_url},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == exit_code
    assert printed in result.stdout
    assert not_printed not in result.stdout
