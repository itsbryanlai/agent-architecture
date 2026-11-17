"""A guard for the pg tier: a new Postgres store must come with a test that builds it."""

from __future__ import annotations

from pathlib import Path

from tests.pg import conftest


def test_every_postgres_store_is_built_by_a_test_in_this_tier(request):
    """Fails if a Postgres*Store class under src/agentos is never constructed by a test in tests/pg."""
    ran = {Path(str(item.fspath)).name for item in request.session.items}
    all_modules = {path.name for path in Path(__file__).parent.glob("test_*.py")}
    if not all_modules <= ran:
        import pytest

        pytest.skip("only part of the pg tier was selected; the guard needs all of it")

    untested = sorted(set(conftest.postgres_store_classes()) - conftest.CONSTRUCTED)

    assert untested == []
