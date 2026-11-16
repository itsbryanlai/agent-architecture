"""Connection pool and worker concurrency settings."""

import pytest

from agentos.config import ConfigError, load_settings, validate_worker_capacity


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (
        "AGENTOS_DB_POOL_MIN",
        "AGENTOS_DB_POOL_MAX",
        "AGENTOS_MAX_CONCURRENT_ACTIVITIES",
    ):
        monkeypatch.delenv(name, raising=False)


def test_the_defaults_let_every_activity_have_a_connection():
    """Fails if the shipped defaults start a worker whose activities would queue for connections."""
    validate_worker_capacity(load_settings())


@pytest.mark.parametrize(
    ("pool_min", "pool_max", "activities"),
    [("2", "8", "20"), ("10", "5", "4")],
    ids=["pool smaller than the activities", "pool max below its min"],
)
def test_a_pool_too_small_for_the_worker_is_refused(monkeypatch, pool_min, pool_max, activities):
    """Fails if the worker starts with fewer connections than activities, or max below min."""
    monkeypatch.setenv("AGENTOS_DB_POOL_MIN", pool_min)
    monkeypatch.setenv("AGENTOS_DB_POOL_MAX", pool_max)
    monkeypatch.setenv("AGENTOS_MAX_CONCURRENT_ACTIVITIES", activities)

    with pytest.raises(ConfigError, match="AGENTOS_DB_POOL_MAX"):
        validate_worker_capacity(load_settings())


def test_pool_and_activity_sizes_come_from_the_environment(monkeypatch):
    """Fails if the pool size and activity limit the operator sets are ignored."""
    monkeypatch.setenv("AGENTOS_DB_POOL_MIN", "3")
    monkeypatch.setenv("AGENTOS_DB_POOL_MAX", "12")
    monkeypatch.setenv("AGENTOS_MAX_CONCURRENT_ACTIVITIES", "10")

    settings = load_settings()

    assert settings.pool_args() == {"min_size": 3, "max_size": 12}
    assert settings.max_concurrent_activities == 10
    validate_worker_capacity(settings)


@pytest.mark.parametrize("raw", ["0", "-2", "many"])
def test_a_pool_size_that_is_not_a_positive_number_is_refused(monkeypatch, raw):
    """Fails if a zero, negative or non-numeric pool size is accepted."""
    monkeypatch.setenv("AGENTOS_DB_POOL_MAX", raw)

    with pytest.raises(ConfigError, match="AGENTOS_DB_POOL_MAX"):
        load_settings()
