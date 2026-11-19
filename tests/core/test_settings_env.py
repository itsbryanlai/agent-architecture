import pytest

from agentos.config import DEFAULT_TEMPORAL_ADDRESS, ConfigError, load_settings


def test_generic_infra_vars_are_ignored(monkeypatch):
    """Fails if DATABASE_URL or TEMPORAL_ADDRESS, set for another project, redirects the platform."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://other:other@remote.example:5432/other")
    monkeypatch.setenv("TEMPORAL_ADDRESS", "remote.example:7233")
    monkeypatch.delenv("AGENTOS_DATABASE_URL", raising=False)
    monkeypatch.delenv("AGENTOS_TEMPORAL_ADDRESS", raising=False)

    settings = load_settings()

    assert settings.database_url is None
    assert settings.temporal_address == DEFAULT_TEMPORAL_ADDRESS


def test_a_missing_database_url_is_an_error_that_names_the_variable(monkeypatch):
    """Fails if a process can start with no database URL, or the error names another variable."""
    monkeypatch.delenv("AGENTOS_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="AGENTOS_DATABASE_URL is not set"):
        load_settings().require("database_url")


def test_prefixed_infra_vars_are_read(monkeypatch):
    """Fails if the AGENTOS_-prefixed database and Temporal variables are not read."""
    monkeypatch.setenv("AGENTOS_DATABASE_URL", "postgresql://u:p@db.example:6543/agentos")
    monkeypatch.setenv("AGENTOS_TEMPORAL_ADDRESS", "temporal.example:7234")

    settings = load_settings()

    assert settings.require("database_url") == "postgresql://u:p@db.example:6543/agentos"
    assert settings.temporal_address == "temporal.example:7234"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, "files"), ("env", "env"), ("files", "files"), ("bogus", None)],
    ids=["unset", "env", "files", "unknown"],
)
def test_the_key_source_setting_is_read_and_an_unknown_source_is_refused(
    monkeypatch, raw, expected
):
    """Fails if AGENTOS_KEY_SOURCE is not read, defaults to variables, or accepts an unknown source."""
    if raw is None:
        monkeypatch.delenv("AGENTOS_KEY_SOURCE", raising=False)
    else:
        monkeypatch.setenv("AGENTOS_KEY_SOURCE", raw)
    if expected is None:
        with pytest.raises(ConfigError, match="AGENTOS_KEY_SOURCE"):
            load_settings()
    else:
        assert load_settings().key_source == expected
