from urllib.parse import urlparse

from agentos.config import (
    DEFAULT_DATABASE_URL,
    DEFAULT_TEMPORAL_ADDRESS,
    load_settings,
)


def test_generic_infra_vars_are_ignored(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://other:other@remote.example:5432/other")
    monkeypatch.setenv("TEMPORAL_ADDRESS", "remote.example:7233")
    monkeypatch.delenv("AGENTOS_DATABASE_URL", raising=False)
    monkeypatch.delenv("AGENTOS_TEMPORAL_ADDRESS", raising=False)

    settings = load_settings()

    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.temporal_address == DEFAULT_TEMPORAL_ADDRESS


def test_prefixed_infra_vars_are_read(monkeypatch):
    monkeypatch.setenv("AGENTOS_DATABASE_URL", "postgresql://u:p@db.example:6543/agentos")
    monkeypatch.setenv("AGENTOS_TEMPORAL_ADDRESS", "temporal.example:7234")

    settings = load_settings()

    assert settings.database_url == "postgresql://u:p@db.example:6543/agentos"
    assert settings.temporal_address == "temporal.example:7234"


def test_default_database_url_uses_port_55432():
    assert urlparse(DEFAULT_DATABASE_URL).port == 55432
