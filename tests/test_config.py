import pytest

from agentos.config import ConfigError, parse_agents_config
from tests.conftest import agents_dict


def test_config_rejects_missing_property_mapping():
    raw = agents_dict()
    db = raw["agents"]["it-helper"]["grants"]["notion"]["databases"][0]
    del db["properties"]["request_id"]
    with pytest.raises(ConfigError) as exc:
        parse_agents_config(raw)
    message = str(exc.value)
    assert "it-helper" in message
    assert "request_id" in message
