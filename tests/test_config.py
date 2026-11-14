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


def test_admins_default_to_none_and_are_read_in_order():
    assert parse_agents_config(agents_dict()).admins == ()
    raw = agents_dict()
    raw["admins"] = ["U_B", "U_A"]
    assert parse_agents_config(raw).admins == ("U_B", "U_A")
    raw["admins"] = []
    assert parse_agents_config(raw).admins == ()


@pytest.mark.parametrize(
    ("admins", "fragment"),
    [
        ("U_A", "list"),
        ({"U_A": 1}, "list"),
        ([""], "non-empty"),
        (["  "], "non-empty"),
        ([" U_A"], "non-empty"),
        ([7], "non-empty"),
        ([None], "non-empty"),
        (["U_A", "U_B", "U_A"], "more than once"),
    ],
)
def test_bad_admins_are_rejected_with_the_tenant_named(admins, fragment):
    raw = agents_dict()
    raw["admins"] = admins
    with pytest.raises(ConfigError) as exc:
        parse_agents_config(raw)
    assert "acme" in str(exc.value)
    assert fragment in str(exc.value)
