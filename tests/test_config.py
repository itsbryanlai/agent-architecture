import pytest

from agentos.config import ConfigError, agent_definition, parse_agent, parse_agents_config
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


def _helper(**over) -> dict:
    return {**agents_dict()["agents"]["it-helper"], **over}


@pytest.mark.parametrize(("given", "limit"), [(None, 2), (1, 1), (10, 10)])
def test_max_writes_per_turn_defaults_to_two_and_round_trips(given, limit):
    """Fails if the write limit is lost between the definition and the agent, or a default is stored."""
    agent = parse_agent("it-helper", _helper(max_writes_per_turn=given), 1)

    assert agent.max_writes_per_turn == limit
    assert ("max_writes_per_turn" in agent_definition(agent)) == (limit != 2)
    assert parse_agent("it-helper", agent_definition(agent), 1) == agent


@pytest.mark.parametrize("bad", [0, 11, -1, 2.5, "3", True, [2]], ids=repr)
def test_max_writes_per_turn_outside_one_to_ten_is_refused(bad):
    """Fails if a limit that is no whole number from 1 to 10 is accepted."""
    with pytest.raises(ConfigError, match="max_writes_per_turn"):
        parse_agent("it-helper", _helper(max_writes_per_turn=bad), 1)
