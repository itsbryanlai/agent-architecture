import copy
from pathlib import Path

import pytest

from agentos.config import (
    ConfigError,
    agent_definition,
    load_agents_config,
    parse_agent,
    parse_agents_config,
)
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


# --- agent IDs and the strictness of a definition -------------------------------------------


def block() -> dict:
    """A valid definition, as the database stores it (no version)."""
    definition = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del definition["version"]
    return definition


@pytest.mark.parametrize(
    ("agent_id", "ok"),
    [
        ("it-helper", True),
        ("a1", True),
        ("a" + "b" * 39, True),
        ("a", False),
        ("a" * 41, False),
        ("1helper", False),
        ("-helper", False),
        ("It-helper", False),
        ("it_helper", False),
        ("a/b", False),
        ("a:b", False),
        ("a b", False),
        ("it-hеlper", False),  # the e is Cyrillic
        ("ab\n", False),
        ("", False),
    ],
    ids=repr,
)
def test_an_agent_id_outside_the_pattern_is_refused(agent_id: str, ok: bool):
    """Fails if an ID the console cannot route, that looks like another, or that has a colon is
    accepted, or if a plain one is refused."""
    if ok:
        assert parse_agent(agent_id, block(), 1).agent_id == agent_id
    else:
        with pytest.raises(ConfigError, match="agent ID"):
            parse_agent(agent_id, block(), 1)


def _set(path: str, value):
    """A change to a definition: set the dotted ``path`` (a list index is a number)."""

    def apply(definition: dict) -> None:
        *parents, last = path.split(".")
        node = definition
        for key in parents:
            node = node[int(key)] if isinstance(node, list) else node[key]
        if isinstance(node, list):
            node[int(last)] = value
        else:
            node[last] = value

    return apply


def _drop(path: str):
    def apply(definition: dict) -> None:
        *parents, last = path.split(".")
        node = definition
        for key in parents:
            node = node[int(key)] if isinstance(node, list) else node[key]
        del node[last]

    return apply


DB = "grants.notion.databases.0"
BAD_DEFINITIONS = [
    # An unknown key at the top, inside a grant, and three levels down.
    (_set("audiance", {"dm": "everyone"}), "unknown field 'audiance'"),
    (_set("grants.notion.tables", []), "grants.notion: unknown field 'tables'"),
    (_set(f"{DB}.properties.titel", "Name"), "unknown field 'titel'"),
    (_set(f"{DB}.colour", "red"), "unknown field 'colour'"),
    (_set("follow_up.poll_every", 5), "follow_up: unknown field 'poll_every'"),
    (_set("approval.expire", 5), "approval: unknown field 'expire'"),
    # The wrong type: a bool for an int, text for a number or a list, a number for text.
    (_set("max_steps", True), "'max_steps' must be a whole number"),
    (_set("max_steps", "6"), "'max_steps' must be a whole number"),
    (_set("max_steps", 6.0), "'max_steps' must be a whole number"),
    (_set("follow_up.stale_after_days", True), "'stale_after_days' must be a whole number"),
    (_set(f"{DB}.properties.done_values", "Done"), "'done_values' must be a list"),
    (_set("specialists", "other-agent"), "'specialists' must be a list"),
    (_set("name", 7), "'name' must be non-empty text"),
    (_set("description", ["a"]), "'description' must be text"),
    (_set("grants.notion.databases", {"id": "x"}), "expected a list"),
    (_set("grants", []), "grants: expected a mapping"),
    # Out of range.
    (_set("max_steps", 0), "from 1 to 50"),
    (_set("max_steps", 51), "from 1 to 50"),
    (_set("follow_up.poll_interval_minutes", 0), "'poll_interval_minutes' must be"),
    (_set("follow_up.stale_after_days", 100000), "'stale_after_days' must be"),
    (_set("approval.expires_after_hours", -1), "'expires_after_hours' must be"),
    (_set("follow_up.nudge", "yes"), "'nudge' must be true or false"),
    (_set("follow_up.max_nudges", True), "'max_nudges' must be a whole number"),
    (_set("follow_up.max_nudges", 11), "'max_nudges' must be"),
    (_set(f"{DB}.properties.assignee", 7), "'assignee' must be non-empty text"),
    (_drop("max_steps"), "missing required field 'max_steps'"),
]


@pytest.mark.parametrize(("change", "fragment"), BAD_DEFINITIONS, ids=lambda v: str(v)[:40])
def test_a_definition_with_a_stray_key_a_wrong_type_or_a_number_out_of_range_is_refused(
    change, fragment: str
):
    """Fails if a misspelt key reads as 'not set', a bool or a string passes as a number or a
    list, or a number outside its bounds is accepted, or the error does not say where."""
    definition = block()
    change(definition)
    with pytest.raises(ConfigError) as raised:
        parse_agent("it-helper", definition, 1)
    assert fragment in str(raised.value)
    assert "it-helper" in str(raised.value)


def test_the_shipped_agent_files_still_parse():
    """Fails if a tracked agent file would be refused by the strict definition rules."""
    for name in ("agents.yaml", "agents.example.yaml"):
        path = Path(__file__).resolve().parents[2] / "config" / name
        if path.exists():
            assert load_agents_config(path).agents


def _helper(**over) -> dict:
    return {**agents_dict()["agents"]["it-helper"], **over}


@pytest.mark.parametrize("limit", [1, 10])
def test_max_writes_per_turn_round_trips(limit):
    """Fails if the write limit is lost between the definition and the agent."""
    agent = parse_agent("it-helper", _helper(max_writes_per_turn=limit), 1)

    assert agent.max_writes_per_turn == limit
    assert agent_definition(agent)["max_writes_per_turn"] == limit
    assert parse_agent("it-helper", agent_definition(agent), 1) == agent


@pytest.mark.parametrize("bad", [0, 11, -1, 2.5, "3", True, [2]], ids=repr)
def test_max_writes_per_turn_outside_one_to_ten_is_refused(bad):
    """Fails if a limit that is no whole number from 1 to 10 is accepted."""
    with pytest.raises(ConfigError, match="max_writes_per_turn"):
        parse_agent("it-helper", _helper(max_writes_per_turn=bad), 1)


def test_the_assignee_and_nudge_fields_round_trip():
    """Fails if the assignee mapping or the nudge policy is lost between the definition and the agent."""
    definition = block()
    db = definition["grants"]["notion"]["databases"][0]
    db["properties"]["assignee"] = "Assignee"
    db["property_ids"]["assignee"] = "asg1"
    definition["follow_up"] = {**definition["follow_up"], "nudge": False, "max_nudges": 5}

    agent = parse_agent("it-helper", definition, 1)

    grant = agent.grants.notion_databases[0]
    assert (grant.assignee_prop, grant.assignee_prop_id) == ("Assignee", "asg1")
    assert (agent.follow_up.nudge, agent.follow_up.max_nudges) == (False, 5)
    assert parse_agent("it-helper", agent_definition(agent), 1) == agent
