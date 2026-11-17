"""Shared test helpers."""

from datetime import date
from decimal import Decimal

from agentos import ids
from agentos.usage import (
    PriceEntry,
)
from tests.conftest import ROOT_A, agents_dict
from tests.helpers.agents import helper_block

D = Decimal


def price(model="m", effective_from=date(2026, 1, 1), i="3", o="15", cr="0.3", cw="3.75"):
    return PriceEntry(model, effective_from, D(i), D(o), D(cr), D(cw))


def _page_agents():
    raw = agents_dict()
    raw["agents"]["it-helper"]["grants"]["notion"]["pages"] = [{"id": ROOT_A, "name": "IT wiki"}]
    return raw


SCOPE = ids.knowledge_scope(ROOT_A)


def block(team=None):
    definition = helper_block()
    if team:
        definition["team"] = team
    return definition
