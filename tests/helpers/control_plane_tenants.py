"""A tenant file with the given admins."""

from agentos.config import TenantsConfig, parse_agents_config
from tests.conftest import agents_dict


def tenants(*admins: str) -> TenantsConfig:
    raw = agents_dict()
    raw["admins"] = list(admins)
    return TenantsConfig.of(parse_agents_config(raw))
