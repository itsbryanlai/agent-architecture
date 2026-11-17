"""Helpers that build agent definitions and publish them in a store."""

import copy

from tests.conftest import TENANT, agents_dict

ADMIN = "U_ADMIN"


EDITOR = "U_EDITOR"


def helper_block() -> dict:
    block = copy.deepcopy(agents_dict()["agents"]["it-helper"])
    del block["version"]
    return block


async def make_published(store, agent_id="it-helper", definition=None, tenant=TENANT):
    record = await store.create_agent(tenant, agent_id, definition or helper_block(), EDITOR)
    await store.submit(tenant, agent_id, record.version, EDITOR)
    return await store.publish(tenant, agent_id, record.version, ADMIN)


async def republish(store, agent_id, change, tenant=TENANT):
    """Draft from the published version, apply ``change`` to its definition, publish."""
    draft = await store.create_draft(tenant, agent_id, EDITOR)
    definition = copy.deepcopy(draft.definition)
    change(definition)
    await store.replace_draft(tenant, agent_id, draft.version, definition)
    await store.submit(tenant, agent_id, draft.version, EDITOR)
    return await store.publish(tenant, agent_id, draft.version, ADMIN)
