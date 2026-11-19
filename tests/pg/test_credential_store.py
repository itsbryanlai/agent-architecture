"""The credential store, one contract run against the in-memory twin and the Postgres store."""

from __future__ import annotations

import pytest

from agentos.credentials import NOTION, SLACK_BOT, InMemoryCredentialStore, PostgresCredentialStore


@pytest.fixture
def store(store_pool):
    return InMemoryCredentialStore() if store_pool is None else PostgresCredentialStore(store_pool)


@pytest.mark.parametrize("kind", [SLACK_BOT, NOTION], ids=["slack_bot", "notion"])
async def test_a_credential_round_trips_and_a_reinstall_replaces_it(store, tenant: str, kind: str):
    """Fails if a stored credential does not read back, a second put keeps the old value, or
    the database refuses a kind the code stores."""
    assert await store.get(tenant, kind) is None

    await store.put(tenant, kind, b"\x01first", 1)
    row = await store.get(tenant, kind)
    assert (row.tenant_id, row.kind, row.ciphertext, row.key_version) == (
        tenant,
        kind,
        b"\x01first",
        1,
    )

    await store.put(tenant, kind, b"\x02second", 2)
    row = await store.get(tenant, kind)
    assert (row.ciphertext, row.key_version) == (b"\x02second", 2)


async def test_another_tenants_credential_is_invisible(store, tenant: str, other_tenant: str):
    """Fails if a tenant can read a credential stored for another tenant."""
    await store.put(other_tenant, SLACK_BOT, b"\x01secret", 1)

    assert await store.get(tenant, SLACK_BOT) is None


async def test_delete_removes_only_the_named_credential(store, tenant: str):
    """Fails if a delete keeps the row, removes another kind's row, or reports a second
    delete of the same row as a change."""
    await store.put(tenant, NOTION, b"\x01notion", 1)
    await store.put(tenant, SLACK_BOT, b"\x01slack", 1)

    assert await store.delete(tenant, NOTION) is True

    assert await store.get(tenant, NOTION) is None
    assert (await store.get(tenant, SLACK_BOT)).ciphertext == b"\x01slack"
    assert await store.delete(tenant, NOTION) is False
