"""The credential store, one contract run against the in-memory twin and the Postgres store."""

from __future__ import annotations

import pytest

from agentos.credentials import SLACK_BOT, InMemoryCredentialStore, PostgresCredentialStore


@pytest.fixture
def store(store_pool):
    return InMemoryCredentialStore() if store_pool is None else PostgresCredentialStore(store_pool)


async def test_a_credential_round_trips_and_a_reinstall_replaces_it(store, tenant: str):
    """Fails if a stored credential does not read back, or a second put keeps the old value."""
    assert await store.get(tenant, SLACK_BOT) is None

    await store.put(tenant, SLACK_BOT, b"\x01first", 1)
    row = await store.get(tenant, SLACK_BOT)
    assert (row.tenant_id, row.kind, row.ciphertext, row.key_version) == (
        tenant,
        SLACK_BOT,
        b"\x01first",
        1,
    )

    await store.put(tenant, SLACK_BOT, b"\x02second", 2)
    row = await store.get(tenant, SLACK_BOT)
    assert (row.ciphertext, row.key_version) == (b"\x02second", 2)


async def test_another_tenants_credential_is_invisible(store, tenant: str, other_tenant: str):
    """Fails if a tenant can read a credential stored for another tenant."""
    await store.put(other_tenant, SLACK_BOT, b"\x01secret", 1)

    assert await store.get(tenant, SLACK_BOT) is None
