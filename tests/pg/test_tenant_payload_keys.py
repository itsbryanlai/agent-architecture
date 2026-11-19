"""The database key store: wrapped keys, master rotation, concurrent creation, the audited command."""

from __future__ import annotations

import asyncio
import base64
import secrets

import pytest

from agentos.audit import PostgresAuditStore, event_json
from agentos.crypto_keys import (
    DatabaseKeyStore,
    MasterKeys,
    TenantKeyError,
    ensure_tenant_key,
    rotate_tenant_key,
)
from agentos.payload_codec import decrypt_for_tenant, encrypt_for_tenant
from agentos.stores import tenant_transaction
from agentos.tenant_keys import build_parser, execute_db
from agentos.unit_of_work import PostgresUnitOfWork


@pytest.mark.parametrize(
    ("row", "usable"),
    [("own", True), ("moved", False), ("malformed", False)],
    ids=["own key", "wrapped key moved from another tenant", "malformed wrapped key"],
)
async def test_the_store_serves_a_tenants_key_and_refuses_a_moved_or_malformed_wrapped_key(
    pool, tenant: str, other_tenant: str, row: str, usable: bool
):
    """Fails if a stored key does not round-trip for its own tenant, a wrapped key moved to another tenant's row or a malformed blob unwraps, or one bad row stops the other tenants' keys."""
    masters = MasterKeys({1: secrets.token_bytes(32)})
    await ensure_tenant_key(pool, masters, tenant)
    if row == "moved":
        async with tenant_transaction(pool, tenant) as conn:
            cur = await conn.execute(
                "SELECT wrapped_key, master_version FROM tenant_payload_keys "
                "WHERE tenant_id = %s AND version = 1",
                (tenant,),
            )
            blob, master_version = await cur.fetchone()
        async with tenant_transaction(pool, other_tenant) as conn:
            await conn.execute(
                "INSERT INTO tenant_payload_keys (tenant_id, version, wrapped_key, master_version) "
                "VALUES (%s, 1, %s, %s)",
                (other_tenant, blob, master_version),
            )
    elif row == "malformed":
        async with tenant_transaction(pool, other_tenant) as conn:
            await conn.execute(
                "INSERT INTO tenant_payload_keys (tenant_id, version, wrapped_key, master_version) "
                "VALUES (%s, 1, %s, 1)",
                (other_tenant, b"\x00"),
            )
    store = DatabaseKeyStore(pool, masters)
    await store.refresh()
    target = tenant if row == "own" else other_tenant
    if usable:
        blob, version = encrypt_for_tenant(store, target, b"a secret")
        assert decrypt_for_tenant(store, target, blob, version) == b"a secret"
    else:
        with pytest.raises(TenantKeyError) as err:
            store.key(target, 1)
        assert target in str(err.value)
        # The tenant with the good row keeps its own key.
        blob, version = encrypt_for_tenant(store, tenant, b"still works")
        assert decrypt_for_tenant(store, tenant, blob, version) == b"still works"


async def test_two_concurrent_creators_make_one_version_1(pool, tenant: str):
    """Fails if two concurrent ensure_tenant_key calls store two version-1 rows or return different versions."""
    masters = MasterKeys({1: secrets.token_bytes(32)})
    versions = await asyncio.gather(
        ensure_tenant_key(pool, masters, tenant), ensure_tenant_key(pool, masters, tenant)
    )
    assert versions == [1, 1]
    async with tenant_transaction(pool, tenant) as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM tenant_payload_keys WHERE tenant_id = %s", (tenant,)
        )
        assert (await cur.fetchone())[0] == 1


async def test_master_rotation_keeps_old_keys_readable_and_wraps_new_ones_with_the_newest(
    pool, tenant: str, other_tenant: str
):
    """Fails if a key wrapped under master v1 stops opening after v2 is added, or a new key is wrapped under an older master."""
    master1 = secrets.token_bytes(32)
    await ensure_tenant_key(pool, MasterKeys({1: master1}), tenant)
    first = DatabaseKeyStore(pool, MasterKeys({1: master1}))
    await first.refresh()
    old_blob, old_version = encrypt_for_tenant(first, tenant, b"written under master v1")
    assert old_version == 1

    masters2 = MasterKeys({1: master1, 2: secrets.token_bytes(32)})
    await ensure_tenant_key(pool, masters2, other_tenant)
    await rotate_tenant_key(pool, masters2, tenant)
    both = DatabaseKeyStore(pool, masters2)
    await both.refresh()
    assert both.versions(tenant) == [1, 2]
    assert decrypt_for_tenant(both, tenant, old_blob, old_version) == b"written under master v1"
    new_blob, new_version = encrypt_for_tenant(both, tenant, b"written after rotation")
    assert new_version == 2
    assert decrypt_for_tenant(both, tenant, new_blob, new_version) == b"written after rotation"

    # Only v2 present: the new keys open, the v1-wrapped one names the missing variable.
    only2 = DatabaseKeyStore(pool, MasterKeys({2: masters2.key(2)}))
    await only2.refresh()
    assert decrypt_for_tenant(only2, tenant, new_blob, new_version) == b"written after rotation"
    blob, version = encrypt_for_tenant(only2, other_tenant, b"a new tenant")
    assert decrypt_for_tenant(only2, other_tenant, blob, version) == b"a new tenant"
    with pytest.raises(TenantKeyError) as err:
        only2.key(tenant, 1)
    assert "AGENTOS_MASTER_KEY_V1" in str(err.value)


@pytest.mark.parametrize("command", ["create", "rotate"])
async def test_the_db_key_command_audits_the_operator_without_key_material(
    pool, tenant: str, command: str, capsys, monkeypatch
):
    """Fails if create or rotate in db mode leaves no operator audit, or prints or audits key material."""
    monkeypatch.setattr("agentos.tenant_keys.operator_identity", lambda: "operator:ops")
    masters = MasterKeys({1: secrets.token_bytes(32)})
    await ensure_tenant_key(pool, masters, tenant)
    audit = PostgresAuditStore(pool)
    args = build_parser().parse_args([command, tenant])

    assert (
        await execute_db(args, pool, masters, audit, PostgresUnitOfWork(pool), request_id="inv-1")
        == 0
    )

    events = await audit.list(tenant)
    assert [(e.kind, e.tool, e.identity) for e in events] == [
        ("payload_key", command, "operator:ops")
    ]
    store = DatabaseKeyStore(pool, masters)
    await store.refresh()
    materials = [base64.b64encode(store.key(tenant, v)).decode() for v in store.versions(tenant)]
    captured = capsys.readouterr()
    assert all(m not in captured.out + captured.err for m in materials)
    assert all(m not in event_json(e) for m in materials for e in events)
