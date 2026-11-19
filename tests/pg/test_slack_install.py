"""The real install over Postgres: the tenant, key, credential and audit commit together."""

from __future__ import annotations

import secrets
import uuid

import pytest

from agentos import ids
from agentos.audit import PostgresAuditStore, event_json
from agentos.credentials import PostgresCredentialStore, TenantCredentials
from agentos.crypto_keys import DatabaseKeyStore, MasterKeys, tenant_key_material
from agentos.slack_install import InstallConfig, Installer
from agentos.stores import tenant_transaction
from agentos.tenant_store import PostgresTenantStore
from agentos.unit_of_work import PostgresUnitOfWork

BOT_TOKEN = "xoxb-made-up-pg-install"
INSTALLER = "U_INSTALLER"


class RecordThenFail(PostgresAuditStore):
    """Writes the event on the unit's connection, then fails as a lost connection would."""

    async def record(self, event, conn=None):
        await super().record(event, conn=conn)
        raise RuntimeError("audit failed after the insert")


def installer_for(pool, team: str, masters: MasterKeys, audit: PostgresAuditStore) -> Installer:
    return Installer(
        InstallConfig(
            client_id="1111.2222",
            client_secret="made-up-client-secret",
            console_url="https://console.example.com",
            allowed_teams=frozenset({team}),
            scopes=("chat:write",),
        ),
        object(),  # install() is driven directly; the exchange is the callback's job
        PostgresTenantStore(pool),
        TenantCredentials(PostgresCredentialStore(pool), DatabaseKeyStore(pool, masters)),
        audit,
        PostgresUnitOfWork(pool),
        ensure_key=lambda tenant_id, conn: tenant_key_material(pool, masters, tenant_id, conn=conn),
    )


@pytest.mark.parametrize("outcome", ["success", "audit fails"], ids=["success", "audit-fails"])
async def test_the_install_commits_the_tenant_key_token_and_audit_together(pool, outcome):
    """Fails if the install commits the tenant row before the key, token and audit, so a
    failed audit insert leaves a tenant with no key or token behind."""
    team = f"T{uuid.uuid4().hex[:8].upper()}"
    tenant_id = ids.tenant_for_slack_team(team)
    masters = MasterKeys({1: secrets.token_bytes(32)})
    audit = RecordThenFail(pool) if outcome == "audit fails" else PostgresAuditStore(pool)

    if outcome == "audit fails":
        with pytest.raises(RuntimeError):
            await installer_for(pool, team, masters, audit).install(team, BOT_TOKEN, INSTALLER)
        assert await PostgresTenantStore(pool).load(tenant_id) is None
        async with tenant_transaction(pool, tenant_id) as conn:
            counts = []
            for table in ("tenant_payload_keys", "tenant_credentials", "audit_events"):
                cur = await conn.execute(
                    f"SELECT count(*) FROM {table} WHERE tenant_id = %s", (tenant_id,)
                )
                counts.append((await cur.fetchone())[0])
        assert counts == [0, 0, 0]
        # The retry is not refused as "already done": it creates the whole tenant once.
        result = await installer_for(pool, team, masters, PostgresAuditStore(pool)).install(
            team, BOT_TOKEN, INSTALLER
        )
        assert result.created is True
    else:
        result = await installer_for(pool, team, masters, audit).install(team, BOT_TOKEN, INSTALLER)
        assert result.created is True

    assert result.tenant_id == tenant_id
    row = await PostgresTenantStore(pool).load(tenant_id)
    assert (row.config.slack_team_id, row.config.admins, row.config.default_agent) == (
        team,
        (INSTALLER,),
        None,
    )
    keys = DatabaseKeyStore(pool, masters)
    await keys.refresh()
    assert keys.versions(tenant_id) == [1]
    credentials = TenantCredentials(PostgresCredentialStore(pool), keys)
    assert await credentials.slack_bot_token(row.config) == BOT_TOKEN
    events = await PostgresAuditStore(pool).list(tenant_id)
    assert [(e.kind, e.tool, e.requester_user_id, e.target) for e in events] == [
        ("install", "install", INSTALLER, team)
    ]
    assert BOT_TOKEN not in event_json(events[0])
