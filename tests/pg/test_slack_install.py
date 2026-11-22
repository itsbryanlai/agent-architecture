"""The real install over Postgres: the tenant, key, credential and audit commit together."""

from __future__ import annotations

import json
import secrets
import uuid

import httpx
import pytest
from slack_sdk.signature import SignatureVerifier

from agentos import ids
from agentos.audit import PostgresAuditStore, event_json
from agentos.config import Settings
from agentos.credentials import NOTION, SLACK_BOT, PostgresCredentialStore, TenantCredentials
from agentos.crypto_keys import DatabaseKeyStore, MasterKeys, tenant_key_material
from agentos.gateway.app import create_app
from agentos.slack_install import InstallConfig, Installer
from agentos.stores import InMemoryTaskLinkStore, tenant_transaction
from agentos.tenant_store import PostgresTenantStore, TenantDirectory
from agentos.unit_of_work import PostgresUnitOfWork
from tests.helpers.gateway import FakeTemporalClient, signed_headers

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


async def test_the_uninstall_event_disables_and_deletes_as_the_app_role(pool):
    """Fails if the gateway's uninstall unit of work cannot disable the tenant, delete its
    Slack credential and audit once as the app role, or a repeat event changes anything."""
    team = f"T{uuid.uuid4().hex[:8].upper()}"
    tenant_id = ids.tenant_for_slack_team(team)
    masters = MasterKeys({1: secrets.token_bytes(32)})
    await installer_for(pool, team, masters, PostgresAuditStore(pool)).install(
        team, BOT_TOKEN, INSTALLER
    )
    keys = DatabaseKeyStore(pool, masters)
    await keys.refresh()
    credentials = TenantCredentials(PostgresCredentialStore(pool), keys)
    await credentials.store_with_current_key(tenant_id, NOTION, "ntn-made-up-kept")

    secret = "made-up-signing-secret"
    app = create_app(
        Settings(),
        TenantDirectory(PostgresTenantStore(pool)),
        FakeTemporalClient(),
        InMemoryTaskLinkStore(),
        SignatureVerifier(secret),
        tenant_store=PostgresTenantStore(pool),
        credentials=credentials,
        audit=PostgresAuditStore(pool),
        uow=PostgresUnitOfWork(pool),
    )
    body = json.dumps(
        {
            "type": "event_callback",
            "team_id": team,
            "event_id": "EvU1",
            "event": {"type": "app_uninstalled"},
        }
    ).encode()
    headers = signed_headers(secret, body, "application/json")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://gateway.example.com"
    ) as client:
        assert (
            await client.post("/slack/events", content=body, headers=headers)
        ).status_code == 200
        # A Slack retry of the same event changes nothing more.
        assert (
            await client.post("/slack/events", content=body, headers=headers)
        ).status_code == 200

    ref = await PostgresTenantStore(pool).ref_for_slack_team(team)
    assert ref.enabled is False
    assert await PostgresCredentialStore(pool).get(tenant_id, SLACK_BOT) is None
    assert await PostgresCredentialStore(pool).get(tenant_id, NOTION) is not None
    events = await PostgresAuditStore(pool).list(tenant_id)
    uninstalls = [e for e in events if e.kind == "uninstall"]
    assert [(e.tool, e.target, e.identity) for e in uninstalls] == [
        ("app_uninstalled", team, "system:uninstall")
    ]
    assert BOT_TOKEN not in event_json(uninstalls[0])
    assert {e.kind for e in events} == {"install", "uninstall"}
