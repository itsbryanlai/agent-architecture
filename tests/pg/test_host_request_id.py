"""Host commands that write an audit event take --request-id and print the one they generated."""

from __future__ import annotations

import base64
import re
import secrets
from datetime import UTC, datetime

import pytest

from agentos import notion_webhook, retention, switches, tenant_keys, usage_reprice
from agentos.agent_store import PostgresAgentStore
from agentos.audit import PostgresAuditStore
from agentos.crypto_keys import EnvKeyStore, MasterKeys, ensure_tenant_key, key_var_name
from agentos.payload_codec import encrypt_for_tenant
from agentos.unit_of_work import PostgresUnitOfWork
from agentos.usage import PostgresUsageStore, UsageEvent
from tests.pg.conftest import TestDatabase

PRICES = "prices:\n  - {model: m, effective_from: 2026-01-01, input: 3, output: 0, cache_read: 0, cache_write: 0}\n"
AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


async def _switches(ctx, request_id):
    argv = ["--tenant", ctx.tenant, "{}", "--writes"] + request_id
    on = switches.build_parser().parse_args([argv[0], argv[1], "on", "--writes"])
    off = switches.build_parser().parse_args([*argv[:2], "off", "--writes", *request_id])
    stores = (
        switches.PostgresSwitchStore(ctx.pool),
        PostgresAuditStore(ctx.pool),
        PostgresAgentStore(ctx.pool),
        PostgresUnitOfWork(ctx.pool),
    )
    await switches.execute(on, *stores)  # back on, so the next off is a real change
    return await switches.execute(off, *stores)


async def _tenant_keys(ctx, request_id):
    args = tenant_keys.build_parser().parse_args(["rotate", ctx.tenant, *request_id])
    return await tenant_keys.execute_db(
        args, ctx.pool, ctx.masters, PostgresAuditStore(ctx.pool), PostgresUnitOfWork(ctx.pool)
    )


async def _webhook(ctx, request_id):
    await ctx.webhook_store.put_pending(
        ctx.tenant, *encrypt_for_tenant(ctx.env_keys, ctx.tenant, b"token")
    )
    args = notion_webhook.build_parser().parse_args(["--tenant", ctx.tenant, "show", *request_id])
    return await notion_webhook.execute(
        args,
        ctx.webhook_store,
        PostgresAuditStore(ctx.pool),
        ctx.env_keys,
        PostgresUnitOfWork(ctx.pool),
    )


async def _retention(ctx, request_id):
    args = retention.build_set_parser().parse_args(
        [
            "--tenant",
            ctx.tenant,
            "--url-var",
            "TEST_OWNER_URL",
            "--messages-days",
            "200",
            *request_id,
        ]
    )
    return await retention.run_set(args)


async def _reprice(ctx, request_id):
    ctx.rows += 1
    await PostgresUsageStore(ctx.pool).record(
        UsageEvent(
            ctx.tenant, f"k{ctx.rows}", "llm", "aa", 1, "m", 1_000_000, 0, 0, 0, None, "c", AT
        )
    )
    args = usage_reprice.build_parser().parse_args(
        ["reprice", "--tenant", ctx.tenant, "--month", "2026-11", "--yes", "--prices", ctx.prices]
        + ["--url-var", "TEST_OWNER_URL", *request_id]
    )
    return await usage_reprice.run(args)


COMMANDS = [
    (_switches, "switch_off"),
    (_tenant_keys, "payload_key"),
    (_webhook, "webhook_token"),
    (_retention, "retention_set"),
    (_reprice, "usage_repriced"),
]


@pytest.mark.parametrize(("attempt", "kind"), COMMANDS, ids=[c[0].__name__[1:] for c in COMMANDS])
async def test_a_retry_with_the_printed_request_id_records_one_audit_event(
    db: TestDatabase, pool, tenant: str, tmp_path, monkeypatch, capsys, attempt, kind
):
    """Fails if a host command does not print its generated request ID, or a retry passing it records a second audit event."""
    monkeypatch.setenv("TEST_OWNER_URL", db.owner_url)
    with db.owner() as c:
        c.execute(
            "INSERT INTO tenants (tenant_id, slack_team_id, default_agent) VALUES (%s, %s, 'aa')",
            (tenant, f"T-{tenant}"),
        )
    masters = MasterKeys({1: secrets.token_bytes(32)})
    await ensure_tenant_key(pool, masters, tenant)
    (tmp_path / "prices.yaml").write_text(PRICES)
    ctx = type("Ctx", (), {})()
    ctx.tenant, ctx.pool, ctx.masters, ctx.rows = tenant, pool, masters, 0
    ctx.prices = str(tmp_path / "prices.yaml")
    ctx.env_keys = EnvKeyStore(
        {key_var_name(tenant, 1): base64.b64encode(secrets.token_bytes(32)).decode()}
    )
    ctx.webhook_store = notion_webhook.PostgresWebhookTokenStore(pool)

    assert await attempt(ctx, []) == 0
    # A command that first resets state (the switch) prints an ID for that step too; the last is the change's.
    printed = re.findall(
        r"request id: (\w+); pass --request-id \1 to retry this exact change",
        capsys.readouterr().err,
    )
    assert printed
    assert await attempt(ctx, ["--request-id", printed[-1]]) == 0

    events = [e for e in await PostgresAuditStore(pool).list(tenant) if e.kind == kind]
    assert len(events) == 1


async def test_a_retried_rotate_with_its_request_id_adds_one_key_version(
    db: TestDatabase, pool, tenant: str
):
    """Fails if retrying a tenant key rotate with the same request ID adds a second version."""
    with db.owner() as c:
        c.execute(
            "INSERT INTO tenants (tenant_id, slack_team_id, default_agent) VALUES (%s, %s, 'aa')",
            (tenant, f"T-{tenant}"),
        )
    masters = MasterKeys({1: secrets.token_bytes(32)})
    await ensure_tenant_key(pool, masters, tenant)
    args = tenant_keys.build_parser().parse_args(["rotate", tenant, "--request-id", "r1"])
    for _ in range(2):
        assert (
            await tenant_keys.execute_db(
                args, pool, masters, PostgresAuditStore(pool), PostgresUnitOfWork(pool)
            )
            == 0
        )

    with db.owner() as c:
        versions = c.execute(
            "SELECT count(*) FROM tenant_payload_keys WHERE tenant_id = %s", (tenant,)
        ).fetchone()[0]
    assert versions == 2
