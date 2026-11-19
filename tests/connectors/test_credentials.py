"""The credential resolvers: the credential row first, the env variable as fallback."""

from __future__ import annotations

import secrets as token_source
from dataclasses import replace

import pytest

from agentos.config import ConfigError, TenantSecrets, parse_agents_config
from agentos.credentials import NOTION, SLACK_BOT, InMemoryCredentialStore, TenantCredentials
from agentos.payload_codec import encrypt_with_tenant_key
from tests.conftest import TENANT, agents_dict
from tests.helpers.keys import RefreshableKeyStore


@pytest.mark.parametrize("resolver", ["slack", "notion"])
@pytest.mark.parametrize(
    "case",
    [
        "row wins over env",
        "no row falls back to env",
        "neither names the variable",
        "key miss reloads",
        "replaced row",
        "undecryptable row",
    ],
    ids=["row-wins", "env-fallback", "neither", "key-miss", "replaced", "undecryptable"],
)
async def test_the_resolver_reads_the_row_first_then_the_env(resolver, case, monkeypatch):
    """Fails if either resolver prefers the env variable over a stored credential, serves a
    stale or undecryptable row, or guesses a token when neither source exists (Notion then
    returns None: the tenant is Slack-only)."""
    kind = SLACK_BOT if resolver == "slack" else NOTION
    env_field = "slack_bot_token_env" if resolver == "slack" else "notion_token_env"
    prefix = "xoxb" if resolver == "slack" else "ntn"
    row_token = f"{prefix}-made-up-from-the-row"
    env_token = f"{prefix}-made-up-from-the-env"
    config = replace(
        parse_agents_config(agents_dict()),
        secrets=TenantSecrets(**{env_field: "MY_SECRET"}),
    )
    key = token_source.token_bytes(32)
    store = InMemoryCredentialStore()
    keys = RefreshableKeyStore({(TENANT, 1): key})
    monkeypatch.setenv("MY_SECRET", env_token)
    if case in ("row wins over env", "key miss reloads", "replaced row", "undecryptable row"):
        await store.put(TENANT, kind, encrypt_with_tenant_key(TENANT, key, row_token.encode()), 1)
    if case in ("row wins over env", "replaced row"):
        await keys.refresh()
    if case == "undecryptable row":
        # The store refreshes with another key: the row no longer decrypts.
        keys = RefreshableKeyStore({(TENANT, 1): token_source.token_bytes(32)})
        await keys.refresh()
    if case == "neither names the variable":
        monkeypatch.delenv("MY_SECRET", raising=False)
    credentials = TenantCredentials(store, keys)
    resolve = credentials.slack_bot_token if resolver == "slack" else credentials.notion_token

    if case == "neither names the variable":
        if resolver == "slack":
            with pytest.raises(ConfigError) as err:
                await resolve(config)
            assert "MY_SECRET" in str(err.value) and TENANT in str(err.value)
            assert row_token not in str(err.value)
        else:
            assert await resolve(config) is None
        return
    if case == "undecryptable row":
        with pytest.raises(ConfigError) as err:
            await resolve(config)
        assert TENANT in str(err.value) and "version 1" in str(err.value)
        assert row_token not in str(err.value) and env_token not in str(err.value)
        return

    expected = env_token if case == "no row falls back to env" else row_token
    assert await resolve(config) == expected
    if case == "replaced row":
        replaced = f"{prefix}-made-up-replacement"
        await store.put(TENANT, kind, encrypt_with_tenant_key(TENANT, key, replaced.encode()), 1)
        assert await resolve(config) == replaced
