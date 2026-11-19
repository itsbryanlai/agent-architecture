"""The Slack bot token resolver: the credential row first, the env variable as fallback."""

from __future__ import annotations

import secrets as token_source
from dataclasses import replace

import pytest

from agentos.config import ConfigError, TenantSecrets, parse_agents_config
from agentos.credentials import SLACK_BOT, InMemoryCredentialStore, TenantCredentials
from agentos.payload_codec import encrypt_with_tenant_key
from tests.conftest import TENANT, agents_dict
from tests.helpers.keys import RefreshableKeyStore

ROW_TOKEN = "xoxb-made-up-from-the-row"
ENV_TOKEN = "xoxb-made-up-from-the-env"


@pytest.mark.parametrize(
    "case",
    [
        "row wins over env",
        "no row falls back to env",
        "neither names the variable",
        "key miss reloads",
        "replaced row",
    ],
    ids=["row-wins", "env-fallback", "neither", "key-miss", "replaced"],
)
async def test_the_resolver_reads_the_row_first_then_the_env(case, monkeypatch):
    """Fails if the resolver prefers the env variable over a stored credential, serves a stale
    or undecryptable row, or guesses a token when neither source exists."""
    config = replace(
        parse_agents_config(agents_dict()),
        secrets=TenantSecrets(slack_bot_token_env="MY_SLACK"),
    )
    key = token_source.token_bytes(32)
    store = InMemoryCredentialStore()
    keys = RefreshableKeyStore({(TENANT, 1): key})
    monkeypatch.setenv("MY_SLACK", ENV_TOKEN)
    if case in ("row wins over env", "key miss reloads", "replaced row"):
        await store.put(
            TENANT, SLACK_BOT, encrypt_with_tenant_key(TENANT, key, ROW_TOKEN.encode()), 1
        )
    if case in ("row wins over env", "replaced row"):
        await keys.refresh()
    if case == "neither names the variable":
        monkeypatch.delenv("MY_SLACK", raising=False)
    credentials = TenantCredentials(store, keys)

    if case == "neither names the variable":
        with pytest.raises(ConfigError) as err:
            await credentials.slack_bot_token(config)
        assert "MY_SLACK" in str(err.value) and TENANT in str(err.value)
        assert ROW_TOKEN not in str(err.value)
        return

    expected = ENV_TOKEN if case == "no row falls back to env" else ROW_TOKEN
    assert await credentials.slack_bot_token(config) == expected
    if case == "replaced row":
        replaced = "xoxb-made-up-replacement"
        await store.put(
            TENANT, SLACK_BOT, encrypt_with_tenant_key(TENANT, key, replaced.encode()), 1
        )
        assert await credentials.slack_bot_token(config) == replaced
