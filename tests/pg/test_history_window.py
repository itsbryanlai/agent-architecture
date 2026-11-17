"""The model's window of history, cut in SQL from the sizes stored on each row."""

from __future__ import annotations

import json

import pytest

from agentos.history_view import char_len, window
from agentos.stores import PostgresHistoryStore
from tests.pg.conftest import MIGRATIONS, TestDatabase


def text(role: str, body: str) -> tuple[str, list[dict]]:
    return role, [{"type": "text", "text": body}]


def tool_turn(n: int, size: int) -> list[tuple[str, list[dict]]]:
    """A user question, an assistant tool call, its result, and the answer."""
    return [
        text("user", f"question {n} " + "q" * size),
        ("assistant", [{"type": "tool_use", "id": f"u{n}", "name": "t", "input": {"n": n}}]),
        ("user", [{"type": "tool_result", "tool_use_id": f"u{n}", "content": "r" * size}]),
        text("assistant", f"answer {n} é " + "a" * size),
    ]


async def fill(store: PostgresHistoryStore, tenant: str, conversation: str, turns) -> None:
    step = 0
    for turn in turns:
        for role, content in turn:
            step += 1
            await store.append(tenant, conversation, f"s{step}", role, content)


@pytest.mark.parametrize("budget", [10, 700, 1500, 3000, 10_000_000], ids=str)
async def test_the_sql_window_is_the_window_of_the_whole_thread(pool, tenant: str, budget: int):
    """Fails if the SQL cut differs from cutting the whole loaded thread, at any budget."""
    store = PostgresHistoryStore(pool)
    await fill(store, tenant, "c", [tool_turn(n, 100) for n in range(12)])

    got = await store.load_window(tenant, "c", budget)

    assert got == window(await store.load(tenant, "c"), budget)
    if budget == 700:
        assert 0 < len(got) < 48  # a long thread yields only its tail


async def test_a_thread_with_no_turn_start_comes_back_whole(pool, tenant: str):
    """Fails if a thread with no user text, over budget, loses messages the old window kept."""
    store = PostgresHistoryStore(pool)
    await fill(store, tenant, "c", [[("assistant", [{"type": "text", "text": "x" * 50}])] * 6])

    assert len(await store.load_window(tenant, "c", 100)) == 6


async def test_append_records_the_size_the_window_counts(db: TestDatabase, pool, tenant: str):
    """Fails if a row's char_len differs from the size the in-memory window measures."""
    store = PostgresHistoryStore(pool)
    role, content = text("user", "héllo " * 20)
    await store.append(tenant, "c", "s1", role, content)
    with db.owner() as conn:
        (stored,) = conn.execute(
            "SELECT char_len FROM conversation_messages WHERE tenant_id = %s", (tenant,)
        ).fetchone()
    assert stored == char_len({"role": role, "content": content})


def test_the_migration_fills_the_size_of_rows_written_before_it(db: TestDatabase, tenant: str):
    """Fails if rows with no stored size keep size 0 after the migration, so a window ignores them."""
    role, content = text("user", "héllo " * 20)
    with db.owner() as conn:
        conn.execute(
            "INSERT INTO conversation_messages (tenant_id, conversation_id, seq, role, content) "
            "VALUES (%s, 'old', 1, %s, %s::jsonb)",
            (tenant, role, json.dumps(content)),
        )
        conn.execute((MIGRATIONS / "024_efficiency_retention.sql").read_text())
        (stored,) = conn.execute(
            "SELECT char_len FROM conversation_messages WHERE tenant_id = %s", (tenant,)
        ).fetchone()
    assert stored == char_len({"role": role, "content": content})
