"""Members: the admins and creators a tenant added through the app.

A tenant has two roles. An admin approves every publish and manages members; a
creator drafts and submits agents. The first admins are not rows here: they are
the tenant YAML file's ``admins`` list, and ``control.py`` combines the two to
decide a user's role. This module only stores rows.

It is a module of its own, and not part of ``agent_store.py``, because members
and agents change for different reasons and are checked by different rules; the
write methods have names no other store uses, so the test that keeps writes
inside ``control.py`` can find every call.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos.stores import tenant_transaction

ADMIN = "admin"
CREATOR = "creator"
ROLES = (ADMIN, CREATOR)


class MemberError(ValueError):
    """The change names a member that does not exist, or one that already does."""


class MemberNotFound(MemberError):
    """The change names a user who is not a member."""


class InvalidMember(MemberError):
    """The user ID or the role is not one a member can have."""


@dataclass(frozen=True)
class MemberRecord:
    tenant_id: str
    user_id: str
    role: str
    added_by: str
    added_at: datetime | None = None


def check_member(user_id: str, role: str) -> None:
    if not isinstance(user_id, str) or not user_id.strip() or user_id != user_id.strip():
        raise InvalidMember("a member needs a Slack user ID")
    if role not in ROLES:
        raise InvalidMember(f"role must be one of {', '.join(ROLES)}")


class MemberStore(Protocol):
    async def get_member(self, tenant_id: str, user_id: str) -> MemberRecord | None: ...

    async def list_members(self, tenant_id: str) -> list[MemberRecord]:
        """Every member row of the tenant, by user ID. YAML admins are not rows."""
        ...

    async def add_member(self, tenant_id: str, user_id: str, role: str, added_by: str) -> None:
        """Insert a member. Refused with ``MemberError`` when the user already has a row."""
        ...

    async def set_member_role(self, tenant_id: str, user_id: str, role: str) -> None:
        """Change a member's role. Refused with ``MemberError`` when there is no row."""
        ...

    async def remove_member(self, tenant_id: str, user_id: str) -> None:
        """Delete a member's row. Refused with ``MemberError`` when there is no row."""
        ...


def _no_member(user_id: str) -> MemberNotFound:
    return MemberNotFound(f"'{user_id}' is not a member")


_COLUMNS = "tenant_id, user_id, role, added_by, added_at"


class PostgresMemberStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def get_member(self, tenant_id: str, user_id: str) -> MemberRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM members WHERE tenant_id = %s AND user_id = %s",
                (tenant_id, user_id),
            )
            row = await cur.fetchone()
        return None if row is None else MemberRecord(*row)

    async def list_members(self, tenant_id: str) -> list[MemberRecord]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM members WHERE tenant_id = %s ORDER BY user_id",
                (tenant_id,),
            )
            return [MemberRecord(*row) for row in await cur.fetchall()]

    async def add_member(self, tenant_id: str, user_id: str, role: str, added_by: str) -> None:
        check_member(user_id, role)
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO members (tenant_id, user_id, role, added_by) VALUES (%s, %s, %s, %s)
                ON CONFLICT (tenant_id, user_id) DO NOTHING
                RETURNING user_id
                """,
                (tenant_id, user_id, role, added_by),
            )
            if await cur.fetchone() is None:
                raise MemberError(f"'{user_id}' is already a member")

    async def set_member_role(self, tenant_id: str, user_id: str, role: str) -> None:
        check_member(user_id, role)
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                UPDATE members SET role = %s WHERE tenant_id = %s AND user_id = %s
                RETURNING user_id
                """,
                (role, tenant_id, user_id),
            )
            if await cur.fetchone() is None:
                raise _no_member(user_id)

    async def remove_member(self, tenant_id: str, user_id: str) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "DELETE FROM members WHERE tenant_id = %s AND user_id = %s RETURNING user_id",
                (tenant_id, user_id),
            )
            if await cur.fetchone() is None:
                raise _no_member(user_id)


class InMemoryMemberStore:
    def __init__(self) -> None:
        self._members: dict[tuple[str, str], MemberRecord] = {}

    async def get_member(self, tenant_id: str, user_id: str) -> MemberRecord | None:
        return self._members.get((tenant_id, user_id))

    async def list_members(self, tenant_id: str) -> list[MemberRecord]:
        return sorted(
            (m for (t, _), m in self._members.items() if t == tenant_id),
            key=lambda m: m.user_id,
        )

    async def add_member(self, tenant_id: str, user_id: str, role: str, added_by: str) -> None:
        check_member(user_id, role)
        if (tenant_id, user_id) in self._members:
            raise MemberError(f"'{user_id}' is already a member")
        self._members[(tenant_id, user_id)] = MemberRecord(
            tenant_id, user_id, role, added_by, datetime.now(UTC)
        )

    async def set_member_role(self, tenant_id: str, user_id: str, role: str) -> None:
        check_member(user_id, role)
        current = self._members.get((tenant_id, user_id))
        if current is None:
            raise _no_member(user_id)
        self._members[(tenant_id, user_id)] = replace(current, role=role)

    async def remove_member(self, tenant_id: str, user_id: str) -> None:
        if self._members.pop((tenant_id, user_id), None) is None:
            raise _no_member(user_id)
