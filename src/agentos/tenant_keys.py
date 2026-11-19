"""Create or rotate a tenant's payload encryption key.

    uv run python -m agentos.tenant_keys create <tenant_id>
    uv run python -m agentos.tenant_keys rotate <tenant_id>
    uv run python -m agentos.tenant_keys value <tenant_id> [--next]
    uv run python -m agentos.tenant_keys master-value [--next]

With the ``files`` source, ``create`` and ``rotate`` print the path of the new
key file and nothing else. They never overwrite an existing key: losing a key
makes that tenant's stored workflow data unreadable. ``rotate`` adds the next
version; the next payload any process encodes uses it, so copy the file to every
host that reads the key directory first. Older versions stay so existing
histories still decode. Never delete an old version while any history written
under it is kept.

With the ``db`` source, ``create`` and ``rotate`` write the tenant's wrapped key
to the database instead (``create`` is idempotent: an existing key is left
untouched), print the key version, and audit the operator as ``payload_key``.
No key material is printed or audited. ``value`` is refused in this mode: the
tenant's key lives in the database, not in a host variable.

``value`` is for a host that keeps keys in variables instead of files (see
docs/operations.md): it prints one new key value as
``AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>=<base64>`` for the operator to paste into the
host's variables, and a warning on stderr. It never reads or prints an existing
key and writes no file; ``--next`` names the version after the tenant's newest.

``master-value`` does the same for the master key of the ``db`` source: it
prints one new ``AGENTOS_MASTER_KEY_V<n>=<base64>`` value, once, and stores it
nowhere. ``--next`` names the version after the newest master variable, for a
master rotation; every older master version stays while a tenant key wrapped
under it is kept.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import secrets
import sys
import uuid

from psycopg_pool import AsyncConnectionPool

from agentos import ids
from agentos.audit import AuditStore
from agentos.config import (
    KEY_SOURCE_DB,
    MASTER_KEY_ENV_PREFIX,
    TENANT_ID_PATTERN,
    Settings,
    load_settings,
    master_key_env,
)
from agentos.crypto_keys import (
    KEY_BYTES,
    KeyExists,
    LocalKeyStore,
    MasterKeys,
    TenantKeyError,
    build_key_store,
    create_key,
    ensure_tenant_key,
    key_var_name,
    master_keys_from_env,
    rotate_tenant_key,
)
from agentos.models import AuditEvent
from agentos.switches import operator_identity
from agentos.unit_of_work import PostgresUnitOfWork, UnitOfWork


def _print_value(args: argparse.Namespace, key_dir: str | None) -> int:
    store = (
        LocalKeyStore(key_dir)
        if key_dir is not None
        else asyncio.run(build_key_store(load_settings()))
    )
    try:
        if args.next:
            version = store.latest_version(args.tenant_id) + 1
        else:
            if store.versions(args.tenant_id):
                raise KeyExists(
                    f"tenant '{args.tenant_id}' already has a key; add the next version with --next"
                )
            version = 1
        name = key_var_name(args.tenant_id, version)
    except KeyExists as e:
        print(str(e), file=sys.stderr)
        return 1
    except TenantKeyError as e:
        print(str(e), file=sys.stderr)
        return 2
    encoded = base64.b64encode(secrets.token_bytes(KEY_BYTES)).decode()
    print(f"{name}={encoded}")
    print(
        "warning: this value is shown once; store it in the host's variables now. "
        "Keep every older version: deleting one makes the data written under it unreadable.",
        file=sys.stderr,
    )
    return 0


def _print_master_value(args: argparse.Namespace) -> int:
    env = master_key_env()
    if not env:
        version = 1
    else:
        try:
            masters = master_keys_from_env(env)
        except TenantKeyError as e:
            print(str(e), file=sys.stderr)
            return 2
        if not args.next:
            print(
                "a master key already exists; add the next version with --next",
                file=sys.stderr,
            )
            return 1
        version = masters.latest + 1
    encoded = base64.b64encode(secrets.token_bytes(KEY_BYTES)).decode()
    print(f"{MASTER_KEY_ENV_PREFIX}{version}={encoded}")
    print(
        "warning: this value is shown once; store it in the host's variables now. "
        "Keep every older master version: deleting one makes the tenant keys wrapped "
        "under it unreadable.",
        file=sys.stderr,
    )
    return 0


def _key_event(tenant_id: str, command: str, identity: str, invocation: str) -> AuditEvent:
    return AuditEvent(
        tenant_id=tenant_id,
        event_key=ids.audit_control_key(tenant_id, "payload_key", f"{command}:{invocation}"),
        kind="payload_key",
        agent_id=None,
        agent_version=None,
        requester_user_id=identity,
        identity=identity,
        tool=command,
    )


async def execute_db(
    args: argparse.Namespace,
    pool: AsyncConnectionPool,
    masters: MasterKeys,
    audit: AuditStore,
    uow: UnitOfWork,
    request_id: str | None = None,
) -> int:
    """Run one parsed create/rotate against the database, audited as the operator.

    The key and its audit event commit in one transaction; a refused command
    changes nothing and records nothing.
    """
    if not TENANT_ID_PATTERN.fullmatch(args.tenant_id):
        print(f"'{args.tenant_id}' is not a valid tenant ID")
        return 2
    identity = operator_identity()
    invocation = request_id or uuid.uuid4().hex
    try:
        async with uow.begin(args.tenant_id) as conn:
            if args.command == "create":
                version = await ensure_tenant_key(pool, masters, args.tenant_id, conn=conn)
            else:
                version = await rotate_tenant_key(pool, masters, args.tenant_id, conn=conn)
            await audit.record(
                _key_event(args.tenant_id, args.command, identity, invocation), conn=conn
            )
    except TenantKeyError as err:
        print(f"refused: {err}", file=sys.stderr)
        return 2
    print(f"{args.command}: tenant '{args.tenant_id}' key version {version}")
    return 0


async def _db_command(args: argparse.Namespace, settings: Settings) -> int:
    from agentos.audit import PostgresAuditStore
    from agentos.schema_version import require_current_schema

    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        await require_current_schema(pool)
        masters = master_keys_from_env()
        return await execute_db(
            args, pool, masters, PostgresAuditStore(pool), PostgresUnitOfWork(pool)
        )
    except TenantKeyError as err:
        print(f"refused: {err}", file=sys.stderr)
        return 2
    finally:
        await pool.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m agentos.tenant_keys")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser(
        "create", help="create the tenant's key (file, or database row in db mode)"
    )
    create.add_argument("tenant_id")
    rotate = sub.add_parser("rotate", help="add the tenant's next key version")
    rotate.add_argument("tenant_id")
    value = sub.add_parser("value", help="print a new key value once, for a host variable")
    value.add_argument("tenant_id")
    value.add_argument("--next", action="store_true", help="the version after the tenant's newest")
    master_value = sub.add_parser(
        "master-value", help="print a new master key value once, for a host variable (db mode)"
    )
    master_value.add_argument(
        "--next", action="store_true", help="the version after the newest master variable"
    )
    return parser


def main(argv: list[str] | None = None, key_dir: str | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "master-value":
        return _print_master_value(args)
    settings = load_settings() if key_dir is None else None
    if settings is not None and settings.key_source == KEY_SOURCE_DB:
        if args.command == "value":
            print(
                "the db key source keeps tenant keys in the database; use create or rotate",
                file=sys.stderr,
            )
            return 2
        return asyncio.run(_db_command(args, settings))
    if args.command == "value":
        return _print_value(args, key_dir)
    directory = key_dir or settings.key_dir
    try:
        path = create_key(directory, args.tenant_id, rotate=args.command == "rotate")
    except KeyExists as e:
        print(str(e), file=sys.stderr)
        return 1
    except TenantKeyError as e:
        print(str(e), file=sys.stderr)
        return 2
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
