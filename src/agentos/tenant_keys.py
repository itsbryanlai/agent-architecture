"""Create or rotate a tenant's payload encryption key.

    uv run python -m agentos.tenant_keys create <tenant_id>
    uv run python -m agentos.tenant_keys rotate <tenant_id>
    uv run python -m agentos.tenant_keys value <tenant_id> [--next]

``create`` and ``rotate`` print the path of the new key file and nothing else.
They never overwrite an existing key: losing a key makes that tenant's stored
workflow data unreadable. ``rotate`` adds the next version; the next payload any
process encodes uses it, so copy the file to every host that reads the key
directory first. Older versions stay so existing histories still decode. Never
delete an old version while any history written under it is kept.

``value`` is for a host that keeps keys in variables instead of files (see
docs/operations.md): it prints one new key value as
``AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>=<base64>`` for the operator to paste into the
host's variables, and a warning on stderr. It never reads or prints an existing
key and writes no file; ``--next`` names the version after the tenant's newest.
"""

from __future__ import annotations

import argparse
import base64
import secrets
import sys

from agentos.config import load_settings
from agentos.crypto_keys import (
    KEY_BYTES,
    KeyExists,
    LocalKeyStore,
    TenantKeyError,
    build_key_store,
    create_key,
    key_var_name,
)


def _print_value(args: argparse.Namespace, key_dir: str | None) -> int:
    store = LocalKeyStore(key_dir) if key_dir is not None else build_key_store(load_settings())
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


def main(argv: list[str] | None = None, key_dir: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m agentos.tenant_keys")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create", help="create the tenant's key file")
    create.add_argument("tenant_id")
    rotate = sub.add_parser("rotate", help="add the tenant's next key version")
    rotate.add_argument("tenant_id")
    value = sub.add_parser("value", help="print a new key value once, for a host variable")
    value.add_argument("tenant_id")
    value.add_argument("--next", action="store_true", help="the version after the tenant's newest")
    args = parser.parse_args(argv)
    if args.command == "value":
        return _print_value(args, key_dir)
    directory = key_dir or load_settings().key_dir
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
