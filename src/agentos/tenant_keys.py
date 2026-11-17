"""Create or rotate a tenant's payload encryption key.

    uv run python -m agentos.tenant_keys create <tenant_id>
    uv run python -m agentos.tenant_keys rotate <tenant_id>

Prints the path of the new key file and nothing else. It never overwrites an
existing key: losing a key makes that tenant's stored workflow data unreadable.
``rotate`` adds the next version; the next payload any process encodes uses it,
so copy the file to every host that reads the key directory first. Older
versions stay so existing histories still decode. Never delete
an old version while any history written under it is kept.
"""

from __future__ import annotations

import argparse
import sys

from agentos.config import load_settings
from agentos.crypto_keys import KeyExists, TenantKeyError, create_key


def main(argv: list[str] | None = None, key_dir: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m agentos.tenant_keys")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create", help="create the tenant's key file")
    create.add_argument("tenant_id")
    rotate = sub.add_parser("rotate", help="add the tenant's next key version")
    rotate.add_argument("tenant_id")
    args = parser.parse_args(argv)
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
