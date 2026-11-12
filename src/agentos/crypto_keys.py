"""Per-tenant encryption keys, kept in owner-only files outside the repo.

A key is 32 random bytes, base64-encoded, in ``<directory>/<tenant_id>.key``.
``LocalKeyStore`` is the only place that reads one. It is the seam where a
cloud key service replaces local files later.

Rules enforced here:

* A key file that is missing, or readable by anyone but its owner, is refused.
* An error names the path and the tenant. It never contains key material.
* A key is never logged and never appears in ``repr``.
"""

from __future__ import annotations

import base64
import binascii
import os
import stat
from collections.abc import Iterable
from pathlib import Path

from agentos.config import TENANT_ID_PATTERN

KEY_BYTES = 32
KEY_FILE_MODE = 0o600
KEY_DIR_MODE = 0o700


class TenantKeyError(RuntimeError):
    """A tenant's key is missing, unsafe or malformed. Names the file, never the key."""


class KeyExists(TenantKeyError):
    """A key for this tenant already exists and is left untouched."""


def _check_tenant(tenant_id: str) -> None:
    # The tenant ID becomes part of a file name, so it must not hold a path.
    if not TENANT_ID_PATTERN.fullmatch(tenant_id):
        raise TenantKeyError(f"'{tenant_id}' is not a valid tenant ID")


class LocalKeyStore:
    def __init__(self, directory: str | Path):
        self._directory = Path(directory).expanduser()
        self._cache: dict[str, bytes] = {}

    def __repr__(self) -> str:
        return f"LocalKeyStore({str(self._directory)!r})"

    def path(self, tenant_id: str) -> Path:
        _check_tenant(tenant_id)
        return self._directory / f"{tenant_id}.key"

    def key(self, tenant_id: str) -> bytes:
        """The tenant's 256-bit key. Raises ``TenantKeyError`` if it can't be used safely."""
        cached = self._cache.get(tenant_id)
        if cached is not None:
            return cached
        path = self.path(tenant_id)
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except FileNotFoundError:
            raise TenantKeyError(
                f"no encryption key for tenant '{tenant_id}' at {path}; "
                f"create one with: uv run python -m agentos.tenant_keys create {tenant_id}"
            ) from None
        if mode & 0o077:
            raise TenantKeyError(
                f"key file {path} for tenant '{tenant_id}' has mode {mode:04o}; "
                f"it must be readable by its owner only (0600)"
            )
        try:
            key = base64.b64decode(path.read_bytes().strip(), validate=True)
        except (binascii.Error, ValueError):
            raise TenantKeyError(
                f"key file {path} for tenant '{tenant_id}' is not valid base64"
            ) from None
        if len(key) != KEY_BYTES:
            raise TenantKeyError(
                f"key file {path} for tenant '{tenant_id}' holds {len(key)} bytes, "
                f"expected {KEY_BYTES}"
            )
        self._cache[tenant_id] = key
        return key


def create_key(directory: str | Path, tenant_id: str) -> Path:
    """Write a new random key for the tenant and return its path.

    Never overwrites: the file is created exclusively, so an existing key
    raises ``KeyExists`` and stays as it was.
    """
    import secrets

    store = LocalKeyStore(directory)
    path = store.path(tenant_id)
    directory = path.parent
    if not directory.exists():
        directory.mkdir(parents=True, mode=KEY_DIR_MODE)
        os.chmod(directory, KEY_DIR_MODE)
    encoded = base64.b64encode(secrets.token_bytes(KEY_BYTES))
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, KEY_FILE_MODE)
    except FileExistsError:
        raise KeyExists(
            f"a key for tenant '{tenant_id}' already exists at {path}; left unchanged"
        ) from None
    with os.fdopen(fd, "wb") as f:
        f.write(encoded + b"\n")
    return path


def check_keys(keys: LocalKeyStore, tenant_ids: Iterable[str]) -> list[str]:
    """Problems with the key of each given tenant, one message each, [] if all are usable.

    Messages name the path and the tenant only, never key material.
    """
    problems = []
    for tenant_id in tenant_ids:
        try:
            keys.key(tenant_id)
        except TenantKeyError as e:
            problems.append(str(e))
    return problems


def startup_key_failure(keys: LocalKeyStore, tenant_ids: Iterable[str]) -> str | None:
    """A one-line reason to refuse to start, or None when every tenant has a usable key."""
    problems = check_keys(keys, tenant_ids)
    if not problems:
        return None
    return (
        f"refusing to start: {len(problems)} tenant key problem(s): "
        + "; ".join(problems)
        + ". Create a missing key with: uv run python -m agentos.tenant_keys create <tenant_id>"
    )
