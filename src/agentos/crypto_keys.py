"""Per-tenant encryption keys, kept in owner-only files outside the repo.

A key is 32 random bytes, base64-encoded, in ``<directory>/<tenant_id>.key``
(version 1) or ``<directory>/<tenant_id>.vN.key`` (a later version, made by
rotating). New payloads use the newest version; older ones stay readable.
``LocalKeyStore`` is the only place that reads one. It is the seam where a
cloud key service replaces local files later.

``EnvKeyStore`` reads the same keys from host variables named
``AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>`` (``<TENANT>`` is the tenant ID upper-cased
with ``-`` turned to ``_``), for hosts without a key directory. Every process
chooses between the two through ``build_key_store``, so the worker, the gateway
and the Temporal client always agree.

Rules enforced here:

* A key file that is missing, or readable by anyone but its owner, is refused.
* An error names the path or variable and the tenant. It never contains key material.
* A key is never logged and never appears in ``repr``.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
import stat
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Protocol

from agentos.config import (
    KEY_SOURCE_ENV,
    PAYLOAD_KEY_ENV_PREFIX,
    TENANT_ID_PATTERN,
    Settings,
    payload_key_env,
)

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


def key_id(tenant_id: str, version: int) -> str:
    """The ID a payload carries for a key: ``tenant/vN``."""
    return f"{tenant_id}/v{version}"


def parse_key_id(raw: str) -> tuple[str, int, bool]:
    """Split a payload's key ID into (tenant, version, legacy).

    An ID without a version (``tenant``) was written before keys were
    versioned; it names version 1 and is marked legacy. Raises ``TenantKeyError``
    for anything that is neither form.
    """
    tenant, sep, version = raw.partition("/")
    if not sep:
        _check_tenant(tenant)
        return tenant, 1, True
    if not (version.startswith("v") and version[1:].isdigit() and int(version[1:]) >= 1):
        raise TenantKeyError(f"'{raw}' is not a valid key ID")
    _check_tenant(tenant)
    return tenant, int(version[1:]), False


class KeyStore(Protocol):
    """What the codec and the start-up checks need from a key store."""

    def versions(self, tenant_id: str) -> list[int]:
        """The versions the tenant has a key for, ascending."""
        ...

    def latest_version(self, tenant_id: str) -> int:
        """The version new payloads are encrypted with. Raises if the tenant has no key."""
        ...

    def key(self, tenant_id: str, version: int = 1) -> bytes:
        """The tenant's 256-bit key. Raises ``TenantKeyError`` if it can't be used safely."""
        ...


class LocalKeyStore:
    """Keys by tenant and version. Version 1 is ``<tenant>.key``, version N is ``<tenant>.vN.key``."""

    def __init__(self, directory: str | Path):
        self._directory = Path(directory).expanduser()
        self._cache: dict[tuple[str, int], bytes] = {}

    def __repr__(self) -> str:
        return f"LocalKeyStore({str(self._directory)!r})"

    def path(self, tenant_id: str, version: int = 1) -> Path:
        _check_tenant(tenant_id)
        name = f"{tenant_id}.key" if version == 1 else f"{tenant_id}.v{version}.key"
        return self._directory / name

    def versions(self, tenant_id: str) -> list[int]:
        """The versions the tenant has a key file for, ascending."""
        _check_tenant(tenant_id)
        found: set[int] = set()
        if self.path(tenant_id).exists():
            found.add(1)
        try:
            names = [p.name for p in self._directory.iterdir()]
        except FileNotFoundError:
            names = []
        prefix = f"{tenant_id}.v"
        for name in names:
            if name.startswith(prefix) and name.endswith(".key"):
                number = name[len(prefix) : -len(".key")]
                if number.isdigit() and int(number) >= 2:
                    found.add(int(number))
        return sorted(found)

    def latest_version(self, tenant_id: str) -> int:
        """The version new payloads are encrypted with. Raises if the tenant has no key."""
        found = self.versions(tenant_id)
        if not found:
            # Raises the same "no key, create one" error as a direct read.
            self.key(tenant_id, 1)
        return found[-1]

    def key(self, tenant_id: str, version: int = 1) -> bytes:
        """The tenant's 256-bit key. Raises ``TenantKeyError`` if it can't be used safely."""
        cached = self._cache.get((tenant_id, version))
        if cached is not None:
            return cached
        path = self.path(tenant_id, version)
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
        self._cache[(tenant_id, version)] = key
        return key


def key_var_name(tenant_id: str, version: int) -> str:
    """The variable that holds one key version: ``AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>``."""
    _check_tenant(tenant_id)
    return f"{PAYLOAD_KEY_ENV_PREFIX}{tenant_id.upper().replace('-', '_')}_V{version}"


# The tenant part is greedy, so the version is the last ``_V<n>``: a tenant ID
# may itself end in ``-v1`` (``a-v1`` becomes ``A_V1``).
_KEY_VAR_NAME = re.compile(re.escape(PAYLOAD_KEY_ENV_PREFIX) + r"(.+)_V([0-9]+)")


class EnvKeyStore:
    """Keys from ``AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>`` variables, base64 of 32 bytes.

    ``<TENANT>`` is the tenant ID upper-cased with ``-`` turned to ``_``. The
    tenant ID alphabet holds no ``_``, so two valid tenant IDs never map to one
    variable name; the ambiguity that can happen is two variables claiming one
    tenant's key version (a lower-cased or hyphenated spelling beside the
    canonical one), and that is refused when the store is first read, so a
    process refuses to start on it. Values are key material: they are kept in
    memory only, never logged, never in ``repr`` and never in an error.
    """

    def __init__(self, env: Mapping[str, str]):
        self._env = {
            name: value for name, value in env.items() if name.startswith(PAYLOAD_KEY_ENV_PREFIX)
        }
        self._index: dict[tuple[str, int], str] | None = None
        self._cache: dict[tuple[str, int], bytes] = {}

    def __repr__(self) -> str:
        return "EnvKeyStore()"

    def _names(self) -> dict[tuple[str, int], str]:
        """(tenant, version) to variable name, parsed once. Raises on a bad or shared name."""
        if self._index is None:
            index: dict[tuple[str, int], str] = {}
            for name in sorted(self._env):
                match = _KEY_VAR_NAME.fullmatch(name)
                if match is None or int(match.group(2)) < 1:
                    raise TenantKeyError(
                        f"variable {name} is not a payload key variable; the name is "
                        f"{PAYLOAD_KEY_ENV_PREFIX}<TENANT>_V<n>, the tenant ID upper-cased "
                        "with '-' turned to '_'"
                    )
                tenant = match.group(1).lower().replace("_", "-")
                if not TENANT_ID_PATTERN.fullmatch(tenant):
                    raise TenantKeyError(f"variable {name} does not name a valid tenant ID")
                claim = (tenant, int(match.group(2)))
                if claim in index:
                    raise TenantKeyError(
                        f"variables {index[claim]} and {name} both claim version {claim[1]} "
                        f"of tenant '{tenant}'; one key version has one variable"
                    )
                index[claim] = name
            self._index = index
        return self._index

    def versions(self, tenant_id: str) -> list[int]:
        """The versions the tenant has a key variable for, ascending."""
        _check_tenant(tenant_id)
        return sorted(version for (tenant, version) in self._names() if tenant == tenant_id)

    def latest_version(self, tenant_id: str) -> int:
        """The version new payloads are encrypted with. Raises if the tenant has no key."""
        found = self.versions(tenant_id)
        if not found:
            # Raises the same "no key, set one" error as a direct read.
            self.key(tenant_id, 1)
        return found[-1]

    def key(self, tenant_id: str, version: int = 1) -> bytes:
        """The tenant's 256-bit key. Raises ``TenantKeyError`` naming the variable, never the value."""
        cached = self._cache.get((tenant_id, version))
        if cached is not None:
            return cached
        name = self._names().get((tenant_id, version))
        if name is None:
            raise TenantKeyError(
                f"no encryption key for tenant '{tenant_id}' version {version}; "
                f"set {key_var_name(tenant_id, version)} to a new base64 key "
                f"(uv run python -m agentos.tenant_keys value {tenant_id})"
            )
        try:
            key = base64.b64decode(self._env[name].strip(), validate=True)
        except (binascii.Error, ValueError):
            raise TenantKeyError(
                f"variable {name} for tenant '{tenant_id}' is not valid base64"
            ) from None
        if len(key) != KEY_BYTES:
            raise TenantKeyError(
                f"variable {name} for tenant '{tenant_id}' holds {len(key)} bytes, "
                f"expected {KEY_BYTES}"
            )
        self._cache[(tenant_id, version)] = key
        return key


def build_key_store(settings: Settings) -> KeyStore:
    """The one place a process chooses its key store, so every process agrees.

    ``AGENTOS_KEY_SOURCE=env`` reads the key variables; the default ``files``
    reads ``AGENTOS_KEY_DIR``.
    """
    if settings.key_source == KEY_SOURCE_ENV:
        return EnvKeyStore(payload_key_env())
    return LocalKeyStore(settings.key_dir)


def create_key(directory: str | Path, tenant_id: str, *, rotate: bool = False) -> Path:
    """Write a new random key for the tenant and return its path.

    Without ``rotate`` this creates version 1 and an existing key raises
    ``KeyExists``. With ``rotate`` it creates the next version after the
    newest one (the tenant must already have a key); older versions stay, so
    payloads written under them still decode. Files are created exclusively,
    so an existing key is never overwritten.
    """
    import secrets

    store = LocalKeyStore(directory)
    if rotate:
        existing = store.versions(tenant_id)
        if not existing:
            raise TenantKeyError(
                f"tenant '{tenant_id}' has no key to rotate; "
                f"create one with: uv run python -m agentos.tenant_keys create {tenant_id}"
            )
        version = existing[-1] + 1
    else:
        version = 1
    path = store.path(tenant_id, version)
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


def check_keys(keys: KeyStore, tenant_ids: Iterable[str]) -> list[str]:
    """Problems with the key of each given tenant, one message each, [] if all are usable.

    Messages name the path or variable and the tenant only, never key material.
    """
    problems = []
    for tenant_id in tenant_ids:
        try:
            for version in keys.versions(tenant_id) or [1]:
                keys.key(tenant_id, version)
        except TenantKeyError as e:
            problems.append(str(e))
    return problems


def startup_key_failure(keys: KeyStore, tenant_ids: Iterable[str]) -> str | None:
    """A one-line reason to refuse to start, or None when every tenant has a usable key."""
    problems = check_keys(keys, tenant_ids)
    if not problems:
        return None
    return (
        f"refusing to start: {len(problems)} tenant key problem(s): "
        + "; ".join(problems)
        + ". Create a missing key with: uv run python -m agentos.tenant_keys create <tenant_id>"
        + " (key files) or value <tenant_id> (key variables)"
    )
