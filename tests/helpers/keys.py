"""A key store twin for codec tests: memory served, an async refresh re-reads a shared source."""

from __future__ import annotations

from agentos.crypto_keys import TenantKeyError


class RefreshableKeyStore:
    """Serves keys from memory until ``refresh`` copies the shared source again.

    The ``DatabaseKeyStore``'s contract without a database: a key added to the
    source after a refresh is invisible until the next one.
    """

    def __init__(self, source: dict[tuple[str, int], bytes]):
        self._source = source
        self._cache: dict[tuple[str, int], bytes] = {}

    async def refresh(self) -> None:
        self._cache = dict(self._source)

    def versions(self, tenant_id: str) -> list[int]:
        return sorted(version for (tenant, version) in self._cache if tenant == tenant_id)

    def latest_version(self, tenant_id: str) -> int:
        found = self.versions(tenant_id)
        if not found:
            self.key(tenant_id, 1)
        return found[-1]

    def key(self, tenant_id: str, version: int = 1) -> bytes:
        try:
            return self._cache[(tenant_id, version)]
        except KeyError:
            raise TenantKeyError(
                f"no encryption key for tenant '{tenant_id}' version {version}"
            ) from None
