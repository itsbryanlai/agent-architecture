"""Encrypts every payload Temporal stores, with the key of the tenant it belongs to.

Temporal's data converter hands this codec a serialization context before each
encode or decode. The context names the workflow the payload belongs to, and
every workflow ID starts with the tenant ID up to its first ":". That is the
only source of the tenant: it comes from the runtime, never from payload
content.

Rules enforced here:

* A payload is always encrypted (AES-256-GCM, a fresh random nonce, the
  workflow ID as associated data) with the newest key of its own tenant. Its
  key ID is ``tenant/vN``.
* If no tenant can be derived, encoding fails. Nothing is ever sent unencrypted.
* Decoding checks the payload's key ID against the tenant the codec is bound
  to, not the other way round: a payload naming another tenant is refused, as
  is one moved to another workflow (the associated data would not match). A
  codec with no workflow bound refuses every encrypted payload.
* A tenant may hold several key versions (see ``crypto_keys``); decoding
  accepts any version the tenant still has.
* A payload without the encrypted tag is refused unless the codec was built
  with ``allow_plaintext=True`` (a setting, off by default).
* Payloads written before keys were versioned carry the bare tenant ID as key
  ID and the tenant ID as associated data. They decode as version 1 while
  ``allow_legacy`` is on, which is the default. Nothing is ever encoded that way.
* Keys come from the key store and never appear in logs, errors or ``repr``.
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from typing import Self

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    ActivitySerializationContext,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)

from agentos.config import TENANT_ID_PATTERN
from agentos.crypto_keys import KeyStore, TenantKeyError, key_id, parse_key_id

ENCRYPTED_ENCODING = b"binary/encrypted"
KEY_ID_METADATA = "encryption-key-id"
NONCE_METADATA = "nonce"
NONCE_BYTES = 12


# Temporal starts a scheduled run under the schedule action's workflow ID plus
# "-" and the nominal time. The input was encoded under the action's own ID, so
# the suffix is dropped before the ID is used as associated data.
_SCHEDULED_RUN_SUFFIX = re.compile(r"-\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def associated_data(workflow_id: str) -> bytes:
    """The bytes a payload of this workflow is authenticated against."""
    return _SCHEDULED_RUN_SUFFIX.sub("", workflow_id).encode()


class NoTenantForPayload(RuntimeError):
    """A payload was about to be encoded with no tenant to encrypt it for."""


class PayloadRefused(RuntimeError):
    """A payload was not decoded: wrong tenant, wrong workflow, plaintext or unbound."""


def tenant_from_workflow_id(workflow_id: str | None) -> str | None:
    """The text before the first ":" of a workflow ID, if it is a valid tenant ID."""
    if not workflow_id or ":" not in workflow_id:
        return None
    tenant = workflow_id.split(":", 1)[0]
    return tenant if TENANT_ID_PATTERN.fullmatch(tenant) else None


def encrypt_for_tenant(keys: KeyStore, tenant_id: str, plaintext: bytes) -> tuple[bytes, int]:
    """A small value under the tenant's newest key: (nonce || ciphertext, key version).

    The same AES-256-GCM cipher the payloads use, with the tenant ID as the
    associated data, so a value stored for one tenant does not decrypt for
    another. For a secret kept outside Temporal (the Notion webhook token);
    the value never appears in a log or an error.
    """
    version = keys.latest_version(tenant_id)
    nonce = os.urandom(NONCE_BYTES)
    data = AESGCM(keys.key(tenant_id, version)).encrypt(nonce, plaintext, tenant_id.encode())
    return nonce + data, version


def decrypt_for_tenant(keys: KeyStore, tenant_id: str, blob: bytes, version: int) -> bytes:
    """The value ``encrypt_for_tenant`` stored, under the key version it names."""
    nonce, data = blob[:NONCE_BYTES], blob[NONCE_BYTES:]
    return AESGCM(keys.key(tenant_id, version)).decrypt(nonce, data, tenant_id.encode())


class TenantPayloadCodec(PayloadCodec, WithSerializationContext):
    def __init__(
        self,
        keys: KeyStore,
        tenant_id: str | None = None,
        workflow_id: str | None = None,
        *,
        allow_plaintext: bool = False,
        allow_legacy: bool = True,
    ):
        self._keys = keys
        self._tenant_id = tenant_id
        self._workflow_id = workflow_id
        self._allow_plaintext = allow_plaintext
        self._allow_legacy = allow_legacy

    def __repr__(self) -> str:
        return f"TenantPayloadCodec(tenant_id={self._tenant_id!r})"

    def with_context(self, context: SerializationContext) -> Self:
        workflow_id: str | None = None
        if isinstance(context, (WorkflowSerializationContext, ActivitySerializationContext)):
            workflow_id = context.workflow_id
        tenant = tenant_from_workflow_id(workflow_id)
        # A context with no usable workflow ID gives an unbound codec: it
        # refuses to encode, and refuses every encrypted payload.
        return type(self)(
            self._keys,
            tenant,
            workflow_id if tenant else None,
            allow_plaintext=self._allow_plaintext,
            allow_legacy=self._allow_legacy,
        )

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if self._tenant_id is None or self._workflow_id is None:
            raise NoTenantForPayload(
                "refusing to send a payload: no tenant could be derived from its workflow ID"
            )
        tenant = self._tenant_id
        version = self._keys.latest_version(tenant)
        aead = AESGCM(self._keys.key(tenant, version))
        aad = associated_data(self._workflow_id)
        out: list[Payload] = []
        for payload in payloads:
            nonce = os.urandom(NONCE_BYTES)
            ciphertext = aead.encrypt(nonce, payload.SerializeToString(), aad)
            out.append(
                Payload(
                    metadata={
                        "encoding": ENCRYPTED_ENCODING,
                        KEY_ID_METADATA: key_id(tenant, version).encode(),
                        NONCE_METADATA: nonce,
                    },
                    data=ciphertext,
                )
            )
        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for payload in payloads:
            if payload.metadata.get("encoding") != ENCRYPTED_ENCODING:
                if not self._allow_plaintext:
                    raise PayloadRefused("refusing an unencrypted payload")
                out.append(payload)
                continue
            if self._tenant_id is None or self._workflow_id is None:
                raise PayloadRefused("refusing an encrypted payload: no workflow bound")
            try:
                tenant, version, legacy = parse_key_id(payload.metadata[KEY_ID_METADATA].decode())
            except (KeyError, UnicodeDecodeError, TenantKeyError):
                raise PayloadRefused("refusing a payload with a malformed key ID") from None
            if tenant != self._tenant_id:
                raise PayloadRefused(
                    f"refusing a payload for tenant '{tenant}' in a workflow of "
                    f"tenant '{self._tenant_id}'"
                )
            if legacy and not self._allow_legacy:
                raise PayloadRefused("refusing a payload with an unversioned key ID")
            aad = tenant.encode() if legacy else associated_data(self._workflow_id)
            aead = AESGCM(self._keys.key(tenant, version))
            plain = aead.decrypt(payload.metadata[NONCE_METADATA], payload.data, aad)
            out.append(Payload.FromString(plain))
        return out
