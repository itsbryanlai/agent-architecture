"""Encrypts every payload Temporal stores, with the key of the tenant it belongs to.

Temporal's data converter hands this codec a serialization context before each
encode or decode. The context names the workflow the payload belongs to, and
every workflow ID starts with the tenant ID up to its first ":". That is the
only source of the tenant: it comes from the runtime, never from payload
content.

Rules enforced here:

* A payload is always encrypted (AES-256-GCM, a fresh random nonce, the tenant
  ID as associated data) with the key of its own tenant.
* If no tenant can be derived, encoding fails. Nothing is ever sent unencrypted.
* Decoding needs only the key ID in the payload's own metadata. A payload
  without the encrypted tag is passed through, so histories written before
  encryption still read.
* Keys come from the key store and never appear in logs, errors or ``repr``.
"""

from __future__ import annotations

import os
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
from agentos.crypto_keys import LocalKeyStore

ENCRYPTED_ENCODING = b"binary/encrypted"
KEY_ID_METADATA = "encryption-key-id"
NONCE_METADATA = "nonce"
NONCE_BYTES = 12


class NoTenantForPayload(RuntimeError):
    """A payload was about to be encoded with no tenant to encrypt it for."""


def tenant_from_workflow_id(workflow_id: str | None) -> str | None:
    """The text before the first ":" of a workflow ID, if it is a valid tenant ID."""
    if not workflow_id or ":" not in workflow_id:
        return None
    tenant = workflow_id.split(":", 1)[0]
    return tenant if TENANT_ID_PATTERN.fullmatch(tenant) else None


class TenantPayloadCodec(PayloadCodec, WithSerializationContext):
    def __init__(self, keys: LocalKeyStore, tenant_id: str | None = None):
        self._keys = keys
        self._tenant_id = tenant_id

    def __repr__(self) -> str:
        return f"TenantPayloadCodec(tenant_id={self._tenant_id!r})"

    def with_context(self, context: SerializationContext) -> Self:
        workflow_id: str | None = None
        if isinstance(context, (WorkflowSerializationContext, ActivitySerializationContext)):
            workflow_id = context.workflow_id
        # A context with no usable workflow ID gives an unbound codec: it can
        # still decode, and it refuses to encode.
        return type(self)(self._keys, tenant_from_workflow_id(workflow_id))

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if self._tenant_id is None:
            raise NoTenantForPayload(
                "refusing to send a payload: no tenant could be derived from its workflow ID"
            )
        tenant = self._tenant_id
        aead = AESGCM(self._keys.key(tenant))
        out: list[Payload] = []
        for payload in payloads:
            nonce = os.urandom(NONCE_BYTES)
            ciphertext = aead.encrypt(nonce, payload.SerializeToString(), tenant.encode())
            out.append(
                Payload(
                    metadata={
                        "encoding": ENCRYPTED_ENCODING,
                        KEY_ID_METADATA: tenant.encode(),
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
                out.append(payload)
                continue
            key_id = payload.metadata[KEY_ID_METADATA].decode()
            aead = AESGCM(self._keys.key(key_id))
            plain = aead.decrypt(payload.metadata[NONCE_METADATA], payload.data, key_id.encode())
            out.append(Payload.FromString(plain))
        return out
