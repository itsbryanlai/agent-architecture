"""The one way to connect to Temporal, so every client encrypts its payloads."""

from __future__ import annotations

import dataclasses

from psycopg_pool import AsyncConnectionPool
from temporalio.client import Client
from temporalio.converter import DataConverter, DefaultFailureConverterWithEncodedAttributes

from agentos.config import Settings
from agentos.crypto_keys import KeyStore, build_key_store
from agentos.payload_codec import TenantPayloadCodec


def tenant_data_converter(keys: KeyStore, *, allow_plaintext: bool = False) -> DataConverter:
    """The default data converter with the per-tenant encrypting codec.

    Failures move their message and stack trace into encoded attributes, so
    the codec encrypts those too: an error text can quote user or page content.
    Unencrypted payloads are refused unless ``allow_plaintext`` is set, which is
    only for reading a history written before encryption existed.
    """
    return dataclasses.replace(
        DataConverter.default,
        payload_codec=TenantPayloadCodec(keys, allow_plaintext=allow_plaintext),
        failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
    )


async def build_temporal_client(
    settings: Settings,
    keys: KeyStore | None = None,
    pool: AsyncConnectionPool | None = None,
) -> Client:
    """Connect with the encrypting converter.

    Pass the process's key store to share one cache; otherwise the factory
    builds one, and the ``db`` source then needs the database pool.
    """
    if keys is None:
        keys = await build_key_store(settings, pool)
    return await Client.connect(
        settings.temporal_address,
        data_converter=tenant_data_converter(
            keys, allow_plaintext=settings.allow_plaintext_payloads
        ),
    )
