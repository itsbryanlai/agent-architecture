"""The one way to connect to Temporal, so every client encrypts its payloads."""

from __future__ import annotations

import dataclasses

from temporalio.client import Client
from temporalio.converter import DataConverter, DefaultFailureConverterWithEncodedAttributes

from agentos.config import Settings
from agentos.crypto_keys import LocalKeyStore
from agentos.payload_codec import TenantPayloadCodec


def tenant_data_converter(keys: LocalKeyStore) -> DataConverter:
    """The default data converter with the per-tenant encrypting codec.

    Failures move their message and stack trace into encoded attributes, so
    the codec encrypts those too: an error text can quote user or page content.
    """
    return dataclasses.replace(
        DataConverter.default,
        payload_codec=TenantPayloadCodec(keys),
        failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
    )


async def build_temporal_client(settings: Settings) -> Client:
    keys = LocalKeyStore(settings.key_dir)
    return await Client.connect(
        settings.temporal_address, data_converter=tenant_data_converter(keys)
    )
