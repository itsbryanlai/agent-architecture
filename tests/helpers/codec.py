"""Helpers that read the payloads out of a workflow history."""

from __future__ import annotations

import json

from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType


def all_payloads(message) -> list[Payload]:
    """Every Payload message anywhere inside a protobuf message."""
    found: list[Payload] = []

    def walk(m) -> None:
        if isinstance(m, Payload):
            found.append(m)
            return
        for field, value in m.ListFields():
            if field.type != field.TYPE_MESSAGE:
                continue
            if field.is_repeated:
                items = value.values() if field.message_type.GetOptions().map_entry else value
                for item in items:
                    if hasattr(item, "ListFields"):
                        walk(item)
            else:
                walk(value)

    walk(message)
    return found


def is_sdk_patch_record(event, payload: Payload) -> bool:
    """True for the markers and search attribute the SDK writes for workflow.patched.

    Temporal core writes these itself, outside the data converter, so no codec can
    reach them. They hold only the patch IDs from the workflow code.
    """
    if event.event_type == EventType.EVENT_TYPE_MARKER_RECORDED:
        if event.marker_recorded_event_attributes.marker_name != "core_patch":
            return False
        return set(json.loads(payload.data)) == {"id", "deprecated"}
    if event.event_type == EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
        fields = event.upsert_workflow_search_attributes_event_attributes.search_attributes
        return payload.metadata.get("type") == b"KeywordList" and list(fields.indexed_fields) == [
            "TemporalChangeVersion"
        ]
    return False
