"""Connector manifests: the safety metadata that sits on top of each tool.

Calling a tool is generic; what makes a tool safe for an agent that does work
is its risk class, whether it is scoped to the requester, and how a write
avoids duplicates. That metadata lives in a YAML manifest per connector.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import yaml

MANIFEST_DIR = Path(__file__).parent / "manifests"


class RiskClass(str, Enum):
    READ = "read"
    WRITE_REVERSIBLE = "write_reversible"
    WRITE_IRREVERSIBLE = "write_irreversible"


@dataclass(frozen=True)
class ToolSpec:
    name: str
    risk: RiskClass
    description: str
    input_schema: dict
    requester_scoped: str | None = None  # name of the hidden person field
    idempotency: str | None = None  # idempotency method, e.g. "marker_property"
    offer_to_llm: bool = True

    def llm_definition(self) -> dict:
        """The Anthropic tool definition, with the requester field removed.

        The person field of a requester-scoped tool is filled in by the runtime,
        so the LLM must not see it or be able to set it.
        """
        schema = copy.deepcopy(self.input_schema)
        if self.requester_scoped:
            schema.get("properties", {}).pop(self.requester_scoped, None)
            required = [r for r in schema.get("required", []) if r != self.requester_scoped]
            if required:
                schema["required"] = required
            else:
                schema.pop("required", None)
        return {"name": self.name, "description": self.description, "input_schema": schema}


@dataclass(frozen=True)
class Manifest:
    connector: str
    tools: dict[str, ToolSpec]


def load_manifest(path: str | Path) -> Manifest:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    tools = {}
    for name, spec in (raw.get("tools") or {}).items():
        tools[name] = ToolSpec(
            name=name,
            risk=RiskClass(spec["risk"]),
            description=str(spec.get("description", "")).strip(),
            input_schema=spec.get("input_schema") or {"type": "object", "properties": {}},
            requester_scoped=spec.get("requester_scoped"),
            idempotency=spec.get("idempotency"),
            offer_to_llm=bool(spec.get("offer_to_llm", True)),
        )
    return Manifest(connector=str(raw["connector"]), tools=tools)
