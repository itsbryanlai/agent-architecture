"""Connector manifests: the safety metadata that sits on top of each tool.

Calling a tool is generic; what makes a tool safe for an agent that does work
is its risk class, whether it is scoped to the requester, how a write avoids
duplicates, and whether an admin may let it run without the requester's
approval (``low_risk``, a comment or a suggested edit only). That metadata lives in a YAML
manifest per connector.

The runtime reads it. ``screen`` classifies a call the model made: a read tool runs at
once, a tool whose ``risk`` is a write becomes a task that asks for the requester's
approval, unless an admin's policy lets a low-risk action run without it (D-43; its
``idempotency`` method says which kind of task carries it out), and input that does
not fit the tool's schema is refused with a message for the model. ``TOOLS`` holds every
manifest's tools by name.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import yaml

from agentos.models import READ_KIND

MANIFEST_DIR = Path(__file__).parent / "manifests"

# The kind of task that carries out a write, by the idempotency method its manifest declares:
# an item carrying a marker property is created, a comment ending in a marker is posted, a
# sheet row carrying developer metadata is appended, a Doc carrying a marker appProperty is
# copied from a template, a Doc section is replaced at the read revision, or suggested at it
# (a suggested edit is a replace task whose tool is ``suggest_edit``, D-54).
WRITE_KINDS = {
    "marker_property": "create_item",
    "marker_text": "add_comment",
    "developer_metadata": "append_row",
    "app_properties": "create_doc",
    "revision": "replace_section",
    "revision_suggest": "replace_section",
}

# The idempotency methods of the actions an admin may relax (D-43): a comment, and a
# suggested edit the Doc's owner still accepts or rejects (D-54). Never a create, an
# append or a replace.
LOW_RISK_METHODS = frozenset({"marker_text", "revision_suggest"})


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
    # An admin may turn requester approval off for this action (D-43). Only a comment
    # action may carry it; loading refuses the flag on anything else.
    low_risk: bool = False
    connector: str = ""  # the manifest's connector; empty for a spec built outside one
    # Set on an action an agent's creator may switch on or off (D-52): a short phrase that
    # completes "This agent may ...". A tool with no label is never a choice (a knowledge
    # search follows the grants; the task list follows the write actions).
    label: str = ""
    service: str = ""  # the manifest's service name, shown beside its actions
    # The kind of grant the action works on (each connector's ``grant_kinds``) and
    # how to say it, so the console can tell a creator who ticked an action that its grant
    # is missing. Empty for a tool with no grant of its own.
    needs: str = ""
    needs_text: str = ""

    @property
    def is_write(self) -> bool:
        return self.risk is not RiskClass.READ

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
    connector = str(raw["connector"])
    tools = {}
    for name, spec in (raw.get("tools") or {}).items():
        risk = RiskClass(spec["risk"])
        low_risk = bool(spec.get("low_risk", False))
        if low_risk and (
            risk is RiskClass.READ or (spec.get("idempotency") or "") not in LOW_RISK_METHODS
        ):
            raise ValueError(
                f"tool '{name}': low_risk may only mark a comment or a suggested edit, "
                "never a create, append or replace"
            )
        tools[name] = ToolSpec(
            name=name,
            risk=risk,
            description=str(spec.get("description", "")).strip(),
            input_schema=spec.get("input_schema") or {"type": "object", "properties": {}},
            requester_scoped=spec.get("requester_scoped"),
            idempotency=spec.get("idempotency"),
            offer_to_llm=bool(spec.get("offer_to_llm", True)),
            low_risk=low_risk,
            connector=connector,
            label=str(spec.get("label", "")).strip(),
            needs=str(spec.get("needs", "")).strip(),
            needs_text=str(spec.get("needs_text", "")).strip(),
            service=str(raw.get("service", "")).strip(),
        )
    return Manifest(connector=connector, tools=tools)


def _load_all() -> dict[str, ToolSpec]:
    tools: dict[str, ToolSpec] = {}
    for path in sorted(MANIFEST_DIR.glob("*.yaml")):
        for name, spec in load_manifest(path).tools.items():
            if name in tools:
                raise ValueError(f"tool '{name}' is declared in more than one manifest")
            tools[name] = spec
    return tools


TOOLS: dict[str, ToolSpec] = _load_all()

_JSON_TYPES = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "array": list,
    "object": dict,
}


def _is_type(value: object, kind: str) -> bool:
    if kind in ("integer", "number") and isinstance(value, bool):
        return False
    return isinstance(value, _JSON_TYPES[kind])


def check_input(schema: dict, value: object) -> str | None:
    """Why ``value`` does not fit the object ``schema``, or None.

    Covers what the manifests use: an object with named properties of a JSON type (and an
    optional ``enum``), a ``required`` list, and no properties beyond the named ones.
    """
    if not isinstance(value, dict):
        return "the input must be an object"
    properties = schema.get("properties", {})
    for name in schema.get("required", []):
        if name not in value:
            return f"missing required field '{name}'"
    for name, item in value.items():
        if name not in properties:
            return f"unexpected field '{name}'"
        kind = properties[name].get("type")
        if kind in _JSON_TYPES and not _is_type(item, kind):
            return f"'{name}' must be {'an' if kind[0] in 'aeiou' else 'a'} {kind}"
        if "enum" in properties[name] and item not in properties[name]["enum"]:
            return f"'{name}' is not one of the allowed values"
    return None


def screen(name: str, tool_input: object) -> tuple[str, str]:
    """What the manifests say about a call the model made, as ``(kind, problem)``.

    ``kind`` is ``READ_KIND``, or the kind of task a write becomes (see ``WRITE_KINDS``), or ""
    for a name no manifest offers to the model. ``problem`` is why the input does not fit
    the tool's schema ("" when it does), worded for the model. A write whose idempotency
    method no task implements is not run: its kind is "".
    """
    spec = TOOLS.get(name)
    if spec is None or not spec.offer_to_llm:
        return "", ""
    kind = READ_KIND
    if spec.is_write:
        kind = WRITE_KINDS.get(spec.idempotency or "", "")
        if not kind:
            return "", ""
    problem = check_input(spec.llm_definition()["input_schema"], tool_input)
    return kind, "" if problem is None else f"Invalid input for {name}: {problem}."


def low_risk_actions() -> list[str]:
    """The actions an admin may turn requester approval off for (D-43), by name."""
    return sorted(name for name, spec in TOOLS.items() if spec.low_risk)


def selectable_actions() -> dict[str, list[ToolSpec]]:
    """The actions an agent's creator chooses from (D-52), by service name, from the manifests.

    An action is a tool offered to the model that carries a ``label``. A new connector's
    manifest adds its rows here and nowhere else.
    """
    found: dict[str, list[ToolSpec]] = {}
    for spec in TOOLS.values():
        if spec.offer_to_llm and spec.label:
            found.setdefault(spec.service or spec.connector, []).append(spec)
    return found


def selectable_action_names() -> frozenset[str]:
    """Every selectable action's tool name."""
    return frozenset(s.name for specs in selectable_actions().values() for s in specs)


def action_label(name: str) -> str:
    """The phrase for a selectable action, or the tool name for any other."""
    spec = TOOLS.get(name)
    return spec.label if spec is not None and spec.label else name


def require_idempotency(method: str) -> None:
    """Raise unless some write in the manifests declares ``method``.

    A write activity names the idempotency method it implements. When no manifest declares
    that method for a write, the manifests describe writes this code does not make (or this
    one is no longer declared), so it stops before anything is sent.
    """
    if not any(spec.is_write and spec.idempotency == method for spec in TOOLS.values()):
        raise ValueError(f"no write in the connector manifests declares idempotency '{method}'")
