"""What an agent's creator chooses and sees about its actions (D-52).

The list of choices comes from the connector manifests (``selectable_actions``), so a new
connector adds rows with no code here. ``action_sections`` is the checkbox list grouped by
service; ``action_hints`` tells a creator, in plain words, about an action ticked without
the grant it works on and a granted service with no action ticked.
"""

from __future__ import annotations

from dataclasses import dataclass

from agentos.config import AgentVersion, effective_actions
from agentos.connectors.manifest import selectable_actions


@dataclass(frozen=True)
class ActionRow:
    name: str
    label: str
    on: bool


@dataclass(frozen=True)
class ActionSection:
    service: str
    rows: list[ActionRow]


def grant_kinds(agent: AgentVersion) -> set[str]:
    """The kinds of grant the agent holds, in the words of a manifest's ``needs``."""
    kinds: set[str] = set()
    if agent.grants.notion_databases:
        kinds.add("notion_database")
    for grant in agent.grants.drive:
        kinds.add(grant.kind)
        kinds.add("drive_item")
    return kinds


def action_sections(agent: AgentVersion) -> list[ActionSection]:
    """Every selectable action by service, ticked from the version's effective actions."""
    chosen = effective_actions(agent)
    return [
        ActionSection(service, [ActionRow(s.name, s.label, s.name in chosen) for s in specs])
        for service, specs in selectable_actions().items()
    ]


def action_hints(agent: AgentVersion) -> list[str]:
    """Plain-words notes on a mismatch between the chosen actions and the grants."""
    chosen, held = effective_actions(agent), grant_kinds(agent)
    hints: list[str] = []
    for service, specs in selectable_actions().items():
        hints += [
            f"{s.label} needs {s.needs_text} granted below."
            for s in specs
            if s.name in chosen and s.needs and s.needs not in held
        ]
        if not any(s.name in chosen for s in specs):
            granted = [s.needs_text for s in specs if s.needs and s.needs in held]
            if granted:
                hints.append(
                    f"{granted[0][:1].upper()}{granted[0][1:]} is granted but no "
                    f"{service} action is ticked."
                )
    return hints
