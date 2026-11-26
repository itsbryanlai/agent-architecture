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
from agentos.connectors.registry import held_grant_kinds


@dataclass(frozen=True)
class ActionRow:
    name: str
    label: str
    on: bool


@dataclass(frozen=True)
class ActionSection:
    service: str
    rows: list[ActionRow]


@dataclass(frozen=True)
class ActionHint:
    key: str  # the action the note is about, or the service when no action of it is ticked
    text: str


def action_sections(agent: AgentVersion) -> list[ActionSection]:
    """Every selectable action by service, ticked from the version's effective actions."""
    chosen = effective_actions(agent)
    return [
        ActionSection(service, [ActionRow(s.name, s.label, s.name in chosen) for s in specs])
        for service, specs in selectable_actions().items()
    ]


def action_hints(agent: AgentVersion) -> list[ActionHint]:
    """Plain-words notes on a mismatch between the chosen actions and the grants."""
    chosen, held = effective_actions(agent), held_grant_kinds(agent.grants)
    hints: list[ActionHint] = []
    for service, specs in selectable_actions().items():
        hints += [
            ActionHint(s.name, f"{s.label} needs {s.needs_text} granted below.")
            for s in specs
            if s.name in chosen and s.needs and s.needs not in held
        ]
        if not any(s.name in chosen for s in specs):
            granted = [s.needs_text for s in specs if s.needs and s.needs in held]
            if granted:
                hints.append(
                    ActionHint(
                        service,
                        f"{granted[0][:1].upper()}{granted[0][1:]} is granted but no "
                        f"{service} action is ticked.",
                    )
                )
    return hints
