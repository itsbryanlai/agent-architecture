"""What a version changes about who can reach what, compared with the published one.

An admin approves a version knowing what it adds. ``diff_access`` lists the grants and
audience that were added, removed or changed, and says whether the version *widens*
access: any new grant, a grant pointed at other properties, a new channel, direct
messages opened wider, guests let in. A widening publish needs the approver's
acknowledgement (``ControlPlane.publish_version``), which names the published version the
diff was computed against. With no published version to compare with, everything the version
grants and every place it answers is new, so a first publish widens whenever it grants or
reaches anything. An audience that closes direct messages to nobody says so plainly, with
the way to keep them open; that is a narrowing, not a widening.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from agentos.audience import EffectiveAudience
from agentos.config import AgentVersion, Grants
from agentos.connectors.notion import normalize_id


class WideningNotAcknowledged(Exception):
    """A publish was refused because the version widens access and the approver did not say
    they had seen it."""


@dataclass
class AccessChange:
    """One line of the diff: ``kind`` is ``added``, ``removed`` or ``changed``."""

    kind: str
    what: str
    widens: bool

    def text(self) -> str:
        return f"{self.kind}: {self.what}"


@dataclass
class AccessDiff:
    baseline: bool  # False when nothing is published to compare with
    # The published version number this was computed against, or "none". A publish names it.
    published: str = "none"
    changes: list[AccessChange] = field(default_factory=list)

    @property
    def widens(self) -> bool:
        return any(c.widens for c in self.changes)


_DRIVE_KIND_LABELS = {
    "folder": "drive folder",
    "file": "drive file",
    "shared_drive": "drive shared drive",
    "sheet": "drive sheet",
}


def _grants(agent: AgentVersion) -> dict[str, tuple[str, str, tuple]]:
    """Normalised ID -> (kind, label, mapping) of every Notion and Drive grant."""
    found: dict[str, tuple[str, str, tuple]] = {}
    for db in agent.grants.notion_databases:
        mapping = (
            db.title_prop_id or db.title_prop,
            db.status_prop_id or db.status_prop,
            db.requested_by_prop_id or db.requested_by_prop,
            db.request_id_prop_id or db.request_id_prop,
            db.assignee_prop_id or db.assignee_prop,
        )
        found[normalize_id(db.id)] = ("database", db.name, mapping)
    for page in agent.grants.notion_pages:
        found[normalize_id(page.id)] = ("page", page.name, ())
    for grant in agent.grants.drive:
        # A sheet's mapping is its tab and columns: pointing the append at another tab or
        # other columns reads as "uses other properties", a widening the approver sees.
        mapping = (grant.tab, *grant.columns) if grant.kind == "sheet" else ()
        found[grant.id] = (_DRIVE_KIND_LABELS[grant.kind], grant.name, mapping)
    return found


def diff_access(
    old: AgentVersion | None,
    new: AgentVersion,
    old_audience: EffectiveAudience | None,
    new_audience: EffectiveAudience,
) -> AccessDiff:
    baseline = old is not None and old_audience is not None
    if old is None or old_audience is None:
        # Nothing published: everything the version grants and every place it answers is new.
        old = replace(new, grants=Grants())
        old_audience = EffectiveAudience(True, (), False, (), False)
    changes: list[AccessChange] = []
    before, after = _grants(old), _grants(new)
    for nid, (kind, label, mapping) in after.items():
        if nid not in before:
            changes.append(AccessChange("added", f"{kind} grant '{label}' ({nid})", True))
        elif before[nid][2] != mapping:
            changes.append(
                AccessChange(
                    "changed", f"{kind} grant '{label}' ({nid}) uses other properties", True
                )
            )
        elif before[nid][1] != label:
            changes.append(
                AccessChange("changed", f"{kind} grant ({nid}) is now labelled '{label}'", False)
            )
    for nid, (kind, label, _) in before.items():
        if nid not in after:
            changes.append(AccessChange("removed", f"{kind} grant '{label}' ({nid})", False))

    for channel in sorted(set(new_audience.channels) - set(old_audience.channels)):
        changes.append(AccessChange("added", f"channel {channel}", True))
    for channel in sorted(set(old_audience.channels) - set(new_audience.channels)):
        changes.append(AccessChange("removed", f"channel {channel}", False))
    if new_audience.any_unmapped_channel and not old_audience.any_unmapped_channel:
        changes.append(AccessChange("added", "every channel the tenant file does not map", True))
    dm_closed = not new_audience.dm_everyone and not new_audience.dm_user_groups
    if dm_closed and (old_audience.dm_everyone or old_audience.dm_user_groups or not baseline):
        # An audience that omits dm closes direct messages to nobody; say so plainly,
        # with the way to keep them open, rather than as a technical comparison.
        changes.append(
            AccessChange(
                "changed", "direct messages: nobody (add dm: everyone to keep them open)", False
            )
        )
    elif new_audience.dm_everyone != old_audience.dm_everyone:
        wider = new_audience.dm_everyone
        changes.append(
            AccessChange(
                "changed",
                "direct messages are open to everyone"
                if wider
                else "direct messages no longer open to everyone",
                wider,
            )
        )
    if not old_audience.dm_everyone:
        for group in sorted(set(new_audience.dm_user_groups) - set(old_audience.dm_user_groups)):
            changes.append(AccessChange("added", f"direct messages for user group {group}", True))
    if new_audience.guests != old_audience.guests:
        changes.append(
            AccessChange(
                "changed",
                "guests allowed" if new_audience.guests else "guests no longer allowed",
                new_audience.guests,
            )
        )
    return AccessDiff(baseline=baseline, changes=changes)
