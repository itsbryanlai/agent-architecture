"""The connectors this deployment runs, keyed by the manifest's connector name.

The composition root: the one place that names concrete connectors. Activities
build a connector for an agent's grants and classify errors through here, so a
new connector is registered once and every call site reaches it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

from agentos.config import AgentVersion
from agentos.connectors import drive_errors, notion_errors
from agentos.connectors.base import Connector, ConnectorErrors
from agentos.connectors.drive import DriveConnector
from agentos.connectors.notion import NotionConnector
from agentos.tenants import TenantRuntime

ConnectorFactory = Callable[[TenantRuntime, AgentVersion], Connector]


def _notion(runtime: TenantRuntime, agent: AgentVersion) -> Connector:
    return NotionConnector(
        runtime.notion_api, agent.grants.notion_databases, users=runtime.notion_users
    )


def _drive(runtime: TenantRuntime, agent: AgentVersion) -> Connector:
    return DriveConnector(runtime.drive, agent.grants.drive)


_FACTORIES: dict[str, ConnectorFactory] = {"notion": _notion, "drive": _drive}
_ERRORS: dict[str, ConnectorErrors] = {"notion": notion_errors, "drive": drive_errors}


def connector_for(name: str, runtime: TenantRuntime, agent: AgentVersion) -> Connector:
    """The connector ``name`` limited to the grants of this version of the agent."""
    return _FACTORIES[name](runtime, agent)


def error_classes() -> Iterator[ConnectorErrors]:
    """Every registered connector's error table, for an error whose source is unknown."""
    return iter(_ERRORS.values())
