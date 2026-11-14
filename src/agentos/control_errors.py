"""How the control plane's refusals map to HTTP statuses.

The JSON API and the web pages both turn a ``ControlPlane`` refusal into a
status and show its message, so the mapping lives here once.
"""

from agentos.agent_store import AgentNotFound, AgentStoreError
from agentos.config import ConfigError
from agentos.control import Forbidden
from agentos.grant_checker import GrantCheckUnavailable, GrantProblems
from agentos.members import InvalidMember, MemberError, MemberNotFound

STATUS_BY_ERROR: dict[type[Exception], int] = {
    Forbidden: 403,
    AgentNotFound: 404,
    MemberNotFound: 404,
    InvalidMember: 422,
    AgentStoreError: 409,
    MemberError: 409,
    ConfigError: 422,
    GrantProblems: 409,
    GrantCheckUnavailable: 502,
}

REFUSALS = tuple(STATUS_BY_ERROR)


def status_of(err: Exception) -> int | None:
    """The status for a refusal the control plane raises, or None for anything else.

    The nearest class in the error's own hierarchy decides, so ``AgentNotFound``
    is a 404 although its parent ``AgentStoreError`` is a 409.
    """
    for cls in type(err).__mro__:
        if cls in STATUS_BY_ERROR:
            return STATUS_BY_ERROR[cls]
    return None
