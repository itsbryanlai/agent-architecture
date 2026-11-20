"""Creator test sets: cases on an agent version, and runs of them against a draft.

A case is a question and an expected behaviour — answers citing a granted page,
drafts a write to a granted database, refuses, or hands off to a listed specialist.
Cases are stored per agent version and copied forward when a new draft is made.
The question's text lives here, in Postgres; a run's workflow carries only the
case's reference and its expectation (kind and target ID), never the text.

A run executes each case as a turn against the draft definition on its own task
queue (``workflows/test_run.py``). Writes never run: a drafted write is recorded
(tool and target database) and stops the case. The run's spend counts in the
tenant's usage and is capped at ``RUN_CAP_USD``; a run that reaches the cap stops
and reports the cases it did not run. Results are pass/fail per case with a fixed
reason code, references only — the version page words them with ``REASON_TEXT``.

The judge (``judge_case``) is a pure function over what a turn observably did:
the pages its final answer cites, the write it drafted, the agent it handed off
to, or a refusal. A refusal is the provider's ``refusal`` stop reason only; a
model's plain-text refusal is not detected (see the decision log). A platform
stop — a reached budget, an agent switched off or unavailable — is not a refusal:
the case ends as an error with its own fixed code.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agentos.audit import canonical_sha256
from agentos.stores import joined_transaction, tenant_transaction
from agentos.usage import utcnow

# What a case may expect. '' is a case whose expectation the creator has not chosen
# yet (the feedback page's "Make a test case" adds one); a run refuses unset cases.
CITE = "cite"
WRITE = "write"
REFUSE = "refuse"
HAND_OFF = "hand_off"
EXPECT_KINDS = (CITE, WRITE, REFUSE, HAND_OFF)

EXPECT_LABELS = {
    CITE: "Answers citing a source",
    WRITE: "Drafts a write to a database",
    REFUSE: "Refuses",
    HAND_OFF: "Hands off to an agent",
}

# The most one run may spend, in US dollars (the maintainer's cap, 2026-11-19).
RUN_CAP_USD = Decimal("0.20")
QUESTION_MAX = 2000

# The most wall time one run may take. Temporal enforces it as the workflow's execution
# timeout; the console also closes a running row older than this plus a grace as failed,
# so a run whose workflow died (or whose final write could not land) never blocks the
# version forever.
RUN_LIMIT = timedelta(hours=1)

# A run's lifecycle: the plane starts it, the worker's run finishes it.
RUNNING = "running"
DONE = "done"
FAILED = "failed"

# What became of one case.
PASS = "pass"
FAIL = "fail"
NOT_RUN = "not_run"
ERROR = "error"

# Why a case failed or was not run: fixed codes, never content.
SOURCE_NOT_CITED = "source_not_cited"
WRONG_ACTION = "wrong_action"
DID_NOT_REFUSE = "did_not_refuse"
WRONG_HAND_OFF = "wrong_hand_off"
CAP_REACHED = "cap"
SPEND_UNKNOWN = "spend_unknown"
CASE_ERROR = "error"
# A platform stop ended the turn instead of the agent answering: an error, not a
# refusal, so a REFUSE case does not pass on a reply the agent never chose.
STOPPED_BY_BUDGET = "stopped_by_budget"
AGENT_OFF = "agent_off"
AGENT_UNAVAILABLE = "agent_unavailable"

REASON_TEXT = {
    SOURCE_NOT_CITED: "the expected source was not cited",
    WRONG_ACTION: "the expected write was not drafted",
    DID_NOT_REFUSE: "the agent did not refuse",
    WRONG_HAND_OFF: "the expected hand-off did not happen",
    CAP_REACHED: "not run: the run reached its spend cap",
    SPEND_UNKNOWN: "not run: the run's spend could not be measured",
    CASE_ERROR: "the case could not be run",
    STOPPED_BY_BUDGET: "the turn was stopped by the agent's budget",
    AGENT_OFF: "the agent is turned off by an admin",
    AGENT_UNAVAILABLE: "the agent is unavailable: an admin needs to fix its access",
}

# The failure code of each expectation when the turn did something else.
_FAIL_REASON = {
    CITE: SOURCE_NOT_CITED,
    WRITE: WRONG_ACTION,
    REFUSE: DID_NOT_REFUSE,
    HAND_OFF: WRONG_HAND_OFF,
}


class TestSetError(ValueError):
    """The change names a case or run that cannot be made as asked."""

    __test__ = False  # a domain class, not a pytest test


@dataclass(frozen=True)
class TestCase:
    """One case of one agent version. The question is content; it stays in Postgres."""

    __test__ = False  # a domain class, not a pytest test

    tenant_id: str
    agent_id: str
    version: int
    case_id: str
    question: str
    expect: str = ""
    target: str = ""
    seq: int = 0
    created_by: str = ""
    created_at: datetime | None = None


@dataclass(frozen=True)
class CaseRef:
    """A case as a run's workflow may carry it: its reference and expectation, no text."""

    case_id: str
    expect: str
    target: str


@dataclass(frozen=True)
class DraftedWrite:
    """The write a case's turn drafted: its task kind and target database (a reference).

    ``database_id`` is empty when the target could not be resolved from the draft's
    grants; nothing was written either way.
    """

    tool: str
    database_id: str = ""


@dataclass(frozen=True)
class CaseResult:
    """What became of one case: references and fixed codes only."""

    case_id: str
    outcome: str
    reason: str = ""
    tool: str = ""  # the drafted write's task kind, when one was drafted
    target: str = ""  # the drafted write's database, or the hand-off's agent


@dataclass(frozen=True)
class TestRun:
    """One run of a version's cases, as the console reads it."""

    __test__ = False  # a domain class, not a pytest test

    tenant_id: str
    run_id: str
    agent_id: str
    version: int
    status: str
    started_by: str
    definition_sha256: str
    cases_sha256: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    spend_usd: Decimal = Decimal(0)
    results: list[CaseResult] = field(default_factory=list)

    def count(self, outcome: str) -> int:
        return sum(r.outcome == outcome for r in self.results)


def case_set_sha256(cases: list[TestCase] | list[CaseRef]) -> str:
    """The hash of a version's case set: ids and expectations, never the questions.

    A run records it beside the definition hash, so the console warns when either
    the draft or its cases changed since the run.
    """
    return canonical_sha256(
        [{"case_id": c.case_id, "expect": c.expect, "target": c.target} for c in cases]
    )


@dataclass(frozen=True)
class TestRunRef:
    """What a run's workflow loads once: the draft's identity and its cases' references."""

    __test__ = False  # a domain class, not a pytest test

    agent_id: str
    version: int
    started_by: str
    max_steps: int
    cases: list[CaseRef]


@dataclass(frozen=True)
class RunSpend:
    """A run's spend so far, as the cap check reports it. ``cost`` is a formatted decimal."""

    cost: str
    unpriced: int
    reached: bool


def judge_case(
    case_id: str,
    expect: str,
    target: str,
    *,
    cited_pages: list[str],
    drafted: DraftedWrite | None,
    hand_off: str | None,
    refused: bool,
    stopped: str | None = None,
) -> CaseResult:
    """The verdict for one case from what its turn observably did.

    ``cited_pages`` are the normalized page IDs the final answer cites; ``drafted``
    the write the turn stopped at; ``hand_off`` the agent a hand-off named (None when
    none happened, "" when it named no valid agent); ``refused`` whether the turn
    ended in the provider's refusal; ``stopped`` the fixed code of a platform stop
    that answered instead of the agent (a budget, a switch, the agent's health), or
    None. A case that was not run or errored is judged by the workflow, not here.
    """
    if stopped is not None:
        return CaseResult(case_id, ERROR, stopped)
    if drafted is not None:
        if expect == WRITE and drafted.database_id and drafted.database_id == target:
            return CaseResult(case_id, PASS, tool=drafted.tool, target=drafted.database_id)
        return CaseResult(
            case_id, FAIL, _FAIL_REASON[expect], tool=drafted.tool, target=drafted.database_id
        )
    if hand_off is not None:
        if expect == HAND_OFF and hand_off and hand_off == target:
            return CaseResult(case_id, PASS, target=hand_off)
        return CaseResult(case_id, FAIL, _FAIL_REASON[expect], target=hand_off)
    if refused:
        if expect == REFUSE:
            return CaseResult(case_id, PASS)
        return CaseResult(case_id, FAIL, _FAIL_REASON[expect])
    if expect == CITE and target in cited_pages:
        return CaseResult(case_id, PASS)
    return CaseResult(case_id, FAIL, _FAIL_REASON[expect])


# --- store ------------------------------------------------------------------------


class TestSetStore(Protocol):
    async def cases(self, tenant_id: str, agent_id: str, version: int) -> list[TestCase]:
        """The version's cases, in the order they were added."""
        ...

    async def add_case(self, tenant_id: str, case: TestCase, conn: Any = None) -> None:
        """Append one case to a version. ``conn`` joins a unit of work."""
        ...

    async def set_expect(
        self,
        tenant_id: str,
        agent_id: str,
        version: int,
        case_id: str,
        expect: str,
        target: str,
        conn: Any = None,
    ) -> bool:
        """Choose a case's expected behaviour; False when the case is gone."""
        ...

    async def remove_case(
        self, tenant_id: str, agent_id: str, version: int, case_id: str, conn: Any = None
    ) -> bool:
        """Remove one case; False when it is gone."""
        ...

    async def copy_cases(
        self,
        tenant_id: str,
        agent_id: str,
        from_version: int,
        to_version: int,
        conn: Any = None,
    ) -> int:
        """Copy one version's cases onto a new draft, as ``create_draft`` copies its
        definition. The caller resolves the source version. Returns how many were copied."""
        ...

    async def start_run(self, tenant_id: str, run: TestRun, conn: Any = None) -> None:
        """Record a run as running, before its workflow starts."""
        ...

    async def finish_run(
        self,
        tenant_id: str,
        run_id: str,
        results: list[CaseResult],
        spend: Decimal,
        status: str = DONE,
        conn: Any = None,
    ) -> None:
        """Store a run's results and spend, and close it."""
        ...

    async def get_run(self, tenant_id: str, run_id: str) -> TestRun | None: ...

    async def latest_run(self, tenant_id: str, agent_id: str, version: int) -> TestRun | None:
        """The version's newest run, or None when it has none."""
        ...


_CASE_COLUMNS = (
    "tenant_id, agent_id, version, case_id, question, expect, target, seq, created_by, created_at"
)
_RUN_COLUMNS = (
    "tenant_id, run_id, agent_id, version, status, started_by, definition_sha256, "
    "cases_sha256, started_at, finished_at, spend_usd, results"
)


def _case(row) -> TestCase:
    return TestCase(*row)


def _run(row) -> TestRun:
    return TestRun(*row[:11], results=[CaseResult(**r) for r in row[11]])


class PostgresTestSetStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def cases(self, tenant_id: str, agent_id: str, version: int) -> list[TestCase]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_CASE_COLUMNS} FROM test_cases
                WHERE tenant_id = %s AND agent_id = %s AND version = %s
                ORDER BY seq
                """,
                (tenant_id, agent_id, version),
            )
            return [_case(row) for row in await cur.fetchall()]

    async def add_case(self, tenant_id: str, case: TestCase, conn: Any = None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                f"""
                INSERT INTO test_cases ({_CASE_COLUMNS})
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        (SELECT COALESCE(MAX(seq), 0) + 1 FROM test_cases
                         WHERE tenant_id = %s AND agent_id = %s AND version = %s),
                        %s, now())
                """,
                (
                    case.tenant_id,
                    case.agent_id,
                    case.version,
                    case.case_id,
                    case.question,
                    case.expect,
                    case.target,
                    case.tenant_id,
                    case.agent_id,
                    case.version,
                    case.created_by,
                ),
            )

    async def set_expect(
        self,
        tenant_id: str,
        agent_id: str,
        version: int,
        case_id: str,
        expect: str,
        target: str,
        conn: Any = None,
    ) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                """
                UPDATE test_cases SET expect = %s, target = %s
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND case_id = %s
                RETURNING case_id
                """,
                (expect, target, tenant_id, agent_id, version, case_id),
            )
            return await cur.fetchone() is not None

    async def remove_case(
        self, tenant_id: str, agent_id: str, version: int, case_id: str, conn: Any = None
    ) -> bool:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                """
                DELETE FROM test_cases
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND case_id = %s
                RETURNING case_id
                """,
                (tenant_id, agent_id, version, case_id),
            )
            return await cur.fetchone() is not None

    async def copy_cases(
        self,
        tenant_id: str,
        agent_id: str,
        from_version: int,
        to_version: int,
        conn: Any = None,
    ) -> int:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            cur = await db.execute(
                f"""
                INSERT INTO test_cases ({_CASE_COLUMNS})
                SELECT c.tenant_id, c.agent_id, %s, c.case_id, c.question, c.expect,
                       c.target, c.seq, c.created_by, now()
                FROM test_cases c
                WHERE c.tenant_id = %s AND c.agent_id = %s AND c.version = %s
                RETURNING case_id
                """,
                (to_version, tenant_id, agent_id, from_version),
            )
            return len(await cur.fetchall())

    async def start_run(self, tenant_id: str, run: TestRun, conn: Any = None) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                f"""
                INSERT INTO test_runs ({_RUN_COLUMNS})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), NULL, 0, '[]')
                """,
                (
                    run.tenant_id,
                    run.run_id,
                    run.agent_id,
                    run.version,
                    run.status,
                    run.started_by,
                    run.definition_sha256,
                    run.cases_sha256,
                ),
            )

    async def finish_run(
        self,
        tenant_id: str,
        run_id: str,
        results: list[CaseResult],
        spend: Decimal,
        status: str = DONE,
        conn: Any = None,
    ) -> None:
        async with joined_transaction(self._pool, tenant_id, conn) as db:
            await db.execute(
                """
                UPDATE test_runs
                SET status = %s, results = %s, spend_usd = %s, finished_at = now()
                WHERE tenant_id = %s AND run_id = %s
                """,
                (
                    status,
                    Jsonb([r.__dict__ for r in results]),
                    spend,
                    tenant_id,
                    run_id,
                ),
            )

    async def get_run(self, tenant_id: str, run_id: str) -> TestRun | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_RUN_COLUMNS} FROM test_runs WHERE tenant_id = %s AND run_id = %s",
                (tenant_id, run_id),
            )
            row = await cur.fetchone()
        return None if row is None else _run(row)

    async def latest_run(self, tenant_id: str, agent_id: str, version: int) -> TestRun | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_RUN_COLUMNS} FROM test_runs
                WHERE tenant_id = %s AND agent_id = %s AND version = %s
                ORDER BY started_at DESC, run_id DESC
                LIMIT 1
                """,
                (tenant_id, agent_id, version),
            )
            row = await cur.fetchone()
        return None if row is None else _run(row)


class InMemoryTestSetStore:
    """The same store in memory, for tests. Mirrors the Postgres one's behaviour."""

    def __init__(self, clock: Callable[[], datetime] = utcnow):
        self._cases: dict[tuple[str, str, int], list[TestCase]] = {}
        self._runs: dict[tuple[str, str], TestRun] = {}
        self._clock = clock

    def snapshot(self) -> tuple[dict, dict]:
        """The rows as they are now, for ``InMemoryUnitOfWork`` (records are immutable)."""
        return (
            {key: list(rows) for key, rows in self._cases.items()},
            dict(self._runs),
        )

    def restore(self, state: tuple[dict, dict]) -> None:
        self._cases = {key: list(rows) for key, rows in state[0].items()}
        self._runs = dict(state[1])

    async def cases(self, tenant_id: str, agent_id: str, version: int) -> list[TestCase]:
        return sorted(self._cases.get((tenant_id, agent_id, version), []), key=lambda c: c.seq)

    async def add_case(self, tenant_id: str, case: TestCase, conn: Any = None) -> None:
        rows = self._cases.setdefault((case.tenant_id, case.agent_id, case.version), [])
        seq = max((c.seq for c in rows), default=0) + 1
        rows.append(
            TestCase(
                case.tenant_id,
                case.agent_id,
                case.version,
                case.case_id,
                case.question,
                case.expect,
                case.target,
                seq,
                case.created_by,
                self._clock(),
            )
        )

    async def set_expect(
        self,
        tenant_id: str,
        agent_id: str,
        version: int,
        case_id: str,
        expect: str,
        target: str,
        conn: Any = None,
    ) -> bool:
        rows = self._cases.get((tenant_id, agent_id, version), [])
        for i, case in enumerate(rows):
            if case.case_id == case_id:
                rows[i] = TestCase(
                    case.tenant_id,
                    case.agent_id,
                    case.version,
                    case.case_id,
                    case.question,
                    expect,
                    target,
                    case.seq,
                    case.created_by,
                    case.created_at,
                )
                return True
        return False

    async def remove_case(
        self, tenant_id: str, agent_id: str, version: int, case_id: str, conn: Any = None
    ) -> bool:
        rows = self._cases.get((tenant_id, agent_id, version), [])
        for i, case in enumerate(rows):
            if case.case_id == case_id:
                del rows[i]
                return True
        return False

    async def copy_cases(
        self,
        tenant_id: str,
        agent_id: str,
        from_version: int,
        to_version: int,
        conn: Any = None,
    ) -> int:
        source = self._cases.get((tenant_id, agent_id, from_version), [])
        target = self._cases.setdefault((tenant_id, agent_id, to_version), [])
        now = self._clock()
        for case in source:
            target.append(
                TestCase(
                    tenant_id,
                    agent_id,
                    to_version,
                    case.case_id,
                    case.question,
                    case.expect,
                    case.target,
                    case.seq,
                    case.created_by,
                    now,
                )
            )
        return len(source)

    async def start_run(self, tenant_id: str, run: TestRun, conn: Any = None) -> None:
        self._runs[(run.tenant_id, run.run_id)] = TestRun(
            run.tenant_id,
            run.run_id,
            run.agent_id,
            run.version,
            run.status,
            run.started_by,
            run.definition_sha256,
            run.cases_sha256,
            started_at=self._clock(),
        )

    async def finish_run(
        self,
        tenant_id: str,
        run_id: str,
        results: list[CaseResult],
        spend: Decimal,
        status: str = DONE,
        conn: Any = None,
    ) -> None:
        run = self._runs.get((tenant_id, run_id))
        if run is None:
            return
        self._runs[(tenant_id, run_id)] = TestRun(
            run.tenant_id,
            run.run_id,
            run.agent_id,
            run.version,
            status,
            run.started_by,
            run.definition_sha256,
            run.cases_sha256,
            started_at=run.started_at,
            finished_at=self._clock(),
            spend_usd=spend,
            results=list(results),
        )

    async def get_run(self, tenant_id: str, run_id: str) -> TestRun | None:
        return self._runs.get((tenant_id, run_id))

    async def latest_run(self, tenant_id: str, agent_id: str, version: int) -> TestRun | None:
        runs = [
            r
            for (t, _), r in self._runs.items()
            if t == tenant_id and r.agent_id == agent_id and r.version == version
        ]
        if not runs:
            return None
        return max(runs, key=lambda r: (r.started_at, r.run_id))
