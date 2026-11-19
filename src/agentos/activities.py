"""Temporal activities: every piece of I/O the workflows do.

Rules kept here:

* Every activity works for exactly one tenant, named by its arguments: the
  ``TurnContext`` / ``TaskSpec`` tenant, or an explicit ``tenant_id``. It uses
  only that tenant's Slack and Notion clients, and an unknown tenant is a
  non-retryable failure, never a fallback to some other tenant.
* The agent uses its own access. Notion calls go through a ``NotionConnector``
  built from the grants of the agent's current published version, read from
  the agent store at call time, which checks every call before it is sent. A
  grant removed by a new published version applies to the next call, also for
  a write that was approved earlier. A turn's model steps and audience check
  read the version the turn started with instead (``_pinned``), so a publish
  in the middle of a turn does not change its prompt or tools.
* The requester comes from ``TurnContext`` / ``TaskSpec``, which the runtime
  filled in from the Slack event. Requester-scoped tool fields (the person
  whose records are read) are filled in here, never by the LLM.
* Connector errors that a retry cannot fix become non-retryable failures.
* An approved Notion write (a new item, a comment) is made exactly once. Notion has no
  idempotency key and its search lags a write, so Postgres decides which attempt writes:
  the activity claims the key in ``external_writes`` with a lease, sends the write, and
  stores the result in the same activity. A retry returns the stored result; an attempt
  that finds a live lease writes nothing and fails with a retryable "in progress" error;
  an attempt that takes over from one that may have sent the write looks for it before
  writing again. The requester's Notion user is resolved when the card is posted, and the
  line that goes into the page body when there is none is shown on that card.
* Slack posts are idempotent: the posted ``ts`` is stored under an idempotency
  key, and a repeat call with the same key returns it without posting again.
* Tool output text is stored by the activity that ran the tool and is put into
  chat history by ``append_tool_results``. Workflow history carries only a
  reference (``ToolResult.stored``), never the text a tool read.
* Chat and document content stays out of Temporal history (``agentos.content_store``): the
  activities that handle it take IDs and read or write the text in Postgres themselves.
  ``ingest_message`` appends the text a person wrote; ``call_model`` stores the model's
  reply, and each tool call's input by ID, and returns only what the workflow decides on;
  ``finalize_model_reply`` shows a stored reply; the tool and task activities read inputs
  and task payloads by reference. The older activities that take content as an argument
  (``call_llm``, ``finalize_reply``, ``run_read_tool``) stay for histories recorded before.
* Retrieved document text is untrusted data. It is wrapped in ``<source>`` tags
  and the system prompt tells the model never to follow instructions in it.
  Knowledge comes only from the agent's own granted pages, and each page is
  re-checked with Notion before the model sees it.
* Every action is audited by the activity that does it (see ``agentos.audit``):
  an audit write that fails fails the activity, so Temporal retries it and
  nothing is done without a record. Events hold IDs and hashes, never text.
* Kill switches (see ``agentos.switches``) are read from the store at every check, with
  no cache: a turned-off agent answers with a fixed text and calls no model, a turned-off
  connector fails its tool calls, and turned-off writes refuse an approved write before
  the external call. The checks live in these activities, never in workflow code.
* Log lines carry tenant, agent and workflow IDs only, never message text,
  document content or secrets.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import NoReturn, TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agentos import ids, task_notices
from agentos.agent_health import (
    UNAVAILABLE as HEALTH_UNAVAILABLE,
)
from agentos.agent_health import (
    AgentUnavailable,
    HealthStore,
    InMemoryAgentHealthStore,
    agent_unavailable_text,
)
from agentos.agent_store import AgentStore
from agentos.audience import (
    HEALTH,
    OFF,
    PAUSED,
    UNAVAILABLE,
    AudienceChecker,
    AudienceDecision,
    no_longer_act_text,
)
from agentos.audience_pauses import InMemoryPauseStore, PauseStore
from agentos.audit import AuditStore, agent_identity, canonical_sha256
from agentos.budgets import SPEND_UNKNOWN, BudgetGuard
from agentos.budgets import STOP_REASON as BUDGET_STOP_REASON
from agentos.config import AGENT_ID_PATTERN, AgentVersion, Settings, embed_max_distance
from agentos.connectors import notion_errors
from agentos.connectors.manifest import (
    MANIFEST_DIR,
    TOOLS,
    load_manifest,
    require_idempotency,
    screen,
)
from agentos.connectors.notion import (
    ConnectorError,
    NotionConnector,
    normalize_id,
)
from agentos.connectors.notion_limiter import bulk_priority
from agentos.content_store import ContentStore, InMemoryContentStore, StoredStep
from agentos.feedback import answer_blocks
from agentos.history_view import prepare_for_model
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.search import (
    ITEM_RULES,
    KNOWLEDGE_RULES,
    SEARCH_CANDIDATES,
    PageChecker,
    cited_markers,
    escape_attribute,
    format_sources,
    searched_footer,
    searched_pages,
    select_passages,
    sources_footer,
)
from agentos.knowledge.store import ChunkHit, KnowledgeStore
from agentos.llm import LLMClient
from agentos.models import (
    CUT_SHORT,
    EDITABLE_FIELDS,
    NO_ANSWER,
    AuditEvent,
    Citation,
    DraftOutcome,
    LLMResponse,
    NotionItem,
    StepResult,
    TaskSpec,
    ToolCall,
    ToolCallRef,
    ToolResult,
    TurnContext,
    assistant_blocks,
    check_field,
)
from agentos.slack_client import (
    SlackHistoryUnavailable,
    SlackLookupError,
    SlackPoster,
    post_metadata,
)
from agentos.slack_format import (
    escape_slack,
    mentioned_user_ids,
    render_mentions,
    to_slack_mrkdwn,
)
from agentos.stores import (
    ExternalWriteStore,
    HistoryStore,
    IdempotencyStore,
    InMemoryExternalWriteStore,
    TaskLinkStore,
    ToolOutputStore,
)
from agentos.switches import (
    NOTION,
    SWITCHED_OFF,
    AgentOff,
    Effect,
    InMemorySwitchStore,
    SwitchStore,
    agent_off_card_text,
    agent_off_text,
    connector_off_text,
    log_stop,
    read_state,
)
from agentos.tasks import (
    REFRESH_WAIT,
    InMemoryTaskStore,
    TaskRecord,
    TaskStore,
    format_task_list,
    listed_state,
)
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant
from agentos.threads import (
    InMemoryOwnerPingStore,
    InMemoryThreadStore,
    OwnerPingStore,
    ThreadStore,
)
from agentos.usage import UsageRecorder
from agentos.workflows import ACTIVITY_TIMEOUT, WRITE_LEASE_SECONDS

log = logging.getLogger("agentos.activities")

MAX_LISTED_ITEMS = 20
HAND_OFF_TOOL = "hand_off"
MY_TASKS_TOOL = "my_tasks"
# Published versions are immutable, so one is kept per (tenant, agent, version). A worker
# holds the versions its running turns use, and few of them.
VERSION_CACHE_SIZE = 256

# An attempt that takes over from one that may have sent a write looks for the result
# this many times, this far apart, before it writes again. Notion's search can lag.
VISIBILITY_TRIES = 4
VISIBILITY_INTERVAL = 2.5
# A post that may have been sent is looked up in the channel's history from this long before
# the lookup; a thread post is looked up in the thread, whatever its age.
POST_LOOKUP_WINDOW_SECONDS = 3600.0
# A post's lease outlasts the activity timeout, so a live attempt always holds it, but ends
# soon enough that the retries after a crashed worker (the activity timeout, then backoff) get it.
POST_LEASE_SECONDS = ACTIVITY_TIMEOUT.total_seconds() + 5
# A retryable failure: another attempt holds the lease for this write.
WRITE_IN_PROGRESS = "WriteInProgress"
REQUESTER_NAME_MAX = 80

# Stop reasons whose reply is a fixed runtime text (agent off or unavailable, budget
# reached, spend unknown), not a model answer: it carries no feedback buttons.
FIXED_STOP_REASONS = frozenset({SWITCHED_OFF, BUDGET_STOP_REASON, SPEND_UNKNOWN})

T = TypeVar("T")


class _StopLooking(Exception):
    """Raised by a ``find`` that cannot look: ``_write_once`` then writes without more tries."""


def _beat() -> None:
    """Tell Temporal the attempt is alive. A no-op outside an activity (a direct test call)."""
    try:
        activity.heartbeat()
    except RuntimeError:
        pass


def requested_by_line(display_name: str, agent_name: str) -> str:
    """The sentence a page carries when its requester has no Notion user."""
    return f"Requested by {display_name[:REQUESTER_NAME_MAX]} via {agent_name[:REQUESTER_NAME_MAX]}"


def _item_ref(item: NotionItem) -> str:
    return json.dumps(
        {"page_id": item.page_id, "url": item.url, "title": item.title, "status": item.status}
    )


def _item_from_ref(ref: str) -> NotionItem:
    return NotionItem(**json.loads(ref))


def _non_retryable(err: Exception) -> ApplicationError:
    return ApplicationError(str(err), type=type(err).__name__, non_retryable=True)


def _tool_connector(name: str) -> str:
    """The manifest connector of a tool, for the gate's connector scope.

    A name no manifest declares answers to the Notion switch, as every read did before
    the platform manifest existed; the call itself is refused as unknown either way.
    """
    spec = TOOLS.get(name)
    return spec.connector if spec is not None and spec.connector else NOTION


def _is_client_error(err: Exception) -> bool:
    """A 4xx (other than 408 and 429) will not succeed on retry.

    A Notion API error is judged by its error code (``notion_errors``): a 409 conflict is
    retried and a missing page is not. The Anthropic and OpenAI SDK errors carry the HTTP
    status in ``status_code``, and anything else with a ``status`` is judged by it.
    Timeouts (408) and rate limits (429) are transient, so they stay retryable.
    """
    if notion_errors.is_notion_error(err):
        return notion_errors.is_final(err)
    for attr in ("status", "status_code"):
        status = getattr(err, attr, None)
        if isinstance(status, int) and not isinstance(status, bool):
            return 400 <= status < 500 and status not in (408, 429)
    return False


def _reraise(err: Exception) -> NoReturn:
    """Raise a retryable error for Temporal to retry: as it is, or, when Notion named how
    long to wait (``Retry-After`` on a 429), as a failure that carries that wait."""
    delayed = notion_errors.retry_failure(err)
    if delayed is not None:
        raise delayed from err
    raise err


def _activity_ref(fallback_workflow_id: str) -> tuple[str, str, str, int]:
    """Workflow ID, run ID, activity ID and attempt number of the running activity.

    The first three name one activity of one run and stay the same when Temporal
    retries it. The attempt number tells the tries apart, so each try that the
    provider answered (and billed) gets its own usage row. Outside an activity (a
    direct call in a test) there is no such name, so a random one stands in and
    nothing is deduplicated.
    """
    try:
        info = activity.info()
    except RuntimeError:
        return fallback_workflow_id, uuid.uuid4().hex, "0", 1
    return info.workflow_id, info.workflow_run_id, info.activity_id, info.attempt


def hand_off_tool(specialists: list[AgentVersion]) -> dict:
    """The tool a front door uses to give the thread to one of its specialists."""
    listing = "\n".join(f"- {s.agent_id}: {s.name}. {s.description}" for s in specialists)
    return {
        "name": HAND_OFF_TOOL,
        "description": (
            "Hand this conversation to a specialist agent, who answers the user's message "
            f"and owns the rest of the thread. Specialists:\n{listing}"
        ),
        "input_schema": {
            "type": "object",
            "properties": {"agent": {"type": "string", "enum": [s.agent_id for s in specialists]}},
            "required": ["agent"],
        },
    }


class AgentActivities:
    def __init__(
        self,
        settings: Settings,
        registry: TenantRegistry,
        history: HistoryStore,
        idem: IdempotencyStore,
        links: TaskLinkStore,
        llm: LLMClient,
        tool_outputs: ToolOutputStore,
        audit: AuditStore,
        agents: AgentStore,
        knowledge: KnowledgeStore | None = None,
        embedder: Embedder | None = None,
        audience: AudienceChecker | None = None,
        usage: UsageRecorder | None = None,
        budgets: BudgetGuard | None = None,
        pauses: PauseStore | None = None,
        switches: SwitchStore | None = None,
        external_writes: ExternalWriteStore | None = None,
        threads: ThreadStore | None = None,
        owner_pings: OwnerPingStore | None = None,
        content: ContentStore | None = None,
        health: HealthStore | None = None,
        tasks: TaskStore | None = None,
    ):
        self.settings = settings
        self.content = content or InMemoryContentStore()
        self.threads = threads or InMemoryThreadStore()
        self.owner_pings = owner_pings or InMemoryOwnerPingStore()
        # Tests move the day the owner is mentioned for by replacing this.
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)
        self._versions: OrderedDict[tuple[str, str, int], AgentVersion] = OrderedDict()
        self.writes = external_writes or InMemoryExternalWriteStore()
        self.visibility_tries = VISIBILITY_TRIES
        self.visibility_interval = VISIBILITY_INTERVAL
        self._history_refusals_logged: set[tuple[str, str]] = set()
        self.switches = switches or InMemorySwitchStore()
        self.health = health or InMemoryAgentHealthStore()
        self.tasks = tasks or InMemoryTaskStore()
        # How long the in-thread wait for a poke-answered poll may take. Tests move it.
        self.refresh_wait_seconds = REFRESH_WAIT.total_seconds()
        self.usage = usage
        self.budgets = budgets
        self.registry = registry
        self.pauses = pauses or InMemoryPauseStore()
        self.audience = audience or AudienceChecker(
            registry, agents, pauses=self.pauses, switches=self.switches, health=self.health
        )
        self.knowledge = knowledge
        self.embedder = embedder
        self.knowledge_tool = load_manifest(MANIFEST_DIR / "knowledge.yaml").tools[
            "search_knowledge"
        ]
        self.history = history
        self.tool_outputs = tool_outputs
        self.audit = audit
        self.agents = agents
        self.idem = idem
        self.links = links
        self.llm = llm

    def all(self) -> list:
        return [
            self.load_agent,
            self.append_history,
            self.append_tool_results,
            self.call_llm,
            self.post_placeholder,
            self.update_progress,
            self.finalize_reply,
            self.post_final_reply,
            self.finalize_searched_reply,
            self.post_final_searched_reply,
            self.finalize_notice,
            self.post_final_notice,
            self.run_read_tool,
            self.post_approval_card,
            self.update_card,
            self.update_card_line,
            self.refresh_card,
            self.prepare_comment,
            self.add_notion_comment,
            self.create_notion_item,
            self.reconcile_notion_comment,
            self.reconcile_notion_item,
            self.get_notion_item,
            self.post_in_thread,
            self.post_notice_in_thread,
            self.record_hand_off,
            self.check_audience,
            self.check_before_write,
            self.record_audience_refusal,
            self.record_approval_closed,
            self.thread_agent,
            self.remember_thread_agent,
            self.ingest_message,
            self.discard_message,
            self.call_model,
            self.finalize_model_reply,
            self.post_final_model_reply,
            self.finalize_searched_model_reply,
            self.post_final_searched_model_reply,
            self.run_stored_read_tool,
            self.draft_task,
            self.prepare_stored_comment,
            self.apply_edit,
            self.post_task_notice,
            self.watch_notion_item,
            self.record_task_state,
            self.task_in_thread,
            self.wait_task_refresh,
        ]

    # --- helpers ---------------------------------------------------------------

    def _log(self, name: str, tenant_id: str, agent_id: str, **extra: str) -> None:
        try:
            workflow_id = activity.info().workflow_id
        except RuntimeError:
            workflow_id = "-"
        fields = " ".join(f"{k}={v}" for k, v in extra.items())
        log.info(
            "activity=%s tenant_id=%s agent_id=%s workflow_id=%s %s",
            name,
            tenant_id,
            agent_id,
            workflow_id,
            fields,
        )

    def _runtime(self, tenant_id: str) -> TenantRuntime:
        """The clients and config of the tenant an activity works for.

        An unknown tenant is a retryable failure, never answered with another
        tenant's runtime: a tenant installed seconds ago appears on the worker's
        next directory refresh (every 5 s), and the shared activity retry policy
        (5 attempts, 1 s doubling to 8 s, about 15 s in total) waits that out.
        """
        try:
            return self.registry.get(tenant_id)
        except UnknownTenant as err:
            raise ApplicationError(str(err), type="UnknownTenant") from err

    async def _agent(self, tenant_id: str, agent_id: str) -> AgentVersion:
        """The agent's current published version, read from the store on every call."""
        self._runtime(tenant_id)
        agent = await self.agents.published(tenant_id, agent_id)
        if agent is None:
            raise ApplicationError(
                f"unknown agent '{agent_id}'", type="ValueError", non_retryable=True
            )
        return agent

    async def _pinned(self, ctx: TurnContext) -> AgentVersion:
        """The version of the agent the turn started with, however often it was published since.

        A turn is pinned: the model steps and the audience check of one turn read the same
        prompt, tools and specialists, and the audit and usage rows that name
        ``ctx.agent_version`` describe what actually ran. Activities that act with the agent's
        grants do not use this: they read the live published version (``_agent``), so a grant
        revoked after a turn started still stops what that turn proposed.
        """
        key = (ctx.tenant_id, ctx.agent_id, ctx.agent_version)
        cached = self._versions.get(key)
        if cached is not None:
            self._versions.move_to_end(key)
            return cached
        self._runtime(ctx.tenant_id)
        record = await self.agents.get_version(ctx.tenant_id, ctx.agent_id, ctx.agent_version)
        if record is None:
            raise ApplicationError(
                f"unknown version {ctx.agent_version} of agent '{ctx.agent_id}'",
                type="ValueError",
                non_retryable=True,
            )
        agent = record.agent()
        self._versions[key] = agent
        while len(self._versions) > VERSION_CACHE_SIZE:
            self._versions.popitem(last=False)
        return agent

    def _connector_for(self, tenant_id: str, agent: AgentVersion) -> NotionConnector:
        """A connector limited to the grants of this version of the agent."""
        runtime = self._runtime(tenant_id)
        return NotionConnector(
            runtime.notion_api, agent.grants.notion_databases, users=runtime.notion_users
        )

    async def _connector(self, tenant_id: str, agent_id: str) -> NotionConnector:
        return self._connector_for(tenant_id, await self._agent(tenant_id, agent_id))

    async def _gate_refusal(
        self, tenant_id: str, agent_id: str, effect: Effect, connector: str = NOTION
    ) -> Exception | None:
        """Why the agent may not do ``effect`` now, or None. The one place the gate is read.

        Every activity that acts for an agent calls this (or ``_gate``) first, so one read
        of the switches applies the agent, writes and connector scopes that matter to the
        effect, and one read of the agent's health stops an agent whose grant check failed.
        An admin's switch is checked first; health covers every effect. The connector scope
        covers the tool's own connector (``_tool_connector``), Notion by default. Read from
        the stores now, never cached. An unreadable switch or health row raises, so the step
        fails and is retried rather than let through.
        """
        state = await read_state(self.switches, tenant_id)
        refusal = state.refusal(agent_id, effect, connector)
        if refusal is None:
            row = await self.health.get(tenant_id, agent_id)
            if row is not None and row.status == HEALTH_UNAVAILABLE:
                agent = await self._agent(tenant_id, agent_id)
                refusal = AgentUnavailable(agent.name)
        if refusal is not None:
            log.info(
                "%s refused tenant_id=%s agent_id=%s reason=%s",
                effect.value,
                tenant_id,
                agent_id,
                type(refusal).__name__,
            )
        return refusal

    async def _gate(self, tenant_id: str, agent_id: str, effect: Effect) -> None:
        """Stop for good, before any call, when a switch covers the effect.

        Non-retryable: a retry within the minute would find it off again. The next step
        reads the switches afresh.
        """
        refusal = await self._gate_refusal(tenant_id, agent_id, effect)
        if refusal is not None:
            raise _non_retryable(refusal)

    async def _spec_payload(self, spec: TaskSpec) -> dict:
        """The task's payload: the revision its ``draft_ref`` names, else what it carries.

        A write reads the revision the approval named (``spec.revision``), never the latest.
        """
        if not spec.draft_ref:
            return spec.payload
        payload = await self.content.get_draft(spec.ctx.tenant_id, spec.draft_ref, spec.revision)
        if payload is None:
            raise ApplicationError(
                f"task draft revision {spec.revision} is missing",
                type="ValueError",
                non_retryable=True,
            )
        return payload

    async def _gate_write(self, spec: TaskSpec, task_id: str, idem_key: str, target_field: str):
        """Refuse an approved write, before any external call, while a switch covers it.

        The approval and the refusal are audited (the write as an error), and the
        failure is non-retryable, so the task ends with the reason and nothing was
        changed. The requester can ask again later. ``target_field`` names the payload
        field that holds what the write is aimed at (a page or a database).
        """
        ctx = spec.ctx
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.WRITE)
        if refusal is None:
            return
        payload = await self._spec_payload(spec)
        target = payload[target_field]
        await self._record_approval(spec, payload, task_id, target)
        await self._record_write(spec, payload, task_id, idem_key, target, "error")
        raise _non_retryable(refusal)

    async def _check_method(
        self,
        spec: TaskSpec,
        payload: dict,
        task_id: str,
        target: str,
        idem_key: str,
        method: str,
    ) -> None:
        """Stop an approved write whose idempotency method no connector manifest declares.

        The manifest says how a retried write avoids a duplicate; the activity makes the write
        one way. When they disagree the write is refused before anything is sent, audited like
        any other refused write, and not retried.
        """
        try:
            require_idempotency(method)
        except ValueError as err:
            await self._record_approval(spec, payload, task_id, target)
            await self._record_write(spec, payload, task_id, idem_key, target, "error")
            raise _non_retryable(err) from err

    async def _requester(self, spec: TaskSpec, task_id: str, agent: AgentVersion) -> dict:
        """Who asked, as Notion will show it: ``{"notion_user_id", "line"}``.

        Resolved once per task, when the card is posted, and kept: the line the card shows
        before approval is the line the page gets, and a lookup that changes in between
        cannot change what was approved. The Notion user is the one whose email matches
        the Slack profile email, from the tenant's cache. With none, ``line`` is the
        sentence for the page body and ``notion_user_id`` is None.
        """
        ctx = spec.ctx
        stored = await self.idem.get(ctx.tenant_id, f"{task_id}:requester")
        if stored is not None:
            return stored
        runtime = self._runtime(ctx.tenant_id)
        email = await runtime.slack.user_email(ctx.requester_user_id)
        user_id = await runtime.notion_users.user_id(runtime.notion_api, email)
        line = None
        if user_id is None:
            info = await runtime.slack.user_info(ctx.requester_user_id)
            line = requested_by_line(
                info.display_name or email or ctx.requester_user_id, agent.name
            )
        return await self.idem.put_if_absent(
            ctx.tenant_id, f"{task_id}:requester", {"notion_user_id": user_id, "line": line}
        )

    async def _write_once(
        self,
        tenant_id: str,
        idem_key: str,
        find: Callable[[], Awaitable[T | None]],
        write: Callable[[], Awaitable[T]],
        to_ref: Callable[[T], str],
        from_ref: Callable[[str], T],
        lease_seconds: float = WRITE_LEASE_SECONDS,
        issued: bool = False,
        look_first: bool = True,
    ) -> T:
        """Make one external write happen once, however many attempts run it.

        A stored result is returned without touching the target. Otherwise the key is
        claimed with a lease that outlasts the activity timeout. Another attempt's live
        lease means this one writes nothing and fails with a retryable error. After a
        claim that took over from an attempt that had sent the write, ``find`` is tried
        a few times first, because the target's search can lag. Then the write is marked
        as about to be sent, sent, and its result stored right away. Every step after the
        claim presents the claim's token, and the store ignores a token whose lease was
        taken over: a stale attempt cannot end the new holder's lease, mark a write, or
        store a result, and one that finds it lost the lease writes nothing. Any failure ends
        the lease at once so the retry does not wait for it, but keeps the mark, so that
        retry looks before it writes.

        The look-before-write window is bounded by ``VISIBILITY_TRIES`` and
        ``VISIBILITY_INTERVAL`` (and the task's by ``RECONCILE_TRIES`` and
        ``RECONCILE_INTERVAL``). A target whose search lags a write by longer than that
        window gets a second item from the retry: that residual risk is accepted, and
        widening the window is the way to narrow it.

        ``issued`` says the caller knows an earlier attempt may have sent the write, though
        the store has no mark of it (a record from before the store was used for it). A
        ``find`` that raises ``_StopLooking`` has no way to look: the write is made at once. A fresh claim looks once
        before writing unless ``look_first`` is False (a write the target is known not to hold,
        where a look is a wasted call).
        """
        claim = await self.writes.claim(tenant_id, idem_key, lease_seconds)
        if claim.status == "done":
            return from_ref(claim.result_ref)
        if claim.status == "busy":
            raise self._in_progress()
        try:
            made = None
            tries = self.visibility_tries if claim.issued or issued else int(look_first)
            for look in range(tries):
                if look:
                    _beat()
                    await asyncio.sleep(self.visibility_interval)
                try:
                    made = await find()
                except _StopLooking:
                    break
                if made is not None:
                    break
            if made is None:
                _beat()
                if not await self.writes.mark_issued(
                    tenant_id, idem_key, claim.token, lease_seconds
                ):
                    raise self._in_progress()
                made = await write()
            _beat()
            stored = await self.writes.complete(tenant_id, idem_key, claim.token, to_ref(made))
            if stored is None:
                raise self._in_progress()
            return from_ref(stored)
        except Exception:
            await self.writes.release(tenant_id, idem_key, claim.token)
            raise

    @staticmethod
    def _in_progress() -> ApplicationError:
        return ApplicationError(
            "another attempt is writing this; trying again shortly", type=WRITE_IN_PROGRESS
        )

    async def _record(
        self,
        ctx: TurnContext,
        kind: str,
        event_key: str,
        *,
        outcome: str = "ok",
        conversation_id: str | None = None,
        task_id: str | None = None,
        tool: str | None = None,
        target: str | None = None,
        input_sha256: str | None = None,
        output_sha256: str | None = None,
        approver_user_id: str | None = None,
    ) -> None:
        """Append one audit event for the agent and requester in ``ctx``."""
        await self.audit.record(
            AuditEvent(
                tenant_id=ctx.tenant_id,
                event_key=event_key,
                kind=kind,
                agent_id=ctx.agent_id,
                agent_version=ctx.agent_version,
                requester_user_id=ctx.requester_user_id,
                identity=agent_identity(ctx.agent_id),
                outcome=outcome,
                conversation_id=conversation_id,
                task_id=task_id,
                tool=tool,
                target=target,
                input_sha256=input_sha256,
                output_sha256=output_sha256,
                approver_user_id=approver_user_id,
            )
        )

    async def _record_approval(
        self, spec: TaskSpec, payload: dict, task_id: str, target: str
    ) -> None:
        """Record that the requester approved exactly this payload, before it runs.

        The task workflow accepts a decision only from the requester, so the
        approver is the requester. Nothing reaches this activity until then.
        ``payload`` is the revision the approval named; the event holds its hash.
        """
        ctx = spec.ctx
        await self._record(
            ctx,
            "approval",
            ids.audit_approval_key(task_id, spec.approval_nonce),
            task_id=task_id,
            tool=spec.tool_name,
            target=target,
            input_sha256=canonical_sha256(payload),
            approver_user_id=ctx.requester_user_id,
        )

    async def _record_write(
        self,
        spec: TaskSpec,
        payload: dict,
        task_id: str,
        idem_key: str,
        target: str,
        outcome: str,
        output: object = None,
    ) -> None:
        ctx = spec.ctx
        await self._record(
            ctx,
            "write",
            ids.audit_write_key(idem_key, outcome),
            outcome=outcome,
            task_id=task_id,
            tool=spec.tool_name,
            target=target,
            input_sha256=canonical_sha256(payload),
            output_sha256=None if output is None else canonical_sha256(output),
            approver_user_id=ctx.requester_user_id,
        )

    async def _post_once(
        self,
        tenant_id: str,
        idem_key: str,
        channel: str,
        text: str,
        thread_ts: str | None,
        blocks: list[dict] | None = None,
    ) -> str:
        """Post once per ``idem_key`` and return the message ts.

        Slack has no idempotency key, so the post is made once the way a Notion write is
        (``_write_once``): one lease per post, so overlapping attempts never both post, and
        the ts stored as the result. Every post carries a marker in its message metadata (a
        hash of the tenant and the key, never content). An attempt that follows one that got
        as far as sending first looks the post up by that marker, in the thread's replies or
        in the channel's recent history, and uses its ts if Slack has it. Only when nothing
        is found does it post.

        The lookup needs the history scopes. When Slack refuses it (a missing scope, or a
        channel the bot has not joined) the post is made again and a possible duplicate is
        logged with the tenant and key; the scope is named once per tenant.

        A ts already stored under ``idem_key`` is returned without a call. A ``:pending``
        marker written by an earlier version, with no ts, counts as an attempt that sent.
        """
        existing = await self.idem.get(tenant_id, idem_key)
        if existing is not None:
            return existing["ts"]
        sent_before = await self.idem.get(tenant_id, f"{idem_key}:pending") is not None
        slack = self._runtime(tenant_id).slack
        marker = ids.slack_post_marker(tenant_id, idem_key)
        since = self.clock().timestamp() - POST_LOOKUP_WINDOW_SECONDS

        async def find() -> str | None:
            try:
                return await slack.find_post(channel, thread_ts or None, marker, since)
            except SlackHistoryUnavailable as refused:
                self._note_history_refused(tenant_id, refused)
                log.warning(
                    "possible duplicate Slack post tenant_id=%s idem_key=%s", tenant_id, idem_key
                )
                raise _StopLooking from refused

        ts = await self._write_once(
            tenant_id,
            ids.slack_post_write_key(idem_key),
            find=find,
            write=lambda: slack.post_message(
                channel,
                text,
                thread_ts=thread_ts,
                blocks=blocks,
                metadata=post_metadata(marker),
            ),
            to_ref=lambda ts: ts,
            from_ref=lambda ref: ref,
            lease_seconds=POST_LEASE_SECONDS,
            issued=sent_before,
            look_first=False,
        )
        stored = await self.idem.put_if_absent(tenant_id, idem_key, {"channel": channel, "ts": ts})
        return stored["ts"]

    def _note_history_refused(self, tenant_id: str, refused: SlackHistoryUnavailable) -> None:
        """Say once per tenant that Slack refused to show history, and what it lacks."""
        what = refused.scope or refused.code
        if (tenant_id, what) in self._history_refusals_logged:
            return
        self._history_refusals_logged.add((tenant_id, what))
        log.warning(
            "Slack refused to read history, so a retried post cannot be looked up "
            "tenant_id=%s missing=%s",
            tenant_id,
            what,
        )

    # --- conversation ------------------------------------------------------------

    @activity.defn(name="load_agent")
    async def load_agent(self, tenant_or_agent: str, agent_id: str | None = None) -> AgentVersion:
        """Return the agent's current definition for the named tenant.

        Called as ``load_agent(tenant_id, agent_id)``.

        Replay compatibility: workflows that were already running when the
        tenant argument was added call this with one argument, the agent ID. A
        one-argument call is read that way, and the tenant is the registry's
        only tenant; with several tenants it fails for good, because the
        tenant cannot be known. Remove this fallback in a later cleanup, once
        no workflow that started before the change is still running.
        """
        if agent_id is None:
            agent_id = tenant_or_agent
            tenant_id = self.registry.only_tenant()
            if tenant_id is None:
                raise ApplicationError(
                    "load_agent was called without a tenant, and more than one tenant is "
                    "configured",
                    type="UnknownTenant",
                    non_retryable=True,
                )
        else:
            tenant_id = tenant_or_agent
        self._log("load_agent", tenant_id, agent_id)
        return await self._agent(tenant_id, agent_id)

    @activity.defn(name="append_history")
    async def append_history(
        self,
        tenant_id: str,
        conversation_id: str,
        step_key: str,
        role: str,
        content: list[dict],
    ) -> int:
        self._log("append_history", tenant_id, "-", role=role, step_key=step_key)
        if not isinstance(step_key, str) or not step_key:
            raise ValueError("step_key must be a non-empty string")
        if role not in ("user", "assistant"):
            raise ValueError(f"invalid role {role!r}")
        return await self.history.append(tenant_id, conversation_id, step_key, role, content)

    @activity.defn(name="append_tool_results")
    async def append_tool_results(
        self,
        tenant_id: str,
        conversation_id: str,
        step_key: str,
        results: list[ToolResult],
    ) -> int:
        """Append the one user message of tool_result blocks, in the order given.

        A stored result is resolved from the tool output store here, so the
        text goes from the store into history without passing through the
        workflow. Appending under the step key keeps a retry from storing the
        message twice.
        """
        self._log("append_tool_results", tenant_id, "-", step_key=step_key)
        if not isinstance(step_key, str) or not step_key:
            raise ValueError("step_key must be a non-empty string")
        stored = await self.tool_outputs.get_many(
            tenant_id, conversation_id, [r.tool_use_id for r in results if r.stored]
        )
        blocks: list[dict] = []
        for r in results:
            if r.stored:
                if r.tool_use_id not in stored:
                    raise ApplicationError(
                        "stored tool output is missing",
                        type="ValueError",
                        non_retryable=True,
                    )
                content, is_error = stored[r.tool_use_id]
            else:
                content, is_error = r.content or "", r.is_error
            blocks.append(
                {
                    "type": "tool_result",
                    "tool_use_id": r.tool_use_id,
                    "content": content,
                    "is_error": is_error,
                }
            )
        return await self.history.append(tenant_id, conversation_id, step_key, "user", blocks)

    @activity.defn(name="call_llm")
    async def call_llm(self, ctx: TurnContext, conversation_id: str) -> LLMResponse:
        self._log("call_llm", ctx.tenant_id, ctx.agent_id)
        # Before the budget and before any model call: an agent or a tenant switched off
        # answers with a fixed text, bills nothing and records no usage row.
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        return await self._model_response(ctx, conversation_id, refusal)

    async def _model_response(
        self, ctx: TurnContext, conversation_id: str, refusal: Exception | None
    ) -> LLMResponse:
        """One model call for the thread, unless a switch or a budget answers instead."""
        agent = await self._pinned(ctx)
        if isinstance(refusal, AgentOff):
            log_stop(ctx.tenant_id, ctx.agent_id, refusal.switch)
            return LLMResponse(
                text=agent_off_text(agent.name), tool_calls=[], stop_reason=SWITCHED_OFF
            )
        if isinstance(refusal, AgentUnavailable):
            return LLMResponse(
                text=agent_unavailable_text(agent.name), tool_calls=[], stop_reason=SWITCHED_OFF
            )
        if self.budgets is not None:
            # Before any model call: a reached budget answers with a fixed text, bills nothing
            # and records no usage row.
            stopped = await self.budgets.stop_response(ctx.tenant_id, ctx.agent_id, agent.name)
            if stopped is not None:
                return stopped
        messages = prepare_for_model(await self.history.load_window(ctx.tenant_id, conversation_id))
        tools = self._connector_for(ctx.tenant_id, agent).tools_for_llm()
        if agent.grants.notion_databases:
            # Only an agent with a write grant starts tasks, so only it has any to list.
            tools = tools + [TOOLS[MY_TASKS_TOOL].llm_definition()]
        # Every agent has the Notion read tools, whose output is page text people wrote.
        system = f"{agent.system_prompt}\n\n{ITEM_RULES}"
        if agent.grants.notion_pages:
            # Search is offered only to an agent that was granted pages, and
            # the model is told to treat what it returns as data.
            tools = tools + [self.knowledge_tool.llm_definition()]
            system = f"{system}\n\n{KNOWLEDGE_RULES}"
        if agent.specialists:
            # Only specialists this asker may use here are offered; if none, the
            # front door answers alone.
            specialists = await self.audience.reachable(
                ctx.tenant_id,
                [await self._agent(ctx.tenant_id, s) for s in agent.specialists],
                ctx.requester_user_id,
                ctx.channel,
                ctx.channel_kind,
            )
            if specialists:
                tools = tools + [hand_off_tool(specialists)]
        try:
            resp = await self.llm.complete(system, messages, tools)
        except Exception as err:
            if _is_client_error(err):
                # The provider rejected the request (bad key, bad request):
                # retrying sends the same request and fails the same way.
                log.warning(
                    "call_llm rejected tenant_id=%s agent_id=%s error_type=%s",
                    ctx.tenant_id,
                    ctx.agent_id,
                    type(err).__name__,
                )
                raise _non_retryable(err) from err
            raise
        resp.tool_calls = [self._screened(call) for call in resp.tool_calls]
        if self.usage is not None:
            await self.usage.llm_call(
                tenant_id=ctx.tenant_id,
                agent_id=ctx.agent_id,
                agent_version=ctx.agent_version,
                conversation_id=conversation_id,
                key=ids.usage_call_key(ctx.tenant_id, *_activity_ref(conversation_id)),
                usage=resp.usage,
                fallback_model=self.settings.model,
            )
        return resp

    @staticmethod
    def _screened(call: ToolCall) -> ToolCall:
        """The call with what the connector manifests say about it (see ``screen``)."""
        kind, problem = screen(call.name, call.input)
        return replace(call, kind=kind, problem=problem)

    # --- conversation, content kept in Postgres ----------------------------------------------

    @activity.defn(name="ingest_message")
    async def ingest_message(self, tenant_id: str, conversation_id: str, event_id: str) -> bool:
        """Append the text a person wrote to the thread's history; False when it has none.

        The gateway stored the text under the event ID (``inbound_events``) before the
        workflow heard of the message, so the text never passed through Temporal. A message
        with nothing but whitespace is not stored: the model rejects an empty text block on
        every later call. A repeat appends nothing twice, because the step key is fixed.
        """
        self._log("ingest_message", tenant_id, "-")
        self._runtime(tenant_id)
        row = await self.content.get_inbound(tenant_id, event_id)
        if row is None:
            raise ApplicationError(
                "the inbound event is missing", type="ValueError", non_retryable=True
            )
        if row.get("consumed"):
            # A retry after the text was taken: history has it already.
            return bool(row.get("had_text"))
        text = row.get("text")
        had_text = isinstance(text, str) and bool(text.strip())
        if had_text:
            await self.history.append(
                tenant_id,
                conversation_id,
                f"{event_id}:user",
                "user",
                [{"type": "text", "text": text}],
            )
        # History keeps the text now; the inbound row keeps only its IDs.
        await self._consume(tenant_id, event_id, row, had_text)
        return had_text

    async def _consume(self, tenant_id: str, event_id: str, row: dict, had_text: bool) -> None:
        stub = {k: v for k, v in row.items() if k not in ("text", "fields")}
        stub.update(consumed=True, had_text=had_text)
        await self.content.consume_inbound(tenant_id, event_id, stub, self.clock())

    @activity.defn(name="discard_message")
    async def discard_message(self, tenant_id: str, event_id: str) -> None:
        """Clear an inbound event's text when its turn ended before ingest.

        A row that is gone or already consumed is left as it is: a consumed row's
        ``had_text`` is what a retried ingest reads.
        """
        self._log("discard_message", tenant_id, "-")
        self._runtime(tenant_id)
        row = await self.content.get_inbound(tenant_id, event_id)
        if row is None or row.get("consumed"):
            return
        await self._consume(tenant_id, event_id, row, had_text=False)

    @activity.defn(name="call_model")
    async def call_model(self, ctx: TurnContext, conversation_id: str, step_key: str) -> StepResult:
        """One model step. The reply is stored; what comes back is only what the workflow decides on.

        The model's answer is stored first, under ``step_key`` (``model_steps``), so an attempt
        that runs again after the model answered reads it instead of asking again, and its
        tool-call IDs are the ones already stored. A step that calls tools is then appended to
        history, and each call's input is stored by tool-use ID. A final reply is not appended
        here: ``finalize_model_reply`` stores it once it has been shown.
        """
        self._log("call_model", ctx.tenant_id, ctx.agent_id)
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        step = await self.content.get_step(ctx.tenant_id, conversation_id, step_key)
        if step is None:
            resp = await self._model_response(ctx, conversation_id, refusal)
            step = await self.content.put_step(
                ctx.tenant_id, conversation_id, step_key, _stored_step(resp)
            )
        uses = [b for b in step.content if b.get("type") == "tool_use"]
        if uses:
            await self.history.append(
                ctx.tenant_id, conversation_id, f"{step_key}:assistant", "assistant", step.content
            )
            for block in uses:
                await self.content.put_tool_input(
                    ctx.tenant_id, conversation_id, block["id"], block.get("input")
                )
        return StepResult(
            stop_reason=step.stop_reason,
            tool_calls=[_call_ref(block) for block in uses],
            has_text=_has_text(step),
        )

    async def _show_final_text(
        self,
        ctx: TurnContext,
        slack: SlackPoster,
        placeholder_ts: str,
        text: str,
        post_key: str | None,
        buttons_for: Callable[[str], list[dict]] | None = None,
    ) -> str:
        """Show the turn's final text: edit the placeholder, or, when ``post_key`` is set, post
        it as a new message in the thread. A dropped progress edit may still land on the
        placeholder after the reply, so the reply must not be written there; the new message is
        posted once under ``post_key`` (the same marker and lease as any other post).

        Returns the ts of the message that shows the text. ``buttons_for``, when set, builds
        the answer's feedback buttons from that ts: the placeholder edit carries them at once;
        a new message's ts is Slack's, so it is posted first and edited once more to carry them.
        """
        if post_key is None:
            blocks = buttons_for(placeholder_ts) if buttons_for is not None else None
            await slack.update_message(ctx.channel, placeholder_ts, text, blocks=blocks)
            return placeholder_ts
        ts = await self._post_once(ctx.tenant_id, post_key, ctx.channel, text, ctx.thread_ts)
        if buttons_for is not None:
            await slack.update_message(ctx.channel, ts, text, blocks=buttons_for(ts))
        return ts

    def _answer_buttons(
        self, ctx: TurnContext, conversation_id: str, text: str
    ) -> Callable[[str], list[dict]]:
        """The feedback buttons of one answer, built from the ts of the message that shows it."""

        def buttons(answer_ts: str) -> list[dict]:
            return answer_blocks(text, conversation_id, answer_ts, ctx.agent_id, ctx.agent_version)

        return buttons

    @activity.defn(name="finalize_model_reply")
    async def finalize_model_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        step_key: str,
        citations: list[Citation],
        cut_short: bool,
    ) -> None:
        """Show the reply ``call_model`` stored under ``step_key``, then keep it in history.

        Like ``finalize_reply`` for a model answer: a Sources line from the citations, Markdown
        converted to Slack, an "answer" audit event. ``cut_short`` adds the note for a reply the
        model cut off. History stores the reply only once Slack has shown it, so a failure to
        post never leaves history claiming the user saw an answer.
        """
        self._log("finalize_model_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_model_reply(
            ctx, placeholder_ts, step_key, citations, cut_short, None, []
        )

    @activity.defn(name="post_final_model_reply")
    async def post_final_model_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        step_key: str,
        citations: list[Citation],
        cut_short: bool,
        post_key: str,
    ) -> None:
        """``finalize_model_reply`` for a turn whose dropped progress edit may still land on the
        placeholder: the reply is posted as a new message in the thread, once under ``post_key``,
        instead of editing the placeholder."""
        self._log("post_final_model_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_model_reply(
            ctx, placeholder_ts, step_key, citations, cut_short, post_key, []
        )

    @activity.defn(name="finalize_searched_model_reply")
    async def finalize_searched_model_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        step_key: str,
        citations: list[Citation],
        cut_short: bool,
        searched: list[Citation],
    ) -> None:
        """``finalize_model_reply`` plus the passages this turn's searches returned, so an
        answer that cites none of them still shows where it searched.

        A separate activity, not an optional argument of ``finalize_model_reply``: Temporal
        decodes by type only when the call carries every argument, so calls already in a
        history would stop decoding.
        """
        self._log("finalize_searched_model_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_model_reply(
            ctx, placeholder_ts, step_key, citations, cut_short, None, searched
        )

    @activity.defn(name="post_final_searched_model_reply")
    async def post_final_searched_model_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        step_key: str,
        citations: list[Citation],
        cut_short: bool,
        searched: list[Citation],
        post_key: str,
    ) -> None:
        """``finalize_searched_model_reply`` for a turn whose dropped progress edit may still
        land on the placeholder: the reply is posted as a new message in the thread, once
        under ``post_key``, instead of editing the placeholder."""
        self._log("post_final_searched_model_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_model_reply(
            ctx, placeholder_ts, step_key, citations, cut_short, post_key, searched
        )

    async def _finalize_model_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        step_key: str,
        citations: list[Citation],
        cut_short: bool,
        post_key: str | None,
        searched: list[Citation],
    ) -> None:
        conversation_id = ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts)
        step = await self.content.get_step(ctx.tenant_id, conversation_id, step_key)
        if step is None or step.reply_text is None:
            raise ApplicationError(
                "the stored reply is missing", type="ValueError", non_retryable=True
            )
        text = step.reply_text
        shown = text + CUT_SHORT if cut_short else text
        footer = await self._sources_footer(ctx, text, citations, searched)
        slack = self._runtime(ctx.tenant_id).slack
        rendered = await self._rendered_reply(ctx, slack, shown)
        full = rendered + footer
        # A fixed stop text (agent off or unavailable, budget) is recorded as an answer but
        # is not one: it carries no feedback buttons.
        buttons_for = (
            self._answer_buttons(ctx, conversation_id, full)
            if step.stop_reason not in FIXED_STOP_REASONS
            else None
        )
        await self._show_final_text(ctx, slack, placeholder_ts, full, post_key, buttons_for)
        await self._record(
            ctx,
            "answer",
            ids.audit_answer_key(conversation_id, placeholder_ts),
            conversation_id=conversation_id,
            output_sha256=canonical_sha256(shown),
        )
        await self.history.append(
            ctx.tenant_id, conversation_id, f"{step_key}:assistant", "assistant", step.content
        )

    @activity.defn(name="post_placeholder")
    async def post_placeholder(self, ctx: TurnContext, idem_key: str) -> str:
        self._log("post_placeholder", ctx.tenant_id, ctx.agent_id)
        return await self._post_once(
            ctx.tenant_id, idem_key, ctx.channel, "Thinking…", ctx.thread_ts
        )

    @activity.defn(name="update_progress")
    async def update_progress(self, ctx: TurnContext, placeholder_ts: str, text: str) -> None:
        """Edit the placeholder to a progress label while the turn runs.

        The workflow awaits this inside the step, before the step's result returns, and
        drops a failure. Setting the same text twice is the same edit, so a retry needs no
        lease. A dropped attempt may still reach Slack after the reply; the workflow marks
        the placeholder unsafe and posts the final text as a new message instead.
        """
        self._log("update_progress", ctx.tenant_id, ctx.agent_id)
        slack = self._runtime(ctx.tenant_id).slack
        await slack.update_message(ctx.channel, placeholder_ts, text)

    @activity.defn(name="finalize_reply")
    async def finalize_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        text: str,
        citations: list[Citation] | None = None,
        audit_answer: bool = False,
    ) -> None:
        """Post the final reply, with a Sources line when the answer cites pages.

        The model writes only numbers like ``[2]``. The runtime keeps the
        numbers that match a citation of this turn and whose scope is still
        granted to the agent, looks up each page's title and link in the
        store, and appends them. The model never writes a link. Chat history
        keeps the model's text without the Sources line.

        The model writes Markdown; what is posted is converted to Slack
        mrkdwn. The footer is built from the unconverted text and is already
        mrkdwn.

        ``audit_answer`` is set by the workflow only for a model answer, so
        notices posted through this path are not recorded as answers.
        """
        self._log("finalize_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_reply(ctx, placeholder_ts, text, citations, audit_answer, None, [])

    @activity.defn(name="post_final_reply")
    async def post_final_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        text: str,
        citations: list[Citation] | None,
        audit_answer: bool,
        post_key: str,
    ) -> None:
        """``finalize_reply`` for a turn whose dropped progress edit may still land on the
        placeholder: the reply is posted as a new message in the thread, once under ``post_key``,
        instead of editing the placeholder."""
        self._log("post_final_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_reply(ctx, placeholder_ts, text, citations, audit_answer, post_key, [])

    @activity.defn(name="finalize_searched_reply")
    async def finalize_searched_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        text: str,
        citations: list[Citation],
        audit_answer: bool,
        searched: list[Citation],
    ) -> None:
        """``finalize_reply`` plus the passages this turn's searches returned, so an answer
        that cites none of them still shows where it searched.

        A separate activity, not an optional argument of ``finalize_reply``: Temporal decodes
        by type only when the call carries every argument, so calls already in a history
        would stop decoding.
        """
        self._log("finalize_searched_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_reply(
            ctx, placeholder_ts, text, citations, audit_answer, None, searched
        )

    @activity.defn(name="post_final_searched_reply")
    async def post_final_searched_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        text: str,
        citations: list[Citation],
        audit_answer: bool,
        searched: list[Citation],
        post_key: str,
    ) -> None:
        """``finalize_searched_reply`` for a turn whose dropped progress edit may still land
        on the placeholder: the reply is posted as a new message in the thread, once under
        ``post_key``, instead of editing the placeholder."""
        self._log("post_final_searched_reply", ctx.tenant_id, ctx.agent_id)
        await self._finalize_reply(
            ctx, placeholder_ts, text, citations, audit_answer, post_key, searched
        )

    async def _finalize_reply(
        self,
        ctx: TurnContext,
        placeholder_ts: str,
        text: str,
        citations: list[Citation] | None,
        audit_answer: bool,
        post_key: str | None,
        searched: list[Citation],
    ) -> None:
        footer = await self._sources_footer(ctx, text, citations or [], searched)
        slack = self._runtime(ctx.tenant_id).slack
        rendered = await self._rendered_reply(ctx, slack, text)
        full = rendered + footer
        conversation_id = ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts)
        buttons_for = self._answer_buttons(ctx, conversation_id, full) if audit_answer else None
        await self._show_final_text(ctx, slack, placeholder_ts, full, post_key, buttons_for)
        if audit_answer:
            await self._record(
                ctx,
                "answer",
                ids.audit_answer_key(conversation_id, placeholder_ts),
                conversation_id=conversation_id,
                output_sha256=canonical_sha256(text),
            )

    @activity.defn(name="finalize_notice")
    async def finalize_notice(
        self, ctx: TurnContext, placeholder_ts: str, text: str, tool: str
    ) -> None:
        """Show a non-answer final text on the placeholder and audit it as a ``notice``.

        Like ``finalize_reply`` for the fixed notices (step limit, hand-off announcement,
        failure): no citations, the reason in ``tool``, the hash of the text shown. A retry
        updates the same message and records once.
        """
        self._log("finalize_notice", ctx.tenant_id, ctx.agent_id, tool=tool)
        await self._finalize_notice(ctx, placeholder_ts, text, tool, None)

    @activity.defn(name="post_final_notice")
    async def post_final_notice(
        self, ctx: TurnContext, placeholder_ts: str, text: str, tool: str, post_key: str
    ) -> None:
        """``finalize_notice`` for a turn whose dropped progress edit may still land on the
        placeholder: the notice is posted as a new message in the thread, once under
        ``post_key``, instead of editing the placeholder."""
        self._log("post_final_notice", ctx.tenant_id, ctx.agent_id, tool=tool)
        await self._finalize_notice(ctx, placeholder_ts, text, tool, post_key)

    async def _finalize_notice(
        self, ctx: TurnContext, placeholder_ts: str, text: str, tool: str, post_key: str | None
    ) -> None:
        slack = self._runtime(ctx.tenant_id).slack
        rendered = await self._rendered_reply(ctx, slack, text)
        await self._show_final_text(ctx, slack, placeholder_ts, rendered, post_key)
        conversation_id = ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts)
        await self._record(
            ctx,
            "notice",
            ids.audit_notice_key(conversation_id, tool, placeholder_ts),
            conversation_id=conversation_id,
            tool=tool,
            output_sha256=canonical_sha256(text),
        )

    async def _rendered_reply(self, ctx: TurnContext, slack: SlackPoster, text: str) -> str:
        """The model's text as posted mrkdwn: only the requester's own mention pings."""
        converted = to_slack_mrkdwn(text)
        names: dict[str, str] = {}
        for user_id in mentioned_user_ids(converted):
            if user_id == ctx.requester_user_id:
                continue
            try:
                names[user_id] = (await slack.user_info(user_id)).display_name
            except SlackLookupError:
                names[user_id] = "someone"
        return render_mentions(converted, ctx.requester_user_id, names)

    async def _sources_footer(
        self, ctx: TurnContext, text: str, citations: list[Citation], searched: list[Citation]
    ) -> str:
        """The footer of a reply: a Sources line for the numbers it cites, else a Searched
        line naming the pages whose passages this turn returned but the answer did not cite.
        Both are built from stored page metadata of scopes still granted to the agent.
        """
        if (not citations and not searched) or self.knowledge is None:
            return ""
        scopes = await self._granted_scopes(ctx.tenant_id, ctx.agent_id)
        cited = [c for c in cited_markers(text, citations) if c.scope in scopes]
        if cited:
            meta = await self.knowledge.page_meta(ctx.tenant_id, scopes, [c.page_id for c in cited])
            return sources_footer(cited, meta)
        pages = searched_pages(searched, scopes)
        if not pages:
            return ""
        meta = await self.knowledge.page_meta(ctx.tenant_id, scopes, [c.page_id for c in pages])
        return searched_footer(pages, meta)

    async def _granted_scopes(self, tenant_id: str, agent_id: str) -> list[str]:
        agent = await self._agent(tenant_id, agent_id)
        return [ids.knowledge_scope(g.id) for g in agent.grants.notion_pages]

    @activity.defn(name="run_read_tool")
    async def run_read_tool(
        self, ctx: TurnContext, conversation_id: str, call: ToolCall, first_marker: int
    ) -> ToolResult:
        """Run a read tool and store its output text; return only a reference.

        ``first_marker`` is the first citation number this call may use; only
        ``search_knowledge`` uses it, and it returns the citations it numbered.
        """
        self._log("run_read_tool", ctx.tenant_id, ctx.agent_id, tool=call.name)
        refusal = await self._gate_refusal(
            ctx.tenant_id, ctx.agent_id, Effect.READ, _tool_connector(call.name)
        )
        return await self._run_read(ctx, conversation_id, call, first_marker, refusal)

    @activity.defn(name="run_stored_read_tool")
    async def run_stored_read_tool(
        self, ctx: TurnContext, conversation_id: str, call: ToolCallRef, first_marker: int
    ) -> ToolResult:
        """``run_read_tool`` for a call whose input is stored by tool-use ID, not passed in."""
        self._log("run_stored_read_tool", ctx.tenant_id, ctx.agent_id, tool=call.name)
        refusal = await self._gate_refusal(
            ctx.tenant_id, ctx.agent_id, Effect.READ, _tool_connector(call.name)
        )
        found, tool_input = await self.content.get_tool_input(
            ctx.tenant_id, conversation_id, call.id
        )
        if not found:
            raise ApplicationError(
                "the tool call's input is missing", type="ValueError", non_retryable=True
            )
        full = ToolCall(call.id, call.name, tool_input if isinstance(tool_input, dict) else {})
        return await self._run_read(ctx, conversation_id, full, first_marker, refusal)

    async def _run_read(
        self,
        ctx: TurnContext,
        conversation_id: str,
        call: ToolCall,
        first_marker: int,
        refusal: Exception | None,
    ) -> ToolResult:
        citations: list[Citation] = []
        if isinstance(refusal, AgentOff):
            # A call dispatched before the agent was turned off. The model gets an error
            # it can pass on; nothing is read.
            agent = await self._agent(ctx.tenant_id, ctx.agent_id)
            text, is_error = agent_off_text(agent.name), True
        elif isinstance(refusal, AgentUnavailable):
            agent = await self._agent(ctx.tenant_id, ctx.agent_id)
            text, is_error = agent_unavailable_text(agent.name), True
        elif refusal is not None:
            # A connector switch: the refusal names the connector the tool reads through.
            text, is_error = str(refusal), True
        elif call.name == "search_knowledge":
            text, is_error, citations = await self._search_knowledge(ctx, call, first_marker)
        elif call.name == MY_TASKS_TOOL:
            text, is_error = await self._my_tasks(ctx)
        else:
            text, is_error = await self._read_tool_output(ctx, call)
        await self.tool_outputs.put(ctx.tenant_id, conversation_id, call.id, text, is_error)
        await self._record(
            ctx,
            "tool_call",
            ids.audit_tool_call_key(conversation_id, call.id),
            outcome="error" if is_error else "ok",
            conversation_id=conversation_id,
            tool=call.name,
            input_sha256=canonical_sha256(call.input),
            output_sha256=canonical_sha256(text),
        )
        return ToolResult(call.id, stored=True, is_error=is_error, citations=citations)

    async def _search_knowledge(
        self, ctx: TurnContext, call: ToolCall, first_marker: int
    ) -> tuple[str, bool, list[Citation]]:
        """Search the agent's own granted pages and number the passages found.

        The scopes come from this agent's page grants, and the store filters by
        them inside its query, and only chunks of the embedding model in use count.
        Chunks beyond the model's distance cut-off are dropped. Each page found is then
        re-checked with Notion (not trashed, still where it was indexed under its granted
        root) before it can reach the model. The numbered passages are stored as the tool output; only the
        citation numbers and page IDs go back to the workflow.
        """
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        roots = {ids.knowledge_scope(g.id): g.id for g in agent.grants.notion_pages}
        if not roots or self.knowledge is None or self.embedder is None:
            self._log_search(ctx, len(roots), 0, 0, [])
            return "This agent has no documents to search.", True, []
        query = call.input.get("query")
        if not isinstance(query, str) or not query.strip():
            self._log_search(ctx, len(roots), 0, 0, [])
            return "search_knowledge needs a non-empty 'query'.", True, []
        try:
            [vector] = await self.embedder.embed([query.strip()])
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            raise
        model = self.embedder.model
        hits = await self.knowledge.search(
            ctx.tenant_id, list(roots), vector, limit=SEARCH_CANDIDATES, embed_model=model
        )
        candidates = len(hits)
        cutoff = embed_max_distance(model, self.settings.embed_max_distance)
        if cutoff is not None:
            hits = [hit for hit in hits if hit.distance <= cutoff]
        checker = PageChecker(self._runtime(ctx.tenant_id).notion_api)
        kept = await select_passages(hits, checker, roots)
        self._log_search(ctx, len(roots), candidates, candidates - len(hits), kept)
        citations = [
            Citation(first_marker + i, hit.scope, hit.page_id, distance=hit.distance)
            for i, hit in enumerate(kept)
        ]
        passages = [(c.marker, hit.title, hit.text) for c, hit in zip(citations, kept)]
        return format_sources(passages), False, citations

    def _log_search(
        self, ctx: TurnContext, scopes: int, hits: int, dropped: int, kept: list[ChunkHit]
    ) -> None:
        """One line per search_knowledge call: what it found and kept, enough to tell a
        search that found nothing from one whose passages the answer did not cite.
        Page IDs and distances only, never a title or passage text.
        """
        best: dict[str, float] = {}
        for hit in kept:
            best.setdefault(hit.page_id, hit.distance)
        pages = ",".join(f"{page_id}:{distance:.4f}" for page_id, distance in best.items())
        log.info(
            "search_knowledge returned tenant_id=%s agent_id=%s scopes=%d hits=%d dropped=%d"
            " kept=%d pages=%s",
            ctx.tenant_id,
            ctx.agent_id,
            scopes,
            hits,
            dropped,
            len(kept),
            pages,
        )

    async def _my_tasks(self, ctx: TurnContext) -> tuple[str, bool]:
        """The asker's tasks from the platform's store: no connector call, no Notion search.

        The requester is the runtime's, never a model argument; the store filters by it.
        The write grant is checked here too, not only where the tool is offered: a model
        can invent a call it was never offered, and an agent that cannot start tasks has
        none to list.
        """
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        if not agent.grants.notion_databases:
            return "This agent has no write access, so it has no requests to list.", True
        records = await self.tasks.list_for_requester(ctx.tenant_id, ctx.requester_user_id)
        team_id = self._runtime(ctx.tenant_id).config.slack_team_id
        return format_task_list(records, team_id=team_id), False

    async def _read_tool_output(self, ctx: TurnContext, call: ToolCall) -> tuple[str, bool]:
        """The tool's output text and whether it is an error."""
        connector = await self._connector(ctx.tenant_id, ctx.agent_id)
        try:
            if call.name == "search_items":
                query = call.input.get("query")
                if not isinstance(query, str) or not query.strip():
                    return "search_items needs a non-empty 'query'.", True
                items = await connector.search_items(query.strip())
            elif call.name == "my_items":
                # The person is always the requester from the runtime; any
                # person field in the LLM's input is ignored.
                email = await self._runtime(ctx.tenant_id).slack.user_email(ctx.requester_user_id)
                if not email:
                    return "I couldn't read your Slack email, so I can't find your items.", True
                items = await connector.my_items(email)
            else:
                return f"Unknown tool '{call.name}'.", True
        except ConnectorError as err:
            return str(err), True
        except Exception as err:  # noqa: BLE001 - final Notion errors become a result, the rest re-raise
            # Notion answered that it will not do this, now or on a retry (a revoked
            # database, a refused key): the model gets an error result it can pass on and
            # the turn goes on. Anything retryable is raised for Temporal.
            if notion_errors.is_final(err):
                log.warning(
                    "read tool refused by notion tenant_id=%s agent_id=%s tool=%s code=%s",
                    ctx.tenant_id,
                    ctx.agent_id,
                    call.name,
                    getattr(err, "code", None),
                )
                return notion_errors.entry_for(err).text, True
            _reraise(err)
        return _format_items(items), False

    # --- task ------------------------------------------------------------------------

    @activity.defn(name="post_approval_card")
    async def post_approval_card(self, spec: TaskSpec, task_id: str) -> str:
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        self._log("post_approval_card", ctx.tenant_id, ctx.agent_id)
        key = f"{task_id}:card"
        existing = await self.idem.get(ctx.tenant_id, key)
        if existing is not None:
            return existing["ts"]
        payload = await self._spec_payload(spec)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector_for(ctx.tenant_id, agent).grant_for(payload["database_id"])
        line = await self._card_requester_line(spec, task_id, agent)
        dm = await self._runtime(ctx.tenant_id).slack.open_dm(ctx.requester_user_id)
        blocks = approval_card_blocks(
            spec, task_id, agent.name, grant.name, requester_line=line, payload=payload
        )
        text = _card_text(spec, payload, grant.name)
        return await self._post_once(ctx.tenant_id, key, dm, text, None, blocks)

    async def _card_requester_line(
        self, spec: TaskSpec, task_id: str, agent: AgentVersion
    ) -> str | None:
        """The body line the card must show, or None for a comment or a matched requester."""
        if spec.tool_name == "add_comment":
            return None
        return (await self._requester(spec, task_id, agent))["line"]

    @activity.defn(name="update_card")
    async def update_card(self, spec: TaskSpec, task_id: str, card_ts: str, text: str) -> None:
        payload = await self._spec_payload(spec)
        subject = task_notices.subject(spec.tool_name, payload)
        await self._replace_card(spec, task_id, card_ts, f"{text}: {subject}")

    @activity.defn(name="update_card_line")
    async def update_card_line(self, spec: TaskSpec, task_id: str, card_ts: str, line: str) -> None:
        """Replace the card with ``line`` as it is.

        A separate activity, not an optional argument of ``update_card``: Temporal decodes
        by type only when the call carries every argument, so calls already in a history
        would stop decoding.
        """
        await self._replace_card(spec, task_id, card_ts, line)

    async def _replace_card(self, spec: TaskSpec, task_id: str, card_ts: str, text: str) -> None:
        ctx = spec.ctx
        self._log("update_card", ctx.tenant_id, ctx.agent_id)
        record = await self.idem.get(ctx.tenant_id, f"{task_id}:card")
        slack = self._runtime(ctx.tenant_id).slack
        channel = record["channel"] if record else await slack.open_dm(ctx.requester_user_id)
        line = escape_slack(text)
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": line}}]
        await slack.update_message(channel, card_ts, line, blocks=blocks)

    @activity.defn(name="refresh_card")
    async def refresh_card(self, spec: TaskSpec, task_id: str, card_ts: str) -> None:
        """Redraw the approval card in place after an edit.

        It shows the edited draft and buttons carrying the new nonce, so a
        click on an older version of the card no longer matches.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        self._log("refresh_card", ctx.tenant_id, ctx.agent_id)
        record = await self.idem.get(ctx.tenant_id, f"{task_id}:card")
        slack = self._runtime(ctx.tenant_id).slack
        channel = record["channel"] if record else await slack.open_dm(ctx.requester_user_id)
        payload = await self._spec_payload(spec)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector_for(ctx.tenant_id, agent).grant_for(payload["database_id"])
        line = await self._card_requester_line(spec, task_id, agent)
        blocks = approval_card_blocks(
            spec,
            task_id,
            agent.name,
            grant.name,
            edited=True,
            requester_line=line,
            payload=payload,
        )
        await slack.update_message(
            channel, card_ts, _card_text(spec, payload, grant.name), blocks=blocks
        )

    @activity.defn(name="prepare_comment")
    async def prepare_comment(self, spec: TaskSpec) -> dict:
        """Resolve the item to comment on and return the payload to freeze.

        The page's database must be one this agent was granted; otherwise this
        fails before any card is posted or anything is written.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("prepare_comment", ctx.tenant_id, ctx.agent_id)
        return await self._resolve_comment(spec, spec.payload)

    @activity.defn(name="prepare_stored_comment")
    async def prepare_stored_comment(self, spec: TaskSpec) -> DraftOutcome:
        """``prepare_comment`` for a task whose payload is a stored draft.

        The resolved payload is stored as the next revision, which the card then shows and the
        approval names; nothing was shown or approved before it. A retry finds it stored.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("prepare_stored_comment", ctx.tenant_id, ctx.agent_id)
        revision = spec.revision + 1
        resolved = await self.content.get_draft(ctx.tenant_id, spec.draft_ref, revision)
        if resolved is None:
            resolved = await self._resolve_comment(spec, await self._spec_payload(spec))
            await self.content.put_draft(ctx.tenant_id, spec.draft_ref, revision, resolved)
        return DraftOutcome(revision=revision, database_id=resolved["database_id"])

    async def _resolve_comment(self, spec: TaskSpec, payload: dict) -> dict:
        ctx = spec.ctx
        try:
            connector = await self._connector(ctx.tenant_id, ctx.agent_id)
            item = await connector.resolve_item(payload["item"])
        except ConnectorError as err:
            raise _non_retryable(err) from err
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            _reraise(err)
        return {
            "page_id": item.page_id,
            "database_id": item.database_id,
            "item_title": item.title,
            "item_url": item.url,
            "comment": payload["comment"],
        }

    @activity.defn(name="draft_task")
    async def draft_task(
        self, ctx: TurnContext, conversation_id: str, call: ToolCallRef, task_id: str
    ) -> DraftOutcome:
        """Freeze what the model asked a write tool for, as revision 1 of the task's draft.

        Reads the call's stored input, checks it the way the model is told ("needs a non-empty
        'title'", a length), and stores exactly the fields that will be created. The payload is
        shown on the approval card and run as it is; the workflow never sees it. ``error`` is
        the line for the model when the input cannot be a task.
        """
        self._log("draft_task", ctx.tenant_id, ctx.agent_id, tool=call.kind)
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        if refusal is not None:
            if isinstance(refusal, AgentOff):
                return DraftOutcome(error=agent_off_text(agent.name))
            if isinstance(refusal, AgentUnavailable):
                return DraftOutcome(error=agent_unavailable_text(agent.name))
            raise _non_retryable(refusal)
        found, raw = await self.content.get_tool_input(ctx.tenant_id, conversation_id, call.id)
        if not found:
            raise ApplicationError(
                "the tool call's input is missing", type="ValueError", non_retryable=True
            )
        tool_input = raw if isinstance(raw, dict) else {}
        kind = call.kind
        if kind == "add_comment":
            item, comment = tool_input.get("item"), tool_input.get("comment")
            if not isinstance(item, str) or not item.strip():
                return DraftOutcome(error=f"{kind} needs a non-empty 'item'.")
            if not isinstance(comment, str) or not comment.strip():
                return DraftOutcome(error=f"{kind} needs a non-empty 'comment'.")
            if error := check_field(kind, "comment", comment.strip()):
                return DraftOutcome(error=error)
            # The task resolves the item and checks the grant before any card.
            payload = {"item": item.strip(), "comment": comment.strip()}
        elif kind in EDITABLE_FIELDS:
            title, details = tool_input.get("title"), tool_input.get("details")
            if not isinstance(title, str) or not title.strip():
                return DraftOutcome(error=f"{kind} needs a non-empty 'title'.")
            if not isinstance(details, str) or not details.strip():
                return DraftOutcome(error=f"{kind} needs a non-empty 'details'.")
            if not agent.grants.notion_databases:
                return DraftOutcome(error="This agent has no Notion database to create items in.")
            for name, value in (("title", title), ("details", details)):
                if error := check_field(kind, name, value.strip()):
                    return DraftOutcome(error=error)
            # The payload is reduced to exactly the fields that will be created, and
            # frozen here: the approval card shows it and the task runs it as is.
            payload = {
                "title": title.strip(),
                "details": details.strip(),
                "database_id": agent.grants.notion_databases[0].id,
            }
        else:
            return DraftOutcome(error=f"Unknown tool '{call.name}'.")
        await self.content.put_draft(ctx.tenant_id, task_id, 1, payload)
        return DraftOutcome(revision=1, database_id=payload.get("database_id", ""))

    @activity.defn(name="apply_edit")
    async def apply_edit(self, spec: TaskSpec, edit_id: str) -> bool:
        """Store a saved edit as the next revision of the draft; False when it does not fit.

        The values come from the ``inbound_events`` row the gateway wrote (``edit_id``); the
        workflow only names it. Every field is checked again as the model's first draft is,
        so an accepted value always fits Notion and the card. The new revision is the old one
        with the edited fields replaced; the old one is left as it is.
        """
        ctx = spec.ctx
        self._log("apply_edit", ctx.tenant_id, ctx.agent_id)
        if await self.content.get_draft(ctx.tenant_id, spec.draft_ref, spec.revision + 1):
            return True  # a retry: the revision is stored and the form's values are cleared
        row = await self.content.get_inbound(ctx.tenant_id, edit_id)
        limits = EDITABLE_FIELDS.get(spec.tool_name, {})
        fields = (row or {}).get("fields")
        if not row or row.get("task_id") != spec.draft_ref or not isinstance(fields, dict):
            log.warning("ignored edit: no stored form for this task tenant_id=%s", ctx.tenant_id)
            return False
        if set(fields) != set(limits):
            log.warning(
                "ignored edit: fields are not the editable ones tenant_id=%s", ctx.tenant_id
            )
            return False
        clean: dict = {}
        for key, value in fields.items():
            text = value.strip() if isinstance(value, str) else ""
            if not text or check_field(spec.tool_name, key, text):
                log.warning("ignored edit: invalid value for a field tenant_id=%s", ctx.tenant_id)
                return False
            clean[key] = text
        current = await self._spec_payload(spec)
        await self.content.put_draft(
            ctx.tenant_id, spec.draft_ref, spec.revision + 1, {**current, **clean}
        )
        await self._consume(ctx.tenant_id, edit_id, row, True)
        return True

    @activity.defn(name="post_task_notice")
    async def post_task_notice(
        self,
        spec: TaskSpec,
        task_id: str,
        kind: str,
        idem_key: str,
        reason: str = "",
        status: str = "",
        days: int = 0,
    ) -> str:
        """Post one of the task's thread lines (``agentos.task_notices``), written from the stored draft.

        The workflow names the kind, and the small values that are not content (a failure
        reason, a status, a number of days); the title or comment the line mentions is read
        here. Lines about the item a create made read it from the write's stored result and
        fall back to the approved title when no result was stored.
        """
        ctx = spec.ctx
        self._log("post_task_notice", ctx.tenant_id, ctx.agent_id, kind=kind)
        payload = await self._spec_payload(spec)
        subject, url = task_notices.subject(spec.tool_name, payload), payload.get("item_url", "")
        if kind in task_notices.ITEM_NOTICES:
            stored = await self.writes.result(ctx.tenant_id, f"{task_id}:create")
            if stored is not None:
                item = _item_from_ref(stored)
                subject, url = item.title, item.url
        text = task_notices.notice_text(
            kind,
            subject=subject,
            reason=reason,
            url=url,
            status=status,
            requester=ctx.requester_user_id,
            days=days,
        )
        return await self._post_once(ctx.tenant_id, idem_key, ctx.channel, text, ctx.thread_ts)

    @activity.defn(name="add_notion_comment")
    async def add_notion_comment(self, spec: TaskSpec, task_id: str) -> str:
        ctx = spec.ctx
        idem_key = f"{task_id}:comment"
        await self._gate_write(spec, task_id, idem_key, "page_id")
        self._log("add_notion_comment", ctx.tenant_id, ctx.agent_id)
        payload = await self._spec_payload(spec)
        page_id = payload["page_id"]
        await self._check_method(spec, payload, task_id, page_id, idem_key, "marker_text")
        # The live published version, not the turn's pinned one, on purpose: a grant revoked
        # since this write was approved must stop it.
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        connector = self._connector_for(ctx.tenant_id, agent)
        text = payload["comment"]
        await self._record_approval(spec, payload, task_id, page_id)
        try:
            await connector.resolve_item(page_id)
            comment_id = await self._write_once(
                ctx.tenant_id,
                idem_key,
                find=lambda: connector.find_comment(page_id, idem_key),
                write=lambda: connector.create_comment(page_id, text, idem_key, agent.name),
                to_ref=lambda comment_id: comment_id,
                from_ref=lambda ref: ref,
            )
        except Exception as err:
            if isinstance(err, ConnectorError) or _is_client_error(err):
                await self._record_write(spec, payload, task_id, idem_key, page_id, "error")
                raise _non_retryable(err) from err
            _reraise(err)
        await self._record_write(
            spec, payload, task_id, idem_key, page_id, "ok", {"comment_id": comment_id}
        )
        return comment_id

    @activity.defn(name="create_notion_item")
    async def create_notion_item(self, spec: TaskSpec, task_id: str) -> NotionItem:
        """Create the approved item once. A task with a stored draft gets back the page ID and
        status only: the title and link are content, and the workflow does not carry them."""
        ctx = spec.ctx
        idem_key = f"{task_id}:create"
        await self._gate_write(spec, task_id, idem_key, "database_id")
        self._log("create_notion_item", ctx.tenant_id, ctx.agent_id)
        payload = await self._spec_payload(spec)
        database_id = payload["database_id"]
        await self._check_method(spec, payload, task_id, database_id, idem_key, "marker_property")
        # The live published version, not the turn's pinned one, on purpose: a grant revoked
        # since this write was approved must stop it.
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        connector = self._connector_for(ctx.tenant_id, agent)
        await self._record_approval(spec, payload, task_id, database_id)
        try:
            connector.grant_for(database_id)
            requester = await self._requester(spec, task_id, agent)
            item = await self._write_once(
                ctx.tenant_id,
                idem_key,
                find=lambda: connector.find_item(database_id, idem_key),
                write=lambda: connector.create_item(
                    database_id,
                    payload["title"],
                    payload["details"],
                    idem_key,
                    notion_user_id=requester["notion_user_id"],
                    requester_line=requester["line"],
                ),
                to_ref=_item_ref,
                from_ref=_item_from_ref,
            )
        except Exception as err:
            if isinstance(err, ConnectorError) or _is_client_error(err):
                await self._record_write(spec, payload, task_id, idem_key, database_id, "error")
                raise _non_retryable(err) from err
            _reraise(err)
        await self.links.link(ctx.tenant_id, normalize_id(item.page_id), task_id)
        await self._record_write(
            spec, payload, task_id, idem_key, item.page_id, "ok", {"page_id": item.page_id}
        )
        return _without_content(spec, item)

    @activity.defn(name="reconcile_notion_item")
    async def reconcile_notion_item(self, spec: TaskSpec, task_id: str) -> NotionItem | None:
        """After the create ran out of retries: is the item there? Reads only, one look.

        The task calls this a few times, a few seconds apart. None means this look did not
        find it, which is not proof that nothing was filed.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("reconcile_notion_item", ctx.tenant_id, ctx.agent_id)
        payload = await self._spec_payload(spec)
        connector = await self._connector(ctx.tenant_id, ctx.agent_id)
        idem_key = f"{task_id}:create"
        ref = await self.writes.result(ctx.tenant_id, idem_key)
        item = (
            _item_from_ref(ref)
            if ref is not None
            else await connector.find_item(payload["database_id"], idem_key)
        )
        if item is None:
            return None
        await self.links.link(ctx.tenant_id, normalize_id(item.page_id), task_id)
        await self._record_write(
            spec, payload, task_id, idem_key, item.page_id, "ok", {"page_id": item.page_id}
        )
        return _without_content(spec, item)

    @activity.defn(name="reconcile_notion_comment")
    async def reconcile_notion_comment(self, spec: TaskSpec, task_id: str) -> str | None:
        """The comment counterpart of ``reconcile_notion_item``: its ID, or None for "not seen"."""
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("reconcile_notion_comment", ctx.tenant_id, ctx.agent_id)
        payload = await self._spec_payload(spec)
        connector = await self._connector(ctx.tenant_id, ctx.agent_id)
        idem_key = f"{task_id}:comment"
        page_id = payload["page_id"]
        comment_id = await self.writes.result(ctx.tenant_id, idem_key)
        if comment_id is None:
            await connector.resolve_item(page_id)
            comment_id = await connector.find_comment(page_id, idem_key)
        if comment_id is None:
            return None
        await self._record_write(
            spec, payload, task_id, idem_key, page_id, "ok", {"comment_id": comment_id}
        )
        return comment_id

    @activity.defn(name="get_notion_item")
    async def get_notion_item(self, tenant_id: str, agent_id: str, page_id: str) -> NotionItem:
        """Read one item. Takes IDs only: the watch polls this thousands of times."""
        refusal = await self._gate_refusal(tenant_id, agent_id, Effect.POLL)
        self._log("get_notion_item", tenant_id, agent_id)
        return await self._poll_item(tenant_id, agent_id, page_id, refusal)

    @activity.defn(name="watch_notion_item")
    async def watch_notion_item(self, tenant_id: str, agent_id: str, page_id: str) -> NotionItem:
        """``get_notion_item`` for a task whose title is not carried by its workflow: only the
        page ID and the status come back."""
        refusal = await self._gate_refusal(tenant_id, agent_id, Effect.POLL)
        self._log("watch_notion_item", tenant_id, agent_id)
        item = await self._poll_item(tenant_id, agent_id, page_id, refusal)
        return NotionItem(page_id=item.page_id, url="", title="", status=item.status)

    async def _poll_item(
        self, tenant_id: str, agent_id: str, page_id: str, refusal: Exception | None
    ) -> NotionItem:
        if isinstance(refusal, AgentOff):
            # An admin turned the agent off: the task ends the watch. No Notion call is made.
            raise _non_retryable(refusal)
        if isinstance(refusal, AgentUnavailable):
            # Retryable on purpose, like a connector switched off: health is a recoverable
            # state, so the task keeps watching and polls again once a check passes. The
            # workflow treats a poll that fails after its retries as "still watching".
            # No Notion call is made.
            raise ApplicationError(str(refusal), type="AgentUnavailable")
        if refusal is not None:
            # Retryable on purpose: the task workflow treats a poll that fails after its
            # retries as "still watching" and polls again later, while a permanent
            # failure would end the watch. No Notion call is made.
            raise ApplicationError(connector_off_text("notion"), type="ConnectorOff")
        try:
            connector = await self._connector(tenant_id, agent_id)
            with bulk_priority():
                return await connector.get_item(page_id)
        except ConnectorError as err:
            raise _non_retryable(err) from err
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            _reraise(err)

    @activity.defn(name="record_task_state")
    async def record_task_state(
        self, spec: TaskSpec, task_id: str, state: str, status: str, updated_at: datetime
    ) -> None:
        """Store the task's latest report in the ``tasks`` table, one row per task ID.

        The workflow names its own state, the item's last status and the time of the
        report; the title, link and page are read here from the write's stored result
        or the stored draft, so they never pass through workflow history. A retry
        stores the same row again.
        """
        ctx = spec.ctx
        self._log("record_task_state", ctx.tenant_id, ctx.agent_id, state=state)
        title, url, page_id = await self._task_item_meta(spec, task_id)
        await self.tasks.record(
            TaskRecord(
                tenant_id=ctx.tenant_id,
                task_id=task_id,
                requester_user_id=ctx.requester_user_id,
                agent_id=ctx.agent_id,
                title=title,
                url=url,
                page_id=page_id,
                state=listed_state(state),
                last_status=status,
                updated_at=updated_at,
                channel=ctx.channel,
                thread_ts=ctx.thread_ts,
            )
        )

    async def _task_item_meta(self, spec: TaskSpec, task_id: str) -> tuple[str, str, str]:
        """The item's title, link and page ID: the write's stored result once it exists,
        else the stored draft (a comment's resolved item carries its own)."""
        payload = await self._spec_payload(spec)
        if spec.tool_name == "add_comment":
            return (
                payload.get("item_title", ""),
                payload.get("item_url", ""),
                payload.get("page_id", ""),
            )
        stored = await self.writes.result(spec.ctx.tenant_id, f"{task_id}:create")
        if stored is not None:
            item = _item_from_ref(stored)
            return item.title, item.url, item.page_id
        return payload.get("title", ""), "", ""

    @activity.defn(name="task_in_thread")
    async def task_in_thread(
        self, tenant_id: str, channel: str, thread_ts: str, requester_user_id: str
    ) -> TaskRecord | None:
        """The asker's newest watching task in this thread, or None.

        Only a watching task has a watch to poke. The row's page ID names the item the
        ``external_update`` signal must carry, and its ``updated_at`` is the mark the
        refresh wait watches.
        """
        self._log("task_in_thread", tenant_id, "-")
        return await self.tasks.watching_in_thread(tenant_id, channel, thread_ts, requester_user_id)

    @activity.defn(name="wait_task_refresh")
    async def wait_task_refresh(self, tenant_id: str, task_id: str, before: datetime) -> bool:
        """Wait until the task reports again after ``before``, or the budget runs out.

        The poke went through the watch's own signal path, so the report lands within
        ``POKE_GAP`` plus the poll's own time. The wait is real time inside the activity;
        the marks it compares are the task's workflow time. Best-effort: a task that
        never reports is listed as it was last recorded.
        """
        self._log("wait_task_refresh", tenant_id, "-", task_id=task_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.refresh_wait_seconds
        while loop.time() < deadline:
            await asyncio.sleep(0.5)
            row = await self.tasks.get(tenant_id, task_id)
            if row is None or row.updated_at > before:
                return True
        return False

    @activity.defn(name="post_in_thread")
    async def post_in_thread(self, ctx: TurnContext, text: str, idem_key: str) -> str:
        self._log("post_in_thread", ctx.tenant_id, ctx.agent_id)
        return await self._post_once(ctx.tenant_id, idem_key, ctx.channel, text, ctx.thread_ts)

    @activity.defn(name="post_notice_in_thread")
    async def post_notice_in_thread(
        self, ctx: TurnContext, text: str, idem_key: str, tool: str
    ) -> str:
        """Post a fixed notice in the thread and audit it as a ``notice``: the reason in
        ``tool``, the hash of the text shown, a key stable per ``idem_key``."""
        self._log("post_notice_in_thread", ctx.tenant_id, ctx.agent_id, tool=tool)
        ts = await self._post_once(ctx.tenant_id, idem_key, ctx.channel, text, ctx.thread_ts)
        conversation_id = ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts)
        await self._record(
            ctx,
            "notice",
            ids.audit_notice_key(conversation_id, tool, idem_key),
            conversation_id=conversation_id,
            tool=tool,
            output_sha256=canonical_sha256(text),
        )
        return ts

    @activity.defn(name="record_hand_off")
    async def record_hand_off(self, ctx: TurnContext, target_agent: str, event_id: str) -> None:
        """Audit a front door handing the thread to one of its specialists.

        ``ctx`` is the front door's; ``target_agent`` is the specialist.
        """
        self._log("record_hand_off", ctx.tenant_id, ctx.agent_id, target=target_agent)
        await self._agent(ctx.tenant_id, target_agent)
        await self._record(
            ctx,
            "hand_off",
            ids.audit_hand_off_key(ctx.tenant_id, event_id),
            conversation_id=ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts),
            target=target_agent,
        )

    @activity.defn(name="thread_agent")
    async def thread_agent(
        self, tenant_id: str, agent_id: str, channel: str, thread_ts: str
    ) -> str:
        """The agent that owns the thread: its specialist after a hand-off, else ``agent_id``.

        ``agent_id`` is the agent the channel maps to, which the conversation starts with. A
        stored specialist is used only while that agent still lists it and it is still
        published, so a changed definition is never overridden by an old row.
        """
        self._runtime(tenant_id)
        stored = await self.threads.agent_for(
            tenant_id, ids.thread_key(tenant_id, channel, thread_ts)
        )
        if stored is None or stored == agent_id:
            return agent_id
        front = await self.agents.published(tenant_id, agent_id)
        if front is None or stored not in front.specialists:
            return agent_id
        if await self.agents.published(tenant_id, stored) is None:
            return agent_id
        return stored

    @activity.defn(name="remember_thread_agent")
    async def remember_thread_agent(
        self, tenant_id: str, channel: str, thread_ts: str, agent_id: str
    ) -> None:
        """Keep the specialist a thread was handed to, so it survives the workflow closing."""
        await self._agent(tenant_id, agent_id)
        await self.threads.set_agent(
            tenant_id, ids.thread_key(tenant_id, channel, thread_ts), agent_id, self.clock()
        )

    async def _audience_decision(self, ctx: TurnContext, agent: AgentVersion) -> AudienceDecision:
        """May the requester use ``agent`` in this place? A failed lookup raises.

        A transient Slack failure is retried (after Slack's own delay when it gave one);
        a lasting one fails for good.
        """
        try:
            decision = await self.audience.decide(
                ctx.tenant_id, agent, ctx.requester_user_id, ctx.channel, ctx.channel_kind
            )
        except SlackLookupError as err:
            log.warning(
                "audience lookup failed tenant_id=%s agent_id=%s code=%s transient=%s",
                ctx.tenant_id,
                ctx.agent_id,
                err.code,
                err.transient,
            )
            if err.transient:
                if err.retry_after is not None:
                    # Slack said when to come back; Temporal waits that long, not its backoff.
                    raise ApplicationError(
                        str(err),
                        type="SlackLookupError",
                        next_retry_delay=timedelta(seconds=err.retry_after),
                    ) from err
                raise
            raise ApplicationError(str(err), type="SlackLookupError", non_retryable=True) from err
        return decision

    @activity.defn(name="check_audience")
    async def check_audience(self, ctx: TurnContext) -> AudienceDecision:
        """May the requester use this agent in this place?

        Asker, place and agent come from ``ctx``, which the runtime built from the Slack
        event. A transient Slack failure is retried; any failure that survives the
        retries reaches the workflow as an error, which refuses the turn. On a refusal the
        decision carries the agent's creator, for the reply to name, and says whether the
        reply may mention them: the first refusal of an asker in a place each UTC day does,
        later ones do not, so the creator is not paged every time. A channel where the
        agent is paused refuses with reason ``paused``, before any Slack lookup.
        """
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        self._log("check_audience", ctx.tenant_id, ctx.agent_id)
        if isinstance(refusal, AgentOff):
            # An agent that is off is not available to anyone: the turn, or a hand-off to it,
            # is refused with the fixed "turned off" reply.
            return AudienceDecision(False, OFF)
        if isinstance(refusal, AgentUnavailable):
            # An agent whose grant check failed is not available to anyone either.
            return AudienceDecision(False, HEALTH)
        agent = await self._pinned(ctx)
        decision = await self._audience_decision(ctx, agent)
        if not decision.allowed:
            record = await self.agents.get_agent(ctx.tenant_id, ctx.agent_id)
            decision.owner = None if record is None else record.created_by
            if decision.owner and decision.reason not in (UNAVAILABLE, PAUSED):
                # The reply mentions the owner, which pages them: once a day for each person
                # in each place, so someone who keeps asking cannot keep paging the owner.
                decision.notify_owner = await self.owner_pings.claim(
                    ctx.tenant_id,
                    ctx.agent_id,
                    ctx.channel,
                    ctx.requester_user_id,
                    self.clock().astimezone(UTC).date(),
                )
        return decision

    @activity.defn(name="check_before_write")
    async def check_before_write(self, spec: TaskSpec, task_id: str) -> str | None:
        """Just before an approved write: may the agent still act for the requester here?

        None means go on. Otherwise the line the card shows, and nothing is to be written:
        the agent was turned off, the agent is unavailable (its grant check failed), the
        agent's channel was paused, or the requester is no longer in the audience of the
        agent's current published version. A switch that covers the write itself (writes,
        the connector) is left to the write activity, which says so in its own words. A
        lookup that fails for good fails the activity.
        """
        ctx = spec.ctx
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.WRITE)
        self._log("check_before_write", ctx.tenant_id, ctx.agent_id)
        payload = await self._spec_payload(spec)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        if isinstance(refusal, AgentOff):
            line = agent_off_card_text(agent.name)
        elif isinstance(refusal, AgentUnavailable):
            line = agent_unavailable_text(agent.name)
        elif not (await self._audience_decision(ctx, agent)).allowed:
            line = no_longer_act_text(agent.name)
        else:
            return None
        if spec.tool_name == "add_comment":
            target, idem_key = payload["page_id"], f"{task_id}:comment"
        else:
            target, idem_key = payload["database_id"], f"{task_id}:create"
        await self._record_approval(spec, payload, task_id, target)
        await self._record_write(spec, payload, task_id, idem_key, target, "error")
        return line

    @activity.defn(name="record_audience_refusal")
    async def record_audience_refusal(
        self, ctx: TurnContext, event_id: str, reason: str, target: str | None = None
    ) -> None:
        """Audit an audience refusal: a turn, or a hand-off between two agents.

        ``ctx`` is the agent the event names and ``target`` the other party: the
        asked agent for a turn; for a refused hand-off the specialist and the
        front door (pre-patch histories name them the other way round). The
        event holds IDs and the reason code, never the message. A lookup
        failure is recorded as an error.
        """
        self._log("record_audience_refusal", ctx.tenant_id, ctx.agent_id, reason=reason)
        await self._record(
            ctx,
            "audience_refused",
            ids.audit_audience_refused_key(ctx.tenant_id, event_id, ctx.agent_id, target or ""),
            outcome="error" if reason == UNAVAILABLE else "ok",
            conversation_id=ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts),
            tool=reason,
            target=target,
        )

    @activity.defn(name="record_approval_closed")
    async def record_approval_closed(self, spec: TaskSpec, task_id: str, tool: str) -> None:
        """Audit an approval that closed without a write: ``declined`` or ``expired``.

        The event holds the hash of the proposed payload (the revision the card
        named), never its text. The requester is the approver on a decline; an
        expiry has no approver. A missing draft revision is non-retryable.
        """
        ctx = spec.ctx
        self._log("record_approval_closed", ctx.tenant_id, ctx.agent_id, tool=tool)
        payload = await self._spec_payload(spec)
        await self._record(
            ctx,
            "approval_closed",
            ids.audit_approval_closed_key(task_id, spec.approval_nonce),
            task_id=task_id,
            tool=tool,
            input_sha256=canonical_sha256(payload),
            approver_user_id=ctx.requester_user_id if tool == "declined" else None,
        )


def _stored_step(resp: LLMResponse) -> StoredStep:
    """What the model returned, as it is stored for the step: its blocks and, for a final reply, its text."""
    if resp.tool_calls:
        return StoredStep(resp.stop_reason, assistant_blocks(resp), None)
    text = resp.text or NO_ANSWER
    content = [dict(b) for b in resp.content] or [{"type": "text", "text": text}]
    if not any(b.get("type") == "text" for b in content):
        content.append({"type": "text", "text": text})
    return StoredStep(resp.stop_reason, content, text)


def _has_text(step: StoredStep) -> bool:
    """Whether the model wrote text of its own (a final reply with none shows ``NO_ANSWER``)."""
    if step.reply_text is not None:
        return step.reply_text != NO_ANSWER
    return any(b.get("type") == "text" and str(b.get("text", "")).strip() for b in step.content)


def _call_ref(block: dict) -> ToolCallRef:
    """What the workflow may know about a tool call: never its input, only values derived from it."""
    name, tool_input = block.get("name", ""), block.get("input")
    kind, problem = screen(name, tool_input)
    target = tool_input.get("agent") if isinstance(tool_input, dict) else None
    return ToolCallRef(
        id=block["id"],
        name=name,
        kind=kind,
        problem=problem,
        input_sha256=canonical_sha256(tool_input),
        target=target if isinstance(target, str) and AGENT_ID_PATTERN.fullmatch(target) else "",
    )


def _format_items(items: list[NotionItem]) -> str:
    if not items:
        return "No matching items."
    # Titles and statuses are text people wrote in Notion: untrusted data, wrapped and
    # escaped the way knowledge passages are, one line per item.
    lines = [
        f'<item url="{escape_attribute(i.url)}" status="{escape_attribute(i.status or "none")}">'
        f"{escape_attribute(i.title)}</item>"
        for i in items[:MAX_LISTED_ITEMS]
    ]
    if len(items) > MAX_LISTED_ITEMS:
        lines.append(f"... and {len(items) - MAX_LISTED_ITEMS} more")
    return "<items>\n" + "\n".join(lines) + "\n</items>"


def _without_content(spec: TaskSpec, item: NotionItem) -> NotionItem:
    """``item`` as a task's workflow may carry it: a task with a stored draft gets no title or link."""
    if not spec.draft_ref:
        return item
    return NotionItem(page_id=item.page_id, url="", title="", status=item.status)


def _card_text(spec: TaskSpec, payload: dict, db_name: str) -> str:
    """The plain-text fallback of the approval card."""
    if spec.tool_name == "add_comment":
        return escape_slack(f"Approve comment on {payload['item_title']}")
    return escape_slack(f"Approve new item in {db_name}: {payload['title']}")


def approval_card_blocks(
    spec: TaskSpec,
    task_id: str,
    agent_name: str,
    db_name: str,
    edited: bool = False,
    requester_line: str | None = None,
    payload: dict | None = None,
) -> list:
    """The approval card: the frozen payload, who it runs as, Submit / Edit / Cancel.

    The button value carries the task workflow ID and the approval nonce. The
    task workflow accepts a click only if the nonce matches the current draft
    and the clicker is the requester. An edit issues a new nonce, so buttons on
    an older version of the card stop working. ``payload`` is the revision the card shows;
    it defaults to the payload the spec carries.
    """
    value = json.dumps({"task_id": task_id, "nonce": spec.approval_nonce})
    shown = dict(spec.payload if payload is None else payload)
    if "item_title" in shown:
        # A Notion title can be 2000 characters; the card's head is not the approved text.
        shown["item_title"] = shown["item_title"][:200]
    payload = {k: escape_slack(v) if isinstance(v, str) else v for k, v in shown.items()}
    agent_name = escape_slack(agent_name)
    db_name = escape_slack(db_name)
    if spec.tool_name == "add_comment":
        head = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{agent_name}* wants to comment on *{payload['item_title']}*",
            },
        }
        body = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Comment*\n{payload['comment']}"},
        }
        note = "Nothing is posted until you click Submit."
    else:
        head = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{agent_name}* wants to add an item to *{db_name}* in Notion.",
            },
        }
        body = {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Title*\n{payload['title']}"},
                {"type": "mrkdwn", "text": f"*Details*\n{payload['details']}"},
            ],
        }
        note = "Nothing is filed until you click Submit."
    if requester_line:
        # The requester has no Notion user, so the page body carries this line. It is
        # shown here so that what is approved is what is written.
        body = [
            body,
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (f"*Added to the page*\n{escape_slack(requester_line)}"),
                },
            },
        ]
    else:
        body = [body]
    context = [{"type": "mrkdwn", "text": f"Will run as the {agent_name} agent. {note}"}]
    if edited:
        context.append({"type": "mrkdwn", "text": "Edited — review and approve again"})
    return [
        head,
        *body,
        {"type": "context", "elements": context},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "action_id": "task_approve",
                    "text": {"type": "plain_text", "text": "Submit"},
                    "style": "primary",
                    "value": value,
                },
                {
                    "type": "button",
                    "action_id": "task_edit",
                    "text": {"type": "plain_text", "text": "Edit"},
                    "value": value,
                },
                {
                    "type": "button",
                    "action_id": "task_cancel",
                    "text": {"type": "plain_text", "text": "Cancel"},
                    "value": value,
                },
            ],
        },
    ]
