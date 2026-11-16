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
  a write that was approved earlier.
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
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agentos import ids
from agentos.agent_store import AgentStore
from agentos.audience import OFF, UNAVAILABLE, AudienceChecker, AudienceDecision, no_longer_act_text
from agentos.audience_pauses import InMemoryPauseStore, PauseStore
from agentos.audit import AuditStore, agent_identity, canonical_sha256
from agentos.budgets import BudgetGuard
from agentos.config import AgentVersion, Settings
from agentos.connectors.manifest import MANIFEST_DIR, load_manifest
from agentos.connectors.notion import (
    ConnectorError,
    NotionConnector,
    normalize_id,
)
from agentos.history_view import prepare_for_model
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.search import (
    KNOWLEDGE_RULES,
    SEARCH_CANDIDATES,
    SEARCH_RESULTS,
    PageChecker,
    cited_markers,
    format_sources,
    sources_footer,
)
from agentos.knowledge.store import KnowledgeStore
from agentos.llm import LLMClient
from agentos.models import (
    AuditEvent,
    Citation,
    LLMResponse,
    NotionItem,
    TaskSpec,
    ToolCall,
    ToolResult,
    TurnContext,
)
from agentos.slack_client import SlackLookupError
from agentos.slack_format import escape_slack, to_slack_mrkdwn
from agentos.stores import (
    ExternalWriteStore,
    HistoryStore,
    IdempotencyStore,
    InMemoryExternalWriteStore,
    TaskLinkStore,
    ToolOutputStore,
)
from agentos.switches import (
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
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant
from agentos.usage import UsageRecorder
from agentos.workflows import WRITE_LEASE_SECONDS

log = logging.getLogger("agentos.activities")

READ_TOOLS = ("search_items", "my_items", "search_knowledge")
MAX_LISTED_ITEMS = 20
HAND_OFF_TOOL = "hand_off"

# An attempt that takes over from one that may have sent a write looks for the result
# this many times, this far apart, before it writes again. Notion's search can lag.
VISIBILITY_TRIES = 4
VISIBILITY_INTERVAL = 2.5
# A retryable failure: another attempt holds the lease for this write.
WRITE_IN_PROGRESS = "WriteInProgress"
REQUESTER_NAME_MAX = 80

T = TypeVar("T")


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


def _is_client_error(err: Exception) -> bool:
    """A 4xx (other than 408 and 429) will not succeed on retry.

    Notion's errors carry the HTTP status in ``status``; the Anthropic and
    OpenAI SDK errors carry it in ``status_code``. Timeouts (408) and rate
    limits (429) are transient, so they stay retryable.
    """
    for attr in ("status", "status_code"):
        status = getattr(err, attr, None)
        if isinstance(status, int) and not isinstance(status, bool):
            return 400 <= status < 500 and status not in (408, 429)
    return False


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
    ):
        self.settings = settings
        self.writes = external_writes or InMemoryExternalWriteStore()
        self.visibility_tries = VISIBILITY_TRIES
        self.visibility_interval = VISIBILITY_INTERVAL
        self.switches = switches or InMemorySwitchStore()
        self.usage = usage
        self.budgets = budgets
        self.registry = registry
        self.pauses = pauses or InMemoryPauseStore()
        self.audience = audience or AudienceChecker(
            registry, agents, pauses=self.pauses, switches=self.switches
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
            self.finalize_reply,
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
            self.record_hand_off,
            self.check_audience,
            self.check_before_write,
            self.record_audience_refusal,
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

        An unknown tenant fails the activity for good; it is never answered
        with another tenant's runtime.
        """
        try:
            return self.registry.get(tenant_id)
        except UnknownTenant as err:
            raise _non_retryable(err) from err

    async def _agent(self, tenant_id: str, agent_id: str) -> AgentVersion:
        """The agent's current published version, read from the store on every call."""
        self._runtime(tenant_id)
        agent = await self.agents.published(tenant_id, agent_id)
        if agent is None:
            raise ApplicationError(
                f"unknown agent '{agent_id}'", type="ValueError", non_retryable=True
            )
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
        self, tenant_id: str, agent_id: str, effect: Effect
    ) -> Exception | None:
        """Why the agent may not do ``effect`` now, or None. The one place switches are read.

        Every activity that acts for an agent calls this (or ``_gate``) first, so one read
        of the switches applies the agent, writes and connector scopes that matter to the
        effect. Read from the store now, never cached. An unreadable switch raises, so the
        step fails and is retried rather than let through.
        """
        state = await read_state(self.switches, tenant_id)
        refusal = state.refusal(agent_id, effect)
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

    async def _gate_write(self, spec: TaskSpec, task_id: str, target: str, idem_key: str):
        """Refuse an approved write, before any external call, while a switch covers it.

        The approval and the refusal are audited (the write as an error), and the
        failure is non-retryable, so the task ends with the reason and nothing was
        changed. The requester can ask again later.
        """
        ctx = spec.ctx
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.WRITE)
        if refusal is None:
            return
        await self._record_approval(spec, task_id, target)
        await self._record_write(spec, task_id, idem_key, target, "error")
        raise _non_retryable(refusal)

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
    ) -> T:
        """Make one external write happen once, however many attempts run it.

        A stored result is returned without touching the target. Otherwise the key is
        claimed with a lease that outlasts the activity timeout. Another attempt's live
        lease means this one writes nothing and fails with a retryable error. After a
        claim that took over from an attempt that had sent the write, ``find`` is tried
        a few times first, because the target's search can lag. Then the write is marked
        as about to be sent, sent, and its result stored right away. Any failure ends
        the lease at once so the retry does not wait for it, but keeps the mark, so that
        retry looks before it writes.
        """
        claim = await self.writes.claim(tenant_id, idem_key, WRITE_LEASE_SECONDS)
        if claim.status == "done":
            return from_ref(claim.result_ref)
        if claim.status == "busy":
            raise ApplicationError(
                "another attempt is writing this; trying again shortly",
                type=WRITE_IN_PROGRESS,
            )
        try:
            made = None
            for look in range(self.visibility_tries if claim.issued else 1):
                if look:
                    _beat()
                    await asyncio.sleep(self.visibility_interval)
                made = await find()
                if made is not None:
                    break
            if made is None:
                _beat()
                await self.writes.mark_issued(tenant_id, idem_key, WRITE_LEASE_SECONDS)
                made = await write()
            _beat()
            return from_ref(await self.writes.complete(tenant_id, idem_key, to_ref(made)))
        except Exception:
            await self.writes.release(tenant_id, idem_key)
            raise

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

    async def _record_approval(self, spec: TaskSpec, task_id: str, target: str) -> None:
        """Record that the requester approved exactly this payload, before it runs.

        The task workflow accepts a decision only from the requester, so the
        approver is the requester. Nothing reaches this activity until then.
        """
        ctx = spec.ctx
        await self._record(
            ctx,
            "approval",
            ids.audit_approval_key(task_id, spec.approval_nonce),
            task_id=task_id,
            tool=spec.tool_name,
            target=target,
            input_sha256=canonical_sha256(spec.payload),
            approver_user_id=ctx.requester_user_id,
        )

    async def _record_write(
        self,
        spec: TaskSpec,
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
            input_sha256=canonical_sha256(spec.payload),
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

        Delivery is at least once. A crash between Slack accepting the post and
        the ts being recorded can repeat the post, because Slack has no
        idempotency key. A "pending" marker is written before posting, so a
        call that finds the marker without a ts logs a possible duplicate.
        """
        existing = await self.idem.get(tenant_id, idem_key)
        if existing is not None:
            return existing["ts"]
        pending_key = f"{idem_key}:pending"
        if await self.idem.get(tenant_id, pending_key) is not None:
            log.warning(
                "possible duplicate Slack post tenant_id=%s idem_key=%s", tenant_id, idem_key
            )
        await self.idem.put_if_absent(tenant_id, pending_key, {"channel": channel})
        slack = self._runtime(tenant_id).slack
        ts = await slack.post_message(channel, text, thread_ts=thread_ts, blocks=blocks)
        stored = await self.idem.put_if_absent(tenant_id, idem_key, {"channel": channel, "ts": ts})
        return stored["ts"]

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
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        if isinstance(refusal, AgentOff):
            log_stop(ctx.tenant_id, ctx.agent_id, refusal.switch)
            return LLMResponse(
                text=agent_off_text(agent.name), tool_calls=[], stop_reason=SWITCHED_OFF
            )
        if self.budgets is not None:
            # Before any model call: a reached budget answers with a fixed text, bills nothing
            # and records no usage row.
            stopped = await self.budgets.stop_response(ctx.tenant_id, ctx.agent_id, agent.name)
            if stopped is not None:
                return stopped
        messages = prepare_for_model(await self.history.load(ctx.tenant_id, conversation_id))
        tools = self._connector_for(ctx.tenant_id, agent).tools_for_llm()
        system = agent.system_prompt
        if agent.grants.notion_pages:
            # Search is offered only to an agent that was granted pages, and
            # the model is told to treat what it returns as data.
            tools = tools + [self.knowledge_tool.llm_definition()]
            system = f"{agent.system_prompt}\n\n{KNOWLEDGE_RULES}"
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

    @activity.defn(name="post_placeholder")
    async def post_placeholder(self, ctx: TurnContext, idem_key: str) -> str:
        self._log("post_placeholder", ctx.tenant_id, ctx.agent_id)
        return await self._post_once(
            ctx.tenant_id, idem_key, ctx.channel, "Thinking…", ctx.thread_ts
        )

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
        footer = await self._sources_footer(ctx, text, citations or [])
        slack = self._runtime(ctx.tenant_id).slack
        await slack.update_message(ctx.channel, placeholder_ts, to_slack_mrkdwn(text) + footer)
        if audit_answer:
            conversation_id = ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts)
            await self._record(
                ctx,
                "answer",
                ids.audit_answer_key(conversation_id, placeholder_ts),
                conversation_id=conversation_id,
                output_sha256=canonical_sha256(text),
            )

    async def _sources_footer(self, ctx: TurnContext, text: str, citations: list[Citation]) -> str:
        if not citations or self.knowledge is None:
            return ""
        scopes = await self._granted_scopes(ctx.tenant_id, ctx.agent_id)
        cited = [c for c in cited_markers(text, citations) if c.scope in scopes]
        if not cited:
            return ""
        meta = await self.knowledge.page_meta(ctx.tenant_id, scopes, [c.page_id for c in cited])
        return sources_footer(cited, meta)

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
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.READ)
        citations: list[Citation] = []
        if isinstance(refusal, AgentOff):
            # A call dispatched before the agent was turned off. The model gets an error
            # it can pass on; nothing is read.
            agent = await self._agent(ctx.tenant_id, ctx.agent_id)
            text, is_error = agent_off_text(agent.name), True
        elif refusal is not None:
            # Reading and searching Notion both go through the connector.
            text, is_error = connector_off_text("notion"), True
        elif call.name == "search_knowledge":
            text, is_error, citations = await self._search_knowledge(ctx, call, first_marker)
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
        them inside its query. Each page found is then re-checked with Notion
        (not trashed, still under its granted root) before it can reach the
        model. The numbered passages are stored as the tool output; only the
        citation numbers and page IDs go back to the workflow.
        """
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        roots = {ids.knowledge_scope(g.id): g.id for g in agent.grants.notion_pages}
        if not roots or self.knowledge is None or self.embedder is None:
            return "This agent has no documents to search.", True, []
        query = call.input.get("query")
        if not isinstance(query, str) or not query.strip():
            return "search_knowledge needs a non-empty 'query'.", True, []
        try:
            [vector] = await self.embedder.embed([query.strip()])
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            raise
        hits = await self.knowledge.search(
            ctx.tenant_id, list(roots), vector, limit=SEARCH_CANDIDATES
        )
        checker = PageChecker(self._runtime(ctx.tenant_id).notion_api)
        live: dict[tuple[str, str], bool] = {}
        kept = []
        for hit in hits:
            key = (hit.scope, hit.page_id)
            if key not in live:
                live[key] = await checker.under_root(hit.page_id, roots[hit.scope])
            if live[key]:
                kept.append(hit)
        kept = kept[:SEARCH_RESULTS]
        citations = [
            Citation(first_marker + i, hit.scope, hit.page_id) for i, hit in enumerate(kept)
        ]
        passages = [(c.marker, hit.title, hit.text) for c, hit in zip(citations, kept)]
        return format_sources(passages), False, citations

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
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector_for(ctx.tenant_id, agent).grant_for(spec.payload["database_id"])
        line = await self._card_requester_line(spec, task_id, agent)
        dm = await self._runtime(ctx.tenant_id).slack.open_dm(ctx.requester_user_id)
        blocks = approval_card_blocks(spec, task_id, agent.name, grant.name, requester_line=line)
        text = _card_text(spec, grant.name)
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
        await self._replace_card(spec, task_id, card_ts, f"{text}: {_subject(spec)}")

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
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector_for(ctx.tenant_id, agent).grant_for(spec.payload["database_id"])
        line = await self._card_requester_line(spec, task_id, agent)
        blocks = approval_card_blocks(
            spec, task_id, agent.name, grant.name, edited=True, requester_line=line
        )
        await slack.update_message(channel, card_ts, _card_text(spec, grant.name), blocks=blocks)

    @activity.defn(name="prepare_comment")
    async def prepare_comment(self, spec: TaskSpec) -> dict:
        """Resolve the item to comment on and return the payload to freeze.

        The page's database must be one this agent was granted; otherwise this
        fails before any card is posted or anything is written.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("prepare_comment", ctx.tenant_id, ctx.agent_id)
        try:
            connector = await self._connector(ctx.tenant_id, ctx.agent_id)
            item = await connector.resolve_item(spec.payload["item"])
        except ConnectorError as err:
            raise _non_retryable(err) from err
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            raise
        return {
            "page_id": item.page_id,
            "database_id": item.database_id,
            "item_title": item.title,
            "item_url": item.url,
            "comment": spec.payload["comment"],
        }

    @activity.defn(name="add_notion_comment")
    async def add_notion_comment(self, spec: TaskSpec, task_id: str) -> str:
        ctx = spec.ctx
        self._log("add_notion_comment", ctx.tenant_id, ctx.agent_id)
        page_id = spec.payload["page_id"]
        idem_key = f"{task_id}:comment"
        await self._gate_write(spec, task_id, page_id, idem_key)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        connector = self._connector_for(ctx.tenant_id, agent)
        text = spec.payload["comment"]
        await self._record_approval(spec, task_id, page_id)
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
                await self._record_write(spec, task_id, idem_key, page_id, "error")
                raise _non_retryable(err) from err
            raise
        await self._record_write(spec, task_id, idem_key, page_id, "ok", {"comment_id": comment_id})
        return comment_id

    @activity.defn(name="create_notion_item")
    async def create_notion_item(self, spec: TaskSpec, task_id: str) -> NotionItem:
        ctx = spec.ctx
        self._log("create_notion_item", ctx.tenant_id, ctx.agent_id)
        database_id = spec.payload["database_id"]
        idem_key = f"{task_id}:create"
        await self._gate_write(spec, task_id, database_id, idem_key)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        connector = self._connector_for(ctx.tenant_id, agent)
        await self._record_approval(spec, task_id, database_id)
        try:
            connector.grant_for(database_id)
            requester = await self._requester(spec, task_id, agent)
            item = await self._write_once(
                ctx.tenant_id,
                idem_key,
                find=lambda: connector.find_item(database_id, idem_key),
                write=lambda: connector.create_item(
                    database_id,
                    spec.payload["title"],
                    spec.payload["details"],
                    idem_key,
                    notion_user_id=requester["notion_user_id"],
                    requester_line=requester["line"],
                ),
                to_ref=_item_ref,
                from_ref=_item_from_ref,
            )
        except Exception as err:
            if isinstance(err, ConnectorError) or _is_client_error(err):
                await self._record_write(spec, task_id, idem_key, database_id, "error")
                raise _non_retryable(err) from err
            raise
        await self.links.link(ctx.tenant_id, normalize_id(item.page_id), task_id)
        await self._record_write(
            spec, task_id, idem_key, item.page_id, "ok", {"page_id": item.page_id}
        )
        return item

    @activity.defn(name="reconcile_notion_item")
    async def reconcile_notion_item(self, spec: TaskSpec, task_id: str) -> NotionItem | None:
        """After the create ran out of retries: is the item there? Reads only, one look.

        The task calls this a few times, a few seconds apart. None means this look did not
        find it, which is not proof that nothing was filed.
        """
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("reconcile_notion_item", ctx.tenant_id, ctx.agent_id)
        connector = await self._connector(ctx.tenant_id, ctx.agent_id)
        idem_key = f"{task_id}:create"
        ref = await self.writes.result(ctx.tenant_id, idem_key)
        item = (
            _item_from_ref(ref)
            if ref is not None
            else await connector.find_item(spec.payload["database_id"], idem_key)
        )
        if item is None:
            return None
        await self.links.link(ctx.tenant_id, normalize_id(item.page_id), task_id)
        await self._record_write(
            spec, task_id, idem_key, item.page_id, "ok", {"page_id": item.page_id}
        )
        return item

    @activity.defn(name="reconcile_notion_comment")
    async def reconcile_notion_comment(self, spec: TaskSpec, task_id: str) -> str | None:
        """The comment counterpart of ``reconcile_notion_item``: its ID, or None for "not seen"."""
        ctx = spec.ctx
        await self._gate(ctx.tenant_id, ctx.agent_id, Effect.READ)
        self._log("reconcile_notion_comment", ctx.tenant_id, ctx.agent_id)
        connector = await self._connector(ctx.tenant_id, ctx.agent_id)
        idem_key = f"{task_id}:comment"
        page_id = spec.payload["page_id"]
        comment_id = await self.writes.result(ctx.tenant_id, idem_key)
        if comment_id is None:
            await connector.resolve_item(page_id)
            comment_id = await connector.find_comment(page_id, idem_key)
        if comment_id is None:
            return None
        await self._record_write(spec, task_id, idem_key, page_id, "ok", {"comment_id": comment_id})
        return comment_id

    @activity.defn(name="get_notion_item")
    async def get_notion_item(self, tenant_id: str, agent_id: str, page_id: str) -> NotionItem:
        """Read one item. Takes IDs only: the watch polls this thousands of times."""
        refusal = await self._gate_refusal(tenant_id, agent_id, Effect.POLL)
        self._log("get_notion_item", tenant_id, agent_id)
        if isinstance(refusal, AgentOff):
            # The agent is off: the task ends the watch, with no post. No Notion call is made.
            raise _non_retryable(refusal)
        if refusal is not None:
            # Retryable on purpose: the task workflow treats a poll that fails after its
            # retries as "still watching" and polls again later, while a permanent
            # failure would end the watch. No Notion call is made.
            raise ApplicationError(connector_off_text("notion"), type="ConnectorOff")
        try:
            connector = await self._connector(tenant_id, agent_id)
            return await connector.get_item(page_id)
        except ConnectorError as err:
            raise _non_retryable(err) from err
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            raise

    @activity.defn(name="post_in_thread")
    async def post_in_thread(self, ctx: TurnContext, text: str, idem_key: str) -> str:
        self._log("post_in_thread", ctx.tenant_id, ctx.agent_id)
        return await self._post_once(ctx.tenant_id, idem_key, ctx.channel, text, ctx.thread_ts)

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
        """May the requester use this agent in this place? Read-only.

        Asker, place and agent come from ``ctx``, which the runtime built from the Slack
        event. A transient Slack failure is retried; any failure that survives the
        retries reaches the workflow as an error, which refuses the turn. On a refusal the
        decision carries the agent's creator, for the reply to name. A channel where the
        agent is paused refuses with reason ``paused``, before any Slack lookup.
        """
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.MODEL)
        self._log("check_audience", ctx.tenant_id, ctx.agent_id)
        if isinstance(refusal, AgentOff):
            # An agent that is off is not available to anyone: the turn, or a hand-off to it,
            # is refused with the fixed "turned off" reply.
            return AudienceDecision(False, OFF)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        decision = await self._audience_decision(ctx, agent)
        if not decision.allowed:
            record = await self.agents.get_agent(ctx.tenant_id, ctx.agent_id)
            decision.owner = None if record is None else record.created_by
        return decision

    @activity.defn(name="check_before_write")
    async def check_before_write(self, spec: TaskSpec, task_id: str) -> str | None:
        """Just before an approved write: may the agent still act for the requester here?

        None means go on. Otherwise the line the card shows, and nothing is to be written:
        the agent was turned off, the agent's channel was paused, or the requester is no
        longer in the audience of the agent's current published version. A switch that
        covers the write itself (writes, the connector) is left to the write activity,
        which says so in its own words. A lookup that fails for good fails the activity.
        """
        ctx = spec.ctx
        refusal = await self._gate_refusal(ctx.tenant_id, ctx.agent_id, Effect.WRITE)
        self._log("check_before_write", ctx.tenant_id, ctx.agent_id)
        agent = await self._agent(ctx.tenant_id, ctx.agent_id)
        if isinstance(refusal, AgentOff):
            line = agent_off_card_text(agent.name)
        elif not (await self._audience_decision(ctx, agent)).allowed:
            line = no_longer_act_text(agent.name)
        else:
            return None
        if spec.tool_name == "add_comment":
            target, idem_key = spec.payload["page_id"], f"{task_id}:comment"
        else:
            target, idem_key = spec.payload["database_id"], f"{task_id}:create"
        await self._record_approval(spec, task_id, target)
        await self._record_write(spec, task_id, idem_key, target, "error")
        return line

    @activity.defn(name="record_audience_refusal")
    async def record_audience_refusal(
        self, ctx: TurnContext, event_id: str, reason: str, target: str | None = None
    ) -> None:
        """Audit a turn, or a hand-off to ``target``, refused by an audience.

        ``ctx`` is the agent that was asked. The event holds IDs and the reason
        code, never the message. A lookup failure is recorded as an error.
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


def _format_items(items: list[NotionItem]) -> str:
    if not items:
        return "No matching items."
    lines = [
        f"- {i.title} (status: {i.status or 'none'}) {i.url}" for i in items[:MAX_LISTED_ITEMS]
    ]
    if len(items) > MAX_LISTED_ITEMS:
        lines.append(f"... and {len(items) - MAX_LISTED_ITEMS} more")
    return "\n".join(lines)


def _subject(spec: TaskSpec) -> str:
    """The short name of what a task is about, for card and thread lines."""
    if spec.tool_name == "add_comment":
        return spec.payload.get("item_title") or spec.payload["item"]
    return spec.payload["title"]


def _card_text(spec: TaskSpec, db_name: str) -> str:
    """The plain-text fallback of the approval card."""
    if spec.tool_name == "add_comment":
        return escape_slack(f"Approve comment on {spec.payload['item_title']}")
    return escape_slack(f"Approve new item in {db_name}: {spec.payload['title']}")


def approval_card_blocks(
    spec: TaskSpec,
    task_id: str,
    agent_name: str,
    db_name: str,
    edited: bool = False,
    requester_line: str | None = None,
) -> list:
    """The approval card: the frozen payload, who it runs as, Submit / Edit / Cancel.

    The button value carries the task workflow ID and the approval nonce. The
    task workflow accepts a click only if the nonce matches the current draft
    and the clicker is the requester. An edit issues a new nonce, so buttons on
    an older version of the card stop working.
    """
    value = json.dumps({"task_id": task_id, "nonce": spec.approval_nonce})
    shown = dict(spec.payload)
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
