"""Temporal activities: every piece of I/O the workflows do.

Rules kept here:

* Every activity works for exactly one tenant, named by its arguments: the
  ``TurnContext`` / ``TaskSpec`` tenant, or an explicit ``tenant_id``. It uses
  only that tenant's Slack and Notion clients, and an unknown tenant is a
  non-retryable failure, never a fallback to some other tenant.
* The agent uses its own access. Notion calls go through a ``NotionConnector``
  built from that agent's grants, which checks every call before it is sent.
* The requester comes from ``TurnContext`` / ``TaskSpec``, which the runtime
  filled in from the Slack event. Requester-scoped tool fields (the person
  whose records are read) are filled in here, never by the LLM.
* Connector errors that a retry cannot fix become non-retryable failures.
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
* Log lines carry tenant, agent and workflow IDs only, never message text,
  document content or secrets.
"""

from __future__ import annotations

import json
import logging

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agentos import ids
from agentos.audit import AuditStore, agent_identity, canonical_sha256
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
from agentos.slack_format import escape_slack, to_slack_mrkdwn
from agentos.stores import HistoryStore, IdempotencyStore, TaskLinkStore, ToolOutputStore
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant

log = logging.getLogger("agentos.activities")

READ_TOOLS = ("search_items", "my_items", "search_knowledge")
MAX_LISTED_ITEMS = 20
HAND_OFF_TOOL = "hand_off"


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
        knowledge: KnowledgeStore | None = None,
        embedder: Embedder | None = None,
    ):
        self.settings = settings
        self.registry = registry
        self.knowledge = knowledge
        self.embedder = embedder
        self.knowledge_tool = load_manifest(MANIFEST_DIR / "knowledge.yaml").tools[
            "search_knowledge"
        ]
        self.history = history
        self.tool_outputs = tool_outputs
        self.audit = audit
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
            self.refresh_card,
            self.prepare_comment,
            self.add_notion_comment,
            self.create_notion_item,
            self.get_notion_item,
            self.post_in_thread,
            self.record_hand_off,
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

    def _agent(self, tenant_id: str, agent_id: str) -> AgentVersion:
        agent = self._runtime(tenant_id).config.agents.get(agent_id)
        if agent is None:
            raise ApplicationError(
                f"unknown agent '{agent_id}'", type="ValueError", non_retryable=True
            )
        return agent

    def _connector(self, tenant_id: str, agent_id: str) -> NotionConnector:
        self._agent(tenant_id, agent_id)
        return self._runtime(tenant_id).connectors[agent_id]

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
        return self._agent(tenant_id, agent_id)

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
        agent = self._agent(ctx.tenant_id, ctx.agent_id)
        messages = prepare_for_model(await self.history.load(ctx.tenant_id, conversation_id))
        tools = self._connector(ctx.tenant_id, ctx.agent_id).tools_for_llm()
        system = agent.system_prompt
        if agent.grants.notion_pages:
            # Search is offered only to an agent that was granted pages, and
            # the model is told to treat what it returns as data.
            tools = tools + [self.knowledge_tool.llm_definition()]
            system = f"{agent.system_prompt}\n\n{KNOWLEDGE_RULES}"
        if agent.specialists:
            specialists = [self._agent(ctx.tenant_id, s) for s in agent.specialists]
            tools = tools + [hand_off_tool(specialists)]
        try:
            return await self.llm.complete(system, messages, tools)
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
        scopes = self._granted_scopes(ctx.tenant_id, ctx.agent_id)
        cited = [c for c in cited_markers(text, citations) if c.scope in scopes]
        if not cited:
            return ""
        meta = await self.knowledge.page_meta(ctx.tenant_id, scopes, [c.page_id for c in cited])
        return sources_footer(cited, meta)

    def _granted_scopes(self, tenant_id: str, agent_id: str) -> list[str]:
        agent = self._agent(tenant_id, agent_id)
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
        citations: list[Citation] = []
        if call.name == "search_knowledge":
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
        agent = self._agent(ctx.tenant_id, ctx.agent_id)
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
        connector = self._connector(ctx.tenant_id, ctx.agent_id)
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
        self._log("post_approval_card", ctx.tenant_id, ctx.agent_id)
        key = f"{task_id}:card"
        existing = await self.idem.get(ctx.tenant_id, key)
        if existing is not None:
            return existing["ts"]
        agent = self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector(ctx.tenant_id, ctx.agent_id).grant_for(spec.payload["database_id"])
        dm = await self._runtime(ctx.tenant_id).slack.open_dm(ctx.requester_user_id)
        blocks = approval_card_blocks(spec, task_id, agent.name, grant.name)
        text = _card_text(spec, grant.name)
        return await self._post_once(ctx.tenant_id, key, dm, text, None, blocks)

    @activity.defn(name="update_card")
    async def update_card(self, spec: TaskSpec, task_id: str, card_ts: str, text: str) -> None:
        ctx = spec.ctx
        self._log("update_card", ctx.tenant_id, ctx.agent_id)
        record = await self.idem.get(ctx.tenant_id, f"{task_id}:card")
        slack = self._runtime(ctx.tenant_id).slack
        channel = record["channel"] if record else await slack.open_dm(ctx.requester_user_id)
        line = escape_slack(f"{text}: {_subject(spec)}")
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": line}}]
        await slack.update_message(channel, card_ts, line, blocks=blocks)

    @activity.defn(name="refresh_card")
    async def refresh_card(self, spec: TaskSpec, task_id: str, card_ts: str) -> None:
        """Redraw the approval card in place after an edit.

        It shows the edited draft and buttons carrying the new nonce, so a
        click on an older version of the card no longer matches.
        """
        ctx = spec.ctx
        self._log("refresh_card", ctx.tenant_id, ctx.agent_id)
        record = await self.idem.get(ctx.tenant_id, f"{task_id}:card")
        slack = self._runtime(ctx.tenant_id).slack
        channel = record["channel"] if record else await slack.open_dm(ctx.requester_user_id)
        agent = self._agent(ctx.tenant_id, ctx.agent_id)
        grant = self._connector(ctx.tenant_id, ctx.agent_id).grant_for(spec.payload["database_id"])
        blocks = approval_card_blocks(spec, task_id, agent.name, grant.name, edited=True)
        await slack.update_message(channel, card_ts, _card_text(spec, grant.name), blocks=blocks)

    @activity.defn(name="prepare_comment")
    async def prepare_comment(self, spec: TaskSpec) -> dict:
        """Resolve the item to comment on and return the payload to freeze.

        The page's database must be one this agent was granted; otherwise this
        fails before any card is posted or anything is written.
        """
        ctx = spec.ctx
        self._log("prepare_comment", ctx.tenant_id, ctx.agent_id)
        try:
            item = await self._connector(ctx.tenant_id, ctx.agent_id).resolve_item(
                spec.payload["item"]
            )
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
        agent = self._agent(ctx.tenant_id, ctx.agent_id)
        page_id = spec.payload["page_id"]
        idem_key = f"{task_id}:comment"
        await self._record_approval(spec, task_id, page_id)
        try:
            comment_id = await self._connector(ctx.tenant_id, ctx.agent_id).add_comment(
                page_id, spec.payload["comment"], idem_key, agent.name
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
        connector = self._connector(ctx.tenant_id, ctx.agent_id)
        database_id = spec.payload["database_id"]
        idem_key = f"{task_id}:create"
        await self._record_approval(spec, task_id, database_id)
        email = await self._runtime(ctx.tenant_id).slack.user_email(ctx.requester_user_id)
        try:
            item = await connector.create_item(
                database_id=database_id,
                title=spec.payload["title"],
                details=spec.payload["details"],
                requester_email=email,
                idem_key=idem_key,
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

    @activity.defn(name="get_notion_item")
    async def get_notion_item(self, tenant_id: str, agent_id: str, page_id: str) -> NotionItem:
        """Read one item. Takes IDs only: the watch polls this thousands of times."""
        self._log("get_notion_item", tenant_id, agent_id)
        try:
            return await self._connector(tenant_id, agent_id).get_item(page_id)
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
        self._agent(ctx.tenant_id, target_agent)
        await self._record(
            ctx,
            "hand_off",
            ids.audit_hand_off_key(ctx.tenant_id, event_id),
            conversation_id=ids.conversation_id(ctx.tenant_id, ctx.channel, ctx.thread_ts),
            target=target_agent,
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
    spec: TaskSpec, task_id: str, agent_name: str, db_name: str, edited: bool = False
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
    context = [{"type": "mrkdwn", "text": f"Will run as the {agent_name} agent. {note}"}]
    if edited:
        context.append({"type": "mrkdwn", "text": "Edited — review and approve again"})
    return [
        head,
        body,
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
