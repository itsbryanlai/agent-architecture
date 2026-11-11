"""Temporal activities: every piece of I/O the workflows do.

Rules kept here:

* The agent uses its own access. Notion calls go through a ``NotionConnector``
  built from that agent's grants, which checks every call before it is sent.
* The requester comes from ``TurnContext`` / ``TaskSpec``, which the runtime
  filled in from the Slack event. Requester-scoped tool fields (the person
  whose records are read) are filled in here, never by the LLM.
* Connector errors that a retry cannot fix become non-retryable failures.
* Slack posts are idempotent: the posted ``ts`` is stored under an idempotency
  key, and a repeat call with the same key returns it without posting again.
* Log lines carry tenant, agent and workflow IDs only, never message text,
  document content or secrets.
"""

from __future__ import annotations

import json
import logging

from temporalio import activity
from temporalio.exceptions import ApplicationError

from agentos.config import AgentsConfig, AgentVersion, Settings
from agentos.connectors.notion import (
    ConnectorError,
    NotionAPI,
    NotionConnector,
    normalize_id,
)
from agentos.llm import LLMClient
from agentos.models import LLMResponse, NotionItem, TaskSpec, ToolCall, ToolResult, TurnContext
from agentos.slack_client import SlackPoster
from agentos.stores import HistoryStore, IdempotencyStore, TaskLinkStore

log = logging.getLogger("agentos.activities")

READ_TOOLS = ("search_items", "my_items")
MAX_LISTED_ITEMS = 20


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


class AgentActivities:
    def __init__(
        self,
        settings: Settings,
        agents: AgentsConfig,
        history: HistoryStore,
        idem: IdempotencyStore,
        links: TaskLinkStore,
        llm: LLMClient,
        slack: SlackPoster,
        notion_api: NotionAPI,
    ):
        self.settings = settings
        self.agents = agents
        self.history = history
        self.idem = idem
        self.links = links
        self.llm = llm
        self.slack = slack
        # One connector per agent, each limited to that agent's own grants.
        self.connectors = {
            agent_id: NotionConnector(notion_api, agent.grants.notion_databases)
            for agent_id, agent in agents.agents.items()
        }

    def all(self) -> list:
        return [
            self.load_agent,
            self.append_history,
            self.call_llm,
            self.post_placeholder,
            self.finalize_reply,
            self.run_read_tool,
            self.post_approval_card,
            self.update_card,
            self.create_notion_item,
            self.get_notion_item,
            self.post_in_thread,
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

    def _agent(self, agent_id: str) -> AgentVersion:
        agent = self.agents.agents.get(agent_id)
        if agent is None:
            raise ApplicationError(
                f"unknown agent '{agent_id}'", type="ValueError", non_retryable=True
            )
        return agent

    def _connector(self, agent_id: str) -> NotionConnector:
        self._agent(agent_id)
        return self.connectors[agent_id]

    async def _post_once(
        self,
        tenant_id: str,
        idem_key: str,
        channel: str,
        text: str,
        thread_ts: str | None,
        blocks: list[dict] | None = None,
    ) -> str:
        existing = await self.idem.get(tenant_id, idem_key)
        if existing is not None:
            return existing["ts"]
        ts = await self.slack.post_message(channel, text, thread_ts=thread_ts, blocks=blocks)
        stored = await self.idem.put_if_absent(tenant_id, idem_key, {"channel": channel, "ts": ts})
        return stored["ts"]

    # --- conversation ------------------------------------------------------------

    @activity.defn(name="load_agent")
    async def load_agent(self, agent_id: str) -> AgentVersion:
        self._log("load_agent", self.agents.tenant_id, agent_id)
        return self._agent(agent_id)

    @activity.defn(name="append_history")
    async def append_history(
        self, tenant_id: str, conversation_id: str, role: str, content: list[dict]
    ) -> int:
        self._log("append_history", tenant_id, "-", role=role)
        if role not in ("user", "assistant"):
            raise ValueError(f"invalid role {role!r}")
        return await self.history.append(tenant_id, conversation_id, role, content)

    @activity.defn(name="call_llm")
    async def call_llm(self, ctx: TurnContext, conversation_id: str) -> LLMResponse:
        self._log("call_llm", ctx.tenant_id, ctx.agent_id)
        agent = self._agent(ctx.agent_id)
        messages = await self.history.load(ctx.tenant_id, conversation_id)
        tools = self._connector(ctx.agent_id).tools_for_llm()
        try:
            return await self.llm.complete(agent.system_prompt, messages, tools)
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
    async def finalize_reply(self, ctx: TurnContext, placeholder_ts: str, text: str) -> None:
        self._log("finalize_reply", ctx.tenant_id, ctx.agent_id)
        await self.slack.update_message(ctx.channel, placeholder_ts, text)

    @activity.defn(name="run_read_tool")
    async def run_read_tool(self, ctx: TurnContext, call: ToolCall) -> ToolResult:
        self._log("run_read_tool", ctx.tenant_id, ctx.agent_id, tool=call.name)
        connector = self._connector(ctx.agent_id)
        try:
            if call.name == "search_items":
                query = call.input.get("query")
                if not isinstance(query, str) or not query.strip():
                    return ToolResult(call.id, "search_items needs a non-empty 'query'.", True)
                items = await connector.search_items(query.strip())
            elif call.name == "my_items":
                # The person is always the requester from the runtime; any
                # person field in the LLM's input is ignored.
                email = await self.slack.user_email(ctx.requester_user_id)
                if not email:
                    return ToolResult(
                        call.id,
                        "I couldn't read your Slack email, so I can't find your items.",
                        True,
                    )
                items = await connector.my_items(email)
            else:
                return ToolResult(call.id, f"Unknown tool '{call.name}'.", True)
        except ConnectorError as err:
            return ToolResult(call.id, str(err), True)
        return ToolResult(call.id, _format_items(items))

    # --- task ------------------------------------------------------------------------

    @activity.defn(name="post_approval_card")
    async def post_approval_card(self, spec: TaskSpec, task_id: str) -> str:
        ctx = spec.ctx
        self._log("post_approval_card", ctx.tenant_id, ctx.agent_id)
        key = f"{task_id}:card"
        existing = await self.idem.get(ctx.tenant_id, key)
        if existing is not None:
            return existing["ts"]
        agent = self._agent(ctx.agent_id)
        grant = self._connector(ctx.agent_id).grant_for(spec.payload["database_id"])
        dm = await self.slack.open_dm(ctx.requester_user_id)
        blocks = approval_card_blocks(spec, task_id, agent.name, grant.name)
        text = f"Approve new item in {grant.name}: {spec.payload['title']}"
        return await self._post_once(ctx.tenant_id, key, dm, text, None, blocks)

    @activity.defn(name="update_card")
    async def update_card(self, spec: TaskSpec, task_id: str, card_ts: str, text: str) -> None:
        ctx = spec.ctx
        self._log("update_card", ctx.tenant_id, ctx.agent_id)
        record = await self.idem.get(ctx.tenant_id, f"{task_id}:card")
        channel = record["channel"] if record else await self.slack.open_dm(ctx.requester_user_id)
        line = f"{text}: {spec.payload['title']}"
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": line}}]
        await self.slack.update_message(channel, card_ts, line, blocks=blocks)

    @activity.defn(name="create_notion_item")
    async def create_notion_item(self, spec: TaskSpec, task_id: str) -> NotionItem:
        ctx = spec.ctx
        self._log("create_notion_item", ctx.tenant_id, ctx.agent_id)
        connector = self._connector(ctx.agent_id)
        email = await self.slack.user_email(ctx.requester_user_id)
        try:
            item = await connector.create_item(
                database_id=spec.payload["database_id"],
                title=spec.payload["title"],
                details=spec.payload["details"],
                requester_email=email,
                idem_key=f"{task_id}:create",
            )
        except ConnectorError as err:
            raise _non_retryable(err) from err
        except Exception as err:
            if _is_client_error(err):
                raise _non_retryable(err) from err
            raise
        await self.links.link(ctx.tenant_id, normalize_id(item.page_id), task_id)
        return item

    @activity.defn(name="get_notion_item")
    async def get_notion_item(self, spec: TaskSpec, page_id: str) -> NotionItem:
        ctx = spec.ctx
        self._log("get_notion_item", ctx.tenant_id, ctx.agent_id)
        try:
            return await self._connector(ctx.agent_id).get_item(page_id)
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


def _format_items(items: list[NotionItem]) -> str:
    if not items:
        return "No matching items."
    lines = [
        f"- {i.title} (status: {i.status or 'none'}) {i.url}" for i in items[:MAX_LISTED_ITEMS]
    ]
    if len(items) > MAX_LISTED_ITEMS:
        lines.append(f"... and {len(items) - MAX_LISTED_ITEMS} more")
    return "\n".join(lines)


def approval_card_blocks(spec: TaskSpec, task_id: str, agent_name: str, db_name: str) -> list:
    """The approval card: the frozen payload, who it runs as, Submit / Cancel.

    The button value carries the task workflow ID and the approval nonce. The
    task workflow accepts the click only if the nonce matches and the clicker
    is the requester.
    """
    value = json.dumps({"task_id": task_id, "nonce": spec.approval_nonce})
    payload = spec.payload
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{agent_name}* wants to add an item to *{db_name}* in Notion.",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Title*\n{payload['title']}"},
                {"type": "mrkdwn", "text": f"*Details*\n{payload['details']}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Will run as the {agent_name} agent. Nothing is filed until you "
                    "click Submit.",
                }
            ],
        },
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
                    "action_id": "task_cancel",
                    "text": {"type": "plain_text", "text": "Cancel"},
                    "value": value,
                },
            ],
        },
    ]
