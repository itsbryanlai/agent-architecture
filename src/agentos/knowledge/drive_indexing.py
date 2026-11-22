"""Activities that index a granted Drive folder or file into the knowledge store.

Rules kept here (the Drive shape of ``agentos.knowledge.indexing``):

* File text is exported, chunked, embedded and stored inside one activity. Temporal
  workflow history sees only file IDs and counts, never text.
* Every activity works for exactly one tenant, named by its ``tenant_id`` argument, and
  reads with that tenant's own Drive account (the address its files are shared with),
  only under a folder, file or shared drive some published agent of the tenant was granted.
* A node ends a sync ``ok`` (stored or unchanged), ``gone`` (trashed, 404, or a 403 whose
  reason is lost sharing — Drive prunes a file it can no longer read, unlike Notion which
  keeps a 403), or ``failed`` (a lasting refusal: a disabled-API 403, a 400, or a transient
  error whose retries ran out). A failed node keeps what is stored for it and the run goes
  on; one bad file never fails the run, whatever step of its pipeline refused. A rate
  limit (a 429 or a quota 403), timeout or server error is retried by Temporal with the
  wait Google asked for; a failure that lasts to the last attempt is counted, never silent.
* A file whose Drive ``version`` is unchanged is not exported again.
* A folder lists at most ``MAX_CHILDREN_PER_NODE`` children; a longer listing is truncated
  and logged, and the walk counts itself incomplete.
* While an admin has Drive turned off for the tenant nothing is read from Drive: a sync or
  prune step of a full run fails for good, so the run ends without pruning anything.
* While the tenant's monthly spend is at or over its budget no embedding call is made; the
  file is counted ``deferred`` and kept, and a later run indexes it once the budget allows.
* Only a complete walk prunes: a failed folder, a truncated listing or the discovery cap
  leaves the stored rows of everything the walk could not see untouched.
* A scope whose root is gone or unreadable (any failure, not only a gone one) is not pruned
  on that evidence alone: see ``prune_drive_scope``, which reuses Notion's repeated-run
  guard. The walk's outcome is recorded on the connection: ``error`` with a code when any
  node failed, ``connected`` otherwise.
* Log lines carry the tenant, scope and file ID only.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from temporalio import activity

from agentos import ids
from agentos.activities import _activity_ref, _is_client_error, _non_retryable, _reraise
from agentos.agent_store import AgentStore, with_published_agents
from agentos.audience_watch import _is_user_id
from agentos.budgets import TENANT, BudgetGuard
from agentos.config import DriveGrant
from agentos.connectors import drive_errors
from agentos.connectors.drive import DriveReader
from agentos.connectors.google_iam import (
    EXPORTABLE,
    MIME_FOLDER,
    DriveFile,
    GoogleAccounts,
    GoogleError,
)
from agentos.drive_connections import CONNECTED, ERROR, DriveConnectionStore
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.indexing import (
    FAILED,
    GONE,
    GONE_RUNS_BEFORE_PRUNE,
    OK,
    ScopeNotGranted,
    content_hash,
    parse_run_start,
)
from agentos.knowledge.notion_text import TextBlock, chunk
from agentos.knowledge.store import KnowledgeStore, PageSync
from agentos.slack_format import escape_slack
from agentos.stores import IdempotencyStore, InMemoryIdempotencyStore
from agentos.switches import (
    DRIVE,
    ConnectorOff,
    InMemorySwitchStore,
    SwitchStore,
    connector_off_text,
    read_state,
)
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant, granted_drive_roots
from agentos.usage import UsageRecorder, utcnow
from agentos.workflows import RETRY

log = logging.getLogger("agentos.drive_indexing")

# One activity's child listing is bounded so a huge folder cannot bloat the history; the
# walk's own discovery cap (workflows.drive_indexing) governs the whole scope. A truncated
# listing makes the walk incomplete: it prunes nothing that run.
MAX_CHILDREN_PER_NODE = 2000


@dataclass
class DriveSyncResult:
    """What syncing one node tells the workflow: a folder's children, or how a file fared.

    ``error_code`` is the short class of a gone or failed node's error (the connection row
    stores it); ``truncated`` says the folder listed more children than one activity carries.
    """

    child_ids: list[str] = field(default_factory=list)
    is_file: bool = False
    changed: bool = False
    chunks: int = 0
    budget_stopped: bool = False
    skipped_type: bool = False
    status: str = OK
    error_code: str | None = None
    truncated: bool = False


def _blocks(sections) -> list[TextBlock]:
    """An export's sections as heading/text blocks: a Sheet tab's name becomes a heading."""
    blocks: list[TextBlock] = []
    for section in sections:
        if section.heading:
            blocks.append(TextBlock("heading", section.heading))
        for line in section.text.splitlines():
            if line.strip():
                blocks.append(TextBlock("text", line))
    return blocks


class DriveIndexActivities:
    def __init__(
        self,
        registry: TenantRegistry,
        store: KnowledgeStore,
        embedder: Embedder,
        agents: AgentStore,
        google: GoogleAccounts | None = None,
        drive_connections: DriveConnectionStore | None = None,
        usage: UsageRecorder | None = None,
        switches: SwitchStore | None = None,
        budgets: BudgetGuard | None = None,
        idem: IdempotencyStore | None = None,
    ):
        self.registry = registry
        self.store = store
        self.embedder = embedder
        self.agents = agents
        self.google = google
        self.drive_connections = drive_connections
        self.usage = usage
        self.switches = switches or InMemorySwitchStore()
        self.budgets = budgets
        self.idem = idem or InMemoryIdempotencyStore()

    def all(self) -> list:
        return [self.sync_drive_nodes, self.prune_drive_scope]

    def _runtime(self, tenant_id: str) -> TenantRuntime:
        try:
            return self.registry.get(tenant_id)
        except UnknownTenant as err:
            raise _non_retryable(err) from err

    async def _drive_off(self, tenant_id: str) -> bool:
        return (await read_state(self.switches, tenant_id)).connector_off(DRIVE)

    async def _require_drive(self, tenant_id: str, scope: str) -> None:
        if await self._drive_off(tenant_id):
            log.info(
                "drive indexing stopped tenant_id=%s scope=%s connector=drive", tenant_id, scope
            )
            raise _non_retryable(ConnectorOff(connector_off_text(DRIVE)))

    async def _require_granted(
        self, tenant_id: str, scope: str
    ) -> tuple[TenantRuntime, DriveGrant]:
        runtime = self._runtime(tenant_id)
        roots = granted_drive_roots(await with_published_agents(self.agents, runtime.config))
        if scope not in roots:
            log.warning("drive scope not granted tenant_id=%s scope=%s", tenant_id, scope)
            raise _non_retryable(ScopeNotGranted(f"scope {scope} is no longer granted"))
        return runtime, roots[scope]

    async def _reader(self, tenant_id: str) -> DriveReader:
        """The tenant's Drive reader, from its connection row. A tenant with no connection
        or a deployment with no Google client cannot read Drive: a non-retryable refusal."""
        if self.google is None or self.drive_connections is None:
            raise _non_retryable(ConnectorOff("Google Drive is not set up on this deployment."))
        row = await self.drive_connections.get(tenant_id)
        if row is None:
            raise _non_retryable(
                ConnectorOff("Google Drive is not connected for this organisation.")
            )
        return DriveReader(self.google, row.service_account_email)

    @activity.defn(name="sync_drive_nodes")
    async def sync_drive_nodes(
        self, tenant_id: str, scope: str, node_ids: list[str]
    ) -> list[DriveSyncResult]:
        """Sync a batch of nodes of one scope: a folder lists its children, a file is
        exported and stored when its version changed. The grant and switch are checked once
        for the batch; a transient error fails the activity after the others finish."""
        await self._require_drive(tenant_id, scope)
        _, grant = await self._require_granted(tenant_id, scope)
        reader = await self._reader(tenant_id)
        outcomes = await asyncio.gather(
            *[self._sync_node(reader, tenant_id, scope, node_id, grant) for node_id in node_ids],
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        return list(outcomes)

    async def _sync_node(
        self, reader: DriveReader, tenant_id: str, scope: str, node_id: str, grant: DriveGrant
    ) -> DriveSyncResult:
        if node_id == grant.id and grant.kind == "shared_drive":
            # A shared drive's root is read drive-scoped: drives.get, then a corpora list.
            try:
                await reader.get_drive(node_id)
                children = await reader.list_drive_children(node_id)
            except Exception as err:  # noqa: BLE001 - classified, then re-raised
                return self._classify(tenant_id, scope, node_id, err, is_file=False)
            return self._folder_result(tenant_id, scope, node_id, children)
        try:
            file = await reader.get_file(node_id)
        except Exception as err:  # noqa: BLE001 - classified, then re-raised
            return self._classify(tenant_id, scope, node_id, err, is_file=False)
        if file.trashed:
            log.info("drive file gone tenant_id=%s scope=%s file_id=%s", tenant_id, scope, node_id)
            return DriveSyncResult(status=GONE, error_code="not_found")
        if file.mime_type == MIME_FOLDER:
            try:
                children = await reader.list_children(node_id)
            except Exception as err:  # noqa: BLE001
                return self._classify(tenant_id, scope, node_id, err, is_file=False)
            return self._folder_result(tenant_id, scope, node_id, children)
        return await self._sync_file(reader, tenant_id, scope, file)

    def _folder_result(
        self, tenant_id: str, scope: str, node_id: str, children: list[DriveFile]
    ) -> DriveSyncResult:
        """A folder's children, bounded: a longer listing is truncated and logged, and the
        walk counts itself incomplete rather than pruning what it never saw."""
        if len(children) > MAX_CHILDREN_PER_NODE:
            log.warning(
                "drive children truncated tenant_id=%s scope=%s file_id=%s children=%d cap=%d",
                tenant_id,
                scope,
                node_id,
                len(children),
                MAX_CHILDREN_PER_NODE,
            )
            return DriveSyncResult(
                child_ids=[c.id for c in children[:MAX_CHILDREN_PER_NODE]], truncated=True
            )
        return DriveSyncResult(child_ids=[c.id for c in children])

    def _classify(
        self, tenant_id: str, scope: str, node_id: str, err: Exception, is_file: bool
    ) -> DriveSyncResult:
        """A gone node leaves the index; a lasting refusal is counted; a transient one is
        retried, and on the last attempt counted rather than failing the whole run."""
        code = err.code if isinstance(err, GoogleError) else type(err).__name__
        if drive_errors.is_gone(err):
            log.info("drive node gone tenant_id=%s scope=%s file_id=%s", tenant_id, scope, node_id)
            return DriveSyncResult(is_file=is_file, status=GONE, error_code=code)
        if _is_client_error(err) or self._last_attempt():
            log.warning(
                "drive node failed tenant_id=%s scope=%s file_id=%s error_type=%s",
                tenant_id,
                scope,
                node_id,
                type(err).__name__,
            )
            return DriveSyncResult(is_file=is_file, status=FAILED, error_code=code)
        _reraise(err)

    @staticmethod
    def _last_attempt() -> bool:
        try:
            info = activity.info()
        except RuntimeError:
            return False
        return info.attempt >= RETRY.maximum_attempts

    async def _sync_file(
        self, reader: DriveReader, tenant_id: str, scope: str, file: DriveFile
    ) -> DriveSyncResult:
        if file.mime_type not in EXPORTABLE:
            return DriveSyncResult(is_file=True, skipped_type=True)
        try:
            return await self._export_and_store(reader, tenant_id, scope, file)
        except Exception as err:  # noqa: BLE001 - classified, then re-raised
            # The export, the embedder, the store or the budget refused for good: retrying
            # sends the same request and fails the same way, and one file must not stop the
            # run. What is stored for it stays.
            return self._classify(tenant_id, scope, file.id, err, is_file=True)

    async def _export_and_store(
        self, reader: DriveReader, tenant_id: str, scope: str, file: DriveFile
    ) -> DriveSyncResult:
        model = self.embedder.model
        state = await self.store.page_state(tenant_id, scope, file.id)
        if (
            state is not None
            and state.sync.last_edited_time == file.version
            and state.sync.embed_model == model
        ):
            return DriveSyncResult(is_file=True)
        sections = await reader.export_file(file.id, file.mime_type)
        blocks = _blocks(sections)
        chunks = chunk(file.name, blocks)
        digest = content_hash(model, file.name, chunks)
        if state is not None and state.content_hash == digest:
            await self.store.touch_page(tenant_id, scope, file.id, digest, self._sync(model, file))
            return DriveSyncResult(is_file=True, chunks=len(chunks))
        if chunks and self.budgets is not None and await self.budgets.tenant_stopped(tenant_id):
            log.info(
                "drive indexing stopped tenant_id=%s scope=%s budget=%s", tenant_id, scope, TENANT
            )
            return DriveSyncResult(is_file=True, budget_stopped=True, chunks=len(chunks))
        vectors = await self.embedder.embed(chunks) if chunks else []
        if chunks and self.usage is not None:
            await self.usage.embedding(
                tenant_id=tenant_id,
                key=ids.usage_embedding_key(
                    tenant_id, *_activity_ref(f"{tenant_id}:drive-index"), scope, file.id
                ),
                model=model,
                billed=getattr(vectors, "billed", None),
            )
        await self.store.replace_page(
            tenant_id,
            scope,
            file.id,
            file.name,
            file.web_view_link,
            digest,
            list(zip(chunks, vectors)),
            self._sync(model, file),
        )
        log.info(
            "drive file indexed tenant_id=%s scope=%s file_id=%s chunks=%d",
            tenant_id,
            scope,
            file.id,
            len(chunks),
        )
        return DriveSyncResult(is_file=True, changed=True, chunks=len(chunks))

    @staticmethod
    def _sync(model: str, file: DriveFile) -> PageSync:
        # The Drive version rides in last_edited_time; the Notion-only fields stay empty.
        return PageSync(embed_model=model, last_edited_time=file.version)

    @activity.defn(name="prune_drive_scope")
    async def prune_drive_scope(
        self,
        tenant_id: str,
        scope: str,
        seen_file_ids: list[str],
        run_started: str | None = None,
        root_gone: bool = False,
        confirm_gone: bool = False,
        walk_complete: bool = True,
        error_code: str | None = None,
    ) -> int:
        """Delete stored files of the scope the walk did not see, and record the walk's
        outcome on the Drive connection. Only a complete walk prunes: a failed folder or a
        truncated listing leaves every stored row untouched. A root that is gone or
        unreadable is pruned only after repeated runs (Notion's guard), never on one run's
        evidence. ``error_code`` is the first failed node's short class, recorded with the
        connection's ``error`` status; a walk with no failures records ``connected``."""
        await self._require_drive(tenant_id, scope)
        runtime, grant = await self._require_granted(tenant_id, scope)
        before = parse_run_start(run_started)
        seen = set(seen_file_ids)
        if root_gone:
            health = await self.store.record_scope_gone(tenant_id, scope, run_started or "")
            if health.notified_at is None:
                await self._notify_owner(runtime, tenant_id, scope, grant, health.first_seen)
                await self.store.mark_scope_notified(tenant_id, scope)
            log.warning(
                "drive scope root gone tenant_id=%s scope=%s runs=%d",
                tenant_id,
                scope,
                health.consecutive_gone,
            )
            await self._record_check(tenant_id, ERROR, error_code or "not_found")
            if not confirm_gone and health.consecutive_gone < GONE_RUNS_BEFORE_PRUNE:
                return 0
            seen = set()
        else:
            await self.store.clear_scope_health(tenant_id, scope)
            if error_code is not None:
                await self._record_check(tenant_id, ERROR, error_code)
            else:
                await self._record_check(tenant_id, CONNECTED, None)
            if not walk_complete:
                log.warning(
                    "drive walk incomplete tenant_id=%s scope=%s pruned=0", tenant_id, scope
                )
                return 0
        stored = await self.store.page_hashes(tenant_id, scope)
        gone = [page_id for page_id in stored if page_id not in seen]
        deleted = await self.store.delete_pages(tenant_id, scope, gone, indexed_before=before)
        if root_gone:
            await self.store.clear_scope_health(tenant_id, scope)
        log.info("drive scope pruned tenant_id=%s scope=%s files=%d", tenant_id, scope, deleted)
        return deleted

    async def _record_check(self, tenant_id: str, status: str, last_error: str | None) -> None:
        if self.drive_connections is not None:
            await self.drive_connections.record_check(tenant_id, status, last_error, utcnow())

    async def _notify_owner(
        self, runtime: TenantRuntime, tenant_id: str, scope: str, grant: DriveGrant, first_seen
    ) -> None:
        """Tell the owner of each agent granted the scope (the tenant's admins when none can
        be messaged) that its Drive root can no longer be read. Once per person. The text
        names the root's label and ID, never file content."""
        published = await self.agents.published_all(tenant_id)
        granting = [
            agent
            for agent in published.values()
            if any(ids.drive_scope(g.id) == scope for g in agent.grants.drive)
        ]
        people: list[str] = []
        for agent in granting:
            record = await self.agents.get_agent(tenant_id, agent.agent_id)
            if record is not None:
                people.append(record.created_by)
        people = [p for p in people if _is_user_id(p)] or [
            p for p in runtime.config.admins if _is_user_id(p)
        ]
        names = ", ".join(sorted(escape_slack(agent.name) for agent in granting))
        text = (
            f"The Google Drive folder or file '{escape_slack(grant.name)}' ({grant.id}) that "
            f"{names} reads from can no longer be reached: it was deleted, trashed or unshared "
            "from the workspace's Drive address, or Google is refusing to read it. Its indexed "
            f"files are kept for now. If it is still unreadable after {GONE_RUNS_BEFORE_PRUNE} "
            "nightly indexing runs in a row, they are deleted; share it with the address again "
            "to keep them."
        )
        slack = runtime.slack
        for person in dict.fromkeys(people):
            notice = ids.scope_gone_notice_key(tenant_id, scope, person, first_seen.isoformat())
            if await self.idem.get(tenant_id, notice) is not None:
                continue
            channel = await slack.open_dm(person)
            ts = await slack.post_message(channel, text)
            await self.idem.put_if_absent(tenant_id, notice, {"ts": ts})
