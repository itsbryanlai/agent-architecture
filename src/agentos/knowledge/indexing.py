"""Activities that index a granted page tree into the knowledge store.

Rules kept here:

* Page text is fetched, chunked, embedded and stored inside one activity per
  page. Temporal workflow history sees only page IDs and counts, never text.
* Every activity works for exactly one tenant, named by its ``tenant_id``
  argument. It reads pages with that tenant's own Notion client and only under
  the root pages that tenant granted. An unknown tenant is a non-retryable
  failure, never a fallback to another tenant.
* The agent uses its own access: pages are read with the same Notion token the
  agents use, and only under a root page an agent was granted.
* A page ends a sync in one of three states. ``ok``: read and stored (or unchanged).
  ``gone``: trashed or a 404, so it and what hung beneath it leave the index. ``failed``:
  anything else Notion or the store refused for good (403 included). A failed page keeps
  what is stored for it and for everything beneath it, and the run goes on. Only a 429, a
  timeout or a server error is retried, and then the activity fails.
* A scope whose root page is gone is never pruned on that evidence alone: see
  ``prune_scope``.
* A page whose ``last_edited_time`` has not changed since it was stored is not read again;
  its stored sub-page IDs are used to go on down the tree. Database rows and the pages of
  child databases are found by querying the database, which a page edit does not reflect.
* While an admin has Notion turned off for the tenant (see ``agentos.switches``) nothing
  is read from Notion: a page event is dropped (the nightly run catches up), and a page
  or prune step of a full run fails for good, so the run ends without pruning anything.
  The switch is read afresh at every step.
* While the tenant's monthly spend is at or over its tenant-wide budget (see
  ``agentos.budgets``) no embedding call is made. The page is neither stored nor skipped: its
  sub-page IDs still go back so the run keeps every page it saw, and nothing is pruned
  because of the stop. Indexing resumes by itself when the budget is raised or removed or
  the UTC month turns. Agent budgets never stop indexing.
* Log lines carry the tenant, scope and page ID only.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from temporalio import activity

from agentos import ids
from agentos.activities import _activity_ref, _is_client_error, _non_retryable
from agentos.agent_store import AgentStore, with_published_agents
from agentos.audience_watch import _is_user_id
from agentos.budgets import TENANT, BudgetGuard
from agentos.config import EMBED_DIMENSIONS
from agentos.connectors.notion import NotionAPI, normalize_id
from agentos.knowledge.embedder import Embedder
from agentos.knowledge.notion_text import (
    PageUnavailable,
    chunk,
    fetch_page,
    parent_ref,
    read_page,
)
from agentos.knowledge.search import PageChecker
from agentos.knowledge.store import KnowledgeStore, PageState, PageSync
from agentos.slack_format import escape_slack
from agentos.stores import IdempotencyStore, InMemoryIdempotencyStore
from agentos.switches import (
    NOTION,
    ConnectorOff,
    InMemorySwitchStore,
    SwitchStore,
    connector_off_text,
    read_state,
)
from agentos.tenants import TenantRegistry, TenantRuntime, UnknownTenant, granted_roots
from agentos.usage import UsageRecorder

OK = "ok"
GONE = "gone"
FAILED = "failed"

# Consecutive runs that must find a scope's root page gone before its index is deleted.
GONE_RUNS_BEFORE_PRUNE = 3

# Notion rounds ``last_edited_time`` down to the minute. A page edited within this long before
# it was stored may have changed again inside that minute, so it is read once more.
EDIT_GRACE = timedelta(minutes=1)

log = logging.getLogger("agentos.indexing")


class ScopeNotGranted(Exception):
    """No published agent of the tenant has the scope granted any more."""


@dataclass
class SyncResult:
    """What syncing one page tells the workflow: where to go on, and how the page fared.

    ``status`` is ``ok``, ``gone`` or ``failed`` (see the module docstring). A failed page
    still returns the sub-pages stored for it, so the walk goes on beneath it and keeps them.
    """

    child_page_ids: list[str] = field(default_factory=list)
    changed: bool = False
    chunks: int = 0
    # True when the page is gone. The same fact as ``status == "gone"``, kept so a result
    # recorded before ``status`` existed still reads right.
    skipped: bool = False
    budget_stopped: bool = False  # changed, but not embedded: the tenant budget is reached
    status: str = OK

    @property
    def gone(self) -> bool:
        return self.skipped or self.status == GONE


@dataclass
class RefreshResult:
    """Scopes only, never page text: indexed scopes and scopes the page left."""

    indexed: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)


def content_hash(model: str, title: str, chunks: list[str]) -> str:
    """Hash of everything that decides what is stored for a page.

    The embedding model and its dimensions are part of it, so changing the
    model re-indexes every page.
    """
    h = hashlib.sha256()
    for part in (model, str(EMBED_DIMENSIONS), title, "\x00".join(chunks)):
        h.update(part.encode())
        h.update(b"\x01")
    return h.hexdigest()


def _unchanged(state: PageState, edited: str, model: str) -> bool:
    """True when the stored page is current: same edit time and model, stored after the edit.

    Notion rounds the edit time to the minute, so a page stored less than ``EDIT_GRACE``
    after its last edit is read again.
    """
    if not edited or state.sync.last_edited_time != edited or state.sync.embed_model != model:
        return False
    try:
        edited_at = datetime.fromisoformat(edited)
    except ValueError:
        return False
    return state.indexed_at >= edited_at + EDIT_GRACE


def _union(*lists: list[str]) -> list[str]:
    return list(dict.fromkeys(item for items in lists for item in items))


def parse_run_start(run_started: str | None) -> datetime | None:
    return datetime.fromisoformat(run_started) if run_started else None


class IndexActivities:
    def __init__(
        self,
        registry: TenantRegistry,
        store: KnowledgeStore,
        embedder: Embedder,
        agents: AgentStore,
        usage: UsageRecorder | None = None,
        switches: SwitchStore | None = None,
        budgets: BudgetGuard | None = None,
        idem: IdempotencyStore | None = None,
    ):
        self.budgets = budgets
        self.switches = switches or InMemorySwitchStore()
        self.idem = idem or InMemoryIdempotencyStore()
        self.usage = usage
        self.registry = registry
        self.agents = agents
        self.store = store
        self.embedder = embedder

    def _runtime(self, tenant_id: str) -> TenantRuntime:
        """The config and Notion client of the tenant an activity works for."""
        try:
            return self.registry.get(tenant_id)
        except UnknownTenant as err:
            raise _non_retryable(err) from err

    async def _notion_off(self, tenant_id: str) -> bool:
        return (await read_state(self.switches, tenant_id)).connector_off(NOTION)

    async def _require_notion(self, tenant_id: str, scope: str) -> None:
        """Fail a step of a full run for good while Notion is turned off for the tenant."""
        if await self._notion_off(tenant_id):
            log.info("indexing stopped tenant_id=%s scope=%s connector=notion", tenant_id, scope)
            raise _non_retryable(ConnectorOff(connector_off_text("notion")))

    async def _granted(self, runtime: TenantRuntime) -> dict[str, str]:
        """Scope to root page for the pages granted by the tenant's published agents."""
        return granted_roots(await with_published_agents(self.agents, runtime.config))

    async def _require_granted(self, tenant_id: str, scope: str) -> tuple[TenantRuntime, str]:
        """The tenant's runtime and the scope's root page, only while some published agent of
        the tenant has the scope granted.

        A scope is derived from its root page ID, so a scope still granted
        always has the root it was started with. A schedule left over from a
        removed grant fails here, before any Notion or embedding call. The grants are read
        once per call.
        """
        runtime = self._runtime(tenant_id)
        roots = await self._granted(runtime)
        if scope not in roots:
            log.warning("scope not granted tenant_id=%s scope=%s", tenant_id, scope)
            raise _non_retryable(ScopeNotGranted(f"scope {scope} is no longer granted"))
        return runtime, roots[scope]

    def all(self) -> list:
        return [self.sync_page, self.sync_pages, self.prune_scope, self.refresh_page]

    @activity.defn(name="sync_page")
    async def sync_page(
        self, tenant_id: str, scope: str, page_id: str, parent_id: str | None = None
    ) -> SyncResult:
        """Read one page and, when its content changed, embed and store it.

        ``parent_id`` is the page the walk found this one under; it gives the page its
        ancestor chain. See ``sync_pages``, which a run uses for a batch of pages.
        """
        await self._require_notion(tenant_id, scope)
        runtime, root = await self._require_granted(tenant_id, scope)
        return await self._sync_one(
            runtime, tenant_id, scope, root, normalize_id(page_id), parent_id, {}
        )

    @activity.defn(name="sync_pages")
    async def sync_pages(
        self, tenant_id: str, scope: str, page_ids: list[str], parent_ids: list[str]
    ) -> list[SyncResult]:
        """Sync a batch of pages of one scope, checking the grant once for the batch.

        ``parent_ids`` is parallel to ``page_ids`` ("" for the root). A page that fails for
        good is reported as ``failed`` and does not stop the others; a transient error
        (a rate limit, a server error) fails the activity after the other pages finished,
        and a retry skips what is already stored.
        """
        await self._require_notion(tenant_id, scope)
        runtime, root = await self._require_granted(tenant_id, scope)
        chains: dict[str, list[str]] = {}
        outcomes = await asyncio.gather(
            *[
                self._sync_one(
                    runtime,
                    tenant_id,
                    scope,
                    root,
                    normalize_id(page_id),
                    parent_id or None,
                    chains,
                )
                for page_id, parent_id in zip(page_ids, parent_ids)
            ],
            return_exceptions=True,
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        return list(outcomes)

    async def _sync_one(
        self,
        runtime: TenantRuntime,
        tenant_id: str,
        scope: str,
        root: str,
        page_id: str,
        parent_id: str | None,
        chains: dict[str, list[str]],
        ancestors: list[str] | None = None,
    ) -> SyncResult:
        """Sync one page and say which of the three states it ended in."""
        try:
            page = await fetch_page(runtime.notion_api, page_id)
        except PageUnavailable:
            log.info("page gone tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
            return SyncResult(skipped=True, status=GONE)
        except Exception as err:
            if getattr(err, "status", None) == 404:
                log.info("page gone tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
                return SyncResult(skipped=True, status=GONE)
            if _is_client_error(err):
                return await self._failed(tenant_id, scope, page_id, err)
            raise
        state = await self.store.page_state(tenant_id, scope, page_id)
        try:
            if ancestors is None:
                ancestors = await self._ancestors(
                    tenant_id, scope, root, page_id, parent_id, chains
                )
            return await self._index(runtime, tenant_id, scope, page_id, page, state, ancestors)
        except Exception as err:
            # Notion, the embedder or the store refused the request for good: retrying sends
            # the same request and fails the same way, and one page must not stop the run.
            if _is_client_error(err):
                return await self._failed(tenant_id, scope, page_id, err, state)
            raise

    async def _failed(
        self,
        tenant_id: str,
        scope: str,
        page_id: str,
        err: Exception,
        state: PageState | None = None,
    ) -> SyncResult:
        """A page Notion or the store refused for good. What is stored for it stays, and the
        sub-pages stored for it go back, so the walk keeps everything beneath it."""
        log.warning(
            "page failed tenant_id=%s scope=%s page_id=%s error_type=%s",
            tenant_id,
            scope,
            page_id,
            type(err).__name__,
        )
        state = state or await self.store.page_state(tenant_id, scope, page_id)
        children = _union(state.sync.child_page_ids, state.sync.row_page_ids) if state else []
        return SyncResult(children, status=FAILED)

    async def _ancestors(
        self,
        tenant_id: str,
        scope: str,
        root: str,
        page_id: str,
        parent_id: str | None,
        chains: dict[str, list[str]],
    ) -> list[str]:
        """Page IDs from the root down to the page's parent, from the parent's stored chain.

        A parent that is not stored (the budget stopped it) or was stored before chains were
        kept gives the shorter chain root, parent. With no parent given the chain is empty,
        and a search then walks up from the page instead.
        """
        if page_id == root or not parent_id:
            return []
        parent_id = normalize_id(parent_id)
        if parent_id not in chains:
            if parent_id == root:
                chains[parent_id] = [root]
            else:
                parent = await self.store.page_state(tenant_id, scope, parent_id)
                above = parent.sync.ancestors if parent and parent.sync.ancestors else [root]
                chains[parent_id] = [*above, parent_id]
        return list(chains[parent_id])

    async def _rows(self, api: NotionAPI, database_ids: list[str]) -> list[str]:
        """IDs of the rows of child databases. A row is indexed as a page of the same scope."""
        rows: list[str] = []
        for database_id in database_ids:
            rows += [normalize_id(row["id"]) for row in await api.query_database(database_id, None)]
        return _union(rows)

    @activity.defn(name="refresh_page")
    async def refresh_page(self, tenant_id: str, page_id: str) -> RefreshResult:
        """Bring one page up to date in every granted scope, after a Notion event.

        For each granted root: a page that is live and still under the root is
        re-read and re-embedded if its content changed (this also indexes a
        newly created page, or one moved in from outside); a page that is no
        longer under the root, or is trashed, is removed from that scope (one Notion answers 403
        for is kept).
        Only this one page is synced: sub-pages of a newly created or moved-in
        page wait for the next full indexing run. Notion lookups are cached
        for the call by one page checker, and the grants are read once. The result and the
        logs hold scope and page IDs only, never page text.
        """
        page_id = normalize_id(page_id)
        runtime = self._runtime(tenant_id)
        result = RefreshResult()
        if await self._notion_off(tenant_id):
            log.info("page refresh skipped tenant_id=%s connector=notion", tenant_id)
            return result
        checker = PageChecker(runtime.notion_api)
        for scope, root in (await self._granted(runtime)).items():
            chain = await checker.ancestry(page_id, root)
            if chain is not None:
                synced = await self._sync_one(
                    runtime, tenant_id, scope, root, page_id, None, {}, ancestors=chain
                )
                if synced.status == OK and not synced.budget_stopped:
                    result.indexed.append(scope)
            elif checker.forbidden:
                # Notion refused to show the page or a parent (403): it may still be there,
                # so what is stored stays, as in a full run. Only a 404, a trashed page or a
                # page moved out of the root removes it.
                log.info("page kept tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
            elif await self.store.page_state(tenant_id, scope, page_id) is not None:
                await self.store.delete_pages(tenant_id, scope, [page_id])
                result.removed.append(scope)
                log.info("page removed tenant_id=%s scope=%s page_id=%s", tenant_id, scope, page_id)
        return result

    async def _index(
        self,
        runtime: TenantRuntime,
        tenant_id: str,
        scope: str,
        page_id: str,
        page: dict,
        state: PageState | None,
        ancestors: list[str],
    ) -> SyncResult:
        api = runtime.notion_api
        model = self.embedder.model
        edited = page.get("last_edited_time", "")
        if state is not None and _unchanged(state, edited, model):
            # Nothing on the page changed: no block fetch. Rows of its child databases can
            # change without the page changing, so they are asked for again.
            rows = await self._rows(api, state.sync.child_database_ids)
            sync = PageSync(
                model,
                edited,
                parent_ref(page),
                ancestors,
                state.sync.child_page_ids,
                state.sync.child_database_ids,
                rows,
            )
            if sync != state.sync:
                await self.store.touch_page(tenant_id, scope, page_id, state.content_hash, sync)
            return SyncResult(_union(sync.child_page_ids, rows))

        content = await read_page(api, page_id, page)
        rows = await self._rows(api, content.child_database_ids)
        children = _union(content.child_page_ids, rows)
        sync = PageSync(
            model,
            edited,
            content.parent_ref,
            ancestors,
            content.child_page_ids,
            content.child_database_ids,
            rows,
        )
        chunks = chunk(content.title, content.blocks)
        digest = content_hash(model, content.title, chunks)
        if state is not None and state.content_hash == digest:
            await self.store.touch_page(tenant_id, scope, page_id, digest, sync)
            return SyncResult(children, changed=False, chunks=len(chunks))
        if chunks and self.budgets is not None and await self.budgets.tenant_stopped(tenant_id):
            log.info("indexing stopped tenant_id=%s scope=%s budget=%s", tenant_id, scope, TENANT)
            return SyncResult(children, budget_stopped=True, chunks=len(chunks))
        vectors = await self.embedder.embed(chunks) if chunks else []
        if chunks and self.usage is not None:
            await self.usage.embedding(
                tenant_id=tenant_id,
                key=ids.usage_embedding_key(
                    tenant_id, *_activity_ref(f"{tenant_id}:index"), scope, page_id
                ),
                model=model,
                billed=getattr(vectors, "billed", None),
            )
        await self.store.replace_page(
            tenant_id,
            scope,
            page_id,
            content.title,
            content.url,
            digest,
            list(zip(chunks, vectors)),
            sync,
        )
        log.info(
            "page indexed tenant_id=%s scope=%s page_id=%s chunks=%d",
            tenant_id,
            scope,
            page_id,
            len(chunks),
        )
        return SyncResult(children, changed=True, chunks=len(chunks))

    @activity.defn(name="prune_scope")
    async def prune_scope(
        self,
        tenant_id: str,
        scope: str,
        seen_page_ids: list[str],
        run_started: str | None = None,
        root_gone: bool = False,
        confirm_gone: bool = False,
    ) -> int:
        """Delete stored pages of the scope that the run did not see.

        ``seen_page_ids`` are the pages the run kept (every page it reached that is not
        gone). A page stored since ``run_started`` (a page event indexed it while the run was
        going) is never deleted by this run.

        A run that found the scope's root page gone (``root_gone``) deletes nothing at first:
        a page unshared for a minute must not cost the whole index. The scope is recorded as
        unhealthy, the agent's owner is told once, and the scope's pages are deleted only when
        ``GONE_RUNS_BEFORE_PRUNE`` runs in a row found the root gone, or when an admin ran the
        index command with ``confirm_gone``. A run that finds the root again clears the record.
        """
        await self._require_notion(tenant_id, scope)
        runtime, root = await self._require_granted(tenant_id, scope)
        before = parse_run_start(run_started)
        seen = set(seen_page_ids)
        if root_gone:
            health = await self.store.record_scope_gone(tenant_id, scope, run_started or "")
            if health.notified_at is None:
                await self._notify_owner(runtime, tenant_id, scope, root, health.first_seen)
                await self.store.mark_scope_notified(tenant_id, scope)
            log.warning(
                "scope root gone tenant_id=%s scope=%s runs=%d",
                tenant_id,
                scope,
                health.consecutive_gone,
            )
            if not confirm_gone and health.consecutive_gone < GONE_RUNS_BEFORE_PRUNE:
                return 0
            seen = set()
        else:
            await self.store.clear_scope_health(tenant_id, scope)
        stored = await self.store.page_hashes(tenant_id, scope)
        gone = [page_id for page_id in stored if page_id not in seen]
        deleted = await self.store.delete_pages(tenant_id, scope, gone, indexed_before=before)
        if root_gone:
            await self.store.clear_scope_health(tenant_id, scope)
        log.info("scope pruned tenant_id=%s scope=%s pages=%d", tenant_id, scope, deleted)
        return deleted

    async def _notify_owner(
        self, runtime: TenantRuntime, tenant_id: str, scope: str, root: str, first_seen: datetime
    ) -> None:
        """Tell the owner of each agent granted the scope (the tenant's admins when none can
        be messaged) that its root page is gone. Once per person: the message has a fixed
        idempotency key. The text names the agents and the page ID, never page content."""
        published = await self.agents.published_all(tenant_id)
        granting = [
            agent
            for agent in published.values()
            if any(ids.knowledge_scope(g.id) == scope for g in agent.grants.notion_pages)
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
            f"The Notion page {root} that {names} reads from can no longer be reached: it was "
            "deleted, trashed or unshared. Its indexed pages are kept for now. If the page is "
            f"still gone after {GONE_RUNS_BEFORE_PRUNE} nightly indexing runs in a row, they are "
            "deleted; share the page with the integration again to keep them."
        )
        slack = runtime.slack
        for person in dict.fromkeys(people):
            notice = ids.scope_gone_notice_key(tenant_id, scope, person, first_seen.isoformat())
            if await self.idem.get(tenant_id, notice) is not None:
                continue
            channel = await slack.open_dm(person)
            ts = await slack.post_message(channel, text)
            await self.idem.put_if_absent(tenant_id, notice, {"ts": ts})
