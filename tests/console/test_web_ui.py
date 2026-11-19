"""The web console pages: what each role sees, what a form may do, and what is escaped.

Everything is in memory: the stores, a fake Notion and a fake Slack sign-in.
"""

import html
import re
from urllib.parse import parse_qs, urlsplit

import pytest

from agentos.agent_health import AVAILABLE, UNAVAILABLE, AgentHealth
from agentos.config import parse_agent
from agentos.web_ui import SKELETON, definition_text, parse_definition
from tests.conftest import DB_ID, GLOBEX, TENANT, default_schema
from tests.helpers.agents import helper_block
from tests.helpers.console_harness import (
    ADMIN2,
    CONSOLE_URL,
    OTHER,
    OWNER,
    ROOT,
    ConsoleHarness,
    csrf_in,
    csrf_token,
    post,
    using,
)

UI = f"/ui/{TENANT}"
VERSION = f"{UI}/agents/it-helper/versions/1"


@pytest.fixture
async def h():
    harness = ConsoleHarness()
    await harness.seed_members()
    return harness


def kinds(h) -> list[str]:
    return [e.kind for e in h.audit.events if e.tenant_id == TENANT]


async def make_pending(h, user=OWNER):
    await h.plane.add_agent(TENANT, user, "it-helper", helper_block())
    await h.plane.submit_version(TENANT, user, "it-helper", 1)


async def shown(h, action: str) -> dict[str, str]:
    """The hidden field an admin's publish form carries: the hash of the version just shown."""
    if action != "publish":
        return {}
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    now = await h.agents.published(TENANT, "it-helper")
    return {
        "definition_sha256": record.definition_sha256,
        "widening": "yes",
        "reviewed_published": "none" if now is None else str(now.version),
    }


async def status_of_version(h, version=1) -> str:
    return (await h.agents.get_version(TENANT, "it-helper", version)).status


def strip(html: str) -> str:
    return re.sub(r"\s+", " ", html)


# --- every page renders for the roles that may see it -------------------------------


async def admin_page(h, path):
    await make_pending(h)
    c = await h.signed_in(ADMIN2)
    async with using(c):
        return await c.get(path)


async def test_the_agents_page_links_the_admin_pages_and_offers_a_new_agent(h):
    """Fails if an admin's agent list leaves out the agent, the members and audit links, or New agent, or the new-agent form carries no request token."""
    page = await admin_page(h, f"{UI}/agents")
    assert page.status_code == 200 and "it-helper" in page.text
    assert f'href="{UI}/members"' in page.text and f'href="{UI}/audit"' in page.text
    assert "New agent" in page.text
    assert 'name="request_id"' in page.text


@pytest.mark.parametrize("status", [UNAVAILABLE, AVAILABLE], ids=["unavailable", "available"])
async def test_the_agents_list_badges_an_unavailable_agent(h, status):
    """Fails if the agents list does not badge an unavailable agent, or badges an available one."""
    await make_pending(h)
    await h.health.record(AgentHealth(TENANT, "it-helper", 1, status, None, h.clock()))
    c = await h.signed_in(ROOT)
    async with using(c):
        page = (await c.get(f"{UI}/agents")).text
    assert ('<span class="status">unavailable</span>' in page) is (status == UNAVAILABLE)


async def test_the_agent_page_lists_its_pending_version_and_its_creator(h):
    """Fails if the agent page does not list version 1 as pending with its creator."""
    page = await admin_page(h, f"{UI}/agents/it-helper")
    assert page.status_code == 200 and ">1</a>" in page.text
    assert "pending" in page.text and OWNER in page.text


async def test_the_version_page_offers_an_admin_the_controls_of_a_pending_version(h):
    """Fails if a pending version's page lacks reject, publish, check-grants or withdraw, offers submit, hides the definition, or a mutating control carries no request token."""
    page = await admin_page(h, VERSION)
    assert page.status_code == 200
    for control in ("/reject", "/publish", "/check-grants", "/withdraw"):
        assert f'action="{VERSION}{control}"' in page.text, control
    for control in ("/reject", "/publish", "/withdraw"):
        form = re.search(rf'<form[^>]*action="{VERSION}{control}".*?</form>', page.text, re.DOTALL)
        assert form and 'name="request_id"' in form.group(0), control
    assert "/submit" not in page.text  # it is already pending
    assert "system_prompt" in page.text and "<pre>" in page.text


async def test_the_members_page_lists_members_and_their_source(h):
    """Fails if the members page does not show the tenant-file admin as such, the creator, or the add form, or a form carries no request token."""
    page = await admin_page(h, f"{UI}/members")
    assert page.status_code == 200
    assert ROOT in page.text and "set in the tenant file" in page.text
    assert OWNER in page.text and "Add a member" in page.text
    assert 'name="request_id"' in page.text


async def test_the_audit_page_lists_events_with_shortened_hashes(h):
    """Fails if the audit page leaves out an event kind, or shows a hash that is not a shortened one."""
    page = await admin_page(h, f"{UI}/audit")
    assert page.status_code == 200
    assert "agent_create" in page.text and "agent_submit" in page.text
    assert re.search(r"<code title=\"[0-9a-f]{64}\">[0-9a-f]{12}</code>", page.text)


async def test_a_creator_sees_their_own_agent_with_editing_controls_only(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    c = await h.signed_in(OWNER)
    async with using(c):
        agents = await c.get(f"{UI}/agents")
        assert agents.status_code == 200 and "it-helper" in agents.text
        assert f'href="{UI}/members"' not in agents.text
        assert f'href="{UI}/audit"' not in agents.text

        draft = await c.get(VERSION)
        assert f'action="{VERSION}/submit"' in draft.text
        assert f'action="{VERSION}/check-grants"' in draft.text
        assert "<textarea" in draft.text and f'action="{VERSION}/edit"' in draft.text
        for control in ("/submit", "/edit"):
            form = re.search(
                rf'<form[^>]*action="{VERSION}{control}".*?</form>', draft.text, re.DOTALL
            )
            assert form and 'name="request_id"' in form.group(0), control
        for admin_only in ("/publish", "/reject"):
            assert admin_only not in draft.text

        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        pending = await c.get(VERSION)
        assert f'action="{VERSION}/withdraw"' in pending.text
        for admin_only in ("/publish", "/reject", "/edit"):
            assert admin_only not in pending.text
        assert "<textarea" not in pending.text


async def test_a_creator_cannot_open_an_agent_another_creator_made_and_it_is_not_listed(h):
    """Fails if a creator sees another creator's agent in the list, or can open its pages
    or its definition, or if an admin cannot."""
    await make_pending(h, OWNER)
    c = await h.signed_in(OTHER)
    async with using(c):
        listing = await c.get(f"{UI}/agents")
        assert listing.status_code == 200 and "it-helper" not in listing.text
        for path in (VERSION, f"{UI}/agents/it-helper", f"{UI}/agents/nobody"):
            page = await c.get(path)
            assert page.status_code == 404, path
            assert "system_prompt" not in page.text
    admin = await h.signed_in(ROOT)
    async with using(admin):
        assert "system_prompt" in (await admin.get(VERSION)).text


async def test_before_sign_in_an_unknown_tenant_and_a_real_one_answer_alike(h):
    """Fails if the JSON API or a console page tells a caller with no session which tenant IDs
    exist, by status or by body."""
    async with h.client() as c:
        for path in ("/tenants/{}/agents", "/tenants/{}/me", "/ui/{}/agents"):
            real = await c.get(path.format(TENANT), follow_redirects=False)
            unknown = await c.get(path.format("no-such-tenant"), follow_redirects=False)
            assert real.status_code == unknown.status_code, path
            assert real.status_code in (303, 401)
            assert real.text == unknown.text, path
            assert real.headers.get("location") == unknown.headers.get("location")
        signed = await h.signed_in(ROOT)
    async with using(signed):
        other = await signed.get(f"/tenants/{GLOBEX}/agents")
        unknown = await signed.get("/tenants/no-such-tenant/agents")
        assert (other.status_code, other.text) == (unknown.status_code, unknown.text)


async def test_the_admin_pages_are_refused_to_a_creator_by_the_control_plane(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        for path in ("/members", "/audit", "/settings"):
            response = await c.get(UI + path)
            assert response.status_code == 403
            assert "may not do this" in response.text
            assert OTHER not in response.text  # no member data in the refusal


async def test_pages_need_a_session(h):
    await make_pending(h)
    async with h.client() as c:
        for path in (
            f"{UI}/agents",
            f"{UI}/agents/it-helper",
            VERSION,
            f"{UI}/members",
            f"{UI}/audit",
            f"{UI}/settings",
        ):
            response = await c.get(path)
            assert response.status_code == 303 and response.headers["location"] == "/login", path
        for path in (f"{VERSION}/publish", f"{UI}/settings/routing"):
            posted = await post(c, path, "anything")
            assert posted.status_code == 303 and posted.headers["location"] == "/login", path
    assert await status_of_version(h) == "pending"


async def test_a_user_who_lost_their_role_sees_no_access_with_no_data(h):
    await make_pending(h)
    c = await h.signed_in(OWNER)
    async with using(c):
        await h.members.remove_member(TENANT, OWNER)
        response = await c.get(f"{UI}/agents")
        assert response.status_code == 403
        assert "no role" in response.text and "it-helper" not in response.text


async def test_an_unknown_agent_or_version_is_a_404_page(h):
    await make_pending(h)
    c = await h.signed_in(ROOT)
    async with using(c):
        assert (await c.get(f"{UI}/agents/ghost")).status_code == 404
        missing = await c.get(f"{UI}/agents/it-helper/versions/9")
        assert missing.status_code == 404 and "has no version 9" in missing.text
        assert (await c.get(f"{UI}/agents/it-helper/versions/abc")).status_code == 404


async def test_an_unexpected_failure_is_a_plain_page_without_details(h):
    async def broken(*args, **kwargs):
        raise RuntimeError("secret-detail-from-the-database")

    h.plane.list_agents = broken
    c = await h.signed_in(ROOT)
    async with using(c):
        response = await c.get(f"{UI}/agents")
    assert response.status_code == 500
    assert "secret-detail" not in response.text and "Traceback" not in response.text


# --- the lifecycle through the forms ------------------------------------------------


async def test_creating_an_agent_through_the_form_makes_a_draft_and_says_so(h):
    """Fails if the create form redirects elsewhere, shows no notice, or audits another identity."""
    owner = await h.signed_in(OWNER)
    async with using(owner):
        created = await post(
            owner,
            f"{UI}/agents",
            await csrf_token(owner),
            agent_id="it-helper",
            definition=definition_text(helper_block()),
        )
        assert created.status_code == 303
        assert created.headers["location"] == f"{VERSION}?done=created"
        page = await owner.get(created.headers["location"])
    assert "Agent created as a draft." in page.text
    assert await status_of_version(h) == "draft"
    assert [(e.kind, e.identity) for e in h.audit.events] == [("agent_create", f"user:{OWNER}")]


async def test_saving_the_edit_form_replaces_the_draft_definition(h):
    """Fails if the edit form does not store the posted definition, or audits another identity."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    owner = await h.signed_in(OWNER)
    edited = helper_block()
    edited["max_steps"] = 4
    async with using(owner):
        saved = await post(
            owner, f"{VERSION}/edit", await csrf_token(owner), definition=definition_text(edited)
        )
    assert saved.status_code == 303
    assert (await h.agents.get_version(TENANT, "it-helper", 1)).definition["max_steps"] == 4
    assert [(e.kind, e.identity) for e in h.audit.events][-1] == ("agent_edit", f"user:{OWNER}")


async def test_submitting_the_form_makes_the_version_pending(h):
    """Fails if the submit form leaves the version a draft, or audits another identity."""
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    owner = await h.signed_in(OWNER)
    async with using(owner):
        submitted = await post(owner, f"{VERSION}/submit", await csrf_token(owner))
    assert submitted.status_code == 303
    assert await status_of_version(h) == "pending"
    assert [(e.kind, e.identity) for e in h.audit.events][-1] == ("agent_submit", f"user:{OWNER}")


async def test_an_admin_publishing_through_the_form_records_the_admin_and_says_so(h):
    """Fails if the publish form does not publish as the admin, shows no notice, or skips the audit events."""
    await make_pending(h)
    admin = await h.signed_in(ADMIN2)
    async with using(admin):
        published = await post(
            admin, f"{VERSION}/publish", await csrf_token(admin), **await shown(h, "publish")
        )
        assert published.status_code == 303
        page = await admin.get(published.headers["location"])
    assert "Published." in page.text
    record = await h.agents.get_version(TENANT, "it-helper", 1)
    assert record.status == "published" and record.approved_by == ADMIN2
    assert [(e.kind, e.identity) for e in h.audit.events][-2:] == [
        ("agent_publish", f"user:{ADMIN2}"),
        ("widening_confirm", f"user:{ADMIN2}"),
    ]


async def test_withdraw_and_reject_send_a_pending_version_back_to_draft(h):
    await make_pending(h)
    owner, admin = await h.signed_in(OWNER), await h.signed_in(ADMIN2)
    async with using(owner), using(admin):
        assert (
            await post(owner, f"{VERSION}/withdraw", await csrf_token(owner))
        ).status_code == 303
        assert await status_of_version(h) == "draft"
        await post(owner, f"{VERSION}/submit", await csrf_token(owner))
        assert (await post(admin, f"{VERSION}/reject", await csrf_token(admin))).status_code == 303
        assert await status_of_version(h) == "draft"
    assert kinds(h) == [
        "agent_create",
        "agent_submit",
        "agent_withdraw",
        "agent_submit",
        "agent_reject",
    ]


async def test_a_new_draft_can_be_copied_from_a_version(h):
    await make_pending(h)
    await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        page = await c.get(VERSION)
        assert 'name="from_version" value="1"' in page.text
        for fields in ({"from_version": "1"}, {}):
            made = await post(c, f"{UI}/agents/it-helper/drafts", token, **fields)
            assert made.status_code == 303
        assert [v.version for v in await h.agents.versions(TENANT, "it-helper")] == [3, 2, 1]


async def test_an_invalid_definition_comes_back_inline_with_the_text_kept(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        for text, message in [
            ("name: [unclosed", "not valid YAML"),
            ("- just\n- a list\n", "must be a YAML mapping"),
            ("name: No prompt\nmax_steps: 3\n", "system_prompt"),
        ]:
            response = await post(c, f"{UI}/agents", token, agent_id="bad", definition=text)
            assert response.status_code == 422, text
            assert message in response.text
            assert text.split("\n")[0] in response.text
            assert 'value="bad"' in response.text
        assert await h.agents.get_agent(TENANT, "bad") is None
    assert kinds(h) == []


async def test_a_bad_edit_keeps_the_draft_and_shows_the_text(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        response = await post(c, f"{VERSION}/edit", token, definition="name: only a name\n")
        assert response.status_code == 422
        assert "only a name" in response.text and "system_prompt" in response.text
    stored = await h.agents.get_version(TENANT, "it-helper", 1)
    assert stored.definition["name"] == helper_block()["name"]


async def test_a_wrong_state_and_a_duplicate_are_409_pages(h):
    await make_pending(h)
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        edit = await post(c, f"{VERSION}/edit", token, definition=definition_text(helper_block()))
        assert edit.status_code == 409 and "needs" in edit.text
        again = await post(c, f"{VERSION}/submit", token)
        assert again.status_code == 409
        duplicate = await post(
            c,
            f"{UI}/agents",
            token,
            agent_id="it-helper",
            definition=definition_text(helper_block()),
        )
        assert duplicate.status_code == 409


async def test_html_in_a_definition_is_escaped_everywhere_it_is_shown(h):
    definition = helper_block()
    definition["system_prompt"] = '<script>alert("pwned")</script> & <img src=x onerror=1>'
    definition["name"] = "<b>bold</b>"
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        created = await post(
            c, f"{UI}/agents", token, agent_id="it-helper", definition=definition_text(definition)
        )
        draft = await c.get(created.headers["location"])
        await h.plane.submit_version(TENANT, OWNER, "it-helper", 1)
        pending = await c.get(VERSION)
        rejected = await post(  # an invalid re-render echoes the text too
            c, f"{UI}/agents", token, agent_id="x", definition="<script>alert(1)</script>"
        )
    for page in (draft, pending, rejected):
        assert "<script" not in page.text
        assert "<img" not in page.text and "<b>bold" not in page.text
    assert "&lt;script&gt;alert(&#34;pwned&#34;)&lt;/script&gt;" in pending.text
    assert "&lt;b&gt;bold&lt;/b&gt;" in pending.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rejected.text


async def test_a_notice_comes_only_from_a_fixed_key_never_from_the_url(h):
    c = await h.signed_in(OWNER)
    async with using(c):
        page = await c.get(f"{UI}/agents", params={"done": "<script>alert(1)</script>"})
        known = await c.get(f"{UI}/agents", params={"done": "saved"})
    assert page.status_code == 200 and "alert" not in page.text
    assert "Draft saved." in known.text


async def test_pages_load_nothing_from_outside_and_run_no_script(h):
    await make_pending(h)
    c = await h.signed_in(ROOT)
    async with using(c):
        pages = [
            await c.get(UI + p) for p in ("/agents", "/members", "/audit", "/agents/it-helper")
        ]
        pages.append(await c.get(VERSION))
    for page in pages:
        assert page.status_code == 200
        assert "<script" not in page.text and "<link" not in page.text
        assert not re.search(r'(src|href)="https?:', page.text)


def test_the_new_agent_skeleton_is_a_valid_definition():
    parse_agent("skeleton", parse_definition(SKELETON), 1)


def test_a_definition_round_trips_through_its_text():
    definition = helper_block()
    definition["system_prompt"] = "line one\nline two with: a colon\n\n  indented\n"
    assert parse_definition(definition_text(definition)) == definition


# --- the grant check ----------------------------------------------------------------


async def test_the_grant_check_button_shows_the_result(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        clean = await post(c, f"{VERSION}/check-grants", token)
        assert clean.status_code == 200
        assert "no problems" in clean.text and "IT requests" in clean.text and DB_ID in clean.text

        schema = default_schema()
        schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
        h.notion.set_schema(DB_ID, schema)
        broken = await post(c, f"{VERSION}/check-grants", token)
        assert broken.status_code == 200 and "problems found" in broken.text
        assert "request_id" in broken.text
    assert kinds(h) == ["agent_create"]  # the check only reads


async def test_the_grant_check_is_refused_to_a_stranger(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    c = await h.signed_in(OTHER)
    async with using(c):
        response = await post(c, f"{VERSION}/check-grants", await csrf_token(c))
    assert response.status_code == 403


async def test_a_refused_publish_shows_the_grant_problems_and_changes_nothing(h):
    await make_pending(h)
    schema = default_schema()
    schema["Agent request ID"] = {"id": "reqid1", "type": "people"}
    h.notion.set_schema(DB_ID, schema)
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        refused = await post(c, f"{VERSION}/publish", token, **await shown(h, "publish"))
        assert refused.status_code == 409
        assert "grant problem" in refused.text and "request_id" in refused.text
        assert "problems found" in refused.text
        assert await status_of_version(h) == "pending"
        h.notion.set_schema(DB_ID, default_schema())
        assert (
            await post(c, f"{VERSION}/publish", token, **await shown(h, "publish"))
        ).status_code == 303
    assert await status_of_version(h) == "published"


async def test_a_stale_hash_publish_redirects_to_the_version_page_with_the_error(h):
    """Fails if a stale-hash publish renders a page at the POST URL instead of redirecting to the version page."""
    await make_pending(h)
    admin = await h.signed_in(ADMIN2)
    fields = await shown(h, "publish")
    fields["definition_sha256"] = "0" * 64
    async with using(admin):
        refused = await post(admin, f"{VERSION}/publish", await csrf_token(admin), **fields)
        assert refused.status_code == 303
        location = refused.headers["location"]
        assert location == f"{VERSION}?refused=version-changed"
        page = await admin.get(location)
    assert page.status_code == 200
    assert "changed since you opened it" in page.text
    assert await status_of_version(h) == "pending"


NOTION_DOWN = "Couldn't reach Notion just now; nothing was changed. Try again."


class Down:
    """A Notion that cannot be reached; the message must never reach the page."""

    async def retrieve_schema(self, database_id):
        raise TimeoutError("boom with a token inside")


@pytest.mark.parametrize("path", ["publish", "check-grants"])
async def test_notion_being_unreachable_is_a_503_page_with_a_message(h, path):
    await make_pending(h)
    h.notion = Down()
    c = await h.signed_in(ROOT)
    async with using(c):
        response = await post(c, f"{VERSION}/{path}", await csrf_token(c), **await shown(h, path))
    assert response.status_code == 503
    assert NOTION_DOWN in html.unescape(response.text)
    assert "token" not in response.text and "boom" not in response.text
    assert "<h1>it-helper version 1" in response.text  # the version page, not a bare message
    assert await status_of_version(h) == "pending"
    assert "agent_publish" not in kinds(h)


async def test_the_grant_check_button_changes_nothing_when_notion_is_down(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    before = await h.agents.get_version(TENANT, "it-helper", 1)
    h.notion = Down()
    c = await h.signed_in(OWNER)
    async with using(c):
        response = await post(c, f"{VERSION}/check-grants", await csrf_token(c))
    assert response.status_code == 503 and NOTION_DOWN in html.unescape(response.text)
    assert await h.agents.get_version(TENANT, "it-helper", 1) == before
    assert kinds(h) == ["agent_create"]


async def test_a_console_page_is_never_a_502_or_a_504():
    from agentos.control_errors import STATUS_BY_ERROR, page_status

    for status in STATUS_BY_ERROR.values():
        assert page_status(status) not in (502, 504)
    assert page_status(502) == 503 and page_status(504) == 503
    assert page_status(409) == 409 and page_status(404) == 404


@pytest.mark.parametrize("path", ["publish", "check-grants"])
async def test_an_unexpected_error_is_a_plain_500_page(h, path):
    await make_pending(h)

    class Buggy:
        async def retrieve_schema(self, database_id):
            raise RuntimeError("secret detail inside")

    h.notion = Buggy()
    c = await h.signed_in(ROOT)
    async with using(c):
        response = await post(c, f"{VERSION}/{path}", await csrf_token(c), **await shown(h, path))
    assert response.status_code == 500
    assert "Internal error." in response.text
    assert "secret" not in response.text and "RuntimeError" not in response.text
    assert await status_of_version(h) == "pending"


# --- members and the audit view -----------------------------------------------------


async def test_members_are_added_changed_and_removed_through_the_forms(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        added = await post(c, f"{UI}/members", token, user_id="U_NEW", role="creator")
        assert added.status_code == 303 and added.headers["location"].endswith("member-added")
        assert (await h.members.get_member(TENANT, "U_NEW")).role == "creator"
        assert (await post(c, f"{UI}/members/U_NEW/role", token, role="admin")).status_code == 303
        assert (await h.members.get_member(TENANT, "U_NEW")).role == "admin"
        assert (await post(c, f"{UI}/members/U_NEW/remove", token)).status_code == 303
        assert await h.members.get_member(TENANT, "U_NEW") is None
    assert kinds(h) == ["member_add", "member_role", "member_remove"]


async def test_member_refusals_are_pages_with_the_api_messages(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        duplicate = await post(c, f"{UI}/members", token, user_id=OWNER, role="creator")
        assert duplicate.status_code == 409 and "already a member" in duplicate.text
        bad_role = await post(c, f"{UI}/members", token, user_id="U_NEW", role="owner")
        assert bad_role.status_code == 422
        config_admin = await post(c, f"{UI}/members/{ROOT}/remove", token)
        assert config_admin.status_code == 403 and "tenant file" in config_admin.text
        ghost = await post(c, f"{UI}/members/U_GHOST/remove", token)
        assert ghost.status_code == 404


async def test_a_creator_cannot_drive_the_admin_forms(h):
    await make_pending(h)
    c = await h.signed_in(OWNER)
    async with using(c):
        token = await csrf_token(c)
        for path, fields in [
            (f"{VERSION}/publish", {}),
            (f"{VERSION}/reject", {}),
            (f"{UI}/members", {"user_id": "U_NEW", "role": "admin"}),
            (f"{UI}/members/{OTHER}/role", {"role": "admin"}),
            (f"{UI}/members/{OTHER}/remove", {}),
            (f"{UI}/settings/routing", {"default_agent": "it-helper"}),
            (f"{UI}/settings/notion/disconnect", {}),
            (f"{UI}/settings/webhook/reveal", {}),
            (f"{UI}/settings/webhook/confirm", {}),
            (f"{UI}/settings/webhook/clear", {}),
        ]:
            response = await post(c, path, token, **fields)
            assert response.status_code == 403, path
            assert "may not do this" in response.text
    assert await status_of_version(h) == "pending"
    assert await h.members.get_member(TENANT, "U_NEW") is None
    assert (await h.members.get_member(TENANT, OTHER)).role == "creator"
    assert kinds(h) == ["agent_create", "agent_submit"]


async def test_another_creator_cannot_submit_or_edit_an_agent_they_do_not_own(h):
    await h.plane.add_agent(TENANT, OWNER, "it-helper", helper_block())
    c = await h.signed_in(OTHER)
    async with using(c):
        token = await csrf_token(c)
        edit = await post(c, f"{VERSION}/edit", token, definition=definition_text(helper_block()))
        submit = await post(c, f"{VERSION}/submit", token)
    assert edit.status_code == 403 and submit.status_code == 403
    assert await status_of_version(h) == "draft"


async def test_the_audit_view_is_newest_first_and_paged_by_seq(h):
    for i in range(60):
        await h.plane.invite_member(TENANT, ROOT, f"U_BULK{i:02}", "creator")
    c = await h.signed_in(ROOT)
    async with using(c):
        first = await c.get(f"{UI}/audit")
        seqs = [int(n) for n in re.findall(r"<tr>\s*<td>(\d+)</td>", first.text)]
        assert len(seqs) == 50 and seqs == sorted(seqs, reverse=True)
        link = re.search(r'href="([^"]*audit\?before=\d+)"', first.text)
        assert link and link.group(1) == f"{UI}/audit?before={seqs[-1]}"
        second = await c.get(link.group(1))
        older = [int(n) for n in re.findall(r"<tr>\s*<td>(\d+)</td>", second.text)]
        assert len(older) == 10 and max(older) < min(seqs)
        assert "Older events" not in second.text
        junk = await c.get(f"{UI}/audit", params={"before": "x"})
        assert junk.status_code == 200


async def test_the_audit_view_shows_actors_agents_approvers_and_hashes_only(h):
    await make_pending(h)
    await h.plane.publish_version(TENANT, ROOT, "it-helper", 1)
    c = await h.signed_in(ROOT)
    async with using(c):
        page = strip((await c.get(f"{UI}/audit")).text)
    assert "agent_publish" in page and "it-helper v1" in page
    assert f"<td>{ROOT}</td>" in page and f"<td>{OWNER}</td>" in page
    assert "user:" + ROOT in page
    assert helper_block()["system_prompt"] not in page


# --- forms: CSRF and origin ---------------------------------------------------------


FORM_ROUTES = [
    ("members", {"user_id": "U_NEW", "role": "creator"}),
    ("switches/off", {"scope": "writes", "target": ""}),
    ("budgets", {"agent_id": "mine", "amount": "5"}),
    ("settings/routing", {"default_agent": "it-helper"}),
    ("settings/notion/start", {}),
    ("settings/notion/disconnect", {}),
    ("settings/webhook/reveal", {}),
    ("settings/webhook/confirm", {}),
    ("settings/webhook/clear", {}),
]


async def test_a_form_without_the_right_token_or_origin_is_refused_and_changes_nothing(h):
    """Fails if a form post with a missing, wrong or cross-origin token or origin is accepted on any form route."""
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        for route, form in FORM_ROUTES:
            refused = [
                await post(c, f"{UI}/{route}", None, **form),
                await post(c, f"{UI}/{route}", "", **form),
                await post(c, f"{UI}/{route}", token + "x", **form),
                await post(c, f"{UI}/{route}", token[:-1], **form),
                await post(c, f"{UI}/{route}", "a" * len(token), **form),
            ]
            assert [r.status_code for r in refused] == [403] * 5, route
        content = "user_id=U_NEW&role=creator"
        urlencoded = {"Content-Type": "application/x-www-form-urlencoded"}
        wrong_origins = [
            {"Origin": "https://evil.example"},
            {"Origin": "null"},
            {"Origin": "http://console.example.com"},
            {"Origin": "https://other.example.org"},
            {"Origin": "https://console.example.com:444"},
            {},  # neither Origin nor Referer
            {"Referer": "https://evil.example/page"},
            {"Referer": "not a url"},
            {"Origin": CONSOLE_URL, "Sec-Fetch-Site": "cross-site"},
        ]
        for headers in wrong_origins:
            response = await c.post(
                f"{UI}/members",
                content=content + f"&csrf={token}",
                headers={**urlencoded, **headers},
            )
            assert response.status_code == 403, headers
        assert await h.members.get_member(TENANT, "U_NEW") is None
        assert await h.switches.list(TENANT) == []
        assert await h.budgets.list(TENANT) == []
        assert kinds(h) == []


async def test_the_referer_stands_in_when_the_browser_sends_no_origin(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        response = await c.post(
            f"{UI}/members",
            content=f"user_id=U_NEW&role=creator&csrf={token}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{CONSOLE_URL}/ui/{TENANT}/members",
            },
        )
    assert response.status_code == 303
    assert (await h.members.get_member(TENANT, "U_NEW")).role == "creator"


async def test_a_token_from_another_session_does_not_work(h):
    one, two = await h.signed_in(ROOT), await h.signed_in(ADMIN2)
    async with using(one), using(two):
        theirs = await csrf_token(two)
        response = await post(one, f"{UI}/members", theirs, user_id="U_NEW", role="creator")
    assert response.status_code == 403
    assert await h.members.get_member(TENANT, "U_NEW") is None


async def test_the_token_is_in_every_form_and_differs_per_session(h):
    await make_pending(h)
    one, two = await h.signed_in(ROOT), await h.signed_in(ADMIN2)
    async with using(one), using(two):
        page = await one.get(VERSION)
        forms = re.findall(r'<form[^>]*method="post"', page.text)
        assert len(forms) == len(re.findall(r'name="csrf"', page.text)) >= 5
        assert csrf_in(page.text) != csrf_in((await two.get(VERSION)).text)


async def test_the_form_body_has_a_size_limit(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        huge = await post(c, f"{UI}/agents", token, agent_id="x", definition="a" * 300_000)
    assert huge.status_code == 413


async def test_a_form_post_is_refused_on_the_json_routes(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        response = await post(c, f"/tenants/{TENANT}/members", token, user_id="U_X", role="admin")
    assert response.status_code == 415
    assert await h.members.get_member(TENANT, "U_X") is None


# --- headers, hosts and origins -----------------------------------------------------


async def test_security_headers_are_on_pages_redirects_json_and_refusals(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        responses = [
            await c.get(f"{UI}/agents"),
            await c.get("/login"),
            await c.get(f"/tenants/{TENANT}/me"),
            await c.get("/healthz"),
            await c.get(f"{UI}/agents", headers={"Host": "evil.example"}),
            await post(c, f"{UI}/members", None),
        ]
    assert [r.status_code for r in responses] == [200, 303, 200, 200, 421, 403]
    for response in responses:
        assert response.headers["content-security-policy"] == (
            "default-src 'self'; style-src 'self' 'unsafe-inline'; form-action 'self'; "
            "frame-ancestors 'none'"
        )
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["referrer-policy"] == "same-origin"
        assert response.headers["cache-control"] == "no-store"


async def test_the_host_allowlist_follows_the_console_url(h):
    async with h.client() as c:
        assert (await c.get("/login")).status_code == 303
        for host in ("evil.example", "console.example.com.evil.example", "other.example.org"):
            refused = await c.get("/login", headers={"Host": host})
            assert refused.status_code == 421, host
        for host in ("127.0.0.1:8100", "localhost:8100", "[::1]:8100"):
            assert (await c.get("/login", headers={"Host": host})).status_code == 303, host
    elsewhere = ConsoleHarness("https://other.example.org")
    async with elsewhere.client() as c:
        assert (await c.get("/login")).status_code == 303
        refused = await c.get("/login", headers={"Host": "console.example.com"})
        assert refused.status_code == 421
        assert (await c.get("/healthz", headers={"Host": "anything"})).status_code == 200


FROM_SLACK = {
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
}


async def test_slack_may_send_the_browser_back_from_another_site_to_the_callback(h):
    async with h.client() as c:
        login = await c.get("/login")
        arriving = await c.get(
            "/auth/slack/callback",
            params={"code": "x", "state": "wrong"},
            headers={"Sec-Fetch-Site": "cross-site"},
        )
        assert login.status_code == 303
        assert arriving.status_code == 400  # reached the callback, which checked the state


async def test_the_redirect_after_sign_in_lands_on_the_console_although_the_browser_calls_it_cross_site(
    h,
):
    async with h.client() as c:
        login = await c.get("/login")
        query = parse_qs(urlsplit(login.headers["location"]).query)
        h.slack.nonce = query["nonce"][0]
        callback = await c.get(
            "/auth/slack/callback",
            params={"code": "made-up-code", "state": query["state"][0]},
            headers=FROM_SLACK,
        )
        assert callback.status_code == 303
        landing = await c.get(callback.headers["location"], headers=FROM_SLACK)
        assert landing.status_code == 200 and "New agent" in landing.text


async def test_a_link_from_another_site_opens_the_console_pages(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        for site in ("cross-site", "same-site"):
            headers = {**FROM_SLACK, "Sec-Fetch-Site": site}
            agents = await c.get(f"{UI}/agents", headers=headers)
            assert agents.status_code == 200 and "New agent" in agents.text, site
            assert (await c.head(f"{UI}/agents", headers=headers)).status_code == 200, site
            assert (await c.get("/", headers=headers)).status_code == 303, site
            assert (await c.get("/login", headers=headers)).status_code == 303, site


async def test_a_cross_site_page_get_that_is_not_a_top_level_navigation_is_refused(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        refused = [
            {**FROM_SLACK, "Sec-Fetch-Dest": "iframe"},
            {**FROM_SLACK, "Sec-Fetch-Dest": "frame"},
            {**FROM_SLACK, "Sec-Fetch-Dest": "object"},
            {**FROM_SLACK, "Sec-Fetch-Dest": "embed"},
            {**FROM_SLACK, "Sec-Fetch-Dest": "empty"},
            {"Sec-Fetch-Site": "cross-site"},
            {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "navigate"},
            {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Dest": "document"},
            {**FROM_SLACK, "Sec-Fetch-Mode": "no-cors"},
            {**FROM_SLACK, "Sec-Fetch-Mode": "cors"},
        ]
        for headers in refused:
            for path in (f"{UI}/agents", "/login", "/"):
                response = await c.get(path, headers=headers)
                assert response.status_code == 403, (path, headers)
                assert "agents" not in response.text and "New agent" not in response.text


async def test_only_pages_are_open_to_a_cross_site_navigation(h):
    c = await h.signed_in(ROOT)
    async with using(c):
        token = await csrf_token(c)
        fields = f"user_id=U_NEW&role=creator&csrf={token}"
        form = {"Content-Type": "application/x-www-form-urlencoded", "Origin": CONSOLE_URL}
        posted = await c.post(f"{UI}/members", content=fields, headers={**form, **FROM_SLACK})
        assert posted.status_code == 403
        logout = await c.post("/logout", content=f"csrf={token}", headers={**form, **FROM_SLACK})
        assert logout.status_code == 403
        assert await h.members.get_member(TENANT, "U_NEW") is None
        for path in (f"/tenants/{TENANT}/agents", f"/tenants/{TENANT}/me"):
            response = await c.get(path, headers=FROM_SLACK)
            assert response.status_code == 403, path
        elsewhere = await c.get("/auth/slack/other", headers=FROM_SLACK)
        assert elsewhere.status_code == 403


async def test_other_tenants_data_is_not_reachable_through_a_session(h):
    await h.plane.add_agent(GLOBEX, ROOT, "globex-secret", helper_block())
    globex_events = len([e for e in h.audit.events if e.tenant_id == GLOBEX])
    c = await h.signed_in(OWNER)
    async with using(c):
        for path in (
            f"/ui/{GLOBEX}/agents",
            f"/ui/{GLOBEX}/agents/globex-secret",
            f"/ui/{GLOBEX}/settings",
        ):
            response = await c.get(path)
            assert response.status_code == 403 and "globex-secret" not in response.text
        token = await csrf_token(c)
        for path, fields in (
            (f"/ui/{GLOBEX}/members", {"user_id": "U_NEW", "role": "admin"}),
            (f"/ui/{GLOBEX}/settings/routing", {"default_agent": "it-helper"}),
        ):
            posted = await post(c, path, token, **fields)
            assert posted.status_code == 403, path
    assert await h.members.get_member(GLOBEX, "U_NEW") is None
    assert len([e for e in h.audit.events if e.tenant_id == GLOBEX]) == globex_events
