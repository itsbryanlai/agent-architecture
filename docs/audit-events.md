# Audit events

The single reference for every kind of event in the audit log (`audit_events`). The kinds are
`AUDIT_KINDS` in `src/agentos/models.py`, and the same set is seeded into the `audit_kinds`
table by `scripts/init_db.sql` and the migrations. A kind that is not in both cannot be written.

Contents: [Columns](#columns) · [Conversation and tools](#conversation-and-tools) ·
[Writes](#writes) · [Agent lifecycle](#agent-lifecycle) · [Members](#members) ·
[Audiences](#audiences) · [Budgets and usage](#budgets-and-usage) · [Switches](#switches) ·
[Retention](#retention) · [Access review](#access-review) · [What the log never holds](#what-the-log-never-holds) ·
[How to read it](#how-to-read-it) · [What you see in a live run](#what-you-see-in-a-live-run)

## Columns

Every row has `tenant_id`, `seq` (insertion order, may have gaps), `at`, `kind`, `event_key`
(unique per tenant) and `outcome` (`ok` or `error`). The tables below say what the other
columns hold for each kind:

- **requester**: `requester_user_id`. **identity**: whose access was used.
- Identity is one of `agent:<agent id>` (an agent's own access, for what an agent does in a
  conversation), `user:<Slack user ID>` (a person using the console or API),
  `operator:<OS login>` (a host command), or `system:audience-watch` (the shared-channel watch).
- **agent / version**: `agent_id` and `agent_version`. A version is always present when an agent is,
  except for a budget or switch on an agent, which belong to the agent and not to one of its versions,
  and an `audience_resume` for an agent that no longer has a published version.
- **tool / target / approver / in / out**: `tool`, `target`, `approver_user_id`,
  `input_sha256`, `output_sha256`. A dash means the column is empty.

An event key is built in `src/agentos/ids.py`. A key built from the thing that happened means a
retried activity records once. A key ending in a random value is recorded again if the caller
retries without a request ID (noted per kind).

## Conversation and tools

Written by the worker's activities, as the agent: identity is always `agent:<agent id>`, requester
is the Slack user who asked, agent and version are the ones the turn is pinned to.

| Kind | Written when | tool | target | approver | in / out | Refused or failed | Event key |
|---|---|---|---|---|---|---|---|
| `answer` | The model's final reply has been shown in Slack (a Sources line included). The fixed stop texts the model call returns as the turn's reply (agent switched off, budget reached, spend unknown) and the no-answer text record as `answer` too. Hand-off announcements, step-limit, failure and empty-message notices are `notice`, not answers; audience refusals are `audience_refused`. | - | - | - | out: hash of the text shown; in: - | Not written if the reply was not shown; the activity retries | `<conversation>:audit:answer:<placeholder ts>` |
| `notice` | A final text that is not a model answer was shown: the hand-off announcement, the step-limit notice, the failure notice or the empty-message prompt. A failure notice posted before an agent could be loaded records version 0. | why: `step_limit`, `hand_off`, `failure`, `empty_message` | - | - | out: hash of the text shown; in: - | Not written if the text was not shown; the activity retries | `<conversation>:audit:notice:<reason>:<placeholder ts or reply idempotency key>` |
| `tool_call` | A read tool ran (`search_knowledge`, a Notion read) and its output was stored. | tool name | - | - | in: hash of the tool input; out: hash of the output text | Written with `error` when the tool failed, or when the agent or the connector was switched off (the model got the off message); nothing is read then | `<conversation>:audit:tool:<tool use id>` |
| `hand_off` | A front door hands the thread to a specialist, before the announcement is shown. Agent and version are the front door's. | - | specialist's agent ID | - | - | A hand-off the specialist's audience refuses is `audience_refused`, not this | `<tenant>:audit:hand-off:<event id>` |

## Writes

Written when an approved write is carried out, as the agent. Requester is the person who asked;
the approver is the same person (only the requester can approve). Tool is the write tool's name
(`add_comment` for a comment, otherwise the create tool the agent's connector declares). Target is the Notion database ID
for a create or the page ID for a comment (after a create, the new page ID on success).
`approval_closed` is the exception: it is written when no approved write is carried out (a decline
or an expiry), with its own tool, target and approver as its row states.

| Kind | Written when | in / out | Refused or failed | Event key |
|---|---|---|---|---|
| `approval` | The approved write starts, before anything is sent. Records the exact payload that was approved. | in: hash of the approved payload; out: - | Also written for a write refused later (switch, audience, idempotency method), because the approval did happen | `<task>:audit:approval:<approval nonce>` |
| `write` | The write finished (or a reconciliation found it done): `ok`. Or it was refused or failed for good: `error`. | in: hash of the payload; out: hash of `{page_id}` or `{comment_id}` on `ok`, - on `error` | Written with `error` when a kill switch covers writes or the connector, the agent was turned off, the requester left the audience, the agent no longer has the grant, the connector rejected it, or no connector manifest declares the idempotency method. Nothing was changed. | `<task>:<create or comment>:audit:write:<outcome>` |
| `approval_closed` | The approval closed without a write: the requester declined it, or it expired. Recorded before the card is updated. Tool is `declined` or `expired` (not the write tool's name); target is empty; the approver is the requester on a decline, empty on an expiry. | in: hash of the proposed payload (the revision the card named); out: - | Not written if the audit store stayed down through the activity's retries, or if the named draft revision is missing (non-retryable); either way the card still closes | `<task>:audit:approval-closed:<approval nonce>` |

A write that is never approved (declined, expired) records one `approval_closed` event and no `approval` or `write`: nothing ran.

## Agent lifecycle

Written by the control plane in the same transaction as the change, so a failed insert leaves no
change behind. Requester and identity: the person acting, as `user:<Slack ID>` (requester is the bare ID).
Each row names the agent and the version it concerns, and holds the hash of that version's definition
as `input_sha256`. Target, tool and out are empty. Event key: `<tenant>:audit:control:<kind>:<request ID>`,
where the request ID is the caller's idempotency value (the API's `Idempotency-Key`); without one it is
random and a retry records again. A refused action (wrong role, bad state) writes nothing.

| Kind | Written when | Requester / approver | Notes |
|---|---|---|---|
| `agent_create` | An admin or creator creates an agent (its first draft) | actor / - | |
| `agent_draft` | A new draft is made from a version | actor / - | A retried `new_draft` with the same request ID returns the draft the first attempt made |
| `agent_edit` | A draft's definition is replaced | actor / - | Hash is of the edited definition |
| `agent_submit` | A draft is submitted for review | actor / - | Hash is the one the admin will be asked to review |
| `agent_withdraw` | A pending version is withdrawn by its editor | actor / - | |
| `agent_reject` | An admin rejects a pending version | actor / - | Admin only |
| `agent_publish` | An admin publishes a pending version | the submitter / the admin (may be the same person) | Admin only. Written first; `widening_confirm` and `audience_confirm` follow it in the same transaction when they apply |
| `agent_import` | The one-time YAML import inserts an agent as published version 1 | `import:yaml` / `import:yaml` | Identity `user:import:yaml`. Key `<tenant>:audit:import:<agent id>`: once per agent, ever |

## Members

Written by the control plane in the same transaction. Admin only. Agent and version are empty (a
membership belongs to the tenant). Identity `user:<Slack ID>`; requester is the admin. Target is the
member's Slack user ID. Key as for the agent lifecycle.

| Kind | Written when | tool | Notes |
|---|---|---|---|
| `member_add` | A member is added | `role:<admin or creator>` | |
| `member_role` | A member's role changes | `role:<new role>` | |
| `member_remove` | A member is removed | - | Admins listed in the tenant file can't be changed, and the last admin can't be demoted or removed: those refusals are not audited |

## Audiences

| Kind | Written when | Requester / identity | Agent / version | tool | target | approver | outcome | Event key |
|---|---|---|---|---|---|---|---|---|
| `audience_refused` | A turn is refused because the asker is not in the agent's audience, or a hand-off to a specialist is refused for the same reason. | Slack user who asked / `agent:<agent id>` | The agent that was asked (for a refused hand-off, the specialist, at its published version) | reason code: `channel`, `dm`, `guest`, `external`, `deactivated`, `unavailable`, `off`, `paused` | the front door's ID for a refused hand-off, else - | - | `error` when the lookup failed (`unavailable`), else `ok` | `<tenant>:audit:audience-refused:<event id>:<agent id>:<front door or empty>` |
| `audience_confirm` | An admin publishes a version after confirming the counted audience (total, guests, other workspaces). Written only when the plane counts audiences. | submitter / `user:<admin>` | Published version | - | `total=N guests=N external=N` | the admin | `ok` | Control key |
| `audience_pause` | The watch finds a channel an agent answers in has been shared with another organisation, and pauses the agent there. One row per agent paused. | `system:audience-watch` for both | The agent's published version | why: `channel_shared` | the channel ID | - | `ok` | `<tenant>:audit:audience-pause:<agent id>:<channel>:<paused at>` (once per pause) |
| `audience_resume` | An editor or admin ends a pause after confirming the channel's people. | actor / `user:<actor>` | The published version; the agent with an empty version when it has no published version | the pause's reason | `<channel> total=N guests=N external=N` | the actor | `ok` | Control key |

## Budgets and usage

| Kind | Written when | Requester / identity | Agent / version | tool | target | outcome | Event key |
|---|---|---|---|---|---|---|---|
| `budget_set` | An admin sets or lifts a monthly budget. | admin / `user:<admin>` | The agent, or empty for the tenant-wide budget; never a version | `set` or `remove` | The amount (`0` for `remove`) | `ok` | Control key. Records only on a real change: a remove with no budget and a set of an unchanged amount record nothing |
| `usage_repriced` | An owner runs `python -m agentos.usage reprice --yes` and at least one row was priced. A dry run, or a run with nothing to price, writes nothing. | `operator:<OS login>` for both | none | `reprice` | `<YYYY-MM> rows=<count>` | `ok` | `<tenant>:audit:control:usage_repriced:<month>:<random>`: a repeat run records again |

## Switches

Written by the control plane, or by the host command that turns switches off and on
(`operator:<OS login>`, which names the requester too). Written only when the switch actually
changed: turning off what is off, or on what is on, records nothing.

| Kind | Written when | Requester / identity | Agent / version | tool | target | outcome | Event key |
|---|---|---|---|---|---|---|---|
| `switch_off` | A switch is turned off | admin's Slack ID / `user:<admin>`, or `operator:<login>` for both | The agent for an `agent` switch, else empty; never a version | scope: `agent`, `all_agents`, `connector`, `writes` | the agent ID or connector name; empty for `all_agents` and `writes` | `ok` | Control key |
| `switch_on` | A switch is turned back on | as above | as above | scope | as above | `ok` | Control key |

## Retention

| Kind | Written when | Requester / identity | Agent / version | tool | target | outcome | Event key |
|---|---|---|---|---|---|---|---|
| `retention_set` | An owner sets a tenant's retention limits from the command line. | `operator:<OS login>` for both | none | `retention` | `kind=days` pairs, sorted, comma-separated | `ok` | `<tenant>:audit:control:retention_set:<random>`: a repeat run records again |

Rows in `audit_events` are not subject to retention themselves.

## Access review

Written with the publish or the pick, by the control plane. Identity `user:<Slack ID>`.

| Kind | Written when | Requester / approver | Agent / version | target | Event key |
|---|---|---|---|---|---|
| `widening_confirm` | An admin publishes a version that widens access (new grant, other properties, new channel, wider direct messages, guests; a first publish always widens) and acknowledges it. Written with `agent_publish` in the same transaction. | the submitter / the admin | The published version | `<N> widening change(s)` | Control key |
| `grant_pick` | A creator chooses grants from their own Notion in the console. | the creator / - | The version being edited | The picked object IDs, comma-separated, cut at 300 characters | Control key |

## What the log never holds

- No message text and no document text. Where something is proved, it is a SHA-256 of canonical
  JSON (sorted keys, no spaces): the answer or notice shown, a tool's input and output, an approved payload,
  an agent definition. The rest is IDs, scope names and small values (counts, amounts, role names).
- Rows are append-only: the database refuses UPDATE and DELETE for every role.
- Rows are exempt from retention and kept until someone removes them outside the app.
- A refused control-plane action (wrong role, last admin, stale review) is not audited. The refusals that
  are audited are the ones above: `audience_refused`, a refused write as `approval` + `write` with
  `error`, and a requester's decline as `approval_closed` with tool `declined`.

## How to read it

- Console: the **Audit** page at `/ui/<tenant>/audit` (admins only), newest first, with a link to older events.
- API: `GET /tenants/<tenant>/audit?limit=&before=` (admins only). `before` is the `seq` of the oldest event
  of the previous page; the response carries `next_before` when there may be more. `seq` has gaps, so page by it, not by count.
- Command line: `uv run python -m agentos.audit --tenant <id> --limit 50` prints JSON lines, newest first.

## What you see in a live run

- A DM question that searches the knowledge: `tool_call` (`search_knowledge`, `ok`), then `answer`.
- A request that files a Notion item: `approval`, then `write` (`ok`, target the new page ID). If the
  agent was switched off between approval and write: `approval`, then `write` with `error`.
- A front door that hands off: `hand_off` (front door as the agent, specialist as target), a `notice` (`hand_off`) for the announcement, then the specialist's own `tool_call` and `answer` rows.
- Someone outside an agent's audience asks it: `audience_refused` (the reason in `tool`).
- Publishing a version: `agent_publish`, then `widening_confirm` when it widens, then `audience_confirm`
  (all in one transaction, in that `seq` order).
- Turning writes off from the host: `switch_off` as `operator:<login>`, scope `writes`.
- Repricing a month: `usage_repriced` as `operator:<login>`, target `2026-11 rows=N`.
