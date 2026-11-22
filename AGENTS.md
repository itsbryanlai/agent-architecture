# Agent instructions

Slack agents that do work, on Temporal. The design is in `docs/product.md`,
`docs/architecture.md` and `docs/decisions.md`. Code that must differ from them is
reported as a deviation; the docs are changed only with the maintainer's go.

## Product decisions

Every product decision, and every option proposed for one, is checked against two
criteria and states how it meets them:

1. **Seamless for the user.** Count the steps a customer, admin or end user takes
   (installing, connecting a system, granting access, approving). Fewer is better: no
   developer consoles, no pasted keys or tokens, ideally one click or one share.
2. **Scalable for the product.** It keeps working as tenants, users, agents and
   connectors grow: no per-tenant manual step, no hand-run operation, no limit the
   product will outgrow soon.

An option that fails either criterion is named as such, with the cost of meeting it.

## Rules the code must keep

- An agent acts with its own granted access, never the asker's. Grants are
  checked before any connector call.
- Identity comes from the runtime, never from the LLM.
- Temporal history carries references and small values, never chat or document
  content.
- Writes never run without approval from the requester, unless an admin turned
  approval off for that low-risk action; what was approved, or what the policy
  allowed, is exactly what runs; every external write is idempotent.
- Every workflow ID, row and log line carries the tenant ID. IDs are built only in
  `src/agentos/ids.py`.
- Temporal workflow code stays deterministic: `workflow.now()`,
  `workflow.uuid4()`, `workflow.sleep()`; no I/O. A change that alters the commands
  an existing history produces goes behind `workflow.patched("<name>")`, and
  `tests/core/test_conversation_replay.py` keeps passing.
- Committed files never name local planning paths, ticket IDs or tool
  attribution.

## Verification

Run from the repository root.

    uv sync
    uv run ruff check .
    uv run ruff format --check .
    bash -n scripts/setup-wizard.sh
    docker compose config --quiet       # needs AGENTOS_OWNER_DB_PASSWORD and AGENTOS_APP_DB_PASSWORD set (any value for the syntax check)
    uv run pytest <the test files for the change> -v
    uv run pytest tests/pg -m pg -v     # needs the Compose Postgres on 127.0.0.1:55432
    uv run python scripts/record_replay_fixtures.py --check
    git diff --check

The full suite is `uv run pytest -q`. Tests use fakes only (scripted LLM, fake
Slack, fake Notion, in-memory stores), the real-Postgres tier above, and
Temporal's time-skipping test server. They never touch live Slack, Notion or a
model endpoint, and never need an API key.

## Test rules

Before writing a test:

1. Name the defect. The docstring is one line: "Fails if <a specific wrong behaviour>". If that line cannot be written, the test is not written.
2. Stay inside the test budget the task gives: one test per decision made, one per external side effect, one per refusal path, plus one parametrized table per input-validation rule.
3. Look for an existing guard first. CSRF, Origin, security headers, tenant-first `set_config` and "creator forbidden" each have one test; a new page or store is a row in it, not a copy.

What a test may assert:

4. Never the text of a file: no `read_text()` of SQL, source, templates, README or config; no AST scans. Run SQL in the `pg` tier. What may be committed is checked when committing, not by a test. One exception: the activity-gate structural test in `tests/core/test_agent_gate.py`, which the design requires.
5. Never a fake's own logic. A test whose subject is an `InMemory*` or `Fake*` class is deleted. Store behaviour is one contract suite, parametrized over the fake and the real store.
6. Never a constant, a default or shipped config content. Test the mechanism that reads it, with one input. One exception: `test_the_shipped_agent_files_still_parse` checks that the shipped example files parse.
7. Never internals: no private attributes, no idempotency-key strings, no positional constructor arguments of a mock. Assert what a user, Slack, Notion or the database would see.
8. The body proves the name. A test called "survives continue-as-new" performs a continue-as-new. "No content" asserts the exact text. No `or` in an assertion.
9. Absence needs a settle point. To assert "only one" or "nothing posted", first cause a later observable event (a sentinel message and its reply), then count. Never right after the first event, never after a sleep.

Shape:

10. Same arrange and act with different input is one parametrized test. A refusal and its matching acceptance are rows of one table with an expected outcome.
11. One behaviour per test. No lifecycle tours with 25 assertions.
12. No real time. Inject the clock; use the time-skipping server. No `asyncio.sleep` to order timestamps, no `asyncio.run` inside a sync test.
13. Helpers live in `conftest.py` or `tests/helpers/`. A test file never imports from another test file. Tests sit in `tests/core/`, `tests/console/`, `tests/knowledge/` and `tests/connectors/`; the real-Postgres tier is `tests/pg/`.
14. One Temporal test server per session, one task queue per test. A test that needs its own (the encrypting converter, a worker restart) says so in a comment.

Fakes:

15. A fake keeps what production sends. If the code passes a value, the fake stores it and a test reads it back.
16. Every external write gets two failure tests: the response lost after the side effect, and a second attempt overlapping the first. The fake carries the injection points (`fail_after_write`, `delay_visibility`) in one place, not monkeypatched per test.
17. A real client gets a transport-level test: drive `RealNotionAPI`, the Slack client's paging and error mapping, and the LLM clients through a mocked HTTP transport that raises the SDK's real exception types.

Workflows:

18. A change to workflow code adds a replay fixture recorded by the new code, from the fixture recording script (`scripts/record_replay_fixtures.py`, which records each scenario in a legacy shape and a reference shape under the encrypting converter), and keeps the old ones. A fixture no script can regenerate is deleted.
