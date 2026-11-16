# Replay fixtures

Each `.json` file is the recorded history of one workflow run. `tests/test_replay.py` replays
every file on the current workflow code. A replay fails with a non-determinism error when a
change to a workflow alters the commands an existing history would issue, which is what would
happen to a workflow that is running when the change is deployed.

## Where they come from

`scripts/record_replay_fixtures.py` runs each scenario in `tests/replay_support.py` against
Temporal's time-skipping test server, with fakes and the production encrypting data converter
under a fixed test key, and writes the histories here. Each file starts with a header: the
code commit it was recorded at, the date, the converter, and the scenario.

A scenario is recorded in two shapes. `_inline` is the shape from before content moved to
Postgres (the text rides in the signal, a task's payload in its spec); `_stored` is the shape
now (the signal and the spec carry references). Both stay, because both kinds of workflow can
be running at once. A scenario that continues as new writes one file per run (`.run1`,
`.run2`, ...). A task's child run is written next to its conversation.

`plain_turn`, `step_limit` and `partial_failure` were recorded earlier by hand, with the
default converter and no header. They are kept as they are.

## Commands

    uv run python scripts/record_replay_fixtures.py                 # record every scenario
    uv run python scripts/record_replay_fixtures.py watch_done_stored   # record one
    uv run python scripts/record_replay_fixtures.py --check         # replay what is recorded

## When you change a workflow

1. Gate the change that alters commands behind `workflow.patched("<name>")`.
2. Add a scenario that exercises the new path, or extend one, and record it with the script.
3. Keep the old fixtures. They are the proof that workflows already running still replay.
   Record an old scenario again only when the scenario itself changed, never to make a replay
   pass.
4. Run `--check` and `uv run pytest tests/test_replay.py`.
