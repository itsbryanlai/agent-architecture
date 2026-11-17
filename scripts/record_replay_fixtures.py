"""Record workflow histories for the replay tests, or check that the recorded ones still replay.

    uv run python scripts/record_replay_fixtures.py           # record every scenario
    uv run python scripts/record_replay_fixtures.py --check   # replay what is recorded

Recording runs each scenario against Temporal's time-skipping test server with fakes and the
production encrypting data converter, under a fixed test key, and writes the histories to
``tests/fixtures/replay``. Nothing here reaches Slack, Notion or a model.
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tests.helpers import replay_support as rs


def commit() -> str:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    except OSError:
        return "unknown"
    return f"{head or 'unknown'}{'+uncommitted' if dirty else ''}"


async def record(only: list[str]) -> int:
    rs.FIXTURES.mkdir(parents=True, exist_ok=True)
    names = only or list(rs.SCENARIOS)
    unknown = [n for n in names if n not in rs.SCENARIOS]
    if unknown:
        print(f"unknown scenario: {', '.join(unknown)}", file=sys.stderr)
        return 2
    head = commit()
    with tempfile.TemporaryDirectory() as tmp:
        for name in names:
            # A fresh server and fakes per scenario, so none depends on another's leftovers.
            async with rs.rig(Path(tmp) / "keys") as r:
                recorded = await rs.SCENARIOS[name](r)
            for rec in recorded:
                print("wrote", rs.write_fixture(rs.FIXTURES, name, rec, head).relative_to(ROOT))
    return 0


async def check() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        failed = await rs.replay_all(rs.FIXTURES, Path(tmp) / "keys")
    count = len(list(rs.FIXTURES.glob("*.json")))
    if failed:
        print("these fixtures no longer replay; add a patch or record again if intended:")
        print("\n".join(failed))
        return 1
    print(f"fixtures up to date: all {count} replay on the current workflow code")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="replay the recorded fixtures")
    parser.add_argument("scenario", nargs="*", help="record only these scenarios")
    args = parser.parse_args()
    return asyncio.run(check() if args.check else record(args.scenario))


if __name__ == "__main__":
    sys.exit(main())
