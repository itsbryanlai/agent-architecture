"""The ``pg`` tier: SQL run against a real Postgres, in throwaway databases.

Needs the pgvector Postgres that Compose publishes on 127.0.0.1:55432 and a role that
can create databases. The tier skips, with the reason in the report, when the server is
not there. It touches no database it did not create (names ``agentos_test_<pid>_<n>``),
never changes a role's password, and uses no key.

Isolation: one database per session holds every store and policy test. Each test uses
tenant IDs of its own, so rows never meet, and nothing is truncated (``audit_events``
refuses TRUNCATE by design). A test that changes the schema, or counts something no
tenant owns, builds a database of its own with ``databases.create()``.

The app role is the cluster-wide ``agentos_app``, whose password this tier must not
touch. A connection to a test database therefore logs in as the owner and switches to
``agentos_app`` for the session (``options=-c role=agentos_app``): privileges and
row-level security are those of the app role, which owns nothing and cannot bypass them.
"""

from __future__ import annotations

import importlib.util
import itertools
import os
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from psycopg_pool import AsyncConnectionPool

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
MIGRATIONS = SCRIPTS / "migrations"
INIT_DB = SCRIPTS / "init_db.sql"
BASE_SCHEMA = Path(__file__).parent / "fixtures" / "base_schema.sql"
OWNER_URL_VAR = "AGENTOS_PG_TEST_OWNER_URL"
DEFAULT_OWNER_URL = "postgresql://agentos:agentos@127.0.0.1:55432/postgres"
SKIP_REASON = (
    "no Postgres for the pg tier: start it with 'docker compose up -d postgres' "
    f"(host port 55432) or set {OWNER_URL_VAR} to a role that can create databases"
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if not Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            continue
        callspec = getattr(item, "callspec", None)
        if callspec is not None and callspec.params.get("store_kind") == "fake":
            continue  # a contract test run against the in-memory twin needs no Postgres
        item.add_marker(pytest.mark.pg)


@pytest.fixture(params=["fake", "real"])
def store_kind(request) -> str:
    """Which twin a contract test runs against: the in-memory fake or the Postgres store."""
    return request.param


def postgres_store_classes() -> dict[str, type]:
    """Every ``Postgres*Store`` class under src/agentos, found by importing the package."""
    import inspect
    import pkgutil

    import agentos

    found: dict[str, type] = {}
    for info in pkgutil.walk_packages(agentos.__path__, "agentos."):
        module = importlib.import_module(info.name)
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if (
                name.startswith("Postgres")
                and name.endswith("Store")
                and cls.__module__ == module.__name__
            ):
                found[name] = cls
    return found


# Filled by the fixture below: the Postgres store classes some test in this folder built.
CONSTRUCTED: set[str] = set()


@pytest.fixture(scope="session", autouse=True)
def register_constructed_stores() -> Iterator[None]:
    """Records each Postgres*Store a test constructs, for the coverage guard."""
    patched: list[tuple[type, object]] = []
    for name, cls in postgres_store_classes().items():
        original = cls.__init__

        def init(self, *args, _name=name, _original=original, **kwargs):
            CONSTRUCTED.add(_name)
            _original(self, *args, **kwargs)

        cls.__init__ = init
        patched.append((cls, original))
    try:
        yield
    finally:
        for cls, original in patched:
            cls.__init__ = original


def load_migrate():
    """scripts/migrate.py as a module (scripts is not a package)."""
    spec = importlib.util.spec_from_file_location("migrate", SCRIPTS / "migrate.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["migrate"] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class TestDatabase:
    __test__ = False  # not a test class, despite the name

    name: str
    owner_url: str = field(repr=False)
    app_url: str = field(repr=False)

    def owner(self, **kwargs) -> psycopg.Connection:
        return psycopg.connect(self.owner_url, autocommit=True, **kwargs)

    def execute_file(self, path: Path) -> None:
        with self.owner() as conn:
            conn.execute(path.read_text())

    def pool(self) -> AsyncConnectionPool:
        return AsyncConnectionPool(self.app_url, open=False)


class Databases:
    """Creates databases next to the maintainer's and drops every one it made."""

    def __init__(self, owner_url: str):
        self._owner_url = owner_url
        self._numbers = itertools.count(1)
        self._made: list[str] = []

    def create(self, *, init: bool = False) -> TestDatabase:
        """An empty database, or one built from init_db.sql."""
        name = f"agentos_test_{os.getpid()}_{next(self._numbers)}"
        with psycopg.connect(self._owner_url, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{name}"')
        self._made.append(name)
        db = TestDatabase(
            name=name,
            owner_url=make_conninfo(self._owner_url, dbname=name),
            app_url=make_conninfo(self._owner_url, dbname=name, options="-c role=agentos_app"),
        )
        if init:
            db.execute_file(INIT_DB)
        return db

    def drop_all(self) -> None:
        with psycopg.connect(self._owner_url, autocommit=True) as conn:
            for name in self._made:
                conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        self._made.clear()


@pytest.fixture(scope="session")
def databases() -> Iterator[Databases]:
    owner_url = os.environ.get(OWNER_URL_VAR) or DEFAULT_OWNER_URL
    try:
        psycopg.connect(owner_url, connect_timeout=3).close()
    except psycopg.OperationalError:
        pytest.skip(SKIP_REASON)
    made = Databases(owner_url)
    try:
        yield made
    finally:
        made.drop_all()


@pytest.fixture(scope="session")
def db(databases: Databases) -> TestDatabase:
    """The shared database, built from init_db.sql."""
    return databases.create(init=True)


@pytest.fixture
async def pool(db: TestDatabase) -> AsyncIterator[AsyncConnectionPool]:
    """A pool of app-role connections to the shared database."""
    pool = db.pool()
    await pool.open()
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def store_pool(store_kind: str, request) -> AsyncIterator[AsyncConnectionPool | None]:
    """The app-role pool for a contract test's real run; None for the in-memory twin."""
    if store_kind == "fake":
        yield None
        return
    pool = request.getfixturevalue("db").pool()
    await pool.open()
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def tenant() -> str:
    return f"t-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def other_tenant() -> str:
    return f"t-{uuid.uuid4().hex[:10]}"
