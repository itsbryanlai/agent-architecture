"""A comparable picture of a database's schema, read from the catalogs.

Column order is left out on purpose: a column added by a migration sits last, one in
init_db.sql sits where it was written, and nothing reads columns by position.
"""

from __future__ import annotations

import psycopg

# Objects an extension owns (pgvector's types and functions) are not the schema's.
_NOT_EXTENSION = "NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = {oid} AND d.deptype = 'e')"

QUERIES = {
    "tables": """
        SELECT c.relname, c.relkind::text, c.relrowsecurity, c.relforcerowsecurity,
               pg_get_userbyid(c.relowner), c.relacl::text, c.reloptions::text
        FROM pg_class c
        WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'S', 'v', 'm')
        ORDER BY 1""",
    "columns": """
        SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
               pg_get_expr(d.adbin, d.adrelid), a.attidentity::text, a.attgenerated::text,
               a.attacl::text
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r'
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY 1, 2""",
    "constraints": """
        SELECT conrelid::regclass::text, conname, contype::text, pg_get_constraintdef(oid)
        FROM pg_constraint WHERE connamespace = 'public'::regnamespace
        ORDER BY 1, 2""",
    "indexes": """
        SELECT tablename, indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'public' ORDER BY 1, 2""",
    "policies": """
        SELECT tablename, policyname, permissive, roles::text, cmd, qual, with_check
        FROM pg_policies WHERE schemaname = 'public' ORDER BY 1, 2""",
    "triggers": """
        SELECT tgrelid::regclass::text, tgname, tgenabled::text, pg_get_triggerdef(oid)
        FROM pg_trigger WHERE NOT tgisinternal ORDER BY 1, 2""",
    "functions": f"""
        SELECT p.proname, pg_get_functiondef(p.oid), pg_get_userbyid(p.proowner),
               p.prosecdef, p.proacl::text
        FROM pg_proc p
        WHERE p.pronamespace = 'public'::regnamespace AND {_NOT_EXTENSION.format(oid="p.oid")}
        ORDER BY 1, 2""",
    "extensions": "SELECT extname FROM pg_extension ORDER BY 1",
    "schema_migrations": "SELECT version FROM schema_migrations ORDER BY 1",
    "audit_kinds": "SELECT kind FROM audit_kinds ORDER BY 1",
}


def dump(conn: psycopg.Connection) -> dict[str, list[tuple]]:
    return {name: conn.execute(query).fetchall() for name, query in QUERIES.items()}


def difference(left: dict[str, list[tuple]], right: dict[str, list[tuple]]) -> list[str]:
    """One line per row that only one side has, so a failure names the object."""
    lines = []
    for section, rows in left.items():
        only_left = [row for row in rows if row not in right[section]]
        only_right = [row for row in right[section] if row not in rows]
        lines += [f"{section}: only in the first: {row}" for row in only_left]
        lines += [f"{section}: only in the second: {row}" for row in only_right]
    return lines
