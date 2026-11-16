"""Rows the pg-tier tests insert directly."""

import psycopg


def seed_version(conn: psycopg.Connection, tenant: str, agent: str, status: str, version: int = 1):
    """An agent version in a given status, with the fields every status needs filled in."""
    conn.execute(
        "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%s, %s, 'u') "
        "ON CONFLICT DO NOTHING",
        (tenant, agent),
    )
    conn.execute(
        "INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition, "
        "created_by, submitted_by, approved_by, published_at, definition_sha256) "
        "VALUES (%s, %s, %s, %s, '{}', 'u', 'u', 'u', '2026-01-01', %s)",
        (tenant, agent, version, status, None if status == "draft" else "h"),
    )
