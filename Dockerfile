# One image for the three app processes; the start command picks which:
#   worker:  python -m agentos.worker
#   gateway: /bin/sh -c "exec uvicorn agentos.gateway.app:app --host 0.0.0.0 --port $PORT"
#   console: /bin/sh -c "exec python -m agentos.control_api --host 0.0.0.0 --port $PORT"
# Secrets come only from the environment at run time; no layer carries one.

FROM ghcr.io/astral-sh/uv:0.11.23 AS uv

FROM python:3.12-slim-bookworm AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable
COPY config ./config
# The console reads the app's bot scopes from the manifest at start-up (the install).
COPY slack ./slack
COPY scripts/migrate.py ./scripts/migrate.py
COPY scripts/migrations ./scripts/migrations
COPY scripts/init_db.sql ./scripts/init_db.sql

FROM python:3.12-slim-bookworm
RUN useradd --create-home --uid 10001 agentos
WORKDIR /app
COPY --from=builder --chown=agentos:agentos /app /app
ENV PATH="/app/.venv/bin:$PATH"
USER agentos
