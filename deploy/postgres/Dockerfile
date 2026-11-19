# Postgres 16 with pgvector, the same image the local Compose file uses.
# The app role and the schema come from scripts/migrate.py (the worker's
# pre-deploy command, with --set-app-password): on an empty database it builds
# the schema from scripts/init_db.sql and records every migration as applied;
# on an existing one it applies the migrations the database lacks.
# See docs/operations.md, Railway.
FROM pgvector/pgvector:0.8.1-pg16
