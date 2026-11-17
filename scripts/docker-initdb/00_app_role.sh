#!/bin/sh
# Creates the app role with the password from AGENTOS_APP_DB_PASSWORD. The Postgres image
# runs it once, on the first start of an empty data directory, before 10_init_db.sql.
# psql reads the password from its own environment: it is never in a file or an argument.
set -eu
: "${AGENTOS_APP_DB_PASSWORD:?AGENTOS_APP_DB_PASSWORD must be set}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv app_password AGENTOS_APP_DB_PASSWORD
CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD :'app_password';
SQL
