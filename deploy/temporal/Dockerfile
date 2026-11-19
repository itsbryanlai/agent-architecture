# The dev server with SQLite on a volume, as the local Compose file runs it.
# Private only: no public domain. The database file lives on the /data volume.
FROM temporalio/temporal:1.9.1
# The image runs as the non-root "temporal" user. A local Docker volume takes
# this directory's ownership on first use; Railway mounts a volume root-owned
# (see docs/operations.md, Railway).
USER root
RUN mkdir /data && chown temporal:temporal /data
USER temporal
# The start command is set per service; clear the base image's entrypoint so
# the command runs exactly as written.
ENTRYPOINT []
CMD ["temporal", "server", "start-dev", "--ip", "0.0.0.0", "--db-filename", "/data/temporal.db"]
