#!/bin/sh
# Serve on the port the platform injects, then pull the embedding model only
# when the volume does not hold it yet. The pull needs a running server, so the
# server starts first and the script ends by waiting on it (its exit code is
# this script's exit code). A failed pull exits non-zero and the restart policy
# tries again; a present model is never pulled again.
set -eu
: "${OLLAMA_MODELS:=/data/models}"
export OLLAMA_MODELS
export OLLAMA_HOST="0.0.0.0:${PORT:-11434}"
ollama serve &
server=$!
trap 'kill "$server" 2>/dev/null || true' TERM INT
until ollama list >/dev/null 2>&1; do
  kill -0 "$server" 2>/dev/null || break
  sleep 1
done
if kill -0 "$server" 2>/dev/null; then
  if ! ollama show bge-m3 >/dev/null 2>&1; then
    ollama pull bge-m3
  fi
fi
wait "$server"
