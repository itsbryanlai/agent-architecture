# Ollama, pinned. Private only: no public domain. The entrypoint serves on the
# injected PORT and pulls bge-m3 on the first start; models live on the /data
# volume (OLLAMA_MODELS=/data/models), so a restart never pulls again.
FROM ollama/ollama:0.40.16
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
