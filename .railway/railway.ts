// The whole hosted project as code: three app processes from the root
// Dockerfile, three infrastructure images from deploy/. Apply with the Railway
// CLI (railway config plan, then railway config apply); deploys do not read
// this file. Secrets are never written here: each is a project shared
// variable, set once in the dashboard, and a service gets only the ones it
// references as ${{shared.NAME}}. preserve() keeps an optional value set in
// that one service's variables (unset unless needed). Non-secret values and
// references are declared below; ${{VAR}} inside a string is Railway's reference syntax,
// resolved from the same service's variables at run time.
//
// Public domains for gateway and console are generated in the dashboard (a
// generated domain is not part of this file). postgres, temporal, ollama and
// worker have no public domain: they are reachable only over the private
// network at <service>.railway.internal.
import { defineRailway, github, preserve, project, service, volume } from "railway/iac";

const REPO = "itsbryanlai/agent-architecture";

// The app role's URL; the password is the shared AGENTOS_APP_DB_PASSWORD.
const APP_DATABASE_URL =
  "postgresql://agentos_app:${{shared.AGENTOS_APP_DB_PASSWORD}}@postgres.railway.internal:5432/agentos";
const TEMPORAL_ADDRESS = "temporal.railway.internal:7233";
// Railway records a volume's region at creation; declaring it keeps a later plan
// from reading the region as a removal.
const VOLUME_REGION = "us-west2";

export default defineRailway(() => {
  const postgresData = volume("postgres-data", { sizeMB: 5000, region: VOLUME_REGION });
  const temporalData = volume("temporal-data", { sizeMB: 1024, region: VOLUME_REGION });
  const ollamaModels = volume("ollama-models", { sizeMB: 5000, region: VOLUME_REGION });

  const postgres = service("postgres", {
    source: github(REPO, { rootDirectory: "deploy/postgres" }),
    build: { builder: "DOCKERFILE" },
    deploy: {
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      POSTGRES_USER: "agentos",
      POSTGRES_DB: "agentos",
      // The owner role's password; set it in the service's variables.
      POSTGRES_PASSWORD: "${{shared.AGENTOS_OWNER_DB_PASSWORD}}",
      PORT: "5432",
    },
    volumeMounts: { "/var/lib/postgresql/data": postgresData },
  });

  const temporal = service("temporal", {
    source: github(REPO, { rootDirectory: "deploy/temporal" }),
    build: { builder: "DOCKERFILE" },
    start: "temporal server start-dev --ip 0.0.0.0 --db-filename /data/temporal.db",
    deploy: {
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      PORT: "7233",
      // Railway mounts a volume root-owned and this image runs as the non-root
      // "temporal" user, so only this service runs as root on Railway. The app
      // image stays non-root and mounts no volume.
      RAILWAY_RUN_UID: "0",
    },
    volumeMounts: { "/data": temporalData },
  });

  const ollama = service("ollama", {
    source: github(REPO, { rootDirectory: "deploy/ollama" }),
    build: { builder: "DOCKERFILE" },
    // The entrypoint reads PORT and OLLAMA_MODELS from the environment.
    start: "/bin/sh /entrypoint.sh",
    deploy: {
      healthcheckPath: "/",
      healthcheckTimeout: 300,
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      PORT: "11434",
      OLLAMA_MODELS: "/data/models",
    },
    volumeMounts: { "/data": ollamaModels },
  });

  const worker = service("worker", {
    source: github(REPO),
    build: { builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: "python -m agentos.worker",
    // The only service that migrates: one schema writer per deploy. It runs
    // before the worker starts, with this service's variables; on the first
    // deploy it builds an empty database from scripts/init_db.sql. The gateway
    // and console refuse a behind schema at start-up, so deploy the worker
    // first when a change adds a migration.
    preDeploy: ["python scripts/migrate.py --set-app-password"],
    deploy: {
      preDeployTimeoutSeconds: 300,
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      AGENTOS_DATABASE_URL: APP_DATABASE_URL,
      AGENTOS_APP_DB_PASSWORD: "${{shared.AGENTOS_APP_DB_PASSWORD}}",
      // Read only by the pre-deploy command, never by the worker process.
      AGENTOS_OWNER_DATABASE_URL:
        "postgresql://agentos:${{shared.AGENTOS_OWNER_DB_PASSWORD}}@postgres.railway.internal:5432/agentos",
      AGENTOS_OWNER_DB_PASSWORD: "${{shared.AGENTOS_OWNER_DB_PASSWORD}}",
      AGENTOS_TEMPORAL_ADDRESS: TEMPORAL_ADDRESS,
      AGENTOS_EMBED_BASE_URL: "http://ollama.railway.internal:11434/v1",
      AGENTOS_EMBED_MODEL: "bge-m3",
      // Per-tenant payload keys come from variables, the same in the gateway
      // (docs/operations.md, Payload keys on a host without files).
      AGENTOS_KEY_SOURCE: "env",
      AGENTOS_PAYLOAD_KEY_ACME_V1: "${{shared.AGENTOS_PAYLOAD_KEY_ACME_V1}}",
      // The hosted chat model is Qwen qwen3.7-plus; the model must have an
      // entry in config/prices.yaml or the worker refuses to start. Anthropic
      // is supported too: AGENTOS_LLM_PROVIDER=anthropic with ANTHROPIC_API_KEY.
      AGENTOS_LLM_PROVIDER: "qwen",
      AGENTOS_MODEL: "qwen3.7-plus",
      ANTHROPIC_API_KEY: preserve(),
      QWEN_API_KEY: "${{shared.QWEN_API_KEY}}",
      QWEN_BASE_URL: "${{shared.QWEN_BASE_URL}}",
      SLACK_BOT_TOKEN: "${{shared.SLACK_BOT_TOKEN}}",
      NOTION_TOKEN: "${{shared.NOTION_TOKEN}}",
    },
  });

  const gateway = service("gateway", {
    source: github(REPO),
    build: { builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    // Dockerfile start commands run in exec form: the shell wrapper expands
    // PORT, and exec keeps uvicorn as the process the signals reach.
    start: '/bin/sh -c "exec uvicorn agentos.gateway.app:app --host 0.0.0.0 --port $PORT"',
    deploy: {
      healthcheckPath: "/healthz",
      healthcheckTimeout: 300,
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      AGENTOS_DATABASE_URL: APP_DATABASE_URL,
      AGENTOS_APP_DB_PASSWORD: "${{shared.AGENTOS_APP_DB_PASSWORD}}",
      AGENTOS_TEMPORAL_ADDRESS: TEMPORAL_ADDRESS,
      // The same payload keys as the worker; the Notion webhook token is
      // encrypted with them in the database, so the gateway needs no volume.
      AGENTOS_KEY_SOURCE: "env",
      AGENTOS_PAYLOAD_KEY_ACME_V1: "${{shared.AGENTOS_PAYLOAD_KEY_ACME_V1}}",
      SLACK_SIGNING_SECRET: "${{shared.SLACK_SIGNING_SECRET}}",
      SLACK_BOT_TOKEN: "${{shared.SLACK_BOT_TOKEN}}",
      NOTION_TOKEN: "${{shared.NOTION_TOKEN}}",
      // Optional local override; leave unset so the token lives in the
      // database (docs/operations.md, Notion webhook).
      NOTION_WEBHOOK_VERIFICATION_TOKEN: preserve(),
    },
  });

  const console = service("console", {
    source: github(REPO),
    build: { builder: "DOCKERFILE", dockerfilePath: "Dockerfile" },
    start: '/bin/sh -c "exec python -m agentos.control_api --host 0.0.0.0 --port $PORT"',
    deploy: {
      // /healthz bypasses the console's Host guard, so Railway's
      // healthcheck.railway.app probe passes.
      healthcheckPath: "/healthz",
      healthcheckTimeout: 300,
      restartPolicyType: "ON_FAILURE",
      restartPolicyMaxRetries: 10,
    },
    env: {
      AGENTOS_DATABASE_URL: APP_DATABASE_URL,
      AGENTOS_APP_DB_PASSWORD: "${{shared.AGENTOS_APP_DB_PASSWORD}}",
      // Sign-in with Slack is required to bind beyond loopback; all three
      // must be set. The console's Host and Origin allowlists are built from
      // AGENTOS_CONSOLE_URL, so it must be the exact public base URL.
      SLACK_CLIENT_ID: "${{shared.SLACK_CLIENT_ID}}",
      SLACK_CLIENT_SECRET: "${{shared.SLACK_CLIENT_SECRET}}",
      AGENTOS_CONSOLE_URL: "https://${{RAILWAY_PUBLIC_DOMAIN}}",
      NOTION_TOKEN: "${{shared.NOTION_TOKEN}}",
      SLACK_BOT_TOKEN: "${{shared.SLACK_BOT_TOKEN}}",
      // Optional: the "Choose from Notion" grant picker.
      NOTION_OAUTH_CLIENT_ID: "${{shared.NOTION_OAUTH_CLIENT_ID}}",
      NOTION_OAUTH_CLIENT_SECRET: "${{shared.NOTION_OAUTH_CLIENT_SECRET}}",
    },
  });

  return project("agent-architecture", {
    resources: [postgres, temporal, ollama, worker, gateway, console],
  });
});
