# Connector health and self-repair

Status: **target** (roadmap, D-16). Nothing in this document is built. The system that exists is described in [architecture.md](../architecture.md); the built / not built ledger is [status.md](../status.md).

Custom connectors (D-15) are untested when they're added, so they will fail. This document describes how failures are caught and, later, fixed by a coding agent working inside strict limits.

## Which failures can be fixed with code

| Cause | Route |
|---|---|
| Wrong manifest: field mapping, schema, missing idempotency declaration | Coding agent |
| Bug in a connector server we own | Coding agent |
| Bug in a vendor's or tenant's MCP server | Fix-up layer in our wrapper if possible, otherwise report to the owner. We never change code we don't own |
| Expired login, missing permission, rate limit, target system down | Admin notification or normal retries. Never the coding agent |

## RemediationWorkflow

```
Connector failures ──signal-with-start──▶ RemediationWorkflow
                                            ID = {cell}:remediate:{connector}:{failure_signature}
  1. Circuit breaker: pause writes on the connector (reads continue), notify the tenant admin
  2. Classify the cause → not fixable with code? route as in the table above and stop
  3. Build a redacted failure bundle (see below)
  4. Coding agent activity: long timeout, heartbeats, runs in a sandbox, pushes to a branch
  5. Verify: contract tests, replay of recorded redacted traffic, read-only checks against the live system
  6. Human approval (signal; may wait days): tenant admin for a custom connector, our team for a certified one
  7. Canary rollout → watch error rate on a durable timer → promote, or roll back automatically
  8. Re-enable writes; ask users whose tasks were stuck to approve again
```

Using the failure signature (connector, tool, error class, and the shape of the request) in the workflow ID means many identical failures start **one** remediation. Later failures with the same signature are signals to the existing workflow and only add to its evidence.

## Rules
- **Failed writes are never replayed automatically.** A write that failed may have partly succeeded. After a fix, stuck tasks resume only after the requester approves again.
- **Safety fields are locked.** The coding agent can't change risk class, approval policy, grants, or idempotency declarations. A change to any of them fails verification and needs a human to make it.
- **Tenant isolation.** The failure bundle contains error messages, schemas, and the *shape* of requests and responses, with values redacted and no record contents. A fix for a custom connector deploys only to the tenant that owns it. Sharing it with others means promoting it to a certified connector, which we review.
- **The agent never deploys.** Only the pipeline deploys, with credentials the agent never has, and only after verification and approval.

## Boundaries for the coding agent
- It runs in a **separate platform-operations cell and cloud account**, never in a tenant cell.
- Each run gets a **throwaway sandbox** (a microVM or gVisor) that is destroyed afterwards.
- **No production credentials.** Network access is limited to the git host, a package mirror, and sandbox or recorded test targets.
- **Read** access: the redacted failure bundle and connector source. **Write** access: its own branch only.
- Every run is recorded (prompt, actions, diff, test results) in the platform audit log.

## Staged rollout
1. **v2:** classification, circuit breaker, and failure reports to the admin. No coding agent yet.
2. **v3:** the coding agent proposes fixes; a human approves every deploy.
3. **Later:** automatic deploy only for manifest-only fixes to `read` tools, once the record of approved fixes supports it. Fixes to write tools always need a human.
