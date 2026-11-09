# Product

Status: draft · Last updated 2026-11-06

## One-line pitch

Agents that do real work inside your company: they file tickets, submit requests, and follow up over days. They live in Slack, and each one can only reach the access its creator granted it.

## What we are building

There are three product shapes in this space. **v1 is shape 2 with a shape-1 front door**, and the architecture is designed for shape 3 from day one.

| Shape | What it is | Comparable products | Where it sits |
|---|---|---|---|
| 1. Company assistant | One bot that knows everything *you* can access | Glean, ChatGPT Enterprise, Gemini Enterprise, Copilot | v1 front door; broadened on the roadmap |
| 2. Agent builder | Approved creators build specialist agents (HR helper, IT helpdesk, Sales assistant) | Copilot Studio, custom GPTs, Slack agents | **v1 core** |
| 3. Agents that do work | Agents that take actions, wait for approvals and events, and follow up over days | Moveworks, ServiceNow AI | Architecture target; v1 ships one write action to prove it |

**Why shape 3 is the differentiator.** Answering questions is quickly becoming a commodity. What's hard is doing things reliably: an action that must not run twice, an approval that comes back three days later, a follow-up that has to fire even after a deploy. Durable execution on Temporal is built for this, and it's the thing competitors find hardest to copy.

## Users

### 1. Employee (around 95% of users)
- Never opens a dashboard. For them the product is a Slack bot.
- Needs: correct answers with sources, actions that need one click and no form filling, follow-ups that actually come.

### 2. Agent creator (approved only)
- Typically a team lead or ops person in HR, IT, Sales ops, or Finance. An admin has to grant them the creator role.
- Configures: name, instructions, **access grants** (which knowledge scopes and actions the agent may use; only scopes the creator can access themselves), approval policy, and the **audience** (which channels and groups can reach it).
- Is accountable for the audience: when deploying, they confirm who will be able to see what the agent can access.
- Needs: a way to test before publishing, and a list of unanswered or failed questions after launch so they know what to fix.

### 3. Tenant admin (IT/security)
- Decides whether we get bought at all.
- Controls: which connectors are installed and the credentials they use, who can be a creator, approval of agents before they're published or given more access, allowed models, which actions may ever run without approval, data retention.
- Needs: an audit log (who asked, which agent, which identity was used, what was read or changed, who approved), cost per team, and the ability to turn everything off immediately (a kill switch).

## Core journeys (v1)

1. **First contact.** The employee DMs the front-door assistant or @mentions an agent in a channel where it's deployed. There's no setup: the agent already has the access its creator granted.
2. **Ask.** The agent answers from the sources it was granted and cites each one. Questions about the employee's own records ("my open requests", "my PTO balance") are answered where they are asked. The platform fills in who is asking, so "my" always means the asker. Answering in a channel is safe because the creator has confirmed that the channel's members may see what the agent can reach.
3. **Act.** The agent drafts the action (for example "Add to IT requests in Notion: laptop battery swelling, priority high") and shows it as a Slack card with *Submit*, *Edit*, and *Cancel*. Only the requesting user can approve it. The action runs exactly once.
4. **Follow up.** The agent keeps watching the request. Days later it posts in the same thread: "Your request was marked Done. Anything else?" Or: "No movement in 3 days. Want me to nudge the assignee?"
5. **Route.** The front-door assistant answers general questions itself and hands specialist ones to the right agent, saying which agent took over.

## Product principles

- **Meet people in Slack.** Never send an employee to a portal to finish something.
- **Agents have their own access, and someone answers for it.** The creator grants access (never more than they have themselves) and decides who can reach the agent. The admin approves.
- **Ask before changing anything.** Write actions need approval by default. Only admins can relax that, and only for each action separately.
- **Always show sources and identity.** Every answer is cited. Every action says whose account it used.
- **Every agent has an owner.** Like a Slack channel, an agent has a named owner who answers for it.
- **Durable by default.** An approval or follow-up survives restarts, deploys, and outages.

## v1 scope

- Slack only (one Slack app installed per workspace, which is one tenant).
- A front-door assistant plus agents built by approved creators.
- Read connectors: Notion, Confluence, and Google Drive (knowledge).
- **One write action:** add an item to a Notion database (such as a request tracker) or comment on one, with approval, followed by durable follow-up until its status reaches done.
- Admin console: connectors, creator roles, approval policy, audit log, usage and cost.
- Multi-tenant SaaS.

### Out of scope for v1
- Microsoft Teams, Telegram, web chat.
- Self-serve agent creation for every employee.
- Agents that run on schedules or external events without a user starting them.
- Customer-hosted (self-hosted) deployment.

## Roadmap

| Phase | Theme | Items |
|---|---|---|
| v1 | Prove agents can do work | See v1 scope above |
| v2 | More actions, more systems | More write connectors (Jira, ServiceNow, Workday/HRIS, Google Calendar); approvals by other people (manager approves an employee's request); multi-step actions that are undone if a later step fails (sagas); MCP-based custom connectors added by tenant admins; **connector health**: failures sorted by cause, writes paused automatically on a broken connector, failure reports to the admin |
| v3 | Proactive agents | Scheduled agents ("every Monday, summarize open P1s"); agents triggered by events (new Jira ticket, new hire in HRIS); agents that hand work to each other; **connector self-repair**: a sandboxed coding agent proposes fixes for connector failures, and a human approves before deploy |
| v4 | Broader assistant | Front door grows into a full company assistant (shape 1) across all connected sources |
| Later | Reach and deployment | Microsoft Teams, web portal, Telegram if a market needs it; self-serve agent creation with guardrails; dedicated or customer-hosted cells for regulated tenants; automatic deployment of connector fixes for low-risk changes once the track record supports it |
