# Linear — Source of Truth for Tasks

Linear is where all work is tracked: bugs, features, improvements, spikes. Notion holds capability specs; Linear holds execution.

## Workspace

- **URL:** https://linear.app/tektii
- **Team:** `Tektii` (issue prefix `TEK-`)
- **Area label for this repo:** `area/gateway`

## Conventions for Agents

Every issue an agent creates or works on must follow these rules.

### Required labels

- Exactly one `area/*` label. For work in this repo, use `area/gateway`. For work spanning multiple repos, use `area/cross-repo`.
- On creation, add `agent/created`.
- On pickup, add `agent/in-progress`.
- On completion, add `needs-human-review` and remove `agent/in-progress`.

Because `area/gateway` is shared with the core OSS gateway repo (tektii-gateway), make it clear in the title whether the work targets the Python SDK (e.g. `[py-sdk] Fix auto-ACK race condition`).

### Status lifecycle (agents)

| From | To | When |
|------|----|----|
| Backlog / Todo | In Progress | Agent picks up a ticket |
| In Progress | In Review | Agent finishes work (add `needs-human-review`, drop `agent/in-progress`) |

Agents must NEVER:

- Move tickets to `Done` or `Canceled` — human decision only.
- Delete comments or attachments (blocked in MCP permissions).
- Close tickets outside the standard flow.

### Capability linking

If the work corresponds to a Notion capability, paste the Notion capability URL in the issue description. Do NOT create Linear projects or initiatives — the workspace intentionally uses issues + labels only.

## GitHub integration

Reference `TEK-123` in PR titles or branch names to auto-link. Linear shows the PR on the ticket and updates status on merge.

## MCP tool cheat sheet

Agents access Linear via the `linear` MCP server. Common tool calls below.

### Read

```
list_issues(team="Tektii", query="py-sdk")
list_issues(label="needs-human-review")
list_issue_statuses(team="Tektii")
list_issue_labels()
get_issue(query="TEK-123")
list_comments(issue="TEK-123")
search_documentation(query="…")
```

### Create a ticket

```
save_issue(
  team="Tektii",
  title="[py-sdk] Bug: AsyncEventStream leaks WebSocket on cancel",
  description="## Summary\n...\n## Repro\n1. ...",
  labels=["Bug", "area/gateway", "agent/created"],
  status="Todo",
  priority=2
)
```

### Pick up a ticket

```
save_issue(
  id="<issue-id>",
  status="In Progress",
  labels=[<existing-labels>, "agent/in-progress"]
)
save_comment(issue="<issue-id>", body="Picked up by agent. Approach: ...")
```

### Finish a ticket

```
save_issue(
  id="<issue-id>",
  status="In Review",
  labels=[<existing-minus-in-progress>, "needs-human-review"]
)
save_comment(issue="<issue-id>", body="Completed. PR: #123. Tests: 153/153 green.")
```

## Common queries

**What needs human attention?**

```
list_issues(label="needs-human-review", team="Tektii")
```

**What's ready for an agent to pick up?**

```
list_issues(team="Tektii", status="Todo", label="area/gateway", query="py-sdk")
```

**Is anyone else working on X?**

```
list_issues(query="<keyword>", status="In Progress")
```

## Scope note

Tickets labelled `area/gateway` span both the core OSS gateway (tektii-gateway) and this Python SDK. Use a `[py-sdk]` prefix in the title when the work is SDK-only. Work that requires changes in both the gateway and the SDK should also carry `area/cross-repo`.

For the canonical capability specs, see the parent repo `CLAUDE.md` which points to the Notion Capabilities DB.
