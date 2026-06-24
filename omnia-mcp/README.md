# Omnia MCP Server

Exposes your **live Omnia data** (tasks, calendar, contexts, messages) to Claude Code
as on-demand tools, so you can strategize about your real life/Omnia with full context.

This is an **operator tool**. It reads your data only. It knows nothing about the
website's source code — keep it separate from your code repos.

## What it gives you in Claude Code

| Tool | What it pulls |
|------|---------------|
| `get_tasks(status, context, due)` | Your tasks, filtered — pulls one slice, not everything |
| `get_calendar(range)` | Calendar events for today / week / month / a date range |
| `search_contexts(query)` | Your folders/contexts (Work, a deal, etc.) |
| `get_messages(query, limit)` | Recent or matching inbox messages |

Each tool fetches **on demand** — nothing is bulk-loaded into context.

## Setup

```bash
cd omnia-mcp
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then fill in your API base URL + token
```

## Wire it to your API

Open `omnia_client.py` and fill in the 3 marked spots:
1. `.env` → `OMNIA_API_BASE_URL` and `OMNIA_API_TOKEN`
2. The auth header shape (`Authorization: Bearer …` vs `x-api-key`)
3. The real endpoint paths + how to read each response

That file is the only place tied to your real API.

## Register it with Claude Code

```bash
claude mcp add omnia -- python /absolute/path/to/omnia-mcp/server.py
```

Or add to `.mcp.json`:

```json
{
  "mcpServers": {
    "omnia": { "command": "python", "args": ["/absolute/path/to/omnia-mcp/server.py"] }
  }
}
```

Then in Claude Code the tools appear as `get_tasks`, `get_calendar`, etc. Your
`/omnia` skill should tell Claude to load `me.md` and use these tools to pull
specific context on demand — never to fetch everything up front.

## Next steps

- Add a one-page `me.md` (who you are, what Omnia is, current priorities).
- Point the existing `/omnia` skill at this server.
- Later: add **write** tools (`create_task`, `update_task`) if you want to act from Claude Code.
