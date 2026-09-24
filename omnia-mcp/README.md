# Omnia MCP Server

Exposes your **live Life-Omnia data** to Claude Code, **read-only**, so you can
strategize over your real tasks/lists (and, via introspection, pipeline/contacts)
on demand. Operator tool — separate from the website's source code.

Tasks live in **Omnia Lists** (`omnia_lists` / `omnia_tasks`). No Todoist.

## Tools

| Tool | What it does |
|------|--------------|
| `get_lists()` | Your Omnia lists (To-do / Long Term), excluding archived |
| `get_tasks(status, list_name, due)` | Tasks, filtered — `status` open/done/all, `due` today/week/overdue |
| `list_tables()` | All tables (to discover pipeline/contacts/messages/etc.) |
| `describe_table(name)` | A table's columns + types |
| `query(sql)` | A single read-only `SELECT`/`WITH` (capped 100 rows) for domains without typed tools yet |

## Go live (≈15 min)

### 1. Create a read-only Neon role
In the Neon SQL editor for the **Life Omnia** project (`ep-spring-firefly`):

```sql
CREATE ROLE omnia_readonly WITH LOGIN PASSWORD 'choose-a-strong-password';
GRANT CONNECT ON DATABASE neondb TO omnia_readonly;
GRANT USAGE ON SCHEMA public TO omnia_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO omnia_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO omnia_readonly;
```

This role can **only read** — it physically cannot write or delete.

### 2. Configure `.env`
```bash
cd omnia-mcp
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```
Edit `.env`:
- `OMNIA_DB_DSN` — the pooled connection string for the `omnia_readonly` role (Neon dashboard → Connection Details → role = omnia_readonly, pooled).
- `OMNIA_USER_ID` — your Omnia `user_id`. Find it with:
  `SELECT id, email FROM users;` in the Neon SQL editor.

### 3. Register with Claude Code
```bash
claude mcp add omnia -- python /absolute/path/to/omnia-mcp/server.py
```
Or in `.mcp.json`:
```json
{ "mcpServers": { "omnia": { "command": "python", "args": ["/abs/path/omnia-mcp/server.py"] } } }
```

### 4. Test
In Claude Code: *"what's on my plate this week?"* → `get_tasks(due="week")` pulls live tasks. **Live.**

## Extending to pipeline / contacts / messages
Those tables exist in the same DB; we just haven't written typed tools yet.
Use `list_tables()` + `describe_table('omnia_deals')` (etc.) to see the real
columns, then add a typed tool in `omnia_client.py` + `server.py` the same way
`get_tasks` is built. Until then, `query("SELECT ... WHERE user_id = '<id>'")`
works read-only.

## Notes
- Everything is scoped to `OMNIA_USER_ID`. The read-only role + forced read-only
  transactions are two independent write guards.
- `.env` is gitignored. Don't commit it.
