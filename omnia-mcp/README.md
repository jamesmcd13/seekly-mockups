# Omnia MCP Server

Exposes your **live Life-Omnia data** to Claude Code: read tools (read-only role +
read-only transactions) plus a narrow set of scoped write tools. Operator tool —
separate from the website's source code.

To-dos live in **Shared Lists** (`shared_lists` / `shared_list_items`); Quick ToDo
is the default list. The old **Omnia Lists** (`omnia_lists` / `omnia_tasks`) were
retired on 2026-09-25 (hidden in the app): nothing writes to them any more, and
they are readable with `include_legacy=true`. No Todoist.

## Tools

| Tool | What it does |
|------|--------------|
| `get_lists(include_legacy)` | Your Shared Lists (title, project, open count, id; `[default]` = Quick ToDo, `[private]`) |
| `get_tasks(status, list_name, due, include_legacy)` | Shared-list to-dos with ids — `status` open/done/all, `due` today (incl. pinned)/week/overdue |
| `add_task(title, list_name, due_date, description, priority, pin_today, pin_focus, list_id, source)` | Add a to-do; empty list -> Quick ToDo; an unknown list is refused (never silently shared); "Top to Do Today" -> Today; filing into Today / Focus pins it |
| `add_todo(text, priority, pin_today, pin_focus, source)` | Add to Quick ToDo (P1 default) |
| `add_list(name, kind, project, source)` | Create a Shared List (dedupes by title) |
| `update_todo(kind, item_id, text, due_date, priority, note, list_id, list_title, pin_today, today_date, today_rank, pin_focus)` | Edit / move / pin one to-do (returns `Updated.`) |
| `complete_task(task_id)` / `delete_todo(kind, item_id)` | Check off (with planner done-sync) / delete one to-do; retired Omnia-Lists ids: complete only |
| `create_event(...)` | Not available (raises): no safe service path for calendar create yet |
| `list_tables()` | All tables (to discover pipeline/contacts/messages/etc.) |
| `describe_table(name)` | A table's columns + types |
| `query(sql)` | A single read-only `SELECT`/`WITH` (capped 100 rows) for domains without typed tools yet |
| `get_shared_lists(query)` | Shared Lists (id, title, project, open count) + `today_list_id` / `quick_todo_list_id` |
| `add_shared_item(list_title, text, priority, due_date, list_id, pin_today)` | Add to a shared list by id or title; `pin_today` puts it in the TODAY band |
| `pin_shared_item_today(item_id, pin)` | Pin / unpin an existing shared item to Today |
| `planner_get_day` / `planner_set_day` / `planner_add_task` / `planner_start` / `planner_stop` / `planner_snooze` / `planner_set_busy` | James's private planner (`james:private`): lanes A/B, timer runs, source-item done-sync. Mirrors omnia-platform `services/planner*.py` |

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
  transactions are two independent write guards for the READ tools; the write
  tools use the separate write DSN (`OMNIA_DB_DSN_RW`, else the backend .env).
- Every write stamps provenance in `shared_list_items.origin_by`: the `source`
  argument, else `$OMNIA_MCP_SOURCE`, else `brain:gv` inside omnia-gv-command,
  else `mcp:<tool>`.
- `tests/integration_shared_writers.py` exercises every writer against a Neon
  BRANCH (`OMNIA_TEST_DB_HOST=<branch host>`); it refuses the prod endpoint.
- `.env` is gitignored. Don't commit it.
