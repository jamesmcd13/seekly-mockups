"""
Read-only adapter to the Life-Omnia Neon Postgres DB.

To-dos live in SHARED LISTS (shared_lists / shared_list_items). The old Omnia
Lists (omnia_lists / omnia_tasks) were retired on 2026-09-25 (hidden in the app,
read-only); get_lists / get_tasks read them only with include_legacy=True.

To run it you supply two things in .env (never in code/chat):
  OMNIA_DB_DSN    postgresql://<readonly-user>:<pw>@<host>/<db>?sslmode=require
  OMNIA_USER_ID   your Omnia user_id (RLS/tenant key; everything is scoped to it)

Safety: use a READ-ONLY Neon role (SQL to create one is in the README). The
connection also forces read-only transactions as a second guard.
"""

from __future__ import annotations

import os
from pathlib import Path
import asyncpg
from dotenv import load_dotenv

# Load .env from THIS file's folder, regardless of the process cwd (Claude Code
# launches the MCP server from the project dir, not omnia-mcp/).
load_dotenv(Path(__file__).resolve().parent / ".env")

DSN = os.environ["OMNIA_DB_DSN"]
USER_ID = os.environ["OMNIA_USER_ID"]

_pool: asyncpg.Pool | None = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DSN,
            min_size=1,
            max_size=4,
            # second guard on top of the read-only DB role
            server_settings={"default_transaction_read_only": "on"},
        )
    return _pool


async def _rows(sql: str, *args) -> list[asyncpg.Record]:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


# --- Lists / Tasks: SHARED LISTS (the old Omnia Lists are retired) ------------
# Planner PR #432 (2026-09-25) hid omnia_lists / omnia_tasks in the app and
# every writer now targets Shared Lists, so these readers return Shared Lists:
# the shared workspace (USER_ID, which Michael can also see) plus James's
# private lists workspace once private mode exists. Never the planner
# workspace. The retired lists are still readable with include_legacy=True.

LISTS_PRIVATE_WS = f"{USER_ID}:lists:private"
_LIST_WORKSPACES = [USER_ID, LISTS_PRIVATE_WS]


def _like(q: str) -> str:
    """A literal ILIKE contains-pattern (% and _ escaped; pair with ESCAPE '\\')."""
    esc = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{esc}%"


def _ws_tag(ws: str) -> str:
    return "private" if (ws or "").endswith(":private") else ""


async def _legacy_lists() -> list[str]:
    rows = await _rows(
        """
        SELECT name, kind, is_template
        FROM omnia_lists
        WHERE user_id = $1 AND NOT archived
        ORDER BY kind, name
        """,
        USER_ID,
    )
    return [f"- {r['name']} ({r['kind']}){' [template]' if r['is_template'] else ''}"
            for r in rows]


async def get_lists(include_legacy: bool = False) -> str:
    rows = await _rows(
        r"""
        SELECT l.id, l.title, l.workspace_id, l.is_quick_default, p.title AS project,
               (SELECT count(*) FROM shared_list_items i
                 WHERE i.list_id = l.id AND i.workspace_id = l.workspace_id
                   AND i.parent_item_id IS NULL AND NOT i.done) AS open_items
          FROM shared_lists l
          LEFT JOIN shared_projects p
            ON p.id = l.project_id AND p.workspace_id = ANY($1::text[])
         WHERE l.workspace_id = ANY($1::text[]) AND NOT l.archived
         ORDER BY l.is_quick_default DESC, (lower(l.title) = 'today') DESC,
                  (l.workspace_id = $2) DESC, p.title NULLS FIRST, l.position, l.title
        """,
        _LIST_WORKSPACES, USER_ID,
    )
    out = ["Shared Lists (every to-do lives here; Quick ToDo is the default):"]
    for r in rows:
        tags = [t for t in ("default" if r["is_quick_default"] else "",
                            _ws_tag(r["workspace_id"])) if t]
        proj = f" · {r['project']}" if r["project"] else ""
        tag_s = f" [{', '.join(tags)}]" if tags else ""
        out.append(f"- {r['title']}{proj} · {r['open_items']} open{tag_s} [id {r['id']}]")
    if len(out) == 1:
        out.append("(no shared lists)")
    if include_legacy:
        legacy = await _legacy_lists()
        out.append("")
        out.append("Retired Omnia Lists (hidden in the app, read-only):")
        out.extend(legacy or ["(none)"])
    return "\n".join(out)


async def _legacy_tasks(status: str, list_name: str | None, due: str | None) -> list[str]:
    where = ["t.user_id = $1"]
    args: list = [USER_ID]
    if status == "open":
        where.append("t.status <> 'done'")
    elif status == "done":
        where.append("t.status = 'done'")
    if due == "today":
        where.append("t.due_date <= CURRENT_DATE")
    elif due == "week":
        where.append("t.due_date <= CURRENT_DATE + INTERVAL '7 days'")
    elif due == "overdue":
        where.append("t.due_date < CURRENT_DATE AND t.status <> 'done'")
    if list_name:
        args.append(_like(list_name))
        where.append(f"l.name ILIKE ${len(args)} ESCAPE '\\'")
    rows = await _rows(
        f"""
        SELECT t.id, t.name AS title, t.due_date, t.priority, t.status, t.rrule, l.name AS list
        FROM omnia_tasks t
        LEFT JOIN omnia_lists l ON l.id = t.list_id AND l.user_id = t.user_id
        WHERE {' AND '.join(where)}
        ORDER BY t.due_date NULLS LAST
        LIMIT 200
        """,
        *args,
    )
    out = []
    for r in rows:
        lst = f" [{r['list']}]" if r["list"] else ""
        due_s = f" (due {r['due_date']})" if r["due_date"] else ""
        pri = f" !P{r['priority']}" if r["priority"] not in (None, 0) else ""
        rec = " ↻" if r["rrule"] else ""
        done = " ✓" if r["status"] == "done" else ""
        out.append(f"- {r['title']}{lst}{due_s}{pri}{rec}{done} [id {r['id']}]")
    return out


async def get_tasks(status: str = "open", list_name: str | None = None,
                    due: str | None = None, include_legacy: bool = False) -> str:
    """Shared Lists to-dos (top-level items). status open|done|all; list_name =
    partial list title; due today (due today or earlier, or pinned to Today) |
    week | overdue. Each line ends with the item id."""
    where = ["i.workspace_id = ANY($1::text[])", "NOT l.archived", "i.parent_item_id IS NULL"]
    args: list = [_LIST_WORKSPACES]
    if status == "open":
        where.append("NOT i.done")
    elif status == "done":
        where.append("i.done")
    # due_date is a calendar day stored as a timestamptz at UTC midnight; the
    # app reads its UTC date part. "Today" is the Pacific day.
    day_sql = "(i.due_date AT TIME ZONE 'UTC')::date"
    today_sql = "(now() AT TIME ZONE 'America/Los_Angeles')::date"
    if due == "today":
        where.append(f"({day_sql} <= {today_sql} OR i.today_pinned)")
    elif due == "week":
        where.append(f"{day_sql} <= {today_sql} + 7")
    elif due == "overdue":
        where.append(f"{day_sql} < {today_sql} AND NOT i.done")
    if list_name:
        args.append(_like(list_name))
        where.append(f"l.title ILIKE ${len(args)} ESCAPE '\\'")
    rows = await _rows(
        f"""
        SELECT i.id, i.text, {day_sql} AS due, i.priority, i.done, i.today_pinned,
               i.is_focus, l.title AS list, l.workspace_id
          FROM shared_list_items i
          JOIN shared_lists l ON l.id = i.list_id AND l.workspace_id = i.workspace_id
         WHERE {' AND '.join(where)}
         ORDER BY i.done, i.due_date NULLS LAST, i.priority NULLS LAST, i.rank NULLS LAST,
                  i.created_at
         LIMIT 200
        """,
        *args,
    )
    out = []
    for r in rows:
        where_s = r["list"] + (", private" if _ws_tag(r["workspace_id"]) else "")
        due_s = f" (due {r['due']})" if r["due"] else ""
        pri = f" !P{r['priority']}" if r["priority"] else ""
        pins = (" · Today" if r["today_pinned"] else "") + (" · Focus" if r["is_focus"] else "")
        done = " ✓" if r["done"] else ""
        out.append(f"- {r['text']} [{where_s}]{due_s}{pri}{pins}{done} [id {r['id']}]")
    if not out:
        out = ["No to-dos match that filter."]
    if include_legacy:
        legacy = await _legacy_tasks(status, list_name, due)
        out.append("")
        out.append("Retired Omnia Lists (hidden in the app, read-only; complete_task still works):")
        out.extend(legacy or ["(none)"])
    return "\n".join(out)


# --- Events / Contacts (typed, real schema) ----------------------------------

async def get_events(days_ahead: int = 7) -> str:
    """Upcoming calendar events (omnia_events) from now to now+days_ahead,
    soonest first. Scoped to this user_id, same as get_tasks."""
    try:
        days = int(days_ahead)
    except (TypeError, ValueError):
        days = 7
    if days < 1:
        days = 1
    rows = await _rows(
        """
        SELECT name AS title, start_at, end_at, all_day, location, description
        FROM omnia_events
        WHERE user_id = $1
          AND start_at >= now()
          AND start_at < now() + make_interval(days => $2)
        ORDER BY start_at
        LIMIT 200
        """,
        USER_ID, days,
    )
    if not rows:
        return f"No events in the next {days} day(s)."
    out = []
    for r in rows:
        when = f"{r['start_at']}"
        if r["end_at"]:
            when += f" to {r['end_at']}"
        if r["all_day"]:
            when += " (all day)"
        loc = f" @ {r['location']}" if r["location"] else ""
        note = f" ({r['description']})" if r["description"] else ""
        out.append(f"- {r['title']}: {when}{loc}{note}")
    return "\n".join(out)


async def get_contacts(query: str | None = None, limit: int = 20) -> str:
    """Contacts (name, phone, email) for this user, excluding soft-deleted rows.
    If `query` is given, case-insensitive contains-match on name/email/phone (the
    LIKE metacharacters % and _ are escaped). Scoped to this user_id."""
    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = 20
    lim = max(1, min(lim, 200))
    where = ["user_id = $1", "deleted_at IS NULL"]
    args: list = [USER_ID]
    q = (query or "").strip()
    if q:
        # Escape LIKE wildcards so e.g. "50%" or "a_b" match literally.
        esc = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        args.append(f"%{esc}%")
        i = len(args)
        where.append(
            f"(name ILIKE ${i} ESCAPE '\\' "
            f"OR primary_email ILIKE ${i} ESCAPE '\\' "
            f"OR phone ILIKE ${i} ESCAPE '\\')"
        )
    args.append(lim)
    sql = f"""
        SELECT name, phone, primary_email
        FROM contacts
        WHERE {' AND '.join(where)}
        ORDER BY name NULLS LAST
        LIMIT ${len(args)}
    """
    rows = await _rows(sql, *args)
    if not rows:
        return "No matching contacts." if q else "No contacts found."
    out = []
    for r in rows:
        phone = f" | {r['phone']}" if r["phone"] else ""
        email = f" | {r['primary_email']}" if r["primary_email"] else ""
        out.append(f"- {r['name'] or '(no name)'}{phone}{email}")
    return "\n".join(out)


# --- Generic introspection (to extend to pipeline/contacts/etc. safely) ------

async def list_tables() -> str:
    rows = await _rows(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """
    )
    return "\n".join(f"- {r['table_name']}" for r in rows) or "(no tables)"


async def describe_table(name: str) -> str:
    rows = await _rows(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position
        """,
        name,
    )
    if not rows:
        return f"No table named '{name}' in public schema."
    return "\n".join(f"- {r['column_name']}: {r['data_type']}" for r in rows)


async def run_select(sql: str) -> str:
    """Run a read-only SELECT/WITH query. Rejects anything else."""
    stripped = sql.strip().rstrip(";").lstrip()
    head = stripped[:6].lower()
    if not (head.startswith("select") or head.startswith("with")):
        return "Only SELECT/WITH queries are allowed."
    if ";" in stripped:
        return "One statement only — remove the semicolon."
    rows = await _rows(stripped)
    if not rows:
        return "(0 rows)"
    cols = list(rows[0].keys())
    out = [" | ".join(cols)]
    for r in rows[:100]:
        out.append(" | ".join("" if r[c] is None else str(r[c]) for c in cols))
    if len(rows) > 100:
        out.append(f"... ({len(rows)} rows, showing 100)")
    return "\n".join(out)
