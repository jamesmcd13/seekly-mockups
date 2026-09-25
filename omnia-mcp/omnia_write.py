"""
Read-WRITE adapter to the Life-Omnia Neon Postgres DB.

Companion to omnia_client.py (which stays strictly read-only). This module holds
the *write* helpers used by the write tools in server.py — add a task, complete a
task, create a calendar event, add a contact. Everything is scoped to a single
user_id and the operations are deliberately narrow.

SAFETY (matches James's production-DB rules):
  - INSERT + tightly-scoped single-row UPDATE + tightly-scoped single-row DELETE.
    Every DELETE/UPDATE is keyed by a specific id AND scoped by user_id/workspace_id,
    so the blast radius is exactly one row the caller owns. NO bulk delete, NO
    unscoped DELETE/UPDATE, NO DDL, NO TRUNCATE — not anywhere in this file.
  - Contact "delete" is a SOFT delete (sets deleted_at), never a hard wipe.
  - Every statement is scoped by user_id / workspace_id.
  - Writes are reversible via Neon point-in-time restore.
  - Deletes/edits sit behind the GV brain's DELETE-confirm gate (James echoes the
    exact target and replies the literal word DELETE). This module just executes.

Creds (never in code/chat):
  Prefers OMNIA_DB_DSN_RW; else DATABASE_URL / OMNIA_DATABASE_URL; else reads
  DATABASE_URL from the Omnia backend .env. The DSN is never printed.
  user_id: OMNIA_USER_ID (same tenant key the read side uses).
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

USER_ID = os.environ["OMNIA_USER_ID"]

# Fallback path to the Omnia backend .env (same one omnia_pages.py reads).
_BACKEND_ENV = Path(os.environ.get(
    "OMNIA_BACKEND_ENV",
    r"C:\Users\James\dev\omnia-platform\backend\.env",
))


def _dsn_from_backend_env() -> str | None:
    if not _BACKEND_ENV.exists():
        return None
    for line in _BACKEND_ENV.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        for key in ("DATABASE_URL=", "OMNIA_DATABASE_URL="):
            if line.startswith(key):
                return line[len(key):].strip().strip('"').strip("'")
    return None


def _normalize_dsn(url: str) -> str:
    """Make a SQLAlchemy-style URL safe for asyncpg and ensure SSL for Neon."""
    # Strip driver suffix: postgresql+asyncpg:// / postgres+psycopg:// -> postgresql://
    url = re.sub(r"^postgres(ql)?\+[a-z0-9]+://", "postgresql://", url)
    url = re.sub(r"^postgres://", "postgresql://", url)
    # asyncpg doesn't understand libpq channel_binding; drop it. Rebuild the query
    # string so "?channel_binding=require&sslmode=require" can't become "?&sslmode"
    # (asyncpg rejects an empty query field).
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != "channel_binding"]
    if not any(k == "sslmode" for k, _ in query):
        query.append(("sslmode", "require"))
    return urlunsplit(parts._replace(query=urlencode(query)))


def _rw_dsn() -> str:
    raw = (
        os.environ.get("OMNIA_DB_DSN_RW")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("OMNIA_DATABASE_URL")
        or _dsn_from_backend_env()
    )
    if not raw:
        raise RuntimeError(
            "No write DSN found. Set OMNIA_DB_DSN_RW in omnia-mcp/.env, or ensure "
            "the Omnia backend .env has DATABASE_URL."
        )
    return _normalize_dsn(raw)


_pool: asyncpg.Pool | None = None
_pool_loop = None


async def _get_pool() -> asyncpg.Pool:
    """One pool per event loop: a pool is bound to the loop that made it, and
    scripts (dennis_report.py) call asyncio.run() once per day."""
    global _pool, _pool_loop
    loop = asyncio.get_running_loop()
    if _pool is None or _pool_loop is not loop:
        _pool = await asyncpg.create_pool(_rw_dsn(), min_size=1, max_size=3)
        _pool_loop = loop
    return _pool


# --- list resolution ---------------------------------------------------------

async def _resolve_list(conn: asyncpg.Connection, list_name: str):
    """Return (list_id, resolved_name) or raise ValueError with guidance."""
    rows = await conn.fetch(
        """
        SELECT id, name FROM omnia_lists
        WHERE user_id = $1 AND NOT archived AND name ILIKE $2
        ORDER BY (lower(name) = lower($3)) DESC, position
        """,
        USER_ID, f"%{list_name}%", list_name,
    )
    if not rows:
        raise ValueError(
            f"No list matching '{list_name}'. Use add_list first, or pick an existing one."
        )
    exact = [r for r in rows if r["name"].lower() == list_name.lower()]
    if len(exact) == 1:
        return exact[0]["id"], exact[0]["name"]
    if len(exact) > 1:
        # Duplicate list names — cannot pick safely; caller must use the id.
        ids = ", ".join(f"{r['name']} (id {r['id']})" for r in exact)
        raise ValueError(
            f"'{list_name}' matches {len(exact)} lists with that exact name: {ids}. "
            f"Resolve the duplicate in Omnia, or add via the specific list_id."
        )
    if len(rows) == 1:
        return rows[0]["id"], rows[0]["name"]
    opts = ", ".join(f"'{r['name']}'" for r in rows)
    raise ValueError(f"'{list_name}' is ambiguous — matches: {opts}. Be more specific.")


# --- write operations --------------------------------------------------------

async def add_task(title: str, list_name: str, due_date: str | None = None,
                   description: str | None = None) -> str:
    if not title.strip():
        return "Task title is empty."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        try:
            list_id, list_disp = await _resolve_list(conn, list_name)
        except ValueError as e:
            return str(e)
        due_val = None
        if due_date:
            try:
                due_val = date.fromisoformat(due_date.strip())
            except ValueError:
                return f"due_date must be YYYY-MM-DD, got '{due_date}'."
        tid = uuid.uuid4()
        pos_row = await conn.fetchrow(
            "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM omnia_tasks "
            "WHERE user_id = $1 AND list_id = $2",
            USER_ID, list_id,
        )
        await conn.execute(
            """
            INSERT INTO omnia_tasks
                (id, user_id, list_id, name, description, due_date, status,
                 position, source, source_agent, created_by, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,'todo',$7,'omnia','personal','claude',now(),now())
            """,
            tid, USER_ID, list_id, title.strip(), description,
            due_val, pos_row["p"],
        )
    due_s = f" (due {due_date})" if due_date else ""
    return f"Added task '{title.strip()}' to list '{list_disp}'{due_s}. [id {tid}]"


async def add_shared_todo(text: str, priority: int = 1) -> str:
    """Add a to-do to the DEFAULT shared quick list (the one flagged
    is_quick_default, e.g. 'Quick ToDo'), at the given priority.

    This is the default landing spot for an unqualified "add a to-do" — a shared
    list, not a personal one. priority is Omnia's smallint 1..5 where 1 = P1
    (highest); values outside that range are clamped to 1.
    """
    if not text.strip():
        return "To-do text is empty."
    try:
        pr = int(priority)
    except (TypeError, ValueError):
        pr = 1
    if pr < 1 or pr > 5:
        pr = 1
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, title FROM shared_lists "
            "WHERE workspace_id = $1 AND is_quick_default = true AND archived = false "
            "ORDER BY created_at LIMIT 1",
            USER_ID,
        )
        if row is None:  # fallback if the quick-default flag is ever cleared
            row = await conn.fetchrow(
                "SELECT id, title FROM shared_lists "
                "WHERE workspace_id = $1 AND archived = false "
                "AND (title ILIKE 'Quick ToDo' OR title ILIKE 'Master To-Do') "
                "ORDER BY (title ILIKE 'Quick ToDo') DESC, created_at LIMIT 1",
                USER_ID,
            )
        if row is None:
            return "No shared to-do list found to add into."
        list_id, list_title = row["id"], row["title"]
        pos_row = await conn.fetchrow(
            "SELECT COALESCE(MAX(position), -1) + 1 AS p FROM shared_list_items WHERE list_id = $1",
            list_id,
        )
        iid = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO shared_list_items
                (id, list_id, workspace_id, text, priority, position,
                 created_by, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,'claude',now(),now())
            """,
            iid, list_id, USER_ID, text.strip(), pr, pos_row["p"],
        )
    return f"Added to shared list '{list_title}' at P{pr}: '{text.strip()}'. [id {iid}]"


# --- resolve helpers: find the exact rows a delete phrase refers to ------------
# Read-only. Used by the backup watcher's Python delete path to turn a phrase like
# "the dentist reminder" into concrete ids to echo + delete, without handing the
# headless model any raw-SQL or delete tools.

def _like_arg(query: str) -> str:
    """Wrap a user string as a literal ILIKE contains-pattern: the LIKE
    metacharacters % and _ are escaped so 'delete %' cannot match everything.
    Pair with `ESCAPE '\\'` in the query."""
    esc = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{esc}%"


async def find_todos(query: str, limit: int = 25) -> list[dict]:
    """Open to-dos (omnia_tasks + shared_list_items) whose text matches `query`.
    Returns [{kind:'task'|'shared', id, label}]. Scoped to this user/workspace."""
    q = (query or "").strip()
    if not q:
        return []
    like = _like_arg(q)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            r"""
            SELECT 'task' AS kind, id::text AS id, name AS label
              FROM omnia_tasks
             WHERE user_id = $1 AND status <> 'done' AND name ILIKE $2 ESCAPE '\'
            UNION ALL
            SELECT 'shared' AS kind, id::text, text
              FROM shared_list_items
             WHERE workspace_id = $1 AND done = false AND text ILIKE $2 ESCAPE '\'
             LIMIT $3
            """,
            USER_ID, like, limit,
        )
    return [{"kind": r["kind"], "id": r["id"], "label": r["label"]} for r in rows]


async def find_contacts(query: str, limit: int = 25) -> list[dict]:
    """Active contacts whose name or email matches `query`.
    Returns [{id, label}]. Scoped to this user; excludes soft-deleted."""
    q = (query or "").strip()
    if not q:
        return []
    like = _like_arg(q)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            r"SELECT id::text AS id, name, primary_email FROM contacts "
            r"WHERE user_id = $1 AND deleted_at IS NULL "
            r"AND (name ILIKE $2 ESCAPE '\' OR primary_email ILIKE $2 ESCAPE '\') LIMIT $3",
            USER_ID, like, limit,
        )
    return [{"id": r["id"],
             "label": r["name"] or r["primary_email"] or r["id"]} for r in rows]


# --- edit / delete: single-row, scoped, gated behind the GV DELETE-confirm ------

def _clamp_priority(priority):
    try:
        pr = int(priority)
    except (TypeError, ValueError):
        return None
    return pr if 1 <= pr <= 5 else None


async def delete_todo(kind: str, item_id: str) -> str:
    """Hard-delete ONE to-do the caller owns. kind='task' (omnia_tasks, keyed by
    user_id) or kind='shared' (shared_list_items, keyed by workspace_id). FK
    children (subtasks, completions) cascade. Reversible via Neon PITR."""
    if kind not in ("task", "shared"):
        return "kind must be 'task' or 'shared'."
    try:
        iid = uuid.UUID(str(item_id))
    except ValueError:
        return "item_id must be a valid UUID."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if kind == "task":
            row = await conn.fetchrow(
                "SELECT name FROM omnia_tasks WHERE user_id=$1 AND id=$2", USER_ID, iid)
            if row is None:
                return "No such task for this user."
            await conn.execute(
                "DELETE FROM omnia_tasks WHERE user_id=$1 AND id=$2", USER_ID, iid)
            return f"Deleted task '{row['name']}'."
        row = await conn.fetchrow(
            "SELECT text FROM shared_list_items WHERE workspace_id=$1 AND id=$2", USER_ID, iid)
        if row is None:
            return "No such shared to-do for this workspace."
        await conn.execute(
            "DELETE FROM shared_list_items WHERE workspace_id=$1 AND id=$2", USER_ID, iid)
        return f"Deleted to-do '{row['text']}'."


async def update_todo(kind: str, item_id: str, text: str | None = None,
                      due_date: str | None = None, priority: int | None = None) -> str:
    """Edit ONE to-do the caller owns (rename / reschedule / re-prioritize). Only
    the provided fields change. kind='task' or 'shared'. Column names below are
    hard-coded literals; only values are ever parameterized."""
    if kind not in ("task", "shared"):
        return "kind must be 'task' or 'shared'."
    try:
        iid = uuid.UUID(str(item_id))
    except ValueError:
        return "item_id must be a valid UUID."
    text_col = "name" if kind == "task" else "text"
    scope_col = "user_id" if kind == "task" else "workspace_id"
    table = "omnia_tasks" if kind == "task" else "shared_list_items"

    sets, args = [], []
    if text is not None and text.strip():
        args.append(text.strip()); sets.append(f"{text_col}=${len(args)}")
    if due_date is not None and due_date.strip():
        try:
            due_val = (date.fromisoformat(due_date.strip()) if kind == "task"
                       else datetime.fromisoformat(due_date.strip()))
        except ValueError:
            return "due_date must be an ISO date (YYYY-MM-DD)."
        args.append(due_val); sets.append(f"due_date=${len(args)}")
    if priority is not None:
        pr = _clamp_priority(priority)
        if pr is None:
            return "priority must be an integer 1..5."
        args.append(pr); sets.append(f"priority=${len(args)}")
    if not sets:
        return "Nothing to update (give text, due_date, or priority)."

    args.extend([USER_ID, iid])
    sql = (f"UPDATE {table} SET {', '.join(sets)}, updated_at=now() "
           f"WHERE {scope_col}=${len(args) - 1} AND id=${len(args)}")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        status = await conn.execute(sql, *args)
    return "Updated." if status.endswith(" 1") else "No such item for this user."


async def soft_delete_contact(contact_id: str) -> str:
    """SOFT-delete a contact (sets deleted_at + is_active=false), scoped to user.
    Recoverable; never a hard wipe."""
    try:
        cid = uuid.UUID(str(contact_id))
    except ValueError:
        return "contact_id must be a valid UUID."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name FROM contacts WHERE user_id=$1 AND id=$2 AND deleted_at IS NULL",
            USER_ID, cid)
        if row is None:
            return "No such active contact for this user."
        await conn.execute(
            "UPDATE contacts SET deleted_at=now(), is_active=false, updated_at=now() "
            "WHERE user_id=$1 AND id=$2", USER_ID, cid)
    return f"Deleted contact '{row['name']}'."


async def update_contact(contact_id: str, name: str | None = None, phone: str | None = None,
                         email: str | None = None, company: str | None = None,
                         notes: str | None = None) -> str:
    """Edit ONE contact the caller owns. Only provided fields change. Column names
    are hard-coded literals; only values are parameterized."""
    try:
        cid = uuid.UUID(str(contact_id))
    except ValueError:
        return "contact_id must be a valid UUID."
    fields = {"name": name, "phone": phone, "primary_email": email,
              "company": company, "notes": notes}
    sets, args = [], []
    for col, val in fields.items():
        if val is not None:
            args.append(val.strip()); sets.append(f"{col}=${len(args)}")
    if not sets:
        return "Nothing to update."
    args.extend([USER_ID, cid])
    sql = (f"UPDATE contacts SET {', '.join(sets)}, updated_at=now() "
           f"WHERE user_id=${len(args) - 1} AND id=${len(args)} AND deleted_at IS NULL")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        status = await conn.execute(sql, *args)
    return "Updated." if status.endswith(" 1") else "No such active contact for this user."


async def complete_task(task_id: str) -> str:
    try:
        tid = uuid.UUID(str(task_id))
    except ValueError:
        return "task_id must be a valid UUID (from get_tasks)."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name, status FROM omnia_tasks WHERE user_id = $1 AND id = $2",
            USER_ID, tid,
        )
        if row is None:
            return "No task with that id for this user."
        if row["status"] == "done":
            return f"'{row['name']}' is already done."
        async with conn.transaction():
            await conn.execute(
                "UPDATE omnia_tasks SET status='done', completed_at=now(), updated_at=now() "
                "WHERE user_id=$1 AND id=$2",
                USER_ID, tid,
            )
            await conn.execute(
                "INSERT INTO omnia_task_completions (id, user_id, task_id, completed_on, created_at) "
                "VALUES ($1,$2,$3,CURRENT_DATE,now())",
                uuid.uuid4(), USER_ID, tid,
            )
    return f"Completed '{row['name']}'."


class CalendarCreateNotAvailable(RuntimeError):
    """create_event does NOT create anything. Raised (never returned as prose) so
    no caller can mistake the refusal for a success-shaped result."""


async def create_event(title: str, start_at: str, end_at: str | None = None,
                       all_day: bool = False, location: str | None = None,
                       description: str | None = None) -> str:
    # RAISES, always (text-brain v2, 2026-09-25). It used to RETURN a normal
    # string on refusal, so a model reading "the tool result" could (and did,
    # audit 2026-09-25) narrate success. A refusal must be an error.
    #
    # Why nothing is written here: inserting omnia_events rows directly over
    # asyncpg is wrong (CHECK / NOT NULL failures, and no provider push). The
    # real path is the backend's HTTP POST /v1/events, which inserts the row AND
    # pushes it to Outlook/Google via calendar_writer; the text-brain gets that in
    # Phase 3 (needs the Phase 2 service token). See DEBUGLOG 2026-09-01 and
    # omnia-gv-command/_AUDIT_textbrain_2026_09_25.md.
    raise CalendarCreateNotAvailable(
        "create_event is not available: nothing was created. Omnia events are "
        "created through the backend POST /v1/events (not wired to this server yet). "
        "Create the appointment on the Outlook or Google calendar instead.")


async def add_contact(name: str, email: str | None = None, phone: str | None = None,
                      company: str | None = None, notes: str | None = None) -> str:
    if not name.strip():
        return "Contact name is empty."
    pool = await _get_pool()
    cid = uuid.uuid4()
    async with pool.acquire() as conn:
        if email:
            dup = await conn.fetchrow(
                "SELECT name FROM contacts WHERE user_id=$1 AND lower(primary_email)=lower($2) "
                "AND deleted_at IS NULL",
                USER_ID, email,
            )
            if dup:
                return f"A contact with {email} already exists ('{dup['name']}'). Not adding a duplicate."
        await conn.execute(
            """
            INSERT INTO contacts
                (id, user_id, name, primary_email, phone, company, notes,
                 is_active, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,true,now(),now())
            """,
            cid, USER_ID, name.strip(), email, phone, company, notes,
        )
    bits = ", ".join(b for b in (email, phone, company) if b)
    return f"Added contact '{name.strip()}'" + (f" ({bits})" if bits else "") + f". [id {cid}]"


async def add_list(name: str, kind: str = "todo") -> str:
    if not name.strip():
        return "List name is empty."
    if kind not in ("todo", "longterm"):
        return "kind must be 'todo' or 'longterm'."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        dup = await conn.fetchrow(
            "SELECT id FROM omnia_lists WHERE user_id=$1 AND lower(name)=lower($2) AND NOT archived",
            USER_ID, name.strip(),
        )
        if dup:
            return f"List '{name.strip()}' already exists."
        lid = uuid.uuid4()
        pos = await conn.fetchval(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM omnia_lists WHERE user_id=$1", USER_ID
        )
        await conn.execute(
            "INSERT INTO omnia_lists (id, user_id, name, kind, position, archived, created_at, updated_at) "
            "VALUES ($1,$2,$3,$4,$5,false,now(),now())",
            lid, USER_ID, name.strip(), kind, pos,
        )
    return f"Created list '{name.strip()}' ({kind}). [id {lid}]"




# --- shared lists (the 'james' workspace Michael can see) ----------------------
# Every statement here is scoped workspace_id = USER_ID (exact) and never touches
# the private planner workspace.

# Today in Pacific, computed by Postgres (the MCP's Windows venv has no tzdata,
# so zoneinfo can't load America/Los_Angeles here). Same day as the backend's
# _today_local(); the server, never a client clock, owns the date.
_TODAY_PT_SQL = "(now() AT TIME ZONE 'America/Los_Angeles')::date"


async def _pin_today(conn, item_id: uuid.UUID) -> bool:
    """Pin ONE shared item to Today, the way the backend does it
    (services/planner_links.pin_source_today): today_pinned = true, today_date =
    today (PT), today_rank appended at the END of the Today bucket. Idempotent:
    an already-pinned item is left alone. Returns True when it changed."""
    row = await conn.fetchrow(
        "SELECT today_pinned FROM shared_list_items WHERE workspace_id=$1 AND id=$2 "
        "AND is_private = false FOR UPDATE", USER_ID, item_id)
    if row is None or row["today_pinned"]:
        return False
    top = await conn.fetchval(
        "SELECT MAX(today_rank) FROM shared_list_items WHERE workspace_id=$1 AND today_pinned",
        USER_ID)
    await conn.execute(
        f"UPDATE shared_list_items SET today_pinned=true, today_date={_TODAY_PT_SQL}, "
        "today_rank=$3, updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
        USER_ID, item_id, (float(top) if top is not None else 0.0) + _RANK_STEP)
    return True


async def get_shared_lists(query: str | None = None) -> str:
    """The user's Shared Lists (id, title, project, open count), plus the ids of
    the 'Today' list and the Quick ToDo (is_quick_default) list. JSON."""
    q = (query or "").strip()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            r"""
            SELECT l.id, l.title, l.is_quick_default, p.title AS project,
                   (SELECT count(*) FROM shared_list_items i
                     WHERE i.list_id = l.id AND i.workspace_id = $1
                       AND i.parent_item_id IS NULL AND NOT i.done) AS open_items
              FROM shared_lists l
              LEFT JOIN shared_projects p ON p.id = l.project_id AND p.workspace_id = $1
             WHERE l.workspace_id = $1 AND l.archived = false
               AND ($2::text = '' OR l.title ILIKE $3 ESCAPE '\')
             ORDER BY l.is_quick_default DESC, (lower(l.title) = 'today') DESC,
                      p.title NULLS FIRST, l.position, l.title
            """,
            USER_ID, q, _like_arg(q),
        )
        today = await conn.fetch(
            "SELECT id FROM shared_lists WHERE workspace_id=$1 AND archived=false "
            "AND lower(title) = 'today' ORDER BY created_at", USER_ID)
        quick = await conn.fetchval(
            "SELECT id FROM shared_lists WHERE workspace_id=$1 AND archived=false "
            "AND is_quick_default ORDER BY created_at LIMIT 1", USER_ID)
    out = {
        "today_list_id": str(today[0]["id"]) if len(today) == 1 else None,
        "quick_todo_list_id": str(quick) if quick else None,
        "lists": [{"id": str(r["id"]), "title": r["title"], "project": r["project"],
                   "open_items": r["open_items"], "quick_default": r["is_quick_default"]}
                  for r in rows],
    }
    if len(today) > 1:
        out["warning"] = (f"{len(today)} lists are titled 'Today'; pick one by id: "
                          + ", ".join(str(r["id"]) for r in today))
    return _json.dumps(out, indent=1)


async def add_shared_item(list_title: str = "", text: str = "", priority: int | None = None,
                          due_date: str | None = None, list_id: str | None = None,
                          pin_today: bool = False) -> str:
    """Add an item to a shared list, chosen by `list_id` (exact) or `list_title`
    (exact or unique partial). Workspace = USER_ID, never the private planner
    workspace. Same semantics as backend POST /v1/shared-lists/{id}/items:
    rank = max(rank in that priority band) + 1000, position = max+1,
    created_by/updated_by 'james' (the person this MCP acts as). pin_today also
    pins the new item to Today (the /shared-lists TODAY band)."""
    if not text.strip():
        return "Item text is empty."
    if not (list_id or "").strip() and not list_title.strip():
        return "Give list_id or list_title."
    pr = None
    if priority is not None:
        try:
            pr = int(priority)
        except (TypeError, ValueError):
            return "priority must be 1..5."
        if pr < 1 or pr > 5:
            return "priority must be 1..5."
    due_val = None
    if due_date:
        try:
            due_val = datetime.fromisoformat(due_date.strip() + "T00:00:00+00:00")
        except ValueError:
            return "due_date must be YYYY-MM-DD."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        if (list_id or "").strip():
            lid = _uuid_or_none(list_id.strip())
            if lid is None:
                return "list_id must be a UUID (see get_shared_lists)."
            pick = await conn.fetch(
                "SELECT id, title FROM shared_lists WHERE workspace_id=$1 AND archived=false "
                "AND id=$2", USER_ID, lid)
            if not pick:
                return f"No shared list with id {list_id.strip()} (see get_shared_lists)."
        else:
            rows = await conn.fetch(
                "SELECT id, title FROM shared_lists WHERE workspace_id=$1 AND archived=false "
                r"AND title ILIKE $2 ESCAPE '\' ORDER BY created_at",
                USER_ID, _like_arg(list_title.strip()),
            )
            exact = [r for r in rows if r["title"].lower() == list_title.strip().lower()]
            pick = exact or rows
            if not pick:
                return f"No shared list matches '{list_title}'."
            if len(pick) > 1:
                names = ", ".join(f"{r['title']} (id {r['id']})" for r in pick[:8])
                return f"'{list_title}' is ambiguous: {names}. Use list_id."
        lid, title = pick[0]["id"], pick[0]["title"]
        async with conn.transaction():
            rank = await conn.fetchval(
                "SELECT COALESCE(MAX(rank), 0) + 1000 FROM shared_list_items "
                "WHERE workspace_id=$1 AND priority IS NOT DISTINCT FROM $2",
                USER_ID, pr,
            )
            pos = await conn.fetchval(
                "SELECT COALESCE(MAX(position), -1) + 1 FROM shared_list_items WHERE list_id=$1",
                lid,
            )
            iid = uuid.uuid4()
            await conn.execute(
                "INSERT INTO shared_list_items (id, list_id, workspace_id, text, priority, due_date, "
                "rank, position, created_by, updated_by, created_at, updated_at) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'james','james',now(),now())",
                iid, lid, USER_ID, text.strip(), pr, due_val, rank, pos,
            )
            if pin_today:
                await _pin_today(conn, iid)
    p = f" at P{pr}" if pr else ""
    t = " (pinned to Today)" if pin_today else ""
    return f"Added to '{title}'{p}{t}: '{text.strip()}'. [id {iid}]"


async def pin_shared_item_today(item_id: str, pin: bool = True) -> str:
    """Pin (or unpin) an EXISTING shared item to Today. Unpin mirrors the backend
    PATCH pin_today=false: today_pinned = false, today_date = NULL."""
    iid = _uuid_or_none(item_id)
    if iid is None:
        return "item_id must be a UUID."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT text, today_pinned FROM shared_list_items WHERE workspace_id=$1 AND id=$2 "
                "AND is_private = false", USER_ID, iid)
            if row is None:
                return "No such shared item for this workspace."
            if pin:
                changed = await _pin_today(conn, iid)
                if not changed:  # already pinned: re-stamp the day, as the PATCH does
                    await conn.execute(
                        f"UPDATE shared_list_items SET today_date={_TODAY_PT_SQL}, "
                        "updated_by='james', updated_at=now() "
                        "WHERE workspace_id=$1 AND id=$2 AND today_date IS DISTINCT FROM "
                        f"{_TODAY_PT_SQL}", USER_ID, iid)
            else:
                changed = bool(row["today_pinned"])
                await conn.execute(
                    "UPDATE shared_list_items SET today_pinned=false, today_date=NULL, "
                    "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
                    USER_ID, iid)
    if not changed:
        return f"'{row['text']}' was already {'on' if pin else 'off'} Today."
    return f"{'Pinned' if pin else 'Unpinned'} '{row['text']}' {'to' if pin else 'from'} Today."


# --- PLANNER (private time blocks) --------------------------------------------
# Mirrors omnia-platform backend/app/services/planner.py, planner_lanes.py,
# planner_runs.py and planner_links.py. CHANGE BOTH TOGETHER.
#
# PRIVACY: every planner row lives in workspace "<USER_ID>:private" (never the
# shared 'james' workspace Michael can read) with is_private = true, inside ONE
# "Planner" list in that workspace. The DB CHECK
# ck_shared_list_items_private_workspace enforces is_private <-> ':private'.
# Every planner statement is EXACT-MATCH workspace_id = PLANNER_WS (never LIKE /
# prefix); nothing takes a workspace arg. The only shared-workspace rows touched
# are a task's SOURCE item (exact USER_ID, is_private = false): pinned to Today
# when a task is planned from it, checked off when the task is done.
#
# Semantics (same as the backend):
#   block = top-level item (parent_item_id NULL) with start_at/end_at;
#   task  = child of a block (parent_item_id), ONE shared order = rank.
#   project tag = tags[0] in PLANNER_PROJECTS.
#   lanes: a 2-lane block has planner_lanes = 2 (+ optional planner_lane_names);
#          each task sits in column planner_lane 'A'/'B'. An "A: "/"B: " text
#          prefix is read as the lane and stripped on write; never written.
#   timers: one planner_task_runs row per start->stop; actual time = the sum.
#          actual_start = first start, actual_end = last stop (kept for readers).
#   start: open a run, status active, actual_start = COALESCE(actual_start, now),
#          actual_end NULL; max 2 active tasks; the block goes active.
#   stop(accomplished): close the run, actual_end = now if it was running; done
#          if accomplished (and its source item is checked off) else back to
#          planned. Block done when all tasks done/skipped.
#   snooze: pause, move to END of the next block that day (or to_block_id).
#   set_day: blocks that day not in the payload are deleted; a kept block's
#          tasks not in its payload are deleted; order = payload order.
# Busy needs the backend's Outlook tokens -> planner_set_busy calls the backend
# service endpoint (PLANNER_SERVICE_TOKEN), it never writes busy_event_id here.

PLANNER_WS = f"{USER_ID}:private"
PLANNER_TZ = "America/Los_Angeles"
PLANNER_PROJECTS = ("seekly", "omnia", "ccre", "cobuy", "18th", "michael", "personal", "other")
PLANNER_MAX_ACTIVE = 2
_RANK_STEP = 1000.0
_MAX_BLOCK = timedelta(hours=16)
_LANES = ("A", "B")
_LANE_PREFIX_RE = re.compile(r"^\s*([AaBb])\s*:\s*(?=\S)")
_LANE_TITLE_RE = re.compile(r"(?:^|[\s:])A[:\s]\s*(.*?)\s*/\s*B[:\s]\s*(.+)$")


def _proj(value) -> str:
    v = (value or "other").strip().lower()
    if v not in PLANNER_PROJECTS:
        raise ValueError(f"project must be one of {', '.join(PLANNER_PROJECTS)}")
    return v


def _clean(value, what: str) -> str:
    v = (value or "").strip()
    if not v:
        raise ValueError(f"{what} is required")
    if len(v) > 500:
        raise ValueError(f"{what} is too long")
    return v


async def _db_now(conn) -> datetime:
    """The DB's clock (as the old SQL now() writes were), so actual_* and run
    times never mix a drifting machine clock with DB timestamps."""
    return await conn.fetchval("SELECT clock_timestamp()")


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ---- lanes (planner_lanes.py) ----

def _split_lane_prefix(text: str):
    """'B: Feedback button' -> ('B', 'Feedback button'); no prefix -> (None, text)."""
    m = _LANE_PREFIX_RE.match(text or "")
    if not m:
        return None, text
    return m.group(1).upper(), text[m.end():]


def _norm_lane(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    v = str(value).strip().upper()
    if v not in _LANES:
        raise ValueError("lane must be A or B")
    return v


def _norm_lanes(value):
    if value is None:
        return None
    try:
        v = int(value)
    except (TypeError, ValueError):
        raise ValueError("lanes must be 1 or 2") from None
    if v not in (1, 2):
        raise ValueError("lanes must be 1 or 2")
    return v


def _clean_lane_names(names):
    if names is None:
        return None
    if not isinstance(names, (list, tuple)) or len(names) != 2:
        raise ValueError("lane_names needs two names")
    out = [(str(n or "")).strip()[:60] for n in names]
    if not all(out):
        raise ValueError("lane names cannot be empty")
    return out


def _jsonb(v):
    return _json.loads(v) if isinstance(v, str) else v


def _lane_count(block, tasks) -> int:
    if block["planner_lanes"] is not None:
        return 2 if block["planner_lanes"] == 2 else 1
    return 2 if any(_split_lane_prefix(t["text"])[0] for t in tasks) else 1


def _task_lane(task, lanes: int):
    if lanes != 2:
        return None
    return _split_lane_prefix(task["text"])[0] or task["planner_lane"] or "A"


def _lane_names(block) -> list:
    stored = _jsonb(block["planner_lane_names"])
    if isinstance(stored, list) and len(stored) == 2:
        return [str(stored[0]) or "A", str(stored[1]) or "B"]
    m = _LANE_TITLE_RE.search(block["text"] or "")
    a = m.group(1).strip() if m else ""
    b = m.group(2).strip() if m else ""
    return [a, b] if a and b else ["A", "B"]


async def _materialize_lanes(conn, block, tasks) -> int:
    """planner_lanes.materialize_lanes: write derived lanes into the columns and
    strip "A: " prefixes, so every write works on stored lanes."""
    lanes = _lane_count(block, tasks)
    if lanes == 2:
        if block["planner_lanes"] != 2:
            await conn.execute(
                "UPDATE shared_list_items SET planner_lanes=2, updated_at=now() "
                "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, block["id"])
        for t in tasks:
            lane, stripped = _split_lane_prefix(t["text"])
            if lane:
                await conn.execute(
                    "UPDATE shared_list_items SET planner_lane=$3, text=$4, updated_at=now() "
                    "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"], lane, stripped)
            elif t["planner_lane"] is None:
                await conn.execute(
                    "UPDATE shared_list_items SET planner_lane='A', updated_at=now() "
                    "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"])
    else:
        await conn.execute(
            "UPDATE shared_list_items SET planner_lane=NULL WHERE workspace_id=$1 "
            "AND is_private AND id = ANY($2::uuid[]) AND planner_lane IS NOT NULL",
            PLANNER_WS, [t["id"] for t in tasks])
    return lanes


async def _renumber(conn, ids: list) -> None:
    """Shared order = (i+1) * 1000, same as the backend's _renumber."""
    for i, tid in enumerate(ids):
        await conn.execute(
            "UPDATE shared_list_items SET rank=$3 WHERE workspace_id=$1 AND id=$2",
            PLANNER_WS, tid, (i + 1) * _RANK_STEP)


# ---- runs (planner_runs.py) ----

async def _runs_for(conn, task_ids: list) -> dict:
    out = {t: [] for t in task_ids}
    if task_ids:
        rows = await conn.fetch(
            "SELECT id, task_id, started_at, ended_at FROM planner_task_runs "
            "WHERE workspace_id=$1 AND task_id = ANY($2::uuid[]) ORDER BY started_at",
            PLANNER_WS, task_ids)
        for r in rows:
            out.setdefault(r["task_id"], []).append(r)
    return out


def _open_of(runs):
    return next((r for r in runs if r["ended_at"] is None), None)


def _last_end(runs):
    ends = [_utc(r["ended_at"]) for r in runs if r["ended_at"] is not None]
    return max(ends) if ends else None


async def _open_run(conn, task, now: datetime) -> None:
    """planner_runs.open_run: start a run unless one is open. Call BEFORE flipping
    the task to active; a stale open run on a non-running task is closed at the
    task's last stop first."""
    runs = (await _runs_for(conn, [task["id"]]))[task["id"]]
    stale = _open_of(runs)
    if stale is not None:
        if task["planner_status"] == "active":
            return
        end = _utc(task["actual_end"]) if task["actual_end"] is not None else _utc(stale["started_at"])
        await conn.execute(
            "UPDATE planner_task_runs SET ended_at=$3 WHERE workspace_id=$1 AND id=$2",
            PLANNER_WS, stale["id"], max(end, _utc(stale["started_at"])))
    await conn.execute(
        "INSERT INTO planner_task_runs (id, task_id, workspace_id, started_at) "
        "VALUES ($1,$2,$3,$4)", uuid.uuid4(), task["id"], PLANNER_WS, now)


async def _close_run(conn, task, now: datetime) -> None:
    """planner_runs.close_run: end the open run; a task running with NO open run
    (older MCP start) gets its run recorded now, from max(actual_start, last end)."""
    runs = (await _runs_for(conn, [task["id"]]))[task["id"]]
    run = _open_of(runs)
    if run is not None:
        await conn.execute(
            "UPDATE planner_task_runs SET ended_at=$3 WHERE workspace_id=$1 AND id=$2",
            PLANNER_WS, run["id"], max(now, _utc(run["started_at"])))
        return
    if task["planner_status"] != "active" or task["actual_start"] is None:
        return
    start = _utc(task["actual_start"])
    last = _last_end(runs)
    if last is not None and last > start:
        start = last
    await conn.execute(
        "INSERT INTO planner_task_runs (id, task_id, workspace_id, started_at, ended_at) "
        "VALUES ($1,$2,$3,$4,$5)", uuid.uuid4(), task["id"], PLANNER_WS, start, max(now, start))


def _run_state(task, runs) -> tuple:
    """planner_runs.run_state -> (closed seconds, running_since | None)."""
    closed = sum(max(0.0, (_utc(r["ended_at"]) - _utc(r["started_at"])).total_seconds())
                 for r in runs if r["ended_at"] is not None)
    running = task["planner_status"] == "active"
    run = _open_of(runs)
    if run is not None:
        since = _utc(run["started_at"]) if running else None
        if not running and task["actual_end"] is not None:
            closed += max(0.0, (_utc(task["actual_end"]) - _utc(run["started_at"])).total_seconds())
        return int(closed), since
    if not runs:
        if task["actual_start"] is None:
            return 0, None
        if running:
            return 0, _utc(task["actual_start"])
        if task["actual_end"] is None:
            return 0, None
        return int(max(0.0, (_utc(task["actual_end"]) - _utc(task["actual_start"])).total_seconds())), None
    if running and task["actual_start"] is not None:
        start = _utc(task["actual_start"])
        last = _last_end(runs)
        return int(closed), (last if last is not None and last > start else start)
    return int(closed), None


# ---- loading + JSON ----

def _uuid_or_none(value):
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


async def _planner_list_id(conn) -> uuid.UUID:
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"{PLANNER_WS}:planner-list")
    lid = await conn.fetchval(
        "SELECT id FROM shared_lists WHERE workspace_id=$1 AND title='Planner' "
        "ORDER BY created_at LIMIT 1", PLANNER_WS)
    if lid:
        return lid
    lid = uuid.uuid4()
    await conn.execute(
        "INSERT INTO shared_lists (id, workspace_id, title, kind, created_by) "
        "VALUES ($1,$2,'Planner','standing',$3)", lid, PLANNER_WS, USER_ID)
    return lid


_ITEM_COLS = ("id, list_id, parent_item_id, text, note, tags, start_at, end_at, today_date, "
              "planner_status, actual_start, actual_end, busy_event_id, rank, source_item_id, "
              "planner_lanes, planner_lane_names, planner_lane, checkin_at")


def _iso(v):
    return v.isoformat() if v is not None else None


def _tags0(tags) -> str:
    tags = _jsonb(tags) or []
    return tags[0] if tags and tags[0] in PLANNER_PROJECTS else "other"


def _task_json(r, runs=None, lanes: int = 1) -> dict:
    secs, since = _run_state(r, runs or [])
    text = r["text"] if lanes != 2 else _split_lane_prefix(r["text"])[1]
    return {"id": str(r["id"]), "block_id": str(r["parent_item_id"]), "text": text,
            "project": _tags0(r["tags"]), "status": r["planner_status"] or "planned",
            "lane": _task_lane(r, lanes),
            "actual_start": _iso(r["actual_start"]), "actual_end": _iso(r["actual_end"]),
            "actual_seconds": secs, "running_since": _iso(since),
            "note": r["note"], "rank": r["rank"],
            "source_item_id": str(r["source_item_id"]) if r["source_item_id"] else None}


def _block_json(r, tasks, run_map=None) -> dict:
    run_map = run_map or {}
    lanes = _lane_count(r, tasks)
    return {"id": str(r["id"]), "title": r["text"], "project": _tags0(r["tags"]),
            "start_at": _iso(r["start_at"]), "end_at": _iso(r["end_at"]),
            "date": _iso(r["today_date"]), "status": r["planner_status"] or "planned",
            "actual_start": _iso(r["actual_start"]), "actual_end": _iso(r["actual_end"]),
            "note": r["note"], "busy": bool(r["busy_event_id"]),
            "lanes": lanes, "lane_names": _lane_names(r) if lanes == 2 else None,
            "checkin_at": _iso(r["checkin_at"]),
            "tasks": [_task_json(t, run_map.get(t["id"], []), lanes) for t in tasks]}


async def _day_blocks(conn, day: date) -> list:
    return await conn.fetch(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND parent_item_id IS NULL "
        "AND start_at >= ($2::date)::timestamp AT TIME ZONE $3 "
        "AND start_at < ($2::date + 1)::timestamp AT TIME ZONE $3 "
        "ORDER BY start_at, created_at", PLANNER_WS, day, PLANNER_TZ)


async def _block_tasks(conn, block_ids: list) -> dict:
    out = {b: [] for b in block_ids}
    if block_ids:
        rows = await conn.fetch(
            f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
            "AND parent_item_id = ANY($2::uuid[]) ORDER BY rank NULLS LAST, created_at",
            PLANNER_WS, block_ids)
        for r in rows:
            out.setdefault(r["parent_item_id"], []).append(r)
    return out


async def _load_block(conn, block_id, for_update: bool = False):
    bid = _uuid_or_none(block_id)
    if bid is None:
        return None
    return await conn.fetchrow(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND id=$2 AND parent_item_id IS NULL" + (" FOR UPDATE" if for_update else ""),
        PLANNER_WS, bid)


async def _day_json(conn, day: date) -> dict:
    blocks = await _day_blocks(conn, day)
    tasks = await _block_tasks(conn, [b["id"] for b in blocks])
    run_map = await _runs_for(conn, [t["id"] for ts in tasks.values() for t in ts])
    return {"date": day.isoformat(),
            "blocks": [_block_json(b, tasks[b["id"]], run_map) for b in blocks]}


async def _task_json_fresh(conn, task_id) -> dict:
    t = await conn.fetchrow(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND id=$2", PLANNER_WS, task_id)
    block = await _load_block(conn, t["parent_item_id"])
    siblings = (await _block_tasks(conn, [block["id"]]))[block["id"]]
    runs = (await _runs_for(conn, [t["id"]]))[t["id"]]
    return _task_json(t, runs, _lane_count(block, siblings))


def _parse_day(value: str) -> date:
    return date.fromisoformat((value or "").strip())


async def planner_get_day(day: str) -> str:
    try:
        d = _parse_day(day)
    except ValueError:
        return "date must be YYYY-MM-DD."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        return _json.dumps(await _day_json(conn, d), indent=1)


async def _to_ts(conn, d: date, value):
    """'HH:MM' (Pacific, on day d) or an ISO timestamp -> timestamptz."""
    v = (value or "").strip() if isinstance(value, str) else ""
    if not v:
        raise ValueError("every block needs start and end")
    if re.fullmatch(r"\d{1,2}:\d{2}", v):
        return await conn.fetchval(
            "SELECT ($1::date + $2::text::time) AT TIME ZONE $3::text", d, v, PLANNER_TZ)
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        return await conn.fetchval("SELECT $1::timestamp AT TIME ZONE $2", dt, PLANNER_TZ)
    return dt


async def planner_set_day(day: str, blocks) -> str:
    """Create/replace the plan for one day. `blocks` = list of
    {id?, title, start, end, project?, note?, lanes?: 1|2, lane_names?: [A, B],
     tasks: [{id?, text, project?, note?, lane?: 'A'|'B'}]}
    with start/end 'HH:MM' Pacific or ISO. Blocks that day not listed are deleted.
    Lanes (services/planner_lanes.apply_day_lanes): explicit `lanes`/`lane` win; an
    "A: "/"B: " text prefix becomes the lane and is stripped; otherwise a block
    keeps its stored lanes. A Busy block (it has an Outlook event) can be neither
    dropped nor re-timed here: the call is refused until planner_set_busy(id, False)."""
    try:
        d = _parse_day(day)
    except ValueError:
        return "date must be YYYY-MM-DD."
    if isinstance(blocks, str):
        blocks = _json.loads(blocks)
    pool = await _get_pool()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                list_id = await _planner_list_id(conn)
                existing = {str(r["id"]): r for r in await _day_blocks(conn, d)}
                keep: set = set()
                # Deletes deferred to the end: a task dropped from block A may be
                # claimed by a LATER block in the same payload.
                claimed: list = []
                kept_blocks: list = []
                for b in blocks:
                    title = _clean(b.get("title"), "title")
                    s = await _to_ts(conn, d, b.get("start") or b.get("start_at"))
                    e = await _to_ts(conn, d, b.get("end") or b.get("end_at"))
                    if e <= s:
                        raise ValueError(f"block '{title}': end must be after start")
                    if e - s > _MAX_BLOCK:
                        raise ValueError(f"block '{title}': a block cannot exceed 16 hours")
                    on_day = await conn.fetchval(
                        "SELECT ($1::timestamptz AT TIME ZONE $2)::date = $3", s, PLANNER_TZ, d)
                    if not on_day:
                        raise ValueError(f"block '{title}' does not start on {d}")
                    tags = _json.dumps([_proj(b.get("project"))]) if b.get("project") else None
                    names = _clean_lane_names(b.get("lane_names"))
                    names_j = _json.dumps(names) if names is not None else None
                    # Lanes for this block, decided before any row is written.
                    tasks_in = b.get("tasks") or []
                    prefixed = [_split_lane_prefix(_clean(t.get("text"), "task text"))
                                for t in tasks_in]
                    asks = [_norm_lane(t.get("lane")) for t in tasks_in]
                    bid = _uuid_or_none(b.get("id"))
                    cur_b = None
                    if bid:
                        cur_b = await conn.fetchrow(
                            "SELECT id, start_at, end_at, busy_event_id, planner_lanes "
                            "FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                            "AND id=$2 AND parent_item_id IS NULL", PLANNER_WS, bid)
                        if cur_b and cur_b["busy_event_id"] and (
                                cur_b["start_at"] != s or cur_b["end_at"] != e):
                            raise ValueError(
                                f"block '{title}' is Busy (has an Outlook event); call "
                                f"planner_set_busy('{bid}', False) before re-timing it")
                    lanes = _norm_lanes(b.get("lanes"))
                    if lanes is None:
                        wants = any(asks) or any(p for p, _ in prefixed)
                        stored2 = cur_b is not None and cur_b["planner_lanes"] == 2
                        lanes = 2 if wants or stored2 else 1
                    if cur_b is None:
                        bid = uuid.uuid4()
                        await conn.execute(
                            "INSERT INTO shared_list_items (id, list_id, workspace_id, is_private, "
                            "text, note, tags, start_at, end_at, today_date, planner_status, "
                            "planner_lanes, planner_lane_names, created_by, updated_by) "
                            "VALUES ($1,$2,$3,true,$4,$5,$6::jsonb,$7,$8,$9,'planned',$10,"
                            "$11::jsonb,'james','james')",
                            bid, list_id, PLANNER_WS, title, b.get("note"),
                            tags or _json.dumps(["other"]), s, e, d, lanes, names_j)
                    else:
                        # A moved block asks its check-in again at its new end.
                        await conn.execute(
                            "UPDATE shared_list_items SET text=$3, note=COALESCE($4, note), "
                            "tags=COALESCE($5::jsonb, tags), "
                            "checkin_at=CASE WHEN start_at IS DISTINCT FROM $6 "
                            "OR end_at IS DISTINCT FROM $7 THEN NULL ELSE checkin_at END, "
                            "start_at=$6, end_at=$7, today_date=$8, planner_lanes=$9, "
                            "planner_lane_names=COALESCE($10::jsonb, planner_lane_names), "
                            "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
                            PLANNER_WS, bid, title, b.get("note"), tags, s, e, d, lanes, names_j)
                    keep.add(str(bid))
                    wanted: list = []
                    for i, t in enumerate(tasks_in):
                        pre, text = prefixed[i]
                        ask = asks[i] or pre
                        ttags = _json.dumps([_proj(t.get("project"))]) if t.get("project") else None
                        rank = (i + 1) * _RANK_STEP
                        tid = _uuid_or_none(t.get("id"))
                        tfound = None
                        if tid:
                            hit = await conn.fetchrow(
                                "SELECT id, parent_item_id FROM shared_list_items WHERE "
                                "workspace_id=$1 AND is_private AND id=$2", PLANNER_WS, tid)
                            if hit is not None and hit["parent_item_id"] is None:
                                raise ValueError("a block id was given as a task id")
                            tfound = hit["id"] if hit is not None else None
                        if tfound:
                            await conn.execute(
                                "UPDATE shared_list_items SET parent_item_id=$3, text=$4, "
                                "note=COALESCE($5, note), tags=CASE WHEN $6::jsonb IS NOT NULL THEN $6::jsonb "
                                "WHEN tags IS NULL OR tags = '[]'::jsonb THEN '[\"other\"]'::jsonb "
                                "ELSE tags END, rank=$7, today_date=$8, planner_lane=CASE WHEN $9::int = 2 "
                                "THEN COALESCE($10::varchar, planner_lane, 'A') ELSE NULL END, "
                                "updated_by='james', updated_at=now() "
                                "WHERE workspace_id=$1 AND id=$2",
                                PLANNER_WS, tid, bid, text, t.get("note"), ttags, rank, d,
                                lanes, ask)
                        else:
                            tid = uuid.uuid4()
                            await conn.execute(
                                "INSERT INTO shared_list_items (id, list_id, parent_item_id, "
                                "workspace_id, is_private, text, note, tags, rank, today_date, "
                                "planner_status, planner_lane, created_by, updated_by) VALUES "
                                "($1,$2,$3,$4,true,$5,$6,$7::jsonb,$8,$9,'planned',$10,"
                                "'james','james')",
                                tid, list_id, bid, PLANNER_WS, text, t.get("note"),
                                ttags or _json.dumps(["other"]), rank, d,
                                (ask or "A") if lanes == 2 else None)
                        wanted.append(tid)
                    claimed.extend(wanted)
                    kept_blocks.append(bid)
                await conn.execute(
                    "DELETE FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                    "AND parent_item_id = ANY($2::uuid[]) AND NOT (id = ANY($3::uuid[]))",
                    PLANNER_WS, kept_blocks, claimed)
                busy_dropped = [r["text"] for k, r in existing.items()
                                if k not in keep and r["busy_event_id"]]
                if busy_dropped:
                    raise ValueError(
                        "these blocks are Busy (have Outlook events): " + ", ".join(busy_dropped)
                        + ". Call planner_set_busy(<id>, False) before dropping them")
                for key, r in existing.items():
                    if key not in keep:
                        await conn.execute(
                            "DELETE FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                            "AND id=$2", PLANNER_WS, r["id"])
                out = await _day_json(conn, d)
    except ValueError as e:
        return f"Not saved: {e}"
    return _json.dumps(out, indent=1)


async def planner_add_task(block_id: str, text: str, lane: str | None = None,
                           project: str | None = None, note: str | None = None,
                           position: int | None = None,
                           source_item_id: str | None = None) -> str:
    """POST /v1/planner/blocks/{id}/tasks: add ONE task to a block at `position`
    in its shared order (None = end). In a 2-lane block the column is `lane`
    (default A); a "B: x" text with no lane turns lanes on and lands in B, prefix
    stripped. source_item_id = the shared item this task is planned from: it must
    be in the shared workspace, gets pinned to Today, and is checked off when the
    task is done."""
    try:
        want_lane = _norm_lane(lane)
        tags = _json.dumps([_proj(project)])
    except ValueError as e:
        return f"Not added: {e}"
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            block = await _load_block(conn, block_id, for_update=True)
            if block is None:
                return "Planner block not found."
            src = None
            if source_item_id:
                sid = _uuid_or_none(source_item_id)
                src = sid and await conn.fetchval(
                    "SELECT id FROM shared_list_items WHERE id=$1 AND workspace_id=$2 "
                    "AND is_private = false", sid, USER_ID)
                if not src:
                    return "Source item not found in the shared lists."
            prefix, stripped = _split_lane_prefix(text or "")
            use_prefix = bool(prefix) and want_lane is None
            try:  # validate before any write, so a refusal saves nothing
                clean = _clean(stripped if use_prefix else text, "text")
            except ValueError as e:
                return f"Not added: {e}"
            existing = (await _block_tasks(conn, [block["id"]]))[block["id"]]
            lanes = await _materialize_lanes(conn, block, existing)
            if use_prefix:
                want_lane = prefix
                if lanes != 2:
                    lanes = 2
                    await conn.execute(
                        "UPDATE shared_list_items SET planner_lanes=2, updated_at=now() "
                        "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, block["id"])
                    await conn.execute(
                        "UPDATE shared_list_items SET planner_lane='A' WHERE workspace_id=$1 "
                        "AND is_private AND parent_item_id=$2", PLANNER_WS, block["id"])
            tid = uuid.uuid4()
            await conn.execute(
                "INSERT INTO shared_list_items (id, list_id, parent_item_id, workspace_id, "
                "is_private, text, note, tags, today_date, planner_status, planner_lane, "
                "source_item_id, created_by, updated_by) VALUES ($1,$2,$3,$4,true,$5,$6,"
                "$7::jsonb,$8,'planned',$9,$10,'james','james')",
                tid, block["list_id"], block["id"], PLANNER_WS, clean, note, tags,
                block["today_date"], (want_lane or "A") if lanes == 2 else None, src)
            order = [t["id"] for t in existing]
            idx = len(order) if position is None else max(0, min(int(position), len(order)))
            order.insert(idx, tid)
            await _renumber(conn, order)
            if src:
                await _pin_today(conn, src)
            out = await _task_json_fresh(conn, tid)
    return _json.dumps(out, indent=1)


async def _load_task(conn, task_id: str):
    tid = _uuid_or_none(task_id)
    if tid is None:
        return None
    return await conn.fetchrow(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND id=$2 AND parent_item_id IS NOT NULL FOR UPDATE", PLANNER_WS, tid)


async def _settle_block(conn, block_id, now: datetime) -> None:
    counts = await conn.fetchrow(
        "SELECT count(*) AS total, count(*) FILTER (WHERE COALESCE(planner_status,'planned') "
        "NOT IN ('done','skipped')) AS open, min(actual_start) AS first FROM shared_list_items "
        "WHERE workspace_id=$1 AND is_private AND parent_item_id=$2", PLANNER_WS, block_id)
    if counts["total"] and not counts["open"]:
        await conn.execute(
            "UPDATE shared_list_items SET planner_status='done', done=true, done_at=$3, "
            "actual_start=COALESCE(actual_start, $4, $3), actual_end=$3, updated_at=now() "
            "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, block_id, now, counts["first"])


async def _complete_source(conn, task, now: datetime) -> bool:
    """services/planner.complete_source: finishing a linked task checks off the
    SHARED item it was planned from (exact USER_ID workspace, never private)."""
    if task["source_item_id"] is None:
        return False
    status = await conn.execute(
        "UPDATE shared_list_items SET done=true, done_at=$3, updated_by='james', "
        "updated_at=now() WHERE id=$1 AND workspace_id=$2 AND is_private = false "
        "AND done = false", task["source_item_id"], USER_ID, now)
    return status.endswith(" 1")


async def planner_start(task_id: str) -> str:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Serialize concurrent starts (same key as the backend), then read the
            # task under the lock so a racing start can't open a second run.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"{PLANNER_WS}:timers")
            t = await _load_task(conn, task_id)
            if t is None:
                return "Planner task not found."
            if t["planner_status"] == "active":
                return f"Already running: '{t['text']}'."
            active = await conn.fetchval(
                "SELECT count(*) FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                "AND parent_item_id IS NOT NULL AND planner_status='active' AND id<>$2",
                PLANNER_WS, t["id"])
            if active >= PLANNER_MAX_ACTIVE:
                return f"At most {PLANNER_MAX_ACTIVE} timers can run at once. Stop one first."
            now = await _db_now(conn)
            await _open_run(conn, t, now)
            await conn.execute(
                "UPDATE shared_list_items SET planner_status='active', done=false, done_at=NULL, "
                "actual_start=COALESCE(actual_start, $3), actual_end=NULL, updated_by='james', "
                "updated_at=now() WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"], now)
            # Restarting a task re-opens a finished block too.
            await conn.execute(
                "UPDATE shared_list_items SET "
                "done=CASE WHEN COALESCE(planner_status,'planned') IN ('planned','done') "
                "THEN false ELSE done END, "
                "done_at=CASE WHEN COALESCE(planner_status,'planned') IN ('planned','done') "
                "THEN NULL ELSE done_at END, "
                "planner_status=CASE WHEN COALESCE(planner_status,'planned') IN ('planned','done') "
                "THEN 'active' ELSE planner_status END, "
                "actual_start=COALESCE(actual_start, $3), actual_end=NULL, updated_at=now() "
                "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["parent_item_id"], now)
    return f"Started '{t['text']}'. [task {t['id']}]"


async def planner_stop(task_id: str, accomplished: bool = False, note: str | None = None) -> str:
    pool = await _get_pool()
    synced = False
    async with pool.acquire() as conn:
        async with conn.transaction():
            t = await _load_task(conn, task_id)
            if t is None:
                return "Planner task not found."
            now = await _db_now(conn)
            running = t["planner_status"] == "active"
            if running:
                await _close_run(conn, t, now)
            if accomplished:
                status = "done"
            elif running:
                status = "planned"
            else:
                status = t["planner_status"] or "planned"
            await conn.execute(
                "UPDATE shared_list_items SET planner_status=$3::varchar, "
                "done=($3::varchar = 'done'), "
                "done_at=CASE WHEN $3::varchar <> 'done' THEN NULL "
                "WHEN $7::boolean OR done_at IS NULL THEN $6::timestamptz ELSE done_at END, "
                "actual_end=CASE WHEN $4::boolean THEN $6::timestamptz ELSE actual_end END, "
                "note=COALESCE($5::text, note), updated_by='james', updated_at=now() "
                "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"], status, running, note, now,
                bool(accomplished))
            if accomplished:
                synced = await _complete_source(conn, t, now)
            await _settle_block(conn, t["parent_item_id"], now)
    extra = " (its list item is checked off too)" if synced else ""
    return f"{'Done' if accomplished else 'Paused'}: '{t['text']}'{extra}."


async def planner_snooze(task_id: str, to_block_id: str | None = None) -> str:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            t = await _load_task(conn, task_id)
            if t is None:
                return "Planner task not found."
            cur = await conn.fetchrow(
                "SELECT id, start_at, today_date FROM shared_list_items WHERE workspace_id=$1 "
                "AND is_private AND id=$2", PLANNER_WS, t["parent_item_id"])
            if to_block_id:
                target = await _load_block(conn, to_block_id, for_update=True)
            else:
                target = await conn.fetchrow(
                    f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 "
                    "AND is_private AND parent_item_id IS NULL AND id<>$2 AND start_at >= $3 "
                    "AND start_at < (COALESCE($4::date, ($3::timestamptz AT TIME ZONE $5)::date) + 1)"
                    "::timestamp AT TIME ZONE $5 "
                    "ORDER BY start_at, created_at LIMIT 1 FOR UPDATE",
                    PLANNER_WS, cur["id"], cur["start_at"], cur["today_date"], PLANNER_TZ)
            if target is None:
                return ("Target block not found." if to_block_id
                        else "No later block today to snooze into.")
            now = await _db_now(conn)
            running = t["planner_status"] == "active"
            if running:
                await _close_run(conn, t, now)
            # Same as backend move_task: lanes are materialized over the target's
            # tasks as they stand (the task itself too, when snoozed in place).
            all_target = (await _block_tasks(conn, [target["id"]]))[target["id"]]
            lanes = await _materialize_lanes(conn, target, all_target)
            others = [r for r in all_target if r["id"] != t["id"]]
            own = await conn.fetchval(
                "SELECT planner_lane FROM shared_list_items WHERE workspace_id=$1 AND id=$2",
                PLANNER_WS, t["id"])
            lane = (own or "A") if lanes == 2 else None
            await conn.execute(
                "UPDATE shared_list_items SET parent_item_id=$3, today_date=$4, planner_lane=$5, "
                "actual_end=CASE WHEN $6::boolean THEN $7::timestamptz ELSE actual_end END, "
                "planner_status='planned', done=false, done_at=NULL, "
                "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
                PLANNER_WS, t["id"], target["id"], target["today_date"], lane, running, now)
            await _renumber(conn, [r["id"] for r in others] + [t["id"]])
            await _settle_block(conn, cur["id"], now)
    return f"Snoozed '{t['text']}' to '{target['text']}'."


async def planner_set_busy(block_id: str, busy: bool) -> str:
    """Flip ONE block Busy/private via the backend (it owns the Outlook tokens):
    POST {OMNIA_API_BASE}/v1/planner/service/busy, Bearer PLANNER_SERVICE_TOKEN."""
    import httpx

    token = os.environ.get("PLANNER_SERVICE_TOKEN", "").strip()
    if not token:
        return "PLANNER_SERVICE_TOKEN is not set in omnia-mcp/.env."
    if _uuid_or_none(block_id) is None:
        return "block_id must be a UUID."
    base = os.environ.get("OMNIA_API_BASE", "https://api.lifeomnia.com").rstrip("/")
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{base}/v1/planner/service/busy",
                              headers={"Authorization": f"Bearer {token}"},
                              json={"block_id": block_id, "busy": bool(busy)})
    if r.status_code != 200:
        return f"Busy toggle failed: HTTP {r.status_code} {r.text[:200]}"
    b = r.json()
    return f"'{b.get('title')}' is now {'Busy' if b.get('busy') else 'private'}."
