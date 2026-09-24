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

import os
import re
import uuid
from datetime import date, datetime
from pathlib import Path

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
    # asyncpg doesn't understand libpq channel_binding; drop it if present.
    url = re.sub(r"([?&])channel_binding=[^&]*", r"\1", url).rstrip("?&")
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url


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


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(_rw_dsn(), min_size=1, max_size=3)
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


async def create_event(title: str, start_at: str, end_at: str | None = None,
                       all_day: bool = False, location: str | None = None,
                       description: str | None = None) -> str:
    if not title.strip():
        return "Event title is empty."
    try:
        datetime.fromisoformat(start_at.strip())
        if end_at:
            datetime.fromisoformat(end_at.strip())
    except ValueError as e:
        return f"Bad timestamp (use ISO 8601, e.g. 2026-08-12T10:00:00-07:00): {e}"
    # omnia_events is a read-model of externally-synced calendars: a CHECK
    # constraint restricts calendar_provider to 'google'/'outlook', and
    # calendar_id/end_at are NOT NULL. A local-only event cannot be inserted, so
    # do NOT fabricate one here. Real appointments must be created on the user's
    # Outlook/Google calendar (via the ms365 / gcal tools), which then syncs into
    # Omnia. See DEBUGLOG 2026-09-01.
    return ("Omnia calendar events sync from Outlook/Google and can't be created "
            "locally. Add the appointment to your Outlook or Google calendar "
            "instead; it will then appear in Omnia.")


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


# --- shared-list add (named list) ---------------------------------------------

async def add_shared_item(list_title: str, text: str, priority: int | None = None,
                          due_date: str | None = None) -> str:
    """Add an item to a NAMED shared list (workspace = USER_ID, never the private
    planner workspace). Same semantics as backend POST /v1/shared-lists/{id}/items:
    rank = max(rank in that priority band) + 1000, position = max+1,
    created_by/updated_by 'james' (the person this MCP acts as)."""
    if not text.strip():
        return "Item text is empty."
    if not list_title.strip():
        return "list_title is empty."
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
        rows = await conn.fetch(
            "SELECT id, title FROM shared_lists WHERE workspace_id=$1 AND archived=false "
            "AND title ILIKE $2 ESCAPE '\' ORDER BY created_at",
            USER_ID, _like_arg(list_title.strip()),
        )
        exact = [r for r in rows if r["title"].lower() == list_title.strip().lower()]
        pick = exact or rows
        if not pick:
            return f"No shared list matches '{list_title}'."
        if len(pick) > 1:
            names = ", ".join(r["title"] for r in pick[:8])
            return f"'{list_title}' is ambiguous: {names}. Use the exact title."
        list_id, title = pick[0]["id"], pick[0]["title"]
        async with conn.transaction():
            rank = await conn.fetchval(
                "SELECT COALESCE(MAX(rank), 0) + 1000 FROM shared_list_items "
                "WHERE workspace_id=$1 AND priority IS NOT DISTINCT FROM $2",
                USER_ID, pr,
            )
            pos = await conn.fetchval(
                "SELECT COALESCE(MAX(position), -1) + 1 FROM shared_list_items WHERE list_id=$1",
                list_id,
            )
            iid = uuid.uuid4()
            await conn.execute(
                "INSERT INTO shared_list_items (id, list_id, workspace_id, text, priority, due_date, "
                "rank, position, created_by, updated_by, created_at, updated_at) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'james','james',now(),now())",
                iid, list_id, USER_ID, text.strip(), pr, due_val, rank, pos,
            )
    p = f" at P{pr}" if pr else ""
    return f"Added to '{title}'{p}: '{text.strip()}'. [id {iid}]"


# --- PLANNER (private time blocks) --------------------------------------------
# Mirrors omnia-platform backend/app/services/planner.py. CHANGE BOTH TOGETHER.
#
# PRIVACY: every planner row lives in workspace "<USER_ID>:private" (never the
# shared 'james' workspace Michael can read) with is_private = true, inside ONE
# "Planner" list in that workspace. The DB CHECK
# ck_shared_list_items_private_workspace enforces is_private <-> ':private'.
# Every statement below is scoped to PLANNER_WS; nothing takes a workspace arg.
#
# Semantics (same as the backend):
#   block = top-level item (parent_item_id NULL) with start_at/end_at;
#   task  = child of a block (parent_item_id), queue order = rank.
#   project tag = tags[0] in PLANNER_PROJECTS.
#   start: status active, actual_start = COALESCE(actual_start, now), actual_end
#          NULL; max 2 active tasks; the block goes active.
#   stop(accomplished): actual_end = now if it was running; done (done=true) if
#          accomplished else back to planned. Block done when all tasks done/skipped.
#   snooze: move to END of the next block that day (or to_block_id), planned.
#   set_day: blocks that day not in the payload are deleted; a kept block's
#          tasks not in its payload are deleted; order = payload order.
# Busy needs the backend's Outlook tokens -> planner_set_busy calls the backend
# service endpoint (PLANNER_SERVICE_TOKEN), it never writes busy_event_id here.

import json as _json  # noqa: E402

PLANNER_WS = f"{USER_ID}:private"
PLANNER_TZ = "America/Los_Angeles"
PLANNER_PROJECTS = ("seekly", "omnia", "ccre", "cobuy", "18th", "michael", "personal", "other")
PLANNER_MAX_ACTIVE = 2
_RANK_STEP = 1000.0


def _proj(value) -> str:
    v = (value or "other").strip().lower()
    if v not in PLANNER_PROJECTS:
        raise ValueError(f"project must be one of {', '.join(PLANNER_PROJECTS)}")
    return v


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


_ITEM_COLS = ("id, parent_item_id, text, note, tags, start_at, end_at, today_date, "
              "planner_status, actual_start, actual_end, busy_event_id, rank")


def _iso(v):
    return v.isoformat() if v is not None else None


def _tags0(tags) -> str:
    if isinstance(tags, str):
        tags = _json.loads(tags or "[]")
    return tags[0] if tags and tags[0] in PLANNER_PROJECTS else "other"


def _task_json(r) -> dict:
    return {"id": str(r["id"]), "block_id": str(r["parent_item_id"]), "text": r["text"],
            "project": _tags0(r["tags"]), "status": r["planner_status"] or "planned",
            "actual_start": _iso(r["actual_start"]), "actual_end": _iso(r["actual_end"]),
            "note": r["note"]}


def _block_json(r, tasks) -> dict:
    return {"id": str(r["id"]), "title": r["text"], "project": _tags0(r["tags"]),
            "start_at": _iso(r["start_at"]), "end_at": _iso(r["end_at"]),
            "date": _iso(r["today_date"]), "status": r["planner_status"] or "planned",
            "actual_start": _iso(r["actual_start"]), "actual_end": _iso(r["actual_end"]),
            "note": r["note"], "busy": bool(r["busy_event_id"]),
            "tasks": [_task_json(t) for t in tasks]}


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


async def _day_json(conn, day: date) -> dict:
    blocks = await _day_blocks(conn, day)
    tasks = await _block_tasks(conn, [b["id"] for b in blocks])
    return {"date": day.isoformat(), "blocks": [_block_json(b, tasks[b["id"]]) for b in blocks]}


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
    {id?, title, start, end, project?, note?, tasks: [{id?, text, project?, note?}]}
    with start/end 'HH:MM' Pacific or ISO. Blocks that day not listed are deleted
    (a dropped Busy block's Outlook event is NOT removed here; call
    planner_set_busy(id, False) first)."""
    try:
        d = _parse_day(day)
    except ValueError:
        return "date must be YYYY-MM-DD."
    if isinstance(blocks, str):
        blocks = _json.loads(blocks)
    dropped_busy: list = []
    pool = await _get_pool()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                list_id = await _planner_list_id(conn)
                existing = {str(r["id"]): r for r in await _day_blocks(conn, d)}
                keep: set = set()
                for b in blocks:
                    title = (b.get("title") or "").strip()
                    if not title:
                        raise ValueError("every block needs a title")
                    s = await _to_ts(conn, d, b.get("start") or b.get("start_at"))
                    e = await _to_ts(conn, d, b.get("end") or b.get("end_at"))
                    if e <= s:
                        raise ValueError(f"block '{title}': end must be after start")
                    on_day = await conn.fetchval(
                        "SELECT ($1::timestamptz AT TIME ZONE $2)::date = $3", s, PLANNER_TZ, d)
                    if not on_day:
                        raise ValueError(f"block '{title}' does not start on {d}")
                    tags = _json.dumps([_proj(b.get("project"))])
                    bid = _uuid_or_none(b.get("id"))
                    found = None
                    if bid:
                        found = await conn.fetchval(
                            "SELECT id FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                            "AND id=$2 AND parent_item_id IS NULL", PLANNER_WS, bid)
                    if found is None:
                        bid = uuid.uuid4()
                        await conn.execute(
                            "INSERT INTO shared_list_items (id, list_id, workspace_id, is_private, "
                            "text, note, tags, start_at, end_at, today_date, planner_status, "
                            "created_by, updated_by) VALUES ($1,$2,$3,true,$4,$5,$6::jsonb,$7,$8,$9,"
                            "'planned','james','james')",
                            bid, list_id, PLANNER_WS, title, b.get("note"), tags, s, e, d)
                    else:
                        await conn.execute(
                            "UPDATE shared_list_items SET text=$3, note=COALESCE($4, note), "
                            "tags=$5::jsonb, start_at=$6, end_at=$7, today_date=$8, "
                            "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
                            PLANNER_WS, bid, title, b.get("note"), tags, s, e, d)
                    keep.add(str(bid))
                    wanted: list = []
                    for i, t in enumerate(b.get("tasks") or []):
                        text = (t.get("text") or "").strip()
                        if not text:
                            raise ValueError(f"block '{title}': empty task text")
                        ttags = _json.dumps([_proj(t.get("project"))])
                        rank = (i + 1) * _RANK_STEP
                        tid = _uuid_or_none(t.get("id"))
                        tfound = None
                        if tid:
                            tfound = await conn.fetchval(
                                "SELECT id FROM shared_list_items WHERE workspace_id=$1 AND "
                                "is_private AND id=$2 AND parent_item_id IS NOT NULL",
                                PLANNER_WS, tid)
                        if tfound:
                            await conn.execute(
                                "UPDATE shared_list_items SET parent_item_id=$3, text=$4, "
                                "note=COALESCE($5, note), tags=$6::jsonb, rank=$7, today_date=$8, "
                                "updated_by='james', updated_at=now() "
                                "WHERE workspace_id=$1 AND id=$2",
                                PLANNER_WS, tid, bid, text, t.get("note"), ttags, rank, d)
                        else:
                            tid = uuid.uuid4()
                            await conn.execute(
                                "INSERT INTO shared_list_items (id, list_id, parent_item_id, "
                                "workspace_id, is_private, text, note, tags, rank, today_date, "
                                "planner_status, created_by, updated_by) VALUES ($1,$2,$3,$4,true,"
                                "$5,$6,$7::jsonb,$8,$9,'planned','james','james')",
                                tid, list_id, bid, PLANNER_WS, text, t.get("note"), ttags, rank, d)
                        wanted.append(tid)
                    await conn.execute(
                        "DELETE FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                        "AND parent_item_id=$2 AND NOT (id = ANY($3::uuid[]))",
                        PLANNER_WS, bid, wanted)
                for key, r in existing.items():
                    if key not in keep:
                        if r["busy_event_id"]:
                            dropped_busy.append(r["text"])
                        await conn.execute(
                            "DELETE FROM shared_list_items WHERE workspace_id=$1 AND is_private "
                            "AND id=$2", PLANNER_WS, r["id"])
                out = await _day_json(conn, d)
    except ValueError as e:
        return f"Not saved: {e}"
    if dropped_busy:
        out["warning"] = ("Deleted Busy block(s) whose Outlook 'Focus block' events remain: "
                          + ", ".join(dropped_busy))
    return _json.dumps(out, indent=1)


async def _load_task(conn, task_id: str):
    tid = _uuid_or_none(task_id)
    if tid is None:
        return None
    return await conn.fetchrow(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND id=$2 AND parent_item_id IS NOT NULL FOR UPDATE", PLANNER_WS, tid)


async def _settle_block(conn, block_id) -> None:
    counts = await conn.fetchrow(
        "SELECT count(*) AS total, count(*) FILTER (WHERE COALESCE(planner_status,'planned') "
        "NOT IN ('done','skipped')) AS open, min(actual_start) AS first FROM shared_list_items "
        "WHERE workspace_id=$1 AND is_private AND parent_item_id=$2", PLANNER_WS, block_id)
    if counts["total"] and not counts["open"]:
        await conn.execute(
            "UPDATE shared_list_items SET planner_status='done', done=true, done_at=now(), "
            "actual_start=COALESCE(actual_start, $3, now()), actual_end=now(), updated_at=now() "
            "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, block_id, counts["first"])


async def planner_start(task_id: str) -> str:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
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
            await conn.execute(
                "UPDATE shared_list_items SET planner_status='active', done=false, done_at=NULL, "
                "actual_start=COALESCE(actual_start, now()), actual_end=NULL, updated_by='james', "
                "updated_at=now() WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"])
            await conn.execute(
                "UPDATE shared_list_items SET "
                "planner_status=CASE WHEN COALESCE(planner_status,'planned')='planned' "
                "THEN 'active' ELSE planner_status END, "
                "actual_start=COALESCE(actual_start, now()), actual_end=NULL, updated_at=now() "
                "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["parent_item_id"])
    return f"Started '{t['text']}'. [task {t['id']}]"


async def planner_stop(task_id: str, accomplished: bool = False, note: str | None = None) -> str:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            t = await _load_task(conn, task_id)
            if t is None:
                return "Planner task not found."
            running = t["planner_status"] == "active"
            if accomplished:
                status = "done"
            elif running:
                status = "planned"
            else:
                status = t["planner_status"] or "planned"
            await conn.execute(
                "UPDATE shared_list_items SET planner_status=$3::varchar, "
                "done=($3::varchar = 'done'), "
                "done_at=CASE WHEN $3::varchar = 'done' THEN now() ELSE NULL END, "
                "actual_end=CASE WHEN $4::boolean THEN now() ELSE actual_end END, "
                "note=COALESCE($5::text, note), updated_by='james', updated_at=now() "
                "WHERE workspace_id=$1 AND id=$2", PLANNER_WS, t["id"], status, running, note)
            await _settle_block(conn, t["parent_item_id"])
    return f"{'Done' if accomplished else 'Paused'}: '{t['text']}'."


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
                target = await conn.fetchrow(
                    "SELECT id, text, today_date FROM shared_list_items WHERE workspace_id=$1 "
                    "AND is_private AND id=$2 AND parent_item_id IS NULL",
                    PLANNER_WS, _uuid_or_none(to_block_id))
            else:
                target = await conn.fetchrow(
                    "SELECT id, text, today_date FROM shared_list_items WHERE workspace_id=$1 "
                    "AND is_private AND parent_item_id IS NULL AND id<>$2 AND start_at >= $3 "
                    "AND start_at < ($4::date + 1)::timestamp AT TIME ZONE $5 "
                    "ORDER BY start_at, created_at LIMIT 1",
                    PLANNER_WS, cur["id"], cur["start_at"], cur["today_date"], PLANNER_TZ)
            if target is None:
                return "No later block today to snooze into."
            rank = await conn.fetchval(
                "SELECT COALESCE(MAX(rank), 0) + $3 FROM shared_list_items WHERE workspace_id=$1 "
                "AND parent_item_id=$2 AND id<>$4", PLANNER_WS, target["id"], _RANK_STEP, t["id"])
            await conn.execute(
                "UPDATE shared_list_items SET parent_item_id=$3, rank=$4, today_date=$5, "
                "actual_end=CASE WHEN planner_status='active' THEN now() ELSE actual_end END, "
                "planner_status='planned', done=false, done_at=NULL, "
                "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
                PLANNER_WS, t["id"], target["id"], rank, target["today_date"])
            await _settle_block(conn, cur["id"])
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
