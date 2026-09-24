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
