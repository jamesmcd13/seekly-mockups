"""
Read-WRITE adapter to the Life-Omnia Neon Postgres DB.

Companion to omnia_client.py (which stays strictly read-only). This module holds
the *write* helpers used by the write tools in server.py — add a to-do, complete
a to-do, add a contact, the private planner. Everything is scoped to a single
user_id and the operations are deliberately narrow.

TO-DOS LIVE IN SHARED LISTS ONLY (2026-09-25). The old Omnia Lists
(omnia_lists / omnia_tasks) are retired: planner PR #432 hid them, so anything
written there vanished from James's view. Every to-do writer here (add_task,
add_todo, add_shared_item, add_list, update_todo, delete_todo, complete_task)
writes shared_lists / shared_list_items. Nothing in this file INSERTs into
omnia_tasks or omnia_lists any more; the only legacy write left is
complete_task checking off an OLD row by id. See the SHARED LISTS section.

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


# --- SHARED LISTS: the one to-do store -----------------------------------------
# WORKSPACES. The shared workspace is USER_ID ('james'; Michael can read it).
# Private lists (private mode, being built 2026-09-25) live in LISTS_PRIVATE_WS =
# '<uid>:lists:private'. List lookups search BOTH and tolerate the private one
# not existing yet. A new item's workspace + is_private always follow its LIST:
# the DB CHECK ck_shared_list_items_private_workspace pins is_private to a
# ':private' workspace, both directions. The PLANNER workspace ('<uid>:private')
# is never a list target here: planner rows go through the planner_* tools only.
#
# PROVENANCE (no migration). created_by = who the row is FOR ('james'), or
# 'claude' for Claude's own rows (add_todo keeps its old 'claude' stamp);
# origin_by (varchar 32, written once, never re-stamped by a PATCH) = the WRITE
# PATH, e.g. 'mcp:add_task', 'brain:gv', 'claude:desktop'. Callers may pass their
# own tag (`source`). The UI badge reads created_by first and only consults
# origin_by on machine rows, so a person row renders exactly as before. The
# 2026-09-25 list migration uses the same convention (origin_by = batch tag).
# shared_lists has no origin column, so add_list records its tag in created_by
# (authorship; non-person strings render as the neutral "auto" badge).
#
# ACTIVITY. MCP writes log no shared_list_activity rows (unchanged from the
# earlier MCP tools), which also keeps a private item's text out of the shared
# activity feed Michael can read.

LISTS_PRIVATE_WS = f"{USER_ID}:lists:private"
_LIST_WORKSPACES = [USER_ID, LISTS_PRIVATE_WS]
QUICK_DEFAULT_PRIORITY = 1
_ORIGIN_MAX = 32
_ORIGIN_BAD = re.compile(r"[^a-z0-9_.:-]+")
# Old Omnia-List names that meant "today". It is always the Today list, pinned
# (feedback_today_list_naming). Used only when no list has the literal name.
_TODAY_ALIASES = {"top to do today", "top to-do today", "top todo today", "plan today",
                  "today list"}
# Kinds a Shared List may have (CHECK ck_shared_lists_kind) + the old
# Omnia-List kinds, mapped so existing add_list callers keep working.
_LIST_KINDS = {"todo": "project", "longterm": "standing", "long_term": "standing",
               "main": "main", "project": "project", "standing": "standing"}
_LIST_COLS = "l.id, l.title, l.workspace_id, l.is_quick_default, p.title AS project"
_LIST_FROM = ("FROM shared_lists l LEFT JOIN shared_projects p "
              "ON p.id = l.project_id AND p.workspace_id = l.workspace_id")


def _ambient_source() -> str:
    """The write path when the caller passed no `source`: $OMNIA_MCP_SOURCE if
    the launcher set it, else 'brain:gv' when this process runs inside the GV
    text-brain (the watcher launches headless Claude, and its executors import
    this module, from C:/Users/James/dev/omnia-gv-command*). '' = unknown."""
    env = (os.environ.get("OMNIA_MCP_SOURCE") or "").strip()
    if env:
        return env
    try:
        cwd = os.getcwd().replace("\\", "/").lower()
    except OSError:
        return ""
    return "brain:gv" if "/omnia-gv-command" in cwd else ""


def _origin(source: str | None, default: str) -> str:
    """A provenance tag for origin_by: lowercase [a-z0-9_.:-], at most 32 chars.
    Explicit `source` > the ambient source > the tool's own default
    ('mcp:<tool>'). A malformed tag is cleaned, never an error: an add must not
    fail on it."""
    raw = (source or "").strip() or _ambient_source()
    s = _ORIGIN_BAD.sub("-", raw.lower()).strip("-")
    return (s or default)[:_ORIGIN_MAX]


def _is_private_ws(ws: str) -> bool:
    """Same test as the DB CHECK (workspace_id LIKE '%:private')."""
    return (ws or "").endswith(":private")


def _priority(value):
    """None/'' -> None; else an int 1..5 (1 = P1, most urgent) or ValueError."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        pr = int(value)
    except (TypeError, ValueError):
        raise ValueError("priority must be 1..5 (1 = P1, highest).") from None
    if not 1 <= pr <= 5:
        raise ValueError("priority must be 1..5 (1 = P1, highest).")
    return pr


def _due(value):
    """'YYYY-MM-DD' -> UTC midnight of that day, the shape shared due_date stores
    (a calendar day in a timestamptz; the UI reads only the date part). A full
    ISO timestamp is accepted and cut to its date. '' -> None."""
    v = (value or "").strip() if isinstance(value, str) else ""
    if not v:
        return None
    try:
        d = date.fromisoformat(v[:10])
    except ValueError:
        raise ValueError(f"due_date must be YYYY-MM-DD, got '{value}'.") from None
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _day(value):
    """'YYYY-MM-DD' -> date, '' -> None, else ValueError."""
    v = (value or "").strip() if isinstance(value, str) else ""
    if not v:
        return None
    try:
        return date.fromisoformat(v[:10])
    except ValueError:
        raise ValueError(f"today_date must be YYYY-MM-DD, got '{value}'.") from None


def _rank(value):
    """An explicit bucket order (float) or None."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError("today_rank must be a number (lower = earlier in Today).") from None


def _list_label(row) -> str:
    return row["title"] + (" (private)" if _is_private_ws(row["workspace_id"]) else "")


async def _list_by_id(conn, list_id: str):
    lid = _uuid_or_none(list_id)
    if lid is None:
        raise ValueError("list_id must be a UUID (see get_shared_lists).")
    row = await conn.fetchrow(
        f"SELECT {_LIST_COLS} {_LIST_FROM} WHERE l.id = $1 "
        "AND l.workspace_id = ANY($2::text[]) AND l.archived = false",
        lid, _LIST_WORKSPACES)
    if row is None:
        raise ValueError(f"No shared list with id {list_id} (see get_shared_lists).")
    return row


async def _quick_list(conn):
    """Quick ToDo: the list flagged is_quick_default (rename-proof), else the
    oldest list titled 'Quick ToDo' (the backend's adopt-by-title step)."""
    row = await conn.fetchrow(
        f"SELECT {_LIST_COLS} {_LIST_FROM} WHERE l.workspace_id = $1 "
        "AND l.is_quick_default AND l.archived = false ORDER BY l.created_at LIMIT 1", USER_ID)
    if row is None:
        row = await conn.fetchrow(
            f"SELECT {_LIST_COLS} {_LIST_FROM} WHERE l.workspace_id = $1 "
            "AND l.archived = false AND lower(l.title) = 'quick todo' "
            "ORDER BY l.created_at LIMIT 1", USER_ID)
    if row is None:
        raise ValueError("No Quick ToDo list found (open /shared-lists once to create it).")
    return row


async def _lists_matching(conn, title: str):
    """(exact, all) non-archived lists in James's list workspaces whose title
    contains `title` (case-insensitive, LIKE metacharacters escaped). Shared
    lists sort before private ones, oldest first."""
    rows = await conn.fetch(
        f"SELECT {_LIST_COLS} {_LIST_FROM} WHERE l.workspace_id = ANY($1::text[]) "
        r"AND l.archived = false AND l.title ILIKE $2 ESCAPE '\' "
        "ORDER BY (l.workspace_id = $3) DESC, l.created_at",
        _LIST_WORKSPACES, _like_arg(title), USER_ID)
    t = title.strip().lower()
    return [r for r in rows if r["title"].strip().lower() == t], rows


def _ambiguous(title: str, rows) -> ValueError:
    names = ", ".join(f"{_list_label(r)} (id {r['id']})" for r in rows[:8])
    return ValueError(f"'{title}' matches several lists: {names}. Pass list_id.")


async def _resolve_list(conn, list_title: str | None = None, list_id: str | None = None,
                        *, fallback_quick: bool):
    """-> (list_row, note). By id (exact) or by title: one exact match wins,
    else one partial match; several -> ValueError (never guess); none -> Quick
    ToDo when `fallback_quick` (note says so), else ValueError. No title and no
    id -> Quick ToDo (or ValueError without fallback)."""
    if (list_id or "").strip():
        return await _list_by_id(conn, list_id.strip()), None
    title = (list_title or "").strip()
    if not title:
        if fallback_quick:
            return await _quick_list(conn), None
        raise ValueError("Give list_id or list_title.")
    exact, rows = await _lists_matching(conn, title)
    if len(exact) == 1:
        return exact[0], None
    if len(exact) > 1:
        raise _ambiguous(title, exact)
    if len(rows) == 1:
        return rows[0], None
    if len(rows) > 1:
        raise _ambiguous(title, rows)
    if not fallback_quick:
        raise ValueError(f"No shared list matches '{title}' (see get_shared_lists).")
    return await _quick_list(conn), f"no list named '{title}', so it went to Quick ToDo"


async def _next_band_rank(conn, ws: str, priority, exclude_id=None) -> float:
    """routes/shared_lists._next_rank_in_band: the end of the priority band."""
    top = await conn.fetchval(
        "SELECT MAX(rank) FROM shared_list_items WHERE workspace_id = $1 "
        "AND priority IS NOT DISTINCT FROM $2::smallint "
        "AND ($3::uuid IS NULL OR id <> $3::uuid)",
        ws, priority, exclude_id)
    return (float(top) if top is not None else 0.0) + _RANK_STEP


async def _insert_item(conn, lst, text: str, *, note=None, priority=None, due=None,
                       created_by: str = "james", origin: str, pin_today: bool = False,
                       today_date=None, today_rank=None, pin_focus: bool = False) -> uuid.UUID:
    """ONE new shared_list_items row at the end of its list + priority band, the
    way POST /v1/shared-lists/{id}/items and /quick-add build it (rank = band
    max + 1000, position = list max + 1). Planner columns stay NULL."""
    ws = lst["workspace_id"]
    iid = uuid.uuid4()
    async with conn.transaction():
        rank = await _next_band_rank(conn, ws, priority)
        pos = await conn.fetchval(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM shared_list_items WHERE list_id = $1",
            lst["id"])
        await conn.execute(
            "INSERT INTO shared_list_items (id, list_id, workspace_id, is_private, text, note, "
            "priority, due_date, rank, position, created_by, updated_by, origin_by, "
            "created_at, updated_at) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'james',$12,now(),now())",
            iid, lst["id"], ws, _is_private_ws(ws), text, note, priority, due, rank, pos,
            created_by, origin)
        if pin_today:
            await _pin_today(conn, iid, ws, day=today_date, rank=today_rank)
        if pin_focus:
            await _pin_focus(conn, iid, ws)
    return iid


def _added_suffix(priority, due, pin_today: bool, pin_focus: bool) -> str:
    bits = ""
    if priority:
        bits += f" at P{priority}"
    if due is not None:
        bits += f" (due {due.date().isoformat()})"
    if pin_today:
        bits += " (pinned to Today)"
    if pin_focus:
        bits += " (in Focus)"
    return bits


# --- write operations --------------------------------------------------------

async def add_task(title: str, list_name: str = "", due_date: str | None = None,
                   description: str | None = None, *, priority: int | None = None,
                   pin_today: bool = False, pin_focus: bool = False,
                   source: str | None = None, list_id: str | None = None) -> str:
    """Add a to-do to a SHARED LIST (the old Omnia Lists are retired).

    `list_name` resolves by title across James's list workspaces (shared, plus
    '<uid>:lists:private' once private mode exists): one exact match, else one
    partial match; several -> refused (pass list_id); none or empty -> Quick
    ToDo, and the reply says so. Old names meaning today ("Top to Do Today")
    land in the Today list, pinned. Priority 1..5 (None = no priority; the Quick
    ToDo default is P1, like the app's quick-add). Returns '... [id <uuid>]'."""
    title = (title or "").strip()
    if not title:
        return "Task title is empty."
    try:
        pr = _priority(priority)
        due = _due(due_date)
    except ValueError as e:
        return str(e)
    origin = _origin(source, "mcp:add_task")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        name = (list_name or "").strip()
        alias_note = None
        if not (list_id or "").strip() and name.lower() in _TODAY_ALIASES:
            exact, _rows = await _lists_matching(conn, name)
            if not exact:
                alias_note = f"'{name}' is the Today list now"
                name, pin_today = "Today", True
        try:
            lst, note = await _resolve_list(conn, name, list_id, fallback_quick=True)
        except ValueError as e:
            return str(e)
        if pr is None and lst["is_quick_default"]:
            pr = QUICK_DEFAULT_PRIORITY
        iid = await _insert_item(conn, lst, title, note=(description or "").strip() or None,
                                 priority=pr, due=due, origin=origin, pin_today=pin_today,
                                 pin_focus=pin_focus)
    why = "; ".join(n for n in (alias_note, note) if n)
    why = f" ({why})" if why else ""
    return (f"Added task '{title}' to list '{_list_label(lst)}'"
            f"{_added_suffix(pr, due, pin_today, pin_focus)}{why}. [id {iid}]")


async def add_shared_todo(text: str, priority: int = 1, *, pin_today: bool = False,
                          pin_focus: bool = False, source: str | None = None) -> str:
    """Add a to-do to the DEFAULT shared quick list (the one flagged
    is_quick_default, i.e. 'Quick ToDo'), at the given priority.

    This is the default landing spot for an unqualified "add a to-do" — a shared
    list, not a personal one. priority is Omnia's smallint 1..5 where 1 = P1
    (highest); values outside that range are clamped to 1 (a GV text must never
    fail on a bad priority). created_by stays 'claude', as before."""
    text = (text or "").strip()
    if not text:
        return "To-do text is empty."
    try:
        pr = int(priority)
    except (TypeError, ValueError):
        pr = 1
    if pr < 1 or pr > 5:
        pr = 1
    pool = await _get_pool()
    async with pool.acquire() as conn:
        try:
            lst = await _quick_list(conn)
        except ValueError as e:
            return str(e)
        iid = await _insert_item(conn, lst, text, priority=pr, created_by="claude",
                                 origin=_origin(source, "mcp:add_todo"),
                                 pin_today=pin_today, pin_focus=pin_focus)
    extra = (" (pinned to Today)" if pin_today else "") + (" (in Focus)" if pin_focus else "")
    return f"Added to shared list '{lst['title']}' at P{pr}{extra}: '{text}'. [id {iid}]"


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


async def find_todos(query: str, limit: int = 25, include_legacy: bool = False) -> list[dict]:
    """OPEN shared to-dos (James's list workspaces, never the planner) whose text
    matches `query`. Returns [{kind:'shared', id, label}]. The retired Omnia
    Lists are hidden in the app, so their rows are left out unless
    `include_legacy` (then as kind 'task'; delete/update_todo refuse those, and
    complete_task still checks one off)."""
    q = (query or "").strip()
    if not q:
        return []
    like = _like_arg(q)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            r"""
            SELECT 'shared' AS kind, i.id::text AS id, i.text AS label
              FROM shared_list_items i
              JOIN shared_lists l ON l.id = i.list_id AND l.workspace_id = i.workspace_id
             WHERE i.workspace_id = ANY($1::text[]) AND i.done = false AND l.archived = false
               AND i.text ILIKE $2 ESCAPE '\'
             ORDER BY i.created_at DESC
             LIMIT $3
            """,
            _LIST_WORKSPACES, like, limit,
        )
        out = [{"kind": r["kind"], "id": r["id"], "label": r["label"]} for r in rows]
        if include_legacy and len(out) < limit:
            legacy = await conn.fetch(
                r"SELECT id::text AS id, name AS label FROM omnia_tasks "
                r"WHERE user_id = $1 AND status <> 'done' AND name ILIKE $2 ESCAPE '\' "
                r"ORDER BY created_at DESC LIMIT $3",
                USER_ID, like, limit - len(out),
            )
            out += [{"kind": "task", "id": r["id"], "label": r["label"]} for r in legacy]
    return out


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
# Every statement is keyed by ONE id and scoped to James's list workspaces
# (never the planner workspace). kind 'task' and 'shared' both mean a Shared
# Lists item now; a retired Omnia-Lists id is only recognised to explain it.

_LEGACY_READONLY = ("That is an item in the retired Omnia Lists (hidden in the app, "
                    "read-only). complete_task can still check it off; to keep it, "
                    "re-add it with add_task.")


async def _load_todo(conn, iid, *, for_update: bool = False):
    """ONE shared to-do in James's list workspaces (never a planner row)."""
    return await conn.fetchrow(
        "SELECT id, text, workspace_id, list_id, priority, done, today_pinned, is_focus "
        "FROM shared_list_items WHERE id = $1 AND workspace_id = ANY($2::text[])"
        + (" FOR UPDATE" if for_update else ""),
        iid, _LIST_WORKSPACES)


async def _is_legacy_task(conn, iid) -> bool:
    return bool(await conn.fetchval(
        "SELECT 1 FROM omnia_tasks WHERE user_id = $1 AND id = $2", USER_ID, iid))


def _priority_or_clear(value):
    """update_todo's priority: None/'' -> (None, False) = leave it; 0 -> (None,
    True) = clear it; 1..5 -> (n, False); anything else -> ValueError."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, False
    try:
        if int(value) == 0:
            return None, True
    except (TypeError, ValueError):
        pass
    return _priority(value), False


async def delete_todo(kind: str, item_id: str) -> str:
    """Hard-delete ONE shared to-do (its subtasks cascade; a planner task planned
    from it just loses its source link). Reversible via Neon PITR. Sits behind
    the GV brain's DELETE-confirm gate. kind 'task' or 'shared' (both mean a
    Shared Lists item now); a retired Omnia-Lists id is refused, never deleted."""
    if kind not in ("task", "shared"):
        return "kind must be 'task' or 'shared'."
    iid = _uuid_or_none(item_id)
    if iid is None:
        return "item_id must be a valid UUID."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await _load_todo(conn, iid, for_update=True)
            if row is None:
                if await _is_legacy_task(conn, iid):
                    return _LEGACY_READONLY
                return "No such to-do for this user."
            await conn.execute(
                "DELETE FROM shared_list_items WHERE workspace_id = $1 AND id = $2",
                row["workspace_id"], iid)
    return f"Deleted to-do '{row['text']}'."


async def update_todo(kind: str, item_id: str, text: str | None = None,
                      due_date: str | None = None, priority: int | None = None, *,
                      note: str | None = None, list_id: str | None = None,
                      list_title: str | None = None, pin_today: bool | None = None,
                      today_date: str | None = None, today_rank=None,
                      pin_focus: bool | None = None) -> str:
    """Edit ONE shared to-do; only what you pass changes. Returns exactly
    'Updated.' on success (callers compare the string).

    text / note: new values. due_date: 'YYYY-MM-DD', or 'clear'. priority: 1..5,
    or 0 to clear it (a changed priority parks the item at the end of its new
    band, as the app does). list_id / list_title: move it to another list in the
    SAME workspace (shared <-> private moves are refused here). pin_today: True
    pins it to Today (today_date defaults to today in Pacific; today_rank to the
    end of Today for a fresh pin), False unpins it; today_date / today_rank on
    their own re-stamp an item. pin_focus: True/False adds it to / takes it out
    of the Focus bucket. Column names are hard-coded literals; only values are
    parameterized."""
    if kind not in ("task", "shared"):
        return "kind must be 'task' or 'shared'."
    iid = _uuid_or_none(item_id)
    if iid is None:
        return "item_id must be a valid UUID."
    clear_due = isinstance(due_date, str) and due_date.strip().lower() in ("clear", "none", "null")
    try:
        due = None if clear_due else _due(due_date)
        pr, clear_pr = _priority_or_clear(priority)
        day = _day(today_date)
        trank = _rank(today_rank)
    except ValueError as e:
        return str(e)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await _load_todo(conn, iid, for_update=True)
            if row is None:
                if await _is_legacy_task(conn, iid):
                    return _LEGACY_READONLY
                return "No such item for this user."
            ws = row["workspace_id"]
            sets: list[str] = []
            args: list = [ws, iid]

            def put(col: str, val, cast: str = "") -> None:
                args.append(val)
                sets.append(f"{col}=${len(args)}{cast}")

            if text is not None and text.strip():
                put("text", text.strip())
            if note is not None and note.strip():
                put("note", note.strip())
            if clear_due:
                sets.append("due_date=NULL")
            elif due is not None:
                put("due_date", due)
            if clear_pr or pr is not None:
                new_pr = None if clear_pr else pr
                if new_pr != row["priority"]:
                    put("priority", new_pr, "::smallint")
                    put("rank", await _next_band_rank(conn, ws, new_pr, iid))
            if (list_id or "").strip() or (list_title or "").strip():
                try:
                    lst, _note = await _resolve_list(conn, list_title, list_id,
                                                     fallback_quick=False)
                except ValueError as e:
                    return str(e)
                if lst["workspace_id"] != ws:
                    return ("Moving an item between shared and private lists isn't supported "
                            "here; use Make private / Share in the app.")
                if lst["id"] != row["list_id"]:
                    put("list_id", lst["id"])
            touches_today = pin_today is not None or day is not None or trank is not None
            if not sets and not touches_today and pin_focus is None:
                return ("Nothing to update (give text, note, due_date, priority, a list, "
                        "pin_today, today_date, today_rank or pin_focus).")
            if sets:
                await conn.execute(
                    f"UPDATE shared_list_items SET {', '.join(sets)}, updated_by='james', "
                    "updated_at=now() WHERE workspace_id=$1 AND id=$2", *args)
            if pin_today is True:
                await _pin_today(conn, iid, ws, day=day, rank=trank, restamp=True)
            elif pin_today is False:
                await _unpin_today(conn, iid, ws)
            elif day is not None or trank is not None:
                await _stamp_today(conn, iid, ws, day, trank)
            if pin_focus is True:
                await _pin_focus(conn, iid, ws)
            elif pin_focus is False:
                await _unpin_focus(conn, iid, ws)
    return "Updated."


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
    """Check off ONE to-do by id. A Shared Lists item: done + done_at, and every
    open planner task planned from it is finished too (list -> planner done-sync,
    the same thing the app's checkbox does via planner_links). A retired
    Omnia-Lists id still works here: legacy rows are read-only except for being
    checked off (status only; nothing is inserted into the old tables)."""
    tid = _uuid_or_none(task_id)
    if tid is None:
        return "task_id must be a valid UUID (from get_tasks)."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await _load_todo(conn, tid, for_update=True)
            if row is not None:
                if row["done"]:
                    return f"'{row['text']}' is already done."
                now = await _db_now(conn)
                await conn.execute(
                    "UPDATE shared_list_items SET done=true, done_at=$3, updated_by='james', "
                    "updated_at=now() WHERE workspace_id=$1 AND id=$2",
                    row["workspace_id"], tid, now)
                # Planner links only ever point at SHARED-workspace items.
                synced = (await _complete_linked_planner_tasks(conn, tid, now)
                          if row["workspace_id"] == USER_ID else 0)
                extra = " (its planner task is done too)" if synced else ""
                return f"Completed '{row['text']}'{extra}."
            legacy = await conn.fetchrow(
                "SELECT name, status FROM omnia_tasks WHERE user_id = $1 AND id = $2 FOR UPDATE",
                USER_ID, tid)
            if legacy is None:
                return "No to-do with that id for this user."
            if legacy["status"] == "done":
                return f"'{legacy['name']}' is already done."
            await conn.execute(
                "UPDATE omnia_tasks SET status='done', completed_at=now(), updated_at=now() "
                "WHERE user_id=$1 AND id=$2", USER_ID, tid)
    return f"Completed '{legacy['name']}' (an item in the retired Omnia Lists)."


async def _complete_linked_planner_tasks(conn, item_id, now: datetime) -> int:
    """services/planner_links.complete_linked_tasks: the shared item was checked
    off, so every open planner task planned from it (source_item_id) is marked
    done (a running timer is closed) and its block settled. Touches ONLY rows in
    the exact private planner workspace. Returns how many tasks changed."""
    rows = await conn.fetch(
        f"SELECT {_ITEM_COLS} FROM shared_list_items WHERE workspace_id=$1 AND is_private "
        "AND parent_item_id IS NOT NULL AND source_item_id=$2 FOR UPDATE",
        PLANNER_WS, item_id)
    changed = 0
    for t in rows:
        if t["planner_status"] == "done":
            continue
        running = t["planner_status"] == "active"
        if running:
            await _close_run(conn, t, now)
        await conn.execute(
            "UPDATE shared_list_items SET planner_status='done', done=true, done_at=$3, "
            "actual_end=CASE WHEN $4::boolean THEN $3 ELSE actual_end END, "
            "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
            PLANNER_WS, t["id"], now, running)
        changed += 1
    if changed:
        for block_id in {t["parent_item_id"] for t in rows}:
            await _settle_block(conn, block_id, now)
    return changed


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
    # pushes it to Outlook/Google via calendar_writer, but it only accepts a Clerk
    # user session. The one service-token route that writes Outlook events
    # (/v1/planner/service/busy) only makes the generic "Focus block" for a
    # planner block, so it is not a general create either. Re-checked 2026-09-25
    # (writers repoint): there is still no safe service path, so this stays an
    # error until the text-brain's Phase 2a service token lands on POST
    # /v1/events. When it does: create on the CCRE Outlook calendar by default
    # and NEVER add attendees unless the caller passed them explicitly (invites
    # message people on James's behalf). See DEBUGLOG 2026-09-01 and
    # omnia-gv-command/_AUDIT_textbrain_2026_09_25.md.
    raise CalendarCreateNotAvailable(
        "calendar create not available: nothing was created. Omnia events are "
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


async def add_list(name: str, kind: str = "todo", project: str = "", *,
                   private: bool = False, source: str | None = None) -> str:
    """Create a SHARED LIST (the old Omnia Lists are retired). Skips duplicates:
    a non-archived list with the same title (any case) in the same workspace is
    returned instead. kind: 'todo' (default, a normal list) | 'longterm' (a
    standing list) | or the Shared Lists kinds main / project / standing.
    project: optional existing project to file it under (exact title, else a
    unique partial match; shared or James's private projects); omitted =
    unfiled. private: create it in James's private lists workspace
    ('<uid>:lists:private', invisible to Michael; private mode contract
    2026-09-25). A list filed in a PRIVATE project is always private. Position is
    the end of that bucket, as POST /v1/shared-lists does."""
    title = (name or "").strip()
    if not title:
        return "List name is empty."
    k = _LIST_KINDS.get((kind or "todo").strip().lower())
    if k is None:
        return "kind must be 'todo' or 'longterm' (or main / project / standing)."
    created_by = _origin(source, "mcp:add_list")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Two racing add_list calls must not both miss the duplicate check.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))",
                               f"{USER_ID}:mcp-add-list")
            project_id = project_title = None
            forced_private = False
            if (project or "").strip():
                prows = await conn.fetch(
                    "SELECT id, title, workspace_id FROM shared_projects "
                    "WHERE workspace_id = ANY($1::text[]) AND archived=false "
                    r"AND title ILIKE $2 ESCAPE '\' ORDER BY (workspace_id = $3) DESC, created_at",
                    _LIST_WORKSPACES, _like_arg(project.strip()), USER_ID)
                pick = ([r for r in prows if r["title"].strip().lower() == project.strip().lower()]
                        or prows)
                if not pick:
                    return f"No Shared Lists project matches '{project.strip()}'."
                if len(pick) > 1:
                    names = ", ".join(f"{r['title']} (id {r['id']})" for r in pick[:8])
                    return f"'{project.strip()}' matches several projects: {names}."
                project_id, project_title = pick[0]["id"], pick[0]["title"]
                forced_private = _is_private_ws(pick[0]["workspace_id"]) and not private
                private = private or _is_private_ws(pick[0]["workspace_id"])
            ws = LISTS_PRIVATE_WS if private else USER_ID
            dup = await conn.fetchrow(
                "SELECT id, title FROM shared_lists WHERE workspace_id=$1 AND archived=false "
                "AND lower(title)=lower($2) ORDER BY created_at LIMIT 1", ws, title)
            if dup:
                return f"List '{dup['title']}' already exists. [id {dup['id']}]"
            pos = await conn.fetchval(
                "SELECT COALESCE(MAX(position), -1) + 1 FROM shared_lists WHERE workspace_id=$1 "
                "AND project_id IS NOT DISTINCT FROM $2::uuid", ws, project_id)
            lid = uuid.uuid4()
            await conn.execute(
                "INSERT INTO shared_lists (id, workspace_id, project_id, title, kind, position, "
                "created_by, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,now(),now())",
                lid, ws, project_id, title, k, pos, created_by)
    where = f" in project '{project_title}'" if project_title else ""
    what = "private list" if private else "shared list"
    why = " (its project is private)" if forced_private else ""
    return f"Created {what} '{title}' ({k}){where}{why}. [id {lid}]"


# --- Today / Focus pins --------------------------------------------------------

# Today in Pacific, computed by Postgres (the MCP's Windows venv has no tzdata,
# so zoneinfo can't load America/Los_Angeles here). Same day as the backend's
# _today_local(); the server, never a client clock, owns the date.
_TODAY_PT_SQL = "(now() AT TIME ZONE 'America/Los_Angeles')::date"


async def _pin_today(conn, item_id: uuid.UUID, ws: str | None = None, *, day=None,
                     rank=None, restamp: bool = False) -> bool:
    """Pin ONE item to Today, the way the backend does it
    (services/planner_links.pin_source_today + the PATCH pin_today): today_pinned
    = true, today_date = `day` or today (PT), today_rank = `rank` or the END of the
    Today bucket. `ws` defaults to the shared workspace (the planner's source
    items). An already-pinned item keeps its place; with `restamp` (or an
    explicit day/rank) its day/rank are re-stamped, as the PATCH re-stamps the
    date. Returns True when the item was newly pinned."""
    ws = ws or USER_ID
    row = await conn.fetchrow(
        "SELECT today_pinned FROM shared_list_items WHERE workspace_id=$1 AND id=$2 "
        "FOR UPDATE", ws, item_id)
    if row is None:
        return False
    if row["today_pinned"]:
        if restamp or day is not None or rank is not None:
            await _stamp_today(conn, item_id, ws, day, rank, default_today=True)
        return False
    if rank is None:
        top = await conn.fetchval(
            "SELECT MAX(today_rank) FROM shared_list_items WHERE workspace_id=$1 "
            "AND today_pinned", ws)
        rank = (float(top) if top is not None else 0.0) + _RANK_STEP
    await conn.execute(
        f"UPDATE shared_list_items SET today_pinned=true, "
        f"today_date=COALESCE($3::date, {_TODAY_PT_SQL}), today_rank=$4::float8, "
        "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2",
        ws, item_id, day, float(rank))
    return True


async def _stamp_today(conn, item_id, ws: str, day=None, rank=None, *,
                       default_today: bool = False) -> None:
    """Re-stamp today_date / today_rank without changing the pin. With
    `default_today`, a missing day means today (PT); otherwise it is kept."""
    fallback = _TODAY_PT_SQL if default_today else "today_date"
    await conn.execute(
        f"UPDATE shared_list_items SET today_date=COALESCE($3::date, {fallback}), "
        "today_rank=COALESCE($4::float8, today_rank), updated_by='james', updated_at=now() "
        "WHERE workspace_id=$1 AND id=$2", ws, item_id, day,
        float(rank) if rank is not None else None)


async def _unpin_today(conn, item_id, ws: str) -> None:
    """PATCH pin_today=false: today_pinned = false, today_date = NULL."""
    await conn.execute(
        "UPDATE shared_list_items SET today_pinned=false, today_date=NULL, "
        "updated_by='james', updated_at=now() WHERE workspace_id=$1 AND id=$2", ws, item_id)


async def _pin_focus(conn, item_id, ws: str) -> bool:
    """PATCH pin_focus=true: is_focus = true and, on a fresh pin, focus_rank at
    the END of the Focus bucket. Returns True when it changed."""
    row = await conn.fetchrow(
        "SELECT is_focus FROM shared_list_items WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
        ws, item_id)
    if row is None or row["is_focus"]:
        return False
    top = await conn.fetchval(
        "SELECT MAX(focus_rank) FROM shared_list_items WHERE workspace_id=$1 AND is_focus "
        "AND id <> $2", ws, item_id)
    await conn.execute(
        "UPDATE shared_list_items SET is_focus=true, focus_rank=$3, updated_by='james', "
        "updated_at=now() WHERE workspace_id=$1 AND id=$2",
        ws, item_id, (float(top) if top is not None else 0.0) + _RANK_STEP)
    return True


async def _unpin_focus(conn, item_id, ws: str) -> None:
    """PATCH pin_focus=false: out of the Focus bucket (focus_rank is kept, as
    the PATCH keeps it)."""
    await conn.execute(
        "UPDATE shared_list_items SET is_focus=false, updated_by='james', updated_at=now() "
        "WHERE workspace_id=$1 AND id=$2", ws, item_id)


# --- shared lists: read + targeted add ------------------------------------------

async def get_shared_lists(query: str | None = None) -> str:
    """James's Shared Lists (the shared workspace, plus his private lists once
    private mode exists): id, title, project, open count, quick_default,
    private. Also the ids of the lists titled Today / Focus / Fathom and of the
    Quick ToDo (is_quick_default) list, so callers target them by id. JSON."""
    q = (query or "").strip()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            r"""
            SELECT l.id, l.title, l.is_quick_default, l.workspace_id, p.title AS project,
                   (SELECT count(*) FROM shared_list_items i
                     WHERE i.list_id = l.id AND i.workspace_id = l.workspace_id
                       AND i.parent_item_id IS NULL AND NOT i.done) AS open_items
              FROM shared_lists l
              LEFT JOIN shared_projects p
                ON p.id = l.project_id AND p.workspace_id = l.workspace_id
             WHERE l.workspace_id = ANY($1::text[]) AND l.archived = false
               AND ($2::text = '' OR l.title ILIKE $3 ESCAPE '\')
             ORDER BY l.is_quick_default DESC, (lower(l.title) = 'today') DESC,
                      (l.workspace_id = $4) DESC, p.title NULLS FIRST, l.position, l.title
            """,
            _LIST_WORKSPACES, q, _like_arg(q), USER_ID,
        )

        async def titled(t: str) -> list:
            return await conn.fetch(
                "SELECT id FROM shared_lists WHERE workspace_id=$1 AND archived=false "
                "AND lower(title) = $2 ORDER BY created_at", USER_ID, t)

        today, focus, fathom = await titled("today"), await titled("focus"), await titled("fathom")
        quick = await conn.fetchval(
            "SELECT id FROM shared_lists WHERE workspace_id=$1 AND archived=false "
            "AND is_quick_default ORDER BY created_at LIMIT 1", USER_ID)

    def one(found) -> str | None:
        return str(found[0]["id"]) if len(found) == 1 else None

    out = {
        "today_list_id": one(today),
        "quick_todo_list_id": str(quick) if quick else None,
        "focus_list_id": one(focus),
        "fathom_list_id": one(fathom),
        "lists": [{"id": str(r["id"]), "title": r["title"], "project": r["project"],
                   "open_items": r["open_items"], "quick_default": r["is_quick_default"],
                   "private": _is_private_ws(r["workspace_id"])}
                  for r in rows],
    }
    warnings = [f"{len(found)} lists are titled '{name}'; pick one by id: "
                + ", ".join(str(r["id"]) for r in found)
                for name, found in (("Today", today), ("Focus", focus), ("Fathom", fathom))
                if len(found) > 1]
    if warnings:
        out["warning"] = " | ".join(warnings)
    return _json.dumps(out, indent=1)


async def add_shared_item(list_title: str = "", text: str = "", priority: int | None = None,
                          due_date: str | None = None, list_id: str | None = None,
                          pin_today: bool = False, *, pin_focus: bool = False,
                          today_date: str | None = None, today_rank=None,
                          note: str | None = None, source: str | None = None) -> str:
    """Add an item to ONE named shared list, chosen by `list_id` (exact) or
    `list_title` (exact, or a unique partial match; no Quick ToDo fallback, use
    add_task for that). Searches the shared workspace and James's private lists
    workspace; never the planner. Same row shape as backend POST
    /v1/shared-lists/{id}/items: rank = end of the priority band, position =
    end of the list, created_by/updated_by 'james', origin_by = the write path.
    pin_today also pins it to Today (today_date / today_rank override the day /
    the order); pin_focus adds it to the Focus bucket."""
    text = (text or "").strip()
    if not text:
        return "Item text is empty."
    if not (list_id or "").strip() and not (list_title or "").strip():
        return "Give list_id or list_title."
    try:
        pr = _priority(priority)
        due = _due(due_date)
        day = _day(today_date)
        trank = _rank(today_rank)
    except ValueError as e:
        return str(e)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        try:
            lst, _note = await _resolve_list(conn, list_title, list_id, fallback_quick=False)
        except ValueError as e:
            return str(e)
        iid = await _insert_item(conn, lst, text, note=(note or "").strip() or None,
                                 priority=pr, due=due,
                                 origin=_origin(source, "mcp:add_shared_item"),
                                 pin_today=pin_today, today_date=day, today_rank=trank,
                                 pin_focus=pin_focus)
    p = f" at P{pr}" if pr else ""
    t = " (pinned to Today)" if pin_today else ""
    f = " (in Focus)" if pin_focus else ""
    return f"Added to '{_list_label(lst)}'{p}{t}{f}: '{text}'. [id {iid}]"


async def pin_shared_item_today(item_id: str, pin: bool = True) -> str:
    """Pin (or unpin) an EXISTING shared item to Today. Pin of an already-pinned
    item re-stamps its day, as the PATCH does; unpin mirrors PATCH
    pin_today=false: today_pinned = false, today_date = NULL."""
    iid = _uuid_or_none(item_id)
    if iid is None:
        return "item_id must be a UUID."
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await _load_todo(conn, iid, for_update=True)
            if row is None:
                return "No such shared item for this workspace."
            ws = row["workspace_id"]
            if pin:
                changed = await _pin_today(conn, iid, ws, restamp=True)
            else:
                changed = bool(row["today_pinned"])
                await _unpin_today(conn, iid, ws)
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
