"""
Read-only adapter to the Life-Omnia Neon Postgres DB.

Wired to the REAL Omnia Lists schema (omnia_lists / omnia_tasks), confirmed from
the omnia-lists-todos domain export. Tasks live here now — no Todoist.

To run it you supply two things in .env (never in code/chat):
  OMNIA_DB_DSN    postgresql://<readonly-user>:<pw>@<host>/<db>?sslmode=require
  OMNIA_USER_ID   your Omnia user_id (RLS/tenant key; everything is scoped to it)

Safety: use a READ-ONLY Neon role (SQL to create one is in the README). The
connection also forces read-only transactions as a second guard.
"""

from __future__ import annotations

import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

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


# --- Lists / Tasks (typed, real schema) --------------------------------------

async def get_lists() -> str:
    rows = await _rows(
        """
        SELECT name, kind, is_template, archived
        FROM omnia_lists
        WHERE user_id = $1 AND NOT archived
        ORDER BY kind, name
        """,
        USER_ID,
    )
    if not rows:
        return "No Omnia lists found."
    out = []
    for r in rows:
        tag = " [template]" if r["is_template"] else ""
        out.append(f"- {r['name']} ({r['kind']}){tag}")
    return "\n".join(out)


async def get_tasks(status: str = "open", list_name: str | None = None,
                    due: str | None = None) -> str:
    where = ["t.user_id = $1"]
    args: list = [USER_ID]

    # status: "open" (default) | "done" | "all"
    if status == "open":
        where.append("t.status <> 'done'")
    elif status == "done":
        where.append("t.status = 'done'")
    # "all" -> no status filter

    if due == "today":
        where.append("t.due_date <= CURRENT_DATE")
    elif due == "week":
        where.append("t.due_date <= CURRENT_DATE + INTERVAL '7 days'")
    elif due == "overdue":
        where.append("t.due_date < CURRENT_DATE AND t.status <> 'done'")

    if list_name:
        args.append(f"%{list_name}%")
        where.append(f"l.name ILIKE ${len(args)}")

    sql = f"""
        SELECT t.title, t.due_date, t.priority, t.status, t.rrule, l.name AS list
        FROM omnia_tasks t
        LEFT JOIN omnia_lists l ON l.id = t.list_id
        WHERE {' AND '.join(where)}
        ORDER BY t.due_date NULLS LAST, t.position
        LIMIT 200
    """
    rows = await _rows(sql, *args)
    if not rows:
        return "No tasks match that filter."
    out = []
    for r in rows:
        lst = f" [{r['list']}]" if r["list"] else ""
        due_s = f" (due {r['due_date']})" if r["due_date"] else ""
        pri = f" !P{r['priority']}" if r["priority"] not in (None, 0) else ""
        rec = " ↻" if r["rrule"] else ""
        done = " ✓" if r["status"] == "done" else ""
        out.append(f"- {r['title']}{lst}{due_s}{pri}{rec}{done}")
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
