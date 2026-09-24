"""
Omnia MCP server — exposes your live Life-Omnia data to Claude Code.

Reads (omnia_client) are strictly read-only. Writes (omnia_write) are narrow and
gated: INSERT + a single scoped status-UPDATE only, no DELETE/DDL, scoped to
user_id, and meant to run behind an agent "propose-then-push" confirmation.

Operator tool: pull your real Omnia data (tasks/lists now; pipeline/contacts via
introspection) on demand for strategy work — separate from the website's code.

Tasks live in Omnia Lists (omnia_lists / omnia_tasks). No Todoist.

Run:  python server.py     (stdio transport — what Claude Code uses)
Wire: see omnia_client.py + README (read-only Neon role + .env).
"""

from mcp.server.fastmcp import FastMCP
import omnia_client as omnia
import omnia_write as omniaw

mcp = FastMCP("omnia")


@mcp.tool()
async def get_lists() -> str:
    """List the user's Omnia lists (To-do and Long Term), excluding archived."""
    return await omnia.get_lists()


@mcp.tool()
async def get_tasks(status: str = "open", list_name: str = "", due: str = "") -> str:
    """Get the user's Omnia tasks. Pull only what's relevant — not everything.

    Args:
        status: "open" (default, not done), "done", or "all".
        list_name: optional list name to filter by (partial match, e.g. "Main St").
        due: optional — "today", "week", or "overdue".
    """
    return await omnia.get_tasks(status=status, list_name=list_name or None, due=due or None)


@mcp.tool()
async def get_events(days_ahead: int = 7) -> str:
    """Get the user's UPCOMING calendar events (read-only), from now through the
    next `days_ahead` days, soonest first. Use for "what's on my schedule",
    "what's tomorrow", etc.

    Args:
        days_ahead: how many days ahead to include (default 7).
    """
    return await omnia.get_events(days_ahead=days_ahead)


@mcp.tool()
async def get_contacts(query: str = "", limit: int = 20) -> str:
    """Look up the user's contacts (read-only): returns name, phone, email.
    Excludes soft-deleted contacts. Use for "find Sarah's number", "what's Bob's
    email", etc.

    Args:
        query: optional search text; case-insensitive match on name/email/phone.
               Omit to list contacts.
        limit: max rows to return (default 20).
    """
    return await omnia.get_contacts(query=query or None, limit=limit)


@mcp.tool()
async def list_tables() -> str:
    """List all tables in the Omnia DB (to discover pipeline/contacts/etc.)."""
    return await omnia.list_tables()


@mcp.tool()
async def describe_table(name: str) -> str:
    """Show a table's columns + types, so new typed tools can be added safely.

    Args:
        name: exact table name (from list_tables).
    """
    return await omnia.describe_table(name)


@mcp.tool()
async def query(sql: str) -> str:
    """Run a single read-only SELECT/WITH query against the Omnia DB.

    Use for pipeline/contacts/messages until typed tools exist. SELECT only;
    one statement; results capped at 100 rows. Always scope by your user_id.

    Args:
        sql: a single SELECT or WITH statement.
    """
    return await omnia.run_select(sql)


# --- WRITE tools (INSERT + safe status-UPDATE only; no DELETE/DDL) ------------
# Behavioral contract for agents: PROPOSE the change to the user and get an
# explicit OK BEFORE calling any of these, then verify with a get_tasks read-back.

@mcp.tool()
async def add_task(title: str, list_name: str, due_date: str = "",
                   description: str = "") -> str:
    """Add a to-do to an existing Omnia list. Propose-then-push: confirm with the
    user first. Verify afterward with get_tasks.

    Args:
        title: the task text.
        list_name: an existing list (exact or unique partial match; ambiguous names are rejected).
        due_date: optional 'YYYY-MM-DD'.
        description: optional longer note.
    """
    return await omniaw.add_task(title, list_name, due_date or None, description or None)


@mcp.tool()
async def add_todo(text: str, priority: int = 1) -> str:
    """Add a to-do to the DEFAULT shared quick list (the 'Quick ToDo' shared list).
    This is the default home for an unqualified "add a to-do" / "remind me to X" /
    "put X on my list" when NO specific list is named. For a SPECIFIC named list
    (Groceries, Hawaii, etc.) use add_task instead.

    Adds are reversible, so no confirmation is needed — just add it.

    Args:
        text: the to-do text.
        priority: 1..5, where 1 = P1 (highest). Defaults to 1 (P1).
    """
    return await omniaw.add_shared_todo(text, priority)


@mcp.tool()
async def complete_task(task_id: str) -> str:
    """Mark an Omnia task done (sets status + logs a completion). Needs the task's
    UUID. Propose-then-push: confirm with the user first.

    Args:
        task_id: the task UUID (query it via the `query` tool if you only have the title).
    """
    return await omniaw.complete_task(task_id)


@mcp.tool()
async def update_todo(kind: str, item_id: str, text: str = "", due_date: str = "",
                      priority: int | None = None) -> str:
    """Edit ONE existing to-do (rename / reschedule / re-prioritize). Only the
    fields you pass change. Edits are reversible, so no confirmation is needed.

    Args:
        kind: 'task' for an Omnia task, 'shared' for a shared-list to-do.
        item_id: the row UUID (find it with the `query` tool by matching text first).
        text: new text/title (optional).
        due_date: new due date 'YYYY-MM-DD' (optional).
        priority: new priority 1..5, 1=P1 (optional).
    """
    return await omniaw.update_todo(kind, item_id, text or None, due_date or None, priority)


@mcp.tool()
async def delete_todo(kind: str, item_id: str) -> str:
    """DELETE ONE to-do. DESTRUCTIVE: only call after the user has confirmed the
    exact target with the literal word DELETE. Reversible via DB point-in-time
    restore, but do not call speculatively.

    Args:
        kind: 'task' for an Omnia task, 'shared' for a shared-list to-do.
        item_id: the row UUID (resolve + echo the exact item to the user first).
    """
    return await omniaw.delete_todo(kind, item_id)


@mcp.tool()
async def update_contact(contact_id: str, name: str = "", phone: str = "",
                         email: str = "", company: str = "", notes: str = "") -> str:
    """Edit ONE existing contact. Only the fields you pass change. Reversible; no
    confirmation needed.

    Args:
        contact_id: the contact UUID (find it with the `query` tool first).
        name/phone/email/company/notes: new values (pass only what changes).
    """
    return await omniaw.update_contact(contact_id, name or None, phone or None,
                                       email or None, company or None, notes or None)


@mcp.tool()
async def delete_contact(contact_id: str) -> str:
    """SOFT-DELETE ONE contact (recoverable; sets deleted_at). DESTRUCTIVE-ish:
    only call after the user confirmed the exact contact with the word DELETE.

    Args:
        contact_id: the contact UUID (resolve + echo the exact contact first).
    """
    return await omniaw.soft_delete_contact(contact_id)


@mcp.tool()
async def create_event(title: str, start_at: str, end_at: str = "",
                       all_day: bool = False, location: str = "",
                       description: str = "") -> str:
    """DEPRECATED: Omnia calendar events sync from Outlook/Google and cannot be
    created locally, so this tool does NOT create anything — it returns a message
    telling you to add the appointment to the user's Outlook or Google calendar
    (via the ms365 / gcal tools), which then syncs into Omnia. Args kept for
    signature stability.

    Args:
        title: event name.
        start_at: ISO timestamp, e.g. '2026-08-12T10:00:00-07:00'.
        end_at: optional ISO end timestamp.
        all_day: true for an all-day event.
        location: optional.
        description: optional.
    """
    return await omniaw.create_event(title, start_at, end_at or None, all_day,
                                     location or None, description or None)


@mcp.tool()
async def add_contact(name: str, email: str = "", phone: str = "",
                      company: str = "", notes: str = "") -> str:
    """Add a contact to Omnia (skips exact-email duplicates). Propose-then-push:
    confirm with the user first. Note: no birthday column — put dates in notes.

    Args:
        name: full name.
        email: optional primary email.
        phone: optional.
        company: optional.
        notes: optional free text (birthdays, how you know them, etc.).
    """
    return await omniaw.add_contact(name, email or None, phone or None,
                                    company or None, notes or None)


@mcp.tool()
async def add_list(name: str, kind: str = "todo") -> str:
    """Create a new Omnia list (skips duplicates). Propose-then-push: confirm first.

    Args:
        name: list name.
        kind: 'todo' (default) or 'longterm'.
    """
    return await omniaw.add_list(name, kind)



@mcp.tool()
async def add_shared_item(list_title: str, text: str, priority: int | None = None,
                          due_date: str = "") -> str:
    """Add an item to a NAMED shared list (e.g. 'CoBuyLA', 'Construction').
    Michael can see shared lists. For the default quick list use add_todo.

    Args:
        list_title: the shared list's title (exact, or a unique partial match).
        text: the item text.
        priority: optional 1..5 (1 = P1, highest).
        due_date: optional 'YYYY-MM-DD'.
    """
    return await omniaw.add_shared_item(list_title, text, priority, due_date or None)


@mcp.tool()
async def planner_get_day(date: str) -> str:
    """James's PRIVATE planner for one day: time blocks (start/end, project,
    status, Busy flag) each with an ordered task queue and timers. JSON.

    Args:
        date: 'YYYY-MM-DD' (Pacific day).
    """
    return await omniaw.planner_get_day(date)


@mcp.tool()
async def planner_set_day(date: str, blocks: list[dict]) -> str:
    """Create or REPLACE the whole private plan for one day. Blocks of that day
    not listed are deleted; a listed block's tasks not listed are deleted; the
    list order is the queue order. Pass ids (from planner_get_day) to keep rows
    and their timers. Private: never visible to Michael.

    Args:
        date: 'YYYY-MM-DD' (Pacific day).
        blocks: [{"id"?: str, "title": str, "start": "HH:MM" | ISO, "end": "HH:MM" | ISO,
                  "project"?: seekly|omnia|ccre|cobuy|18th|michael|personal|other,
                  "note"?: str,
                  "tasks": [{"id"?: str, "text": str, "project"?: str, "note"?: str}]}]
    """
    return await omniaw.planner_set_day(date, blocks)


@mcp.tool()
async def planner_start(task_id: str) -> str:
    """Start the timer on one planner task (max 2 running at once).

    Args:
        task_id: the task UUID from planner_get_day.
    """
    return await omniaw.planner_start(task_id)


@mcp.tool()
async def planner_stop(task_id: str, accomplished: bool = False, note: str = "") -> str:
    """Stop a planner task's timer. accomplished=true marks it done; false pauses
    it (back to planned). When every task in a block is done the block is done.

    Args:
        task_id: the task UUID.
        accomplished: true = done, false = paused.
        note: optional note to store on the task (replaces the old note).
    """
    return await omniaw.planner_stop(task_id, accomplished, note or None)


@mcp.tool()
async def planner_snooze(task_id: str, to_block_id: str = "") -> str:
    """Move a planner task to the END of the next block that day (or to
    to_block_id). A running timer is stopped.

    Args:
        task_id: the task UUID.
        to_block_id: optional target block UUID.
    """
    return await omniaw.planner_snooze(task_id, to_block_id or None)


@mcp.tool()
async def planner_set_busy(block_id: str, busy: bool) -> str:
    """Show one planner block to others as Busy (creates ONE Outlook event
    "Focus block" on James's CCRE calendar, never the task names) or make it
    private again (deletes only that event). Goes through the Omnia backend.

    Args:
        block_id: the block UUID.
        busy: true = Busy, false = private.
    """
    return await omniaw.planner_set_busy(block_id, busy)


if __name__ == "__main__":
    mcp.run()
