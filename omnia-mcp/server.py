"""
Omnia MCP server — exposes your live Life-Omnia data to Claude Code.

Reads (omnia_client) are strictly read-only. Writes (omnia_write) are narrow:
single-row, id-keyed, scoped to James's workspaces, no bulk statements, no DDL;
deletes run only behind a confirmed target.

Operator tool: pull your real Omnia data (tasks/lists now; pipeline/contacts via
introspection) on demand for strategy work — separate from the website's code.

To-dos live in Shared Lists (shared_lists / shared_list_items); the old Omnia
Lists (omnia_lists / omnia_tasks) are retired and read-only. No Todoist.

Run:  python server.py     (stdio transport — what Claude Code uses)
Wire: see omnia_client.py + README (read-only Neon role + .env).
"""

from mcp.server.fastmcp import FastMCP
import omnia_client as omnia
import omnia_write as omniaw

mcp = FastMCP("omnia")


@mcp.tool()
async def get_lists(include_legacy: bool = False) -> str:
    """List James's SHARED LISTS (where every to-do lives): title, project, open
    count, [default] = Quick ToDo, [private] = a private list, and each list's
    id. The old Omnia Lists are retired (hidden in the app).

    Args:
        include_legacy: true = also list the retired Omnia Lists (read-only).
    """
    return await omnia.get_lists(include_legacy=include_legacy)


@mcp.tool()
async def get_tasks(status: str = "open", list_name: str = "", due: str = "",
                    include_legacy: bool = False) -> str:
    """Get James's to-dos from SHARED LISTS. Pull only what's relevant — not
    everything. Each line ends with the item id (for complete_task /
    update_todo / delete_todo).

    Args:
        status: "open" (default, not done), "done", or "all".
        list_name: optional list name to filter by (partial match, e.g. "Quick").
        due: optional — "today" (due today or earlier, or pinned to Today),
             "week", or "overdue".
        include_legacy: true = also show items from the retired Omnia Lists.
    """
    return await omnia.get_tasks(status=status, list_name=list_name or None, due=due or None,
                                 include_legacy=include_legacy)


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


# --- WRITE tools ------------------------------------------------------------------
# To-dos live in SHARED LISTS only: the old Omnia Lists (omnia_tasks /
# omnia_lists) are retired and nothing here writes to them (complete_task can
# still check an old one off). Every write is scoped to James's list workspaces
# and stamps its provenance in origin_by (`source`, default 'mcp:<tool>').
# Behavioral contract for agents: adds/edits are reversible; deletes only after
# the user confirmed the exact target.

@mcp.tool()
async def add_task(title: str, list_name: str = "", due_date: str = "",
                   description: str = "", priority: int | None = None,
                   pin_today: bool = False, pin_focus: bool = False,
                   list_id: str = "", source: str = "") -> str:
    """Add a to-do to a SHARED LIST (Omnia's one to-do store). With no list, or a
    list name that doesn't exist, it lands in Quick ToDo (the reply says so).
    Old list names meaning today ("Top to Do Today") go to the Today list,
    pinned. Verify afterward with get_tasks.

    Args:
        title: the to-do text.
        list_name: a shared list's title (exact or unique partial match; an
            ambiguous name is refused, pass list_id). Empty = Quick ToDo.
        due_date: optional 'YYYY-MM-DD'.
        description: optional longer note.
        priority: optional 1..5 (1 = P1, highest). Quick ToDo defaults to P1.
        pin_today: true = also pin it to Today.
        pin_focus: true = also add it to Focus (today's must-dos).
        list_id: optional list UUID (from get_lists / get_shared_lists); wins.
        source: optional provenance tag, e.g. 'brain:gv', 'claude:desktop'.
    """
    return await omniaw.add_task(title, list_name, due_date or None, description or None,
                                 priority=priority, pin_today=pin_today, pin_focus=pin_focus,
                                 source=source or None, list_id=list_id or None)


@mcp.tool()
async def add_todo(text: str, priority: int = 1, pin_today: bool = False,
                   pin_focus: bool = False, source: str = "") -> str:
    """Add a to-do to the DEFAULT shared quick list (the 'Quick ToDo' shared list).
    This is the default home for an unqualified "add a to-do" / "remind me to X" /
    "put X on my list" when NO specific list is named. For a SPECIFIC named list
    (Groceries, Hawaii, etc.) use add_task instead.

    Adds are reversible, so no confirmation is needed — just add it.

    Args:
        text: the to-do text.
        priority: 1..5, where 1 = P1 (highest). Defaults to 1 (P1).
        pin_today: true = also pin it to Today.
        pin_focus: true = also add it to Focus.
        source: optional provenance tag, e.g. 'brain:gv'.
    """
    return await omniaw.add_shared_todo(text, priority, pin_today=pin_today,
                                        pin_focus=pin_focus, source=source or None)


@mcp.tool()
async def complete_task(task_id: str) -> str:
    """Check off ONE to-do (a Shared Lists item: done + done_at, and a planner
    task planned from it is finished too). An id from the retired Omnia Lists
    still works. Needs the item's UUID (get_tasks prints it).

    Args:
        task_id: the to-do UUID.
    """
    return await omniaw.complete_task(task_id)


@mcp.tool()
async def update_todo(kind: str, item_id: str, text: str = "", due_date: str = "",
                      priority: int | None = None, note: str = "", list_id: str = "",
                      list_title: str = "", pin_today: bool | None = None,
                      today_date: str = "", today_rank: float | None = None,
                      pin_focus: bool | None = None) -> str:
    """Edit ONE existing Shared Lists to-do (rename / reschedule / re-prioritize /
    move / pin). Only the fields you pass change. Edits are reversible, so no
    confirmation is needed. Returns 'Updated.' on success.

    Args:
        kind: 'task' or 'shared' (both mean a Shared Lists item now; kept for
            compatibility). Items in the retired Omnia Lists are read-only.
        item_id: the row UUID (get_tasks prints it).
        text: new text (optional).
        due_date: new due date 'YYYY-MM-DD', or 'clear' (optional).
        priority: new priority 1..5 (1 = P1), or 0 to clear it (optional).
        note: new note (optional).
        list_id / list_title: move it to another list (same shared/private side).
        pin_today: true = pin to Today, false = unpin (optional).
        today_date: 'YYYY-MM-DD' Today stamp (optional; default today when pinning).
        today_rank: order within Today, lower = earlier (optional).
        pin_focus: true = add to Focus, false = remove (optional).
    """
    return await omniaw.update_todo(kind, item_id, text or None, due_date or None, priority,
                                    note=note or None, list_id=list_id or None,
                                    list_title=list_title or None, pin_today=pin_today,
                                    today_date=today_date or None, today_rank=today_rank,
                                    pin_focus=pin_focus)


@mcp.tool()
async def delete_todo(kind: str, item_id: str) -> str:
    """DELETE ONE Shared Lists to-do (and its subtasks). DESTRUCTIVE: only call
    after the user has confirmed the exact target with the literal word DELETE.
    Reversible via DB point-in-time restore, but do not call speculatively.
    Items in the retired Omnia Lists are refused (read-only).

    Args:
        kind: 'task' or 'shared' (both mean a Shared Lists item now).
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
    """NOT AVAILABLE: this tool creates NOTHING and always returns an ERROR (it
    raises; FastMCP turns that into an isError tool result, the server keeps
    running). Create appointments on the user's Outlook or Google calendar (the
    ms365 / gcal tools). Omnia-side creation goes through the backend
    POST /v1/events in a later update. Args kept for signature stability.

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
async def add_list(name: str, kind: str = "todo", project: str = "", private: bool = False,
                   source: str = "") -> str:
    """Create a new SHARED LIST (skips duplicates: an existing list with that
    title is returned instead). Visible to Michael, like every shared list,
    unless private=true.

    Args:
        name: list name.
        kind: 'todo' (default) or 'longterm' (a standing list); also accepts the
            Shared Lists kinds main / project / standing.
        project: optional existing Shared Lists project to file it under.
        private: true = James-only private list (never shown to Michael). A list
            filed in a private project is always private.
        source: optional provenance tag, e.g. 'brain:gv'.
    """
    return await omniaw.add_list(name, kind, project or "", private=private,
                                 source=source or None)



@mcp.tool()
async def get_shared_lists(query: str = "") -> str:
    """List James's SHARED lists (the /shared-lists page, plus his private lists
    once private mode exists): id, title, project, open item count, private.
    Also returns `today_list_id` (the list titled "Today"), `quick_todo_list_id`
    (the Quick ToDo default list), `focus_list_id` and `fathom_list_id`, so you
    can target them by id with add_shared_item(list_id=...). JSON.

    Args:
        query: optional case-insensitive title filter.
    """
    return await omniaw.get_shared_lists(query or None)


@mcp.tool()
async def add_shared_item(list_title: str = "", text: str = "", priority: int | None = None,
                          due_date: str = "", list_id: str = "",
                          pin_today: bool = False, pin_focus: bool = False,
                          today_date: str = "", today_rank: float | None = None,
                          note: str = "", source: str = "") -> str:
    """Add an item to ONE named shared list, by `list_id` (exact, from
    get_shared_lists) or `list_title` (exact, or a unique partial match). No
    fallback: an unknown list is an error (add_task falls back to Quick ToDo).
    Michael can see shared lists; private lists are James's only.

    Args:
        list_title: the shared list's title (used when list_id is empty).
        text: the item text (required).
        priority: optional 1..5 (1 = P1, highest).
        due_date: optional 'YYYY-MM-DD'.
        list_id: optional list UUID; wins over list_title.
        pin_today: true = also pin the new item to Today (the /shared-lists
            TODAY band: today_pinned, today_date = today in Pacific, appended
            at the end of Today's order).
        pin_focus: true = also add it to Focus (today's must-dos).
        today_date: optional 'YYYY-MM-DD' Today stamp (with pin_today).
        today_rank: optional order within Today, lower = earlier (with
            pin_today; e.g. 1000, 2000, 3000 for a Top 3).
        note: optional longer note.
        source: optional provenance tag, e.g. 'claude:plan-day'.
    """
    return await omniaw.add_shared_item(list_title, text, priority, due_date or None,
                                        list_id or None, pin_today, pin_focus=pin_focus,
                                        today_date=today_date or None, today_rank=today_rank,
                                        note=note or None, source=source or None)



@mcp.tool()
async def pin_shared_item_today(item_id: str, pin: bool = True) -> str:
    """Pin an EXISTING shared-list item to Today (or unpin it with pin=false).
    The item stays in its own list; it just shows in the TODAY band, tagged.

    Args:
        item_id: the shared item UUID (find it with the `query` tool).
        pin: true = pin to Today (default), false = unpin.
    """
    return await omniaw.pin_shared_item_today(item_id, pin)


@mcp.tool()
async def planner_get_day(date: str) -> str:
    """James's PRIVATE planner for one day: time blocks (start/end, project,
    status, Busy flag, lanes 1|2 + lane_names, checkin_at) each with ONE shared
    ordered task queue; each task has lane (A/B in a 2-lane block), status,
    actual_seconds (sum of timer runs, pauses excluded), running_since and
    source_item_id. JSON.

    Args:
        date: 'YYYY-MM-DD' (Pacific day).
    """
    return await omniaw.planner_get_day(date)


@mcp.tool()
async def planner_set_day(date: str, blocks: list[dict]) -> str:
    """Create or REPLACE the whole private plan for one day. Blocks of that day
    not listed are deleted; a listed block's tasks not listed are deleted; the
    list order is the shared queue order. Pass ids (from planner_get_day) to keep
    rows and their timers. Private: never visible to Michael. Refused if it would
    drop or re-time a Busy block: call planner_set_busy(id, False) first.

    Two lanes: set "lanes": 2 (and optionally "lane_names": ["Urgent", "Omnia"])
    on the block and "lane": "A"|"B" on each task. Do NOT put "A: "/"B: " in
    task text; if you do, it is read as the lane and stripped. Omitting lanes
    keeps a block's stored lanes.

    Args:
        date: 'YYYY-MM-DD' (Pacific day).
        blocks: [{"id"?: str, "title": str, "start": "HH:MM" | ISO, "end": "HH:MM" | ISO,
                  "project"?: seekly|omnia|ccre|cobuy|18th|michael|personal|other,
                  "note"?: str, "lanes"?: 1 | 2, "lane_names"?: [str, str],
                  "tasks": [{"id"?: str, "text": str, "lane"?: "A" | "B",
                             "project"?: str, "note"?: str}]}]
    """
    return await omniaw.planner_set_day(date, blocks)


@mcp.tool()
async def planner_add_task(block_id: str, text: str, lane: str = "", project: str = "",
                           note: str = "", position: int | None = None,
                           source_item_id: str = "") -> str:
    """Add ONE task to a planner block (same as the app's "+ task" / Plan it).

    Args:
        block_id: the block UUID from planner_get_day.
        text: the task text (no "A: "/"B: " prefix; use `lane`).
        lane: 'A' or 'B' for a 2-lane block (default A; ignored for 1-lane).
        project: optional project tag (default other).
        note: optional note.
        position: 0-based slot in the block's shared order (default end).
        source_item_id: optional shared-list item this task is planned from:
            it gets pinned to Today and is checked off when the task is done.
    """
    return await omniaw.planner_add_task(block_id, text, lane or None, project or None,
                                         note or None, position, source_item_id or None)


@mcp.tool()
async def planner_start(task_id: str) -> str:
    """Start the timer on one planner task (opens a timer run; max 2 running at once).

    Args:
        task_id: the task UUID from planner_get_day.
    """
    return await omniaw.planner_start(task_id)


@mcp.tool()
async def planner_stop(task_id: str, accomplished: bool = False, note: str = "") -> str:
    """Stop a planner task's timer (closes its timer run). accomplished=true marks
    it done and also checks off the shared-list item it was planned from
    (source_item_id); false pauses it (back to planned). When every task in a
    block is done the block is done.

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
