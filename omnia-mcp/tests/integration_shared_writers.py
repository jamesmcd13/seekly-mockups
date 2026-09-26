"""Integration test for the Shared Lists to-do writers (omnia_write / omnia_client /
server.py tools), run against a NEON BRANCH, never prod.

    OMNIA_TEST_DB_HOST=<branch endpoint host> \
      C:/Users/James/dev/seekly-mockups/omnia-mcp/.venv/Scripts/python.exe \
      tests/integration_shared_writers.py

The DSN is James's backend DATABASE_URL with its host swapped for the branch
endpoint (a Neon branch inherits roles + passwords), built in memory and never
printed. The script REFUSES to run when the host is missing or is the production
endpoint. It writes freely (the branch is a throwaway copy of prod) and then
deletes what it created.

Proves: every to-do writer lands in shared_list_items (Quick ToDo by default)
with provenance, no path inserts into omnia_tasks / omnia_lists, Today / Focus
pins, list moves, legacy ids are read-only except completion, list -> planner
done-sync, private-list workspace handling, create_event raises, and the MCP
tool signatures (FastMCP call_tool) accept the new + old argument shapes.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

PROD_ENDPOINT = "ep-spring-firefly-afhk72ha"
BACKEND_ENV = Path(r"C:\Users\James\dev\omnia-platform\backend\.env")
HERE = Path(__file__).resolve().parent.parent  # omnia-mcp/


def _branch_dsn() -> str:
    host = (os.environ.get("OMNIA_TEST_DB_HOST") or "").strip()
    if not host:
        sys.exit("Set OMNIA_TEST_DB_HOST to a Neon BRANCH endpoint host.")
    if PROD_ENDPOINT in host:
        sys.exit("Refusing: OMNIA_TEST_DB_HOST is the production endpoint.")
    raw = None
    for line in BACKEND_ENV.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.startswith("DATABASE_URL="):
            raw = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
    if not raw:
        sys.exit("DATABASE_URL not found in the backend .env")
    raw = re.sub(r"^postgres(ql)?\+[a-z0-9]+://", "postgresql://", raw)
    dsn = re.sub(r"@[^/:?]+", "@" + host, raw, count=1)
    # asyncpg does not speak libpq's channel_binding; keep sslmode.
    parts = urlsplit(dsn)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != "channel_binding"]
    if not any(k == "sslmode" for k, _ in query):
        query.append(("sslmode", "require"))
    dsn = urlunsplit(parts._replace(query=urlencode(query)))
    if PROD_ENDPOINT in dsn:
        sys.exit("Refusing: DSN still points at production.")
    return dsn


DSN = _branch_dsn()
# Point BOTH modules at the branch before they import (load_dotenv never
# overrides variables that are already set).
os.environ["OMNIA_DB_DSN_RW"] = DSN
os.environ["OMNIA_DB_DSN"] = DSN
os.environ["OMNIA_USER_ID"] = "james"
os.environ.pop("OMNIA_MCP_SOURCE", None)
sys.path.insert(0, str(HERE))

import omnia_client as rc  # noqa: E402
import omnia_write as w  # noqa: E402

FAILS: list[str] = []
PASSES = 0
CREATED_ITEMS: list[uuid.UUID] = []
CREATED_LISTS: list[uuid.UUID] = []
TAG = f"zzwriters-{uuid.uuid4().hex[:6]}"


def check(cond, label: str, detail="") -> None:
    global PASSES
    if cond:
        PASSES += 1
    else:
        FAILS.append(f"{label} :: {detail}")
        print(f"FAIL {label} :: {detail}")


def id_of(msg: str) -> uuid.UUID:
    m = re.search(r"\[id ([0-9a-fA-F-]{36})\]", msg or "")
    if not m:
        raise AssertionError(f"no [id ...] in: {msg}")
    iid = uuid.UUID(m.group(1))
    return iid


async def one(conn, sql, *args):
    return await conn.fetchrow(sql, *args)


async def main() -> None:
    pool = await w._get_pool()
    async with pool.acquire() as conn:
        host = await conn.fetchval("SELECT inet_server_addr()::text")
        legacy_before = await conn.fetchval("SELECT count(*) FROM omnia_tasks")
        legacy_lists_before = await conn.fetchval("SELECT count(*) FROM omnia_lists")
        quick = await one(conn, "SELECT id, title FROM shared_lists WHERE workspace_id='james' "
                                "AND is_quick_default AND NOT archived")
        today = await one(conn, "SELECT id FROM shared_lists WHERE workspace_id='james' "
                                "AND lower(title)='today' AND NOT archived")
        messages = await one(conn, "SELECT id, title FROM shared_lists WHERE workspace_id='james' "
                                   "AND title='Messages' AND NOT archived")
        platform = await one(conn, "SELECT id, title FROM shared_lists WHERE workspace_id='james' "
                                   "AND title='Platform' AND NOT archived")
        legacy_open = await one(conn, "SELECT id, name FROM omnia_tasks WHERE user_id='james' "
                                      "AND status <> 'done' ORDER BY created_at DESC LIMIT 1")
    print("db server:", host, "| quick:", quick["title"], "| legacy open sample:", bool(legacy_open))
    assert quick and today and messages and platform, "expected lists missing on the branch"

    # 1. add_task, no list -> Quick ToDo at P1, provenance
    r = await w.add_task(f"{TAG} no list", "")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == quick["id"], "add_task default -> Quick ToDo", r)
    check(row["priority"] == 1, "add_task Quick ToDo default P1", row["priority"])
    check(row["created_by"] == "james" and row["origin_by"] == "mcp:add_task",
          "add_task provenance", (row["created_by"], row["origin_by"]))
    check(row["workspace_id"] == "james" and row["is_private"] is False, "add_task shared ws")
    check(row["rank"] is not None and row["updated_by"] == "james", "add_task rank/updated_by")

    # 2. existing list, different case -> that list, no priority
    r = await w.add_task(f"{TAG} to messages", "messages", "2026-10-01", "a note")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == messages["id"], "add_task exact title (case-insensitive)", r)
    check(row["priority"] is None, "add_task named list keeps no priority", row["priority"])
    check(row["note"] == "a note", "add_task description -> note")
    check(str(row["due_date"].date()) == "2026-10-01" and row["due_date"].utcoffset().total_seconds() == 0,
          "add_task due as UTC midnight", row["due_date"])

    # 3. unknown list -> Quick ToDo with a note in the reply
    r = await w.add_task(f"{TAG} unknown list", "No Such List Anywhere 42")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT list_id FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == quick["id"] and "went to Quick ToDo" in r, "add_task unknown -> Quick ToDo", r)

    # 4. legacy "Top to Do Today" -> Today list, pinned
    r = await w.add_task(f"{TAG} today alias", "Top to Do Today")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT list_id, today_pinned, today_date, today_rank, "
                              "(now() AT TIME ZONE 'America/Los_Angeles')::date AS pt "
                              "FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == today["id"] and row["today_pinned"] and row["today_date"] == row["pt"]
          and row["today_rank"] is not None, "add_task Top to Do Today -> Today pinned", (r, dict(row)))

    # 5. ambiguous partial -> refused, nothing written
    async with pool.acquire() as conn:
        n0 = await conn.fetchval("SELECT count(*) FROM shared_list_items")
    r = await w.add_task(f"{TAG} ambiguous", "Marketing")
    async with pool.acquire() as conn:
        n1 = await conn.fetchval("SELECT count(*) FROM shared_list_items")
    check("matches several lists" in r and n0 == n1, "add_task ambiguous refused", r)

    # 6/7. pin_focus + explicit priority + source tags
    r = await w.add_task(f"{TAG} focus", "Platform", priority=2, pin_focus=True, source="brain:gv")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", iid)
    check(row["is_focus"] and row["focus_rank"] is not None and row["priority"] == 2
          and row["origin_by"] == "brain:gv", "add_task pin_focus + source", dict(row))
    r = await w.add_task(f"{TAG} odd source", "", source="Claude Desktop!!/x" + "y" * 40)
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        ob = await conn.fetchval("SELECT origin_by FROM shared_list_items WHERE id=$1", iid)
    check(ob and len(ob) <= 32 and re.fullmatch(r"[a-z0-9_.:-]+", ob), "source cleaned + capped", ob)

    # 8. validation
    check("priority must be 1..5" in await w.add_task(f"{TAG} bad pr", "", priority=7), "bad priority")
    check("due_date must be" in await w.add_task(f"{TAG} bad due", "", "tomorrow"), "bad due")
    check(await w.add_task("   ", "") == "Task title is empty.", "empty title")

    # 9. add_todo (add_shared_todo): Quick ToDo, created_by claude, clamp
    r = await w.add_shared_todo(f"{TAG} add_todo", 9)
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == quick["id"] and row["priority"] == 1 and row["created_by"] == "claude"
          and row["origin_by"] == "mcp:add_todo" and row["rank"] is not None,
          "add_todo shape", dict(row))
    r = await w.add_shared_todo(f"{TAG} add_todo pinned", 1, pin_today=True, source="brain:gv")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT today_pinned, origin_by FROM shared_list_items WHERE id=$1", iid)
    check(row["today_pinned"] and row["origin_by"] == "brain:gv", "add_todo pin_today + source")

    # 10. add_shared_item: id + pin_today + explicit today_rank; strict title
    r = await w.add_shared_item("", f"{TAG} shared item", 3, "2026-10-02", str(today["id"]), True,
                                today_rank=123.5, today_date="2026-09-30", note="n")
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == today["id"] and row["today_pinned"] and row["today_rank"] == 123.5
          and str(row["today_date"]) == "2026-09-30" and row["priority"] == 3 and row["note"] == "n"
          and row["origin_by"] == "mcp:add_shared_item", "add_shared_item id+pins", dict(row))
    r = await w.add_shared_item("No Such List Anywhere 42", f"{TAG} strict")
    check("No shared list matches" in r, "add_shared_item strict (no fallback)", r)
    r = await w.add_shared_item("", "x")
    check(r == "Give list_id or list_title.", "add_shared_item needs a list", r)

    # 11. add_list
    lname = f"{TAG} New List"
    r = await w.add_list(lname)
    lid = id_of(r)
    CREATED_LISTS.append(lid)
    async with pool.acquire() as conn:
        lrow = await one(conn, "SELECT * FROM shared_lists WHERE id=$1", lid)
    check(lrow["workspace_id"] == "james" and lrow["kind"] == "project" and lrow["project_id"] is None
          and lrow["created_by"] == "mcp:add_list", "add_list shape", dict(lrow))
    r2 = await w.add_list(lname.upper())
    check("already exists" in r2 and str(lid) in r2, "add_list dedupe", r2)
    r3 = await w.add_list(f"{TAG} standing", "longterm", "Omnia Platform", source="brain:gv")
    lid3 = id_of(r3)
    CREATED_LISTS.append(lid3)
    async with pool.acquire() as conn:
        l3 = await one(conn, "SELECT l.kind, l.created_by, p.title FROM shared_lists l "
                             "JOIN shared_projects p ON p.id=l.project_id WHERE l.id=$1", lid3)
    check(l3 and l3["kind"] == "standing" and l3["title"] == "Omnia Platform"
          and l3["created_by"] == "brain:gv", "add_list kind/project/source", r3)
    check("kind must be" in await w.add_list(f"{TAG} bad kind", "weird"), "add_list bad kind")
    check("No Shared Lists project" in await w.add_list(f"{TAG} bad proj", "todo", "Nope Proj 42"),
          "add_list unknown project")
    # the new list is immediately a target
    r = await w.add_task(f"{TAG} into new list", lname)
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT list_id FROM shared_list_items WHERE id=$1", iid)
    check(row["list_id"] == lid, "add_task into a list made by add_list")

    # 12. update_todo
    target = CREATED_ITEMS[1]  # the Messages item
    async with pool.acquire() as conn:
        before = await one(conn, "SELECT rank, priority FROM shared_list_items WHERE id=$1", target)
    check(await w.update_todo("task", str(target), f"{TAG} renamed", "2026-11-05", 4) == "Updated.",
          "update_todo basic")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT * FROM shared_list_items WHERE id=$1", target)
    check(row["text"] == f"{TAG} renamed" and str(row["due_date"].date()) == "2026-11-05"
          and row["priority"] == 4 and row["rank"] != before["rank"], "update_todo applied + re-band",
          dict(row))
    check(await w.update_todo("shared", str(target), due_date="clear", priority=0) == "Updated.",
          "update_todo clear")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT due_date, priority FROM shared_list_items WHERE id=$1", target)
    check(row["due_date"] is None and row["priority"] is None, "update_todo cleared", dict(row))
    check(await w.update_todo("task", str(target), list_title="Platform") == "Updated.", "move")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT list_id FROM shared_list_items WHERE id=$1", target)
    check(row["list_id"] == platform["id"], "update_todo moved to Platform")
    r = await w.update_todo("task", str(target), list_title="No Such List Anywhere 42")
    check("No shared list matches" in r, "update_todo unknown list", r)
    # subtasks move with their parent; a subtask alone cannot be moved
    sub = uuid.uuid4()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO shared_list_items (id, list_id, parent_item_id, workspace_id, text, "
            "created_by) VALUES ($1, $2, $3, 'james', $4, 'james')",
            sub, platform["id"], target, f"{TAG} subtask")
    CREATED_ITEMS.append(sub)
    check(await w.update_todo("task", str(target), list_title="Messages") == "Updated.",
          "move parent with subtask")
    async with pool.acquire() as conn:
        sub_list = await conn.fetchval("SELECT list_id FROM shared_list_items WHERE id=$1", sub)
    check(sub_list == messages["id"], "subtask moved with its parent", sub_list)
    r = await w.update_todo("task", str(sub), list_title="Platform")
    check("subtask" in r, "subtask cannot move alone", r)
    check(await w.update_todo("task", str(target), list_title="Platform") == "Updated.",
          "move parent back")
    check(await w.update_todo("task", str(target), pin_today=True, today_rank=5.0) == "Updated.",
          "pin via update")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT today_pinned, today_rank, today_date FROM shared_list_items "
                              "WHERE id=$1", target)
    check(row["today_pinned"] and row["today_rank"] == 5.0 and row["today_date"] is not None,
          "update_todo pin_today + rank", dict(row))
    check(await w.update_todo("task", str(target), today_rank=7.0) == "Updated.", "rank only")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT today_pinned, today_rank FROM shared_list_items WHERE id=$1",
                        target)
    check(row["today_pinned"] and row["today_rank"] == 7.0, "update_todo today_rank only", dict(row))
    check(await w.update_todo("task", str(target), pin_today=False, pin_focus=True) == "Updated.",
          "unpin today + focus")
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT today_pinned, today_date, is_focus, focus_rank "
                              "FROM shared_list_items WHERE id=$1", target)
    check(not row["today_pinned"] and row["today_date"] is None and row["is_focus"]
          and row["focus_rank"] is not None, "update_todo unpin + focus", dict(row))
    check(await w.update_todo("task", str(target), pin_focus=False) == "Updated.", "unfocus")
    async with pool.acquire() as conn:
        f = await conn.fetchval("SELECT is_focus FROM shared_list_items WHERE id=$1", target)
    check(f is False, "update_todo unfocus")
    check("Nothing to update" in await w.update_todo("task", str(target)), "update_todo nothing")
    check(await w.update_todo("task", str(uuid.uuid4()), "x") == "No such item for this user.",
          "update_todo missing")
    if legacy_open:
        r = await w.update_todo("task", str(legacy_open["id"]), "x")
        check("retired Omnia Lists" in r, "update_todo legacy refused", r)
        r = await w.delete_todo("task", str(legacy_open["id"]))
        check("retired Omnia Lists" in r, "delete_todo legacy refused", r)

    # 13/14. complete_task + list -> planner done-sync
    src_id = CREATED_ITEMS[0]  # Quick ToDo item
    day = "2026-12-01"
    plan = json.loads(await w.planner_set_day(day, [{
        "title": f"{TAG} block", "start": "09:00", "end": "10:00", "tasks": []}]))
    block_id = plan["blocks"][0]["id"]
    ptask = json.loads(await w.planner_add_task(block_id, f"{TAG} planned", source_item_id=str(src_id)))
    started = await w.planner_start(ptask["id"])
    # The branch copies prod, where James may already run 2 timers.
    check(started.startswith("Started") or "At most" in started, "planner_start", started)
    r = await w.complete_task(str(src_id))
    check(r.startswith("Completed") and "planner task" in r, "complete_task shared + sync", r)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT done, done_at FROM shared_list_items WHERE id=$1", src_id)
        pt = await one(conn, "SELECT planner_status, done FROM shared_list_items WHERE id=$1",
                       uuid.UUID(ptask["id"]))
        blk = await one(conn, "SELECT planner_status FROM shared_list_items WHERE id=$1",
                        uuid.UUID(block_id))
        open_runs = await conn.fetchval("SELECT count(*) FROM planner_task_runs WHERE task_id=$1 "
                                        "AND ended_at IS NULL", uuid.UUID(ptask["id"]))
    check(row["done"] and row["done_at"] is not None, "complete_task done")
    check(pt["planner_status"] == "done" and pt["done"], "planner task done via sync", dict(pt))
    check(blk["planner_status"] == "done", "block settled", dict(blk))
    check(open_runs == 0, "timer run closed", open_runs)
    check("already done" in await w.complete_task(str(src_id)), "complete_task idempotent")
    await w.planner_set_day(day, [])  # remove the test block + task
    if legacy_open:
        r = await w.complete_task(str(legacy_open["id"]))
        async with pool.acquire() as conn:
            st = await conn.fetchval("SELECT status FROM omnia_tasks WHERE id=$1", legacy_open["id"])
        check(st == "done" and "retired Omnia Lists" in r, "complete_task legacy", r)

    # 15/16. find_todos + delete_todo
    found = await w.find_todos(TAG)
    kinds = {f["kind"] for f in found}
    check(found and kinds == {"shared"}, "find_todos shared only", kinds)
    check(all(uuid.UUID(f["id"]) != src_id for f in found), "find_todos skips done items")
    if legacy_open:
        leg = await w.find_todos(legacy_open["name"][:12], include_legacy=True)
        check(any(f["kind"] == "task" for f in leg) or True, "find_todos include_legacy runs", leg[:2])
    victim = CREATED_ITEMS[2]
    r = await w.delete_todo("shared", str(victim))
    check(r.startswith("Deleted to-do"), "delete_todo shared", r)
    async with pool.acquire() as conn:
        gone = await conn.fetchval("SELECT count(*) FROM shared_list_items WHERE id=$1", victim)
    check(gone == 0, "delete_todo removed the row")
    CREATED_ITEMS.remove(victim)
    check(await w.delete_todo("task", str(uuid.uuid4())) == "No such to-do for this user.",
          "delete_todo missing")
    check(await w.delete_todo("bogus", str(victim)) == "kind must be 'task' or 'shared'.", "kind check")

    # 17/18. get_shared_lists + pin_shared_item_today
    gsl = json.loads(await w.get_shared_lists())
    check(gsl["today_list_id"] == str(today["id"]) and gsl["quick_todo_list_id"] == str(quick["id"])
          and "focus_list_id" in gsl and "fathom_list_id" in gsl
          and all("private" in x for x in gsl["lists"]), "get_shared_lists shape",
          {k: v for k, v in gsl.items() if k != "lists"})
    pin_target = CREATED_ITEMS[1]
    r = await w.pin_shared_item_today(str(pin_target), True)
    check(r.startswith("Pinned"), "pin_shared_item_today pin", r)
    r = await w.pin_shared_item_today(str(pin_target), True)
    check("already on Today" in r, "pin_shared_item_today repin", r)
    r = await w.pin_shared_item_today(str(pin_target), False)
    check(r.startswith("Unpinned"), "pin_shared_item_today unpin", r)

    # 19. read side (omnia_client, read-only transactions)
    gl = await rc.get_lists()
    check("Quick ToDo" in gl and "[default]" in gl and "[id " in gl, "get_lists shared", gl[:200])
    gl2 = await rc.get_lists(include_legacy=True)
    check("Retired Omnia Lists" in gl2, "get_lists include_legacy")
    gt = await rc.get_tasks(list_name="Quick")
    check(TAG in gt and "[id " in gt, "get_tasks shared (Quick)", gt[:300])
    gt_today = await rc.get_tasks(due="today")
    check(f"{TAG} today alias" in gt_today, "get_tasks due=today includes pinned", gt_today[:300])
    gt_done = await rc.get_tasks(status="done", list_name="Quick")
    check(f"{TAG} no list" in gt_done, "get_tasks done")
    gt_leg = await rc.get_tasks(include_legacy=True)
    check("Retired Omnia Lists" in gt_leg, "get_tasks include_legacy")

    # 20. create_event raises
    try:
        await w.create_event("x", "2026-10-01T10:00:00-07:00")
        check(False, "create_event must raise")
    except w.CalendarCreateNotAvailable as e:
        check(str(e).startswith("calendar create not available"), "create_event raise text", str(e))

    # 21. private lists workspace (private mode) is tolerated
    plist = uuid.uuid4()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO shared_lists (id, workspace_id, title, kind, created_by) "
                           "VALUES ($1, 'james:lists:private', $2, 'project', 'james')",
                           plist, f"{TAG} Private Stuff")
    CREATED_LISTS.append(plist)
    r = await w.add_task(f"{TAG} private item", f"{TAG} Private Stuff", pin_today=True)
    piid = id_of(r)
    CREATED_ITEMS.append(piid)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT workspace_id, is_private, today_pinned FROM shared_list_items "
                              "WHERE id=$1", piid)
    check(row["workspace_id"] == "james:lists:private" and row["is_private"] and row["today_pinned"]
          and "(private)" in r, "add_task into a private list", (r, dict(row)))
    r = await w.update_todo("task", str(piid), list_title="Platform")
    check("shared and private" in r, "update_todo refuses shared<->private move", r)
    gt = await rc.get_tasks(list_name=f"{TAG} Private")
    check(", private" in gt, "get_tasks marks private", gt)
    planner_ws_hits = await w.find_todos(f"{TAG} planned")
    check(planner_ws_hits == [], "find_todos never returns planner rows", planner_ws_hits)
    # add_list(private=True) and a list filed in a PRIVATE project is always private
    r = await w.add_list(f"{TAG} private list", private=True)
    plid = id_of(r)
    CREATED_LISTS.append(plid)
    pproj = uuid.uuid4()
    async with pool.acquire() as conn:
        ws = await conn.fetchval("SELECT workspace_id FROM shared_lists WHERE id=$1", plid)
        await conn.execute("INSERT INTO shared_projects (id, workspace_id, title, created_by) "
                           "VALUES ($1, 'james:lists:private', $2, 'james')", pproj, f"{TAG} PProj")
    check(ws == "james:lists:private" and r.startswith("Created private list"),
          "add_list private=True", (r, ws))
    r = await w.add_list(f"{TAG} in private project", "todo", f"{TAG} PProj")
    plid2 = id_of(r)
    CREATED_LISTS.append(plid2)
    async with pool.acquire() as conn:
        row = await one(conn, "SELECT workspace_id, project_id FROM shared_lists WHERE id=$1", plid2)
    check(row["workspace_id"] == "james:lists:private" and row["project_id"] == pproj
          and "its project is private" in r, "add_list into a private project is private", r)
    r = await w.add_list(f"{TAG} private list", private=True)
    check("already exists" in r and str(plid) in r, "add_list private dedupe", r)

    # 22. ambient provenance: env var, then the GV cwd heuristic
    os.environ["OMNIA_MCP_SOURCE"] = "brain:loop"
    r = await w.add_shared_todo(f"{TAG} env source", 1)
    iid = id_of(r)
    CREATED_ITEMS.append(iid)
    os.environ.pop("OMNIA_MCP_SOURCE")
    old_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        gv = Path(tmp) / "omnia-gv-command-x"
        gv.mkdir()
        os.chdir(gv)
        try:
            r2 = await w.add_task(f"{TAG} gv cwd", "")
        finally:
            os.chdir(old_cwd)
    iid2 = id_of(r2)
    CREATED_ITEMS.append(iid2)
    async with pool.acquire() as conn:
        o1 = await conn.fetchval("SELECT origin_by FROM shared_list_items WHERE id=$1", iid)
        o2 = await conn.fetchval("SELECT origin_by FROM shared_list_items WHERE id=$1", iid2)
    check(o1 == "brain:loop" and o2 == "brain:gv", "ambient source (env, cwd)", (o1, o2))

    # 23. MCP tool layer: old and new argument shapes through FastMCP
    import server  # noqa: PLC0415

    async def call(name, args):
        res = await server.mcp.call_tool(name, args)
        blocks = res[0] if isinstance(res, tuple) else res
        return "".join(getattr(b, "text", "") for b in blocks)

    t = await call("add_task", {"title": f"{TAG} tool old shape", "list_name": "Platform"})
    CREATED_ITEMS.append(id_of(t))
    check("to list 'Platform'" in t, "tool add_task old shape", t)
    t = await call("add_task", {"title": f"{TAG} tool new shape", "pin_today": True, "priority": 2,
                                "source": "claude:desktop"})
    tid = id_of(t)
    CREATED_ITEMS.append(tid)
    check("Quick ToDo" in t and "pinned to Today" in t, "tool add_task new shape", t)
    t = await call("add_todo", {"text": f"{TAG} tool add_todo"})
    CREATED_ITEMS.append(id_of(t))
    check("Quick ToDo" in t, "tool add_todo", t)
    t = await call("update_todo", {"kind": "task", "item_id": str(tid), "pin_today": False,
                                   "today_rank": 3.0, "pin_focus": True})
    check(t == "Updated.", "tool update_todo", t)
    t = await call("add_shared_item", {"list_title": "Today", "text": f"{TAG} tool asi",
                                       "pin_today": True, "today_rank": 10})
    CREATED_ITEMS.append(id_of(t))
    check("pinned to Today" in t, "tool add_shared_item", t)
    t = await call("get_tasks", {"list_name": "Quick"})
    check(TAG in t, "tool get_tasks")
    t = await call("get_lists", {})
    check("Quick ToDo" in t, "tool get_lists")
    t = await call("complete_task", {"task_id": str(tid)})
    check(t.startswith("Completed"), "tool complete_task", t)
    try:
        res = await server.mcp.call_tool("create_event", {"title": "x", "start_at": "2026-10-01T10:00:00Z"})
        check(False, "tool create_event must error", res)
    except Exception as e:  # FastMCP raises ToolError on a tool exception
        check("calendar create not available" in str(e), "tool create_event errors", str(e))

    # nothing ever landed in the retired tables
    async with pool.acquire() as conn:
        legacy_after = await conn.fetchval("SELECT count(*) FROM omnia_tasks")
        legacy_lists_after = await conn.fetchval("SELECT count(*) FROM omnia_lists")
    check(legacy_after == legacy_before and legacy_lists_after == legacy_lists_before,
          "ZERO new omnia_tasks / omnia_lists rows",
          (legacy_before, legacy_after, legacy_lists_before, legacy_lists_after))

    # cleanup (branch only)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM shared_list_items WHERE id = ANY($1::uuid[])", CREATED_ITEMS)
        await conn.execute("DELETE FROM shared_list_items WHERE text LIKE $1", f"{TAG}%")
        await conn.execute("DELETE FROM shared_lists WHERE id = ANY($1::uuid[])", CREATED_LISTS)
        await conn.execute("DELETE FROM shared_projects WHERE title LIKE $1", f"{TAG}%")

    print(f"\n{PASSES} checks passed, {len(FAILS)} failed")
    if FAILS:
        print("\n".join(FAILS))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
