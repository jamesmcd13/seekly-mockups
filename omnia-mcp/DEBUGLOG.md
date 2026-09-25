# omnia-mcp Debug Log

Newest first.

---

## 2026-09-25: zoneinfo has no tzdata in the MCP venv; DSN normalizer could emit "?&sslmode"

**Symptom:** while adding `pin_today` (planner follow-ups), importing
`omnia_write` under the MCP's own venv raised
`ZoneInfoNotFoundError: 'No time zone found with key America/Los_Angeles'`.
Separately, a Neon branch DSN of the form `?channel_binding=require&sslmode=require`
normalized to `?&sslmode=require`, which asyncpg rejects (`bad query field: ''`).

**Context:** caught on the Neon-branch check before shipping; prod never saw
either. The live server would have died at import (a module-level `ZoneInfo`).

**Root cause:** (1) Windows Python ships no IANA tz database; `zoneinfo` needs
the `tzdata` package, which the venv does not have. (2) `_normalize_dsn` removed
`channel_binding=...` with a regex that left the separator behind when it was
the FIRST query field (same bug class as backend B2, 2026-09-25).

**Fix:** today-in-Pacific is computed by Postgres
(`(now() AT TIME ZONE 'America/Los_Angeles')::date`), no `zoneinfo` import.
`_normalize_dsn` rebuilds the query with `urlsplit`/`parse_qsl`/`urlencode`
(verified: the prod DSN normalizes to the same string as before).

**Prevention:** never use `zoneinfo` in omnia-mcp without adding `tzdata` to
requirements; import-test every change with `.venv/Scripts/python.exe`, not the
system Python.

**Tags:** #omnia-mcp #windows #timezone #dsn #asyncpg

---

## 2026-09-01 — Omnia MCP write tools couldn't create tasks-with-dates or events

**Symptom:** `add_task` with a `due_date` and `create_event` both 500'd. Errors:
`'str' object has no attribute 'toordinal'` (add_task), and
`expected a datetime.date or datetime.datetime instance, got 'str'` (create_event).

**Context:** Surfaced live while the GV brain tried to execute an approved
"Dr. Pai appointment + 4 reminders" command. Nothing dated could be written to
Omnia — a core reason the brain "wasn't working."

**Root cause:**
1. `omnia_write.add_task` passed the raw `due_date` string to asyncpg for the
   `omnia_tasks.due_date` (DATE) column. asyncpg requires a `datetime.date`.
2. `omnia_write.create_event` passed `start_at`/`end_at` strings for
   `timestamptz` columns; the `$5::timestamptz` cast does not make asyncpg accept
   a str — it still wants a `datetime`.
3. Deeper: `omnia_events` is a **read-model of externally-synced calendars**.
   `CHECK ck_omnia_events_calendar_provider` restricts `calendar_provider` to
   `google`/`outlook`, and `calendar_id` + `end_at` are NOT NULL. A local-only
   event cannot be inserted at all. `create_event` was written against an older
   schema and never actually worked against prod.

**Fix:**
- `add_task`: parse `due_date` → `date.fromisoformat()` (guarded); pass the date.
- `create_event`: validate timestamps, then return a clear message that Omnia
  events sync from Outlook/Google and can't be created locally — instead of a
  500. Real appointments must be created on the user's Outlook/Google calendar
  (ms365 / gcal), which then syncs into Omnia.
- Dr. Pai appointment was created on the CCRE Outlook calendar via ms365; the 4
  reminders were created as Omnia tasks in the "Personal" list.

**Prevention:** MCP write tools must construct DB-typed values (date/datetime),
never pass user strings straight to asyncpg. When a table is a sync mirror, the
write tool must target the source system, not INSERT into the mirror.

**Follow-up:** the running MCP server processes still hold the OLD code — they
need a restart (Claude Code MCP reconnect) to pick up the add_task date fix. The
GV brain's `create_event` whitelist action should eventually route to
Outlook/Google rather than `omnia_events`.

**Tags:** #omnia-mcp #asyncpg #date-encoding #schema-drift #gv-brain
