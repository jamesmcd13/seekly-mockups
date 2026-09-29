# omnia-mcp Debug Log

Newest first.

---

## 2026-09-29: contacts added through the MCP never appeared on /contacts

**Symptom:** `add_contact` returned "Added contact ..." but the person never
showed on the /contacts page; the /add skill had to route contacts to a to-do
instead.

**Context:** the /contacts page is the partner-contacts surface
(`GET/POST /v1/partner-contacts`, omnia-platform
`backend/app/routes/contacts_partner.py`). Its `_bucket_for` maps James to the
`contacts.user_id` bucket `"james_new"` (the "new curated book": 10 hand-entered
+ 1,552 promoted by `import_legacy_contacts.py` on 08-12). The MCP scoped every
contact read/write by `OMNIA_USER_ID` = `"james"`, which is the LEGACY book now
shown only at /contacts-old.

**Tried:** checked the legacy bucket for MCP-written rows since the 08-12
import: none (the only new rows, 3 on 08-31, came from the BlueBubbles sync,
`contact_identifiers.source='bluebubbles'`). So the bug was latent: nothing was
lost yet, but the next MCP add would have vanished.

**Root cause:** the contacts store split into two buckets (Aug 2026) and the MCP
was never repointed; the old bucket still accepts inserts, so nothing failed.
Same class as the 2026-09-25 retired-Omnia-Lists entry below.

**Fix:** `CONTACTS_BUCKET = "james_new"` (env override
`OMNIA_CONTACTS_BUCKET`). add/update/soft_delete/find_contacts + get_contacts
mirror the API: owner_user_id='james', book/visibility/tags server defaults,
lowercased email, live = archived_at/deleted_at NULL + is_active, non-live
holders release their email, delete = archived_at + dismiss open manual
follow-ups. Deliberate differences: add MERGES into a live same-email (or,
without email, same 10-digit phone) contact with the SAME name instead of 409
(other name or several matches -> refused, nothing written); update_contact
gains append_notes (appends on the row-locked contact); delete keeps the
activity/extras sidecars (reversible) and refuses shared rows; edits never touch
Michael's shared rows. Proven by `tests/rollback_contacts_bucket.py` (prod, one
transaction, always rolled back; 36 checks).

**Prevention:** any store keyed by a bucket that is NOT the tenant key needs
the bucket named once (constant) and a test that counts rows written to the old
bucket (the rollback test asserts zero). When a backend adds a second book,
grep the MCP + scripts for `user_id` writers to that table.

**Tags:** #contacts #bucket #james_new #mcp #silent-wrong-target

---

## 2026-09-25: to-dos added through the MCP vanished (they landed in the retired Omnia Lists)

**Symptom:** 4 "URGENT" to-dos added today with `add_task` (plus `add_list` /
`complete_task` / `update_todo kind=task` edits) never showed up anywhere James
looks. `add_todo` items sorted last in Top Priorities.

**Context:** planner PR #432 (omnia-platform) hid the old Omnia Lists
(`omnia_tasks` / `omnia_lists`) and made Shared Lists the only list UI. The MCP
still INSERTed `add_task` rows into `omnia_tasks` and `add_list` rows into
`omnia_lists`, so every such write went straight into a hidden table.
Separately, `add_shared_todo` never set `rank`, so its rows sorted after every
ranked item in the P1 band.

**Root cause:** the writers were never repointed when the list store changed;
the old tables still accept inserts, so nothing failed loudly.

**Fix:** every to-do writer targets `shared_list_items` / `shared_lists`
(Quick ToDo default; list lookup by title across the shared workspace and the
future `<uid>:lists:private`, ambiguous names refused, unknown -> Quick ToDo),
with rank/position computed like the backend, provenance in `origin_by`, Today /
Focus pins, list -> planner done-sync on complete. Legacy ids: complete only.
Proven on a Neon branch by `tests/integration_shared_writers.py` (88 checks,
incl. zero new omnia_tasks / omnia_lists rows).

**Prevention:** when a store is retired, grep every writer (MCP, scripts,
skills, backend) BEFORE hiding its UI, and leave a check that counts new rows in
the retired table. `tests/integration_shared_writers.py` asserts it.

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
