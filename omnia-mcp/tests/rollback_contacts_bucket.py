"""Contacts-bucket test for the MCP contact writers (add/update/delete/find),
run against the REAL database inside ONE transaction that is always ROLLED
BACK, so nothing persists. Every writer's own `conn.transaction()` becomes a
SAVEPOINT inside the outer transaction.

    C:/Users/James/dev/seekly-mockups/omnia-mcp/.venv/Scripts/python.exe \
      tests/rollback_contacts_bucket.py

Proves: add_contact writes the /contacts book ('james_new', owner 'james',
book 'curated', visibility 'private', tags []), never the legacy 'james' book;
email dedup merges (blank-fill + notes append) instead of duplicating; phone
dedup when no email; update_contact PATCH semantics + live-email clash refusal
+ legacy ids unreachable; delete archives (recoverable), refuses shared rows,
and releases the email so a re-add works. get_contacts reads the new book.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

import asyncpg  # noqa: E402

import omnia_client as omnia  # noqa: E402
import omnia_write as w  # noqa: E402

FAILS: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("PASS " if cond else "FAIL ") + label)
    if not cond:
        FAILS.append(label)


class _OnePool:
    """Stands in for the asyncpg pool: always hands out the one connection that
    holds the outer (to-be-rolled-back) transaction."""

    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


async def main() -> None:
    conn = await asyncpg.connect(w._rw_dsn())
    outer = conn.transaction()
    await outer.start()
    try:
        async def fake_pool():
            return _OnePool(conn)

        w._get_pool = fake_pool
        B = w.CONTACTS_BUCKET
        check(B == "james_new", f"bucket is james_new (got {B})")
        tag = uuid.uuid4().hex[:10]
        email = f"zz-mcp-test-{tag}@example.invalid"
        email2 = f"zz-mcp-test2-{tag}@example.invalid"
        phone = f"+1626555{tag[:4].translate(str.maketrans('abcdef', '123456'))}"
        legacy_before = await conn.fetchval("SELECT count(*) FROM contacts WHERE user_id='james'")

        # 1. create
        msg = await w.add_contact(f"ZZ Test {tag}", email.upper(), phone, None, "met at open house")
        check(msg.startswith("Added contact"), f"add: {msg}")
        row = await conn.fetchrow("SELECT * FROM contacts WHERE lower(primary_email)=$1", email)
        check(row is not None and row["user_id"] == B, "row lands in james_new")
        check(row["primary_email"] == email, "email lowercased")
        check(row["owner_user_id"] == w.USER_ID == "james", "owner_user_id = james")
        check(row["book"] == "curated" and row["visibility"] == "private", "book curated / private")
        check(row["tags"] in ("[]", []) and row["is_active"] and row["archived_at"] is None,
              "tags [] + live")
        cid = row["id"]

        # 2a. same email, DIFFERENT name -> refused, nothing written, no '[id ' marker
        before = await conn.fetchrow("SELECT notes, company, updated_at FROM contacts WHERE id=$1", cid)
        msg = await w.add_contact("Different Name", email, None, "Acme", "x")
        after = await conn.fetchrow("SELECT notes, company, updated_at FROM contacts WHERE id=$1", cid)
        check("Not adding" in msg and f"(id {cid})" in msg and "[id " not in msg,
              f"same email, other name refused: {msg}")
        check(before == after, "refusal wrote nothing")

        # 2b. same email + same name (case/space-insensitive) -> merge, no duplicate
        msg = await w.add_contact(f"  zz test   {tag} ", f"  {email}  ", "+19999999999", "Acme",
                                  "birthday 3/4")
        check("merged company, notes" in msg and "Kept existing phone" in msg, f"merge: {msg}")
        n = await conn.fetchval("SELECT count(*) FROM contacts WHERE lower(primary_email)=$1", email)
        r = await conn.fetchrow("SELECT * FROM contacts WHERE id=$1", cid)
        check(n == 1, "no duplicate on same email")
        check(r["name"] == f"ZZ Test {tag}" and r["phone"] == phone, "name/phone not overwritten")
        check(r["notes"] == "met at open house\n\nbirthday 3/4", f"notes appended: {r['notes']!r}")

        msg = await w.add_contact(f"ZZ Test {tag}", email, None, None, "birthday 3/4")
        check("Nothing new" in msg, f"idempotent re-add: {msg}")

        # 3. no email, same phone in another format + same name -> merge; other name -> refuse
        digits = phone[2:]
        fmt = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        msg = await w.add_contact(f"ZZ Test {tag}", None, fmt, None, "text first")
        check(str(cid) in msg and "merged notes" in msg, f"phone dedup: {msg}")
        msg = await w.add_contact("Phone Other", None, fmt, None, "y")
        check("Not adding" in msg, f"same phone, other name refused: {msg}")

        # 3b. email held by TWO live rows (legacy mixed case) -> refused, lists both
        dup2 = uuid.uuid4()
        await conn.execute(
            "INSERT INTO contacts (id, user_id, owner_user_id, name, primary_email) "
            "VALUES ($1,$2,'james',$3,$4)", dup2, B, f"ZZ Test {tag}", email.upper())
        msg = await w.add_contact(f"ZZ Test {tag}", email, None, None, "z")
        check("Not adding" in msg and str(dup2) in msg and str(cid) in msg,
              f"multi-match email refused: {msg}")
        await conn.execute("UPDATE contacts SET archived_at=now(), primary_email=NULL WHERE id=$1", dup2)

        # 3c. append_notes on the locked row
        msg = await w.update_contact(str(cid), notes="  called 9/29  ", append_notes=True)
        r = await conn.fetchrow("SELECT notes FROM contacts WHERE id=$1", cid)
        check(msg == "Updated." and r["notes"].endswith("\ncalled 9/29")
              and r["notes"].startswith("met at open house"), f"append_notes: {r['notes']!r}")

        # 4. find_contacts
        found = await w.find_contacts(f"ZZ Test {tag}")
        check([f["id"] for f in found] == [str(cid)], "find_contacts finds it in james_new")

        # 5. update_contact
        msg = await w.update_contact(str(cid), name=f"ZZ Renamed {tag}", email=email2.upper())
        r = await conn.fetchrow("SELECT * FROM contacts WHERE id=$1", cid)
        check(msg == "Updated." and r["name"] == f"ZZ Renamed {tag}" and r["primary_email"] == email2,
              f"update name+email: {msg}")
        await conn.execute("UPDATE contacts SET primary_email=$2 WHERE id=$1", cid, email2.upper())
        msg = await w.update_contact(str(cid), email=email2)
        r = await conn.fetchrow("SELECT primary_email FROM contacts WHERE id=$1", cid)
        check(msg == "Updated." and r["primary_email"] == email2, f"case-only email change lowercases: {msg}")
        other = await conn.fetchrow(
            "SELECT id, primary_email FROM contacts WHERE user_id=$1 AND primary_email IS NOT NULL "
            "AND archived_at IS NULL AND deleted_at IS NULL AND is_active AND id<>$2 LIMIT 1", B, cid)
        msg = await w.update_contact(str(cid), email=other["primary_email"])
        check("already belongs" in msg, f"email clash refused: {msg}")
        msg = await w.update_contact(str(cid), name="   ")
        check(msg == "Name cannot be empty.", "blank name refused")
        legacy_id = await conn.fetchval(
            "SELECT id FROM contacts WHERE user_id='james' AND archived_at IS NULL LIMIT 1")
        msg = await w.update_contact(str(legacy_id), notes="should not land")
        check(msg.startswith("No such active contact"), "legacy 'james' row unreachable by update")
        msg = await w.soft_delete_contact(str(legacy_id))
        check(msg.startswith("No such active contact"), "legacy 'james' row unreachable by delete")
        mid = uuid.uuid4()
        await conn.execute(
            "INSERT INTO contacts (id, user_id, owner_user_id, name, visibility) "
            "VALUES ($1,'partner:zz-test','zz-test',$2,'shared')", mid, f"ZZ Shared {tag}")
        msg = await w.update_contact(str(mid), notes="no")
        check("Michael shared" in msg, f"shared-by-Michael update explained: {msg}")
        msg = await w.soft_delete_contact(str(mid))
        check("Michael shared" in msg, f"shared-by-Michael delete explained: {msg}")

        # 6. delete: shared refused; own archived; email released on re-add
        await conn.execute("UPDATE contacts SET visibility='shared' WHERE id=$1", cid)
        msg = await w.soft_delete_contact(str(cid))
        check("shared with Michael" in msg, f"shared delete refused: {msg}")
        await conn.execute("UPDATE contacts SET visibility='private' WHERE id=$1", cid)
        rid = await conn.fetchval(
            "INSERT INTO reminders (user_id, contact_id, due_at, source) "
            "VALUES ($1,$2,now()+interval '1 day','manual') RETURNING id", B, cid)
        msg = await w.soft_delete_contact(str(cid))
        r = await conn.fetchrow("SELECT archived_at, primary_email FROM contacts WHERE id=$1", cid)
        rem = await conn.fetchval("SELECT dismissed_at FROM reminders WHERE id=$1", rid)
        check(msg.startswith("Deleted contact") and r["archived_at"] is not None, f"archived: {msg}")
        check(rem is not None, "open manual follow-up dismissed")
        check(await w.find_contacts(f"ZZ Renamed {tag}") == [], "archived row hidden from find")
        msg = await w.add_contact(f"ZZ Again {tag}", email2)
        old = await conn.fetchval("SELECT primary_email FROM contacts WHERE id=$1", cid)
        check(msg.startswith("Added contact") and old is None, f"re-add frees archived email: {msg}")

        legacy_after = await conn.fetchval("SELECT count(*) FROM contacts WHERE user_id='james'")
        check(legacy_after == legacy_before, "zero rows written to the legacy 'james' book")
    finally:
        await outer.rollback()
        await conn.close()

    # after rollback: nothing persisted (fresh connection)
    conn = await asyncpg.connect(w._rw_dsn())
    left = await conn.fetchval(
        "SELECT count(*) FROM contacts WHERE primary_email LIKE 'zz-mcp-test%@example.invalid' "
        "OR name LIKE 'ZZ %' || $1", tag)
    await conn.close()
    check(left == 0, "rollback left nothing behind")

    # get_contacts (read-only role) reads the /contacts book
    out = await omnia.get_contacts("Colich", 20)
    print(out)
    check("[id " in out and "Michael Colich" in out, "get_contacts returns james_new rows with ids")
    out = await omnia.get_contacts("Elvis", 20)
    check("Elvis" not in out, "legacy-only 'Elvis' (james bucket) not listed")

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILED'}")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    asyncio.run(main())
