"""
Adapter that talks to the live Omnia backend.

This is the ONLY file you have to wire to your real API. Everything the MCP
server exposes flows through here, so the rest of the code stays clean.

To finish wiring, fill in the three TODO areas below:
  1. OMNIA_API_BASE_URL + OMNIA_API_TOKEN (via .env)
  2. The auth header shape your API expects (Bearer? x-api-key?)
  3. The real endpoint paths + how to read the JSON each one returns
"""

from __future__ import annotations

import os
import httpx
from dotenv import load_dotenv

load_dotenv()  # read .env so OMNIA_API_BASE_URL / OMNIA_API_TOKEN are available


class OmniaClient:
    def __init__(self) -> None:
        # 1. CONFIG — set these in .env (see .env.example)
        self.base_url = os.environ["OMNIA_API_BASE_URL"].rstrip("/")
        self.token = os.environ["OMNIA_API_TOKEN"]

    def _headers(self) -> dict[str, str]:
        # 2. AUTH — adjust to whatever your API expects.
        #    Bearer token shown; swap to {"x-api-key": self.token} if that's your scheme.
        return {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}

    async def _get(self, path: str, params: dict | None = None) -> object:
        async with httpx.AsyncClient(timeout=20.0) as http:
            resp = await http.get(
                f"{self.base_url}{path}", headers=self._headers(), params=params or {}
            )
            resp.raise_for_status()
            return resp.json()

    # 3. ENDPOINTS — each method below assumes a path + response shape.
    #    Update the path strings and the formatting to match your real API.

    async def get_tasks(
        self, status: str = "active", context: str | None = None, due: str | None = None
    ) -> str:
        params = {"status": status}
        if context:
            params["context"] = context
        if due:
            params["due"] = due  # e.g. "today" | "week" | "overdue"
        data = await self._get("/api/tasks", params)  # TODO: real path
        return _format_tasks(data)

    async def get_calendar(self, range: str = "week") -> str:
        # range: "today" | "week" | "month" | ISO "2026-06-24..2026-06-30"
        data = await self._get("/api/calendar", {"range": range})  # TODO: real path
        return _format_calendar(data)

    async def search_contexts(self, query: str | None = None) -> str:
        params = {"q": query} if query else {}
        data = await self._get("/api/contexts", params)  # TODO: real path
        return _format_contexts(data)

    async def get_messages(self, query: str | None = None, limit: int = 20) -> str:
        params: dict = {"limit": limit}
        if query:
            params["q"] = query
        data = await self._get("/api/messages", params)  # TODO: real path
        return _format_messages(data)


# --- Formatters: turn raw JSON into compact, Claude-friendly markdown ---------
# Keep output terse — this lands directly in the Claude Code context window.

def _format_tasks(data: object) -> str:
    items = data.get("tasks", data) if isinstance(data, dict) else data
    if not items:
        return "No tasks found for that filter."
    lines = []
    for t in items:
        due = f" (due {t['due_date']})" if t.get("due_date") else ""
        ctx = f" [{t['context']}]" if t.get("context") else ""
        pri = f" !{t['priority']}" if t.get("priority") not in (None, "normal") else ""
        lines.append(f"- {t.get('title', '(untitled)')}{ctx}{due}{pri}")
    return "\n".join(lines)


def _format_calendar(data: object) -> str:
    items = data.get("events", data) if isinstance(data, dict) else data
    if not items:
        return "No calendar events in that range."
    return "\n".join(
        f"- {e.get('start', '?')}: {e.get('title', '(untitled)')}"
        + (f" @ {e['location']}" if e.get("location") else "")
        for e in items
    )


def _format_contexts(data: object) -> str:
    items = data.get("contexts", data) if isinstance(data, dict) else data
    if not items:
        return "No matching contexts/folders."
    return "\n".join(
        f"- {c.get('name', '(unnamed)')} ({c.get('kind', 'context')})"
        + (f" — {c['open_task_count']} open" if c.get("open_task_count") is not None else "")
        for c in items
    )


def _format_messages(data: object) -> str:
    items = data.get("messages", data) if isinstance(data, dict) else data
    if not items:
        return "No matching messages."
    return "\n".join(
        f"- {m.get('date', '?')} {m.get('from', '?')}: {m.get('snippet', m.get('body', ''))[:160]}"
        for m in items
    )
