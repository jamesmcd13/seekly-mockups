"""
Omnia MCP server — exposes your live Omnia data to Claude Code as on-demand tools.

This is your OPERATOR tool: it loads your actual life/Omnia data (tasks, calendar,
contexts, messages) so you can strategize in Claude Code with full context — and it
deliberately knows NOTHING about the website's source code.

Progressive disclosure: each tool pulls ONE slice on demand. Nothing is bulk-loaded.

Run:    python server.py        (stdio transport, which is what Claude Code uses)
Wire:   see omnia_client.py for the API details to fill in.
"""

from mcp.server.fastmcp import FastMCP
from omnia_client import OmniaClient

mcp = FastMCP("omnia")
omnia = OmniaClient()


@mcp.tool()
async def get_tasks(status: str = "active", context: str = "", due: str = "") -> str:
    """Get the user's tasks. Pull only what's relevant — don't fetch everything.

    Args:
        status: "active" (default), "proposed", "done", or "all".
        context: optional folder/context name to filter by (e.g. "Errands").
        due: optional window — "today", "week", or "overdue".
    """
    return await omnia.get_tasks(status=status, context=context or None, due=due or None)


@mcp.tool()
async def get_calendar(range: str = "week") -> str:
    """Get calendar events.

    Args:
        range: "today", "week" (default), "month", or an ISO range "YYYY-MM-DD..YYYY-MM-DD".
    """
    return await omnia.get_calendar(range=range)


@mcp.tool()
async def search_contexts(query: str = "") -> str:
    """List or search the user's contexts/folders (Work, a specific deal, etc.).

    Args:
        query: optional search text. Omit to list all contexts.
    """
    return await omnia.search_contexts(query=query or None)


@mcp.tool()
async def get_messages(query: str = "", limit: int = 20) -> str:
    """Get recent or matching messages from the user's Omnia inbox.

    Args:
        query: optional search text. Omit for the most recent messages.
        limit: max messages to return (default 20).
    """
    return await omnia.get_messages(query=query or None, limit=limit)


if __name__ == "__main__":
    mcp.run()
