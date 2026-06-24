"""
Omnia MCP server — exposes your live Life-Omnia data to Claude Code, read-only.

Operator tool: pull your real Omnia data (tasks/lists now; pipeline/contacts via
introspection) on demand for strategy work — separate from the website's code.

Tasks live in Omnia Lists (omnia_lists / omnia_tasks). No Todoist.

Run:  python server.py     (stdio transport — what Claude Code uses)
Wire: see omnia_client.py + README (read-only Neon role + .env).
"""

from mcp.server.fastmcp import FastMCP
import omnia_client as omnia

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


if __name__ == "__main__":
    mcp.run()
