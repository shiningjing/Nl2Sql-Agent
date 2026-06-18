"""MCP tool: validate_sql — L1 regex + L3 AST SQL validation.

Usage:
    python tools/mcp/validate_sql_server.py
    # Stdio MCP server, connect via fastmcp Client or MCP inspector.

Input:  {sql, dialect}
Output: {valid, issues[], statement_type, table_references}

Delegates to guard.safety_rules.check_safety() for all validation logic.
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from fastmcp import FastMCP
from guard.safety_rules import check_safety

mcp = FastMCP("validate-sql")


@mcp.tool
def validate_sql(sql: str, dialect: str = "sqlite") -> dict:
    """Validate a SQL string for safety and syntax.

    Performs L1 regex checks (forbidden keywords, multi-statement) then
    L3 AST validation via sqlglot. Returns structured issues and metadata.
    """
    return check_safety(sql, dialect)


if __name__ == "__main__":
    mcp.run()
