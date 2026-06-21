package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	s := server.NewMCPServer(
		"nl2sql-mcp-server",
		"0.1.0",
		server.WithToolCapabilities(true),
	)

	// ── validate_sql tool ──
	s.AddTool(mcp.NewTool("validate_sql",
		mcp.WithDescription("Validate a SQL string for safety and syntax. Returns structured issues and metadata."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("The SQL string to validate")),
		mcp.WithString("dialect", mcp.Description("SQL dialect: sqlite, mysql, or postgres (default: sqlite)")),
	), validateSQLHandler)

	// ── execute_readonly_sql tool ──
	s.AddTool(mcp.NewTool("execute_readonly_sql",
		mcp.WithDescription("Execute a read-only SQL query with hard safety limits. Only SELECT/WITH allowed."),
		mcp.WithString("sql", mcp.Required(), mcp.Description("The SQL query to execute (SELECT/WITH only)")),
		mcp.WithString("database_url", mcp.Required(), mcp.Description("SQLAlchemy-style database URL")),
		mcp.WithNumber("max_rows", mcp.Description("Maximum rows to return (default 200, hard cap 1000)")),
		mcp.WithNumber("timeout_ms", mcp.Description("Query timeout in milliseconds (default 60000, max 120000)")),
	), executeSQLHandler)

	if err := server.ServeStdio(s); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func getArgs(req mcp.CallToolRequest) map[string]interface{} {
	if args, ok := req.Params.Arguments.(map[string]interface{}); ok {
		return args
	}
	return map[string]interface{}{}
}

func validateSQLHandler(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := getArgs(req)
	sql, _ := args["sql"].(string)
	dialect, _ := args["dialect"].(string)
	if dialect == "" {
		dialect = "sqlite"
	}

	result := checkSafety(sql, dialect)
	jsonBytes, _ := json.Marshal(result)

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func executeSQLHandler(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := getArgs(req)
	sql, _ := args["sql"].(string)
	databaseURL, _ := args["database_url"].(string)

	maxRows := execDefaultMaxRows
	if v, ok := args["max_rows"].(float64); ok {
		maxRows = int(v)
	}
	timeoutMS := execDefaultTimeout
	if v, ok := args["timeout_ms"].(float64); ok {
		timeoutMS = int(v)
	}

	result := executeReadonlySQL(sql, databaseURL, maxRows, timeoutMS)
	jsonBytes, _ := json.Marshal(result)

	return mcp.NewToolResultText(string(jsonBytes)), nil
}
