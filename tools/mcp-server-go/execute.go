package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	execMaxRowsHardCap = 1000
	execDefaultMaxRows = 200
	execDefaultTimeout = 60_000 // ms
	execMaxTimeout     = 120_000
)

// ExecResult is the structured output of execute_readonly_sql.
type ExecResult struct {
	Success      bool     `json:"success"`
	Error        *string  `json:"error"`
	ErrorType    *string  `json:"error_type"`
	Data         [][]any  `json:"data"`
	Columns      []string `json:"columns"`
	RowCount     int      `json:"row_count"`
	ExecutionMS  int64    `json:"execution_ms"`
}

func executeReadonlySQL(sqlStr, databaseURL string, maxRows, timeoutMS int) ExecResult {
	t0 := time.Now()

	// ── Phase 1: input validation ──
	if err := validateInput(sqlStr, maxRows, timeoutMS); err != nil {
		elapsed := time.Since(t0).Milliseconds()
		err.ErrorType = strPtr("invalid_input")
		err.ExecutionMS = elapsed
		return *err
	}

	// Derive dialect from URL
	dialect := dialectFromURL(databaseURL)

	// Safety check
	safety := checkSafety(sqlStr, dialect)
	if !safety.Valid {
		elapsed := time.Since(t0).Milliseconds()
		detail := "Unknown validation error"
		if len(safety.Issues) > 0 {
			detail = safety.Issues[0].Detail
		}
		return ExecResult{
			Success:     false,
			Error:        strPtr(detail),
			ErrorType:    strPtr("sql_validation"),
			ExecutionMS: elapsed,
		}
	}

	// ── Phase 2: execute ──
	return doExecute(sqlStr, databaseURL, dialect, maxRows, timeoutMS, t0)
}

func validateInput(sqlStr string, maxRows, timeoutMS int) *ExecResult {
	trimmed := strings.TrimSpace(sqlStr)
	if trimmed == "" {
		return &ExecResult{
			Success:   false,
			Error:     strPtr("SQL is empty."),
			ErrorType: strPtr("invalid_input"),
		}
	}
	if maxRows > execMaxRowsHardCap {
		return &ExecResult{
			Success: false,
			Error:   strPtr(fmt.Sprintf("max_rows %d exceeds hard limit %d", maxRows, execMaxRowsHardCap)),
		}
	}
	if timeoutMS > execMaxTimeout {
		return &ExecResult{
			Success: false,
			Error:   strPtr(fmt.Sprintf("timeout_ms %d exceeds hard limit %d", timeoutMS, execMaxTimeout)),
		}
	}
	return nil
}

func dialectFromURL(url string) string {
	lower := strings.ToLower(url)
	if strings.HasPrefix(lower, "postgresql") {
		return "postgres"
	}
	if strings.HasPrefix(lower, "mysql") {
		return "mysql"
	}
	return "sqlite"
}

func doExecute(sqlStr, databaseURL, dialect string, maxRows, timeoutMS int, t0 time.Time) ExecResult {
	// Auto-wrap LIMIT
	sqlToRun := autoLimit(sqlStr, maxRows)

	// Open DB connection
	driverName := driverForURL(databaseURL)
	db, err := sql.Open(driverName, dsnFromURL(databaseURL))
	if err != nil {
		elapsed := time.Since(t0).Milliseconds()
		return ExecResult{
			Success:     false,
			Error:        strPtr(err.Error()),
			ErrorType:    strPtr("execution_error"),
			ExecutionMS: elapsed,
		}
	}
	defer db.Close()

	// Connection-level timeout via context
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		elapsed := time.Since(t0).Milliseconds()
		errStr := err.Error()
		errType := "execution_error"
		if strings.Contains(strings.ToLower(errStr), "timeout") {
			errType = "timeout"
		}
		return ExecResult{
			Success:     false,
			Error:       &errStr,
			ErrorType:   &errType,
			ExecutionMS: elapsed,
		}
	}
	defer conn.Close()

	// Set statement timeout for supported dialects
	setStatementTimeout(conn, dialect, timeoutMS)

	// Execute
	rows, err := conn.QueryContext(ctx, sqlToRun)
	if err != nil {
		elapsed := time.Since(t0).Milliseconds()
		errStr := err.Error()
		errType := "execution_error"
		if strings.Contains(strings.ToLower(errStr), "timeout") {
			errType = "timeout"
		}
		return ExecResult{
			Success:     false,
			Error:       &errStr,
			ErrorType:   &errType,
			ExecutionMS: elapsed,
		}
	}
	defer rows.Close()

	// Read columns
	columns, err := rows.Columns()
	if err != nil {
		elapsed := time.Since(t0).Milliseconds()
		errStr := err.Error()
		errType := "execution_error"
		return ExecResult{
			Success:     false,
			Error:       &errStr,
			ErrorType:   &errType,
			ExecutionMS: elapsed,
		}
	}

	// Read rows
	var data [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			elapsed := time.Since(t0).Milliseconds()
			errStr := err.Error()
			return ExecResult{
				Success:     false,
				Error:       &errStr,
				ErrorType:   strPtr("execution_error"),
				ExecutionMS: elapsed,
			}
		}
		data = append(data, values)
	}

	elapsed := time.Since(t0).Milliseconds()
	return ExecResult{
		Success:     true,
		Data:        data,
		Columns:     columns,
		RowCount:    len(data),
		ExecutionMS: elapsed,
	}
}

func autoLimit(sqlStr string, maxRows int) string {
	clean := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sqlStr), ";"))
	if !strings.Contains(strings.ToUpper(clean), "LIMIT") {
		return fmt.Sprintf("SELECT * FROM (%s) AS _sub LIMIT %d", clean, maxRows)
	}
	return clean
}

func driverForURL(url string) string {
	lower := strings.ToLower(url)
	if strings.HasPrefix(lower, "postgresql") {
		return "postgres"
	}
	if strings.HasPrefix(lower, "mysql") {
		return "mysql"
	}
	return "sqlite3"
}

func dsnFromURL(url string) string {
	const sqlitePrefix = "sqlite:///"
	if strings.HasPrefix(url, sqlitePrefix) {
		return strings.TrimPrefix(url, sqlitePrefix)
	}
	// PostgreSQL and MySQL URLs pass through
	return url
}

func setStatementTimeout(conn *sql.Conn, dialect string, timeoutMS int) {
	timeoutSec := timeoutMS / 1000
	if timeoutSec < 1 {
		timeoutSec = 1
	}
	switch dialect {
	case "postgres":
		conn.ExecContext(context.Background(),
			fmt.Sprintf("SET statement_timeout = '%ds'", timeoutSec))
	case "mysql":
		conn.ExecContext(context.Background(),
			fmt.Sprintf("SET max_execution_time = %d", timeoutMS))
	}
}

func strPtr(s string) *string { return &s }
