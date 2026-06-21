package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// ── L1: Regex checks ──────────────────────────────────────────────────────

var (
	reForbidden = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|PRAGMA|REPLACE|MERGE|GRANT|REVOKE)\b`)
	reSelect    = regexp.MustCompile(`(?i)\b(SELECT|WITH)\b`)
	reTableRef  = regexp.MustCompile(`(?i)\bFROM\s+(\w+)`)
	reJoinRef   = regexp.MustCompile(`(?i)\bJOIN\s+(\w+)`)
)

// SafetyIssue describes a single validation problem.
type SafetyIssue struct {
	Type   string `json:"type"`
	Detail string `json:"detail"`
}

// SafetyResult is the output of validate_sql.
type SafetyResult struct {
	Valid           bool          `json:"valid"`
	Issues          []SafetyIssue `json:"issues"`
	StatementType   string        `json:"statement_type"`
	TableReferences []string      `json:"table_references"`
}

// checkSafety runs L1 (regex) + L2 (DB prepare) validation on a SQL string.
func checkSafety(sqlStr, dialect string) SafetyResult {
	result := SafetyResult{Valid: true, Issues: []SafetyIssue{}}

	trimmed := strings.TrimSpace(sqlStr)
	if trimmed == "" {
		result.Valid = false
		result.Issues = append(result.Issues, SafetyIssue{Type: "empty_sql", Detail: "SQL string is empty."})
		return result
	}

	// ── L1: no forbidden keywords (check before SELECT requirement) ──
	if match := reForbidden.FindString(trimmed); match != "" {
		result.Valid = false
		result.Issues = append(result.Issues, SafetyIssue{
			Type:   "forbidden_keyword",
			Detail: fmt.Sprintf("Forbidden statement type: %s.", strings.ToUpper(match)),
		})
	}

	// ── L1: must contain SELECT or WITH ──
	if !reSelect.MatchString(trimmed) {
		result.Valid = false
		result.Issues = append(result.Issues, SafetyIssue{
			Type:   "missing_select",
			Detail: "SQL must start with SELECT or WITH.",
		})
		return result
	}

	// ── L1: determine statement type ──
	if strings.HasPrefix(strings.ToUpper(trimmed), "WITH") || strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
		result.StatementType = "SELECT"
	} else {
		result.StatementType = "OTHER"
	}

	// ── L1: multi-statement detection ──
	statements := splitStatements(trimmed)
	if len(statements) > 1 {
		result.Valid = false
		result.Issues = append(result.Issues, SafetyIssue{
			Type:   "multi_statement",
			Detail: fmt.Sprintf("Multiple statements detected (%d).", len(statements)),
		})
	}

	// ── L1: extract table references (regex-based) ──
	result.TableReferences = extractTableRefs(trimmed)

	// ── L2: SQLite prepare check (syntax validation) ──
	tryPrepare(trimmed, &result)

	return result
}

func tryPrepare(sqlStr string, result *SafetyResult) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return
	}
	defer db.Close()

	_, err = db.Prepare(sqlStr)
	if err != nil {
		msg := err.Error()
		lower := strings.ToLower(msg)
		// "no such table" is expected on :memory: — skip syntax check in that case
		if strings.Contains(lower, "no such table") {
			return
		}
		result.Issues = append(result.Issues, SafetyIssue{
			Type:   "ast_syntax",
			Detail: fmt.Sprintf("SQL prepare error: %s", truncate(msg, 120)),
		})
		if result.Valid {
			result.Valid = false
		}
	}
}

// ── Statement splitting ────────────────────────────────────────────────────

func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// ── Table reference extraction ─────────────────────────────────────────────

func extractTableRefs(sql string) []string {
	var refs []string
	matches := reTableRef.FindAllStringSubmatch(sql, -1)
	for _, m := range matches {
		if len(m) > 1 {
			refs = append(refs, m[1])
		}
	}
	joinMatches := reJoinRef.FindAllStringSubmatch(sql, -1)
	for _, m := range joinMatches {
		if len(m) > 1 {
			refs = append(refs, m[1])
		}
	}
	return uniqueStrings(refs)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		s = strings.Trim(strings.TrimSpace(s), "`\"'[]")
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
