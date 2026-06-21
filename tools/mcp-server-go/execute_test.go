package main

import (
	"testing"
)

func TestValidateInput_Empty(t *testing.T) {
	err := validateInput("", 200, 30000)
	if err == nil {
		t.Fatal("expected error for empty SQL")
	}
	if err.ErrorType == nil || *err.ErrorType != "invalid_input" {
		t.Fatalf("expected invalid_input error type, got %v", err.ErrorType)
	}
}

func TestValidateInput_MaxRowsTooHigh(t *testing.T) {
	err := validateInput("SELECT 1", 2000, 30000)
	if err == nil {
		t.Fatal("expected error for max_rows > 1000")
	}
}

func TestValidateInput_TimeoutTooHigh(t *testing.T) {
	err := validateInput("SELECT 1", 200, 999999)
	if err == nil {
		t.Fatal("expected error for timeout > 120000")
	}
}

func TestAutoLimit_AddsLimit(t *testing.T) {
	result := autoLimit("SELECT * FROM schools", 200)
	expected := "SELECT * FROM (SELECT * FROM schools) AS _sub LIMIT 200"
	if result != expected {
		t.Fatalf("expected %q, got %q", expected, result)
	}
}

func TestAutoLimit_KeepsExistingLimit(t *testing.T) {
	result := autoLimit("SELECT * FROM schools LIMIT 50", 200)
	if result != "SELECT * FROM schools LIMIT 50" {
		t.Fatalf("expected limit to be preserved, got %q", result)
	}
}

func TestDialectFromURL(t *testing.T) {
	cases := []struct {
		url, expected string
	}{
		{"sqlite:///data/test.db", "sqlite"},
		{"postgresql://user:pass@host/db", "postgres"},
		{"mysql://user:pass@host/db", "mysql"},
	}
	for _, c := range cases {
		if got := dialectFromURL(c.url); got != c.expected {
			t.Fatalf("dialectFromURL(%q): expected %q, got %q", c.url, c.expected, got)
		}
	}
}

func TestDSNFromURL_SQLite(t *testing.T) {
	result := dsnFromURL("sqlite:///data/test.db")
	if result != "data/test.db" && result != "/data/test.db" {
		t.Fatalf("unexpected DSN: %q", result)
	}
}
