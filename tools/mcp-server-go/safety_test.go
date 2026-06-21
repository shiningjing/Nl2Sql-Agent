package main

import (
	"testing"
)

func TestValidSelect(t *testing.T) {
	r := checkSafety("SELECT * FROM schools", "sqlite")
	if !r.Valid {
		t.Fatalf("expected valid, got issues: %+v", r.Issues)
	}
	if r.StatementType != "SELECT" {
		t.Fatalf("expected SELECT, got %s", r.StatementType)
	}
	found := false
	for _, tbl := range r.TableReferences {
		if tbl == "schools" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected schools in table_references, got %v", r.TableReferences)
	}
}

func TestForbiddenInsert(t *testing.T) {
	r := checkSafety("INSERT INTO t VALUES (1)", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for INSERT")
	}
	hasForbidden := false
	for _, i := range r.Issues {
		if i.Type == "forbidden_keyword" {
			hasForbidden = true
			break
		}
	}
	if !hasForbidden {
		t.Fatalf("expected forbidden_keyword issue, got %+v", r.Issues)
	}
}

func TestMultiStatement(t *testing.T) {
	r := checkSafety("SELECT 1; SELECT 2", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for multi-statement")
	}
}

func TestSyntaxError(t *testing.T) {
	r := checkSafety("SELECT * FROM WHERE", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for syntax error")
	}
}

func TestEmptySQL(t *testing.T) {
	r := checkSafety("", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for empty SQL")
	}
}

func TestMissingSelect(t *testing.T) {
	r := checkSafety("SET autocommit = 1", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for missing SELECT")
	}
}

func TestCTEAllowed(t *testing.T) {
	r := checkSafety("WITH x AS (SELECT * FROM t) SELECT * FROM x", "sqlite")
	if !r.Valid {
		t.Fatalf("expected valid CTE, got issues: %+v", r.Issues)
	}
}

func TestDropRejected(t *testing.T) {
	r := checkSafety("DROP TABLE schools", "sqlite")
	if r.Valid {
		t.Fatal("expected invalid for DROP")
	}
}
