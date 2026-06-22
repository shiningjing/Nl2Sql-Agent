package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	healthHandler(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var status map[string]string
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if status["gateway"] != "ok" {
		t.Fatalf("expected gateway=ok, got %q", status["gateway"])
	}
	if _, ok := status["api"]; !ok {
		t.Fatal("expected api key in response")
	}
}

func TestMetricsHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	metricsHandler(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestProxyHandler_ReturnsHandler(t *testing.T) {
	h := proxyHandler("http://127.0.0.1:8000")
	if h == nil {
		t.Fatal("expected handler, got nil")
	}
}
