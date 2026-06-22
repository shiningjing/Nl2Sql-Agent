package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	status := map[string]string{
		"gateway": "ok",
	}

	apiURL := os.Getenv("PROXY_TARGET")
	if apiURL == "" {
		apiURL = "http://127.0.0.1:8000"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	if resp, err := client.Get(apiURL + "/api/v1/health"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			status["api"] = "ok"
		} else {
			status["api"] = "unhealthy"
		}
	} else {
		status["api"] = "down"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"service": "nl2sql-gateway",
		"version": "0.1.0",
	})
}
