package main

// DEPRECATED (M1, 2026-08): 已被 gateway-java (Spring Boot) 替代，仅为参照保留，M4 后删除。

import (
	"log"
	"net/http"
	"os"
)

func main() {
	target := os.Getenv("PROXY_TARGET")
	if target == "" {
		target = "http://127.0.0.1:8000"
	}
	port := os.Getenv("GATEWAY_PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()

	// /health — aggregated health check
	mux.HandleFunc("/health", healthHandler)

	// /metrics — basic stats
	mux.HandleFunc("/metrics", metricsHandler)

	// /* — reverse proxy to FastAPI
	mux.HandleFunc("/", loggingMiddleware(proxyHandler(target)))

	log.Printf("Gateway listening on :%s → %s", port, target)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}
