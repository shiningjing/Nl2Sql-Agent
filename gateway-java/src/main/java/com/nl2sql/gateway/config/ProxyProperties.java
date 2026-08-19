package com.nl2sql.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "engine")
public record ProxyProperties(
        String baseUrl,
        String healthPath,
        int connectTimeoutMs,
        long responseTimeoutMs) {
}
