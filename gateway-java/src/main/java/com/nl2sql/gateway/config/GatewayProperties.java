package com.nl2sql.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(Ratelimit ratelimit, Cache cache) {

    public record Ratelimit(int globalQps) {
    }

    public record Cache(int resultTtlSeconds) {
    }
}
