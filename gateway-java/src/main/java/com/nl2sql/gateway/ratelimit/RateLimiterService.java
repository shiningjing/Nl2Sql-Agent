package com.nl2sql.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** 滑动窗口限流：ZSET + Lua 原子（清理窗口外→计数→未满则占位）。fail-open。 */
@Service
public class RateLimiterService {

    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>("""
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local member = ARGV[4]
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            if redis.call('ZCARD', key) < limit then
                redis.call('ZADD', key, now, member)
                redis.call('PEXPIRE', key, window)
                return 1
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Counter userRejected;
    private final Counter globalRejected;

    public RateLimiterService(StringRedisTemplate redis, MeterRegistry meters) {
        this.redis = redis;
        this.userRejected = Counter.builder("gateway.ratelimit.rejected").tag("type", "user")
                .register(meters);
        this.globalRejected = Counter.builder("gateway.ratelimit.rejected").tag("type", "global")
                .register(meters);
    }

    public boolean allowUser(String userTag, int qpsLimit) {
        return check("rl:" + userTag, 1000, qpsLimit, userRejected);
    }

    public boolean allowGlobal(int globalQps) {
        return check("rl:__global__", 1000, globalQps, globalRejected);
    }

    private boolean check(String key, long windowMs, int limit, Counter rejected) {
        try {
            long now = System.currentTimeMillis();
            String member = now + "-" + ThreadLocalRandom.current().nextLong(1L << 40);
            Long r = redis.execute(SCRIPT, List.of(key),
                    String.valueOf(now), String.valueOf(windowMs), String.valueOf(limit), member);
            if (r != null && r == 1) {
                return true;
            }
            rejected.increment();
            return false;
        } catch (Exception e) {
            return true;   // fail-open：限流器故障不拒绝流量
        }
    }
}
