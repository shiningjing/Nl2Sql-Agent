package com.nl2sql.gateway.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/** 日配额：quota:{tag}:{yyyyMMdd} INCR + 首次设 48h TTL。fail-open。 */
@Service
public class QuotaService {

    private static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>("""
            local cur = redis.call('INCR', KEYS[1])
            if cur == 1 then redis.call('EXPIRE', KEYS[1], 172800) end
            if cur > tonumber(ARGV[1]) then return 0 end
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Counter quotaRejected;

    public QuotaService(StringRedisTemplate redis, MeterRegistry meters) {
        this.redis = redis;
        this.quotaRejected = Counter.builder("gateway.quota.rejected").register(meters);
    }

    public boolean tryConsume(String userTag, long dailyQuota) {
        try {
            Long r = redis.execute(SCRIPT,
                    List.of("quota:" + userTag + ":" + LocalDate.now().toString().replace("-", "")),
                    String.valueOf(dailyQuota));
            if (r != null && r == 1) {
                return true;
            }
            quotaRejected.increment();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
