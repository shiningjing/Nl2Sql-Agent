package com.nl2sql.gateway.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nl2sql.gateway.config.GatewayProperties;
import com.nl2sql.gateway.policy.UserPolicyService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;

/** qcache:{sha256(question|db_id)} → 完整结果 JSON；TTL±10% 抖动防雪崩；故障降级为 miss。 */
@Service
public class ResultCacheService {

    private final StringRedisTemplate redis;
    private final ObjectMapper om;
    private final GatewayProperties props;
    private final Counter hits;
    private final Counter misses;

    public ResultCacheService(StringRedisTemplate redis, ObjectMapper om,
                              GatewayProperties props, MeterRegistry meters) {
        this.redis = redis;
        this.om = om;
        this.props = props;
        this.hits = Counter.builder("gateway.cache").tag("result", "hit").register(meters);
        this.misses = Counter.builder("gateway.cache").tag("result", "miss").register(meters);
    }

    public static String key(String question, String dbId) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256")
                    .digest((question + "|" + (dbId == null ? "" : dbId)).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("qcache:");
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 命中返回完整结果 JSON（status/sql/exec_result/token_usage/node_timings），未命中 null。 */
    public String get(String question, String dbId) {
        try {
            String v = redis.opsForValue().get(key(question, dbId));
            if (v != null) {
                hits.increment();
            } else {
                misses.increment();
            }
            return v;
        } catch (Exception e) {
            return null;   // 缓存层故障降级为 miss
        }
    }

    public void put(String question, String dbId, String resultJson) {
        try {
            redis.opsForValue().set(key(question, dbId), resultJson,
                    UserPolicyService.jitter(Duration.ofSeconds(props.cache().resultTtlSeconds()), 0.1));
        } catch (Exception ignore) {
            // 写缓存失败不影响主流程
        }
    }
}
