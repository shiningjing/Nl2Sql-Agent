package com.nl2sql.gateway.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class UserPolicyService {

    public record Policy(String userTag, String role, List<String> allowedDbIds,
                         int qpsLimit, long dailyQuota, boolean enabled) {
        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean isAdmin() {
            return "admin".equals(role);
        }
    }

    private static final String NEGATIVE = "\u0000NEG";
    private final UserPolicyMapper mapper;
    private final StringRedisTemplate redis;
    private final ObjectMapper om;

    public UserPolicyService(UserPolicyMapper mapper, StringRedisTemplate redis, ObjectMapper om) {
        this.mapper = mapper;
        this.redis = redis;
        this.om = om;
    }

    /** 返回 null = 未知用户（调用方 401）。缓存 miss 读库回填；未知名负缓存 60s 防穿透。 */
    public Policy resolve(String userTag) {
        String key = "policy:" + userTag;
        String raw = redis.opsForValue().get(key);
        if (raw != null) {
            return NEGATIVE.equals(raw) ? null : parse(raw);
        }
        UserPolicy row = mapper.selectById(userTag);
        if (row == null) {
            redis.opsForValue().set(key, NEGATIVE, Duration.ofSeconds(60));
            return null;
        }
        Policy p = toPolicy(row);
        try {
            redis.opsForValue().set(key, om.writeValueAsString(p), jitter(Duration.ofSeconds(600), 0.1));
        } catch (Exception ignore) {
            // 缓存写失败不阻塞
        }
        return p;
    }

    /** admin 更新策略：upsert MySQL + 删缓存（下次请求读新值）。 */
    public Policy update(UserPolicy row) {
        mapper.upsert(row);
        redis.delete("policy:" + row.userTag);
        return toPolicy(row);
    }

    public void evict(String userTag) {
        redis.delete("policy:" + userTag);
    }

    private Policy parse(String json) {
        try {
            return om.readValue(json, Policy.class);
        } catch (Exception e) {
            return null;
        }
    }

    private Policy toPolicy(UserPolicy row) {
        List<String> dbs;
        try {
            dbs = om.readValue(row.allowedDbIds, new com.fasterxml.jackson.core.type.TypeReference<>() {
            });
        } catch (Exception e) {
            dbs = List.of();
        }
        return new Policy(row.userTag, row.role, dbs, row.qpsLimit, row.dailyQuota, row.status == 1);
    }

    static Duration jitter(Duration base, double ratio) {
        long millis = base.toMillis();
        long delta = (long) (millis * ratio);
        return Duration.ofMillis(Math.max(1, millis - delta + ThreadLocalRandom.current().nextLong(2 * delta)));
    }
}
