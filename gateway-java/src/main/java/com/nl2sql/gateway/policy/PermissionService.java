package com.nl2sql.gateway.policy;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/** 数据权限（白名单语义，"*" 放行，库是否存在由引擎报错）+ 任务归属（taskowner 自有 key）。 */
@Service
public class PermissionService {

    private final StringRedisTemplate redis;

    public PermissionService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean dbAllowed(UserPolicyService.Policy policy, String dbId) {
        if (policy.allowedDbIds().contains("*")) {
            return true;
        }
        return dbId == null || dbId.isEmpty() || policy.allowedDbIds().contains(dbId);
    }

    public void setOwner(String taskId, String userTag) {
        redis.opsForValue().set("taskowner:" + taskId, userTag, Duration.ofSeconds(86400));
    }

    public boolean isOwnerOrAdmin(String taskId, UserPolicyService.Policy policy) {
        if (policy.isAdmin()) {
            return true;
        }
        String owner = redis.opsForValue().get("taskowner:" + taskId);
        return policy.userTag().equals(owner);
    }
}
