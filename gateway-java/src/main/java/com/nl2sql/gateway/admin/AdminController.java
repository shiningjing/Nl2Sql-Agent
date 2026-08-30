package com.nl2sql.gateway.admin;

import com.nl2sql.gateway.policy.RequireRole;
import com.nl2sql.gateway.policy.UserPolicy;
import com.nl2sql.gateway.policy.UserPolicyService;
import com.nl2sql.gateway.recorder.TaskRecord;
import com.nl2sql.gateway.recorder.TaskRecordMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final UserPolicyService policyService;
    private final TaskRecordMapper taskRecordMapper;

    public AdminController(UserPolicyService policyService, TaskRecordMapper taskRecordMapper) {
        this.policyService = policyService;
        this.taskRecordMapper = taskRecordMapper;
    }

    public record UpdatePolicyRequest(String role, List<String> allowed_db_ids,
                                      Integer qps_limit, Long daily_quota, Integer status) {
    }

    @PutMapping("/users/{tag}")
    @RequireRole("admin")
    public ResponseEntity<?> updateUser(@PathVariable String tag, @RequestBody UpdatePolicyRequest req) {
        UserPolicy row = new UserPolicy();
        row.userTag = tag;
        row.role = req.role() == null ? "user" : req.role();
        row.allowedDbIds = toJson(req.allowed_db_ids() == null ? List.of("*") : req.allowed_db_ids());
        row.qpsLimit = req.qps_limit() == null ? 10 : req.qps_limit();
        row.dailyQuota = req.daily_quota() == null ? 1000 : req.daily_quota();
        row.status = req.status() == null ? 1 : req.status();
        return ResponseEntity.ok(Map.of("user_tag", tag, "policy", policyService.update(row)));
    }

    @GetMapping("/stats")
    @RequireRole("admin")
    public Map<String, Object> stats() {
        List<Map<String, Object>> byStatus = taskRecordMapper.selectMaps(
                new com.baomidou.mybatisplus.core.conditions.query.QueryWrapper<TaskRecord>()
                        .select("status", "COUNT(*) AS cnt", "AVG(total_latency_ms) AS avg_ms",
                                "SUM(cache_hit) AS cache_hits")
                        .groupBy("status"));
        return Map.of("by_status", byStatus);
    }

    private static String toJson(List<String> l) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(l);
        } catch (Exception e) {
            return "[\"*\"]";
        }
    }
}
