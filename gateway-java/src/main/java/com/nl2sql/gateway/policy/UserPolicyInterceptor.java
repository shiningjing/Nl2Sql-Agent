package com.nl2sql.gateway.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;

/** 身份闸门：X-User-Id → 策略；未知 401 / 禁用 403 / @RequireRole 不足 403；/api/v1/eval/** 强制 admin。 */
@Component
public class UserPolicyInterceptor implements HandlerInterceptor {

    public static final String POLICY_ATTR = "userPolicy";
    private final UserPolicyService policyService;
    private final ObjectMapper om = new ObjectMapper();

    public UserPolicyInterceptor(UserPolicyService policyService) {
        this.policyService = policyService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String tag = request.getHeader("X-User-Id");
        if (tag == null || tag.isBlank()) {
            return reject(response, 401, "missing X-User-Id header");
        }
        UserPolicyService.Policy policy = policyService.resolve(tag);
        if (policy == null) {
            return reject(response, 401, "unknown user: " + tag);
        }
        if (!policy.enabled()) {
            return reject(response, 403, "user disabled: " + tag);
        }
        String required = request.getRequestURI().startsWith("/api/v1/eval/") ? "admin" : null;
        if (required == null && handler instanceof HandlerMethod hm) {
            RequireRole rr = hm.getMethodAnnotation(RequireRole.class);
            if (rr != null) {
                required = rr.value();
            }
        }
        if ("admin".equals(required) && !policy.isAdmin()) {
            return reject(response, 403, "admin role required");
        }
        request.setAttribute(POLICY_ATTR, policy);
        return true;
    }

    private boolean reject(HttpServletResponse response, int code, String detail) throws Exception {
        response.setStatus(code);
        response.setContentType("application/json");
        response.getWriter().write(om.writeValueAsString(java.util.Map.of("detail", detail)));
        return false;
    }
}
