package com.nl2sql.gateway.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.method.HandlerMethod;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserPolicyInterceptorTest {

    static class Dummy {
        @RequireRole("admin")
        public void adminOnly() {
        }

        public void plain() {
        }
    }

    @Mock
    UserPolicyService policyService;

    private UserPolicyInterceptor interceptor;

    private final UserPolicyService.Policy user = new UserPolicyService.Policy(
            "u1", "user", List.of("*"), 100, 1000L, true);
    private final UserPolicyService.Policy admin = new UserPolicyService.Policy(
            "root", "admin", List.of("*"), 100, 1000L, true);

    @BeforeEach
    void setUp() {
        interceptor = new UserPolicyInterceptor(policyService);
    }

    private MockHttpServletRequest req(String method, String uri) {
        return new MockHttpServletRequest(method, uri);
    }

    @Test
    void missingHeaderRejected401() throws Exception {
        MockHttpServletResponse resp = new MockHttpServletResponse();
        boolean ok = interceptor.preHandle(req("GET", "/api/v1/task/submit"), resp, new Object());

        assertThat(ok).isFalse();
        assertThat(resp.getStatus()).isEqualTo(401);
        assertThat(resp.getContentAsString()).contains("missing X-User-Id");
    }

    @Test
    void unknownUserRejected401() throws Exception {
        when(policyService.resolve(anyString())).thenReturn(null);
        MockHttpServletRequest request = req("GET", "/api/v1/task/submit");
        request.addHeader("X-User-Id", "ghost");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, new Object())).isFalse();
        assertThat(resp.getStatus()).isEqualTo(401);
        assertThat(resp.getContentAsString()).contains("unknown user");
    }

    @Test
    void disabledUserRejected403() throws Exception {
        UserPolicyService.Policy disabled = new UserPolicyService.Policy(
                "u2", "user", List.of("*"), 100, 1000L, false);
        when(policyService.resolve("u2")).thenReturn(disabled);
        MockHttpServletRequest request = req("GET", "/api/v1/task/submit");
        request.addHeader("X-User-Id", "u2");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, new Object())).isFalse();
        assertThat(resp.getStatus()).isEqualTo(403);
        assertThat(resp.getContentAsString()).contains("disabled");
    }

    @Test
    void evalPathRequiresAdminEvenWithoutAnnotation() throws Exception {
        when(policyService.resolve("u1")).thenReturn(user);
        MockHttpServletRequest request = req("POST", "/api/v1/eval/run");
        request.addHeader("X-User-Id", "u1");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, new Object())).isFalse();
        assertThat(resp.getStatus()).isEqualTo(403);
        assertThat(resp.getContentAsString()).contains("admin role required");
    }

    @Test
    void requireRoleAnnotationBlocksNonAdmin() throws Exception {
        when(policyService.resolve("u1")).thenReturn(user);
        HandlerMethod handler = new HandlerMethod(new Dummy(), Dummy.class.getMethod("adminOnly"));
        MockHttpServletRequest request = req("GET", "/api/v1/task/abc/status");
        request.addHeader("X-User-Id", "u1");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, handler)).isFalse();
        assertThat(resp.getStatus()).isEqualTo(403);
    }

    @Test
    void adminPassesAnnotatedMethodAndPolicyAttributeSet() throws Exception {
        when(policyService.resolve("root")).thenReturn(admin);
        HandlerMethod handler = new HandlerMethod(new Dummy(), Dummy.class.getMethod("adminOnly"));
        MockHttpServletRequest request = req("GET", "/api/v1/task/abc/status");
        request.addHeader("X-User-Id", "root");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, handler)).isTrue();
        assertThat(request.getAttribute(UserPolicyInterceptor.POLICY_ATTR)).isEqualTo(admin);
    }

    @Test
    void plainUserPlainPathPasses() throws Exception {
        when(policyService.resolve("u1")).thenReturn(user);
        HandlerMethod handler = new HandlerMethod(new Dummy(), Dummy.class.getMethod("plain"));
        MockHttpServletRequest request = req("GET", "/api/v1/task/abc/status");
        request.addHeader("X-User-Id", "u1");

        MockHttpServletResponse resp = new MockHttpServletResponse();
        assertThat(interceptor.preHandle(request, resp, handler)).isTrue();
        assertThat(request.getAttribute(UserPolicyInterceptor.POLICY_ATTR)).isEqualTo(user);
    }
}
