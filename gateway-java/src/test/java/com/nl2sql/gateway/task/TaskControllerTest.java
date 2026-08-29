package com.nl2sql.gateway.task;

import com.nl2sql.gateway.config.GatewayProperties;
import com.nl2sql.gateway.policy.PermissionService;
import com.nl2sql.gateway.policy.UserPolicyInterceptor;
import com.nl2sql.gateway.policy.UserPolicyService;
import com.nl2sql.gateway.ratelimit.QuotaService;
import com.nl2sql.gateway.ratelimit.RateLimiterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TaskController.class)
@Import(TaskController.class)
class TaskControllerTest {

    /** 切片会加载 WebConfig(注册身份闸门)——本类只测控制器，闸门放行；闸门自身有专测。 */
    @MockBean
    UserPolicyInterceptor policyInterceptor;

    @MockBean
    PermissionService permission;
    @MockBean
    RateLimiterService rateLimiter;
    @MockBean
    QuotaService quota;
    @MockBean
    com.nl2sql.gateway.cache.ResultCacheService resultCache;   // 默认 get→null，不短路

    @TestConfiguration
    static class Cfg {
        @Bean
        GatewayProperties gatewayProperties() {
            return new GatewayProperties(new GatewayProperties.Ratelimit(200),
                    new GatewayProperties.Cache(1800));
        }
    }

    static final UserPolicyService.Policy POLICY = new UserPolicyService.Policy(
            "local_user", "user", List.of("*"), 100, 1000L, true);

    @Autowired
    MockMvc mvc;

    @MockBean
    TaskStoreService store;
    @MockBean
    KafkaPublisher publisher;
    @MockBean
    TaskStreamService streamService;

    @BeforeEach
    void allowAll() throws Exception {
        lenient().when(policyInterceptor.preHandle(any(), any(), any())).thenReturn(true);
        lenient().when(permission.dbAllowed(any(), any())).thenReturn(true);
        lenient().when(permission.isOwnerOrAdmin(anyString(), any())).thenReturn(true);
        lenient().when(rateLimiter.allowGlobal(anyInt())).thenReturn(true);
        lenient().when(rateLimiter.allowUser(anyString(), anyInt())).thenReturn(true);
        lenient().when(quota.tryConsume(anyString(), anyLong())).thenReturn(true);
    }

    @Test
    void submitReturns202WithTaskId() throws Exception {
        when(store.idempotentGet(anyString())).thenReturn(null);
        mvc.perform(post("/api/v1/task/submit").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"q\",\"db_id\":\"california_schools\"}")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.task_id").isNotEmpty())
                .andExpect(jsonPath("$.status").value("PENDING"));
        verify(store).create(anyString(), eq("q"), eq("california_schools"), eq(""));
        verify(publisher).publishSubmitted(anyString(),
                argThat(p -> "q".equals(p.get("question")) && "california_schools".equals(p.get("db_id"))));
    }

    @Test
    void submitAppliesPayloadDefaultsLikePython() throws Exception {
        when(store.idempotentGet(anyString())).thenReturn(null);
        mvc.perform(post("/api/v1/task/submit").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"q\"}")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isAccepted());
        verify(publisher).publishSubmitted(anyString(), argThat(p ->
                Boolean.TRUE.equals(p.get("rag_schema"))
                        && Boolean.TRUE.equals(p.get("multi_candidate"))
                        && Boolean.FALSE.equals(p.get("rag_column_prune"))
                        && Integer.valueOf(8).equals(p.get("rag_k"))));
    }

    @Test
    void submitIdempotentHitReturnsExistingTaskId() throws Exception {
        when(store.idempotentGet(anyString())).thenReturn("existing123");
        mvc.perform(post("/api/v1/task/submit").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"question\":\"q\",\"idempotency_key\":\"k1\"}")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.task_id").value("existing123"));
        verify(publisher, never()).publishSubmitted(anyString(), any());
    }

    @Test
    void status404MatchesFastApiDetailShape() throws Exception {
        when(store.get("nope")).thenReturn(null);
        mvc.perform(get("/api/v1/task/nope/status")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Task not found or expired"));
    }

    @Test
    void statusReturns14FieldsWithoutDatabaseUrl() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "RUNNING";
        s.question = "q";
        s.db_id = "d";
        s.created_at = "c";
        s.updated_at = "u";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(get("/api/v1/task/t1/status")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.database_url").doesNotExist())
                .andExpect(jsonPath("$.conversation_turns").doesNotExist())
                .andExpect(jsonPath("$.node").doesNotExist())
                .andExpect(jsonPath("$.token_usage").exists());
    }

    @Test
    void cancelMissingTaskReturnsNotFoundStatus() throws Exception {
        when(store.get("t1")).thenReturn(null);
        mvc.perform(post("/api/v1/task/t1/cancel")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(jsonPath("$.status").value("not_found"));
    }

    @Test
    void cancelTerminalReturnsStateStatus() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "SUCCESS";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/cancel")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(jsonPath("$.status").value("SUCCESS"));
        verify(store, never()).requestCancel(anyString());
    }

    @Test
    void cancelRunningSetsFlag() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "RUNNING";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/cancel")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(jsonPath("$.status").value("cancelled"));
        verify(store).requestCancel("t1");
    }

    @Test
    void feedbackOnRunningTaskReturns400() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "RUNNING";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/feedback").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"feedback\":\"add group by\"}")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isBadRequest());
    }

    @Test
    void feedbackAcceptedTransitionsToRunningAndPublishes() throws Exception {
        TaskState s = new TaskState();
        s.task_id = "t1";
        s.status = "SUCCESS";
        s.question = "q";
        s.db_id = "d";
        when(store.get("t1")).thenReturn(s);
        mvc.perform(post("/api/v1/task/t1/feedback").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"feedback\":\"add group by\"}")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("accepted"))
                .andExpect(jsonPath("$.turn").value(1));
        verify(publisher).publishFeedback(eq("t1"),
                argThat(p -> "add group by".equals(p.get("feedback"))));
        verify(store).save(argThat(saved -> "RUNNING".equals(saved.status)
                && saved.progress == 0 && saved.node == null));
    }

    @Test
    void scanStaleReturnsCounts() throws Exception {
        when(store.scanStale()).thenReturn(java.util.List.of("t1", "t2"));
        mvc.perform(post("/api/v1/task/scan-stale")
                        .requestAttr(UserPolicyInterceptor.POLICY_ATTR, POLICY))
                .andExpect(jsonPath("$.stale_count").value(2))
                .andExpect(jsonPath("$.stale_task_ids[0]").value("t1"));
    }
}
