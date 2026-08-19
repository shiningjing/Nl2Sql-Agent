package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TraceIdFilterTest {

    private final TraceIdFilter filter = new TraceIdFilter();

    @Test
    void generatesTraceIdWhenAbsent() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/task");
        MockHttpServletResponse res = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);
        filter.doFilter(req, res, chain);
        String traceId = res.getHeader("X-Trace-Id");
        assertThat(traceId).hasSize(16).matches("[0-9a-f]+");
        assertThat(req.getAttribute("traceId")).isEqualTo(traceId);
        verify(chain).doFilter(any(), any());
        assertThat(MDC.get("traceId")).isNull(); // 已清理
    }

    @Test
    void reusesIncomingTraceId() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/task");
        req.addHeader("X-Trace-Id", "abc123def456abc7");
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, mock(FilterChain.class));
        assertThat(res.getHeader("X-Trace-Id")).isEqualTo("abc123def456abc7");
        assertThat(req.getAttribute("traceId")).isEqualTo("abc123def456abc7");
    }

    @Test
    void exposesTraceIdInMdcDuringChain() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/");
        MockHttpServletResponse res = new MockHttpServletResponse();
        final String[] seen = new String[1];
        filter.doFilter(req, res, (request, response) -> seen[0] = MDC.get("traceId"));
        assertThat(seen[0]).isNotBlank();
    }
}
