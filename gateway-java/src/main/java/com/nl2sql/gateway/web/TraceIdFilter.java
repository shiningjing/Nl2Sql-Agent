package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/** 生成/复用 traceId：写入 MDC（日志用）、请求属性（代理转发用）、响应头（客户端可见）。 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter extends OncePerRequestFilter {

    public static final String TRACE_HEADER = "X-Trace-Id";
    public static final String TRACE_ATTR = "traceId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String incoming = request.getHeader(TRACE_HEADER);
        String traceId = (incoming != null && incoming.matches("[0-9a-zA-Z\\-]{4,64}"))
                ? incoming
                : UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        MDC.put(TRACE_ATTR, traceId);
        request.setAttribute(TRACE_ATTR, traceId);
        response.setHeader(TRACE_HEADER, traceId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(TRACE_ATTR);
        }
    }
}
