package com.nl2sql.gateway.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** JSON 单行访问日志，格式对齐 Go 网关（method/path/status/elapsed_ms/client_ip + traceId）。 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class AccessLogFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(AccessLogFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long start = System.nanoTime();
        StatusCapture capture = new StatusCapture(response);
        try {
            filterChain.doFilter(request, capture);
        } finally {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            String clientIp = request.getHeader("X-Forwarded-For");
            if (clientIp == null || clientIp.isBlank()) {
                clientIp = request.getRemoteAddr();
            }
            log.info("{\"method\":\"{}\",\"path\":\"{}\",\"status\":{},\"elapsed_ms\":{},\"client_ip\":\"{}\",\"traceId\":\"{}\"}",
                    request.getMethod(), request.getRequestURI(), capture.status, elapsedMs,
                    clientIp, request.getAttribute(TraceIdFilter.TRACE_ATTR));
        }
    }

    private static final class StatusCapture extends HttpServletResponseWrapper {

        private int status = 200;

        StatusCapture(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setStatus(int sc) {
            this.status = sc;
            super.setStatus(sc);
        }

        @Override
        public void sendError(int sc) throws IOException {
            this.status = sc;
            super.sendError(sc);
        }

        @Override
        public void sendError(int sc, String msg) throws IOException {
            this.status = sc;
            super.sendError(sc, msg);
        }
    }
}
