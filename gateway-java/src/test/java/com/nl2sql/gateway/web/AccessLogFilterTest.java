package com.nl2sql.gateway.web;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;

class AccessLogFilterTest {

    private final AccessLogFilter filter = new AccessLogFilter();
    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void attachAppender() {
        logger = (Logger) LoggerFactory.getLogger(AccessLogFilter.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
    }

    @Test
    void logsJsonAccessLine() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/api/v1/task/submit");
        req.setRemoteAddr("192.168.1.5");
        req.addHeader("X-Forwarded-For", "10.0.0.9");
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, (request, response) ->
                ((jakarta.servlet.http.HttpServletResponse) response).setStatus(202));
        assertThat(appender.list).hasSize(1);
        String line = appender.list.get(0).getFormattedMessage();
        assertThat(line).contains("\"method\":\"POST\"")
                .contains("\"path\":\"/api/v1/task/submit\"")
                .contains("\"status\":202")
                .contains("\"elapsed_ms\":")
                .contains("\"client_ip\":\"10.0.0.9\"");
    }

    @Test
    void fallsBackToRemoteAddr() throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/v1/health");
        req.setRemoteAddr("127.0.0.1");
        MockHttpServletResponse res = new MockHttpServletResponse();
        filter.doFilter(req, res, (request, response) -> { });
        String line = appender.list.get(0).getFormattedMessage();
        assertThat(line).contains("\"client_ip\":\"127.0.0.1\"");
    }
}
