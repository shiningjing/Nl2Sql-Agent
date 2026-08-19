package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.config.ProxyProperties;
import com.nl2sql.gateway.web.TraceIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** JDK HttpClient 阻塞式转发（虚拟线程让阻塞 IO 成为合理选择）。 */
@Service
public class HttpProxyService {

    private static final Set<String> HOP_BY_HOP =
            Set.of("host", "connection", "content-length", "transfer-encoding", "keep-alive");

    private final ProxyProperties props;
    private final HttpClient httpClient;

    public HttpProxyService(ProxyProperties props) {
        this.props = props;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .build();
    }

    /** 非流式转发：完整读取引擎响应。 */
    public EngineResponse forward(HttpServletRequest request, byte[] body)
            throws IOException, InterruptedException {
        HttpResponse<byte[]> engineResp =
                httpClient.send(buildRequest(request, body), HttpResponse.BodyHandlers.ofByteArray());
        return new EngineResponse(engineResp.statusCode(), engineResp.headers(), engineResp.body());
    }

    /** 流式转发：只拿响应头 + InputStream，由调用方边读边写。 */
    public HttpResponse<InputStream> forwardStream(HttpServletRequest request, byte[] body)
            throws IOException, InterruptedException {
        return httpClient.send(buildRequest(request, body), HttpResponse.BodyHandlers.ofInputStream());
    }

    private HttpRequest buildRequest(HttpServletRequest request, byte[] body) {
        StringBuilder uri = new StringBuilder(props.baseUrl()).append(request.getRequestURI());
        if (request.getQueryString() != null) {
            uri.append('?').append(request.getQueryString());
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uri.toString()));

        Set<String> seen = new HashSet<>();
        request.getHeaderNames().asIterator().forEachRemaining(name -> {
            seen.add(name.toLowerCase());
            if (HOP_BY_HOP.contains(name.toLowerCase())) {
                return;
            }
            List<String> values = Collections.list(request.getHeaders(name));
            values.forEach(v -> builder.header(name, v));
        });
        String traceId = (String) request.getAttribute(TraceIdFilter.TRACE_ATTR);
        if (traceId != null && !seen.contains(TraceIdFilter.TRACE_HEADER.toLowerCase())) {
            builder.header(TraceIdFilter.TRACE_HEADER, traceId);
        }

        String method = request.getMethod();
        if ("GET".equals(method)) {
            builder.GET();
        } else if ("DELETE".equals(method)) {
            builder.DELETE();
        } else {
            builder.method(method, (body == null || body.length == 0)
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body));
        }
        return builder.build();
    }
}
