package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.web.TraceIdFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** /api/v1/** 统一入口：路径以 /stream 结尾走流式透传，其余非流式转发。 */
@RestController
public class ProxyController {

    private final HttpProxyService proxyService;

    public ProxyController(HttpProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @RequestMapping(value = "/api/v1/**",
            method = {RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT,
                    RequestMethod.DELETE, RequestMethod.PATCH})
    public void proxy(HttpServletRequest request, HttpServletResponse response, InputStream rawBody)
            throws IOException, InterruptedException {
        byte[] body = rawBody.readAllBytes();
        if (request.getRequestURI().endsWith("/stream")) {
            streamToClient(request, response, body);
            return;
        }
        EngineResponse resp = proxyService.forward(request, body);
        writeEngineResponse(resp, response);
    }

    /** SSE 流式透传：响应头直接落地，之后字节边读边 flush，绝不攒批。 */
    private void streamToClient(jakarta.servlet.http.HttpServletRequest request,
                                HttpServletResponse response, byte[] body)
            throws IOException, InterruptedException {
        java.net.http.HttpResponse<InputStream> engineResp;
        try {
            engineResp = proxyService.forwardStream(request, body);
        } catch (IOException e) {
            writeUnavailable(request, response, "engine connect failed");
            return;
        }
        response.setStatus(engineResp.statusCode());
        response.setContentType(engineResp.headers().firstValue("Content-Type")
                .orElse("application/octet-stream"));
        try (InputStream in = engineResp.body();
             OutputStream out = response.getOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        }
    }

    private void writeUnavailable(jakarta.servlet.http.HttpServletRequest request,
                                  HttpServletResponse response, String detail) throws IOException {
        response.setStatus(503);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"engine unavailable\",\"detail\":\"" + detail
                + "\",\"traceId\":\"" + request.getAttribute(TraceIdFilter.TRACE_ATTR) + "\"}");
    }

    private void writeEngineResponse(EngineResponse resp, HttpServletResponse response) throws IOException {
        response.setStatus(resp.status());
        response.setContentType(resp.headers().firstValue("Content-Type").orElse("application/octet-stream"));
        resp.headers().map().forEach((name, values) -> {
            String lower = name.toLowerCase();
            if (!lower.equals("content-type") && !lower.equals("content-length")
                    && !lower.equals("transfer-encoding")) {
                values.forEach(v -> response.addHeader(name, v));
            }
        });
        try (OutputStream out = response.getOutputStream()) {
            out.write(resp.body());
        }
    }
}
