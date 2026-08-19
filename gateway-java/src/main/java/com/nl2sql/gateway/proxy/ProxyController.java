package com.nl2sql.gateway.proxy;

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
            // Task 5 实现流式分支
            response.setStatus(501);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"stream proxy not implemented yet\"}");
            return;
        }
        EngineResponse resp = proxyService.forward(request, body);
        writeEngineResponse(resp, response);
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
