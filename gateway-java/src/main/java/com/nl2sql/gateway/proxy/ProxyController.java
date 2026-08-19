package com.nl2sql.gateway.proxy;

import com.nl2sql.gateway.web.TraceIdFilter;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/** /api/v1/** 统一入口：路径以 /stream 结尾走流式透传，其余非流式转发（熔断+超时装饰）。 */
@RestController
public class ProxyController {

    private final HttpProxyService proxyService;
    private final CircuitBreaker circuitBreaker;
    private final TimeLimiter timeLimiter;
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public ProxyController(HttpProxyService proxyService,
                           CircuitBreakerRegistry breakerRegistry,
                           TimeLimiterRegistry timeLimiterRegistry) {
        this.proxyService = proxyService;
        this.circuitBreaker = breakerRegistry.circuitBreaker("engine");
        this.timeLimiter = timeLimiterRegistry.timeLimiter("engine");
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
        EngineResponse resp;
        try {
            resp = executeWithResilience(() -> {
                try {
                    return proxyService.forward(request, body);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (EngineUnavailableException e) {
            writeUnavailable(request, response, e.getMessage());
            return;
        }
        writeEngineResponse(resp, response);
    }

    /** 非流式路径统一装饰：虚拟线程上执行 + TimeLimiter + CircuitBreaker。 */
    private EngineResponse executeWithResilience(Supplier<EngineResponse> supplier) {
        try {
            CompletableFuture<EngineResponse> future =
                    CompletableFuture.supplyAsync(supplier, virtualExecutor);
            return circuitBreaker.decorateCallable(
                    timeLimiter.decorateFutureSupplier(() -> future)).call();
        } catch (CallNotPermittedException e) {
            throw new EngineUnavailableException("circuit breaker open", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new EngineUnavailableException("engine timeout", e);
        } catch (ExecutionException e) {
            throw new EngineUnavailableException("engine unavailable", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EngineUnavailableException("interrupted", e);
        } catch (Exception e) {
            throw new EngineUnavailableException("engine unavailable", e);
        }
    }

    /** SSE 流式透传：响应头直接落地，之后字节边读边 flush，绝不攒批。 */
    private void streamToClient(HttpServletRequest request,
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

    private void writeUnavailable(HttpServletRequest request,
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
