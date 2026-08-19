package com.nl2sql.gateway.health;

import com.nl2sql.gateway.config.ProxyProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** actuator /health 的 engine 组件：GET {base-url}{health-path}，短超时不拖垮整体健康检查。 */
@Component("engine")
public class EngineHealthIndicator implements HealthIndicator {

    private final ProxyProperties props;
    private final HttpClient httpClient;

    public EngineHealthIndicator(ProxyProperties props) {
        this.props = props;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .build();
    }

    @Override
    public Health health() {
        try {
            HttpResponse<String> resp = httpClient.send(
                    HttpRequest.newBuilder(URI.create(props.baseUrl() + props.healthPath())).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                return Health.up().withDetail("httpStatus", 200).build();
            }
            return Health.down().withDetail("httpStatus", resp.statusCode()).build();
        } catch (Exception e) {
            String detail = (e.getMessage() != null) ? e.getMessage() : e.getClass().getSimpleName();
            return Health.down().withDetail("error", detail).build();
        }
    }
}
