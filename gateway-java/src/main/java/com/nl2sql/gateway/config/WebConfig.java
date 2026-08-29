package com.nl2sql.gateway.config;

import com.nl2sql.gateway.policy.UserPolicyInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final UserPolicyInterceptor interceptor;

    public WebConfig(UserPolicyInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor)
                .addPathPatterns("/api/v1/task/**", "/api/v1/query/**", "/api/v1/eval/**", "/api/v1/admin/**")
                .excludePathPatterns("/api/v1/health");
    }
}
