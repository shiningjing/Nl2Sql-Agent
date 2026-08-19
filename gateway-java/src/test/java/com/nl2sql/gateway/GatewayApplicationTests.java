package com.nl2sql.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "engine.base-url=http://127.0.0.1:1")
class GatewayApplicationTests {

    @Test
    void contextLoads() {
    }
}
