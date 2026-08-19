package com.nl2sql.gateway.proxy;

import java.net.http.HttpHeaders;

/** 非流式转发结果：状态码 + 响应头 + 响应体字节。 */
public record EngineResponse(int status, HttpHeaders headers, byte[] body) {
}
