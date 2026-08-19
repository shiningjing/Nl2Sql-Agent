package com.nl2sql.gateway.proxy;

/** 引擎不可达/超时/熔断打开时的内部信号，统一映射为 503。 */
public class EngineUnavailableException extends RuntimeException {

    public EngineUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
