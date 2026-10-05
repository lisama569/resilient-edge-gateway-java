package com.github.lisama569.resilientedgegatewayjava.gateway.error;

import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 把网关失败原因映射成可区分的 HTTP 状态码与 reason 字段：
 * 429=限流（调用方/上游可由 reason 进一步区分），
 * 503=无可用节点或全部熔断，504=上游超时，502=上游错误/重试耗尽。
 */
@RestControllerAdvice
public class GatewayExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GatewayExceptionHandler.class);

    @ExceptionHandler(GatewayException.class)
    public ResponseEntity<Map<String, Object>> handle(GatewayException e) {
        HttpStatus status = switch (e.getReason()) {
            case RATE_LIMITED_CALLER, RATE_LIMITED_UPSTREAM -> HttpStatus.TOO_MANY_REQUESTS;
            case NO_AVAILABLE_NODE, ALL_NODES_CIRCUIT_OPEN, ALL_ATTEMPTS_FAILED ->
                    HttpStatus.SERVICE_UNAVAILABLE;
            case UPSTREAM_TIMEOUT -> HttpStatus.GATEWAY_TIMEOUT;
            case UPSTREAM_ERROR -> HttpStatus.BAD_GATEWAY;
            case BUDGET_EXHAUSTED -> HttpStatus.REQUEST_TIMEOUT;
        };
        log.warn("[gateway] 返回失败 status={} reason={} message={}",
                status.value(), e.getReason(), e.getMessage());
        return ResponseEntity.status(status).body(Map.of(
                "reason", e.getReason().name(),
                "message", e.getMessage() == null ? "" : e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "reason", "BAD_REQUEST", "message", e.getMessage() == null ? "" : e.getMessage()));
    }
}
