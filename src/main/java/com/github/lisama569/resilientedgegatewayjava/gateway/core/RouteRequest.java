package com.github.lisama569.resilientedgegatewayjava.gateway.core;

/** 请求特征：调用方、会话标识、是否幂等。 */
public record RouteRequest(String callerId, String sessionId, boolean idempotent, String operation) {
}
