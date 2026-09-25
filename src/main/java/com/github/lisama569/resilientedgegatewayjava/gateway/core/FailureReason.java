package com.github.lisama569.resilientedgegatewayjava.gateway.core;

/** 可区分的失败原因。 */
public enum FailureReason {
    NONE,
    RATE_LIMITED_CALLER,
    RATE_LIMITED_UPSTREAM,
    NO_AVAILABLE_NODE,
    CIRCUIT_OPEN,
    TIMEOUT,
    UPSTREAM_ERROR,
    NON_IDEMPOTENT_RETRY_REFUSED
}
