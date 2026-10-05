package com.github.lisama569.resilientedgegatewayjava.gateway.model;

/**
 * 网关失败原因。不同故障场景必须返回不同的原因，调用方可以据此区分
 * 调用方限流 / 上游维度限流 / 无可用节点 / 熔断隔离 / 超时 / 上游错误。
 */
public enum FailureReason {
    /** 调用方维度限流（caller 配额耗尽） */
    RATE_LIMITED_CALLER,
    /** 上游节点维度限流（node 配额耗尽） */
    RATE_LIMITED_UPSTREAM,
    /** 没有任何已启用的节点（全部被摘除或下线） */
    NO_AVAILABLE_NODE,
    /** 有节点但全部处于熔断打开状态（被连续失败隔离） */
    ALL_NODES_CIRCUIT_OPEN,
    /** 上游调用超时 */
    UPSTREAM_TIMEOUT,
    /** 上游返回错误（HTTP 5xx 等） */
    UPSTREAM_ERROR,
    /** 重试后所有节点仍然失败 */
    ALL_ATTEMPTS_FAILED,
    /** 调用方给定的端到端时间预算耗尽（含预算提前掐断单次尝试），不计节点失败 */
    BUDGET_EXHAUSTED
}
