package com.github.lisama569.resilientedgegatewayjava.gateway.core;

/** 网关结果：命中节点、是否成功、可区分失败原因、判定依据。 */
public record GatewayResult(boolean success, String nodeId, FailureReason failureReason, String detail) {

    public static GatewayResult ok(String nodeId, String detail) {
        return new GatewayResult(true, nodeId, FailureReason.NONE, detail);
    }

    public static GatewayResult fail(FailureReason reason, String detail) {
        return new GatewayResult(false, null, reason, detail);
    }

    public static GatewayResult fail(String nodeId, FailureReason reason, String detail) {
        return new GatewayResult(false, nodeId, reason, detail);
    }
}
