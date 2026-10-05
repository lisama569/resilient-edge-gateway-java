package com.github.lisama569.resilientedgegatewayjava.gateway.model;

import java.util.List;

/**
 * 网关返回给调用方的响应。{@code attempts} 按顺序记录每次尝试的节点与判定依据，
 * 使"转发到哪个节点、为什么转移"在结果中可观察、可复现。
 */
public final class ProxyResponse {

    private final int status;
    private final String body;
    private final String servedBy;
    private final List<Attempt> attempts;
    /** 请求结束时剩余的时间预算（毫秒）；未设置预算时为 null。 */
    private final Long budgetRemainingMs;

    public ProxyResponse(int status, String body, String servedBy, List<Attempt> attempts) {
        this(status, body, servedBy, attempts, null);
    }

    public ProxyResponse(int status, String body, String servedBy, List<Attempt> attempts,
                         Long budgetRemainingMs) {
        this.status = status;
        this.body = body;
        this.servedBy = servedBy;
        this.attempts = List.copyOf(attempts);
        this.budgetRemainingMs = budgetRemainingMs;
    }

    public int getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public String getServedBy() {
        return servedBy;
    }

    public List<Attempt> getAttempts() {
        return attempts;
    }

    /** 剩余预算（毫秒）；未设置预算时为 null。 */
    public Long getBudgetRemainingMs() {
        return budgetRemainingMs;
    }

    /** 单次上游尝试的记录：命中节点、结果与判定依据。 */
    public record Attempt(String nodeId, String outcome, String reason, long elapsedMs) {
        public boolean isSuccess() {
            return "SUCCESS".equals(outcome);
        }
    }

    @Override
    public String toString() {
        return "ProxyResponse{status=" + status + ", servedBy='" + servedBy
                + "', attempts=" + attempts + '}';
    }
}
