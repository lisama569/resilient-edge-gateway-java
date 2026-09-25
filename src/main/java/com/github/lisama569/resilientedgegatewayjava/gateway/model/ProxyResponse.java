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

    public ProxyResponse(int status, String body, String servedBy, List<Attempt> attempts) {
        this.status = status;
        this.body = body;
        this.servedBy = servedBy;
        this.attempts = List.copyOf(attempts);
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
