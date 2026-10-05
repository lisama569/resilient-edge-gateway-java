package com.github.lisama569.resilientedgegatewayjava.gateway.model;

/**
 * 进入网关的请求特征。
 * <ul>
 *   <li>{@code caller}：调用方标识，用于按调用方维度限流；</li>
 *   <li>{@code sessionId}：会话标识（如会话 cookie / 用户 id），
 *       非空时在其绑定节点健康期间必须保持稳定落点（会话粘滞）；</li>
 *   <li>{@code method}：HTTP 方法，GET/HEAD 等视为幂等，失败可以安全重试；</li>
 *   <li>{@code path}：请求路径，作为无会话时的路由哈希特征。</li>
 * </ul>
 */
public final class ProxyRequest {

    private final String caller;
    private final String requestId;
    private final String sessionId;
    private final String method;
    private final String path;
    /** 调用方可接受的端到端最长总耗时（毫秒）；null 或 <=0 表示不设预算。 */
    private final Long budgetMs;

    public ProxyRequest(String caller, String requestId, String sessionId, String method, String path) {
        this(caller, requestId, sessionId, method, path, null);
    }

    public ProxyRequest(String caller, String requestId, String sessionId, String method,
                        String path, Long budgetMs) {
        this.caller = caller == null ? "anonymous" : caller;
        this.requestId = (requestId == null || requestId.isBlank())
                ? caller + ':' + sessionId + ':' + method + ':' + path : requestId;
        this.sessionId = (sessionId == null || sessionId.isBlank()) ? null : sessionId;
        this.method = method == null ? "GET" : method.toUpperCase();
        this.path = path == null ? "/" : path;
        this.budgetMs = (budgetMs == null || budgetMs <= 0) ? null : budgetMs;
    }

    /** 调用方给定的端到端时间预算（毫秒）；未设置时返回 null。 */
    public Long getBudgetMs() {
        return budgetMs;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getCaller() {
        return caller;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getMethod() {
        return method;
    }

    public String getPath() {
        return path;
    }

    /**
     * 是否允许失败转移重试。只有显式幂等方法才允许，POST/PUT/PATCH/DELETE
     * 等非幂等请求即使失败也不得在第二个节点上重复执行。
     */
    public boolean isIdempotent() {
        return switch (method) {
            case "GET", "HEAD", "OPTIONS", "PUT", "DELETE" -> true;
            default -> false;
        };
    }

    @Override
    public String toString() {
        return "ProxyRequest{caller='" + caller + "', sessionId='" + sessionId
                + "', method=" + method + ", path='" + path + "'}";
    }
}
