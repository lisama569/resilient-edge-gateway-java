package com.github.lisama569.resilientedgegatewayjava.gateway.model;

/**
 * 单个请求的端到端时间预算：从网关收到请求起计时，覆盖后续所有
 * 重试与失败转移。每次尝试实际等待时长取"节点自身超时"与"剩余预算"
 * 的较小值；剩余预算不足以再试下一个节点时立即停止。
 *
 * <p>实例只在单个请求的处理线程内使用，不共享任何可变状态，
 * 因此并发请求（即使来自同一调用方）的预算互不影响。
 */
public final class RequestBudget {

    private final long budgetMs;
    private final long deadlineNanos;

    private RequestBudget(long budgetMs) {
        this.budgetMs = budgetMs;
        this.deadlineNanos = System.nanoTime() + budgetMs * 1_000_000L;
    }

    /** 根据请求创建预算；请求未设置预算时返回 null（行为与旧版完全一致）。 */
    public static RequestBudget of(ProxyRequest request) {
        Long budget = request.getBudgetMs();
        return budget == null ? null : new RequestBudget(budget);
    }

    /** 调用方给定的总预算（毫秒）。 */
    public long budgetMs() {
        return budgetMs;
    }

    /** 剩余预算（毫秒），不小于 0。 */
    public long remainingMs() {
        long left = deadlineNanos - System.nanoTime();
        return Math.max(0, left / 1_000_000L);
    }

    /** 预算是否已经耗尽（剩余时间不足以再发起一次尝试）。 */
    public boolean isExhausted() {
        return remainingMs() <= 0;
    }

    /** 本次尝试实际允许等待的时长：节点超时与剩余预算的较小值。 */
    public long attemptWaitMs(UpstreamNode node) {
        return Math.min(node.getTimeoutMs(), remainingMs());
    }

    /** 本次尝试若被掐断，是否由预算（而非节点自身超时）导致。 */
    public boolean wouldCutByBudget(UpstreamNode node) {
        return remainingMs() < node.getTimeoutMs();
    }
}
