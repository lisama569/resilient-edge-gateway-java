package com.github.lisama569.resilientedgegatewayjava.gateway.model;

/**
 * 单次请求的端到端时间预算。在网关收到请求时创建并开始计时，
 * 之后的每次尝试（含重试与失败转移）都从同一个预算中扣减。
 *
 * <p>实例完全归属于单个请求、创建后不再修改（只读取单调时钟），
 * 因此并发请求之间不存在任何共享状态，各算各的。
 * 未设置预算时使用共享的 {@link #none()} 常量，语义等同于"无限"。
 */
public final class TimeBudget {

    private static final TimeBudget NONE = new TimeBudget(0, 0);

    private final long budgetMs;
    private final long startNanos;

    private TimeBudget(long budgetMs, long startNanos) {
        this.budgetMs = budgetMs;
        this.startNanos = startNanos;
    }

    /** 依据请求创建预算；budgetMs 为 null 或 <=0 时视为不设预算。 */
    public static TimeBudget of(Long budgetMs) {
        if (budgetMs == null || budgetMs <= 0) {
            return NONE;
        }
        return new TimeBudget(budgetMs, System.nanoTime());
    }

    /** 无预算实例：剩余时间视为无限，行为与未引入预算前完全一致。 */
    public static TimeBudget none() {
        return NONE;
    }

    public boolean hasBudget() {
        return budgetMs > 0;
    }

    /** 已耗时（毫秒）。无预算时返回 0。 */
    public long elapsedMs() {
        if (!hasBudget()) {
            return 0;
        }
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /** 剩余预算（毫秒）；无预算时返回 {@link Long#MAX_VALUE}。 */
    public long remainingMs() {
        if (!hasBudget()) {
            return Long.MAX_VALUE;
        }
        return budgetMs - elapsedMs();
    }

    /** 预算是否已经耗尽（剩余 <= 0）。无预算时恒为 false。 */
    public boolean isExhausted() {
        return hasBudget() && remainingMs() <= 0;
    }

    /**
     * 本次尝试实际允许等待的最长毫秒数：节点自身超时与剩余预算取较小者。
     */
    public long attemptTimeoutMs(long nodeTimeoutMs) {
        return Math.min(nodeTimeoutMs, remainingMs());
    }

    @Override
    public String toString() {
        return hasBudget()
                ? "TimeBudget{budgetMs=" + budgetMs + ", remainingMs=" + remainingMs() + '}'
                : "TimeBudget{none}";
    }
}
