package com.github.lisama569.resilientedgegatewayjava.gateway.health;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * 节点健康判定（熔断器语义）：
 * - CLOSED：正常参与分配，连续失败达到阈值后进入 OPEN；
 * - OPEN：快速隔离，不参与分配，冷却期结束后进入 HALF_OPEN；
 * - HALF_OPEN：放行一次探测，成功则 CLOSED，失败则重新 OPEN。
 * 时钟可注入，测试中以虚拟时间推进，结论可复现。
 */
public class HealthManager {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private static final class NodeHealth {
        volatile State state = State.CLOSED;
        final AtomicInteger consecutiveFailures = new AtomicInteger();
        volatile long openedAtMillis;
    }

    private final int failureThreshold;
    private final long openCooldownMillis;
    private final LongSupplier clock;
    private final Map<String, NodeHealth> health = new ConcurrentHashMap<>();

    public HealthManager(int failureThreshold, long openCooldownMillis) {
        this(failureThreshold, openCooldownMillis, System::currentTimeMillis);
    }

    public HealthManager(int failureThreshold, long openCooldownMillis, LongSupplier clock) {
        this.failureThreshold = failureThreshold;
        this.openCooldownMillis = openCooldownMillis;
        this.clock = clock;
    }

    private NodeHealth of(String nodeId) {
        return health.computeIfAbsent(nodeId, k -> new NodeHealth());
    }

    /** 节点当前是否可参与分配（含 HALF_OPEN 探测放行）。 */
    public boolean isAvailable(String nodeId) {
        NodeHealth h = of(nodeId);
        State s = h.state;
        if (s == State.CLOSED) {
            return true;
        }
        if (s == State.OPEN && clock.getAsLong() - h.openedAtMillis >= openCooldownMillis) {
            // 冷却期结束，CAS 进入 HALF_OPEN，保证只有一个探测请求被放行
            synchronized (h) {
                if (h.state == State.OPEN
                        && clock.getAsLong() - h.openedAtMillis >= openCooldownMillis) {
                    h.state = State.HALF_OPEN;
                    return true;
                }
            }
            return h.state == State.HALF_OPEN;
        }
        return s == State.HALF_OPEN;
    }

    public void onSuccess(String nodeId) {
        NodeHealth h = of(nodeId);
        synchronized (h) {
            h.consecutiveFailures.set(0);
            h.state = State.CLOSED;
        }
    }

    public void onFailure(String nodeId) {
        NodeHealth h = of(nodeId);
        synchronized (h) {
            int failures = h.consecutiveFailures.incrementAndGet();
            if (h.state == State.HALF_OPEN || failures >= failureThreshold) {
                h.state = State.OPEN;
                h.openedAtMillis = clock.getAsLong();
            }
        }
    }

    public State stateOf(String nodeId) {
        return of(nodeId).state;
    }

    public int consecutiveFailuresOf(String nodeId) {
        return of(nodeId).consecutiveFailures.get();
    }

    /** 摘除节点时清理健康状态，避免重新加入后沿用旧状态。 */
    public void reset(String nodeId) {
        health.remove(nodeId);
    }
}
