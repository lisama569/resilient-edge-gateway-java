package com.github.lisama569.resilientedgegatewayjava.gateway.model;

import java.util.Objects;

/**
 * 上游节点定义。权重 weight 必须为正整数，节点被摘除/下线时 enabled=false，
 * 摘除后新流量不得再进入该节点。运行期状态（熔断计数）不放在这里，
 * 由 {@code HealthTracker} 维护，保证节点定义本身是不可变配置。
 */
public final class UpstreamNode {

    private final String id;
    private final int weight;
    private final boolean enabled;
    private final int timeoutMs;

    public UpstreamNode(String id, int weight, boolean enabled, int timeoutMs) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("node weight must be positive: " + id);
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("node timeoutMs must be positive: " + id);
        }
        this.id = id;
        this.weight = weight;
        this.enabled = enabled;
        this.timeoutMs = timeoutMs;
    }

    public String getId() {
        return id;
    }

    public int getWeight() {
        return weight;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    /** 摘除/恢复/调权时产生新的不可变实例，权重与 id 保持不变。 */
    public UpstreamNode withEnabled(boolean newEnabled) {
        return new UpstreamNode(id, weight, newEnabled, timeoutMs);
    }

    public UpstreamNode withWeight(int newWeight) {
        return new UpstreamNode(id, newWeight, enabled, timeoutMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UpstreamNode that)) {
            return false;
        }
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "UpstreamNode{id='" + id + "', weight=" + weight
                + ", enabled=" + enabled + ", timeoutMs=" + timeoutMs + '}';
    }
}
