package com.github.lisama569.resilientedgegatewayjava.gateway.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关配置（application.properties 中以 {@code gateway.} 为前缀）。
 *
 * <pre>
 * gateway.nodes[0].id=node-a
 * gateway.nodes[0].weight=3
 * gateway.caller-default-permits-per-sec=10
 * gateway.upstream-default-permits-per-sec=100
 * gateway.failure-threshold=2          # 连续失败多少次后熔断隔离
 * gateway.recover-after-ms=200         # 熔断多久后进入半开试探
 * gateway.max-attempts=3
 * </pre>
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private List<NodeConfig> nodes = new ArrayList<>();
    /** 每个调用方每秒放行上限（0 表示不限制）。 */
    private int callerDefaultPermitsPerSec = 20;
    /** 每个上游节点每秒放行上限（0 表示不限制）。 */
    private int upstreamDefaultPermitsPerSec = 100;
    /** 可单独覆盖某个调用方的配额：key=caller。 */
    private java.util.Map<String, Integer> callerOverrides = new java.util.HashMap<>();
    /** 可单独覆盖某个节点的配额：key=nodeId。 */
    private java.util.Map<String, Integer> upstreamOverrides = new java.util.HashMap<>();
    /** 连续失败达到该阈值即熔断隔离。 */
    private int failureThreshold = 2;
    /** 熔断打开后经过多久允许一次半开试探。 */
    private long recoverAfterMs = 500;
    /** 单次请求最多尝试节点数（含首次）；非幂等请求恒为 1。 */
    private int maxAttempts = 3;

    public List<NodeConfig> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeConfig> nodes) {
        this.nodes = nodes;
    }

    public int getCallerDefaultPermitsPerSec() {
        return callerDefaultPermitsPerSec;
    }

    public void setCallerDefaultPermitsPerSec(int v) {
        this.callerDefaultPermitsPerSec = v;
    }

    public int getUpstreamDefaultPermitsPerSec() {
        return upstreamDefaultPermitsPerSec;
    }

    public void setUpstreamDefaultPermitsPerSec(int v) {
        this.upstreamDefaultPermitsPerSec = v;
    }

    public java.util.Map<String, Integer> getCallerOverrides() {
        return callerOverrides;
    }

    public void setCallerOverrides(java.util.Map<String, Integer> v) {
        this.callerOverrides = v;
    }

    public java.util.Map<String, Integer> getUpstreamOverrides() {
        return upstreamOverrides;
    }

    public void setUpstreamOverrides(java.util.Map<String, Integer> v) {
        this.upstreamOverrides = v;
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public long getRecoverAfterMs() {
        return recoverAfterMs;
    }

    public void setRecoverAfterMs(long recoverAfterMs) {
        this.recoverAfterMs = recoverAfterMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public static class NodeConfig {
        private String id;
        private int weight = 1;
        private boolean enabled = true;
        private int timeoutMs = 300;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(int timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
    }
}
