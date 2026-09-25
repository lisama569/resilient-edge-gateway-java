package com.github.lisama569.resilientedgegatewayjava.gateway.health;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 节点健康判定：连续失败计数达到阈值后快速隔离（熔断打开），
 * 经过 recoverAfterMs 后允许单次半开试探，试探成功则恢复、失败则重新计时。
 *
 * <p>全部状态以节点 id 为 key 存放在 {@link ConcurrentHashMap} 中，
 * 每个节点独立加锁，并发上报不会丢失计数。
 */
@Component
public class HealthTracker {

    private static final Logger log = LoggerFactory.getLogger(HealthTracker.class);

    public enum State { HEALTHY, CIRCUIT_OPEN, HALF_OPEN }

    private static final class NodeHealth {
        int consecutiveFailures;
        long openedAtMs;
        State state = State.HEALTHY;
    }

    private final ConcurrentHashMap<String, NodeHealth> health = new ConcurrentHashMap<>();
    private final GatewayProperties props;

    public HealthTracker(GatewayProperties props) {
        this.props = props;
    }

    /** 配置重新加载后清理已删除节点的状态。 */
    public void retainOnly(java.util.Set<String> nodeIds) {
        health.keySet().retainAll(nodeIds);
    }

    private NodeHealth of(UpstreamNode node) {
        return health.computeIfAbsent(node.getId(), k -> new NodeHealth());
    }

    /**
     * 节点当前是否可以接收新流量。
     * 熔断打开且未到试探时间 -> false；到时间 -> 置 HALF_OPEN 放行一次试探。
     */
    public boolean isAvailable(UpstreamNode node) {
        if (!node.isEnabled()) {
            return false;
        }
        NodeHealth h = of(node);
        synchronized (h) {
            if (h.state == State.CIRCUIT_OPEN) {
                if (System.currentTimeMillis() - h.openedAtMs >= props.getRecoverAfterMs()) {
                    h.state = State.HALF_OPEN;
                    log.info("[health] node={} 熔断冷却结束 -> 半开试探", node.getId());
                    return true;
                }
                return false;
            }
            return true;
        }
    }

    public boolean isCircuitOpen(UpstreamNode node) {
        NodeHealth h = health.get(node.getId());
        if (h == null) {
            return false;
        }
        synchronized (h) {
            return h.state == State.CIRCUIT_OPEN;
        }
    }

    public State stateOf(String nodeId) {
        NodeHealth h = health.get(nodeId);
        if (h == null) {
            return State.HEALTHY;
        }
        synchronized (h) {
            return h.state;
        }
    }

    public void recordSuccess(UpstreamNode node) {
        NodeHealth h = of(node);
        synchronized (h) {
            boolean recovered = h.state != State.HEALTHY || h.consecutiveFailures != 0;
            h.consecutiveFailures = 0;
            h.state = State.HEALTHY;
            if (recovered) {
                log.info("[health] node={} 试探/调用成功，恢复参与分配", node.getId());
            }
        }
    }

    /**
     * @return true 表示本次失败后节点被熔断隔离
     */
    public boolean recordFailure(UpstreamNode node) {
        NodeHealth h = of(node);
        synchronized (h) {
            h.consecutiveFailures++;
            if (h.state == State.HALF_OPEN
                    || h.consecutiveFailures >= props.getFailureThreshold()) {
                h.state = State.CIRCUIT_OPEN;
                h.openedAtMs = System.currentTimeMillis();
                log.warn("[health] node={} 连续失败 {} 次，熔断隔离 {}ms",
                        node.getId(), h.consecutiveFailures, props.getRecoverAfterMs());
                return true;
            }
            return false;
        }
    }

    /** 管理面手动摘除节点后的健康状态重置（恢复上线时清零历史计数）。 */
    public void reset(String nodeId) {
        NodeHealth h = health.get(nodeId);
        if (h != null) {
            synchronized (h) {
                h.consecutiveFailures = 0;
                h.state = State.HEALTHY;
            }
        }
    }
}
