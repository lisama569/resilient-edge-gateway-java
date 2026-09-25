package com.github.lisama569.resilientedgegatewayjava.gateway.route;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 节点注册表。维护"期望配置 -> 当前生效节点表"的映射，
 * 调权、摘除、上线都在这里修改并原子下发给 {@link LoadBalancer}，
 * 新流量在下发完成后立即不再进入不可用节点。
 */
@Component
public class NodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);

    private final GatewayProperties props;
    private final LoadBalancer loadBalancer;
    private final HealthTracker healthTracker;

    /** id -> 节点定义，使用同步 Map 保证并发管理操作与下发一致。 */
    private final Map<String, UpstreamNode> nodes = new LinkedHashMap<>();

    public NodeRegistry(GatewayProperties props, LoadBalancer loadBalancer,
                        HealthTracker healthTracker) {
        this.props = props;
        this.loadBalancer = loadBalancer;
        this.healthTracker = healthTracker;
    }

    @PostConstruct
    public synchronized void initFromProperties() {
        nodes.clear();
        for (GatewayProperties.NodeConfig c : props.getNodes()) {
            nodes.put(c.getId(), new UpstreamNode(
                    c.getId(), c.getWeight(), c.isEnabled(), c.getTimeoutMs()));
        }
        publish();
    }

    private void publish() {
        loadBalancer.updateNodes(new ArrayList<>(nodes.values()));
    }

    public synchronized List<UpstreamNode> list() {
        return List.copyOf(nodes.values());
    }

    /** 调整权重：必须为正数；调权后新流量立即按新权重比例进入。 */
    public synchronized UpstreamNode adjustWeight(String nodeId, int newWeight) {
        UpstreamNode n = require(nodeId);
        UpstreamNode updated = n.withWeight(newWeight);
        nodes.put(nodeId, updated);
        publish();
        log.info("[registry] 节点 {} 权重 {} -> {}", nodeId, n.getWeight(), newWeight);
        return updated;
    }

    /** 摘除/下线节点：新流量立即停止进入（健康状态保留，重新上线时可重置）。 */
    public synchronized UpstreamNode setEnabled(String nodeId, boolean enabled) {
        UpstreamNode n = require(nodeId);
        UpstreamNode updated = n.withEnabled(enabled);
        nodes.put(nodeId, updated);
        if (enabled) {
            healthTracker.reset(nodeId);
        }
        publish();
        log.info("[registry] 节点 {} {}", nodeId, enabled ? "重新上线" : "被摘除/下线");
        return updated;
    }

    private UpstreamNode require(String nodeId) {
        UpstreamNode n = nodes.get(nodeId);
        if (n == null) {
            throw new IllegalArgumentException("node not found: " + nodeId);
        }
        return n;
    }
}
