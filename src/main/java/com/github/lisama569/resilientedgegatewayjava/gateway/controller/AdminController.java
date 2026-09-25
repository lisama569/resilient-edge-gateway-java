package com.github.lisama569.resilientedgegatewayjava.gateway.controller;

import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.NodeRegistry;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 本地管理面：驱动故障/恢复场景并观察内部状态，全部在进程内生效。
 */
@RestController
public class AdminController {

    private final NodeRegistry registry;
    private final HealthTracker healthTracker;
    private final SimulatedUpstreamCluster cluster;
    private final RateLimiter rateLimiter;

    public AdminController(NodeRegistry registry, HealthTracker healthTracker,
                           SimulatedUpstreamCluster cluster, RateLimiter rateLimiter) {
        this.registry = registry;
        this.healthTracker = healthTracker;
        this.cluster = cluster;
        this.rateLimiter = rateLimiter;
    }

    /** 节点配置 + 健康状态总览。 */
    @GetMapping("/admin/nodes")
    public List<Map<String, Object>> nodes() {
        return registry.list().stream().map(n -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", n.getId());
            m.put("weight", n.getWeight());
            m.put("enabled", n.isEnabled());
            m.put("timeoutMs", n.getTimeoutMs());
            m.put("health", healthTracker.stateOf(n.getId()).name());
            m.put("invocations", cluster.invocationCountOf(n.getId()));
            return m;
        }).toList();
    }

    /** 调整节点权重（必须为正整数，禁止静默忽略）。 */
    @PostMapping("/admin/nodes/{nodeId}/weight")
    public Map<String, Object> weight(@PathVariable String nodeId,
                                      @RequestParam int value) {
        UpstreamNode n = registry.adjustWeight(nodeId, value);
        return Map.of("id", n.getId(), "weight", n.getWeight());
    }

    /** 摘除(false)/重新上线(true)节点。 */
    @PostMapping("/admin/nodes/{nodeId}/enabled")
    public Map<String, Object> enabled(@PathVariable String nodeId,
                                       @RequestParam boolean value) {
        UpstreamNode n = registry.setEnabled(nodeId, value);
        return Map.of("id", n.getId(), "enabled", n.isEnabled(),
                "health", healthTracker.stateOf(n.getId()).name());
    }

    /** 设置模拟行为：OK / FAIL / TIMEOUT / FAIL_FIRST_N。 */
    @PostMapping("/admin/nodes/{nodeId}/behavior")
    public Map<String, Object> behavior(@PathVariable String nodeId,
                                        @RequestParam String mode,
                                        @RequestParam(defaultValue = "0") int n,
                                        @RequestParam(defaultValue = "0") int delayMs) {
        SimulatedUpstreamCluster.Behavior b = switch (mode.toUpperCase()) {
            case "OK" -> SimulatedUpstreamCluster.Behavior.ok();
            case "FAIL" -> SimulatedUpstreamCluster.Behavior.fail();
            case "TIMEOUT" -> SimulatedUpstreamCluster.Behavior.timeout(
                    delayMs > 0 ? delayMs : 1000);
            case "FAIL_FIRST_N" -> SimulatedUpstreamCluster.Behavior.failFirstN(Math.max(1, n));
            default -> throw new IllegalArgumentException("unknown mode: " + mode);
        };
        cluster.setBehavior(nodeId, b);
        return Map.of("nodeId", nodeId, "mode", mode.toUpperCase());
    }

    /** 观测：限流窗口用量与非幂等重复执行计数。 */
    @GetMapping("/admin/observability")
    public Map<String, Object> observability() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("duplicateNonIdempotentExecutions", cluster.duplicateExecutionCount());
        m.put("executedNonIdempotentRequestIds", cluster.executedNonIdempotentRequestIds());
        return m;
    }

    /** 清空模拟行为与限流计数（测试隔离用）。 */
    @PostMapping("/admin/reset")
    public Map<String, Object> reset() {
        cluster.reset();
        rateLimiter.reset();
        return Map.of("reset", true);
    }
}
