package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.LoadBalancer;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.NodeRegistry;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NodeManagementTest {

    private GatewayProperties props;
    private HealthTracker health;
    private LoadBalancer lb;
    private SimulatedUpstreamCluster cluster;
    private NodeRegistry registry;
    private GatewayService service;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setRecoverAfterMs(10_000);
        props.setMaxAttempts(3);
        props.setCallerDefaultPermitsPerSec(0);
        props.setUpstreamDefaultPermitsPerSec(0);
        props.getNodes().clear();
        props.getNodes().add(nodeProps("node-a", 1));
        props.getNodes().add(nodeProps("node-b", 1));
        props.getNodes().add(nodeProps("node-c", 1));

        health = new HealthTracker(props);
        lb = new LoadBalancer(health);
        lb.setDeterministicFirst(true);
        cluster = new SimulatedUpstreamCluster();
        registry = new NodeRegistry(props, lb, health);
        registry.initFromProperties();
        service = new GatewayService(lb, health, new RateLimiter(props), cluster, props);
    }

    private static GatewayProperties.NodeConfig nodeProps(String id, int weight) {
        GatewayProperties.NodeConfig c = new GatewayProperties.NodeConfig();
        c.setId(id);
        c.setWeight(weight);
        c.setEnabled(true);
        c.setTimeoutMs(500);
        return c;
    }

    @Test
    void disabledNodeNeverReceivesTraffic() {
        registry.setEnabled("node-a", false);
        for (int i = 0; i < 20; i++) {
            ProxyResponse resp = service.proxy(
                    new ProxyRequest("caller-1", "r" + i, null, "GET", "/x"));
            assertFalse("node-a".equals(resp.getServedBy()),
                    "摘除后新流量不得进入 node-a");
        }
        assertEquals(0, cluster.invocationCountOf("node-a"));
        System.out.println("node-a 摘除后 20 次请求调用计数为 0，新流量全部不进入");
    }

    @Test
    void zeroWeightIsRejectedNotSilentlyIgnored() {
        assertThrows(IllegalArgumentException.class,
                () -> registry.adjustWeight("node-a", 0),
                "权重必须为正，禁止静默忽略非法权重");
        UpstreamNode updated = registry.adjustWeight("node-a", 9);
        assertEquals(9, updated.getWeight());
        System.out.println("非法权重 0 被拒绝；合法调权到 9 立即生效");
    }

    @Test
    void nodeSelfRecoversAfterFailFirstN() {
        // node-a 先失败 1 次（未达熔断阈值 2），随后自愈成功：
        // 第一次 GET 转移到 node-b；之后再请求应能重新选中 node-a 并成功
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.failFirstN(1));

        ProxyResponse first = service.proxy(
                new ProxyRequest("caller-1", "r1", null, "GET", "/x"));
        assertEquals("node-b", first.getServedBy());
        assertEquals(2, first.getAttempts().size(), "node-a 失败后必须转移到 node-b");
        System.out.println("第一次请求: node-a 故障 -> 转移 node-b，尝试链=" + first.getAttempts());

        ProxyResponse second = service.proxy(
                new ProxyRequest("caller-1", "r2", null, "GET", "/x"));
        assertEquals("node-a", second.getServedBy(), "node-a 自愈后应重新参与分配并被选中");
        assertEquals(200, second.getStatus());
        System.out.println("第二次请求: node-a 已恢复，直接由 node-a 成功服务");
    }

    @Test
    void disabledThenReenabledNodeServesAgain() {
        registry.setEnabled("node-b", false);
        registry.setEnabled("node-c", false);
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "r1", null, "GET", "/x"));
        assertEquals("node-a", resp.getServedBy());

        registry.setEnabled("node-b", true);
        registry.setEnabled("node-a", false);
        ProxyResponse resp2 = service.proxy(
                new ProxyRequest("caller-1", "r2", null, "GET", "/x"));
        assertEquals("node-b", resp2.getServedBy(), "重新上线的节点应再次参与分配");
        System.out.println("node-b 下线后重新上线，新流量恢复进入 node-b");
    }
}
