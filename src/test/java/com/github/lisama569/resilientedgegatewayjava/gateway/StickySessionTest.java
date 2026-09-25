package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.RouteDecision;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.LoadBalancer;
import java.util.List;
import org.junit.jupiter.api.Test;

class StickySessionTest {

    private LoadBalancer newBalancer(List<UpstreamNode> nodes) {
        GatewayProperties props = new GatewayProperties();
        props.setFailureThreshold(2);
        HealthTracker health = new HealthTracker(props);
        LoadBalancer lb = new LoadBalancer(health);
        lb.updateNodes(nodes);
        return lb;
    }

    @Test
    void sessionStaysOnBoundNodeWhileHealthy() {
        LoadBalancer lb = newBalancer(List.of(
                new UpstreamNode("node-a", 1, true, 200),
                new UpstreamNode("node-b", 1, true, 200),
                new UpstreamNode("node-c", 1, true, 200)));

        ProxyRequest first = new ProxyRequest("c1", "r1", "sess-X", "GET", "/p");
        String bound = lb.select(first, null).node().getId();
        System.out.println("会话 sess-X 首次绑定节点: " + bound);

        for (int i = 0; i < 50; i++) {
            RouteDecision d = lb.select(
                    new ProxyRequest("c1", "r" + i, "sess-X", "GET", "/p" + i), null);
            assertEquals(bound, d.node().getId(), "会话在健康期必须稳定落点");
            assertEquals(RouteDecision.STICKY_SESSION, d.reason(),
                    "判定依据必须是 STICKY_SESSION");
        }
        System.out.println("会话 sess-X 连续 50 次稳定落在 " + bound + "（依据=STICKY_SESSION）");
    }

    @Test
    void sessionReboundsAfterNodeDisabled() {
        List<UpstreamNode> nodes = new java.util.ArrayList<>(List.of(
                new UpstreamNode("node-a", 1, true, 200),
                new UpstreamNode("node-b", 1, true, 200)));
        GatewayProperties props = new GatewayProperties();
        HealthTracker health = new HealthTracker(props);
        LoadBalancer lb = new LoadBalancer(health);
        lb.setDeterministicFirst(true);
        lb.updateNodes(nodes);

        String bound = lb.select(
                new ProxyRequest("c1", "r1", "sess-Y", "GET", "/p"), null).node().getId();
        assertEquals("node-a", bound);
        System.out.println("会话 sess-Y 绑定节点: " + bound);

        // 摘除绑定节点
        List<UpstreamNode> updated = nodes.stream()
                .map(n -> n.getId().equals(bound) ? n.withEnabled(false) : n).toList();
        lb.updateNodes(updated);

        RouteDecision next = lb.select(
                new ProxyRequest("c1", "r2", "sess-Y", "GET", "/p"), null);
        assertNotEquals(bound, next.node().getId(), "摘除后新流量不得再进入旧节点");
        assertEquals(RouteDecision.STICKY_REBOUND, next.reason(), "必须重新绑定并记录依据");
        System.out.println("绑定节点 " + bound + " 被摘除，会话重新绑定到 "
                + next.node().getId() + "（依据=STICKY_REBOUND）");
    }

    @Test
    void sessionReboundsAfterCircuitOpenAndNeverSelectsOpenNode() {
        GatewayProperties props = new GatewayProperties();
        props.setFailureThreshold(2);
        HealthTracker health = new HealthTracker(props);
        LoadBalancer lb = new LoadBalancer(health);
        UpstreamNode a = new UpstreamNode("node-a", 1, true, 200);
        UpstreamNode b = new UpstreamNode("node-b", 1, true, 200);
        lb.updateNodes(List.of(a, b));

        String bound = lb.select(
                new ProxyRequest("c1", "r1", "sess-Z", "GET", "/p"), null).node().getId();
        UpstreamNode boundNode = bound.equals("node-a") ? a : b;
        UpstreamNode otherNode = bound.equals("node-a") ? b : a;

        health.recordFailure(boundNode);
        boolean opened = health.recordFailure(boundNode);
        org.junit.jupiter.api.Assertions.assertTrue(opened, "连续失败 2 次必须熔断");
        System.out.println("绑定节点 " + bound + " 连续失败 2 次 -> 熔断隔离");

        for (int i = 0; i < 10; i++) {
            RouteDecision d = lb.select(
                    new ProxyRequest("c1", "r" + i, "sess-Z", "GET", "/p"), null);
            assertEquals(otherNode.getId(), d.node().getId(),
                    "熔断节点不得再接收新流量");
        }
        System.out.println("熔断后 10 次请求全部落在 " + otherNode.getId());
        assertEquals(otherNode.getId(), lb.bindingOf("sess-Z"), "会话应已重新绑定");
    }
}
