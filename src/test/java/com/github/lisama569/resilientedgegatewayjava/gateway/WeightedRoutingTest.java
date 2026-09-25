package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.LoadBalancer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 加权路由：权重 3:2:1 时，大样本落点比例必须近似权重比例，
 * 证明权重不会被静默忽略。
 */
class WeightedRoutingTest {

    @Test
    void distributionFollowsWeights() {
        GatewayProperties props = new GatewayProperties();
        HealthTracker health = new HealthTracker(props);
        LoadBalancer lb = new LoadBalancer(health);
        lb.updateNodes(List.of(
                new UpstreamNode("node-a", 3, true, 200),
                new UpstreamNode("node-b", 2, true, 200),
                new UpstreamNode("node-c", 1, true, 200)));

        Map<String, Integer> hits = new HashMap<>();
        int total = 6000;
        for (int i = 0; i < total; i++) {
            ProxyRequest req = new ProxyRequest("caller-1", "req-" + i,
                    null, "GET", "/items/" + i);
            String id = lb.select(req, null).node().getId();
            hits.merge(id, 1, Integer::sum);
        }

        double ratioA = hits.get("node-a") / (double) total;
        double ratioB = hits.get("node-b") / (double) total;
        double ratioC = hits.get("node-c") / (double) total;
        System.out.printf("加权落点比例: node-a=%.3f node-b=%.3f node-c=%.3f (期望 0.5/0.333/0.167)%n",
                ratioA, ratioB, ratioC);

        assertEquals(1.0, ratioA + ratioB + ratioC, 1e-9);
        // 容忍 ±4% 的抽样误差
        assertTrue(Math.abs(ratioA - 0.5) < 0.04, "node-a ratio=" + ratioA);
        assertTrue(Math.abs(ratioB - 1.0 / 3) < 0.04, "node-b ratio=" + ratioB);
        assertTrue(Math.abs(ratioC - 1.0 / 6) < 0.04, "node-c ratio=" + ratioC);
    }
}
