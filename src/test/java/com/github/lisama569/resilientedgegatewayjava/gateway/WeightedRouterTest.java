package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.core.RouteRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.routing.WeightedRouter;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class WeightedRouterTest {

    private static RouteRequest anon() {
        return new RouteRequest("caller", null, true, "op");
    }

    @Test
    void weightedDistributionIsDeterministicAndProportional() {
        UpstreamNode a = new UpstreamNode("a", 5);
        UpstreamNode b = new UpstreamNode("b", 3);
        UpstreamNode c = new UpstreamNode("c", 2);
        List<UpstreamNode> candidates = List.of(a, b, c);
        WeightedRouter router = new WeightedRouter();

        List<String> sequence = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sequence.add(router.route(anon(), candidates).orElseThrow().getId());
        }
        Map<String, Long> counts = new TreeMap<>();
        sequence.forEach(id -> counts.merge(id, 1L, Long::sum));
        System.out.println("SWRR distribution over 100 calls: " + counts);

        assertEquals(50, counts.get("a"));
        assertEquals(30, counts.get("b"));
        assertEquals(20, counts.get("c"));

        // 可复现：新路由器重放同样调用，序列完全一致
        WeightedRouter replay = new WeightedRouter();
        for (int i = 0; i < 100; i++) {
            assertEquals(sequence.get(i), replay.route(anon(), candidates).orElseThrow().getId());
        }
    }

    @Test
    void zeroWeightNodeReceivesNoTraffic() {
        UpstreamNode a = new UpstreamNode("a", 1);
        UpstreamNode b = new UpstreamNode("b", 0);
        WeightedRouter router = new WeightedRouter();
        for (int i = 0; i < 20; i++) {
            assertEquals("a", router.route(anon(), List.of(a, b)).orElseThrow().getId());
        }
    }

    @Test
    void stickySessionStaysOnBoundNodeWhileHealthy() {
        UpstreamNode a = new UpstreamNode("a", 1);
        UpstreamNode b = new UpstreamNode("b", 1);
        List<UpstreamNode> candidates = List.of(a, b);
        WeightedRouter router = new WeightedRouter();
        RouteRequest req = new RouteRequest("caller", "session-1", true, "op");

        String first = router.route(req, candidates).orElseThrow().getId();
        for (int i = 0; i < 10; i++) {
            assertEquals(first, router.route(req, candidates).orElseThrow().getId(),
                    "sticky session must stay on bound node");
        }
        assertEquals(Optional.of(first), router.bindingOf("session-1"));
    }

    @Test
    void stickySessionRebindsWhenBoundNodeRemoved() {
        UpstreamNode a = new UpstreamNode("a", 1);
        UpstreamNode b = new UpstreamNode("b", 1);
        WeightedRouter router = new WeightedRouter();
        RouteRequest req = new RouteRequest("caller", "session-2", true, "op");

        String bound = router.route(req, List.of(a, b)).orElseThrow().getId();
        List<UpstreamNode> remaining = new ArrayList<>(List.of(a, b));
        remaining.removeIf(n -> n.getId().equals(bound));
        router.evictNode(bound);

        String rebound = router.route(req, remaining).orElseThrow().getId();
        assertNotEquals(bound, rebound);
        assertEquals(rebound, router.route(req, remaining).orElseThrow().getId(),
                "rebound session must stick to the new node");
    }

    @Test
    void emptyOrAllZeroWeightReturnsEmpty() {
        WeightedRouter router = new WeightedRouter();
        assertTrue(router.route(anon(), List.of()).isEmpty());
        assertTrue(router.route(anon(), List.of(new UpstreamNode("x", 0))).isEmpty());
    }
}
