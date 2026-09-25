package com.github.lisama569.resilientedgegatewayjava.gateway.web;

import com.github.lisama569.resilientedgegatewayjava.gateway.core.GatewayResult;
import com.github.lisama569.resilientedgegatewayjava.gateway.core.GatewayService;
import com.github.lisama569.resilientedgegatewayjava.gateway.core.RouteRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthManager;
import com.github.lisama569.resilientedgegatewayjava.gateway.routing.WeightedRouter;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** REST 入口：转发请求、观测与运维节点。 */
@RestController
@RequestMapping("/gateway")
public class GatewayController {

    private final GatewayService gatewayService;
    private final UpstreamRegistry registry;
    private final HealthManager healthManager;
    private final WeightedRouter router;

    public GatewayController(GatewayService gatewayService, UpstreamRegistry registry,
                             HealthManager healthManager, WeightedRouter router) {
        this.gatewayService = gatewayService;
        this.registry = registry;
        this.healthManager = healthManager;
        this.router = router;
    }

    @PostMapping("/invoke")
    public GatewayResult invoke(@RequestParam String callerId,
                                @RequestParam(required = false) String sessionId,
                                @RequestParam(defaultValue = "true") boolean idempotent,
                                @RequestParam(defaultValue = "op") String operation) {
        return gatewayService.handle(new RouteRequest(callerId, sessionId, idempotent, operation));
    }

    @GetMapping("/nodes")
    public List<Map<String, Object>> nodes() {
        return registry.snapshot().stream()
                .map(n -> Map.<String, Object>of(
                        "id", n.getId(),
                        "weight", n.getWeight(),
                        "health", healthManager.stateOf(n.getId()).name(),
                        "executions", n.getExecutionCount()))
                .toList();
    }

    @PostMapping("/nodes/{id}/weight")
    public ResponseEntity<Map<String, Object>> updateWeight(@PathVariable String id,
                                                            @RequestParam int weight) {
        registry.updateWeight(id, weight);
        return ResponseEntity.ok(Map.of("id", id, "weight", weight));
    }

    @PostMapping("/nodes/{id}/fail")
    public Map<String, Object> fail(@PathVariable String id) {
        UpstreamNode node = registry.get(id);
        if (node == null) {
            return Map.of("error", "unknown node: " + id);
        }
        node.setFailing(true);
        return Map.of("id", id, "failing", true);
    }

    @PostMapping("/nodes/{id}/recover")
    public Map<String, Object> recover(@PathVariable String id) {
        UpstreamNode node = registry.get(id);
        if (node == null) {
            return Map.of("error", "unknown node: " + id);
        }
        node.setFailing(false);
        return Map.of("id", id, "failing", false);
    }

    @DeleteMapping("/nodes/{id}")
    public Map<String, Object> remove(@PathVariable String id) {
        UpstreamNode removed = registry.removeNode(id);
        if (removed == null) {
            return Map.of("error", "unknown node: " + id);
        }
        healthManager.reset(id);
        router.evictNode(id);
        return Map.of("removed", id);
    }

    @PostMapping("/nodes")
    public Map<String, Object> add(@RequestParam String id, @RequestParam int weight) {
        registry.addNode(new UpstreamNode(id, weight));
        return Map.of("added", id, "weight", weight);
    }
}
