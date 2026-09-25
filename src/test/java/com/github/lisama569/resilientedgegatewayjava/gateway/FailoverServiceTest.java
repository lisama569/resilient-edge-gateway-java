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
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FailoverServiceTest {

    private GatewayProperties props;
    private HealthTracker health;
    private LoadBalancer lb;
    private SimulatedUpstreamCluster cluster;
    private RateLimiter rateLimiter;
    private GatewayService service;

    private UpstreamNode a;
    private UpstreamNode b;
    private UpstreamNode c;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setRecoverAfterMs(10_000); // 测试期间不自动恢复，保证结论确定
        props.setMaxAttempts(3);
        props.setCallerDefaultPermitsPerSec(0);
        props.setUpstreamDefaultPermitsPerSec(0);
        health = new HealthTracker(props);
        lb = new LoadBalancer(health);
        cluster = new SimulatedUpstreamCluster();
        rateLimiter = new RateLimiter(props);
        service = new GatewayService(lb, health, rateLimiter, cluster, props);

        a = new UpstreamNode("node-a", 1, true, 500);
        b = new UpstreamNode("node-b", 1, true, 500);
        c = new UpstreamNode("node-c", 1, true, 500);
        lb.setDeterministicFirst(true);
        lb.updateNodes(List.of(a, b, c));
    }

    @Test
    void idempotentRequestFailsOverToHealthyNode() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.fail());
        ProxyRequest req = new ProxyRequest("caller-1", "req-1", null, "GET", "/orders/1");
        ProxyResponse resp = service.proxy(req);

        System.out.println("幂等请求尝试链: " + resp.getAttempts());
        assertTrue(resp.getAttempts().size() >= 2, "失败后必须转移到第二个节点");
        assertEquals("node-a", resp.getAttempts().get(0).nodeId(), "首个节点应为 node-a");
        assertEquals("FAILED", resp.getAttempts().get(0).outcome());
        assertTrue(resp.getAttempts().get(0).reason().contains("UPSTREAM_ERROR"),
                "失败原因必须可区分: " + resp.getAttempts().get(0).reason());
        assertEquals("SUCCESS", resp.getAttempts().get(resp.getAttempts().size() - 1).outcome());
        assertFalse("node-a".equals(resp.getServedBy()), "最终服务节点不得是故障节点");
        assertEquals(1, cluster.invocationCountOf("node-a"));
        System.out.println("最终由 " + resp.getServedBy() + " 提供服务，故障转移成功");
    }

    @Test
    void failedNodeGetsIsolatedAfterThresholdAndAllFailReasonIsDistinct() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.fail());
        // 两次 GET 失败转移把 node-a 的连续失败数打到 2 -> 熔断
        for (int i = 1; i <= 2; i++) {
            ProxyResponse resp = service.proxy(
                    new ProxyRequest("caller-1", "req-a" + i, null, "GET", "/x"));
            assertFalse("node-a".equals(resp.getServedBy()));
        }
        assertFalse(health.isAvailable(a), "连续失败 2 次后 node-a 必须被隔离");
        assertEquals(HealthTracker.State.CIRCUIT_OPEN, health.stateOf("node-a"));
        System.out.println("node-a 两次连续失败后熔断隔离");

        // 三个节点全部故障：该请求实际尝试到的节点全部失败 -> ALL_ATTEMPTS_FAILED
        cluster.setBehavior("node-b", SimulatedUpstreamCluster.Behavior.fail());
        cluster.setBehavior("node-c", SimulatedUpstreamCluster.Behavior.fail());
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.proxy(
                        new ProxyRequest("caller-1", "req-dead", null, "GET", "/x")));
        assertEquals(FailureReason.ALL_ATTEMPTS_FAILED, e.getReason());
        System.out.println("尝试到的节点全部失败 -> 原因=" + e.getReason());

        // 重置健康（模拟恢复窗口）但上游仍全错 -> 每个节点各试 1 次、均未达熔断阈值，
        // 此时必须给出 ALL_ATTEMPTS_FAILED，与熔断原因区分开
        health.reset("node-a");
        health.reset("node-b");
        health.reset("node-c");
        GatewayException e2 = assertThrows(GatewayException.class,
                () -> service.proxy(
                        new ProxyRequest("caller-1", "req-dead-2", null, "GET", "/x")));
        assertEquals(FailureReason.ALL_ATTEMPTS_FAILED, e2.getReason());
        System.out.println("三节点各失败 1 次（未熔断）即耗尽重试 -> 原因=" + e2.getReason());
    }

    @Test
    void nonIdempotentRequestIsNeverRetried() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.fail());
        ProxyRequest req = new ProxyRequest("caller-1", "req-post-1", null, "POST", "/payments");

        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(req));
        assertEquals(FailureReason.UPSTREAM_ERROR, e.getReason(),
                "非幂等失败必须返回明确的上游错误原因，而不是模糊失败");
        assertEquals(1, cluster.invocationCountOf("node-a"),
                "非幂等请求在故障节点只能执行一次");
        assertEquals(0, cluster.invocationCountOf("node-b"),
                "非幂等请求严禁转移到其他节点重复执行");
        assertEquals(0, cluster.invocationCountOf("node-c"));
        assertEquals(0, cluster.duplicateExecutionCount());
        System.out.println("POST 非幂等请求仅在 node-a 执行 1 次，未发生任何转移重试");
    }

    @Test
    void timeoutHasDistinctReason() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(800));
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-to", null, "GET", "/slow"));
        ProxyResponse.Attempt first = resp.getAttempts().get(0);
        assertEquals("node-a", first.nodeId());
        assertTrue(first.reason().contains("UPSTREAM_TIMEOUT"),
                "超时必须有可区分原因: " + first.reason());
        System.out.println("超时尝试记录: " + first + "，最终转移由 " + resp.getServedBy() + " 成功");
    }

    @Test
    void noEnabledNodeAndAllCircuitOpenHaveDistinctReasons() {
        // 场景 1：全部摘除 -> NO_AVAILABLE_NODE
        lb.updateNodes(List.of(
                a.withEnabled(false), b.withEnabled(false), c.withEnabled(false)));
        GatewayException e1 = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "r1", null, "GET", "/x")));
        assertEquals(FailureReason.NO_AVAILABLE_NODE, e1.getReason());
        System.out.println("节点全部摘除 -> " + e1.getReason());

        // 场景 2：节点都启用但全部熔断 -> ALL_NODES_CIRCUIT_OPEN
        lb.updateNodes(List.of(a, b, c));
        health.recordFailure(a);
        health.recordFailure(a);
        health.recordFailure(b);
        health.recordFailure(b);
        health.recordFailure(c);
        health.recordFailure(c);
        GatewayException e2 = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "r2", null, "GET", "/x")));
        assertEquals(FailureReason.ALL_NODES_CIRCUIT_OPEN, e2.getReason());
        System.out.println("节点全部熔断 -> " + e2.getReason());
    }
}
