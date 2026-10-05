package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 端到端时间预算：
 * 预算掐断提前失败且原因可区分、单次等待取 min(节点超时, 剩余预算)、
 * 预算掐断不误伤节点健康、并发预算互不影响、不带预算行为不回归。
 */
class TimeBudgetTest {

    private static final int NODE_TIMEOUT = 400;

    private GatewayProperties props;
    private HealthTracker health;
    private LoadBalancer lb;
    private SimulatedUpstreamCluster cluster;
    private GatewayService service;

    private UpstreamNode a;
    private UpstreamNode b;
    private UpstreamNode c;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setRecoverAfterMs(10_000); // 测试期间不自动恢复
        props.setMaxAttempts(3);
        props.setCallerDefaultPermitsPerSec(0);
        props.setUpstreamDefaultPermitsPerSec(0);
        health = new HealthTracker(props);
        lb = new LoadBalancer(health);
        cluster = new SimulatedUpstreamCluster();
        service = new GatewayService(lb, health, new RateLimiter(props), cluster, props);

        a = new UpstreamNode("node-a", 1, true, NODE_TIMEOUT);
        b = new UpstreamNode("node-b", 1, true, NODE_TIMEOUT);
        c = new UpstreamNode("node-c", 1, true, NODE_TIMEOUT);
        lb.setDeterministicFirst(true);
        lb.updateNodes(List.of(a, b, c));
    }

    @Test
    void budgetCutAbortsEarlyWithDistinctReasonAndDoesNotHarmNode() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(1000));

        // 连续 3 次（已超过熔断阈值 2）被预算掐断，节点健康必须毫发无损
        for (int i = 1; i <= 3; i++) {
            final String reqId = "req-b" + i;
            long start = System.currentTimeMillis();
            GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                    new ProxyRequest("caller-1", reqId, null, "GET", "/slow", 150L)));
            long elapsed = System.currentTimeMillis() - start;

            assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason(),
                    "预算掐断必须返回可区分原因，而不是 UPSTREAM_TIMEOUT: " + e.getReason());
            assertTrue(elapsed >= 130, "应等到预算耗尽才返回，实际 " + elapsed + "ms");
            assertTrue(elapsed < 350,
                    "必须在预算内提前返回，不能拖到节点超时 " + NODE_TIMEOUT + "ms，实际 " + elapsed + "ms");
            System.out.println("第 " + i + " 次：预算 150ms 掐断，实际耗时 " + elapsed
                    + "ms，原因=" + e.getReason());
        }

        assertTrue(health.isAvailable(a), "预算掐断不得影响节点可用性");
        assertEquals(HealthTracker.State.HEALTHY, health.stateOf("node-a"),
                "被预算掐断 3 次（>熔断阈值 2）后节点仍必须健康，不能被误伤熔断");
        assertEquals(3, cluster.invocationCountOf("node-a"),
                "节点未被隔离，后续请求仍应进入该节点");
        System.out.println("node-a 被预算掐断 3 次后状态=" + health.stateOf("node-a") + "，未被误伤");
    }

    @Test
    void attemptWaitIsMinOfNodeTimeoutAndRemainingBudget() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(1000));

        // 情形 1：预算 150ms < 节点超时 400ms -> 按预算掐表，约 150ms 返回
        long start1 = System.currentTimeMillis();
        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "req-min-1", null, "GET", "/slow", 150L)));
        long elapsed1 = System.currentTimeMillis() - start1;
        assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason());
        assertTrue(elapsed1 >= 130 && elapsed1 < 350,
                "等待应取剩余预算 150ms，实际 " + elapsed1 + "ms");
        System.out.println("预算更紧：等待=" + elapsed1 + "ms（预算 150ms 生效）");

        // 情形 2：预算 5000ms > 节点超时 400ms -> 按节点超时掐表，约 400ms 判定真实超时
        health.reset("node-a");
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-min-2", null, "GET", "/slow", 5000L));
        ProxyResponse.Attempt first = resp.getAttempts().get(0);
        assertEquals("node-a", first.nodeId());
        assertEquals("FAILED", first.outcome());
        assertTrue(first.reason().contains("UPSTREAM_TIMEOUT"),
                "节点超时更紧时应判定为真实节点超时: " + first.reason());
        assertTrue(first.elapsedMs() >= 350 && first.elapsedMs() < 1000,
                "等待应取节点超时 400ms，实际 " + first.elapsedMs() + "ms");
        assertNotNull(first.budgetRemainingMs(), "带预算的请求尝试记录必须含剩余预算");
        assertTrue(first.budgetRemainingMs() <= 5000);
        assertEquals("node-b", resp.getServedBy(), "真实超时计入失败后可转移到 node-b");
        System.out.println("节点超时更紧：第一次尝试等待=" + first.elapsedMs()
                + "ms（节点超时 400ms 生效），剩余预算=" + first.budgetRemainingMs() + "ms");
    }

    @Test
    void generousBudgetKeepsRealTimeoutAndCircuitBehavior() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(1000));

        // 宽松预算下，真实节点超时必须照旧计入连续失败并触发熔断
        for (int i = 1; i <= 2; i++) {
            ProxyResponse resp = service.proxy(
                    new ProxyRequest("caller-1", "req-g" + i, null, "GET", "/slow", 5000L));
            assertEquals("FAILED", resp.getAttempts().get(0).outcome());
            assertTrue(resp.getAttempts().get(0).reason().contains("UPSTREAM_TIMEOUT"));
        }
        assertEquals(HealthTracker.State.CIRCUIT_OPEN, health.stateOf("node-a"),
                "宽松预算下真实超时必须照常累计并熔断");
        System.out.println("宽松预算下 node-a 两次真实超时后正常熔断，既有规则不变");
    }

    @Test
    void concurrentRequestsWithDifferentBudgetsDoNotInterfere() throws Exception {
        // 所有节点"慢但健康"：300ms 后成功；节点超时 400ms
        for (String id : new String[]{"node-a", "node-b", "node-c"}) {
            cluster.setBehavior(id, SimulatedUpstreamCluster.Behavior.slowOk(300));
        }

        int small = 4, large = 4;
        ExecutorService pool = Executors.newFixedThreadPool(small + large);
        CountDownLatch ready = new CountDownLatch(small + large);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<Object>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < small + large; i++) {
                final boolean tightBudget = i < small;
                final long budget = tightBudget ? 100L : 5000L;
                final String reqId = (tightBudget ? "tight-" : "loose-") + i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    long start = System.currentTimeMillis();
                    try {
                        ProxyResponse resp = service.proxy(new ProxyRequest(
                                "caller-x", reqId, null, "GET", "/p", budget));
                        return "OK:" + resp.getServedBy() + ":" + (System.currentTimeMillis() - start);
                    } catch (GatewayException ge) {
                        return "ERR:" + ge.getReason() + ":" + (System.currentTimeMillis() - start);
                    }
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();

            int tightAborted = 0, looseSucceeded = 0;
            for (int i = 0; i < futures.size(); i++) {
                String result = (String) futures.get(i).get(15, TimeUnit.SECONDS);
                long elapsed = Long.parseLong(result.substring(result.lastIndexOf(':') + 1));
                if (i < small) {
                    assertEquals("ERR:BUDGET_EXHAUSTED:" + elapsed, result,
                            "紧预算请求必须因预算耗尽提前结束: " + result);
                    assertTrue(elapsed < 350, "紧预算请求不得被拖到节点超时，实际 " + elapsed + "ms");
                    tightAborted++;
                } else {
                    assertEquals("OK:node-a:" + elapsed, result,
                            "宽预算请求必须正常等到节点成功: " + result);
                    assertTrue(elapsed >= 280, "宽预算请求应等到慢节点返回（~300ms），实际 " + elapsed);
                    looseSucceeded++;
                }
                System.out.println("并发请求 " + i + " -> " + result);
            }
            assertEquals(small, tightAborted);
            assertEquals(large, looseSucceeded);

            // 紧预算请求全部被掐断，但没有产生任何节点失败记录
            for (String id : new String[]{"node-a", "node-b", "node-c"}) {
                assertEquals(HealthTracker.State.HEALTHY, health.stateOf(id),
                        "并发预算掐断不得误伤节点 " + id);
            }
            System.out.println("并发下 " + small + " 个紧预算请求各自提前结束，"
                    + large + " 个宽预算请求正常成功，节点全部保持健康");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void noBudgetKeepsExistingBehavior() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(1000));

        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-nb", null, "GET", "/slow"));
        ProxyResponse.Attempt first = resp.getAttempts().get(0);
        assertEquals("FAILED", first.outcome());
        assertTrue(first.reason().contains("UPSTREAM_TIMEOUT"));
        assertTrue(first.elapsedMs() >= 350,
                "不带预算时等待应完全等于节点超时 400ms，实际 " + first.elapsedMs() + "ms");
        assertNull(first.budgetRemainingMs(), "不带预算时尝试记录不得携带预算信息");
        assertEquals("node-b", resp.getServedBy());
        assertEquals(1, health.stateOf("node-a") == HealthTracker.State.HEALTHY ? 1 : 0,
                "单次真实超时未到阈值，节点仍健康（计数已累计 1）");
        System.out.println("不带预算：第一次尝试=" + first + "，转移 node-b 成功，行为与旧版一致");
    }
}
