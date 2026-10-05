package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
 * 场景1 预算掐断本次尝试 -> BUDGET_EXHAUSTED，与超时/熔断/限流可区分；
 * 场景2 单次尝试等待 = min(节点超时, 剩余预算) 的两个方向边界；
 * 场景3 预算掐断不计节点失败、不影响健康与熔断；
 * 场景4 同一调用方并发请求各自独立预算互不影响；
 * 场景5 不带预算时行为与旧版完全一致（回归）。
 */
class TimeBudgetTest {

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
        props.setRecoverAfterMs(10_000);
        props.setMaxAttempts(3);
        props.setCallerDefaultPermitsPerSec(0);
        props.setUpstreamDefaultPermitsPerSec(0);
        health = new HealthTracker(props);
        lb = new LoadBalancer(health);
        cluster = new SimulatedUpstreamCluster();
        service = new GatewayService(lb, health, new RateLimiter(props), cluster, props);

        a = new UpstreamNode("node-a", 1, true, 500);
        b = new UpstreamNode("node-b", 1, true, 500);
        c = new UpstreamNode("node-c", 1, true, 500);
        lb.setDeterministicFirst(true);
        lb.updateNodes(List.of(a, b, c));
    }

    @Test
    void budgetCutAttemptFailsFastWithDistinctReason() {
        // 节点自身超时 500ms，但调用方只给 200ms 预算 -> 约 200ms 即被掐断
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        long start = System.currentTimeMillis();
        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "req-b1", null, "GET", "/slow", 200L)));
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason(),
                "预算耗尽必须给出可区分原因，而非 UPSTREAM_TIMEOUT");
        assertNotEquals(FailureReason.UPSTREAM_TIMEOUT, e.getReason());
        assertTrue(elapsed < 450,
                "必须在预算附近提前返回，不能拖到节点超时 500ms，实际=" + elapsed + "ms");
        System.out.println("场景1 预算掐断: reason=" + e.getReason()
                + " 总耗时=" + elapsed + "ms（预算 200ms，节点超时 500ms）");
    }

    @Test
    void attemptWaitIsMinOfNodeTimeoutAndRemainingBudget() {
        // 方向一：剩余预算(200ms) < 节点超时(500ms) -> 本次只等约 200ms
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "req-min1", null, "GET", "/slow", 200L)));
        assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason());
        System.out.println("场景2a 等待取较小值: 预算200ms < 节点超时500ms -> 按预算掐断");

        // 方向二：预算(5000ms) > 节点超时(500ms) -> 仍按节点超时判定为真正的 UPSTREAM_TIMEOUT
        cluster.reset();
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        long start = System.currentTimeMillis();
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-min2", null, "GET", "/slow", 5_000L));
        long firstElapsed = resp.getAttempts().get(0).elapsedMs();
        ProxyResponse.Attempt first = resp.getAttempts().get(0);
        assertEquals("node-a", first.nodeId());
        assertEquals("FAILED", first.outcome());
        assertTrue(first.reason().contains("UPSTREAM_TIMEOUT"),
                "预算宽裕时节点超时仍按节点超时处理: " + first.reason());
        assertTrue(firstElapsed >= 400 && firstElapsed < 1_500,
                "等待应贴近节点超时 500ms 而非预算 5000ms，实际=" + firstElapsed + "ms");
        assertEquals("SUCCESS", resp.getAttempts().get(resp.getAttempts().size() - 1).outcome());
        assertEquals("node-b", resp.getServedBy());
        System.out.println("场景2b 等待取较小值: 预算5000ms > 节点超时500ms -> 首次尝试实际等待 "
                + firstElapsed + "ms 按节点超时，转移 " + resp.getServedBy() + " 成功，"
                + "剩余预算=" + resp.getBudgetRemainingMs() + "ms");
    }

    @Test
    void budgetCutDoesNotHarmNodeHealthOrCircuit() {
        // 连续多次短预算请求被掐断，节点健康与熔断统计必须完全不受影响
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        for (int i = 1; i <= 3; i++) {
            String reqId = "req-h" + i;
            GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                    new ProxyRequest("caller-1", reqId, null, "GET", "/slow", 150L)));
            assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason());
        }
        assertTrue(health.isAvailable(a), "预算掐断不得影响节点可用性");
        assertEquals(HealthTracker.State.HEALTHY, health.stateOf("node-a"),
                "预算掐断不得计入连续失败/熔断统计");
        assertTrue(health.isAvailable(b));
        assertTrue(health.isAvailable(c));

        // 节点恢复快速响应后，不带预算的请求照常成功，证明未被误伤隔离
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.ok());
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-h9", null, "GET", "/ok"));
        assertEquals("node-a", resp.getServedBy(), "健康节点不得因短预算请求被隔离");
        System.out.println("场景3 三次预算掐断后 node-a 状态=" + health.stateOf("node-a")
                + "，后续请求仍命中 node-a 成功");
    }

    @Test
    void genuineTimeoutStillCountsButBudgetStopIsDistinct() {
        // 预算 600ms：第一次尝试真实节点超时(500ms)计失败；
        // 转移到 node-b 时剩余约 100ms 不够再试 -> 预算掐断停止，原因可区分
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        cluster.setBehavior("node-b", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "req-mix", null, "GET", "/slow", 600L)));
        assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason(),
                "最终原因必须是预算耗尽，而不是 UPSTREAM_TIMEOUT 或 ALL_ATTEMPTS_FAILED");

        // node-a 的真实超时要计入失败统计；node-b 只是被预算掐断，不计失败
        assertEquals(1, cluster.invocationCountOf("node-a"));
        // 失败阈值为 2：node-a 已被计入 1 次真实失败，再补 1 次即熔断，
        // 以此证明真实超时确实计入了连续失败统计
        assertTrue(health.recordFailure(a), "node-a 的真实超时必须已计入失败统计");
        assertEquals(HealthTracker.State.CIRCUIT_OPEN, health.stateOf("node-a"));
        assertEquals(HealthTracker.State.HEALTHY, health.stateOf("node-b"),
                "node-b 被预算掐断，不得计入失败统计");
        assertTrue(health.isAvailable(b));
        System.out.println("场景-混合: node-a 真实超时计失败，node-b 预算掐断不计失败，"
                + "最终 reason=" + e.getReason());
    }

    @Test
    void concurrentRequestsHaveIndependentBudgets() throws Exception {
        // 同一调用方并发三个请求：超短预算 / 宽松预算 / 无预算，结果必须各自独立
        props.setFailureThreshold(100); // 本场景聚焦预算隔离，不让熔断介入
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));

        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch ready = new CountDownLatch(3);
        CountDownLatch go = new CountDownLatch(1);
        try {
            Future<String> tight = pool.submit(() -> {
                ready.countDown();
                go.await();
                long t0 = System.currentTimeMillis();
                try {
                    service.proxy(new ProxyRequest("caller-x", "req-tight", null, "GET", "/slow", 150L));
                    return "UNEXPECTED_SUCCESS";
                } catch (GatewayException ge) {
                    return ge.getReason() + ":" + (System.currentTimeMillis() - t0);
                }
            });
            Future<ProxyResponse> loose = pool.submit(() -> {
                ready.countDown();
                go.await();
                return service.proxy(
                        new ProxyRequest("caller-x", "req-loose", null, "GET", "/slow", 5_000L));
            });
            Future<ProxyResponse> none = pool.submit(() -> {
                ready.countDown();
                go.await();
                return service.proxy(new ProxyRequest("caller-x", "req-none", null, "GET", "/slow"));
            });
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();

            String tightResult = tight.get(10, TimeUnit.SECONDS);
            ProxyResponse looseResp = loose.get(10, TimeUnit.SECONDS);
            ProxyResponse noneResp = none.get(10, TimeUnit.SECONDS);

            assertTrue(tightResult.startsWith("BUDGET_EXHAUSTED:"),
                    "短预算请求必须按自己的预算失败: " + tightResult);
            long tightElapsed = Long.parseLong(tightResult.split(":")[1]);
            assertTrue(tightElapsed < 450,
                    "短预算请求不得被其他请求拖长，实际=" + tightElapsed + "ms");

            assertEquals("node-b", looseResp.getServedBy(), "宽松预算请求应真实超时后转移成功");
            assertEquals("node-b", noneResp.getServedBy(), "无预算请求行为不变");
            assertTrue(looseResp.getAttempts().get(0).reason().contains("UPSTREAM_TIMEOUT"),
                    "宽松预算请求首次尝试必须是真正的节点超时");
            assertNull(noneResp.getBudgetRemainingMs(), "无预算请求不携带预算信息");
            assertTrue(looseResp.getBudgetRemainingMs() != null
                            && looseResp.getBudgetRemainingMs() > 0,
                    "宽松预算请求应剩余大量预算");
            System.out.println("场景4 并发隔离: 短预算=" + tightResult
                    + "ms 宽松预算剩余=" + looseResp.getBudgetRemainingMs()
                    + "ms 无预算剩余=" + noneResp.getBudgetRemainingMs());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void nonIdempotentWithBudgetCutTriesOnlyOnce() {
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        GatewayException e = assertThrows(GatewayException.class, () -> service.proxy(
                new ProxyRequest("caller-1", "req-post-b", null, "POST", "/pay", 150L)));
        assertEquals(FailureReason.BUDGET_EXHAUSTED, e.getReason());
        assertEquals(1, cluster.invocationCountOf("node-a"), "非幂等请求只执行一次");
        assertEquals(0, cluster.invocationCountOf("node-b"), "非幂等请求严禁转移");
        assertEquals(0, cluster.duplicateExecutionCount());
        assertEquals(HealthTracker.State.HEALTHY, health.stateOf("node-a"),
                "预算掐断不得误伤节点");
        System.out.println("场景-非幂等: POST 预算掐断仅执行 1 次，无转移无重复执行");
    }

    @Test
    void noBudgetBehavesExactlyAsBefore() {
        // 回归：不带预算时，节点超时 -> 计失败 -> 幂等转移成功，响应不含预算信息
        cluster.setBehavior("node-a", SimulatedUpstreamCluster.Behavior.timeout(2_000));
        ProxyResponse resp = service.proxy(
                new ProxyRequest("caller-1", "req-legacy", null, "GET", "/slow"));
        ProxyResponse.Attempt first = resp.getAttempts().get(0);
        assertEquals("node-a", first.nodeId());
        assertEquals("FAILED", first.outcome());
        assertTrue(first.reason().contains("UPSTREAM_TIMEOUT"));
        assertEquals("node-b", resp.getServedBy());
        assertNull(resp.getBudgetRemainingMs(), "不带预算时响应不得出现预算字段");
        System.out.println("场景5 回归: 无预算请求尝试链=" + resp.getAttempts()
                + "，预算字段=" + resp.getBudgetRemainingMs());
    }
}
