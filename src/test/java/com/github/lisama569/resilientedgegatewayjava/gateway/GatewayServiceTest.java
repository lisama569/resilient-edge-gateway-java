package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.core.FailureReason;
import com.github.lisama569.resilientedgegatewayjava.gateway.core.GatewayResult;
import com.github.lisama569.resilientedgegatewayjava.gateway.core.GatewayService;
import com.github.lisama569.resilientedgegatewayjava.gateway.core.RouteRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthManager;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.routing.WeightedRouter;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class GatewayServiceTest {

    private UpstreamRegistry registry;
    private HealthManager healthManager;
    private WeightedRouter router;
    private AtomicLong now;
    private GatewayService gateway;
    private UpstreamNode a, b, c;

    @BeforeEach
    void setUp() {
        now = new AtomicLong(0);
        registry = new UpstreamRegistry();
        a = new UpstreamNode("a", 1);
        b = new UpstreamNode("b", 1);
        c = new UpstreamNode("c", 1);
        registry.addNode(a);
        registry.addNode(b);
        registry.addNode(c);
        healthManager = new HealthManager(2, 1000, now::get);
        router = new WeightedRouter();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (gateway != null) {
            gateway.close();
        }
    }

    private GatewayService newGateway(RateLimiter limiter, long timeoutMillis, int maxAttempts) {
        gateway = new GatewayService(registry, healthManager, limiter, router,
                timeoutMillis, maxAttempts);
        return gateway;
    }

    private static RateLimiter unlimited() {
        return new RateLimiter(1_000_000, 1_000_000, 1_000_000, 1_000_000);
    }

    @Test
    void idempotentRequestFailsOverToHealthyNode() {
        GatewayService gw = newGateway(unlimited(), 200, 3);
        a.setFailing(true);
        RouteRequest req = new RouteRequest("caller", null, true, "op");

        // 权重相同，第一次路由必命中 a（SWRR 首个序列由 id 顺序决定前先累计相同，
        // 这里直接反复调用直到命中过 a，验证转移行为）
        GatewayResult result = null;
        for (int i = 0; i < 3; i++) {
            result = gw.handle(req);
            assertTrue(result.success(), "幂等请求应转移到健康节点成功: " + result);
        }
        assertTrue(a.getExecutionCount() >= 1, "a 至少被尝试过一次");
        assertTrue(b.getExecutionCount() + c.getExecutionCount() >= 1,
                "失败请求必须转移到 b 或 c 执行");
        System.out.println("failover: a.exec=" + a.getExecutionCount()
                + ", b.exec=" + b.getExecutionCount() + ", c.exec=" + c.getExecutionCount());
    }

    @Test
    void nonIdempotentRequestIsNeverExecutedTwice() {
        GatewayService gw = newGateway(unlimited(), 200, 3);
        a.setFailing(true);
        b.setFailing(true);
        c.setFailing(true);
        RouteRequest req = new RouteRequest("caller", null, false, "charge");

        GatewayResult result = gw.handle(req);
        assertFalse(result.success());
        long totalExecutions = a.getExecutionCount() + b.getExecutionCount() + c.getExecutionCount();
        System.out.println("non-idempotent: reason=" + result.failureReason()
                + ", totalExecutions=" + totalExecutions);
        assertEquals(1, totalExecutions, "非幂等请求失败时绝不允许在其它节点重复执行");
        assertEquals(FailureReason.UPSTREAM_ERROR, result.failureReason());
    }

    @Test
    void timeoutIsDistinguishableAndTripsCircuit() {
        GatewayService gw = newGateway(unlimited(), 100, 1);
        a.setLatencyMillis(5_000); // 远超超时
        b.setWeight(0);
        c.setWeight(0); // 只留 a 可选

        RouteRequest req = new RouteRequest("caller", null, true, "op");
        GatewayResult r1 = gw.handle(req);
        assertEquals(FailureReason.TIMEOUT, r1.failureReason());
        GatewayResult r2 = gw.handle(req);
        assertEquals(FailureReason.TIMEOUT, r2.failureReason());
        assertEquals(HealthManager.State.OPEN, healthManager.stateOf("a"),
                "连续超时达到阈值应熔断");

        GatewayResult r3 = gw.handle(req);
        System.out.println("circuit-open result: " + r3);
        assertEquals(FailureReason.CIRCUIT_OPEN, r3.failureReason(),
                "熔断后应快速失败且原因可区分");
        assertEquals(2, a.getExecutionCount(), "熔断后不得再打到故障节点");
    }

    @Test
    void recoveredNodeRejoinsAfterCooldown() {
        GatewayService gw = newGateway(unlimited(), 200, 1);
        b.setWeight(0);
        c.setWeight(0);
        a.setFailing(true);
        RouteRequest req = new RouteRequest("caller", null, true, "op");

        gw.handle(req);
        gw.handle(req); // 2 次失败 -> OPEN
        assertEquals(FailureReason.CIRCUIT_OPEN, gw.handle(req).failureReason());

        a.setFailing(false); // 节点恢复
        now.addAndGet(1000); // 冷却期结束
        GatewayResult result = gw.handle(req);
        System.out.println("recovered: " + result);
        assertTrue(result.success(), "恢复后应重新参与分配并成功");
        assertEquals("a", result.nodeId());
        assertEquals(HealthManager.State.CLOSED, healthManager.stateOf("a"));
    }

    @Test
    void callerRateLimitReasonIsDistinguishable() {
        RateLimiter limiter = new RateLimiter(2, 0, 1000, 0);
        GatewayService gw = newGateway(limiter, 200, 1);
        RouteRequest req = new RouteRequest("caller", null, true, "op");
        assertTrue(gw.handle(req).success());
        assertTrue(gw.handle(req).success());
        GatewayResult r3 = gw.handle(req);
        System.out.println("caller-limited: " + r3);
        assertEquals(FailureReason.RATE_LIMITED_CALLER, r3.failureReason());
        assertEquals(2, a.getExecutionCount() + b.getExecutionCount() + c.getExecutionCount(),
                "被限流的请求不得消耗上游执行");
    }

    @Test
    void upstreamRateLimitDoesNotConsumeCallerQuotaOfOtherCallers() {
        // 上游容量 1：第一个请求占满后，其余请求在上游维度被拒
        RateLimiter limiter = new RateLimiter(1000, 0, 1, 0);
        GatewayService gw = newGateway(limiter, 200, 1);
        b.setWeight(0);
        c.setWeight(0);

        GatewayResult r1 = gw.handle(new RouteRequest("c1", null, true, "op"));
        assertTrue(r1.success());
        GatewayResult r2 = gw.handle(new RouteRequest("c1", null, true, "op"));
        System.out.println("upstream-limited: " + r2);
        assertEquals(FailureReason.RATE_LIMITED_UPSTREAM, r2.failureReason());
        // 其它 caller 的配额不受影响（caller 维度独立统计）
        assertEquals(1000, (long) limiter.callerAvailableTokens("c2"),
                "其它 caller 的配额不得被上游维度限流消耗");
        assertEquals(1000 - 2, (long) limiter.callerAvailableTokens("c1"),
                "c1 两次调用各消耗 1 个 caller 令牌");
    }

    @Test
    void concurrentCallerBurstRespectsCapacityExactly() throws Exception {
        int capacity = 30;
        RateLimiter limiter = new RateLimiter(capacity, 0, 1_000_000, 1_000_000);
        GatewayService gw = newGateway(limiter, 200, 1);

        int threads = 150;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<GatewayResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Callable<GatewayResult> task = () -> {
                start.await();
                return gw.handle(new RouteRequest("hot-caller", null, true, "op"));
            };
            futures.add(pool.submit(task));
        }
        start.countDown();
        long ok = 0, limited = 0;
        for (Future<GatewayResult> f : futures) {
            GatewayResult r = f.get();
            if (r.success()) {
                ok++;
            } else if (r.failureReason() == FailureReason.RATE_LIMITED_CALLER) {
                limited++;
            }
        }
        pool.close();
        System.out.println("gateway burst: capacity=" + capacity + ", ok=" + ok + ", limited=" + limited);
        assertEquals(capacity, ok, "突发并发下放行量不得超过配置上限");
        assertEquals(threads - capacity, limited);
    }

    @Test
    void removedNodeReceivesNoNewTraffic() {
        GatewayService gw = newGateway(unlimited(), 200, 1);
        RouteRequest sticky = new RouteRequest("caller", "s1", true, "op");
        GatewayResult first = gw.handle(sticky);
        assertTrue(first.success());
        String bound = first.nodeId();

        registry.removeNode(bound);
        healthManager.reset(bound);
        router.evictNode(bound);

        for (int i = 0; i < 10; i++) {
            GatewayResult r = gw.handle(sticky);
            assertTrue(r.success());
            assertNotEquals(bound, r.nodeId(), "被摘除节点不得再接收新流量");
        }
    }
}
