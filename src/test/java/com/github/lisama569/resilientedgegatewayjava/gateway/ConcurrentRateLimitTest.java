package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConcurrentRateLimitTest {

    @Test
    void burstNeverExceedsPermitLimit() throws Exception {
        GatewayProperties props = new GatewayProperties();
        props.setCallerDefaultPermitsPerSec(50);
        RateLimiter limiter = new RateLimiter(props);

        int threads = 200;
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (limiter.tryAcquire(RateLimiter.DIM_CALLER, "tenant-1")) {
                    allowed.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        System.out.printf("并发 %d 突发，caller 配额 50，实际放行 %d%n", threads, allowed.get());
        assertEquals(50, allowed.get(), "突发并发下放行数必须严格等于配额，不得超发");
    }

    @Test
    void callerAndUpstreamDimensionsAreIndependent() throws Exception {
        GatewayProperties props = new GatewayProperties();
        props.setCallerDefaultPermitsPerSec(3);
        props.setUpstreamDefaultPermitsPerSec(100);
        RateLimiter limiter = new RateLimiter(props);

        int callerAllowed = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire(RateLimiter.DIM_CALLER, "tenant-A")) {
                callerAllowed++;
            }
        }
        assertEquals(3, callerAllowed, "调用方维度 3 个配额后必须限流");
        // 调用方被限流不影响任何上游节点维度的计数
        int upstreamAllowed = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire(RateLimiter.DIM_UPSTREAM, "node-a")) {
                upstreamAllowed++;
            }
        }
        assertEquals(10, upstreamAllowed, "上游维度必须拥有独立 100 配额，不受调用方限流影响");
        System.out.println("调用方放满 3 后，上游维度仍独立放行 10 次");
    }

    @Test
    void differentCallersHaveIsolatedQuotas() throws Exception {
        GatewayProperties props = new GatewayProperties();
        props.setCallerDefaultPermitsPerSec(2);
        RateLimiter limiter = new RateLimiter(props);

        Map<String, AtomicInteger> allowed = new ConcurrentHashMap<>();
        int threads = 60;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String caller = "tenant-" + (i % 3);
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (limiter.tryAcquire(RateLimiter.DIM_CALLER, caller)) {
                    allowed.computeIfAbsent(caller, k -> new AtomicInteger()).incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        for (String caller : List.of("tenant-0", "tenant-1", "tenant-2")) {
            assertEquals(2, allowed.get(caller).get(),
                    caller + " 必须独立享有 2 个配额，不能被其他调用方占用");
        }
        System.out.println("3 个调用方在突发并发下各自精确放行 2，互不影响: " + allowed);
        assertTrue(true);
    }
}
