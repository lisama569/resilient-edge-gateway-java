package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
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

class RateLimiterTest {

    @Test
    void concurrentBurstNeverExceedsCallerCapacity() throws Exception {
        long capacity = 20;
        // 补充速率设为 0，排除时间因素，纯验证并发上限
        RateLimiter limiter = new RateLimiter(capacity, 0, 1000, 0);
        int threads = 200;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(32);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                start.await();
                return limiter.tryAcquireCaller("caller-x");
            });
        }
        List<Future<Boolean>> futures = new ArrayList<>();
        for (Callable<Boolean> t : tasks) {
            futures.add(pool.submit(t));
        }
        start.countDown();
        long granted = 0;
        for (Future<Boolean> f : futures) {
            if (f.get()) {
                granted++;
            }
        }
        pool.close();
        System.out.println("caller burst: capacity=" + capacity + ", threads=" + threads
                + ", granted=" + granted);
        assertEquals(capacity, granted, "放行量必须精确等于容量上限");
    }

    @Test
    void callerAndUpstreamDimensionsAreIndependent() {
        RateLimiter limiter = new RateLimiter(2, 0, 3, 0);
        assertTrue(limiter.tryAcquireCaller("c1"));
        assertTrue(limiter.tryAcquireCaller("c1"));
        assertFalse(limiter.tryAcquireCaller("c1"), "caller 配额耗尽");

        // 上游维度不受 caller 维度消耗影响
        assertTrue(limiter.tryAcquireUpstream("n1"));
        assertTrue(limiter.tryAcquireUpstream("n1"));
        assertTrue(limiter.tryAcquireUpstream("n1"));
        assertFalse(limiter.tryAcquireUpstream("n1"), "upstream 配额耗尽");

        // 不同 caller / 不同 upstream 各自独立
        assertTrue(limiter.tryAcquireCaller("c2"));
        assertTrue(limiter.tryAcquireUpstream("n2"));
    }

    @Test
    void tokensRefillWithVirtualClock() {
        AtomicLong now = new AtomicLong(0);
        RateLimiter limiter = new RateLimiter(1, 10, 1, 10, now::get);
        assertTrue(limiter.tryAcquireCaller("c"));
        assertFalse(limiter.tryAcquireCaller("c"));
        now.addAndGet(100); // 10/s -> 100ms 补 1 个
        assertTrue(limiter.tryAcquireCaller("c"));
        assertFalse(limiter.tryAcquireCaller("c"));
    }
}
