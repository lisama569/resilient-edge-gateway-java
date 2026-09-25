package com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 双维度限流：按调用方与按上游节点，各自独立的令牌桶，互不影响配额统计。
 * 突发并发下通过同步取令牌保证放行量不超过上限；时钟可注入，测试可复现。
 */
public class RateLimiter {

    private static final class Bucket {
        private final long capacity;
        private final double refillPerMillis;
        private double tokens;
        private long lastRefillMillis;

        Bucket(long capacity, double refillPerSecond, long nowMillis) {
            this.capacity = capacity;
            this.refillPerMillis = refillPerSecond / 1000.0;
            this.tokens = capacity;
            this.lastRefillMillis = nowMillis;
        }

        synchronized boolean tryAcquire(long nowMillis) {
            refill(nowMillis);
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }

        synchronized double availableTokens(long nowMillis) {
            refill(nowMillis);
            return tokens;
        }

        private void refill(long nowMillis) {
            if (nowMillis > lastRefillMillis) {
                tokens = Math.min(capacity, tokens + (nowMillis - lastRefillMillis) * refillPerMillis);
                lastRefillMillis = nowMillis;
            }
        }
    }

    private final long callerCapacity;
    private final double callerRefillPerSecond;
    private final long upstreamCapacity;
    private final double upstreamRefillPerSecond;
    private final LongSupplier clock;
    private final Map<String, Bucket> callerBuckets = new ConcurrentHashMap<>();
    private final Map<String, Bucket> upstreamBuckets = new ConcurrentHashMap<>();

    public RateLimiter(long callerCapacity, double callerRefillPerSecond,
                       long upstreamCapacity, double upstreamRefillPerSecond) {
        this(callerCapacity, callerRefillPerSecond, upstreamCapacity, upstreamRefillPerSecond,
                System::currentTimeMillis);
    }

    public RateLimiter(long callerCapacity, double callerRefillPerSecond,
                       long upstreamCapacity, double upstreamRefillPerSecond,
                       LongSupplier clock) {
        this.callerCapacity = callerCapacity;
        this.callerRefillPerSecond = callerRefillPerSecond;
        this.upstreamCapacity = upstreamCapacity;
        this.upstreamRefillPerSecond = upstreamRefillPerSecond;
        this.clock = clock;
    }

    public boolean tryAcquireCaller(String callerId) {
        return callerBuckets
                .computeIfAbsent(callerId, k -> new Bucket(callerCapacity, callerRefillPerSecond, clock.getAsLong()))
                .tryAcquire(clock.getAsLong());
    }

    public boolean tryAcquireUpstream(String nodeId) {
        return upstreamBuckets
                .computeIfAbsent(nodeId, k -> new Bucket(upstreamCapacity, upstreamRefillPerSecond, clock.getAsLong()))
                .tryAcquire(clock.getAsLong());
    }

    /** 仅供测试与观测：某调用方当前可用令牌数。 */
    public double callerAvailableTokens(String callerId) {
        Bucket b = callerBuckets.get(callerId);
        return b == null ? callerCapacity : b.availableTokens(clock.getAsLong());
    }

    public double upstreamAvailableTokens(String nodeId) {
        Bucket b = upstreamBuckets.get(nodeId);
        return b == null ? upstreamCapacity : b.availableTokens(clock.getAsLong());
    }
}
