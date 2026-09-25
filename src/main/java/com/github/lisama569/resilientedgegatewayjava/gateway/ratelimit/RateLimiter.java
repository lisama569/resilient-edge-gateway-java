package com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 固定窗口限流器。按"维度 + key"独立统计，调用方维度与上游维度互不影响，
 * 不同 caller / 不同 node 之间也各自独立计数、互不借用配额。
 *
 * <p>计数与窗口翻转使用 synchronized 临界区保证一致性：并发突发下，
 * 放行数严格不超过 permitsPerSec，不会因为并发自增而超发。
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    public static final String DIM_CALLER = "caller";
    public static final String DIM_UPSTREAM = "upstream";

    private static final class Counter {
        long windowStartMs = System.currentTimeMillis();
        int used;
    }

    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final GatewayProperties props;

    public RateLimiter(GatewayProperties props) {
        this.props = props;
    }

    private int permitsFor(String dimension, String key) {
        if (DIM_CALLER.equals(dimension)) {
            return props.getCallerOverrides().getOrDefault(key, props.getCallerDefaultPermitsPerSec());
        }
        if (DIM_UPSTREAM.equals(dimension)) {
            return props.getUpstreamOverrides().getOrDefault(key, props.getUpstreamDefaultPermitsPerSec());
        }
        throw new IllegalArgumentException("unknown rate-limit dimension: " + dimension);
    }

    /**
     * 尝试占用一个配额。
     *
     * @return true 放行；false 被限流（调用方可据此区分 RATE_LIMITED_CALLER / RATE_LIMITED_UPSTREAM）
     */
    public boolean tryAcquire(String dimension, String key) {
        int permits = permitsFor(dimension, key);
        if (permits <= 0) {
            return true; // 0 表示不限制
        }
        Counter c = counters.computeIfAbsent(dimension + ':' + key, k -> new Counter());
        boolean allowed;
        int usedAfter;
        synchronized (c) {
            long now = System.currentTimeMillis();
            if (now - c.windowStartMs >= 1000L) {
                c.windowStartMs = now;
                c.used = 0;
            }
            if (c.used < permits) {
                c.used++;
                allowed = true;
            } else {
                allowed = false;
            }
            usedAfter = c.used;
        }
        if (!allowed) {
            log.warn("[ratelimit] 维度={} key={} 被限流（窗口已用 {}/{}）", dimension, key, usedAfter, permits);
        }
        return allowed;
    }

    /** 当前窗口已用量（测试可观察用）。 */
    public int currentUsage(String dimension, String key) {
        Counter c = counters.get(dimension + ':' + key);
        if (c == null) {
            return 0;
        }
        synchronized (c) {
            if (System.currentTimeMillis() - c.windowStartMs >= 1000L) {
                return 0;
            }
            return c.used;
        }
    }

    /** 清空所有计数（测试/管理面用）。 */
    public void reset() {
        counters.clear();
    }
}
