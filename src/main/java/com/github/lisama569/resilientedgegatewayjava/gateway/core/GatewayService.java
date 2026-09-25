package com.github.lisama569.resilientedgegatewayjava.gateway.core;

import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthManager;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.routing.WeightedRouter;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 网关编排，单次请求的处理顺序：
 * 1. 调用方维度限流（超限即拒，不消耗上游配额）；
 * 2. 在健康节点中按权重/会话粘滞路由；
 * 3. 命中节点的上游维度限流；
 * 4. 带超时执行，结果回写健康状态；
 * 5. 幂等请求失败后在其余健康节点间转移；非幂等请求只执行一次，绝不重试。
 * 每一步失败都返回可区分的 FailureReason。
 */
public class GatewayService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GatewayService.class);

    private final UpstreamRegistry registry;
    private final HealthManager healthManager;
    private final RateLimiter rateLimiter;
    private final WeightedRouter router;
    private final long timeoutMillis;
    private final int maxAttempts;
    private final ExecutorService executor;

    public GatewayService(UpstreamRegistry registry, HealthManager healthManager,
                          RateLimiter rateLimiter, WeightedRouter router,
                          long timeoutMillis, int maxAttempts) {
        this.registry = registry;
        this.healthManager = healthManager;
        this.rateLimiter = rateLimiter;
        this.router = router;
        this.timeoutMillis = timeoutMillis;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public GatewayResult handle(RouteRequest request) {
        log.info("request features: callerId={}, sessionId={}, idempotent={}, operation={}",
                request.callerId(), request.sessionId(), request.idempotent(), request.operation());

        if (!rateLimiter.tryAcquireCaller(request.callerId())) {
            log.info("decision: caller [{}] rate limited", request.callerId());
            return GatewayResult.fail(FailureReason.RATE_LIMITED_CALLER,
                    "caller quota exhausted: " + request.callerId());
        }

        int attempts = request.idempotent() ? maxAttempts : 1;
        List<String> tried = new ArrayList<>();
        GatewayResult lastFailure = null;

        for (int i = 0; i < attempts; i++) {
            List<UpstreamNode> candidates = healthyCandidates(tried);
            if (candidates.isEmpty()) {
                FailureReason reason = noEligibleReason();
                log.info("decision: no eligible node, reason={}", reason);
                return lastFailure != null ? lastFailure
                        : GatewayResult.fail(reason, "no eligible upstream node");
            }

            Optional<UpstreamNode> selected = router.route(request, candidates);
            if (selected.isEmpty()) {
                FailureReason reason = noEligibleReason();
                log.info("decision: router returned empty, reason={}", reason);
                return GatewayResult.fail(reason,
                        "all healthy nodes have zero weight or are removed");
            }
            UpstreamNode node = selected.get();

            if (!rateLimiter.tryAcquireUpstream(node.getId())) {
                log.info("decision: upstream [{}] rate limited, trying next candidate", node.getId());
                tried.add(node.getId());
                lastFailure = GatewayResult.fail(node.getId(), FailureReason.RATE_LIMITED_UPSTREAM,
                        "upstream quota exhausted: " + node.getId());
                continue;
            }

            GatewayResult result = executeOn(node, request);
            if (result.success()) {
                return result;
            }
            tried.add(node.getId());
            lastFailure = result;
            if (!request.idempotent()) {
                log.info("decision: non-idempotent request, retry refused after failure on [{}]",
                        node.getId());
                return result;
            }
            log.info("decision: idempotent request, failing over from [{}], attempt {}/{}",
                    node.getId(), i + 1, attempts);
        }
        return lastFailure != null ? lastFailure
                : GatewayResult.fail(FailureReason.NO_AVAILABLE_NODE, "attempts exhausted");
    }

    /** 无可用节点时区分原因：注册表中仍有权重 > 0 的节点说明是熔断隔离，否则是确实无节点可用。 */
    private FailureReason noEligibleReason() {
        return registry.snapshot().stream().anyMatch(n -> n.getWeight() > 0)
                ? FailureReason.CIRCUIT_OPEN
                : FailureReason.NO_AVAILABLE_NODE;
    }

    private List<UpstreamNode> healthyCandidates(List<String> excluded) {
        return registry.snapshot().stream()
                .filter(n -> !excluded.contains(n.getId()))
                .filter(n -> healthManager.isAvailable(n.getId()))
                .toList();
    }

    private GatewayResult executeOn(UpstreamNode node, RouteRequest request) {
        Future<Long> future = executor.submit(() -> node.execute(request));
        try {
            long seq = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            healthManager.onSuccess(node.getId());
            log.info("decision: hit node [{}], exec seq={}, health=CLOSED", node.getId(), seq);
            return GatewayResult.ok(node.getId(), "executed on " + node.getId() + ", seq=" + seq);
        } catch (TimeoutException e) {
            future.cancel(true);
            healthManager.onFailure(node.getId());
            log.info("decision: node [{}] timed out after {}ms, consecutiveFailures={}",
                    node.getId(), timeoutMillis, healthManager.consecutiveFailuresOf(node.getId()));
            return GatewayResult.fail(node.getId(), FailureReason.TIMEOUT,
                    "timeout after " + timeoutMillis + "ms on " + node.getId());
        } catch (ExecutionException e) {
            healthManager.onFailure(node.getId());
            log.info("decision: node [{}] upstream error: {}, consecutiveFailures={}",
                    node.getId(), e.getCause().getMessage(),
                    healthManager.consecutiveFailuresOf(node.getId()));
            return GatewayResult.fail(node.getId(), FailureReason.UPSTREAM_ERROR,
                    "upstream error on " + node.getId() + ": " + e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return GatewayResult.fail(node.getId(), FailureReason.UPSTREAM_ERROR, "interrupted");
        }
    }

    @Override
    public void close() {
        executor.close();
    }
}
