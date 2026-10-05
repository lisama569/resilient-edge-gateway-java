package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.RequestBudget;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.RouteDecision;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.route.LoadBalancer;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamInvoker;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 网关主流程：
 * <pre>
 *   调用方限流 -> 选节点 -> 上游维度限流 -> 带超时调用
 *      -> 成功：记录健康，返回（含完整尝试链）
 *      -> 失败：记录连续失败/熔断；仅幂等请求转移到下一节点，非幂等请求立即失败
 * </pre>
 */
@Service
public class GatewayService {

    private static final Logger log = LoggerFactory.getLogger(GatewayService.class);

    private final LoadBalancer loadBalancer;
    private final HealthTracker healthTracker;
    private final RateLimiter rateLimiter;
    private final UpstreamInvoker invoker;
    private final com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties props;
    private final ExecutorService invokePool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "upstream-invoke");
        t.setDaemon(true);
        return t;
    });

    public GatewayService(LoadBalancer loadBalancer, HealthTracker healthTracker,
                          RateLimiter rateLimiter, UpstreamInvoker invoker,
                          com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties props) {
        this.loadBalancer = loadBalancer;
        this.healthTracker = healthTracker;
        this.rateLimiter = rateLimiter;
        this.invoker = invoker;
        this.props = props;
    }

    public ProxyResponse proxy(ProxyRequest request) {
        log.info("[gateway] 收到请求 {}", request);

        // 0) 端到端时间预算：每个请求独立计时，不共享状态，并发互不影响
        RequestBudget budget = RequestBudget.of(request);
        if (budget != null) {
            log.info("[gateway] 请求 {} 设置端到端预算 {}ms", request.getRequestId(), budget.budgetMs());
        }

        // 1) 调用方维度限流（独立配额，与上游维度互不影响）
        if (!rateLimiter.tryAcquire(RateLimiter.DIM_CALLER, request.getCaller())) {
            throw new GatewayException(FailureReason.RATE_LIMITED_CALLER,
                    "caller '" + request.getCaller() + "' exceeded caller quota");
        }

        int maxAttempts = request.isIdempotent()
                ? Math.max(1, props.getMaxAttempts()) : 1;
        if (!request.isIdempotent()) {
            log.info("[gateway] 请求 {} 非幂等，最多只尝试 1 个节点，禁止失败转移重试", request.getRequestId());
        }

        List<UpstreamNode> tried = new ArrayList<>();
        List<ProxyResponse.Attempt> attempts = new ArrayList<>();

        for (int i = 1; i <= maxAttempts; i++) {
            // 预算已耗尽：剩余时间不够再试任何节点，立即停止并给出可区分原因
            if (budget != null && budget.isExhausted()) {
                log.warn("[gateway] 请求 {} 预算耗尽（剩余 0ms），在第 {} 次尝试前停止",
                        request.getRequestId(), i);
                throw new GatewayException(FailureReason.BUDGET_EXHAUSTED,
                        "budget exhausted before attempt " + i + " for request " + request);
            }
            RouteDecision decision = loadBalancer.select(
                    request, i == 1 ? null : tried);
            if (decision == null) {
                List<UpstreamNode> snapshot = loadBalancer.currentNodes();
                boolean anyEnabled = snapshot.stream().anyMatch(UpstreamNode::isEnabled);
                if (!anyEnabled) {
                    // 节点被摘除/下线/从未配置
                    throw new GatewayException(FailureReason.NO_AVAILABLE_NODE,
                            "no routable node for request " + request + " at attempt " + i);
                }
                // 有启用节点：若没有任何一个健康，说明全部熔断中；
                // 否则只是候选节点都已在本次请求中尝试过（重试耗尽）
                boolean anyHealthy = snapshot.stream().anyMatch(healthTracker::isAvailable);
                if (!anyHealthy) {
                    throw new GatewayException(FailureReason.ALL_NODES_CIRCUIT_OPEN,
                            "all enabled nodes circuit-open for request " + request);
                }
                // 走循环外的聚合逻辑，给出与最终尝试一致的原因（超时/上游错误/限流）
                break;
            }
            UpstreamNode node = decision.node();

            // 2) 上游节点维度限流（只影响该节点，不影响其他节点配额）
            if (!rateLimiter.tryAcquire(RateLimiter.DIM_UPSTREAM, node.getId())) {
                attempts.add(new ProxyResponse.Attempt(node.getId(),
                        "REJECTED", "RATE_LIMITED_UPSTREAM@" + decision.reason(), 0));
                // 节点维度限流不算节点故障，不熔断；幂等请求可转移到其他节点
                tried.add(node);
                log.warn("[gateway] 节点 {} 上游维度限流，{} 请求 {}",
                        node.getId(),
                        request.isIdempotent() ? "转移" : "终止",
                        request.getRequestId());
                if (!request.isIdempotent()) {
                    throw new GatewayException(FailureReason.RATE_LIMITED_UPSTREAM,
                            "upstream node '" + node.getId() + "' quota exhausted");
                }
                continue;
            }

            // 3) 带超时调用上游：实际等待 = min(节点超时, 剩余预算)
            long start = System.currentTimeMillis();
            if (budget != null) {
                log.info("[gateway] 请求 {} 第 {} 次尝试节点 {}：剩余预算 {}ms，本次最多等待 {}ms",
                        request.getRequestId(), i, node.getId(),
                        budget.remainingMs(), budget.attemptWaitMs(node));
            }
            try {
                ProxyResponse resp = invokeWithTimeout(node, request, budget);
                healthTracker.recordSuccess(node);
                long elapsed = System.currentTimeMillis() - start;
                attempts.add(new ProxyResponse.Attempt(node.getId(),
                        "SUCCESS", decision.reason(), elapsed));
                log.info("[gateway] 请求 {} 最终命中节点 {} 成功（第 {} 次尝试，依据={}，耗时 {}ms{}）",
                        request.getRequestId(), node.getId(), i, decision.reason(), elapsed,
                        budget == null ? "" : "，剩余预算 " + budget.remainingMs() + "ms");
                return new ProxyResponse(resp.getStatus(), resp.getBody(),
                        node.getId(), attempts,
                        budget == null ? null : budget.remainingMs());
            } catch (GatewayException e) {
                long elapsed = System.currentTimeMillis() - start;
                if (e.getReason() == FailureReason.BUDGET_EXHAUSTED) {
                    // 预算掐断：不算节点失败，不计入健康/熔断统计，也不再转移
                    attempts.add(new ProxyResponse.Attempt(node.getId(),
                            "ABORTED", "BUDGET_EXHAUSTED@" + decision.reason(), elapsed));
                    log.warn("[gateway] 请求 {} 在节点 {} 被预算掐断（耗时 {}ms，剩余预算 0ms），"
                                    + "不影响节点健康与熔断统计",
                            request.getRequestId(), node.getId(), elapsed);
                    throw e;
                }
                boolean opened = healthTracker.recordFailure(node);
                String detail = e.getReason() + (opened ? "+CIRCUIT_OPENED" : "");
                attempts.add(new ProxyResponse.Attempt(node.getId(),
                        "FAILED", detail + "@" + decision.reason(), elapsed));
                tried.add(node);
                log.warn("[gateway] 请求 {} 在节点 {} 失败：{}（依据={}，熔断={}）",
                        request.getRequestId(), node.getId(), e.getReason(),
                        decision.reason(), opened);
                if (!request.isIdempotent()) {
                    // 非幂等：绝不重试到第二个节点
                    throw e;
                }
                // 幂等：继续循环转移
            }
        }

        // 聚合最终原因：有限流拒绝 -> 上游限流；全部尝试都是超时 -> 超时；否则为重试耗尽
        boolean allTimeout = !attempts.isEmpty() && attempts.stream()
                .allMatch(a -> a.reason().contains(FailureReason.UPSTREAM_TIMEOUT.name()));
        FailureReason finalReason = attempts.stream()
                .anyMatch(a -> a.outcome().equals("REJECTED"))
                ? FailureReason.RATE_LIMITED_UPSTREAM
                : (allTimeout ? FailureReason.UPSTREAM_TIMEOUT
                        : FailureReason.ALL_ATTEMPTS_FAILED);
        throw new GatewayException(finalReason,
                "all " + attempts.size() + " attempt(s) failed for request " + request);
    }

    private ProxyResponse invokeWithTimeout(UpstreamNode node, ProxyRequest request,
                                            RequestBudget budget) {
        // 实际等待时长取节点超时与剩余预算的较小值；是否在尝试开始前就被预算
        // 收窄必须在等待前判定，避免等待结束后剩余预算自然变小造成误判
        boolean budgetCapped = budget != null && budget.wouldCutByBudget(node);
        long waitMs = budget == null ? node.getTimeoutMs() : budget.attemptWaitMs(node);
        Future<ProxyResponse> future = invokePool.submit(() -> invoker.invoke(node, request));
        try {
            return future.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            if (budgetCapped) {
                // 是调用方预算把这次尝试提前掐断，而非节点自身超时：
                // 不作为节点失败处理，不影响健康与熔断统计
                throw new GatewayException(FailureReason.BUDGET_EXHAUSTED,
                        "budget cut attempt on node " + node.getId() + " after " + waitMs
                                + "ms (node timeout " + node.getTimeoutMs() + "ms)");
            }
            throw new GatewayException(FailureReason.UPSTREAM_TIMEOUT,
                    "node " + node.getId() + " timed out after " + node.getTimeoutMs() + "ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GatewayException(FailureReason.UPSTREAM_TIMEOUT,
                    "interrupted while waiting node " + node.getId());
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof GatewayException ge) {
                throw ge;
            }
            throw new GatewayException(FailureReason.UPSTREAM_ERROR,
                    "node " + node.getId() + " error: " + cause);
        }
    }
}
