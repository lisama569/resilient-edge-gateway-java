package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.RouteDecision;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.TimeBudget;
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
        // 端到端时间预算：从收到请求这一刻开始计时，后续重试/转移都从这同一个预算里扣。
        // 预算对象是请求级局部变量，并发请求之间没有任何共享状态。
        TimeBudget budget = TimeBudget.of(request.getBudgetMs());
        log.info("[gateway] 收到请求 {} 预算={}", request,
                budget.hasBudget() ? budget.remainingMs() + "ms" : "无");

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
            // 剩余预算已经不够再发起一次尝试：立即停止，不硬撑到节点超时之后才回结果
            if (budget.isExhausted()) {
                log.warn("[gateway] 请求 {} 预算耗尽（已耗时 {}ms），停止继续尝试，已尝试 {} 次",
                        request.getRequestId(), budget.elapsedMs(), attempts.size());
                throw new GatewayException(FailureReason.BUDGET_EXHAUSTED,
                        "budget exhausted after " + budget.elapsedMs() + "ms, "
                                + attempts.size() + " attempt(s) made for request " + request);
            }
            Long remainingAtStart = budget.hasBudget() ? budget.remainingMs() : null;

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
                        "REJECTED", "RATE_LIMITED_UPSTREAM@" + decision.reason(), 0,
                        remainingAtStart));
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

            // 3) 带超时调用上游：实际等待上限 = min(节点自身超时, 当前剩余预算)
            long attemptTimeoutMs = budget.attemptTimeoutMs(node.getTimeoutMs());
            boolean budgetTighter = attemptTimeoutMs < node.getTimeoutMs();
            if (budgetTighter) {
                log.info("[gateway] 请求 {} 剩余预算 {}ms 小于节点 {} 超时 {}ms，本次尝试按预算掐表",
                        request.getRequestId(), remainingAtStart, node.getId(), node.getTimeoutMs());
            }
            long start = System.currentTimeMillis();
            try {
                ProxyResponse resp = invokeWithTimeout(node, request, attemptTimeoutMs, budgetTighter);
                healthTracker.recordSuccess(node);
                long elapsed = System.currentTimeMillis() - start;
                attempts.add(new ProxyResponse.Attempt(node.getId(),
                        "SUCCESS", decision.reason(), elapsed, remainingAtStart));
                log.info("[gateway] 请求 {} 最终命中节点 {} 成功（第 {} 次尝试，依据={}，剩余预算={}）",
                        request.getRequestId(), node.getId(), i, decision.reason(),
                        budget.hasBudget() ? budget.remainingMs() + "ms" : "无");
                return new ProxyResponse(resp.getStatus(), resp.getBody(),
                        node.getId(), attempts);
            } catch (GatewayException e) {
                long elapsed = System.currentTimeMillis() - start;
                if (e.getReason() == FailureReason.BUDGET_EXHAUSTED) {
                    // 是调用方预算把这次尝试提前掐断的：不算节点失败，
                    // 不累计连续失败、不影响熔断统计，且预算已尽，直接结束
                    attempts.add(new ProxyResponse.Attempt(node.getId(),
                            "ABORTED", "BUDGET_EXHAUSTED@" + decision.reason(),
                            elapsed, remainingAtStart));
                    log.warn("[gateway] 请求 {} 在节点 {} 的尝试被预算掐断（等了 {}ms，节点超时 {}ms），"
                                    + "不计节点失败，停止重试",
                            request.getRequestId(), node.getId(), elapsed, node.getTimeoutMs());
                    throw e;
                }
                boolean opened = healthTracker.recordFailure(node);
                String detail = e.getReason() + (opened ? "+CIRCUIT_OPENED" : "");
                attempts.add(new ProxyResponse.Attempt(node.getId(),
                        "FAILED", detail + "@" + decision.reason(), elapsed, remainingAtStart));
                tried.add(node);
                log.warn("[gateway] 请求 {} 在节点 {} 失败：{}（依据={}，熔断={}，剩余预算={}）",
                        request.getRequestId(), node.getId(), e.getReason(),
                        decision.reason(), opened,
                        budget.hasBudget() ? budget.remainingMs() + "ms" : "无");
                if (!request.isIdempotent()) {
                    // 非幂等：绝不重试到第二个节点
                    throw e;
                }
                // 幂等：继续循环转移（下一轮先检查剩余预算是否还够）
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

    /**
     * @param attemptTimeoutMs 本次尝试实际允许等待的上限（已取节点超时与剩余预算的较小值）
     * @param budgetTighter    true 表示该上限来自调用方预算而非节点自身超时；
     *                         此时超时属于"预算掐断"，返回 BUDGET_EXHAUSTED 且不算节点失败
     */
    private ProxyResponse invokeWithTimeout(UpstreamNode node, ProxyRequest request,
                                            long attemptTimeoutMs, boolean budgetTighter) {
        Future<ProxyResponse> future = invokePool.submit(() -> invoker.invoke(node, request));
        try {
            return future.get(attemptTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            if (budgetTighter) {
                throw new GatewayException(FailureReason.BUDGET_EXHAUSTED,
                        "budget cut attempt on node " + node.getId() + " after "
                                + attemptTimeoutMs + "ms (node timeout "
                                + node.getTimeoutMs() + "ms)");
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
