package com.github.lisama569.resilientedgegatewayjava.gateway.upstream;

import com.github.lisama569.resilientedgegatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.FailureReason;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyResponse;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 进程内模拟上游集群。不发起任何真实网络调用：
 * 每个节点的行为（成功 / 抛错 / 超时延迟 / 先失败 N 次后恢复）可通过管理面设置，
 * 从而让故障与恢复场景完全确定、可复现。
 *
 * <p>同时记录每个节点被调用次数，并对非幂等请求做"重复执行检测"：
 * 同一个非幂等 requestId 若在任意节点上被执行第二次，计入 duplicateExecutions，
 * 用于在测试中证明失败转移没有重复执行 POST 等请求。
 */
@Component
public class SimulatedUpstreamCluster implements UpstreamInvoker {

    private static final Logger log = LoggerFactory.getLogger(SimulatedUpstreamCluster.class);

    public enum Mode { OK, FAIL, TIMEOUT, FAIL_FIRST_N, SLOW_OK }

    public static final class Behavior {
        final Mode mode;
        final int n;
        final int delayMs;

        private Behavior(Mode mode, int n, int delayMs) {
            this.mode = mode;
            this.n = n;
            this.delayMs = delayMs;
        }

        public static Behavior ok() {
            return new Behavior(Mode.OK, 0, 0);
        }

        public static Behavior fail() {
            return new Behavior(Mode.FAIL, 0, 0);
        }

        /** 睡眠 delayMs，由网关侧按节点 timeout 判定为超时。 */
        public static Behavior timeout(int delayMs) {
            return new Behavior(Mode.TIMEOUT, 0, delayMs);
        }

        /** 延迟 delayMs 后正常返回成功，用于模拟"慢但健康"的节点。 */
        public static Behavior slowOk(int delayMs) {
            return new Behavior(Mode.SLOW_OK, 0, delayMs);
        }

        /** 前 n 次调用失败，之后恢复成功，用于模拟短暂故障后自愈。 */
        public static Behavior failFirstN(int n) {
            return new Behavior(Mode.FAIL_FIRST_N, n, 0);
        }
    }

    private final Map<String, Behavior> behaviors = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> invocationCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> remainingFails = new ConcurrentHashMap<>();
    /** 非幂等请求 id -> 首次执行它的节点。再次出现即说明被重复执行。 */
    private final Map<String, String> executedNonIdempotent = new ConcurrentHashMap<>();
    private final AtomicInteger duplicateExecutions = new AtomicInteger();

    public SimulatedUpstreamCluster() {
    }

    public void setBehavior(String nodeId, Behavior behavior) {
        behaviors.put(nodeId, behavior);
        if (behavior.mode == Mode.FAIL_FIRST_N) {
            remainingFails.put(nodeId, new AtomicInteger(behavior.n));
        } else {
            remainingFails.remove(nodeId);
        }
        log.info("[upstream] 节点 {} 行为设置为 {}", nodeId, behavior.mode);
    }

    public int invocationCountOf(String nodeId) {
        AtomicInteger c = invocationCounts.get(nodeId);
        return c == null ? 0 : c.get();
    }

    public int duplicateExecutionCount() {
        return duplicateExecutions.get();
    }

    public List<String> duplicateRequestIds() {
        // 简化：重复次数通过计数暴露；具体请求 id 由测试日志观察
        return List.of();
    }

    public void reset() {
        behaviors.clear();
        invocationCounts.clear();
        remainingFails.clear();
        executedNonIdempotent.clear();
        duplicateExecutions.set(0);
    }

    @Override
    public ProxyResponse invoke(UpstreamNode node, ProxyRequest request) {
        invocationCounts.computeIfAbsent(node.getId(), k -> new AtomicInteger()).incrementAndGet();

        if (!request.isIdempotent()) {
            String firstNode = executedNonIdempotent.putIfAbsent(request.getRequestId(), node.getId());
            if (firstNode != null) {
                duplicateExecutions.incrementAndGet();
                log.error("[upstream] 非幂等请求 {} 在节点 {} 被重复执行（首次执行节点={}）",
                        request.getRequestId(), node.getId(), firstNode);
            }
        }

        Behavior b = behaviors.getOrDefault(node.getId(), Behavior.ok());
        boolean shouldFail = false;
        if (b.mode == Mode.FAIL) {
            shouldFail = true;
        } else if (b.mode == Mode.FAIL_FIRST_N) {
            AtomicInteger left = remainingFails.get(node.getId());
            shouldFail = left != null && left.getAndDecrement() > 0;
        }

        if (b.mode == Mode.TIMEOUT && b.delayMs > 0) {
            try {
                Thread.sleep(b.delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GatewayException(FailureReason.UPSTREAM_TIMEOUT,
                        "node " + node.getId() + " interrupted while sleeping");
            }
            // 睡眠结束也不返回成功：网关侧通常已先超时；若超时设置更宽则记为上游错误
            throw new GatewayException(FailureReason.UPSTREAM_TIMEOUT,
                    "node " + node.getId() + " simulated timeout after " + b.delayMs + "ms");
        }

        if (b.mode == Mode.SLOW_OK && b.delayMs > 0) {
            try {
                Thread.sleep(b.delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GatewayException(FailureReason.UPSTREAM_TIMEOUT,
                        "node " + node.getId() + " interrupted while sleeping");
            }
            // 睡眠结束后走正常成功路径
        }

        if (shouldFail) {
            log.warn("[upstream] 节点 {} 处理请求 {} 返回上游错误", node.getId(), request);
            throw new GatewayException(FailureReason.UPSTREAM_ERROR,
                    "node " + node.getId() + " returned 500 (simulated)");
        }

        log.info("[upstream] 节点 {} 成功处理请求 {}", node.getId(), request);
        return new ProxyResponse(200,
                "hello from " + node.getId(),
                node.getId(),
                List.of(new ProxyResponse.Attempt(node.getId(), "SUCCESS", "UPSTREAM_OK", 0)));
    }

    /** 测试观察用：当前已经实际执行过非幂等请求的请求 id 集合。 */
    public Set<String> executedNonIdempotentRequestIds() {
        return Set.copyOf(executedNonIdempotent.keySet());
    }
}
