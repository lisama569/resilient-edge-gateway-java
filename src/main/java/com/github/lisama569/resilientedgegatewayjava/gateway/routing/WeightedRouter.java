package com.github.lisama569.resilientedgegatewayjava.gateway.routing;

import com.github.lisama569.resilientedgegatewayjava.gateway.core.RouteRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 加权路由 + 会话粘滞。
 * 采用平滑加权轮询（SWRR）：选择序列只取决于节点权重与调用次序，
 * 不依赖随机数，因此相同输入下路由结果确定、可复现。
 * 权重为 0 的节点不参与分配（视为静默降权，不会被忽略）。
 * 带会话标识的请求在绑定节点仍存在于候选集时保持稳定落点；
 * 绑定节点被摘除或权重归零后自动重绑到新节点。
 */
public class WeightedRouter {

    /** sessionId -> nodeId 的粘滞绑定。 */
    private final Map<String, String> stickyBindings = new ConcurrentHashMap<>();
    /** nodeId -> SWRR 当前权重累计值。 */
    private final Map<String, Long> currentWeights = new ConcurrentHashMap<>();

    /**
     * 在候选节点中选择一个节点。
     * 候选集应由调用方过滤为"健康且权重 > 0"的节点。
     */
    public Optional<UpstreamNode> route(RouteRequest request, List<UpstreamNode> candidates) {
        List<UpstreamNode> eligible = candidates.stream()
                .filter(n -> n.getWeight() > 0)
                .toList();
        if (eligible.isEmpty()) {
            return Optional.empty();
        }

        String sessionId = request.sessionId();
        if (sessionId != null && !sessionId.isEmpty()) {
            String bound = stickyBindings.get(sessionId);
            if (bound != null) {
                Optional<UpstreamNode> boundNode = eligible.stream()
                        .filter(n -> n.getId().equals(bound))
                        .findFirst();
                if (boundNode.isPresent()) {
                    return boundNode;
                }
                // 绑定节点已不可用：解除旧绑定，按权重重新选择并重绑
                stickyBindings.remove(sessionId, bound);
            }
            Optional<UpstreamNode> selected = selectByWeight(eligible);
            selected.ifPresent(n -> stickyBindings.put(sessionId, n.getId()));
            return selected;
        }
        return selectByWeight(eligible);
    }

    /** 平滑加权轮询：每轮各节点 current += weight，取最大者后 current -= total。 */
    private Optional<UpstreamNode> selectByWeight(List<UpstreamNode> eligible) {
        synchronized (currentWeights) {
            long total = eligible.stream().mapToLong(UpstreamNode::getWeight).sum();
            UpstreamNode best = null;
            long bestCurrent = Long.MIN_VALUE;
            for (UpstreamNode node : eligible) {
                long current = currentWeights.merge(node.getId(), (long) node.getWeight(), Long::sum);
                if (current > bestCurrent) {
                    bestCurrent = current;
                    best = node;
                }
            }
            if (best == null) {
                return Optional.empty();
            }
            currentWeights.merge(best.getId(), -total, Long::sum);
            return Optional.of(best);
        }
    }

    public void unbindSession(String sessionId) {
        stickyBindings.remove(sessionId);
    }

    /** 节点被摘除时清理其路由状态，避免重新加入后沿用旧累计值。 */
    public void evictNode(String nodeId) {
        currentWeights.remove(nodeId);
        stickyBindings.values().removeIf(nodeId::equals);
    }

    /** 仅供测试观测。 */
    public Optional<String> bindingOf(String sessionId) {
        return Optional.ofNullable(stickyBindings.get(sessionId));
    }
}
