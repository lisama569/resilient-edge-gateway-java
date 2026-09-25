package com.github.lisama569.resilientedgegatewayjava.gateway.route;

import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.ProxyRequest;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.RouteDecision;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 负载均衡与会话粘滞。
 *
 * <p>规则：
 * <ol>
 *   <li>只在 enabled 且健康（未熔断）的节点中选择；摘除、下线、熔断的节点不接收新流量。</li>
 *   <li>带 sessionId 的请求优先落在已绑定节点；绑定节点不可用时重新绑定（粘滞只在健康期生效）。</li>
 *   <li>无绑定/无会话时按节点权重做加权区间抽取，权重不会被静默忽略。</li>
 *   <li>失败转移（exclusions 非空）时排除已尝试节点，在其余健康节点中继续加权选择。</li>
 * </ol>
 */
@Component
public class LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(LoadBalancer.class);

    private final HealthTracker healthTracker;

    /** 当前生效的节点表，整体替换以保证调权/摘除的原子可见性。 */
    private volatile List<UpstreamNode> nodes = Collections.emptyList();
    /** sessionId -> 绑定节点 id。节点被摘除/熔断后重新绑定。 */
    private final Map<String, String> sessionBindings = new ConcurrentHashMap<>();
    /**
     * 测试开关：true 时按节点表顺序选择第一个可用节点，使故障转移结论完全确定。
     * 生产默认 false，走加权随机。
     */
    private volatile boolean deterministicFirst = false;

    /** 仅测试使用：启用确定性（顺序）选择。 */
    public void setDeterministicFirst(boolean value) {
        this.deterministicFirst = value;
    }

    public LoadBalancer(HealthTracker healthTracker) {
        this.healthTracker = healthTracker;
    }

    /** 管理面/配置加载时整体替换节点表（调权、摘除、下线均走这里）。 */
    public void updateNodes(List<UpstreamNode> newNodes) {
        List<UpstreamNode> snapshot = List.copyOf(newNodes);
        this.nodes = snapshot;
        Set<String> alive = new HashSet<>();
        for (UpstreamNode n : snapshot) {
            alive.add(n.getId());
        }
        // 绑定到已删除节点的会话立即失效，新流量不会再落到不存在的节点
        sessionBindings.values().retainAll(alive);
        healthTracker.retainOnly(alive);
        log.info("[route] 节点表更新: {}", snapshot);
    }

    public List<UpstreamNode> currentNodes() {
        return nodes;
    }

    public String bindingOf(String sessionId) {
        return sessionId == null ? null : sessionBindings.get(sessionId);
    }

    /**
     * 选择上游节点。
     *
     * @param exclusions 本次请求已经尝试过、需要排除的节点（失败转移用）
     * @return 路由结论；无健康可用节点时返回 null（由上层给出可区分原因）
     */
    public RouteDecision select(ProxyRequest request, List<UpstreamNode> exclusions) {
        List<UpstreamNode> snapshot = nodes;
        Set<String> excludedIds = new HashSet<>();
        if (exclusions != null) {
            for (UpstreamNode n : exclusions) {
                excludedIds.add(n.getId());
            }
        }

        List<UpstreamNode> eligible = new ArrayList<>();
        for (UpstreamNode n : snapshot) {
            if (!excludedIds.contains(n.getId()) && healthTracker.isAvailable(n)) {
                eligible.add(n);
            }
        }
        if (eligible.isEmpty()) {
            log.info("[route] 请求 {} 无可用节点（排除={}）", request, excludedIds);
            return null;
        }

        // 1) 会话粘滞：绑定节点仍在健康集合中则保持落点
        String sessionId = request.getSessionId();
        if (sessionId != null && exclusions == null) {
            String boundId = sessionBindings.get(sessionId);
            if (boundId != null) {
                for (UpstreamNode n : eligible) {
                    if (n.getId().equals(boundId)) {
                        log.info("[route] 请求 {} 命中粘滞节点 {}（依据=STICKY_SESSION）", request, boundId);
                        return new RouteDecision(n, RouteDecision.STICKY_SESSION);
                    }
                }
            }
        }

        // 2) 加权区间抽取（或测试模式下顺序选取第一个）
        UpstreamNode chosen = deterministicFirst
                ? eligible.get(0) : weightedPick(eligible);

        // 3) 记录/迁移会话绑定
        String reason;
        if (sessionId != null && exclusions == null) {
            String previous = sessionBindings.put(sessionId, chosen.getId());
            reason = (previous != null && !previous.equals(chosen.getId()))
                    ? RouteDecision.STICKY_REBOUND : RouteDecision.WEIGHTED;
        } else {
            reason = (exclusions != null && !exclusions.isEmpty())
                    ? RouteDecision.RETRY_FALLBACK : RouteDecision.WEIGHTED;
        }
        log.info("[route] 请求 {} 命中节点 {}（依据={}，候选权重={}）",
                request, chosen.getId(), reason, weightView(eligible));
        return new RouteDecision(chosen, reason);
    }

    private static UpstreamNode weightedPick(List<UpstreamNode> eligible) {
        long total = 0;
        for (UpstreamNode n : eligible) {
            total += n.getWeight();
        }
        long r = ThreadLocalRandom.current().nextLong(total);
        long acc = 0;
        for (UpstreamNode n : eligible) {
            acc += n.getWeight();
            if (r < acc) {
                return n;
            }
        }
        return eligible.get(eligible.size() - 1);
    }

    private static String weightView(List<UpstreamNode> eligible) {
        StringBuilder sb = new StringBuilder();
        for (UpstreamNode n : eligible) {
            if (!sb.isEmpty()) {
                sb.append(',');
            }
            sb.append(n.getId()).append('=').append(n.getWeight());
        }
        return sb.toString();
    }
}
