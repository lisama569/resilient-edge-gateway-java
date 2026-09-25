package com.github.lisama569.resilientedgegatewayjava.gateway.model;

/**
 * 一次路由选择的结论：命中的节点以及选择依据。
 * 判定依据必须可区分：
 * <ul>
 *   <li>{@code STICKY_SESSION}：会话已绑定到健康节点，保持原落点；</li>
 *   <li>{@code STICKY_REBOUND}：原绑定节点不可用，会话重新绑定到新节点；</li>
 *   <li>{@code WEIGHTED}：无会话或新会话，按权重区间加权抽取；</li>
 *   <li>{@code RETRY_FALLBACK}：上一次尝试失败，转移到下一个可用节点。</li>
 * </ul>
 */
public record RouteDecision(UpstreamNode node, String reason) {

    public static final String STICKY_SESSION = "STICKY_SESSION";
    public static final String STICKY_REBOUND = "STICKY_REBOUND";
    public static final String WEIGHTED = "WEIGHTED";
    public static final String RETRY_FALLBACK = "RETRY_FALLBACK";

    public RouteDecision {
        if (node == null) {
            throw new IllegalArgumentException("routed node must not be null");
        }
        if (reason == null) {
            throw new IllegalArgumentException("route reason must not be null");
        }
    }
}
