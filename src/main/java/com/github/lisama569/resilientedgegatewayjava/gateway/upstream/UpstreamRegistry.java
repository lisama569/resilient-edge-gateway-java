package com.github.lisama569.resilientedgegatewayjava.gateway.upstream;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 上游节点注册表：增删节点、调整权重。
 * 读路径返回不可变快照，路由过程中不受并发增删影响。
 */
public class UpstreamRegistry {

    private final ConcurrentMap<String, UpstreamNode> nodes = new ConcurrentHashMap<>();

    public void addNode(UpstreamNode node) {
        nodes.put(node.getId(), node);
    }

    public UpstreamNode removeNode(String nodeId) {
        return nodes.remove(nodeId);
    }

    public void updateWeight(String nodeId, int weight) {
        UpstreamNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("unknown node: " + nodeId);
        }
        node.setWeight(weight);
    }

    public UpstreamNode get(String nodeId) {
        return nodes.get(nodeId);
    }

    /** 当前节点的不可变快照（按 id 排序，保证路由结果可复现）。 */
    public List<UpstreamNode> snapshot() {
        return nodes.values().stream()
                .sorted((a, b) -> a.getId().compareTo(b.getId()))
                .toList();
    }
}
