package com.github.lisama569.resilientedgegatewayjava.gateway.upstream;

import com.github.lisama569.resilientedgegatewayjava.gateway.core.RouteRequest;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内模拟上游节点：可控延迟、故障注入、非幂等执行计数。
 * 所有状态均为线程安全，便于并发场景下得出可复现结论。
 */
public class UpstreamNode {

    private final String id;
    private final AtomicInteger weight;
    private final AtomicLong executionCount = new AtomicLong();
    /** 故障注入开关：打开后 execute 直接抛异常。 */
    private final AtomicBoolean failing = new AtomicBoolean(false);
    /** 模拟处理延迟（毫秒）。 */
    private volatile long latencyMillis = 0;

    public UpstreamNode(String id, int weight) {
        if (weight < 0) {
            throw new IllegalArgumentException("weight must be >= 0");
        }
        this.id = id;
        this.weight = new AtomicInteger(weight);
    }

    public String getId() {
        return id;
    }

    public int getWeight() {
        return weight.get();
    }

    public void setWeight(int weight) {
        if (weight < 0) {
            throw new IllegalArgumentException("weight must be >= 0");
        }
        this.weight.set(weight);
    }

    public void setFailing(boolean failing) {
        this.failing.set(failing);
    }

    public boolean isFailing() {
        return failing.get();
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    /**
     * 模拟执行一次请求，返回该节点的累计执行序号（从 1 开始）。
     * 计数先于模拟延迟递增，用于验证非幂等请求是否被重复执行。
     */
    public long execute(RouteRequest request) throws InterruptedException {
        long seq = executionCount.incrementAndGet();
        if (failing.get()) {
            throw new SimulatedUpstreamException("node " + id + " is injected-failing");
        }
        long latency = latencyMillis;
        if (latency > 0) {
            Thread.sleep(latency);
        }
        return seq;
    }

    public long getExecutionCount() {
        return executionCount.get();
    }

    /** 模拟上游异常类型，便于与超时等其他失败区分。 */
    public static class SimulatedUpstreamException extends RuntimeException {
        public SimulatedUpstreamException(String message) {
            super(message);
        }
    }
}
