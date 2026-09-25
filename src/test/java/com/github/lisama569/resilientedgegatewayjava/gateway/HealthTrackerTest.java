package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties;
import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker;
import com.github.lisama569.resilientedgegatewayjava.gateway.model.UpstreamNode;
import org.junit.jupiter.api.Test;

class HealthTrackerTest {

    @Test
    void opensOnConsecutiveFailuresAndRecoversAfterProbe() throws InterruptedException {
        GatewayProperties props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setRecoverAfterMs(100);
        HealthTracker health = new HealthTracker(props);
        UpstreamNode node = new UpstreamNode("node-a", 1, true, 200);

        // 单次失败未达阈值，仍可用
        assertFalse(health.recordFailure(node));
        assertTrue(health.isAvailable(node), "未达失败阈值不应隔离");
        System.out.println("第 1 次失败：未达阈值，node-a 仍可接收流量");

        // 连续第 2 次失败 -> 熔断
        assertTrue(health.recordFailure(node));
        assertFalse(health.isAvailable(node), "熔断打开期间必须快速隔离");
        System.out.println("第 2 次失败：node-a 熔断隔离，新流量不可进入");

        // 冷却期未到仍隔离
        assertFalse(health.isAvailable(node));

        // 冷却结束 -> 半开放行一次试探
        Thread.sleep(120);
        assertTrue(health.isAvailable(node), "冷却后应允许半开试探");
        System.out.println("冷却 100ms 后：node-a 进入 HALF_OPEN，放行一次试探");

        // 试探成功 -> 恢复
        health.recordSuccess(node);
        assertTrue(health.isAvailable(node));
        System.out.println("试探成功：node-a 恢复 HEALTHY，重新参与分配");
    }

    @Test
    void halfOpenFailureReopensCircuit() throws InterruptedException {
        GatewayProperties props = new GatewayProperties();
        props.setFailureThreshold(2);
        props.setRecoverAfterMs(80);
        HealthTracker health = new HealthTracker(props);
        UpstreamNode node = new UpstreamNode("node-a", 1, true, 200);

        health.recordFailure(node);
        health.recordFailure(node);
        assertFalse(health.isAvailable(node));
        Thread.sleep(90);
        assertTrue(health.isAvailable(node)); // 半开
        assertTrue(health.recordFailure(node), "半开期间失败必须立即重新熔断");
        assertFalse(health.isAvailable(node), "重新熔断期间继续隔离");
        System.out.println("半开试探失败 -> 立即重新熔断隔离");
    }
}
