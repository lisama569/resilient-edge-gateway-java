package com.github.lisama569.resilientedgegatewayjava.gateway;

import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class HealthManagerTest {

    @Test
    void consecutiveFailuresOpenCircuitThenCooldownAllowsProbe() {
        AtomicLong now = new AtomicLong(0);
        HealthManager hm = new HealthManager(3, 1000, now::get);

        assertTrue(hm.isAvailable("n1"));
        hm.onFailure("n1");
        hm.onFailure("n1");
        assertTrue(hm.isAvailable("n1"), "未达阈值仍应可用");
        hm.onFailure("n1");
        assertEquals(HealthManager.State.OPEN, hm.stateOf("n1"));
        assertFalse(hm.isAvailable("n1"), "熔断期间必须被隔离");

        now.addAndGet(999);
        assertFalse(hm.isAvailable("n1"), "冷却期未结束不得放行");
        now.addAndGet(1);
        assertTrue(hm.isAvailable("n1"), "冷却期结束应放行探测");
        assertEquals(HealthManager.State.HALF_OPEN, hm.stateOf("n1"));
    }

    @Test
    void successInHalfOpenClosesCircuitAndResetsCounter() {
        AtomicLong now = new AtomicLong(0);
        HealthManager hm = new HealthManager(2, 500, now::get);
        hm.onFailure("n1");
        hm.onFailure("n1");
        assertFalse(hm.isAvailable("n1"));

        now.addAndGet(500);
        assertTrue(hm.isAvailable("n1"));
        hm.onSuccess("n1");
        assertEquals(HealthManager.State.CLOSED, hm.stateOf("n1"));
        assertEquals(0, hm.consecutiveFailuresOf("n1"));
        assertTrue(hm.isAvailable("n1"), "恢复后应重新参与分配");
    }

    @Test
    void failureInHalfOpenReopensImmediately() {
        AtomicLong now = new AtomicLong(0);
        HealthManager hm = new HealthManager(5, 500, now::get);
        hm.onFailure("n1"); // 半开状态下的一次失败即重新熔断
        now.addAndGet(500);
        // 先制造 OPEN：连续 5 次失败
        for (int i = 0; i < 4; i++) {
            hm.onFailure("n1");
        }
        assertEquals(HealthManager.State.OPEN, hm.stateOf("n1"));
        now.addAndGet(500);
        assertTrue(hm.isAvailable("n1")); // 进入 HALF_OPEN
        hm.onFailure("n1");
        assertEquals(HealthManager.State.OPEN, hm.stateOf("n1"), "探测失败应立即重新熔断");
        assertFalse(hm.isAvailable("n1"));
    }

    @Test
    void successResetsConsecutiveFailureCount() {
        HealthManager hm = new HealthManager(3, 1000);
        hm.onFailure("n1");
        hm.onFailure("n1");
        hm.onSuccess("n1");
        hm.onFailure("n1");
        hm.onFailure("n1");
        assertTrue(hm.isAvailable("n1"), "计数被重置后不应误触发熔断");
    }
}
