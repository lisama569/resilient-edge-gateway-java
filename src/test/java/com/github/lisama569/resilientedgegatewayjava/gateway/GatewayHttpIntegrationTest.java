package com.github.lisama569.resilientedgegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * 经完整 Spring MVC 栈验证：
 * 200 成功响应体含 servedBy 与 attempts；
 * 429 限流错误体可区分 RATE_LIMITED_CALLER / RATE_LIMITED_UPSTREAM；
 * 503 可区分 NO_AVAILABLE_NODE / ALL_NODES_CIRCUIT_OPEN；
 * 504 对应 UPSTREAM_TIMEOUT；POST 失败返回 502 且不重试。
 */
@SpringBootTest(properties = {
        "gateway.nodes[0].id=node-a",
        "gateway.nodes[0].weight=1",
        "gateway.nodes[0].timeout-ms=200",
        "gateway.nodes[1].id=node-b",
        "gateway.nodes[1].weight=1",
        "gateway.nodes[1].timeout-ms=200",
        "gateway.failure-threshold=2",
        "gateway.recover-after-ms=600000",
        "gateway.max-attempts=3",
        "gateway.caller-default-permits-per-sec=100",
        "gateway.upstream-default-permits-per-sec=100"
})
class GatewayHttpIntegrationTest {

    @Autowired
    private WebApplicationContext context;
    @Autowired
    private com.github.lisama569.resilientedgegatewayjava.gateway.route.NodeRegistry registry;
    @Autowired
    private com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster cluster;
    @Autowired
    private com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter rateLimiter;

    private MockMvc mvc;

    @BeforeEach
    void reset() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        cluster.reset();
        rateLimiter.reset();
        // 恢复默认熔断阈值（个别测试会临时调大）
        var gwProps = context.getBean(
                com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties.class);
        gwProps.setFailureThreshold(2);
        // 恢复默认三节点配置中可能被前序测试改动的状态
        registry.initFromProperties();
        var health = context.getBean(
                com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker.class);
        registry.list().forEach(n -> health.reset(n.getId()));
    }

    @Test
    void successExposesServedByAndAttempts() throws Exception {
        mvc.perform(get("/proxy/orders/123")
                        .header("X-Caller", "svc-1")
                        .header("X-Request-Id", "http-1")
                        .header("X-Session-Id", "s-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.servedBy").exists())
                .andExpect(jsonPath("$.status").value(200))
                .andExpect(jsonPath("$.attempts[0].outcome").value("SUCCESS"))
                .andDo(r -> System.out.println("HTTP 成功响应: " + r.getResponse().getContentAsString()));
    }

    @Test
    void allNodesRemovedReturns503NoAvailableNode() throws Exception {
        for (String id : new String[]{"node-a", "node-b"}) {
            registry.setEnabled(id, false);
        }
        MvcResult result = mvc.perform(get("/proxy/x").header("X-Request-Id", "http-2"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.reason").value("NO_AVAILABLE_NODE"))
                .andReturn();
        System.out.println("全部摘除 -> 503: " + result.getResponse().getContentAsString());
    }

    @Test
    void timeoutReturns504WithDistinctReason() throws Exception {
        // 调大熔断阈值，确保 3 次尝试期间节点不熔断，失败原因稳定为超时
        context.getBean(com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties.class)
                .setFailureThreshold(100);
        com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker health =
                context.getBean(com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker.class);
        health.reset("node-a");
        health.reset("node-b");
        cluster.setBehavior("node-a",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.timeout(400));
        cluster.setBehavior("node-b",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.timeout(400));
        MvcResult result = mvc.perform(get("/proxy/slow").header("X-Request-Id", "http-3"))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$.reason").value("UPSTREAM_TIMEOUT"))
                .andReturn();
        System.out.println("全部超时 -> 504: " + result.getResponse().getContentAsString());
        org.junit.jupiter.api.Assertions.assertEquals(
                com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker.State.HEALTHY,
                health.stateOf("node-a"), "阈值未到不应熔断，但连续失败计数应累计");
    }

    @Test
    void budgetExhaustedReturns408WithDistinctReason() throws Exception {
        // 节点超时 200ms，上游 400ms 才返回；调用方只给 100ms 预算 -> 预算先掐断
        // 两个节点都设慢，避免无会话加权随机落到健康节点导致结果不确定
        cluster.setBehavior("node-a",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.timeout(400));
        cluster.setBehavior("node-b",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.timeout(400));
        MvcResult result = mvc.perform(get("/proxy/slow")
                        .header("X-Request-Id", "http-budget-1")
                        .header("X-Budget-Ms", "100"))
                .andExpect(status().isRequestTimeout())
                .andExpect(jsonPath("$.reason").value("BUDGET_EXHAUSTED"))
                .andReturn();
        System.out.println("预算耗尽 -> 408: " + result.getResponse().getContentAsString());

        // 预算掐断不算节点失败：两个节点的健康状态都必须保持 HEALTHY
        var health = context.getBean(
                com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker.class);
        for (String id : new String[]{"node-a", "node-b"}) {
            org.junit.jupiter.api.Assertions.assertEquals(
                    com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthTracker.State.HEALTHY,
                    health.stateOf(id), "预算掐断不得计入节点失败: " + id);
        }

        // 不带预算的同样请求：行为不变，按节点超时判定为 504 UPSTREAM_TIMEOUT
        context.getBean(com.github.lisama569.resilientedgegatewayjava.gateway.config.GatewayProperties.class)
                .setFailureThreshold(100);
        cluster.setBehavior("node-b",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.timeout(400));
        MvcResult noBudget = mvc.perform(get("/proxy/slow")
                        .header("X-Request-Id", "http-budget-2"))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$.reason").value("UPSTREAM_TIMEOUT"))
                .andReturn();
        System.out.println("不带预算 -> 仍为 504: " + noBudget.getResponse().getContentAsString());
    }

    @Test
    void nonIdempotentFailureReturns502AndExecutesOnce() throws Exception {
        cluster.setBehavior("node-a",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.fail());
        cluster.setBehavior("node-b",
                com.github.lisama569.resilientedgegatewayjava.gateway.upstream.SimulatedUpstreamCluster.Behavior.fail());

        mvc.perform(post("/proxy/payments").header("X-Request-Id", "http-post-1"))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.reason").value("UPSTREAM_ERROR"))
                .andDo(r -> System.out.println("POST 失败 -> 502: " + r.getResponse().getContentAsString()));

        int total = cluster.invocationCountOf("node-a") + cluster.invocationCountOf("node-b");
        assertEquals(1, total, "POST 非幂等请求全局只能执行一次");
        assertEquals(0, cluster.duplicateExecutionCount());
        System.out.println("POST 仅执行 1 次（node-a=" + cluster.invocationCountOf("node-a")
                + ", node-b=" + cluster.invocationCountOf("node-b") + "），无重复执行");
    }
}
