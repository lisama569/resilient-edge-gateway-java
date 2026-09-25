package com.github.lisama569.resilientedgegatewayjava.gateway.core;

import com.github.lisama569.resilientedgegatewayjava.gateway.health.HealthManager;
import com.github.lisama569.resilientedgegatewayjava.gateway.ratelimit.RateLimiter;
import com.github.lisama569.resilientedgegatewayjava.gateway.routing.WeightedRouter;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamNode;
import com.github.lisama569.resilientedgegatewayjava.gateway.upstream.UpstreamRegistry;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/** 网关装配：全部组件进程内构造，不依赖外部服务。 */
@Configuration
public class GatewayConfig {

    @Bean
    @ConfigurationProperties(prefix = "gateway")
    public GatewayProperties gatewayProperties() {
        return new GatewayProperties();
    }

    @Bean
    public UpstreamRegistry upstreamRegistry(GatewayProperties props) {
        UpstreamRegistry registry = new UpstreamRegistry();
        for (NodeSpec spec : props.getNodes()) {
            registry.addNode(new UpstreamNode(spec.getId(), spec.getWeight()));
        }
        return registry;
    }

    @Bean
    public HealthManager healthManager(GatewayProperties props) {
        return new HealthManager(props.getFailureThreshold(), props.getOpenCooldownMillis());
    }

    @Bean
    public RateLimiter rateLimiter(GatewayProperties props) {
        return new RateLimiter(props.getCallerCapacity(), props.getCallerRefillPerSecond(),
                props.getUpstreamCapacity(), props.getUpstreamRefillPerSecond());
    }

    @Bean
    public WeightedRouter weightedRouter() {
        return new WeightedRouter();
    }

    @Bean
    public GatewayService gatewayService(UpstreamRegistry registry, HealthManager healthManager,
                                         RateLimiter rateLimiter, WeightedRouter router,
                                         GatewayProperties props) {
        return new GatewayService(registry, healthManager, rateLimiter, router,
                props.getTimeoutMillis(), props.getMaxAttempts());
    }

    /** 可调配置，默认值保证本地开箱即可验证。 */
    public static class GatewayProperties {
        private long timeoutMillis = 500;
        private int maxAttempts = 3;
        private int failureThreshold = 3;
        private long openCooldownMillis = 5_000;
        private long callerCapacity = 50;
        private double callerRefillPerSecond = 10;
        private long upstreamCapacity = 100;
        private double upstreamRefillPerSecond = 50;
        private List<NodeSpec> nodes = List.of(
                new NodeSpec("node-a", 5),
                new NodeSpec("node-b", 3),
                new NodeSpec("node-c", 2));

        public long getTimeoutMillis() { return timeoutMillis; }
        public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        public int getFailureThreshold() { return failureThreshold; }
        public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }
        public long getOpenCooldownMillis() { return openCooldownMillis; }
        public void setOpenCooldownMillis(long v) { this.openCooldownMillis = v; }
        public long getCallerCapacity() { return callerCapacity; }
        public void setCallerCapacity(long callerCapacity) { this.callerCapacity = callerCapacity; }
        public double getCallerRefillPerSecond() { return callerRefillPerSecond; }
        public void setCallerRefillPerSecond(double v) { this.callerRefillPerSecond = v; }
        public long getUpstreamCapacity() { return upstreamCapacity; }
        public void setUpstreamCapacity(long upstreamCapacity) { this.upstreamCapacity = upstreamCapacity; }
        public double getUpstreamRefillPerSecond() { return upstreamRefillPerSecond; }
        public void setUpstreamRefillPerSecond(double v) { this.upstreamRefillPerSecond = v; }
        public List<NodeSpec> getNodes() { return nodes; }
        public void setNodes(List<NodeSpec> nodes) { this.nodes = nodes; }
    }

    public static class NodeSpec {
        private String id;
        private int weight;

        public NodeSpec() {
        }

        public NodeSpec(String id, int weight) {
            this.id = id;
            this.weight = weight;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public int getWeight() { return weight; }
        public void setWeight(int weight) { this.weight = weight; }
    }
}
