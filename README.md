# Resilient Edge Gateway (Java)

在现有 Spring Boot 骨架内实现的**本地反向代理网关**。上游为进程内可控节点
（`UpstreamNode`），不依赖任何外部服务或真实凭据，路由、限流、故障转移的
全部行为均可在本地确定性地复现与验证。

## 组件与请求处理顺序

```
GatewayService.handle(request)
  1. 调用方维度限流 (RateLimiter.tryAcquireCaller)   —— 超限即拒，不消耗上游配额
  2. 健康节点候选过滤 (HealthManager.isAvailable)
  3. 加权路由 + 会话粘滞 (WeightedRouter.route)
  4. 上游节点维度限流 (RateLimiter.tryAcquireUpstream)
  5. 带超时执行 (虚拟线程 + Future.get(timeout))
  6. 结果回写健康状态；幂等请求在其余健康节点间转移，非幂等请求绝不重试
```

## 路由与权重规则

- **平滑加权轮询（SWRR）**：选择序列只取决于节点权重与调用次序，不依赖随机数，
  相同输入下路由结果完全确定、可复现。例如权重 5:3:2 的节点在 100 次调用中
  精确命中 50/30/20 次（见 `WeightedRouterTest`）。
- **权重为 0** 的节点不参与分配（显式降权，不会被静默忽略）。
- **会话粘滞**：携带 `sessionId` 的请求在绑定节点健康且在候选集内时保持稳定落点；
  绑定节点被摘除、权重归零或熔断后，自动按权重重绑到新节点并保持后续稳定。
- **节点摘除**（`DELETE /gateway/nodes/{id}`）会同时清理路由累计权重、会话绑定
  与健康状态，新流量不再进入该节点。

## 健康判定与故障转移

- 熔断器语义：`CLOSED → (连续失败 ≥ failureThreshold) → OPEN →
  (冷却 openCooldownMillis) → HALF_OPEN → (探测成功) → CLOSED`；
  探测失败立即重新 `OPEN`。
- 超时与上游异常都计为失败；`OPEN` 期间节点被快速隔离，请求快速失败。
- **幂等请求**：失败后在其余健康节点间转移，最多 `maxAttempts` 次。
- **非幂等请求**：只执行一次，失败后绝不转移到其它节点重复执行
  （由 `GatewayServiceTest.nonIdempotentRequestIsNeverExecutedTwice` 验证执行计数 = 1）。

### 可区分的失败原因（`FailureReason`）

| 原因 | 含义 |
|---|---|
| `RATE_LIMITED_CALLER` | 调用方配额耗尽 |
| `RATE_LIMITED_UPSTREAM` | 命中节点的上游配额耗尽 |
| `CIRCUIT_OPEN` | 注册表中仍有有权节点，但全部被熔断隔离 |
| `NO_AVAILABLE_NODE` | 无节点 / 全部权重为 0 |
| `TIMEOUT` | 执行超过 `timeoutMillis` |
| `UPSTREAM_ERROR` | 上游抛出异常 |

## 限流策略

- 双维度令牌桶：**按调用方** 与 **按上游节点**，桶相互独立，
  某一维度的消耗不影响另一维度的配额统计。
- 取令牌操作同步化，突发并发下放行量精确等于容量上限
  （`RateLimiterTest` 与 `GatewayServiceTest.concurrentCallerBurstRespectsCapacityExactly`
  中 200/150 并发线程突刺，放行量精确等于容量）。
- 时钟可注入，测试用虚拟时间验证补充速率，无真实等待。

## 配置（`application.properties`，前缀 `gateway.`）

| 键 | 默认值 | 说明 |
|---|---|---|
| `gateway.timeoutMillis` | 500 | 单次执行超时 |
| `gateway.maxAttempts` | 3 | 幂等请求最大尝试次数 |
| `gateway.failureThreshold` | 3 | 熔断连续失败阈值 |
| `gateway.openCooldownMillis` | 5000 | 熔断冷却期 |
| `gateway.callerCapacity` / `callerRefillPerSecond` | 50 / 10 | 调用方令牌桶 |
| `gateway.upstreamCapacity` / `upstreamRefillPerSecond` | 100 / 50 | 上游令牌桶 |
| `gateway.nodes[i].id` / `weight` | a:5, b:3, c:2 | 初始节点 |

## REST 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /gateway/invoke?callerId=&sessionId=&idempotent=&operation=` | 转发一次请求 |
| `GET /gateway/nodes` | 节点权重、健康状态、执行计数 |
| `POST /gateway/nodes?id=&weight=` | 新增节点 |
| `DELETE /gateway/nodes/{id}` | 摘除节点 |
| `POST /gateway/nodes/{id}/weight?weight=` | 调整权重 |
| `POST /gateway/nodes/{id}/fail` / `recover` | 故障注入 / 恢复 |

## 兼容范围

- Java 21+（使用虚拟线程），Spring Boot 3.4.x。
- 全部状态在单进程内存中，不保证跨实例一致；适用于本地验证与单实例部署。
- 上游为进程内模拟节点；接入真实上游时替换 `UpstreamNode.execute` 即可，
  路由/限流/健康判定逻辑不变。

## 验证方法

```bash
# 单元测试（含并发限流、故障与恢复、非幂等重试场景，
# 日志打印请求特征、命中节点与判定依据）
mvn test

# 本地启动并手工验证
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=18099"
curl -s http://localhost:18099/gateway/nodes
curl -s -X POST "http://localhost:18099/gateway/invoke?callerId=u1&sessionId=s1"
curl -s -X POST "http://localhost:18099/gateway/nodes/node-a/fail"
curl -s -X POST "http://localhost:18099/gateway/invoke?callerId=u1&idempotent=false&operation=charge"
```

测试日志示例（判定依据可观测）：

```
request features: callerId=caller, sessionId=null, idempotent=true, operation=op
decision: node [a] timed out after 100ms, consecutiveFailures=2
decision: idempotent request, failing over from [a], attempt 1/3
decision: hit node [b], exec seq=1, health=CLOSED
```
