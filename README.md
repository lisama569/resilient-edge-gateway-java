# 本地反向代理网关（resilient-edge-gateway-java）

在 Spring Boot 服务框架内实现的一套**进程内本地反向代理网关**。上游节点由
`SimulatedUpstreamCluster` 在进程内模拟（成功 / 抛错 / 超时 / 先失败 N 次后自愈均可控），
不发起任何真实网络调用、不依赖外部服务或真实凭据。并发、故障与恢复场景的路由结论
完全确定、可复现。

## 1. 架构与请求链路

```
HTTP 请求 (/proxy/**)
  └─ GatewayController        从请求头提取请求特征（含 X-Max-Budget-Ms 时间预算）
       └─ GatewayService      主编排
            0. RequestBudget  端到端时间预算：从收到请求起计时（可选）
            1. RateLimiter    调用方维度限流（caller 独立配额）
            2. LoadBalancer   按会话粘滞 / 权重选择健康节点
            3. RateLimiter    上游节点维度限流（node 独立配额）
            4. 带超时调用 SimulatedUpstreamCluster（等待=min(节点超时, 剩余预算)）
            5. HealthTracker  成功清零 / 失败累计并熔断（预算掐断不计失败）
            6. 仅幂等请求允许失败转移到下一节点（非幂等只尝试 1 次）
```

核心组件均在 `com.github.lisama569.resilientedgegatewayjava.gateway` 包下：

| 组件 | 职责 |
| --- | --- |
| `model.ProxyRequest` | 请求特征：caller / requestId / sessionId / method / path / budgetMs |
| `model.RequestBudget` | 单请求端到端时间预算：剩余预算、每次尝试实际等待时长 |
| `model.RouteDecision` | 路由结论：命中节点 + 判定依据（STICKY_SESSION / STICKY_REBOUND / WEIGHTED / RETRY_FALLBACK） |
| `model.ProxyResponse` | 响应 + 完整 attempts 尝试链（节点、结果、原因、耗时） |
| `model.FailureReason` | 可区分的失败原因枚举 |
| `route.LoadBalancer` | 加权选择 + 会话粘滞 + 失败转移 |
| `route.NodeRegistry` | 节点注册表：配置初始化、调权、摘除、上线（原子整体下发） |
| `health.HealthTracker` | 连续失败计数、熔断隔离、半开试探、恢复 |
| `ratelimit.RateLimiter` | 调用方 / 上游双维度独立固定窗口限流 |
| `upstream.SimulatedUpstreamCluster` | 进程内可控上游 + 非幂等重复执行检测 |
| `GatewayService` | 限流→路由→超时调用→健康反馈→重试编排 |

## 2. 路由与权重规则

1. **候选集**：只在 `enabled=true` 且健康（未熔断、非半开拒绝）的节点中选择。
   节点被摘除、下线或熔断后，新流量立即不再进入该节点（节点表整体替换，原子可见）。
2. **会话粘滞**：请求带 `X-Session-Id` 时，在其绑定节点健康期间保持稳定落点，
   判定依据 `STICKY_SESSION`；绑定节点摘除/熔断后重新绑定，依据 `STICKY_REBOUND`。
3. **加权选择**：无会话或新会话按节点权重做加权区间抽取，权重为正整数（非法权重
   直接拒绝，不会被静默忽略）。权重 3:2:1 在大样本下落点比例近似 3:2:1。
4. **失败转移**：幂等请求失败后在未尝试过的健康节点中继续加权选择，依据
   `RETRY_FALLBACK`；非幂等请求（POST/PATCH 等）**只尝试一个节点，绝不重复执行**。

## 3. 健康与熔断策略

- 节点**连续失败**（含上游错误与超时）达到 `gateway.failure-threshold`（默认 2）次后
  立即熔断隔离，新流量不可进入。
- 经过 `gateway.recover-after-ms`（默认 300ms）冷却后进入**半开**，仅放行一次试探：
  成功则恢复 HEALTHY 重新参与分配；失败则立即重新熔断并重新计时。
- 每个节点独立加锁计数，并发上报不丢失；节点重新上线时健康计数重置。

## 4. 限流策略

- 固定 1 秒窗口，两个维度**完全独立**、互不借用配额：
  - `caller` 维度：默认 `gateway.caller-default-permits-per-sec`，可用
    `gateway.caller-overrides.<caller>` 单独覆盖；
  - `upstream` 维度：默认 `gateway.upstream-default-permits-per-sec`，可用
    `gateway.upstream-overrides.<node-id>` 单独覆盖。
- 同一维度不同 key（不同调用方 / 不同节点）也各自独立计数。
- 窗口翻转与计数在临界区内完成，**突发并发下实际放行数严格等于配额，不会超发**。
- 某调用方被限流不影响上游节点配额，反之亦然。

## 5. 端到端时间预算（X-Max-Budget-Ms，可选）

调用方通过请求头 `X-Max-Budget-Ms: <毫秒>` 声明"这次请求我最多能等多久"。

- **计时起点**：网关收到请求即开始计时，后续所有重试与失败转移都计入同一预算。
- **每次尝试实际等待** = `min(节点自身 timeout, 当前剩余预算)`。两个方向都取较小值：
  预算比节点超时短则按预算提前掐断；预算宽裕则仍按节点超时判定。
- **提前停止**：剩余预算不足以再试下一个节点时立即返回
  `BUDGET_EXHAUSTED`（HTTP 504，与 `UPSTREAM_TIMEOUT` 的 reason 可区分），
  不会硬撑到节点超时才回结果。
- **预算掐断 ≠ 节点失败**：被预算提前掐断的尝试**不计入**该节点的连续失败与
  熔断统计（attempt 记录为 `ABORTED/BUDGET_EXHAUSTED`），短预算请求不会误伤
  健康节点；而真正的节点超时 / 上游错误仍按原规则计入健康统计并触发熔断。
- **与既有能力的关系**：会话粘滞、权重分配、非幂等只试一次、双维度限流、
  熔断隔离的判定规则完全不变；预算只是在"每次尝试等多久"和"是否还值得再试
  下一个节点"两个决策点上叠加约束。
- **并发隔离**：预算对象随单个请求创建、仅在该请求的处理线程内使用，不共享
  任何可变状态；同一调用方并发多个不同预算的请求互不影响。
- **兼容范围**：不带 `X-Max-Budget-Ms`（或值 <= 0）时行为与旧版完全一致，
  响应中 `budgetRemainingMs` 为 `null`。

成功响应会携带 `budgetRemainingMs`（剩余预算），每次尝试记录实际耗时
`elapsedMs`；日志会打印每次尝试前的剩余预算与本次允许等待时长，以及本次是
"预算用完提前结束"还是"被节点真正拖超时"：

```
[gateway] 请求 req-1 第 1 次尝试节点 node-a：剩余预算 200ms，本次最多等待 200ms
[gateway] 请求 req-1 在节点 node-a 被预算掐断（耗时 201ms，剩余预算 0ms），不影响节点健康与熔断统计
```

## 6. 失败原因与 HTTP 映射（结果可区分，不返回模糊失败）

| FailureReason | 含义 | HTTP |
| --- | --- | --- |
| `RATE_LIMITED_CALLER` | 调用方维度配额耗尽 | 429 |
| `RATE_LIMITED_UPSTREAM` | 上游节点维度配额耗尽 | 429 |
| `NO_AVAILABLE_NODE` | 无启用节点（全部摘除/下线/未配置） | 503 |
| `ALL_NODES_CIRCUIT_OPEN` | 启用节点全部熔断中 | 503 |
| `UPSTREAM_TIMEOUT` | 上游调用超过节点 timeout | 504 |
| `UPSTREAM_ERROR` | 上游返回错误（非幂等失败直接返回此原因，502） | 502 |
| `ALL_ATTEMPTS_FAILED` | 幂等请求尝试多个节点后仍全部失败 | 503 |
| `BUDGET_EXHAUSTED` | 调用方给定的端到端时间预算耗尽（提前停止，非节点故障） | 504 |

错误响应体形如：`{"reason":"UPSTREAM_TIMEOUT","message":"node node-a timed out after 200ms"}`。
成功响应体形如：

```json
{"status":200,"body":"hello from node-b","servedBy":"node-b",
 "attempts":[{"nodeId":"node-a","outcome":"FAILED","reason":"UPSTREAM_ERROR+CIRCUIT_OPENED@WEIGHTED","elapsedMs":2},
             {"nodeId":"node-b","outcome":"SUCCESS","reason":"RETRY_FALLBACK","elapsedMs":1}]}
```

## 7. 使用方式与本地验证

### 7.1 运行测试（全部本地、无需任何外部依赖）

```bash
mvn test
```

30 个测试覆盖：

- `WeightedRoutingTest`：6000 样本验证 3:2:1 权重分布；
- `StickySessionTest`：会话健康期稳定落点、摘除/熔断后重新绑定且新流量不进入旧节点；
- `HealthTrackerTest`：连续失败熔断、半开试探成功恢复、试探失败重新熔断；
- `FailoverServiceTest`：幂等失败转移链、连续失败隔离、非幂等绝不重试、超时原因、
  无启用节点 vs 全部熔断两种原因可区分；
- `ConcurrentRateLimitTest`：200 突发并发严格放行配额数、双维度独立、不同调用方配额隔离；
- `NodeManagementTest`：摘除节点调用计数恒为 0、非法权重被拒绝、FAIL_FIRST_N 自愈、
  下线后重新上线恢复服务；
- `TimeBudgetTest`：预算掐断提前失败且原因可区分、单次尝试等待取
  min(节点超时, 剩余预算) 两个方向边界、预算掐断不误伤节点健康/熔断、
  真实超时仍计入失败统计、同调用方并发不同预算互不影响、非幂等预算掐断只试一次、
  不带预算时行为完全回归；
- `GatewayHttpIntegrationTest`：完整 HTTP 栈验证 200/429/503/504/502 与错误体 reason。

测试日志（INFO）会打印**请求特征、命中节点与判定依据**，例如：

```
[route] 请求 ProxyRequest{caller='c1', sessionId='sess-Z', method=GET, path='/p'}
        命中节点 node-a（依据=STICKY_SESSION，候选权重=node-a=1,node-b=1）
[health] node=node-a 连续失败 2 次，熔断隔离 300ms
[gateway] 请求 req-1 在节点 node-a 失败：UPSTREAM_TIMEOUT（依据=WEIGHTED，熔断=true）
[ratelimit] 维度=caller key=tenant-1 被限流（窗口已用 50/50）
```

### 7.2 启动服务并手工演练

```bash
mvn spring-boot:run            # server.port=0，启动日志中查看实际端口
```

代理调用（请求头携带请求特征）：

```bash
curl -s -H 'X-Caller: svc-1' -H 'X-Session-Id: sess-1' -H 'X-Request-Id: r1' \
     http://localhost:<port>/proxy/orders/123
```

带端到端时间预算的调用（预算 200ms，节点超时默认 300ms 时会被预算提前掐断）：

```bash
# 先把 node-a 设为慢节点（延迟 2000ms），再用短预算调用：
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/behavior?mode=TIMEOUT&delayMs=2000'
curl -s -H 'X-Caller: svc-1' -H 'X-Max-Budget-Ms: 200' http://localhost:<port>/proxy/slow
# => HTTP 504 {"reason":"BUDGET_EXHAUSTED","message":"budget cut attempt on node node-a after ..."}
# 对比：不带预算时同一请求按节点超时处理，reason=UPSTREAM_TIMEOUT 并发生失败转移
curl -s -H 'X-Caller: svc-1' http://localhost:<port>/proxy/slow
```

通过管理面驱动故障/恢复（全部进程内生效）：

```bash
curl -s http://localhost:<port>/admin/nodes                      # 查看节点权重/健康/调用次数
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/behavior?mode=FAIL'
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/behavior?mode=TIMEOUT&delayMs=500'
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/behavior?mode=FAIL_FIRST_N&n=2'
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/behavior?mode=OK'
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/enabled?value=false'  # 摘除
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/enabled?value=true'   # 重新上线
curl -s -X POST 'http://localhost:<port>/admin/nodes/node-a/weight?value=5'       # 调权
curl -s http://localhost:<port>/admin/observability              # 非幂等重复执行计数
```

### 7.3 关键配置（application.properties）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `gateway.nodes[i].id/weight/enabled/timeout-ms` | node-a/b/c，权重 3/2/1 | 上游节点 |
| `gateway.failure-threshold` | 2 | 连续失败多少次熔断 |
| `gateway.recover-after-ms` | 300 | 熔断冷却后半开试探 |
| `gateway.max-attempts` | 3 | 幂等请求最多尝试节点数（非幂等恒为 1） |
| `gateway.caller-default-permits-per-sec` | 100 | 调用方默认配额（0=不限） |
| `gateway.upstream-default-permits-per-sec` | 100 | 上游默认配额（0=不限） |

## 8. 兼容范围

- Java 21（源码/目标版本），Spring Boot 3.4.x / Spring Framework 6.x，Jakarta Servlet。
- 仅依赖 `spring-boot-starter-web` 与测试 starter，无额外中间件、无外部网络调用。
- 上游"调用"为进程内方法调用（超时用受控线程 + `Future.get(timeout)` 模拟），
  因此结论不受真实网络抖动影响；生产接入真实上游时只需替换 `UpstreamInvoker` 实现。
