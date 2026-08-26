# Redis Cluster 路由与流水线重定向生命周期分析

> 面向 go-redis v9（`github.com/redis/go-redis/v9`）线上 Redis Cluster 扩缩容期间
> "少量流水线请求延迟升高"的故障复盘文档。目标是把命令从公开入口到回复解码、
> 重定向、重试、连接复用的完整路径讲清楚，并把每个关键不变量映射到具体源码入口和
> 回归测试。
>
> **阅读约定 —— 三类结论必须分辨清楚：**
> - **【源码保证】** 直接由源码逻辑保证的行为，给出文件与函数入口。
> - **【测试佐证】** 有回归测试覆盖，给出测试名与本次实际运行结果（见文末"测试运行记录"）。
> - **【待确认推断】** 由代码结构推断、但当前仓库未见针对性断言的结论，复盘时需实测确认。
>
> 本文档只新增，不修改任何 Go 源码、测试或既有文档。行号基于本次评审时的工作树，
> 若后续代码变动请以函数名为准重新定位。

---

## 0. 结论速览（给赶时间的复盘同学）

扩缩容期间"少量流水线请求延迟升高"的根因通常落在下面三类边界上，本文逐一展开：

1. **重定向即路由变化，不做退避睡眠。** MOVED/ASK 立刻重试目标节点，不进入
   `retryBackoff` 睡眠；真正引入延迟的是**拓扑刷新的滞后**（`LazyReload` 异步 +
   200ms 冷却窗口）导致在窗口内的请求持续吃 MOVED 往返，直到快照更新。
2. **流水线按节点拆分、并行 fan-out，只重排失败子集。** 成功命令不进入 `failedCmds`，
   因此重定向重试不会重复执行已成功命令；延迟来自被重定向命令的额外 RTT（ASK 还多一个
   `ASKING` 命令的往返）。
3. **传输错误 / Redis 错误 / 上下文取消 / 禁止重试命令走完全不同的分支**，决定了剩余命令
   是被重排、被标记同一错误、还是连接被作废。判断错误分支是复盘定位的关键。

---

## 1. 公开入口与调用骨架

### 1.1 单命令入口

单节点客户端 `Client.Process`（[redis.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go)）
最终落到 `baseClient.process` → `_process`；集群客户端 `ClusterClient` 覆写了
`process`，把命令送入集群专用的路由 + 重定向循环。

- 集群单命令核心：[osscluster.go `ClusterClient.process`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1298-L1398)
- 单节点单命令核心：[redis.go `baseClient._process`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1486)

### 1.2 Pipeline / TxPipeline 入口

三个用户可见的构造入口，都返回 `Pipeliner`：

| 入口 | 构造 | 执行函数（exec） | 说明 |
| --- | --- | --- | --- |
| `ClusterClient.Pipeline()` | [pipeline.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/pipeline.go) 的 `Pipeline` | `processPipelineHook` → [`processPipeline`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1788) | 无槽约束，跨节点 fan-out |
| `ClusterClient.TxPipeline()` | [osscluster.go L2153](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2152-L2162) | `wrapMultiExec` → `processTxPipelineHook` → [`processTxPipeline`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2251) | MULTI/EXEC，强制同槽 |
| 自动流水线（AutoPipeline） | [autopipeline.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/autopipeline.go) | 批攒够后直接调 `withProcessPipelineHook` → `processPipeline` | 无显式 `Exec`，后台 flush |

`Pipeline.Exec` 清空累积的 `c.cmds` 后把切片交给 exec 闭包
（[pipeline.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/pipeline.go)）。
单节点 Pipeline 与 TxPipeline 的 exec 走
[redis.go `generalProcessPipeline`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1769)；
集群则走上表中的集群实现。

---

## 2. 命令键 → 槽位

**【源码保证】** 槽位计算入口是
[osscluster.go `cmdSlot`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2892)
→ `cmdSlotWithPos`。流程：

1. 通过 `cmdFirstKeyPosWithInfo`（[command.go L301](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L301)）
   拿到命令的第一个键位置。keyless 命令（如 `PING`、`CLUSTER *`）返回位置 0，
   `cmdSlot` 据此返回 `-1`（无槽）。`EVAL/EVALSHA` 读 `numkeys`、`MEMORY USAGE`
   等有特例处理。
2. 用 `stringArg`（[command.go L495](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L495)）
   取出键（兼容 `string` / `*string` / `[]byte`），交给
   [internal/hashtag](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/internal/hashtag/hashtag.go)：
   `Key()` 提取 `{tag}` 花括号标签，`Slot() = CRC16(tag) % 16384`；空 key 返回随机槽。
3. **槽位缓存**：`cmdSlot` 在 `prefferedSlot == -1` 时会读取/回填命令上的 `slotCache`
   （[command.go `cachedSlot`/`setCachedSlot` L525/L533](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L525)，
   缓存值存 `slot+1` 以便用 0 区分"未缓存"）。同一命令对象重放时不重复计算。

TxPipeline 用 `slottedKeyedCommands`
（[osscluster.go L2366](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2364-L2394)）
只统计**有键**的命令，并对整批共享一个 `prefferedRandomSlot`，keyless 命令不参与
跨槽判定。

---

## 3. 集群状态快照与节点选择

### 3.1 快照读取

**【源码保证】** 集群拓扑保存在 `clusterState`（`Masters`/`Slaves`/`slots`），通过
`atomic.Value` 无锁读取。持有者是 `clusterStateHolder`：

- [`Get`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1146)：
  快照为 nil 时同步 `Reload`；快照存在但超过 `reloadInterval` 时触发异步 `LazyReload`
  并**立即返回旧快照**。
- [`ReloadOrGet`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1159)：
  强制 `Reload`，失败回退到 `Get`。
- [`loadState`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1576)：
  以随机顺序遍历 `addrs` 尝试 `CLUSTER SLOTS`，全部失败则清空 `activeAddrs`。

> **复盘要点（延迟来源）**：扩缩容后旧快照仍会被返回，直到 `LazyReload` 落地。
> 在这段窗口内命中迁移槽的请求会持续收到 MOVED/ASK，每次多一次 RTT——这正是
> "少量请求延迟升高"最常见的解释。见 §5、§8。

### 3.2 主/副本选择

**【源码保证】** 单命令通过 `cmdNode`/`cmdNodeWithShardPicker`
（[osscluster.go L1315-L1319](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1313-L1323)）
选节点；具体到槽：

- [`slotMasterNode`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2998)：主节点。
- [`slotReadOnlyNode`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2983)：
  按 `RouteByLatency` / `RouteRandomly` / `ReplicaOnly` 策略选只读副本。

只有当 `c.opt.ReadOnly && cmdsAreReadOnly(cmds)` 时，Pipeline 才走只读分支把命令映射到
副本（[mapCmdsByNode L1860-L1905](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1860-L1905)）；
否则一律映射到主节点（L1907-L1942）。

keyless 命令（`slot == -1`）用 `ShardPicker` 选任意节点；只读分支从 masters+slaves
里选，写分支只从 masters 里选。**注意**：这里刻意用索引直接取而不是
`append(Masters, Slaves...)`——后者会写入共享快照的空闲容量造成 data race
（[L1887-L1895 注释](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1887-L1895)；
同类注释也在 [osscluster_router.go `pickArbitraryNode`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster_router.go#L493)）。

---

## 4. 流水线执行：拆分、借连接、写入、读回

集群 Pipeline 的主循环
[`processPipeline`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1788-L1852)：

```
processPipeline(cmds)
  cmdsMap = newCmdsMap()
  mapCmdsByNode(cmdsMap, cmds)          // 按节点分桶；失败则整批 setCmdsErr 后返回
  for attempt in 0..MaxRedirects:
      if attempt > 0: Sleep(retryBackoff)   // 仅重试才退避
      failedCmds = newCmdsMap()
      for node, cmds in cmdsMap.m:      // 每节点一个 goroutine，并行 fan-out
          go processPipelineNode(node, cmds, failedCmds)
      wg.Wait()
      if len(failedCmds.m) == 0: break  // 没有需要重排的命令 → 完成
      cmdsMap = failedCmds              // 只对失败子集重试
  return cmdsFirstErr(cmds)
```

### 4.1 按节点拆分

**【源码保证】** [`mapCmdsByNode`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1854)
逐命令算槽、选节点、`cmdsMap.Add(node, cmd)`。若某命令的请求策略
`!CanBeUsedInPipeline()`（如 `ReqAllNodes/ReqAllShards/ReqMultiShard`），
用户 Pipeline 采用 **all-or-nothing**：整批 `setCmdsErr` 后返回，任何命令都不发出
（[L1866-L1879](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1866-L1879)）。
AutoPipeline 则在 submit 阶段就用 preflight 拒绝这类命令，避免污染合并批（见 §7）。

### 4.2 借用连接（含 spill）

**【源码保证】** 每节点用
[`processPipelineNode`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1956)
→ `withProcessPipelineHook` → `withPipelineConn`
→ [`processPipelineNodeConn`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2015)。
连接借用在
[redis.go `withPipelineConn`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1217)：
优先用专用 pipeline 连接池，饱和时 spill 到主连接池。

### 4.3 写入与按序读回异构回复

**【源码保证】** `processPipelineNodeConn` 用 `WithWriter` 写入
`injected + cmds`（injected 是 ASK 场景插入的 `ASKING` 前置命令），再用 `WithReader`
读；读逻辑在
[`pipelineReadCmds`（集群版）L2073](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2073-L2123)：

```
for i, cmd in cmds:
    processPendingPushNotificationWithReader(...)   // 先排空 RESP3 push 帧
    err = cmd.readReply(rd); cmd.SetErr(err)
    if err == nil: continue
    if checkMovedErr(cmd, err, failedCmds): continue // MOVED/ASK → 加入 failedCmds
    if ReadOnly && isBadConn(...): node.MarkAsFailing()
    if !isRedisError(err):                           // 传输错误
        if shouldRetry && !cmdsContainNoRetry: mapCmdsByNode(failedCmds, cmds) // 整批重排
        setCmdsErr(cmds[i+1:], err)                  // 剩余命令标记同一错误
        return err
```

**异构回复解码**：每个 `cmd.readReply(rd)` 用命令自己的类型解析器读取该命令的回复，
因此一批混合 bulk/array/int/map/nil/double/大对象/二进制安全值的命令能各取其正确类型。
关键前提是**读指针始终对齐**——所以每次读之前先排空 RESP3 push 帧
（`processPendingPushNotificationWithReader`，
[L2089 注释](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2082-L2091)）。
一旦一个 push 帧被误当成命令回复，后续所有回复都会错位一个命令。

**【测试佐证】** `TestPipelineHeterogeneousReplyDemux` / `...RESP2` /
`TestAutoPipelineHeterogeneousReplyDemux` / `...RESP2Batch`
（[pipeline_exec_test.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/pipeline_exec_test.go)）
断言九种异构回复在一批里各得其正确类型；
`TestStandalonePipelineReadDrainsPushMidBatch` /
`TestClusterPipelineReadDrainsPushMidBatch`
（[autopipeline_internal_test.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/autopipeline_internal_test.go)）
断言批中 push 帧被排空而不错位。均已通过（见文末）。

---

## 5. MOVED 与 ASK 的差异

这是复盘最需要分清的一节。判定入口是
[error.go `isMovedError`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/error.go#L230)，
返回 `(moved, ask, addr)`。

### 5.1 单命令路径

**【源码保证】**
[osscluster.go L1358-L1381](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1358-L1381)：

- 无论 MOVED 还是 ASK，都 `c.state.LazyReload()` + `c.nodes.GetOrCreate(addr)` 拿到
  目标节点，然后 `continue` **立即重试，不睡眠**（循环顶部
  `if attempt > 0 && !moved && !ask` 才 `Sleep(retryBackoff)`，见 L1307）。
- **ASK 特有**：下一次循环进入 `if ask` 分支
  （[L1325-L1330](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1325-L1330)），
  用一个临时 Pipeline 依次发送 `ASKING` + 原命令；`ask` 标志随即清零（一次性）。

### 5.2 Pipeline 路径

**【源码保证】**
[`checkMovedErr`](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2125-L2150)：

- **MOVED**：`c.state.LazyReload()`（刷新拓扑）+ `failedCmds.Add(node, cmd)`
  （只把这一条命令排到目标节点重试）。
- **ASK**：**不**触发 LazyReload；`failedCmds.Add(node, NewCmd(ctx,"asking"), cmd)`
  ——把 `ASKING` 作为**前置注入命令**和原命令一起排到目标节点。下一轮
  `processPipelineNodeConn` 会把这个 `asking` 作为 injected 写在原命令前面。

### 5.3 TxPipeline 路径

**【源码保证】**
[`processTxPipeline` L2316-L2355](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2316-L2355)
用 `txOutcomeKind` 分类：

- `txRetryMoved`：`LazyReload` + 直接路由到 MOVED 给出的 `addr`（因为本地槽映射可能仍
  滞后），`asking=false`。
- `txRetryAsk`：**不** LazyReload，`asking=true`，下一轮整个 MULTI/EXEC 事务前加 `ASKING`。

### 5.4 差异汇总

| 维度 | MOVED | ASK |
| --- | --- | --- |
| 语义 | 槽已永久迁移到目标节点 | 槽正在迁移，本条请求临时去 importing 目标 |
| 触发拓扑刷新 `LazyReload` | **是** | **否**（迁移未完成，刷新无意义且会抖动） |
| 目标节点 | MOVED 携带的 addr | ASK 携带的 addr（importing 侧） |
| `ASKING` 前置命令 | 无 | **有**，且是一次性（单命令 pipe / injected / 事务前缀） |
| 是否退避睡眠 | 否，立即重试 | 否，立即重试 |
| 单命令实现 | L1360 `LazyReload`+`GetOrCreate` | L1325 `ASKING`+cmd 临时 pipeline |
| Pipeline 实现 | `checkMovedErr` MOVED 分支 | `checkMovedErr` 注入 `asking`+cmd |
| Tx 实现 | `txRetryMoved` | `txRetryAsk`（`asking=true`） |

> **复盘要点**：如果监控看到大量 MOVED，说明拓扑确实变了但客户端快照滞后；如果看到
> 大量 ASK，说明槽正在迁移途中（扩缩容 resharding 进行时）。ASK 每条多一个 `ASKING`
> 往返，这是扩缩容期间流水线尾延迟升高的直接来源之一。

---

## 6. 为什么成功的同批命令不会被重复执行

这是复盘中"是否发生重复写"的核心问题。**【源码保证】** 由三条不变量共同保证：

1. **只重排失败子集。** `processPipeline` 的重试循环把 `cmdsMap = failedCmds`
   （[L1837](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1837)），
   而成功的命令根本不会被 `checkMovedErr`/传输错误分支加入 `failedCmds`——所以下一轮只
   重发被重定向或失败的命令。

2. **`setCmdsErr` 只覆盖无错命令。**
   [command.go L254](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L254)：
   `if cmd.rawErr() == nil { cmd.SetErr(e) }`——已成功（或已持有自己错误）的命令不会被
   批量错误覆盖。

3. **事务读阶段的 IO 错误视为 fatal，不重放。**
   [`txReadFatal` L2593](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2593)：
   EXEC 已发出后读回复阶段的 IO error，事务可能已在服务端提交，重试会重复执行非幂等命令，
   因此归为 `txFatal` 直接返回，绝不重试。

**【测试佐证】** `TestTruthRetriesDoNotDuplicate`
（[autopipeline_truth_test.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/autopipeline_truth_test.go)）
断言 "at-most-once"：10000 次自增 == 10000 个成功 future，重试不产生重复自增。已通过。

---

## 7. 传输错误 / Redis 错误 / 上下文取消 / 禁止重试命令

### 7.1 错误分类器

- [`shouldRetry` error.go L84](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/error.go#L84)：
  EOF / dial error / pool timeout 可重试；`LOADING`/`READONLY`/`MASTERDOWN`/
  `CLUSTERDOWN`/`TRYAGAIN`/`NOREPLICAS` 可重试；**`context.Canceled` /
  `context.DeadlineExceeded` 不重试**。
- [`isRedisError` L177](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/error.go#L177)：
  区分服务端 `-ERR` 类错误与传输错误。
- [`isBadConn` L188](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/error.go#L188)：
  context error / 不可用超时 / readonly / movedSameConnAddr 时判连接不可复用。
- 命令级禁止重试：[command.go `NoRetry` L573](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L573)
  （默认 false），批级 `cmdsContainNoRetry`
  （[command.go L276](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/command.go#L276)）。

### 7.2 对剩余命令与连接的影响（Pipeline）

**【源码保证】** 看
[集群 `pipelineReadCmds` L2092-L2113](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2092-L2113)：

- **传输错误（非 redis error）**：读指针已错乱，无法信任后续回复。若 `shouldRetry` 且
  批内无 NoRetry 命令，`mapCmdsByNode(failedCmds, cmds)` 把**整批**重排到新连接；同时
  `setCmdsErr(cmds[i+1:], err)` 给剩余命令打上同一错误，然后 `return err`。`ReadOnly`
  下还会 `node.MarkAsFailing()` 让该副本短期内不被选中。**连接不复用**（transport
  error 由 `processPipelineNodeConn` 上层判 bad conn 后作废）。
- **Redis 错误（服务端 `-ERR`，如 WRONGTYPE）**：`isRedisError` 为真，**不进入**上面的
  重排分支——该错误被隔离在触发它的那条命令上（`cmd.SetErr(err)` 已在 L2093 设置），
  **兄弟命令继续正常读取各自回复**，**连接可继续复用**（读指针没乱）。
- **上下文取消**：`context.Canceled/DeadlineExceeded` 被 `shouldRetry` 判为不可重试；
  在读循环里被当作非 redis error 处理，剩余命令 `setCmdsErr` 打上 context 错误后返回。

### 7.3 单节点 Pipeline 的对应逻辑

**【源码保证】**
[redis.go `pipelineReadCmds` L1937-L1954](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1937-L1954)：
`err != nil && !isRedisError(err)` 时 `setCmdsErr(cmds[i+1:], err)` 并返回；否则读完
后 `return cmds[0].rawErr()`——保留 `LOADING` 等**首命令可重试**错误让外层
`generalProcessPipeline`（[L1820](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1820)）
决定是否重试。

**【测试佐证】**（均已通过，见文末）：

- `TestPipelineRetriesOnNetworkError`：写失败后恰好重拨一次，全部命令拿到正确结果。
- `TestPipelineRetryExhausted`：坏连接耗尽 `MaxRetries+1` 次尝试后返回错误、不 hang。
- `TestPipelineWrongTypeNotRetriedIsolated`：WRONGTYPE 隔离到坏命令，兄弟命令正常，不重试。
- `TestPipelineContextCanceledNotRetried`：预取消 context 不重试，每条命令带 context 错误。
- `TestNoRetryRunsIsolatePolicyAndPreserveOrder` / `TestRetryRunsStopAfterFailedPrefix`：
  NoRetry 命令隔离执行、失败前缀后停重试。

### 7.4 决策表：故障类型 → 剩余命令 → 连接复用

| 故障类型 | 判定入口 | 是否重试 | 目标 | 对剩余命令 | 连接是否复用 |
| --- | --- | --- | --- | --- | --- |
| MOVED | `isMovedError`→`checkMovedErr` | 是（立即，无退避） | MOVED addr | 各自独立，仅被重定向命令重排 | 复用（该命令换目标节点连接） |
| ASK | `isMovedError`→`checkMovedErr` | 是（立即，无退避） | ASK addr | 各自独立，重排时前插 `ASKING` | 复用（目标节点连接） |
| 传输错误（EOF/dial/超时） | `!isRedisError` + `shouldRetry` | 是（退避后整批重排） | 新连接/新节点 | `cmds[i+1:]` 打同一错误后整批重排 | **作废**，重拨；副本会 MarkAsFailing |
| Redis 错误（WRONGTYPE 等） | `isRedisError` | 否 | — | **不受影响**，各读各的回复 | 复用 |
| LOADING/CLUSTERDOWN/TRYAGAIN 等 | `shouldRetry`=true | 是（退避后重试） | 同/换节点 | 保留在 `cmds[0].rawErr()` 触发外层重试 | 依错误而定 |
| 上下文取消/超时 | `context.*`，`shouldRetry`=false | 否 | — | `cmds[i+1:]` 打 context 错误 | 视 `isBadConn` 作废 |
| 禁止重试命令（NoRetry） | `cmdsContainNoRetry` | 否 | — | 失败前缀后停止重排 | 依错误而定 |
| 重定向次数耗尽（MaxRedirects） | 循环自然结束 | — | — | 保留最后一次 MOVED/ASK 或错误 | — |
| 重试次数耗尽（MaxRetries） | `generalProcessPipeline` 尾部 | — | — | `!isRedisError` 时补 `setCmdsErr` | — |

---

## 8. 拓扑懒加载：合并并发刷新请求

**【源码保证】** 单飞（single-flight）逻辑在
[`LazyReload` L1111-L1144](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L1111-L1144)，
用两个 `atomic.Uint32`：

```
LazyReload():
  if !reloading.CompareAndSwap(0,1):   // 已有 reload 在跑
      reloadPending.Store(1)           // 只记"还需要再刷一次"，立即返回
      return
  go for {
      Reload()                         // 失败则清零两标志退出
      reloadPending.Store(0)           // 冷却前清 pending，捕获 reload 期间到达的通知
      Sleep(200ms)                     // 冷却窗口
      if reloadPending.Load() == 0:    // 冷却期无新请求 → 收工
          reloading.Store(0); return
      // 否则再循环刷一次（把冷却期内的所有请求合并成一次）
  }
```

效果：任意时刻最多一个 reload goroutine 在跑；冷却窗口内到达的**任意多个**刷新请求都
被合并成**一次**后续 reload。

**【测试佐证】** `TestLazyReloadQueueBehavior`
（[osscluster_lazy_reload_test.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster_lazy_reload_test.go)）
7 个子用例，全部通过（见文末）：

- `SingleReload`：单次触发 → 1 次 reload。
- `ConcurrentReloadsDeduplication`：10 个并发 `LazyReload` → 去重成 1 次。
- `PendingReloadDuringCooldown` / `MultiplePendingReloadsCollapsed`：冷却期内的多次请求
  合并成额外 1 次（共 2 次）。
- `CascadingSMIGRATEDScenario`：模拟扩缩容期间 5 个 SMIGRATED 通知连环到达 → 1~2 次
  reload。这条正是本次故障场景的直接建模。

> **复盘要点（延迟窗口）**：`LazyReload` 是异步的且带 200ms 冷却。这意味着扩缩容瞬间，
> 客户端不会因为"每个 MOVED 都刷新"而把控制面打爆，但代价是在 reload 落地前的这段窗口里，
> 命中迁移槽的请求会持续吃 MOVED/ASK 往返——**这就是"少量请求延迟升高、随后自愈"的机制**。

---

## 9. 三种流水线的差异对照

| 维度 | 普通 Pipeline | TxPipeline | AutoPipeline（自动流水线） |
| --- | --- | --- | --- |
| 槽约束 | 无，跨节点 fan-out（`mapCmdsByNode`） | **强制同槽**，跨槽返回 `ErrCrossSlot`（[L2289-L2293](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster.go#L2289-L2293)） | 集群面按 shard 分片，preflight 拒绝非 pipelineable 命令 |
| 并发/顺序 | 每节点一个 goroutine 并行；节点内保序 | 单节点单连接串行 MULTI/EXEC | stripe 分片（默认 `apEnqueueStripes=8`）；`MaxConcurrentBatches` 默认 1，`Unordered` 可放宽 |
| 触发 | 显式 `Exec()` | 显式 `Exec()`（外包 MULTI/EXEC） | 无显式 Exec，后台 flusher 攒批（自适应延迟窗口） |
| Hook | `ProcessPipelineHook` | `processTxPipelineHook` | 批走 `ProcessPipelineHook`；solo 走 `withProcessHook`；divert 走 `process` |
| 重定向 | 每命令独立 MOVED/ASK 重排（`checkMovedErr`） | 整个事务级 MOVED/ASK（`txRetryMoved/txRetryAsk`） | 复用 `processPipeline`，与普通 Pipeline 同一重排逻辑 |
| 关闭 | 随客户端关闭 | 随客户端关闭 | `AutoPipeliner.Close` → `drainAll`（30s backstop）；`sharedClosed` 后所有入口拒绝新命令 |
| 结果暴露 | `Exec` 返回错误 + 每命令 `Err()` | `Exec` 返回错误 + 每命令 `Err()`（EXECABORT 标记全部未执行命令） | **无 Exec 返回值**，只能靠每命令/future 暴露——所以 `generalProcessPipeline` 尾部要补 `setCmdsErr`（[L1840-L1849](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1840-L1849)） |

### 9.1 AutoPipeline 关键调用点

**【源码保证】** AutoPipeline 的 `dispatchCmds` 直接调
`ap.pipeliner.withProcessPipelineHook(ctx, cmds, func(){ return ap.pipeliner.processPipeline(ctx, cmds) })`
（[autopipeline.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/autopipeline.go) 内 dispatch 路径），
即复用集群/单节点的同一条 `processPipeline` 路径，因此**继承了 §4–§7 的全部重排/重试/
去重保证**。特殊命令走 `runOutsidePipeline`（divert，单独连接不批处理）。

**【测试佐证】**（均已通过）：

- `TestTruthCancelNeverLies`：取消场景下 future 结果要么成功要么报错，绝不"谎报"。
- `TestTruthCloseDrains`：Close 时已确认执行的 5000 条全部兑现，其余干净拒绝。
- `TestTruthResponseDemux` / `TestTruthRetriesDoNotDuplicate`：多路复用正确、重试不重复。
- `TestClusterNodeHookResultPeekNoDeadlock`
  （[osscluster_autopipeline_nodehook_test.go](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/osscluster_autopipeline_nodehook_test.go)）：
  验证 redisotel 在 NODE 客户端安装的 hook 在 `next()` 前读取每条命令的 `Err()`/`String()`
  不会与 batch-executor 相互死锁（deferred face 上历史 bug）。本次运行显示装了 3 个 node
  hook 且未 hang。

---

## 10. 调用方最终看到什么

**【源码保证】**

- **旧状态（快照滞后）**：`Get` 会返回旧快照并异步刷新（§3.1、§8），调用方在窗口内
  可能收到 MOVED/ASK，被客户端自动跟随；对上层通常表现为**延迟升高但结果正确**。
- **节点失效**：传输错误触发 `node.MarkAsFailing()` + 整批重排到其他节点（§7.2）；
  若 `loadState` 遍历所有 addr 全失败，`activeAddrs` 被清空，后续 `Get` 会尝试重建。
- **重定向耗尽（MaxRedirects）**：`process`/`processPipeline`/`processTxPipeline` 循环
  自然结束，调用方看到最后一次 MOVED/ASK 错误或底层错误（`cmdsFirstErr`）。
- **重试耗尽（MaxRetries）**：单节点 `generalProcessPipeline` 尾部对 `!isRedisError` 的
  `lastErr` 补 `setCmdsErr`（[L1847](file:///e:/newGsb/generated/question-exchange/questions/GSB-009/Poseidon/redis.go#L1847)），
  保证 AutoPipeline 这种没有 `Exec` 返回值的调用方也能在每条命令上看到错误而不是零值。

**【待确认推断】** 扩缩容期间"少量请求延迟升高"的最可能路径：`LazyReload` 冷却窗口
（200ms）+ 每次 ASK 多一个 `ASKING` 往返，叠加并发批中被重定向的子集重排。当前仓库的
单元/逻辑测试覆盖了各分支的正确性，但**没有针对"扩缩容期间 P99 延迟"的量化基准测试**；
复盘若要给出具体延迟数字，需在预生产集群上用 resharding 场景实测（本仓库测试环境无法
复现真实迁移时序）。

---

## 11. 关键不变量 → 源码入口 → 回归测试映射

| # | 不变量 | 源码入口 | 回归测试 |
| --- | --- | --- | --- |
| I1 | 键→槽用 CRC16(hashtag)%16384，空 key 随机槽，结果缓存 | `cmdSlot`/`internal/hashtag`/`cachedSlot` | （hashtag 单测，本文未运行） |
| I2 | 旧快照可被读取，刷新异步进行 | `clusterStateHolder.Get` L1146 | `TestLazyReloadQueueBehavior` |
| I3 | 并发刷新请求被单飞合并（冷却 200ms） | `LazyReload` L1111 | `TestLazyReloadQueueBehavior/*`（含 `CascadingSMIGRATEDScenario`） |
| I4 | Pipeline 按节点并行、只重排失败子集 | `processPipeline` L1788 | `TestTruthClusterOrderAndDemux`、`TestTruthResponseDemux` |
| I5 | MOVED 刷新拓扑；ASK 不刷新且注入 ASKING | `checkMovedErr` L2125 / `process` L1325 | （集群集成测试，需真实迁移；本文未运行） |
| I6 | 成功命令不重复执行（at-most-once） | `setCmdsErr` L254 / `failedCmds` 语义 / `txReadFatal` | `TestTruthRetriesDoNotDuplicate` |
| I7 | Redis 错误隔离，不污染兄弟命令，连接可复用 | `pipelineReadCmds` `isRedisError` 分支 | `TestPipelineWrongTypeNotRetriedIsolated` |
| I8 | 传输错误整批重排、连接作废、副本 MarkAsFailing | `pipelineReadCmds` L2107-L2113 | `TestPipelineRetriesOnNetworkError`、`TestPipelineRetryExhausted` |
| I9 | 上下文取消不重试 | `shouldRetry` L84 | `TestPipelineContextCanceledNotRetried`、`TestTruthCancelNeverLies` |
| I10 | 异构回复按序解码、push 帧先排空不错位 | `pipelineReadCmds` + `processPendingPushNotificationWithReader` | `TestPipelineHeterogeneousReplyDemux*`、`Test*PipelineReadDrainsPushMidBatch` |
| I11 | TxPipeline 强制同槽，跨槽 `ErrCrossSlot` | `processTxPipeline` L2289 | （集群集成测试，需真实集群；本文未运行） |
| I12 | NoRetry 命令不重试、失败前缀后停止 | `cmdsContainNoRetry` L276 | `TestNoRetryRunsIsolatePolicyAndPreserveOrder`、`TestRetryRunsStopAfterFailedPrefix` |
| I13 | node hook 在 next 前读结果不死锁 | `processPipelineNode` executed 守卫 | `TestClusterNodeHookResultPeekNoDeadlock` |
| I14 | AutoPipeline 复用 processPipeline，Close 干净排空 | `dispatchCmds`/`AutoPipeliner.Close` | `TestTruthCloseDrains`、`TestTruthRetriesDoNotDuplicate` |

---

## 12. 时序图

### 12.1 正常路径（无重定向）

```
调用方         ClusterClient        clusterState        节点A(主)      节点B(主)
  |  Pipeline    |                     |                  |             |
  |  .Exec() --> |                     |                  |             |
  |              | mapCmdsByNode ----> | Get()(快照)      |             |
  |              | <--- {A:[c1,c3], B:[c2]} ------------- |             |
  |              | go processPipelineNode(A) -----------> |             |
  |              | go processPipelineNode(B) --------------------------> |
  |              |   写 c1,c3 --------------------------> |             |
  |              |   写 c2 --------------------------------------------> |
  |              |   读 c1,c3 回复 <---------------------- |             |
  |              |   读 c2 回复 <--------------------------------------- |
  |              | failedCmds 为空 → 完成                  |             |
  | <-- 结果 ----|                     |                  |             |
```

### 12.2 MOVED 路径（槽已迁移）

```
调用方      ClusterClient       clusterState      节点A(旧主)    节点B(新主)
  |  .Exec()-->|                    |                |             |
  |            | mapCmdsByNode ---> | 旧快照 → A     |             |
  |            | 写/读 c1 --------> |                |             |
  |            |   c1 回复: -MOVED 5000 B:6379 <---- |             |
  |            | checkMovedErr:                       |            |
  |            |   state.LazyReload() -----> [异步刷新, 200ms冷却]  |
  |            |   failedCmds.Add(B, c1)              |            |
  |            | 第2轮: 写/读 c1 -------------------------------> |
  |            |   c1 回复: OK <-------------------------------- |
  |            | failedCmds 为空 → 完成                |            |
  | <-- 结果---|  (成功命令 c2,c3 不重发)             |            |
```

### 12.3 ASK 路径（槽迁移进行中）

```
调用方      ClusterClient       节点A(migrating 源)   节点B(importing 目标)
  |  .Exec()-->|                     |                     |
  |            | 写/读 c1 ---------> |                     |
  |            |   c1 回复: -ASK 5000 B:6379 <--- |         |
  |            | checkMovedErr(ASK):                        |
  |            |   [不做 LazyReload]                        |
  |            |   failedCmds.Add(B, ASKING, c1) // 前插 ASKING
  |            | 第2轮: 写 ASKING,c1 ------------------------> |
  |            |   读 +OK(ASKING), c1回复 <------------------- |
  |            | 完成                 |                     |
  | <-- 结果---|                      |                     |
  |            | 注意: 拓扑未刷新，下一条命中同槽的请求仍先打 A
```

---

## 13. 测试运行记录

运行环境：Windows 11 / `go version go1.26.1 windows/amd64`；工作目录
`e:\newGsb\generated\question-exchange\questions\GSB-009\Poseidon`。运行时本机存在可用的
Redis 端点（默认 `:6379`），故依赖服务器的用例真实执行而非 skip；`TestClusterNodeHookResultPeekNoDeadlock`
日志显示成功连上并安装了 3 个 node hook。集群 MOVED/ASK 真实迁移用例（I5、I11）需要
`make docker.start` 起多分片集群并执行 resharding，本次未在该环境运行，已在映射表中标注。

### 命令 1 —— 拓扑懒加载单飞

```
go test -run 'TestLazyReloadQueueBehavior' -v -count=1 .
```

结果：**PASS**（`ok github.com/redis/go-redis/v9`）。7 个子用例全部通过：
`SingleReload`、`ConcurrentReloadsDeduplication`、`PendingReloadDuringCooldown`、
`MultiplePendingReloadsCollapsed`、`ReloadAfterCooldownPeriod`、`ErrorDuringReload`、
`CascadingSMIGRATEDScenario`。

### 命令 2 —— 节点 Hook / 重试耗尽 / 上下文取消

```
go test -run 'TestLazyReloadQueueBehavior|TestPipelineRetryExhausted|TestPipelineContextCanceledNotRetried|TestClusterNodeHookResultPeekNoDeadlock' -v -count=1 .
```

结果：**PASS**。关键日志：
`TestClusterNodeHookResultPeekNoDeadlock` → `node hooks installed: 3` / `no hang, err=<nil>`；
`TestPipelineRetryExhausted`、`TestPipelineContextCanceledNotRetried` 均 PASS。

### 命令 3 —— 流水线重试 / 隔离 / 取消 / 关闭 / NoRetry

```
go test -run 'TestPipelineRetriesOnNetworkError|TestPipelineWrongTypeNotRetriedIsolated|TestTruthRetriesDoNotDuplicate|TestTruthCancelNeverLies|TestTruthCloseDrains|TestNoRetryRunsIsolatePolicyAndPreserveOrder|TestRetryRunsStopAfterFailedPrefix' -v -count=1 .
```

结果：**PASS**。关键日志：
`TestTruthCancelNeverLies` → `cancel split: 2000 ok, 0 error (both outcomes legal; lies are not)`；
`TestTruthCloseDrains` → `close drain: 5000/5000 confirmed executed, rest cleanly rejected`；
`TestTruthRetriesDoNotDuplicate` → `at-most-once: 10000 increments == 10000 successful futures`。

### 命令 4 —— 异构回复解码 / push 排空 / 任意节点并发

```
go test -run 'TestPipelineHeterogeneousReplyDemux|TestAutoPipelineHeterogeneousReplyDemux|TestTruthResponseDemux|TestPickArbitraryNodeConcurrent|TestStandalonePipelineReadDrainsPushMidBatch|TestClusterPipelineReadDrainsPushMidBatch' -v -count=1 .
```

结果：**PASS**。覆盖：`TestPipelineHeterogeneousReplyDemux(+RESP2)`、
`TestAutoPipelineHeterogeneousReplyDemux(+RESP2Batch)`、`TestTruthResponseDemux`、
`TestStandalonePipelineReadDrainsPushMidBatch`、`TestClusterPipelineReadDrainsPushMidBatch`、
`TestPickArbitraryNodeConcurrent`。

> 说明：以上命令均只读地运行既有测试，未修改任何 Go 源码或测试。集群真实迁移相关的
> MOVED/ASK 端到端断言（I5、I11）依赖 `make docker.start` 的多分片集群，未在本次环境执行，
> 复盘若需覆盖请在具备 Docker 集群栈的机器上补跑。
```
