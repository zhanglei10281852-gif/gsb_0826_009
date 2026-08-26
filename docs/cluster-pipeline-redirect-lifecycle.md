# Redis Cluster 扩缩容期间 Pipeline 重定向生命周期分析

> 用途：线上扩缩容期间少量流水线请求延迟升高的故障复盘参考范围：go-redis
> v9（本仓库工作区）：`ClusterClient`
> 路由、Pipeline/TxPipeline/AutoPipeline 执行与重试边界约定：文中【源码】= 直接读代码确认的事实；【测试】= 有回归测试佐证；【推断】= 由代码行为推出、未找到直接测试或注释佐证的结论，需在复盘中单独验证。

---

## 1. 入口地图：单命令与 Pipeline 从哪进

| 公开入口                                               | 直达的内部路径                                                                                                  | 位置                                                                                   |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ClusterClient.Get/Set/...`（cmdable）                 | `cmdable = c.Process` → `processHook` → `process`                                                               | [osscluster.go](../osscluster.go#L1222), [osscluster.go](../osscluster.go#L1292-L1298) |
| `ClusterClient.Pipeline().Exec(ctx)`                   | `Pipeline.Exec` → `c.processPipelineHook` → hooks → `c.processPipeline`                                         | [pipeline.go](../pipeline.go#L104-L113), [osscluster.go](../osscluster.go#L1627-L1633) |
| `ClusterClient.TxPipeline().Exec(ctx)`                 | `Pipeline.Exec` → 闭包 `wrapMultiExec` → `c.processTxPipelineHook` → `c.processTxPipeline`                      | [osscluster.go](../osscluster.go#L2153-L2162)                                          |
| `ClusterClient.AutoPipeline()` / `AsyncAutoPipeline()` | `AutoPipeliner.Process` → 分片入队 → flusher → `dispatchCmds` → `withProcessPipelineHook` → `c.processPipeline` | [autopipeline.go](../autopipeline.go#L2071-L2104)                                      |

【源码】三条 Pipeline 路径最终共享同一个执行内核 `ClusterClient.processPipeline`（TxPipeline 是
`processTxPipeline`，但单命令 ASK 重试借用的是节点级 `Pipeline`，见 §5）。hooks 链的初始末端在
`initHooks` 里分别接 `c.process` / `c.processPipeline` /
`c.processTxPipeline`（[osscluster.go](../osscluster.go#L1223-L1228)），用户 hook 通过 `AddHook`
按 FIFO 顺序包裹（[redis.go](../redis.go#L226-L236)：`withProcessPipelineHook`
从 slice 末尾向前包，先注册的最外层）。

## 2. 槽位计算与节点选择

### 2.1 槽位计算

【源码】槽位链路：

1. `ClusterClient.cmdSlot(cmd, -1)`（[osscluster.go](../osscluster.go#L2892-L2909)）：`prefferedSlot == -1`
   时先查命令上的槽位缓存 `cmd.cachedSlot()`，命中即用；否则计算后由 `setCachedSlot`
   写回。该缓存与 AutoPipeline 的分片路由和落盘路由（`mapCmdsByNode`）共享一次计算，不重复算两次。
2. `cmdFirstKeyPosWithInfo`（[command.go](../command.go#L300-L331)）定位第一个 key：优先命令自带
   `firstKeyPos`；`keylessCommands`
   表（PING/TIME/CONFIG/WAIT/MULTI/EXEC/ASKING 等，[command.go](../command.go#L26-L74)）与模块静态策略返回 0（无 key）；`eval/evalsha`
   由运行时 `numkeys` 参数决定返回 3 或 0；`cluster getkeysinslot/countkeysinslot` 特判直接把
   `args[2]` 当槽位（[osscluster.go](../osscluster.go#L2914-L2920)）。
3. 无 key 时槽位为 `-1`，是"交给
   `ShardPicker`"的哨兵值（[osscluster.go](../osscluster.go#L2922-L2932)），不是随机槽。
4. 有 key 时 `hashtag.Slot(key)`：`key == ""` 返回随机槽（避免空 key 确定性集中）；否则取第一个配对
   `{...}`
   内的子串（无配平花括号取全 key），`CRC16(key) % 16384`（[internal/hashtag/hashtag.go](../internal/hashtag/hashtag.go#L49-L88)）。

【测试】`internal/hashtag` 包测试（本次运行 `go test ./internal/hashtag/...`
PASS）覆盖 CRC16 与 hashtag 提取；`osscluster_test.go` 的 `returns CrossSlot error` 以
`hashtag.Slot("A{s}") != hashtag.Slot("B{t}")` 反证 tag 语义（需真实集群）。

### 2.2 集群状态快照读取

【源码】`clusterStateHolder`（[osscluster.go](../osscluster.go#L1086-L1165)）：

- `state` 是 `atomic.Value`，读快照无锁。`Get(ctx)` 返回当前快照；首次为空则同步
  `Reload`；快照年龄超过
  `ClusterStateReloadInterval`（默认 60s，[osscluster.go](../osscluster.go#L290-L292)）则触发一次
  `LazyReload`，**仍返回旧快照**（懒加载不阻塞调用方）。
- 快照内含 `Masters`、`Slaves` 与按区间排序的槽位表；`slotNodes(slot)`
  二分查找槽位所在区间（[osscluster.go](../osscluster.go#L1070-L1082)），`nodes[0]`
  是主节点，其余为副本。

### 2.3 选主还是选只读副本

【源码】单命令路径 `cmdNode` /
`cmdNodeWithShardPicker`（[osscluster.go](../osscluster.go#L2934-L2996)）：

- `ReadOnly=false`（默认）：一律 `state.slotMasterNode(slot)` 即 `slotNodes(slot)[0]`；槽位无映射时
  `nodes.Random()` 兜底。
- `ReadOnly=true` 且命令在 `cmdsInfoCache` 中标为只读：走 `slotReadOnlyNode`，按
  `RouteByLatency`（`slotClosestNode`，全部副本失效时退到最低延迟的失效副本，再退到随机节点）、`RouteRandomly`（`slotRandomNode`）、配置了
  `ShardPicker`（`slotShardPickerSlaveNode`，全部副本失效/加载中退主节点）的顺序裁决；默认
  `slotSlaveNode`（副本失效或 LOADING 退主节点）。
- 槽位 `-1`（无 key 命令）：`ShardPicker` 在 `state.Masters` 上选（单命令）或在 `Masters+Slaves`
  上选（只读批，`mapCmdsByNode`
  只读分支，[osscluster.go](../osscluster.go#L1882-L1895)）。`ShardPicker` 实现在
  `internal/routing/shard_picker.go`（round-robin 默认 / random / static）。

【源码】Pipeline 的节点选择在 `mapCmdsByNode` 里按**整个批次是否全部只读**二选一：`ReadOnly=true` 且
`cmdsAreReadOnly` 为真时全批走 `slotReadOnlyNode`；其余情况（含
`ReadOnly=false`，或批内混任一写命令）全部钉在主节点（[osscluster.go](../osscluster.go#L1860-L1905)、[osscluster.go](../osscluster.go#L1907-L1944)）。【推断】混合读写的 Pipeline 在
`ReadOnly=true` 下也享受不到副本分流，这与单命令逐命令决策不同，复盘"副本利用率为零"时应先查这一点。

## 3. Pipeline 的拆分、执行与异构回复读取

### 3.1 按节点拆分

【源码】`processPipeline`（[osscluster.go](../osscluster.go#L1788-L1852)）：

1. `mapCmdsByNode` 把命令按目标节点聚合成
   `cmdsMap{node → []Cmder}`（线程安全 map，[osscluster.go](../osscluster.go#L3076-L3091)）。映射失败（含策略拒绝，见下）时
   `setCmdsErr` 给**全部**命令盖章后整体返回——全有或全无，任一命令未发出。
2. 策略闸门：命令经 `cmdInfoResolver.GetCommandPolicy` 解析出
   `RequestPolicy`；`ReqAllNodes/ReqAllShards/ReqMultiShard` 的 `CanBeUsedInPipeline()`
   为 false（[internal/routing/policy.go](../internal/routing/policy.go#L137-L139)），映射阶段直接报错
   `cannot pipeline command ...` 并盖到每条命令上。
3. 重试循环 `for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++`（默认
   `MaxRedirects=3`，[osscluster.go](../osscluster.go#L213-L218)）：每轮为每个节点起一个 goroutine 跑
   `processPipelineNode`，`wg.Wait()` 后把 `failedCmds` 作为下一轮输入；`failedCmds`
   为空即结束。attempt>0 时先
   `internal.Sleep(ctx, retryBackoff(attempt))`（10ms 起步、上限 1s 的指数退避，[osscluster.go](../osscluster.go#L265-L276)、[osscluster.go](../osscluster.go#L2811-L2813)）。

【源码】注意与单命令的关键差异：**Pipeline 对 MOVED/ASK 跟随也要先过退避**。单命令路径的 `moved/ask`
标志会跳过 `Sleep`（[osscluster.go](../osscluster.go#L1307)），而 `processPipeline` 的 `attempt > 0`
分支无条件 sleep（[osscluster.go](../osscluster.go#L1811-L1819)）。TxPipeline 则恢复了"重定向立即跟随"（`redirected`
标志，[osscluster.go](../osscluster.go#L2306)）。【推断】扩缩容期间普通 Pipeline 每次重定向至少多付一次
`retryBackoff(1)`（默认约 10ms），这是"少量流水线请求延迟升高"的一个具体来源：不是重定向本身慢，而是重试循环在跟随 MOVED/ASK 之前先睡了退避。

### 3.2 借用连接与写入

【源码】`processPipelineNode`（[osscluster.go](../osscluster.go#L1956-L2013)）：

- 先跑节点级 `withProcessPipelineHook`（`OnNewNode`
  注册的 hook，如 redisotel 的 tracing，就在这里生效）；hook 不调 `next`
  且返回 nil 视为"自行服务成功"，返回 error 视为蓄意中止，均不重映射重试。
- 经 `node.Client.withPipelineConn`
  借连接（[redis.go](../redis.go#L1217-L1262)）：默认每节点有独立 pipeline 池（`PipelinePoolSize<0`
  可关闭），池满（`ErrPoolTimeout`/`ErrPoolExhausted`）或初始化失败时"溢出"到主池；归还走共享的
  `releaseConnToPool`（[redis.go](../redis.go#L1165-L1188)）：`isBadConn` 为真时 `Remove`
  销毁，否则排干 push 通知后 `Put` 回池。借连接失败（fn 未执行）且非上下文错误时
  `node.MarkAsFailing()`，整批重新映射进 `failedCmds`。
- `processPipelineNodeConn`（[osscluster.go](../osscluster.go#L2015-L2071)）在
  `cn.WithWriter(WriteTimeout)` 内先经 HIMPORT 注入命令再
  `writeCmds(wr, cmds)`（[command.go](../command.go#L284-L295)：`wr.WriteArgs(cmd.Args())`
  逐条写 RESP）：**写失败是整批事件**：`isBadConn` 则标记节点失效；`shouldRetry(err, true)` 且批内无
  `NoRetry` 命令时整批重映射进 `failedCmds`；`setCmdsErr(cmds, err)`
  只给尚无错的命令盖章（`setCmdsErr` 有 `rawErr()==nil`
  守卫，[command.go](../command.go#L253-L260)）。

### 3.3 按顺序读取异构回复

【源码】`pipelineReadCmds`（[osscluster.go](../osscluster.go#L2073-L2123)）：

- 逐命令
  `cmd.readReply(rd)`：回复与命令**按下标一一配对**，异构类型（bulk/array/int/map/nil/double/大二进制）由每条命令自己的解析器消费，读指针天然对齐；每条之前先
  `processPendingPushNotificationWithReader` 排干 RESP3
  push 帧，防止 push 帧被当成某条命令的回复导致整体错位（这是本仓库修过的真实缺陷，见下测试）。
- 每条 `cmd.SetErr(err)`。`err==nil`
  继续下一条——**成功的命令留在原批，不进 failedCmds，不会被重复执行**。
- `checkMovedErr`（[osscluster.go](../osscluster.go#L2125-L2150)）：MOVED → `c.state.LazyReload()` +
  `failedCmds.Add(新节点, cmd)`；ASK →
  `failedCmds.Add(新节点, NewCmd("asking"), cmd)`（asking 命令与业务命令入同一节点桶，下一轮同批发往新节点）。**只有被重定向的命令进入下一轮**，同批其余命令已拿到正确回复。
- 传输错误（非 Redis 错误）：`setCmdsErr(cmds[i+1:], err)`
  给**尚未读回复的命令**盖章，整批重映射（若可重试），本轮终止。注意重映射的是完整 `cmds` 而非
  `cmds[i:]`——已成功的命令也会出现在 failedCmds 里，见 §6.3 的幂等性讨论。
- Redis 错误（`isRedisError`，如 WRONGTYPE）：既不标节点失效也不重排，错误留在该命令上，继续读下一条。
- 循环结束后兜底：`cmds[0].rawErr()` 若是可重试错误（典型：首条回复是 LOADING）则整批重排再试一轮。

【测试】`TestPipelineHeterogeneousReplyDemux`（RESP3）与同名 RESP2 变体（[pipeline_exec_test.go](../pipeline_exec_test.go#L83-L132)）固化 9 种异构回复不串位；`TestClusterPipelineReadDrainsPushMidBatch`（[autopipeline_internal_test.go](../autopipeline_internal_test.go#L527-L548)）用 net.Pipe 注入 push 帧证明集群读循环不错位；`TestPipelineWrongTypeNotRetriedIsolated`（[pipeline_exec_test.go](../pipeline_exec_test.go#L282-L309)）证明 WRONGTYPE 只污染自己、兄弟命令正常。

### 3.4 时序图一：正常批次

```
调用方         processPipeline        mapCmdsByNode      节点A goroutine            节点B goroutine
  |  Exec(cmds)      |                     |                    |                         |
  |----------------->|  state.Get(快照)    |                    |                         |
  |                  |-------------------->| slot→node 分组     |                         |
  |                  |  cmdsMap{A:[c1 c3], B:[c2]}             |                         |
  |                  |---------- 每节点起 goroutine ---------->|                         |
  |                  |                     |  withPipelineConn  |  withPipelineConn       |
  |                  |                     |  WithWriter:       |  WithWriter:            |
  |                  |                     |   write c1,c3      |   write c2              |
  |                  |                     |  WithReader:       |  WithReader:            |
  |                  |                     |   read c1,c3 顺序读|   read c2               |
  |                  |                     |  归还连接(Put)     |  归还连接(Put)          |
  |                  |<--------- wg.Wait(), failedCmds 为空 ---|                         |
  |  cmds, nil       |                     |                    |                         |
  |<-----------------|                     |                    |                         |
```

## 4. 拓扑懒加载：并发刷新如何合并

【源码】`LazyReload`（[osscluster.go](../osscluster.go#L1111-L1144)）：

- `reloading` 原子 CAS(0→1) 抢刷新权，抢不到的调用方把 `reloadPending`
  置 1 直接返回——**多个并发 MOVED 只产生一个在跑的刷新 goroutine**。
- 抢到者后台执行 `Reload`（同步 `loadState`）→ 清 pending → **冷却 200ms**
  → 检查 pending：有则再刷一轮，无则复位退出。冷却窗口把扩缩容期间的密集 MOVED/SMIGRATED 通知折叠成少量刷新。
- `loadState`（[osscluster.go](../osscluster.go#L1576-L1625)）：优先 `opt.ClusterSlots`
  回调；否则按随机排列遍历已知地址逐个 `CLUSTER SLOTS`，首个成功者构建新状态；全部失败时清空
  `activeAddrs`（让初始种子地址重新 DNS 解析）并返回首个错误。
- 触发源：MOVED（单命令/普通 Pipeline/TxPipeline 三处）、`Get` 发现快照过期、`ReloadState()`
  手动、维护通知子系统的 SMIGRATED 回调（[osscluster.go](../osscluster.go#L1233-L1248)）。**ASK 不触发刷新**（见 §5）。

【源码】旧状态语义：`Get`
返回的是"最后一次成功刷新"的快照；刷新在后台进行时，路由继续用旧快照——MOVED 重试路径不等刷新落地，直接用错误里的权威地址
`nodes.GetOrCreate(addr)`
获取节点（[osscluster.go](../osscluster.go#L1375-L1379)、[osscluster.go](../osscluster.go#L2133)）。刷新失败时旧快照继续服役，下次触发再来。

【测试】`TestLazyReloadQueueBehavior`
七个子测试（[osscluster_lazy_reload_test.go](../osscluster_lazy_reload_test.go)）：并发去重（50 并发→1 次）、冷却期 pending 合并（多次触发→恰好 2 次）、错误后可再触发、及 SMIGRATED 级联折叠（→2 次）。本次运行全部 PASS（命令见 §10）。

## 5. MOVED 与 ASK 的差异

| 维度                      | MOVED                                               | ASK                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 语义                      | 槽位**永久**迁走                                    | 槽位**迁移中**，目标节点临时代管                                                                                                                                                                                                                                                                                                                                                                                                 |
| 拓扑刷新                  | 触发 `LazyReload`（异步、合并）                     | **不触发**，一次性重定向                                                                                                                                                                                                                                                                                                                                                                                                         |
| 目标节点                  | `nodes.GetOrCreate(addr)` 指向 MOVED 携带的权威地址 | 同样指向 ASK 携带的地址，但只在本次重试生效，不写入状态                                                                                                                                                                                                                                                                                                                                                                          |
| 前置命令                  | 无                                                  | 下一轮发往新节点前先发 `ASKING`：单命令用节点级 mini-pipeline 发 `asking+cmd`（[osscluster.go](../osscluster.go#L1325-L1330)）；普通 Pipeline 把 `asking` 插入同一 failedCmds 桶随批发出（[osscluster.go](../osscluster.go#L2144-L2147)）；TxPipeline 把 ASKING 放在 MULTI **之外**（MULTI 里 ASKING 会被排队、EXEC 前标志未置位，[osscluster.go](../osscluster.go#L2202-L2205)、[osscluster.go](../osscluster.go#L2400-L2403)） |
| 重试时机（单命令/Tx）     | 立即，无退避                                        | 立即，无退避                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 重试时机（普通 Pipeline） | 下一轮 attempt，先过 `retryBackoff`                 | 同左                                                                                                                                                                                                                                                                                                                                                                                                                             |

### 5.1 时序图二：MOVED（Pipeline 视角）

```
节点A(旧主)          processPipeline attempt0        LazyReload后台         节点B(新主,MOVED addr)
   |  read c2 → -MOVED 3999 B         |                    |                     |
   |<---------------------------------|                    |                     |
   |                     | checkMovedErr: LazyReload()     |                     |
   |                     | failedCmds{B:[c2]}               |                     |
   |                     |------------------------------->| Reload(loadState)   |
   |                     | 其余命令本轮已成功,不入列         |                     |
   |                     | sleep retryBackoff(1)           |                     |
   |                     | attempt1: mapCmdsByNode(B:[c2]) |                     |
   |                     |--------------------------------------------------------> write c2
   |                     |<-------------------------------------------------------- 回复 c2
   |                     | 成功,不再重排                      |                     |
```

【源码】关键点：MOVED 携带的地址来自 Redis 错误本身（`isMovedError` 先查
`proto.IsMovedError/IsAskError` 类型化错误并调 `Addr()`，兼容纯字符串解析，`internal.GetAddr`
归一化，[error.go](../error.go#L230-L265)），不依赖尚未完成的状态刷新；若 MOVED 指向刚用过的同一地址，`isBadConn`
的 `isMovedSameConnAddr`
分支会把连接当坏连接销毁以强制 DNS 重解析（[error.go](../error.go#L209-L213)）。

### 5.2 时序图三：ASK（Pipeline 视角）

```
节点A(迁出方)        processPipeline attempt0            节点B(迁入方,ASK addr)
   |  read c2 → -ASK 3999 B       |                            |
   |<-----------------------------|                            |
   |                   | checkMovedErr: 不 LazyReload           |
   |                   | failedCmds{B:[asking, c2]}            |
   |                   | sleep retryBackoff(1)                 |
   |                   | attempt1: 同桶顺序写出                  |
   |                   |--------------------------------------> ASKING; c2
   |                   |<-------------------------------------- +OK; 回复 c2
   |                   | 后续命令/批次仍按旧快照路由到 A,          |
   |                   | 直到迁移完成后的 MOVED 或定时刷新更新拓扑   |
```

【推断】因为 ASK 不刷新拓扑，扩缩容/迁移窗口内每个命中迁移槽的 Pipeline 都至少多付一次"ASK 往返 + 退避 +
ASKING 重试"；只有迁移完成（MOVED）或快照过期刷新后路由才稳定到新主。若观测到延迟毛刺集中在迁移窗口且呈"每个批次偶发一条"，与 ASK 路径的开销形状一致。

### 5.3 成功命令为何不重复执行

【源码】三个互相独立的保证：

1. `failedCmds`
   只收集需要再执行的命令：MOVED/ASK 各收自己（[osscluster.go](../osscluster.go#L2099-L2101)）；传输错误重排整批但那是回复未读全、执行状态未知的保守路径（§6.3）。
2. `setCmdsErr` 只给 `rawErr()==nil`
   的命令盖章，已成功命令的值不被覆盖（[command.go](../command.go#L253-L260)）。
3. 重定向只重排**收到 MOVED/ASK 的那一条**，同批已读到正常回复的命令不带入下一轮——服务器端这些命令只执行了一次，客户端也不会重发。

## 6. 故障与重试决策表

【源码】判定函数：`shouldRetry`（[error.go](../error.go#L84-L175)）、`isRedisError`（[error.go](../error.go#L177-L186)）、`isBadConn`（[error.go](../error.go#L188-L228)）、`cmdsContainNoRetry`（[command.go](../command.go#L275-L282)）、节点失效标记
`MarkAsFailing`、连接去留 `releaseConnToPool`（[redis.go](../redis.go#L1165-L1188)）。

### 6.1 单命令（`process`）与 Pipeline 逐命令读循环的判定

| 错误类别                  | 典型                                                                            | 重试？                                                                                       | 换节点？                 | 拓扑刷新        | 节点标记失效                                      | 连接去留                        | 剩余/同批命令                                                        |
| ------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ------------------------ | --------------- | ------------------------------------------------- | ------------------------------- | -------------------------------------------------------------------- |
| MOVED                     | `-MOVED 3999 B`                                                                 | 立即，计入 MaxRedirects                                                                      | 是，定向到 B             | 触发 LazyReload | 否                                                | 复用（同地址 MOVED 除外，销毁） | 仅该命令重排                                                         |
| ASK                       | `-ASK 3999 B`                                                                   | 立即                                                                                         | 是，ASKING+B             | 不触发          | 否                                                | 复用                            | 仅该命令重排                                                         |
| READONLY                  | 写到副本                                                                        | 单命令：是，`node=nil` 重选并触发 LazyReload；Pipeline 读循环内按 Redis 错误隔离，不整批重试 | 单命令换节点             | 单命令触发      | 否                                                | 销毁（isBadConn 特例）          | 仅该命令                                                             |
| LOADING                   | 副本加载中                                                                      | 是（`ReadOnly` 时换节点；Pipeline 首条 LOADING 整批重排）                                    | 视路径                   | 否              | 单命令 ReadOnly 时标记                            | 复用                            | 首条触发时整批                                                       |
| 传输错误（EOF/dial/超时） | 断连、写入失败                                                                  | `shouldRetry`：EOF/dial/池超时恒真；`net.Error.Timeout()` 看 `retryTimeout` 参数             | attempt≥1 后标记失效重选 | 否              | 是：借失败、ReadOnly 读坏连接时标记               | 销毁（isBadConn）               | Pipeline：整批重排（读阶段失败时含已读成功命令，见 §6.3 幂等性风险） |
| Redis 错误                | WRONGTYPE、arity、NOPERM                                                        | 否                                                                                           | 否                       | 否              | 否                                                | 复用（连接不脏）                | 错误隔离在本命令，继续读                                             |
| 上下文取消/超时           | ctx.Canceled / DeadlineExceeded                                                 | 否（`shouldRetry` 显式 false）                                                               | 否                       | 否              | **否**（`isContextError` 分支跳过 MarkAsFailing） | 销毁                            | Pipeline：未读命令盖 ctx 错，整轮终止                                |
| NoRetry 命令              | `RawWriteToCmd`、`ZeroCopyStringCmd`（[command.go](../command.go#L1015-L1119)） | 整批含其一即禁重试                                                                           | 否                       | 否              | 否                                                | 否                              | 出错即终止，不重试                                                   |
| 重试耗尽                  | attempt > MaxRedirects                                                          | 否                                                                                           | 否                       | 否              | 否                                                | 否                              | `lastErr` 盖到仍无错的命令；`Exec` 返回 `cmdsFirstErr`               |

### 6.2 上下文取消的专门语义

【源码】取消不重试、不污染节点健康度，但有三个细节：①`internal.Sleep` 在退避期间感知取消，直接
`setCmdsErr` 后返回（[osscluster.go](../osscluster.go#L1811-L1819)）；②借连接失败属上下文错误时不
`MarkAsFailing`（[osscluster.go](../osscluster.go#L1985-L1991)）；③独立客户端路径在 `MaxRetries`
耗尽时会把 `lastErr`
补盖到每条命令上（[redis.go](../redis.go#L1840-L1849)，注释明确为 AutoPipeline 兜底——它没有 Exec 返回值可呈现错误）。

【测试】`TestPipelineContextCanceledNotRetried`（PASS）、Ginkgo 规格
`should not fail node with context.Canceled/DeadlineExceeded error`（[osscluster_test.go](../osscluster_test.go#L722-L753)，需真实集群）分别钉住"不重试"与"不标节点失效"。

### 6.3 传输错误与幂等性（复盘重点）

【源码】写阶段失败：命令未上 wire，整批重排安全。读阶段失败：回复未读全，服务器**可能已执行部分或全部命令**，此时集群 Pipeline 重排的是完整
`cmds`（[osscluster.go](../osscluster.go#L2108-L2111)）而非未读后缀——`INCR/APPEND`
这类非幂等命令存在被重放的风险窗口。TxPipeline 在此恰恰相反：写入后读失败一律 `txFatal`
不重试（[osscluster.go](../osscluster.go#L2588-L2595)，注释明说防止事务双提交），错误直接上浮、连接以
`errTxDirtyConn` 销毁。

【推断】扩缩容期间延迟升高若伴随 `INCR`
类计数偏大，应优先怀疑"普通 Pipeline 读超时→整批重放"，而不是重定向逻辑；这是普通 Pipeline 与 TxPipeline 在故障语义上的实质分叉点。

## 7. 三种流水线的保证对照

| 维度           | 普通 Pipeline                                            | TxPipeline                                                                                                                                                                                                                                                                         | AutoPipeline（阻塞/异步）                                                                                                                                                                                                                   |
| -------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 槽位约束       | 无；按节点拆分并发执行                                   | 全部带 key 命令同槽，跨槽 `ErrCrossSlot` 预检失败并盖到每条命令（[osscluster.go](../osscluster.go#L2280-L2293)）；纯无 key 批次随机槽                                                                                                                                              | 无（同普通 Pipeline）；跨槽命令自然按槽分片                                                                                                                                                                                                 |
| 原子性         | 无                                                       | 有（MULTI/EXEC），N+2 条回复恒定，重定向/中止路径连接都干净（[osscluster.go](../osscluster.go#L2168-L2208)）                                                                                                                                                                       | 无                                                                                                                                                                                                                                          |
| 执行并发与顺序 | 节点间并发；单节点内保持入队顺序                         | 单节点串行事务                                                                                                                                                                                                                                                                     | 同 key 命令因槽路由落同一分片保持顺序；跨节点并发（[osscluster.go](../osscluster.go#L1691-L1744)：`slot*nShards/16384` 桶化）；含 NoRetry 命令的混合批按**连续段**拆分顺序派发，不重排（[autopipeline.go](../autopipeline.go#L2079-L2099)） |
| Hook           | 客户端级 1 次 + 每个节点子批各 1 次节点级 hook           | 同左（hook 看到的是含 ASKING/MULTI/EXEC 的 wire）                                                                                                                                                                                                                                  | `dispatchCmds` 直接调客户端 hook 包裹的 `processPipeline`，与 `Pipeline.Exec` 同一入口；hook 短路返回 nil 视为成功自服务，返回 error 且批全成功时把裁决盖到整批（[autopipeline.go](../autopipeline.go#L2100-L2125)）                        |
| 策略闸门       | 映射期全有或全无：含 ReqAll\*/ReqMultiShard 命令整批报错 | 同普通 Pipeline                                                                                                                                                                                                                                                                    | 提交时 preflight 拒绝单个命令，不污染他人合批；ReqSpecial（FT.CURSOR）被 divert 出合批走独立连接（[osscluster.go](../osscluster.go#L1698-L1730)）                                                                                           |
| MOVED/ASK      | 见 §5：逐命令收集、整轮退避、ASKING 入桶                 | `txOutcome` 五分类（success/moved/ask/tryagain/conn/fatal）：MOVED 定向+LazyReload；ASK 顶层 ASKING 重发整事务；TRYAGAIN/CLUSTERDOWN 退避后同节点重试；读阶段 IO 错 fatal 防双提交（[osscluster.go](../osscluster.go#L2251-L2362)、[osscluster.go](../osscluster.go#L2613-L2646)） | 走 `processPipeline`，继承普通 Pipeline 全部重定向语义                                                                                                                                                                                      |
| 读阶段传输错误 | 整批可能重放（非幂等风险）                               | 一律 fatal 不重试                                                                                                                                                                                                                                                                  | 同普通 Pipeline                                                                                                                                                                                                                             |
| 关闭语义       | `Pipeline` 对象不可并发用；`Discard` 清队列              | 同左                                                                                                                                                                                                                                                                               | `Close` 幂等、先停 flusher 再关客户端（`ClusterClient.Close` 先关两个缓存的 AutoPipeliner 再关节点，[osscluster.go](../osscluster.go#L1269-L1279)）；关闭后 getter 拒绝重建；`drainAll` 尽量冲刷未决 future                                 |
| 上下文         | 逐命令 ctx 在批内共享 Exec 的取消语义                    | 同左                                                                                                                                                                                                                                                                               | 入队后逐命令 ctx 不再生效：批次在引擎自有长生命周期上下文上执行；阻塞命令（BLPOP 等，`readTimeout()!=nil`）不合批、直接用调用方 ctx（[autopipeline.go](../autopipeline.go#L516-L523)）                                                      |

## 8. 旧状态、节点失效与重试耗尽：调用方最终看到什么

【源码】

- **旧状态服役**：刷新后台进行时一切路由照旧；MOVED 重试定向到错误内地址，新节点由
  `nodes.GetOrCreate(addr)` 现取现建（节点级 `Client` 惰性创建，`OnNewNode` hook 即装）。
- **节点失效**：`MarkAsFailing`
  只影响选择器（`slotSlaveNode`/`slotRandomNode`/`slotClosestNode`/`slotShardPickerSlaveNode`
  都跳过 failing/loading 节点），不摘拓扑；`slotClosestNode`
  在全副本失效时退到最低延迟失效副本，再 ping 超时退到集群随机节点（[osscluster.go](../osscluster.go#L987-L1028)）——"没有可用节点"被刻意做成最后手段。
- **重试耗尽**：`MaxRedirects`（默认 3）轮跑完仍失败时，`processPipeline` 返回
  `cmdsFirstErr(cmds)`——**每条命令各留自己的错**（MOVED 命令的错误、被整批盖章的传输错误等），`Exec`
  把第一个错误返回给调用方。单命令路径返回 `lastErr` 并已经 `Process` 盖到命令上。`loadState`
  全节点不可达时清空
  `activeAddrs`，后续靠种子地址 DNS 重解析恢复（[osscluster.go](../osscluster.go#L1614-L1624)）。
- **策略拒绝**：不可入 Pipeline 的命令让整个 `Exec` 以
  `cannot pipeline command ...`，每条命令都有错，任何命令未发出（AutoPipeline 侧只拒绝肇事命令）。

【测试】`TestPipelineRetryExhausted`（PASS）验证耗尽后 `Exec` 返回错误且拨号次数 ≥
MaxRetries+1；`TestClusterPipelineNonPipelinableAllOrNothing` /
`TestAPClusterNonPipelinableRejectedAtSubmit`（需真实集群，本次 SKIP）分别钉住两种拒绝语义。

## 9. 关键不变量 → 源码入口 → 回归测试

| #   | 不变量                                                                  | 源码入口                                                                                            | 回归测试（本次运行结果）                                                                                                                                              |
| --- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | 槽位 = CRC16(hashtag) % 16384；无 key 为 -1 哨兵给 ShardPicker          | [hashtag.go](../internal/hashtag/hashtag.go#L76-L82)、[osscluster.go](../osscluster.go#L2922-L2932) | `internal/hashtag` 包测试（PASS）                                                                                                                                     |
| 2   | 状态快照读无锁；过期只触发懒刷新、当前仍用旧快照                        | [osscluster.go](../osscluster.go#L1146-L1157)                                                       | `TestLazyReloadQueueBehavior`（PASS）                                                                                                                                 |
| 3   | 并发 MOVED 只刷一次；冷却窗 pending 折叠                                | [osscluster.go](../osscluster.go#L1111-L1144)                                                       | 同上 7 子测试（PASS）                                                                                                                                                 |
| 4   | Pipeline 按节点分组、节点内顺序、异构回复按下标配对                     | [osscluster.go](../osscluster.go#L1854-L1944)、[osscluster.go](../osscluster.go#L2073-L2123)        | `TestPipelineHeterogeneousReplyDemux`（需 Redis，SKIP）；`TestClusterPipelineReadDrainsPushMidBatch`（PASS）                                                          |
| 5   | MOVED 刷新拓扑+定向新节点；ASK 不刷新、前置 ASKING                      | [osscluster.go](../osscluster.go#L2125-L2150)、[osscluster.go](../osscluster.go#L1359-L1381)        | Ginkgo `should follow redirects`（Pipeline/TxPipeline 共用，[osscluster_test.go](../osscluster_test.go#L638-L686)，需集群，SKIP）                                     |
| 6   | 成功命令不进 failedCmds、不被重发、值不被覆盖                           | [osscluster.go](../osscluster.go#L2095-L2101)、[command.go](../command.go#L253-L260)                | `TestPipelineWrongTypeNotRetriedIsolated`（需 Redis，SKIP）                                                                                                           |
| 7   | Redis 错误隔离到本命令且连接可复用                                      | [osscluster.go](../osscluster.go#L2107)、[error.go](../error.go#L203-L216)                          | 同上（SKIP）                                                                                                                                                          |
| 8   | ctx 取消：不重试、不 MarkAsFailing                                      | [osscluster.go](../osscluster.go#L1985-L1991)、[error.go](../error.go#L102-L105)                    | `TestPipelineContextCanceledNotRetried`（PASS）                                                                                                                       |
| 9   | NoRetry 命令禁止整批重试                                                | [command.go](../command.go#L275-L282)、[osscluster.go](../osscluster.go#L2034)                      | —（【推断】未见直接单测；间接由 `RawWriteToCmd`/`ZeroCopyStringCmd` 注释与 autopipeline 分段派发测试覆盖）                                                            |
| 10  | TxPipeline 跨槽客户端预检拒绝                                           | [osscluster.go](../osscluster.go#L2280-L2293)                                                       | Ginkgo `returns CrossSlot error`（需集群，SKIP）                                                                                                                      |
| 11  | TxPipeline 读阶段 IO 错不重试（防双提交）                               | [osscluster.go](../osscluster.go#L2588-L2595)                                                       | —（【推断】由注释与代码路径确认，未见无集群单测）                                                                                                                     |
| 12  | 节点级 hook 短路语义（nil=自服务成功，err=中止不重试）                  | [osscluster.go](../osscluster.go#L1994-L2012)                                                       | `TestClusterNodeHookShortCircuit` / `TestClusterNodeHookPostNextError`（需集群，SKIP）；`TestClusterNodeHookResultPeekNoDeadlock`（需集群，SKIP）                     |
| 13  | 不可入 Pipeline 策略：Pipeline 全有或全无 / AutoPipeline 提交期单点拒绝 | [osscluster.go](../osscluster.go#L1866-L1879)、[osscluster.go](../osscluster.go#L1706-L1716)        | `TestClusterPipelineNonPipelinableAllOrNothing` / `TestAPClusterNonPipelinableRejectedAtSubmit`（需集群，SKIP）；`TestClusterAutoPipelineOptionsShardDefault`（PASS） |
| 14  | 传输错误重排整批（普通 Pipeline）vs 直接 fatal（TxPipeline）            | [osscluster.go](../osscluster.go#L2107-L2113)、[osscluster.go](../osscluster.go#L2469-L2474)        | `TestPipelineRetriesOnNetworkError` / `TestPipelineRetryExhausted`（后者 PASS，前者需 Redis SKIP）                                                                    |
| 15  | 普通 Pipeline 跟随重定向前先退避                                        | [osscluster.go](../osscluster.go#L1811-L1819)                                                       | —（【推断】代码路径明确，无直接测试钉住退避存在）                                                                                                                     |

## 10. 本次实际运行的测试与结果

环境：Windows，go1.26.1；**Docker 未运行**（无法启动 docker-compose 测试栈，`:16600` 集群与 `:6379`
单机均不可达）。命令在工作区根目录执行：

```sh
go test -count=1 -run "TestLazyReloadQueueBehavior" -v .
# PASS：7/7 子测试（SingleReload / ConcurrentReloadsDeduplication /
#   PendingReloadDuringCooldown / MultiplePendingReloadsCollapsed /
#   ReloadAfterCooldownPeriod / ErrorDuringReload / CascadingSMIGRATEDScenario），2.81s

go test -count=1 -run "TestPipelineRetryExhausted|TestPipelineContextCanceledNotRetried" -v .
# PASS：2/2（重试耗尽返回错误且拨号≥3 次；预取消 ctx 不重试且错误落在每条命令上）

go test -count=1 -run "TestClusterAutoPipelineOptionsShardDefault|TestClusterPipelineReadDrainsPushMidBatch|TestStandalonePipelineReadDrainsPushMidBatch" -v .
# PASS：3/3（集群默认多分片且不污染调用方配置；两个 push 帧中途注入不错位）

go test -count=1 ./internal/hashtag/... ./internal/routing/...
# PASS：两个包全量

go test -count=1 -run "TestClusterNodeHookResultPeekNoDeadlock|TestClusterPipelineNonPipelinableAllOrNothing|TestAPClusterNonPipelinableRejectedAtSubmit|TestClusterNodeHookShortCircuit|TestClusterNodeHookPostNextError|TestClusterAutoPipelineBasic|TestClusterAutoPipelineConcurrency|TestClusterAutoPipelineCrossSlot|TestClusterPipelineUsesDedicatedPool" -v .
# SKIP：9/9 —— 需要 :16600-:16602 真实集群（"cluster not reachable"），无失败
```

复盘落地建议（在具备 Docker 的环境补跑）：`make docker.start` 后执行
`go test -run TestGinkgoSuite . -ginkgo.focus="should follow redirects"` 与上表中全部 SKIP 项。

## 11. 与线上现象的对应假设（待确认）

1. 【推断】**普通 Pipeline 的重定向退避**：`processPipeline` 在跟随 MOVED/ASK 前无条件
   `retryBackoff(attempt)`（默认首轮约 10ms，上限 1s 指数增长）。扩缩容期间迁移槽越多，命中重定向的 Pipeline 越多，尾延迟呈离散台阶。单命令与 TxPipeline 无此项。缓解方向：扩缩容窗口内降低迁移并发，或评估是否让 Pipeline 重定向像单命令一样跳过退避（改动前需补测试，见 §9-15）。
2. 【推断】**ASK 窗口的逐批开销**：ASK 不刷新拓扑，迁移窗口内每个命中批次多一次往返+退避；监控上表现为"每个节点桶偶发一条慢命令"而非整批失败。
3. 【源码】**非幂等重放风险**：普通 Pipeline 读阶段传输错误整批重排（含已成功命令）。若故障伴随计数偏大，比对 TxPipeline 路径可隔离此变量。
4. 【源码】**混合读写批全部落主**（`ReadOnly=true` 时）：`mapCmdsByNode`
   按整批只读性二选一，混一个写命令即全批上主节点，副本读分流失效，扩缩容期间主节点压力被人为放大。
