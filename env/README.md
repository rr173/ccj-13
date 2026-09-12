# 在线记录结构迁移切换服务(按业务分组批次 + 计划编排)

页面与服务共同完成**按业务分组、可审计**的结构切换：创建批次 → 冻结窗 → 双读校验 → 一次性原子切换 → 可单独恢复；
管理员还可把多个已有批次编排成**迁移计划**，按唯一顺序与依赖自动推进，失败重试、超限停住、可暂停/恢复/取消。

## 批次模型

- 管理员先**创建批次**：指定业务组名 `biz` 与记录范围 `[id_start, id_end]`（闭区间）。
- 范围之间**不允许重叠**，因此任意记录至多属于一个批次；批次外的记录**正常读写**，不受任何闸门约束。
- 冻结、校验、切换、恢复都**只针对一个批次**：回填、比对、清理都只作用于该批次范围。
- 每个批次有独立的阶段、epoch、水位、freeze_version 与操作者审计，互不影响。

## 状态机(每个批次独立)

```
NORMAL ──freeze──▶ FROZEN ──validate──▶ VALIDATING ──通过──▶ VALIDATED ──cutover──▶ DONE
  ▲                  │                      │ 失败(差异落审计, 保持冻结)      │
  │                  │◀─────────────────────┘                              │
  │                  └────────── recover(必须带 reason) ───────────────────┤
  └────────────────────────────────────────────────────────────────────────┘
```

- **DONE 是终态**，不提供自动回退 —— 从机制上杜绝"两套状态都说自己已切开"。
- 批次内每次迁移动作 `epoch+1`，且以 epoch 为条件更新（栅栏令牌）；Postgres 下另加行锁。
  **两个管理员同时推进同一批次，只有一个能成功**，其余收到 409。
- cutover 是**单事务**（翻阶段 + 生效新结构 + 写审计），崩溃即整体回滚，无半切开状态。
- 校验回填按**水位**（已处理的最大记录 id）断点续跑，upsert 幂等，服务重启后重跑即可。

## 行为约定(按记录所属批次)

| 所属批次阶段 | 写入 | 读取 |
|---|---|---|
| 无批次 / NORMAL | 旧结构可写 | 旧结构 |
| FROZEN / VALIDATING / VALIDATED | **范围内拒绝**（423 + 批次 + freeze_version） | 同一编号**同时返回新旧两份 + 差异** |
| DONE | 范围内新结构可写；**旧路径明确失败**（410） | 新结构 |

- 校验发现批次内不一致：列出差异（记录/字段/旧值/新值），保持冻结，**阻止该批次切换**；cutover 前还会复核一次。
  差异类型：`__missing__`（新表缺行）、字段值不一致、`__extra__`（新表多出旧表没有的记录，切换后会静默生效，同样阻止）。
  **范围外的差异不参与比对**，不影响本批次切换。
- recover：该批次回到冻结前可写状态，同事务清理**本批次范围内**的半迁移数据（新表回填行 + 水位归零），
  **不会清掉其他批次的数据**；原因必填并落审计。
- 审计：批次、操作者、动作、阶段迁移、epoch、应用版本、freeze_version、水位、差异明细、恢复原因，只追加；
  重启时每个在途批次各落一条 boot 审计。

## 迁移计划编排

管理员把**已有批次**组织成计划：每个步骤绑定一个批次，步骤有计划内唯一 `seq` 与 `depends_on`(依赖步骤的 seq)。

- **保存即聚合校验，不通过整体拒绝(不落任何数据)**，原因逐条返回：
  步骤为空 / seq 非正整数或重复 / 批次不存在 / 同一批次被计划内多个步骤重复占用 /
  批次已被其他**未终结计划**占用(完成或已取消的计划释放占用) / 批次已是 DONE 终态 /
  依赖的 seq 不存在 / 自依赖 / 依赖图成环(Kahn 拓扑判定)。
- **依赖门控**：只有依赖步骤全部 SUCCESS 的步骤才会被执行；每步实际调用现有批次流程
  `freeze → validate → cutover`(全部复用批次幂等框架)，按批次**当前阶段**驱动，重复/重启不二次推进。
- **重试与停住**：每步可设 `max_retries`(首次失败后的额外重试次数)。失败自动重试，
  超过次数步骤 `HALTED`、计划 `HALTED` 并记录最近错误，**后续步骤永远不被放行**；
  管理员修复数据后 `resume`：失败步骤计数清零并进入**新一轮尝试**(attempt_round+1，批次幂等键随之换轮)。
- **生命周期**：`start / pause / resume / cancel`，均要求 `idempotency_key` 且重复提交幂等。
  暂停在步骤边界生效；取消把未开始步骤置 SKIPPED(执行中的步骤自然跑完，已 DONE 的批次不回退)。
- **步骤留痕**：步骤表记录状态、尝试次数、尝试轮次、操作者、失败原因；每次尝试另落
  `plan_step_events`(start/retry/success/fail/halted/reset/skip)；计划生命周期落审计。
- **重启安全**：RUNNING 标记在步骤执行前就提交为崩溃边界；重启对账把遗留的 RUNNING 步骤
  复位为 PENDING(计数与批次进度保留, 批次动作本身幂等)，RUNNING 计划由后台 worker 从安全停止点续跑，
  PAUSED/HALTED 等用户态保持不变 —— 计划不会回到错误的"假运行"，也不会丢失已完成步骤。
- 后台单实例 worker(0.5s 轮询，可用 `PLAN_WORKER_POLL_INTERVAL` 调整；`PLAN_WORKER_ENABLED=0` 关闭)，
  每个 tick 每个计划至多推进一步；Postgres 行锁 / SQLite 写锁串行化 worker 与管理操作。

### 计划与步骤状态

```
DRAFT ─start→ RUNNING ─pause→ PAUSED ─resume→ RUNNING
                │  └─某步重试耗尽→ HALTED ─resume→ RUNNING(失败步骤换新一轮重试)
                ├─cancel→ CANCELED(终态, 未开始步骤 SKIPPED)
                └─全部步骤成功→ COMPLETED(终态)

步骤: BLOCKED(依赖未满足) → PENDING/FAILED(待重试) → RUNNING → SUCCESS
                                                  └→ HALTED(超限); 计划取消 → SKIPPED
```

## 运行

```bash
# Docker(推荐, Postgres + 应用)
docker compose up --build

# 本地开发(SQLite)
pip install -r requirements.txt
uvicorn app.main:app --reload
```

控制台： http://localhost:8000 （创建批次 / 每批次阶段·进度·差异·操作 + 写入探测 + 审计日志）

## API 摘要

```
GET  /api/status                                   服务版本 + 全部批次 + 全部计划(步骤/依赖/进度/最近错误)
POST /api/admin/batches          {operator, idempotency_key, biz, id_start, id_end}
GET  /api/admin/batches                            批次列表
GET  /api/admin/batches/{id}                       批次详情(冻结窗内含实时差异)
POST /api/admin/batches/{id}/freeze   {operator, idempotency_key}
POST /api/admin/batches/{id}/validate {operator, idempotency_key}
POST /api/admin/batches/{id}/cutover  {operator, idempotency_key, expected_epoch?}
POST /api/admin/batches/{id}/recover  {operator, idempotency_key, reason}
POST /api/admin/plans   {operator, idempotency_key, name, max_retries,
                         steps:[{seq, batch_id, depends_on:[seq...], max_retries?}]}
GET  /api/admin/plans                              计划列表(含步骤/依赖/事件流水)
GET  /api/admin/plans/{id}                         计划详情
POST /api/admin/plans/{id}/start|pause|resume|cancel   {operator, idempotency_key}
GET  /api/admin/audit?batch_id=&plan_id=           审计日志(可按批次或计划过滤)
POST /api/records                                  旧结构写入(批次冻结期 423 / 批次切换后 410)
POST /api/v2/records                               新结构写入(仅所属批次 DONE)
GET  /api/records/{id}                             按所属批次阶段返回 旧/新/双读+diff
GET  /api/records/{id}/compare                     批次冻结窗内双读比对
```

幂等：所有管理动作要求 `idempotency_key`，重复执行返回首次结果（`replayed: true`），无副作用；
请求哈希包含动作与目标(批次或计划)，同键跨目标/跨动作/不同请求体复用返回 409。重启后重放依然正确。
计划动作与批次动作使用独立命名空间(`plan.*` 与 `freeze/validate/...`)，互不撞键。

## 测试

```bash
python3 -m pytest tests/ -q   # 28 个用例:
# 批次(16): 批次创建与范围重叠拒绝/批次外正常读写/双读差异/范围内外多余记录拦截/
#           单独恢复不清其他批次/幂等重放(含跨批次)/epoch 栅栏双人推进只一人成功/重启保持
# 计划(12): 建计划聚合拒绝(批次不存在/重复占用/跨计划占用/DONE 终态/依赖不存在/成环)/
#           依赖顺序推进/上游失败下游保持 BLOCKED/重试耗尽 HALTED/恢复换轮重试成功/
#           暂停在步骤边界/取消跳过未开始步骤/计划动作幂等/重启 RUNNING 步骤复位并自动续跑/
#           HALTED 重启不偷跑/状态与审计接口
```
