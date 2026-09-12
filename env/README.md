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

## 风险审批与执行窗口

创建计划时可声明**风险等级**与**允许执行的时间窗口**，两者都持久化在库中，服务重启不丢失。

### 风险审批

- `risk_level=LOW`(默认)：无需审批，创建后可直接启动。
- `risk_level=HIGH`：计划创建即 `PENDING`，**必须由不同于创建者的另一名管理员** `approve`
  审批通过后才能启动；创建者审批自己的高风险计划会被 409 拒绝。
- `reject` 拒绝审批**必须带原因**，原因落计划审计、在计划详情中可见，并在启动闸门处阻止启动；
  被拒绝的计划可由另一名管理员重新 `approve` 通过(拒绝原因随之清除)。
- 启动前可 `revoke-approval` 撤销已通过的审批(APPROVED → PENDING)，启动闸门重新关闭。
- 审批 / 拒绝 / 撤销都**只允许在启动前(DRAFT)**操作，计划启动(以及之后的暂停/停住)时状态锁定。
- 重复审批、重复拒绝、重复撤销**幂等**：状态已在目标态时返回 `already_in_state: true`，
  不产生第二次状态变化、不写第二条审计；同键重放走 `replayed: true`。
  审批字段：`approval_status`(NOT_REQUIRED/PENDING/APPROVED/REJECTED)、`approved_by/at`、`reject_reason`。

### 执行窗口

- 创建计划时可给一组闭区间窗口 `windows:[{starts_at, ends_at}]`(ISO 8601，建议 UTC 如
  `2026-09-12T22:00:00Z`)；留空表示**不限制**。窗口开始必须早于结束，非法窗口聚合拒绝、不落数据。
- 计划只在**任一窗口内**推进：worker tick 在步骤边界检查窗口，离开窗口时计划保持 `RUNNING`
  但暂停推进(`window_open=false`，页面显示"窗口外暂停中")，**重新进入窗口后自动继续**，
  进出窗口都写 `plan.window_pause / plan.window_resume` 审计。正在执行的单步会跑完，不会被切到一半。
- 启动时落在窗口外也允许把计划置为 RUNNING，但不推进，窗口到达后自动开跑。
- 启动前(DRAFT)管理员可用 `PUT /api/admin/plans/{id}/window` **整体替换**窗口，传空列表即清空限制；
  与当前完全相同的窗口请求幂等无副作用。启动后窗口锁定不可修改。
- 计划详情实时返回 `windows`、`has_windows`、`in_window`(按当前时刻重新判定)与持久化的
  `window_open`(最近一次运行判定)；重启时 boot 对账按窗口边界重新判定运行态并对跨边界情况补审计。
- 高风险 + 窗口两道闸门叠加：审批决定"能不能启动"，窗口决定"启动后什么时候推进"。

### 计划与步骤状态

```
DRAFT ─start→ RUNNING ─pause→ PAUSED ─resume→ RUNNING
                │  └─某步重试耗尽→ HALTED ─resume→ RUNNING(失败步骤换新一轮重试)
                ├─cancel→ CANCELED(终态, 未开始步骤 SKIPPED)
                └─全部步骤成功→ COMPLETED(终态)

步骤: BLOCKED(依赖未满足) → PENDING/FAILED(待重试) → RUNNING → SUCCESS
                                                  └→ HALTED(超限); 计划取消 → SKIPPED
```

## 迁移回放与报告(只读)

管理员可针对**已有迁移计划**先固化一份**持久化审计检查点**，再选择"计划 + 检查点"创建**回放任务**，
由后台 worker 按**原计划依赖顺序**逐步把检查点快照重放成报告。**回放全程只读**：绝不修改原批次、
计划、业务新旧表与业务审计，只写 `replay_*` 表。

### 审计检查点(replay_checkpoints)

`POST /api/admin/checkpoints {plan_id}` 在创建时刻为计划固化：

- 全局审计游标 `audit_cursor_id`（当时最大 `audit_log.id`）；
- 计划级证据：计划须为 `COMPLETED` 且存在 `plan.create` 审计；
- 逐步骤(批次)证据与快照：步骤须 SUCCESS、批次仍存在、批次有 `freeze` 与 `cutover` 审计
  （记录关键审计 id，回放时重新核验存在性），并固化批次行（阶段/epoch/水位/freeze_version/
  active_schema）与范围内**旧表全量快照**（预期状态的唯一来源）及当时新表产出快照。
- 任何计划都允许固化：审计不完整时检查点状态为 **INCOMPLETE** 并逐条列出原因（仍持久化、可查看），
  **只有 COMPLETE 检查点可用于回放**；用 INCOMPLETE 检查点创建回放会被 409 明确拒绝。
- 检查点只追加、不可变。

### 回放任务状态机

```
QUEUED ──获得并发额度──▶ RUNNING ──全部步骤报告完成──▶ COMPLETED(终态)
  ▲                        │
  └──── resume(重新排队) ── PAUSED ◀── pause(步骤边界)
                           │
                           ├─ cancel ──▶ CANCELED(终态, 未开始步骤 SKIPPED)
                           └─ 检查点缺失/审计不完整/快照无法还原/批次已不存在 ──▶ FAILED(终态)
```

- **排队与并发**：任务创建即 `QUEUED`；同时处于 RUNNING 的回放不超过 `REPLAY_MAX_CONCURRENCY`
  （默认 2，至少 1），超出排队；暂停后 `resume` 重新进入 QUEUED，再次受并发闸门约束。
  worker 调度在 Postgres 下用咨询锁串行（SQLite 写事务天然串行），不会越过上限或重复执行。
- **暂停/恢复/取消**：`pause` 在步骤边界生效（页面显示进度、当前步骤、差异数量、最近错误）；
  `cancel` 把未开始步骤置 SKIPPED；**已完成步骤的报告在 FAILED/CANCELED 后仍然保留**。
- **明确失败（FAILED，原因带错误码，未执行步骤 SKIPPED）**：
  - `checkpoint_missing`：检查点已被删除；
  - `checkpoint_incomplete`：检查点审计状态不完整；
  - `audit_incomplete`：检查点记录的 freeze/cutover 关键审计在回放时已不存在（审计被删）；
  - `snapshot_unrecoverable`：步骤的旧表快照为空/损坏，批次数据已无法还原；
  - `batch_missing`：批次业务行已不存在。
  计划不存在/检查点不存在则在**创建回放时**直接 404；检查点不属于该计划则 409。
- **重启安全**：RUNNING 是崩溃边界（标记先提交、报告后提交）。重启对账把遗留 RUNNING 任务
  复位到 **QUEUED**（安全的待执行位置）、其遗留 RUNNING 步骤复位为 PENDING（报告未提交，回放只读安全重跑），
  SUCCESS 报告原样保留，worker 自动续跑。
- **幂等**：检查点固化与回放创建/暂停/恢复/取消都要求 `idempotency_key`（`replay.*` 命名空间）；
  同一(计划,检查点)重复创建**幂等返回已有非终态任务**（`already_active:true`，不产生第二个任务），
  重复控制请求回显首次结果无副作用。
- **报告**：`GET /api/admin/replays/{id}` 返回任务视图（进度/当前步骤/差异计数/最近错误/事件流水）；
  `GET /api/admin/replays/{id}/report` 返回每步完整报告——**预期状态**（检查点批次快照 + 由旧表快照
  按转换规则推导出的记录）、**实际状态**（当前批次行 + 范围内新表记录）、字段差异
  （`__missing__` 缺行 / `name`/`email`/`tags` 不一致 / `__extra__` 预期外记录，范围外不参与）
  与批次级状态差异（phase/active_schema/epoch/freeze_version 漂移）。差异是**发现项**：任务仍 COMPLETED，
  只有数据无法还原类错误才 FAILED。
- 回放 worker 与计划 worker 同生命周期（单实例后台线程；测试可用 `PLAN_WORKER_ENABLED=0` 一并关闭）。

## 回放报告复核工作流

管理员对**已完成（COMPLETED）回放**的每个步骤提交复核结论，系统把结论与**回放报告版本**绑定、
只追加保留历史；全部步骤通过才能确认，发现问题进入待处理队列，可重新打开进入新一轮复核。

### 复核状态机(回放任务级)

```
UNREVIEWED ──提交首条复核──▶ REVIEWING ──任一步骤 FAIL──▶ PENDING(待处理)
                               │                            │ reopen(报告版本+1,
                               │ 全部 SUCCESS 步骤 PASS     │  旧版本结论保留为历史)
                               ▼                            ▼
                            CONFIRMED(终态) ◀────────── UNREVIEWED(新一轮复核)
```

- **提交复核**：`POST /api/admin/replays/{id}/reviews`，携带 `step_seq`、`report_version`、
  `verdict`(PASS/FAIL)、`issue`(问题说明，FAIL 必填)、`fix_tags`(修复标签)。
  结论按 `(任务, 报告版本, 步骤)` 唯一落库（唯一约束兜底），只追加、永不修改。
- **写入校验**：回放须 COMPLETED 且未确认；`report_version` 必须等于当前版本——
  **过期版本返回 409 冲突，不写入也不覆盖已有新结论**；步骤须 SUCCESS（报告已生成）；
  同一版本同一步骤只允许一条结论，重复写入（不同幂等键）返回 409，改判须重新打开进入新版本。
- **确认**：`POST .../review/confirm`（带 `report_version` 防止基于过期报告确认）——
  仅当前版本全部 SUCCESS 步骤均 PASS 才允许，存在 FAIL 或未复核步骤返回 409；
  确认后 `review_status=CONFIRMED` 并记录确认人/时间，复核锁定，重复确认幂等。
- **待处理与重新打开**：任一 FAIL 结论使回放进入 `PENDING`（待处理队列可查）；
  `POST .../review/reopen` 把报告版本 +1、状态回到 UNREVIEWED 开始新一轮复核，
  **旧版本结论全部保留为历史**，仅 PENDING 状态可重新打开。
- **持久化与幂等**：复核状态、历史结论、待处理队列全部落库，服务重启不丢失；
  提交/确认/重新打开都要求 `idempotency_key`（`replay.review.*` 命名空间），
  同一请求重复提交返回首次结果（`replayed: true`），不产生重复结论。
- **查询**：`GET /api/admin/replays/{id}/reviews` 返回当前版本逐步结论、全部历史版本结论
  与复核事件流水；`GET /api/admin/review-queue?status=PENDING` 按复核状态查询回放
  （默认待处理队列）；回放列表/详情/报告与 `/api/status` 都带 `review` 进度汇总。
- 页面：回放卡片显示复核状态徽章、报告版本与进度（已复核/通过/问题数），
  "复核"面板逐步展示结论/问题说明/修复标签、提交表单、确认/重新打开操作与历史变更；
  回放任务区顶部实时列出待处理回放队列。

## 批量复核与分派

在单步复核之上，管理员可以**筛选待处理任务、批量分派复核人、批量提交步骤结论**；
分派关系、分派历史、批量结果与待处理队列全部落库，服务重启后保留。

- **筛选**：`GET /api/admin/review-tasks?review_status=&biz=&report_version=` 按复核状态、
  业务分组（经回放步骤关联的批次 `biz`）、报告版本过滤进入复核流程（COMPLETED）的任务，
  返回当前分派人、分派历史与复核进度，供批量操作选择。
- **分派**：`POST /api/admin/review-batch/assign {assignee, replay_ids, reason?}` 把一批任务
  分派给指定复核人。当前分派人记录在 `replay_tasks.assignee`，每次分派/改派落一行
  `replay_assignments` 历史（只追加，含操作者、分派时报告版本、说明）并写 `review.assign` 事件，
  任务详情可见。**重复分派（同人）幂等**（`already_assigned: true`，不产生新历史）；
  已确认（CONFIRMED）/不存在/未完成的任务逐项失败。
- **分派权限**：任务已分派后，**只有被分派的复核人能提交该任务的步骤结论**
  （单项与批量提交同样校验，其他人 409）；未分派的任务不限制提交人。
  重新打开（报告版本 +1）不清空分派关系。
- **批量复核**：`POST /api/admin/review-batch/reviews {report_version, items:[{replay_id,
  step_seq, verdict, issue?, fix_tags?}]}`。**所有项必须基于同一报告版本**——逐项校验任务
  当前版本，版本已变化/任务已确认/无权限（任务分派给他人）/步骤已有结论等**逐项返回失败
  原因，成功项独立提交不被回滚**；每项复用单项复核的全部校验。
- **批量结果查询**：每次批量操作落一行 `replay_batch_ops`（总数/成功/失败 + 逐项结果）。
  `GET /api/admin/review-batch/{id}` 返回逐项成功/失败原因；`GET /api/admin/review-batch`
  返回最近批量操作列表（进度汇总）。
- **幂等**：批量分派与批量复核都要求 `idempotency_key`（`replay.batch.*` 命名空间），
  同一请求重放返回首次结果（`replayed: true`），不产生重复历史/重复结论；
  同键不同请求体返回 409。
- 页面"批量复核与分派"区：筛选条件 + 结果表格（勾选、分派人列）、批量分派表单、
  批量复核表单、最近一次批量操作的进度条与逐项失败原因表、批量操作记录列表与按 ID 查询。

## 回放证据归档（不可变归档包）

管理员可以为**已 COMPLETED 的回放**按**指定报告版本**生成不可变归档包。归档严格只读：
绝不修改批次/计划/回放/复核/业务数据，只写 `replay_archive_*` 表与归档包文件；
活动归档（QUEUED/RUNNING/PAUSED）期间，对应回放的复核提交/确认/重新打开/分派一律
被拒绝（归档期间不能修改原回放或复核数据）。

- **归档包内容**（确定性 zip，固定文件顺序与时间戳）：
  - `metadata.json`：归档 id、指定报告版本、操作者、时间、摘要算法；
  - `replay/task.json`：回放任务快照（报告版本、复核状态、确认人、差异汇总）；
  - `replay/steps.json`：**全部步骤报告**（预期/实际状态、字段差异、批次状态差异、检查点证据）；
  - `review/conclusions.json`：**指定版本逐步复核结论** + **全量历史版本结论**；
  - `review/assignments.json`：**复核分派历史**（只追加）；
  - `audit/summary.json`：**审计摘要**（计划级审计 + 每步批次 freeze/cutover 关键证据与计数）；
  - `manifest.json`：逐文件大小与 sha256、包**内容摘要**（仿 git 风格：逐文件 sha256 与
    文件名有序拼接后再 sha256）。内容摘要同时落库，供下载后离线比对。
- **归档任务状态机**：`QUEUED`（排队等并发额度）→ `RUNNING` → `COMPLETED`（包不可变）；
  `pause` 在归档单元边界停住（PAUSED），`resume` 重新排队（再次受并发闸门约束）；
  `cancel` 终止不生成包；**版本冲突 / 缺失步骤 / 数据被删除 / 关键审计缺失 / 摘要不一致**
  一律 `FAILED`，失败码（`version_conflict`/`missing_step`/`data_deleted`/
  `audit_incomplete`/`digest_mismatch`/`package_missing`）、失败原因、已采集单元与事件流水
  永久保留可查询。归档单元顺序：校验 → 逐步报告 → 复核结论 → 分派历史 → 审计摘要 → 打包，
  每个单元产物先入 `staging` 提交（崩溃边界），重启后遗留 RUNNING 归档回到 QUEUED，
  从首个未完成单元续跑。
- **幂等**：创建/暂停/恢复/取消走 `archive.*` 幂等命名空间；同一（回放，报告版本）的重复
  创建幂等返回已有归档（活动中或已完成），FAILED/CANCELED 后允许重新发起；
  暂停/恢复/取消的同态重复调用无副作用，同键重放返回首次结果。
- **并发**：同时 RUNNING 的归档不超过 `ARCHIVE_MAX_CONCURRENCY`（默认 2），其余排队
  （Postgres 咨询锁串行认领，SQLite 写事务串行）。
- **下载与校验**：`GET /api/admin/archives/{id}/download` 下载 zip；
  `GET /api/admin/archives/{id}/verify` 回读包重算逐文件与内容摘要并比对——
  摘要不一致或包文件被删时，归档明确置 FAILED 并保留失败记录（原包保留取证）。
  包文件落在 `ARCHIVE_STORE_DIR`（默认 `./archive_store`，原子写：先 `.tmp` 再替换）。
- 页面"回放证据归档"区：归档任务表显示进度条、当前归档单元、失败码与原因、内容摘要、
  暂停/恢复/取消/下载/校验摘要/详情（包清单与事件流水）；回放卡片在归档期间显示冻结标记。

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
POST /api/admin/plans   {operator, idempotency_key, name, risk_level?, max_retries,
                         steps:[{seq, batch_id, depends_on:[seq...], max_retries?}],
                         windows?:[{starts_at, ends_at}]}
GET  /api/admin/plans                              计划列表(含步骤/依赖/事件流水/审批/窗口)
GET  /api/admin/plans/{id}                         计划详情
POST /api/admin/plans/{id}/start|pause|resume|cancel   {operator, idempotency_key}
POST /api/admin/plans/{id}/approve                 {operator, idempotency_key} 高风险审批通过(不得为创建者)
POST /api/admin/plans/{id}/reject                  {operator, idempotency_key, reason} 拒绝并阻止启动
POST /api/admin/plans/{id}/revoke-approval         {operator, idempotency_key} 启动前撤销审批
PUT  /api/admin/plans/{id}/window                  {operator, idempotency_key, windows:[{starts_at,ends_at}]}
                                                    (启动前整体替换; 空列表清空限制; POST 同义)
POST /api/admin/checkpoints          {operator, idempotency_key, plan_id}  固化审计检查点与批次快照
GET  /api/admin/checkpoints                         检查点列表
GET  /api/admin/checkpoints/{id}                    检查点详情(逐步骤审计证据/快照统计/不完整原因)
POST /api/admin/replays              {operator, idempotency_key, plan_id, checkpoint_id} 创建回放并排队
GET  /api/admin/replays                             回放任务列表(进度/当前步骤/差异数/最近错误)
GET  /api/admin/replays/{id}                        回放任务详情
GET  /api/admin/replays/{id}/report                 回放报告详情(逐步预期/实际状态与字段差异)
POST /api/admin/replays/{id}/pause|resume|cancel    {operator, idempotency_key}
POST /api/admin/replays/{id}/reviews   {operator, idempotency_key, step_seq, report_version,
                                        verdict, issue?, fix_tags?}   提交单步复核结论(版本/步骤状态校验)
GET  /api/admin/replays/{id}/reviews                  复核记录(当前版本逐步结论+历史版本+复核事件)
POST /api/admin/replays/{id}/review/confirm  {operator, idempotency_key, report_version}
                                                      全部步骤 PASS 才允许确认
POST /api/admin/replays/{id}/review/reopen   {operator, idempotency_key, reason?}
                                                      待处理回放重新打开(报告版本+1, 历史保留)
GET  /api/admin/review-queue?status=PENDING           按复核状态查询回放(待处理队列)
GET  /api/admin/review-tasks?review_status=&biz=&report_version=
                                                      筛选待处理任务(状态/业务分组/报告版本)
POST /api/admin/review-batch/assign   {operator, idempotency_key, assignee, replay_ids, reason?}
                                                      批量分派复核人(逐项失败原因, 重复分派幂等)
POST /api/admin/review-batch/reviews  {operator, idempotency_key, report_version,
                                       items:[{replay_id, step_seq, verdict, issue?, fix_tags?}]}
                                                      批量提交复核结论(同一报告版本, 逐项独立提交)
GET  /api/admin/review-batch                          最近批量操作列表(进度汇总)
GET  /api/admin/review-batch/{id}                     批量操作结果(逐项成功/失败原因)
POST /api/admin/archives            {operator, idempotency_key, replay_id, report_version}
                                                      为已完成回放生成不可变证据归档并排队
GET  /api/admin/archives                             归档任务列表(含失败/取消记录)
GET  /api/admin/archives/{id}                        归档详情(进度/当前单元/失败原因/摘要/清单/事件)
POST /api/admin/archives/{id}/pause|resume|cancel    {operator, idempotency_key}
GET  /api/admin/archives/{id}/download?operator=     下载不可变归档包(zip)
GET  /api/admin/archives/{id}/verify?operator=       重算摘要校验(不一致/包缺失 -> FAILED 并保留记录)
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
python3 -m pytest tests/ -q   # 109 个用例:
# 批次(16): 批次创建与范围重叠拒绝/批次外正常读写/双读差异/范围内外多余记录拦截/
#           单独恢复不清其他批次/幂等重放(含跨批次)/epoch 栅栏双人推进只一人成功/重启保持
# 计划(13): 建计划聚合拒绝(批次不存在/重复占用/跨计划占用/DONE 终态/依赖不存在/成环)/
#           依赖顺序推进/上游失败下游保持 BLOCKED/重试耗尽 HALTED/恢复换轮重试成功/
#           暂停在步骤边界/取消跳过未开始步骤/计划动作幂等/重启 RUNNING 步骤复位并自动续跑/
#           HALTED 重启不偷跑/状态与审计接口
# 审批与窗口(13): 低风险免审批直启/高风险须他人审批(创建者自审被拒)/拒绝带原因阻止启动且可重审/
#           启动前撤销审批/重复审批拒绝撤销幂等(同键重放+跨键无副作用, 审计不重复)/
#           低风险审批操作被拒/启动后审批锁定/窗口外暂停窗口内继续(审计)/启动时窗口外等待/
#           窗口仅启动前可改且幂等/非法窗口原子拒绝/高风险+窗口双闸门叠加/重启保持审批与窗口边界
# 回放(25): 检查点计划不存在 404/运行中计划检查点 INCOMPLETE 且原因可见/检查点固化幂等/
#           回放创建计划·检查点缺失 404/检查点跨计划拒绝/不完整检查点拒绝/重复创建幂等回显/
#           干净回放零差异/字段漂移+缺失检测(范围外不参与)/__extra__ 多余记录+批次状态漂移/
#           审计删除 FAILED 且保留已完成报告/快照损坏 FAILED/检查点中途删除 FAILED/批次删除 FAILED/
#           暂停在步骤边界+恢复续跑+终态拒绝/排队取消全部 SKIPPED/执行中取消保留报告/排队暂停不被认领/
#           并发上限排队+完成后放行/重启 RUNNING 复位排队续跑不丢报告/回放只读(批次/业务表/审计不变)/
#           依赖顺序 1→2→3/控制请求同键幂等重放/status 汇总/失败终态 worker 不再处理
# 复核(11): 逐步复核进度与确认流程/FAIL 进入待处理且问题说明+修复标签落库/重新打开版本+1 历史保留/
#           过期版本冲突不覆盖新结论/同版本同步骤重复结论拒绝/同键幂等重放不产生重复结论/
#           未全部 PASS 与待处理时确认被拒/非 COMPLETED 回放复核被拒/非 SUCCESS 步骤与未知步骤校验/
#           重启后复核状态·历史·待处理队列保留/按状态查询待处理回放/状态与详情接口带复核进度
# 批量复核与分派(11): 按状态/业务分组/报告版本筛选待处理任务/批量分派与历史保留(改派追加)/
#           重复分派幂等/已确认与不存在任务逐项失败/分派后仅被分派人可提交(单项+批量)/
#           批量复核逐项失败(版本变化·已确认·无权限)且成功项不回滚/同一报告版本约束/
#           批量分派与批量提交同键幂等重放/批量结果查询接口/重启后分派关系·批量结果·队列保留
# 归档(20): 非 COMPLETED/不存在回放拒绝/创建时归档版本冲突 409/创建同键幂等重放/
#           完整归档包内容(元信息·任务·全步骤报告·复核结论含历史·分派历史·审计摘要·清单)/
#           逐文件 sha256 与内容摘要校验通过/zip 下载/载荷摘要确定性/
#           同(回放,版本)重复归档幂等回显(活动中+已完成)/暂停恢复取消同态与同键幂等/终态拒绝控制/
#           取消保留记录不出包且可重新归档/暂停在单元边界从下一单元续跑/并发上限排队与放行/
#           执行中版本冲突 version_conflict/步骤报告缺失 missing_step/批次删除 data_deleted/
#           回放删除 data_deleted/篡改包摘要不一致 digest_mismatch 且失败留痕/包被删 package_missing/
#           归档活动期间复核提交与分派被冻结/归档严格只读不改原报告/重启 RUNNING 复位续跑已采集单元不丢/
#           status 汇总含归档与并发上限
```
