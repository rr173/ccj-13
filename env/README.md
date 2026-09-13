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

## 归档目录与生命周期管理

在不可变归档包之上提供**归档目录检索、保留策略与清理计划**：管理员按回放、业务分组、
报告版本与内容摘要检索已完成归档，为归档设置带到期时间的保留策略或永久保留标记，
并可发起逐项返回跳过原因的清理计划。保留策略、摘要引用关系与清理进度全部落库，
服务重启后保留。

### 归档目录检索

- `GET /api/admin/archives?replay_id=&biz=&report_version=&content_digest=&status=&retention=&include_cleaned=`
  多维检索：按回放、业务分组（归档时从各步骤批次反查并固化的 `biz_groups`）、报告版本、
  内容摘要（完整 sha256 或 ≥12 位前缀消歧）、归档状态、保留模式过滤；默认只返回**未清理**
  的存活归档，`include_cleaned=true` 可查已清理记录。
- `GET /api/admin/archives/by-digest/{digest}` 按摘要查询归档：返回同摘要存活归档列表与
  **去重组引用关系**（canonical 成员、存活引用数）。

### 保留策略

- `PUT /api/admin/archives/{id}/retention {mode, retain_until?}`（POST 同义，走
  `archive.retention` 幂等命名空间）：
  - `PERMANENT` 永久保留标记；`UNTIL` 带到期时间（必须晚于当前时间，ISO 8601），
    到期前受保护；`NONE` 清除策略。仅 **COMPLETED** 归档可设置，已清理归档拒绝。
  - 策略字段（模式/到期时间/设置人/设置时间）持久化，重启保留；归档视图实时返回
    `retention`（`retained/retain_reason/expired`）。

### 同摘要去重与引用关系

- 每个 COMPLETED 归档完成时按内容摘要在 `archive_digest_members` 登记成员关系：
  同摘要多个归档**物理 zip 只保留一份**（canonical 成员持有文件），其余成员共享该路径，
  归档视图带 `digest_group`（`is_canonical/reference_count/member_count`）。
- 清理 canonical 前必须把物理文件**移交**给同摘要最早的存活成员（其包须存在且摘要匹配）；
  移交失败逐项跳过（`digest_referenced`），**仍被引用的记录与文件绝不删除**；非 canonical
  成员清理只解除自身引用；最后一个引用解除后物理文件才删除。

### 清理计划

管理员对一批归档发起清理（逐项独立事务提交，成功项不回滚，跳过/失败项带机器可读原因）：

```
QUEUED ─claim(单 RUNNING 串行)─▶ RUNNING ─全部项处理完─▶ COMPLETED(终态)
  ▲                                ├─pause(逐项边界)─▶ PAUSED ─resume─┘
  └──────────── resume ────────────┴─某 tick 未预期错误─▶ FAILED ─resume(失败项重新排队)
                                   └─cancel─▶ CANCELED(未处理项 SKIPPED_CANCELED)
```

- 逐项跳过原因（`reason_code`，永久可查）：`not_found`（归档不存在）/`not_completed`
  （归档活动中）/`already_cleaned`/`retained_until`（保留期内）/`retained_permanent`
  （永久保留）/`in_use_download`（正在下载）/`in_use_verify`（正在摘要校验）/
  `package_missing`/`file_delete_failed`/`digest_referenced`（同摘要仍被引用）/
  `internal_error`。
- **下载/校验并发协调**：下载与摘要校验在归档行持有使用计数（`active_downloads/
  active_verifies`，行锁 + 写事务串行化）；下载响应发送期间计数 >0，清理逐项跳过，
  **归档正在下载或校验时物理文件绝不被删除**；崩溃遗留计数由重启对账清零。
- 清理是**软删除**：归档行打 `cleaned_at/cleaned_by/cleanup_plan_id` 标记、默认目录检索
  不返回（记录与事件永久保留可查），物理文件按上述引用规则处理。
- `POST /api/admin/archive-cleanups` 创建排队（`archive.cleanup.*` 幂等命名空间，同键重放
  返回首次结果）；`GET /api/admin/archive-cleanups` 列表；`GET …/{id}` 详情含**逐项
  跳过原因**与事件流水；`pause/resume/cancel` 均幂等（FAILED 可 resume，失败项重新排队）。
  每个 tick 至多处理 `CLEANUP_ITEMS_PER_TICK`（默认 5）项，暂停/取消在逐项边界生效。
- 后台单实例 worker（`CLEANUP_WORKER_POLL_INTERVAL`，随 PLAN_WORKER_ENABLED 一并开关）；
  重启对账把遗留 RUNNING 计划复位 QUEUED、RUNNING 单项复位 PENDING，逐项进度、跳过原因、
  保留策略、摘要引用关系全部保留。
- 页面"归档目录与生命周期"区：多维检索表单与结果表（保留状态徽章、引用数、下载/校验占用、
  清理状态、保留策略设置按钮）、清理计划表（进度条/当前项/最近错误/暂停恢复取消/逐项结果），
  逐项结果表展示每项的跳过/失败原因码与说明。

## 迁移前数据质量门禁

管理员在计划**启动前**为其绑定一组**可版本化**的数据质量规则, 对计划涉及的批次
生成质量扫描任务; 扫描结果生成后, 只有**全部阻断级(BLOCKER)问题被修复或豁免**、
且扫描结果仍有效(规则版本未变 / 批次数据未变 / 未过期), 计划才允许通过质量门禁
进入启动流程。门禁与高风险审批、执行窗口是**叠加闸门**。

### 规则与版本(quality_rule_sets / quality_rule_versions)

- 一个计划至多一个规则集; 规则整体版本化, 每次修改且**内容摘要变化**才新增一个
  **不可变版本**(`rules` 规范化后 sha256), 相同内容重复保存幂等无副作用;
  旧版本永不修改, 扫描/问题/修复/豁免都绑定产生时的规则版本。
- 四类规则(校验非法时聚合返回全部原因, 不落任何数据):
  - `required` 必填(`params.allow_blank` 可放行纯空白);
  - `format` 格式: `params.pattern` 正则(保存时编译校验);
  - `cross_field` 跨字段一致性: `params.other_field` + `op`
    (`eq/ne/contains/not_contains/regex_match`);
  - `range` 范围: 数值 `min/max`, 或字符串/数组长度 `min_length/max_length`。
- 严重级别 `BLOCKER/WARNING/INFO`; 只有 BLOCKER 阻断门禁, WARNING/INFO 仅提示。
- 规则作用于批次范围内的**旧结构记录**(迁移前事实来源), 可作用字段
  `name/email/tags_csv/tags(派生)/id`。

### 质量扫描任务(quality_scans / quality_scan_batches / quality_issues)

- 对计划涉及的每个批次逐批扫描, 按批次记录状态、记录数、问题数与**批次数据指纹**
  (扫描完成时范围内旧表全量内容 sha256); 问题按 `(扫描,批次,规则,记录,字段)` 唯一,
  带严重级别、问题说明与**可追踪样本**(命中时的完整记录快照, 每条 规则×批次 至多
  保留 500 个样本, 超出仍计数)。
- 状态机(与回放任务同构):
  ```
  QUEUED ─claim(并发额度)─▶ RUNNING ─全部批次扫完─▶ COMPLETED(终态)
    ▲                          │
    └──── resume ──── PAUSED ◀──┘(pause 在批次边界生效)
                                 │
                                 ├─ cancel ─▶ CANCELED(终态, 未开始批次 SKIPPED)
                                 └─ 规则版本缺失/批次删除/执行异常 ─▶ FAILED(终态, 可 resume)
  ```
- 并发受 `QUALITY_SCAN_MAX_CONCURRENCY`(默认 2, 至少 1)限制, 超出排队;
  暂停/恢复/取消均幂等(同态重复 `already_in_state:true`, 同键重放 `replayed:true`)。
- 重复扫描幂等: 同一计划已有活动(QUEUED/RUNNING/PAUSED)扫描时返回已有任务
  (`already_active:true`); 终态后允许重新发起。扫描只能在计划 **DRAFT** 时发起。
- 结果**有效期**: `QUALITY_SCAN_TTL_SECONDS`(默认 86400 秒), COMPLETED 时记录
  `expires_at`。**规则版本变化、批次数据变化(指纹漂移)或结果过期 -> 门禁 STALE,
  旧结果不能直接放行**, 必须重新扫描。
- 重启安全: RUNNING 标记为崩溃边界(先提交后扫描); 重启把遗留 RUNNING 扫描复位
  QUEUED、RUNNING 批次复位 PENDING(问题未提交, 安全重扫), 已 SUCCESS 批次与其
  问题、数据指纹全部保留, worker 自动续跑。
- 同规则版本重新扫描时, 历史**有效豁免自动继承**到新扫描的相同问题
  (解决 TTL 过期重扫后无需重复豁免; 规则版本变化不继承)。

### 修复批次与豁免(quality_fix_batches / quality_exemptions)

- **修复批次**: `POST .../quality-fixes {issue_ids}` 逐项在**当前数据**上重跑问题
  对应规则做核验, 每项给结论: `RESOLVED`(违规消失, 问题置 FIXED) /
  `STILL_OPEN`(仍违规) / `NOT_FOUND`(记录已不存在) / `REJECTED`(问题不存在、
  不属于最近一次 COMPLETED 扫描或已非 OPEN)。修复批次**只追加**, 绑定扫描规则版本;
  修复会改变数据指纹, 因此修复后需重新扫描, 门禁才重新评估。
- **豁免**: 对单个 BLOCKER 问题提交带原因的豁免申请(**提交即批准**), 与规则版本
  绑定、只追加保留历史; 可撤销(豁免置 REVOKED, 问题回到 OPEN, 门禁重新阻断);
  重复豁免幂等; 非阻断/已 FIXED 的问题不能豁免。豁免不改数据, 不触发结果失效。

### 门禁判定与接口

实时判定状态: `NOT_CONFIGURED`(未绑定规则, 不约束, 兼容历史行为) /
`NOT_SCANNED` / `RUNNING` / `FAILED` / `CANCELED` / `STALE` / `BLOCKED` / `PASS`。
仅 PASS 放行启动(在 `do_start` 内与高风险审批串联)。

- 页面显示: 规则版本(当前版本+全部历史版本)、扫描进度(批次/记录/当前批次)、
  问题分布(严重级别 × 处理状态)、修复/豁免历史与门禁状态徽章(计划卡片上同步展示,
  未通过时启动按钮禁用并给出原因)。
- 幂等: 规则保存/扫描创建与控制/修复/豁免全部要求 `idempotency_key`
  (`quality.*` 命名空间), 同键跨动作/跨目标/不同请求体复用返回 409。
- 配置: `QUALITY_SCAN_MAX_CONCURRENCY`(默认 2)、`QUALITY_SCAN_TTL_SECONDS`
  (默认 86400)、`QUALITY_WORKER_POLL_INTERVAL`(默认 0.5s);
  worker 随 `PLAN_WORKER_ENABLED=0` 一并关闭, 测试手动 `claim_due_scans` +
  `run_scan_tick` 驱动。

## 质量结果失效后的自动重扫编排

计划启动后进入执行期, 质量门禁继续作为**叠加闸门**存在: 批次数据写入、规则版本
变化或扫描结果过期时, 系统为受影响计划**自动编排唯一的重扫任务**并记录触发
原因; 计划执行器在**步骤边界**(取到候选步骤前后)遇到门禁失效即暂停推进,
重扫完成且阻断问题处理完后计划**从原步骤自动继续**。全部状态落库, 服务重启后
待处理重扫与暂停状态继续保留。

### 触发来源与唯一重扫

- 三类失效来源即时触发: **批次数据写入**(`POST /api/records` 命中执行态计划
  涉及的批次且内容实际变化, 受影响批次记入范围)、**规则版本变化**(执行态计划
  保存新规则版本, 重扫基于新版本)、**扫描过期**(worker 每 tick 兜底, TTL 到期)。
- 每次编排产生且仅产生一个 `scan_source=auto_rescan` 的扫描: 记录首次触发来源
  (`trigger_source`: `batch_data_write`/`rule_version_change`/`scan_expired`/
  `gate_blocked`/`scan_failed_retry`)、触发原因列表(`triggers`, 重复触发**去重
  合并**)、`supersedes_scan_id`(取代的旧扫描)与 `affected_batch_ids`(受影响
  批次); 重扫仍覆盖计划**全部批次**(跨批次影响范围可追溯)。
- **重复触发不生成重复任务**: 计划同时至多一个活动(QUEUED/RUNNING/PAUSED)
  扫描(数据库部分唯一索引兜底, Postgres 咨询锁 / SQLite 进程互斥锁串行化);
  新触发只把原因合并进在途任务。执行态计划在门禁暂停中也允许管理员**手动发起**
  扫描(幂等复用在途任务), 普通 RUNNING 计划不允许手动扫描。
- DRAFT/PAUSED 计划不自动排队: DRAFT 沿用"门禁实时 STALE + 手动扫描"流程;
  用户主动暂停的计划恢复后由执行器门禁兜底编排。

### 计划暂停与自动恢复(quality hold)

- 执行器在步骤边界调用门禁: 不通过即把计划置为**质量暂停**(`quality_hold=true`,
  计划状态仍为 RUNNING/HALTED), 落一条只追加的 `quality_gate_holds`(ACTIVE):
  记录触发来源、暂停原因(机器可读 `reason_code` 为门禁状态 STALE/BLOCKED/
  RUNNING/FAILED/...)、**停留的步骤**(步骤保持 PENDING/FAILED, 尝试计数与轮次
  不复位)、关联的唯一重扫与合并触发历史, 并写 `plan.quality_hold` 计划审计。
- worker 每 tick 复评: 重扫 COMPLETED 且阻断问题全部 FIXED/EXEMPTED(门禁 PASS)
  时 hold 自动置 RESUMED(`resume_mode=auto_gate_pass`, 记录依据扫描/时间),
  写 `plan.quality_resume` 审计, 计划**从暂停时的原步骤**继续; 重扫发现新阻断
  问题则保持暂停并把原因刷新为 BLOCKED(修复批次本身为执行态计划再次编排重扫,
  豁免不重扫、凭当前有效扫描直接恢复)。
- **重扫失败可恢复**: 重扫 FAILED 时暂停不解除(原因反映失败码), 管理员对扫描
  `resume`(失败批次重试)后自愈, 成功且门禁通过即继续; 重扫被暂停则计划等待。
- **计划取消后不再自动恢复**: `cancel` 把 ACTIVE hold 置 CANCELED、活动重扫
  (自动/手动)随计划取消; 此后即使扫描完成、门禁通过或再发生写入, 计划也不复活,
  不会有新的自动重扫。计划 COMPLETED 同理(批次 DONE, 旧路径写入 410)。
- 暂停/恢复历史接口: `GET /api/admin/plans/{id}/quality-holds`
  (当前 ACTIVE + 全量 RESUMED/CANCELED 历史); `POST /api/admin/quality-rescan/sweep`
  可手动触发一次与 worker 等价的兜底编排(关闭后台线程的测试/运维用)。
- 页面: 计划卡片显示"质量门禁暂停"横幅(触发来源/停留步骤/暂停原因/关联重扫/
  历史按钮), 质量门禁区显示每个扫描的"自动重扫/手动扫描"标记、触发来源、
  取代扫描、受影响批次、合并触发次数, 以及暂停与自动恢复历史。

## 审计事件回放与补偿(不可变事件流 / 时间点快照 / 补偿执行撤销)

在质量门禁与迁移计划之上新增统一的不可变审计事件流, 并基于它提供时间点回放快照、
快照校验与待补偿动作的预览/执行/重试/撤销。

### 统一事件流(audit_events)

- 只追加, 任何代码路径都不允许 UPDATE/DELETE; 补偿只追加 `COMP_*` 事件, 不改原事件。
- 每个事件带:
  - `global_seq`: 全库严格递增顺序号(插入加锁分配);
  - `stream_key` / `stream_seq`: 计划相关事件以 `plan_id` 为流键, 批次写事件以
    `batch:<id>` 为流键, 备注落到 `global`; 单流 seq 从 1 连续递增;
  - `stream_hash`: 单流哈希链(含 seq/类型/关联 id/载荷/时间戳/操作者/前哈希),
    篡改或丢失事件即断链;
  - `correlation_id`: 同一逻辑事件扇出到计划流与批次流时共享, 批次查询据此折叠;
  - `dedupe_key`: 投影去重, 重复请求/幂等重放返回同一行(不产生第二个顺序号)。
- 事件类型: `BATCH_WRITE`(批次内记录写入)、`BATCH_CREATE/FREEZE/VALIDATE/CUTOVER/
  RECOVER/ATTACH`、`RULE_CHANGED`、`SCAN_STATUS`、`QUALITY_HOLD`、
  `PLAN_ADVANCE`、`PLAN_CANCEL`、`COMP_EXECUTED/COMP_FAILED/COMP_UNDONE`,
  以及唯一允许运维直接写入的 `EXTERNAL_NOTE`(显式补录备注)。
- 查询: `GET /api/admin/audit-events` 支持按计划(连续 seq)、批次(批次流+计划流投影,
  按 correlation 折叠并标注 `other_plan_ids`)、时间范围(闭区间)、事件类型过滤,
  keyset 分页(`after_global_seq`)。补录备注 `POST /api/admin/audit-events`,
  时间戳早于流上一事件超过 `AUDIT_MAX_OUT_OF_ORDER_SECONDS`(默认 300s)按乱序拒绝。

### 时间点快照(audit_snapshots)

运维对已 `COMPLETED`/`CANCELED` 的计划选择时间点生成快照
(`POST /api/admin/audit-snapshots`, `target_at` 缺省取计划终结事件时间)。
快照逐行固化事件副本(`audit_snapshot_events`)与每个步骤批次的目标时点基线
(`audit_snapshot_batches`, 含预期阶段/epoch/freeze_version 与旧表全量基线)。

生成即校验, 任一不满足快照置 `REJECTED`(仍持久化可查, HTTP 422 并逐条给原因),
通过为 `VALID` 并带 TTL(`AUDIT_SNAPSHOT_TTL_SECONDS`, 默认 86400):

| 原因码 | 含义 |
| --- | --- |
| `plan_not_terminal` | 计划尚未 COMPLETED/CANCELED, 或流中找不到终结事件 |
| `target_before_end` | 目标时间早于计划终结时间 |
| `stream_gap` | 计划流 stream_seq 不连续(有缺口), 缺失序号随原因返回 |
| `chain_broken` | 哈希链重算不一致(事件被篡改/丢失), 断链位置随原因返回 |
| `out_of_order` | 事件时间戳倒流超过容忍阈值(乱序事件) |
| `rule_version_gap` | 规则版本倒挂/跳号、引用的版本不存在或内容摘要不一致 |
| `batch_version_gap` | 批次 epoch 增量不连续/倒挂, 或 freeze_version 不一致 |
| `batch_state_drift` | 目标时点批次预期状态与当前批次行不一致(回放无法还原) |

### 补偿任务(compensation_tasks / compensation_actions)

`GET .../preview` 从快照基线推导待补偿动作(纯计算, 不落库):
`record_backfill`(新表缺失/不一致, 按转换规则回填修正)、`record_cleanup`
(基线外多余新表记录, 删除)、`batch_unfreeze`(取消计划中仍 FROZEN 等的批次恢复
NORMAL)。`POST /api/admin/compensations` 基于 VALID 快照创建任务(动作落 PENDING)。

- **不越过质量门禁**: 每个动作执行前实时复核该计划质量门禁, 门禁非
  PASS/NOT_CONFIGURED 时该动作 FAILED(`gate_status` 记录状态), 不产生副作用;
  门禁恢复后可逐动作重试。
- **计划取消后拒绝补偿**: CANCELED 计划的快照可预览但 `execution_allowed=false`,
  执行时每个动作 FAILED(`PLAN_CANCELED`)。
- **幂等执行**: 动作带确定性 `action_key`(快照+类型+批次+记录), 同快照同时至多
  一个非终态任务(部分唯一索引兜底), 重复请求返回首次结果, 不重复写入。
- **逐动作失败重试**: `POST .../{task_id}/retry`(仅 FAILED), 动作逐个独立事务,
  部分失败停 `PARTIAL`, 已成功动作不回滚。
- **整体撤销**: `POST .../{task_id}/undo` 对 SUCCESS 动作按 seq 逆序用执行时捕获的
  `before_image` 恢复现场, 只追加 `COMP_UNDONE` 并关联原 `COMP_EXECUTED` 事件;
  撤销不被快照 TTL/门禁卡死, 部分撤销失败停 `UNDO_PARTIAL` 可继续。
- **快照过期**: VALID 快照超过 `expires_at` 后执行/重试被 409 拒绝(撤销不受限)。
- **重启续跑**: 遗留 RUNNING 任务回 QUEUED、UNDO_RUNNING 回 UNDO_PARTIAL,
  动作状态/进度/失败原因/撤销镜像全部保留; worker 受
  `COMPENSATION_MAX_CONCURRENCY` 限制认领执行与撤销。

#### 风险分级与双人审批(compensation_approvals)

补偿任务创建时按动作构成自动推导风险等级(可显式 `risk_level=HIGH`):

- **LOW(免审批)**: 仅 `record_backfill` 回填/修正动作, 创建即 `NOT_REQUIRED`, 可直接执行;
- **HIGH(双人审批)**: 含 `record_cleanup`/`batch_unfreeze` 等破坏性动作, 或基于
  CANCELED 计划快照的补偿, 创建即 `PENDING`, 必须收集**同一审批轮次内两名不同
  操作者**的独立通过才能执行;
- **职责分离**: 审批人不能是任务创建者; 执行操作者不能是任一审批人(创建者可以执行);
- **拒绝**: 任一审批人带原因拒绝即关闭闸门(`REJECTED`), 当轮已收集通过置
  `SUPERSEDED`, 必须在新一轮重新收集两名通过;
- **审批依据自动失效**: 首轮通过时固化审批依据(快照状态/目标点/流边界/TTL、
  质量门禁状态/规则版本与摘要/最新扫描、计划状态、批次 phase/epoch/freeze_version);
  审批收集后任一依据变化, worker sweep 与执行/重试/预约的惰性复核都会把当前轮次
  已收集审批置 `INVALIDATED` 并记录机器可读原因
  (`snapshot_expired`/`snapshot_changed`/`gate_status_changed`/
  `plan_status_changed`/`batch_version_gap`), 任务审批状态回到 `INVALIDATED`,
  开启新一轮(`approval_round+1`)重新收集; 审批记录只追加, 失效历史完整可查;
- **并发去重**: `(task_id, approval_round, operator)` 唯一约束兜底, 同一操作者
  并发/重复提交只产生一条有效审批(幂等返回)。

#### 限定执行窗口(compensation_windows)

运维可为活动补偿任务预约限定执行窗口(闭区间, 可多个; 空列表清空):

- 窗口外**显式执行/重试 409 拒绝**; worker 在**动作边界自动暂停**,
  已完成动作与失败动作进度全部保留(`window_pause_reason` 记录原因),
  重新进入窗口后自动从下一个未完成动作继续(暂停/恢复只追加 `COMP_WINDOW` 事件);
- **冲突检测**: 预约窗口与其他活动补偿任务时间重叠且动作批次集合相交时 409,
  返回每个占用者的任务 id/创建者/双方窗口/冲突时间段/共享批次;
- 终态任务不能再改窗口; 与当前窗口完全相同的预约幂等无副作用;
- 窗口边界持久化, 重启后按当前时刻重新判定, 不丢暂停状态也不偷跑。

#### 任务取消

`POST .../{task_id}/cancel` 取消 QUEUED/RUNNING/PARTIAL 任务(终态):
未执行动作终止, 已完成动作保留; 未决审批置 `TASK_CANCELED`, 审批历史仍可查询。

页面"审计事件回放与补偿"卡片展示事件链(全局序/流序/哈希/载荷/投影关联)、
快照校验结果与逐条原因、补偿动作状态/门禁/操作者/执行与撤销事件关联,
并展示**风险分级、审批状态/当前审批人、审批失效历史、预约窗口、窗口冲突详情、
窗口暂停原因与取消信息**, 提供查询、补录、生成快照、预览、创建任务、
双人审批/拒绝、预约窗口、执行、重试、取消、撤销操作。

## 审计证据查询与一致性证明

在统一不可变事件流之上, 为运维提供**固定读取边界**的跨流证据时间线与**可复算**的
分段 JSONL 证据包。严格只读(除自身会话/导出/下载令牌表外不写任何业务数据)。

### 证据查询会话(evidence_sessions / evidence_pages)

- `POST /api/admin/evidence/sessions {plan_id, start_global_seq?, start_ts?, end_ts?,
  event_types?, sources?}` 按 plan_id 创建**持久化会话**: 一次性固定筛选条件、起始
  global_seq 与读取边界 `upper_global_seq`(创建时刻全库最大 global_seq)以及每条涉及流
  (计划流 + 各步骤批次流)的**边界锚点**(尾 stream_seq/hash/global_seq)。补偿控制流
  (`COMP_*`)追加在计划流上, 因此跨计划流/批次流/补偿控制流的统一时间线天然完整。
- **固定边界**: 会话之后新写入事件 global_seq 必然更大, 永远不进入已开始的结果集 ——
  翻页不漏事件、不重复事件, 也不会把新事件插入旧结果; 同计划先后两个会话边界相互独立。
  无结果是合法边界(`expected_count=0`, 立即 CLOSED, 仍可导出空包)。
- 翻页 `POST .../sessions/{id}/pages {cursor?, limit?}`: keyset 稳定游标(HMAC 不透明,
  服务端 `delivered` 位置单调推进), **cursor 只能沿同一会话继续**: 第一页可省略,
  之后必须用上一页 `next_cursor`; 漏页/重放/跨会话/损坏 → 409 `cursor_invalid`,
  状态不推进。
- **每页哈希链连续性证明**(一次请求的固定读取边界内完成):
  - global_seq 片段内严格递增且不与已交付位置重复(`global_seq_duplicate`);
  - 按流重算 `stream_hash` 逐行一致(`stream_hash_mismatch`),
    `prev_stream_hash` 与 DB 实读的流上前一事件衔接(`prev_hash_mismatch`),
    `stream_seq` 从流上前一事件 +1 连续(`stream_seq_gap`/`stream_seq_duplicate`);
    **片段前边界事件也自证哈希**(已交付页被篡改照样拦住);
  - 时间戳倒流超 `AUDIT_MAX_OUT_OF_ORDER_SECONDS` → `out_of_order`;
  - 各流边界锚点每页复核: 边界内删除/篡改尾事件 → `boundary_anchor_mismatch`
    (边界之后正常追加不报错, 但其前向哈希必须仍连回锚点)。
  - 任一失败: **拒绝返回该页**, HTTP 422 + 机器可读 `breaks[]`(原因码/流/stream_seq/
    global_seq/期望与实际值), 会话置 **BROKEN 锁定**(断链证据永久保留), 不能继续翻页
    或导出; 需要取证时新建会话。
- 每条事件返回: 事件来源(`internal/api/system`)、操作者、流归属(`flow=plan/batch`)、
  关联批次/步骤/扫描、补偿任务 id 与动作 seq、correlation 与逐行哈希;
  每页返回固定条件/固定边界/当前游标/片段前后流边界/`fragment_digest`。
  逐页留痕(evidence_pages: 范围/条目数/片段摘要/流边界)持久化可查。

### 证据导出任务(evidence_exports / evidence_export_segments)

异步分段生成**确定性 JSONL 证据包**(zip; worker 受 `EVIDENCE_EXPORT_MAX_CONCURRENCY`
限制, 随 `PLAN_WORKER_ENABLED` 一并开关):

```
QUEUED ─claim→ RUNNING ─全部分段+清单完成→ COMPLETED(终态, 包不可变)
 │               ├─ pause(分段边界)→ PAUSED ─resume→ QUEUED
 │               ├─ 导出期间事件链变化 → PAUSED(chain_changed, 记录断链证据; 绝不出完整包)
 │               └─ 可恢复错误 → FAILED ─resume→ QUEUED(失败重试不重复写包)
 └────────────────────────── cancel → CANCELED(终态, 禁止继续)
```

- **分段幂等**: 段内容完全由(固定边界, 段序号, 段大小)决定; 段产物
  (`segments/segment-NNNNN.jsonl` 原子写 + 逐行 `fragment_digest` + 文件 sha256)
  先落库提交(崩溃边界), 重启/重试只生成缺失或失配段, 好段字节级复用;
  打包前对全量范围**再做一次链证明**并逐段复算摘要。
- **导出期间检测到事件链变化 → 自动 PAUSED**(`paused_reason=chain_changed`,
  `chain_break` 带原因码/流/位置/分段号), 绝不产出标记完整的包; `resume` 前从边界起点
  **重新全量校验**, 不通过 → 409 保持暂停; 通过后失配段重生成、从首个未完成段继续。
- 暂停/恢复/取消/创建全部走 `evidence.*` 幂等命名空间(同态重复 `already_in_state`,
  同键重放 `replayed`); CANCELED 后禁止 resume/pause; FAILED 可 resume
  (被外部删改的段文件检测后仅重生成该段)。
- 包内容(确定性 zip, 固定文件顺序/时间戳):
  - `metadata.json` 会话/边界/筛选/操作者/段大小/内容清单;
  - `events.jsonl` 全量规范化事件行; `segments/segment-*.jsonl` 分段;
  - `manifest.json`: 逐段(范围/计数/逐行摘要/文件 sha256)、**每条流摘要**
    (事件数/起止 stream_seq/global_seq/补偿控制事件数/操作者/批次/`stream_digest`)、
    逐文件 sha256、`content_digest`(仿 git 风格文件名有序拼接)与
    **`manifest_hash`(manifest 除自身外字段的 sha256, 可离线复算)**。
  - 事件规范化字段与翻页 `fragment_digest` 完全一致, 页面留痕可与包内段直接核对。
- **一次性下载**: `POST .../exports/{id}/download-token`(仅会话创建者, 令牌明文只返回
  一次, 默认 10 分钟过期, 持久化支持重启兑换) → `GET .../downloads/{token}`,
  首次下载即失效(重复/过期/缺失明确 409/404); `GET .../exports/{id}/verify`
  回读包重算逐文件/content/manifest 摘要, 不一致或包缺失 → FAILED 留证(原包保留)。
- 页面"审计证据查询与一致性证明"区: 创建会话表单、固定条件/边界锚点/当前 cursor/
  校验状态徽章、逐页证明(片段摘要/前后流边界/断链位置)、导出任务表(进度条/暂停原因/
  断链证据/失败原因/逐段与流摘要/manifest_hash)、暂停恢复取消/校验/一次性下载操作。
- 配置: `EVIDENCE_SEGMENT_SIZE`(默认 200)、`EVIDENCE_PAGE_LIMIT`(默认 100)、
  `EVIDENCE_EXPORT_MAX_CONCURRENCY`(默认 2)、`EVIDENCE_WORKER_POLL_INTERVAL`、
  `EVIDENCE_STORE_DIR`(默认 `./evidence_store`)、`EVIDENCE_DOWNLOAD_TTL_SECONDS`
  (默认 600)、`EVIDENCE_CURSOR_SECRET`(游标 HMAC 密钥)。
- 接口文档见 [`docs_evidence.md`](docs_evidence.md)。

### 证据复核与签署归档(evidence_reviews / evidence_review_conclusions)

在已翻完(CLOSED)且未断链的查询会话与 **COMPLETED** 证据包之上做**双人逐事件复核**:

```
OPEN ─两名不同操作者对固定范围全部事件签署→ archive → ARCHIVED(终态, 签署摘要不可变)
  │  ▲
  │  └ reverify 通过(依据修复; scope_version+1, 旧结论留史, 当前签署清空重签)
  └─ 提交/归档/reverify 发现依据变化 → INVALIDATED(冻结提交与归档, 机器可读原因)
```

- `POST /api/admin/evidence/reviews {session_id, export_id?}` 创建即**固定依据**(仅会话
  创建者; 会话须 CLOSED 且有 COMPLETED 包): 筛选条件快照、起止 global_seq、范围内
  事件数、**范围指纹**(逐行规范化事件有序拼接 sha256)、导出 id 与 **manifest_hash**。
  同(会话,导出)重复创建幂等回显; 已归档后禁止再建。
- 逐事件结论 `POST .../reviews/{id}/conclusions` 带 **`If-Match: <version>`** 头:
  `CONFIRMED/QUESTIONED/EXCLUDED`(存疑·排除说明必填), 只能引用固定会话范围内事件
  (越界 409 `event_out_of_scope`)。每次成功提交 version+1; 过期/缺失 If-Match → 409
  (`version_conflict`/`if_match_required`, 后写不覆盖先写); 同一依据版本
  (event, operator) 唯一, 重复签署 409 `duplicate_signature` 不覆盖; 同键重放幂等。
- **依据再校验**: 每次提交/归档/显式 `POST .../reviews/{id}/reverify` 都重新
  全量重算哈希链、范围指纹、manifest_hash 固定值比对并回读 zip 包重算摘要(只读,
  不改会话/导出/事件)。不一致 → 复核单 **INVALIDATED**, `invalid_reason` 记录
  `chain_broken/scope_changed/manifest_hash_mismatch/package_verification_failed`
  及定位明细; 修复后 reverify 通过 → OPEN(scope_version+1, 历史结论在 `history[]` 保留)。
- **归档** `POST .../reviews/{id}/archive`: 每个固定范围内事件均有两名不同操作者的
  当前版本结论(缺签 `signatures_incomplete`, 仅一人 `two_operators_required`)且归档前
  依据再校验通过; 生成不可变**签署摘要** `signed_summary`(结论统计 by_verdict、
  事件范围 first/last/upper global_seq 与 scope_fingerprint、操作者列表、
  manifest_hash、逐事件签署明细)与可离线复算的 `signature_hash`。归档后提交/重新
  校验/再创建一律拒绝; 重复归档幂等(`already_in_state`)。
- 页面/接口展示: 复核单详情含固定依据、`version`/`scope_version`、**待处理数量
  pending_count**、每个事件详情与其结论/说明/操作者/签署版本、失效原因、事件流水、
  历史版本签署与归档签署摘要; 列表支持 session/export/plan/status 过滤。
- 严格只读: 事件链、查询会话/页面与已完成导出包不被本流程修改(仅写
  evidence_reviews* 自身表)。页面"证据复核与签署归档"区提供创建/签署/重新校验/归档操作。
- 接口文档见 [`docs_evidence.md`](docs_evidence.md) 第 5–6 节。

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
POST /api/admin/plans/{id}/window                  (启动前整体替换; 空列表清空限制; POST 同义)
PUT/POST /api/admin/plans/{id}/quality-rules  {operator, idempotency_key, plan_id,
                                                rules:[{id,name,type,field,severity,params}], note?}
                                                      绑定/更新可版本化质量规则(内容变化才升版本)
GET  /api/admin/plans/{id}/quality-rules             规则集(当前版本规则+全部历史版本)
POST /api/admin/plans/{id}/quality-scans             对计划涉及批次生成质量扫描并排队(活动扫描幂等)
GET  /api/admin/plans/{id}/quality-gate              门禁结果(规则版本/最新扫描/失效原因/问题计数)
GET  /api/admin/plans/{id}/quality-overview          规则版本+扫描列表(进度/问题分布)+门禁
GET  /api/admin/plans/{id}/quality-issues?scan_id=&batch_id=&severity=&status_filter=&rule_type=
                                                      按计划查询质量问题(含可追踪样本)
GET  /api/admin/plans/{id}/quality-history           修复批次与豁免全量历史(绑定规则版本)
GET  /api/admin/plans/{id}/quality-holds             质量门禁暂停/自动恢复历史(触发来源/关联重扫/暂停原因)
POST /api/admin/quality-rescan/sweep                 手动触发一次失效兜底编排(过期/版本变化; 与 worker tick 等价)
GET  /api/admin/quality-scans?plan_id=&status_filter= 质量扫描任务列表
GET  /api/admin/quality-scans/{id}                   扫描详情(批次进度/问题分布/过期原因/事件流水)
GET  /api/admin/quality-scans/{id}/issues?severity=&status_filter=  扫描问题明细(含样本)
POST /api/admin/quality-scans/{id}/pause|resume|cancel {operator, idempotency_key}
POST /api/admin/plans/{id}/quality-fixes      {operator, idempotency_key, plan_id,
                                                issue_ids:[...], note?}  创建修复批次(逐项重跑规则核验)
POST /api/admin/plans/{id}/quality-exemptions {operator, idempotency_key, plan_id,
                                                issue_id, reason}       阻断问题豁免(带原因,绑定规则版本)
POST /api/admin/plans/{id}/quality-exemptions/{eid}/revoke {operator, idempotency_key,
                                                plan_id, reason}        撤销豁免(问题重新打开)
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
GET  /api/admin/archives             归档目录检索(?replay_id=&biz=&report_version=&content_digest=
                                                      &status=&retention=&include_cleaned=; 默认仅存活归档)
GET  /api/admin/archives/by-digest/{content_digest}   按内容摘要(完整/≥12位前缀)查询归档与引用关系
GET  /api/admin/archives/{id}                        归档详情(进度/当前单元/失败原因/摘要/保留/引用/清单/事件)
PUT  /api/admin/archives/{id}/retention {operator, idempotency_key, mode, retain_until?}
                                                      保留策略: UNTIL(到期保留)/PERMANENT(永久)/NONE(清除)
POST /api/admin/archives/{id}/pause|resume|cancel    {operator, idempotency_key}
GET  /api/admin/archives/{id}/download?operator=     下载不可变归档包(下载期间清理逐项跳过)
GET  /api/admin/archives/{id}/verify?operator=       重算摘要校验(校验期间清理逐项跳过; 不一致/包缺失 -> FAILED)
POST /api/admin/archive-cleanups      {operator, idempotency_key, archive_ids:[...]}
                                                      发起归档清理计划并排队(逐项跳过原因)
GET  /api/admin/archive-cleanups                     清理计划列表(进度汇总)
GET  /api/admin/archive-cleanups/{id}                清理计划详情(逐项 CLEANED/SKIPPED/失败原因 + 事件)
POST /api/admin/archive-cleanups/{id}/pause|resume|cancel  {operator, idempotency_key}
GET  /api/admin/audit?batch_id=&plan_id=           审计日志(可按批次或计划过滤)
POST /api/records                                  旧结构写入(批次冻结期 423 / 批次切换后 410; 执行态计划涉及批次写入自动编排重扫)
POST /api/v2/records                               新结构写入(仅所属批次 DONE)
GET  /api/records/{id}                             按所属批次阶段返回 旧/新/双读+diff
GET  /api/records/{id}/compare                     批次冻结窗内双读比对

# ---- 审计事件回放与补偿 ----
GET  /api/admin/audit-events?plan_id=&batch_id=&start_ts=&end_ts=&event_type=&after_global_seq=&limit=
                                                     统一不可变事件链(keyset 分页; 批次查询按 correlation 折叠)
POST /api/admin/audit-events                       {operator, idempotency_key, content, plan_id?, batch_id?, event_ts?}
                                                     显式补录备注(EXTERNAL_NOTE; 乱序超阈值 409)
POST /api/admin/audit-snapshots                    {operator, idempotency_key, plan_id, target_at?, ttl_seconds?}
                                                     生成时间点快照并校验(REJECTED 返回 422 且逐条原因, 快照仍持久化)
GET  /api/admin/audit-snapshots?plan_id=&status_filter=
GET  /api/admin/audit-snapshots/{id}               快照详情(固化事件链 + 批次版本/规则版本校验结果)
GET  /api/admin/audit-snapshots/{id}/preview       待补偿动作预览(类型/目标/预期/当前/门禁/execution_allowed)
POST /api/admin/compensations                      {operator, idempotency_key, snapshot_id, risk_level?}
                                                     基于 VALID 快照创建补偿任务(同快照同时唯一;
                                                     风险按动作自动推导, 破坏性动作 HIGH 须双人审批)
GET  /api/admin/compensations?plan_id=&snapshot_id=&status_filter=
GET  /api/admin/compensations/{id}                 补偿任务详情(风险/审批状态/审批与失效历史/窗口/冲突/暂停原因)
POST /api/admin/compensations/{id}/approvals       {operator, idempotency_key} 双人审批通过(创建者不可; 两人独立)
POST /api/admin/compensations/{id}/rejections      {operator, idempotency_key, reason} 审批拒绝(原因必填)
PUT  /api/admin/compensations/{id}/windows         {operator, idempotency_key, windows:[{starts_at,ends_at}]}
                                                     预约/替换限定执行窗口(冲突 409 返回占用者与冲突时间; 空列表清空)
POST /api/admin/compensations/{id}/cancel          {operator, idempotency_key, reason?} 取消任务(保留已完成动作)
POST /api/admin/compensations/{id}/execute         {operator, idempotency_key} 幂等执行(审批/窗口闸门; 部分失败 PARTIAL)
POST /api/admin/compensations/{id}/retry           {operator, idempotency_key, action_seq} 逐动作失败重试
POST /api/admin/compensations/{id}/undo            {operator, idempotency_key} 逆序整体撤销(before_image 恢复, 不受 TTL 限制)

# ---- 审计证据查询与一致性证明 ----
POST /api/admin/evidence/sessions       {operator, idempotency_key, plan_id, start_global_seq?,
                                         start_ts?, end_ts?, event_types?, sources?}
                                                      创建固定边界证据会话(计划流+批次流+补偿控制流)
GET  /api/admin/evidence/sessions[?plan_id=&status_filter=]
GET  /api/admin/evidence/sessions/{id}[?with_pages=]   会话详情(条件/边界锚点/cursor/校验状态/逐页留痕)
POST /api/admin/evidence/sessions/{id}/pages {operator, cursor?, limit?}
                                                      沿会话翻一页(哈希链连续性证明; 断链 422+breaks,
                                                      游标错误 409; BROKEN 会话锁定)
POST /api/admin/evidence/exports        {operator, idempotency_key, session_id, segment_size?}
                                                      异步分段 JSONL 证据包并排队(活动导出幂等回显)
GET  /api/admin/evidence/exports[?session_id=&plan_id=&status_filter=]
GET  /api/admin/evidence/exports/{id}[?with_events=]   导出详情(进度/断链证据/逐段/流摘要/manifest/事件)
POST /api/admin/evidence/exports/{id}/pause|resume|cancel {operator, idempotency_key}
                                                      暂停(分段边界)/恢复(断链先全量重校, 从上次成功段继续)/取消(终态)
GET  /api/admin/evidence/exports/{id}/verify?operator= 回读包重算逐文件/content/manifest 摘要(失配→FAILED)
POST /api/admin/evidence/exports/{id}/download-token {operator, idempotency_key}
                                                      签发一次性下载令牌(仅会话创建者, 明文仅此返回)
GET  /api/admin/evidence/downloads/{token}?operator=  兑换一次性下载(zip; 重复/过期 409/404)

# ---- 证据复核与签署归档 ----
POST /api/admin/evidence/reviews        {operator, idempotency_key, session_id, export_id?}
                                                      从 CLOSED 会话+COMPLETED 包创建复核单(固定依据)
GET  /api/admin/evidence/reviews[?session_id=&export_id=&plan_id=&status_filter=]
GET  /api/admin/evidence/reviews/{id}[?pending_only=&limit=&offset=&with_history=&with_events=]
                                                      复核单页面: 事件详情/结论/说明/操作者/版本/待处理量
POST /api/admin/evidence/reviews/{id}/conclusions (If-Match: <version>)
                   {operator, idempotency_key, global_seq, verdict: CONFIRMED|QUESTIONED|EXCLUDED, note?}
                                                      逐事件签署(越界/过期/重复签署 409; 依据变化→INVALIDATED)
POST /api/admin/evidence/reviews/{id}/reverify {operator, idempotency_key}
                                                      重新校验固定依据(链/范围指纹/manifest_hash/包)
POST /api/admin/evidence/reviews/{id}/archive {operator, idempotency_key}
                                                      双人签全后归档(不可变签署摘要; 终态禁止修改)
```

幂等：所有管理动作要求 `idempotency_key`，重复执行返回首次结果（`replayed: true`），无副作用；
请求哈希包含动作与目标(批次或计划)，同键跨目标/跨动作/不同请求体复用返回 409。重启后重放依然正确。
计划动作与批次动作使用独立命名空间(`plan.*` 与 `freeze/validate/...`)，互不撞键。

## 测试

```bash
python3 -m pytest tests/ -q   # 315 个用例(含审计证据查询与一致性证明 36 个、证据复核与签署归档 22 个):
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
# 归档目录与生命周期(16): 按业务分组/回放/报告版本/状态/保留模式多维检索(默认仅存活)/
#           按摘要查询接口(完整+前缀消歧, 引用组)/保留策略 PERMANENT·UNTIL·NONE 与非法参数拒绝/
#           保留期与永久保留逐项跳过, 到期后可清理/清理逐项跳过原因(不存在·活动中·已清理)/
#           清理计划排队执行/逐项边界暂停恢复续跑/取消未处理项跳过/创建与控制幂等/
#           下载/校验占用计数期间清理跳过且文件不删(HTTP 下载持有计数)/
#           同摘要去重非canonical清理解除引用保文件/canonical移交物理文件与引用数/
#           移交失败 digest_referenced 跳过/重启保留保留策略·引用关系·逐项清理进度与 RUNNING 复位/
#           重启清零在途占用计数/FAILED 计划 resume 重试失败项/status 含清理计划且已清理归档不展示
# 迁移前质量门禁(39): 无规则计划不受门禁/规则保存版本化与历史不可变/同内容不升版本/同键重放/
#           规则聚合校验不落数据/四类规则求值(必填·格式·跨字段eq/ne·数值与长度范围)/
#           严重级别与可追踪样本/多批次扫描进度与数据指纹/无规则与已启动计划拒绝扫描/
#           活动扫描重复创建幂等/暂停在批次边界(排队暂停不被认领)/恢复续跑/取消跳过未开始批次/
#           批次删除 FAILED+resume 成功/并发上限排队与完成后放行/
#           阻断问题阻止启动且 WARNING 不阻断/修复批次 RESOLVED·STILL_OPEN·NOT_FOUND·REJECTED/
#           修复后需重扫/豁免放行不改数据/豁免幂等与撤销重开/修复豁免历史绑定规则版本/
#           规则版本变化旧结果 STALE/批次数据变化 STALE/结果过期 STALE/同版本重扫豁免继承/
#           扫描控制幂等与状态冲突/同键跨动作复用 409/重启 RUNNING 复位排队与 RUNNING 批次复位/
#           重启后规则·扫描·豁免·门禁状态保留/按计划多维查询问题与门禁结果/总览与扫描列表/
#           门禁与高风险审批叠加
# 质量结果失效自动重扫(25): 批次写入执行态计划自动编排唯一重扫(触发来源/取代关联/受影响
#           批次)/重复写入合并不重复/DRAFT 与批次外写入不触发/同值写入不触发/
#           规则版本变化即时重扫(基于新版本)/DRAFT 版本变化不自动/过期 sweep 兜底编排且不重复/
#           执行器步骤边界暂停停原步骤(计数不复位)→干净重扫完成后从原步骤自动恢复/
#           重扫发现阻断问题保持暂停(BLOCKED)/修复后重扫恢复/豁免不重扫直接恢复/
#           多触发来源合并唯一任务/并发双会话只落一个重扫/重扫 FAILED→resume 自愈恢复/
#           计划取消终止 hold 与重扫且永不复活/COMPLETED 后写入 410 不编排/
#           重扫覆盖计划全部批次且记录受影响范围/DONE 批次不影响其他执行态计划/
#           重启保留待处理重扫与暂停(跑一半的重扫续跑)/重启后过期补编排/
#           接口与 status 展示触发来源/重扫关联/暂停/多轮暂停恢复历史/
#           hold 中允许手动扫描且通过后解除/普通 RUNNING 拒绝手动扫描/
#           用户暂停叠加后取消/重扫被暂停不恢复/步骤完成后的边界也复评门禁
# 审计事件回放与补偿(42): 流序连续/全局序递增/哈希链可重算(含操作者)/生命周期·规则·扫描·
#           暂停事件投影/写入事件/批次流扇出与 correlation 折叠/keyset 分页/时间范围过滤/
#           显式补录幂等与乱序拒绝/跨批次流隔离/正常快照 VALID/非终态·早于终结·流缺口·断链·
#           篡改·批次漂移·规则版本冲突·批次 epoch 跳变·乱序·取消计划快照拒绝原因/
#           REJECTED 快照持久化且不能建补偿/快照 TTL 过期/动作预览(回填·清理·解冻)/
#           幂等执行不重复写入/只追加 COMP_* 不改原事件/门禁阻断 FAILED 后恢复重试成功/
#           取消计划补偿逐动作 PLAN_CANCELED 拒绝/部分失败 PARTIAL/多余记录清理与撤销/
#           before_image 撤销恢复(插入删除·改值还原·撤销关联执行事件)/执行与撤销操作者/
#           同快照并发不建重复任务/worker 逐动作执行/COMP_FAILED 事件/
#           重启 RUNNING→QUEUED·PARTIAL 保留失败原因·UNDO_RUNNING→UNDO_PARTIAL 续撤销
# 审计证据查询与一致性证明(36): 跨计划/批次/补偿控制流统一时间线排序/时间范围与类型过滤/
#           start_global_seq/固定边界后新增事件不进入既有会话(前后边界稳定, 多会话边界独立)/
#           翻页不漏不重不插新(片段摘要+流边界)/无游标只能首页/旧游标重放/跨会话游标/坏游标拒绝/
#           缺失 stream_seq_gap+prev_hash+stream_hash 拒绝并锁定会话/payload 篡改/重链后锚点不匹配/
#           边界尾事件删除/乱序事件/片段前边界被篡改/重复 global_seq/
#           导出包 manifest/content/逐流摘要可复算/重启分段幂等不重写/暂停恢复分段边界/取消终态禁止继续/
#           导出期间链变化 PAUSED+断链证据且不出完整包/恢复前重校失败保持暂停/
#           失败重试仅重生成失配段(好段不重写)/活动导出与完成后创建幂等/空结果边界导出空包/
#           一次性下载令牌(单次使用/重放/未知 404/未完成拒绝)/仅创建者权限/未知计划 404/非法过滤 422/
#           会话与控制幂等(同键重放/跨请求体 409)/重启 RUNNING 回 QUEUED 续跑/
#           高风险计划审批+撤销+窗口与证据接口共存/补偿列表与撤销接口不回归
# 证据复核与签署归档(22): 创建固定依据(筛选/起止 global_seq/manifest_hash/范围指纹)/
#           须 CLOSED 会话+COMPLETED 包/创建者权限/不存在与跨会话导出拒绝/幂等创建与列表过滤/
#           页面展示事件详情+结论+说明+操作者+版本+待处理量/存疑排除须带说明/双签后待处理下降/
#           If-Match 缺失非法过期冲突不覆盖/引用会话范围外事件拒绝/同操作者重复签署去重/同键重放不重复/
#           双人双签才允许归档(缺签/仅一人拒绝)/归档不可变签署摘要可离线复算/归档后禁止提交重校重建(幂等回显)/
#           混合结论统计与重启保留/链变化 INVALIDATED 机器可读原因且禁止提交归档/修复后重校 scope_version+1 旧结论留史重签归档/
#           证据包被篡改失效/分页导出下载校验与补偿审批窗口接口不回归
```
