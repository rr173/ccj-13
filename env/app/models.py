from sqlalchemy import (
    JSON, Boolean, Column, DateTime, ForeignKey, Index, Integer, String,
    UniqueConstraint, func,
)
from sqlalchemy.orm import relationship

from datetime import datetime, timezone

from .db import Base


def _utcnow() -> datetime:
    """Python 端 UTC naive 默认时间(微秒精度): 回放排队 FIFO 依赖创建时刻排序,
    SQLite 的 CURRENT_TIMESTAMP 只有秒级精度会让同秒创建的任务顺序不确定。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)

# 每个批次独立的阶段状态机:
#   NORMAL --freeze--> FROZEN --validate--> VALIDATING --(通过)--> VALIDATED --cutover--> DONE
#     ^                  |                      |                        |
#     +---- recover(必须带 reason, 回到冻结前可写状态) ----+------------+
# DONE 为终态: 不允许自动回退, 避免"两套状态都说自己已切开"。
PHASES = ("NORMAL", "FROZEN", "VALIDATING", "VALIDATED", "DONE")


class MigrationBatch(Base):
    """按业务分组的迁移批次。一批覆盖一段记录 id 范围 [id_start, id_end],
    范围之间不允许重叠, 因此任意记录至多属于一个批次。

    epoch 为批次内栅栏令牌: 每次迁移动作 +1, 所有变更都带 epoch 条件更新,
    防止并发管理员/重启后出现双主状态。批次外的记录不受任何闸门约束。"""

    __tablename__ = "migration_batches"

    id = Column(String(32), primary_key=True)          # "B" + 随机串
    biz = Column(String(128), nullable=False)          # 业务分组名
    id_start = Column(Integer, nullable=False)         # 记录范围(含)
    id_end = Column(Integer, nullable=False)           # 记录范围(含)
    phase = Column(String(32), nullable=False, default="NORMAL")
    epoch = Column(Integer, nullable=False, default=0)
    freeze_version = Column(String(64), nullable=True)   # 本次冻结窗标识
    watermark = Column(Integer, nullable=True)  # 回填水位(已处理的最大记录 id), 冻结时置为 id_start-1
    active_schema = Column(String(8), nullable=False, default="old")  # old | new
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class RecordOld(Base):
    """旧结构: 标签是逗号分隔字符串。"""

    __tablename__ = "records_old"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=False, default="")
    tags_csv = Column(String(500), nullable=False, default="")


class RecordNew(Base):
    """新结构: 标签是数组, 带 schema_version。"""

    __tablename__ = "records_new"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=False, default="")
    tags = Column(JSON, nullable=False, default=list)
    schema_version = Column(Integer, nullable=False, default=2)


class AuditLog(Base):
    """只追加审计日志: 批次、操作者、动作、阶段迁移、版本、水位、差异、恢复原因。"""

    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, server_default=func.now())
    batch_id = Column(String(32), nullable=True, index=True)  # 所属批次
    plan_id = Column(String(32), nullable=True, index=True)   # 所属迁移计划(计划级审计)
    step_id = Column(Integer, nullable=True)                  # 所属计划步骤
    operator = Column(String(128), nullable=False)
    action = Column(String(32), nullable=False)       # create/freeze/validate/cutover/recover/boot/plan.*
    from_phase = Column(String(32), nullable=True)
    to_phase = Column(String(32), nullable=True)
    epoch = Column(Integer, nullable=True)
    app_version = Column(String(32), nullable=True)
    freeze_version = Column(String(64), nullable=True)
    watermark = Column(Integer, nullable=True)
    diffs = Column(JSON, nullable=True)               # 校验差异明细
    reason = Column(String(500), nullable=True)       # 恢复/失败原因


# 迁移计划状态机:
#   DRAFT --start--> RUNNING --pause--> PAUSED --resume--> RUNNING
#      |                |                   |
#      |                +--某步重试耗尽--> HALTED --resume--> RUNNING
#      |                |
#      +--cancel--------+--cancel--> CANCELED(终态)
#   RUNNING --全部步骤成功--> COMPLETED(终态)
# 计划风险等级: 高风险计划启动前必须经"不同于创建者"的另一名管理员审批通过。
RISK_LEVELS = ("LOW", "HIGH")
# 审批状态机(仅高风险计划使用; 低风险计划恒为 NOT_REQUIRED, 可直接启动):
#   PENDING --approve--> APPROVED --revoke--> PENDING(启动前可撤销审批)
#   PENDING --reject----> REJECTED --approve--> APPROVED(拒绝后可重新审批通过)
APPROVAL_STATUSES = ("NOT_REQUIRED", "PENDING", "APPROVED", "REJECTED")
PLAN_STATUSES = ("DRAFT", "RUNNING", "PAUSED", "HALTED", "COMPLETED", "CANCELED")
# 步骤状态: BLOCKED 依赖未满足; PENDING 等待执行; RUNNING 执行中; SUCCESS 成功;
#           FAILED 一次尝试失败(仍有重试额度, 下一 tick 自动重试); HALTED 重试耗尽, 计划停住
STEP_STATUSES = ("BLOCKED", "PENDING", "RUNNING", "SUCCESS", "FAILED", "HALTED", "SKIPPED")


class MigrationPlan(Base):
    """迁移计划: 把多个已有迁移批次组织成带唯一顺序与依赖的步骤图。

    只能在依赖步骤成功后推进下一步; 每步执行现有批次流程(freeze->validate->cutover)。
    失败按 max_retries 自动重试, 超过次数进入 HALTED 并阻止后续步骤。
    批次在同一计划内、以及在未终结的计划之间都不允许被重复占用。"""

    __tablename__ = "migration_plans"

    id = Column(String(32), primary_key=True)          # "P" + 随机串
    name = Column(String(200), nullable=False)
    status = Column(String(16), nullable=False, default="DRAFT", index=True)
    max_retries = Column(Integer, nullable=False, default=0)  # 每步首次失败后的额外重试次数
    last_error = Column(String(500), nullable=True)    # 最近一次错误(页面"最近错误")
    failed_step_id = Column(Integer, nullable=True)    # 当前卡住的步骤
    # 风险与审批
    risk_level = Column(String(8), nullable=False, default="LOW")
    approval_status = Column(String(16), nullable=False, default="NOT_REQUIRED")
    approved_by = Column(String(128), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    reject_reason = Column(String(500), nullable=True)  # 最近一次拒绝原因(拒绝时阻止启动)
    # 执行窗口运行态闸门: None=计划无窗口限制; True/False=最近一次判定在窗口内/外。
    # 只是持久化的"最近判定": 窗口边界本身在 plan_windows 表, 每次 tick 重新判定,
    # 重启不会丢失窗口外暂停状态, 重新进入窗口也能自动继续。
    # 质量门禁失效后的自动暂停(quality hold): True 表示执行器在步骤边界因门禁
    # 失效(数据写入/规则版本变化/扫描过期/重扫发现阻断问题)暂停推进, 等待自动
    # 重扫完成且门禁重新通过。暂停历史在 quality_gate_holds 表(只追加)。
    # 与用户手动 PAUSED 独立: quality_hold 期间计划状态仍为 RUNNING/HALTED,
    # worker 每个 tick 重新评估门禁, 通过后自动从原步骤继续。
    quality_hold = Column(Boolean, nullable=False, default=False)
    window_open = Column(Boolean, nullable=True)
    created_by = Column(String(128), nullable=False)
    started_by = Column(String(128), nullable=True)
    updated_by = Column(String(128), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    windows = relationship("PlanWindow", cascade="all, delete-orphan",
                           order_by="PlanWindow.starts_at")


class PlanWindow(Base):
    """允许执行的时间窗口(闭区间 [starts_at, ends_at], UTC 存储):
    计划只在任一窗口内推进; 窗口外在步骤边界暂停, 重新进入窗口后由 worker 自动继续。
    启动前管理员可以整体替换/清空窗口。无窗口行的计划不受时间限制。"""

    __tablename__ = "plan_windows"
    __table_args__ = (
        UniqueConstraint("plan_id", "starts_at", "ends_at", name="uq_plan_window"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    starts_at = Column(DateTime, nullable=False)
    ends_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class PlanStep(Base):
    """计划步骤: 绑定一个已有批次, seq 在计划内唯一, 依赖通过 PlanStepDependency 表达。

    attempts 记录已执行的尝试次数(首次执行即 +1); 每次尝试另落一条 PlanStepEvent,
    记录状态、操作者(启动/恢复计划的管理员)、失败原因与批次动作结果。"""

    __tablename__ = "plan_steps"
    __table_args__ = (
        UniqueConstraint("plan_id", "seq", name="uq_plan_step_seq"),
        UniqueConstraint("plan_id", "batch_id", name="uq_plan_step_batch"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String(32), ForeignKey("migration_plans.id"), nullable=False, index=True)
    seq = Column(Integer, nullable=False)              # 计划内唯一顺序
    batch_id = Column(String(32), ForeignKey("migration_batches.id"), nullable=False)
    status = Column(String(16), nullable=False, default="BLOCKED")
    attempts = Column(Integer, nullable=False, default=0)
    # 尝试轮次: 每次从 HALTED 恢复 +1。批次动作幂等键带轮次,
    # 保证同一轮内崩溃重放走旧结果, 恢复后修复数据是全新尝试而非重放旧失败
    attempt_round = Column(Integer, nullable=False, default=1)
    max_retries = Column(Integer, nullable=False, default=0)
    last_error = Column(String(500), nullable=True)
    executed_by = Column(String(128), nullable=True)   # 实际推进该步骤的操作者(计划启动者)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    deps = relationship("PlanStepDependency", cascade="all, delete-orphan")


class PlanStepDependency(Base):
    """步骤依赖边: step_id 必须等 depends_on_seq 对应步骤成功后才能执行。"""

    __tablename__ = "plan_step_dependencies"
    __table_args__ = (
        UniqueConstraint("plan_id", "step_id", "depends_on_seq", name="uq_plan_dep"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String(32), ForeignKey("migration_plans.id"), nullable=False, index=True)
    step_id = Column(Integer, ForeignKey("plan_steps.id"), nullable=False, index=True)
    depends_on_seq = Column(Integer, nullable=False)


class PlanStepEvent(Base):
    """步骤执行流水(只追加): 每次尝试的状态、操作者、失败原因、批次动作结果快照。"""

    __tablename__ = "plan_step_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, server_default=func.now())
    plan_id = Column(String(32), ForeignKey("migration_plans.id"), nullable=False, index=True)
    step_id = Column(Integer, nullable=False, index=True)
    attempt = Column(Integer, nullable=False)
    event = Column(String(32), nullable=False)         # start/retry/success/fail/halted/reset/skip
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)               # 批次动作返回 / 差异等


class IdempotencyKey(Base):
    """幂等键: 重复执行返回首次结果, 不产生二次副作用。
    请求哈希含动作与批次, 同键跨批次/跨动作复用会被拒绝。"""

    __tablename__ = "idempotency_keys"

    key = Column(String(128), primary_key=True)
    action = Column(String(64), nullable=False)
    request_hash = Column(String(64), nullable=False)
    response_json = Column(JSON, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


# ---------- 迁移回放与报告 ----------
# 回放任务状态机:
#   QUEUED(排队, 等待并发额度) --claim--> RUNNING --pause(步骤边界)--> PAUSED --resume--> QUEUED
#      |                                     |
#      +--cancel--> CANCELED(终态)           +--全部步骤报告完成--> COMPLETED(终态)
#                                            +--检查点缺失/审计不完整/快照无法还原--> FAILED(终态)
# 已完成步骤的报告在任何终态下都保留; 取消时未开始步骤置 SKIPPED。
CHECKPOINT_STATUSES = ("COMPLETE", "INCOMPLETE")
REPLAY_TASK_STATUSES = ("QUEUED", "RUNNING", "PAUSED", "CANCELED", "COMPLETED", "FAILED")
REPLAY_STEP_STATUSES = ("PENDING", "RUNNING", "SUCCESS", "FAILED", "SKIPPED")
REPLAY_TERMINAL_STATUSES = ("CANCELED", "COMPLETED", "FAILED")
REPLAY_ACTIVE_STATUSES = ("QUEUED", "RUNNING", "PAUSED")


class ReplayCheckpoint(Base):
    """持久化审计检查点: 面向一个已有迁移计划, 在创建时刻固化审计游标、
    计划级审计证据与每个步骤(批次)的审计证据和批次数据快照。

    只有 COMPLETE 的检查点可用于创建回放任务: 计划已 COMPLETED、每步批次都有
    freeze/cutover 审计且计划步骤均 SUCCESS。INCOMPLETE 检查点仍持久化并列出原因,
    但回放创建会被明确拒绝。检查点只追加、不可变(回放只读它, 永不修改它)。"""

    __tablename__ = "replay_checkpoints"

    id = Column(String(32), primary_key=True)          # "C" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    status = Column(String(16), nullable=False, default="INCOMPLETE", index=True)
    audit_cursor_id = Column(Integer, nullable=False)  # 创建时全局最大审计 id(WAL 位置)
    issues = Column(JSON, nullable=True)               # 计划级不完整原因
    total_steps = Column(Integer, nullable=False, default=0)
    complete_steps = Column(Integer, nullable=False, default=0)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow)

    steps = relationship("ReplayCheckpointStep", cascade="all, delete-orphan",
                         order_by="ReplayCheckpointStep.seq")


class ReplayCheckpointStep(Base):
    """检查点内单个计划步骤(批次)的固化内容:
    批次行快照(阶段/epoch/水位/freeze_version/active_schema)、
    关键审计 id(freeze/cutover, 回放时重新核验存在性)、
    范围内旧表全量快照(回放预期状态的唯一来源)与当时新表产出快照。"""

    __tablename__ = "replay_checkpoint_steps"
    __table_args__ = (
        UniqueConstraint("checkpoint_id", "seq", name="uq_checkpoint_step_seq"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    checkpoint_id = Column(String(32), ForeignKey("replay_checkpoints.id"),
                           nullable=False, index=True)
    plan_step_id = Column(Integer, nullable=False)
    seq = Column(Integer, nullable=False)
    batch_id = Column(String(32), nullable=False)
    audit_status = Column(String(16), nullable=False, default="INCOMPLETE")
    audit_issues = Column(JSON, nullable=True)
    required_audit_ids = Column(JSON, nullable=False, default=list)  # freeze/cutover 审计 id
    audit_action_ids = Column(JSON, nullable=False, default=list)    # 该批次全部审计 id(证据)
    # 批次行快照
    batch_phase = Column(String(32), nullable=True)
    batch_epoch = Column(Integer, nullable=True)
    batch_watermark = Column(Integer, nullable=True)
    batch_freeze_version = Column(String(64), nullable=True)
    batch_active_schema = Column(String(8), nullable=True)
    # 批次数据快照(不可变; None 表示快照损坏 -> 回放明确失败)
    old_records = Column(JSON, nullable=True)   # [old_to_dict ...] 预期状态来源
    new_records = Column(JSON, nullable=True)   # [new_to_dict ...] 切换当时的新表产出
    old_count = Column(Integer, nullable=False, default=0)
    new_count = Column(Integer, nullable=False, default=0)


class ReplayTask(Base):
    """迁移回放任务: 基于(计划, 检查点)对原计划做只读重放。

    回放绝不写批次/计划/业务记录/审计日志, 只写 replay_* 表;
    同一(计划,检查点)同时至多有一个非终态任务, 重复创建幂等返回已有任务。
    并发执行数受 REPLAY_MAX_CONCURRENCY 限制, 超出的任务排队等待。"""

    __tablename__ = "replay_tasks"

    id = Column(String(32), primary_key=True)          # "R" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    # 注意: 刻意不加 FK 约束 —— 检查点被删/损坏是必须能观测并明确失败的场景
    checkpoint_id = Column(String(32), nullable=False, index=True)
    plan_name = Column(String(200), nullable=False)
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
    total_steps = Column(Integer, nullable=False, default=0)
    completed_steps = Column(Integer, nullable=False, default=0)
    diff_count = Column(Integer, nullable=False, default=0)   # 累计字段差异数
    state_diff_count = Column(Integer, nullable=False, default=0)
    current_seq = Column(Integer, nullable=True)             # 当前正在回放的步骤
    last_error = Column(String(500), nullable=True)          # 页面"最近错误"
    failure_reason = Column(String(500), nullable=True)      # FAILED 终态原因
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)
    # 报告复核工作流(仅 COMPLETED 回放进入): 复核结论与报告版本绑定,
    # 全部步骤 PASS 才可确认; 任一 FAIL 进入 PENDING(待处理),
    # 重新打开报告版本 +1 开始新一轮复核, 旧版本结论原样保留为历史
    report_version = Column(Integer, nullable=False, default=1)
    review_status = Column(String(16), nullable=False, default="UNREVIEWED", index=True)
    confirmed_by = Column(String(128), nullable=True)
    confirmed_at = Column(DateTime, nullable=True)
    # 复核分派: 当前被分派的复核人(None=未分派, 任何人可提交);
    # 已分派后只有被分派人能提交该任务的步骤结论。改派只改本列,
    # 历史在 replay_assignments 只追加保留。重新打开(版本+1)不清空分派关系。
    assignee = Column(String(128), nullable=True)

    task_steps = relationship("ReplayTaskStep", cascade="all, delete-orphan",
                              order_by="ReplayTaskStep.seq")
    events = relationship("ReplayTaskEvent", cascade="all, delete-orphan",
                          order_by="desc(ReplayTaskEvent.id)")


class ReplayTaskStep(Base):
    """回放步骤报告: 从检查点快照生成预期状态、读取当前业务表得到实际状态、
    记录字段级差异与批次级状态差异。RUNNING 标记先提交(崩溃边界), 报告随步骤成功同事务落库。"""

    __tablename__ = "replay_task_steps"
    __table_args__ = (
        UniqueConstraint("task_id", "seq", name="uq_replay_step_seq"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("replay_tasks.id"),
                     nullable=False, index=True)
    checkpoint_step_id = Column(Integer, nullable=True)  # 无 FK: 快照缺失要能明确失败
    plan_step_id = Column(Integer, nullable=False)
    seq = Column(Integer, nullable=False)
    batch_id = Column(String(32), nullable=False)
    depends_on = Column(JSON, nullable=False, default=list)  # [seq ...] 原计划依赖
    status = Column(String(16), nullable=False, default="PENDING")
    expected_state = Column(JSON, nullable=True)
    actual_state = Column(JSON, nullable=True)
    diffs = Column(JSON, nullable=True)               # 记录字段差异(含 __missing__/__extra__)
    state_diffs = Column(JSON, nullable=True)         # 批次级状态差异(phase/active_schema)
    diff_count = Column(Integer, nullable=False, default=0)
    state_diff_count = Column(Integer, nullable=False, default=0)
    last_error = Column(String(500), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


class ReplayTaskEvent(Base):
    """回放任务事件流水(只追加): 创建/排队/认领执行/暂停/恢复/取消/
    步骤开始/成功/失败/跳过/完成/重启对账/复核提交/确认/重新打开。
    回放不写业务审计表 audit_log。"""

    __tablename__ = "replay_task_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, default=_utcnow)
    task_id = Column(String(32), ForeignKey("replay_tasks.id"),
                     nullable=False, index=True)
    step_seq = Column(Integer, nullable=True)
    event = Column(String(32), nullable=False)
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)


# 回放复核状态机(任务级, 仅 COMPLETED 回放进入复核流程):
#   UNREVIEWED --提交首条复核--> REVIEWING --任一步骤 FAIL--> PENDING(待处理)
#   PENDING --reopen(报告版本+1)--> UNREVIEWED(新一轮复核, 旧版本结论保留为历史)
#   REVIEWING --全部 SUCCESS 步骤 PASS 后 confirm--> CONFIRMED(终态, 复核锁定)
REVIEW_STATUSES = ("UNREVIEWED", "REVIEWING", "PENDING", "CONFIRMED")
REVIEW_VERDICTS = ("PASS", "FAIL")


class ReplayReview(Base):
    """步骤复核结论(只追加): 与回放报告版本绑定, 同一(任务, 版本, 步骤)至多一条
    结论, 并发/重复写入由唯一约束兜底; 重新打开后报告版本 +1, 旧版本结论
    原样保留为历史, 永不修改。"""

    __tablename__ = "replay_reviews"
    __table_args__ = (
        UniqueConstraint("task_id", "report_version", "step_seq",
                         name="uq_replay_review_step"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("replay_tasks.id"),
                     nullable=False, index=True)
    report_version = Column(Integer, nullable=False)   # 绑定的回放报告版本
    step_seq = Column(Integer, nullable=False)
    batch_id = Column(String(32), nullable=False)
    verdict = Column(String(8), nullable=False)        # PASS | FAIL
    issue = Column(String(500), nullable=True)         # 问题说明(FAIL 必填)
    fix_tags = Column(JSON, nullable=False, default=list)  # 修复标签
    operator = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow)


class ReplayAssignment(Base):
    """复核分派历史(只追加): 每次分派/改派落一行, 任务当前分派人在
    replay_tasks.assignee; 历史永不修改, 在任务详情中可见, 重启后保留。"""

    __tablename__ = "replay_assignments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("replay_tasks.id"),
                     nullable=False, index=True)
    assignee = Column(String(128), nullable=False)     # 被分派的复核人
    operator = Column(String(128), nullable=False)     # 执行分派的管理员
    report_version = Column(Integer, nullable=False)   # 分派时的报告版本
    reason = Column(String(500), nullable=True)        # 分派说明(可选)
    created_at = Column(DateTime, default=_utcnow)


# 批量操作类型: assign=批量分派, review=批量提交复核结论
BATCH_OP_ACTIONS = ("assign", "review")


class ReplayBatchOp(Base):
    """批量复核/分派操作结果(持久化): 逐项成功/失败原因落库, 服务重启后仍可查询;
    idempotency_key 唯一, 同一批量请求重放返回首次结果, 不产生二次副作用。"""

    __tablename__ = "replay_batch_ops"

    id = Column(String(32), primary_key=True)          # "BO" + 随机串
    action = Column(String(16), nullable=False)        # assign | review
    operator = Column(String(128), nullable=False)
    idempotency_key = Column(String(128), nullable=False, unique=True)
    assignee = Column(String(128), nullable=True)      # action=assign 时的目标复核人
    report_version = Column(Integer, nullable=True)    # action=review 时基于的报告版本
    total = Column(Integer, nullable=False, default=0)
    succeeded = Column(Integer, nullable=False, default=0)
    failed = Column(Integer, nullable=False, default=0)
    results = Column(JSON, nullable=False, default=list)  # 逐项结果(含失败原因)
    created_at = Column(DateTime, default=_utcnow)


# ---------- 回放证据归档 ----------
# 归档任务状态机(与回放任务同构):
#   QUEUED(排队, 等待并发额度) --claim--> RUNNING --pause(单元边界)--> PAUSED --resume--> QUEUED
#      |                                     |
#      +--cancel--> CANCELED(终态)           +--全部归档单元完成--> COMPLETED(终态, 包不可变)
#                                            +--版本冲突/缺失步骤/数据被删/摘要不一致--> FAILED(终态)
# FAILED/CANCELED 记录永久保留可查询; COMPLETED 归档包可下载、可重新校验摘要。
ARCHIVE_STATUSES = ("QUEUED", "RUNNING", "PAUSED", "CANCELED", "COMPLETED", "FAILED")
ARCHIVE_TERMINAL_STATUSES = ("CANCELED", "COMPLETED", "FAILED")
ARCHIVE_ACTIVE_STATUSES = ("QUEUED", "RUNNING", "PAUSED")


class ReplayArchive(Base):
    """回放证据归档任务: 为已 COMPLETED 的回放按指定报告版本生成不可变归档包。

    包内必须包含: 指定报告版本、逐步步骤报告、该版本复核结论(含全量历史)、
    复核分派历史与审计摘要, 并计算可校验的内容摘要(sha256, 逐文件摘要合成)。
    归档严格只读: 绝不修改回放/复核/业务数据, 只写 replay_archive_* 表与归档包文件;
    归档活动期间(QUEUED/RUNNING/PAUSED)对应回放的复核写入/分派/重新打开被拒绝。
    同一(回放, 报告版本)的重复归档请求幂等返回已有归档(活动中或已完成)。
    并发执行数受 ARCHIVE_MAX_CONCURRENCY 限制, 超出排队; 重启后任务/失败记录/
    已完成归档均可查询, 遗留 RUNNING 任务回到排队位置续跑。"""

    __tablename__ = "replay_archives"

    id = Column(String(32), primary_key=True)          # "A" + 随机串
    # 刻意不加 FK: 回放被删除是必须能观测并明确失败(data_deleted)的场景
    replay_id = Column(String(32), nullable=False, index=True)
    plan_id = Column(String(32), nullable=True)
    plan_name = Column(String(200), nullable=True)
    checkpoint_id = Column(String(32), nullable=True)
    report_version = Column(Integer, nullable=False)   # 归档指定(锁定)的报告版本
    # 归档目录检索: 归档时从回放各步骤关联批次反查并固化的业务分组集合
    biz_groups = Column(JSON, nullable=True)
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
    # 进度按归档单元计: 1(校验) + N(逐步报告) + 复核结论 + 分派历史 + 审计摘要 + 打包
    total_units = Column(Integer, nullable=False, default=0)
    completed_units = Column(Integer, nullable=False, default=0)
    current_stage = Column(String(256), nullable=True)  # 当前归档单元(页面"当前步骤")
    # 归档单元产物的暂存(打包后清空); 崩溃/重启后按已提交单元续跑, 不重复副作用
    staging = Column(JSON, nullable=True)
    manifest = Column(JSON, nullable=True)              # 包清单(逐文件大小/摘要/总摘要)
    content_digest = Column(String(64), nullable=True, index=True)  # 包内容摘要(sha256 hex)
    digest_algorithm = Column(String(16), nullable=False, default="sha256")
    package_path = Column(String(500), nullable=True)   # 归档包(zip)落盘路径(同摘要成员共享同一物理文件)
    package_size = Column(Integer, nullable=True)
    failure_code = Column(String(48), nullable=True)    # 机器可读失败码
    failure_reason = Column(String(500), nullable=True)  # FAILED 终态原因(页面展示)
    # ---------- 保留策略(归档目录生命周期) ----------
    # NONE=未设置保留策略(默认可被清理); UNTIL=保留到 retain_until(到期前不可清理);
    # PERMANENT=永久保留标记(任何清理计划都跳过)。策略持久化, 服务重启不丢失。
    retention_mode = Column(String(16), nullable=False, default="NONE")
    retain_until = Column(DateTime, nullable=True)
    retention_set_by = Column(String(128), nullable=True)
    retention_set_at = Column(DateTime, nullable=True)
    # ---------- 下载/校验并发协调(使用计数) ----------
    # 计数 > 0 表示归档包正在下载或摘要校验, 清理逐项跳过(in_use_download/in_use_verify);
    # 行锁 + 写事务串行化清理与下载/校验, 归档正在使用时物理文件绝不被删除。
    # 进程崩溃遗留的计数由重启对账清零(进程死亡意味着在途请求已不存在)。
    active_downloads = Column(Integer, nullable=False, default=0)
    active_verifies = Column(Integer, nullable=False, default=0)
    # ---------- 清理(软删除): 清理只打标记不删归档行, 操作结果与跳过原因永久可查 ----------
    cleaned_at = Column(DateTime, nullable=True, index=True)
    cleaned_by = Column(String(128), nullable=True)
    cleanup_plan_id = Column(String(32), nullable=True)
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    events = relationship("ReplayArchiveEvent", cascade="all, delete-orphan",
                          order_by="desc(ReplayArchiveEvent.id)")
    digest_membership = relationship(
        "ArchiveDigestMember", uselist=False, cascade="all, delete-orphan",
        back_populates="archive")


class ReplayArchiveEvent(Base):
    """归档任务事件流水(只追加): 创建/排队/认领/暂停/恢复/取消/单元进度/
    完成/失败/重启对账/下载/校验/保留策略/清理。归档不写回放事件表与业务审计表。"""

    __tablename__ = "replay_archive_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, default=_utcnow)
    archive_id = Column(String(32), ForeignKey("replay_archives.id"),
                        nullable=False, index=True)
    stage = Column(String(128), nullable=True)          # 发生时的归档单元
    event = Column(String(32), nullable=False)
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)


# ---------- 归档目录: 同摘要去重与引用关系 ----------
# 每个 COMPLETED 归档按内容摘要(content_digest)在 archive_digest_members 登记一行;
# 同摘要的多个归档互为去重成员, 物理 zip 只保留一份(canonical 指向的成员文件),
# 其余成员共享该路径。引用数 = 同摘要中仍存活(未清理)的成员数; 清理 canonical 前
# 必须把物理文件移交给其他存活成员(引用记录保留), 任何被引用的记录/文件不受影响。
class ArchiveDigestMember(Base):
    __tablename__ = "archive_digest_members"
    __table_args__ = (
        UniqueConstraint("archive_id", name="uq_digest_member_archive"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_digest = Column(String(64), nullable=False, index=True)
    archive_id = Column(String(32), ForeignKey("replay_archives.id"),
                        nullable=False, index=True)
    # 是否持有物理 zip 文件的规范成员; 同摘要至多一行为 True
    is_canonical = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=_utcnow)

    archive = relationship("ReplayArchive", back_populates="digest_membership")


# ---------- 归档清理计划(生命周期) ----------
# QUEUED(排队) -> RUNNING -> COMPLETED(终态); pause 在逐项边界停住(PAUSED),
# resume 重新排队; cancel 把未处理项置 SKIPPED; 清理执行遇未预期错误 -> FAILED
# (保留进度, 可 resume 从未完成项继续)。逐项跳过原因永久保留可查。
CLEANUP_PLAN_STATUSES = ("QUEUED", "RUNNING", "PAUSED", "CANCELED", "COMPLETED", "FAILED")
CLEANUP_ITEM_STATUSES = ("PENDING", "RUNNING", "CLEANED", "SKIPPED", "FAILED", "SKIPPED_CANCELED")
CLEANUP_ACTIVE_STATUSES = ("QUEUED", "RUNNING", "PAUSED")

# 逐项跳过/失败原因(机器可读)
CLEANUP_REASONS = (
    "not_found",                 # 归档不存在
    "not_completed",             # 归档尚在活动状态(QUEUED/RUNNING/PAUSED)
    "already_cleaned",           # 已被清理(可能由其他计划清理)
    "retained_until",            # 仍在带到期时间的保留期内
    "retained_permanent",        # 永久保留标记
    "in_use_download",           # 归档正在下载
    "in_use_verify",             # 归档正在摘要校验
    "package_missing",           # 包文件已不存在(记录仍软清理)
    "file_delete_failed",        # 物理文件删除失败
    "digest_referenced",         # 规范成员移交失败: 同摘要仍有其他引用
    "internal_error",            # 未预期错误
)


class ArchiveCleanupPlan(Base):
    """归档清理计划: 管理员对一批归档发起的生命周期清理。

    逐项独立事务提交: 成功的项 CLEANED(归档软删除, 物理包按引用规则处理),
    被保留策略/下载校验占用等阻止的项 SKIPPED 并记录跳过原因。计划排队执行,
    支持暂停/恢复/取消, 进度、逐项结果与跳过原因全部落库, 重启后保留。"""

    __tablename__ = "archive_cleanup_plans"

    id = Column(String(32), primary_key=True)          # "CP" + 随机串
    operator = Column(String(128), nullable=False)
    idempotency_key = Column(String(128), nullable=True, unique=True)
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
    total_items = Column(Integer, nullable=False, default=0)
    cleaned_items = Column(Integer, nullable=False, default=0)
    skipped_items = Column(Integer, nullable=False, default=0)
    failed_items = Column(Integer, nullable=False, default=0)
    current_archive_id = Column(String(32), nullable=True)  # 当前处理项(页面进度)
    last_error = Column(String(500), nullable=True)
    events = Column(JSON, nullable=False, default=list)     # 计划事件流水(只追加)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    items = relationship("ArchiveCleanupItem", cascade="all, delete-orphan",
                         order_by="ArchiveCleanupItem.id")


class ArchiveCleanupItem(Base):
    """清理计划逐项结果: 每个归档一行; 状态(CLEANED/SKIPPED/...)与机器可读
    跳过/失败原因、说明在同事务落库, 重启后可查询, 是页面逐项跳过原因的来源。"""

    __tablename__ = "archive_cleanup_items"
    __table_args__ = (
        UniqueConstraint("plan_id", "archive_id", name="uq_cleanup_item_archive"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(String(32), ForeignKey("archive_cleanup_plans.id"),
                     nullable=False, index=True)
    # 刻意不加 FK 到 replay_archives: 计划创建时归档 id 可能在执行时已不存在
    archive_id = Column(String(32), nullable=False, index=True)
    position = Column(Integer, nullable=False)         # 计划内顺序
    status = Column(String(20), nullable=False, default="PENDING", index=True)
    reason_code = Column(String(32), nullable=True)    # CLEANUP_REASONS 之一
    reason = Column(String(500), nullable=True)        # 人类可读跳过/失败说明
    processed_by = Column(String(128), nullable=True)
    processed_at = Column(DateTime, nullable=True)


# ---------- 迁移前数据质量门禁 ----------
# 规则类型: required=必填, format=格式(正则), cross_field=跨字段一致性, range=范围约束
QUALITY_RULE_TYPES = ("required", "format", "cross_field", "range")
# 严重级别: BLOCKER=阻断(必须修复或豁免才能通过门禁), WARNING/INFO 只提示不阻断
QUALITY_SEVERITIES = ("BLOCKER", "WARNING", "INFO")
# 质量扫描任务状态机(与回放任务同构):
#   QUEUED(排队, 等待并发额度) --claim--> RUNNING --pause(批次边界)--> PAUSED --resume--> QUEUED
#      |                                     |
#      +--cancel--> CANCELED(终态)           +--全部批次扫描完成--> COMPLETED(终态)
#                                            +--规则缺失/执行异常--> FAILED(终态)
QUALITY_SCAN_STATUSES = ("QUEUED", "RUNNING", "PAUSED", "CANCELED", "COMPLETED", "FAILED")
QUALITY_SCAN_TERMINAL_STATUSES = ("CANCELED", "COMPLETED", "FAILED")
QUALITY_SCAN_ACTIVE_STATUSES = ("QUEUED", "RUNNING", "PAUSED")
QUALITY_SCAN_BATCH_STATUSES = ("PENDING", "RUNNING", "SUCCESS", "FAILED", "SKIPPED")
# 问题处理状态: OPEN=待处理; FIXED=修复批次核验已解决; EXEMPTED=已豁免(含新一轮扫描继承)
QUALITY_ISSUE_STATUSES = ("OPEN", "FIXED", "EXEMPTED")
# 豁免申请状态(管理员带原因提交即批准, 可撤销; 全程只追加保留历史)
QUALITY_EXEMPTION_STATUSES = ("APPROVED", "REVOKED")
# 修复批次逐项核验结论: RESOLVED=违规已消失; STILL_OPEN=仍违规; NOT_FOUND=记录已不存在
QUALITY_FIX_VERDICTS = ("RESOLVED", "STILL_OPEN", "NOT_FOUND", "ALREADY_RESOLVED", "REJECTED")


class QualityRuleSet(Base):
    """计划级数据质量规则集: 一个迁移计划至多一个规则集, 规则整体版本化。

    每次修改规则(内容摘要变化)新增一个 QualityRuleVersion, 旧版本永不修改;
    扫描任务与问题/修复/豁免都绑定生成时的规则版本, 规则版本变化后旧扫描
    结果不能放行, 必须基于新版本重新扫描。"""

    __tablename__ = "quality_rule_sets"

    id = Column(String(32), primary_key=True)          # "QRS" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, unique=True, index=True)
    current_version = Column(Integer, nullable=False, default=1)
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    versions = relationship("QualityRuleVersion", cascade="all, delete-orphan",
                            order_by="QualityRuleVersion.version")


class QualityRuleVersion(Base):
    """规则的一个不可变版本: rules 为规范化后的规则定义列表(JSON),
    content_digest 是规则内容的确定性摘要(相同内容重复保存不产生新版本)。"""

    __tablename__ = "quality_rule_versions"
    __table_args__ = (
        UniqueConstraint("ruleset_id", "version", name="uq_quality_rule_version"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    ruleset_id = Column(String(32), ForeignKey("quality_rule_sets.id"),
                        nullable=False, index=True)
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    version = Column(Integer, nullable=False)
    rules = Column(JSON, nullable=False, default=list)
    rule_count = Column(Integer, nullable=False, default=0)
    content_digest = Column(String(64), nullable=False, index=True)
    note = Column(String(500), nullable=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow)


class QualityScan(Base):
    """质量扫描任务: 对计划涉及的批次逐个扫描旧结构数据, 按批次记录问题明细、
    严重级别与可追踪样本(记录快照)。

    任务排队/执行/暂停/恢复/取消/失败, 支持并发闸门; 结果与规则版本
    (rule_version + content_digest)、批次数据指纹(data_fingerprint)和
    过期时间(expires_at)绑定: 规则版本变化、批次数据变化或结果过期时,
    门禁判定为 STALE, 旧结果不能放行。"""

    __tablename__ = "quality_scans"
    __table_args__ = (
        # 数据库层兜底: 同一计划同时至多一个活动(QUEUED/RUNNING/PAUSED)扫描。
        # 手动扫描与自动重扫共用该不变量, 重复/并发触发在唯一约束上合并,
        # 绝不落第二个活动任务(Postgres/SQLite 均支持部分索引)。
        Index("uq_quality_scan_active_plan", "plan_id", unique=True,
              postgresql_where=Column("status").in_(QUALITY_SCAN_ACTIVE_STATUSES),
              sqlite_where=Column("status").in_(QUALITY_SCAN_ACTIVE_STATUSES)),
    )

    id = Column(String(32), primary_key=True)          # "QS" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    # 刻意不加 FK 到规则集/版本: 防御性地允许规则缺失被观测为明确失败
    ruleset_id = Column(String(32), nullable=False)
    rule_version = Column(Integer, nullable=False)
    rule_digest = Column(String(64), nullable=False)
    rules_snapshot = Column(JSON, nullable=False, default=list)  # 自包含规则快照(修复核验用)
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
    # ---------- 自动重扫编排 ----------
    # scan_source: manual=管理员在计划启动前手动发起; auto_rescan=质量结果失效后
    # 系统为受影响计划自动编排的重扫。自动重扫同样覆盖计划涉及的全部批次,
    # 但创建不受"计划必须 DRAFT"限制(计划可能正在执行/已停住)。
    scan_source = Column(String(16), nullable=False, default="manual")
    # 首次触发来源(RESCAN_TRIGGERS 之一): batch_data_write/rule_version_change/
    # scan_expired/gate_blocked; 重复触发不产生重复任务, 而是合并进同一活动任务。
    trigger_source = Column(String(32), nullable=True, index=True)
    # 触发原因列表(去重合并, 只追加): [{source, reason, batch_id?, at, operator}],
    # 页面/接口展示"为什么会有这个重扫"。
    triggers = Column(JSON, nullable=True, default=list)
    # 本重扫所取代的上一次扫描(结果失效的那次); 手动扫描为 None。
    supersedes_scan_id = Column(String(32), nullable=True, index=True)
    # 受影响范围: 触发时涉及的批次 id 列表(跨批次影响范围可查);
    # 实际扫描仍逐批覆盖计划全部步骤批次。
    affected_batch_ids = Column(JSON, nullable=True, default=list)
    total_batches = Column(Integer, nullable=False, default=0)
    completed_batches = Column(Integer, nullable=False, default=0)
    total_records = Column(Integer, nullable=False, default=0)
    total_issues = Column(Integer, nullable=False, default=0)
    blocker_issues = Column(Integer, nullable=False, default=0)
    warning_issues = Column(Integer, nullable=False, default=0)
    info_issues = Column(Integer, nullable=False, default=0)
    open_blocker_issues = Column(Integer, nullable=False, default=0)
    current_batch_id = Column(String(32), nullable=True)
    ttl_seconds = Column(Integer, nullable=False, default=86400)
    expires_at = Column(DateTime, nullable=True)       # COMPLETED 时按 finished_at + ttl 计算
    last_error = Column(String(500), nullable=True)
    failure_code = Column(String(48), nullable=True)
    failure_reason = Column(String(500), nullable=True)
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow, index=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    batches = relationship("QualityScanBatch", cascade="all, delete-orphan",
                           order_by="QualityScanBatch.seq")
    issues = relationship("QualityIssue", cascade="all, delete-orphan")
    # scan_id 刻意不加 FK(质量事件也存在于无扫描上下文, 如规则版本创建),
    # 用显式 join 条件表达一对多关系
    events = relationship(
        "QualityEvent",
        primaryjoin="QualityScan.id == foreign(QualityEvent.scan_id)",
        cascade="all, delete-orphan",
        order_by="desc(QualityEvent.id)")


class QualityScanBatch(Base):
    """扫描任务内单批次的扫描结果: 记录数、问题计数与批次数据指纹
    (扫描完成时范围内旧表全量内容的摘要, 门禁据此判定批次数据是否变化)。"""

    __tablename__ = "quality_scan_batches"
    __table_args__ = (
        UniqueConstraint("scan_id", "batch_id", name="uq_quality_scan_batch"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    scan_id = Column(String(32), ForeignKey("quality_scans.id"),
                     nullable=False, index=True)
    batch_id = Column(String(32), nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    biz = Column(String(128), nullable=True)
    status = Column(String(16), nullable=False, default="PENDING")
    record_count = Column(Integer, nullable=False, default=0)
    issue_count = Column(Integer, nullable=False, default=0)
    blocker_count = Column(Integer, nullable=False, default=0)
    data_fingerprint = Column(String(64), nullable=True)
    last_error = Column(String(500), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)


class QualityIssue(Base):
    """质量问题明细(按批次 + 记录 + 规则): 严重级别、问题说明与可追踪样本
    (命中时的完整记录快照)。问题处理状态(FIXED/EXEMPTED)与处理历史绑定,
    修复/豁免都记录产生时的规则版本。(scan, batch, rule, record, field) 唯一。"""

    __tablename__ = "quality_issues"
    __table_args__ = (
        UniqueConstraint("scan_id", "batch_id", "rule_id", "record_id", "field",
                         name="uq_quality_issue"),
    )

    id = Column(String(32), primary_key=True)          # "QI" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    scan_id = Column(String(32), ForeignKey("quality_scans.id"),
                     nullable=False, index=True)
    batch_id = Column(String(32), nullable=False, index=True)
    record_id = Column(Integer, nullable=False, index=True)
    rule_version = Column(Integer, nullable=False)
    rule_id = Column(String(64), nullable=False)
    rule_name = Column(String(200), nullable=False)
    rule_type = Column(String(16), nullable=False)
    severity = Column(String(16), nullable=False, index=True)
    field = Column(String(128), nullable=True)
    message = Column(String(500), nullable=False)
    sample = Column(JSON, nullable=True)               # 命中记录快照(可追踪样本)
    status = Column(String(16), nullable=False, default="OPEN", index=True)
    resolution_type = Column(String(16), nullable=True)   # fix | exemption
    resolution_id = Column(String(32), nullable=True)     # 修复批次/豁免 id
    resolved_by = Column(String(128), nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow)


class QualityFixBatch(Base):
    """修复批次: 管理员针对扫描问题发起的修复核验批次, 逐项重新执行规则核验
    当前数据(违规消失->RESOLVED 并置问题 FIXED; 仍违规->STILL_OPEN; 记录已删除
    ->NOT_FOUND)。修复批次只追加, 与扫描时规则版本绑定, 是修复历史的来源。"""

    __tablename__ = "quality_fix_batches"

    id = Column(String(32), primary_key=True)          # "QF" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    scan_id = Column(String(32), nullable=False, index=True)
    rule_version = Column(Integer, nullable=False)
    operator = Column(String(128), nullable=False)
    note = Column(String(500), nullable=True)
    total = Column(Integer, nullable=False, default=0)
    resolved = Column(Integer, nullable=False, default=0)
    still_open = Column(Integer, nullable=False, default=0)
    not_found = Column(Integer, nullable=False, default=0)
    rejected = Column(Integer, nullable=False, default=0)
    results = Column(JSON, nullable=False, default=list)   # 逐项核验结果
    created_at = Column(DateTime, default=_utcnow)


class QualityExemption(Base):
    """豁免申请(带原因, 提交即批准; 可撤销): 针对单个阻断问题, 绑定规则版本。
    只追加: 撤销不改写原行而是置 REVOKED; 新一轮同版本扫描可继承有效豁免。"""

    __tablename__ = "quality_exemptions"

    id = Column(String(32), primary_key=True)          # "QE" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    issue_id = Column(String(32), nullable=False, index=True)
    scan_id = Column(String(32), nullable=False, index=True)
    batch_id = Column(String(32), nullable=False)
    rule_id = Column(String(64), nullable=False)
    record_id = Column(Integer, nullable=False)
    field = Column(String(128), nullable=True)         # 命中字段(新扫描继承豁免时匹配)
    rule_version = Column(Integer, nullable=False)
    reason = Column(String(500), nullable=False)
    status = Column(String(16), nullable=False, default="APPROVED", index=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow)
    revoked_by = Column(String(128), nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    revoke_reason = Column(String(500), nullable=True)


class QualityEvent(Base):
    """质量门禁事件流水(只追加): 规则版本创建、扫描创建/排队/认领/暂停/恢复/
    取消/完成/失败/重启对账、修复批次、豁免提交/撤销、自动重扫编排、
    计划门禁暂停/恢复。质量模块不写业务审计表(暂停/恢复同步落 plan 审计)。"""

    __tablename__ = "quality_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, default=_utcnow)
    plan_id = Column(String(32), nullable=False, index=True)
    scan_id = Column(String(32), nullable=True, index=True)
    event = Column(String(48), nullable=False)
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)


# ---------- 质量结果失效后的自动重扫编排 ----------
# 自动重扫触发来源:
#   batch_data_write   批次范围内旧结构数据写入(指纹将漂移/已漂移)
#   rule_version_change 规则新版本发布, 旧扫描绑定的规则版本不再是当前版本
#   scan_expired       最近一次完成扫描超过 TTL(expires_at)
#   gate_blocked       执行器在步骤边界发现门禁失效(兜底来源)
#   scan_failed_retry  上一次(自动)扫描 FAILED 后的恢复重试
RESCAN_TRIGGERS = (
    "batch_data_write", "rule_version_change", "scan_expired",
    "gate_blocked", "scan_failed_retry",
)
# 门禁暂停(QualityGateHold)状态机:
#   ACTIVE  计划执行器因门禁失效暂停推进, 等待自动重扫与阻断问题处理;
#   RESUMED 重扫完成且门禁重新 PASS, 计划从原步骤自动恢复;
#   CANCELED 计划在暂停期间被管理员取消, 不再自动恢复(终态)。
GATE_HOLD_STATUSES = ("ACTIVE", "RESUMED", "CANCELED")
# 恢复方式: auto=worker 检测到门禁 PASS 自动恢复; manual_cancel=计划取消。
GATE_HOLD_RESUME_AUTO = "auto_gate_pass"


class QualityGateHold(Base):
    """计划因质量门禁失效而暂停的一次留痕(只追加, 恢复不改写而是终态化)。

    每次执行器在步骤边界发现门禁失效都会开一段 hold(同一计划同时至多一段
    ACTIVE); hold 关联系统编排的唯一自动重扫任务(rescan_scan_id), 记录首次
    触发来源、暂停原因(计划步骤停在原步骤不复位)与全部触发原因的合并历史;
    重扫完成、阻断问题处理完且门禁重新通过后自动 RESUMED(记录恢复时扫描),
    计划从暂停时的原步骤继续推进; 计划取消则 CANCELED, 不再自动恢复。"""

    __tablename__ = "quality_gate_holds"

    id = Column(String(32), primary_key=True)          # "QH" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    status = Column(String(16), nullable=False, default="ACTIVE", index=True)
    trigger_source = Column(String(32), nullable=False)   # RESCAN_TRIGGERS 之一
    reason = Column(String(500), nullable=False)          # 暂停原因(人类可读)
    reason_code = Column(String(32), nullable=True)       # 机器可读门禁状态(STALE/BLOCKED/...)
    # 暂停时计划停留的步骤(从原步骤继续, 不重置尝试计数/轮次)
    paused_at_step_id = Column(Integer, nullable=True)
    paused_at_seq = Column(Integer, nullable=True)
    # 关联的自动重扫(唯一): 创建 hold 时同步编排, 重复触发合并到该任务
    rescan_scan_id = Column(String(32), nullable=True, index=True)
    # 触发原因合并历史(只追加): 与扫描 triggers 同源记录
    triggers = Column(JSON, nullable=False, default=list)
    # 恢复留痕
    resume_mode = Column(String(32), nullable=True)       # auto_gate_pass
    resume_scan_id = Column(String(32), nullable=True)    # 恢复时门禁依据的扫描
    resumed_by = Column(String(128), nullable=True)
    resumed_at = Column(DateTime, nullable=True)
    canceled_by = Column(String(128), nullable=True)
    canceled_at = Column(DateTime, nullable=True)
    created_by = Column(String(128), nullable=False, default="system")
    created_at = Column(DateTime, default=_utcnow, index=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


# ======================================================================
# ---------- 审计事件回放与补偿(audit replay & compensation) ----------
# ======================================================================
# 统一的不可变审计事件流: 把批次写入(BATCH_WRITE)、批次生命周期
# (BATCH_CREATE/FREEZE/VALIDATE/CUTOVER/RECOVER)、规则变更(RULE_CHANGED)、
# 扫描状态变化(SCAN_STATUS)、质量暂停(QUALITY_HOLD)、计划推进(PLAN_ADVANCE)
# 与计划取消(PLAN_CANCEL)等全部投影到同一只追加事件表。
#
# - global_seq: 全库严格递增顺序号(插入时锁分配), 是跨流总顺序;
# - stream_key: 每条流的归属。计划相关事件流以 plan_id 为 key(同一逻辑事件
#   对相关计划各投影一份, 共享 correlation_id); 批次写等无计划上下文事件以
#   "batch:" + batch_id 为 key; 纯外部备注以 "global" 为 key;
# - stream_seq: 单流(同 stream_key)内从 1 连续递增, 是"事件连续性"校验依据;
# - stream_hash: 单流哈希链 prev_hash = sha256(stream_seq|event_type|
#   correlation_id|payload_json|ts|prev_stream_hash), 任何篡改/缺口都会断链;
# - dedupe_key: 投影去重键, 同一逻辑事件的重投影(重复请求/幂等重放)返回已有行。
AUDIT_EVENT_TYPES = (
    "EXTERNAL_NOTE",    # 运维显式补录的备注(唯一允许直接写入的类型)
    "BATCH_WRITE",      # 批次范围内记录写入(旧路径/新路径)
    "BATCH_CREATE",     # 批次创建
    "BATCH_FREEZE",     # 批次冻结
    "BATCH_VALIDATE",   # 批次校验(通过或阻止)
    "BATCH_CUTOVER",    # 批次切换
    "BATCH_RECOVER",    # 批次恢复
    "BATCH_ATTACH",     # 计划创建时把步骤批次锚定进该计划事件流
    "RULE_CHANGED",     # 质量规则新版本发布
    "SCAN_STATUS",      # 扫描状态变化(创建/排队/认领/暂停/恢复/完成/失败/取消)
    "QUALITY_HOLD",     # 质量门禁暂停打开/恢复/取消
    "PLAN_ADVANCE",     # 计划推进(创建/启动/暂停/恢复/步骤成功/完成/审批/窗口等)
    "PLAN_CANCEL",      # 计划取消
    "COMP_EXECUTED",    # 补偿动作执行成功(只追加, 不改原事件)
    "COMP_FAILED",      # 补偿动作执行失败(只追加, 可重试)
    "COMP_UNDONE",      # 补偿动作撤销(只追加, 关联原执行事件)
    "COMP_APPROVAL",    # 补偿审批: 通过/拒绝/失效/任务取消(只追加)
    "COMP_WINDOW",      # 补偿执行窗口: 预约/替换/清空/窗口外暂停/重新进入继续(只追加)
)
# 允许运维显式补录 / 回放校验接受的乱序时钟偏移(秒):
# event_ts 早于流上一事件超过该阈值即视为乱序, 显式补录直接拒绝,
# 历史流中的乱序会让该计划的快照校验失败(out_of_order)。
AUDIT_MAX_OUT_OF_ORDER_SECONDS = 300


class AuditEvent(Base):
    """统一不可变审计事件流(只追加): 顺序号 + 哈希链 + 投影去重。

    任何代码路径都不允许 UPDATE/DELETE 本表; 补偿只追加 COMP_* 事件,
    原事件永远不可变。分页接口按计划(stream_key=plan_id)/批次
    (batch_id 过滤)/时间范围(event_ts)查询。"""

    __tablename__ = "audit_events"
    __table_args__ = (
        # 单流顺序号唯一: 连续性校验与分页都依赖它
        UniqueConstraint("stream_key", "stream_seq", name="uq_audit_stream_seq"),
        # 投影去重: 同逻辑事件重投影(含同一 correlation 的计划扇出例外,
        # correlation 在不同 stream_key 下可重复, 故去重键含 stream_key)
        UniqueConstraint("stream_key", "dedupe_key", name="uq_audit_dedupe"),
        Index("ix_audit_events_batch_ts", "batch_id", "event_ts"),
        Index("ix_audit_events_plan_ts", "plan_id", "event_ts"),
        Index("ix_audit_events_ts", "event_ts"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    global_seq = Column(Integer, nullable=False, unique=True, index=True)
    stream_key = Column(String(64), nullable=False, index=True)
    stream_seq = Column(Integer, nullable=False)
    event_type = Column(String(32), nullable=False, index=True)
    # 逻辑关联
    plan_id = Column(String(32), nullable=True, index=True)
    batch_id = Column(String(32), nullable=True, index=True)
    scan_id = Column(String(32), nullable=True, index=True)
    step_seq = Column(Integer, nullable=True)
    # 同一逻辑事件(批次动作扇出到多个计划流/补偿关联)的关联标识
    correlation_id = Column(String(64), nullable=False, index=True)
    # 单流哈希链
    prev_global_seq = Column(Integer, nullable=True)
    prev_stream_hash = Column(String(64), nullable=True)
    stream_hash = Column(String(64), nullable=False)
    # 业务载荷: 批次动作带 phase/epoch/freeze_version/watermark(批次版本),
    # 规则事件带 rule_version/content_digest(规则版本), 扫描事件带状态与规则版本
    payload = Column(JSON, nullable=False, default=dict)
    operator = Column(String(128), nullable=False)
    source = Column(String(32), nullable=False, default="internal")  # internal | api | system
    dedupe_key = Column(String(128), nullable=False)
    event_ts = Column(DateTime, nullable=False, default=_utcnow)
    created_at = Column(DateTime, default=_utcnow)


# 回放快照状态机:
#   VALID(校验通过, 可预览/执行补偿, 有 TTL) --执行补偿--> 不改变快照
#   REJECTED(校验失败, 永久拒绝, reasons 逐条说明; 记录保留可查)
#   VALID 快照超过 expires_at -> 只读, 补偿执行被拒绝(snapshot_expired),
#          可重新生成; 撤销(undo)不受过期限制(回滚不允许被 TTL 卡死)。
AUDIT_SNAPSHOT_STATUSES = ("VALID", "REJECTED")
# 快照校验失败原因(机器可读):
#   plan_not_terminal   计划尚未 COMPLETED/CANCELED, 不允许生成回放快照
#   target_before_end   目标时间早于计划终结事件(未到可回放点)
#   stream_gap          计划事件流顺序号不连续(存在缺口)
#   chain_broken        哈希链无法重算(事件被篡改/丢失)
#   out_of_order        事件时间戳倒流超过容忍阈值(乱序事件)
#   rule_version_gap    规则版本不兼容: 引用了不存在的规则版本/版本跳跃/摘要不一致
#   batch_version_gap   批次版本不兼容: epoch 倒挂/缺失版本增量/freeze_version 不一致
#   batch_state_drift   目标时点批次预期状态与当前批次状态不一致(回放无法还原)
SNAPSHOT_REJECT_REASONS = (
    "plan_not_terminal", "target_before_end", "stream_gap", "chain_broken",
    "out_of_order", "rule_version_gap", "batch_version_gap", "batch_state_drift",
)
AUDIT_SNAPSHOT_DEFAULT_TTL_SECONDS = 24 * 3600


class AuditSnapshot(Base):
    """针对已终结(COMPLETED/CANCELED)计划在某个时间点生成的回放快照。

    创建时把计划事件流截至 target_at 的事件逐行固化(AuditSnapshotEvent),
    并校验: 事件连续性(stream_seq 无缺口)、哈希链完整、时间戳不乱序、
    规则版本(存在/摘要一致/无跳跃)与批次版本(epoch 不倒挂、版本增量连续、
    freeze_version 与批次审计一致), 以及目标时点批次预期状态与当前状态一致。
    任一不满足 -> REJECTED(reasons 逐条说明), 不允许补偿。
    快照不可变; VALID 快照有 TTL, 过期后补偿执行拒绝(撤销除外)。"""

    __tablename__ = "audit_snapshots"

    id = Column(String(32), primary_key=True)          # "AS" + 随机串
    # 刻意不加 FK: 计划被删是必须能观测并拒绝(data_deleted)的场景
    plan_id = Column(String(32), nullable=False, index=True)
    plan_name = Column(String(200), nullable=True)
    plan_status_at_create = Column(String(16), nullable=False)  # COMPLETED | CANCELED
    target_at = Column(DateTime, nullable=False)       # 回放时间点(闭区间, UTC naive)
    # 固化时刻的流边界
    last_stream_seq = Column(Integer, nullable=False, default=0)
    last_global_seq = Column(Integer, nullable=False, default=0)
    last_stream_hash = Column(String(64), nullable=True)
    status = Column(String(16), nullable=False, default="REJECTED", index=True)
    reasons = Column(JSON, nullable=False, default=list)   # [{code, message, ...}]
    rule_versions = Column(JSON, nullable=False, default=list)   # 固化的规则版本链
    batch_versions = Column(JSON, nullable=False, default=list)  # 固化的批次版本链
    event_count = Column(Integer, nullable=False, default=0)
    ttl_seconds = Column(Integer, nullable=False, default=AUDIT_SNAPSHOT_DEFAULT_TTL_SECONDS)
    expires_at = Column(DateTime, nullable=True, index=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow, index=True)

    events = relationship("AuditSnapshotEvent", cascade="all, delete-orphan",
                          order_by="AuditSnapshotEvent.stream_seq")
    batches = relationship("AuditSnapshotBatch", cascade="all, delete-orphan",
                           order_by="AuditSnapshotBatch.seq")


class AuditSnapshotEvent(Base):
    """快照内逐行固化的事件(不可变副本): 回放/校验以本表为准,
    不依赖事后可能被外部工具触碰的事件表。"""

    __tablename__ = "audit_snapshot_events"
    __table_args__ = (
        UniqueConstraint("snapshot_id", "stream_seq", name="uq_snapshot_event_seq"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(32), ForeignKey("audit_snapshots.id"),
                         nullable=False, index=True)
    audit_event_id = Column(Integer, nullable=False)
    global_seq = Column(Integer, nullable=False)
    stream_seq = Column(Integer, nullable=False)
    event_type = Column(String(32), nullable=False)
    batch_id = Column(String(32), nullable=True)
    scan_id = Column(String(32), nullable=True)
    correlation_id = Column(String(64), nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    operator = Column(String(128), nullable=False)
    event_ts = Column(DateTime, nullable=False)
    stream_hash = Column(String(64), nullable=False)


class AuditSnapshotBatch(Base):
    """快照内每个计划步骤批次在目标时点的预期状态与旧表基线:
    补偿动作(记录回填/清理/解冻)推导的唯一来源。"""

    __tablename__ = "audit_snapshot_batches"
    __table_args__ = (
        UniqueConstraint("snapshot_id", "batch_id", name="uq_snapshot_batch"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(String(32), ForeignKey("audit_snapshots.id"),
                         nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    plan_step_id = Column(Integer, nullable=False)
    batch_id = Column(String(32), nullable=False)
    biz = Column(String(128), nullable=True)
    id_start = Column(Integer, nullable=False)
    id_end = Column(Integer, nullable=False)
    expected_phase = Column(String(32), nullable=False)
    expected_epoch = Column(Integer, nullable=False)
    expected_freeze_version = Column(String(64), nullable=True)
    expected_schema = Column(String(8), nullable=False, default="old")
    # 目标时点范围内旧表全量基线(记录修复的预期来源)
    expected_old_records = Column(JSON, nullable=False, default=list)
    expected_old_count = Column(Integer, nullable=False, default=0)


# 补偿任务状态机:
#   QUEUED(排队等并发额度) --claim--> RUNNING --全部动作成功--> COMPLETED(终态)
#                                    +--部分动作失败--> PARTIAL(可逐动作/整任务重试)
#   RUNNING/PARTIAL --undo--> UNDO_RUNNING --全部撤销成功--> UNDONE(终态)
#                                        +--部分撤销失败--> UNDO_PARTIAL(可继续撤销)
#   QUEUED/RUNNING/PARTIAL --cancel--> CANCELED(终态, 已完成动作保留, 不再执行)
#   重启对账: 遗留 RUNNING/UNDO_RUNNING 回到 QUEUED/UNDO_RUNNING 安全位置续跑,
#             动作级进度(PENDING/SUCCESS/FAILED/UNDONE)与失败原因持久化保留。
COMP_TASK_STATUSES = (
    "QUEUED", "RUNNING", "PARTIAL", "COMPLETED",
    "UNDO_RUNNING", "UNDO_PARTIAL", "UNDONE", "CANCELED",
)
COMP_TASK_TERMINAL_STATUSES = ("COMPLETED", "UNDONE", "CANCELED")
COMP_TASK_ACTIVE_STATUSES = ("QUEUED", "RUNNING", "PARTIAL",
                            "UNDO_RUNNING", "UNDO_PARTIAL")
# 可被取消的执行态(撤销态不允许取消; CANCELED 只作用于执行流程)
COMP_TASK_CANCELABLE_STATUSES = ("QUEUED", "RUNNING", "PARTIAL")
COMP_TASK_UNDO_STATUSES = ("UNDO_RUNNING", "UNDO_PARTIAL", "UNDONE")
# 补偿动作状态:
#   PENDING 待执行; SUCCESS 执行成功; FAILED 执行失败(last_error 保留, 可重试);
#   SKIPPED 推导时即不适用/被门禁策略跳过; UNDOING 撤销中; UNDONE 已撤销。
COMP_ACTION_STATUSES = ("PENDING", "SUCCESS", "FAILED", "SKIPPED", "UNDOING", "UNDONE")
# 补偿动作类型:
#   record_backfill 目标时点旧表有而当前新表缺失/不一致 -> 按转换规则回填/修正新表
#   record_cleanup  当前新表有而目标时点基线没有(__extra__) -> 删除多余新表记录
#   batch_unfreeze  批次未走到预期终态(取消计划中仍 FROZEN/...) -> 恢复到可写 NORMAL
COMP_ACTION_TYPES = ("record_backfill", "record_cleanup", "batch_unfreeze")

# ---------- 补偿风险分级 / 双人审批 / 失效留痕 ----------
# 风险等级(创建补偿任务时按动作构成自动推导):
#   LOW   仅 record_backfill(按目标时点基线回填/修正), 免审批可直接执行;
#   HIGH  含 record_cleanup(删除多余数据)/batch_unfreeze(解冻批次)等破坏性动作,
#         必须收集两名不同操作者的独立 APPROVED 才能执行。
COMP_RISK_LEVELS = ("LOW", "HIGH")
# 审批状态(任务级, 仅高风险使用; 低风险恒 NOT_REQUIRED):
#   NOT_REQUIRED 低风险免审批
#   PENDING      高风险, 已收集审批不足两人, 不可执行
#   APPROVED     已收集当前审批轮次两名不同操作者的独立通过, 可以执行
#   REJECTED     任一审批人拒绝(带原因), 闸门关闭, 需重新收集两轮通过
#   INVALIDATED  审批收集后审批依据(快照/质量门禁/计划状态)发生变化,
#                已收集审批全部失效, 需重新收集
#   CANCELED     任务被取消(终态), 审批不再有效
COMP_APPROVAL_STATUSES = (
    "NOT_REQUIRED", "PENDING", "APPROVED", "REJECTED", "INVALIDATED", "CANCELED")
# 单条审批记录状态(只追加): APPROVED=通过; REJECTED=拒绝(带原因);
# INVALIDATED=审批依据变化被系统失效; SUPERSEDED=拒绝新审批时被新轮次取代;
# TASK_CANCELED=任务被取消, 未决审批随之终止。
COMP_APPROVAL_DECISION_STATES = (
    "APPROVED", "REJECTED", "INVALIDATED", "SUPERSEDED", "TASK_CANCELED")
# 高风险补偿执行所需的独立审批人数(两名不同操作者)
COMP_REQUIRED_APPROVALS = 2
# 审批失效原因(机器可读):
#   snapshot_expired    快照超过有效期
#   snapshot_changed    快照状态/目标点/流边界变化
#   gate_status_changed 质量门禁状态变化(规则版本/数据指纹/扫描/阻断问题)
#   plan_status_changed 计划状态变化(如被取消)
#   batch_version_gap   批次版本变化(epoch/freeze_version/阶段漂移)
#   task_canceled       任务被取消, 未决审批终止
#   rejection_reset     一名审批人拒绝, 此前已收集的通过失效
#   task_rerun          (保留) 新一轮审批收集
COMP_APPROVAL_INVALIDATE_REASONS = (
    "snapshot_expired", "snapshot_changed", "gate_status_changed",
    "plan_status_changed", "batch_version_gap", "task_canceled",
    "rejection_reset")


class CompensationTask(Base):
    """对一个 VALID 快照发起的补偿执行(同一快照同时至多一个非终态任务,
    唯一部分索引兜底, 并发执行不能重复写入)。

    补偿不变量:
    1. 绝不 UPDATE/DELETE audit_events 原事件, 只追加 COMP_EXECUTED/COMP_FAILED/
       COMP_UNDONE 事件;
    2. 每个动作执行前实时复核质量门禁: 门禁不通过(STALE/BLOCKED/...)则该动作
       FAILED(gate_blocked), 补偿不能越过质量门禁;
    3. 计划已 CANCELED 的快照只允许预览, 任何执行都拒绝(plan_canceled);
    4. 动作逐个独立事务提交, 部分失败不回滚已成功动作, 可逐动作重试;
    5. 动作带确定性幂等键(快照+动作), 崩溃/并发重放走首次结果, 不重复写入;
    6. 撤销按动作逆序执行, 用执行时捕获的 before_image 恢复现场, 同样只追加事件。
    """

    __tablename__ = "compensation_tasks"
    __table_args__ = (
        # 同一快照同时至多一个非终态补偿任务(数据库层兜底并发重复执行)
        Index("uq_comp_active_snapshot", "snapshot_id", unique=True,
              postgresql_where=Column("status").in_(
                  ("QUEUED", "RUNNING", "PARTIAL",
                   "UNDO_RUNNING", "UNDO_PARTIAL")),
              sqlite_where=Column("status").in_(
                  ("QUEUED", "RUNNING", "PARTIAL",
                   "UNDO_RUNNING", "UNDO_PARTIAL"))),
    )

    id = Column(String(32), primary_key=True)          # "CT" + 随机串
    # 刻意不加 FK: 快照缺失/被删必须能观测并明确失败
    snapshot_id = Column(String(32), nullable=False, index=True)
    plan_id = Column(String(32), nullable=False, index=True)
    plan_status = Column(String(16), nullable=False)   # 创建任务时计划状态
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
    total_actions = Column(Integer, nullable=False, default=0)
    success_actions = Column(Integer, nullable=False, default=0)
    failed_actions = Column(Integer, nullable=False, default=0)
    undone_actions = Column(Integer, nullable=False, default=0)
    current_action_seq = Column(Integer, nullable=True)
    last_error = Column(String(500), nullable=True)
    failure_reason = Column(String(500), nullable=True)
    # ---------- 风险分级与双人审批 ----------
    risk_level = Column(String(8), nullable=False, default="LOW")
    approval_status = Column(String(16), nullable=False, default="NOT_REQUIRED")
    # 当前审批轮次: 每次失效/拒绝重新收集 +1; 审批记录只追加并带轮次
    approval_round = Column(Integer, nullable=False, default=0)
    reject_reason = Column(String(500), nullable=True)   # 最近一次拒绝原因
    # 当前轮次审批依据指纹(JSON): 首轮收集时固化; 之后复核变化即使审批失效
    approval_basis = Column(JSON, nullable=True)
    canceled_by = Column(String(128), nullable=True)
    canceled_at = Column(DateTime, nullable=True)
    cancel_reason = Column(String(500), nullable=True)
    # ---------- 限定执行窗口运行态 ----------
    # None=未预约窗口(不限制); True/False=最近一次判定在窗口内/窗口外(窗口暂停)
    window_open = Column(Boolean, nullable=True)
    window_pause_reason = Column(String(500), nullable=True)
    created_by = Column(String(128), nullable=False)
    updated_by = Column(String(128), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow, index=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    actions = relationship("CompensationAction", cascade="all, delete-orphan",
                           order_by="CompensationAction.seq")
    events = relationship("CompensationTaskEvent", cascade="all, delete-orphan",
                          order_by="desc(CompensationTaskEvent.id)")
    windows = relationship("CompensationWindow", cascade="all, delete-orphan",
                           order_by="CompensationWindow.starts_at")
    approvals = relationship("CompensationApproval", cascade="all, delete-orphan",
                             order_by="CompensationApproval.id")


class CompensationAction(Base):
    """快照中推导出的单个待补偿动作: 类型、目标、预期值、执行结果、
    失败原因、撤销前镜像与审计事件关联(执行/撤销对应的 audit_events 行 id)。"""

    __tablename__ = "compensation_actions"
    __table_args__ = (
        UniqueConstraint("task_id", "seq", name="uq_comp_action_seq"),
        # 确定性动作键(任务内唯一): 同快照同目标只推导一次, 是幂等执行/崩溃重放
        # 的依据; 同一快照旧任务 UNDONE 后可再建新任务, 故不做跨任务全局唯一
        UniqueConstraint("task_id", "action_key", name="uq_comp_task_action_key"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("compensation_tasks.id"),
                     nullable=False, index=True)
    snapshot_id = Column(String(32), nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    action_key = Column(String(160), nullable=False)
    action_type = Column(String(24), nullable=False)
    batch_id = Column(String(32), nullable=False, index=True)
    record_id = Column(Integer, nullable=True, index=True)
    # 执行依据(预期新表记录 / 预期存在性等, JSON)
    expected = Column(JSON, nullable=True)
    # 执行时捕获的撤销前镜像(被删新表行/被恢复批次的原阶段...)
    before_image = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)               # 执行结果摘要
    status = Column(String(16), nullable=False, default="PENDING", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(String(500), nullable=True)
    gate_status = Column(String(32), nullable=True)    # 最近一次门禁判定状态
    # 审计事件关联(撤销关联): 执行成功/撤销成功对应的 audit_events.global_seq
    executed_event_global_seq = Column(Integer, nullable=True)
    undone_event_global_seq = Column(Integer, nullable=True)
    executed_by = Column(String(128), nullable=True)
    executed_at = Column(DateTime, nullable=True)
    undone_by = Column(String(128), nullable=True)
    undone_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


class CompensationTaskEvent(Base):
    """补偿任务事件流水(只追加): 创建/排队/认领/执行开始/动作成功/动作失败/
    门禁拒绝/部分完成/完成/重试/撤销开始/动作撤销/撤销部分完成/已撤销/重启对账。
    业务审计效果同时投影到统一 audit_events(COMP_*), 本表是任务视角流水。"""

    __tablename__ = "compensation_task_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, default=_utcnow)
    task_id = Column(String(32), ForeignKey("compensation_tasks.id"),
                     nullable=False, index=True)
    action_seq = Column(Integer, nullable=True)
    event = Column(String(40), nullable=False)
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)


class CompensationApproval(Base):
    """补偿任务双人审批记录(只追加): 高风险补偿执行前必须收集同一审批轮次内
    两名不同操作者的独立通过。审批人不能是任务创建者, 也不能是执行操作者。

    - 同一(任务, 审批轮次, 审批人)至多一条有效结论(唯一约束兜底并发去重);
    - 审批依据(快照/质量门禁/计划/批次版本)变化时, 当前轮次全部 APPROVED 记录
      被置 INVALIDATED 并记录原因, 任务审批状态回到 INVALIDATED 需重新收集;
    - 任一审批人拒绝(REJECTED 记录带原因), 该轮次其余通过置 SUPERSEDED,
      任务审批状态为 REJECTED, 重新收集开启新一轮;
    - 任务取消时未决记录置 TASK_CANCELED, 全部历史保留可查。"""

    __tablename__ = "compensation_approvals"
    __table_args__ = (
        # 同一轮次同一审批人只有一条结论: 并发双人审批去重的数据库兜底
        UniqueConstraint("task_id", "approval_round", "operator",
                         name="uq_comp_approval_round_operator"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("compensation_tasks.id"),
                     nullable=False, index=True)
    approval_round = Column(Integer, nullable=False, default=1)
    decision = Column(String(16), nullable=False)         # APPROVED/REJECTED/INVALIDATED/...
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)           # 拒绝/失效原因
    # 该条结论提交时任务审批依据指纹(事后可核对是"依据什么批准的")
    basis = Column(JSON, nullable=True)
    # 失效留痕(只追加行的状态化字段): 失效原因/操作者(系统)/时刻
    invalidated_reason = Column(String(32), nullable=True)
    invalidated_by = Column(String(128), nullable=True)
    invalidated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=_utcnow)


class CompensationWindow(Base):
    """补偿任务的限定执行窗口(闭区间 [starts_at, ends_at], UTC 存储):
    任务只在任一窗口内执行动作; 窗口外在动作边界自动暂停(保留已完成动作),
    重新进入窗口后 worker 自动继续; 窗口外显式执行/重试明确拒绝。

    预约/替换窗口时与其他活动补偿任务做批次重叠检测: 时间重叠且动作批次集合
    相交即冲突, 明确返回占用者与冲突时间。无窗口行的任务不受时间限制。"""

    __tablename__ = "compensation_windows"
    __table_args__ = (
        UniqueConstraint("task_id", "starts_at", "ends_at",
                         name="uq_comp_window"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("compensation_tasks.id"),
                     nullable=False, index=True)
    starts_at = Column(DateTime, nullable=False)
    ends_at = Column(DateTime, nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, default=_utcnow)
