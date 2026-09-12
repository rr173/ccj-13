from sqlalchemy import (
    JSON, Boolean, Column, DateTime, ForeignKey, Integer, String,
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
