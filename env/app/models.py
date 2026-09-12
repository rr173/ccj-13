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

    id = Column(String(32), primary_key=True)          # "QS" + 随机串
    plan_id = Column(String(32), ForeignKey("migration_plans.id"),
                     nullable=False, index=True)
    # 刻意不加 FK 到规则集/版本: 防御性地允许规则缺失被观测为明确失败
    ruleset_id = Column(String(32), nullable=False)
    rule_version = Column(Integer, nullable=False)
    rule_digest = Column(String(64), nullable=False)
    rules_snapshot = Column(JSON, nullable=False, default=list)  # 自包含规则快照(修复核验用)
    status = Column(String(16), nullable=False, default="QUEUED", index=True)
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
    取消/完成/失败/重启对账、修复批次、豁免提交/撤销。质量模块不写业务审计表。"""

    __tablename__ = "quality_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, default=_utcnow)
    plan_id = Column(String(32), nullable=False, index=True)
    scan_id = Column(String(32), nullable=True, index=True)
    event = Column(String(48), nullable=False)
    operator = Column(String(128), nullable=False)
    reason = Column(String(500), nullable=True)
    detail = Column(JSON, nullable=True)
