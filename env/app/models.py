from sqlalchemy import (
    JSON, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, func,
)
from sqlalchemy.orm import relationship

from .db import Base

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
    created_by = Column(String(128), nullable=False)
    started_by = Column(String(128), nullable=True)
    updated_by = Column(String(128), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


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
