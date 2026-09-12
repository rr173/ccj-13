from sqlalchemy import JSON, Column, DateTime, Integer, String, func

from .db import Base

# 迁移阶段状态机:
#   NORMAL --freeze--> FROZEN --validate--> VALIDATING --(通过)--> VALIDATED --cutover--> DONE
#     ^                  |                      |                        |
#     +---- recover(必须带 reason, 回到冻结前可写状态) ----+------------+
# DONE 为终态: 不允许自动回退, 避免"两套状态都说自己已切开"。
PHASES = ("NORMAL", "FROZEN", "VALIDATING", "VALIDATED", "DONE")


class MigrationState(Base):
    """单行表(id=1), 全局迁移状态。epoch 为栅栏令牌: 每次迁移动作 +1,
    所有变更都带 epoch 条件更新, 防止并发管理员/重启后出现双主状态。"""

    __tablename__ = "migration_state"

    id = Column(Integer, primary_key=True)  # 恒为 1
    phase = Column(String(32), nullable=False, default="NORMAL")
    epoch = Column(Integer, nullable=False, default=0)
    freeze_version = Column(String(64), nullable=True)   # 本次冻结窗标识
    watermark = Column(Integer, nullable=False, default=0)  # 回填水位(已处理的最大记录 id)
    active_schema = Column(String(8), nullable=False, default="old")  # old | new
    updated_by = Column(String(128), nullable=True)
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
    """只追加审计日志: 操作者、动作、阶段迁移、版本、水位、差异、恢复原因。"""

    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, server_default=func.now())
    operator = Column(String(128), nullable=False)
    action = Column(String(32), nullable=False)       # freeze/validate/cutover/recover/boot
    from_phase = Column(String(32), nullable=True)
    to_phase = Column(String(32), nullable=True)
    epoch = Column(Integer, nullable=True)
    app_version = Column(String(32), nullable=True)
    freeze_version = Column(String(64), nullable=True)
    watermark = Column(Integer, nullable=True)
    diffs = Column(JSON, nullable=True)               # 校验差异明细
    reason = Column(String(500), nullable=True)       # 恢复/失败原因


class IdempotencyKey(Base):
    """幂等键: 重复执行返回首次结果, 不产生二次副作用。"""

    __tablename__ = "idempotency_keys"

    key = Column(String(128), primary_key=True)
    action = Column(String(64), nullable=False)
    request_hash = Column(String(64), nullable=False)
    response_json = Column(JSON, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
