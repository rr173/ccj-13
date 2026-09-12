from typing import Any, Optional

from pydantic import BaseModel, Field


class RecordIn(BaseModel):
    id: int
    name: str
    email: str = ""
    tags_csv: str = ""          # 旧结构写入
    tags: Optional[list[str]] = None  # 新结构写入(v2)


class BatchCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    biz: str = Field(min_length=1)   # 业务分组名
    id_start: int                    # 记录范围(含)
    id_end: int                      # 记录范围(含)


class AdminAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    expected_epoch: Optional[int] = None  # 客户端栅栏: 防止基于过期状态做决定


class RecoverAction(AdminAction):
    reason: str = Field(min_length=1)  # 恢复必须说明原因


class BatchOut(BaseModel):
    id: str
    biz: str
    id_start: int
    id_end: int
    phase: str
    epoch: int
    freeze_version: Optional[str]
    watermark: Optional[int]
    active_schema: str
    progress: dict[str, int]
    pending_diffs: int
    diffs: list[dict[str, Any]] = []


class ActionResult(BaseModel):
    ok: bool
    batch_id: Optional[str] = None
    phase: Optional[str] = None
    epoch: Optional[int] = None
    replayed: bool = False
    diffs: list[dict[str, Any]] = []
    detail: str = ""


# ---------- 迁移计划编排 ----------

class PlanStepIn(BaseModel):
    seq: int = Field(ge=1)                       # 计划内唯一顺序
    batch_id: str = Field(min_length=1)          # 必须是已存在的批次
    depends_on: list[int] = []                   # 依赖步骤的 seq(必须全部成功后才执行)
    max_retries: Optional[int] = Field(default=None, ge=0, le=10)


class PlanCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    max_retries: int = Field(default=0, ge=0, le=10)  # 每步首次失败后的额外重试次数
    steps: list[PlanStepIn]


class PlanAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
