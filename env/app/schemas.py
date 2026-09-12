from typing import Any, Optional

from pydantic import BaseModel, Field


class RecordIn(BaseModel):
    id: int
    name: str
    email: str = ""
    tags_csv: str = ""          # 旧结构写入
    tags: Optional[list[str]] = None  # 新结构写入(v2)


class AdminAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    expected_epoch: Optional[int] = None  # 客户端栅栏: 防止基于过期状态做决定


class RecoverAction(AdminAction):
    reason: str = Field(min_length=1)  # 恢复必须说明原因


class StatusOut(BaseModel):
    phase: str
    epoch: int
    freeze_version: Optional[str]
    watermark: int
    active_schema: str
    app_version: str
    pending_diffs: int


class ActionResult(BaseModel):
    ok: bool
    phase: str
    epoch: int
    replayed: bool = False
    diffs: list[dict[str, Any]] = []
    detail: str = ""
