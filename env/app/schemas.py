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


class PlanWindowIn(BaseModel):
    starts_at: str = Field(min_length=1)          # ISO 8601(建议 UTC, 如 2026-09-12T22:00:00Z)
    ends_at: str = Field(min_length=1)


class PlanCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    name: str = Field(min_length=1)
    risk_level: str = "LOW"                        # LOW 可直接启动; HIGH 须另一名管理员审批
    max_retries: int = Field(default=0, ge=0, le=10)  # 每步首次失败后的额外重试次数
    steps: list[PlanStepIn]
    windows: list[PlanWindowIn] = []               # 允许执行的时间窗口(空=不限制)


class PlanAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class PlanRejectAction(PlanAction):
    reason: str = Field(min_length=1)              # 拒绝原因必填, 落审计并阻止启动


class PlanWindowAction(PlanAction):
    # 整体替换窗口; 空列表/省略即清空窗口限制(仅启动前 DRAFT 可修改)
    windows: list[PlanWindowIn] = []


# ---------- 迁移回放与报告 ----------

class CheckpointCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)                # 必须是已存在的迁移计划


class ReplayCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    checkpoint_id: str = Field(min_length=1)


class ReplayAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


# ---------- 回放报告复核 ----------

class ReviewSubmit(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    step_seq: int = Field(ge=1)                    # 被复核的步骤
    report_version: int = Field(ge=1)              # 必须等于当前报告版本(过期 409)
    verdict: str = Field(min_length=1)             # PASS | FAIL
    issue: Optional[str] = None                    # 问题说明(FAIL 时必填)
    fix_tags: list[str] = []                       # 修复标签


class ReviewConfirm(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    report_version: int = Field(ge=1)              # 防止基于过期报告确认


class ReviewReopen(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: Optional[str] = None                   # 重新打开原因(可选, 落事件流水)


# ---------- 批量复核与分派 ----------

class ReviewBatchAssign(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    assignee: str = Field(min_length=1)            # 被分派的复核人
    replay_ids: list[str] = Field(min_length=1, max_length=200)  # 待分派的回放任务
    reason: Optional[str] = None                   # 分派说明(可选, 落分派历史)


class ReviewBatchReviewItem(BaseModel):
    replay_id: str = Field(min_length=1)
    step_seq: int = Field(ge=1)
    verdict: str = Field(min_length=1)             # PASS | FAIL
    issue: Optional[str] = None                    # 问题说明(FAIL 时必填)
    fix_tags: list[str] = []                       # 修复标签


class ReviewBatchReview(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    report_version: int = Field(ge=1)              # 批量项必须同属该报告版本
    items: list[ReviewBatchReviewItem] = Field(min_length=1, max_length=200)


# ---------- 回放证据归档 ----------

class ArchiveCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    replay_id: str = Field(min_length=1)               # 已 COMPLETED 的回放任务
    report_version: int = Field(ge=1)                 # 归档必须锁定的报告版本


class ArchiveAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


# ---------- 归档目录: 保留策略与清理计划 ----------

class ArchiveRetention(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    mode: str = Field(min_length=1)             # NONE | UNTIL | PERMANENT
    retain_until: Optional[str] = None          # ISO 8601, mode=UNTIL 必填(未来时刻)


class CleanupCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    archive_ids: list[str] = Field(min_length=1, max_length=500)


class CleanupAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


# ---------- 迁移前数据质量门禁 ----------

class QualityRuleIn(BaseModel):
    id: str = Field(min_length=1, max_length=64)       # 规则稳定标识(版本间沿用)
    name: Optional[str] = None                          # 规则名称
    type: str = Field(min_length=1)                     # required | format | cross_field | range
    field: Optional[str] = None                          # 作用字段(name/email/tags_csv/tags/id)
    severity: str = "BLOCKER"                            # BLOCKER | WARNING | INFO
    enabled: bool = True
    params: dict[str, Any] = {}                           # pattern/format/op/other_field/min/max/...


class QualityRulesSave(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    rules: list[QualityRuleIn] = Field(min_length=1, max_length=500)
    note: Optional[str] = None                           # 版本说明


class QualityScanCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)


class QualityScanAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class QualityFixCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    issue_ids: list[str] = Field(min_length=1, max_length=1000)
    note: Optional[str] = None


class QualityExemptionCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    issue_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)                    # 豁免原因必填, 与规则版本一起留痕


class QualityExemptionRevoke(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


# ---------- 审计事件回放与补偿 ----------

class AuditEventNote(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)       # 显式补录去重键
    content: str = Field(min_length=1, max_length=2000)
    event_ts: Optional[str] = None                  # ISO 8601(默认当前; 乱序超阈值拒绝)
    plan_id: Optional[str] = None
    batch_id: Optional[str] = None


class AuditSnapshotCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    target_at: Optional[str] = None                # ISO 8601(默认计划终结时间)
    ttl_seconds: Optional[int] = Field(default=None, ge=60, le=30 * 24 * 3600)


class CompensationCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    risk_level: Optional[str] = None              # 可选: 显式 HIGH(默认按动作构成推导)


class CompensationAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class CompensationApprovalAction(BaseModel):
    operator: str = Field(min_length=1)           # 审批人(不能是创建者/执行人)
    idempotency_key: str = Field(min_length=1)


class CompensationRejectAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: str = Field(min_length=1)             # 拒绝原因必填, 落审批历史并阻止执行


class CompensationWindowIn(BaseModel):
    starts_at: str = Field(min_length=1)          # ISO 8601 UTC
    ends_at: str = Field(min_length=1)


class CompensationWindowAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    windows: list[CompensationWindowIn] = []      # 整体替换; 空列表=清空窗口限制


class CompensationCancelAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: Optional[str] = None                  # 取消原因(关联审批历史)


class CompensationRetry(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    action_seq: int = Field(ge=1)


# ---------- 审计证据查询与一致性证明 ----------

class EvidenceSessionCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    start_global_seq: Optional[int] = Field(default=None, ge=0)
    start_ts: Optional[str] = None                  # ISO 8601 闭区间
    end_ts: Optional[str] = None
    event_types: Optional[list[str]] = None         # 白名单: AUDIT_EVENT_TYPES
    sources: Optional[list[str]] = None             # internal | api | system


class EvidencePageQuery(BaseModel):
    operator: str = Field(min_length=1)
    cursor: Optional[str] = None                    # 上一页签发的 next_cursor
    limit: Optional[int] = Field(default=None, ge=1, le=500)


class EvidenceExportCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    segment_size: Optional[int] = Field(default=None, ge=1, le=1000)


class EvidenceExportAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


class EvidenceDownloadIssue(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


# ---------- 证据复核与签署归档 ----------

class EvidenceReviewCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    export_id: Optional[str] = None               # 省略取该会话最近 COMPLETED 包


class EvidenceReviewConclusionSubmit(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    global_seq: int = Field(ge=1)                 # 必须落在固定会话范围内
    verdict: str = Field(min_length=1)            # CONFIRMED | QUESTIONED | EXCLUDED
    note: Optional[str] = Field(default=None, max_length=2000)
    # 乐观版本: 必须等于复核单当前 version(等价于 If-Match, 也可由请求头携带)


class EvidenceReviewAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)


# ---------- 证据封存分发与离线校验 ----------

class EvidenceRecipientRegister(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    recipient: str = Field(min_length=1, max_length=64)     # 接收方账号
    name: Optional[str] = Field(default=None, max_length=128)
    contact: Optional[str] = Field(default=None, max_length=200)


class EvidenceRecipientDisable(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: Optional[str] = Field(default=None, max_length=500)


class EvidenceDistributionCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    review_id: str = Field(min_length=1)                    # 必须已归档
    recipient: str = Field(min_length=1, max_length=64)     # 必须为在册 ACTIVE
    redaction_policy: str = Field(default="STANDARD",
                                  min_length=1, max_length=16)
    valid_until: Optional[str] = None                       # ISO 8601; 与 ttl_seconds 二选一
    ttl_seconds: Optional[int] = Field(default=None, ge=60, le=31_536_000)


class EvidenceDistributionAction(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: Optional[str] = Field(default=None, max_length=500)


class EvidenceDistributionTokenIssue(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    ttl_seconds: Optional[int] = Field(default=None, ge=30, le=86_400)
    # 多接收方: 管理员可指定为任一已分派接收方代签(默认包主接收方)
    recipient: Optional[str] = Field(default=None, min_length=1, max_length=64)


# ---------- 多接收方分派 / 接收回执 / 延期审批 / 生命周期 ----------

class EvidenceAssignmentCreate(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    recipient: str = Field(min_length=1, max_length=64)
    receipt_due_at: Optional[str] = None    # ISO 8601; 默认包 valid_until, 不得晚于它
    required_global_seqs: Optional[list[int]] = None  # 必须确认事件; 省略=包内全部
    note: Optional[str] = Field(default=None, max_length=500)


class EvidenceReceiptEventItem(BaseModel):
    global_seq: int = Field(ge=1)
    result: str = Field(min_length=1)        # CONFIRMED | ANOMALY | REJECTED
    note: Optional[str] = Field(default=None, max_length=2000)


class EvidenceReceiptSubmit(BaseModel):
    operator: str = Field(min_length=1)                       # 必须为已分派接收方本人
    idempotency_key: str = Field(min_length=1)
    download_id: str = Field(min_length=1)                    # 已成功兑换的一次性令牌 id
    manifest_hash: str = Field(min_length=64, max_length=64)  # 固定包摘要
    content_digest: str = Field(min_length=64, max_length=64)
    receipt_type: str = Field(min_length=1)                   # SIGNED | PARTIAL | REJECTED
    note: Optional[str] = Field(default=None, max_length=2000)
    events: Optional[list[EvidenceReceiptEventItem]] = None


class EvidenceExtensionRequest(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    new_valid_until: str = Field(min_length=1)                # ISO 8601, 必须晚于当前有效期
    reason: Optional[str] = Field(default=None, max_length=500)


class EvidenceExtensionApproval(BaseModel):
    operator: str = Field(min_length=1)                       # 不能是申请人; 两名不同操作者
    idempotency_key: str = Field(min_length=1)


class EvidenceExtensionReject(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)


class EvidenceDistributionRecover(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    new_valid_until: Optional[str] = None    # ISO 8601; 与 extend_seconds 二选一
    extend_seconds: Optional[int] = Field(default=None, ge=60, le=31_536_000)
    reason: Optional[str] = Field(default=None, max_length=500)


# ---------- 异常回执争议处理工作流 ----------

class EvidenceDisputeOpen(BaseModel):
    operator: str = Field(min_length=1)                       # 管理员(包创建者)
    idempotency_key: str = Field(min_length=1)
    assignee: Optional[str] = Field(default=None,
                                    min_length=1, max_length=64)  # 当场指定处理人
    reason: Optional[str] = Field(default=None, max_length=2000)  # 打开原因
    handling_opinion: Optional[str] = Field(default=None,
                                            max_length=4000)
    supplementary_evidence: Optional[str] = Field(default=None,
                                                  max_length=4000)


class EvidenceDisputeAssign(BaseModel):
    operator: str = Field(min_length=1)                       # 打开争议的管理员
    idempotency_key: str = Field(min_length=1)
    assignee: str = Field(min_length=1, max_length=64)        # 不能与打开管理员相同
    handling_opinion: str = Field(min_length=1, max_length=4000)
    supplementary_evidence: str = Field(min_length=1, max_length=4000)
    reason: Optional[str] = Field(default=None, max_length=500)


class EvidenceDisputeResolve(BaseModel):
    operator: str = Field(min_length=1)                       # 必须是当前处理人
    idempotency_key: str = Field(min_length=1)
    resolution: str = Field(min_length=1, max_length=4000)    # 处理结论(必填)
    reason: Optional[str] = Field(default=None, max_length=500)


class EvidenceDisputeClose(BaseModel):
    operator: str = Field(min_length=1)                       # 管理员(包创建者)
    idempotency_key: str = Field(min_length=1)
    note: Optional[str] = Field(default=None, max_length=2000)


class EvidenceDisputeReopen(BaseModel):
    operator: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=2000)        # 退回原因(必填)
    new_assignee: Optional[str] = Field(default=None,
                                        min_length=1, max_length=64)
