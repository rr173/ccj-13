"""异常回执争议处理工作流: 在 PARTIAL/REJECTED 接收回执之上打开争议单。

设计要点
========
1. 争议单(evidence_receipt_disputes)只能由管理员从一张 **PARTIAL 或 REJECTED**
   回执打开; 每张回执至多一张争议单(uq_evidence_dispute_receipt 唯一约束兜底),
   重复打开(含不同幂等键)幂等回显已有争议单 —— 关闭后也不允许就同一回执重开。
2. 状态机: OPEN --(指定处理人)--> ASSIGNED --(处理人提交结论)--> RESOLVED
   --(管理员确认)--> CLOSED; 管理员可把 RESOLVED 退回 ASSIGNED(要求补充处理)。
   - 处理人在**被指定之后**才能提交处理结论, 且必须是当前处理人本人;
   - 只有管理员(分发包创建者)确认后争议单才关闭, 关闭人不得是处理人本人;
   - 处理人不能与打开争议的管理员相同;
   - CLOSED 为终态, 拒绝一切后续状态变化。
3. 打开时固化**只读依据**: 原始回执(含逐事件结果 receipt_snapshot)与分发包摘要
   (package_snapshot: package_id/状态/各类摘要/事件范围)。之后争议单只依据快照
   展示与处理, 绝不改写原始回执、逐事件结果与分发包摘要(争议流程只写自己的表,
   并向分发包事件流追加 dispute.* 留痕)。
4. 过期(EXPIRED/PENDING_PROCESS)或撤销(REVOKED)的分发包**不能新开**争议;
   已存在的争议不受之后包撤销/过期影响(打开时刻是唯一闸门)。
5. 每次状态动作都向争议单事件流水追加一行(操作者/时间/原因/前后状态);
   所有状态落库, 服务重启后争议单、处理人、结论与事件流水完整保留。
6. 幂等: 全部写动作走 evidence.distribution.dispute.* 幂等框架, 同键重放回显
   首次结果; 自然幂等(无键/异键重复打开同一回执)返回已有争议单。
"""
import uuid
from datetime import datetime

from sqlalchemy.orm import Session

from . import distribution
from .models import (
    EVIDENCE_DISPUTE_EVENTS, EVIDENCE_DISPUTE_STATUSES,
    EvidenceDistribution, EvidenceDistributionReceipt, EvidenceReceiptDispute,
    EvidenceReceiptDisputeEvent,
)

NAMESPACE = "evidence.distribution.dispute"

# 异常回执类型: 只有 PARTIAL/REJECTED 可以打开争议
DISPUTABLE_RECEIPT_TYPES = (distribution.RECEIPT_PARTIAL,
                           distribution.RECEIPT_REJECTED)
# 不允许新开争议的包派生状态
_DISPUTE_BLOCKED_PACKAGE_STATUS = {
    "REVOKED": ("package_revoked", "分发包已撤销, 不能新开争议(历史争议只读保留)"),
    "EXPIRED": ("package_expired", "分发包已过有效期, 不能新开争议(请先恢复或重新签发)"),
    "PENDING_PROCESS": ("package_pending_process",
                        "分发包已进入待处理状态, 不能新开争议(请先恢复或重新签发)"),
}


class DisputeError(Exception):
    """争议单状态/权限/参数错误 -> 409/403/404/422。"""

    def __init__(self, reason: str, code: str = "dispute_conflict",
                 status_code: int = 409, extra: dict | None = None):
        super().__init__(reason)
        self.code = code
        self.status_code = status_code
        self.extra = extra or {}


# 与分发域共用同一把进程内栅栏(串行化同包上的回执/争议写动作)
_lock = distribution._lock


# ---------- 基础工具 ----------

def _now() -> datetime:
    return distribution._now()


def _clean(v, limit):
    v = (v or "").strip()
    return v[:limit] or None


def normalize_handler(handler: str) -> str:
    """处理人账号规范化: 复用接收方账号字符规则(不要求其在接收方名册内 ——
    争议处理人是内部处理角色, 可以是管理员账号)。"""
    return distribution.normalize_recipient(handler)


def get_dispute(db: Session, dispute_id: str) -> EvidenceReceiptDispute:
    d = db.get(EvidenceReceiptDispute, dispute_id)
    if d is None:
        raise DisputeError(f"争议单 {dispute_id} 不存在",
                          code="dispute_not_found", status_code=404)
    return d


def get_receipt(db: Session, receipt_id: str
                ) -> EvidenceDistributionReceipt:
    r = db.get(EvidenceDistributionReceipt, receipt_id)
    if r is None:
        raise DisputeError(f"接收回执 {receipt_id} 不存在",
                          code="receipt_not_found", status_code=404)
    return r


def find_dispute_for_receipt(db: Session, receipt_id: str
                             ) -> EvidenceReceiptDispute | None:
    return (db.query(EvidenceReceiptDispute)
            .filter(EvidenceReceiptDispute.receipt_id == receipt_id).first())


def _append_event(db: Session, dispute: EvidenceReceiptDispute, event: str,
                  operator: str, *, from_status: str | None,
                  to_status: str | None, reason: str | None = None,
                  detail: dict | None = None) -> None:
    db.add(EvidenceReceiptDisputeEvent(
        dispute_id=dispute.id, event=event, operator=operator,
        from_status=from_status, to_status=to_status,
        reason=reason, detail=detail))
    # 同步向分发包事件流追加一行(争议流程对分发侧可观测)
    distribution._add_event(
        db, dispute.distribution_id, event, operator,
        reason=(f"争议 {dispute.id}: {reason}" if reason else
                f"争议 {dispute.id}: {event}")[:500],
        detail={"dispute_id": dispute.id,
                "receipt_id": dispute.receipt_id, **(detail or {})})


def receipt_snapshot(r: EvidenceDistributionReceipt) -> dict:
    """打开时刻固化的原始回执只读副本(含逐事件结果)。"""
    return distribution.receipt_to_dict(r, with_events=True)


def package_snapshot(dist: EvidenceDistribution) -> dict:
    """打开时刻固化的分发包摘要只读副本(不含完整 manifest/流水)。"""
    return distribution.distribution_to_dict(
        dist, with_manifest=False, with_events=False, with_receipts=False)


# ---------- 打开争议 ----------

def open_dispute(db: Session, receipt_id: str, *, operator: str,
                 assignee: str | None = None,
                 reason: str | None = None,
                 handling_opinion: str | None = None,
                 supplementary_evidence: str | None = None
                 ) -> tuple[EvidenceReceiptDispute, dict]:
    """管理员把一张 PARTIAL/REJECTED 回执打开为争议单。

    返回 (争议单, {deduped, assigned})。重复打开同一回执(任意幂等键)幂等回显
    已有争议单; assignee 省略 -> 争议停留 OPEN 等待指派, 当场指定 -> 直接
    ASSIGNED 并记录处理意见/补充证据摘要。"""
    reason_c = _clean(reason, 2000)
    opinion_c = _clean(handling_opinion, 4000)
    evidence_c = _clean(supplementary_evidence, 4000)
    assignee_c = normalize_handler(assignee) if assignee else None
    if assignee_c and assignee_c == operator:
        raise DisputeError(
            "处理人不能与打开争议的管理员相同(职责分离)",
            code="assignee_is_opener")
    # 当场指定处理人即同时记录处理意见与补充证据摘要(缺一不可);
    # 不指定处理人时争议停留 OPEN, 后续 assign 时再记录。
    if assignee_c and (not opinion_c or not evidence_c):
        raise DisputeError(
            "打开争议并指定处理人时必须同时记录处理意见与补充证据摘要"
            "(或先打开为 OPEN, 稍后指派)",
            code="handling_detail_required", status_code=422)
    with _lock:
        receipt = get_receipt(db, receipt_id)
        dist = distribution.get_distribution(db, receipt.distribution_id)
        if operator != dist.created_by:
            raise DisputeError(
                f"只有分发包创建管理员 {dist.created_by} 可以打开回执争议",
                code="not_distribution_admin", status_code=403)
        # 自然幂等: 同一回执已有争议单 -> 原样回显(状态/结论以首次创建为准)
        existing = find_dispute_for_receipt(db, receipt_id)
        if existing is not None:
            _append_event(db, existing, "dispute.open", operator,
                          from_status=existing.status,
                          to_status=existing.status,
                          reason="同一回执重复打开争议, 幂等回显已有争议单",
                          detail={"idempotent_replay": True,
                                  "receipt_id": receipt_id})
            db.commit()
            return existing, {"deduped": True,
                              "assigned": existing.assignee is not None}
        if receipt.recipient_id == operator:
            # 管理员原则上不是回执接收方; 即便同账号也不允许(打开人即被异议回执人)
            raise DisputeError(
                "打开争议的管理员不能同时是该异常回执的提交接收方",
                code="opener_is_recipient", status_code=403)
        if receipt.receipt_type not in DISPUTABLE_RECEIPT_TYPES:
            raise DisputeError(
                f"只有 PARTIAL/REJECTED 回执可以打开争议, 回执 "
                f"{receipt_id} 为 {receipt.receipt_type}",
                code="receipt_not_disputable")
        st = distribution.effective_status(dist)
        if st in _DISPUTE_BLOCKED_PACKAGE_STATUS:
            code, msg = _DISPUTE_BLOCKED_PACKAGE_STATUS[st]
            raise DisputeError(msg, code=code)

        now = _now()
        assigned_now = assignee_c is not None
        dispute = EvidenceReceiptDispute(
            id="EDC" + uuid.uuid4().hex[:12],
            receipt_id=receipt.id, distribution_id=dist.id,
            assignment_id=receipt.assignment_id,
            receipt_type=receipt.receipt_type,
            recipient_id=receipt.recipient_id,
            status="ASSIGNED" if assigned_now else "OPEN",
            opened_by=operator, opened_at=now, open_reason=reason_c,
            assignee=assignee_c if assigned_now else None,
            assigned_at=now if assigned_now else None,
            handling_opinion=opinion_c if assigned_now else None,
            supplementary_evidence=evidence_c if assigned_now else None,
            receipt_snapshot=receipt_snapshot(receipt),
            package_snapshot=package_snapshot(dist))
        db.add(dispute)
        db.flush()
        _append_event(db, dispute, "dispute.open", operator,
                      from_status=None, to_status=dispute.status,
                      reason=(f"打开异常回执争议(回执 {receipt.receipt_type}, "
                              f"接收方 {receipt.recipient_id})"
                              + (f": {reason_c}" if reason_c else "")
                              + (f"; 当场指定处理人 {assignee_c}"
                                 if assigned_now else "; 待指定处理人"))[:2000],
                      detail={"receipt_id": receipt.id,
                              "receipt_type": receipt.receipt_type,
                              "recipient": receipt.recipient_id,
                              "package_id": dist.id,
                              "package_status_at_open": st,
                              "assignee": assignee_c})
        db.commit()
        return dispute, {"deduped": False, "assigned": assigned_now}


# ---------- 指定/改派处理人 ----------

def assign_dispute(db: Session, dispute_id: str, *, operator: str,
                   assignee: str, handling_opinion: str,
                   supplementary_evidence: str,
                   reason: str | None = None) -> EvidenceReceiptDispute:
    """管理员为 OPEN/RESOLVED 争议指定(或改派)处理人, 记录处理意见与补充证据
    摘要。OPEN/ASSIGNED/RESOLVED(退回处理)可调用; RESOLVED 指派新人视为退回
    并重新分派; CLOSED 拒绝。"""
    assignee_c = normalize_handler(assignee)
    opinion_c = _clean(handling_opinion, 4000)
    evidence_c = _clean(supplementary_evidence, 4000)
    if not opinion_c:
        raise DisputeError("指定处理人必须记录处理意见",
                           code="handling_opinion_required", status_code=422)
    if not evidence_c:
        raise DisputeError("指定处理人必须记录补充证据摘要",
                           code="supplementary_evidence_required",
                           status_code=422)
    with _lock:
        dispute = get_dispute(db, dispute_id)
        if operator != dispute.opened_by:
            raise DisputeError(
                f"只有打开争议的管理员 {dispute.opened_by} 可以指定/改派处理人",
                code="not_dispute_admin", status_code=403)
        if dispute.status == "CLOSED":
            raise DisputeError(
                f"争议单 {dispute_id} 已关闭(CLOSED), 不能再指定处理人",
                code="dispute_closed")
        if assignee_c == dispute.opened_by:
            raise DisputeError(
                "处理人不能与打开争议的管理员相同(职责分离)",
                code="assignee_is_opener")
        if dispute.status not in ("OPEN", "ASSIGNED", "RESOLVED"):
            raise DisputeError(
                f"争议单当前状态 {dispute.status}, 不能指定处理人",
                code="invalid_dispute_state")
        prev_status = dispute.status
        prev_assignee = dispute.assignee
        reassigned = prev_assignee is not None and prev_assignee != assignee_c
        returning = prev_status == "RESOLVED"
        now = _now()
        dispute.assignee = assignee_c
        dispute.assigned_at = now
        dispute.handling_opinion = opinion_c
        dispute.supplementary_evidence = evidence_c
        dispute.resolution = None
        dispute.resolved_by = None
        dispute.resolved_at = None
        dispute.status = "ASSIGNED"
        if returning:
            dispute.reopen_count += 1
        db.flush()
        verb = ("退回并重新分派" if returning else
                "改派处理人" if reassigned else "指定处理人")
        event_type = "dispute.reopen" if returning else "dispute.assign"
        _append_event(db, dispute, event_type, operator,
                      from_status=prev_status, to_status="ASSIGNED",
                      reason=(f"{verb} {assignee_c}, 记录处理意见与补充证据摘要"
                              + (f": {_clean(reason, 400) or ''}"
                                 if reason else ""))[:2000],
                      detail={"assignee": assignee_c,
                              "previous_assignee": prev_assignee
                              if reassigned else None,
                              "reassigned": reassigned,
                              "returned_for_rework": returning,
                              "handling_opinion": opinion_c,
                              "supplementary_evidence": evidence_c})
        db.commit()
        return dispute


# ---------- 处理人提交处理结论 ----------

def resolve_dispute(db: Session, dispute_id: str, *, operator: str,
                    resolution: str, reason: str | None = None
                    ) -> EvidenceReceiptDispute:
    """当前处理人在被指定后提交处理结论 -> RESOLVED(等待管理员确认)。"""
    resolution_c = _clean(resolution, 4000)
    if not resolution_c:
        raise DisputeError("处理结论不能为空",
                           code="resolution_required", status_code=422)
    with _lock:
        dispute = get_dispute(db, dispute_id)
        if dispute.status == "CLOSED":
            raise DisputeError(
                f"争议单 {dispute_id} 已关闭, 不能再提交处理结论",
                code="dispute_closed")
        if dispute.assignee is None or dispute.status == "OPEN":
            raise DisputeError(
                "争议单尚未指定处理人(OPEN), 处理人在被指定后才能提交结论",
                code="dispute_not_assigned", status_code=403)
        if operator != dispute.assignee:
            raise DisputeError(
                f"操作者 {operator} 不是争议单当前处理人 "
                f"{dispute.assignee}, 无权提交处理结论",
                code="not_current_assignee", status_code=403)
        if dispute.status == "RESOLVED":
            raise DisputeError(
                f"处理人 {dispute.resolved_by} 已提交处理结论, 等待管理员确认; "
                "不能重复提交(如需修改请由管理员退回处理)",
                code="dispute_already_resolved")
        if dispute.status != "ASSIGNED":
            raise DisputeError(
                f"争议单当前状态 {dispute.status}, 不能提交处理结论",
                code="invalid_dispute_state")
        prev_status = dispute.status
        now = _now()
        dispute.status = "RESOLVED"
        dispute.resolution = resolution_c
        dispute.resolved_by = operator
        dispute.resolved_at = now
        db.flush()
        _append_event(db, dispute, "dispute.resolve", operator,
                      from_status=prev_status, to_status="RESOLVED",
                      reason=(f"处理人 {operator} 提交处理结论"
                              + (f": {resolution_c[:300]}" if resolution_c
                                 else ""))[:2000],
                      detail={"assignee": operator,
                              "resolution": resolution_c,
                              "note": _clean(reason, 2000)})
        db.commit()
        return dispute


# ---------- 管理员确认关闭 / 退回处理 ----------

def close_dispute(db: Session, dispute_id: str, *, operator: str,
                  note: str | None = None) -> EvidenceReceiptDispute:
    """管理员确认处理结论后关闭争议单(CLOSED 终态)。

    关闭须由打开争议的管理员(或其他管理员 —— 这里限定包创建管理员)执行,
    且不得是处理人本人; 只有 RESOLVED 可关闭。"""
    note_c = _clean(note, 2000)
    with _lock:
        dispute = get_dispute(db, dispute_id)
        dist = distribution.get_distribution(db, dispute.distribution_id)
        if operator != dist.created_by:
            raise DisputeError(
                f"只有分发包创建管理员 {dist.created_by} 可以确认关闭争议",
                code="not_distribution_admin", status_code=403)
        if dispute.status == "CLOSED":
            raise DisputeError(
                f"争议单 {dispute_id} 已关闭, 不能重复关闭",
                code="dispute_closed")
        if dispute.status != "RESOLVED":
            raise DisputeError(
                f"争议单当前状态 {dispute.status}: 只有处理人已提交结论的 "
                "RESOLVED 争议才能由管理员确认关闭",
                code="dispute_not_resolved")
        if dispute.resolved_by and operator == dispute.resolved_by:
            raise DisputeError(
                "管理员不能确认关闭自己作为处理人提交结论的争议(职责分离)",
                code="closer_is_assignee", status_code=403)
        prev_status = dispute.status
        now = _now()
        dispute.status = "CLOSED"
        dispute.closed_by = operator
        dispute.closed_at = now
        dispute.close_note = note_c
        db.flush()
        _append_event(db, dispute, "dispute.close", operator,
                      from_status=prev_status, to_status="CLOSED",
                      reason=(f"管理员 {operator} 确认处理结论并关闭争议"
                              + (f": {note_c}" if note_c else ""))[:2000],
                      detail={"closed_by": operator,
                              "resolved_by": dispute.resolved_by,
                              "resolution": dispute.resolution,
                              "note": note_c})
        db.commit()
        return dispute


def reopen_dispute(db: Session, dispute_id: str, *, operator: str,
                   reason: str,
                   new_assignee: str | None = None
                   ) -> EvidenceReceiptDispute:
    """管理员把 RESOLVED 争议退回处理(回到 ASSIGNED); 可同时改派处理人。
    退回原因必填。"""
    reason_c = _clean(reason, 2000)
    if not reason_c:
        raise DisputeError("退回处理必须填写原因",
                           code="reopen_reason_required", status_code=422)
    new_assignee_c = normalize_handler(new_assignee) if new_assignee else None
    with _lock:
        dispute = get_dispute(db, dispute_id)
        dist = distribution.get_distribution(db, dispute.distribution_id)
        if operator != dist.created_by:
            raise DisputeError(
                f"只有分发包创建管理员 {dist.created_by} 可以退回争议",
                code="not_distribution_admin", status_code=403)
        if dispute.status == "CLOSED":
            raise DisputeError(
                f"争议单 {dispute_id} 已关闭, 不能退回(终态)",
                code="dispute_closed")
        if dispute.status != "RESOLVED":
            raise DisputeError(
                f"争议单当前状态 {dispute.status}: 只有 RESOLVED 可以退回处理",
                code="dispute_not_resolved")
        target = new_assignee_c or dispute.assignee
        if target and target == dispute.opened_by:
            raise DisputeError(
                "处理人不能与打开争议的管理员相同(职责分离)",
                code="assignee_is_opener")
        prev_status = dispute.status
        now = _now()
        dispute.status = "ASSIGNED"
        dispute.reopen_count += 1
        if new_assignee_c:
            dispute.assignee = new_assignee_c
            dispute.assigned_at = now
        dispute.resolution = None
        dispute.resolved_by = None
        dispute.resolved_at = None
        db.flush()
        _append_event(db, dispute, "dispute.reopen", operator,
                      from_status=prev_status, to_status="ASSIGNED",
                      reason=(f"管理员 {operator} 退回处理: {reason_c}"
                              + (f"; 改派 {new_assignee_c}"
                                 if new_assignee_c else ""))[:2000],
                      detail={"reason": reason_c,
                              "new_assignee": new_assignee_c})
        db.commit()
        return dispute


# ---------- 查询 / 视图 ----------

def list_disputes(db: Session, *, status_filter: str | None = None,
                  package_id: str | None = None,
                  assignee: str | None = None,
                  receipt_id: str | None = None,
                  pending_only: bool = False,
                  limit: int = 100) -> list[EvidenceReceiptDispute]:
    q = db.query(EvidenceReceiptDispute)
    if status_filter:
        q = q.filter(EvidenceReceiptDispute.status == status_filter)
    if package_id:
        q = q.filter(EvidenceReceiptDispute.distribution_id == package_id)
    if assignee:
        q = q.filter(EvidenceReceiptDispute.assignee == assignee)
    if receipt_id:
        q = q.filter(EvidenceReceiptDispute.receipt_id == receipt_id)
    if pending_only:
        q = q.filter(EvidenceReceiptDispute.status != "CLOSED")
    rows = (q.order_by(EvidenceReceiptDispute.created_at.desc(),
                       EvidenceReceiptDispute.id.desc())
            .limit(min(max(1, limit), 500)).all())
    return rows


def dispute_to_dict(d: EvidenceReceiptDispute, *,
                    with_events: bool = True,
                    with_snapshots: bool = False) -> dict:
    out = {
        "dispute_id": d.id,
        "receipt_id": d.receipt_id,
        "package_id": d.distribution_id,
        "assignment_id": d.assignment_id,
        "receipt_type": d.receipt_type,
        "recipient": d.recipient_id,
        "status": d.status,
        "opened_by": d.opened_by,
        "opened_at": d.opened_at.isoformat() if d.opened_at else None,
        "open_reason": d.open_reason,
        "assignee": d.assignee,
        "assigned_at": d.assigned_at.isoformat() if d.assigned_at else None,
        "handling_opinion": d.handling_opinion,
        "supplementary_evidence": d.supplementary_evidence,
        "resolution": d.resolution,
        "resolved_by": d.resolved_by,
        "resolved_at": d.resolved_at.isoformat() if d.resolved_at else None,
        "closed_by": d.closed_by,
        "closed_at": d.closed_at.isoformat() if d.closed_at else None,
        "close_note": d.close_note,
        "reopen_count": d.reopen_count,
        "created_at": d.created_at.isoformat() if d.created_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    }
    if with_events:
        out["events"] = [dispute_event_to_dict(e)
                         for e in sorted(d.events, key=lambda x: x.id)]
    if with_snapshots:
        # 只读依据快照(打开时刻固化): 原始回执(含逐事件结果) + 分发包摘要
        out["receipt_snapshot"] = d.receipt_snapshot
        out["package_snapshot"] = d.package_snapshot
    return out


def dispute_event_to_dict(e: EvidenceReceiptDisputeEvent) -> dict:
    return {
        "id": e.id,
        "ts": e.ts.isoformat() if e.ts else None,
        "event": e.event,
        "from_status": e.from_status,
        "to_status": e.to_status,
        "operator": e.operator,
        "reason": e.reason,
        "detail": e.detail,
    }


def dispute_summary(d: EvidenceReceiptDispute) -> dict:
    """分发包详情/回执列表里嵌入的争议摘要(不含快照/流水)。"""
    return {
        "dispute_id": d.id,
        "receipt_id": d.receipt_id,
        "status": d.status,
        "receipt_type": d.receipt_type,
        "recipient": d.recipient_id,
        "opened_by": d.opened_by,
        "opened_at": d.opened_at.isoformat() if d.opened_at else None,
        "assignee": d.assignee,
        "resolution": d.resolution,
        "resolved_by": d.resolved_by,
        "resolved_at": d.resolved_at.isoformat()
        if d.resolved_at else None,
        "closed_by": d.closed_by,
        "closed_at": d.closed_at.isoformat() if d.closed_at else None,
    }


def require_dispute_view_access(db: Session, d: EvidenceReceiptDispute,
                                operator: str) -> None:
    """查看权限: 分发包创建管理员、争议处理人(含打开人)、被异议回执接收方。"""
    dist = db.get(EvidenceDistribution, d.distribution_id)
    if operator in (d.opened_by, d.assignee, d.recipient_id):
        return
    if dist is not None and operator == dist.created_by:
        return
    if dist is not None and \
            distribution._get_assignment(dist, operator) is not None:
        return
    raise distribution.DistributionForbidden(
        f"操作者 {operator} 无权查看争议单 {d.id}")


# ---------- 幂等框架(evidence.distribution.dispute.* 命名空间) ----------

def run_dispute_action(db: Session, *, action: str, operator: str,
                       idempotency_key: str, payload: dict, fn,
                       dispute_id: str | None = None,
                       receipt_id: str | None = None
                       ) -> tuple[dict, bool]:
    req_hash = distribution._hash_obj({
        "action": f"{NAMESPACE}.{action}",
        "dispute_id": dispute_id, "receipt_id": receipt_id,
        "payload": payload})
    from .models import IdempotencyKey
    existing = db.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise DisputeError("幂等键被不同请求复用",
                               code="idempotency_reuse")
        return existing.response_json, True
    result = fn(db)
    if isinstance(result, tuple):  # open_dispute 的 (dispute, info)
        obj, info = result
        result = dispute_to_dict(obj)
        result.update(info)
    else:
        result = dispute_to_dict(result)
    db.add(IdempotencyKey(key=idempotency_key,
                          action=f"{NAMESPACE}.{action}",
                          request_hash=req_hash, response_json=result))
    db.commit()
    return result, False
