"""证据复核与签署归档: 在固定边界证据查询会话与已完成导出包之上做双人逐事件复核。

设计要点
========
1. 复核单(evidence_reviews): 运维人员只能从一个**已翻完(CLOSED)且未断链**、
   并且存在 **COMPLETED 且摘要校验通过**导出包的证据会话创建复核单。创建时一次性
   **固定依据**(之后永不变更):
   - 会话筛选条件快照(filters)、起始/结束 global_seq、范围内事件数与每条流边界;
   - 固定范围内事件列表的指纹(scope_fingerprint, 由逐行规范化事件摘要有序拼接);
   - 导出 id、manifest_hash、content_digest。
   底层事件链、查询结果(会话/页面)与已完成导出包全程只读, 本模块只写
   evidence_reviews* 自身表。

2. 逐事件签署(evidence_review_conclusions): 结论只能引用固定范围内的事件
   (按 global_seq 定位); 结论为 CONFIRMED/QUESTIONED/EXCLUDED, 存疑与排除必须
   带说明。同一依据版本(scope_version)下 (event, operator) 唯一: 每名操作者对
   每个事件只能签一次, 重复签署拒绝且不覆盖; 同一会话需要**两名不同操作者**分别
   签署。提交必须带 If-Match(=复核单当前 version), 过期版本 -> 409 拒绝,
   version 在每次成功提交后 +1(乐观并发, 后写不覆盖先写)。

3. 依据再校验: 每次提交结论/归档前(以及显式 reverify)重新:
   - 在会话固定边界内全量重算哈希链(复用 evidence.verify_fragment);
   - 重算固定范围指纹比对 scope_fingerprint;
   - 回读导出包重算 manifest/content 摘要(只读, 不改动导出任务状态);
   任一不一致: 复核单转 INVALIDATED, 记录机器可读原因(invalid_reason,
   原因码 chain_broken/scope_changed/manifest_hash_mismatch/
   package_verification_failed), 冻结提交与归档; 依据修复后重新校验通过:
   scope_version+1, 旧结论作为历史保留, 当前签署集合清空, 基于新版本重新逐事件
   提交结论。

4. 归档: 固定范围内**每个事件**都有两名不同操作者的当前版本结论后才允许归档;
   归档生成不可变签署摘要(结论统计/事件范围/操作者列表/manifest_hash/
   scope_fingerprint/逐事件签署明细)与 signature_hash; ARCHIVED 为终态,
   禁止任何修改。

5. 并发: 进程内复用 evidence._evidence_lock 栅栏 + 数据库唯一约束兜底;
   所有管理操作走 evidence 幂等框架(evidence.review.* 命名空间), 同键重放返回
   首次结果。
"""
import hashlib
import uuid
from datetime import datetime

from sqlalchemy.orm import Session

from . import evidence
from .models import (
    AuditEvent, EvidenceExport, EvidenceReview, EvidenceReviewConclusion,
    EvidenceReviewEvent, EvidenceSession,
)

REVIEW_NAMESPACE = "evidence.review"

# 创建/提交/归档校验失败 -> 与证据模块相同的 HTTP 语义
ReviewNotFound = evidence.EvidenceNotFound


class ReviewStateError(Exception):
    """状态不允许 / 权限不符 / 幂等键复用 / 引用越界 / 重复签署 -> 409。"""

    def __init__(self, reason: str, code: str = "review_conflict",
                 extra: dict | None = None):
        super().__init__(reason)
        self.code = code
        self.extra = extra or {}


class ReviewVersionConflict(Exception):
    """If-Match 与复核单当前版本不一致(过期或重复写入) -> 409。"""

    def __init__(self, reason: str, *, expected: int, supplied: int | None,
                 code: str = "version_conflict"):
        super().__init__(reason)
        self.code = code
        self.expected = expected
        self.supplied = supplied


# 与 evidence 模块共用同一把进程内栅栏(同一证据域串行化)
_lock = evidence._evidence_lock


# ---------- 工具 ----------

def _now() -> datetime:
    return evidence.now_utc_naive()


def get_review(db: Session, review_id: str) -> EvidenceReview:
    r = db.get(EvidenceReview, review_id)
    if r is None:
        raise ReviewNotFound(f"证据复核单 {review_id} 不存在")
    return r


def _add_event(db: Session, review_id: str, event: str, operator: str, *,
               reason: str | None = None, detail: dict | None = None) -> None:
    db.add(EvidenceReviewEvent(
        review_id=review_id, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail))


def _scoped_rows(db: Session, s: EvidenceSession,
                 review: EvidenceReview | None = None) -> list[AuditEvent]:
    """固定范围内的全部事件(与会话翻页/导出同一查询与排序)。"""
    start_gs = review.start_global_seq if review is not None else s.start_global_seq
    return evidence._query_scoped(db, s, after_gs=start_gs, limit=1_000_000)


def scope_fingerprint(rows: list[AuditEvent]) -> str:
    """固定范围指纹: 逐行规范化事件(与段/页面同一序列化)有序拼接的 sha256。

    任何行内容/顺序/数量变化都会改变指纹。"""
    h = hashlib.sha256()
    for r in rows:
        h.update(evidence.canonical_event_bytes(r))
    return h.hexdigest()


def _verify_package_readonly(e: EvidenceExport) -> dict:
    """回读导出包重算摘要(只读): 不改动导出任务状态。

    复用 evidence.verify_package_bytes, 返回 {valid, issues, reason_code}。"""
    if e.status != "COMPLETED" or not e.package_path:
        return {"valid": False,
                "issues": [f"导出 {e.id} 当前状态 {e.status}, 证据包不可用"],
                "reason_code": "package_unavailable"}
    return evidence.verify_package_bytes(
        e.package_path, expected_manifest=e.manifest,
        expected_content_digest=e.content_digest,
        expected_manifest_hash=e.manifest_hash)


def _invalidate(db: Session, review: EvidenceReview, code: str, message: str,
                *, operator: str = "system",
                breaks: list[dict] | None = None,
                detail: dict | None = None) -> None:
    """把复核单置为 INVALIDATED 并留机器可读原因(已经是该状态则仅追加流水)。"""
    reason = {"code": code, "message": message}
    if breaks:
        reason["breaks"] = breaks
    if detail:
        reason["detail"] = detail
    review.status = "INVALIDATED"
    review.invalid_reason = reason
    review.invalidated_at = _now()
    _add_event(db, review.id, "review.invalidated", operator,
               reason=f"[{code}] {message}"[:500], detail=reason)


def reverify_basis(db: Session, review: EvidenceReview, *,
                   operator: str = "system",
                   invalidate: bool = True) -> tuple[bool, dict | None]:
    """重新校验固定依据(只读事件链/会话/导出, 只写复核单自身状态)。

    返回 (是否通过, 失效原因 dict|None)。不通过且 invalidate=True 时把复核单
    转为 INVALIDATED; 通过时若当前为 INVALIDATED 则恢复为 OPEN 并推进
    scope_version(旧结论保留为历史, 当前签署集合清空)。
    """
    s = evidence.get_session(db, review.session_id)
    e = evidence.get_export(db, review.export_id)

    # 1) 固定范围事件链全量重算
    rows = _scoped_rows(db, s, review)
    breaks = evidence.verify_fragment(db, s, rows,
                                      prev_delivered_gs=review.start_global_seq)
    if breaks:
        reason = {"code": "chain_broken",
                  "message": "固定会话范围重新校验发现哈希链变化: "
                             + breaks[0]["message"],
                  "breaks": breaks[:20]}
        if invalidate:
            _invalidate(db, review, "chain_broken", reason["message"],
                        operator=operator, breaks=breaks[:20])
        return False, reason

    # 2) 范围指纹(数量/内容/顺序)
    fp = scope_fingerprint(rows)
    if len(rows) != review.total_events or fp != review.scope_fingerprint:
        detail = {"expected_events": review.total_events,
                  "actual_events": len(rows),
                  "expected_fingerprint": review.scope_fingerprint,
                  "actual_fingerprint": fp}
        reason = {"code": "scope_changed",
                  "message": ("固定事件范围发生变化: 创建时 "
                              f"{review.total_events} 个事件, 当前 {len(rows)} 个"
                              if len(rows) != review.total_events
                              else "固定事件范围内容指纹与创建时不一致(事件被改写)"),
                  "detail": detail}
        if invalidate:
            _invalidate(db, review, "scope_changed", reason["message"],
                        operator=operator, detail=detail)
        return False, reason

    # 3) 导出 manifest_hash 固定值比对(库内记录未被外部改)
    if e.manifest_hash != review.export_manifest_hash:
        detail = {"expected_manifest_hash": review.export_manifest_hash,
                  "actual_manifest_hash": e.manifest_hash}
        reason = {"code": "manifest_hash_mismatch",
                  "message": "导出 manifest_hash 与复核单固定值不一致",
                  "detail": detail}
        if invalidate:
            _invalidate(db, review, "manifest_hash_mismatch",
                        reason["message"], operator=operator, detail=detail)
        return False, reason

    # 4) 回读证据包重算摘要(只读)
    check = _verify_package_readonly(e)
    if not check["valid"]:
        detail = {"reason_code": check.get("reason_code"),
                  "issues": check.get("issues", [])}
        reason = {"code": "package_verification_failed",
                  "message": "导出证据包重新校验失败: "
                             + "; ".join(check.get("issues", [])[:3]),
                  "detail": detail}
        if invalidate:
            _invalidate(db, review, "package_verification_failed",
                        reason["message"], operator=operator, detail=detail)
        return False, reason

    # 通过: 失效单恢复(依据版本 +1, 旧结论留史)
    if review.status == "INVALIDATED":
        review.status = "OPEN"
        review.scope_version += 1
        review.invalid_reason = None
        review.invalidated_at = None
        _add_event(db, review.id, "review.reverified", operator,
                   reason=(f"固定依据重新校验通过, 恢复复核(依据版本 "
                           f"{review.scope_version}): 旧结论保留为历史, "
                           "请基于新版本重新逐事件签署"),
                   detail={"scope_version": review.scope_version})
    return True, None


# ---------- 创建复核单 ----------

def create_review(db: Session, *, operator: str, session_id: str,
                  export_id: str | None = None) -> EvidenceReview:
    """从已完成且校验通过的查询会话/导出包创建复核单, 固定全部依据。"""
    s = evidence.get_session(db, session_id)
    evidence._require_creator(s, operator)
    if s.status == "BROKEN":
        raise ReviewStateError(
            f"证据会话 {session_id} 已断链(BROKEN), 不能创建复核单: "
            f"{(s.broken_reason or {}).get('code')}",
            code="session_broken")
    if s.status != "CLOSED":
        raise ReviewStateError(
            f"证据会话 {session_id} 当前状态 {s.status}, 必须翻到固定边界"
            "(CLOSED)后才能创建复核单(确保复核范围是完整查询结果)",
            code="session_not_closed")
    # 选定导出: 显式指定优先, 否则取该会话最近的 COMPLETED 包
    if export_id:
        e = evidence.get_export(db, export_id)
        if e.session_id != session_id:
            raise ReviewStateError(
                f"导出 {export_id} 不属于会话 {session_id}, 不能作为复核依据",
                code="export_session_mismatch")
    else:
        e = (db.query(EvidenceExport)
             .filter(EvidenceExport.session_id == session_id,
                     EvidenceExport.status == "COMPLETED")
             .order_by(EvidenceExport.created_at.desc(),
                       EvidenceExport.id.desc()).first())
        if e is None:
            raise ReviewStateError(
                f"会话 {session_id} 没有 COMPLETED 的证据导出包, "
                "请先完成分段导出并通过摘要校验后再创建复核单",
                code="export_not_completed")
    if e.status != "COMPLETED" or not e.manifest_hash:
        raise ReviewStateError(
            f"证据导出 {e.id} 当前状态 {e.status}, 尚无完成的 manifest_hash, "
            "不能作为复核依据", code="export_not_completed")
    with _lock:
        # 同一(会话,导出)只允许一个未归档复核单; 已归档后禁止再建(签署归档的终结性)
        existing = (db.query(EvidenceReview)
                    .filter(EvidenceReview.session_id == session_id,
                            EvidenceReview.export_id == e.id)
                    .order_by(EvidenceReview.created_at.desc()).first())
        if existing is not None:
            if existing.status == "ARCHIVED":
                raise ReviewStateError(
                    f"会话 {session_id}/导出 {e.id} 的复核单 {existing.id} "
                    "已归档(终态), 不能再次创建复核单",
                    code="review_already_archived")
            return existing
        rows = _scoped_rows(db, s)
        # 创建即做一次完整依据校验(包必须当前可回读且摘要一致)
        fp = scope_fingerprint(rows)
        rid = "ER" + uuid.uuid4().hex[:10]
        review = EvidenceReview(
            id=rid, session_id=session_id, export_id=e.id, plan_id=s.plan_id,
            status="OPEN", fixed_filters=session_to_filters(s),
            fixed_upper_global_seq=s.upper_global_seq,
            start_global_seq=s.start_global_seq,
            first_global_seq=(rows[0].global_seq if rows else None),
            last_global_seq=(rows[-1].global_seq if rows else None),
            total_events=len(rows), scope_fingerprint=fp,
            export_manifest_hash=e.manifest_hash,
            export_content_digest=e.content_digest,
            version=0, scope_version=1, created_by=operator)
        db.add(review)
        db.flush()
        ok, reason = reverify_basis(db, review, operator=operator, invalidate=False)
        if not ok:
            db.rollback()
            raise ReviewStateError(
                f"创建复核单前依据校验未通过[{reason['code']}]: {reason['message']}",
                code=reason["code"])
        _add_event(db, rid, "review.create", operator,
                   reason=(f"创建证据复核单: 会话 {session_id}, 导出 {e.id}, "
                           f"固定范围 global_seq "
                           f"{s.start_global_seq}..{s.upper_global_seq}, "
                           f"{len(rows)} 个事件, "
                           f"manifest_hash={e.manifest_hash[:16]}…"),
                   detail={"session_id": session_id, "export_id": e.id,
                           "total_events": len(rows),
                           "scope_fingerprint": fp,
                           "manifest_hash": e.manifest_hash})
        db.flush()
        return review


def session_to_filters(s: EvidenceSession) -> dict:
    return {
        "plan_id": s.plan_id,
        "start_global_seq": s.start_global_seq,
        "start_ts": s.start_ts.isoformat() if s.start_ts else None,
        "end_ts": s.end_ts.isoformat() if s.end_ts else None,
        "event_types": list(s.event_types or []),
        "sources": list(s.sources or []),
        "streams": list(s.include_streams or []),
        "upper_global_seq": s.upper_global_seq,
    }


# ---------- 逐事件提交结论 ----------

def _current_conclusions(db: Session, review: EvidenceReview
                         ) -> list[EvidenceReviewConclusion]:
    return (db.query(EvidenceReviewConclusion)
            .filter(EvidenceReviewConclusion.review_id == review.id,
                    EvidenceReviewConclusion.scope_version
                    == review.scope_version).all())


def _stats(db: Session, review: EvidenceReview) -> dict:
    """当前依据版本的签署统计与待处理数量。"""
    rows = _current_conclusions(db, review)
    by_event: dict[int, dict] = {}
    verdict_counts = {"CONFIRMED": 0, "QUESTIONED": 0, "EXCLUDED": 0}
    for r in rows:
        slot = by_event.setdefault(r.event_global_seq, {"operators": set()})
        slot["operators"].add(r.operator)
        verdict_counts[r.verdict] = verdict_counts.get(r.verdict, 0) + 1
    signed_events = sum(1 for v in by_event.values() if len(v["operators"]) >= 2)
    distinct_operators = {r.operator for r in rows}
    pending = max(0, review.total_events - signed_events)
    return {
        "total_events": review.total_events,
        "signed_events": signed_events,
        "pending_count": pending,
        "conclusion_count": len(rows),
        "distinct_operators": sorted(distinct_operators),
        "operator_count": len(distinct_operators),
        "verdict_counts": verdict_counts,
    }


def submit_conclusion(db: Session, review_id: str, *, operator: str,
                      global_seq: int, verdict: str, note: str | None,
                      expected_version: int | None) -> dict:
    """对固定范围内的单个事件提交一名操作者的签署结论(If-Match 版本栅栏)。"""
    if verdict not in ("CONFIRMED", "QUESTIONED", "EXCLUDED"):
        raise ReviewStateError(
            f"复核结论必须是 CONFIRMED/QUESTIONED/EXCLUDED 之一(收到 {verdict})",
            code="invalid_verdict")
    if verdict in ("QUESTIONED", "EXCLUDED") and not (note and note.strip()):
        raise ReviewStateError(
            f"结论 {verdict} 必须填写问题/排除说明", code="note_required")
    if expected_version is None:
        raise ReviewVersionConflict(
            "提交结论必须携带 If-Match 版本(复核单当前 version)",
            expected=-1, supplied=None, code="if_match_required")
    with _lock:
        review = get_review(db, review_id)
        if review.status == "ARCHIVED":
            raise ReviewStateError(
                f"复核单 {review_id} 已归档(终态), 禁止修改结论",
                code="review_archived")
        if review.status == "INVALIDATED":
            raise ReviewStateError(
                f"复核单 {review_id} 已失效(INVALIDATED: "
                f"{(review.invalid_reason or {}).get('code')}), 请在依据修复并"
                "重新校验通过后基于新版本重新提交结论",
                code="review_invalidated")
        if int(expected_version) != review.version:
            raise ReviewVersionConflict(
                f"If-Match 版本过期或重复写入: 提交基于版本 {expected_version}, "
                f"复核单当前版本 {review.version}(结论已被其他写入推进)",
                expected=review.version, supplied=int(expected_version))
        # 固定依据再校验(任何变化先失效再拒绝)
        ok, reason = reverify_basis(db, review, operator=operator)
        db.commit()  # 失效状态必须持久化
        if not ok:
            raise ReviewStateError(
                f"固定依据校验未通过[{reason['code']}], 复核单已转 INVALIDATED: "
                + reason["message"], code=reason["code"])
        review = get_review(db, review_id)
        s = evidence.get_session(db, review.session_id)
        # 引用必须落在固定会话范围内(流/global_seq 边界 + 创建时的时间/类型/来源过滤)
        scope_rows = _scoped_rows(db, s, review)
        ev = next((r for r in scope_rows if r.global_seq == global_seq), None)
        if ev is None:
            raise ReviewStateError(
                f"事件 global_seq={global_seq} 不在复核单固定会话范围内"
                f"(流 {s.include_streams}, "
                f"global_seq {review.start_global_seq}.."
                f"{review.fixed_upper_global_seq} 且须满足创建时的时间/类型/来源过滤), "
                "复核结论只能引用固定会话中的事件",
                code="event_out_of_scope",
                extra={"global_seq": global_seq,
                       "upper_global_seq": review.fixed_upper_global_seq,
                       "streams": s.include_streams})
        # 同一依据版本同操作者同事件只能签一次(去重, 不覆盖)
        dup = (db.query(EvidenceReviewConclusion)
               .filter(EvidenceReviewConclusion.review_id == review.id,
                       EvidenceReviewConclusion.scope_version
                       == review.scope_version,
                       EvidenceReviewConclusion.event_global_seq == global_seq,
                       EvidenceReviewConclusion.operator == operator).first())
        if dup is not None:
            raise ReviewStateError(
                f"操作者 {operator} 已在依据版本 {review.scope_version} 对事件 "
                f"global_seq={global_seq} 签署 {dup.verdict}, 不能覆盖或重复签署",
                code="duplicate_signature",
                extra={"global_seq": global_seq,
                       "existing_verdict": dup.verdict})
        db.add(EvidenceReviewConclusion(
            review_id=review.id, scope_version=review.scope_version,
            event_global_seq=ev.global_seq, event_stream_key=ev.stream_key,
            event_stream_seq=ev.stream_seq, event_type=ev.event_type,
            verdict=verdict, note=(note[:2000] if note else None),
            operator=operator, review_version=review.version))
        review.version += 1
        _add_event(db, review.id, "conclusion.submit", operator,
                   reason=f"事件 global_seq={global_seq} 结论 {verdict}",
                   detail={"global_seq": global_seq, "verdict": verdict,
                           "scope_version": review.scope_version,
                           "review_version": review.version,
                           "has_note": bool(note)})
        db.flush()  # autoflush=False: 先落当前结论, 统计查询才能读到
        stats = _stats(db, review)
        db.commit()
        return {"review_id": review.id, "version": review.version,
                "scope_version": review.scope_version,
                "global_seq": global_seq, "verdict": verdict,
                "operator": operator, "stats": stats}


def reverify(db: Session, review_id: str, *, operator: str) -> dict:
    """显式重新校验固定依据; 通过则失效单恢复 OPEN(依据版本+1, 旧结论留史)。"""
    with _lock:
        review = get_review(db, review_id)
        if review.status == "ARCHIVED":
            raise ReviewStateError(
                f"复核单 {review_id} 已归档(终态), 无需重新校验",
                code="review_archived")
        ok, reason = reverify_basis(db, review, operator=operator)
        stats = _stats(db, review)
        db.commit()
        return {"review_id": review.id, "valid": ok,
                "status": review.status,
                "scope_version": review.scope_version,
                "invalid_reason": review.invalid_reason,
                "stats": stats}


# ---------- 归档 ----------

def _build_signed_summary(review: EvidenceReview,
                          rows: list[AuditEvent],
                          conclusions: list[EvidenceReviewConclusion]) -> dict:
    current = [c for c in conclusions if c.scope_version == review.scope_version]
    verdict_counts = {"CONFIRMED": 0, "QUESTIONED": 0, "EXCLUDED": 0}
    per_event: dict[int, list[dict]] = {}
    for c in current:
        verdict_counts[c.verdict] = verdict_counts.get(c.verdict, 0) + 1
        per_event.setdefault(c.event_global_seq, []).append({
            "global_seq": c.event_global_seq,
            "stream_key": c.event_stream_key,
            "stream_seq": c.event_stream_seq,
            "event_type": c.event_type,
            "verdict": c.verdict, "note": c.note,
            "operator": c.operator,
            "submitted_review_version": c.review_version,
            "signed_at": c.created_at.isoformat() if c.created_at else None,
        })
    events_signed = [
        {"global_seq": gs,
         "signers": sorted(v["operator"] for v in items),
         "verdicts": sorted(v["verdict"] for v in items)}
        for gs, items in sorted(per_event.items())]
    return {
        "review_id": review.id,
        "session_id": review.session_id,
        "export_id": review.export_id,
        "plan_id": review.plan_id,
        "digest_algorithm": evidence.DIGEST_ALGORITHM,
        "scope_version": review.scope_version,
        "fixed_filters": review.fixed_filters,
        "event_range": {
            "start_global_seq": review.start_global_seq,
            "upper_global_seq": review.fixed_upper_global_seq,
            "first_global_seq": review.first_global_seq,
            "last_global_seq": review.last_global_seq,
            "total_events": review.total_events,
            "scope_fingerprint": review.scope_fingerprint,
        },
        "manifest_hash": review.export_manifest_hash,
        "content_digest": review.export_content_digest,
        "operators": sorted({c.operator for c in current}),
        "operator_count": len({c.operator for c in current}),
        "conclusion_stats": {
            "total": len(current),
            "events_signed": len(events_signed),
            "by_verdict": verdict_counts,
        },
        "events": events_signed,
        "created_by": review.created_by,
        "created_at": review.created_at.isoformat() if review.created_at else None,
    }


def archive_review(db: Session, review_id: str, *, operator: str) -> dict:
    """两名不同操作者完成全部事件签署后归档, 生成不可变签署摘要。"""
    with _lock:
        review = get_review(db, review_id)
        if review.status == "ARCHIVED":
            return {"ok": True, "already_in_state": True,
                    "review_id": review.id,
                    "signature_hash": review.signature_hash,
                    "detail": "复核单已归档, 重复归档无副作用"}
        if review.status == "INVALIDATED":
            raise ReviewStateError(
                f"复核单 {review_id} 已失效(INVALIDATED: "
                f"{(review.invalid_reason or {}).get('code')}), 禁止归档: "
                "请修复依据并重新校验后重新签署",
                code="review_invalidated")
        # 归档前最后一次全量依据校验
        ok, reason = reverify_basis(db, review, operator=operator)
        db.commit()
        if not ok:
            raise ReviewStateError(
                f"归档前依据校验未通过[{reason['code']}], 复核单已转 INVALIDATED, "
                "禁止归档: " + reason["message"], code=reason["code"])
        review = get_review(db, review_id)
        s = evidence.get_session(db, review.session_id)
        rows = _scoped_rows(db, s, review)
        if review.total_events == 0:
            raise ReviewStateError(
                "固定范围内没有事件, 不存在可签署归档的证据",
                code="empty_scope")
        allc = (db.query(EvidenceReviewConclusion)
                .filter(EvidenceReviewConclusion.review_id == review.id).all())
        current = [c for c in allc if c.scope_version == review.scope_version]
        distinct = {c.operator for c in current}
        if len(distinct) < 2:
            raise ReviewStateError(
                f"归档要求两名不同操作者分别签署, 当前只有 {sorted(distinct)}",
                code="two_operators_required")
        per_event: dict[int, set[str]] = {}
        for c in current:
            per_event.setdefault(c.event_global_seq, set()).add(c.operator)
        missing = [r.global_seq for r in rows
                   if len(per_event.get(r.global_seq, set())) < 2]
        if missing:
            preview = missing[:10]
            raise ReviewStateError(
                f"尚有 {len(missing)} 个事件未得到两名不同操作者签署, 不能归档"
                f"(首批 global_seq={preview})",
                code="signatures_incomplete",
                extra={"pending_global_seqs": preview,
                       "pending_count": len(missing)})
        summary = _build_signed_summary(review, rows, allc)
        summary["archived_by"] = operator
        summary["archived_at"] = _now().isoformat()
        signature_hash = evidence._hash_obj(
            {k: v for k, v in summary.items() if k != "signature_hash"})
        summary["signature_hash"] = signature_hash
        review.status = "ARCHIVED"
        review.signed_summary = summary
        review.signature_hash = signature_hash
        review.archived_by = operator
        review.archived_at = _now()
        _add_event(db, review.id, "review.archive", operator,
                   reason=(f"复核单归档: {review.total_events} 个事件, "
                           f"{len(distinct)} 名操作者, "
                           f"signature_hash={signature_hash[:16]}…, "
                           f"manifest_hash={review.export_manifest_hash[:16]}…"),
                   detail={"signature_hash": signature_hash,
                           "operators": sorted(distinct),
                           "total_events": review.total_events,
                           "manifest_hash": review.export_manifest_hash})
        db.commit()
        return {"ok": True, "review_id": review.id,
                "status": "ARCHIVED", "signature_hash": signature_hash,
                "signed_summary": summary}


# ---------- 视图 ----------

def _conclusion_to_dict(c: EvidenceReviewConclusion) -> dict:
    return {
        "id": c.id, "scope_version": c.scope_version,
        "global_seq": c.event_global_seq,
        "event": {"global_seq": c.event_global_seq,
                  "stream_key": c.event_stream_key,
                  "stream_seq": c.event_stream_seq,
                  "event_type": c.event_type},
        "verdict": c.verdict, "note": c.note, "operator": c.operator,
        "review_version": c.review_version,
        "current": None,  # 由调用方按 scope_version 标注
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


def review_to_dict(db: Session, r: EvidenceReview, *,
                   with_events_detail: bool = False,
                   pending_only: bool = False,
                   limit: int | None = None,
                   offset: int = 0,
                   with_history: bool = True) -> dict:
    stats = _stats(db, r)
    current_rows = (db.query(EvidenceReviewConclusion)
                    .filter(EvidenceReviewConclusion.review_id == r.id,
                            EvidenceReviewConclusion.scope_version
                            == r.scope_version)
                    .order_by(EvidenceReviewConclusion.event_global_seq,
                              EvidenceReviewConclusion.id).all())
    per_event: dict[int, list[dict]] = {}
    for c in current_rows:
        d = _conclusion_to_dict(c)
        d["current"] = True
        per_event.setdefault(c.event_global_seq, []).append(d)
    s = evidence.get_session(db, r.session_id)
    scope_rows = _scoped_rows(db, s, r)
    scope_by_seq = {row.global_seq: row for row in scope_rows}
    items = []
    for row in scope_rows:
        signers = per_event.get(row.global_seq, [])
        if pending_only and len({x["operator"] for x in signers}) >= 2:
            continue
        items.append({
            "event": evidence.event_view(row),
            "signatures": signers,
            "signature_count": len(signers),
            "operators": sorted(x["operator"] for x in signers),
            "pending": len({x["operator"] for x in signers}) < 2,
        })
    total_scope = len(items)
    if limit is not None:
        items = items[offset:offset + max(1, min(500, int(limit)))]
    out = {
        "review_id": r.id,
        "session_id": r.session_id,
        "export_id": r.export_id,
        "plan_id": r.plan_id,
        "status": r.status,
        "version": r.version,
        "scope_version": r.scope_version,
        "created_by": r.created_by,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        "fixed": {
            "filters": r.fixed_filters,
            "start_global_seq": r.start_global_seq,
            "upper_global_seq": r.fixed_upper_global_seq,
            "first_global_seq": r.first_global_seq,
            "last_global_seq": r.last_global_seq,
            "total_events": r.total_events,
            "scope_fingerprint": r.scope_fingerprint,
            "export_manifest_hash": r.export_manifest_hash,
            "export_content_digest": r.export_content_digest,
        },
        "stats": stats,
        "pending_count": stats["pending_count"],
        "invalid_reason": r.invalid_reason,
        "invalidated_at": r.invalidated_at.isoformat()
        if r.invalidated_at else None,
        "archived_by": r.archived_by,
        "archived_at": r.archived_at.isoformat() if r.archived_at else None,
        "signature_hash": r.signature_hash,
        "items": items,
        "item_offset": offset,
        "item_limit": limit,
        "item_total": total_scope,
    }
    if with_events_detail:
        out["events"] = [{
            "id": x.id, "ts": x.ts.isoformat() if x.ts else None,
            "event": x.event, "operator": x.operator,
            "reason": x.reason, "detail": x.detail,
        } for x in sorted(r.events, key=lambda x: x.id)]
    if with_history:
        history = (db.query(EvidenceReviewConclusion)
                   .filter(EvidenceReviewConclusion.review_id == r.id,
                           EvidenceReviewConclusion.scope_version
                           != r.scope_version)
                   .order_by(EvidenceReviewConclusion.id).all())
        out["history"] = [_conclusion_to_dict_hist(c) for c in history]
    if r.signed_summary:
        out["signed_summary"] = r.signed_summary
    return out


def _conclusion_to_dict_hist(c: EvidenceReviewConclusion) -> dict:
    d = _conclusion_to_dict(c)
    d["current"] = False
    return d


def list_reviews(db: Session, *, session_id: str | None = None,
                 export_id: str | None = None, plan_id: str | None = None,
                 status_filter: str | None = None,
                 limit: int = 50) -> list[EvidenceReview]:
    q = db.query(EvidenceReview)
    if session_id:
        q = q.filter(EvidenceReview.session_id == session_id)
    if export_id:
        q = q.filter(EvidenceReview.export_id == export_id)
    if plan_id:
        q = q.filter(EvidenceReview.plan_id == plan_id)
    if status_filter:
        q = q.filter(EvidenceReview.status == status_filter)
    return (q.order_by(EvidenceReview.created_at.desc(),
                       EvidenceReview.id.desc())
            .limit(min(max(1, limit), 200)).all())


# ---------- 幂等框架(evidence.review.* 命名空间) ----------

def run_review_action(db: Session, *, action: str, operator: str,
                      idempotency_key: str, payload: dict, fn,
                      review_id: str | None = None) -> tuple[dict, bool]:
    req_hash = evidence._hash_obj(
        {"action": f"{REVIEW_NAMESPACE}.{action}",
         "review_id": review_id, "payload": payload})
    from .models import IdempotencyKey
    from sqlalchemy.exc import IntegrityError
    existing = db.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise ReviewStateError("幂等键被不同请求复用",
                                   code="idempotency_reuse")
        return existing.response_json, True
    result = fn(db)
    if review_id is not None:
        fresh = get_review(db, review_id)
        result.setdefault("review_id", review_id)
        result["status"] = fresh.status
    db.add(IdempotencyKey(key=idempotency_key,
                          action=f"{REVIEW_NAMESPACE}.{action}",
                          request_hash=req_hash, response_json=result))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = db.get(IdempotencyKey, idempotency_key)
        if winner is not None and winner.request_hash == req_hash:
            return winner.response_json, True
        raise
    return result, False
