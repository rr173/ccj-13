"""审计事件回放与补偿: 统一不可变事件流 + 时间点快照校验 + 补偿动作执行/撤销。

设计要点
========
1. 统一事件流(audit_events): 批次写入/生命周期、规则变更、扫描状态变化、
   质量暂停、计划推进与取消全部投影到同一只追加事件表。每个事件带:
   - global_seq: 全库严格递增顺序号(插入时加锁分配);
   - stream_key/stream_seq: 计划流以 plan_id 为 key, 批次事件以 "batch:<id>"
     为 key, 备注流为 "global"; 单流 seq 从 1 连续递增(连续性校验依据);
   - stream_hash: 单流哈希链(prev 链), 篡改/丢失事件即断链;
   - dedupe_key: 同一逻辑事件重投影(重复请求/幂等重放/扇出)返回已有行。
   任何路径都不允许 UPDATE/DELETE 事件行; 补偿只追加 COMP_* 事件。
2. 快照(audit_snapshots): 对 COMPLETED/CANCELED 计划在目标时间点固化事件链与
   批次基线, 校验事件连续性、哈希链、乱序、规则版本与批次版本以及目标时点批次
   状态与当前一致性。任一不满足 -> REJECTED 并逐条给出机器可读原因; VALID
   快照带 TTL, 过期后执行补偿被拒绝(撤销不受限)。
3. 补偿(compensation_tasks/actions): 从快照基线推导待补偿动作
   (record_backfill / record_cleanup / batch_unfreeze), 支持预览、幂等执行、
   逐动作失败重试与逆序整体撤销。执行前实时复核质量门禁, 门禁不通过则该动作
   FAILED(gate_blocked); CANCELED 计划的快照只可预览, 执行一律拒绝。
   动作逐个独立事务提交, 进度/失败原因/撤销镜像全部落库, 重启后续跑。
4. 并发: worker 认领受 COMPENSATION_MAX_CONCURRENCY 限制(Postgres 咨询锁串行),
   同一快照同时至多一个非终态补偿任务(部分唯一索引 + 锁兜底),
   动作带确定性幂等键, 并发执行不会重复写入。
"""
import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import plans, quality, service
from .models import (
    AUDIT_EVENT_TYPES, AUDIT_SNAPSHOT_STATUSES, COMP_TASK_ACTIVE_STATUSES,
    COMP_TASK_STATUSES, IdempotencyKey, MigrationBatch, MigrationPlan, PlanStep,
    QualityRuleVersion, QualityScan, RecordNew, RecordOld, AuditEvent,
    AuditSnapshot, AuditSnapshotBatch, AuditSnapshotEvent, CompensationAction,
    CompensationTask, CompensationTaskEvent,
)

GLOBAL_STREAM = "global"
BATCH_STREAM_PREFIX = "batch:"
# 计划终结事件动作名(plans.plan_audit 写入) -> 判定 target_at 是否到达可回放点
_PLAN_TERMINAL_ACTIONS = ("plan.cancel", "plan.complete")
# 扫描 QualityEvent 事件名 -> 是否属于扫描状态变化 + 新状态
_SCAN_STATUS_EVENTS = {
    "scan.create": "QUEUED", "scan.queue": "QUEUED", "scan.claim": "RUNNING",
    "scan.pause": "PAUSED", "scan.resume": "QUEUED",
    "scan.complete": "COMPLETED", "scan.failed": "FAILED",
    "scan.cancel": "CANCELED", "rescan.enqueue": "QUEUED",
    "rescan.cancel_with_plan": "CANCELED",
}


class AuditReplayNotFound(Exception):
    """快照 / 补偿任务 / 动作不存在 -> 404。"""


class AuditReplayStateError(Exception):
    """当前状态不允许该操作(快照 REJECTED/过期/计划已取消/状态机冲突) -> 409。"""


class AuditEventRejected(Exception):
    """显式补录事件被拒(乱序/类型非法) -> 409。"""


# SQLite 写事务天然串行, 但显式补录/扇出在不同会话间仍需一个进程内栅栏;
# 多副本部署以 Postgres 咨询锁为准。
_append_lock = threading.RLock()


def now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------- 并发 / TTL 配置 ----------

def max_concurrency() -> int:
    try:
        return max(1, int(os.getenv("COMPENSATION_MAX_CONCURRENCY", "2")))
    except ValueError:
        return 2


def snapshot_ttl_seconds() -> int:
    try:
        return max(60, int(os.getenv("AUDIT_SNAPSHOT_TTL_SECONDS",
                                     str(24 * 3600))))
    except ValueError:
        return 24 * 3600


def max_out_of_order_seconds() -> int:
    from .models import AUDIT_MAX_OUT_OF_ORDER_SECONDS
    try:
        return max(0, int(os.getenv("AUDIT_MAX_OUT_OF_ORDER_SECONDS",
                                    str(AUDIT_MAX_OUT_OF_ORDER_SECONDS))))
    except ValueError:
        return AUDIT_MAX_OUT_OF_ORDER_SECONDS


# ======================================================================
# ---------- 统一事件流: 追加 / 哈希链 / 去重 / 查询 ----------
# ======================================================================

def _hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload or {}, sort_keys=True, default=str,
                   ensure_ascii=False).encode()
    ).hexdigest()


def _chain_hash(stream_seq: int, event_type: str, correlation_id: str,
                payload: dict, event_ts: datetime, prev_hash: str | None,
                operator: str | None = None) -> str:
    basis = {
        "stream_seq": stream_seq, "event_type": event_type,
        "correlation_id": correlation_id, "payload": payload or {},
        "event_ts": event_ts.isoformat(), "operator": operator or "",
        "prev": prev_hash or "",
    }
    return _hash_payload(basis)


def _lock_append(session: Session) -> None:
    """串行化 global_seq/stream_seq 分配与哈希链读取-修改-写入。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260922)"))


def _append_one(session: Session, *, stream_key: str, event_type: str,
                correlation_id: str, dedupe_key: str, payload: dict,
                operator: str, source: str, event_ts: datetime,
                plan_id: str | None = None, batch_id: str | None = None,
                scan_id: str | None = None, step_seq: int | None = None,
                out_of_order_ok: bool = False) -> AuditEvent:
    """向单条流追加一个事件(同事务, 不提交)。

    seq 分配: global_seq = max+1; stream_seq 按该 stream_key 现有最大 seq +1。
    乱序: event_ts 早于流上一事件超过容忍阈值时拒绝(内部钩子传业务当前时间,
    显式补录受此约束)。dedupe_key 已存在(同流) -> 返回已有行(幂等重投影)。
    """
    if event_type not in AUDIT_EVENT_TYPES:
        raise ValueError(f"未知审计事件类型: {event_type}")
    # 去重(同流): 重复请求/扇出重投影直接返回已有行, 不产生第二个顺序号
    existing = (session.query(AuditEvent)
                .filter(AuditEvent.stream_key == stream_key,
                        AuditEvent.dedupe_key == dedupe_key).first())
    if existing is not None:
        return existing
    prev = (session.query(AuditEvent)
            .filter(AuditEvent.stream_key == stream_key)
            .order_by(AuditEvent.stream_seq.desc()).first())
    max_global = session.query(func.max(AuditEvent.global_seq)).scalar() or 0
    if (not out_of_order_ok and prev is not None and prev.event_ts is not None
            and event_ts < prev.event_ts
            - timedelta(seconds=max_out_of_order_seconds())):
        raise AuditEventRejected(
            f"乱序事件: event_ts={event_ts.isoformat()} 早于流上一事件 "
            f"{prev.event_ts.isoformat()} 超过 {max_out_of_order_seconds()}s 容忍阈值")
    stream_seq = (prev.stream_seq + 1) if prev is not None else 1
    stream_hash = _chain_hash(stream_seq, event_type, correlation_id, payload,
                              event_ts, prev.stream_hash if prev else None,
                              operator)
    row = AuditEvent(
        global_seq=max_global + 1, stream_key=stream_key, stream_seq=stream_seq,
        event_type=event_type, plan_id=plan_id, batch_id=batch_id,
        scan_id=scan_id, step_seq=step_seq, correlation_id=correlation_id,
        prev_global_seq=prev.global_seq if prev else None,
        prev_stream_hash=prev.stream_hash if prev else None,
        stream_hash=stream_hash, payload=payload or {}, operator=operator,
        source=source, dedupe_key=dedupe_key, event_ts=event_ts)
    session.add(row)
    session.flush()
    return row


def append_event(session: Session, *, event_type: str, operator: str,
                 payload: dict | None = None, plan_ids: list[str] | None = None,
                 batch_id: str | None = None, scan_id: str | None = None,
                 step_seq: int | None = None, source: str = "internal",
                 event_ts: datetime | None = None,
                 dedupe_token: str | None = None,
                 correlation_id: str | None = None,
                 out_of_order_ok: bool = False) -> list[AuditEvent]:
    """事件投影入口: 把一个逻辑事件投影到相关计划流, 以及批次/全局流。

    - plan_ids 非空: 对每个计划流各投影一份(共享 correlation_id, 各自连续 seq);
    - batch_id 非空且不属于这些计划: 另投影一份到 "batch:<batch_id>" 流;
    - 既无计划也无批次: 投影到 global 流(EXTERNAL_NOTE)。
    全程在调用方事务/锁内, 不自行 commit。返回实际写入(或幂等命中)的事件行。
    """
    event_ts = event_ts or now_utc_naive()
    payload = payload or {}
    correlation_id = correlation_id or uuid.uuid4().hex
    dedupe_token = dedupe_token or uuid.uuid4().hex
    with _append_lock:
        _lock_append(session)
        rows: list[AuditEvent] = []
        plan_ids = [p for p in dict.fromkeys(plan_ids or []) if p]
        for pid in plan_ids:
            rows.append(_append_one(
                session, stream_key=pid, event_type=event_type,
                correlation_id=correlation_id,
                dedupe_key=f"{dedupe_token}:plan:{pid}", payload=payload,
                operator=operator, source=source, event_ts=event_ts,
                plan_id=pid, batch_id=batch_id, scan_id=scan_id,
                step_seq=step_seq, out_of_order_ok=out_of_order_ok))
        if batch_id is not None:
            rows.append(_append_one(
                session, stream_key=f"{BATCH_STREAM_PREFIX}{batch_id}",
                event_type=event_type, correlation_id=correlation_id,
                dedupe_key=f"{dedupe_token}:batch:{batch_id}", payload=payload,
                operator=operator, source=source, event_ts=event_ts,
                plan_id=None, batch_id=batch_id, scan_id=scan_id,
                step_seq=step_seq, out_of_order_ok=out_of_order_ok))
        if not plan_ids and batch_id is None:
            rows.append(_append_one(
                session, stream_key=GLOBAL_STREAM, event_type=event_type,
                correlation_id=correlation_id,
                dedupe_key=f"{dedupe_token}:global", payload=payload,
                operator=operator, source=source, event_ts=event_ts,
                out_of_order_ok=out_of_order_ok))
        return rows


def plans_referencing_batch(session: Session, batch_id: str,
                            *, active_only: bool = False) -> list[str]:
    """引用某批次的计划 id(按 PlanStep)。active_only 时只返回未终结计划。"""
    q = (session.query(MigrationPlan.id)
         .join(PlanStep, PlanStep.plan_id == MigrationPlan.id)
         .filter(PlanStep.batch_id == batch_id))
    if active_only:
        q = q.filter(MigrationPlan.status.notin_(plans.TERMINAL_PLAN_STATUSES))
    return [r[0] for r in q.order_by(MigrationPlan.id).distinct().all()]


def append_external_note(session: Session, *, operator: str, content: str,
                         event_ts: datetime | None = None,
                         plan_id: str | None = None,
                         batch_id: str | None = None,
                         dedupe_key: str | None = None) -> dict:
    """运维显式补录备注(唯一允许直接写入的事件类型)。

    显式补录受乱序阈值约束; dedupe_key 由调用方提供(幂等重放返回首次结果)。
    """
    token = dedupe_key or f"note:{uuid.uuid4().hex}"
    rows = append_event(
        session, event_type="EXTERNAL_NOTE", operator=operator, source="api",
        event_ts=event_ts, plan_ids=[plan_id] if plan_id else None,
        batch_id=batch_id,
        payload={"content": content[:2000]},
        dedupe_token=token, correlation_id=f"note-{token}")
    first = rows[0]
    return {"global_seq": first.global_seq, "stream_seq": first.stream_seq,
            "stream_key": first.stream_key,
            "event_ts": first.event_ts.isoformat() if first.event_ts else None}


def query_events(session: Session, *, plan_id: str | None = None,
                 batch_id: str | None = None,
                 start_ts: datetime | None = None,
                 end_ts: datetime | None = None,
                 event_type: str | None = None,
                 limit: int = 50,
                 after_global_seq: int | None = None) -> dict:
    """分页查询事件链(keyset 分页, 按 global_seq 升序)。

    - plan_id: 该计划流(stream_key=plan_id)的全部投影事件, 连续 seq;
    - batch_id: 批次流 + 任何带该 batch_id 的计划流事件(按 correlation 去重,
      一个逻辑事件只返回一次, 另附 other_plan_ids 表示它还投影到哪些计划);
    - 时间范围闭区间 [start_ts, end_ts]; limit/after_global_seq 翻页。
    """
    limit = max(1, min(limit, 500))
    q = session.query(AuditEvent)
    if plan_id:
        q = q.filter(AuditEvent.stream_key == plan_id)
    elif batch_id:
        q = q.filter(
            (AuditEvent.batch_id == batch_id)
            | (AuditEvent.stream_key == f"{BATCH_STREAM_PREFIX}{batch_id}"))
    if start_ts:
        q = q.filter(AuditEvent.event_ts >= start_ts)
    if end_ts:
        q = q.filter(AuditEvent.event_ts <= end_ts)
    if event_type:
        q = q.filter(AuditEvent.event_type == event_type)
    if after_global_seq:
        q = q.filter(AuditEvent.global_seq > after_global_seq)
    rows = (q.order_by(AuditEvent.global_seq.asc(), AuditEvent.id.asc())
            .limit(limit + 1).all())
    has_more = len(rows) > limit
    rows = rows[:limit]
    # 批次查询: 同一逻辑事件(共享 correlation_id 且类型/批次相同)在多个计划流
    # 各有投影, 这里按 correlation 折叠成一条, 保留它投影到的全部计划 id。
    items: list[dict] = []
    seen_corr: dict[str, dict] = {}
    for r in rows:
        if batch_id and r.correlation_id in seen_corr and r.batch_id == batch_id:
            agg = seen_corr[r.correlation_id]
            if r.plan_id and r.plan_id not in agg["other_plan_ids"]:
                agg["other_plan_ids"].append(r.plan_id)
            continue
        item = event_to_dict(r)
        item["other_plan_ids"] = []
        if batch_id and r.plan_id:
            item["other_plan_ids"] = [r.plan_id]
        items.append(item)
        if batch_id:
            seen_corr[r.correlation_id] = item
    next_cursor = rows[-1].global_seq if has_more and rows else None
    return {"items": items, "limit": limit, "has_more": has_more,
            "next_after_global_seq": next_cursor}


def event_to_dict(r: AuditEvent) -> dict:
    return {
        "id": r.id, "global_seq": r.global_seq, "stream_key": r.stream_key,
        "stream_seq": r.stream_seq, "event_type": r.event_type,
        "plan_id": r.plan_id, "batch_id": r.batch_id, "scan_id": r.scan_id,
        "step_seq": r.step_seq, "correlation_id": r.correlation_id,
        "prev_global_seq": r.prev_global_seq,
        "prev_stream_hash": r.prev_stream_hash, "stream_hash": r.stream_hash,
        "payload": r.payload, "operator": r.operator, "source": r.source,
        "event_ts": r.event_ts.isoformat() if r.event_ts else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


# ======================================================================
# ---------- 内部事件投影钩子(由 service / plans / quality 调用) ----------
# ======================================================================

# AuditLog.action 前缀 -> 统一事件类型; 未列出的 plan.* 归 PLAN_ADVANCE
_PLAN_ACTION_TYPE = {
    "plan.cancel": "PLAN_CANCEL",
}
_QUALITY_HOLD_ACTIONS = {
    "hold.open", "hold.resume", "hold.cancel", "hold.update",
}
_RULE_VERSION_ACTION = "rules.version_create"


def emit_batch_event(session: Session, batch: MigrationBatch, *, operator: str,
                     action: str, from_phase: str | None, reason: str | None,
                     diffs: list | None) -> None:
    """批次生命周期审计钩子(service.audit 内调用, 同一事务)。

    对当前把该批次纳入步骤的计划流各投影一份, 以及批次流自身。
    载荷固化批次版本(phase/epoch/freeze_version/watermark), 供快照校验。
    token 含 epoch: 每次栅栏推进都是唯一事件; 同一逻辑动作重放由上层
    幂等框架(IdempotencyKey)挡在更早处, 不会走到这里。"""
    type_map = {
        "create": "BATCH_CREATE", "freeze": "BATCH_FREEZE",
        "validate": "BATCH_VALIDATE", "cutover": "BATCH_CUTOVER",
        "recover": "BATCH_RECOVER", "boot": None,
    }
    event_type = type_map.get(action, None)
    if event_type is None:
        return
    plan_ids = plans_referencing_batch(session, batch.id)
    payload = {
        "action": action, "batch_id": batch.id, "biz": batch.biz,
        "id_range": [batch.id_start, batch.id_end],
        "from_phase": from_phase, "phase": batch.phase, "epoch": batch.epoch,
        "freeze_version": batch.freeze_version, "watermark": batch.watermark,
        "active_schema": batch.active_schema,
        "reason": (reason[:500] if reason else None),
        "diff_count": len(diffs or []),
    }
    append_event(session, event_type=event_type, operator=operator,
                 payload=payload, plan_ids=plan_ids, batch_id=batch.id,
                 dedupe_token=f"batch:{action}:{batch.id}:{batch.epoch}:"
                             f"{from_phase or '-'}:{batch.freeze_version or '-'}")


def emit_batch_write(session: Session, *, batch: MigrationBatch,
                     record_id: int, path: str, record: dict,
                     operator: str, changed: bool) -> None:
    """批次范围内记录写入钩子(旧/新写入路径, 业务事务提交后调用)。

    只投影到一个活动(未终结)计划流(同时至多一个), 否则仅批次流 —— 已完成
    计划之后的写入不属于该计划事件流, 保证跨批次/跨计划隔离。"""
    active = plans_referencing_batch(session, batch.id, active_only=True)
    payload = {
        "batch_id": batch.id, "record_id": record_id, "path": path,
        "record": record, "changed": changed, "phase": batch.phase,
        "epoch": batch.epoch,
    }
    append_event(session, event_type="BATCH_WRITE", operator=operator,
                 payload=payload, plan_ids=active[:1], batch_id=batch.id)
    # 每次写入都是独立事件(写入接口自身的幂等由上层保证, 这里不用确定性 token,
    # 否则同 epoch 内对同一记录的多次写入会被错误去重)


def emit_plan_attach(session: Session, plan: MigrationPlan, steps: list,
                     operator: str) -> None:
    """计划创建后: 把每个步骤批次锚定进该计划事件流(BATCH_ATTACH)。

    这是批次版本进入计划流的起点(快照的批次版本链从锚定 epoch 起校验)。"""
    by_id = {s.id: s.seq for s in steps}
    for st in steps:
        batch = session.get(MigrationBatch, st.batch_id)
        if batch is None:
            continue
        append_event(
            session, event_type="BATCH_ATTACH", operator=operator,
            plan_ids=[plan.id], batch_id=batch.id, step_seq=st.seq,
            payload={"batch_id": batch.id, "biz": batch.biz,
                     "id_range": [batch.id_start, batch.id_end],
                     "phase": batch.phase, "epoch": batch.epoch,
                     "freeze_version": batch.freeze_version,
                     "plan_step_id": st.id},
            dedupe_token=f"attach:{plan.id}:{batch.id}")


def emit_plan_event(session: Session, plan: MigrationPlan, *, operator: str,
                    action: str, reason: str | None,
                    step_id: int | None) -> None:
    """计划级审计钩子(plans.plan_audit 内调用, 同一事务)。"""
    event_type = _PLAN_ACTION_TYPE.get(action, "PLAN_ADVANCE")
    step_seq = None
    batch_id = None
    if step_id is not None:
        st = session.get(PlanStep, step_id)
        if st is not None:
            step_seq = st.seq
            batch_id = st.batch_id
    batch = session.get(MigrationBatch, batch_id) if batch_id else None
    append_event(
        session, event_type=event_type, operator=operator, plan_ids=[plan.id],
        batch_id=batch_id, step_seq=step_seq,
        payload={"action": action, "plan_id": plan.id,
                 "plan_status": plan.status, "step_id": step_id,
                 "batch_phase": batch.phase if batch else None,
                 "batch_epoch": batch.epoch if batch else None,
                 "reason": (reason[:500] if reason else None)},
        dedupe_token=f"plan:{action}:{plan.id}:{step_id or '-'}:{plan.status}:"
                     f"{uuid.uuid4().hex[:8]}")


def emit_quality_event(session: Session, *, plan_id: str, event: str,
                       operator: str, scan_id: str | None,
                       reason: str | None, detail: dict | None) -> None:
    """质量模块事件钩子(quality.add_event 内调用, 同一事务)。

    规则版本发布 -> RULE_CHANGED; hold.* -> QUALITY_HOLD;
    扫描状态变化 -> SCAN_STATUS(载荷带新旧状态与规则版本)。"""
    detail = detail or {}
    if event == _RULE_VERSION_ACTION:
        append_event(
            session, event_type="RULE_CHANGED", operator=operator,
            plan_ids=[plan_id], scan_id=scan_id,
            payload={"event": event, "rule_version": detail.get("version"),
                     "rule_count": detail.get("rule_count"),
                     "content_digest": detail.get("content_digest"),
                     "reason": (reason[:500] if reason else None)},
            dedupe_token=f"rule:{plan_id}:{detail.get('version')}")
        return
    if event in _QUALITY_HOLD_ACTIONS:
        append_event(
            session, event_type="QUALITY_HOLD", operator=operator,
            plan_ids=[plan_id], scan_id=scan_id,
            payload={"event": event, "reason": (reason[:500] if reason else None),
                     **{k: v for k, v in detail.items()
                        if k in ("gate_status", "rescan_scan_id",
                                 "paused_at_seq")}},
            dedupe_token=f"hold:{plan_id}:{event}:{uuid.uuid4().hex[:12]}")
        return
    new_status = _SCAN_STATUS_EVENTS.get(event)
    if new_status is None:
        return
    rule_version = None
    if scan_id:
        sc = session.get(QualityScan, scan_id)
        if sc is not None:
            rule_version = sc.rule_version
    append_event(
        session, event_type="SCAN_STATUS", operator=operator,
        plan_ids=[plan_id], scan_id=scan_id,
        payload={"event": event, "new_status": new_status,
                 "rule_version": rule_version or detail.get("rule_version"),
                 "scan_source": detail.get("scan_source"),
                 "reason": (reason[:500] if reason else None),
                 "detail_keys": sorted(detail.keys())},
        dedupe_token=f"scan:{scan_id or plan_id}:{event}:{new_status}:"
                     f"{uuid.uuid4().hex[:8]}")


# ======================================================================
# ---------- 回放快照: 生成 / 校验 / 视图 ----------
# ======================================================================

def _reason(code: str, message: str, **extra) -> dict:
    out = {"code": code, "message": message}
    out.update(extra)
    return out


def get_snapshot(session: Session, snapshot_id: str) -> AuditSnapshot:
    snap = session.get(AuditSnapshot, snapshot_id)
    if snap is None:
        raise AuditReplayNotFound(f"回放快照 {snapshot_id} 不存在")
    return snap


def is_expired(snap: AuditSnapshot, at: datetime | None = None) -> bool:
    if snap.status != "VALID" or snap.expires_at is None:
        return False
    return (at or now_utc_naive()) > snap.expires_at


def _recompute_chain(events: list[AuditEvent]) -> list[int]:
    """重算计划流哈希链, 返回断链位置的 stream_seq 列表。"""
    broken: list[int] = []
    prev_hash = None
    for r in events:
        expect = _chain_hash(r.stream_seq, r.event_type, r.correlation_id,
                             r.payload, r.event_ts, prev_hash, r.operator)
        if expect != r.stream_hash:
            broken.append(r.stream_seq)
        prev_hash = r.stream_hash
    return broken


def _validate_rule_versions(session: Session, plan_id: str,
                            events: list[AuditEvent]) -> tuple[list[dict], list[dict]]:
    """校验规则版本链: RULE_CHANGED 的版本号单调连续、摘要与库中不可变版本一致;
    SCAN_STATUS 引用的规则版本必须在当时已存在。返回 (固化版本链, 拒绝原因)。"""
    reasons: list[dict] = []
    chain: list[dict] = []
    seen_versions: set[int] = set()
    last_version = 0
    ruleset_ids: dict[int, str] = {}
    for r in events:
        if r.event_type != "RULE_CHANGED":
            continue
        ver = (r.payload or {}).get("rule_version")
        digest = (r.payload or {}).get("content_digest")
        if not isinstance(ver, int):
            reasons.append(_reason("rule_version_gap",
                                   f"事件 global_seq={r.global_seq} 的规则版本号非法: {ver!r}"))
            continue
        if ver <= last_version:
            reasons.append(_reason("rule_version_gap",
                                   f"规则版本倒挂/重复: v{ver} 出现在 v{last_version} 之后",
                                   version=ver, last_version=last_version))
        last_version = max(last_version, ver)
        chain.append({"version": ver, "content_digest": digest,
                      "event_global_seq": r.global_seq})
        seen_versions.add(ver)
        # 与库中不可变规则版本核对(版本存在 + 摘要一致)
        rv = (session.query(QualityRuleVersion)
              .join(MigrationPlan, MigrationPlan.id == plan_id)
              .filter(QualityRuleVersion.plan_id == plan_id,
                      QualityRuleVersion.version == ver).first())
        if rv is None:
            reasons.append(_reason("rule_version_gap",
                                   f"事件引用的规则版本 v{ver} 在规则集中不存在",
                                   version=ver))
        else:
            ruleset_ids[ver] = rv.ruleset_id
            if digest and rv.content_digest != digest:
                reasons.append(_reason(
                    "rule_version_gap",
                    f"规则版本 v{ver} 内容摘要不一致: 事件记录 {digest[:12]}…, "
                    f"规则集为 {rv.content_digest[:12]}…", version=ver))
    # 版本号必须连续(1..N), 中间缺失即为版本缺口
    if seen_versions:
        missing = [v for v in range(1, max(seen_versions) + 1)
                   if v not in seen_versions]
        if missing:
            reasons.append(_reason("rule_version_gap",
                                   f"规则版本链存在缺口: 缺少 v{missing}",
                                   missing=missing))
    # SCAN_STATUS 引用的规则版本必须已被 RULE_CHANGED 覆盖(扫描不能基于未知版本)
    for r in events:
        if r.event_type != "SCAN_STATUS":
            continue
        ver = (r.payload or {}).get("rule_version")
        if isinstance(ver, int) and seen_versions and ver not in seen_versions:
            reasons.append(_reason(
                "rule_version_gap",
                f"扫描事件 global_seq={r.global_seq} 引用了事件链中不存在的规则版本 v{ver}",
                version=ver))
    return chain, reasons


def _validate_batch_versions(session: Session, plan_id: str,
                             events: list[AuditEvent],
                             terminal_at: datetime) -> tuple[list[dict], list[dict]]:
    """校验批次版本链与目标时点状态一致性。

    批次版本: 同一批次的 epoch 在计划流内单调不倒挂; 每次生命周期动作
    (FREEZE/VALIDATE/CUTOVER/RECOVER)epoch 必须恰好 +1(缺失增量即版本缺口);
    freeze_version 必须与对应 freeze 事件一致; 目标时点最后一次批次事件的
    阶段/epoch/freeze_version 必须与当前批次行一致(回放可还原)。
    """
    reasons: list[dict] = []
    per_batch: dict[str, dict] = {}
    lifecycle = {"BATCH_FREEZE", "BATCH_VALIDATE", "BATCH_CUTOVER",
                 "BATCH_RECOVER"}
    for r in events:
        bid = r.batch_id
        if not bid:
            continue
        st = per_batch.setdefault(bid, {
            "batch_id": bid, "last_epoch": None, "last_phase": None,
            "last_freeze_version": None, "events": [],
            "attach_epoch": None, "target_phase": None, "target_epoch": None,
            "target_freeze_version": None,
        })
        payload = r.payload or {}
        epoch = payload.get("epoch")
        phase = payload.get("phase")
        fv = payload.get("freeze_version")
        if r.event_type == "BATCH_ATTACH":
            st["attach_epoch"] = epoch
            st["events"].append({"global_seq": r.global_seq,
                                 "type": r.event_type, "epoch": epoch})
            continue
        st["events"].append({"global_seq": r.global_seq, "type": r.event_type,
                             "epoch": epoch, "phase": phase})
        if r.event_type in lifecycle and isinstance(epoch, int):
            if st["last_epoch"] is None:
                base = st["attach_epoch"]
                if base is not None and epoch != base + 1:
                    reasons.append(_reason(
                        "batch_version_gap",
                        f"批次 {bid} 首个动作 {r.event_type} 的 epoch={epoch}, "
                        f"相对锚定 epoch={base} 不是 +1(疑似缺失批次版本事件)",
                        batch_id=bid, epoch=epoch, attach_epoch=base))
            elif epoch != st["last_epoch"] + 1:
                reasons.append(_reason(
                    "batch_version_gap",
                    f"批次 {bid} 在事件 global_seq={r.global_seq}({r.event_type}) "
                    f"epoch={epoch}, 上一事件 epoch={st['last_epoch']}, 版本增量不连续",
                    batch_id=bid, epoch=epoch, last_epoch=st["last_epoch"]))
            if isinstance(st["last_epoch"], int) and epoch < st["last_epoch"]:
                reasons.append(_reason(
                    "batch_version_gap",
                    f"批次 {bid} epoch 倒挂: {epoch} < {st['last_epoch']}",
                    batch_id=bid))
        # freeze_version 只能由 FREEZE 事件置位, 后续事件携带的版本须与其一致
        if r.event_type == "BATCH_FREEZE" and fv:
            st["last_freeze_version"] = fv
        elif fv and st["last_freeze_version"] and fv != st["last_freeze_version"]:
            reasons.append(_reason(
                "batch_version_gap",
                f"批次 {bid} 事件 global_seq={r.global_seq}({r.event_type}) "
                f"freeze_version={fv} 与冻结事件版本 {st['last_freeze_version']} 不一致",
                batch_id=bid))
        if r.event_ts <= terminal_at:
            st["target_phase"] = phase
            st["target_epoch"] = epoch
            st["target_freeze_version"] = fv or st["target_freeze_version"]
        if isinstance(epoch, int):
            st["last_epoch"] = epoch
        if phase:
            st["last_phase"] = phase

    chain: list[dict] = []
    for bid, st in per_batch.items():
        batch = session.get(MigrationBatch, bid)
        chain.append({
            "batch_id": bid, "attach_epoch": st["attach_epoch"],
            "target_phase": st["target_phase"], "target_epoch": st["target_epoch"],
            "target_freeze_version": st["target_freeze_version"],
            "last_event_phase": st["last_phase"], "last_event_epoch": st["last_epoch"],
            "exists_now": batch is not None,
            "phase_now": batch.phase if batch else None,
            "epoch_now": batch.epoch if batch else None,
            "freeze_version_now": batch.freeze_version if batch else None,
        })
        # 目标时点状态必须与当前批次行一致(COMPLETED/CANCELED 后批次不再被
        # 计划驱动; 不一致说明目标时点无法被回放还原)
        if batch is None:
            reasons.append(_reason("batch_state_drift",
                                   f"批次 {bid} 当前已不存在, 无法回放还原",
                                   batch_id=bid))
            continue
        if st["target_phase"] is not None and batch.phase != st["target_phase"]:
            reasons.append(_reason(
                "batch_state_drift",
                f"批次 {bid} 目标时点阶段 {st['target_phase']} 与当前阶段 "
                f"{batch.phase} 不一致", batch_id=bid,
                target_phase=st["target_phase"], phase_now=batch.phase))
        if st["target_epoch"] is not None and batch.epoch != st["target_epoch"]:
            reasons.append(_reason(
                "batch_state_drift",
                f"批次 {bid} 目标时点 epoch={st['target_epoch']} 与当前 "
                f"epoch={batch.epoch} 不一致(批次版本不兼容)", batch_id=bid,
                target_epoch=st["target_epoch"], epoch_now=batch.epoch))
    return chain, reasons


def create_snapshot(session: Session, *, operator: str, plan_id: str,
                    target_at: datetime | None = None,
                    ttl_seconds: int | None = None) -> AuditSnapshot:
    """为已终结计划生成回放快照(VALID 或 REJECTED 都持久化)。

    target_at 省略时取计划终结事件时间; 早于终结时间 -> REJECTED(target_before_end)。
    """
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise AuditReplayNotFound(f"迁移计划 {plan_id} 不存在")
    ttl = ttl_seconds or snapshot_ttl_seconds()
    # 计划流截至当前的全部事件(含 target_at 之后, 用于找终结事件)
    all_events = (session.query(AuditEvent)
                  .filter(AuditEvent.stream_key == plan_id)
                  .order_by(AuditEvent.stream_seq).all())
    terminal_event = None
    for r in reversed(all_events):
        action = (r.payload or {}).get("action")
        if r.event_type in ("PLAN_CANCEL",) or action in _PLAN_TERMINAL_ACTIONS:
            terminal_event = r
            break
    reasons: list[dict] = []
    if plan.status not in ("COMPLETED", "CANCELED"):
        reasons.append(_reason(
            "plan_not_terminal",
            f"计划当前状态为 {plan.status}, 只有 COMPLETED/CANCELED 计划可生成回放快照"))
    if terminal_event is None:
        reasons.append(_reason(
            "plan_not_terminal",
            "计划事件流中找不到终结事件(plan.complete/plan.cancel), 无法确定回放终点"))
    # 默认目标时间 = 终结事件时间
    if target_at is None:
        target_at = terminal_event.event_ts if terminal_event else now_utc_naive()
    if terminal_event is not None and target_at < terminal_event.event_ts:
        reasons.append(_reason(
            "target_before_end",
            f"目标时间 {target_at.isoformat()} 早于计划终结时间 "
            f"{terminal_event.event_ts.isoformat()}, 该时点计划尚未终结",
            target_at=target_at.isoformat(),
            terminal_at=terminal_event.event_ts.isoformat()))

    # 截至目标时间的事件(快照固化内容)
    events = [r for r in all_events if r.event_ts <= target_at]

    # 1) 连续性: stream_seq 必须从 1 开始且无缺口; global_seq 也不应有洞
    seqs = sorted(r.stream_seq for r in events)
    if not seqs:
        reasons.append(_reason("stream_gap", "目标时间之前计划事件流为空, 无可回放内容"))
    else:
        expected = list(range(1, len(seqs) + 1))
        if seqs != expected:
            missing = sorted(set(expected) - set(seqs))
            reasons.append(_reason(
                "stream_gap",
                f"计划事件流顺序号不连续, 缺失 stream_seq={missing[:20]}",
                missing=missing[:50]))
        gseqs = sorted(r.global_seq for r in events)
        gmissing = [g for g in range(gseqs[0], gseqs[-1] + 1)
                    if g not in set(gseqs)]
        # global 洞只有当落在本计划流相邻事件之间才算本流缺口(其他流插入是正常的),
        # 因此 global 洞不直接判失败; 仅 stream_seq 缺口判 stream_gap。
        _ = gmissing

    # 2) 哈希链: 重算必须一致
    broken = _recompute_chain(events)
    if broken:
        reasons.append(_reason(
            "chain_broken",
            f"计划事件流哈希链校验失败, 断链位置 stream_seq={broken[:20]}",
            broken_at=broken[:50]))

    # 3) 乱序: 按 stream_seq 遍历时 event_ts 不得倒流超过容忍阈值
    oo_skew = max_out_of_order_seconds()
    prev_ts = None
    for r in events:
        if prev_ts is not None and r.event_ts < prev_ts - timedelta(seconds=oo_skew):
            reasons.append(_reason(
                "out_of_order",
                f"乱序事件: stream_seq={r.stream_seq} 时间 {r.event_ts.isoformat()} "
                f"早于前一事件 {prev_ts.isoformat()} 超过 {oo_skew}s",
                stream_seq=r.stream_seq))
            break
        if prev_ts is None or r.event_ts > prev_ts:
            prev_ts = r.event_ts

    # 4) 规则版本链
    rule_chain, rule_reasons = _validate_rule_versions(session, plan_id, events)
    reasons.extend(rule_reasons)

    # 5) 批次版本链 + 目标时点状态一致性
    batch_chain, batch_reasons = _validate_batch_versions(
        session, plan_id, events, target_at)
    reasons.extend(batch_reasons)

    status = "VALID" if not reasons else "REJECTED"
    snap_id = "AS" + uuid.uuid4().hex[:10]
    snap = AuditSnapshot(
        id=snap_id, plan_id=plan_id, plan_name=plan.name,
        plan_status_at_create=plan.status, target_at=target_at,
        last_stream_seq=events[-1].stream_seq if events else 0,
        last_global_seq=events[-1].global_seq if events else 0,
        last_stream_hash=events[-1].stream_hash if events else None,
        status=status, reasons=reasons, rule_versions=rule_chain,
        batch_versions=batch_chain, event_count=len(events),
        ttl_seconds=ttl,
        expires_at=(now_utc_naive() + timedelta(seconds=ttl)
                    if status == "VALID" else None),
        created_by=operator)
    session.add(snap)
    session.flush()

    # 逐行固化事件(不可变副本)
    for r in events:
        session.add(AuditSnapshotEvent(
            snapshot_id=snap_id, audit_event_id=r.id, global_seq=r.global_seq,
            stream_seq=r.stream_seq, event_type=r.event_type, batch_id=r.batch_id,
            scan_id=r.scan_id, correlation_id=r.correlation_id, payload=r.payload,
            operator=r.operator, event_ts=r.event_ts, stream_hash=r.stream_hash))

    # 逐批次固化目标时点基线(VALID 才有补偿意义, 但 REJECTED 也固化便于排障)
    if terminal_event is not None:
        steps = plans.steps_of(session, plan_id)
        for st_step in steps:
            batch = session.get(MigrationBatch, st_step.batch_id)
            if batch is None:
                continue
            old_rows = (session.query(RecordOld)
                        .filter(service._in_range(RecordOld.id, batch))
                        .order_by(RecordOld.id).all())
            old_records = [service.old_to_dict(x) for x in old_rows]
            chain_entry = next((c for c in batch_chain
                                if c["batch_id"] == batch.id), {})
            session.add(AuditSnapshotBatch(
                snapshot_id=snap_id, seq=st_step.seq, plan_step_id=st_step.id,
                batch_id=batch.id, biz=batch.biz,
                id_start=batch.id_start, id_end=batch.id_end,
                expected_phase=chain_entry.get("target_phase") or batch.phase,
                expected_epoch=chain_entry.get("target_epoch")
                if chain_entry.get("target_epoch") is not None else batch.epoch,
                expected_freeze_version=chain_entry.get("target_freeze_version"),
                expected_schema=batch.active_schema,
                expected_old_records=old_records,
                expected_old_count=len(old_records)))
    session.flush()
    return snap


# ======================================================================
# ---------- 补偿动作推导(预览) ----------
# ======================================================================

def derive_actions(session: Session, snap: AuditSnapshot) -> list[dict]:
    """根据快照基线推导待补偿动作(纯计算, 不落库)。

    - record_backfill: 基线旧表记录按转换规则应在新表存在; 缺失/不一致 -> 回填修正;
    - record_cleanup:  当前新表在范围内多出基线没有的记录 -> 清理;
    - batch_unfreeze:  目标批次未走到预期终态(CANCELED 计划中仍 FROZEN/...)。
    每个动作带确定性 action_key 与预览说明; gate 状态实时附带(执行前还会复核)。
    """
    actions: list[dict] = []
    seq = 0
    for sb in sorted(snap.batches, key=lambda b: b.seq):
        batch = session.get(MigrationBatch, sb.batch_id)
        # 记录级: 以目标时点批次预期阶段为准。
        # - 已完成计划(DONE): 以旧表基线核对新表 -> 回填/清理;
        # - 已取消计划: 批次未切换, 新表内容不是有效产出(可能是半迁移数据),
        #   不做记录级"补偿"(执行本就拒绝), 只给批次级解冻建议。
        if snap.plan_status_at_create != "CANCELED":
            # 记录级: 以快照基线(目标时点旧表)为预期来源
            baseline = sb.expected_old_records or []
            baseline_ids = {r["id"] for r in baseline}
            for old in baseline:
                expected = service.transform(_dict_to_old(old))
                cur = session.get(RecordNew, old["id"])
                if cur is None:
                    seq += 1
                    actions.append(_preview_action(
                        snap, seq, "record_backfill", sb.batch_id, old["id"],
                        expected, "新表缺失记录, 按目标时点基线回填",
                        current=None))
                else:
                    cur_d = service.new_to_dict(cur)
                    if cur_d != expected:
                        seq += 1
                        actions.append(_preview_action(
                            snap, seq, "record_backfill", sb.batch_id, old["id"],
                            expected, "新表记录与目标时点基线不一致, 按基线修正",
                            current=cur_d))
            # 范围内多余的新表记录(基线没有)
            extra_q = (session.query(RecordNew)
                       .filter(RecordNew.id >= sb.id_start, RecordNew.id <= sb.id_end))
            if baseline_ids:
                extra_q = extra_q.filter(RecordNew.id.notin_(sorted(baseline_ids)))
            for cur in extra_q.order_by(RecordNew.id):
                seq += 1
                actions.append(_preview_action(
                    snap, seq, "record_cleanup", sb.batch_id, cur.id, None,
                    "当前新表存在目标时点基线之外的多余记录, 建议清理",
                    current=service.new_to_dict(cur)))
        # 批次级: 取消计划中未走完生命周期的批次建议解冻
        if snap.plan_status_at_create == "CANCELED" and batch is not None:
            if batch.phase not in ("NORMAL", "DONE"):
                seq += 1
                actions.append(_preview_action(
                    snap, seq, "batch_unfreeze", sb.batch_id, None,
                    {"target_phase": "NORMAL", "phase_now": batch.phase,
                     "epoch_now": batch.epoch},
                    f"计划已取消, 批次仍处于 {batch.phase}, 建议恢复到可写 NORMAL",
                    current={"phase": batch.phase, "epoch": batch.epoch}))
    # 实时门禁状态(预览提示; 执行前逐动作复核)
    gate = quality.evaluate_gate(session, snap.plan_id)
    for a in actions:
        a["gate_status"] = gate.get("status")
        a["gate_passed"] = quality.gate_allows_execution(gate)
        # CANCELED 计划: 动作可预览但执行会被拒绝
        a["execution_allowed"] = snap.plan_status_at_create != "CANCELED"
    return actions


def _dict_to_old(d: dict):
    return RecordOld(id=d["id"], name=d.get("name", ""),
                     email=d.get("email", ""), tags_csv=d.get("tags_csv", ""))


def _preview_action(snap: AuditSnapshot, seq: int, action_type: str,
                    batch_id: str, record_id: int | None, expected,
                    description: str, current) -> dict:
    key = action_key(snap.id, action_type, batch_id, record_id)
    return {
        "seq": seq, "action_type": action_type, "batch_id": batch_id,
        "record_id": record_id, "action_key": key,
        "expected": expected, "current": current, "description": description,
        "snapshot_id": snap.id,
    }


def action_key(snapshot_id: str, action_type: str, batch_id: str,
               record_id: int | None) -> str:
    return f"{snapshot_id}:{action_type}:{batch_id}:{record_id or '-'}"


# ======================================================================
# ---------- 补偿任务: 创建 / 执行 / 重试 / 撤销 ----------
# ======================================================================

def get_task(session: Session, task_id: str) -> CompensationTask:
    task = session.get(CompensationTask, task_id)
    if task is None:
        raise AuditReplayNotFound(f"补偿任务 {task_id} 不存在")
    return task


def _add_task_event(session: Session, task: CompensationTask, event: str,
                    operator: str, *, action_seq: int | None = None,
                    reason: str | None = None, detail: dict | None = None) -> None:
    session.add(CompensationTaskEvent(
        task_id=task.id, action_seq=action_seq, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail))


def _lock_scheduling(session: Session) -> None:
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260923)"))


def _lock_creation(session: Session) -> None:
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260924)"))


def create_task(session: Session, *, operator: str, snapshot_id: str) -> CompensationTask:
    """基于 VALID 快照创建补偿任务(推导动作落 PENDING 并排队)。

    同一快照同时至多一个非终态任务: 重复创建幂等返回已有任务。
    REJECTED/不存在快照拒绝; CANCELED 计划允许建任务(动作可预览), 但执行拒绝。
    """
    snap = get_snapshot(session, snapshot_id)
    with _append_lock:
        _lock_creation(session)
        existing = (session.query(CompensationTask)
                    .filter(CompensationTask.snapshot_id == snapshot_id,
                            CompensationTask.status.in_(COMP_TASK_ACTIVE_STATUSES))
                    .order_by(CompensationTask.created_at.desc()).first())
        if existing is not None:
            return existing
        if snap.status != "VALID":
            raise AuditReplayStateError(
                f"快照 {snapshot_id} 校验状态为 REJECTED, 不允许生成补偿任务: "
                + "; ".join(r["message"] for r in (snap.reasons or [])[:3]))
        previews = derive_actions(session, snap)
        task = CompensationTask(
            id="CT" + uuid.uuid4().hex[:10], snapshot_id=snapshot_id,
            plan_id=snap.plan_id, plan_status=snap.plan_status_at_create,
            status="QUEUED", total_actions=len(previews),
            created_by=operator, updated_by=operator)
        session.add(task)
        session.flush()
        for p in previews:
            session.add(CompensationAction(
                task_id=task.id, snapshot_id=snapshot_id, seq=p["seq"],
                action_key=p["action_key"], action_type=p["action_type"],
                batch_id=p["batch_id"], record_id=p["record_id"],
                expected=p["expected"], status="PENDING"))
        _add_task_event(
            session, task, "create", operator,
            reason=(f"基于快照 {snapshot_id}(计划 {snap.plan_id}, "
                    f"{snap.plan_status_at_create})创建补偿任务, "
                    f"{len(previews)} 个待补偿动作"))
        session.flush()
        return task


def _append_comp_event(session: Session, task: CompensationTask, action,
                       event_type: str, operator: str, detail: dict,
                       dedupe_token: str) -> int:
    """补偿效果只追加到统一事件流(不改原事件), 返回新事件 global_seq。"""
    action_key = action.action_key if action is not None else None
    rows = append_event(
        session, event_type=event_type, operator=operator, plan_ids=[task.plan_id],
        batch_id=action.batch_id if action else None, source="system",
        payload={"task_id": task.id,
                 "action_seq": action.seq if action else None,
                 "action_type": action.action_type if action else None,
                 "action_key": action_key, **detail},
        dedupe_token=dedupe_token)
    return rows[0].global_seq


def _evaluate_action_gate(session: Session, task: CompensationTask) -> tuple[bool, dict]:
    """执行前实时复核质量门禁: PASS/NOT_CONFIGURED 放行; 否则拒绝(不越过门禁)。"""
    gate = quality.evaluate_gate(session, task.plan_id)
    allowed = quality.gate_allows_execution(gate)
    return allowed, gate


def _execute_one_action(session: Session, task_id: str, action_seq: int,
                        operator: str) -> None:
    """执行单个动作(独立事务边界: 本函数结束时恰好提交一次; 异常回滚并把
    FAILED 与失败原因在随后的独立事务中持久化, 已成功动作不受影响)。"""
    task = get_task(session, task_id)
    action = next(a for a in task.actions if a.seq == action_seq)
    action.attempts += 1
    action.last_error = None
    action.status = "PENDING"
    session.flush()
    _add_task_event(session, task, "action.start", operator,
                    action_seq=action.seq,
                    reason=f"开始执行 {action.action_type}"
                           + (f" 记录 {action.record_id}" if action.record_id else ""))
    try:
        # 计划已取消: 补偿一律拒绝
        plan = session.get(MigrationPlan, task.plan_id)
        if plan is not None and plan.status == "CANCELED":
            raise _ActionBlocked(
                "PLAN_CANCELED",
                "计划已取消, 补偿动作被拒绝(plan_canceled)")
        # 质量门禁: 不通过则该动作失败(补偿不能越过门禁)
        allowed, gate = _evaluate_action_gate(session, task)
        action.gate_status = gate.get("status")
        if not allowed:
            raise _ActionBlocked(
                gate.get("status") or "GATE_BLOCKED",
                "质量门禁未通过(" + str(gate.get("status")) + "): "
                + "; ".join(gate.get("reasons") or [])[:400])
        detail = _apply_action(session, task, action, operator)
        action.status = "SUCCESS"
        action.result = detail
        action.executed_by = operator
        action.executed_at = now_utc_naive()
        gseq = _append_comp_event(
            session, task, action, "COMP_EXECUTED", operator, detail,
            dedupe_token=f"comp:exec:{action.action_key}")
        action.executed_event_global_seq = gseq
        _add_task_event(session, task, "action.success", operator,
                        action_seq=action.seq, detail=detail)
        _refresh_task_counters(session, task)
        _finish_execution(session, task)
        session.commit()
    except _ActionBlocked as e:
        _fail_action(session, task_id, action_seq, operator,
                     gate_status=e.code, reason=str(e),
                     event="action.gate_blocked"
                     if e.code != "PLAN_CANCELED" else "action.rejected")
    except Exception as e:  # noqa: BLE001 - 逐动作失败隔离, 原因持久化可重试
        _fail_action(session, task_id, action_seq, operator,
                     gate_status=action.gate_status,
                     reason=f"执行异常: {e}"[:500], event="action.failed")


class _ActionBlocked(Exception):
    """动作被门禁/计划状态显式阻止(非系统异常, 直接记 FAILED 原因)。"""

    def __init__(self, code: str, reason: str):
        super().__init__(reason)
        self.code = code
        self.reason = reason


def _fail_action(session: Session, task_id: str, action_seq: int,
                 operator: str, *, gate_status: str | None, reason: str,
                 event: str) -> None:
    """把动作持久化为 FAILED(独立事务, 不影响已成功动作), 任务停在 PARTIAL。"""
    session.rollback()
    task = get_task(session, task_id)
    action = next(a for a in task.actions if a.seq == action_seq)
    action.attempts = (action.attempts or 0) + 1  # 成功路径的 +1 随回滚撤销
    action.status = "FAILED"
    action.last_error = reason[:500]
    action.gate_status = gate_status
    _add_task_event(session, task, event, operator,
                    action_seq=action.seq, reason=reason[:500],
                    detail={"gate_status": gate_status})
    # 失败也只追加统一事件流事件(不改原事件); 每次失败尝试都是独立 COMP_FAILED
    try:
        _append_comp_event(
            session, task, action, "COMP_FAILED", operator,
            {"gate_status": gate_status, "error": reason[:400],
             "attempt": action.attempts},
            dedupe_token=f"comp:fail:{action.action_key}:{action.attempts}:"
                         f"{uuid.uuid4().hex[:6]}")
    except Exception:  # noqa: BLE001 - 失败事件投影异常不影响 FAILED 状态落库
        pass
    _refresh_task_counters(session, task)
    # 仍有未执行动作时保持 RUNNING(后续动作继续跑); 否则停在 PARTIAL 等运维重试
    task.status = ("RUNNING" if any(a.status == "PENDING" for a in task.actions)
                   else "PARTIAL")
    task.last_error = f"动作 seq={action.seq} 失败: {reason}"[:500]
    session.commit()


def _apply_action(session: Session, task: CompensationTask,
                  action: CompensationAction, operator: str) -> dict:
    """真正落地补偿副作用, 返回结果摘要; 同时捕获 before_image 供撤销。"""
    if action.action_type == "record_backfill":
        cur = session.get(RecordNew, action.record_id)
        action.before_image = (None if cur is None
                               else service.new_to_dict(cur))
        expected = action.expected or {}
        if cur is None:
            session.add(RecordNew(
                id=action.record_id, name=expected.get("name", ""),
                email=expected.get("email", ""), tags=expected.get("tags", []),
                schema_version=expected.get("schema_version", 2)))
            op = "inserted"
        else:
            cur.name = expected.get("name", cur.name)
            cur.email = expected.get("email", cur.email)
            cur.tags = expected.get("tags", [])
            cur.schema_version = expected.get("schema_version", 2)
            op = "updated"
        return {"op": op, "record_id": action.record_id,
                "batch_id": action.batch_id, "expected": expected}

    if action.action_type == "record_cleanup":
        cur = session.get(RecordNew, action.record_id)
        action.before_image = (None if cur is None
                               else service.new_to_dict(cur))
        if cur is None:
            return {"op": "noop", "record_id": action.record_id,
                    "reason": "记录已不存在, 清理幂等无副作用"}
        session.delete(cur)
        return {"op": "deleted", "record_id": action.record_id,
                "batch_id": action.batch_id, "removed": action.before_image}

    if action.action_type == "batch_unfreeze":
        batch = service.get_batch(session, action.batch_id)
        action.before_image = {
            "phase": batch.phase, "epoch": batch.epoch,
            "freeze_version": batch.freeze_version,
            "watermark": batch.watermark, "active_schema": batch.active_schema,
        }
        if batch.phase == "NORMAL":
            return {"op": "noop", "batch_id": batch.id, "reason": "批次已是 NORMAL"}
        if batch.phase == "DONE":
            raise AuditReplayStateError("批次已 DONE, 不允许解冻")
        # 复用既有恢复: 清理半迁移数据并回 NORMAL(走批次栅栏, 落业务审计)
        res = service.do_recover(session, batch, operator,
                                 f"补偿任务 {task.id} 撤销计划取消后遗留冻结状态")
        return {"op": "recovered", "batch_id": batch.id, "detail": res.get("detail")}

    raise AuditReplayStateError(f"未知补偿动作类型 {action.action_type}")


def _refresh_task_counters(session: Session, task: CompensationTask) -> None:
    acts = task.actions
    task.total_actions = len(acts)
    task.success_actions = sum(1 for a in acts if a.status == "SUCCESS")
    task.failed_actions = sum(1 for a in acts if a.status == "FAILED")
    task.undone_actions = sum(1 for a in acts if a.status == "UNDONE")


def run_execution_tick(session: Session, task_id: str,
                       *, include_failed: bool = False) -> bool:
    """认领(QUEUED->RUNNING)并执行下一个 PENDING 动作; 无动作时收口任务。

    返回是否执行了动作。单个动作 = 单个事务边界(见 _execute_one_action 内部
    恰好提交一次): 部分失败不回滚已成功动作。worker 正常执行只取 PENDING ——
    FAILED 动作必须由运维显式 retry_action, 避免门禁未恢复时每 tick 空转。"""
    task = _lock_task(session, task_id)
    if task.status not in ("QUEUED", "RUNNING", "PARTIAL"):
        session.rollback()
        return False
    if task.status == "QUEUED":
        task.status = "RUNNING"
        task.started_at = task.started_at or now_utc_naive()
        _add_task_event(session, task, "claim", "system",
                        reason="获得并发额度, 开始执行补偿")
    wanted = ("PENDING", "FAILED") if include_failed else ("PENDING",)
    action = next((a for a in sorted(task.actions, key=lambda x: x.seq)
                   if a.status in wanted), None)
    if action is None:
        _finish_execution(session, task)
        session.commit()
        return False
    task.current_action_seq = action.seq
    task.updated_by = task.created_by
    action_seq = action.seq
    operator = task.created_by
    # 单动作事务: 认领/claim 事件随该动作一起提交(内部提交后 identity map
    # 可能持有过期状态, 执行函数内部按 id 重新读取)
    session.expire_all()
    _execute_one_action(session, task_id, action_seq, operator)
    return True


def _finish_execution(session: Session, task: CompensationTask) -> None:
    """根据动作状态收口任务: 还有 PENDING -> RUNNING; 否则有 FAILED -> PARTIAL;
    全部 SUCCESS -> COMPLETED。"""
    task.current_action_seq = None
    has_pending = any(a.status == "PENDING" for a in task.actions)
    failed = [a for a in task.actions if a.status == "FAILED"]
    if has_pending:
        task.status = "RUNNING"
        return
    _refresh_task_counters(session, task)
    if failed:
        task.status = "PARTIAL"
        task.last_error = f"{len(failed)} 个动作失败, 可逐动作重试"
        _add_task_event(session, task, "partial", task.updated_by or "system",
                        reason=task.last_error)
    else:
        task.status = "COMPLETED"
        task.finished_at = now_utc_naive()
        task.last_error = None
        _add_task_event(session, task, "complete", task.updated_by or "system",
                        reason=f"全部 {task.total_actions} 个补偿动作执行成功")


def execute_all(session: Session, task_id: str, operator: str) -> CompensationTask:
    """同步执行所有 PENDING 动作(测试/运维确定性驱动): 循环 tick 直到无 PENDING。

    FAILED 动作不在自动循环内重试(必须显式 retry_action); 遇 FAILED 即收口为
    PARTIAL, 后续 PENDING 仍会继续执行。"""
    task = get_task(session, task_id)
    if task.status in ("UNDO_RUNNING", "UNDO_PARTIAL", "UNDONE"):
        raise AuditReplayStateError(f"补偿任务 {task_id} 处于 {task.status}, 不能再执行")
    # 快照过期: 执行拒绝(撤销不受此限)
    snap = get_snapshot(session, task.snapshot_id)
    if is_expired(snap):
        raise AuditReplayStateError(
            f"快照 {snap.id} 已过期(有效期至 {snap.expires_at.isoformat()}Z), "
            "请重新生成快照后再执行补偿")
    guard = 0
    # 无待补偿动作的任务也要正确收口(QUEUED -> COMPLETED)
    if not any(a.status == "PENDING" for a in get_task(session, task_id).actions):
        run_execution_tick(session, task_id)
    while True:
        task = get_task(session, task_id)
        if not any(a.status == "PENDING" for a in task.actions):
            break
        ran = run_execution_tick(session, task_id)
        if not ran:
            break
        guard += 1
        if guard > 10000:
            raise AuditReplayStateError("补偿执行步数异常超限")
    return get_task(session, task_id)


def retry_action(session: Session, task_id: str, action_seq: int,
                 operator: str) -> CompensationAction:
    """逐动作失败重试: 仅 FAILED 动作可重试。

    幂等: 成功动作再次请求直接返回; 副作用本身按确定性 action_key 去重
    (COMP_EXECUTED 事件同键不重复追加)。重试成功且无未决动作 -> COMPLETED。"""
    task = get_task(session, task_id)
    snap = get_snapshot(session, task.snapshot_id)
    if is_expired(snap):
        raise AuditReplayStateError(
            f"快照 {snap.id} 已过期, 不能重试执行, 请重新生成快照")
    action = next((a for a in task.actions if a.seq == action_seq), None)
    if action is None:
        raise AuditReplayNotFound(
            f"补偿任务 {task_id} 没有 seq={action_seq} 的动作")
    if action.status == "SUCCESS":
        return action
    if action.status == "UNDONE":
        raise AuditReplayStateError("动作已撤销, 不能重试(请重新生成快照)")
    if action.status != "FAILED":
        raise AuditReplayStateError(
            f"动作 seq={action_seq} 当前状态 {action.status}, 仅 FAILED 可重试")
    _execute_one_action(session, task_id, action_seq, operator)
    session.expire_all()
    return next(a for a in get_task(session, task_id).actions
                if a.seq == action_seq)


def _lock_task(session: Session, task_id: str) -> CompensationTask:
    """Postgres 行锁串行化同一任务的 worker/手动并发; SQLite 写锁串行。"""
    if session.bind.dialect.name != "sqlite":
        locked = session.get(CompensationTask, task_id, with_for_update=True)
        if locked is not None:
            return locked
    return get_task(session, task_id)


# ---------- 整体撤销(逆序, 用 before_image 恢复) ----------

def undo_all(session: Session, task_id: str, operator: str) -> CompensationTask:
    """整体撤销: 对所有 SUCCESS 动作按 seq 逆序用 before_image 恢复现场。

    撤销也是补偿, 只追加 COMP_UNDONE 事件(关联原 COMP_EXECUTED 事件);
    撤销不要求质量门禁通过(回滚不允许被 TTL/门禁卡死), 但记录当时门禁状态。
    动作逐个独立事务, 部分撤销失败停在 UNDO_PARTIAL, 可继续撤销。"""
    task = get_task(session, task_id)
    if task.status not in ("COMPLETED", "PARTIAL", "UNDO_PARTIAL"):
        raise AuditReplayStateError(
            f"补偿任务 {task_id} 当前状态 {task.status}, 不允许撤销"
            "(仅 COMPLETED/PARTIAL/UNDO_PARTIAL 可撤销)")
    task.status = "UNDO_RUNNING"
    task.updated_by = operator
    _add_task_event(session, task, "undo.start", operator,
                    reason="开始逆序撤销全部已执行补偿动作")
    session.commit()
    # 逐动作独立事务, worker 可在重启后从 UNDO_PARTIAL 续跑
    while run_undo_tick(session, task_id):
        pass
    return get_task(session, task_id)


def run_undo_tick(session: Session, task_id: str) -> bool:
    """撤销推进一个 SUCCESS 动作(逆序); 无动作时收口任务。返回是否处理了动作。"""
    task = _lock_task(session, task_id)
    if task.status not in ("UNDO_RUNNING", "UNDO_PARTIAL"):
        session.rollback()
        return False
    action = next((a for a in sorted(task.actions, key=lambda x: x.seq,
                                     reverse=True)
                   if a.status == "SUCCESS"), None)
    if action is None:
        _finish_undo(session, task)
        session.commit()
        return False
    action_seq = action.seq
    _undo_one_action(session, task_id, action_seq, task.updated_by or "system")
    # 若本动作撤销失败(回到 SUCCESS), 停在 UNDO_PARTIAL 不空转
    fresh = get_task(session, task_id)
    still = next((a for a in fresh.actions if a.seq == action_seq), None)
    if still is not None and still.status == "SUCCESS":
        _finish_undo(session, fresh)
        session.commit()
        return False
    return True


def _finish_undo(session: Session, task: CompensationTask) -> None:
    remaining = [a for a in task.actions if a.status == "SUCCESS"]
    _refresh_task_counters(session, task)
    task.current_action_seq = None
    if remaining:
        task.status = "UNDO_PARTIAL"
        task.last_error = f"{len(remaining)} 个动作撤销失败, 可继续撤销"
        _add_task_event(session, task, "undo.partial", task.updated_by or "system",
                        reason=task.last_error)
    else:
        task.status = "UNDONE"
        task.finished_at = now_utc_naive()
        task.last_error = None
        _add_task_event(session, task, "undone", task.updated_by or "system",
                        reason="全部已执行补偿动作已撤销, 现场已按镜像恢复")


def _undo_one_action(session: Session, task_id: str, action_seq: int,
                     operator: str) -> None:
    """撤销单个动作(独立事务边界, 失败隔离: 失败回到 SUCCESS 并记录原因)。"""
    task = get_task(session, task_id)
    action = next(a for a in task.actions if a.seq == action_seq)
    if action.status == "UNDONE":
        return
    try:
        gate = quality.evaluate_gate(session, task.plan_id)
        gate_status = gate.get("status")
        detail = _revert_action(session, task, action, operator)
        action.status = "UNDONE"
        action.undone_by = operator
        action.undone_at = now_utc_naive()
        action.gate_status = gate_status
        gseq = _append_comp_event(
            session, task, action, "COMP_UNDONE", operator,
            {"reverted": detail, "gate_status": gate_status,
             "executed_event_global_seq": action.executed_event_global_seq},
            dedupe_token=f"comp:undo:{action.action_key}")
        action.undone_event_global_seq = gseq
        _refresh_task_counters(session, task)
        _add_task_event(session, task, "action.undone", operator,
                        action_seq=action.seq, detail=detail)
        session.commit()
    except Exception as e:  # noqa: BLE001
        session.rollback()
        task = get_task(session, task_id)
        action = next(a for a in task.actions if a.seq == action_seq)
        action.status = "SUCCESS"  # 回到可撤销状态, 可继续撤销
        action.last_error = f"撤销异常: {e}"[:500]
        _add_task_event(session, task, "action.undo_failed", operator,
                        action_seq=action.seq, reason=action.last_error)
        session.commit()


def _revert_action(session: Session, task: CompensationTask,
                   action: CompensationAction, operator: str) -> dict:
    """按 before_image 恢复现场。"""
    img = action.before_image
    if action.action_type == "record_backfill":
        cur = session.get(RecordNew, action.record_id)
        if img is None:
            # 执行前不存在 -> 删除补偿插入的行
            if cur is not None:
                session.delete(cur)
            return {"op": "remove_inserted", "record_id": action.record_id}
        # 执行前存在 -> 恢复旧值
        if cur is not None:
            cur.name = img.get("name", cur.name)
            cur.email = img.get("email", cur.email)
            cur.tags = img.get("tags", [])
            cur.schema_version = img.get("schema_version", 2)
        else:
            session.add(RecordNew(
                id=action.record_id, name=img.get("name", ""),
                email=img.get("email", ""), tags=img.get("tags", []),
                schema_version=img.get("schema_version", 2)))
        return {"op": "restored", "record_id": action.record_id, "image": img}

    if action.action_type == "record_cleanup":
        if img is None:
            return {"op": "noop", "record_id": action.record_id,
                    "reason": "执行前即无记录"}
        cur = session.get(RecordNew, action.record_id)
        if cur is None:
            session.add(RecordNew(
                id=action.record_id, name=img.get("name", ""),
                email=img.get("email", ""), tags=img.get("tags", []),
                schema_version=img.get("schema_version", 2)))
        return {"op": "reinserted", "record_id": action.record_id, "image": img}

    if action.action_type == "batch_unfreeze":
        batch = service.get_batch(session, action.batch_id)
        # 恢复到撤销前镜像: 阶段/水位/freeze_version 恢复(不伪造状态,
        # 落一条业务审计说明这是补偿撤销)
        if not img:
            return {"op": "noop", "batch_id": batch.id, "reason": "无撤销镜像"}
        service.bump(session, batch, phase=img["phase"], operator=operator,
                     freeze_version=img.get("freeze_version"),
                     watermark=img.get("watermark"),
                     active_schema=img.get("active_schema", "old"))
        st = service.get_batch(session, batch.id)
        service.audit(session, st, operator=operator, action="recover",
                      from_phase="NORMAL",
                      reason=f"补偿任务 {task.id} 撤销: 批次恢复到补偿前镜像 "
                             f"{img['phase']}(epoch {img['epoch']})")
        return {"op": "phase_restored", "batch_id": batch.id, "image": img}

    raise AuditReplayStateError(f"未知补偿动作类型 {action.action_type}")


# ======================================================================
# ---------- worker 认领 / 重启对账 ----------
# ======================================================================

def claim_due_tasks(session: Session) -> list[str]:
    """按并发额度把 QUEUED 补偿任务置 RUNNING, 返回认领 id(FIFO)。"""
    _lock_scheduling(session)
    try:
        running = (session.query(func.count(CompensationTask.id))
                   .filter(CompensationTask.status.in_(("RUNNING", "UNDO_RUNNING")))
                   .scalar()) or 0
        slots = max_concurrency() - running
        if slots <= 0:
            session.rollback()
            return []
        due = (session.query(CompensationTask)
               .filter(CompensationTask.status == "QUEUED")
               .order_by(CompensationTask.created_at, CompensationTask.id)
               .limit(slots).all())
        claimed: list[str] = []
        at = now_utc_naive()
        for t in due:
            t.status = "RUNNING"
            t.started_at = t.started_at or at
            t.updated_by = "system"
            _add_task_event(session, t, "claim", "system",
                            reason=f"获得并发额度, 开始执行(上限 {max_concurrency()})")
            claimed.append(t.id)
        session.commit()
        return claimed
    except Exception:
        session.rollback()
        raise


def boot_recover_compensation(session: Session) -> None:
    """重启对账: 进程死亡时不可能有动作真正在执行。

    - RUNNING 任务回到 QUEUED(worker 重新认领续跑, PENDING/FAILED 动作进度保留);
    - UNDO_RUNNING 回到 UNDO_PARTIAL(继续撤销, 已 UNDONE 动作保留);
    - 动作的 UNDOING 复位为 SUCCESS(撤销未提交, 可重新撤销)。
    """
    tasks = session.query(CompensationTask).order_by(CompensationTask.id).all()
    for t in tasks:
        dirty = False
        if t.status == "RUNNING":
            t.status = "QUEUED"
            t.current_action_seq = None
            _add_task_event(session, t, "boot.reset", "system",
                            reason="重启恢复: RUNNING 补偿任务不可能跨进程存活, "
                                   "回到排队位置, 动作进度与失败原因保留")
            dirty = True
        elif t.status == "UNDO_RUNNING":
            t.status = "UNDO_PARTIAL"
            t.current_action_seq = None
            _add_task_event(session, t, "boot.reset_undo", "system",
                            reason="重启恢复: 撤销中断, 回到 UNDO_PARTIAL, "
                                   "已撤销动作保留, 可继续撤销")
            dirty = True
        for a in t.actions:
            if a.status == "UNDOING":
                a.status = "SUCCESS"
                a.last_error = "重启恢复: 撤销未完成, 复位为 SUCCESS 可重新撤销"
                dirty = True
        if dirty:
            session.commit()


class CompensationWorker:
    """单实例后台线程: 认领 QUEUED 补偿任务并每个 tick 推进一步。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("COMPENSATION_WORKER_POLL_INTERVAL",
                                                   "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop,
                                        name="compensation-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        from .db import SessionLocal
        db = SessionLocal()
        try:
            claimed = claim_due_tasks(db)
            run_ids = [r[0] for r in (db.query(CompensationTask.id)
                                      .filter(CompensationTask.status.in_(
                                          ("RUNNING", "UNDO_RUNNING")))
                                      .order_by(CompensationTask.id).all())]
        finally:
            db.close()
        ids = list(claimed) + [i for i in run_ids if i not in claimed]
        advanced = 0
        for tid in ids:
            db = SessionLocal()
            try:
                task = db.get(CompensationTask, tid)
                if task.status == "RUNNING" and run_execution_tick(db, tid):
                    advanced += 1
                elif task.status == "UNDO_RUNNING" and run_undo_tick(db, tid):
                    advanced += 1
            except Exception:
                db.rollback()
            finally:
                db.close()
        return advanced

    def _loop(self) -> None:
        while not self._stop.wait(self.poll_interval):
            try:
                self.tick_once()
            except Exception:
                time.sleep(self.poll_interval)


# ======================================================================
# ---------- 幂等框架(comp.* 命名空间) ----------
# ======================================================================

def _hash(payload: dict) -> str:
    return _hash_payload(payload)


def run_comp_action(session: Session, *, action: str, operator: str,
                    idempotency_key: str, payload: dict, fn,
                    task_id: str | None = None,
                    snapshot_id: str | None = None) -> tuple[dict, bool]:
    """与其他模块同构的幂等封装: fn(session, task_or_none) -> dict。"""
    req_hash = _hash({"action": f"comp.{action}", "task_id": task_id,
                      "snapshot_id": snapshot_id, "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise AuditReplayStateError("幂等键被不同请求复用")
        return existing.response_json, True
    task = get_task(session, task_id) if task_id else None
    result = fn(session, task)
    if task_id:
        fresh = get_task(session, task_id)
        result["task_id"] = task_id
        result["status"] = fresh.status
    session.add(IdempotencyKey(key=idempotency_key, action=f"comp.{action}",
                               request_hash=req_hash, response_json=result))
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        winner = session.get(IdempotencyKey, idempotency_key)
        if winner is not None and winner.request_hash == req_hash:
            return winner.response_json, True
        raise
    return result, False


# ======================================================================
# ---------- 视图 ----------
# ======================================================================

def snapshot_to_dict(snap: AuditSnapshot, *, with_events: bool = False,
                     with_actions: bool = False,
                     session: Session | None = None) -> dict:
    out = {
        "snapshot_id": snap.id, "plan_id": snap.plan_id,
        "plan_name": snap.plan_name,
        "plan_status_at_create": snap.plan_status_at_create,
        "target_at": snap.target_at.isoformat() if snap.target_at else None,
        "status": snap.status, "reasons": snap.reasons or [],
        "last_stream_seq": snap.last_stream_seq,
        "last_global_seq": snap.last_global_seq,
        "last_stream_hash": snap.last_stream_hash,
        "event_count": snap.event_count,
        "rule_versions": snap.rule_versions or [],
        "batch_versions": snap.batch_versions or [],
        "ttl_seconds": snap.ttl_seconds,
        "expires_at": snap.expires_at.isoformat() if snap.expires_at else None,
        "expired": is_expired(snap),
        "created_by": snap.created_by,
        "created_at": snap.created_at.isoformat() if snap.created_at else None,
    }
    if with_events:
        out["events"] = [{
            "stream_seq": e.stream_seq, "global_seq": e.global_seq,
            "event_type": e.event_type, "batch_id": e.batch_id,
            "scan_id": e.scan_id, "correlation_id": e.correlation_id,
            "operator": e.operator, "event_ts": e.event_ts.isoformat()
            if e.event_ts else None, "payload": e.payload,
            "stream_hash": e.stream_hash,
        } for e in sorted(snap.events, key=lambda x: x.stream_seq)]
        out["batches"] = [{
            "seq": b.seq, "batch_id": b.batch_id, "biz": b.biz,
            "id_range": [b.id_start, b.id_end],
            "expected_phase": b.expected_phase, "expected_epoch": b.expected_epoch,
            "expected_freeze_version": b.expected_freeze_version,
            "expected_old_count": b.expected_old_count,
        } for b in sorted(snap.batches, key=lambda x: x.seq)]
    if with_actions and session is not None and snap.status == "VALID":
        out["actions_preview"] = derive_actions(session, snap)
    return out


def action_to_dict(a: CompensationAction) -> dict:
    return {
        "seq": a.seq, "action_type": a.action_type, "batch_id": a.batch_id,
        "record_id": a.record_id, "status": a.status, "attempts": a.attempts,
        "expected": a.expected, "before_image": a.before_image, "result": a.result,
        "last_error": a.last_error, "gate_status": a.gate_status,
        "executed_by": a.executed_by,
        "executed_at": a.executed_at.isoformat() if a.executed_at else None,
        "executed_event_global_seq": a.executed_event_global_seq,
        "undone_by": a.undone_by,
        "undone_at": a.undone_at.isoformat() if a.undone_at else None,
        "undone_event_global_seq": a.undone_event_global_seq,
    }


def task_to_dict(task: CompensationTask, *, with_actions: bool = True,
                 with_events: bool = True) -> dict:
    out = {
        "task_id": task.id, "snapshot_id": task.snapshot_id,
        "plan_id": task.plan_id, "plan_status": task.plan_status,
        "status": task.status, "total_actions": task.total_actions,
        "success_actions": task.success_actions, "failed_actions": task.failed_actions,
        "undone_actions": task.undone_actions,
        "current_action_seq": task.current_action_seq,
        "last_error": task.last_error, "failure_reason": task.failure_reason,
        "created_by": task.created_by, "updated_by": task.updated_by,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "finished_at": task.finished_at.isoformat() if task.finished_at else None,
        "created_at": task.created_at.isoformat() if task.created_at else None,
    }
    if with_actions:
        out["actions"] = [action_to_dict(a)
                          for a in sorted(task.actions, key=lambda x: x.seq)]
    if with_events:
        out["events"] = [{
            "id": e.id,
            "ts": e.ts.isoformat() if e.ts else None,
            "action_seq": e.action_seq, "event": e.event,
            "operator": e.operator, "reason": e.reason, "detail": e.detail,
        } for e in task.events]
    return out
