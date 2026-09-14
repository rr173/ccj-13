"""回执审计看板: 在证据分发/回执/争议域之上的只读审计视图。

设计要点
========
1. **固定查询时点(查询即快照)**: POST .../receipt-audit/queries 创建查询时,
   规范化筛选条件(分发包/接收方/状态/时间范围/条目类型), 并在同一把进程锁内
   确定读取边界:
       upper_receipt_ts       = 创建时刻(服务端 UTC)
       upper_dispute_event_id = max(evidence_receipt_dispute_events.id)
   回执只在服务端提交时刻写入(不可经 API 回填), 争议事件自增 id 单调, 因此边界
   内符合筛选的回执与争议事件可一次性物化为**升序条目快照**(feed_json)。之后
   新增的回执/争议事件永远不会插入已开始查询的分页结果; 争议单之后的状态流转
   (指派/结论/关闭)也不改变快照内条目(快照内带状态副本)。包卡片的各项聚合
   (完成率/异常/待处理争议)同样只统计边界内数据(争议状态按边界内事件重放)。
2. **稳定游标 + 严格顺序翻页**: 条目按 (事件时间, 类型序 RECEIPT<DISPUTE_EVENT,
   自增 id) 升序, 下标是确定性位置。游标为 HMAC 签名的不透明串, 内含
   (query_id, 下一条目下标, 页码, 条件指纹)。服务端持久化 pages_delivered 与
   last_cursor:
     - 无游标只能取第一页, 已开始查询再无游标取页 -> 拒绝(防漏页);
     - 游标必须等于本查询上一页签发的游标: 跳页/重复使用一律拒绝;
     - 游标签名损坏/属于其他查询/条件指纹不符 -> 拒绝。
3. **分页与 CSV 导出严格一致**: CSV 直接读取同一查询的 feed_json, 按相同顺序
   输出相同行; 文件 sha256(file_digest) 与快照摘要(feed_digest)随导出持久化。
   空结果导出只含表头, 仍是合法 CSV。同 (查询, 幂等键) 重放返回首次导出。
4. **包汇总(看板卡片)**: 对分发包/接收方范围内的每个包(不受时间窗口/状态/
   类型过滤影响), 以固定边界数据计算完成率(已完成回执分派/总分派)、异常数量
   (PARTIAL/REJECTED 回执及逐事件异常)、待处理争议数(边界内状态非 CLOSED)
   与最近事件(查询快照内该包最近 5 条, 随时间/状态过滤变化)。
5. **操作日志**: 查询创建、翻页(含拒绝)、导出创建与 CSV 下载全部只追加落
   receipt_audit_op_logs(被拒绝的请求也留痕)。
"""
import base64
import csv
import hashlib
import hmac
import io
import json
import os
import uuid
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import distribution, evidence
from .models import (
    EVIDENCE_DISPUTE_STATUSES, EVIDENCE_RECEIPT_TYPES, RECEIPT_AUDIT_KINDS,
    RECEIPT_AUDIT_STATUSES, ReceiptAuditExport, ReceiptAuditOpLog,
    ReceiptAuditPage, ReceiptAuditQuery, EvidenceDistribution,
    EvidenceDistributionAssignment, EvidenceDistributionReceipt,
    EvidenceReceiptDispute, EvidenceReceiptDisputeEvent, EvidenceRecipient,
)

# 状态过滤允许的分组白名单(回执类型/分派状态/争议状态)
ALLOWED_STATUS_FILTERS = tuple(RECEIPT_AUDIT_STATUSES)
# 争议单状态集合(用于区分"状态"过滤落在回执还是争议条目)
_DISPUTE_STATUS = set(EVIDENCE_DISPUTE_STATUSES)
_RECEIPT_TYPE_STATUS = set(EVIDENCE_RECEIPT_TYPES)

# 类型序: 同一事件时刻回执排在争议事件之前(稳定的并列次序)
_KIND_ORDER = {"RECEIPT": 0, "DISPUTE_EVENT": 1}

MAX_PAGE_LIMIT = 500
DEFAULT_PAGE_LIMIT = 50
MAX_EXPORT_ROWS = 100_000
SUMMARY_RECENT_EVENTS = 5

# CSV 列(顺序即文件列序; 与 feed 条目字段一一对应, 保证分页/导出一致)
CSV_COLUMNS = [
    "position", "kind", "event_ts", "package_id", "recipient", "status",
    "receipt_id", "receipt_type", "dispute_id", "dispute_event",
    "assignment_id", "total_required", "confirmed_count", "anomaly_count",
    "rejected_event_count", "operator", "summary",
]


class ReceiptAuditError(Exception):
    """看板查询/游标/导出错误 -> 404/409/422。"""

    def __init__(self, reason: str, code: str = "receipt_audit_conflict",
                 status_code: int = 409, extra: dict | None = None):
        super().__init__(reason)
        self.code = code
        self.status_code = status_code
        self.extra = extra or {}


# 与分发/证据域共用同一把进程内栅栏(串行化游标推进/边界确定)
_lock = distribution._lock


# ---------- 配置 / 基础工具 ----------

def _now() -> datetime:
    return evidence.now_utc_naive()


def store_dir() -> str:
    d = os.getenv("RECEIPT_AUDIT_STORE_DIR",
                  os.path.join(distribution.store_dir(), "receipt-audit"))
    return os.path.abspath(d)


def ensure_store_dir() -> None:
    os.makedirs(store_dir(), exist_ok=True)


def _canonical_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), default=str) + "\n")\
        .encode("utf-8")


def _hash_obj(payload: dict) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _cursor_secret() -> bytes:
    sec = os.getenv("RECEIPT_AUDIT_CURSOR_SECRET")
    if sec:
        return sec.encode("utf-8")
    from .db import DATABASE_URL
    return hashlib.sha256(
        ("receipt-audit-cursor:" + DATABASE_URL).encode()).digest()


def encode_cursor(query_id: str, position: int, page_no: int,
                  filters_hash: str) -> str:
    """不透明游标: query_id.位置.页码.条件指纹短哈希 + HMAC 签名, urlsafe base64。"""
    fp = filters_hash[:16]
    body = f"{query_id}.{position}.{page_no}.{fp}"
    sig = hmac.new(_cursor_secret(), body.encode(),
                   hashlib.sha256).hexdigest()[:16]
    raw = f"{body}.{sig}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(token: str) -> tuple[str, int, int, str] | None:
    try:
        pad = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(token + pad).decode()
        query_id, pos, page_no, fp, sig = raw.rsplit(".", 4)
        body = f"{query_id}.{pos}.{page_no}.{fp}"
        expect = hmac.new(_cursor_secret(), body.encode(),
                          hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(sig, expect):
            return None
        return query_id, int(pos), int(page_no), fp
    except Exception:
        return None


def _new_id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex[:14]


# ---------- 操作日志(只追加, 拒绝也记) ----------

def record_rejection(db: Session, operation: str, *, operator: str,
                     code: str, reason: str,
                     query_id: str | None = None,
                     detail: dict | None = None) -> None:
    """在独立短事务中记录一次被拒绝的操作(随后路由会 rollback 主事务,
    本日志必须先落库)。已在错误路径内自行提交日志时不重复调用。"""
    try:
        _log(db, operation, operator=operator, query_id=query_id, ok=False,
             reason_code=code, detail={"reason": reason, **(detail or {})})
        db.commit()
    except Exception:  # noqa: BLE001 - 日志失败不得掩盖原始拒绝
        db.rollback()


def _log(db: Session, operation: str, *, operator: str,
         query_id: str | None = None, export_id: str | None = None,
         ok: bool = True, reason_code: str | None = None,
         detail: dict | None = None, commit: bool = False) -> ReceiptAuditOpLog:
    row = ReceiptAuditOpLog(
        operation=operation, query_id=query_id, export_id=export_id,
        operator=operator[:128], ok=ok, reason_code=reason_code, detail=detail)
    db.add(row)
    db.flush()
    if commit:
        db.commit()
    return row


def list_op_logs(db: Session, *, query_id: str | None = None,
                 operation: str | None = None,
                 limit: int = 200) -> list[ReceiptAuditOpLog]:
    q = db.query(ReceiptAuditOpLog)
    if query_id:
        q = q.filter(ReceiptAuditOpLog.query_id == query_id)
    if operation:
        q = q.filter(ReceiptAuditOpLog.operation == operation)
    return (q.order_by(ReceiptAuditOpLog.id.desc())
            .limit(min(max(1, limit), 1000)).all())


def op_log_to_dict(r: ReceiptAuditOpLog) -> dict:
    return {
        "id": r.id,
        "ts": r.ts.isoformat() if r.ts else None,
        "operation": r.operation, "query_id": r.query_id,
        "export_id": r.export_id, "operator": r.operator, "ok": r.ok,
        "reason_code": r.reason_code, "detail": r.detail,
    }


# ---------- 固定结果集物化 ----------

def _normalize_filters(*, package_id, recipient, status, kinds,
                       start_ts, end_ts) -> dict:
    """规范化并校验筛选条件, 返回可持久化的条件字典(值为 naive UTC 时间)。"""
    if start_ts and end_ts and end_ts < start_ts:
        raise ReceiptAuditError(
            f"非法时间范围: start_ts({start_ts.isoformat()}) 晚于 "
            f"end_ts({end_ts.isoformat()})",
            code="invalid_time_range", status_code=422,
            extra={"start_ts": start_ts.isoformat(),
                   "end_ts": end_ts.isoformat()})
    statuses = list(status or [])
    bad = [s for s in statuses if s not in ALLOWED_STATUS_FILTERS]
    if bad:
        raise ReceiptAuditError(
            f"状态过滤必须是 {list(ALLOWED_STATUS_FILTERS)} 的子集, 收到 {bad}",
            code="invalid_status", status_code=422, extra={"bad": bad})
    ks = list(kinds or [])
    bad_kinds = [k for k in ks if k not in RECEIPT_AUDIT_KINDS]
    if bad_kinds:
        raise ReceiptAuditError(
            f"条目类型必须是 {list(RECEIPT_AUDIT_KINDS)} 之一, 收到 {bad_kinds}",
            code="invalid_kind", status_code=422, extra={"bad": bad_kinds})
    # 状态过滤按域分组: 回执类状态只匹配回执; 争议类状态只匹配争议事件。
    # 同组内为"或"语义(如 SIGNED,PARTIAL 命中任一), 两组之间也为并集。
    receipt_status = sorted(set(statuses) & _RECEIPT_TYPE_STATUS)
    dispute_status = sorted(set(statuses) & _DISPUTE_STATUS)
    # PENDING/OVERDUE 描述的是"未回执分派", 看板条目流内没有对应行(体现在包
    # 汇总的 pending/overdue), 不参与条目匹配。
    return {
        "package_id": package_id or None,
        "recipient": recipient or None,
        "status": sorted(set(statuses)),
        "receipt_status": receipt_status,
        "dispute_status": dispute_status,
        "kinds": sorted(set(ks)),
        "start_ts": start_ts.isoformat() if start_ts else None,
        "end_ts": end_ts.isoformat() if end_ts else None,
    }


def _filter_receipts(db: Session, f: dict, upper_ts: datetime | None
                     ) -> list[EvidenceDistributionReceipt]:
    q = db.query(EvidenceDistributionReceipt)
    # 固定时间上界: 创建时刻之后提交的回执永不进入本查询快照
    if upper_ts is not None:
        q = q.filter(EvidenceDistributionReceipt.submitted_at <= upper_ts)
    if f["package_id"]:
        q = q.filter(EvidenceDistributionReceipt.distribution_id
                     == f["package_id"])
    if f["recipient"]:
        q = q.filter(EvidenceDistributionReceipt.recipient_id == f["recipient"])
    if f["start_ts"]:
        q = q.filter(EvidenceDistributionReceipt.submitted_at
                     >= datetime.fromisoformat(f["start_ts"]))
    if f["end_ts"]:
        q = q.filter(EvidenceDistributionReceipt.submitted_at
                     <= datetime.fromisoformat(f["end_ts"]))
    kinds = set(f["kinds"])
    if kinds and "RECEIPT" not in kinds:
        return []
    # 状态过滤非空但不含任何回执类状态时, 回执一律不命中
    if f["status"] and not f.get("receipt_status"):
        return []
    wanted = set(f.get("receipt_status") or [])
    if wanted:
        q = q.filter(EvidenceDistributionReceipt.receipt_type.in_(wanted))
    return q.all()


def _filter_dispute_events(db: Session, f: dict, upper_id: int
                           ) -> list[EvidenceReceiptDisputeEvent]:
    q = db.query(EvidenceReceiptDisputeEvent).filter(
        EvidenceReceiptDisputeEvent.id <= upper_id)
    if f["package_id"]:
        q = q.filter(EvidenceReceiptDisputeEvent.dispute_id.in_(
            db.query(EvidenceReceiptDispute.id)
            .filter(EvidenceReceiptDispute.distribution_id
                    == f["package_id"])))
    if f["recipient"]:
        q = q.filter(EvidenceReceiptDisputeEvent.dispute_id.in_(
            db.query(EvidenceReceiptDispute.id)
            .filter(EvidenceReceiptDispute.recipient_id == f["recipient"])))
    if f["start_ts"]:
        q = q.filter(EvidenceReceiptDisputeEvent.ts
                     >= datetime.fromisoformat(f["start_ts"]))
    if f["end_ts"]:
        q = q.filter(EvidenceReceiptDisputeEvent.ts
                     <= datetime.fromisoformat(f["end_ts"]))
    kinds = set(f["kinds"])
    if kinds and "DISPUTE_EVENT" not in kinds:
        return []
    # 状态过滤非空但不含任何争议状态时, 争议事件一律不命中
    if f["status"] and not f.get("dispute_status"):
        return []
    wanted = set(f.get("dispute_status") or [])
    if wanted:
        # 事件反映争议单进入/处于某状态: to_status(进入)/from_status(离开前)
        # 任一命中即可, 例如 filter=RESOLVED 时该争议的 resolve 与 reopen 都可见
        from sqlalchemy import or_
        q = q.filter(or_(EvidenceReceiptDisputeEvent.to_status.in_(wanted),
                         EvidenceReceiptDisputeEvent.from_status.in_(wanted)))
    return q.all()


def _receipt_row(r: EvidenceDistributionReceipt) -> dict:
    summary_parts = []
    if r.anomaly_count:
        summary_parts.append(f"异常 {r.anomaly_count}")
    if r.rejected_event_count:
        summary_parts.append(f"拒认事件 {r.rejected_event_count}")
    if r.note:
        summary_parts.append(r.note[:80])
    return {
        "row_id": f"R{r.id}",
        "kind": "RECEIPT",
        "event_ts": r.submitted_at.isoformat() if r.submitted_at else None,
        # 并列次序: 回执 PK 是字符串(EDR+随机), 用确定性字符串键(全局唯一),
        # 同事件时刻的回执按 id 排序, 跨查询重建顺序稳定
        "_tie": f"R{r.id}",
        "package_id": r.distribution_id,
        "recipient": r.recipient_id,
        "status": r.receipt_type,                     # 快照时刻状态副本
        "receipt_id": r.id,
        "receipt_type": r.receipt_type,
        "assignment_id": r.assignment_id,
        "download_id": r.download_id,
        "redeemed_at": r.redeemed_at.isoformat()
        if r.redeemed_at else None,
        "total_required": r.total_required,
        "confirmed_count": r.confirmed_count,
        "anomaly_count": r.anomaly_count,
        "rejected_event_count": r.rejected_event_count,
        "anomalies": list(r.anomalies or [])[:20],
        "operator": r.submitted_by,
        "note": r.note,
        "summary": "; ".join(summary_parts) or "签收",
    }


def _dispute_event_row(e: EvidenceReceiptDisputeEvent,
                       d: EvidenceReceiptDispute) -> dict:
    # 快照时刻的状态副本: 事件动作发生后的状态(to_status), 打开时为初始状态
    status = e.to_status or e.from_status or d.status
    return {
        "row_id": f"D{e.id}",
        "kind": "DISPUTE_EVENT",
        "event_ts": e.ts.isoformat() if e.ts else None,
        # 并列次序: 争议事件有整数自增 PK, 零填充保证字典序与数值序一致
        "_tie": f"D{e.id:012d}",
        "package_id": d.distribution_id,
        "recipient": d.recipient_id,
        "status": status,
        "receipt_id": d.receipt_id,
        "receipt_type": d.receipt_type,
        "dispute_id": d.id,
        "dispute_event": e.event,
        "assignment_id": d.assignment_id,
        "from_status": e.from_status,
        "to_status": e.to_status,
        "operator": e.operator,
        "note": e.reason,
        "summary": (e.reason or "")[:120],
    }


def _sort_key(row: dict):
    ts = row["event_ts"] or ""
    # (事件时间 ISO 字符串字典序与 UTC 时刻一致, 类型序, 确定性并列键)
    return (ts, _KIND_ORDER[row["kind"]], row["_tie"])


def materialize_feed(db: Session, f: dict, *,
                     upper_receipt_ts: datetime | None,
                     upper_dispute_event_id: int) -> list[dict]:
    """在固定边界内物化符合筛选的升序条目快照(纯函数式只读)。"""
    receipts = _filter_receipts(db, f, upper_receipt_ts)
    dep_events = _filter_dispute_events(db, f, upper_dispute_event_id)
    disputes = {d.id: d for d in db.query(EvidenceReceiptDispute).all()}
    rows: list[dict] = []
    for r in receipts:
        rows.append(_receipt_row(r))
    for e in dep_events:
        d = disputes.get(e.dispute_id)
        if d is None:
            # 正常不会发生(外键); 跳过孤儿事件以免看板报错
            continue
        rows.append(_dispute_event_row(e, d))
    rows.sort(key=_sort_key)
    # position 在最终顺序上分配(从 0 开始), 游标按位置推进
    for i, row in enumerate(rows):
        row["position"] = i
    return rows


# ---------- 包汇总(固定边界; 按分发包/接收方范围, 不受时间/状态/类型过滤) ----------

def _package_ids_in_scope(db: Session, f: dict, *,
                          upper_receipt_ts: datetime | None) -> list[str]:
    """看板卡片应覆盖的包: 显式指定分发包时就是该包; 否则取固定时点前已分派
    (或已回执)接收方的全部包。与时间/状态/类型过滤无关 —— 即使查询窗口内
    没有任何事件, 管理员也要看到这些包的完成率与待处理争议。"""
    if f["package_id"]:
        return [f["package_id"]]
    a_q = db.query(EvidenceDistributionAssignment.distribution_id).distinct()
    if upper_receipt_ts is not None:
        a_q = a_q.filter(
            EvidenceDistributionAssignment.created_at <= upper_receipt_ts)
    if f["recipient"]:
        a_q = a_q.filter(EvidenceDistributionAssignment.recipient_id
                         == f["recipient"])
    ids = {r[0] for r in a_q.all()}
    # 兜底: 接收方只作为回执接收方出现(理论不会, 分派总先于回执)
    if f["recipient"]:
        r_q = (db.query(EvidenceDistributionReceipt.distribution_id)
               .filter(EvidenceDistributionReceipt.recipient_id
                       == f["recipient"]).distinct())
        if upper_receipt_ts is not None:
            r_q = r_q.filter(EvidenceDistributionReceipt.submitted_at
                             <= upper_receipt_ts)
        ids |= {r[0] for r in r_q.all()}
    return sorted(ids)


def _dispute_state_at(d: EvidenceReceiptDispute, events: list,
                      upper_event_id: int) -> str | None:
    """争议单在固定争议事件边界(upper_event_id)时的状态: 取边界内最后一个
    事件的 to_status; 边界内无事件(争议在固定时点之后才打开)返回 None。"""
    in_scope = sorted((e for e in events
                       if e.dispute_id == d.id and e.id <= upper_event_id),
                      key=lambda x: x.id)
    if not in_scope:
        return None
    return in_scope[-1].to_status or in_scope[-1].from_status or d.status


def _package_summaries(db: Session, f: dict, feed: list[dict], *,
                       upper_receipt_ts: datetime | None,
                       upper_dispute_event_id: int,
                       fixed_at: datetime) -> list[dict]:
    """对范围内每个包, 用固定边界数据计算完成率/异常/待处理争议/最近事件。

    完成率/异常/待处理争议统计只受 分发包/接收方 过滤影响(包级口径),
    不受 状态/时间范围/类型 过滤影响 —— 卡片描述的是"这个包在固定时点
    的整体处置情况"; 最近事件则取自本查询快照(随时间/状态/类型过滤变化)。
    所有聚合都限定在固定边界内: 固定时点后新增分派/回执/争议不改变卡片。"""
    package_ids = _package_ids_in_scope(
        db, f, upper_receipt_ts=upper_receipt_ts)
    if not package_ids:
        return []
    dists = {d.id: d for d in
             db.query(EvidenceDistribution)
             .filter(EvidenceDistribution.id.in_(package_ids)).all()}
    # 固定边界内的全量分派/回执(仅按包/接收方范围收敛)
    a_q = db.query(EvidenceDistributionAssignment).filter(
        EvidenceDistributionAssignment.distribution_id.in_(package_ids))
    if upper_receipt_ts is not None:
        a_q = a_q.filter(EvidenceDistributionAssignment.created_at
                         <= upper_receipt_ts)
    r_q = db.query(EvidenceDistributionReceipt).filter(
        EvidenceDistributionReceipt.distribution_id.in_(package_ids))
    if upper_receipt_ts is not None:
        r_q = r_q.filter(EvidenceDistributionReceipt.submitted_at
                         <= upper_receipt_ts)
    d_q = db.query(EvidenceReceiptDispute).filter(
        EvidenceReceiptDispute.distribution_id.in_(package_ids))
    if f["recipient"]:
        a_q = a_q.filter(
            EvidenceDistributionAssignment.recipient_id == f["recipient"])
        r_q = r_q.filter(EvidenceDistributionReceipt.recipient_id
                         == f["recipient"])
        d_q = d_q.filter(EvidenceReceiptDispute.recipient_id
                         == f["recipient"])
    assignments = a_q.all()
    receipts = r_q.all()
    disputes = d_q.all()
    # 争议状态按"固定边界内最后一个争议事件"重放, 不读争议单当前状态
    dep_event_q = db.query(EvidenceReceiptDisputeEvent).filter(
        EvidenceReceiptDisputeEvent.id <= upper_dispute_event_id,
        EvidenceReceiptDisputeEvent.dispute_id.in_(
            [d.id for d in disputes] or ["-"]))
    dep_events = dep_event_q.all()
    a_by_pkg: dict[str, list] = {}
    r_by_pkg: dict[str, list] = {}
    d_state_by_pkg: dict[str, list[str]] = {}
    for a in assignments:
        a_by_pkg.setdefault(a.distribution_id, []).append(a)
    for r in receipts:
        r_by_pkg.setdefault(r.distribution_id, []).append(r)
    for d in disputes:
        st = _dispute_state_at(d, dep_events, upper_dispute_event_id)
        if st is not None:
            d_state_by_pkg.setdefault(d.distribution_id, []).append(st)
    feed_by_pkg: dict[str, list] = {}
    for row in feed:
        if row.get("package_id"):
            feed_by_pkg.setdefault(row["package_id"], []).append(row)

    out: list[dict] = []
    for pid in package_ids:
        pkg_asg = a_by_pkg.get(pid, [])
        pkg_rec = r_by_pkg.get(pid, [])
        dispute_states = d_state_by_pkg.get(pid, [])
        # 分派在边界时刻的状态: 终态(SIGNED/PARTIAL/REJECTED)只在固定时点前
        # 已有对应回执时成立, 否则回退 PENDING(当前状态可能已被之后的回执推进)
        rec_by_assignment = {r.assignment_id: r for r in pkg_rec}

        def _state_at(a) -> str:
            if a.status in ("SIGNED", "PARTIAL", "REJECTED"):
                return a.status if a.id in rec_by_assignment else "PENDING"
            return a.status

        total_recipients = len(pkg_asg)
        completed = sum(1 for a in pkg_asg
                        if _state_at(a) in ("SIGNED", "PARTIAL", "REJECTED"))
        completion_rate = round(completed / total_recipients, 4) \
            if total_recipients else 0.0
        states = [_state_at(a) for a in pkg_asg]
        signed = states.count("SIGNED")
        partial = states.count("PARTIAL")
        rejected = states.count("REJECTED")
        pending = states.count("PENDING")
        overdue = states.count("OVERDUE")
        anomaly_receipts = sum(1 for r in pkg_rec
                               if r.receipt_type in ("PARTIAL", "REJECTED"))
        anomaly_events = sum(int(r.anomaly_count or 0)
                             + int(r.rejected_event_count or 0)
                             for r in pkg_rec)
        open_disputes = sum(1 for st in dispute_states if st != "CLOSED")
        dist = dists.get(pid)
        recent = sorted(feed_by_pkg.get(pid, []),
                        key=_sort_key, reverse=True)[:SUMMARY_RECENT_EVENTS]
        out.append({
            "package_id": pid,
            "status": (distribution.effective_status(dist, now=fixed_at)
                       if dist is not None else None),
            "plan_id": dist.plan_id if dist is not None else None,
            "total_recipients": total_recipients,
            "completed_receipts": completed,
            "completion_rate": completion_rate,
            "signed": signed, "partial": partial, "rejected": rejected,
            "pending": pending, "overdue": overdue,
            "anomaly_receipt_count": anomaly_receipts,
            "anomaly_event_count": anomaly_events,
            "dispute_count": len(dispute_states),
            "open_dispute_count": open_disputes,
            "recent_events": [_compact_event(x) for x in recent],
        })
    # 看板卡片按最近活动(快照内最新事件时间)倒序
    out.sort(key=lambda x: (x["recent_events"][0]["event_ts"]
                            if x["recent_events"] else ""), reverse=True)
    return out


def _compact_event(row: dict) -> dict:
    return {
        "kind": row["kind"], "event_ts": row["event_ts"],
        "recipient": row["recipient"], "status": row["status"],
        "receipt_id": row.get("receipt_id"),
        "dispute_id": row.get("dispute_id"),
        "dispute_event": row.get("dispute_event"),
        "operator": row.get("operator"), "summary": row.get("summary"),
    }


# ---------- 创建查询(固定时点) ----------

def get_query(db: Session, query_id: str) -> ReceiptAuditQuery:
    q = db.get(ReceiptAuditQuery, query_id)
    if q is None:
        raise ReceiptAuditError(f"回执审计查询 {query_id} 不存在",
                               code="query_not_found", status_code=404)
    return q


def create_query(db: Session, *, operator: str, package_id: str | None = None,
                 recipient: str | None = None,
                 status: list[str] | None = None,
                 kinds: list[str] | None = None,
                 start_ts: datetime | None = None,
                 end_ts: datetime | None = None) -> ReceiptAuditQuery:
    """创建固定时点看板查询: 校验条件/接收方/分发包, 取边界并物化快照。"""
    try:
        recipient_n = distribution.normalize_recipient(recipient) \
            if recipient else None
    except distribution.DistributionStateError as e:
        raise ReceiptAuditError(str(e), code=getattr(e, "code",
                                                     "invalid_recipient"),
                                status_code=422,
                                extra=getattr(e, "extra", None))
    filters = _normalize_filters(
        package_id=package_id, recipient=recipient_n, status=status,
        kinds=kinds, start_ts=start_ts, end_ts=end_ts)
    with _lock:
        # 未知接收方: 显式拒绝(区别于"在册但无数据"的空结果)
        if recipient_n is not None:
            rcpt = db.get(EvidenceRecipient, recipient_n)
            if rcpt is None:
                raise ReceiptAuditError(
                    f"接收方 {recipient_n} 未登记, 不能作为看板查询条件",
                    code="recipient_unknown", status_code=404,
                    extra={"recipient": recipient_n})
        # 未知分发包: 同样显式 404(避免把拼错的包误判为空结果)
        if package_id:
            if db.get(EvidenceDistribution, package_id) is None:
                raise ReceiptAuditError(
                    f"分发包 {package_id} 不存在",
                    code="package_not_found", status_code=404,
                    extra={"package_id": package_id})
        now = _now()
        # 固定边界: 回执按服务端提交时刻(同一进程锁内提交, 之后时刻的回执
        # 不会进入本快照); 争议事件按全库最大自增流水 id
        upper_receipt_ts = now
        upper_dispute = db.query(
            func.max(EvidenceReceiptDisputeEvent.id)).scalar() or 0
        feed = materialize_feed(
            db, filters, upper_receipt_ts=upper_receipt_ts,
            upper_dispute_event_id=upper_dispute)
        if len(feed) > MAX_EXPORT_ROWS:
            raise ReceiptAuditError(
                f"固定时点结果集 {len(feed)} 条超过上限 {MAX_EXPORT_ROWS}, "
                "请缩小时间范围或增加过滤条件",
                code="result_too_large", status_code=422,
                extra={"total_items": len(feed), "limit": MAX_EXPORT_ROWS})
        summaries = _package_summaries(
            db, filters, feed, upper_receipt_ts=upper_receipt_ts,
            upper_dispute_event_id=upper_dispute, fixed_at=now)
        qid = _new_id("RAQ")
        q = ReceiptAuditQuery(
            id=qid, filters_json=filters,
            filters_hash=_hash_obj(filters),
            package_id=filters["package_id"],
            recipient=filters["recipient"],
            start_ts=(datetime.fromisoformat(filters["start_ts"])
                      if filters["start_ts"] else None),
            end_ts=(datetime.fromisoformat(filters["end_ts"])
                    if filters["end_ts"] else None),
            upper_receipt_ts=upper_receipt_ts,
            upper_dispute_event_id=upper_dispute, fixed_at=now,
            feed_json=feed, feed_digest=feed_digest(feed),
            total_items=len(feed), summaries_json=summaries,
            package_count=len(summaries), created_by=operator)
        db.add(q)
        db.flush()
        _log(db, "query.create", operator=operator, query_id=qid,
             detail={"filters": filters, "total_items": len(feed),
                     "package_count": len(summaries),
                     "upper_receipt_ts": upper_receipt_ts.isoformat(),
                     "upper_dispute_event_id": upper_dispute,
                     "empty": len(feed) == 0})
        db.commit()
        return q


def feed_digest(feed: list[dict]) -> str:
    """快照摘要: 对去内部辅助键的条目数组规范化后取 sha256(分页/导出共用)。"""
    return _hash_obj({"items": [_digest_item(x) for x in feed]})


def _digest_item(row: dict) -> dict:
    return {k: v for k, v in row.items() if not k.startswith("_")}


# ---------- 翻页(严格顺序游标) ----------

def page_query(db: Session, query_id: str, *, operator: str,
               cursor: str | None = None, limit: int | None = None) -> dict:
    lim = max(1, min(MAX_PAGE_LIMIT, int(limit or DEFAULT_PAGE_LIMIT)))
    with _lock:
        q = get_query(db, query_id)
        total = q.total_items
        feed = q.feed_json or []

        def reject(code: str, reason: str, *, status_code: int = 409,
                   extra: dict | None = None, persist: bool = True):
            if persist:
                # 独立提交, 保证主事务随后 rollback 时拒绝留痕不丢失
                _log(db, "query.page", operator=operator, query_id=query_id,
                     ok=False, reason_code=code,
                     detail={"reason": reason, **(extra or {})})
                db.commit()
            raise ReceiptAuditError(reason, code=code,
                                    status_code=status_code, extra=extra)

        if cursor is None:
            if q.status == "CLOSED":
                # 已翻完固定快照: 幂等回显终止空页(不报错, 也不暴露新事件)
                return _page_response(
                    q, [], q.last_position, q.pages_delivered + 1,
                    has_more=False, next_cursor=None, digest=None)
            if q.pages_delivered > 0:
                reject("cursor_required",
                       "查询已开始翻页, 后续请求必须携带上一页返回的 "
                       "next_cursor(防止漏页/重页)",
                       extra={"pages_delivered": q.pages_delivered})
            position = 0
            page_no = 1
        else:
            decoded = decode_cursor(cursor)
            if decoded is None:
                reject("cursor_invalid",
                       "游标签名无效或已损坏(非本服务签发)", status_code=422)
            cqid, cpos, cpage, cfp = decoded
            if cqid != query_id:
                reject("cursor_other_query",
                       "游标属于另一个看板查询, 不能跨查询续页",
                       extra={"cursor_query_id": cqid})
            if cfp != q.filters_hash[:16]:
                reject("cursor_filters_mismatch",
                       "游标内查询条件与本查询固定条件不一致(查询条件不能变化)",
                       extra={"cursor_fingerprint": cfp,
                              "query_fingerprint": q.filters_hash[:16]})
            if cursor != q.last_cursor:
                # 不是最近签发游标: 旧页码=重复使用, 未来页码=跳页
                expected_page = q.pages_delivered + 1
                code = ("cursor_reused" if cpage < expected_page
                        else "cursor_invalid")
                reject(code,
                       "游标不是本查询最近一页签发的游标(重复使用或跳页): "
                       f"游标下一页={cpage}, 期望下一页={expected_page}",
                       extra={"cursor_page": cpage,
                              "expected_page": expected_page,
                              "pages_delivered": q.pages_delivered})
            position = cpos
            # 游标页码必须等于"下一页页码"(last_cursor 精确匹配时自然成立,
            # 此校验为防御性兜底)
            page_no = q.pages_delivered + 1
            if cpage != page_no:
                reject("cursor_invalid",
                       "游标页码与查询进度不一致",
                       extra={"cursor_page": cpage, "expected_page": page_no})

        # 空结果 / 已到末尾: 幂等返回空页(不推进、不签游标), 并关闭查询
        if position >= total:
            if q.status == "ACTIVE":
                q.status = "CLOSED"
                q.closed_at = _now()
            _log(db, "query.page", operator=operator, query_id=query_id,
                 detail={"page_no": page_no, "item_count": 0,
                         "position": position, "end": True})
            db.commit()
            return _page_response(q, [], position, page_no, has_more=False,
                                  next_cursor=None, digest=None)

        end = min(position + lim, total)
        items = feed[position:end]
        has_more = end < total
        # 游标页码为"该游标将取的下一页页码", 与已交付页数配对, 便于检测跳页
        next_page_no = page_no + 1
        next_cursor = (encode_cursor(q.id, end, next_page_no, q.filters_hash)
                       if has_more else None)
        digest = _hash_obj({"page": page_no,
                            "items": [_digest_item(x) for x in items]})
        db.add(ReceiptAuditPage(
            query_id=q.id, page_no=page_no, from_position=position,
            to_position=end, item_count=len(items), page_digest=digest,
            created_by=operator))
        q.pages_delivered = page_no
        q.last_position = end
        q.last_cursor = next_cursor
        if not has_more:
            q.status = "CLOSED"
            q.closed_at = _now()
        _log(db, "query.page", operator=operator, query_id=query_id,
             detail={"page_no": page_no, "item_count": len(items),
                     "from_position": position, "to_position": end,
                     "has_more": has_more})
        db.commit()
        return _page_response(q, items, position, page_no,
                              has_more=has_more, next_cursor=next_cursor,
                              digest=digest)


def _page_response(q: ReceiptAuditQuery, items: list[dict], position: int,
                   page_no: int, *, has_more: bool,
                   next_cursor: str | None, digest: str | None) -> dict:
    return {
        "query_id": q.id,
        "page_no": page_no,
        "fixed_at": q.fixed_at.isoformat() if q.fixed_at else None,
        "filters": q.filters_json,
        "boundary": {
            "upper_receipt_ts": (q.upper_receipt_ts.isoformat()
                                 if q.upper_receipt_ts else None),
            "upper_dispute_event_id": q.upper_dispute_event_id,
        },
        "cursor": {"next_cursor": next_cursor, "has_more": has_more,
                   "position": position + len(items)},
        "items": [_digest_item(x) for x in items],
        "item_count": len(items),
        "total_items": q.total_items,
        "empty": q.total_items == 0,
        "page_digest": digest,
        "pages_delivered": q.pages_delivered,
        "query_status": q.status,
    }


def query_to_dict(q: ReceiptAuditQuery, *, with_summaries: bool = True,
                  with_pages: bool = False) -> dict:
    out = {
        "query_id": q.id,
        "filters": q.filters_json,
        "fixed_at": q.fixed_at.isoformat() if q.fixed_at else None,
        "boundary": {
            "upper_receipt_ts": (q.upper_receipt_ts.isoformat()
                                 if q.upper_receipt_ts else None),
            "upper_dispute_event_id": q.upper_dispute_event_id,
        },
        "total_items": q.total_items,
        "empty": q.total_items == 0,
        "package_count": q.package_count,
        "feed_digest": q.feed_digest,
        "pages_delivered": q.pages_delivered,
        "last_position": q.last_position,
        "status": q.status,
        "created_by": q.created_by,
        "created_at": q.created_at.isoformat() if q.created_at else None,
        "closed_at": q.closed_at.isoformat() if q.closed_at else None,
    }
    if with_summaries:
        out["packages"] = q.summaries_json or []
    if with_pages:
        out["pages"] = [{
            "page_no": p.page_no, "from_position": p.from_position,
            "to_position": p.to_position, "item_count": p.item_count,
            "page_digest": p.page_digest,
            "created_by": p.created_by,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        } for p in sorted(q.pages, key=lambda x: x.page_no)]
    return out


def list_queries(db: Session, *, package_id: str | None = None,
                 recipient: str | None = None, limit: int = 50
                 ) -> list[ReceiptAuditQuery]:
    qq = db.query(ReceiptAuditQuery)
    if package_id:
        qq = qq.filter(ReceiptAuditQuery.package_id == package_id)
    if recipient:
        qq = qq.filter(ReceiptAuditQuery.recipient == recipient)
    return (qq.order_by(ReceiptAuditQuery.created_at.desc(),
                        ReceiptAuditQuery.id.desc())
            .limit(min(max(1, limit), 200)).all())


# ---------- CSV 导出(与分页同源同序, 幂等) ----------

def _csv_row(row: dict, position: int) -> dict:
    return {
        "position": position,
        "kind": row["kind"],
        "event_ts": row.get("event_ts") or "",
        "package_id": row.get("package_id") or "",
        "recipient": row.get("recipient") or "",
        "status": row.get("status") or "",
        "receipt_id": row.get("receipt_id") or "",
        "receipt_type": row.get("receipt_type") or "",
        "dispute_id": row.get("dispute_id") or "",
        "dispute_event": row.get("dispute_event") or "",
        "assignment_id": row.get("assignment_id") or "",
        "total_required": row.get("total_required") or "",
        "confirmed_count": row.get("confirmed_count") or "",
        "anomaly_count": row.get("anomaly_count") or "",
        "rejected_event_count": row.get("rejected_event_count") or "",
        "operator": row.get("operator") or "",
        "summary": row.get("summary") or "",
    }


def render_csv(feed: list[dict]) -> bytes:
    """把固定快照渲染为 CSV(utf-8-sig, 便于 Excel; 行序=快照顺序)。

    空结果仍输出表头, 是合法的空 CSV。"""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS,
                            extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in feed:
        writer.writerow(_csv_row(row, row["position"]))
    return b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8")


def create_export(db: Session, query_id: str, *, operator: str,
                  idempotency_key: str) -> ReceiptAuditExport:
    """为固定查询生成 CSV 导出并落盘。同 (查询, 幂等键) 重放返回首次导出。"""
    with _lock:
        q = get_query(db, query_id)
        # 幂等键在本命名空间内全局唯一: 同键不同请求/查询 -> 拒绝
        existing = (db.query(ReceiptAuditExport)
                    .filter(ReceiptAuditExport.idempotency_key
                            == idempotency_key).first())
        if existing is not None:
            if existing.query_id != query_id:
                _log(db, "query.export", operator=operator,
                     query_id=query_id, export_id=existing.id, ok=False,
                     reason_code="idempotency_reuse",
                     detail={"reason": "幂等键被另一个看板查询的导出复用",
                             "bound_query_id": existing.query_id})
                db.commit()
                raise ReceiptAuditError(
                    "幂等键已被另一个看板查询的导出使用, 不能复用",
                    code="idempotency_reuse")
            _log(db, "query.export", operator=operator, query_id=query_id,
                 export_id=existing.id,
                 detail={"replayed": True, "row_count": existing.row_count})
            db.commit()
            return existing
        feed = q.feed_json or []
        content = render_csv(feed)
        digest = hashlib.sha256(content).hexdigest()
        eid = _new_id("RAE")
        file_name = f"receipt-audit-{q.id}-{eid}.csv"
        path = os.path.join(store_dir(), file_name)
        with open(path, "wb") as fh:
            fh.write(content)
        exp = ReceiptAuditExport(
            id=eid, query_id=q.id, operator=operator,
            row_count=len(feed), file_name=file_name, file_path=path,
            file_size=len(content), file_digest=digest,
            feed_digest=q.feed_digest, idempotency_key=idempotency_key)
        db.add(exp)
        db.flush()
        _log(db, "query.export", operator=operator, query_id=query_id,
             export_id=eid, detail={
                 "row_count": len(feed), "file_size": len(content),
                 "file_digest": digest, "empty": len(feed) == 0})
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            winner = (db.query(ReceiptAuditExport)
                      .filter(ReceiptAuditExport.idempotency_key
                              == idempotency_key).first())
            if winner is not None and winner.query_id == query_id:
                return winner
            raise ReceiptAuditError("幂等键被不同导出复用",
                                    code="idempotency_reuse")
        return exp


def get_export(db: Session, export_id: str) -> ReceiptAuditExport:
    e = db.get(ReceiptAuditExport, export_id)
    if e is None:
        raise ReceiptAuditError(f"看板导出 {export_id} 不存在",
                               code="export_not_found", status_code=404)
    return e


def export_path_for_download(db: Session, export: ReceiptAuditExport,
                             operator: str) -> str:
    if not os.path.exists(export.file_path):
        _log(db, "query.export_download", operator=operator,
             query_id=export.query_id, export_id=export.id, ok=False,
             reason_code="export_file_missing",
             detail={"file_path": export.file_path})
        db.commit()
        raise ReceiptAuditError(
            f"导出 CSV 文件缺失({export.file_name}), 请重新导出",
            code="export_file_missing", status_code=404)
    export.download_count += 1
    _log(db, "query.export_download", operator=operator,
         query_id=export.query_id, export_id=export.id,
         detail={"download_count": export.download_count,
                 "file_digest": export.file_digest})
    db.commit()
    return export.file_path


def export_to_dict(e: ReceiptAuditExport) -> dict:
    return {
        "export_id": e.id, "query_id": e.query_id, "operator": e.operator,
        "row_count": e.row_count, "file_name": e.file_name,
        "file_size": e.file_size, "file_digest": e.file_digest,
        "feed_digest": e.feed_digest, "download_count": e.download_count,
        "empty": e.row_count == 0,
        "idempotency_key": e.idempotency_key,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "download_url": f"/api/admin/evidence/receipt-audit/exports/{e.id}/download",
    }


def list_exports(db: Session, *, query_id: str | None = None,
                 limit: int = 100) -> list[ReceiptAuditExport]:
    qq = db.query(ReceiptAuditExport)
    if query_id:
        qq = qq.filter(ReceiptAuditExport.query_id == query_id)
    return (qq.order_by(ReceiptAuditExport.created_at.desc(),
                        ReceiptAuditExport.id.desc())
            .limit(min(max(1, limit), 500)).all())
