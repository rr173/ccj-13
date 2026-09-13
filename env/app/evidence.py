"""审计证据查询与一致性证明: 固定边界会话分页 + 哈希链证明 + 异步分段证据包导出。

设计要点
========
1. 证据查询会话(evidence_sessions): 运维按 plan_id 查询跨计划流(plan_id)、
   批次流("batch:<id>")与补偿控制流(COMP_* 事件)的统一事件时间线。创建会话时
   一次性固定筛选条件、起始 global_seq 与读取边界 upper_global_seq(创建时刻
   全库最大 global_seq); 之后新写入事件的 global_seq 必然更大, 永远不会插入
   已经开始的结果集 —— 翻页不会漏事件、不会重复事件, 也不受新写入影响。
2. 每页证明(proof): 每次翻页在**一次请求的固定读取边界**内重算返回片段及其
   前后边界的哈希链连续性:
   - global_seq 在片段内严格递增且与已交付位置不重复(global_seq_duplicate);
   - 按流(stream_key)分组重算 stream_hash, 必须与存储逐行一致
     (stream_hash_mismatch); 每行 prev_stream_hash 必须与同流前一事件衔接
     (prev_hash_mismatch);
   - 单流 stream_seq 从 DB 实读的前一行 +1 起连续(stream_seq_gap/duplicate);
   - 时间戳倒流超过 AUDIT_MAX_OUT_OF_ORDER_SECONDS -> out_of_order;
   - 边界锚点(创建时各流在 upper 处的尾事件)每页复核未被删改
     (boundary_anchor_mismatch);
   - cursor 必须沿同一会话上一页签发(cursor_invalid)。
   任一失败: 拒绝导出本页、会话置 BROKEN 并返回机器可读的断链位置与原因。
3. 证据导出(evidence_exports): 基于会话异步分段生成确定性 JSONL 证据包。
   段内容完全由(固定边界, 段序号, 段大小)决定, 段产物(文件+摘要)先落库
   再进入下一段(崩溃边界), 重跑幂等、不重复写包。导出期间检测到事件链变化
   -> 自动 PAUSED(chain_changed)并记录断链证据, 绝不产出标记为 COMPLETED
   的包; resume 前从边界起点重新全量校验, 通过后从首个未完成分段续跑。
   支持暂停/恢复/取消/失败重试; 完成后提供一次性下载令牌与 manifest
   (逐段/逐流摘要 + 整体 manifest_hash, 可离线复算)。
4. 并发: worker 认领受 EVIDENCE_EXPORT_MAX_CONCURRENCY 限制(Postgres 咨询锁
   串行认领, SQLite 写事务天然串行); 所有管理操作走幂等框架(evidence.*
   命名空间), 同键重放返回首次结果。
"""
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import auditreplay
from .models import (
    AUDIT_EVENT_TYPES, AuditEvent, EvidenceDownload, EvidenceExport,
    EvidenceExportEvent, EvidenceExportSegment, EvidencePage,
    EvidenceSession, IdempotencyKey, MigrationPlan, PlanStep,
)

EVIDENCE_NAMESPACE = "evidence"
DIGEST_ALGORITHM = "sha256"
# 补偿控制流事件类型(统一投影在计划流上, 这里用于时间线标注/类型过滤)
COMP_EVENT_TYPES = ("COMP_EXECUTED", "COMP_FAILED", "COMP_UNDONE",
                    "COMP_APPROVAL", "COMP_WINDOW")

EVIDENCE_ALLOWED_PAUSE = {"QUEUED", "RUNNING"}
EVIDENCE_ALLOWED_RESUME = {"PAUSED", "FAILED"}
EVIDENCE_ALLOWED_CANCEL = {"QUEUED", "RUNNING", "PAUSED", "FAILED"}

# 断链 -> HTTP 状态: 客户端请求类游标错误用 409, 链本身被破坏用 422
BREAK_HTTP_STATUS = {"cursor_invalid": 409}


class EvidenceNotFound(Exception):
    """会话 / 导出 / 令牌不存在 -> 404。"""


class EvidenceStateError(Exception):
    """当前状态不允许该操作 / 权限不符 / 幂等键复用 -> 409。"""


class EvidenceChainBroken(Exception):
    """哈希链一致性校验失败 -> 422(会话置 BROKEN, 机器可读断链位置)。"""

    def __init__(self, breaks: list[dict]):
        super().__init__("; ".join(b.get("message", b.get("code", "chain broken"))
                                   for b in breaks[:3]))
        self.breaks = breaks


class EvidenceExportFatal(Exception):
    """导出执行中的致命(可恢复)错误: 任务 FAILED, 可 resume 重试。"""

    def __init__(self, code: str, reason: str):
        super().__init__(reason)
        self.code = code
        self.reason = reason


# SQLite 写事务天然串行; 进程内再加一把栅栏保护 cursor/边界推进。
_evidence_lock = threading.RLock()


# ---------- 配置 ----------

def max_concurrency() -> int:
    try:
        return max(1, int(os.getenv("EVIDENCE_EXPORT_MAX_CONCURRENCY", "2")))
    except ValueError:
        return 2


def default_segment_size() -> int:
    try:
        return max(1, min(1000, int(os.getenv("EVIDENCE_SEGMENT_SIZE", "200"))))
    except ValueError:
        return 200


def default_page_limit() -> int:
    try:
        return max(1, min(500, int(os.getenv("EVIDENCE_PAGE_LIMIT", "100"))))
    except ValueError:
        return 100


def download_ttl_seconds() -> int:
    try:
        return max(30, int(os.getenv("EVIDENCE_DOWNLOAD_TTL_SECONDS", "600")))
    except ValueError:
        return 600


def store_dir() -> str:
    d = os.getenv("EVIDENCE_STORE_DIR",
                  os.getenv("ARCHIVE_STORE_DIR",
                            os.path.join(".", "evidence_store")))
    return os.path.abspath(d)


def ensure_store_dir() -> None:
    os.makedirs(store_dir(), exist_ok=True)


def now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def out_of_order_seconds() -> int:
    return auditreplay.max_out_of_order_seconds()


# ---------- 哈希 / 规范化 ----------

def _hash_obj(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str,
                   ensure_ascii=False).encode()
    ).hexdigest()


# 证据规范化的事件字段(不含 id/created_at 等视图/存储派生字段;
# 片段、页面与导出包共用同一规范化, 摘要可跨处复算)
EVENT_CANON_FIELDS = (
    "global_seq", "stream_key", "stream_seq", "event_type", "plan_id",
    "batch_id", "scan_id", "step_seq", "correlation_id", "prev_global_seq",
    "prev_stream_hash", "stream_hash", "payload", "operator", "source",
    "event_ts",
)


def canonical_event(r: AuditEvent | dict) -> dict:
    """事件的规范化表示(分页/导出/复算共用)。"""
    out = {}
    for k in EVENT_CANON_FIELDS:
        v = getattr(r, k) if not isinstance(r, dict) else r.get(k)
        if isinstance(v, datetime):
            v = v.isoformat()
        out[k] = v
    return out


def canonical_event_bytes(r: AuditEvent | dict) -> bytes:
    return (json.dumps(canonical_event(r), ensure_ascii=False, sort_keys=True,
                       default=str, separators=(",", ":")) + "\n").encode("utf-8")


def fragment_digest(rows: list[AuditEvent | dict]) -> str:
    """片段摘要: 逐行规范化 JSON(有序键, 末尾换行)拼接后的 sha256。

    与导出 JSONL 行格式完全一致, 因此页面留痕的 fragment_digest 可与
    导出包内对应段的行摘要直接复算比对。"""
    h = hashlib.sha256()
    for r in rows:
        h.update(canonical_event_bytes(r))
    return h.hexdigest()


def _cursor_secret() -> bytes:
    # 进程级稳定密钥: 游标只是不透明句柄, 真实状态全部在服务端会话行内。
    sec = os.getenv("EVIDENCE_CURSOR_SECRET")
    if sec:
        return sec.encode("utf-8")
    # 未配置时用数据库 URL 派生(单副本部署; 密钥变化只让旧游标失效, 不影响正确性)
    from .db import DATABASE_URL
    return hashlib.sha256(("evidence-cursor:" + DATABASE_URL).encode()).digest()


def encode_cursor(session_id: str, global_seq: int, event_id: int,
                  page_no: int) -> str:
    body = f"{session_id}.{global_seq}.{event_id}.{page_no}"
    sig = hmac.new(_cursor_secret(), body.encode(), hashlib.sha256).hexdigest()[:16]
    import base64
    raw = f"{body}.{sig}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(token: str) -> tuple[str, int, int, int] | None:
    import base64
    try:
        pad = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(token + pad).decode()
        session_id, gs, eid, page_no, sig = raw.rsplit(".", 4)
        body = f"{session_id}.{gs}.{eid}.{page_no}"
        expect = hmac.new(_cursor_secret(), body.encode(),
                          hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(sig, expect):
            return None
        return session_id, int(gs), int(eid), int(page_no)
    except Exception:
        return None


# ---------- 会话作用域: 计划流 / 批次流 / 补偿控制流 ----------

def _batch_ids_of_plan(session: Session, plan_id: str) -> list[str]:
    return [r[0] for r in (session.query(PlanStep.batch_id)
                           .join(MigrationPlan, MigrationPlan.id == PlanStep.plan_id)
                           .filter(PlanStep.plan_id == plan_id)
                           .order_by(PlanStep.batch_id)
                           .distinct().all())]


def resolve_stream_keys(session: Session, plan_id: str) -> list[str]:
    """会话固定涉及的流集合(创建时解析后不可变):
    计划流(plan_id) + 该计划各步骤批次流("batch:<id>")。

    补偿控制流(COMP_*)事件统一追加在计划流上, 因此天然包含;
    计划创建后新追加的步骤也在计划流内, 不需要额外流。"""
    keys = [plan_id]
    for bid in _batch_ids_of_plan(session, plan_id):
        keys.append(f"batch:{bid}")
    return list(dict.fromkeys(keys))


# ---------- 锁 ----------

def _lock_scheduling(db: Session) -> None:
    if db.bind.dialect.name == "postgresql":
        db.execute(text("SELECT pg_advisory_xact_lock(20260925)"))


def _lock_creation(db: Session) -> None:
    if db.bind.dialect.name == "postgresql":
        db.execute(text("SELECT pg_advisory_xact_lock(20260926)"))


# ======================================================================
# ---------- 证据查询会话: 创建 / 固定边界 ----------
# ======================================================================

def get_session(db: Session, session_id: str) -> EvidenceSession:
    s = db.get(EvidenceSession, session_id)
    if s is None:
        raise EvidenceNotFound(f"证据查询会话 {session_id} 不存在")
    return s


def get_export(db: Session, export_id: str) -> EvidenceExport:
    e = db.get(EvidenceExport, export_id)
    if e is None:
        raise EvidenceNotFound(f"证据导出任务 {export_id} 不存在")
    return e


def _require_creator(session: EvidenceSession, operator: str) -> None:
    """证据查询/导出是取证操作: 只有会话创建者可沿会话翻页/发起与控制导出。"""
    if operator != session.created_by:
        raise EvidenceStateError(
            f"证据会话 {session.id} 由 {session.created_by} 创建, "
            f"操作者 {operator} 无权在该会话上操作(取证操作限定创建者)")


def create_session(db: Session, *, operator: str, plan_id: str,
                   start_global_seq: int | None = None,
                   start_ts: datetime | None = None,
                   end_ts: datetime | None = None,
                   event_types: list[str] | None = None,
                   sources: list[str] | None = None,
                   page_limit: int | None = None) -> EvidenceSession:
    """创建持久化证据查询会话: 固定筛选条件、起始 global_seq 与读取边界。

    一次请求内确定 upper_global_seq(当时全库最大 global_seq)与各流边界锚点;
    之后新写入事件 global_seq 更大, 不会进入本会话结果集。
    """
    plan = db.get(MigrationPlan, plan_id)
    if plan is None:
        raise EvidenceNotFound(f"迁移计划 {plan_id} 不存在")
    types = list(event_types or [])
    bad = [t for t in types if t not in AUDIT_EVENT_TYPES]
    if bad:
        raise EvidenceStateError(f"未知事件类型: {bad}")
    srcs = list(sources or [])
    bad_src = [s for s in srcs if s not in ("internal", "api", "system")]
    if bad_src:
        raise EvidenceStateError(
            f"事件来源必须是 internal/api/system 之一(收到 {bad_src})")
    if start_ts and end_ts and end_ts < start_ts:
        raise EvidenceStateError("end_ts 早于 start_ts, 时间范围非法")
    start_gs = max(0, int(start_global_seq or 0))
    with _evidence_lock:
        _lock_creation(db)
        stream_keys = resolve_stream_keys(db, plan_id)
        upper = db.query(func.max(AuditEvent.global_seq)).scalar() or 0
        upper_row = (db.query(AuditEvent)
                     .filter(AuditEvent.global_seq == upper).first())
        # 各流边界锚点: 边界内该流尾事件(用于 boundary_anchor_mismatch 检测)
        anchors: dict[str, dict] = {}
        for sk in stream_keys:
            tail = (db.query(AuditEvent)
                    .filter(AuditEvent.stream_key == sk,
                            AuditEvent.global_seq <= upper)
                    .order_by(AuditEvent.stream_seq.desc()).first())
            if tail is None:
                continue
            anchors[sk] = {
                "tail_stream_seq": tail.stream_seq,
                "tail_stream_hash": tail.stream_hash,
                "tail_global_seq": tail.global_seq,
            }
        sid = "ES" + uuid.uuid4().hex[:10]
        s = EvidenceSession(
            id=sid, plan_id=plan_id, start_global_seq=start_gs,
            start_ts=start_ts, end_ts=end_ts, event_types=types,
            sources=srcs, include_streams=stream_keys,
            upper_global_seq=upper,
            upper_event_id=(upper_row.id if upper_row else 0),
            upper_event_ts=(upper_row.event_ts if upper_row else None),
            boundary_anchors=anchors, created_by=operator)
        db.add(s)
        db.flush()
        # 固定边界内的预期总数(无结果也是合法边界: expected_count=0)
        s.expected_count = _scoped_query(db, s, after_gs=start_gs).count()
        rows_for_streams = (_scoped_query(db, s, after_gs=start_gs)
                            .with_entities(AuditEvent.stream_key).distinct().all())
        s.expected_streams = sorted({r[0] for r in rows_for_streams})
        db.flush()
        return s


def _scoped_query(db: Session, s: EvidenceSession, *, after_gs: int):
    q = (db.query(AuditEvent)
         .filter(AuditEvent.stream_key.in_(s.include_streams or []),
                 AuditEvent.global_seq > after_gs,
                 AuditEvent.global_seq <= s.upper_global_seq))
    if s.start_ts:
        q = q.filter(AuditEvent.event_ts >= s.start_ts)
    if s.end_ts:
        q = q.filter(AuditEvent.event_ts <= s.end_ts)
    if s.event_types:
        q = q.filter(AuditEvent.event_type.in_(s.event_types))
    if s.sources:
        q = q.filter(AuditEvent.source.in_(s.sources))
    return q


def _query_scoped(db: Session, s: EvidenceSession, *, after_gs: int,
                  limit: int) -> list[AuditEvent]:
    """按会话固定筛选条件读取一页(严格限定在固定边界内)。

    global_seq 排序, (global_seq, id) 双键保证同批插入顺序稳定。
    """
    q = (db.query(AuditEvent)
         .filter(AuditEvent.stream_key.in_(s.include_streams or []),
                 AuditEvent.global_seq > after_gs,
                 AuditEvent.global_seq <= s.upper_global_seq))
    if s.start_ts:
        q = q.filter(AuditEvent.event_ts >= s.start_ts)
    if s.end_ts:
        q = q.filter(AuditEvent.event_ts <= s.end_ts)
    if s.event_types:
        q = q.filter(AuditEvent.event_type.in_(s.event_types))
    if s.sources:
        q = q.filter(AuditEvent.source.in_(s.sources))
    return (q.order_by(AuditEvent.global_seq.asc(), AuditEvent.id.asc())
            .limit(limit).all())


# ======================================================================
# ---------- 哈希链一致性证明(片段 + 前后边界) ----------
# ======================================================================

def _break(code: str, message: str, **extra) -> dict:
    out = {"code": code, "message": message}
    out.update(extra)
    return out


def verify_fragment(db: Session, s: EvidenceSession,
                    rows: list[AuditEvent], *,
                    prev_delivered_gs: int) -> list[dict]:
    """对返回片段及其前后边界做哈希链连续性校验, 返回断链原因列表(空=通过)。

    在一次请求的固定读取边界(upper_global_seq)内完成全部读取与重算。
    """
    breaks: list[dict] = []
    if not rows:
        return breaks
    # 1) 全局序: 片段内严格递增, 且首行必须紧接已交付位置(不重复)
    last_gs = prev_delivered_gs
    for r in rows:
        if r.global_seq <= last_gs:
            breaks.append(_break(
                "global_seq_duplicate",
                f"global_seq={r.global_seq} 不大于已交付边界 {last_gs}"
                "(重复或回退, 游标与会话不一致)",
                global_seq=r.global_seq, expected_after=last_gs))
            break
        last_gs = r.global_seq
    gseqs = [r.global_seq for r in rows]
    if gseqs != sorted(set(gseqs)):
        breaks.append(_break("global_seq_duplicate",
                             "片段内 global_seq 存在重复或非递增",
                             global_seqs=gseqs))

    # 2) 按流分组: 重算 stream_hash + prev 衔接 + stream_seq 连续 + 乱序
    by_stream: dict[str, list[AuditEvent]] = {}
    for r in rows:
        by_stream.setdefault(r.stream_key, []).append(r)
    oo_limit = out_of_order_seconds()
    for sk, srows in sorted(by_stream.items()):
        srows.sort(key=lambda x: x.stream_seq)
        # 从 DB 实读该流全部边界内事件(含被时间/类型过滤掉的行),
        # 得到本片段在该流上的直接前一行与期望 stream_seq。
        first_seq = srows[0].stream_seq
        db_prev = (db.query(AuditEvent)
                   .filter(AuditEvent.stream_key == sk,
                           AuditEvent.stream_seq < first_seq)
                   .order_by(AuditEvent.stream_seq.desc()).first())
        prev_hash = db_prev.stream_hash if db_prev else None
        prev_seq = db_prev.stream_seq if db_prev else 0
        # 该流是否存在重复 stream_seq(直接 SQL 篡改才可能出现)
        dup_seqs = {seq for seq, cnt in (
            db.query(AuditEvent.stream_seq, func.count(AuditEvent.id))
            .filter(AuditEvent.stream_key == sk)
            .group_by(AuditEvent.stream_seq)
            .having(func.count(AuditEvent.id) > 1).all())}
        # 前一行本身也必须完好(它是片段的"前边界")
        if db_prev is not None:
            db_prev_prev = (db.query(AuditEvent)
                            .filter(AuditEvent.stream_key == sk,
                                    AuditEvent.stream_seq < db_prev.stream_seq)
                            .order_by(AuditEvent.stream_seq.desc()).first())
            expect_prev = auditreplay._chain_hash(
                db_prev.stream_seq, db_prev.event_type, db_prev.correlation_id,
                db_prev.payload, db_prev.event_ts,
                db_prev_prev.stream_hash if db_prev_prev else None,
                db_prev.operator)
            if expect_prev != db_prev.stream_hash:
                breaks.append(_break(
                    "stream_hash_mismatch",
                    f"片段前边界事件流 {sk} stream_seq={db_prev.stream_seq} "
                    "哈希重算不一致(前边界事件被篡改或其前序缺失)",
                    stream_key=sk, stream_seq=db_prev.stream_seq,
                    global_seq=db_prev.global_seq,
                    expected=expect_prev, actual=db_prev.stream_hash))
        prev_ts = db_prev.event_ts if db_prev else None
        for r in srows:
            if r.stream_seq != prev_seq + 1:
                code = ("stream_seq_duplicate" if r.stream_seq in dup_seqs
                        else "stream_seq_gap")
                breaks.append(_break(
                    code,
                    f"流 {sk} 顺序号不连续: stream_seq={r.stream_seq}, "
                    f"期望 {prev_seq + 1}(事件缺失或重复)",
                    stream_key=sk, stream_seq=r.stream_seq,
                    expected=prev_seq + 1, global_seq=r.global_seq))
            if r.prev_stream_hash != prev_hash:
                breaks.append(_break(
                    "prev_hash_mismatch",
                    f"流 {sk} stream_seq={r.stream_seq} 的 prev_stream_hash "
                    f"与前一事件(stream_seq={prev_seq})不衔接",
                    stream_key=sk, stream_seq=r.stream_seq,
                    global_seq=r.global_seq, expected_prev=prev_hash,
                    actual_prev=r.prev_stream_hash))
            expect = auditreplay._chain_hash(
                r.stream_seq, r.event_type, r.correlation_id, r.payload,
                r.event_ts, prev_hash, r.operator)
            if expect != r.stream_hash:
                breaks.append(_break(
                    "stream_hash_mismatch",
                    f"流 {sk} stream_seq={r.stream_seq} 哈希重算不一致"
                    "(事件被篡改或前序缺失)",
                    stream_key=sk, stream_seq=r.stream_seq,
                    global_seq=r.global_seq, expected=expect,
                    actual=r.stream_hash))
            if (prev_ts is not None and r.event_ts is not None
                    and r.event_ts < prev_ts
                    - timedelta(seconds=oo_limit)):
                breaks.append(_break(
                    "out_of_order",
                    f"乱序事件: 流 {sk} stream_seq={r.stream_seq} 时间 "
                    f"{r.event_ts.isoformat()} 早于前一事件 "
                    f"{prev_ts.isoformat()} 超过 {oo_limit}s",
                    stream_key=sk, stream_seq=r.stream_seq,
                    global_seq=r.global_seq))
            prev_hash = r.stream_hash
            prev_seq = r.stream_seq
            if prev_ts is None or r.event_ts > prev_ts:
                prev_ts = r.event_ts

    # 3) 边界锚点: 本页读取后复核所有涉及流的尾事件仍与创建时一致。
    #    任何被命中的流上锚点被删/改都立即断(不必等到最后一页)。
    touched = set(by_stream)
    for sk in touched:
        anchor = (s.boundary_anchors or {}).get(sk)
        if not anchor:
            continue
        tail = (db.query(AuditEvent)
                .filter(AuditEvent.stream_key == sk)
                .order_by(AuditEvent.stream_seq.desc()).first())
        if tail is None:
            breaks.append(_break(
                "boundary_anchor_mismatch",
                f"固定边界锚点丢失: 流 {sk} 在创建会话时尾部为 "
                f"stream_seq={anchor['tail_stream_seq']}, 现整个流已不存在",
                stream_key=sk))
            continue
        if (tail.stream_seq != anchor["tail_stream_seq"]
                or tail.stream_hash != anchor["tail_stream_hash"]
                or tail.global_seq != anchor["tail_global_seq"]):
            # 区分"边界内被删"(尾部缩到边界内)与"边界后追加"(正常)
            if tail.global_seq <= s.upper_global_seq:
                breaks.append(_break(
                    "boundary_anchor_mismatch",
                    f"固定边界被破坏: 流 {sk} 创建时边界尾事件 "
                    f"stream_seq={anchor['tail_stream_seq']}/"
                    f"global_seq={anchor['tail_global_seq']}, "
                    f"现尾部 stream_seq={tail.stream_seq}/"
                    f"global_seq={tail.global_seq}(边界内事件被删除或篡改)",
                    stream_key=sk,
                    expected=anchor,
                    actual={"tail_stream_seq": tail.stream_seq,
                            "tail_stream_hash": tail.stream_hash,
                            "tail_global_seq": tail.global_seq}))
            else:
                # 边界后新事件是正常追加; 但其前向哈希必须仍能连回锚点
                at_anchor = (db.query(AuditEvent)
                             .filter(AuditEvent.stream_key == sk,
                                     AuditEvent.stream_seq
                                     == anchor["tail_stream_seq"]).first())
                if (at_anchor is None
                        or at_anchor.stream_hash != anchor["tail_stream_hash"]):
                    breaks.append(_break(
                        "boundary_anchor_mismatch",
                        f"固定边界锚点事件被删改: 流 {sk} "
                        f"stream_seq={anchor['tail_stream_seq']}",
                        stream_key=sk))
    # 同一问题只报首个位置, 但不同代码/位置全部保留(机器可读)
    return breaks


def _stream_boundaries(rows: list[AuditEvent]) -> dict:
    """片段涉及各流的首尾链位置(响应/留痕共用)。"""
    out: dict[str, dict] = {}
    for r in rows:
        b = out.setdefault(r.stream_key, {
            "first_stream_seq": r.stream_seq,
            "first_stream_hash": r.prev_stream_hash,
            "first_global_seq": r.global_seq,
            "last_stream_seq": r.stream_seq,
            "last_stream_hash": r.stream_hash,
            "last_global_seq": r.global_seq,
        })
        if r.stream_seq < b["first_stream_seq"]:
            b["first_stream_seq"] = r.stream_seq
            b["first_stream_hash"] = r.prev_stream_hash
            b["first_global_seq"] = r.global_seq
        if r.stream_seq > b["last_stream_seq"]:
            b["last_stream_seq"] = r.stream_seq
            b["last_stream_hash"] = r.stream_hash
            b["last_global_seq"] = r.global_seq
    return out


def page_session(db: Session, session_id: str, *, operator: str,
                 cursor: str | None = None,
                 limit: int | None = None) -> dict:
    """沿会话翻一页: 固定边界读取 + 哈希链证明 + cursor 单调推进。

    - 不带 cursor: 仅允许会话的第一页;
    - 带 cursor: 必须与本会话上一页签发的游标完全一致(防漏页/跳页/重页)。
    """
    s = get_session(db, session_id)
    _require_creator(s, operator)
    if s.status == "BROKEN":
        raise EvidenceStateError(
            f"证据会话 {session_id} 已因断链锁定(BROKEN), 不能继续翻页: "
            f"{(s.broken_reason or {}).get('code')}; 请重新创建会话取证")
    lim = max(1, min(500, int(limit or default_page_limit())))
    with _evidence_lock:
        expected_page_no = s.pages_delivered  # 下一页是第 N+1 页
        if cursor is None:
            if s.status == "CLOSED":
                # 已翻完固定边界: 幂等回显终止空页(不报错, 也不暴露新事件)
                return _page_response(s, [], s.delivered_global_seq,
                                      has_more=False, operator=operator,
                                      persisted=False)
            if expected_page_no > 0:
                raise EvidenceChainBroken([_break(
                    "cursor_invalid",
                    f"会话已交付 {expected_page_no} 页, 后续翻页必须携带上一页 "
                    "签发的 next_cursor(防止漏页/重页)",
                    last_cursor=s.last_cursor)])
            after_gs = s.start_global_seq
        else:
            decoded = decode_cursor(cursor)
            if decoded is None:
                raise EvidenceChainBroken([_break(
                    "cursor_invalid", "游标签名无效或已损坏(非本服务签发)")])
            csid, cgs, ceid, cpage = decoded
            if csid != session_id:
                raise EvidenceChainBroken([_break(
                    "cursor_invalid", "游标属于另一个证据会话, 不能跨会话续页",
                    cursor_session=csid)])
            if cursor != s.last_cursor:
                raise EvidenceChainBroken([_break(
                    "cursor_invalid",
                    "游标不是本会话最近一页签发的游标(已过期或重复使用): "
                    f"游标页={cpage}, 已交付页数={expected_page_no}",
                    cursor_page=cpage, pages_delivered=expected_page_no,
                    last_cursor=s.last_cursor)])
            after_gs = cgs
        # 固定边界内读取(一次请求确定结果集; 新写入的 global_seq 超出 upper)
        rows = _query_scoped(db, s, after_gs=after_gs, limit=lim + 1)
        has_more = len(rows) > lim
        rows = rows[:lim]
        breaks = verify_fragment(db, s, rows, prev_delivered_gs=after_gs)
        if breaks:
            s.status = "BROKEN"
            s.broken_reason = breaks[0]
            s.broken_at = now_utc_naive()
            db.commit()  # BROKEN 状态与断链证据必须持久化
            raise EvidenceChainBroken(breaks)
        if not rows and not has_more:
            # 已到固定边界: 幂等返回空页, 会话关闭
            if s.status == "ACTIVE":
                s.status = "CLOSED"
                s.closed_at = now_utc_naive()
            db.commit()
            return _page_response(s, [], after_gs, has_more=False,
                                  operator=operator, persisted=False)
        digest = fragment_digest(rows)
        boundaries = _stream_boundaries(rows)
        last = rows[-1]
        page_no = expected_page_no + 1
        next_cursor = (encode_cursor(s.id, last.global_seq, last.id, page_no)
                       if has_more else None)
        db.add(EvidencePage(
            session_id=s.id, page_no=page_no,
            from_global_seq=rows[0].global_seq,
            to_global_seq=last.global_seq, event_count=len(rows),
            fragment_digest=digest, stream_boundaries=boundaries,
            created_by=operator))
        s.delivered_global_seq = last.global_seq
        s.delivered_event_id = last.id
        s.pages_delivered = page_no
        s.last_cursor = next_cursor
        if not has_more:
            s.status = "CLOSED"
            s.closed_at = now_utc_naive()
        db.commit()  # 游标推进/页面留痕必须跨请求持久化
        return _page_response(s, rows, after_gs, has_more=has_more,
                              operator=operator, persisted=True,
                              page_no=page_no, digest=digest,
                              boundaries=boundaries, next_cursor=next_cursor)


def _page_response(s: EvidenceSession, rows: list[AuditEvent], after_gs: int, *,
                   has_more: bool, operator: str, persisted: bool,
                   page_no: int | None = None, digest: str | None = None,
                   boundaries: dict | None = None,
                   next_cursor: str | None = None) -> dict:
    return {
        "session_id": s.id, "plan_id": s.plan_id,
        "page_no": page_no or (s.pages_delivered + 1),
        "filters": {
            "start_global_seq": s.start_global_seq,
            "start_ts": s.start_ts.isoformat() if s.start_ts else None,
            "end_ts": s.end_ts.isoformat() if s.end_ts else None,
            "event_types": s.event_types or [],
            "sources": s.sources or [],
            "streams": s.include_streams or [],
        },
        "boundary": {
            "upper_global_seq": s.upper_global_seq,
            "upper_event_ts": (s.upper_event_ts.isoformat()
                               if s.upper_event_ts else None),
            "fixed_at": s.created_at.isoformat() if s.created_at else None,
        },
        "cursor": {"last_delivered": s.delivered_global_seq,
                   "next_cursor": next_cursor, "has_more": has_more},
        "items": [event_view(r) for r in rows],
        "item_count": len(rows),
        "proof": {
            "status": "ok",
            "fragment_digest": digest,
            "stream_boundaries": boundaries or {},
            "prev_global_seq": after_gs,
            "verified_at": now_utc_naive().isoformat(),
        },
        "session_status": s.status,
        "pages_delivered": s.pages_delivered,
        "expected_count": s.expected_count,
    }


def event_view(r: AuditEvent) -> dict:
    """统一时间线事件视图: 事件来源/操作者/关联批次/补偿任务与流归属标注。"""
    stream_key = r.stream_key
    if stream_key == r.plan_id:
        flow = "plan"
    elif stream_key.startswith("batch:"):
        flow = "batch"
    elif stream_key == "global":
        flow = "global"
    else:
        flow = "other"
    payload = r.payload or {}
    return {
        "id": r.id, "global_seq": r.global_seq, "stream_key": stream_key,
        "stream_seq": r.stream_seq, "event_type": r.event_type,
        "flow": flow,
        "plan_id": r.plan_id, "batch_id": r.batch_id,
        "scan_id": r.scan_id, "step_seq": r.step_seq,
        "correlation_id": r.correlation_id,
        "prev_global_seq": r.prev_global_seq,
        "prev_stream_hash": r.prev_stream_hash,
        "stream_hash": r.stream_hash,
        "payload": payload, "operator": r.operator, "source": r.source,
        "event_ts": r.event_ts.isoformat() if r.event_ts else None,
        "comp_task_id": payload.get("task_id")
        if r.event_type in COMP_EVENT_TYPES else None,
        "comp_action_seq": payload.get("action_seq")
        if r.event_type in COMP_EVENT_TYPES else None,
        "is_compensation_control": r.event_type in COMP_EVENT_TYPES,
    }


def session_to_dict(s: EvidenceSession, *, with_pages: bool = False) -> dict:
    out = {
        "session_id": s.id, "plan_id": s.plan_id, "status": s.status,
        "filters": {
            "start_global_seq": s.start_global_seq,
            "start_ts": s.start_ts.isoformat() if s.start_ts else None,
            "end_ts": s.end_ts.isoformat() if s.end_ts else None,
            "event_types": s.event_types or [],
            "sources": s.sources or [],
            "streams": s.include_streams or [],
        },
        "boundary": {
            "upper_global_seq": s.upper_global_seq,
            "upper_event_id": s.upper_event_id,
            "upper_event_ts": (s.upper_event_ts.isoformat()
                               if s.upper_event_ts else None),
            "anchors": s.boundary_anchors or {},
        },
        "expected_count": s.expected_count,
        "expected_streams": s.expected_streams or [],
        "cursor": {
            "delivered_global_seq": s.delivered_global_seq,
            "last_cursor": s.last_cursor,
            "pages_delivered": s.pages_delivered,
        },
        "broken_reason": s.broken_reason,
        "broken_at": s.broken_at.isoformat() if s.broken_at else None,
        "closed_at": s.closed_at.isoformat() if s.closed_at else None,
        "created_by": s.created_by,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "exports": [export_to_dict(e, with_segments=False, with_events=False)
                    for e in sorted(s.exports, key=lambda x: x.created_at)],
    }
    if with_pages:
        out["pages"] = [{
            "page_no": p.page_no, "from_global_seq": p.from_global_seq,
            "to_global_seq": p.to_global_seq, "event_count": p.event_count,
            "fragment_digest": p.fragment_digest,
            "stream_boundaries": p.stream_boundaries,
            "created_by": p.created_by,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        } for p in sorted(s.pages, key=lambda x: x.page_no)]
    return out


# ======================================================================
# ---------- 证据导出: 创建 / 控制 / worker ----------
# ======================================================================

def _add_export_event(db: Session, export_id: str, event: str, operator: str,
                      *, segment_no: int | None = None,
                      reason: str | None = None,
                      detail: dict | None = None) -> None:
    db.add(EvidenceExportEvent(
        export_id=export_id, segment_no=segment_no, event=event,
        operator=operator, reason=(reason[:500] if reason else None),
        detail=detail))


def _existing_active_export(db: Session, session_id: str) -> EvidenceExport | None:
    return (db.query(EvidenceExport)
            .filter(EvidenceExport.session_id == session_id,
                    EvidenceExport.status.in_(
                        ("QUEUED", "RUNNING", "PAUSED")))
            .order_by(EvidenceExport.created_at.desc()).first())


def create_export(db: Session, *, operator: str, session_id: str,
                  segment_size: int | None = None) -> EvidenceExport:
    """基于证据会话创建异步分段导出任务并排队。

    会话必须 ACTIVE/CLOSED 且未断链; 同一会话同时至多一个活动导出
    (活动中重复创建幂等回显); COMPLETED/CANCELED 后允许重新导出。
    """
    s = get_session(db, session_id)
    _require_creator(s, operator)
    if s.status == "BROKEN":
        raise EvidenceStateError(
            f"证据会话 {session_id} 已断链(BROKEN), 不能导出证据包: "
            f"{(s.broken_reason or {}).get('code')}")
    size = max(1, min(1000, int(segment_size or default_segment_size())))
    with _evidence_lock:
        _lock_creation(db)
        existing = _existing_active_export(db, session_id)
        if existing is not None:
            return existing
        completed = (db.query(EvidenceExport)
                     .filter(EvidenceExport.session_id == session_id,
                             EvidenceExport.status == "COMPLETED")
                     .order_by(EvidenceExport.created_at.desc()).first())
        if completed is not None:
            # 已完成的不可变包: 重复创建幂等回显(同一边界结果完全一致)
            return completed
        total = s.expected_count
        total_segments = (total + size - 1) // size if total else 0
        eid = "EE" + uuid.uuid4().hex[:10]
        e = EvidenceExport(
            id=eid, session_id=session_id, plan_id=s.plan_id,
            status="QUEUED", segment_size=size, total_segments=total_segments,
            total_events=total, created_by=operator, updated_by=operator)
        db.add(e)
        db.flush()
        # 预登记分段行(段范围在创建时即固定, 是幂等重放的依据)
        for i in range(total_segments):
            lo_rank = i * size
            db.add(EvidenceExportSegment(
                export_id=eid, segment_no=i + 1, from_global_seq=0,
                to_global_seq=0, file_name=f"segments/segment-{i + 1:05d}.jsonl",
                status="PENDING"))
        _add_export_event(
            db, eid, "create", operator,
            reason=(f"创建证据导出: 会话 {session_id}(计划 {s.plan_id}), "
                    f"固定边界 upper_global_seq={s.upper_global_seq}, "
                    f"预期 {total} 个事件, {total_segments} 段(每段 {size} 个)"),
            detail={"session_id": session_id, "upper_global_seq": s.upper_global_seq,
                    "total_events": total, "segment_size": size,
                    "total_segments": total_segments})
        _add_export_event(db, eid, "queue", operator, reason="导出任务已排队")
        db.flush()
        return e


def _pause_export(db: Session, e: EvidenceExport, operator: str,
                  reason_code: str, message: str, *,
                  chain_break: dict | None = None) -> None:
    e.status = "PAUSED"
    e.paused_reason = reason_code
    if chain_break is not None:
        e.chain_break = chain_break
        e.failure_code = chain_break.get("code")
    e.updated_by = operator
    ev = "chain.paused" if reason_code == "chain_changed" else "pause"
    _add_export_event(db, e.id, ev, operator, reason=message,
                      detail={"reason_code": reason_code,
                              "chain_break": chain_break})


def do_pause(db: Session, e: EvidenceExport, operator: str) -> dict:
    if e.status == "PAUSED":
        return {"ok": True, "already_in_state": True,
                "detail": f"导出已暂停({e.paused_reason}), 重复暂停无副作用"}
    if e.status not in EVIDENCE_ALLOWED_PAUSE:
        raise EvidenceStateError(
            f"证据导出 {e.id} 当前状态 {e.status}, 不允许暂停")
    _pause_export(db, e, operator, "user_paused",
                  "用户请求暂停: 将在当前分段边界停住, 已完成分段保留")
    return {"ok": True, "detail": "导出已暂停, 将在分段边界停止"}


def do_resume(db: Session, e: EvidenceExport, operator: str) -> dict:
    if e.status in ("QUEUED", "RUNNING"):
        return {"ok": True, "already_in_state": True,
                "detail": f"导出当前为 {e.status}, 恢复请求无副作用"}
    if e.status not in EVIDENCE_ALLOWED_RESUME:
        raise EvidenceStateError(
            f"证据导出 {e.id} 当前状态 {e.status}, 不允许恢复")
    s = get_session(db, e.session_id)
    if e.status == "PAUSED" and e.paused_reason == "chain_changed":
        # 恢复前必须重新校验: 从固定边界起点全量重算各流哈希链
        breaks = reverify_full_scope(db, s)
        if breaks:
            raise EvidenceStateError(
                "事件链仍未通过一致性校验, 恢复被拒绝(断链证据已保留): "
                + "; ".join(b["message"] for b in breaks[:2]))
        # 链已恢复一致: 清除断链暂停标记
        e.chain_break = None
        e.failure_code = None
        _add_export_event(
            db, e.id, "chain.reverified", operator,
            reason="恢复前重新校验通过: 固定边界内全部流哈希链连续")
    # 续跑前逐段复算: 摘要失配/文件缺失/被篡改的已完成分段回到 PENDING 重生成
    # (段文件由原子替换覆盖, 不产生重复写包; 未受影响的段直接复用不重写)。
    invalidated = _revalidate_done_segments(db, s, e)
    e.status = "QUEUED"
    e.paused_reason = None
    e.failure_code = None
    e.failure_reason = None
    e.updated_by = operator
    _add_export_event(
        db, e.id, "resume", operator,
        reason=("恢复导出: 重新排队, 从上次成功分段继续"
                + (f"(重生成 {invalidated} 个失配分段)" if invalidated else "")))
    return {"ok": True,
            "detail": "导出已恢复并重新排队, 从上次成功分段继续",
            "regenerated_segments": invalidated}


def _revalidate_done_segments(db: Session, s: EvidenceSession,
                              e: EvidenceExport) -> int:
    """续跑前把已完成分段与当前库/文件重新核对; 失配分段回到 PENDING 重生成。

    覆盖三种不一致:
    1. 事件链恢复后段内容与库重算摘要不一致(链变化段) -> PENDING;
    2. 段文件缺失或字节摘要不匹配(被外部删除/篡改) -> PENDING;
    3. 正常完成的分段摘要/文件都匹配 -> 原样复用(失败重试不重复写包)。
    返回被标记重生成的段数。"""
    invalidated = 0
    done = {seg.segment_no: seg for seg in e.segments if seg.status == "DONE"}
    size = e.segment_size
    all_rows = _query_scoped(db, s, after_gs=s.start_global_seq, limit=1000000)
    if len(all_rows) != e.total_events:
        raise EvidenceStateError(
            f"固定边界事件数发生变化(创建时 {e.total_events}, 当前 "
            f"{len(all_rows)}), 不能恢复, 请基于新会话重新导出")
    for i in range(e.total_segments):
        seg = done.get(i + 1)
        if seg is None:
            continue
        chunk = all_rows[i * size:(i + 1) * size]
        digest = fragment_digest(chunk)
        path = os.path.join(store_dir(), e.id, seg.file_name)
        file_ok = (os.path.exists(path)
                   and seg.file_digest == hashlib.sha256(open(path, "rb").read())
                   .hexdigest())
        if digest != seg.fragment_digest or not file_ok:
            seg.status = "PENDING"
            seg.fragment_digest = None
            seg.file_digest = None
            seg.attempts += 1
            invalidated += 1
            _add_export_event(
                db, e.id, "segment.invalidated", "system",
                segment_no=seg.segment_no,
                reason="恢复前重校: 已完成分段与当前事件链或段文件不一致, "
                       "标记重新生成(原子替换, 不产生重复段)")
    done_nos = {seg.segment_no for seg in e.segments if seg.status == "DONE"}
    e.completed_segments = len(done_nos)
    e.exported_events = sum(seg.event_count for seg in e.segments
                            if seg.segment_no in done_nos)
    return invalidated


def do_cancel(db: Session, e: EvidenceExport, operator: str) -> dict:
    if e.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "导出已取消, 重复取消无副作用"}
    if e.status not in EVIDENCE_ALLOWED_CANCEL:
        raise EvidenceStateError(
            f"证据导出 {e.id} 当前状态 {e.status}(终态), 不允许取消")
    e.status = "CANCELED"
    e.paused_reason = None
    e.finished_at = now_utc_naive()
    e.updated_by = operator
    _add_export_event(db, e.id, "cancel", operator,
                      reason="导出被取消: 不生成完整证据包, 已完成分段记录保留可查")
    return {"ok": True, "detail": "导出已取消, 禁止继续, 已完成分段记录保留"}


def _fail_export(db: Session, e: EvidenceExport, code: str, reason: str,
                 operator: str = "system") -> None:
    if e.status in ("COMPLETED", "CANCELED"):
        db.rollback()
        return
    e.status = "FAILED"
    e.failure_code = code
    e.failure_reason = f"[{code}] {reason}"[:500]
    e.finished_at = now_utc_naive()
    e.updated_by = operator
    _add_export_event(db, e.id, "failed", operator, reason=e.failure_reason,
                      detail={"code": code,
                              "completed_segments": e.completed_segments,
                              "total_segments": e.total_segments})
    db.commit()


# ---------- 全量重校(恢复断链暂停前) ----------

def reverify_full_scope(db: Session, s: EvidenceSession) -> list[dict]:
    """从固定边界起点对全部涉及流重算哈希链(导出恢复前的完整证明)。"""
    rows = _query_scoped(db, s, after_gs=s.start_global_seq, limit=1000000)
    return verify_fragment(db, s, rows, prev_delivered_gs=s.start_global_seq)


# ---------- 调度(并发闸门) ----------

def claim_due_exports(db: Session) -> list[str]:
    """按并发额度把 QUEUED 导出认领为 RUNNING(已提交)。"""
    _lock_scheduling(db)
    try:
        running = (db.query(func.count(EvidenceExport.id))
                   .filter(EvidenceExport.status == "RUNNING").scalar()) or 0
        slots = max_concurrency() - running
        if slots <= 0:
            db.rollback()
            return []
        due = (db.query(EvidenceExport)
               .filter(EvidenceExport.status == "QUEUED")
               .order_by(EvidenceExport.created_at, EvidenceExport.id)
               .limit(slots).all())
        claimed: list[str] = []
        at = now_utc_naive()
        for e in due:
            e.status = "RUNNING"
            e.started_at = e.started_at or at
            e.updated_by = "system"
            _add_export_event(db, e.id, "claim", "system",
                              reason=f"获得并发额度, 开始分段生成(上限 {max_concurrency()})")
            claimed.append(e.id)
        db.commit()
        return claimed
    except Exception:
        db.rollback()
        raise


def _segment_rows(db: Session, s: EvidenceSession, size: int,
                  seg_no: int) -> list[AuditEvent]:
    """固定边界下第 seg_no 段(从 1 起)的事件; 用 offset/limit 保证切分稳定。"""
    q = (db.query(AuditEvent)
         .filter(AuditEvent.stream_key.in_(s.include_streams or []),
                 AuditEvent.global_seq > s.start_global_seq,
                 AuditEvent.global_seq <= s.upper_global_seq))
    if s.start_ts:
        q = q.filter(AuditEvent.event_ts >= s.start_ts)
    if s.end_ts:
        q = q.filter(AuditEvent.event_ts <= s.end_ts)
    if s.event_types:
        q = q.filter(AuditEvent.event_type.in_(s.event_types))
    if s.sources:
        q = q.filter(AuditEvent.source.in_(s.sources))
    return (q.order_by(AuditEvent.global_seq.asc(), AuditEvent.id.asc())
            .offset((seg_no - 1) * size).limit(size).all())


def _write_segment_file(export_id: str, seg: EvidenceExportSegment,
                        rows: list[AuditEvent]) -> tuple[bytes, int]:
    """确定性写段文件(JSONL): 先 .tmp 再原子替换; 同输入字节级一致。"""
    ensure_store_dir()
    d = os.path.join(store_dir(), export_id, "segments")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(store_dir(), export_id, seg.file_name)
    blob = b"".join(canonical_event_bytes(r) for r in rows)
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:8]}"
    with open(tmp, "wb") as f:
        f.write(blob)
    os.replace(tmp, path)
    return blob, len(blob)


def run_export_tick(db: Session, export_id: str) -> bool:
    """推进一个 RUNNING 导出至多一个分段, 返回是否执行了分段。"""
    e = db.get(EvidenceExport, export_id)
    if e is None or e.status != "RUNNING":
        db.rollback()
        return False
    s = get_session(db, e.session_id)
    seg = next((x for x in sorted(e.segments, key=lambda x: x.segment_no)
                if x.status != "DONE"), None)
    # 空结果边界: 无分段, 直接进入打包(只产出元信息/manifest)
    if seg is None:
        try:
            _package_export(db, s, e)
            return True
        except _PackageChainChanged as ex:
            db.rollback()
            _pause_export(db, e, "system", "chain_changed", ex.args[0],
                          chain_break=ex.break_info)
            db.commit()
            return True
        except EvidenceExportFatal as ex:
            db.rollback()
            _fail_export(db, e, ex.code, ex.reason)
            return True
        except Exception as ex:  # 防御: 意外错误也明确失败留痕(可重试)
            db.rollback()
            _fail_export(db, e, "internal_error", f"打包时未预期错误: {ex}")
            return True
    # 生成分段前再次确认控制状态(pause/cancel 在分段边界生效)
    e = db.get(EvidenceExport, export_id)
    if e.status != "RUNNING":
        db.rollback()
        return False
    e.current_segment = seg.segment_no
    seg.attempts += 1
    _add_export_event(db, e.id, "segment.start", "system",
                      segment_no=seg.segment_no)
    db.commit()  # 分段边界: 先留痕, 崩溃后从同一未完成分段重跑(幂等)

    try:
        rows = _segment_rows(db, s, e.segment_size, seg.segment_no)
        if not rows:
            raise EvidenceExportFatal(
                "segment_empty",
                f"分段 {seg.segment_no} 读不到事件(固定边界 {e.total_events} "
                "个事件), 导出依据与会话不一致")
        # 该段自身的链证明(段内 + 前边界 + 固定边界锚点)
        prev_gs = (rows[0].global_seq - 1 if seg.segment_no == 1
                   else _prev_segment_last_gs(db, e, seg.segment_no))
        breaks = verify_fragment(db, s, rows, prev_delivered_gs=prev_gs)
        if breaks:
            info = {"code": breaks[0]["code"], "at_segment": seg.segment_no,
                    **breaks[0]}
            raise _SegmentChainChanged(
                f"导出期间检测到事件链变化: {breaks[0]['message']}", info)
        blob, size_bytes = _write_segment_file(export_id, seg, rows)
        digest = fragment_digest(rows)
        file_digest = hashlib.sha256(blob).hexdigest()
        seg = db.get(EvidenceExportSegment, seg.id)
        seg.status = "DONE"
        seg.from_global_seq = rows[0].global_seq
        seg.to_global_seq = rows[-1].global_seq
        seg.event_count = len(rows)
        seg.file_size = size_bytes
        seg.fragment_digest = digest
        seg.file_digest = file_digest
        seg.stream_boundaries = _stream_boundaries(rows)
        done_nos = {x.segment_no for x in e.segments if x.status == "DONE"}
        e.completed_segments = len(done_nos)
        e.exported_events = sum(x.event_count for x in e.segments
                                if x.segment_no in done_nos)
        e.first_global_seq = min(x for x in [e.first_global_seq, rows[0].global_seq]
                                 if x is not None)
        e.last_global_seq = max(x for x in [e.last_global_seq, rows[-1].global_seq]
                                if x is not None)
        e.current_segment = None
        _add_export_event(
            db, e.id, "segment.done", "system", segment_no=seg.segment_no,
            reason=f"分段 {seg.segment_no} 完成: {len(rows)} 个事件, "
                   f"global_seq {rows[0].global_seq}..{rows[-1].global_seq}, "
                   f"摘要 {digest[:16]}…",
            detail={"segment_no": seg.segment_no, "event_count": len(rows),
                    "from_global_seq": rows[0].global_seq,
                    "to_global_seq": rows[-1].global_seq,
                    "fragment_digest": digest, "file_digest": file_digest})
        db.commit()
        return True
    except _SegmentChainChanged as ex:
        db.rollback()
        e = db.get(EvidenceExport, export_id)
        _pause_export(db, e, "system", "chain_changed", str(ex),
                      chain_break=ex.info)
        db.commit()
        return True
    except EvidenceExportFatal as ex:
        db.rollback()
        _fail_export(db, db.get(EvidenceExport, export_id), ex.code, ex.reason)
        return True
    except Exception as ex:  # 防御: 未预期错误 FAILED(可 resume, 已完成段不重写)
        db.rollback()
        _fail_export(db, db.get(EvidenceExport, export_id), "internal_error",
                     f"生成分段 {seg.segment_no} 时未预期错误: {ex}")
        return True


def _prev_segment_last_gs(db: Session, e: EvidenceExport,
                          seg_no: int) -> int:
    prev = (db.query(EvidenceExportSegment)
            .filter(EvidenceExportSegment.export_id == e.id,
                    EvidenceExportSegment.segment_no == seg_no - 1).first())
    return prev.to_global_seq if prev and prev.to_global_seq else 0


class _SegmentChainChanged(Exception):
    def __init__(self, msg: str, info: dict):
        super().__init__(msg)
        self.info = info


class _PackageChainChanged(Exception):
    def __init__(self, msg: str, info: dict):
        super().__init__(msg)
        self.break_info = info


# ---------- 打包: 确定性 JSONL zip + manifest ----------

def _canonical_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2,
                       default=str) + "\n").encode("utf-8")


def _stream_summaries(db: Session, s: EvidenceSession,
                      rows: list[AuditEvent]) -> dict[str, dict]:
    """每条流的摘要: 行 sha256 有序拼接 + 起止 stream_seq/global_seq。"""
    grouped: dict[str, list[AuditEvent]] = {}
    for r in rows:
        grouped.setdefault(r.stream_key, []).append(r)
    out: dict[str, dict] = {}
    for sk in sorted(grouped):
        srows = sorted(grouped[sk], key=lambda x: (x.global_seq, x.id))
        h = hashlib.sha256()
        for r in srows:
            h.update(canonical_event_bytes(r))
        seqs = sorted(r.stream_seq for r in srows)
        gseqs = sorted(r.global_seq for r in srows)
        comp_count = sum(1 for r in srows if r.event_type in COMP_EVENT_TYPES)
        out[sk] = {
            "kind": ("plan" if sk == s.plan_id else
                     "batch" if sk.startswith("batch:") else
                     "global" if sk == "global" else "other"),
            "event_count": len(srows),
            "first_stream_seq": seqs[0], "last_stream_seq": seqs[-1],
            "first_global_seq": gseqs[0], "last_global_seq": gseqs[-1],
            "comp_control_events": comp_count,
            "stream_digest": h.hexdigest(),
            "operators": sorted({r.operator for r in srows if r.operator}),
            "batch_ids": sorted({r.batch_id for r in srows if r.batch_id}),
        }
    return out


def _content_digest(files: dict[str, bytes]) -> tuple[str, dict[str, dict]]:
    """逐文件 sha256 + 内容摘要(文件名有序拼接, 仿 git 风格)。"""
    meta = {}
    h = hashlib.sha256()
    for name in sorted(files):
        blob = files[name]
        digest = hashlib.sha256(blob).hexdigest()
        meta[name] = {"size": len(blob), DIGEST_ALGORITHM: digest}
        h.update(f"{digest}  {name}\n".encode("utf-8"))
    return h.hexdigest(), meta


def _write_zip(path: str, files: dict[str, bytes], manifest: dict) -> None:
    """确定性 zip: 固定时间戳、文件名排序写入, 先 .tmp 再原子替换。"""
    ensure_store_dir()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in sorted(list(files) + ["manifest.json"]):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                blob = files[name] if name in files else _canonical_bytes(manifest)
                zf.writestr(info, blob)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _package_export(db: Session, s: EvidenceSession,
                    e: EvidenceExport) -> None:
    """收尾打包: 全量重校 -> 组装段文件/元信息/清单 -> 摘要 -> 写 zip -> 自检。"""
    all_rows = _query_scoped(db, s, after_gs=s.start_global_seq, limit=1000000)
    # 打包前最后一次全量证明: 任何链变化都不能产出"完整"包
    breaks = verify_fragment(db, s, all_rows,
                             prev_delivered_gs=s.start_global_seq)
    if breaks:
        raise _PackageChainChanged(
            f"打包前检测到事件链变化: {breaks[0]['message']}",
            {"code": breaks[0]["code"], "at_stage": "package", **breaks[0]})
    if len(all_rows) != e.total_events:
        raise EvidenceExportFatal(
            "count_mismatch",
            f"固定边界事件数变化: 创建时 {e.total_events}, 当前 {len(all_rows)}")
    done_segs = sorted(e.segments, key=lambda x: x.segment_no)
    # 段摘要必须与全量数据重新切分的摘要一致(防文件/段落库后被改)
    files: dict[str, bytes] = {}
    segment_manifest = []
    for i, seg in enumerate(done_segs):
        chunk = all_rows[i * e.segment_size:(i + 1) * e.segment_size]
        digest = fragment_digest(chunk)
        if seg.fragment_digest != digest:
            raise EvidenceExportFatal(
                "segment_digest_mismatch",
                f"分段 {seg.segment_no} 摘要与当前数据不一致, 请恢复后重新生成该段")
        path = os.path.join(store_dir(), e.id, seg.file_name)
        if not os.path.exists(path):
            raise EvidenceExportFatal(
                "segment_file_missing",
                f"分段 {seg.segment_no} 文件缺失: {path}")
        with open(path, "rb") as f:
            blob = f.read()
        if hashlib.sha256(blob).hexdigest() != seg.file_digest:
            raise EvidenceExportFatal(
                "segment_file_tampered",
                f"分段 {seg.segment_no} 文件摘要不一致(文件被篡改)")
        files[seg.file_name] = blob
        segment_manifest.append({
            "segment_no": seg.segment_no, "file": seg.file_name,
            "event_count": seg.event_count,
            "from_global_seq": seg.from_global_seq,
            "to_global_seq": seg.to_global_seq,
            "size": seg.file_size,
            "fragment_digest": seg.fragment_digest,
            "file_digest": seg.file_digest,
        })
    stream_summaries = _stream_summaries(db, s, all_rows)
    metadata = {
        "export_id": e.id, "session_id": s.id, "plan_id": s.plan_id,
        "digest_algorithm": DIGEST_ALGORITHM,
        "created_by": e.created_by, "session_created_at": _iso(s.created_at),
        "boundary_fixed_at": _iso(s.created_at),
        "filters": session_to_dict(s)["filters"],
        "boundary": session_to_dict(s)["boundary"],
        "segment_size": e.segment_size,
        "event_count": len(all_rows),
        "first_global_seq": (all_rows[0].global_seq if all_rows else None),
        "last_global_seq": (all_rows[-1].global_seq if all_rows else None),
        "contents": ["metadata.json", "events.jsonl"] + sorted(
            seg["file"] for seg in segment_manifest) + ["manifest.json"],
        "immutable": True,
    }
    # events.jsonl: 全量事件(与逐段拼接字节一致)
    events_blob = b"".join(canonical_event_bytes(r) for r in all_rows)
    files["events.jsonl"] = events_blob
    files["metadata.json"] = _canonical_bytes(metadata)
    content_digest, file_meta = _content_digest(files)
    manifest = {
        "export_id": e.id, "session_id": s.id, "plan_id": s.plan_id,
        "digest_algorithm": DIGEST_ALGORITHM,
        "generated_at": _iso(s.created_at),  # 固定为边界创建时刻, 保证确定性
        "event_count": len(all_rows),
        "start_global_seq": (s.start_global_seq
                             if s.start_global_seq else None),
        "first_global_seq": (all_rows[0].global_seq if all_rows else None),
        "last_global_seq": (all_rows[-1].global_seq if all_rows else None),
        "upper_global_seq": s.upper_global_seq,
        "segment_size": e.segment_size,
        "segments": segment_manifest,
        "streams": stream_summaries,
        "files": file_meta,
        "content_digest": content_digest,
    }
    manifest_hash = _hash_obj({k: v for k, v in manifest.items()
                               if k != "manifest_hash"})
    manifest["manifest_hash"] = manifest_hash
    path = os.path.join(store_dir(), e.id, f"evidence-{e.id}.zip")
    _write_zip(path, files, manifest)
    check = verify_package_bytes(
        path, expected_manifest=manifest,
        expected_content_digest=content_digest,
        expected_manifest_hash=manifest_hash)
    if not check["valid"]:
        raise EvidenceExportFatal(
            "digest_mismatch",
            f"证据包写出后自检失败: {'; '.join(check['issues'])}")
    e.status = "COMPLETED"
    e.package_path = path
    e.package_size = os.path.getsize(path)
    e.manifest = manifest
    e.content_digest = content_digest
    e.manifest_hash = manifest_hash
    e.completed_segments = e.total_segments
    e.current_segment = None
    e.paused_reason = None
    e.chain_break = None
    e.failure_code = None
    e.failure_reason = None
    e.finished_at = now_utc_naive()
    e.updated_by = "system"
    _add_export_event(
        db, e.id, "complete", "system",
        reason=(f"证据包完成: {len(all_rows)} 个事件, "
                f"{e.total_segments} 段, manifest_hash={manifest_hash[:16]}…, "
                "可签发一次性下载令牌"),
        detail={"package_path": path, "package_size": e.package_size,
                "content_digest": content_digest,
                "manifest_hash": manifest_hash,
                "event_count": len(all_rows),
                "stream_count": len(stream_summaries)})
    db.commit()


def _iso(v) -> str | None:
    return v.isoformat() if isinstance(v, datetime) else v


# ---------- 包校验 / 一次性下载 ----------

def verify_package_bytes(path: str, *, expected_manifest: dict | None = None,
                         expected_content_digest: str | None = None,
                         expected_manifest_hash: str | None = None) -> dict:
    """回读 zip 重算逐文件/内容/manifest 摘要并比对。返回校验结果(不抛异常)。"""
    issues: list[str] = []
    if not os.path.exists(path):
        return {"valid": False, "issues": ["证据包文件不存在(可能已被删除)"],
                "reason_code": "package_missing", "files": {}}
    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                issues.append(f"zip 压缩包损坏, 首个坏块: {bad}")
            names = set(zf.namelist())
            blobs: dict[str, bytes] = {n: zf.read(n) for n in names}
    except zipfile.BadZipFile:
        return {"valid": False, "issues": ["证据包不是合法 zip(已损坏或被篡改)"],
                "reason_code": "package_corrupt", "files": {}}
    manifest = expected_manifest
    if manifest is None and "manifest.json" in blobs:
        try:
            manifest = json.loads(blobs["manifest.json"].decode("utf-8"))
        except Exception as ex:
            issues.append(f"manifest.json 无法解析: {ex}")
    files_check: dict[str, dict] = {}
    recomputed_content = None
    payload = {n: b for n, b in blobs.items() if n != "manifest.json"}
    if payload:
        recomputed_content, _ = _content_digest(payload)
    for name in sorted(names - {"manifest.json"}):
        actual = hashlib.sha256(blobs[name]).hexdigest()
        expect = None
        if isinstance(manifest, dict):
            expect = (manifest.get("files") or {}).get(name, {}).get(
                DIGEST_ALGORITHM)
        ok = expect is None or actual == expect
        if not ok:
            issues.append(f"文件 {name} 摘要不一致")
        files_check[name] = {"ok": ok, "expected": expect, "actual": actual}
    want = expected_content_digest
    if want is None and isinstance(manifest, dict):
        want = manifest.get("content_digest")
    if want is not None and recomputed_content != want:
        issues.append("内容摘要(content_digest)重算不一致")
    if isinstance(manifest, dict):
        stored_mh = manifest.get("manifest_hash")
        recomputed_mh = _hash_obj({k: v for k, v in manifest.items()
                                   if k != "manifest_hash"})
        want_mh = expected_manifest_hash or stored_mh
        if want_mh and want_mh != recomputed_mh:
            issues.append("manifest 整体摘要(manifest_hash)重算不一致")
    valid = not issues
    return {"valid": valid, "issues": issues,
            "reason_code": None if valid else "digest_mismatch",
            "content_digest": want,
            "recomputed_content_digest": recomputed_content,
            "manifest_hash": (manifest or {}).get("manifest_hash"),
            "files": files_check}


def verify_export(db: Session, e: EvidenceExport,
                  operator: str = "system") -> dict:
    """回读证据包重算逐段/逐流/manifest 摘要并比对。包缺失/摘要不一致 -> FAILED。"""
    if e.status != "COMPLETED" or not e.package_path:
        raise EvidenceStateError(
            f"证据导出 {e.id} 当前状态 {e.status}, 尚无证据包可校验")
    result = verify_package_bytes(
        e.package_path, expected_manifest=e.manifest,
        expected_content_digest=e.content_digest,
        expected_manifest_hash=e.manifest_hash)
    result["export_id"] = e.id
    result["status"] = e.status
    if result["valid"]:
        _add_export_event(db, e.id, "verify.ok", operator,
                          reason=f"摘要校验通过: manifest_hash={e.manifest_hash[:16]}…")
        db.commit()
        return result
    reason = "; ".join(result["issues"])
    e.status = "FAILED"
    e.failure_code = result.get("reason_code") or "digest_mismatch"
    e.failure_reason = f"[{e.failure_code}] {reason}"[:500]
    _add_export_event(db, e.id, "verify.failed", operator,
                      reason=f"摘要校验失败: {reason}(原包保留取证)")
    db.commit()
    result["status"] = "FAILED"
    return result


def issue_download_token(db: Session, e: EvidenceExport,
                         operator: str) -> EvidenceDownload:
    """为 COMPLETED 证据包签发一次性下载令牌(仅会话创建者)。"""
    if e.status != "COMPLETED":
        raise EvidenceStateError(
            f"证据导出 {e.id} 当前状态 {e.status}, 包未完成, 不能下载")
    s = get_session(db, e.session_id)
    _require_creator(s, operator)
    raw = uuid.uuid4().hex + uuid.uuid4().hex
    dl = EvidenceDownload(
        id="ED" + uuid.uuid4().hex[:10],
        token_hash=hashlib.sha256(raw.encode()).hexdigest(),
        export_id=e.id, session_id=e.session_id, issued_by=operator,
        expires_at=now_utc_naive() + timedelta(seconds=download_ttl_seconds()))
    db.add(dl)
    _add_export_event(db, e.id, "download.issue", operator,
                      reason="签发一次性下载令牌",
                      detail={"download_id": dl.id,
                              "expires_at": dl.expires_at.isoformat()})
    db.flush()
    dl.token_plain = raw  # 仅本次响应返回明文
    return dl


def redeem_download_token(db: Session, token: str,
                          operator: str | None = None) -> tuple[EvidenceExport, str]:
    """兑换一次性下载令牌: 首次使用即标记 used, 重复/过期/不存在 -> 404/409。"""
    th = hashlib.sha256(token.encode()).hexdigest()
    dl = db.query(EvidenceDownload).filter(
        EvidenceDownload.token_hash == th).first()
    if dl is None:
        raise EvidenceNotFound("下载令牌无效(不存在或已被使用)")
    if dl.used_at is not None:
        raise EvidenceStateError(
            f"下载令牌已于 {dl.used_at.isoformat()} 被 {dl.used_by} 使用"
            "(一次性令牌, 请重新签发)")
    if dl.expires_at and now_utc_naive() > dl.expires_at:
        raise EvidenceStateError("下载令牌已过期, 请重新签发")
    e = get_export(db, dl.export_id)
    if e.status != "COMPLETED" or not e.package_path:
        raise EvidenceStateError("证据包当前不可下载(导出已不是 COMPLETED 状态)")
    if not os.path.exists(e.package_path):
        e.status = "FAILED"
        e.failure_code = "package_missing"
        e.failure_reason = "[package_missing] 证据包文件已不存在"
        db.commit()
        raise EvidenceStateError("证据包文件缺失, 导出已标记 FAILED")
    dl.used_at = now_utc_naive()
    dl.used_by = operator or dl.issued_by
    e.download_count += 1
    _add_export_event(db, e.id, "download.redeem", dl.used_by,
                      reason="一次性下载令牌已兑换")
    db.commit()
    return e, e.package_path


# ---------- 视图 ----------

def export_to_dict(e: EvidenceExport, *, with_segments: bool = True,
                   with_events: bool = False) -> dict:
    out = {
        "export_id": e.id, "session_id": e.session_id, "plan_id": e.plan_id,
        "status": e.status,
        "segment_size": e.segment_size,
        "total_segments": e.total_segments,
        "completed_segments": e.completed_segments,
        "current_segment": e.current_segment,
        "total_events": e.total_events,
        "exported_events": e.exported_events,
        "first_global_seq": e.first_global_seq,
        "last_global_seq": e.last_global_seq,
        "paused_reason": e.paused_reason,
        "chain_break": e.chain_break,
        "failure_code": e.failure_code,
        "failure_reason": e.failure_reason,
        "package_size": e.package_size,
        "content_digest": e.content_digest,
        "manifest_hash": e.manifest_hash,
        "download_count": e.download_count,
        "created_by": e.created_by,
        "started_at": e.started_at.isoformat() if e.started_at else None,
        "finished_at": e.finished_at.isoformat() if e.finished_at else None,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "manifest": ({
            "event_count": (e.manifest or {}).get("event_count"),
            "first_global_seq": (e.manifest or {}).get("first_global_seq"),
            "last_global_seq": (e.manifest or {}).get("last_global_seq"),
            "streams": (e.manifest or {}).get("streams"),
            "segments": (e.manifest or {}).get("segments"),
            "content_digest": (e.manifest or {}).get("content_digest"),
            "manifest_hash": (e.manifest or {}).get("manifest_hash"),
        } if e.manifest else None),
    }
    if with_segments:
        out["segments"] = [{
            "segment_no": x.segment_no, "file": x.file_name,
            "status": x.status, "event_count": x.event_count,
            "from_global_seq": x.from_global_seq,
            "to_global_seq": x.to_global_seq, "file_size": x.file_size,
            "fragment_digest": x.fragment_digest,
            "file_digest": x.file_digest, "attempts": x.attempts,
        } for x in sorted(e.segments, key=lambda x: x.segment_no)]
    if with_events:
        out["events"] = [{
            "id": x.id, "ts": x.ts.isoformat() if x.ts else None,
            "segment_no": x.segment_no, "event": x.event,
            "operator": x.operator, "reason": x.reason, "detail": x.detail,
        } for x in sorted(e.events, key=lambda x: x.id)]
    return out


# ---------- 幂等框架(evidence.* 命名空间) ----------

def run_evidence_action(db: Session, *, action: str, operator: str,
                        idempotency_key: str, payload: dict, fn,
                        session_id: str | None = None,
                        export_id: str | None = None) -> tuple[dict, bool]:
    req_hash = _hash_obj({"action": f"{EVIDENCE_NAMESPACE}.{action}",
                          "session_id": session_id, "export_id": export_id,
                          "payload": payload})
    existing = db.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise EvidenceStateError("幂等键被不同请求复用")
        return existing.response_json, True
    result = fn(db)
    if export_id is not None:
        fresh = get_export(db, export_id)
        result["export_id"] = export_id
        result["status"] = fresh.status
    elif session_id is not None:
        fresh = get_session(db, session_id)
        result["session_id"] = session_id
        result["status"] = fresh.status
    db.add(IdempotencyKey(key=idempotency_key,
                          action=f"{EVIDENCE_NAMESPACE}.{action}",
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


# ---------- 重启对账 ----------

def boot_recover_exports(db: Session) -> None:
    """重启对账: 遗留 RUNNING 导出回到 QUEUED(分段是崩溃边界, 已完成分段
    与摘要保留, worker 从首个未完成分段续跑); RUNNING 分段的 PENDING 行
    保持 PENDING(段文件为原子替换, 重跑覆盖不产生重复写包)。"""
    rows = db.query(EvidenceExport).order_by(EvidenceExport.id).all()
    for e in rows:
        if e.status != "RUNNING":
            continue
        e.status = "QUEUED"
        e.current_segment = None
        _add_export_event(
            db, e.id, "boot.reset", "system",
            reason="重启恢复: RUNNING 导出不可能跨进程存活, 回到排队位置, "
                   "已完成分段与摘要保留, 从首个未完成分段续跑")
        db.commit()


# ---------- 后台 worker ----------

class EvidenceExportWorker:
    """单实例后台线程: 认领 QUEUED 证据导出, 每个 tick 至多推进一个分段。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("EVIDENCE_WORKER_POLL_INTERVAL",
                                                   "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop,
                                        name="evidence-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        db = SessionLocal()
        try:
            claimed = claim_due_exports(db)
            # 已认领(RUNNING)的导出每个 tick 继续推进一个分段, 直到完成/暂停
            running = [r[0] for r in (db.query(EvidenceExport.id)
                                      .filter(EvidenceExport.status == "RUNNING")
                                      .order_by(EvidenceExport.id).all())]
        finally:
            db.close()
        advanced = 0
        for eid in list(claimed) + [i for i in running if i not in claimed]:
            db = SessionLocal()
            try:
                if run_export_tick(db, eid):
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


# 延迟导入避免模块加载期循环依赖
from .db import SessionLocal  # noqa: E402
