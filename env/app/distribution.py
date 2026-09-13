"""证据封存分发与离线校验: 在已归档(ARCHIVED)复核单之上创建脱敏分发包。

设计要点
========
1. 分发包(evidence_distributions): 管理员**只能**从一张已归档复核单创建。
   package_id 是固定标识 —— 由复核单不可变签署摘要(review.signature_hash)+
   接收方+脱敏策略+签发序号+有效期派生(sha256 前 24 位), 不含时间随机量:
   同请求(含同幂等键)永远得到同一 package_id; 撤销后重新签发因 issue_no+1
   得到新的 package_id, 旧包记录与文件原样保留。
2. 包内容在创建时刻一次性固化: 只包含该复核单**固定范围内**的事件
   (创建时与归档 scope_fingerprint/事件数交叉核对, 不一致拒绝创建),
   经脱敏后逐行写入 events.jsonl, 并生成 signature.json(逐事件签署,
   脱敏策略下签署人按包内假名呈现)/metadata.json/manifest.json。
   content_digest 为逐文件 sha256 有序拼接; manifest_hash 为清单整体摘要;
   signature_digest 为**带接收方约束**的 HMAC-SHA256(密钥只在服务端)。
   包生成后与原复核单/事件链再无耦合: 撤销、原数据变化都不改变已生成的包。
3. 一次性下载令牌(evidence_distribution_downloads): 明文仅签发当次返回,
   库内只存 token 散列; 令牌绑定接收方(bound_recipient), 兑换时操作者必须
   是该接收方; 首次兑换成功立即失效; 包撤销/过期/令牌过期/接收方被停用
   一律拒绝下载。
4. 独立离线校验:
   - GET  .../distributions/{package_id}/verify 重算服务端留存包;
   - POST .../distributions/verify 接收任意 zip 字节(纯离线, 无需鉴权),
   逐项重算逐行/逐文件/content_digest/manifest_hash/signature_digest 并与
   包内 manifest 比对, 返回机器可读的篡改位置(tampering: 代码+目标+期望值/
   实际值)。撤销/过期只影响"能否下载", 不影响密码学校验结论。
5. 幂等: 创建/撤销/令牌签发/接收方登记全部走幂等框架
   (evidence.distribution.* 命名空间), 同键重放返回首次结果;
   撤销不修改原始事件、复核结论或已归档签署摘要(只写本模块自身的表)。
6. 并发: 复用 evidence._evidence_lock 进程内栅栏 + 数据库唯一约束兜底。
"""
import hashlib
import hmac
import io
import json
import os
import re
import threading
import uuid
import zipfile
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, object_session

from . import evidence, reviews
from .models import (
    EVIDENCE_EXTENSION_REQUIRED_APPROVALS, EVIDENCE_RECEIPT_EVENT_RESULTS,
    EVIDENCE_RECEIPT_TYPES, EVIDENCE_REDACTION_POLICIES, EvidenceDistribution,
    EvidenceDistributionAssignment, EvidenceDistributionDownload,
    EvidenceDistributionEvent, EvidenceDistributionExtension,
    EvidenceDistributionExtensionApproval, EvidenceDistributionReceipt,
    EvidenceDistributionReceiptEvent, EvidenceRecipient, EvidenceReview,
    IdempotencyKey,
)

DISTRIBUTION_NAMESPACE = "evidence.distribution"
DIGEST_ALGORITHM = evidence.DIGEST_ALGORITHM

# 脱敏标记: 固定字符串(不依赖被脱敏值, 保证同策略包字节确定)
REDACTED = "«REDACTED»"
# STANDARD 策略下 payload 内被视为"说明类"而隐藏的键(大小写不敏感)
_SENSITIVE_PAYLOAD_KEYS = {
    "reason", "note", "notes", "detail", "details", "message", "comment",
    "comments", "description", "operator", "operator_name", "operated_by",
    "remark", "remarks",
}
_RECIPIENT_RE = re.compile(r"^[A-Za-z0-9_.@\-]+$")

# 分发包 / 接收方 / 令牌不存在 -> 404(复用证据域语义)
DistributionNotFound = evidence.EvidenceNotFound


class DistributionStateError(Exception):
    """状态不允许 / 权限不符 / 幂等键复用 / 接收方非法 -> 409/403。"""

    def __init__(self, reason: str, code: str = "distribution_conflict",
                 extra: dict | None = None):
        super().__init__(reason)
        self.code = code
        self.extra = extra or {}


class DistributionForbidden(Exception):
    """已认证但不是授权接收方 -> 403。"""

    def __init__(self, reason: str, code: str = "recipient_forbidden"):
        super().__init__(reason)
        self.code = code


# 与证据域共用同一把进程内栅栏
_lock = evidence._evidence_lock


# ---------- 配置 ----------

def _now() -> datetime:
    return evidence.now_utc_naive()


def store_dir() -> str:
    d = os.getenv("EVIDENCE_DISTRIBUTION_STORE_DIR",
                  os.getenv("EVIDENCE_STORE_DIR",
                            os.path.join(".", "evidence_store")))
    return os.path.abspath(d)


def ensure_store_dir() -> None:
    os.makedirs(store_dir(), exist_ok=True)


def default_ttl_seconds() -> int:
    try:
        return max(60, int(os.getenv("EVIDENCE_DISTRIBUTION_TTL_SECONDS",
                                     str(7 * 86400))))
    except ValueError:
        return 7 * 86400


def download_ttl_seconds() -> int:
    try:
        return max(30, int(os.getenv("EVIDENCE_DISTRIBUTION_DOWNLOAD_TTL_SECONDS",
                                     os.getenv("EVIDENCE_DOWNLOAD_TTL_SECONDS",
                                               "600"))))
    except ValueError:
        return 600


def _signing_secret() -> bytes:
    """signature_digest 的 HMAC 密钥: 显式配置优先, 否则由数据库 URL 派生
    (单副本部署; 更换密钥只让历史包的服务端离线签名复算失效, 不影响其余摘要)。"""
    sec = os.getenv("EVIDENCE_DISTRIBUTION_SIGNING_KEY")
    if sec:
        return sec.encode("utf-8")
    from .db import DATABASE_URL
    return hashlib.sha256(
        ("evidence-distribution-sign:" + DATABASE_URL).encode()).digest()


# ---------- 规范化 / 摘要 ----------

def _iso(v) -> str | None:
    return v.isoformat() if isinstance(v, datetime) else v


def _canonical_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2,
                       default=str) + "\n").encode("utf-8")


def _compact_json_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), default=str) + "\n")\
        .encode("utf-8")


def _hash_obj(payload: dict) -> str:
    return hashlib.sha256(_compact_json_bytes(payload)).hexdigest()


def _hmac_sign(payload: dict) -> str:
    return hmac.new(_signing_secret(), _compact_json_bytes(payload),
                    hashlib.sha256).hexdigest()


def _line_digest(line: bytes) -> str:
    return hashlib.sha256(line).hexdigest()


def _pseudonym(recipient: str, operator: str) -> str:
    """脱敏策略下签署人的包内假名: 同包同人稳定, 不暴露真实账号,
    仍可离线验证"两名不同签署人"。"""
    return "signer-" + hmac.new(
        _signing_secret(),
        f"signer:{recipient}:{operator}".encode("utf-8"),
        hashlib.sha256).hexdigest()[:10]


# ---------- 接收方名册 ----------

def normalize_recipient(recipient: str) -> str:
    r = (recipient or "").strip()
    if not r or len(r) > 64 or not _RECIPIENT_RE.match(r):
        raise DistributionStateError(
            f"非法接收方账号 {recipient!r}: 仅允许字母数字与 . _ @ - 且不超过 64 字符",
            code="invalid_recipient")
    return r


def register_recipient(db: Session, *, operator: str, recipient: str,
                       name: str | None = None,
                       contact: str | None = None) -> EvidenceRecipient:
    rid = normalize_recipient(recipient)
    with _lock:
        existing = db.get(EvidenceRecipient, rid)
        if existing is not None:
            if existing.status == "DISABLED":
                raise DistributionStateError(
                    f"接收方 {rid} 已被停用, 不能重新登记(历史包保留可查)",
                    code="recipient_disabled")
            return existing  # 已在册: 登记幂等, 不覆盖资料
        row = EvidenceRecipient(
            id=rid, name=(name or rid), contact=contact,
            status="ACTIVE", registered_by=operator)
        db.add(row)
        db.flush()
        return row


def disable_recipient(db: Session, recipient: str, *, operator: str,
                      reason: str | None = None) -> EvidenceRecipient:
    r = db.get(EvidenceRecipient, normalize_recipient(recipient))
    if r is None:
        raise DistributionNotFound(f"接收方 {recipient} 不在名册中")
    if r.status == "DISABLED":
        return r  # 重复停用幂等
    r.status = "DISABLED"
    r.disabled_at = _now()
    r.disabled_by = operator
    r.disable_reason = (reason[:500] if reason else None)
    db.flush()
    return r


def require_active_recipient(db: Session, recipient: str) -> EvidenceRecipient:
    rid = normalize_recipient(recipient)
    r = db.get(EvidenceRecipient, rid)
    if r is None:
        raise DistributionStateError(
            f"接收方 {rid} 未登记, 请先登记授权接收方再创建分发包",
            code="recipient_not_registered")
    if r.status != "ACTIVE":
        raise DistributionStateError(
            f"接收方 {rid} 已被停用, 不能接收新的分发包",
            code="recipient_disabled")
    return r


def list_recipients(db: Session, *, status: str | None = None,
                    limit: int = 200) -> list[EvidenceRecipient]:
    q = db.query(EvidenceRecipient)
    if status:
        q = q.filter(EvidenceRecipient.status == status)
    return (q.order_by(EvidenceRecipient.created_at, EvidenceRecipient.id)
            .limit(min(max(1, limit), 500)).all())


def recipient_to_dict(r: EvidenceRecipient) -> dict:
    return {
        "recipient": r.id, "name": r.name, "contact": r.contact,
        "status": r.status, "registered_by": r.registered_by,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "disabled_at": r.disabled_at.isoformat() if r.disabled_at else None,
        "disabled_by": r.disabled_by, "disable_reason": r.disable_reason,
    }


# ---------- 脱敏 ----------

REDACTED_FIELDS_BY_POLICY = {
    "NONE": [],
    "STANDARD": [
        "events.operator", "events.payload.reason", "events.payload.note",
        "events.payload.detail", "events.payload.message",
        "events.payload.comment", "events.payload.description",
        "events.payload.operator", "signatures.operator",
        "signatures.note",
    ],
    "FULL": [
        "events.operator", "events.payload", "signatures.operator",
        "signatures.note",
    ],
}


def _redact_payload(payload, policy: str):
    if not isinstance(payload, dict):
        return payload
    if policy == "FULL":
        return {"redacted": True, "by": policy}
    if policy == "STANDARD":
        out = {}
        for k, v in payload.items():
            if k.lower() in _SENSITIVE_PAYLOAD_KEYS:
                out[k] = REDACTED
            else:
                out[k] = v
        return out
    return payload


def redacted_event(ev: dict, policy: str) -> dict:
    """对规范化事件应用脱敏: STANDARD 隐藏操作者与说明类 payload 字段;
    FULL 在其基础上隐藏整段 payload。链哈希/定位字段保留(它们只是散列)。"""
    if policy == "NONE":
        return ev
    out = dict(ev)
    out["operator"] = REDACTED
    out["payload"] = _redact_payload(ev.get("payload"), policy)
    return out


# ---------- 固定 package_id 派生 ----------

def derive_package_id(*, review_id: str, review_signature_hash: str,
                      recipient: str, redaction_policy: str, issue_no: int,
                      valid_from: datetime, valid_until: datetime) -> str:
    body = {
        "kind": "evidence-distribution", "version": 1,
        "review_id": review_id,
        "review_signature_hash": review_signature_hash,
        "recipient": recipient, "redaction_policy": redaction_policy,
        "issue_no": issue_no,
        "valid_from": valid_from.isoformat(),
        "valid_until": valid_until.isoformat(),
    }
    return "EDP" + hashlib.sha256(_compact_json_bytes(body)).hexdigest()[:24]


# ---------- 创建分发包 ----------

def _latest_for(db: Session, review_id: str, recipient: str,
                policy: str) -> EvidenceDistribution | None:
    return (db.query(EvidenceDistribution)
            .filter(EvidenceDistribution.review_id == review_id,
                    EvidenceDistribution.recipient_id == recipient,
                    EvidenceDistribution.redaction_policy == policy)
            .order_by(EvidenceDistribution.issue_no.desc(),
                      EvidenceDistribution.issue_no.desc()).first())


def _redacted_stream_summaries(lines: list[tuple[str, dict, bytes]]
                               ) -> dict[str, dict]:
    """按(脱敏后)包内行计算每条流摘要(不含操作者, 避免泄露)。"""
    grouped: dict[str, list[tuple[dict, bytes]]] = {}
    for _sk, ev, blob in lines:
        grouped.setdefault(ev["stream_key"], []).append((ev, blob))
    out = {}
    for sk in sorted(grouped):
        items = grouped[sk]
        h = hashlib.sha256()
        for _ev, blob in items:
            h.update(blob)
        seqs = sorted(ev["stream_seq"] for ev, _ in items)
        gseqs = sorted(ev["global_seq"] for ev, _ in items)
        out[sk] = {
            "kind": ("batch" if sk.startswith("batch:") else "plan"),
            "event_count": len(items),
            "first_stream_seq": seqs[0], "last_stream_seq": seqs[-1],
            "first_global_seq": gseqs[0], "last_global_seq": gseqs[-1],
            "stream_digest": h.hexdigest(),
        }
    return out


def _signature_events(review: EvidenceReview, recipient: str,
                      policy: str) -> list[dict]:
    """逐事件签署明细(来自归档时不可变 signed_summary; 脱敏下签署人假名化)。"""
    events = (review.signed_summary or {}).get("events") or []
    out = []
    for item in events:
        signers = item.get("signers") or []
        if policy == "NONE":
            shown = list(signers)
        else:
            shown = sorted({_pseudonym(recipient, s) for s in signers})
        out.append({
            "global_seq": item["global_seq"],
            "signers": shown,
            "signer_count": len(set(signers)),
            "verdicts": item.get("verdicts") or [],
        })
    return out


def _quantize(dt: datetime) -> datetime:
    """时间量化到秒: 使 ttl_seconds 表达的有效期在同秒请求间确定一致
    (package_id 派生/自然去重不依赖微秒级时钟)。"""
    return dt.replace(microsecond=0)


def create_distribution(db: Session, *, operator: str, review_id: str,
                        recipient: str, redaction_policy: str,
                        valid_until: datetime,
                        exact_validity: bool = False
                        ) -> tuple[EvidenceDistribution, dict]:
    """从已归档复核单创建脱敏分发包。返回 (包, 额外信息 {deduped,reissued})。

    exact_validity=True 表示调用方显式给定绝对有效期(valid_until ISO),
    去重时要求有效期完全一致; False 表示由 ttl_seconds 派生的相对有效期,
    同参数重试即使跨越秒边界也回显已有有效包(客户端表达的是"相对时长"而非
    某个精确到期时刻)。"""
    if redaction_policy not in EVIDENCE_REDACTION_POLICIES:
        raise DistributionStateError(
            f"脱敏策略必须是 {list(EVIDENCE_REDACTION_POLICIES)} 之一"
            f"(收到 {redaction_policy!r})", code="invalid_redaction_policy")
    valid_until = _quantize(valid_until)
    review = db.get(EvidenceReview, review_id)
    if review is None:
        raise DistributionNotFound(f"证据复核单 {review_id} 不存在")
    if review.status != "ARCHIVED" or not review.signature_hash:
        raise DistributionStateError(
            f"复核单 {review_id} 当前状态 {review.status}: 只有已归档"
            "(ARCHIVED, 已生成不可变签署摘要)的复核单才能创建分发包",
            code="review_not_archived")
    recipient_row = require_active_recipient(db, recipient)
    now = _quantize(_now())
    if valid_until <= now:
        raise DistributionStateError(
            f"有效期止 {valid_until.isoformat()} 已在当前时间之前, 有效期非法",
            code="invalid_validity")
    with _lock:
        latest = _latest_for(db, review_id, recipient_row.id, redaction_policy)
        # 自然幂等: 同(复核单,接收方,策略)已有有效包 -> 原样回显。
        # 显式绝对有效期要求时刻完全一致; 相对 TTL 请求只要求已有包仍有效。
        validity_matches = (latest is not None
                            and (latest.valid_until == valid_until
                                 if exact_validity
                                 else latest.valid_until > now))
        # 自然幂等: 同(复核单,接收方,策略)已有有效包 -> 原样回显
        if (latest is not None and latest.status == "ACTIVE"
                and latest.valid_until > now and validity_matches):
            _add_event(db, latest.id, "dist.idempotent_replay", operator,
                       reason="同参数分发包已存在且有效, 创建请求幂等回显",
                       detail={"package_id": latest.id})
            db.commit()
            return latest, {"deduped": True, "reissued": False}
        # 已恢复(RECOVERED)的包仍在有效期内时, 同参数请求同样幂等回显
        if (latest is not None and latest.status == "RECOVERED"
                and latest.valid_until > now and validity_matches):
            _add_event(db, latest.id, "dist.idempotent_replay", operator,
                       reason="同参数分发包已存在且有效(已恢复), 创建请求幂等回显",
                       detail={"package_id": latest.id})
            db.commit()
            return latest, {"deduped": True, "reissued": False}
        issue_no = (latest.issue_no + 1) if latest is not None else 1
        reissued = latest is not None
        # 固定范围事件(复用复核单同一查询), 必须与归档指纹一致
        s = evidence.get_session(db, review.session_id)
        rows = reviews._scoped_rows(db, s, review)
        fp = reviews.scope_fingerprint(rows)
        if (len(rows) != review.total_events
                or fp != review.scope_fingerprint):
            raise DistributionStateError(
                "复核单固定范围与归档时不一致(事件数或范围指纹变化), "
                "拒绝基于变化后的依据创建分发包(已归档签署摘要不受影响)",
                code="review_basis_changed",
                extra={"expected_events": review.total_events,
                       "actual_events": len(rows),
                       "expected_fingerprint": review.scope_fingerprint,
                       "actual_fingerprint": fp})
        valid_from = now
        pid = derive_package_id(
            review_id=review.id,
            review_signature_hash=review.signature_hash,
            recipient=recipient_row.id, redaction_policy=redaction_policy,
            issue_no=issue_no, valid_from=valid_from,
            valid_until=valid_until)
        if db.get(EvidenceDistribution, pid) is not None:
            # 理论上只有 issue_no 逻辑被绕过才会撞 id
            raise DistributionStateError(
                f"分发包 {pid} 已存在(派生标识冲突)", code="package_exists")

        # 组装脱敏后的包内事件行
        lines: list[tuple[str, dict, bytes]] = []
        for r in rows:
            ev = evidence.canonical_event(r)
            ev = redacted_event(ev, redaction_policy)
            blob = evidence.canonical_event_bytes(ev)
            lines.append((ev["stream_key"], ev, blob))
        events_blob = b"".join(blob for _sk, _ev, blob in lines)
        line_manifest = [{
            "seq": i + 1, "global_seq": ev["global_seq"],
            "stream_key": ev["stream_key"], "stream_seq": ev["stream_seq"],
            "event_type": ev["event_type"],
            "line_digest": _line_digest(blob),
        } for i, (_sk, ev, blob) in enumerate(lines)]
        stream_summaries = _redacted_stream_summaries(lines)
        sig_events = _signature_events(review, recipient_row.id,
                                       redaction_policy)
        stats = (review.signed_summary or {}).get("conclusion_stats") or {}
        signature_doc = {
            "package_id": pid,
            "review_id": review.id,
            "session_id": review.session_id,
            "export_id": review.export_id,
            "plan_id": review.plan_id,
            "digest_algorithm": DIGEST_ALGORITHM,
            "review_signature_hash": review.signature_hash,
            "scope_fingerprint": review.scope_fingerprint,
            "scope_version": review.scope_version,
            "export_manifest_hash": review.export_manifest_hash,
            "export_content_digest": review.export_content_digest,
            "event_range": {
                "start_global_seq": review.start_global_seq,
                "upper_global_seq": review.fixed_upper_global_seq,
                "first_global_seq": review.first_global_seq,
                "last_global_seq": review.last_global_seq,
                "total_events": review.total_events,
            },
            "recipient": recipient_row.id,
            "redaction_policy": redaction_policy,
            "redacted_fields": REDACTED_FIELDS_BY_POLICY[redaction_policy],
            "signer_identity": ("plaintext" if redaction_policy == "NONE"
                                else "package_pseudonym"),
            "valid_from": valid_from.isoformat(),
            "valid_until": valid_until.isoformat(),
            "issue_no": issue_no,
            "conclusion_stats": {
                "total": stats.get("total"),
                "events_signed": stats.get("events_signed"),
                "by_verdict": stats.get("by_verdict"),
            },
            "events": sig_events,
            "signed_at": review.archived_at.isoformat()
            if review.archived_at else None,
        }
        subject = {
            "package_id": pid, "review_id": review.id,
            "recipient": recipient_row.id,
            "redaction_policy": redaction_policy, "issue_no": issue_no,
            "valid_from": valid_from.isoformat(),
            "valid_until": valid_until.isoformat(),
        }
        metadata = {
            "package_id": pid, "review_id": review.id,
            "session_id": review.session_id, "export_id": review.export_id,
            "plan_id": review.plan_id, "digest_algorithm": DIGEST_ALGORITHM,
            "generated_at": now.isoformat(),
            "recipient": recipient_row.id,
            "valid_from": valid_from.isoformat(),
            "valid_until": valid_until.isoformat(),
            "redaction_policy": redaction_policy,
            "redacted_fields": REDACTED_FIELDS_BY_POLICY[redaction_policy],
            "issue_no": issue_no,
            "event_count": len(lines),
            "first_global_seq": review.first_global_seq,
            "last_global_seq": review.last_global_seq,
            "review_binding": {
                "review_id": review.id,
                "review_signature_hash": review.signature_hash,
                "scope_version": review.scope_version,
                "scope_fingerprint": review.scope_fingerprint,
                "total_events": review.total_events,
                "export_manifest_hash": review.export_manifest_hash,
                "export_content_digest": review.export_content_digest,
                "archived_at": review.archived_at.isoformat()
                if review.archived_at else None,
            },
            "signature_subject": subject,
            "contents": ["events.jsonl", "signature.json", "metadata.json",
                         "manifest.json"],
            "immutable": True,
        }
        files = {
            "events.jsonl": events_blob,
            "signature.json": _canonical_bytes(signature_doc),
            "metadata.json": _canonical_bytes(metadata),
        }
        content_digest, file_meta = evidence._content_digest(files)
        manifest_body = {
            "package_id": pid, "review_id": review.id,
            "session_id": review.session_id, "export_id": review.export_id,
            "plan_id": review.plan_id, "digest_algorithm": DIGEST_ALGORITHM,
            "generated_at": now.isoformat(),
            "recipient": recipient_row.id,
            "valid_from": valid_from.isoformat(),
            "valid_until": valid_until.isoformat(),
            "redaction_policy": redaction_policy,
            "redacted_fields": REDACTED_FIELDS_BY_POLICY[redaction_policy],
            "issue_no": issue_no,
            "event_count": len(lines),
            "first_global_seq": review.first_global_seq,
            "last_global_seq": review.last_global_seq,
            "review_signature_hash": review.signature_hash,
            "scope_fingerprint": review.scope_fingerprint,
            "lines": line_manifest,
            "streams": stream_summaries,
            "files": file_meta,
            "content_digest": content_digest,
            "signature_subject": subject,
        }
        manifest_hash = _hash_obj({
            k: v for k, v in manifest_body.items()
            if k not in ("manifest_hash", "signature_digest")})
        signature_digest = _hmac_sign({
            "subject": subject, "content_digest": content_digest,
            "manifest_hash": manifest_hash,
            "review_signature_hash": review.signature_hash})
        manifest = {**manifest_body, "manifest_hash": manifest_hash,
                    "signature_digest": signature_digest}
        path = os.path.join(store_dir(), pid, f"distribution-{pid}.zip")
        evidence._write_zip(path, files, manifest)

        # 写出后自检(篡改/脱敏/签名全部通过才允许落库)
        with open(path, "rb") as f:
            raw = f.read()
        check = verify_package_bytes(raw)
        if not check["valid"]:
            try:
                os.remove(path)
            except OSError:
                pass
            raise DistributionStateError(
                "分发包写出后自检失败: " + "; ".join(check["issues"][:3]),
                code="package_self_check_failed")

        dist = EvidenceDistribution(
            id=pid, review_id=review.id, session_id=review.session_id,
            export_id=review.export_id, plan_id=review.plan_id,
            recipient_id=recipient_row.id,
            review_signature_hash=review.signature_hash,
            review_archived_by=review.archived_by,
            review_archived_at=review.archived_at,
            scope_fingerprint=review.scope_fingerprint,
            scope_version=review.scope_version,
            start_global_seq=review.start_global_seq,
            fixed_upper_global_seq=review.fixed_upper_global_seq,
            first_global_seq=review.first_global_seq,
            last_global_seq=review.last_global_seq,
            total_events=review.total_events,
            redaction_policy=redaction_policy,
            redacted_fields=REDACTED_FIELDS_BY_POLICY[redaction_policy],
            valid_from=valid_from, valid_until=valid_until,
            issue_no=issue_no, status="ACTIVE", created_by=operator,
            package_path=path, package_size=len(raw), manifest=manifest,
            content_digest=content_digest, manifest_hash=manifest_hash,
            signature_digest=signature_digest)
        db.add(dist)
        db.flush()
        # 主接收方自动分派: 最晚回执=包有效期止, 必须确认事件=包内全部事件
        primary = _make_assignment(
            dist, recipient_id=recipient_row.id, due_at=valid_until,
            required_seqs_=None, operator=operator,
            note="创建分发包时的主接收方分派")
        db.add(primary)
        _add_event(db, pid, "dist.recipient.assign", operator,
                   reason=f"分派主接收方 {recipient_row.id}: "
                          f"最晚回执 {valid_until.isoformat()}, "
                          f"必须确认 {len(lines)} 个包内事件(全部)",
                   detail={"assignment_id": primary.id,
                           "recipient": recipient_row.id,
                           "receipt_due_at": valid_until.isoformat(),
                           "required_global_seqs": [],
                           "is_primary": True})
        _add_event(db, pid, "dist.reissue" if reissued else "dist.create",
                   operator,
                   reason=(("撤销/过期后重新签发" if reissued else "创建分发包")
                           + f": 复核单 {review.id}, 接收方 {recipient_row.id}, "
                           f"策略 {redaction_policy}, {len(lines)} 个事件, "
                           f"有效期至 {valid_until.isoformat()}, "
                           f"签发序号 {issue_no}, "
                           f"package_id={pid}, "
                           f"manifest_hash={manifest_hash[:16]}…"),
                   detail={"package_id": pid, "review_id": review.id,
                           "recipient": recipient_row.id,
                           "redaction_policy": redaction_policy,
                           "issue_no": issue_no,
                           "total_events": len(lines),
                           "valid_until": valid_until.isoformat(),
                           "content_digest": content_digest,
                           "manifest_hash": manifest_hash,
                           "signature_digest": signature_digest,
                           "reissued": reissued})
        db.commit()
        return dist, {"deduped": False, "reissued": reissued}


# ---------- 撤销 ----------

def revoke_distribution(db: Session, package_id: str, *, operator: str,
                        reason: str | None = None) -> dict:
    with _lock:
        dist = get_distribution(db, package_id)
        if dist.status == "REVOKED":
            return {"ok": True, "already_in_state": True,
                    "package_id": dist.id, "status": effective_status(dist),
                    "detail": "分发包已撤销, 重复撤销无副作用"}
        now = _now()
        dist.status = "REVOKED"
        dist.revoked_at = now
        dist.revoked_by = operator
        dist.revoke_reason = (reason[:500] if reason else None)
        # 未兑换令牌连带作废(已兑换的历史记录保留)
        outstanding = [d for d in dist.downloads if d.used_at is None
                       and d.revoked_at is None]
        for d in outstanding:
            d.revoked_at = now
        # 审批中的延期申请自动失效(包状态变化)
        invalidated_ext = _invalidate_pending_extensions(
            db, dist, now=now, by=operator,
            reason_code="package_status_changed",
            message=f"分发包被 {operator} 撤销, 审批中的延期申请自动失效")
        _add_event(db, dist.id, "dist.revoke", operator,
                   reason=(f"撤销分发包(接收方 {dist.recipient_id}, "
                           f"{dist.total_events} 个事件): "
                           + (reason or "未提供原因"))[:500],
                   detail={"revoked_tokens": len(outstanding),
                           "invalidated_extensions": invalidated_ext,
                           "review_signature_hash": dist.review_signature_hash})
        db.commit()
        return {"ok": True, "package_id": dist.id,
                "status": "REVOKED", "revoked_tokens": len(outstanding),
                "invalidated_extensions": invalidated_ext}


# ---------- 一次性下载令牌(带接收方约束) ----------

def issue_download_token(db: Session, package_id: str, *, operator: str,
                         ttl_seconds: int | None = None,
                         recipient: str | None = None
                         ) -> EvidenceDistributionDownload:
    with _lock:
        dist = get_distribution(db, package_id)
        st = effective_status(dist)
        if st == "REVOKED":
            raise DistributionStateError(
                f"分发包 {package_id} 已撤销, 不能下载", code="package_revoked")
        if st == "PENDING_PROCESS":
            raise DistributionStateError(
                f"分发包 {package_id} 已到期且回执未完成, 进入待处理状态, "
                "不能下载, 须管理员重新校验并恢复后才可继续",
                code="package_pending_process")
        if st == "EXPIRED":
            raise DistributionStateError(
                f"分发包 {package_id} 已过有效期({dist.valid_until.isoformat()}), "
                "不能下载, 请重新签发", code="package_expired")
        # 多接收方: 显式指定目标时, 操作者须为包创建管理员(代签)或目标接收方本人;
        # 未指定时: 接收方本人以自己为目标, 创建管理员默认包主接收方(兼容旧语义)。
        if recipient:
            target = normalize_recipient(recipient)
            if operator not in (dist.created_by, target):
                raise DistributionForbidden(
                    f"操作者 {operator} 无权为接收方 {target} 代签下载令牌")
        elif operator == dist.created_by:
            target = dist.recipient_id
        else:
            target = operator
        assignment = _get_assignment(dist, target)
        if assignment is None:
            raise DistributionForbidden(
                f"接收方 {target} 不是分发包 {package_id} 的授权接收方, "
                "无权签发下载令牌")
        recipient_row = db.get(EvidenceRecipient, target)
        if recipient_row is None or recipient_row.status != "ACTIVE":
            raise DistributionStateError(
                f"接收方 {target} 已被停用, 不能下载",
                code="recipient_disabled")
        ttl = int(ttl_seconds or download_ttl_seconds())
        now = _now()
        expires = min(now + timedelta(seconds=ttl), dist.valid_until)
        raw = uuid.uuid4().hex + uuid.uuid4().hex
        dl = EvidenceDistributionDownload(
            id="EDD" + uuid.uuid4().hex[:10],
            token_hash=hashlib.sha256(raw.encode()).hexdigest(),
            distribution_id=dist.id, bound_recipient=target,
            issued_by=operator, expires_at=expires)
        db.add(dl)
        _add_event(db, dist.id, "dist.download.issue", operator,
                   reason=f"签发一次性下载令牌(绑定接收方 {target})",
                   detail={"download_id": dl.id,
                           "expires_at": expires.isoformat(),
                           "bound_recipient": target,
                           "assignment_id": assignment.id})
        db.flush()
        dl.token_plain = raw
        return dl


def redeem_download_token(db: Session, token: str, *, operator: str
                          ) -> tuple[EvidenceDistribution, str]:
    """兑换一次性下载令牌。成功即标记已用; 任何拒绝都不改写已用状态。"""
    th = hashlib.sha256(token.encode()).hexdigest()
    dl = (db.query(EvidenceDistributionDownload)
          .filter(EvidenceDistributionDownload.token_hash == th).first())
    if dl is None:
        raise DistributionNotFound("下载令牌无效(不存在或已被使用)")
    dist = get_distribution(db, dl.distribution_id)
    now = _now()

    def _deny(code: str, message: str):
        _add_event(db, dist.id, "dist.download.denied", operator,
                   reason=f"[{code}] {message}"[:500],
                   detail={"code": code, "download_id": dl.id})
        db.commit()
        raise DistributionStateError(message, code=code)

    if dl.used_at is not None:
        _deny("token_already_redeemed",
              f"下载令牌已于 {dl.used_at.isoformat()} 被 {dl.used_by} 兑换"
              "(一次性令牌, 请重新签发)")
    if dl.revoked_at is not None or dist.status == "REVOKED":
        _deny("package_revoked", "分发包已撤销, 下载令牌作废")
    if dist.status == "PENDING_PROCESS":
        _deny("package_pending_process",
              "分发包已到期且回执未完成, 进入待处理状态, 下载已被禁止, "
              "须管理员重新校验并恢复")
    if now > dl.expires_at:
        _deny("token_expired", "下载令牌已过期, 请重新签发")
    if now > dist.valid_until:
        _deny("package_expired",
              f"分发包已过有效期({dist.valid_until.isoformat()}), 不能下载")
    # 接收方约束: 只有授权接收方本人可以兑换
    if operator != dl.bound_recipient:
        _deny("recipient_mismatch",
              f"操作者 {operator} 不是令牌绑定接收方 {dl.bound_recipient}, "
              "无权下载该分发包")
    recipient = db.get(EvidenceRecipient, dl.bound_recipient)
    if recipient is None or recipient.status != "ACTIVE":
        _deny("recipient_disabled",
              f"接收方 {dl.bound_recipient} 已被停用, 不能下载")
    if not os.path.exists(dist.package_path):
        _deny("package_missing", "分发包文件缺失(可能已被外部删除)")
    dl.used_at = now
    dl.used_by = operator
    dist.download_count += 1
    _add_event(db, dist.id, "dist.download.redeem", operator,
               reason="一次性下载令牌兑换成功(首次, 令牌已失效)",
               detail={"download_id": dl.id,
                       "bound_recipient": dl.bound_recipient})
    db.commit()
    return dist, dist.package_path


# ---------- 校验(留存包 / 上传包, 同一套纯函数) ----------

def _tamper(code: str, target: str, message: str, *,
            expected=None, actual=None) -> dict:
    out = {"code": code, "target": target, "message": message}
    if expected is not None:
        out["expected"] = expected
    if actual is not None:
        out["actual"] = actual
    return out


def verify_package_bytes(raw: bytes) -> dict:
    """对 zip 字节做完整离线校验, 返回 {valid, issues, tampering, ...}。

    重算: zip 完整性 -> 必备文件 -> 逐行摘要/顺序/脱敏 -> 逐文件 sha256 ->
    content_digest -> manifest_hash -> signature_digest(HMAC, 接收方约束) ->
    主体(metadata/manifest/signature)一致性。任何不一致都给出具体位置。"""
    tampering: list[dict] = []
    issues: list[str] = []
    result: dict = {"valid": False, "issues": issues, "tampering": tampering}

    def fail(code: str, target: str, message: str, **kw):
        tampering.append(_tamper(code, target, message, **kw))
        issues.append(message)

    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        fail("package_corrupt", "package", "证据包不是合法 zip(已损坏或被篡改)")
        return result
    with zf:
        bad = zf.testzip()
        if bad is not None:
            fail("zip_crc_error", bad, f"zip 压缩数据损坏, 首个坏块: {bad}")
        names = set(zf.namelist())
        blobs: dict[str, bytes] = {}
        for n in names:
            try:
                blobs[n] = zf.read(n)
            except Exception as ex:  # noqa: BLE001
                fail("file_unreadable", n, f"文件 {n} 无法读取: {ex}")
    required = {"manifest.json", "events.jsonl",
                "signature.json", "metadata.json"}
    for n in sorted(required - names):
        fail("file_missing", n, f"必备文件缺失: {n}")

    manifest = None
    if "manifest.json" in blobs:
        try:
            manifest = json.loads(blobs["manifest.json"].decode("utf-8"))
        except Exception as ex:  # noqa: BLE001
            fail("manifest_parse_error", "manifest.json",
                 f"manifest.json 无法解析: {ex}")
    metadata = None
    if "metadata.json" in blobs:
        try:
            metadata = json.loads(blobs["metadata.json"].decode("utf-8"))
        except Exception as ex:  # noqa: BLE001
            fail("metadata_parse_error", "metadata.json",
                 f"metadata.json 无法解析: {ex}")
    signature = None
    if "signature.json" in blobs:
        try:
            signature = json.loads(blobs["signature.json"].decode("utf-8"))
        except Exception as ex:  # noqa: BLE001
            fail("signature_parse_error", "signature.json",
                 f"signature.json 无法解析: {ex}")

    if not isinstance(manifest, dict):
        return result  # 没有清单无法继续

    # 包标识与主体(便于报告)
    result["package_id"] = manifest.get("package_id")
    result["recipient"] = manifest.get("recipient")
    result["review_id"] = manifest.get("review_id")
    result["redaction_policy"] = manifest.get("redaction_policy")
    result["valid_until"] = manifest.get("valid_until")
    result["event_count"] = manifest.get("event_count")
    result["issue_no"] = manifest.get("issue_no")

    policy = manifest.get("redaction_policy")
    if policy not in EVIDENCE_REDACTION_POLICIES:
        fail("invalid_redaction_policy", "manifest.redaction_policy",
             f"未知脱敏策略: {policy!r}", expected=list(EVIDENCE_REDACTION_POLICIES),
             actual=policy)
        policy = policy if isinstance(policy, str) else "NONE"

    # 1) 逐行校验 events.jsonl
    lines_check = {"checked": 0, "mismatches": []}
    result["lines"] = lines_check
    events_blob = blobs.get("events.jsonl", b"")
    raw_lines = [ln for ln in events_blob.split(b"\n") if ln]
    expected_lines = manifest.get("lines") or []
    if manifest.get("event_count") is not None \
            and len(raw_lines) != manifest["event_count"]:
        fail("event_count_mismatch", "events.jsonl",
             f"事件行数与 manifest 不符: 实际 {len(raw_lines)}, "
             f"清单 {manifest['event_count']}",
             expected=manifest["event_count"], actual=len(raw_lines))
    if len(raw_lines) != len(expected_lines):
        fail("line_manifest_count_mismatch", "manifest.lines",
             f"逐行清单条数与 events.jsonl 行数不符: 清单 {len(expected_lines)}, "
             f"实际 {len(raw_lines)}",
             expected=len(raw_lines), actual=len(expected_lines))
    prev_gs = None
    for i, raw_line in enumerate(raw_lines):
        pos = i + 1
        try:
            ev = json.loads(raw_line.decode("utf-8"))
        except Exception as ex:  # noqa: BLE001
            fail("line_parse_error", f"events.jsonl#L{pos}",
                 f"第 {pos} 行不是合法 JSON: {ex}")
            continue
        # raw_lines 已去掉末尾换行; 摘要按"行+换行"复算(与打包行格式一致)
        actual_digest = hashlib.sha256(raw_line + b"\n").hexdigest()
        want = expected_lines[i]["line_digest"] if i < len(expected_lines) \
            else None
        lines_check["checked"] += 1
        if want is not None and actual_digest != want:
            entry = {"seq": pos, "global_seq": ev.get("global_seq"),
                     "expected": want, "actual": actual_digest}
            lines_check["mismatches"].append(entry)
            fail("line_digest_mismatch",
                 f"events.jsonl#seq{pos}(global_seq={ev.get('global_seq')})",
                 f"第 {pos} 行内容摘要不一致(该行被篡改)",
                 expected=want, actual=actual_digest)
        elif i < len(expected_lines):
            m = expected_lines[i]
            if m.get("global_seq") != ev.get("global_seq"):
                fail("line_position_mismatch",
                     f"manifest.lines#seq{pos}",
                     f"第 {pos} 行 global_seq 与清单不一致: "
                     f"实际 {ev.get('global_seq')}, 清单 {m.get('global_seq')}",
                     expected=m.get("global_seq"),
                     actual=ev.get("global_seq"))
        if prev_gs is not None and ev.get("global_seq") is not None \
                and ev["global_seq"] <= prev_gs:
            fail("line_order_invalid", f"events.jsonl#seq{pos}",
                 f"第 {pos} 行 global_seq={ev['global_seq']} 未严格递增"
                 "(行序被改动)", expected=f">{prev_gs}",
                 actual=ev["global_seq"])
        prev_gs = ev.get("global_seq")
        # 脱敏一致性: 声称脱敏却保留了敏感值 -> 脱敏被绕过
        if policy in ("STANDARD", "FULL"):
            if ev.get("operator") not in (REDACTED, None):
                fail("redaction_bypass",
                     f"events.jsonl#seq{pos}.operator",
                     f"第 {pos} 行策略 {policy} 要求隐藏操作者, 实际仍有明文",
                     expected=REDACTED, actual=ev.get("operator"))
            payload = ev.get("payload")
            if policy == "FULL":
                if not (isinstance(payload, dict)
                        and payload.get("redacted") is True):
                    fail("redaction_bypass",
                         f"events.jsonl#seq{pos}.payload",
                         f"第 {pos} 行策略 FULL 要求隐藏整段 payload",
                         expected={"redacted": True}, actual=payload)
            elif isinstance(payload, dict):
                for k, v in payload.items():
                    if k.lower() in _SENSITIVE_PAYLOAD_KEYS \
                            and v != REDACTED and v is not None:
                        fail("redaction_bypass",
                             f"events.jsonl#seq{pos}.payload.{k}",
                             f"第 {pos} 行说明类字段 {k} 未按 STANDARD 脱敏",
                             expected=REDACTED, actual=v)

    # 2) 逐文件 sha256 + content_digest
    files_check: dict[str, dict] = {}
    payload_files = {n: b for n, b in blobs.items() if n != "manifest.json"}
    recomputed_content, _ = evidence._content_digest(payload_files) \
        if payload_files else (None, None)
    for name in sorted(names - {"manifest.json"}):
        actual = hashlib.sha256(blobs[name]).hexdigest()
        expect = (manifest.get("files") or {}).get(name, {}).get(
            DIGEST_ALGORITHM)
        ok = expect is None or actual == expect
        files_check[name] = {"ok": ok, "expected": expect, "actual": actual}
        if not ok:
            fail("file_digest_mismatch", name,
                 f"文件 {name} 的 sha256 与 manifest 记录不一致(文件被篡改)",
                 expected=expect, actual=actual)
    result["files"] = files_check
    want_content = manifest.get("content_digest")
    result["content_digest"] = want_content
    result["recomputed_content_digest"] = recomputed_content
    if want_content and recomputed_content != want_content:
        fail("content_digest_mismatch", "content_digest",
             "内容摘要(content_digest)重算不一致(至少一个内容文件被增删改)",
             expected=want_content, actual=recomputed_content)

    # 3) manifest_hash(剔除 manifest_hash/signature_digest 两个自引用字段)
    recomputed_mh = _hash_obj({
        k: v for k, v in manifest.items()
        if k not in ("manifest_hash", "signature_digest")})
    stored_mh = manifest.get("manifest_hash")
    result["manifest_hash"] = stored_mh
    result["recomputed_manifest_hash"] = recomputed_mh
    if stored_mh and stored_mh != recomputed_mh:
        fail("manifest_hash_mismatch", "manifest.manifest_hash",
             "清单整体摘要(manifest_hash)重算不一致(清单字段被篡改)",
             expected=stored_mh, actual=recomputed_mh)

    # 4) signature_digest(HMAC, 带接收方约束)
    #    两层复算:
    #    a) 用包内登记值复算 -> 验证签名块自身未被伪造/改写(证明本服务签发);
    #    b) 用重算出的 content_digest/manifest_hash 复算 -> 验证签名覆盖的
    #       内容未被改动(只改内容/清单而无法重算 HMAC 时必然失配)。
    subject = manifest.get("signature_subject") or {}
    stored_sig = manifest.get("signature_digest")

    def _sig(content_d, mh):
        return _hmac_sign({
            "subject": subject, "content_digest": content_d,
            "manifest_hash": mh,
            "review_signature_hash": manifest.get("review_signature_hash")})

    recomputed_sig = _sig(want_content, stored_mh)
    sig_over_recomputed = _sig(recomputed_content, recomputed_mh)
    result["signature_digest"] = stored_sig
    result["recomputed_signature_digest"] = recomputed_sig
    result["signature_valid"] = (
        bool(stored_sig) and stored_sig == recomputed_sig
        and stored_sig == sig_over_recomputed)
    if not (bool(stored_sig) and stored_sig == recomputed_sig):
        fail("signature_digest_mismatch", "manifest.signature_digest",
             "签名摘要(signature_digest)重算不一致: 非本服务签发或签名块被篡改",
             expected=stored_sig, actual=recomputed_sig)
    elif stored_sig != sig_over_recomputed:
        fail("signature_digest_mismatch", "manifest.signature_digest",
             "签名摘要与重算内容不一致: 包内容/清单/接收方/有效期被改动, "
             "但签名无法随之重算(密钥只在服务端)",
             expected=stored_sig, actual=sig_over_recomputed)

    # 5) 主体一致性: metadata / signature 必须与 manifest 指向同一包
    def _check_equal(field, left, right, target):
        if right is not None and left != right:
            fail("subject_mismatch", target,
                 f"{target} 与 manifest.{field} 不一致",
                 expected=left, actual=right)

    if isinstance(metadata, dict):
        rb = metadata.get("review_binding") or {}
        for field in ("package_id", "recipient", "review_id",
                      "redaction_policy", "issue_no", "event_count"):
            _check_equal(field, manifest.get(field), metadata.get(field),
                         f"metadata.{field}")
        _check_equal("valid_until", manifest.get("valid_until"),
                     metadata.get("valid_until"), "metadata.valid_until")
        _check_equal("review_signature_hash",
                     manifest.get("review_signature_hash"),
                     rb.get("review_signature_hash"),
                     "metadata.review_binding.review_signature_hash")
    if isinstance(signature, dict):
        for field in ("package_id", "recipient", "review_id",
                      "redaction_policy", "issue_no"):
            _check_equal(field, manifest.get(field), signature.get(field),
                         f"signature.{field}")
        rng = signature.get("event_range") or {}
        _check_equal("total_events", manifest.get("event_count"),
                     rng.get("total_events"),
                     "signature.event_range.total_events")
        _check_equal("scope_fingerprint",
                     manifest.get("scope_fingerprint"),
                     signature.get("scope_fingerprint"),
                     "signature.scope_fingerprint")
        # 逐事件签署明细条数应覆盖事件数, 且每事件两名不同签署人
        sig_events = signature.get("events") or []
        if manifest.get("event_count") is not None \
                and len(sig_events) != manifest["event_count"]:
            fail("signature_event_coverage", "signature.events",
                 f"签署明细覆盖 {len(sig_events)} 个事件, 与包内事件数 "
                 f"{manifest['event_count']} 不符",
                 expected=manifest["event_count"], actual=len(sig_events))
        for item in sig_events:
            signers = item.get("signers") or []
            if len(set(signers)) < 2:
                fail("signature_signers_insufficient",
                     f"signature.events(global_seq={item.get('global_seq')})",
                     f"事件 global_seq={item.get('global_seq')} 缺少两名不同"
                     "签署人", expected=">=2 distinct", actual=len(set(signers)))
        if policy in ("NONE",):
            leaked = [x for x in sig_events
                      if any(str(s).startswith("signer-") for s in
                             (x.get("signers") or []))]
            if leaked:
                fail("signature_identity_mode_mismatch",
                     "signature.events",
                     "策略 NONE 应包含明文签署人, 实际为假名")
        else:
            # 脱敏策略下签署人必须全部为包内假名(signer-<hex10>)
            plain = [s for item2 in sig_events for s in
                     (item2.get("signers") or [])
                     if not re.match(r"^signer-[0-9a-f]{10}$", str(s))]
            if plain:
                fail("signature_identity_leak", "signature.events",
                     f"脱敏策略 {policy} 下签署明细仍含明文操作者账号: "
                     + ", ".join(map(str, plain[:5])))

    result["valid"] = not tampering
    result["reason_code"] = None if result["valid"] else "tampered"
    return result


def verify_stored(db: Session, dist: EvidenceDistribution, *,
                  operator: str = "system") -> dict:
    """校验服务端留存包: 纯字节校验 + 与库内固定摘要比对。不改写生命周期状态
    (撤销/过期只影响能否下载, 不影响密码学结论)。"""
    if not os.path.exists(dist.package_path):
        result = {"valid": False,
                  "issues": ["分发包文件不存在(可能已被外部删除)"],
                  "tampering": [_tamper("package_missing", "package",
                                        "分发包文件不存在")],
                  "reason_code": "package_missing",
                  "package_id": dist.id, "status": effective_status(dist)}
        _add_event(db, dist.id, "dist.verify.failed", operator,
                   reason="分发包文件缺失", detail=result["tampering"])
        db.commit()
        return result
    with open(dist.package_path, "rb") as f:
        raw = f.read()
    result = verify_package_bytes(raw)
    # 与库内创建时固定的摘要交叉比对(防库行/包文件被单独篡改)
    if result.get("manifest_hash") and result["manifest_hash"] != \
            dist.manifest_hash:
        result["valid"] = False
        result["reason_code"] = "tampered"
        result["tampering"].append(_tamper(
            "stored_manifest_hash_mismatch", "db.manifest_hash",
            "包内 manifest_hash 与库内固定记录不一致",
            expected=dist.manifest_hash, actual=result["manifest_hash"]))
        result["issues"].append("包内 manifest_hash 与库内记录不一致")
    if result.get("content_digest") and result["content_digest"] != \
            dist.content_digest:
        result["valid"] = False
        result["reason_code"] = "tampered"
        result["tampering"].append(_tamper(
            "stored_content_digest_mismatch", "db.content_digest",
            "包内 content_digest 与库内固定记录不一致",
            expected=dist.content_digest, actual=result["content_digest"]))
        result["issues"].append("包内 content_digest 与库内记录不一致")
    if result.get("signature_digest") and result["signature_digest"] != \
            dist.signature_digest:
        result["valid"] = False
        result["reason_code"] = "tampered"
        result["tampering"].append(_tamper(
            "stored_signature_digest_mismatch", "db.signature_digest",
            "包内 signature_digest 与库内固定记录不一致",
            expected=dist.signature_digest,
            actual=result["signature_digest"]))
        result["issues"].append("包内 signature_digest 与库内记录不一致")
    result["package_id"] = dist.id
    result["status"] = effective_status(dist)
    result["stored_status"] = dist.status
    result["revoked"] = dist.status == "REVOKED"
    result["expired"] = effective_status(dist) == "EXPIRED"
    ev = "dist.verify.ok" if result["valid"] else "dist.verify.failed"
    _add_event(db, dist.id, ev, operator,
               reason=("摘要与签名校验通过" if result["valid"]
                       else "校验失败, 发现篡改: "
                       + "; ".join(t["message"] for t in
                                   result["tampering"][:3]))[:500],
               detail={"tampering": result["tampering"][:20]})
    db.commit()
    return result


# ---------- 视图 ----------

def effective_status(dist: EvidenceDistribution, now: datetime | None = None
                     ) -> str:
    """派生当前状态:

    REVOKED(库内终态) -> PENDING_PROCESS(回执截止待处理) -> RECOVERED ->
    EXPIRED(RECOVERED 包再次超过有效期) -> ACTIVE。

    历史包(状态仍为库内 ACTIVE)无分派记录时回退到旧的"按 valid_until 派生"
    语义, 保证升级前创建的包行为不变。"""
    if dist.status == "REVOKED":
        return "REVOKED"
    now = now or _now()
    # 待处理是显式的库内生命周期状态(到期扫描进入, 恢复后离开),
    # 即使 valid_until 已过也保持 PENDING_PROCESS, 直到管理员恢复/撤销。
    if dist.status == "PENDING_PROCESS":
        return "PENDING_PROCESS"
    if now > dist.valid_until:
        return "EXPIRED"
    if dist.status == "RECOVERED":
        return "RECOVERED"
    return "ACTIVE"


def _is_downloadable(dist: EvidenceDistribution,
                     now: datetime | None = None) -> bool:
    """是否允许下载: 未撤销、未进入待处理、未超过有效期。"""
    return effective_status(dist, now=now) in ("ACTIVE", "RECOVERED") \
        and (now or _now()) <= dist.valid_until


def get_distribution(db: Session, package_id: str) -> EvidenceDistribution:
    d = db.get(EvidenceDistribution, package_id)
    if d is None:
        raise DistributionNotFound(f"证据分发包 {package_id} 不存在")
    return d


def _add_event(db: Session, distribution_id: str, event: str, operator: str,
               *, reason: str | None = None,
               detail: dict | None = None) -> None:
    db.add(EvidenceDistributionEvent(
        distribution_id=distribution_id, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail))


def require_view_access(dist: EvidenceDistribution, operator: str) -> None:
    """任一已分派接收方本人或创建该包的管理员可查看详情/触发服务端校验。"""
    if operator == dist.created_by or _get_assignment(dist, operator) is not None:
        return
    raise DistributionForbidden(
        f"操作者 {operator} 不是分发包 {dist.id} 的授权接收方"
        f"({dist.recipient_id} 等), 无权查看")


def distribution_to_dict(dist: EvidenceDistribution, *,
                         with_manifest: bool = False,
                         with_events: bool = False,
                         with_receipts: bool = False,
                         now: datetime | None = None) -> dict:
    now = now or _now()
    status = effective_status(dist, now=now)
    outstanding = sum(1 for d in dist.downloads
                      if d.used_at is None and d.revoked_at is None
                      and (not d.expires_at or now <= d.expires_at))
    progress = receipt_progress(dist, now=now)
    out = {
        "package_id": dist.id,
        "review_id": dist.review_id,
        "session_id": dist.session_id,
        "export_id": dist.export_id,
        "plan_id": dist.plan_id,
        "recipient": dist.recipient_id,
        "status": status,                              # 派生当前状态
        "stored_status": dist.status,                  # ACTIVE|PENDING_PROCESS|RECOVERED|REVOKED
        "expired": status == "EXPIRED",
        "revoked": status == "REVOKED",
        "pending_process": status == "PENDING_PROCESS",
        "recovered": dist.status == "RECOVERED",
        "download_allowed": _is_downloadable(dist, now=now),
        "redaction_policy": dist.redaction_policy,
        "redacted_fields": list(dist.redacted_fields or []),
        "issue_no": dist.issue_no,
        "event_count": dist.total_events,
        "first_global_seq": dist.first_global_seq,
        "last_global_seq": dist.last_global_seq,
        "valid_from": dist.valid_from.isoformat()
        if dist.valid_from else None,
        "valid_until": dist.valid_until.isoformat()
        if dist.valid_until else None,
        "review_signature_hash": dist.review_signature_hash,
        "scope_fingerprint": dist.scope_fingerprint,
        "manifest_hash": dist.manifest_hash,
        "content_digest": dist.content_digest,
        "signature_digest": dist.signature_digest,
        "digest_algorithm": DIGEST_ALGORITHM,
        "package_size": dist.package_size,
        "download_count": dist.download_count,
        "active_token_count": outstanding,
        # ---------- 多接收方与回执进度 ----------
        "recipient_count": progress["recipient_count"],
        "receipt_progress": progress,
        "created_by": dist.created_by,
        "created_at": dist.created_at.isoformat()
        if dist.created_at else None,
        "revoked_at": dist.revoked_at.isoformat()
        if dist.revoked_at else None,
        "revoked_by": dist.revoked_by,
        "revoke_reason": dist.revoke_reason,
        "recovered_at": dist.recovered_at.isoformat()
        if getattr(dist, "recovered_at", None) else None,
        "recovered_by": getattr(dist, "recovered_by", None),
    }
    if with_manifest:
        out["manifest"] = dist.manifest
    if with_receipts or with_events:
        out["assignments"] = [
            assignment_to_dict(a, now=now) for a in
            sorted(dist.assignments, key=lambda x: x.id)]
        out["extensions"] = [
            extension_to_dict(e) for e in
            sorted(dist.extensions, key=lambda x: x.created_at)]
    if with_receipts:
        out["receipts"] = [receipt_to_dict(r) for r in
                           sorted(dist.receipts, key=lambda x: x.id)]
    if with_events:
        out["events"] = [{
            "id": e.id, "ts": e.ts.isoformat() if e.ts else None,
            "event": e.event, "operator": e.operator,
            "reason": e.reason, "detail": e.detail,
        } for e in sorted(dist.events, key=lambda x: x.id)]
    return out


def list_distributions(db: Session, *, review_id: str | None = None,
                       recipient: str | None = None,
                       plan_id: str | None = None,
                       status_filter: str | None = None,
                       limit: int = 50,
                       now: datetime | None = None) -> list[EvidenceDistribution]:
    q = db.query(EvidenceDistribution)
    if review_id:
        q = q.filter(EvidenceDistribution.review_id == review_id)
    if recipient:
        # 多接收方: 主接收方或任一已分派接收方
        q = q.filter((EvidenceDistribution.recipient_id == recipient)
                     | (EvidenceDistribution.id.in_(
                         select(EvidenceDistributionAssignment.distribution_id)
                         .where(EvidenceDistributionAssignment.recipient_id
                                == recipient))))
    if plan_id:
        q = q.filter(EvidenceDistribution.plan_id == plan_id)
    if status_filter == "REVOKED":
        q = q.filter(EvidenceDistribution.status == "REVOKED")
    rows = (q.order_by(EvidenceDistribution.created_at.desc(),
                       EvidenceDistribution.id.desc())
            .limit(min(max(1, limit), 200)).all())
    if status_filter in ("ACTIVE", "EXPIRED", "PENDING_PROCESS", "RECOVERED"):
        rows = [d for d in rows if effective_status(d, now=now)
                == status_filter]
    return rows


# ---------- 幂等框架(evidence.distribution.* 命名空间) ----------

def run_distribution_action(db: Session, *, action: str, operator: str,
                            idempotency_key: str, payload: dict, fn,
                            package_id: str | None = None,
                            recipient_id: str | None = None
                            ) -> tuple[dict, bool]:
    req_hash = _hash_obj({"action": f"{DISTRIBUTION_NAMESPACE}.{action}",
                          "package_id": package_id,
                          "recipient_id": recipient_id, "payload": payload})
    existing = db.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise DistributionStateError("幂等键被不同请求复用",
                                         code="idempotency_reuse")
        return existing.response_json, True
    result = fn(db)
    if package_id is not None:
        fresh = get_distribution(db, package_id)
        result.setdefault("package_id", package_id)
        result["status"] = effective_status(fresh)
    db.add(IdempotencyKey(key=idempotency_key,
                          action=f"{DISTRIBUTION_NAMESPACE}.{action}",
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


# ======================================================================
# ---------- 多接收方分派 / 接收回执 / 延期双人审批 / 生命周期 ----------
# ======================================================================

# 回执类型 / 逐事件结果的机器可读常量(同时供接口与测试引用)
RECEIPT_SIGNED = "SIGNED"
RECEIPT_PARTIAL = "PARTIAL"
RECEIPT_REJECTED = "REJECTED"
RESULT_CONFIRMED = "CONFIRMED"
RESULT_ANOMALY = "ANOMALY"
RESULT_REJECTED = "REJECTED"

# 包内事件 global_seq 集合(创建时固定范围)
def package_event_seqs(dist: EvidenceDistribution) -> list[int]:
    seqs = sorted(
        ln["global_seq"] for ln in (dist.manifest or {}).get("lines") or [])
    return seqs


def _get_assignment(dist: EvidenceDistribution, recipient: str
                    ) -> EvidenceDistributionAssignment | None:
    # 直接查库: 跨请求/事务新增的分派可能不在当前对象的关系缓存中
    sess = object_session(dist)
    if sess is not None:
        return sess.query(EvidenceDistributionAssignment).filter(
            EvidenceDistributionAssignment.distribution_id == dist.id,
            EvidenceDistributionAssignment.recipient_id == recipient).first()
    return next((a for a in dist.assignments if a.recipient_id == recipient),
                None)


def required_seqs(assignment: EvidenceDistributionAssignment,
                  dist: EvidenceDistribution) -> list[int]:
    req = assignment.required_global_seqs or []
    return sorted(req) if req else package_event_seqs(dist)


def _make_assignment(dist: EvidenceDistribution, *, recipient_id: str,
                     due_at: datetime, required_seqs_: list[int] | None,
                     operator: str, note: str | None = None
                     ) -> EvidenceDistributionAssignment:
    return EvidenceDistributionAssignment(
        id="EDA" + uuid.uuid4().hex[:12],
        distribution_id=dist.id, recipient_id=recipient_id,
        required_global_seqs=(required_seqs_ or []),
        receipt_due_at=_quantize(due_at), status="PENDING",
        assigned_by=operator, note=(note[:500] if note else None))


def add_assignment(db: Session, package_id: str, *, operator: str,
                   recipient: str, receipt_due_at: datetime | None = None,
                   required_global_seqs: list[int] | None = None,
                   note: str | None = None
                   ) -> EvidenceDistributionAssignment:
    """管理员为已签署归档包追加授权接收方(可配最晚回执时间/必须确认事件范围)。"""
    rid = normalize_recipient(recipient)
    with _lock:
        dist = get_distribution(db, package_id)
        if operator != dist.created_by:
            raise DistributionForbidden(
                f"只有包创建管理员 {dist.created_by} 可以配置授权接收方")
        st = effective_status(dist)
        if st == "REVOKED":
            raise DistributionStateError(
                "分发包已撤销, 不能追加接收方", code="package_revoked")
        if st in ("EXPIRED", "PENDING_PROCESS"):
            raise DistributionStateError(
                f"分发包当前状态 {st}: 已过回执周期, 不能追加接收方, "
                "请恢复或重新签发", code=f"package_{st.lower()}")
        require_active_recipient(db, rid)
        if _get_assignment(dist, rid) is not None:
            raise DistributionStateError(
                f"接收方 {rid} 已在分发包 {package_id} 的授权名单中, 不能重复分派",
                code="recipient_already_assigned")
        # 最晚回执时间默认包有效期止; 显式指定时不得晚于包有效期
        due = _quantize(receipt_due_at or dist.valid_until)
        if due <= _now():
            raise DistributionStateError(
                "最晚回执时间必须在当前时间之后", code="invalid_receipt_due")
        if due > dist.valid_until:
            raise DistributionStateError(
                f"最晚回执时间 {due.isoformat()} 不能晚于包有效期止 "
                f"{dist.valid_until.isoformat()}", code="receipt_due_after_validity")
        # 必须确认的事件范围: 必须全部是包内事件
        if required_global_seqs is not None:
            seqs = sorted(set(int(x) for x in required_global_seqs))
            in_pkg = set(package_event_seqs(dist))
            outside = [x for x in seqs if x not in in_pkg]
            if outside:
                raise DistributionStateError(
                    f"必须确认的事件范围含不在包内的事件: {outside[:10]}",
                    code="event_not_in_package",
                    extra={"outside": outside, "package_seqs": sorted(in_pkg)})
            if not seqs:
                raise DistributionStateError(
                    "必须确认的事件范围不能为空(省略表示包内全部事件)",
                    code="empty_required_scope")
        else:
            seqs = None
        a = _make_assignment(dist, recipient_id=rid, due_at=due,
                             required_seqs_=seqs, operator=operator, note=note)
        db.add(a)
        db.flush()
        _add_event(db, dist.id, "dist.recipient.assign", operator,
                   reason=f"追加授权接收方 {rid}: 最晚回执 {due.isoformat()}, "
                          f"必须确认 "
                          f"{len(seqs) if seqs else dist.total_events} 个事件",
                   detail={"assignment_id": a.id, "recipient": rid,
                           "receipt_due_at": due.isoformat(),
                           "required_global_seqs": seqs or [],
                           "is_primary": False})
        db.commit()
        return a


def assignment_to_dict(a: EvidenceDistributionAssignment,
                       now: datetime | None = None) -> dict:
    now = now or _now()
    out = {
        "assignment_id": a.id,
        "recipient": a.recipient_id,
        "required_global_seqs": list(a.required_global_seqs or []),
        "required_scope_all": not bool(a.required_global_seqs),
        "receipt_due_at": a.receipt_due_at.isoformat()
        if a.receipt_due_at else None,
        "status": a.status,
        "overdue": (a.status == "PENDING" and a.receipt_due_at
                    and now > a.receipt_due_at),
        "assigned_by": a.assigned_by,
        "note": a.note,
        "created_at": a.created_at.isoformat() if a.created_at else None,
        "completed_at": a.completed_at.isoformat() if a.completed_at else None,
    }
    if a.receipt is not None:
        out["receipt"] = receipt_to_dict(a.receipt, with_events=False)
    return out


# ---------- 回执进度 ----------

def receipt_progress(dist: EvidenceDistribution,
                     now: datetime | None = None) -> dict:
    """整体回执进度: 总数/已完成/未完成/按类型统计/未完成接收方/异常原因。"""
    now = now or _now()
    rows = sorted(dist.assignments, key=lambda x: x.id)
    total = len(rows)
    by_status: dict[str, int] = {}
    anomalies: list[dict] = []
    unfinished: list[dict] = []
    for a in rows:
        by_status[a.status] = by_status.get(a.status, 0) + 1
        if a.status in ("PENDING", "OVERDUE"):
            unfinished.append({
                "recipient": a.recipient_id,
                "assignment_id": a.id,
                "receipt_due_at": a.receipt_due_at.isoformat()
                if a.receipt_due_at else None,
                "status": a.status,
                "overdue": bool(a.receipt_due_at and now > a.receipt_due_at),
            })
        if a.receipt is not None:
            for item in (a.receipt.anomalies or []):
                anomalies.append({"recipient": a.recipient_id, **item})
    completed = sum(by_status.get(k, 0) for k in
                    ("SIGNED", "PARTIAL", "REJECTED"))
    return {
        "recipient_count": total,
        "completed": completed,
        "unfinished": total - completed,
        "signed": by_status.get("SIGNED", 0),
        "partial": by_status.get("PARTIAL", 0),
        "rejected": by_status.get("REJECTED", 0),
        "pending": by_status.get("PENDING", 0),
        "overdue": by_status.get("OVERDUE", 0),
        "all_completed": total > 0 and completed == total,
        "unfinished_recipients": unfinished,
        "anomalies": anomalies[:100],
        "anomaly_count": len(anomalies),
    }


def receipt_to_dict(r: EvidenceDistributionReceipt,
                    with_events: bool = True) -> dict:
    out = {
        "receipt_id": r.id,
        "assignment_id": r.assignment_id,
        "package_id": r.distribution_id,
        "recipient": r.recipient_id,
        "receipt_type": r.receipt_type,
        "manifest_hash": r.manifest_hash,
        "content_digest": r.content_digest,
        "download_id": r.download_id,
        "redeemed_at": r.redeemed_at.isoformat() if r.redeemed_at else None,
        "note": r.note,
        "total_required": r.total_required,
        "confirmed_count": r.confirmed_count,
        "anomaly_count": r.anomaly_count,
        "rejected_event_count": r.rejected_event_count,
        "anomalies": r.anomalies,
        "submitted_by": r.submitted_by,
        "submitted_at": r.submitted_at.isoformat() if r.submitted_at else None,
    }
    if with_events:
        out["events"] = [{
            "global_seq": e.global_seq, "result": e.result, "note": e.note,
            "operator": e.operator,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        } for e in sorted(r.events, key=lambda x: x.global_seq)]
    return out


# ---------- 提交回执 ----------

class ReceiptConflict(Exception):
    """回执提交被拒(身份/令牌/摘要/范围/状态)。"""

    def __init__(self, reason: str, code: str = "receipt_rejected",
                 status_code: int = 409, extra: dict | None = None):
        super().__init__(reason)
        self.code = code
        self.status_code = status_code
        self.extra = extra or {}


def submit_receipt(db: Session, package_id: str, *, operator: str,
                   download_id: str, manifest_hash: str, content_digest: str,
                   receipt_type: str, note: str | None = None,
                   events: list[dict] | None = None) -> dict:
    """接收方提交回执(签收/部分异常/拒收)。

    校验链: 身份(必须是已分派接收方本人) -> 包状态(有效且未过该接收方回执截止)
    -> 一次性令牌兑换结果(download_id 存在且已由该接收方成功兑换) ->
    固定 package_id/manifest_hash/content_digest -> 逐事件范围与结论完整性。
    成功后回执与逐事件结论只读; 重复提交同一幂等键在幂等框架层回显首次结果,
    无幂等键的重复提交得到 receipt_already_submitted 拒绝。"""
    if receipt_type not in EVIDENCE_RECEIPT_TYPES:
        raise ReceiptConflict(
            f"回执类型必须是 {list(EVIDENCE_RECEIPT_TYPES)} 之一",
            code="invalid_receipt_type", status_code=422)
    if not isinstance(manifest_hash, str) or len(manifest_hash) != 64 \
            or not isinstance(content_digest, str) or len(content_digest) != 64:
        raise ReceiptConflict(
            "manifest_hash/content_digest 必须为 64 位十六进制摘要",
            code="invalid_digest", status_code=422)
    note = (note or "").strip()[:2000] or None
    with _lock:
        dist = get_distribution(db, package_id)
        st = effective_status(dist)
        if st == "REVOKED":
            raise ReceiptConflict("分发包已撤销, 回执通道只读(不能再提交回执)",
                                  code="package_revoked")
        if st in ("EXPIRED", "PENDING_PROCESS"):
            raise ReceiptConflict(
                f"分发包当前状态 {st}: 回执周期已结束, 不能再提交回执"
                "(已提交回执保持只读)", code=f"package_{st.lower()}")
        assignment = _get_assignment(dist, operator)
        if assignment is None:
            raise ReceiptConflict(
                f"操作者 {operator} 不是分发包 {package_id} 的授权接收方",
                code="recipient_forbidden", status_code=403)
        if assignment.status in ("SIGNED", "PARTIAL", "REJECTED"):
            raise ReceiptConflict(
                f"接收方 {operator} 已提交 {assignment.status} 回执, "
                "回执一次性提交且只读, 不能重复提交",
                code="receipt_already_submitted")
        now = _now()
        if assignment.receipt_due_at and now > assignment.receipt_due_at:
            raise ReceiptConflict(
                f"已超过该接收方最晚回执时间 "
                f"{assignment.receipt_due_at.isoformat()}",
                code="receipt_due_passed")
        # 一次性令牌兑换结果: 必须是本包、绑定该接收方、且已成功兑换的令牌
        dl = None
        for d in dist.downloads:
            if d.id == download_id:
                dl = d
                break
        if dl is None:
            raise ReceiptConflict(
                f"下载令牌兑换记录 {download_id} 不属于分发包 {package_id} "
                "或不存在", code="download_not_redeemed")
        if dl.bound_recipient != operator or dl.used_by != operator \
                or dl.used_at is None:
            raise ReceiptConflict(
                "一次性令牌必须由当前接收方本人成功兑换后才能提交回执"
                f"(令牌绑定 {dl.bound_recipient}, 兑换者 {dl.used_by})",
                code="download_redemption_mismatch", status_code=403)
        # 固定摘要: 拒绝旧摘要/被篡改摘要
        if package_id != dist.id:
            raise ReceiptConflict("回执 package_id 与目标分发包不一致",
                                  code="package_id_mismatch")
        if manifest_hash != dist.manifest_hash:
            raise ReceiptConflict(
                "manifest_hash 与分发包固定清单摘要不一致(旧摘要或被篡改), "
                "拒绝提交回执", code="manifest_hash_mismatch",
                extra={"expected": dist.manifest_hash, "actual": manifest_hash})
        if content_digest != dist.content_digest:
            raise ReceiptConflict(
                "content_digest 与分发包固定内容摘要不一致(旧摘要或被篡改), "
                "拒绝提交回执", code="content_digest_mismatch",
                extra={"expected": dist.content_digest, "actual": content_digest})

        req_seqs = required_seqs(assignment, dist)
        pkg_seqs = set(package_event_seqs(dist))
        # 规范化逐事件结论
        normalized: dict[int, tuple[str, str | None]] = {}
        raw_events = events or []
        if receipt_type in (RECEIPT_SIGNED, RECEIPT_PARTIAL):
            if not raw_events:
                raise ReceiptConflict(
                    f"{receipt_type} 回执必须逐事件提交确认结论",
                    code="events_required")
        for item in raw_events:
            try:
                gs = int(item.get("global_seq"))
                result = item.get("result")
            except (TypeError, ValueError):
                raise ReceiptConflict("事件结论格式非法",
                                      code="invalid_event_item",
                                      status_code=422)
            if result not in EVIDENCE_RECEIPT_EVENT_RESULTS:
                raise ReceiptConflict(
                    f"事件 {gs} 的确认结果 {result!r} 非法, 必须是 "
                    f"{list(EVIDENCE_RECEIPT_EVENT_RESULTS)} 之一",
                    code="invalid_event_result", status_code=422)
            if gs not in pkg_seqs:
                raise ReceiptConflict(
                    f"事件 global_seq={gs} 不在分发包内, 拒绝回执",
                    code="event_not_in_package",
                    extra={"global_seq": gs})
            if gs in normalized:
                raise ReceiptConflict(
                    f"事件 global_seq={gs} 的确认结果重复提交",
                    code="duplicate_event_result")
            ev_note = (item.get("note") or "")
            ev_note = ev_note.strip()[:2000] or None
            if result in (RESULT_ANOMALY, RESULT_REJECTED) and not ev_note:
                raise ReceiptConflict(
                    f"事件 global_seq={gs} 标记为 {result} 必须填写说明",
                    code="event_note_required")
            normalized[gs] = (result, ev_note)

        if receipt_type == RECEIPT_SIGNED:
            missing = [g for g in req_seqs if g not in normalized]
            if missing:
                raise ReceiptConflict(
                    f"签收回执必须覆盖全部必须确认事件, 缺失 {len(missing)} 个: "
                    f"{missing[:10]}", code="required_events_missing",
                    extra={"missing": missing})
            bad = {g: v[0] for g, v in normalized.items()
                   if v[0] != RESULT_CONFIRMED}
            if bad:
                raise ReceiptConflict(
                    "签收回执要求每个事件均为 CONFIRMED, 存在非确认结果: "
                    f"{dict(list(bad.items())[:10])}",
                    code="signed_requires_all_confirmed")
        elif receipt_type == RECEIPT_PARTIAL:
            missing = [g for g in req_seqs if g not in normalized]
            if missing:
                raise ReceiptConflict(
                    f"部分异常回执必须覆盖全部必须确认事件, 缺失 {len(missing)} "
                    f"个: {missing[:10]}", code="required_events_missing",
                    extra={"missing": missing})
            if not any(v[0] in (RESULT_ANOMALY, RESULT_REJECTED)
                       for v in normalized.values()):
                raise ReceiptConflict(
                    "部分异常回执至少要有一个事件为 ANOMALY 或 REJECTED, "
                    "全部确认请提交 SIGNED", code="partial_requires_anomaly")
        else:  # REJECTED
            if not note:
                raise ReceiptConflict(
                    "拒收(REJECTED)回执必须填写整包拒收原因(note)",
                    code="reject_note_required")

        # 只允许确认必须范围内的事件(范围外包内事件多报也拒绝, 防止越权确认)
        extra_confirmed = sorted(g for g in normalized if g not in set(req_seqs))
        if extra_confirmed:
            raise ReceiptConflict(
                f"接收方必须确认的事件范围不含: {extra_confirmed[:10]}",
                code="event_outside_required_scope",
                extra={"outside": extra_confirmed})

        confirmed = sum(1 for v in normalized.values()
                        if v[0] == RESULT_CONFIRMED)
        anomaly = sum(1 for v in normalized.values()
                      if v[0] == RESULT_ANOMALY)
        rejected_ev = sum(1 for v in normalized.values()
                          if v[0] == RESULT_REJECTED)
        anomalies_summary = [{
            "global_seq": g, "result": v[0], "note": v[1],
        } for g, v in sorted(normalized.items())
            if v[0] in (RESULT_ANOMALY, RESULT_REJECTED)]

        receipt = EvidenceDistributionReceipt(
            id="EDR" + uuid.uuid4().hex[:12],
            distribution_id=dist.id, assignment_id=assignment.id,
            recipient_id=operator, receipt_type=receipt_type,
            package_id_confirmed=dist.id,
            manifest_hash=manifest_hash, content_digest=content_digest,
            download_id=dl.id, download_token_hash=dl.token_hash,
            redeemed_at=dl.used_at, note=note,
            total_required=len(req_seqs), confirmed_count=confirmed,
            anomaly_count=anomaly, rejected_event_count=rejected_ev,
            anomalies=anomalies_summary, submitted_by=operator)
        db.add(receipt)
        db.flush()
        for g, (result, ev_note) in sorted(normalized.items()):
            db.add(EvidenceDistributionReceiptEvent(
                receipt_id=receipt.id, global_seq=g, result=result,
                note=ev_note, operator=operator))
        assignment.status = receipt_type
        assignment.completed_at = now
        db.flush()
        _add_event(db, dist.id, "dist.receipt.submit", operator,
                   reason=(f"接收方 {operator} 提交回执 {receipt_type}: "
                           f"确认 {confirmed}, 异常 {anomaly}, "
                           f"拒收事件 {rejected_ev}"
                           + (f"; {note[:120]}" if note else ""))[:500],
                   detail={"receipt_id": receipt.id,
                           "assignment_id": assignment.id,
                           "recipient": operator,
                           "receipt_type": receipt_type,
                           "download_id": dl.id,
                           "manifest_hash": manifest_hash,
                           "content_digest": content_digest,
                           "confirmed": confirmed, "anomaly": anomaly,
                           "rejected_events": rejected_ev,
                           "anomalies": anomalies_summary[:50]})
        progress = receipt_progress(dist)
        result = {
            "ok": True, "package_id": dist.id,
            "receipt_id": receipt.id, "assignment_id": assignment.id,
            "recipient": operator, "receipt_type": receipt_type,
            "total_required": len(req_seqs), "confirmed_count": confirmed,
            "anomaly_count": anomaly,
            "rejected_event_count": rejected_ev,
            "submitted_at": now.isoformat(),
            "progress": progress,
        }
        db.commit()
        return result


# ---------- 延期申请与双人审批 ----------

def extension_basis(dist: EvidenceDistribution) -> dict:
    return {
        "status": dist.status,
        "valid_until": dist.valid_until.isoformat(),
        "manifest_hash": dist.manifest_hash,
        "content_digest": dist.content_digest,
    }


def _approval_rows(db: Session, extension_id: str
                   ) -> list[EvidenceDistributionExtensionApproval]:
    return db.query(EvidenceDistributionExtensionApproval).filter(
        EvidenceDistributionExtensionApproval.extension_id
        == extension_id).order_by(
        EvidenceDistributionExtensionApproval.id).all()


def _pending_extensions(db: Session, dist: EvidenceDistribution
                        ) -> list[EvidenceDistributionExtension]:
    return db.query(EvidenceDistributionExtension).filter(
        EvidenceDistributionExtension.distribution_id == dist.id,
        EvidenceDistributionExtension.status == "PENDING").all()


def _invalidate_pending_extensions(db: Session, dist: EvidenceDistribution, *,
                                   now: datetime, by: str,
                                   reason_code: str, message: str) -> int:
    """包状态/摘要变化时, 审批中的延期申请全部失效(连同已收集的 APPROVED)。"""
    n = 0
    for ext in _pending_extensions(db, dist):
        ext.status = "INVALIDATED"
        ext.invalidated_reason = reason_code
        ext.invalidated_at = now
        for ap in _approval_rows(db, ext.id):
            if ap.decision == "APPROVED":
                ap.decision = "INVALIDATED"
        _add_event(db, dist.id, "dist.extension.invalidated", by or "system",
                   reason=(message + f": 申请 {ext.id}")[:500],
                   detail={"extension_id": ext.id, "reason": reason_code})
        n += 1
    return n


def request_extension(db: Session, package_id: str, *, operator: str,
                      new_valid_until: datetime, reason: str | None = None,
                      idempotency_key: str | None = None
                      ) -> EvidenceDistributionExtension:
    """管理员在回执截止前申请延期; 两名不同操作者审批后才能应用。"""
    new_valid_until = _quantize(new_valid_until)
    with _lock:
        dist = get_distribution(db, package_id)
        if operator != dist.created_by:
            raise DistributionForbidden(
                f"只有包创建管理员 {dist.created_by} 可以申请延期")
        st = effective_status(dist)
        if st == "REVOKED":
            raise DistributionStateError("分发包已撤销, 不能申请延期",
                                         code="package_revoked")
        if st in ("EXPIRED", "PENDING_PROCESS"):
            raise DistributionStateError(
                f"分发包当前状态 {st}: 回执周期已结束, 不能延期, "
                "请走恢复或重新签发", code="extension_window_closed")
        now = _now()
        if new_valid_until <= dist.valid_until:
            raise DistributionStateError(
                f"新有效期止 {new_valid_until.isoformat()} 必须晚于当前有效期止 "
                f"{dist.valid_until.isoformat()}",
                code="invalid_new_valid_until")
        if _pending_extensions(db, dist):
            raise DistributionStateError(
                f"分发包已有审批中的延期申请, 不能重复申请(并发去重)",
                code="extension_already_pending")
        ext = EvidenceDistributionExtension(
            id="EDE" + uuid.uuid4().hex[:12], distribution_id=dist.id,
            status="PENDING", current_valid_until=dist.valid_until,
            new_valid_until=new_valid_until, basis=extension_basis(dist),
            requested_by=operator, reason=(reason[:500] if reason else None),
            idempotency_key=idempotency_key)
        db.add(ext)
        db.flush()
        _add_event(db, dist.id, "dist.extension.request", operator,
                   reason=(f"申请回执延期至 {new_valid_until.isoformat()}: "
                           + (reason or "未提供原因"))[:500],
                   detail={"extension_id": ext.id,
                           "current_valid_until": dist.valid_until.isoformat(),
                           "new_valid_until": new_valid_until.isoformat(),
                           "basis": ext.basis})
        db.commit()
        return ext


def _check_extension_basis(dist: EvidenceDistribution,
                           ext: EvidenceDistributionExtension) -> str | None:
    """复核审批依据: 包状态变化或摘要变化 -> 失效原因(否则 None)。"""
    b = ext.basis or {}
    if dist.status != b.get("status") or dist.valid_until.isoformat() != \
            b.get("valid_until"):
        return "package_status_changed"
    if dist.manifest_hash != b.get("manifest_hash") \
            or dist.content_digest != b.get("content_digest"):
        return "digest_changed"
    return None


def approve_extension(db: Session, extension_id: str, *, operator: str
                      ) -> dict:
    """一名操作者对延期申请投赞成票; 第二名不同操作者通过时原子应用延期。"""
    with _lock:
        ext = get_extension(db, extension_id)
        dist = get_distribution(db, ext.distribution_id)
        if ext.status != "PENDING":
            raise DistributionStateError(
                f"延期申请 {extension_id} 当前状态 {ext.status}, "
                "不再接受审批", code="extension_not_pending")
        if operator == ext.requested_by:
            raise DistributionStateError(
                "申请人不能审批自己发起的延期申请(须两名独立操作者)",
                code="approver_is_requester")
        prior = _approval_rows(db, extension_id)
        existing = next((a for a in prior if a.operator == operator
                         and a.decision in ("APPROVED", "REJECTED")), None)
        if existing is not None:
            raise DistributionStateError(
                f"操作者 {operator} 已对该延期申请作出结论({existing.decision}), "
                "不能重复审批(并发去重)", code="approver_already_decided")
        now = _now()
        # 审批前实时复核依据: 状态/摘要变化 -> 申请自动失效
        reason = _check_extension_basis(dist, ext)
        if reason is not None:
            ext.status = "INVALIDATED"
            ext.invalidated_reason = reason
            ext.invalidated_at = now
            for ap in prior:
                if ap.decision == "APPROVED":
                    ap.decision = "INVALIDATED"
            _add_event(db, dist.id, "dist.extension.invalidated", operator,
                       reason=f"审批时复核发现依据变化({reason}), 延期申请失效",
                       detail={"extension_id": ext.id, "reason": reason})
            db.commit()
            raise DistributionStateError(
                f"审批期间审批依据发生变化({reason}), 延期申请已自动失效",
                code="extension_basis_changed",
                extra={"reason": reason})
        basis_now = extension_basis(dist)
        db.add(EvidenceDistributionExtensionApproval(
            extension_id=ext.id, decision="APPROVED", operator=operator,
            basis=basis_now))
        db.flush()
        # 直接查库统计(关系集合可能未在本会话加载/刷新)
        approver_rows = db.query(
            EvidenceDistributionExtensionApproval.operator).filter(
            EvidenceDistributionExtensionApproval.extension_id == ext.id,
            EvidenceDistributionExtensionApproval.decision == "APPROVED"
        ).all()
        approvers = sorted({r[0] for r in approver_rows})
        _add_event(db, dist.id, "dist.extension.approve", operator,
                   reason=f"延期申请获得审批 {len(approvers)}"
                          f"/{EVIDENCE_EXTENSION_REQUIRED_APPROVALS}: "
                          f"{operator} 赞成",
                   detail={"extension_id": ext.id,
                           "approvers": approvers})
        applied = False
        if len(approvers) >= EVIDENCE_EXTENSION_REQUIRED_APPROVALS:
            # 应用前最后一次复核(防止与失效动作竞争)
            reason = _check_extension_basis(dist, ext)
            if reason is not None:
                ext.status = "INVALIDATED"
                ext.invalidated_reason = reason
                ext.invalidated_at = now
                db.commit()
                raise DistributionStateError(
                    f"应用延期前复核发现依据变化({reason}), 申请已失效",
                    code="extension_basis_changed",
                    extra={"reason": reason})
            _apply_extension(db, dist, ext, approvers=approvers, now=now)
            applied = True
        db.commit()
        return {"ok": True, "extension_id": ext.id,
                "extension_status": ext.status, "applied": applied,
                "approvals": len(approvers),
                "required": EVIDENCE_EXTENSION_REQUIRED_APPROVALS,
                "new_valid_until": ext.new_valid_until.isoformat(),
                "valid_until": dist.valid_until.isoformat()}


def _apply_extension(db: Session, dist: EvidenceDistribution,
                     ext: EvidenceDistributionExtension, *,
                     approvers: list[str], now: datetime) -> None:
    old_until = dist.valid_until
    new_until = ext.new_valid_until
    delta = new_until - old_until
    dist.valid_until = new_until
    moved: list[dict] = []
    # 未完成分派的最晚回执时间顺延同样的增量(不早于新有效期止则钳制);
    # 已完成回执与 OVERDUE 历史保持不变
    for a in dist.assignments:
        if a.status == "PENDING":
            old_due = a.receipt_due_at
            a.receipt_due_at = min(_quantize(old_due + delta), new_until)
            moved.append({"recipient": a.recipient_id,
                          "old_due": old_due.isoformat(),
                          "new_due": a.receipt_due_at.isoformat()})
    ext.status = "APPLIED"
    ext.applied_at = now
    _add_event(db, dist.id, "dist.extension.apply", ",".join(approvers),
               reason=f"两名不同操作者审批通过, 回执截止延期至 "
                      f"{new_until.isoformat()}(顺延 {delta})",
               detail={"extension_id": ext.id, "approvers": approvers,
                       "old_valid_until": old_until.isoformat(),
                       "new_valid_until": new_until.isoformat(),
                       "assignments_moved": moved})


def reject_extension(db: Session, extension_id: str, *, operator: str,
                     reason: str) -> dict:
    with _lock:
        ext = get_extension(db, extension_id)
        dist = get_distribution(db, ext.distribution_id)
        if ext.status != "PENDING":
            raise DistributionStateError(
                f"延期申请 {extension_id} 当前状态 {ext.status}, "
                "不再接受审批", code="extension_not_pending")
        if operator == ext.requested_by:
            raise DistributionStateError(
                "申请人不能审批自己发起的延期申请",
                code="approver_is_requester")
        prior = _approval_rows(db, extension_id)
        existing = next((a for a in prior if a.operator == operator), None)
        if existing is not None:
            raise DistributionStateError(
                f"操作者 {operator} 已对该延期申请作出结论, 不能重复审批",
                code="approver_already_decided")
        now = _now()
        db.add(EvidenceDistributionExtensionApproval(
            extension_id=ext.id, decision="REJECTED", operator=operator,
            reason=reason[:500], basis=extension_basis(dist)))
        db.flush()
        # 已收集的赞成票终态化留痕
        for ap in _approval_rows(db, extension_id):
            if ap.decision == "APPROVED":
                ap.decision = "SUPERSEDED"
        ext.status = "REJECTED"
        ext.rejected_by = operator
        ext.rejected_at = now
        _add_event(db, dist.id, "dist.extension.reject", operator,
                   reason=f"延期申请被拒绝: {reason}"[:500],
                   detail={"extension_id": ext.id})
        db.commit()
        return {"ok": True, "extension_id": ext.id, "status": "REJECTED"}


def get_extension(db: Session, extension_id: str
                  ) -> EvidenceDistributionExtension:
    ext = db.get(EvidenceDistributionExtension, extension_id)
    if ext is None:
        raise DistributionNotFound(f"延期申请 {extension_id} 不存在")
    return ext


def extension_to_dict(ext: EvidenceDistributionExtension) -> dict:
    approvers = sorted({a.operator for a in ext.approvals
                        if a.decision == "APPROVED"})
    return {
        "extension_id": ext.id,
        "package_id": ext.distribution_id,
        "status": ext.status,
        "current_valid_until": ext.current_valid_until.isoformat()
        if ext.current_valid_until else None,
        "new_valid_until": ext.new_valid_until.isoformat()
        if ext.new_valid_until else None,
        "requested_by": ext.requested_by,
        "reason": ext.reason,
        "approvals": [{"operator": a.operator, "decision": a.decision,
                       "reason": a.reason,
                       "created_at": a.created_at.isoformat()
                       if a.created_at else None}
                      for a in sorted(ext.approvals, key=lambda x: x.id)],
        "approvers": approvers,
        "approval_count": len(approvers),
        "required": EVIDENCE_EXTENSION_REQUIRED_APPROVALS,
        "applied_at": ext.applied_at.isoformat() if ext.applied_at else None,
        "invalidated_reason": ext.invalidated_reason,
        "invalidated_at": ext.invalidated_at.isoformat()
        if ext.invalidated_at else None,
        "rejected_by": ext.rejected_by,
        "rejected_at": ext.rejected_at.isoformat()
        if ext.rejected_at else None,
        "created_at": ext.created_at.isoformat() if ext.created_at else None,
    }


# ---------- 到期扫描: 未完成回执 -> 待处理(禁止下载) ----------

def sweep_due_packages(db: Session, *, now: datetime | None = None,
                       operator: str = "system") -> dict:
    """扫描到期分发包: 已过包有效期且仍有接收方未完成回执 -> PENDING_PROCESS,
    未完成分派标记 OVERDUE, 未兑换令牌连带作废, 审批中延期失效。已提交回执、
    原始归档摘要与包文件保持只读(密码学校验结论不受影响)。"""
    now = now or _now()
    due = (db.query(EvidenceDistribution)
           .filter(EvidenceDistribution.status.in_(("ACTIVE", "RECOVERED")),
                   EvidenceDistribution.valid_until < now).all())
    transitioned: list[str] = []
    for dist in due:
        with _lock:
            fresh = db.get(EvidenceDistribution, dist.id)
            if fresh.status not in ("ACTIVE", "RECOVERED"):
                continue
            # 直接查库, 不依赖可能过期的关系集合缓存
            total_assignments = db.query(
                EvidenceDistributionAssignment).filter(
                EvidenceDistributionAssignment.distribution_id == fresh.id)\
                .count()
            if not total_assignments:
                # 无分派的历史包不自动进入待处理(保持旧的过期语义)
                continue
            overdue_assignments = db.query(EvidenceDistributionAssignment)\
                .filter(
                    EvidenceDistributionAssignment.distribution_id == fresh.id,
                    EvidenceDistributionAssignment.status == "PENDING").all()
            if not overdue_assignments:
                # 所有接收方都已按时回执: 包自然结束, 不进待处理
                continue
            db.flush()
            for a in overdue_assignments:
                a.status = "OVERDUE"
            outstanding = [d for d in fresh.downloads if d.used_at is None
                           and d.revoked_at is None]
            for d in outstanding:
                d.revoked_at = now
            _invalidate_pending_extensions(
                db, fresh, now=now, by=operator,
                reason_code="package_status_changed",
                message="回执到期未完成, 包进入待处理状态, 审批中延期自动失效")
            fresh.status = "PENDING_PROCESS"
            _add_event(db, fresh.id, "dist.pending", operator,
                       reason=f"回执截止 {fresh.valid_until.isoformat()} "
                              f"仍有 {len(overdue_assignments)} 个接收方未完成"
                              "回执, 自动进入待处理状态并禁止继续下载",
                       detail={"overdue_recipients":
                                   [a.recipient_id for a in
                                    overdue_assignments],
                               "revoked_tokens": len(outstanding)})
            transitioned.append(fresh.id)
            db.commit()
    return {"swept": len(transitioned), "transitioned": transitioned}


# ---------- 恢复(重新校验通过后续期) ----------

def recover_distribution(db: Session, package_id: str, *, operator: str,
                         new_valid_until: datetime,
                         reason: str | None = None) -> dict:
    """待处理包恢复: 必须重新校验留存包通过, 然后续期(未完成分派重开回执窗口)。
    重新签发(换 package_id/回执周期)是另一条路径: create_distribution。"""
    new_valid_until = _quantize(new_valid_until)
    with _lock:
        dist = get_distribution(db, package_id)
        if dist.status != "PENDING_PROCESS":
            raise DistributionStateError(
                f"分发包当前状态 {dist.status}({effective_status(dist)}): "
                "只有待处理(PENDING_PROCESS)状态的包需要恢复",
                code="not_pending_process")
        now = _now()
        if new_valid_until <= now:
            raise DistributionStateError(
                "恢复后的新有效期止必须在当前时间之后",
                code="invalid_validity")
        # 恢复前必须重新校验通过(文件存在/逐行/逐文件/manifest/签名/库内摘要一致)
        check = verify_stored(db, dist, operator=operator)
        if not check.get("valid"):
            raise DistributionStateError(
                "恢复被拒绝: 重新校验未通过(包文件缺失或摘要/签名不一致), "
                "请撤销后重新签发全新分发包",
                code="recovery_verification_failed",
                extra={"tampering": check.get("tampering", [])[:20]})
        dist.status = "RECOVERED"
        dist.recovered_at = now
        dist.recovered_by = operator
        old_until = dist.valid_until
        dist.valid_until = new_valid_until
        reopened: list[str] = []
        for a in dist.assignments:
            if a.status == "OVERDUE":
                a.status = "PENDING"
                a.receipt_due_at = new_valid_until
                reopened.append(a.recipient_id)
        # 恢复后旧的未决/失效延期不再可审批; 已 APPLIED 的历史保留
        _invalidate_pending_extensions(
            db, dist, now=now, by=operator,
            reason_code="package_status_changed",
            message="包恢复, 旧审批周期中的延期申请自动失效")
        _add_event(db, dist.id, "dist.recover", operator,
                   reason=(f"重新校验通过, 包恢复并续期至 "
                           f"{new_valid_until.isoformat()}, "
                           f"重开 {len(reopened)} 个接收方回执窗口: "
                           + (reason or "未提供原因"))[:500],
                   detail={"old_valid_until": old_until.isoformat(),
                           "new_valid_until": new_valid_until.isoformat(),
                           "reopened_recipients": reopened,
                           "manifest_hash": dist.manifest_hash,
                           "content_digest": dist.content_digest})
        db.commit()
        return {"ok": True, "package_id": dist.id,
                "status": effective_status(dist),
                "valid_until": dist.valid_until.isoformat(),
                "reopened_recipients": reopened}


class DistributionLifecycleWorker:
    """单实例后台线程: 周期性把回执到期未完成的包转入待处理状态。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv(
                                  "EVIDENCE_DISTRIBUTION_SWEEP_INTERVAL",
                                  "5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop,
                                        name="dist-lifecycle-worker",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        db = SessionLocal()
        try:
            return sweep_due_packages(db)["swept"]
        finally:
            db.close()

    def _loop(self) -> None:
        while not self._stop.wait(self.poll_interval):
            try:
                self.tick_once()
            except Exception:
                import time
                time.sleep(self.poll_interval)


# ---------- 升级期补齐: 历史包(无分派)补主接收方分派 ----------

def backfill_assignments(db: Session) -> int:
    """旧库分发包没有分派/回执表数据: 为每个历史包按主接收方补一条分派,
    最晚回执=包有效期止, 范围=包内全部事件。已撤销/过期包同样补齐以正确展示。
    幂等, 可重复执行。"""
    rows = db.query(EvidenceDistribution).order_by(EvidenceDistribution.id).all()
    n = 0
    for dist in rows:
        if dist.assignments:
            continue
        a = EvidenceDistributionAssignment(
            id="EDA" + uuid.uuid4().hex[:12], distribution_id=dist.id,
            recipient_id=dist.recipient_id, required_global_seqs=[],
            receipt_due_at=dist.valid_until, status="PENDING",
            assigned_by=dist.created_by,
            note="历史分发包升级补齐的主接收方分派")
        db.add(a)
        db.flush()
        n += 1
    if n:
        db.commit()
    return n


# 延迟导入避免模块加载期循环依赖
from .db import SessionLocal  # noqa: E402
