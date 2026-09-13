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
import uuid
import zipfile
from datetime import datetime, timedelta

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import evidence, reviews
from .models import (
    EVIDENCE_REDACTION_POLICIES, EvidenceDistribution,
    EvidenceDistributionDownload, EvidenceDistributionEvent,
    EvidenceRecipient, EvidenceReview, IdempotencyKey,
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
        _add_event(db, dist.id, "dist.revoke", operator,
                   reason=(f"撤销分发包(接收方 {dist.recipient_id}, "
                           f"{dist.total_events} 个事件): "
                           + (reason or "未提供原因"))[:500],
                   detail={"revoked_tokens": len(outstanding),
                           "review_signature_hash": dist.review_signature_hash})
        db.commit()
        return {"ok": True, "package_id": dist.id,
                "status": "REVOKED", "revoked_tokens": len(outstanding)}


# ---------- 一次性下载令牌(带接收方约束) ----------

def issue_download_token(db: Session, package_id: str, *, operator: str,
                         ttl_seconds: int | None = None
                         ) -> EvidenceDistributionDownload:
    with _lock:
        dist = get_distribution(db, package_id)
        st = effective_status(dist)
        if st == "REVOKED":
            raise DistributionStateError(
                f"分发包 {package_id} 已撤销, 不能下载", code="package_revoked")
        if st == "EXPIRED":
            raise DistributionStateError(
                f"分发包 {package_id} 已过有效期({dist.valid_until.isoformat()}), "
                "不能下载, 请重新签发", code="package_expired")
        # 仅包创建管理员或授权接收方可请求令牌
        if operator not in (dist.created_by, dist.recipient_id):
            raise DistributionForbidden(
                f"操作者 {operator} 不是分发包 {package_id} 的授权接收方"
                f"({dist.recipient_id}), 无权签发下载令牌")
        recipient = db.get(EvidenceRecipient, dist.recipient_id)
        if recipient is None or recipient.status != "ACTIVE":
            raise DistributionStateError(
                f"接收方 {dist.recipient_id} 已被停用, 不能下载",
                code="recipient_disabled")
        ttl = int(ttl_seconds or download_ttl_seconds())
        now = _now()
        expires = min(now + timedelta(seconds=ttl), dist.valid_until)
        raw = uuid.uuid4().hex + uuid.uuid4().hex
        dl = EvidenceDistributionDownload(
            id="EDD" + uuid.uuid4().hex[:10],
            token_hash=hashlib.sha256(raw.encode()).hexdigest(),
            distribution_id=dist.id, bound_recipient=dist.recipient_id,
            issued_by=operator, expires_at=expires)
        db.add(dl)
        _add_event(db, dist.id, "dist.download.issue", operator,
                   reason=f"签发一次性下载令牌(绑定接收方 {dist.recipient_id})",
                   detail={"download_id": dl.id,
                           "expires_at": expires.isoformat(),
                           "bound_recipient": dist.recipient_id})
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
    """派生当前状态: REVOKED(库内终态) / EXPIRED(超过有效期) / ACTIVE。"""
    if dist.status == "REVOKED":
        return "REVOKED"
    now = now or _now()
    if now > dist.valid_until:
        return "EXPIRED"
    return "ACTIVE"


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
    """只有授权接收方本人或创建该包的管理员可查看详情/触发服务端校验。"""
    if operator in (dist.recipient_id, dist.created_by):
        return
    raise DistributionForbidden(
        f"操作者 {operator} 不是分发包 {dist.id} 的授权接收方"
        f"({dist.recipient_id}), 无权查看")


def distribution_to_dict(dist: EvidenceDistribution, *,
                         with_manifest: bool = False,
                         with_events: bool = False,
                         now: datetime | None = None) -> dict:
    status = effective_status(dist, now=now)
    outstanding = sum(1 for d in dist.downloads
                      if d.used_at is None and d.revoked_at is None
                      and (not d.expires_at or (now or _now()) <= d.expires_at))
    out = {
        "package_id": dist.id,
        "review_id": dist.review_id,
        "session_id": dist.session_id,
        "export_id": dist.export_id,
        "plan_id": dist.plan_id,
        "recipient": dist.recipient_id,
        "status": status,                              # 派生当前状态
        "stored_status": dist.status,                  # ACTIVE|REVOKED
        "expired": status == "EXPIRED",
        "revoked": status == "REVOKED",
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
        "created_by": dist.created_by,
        "created_at": dist.created_at.isoformat()
        if dist.created_at else None,
        "revoked_at": dist.revoked_at.isoformat()
        if dist.revoked_at else None,
        "revoked_by": dist.revoked_by,
        "revoke_reason": dist.revoke_reason,
    }
    if with_manifest:
        out["manifest"] = dist.manifest
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
        q = q.filter(EvidenceDistribution.recipient_id == recipient)
    if plan_id:
        q = q.filter(EvidenceDistribution.plan_id == plan_id)
    if status_filter == "REVOKED":
        q = q.filter(EvidenceDistribution.status == "REVOKED")
    rows = (q.order_by(EvidenceDistribution.created_at.desc(),
                       EvidenceDistribution.id.desc())
            .limit(min(max(1, limit), 200)).all())
    if status_filter in ("ACTIVE", "EXPIRED"):
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
