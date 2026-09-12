"""迁移前数据质量门禁: 规则版本化 -> 批次质量扫描 -> 问题修复/豁免 -> 启动门禁。

设计要点:
1. 规则集(QualityRuleSet)按计划绑定, 一个计划至多一个; 规则整体版本化 —— 每次
   修改且内容摘要变化新增一个不可变版本(QualityRuleVersion), 旧版本永不修改。
   规则支持四类: required(必填) / format(格式, 正则) / cross_field(跨字段一致性) /
   range(范围约束, 数值 min/max 或字符串长度 min_length/max_length);
   严重级别 BLOCKER/WARNING/INFO, 只有 BLOCKER 阻断门禁。
2. 扫描任务(QualityScan)对计划涉及的批次逐个扫描旧结构数据(迁移前事实来源),
   按批次(QualityScanBatch)记录扫描进度、记录数、问题数与批次数据指纹;
   问题(QualityIssue)按 批次+记录+规则+字段 唯一, 带严重级别、说明与命中记录快照
   (可追踪样本)。扫描状态机 QUEUED/RUNNING/PAUSED/CANCELED/COMPLETED/FAILED,
   排队受 QUALITY_SCAN_MAX_CONCURRENCY 并发闸门约束(与回放同构)。
3. 扫描结果与三要素绑定: 规则版本(version+digest)、批次数据指纹(扫描完成时
   范围内旧表内容摘要)、过期时间(ttl_seconds, 默认 24h)。规则版本变化、
   批次数据变化或结果过期 -> 门禁判定 STALE, 旧结果不能直接放行。
4. 问题处理: 管理员可创建修复批次(QualityFixBatch, 逐项重跑规则核验当前数据,
   违规消失才置 FIXED)或提交带原因的豁免申请(QualityExemption, 绑定规则版本,
   提交即批准、可撤销, 只追加保留历史); 同一轮版本的新扫描自动继承有效豁免。
5. 门禁(evaluate_gate): 仅计划绑定了规则集时生效; 必须存在 COMPLETED 且基于
   当前规则版本、数据指纹未漂移、未过期的扫描, 且其全部 BLOCKER 问题都已
   FIXED/EXEMPTED, 计划才允许进入启动流程。无规则集的计划不受门禁约束
   (保持与历史行为兼容)。
6. 幂等: 规则保存/扫描创建与控制/修复/豁免全部走 quality.* 幂等命名空间;
   服务重启后规则、扫描任务、问题处理历史与门禁状态全部落库保留, 遗留 RUNNING
   任务回到 QUEUED、RUNNING 批次回到 PENDING(问题未提交, 安全重跑)。
"""
import hashlib
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import plans, service
from .models import (
    IdempotencyKey, MigrationBatch, MigrationPlan, PlanStep, QualityEvent,
    QualityExemption, QualityFixBatch, QualityGateHold, QualityIssue,
    QualityRuleSet, QualityRuleVersion, QualityScan, QualityScanBatch, RecordOld,
    QUALITY_RULE_TYPES, QUALITY_SCAN_ACTIVE_STATUSES,
    QUALITY_SCAN_STATUSES, QUALITY_SCAN_TERMINAL_STATUSES, QUALITY_SEVERITIES,
    RESCAN_TRIGGERS,
)

# 扫描状态 -> 允许的管理动作(FAILED 可 resume, 与清理计划语义一致)
SCAN_ALLOWED_ACTIONS = {
    "pause": {"QUEUED", "RUNNING"},
    "resume": {"PAUSED", "FAILED"},
    "cancel": {"QUEUED", "RUNNING", "PAUSED"},
}

# 规则可作用字段(旧结构 + 派生字段 tags 与 is_empty 辅助必填语义)
RULE_FIELDS = ("id", "name", "email", "tags_csv", "tags")

# 每条 规则×批次 最多保留的命中样本数(其余仍计数, 明细截断)
MAX_SAMPLES_PER_RULE_BATCH = 500
DEFAULT_TTL_SECONDS = 24 * 3600


class QualityNotFound(Exception):
    """计划 / 规则集 / 扫描 / 问题不存在 -> 404。"""


class QualityStateError(Exception):
    """当前状态不允许该操作(状态机冲突/门禁拒绝/幂等键复用) -> 409。"""


class QualityValidationError(Exception):
    """规则定义聚合校验失败 -> 409, reasons 逐条说明。"""

    def __init__(self, reasons: list[str]):
        super().__init__("; ".join(reasons))
        self.reasons = reasons


class _ScanFatal(Exception):
    """扫描执行中的致命错误: 任务必须 FAILED。code 为机器可读错误码。"""

    def __init__(self, code: str, reason: str):
        super().__init__(reason)
        self.code = code
        self.reason = reason


# ---------- 配置 ----------

def max_concurrency() -> int:
    """同时允许 RUNNING 的质量扫描数, 环境变量 QUALITY_SCAN_MAX_CONCURRENCY(默认 2, 至少 1)。"""
    try:
        return max(1, int(os.getenv("QUALITY_SCAN_MAX_CONCURRENCY", "2")))
    except ValueError:
        return 2


def default_ttl_seconds() -> int:
    try:
        return max(60, int(os.getenv("QUALITY_SCAN_TTL_SECONDS", str(DEFAULT_TTL_SECONDS))))
    except ValueError:
        return DEFAULT_TTL_SECONDS


def now_utc_naive() -> datetime:
    return plans.now_utc_naive()


# ---------- 幂等框架(quality.* 命名空间) ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def run_quality_action(session: Session, *, action: str, operator: str,
                       idempotency_key: str, payload: dict, fn,
                       plan_id: str | None = None,
                       scan_id: str | None = None) -> tuple[dict, bool]:
    """与 replay.run_replay_action 同构: fn 在同事务执行, 结果与幂等键一起落库;
    重复提交(含崩溃重放)返回首次结果。请求哈希带计划/扫描 id, 同键跨目标复用被拒。"""
    req_hash = _hash({"action": f"quality.{action}", "plan_id": plan_id,
                      "scan_id": scan_id, "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise QualityStateError("幂等键被不同请求复用")
        return existing.response_json, True

    result = fn(session)
    session.add(IdempotencyKey(key=idempotency_key, action=f"quality.{action}",
                               request_hash=req_hash, response_json=result))
    try:
        session.commit()
    except IntegrityError:  # 并发同键撞主键: 返回先提交者的结果
        session.rollback()
        winner = session.get(IdempotencyKey, idempotency_key)
        if winner is not None and winner.request_hash == req_hash:
            return winner.response_json, True
        raise
    return result, False


# ---------- 事件流水(只追加; 质量模块不写业务审计 audit_log) ----------

def add_event(session: Session, *, plan_id: str, event: str, operator: str,
              scan_id: str | None = None, reason: str | None = None,
              detail: dict | None = None) -> None:
    session.add(QualityEvent(
        plan_id=plan_id, scan_id=scan_id, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail,
    ))


# ---------- 行锁 / 串行锁 ----------

def _lock_scan(session: Session, scan_id: str) -> QualityScan:
    if session.bind.dialect.name != "sqlite":
        scan = session.get(QualityScan, scan_id, with_for_update=True)
        if scan is not None:
            return scan
    scan = session.get(QualityScan, scan_id)
    if scan is None:
        raise QualityNotFound(f"质量扫描任务 {scan_id} 不存在")
    return scan


def _lock_scheduling(session: Session) -> None:
    """串行化 worker 的并发额度认领。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260920)"))


def _lock_creation(session: Session) -> None:
    """串行化同计划扫描创建的去重判定(手动扫描 + 自动重扫共用)。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260921)"))


# SQLite 没有事务级咨询锁: 用进程内互斥锁串行化扫描创建判定,
# 语义与 Postgres 的 pg_advisory_xact_lock(20260921) 对应 ——
# 重复/并发触发(写入通知、规则版本、过期兜底、执行器门禁)在同一计划上
# 必须合并成唯一任务。多副本部署时以 Postgres 咨询锁为准。
_rescan_creation_lock = threading.RLock()


# ---------- 规则定义规范化与校验 ----------

def _canonical_rule(rule: dict) -> dict:
    """规则的规范化(确定性)表示: 键排序、可选字段缺省, 用于内容摘要与快照。"""
    out = {
        "id": rule["id"],
        "name": rule.get("name") or rule["id"],
        "type": rule["type"],
        "field": rule.get("field"),
        "severity": rule.get("severity", "BLOCKER"),
        "enabled": bool(rule.get("enabled", True)),
    }
    params = rule.get("params") or {}
    for key in ("pattern", "format", "op", "other_field", "value",
                "min", "max", "min_length", "max_length", "allow_blank"):
        if key in params:
            out[key] = params[key]
    return out


def validate_rules(raw_rules: list) -> list[dict]:
    """聚合校验规则定义, 全部通过才返回规范化后的规则列表;
    任何一条不通过都抛 QualityValidationError, 一次性列出全部原因。"""
    reasons: list[str] = []
    if not isinstance(raw_rules, list) or not raw_rules:
        raise QualityValidationError(["规则集至少需要一条规则"])
    seen_ids: set[str] = set()
    normalized: list[dict] = []
    for i, raw in enumerate(raw_rules, 1):
        if not isinstance(raw, dict):
            reasons.append(f"第 {i} 条规则必须是对象")
            continue
        rid = str(raw.get("id") or "").strip()
        label = rid or f"第 {i} 条"
        if not rid:
            reasons.append(f"第 {i} 条规则缺少 id")
        elif rid in seen_ids:
            reasons.append(f"规则 id 重复: {rid}")
        else:
            seen_ids.add(rid)
        rtype = raw.get("type")
        if rtype not in QUALITY_RULE_TYPES:
            reasons.append(f"规则 {label} 的 type 必须是 {list(QUALITY_RULE_TYPES)} 之一"
                           f"(收到 {rtype!r})")
        severity = raw.get("severity", "BLOCKER")
        if severity not in QUALITY_SEVERITIES:
            reasons.append(f"规则 {label} 的 severity 必须是 {list(QUALITY_SEVERITIES)} 之一"
                           f"(收到 {severity!r})")
        field = raw.get("field")
        if rtype != "cross_field":
            if field not in RULE_FIELDS:
                reasons.append(f"规则 {label} 的 field 必须是 {list(RULE_FIELDS)} 之一"
                               f"(收到 {field!r})")
        params = raw.get("params") or {}
        if not isinstance(params, dict):
            reasons.append(f"规则 {label} 的 params 必须是对象")
            params = {}
        if rtype == "format":
            pat = params.get("pattern")
            if not isinstance(pat, str) or not pat:
                reasons.append(f"规则 {label}(format) 的 params.pattern 必填且为非空正则字符串")
            else:
                try:
                    re.compile(pat)
                except re.error as e:
                    reasons.append(f"规则 {label}(format) 的正则无法编译: {e}")
        elif rtype == "range":
            keys = ("min", "max", "min_length", "max_length")
            if not any(k in params for k in keys):
                reasons.append(f"规则 {label}(range) 的 params 至少包含 "
                               f"min/max(数值) 或 min_length/max_length(长度) 之一")
            for k in ("min", "max"):
                if k in params and not isinstance(params[k], (int, float)):
                    reasons.append(f"规则 {label}(range) 的 params.{k} 必须是数字")
            for k in ("min_length", "max_length"):
                if k in params and (not isinstance(params[k], int) or params[k] < 0):
                    reasons.append(f"规则 {label}(range) 的 params.{k} 必须是非负整数")
            if (isinstance(params.get("min"), (int, float))
                    and isinstance(params.get("max"), (int, float))
                    and params["min"] > params["max"]):
                reasons.append(f"规则 {label}(range) 的 min 不能大于 max")
            if (isinstance(params.get("min_length"), int)
                    and isinstance(params.get("max_length"), int)
                    and params["min_length"] > params["max_length"]):
                reasons.append(f"规则 {label}(range) 的 min_length 不能大于 max_length")
        elif rtype == "cross_field":
            other = params.get("other_field")
            op = params.get("op")
            if field not in RULE_FIELDS:
                reasons.append(f"规则 {label}(cross_field) 的 field 必须是 {list(RULE_FIELDS)} 之一")
            if other not in RULE_FIELDS:
                reasons.append(f"规则 {label}(cross_field) 的 params.other_field 必须是 "
                               f"{list(RULE_FIELDS)} 之一(收到 {other!r})")
            if field in RULE_FIELDS and other in RULE_FIELDS and field == other:
                reasons.append(f"规则 {label}(cross_field) 不能与自身比较")
            if op not in ("eq", "ne", "contains", "not_contains", "regex_match"):
                reasons.append(f"规则 {label}(cross_field) 的 params.op 必须是 "
                               f"eq/ne/contains/not_contains/regex_match 之一(收到 {op!r})")
            if op == "regex_match":
                val = params.get("value")
                if not isinstance(val, str) or not val:
                    reasons.append(f"规则 {label}(cross_field) op=regex_match 时 "
                                   f"params.value 必填(正则字符串)")
                else:
                    try:
                        re.compile(val)
                    except re.error as e:
                        reasons.append(f"规则 {label}(cross_field) 的正则无法编译: {e}")
        # required 无额外参数; allow_blank 可选
        normalized.append(_canonical_rule({
            "id": rid, "name": (str(raw.get("name")).strip()[:200] if raw.get("name") else rid),
            "type": rtype, "field": field, "severity": severity,
            "enabled": bool(raw.get("enabled", True)), "params": params,
        }))
    if reasons:
        raise QualityValidationError(reasons)
    return normalized


def _rules_digest(rules: list[dict]) -> str:
    return _hash({"rules": rules})


# ---------- 规则集绑定与版本 ----------

def get_plan(session: Session, plan_id: str) -> MigrationPlan:
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise QualityNotFound(f"迁移计划 {plan_id} 不存在")
    return plan


def get_ruleset(session: Session, plan_id: str) -> QualityRuleSet:
    rs = session.query(QualityRuleSet).filter(QualityRuleSet.plan_id == plan_id).first()
    if rs is None:
        raise QualityNotFound(f"计划 {plan_id} 尚未绑定数据质量规则集")
    return rs


def current_rules(session: Session, plan_id: str, *,
                  fresh: bool = False) -> QualityRuleVersion | None:
    """计划当前规则版本(按规则集内最大版本号取)。

    不经过 rs.current_version 属性: 同事务内规则集行可能已在身份映射中
    (autoflush 关闭时是提升版本号前的旧值)。fresh=True 时先 flush 待提交的
    版本变更并强制从数据库刷新规则集, 保证规则保存事务内编排的重扫基于新版本。"""
    if fresh:
        session.flush()
    rs = (session.query(QualityRuleSet)
          .populate_existing()
          .filter(QualityRuleSet.plan_id == plan_id).first())
    if rs is None:
        return None
    if fresh:
        session.refresh(rs)
    return (session.query(QualityRuleVersion)
            .populate_existing()
            .filter(QualityRuleVersion.ruleset_id == rs.id)
            .order_by(QualityRuleVersion.version.desc())
            .first())


def do_save_rules(session: Session, _ignored, operator: str, plan_id: str,
                  raw_rules: list, note: str | None = None) -> dict:
    """为计划绑定/更新规则集: 校验通过后, 内容摘要与当前版本一致则幂等无副作用;
    否则新增一个不可变规则版本并提升 current_version。"""
    plan = get_plan(session, plan_id)
    rules = validate_rules(raw_rules)
    digest = _rules_digest(rules)
    rs = session.query(QualityRuleSet).filter(QualityRuleSet.plan_id == plan_id).first()
    if rs is None:
        rs = QualityRuleSet(id="QRS" + uuid.uuid4().hex[:10], plan_id=plan_id,
                            current_version=1, created_by=operator, updated_by=operator)
        session.add(rs)
        session.flush()
        version = 1
    else:
        latest = (session.query(QualityRuleVersion)
                  .filter(QualityRuleVersion.ruleset_id == rs.id)
                  .order_by(QualityRuleVersion.version.desc()).first())
        if latest is not None and latest.content_digest == digest:
            # 内容未变: 幂等无副作用, 不产生新版本
            add_event(session, plan_id=plan_id, event="rules.save.idempotent",
                      operator=operator,
                      reason=f"规则内容与 v{latest.version} 完全一致, 保存请求无副作用")
            session.flush()
            return {"ok": True, "ruleset_id": rs.id, "version": latest.version,
                    "already_in_state": True, "content_digest": digest,
                    "detail": f"规则内容与当前 v{latest.version} 完全一致, 未产生新版本"}
        version = rs.current_version + 1
    rv = QualityRuleVersion(
        ruleset_id=rs.id, plan_id=plan_id, version=version, rules=rules,
        rule_count=len(rules), content_digest=digest,
        note=(note[:500] if note else None), created_by=operator)
    session.add(rv)
    rs.current_version = version
    rs.updated_by = operator
    add_event(session, plan_id=plan_id, scan_id=None, event="rules.version_create",
              operator=operator,
              reason=(f"创建规则版本 v{version}: {len(rules)} 条规则"
                      + (f"({note})" if note else "")),
              detail={"version": version, "rule_count": len(rules),
                      "content_digest": digest})
    # 规则版本变化即时失效: 执行态(RUNNING/HALTED)计划立即编排基于新版本的
    # 唯一自动重扫; DRAFT 计划由门禁实时判定 STALE(管理员手动重扫的既有流程)。
    notify_rule_version_changed(session, plan_id, new_version=version,
                                operator=operator, commit=False)
    session.flush()
    return {"ok": True, "ruleset_id": rs.id, "version": version,
            "rule_count": len(rules), "content_digest": digest,
            "detail": (f"规则版本 v{version} 已保存: {len(rules)} 条规则; "
                       f"规则版本变化后旧扫描结果不再放行, 需基于新版本重新扫描")}


# ---------- 规则求值引擎 ----------

def _record_view(rec: RecordOld) -> dict:
    """旧结构记录的求值视图(含派生 tags 列表)。"""
    return {
        "id": rec.id,
        "name": rec.name or "",
        "email": rec.email or "",
        "tags_csv": rec.tags_csv or "",
        "tags": [t for t in (rec.tags_csv or "").split(",") if t],
    }


def _is_blank(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def _violation(rule: dict, field: str | None, message: str) -> dict:
    return {"rule_id": rule["id"], "rule_name": rule["name"],
            "rule_type": rule["type"], "severity": rule["severity"],
            "field": field, "message": message}


def eval_rule(rule: dict, view: dict) -> list[dict]:
    """对单条记录求值单条规则, 返回违规列表(通常 0 或 1 条)。"""
    if not rule.get("enabled", True):
        return []
    rtype = rule["type"]
    field = rule.get("field")
    value = view.get(field) if field in view else None
    if rtype == "required":
        allow_blank = bool(rule.get("allow_blank", False))
        if value is None or (not allow_blank and _is_blank(value)):
            kind = "为空" if value is None or value == "" else "为空白值"
            return [_violation(rule, field, f"必填字段 {field} {kind}")]
        return []
    if rtype == "format":
        if _is_blank(value):
            return []  # 空值交给 required 规则, 格式规则不重复报
        if re.fullmatch(rule["pattern"], str(value)) is None:
            return [_violation(rule, field,
                               f"字段 {field} 值 {value!r} 不匹配格式 {rule['pattern']}")]
        return []
    if rtype == "range":
        if value is None:
            return []
        if "min" in rule or "max" in rule:
            try:
                num = float(value)
            except (TypeError, ValueError):
                return [_violation(rule, field, f"字段 {field} 值 {value!r} 不是数字, 无法做范围约束")]
            if "min" in rule and num < rule["min"]:
                return [_violation(rule, field, f"字段 {field}={num} 小于最小值 {rule['min']}")]
            if "max" in rule and num > rule["max"]:
                return [_violation(rule, field, f"字段 {field}={num} 大于最大值 {rule['max']}")]
        if isinstance(value, str):
            length = len(value)
        elif isinstance(value, (list, dict)):
            length = len(value)
        else:
            length = len(str(value))
        if "min_length" in rule and length < rule["min_length"]:
            return [_violation(rule, field,
                               f"字段 {field} 长度 {length} 小于最小长度 {rule['min_length']}")]
        if "max_length" in rule and length > rule["max_length"]:
            return [_violation(rule, field,
                               f"字段 {field} 长度 {length} 大于最大长度 {rule['max_length']}")]
        return []
    if rtype == "cross_field":
        other = rule["other_field"]
        other_value = view.get(other)
        op = rule["op"]
        if op == "eq":
            if value != other_value:
                return [_violation(rule, field,
                                   f"跨字段一致性: {field}={value!r} 应等于 {other}={other_value!r}")]
        elif op == "ne":
            if value == other_value:
                return [_violation(rule, field,
                                   f"跨字段一致性: {field}={value!r} 不应等于 {other}={other_value!r}")]
        elif op == "contains":
            if value is None or other_value not in str(value):
                return [_violation(rule, field,
                                   f"跨字段一致性: {field}={value!r} 应包含 {other}={other_value!r}")]
        elif op == "not_contains":
            if value is not None and other_value is not None and str(other_value) in str(value):
                return [_violation(rule, field,
                                   f"跨字段一致性: {field}={value!r} 不应包含 {other}={other_value!r}")]
        elif op == "regex_match":
            if _is_blank(value) or re.fullmatch(rule["value"], str(value)) is None:
                return [_violation(rule, field,
                                   f"跨字段一致性: {field}={value!r} 不匹配 {rule['value']}")]
        return []
    return []  # pragma: no cover - 未知类型已在校验阶段拒绝


def rule_for(rules: list[dict], rule_id: str) -> dict | None:
    return next((r for r in rules if r["id"] == rule_id), None)


def eval_rules_against_view(rules: list[dict], view: dict) -> list[dict]:
    out: list[dict] = []
    for rule in rules:
        out.extend(eval_rule(rule, view))
    return out


# ---------- 数据指纹 ----------

def batch_records(session: Session, batch: MigrationBatch):
    return (session.query(RecordOld)
            .filter(service._in_range(RecordOld.id, batch))
            .order_by(RecordOld.id))


def fingerprint_batch(session: Session, batch: MigrationBatch) -> tuple[str, int]:
    """批次范围内旧表当前内容的确定性摘要与记录数(迁移前数据变化的判定依据)。"""
    rows = batch_records(session, batch).all()
    h = hashlib.sha256()
    for rec in rows:
        h.update(json.dumps(
            {"id": rec.id, "name": rec.name, "email": rec.email,
             "tags_csv": rec.tags_csv},
            sort_keys=True, ensure_ascii=False, default=str).encode())
        h.update(b"\n")
    h.update(json.dumps(
        {"batch_id": batch.id, "start": batch.id_start, "end": batch.id_end,
         "count": len(rows)}, sort_keys=True).encode())
    return h.hexdigest(), len(rows)


# ---------- 扫描任务创建 ----------

def plan_batches(session: Session, plan_id: str) -> list[PlanStep]:
    """计划涉及的步骤(按 seq), 批次在扫描时逐个解析(被删要明确失败)。"""
    return plans.steps_of(session, plan_id)


def _active_scan_for(session: Session, plan_id: str) -> QualityScan | None:
    return (session.query(QualityScan)
            .filter(QualityScan.plan_id == plan_id,
                    QualityScan.status.in_(QUALITY_SCAN_ACTIVE_STATUSES))
            .order_by(QualityScan.created_at.desc(), QualityScan.id.desc())
            .first())


def _latest_scan(session: Session, plan_id: str) -> QualityScan | None:
    return (session.query(QualityScan)
            .filter(QualityScan.plan_id == plan_id)
            .order_by(QualityScan.created_at.desc(), QualityScan.id.desc())
            .first())


def _build_scan_rows(session: Session, scan_id: str, plan_id: str,
                     steps: list[PlanStep]) -> None:
    for st in steps:
        batch = session.get(MigrationBatch, st.batch_id)
        session.add(QualityScanBatch(
            scan_id=scan_id, batch_id=st.batch_id, seq=st.seq,
            biz=batch.biz if batch else None, status="PENDING"))


def do_create_scan(session: Session, _ignored, operator: str, plan_id: str) -> dict:
    """基于计划当前规则版本创建质量扫描任务并排队(管理员手动发起)。

    计划不存在 -> 404; 未绑定规则集 -> 409;
    计划 DRAFT 时可手动扫描; 计划 RUNNING 但处于质量门禁暂停(quality hold)时
    也允许手动发起(作为自动重扫之外的人工补救, 扫描通过门禁后暂停同样自动解除);
    其他状态拒绝; 已有活动(QUEUED/RUNNING/PAUSED)扫描时重复创建幂等返回已有任务。
    """
    plan = get_plan(session, plan_id)
    rv = current_rules(session, plan_id)
    if rv is None:
        raise QualityStateError(
            f"计划 {plan_id} 尚未绑定数据质量规则集, 请先保存规则后再发起扫描")
    held = bool(plan.quality_hold)
    if not (plan.status == "DRAFT" or (plan.status == "RUNNING" and held)):
        raise QualityStateError(
            f"计划 {plan_id} 当前状态 {plan.status}"
            + ("(未处于质量门禁暂停)" if plan.status != "DRAFT" else "")
            + ", 质量扫描只能在启动前(DRAFT)或执行中门禁暂停时手动发起")
    steps = plans.steps_of(session, plan_id)
    if not steps:
        raise QualityStateError(f"计划 {plan_id} 没有任何步骤(批次), 无可扫描内容")

    with _rescan_creation_lock:
        _lock_creation(session)
        existing = _active_scan_for(session, plan_id)
        if existing is not None:
            add_event(session, plan_id=plan_id, scan_id=existing.id,
                      event="scan.create.idempotent", operator=operator,
                      reason=f"重复扫描请求幂等返回已有{existing.status}任务 {existing.id}")
            session.flush()
            return {"ok": True, "scan_id": existing.id, "status": existing.status,
                    "already_active": True,
                    "detail": f"该计划已有 {existing.status} 的质量扫描 {existing.id}, "
                              f"重复请求无副作用"}

        scan_id = "QS" + uuid.uuid4().hex[:10]
        ttl = default_ttl_seconds()
        scan = QualityScan(
            id=scan_id, plan_id=plan_id, ruleset_id=rv.ruleset_id,
            rule_version=rv.version, rule_digest=rv.content_digest,
            rules_snapshot=rv.rules, status="QUEUED",
            total_batches=len(steps), ttl_seconds=ttl, scan_source="manual",
            created_by=operator, updated_by=operator)
        session.add(scan)
        session.flush()
        _build_scan_rows(session, scan_id, plan_id, steps)
        add_event(session, plan_id=plan_id, scan_id=scan_id, event="scan.create",
                  operator=operator,
                  detail={"rule_version": rv.version, "total_batches": len(steps),
                          "ttl_seconds": ttl, "scan_source": "manual"})
        add_event(session, plan_id=plan_id, scan_id=scan_id, event="scan.queue",
                  operator=operator,
                  reason=f"质量扫描已排队(规则 v{rv.version}, 共 {len(steps)} 个批次, "
                         f"并发上限 {max_concurrency()}, 结果 {ttl}s 后过期)")
        session.flush()
        return {"ok": True, "scan_id": scan_id, "status": "QUEUED",
                "rule_version": rv.version, "total_batches": len(steps),
                "already_active": False,
                "detail": f"质量扫描 {scan_id} 已创建并排队: 计划 {plan.name}({plan_id}), "
                          f"规则 v{rv.version}, {len(steps)} 个批次"}


# ---------- 暂停 / 恢复 / 取消(均幂等) ----------

def _require_status(scan: QualityScan, action: str) -> None:
    allowed = SCAN_ALLOWED_ACTIONS[action]
    if scan.status not in allowed:
        raise QualityStateError(
            f"质量扫描 {scan.id} 当前状态 {scan.status} 不允许 {action}"
            f"(仅 {sorted(allowed)} 状态可执行该操作)")


def do_pause_scan(session: Session, scan: QualityScan, operator: str) -> dict:
    if scan.status == "PAUSED":
        return {"ok": True, "already_in_state": True,
                "detail": "质量扫描已处于暂停状态, 重复暂停无副作用"}
    _require_status(scan, "pause")
    scan.status = "PAUSED"
    scan.updated_by = operator
    scan.current_batch_id = None  # 暂停在批次边界
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id, event="scan.pause",
              operator=operator,
              reason=("暂停排队中的扫描, 恢复后重新排队" if not scan.started_at
                      else "暂停请求已记录, 将在当前批次边界停住"))
    return {"ok": True, "detail": "质量扫描已暂停, 将在当前批次边界停止(已完成批次问题保留)"}


def do_resume_scan(session: Session, scan: QualityScan, operator: str) -> dict:
    if scan.status in ("QUEUED", "RUNNING"):
        return {"ok": True, "already_in_state": True,
                "detail": f"质量扫描当前为 {scan.status}, 恢复请求无副作用"}
    _require_status(scan, "resume")
    from_status = scan.status
    retried = 0
    if from_status == "FAILED":
        # FAILED 恢复: 失败批次重新排队, 已 SUCCESS 批次保留(其问题保留)
        for sb in scan.batches:
            if sb.status == "FAILED":
                sb.status = "PENDING"
                sb.last_error = None
                sb.started_at = None
                retried += 1
        scan.failure_code = None
        scan.failure_reason = None
    scan.status = "QUEUED"
    scan.updated_by = operator
    scan.last_error = None
    scan.finished_at = None
    scan.current_batch_id = None
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id, event="scan.resume",
              operator=operator,
              reason=(f"恢复失败扫描: 重新排队, {retried} 个失败批次重试, 已完成批次保留"
                      if from_status == "FAILED"
                      else "恢复扫描: 重新排队, 从首个未完成批次续跑"),
              detail={"from_status": from_status, "retried_failed": retried})
    return {"ok": True,
            "detail": (f"质量扫描已恢复: {retried} 个失败批次重新排队"
                       if from_status == "FAILED"
                       else "质量扫描已恢复, 已重新排队等待执行")}


def _apply_cancel_scan(session: Session, scan: QualityScan, operator: str,
                       reason: str, *, event: str = "scan.cancel") -> int:
    """取消扫描的状态收尾(不提交, 供外层事务内联复用): 未开始批次 SKIPPED。
    返回跳过批次数。幂等: 已 CANCELED 直接返回 0。"""
    if scan.status == "CANCELED":
        return 0
    skipped = 0
    for sb in scan.batches:
        if sb.status == "PENDING":
            sb.status = "SKIPPED"
            sb.last_error = "扫描任务被取消, 批次不再扫描"
            skipped += 1
    scan.status = "CANCELED"
    scan.updated_by = operator
    scan.current_batch_id = None
    scan.finished_at = now_utc_naive()
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id, event=event,
              operator=operator, reason=reason, detail={"skipped": skipped})
    return skipped


def do_cancel_scan(session: Session, scan: QualityScan, operator: str) -> dict:
    if scan.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "质量扫描已取消, 重复取消无副作用"}
    _require_status(scan, "cancel")
    skipped = _apply_cancel_scan(
        session, scan, operator, "取消质量扫描: 未开始批次跳过")
    return {"ok": True, "detail": f"质量扫描已取消, {skipped} 个未开始批次跳过, 已有问题保留"}


# ---------- 质量结果失效后的自动重扫编排 ----------
#
# 触发 -> 唯一重扫 -> 计划暂停 -> 完成评估 -> 自动恢复 的闭环:
#
#   批次数据写入 / 规则新版本 / 扫描过期(worker 轮询)
#        │  enqueue_rescan(): 计划同时至多一个活动(自动或手动)扫描;
#        ▼                    重复触发合并进同一任务并追加触发原因
#   唯一 QualityScan(scan_source=auto_rescan, 覆盖计划全部批次)
#        │  计划执行器 tick 在步骤边界 assert_executor_gate():
#        ▼  门禁不通过 -> quality_hold 置位 + QualityGateHold(ACTIVE),
#   计划停在原步骤(PENDING/FAILED 不复位, 尝试计数/轮次保留), worker 每 tick 复评
#        │  重扫 COMPLETED 且阻断问题全部 FIXED/EXEMPTED(门禁 PASS)
#        ▼  QualityGateHold -> RESUMED, quality_hold 清除, 计划从原步骤继续
#   重扫 FAILED/CANCELED 或仍有 OPEN 阻断 -> 保持暂停(可 resume 扫描自愈)
#   计划取消 -> hold CANCELED, 活动重扫随计划取消, 永不自动恢复

# 失效来源 -> 人类可读说明
TRIGGER_TEXT = {
    "batch_data_write": "批次数据写入",
    "rule_version_change": "规则版本变化",
    "scan_expired": "扫描结果过期",
    "gate_blocked": "执行器门禁复核未通过",
    "scan_failed_retry": "重扫失败后的恢复重试",
}


def _trigger_entry(source: str, *, reason: str, batch_ids: list[str] | None = None,
                   operator: str = "system") -> dict:
    return {"source": source, "reason": reason[:500],
            "batch_ids": sorted(set(batch_ids or [])),
            "operator": operator, "at": now_utc_naive().isoformat()}


def _merge_trigger(existing: list | None, entry: dict) -> tuple[list, bool]:
    """把触发原因合并进任务的触发历史(按 source+批次集合去重), 返回(新列表, 是否新增)。"""
    out = list(existing or [])
    key = (entry["source"], tuple(entry["batch_ids"]))
    for t in out:
        if (t.get("source"), tuple(t.get("batch_ids") or [])) == key:
            return out, False
    out.append(entry)
    return out, True


def active_hold(session: Session, plan_id: str) -> QualityGateHold | None:
    return (session.query(QualityGateHold)
            .filter(QualityGateHold.plan_id == plan_id,
                    QualityGateHold.status == "ACTIVE")
            .order_by(QualityGateHold.id.desc()).first())


def enqueue_rescan(session: Session, plan_id: str, *, trigger: str,
                   reason: str, operator: str = "system",
                   affected_batch_ids: list[str] | None = None,
                   commit: bool = True) -> dict:
    """为受影响计划编排唯一的自动重扫任务(核心入口)。

    - 计划不存在/已取消/未绑定规则集/无步骤 -> 不编排, 返回 {enqueued: False, why};
    - 已有活动扫描(QUEUED/RUNNING/PAUSED): 不产生重复任务, 把本次触发原因
      去重合并进该任务的 triggers 与关联 hold, 返回已有 scan_id(merged:true);
    - 否则基于当前规则版本创建 auto_rescan 扫描(覆盖计划全部批次, 受影响范围
      记录在 affected_batch_ids), 取代最近一次扫描。

    并发安全: 创建判定段持有 Postgres 咨询锁(与手动扫描创建同一把锁),
    SQLite 由写事务串行; 任何时刻一个计划至多一个活动扫描, 重复触发无重复任务。
    """
    if trigger not in RESCAN_TRIGGERS:
        raise ValueError(f"未知重扫触发来源: {trigger}")
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        return {"enqueued": False, "why": "plan_missing"}
    if plan.status == "CANCELED":
        return {"enqueued": False, "why": "plan_canceled"}
    # fresh=True: 规则保存事务在提升版本号后同事务调用本函数, 必须读到
    # 刚 flush 的新版本(autoflush 关闭时身份映射可能保留旧的规则集版本号)。
    rv = current_rules(session, plan_id, fresh=True)
    if rv is None:
        return {"enqueued": False, "why": "ruleset_not_configured"}
    steps = plans.steps_of(session, plan_id)
    if not steps:
        return {"enqueued": False, "why": "no_steps"}
    affected = sorted(set(b for b in (affected_batch_ids or []) if b))
    entry = _trigger_entry(trigger, reason=reason, batch_ids=affected,
                           operator=operator)

    with _rescan_creation_lock:
        _lock_creation(session)
        active = _active_scan_for(session, plan_id)
        if active is not None:
            triggers, added = _merge_trigger(active.triggers, entry)
            if added:
                active.triggers = triggers
                active.updated_by = operator
                add_event(
                    session, plan_id=plan_id, scan_id=active.id,
                    event=("rescan.trigger_merged"
                           if active.scan_source == "auto_rescan"
                           else "rescan.trigger_attached"),
                    operator=operator,
                    reason=f"质量结果失效({TRIGGER_TEXT.get(trigger, trigger)}): "
                           f"复用进行中扫描 {active.id}({active.status}), 不创建重复任务",
                    detail={"trigger": trigger, "affected_batch_ids": affected,
                            "trigger_count": len(triggers)})
            hold = active_hold(session, plan_id)
            if hold is not None:
                h_triggers, h_added = _merge_trigger(hold.triggers, entry)
                if h_added:
                    hold.triggers = h_triggers
            if commit:
                session.commit()
            return {"enqueued": True, "created": False, "merged": True,
                    "scan_id": active.id, "status": active.status,
                    "trigger_added": added}

        latest = _latest_scan(session, plan_id)
        scan_id = "QS" + uuid.uuid4().hex[:10]
        ttl = default_ttl_seconds()
        scan = QualityScan(
            id=scan_id, plan_id=plan_id, ruleset_id=rv.ruleset_id,
            rule_version=rv.version, rule_digest=rv.content_digest,
            rules_snapshot=rv.rules, status="QUEUED",
            total_batches=len(steps), ttl_seconds=ttl,
            scan_source="auto_rescan", trigger_source=trigger,
            triggers=[entry], supersedes_scan_id=latest.id if latest else None,
            affected_batch_ids=affected,
            created_by=f"system:{operator}", updated_by=operator)
        session.add(scan)
        session.flush()
        _build_scan_rows(session, scan_id, plan_id, steps)
        add_event(session, plan_id=plan_id, scan_id=scan_id,
                  event="rescan.enqueue", operator=operator,
                  reason=f"质量结果失效({TRIGGER_TEXT.get(trigger, trigger)}), "
                         f"为计划自动编排重扫 {scan_id}(规则 v{rv.version}, "
                         f"{len(steps)} 个批次, 取代 {latest.id if latest else '无'})"
                         + (f", 受影响批次 {affected}" if affected else ""),
                  detail={"trigger": trigger, "rule_version": rv.version,
                          "total_batches": len(steps),
                          "affected_batch_ids": affected,
                          "supersedes_scan_id": latest.id if latest else None,
                          "ttl_seconds": ttl})
        add_event(session, plan_id=plan_id, scan_id=scan_id, event="scan.queue",
                  operator=operator,
                  reason=f"自动重扫已排队(并发上限 {max_concurrency()})")
        if commit:
            session.commit()
        return {"enqueued": True, "created": True, "merged": False,
                "scan_id": scan_id, "status": "QUEUED", "trigger": trigger}


def _current_step(session: Session, plan: MigrationPlan):
    """计划暂停时停留的步骤: 优先 failed_step_id, 否则第一个未完成步骤。"""
    steps = plans.steps_of(session, plan.id)
    if plan.failed_step_id is not None:
        st = next((s for s in steps if s.id == plan.failed_step_id), None)
        if st is not None:
            return st
    return next((s for s in steps
                 if s.status in ("PENDING", "FAILED", "RUNNING", "BLOCKED", "HALTED")),
                None)


def open_or_update_hold(session: Session, plan: MigrationPlan, *, trigger: str,
                        gate: dict, rescan_scan_id: str | None,
                        operator: str = "system") -> QualityGateHold:
    """执行器门禁失效时打开(或复用)质量暂停, 同步保证存在关联重扫。

    同一计划同时至多一段 ACTIVE hold; 门禁状态/关联重扫变化时更新暂停原因
    (如 STALE 等待重扫 -> 重扫完成仍有 BLOCKER), 原因历史只追加。
    暂停的首次触发来源优先取关联重扫记录的来源(数据写入/规则版本/过期),
    没有关联扫描时才是执行器兜底来源 gate_blocked。"""
    reason = "; ".join(gate.get("reasons") or []) or f"门禁状态 {gate.get('status')}"
    entry = _trigger_entry(trigger, reason=reason, batch_ids=[], operator=operator)
    # 首次触发来源以关联重扫为准(展示"为什么暂停")
    origin = trigger
    if rescan_scan_id:
        linked = session.get(QualityScan, rescan_scan_id)
        if linked is not None and linked.trigger_source:
            origin = linked.trigger_source
    hold = active_hold(session, plan.id)
    if hold is None:
        step = _current_step(session, plan)
        hold = QualityGateHold(
            id="QH" + uuid.uuid4().hex[:10], plan_id=plan.id,
            status="ACTIVE", trigger_source=origin,
            reason=reason[:500], reason_code=gate.get("status"),
            paused_at_step_id=step.id if step else None,
            paused_at_seq=step.seq if step else None,
            rescan_scan_id=rescan_scan_id, triggers=[entry],
            created_by=operator)
        session.add(hold)
        plan.quality_hold = True
        plans.plan_audit(
            session, plan, operator=operator, action="plan.quality_hold",
            step_id=step.id if step else None,
            reason=(f"质量门禁失效({gate.get('status')}), 计划在步骤边界暂停, "
                    f"等待自动重扫 {rescan_scan_id or '(编排中)'} 完成且阻断问题处理: "
                    + reason)[:500])
        add_event(session, plan_id=plan.id, scan_id=rescan_scan_id,
                  event="hold.open", operator=operator,
                  reason=f"计划因质量门禁 {gate.get('status')} 暂停于"
                         f"步骤 seq={step.seq if step else '-'}, "
                         f"关联重扫 {rescan_scan_id or '(无)'}",
                  detail={"gate_status": gate.get("status"),
                          "rescan_scan_id": rescan_scan_id,
                          "paused_at_seq": step.seq if step else None})
    else:
        changed = (hold.rescan_scan_id != rescan_scan_id
                   or hold.reason_code != gate.get("status"))
        hold.triggers, trig_added = _merge_trigger(hold.triggers, entry)
        if rescan_scan_id and hold.rescan_scan_id != rescan_scan_id:
            hold.rescan_scan_id = rescan_scan_id
        hold.reason_code = gate.get("status")
        hold.reason = reason[:500]
        if changed or trig_added:
            plans.plan_audit(
                session, plan, operator=operator, action="plan.quality_hold_update",
                step_id=hold.paused_at_step_id,
                reason=(f"质量暂停原因更新({gate.get('status')}), 关联重扫 "
                        f"{hold.rescan_scan_id or '(无)'}: {reason}")[:500])
            add_event(session, plan_id=plan.id, scan_id=hold.rescan_scan_id,
                      event="hold.update", operator=operator,
                      reason=f"质量暂停保持, 最新门禁状态 {gate.get('status')}",
                      detail={"gate_status": gate.get("status"),
                              "rescan_scan_id": hold.rescan_scan_id})
    session.flush()
    return hold


def _reconcile_hold(session: Session, plan: MigrationPlan,
                    operator: str = "system") -> bool:
    """复评 ACTIVE hold: 门禁重新 PASS -> 自动恢复(终态化 hold, 清标记);
    否则按最新门禁状态更新暂停原因。返回是否已恢复。"""
    hold = active_hold(session, plan.id)
    if hold is None:
        return False
    gate = evaluate_gate(session, plan.id)
    if gate_allows_execution(gate):
        at = now_utc_naive()
        hold.status = "RESUMED"
        hold.resume_mode = "auto_gate_pass"
        hold.resume_scan_id = gate.get("scan_id") or gate.get("latest_scan_id")
        hold.resumed_by = operator
        hold.resumed_at = at
        plan.quality_hold = False
        plans.plan_audit(
            session, plan, operator=operator, action="plan.quality_resume",
            step_id=hold.paused_at_step_id,
            reason=(f"自动重扫 {hold.resume_scan_id} 完成且阻断问题已处理"
                    f"(门禁 PASS), 计划从暂停时步骤 "
                    f"seq={hold.paused_at_seq or '-'} 自动继续")[:500])
        add_event(session, plan_id=plan.id, scan_id=hold.resume_scan_id,
                  event="hold.resume", operator=operator,
                  reason=f"门禁重新通过(依据扫描 {hold.resume_scan_id}), "
                         f"计划从步骤 seq={hold.paused_at_seq or '-'} 继续",
                  detail={"resume_scan_id": hold.resume_scan_id,
                          "paused_at_seq": hold.paused_at_seq})
        session.flush()
        return True
    # 未通过: 刷新暂停原因与关联(活动扫描可能是新的自动重扫)
    active = _active_scan_for(session, plan.id)
    open_or_update_hold(session, plan, trigger="gate_blocked", gate=gate,
                        rescan_scan_id=active.id if active else hold.rescan_scan_id,
                        operator=operator)
    return False


def dismiss_holds_on_cancel(session: Session, plan: MigrationPlan,
                            operator: str) -> dict:
    """计划取消(或进入其他终态)时: ACTIVE hold 置 CANCELED(不再自动恢复),
    活动(自动/手动)扫描随计划取消。须在计划状态已置 CANCELED 后、同一事务内调用,
    本函数不提交(由外层 do_cancel 的事务统一提交)。"""
    canceled_scans: list[str] = []
    hold = active_hold(session, plan.id)
    if hold is not None:
        hold.status = "CANCELED"
        hold.canceled_by = operator
        hold.canceled_at = now_utc_naive()
        plan.quality_hold = False
        add_event(session, plan_id=plan.id, scan_id=hold.rescan_scan_id,
                  event="hold.cancel", operator=operator,
                  reason="计划被取消, 质量门禁暂停终止, 不再自动恢复",
                  detail={"rescan_scan_id": hold.rescan_scan_id})
    for sc in (session.query(QualityScan)
               .filter(QualityScan.plan_id == plan.id,
                       QualityScan.status.in_(QUALITY_SCAN_ACTIVE_STATUSES)).all()):
        # 与计划取消同一事务, 不单独提交; RUNNING 批次扫描在批次边界自然收尾
        _apply_cancel_scan(
            session, sc, operator,
            f"计划 {plan.id} 被取消, 活动扫描 {sc.id} 随计划取消, 不再自动恢复",
            event="rescan.cancel_with_plan")
        canceled_scans.append(sc.id)
    if canceled_scans:
        add_event(session, plan_id=plan.id,
                  event="rescan.canceled_with_plan", operator=operator,
                  reason=f"计划取消, {len(canceled_scans)} 个活动扫描终止: "
                         + ", ".join(canceled_scans),
                  detail={"scan_ids": canceled_scans})
    return {"holds_canceled": 1 if hold is not None else 0,
            "scans_canceled": canceled_scans}


def gate_allows_execution(gate: dict) -> bool:
    """执行期放行: 门禁 PASS, 或计划未配置规则(NOT_CONFIGURED, 与启动前一致)。"""
    return bool(gate.get("passed")) and gate.get("status") in ("PASS", "NOT_CONFIGURED")


def assert_executor_gate(session: Session, plan: MigrationPlan) -> dict:
    """计划执行器在步骤边界(tick 取到候选步骤后)调用的运行时门禁。

    门禁 PASS/未配置规则: 若存在 ACTIVE hold 则自动恢复(清标记/落历史), 放行本 tick;
    门禁不通过: 保证存在唯一关联自动重扫(没有活动扫描时立即编排),
    打开/更新质量暂停(quality_hold), 计划本 tick 不推进(步骤保持原状)。
    返回 {blocked, gate, rescan_scan_id}。
    """
    gate = evaluate_gate(session, plan.id)
    if gate_allows_execution(gate):
        resumed = _reconcile_hold(session, plan)
        session.commit()
        return {"blocked": False, "gate": gate, "resumed": resumed}
    # 门禁未通过: 复用活动扫描, 否则编排一次自动重扫(执行器兜底来源)
    active = _active_scan_for(session, plan.id)
    if active is not None:
        rescan_id = active.id
    else:
        res = enqueue_rescan(
            session, plan.id, trigger="gate_blocked",
            reason="执行器在步骤边界复核质量门禁未通过: "
                   + "; ".join(gate.get("reasons") or [gate.get("status", "")]),
            operator="system", commit=False)
        rescan_id = res.get("scan_id")
        session.flush()
    open_or_update_hold(session, plan, trigger="gate_blocked", gate=gate,
                        rescan_scan_id=rescan_id, operator="system")
    session.commit()
    return {"blocked": True, "gate": gate, "rescan_scan_id": rescan_id}


def sweep_due_rescans(session: Session) -> list[dict]:
    """worker 轮询: 为"已绑定规则且处于执行态(RUNNING/HALTED)、质量结果需要
    重新扫描"的计划自动编排重扫。主要兜底扫描过期(TTL)场景 —— 数据写入与
    规则版本变化由对应操作即时触发, 这里按门禁实时状态补漏。

    同时复评所有 ACTIVE hold(重扫完成/失败/阻断处理后自动恢复或更新原因)。
    返回每个受影响计划的编排结果(测试可直接断言)。
    """
    results: list[dict] = []
    plans_rows = (session.query(MigrationPlan)
                  .filter(MigrationPlan.status.in_(("RUNNING", "HALTED")))
                  .order_by(MigrationPlan.id).all())
    for plan in plans_rows:
        rs = session.query(QualityRuleSet).filter(
            QualityRuleSet.plan_id == plan.id).first()
        if rs is None:
            continue
        gate = evaluate_gate(session, plan.id)
        hold = active_hold(session, plan.id)
        if gate_allows_execution(gate):
            if hold is not None:
                if _reconcile_hold(session, plan):
                    session.commit()
                    results.append({"plan_id": plan.id, "action": "hold_resumed",
                                    "scan_id": gate.get("scan_id")})
            continue
        active = _active_scan_for(session, plan.id)
        if active is not None:
            # 扫描在途(排队/执行/暂停): hold 原因随最新门禁刷新, 不重复创建
            if hold is not None:
                open_or_update_hold(session, plan, trigger="gate_blocked",
                                    gate=gate, rescan_scan_id=active.id,
                                    operator="system")
                session.commit()
            continue
        # 无在途扫描且门禁未通过: 判定触发来源并编排唯一重扫
        if gate["status"] in ("STALE", "NOT_SCANNED"):
            reasons = gate.get("reasons") or []
            if any("过期" in r for r in reasons):
                trigger, text = "scan_expired", "扫描结果超过有效期(TTL)"
            elif any("规则版本" in r for r in reasons):
                trigger, text = "rule_version_change", "规则版本已更新"
            elif any("数据" in r or "不存在" in r for r in reasons):
                trigger, text = "batch_data_write", "批次数据发生变化"
            else:
                trigger, text = "gate_blocked", "质量结果失效"
        elif gate["status"] in ("FAILED", "CANCELED"):
            trigger, text = "scan_failed_retry", f"最近扫描 {gate['status'].lower()}"
        else:
            # BLOCKED / RUNNING / NOT_CONFIGURED 等: BLOCKED 靠修复/豁免解决,
            # RUNNING 等扫描自然完成; 不额外编排(避免无意义重扫)
            if hold is not None and gate["status"] != "RUNNING":
                open_or_update_hold(session, plan, trigger="gate_blocked",
                                    gate=gate,
                                    rescan_scan_id=hold.rescan_scan_id,
                                    operator="system")
                session.commit()
            continue
        res = enqueue_rescan(session, plan.id, trigger=trigger, reason=text,
                             operator="system", commit=False)
        session.flush()
        if hold is not None:
            open_or_update_hold(session, plan, trigger=trigger, gate=gate,
                                rescan_scan_id=res.get("scan_id"),
                                operator="system")
        session.commit()
        results.append({"plan_id": plan.id, "action": "rescan_enqueued",
                        "trigger": trigger, **{k: v for k, v in res.items()
                                               if k in ("scan_id", "created", "merged")}})
    return results


# ---------- 失效来源即时通知 ----------

def plans_using_batch(session: Session, batch_id: str) -> list[MigrationPlan]:
    """把批次纳入步骤、且计划未终结(CANCELED/COMPLETED)的计划。"""
    return (session.query(MigrationPlan)
            .join(PlanStep, PlanStep.plan_id == MigrationPlan.id)
            .filter(PlanStep.batch_id == batch_id,
                    MigrationPlan.status.notin_(plans.TERMINAL_PLAN_STATUSES))
            .order_by(MigrationPlan.id).all())


def notify_batch_data_written(session: Session, batch_id: str, *,
                              record_id: int | None = None,
                              operator: str = "api") -> list[dict]:
    """批次范围内旧表发生写入后调用: 为把该批次纳入步骤的执行态计划自动编排
    重扫(跨批次影响: 重扫覆盖计划全部批次, 触发原因记录受影响批次)。

    DRAFT 计划沿用既有"门禁实时判定 STALE + 管理员手动扫描"流程, 不自动排队;
    PAUSED 是用户主动暂停, 同样不自动排队(恢复时执行器门禁会兜底编排)。
    """
    out: list[dict] = []
    for plan in plans_using_batch(session, batch_id):
        if plan.status not in ("RUNNING", "HALTED"):
            continue
        rs = session.query(QualityRuleSet).filter(
            QualityRuleSet.plan_id == plan.id).first()
        if rs is None:
            continue
        reason = (f"批次 {batch_id} 范围内记录"
                  + (f" {record_id} " if record_id is not None else " ")
                  + "发生写入, 扫描数据指纹将漂移/已漂移")
        res = enqueue_rescan(session, plan.id, trigger="batch_data_write",
                             reason=reason, operator=operator,
                             affected_batch_ids=[batch_id], commit=True)
        out.append({"plan_id": plan.id, **res})
    return out


def notify_rule_version_changed(session: Session, plan_id: str, *,
                                new_version: int, operator: str = "system",
                                commit: bool = True) -> dict:
    """规则新版本发布后调用: 执行态计划立即编排基于新版本的重扫;
    DRAFT 计划由门禁实时判定 STALE, 保持管理员手动发起的既有流程。"""
    plan = session.get(MigrationPlan, plan_id)
    if plan is None or plan.status not in ("RUNNING", "HALTED"):
        return {"enqueued": False, "why": "plan_not_active"}
    return enqueue_rescan(
        session, plan_id, trigger="rule_version_change",
        reason=f"规则集发布新版本 v{new_version}, 旧扫描结果不再放行",
        operator=operator, commit=commit)


# ---------- 修复批次(逐项重跑规则核验, 历史只追加) ----------

def _latest_completed_scan(session: Session, plan_id: str) -> QualityScan | None:
    return (session.query(QualityScan)
            .filter(QualityScan.plan_id == plan_id,
                    QualityScan.status == "COMPLETED")
            .order_by(QualityScan.finished_at.desc(), QualityScan.id.desc())
            .first())


def _active_exemptions_map(session: Session, plan_id: str,
                           rule_version: int) -> dict[tuple, QualityExemption]:
    """计划内某规则版本的有效(APPROVED)豁免, 键 (batch_id, rule_id, record_id, field)。"""
    rows = (session.query(QualityExemption)
            .filter(QualityExemption.plan_id == plan_id,
                    QualityExemption.rule_version == rule_version,
                    QualityExemption.status == "APPROVED").all())
    return {(e.batch_id, e.rule_id, e.record_id, e.field): e for e in rows}


def do_create_fix(session: Session, _ignored, operator: str, plan_id: str,
                  issue_ids: list[str], note: str | None) -> dict:
    """创建修复批次: 逐项在当前数据上重跑问题对应规则做核验。

    RESOLVED(违规消失)->问题 FIXED; STILL_OPEN(仍违规)/NOT_FOUND(记录不存在)
    不改问题状态; 问题必须属于该计划最近一次 COMPLETED 扫描且当前为 OPEN。
    修复批次只追加, 与扫描规则版本绑定。逐项独立结论, 非法项 REJECTED 并给原因。
    """
    get_plan(session, plan_id)
    ids = list(dict.fromkeys(i.strip() for i in issue_ids if i and i.strip()))
    if not ids:
        raise QualityStateError("修复批次至少包含一个问题 id")
    scan = _latest_completed_scan(session, plan_id)
    if scan is None:
        raise QualityStateError(f"计划 {plan_id} 还没有 COMPLETED 的质量扫描, 无可修复问题")
    rules = list(scan.rules_snapshot or [])
    results: list[dict] = []
    n_resolved = n_still = n_missing = n_rejected = 0
    at = now_utc_naive()
    fix_id = "QF" + uuid.uuid4().hex[:10]
    for iid in ids:
        issue = session.get(QualityIssue, iid)
        if issue is None:
            n_rejected += 1
            results.append({"issue_id": iid, "verdict": "REJECTED",
                            "reason": f"问题 {iid} 不存在"})
            continue
        if issue.plan_id != plan_id or issue.scan_id != scan.id:
            n_rejected += 1
            results.append({"issue_id": iid, "verdict": "REJECTED",
                            "reason": f"问题 {iid} 不属于计划 {plan_id} 的最近一次扫描 {scan.id}"})
            continue
        if issue.status != "OPEN":
            n_rejected += 1
            results.append({
                "issue_id": iid, "verdict": "REJECTED",
                "reason": (f"问题已被豁免({issue.status})" if issue.status == "EXEMPTED"
                           else f"问题已修复({issue.status}), 无需重复修复")})
            continue
        rule = rule_for(rules, issue.rule_id)
        if rule is None:
            n_rejected += 1
            results.append({"issue_id": iid, "verdict": "REJECTED",
                            "reason": f"扫描规则快照中找不到规则 {issue.rule_id}"})
            continue
        batch = session.get(MigrationBatch, issue.batch_id)
        rec = session.get(RecordOld, issue.record_id) if batch is not None else None
        if batch is None or rec is None or not (batch.id_start <= issue.record_id <= batch.id_end):
            verdict = "NOT_FOUND"
            n_missing += 1
            results.append({"issue_id": iid, "verdict": verdict,
                            "record_id": issue.record_id, "rule_id": issue.rule_id,
                            "reason": "记录在当前批次范围内已不存在"})
            continue
        violations = eval_rule(rule, _record_view(rec))
        still = any(v["field"] == issue.field for v in violations)
        if still:
            verdict = "STILL_OPEN"
            n_still += 1
            results.append({"issue_id": iid, "verdict": verdict,
                            "record_id": issue.record_id, "rule_id": issue.rule_id,
                            "reason": "重跑规则仍违规, 问题保持 OPEN"})
        else:
            verdict = "RESOLVED"
            n_resolved += 1
            issue.status = "FIXED"
            issue.resolution_type = "fix"
            issue.resolution_id = fix_id
            issue.resolved_by = operator
            issue.resolved_at = at
            results.append({"issue_id": iid, "verdict": verdict,
                            "record_id": issue.record_id, "rule_id": issue.rule_id,
                            "reason": "重跑规则已通过, 问题标记为 FIXED"})
            add_event(session, plan_id=plan_id, scan_id=scan.id,
                      event="issue.fix_resolved", operator=operator,
                      reason=f"问题 {iid}(批次 {issue.batch_id} 记录 {issue.record_id} "
                             f"规则 {issue.rule_id}) 修复核验通过",
                      detail={"fix_batch_id": fix_id, "rule_version": scan.rule_version})
    session.add(QualityFixBatch(
        id=fix_id, plan_id=plan_id, scan_id=scan.id,
        rule_version=scan.rule_version, operator=operator,
        note=(note[:500] if note else None), total=len(results),
        resolved=n_resolved, still_open=n_still, not_found=n_missing,
        rejected=n_rejected, results=results))
    _refresh_scan_counters(session, scan)
    add_event(session, plan_id=plan_id, scan_id=scan.id, event="fix.create",
              operator=operator,
              reason=f"创建修复批次 {fix_id}: {len(results)} 项, 解决 {n_resolved}, "
                     f"仍违规 {n_still}, 记录缺失 {n_missing}, 拒绝 {n_rejected}"
                     + (f"({note})" if note else ""),
              detail={"fix_batch_id": fix_id, "resolved": n_resolved,
                      "still_open": n_still, "not_found": n_missing,
                      "rejected": n_rejected, "rule_version": scan.rule_version})
    # 修复会改变批次数据指纹: 执行态计划即时编排重扫(数据写入类失效),
    # DRAFT 计划保持"门禁 STALE -> 手动重扫"的既有流程。
    plan = session.get(MigrationPlan, plan_id)
    if plan is not None and plan.status in ("RUNNING", "HALTED"):
        affected = sorted({
            (session.get(QualityIssue, r["issue_id"]).batch_id)
            for r in results if r.get("verdict") == "RESOLVED"
            and session.get(QualityIssue, r.get("issue_id")) is not None})
        enqueue_rescan(
            session, plan_id, trigger="batch_data_write",
            reason=f"修复批次 {fix_id} 改变了批次数据, 指纹将漂移, 需重新扫描",
            operator=operator, affected_batch_ids=affected, commit=False)
    session.flush()
    return {"ok": n_rejected == 0, "fix_batch_id": fix_id, "scan_id": scan.id,
            "rule_version": scan.rule_version,
            "progress": {"total": len(results), "resolved": n_resolved,
                         "still_open": n_still, "not_found": n_missing,
                         "rejected": n_rejected},
            "results": results,
            "detail": f"修复批次 {fix_id} 完成: 解决 {n_resolved}/{len(results)}, "
                      f"仍违规 {n_still}, 记录缺失 {n_missing}, 拒绝 {n_rejected}; "
                      f"数据已变化, 需重新扫描后门禁才会重新评估"}


# ---------- 豁免申请(带原因, 提交即批准; 可撤销; 历史只追加) ----------

def _do_exempt(session: Session, plan_id: str, issue: QualityIssue, scan: QualityScan,
               operator: str, reason: str) -> QualityExemption:
    exemption_id = "QE" + uuid.uuid4().hex[:10]
    ex = QualityExemption(
        id=exemption_id, plan_id=plan_id, issue_id=issue.id, scan_id=scan.id,
        batch_id=issue.batch_id, rule_id=issue.rule_id, record_id=issue.record_id,
        field=issue.field, rule_version=scan.rule_version,
        reason=reason[:500], status="APPROVED", created_by=operator)
    session.add(ex)
    issue.status = "EXEMPTED"
    issue.resolution_type = "exemption"
    issue.resolution_id = exemption_id
    issue.resolved_by = operator
    issue.resolved_at = now_utc_naive()
    add_event(session, plan_id=plan_id, scan_id=scan.id,
              event="issue.exempt", operator=operator,
              reason=f"问题 {issue.id}(批次 {issue.batch_id} 记录 {issue.record_id} "
                     f"规则 {issue.rule_id}) 豁免批准: {reason}",
              detail={"exemption_id": exemption_id,
                      "rule_version": scan.rule_version})
    return ex


def do_create_exemption(session: Session, _ignored, operator: str, plan_id: str,
                        issue_id: str, reason: str) -> dict:
    """为阻断问题提交带原因的豁免申请(提交即批准), 绑定扫描时规则版本。

    重复豁免(问题已 EXEMPTED 且豁免有效)幂等回显; 已 FIXED/非阻断/不属于最近
    一次扫描的问题拒绝。豁免只追加, 撤销不删除历史。
    """
    get_plan(session, plan_id)
    issue = session.get(QualityIssue, issue_id)
    if issue is None:
        raise QualityNotFound(f"质量问题 {issue_id} 不存在")
    if issue.plan_id != plan_id:
        raise QualityStateError(f"问题 {issue_id} 不属于计划 {plan_id}")
    scan = session.get(QualityScan, issue.scan_id)
    if scan is None or scan.plan_id != plan_id:
        raise QualityStateError(f"问题 {issue_id} 的扫描任务不存在或不属于计划 {plan_id}")
    if issue.status == "EXEMPTED":
        prior = (session.query(QualityExemption)
                 .filter(QualityExemption.issue_id == issue.id,
                         QualityExemption.status == "APPROVED")
                 .order_by(QualityExemption.id.desc()).first())
        if prior is not None:
            return {"ok": True, "already_in_state": True,
                    "exemption_id": prior.id, "issue_id": issue.id,
                    "status": "APPROVED",
                    "detail": f"问题已有有效豁免 {prior.id}(原因: {prior.reason}), 重复豁免无副作用"}
        # 豁免曾被撤销: 重新提交视为新申请
    if issue.status == "FIXED":
        raise QualityStateError(f"问题 {issue_id} 已通过修复关闭(FIXED), 不能豁免")
    if issue.severity != "BLOCKER":
        raise QualityStateError(
            f"问题 {issue_id} 严重级别为 {issue.severity}, 只有阻断级(BLOCKER)问题需要豁免")
    if scan.status != "COMPLETED":
        raise QualityStateError(
            f"问题所属扫描 {scan.id} 当前状态 {scan.status}, 只有 COMPLETED 扫描的问题可豁免")
    ex = _do_exempt(session, plan_id, issue, scan, operator, reason)
    _refresh_scan_counters(session, scan)
    # 豁免不改数据、不产生新扫描: 若执行器正因门禁暂停, 立即复评 —— 全部阻断
    # 问题处理完时计划可凭当前有效扫描自动恢复
    plan = session.get(MigrationPlan, plan_id)
    if plan is not None and active_hold(session, plan_id) is not None:
        _reconcile_hold(session, plan)
    session.flush()
    return {"ok": True, "exemption_id": ex.id, "issue_id": issue.id,
            "status": "APPROVED", "rule_version": scan.rule_version,
            "detail": f"豁免 {ex.id} 已批准(规则 v{scan.rule_version}): {reason}"}


def do_revoke_exemption(session: Session, _ignored, operator: str, plan_id: str,
                        exemption_id: str, reason: str) -> dict:
    """撤销有效豁免: 豁免置 REVOKED(历史保留), 对应问题回到 OPEN;
    撤销后门禁重新要求该阻断问题被处理。重复撤销幂等。"""
    get_plan(session, plan_id)
    ex = session.get(QualityExemption, exemption_id)
    if ex is None:
        raise QualityNotFound(f"豁免 {exemption_id} 不存在")
    if ex.plan_id != plan_id:
        raise QualityStateError(f"豁免 {exemption_id} 不属于计划 {plan_id}")
    if ex.status == "REVOKED":
        return {"ok": True, "already_in_state": True,
                "exemption_id": ex.id, "status": "REVOKED",
                "detail": "豁免已撤销, 重复撤销无副作用"}
    ex.status = "REVOKED"
    ex.revoked_by = operator
    ex.revoked_at = now_utc_naive()
    ex.revoke_reason = reason[:500]
    issue = session.get(QualityIssue, ex.issue_id)
    if issue is not None and issue.status == "EXEMPTED" and issue.resolution_id == ex.id:
        issue.status = "OPEN"
        issue.resolution_type = None
        issue.resolution_id = None
        issue.resolved_by = None
        issue.resolved_at = None
        scan = session.get(QualityScan, ex.scan_id)
        if scan is not None:
            _refresh_scan_counters(session, scan)
    # 撤销豁免后阻断问题重新 OPEN: 刷新执行器暂停原因(门禁回到 BLOCKED),
    # 不自动创建重扫(数据未变, 管理员修复或重新豁免即可继续)
    plan = session.get(MigrationPlan, plan_id)
    if plan is not None and active_hold(session, plan_id) is not None:
        _reconcile_hold(session, plan)
    add_event(session, plan_id=plan_id, scan_id=ex.scan_id,
              event="issue.exempt_revoke", operator=operator,
              reason=f"撤销豁免 {exemption_id}(问题 {ex.issue_id}): {reason}",
              detail={"exemption_id": exemption_id})
    session.flush()
    return {"ok": True, "exemption_id": ex.id, "status": "REVOKED",
            "detail": f"豁免 {exemption_id} 已撤销, 对应阻断问题重新打开(OPEN)"}


# ---------- 扫描执行: 调度 + 批次扫描 ----------

def claim_due_scans(session: Session) -> list[str]:
    """按并发额度把 QUEUED 扫描认领为 RUNNING, 返回认领的扫描 id(已提交)。"""
    _lock_scheduling(session)
    try:
        running = (session.query(func.count(QualityScan.id))
                   .filter(QualityScan.status == "RUNNING").scalar()) or 0
        slots = max_concurrency() - running
        if slots <= 0:
            session.rollback()
            return []
        due = (session.query(QualityScan)
               .filter(QualityScan.status == "QUEUED")
               .order_by(QualityScan.created_at, QualityScan.id)
               .limit(slots).all())
        claimed: list[str] = []
        at = now_utc_naive()
        for sc in due:
            sc.status = "RUNNING"
            sc.started_at = sc.started_at or at
            sc.updated_by = "system"
            add_event(session, plan_id=sc.plan_id, scan_id=sc.id, event="scan.claim",
                      operator="system",
                      reason=f"获得并发额度, 开始执行(并发上限 {max_concurrency()})")
            claimed.append(sc.id)
        session.commit()
        return claimed
    except Exception:
        session.rollback()
        raise


def _scan_batches(session: Session, scan_id: str) -> list[QualityScanBatch]:
    return (session.query(QualityScanBatch)
            .filter(QualityScanBatch.scan_id == scan_id)
            .order_by(QualityScanBatch.seq).all())


def _refresh_scan_counters(session: Session, scan: QualityScan) -> None:
    """按问题表重算扫描的问题计数(修复/豁免后门禁与页面数据保持一致)。

    SessionLocal 关闭了 autoflush, 查询前必须显式 flush, 否则同事务内新增/
    变更的问题行不会被计数。
    """
    session.flush()
    issues = (session.query(QualityIssue)
              .filter(QualityIssue.scan_id == scan.id).all())
    scan.total_issues = len(issues)
    scan.blocker_issues = sum(1 for i in issues if i.severity == "BLOCKER")
    scan.warning_issues = sum(1 for i in issues if i.severity == "WARNING")
    scan.info_issues = sum(1 for i in issues if i.severity == "INFO")
    scan.open_blocker_issues = sum(
        1 for i in issues if i.severity == "BLOCKER" and i.status == "OPEN")


def _fail_scan(session: Session, scan: QualityScan, sb: QualityScanBatch | None,
               code: str, reason: str, operator: str = "system") -> None:
    """致命失败收尾: 当前批次 FAILED(若有), 任务 FAILED, 其余未开始批次 SKIPPED;
    已 SUCCESS 批次的问题原样保留。"""
    scan = _lock_scan(session, scan.id)
    if scan.status in QUALITY_SCAN_TERMINAL_STATUSES:
        session.rollback()
        return
    at = now_utc_naive()
    if sb is not None:
        cur = session.get(QualityScanBatch, sb.id)
        if cur is not None and cur.status == "RUNNING":
            cur.status = "FAILED"
            cur.last_error = f"[{code}] {reason}"[:500]
            cur.finished_at = at
    skipped = 0
    for other in _scan_batches(session, scan.id):
        if other.status == "PENDING":
            other.status = "SKIPPED"
            other.last_error = f"任务因 {code} 失败, 批次未执行"
            skipped += 1
    scan.status = "FAILED"
    scan.failure_code = code
    scan.failure_reason = f"[{code}] {reason}"[:500]
    scan.last_error = scan.failure_reason
    scan.current_batch_id = None
    scan.finished_at = at
    scan.updated_by = operator
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id, event="scan.failed",
              operator=operator, reason=scan.failure_reason,
              detail={"code": code, "skipped_batches": skipped,
                      "scan_source": scan.scan_source})
    # 自动重扫失败: 关联的质量暂停不解除, 刷新原因为扫描失败(可 resume 扫描自愈,
    # 恢复成功且门禁通过后计划自动继续)
    plan = session.get(MigrationPlan, scan.plan_id)
    if plan is not None and active_hold(session, scan.plan_id) is not None:
        gate = evaluate_gate(session, scan.plan_id)
        open_or_update_hold(session, plan, trigger="gate_blocked", gate=gate,
                            rescan_scan_id=scan.id, operator="system")
    session.commit()


def _execute_batch(session: Session, scan: QualityScan,
                   sb: QualityScanBatch) -> None:
    """扫描单个批次: RUNNING 边界先提交, 再扫描旧表并落问题明细与样本。"""
    at = now_utc_naive()
    sb.status = "RUNNING"
    sb.started_at = at
    sb.last_error = None
    scan.current_batch_id = sb.batch_id
    scan.updated_by = "system"
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
              event="scan.batch_start", operator="system",
              reason=f"开始扫描批次 {sb.batch_id}(seq={sb.seq})",
              detail={"batch_id": sb.batch_id, "seq": sb.seq})
    session.commit()  # 崩溃边界: 重启后 RUNNING 扫描/批次被复位到安全位置

    try:
        # 规则版本必须仍存在且摘要未变(扫描期间发布了新版本 -> 本次结果已过期, 明确失败)
        rv = (session.query(QualityRuleVersion)
              .filter(QualityRuleVersion.ruleset_id == scan.ruleset_id,
                      QualityRuleVersion.version == scan.rule_version).first())
        if rv is None:
            raise _ScanFatal(
                "rule_version_missing",
                f"规则版本 v{scan.rule_version} 已不存在, 扫描无法继续, 请重新发起扫描")
        if rv.content_digest != scan.rule_digest:
            raise _ScanFatal(
                "rule_version_changed",
                f"规则版本 v{scan.rule_version} 内容已变化, 本次扫描结果过期, 请重新发起扫描")
        batch = session.get(MigrationBatch, sb.batch_id)
        if batch is None:
            raise _ScanFatal(
                "batch_missing",
                f"步骤 seq={sb.seq} 的批次 {sb.batch_id} 已不存在, 扫描无法继续")
        rules = list(rv.rules or [])
        records = batch_records(session, batch).all()
        # 该 规则×批次 的样本计数(超过上限仍计数, 明细截断)
        sample_counts: dict[str, int] = {}
        issue_count = blocker_count = 0
        for rec in records:
            view = _record_view(rec)
            for v in eval_rules_against_view(rules, view):
                issue_count += 1
                if v["severity"] == "BLOCKER":
                    blocker_count += 1
                n_seen = sample_counts.get(v["rule_id"], 0)
                keep_sample = n_seen < MAX_SAMPLES_PER_RULE_BATCH
                sample_counts[v["rule_id"]] = n_seen + 1
                session.add(QualityIssue(
                    id="QI" + uuid.uuid4().hex[:12], plan_id=scan.plan_id,
                    scan_id=scan.id, batch_id=batch.id, record_id=rec.id,
                    rule_version=scan.rule_version, rule_id=v["rule_id"],
                    rule_name=v["rule_name"], rule_type=v["rule_type"],
                    severity=v["severity"], field=v["field"],
                    message=v["message"][:500],
                    sample=(view if keep_sample else None),
                    status="OPEN"))
        fingerprint, count = fingerprint_batch(session, batch)
    except _ScanFatal as e:
        session.rollback()
        _fail_scan(session, scan, sb, e.code, e.reason)
        return
    except Exception as e:  # 防御: 意外错误明确失败留痕
        session.rollback()
        _fail_scan(session, scan, sb, "internal_error",
                   f"扫描批次 {sb.batch_id} 时发生未预期错误: {e}")
        return

    # 扫描期间任务可能已被暂停/取消: 尊重控制状态收尾
    scan = _lock_scan(session, scan.id)
    sb = session.get(QualityScanBatch, sb.id)
    at = now_utc_naive()

    if scan.status == "CANCELED":
        sb.status = "SKIPPED"
        sb.last_error = "批次扫描时任务已被取消, 问题不计入"
        sb.finished_at = at
        scan.current_batch_id = None
        scan.finished_at = at
        add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
                  event="scan.batch_skip", operator="system",
                  reason=f"批次 {sb.batch_id} 扫描期间任务被取消")
        session.commit()
        return

    sb.status = "SUCCESS"
    sb.record_count = count
    sb.issue_count = issue_count
    sb.blocker_count = blocker_count
    sb.data_fingerprint = fingerprint
    sb.biz = batch.biz
    sb.finished_at = at
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
              event="scan.batch_success", operator="system",
              reason=f"批次 {batch.id} 扫描完成: {count} 条记录, {issue_count} 个问题"
                     f"(阻断 {blocker_count})",
              detail={"batch_id": batch.id, "records": count,
                      "issues": issue_count, "blockers": blocker_count,
                      "data_fingerprint": fingerprint})

    sbs = _scan_batches(session, scan.id)
    scan.completed_batches = sum(1 for x in sbs if x.status == "SUCCESS")
    scan.total_records = sum(x.record_count for x in sbs if x.status == "SUCCESS")
    _refresh_scan_counters(session, scan)
    scan.updated_by = "system"

    if scan.status == "PAUSED":
        scan.current_batch_id = None
        add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
                  event="scan.pause.boundary", operator="system",
                  reason=f"批次 {sb.batch_id} 扫描完成, 任务在批次边界暂停, 恢复后续跑")
        session.commit()
        return

    if all(x.status in ("SUCCESS", "SKIPPED") for x in sbs):
        _complete_scan(session, scan, sbs)
    else:
        scan.current_batch_id = next(
            (x.batch_id for x in sbs if x.status == "PENDING"), None)
    session.commit()


def _complete_scan(session: Session, scan: QualityScan,
                   sbs: list[QualityScanBatch]) -> None:
    """全部批次扫描完成: 继承同规则版本的有效豁免(新扫描中的相同问题直接豁免),
    置 COMPLETED 并按 TTL 计算过期时间。"""
    # 继承历史有效豁免: 同计划、同规则版本、同 批次/规则/记录/字段
    exempt_map = _active_exemptions_map(session, scan.plan_id, scan.rule_version)
    inherited = 0
    if exempt_map:
        issues = (session.query(QualityIssue)
                  .filter(QualityIssue.scan_id == scan.id,
                          QualityIssue.status == "OPEN").all())
        at = now_utc_naive()
        for issue in issues:
            ex = exempt_map.get((issue.batch_id, issue.rule_id,
                                 issue.record_id, issue.field))
            if ex is not None:
                issue.status = "EXEMPTED"
                issue.resolution_type = "exemption"
                issue.resolution_id = ex.id
                issue.resolved_by = f"system:inherited:{ex.created_by}"
                issue.resolved_at = at
                inherited += 1
    _refresh_scan_counters(session, scan)
    at = now_utc_naive()
    scan.status = "COMPLETED"
    scan.current_batch_id = None
    scan.finished_at = at
    scan.expires_at = at + timedelta(seconds=scan.ttl_seconds or default_ttl_seconds())
    scan.last_error = None
    gate = "BLOCKED" if scan.open_blocker_issues else "PASS"
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
              event="scan.complete", operator="system",
              reason=f"全部 {len(sbs)} 个批次扫描完成: {scan.total_records} 条记录, "
                     f"{scan.total_issues} 个问题(阻断 {scan.blocker_issues}, "
                     f"待处理阻断 {scan.open_blocker_issues}"
                     + (f", 继承历史豁免 {inherited} 项" if inherited else "")
                     + (f", 自动重扫(首次来源 {scan.trigger_source})"
                        if scan.scan_source == "auto_rescan" else "")
                     + f"), 门禁初判 {gate}, 结果有效期至 {scan.expires_at.isoformat()}Z",
              detail={"total_issues": scan.total_issues,
                      "blockers": scan.blocker_issues,
                      "open_blockers": scan.open_blocker_issues,
                      "inherited_exemptions": inherited,
                      "scan_source": scan.scan_source,
                      "trigger_source": scan.trigger_source,
                      "gate": gate, "expires_at": scan.expires_at.isoformat()})
    # 自动重扫完成后立即复评质量暂停: 门禁 PASS 则计划自动从原步骤恢复,
    # 否则刷新暂停原因(仍有阻断/结果再次失效)。由外层统一提交, 这里不 commit。
    plan = session.get(MigrationPlan, scan.plan_id)
    if plan is not None and active_hold(session, scan.plan_id) is not None:
        _reconcile_hold(session, plan)


def run_scan_tick(session: Session, scan_id: str) -> bool:
    """推进一个 RUNNING 扫描至多一个 PENDING 批次。返回是否执行了批次。
    暂停/取消在批次边界检查。"""
    scan = _lock_scan(session, scan_id)
    if scan.status != "RUNNING":
        session.rollback()
        return False
    sbs = _scan_batches(session, scan_id)
    candidate = next((x for x in sbs if x.status == "PENDING"), None)
    if candidate is None:
        if all(x.status in ("SUCCESS", "SKIPPED", "FAILED") for x in sbs):
            pending_skips = [x for x in sbs if x.status == "PENDING"]
            for x in pending_skips:
                x.status = "SKIPPED"
            if scan.status == "RUNNING":
                _complete_scan(session, scan, sbs)
                session.commit()
                return True
        session.rollback()
        return False
    _execute_batch(session, scan, candidate)
    return True


# ---------- 重启对账 ----------

def boot_recover_scans(session: Session) -> None:
    """服务重启时:
    1. 遗留 RUNNING 扫描(执行线程已死)回到 QUEUED 重新参与并发排队;
       其遗留 RUNNING 批次复位为 PENDING(问题尚未提交, 安全重扫), SUCCESS 批次不丢;
    2. QUEUED/PAUSED/终态扫描保持不变(排队/暂停是持久化的用户态);
    3. 质量暂停标记(plan.quality_hold)与 quality_gate_holds 的 ACTIVE 行对账:
       待处理重扫(QUEUED/PAUSED/FAILED)与暂停状态跨重启继续保留, worker 续跑;
       计划已取消/完成但标记残留时收敛, 无 ACTIVE hold 的标记清除。"""
    scans = session.query(QualityScan).order_by(QualityScan.id).all()
    for scan in scans:
        if scan.status != "RUNNING":
            continue
        scan.status = "QUEUED"
        scan.current_batch_id = None
        scan.updated_by = "system"
        for sb in _scan_batches(session, scan.id):
            if sb.status == "RUNNING":
                sb.status = "PENDING"
                sb.started_at = None
                sb.last_error = "服务重启时该批次正在扫描, 已复位为待执行(问题未提交, 安全重扫)"
                add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
                          event="scan.boot_reset_batch", operator="system",
                          reason=f"重启恢复: 批次 {sb.batch_id} 复位为 PENDING")
        add_event(session, plan_id=scan.plan_id, scan_id=scan.id,
                  event="scan.boot", operator="system",
                  reason="服务重启: RUNNING 质量扫描回到排队位置, 已完成批次问题保留, worker 将续跑")
        session.commit()
    # 暂停标记对账: 待处理重扫与 ACTIVE hold 跨重启保留; 残留标记收敛
    for plan in session.query(MigrationPlan).order_by(MigrationPlan.id).all():
        hold = active_hold(session, plan.id)
        if hold is not None:
            if plan.status in plans.TERMINAL_PLAN_STATUSES:
                # 计划已终结但 hold 残留(理论上取消路径已收敛): 终止 hold
                hold.status = "CANCELED"
                hold.canceled_by = "system"
                hold.canceled_at = now_utc_naive()
                plan.quality_hold = False
                add_event(session, plan_id=plan.id, scan_id=hold.rescan_scan_id,
                          event="hold.boot_cancel", operator="system",
                          reason=f"重启对账: 计划已 {plan.status}, 残留质量暂停终止")
                session.commit()
            elif not plan.quality_hold:
                plan.quality_hold = True
                add_event(session, plan_id=plan.id, scan_id=hold.rescan_scan_id,
                          event="hold.boot_restore", operator="system",
                          reason="重启对账: 恢复质量门禁暂停标记, 待处理重扫继续保留")
                session.commit()
        elif plan.quality_hold:
            plan.quality_hold = False
            session.commit()


# ---------- 后台 worker ----------

class QualityWorker:
    """单实例后台线程: 认领排队扫描(受并发闸门)并各推进一个批次。

    与 ReplayWorker 相同的单副本假设: SQLite 写锁 / Postgres 咨询锁与行锁
    串行化 worker 与管理动作, 不会越过并发上限或重复执行批次。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("QUALITY_WORKER_POLL_INTERVAL", "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="quality-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        """认领到期扫描并给每个 RUNNING 扫描(含刚认领的)推进一个批次, 返回执行批次数。
        先做失效兜底: 过期/版本变化的执行态计划编排唯一重扫、复评质量暂停。"""
        from .db import SessionLocal
        db = SessionLocal()
        try:
            sweep_due_rescans(db)  # 过期(TTL)等兜底场景, 内部自行提交
        except Exception:
            db.rollback()
        finally:
            db.close()
        db = SessionLocal()
        try:
            claimed = claim_due_scans(db)
            run_ids = [r[0] for r in (db.query(QualityScan.id)
                                      .filter(QualityScan.status == "RUNNING")
                                      .order_by(QualityScan.id).all())]
        finally:
            db.close()
        scan_ids = list(claimed) + [i for i in run_ids if i not in claimed]
        advanced = 0
        for sid in scan_ids:
            db = SessionLocal()
            try:
                if run_scan_tick(db, sid):
                    advanced += 1
            except Exception:  # worker 绝不因单个扫描异常退出; 失败已在任务上留痕
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


# ---------- 门禁评估 ----------

def _scan_stale_reasons(session: Session, scan: QualityScan) -> list[str]:
    """三要素过期判定: 规则版本变化 / 批次数据指纹漂移 / 结果超过 TTL。"""
    reasons: list[str] = []
    rv = (session.query(QualityRuleVersion)
          .filter(QualityRuleVersion.ruleset_id == scan.ruleset_id,
                  QualityRuleVersion.version == scan.rule_version).first())
    rs = session.get(QualityRuleSet, scan.ruleset_id)
    if rs is not None and rs.current_version != scan.rule_version:
        reasons.append(f"规则版本已更新: 扫描基于 v{scan.rule_version}, 当前为 "
                       f"v{rs.current_version}, 旧结果不能放行")
    if rv is not None and rv.content_digest != scan.rule_digest:
        reasons.append(f"规则版本 v{scan.rule_version} 内容已变化, 旧结果不能放行")
    # 批次数据指纹: 范围内旧表当前内容与扫描完成时一致才算未漂移
    for sb in _scan_batches(session, scan.id):
        if sb.status != "SUCCESS" or not sb.data_fingerprint:
            continue
        batch = session.get(MigrationBatch, sb.batch_id)
        if batch is None:
            reasons.append(f"批次 {sb.batch_id} 已不存在")
            continue
        current_fp, _ = fingerprint_batch(session, batch)
        if current_fp != sb.data_fingerprint:
            reasons.append(f"批次 {sb.batch_id}({sb.biz or ''}) 数据在扫描后发生变化, "
                           f"旧结果不能放行")
    if scan.expires_at is not None and now_utc_naive() > scan.expires_at:
        reasons.append(f"扫描结果已过期(有效期至 {scan.expires_at.isoformat()}Z), 请重新扫描")
    return reasons


def evaluate_gate(session: Session, plan_id: str) -> dict:
    """门禁评估视图(实时):
    - 未绑定规则集: status=NOT_CONFIGURED, allowed=True(不约束历史行为);
    - 无 COMPLETED 扫描/扫描活动中/失败取消: NOT_SCANNED/RUNNING/FAILED, 阻止;
    - COMPLETED 但规则版本变化/数据漂移/过期: STALE, 阻止;
    - 最新有效扫描仍有 OPEN 的 BLOCKER: BLOCKED, 阻止;
    - 全部阻断问题 FIXED/EXEMPTED: PASS, 允许计划进入启动流程。
    返回额外携带自动重扫编排信息: quality_hold(暂停摘要)、active_rescan_id。"""
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise QualityNotFound(f"迁移计划 {plan_id} 不存在")
    rs = session.query(QualityRuleSet).filter(QualityRuleSet.plan_id == plan_id).first()
    if rs is None:
        return {"plan_id": plan_id, "status": "NOT_CONFIGURED", "passed": True,
                "reasons": [], "detail": "计划未绑定数据质量规则, 质量门禁不约束"}
    rv = (session.query(QualityRuleVersion)
          .filter(QualityRuleVersion.ruleset_id == rs.id,
                  QualityRuleVersion.version == rs.current_version).first())
    latest = (session.query(QualityScan)
              .filter(QualityScan.plan_id == plan_id)
              .order_by(QualityScan.created_at.desc(), QualityScan.id.desc())
              .first())
    active_scan = _active_scan_for(session, plan_id)
    hold = active_hold(session, plan_id)
    base = {
        "plan_id": plan_id, "ruleset_id": rs.id,
        "current_rule_version": rs.current_version,
        "current_rule_digest": rv.content_digest if rv else None,
        "latest_scan_id": latest.id if latest else None,
        "latest_scan_status": latest.status if latest else None,
        "latest_scan_source": latest.scan_source if latest else None,
        "active_rescan_id": (active_scan.id if active_scan
                             and active_scan.scan_source == "auto_rescan" else None),
        "active_scan_id": active_scan.id if active_scan else None,
        "quality_hold": hold_to_dict(hold) if hold is not None else None,
    }
    if latest is None:
        return {**base, "status": "NOT_SCANNED", "passed": False,
                "reasons": ["计划已绑定质量规则但从未扫描, 启动前必须完成一次质量扫描"],
                "detail": "尚未发起质量扫描"}
    if latest.status in ("QUEUED", "RUNNING", "PAUSED"):
        return {**base, "status": "RUNNING", "passed": False,
                "reasons": [f"最新质量扫描 {latest.id} 正在进行({latest.status}), 完成后才能评估门禁"],
                "detail": "扫描进行中"}
    if latest.status in ("FAILED", "CANCELED"):
        return {**base, "status": latest.status, "passed": False,
                "reasons": [f"最新质量扫描 {latest.id} 状态为 {latest.status}"
                            + (f": {latest.failure_reason}" if latest.failure_reason else "")],
                "detail": "最新扫描未成功完成, 请恢复或重新发起扫描"}
    # COMPLETED
    stale = _scan_stale_reasons(session, latest)
    if stale:
        return {**base, "status": "STALE", "passed": False, "reasons": stale,
                "scan_rule_version": latest.rule_version,
                "expires_at": latest.expires_at.isoformat() if latest.expires_at else None,
                "detail": "扫描结果已失效(规则版本变化/批次数据变化/过期), 旧结果不能放行"}
    issues = (session.query(QualityIssue)
              .filter(QualityIssue.scan_id == latest.id).all())
    open_blockers = [i for i in issues if i.severity == "BLOCKER" and i.status == "OPEN"]
    fixed = sum(1 for i in issues if i.status == "FIXED")
    exempted = sum(1 for i in issues if i.status == "EXEMPTED")
    counts = {"total": len(issues),
              "blocker": sum(1 for i in issues if i.severity == "BLOCKER"),
              "warning": sum(1 for i in issues if i.severity == "WARNING"),
              "info": sum(1 for i in issues if i.severity == "INFO"),
              "open_blocker": len(open_blockers), "fixed": fixed,
              "exempted": exempted}
    if open_blockers:
        sample = [{"issue_id": i.id, "batch_id": i.batch_id, "record_id": i.record_id,
                   "rule_id": i.rule_id, "field": i.field, "message": i.message}
                  for i in open_blockers[:20]]
        return {**base, "status": "BLOCKED", "passed": False,
                "reasons": [f"存在 {len(open_blockers)} 个未处理的阻断级(BLOCKER)问题, "
                            f"必须全部修复或豁免后才能启动"],
                "scan_rule_version": latest.rule_version,
                "expires_at": latest.expires_at.isoformat() if latest.expires_at else None,
                "issue_counts": counts, "open_blockers": sample,
                "detail": "门禁阻断: 仍有阻断级问题待处理"}
    return {**base, "status": "PASS", "passed": True, "reasons": [],
            "scan_rule_version": latest.rule_version,
            "scan_id": latest.id,
            "expires_at": latest.expires_at.isoformat() if latest.expires_at else None,
            "issue_counts": counts,
            "detail": ("全部阻断级问题已处理或豁免, 质量门禁通过"
                       + (f"(问题 {len(issues)}: 修复 {fixed}, 豁免 {exempted}, 其余为非阻断)"
                          if issues else "(零问题)"))}


def assert_gate_allows_start(session: Session, plan_id: str) -> None:
    """计划启动闸门调用: 门禁不通过时抛 PlanStateError(由 plans.do_start 拦成 409)。"""
    gate = evaluate_gate(session, plan_id)
    if not gate["passed"]:
        from .plans import PlanStateError
        raise PlanStateError(
            f"计划 {plan_id} 未通过迁移前数据质量门禁({gate['status']}): "
            + "; ".join(gate["reasons"]))


# ---------- 质量暂停(hold)与恢复历史 ----------

def hold_to_dict(h: QualityGateHold) -> dict:
    return {
        "id": h.id, "plan_id": h.plan_id, "status": h.status,
        "trigger_source": h.trigger_source,
        "trigger_text": TRIGGER_TEXT.get(h.trigger_source, h.trigger_source),
        "reason": h.reason, "reason_code": h.reason_code,
        "paused_at_step_id": h.paused_at_step_id,
        "paused_at_seq": h.paused_at_seq,
        "rescan_scan_id": h.rescan_scan_id,
        "triggers": list(h.triggers or []),
        "resume_mode": h.resume_mode,
        "resume_scan_id": h.resume_scan_id,
        "resumed_by": h.resumed_by,
        "resumed_at": _dt(h.resumed_at),
        "canceled_by": h.canceled_by,
        "canceled_at": _dt(h.canceled_at),
        "created_by": h.created_by,
        "created_at": _dt(h.created_at),
        "updated_at": _dt(h.updated_at),
    }


def holds_view(session: Session, plan_id: str) -> dict:
    """计划的质量门禁暂停/恢复历史: 当前 ACTIVE hold(含关联重扫) + 全量历史。"""
    get_plan(session, plan_id)
    rows = (session.query(QualityGateHold)
            .filter(QualityGateHold.plan_id == plan_id)
            .order_by(QualityGateHold.created_at.desc(), QualityGateHold.id.desc())
            .all())
    active = next((hold_to_dict(h) for h in rows if h.status == "ACTIVE"), None)
    return {"plan_id": plan_id, "quality_hold": active is not None,
            "active": active, "history": [hold_to_dict(h) for h in rows]}


def plan_hold_summary(session: Session, plan_id: str) -> dict | None:
    """计划视图用的轻量暂停摘要(无 ACTIVE hold 返回 None)。"""
    h = active_hold(session, plan_id)
    if h is None:
        return None
    return {"id": h.id, "status": h.status, "trigger_source": h.trigger_source,
            "trigger_text": TRIGGER_TEXT.get(h.trigger_source, h.trigger_source),
            "reason": h.reason, "reason_code": h.reason_code,
            "paused_at_seq": h.paused_at_seq, "rescan_scan_id": h.rescan_scan_id,
            "resume_scan_id": h.resume_scan_id,
            "created_at": _dt(h.created_at), "triggers": list(h.triggers or [])}


# ---------- 查询: 按计划查问题 / 修复豁免历史 / 门禁结果 ----------

def issues_view(session: Session, plan_id: str, *, scan_id: str | None = None,
                batch_id: str | None = None, severity: str | None = None,
                status: str | None = None, rule_type: str | None = None,
                limit: int = 500) -> list[dict]:
    q = session.query(QualityIssue).filter(QualityIssue.plan_id == plan_id)
    if scan_id:
        q = q.filter(QualityIssue.scan_id == scan_id)
    if batch_id:
        q = q.filter(QualityIssue.batch_id == batch_id)
    if severity:
        q = q.filter(QualityIssue.severity == severity)
    if status:
        q = q.filter(QualityIssue.status == status)
    if rule_type:
        q = q.filter(QualityIssue.rule_type == rule_type)
    rows = q.order_by(QualityIssue.severity.desc(), QualityIssue.id).limit(limit).all()
    return [issue_to_dict(i, with_sample=True) for i in rows]


def history_view(session: Session, plan_id: str) -> dict:
    """计划维度的修复批次 + 豁免历史(全部只追加)。"""
    fixes = (session.query(QualityFixBatch)
             .filter(QualityFixBatch.plan_id == plan_id)
             .order_by(QualityFixBatch.id.desc()).all())
    exemptions = (session.query(QualityExemption)
                  .filter(QualityExemption.plan_id == plan_id)
                  .order_by(QualityExemption.id.desc()).all())
    return {
        "plan_id": plan_id,
        "fix_batches": [fix_to_dict(f) for f in fixes],
        "exemptions": [exemption_to_dict(e) for e in exemptions],
    }


# ---------- 视图序列化 ----------

def _dt(v) -> str | None:
    return v.isoformat() if v else None


def rule_version_to_dict(rv: QualityRuleVersion) -> dict:
    return {
        "id": rv.id, "version": rv.version, "rules": list(rv.rules or []),
        "rule_count": rv.rule_count, "content_digest": rv.content_digest,
        "note": rv.note, "created_by": rv.created_by,
        "created_at": _dt(rv.created_at),
    }


def ruleset_to_dict(session: Session, rs: QualityRuleSet, *,
                    with_versions: bool = True) -> dict:
    versions = (session.query(QualityRuleVersion)
                .filter(QualityRuleVersion.ruleset_id == rs.id)
                .order_by(QualityRuleVersion.version).all())
    current = next((v for v in versions if v.version == rs.current_version), None)
    out = {
        "id": rs.id, "plan_id": rs.plan_id,
        "current_version": rs.current_version,
        "current_rules": list(current.rules if current else []),
        "rule_count": current.rule_count if current else 0,
        "content_digest": current.content_digest if current else None,
        "created_by": rs.created_by, "updated_by": rs.updated_by,
        "created_at": _dt(rs.created_at), "updated_at": _dt(rs.updated_at),
    }
    if with_versions:
        out["versions"] = [rule_version_to_dict(v) for v in versions]
    return out


def scan_batch_to_dict(sb: QualityScanBatch) -> dict:
    return {
        "id": sb.id, "seq": sb.seq, "batch_id": sb.batch_id, "biz": sb.biz,
        "status": sb.status, "record_count": sb.record_count,
        "issue_count": sb.issue_count, "blocker_count": sb.blocker_count,
        "data_fingerprint": sb.data_fingerprint, "last_error": sb.last_error,
        "started_at": _dt(sb.started_at), "finished_at": _dt(sb.finished_at),
    }


def issue_to_dict(i: QualityIssue, *, with_sample: bool = True) -> dict:
    out = {
        "id": i.id, "plan_id": i.plan_id, "scan_id": i.scan_id,
        "batch_id": i.batch_id, "record_id": i.record_id,
        "rule_version": i.rule_version, "rule_id": i.rule_id,
        "rule_name": i.rule_name, "rule_type": i.rule_type,
        "severity": i.severity, "field": i.field, "message": i.message,
        "status": i.status, "resolution_type": i.resolution_type,
        "resolution_id": i.resolution_id, "resolved_by": i.resolved_by,
        "resolved_at": _dt(i.resolved_at), "created_at": _dt(i.created_at),
    }
    if with_sample:
        out["sample"] = i.sample
        out["sample_truncated"] = i.sample is None
    return out


def fix_to_dict(f: QualityFixBatch) -> dict:
    return {
        "id": f.id, "plan_id": f.plan_id, "scan_id": f.scan_id,
        "rule_version": f.rule_version, "operator": f.operator, "note": f.note,
        "progress": {"total": f.total, "resolved": f.resolved,
                     "still_open": f.still_open, "not_found": f.not_found,
                     "rejected": f.rejected},
        "total": f.total, "resolved": f.resolved, "still_open": f.still_open,
        "not_found": f.not_found, "rejected": f.rejected,
        "results": list(f.results or []), "created_at": _dt(f.created_at),
    }


def exemption_to_dict(e: QualityExemption) -> dict:
    return {
        "id": e.id, "plan_id": e.plan_id, "issue_id": e.issue_id,
        "scan_id": e.scan_id, "batch_id": e.batch_id, "rule_id": e.rule_id,
        "record_id": e.record_id, "field": e.field,
        "rule_version": e.rule_version,
        "reason": e.reason, "status": e.status, "created_by": e.created_by,
        "created_at": _dt(e.created_at), "revoked_by": e.revoked_by,
        "revoked_at": _dt(e.revoked_at), "revoke_reason": e.revoke_reason,
    }


def scan_to_dict(session: Session, scan: QualityScan, *,
                 with_issues: bool = False,
                 with_events: bool = True) -> dict:
    sbs = _scan_batches(session, scan.id)
    # 问题分布(按严重级别 × 状态)
    issues = session.query(QualityIssue).filter(QualityIssue.scan_id == scan.id).all()
    distribution = {
        "BLOCKER": {"total": 0, "OPEN": 0, "FIXED": 0, "EXEMPTED": 0},
        "WARNING": {"total": 0, "OPEN": 0, "FIXED": 0, "EXEMPTED": 0},
        "INFO": {"total": 0, "OPEN": 0, "FIXED": 0, "EXEMPTED": 0},
    }
    for i in issues:
        d = distribution.setdefault(
            i.severity, {"total": 0, "OPEN": 0, "FIXED": 0, "EXEMPTED": 0})
        d["total"] += 1
        d[i.status] = d.get(i.status, 0) + 1
    out = {
        "id": scan.id, "plan_id": scan.plan_id, "ruleset_id": scan.ruleset_id,
        "rule_version": scan.rule_version, "rule_digest": scan.rule_digest,
        "status": scan.status,
        "scan_source": scan.scan_source or "manual",
        "is_auto_rescan": (scan.scan_source == "auto_rescan"),
        "trigger_source": scan.trigger_source,
        "triggers": list(scan.triggers or []),
        "supersedes_scan_id": scan.supersedes_scan_id,
        "affected_batch_ids": list(scan.affected_batch_ids or []),
        "progress": {"done": scan.completed_batches, "total": scan.total_batches},
        "total_batches": scan.total_batches,
        "completed_batches": scan.completed_batches,
        "total_records": scan.total_records,
        "total_issues": scan.total_issues,
        "issue_counts": {"blocker": scan.blocker_issues,
                         "warning": scan.warning_issues,
                         "info": scan.info_issues,
                         "open_blocker": scan.open_blocker_issues},
        "issue_distribution": distribution,
        "current_batch_id": scan.current_batch_id,
        "ttl_seconds": scan.ttl_seconds,
        "expires_at": _dt(scan.expires_at),
        "stale": (_scan_stale_reasons(session, scan)
                  if scan.status == "COMPLETED" else []),
        "last_error": scan.last_error,
        "failure_code": scan.failure_code,
        "failure_reason": scan.failure_reason,
        "concurrency_limit": max_concurrency(),
        "created_by": scan.created_by, "updated_by": scan.updated_by,
        "started_at": _dt(scan.started_at), "finished_at": _dt(scan.finished_at),
        "created_at": _dt(scan.created_at), "updated_at": _dt(scan.updated_at),
        "batches": [scan_batch_to_dict(x) for x in sbs],
    }
    if with_issues:
        out["issues"] = [issue_to_dict(i) for i in issues]
    if with_events:
        out["events"] = [
            {"id": e.id, "ts": _dt(e.ts), "event": e.event, "operator": e.operator,
             "reason": e.reason, "detail": e.detail}
            for e in (session.query(QualityEvent)
                      .filter(QualityEvent.scan_id == scan.id)
                      .order_by(QualityEvent.id.desc()).limit(100).all())]
    return out


def plan_quality_view(session: Session, plan_id: str) -> dict:
    """计划质量总览: 规则版本 + 扫描列表(进度/分布) + 门禁状态。"""
    plan = get_plan(session, plan_id)
    rs = session.query(QualityRuleSet).filter(QualityRuleSet.plan_id == plan_id).first()
    scans = (session.query(QualityScan)
             .filter(QualityScan.plan_id == plan_id)
             .order_by(QualityScan.created_at.desc(), QualityScan.id.desc()).all())
    return {
        "plan_id": plan_id, "plan_name": plan.name, "plan_status": plan.status,
        "ruleset": ruleset_to_dict(session, rs) if rs is not None else None,
        "gate": evaluate_gate(session, plan_id),
        "scans": [scan_to_dict(session, s, with_issues=False, with_events=False)
                  for s in scans],
    }


def quality_status_overview(session: Session) -> dict:
    """status 接口用的轻量汇总(所有计划门禁状态 + 活动扫描)。"""
    plans_rows = session.query(MigrationPlan).order_by(MigrationPlan.id).all()
    gates = {}
    for p in plans_rows:
        gates[p.id] = evaluate_gate(session, p.id)
    active = (session.query(QualityScan)
              .filter(QualityScan.status.in_(QUALITY_SCAN_ACTIVE_STATUSES))
              .order_by(QualityScan.created_at, QualityScan.id).all())
    active_rescan_ids = [s.id for s in active if s.scan_source == "auto_rescan"]
    held_ids = [p.id for p in plans_rows if p.quality_hold]
    return {
        "concurrency": max_concurrency(),
        "active_scan_count": len(active),
        "active_rescan_count": len(active_rescan_ids),
        "active_rescan_ids": active_rescan_ids,
        "held_plan_count": len(held_ids),
        "held_plan_ids": held_ids,
        "active_scans": [{"id": s.id, "plan_id": s.plan_id, "status": s.status,
                          "rule_version": s.rule_version,
                          "scan_source": s.scan_source or "manual",
                          "trigger_source": s.trigger_source,
                          "progress": {"done": s.completed_batches,
                                       "total": s.total_batches}}
                         for s in active],
        "gates": gates,
    }
