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
    QualityExemption, QualityFixBatch, QualityIssue, QualityRuleSet,
    QualityRuleVersion, QualityScan, QualityScanBatch, RecordOld,
    QUALITY_RULE_TYPES, QUALITY_SCAN_ACTIVE_STATUSES,
    QUALITY_SCAN_STATUSES, QUALITY_SCAN_TERMINAL_STATUSES, QUALITY_SEVERITIES,
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
    """串行化同计划扫描创建的去重判定。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260921)"))


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


def current_rules(session: Session, plan_id: str) -> QualityRuleVersion | None:
    rs = session.query(QualityRuleSet).filter(QualityRuleSet.plan_id == plan_id).first()
    if rs is None:
        return None
    return (session.query(QualityRuleVersion)
            .filter(QualityRuleVersion.ruleset_id == rs.id,
                    QualityRuleVersion.version == rs.current_version)
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
    add_event(session, plan_id=plan_id, event="rules.version_create",
              operator=operator,
              reason=(f"创建规则版本 v{version}: {len(rules)} 条规则"
                      + (f"({note})" if note else "")),
              detail={"version": version, "rule_count": len(rules),
                      "content_digest": digest})
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


def do_create_scan(session: Session, _ignored, operator: str, plan_id: str) -> dict:
    """基于计划当前规则版本创建质量扫描任务并排队。

    计划不存在 -> 404; 未绑定规则集/计划已启动 -> 409;
    已有活动(QUEUED/RUNNING/PAUSED)扫描时重复创建幂等返回已有任务。
    """
    plan = get_plan(session, plan_id)
    rv = current_rules(session, plan_id)
    if rv is None:
        raise QualityStateError(
            f"计划 {plan_id} 尚未绑定数据质量规则集, 请先保存规则后再发起扫描")
    if plan.status != "DRAFT":
        raise QualityStateError(
            f"计划 {plan_id} 当前状态 {plan.status}, 质量扫描只能在启动前(DRAFT)发起")
    steps = plans.steps_of(session, plan_id)
    if not steps:
        raise QualityStateError(f"计划 {plan_id} 没有任何步骤(批次), 无可扫描内容")

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
        total_batches=len(steps), ttl_seconds=ttl,
        created_by=operator, updated_by=operator)
    session.add(scan)
    session.flush()
    for i, st in enumerate(steps, start=1):
        batch = session.get(MigrationBatch, st.batch_id)
        session.add(QualityScanBatch(
            scan_id=scan_id, batch_id=st.batch_id, seq=st.seq,
            biz=batch.biz if batch else None, status="PENDING"))
    add_event(session, plan_id=plan_id, scan_id=scan_id, event="scan.create",
              operator=operator,
              detail={"rule_version": rv.version, "total_batches": len(steps),
                      "ttl_seconds": ttl})
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


def do_cancel_scan(session: Session, scan: QualityScan, operator: str) -> dict:
    if scan.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "质量扫描已取消, 重复取消无副作用"}
    _require_status(scan, "cancel")
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
    add_event(session, plan_id=scan.plan_id, scan_id=scan.id, event="scan.cancel",
              operator=operator,
              reason=f"取消质量扫描: {skipped} 个未开始批次跳过",
              detail={"skipped": skipped})
    return {"ok": True, "detail": f"质量扫描已取消, {skipped} 个未开始批次跳过, 已有问题保留"}


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
              detail={"code": code, "skipped_batches": skipped})
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
                     + f"), 门禁初判 {gate}, 结果有效期至 {scan.expires_at.isoformat()}Z",
              detail={"total_issues": scan.total_issues,
                      "blockers": scan.blocker_issues,
                      "open_blockers": scan.open_blocker_issues,
                      "inherited_exemptions": inherited,
                      "gate": gate, "expires_at": scan.expires_at.isoformat()})


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
    2. QUEUED/PAUSED/终态扫描保持不变(排队/暂停是持久化的用户态)。"""
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
        """认领到期扫描并给每个 RUNNING 扫描(含刚认领的)推进一个批次, 返回执行批次数。"""
        from .db import SessionLocal
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
    - 全部阻断问题 FIXED/EXEMPTED: PASS, 允许计划进入启动流程。"""
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
    base = {
        "plan_id": plan_id, "ruleset_id": rs.id,
        "current_rule_version": rs.current_version,
        "current_rule_digest": rv.content_digest if rv else None,
        "latest_scan_id": latest.id if latest else None,
        "latest_scan_status": latest.status if latest else None,
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
    return {
        "concurrency": max_concurrency(),
        "active_scan_count": len(active),
        "active_scans": [{"id": s.id, "plan_id": s.plan_id, "status": s.status,
                          "rule_version": s.rule_version,
                          "progress": {"done": s.completed_batches,
                                       "total": s.total_batches}}
                         for s in active],
        "gates": gates,
    }
