"""迁移计划编排: 把多个已有批次组织成带顺序与依赖的步骤图。

不变量:
1. 建计划时一次性聚合校验并拒绝保存: 步骤非空、seq 唯一且 >=1、批次存在且未切换(DONE 终态)、
   批次在计划内不重复、不被其他未终结计划占用、依赖的步骤 seq 必须存在、依赖图不允许成环。
2. 只有依赖步骤全部 SUCCESS 的步骤才可执行; worker 每个 tick 每个计划至多推进一步,
   失败按步骤 max_retries 自动重试, 超过次数步骤 HALTED 且计划 HALTED, 后续步骤永远拿不到放行。
3. 步骤执行复用现有批次流程(freeze -> validate -> cutover), 全部走批次幂等框架:
   计划级操作幂等键与批次动作幂等键分开命名空间, 崩溃/重启后重放无副作用。
4. 计划支持 start / pause / resume / cancel: 暂停只在步骤边界生效; 取消后未开始的步骤置 SKIPPED。
5. 重启安全: 任何"执行中"的步骤都不可能跨进程存活, boot 时把遗留 RUNNING 步骤重置为 PENDING
   并落审计 —— 计划永远不会停留在"假运行", 也不会丢失已 SUCCESS 的进度。
"""
import hashlib
import json
import threading
import time
import uuid
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import service
from .models import (
    RISK_LEVELS, AuditLog, IdempotencyKey, MigrationBatch,
    MigrationPlan, PlanStep, PlanStepDependency, PlanStepEvent, PlanWindow,
)
from .service import APP_VERSION

TERMINAL_PLAN_STATUSES = ("COMPLETED", "CANCELED")
# 计划状态 -> 允许的管理动作
ALLOWED_ACTIONS = {
    "start": {"DRAFT"},
    "pause": {"RUNNING"},
    "resume": {"PAUSED", "HALTED"},
    "cancel": {"DRAFT", "RUNNING", "PAUSED", "HALTED"},
}
# 审批 / 窗口管理只允许在启动前(DRAFT)操作: 启动后审批状态与窗口边界都锁定
PRE_LAUNCH_ONLY = {"approve", "reject", "revoke-approval", "window"}


class PlanNotFound(Exception):
    """计划不存在 -> 404。"""


class PlanStateError(Exception):
    """当前计划状态不允许该操作 / 内部推进冲突 -> 409。"""


class PlanValidationError(Exception):
    """建计划图校验失败(批次缺失/重复占用/依赖不存在/成环等) -> 409, reasons 逐条说明。"""

    def __init__(self, reasons: list[str]):
        super().__init__("; ".join(reasons))
        self.reasons = reasons


# ---------- 建计划图校验 ----------

def validate_plan_graph(session: Session, steps: list[dict]) -> list[PlanStep]:
    """聚合校验步骤定义, 全部通过才返回建好(尚未持久化)的 PlanStep 列表;
    任何一条不通过都抛 PlanValidationError, 一次性列出全部原因。"""
    reasons: list[str] = []
    if not steps:
        raise PlanValidationError(["计划至少需要一个步骤"])

    seqs: list[int] = []
    batch_refs: list[str] = []
    deps_by_seq: dict[int, list[int]] = {}
    for i, st in enumerate(steps, 1):
        seq = st.get("seq")
        bid = (st.get("batch_id") or "").strip()
        if not isinstance(seq, int) or seq < 1:
            reasons.append(f"第 {i} 个步骤的顺序号 seq 必须是 >=1 的整数(收到 {seq!r})")
            seq = -1  # 防止后续去重逻辑 KeyError
        else:
            seqs.append(seq)
        if not bid:
            reasons.append(f"步骤 seq={seq} 缺少 batch_id")
        else:
            batch_refs.append(bid)
        deps = [d for d in (st.get("depends_on") or []) if isinstance(d, int)]
        deps_by_seq[seq] = deps

    dup_seqs = sorted({s for s in seqs if seqs.count(s) > 1})
    if dup_seqs:
        reasons.append(f"步骤顺序号重复: {dup_seqs}(每个步骤必须有唯一顺序)")

    dup_batches = sorted({b for b in batch_refs if batch_refs.count(b) > 1})
    if dup_batches:
        reasons.append(f"同一批次在计划内被多个步骤重复占用: {dup_batches}")

    # 批次实体、批次阶段、跨计划占用
    found: dict[str, MigrationBatch] = {}
    for bid in set(batch_refs):
        b = session.get(MigrationBatch, bid)
        if b is None:
            reasons.append(f"批次 {bid} 不存在")
            continue
        found[bid] = b
        if b.phase == "DONE":
            reasons.append(f"批次 {bid}({b.biz}) 已切换完成(DONE 为终态), 不能再纳入计划")
        other = (session.query(PlanStep)
                 .join(MigrationPlan, MigrationPlan.id == PlanStep.plan_id)
                 .filter(PlanStep.batch_id == bid,
                         MigrationPlan.status.notin_(TERMINAL_PLAN_STATUSES))
                 .first())
        if other is not None:
            reasons.append(
                f"批次 {bid}({b.biz}) 已被未终结计划 {other.plan_id} 的步骤 seq={other.seq} 占用")

    seq_set = set(seqs)
    for seq, deps in deps_by_seq.items():
        for d in deps:
            if d not in seq_set:
                reasons.append(f"步骤 seq={seq} 依赖的步骤 seq={d} 不存在")
            if d == seq:
                reasons.append(f"步骤 seq={seq} 不能依赖自身")

    # 成环检测: Kahn 拓扑排序(只看引用了已存在 seq 的边)
    adj: dict[int, set[int]] = {s: set() for s in seqs}
    indeg: dict[int, int] = {s: 0 for s in seqs}
    for seq, deps in deps_by_seq.items():
        if seq not in seq_set:
            continue
        for d in deps:
            if d in seq_set and d != seq:
                # 边 d -> seq
                if seq not in adj[d]:
                    adj[d].add(seq)
                    indeg[seq] += 1
    queue = sorted(s for s, n in indeg.items() if n == 0)
    visited = 0
    while queue:
        node = queue.pop(0)
        visited += 1
        for nxt in sorted(adj[node]):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)
    if visited != len(seqs):
        cyclic = sorted(s for s, n in indeg.items() if n > 0)
        reasons.append(f"步骤依赖存在循环(环上步骤 seq: {cyclic}), 无法确定推进顺序")

    if reasons:
        raise PlanValidationError(reasons)

    ordered = sorted(steps, key=lambda s: s["seq"])
    return [
        PlanStep(seq=s["seq"], batch_id=s["batch_id"],
                 max_retries=s.get("max_retries", 0),
                 status="BLOCKED", attempts=0,
                 deps=[PlanStepDependency(depends_on_seq=d) for d in (s.get("depends_on") or [])])
        for s in ordered
    ]


# ---------- 计划行存取 ----------

def get_plan(session: Session, plan_id: str) -> MigrationPlan:
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise PlanNotFound(f"迁移计划 {plan_id} 不存在")
    return plan


def lock_plan(session: Session, plan_id: str) -> MigrationPlan:
    """Postgres 行锁串行化同一计划的操作; SQLite 写事务天然串行。"""
    if session.bind.dialect.name != "sqlite":
        plan = session.get(MigrationPlan, plan_id, with_for_update=True)
        if plan is not None:
            return plan
    return get_plan(session, plan_id)


def _lock_plan_creation(session: Session) -> None:
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260913)"))


def steps_of(session: Session, plan_id: str) -> list[PlanStep]:
    return (session.query(PlanStep)
            .filter(PlanStep.plan_id == plan_id)
            .order_by(PlanStep.seq)
            .all())


def deps_of(session: Session, plan_id: str) -> dict[int, list[int]]:
    out: dict[int, list[int]] = {}
    for d in (session.query(PlanStepDependency)
              .filter(PlanStepDependency.plan_id == plan_id).all()):
        out.setdefault(d.step_id, []).append(d.depends_on_seq)
    return out


def plan_audit(session: Session, plan: MigrationPlan, *, operator: str, action: str,
               reason: str | None = None, step_id: int | None = None) -> None:
    """计划级审计(只追加): to_phase 记计划状态。"""
    session.add(AuditLog(
        plan_id=plan.id, step_id=step_id, operator=operator, action=action,
        from_phase=None, to_phase=plan.status, epoch=None,
        app_version=APP_VERSION, freeze_version=None, watermark=None,
        reason=reason, diffs=None,
    ))
    # 同步投影到统一不可变审计事件流(计划推进/取消)
    from . import auditreplay
    auditreplay.emit_plan_event(
        session, plan, operator=operator, action=action, reason=reason,
        step_id=step_id)


def add_event(session: Session, *, plan_id: str, step_id: int, attempt: int,
              event: str, operator: str, reason: str | None = None,
              detail: dict | None = None) -> None:
    session.add(PlanStepEvent(
        plan_id=plan_id, step_id=step_id, attempt=attempt, event=event,
        operator=operator, reason=reason, detail=detail,
    ))


# ---------- 幂等框架(与批次动作分命名空间) ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def run_plan_action(session: Session, *, action: str, operator: str,
                    idempotency_key: str, payload: dict, fn,
                    plan_id: str | None = None) -> tuple[dict, bool]:
    """与 service.run_admin_action 同构, 但动作名加 plan. 前缀, 请求哈希带 plan_id。

    fn(session, plan_or_none) -> dict; 结果与幂等键同事务落库, 重复提交返回首次结果。
    """
    req_hash = _hash({"action": f"plan.{action}", "plan_id": plan_id, "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise PlanStateError("幂等键被不同请求复用")
        return existing.response_json, True

    plan = lock_plan(session, plan_id) if plan_id is not None else None
    if plan is None:
        _lock_plan_creation(session)
    result = fn(session, plan)
    if plan_id is not None:
        fresh = get_plan(session, plan_id)
        result["plan_id"] = plan_id
        result["status"] = fresh.status
    session.add(IdempotencyKey(key=idempotency_key, action=f"plan.{action}",
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


# ---------- 创建 / 启动 / 暂停 / 恢复 / 取消 ----------

def _parse_dt(value, field: str) -> datetime:
    """ISO 8601 -> naive UTC datetime(库内统一无时区存储)。"""
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            raise PlanValidationError([f"{field} 不是合法的 ISO 8601 时间: {value!r}"])
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def validate_windows(raw: list[dict] | None) -> list[tuple[datetime, datetime]]:
    """聚合校验执行窗口: 每项需含 starts_at/ends_at 且开始 < 结束。返回规范化后的区间列表。"""
    if not raw:
        return []
    windows: list[tuple[datetime, datetime]] = []
    for i, w in enumerate(raw, 1):
        if not isinstance(w, dict) or not w.get("starts_at") or not w.get("ends_at"):
            raise PlanValidationError([f"第 {i} 个执行窗口必须包含 starts_at 与 ends_at"])
        start = _parse_dt(w["starts_at"], f"第 {i} 个执行窗口 starts_at")
        end = _parse_dt(w["ends_at"], f"第 {i} 个执行窗口 ends_at")
        if start >= end:
            raise PlanValidationError([f"第 {i} 个执行窗口 starts_at 必须早于 ends_at"])
        windows.append((start, end))
    return windows


def now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def in_any_window(windows: list[PlanWindow], at: datetime | None = None) -> bool:
    """当前时刻是否落在任一执行窗口(闭区间)内。无窗口的计划恒为 True。"""
    if not windows:
        return True
    at = at or now_utc_naive()
    return any(w.starts_at <= at <= w.ends_at for w in windows)


def in_any_range(ranges: list[tuple[datetime, datetime]],
                 at: datetime | None = None) -> bool:
    at = at or now_utc_naive()
    return any(s <= at <= e for s, e in ranges)


def windows_of(session: Session, plan_id: str) -> list[PlanWindow]:
    return (session.query(PlanWindow)
            .filter(PlanWindow.plan_id == plan_id)
            .order_by(PlanWindow.starts_at, PlanWindow.id).all())


def _window_desc(windows: list[PlanWindow]) -> str:
    if not windows:
        return "无窗口限制"
    return "; ".join(f"[{w.starts_at.isoformat()}Z, {w.ends_at.isoformat()}Z]" for w in windows)


def do_create_plan(session: Session, _plan, operator: str, name: str,
                   steps: list[dict], max_retries: int,
                   risk_level: str = "LOW",
                   windows: list[dict] | None = None) -> dict:
    if not name or not name.strip():
        raise PlanValidationError(["计划名称不能为空"])
    if max_retries < 0 or max_retries > 10:
        raise PlanValidationError(["max_retries 必须在 0..10 之间"])
    if risk_level not in RISK_LEVELS:
        raise PlanValidationError([f"risk_level 必须是 {RISK_LEVELS} 之一(收到 {risk_level!r})"])
    window_ranges = validate_windows(windows)  # 窗口非法时整体拒绝, 不落任何数据
    # 步骤未显式给 max_retries 时继承计划级默认
    for s in steps:
        s.setdefault("max_retries", max_retries)
    built = validate_plan_graph(session, steps)  # 失败即聚合拒绝, 不写任何数据
    plan_id = "P" + uuid.uuid4().hex[:10]
    # 高风险计划创建即进入待审批, 必须由另一名管理员审批通过后才能启动
    approval = "PENDING" if risk_level == "HIGH" else "NOT_REQUIRED"
    plan = MigrationPlan(
        id=plan_id, name=name.strip(), status="DRAFT", max_retries=max_retries,
        risk_level=risk_level, approval_status=approval,
        created_by=operator, updated_by=operator)
    session.add(plan)
    session.flush()
    for st in built:
        st.plan_id = plan_id
        for d in st.deps:
            d.plan_id = plan_id
        session.add(st)
    for s, e in window_ranges:
        session.add(PlanWindow(plan_id=plan_id, starts_at=s, ends_at=e,
                               created_by=operator))
    session.flush()
    plan.window_open = None if not window_ranges else in_any_window(
        windows_of(session, plan_id))
    session.flush()
    risk_desc = "高风险(启动前须由另一名管理员审批)" if risk_level == "HIGH" else "低风险(可直接启动)"
    # 把每个步骤批次锚定进该计划事件流(批次版本链起点)
    from . import auditreplay
    auditreplay.emit_plan_attach(session, plan, built, operator)
    plan_audit(session, plan, operator=operator, action="plan.create",
               reason=f"创建计划 {name.strip()}, {len(built)} 个步骤, "
                      f"每步默认额外重试 {max_retries} 次, 风险等级 {risk_level}, "
                      f"审批 {approval}, 执行窗口: {_window_desc(windows_of(session, plan_id))}")
    detail = f"计划 {plan_id} 已保存(DRAFT), 共 {len(built)} 个步骤, 校验通过, {risk_desc}"
    if window_ranges:
        detail += f", {len(window_ranges)} 个执行窗口"
    return {"ok": True, "plan_id": plan_id, "risk_level": risk_level,
            "approval_status": approval, "detail": detail}


def _require_pre_launch(plan: MigrationPlan, action: str) -> None:
    """审批/窗口类操作只允许启动前(DRAFT): 启动后状态锁定。"""
    if plan.status != "DRAFT":
        raise PlanStateError(
            f"计划 {plan.id} 当前状态 {plan.status}, {action} 只能在启动前(DRAFT)进行")


def _require_status(plan: MigrationPlan, action: str) -> None:
    allowed = ALLOWED_ACTIONS[action]
    if plan.status not in allowed:
        raise PlanStateError(
            f"计划 {plan.id} 当前状态 {plan.status} 不允许 {action}"
            f"(仅 {sorted(allowed)} 状态可执行该操作)")


def do_start(session: Session, plan: MigrationPlan, operator: str) -> dict:
    _require_status(plan, "start")
    # 迁移前数据质量门禁: 绑定了规则集的计划必须存在"当前规则版本 + 数据未漂移 +
    # 未过期"的完成扫描, 且全部阻断级问题已修复或豁免, 才允许进入启动流程;
    # 未绑定规则集的计划不受约束(保持历史行为)。延迟导入避免模块循环依赖。
    from . import quality
    quality.assert_gate_allows_start(session, plan.id)
    # 审批闸门: 高风险计划必须审批通过; 被拒绝/待审批都阻止启动
    if plan.risk_level == "HIGH":
        if plan.approval_status == "PENDING":
            raise PlanStateError(
                f"高风险计划 {plan.id} 尚未经另一名管理员审批, 不能启动")
        if plan.approval_status == "REJECTED":
            raise PlanStateError(
                f"高风险计划 {plan.id} 的审批已被拒绝"
                + (f": {plan.reject_reason}" if plan.reject_reason else "")
                + ", 不能启动(需重新审批通过)")
        if plan.approval_status != "APPROVED":
            raise PlanStateError(
                f"高风险计划 {plan.id} 审批状态 {plan.approval_status} 异常, 不能启动")
    plan.status = "RUNNING"
    plan.started_by = operator
    plan.updated_by = operator
    plan.last_error = None
    plan.failed_step_id = None
    plan.quality_hold = False  # 启动门禁已通过, 不允许携带执行期暂停标记
    session.flush()
    # 无依赖的首步由 BLOCKED -> PENDING(可立即被 worker 取走)
    ready = 0
    for st in steps_of(session, plan.id):
        if st.status == "BLOCKED" and not deps_of(session, plan.id).get(st.id):
            st.status = "PENDING"
            ready += 1
    # 执行窗口闸门: 无窗口不限制; 有窗口则按当前时刻初始化运行态,
    # 窗口外保持 RUNNING 但不推进(等重新进入窗口自动继续), 状态变化落审计
    wins = windows_of(session, plan.id)
    within = in_any_window(wins)
    plan.window_open = None if not wins else within
    if wins and not within:
        plan_audit(session, plan, operator=operator, action="plan.window_pause",
                   reason=f"启动时不在允许执行窗口内, 保持暂停等待重新进入窗口(窗口: {_window_desc(wins)})")
    elif wins:
        plan_audit(session, plan, operator=operator, action="plan.window_resume",
                   reason=f"启动时处于允许执行窗口内(窗口: {_window_desc(wins)})")
    approval_desc = f", 审批人 {plan.approved_by}" if plan.approved_by else ""
    plan_audit(session, plan, operator=operator, action="plan.start",
               reason=f"启动计划, {ready} 个无依赖步骤进入待执行"
                      f", 风险等级 {plan.risk_level}{approval_desc}")
    if wins and not within:
        return {"ok": True,
                "detail": f"计划已启动, {ready} 个无依赖步骤待执行; 当前不在执行窗口内, 将保持暂停至重新进入窗口"}
    return {"ok": True, "detail": f"计划已启动, {ready} 个无依赖步骤待执行"}


# ---------- 审批: 通过 / 拒绝(带原因) / 撤销(启动前) ----------
# 所有审批请求幂等: 重复的通过/拒绝/撤销不产生第二次状态变化与第二条审计,
# 返回当前审批状态与 already_in_state 标记。

def do_approve(session: Session, plan: MigrationPlan, operator: str) -> dict:
    _require_pre_launch(plan, "审批")
    if plan.risk_level != "HIGH":
        raise PlanStateError(f"计划 {plan.id} 是低风险计划, 无需审批, 可直接启动")
    if operator == plan.created_by:
        raise PlanStateError(
            f"高风险计划必须由不同于创建者({plan.created_by})的另一名管理员审批, "
            f"操作者 {operator} 不能审批自己创建的计划")
    if plan.approval_status == "APPROVED":
        # 幂等: 重复审批(无论是否同一人)直接回显, 无副作用
        return {"ok": True, "already_in_state": True,
                "approval_status": "APPROVED",
                "detail": f"计划已处于审批通过状态(审批人 {plan.approved_by}), 重复审批无副作用"}
    if plan.approval_status not in ("PENDING", "REJECTED"):
        raise PlanStateError(f"计划 {plan.id} 审批状态 {plan.approval_status} 不允许审批")
    from_status = plan.approval_status
    plan.approval_status = "APPROVED"
    plan.approved_by = operator
    plan.approved_at = now_utc_naive()
    plan.reject_reason = None
    plan.updated_by = operator
    session.flush()
    plan_audit(session, plan, operator=operator, action="plan.approve",
               reason=f"高风险计划审批通过(此前 {from_status}, 创建者 {plan.created_by}), 允许启动")
    return {"ok": True, "approval_status": "APPROVED",
            "detail": f"计划已审批通过(操作者 {operator}), 可以启动"}


def do_reject(session: Session, plan: MigrationPlan, operator: str,
              reason: str) -> dict:
    _require_pre_launch(plan, "拒绝审批")
    if plan.risk_level != "HIGH":
        raise PlanStateError(f"计划 {plan.id} 是低风险计划, 无需审批")
    if plan.approval_status == "REJECTED":
        # 幂等: 重复拒绝回显已有原因
        return {"ok": True, "already_in_state": True,
                "approval_status": "REJECTED",
                "detail": f"计划已处于审批拒绝状态(原因: {plan.reject_reason}), 重复请求无副作用"}
    if plan.approval_status != "PENDING":
        raise PlanStateError(
            f"计划 {plan.id} 当前审批状态 {plan.approval_status} 不允许拒绝"
            "(已通过的审批请使用撤销)")
    plan.approval_status = "REJECTED"
    plan.approved_by = None
    plan.approved_at = None
    plan.reject_reason = reason[:500]
    plan.updated_by = operator
    session.flush()
    plan_audit(session, plan, operator=operator, action="plan.reject",
               reason=("高风险计划审批被拒绝, 阻止启动, 原因: " + reason)[:500])
    return {"ok": True, "approval_status": "REJECTED",
            "detail": "审批已拒绝并记录原因, 计划不能启动"}


def do_revoke_approval(session: Session, plan: MigrationPlan,
                       operator: str) -> dict:
    """启动前撤销审批: APPROVED -> PENDING, 启动闸门重新关闭。幂等。"""
    _require_pre_launch(plan, "撤销审批")
    if plan.risk_level != "HIGH":
        raise PlanStateError(f"计划 {plan.id} 是低风险计划, 没有审批可撤销")
    if plan.approval_status in ("PENDING", "REJECTED"):
        # 幂等: 审批本就未通过, 撤销无副作用(REJECTED 保留拒绝原因)
        return {"ok": True, "already_in_state": True,
                "approval_status": plan.approval_status,
                "detail": f"计划审批状态已是 {plan.approval_status}, 撤销请求无副作用"}
    if plan.approval_status != "APPROVED":
        raise PlanStateError(f"计划 {plan.id} 审批状态 {plan.approval_status} 不允许撤销")
    prev_by = plan.approved_by
    plan.approval_status = "PENDING"
    plan.approved_by = None
    plan.approved_at = None
    plan.updated_by = operator
    session.flush()
    plan_audit(session, plan, operator=operator, action="plan.revoke_approval",
               reason=f"启动前撤销审批(原审批人 {prev_by}), 计划需重新审批通过才能启动")
    return {"ok": True, "approval_status": "PENDING",
            "detail": "审批已撤销, 计划在重新审批通过前不能启动"}


def do_update_window(session: Session, plan: MigrationPlan, operator: str,
                     windows: list[dict] | None) -> dict:
    """启动前整体替换执行窗口; 传空列表/空值即清空窗口限制。幂等:
    与当前窗口完全相同的请求回显无副作用, 不写第二条审计。"""
    _require_pre_launch(plan, "修改执行窗口")
    new_ranges = validate_windows(windows)
    old = windows_of(session, plan.id)
    same = (len(old) == len(new_ranges)
            and all((o.starts_at, o.ends_at) == (s, e)
                    for o, (s, e) in zip(old, new_ranges)))
    if same:
        return {"ok": True, "already_in_state": True,
                "window_open": plan.window_open,
                "detail": "执行窗口与当前完全一致, 修改请求无副作用"}
    for w in old:
        session.delete(w)
    session.flush()
    for s, e in new_ranges:
        session.add(PlanWindow(plan_id=plan.id, starts_at=s, ends_at=e,
                               created_by=operator))
    plan.window_open = None if not new_ranges else in_any_range(new_ranges)
    plan.updated_by = operator
    session.flush()
    desc = _window_desc(windows_of(session, plan.id))
    plan_audit(session, plan, operator=operator, action="plan.window_update",
               reason=f"启动前修改执行窗口: {desc}")
    if not new_ranges:
        return {"ok": True, "window_open": None,
                "detail": "已清空执行窗口, 计划不再受时间窗口限制"}
    return {"ok": True, "window_open": plan.window_open,
            "detail": f"执行窗口已更新为 {len(new_ranges)} 个区间: {desc}"}


def do_pause(session: Session, plan: MigrationPlan, operator: str) -> dict:
    _require_status(plan, "pause")
    plan.status = "PAUSED"
    plan.updated_by = operator
    session.flush()
    plan_audit(session, plan, operator=operator, action="plan.pause",
               reason="暂停计划: 当前步骤执行完后不再推进新步骤")
    return {"ok": True, "detail": "计划已暂停, 将在当前步骤边界停止推进"}


def do_resume(session: Session, plan: MigrationPlan, operator: str) -> dict:
    _require_status(plan, "resume")
    from_status = plan.status
    reset = []
    if from_status == "HALTED":
        # 恢复被失败卡住的步骤: 尝试计数清零、轮次 +1(批次动作幂等键随之换轮),
        # 重新待执行(若依赖仍满足)
        for st in steps_of(session, plan.id):
            if st.status == "HALTED":
                st.status = "PENDING"
                st.attempts = 0
                st.attempt_round += 1
                st.last_error = None
                st.executed_by = operator
                reset.append(st.seq)
                add_event(session, plan_id=plan.id, step_id=st.id, attempt=0,
                          event="reset", operator=operator,
                          reason=f"管理员恢复计划, 失败步骤进入第 {st.attempt_round} 轮尝试, 计数清零")
    plan.status = "RUNNING"
    plan.started_by = operator
    plan.updated_by = operator
    plan.last_error = None
    plan.failed_step_id = None
    # 防御性对账: 暂停期间最后一步可能已经跑完; 放行已满足依赖的 BLOCKED 步骤
    all_steps = steps_of(session, plan.id)
    dep_map = deps_of(session, plan.id)
    by_seq = {st.seq: st for st in all_steps}
    for st in all_steps:
        if st.status == "BLOCKED":
            wanted = dep_map.get(st.id, [])
            if wanted and all(by_seq.get(d) and by_seq[d].status == "SUCCESS" for d in wanted):
                st.status = "PENDING"
    session.flush()
    if all(st.status in ("SUCCESS", "SKIPPED") for st in all_steps):
        plan.status = "COMPLETED"
        plan_audit(session, plan, operator=operator, action="plan.complete",
                   reason=f"恢复时发现全部 {len(all_steps)} 个步骤已完成(暂停前最后一步刚跑完)")
    else:
        plan_audit(session, plan, operator=operator, action="plan.resume",
                   reason=f"从 {from_status} 恢复计划"
                          + (f", 重置步骤 seq={reset} 的重试计数" if reset else ""))
    return {"ok": True, "detail": "计划已恢复执行"}


def do_cancel(session: Session, plan: MigrationPlan, operator: str) -> dict:
    _require_status(plan, "cancel")
    from_status = plan.status
    plan.status = "CANCELED"
    plan.updated_by = operator
    skipped = 0
    for st in steps_of(session, plan.id):
        # RUNNING 步骤让它自然跑完(收尾逻辑会尊重 CANCELED); HALTED 保留终态;
        # 其余未开始/待重试的步骤不再执行
        if st.status in ("BLOCKED", "PENDING", "FAILED"):
            st.status = "SKIPPED"
            skipped += 1
            add_event(session, plan_id=plan.id, step_id=st.id, attempt=st.attempts,
                      event="skip", operator=operator, reason="计划被取消, 步骤不再执行")
    # 质量门禁暂停随取消终止: ACTIVE hold 置 CANCELED(不再自动恢复),
    # 活动(自动)重扫随计划取消, 即使之后扫描完成/门禁通过也不会恢复本计划
    from . import quality
    dismissed = quality.dismiss_holds_on_cancel(session, plan, operator)
    session.flush()
    plan_audit(session, plan, operator=operator, action="plan.cancel",
               reason=f"从 {from_status} 取消计划, {skipped} 个未开始步骤置为 SKIPPED"
                      + (f", {len(dismissed['scans_canceled'])} 个质量扫描随计划取消"
                         if dismissed["scans_canceled"] else ""))
    return {"ok": True, "detail": f"计划已取消, {skipped} 个未开始步骤被跳过",
            "quality": dismissed}


# ---------- 步骤执行(复用批次流程) ----------

def _stage_idempotency_key(plan_id: str, step_id: int, stage: str,
                           attempt: int, attempt_round: int) -> str:
    """批次动作的幂等键: 与管理员手工操作分命名空间。
    freeze 只应发生一次(固定键); validate/cutover 按"轮次+尝试编号"区分:
    同一轮内重复(含重启重放)返回批次动作首次结果; HALTED 恢复后轮次 +1,
    修复数据后的重试不会重放上一轮的失败结果。"""
    suffix = "once" if stage == "freeze" else f"r{attempt_round}a{attempt}"
    return f"plan-step:{plan_id}:{step_id}:{stage}:{suffix}"


class _StepFailed(Exception):
    """单次尝试失败(可重试): reason 记录失败原因, detail 为批次动作返回。"""

    def __init__(self, reason: str, detail: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail or {}


def _run_stage(session: Session, plan: MigrationPlan, step: PlanStep,
               batch: MigrationBatch, stage: str, attempt: int,
               operator: str) -> dict:
    """按批次当前阶段驱动一个批次动作, 全部走批次幂等框架(独立提交)。

    freeze/validate/cutover 都是"到达目标阶段即成功": 上一次尝试可能已经推进过批次,
    这里依据批次实际阶段判断, 保证计划侧重试/重启重放不会重复推进。
    """
    key = _stage_idempotency_key(plan.id, step.id, stage, attempt, step.attempt_round)
    try:
        if stage == "freeze":
            if batch.phase == "NORMAL":
                res, _ = service.run_admin_action(
                    session, action="freeze", operator=operator,
                    idempotency_key=key, payload={"by_plan": plan.id},
                    batch_id=batch.id,
                    fn=lambda s, b: service.do_freeze(s, b, operator))
            else:
                res = {"ok": True, "detail": f"批次已在 {batch.phase}, 跳过冻结"}
        elif stage == "validate":
            if batch.phase in ("FROZEN", "VALIDATING", "VALIDATED"):
                res, _ = service.run_admin_action(
                    session, action="validate", operator=operator,
                    idempotency_key=key, payload={"by_plan": plan.id},
                    batch_id=batch.id,
                    fn=lambda s, b: service.do_validate(s, b, operator))
            elif batch.phase == "DONE":
                res = {"ok": True, "detail": "批次已切换, 视为校验通过"}
            else:
                raise _StepFailed(f"批次处于 {batch.phase}, 无法校验(需先冻结)")
            if not res.get("ok"):
                raise _StepFailed(f"校验发现 {len(res.get('diffs', []))} 处差异, 未通过", res)
        elif stage == "cutover":
            if batch.phase == "VALIDATED":
                res, _ = service.run_admin_action(
                    session, action="cutover", operator=operator,
                    idempotency_key=key, payload={"by_plan": plan.id},
                    batch_id=batch.id,
                    fn=lambda s, b: service.do_cutover(s, b, operator, None))
            elif batch.phase == "DONE":
                res = {"ok": True, "detail": "批次已处于 DONE, 无需重复切换"}
            else:
                raise _StepFailed(f"批次处于 {batch.phase}, 无法切换(需先通过校验)")
            if not res.get("ok"):
                raise _StepFailed(f"切换前复核发现 {len(res.get('diffs', []))} 处差异", res)
        else:  # pragma: no cover - 内部编程错误
            raise _StepFailed(f"未知执行阶段 {stage}")
    except service.PhaseError as e:
        # 并发/阶段不允许(例如批次正被其他操作占用): 本次尝试失败, 按重试策略处理
        raise _StepFailed(f"批次 {batch.id} 阶段冲突: {e.detail}", {"diffs": e.diffs})
    except service.EpochConflict as e:
        raise _StepFailed(f"批次 {batch.id} 栅栏冲突: {e}")
    except service.BatchNotFound as e:
        raise _StepFailed(str(e))
    return res


def _attempt_step(session: Session, plan: MigrationPlan, step: PlanStep) -> None:
    """执行步骤的一次尝试: RUNNING 标记先提交(崩溃边界清晰), 再依次跑批次三阶段。

    成功: 步骤 SUCCESS, 放行其下游; 失败: 未超重试次数则 FAILED(下一 tick 自动重试),
    超过则 HALTED 且计划 HALTED, 阻塞全部后续步骤。
    """
    operator = plan.started_by or plan.created_by
    step.attempts += 1
    step.status = "RUNNING"
    step.executed_by = operator
    step.last_error = None
    session.flush()
    add_event(session, plan_id=plan.id, step_id=step.id, attempt=step.attempts,
              event=("start" if step.attempts == 1 else "retry"), operator=operator,
              reason=f"第 {step.attempts} 次尝试(最多额外重试 {step.max_retries} 次)")
    session.commit()  # RUNNING 边界: 崩溃后 boot 能发现并重置

    try:
        # 独立会话视角读取批次, 各阶段动作自带提交
        batch = service.get_batch(session, step.batch_id)
        for stage in ("freeze", "validate", "cutover"):
            batch = service.get_batch(session, step.batch_id)
            _run_stage(session, plan, step, batch, stage, step.attempts, operator)
    except _StepFailed as e:
        session.rollback()
        plan = lock_plan(session, plan.id)
        step = session.get(PlanStep, step.id)
        # 失败收尾期间计划可能已被暂停/取消(管理员操作发生在步骤执行中):
        # 取消 -> 步骤置 SKIPPED 不再重试; 暂停 -> 回到 PENDING, 恢复后续跑; 计数保留
        if plan.status == "CANCELED":
            step.status = "SKIPPED"
            step.last_error = e.reason[:500]
            add_event(session, plan_id=plan.id, step_id=step.id, attempt=step.attempts,
                      event="skip", operator=operator,
                      reason=f"尝试失败但计划已取消: {e.reason}"[:500], detail=e.detail)
            session.commit()
            return
        if plan.status == "PAUSED":
            step.status = "PENDING"
            step.last_error = e.reason[:500]
            add_event(session, plan_id=plan.id, step_id=step.id, attempt=step.attempts,
                      event="fail", operator=operator,
                      reason=f"尝试失败, 计划已暂停, 恢复后续跑: {e.reason}"[:500], detail=e.detail)
            plan.last_error = f"步骤 seq={step.seq} 暂停前最后一次尝试失败: {e.reason}"
            session.commit()
            return
        exhausted = step.attempts > step.max_retries
        step.status = "HALTED" if exhausted else "FAILED"
        step.last_error = e.reason[:500]
        add_event(session, plan_id=plan.id, step_id=step.id, attempt=step.attempts,
                  event=("halted" if exhausted else "fail"), operator=operator,
                  reason=e.reason[:500], detail=e.detail)
        if exhausted:
            plan.status = "HALTED"
            plan.failed_step_id = step.id
            plan.last_error = f"步骤 seq={step.seq}(批次 {step.batch_id}) 重试 {step.attempts} 次后仍失败: {e.reason}"
            plan.updated_by = operator
            plan_audit(session, plan, operator=operator, action="plan.halted",
                       step_id=step.id, reason=plan.last_error[:500])
        else:
            plan.last_error = f"步骤 seq={step.seq}(批次 {step.batch_id}) 第 {step.attempts} 次尝试失败: {e.reason}"
            plan.updated_by = operator
        session.commit()
        return

    # 成功
    step = session.get(PlanStep, step.id)
    plan = lock_plan(session, plan.id)
    step.status = "SUCCESS"
    step.last_error = None
    add_event(session, plan_id=plan.id, step_id=step.id, attempt=step.attempts,
              event="success", operator=operator,
              reason=f"批次 {step.batch_id} 已切换到 DONE(第 {step.attempts} 次尝试成功)")
    # 放行下游: 依赖全部 SUCCESS 的 BLOCKED 步骤转 PENDING(暂停/取消的计划不会被 worker 取走,
    # 提前放行是安全的; 取消时下游已是 SKIPPED, 不受影响)
    all_steps = steps_of(session, plan.id)
    dep_map = deps_of(session, plan.id)
    by_seq = {st.seq: st for st in all_steps}
    for st in all_steps:
        if st.status != "BLOCKED":
            continue
        wanted = dep_map.get(st.id, [])
        if wanted and all(by_seq.get(d) and by_seq[d].status == "SUCCESS" for d in wanted):
            st.status = "PENDING"
    session.flush()
    plan.updated_by = operator
    if plan.status == "CANCELED":
        # 取消发生在最后一步执行中: 批次已实际切开, 记录成功但不改变计划终态
        plan_audit(session, plan, operator=operator, action="plan.step_success",
                   step_id=step.id,
                   reason=f"步骤 seq={step.seq}(批次 {step.batch_id}) 完成, 但计划已取消")
        session.commit()
        return
    if plan.status == "PAUSED":
        # 暂停发生在步骤执行中: 步骤成功保留, 计划停在 PAUSED 等待管理员恢复
        plan_audit(session, plan, operator=operator, action="plan.step_success",
                   step_id=step.id,
                   reason=f"步骤 seq={step.seq}(批次 {step.batch_id}) 完成, 计划保持暂停, 恢复后推进下游")
        session.commit()
        return
    plan.last_error = None
    if all(st.status in ("SUCCESS", "SKIPPED") for st in all_steps):
        plan.status = "COMPLETED"
        plan.failed_step_id = None
        plan.quality_hold = False
        plan_audit(session, plan, operator=operator, action="plan.complete",
                   reason=f"全部 {len(all_steps)} 个步骤成功")
    else:
        plan_audit(session, plan, operator=operator, action="plan.step_success",
                   step_id=step.id, reason=f"步骤 seq={step.seq}(批次 {step.batch_id}) 完成")
    session.commit()


def run_plan_tick(session: Session, plan_id: str) -> bool:
    """推进一个计划至多一个就绪步骤。返回是否执行了步骤。

    仅 RUNNING 计划可推进; 暂停/取消在步骤边界检查。FAILED 步骤(仍有重试额度)
    会被再次取走 —— 每次 tick 只重试一步, 与 worker 的轮询节奏一致。
    执行窗口: 有窗口的 RUNNING 计划只在窗口内推进; 离开窗口在步骤边界暂停
    (状态仍为 RUNNING, window_open=False), 重新进入窗口后自动继续;
    两种状态变化都落计划审计。
    """
    plan = lock_plan(session, plan_id)
    if plan.status != "RUNNING":
        session.rollback()
        return False
    wins = windows_of(session, plan_id)
    if wins:
        within = in_any_window(wins)
        if not within:
            if plan.window_open is not False:
                # 刚离开窗口: 在步骤边界停住, 不取下一个步骤
                plan.window_open = False
                plan.updated_by = "system"
                plan_audit(session, plan, operator="system",
                           action="plan.window_pause",
                           reason=f"已离开允许执行窗口, 计划在步骤边界暂停, "
                                  f"重新进入窗口后自动继续(窗口: {_window_desc(wins)})")
                session.commit()
            else:
                session.rollback()
            return False
        if plan.window_open is False:
            # 重新进入窗口: 落审计后本 tick 即继续推进
            plan.window_open = True
            plan.updated_by = "system"
            plan_audit(session, plan, operator="system",
                       action="plan.window_resume",
                       reason="重新进入允许执行窗口, 计划自动继续推进")
            session.commit()
        else:
            plan.window_open = True
            session.flush()
    steps = steps_of(session, plan_id)
    dep_map = deps_of(session, plan_id)
    by_seq = {st.seq: st for st in steps}
    candidate: PlanStep | None = None
    for st in steps:
        if st.status in ("PENDING", "FAILED"):
            candidate = st
            break
        if st.status == "BLOCKED":
            wanted = dep_map.get(st.id, [])
            if wanted and all(by_seq.get(d) and by_seq[d].status == "SUCCESS" for d in wanted):
                candidate = st
                break
    if candidate is None:
        session.rollback()
        return False
    # 执行中质量门禁(在真正推进步骤前的步骤边界检查): 批次数据写入/规则版本
    # 变化/扫描过期使门禁失效时, 计划暂停在原步骤(步骤状态与尝试计数不复位),
    # 系统编排唯一自动重扫; 重扫完成且阻断问题处理完(门禁 PASS)后自动恢复。
    from . import quality
    gate_check = quality.assert_executor_gate(session, plan)
    if gate_check["blocked"]:
        return False
    _attempt_step(session, plan, candidate)
    # 步骤完成后的步骤边界也复评一次: 本步执行期间质量结果可能已失效(如执行中
    # 触发了重扫且发现阻断问题)。此时不回滚已成功步骤, 但在推进下一步前暂停,
    # 等待重扫与问题处理 —— 后续步骤的批次不会在门禁失效期间被切换。
    fresh = lock_plan(session, plan_id)
    if fresh.status == "RUNNING" and not fresh.quality_hold:
        quality.assert_executor_gate(session, fresh)
    return True


# ---------- 启动恢复: 计划不能回到错误的"运行中" ----------

def boot_recover_plans(session: Session) -> None:
    """重启对账:
    1. 遗留 RUNNING 步骤(进程已死, 不可能真的在跑)重置为 PENDING, attempts 保留,
       FAILED/重试信息落事件与审计 —— 由 worker 重新尝试;
    2. RUNNING 计划保持 RUNNING 等 worker 续跑, 并各落一条 boot 审计;
       PAUSED/HALTED 等用户态保持不变;
    3. 顺带把"依赖已全部成功却仍 BLOCKED"的步骤放行(防御性)。
    """
    plans = session.query(MigrationPlan).order_by(MigrationPlan.id).all()
    for plan in plans:
        steps = steps_of(session, plan.id)
        dep_map = deps_of(session, plan.id)
        by_seq = {st.seq: st for st in steps}
        dirty = False
        for st in steps:
            if st.status == "RUNNING":
                st.status = "PENDING"
                st.last_error = "服务重启时该步骤处于执行中, 已重置为待执行(批次动作幂等, 安全重跑)"
                add_event(session, plan_id=plan.id, step_id=st.id, attempt=st.attempts,
                          event="reset", operator="system",
                          reason="重启恢复: RUNNING 步骤不可能跨进程存活, 重置为 PENDING")
                plan_audit(session, plan, operator="system", action="boot",
                           step_id=st.id,
                           reason=f"重启恢复: 步骤 seq={st.seq} 原处于 RUNNING, 重置为 PENDING")
                dirty = True
            elif st.status == "BLOCKED":
                wanted = dep_map.get(st.id, [])
                if wanted and all(by_seq.get(d) and by_seq[d].status == "SUCCESS"
                                  for d in wanted):
                    st.status = "PENDING"
                    dirty = True
        if plan.status == "RUNNING":
            # 窗口闸门对账: 窗口边界持久化在 plan_windows 表, 重启后按当前时刻
            # 重新判定, 审批状态/窗口边界均不丢失; 跨重启边界的窗口进出落审计
            wins = windows_of(session, plan.id)
            if wins:
                within = in_any_window(wins)
                if plan.window_open is not False and not within:
                    plan.window_open = False
                    plan_audit(session, plan, operator="system",
                               action="plan.window_pause",
                               reason="服务重启对账: 当前不在执行窗口内, 保持暂停, 重新进入窗口后续跑")
                    dirty = True
                elif plan.window_open is False and within:
                    plan.window_open = True
                    plan_audit(session, plan, operator="system",
                               action="plan.window_resume",
                               reason="服务重启对账: 已重新进入执行窗口, worker 将自动继续")
                    dirty = True
                elif plan.window_open is None:
                    plan.window_open = within
                    dirty = True
            plan_audit(session, plan, operator="system", action="boot",
                       reason=f"服务重启, 计划 {plan.id} 为 RUNNING, worker 将从安全停止点续跑")
            dirty = True
        if dirty:
            session.commit()


# ---------- 后台 worker ----------

class PlanWorker:
    """单实例后台线程: 周期性推进所有 RUNNING 计划(每个计划每 tick 一步)。

    多副本部署时应由带行锁/租约的单一调度器替代; 本服务按单实例运行,
    SQLite 写锁 / Postgres 行锁保证 worker 与管理动作不互相破坏。
    """

    def __init__(self, poll_interval: float = 0.5):
        self.poll_interval = poll_interval
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="plan-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        """取所有 RUNNING 计划各推进一步, 返回推进的步骤数。"""
        from .db import SessionLocal
        advanced = 0
        db = SessionLocal()
        try:
            plan_ids = [p.id for p in (db.query(MigrationPlan)
                                       .filter(MigrationPlan.status == "RUNNING")
                                       .order_by(MigrationPlan.id).all())]
        finally:
            db.close()
        for pid in plan_ids:
            db = SessionLocal()
            try:
                if run_plan_tick(db, pid):
                    advanced += 1
            except Exception:  # worker 绝不因单步异常退出; 错误已在步骤/计划上留痕
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


# ---------- 视图 ----------

def step_to_dict(session: Session, plan: MigrationPlan, step: PlanStep,
                 dep_map: dict[int, list[int]], recent_events: dict[int, dict]) -> dict:
    batch = session.get(MigrationBatch, step.batch_id)
    return {
        "id": step.id,
        "seq": step.seq,
        "batch_id": step.batch_id,
        "batch_biz": batch.biz if batch else None,
        "status": step.status,
        "attempts": step.attempts,
        "attempt_round": step.attempt_round,
        "max_retries": step.max_retries,
        "depends_on": dep_map.get(step.id, []),
        "last_error": step.last_error,
        "executed_by": step.executed_by,
        "batch_phase": batch.phase if batch else None,
        "recent_event": recent_events.get(step.id),
        "updated_at": step.updated_at.isoformat() if step.updated_at else None,
    }


def plan_to_dict(session: Session, plan: MigrationPlan, *, with_events: bool = True) -> dict:
    steps = steps_of(session, plan.id)
    dep_map = deps_of(session, plan.id)
    recent_events: dict[int, dict] = {}
    events: list[dict] = []
    if with_events:
        rows = (session.query(PlanStepEvent)
                .filter(PlanStepEvent.plan_id == plan.id)
                .order_by(PlanStepEvent.id.desc()).limit(50).all())
        for ev in rows:
            item = {
                "id": ev.id,
                "ts": ev.ts.isoformat() if ev.ts else None,
                "step_id": ev.step_id,
                "seq": next((s.seq for s in steps if s.id == ev.step_id), None),
                "attempt": ev.attempt, "event": ev.event,
                "operator": ev.operator, "reason": ev.reason, "detail": ev.detail,
            }
            events.append(item)
            recent_events.setdefault(ev.step_id, item)
    step_dicts = [step_to_dict(session, plan, s, dep_map, recent_events) for s in steps]
    total = len(step_dicts)
    succeeded = sum(1 for s in step_dicts if s["status"] == "SUCCESS")
    windows = windows_of(session, plan.id)
    window_dicts = [{
        "starts_at": w.starts_at.isoformat() + "Z",
        "ends_at": w.ends_at.isoformat() + "Z",
        "created_by": w.created_by,
        "created_at": w.created_at.isoformat() if w.created_at else None,
    } for w in windows]
    # 页面实时窗口状态: 无窗口恒为 True; 有窗口每次按当前时刻判定
    in_window = in_any_window(windows)
    # 质量门禁暂停摘要(触发来源/暂停原因/关联重扫/恢复历史入口)
    quality_hold = None
    if plan.quality_hold:
        from . import quality
        quality_hold = quality.plan_hold_summary(session, plan.id)
    return {
        "id": plan.id,
        "name": plan.name,
        "status": plan.status,
        "quality_hold_flag": bool(plan.quality_hold),
        "quality_hold": quality_hold,
        "max_retries": plan.max_retries,
        "last_error": plan.last_error,
        "failed_step_id": plan.failed_step_id,
        "risk_level": plan.risk_level,
        "approval_status": plan.approval_status,
        "approved_by": plan.approved_by,
        "approved_at": plan.approved_at.isoformat() if plan.approved_at else None,
        "reject_reason": plan.reject_reason,
        "windows": window_dicts,
        "has_windows": bool(windows),
        "window_open": (None if not windows else plan.window_open),
        "in_window": in_window,
        "created_by": plan.created_by,
        "started_by": plan.started_by,
        "updated_by": plan.updated_by,
        "created_at": plan.created_at.isoformat() if plan.created_at else None,
        "updated_at": plan.updated_at.isoformat() if plan.updated_at else None,
        "progress": {"done": succeeded, "total": total},
        "steps": step_dicts,
        "dependencies": [
            {"step_seq": s["seq"], "depends_on": s["depends_on"]} for s in step_dicts
        ],
        "events": events,
    }
