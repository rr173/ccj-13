"""迁移回放与报告: 基于(已有迁移计划, 持久化审计检查点)对原计划做只读重放。

设计要点:
1. 检查点(ReplayCheckpoint)面向一个计划固化: 全局审计游标、计划/步骤审计证据、
   每个批次的批次行快照与范围内旧/新表全量数据快照。只有 COMPLETE 检查点
   (计划已 COMPLETED、每步批次都有 freeze/cutover 审计且步骤 SUCCESS)可用于回放;
   INCOMPLETE 检查点仍持久化并逐条列出原因, 但回放创建会被明确拒绝。
2. 回放任务状态机: QUEUED(排队等并发额度) -> RUNNING -> COMPLETED;
   pause 在步骤边界停住(PAUSED), resume 重新排队(QUEUED, 再次受并发闸门约束);
   cancel 终止任务并把未开始步骤置 SKIPPED; 检查点缺失/审计不完整/快照无法还原
   等致命错误 -> FAILED, 已完成步骤的报告原样保留。
3. 回放严格只读: 绝不修改批次、计划、业务记录新旧表与业务审计(audit_log),
   只写 replay_* 表; 预期状态来自检查点快照, 实际状态来自当前业务表, 逐字段出差异。
4. 步骤边界即崩溃边界: RUNNING 标记先提交, 再生成报告; 重启对账把遗留 RUNNING
   任务/步骤复位到 QUEUED/PENDING(已完成报告不丢), worker 从安全位置续跑。
5. 并发: 同时处于 RUNNING 的回放任务不超过 REPLAY_MAX_CONCURRENCY(默认 2),
   其余排队; worker 调度在 Postgres 下用咨询锁串行, SQLite 写事务天然串行。
6. 创建/暂停/恢复/取消全部走幂等框架(replay.* 命名空间), 重复请求无副作用。
7. 报告复核工作流(仅 COMPLETED 回放): 管理员对每个 SUCCESS 步骤提交复核结论
   (PASS/FAIL)、问题说明与修复标签, 结论与报告版本绑定、只追加保留历史;
   任一 FAIL 回放进入 PENDING(待处理), 全部步骤 PASS 才可确认为 CONFIRMED;
   待处理回放可重新打开(报告版本 +1 进入新一轮复核, 旧结论保留为历史)。
   复核写入校验报告版本与步骤状态, 过期版本返回冲突且不覆盖新结论。
"""
import hashlib
import json
import os
import threading
import time
import uuid

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import plans, service
from .models import (
    REVIEW_VERDICTS, AuditLog, MigrationBatch, MigrationPlan, PlanStep,
    RecordNew, RecordOld, ReplayCheckpoint, ReplayCheckpointStep, ReplayReview,
    ReplayTask, ReplayTaskEvent, ReplayTaskStep,
)
from .service import APP_VERSION

# 任务状态 -> 允许的管理动作
REPLAY_ALLOWED_ACTIONS = {
    "pause": {"QUEUED", "RUNNING"},
    "resume": {"PAUSED"},
    "cancel": {"QUEUED", "RUNNING", "PAUSED"},
}


class ReplayNotFound(Exception):
    """计划 / 检查点 / 回放任务不存在 -> 404。"""


class ReplayStateError(Exception):
    """当前状态不允许该操作(检查点不完整/状态机冲突/幂等键复用) -> 409。"""


class _ReplayFatal(Exception):
    """回放执行中的致命错误: 任务必须 FAILED。code 为机器可读错误码。"""

    def __init__(self, code: str, reason: str):
        super().__init__(reason)
        self.code = code
        self.reason = reason


# ---------- 并发配置 ----------

def max_concurrency() -> int:
    """同时允许 RUNNING 的回放任务数, 环境变量 REPLAY_MAX_CONCURRENCY(默认 2, 至少 1)。"""
    try:
        return max(1, int(os.getenv("REPLAY_MAX_CONCURRENCY", "2")))
    except ValueError:
        return 2


def now_utc_naive():
    return plans.now_utc_naive()


# ---------- 幂等框架(replay.* 命名空间, 与批次/计划动作互不撞键) ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def run_replay_action(session: Session, *, action: str, operator: str,
                      idempotency_key: str, payload: dict, fn,
                      task_id: str | None = None) -> tuple[dict, bool]:
    """与 plans.run_plan_action 同构: fn 在同事务执行, 结果与幂等键一起落库;
    重复提交(含崩溃重放)返回首次结果。请求哈希带任务 id, 同键跨任务/跨动作复用被拒。"""
    req_hash = _hash({"action": f"replay.{action}", "task_id": task_id, "payload": payload})
    from .models import IdempotencyKey
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise ReplayStateError("幂等键被不同请求复用")
        return existing.response_json, True

    task = lock_replay_task(session, task_id) if task_id is not None else None
    result = fn(session, task)
    if task_id is not None:
        fresh = get_replay_task(session, task_id)
        result["replay_id"] = task_id
        result["status"] = fresh.status
    session.add(IdempotencyKey(key=idempotency_key, action=f"replay.{action}",
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


# ---------- 行锁 / 创建串行锁 ----------

def get_replay_task(session: Session, task_id: str) -> ReplayTask:
    task = session.get(ReplayTask, task_id)
    if task is None:
        raise ReplayNotFound(f"回放任务 {task_id} 不存在")
    return task


def lock_replay_task(session: Session, task_id: str) -> ReplayTask:
    if session.bind.dialect.name != "sqlite":
        task = session.get(ReplayTask, task_id, with_for_update=True)
        if task is not None:
            return task
    return get_replay_task(session, task_id)


def _lock_creation(session: Session) -> None:
    """串行化回放创建(同一计划+检查点的重复创建竞争)。SQLite 写事务天然串行。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260915)"))


def _lock_scheduling(session: Session) -> None:
    """串行化 worker 的并发额度认领。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260916)"))


# ---------- 事件流水(只追加; 回放不写业务审计 audit_log) ----------

def add_event(session: Session, *, task_id: str, event: str, operator: str,
              step_seq: int | None = None, reason: str | None = None,
              detail: dict | None = None) -> None:
    session.add(ReplayTaskEvent(
        task_id=task_id, step_seq=step_seq, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail,
    ))


# ---------- 检查点 ----------

def _checkpoint_evidence(session: Session, plan: MigrationPlan,
                         step: PlanStep) -> tuple[list[str], list[int], list[int]]:
    """核验一个计划步骤(批次)的审计证据, 返回 (问题列表, 关键审计id, 该批次全部审计id)。

    完整证据: 步骤 SUCCESS + 批次仍存在 + 该批次有 freeze 与 cutover 业务审计。
    """
    issues: list[str] = []
    batch = session.get(MigrationBatch, step.batch_id)
    if batch is None:
        return [f"步骤 seq={step.seq} 的批次 {step.batch_id} 已不存在, 无法固化快照"], [], []
    rows = (session.query(AuditLog)
            .filter(AuditLog.batch_id == batch.id)
            .order_by(AuditLog.id).all())
    action_ids = [r.id for r in rows]
    if step.status != "SUCCESS":
        issues.append(f"步骤 seq={step.seq} 状态为 {step.status}(需 SUCCESS)")
    freeze_id = next((r.id for r in rows if r.action == "freeze"), None)
    cutover_id = next((r.id for r in rows if r.action == "cutover"), None)
    if freeze_id is None:
        issues.append(f"步骤 seq={step.seq} 批次 {batch.id} 缺少 freeze 审计")
    if cutover_id is None:
        issues.append(f"步骤 seq={step.seq} 批次 {batch.id} 缺少 cutover 审计")
    required = [aid for aid in (freeze_id, cutover_id) if aid is not None]
    return issues, required, action_ids


def do_create_checkpoint(session: Session, _task, operator: str,
                         plan_id: str) -> dict:
    """为已有计划固化审计检查点与批次数据快照。任何计划都可固化:
    不完整的检查点(status=INCOMPLETE, 原因逐条列出)同样持久化, 但不能用于回放。"""
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise ReplayNotFound(f"迁移计划 {plan_id} 不存在")
    cursor = session.query(func.max(AuditLog.id)).scalar() or 0
    cp_id = "C" + uuid.uuid4().hex[:10]
    cp = ReplayCheckpoint(
        id=cp_id, plan_id=plan_id, status="INCOMPLETE",
        audit_cursor_id=int(cursor), created_by=operator)
    session.add(cp)
    session.flush()

    plan_issues: list[str] = []
    if plan.status != "COMPLETED":
        plan_issues.append(
            f"计划当前状态为 {plan.status}, 只有 COMPLETED 计划的检查点可用于回放")
    has_create_audit = (session.query(AuditLog.id)
                        .filter(AuditLog.plan_id == plan_id,
                                AuditLog.action == "plan.create")
                        .first())
    if not has_create_audit:
        plan_issues.append("计划缺少 plan.create 审计, 审计链不完整")

    steps = plans.steps_of(session, plan_id)
    if not steps:
        plan_issues.append("计划没有任何步骤, 无可固化内容")
    complete_steps = 0
    for st in steps:
        issues, required_ids, action_ids = _checkpoint_evidence(session, plan, st)
        batch = session.get(MigrationBatch, st.batch_id)
        old_records = new_records = None
        old_count = new_count = 0
        b_phase = b_epoch = b_watermark = b_freeze = b_schema = None
        if batch is not None:
            old_rows = (session.query(RecordOld)
                        .filter(service._in_range(RecordOld.id, batch))
                        .order_by(RecordOld.id).all())
            new_rows = (session.query(RecordNew)
                        .filter(service._in_range(RecordNew.id, batch))
                        .order_by(RecordNew.id).all())
            old_records = [service.old_to_dict(r) for r in old_rows]
            new_records = [service.new_to_dict(r) for r in new_rows]
            old_count, new_count = len(old_records), len(new_records)
            b_phase, b_epoch = batch.phase, batch.epoch
            b_watermark = batch.watermark
            b_freeze = batch.freeze_version
            b_schema = batch.active_schema
        ok = not issues and len(required_ids) == 2
        if ok:
            complete_steps += 1
        session.add(ReplayCheckpointStep(
            checkpoint_id=cp_id, plan_step_id=st.id, seq=st.seq,
            batch_id=st.batch_id,
            audit_status="COMPLETE" if ok else "INCOMPLETE",
            audit_issues=issues or None,
            required_audit_ids=required_ids, audit_action_ids=action_ids,
            batch_phase=b_phase, batch_epoch=b_epoch, batch_watermark=b_watermark,
            batch_freeze_version=b_freeze, batch_active_schema=b_schema,
            old_records=old_records, new_records=new_records,
            old_count=old_count, new_count=new_count))

    cp.total_steps = len(steps)
    cp.complete_steps = complete_steps
    cp.issues = plan_issues or None
    cp.status = "COMPLETE" if not plan_issues and complete_steps == len(steps) and steps else "INCOMPLETE"
    session.flush()
    if cp.status == "COMPLETE":
        detail = f"检查点 {cp_id} 已固化(COMPLETE): 计划 {plan_id} 的 {len(steps)} 个步骤审计与批次快照完整, 可用于回放"
    else:
        all_issues = list(plan_issues)
        detail = (f"检查点 {cp_id} 已固化(INCOMPLETE): "
                  f"{complete_steps}/{len(steps)} 个步骤审计完整; "
                  f"{len(all_issues)} 条计划级问题, 不完整检查点不能用于回放")
    return {"ok": True, "checkpoint_id": cp_id, "plan_id": plan_id,
            "checkpoint_status": cp.status,
            "complete_steps": complete_steps, "total_steps": len(steps),
            "issues": plan_issues, "detail": detail}


def get_checkpoint(session: Session, checkpoint_id: str) -> ReplayCheckpoint:
    cp = session.get(ReplayCheckpoint, checkpoint_id)
    if cp is None:
        raise ReplayNotFound(f"审计检查点 {checkpoint_id} 不存在")
    return cp


# ---------- 回放任务创建 ----------

def _active_replay_for(session: Session, plan_id: str,
                       checkpoint_id: str) -> ReplayTask | None:
    return (session.query(ReplayTask)
            .filter(ReplayTask.plan_id == plan_id,
                    ReplayTask.checkpoint_id == checkpoint_id,
                    ReplayTask.status.in_(("QUEUED", "RUNNING", "PAUSED")))
            .first())


def do_create_replay(session: Session, _task, operator: str,
                     plan_id: str, checkpoint_id: str) -> dict:
    """基于(计划, 检查点)创建回放任务并排队。

    计划不存在/检查点不存在 -> 404 明确失败; 检查点不属于该计划/不完整 -> 409。
    同一(计划,检查点)已有非终态任务时重复创建幂等返回已有任务。
    """
    plan = session.get(MigrationPlan, plan_id)
    if plan is None:
        raise ReplayNotFound(f"迁移计划 {plan_id} 不存在")
    cp = session.get(ReplayCheckpoint, checkpoint_id)
    if cp is None:
        raise ReplayNotFound(f"审计检查点 {checkpoint_id} 不存在, 无法创建回放")
    if cp.plan_id != plan_id:
        raise ReplayStateError(
            f"检查点 {checkpoint_id} 属于计划 {cp.plan_id}, 不属于计划 {plan_id}")
    if cp.status != "COMPLETE":
        step_issues: list[str] = list(cp.issues or [])
        for cs in cp.steps:
            for why in (cs.audit_issues or []):
                step_issues.append(why)
        raise ReplayStateError(
            f"检查点 {checkpoint_id} 审计不完整({cp.complete_steps}/{cp.total_steps} 步完整), "
            f"不能创建回放: {'; '.join(step_issues) or '原因未记录'}")

    _lock_creation(session)
    existing = _active_replay_for(session, plan_id, checkpoint_id)
    if existing is not None:
        # 重复创建幂等: 直接回显已有任务, 不产生第二个任务
        add_event(session, task_id=existing.id, event="create.idempotent",
                  operator=operator,
                  reason=f"重复创建回放请求幂等返回已有任务 {existing.id}")
        session.flush()
        return {"ok": True, "replay_id": existing.id, "status": existing.status,
                "already_active": True,
                "detail": f"该计划+检查点已有{existing.status}回放任务 {existing.id}, 重复创建无副作用"}

    task_id = "R" + uuid.uuid4().hex[:10]
    steps = plans.steps_of(session, plan_id)
    dep_map = plans.deps_of(session, plan_id)
    task = ReplayTask(
        id=task_id, plan_id=plan_id, checkpoint_id=checkpoint_id,
        plan_name=plan.name, status="QUEUED", total_steps=len(steps),
        completed_steps=0, diff_count=0, state_diff_count=0,
        created_by=operator, updated_by=operator)
    session.add(task)
    session.flush()
    cp_step_by_plan_step = {cs.plan_step_id: cs for cs in cp.steps}
    for st in steps:
        cs = cp_step_by_plan_step.get(st.id)
        session.add(ReplayTaskStep(
            task_id=task_id,
            checkpoint_step_id=cs.id if cs is not None else None,
            plan_step_id=st.id, seq=st.seq, batch_id=st.batch_id,
            depends_on=list(dep_map.get(st.id, [])),
            status="PENDING"))
    add_event(session, task_id=task_id, event="create", operator=operator,
              detail={"plan_id": plan_id, "checkpoint_id": checkpoint_id,
                      "total_steps": len(steps)})
    add_event(session, task_id=task_id, event="queue", operator=operator,
              reason=f"回放任务已排队, 等待并发额度(最多同时 {max_concurrency()} 个回放)")
    session.flush()
    return {"ok": True, "replay_id": task_id, "status": "QUEUED",
            "already_active": False,
            "detail": f"回放任务 {task_id} 已创建并排队: 计划 {plan.name}({plan_id}), "
                      f"检查点 {checkpoint_id}, 共 {len(steps)} 个步骤, 全程只读不修改业务数据"}


# ---------- 暂停 / 恢复 / 取消(均幂等) ----------

def _require_status(task: ReplayTask, action: str) -> None:
    allowed = REPLAY_ALLOWED_ACTIONS[action]
    if task.status not in allowed:
        raise ReplayStateError(
            f"回放任务 {task.id} 当前状态 {task.status} 不允许 {action}"
            f"(仅 {sorted(allowed)} 状态可执行该操作)")


def do_pause(session: Session, task: ReplayTask, operator: str) -> dict:
    if task.status == "PAUSED":
        return {"ok": True, "already_in_state": True,
                "detail": "回放任务已处于暂停状态, 重复暂停无副作用"}
    _require_status(task, "pause")
    task.status = "PAUSED"
    task.updated_by = operator
    task.current_seq = None  # 暂停在步骤边界, 不存在"当前步骤"
    add_event(session, task_id=task.id, event="pause", operator=operator,
              reason=("暂停排队中的回放, 恢复后重新排队" if not task.started_at
                      else "暂停请求已记录, 将在当前步骤边界停住"))
    return {"ok": True, "detail": "回放任务已暂停, 将在当前步骤边界停止(已完成报告保留)"}


def do_resume(session: Session, task: ReplayTask, operator: str) -> dict:
    if task.status in ("QUEUED", "RUNNING"):
        return {"ok": True, "already_in_state": True,
                "detail": f"回放任务当前为 {task.status}, 恢复请求无副作用"}
    _require_status(task, "resume")
    task.status = "QUEUED"  # 重新排队, 再次受并发额度约束
    task.updated_by = operator
    task.last_error = None
    add_event(session, task_id=task.id, event="resume", operator=operator,
              reason="恢复回放: 重新排队, 获得并发额度后从下一未完成步骤续跑(已完成报告保留)")
    return {"ok": True, "detail": "回放任务已恢复, 已重新排队等待执行"}


def do_cancel(session: Session, task: ReplayTask, operator: str) -> dict:
    if task.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "回放任务已取消, 重复取消无副作用"}
    _require_status(task, "cancel")
    skipped = 0
    running_step = None
    for ts in task.task_steps:
        if ts.status == "PENDING":
            ts.status = "SKIPPED"
            ts.last_error = "任务被取消, 步骤不再执行"
            skipped += 1
            add_event(session, task_id=task.id, event="step.skip", operator=operator,
                      step_seq=ts.seq, reason="回放任务被取消, 未开始步骤跳过")
        elif ts.status == "RUNNING":
            running_step = ts
    task.status = "CANCELED"
    task.updated_by = operator
    task.current_seq = None
    if running_step is None:
        task.finished_at = now_utc_naive()
    add_event(session, task_id=task.id, event="cancel", operator=operator,
              reason=(f"取消回放: {skipped} 个未开始步骤跳过"
                      + (f", 执行中的步骤 seq={running_step.seq} 跑完报告后收尾"
                         if running_step else "")),
              detail={"skipped": skipped})
    return {"ok": True, "detail": f"回放任务已取消, {skipped} 个未开始步骤跳过, 已完成报告保留"}


# ---------- 报告复核工作流(仅 COMPLETED 回放; 结论与报告版本绑定, 历史只追加) ----------

def _current_reviews(session: Session, task_id: str, version: int) -> list[ReplayReview]:
    return (session.query(ReplayReview)
            .filter(ReplayReview.task_id == task_id,
                    ReplayReview.report_version == version)
            .order_by(ReplayReview.step_seq).all())


def _refresh_review_status(session: Session, task: ReplayTask) -> None:
    """按当前报告版本的复核结论重算任务复核状态:
    任一 FAIL -> PENDING(待处理); 有结论且无 FAIL -> REVIEWING; 无结论 -> UNREVIEWED。"""
    reviews = _current_reviews(session, task.id, task.report_version)
    if any(r.verdict == "FAIL" for r in reviews):
        task.review_status = "PENDING"
    elif reviews:
        task.review_status = "REVIEWING"
    else:
        task.review_status = "UNREVIEWED"


def _require_completed(task: ReplayTask, action: str) -> None:
    if task.status != "COMPLETED":
        raise ReplayStateError(
            f"回放任务 {task.id} 当前状态 {task.status}, 只有 COMPLETED 回放才能{action}")


def _require_current_version(task: ReplayTask, report_version: int) -> None:
    """报告版本栅栏: 过期版本直接冲突, 不写入也不覆盖已有新结论。"""
    if report_version != task.report_version:
        raise ReplayStateError(
            f"报告版本已过期: 请求基于 v{report_version}, 当前报告版本为 "
            f"v{task.report_version}; 过期版本不能写入, 也不会覆盖已有新结论, "
            f"请获取最新报告后重试")


def do_submit_review(session: Session, task: ReplayTask, operator: str,
                     step_seq: int, report_version: int, verdict: str,
                     issue: str | None, fix_tags: list[str] | None) -> dict:
    """提交单个步骤的复核结论(结论/问题说明/修复标签), 与当前报告版本绑定。

    校验: 回放须 COMPLETED 且未确认; 报告版本必须为当前版本(过期 409);
    步骤须存在且 SUCCESS(报告已生成); 同一(版本, 步骤)只允许一条结论,
    重复写入(不同幂等键)返回冲突, 不覆盖已有结论。
    """
    _require_completed(task, "提交复核结论")
    if task.review_status == "CONFIRMED":
        raise ReplayStateError(
            f"回放任务 {task.id} 已确认(CONFIRMED), 复核已锁定, 不能再提交结论")
    _require_current_version(task, report_version)
    verdict = (verdict or "").strip().upper()
    if verdict not in REVIEW_VERDICTS:
        raise ReplayStateError(f"复核结论必须是 PASS 或 FAIL(收到 {verdict!r})")
    issue = (issue or "").strip()[:500]
    if verdict == "FAIL" and not issue:
        raise ReplayStateError("复核结论为 FAIL 时问题说明必填")
    tags: list[str] = []
    for t in (fix_tags or []):
        t = str(t).strip()[:64]
        if t and t not in tags:
            tags.append(t)
    tstep = (session.query(ReplayTaskStep)
             .filter(ReplayTaskStep.task_id == task.id,
                     ReplayTaskStep.seq == step_seq).first())
    if tstep is None:
        raise ReplayNotFound(f"回放任务 {task.id} 没有 seq={step_seq} 的步骤")
    if tstep.status != "SUCCESS":
        raise ReplayStateError(
            f"步骤 seq={step_seq} 当前状态 {tstep.status}, "
            f"只有 SUCCESS(报告已生成)的步骤才能复核")
    existing = (session.query(ReplayReview)
                .filter(ReplayReview.task_id == task.id,
                        ReplayReview.report_version == task.report_version,
                        ReplayReview.step_seq == step_seq).first())
    if existing is not None:
        raise ReplayStateError(
            f"步骤 seq={step_seq} 在报告版本 v{task.report_version} 已有 "
            f"{existing.verdict} 结论({existing.operator} 提交), 不能覆盖; "
            f"如需改判请重新打开回放进入新版本")
    session.add(ReplayReview(
        task_id=task.id, report_version=task.report_version, step_seq=step_seq,
        batch_id=tstep.batch_id, verdict=verdict, issue=issue or None,
        fix_tags=tags, operator=operator))
    session.flush()
    _refresh_review_status(session, task)
    task.updated_by = operator
    add_event(session, task_id=task.id, event="review.submit", operator=operator,
              step_seq=step_seq,
              reason=(f"步骤 seq={step_seq} 复核结论 {verdict}"
                      f"(报告版本 v{task.report_version})"
                      + (f": {issue}" if issue else "")),
              detail={"report_version": task.report_version, "verdict": verdict,
                      "issue": issue or None, "fix_tags": tags})
    total = sum(1 for s in _task_steps(session, task.id) if s.status == "SUCCESS")
    reviewed = len(_current_reviews(session, task.id, task.report_version))
    note = ("; 发现问题, 回放进入待处理(PENDING), 处理问题后可重新打开"
            if task.review_status == "PENDING" else "")
    return {"ok": True, "replay_id": task.id, "report_version": task.report_version,
            "step_seq": step_seq, "verdict": verdict,
            "review_status": task.review_status,
            "review_progress": {"reviewed": reviewed, "total": total},
            "detail": (f"步骤 seq={step_seq} 复核结论 {verdict} 已记录"
                       f"(报告版本 v{task.report_version}); "
                       f"复核进度 {reviewed}/{total}{note}")}


def do_confirm_review(session: Session, task: ReplayTask, operator: str,
                      report_version: int) -> dict:
    """确认回放: 仅当当前报告版本全部 SUCCESS 步骤都 PASS 才允许; 重复确认幂等。"""
    if task.review_status == "CONFIRMED":
        return {"ok": True, "already_in_state": True,
                "detail": "回放已确认, 重复确认无副作用"}
    _require_completed(task, "确认")
    _require_current_version(task, report_version)
    if task.review_status == "PENDING":
        raise ReplayStateError(
            "当前报告版本存在未通过(FAIL)的复核结论, 不能确认; "
            "请先处理问题并重新打开回放")
    success_steps = [s for s in _task_steps(session, task.id) if s.status == "SUCCESS"]
    if not success_steps:
        raise ReplayStateError("回放没有已完成的步骤报告, 无可确认内容")
    by_seq = {r.step_seq: r
              for r in _current_reviews(session, task.id, task.report_version)}
    missing = [s.seq for s in success_steps
               if by_seq.get(s.seq) is None or by_seq[s.seq].verdict != "PASS"]
    if missing:
        raise ReplayStateError(
            f"步骤 {missing} 尚未通过复核(需全部 PASS), 不能确认回放")
    task.review_status = "CONFIRMED"
    task.confirmed_by = operator
    task.confirmed_at = now_utc_naive()
    task.updated_by = operator
    add_event(session, task_id=task.id, event="review.confirm", operator=operator,
              reason=(f"报告版本 v{task.report_version} 全部 "
                      f"{len(success_steps)} 个步骤复核通过, 回放确认"),
              detail={"report_version": task.report_version,
                      "steps": len(success_steps)})
    return {"ok": True, "review_status": "CONFIRMED",
            "detail": f"回放 {task.id} 已确认: 报告版本 v{task.report_version} "
                      f"全部 {len(success_steps)} 个步骤复核通过"}


def do_reopen_review(session: Session, task: ReplayTask, operator: str,
                     reason: str | None) -> dict:
    """重新打开待处理回放: 报告版本 +1 进入新一轮复核, 旧版本结论保留为历史。"""
    _require_completed(task, "重新打开")
    if task.review_status != "PENDING":
        raise ReplayStateError(
            f"回放任务 {task.id} 复核状态为 {task.review_status}, "
            f"仅待处理(PENDING)的回放可重新打开")
    old = task.report_version
    task.report_version = old + 1
    task.review_status = "UNREVIEWED"
    task.updated_by = operator
    add_event(session, task_id=task.id, event="review.reopen", operator=operator,
              reason=(reason or "重新打开回放, 进入新一轮复核"),
              detail={"from_version": old, "to_version": task.report_version})
    return {"ok": True, "report_version": task.report_version,
            "review_status": task.review_status,
            "detail": f"回放已重新打开: 报告版本 v{old} -> v{task.report_version}, "
                      f"历史结论保留, 请基于新版本重新复核"}


# ---------- 回放执行: 调度(并发闸门) + 步骤报告 ----------

def claim_due_tasks(session: Session) -> list[str]:
    """按并发额度把 QUEUED 任务认领为 RUNNING, 返回认领的任务 id(已提交)。"""
    _lock_scheduling(session)
    try:
        running = (session.query(func.count(ReplayTask.id))
                   .filter(ReplayTask.status == "RUNNING").scalar()) or 0
        slots = max_concurrency() - running
        if slots <= 0:
            session.rollback()
            return []
        due = (session.query(ReplayTask)
               .filter(ReplayTask.status == "QUEUED")
               .order_by(ReplayTask.created_at, ReplayTask.id)
               .limit(slots).all())
        claimed: list[str] = []
        at = now_utc_naive()
        for t in due:
            t.status = "RUNNING"
            t.started_at = t.started_at or at
            t.updated_by = "system"
            add_event(session, task_id=t.id, event="claim", operator="system",
                      reason=f"获得并发额度, 开始执行(并发上限 {max_concurrency()})")
            claimed.append(t.id)
        session.commit()
        return claimed
    except Exception:
        session.rollback()
        raise


def _task_steps(session: Session, task_id: str) -> list[ReplayTaskStep]:
    return (session.query(ReplayTaskStep)
            .filter(ReplayTaskStep.task_id == task_id)
            .order_by(ReplayTaskStep.seq).all())


def run_replay_tick(session: Session, task_id: str) -> bool:
    """推进一个 RUNNING 回放任务至多一个就绪步骤(依赖全部 SUCCESS 的首个 PENDING)。
    返回是否执行了步骤。暂停/取消在步骤边界检查。"""
    task = lock_replay_task(session, task_id)
    if task.status != "RUNNING":
        session.rollback()
        return False
    steps = _task_steps(session, task_id)
    seq_map = {s.seq: s for s in steps}
    candidate = None
    for ts in steps:
        if ts.status != "PENDING":
            continue
        wanted = ts.depends_on or []
        if all(seq_map.get(d) is not None and seq_map[d].status == "SUCCESS"
               for d in wanted):
            candidate = ts
            break
    if candidate is None:
        # 无可推进步骤: 正常情况下收尾已在最后一步完成时处理; 此处防御性对账
        if all(s.status in ("SUCCESS", "SKIPPED", "FAILED") for s in steps):
            pending_skips = [s for s in steps if s.status == "PENDING"]
            for s in pending_skips:
                s.status = "SKIPPED"
            if task.status == "RUNNING" and not any(s.status == "FAILED" for s in steps):
                task.status = "COMPLETED"
                task.current_seq = None
                task.finished_at = now_utc_naive()
                add_event(session, task_id=task.id, event="complete", operator="system",
                          reason="全部步骤回放完成")
                session.commit()
                return True
        session.rollback()
        return False
    _execute_step(session, task, candidate)
    return True


def _expected_record(old: dict) -> dict:
    """从检查点里的旧表行快照生成预期新结构行(与 service.transform 同一规则)。"""
    return {
        "id": old["id"],
        "name": old["name"],
        "email": old["email"],
        "tags": [t for t in (old.get("tags_csv") or "").split(",") if t],
        "schema_version": 2,
    }


def _build_step_report(session: Session, task: ReplayTask,
                       tstep: ReplayTaskStep) -> dict:
    """生成单步报告: 预期状态(检查点快照) vs 实际状态(当前业务表) vs 字段差异。

    致命情况(任务必须 FAILED): 检查点缺失/不完整、步骤快照缺失或损坏、
    关键审计被删除(审计不完整)、批次已不存在。全程只读。"""
    cp = session.get(ReplayCheckpoint, task.checkpoint_id)
    if cp is None:
        raise _ReplayFatal(
            "checkpoint_missing",
            f"检查点 {task.checkpoint_id} 不存在(可能已被删除), 回放无法继续")
    if cp.status != "COMPLETE":
        raise _ReplayFatal(
            "checkpoint_incomplete",
            f"检查点 {cp.id} 状态为 {cp.status}(审计不完整), 回放无法继续")
    if tstep.checkpoint_step_id is None:
        raise _ReplayFatal(
            "snapshot_missing",
            f"步骤 seq={tstep.seq} 缺少检查点快照引用, 批次数据无法还原")
    cs = session.get(ReplayCheckpointStep, tstep.checkpoint_step_id)
    if cs is None:
        raise _ReplayFatal(
            "snapshot_missing",
            f"步骤 seq={tstep.seq}(批次 {tstep.batch_id}) 的检查点快照不存在, 批次数据无法还原")
    missing_audit = [aid for aid in (cs.required_audit_ids or [])
                     if session.get(AuditLog, aid) is None]
    if missing_audit:
        raise _ReplayFatal(
            "audit_incomplete",
            f"步骤 seq={tstep.seq} 批次 {cs.batch_id} 的关键审计 {missing_audit} 已不存在, "
            f"审计链不完整, 回放无法继续")
    batch = session.get(MigrationBatch, cs.batch_id)
    if batch is None:
        raise _ReplayFatal(
            "batch_missing",
            f"步骤 seq={tstep.seq} 的批次 {cs.batch_id} 已不存在, 回放无法继续")
    if cs.old_records is None:
        raise _ReplayFatal(
            "snapshot_unrecoverable",
            f"步骤 seq={tstep.seq} 批次 {cs.batch_id} 的旧表快照已损坏(为空), "
            f"批次数据已无法还原")

    # 预期状态: 检查点固化的批次行 + 由旧表快照推导出的新结构记录
    expected_records = [_expected_record(o) for o in cs.old_records]
    expected_batch = {
        "phase": cs.batch_phase, "epoch": cs.batch_epoch,
        "active_schema": cs.batch_active_schema,
        "watermark": cs.batch_watermark,
        "freeze_version": cs.batch_freeze_version,
    }
    # 实际状态: 当前批次行 + 范围内当前新表记录(切换后新表是事实来源)
    actual_rows = (session.query(RecordNew)
                   .filter(service._in_range(RecordNew.id, batch))
                   .order_by(RecordNew.id).all())
    actual_records = [service.new_to_dict(r) for r in actual_rows]
    actual_batch = {
        "phase": batch.phase, "epoch": batch.epoch,
        "active_schema": batch.active_schema,
        "watermark": batch.watermark,
        "freeze_version": batch.freeze_version,
    }

    # 字段级差异: 缺失 / 不一致 / 多余
    diffs: list[dict] = []
    actual_by_id = {r["id"]: r for r in actual_records}
    for exp in expected_records:
        act = actual_by_id.get(exp["id"])
        if act is None:
            diffs.append({"record_id": exp["id"], "field": "__missing__",
                          "expected": exp, "actual": None})
            continue
        for field in ("name", "email", "tags"):
            if exp.get(field) != act.get(field):
                diffs.append({"record_id": exp["id"], "field": field,
                              "expected": exp.get(field), "actual": act.get(field)})
    expected_ids = {r["id"] for r in expected_records}
    for act in actual_records:
        if act["id"] not in expected_ids:
            diffs.append({"record_id": act["id"], "field": "__extra__",
                          "expected": None, "actual": act})

    # 批次级状态差异(属于发现项, 不致命): 阶段/生效结构/epoch/冻结窗
    state_diffs: list[dict] = []
    for field in ("phase", "active_schema", "epoch", "freeze_version"):
        if expected_batch.get(field) != actual_batch.get(field):
            state_diffs.append({"field": field,
                                "expected": expected_batch.get(field),
                                "actual": actual_batch.get(field)})

    return {
        "expected_state": {
            "checkpoint_id": cp.id, "batch_id": batch.id, "seq": tstep.seq,
            "batch": expected_batch,
            "records": expected_records,
            "record_count": len(expected_records),
            "checkpoint_new_snapshot_count": cs.new_count,
        },
        "actual_state": {
            "batch_id": batch.id, "seq": tstep.seq,
            "batch": actual_batch,
            "records": actual_records,
            "record_count": len(actual_records),
        },
        "diffs": diffs,
        "state_diffs": state_diffs,
    }


def _fail_task(session: Session, task: ReplayTask, tstep: ReplayTaskStep,
               code: str, reason: str, operator: str = "system") -> None:
    """致命失败收尾: 当前步骤 FAILED, 任务 FAILED, 其余未开始步骤 SKIPPED,
    已 SUCCESS 步骤的报告原样保留。"""
    task = lock_replay_task(session, task.id)
    if task.status in ("CANCELED", "COMPLETED", "FAILED"):
        session.rollback()
        return
    at = now_utc_naive()
    cur = session.get(ReplayTaskStep, tstep.id)
    if cur is not None and cur.status == "RUNNING":
        cur.status = "FAILED"
        cur.last_error = f"[{code}] {reason}"[:500]
        cur.finished_at = at
        add_event(session, task_id=task.id, event="step.failed", operator=operator,
                  step_seq=cur.seq, reason=f"[{code}] {reason}")
    skipped = 0
    for ts in _task_steps(session, task.id):
        if ts.status == "PENDING":
            ts.status = "SKIPPED"
            ts.last_error = f"任务因 {code} 失败, 步骤未执行"
            skipped += 1
    task.status = "FAILED"
    task.failure_reason = f"[{code}] {reason}"[:500]
    task.last_error = task.failure_reason
    task.current_seq = None
    task.finished_at = at
    task.updated_by = operator
    add_event(session, task_id=task.id, event="failed", operator=operator,
              reason=task.failure_reason,
              detail={"code": code, "skipped_steps": skipped})
    session.commit()


def _execute_step(session: Session, task: ReplayTask,
                  tstep: ReplayTaskStep) -> None:
    """执行单个回放步骤: RUNNING 边界先提交, 再只读生成报告。"""
    at = now_utc_naive()
    tstep.status = "RUNNING"
    tstep.started_at = at
    tstep.last_error = None
    task.current_seq = tstep.seq
    task.updated_by = "system"
    add_event(session, task_id=task.id, event="step.start", operator="system",
              step_seq=tstep.seq,
              detail={"batch_id": tstep.batch_id, "depends_on": tstep.depends_on})
    session.commit()  # 崩溃边界: 重启后 RUNNING 任务/步骤被复位到安全位置

    try:
        report = _build_step_report(session, task, tstep)
    except _ReplayFatal as e:
        session.rollback()
        _fail_task(session, task, tstep, e.code, e.reason)
        return
    except Exception as e:  # 防御: 任何意外都明确失败并留痕, 不丢已完成报告
        session.rollback()
        _fail_task(session, task, tstep, "internal_error",
                   f"生成回放报告时发生未预期错误: {e}")
        return

    # 报告生成期间任务可能已被暂停/取消: 尊重控制状态收尾
    task = lock_replay_task(session, task.id)
    tstep = session.get(ReplayTaskStep, tstep.id)
    at = now_utc_naive()

    if task.status == "CANCELED":
        tstep.status = "SKIPPED"
        tstep.last_error = "报告生成时任务已被取消, 报告不计入"
        tstep.finished_at = at
        task.current_seq = None
        task.finished_at = at
        add_event(session, task_id=task.id, event="step.skip", operator="system",
                  step_seq=tstep.seq, reason="步骤报告生成期间任务被取消")
        session.commit()
        return

    # 正常落报告(暂停发生在执行中也保留这一步, 与计划编排"当前步跑完"语义一致)
    tstep.status = "SUCCESS"
    tstep.expected_state = report["expected_state"]
    tstep.actual_state = report["actual_state"]
    tstep.diffs = report["diffs"]
    tstep.state_diffs = report["state_diffs"]
    tstep.diff_count = len(report["diffs"])
    tstep.state_diff_count = len(report["state_diffs"])
    tstep.finished_at = at
    add_event(session, task_id=task.id, event="step.success", operator="system",
              step_seq=tstep.seq,
              detail={"diffs": tstep.diff_count, "state_diffs": tstep.state_diff_count},
              reason=(f"步骤 seq={tstep.seq} 回放完成: {tstep.diff_count} 处字段差异, "
                      f"{tstep.state_diff_count} 处批次状态差异"))

    steps = _task_steps(session, task.id)
    task.completed_steps = sum(1 for s in steps if s.status == "SUCCESS")
    task.diff_count = sum(s.diff_count for s in steps if s.status == "SUCCESS")
    task.state_diff_count = sum(s.state_diff_count for s in steps
                                if s.status == "SUCCESS")
    task.updated_by = "system"
    if task.status == "PAUSED":
        task.current_seq = None
        add_event(session, task_id=task.id, event="pause.boundary", operator="system",
                  reason=f"步骤 seq={tstep.seq} 报告完成, 任务在步骤边界暂停, 恢复后续跑")
        session.commit()
        return
    if all(s.status in ("SUCCESS", "SKIPPED") for s in steps):
        task.status = "COMPLETED"
        task.current_seq = None
        task.finished_at = now_utc_naive()
        task.last_error = None
        add_event(session, task_id=task.id, event="complete", operator="system",
                  reason=f"全部 {len(steps)} 个步骤回放完成: 累计 {task.diff_count} 处字段差异, "
                         f"{task.state_diff_count} 处批次状态差异",
                  detail={"diff_count": task.diff_count,
                          "state_diff_count": task.state_diff_count})
    elif task.status == "RUNNING":
        # 执行期间可能收到 pause: 状态停留在 PAUSED(上面已处理), 否则正常交还 worker
        task.current_seq = next((s.seq for s in steps if s.status == "PENDING"), None)
    session.commit()


# ---------- 重启对账: 执行中的任务回到安全的待执行位置 ----------

def boot_recover_replays(session: Session) -> None:
    """服务重启时:
    1. 遗留 RUNNING 任务(执行线程已死)回到 QUEUED, 重新参与并发排队;
       其遗留 RUNNING 步骤复位为 PENDING(报告尚未提交, 安全重跑), SUCCESS 报告不丢;
    2. QUEUED/PAUSED/终态任务保持不变(排队/暂停是持久化的用户态)。"""
    tasks = session.query(ReplayTask).order_by(ReplayTask.id).all()
    for task in tasks:
        if task.status != "RUNNING":
            continue
        task.status = "QUEUED"
        task.current_seq = None
        task.updated_by = "system"
        for ts in _task_steps(session, task.id):
            if ts.status == "RUNNING":
                ts.status = "PENDING"
                ts.started_at = None
                ts.last_error = "服务重启时该步骤正在回放, 已复位为待执行(回放只读, 安全重跑)"
                add_event(session, task_id=task.id, event="boot.reset_step",
                          operator="system", step_seq=ts.seq,
                          reason="重启恢复: RUNNING 步骤报告尚未提交, 复位为 PENDING")
        add_event(session, task_id=task.id, event="boot", operator="system",
                  reason="服务重启: RUNNING 回放任务回到排队位置, 已完成报告保留, worker 将续跑")
        session.commit()


# ---------- 后台 worker ----------

class ReplayWorker:
    """单实例后台线程: 认领排队任务(受并发闸门)并各推进一步。

    与 PlanWorker 相同的单副本假设: SQLite 写锁 / Postgres 咨询锁与行锁
    串行化 worker 与管理动作, 不会越过并发上限或重复执行步骤。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("REPLAY_WORKER_POLL_INTERVAL", "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="replay-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        """认领到期任务并给每个 RUNNING 任务(含刚认领的)推进一步, 返回推进的步骤数。"""
        from .db import SessionLocal
        db = SessionLocal()
        try:
            claimed = claim_due_tasks(db)
            run_ids = [r[0] for r in (db.query(ReplayTask.id)
                                      .filter(ReplayTask.status == "RUNNING")
                                      .order_by(ReplayTask.id).all())]
        finally:
            db.close()
        task_ids = list(claimed) + [i for i in run_ids if i not in claimed]
        advanced = 0
        for tid in task_ids:
            db = SessionLocal()
            try:
                if run_replay_tick(db, tid):
                    advanced += 1
            except Exception:  # worker 绝不因单个任务异常退出; 失败已在任务上留痕
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

def _dt(v) -> str | None:
    return v.isoformat() if v else None


def checkpoint_step_to_dict(cs: ReplayCheckpointStep) -> dict:
    return {
        "id": cs.id,
        "seq": cs.seq,
        "plan_step_id": cs.plan_step_id,
        "batch_id": cs.batch_id,
        "audit_status": cs.audit_status,
        "audit_issues": cs.audit_issues or [],
        "required_audit_ids": cs.required_audit_ids or [],
        "batch_snapshot": {
            "phase": cs.batch_phase, "epoch": cs.batch_epoch,
            "active_schema": cs.batch_active_schema,
            "watermark": cs.batch_watermark,
            "freeze_version": cs.batch_freeze_version,
        },
        "old_count": cs.old_count,
        "new_count": cs.new_count,
    }


def checkpoint_to_dict(cp: ReplayCheckpoint, *, with_steps: bool = True) -> dict:
    out = {
        "id": cp.id,
        "plan_id": cp.plan_id,
        "status": cp.status,
        "audit_cursor_id": cp.audit_cursor_id,
        "issues": cp.issues or [],
        "total_steps": cp.total_steps,
        "complete_steps": cp.complete_steps,
        "created_by": cp.created_by,
        "created_at": _dt(cp.created_at),
    }
    if with_steps:
        out["steps"] = [checkpoint_step_to_dict(s) for s in cp.steps]
    return out


def review_to_dict(rv: ReplayReview) -> dict:
    return {
        "id": rv.id,
        "task_id": rv.task_id,
        "report_version": rv.report_version,
        "step_seq": rv.step_seq,
        "batch_id": rv.batch_id,
        "verdict": rv.verdict,
        "issue": rv.issue,
        "fix_tags": list(rv.fix_tags or []),
        "operator": rv.operator,
        "created_at": _dt(rv.created_at),
    }


def review_summary(session: Session, task: ReplayTask) -> dict:
    """当前报告版本的复核进度汇总(列表/详情/状态接口共用)。"""
    reviews = _current_reviews(session, task.id, task.report_version)
    success_seqs = [s.seq for s in _task_steps(session, task.id)
                    if s.status == "SUCCESS"]
    return {
        "status": task.review_status,
        "report_version": task.report_version,
        "total": len(success_seqs),
        "reviewed": len(reviews),
        "passed": sum(1 for r in reviews if r.verdict == "PASS"),
        "failed": sum(1 for r in reviews if r.verdict == "FAIL"),
        "confirmed_by": task.confirmed_by,
        "confirmed_at": _dt(task.confirmed_at),
        "steps": {str(r.step_seq): review_to_dict(r) for r in reviews},
    }


def reviews_view(session: Session, task: ReplayTask) -> dict:
    """复核记录全量视图: 当前版本逐步结论 + 全部历史版本结论 + 复核事件流水。"""
    steps = _task_steps(session, task.id)
    current = {r.step_seq: r
               for r in _current_reviews(session, task.id, task.report_version)}
    history = (session.query(ReplayReview)
               .filter(ReplayReview.task_id == task.id)
               .order_by(ReplayReview.id.desc()).all())
    events = (session.query(ReplayTaskEvent)
              .filter(ReplayTaskEvent.task_id == task.id,
                      ReplayTaskEvent.event.like("review.%"))
              .order_by(ReplayTaskEvent.id.desc()).limit(50).all())
    return {
        "replay_id": task.id,
        "task_status": task.status,
        "review": review_summary(session, task),
        "steps": [
            {"seq": s.seq, "batch_id": s.batch_id, "step_status": s.status,
             "review": review_to_dict(current[s.seq]) if s.seq in current else None}
            for s in steps
        ],
        "history": [review_to_dict(r) for r in history],
        "events": [
            {"id": e.id, "ts": _dt(e.ts), "step_seq": e.step_seq,
             "event": e.event, "operator": e.operator,
             "reason": e.reason, "detail": e.detail}
            for e in events
        ],
    }


def review_queue_view(session: Session, review_status: str) -> list[dict]:
    """按复核状态查询回放任务(待处理队列)。"""
    rows = (session.query(ReplayTask)
            .filter(ReplayTask.review_status == review_status)
            .order_by(ReplayTask.updated_at.desc(), ReplayTask.id).all())
    return [replay_to_dict(session, t, with_report=False) for t in rows]


def replay_step_to_dict(ts: ReplayTaskStep, *, with_report: bool = False) -> dict:
    out = {
        "id": ts.id,
        "seq": ts.seq,
        "plan_step_id": ts.plan_step_id,
        "batch_id": ts.batch_id,
        "depends_on": ts.depends_on or [],
        "status": ts.status,
        "diff_count": ts.diff_count,
        "state_diff_count": ts.state_diff_count,
        "last_error": ts.last_error,
        "started_at": _dt(ts.started_at),
        "finished_at": _dt(ts.finished_at),
    }
    if with_report:
        out.update({
            "expected_state": ts.expected_state,
            "actual_state": ts.actual_state,
            "diffs": ts.diffs or [],
            "state_diffs": ts.state_diffs or [],
        })
    return out


def replay_to_dict(session: Session, task: ReplayTask, *,
                   with_report: bool = False) -> dict:
    steps = _task_steps(session, task.id)
    events = (session.query(ReplayTaskEvent)
              .filter(ReplayTaskEvent.task_id == task.id)
              .order_by(ReplayTaskEvent.id.desc()).limit(50).all())
    return {
        "id": task.id,
        "plan_id": task.plan_id,
        "plan_name": task.plan_name,
        "checkpoint_id": task.checkpoint_id,
        "status": task.status,
        "progress": {"done": task.completed_steps, "total": task.total_steps},
        "total_steps": task.total_steps,
        "completed_steps": task.completed_steps,
        "current_seq": task.current_seq,
        "diff_count": task.diff_count,
        "state_diff_count": task.state_diff_count,
        "last_error": task.last_error,
        "failure_reason": task.failure_reason,
        "concurrency_limit": max_concurrency(),
        "created_by": task.created_by,
        "updated_by": task.updated_by,
        "started_at": _dt(task.started_at),
        "finished_at": _dt(task.finished_at),
        "created_at": _dt(task.created_at),
        "updated_at": _dt(task.updated_at),
        "review": review_summary(session, task),
        "steps": [replay_step_to_dict(s, with_report=with_report) for s in steps],
        "events": [
            {"id": e.id, "ts": _dt(e.ts), "step_seq": e.step_seq,
             "event": e.event, "operator": e.operator,
             "reason": e.reason, "detail": e.detail}
            for e in events
        ],
    }
