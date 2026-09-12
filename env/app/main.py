import os

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .db import Base, SessionLocal, engine
from .models import (
    REVIEW_STATUSES, AuditLog, MigrationBatch, MigrationPlan, RecordNew,
    RecordOld, ReplayArchive, ReplayBatchOp, ReplayCheckpoint, ReplayTask,
)
from .plans import PlanWorker
from .replay import ReplayWorker
from . import archives, plans, replay, service
from .schemas import (
    AdminAction, ArchiveAction, ArchiveCreate, BatchCreate, CheckpointCreate,
    PlanAction, PlanCreate, PlanRejectAction, PlanWindowAction, RecordIn,
    RecoverAction, ReplayAction, ReplayCreate, ReviewBatchAssign,
    ReviewBatchReview, ReviewConfirm, ReviewReopen, ReviewSubmit,
)

APP_VERSION = service.APP_VERSION
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
# 测试可通过 PLAN_WORKER_ENABLED=0 关闭自动线程, 手动 run_plan_tick 做确定性验证。
# 在 startup 时读取(而非 import 时), 便于测试进程内切换。
def _worker_enabled() -> bool:
    return os.getenv("PLAN_WORKER_ENABLED", "1") not in ("0", "false", "False")


app = FastAPI(title="结构迁移切换服务(按业务分组批次)", version=APP_VERSION)
worker = PlanWorker(poll_interval=float(os.getenv("PLAN_WORKER_POLL_INTERVAL", "0.5")))
replay_worker = ReplayWorker()
archive_worker = archives.ArchiveWorker()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _backfill_plan_columns():
    """旧库(无迁移框架)补齐审批/窗口列与窗口表: 已存在的计划按低风险、
    无需审批处理。幂等, 可重复执行。"""
    from sqlalchemy import inspect, text as _text
    inspector = inspect(engine)
    existing = {c["name"] for c in inspector.get_columns("migration_plans")}
    defaults = {
        "risk_level": "VARCHAR(8) NOT NULL DEFAULT 'LOW'",
        "approval_status": "VARCHAR(16) NOT NULL DEFAULT 'NOT_REQUIRED'",
        "approved_by": "VARCHAR(128)",
        "approved_at": "TIMESTAMP",
        "reject_reason": "VARCHAR(500)",
        "window_open": "BOOLEAN",
    }
    with engine.begin() as conn:
        for col, ddl in defaults.items():
            if col not in existing:
                conn.execute(_text(
                    f"ALTER TABLE migration_plans ADD COLUMN {col} {ddl}"))


def _backfill_replay_columns():
    """旧库(无迁移框架)补齐回放复核列: 已存在的回放按未复核、报告版本 1 处理。
    幂等, 可重复执行。"""
    from sqlalchemy import inspect, text as _text
    inspector = inspect(engine)
    existing = {c["name"] for c in inspector.get_columns("replay_tasks")}
    defaults = {
        "report_version": "INTEGER NOT NULL DEFAULT 1",
        "review_status": "VARCHAR(16) NOT NULL DEFAULT 'UNREVIEWED'",
        "confirmed_by": "VARCHAR(128)",
        "confirmed_at": "TIMESTAMP",
        "assignee": "VARCHAR(128)",
    }
    with engine.begin() as conn:
        for col, ddl in defaults.items():
            if col not in existing:
                conn.execute(_text(
                    f"ALTER TABLE replay_tasks ADD COLUMN {col} {ddl}"))


@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    _backfill_plan_columns()
    _backfill_replay_columns()
    db = SessionLocal()
    try:
        service.boot_check(db)
        db.commit()
        # 计划重启对账必须先于 worker: 把遗留 RUNNING 步骤复位, 计划才不会"假运行"
        plans.boot_recover_plans(db)
        db.commit()
        # 回放重启对账: 遗留 RUNNING 回放回到排队位置, 已完成步骤报告保留
        replay.boot_recover_replays(db)
        db.commit()
        # 归档重启对账: 遗留 RUNNING 归档回到排队位置, 已完成单元产物保留
        archives.boot_recover_archives(db)
        db.commit()
        archives.ensure_store_dir()
    finally:
        db.close()
    if _worker_enabled():
        worker.start()
        replay_worker.start()
        archive_worker.start()


@app.on_event("shutdown")
def shutdown():
    worker.stop()
    replay_worker.stop()
    archive_worker.stop()


# ---------- 错误映射 ----------

def _conflict(e: Exception, diffs=None):
    raise HTTPException(status_code=409, detail={"error": "conflict", "reason": str(e), "diffs": diffs or []})


def _not_found(e: Exception):
    raise HTTPException(status_code=404, detail={"error": "batch_not_found", "reason": str(e)})


# ---------- 状态、批次列表与审计 ----------

@app.get("/api/status")
def status(db: Session = Depends(get_db)):
    batches = (db.query(MigrationBatch)
               .order_by(MigrationBatch.created_at, MigrationBatch.id).all())
    plan_rows = (db.query(MigrationPlan)
                 .order_by(MigrationPlan.created_at, MigrationPlan.id).all())
    checkpoint_rows = (db.query(ReplayCheckpoint)
                       .order_by(ReplayCheckpoint.created_at, ReplayCheckpoint.id).all())
    replay_rows = (db.query(ReplayTask)
                   .order_by(ReplayTask.created_at, ReplayTask.id).all())
    archive_rows = (db.query(ReplayArchive)
                    .order_by(ReplayArchive.created_at, ReplayArchive.id).all())
    return {
        "app_version": APP_VERSION,
        "batches": [service.batch_to_dict(db, b) for b in batches],
        "plans": [plans.plan_to_dict(db, p) for p in plan_rows],
        "checkpoints": [replay.checkpoint_to_dict(c, with_steps=False)
                        for c in checkpoint_rows],
        "replays": [replay.replay_to_dict(db, t, with_report=False)
                    for t in replay_rows],
        "archives": [archives.archive_to_dict(db, a, with_events=False)
                     for a in archive_rows],
        "replay_concurrency": replay.max_concurrency(),
        "archive_concurrency": archives.max_concurrency(),
    }


@app.get("/api/admin/batches")
def list_batches(db: Session = Depends(get_db)):
    batches = (db.query(MigrationBatch)
               .order_by(MigrationBatch.created_at, MigrationBatch.id).all())
    return [service.batch_to_dict(db, b) for b in batches]


@app.get("/api/admin/batches/{batch_id}")
def get_batch(batch_id: str, db: Session = Depends(get_db)):
    try:
        batch = service.get_batch(db, batch_id)
    except service.BatchNotFound as e:
        _not_found(e)
    return service.batch_to_dict(db, batch)


@app.get("/api/admin/audit")
def audit_log(batch_id: str | None = None, plan_id: str | None = None,
              limit: int = 100, db: Session = Depends(get_db)):
    q = db.query(AuditLog)
    if batch_id:
        q = q.filter(AuditLog.batch_id == batch_id)
    if plan_id:
        q = q.filter(AuditLog.plan_id == plan_id)
    rows = q.order_by(AuditLog.id.desc()).limit(limit).all()
    return [
        {
            "id": r.id, "ts": r.ts.isoformat() if r.ts else None,
            "batch_id": r.batch_id, "plan_id": r.plan_id, "step_id": r.step_id,
            "operator": r.operator, "action": r.action,
            "from_phase": r.from_phase, "to_phase": r.to_phase, "epoch": r.epoch,
            "app_version": r.app_version, "freeze_version": r.freeze_version,
            "watermark": r.watermark, "reason": r.reason, "diffs": r.diffs,
        }
        for r in rows
    ]


# ---------- 批次创建与迁移动作(幂等 + 栅栏) ----------

def _run(db: Session, action: str, body: AdminAction, fn, batch_id: str | None = None):
    payload = body.model_dump()
    try:
        result, replayed = service.run_admin_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=payload,
            batch_id=batch_id, fn=fn)
    except service.BatchNotFound as e:
        db.rollback()
        _not_found(e)
    except service.EpochConflict as e:
        db.rollback()
        _conflict(e)
    except service.PhaseError as e:
        db.rollback()
        _conflict(e, e.diffs)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/batches", status_code=201)
def create_batch(body: BatchCreate, db: Session = Depends(get_db)):
    return _run(db, "create", body,
                lambda s, _b: service.do_create_batch(
                    s, _b, body.operator, body.biz, body.id_start, body.id_end))


@app.post("/api/admin/batches/{batch_id}/freeze")
def freeze(batch_id: str, body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "freeze", body,
                lambda s, b: service.do_freeze(s, b, body.operator), batch_id)


@app.post("/api/admin/batches/{batch_id}/validate")
def validate(batch_id: str, body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "validate", body,
                lambda s, b: service.do_validate(s, b, body.operator), batch_id)


@app.post("/api/admin/batches/{batch_id}/cutover")
def cutover(batch_id: str, body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "cutover", body,
                lambda s, b: service.do_cutover(s, b, body.operator, body.expected_epoch),
                batch_id)


@app.post("/api/admin/batches/{batch_id}/recover")
def recover(batch_id: str, body: RecoverAction, db: Session = Depends(get_db)):
    return _run(db, "recover", body,
                lambda s, b: service.do_recover(s, b, body.operator, body.reason), batch_id)


# ---------- 迁移计划编排(创建校验 / 启动 / 暂停 / 恢复 / 取消) ----------

def _plan_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={"error": "plan_not_found", "reason": str(e)})


def _plan_conflict(e: Exception, reasons=None):
    raise HTTPException(status_code=409, detail={
        "error": "plan_conflict", "reason": str(e), "reasons": reasons or []})


def _run_plan(db: Session, action: str, body: PlanAction, fn, plan_id: str | None = None):
    payload = body.model_dump()
    try:
        result, replayed = plans.run_plan_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=payload,
            plan_id=plan_id, fn=fn)
    except plans.PlanNotFound as e:
        db.rollback()
        _plan_not_found(e)
    except (plans.PlanStateError, plans.PlanValidationError) as e:
        db.rollback()
        _plan_conflict(e, getattr(e, "reasons", None))
    result["replayed"] = replayed
    return result


@app.post("/api/admin/plans", status_code=201)
def create_plan(body: PlanCreate, db: Session = Depends(get_db)):
    steps = [s.model_dump(exclude_none=True) for s in body.steps]
    windows = [w.model_dump() for w in body.windows]
    try:
        result, replayed = plans.run_plan_action(
            db, action="create", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            plan_id=None,
            fn=lambda s, _p: plans.do_create_plan(
                s, _p, body.operator, body.name, steps, body.max_retries,
                body.risk_level, windows))
    except plans.PlanValidationError as e:
        db.rollback()
        _plan_conflict(e, e.reasons)
    result["replayed"] = replayed
    return result


@app.get("/api/admin/plans")
def list_plans(db: Session = Depends(get_db)):
    rows = (db.query(MigrationPlan)
            .order_by(MigrationPlan.created_at, MigrationPlan.id).all())
    return [plans.plan_to_dict(db, p) for p in rows]


@app.get("/api/admin/plans/{plan_id}")
def get_plan(plan_id: str, db: Session = Depends(get_db)):
    try:
        plan = plans.get_plan(db, plan_id)
    except plans.PlanNotFound as e:
        _plan_not_found(e)
    return plans.plan_to_dict(db, plan)


@app.post("/api/admin/plans/{plan_id}/start")
def start_plan(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    return _run_plan(db, "start", body,
                     lambda s, p: plans.do_start(s, p, body.operator), plan_id)


@app.post("/api/admin/plans/{plan_id}/pause")
def pause_plan(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    return _run_plan(db, "pause", body,
                     lambda s, p: plans.do_pause(s, p, body.operator), plan_id)


@app.post("/api/admin/plans/{plan_id}/resume")
def resume_plan(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    return _run_plan(db, "resume", body,
                     lambda s, p: plans.do_resume(s, p, body.operator), plan_id)


@app.post("/api/admin/plans/{plan_id}/cancel")
def cancel_plan(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    return _run_plan(db, "cancel", body,
                     lambda s, p: plans.do_cancel(s, p, body.operator), plan_id)


# ---------- 风险审批(通过 / 拒绝 / 启动前撤销, 均幂等) ----------

@app.post("/api/admin/plans/{plan_id}/approve")
def approve_plan(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    """高风险计划审批通过: 操作者必须不是计划创建者。重复审批幂等。"""
    return _run_plan(db, "approve", body,
                     lambda s, p: plans.do_approve(s, p, body.operator), plan_id)


@app.post("/api/admin/plans/{plan_id}/reject")
def reject_plan(plan_id: str, body: PlanRejectAction, db: Session = Depends(get_db)):
    """拒绝高风险计划: 原因必填, 落审计并阻止启动。重复拒绝幂等。"""
    return _run_plan(db, "reject", body,
                     lambda s, p: plans.do_reject(s, p, body.operator, body.reason),
                     plan_id)


@app.post("/api/admin/plans/{plan_id}/revoke-approval")
def revoke_plan_approval(plan_id: str, body: PlanAction, db: Session = Depends(get_db)):
    """启动前撤销已通过的审批, 启动闸门重新关闭。重复撤销幂等。"""
    return _run_plan(db, "revoke-approval", body,
                     lambda s, p: plans.do_revoke_approval(s, p, body.operator),
                     plan_id)


# ---------- 执行窗口(仅启动前可整体替换/清空, 幂等) ----------

@app.put("/api/admin/plans/{plan_id}/window")
@app.post("/api/admin/plans/{plan_id}/window")
def update_plan_window(plan_id: str, body: PlanWindowAction,
                       db: Session = Depends(get_db)):
    """整体替换允许执行的时间窗口; 传空列表即清空窗口限制。"""
    windows = [w.model_dump() for w in body.windows]
    return _run_plan(db, "window", body,
                     lambda s, p: plans.do_update_window(s, p, body.operator, windows),
                     plan_id)


# ---------- 迁移回放与报告(只读重放: 不改批次/计划/业务记录) ----------

def _replay_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={"error": "replay_not_found", "reason": str(e)})


def _replay_conflict(e: Exception):
    raise HTTPException(status_code=409, detail={"error": "replay_conflict", "reason": str(e)})


def _run_replay(db: Session, action: str, body, fn, task_id: str | None = None):
    try:
        result, replayed = replay.run_replay_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id, fn=fn)
    except replay.ReplayNotFound as e:
        db.rollback()
        _replay_not_found(e)
    except replay.ReplayStateError as e:
        db.rollback()
        _replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/checkpoints", status_code=201)
def create_checkpoint(body: CheckpointCreate, db: Session = Depends(get_db)):
    """为已有迁移计划固化审计检查点与批次数据快照。计划不存在 -> 404;
    不完整的检查点同样持久化(status=INCOMPLETE), 但不能用于回放。"""
    return _run_replay(db, "checkpoint.create", body,
                       lambda s, _t: replay.do_create_checkpoint(s, _t, body.operator, body.plan_id))


@app.get("/api/admin/checkpoints")
def list_checkpoints(db: Session = Depends(get_db)):
    rows = (db.query(ReplayCheckpoint)
            .order_by(ReplayCheckpoint.created_at, ReplayCheckpoint.id).all())
    return [replay.checkpoint_to_dict(c, with_steps=False) for c in rows]


@app.get("/api/admin/checkpoints/{checkpoint_id}")
def get_checkpoint(checkpoint_id: str, db: Session = Depends(get_db)):
    cp = db.get(ReplayCheckpoint, checkpoint_id)
    if cp is None:
        _replay_not_found(Exception(f"审计检查点 {checkpoint_id} 不存在"))
    return replay.checkpoint_to_dict(cp)


@app.post("/api/admin/replays", status_code=201)
def create_replay(body: ReplayCreate, db: Session = Depends(get_db)):
    """选择已有计划 + 持久化检查点创建回放任务并排队。
    计划/检查点不存在 -> 404; 检查点不属于该计划或审计不完整 -> 409;
    同(计划,检查点)重复创建幂等返回已有任务(already_active=true)。"""
    return _run_replay(db, "create", body,
                       lambda s, _t: replay.do_create_replay(
                           s, _t, body.operator, body.plan_id, body.checkpoint_id))


@app.get("/api/admin/replays")
def list_replays(db: Session = Depends(get_db)):
    rows = (db.query(ReplayTask)
            .order_by(ReplayTask.created_at, ReplayTask.id).all())
    return [replay.replay_to_dict(db, t, with_report=False) for t in rows]


@app.get("/api/admin/replays/{replay_id}")
def get_replay(replay_id: str, db: Session = Depends(get_db)):
    """回放任务详情: 进度、当前步骤、差异数量、最近错误、步骤状态、事件流水。"""
    task = db.get(ReplayTask, replay_id)
    if task is None:
        _replay_not_found(Exception(f"回放任务 {replay_id} 不存在"))
    return replay.replay_to_dict(db, task, with_report=False)


@app.get("/api/admin/replays/{replay_id}/report")
def get_replay_report(replay_id: str, db: Session = Depends(get_db)):
    """回放报告详情: 每个步骤的预期状态、实际状态、字段差异与批次状态差异;
    已完成步骤的报告在任务 FAILED/CANCELED 后仍然保留。"""
    task = db.get(ReplayTask, replay_id)
    if task is None:
        _replay_not_found(Exception(f"回放任务 {replay_id} 不存在"))
    return replay.replay_to_dict(db, task, with_report=True)


@app.post("/api/admin/replays/{replay_id}/pause")
def pause_replay(replay_id: str, body: ReplayAction, db: Session = Depends(get_db)):
    """暂停回放: 在当前步骤边界停住(已完成报告保留); 重复暂停幂等。"""
    return _run_replay(db, "pause", body,
                       lambda s, t: replay.do_pause(s, t, body.operator), replay_id)


@app.post("/api/admin/replays/{replay_id}/resume")
def resume_replay(replay_id: str, body: ReplayAction, db: Session = Depends(get_db)):
    """恢复回放: 重新排队(再次受并发闸门约束), 从下一未完成步骤续跑; 重复恢复幂等。"""
    return _run_replay(db, "resume", body,
                       lambda s, t: replay.do_resume(s, t, body.operator), replay_id)


@app.post("/api/admin/replays/{replay_id}/cancel")
def cancel_replay(replay_id: str, body: ReplayAction, db: Session = Depends(get_db)):
    """取消回放: 未开始步骤置 SKIPPED, 已完成步骤报告保留; 重复取消幂等。"""
    return _run_replay(db, "cancel", body,
                       lambda s, t: replay.do_cancel(s, t, body.operator), replay_id)


# ---------- 回放报告复核(结论与报告版本绑定, 历史只追加) ----------

@app.post("/api/admin/replays/{replay_id}/reviews", status_code=201)
def submit_review(replay_id: str, body: ReviewSubmit, db: Session = Depends(get_db)):
    """提交单步复核结论(PASS/FAIL)、问题说明与修复标签。
    回放须 COMPLETED 且未确认; report_version 过期/步骤非 SUCCESS/同版本同步骤
    已有结论 -> 409, 不覆盖新结论; 同一请求重复提交幂等。"""
    return _run_replay(db, "review.submit", body,
                       lambda s, t: replay.do_submit_review(
                           s, t, body.operator, body.step_seq, body.report_version,
                           body.verdict, body.issue, body.fix_tags), replay_id)


@app.get("/api/admin/replays/{replay_id}/reviews")
def get_replay_reviews(replay_id: str, db: Session = Depends(get_db)):
    """复核记录: 当前版本逐步结论 + 全部历史版本结论 + 复核事件流水。"""
    task = db.get(ReplayTask, replay_id)
    if task is None:
        _replay_not_found(Exception(f"回放任务 {replay_id} 不存在"))
    return replay.reviews_view(db, task)


@app.post("/api/admin/replays/{replay_id}/review/confirm")
def confirm_review(replay_id: str, body: ReviewConfirm, db: Session = Depends(get_db)):
    """确认回放: 仅当前报告版本全部 SUCCESS 步骤均 PASS 才允许;
    报告版本过期/有待处理结论 -> 409; 重复确认幂等。"""
    return _run_replay(db, "review.confirm", body,
                       lambda s, t: replay.do_confirm_review(
                           s, t, body.operator, body.report_version), replay_id)


@app.post("/api/admin/replays/{replay_id}/review/reopen")
def reopen_review(replay_id: str, body: ReviewReopen, db: Session = Depends(get_db)):
    """重新打开待处理(PENDING)回放: 报告版本 +1 进入新一轮复核, 历史结论保留。"""
    return _run_replay(db, "review.reopen", body,
                       lambda s, t: replay.do_reopen_review(
                           s, t, body.operator, body.reason), replay_id)


@app.get("/api/admin/review-queue")
def get_review_queue(status: str = "PENDING", db: Session = Depends(get_db)):
    """按复核状态查询回放任务(待处理队列): 默认 PENDING(复核发现问题待处理)。"""
    if status not in REVIEW_STATUSES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_review_status",
            "reason": f"复核状态必须是 {list(REVIEW_STATUSES)} 之一(收到 {status!r})"})
    return replay.review_queue_view(db, status)


# ---------- 批量复核与分派(逐项独立提交, 结果持久化可查) ----------

@app.get("/api/admin/review-tasks")
def list_review_tasks(review_status: str | None = None, biz: str | None = None,
                      report_version: int | None = None,
                      db: Session = Depends(get_db)):
    """筛选进入复核流程(COMPLETED)的回放任务: 按复核状态、业务分组、报告版本过滤;
    返回带当前分派人与分派历史的任务列表, 供批量分派/批量复核选择。"""
    if review_status is not None and review_status not in REVIEW_STATUSES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_review_status",
            "reason": f"复核状态必须是 {list(REVIEW_STATUSES)} 之一(收到 {review_status!r})"})
    return replay.review_tasks_view(db, review_status, biz, report_version)


def _run_batch(db: Session, action: str, body, fn):
    try:
        result, replayed = replay.run_batch_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(), fn=fn)
    except replay.ReplayStateError as e:
        db.rollback()
        _replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/review-batch/assign", status_code=201)
def batch_assign(body: ReviewBatchAssign, db: Session = Depends(get_db)):
    """批量分派: 把一批回放任务分派给指定复核人。逐项独立提交——已确认/不存在/
    未完成的任务逐项返回失败原因, 成功项不回滚; 重复分派(同人)幂等, 改派保留历史。"""
    return _run_batch(db, "batch.assign", body,
                      lambda s: replay.do_batch_assign(
                          s, body.operator, body.assignee, body.replay_ids,
                          body.reason, body.idempotency_key))


@app.post("/api/admin/review-batch/reviews", status_code=201)
def batch_review(body: ReviewBatchReview, db: Session = Depends(get_db)):
    """批量提交复核结论: 所有项必须基于同一报告版本。逐项独立提交——版本已变化/
    已确认/无权限(任务分派给他人)等逐项返回失败原因, 成功项不回滚。"""
    items = [i.model_dump() for i in body.items]
    return _run_batch(db, "batch.review", body,
                      lambda s: replay.do_batch_review(
                          s, body.operator, body.report_version, items,
                          body.idempotency_key))


@app.get("/api/admin/review-batch")
def list_batch_ops(limit: int = 20, db: Session = Depends(get_db)):
    """最近批量操作列表(分派/复核, 含进度汇总; 逐项结果用详情接口查询)。"""
    rows = (db.query(ReplayBatchOp)
            .order_by(ReplayBatchOp.id.desc()).limit(limit).all())
    return [replay.batch_op_to_dict(op, with_results=False) for op in rows]


@app.get("/api/admin/review-batch/{op_id}")
def get_batch_op(op_id: str, db: Session = Depends(get_db)):
    """批量操作结果查询: 进度(总数/成功/失败)与逐项成功/失败原因, 重启后保留。"""
    op = db.get(ReplayBatchOp, op_id)
    if op is None:
        _replay_not_found(Exception(f"批量操作 {op_id} 不存在"))
    return replay.batch_op_to_dict(op)


# ---------- 回放证据归档(不可变归档包: 版本/步骤报告/复核结论/分派历史/审计摘要 + 摘要) ----------

def _archive_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={
        "error": "archive_not_found", "reason": str(e)})


def _archive_conflict(e: Exception):
    raise HTTPException(status_code=409, detail={
        "error": "archive_conflict", "reason": str(e)})


def _run_archive(db: Session, action: str, body, fn, archive_id: str | None = None):
    try:
        result, replayed = archives.run_archive_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            archive_id=archive_id, fn=fn)
    except archives.ArchiveNotFound as e:
        db.rollback()
        _archive_not_found(e)
    except archives.ArchiveStateError as e:
        db.rollback()
        _archive_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/archives", status_code=201)
def create_archive(body: ArchiveCreate, db: Session = Depends(get_db)):
    """为已 COMPLETED 回放按指定报告版本生成不可变证据归档并排队。
    回放不存在 -> 404; 回放未完成/指定版本与当前报告版本冲突 -> 409;
    同一(回放,版本)重复归档幂等返回已有归档(活动中/已完成)。"""
    return _run_archive(db, "create", body,
                       lambda s, _a: archives.do_create_archive(
                           s, _a, body.operator, body.replay_id, body.report_version))


@app.get("/api/admin/archives")
def list_archives(db: Session = Depends(get_db)):
    """归档任务列表(含排队/执行中/暂停/失败记录/已完成归档, 重启后仍可查询)。"""
    rows = (db.query(ReplayArchive)
            .order_by(ReplayArchive.created_at, ReplayArchive.id).all())
    return [archives.archive_to_dict(db, a, with_events=False) for a in rows]


@app.get("/api/admin/archives/{archive_id}")
def get_archive(archive_id: str, db: Session = Depends(get_db)):
    """归档详情: 进度、当前归档单元、失败原因、摘要、清单与事件流水。"""
    a = db.get(ReplayArchive, archive_id)
    if a is None:
        _archive_not_found(Exception(f"归档任务 {archive_id} 不存在"))
    return archives.archive_to_dict(db, a)


@app.post("/api/admin/archives/{archive_id}/pause")
def pause_archive(archive_id: str, body: ArchiveAction, db: Session = Depends(get_db)):
    """暂停归档: 在当前归档单元边界停住; 重复暂停幂等。"""
    return _run_archive(db, "pause", body,
                       lambda s, a: archives.do_pause(s, a, body.operator), archive_id)


@app.post("/api/admin/archives/{archive_id}/resume")
def resume_archive(archive_id: str, body: ArchiveAction, db: Session = Depends(get_db)):
    """恢复归档: 重新排队(再次受并发闸门约束), 从首个未完成单元续跑; 重复恢复幂等。"""
    return _run_archive(db, "resume", body,
                       lambda s, a: archives.do_resume(s, a, body.operator), archive_id)


@app.post("/api/admin/archives/{archive_id}/cancel")
def cancel_archive(archive_id: str, body: ArchiveAction, db: Session = Depends(get_db)):
    """取消归档: 不生成归档包, 已采集单元与任务记录保留; 重复取消幂等。"""
    return _run_archive(db, "cancel", body,
                       lambda s, a: archives.do_cancel(s, a, body.operator), archive_id)


@app.get("/api/admin/archives/{archive_id}/download")
def download_archive(archive_id: str, operator: str = "system",
                     db: Session = Depends(get_db)):
    """下载不可变归档包(zip)。包不存在(被外部删除) -> 404 并记录失败。"""
    a = db.get(ReplayArchive, archive_id)
    if a is None:
        _archive_not_found(Exception(f"归档任务 {archive_id} 不存在"))
    try:
        path = archives.package_path_for_download(db, a, operator)
    except archives.ArchiveNotFound as e:
        db.rollback()
        _archive_not_found(e)
    except archives.ArchiveStateError as e:
        db.rollback()
        _archive_conflict(e)
    return FileResponse(
        path, media_type="application/zip",
        filename=f"replay-archive-{a.id}-v{a.report_version}.zip")


@app.get("/api/admin/archives/{archive_id}/verify")
def verify_archive(archive_id: str, operator: str = "system",
                   db: Session = Depends(get_db)):
    """重算归档包逐文件摘要与内容摘要并与记录比对; 摘要不一致/包缺失时
    归档明确置为 FAILED 并保留失败记录, 返回中 valid=false 与逐项原因。"""
    a = db.get(ReplayArchive, archive_id)
    if a is None:
        _archive_not_found(Exception(f"归档任务 {archive_id} 不存在"))
    try:
        return archives.verify_archive(db, a, operator)
    except archives.ArchiveStateError as e:
        db.rollback()
        _archive_conflict(e)


# ---------- 记录读写(按所属批次的闸门 + 双读) ----------

def _reject_writes(batch: MigrationBatch):
    raise HTTPException(status_code=423, detail={
        "error": "writes_rejected",
        "reason": f"批次 {batch.id}({batch.biz}) 迁移冻结中, 范围内写入被拒绝 "
                  f"(freeze_version={batch.freeze_version})",
        "batch_id": batch.id,
        "phase": batch.phase,
    })


@app.post("/api/records", status_code=201)
def create_old(rec: RecordIn, db: Session = Depends(get_db)):
    """旧结构写入路径。所属批次冻结期拒绝并说明原因; 批次切换后明确失败。
    批次外记录不受影响, 正常写入。"""
    batch = service.batch_for_record(db, rec.id)
    if batch is not None:
        if batch.phase == "DONE":
            raise HTTPException(status_code=410, detail={
                "error": "old_path_retired",
                "reason": f"记录所属批次 {batch.id} 已切换, 旧写入路径已关闭, 请使用 /api/v2/records",
                "batch_id": batch.id,
            })
        if batch.phase != "NORMAL":
            _reject_writes(batch)
    row = RecordOld(id=rec.id, name=rec.name, email=rec.email, tags_csv=rec.tags_csv)
    db.merge(row)
    db.commit()
    return {"ok": True, "structure": "old", "id": rec.id,
            "batch_id": batch.id if batch else None}


@app.post("/api/v2/records", status_code=201)
def create_new(rec: RecordIn, db: Session = Depends(get_db)):
    """新结构写入路径。仅对所属批次已切换(DONE)的记录开放。"""
    batch = service.batch_for_record(db, rec.id)
    if batch is None or batch.phase != "DONE":
        raise HTTPException(status_code=409, detail={
            "error": "new_path_inactive",
            "reason": (f"记录所属批次 {batch.id} 尚未切换(当前阶段 {batch.phase})"
                       if batch is not None else "记录不在任何已切换批次的范围内"),
            "batch_id": batch.id if batch else None,
        })
    row = RecordNew(id=rec.id, name=rec.name, email=rec.email,
                    tags=rec.tags or [], schema_version=2)
    db.merge(row)
    db.commit()
    return {"ok": True, "structure": "new", "id": rec.id, "batch_id": batch.id}


@app.get("/api/records/{record_id}")
def read_record(record_id: int, db: Session = Depends(get_db)):
    """批次外或 NORMAL 读旧, 所属批次 DONE 读新;
    批次冻结窗内同一编号同时返回新旧两份并给出差异。"""
    batch = service.batch_for_record(db, record_id)
    if batch is not None and batch.phase == "DONE":
        new = db.get(RecordNew, record_id)
        if new is None:
            raise HTTPException(404, "记录不存在")
        return {"source": "new", "batch_id": batch.id, "record": service.new_to_dict(new)}
    old = db.get(RecordOld, record_id)
    if old is None:
        raise HTTPException(404, "记录不存在")
    if batch is None or batch.phase == "NORMAL":
        return {"source": "old", "batch_id": batch.id if batch else None,
                "record": service.old_to_dict(old)}
    new = db.get(RecordNew, record_id)
    expected = service.transform(old)
    diffs = ([] if new is not None
             else [{"record_id": record_id, "field": "__missing__", "old": expected, "new": None}])
    if new is not None:
        diffs = service.diff_records(expected, service.new_to_dict(new), record_id)
    return {
        "source": "dual",
        "batch_id": batch.id,
        "old": service.old_to_dict(old),
        "new": service.new_to_dict(new) if new else None,
        "diff": diffs,
        "consistent": not diffs,
    }


@app.get("/api/records/{record_id}/compare")
def compare_record(record_id: int, db: Session = Depends(get_db)):
    batch = service.batch_for_record(db, record_id)
    if batch is None or batch.phase in ("NORMAL", "DONE"):
        raise HTTPException(409, detail={"error": "not_in_freeze_window",
                                         "reason": "仅批次冻结窗内提供双读比对"})
    return read_record(record_id, db)


# ---------- 管理页面 ----------

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
