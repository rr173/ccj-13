import os
from datetime import datetime

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .db import Base, SessionLocal, engine
from .models import (
    REVIEW_STATUSES, ArchiveCleanupPlan, AuditLog, AuditSnapshot,
    CompensationTask, MigrationBatch, MigrationPlan, QUALITY_ISSUE_STATUSES,
    QUALITY_SCAN_STATUSES, QualityScan, RecordNew, RecordOld, ReplayArchive,
    ReplayBatchOp, ReplayCheckpoint, ReplayTask,
)
from .plans import PlanWorker
from .quality import QualityWorker
from .replay import ReplayWorker
from . import archives, auditreplay, cleanup, plans, quality, replay, service
from .schemas import (
    AdminAction, ArchiveAction, ArchiveCreate, ArchiveRetention,
    AuditEventNote, AuditSnapshotCreate, BatchCreate, CheckpointCreate, CleanupAction,
    CleanupCreate, CompensationAction, CompensationApprovalAction,
    CompensationCancelAction, CompensationCreate, CompensationRejectAction,
    CompensationRetry, CompensationWindowAction, PlanAction, PlanCreate,
    PlanRejectAction, PlanWindowAction, QualityExemptionCreate,
    QualityExemptionRevoke, QualityFixCreate, QualityRulesSave, QualityScanAction,
    QualityScanCreate, RecordIn, RecoverAction, ReplayAction, ReplayCreate,
    ReviewBatchAssign, ReviewBatchReview, ReviewConfirm, ReviewReopen,
    ReviewSubmit,
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
cleanup_worker = cleanup.CleanupWorker()
quality_worker = QualityWorker()
compensation_worker = auditreplay.CompensationWorker()


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
        "quality_hold": "BOOLEAN NOT NULL DEFAULT 0",
    }
    with engine.begin() as conn:
        for col, ddl in defaults.items():
            if col not in existing:
                conn.execute(_text(
                    f"ALTER TABLE migration_plans ADD COLUMN {col} {ddl}"))


def _backfill_quality_rescan_columns():
    """旧库补齐自动重扫编排列(quality_scans)与质量暂停表:
    已有扫描按手动扫描、无触发来源处理。幂等, 可重复执行。"""
    from sqlalchemy import inspect, text as _text
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "quality_scans" in tables:
        existing = {c["name"] for c in inspector.get_columns("quality_scans")}
        defaults = {
            "scan_source": "VARCHAR(16) NOT NULL DEFAULT 'manual'",
            "trigger_source": "VARCHAR(32)",
            "triggers": "JSON",
            "supersedes_scan_id": "VARCHAR(32)",
            "affected_batch_ids": "JSON",
        }
        with engine.begin() as conn:
            for col, ddl in defaults.items():
                if col not in existing:
                    conn.execute(_text(
                        f"ALTER TABLE quality_scans ADD COLUMN {col} {ddl}"))
        # 活动扫描按计划唯一的部分唯一索引(数据库层兜底, 重复/并发触发不产生重复任务;
        # Postgres/SQLite 均支持 WHERE 条件的唯一索引)
        with engine.begin() as conn:
            conn.execute(_text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_quality_scan_active_plan "
                "ON quality_scans(plan_id) "
                "WHERE status IN ('QUEUED','RUNNING','PAUSED')"))


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


def _backfill_archive_columns():
    """旧库补齐归档目录/保留策略/并发协调列: 已存在归档按无保留策略、
    使用计数 0、未清理处理。幂等, 可重复执行。"""
    from sqlalchemy import inspect, text as _text
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "replay_archives" not in tables:
        return
    existing = {c["name"] for c in inspector.get_columns("replay_archives")}
    defaults = {
        "biz_groups": "JSON",
        "retention_mode": "VARCHAR(16) NOT NULL DEFAULT 'NONE'",
        "retain_until": "TIMESTAMP",
        "retention_set_by": "VARCHAR(128)",
        "retention_set_at": "TIMESTAMP",
        "active_downloads": "INTEGER NOT NULL DEFAULT 0",
        "active_verifies": "INTEGER NOT NULL DEFAULT 0",
        "cleaned_at": "TIMESTAMP",
        "cleaned_by": "VARCHAR(128)",
        "cleanup_plan_id": "VARCHAR(32)",
    }
    with engine.begin() as conn:
        for col, ddl in defaults.items():
            if col not in existing:
                conn.execute(_text(
                    f"ALTER TABLE replay_archives ADD COLUMN {col} {ddl}"))


def _backfill_compensation_columns():
    """旧库补齐补偿审批/执行窗口列: 已存在补偿任务按低风险、免审批、
    无窗口限制处理(与历史行为一致)。幂等, 可重复执行。"""
    from sqlalchemy import inspect, text as _text
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "compensation_tasks" not in tables:
        return
    existing = {c["name"] for c in inspector.get_columns("compensation_tasks")}
    defaults = {
        "risk_level": "VARCHAR(8) NOT NULL DEFAULT 'LOW'",
        "approval_status": "VARCHAR(16) NOT NULL DEFAULT 'NOT_REQUIRED'",
        "approval_round": "INTEGER NOT NULL DEFAULT 0",
        "reject_reason": "VARCHAR(500)",
        "approval_basis": "JSON",
        "canceled_by": "VARCHAR(128)",
        "canceled_at": "TIMESTAMP",
        "cancel_reason": "VARCHAR(500)",
        "window_open": "BOOLEAN",
        "window_pause_reason": "VARCHAR(500)",
    }
    with engine.begin() as conn:
        for col, ddl in defaults.items():
            if col not in existing:
                conn.execute(_text(
                    f"ALTER TABLE compensation_tasks ADD COLUMN {col} {ddl}"))


@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    _backfill_plan_columns()
    _backfill_replay_columns()
    _backfill_archive_columns()
    _backfill_quality_rescan_columns()
    _backfill_compensation_columns()
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
        # 归档重启对账: 遗留 RUNNING 归档回到排队位置, 已完成单元产物保留;
        # 清零在途下载/校验计数(进程死亡意味着在途请求已不存在)
        archives.boot_recover_archives(db)
        db.commit()
        # 清理计划重启对账: 遗留 RUNNING 计划回到排队, 逐项进度与跳过原因保留
        cleanup.boot_recover_cleanup_plans(db)
        db.commit()
        # 质量扫描重启对账: 遗留 RUNNING 扫描回到排队, RUNNING 批次复位 PENDING
        quality.boot_recover_scans(db)
        db.commit()
        # 补偿任务重启对账: 遗留 RUNNING 回排队, UNDO_RUNNING 回 UNDO_PARTIAL,
        # 动作进度/失败原因/撤销镜像全部保留, worker 从安全位置续跑
        auditreplay.boot_recover_compensation(db)
        db.commit()
        archives.ensure_store_dir()
    finally:
        db.close()
    if _worker_enabled():
        worker.start()
        replay_worker.start()
        archive_worker.start()
        cleanup_worker.start()
        quality_worker.start()
        compensation_worker.start()


@app.on_event("shutdown")
def shutdown():
    worker.stop()
    replay_worker.stop()
    archive_worker.stop()
    cleanup_worker.stop()
    quality_worker.stop()
    compensation_worker.stop()


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
    cleanup_rows = (db.query(ArchiveCleanupPlan)
                    .order_by(ArchiveCleanupPlan.created_at,
                              ArchiveCleanupPlan.id).all())
    return {
        "app_version": APP_VERSION,
        "batches": [service.batch_to_dict(db, b) for b in batches],
        "plans": [plans.plan_to_dict(db, p) for p in plan_rows],
        "checkpoints": [replay.checkpoint_to_dict(c, with_steps=False)
                        for c in checkpoint_rows],
        "replays": [replay.replay_to_dict(db, t, with_report=False)
                    for t in replay_rows],
        "archives": [archives.archive_to_dict(db, a, with_events=False)
                     for a in archive_rows if a.cleaned_at is None],
        "cleanup_plans": [cleanup.plan_to_dict(db, p, with_items=False)
                          for p in cleanup_rows],
        "quality": quality.quality_status_overview(db),
        "audit_snapshots": [auditreplay.snapshot_to_dict(s) for s in
                            (db.query(AuditSnapshot)
                             .order_by(AuditSnapshot.created_at.desc(),
                                       AuditSnapshot.id.desc()).limit(20).all())],
        "compensations": [auditreplay.task_to_dict(t, with_actions=False,
                                                   with_events=False)
                          for t in (db.query(CompensationTask)
                                    .order_by(CompensationTask.created_at.desc(),
                                              CompensationTask.id.desc())
                                    .limit(20).all())],
        "compensation_concurrency": auditreplay.max_concurrency(),
        "replay_concurrency": replay.max_concurrency(),
        "archive_concurrency": archives.max_concurrency(),
        "quality_concurrency": quality.max_concurrency(),
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


# ---------- 迁移前数据质量门禁(规则版本化 / 扫描任务 / 修复豁免 / 启动门禁) ----------

def _quality_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={
        "error": "quality_not_found", "reason": str(e)})


def _quality_conflict(e: Exception, reasons=None):
    raise HTTPException(status_code=409, detail={
        "error": "quality_conflict", "reason": str(e), "reasons": reasons or []})


def _run_quality(db: Session, action: str, body, fn, plan_id: str | None = None,
                 scan_id: str | None = None):
    try:
        result, replayed = quality.run_quality_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            plan_id=plan_id, scan_id=scan_id, fn=fn)
    except quality.QualityNotFound as e:
        db.rollback()
        _quality_not_found(e)
    except quality.QualityStateError as e:
        db.rollback()
        _quality_conflict(e)
    result["replayed"] = replayed
    return result


@app.put("/api/admin/plans/{plan_id}/quality-rules")
@app.post("/api/admin/plans/{plan_id}/quality-rules")
def save_quality_rules(plan_id: str, body: QualityRulesSave,
                       db: Session = Depends(get_db)):
    """为计划绑定/更新可版本化数据质量规则。内容摘要变化才产生新版本,
    相同内容重复保存幂等无副作用; 规则非法时聚合返回全部原因, 不落任何数据。"""
    rules = [r.model_dump(exclude_none=True) for r in body.rules]
    try:
        result, replayed = quality.run_quality_action(
            db, action="rules.save", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            plan_id=plan_id,
            fn=lambda s: quality.do_save_rules(
                s, None, body.operator, plan_id, rules, body.note))
    except quality.QualityNotFound as e:
        db.rollback()
        _quality_not_found(e)
    except quality.QualityValidationError as e:
        db.rollback()
        _quality_conflict(e, e.reasons)
    except quality.QualityStateError as e:
        db.rollback()
        _quality_conflict(e)
    result["replayed"] = replayed
    return result


@app.get("/api/admin/plans/{plan_id}/quality-rules")
def get_quality_rules(plan_id: str, db: Session = Depends(get_db)):
    """计划的规则集: 当前版本规则 + 全部历史版本(只追加, 永不修改)。"""
    try:
        quality.get_plan(db, plan_id)
        rs = quality.get_ruleset(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)
    return quality.ruleset_to_dict(db, rs)


@app.post("/api/admin/plans/{plan_id}/quality-scans", status_code=201)
def create_quality_scan(plan_id: str, body: QualityScanCreate,
                        db: Session = Depends(get_db)):
    """对计划涉及批次生成质量扫描任务并排队(基于当前规则版本)。
    未绑定规则/计划已启动 -> 409; 已有活动扫描时重复创建幂等返回已有任务。"""
    return _run_quality(db, "scan.create", body,
                        lambda s: quality.do_create_scan(
                            s, None, body.operator, plan_id),
                        plan_id=plan_id)


@app.get("/api/admin/plans/{plan_id}/quality-gate")
def get_quality_gate(plan_id: str, db: Session = Depends(get_db)):
    """质量门禁结果: 规则版本、最新扫描、过期/漂移原因、问题计数与是否放行。"""
    try:
        return quality.evaluate_gate(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)


@app.get("/api/admin/plans/{plan_id}/quality-overview")
def get_plan_quality_overview(plan_id: str, db: Session = Depends(get_db)):
    """计划质量总览: 规则版本、扫描列表(进度/问题分布)、门禁状态。"""
    try:
        return quality.plan_quality_view(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)


@app.get("/api/admin/plans/{plan_id}/quality-holds")
def get_quality_holds(plan_id: str, db: Session = Depends(get_db)):
    """质量门禁暂停与自动恢复历史: 当前 ACTIVE 暂停(触发来源/暂停原因/
    关联的唯一自动重扫) + 全量恢复/取消历史。"""
    try:
        return quality.holds_view(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)


@app.post("/api/admin/quality-rescan/sweep")
def run_quality_rescan_sweep(db: Session = Depends(get_db)):
    """手动触发一次失效兜底扫描(与 worker 每 tick 的动作一致):
    为结果过期/版本变化等执行态计划编排重扫并复评暂停, 返回每个计划的编排结果。
    主要用于运维/测试在关闭后台 worker 时确定性驱动。"""
    return {"results": quality.sweep_due_rescans(db)}


@app.get("/api/admin/plans/{plan_id}/quality-issues")
def list_quality_issues(plan_id: str, scan_id: str | None = None,
                        batch_id: str | None = None, severity: str | None = None,
                        status_filter: str | None = None,
                        rule_type: str | None = None,
                        db: Session = Depends(get_db)):
    """按计划查询质量问题(可按扫描/批次/严重级别/处理状态/规则类型过滤)。"""
    if severity is not None and severity not in quality.QUALITY_SEVERITIES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_severity",
            "reason": f"严重级别必须是 {list(quality.QUALITY_SEVERITIES)} 之一"})
    if status_filter is not None and status_filter not in QUALITY_ISSUE_STATUSES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_issue_status",
            "reason": f"问题状态必须是 {list(QUALITY_ISSUE_STATUSES)} 之一"})
    if rule_type is not None and rule_type not in quality.QUALITY_RULE_TYPES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_rule_type",
            "reason": f"规则类型必须是 {list(quality.QUALITY_RULE_TYPES)} 之一"})
    try:
        quality.get_plan(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)
    return quality.issues_view(
        db, plan_id, scan_id=scan_id, batch_id=batch_id, severity=severity,
        status=status_filter, rule_type=rule_type)


@app.get("/api/admin/plans/{plan_id}/quality-history")
def get_quality_history(plan_id: str, db: Session = Depends(get_db)):
    """修复批次与豁免的全量历史(只追加, 均绑定规则版本)。"""
    try:
        quality.get_plan(db, plan_id)
    except quality.QualityNotFound as e:
        _quality_not_found(e)
    return quality.history_view(db, plan_id)


@app.get("/api/admin/quality-scans")
def list_quality_scans(plan_id: str | None = None,
                       status_filter: str | None = None,
                       db: Session = Depends(get_db)):
    """质量扫描任务列表(可按计划/状态过滤)。"""
    q = db.query(QualityScan)
    if plan_id:
        q = q.filter(QualityScan.plan_id == plan_id)
    if status_filter:
        if status_filter not in QUALITY_SCAN_STATUSES:
            raise HTTPException(status_code=422, detail={
                "error": "invalid_scan_status",
                "reason": f"扫描状态必须是 {list(QUALITY_SCAN_STATUSES)} 之一"})
        q = q.filter(QualityScan.status == status_filter)
    rows = q.order_by(QualityScan.created_at.desc(), QualityScan.id).limit(200).all()
    return [quality.scan_to_dict(db, s, with_issues=False, with_events=False)
            for s in rows]


@app.get("/api/admin/quality-scans/{scan_id}")
def get_quality_scan(scan_id: str, db: Session = Depends(get_db)):
    """扫描详情: 批次进度、问题分布(按严重级别×状态)、过期原因与事件流水。"""
    scan = db.get(QualityScan, scan_id)
    if scan is None:
        _quality_not_found(Exception(f"质量扫描任务 {scan_id} 不存在"))
    return quality.scan_to_dict(db, scan)


@app.get("/api/admin/quality-scans/{scan_id}/issues")
def get_quality_scan_issues(scan_id: str, severity: str | None = None,
                            status_filter: str | None = None,
                            db: Session = Depends(get_db)):
    """扫描问题明细(含可追踪样本快照)。"""
    scan = db.get(QualityScan, scan_id)
    if scan is None:
        _quality_not_found(Exception(f"质量扫描任务 {scan_id} 不存在"))
    return quality.issues_view(
        db, scan.plan_id, scan_id=scan_id, severity=severity,
        status=status_filter)


@app.post("/api/admin/quality-scans/{scan_id}/pause")
def pause_quality_scan(scan_id: str, body: QualityScanAction,
                       db: Session = Depends(get_db)):
    """暂停扫描: 在当前批次边界停住(已完成批次问题保留); 重复暂停幂等。"""
    scan = db.get(QualityScan, scan_id)
    if scan is None:
        _quality_not_found(Exception(f"质量扫描任务 {scan_id} 不存在"))
    return _run_quality(db, "scan.pause", body,
                        lambda s: quality.do_pause_scan(s, scan, body.operator),
                        plan_id=scan.plan_id, scan_id=scan_id)


@app.post("/api/admin/quality-scans/{scan_id}/resume")
def resume_quality_scan(scan_id: str, body: QualityScanAction,
                        db: Session = Depends(get_db)):
    """恢复扫描: 重新排队(再次受并发闸门约束); FAILED 可恢复, 失败批次重试。"""
    scan = db.get(QualityScan, scan_id)
    if scan is None:
        _quality_not_found(Exception(f"质量扫描任务 {scan_id} 不存在"))
    return _run_quality(db, "scan.resume", body,
                        lambda s: quality.do_resume_scan(s, scan, body.operator),
                        plan_id=scan.plan_id, scan_id=scan_id)


@app.post("/api/admin/quality-scans/{scan_id}/cancel")
def cancel_quality_scan(scan_id: str, body: QualityScanAction,
                        db: Session = Depends(get_db)):
    """取消扫描: 未开始批次跳过, 已完成批次问题保留; 重复取消幂等。"""
    scan = db.get(QualityScan, scan_id)
    if scan is None:
        _quality_not_found(Exception(f"质量扫描任务 {scan_id} 不存在"))
    return _run_quality(db, "scan.cancel", body,
                        lambda s: quality.do_cancel_scan(s, scan, body.operator),
                        plan_id=scan.plan_id, scan_id=scan_id)


@app.post("/api/admin/plans/{plan_id}/quality-fixes", status_code=201)
def create_quality_fix(plan_id: str, body: QualityFixCreate,
                       db: Session = Depends(get_db)):
    """创建修复批次: 逐项在当前数据上重跑规则核验, 违规消失才置问题 FIXED。
    逐项给结论(RESOLVED/STILL_OPEN/NOT_FOUND/REJECTED), 修复批次只追加并绑定规则版本。"""
    return _run_quality(db, "fix.create", body,
                        lambda s: quality.do_create_fix(
                            s, None, body.operator, plan_id, body.issue_ids,
                            body.note), plan_id=plan_id)


@app.post("/api/admin/plans/{plan_id}/quality-exemptions", status_code=201)
def create_quality_exemption(plan_id: str, body: QualityExemptionCreate,
                             db: Session = Depends(get_db)):
    """为阻断问题提交带原因的豁免申请(提交即批准), 绑定规则版本; 重复豁免幂等。"""
    return _run_quality(db, "exemption.create", body,
                        lambda s: quality.do_create_exemption(
                            s, None, body.operator, plan_id, body.issue_id,
                            body.reason), plan_id=plan_id)


@app.post("/api/admin/plans/{plan_id}/quality-exemptions/{exemption_id}/revoke")
def revoke_quality_exemption(plan_id: str, exemption_id: str,
                             body: QualityExemptionRevoke,
                             db: Session = Depends(get_db)):
    """撤销豁免(历史保留): 对应阻断问题重新打开, 门禁重新要求处理。"""
    return _run_quality(db, "exemption.revoke", body,
                        lambda s: quality.do_revoke_exemption(
                            s, None, body.operator, plan_id, exemption_id,
                            body.reason), plan_id=plan_id)


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
def list_archives(replay_id: str | None = None, biz: str | None = None,
                  report_version: int | None = None,
                  content_digest: str | None = None,
                  status: str | None = None, retention: str | None = None,
                  include_cleaned: bool = False,
                  db: Session = Depends(get_db)):
    """归档目录检索(多维): 按回放、业务分组、报告版本、内容摘要、归档状态、
    保留状态过滤已完成/进行中的归档; 默认只返回未清理的存活归档
    (include_cleaned=true 可查已清理记录)。"""
    if retention is not None and retention not in archives.RETENTION_MODES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_retention_mode",
            "reason": f"保留模式必须是 {list(archives.RETENTION_MODES)} 之一"
                      f"(收到 {retention!r})"})
    rows = archives.search_archives(
        db, replay_id=replay_id, biz=biz, report_version=report_version,
        content_digest=content_digest, status=status, retention=retention,
        include_cleaned=include_cleaned)
    return [archives.archive_to_dict(db, a, with_events=False) for a in rows]


@app.get("/api/admin/archives/by-digest/{content_digest}")
def archives_by_digest(content_digest: str, db: Session = Depends(get_db)):
    """按内容摘要查询归档: 完整 sha256 精确匹配, >=12 位前缀消歧;
    返回同摘要的存活归档列表与引用关系(去重组)。"""
    rows = archives.find_by_digest(db, content_digest)
    groups = {}
    for a in rows:
        groups.setdefault(a.content_digest,
                          archives.digest_group_view(db, a.content_digest))
    return {"query": content_digest, "count": len(rows),
            "archives": [archives.archive_to_dict(db, a, with_events=False)
                         for a in rows],
            "digest_groups": list(groups.values())}


@app.get("/api/admin/archives/{archive_id}")
def get_archive(archive_id: str, db: Session = Depends(get_db)):
    """归档详情: 进度、当前归档单元、失败原因、摘要、保留策略、引用数、
    清理状态、清单与事件流水。"""
    a = db.get(ReplayArchive, archive_id)
    if a is None:
        _archive_not_found(Exception(f"归档任务 {archive_id} 不存在"))
    return archives.archive_to_dict(db, a)


@app.put("/api/admin/archives/{archive_id}/retention")
@app.post("/api/admin/archives/{archive_id}/retention")
def set_archive_retention(archive_id: str, body: ArchiveRetention,
                          db: Session = Depends(get_db)):
    """设置归档保留策略: UNTIL(带到期时间, 到期前不可清理)/PERMANENT(永久保留
    标记)/NONE(清除)。策略持久化, 服务重启不丢失; 同键重放幂等。"""
    a = db.get(ReplayArchive, archive_id)
    if a is None:
        _archive_not_found(Exception(f"归档任务 {archive_id} 不存在"))
    payload = body.model_dump()
    try:
        result, replayed = archives.run_archive_action(
            db, action="retention", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=payload,
            archive_id=archive_id,
            fn=lambda s, _a: archives.set_retention(
                s, a, body.operator, body.mode, body.retain_until))
    except archives.ArchiveStateError as e:
        db.rollback()
        _archive_conflict(e)
    result["replayed"] = replayed
    return result


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
def download_archive(archive_id: str, background_tasks: BackgroundTasks,
                     operator: str = "system", db: Session = Depends(get_db)):
    """下载不可变归档包(zip)。包不存在(被外部删除) -> 404 并记录失败。
    下载期间持有使用计数(active_downloads+1), 响应发送完毕后在后台任务释放;
    清理计划看到在途下载会逐项跳过(in_use_download), 不会删除文件。"""
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
    # 响应发送后才释放下载计数, 与清理并发协调
    background_tasks.add_task(_release_archive_use, archive_id, "download")
    return FileResponse(
        path, media_type="application/zip",
        filename=f"replay-archive-{a.id}-v{a.report_version}.zip")


def _release_archive_use(archive_id: str, kind: str):
    db = SessionLocal()
    try:
        archives.end_archive_use(db, archive_id, kind)
    finally:
        db.close()


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


# ---------- 归档清理计划(保留策略 / 下载校验占用 / 引用关系逐项判定) ----------

def _cleanup_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={
        "error": "cleanup_plan_not_found", "reason": str(e)})


def _cleanup_conflict(e: Exception):
    raise HTTPException(status_code=409, detail={
        "error": "cleanup_conflict", "reason": str(e)})


def _run_cleanup(db: Session, action: str, body, fn, plan_id: str | None = None):
    try:
        result, replayed = cleanup.run_cleanup_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            plan_id=plan_id, fn=fn)
    except cleanup.CleanupNotFound as e:
        db.rollback()
        _cleanup_not_found(e)
    except cleanup.CleanupStateError as e:
        db.rollback()
        _cleanup_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/archive-cleanups", status_code=201)
def create_cleanup_plan(body: CleanupCreate, db: Session = Depends(get_db)):
    """发起归档清理计划并排队: 逐项判定保留策略(保留期内/永久保留跳过)、
    下载/摘要校验占用(在途跳过)、同摘要引用关系(仍被引用不删除物理文件);
    逐项结果与跳过原因落库可查询, 服务重启后保留。"""
    return _run_cleanup(db, "create", body,
                        lambda s, _p: cleanup.do_create_plan(
                            s, _p, body.operator, body.archive_ids))


@app.get("/api/admin/archive-cleanups")
def list_cleanup_plans(limit: int = 50, db: Session = Depends(get_db)):
    """清理计划列表(排队/执行/暂停/取消/完成/失败, 含进度汇总; 逐项结果用详情)。"""
    rows = (db.query(ArchiveCleanupPlan)
            .order_by(ArchiveCleanupPlan.created_at.desc(),
                      ArchiveCleanupPlan.id.desc()).limit(limit).all())
    return [cleanup.plan_to_dict(db, p, with_items=False) for p in rows]


@app.get("/api/admin/archive-cleanups/{plan_id}")
def get_cleanup_plan(plan_id: str, db: Session = Depends(get_db)):
    """清理计划详情: 进度、当前项、逐项跳过原因与事件流水。"""
    p = db.get(ArchiveCleanupPlan, plan_id)
    if p is None:
        _cleanup_not_found(Exception(f"清理计划 {plan_id} 不存在"))
    return cleanup.plan_to_dict(db, p)


@app.post("/api/admin/archive-cleanups/{plan_id}/pause")
def pause_cleanup_plan(plan_id: str, body: CleanupAction,
                       db: Session = Depends(get_db)):
    """暂停清理计划: 在逐项边界停住(已清理/已跳过项保留); 重复暂停幂等。"""
    return _run_cleanup(db, "pause", body,
                        lambda s, p: cleanup.do_pause(s, p, body.operator), plan_id)


@app.post("/api/admin/archive-cleanups/{plan_id}/resume")
def resume_cleanup_plan(plan_id: str, body: CleanupAction,
                        db: Session = Depends(get_db)):
    """恢复清理计划: 重新排队, 从首个未完成项续跑(PAUSED/FAILED 可恢复,
    FAILED 时失败项重新排队); 重复恢复幂等。"""
    return _run_cleanup(db, "resume", body,
                        lambda s, p: cleanup.do_resume(s, p, body.operator), plan_id)


@app.post("/api/admin/archive-cleanups/{plan_id}/cancel")
def cancel_cleanup_plan(plan_id: str, body: CleanupAction,
                        db: Session = Depends(get_db)):
    """取消清理计划: 未处理项置为跳过, 已清理/已跳过结果保留; 重复取消幂等。"""
    return _run_cleanup(db, "cancel", body,
                        lambda s, p: cleanup.do_cancel(s, p, body.operator), plan_id)


# ---------- 审计事件回放与补偿(统一事件流 / 快照校验 / 补偿执行撤销) ----------

def _audit_replay_not_found(e: Exception):
    raise HTTPException(status_code=404, detail={
        "error": "audit_replay_not_found", "reason": str(e)})


def _audit_replay_conflict(e: Exception):
    raise HTTPException(status_code=409, detail={
        "error": "audit_replay_conflict", "reason": str(e)})


def _parse_iso(value: str | None, field: str):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_timestamp",
            "reason": f"{field} 不是合法的 ISO 8601 时间: {value!r}"})
    if dt.tzinfo is not None:
        from datetime import timezone as _tz
        dt = dt.astimezone(_tz.utc).replace(tzinfo=None)
    return dt


@app.get("/api/admin/audit-events")
def list_audit_events(plan_id: str | None = None, batch_id: str | None = None,
                      start_ts: str | None = None, end_ts: str | None = None,
                      event_type: str | None = None,
                      after_global_seq: int | None = None,
                      limit: int = 50, db: Session = Depends(get_db)):
    """统一审计事件链分页查询(keyset, 按 global_seq 升序)。

    可按计划(计划流, stream_seq 连续)、批次(批次流+投影, 按 correlation 折叠)、
    时间范围(闭区间)与事件类型过滤; after_global_seq 翻页。"""
    if event_type is not None and event_type not in auditreplay.AUDIT_EVENT_TYPES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_event_type",
            "reason": f"事件类型必须是 {list(auditreplay.AUDIT_EVENT_TYPES)} 之一"})
    return auditreplay.query_events(
        db, plan_id=plan_id, batch_id=batch_id,
        start_ts=_parse_iso(start_ts, "start_ts"),
        end_ts=_parse_iso(end_ts, "end_ts"), event_type=event_type,
        limit=limit, after_global_seq=after_global_seq)


@app.post("/api/admin/audit-events", status_code=201)
def add_audit_event_note(body: AuditEventNote, db: Session = Depends(get_db)):
    """运维显式补录备注事件(EXTERNAL_NOTE, 唯一允许直接写入的类型)。

    补录到计划流/批次流/全局流; 时间戳早于流上一事件超过容忍阈值(乱序) -> 409;
    同一 idempotency_key 重放返回首次结果(不产生第二个顺序号)。"""
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="event.note", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            fn=lambda s, _t: auditreplay.append_external_note(
                s, operator=body.operator, content=body.content,
                event_ts=_parse_iso(body.event_ts, "event_ts"),
                plan_id=body.plan_id, batch_id=body.batch_id,
                dedupe_key=body.idempotency_key))
    except auditreplay.AuditEventRejected as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/audit-snapshots", status_code=201)
def create_audit_snapshot(body: AuditSnapshotCreate,
                          db: Session = Depends(get_db)):
    """对已 COMPLETED/CANCELED 计划在目标时间点生成回放快照并校验。

    校验事件连续性、哈希链、乱序、规则版本与批次版本及目标时点状态一致性;
    不满足时快照 REJECTED(逐条原因)且 HTTP 422; 幂等键重放返回首次快照。"""
    target_at = _parse_iso(body.target_at, "target_at")

    def _do(s, _t):
        try:
            snap = auditreplay.create_snapshot(
                s, operator=body.operator, plan_id=body.plan_id,
                target_at=target_at, ttl_seconds=body.ttl_seconds)
        except auditreplay.AuditReplayNotFound:
            raise
        out = auditreplay.snapshot_to_dict(snap)
        if snap.status == "REJECTED":
            raise _SnapshotRejected(out)
        return out

    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="snapshot.create", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            snapshot_id=None, fn=_do)
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except _SnapshotRejected as e:
        db.commit()  # REJECTED 快照仍持久化保留可查
        raise HTTPException(status_code=422, detail={
            "error": "snapshot_rejected",
            "reason": "快照校验未通过, 已拒绝生成(REJECTED 快照已保留可查)",
            "snapshot": e.detail})
    result["replayed"] = replayed
    return result


class _SnapshotRejected(Exception):
    def __init__(self, detail: dict):
        super().__init__("snapshot rejected")
        self.detail = detail


@app.get("/api/admin/audit-snapshots")
def list_audit_snapshots(plan_id: str | None = None,
                         status_filter: str | None = None,
                         limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(AuditSnapshot)
    if plan_id:
        q = q.filter(AuditSnapshot.plan_id == plan_id)
    if status_filter:
        if status_filter not in auditreplay.AUDIT_SNAPSHOT_STATUSES:
            raise HTTPException(status_code=422, detail={
                "error": "invalid_snapshot_status",
                "reason": f"快照状态必须是 {list(auditreplay.AUDIT_SNAPSHOT_STATUSES)}"})
        q = q.filter(AuditSnapshot.status == status_filter)
    rows = (q.order_by(AuditSnapshot.created_at.desc(), AuditSnapshot.id.desc())
            .limit(min(max(1, limit), 200)).all())
    return [auditreplay.snapshot_to_dict(s) for s in rows]


@app.get("/api/admin/audit-snapshots/{snapshot_id}")
def get_audit_snapshot(snapshot_id: str, with_events: bool = True,
                       db: Session = Depends(get_db)):
    snap = db.get(AuditSnapshot, snapshot_id)
    if snap is None:
        _audit_replay_not_found(Exception(f"回放快照 {snapshot_id} 不存在"))
    return auditreplay.snapshot_to_dict(snap, with_events=with_events,
                                        session=db)


@app.get("/api/admin/audit-snapshots/{snapshot_id}/preview")
def preview_snapshot_actions(snapshot_id: str, db: Session = Depends(get_db)):
    """预览快照中的待补偿动作(纯计算, 不落库): 动作类型/目标/预期/当前/门禁。"""
    snap = db.get(AuditSnapshot, snapshot_id)
    if snap is None:
        _audit_replay_not_found(Exception(f"回放快照 {snapshot_id} 不存在"))
    if snap.status != "VALID":
        raise HTTPException(status_code=409, detail={
            "error": "snapshot_rejected",
            "reason": "REJECTED 快照不允许预览补偿动作",
            "reasons": snap.reasons})
    return {"snapshot_id": snap.id, "plan_id": snap.plan_id,
            "plan_status": snap.plan_status_at_create, "expired":
            auditreplay.is_expired(snap),
            "actions": auditreplay.derive_actions(db, snap)}


@app.post("/api/admin/compensations", status_code=201)
def create_compensation(body: CompensationCreate, db: Session = Depends(get_db)):
    """基于 VALID 快照创建补偿任务(动作落 PENDING 并排队)。

    同快照同时至多一个非终态任务, 重复创建幂等返回已有任务; REJECTED 快照拒绝。
    风险等级默认按动作构成自动推导(含清理/解冻为 HIGH), 可显式 risk_level=HIGH。
    HIGH 任务必须收集两名不同于创建者的操作者独立审批后才能执行。"""
    if body.risk_level is not None and body.risk_level not in auditreplay.COMP_RISK_LEVELS:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_risk_level",
            "reason": f"风险等级必须是 {list(auditreplay.COMP_RISK_LEVELS)} 之一"})
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="comp.create", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            snapshot_id=body.snapshot_id,
            fn=lambda s, _t: auditreplay.task_to_dict(
                auditreplay.create_task(
                    s, operator=body.operator, snapshot_id=body.snapshot_id,
                    risk_level=body.risk_level)))
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.get("/api/admin/compensations")
def list_compensations(plan_id: str | None = None,
                       snapshot_id: str | None = None,
                       status_filter: str | None = None,
                       db: Session = Depends(get_db)):
    q = db.query(CompensationTask)
    if plan_id:
        q = q.filter(CompensationTask.plan_id == plan_id)
    if snapshot_id:
        q = q.filter(CompensationTask.snapshot_id == snapshot_id)
    if status_filter:
        if status_filter not in auditreplay.COMP_TASK_STATUSES:
            raise HTTPException(status_code=422, detail={
                "error": "invalid_comp_status",
                "reason": f"补偿任务状态必须是 {list(auditreplay.COMP_TASK_STATUSES)}"})
        q = q.filter(CompensationTask.status == status_filter)
    rows = (q.order_by(CompensationTask.created_at.desc(),
                       CompensationTask.id.desc()).limit(200).all())
    return [auditreplay.task_to_dict(t, with_actions=False, with_events=False)
            for t in rows]


@app.get("/api/admin/compensations/{task_id}")
def get_compensation(task_id: str, db: Session = Depends(get_db)):
    task = db.get(CompensationTask, task_id)
    if task is None:
        _audit_replay_not_found(Exception(f"补偿任务 {task_id} 不存在"))
    return auditreplay.task_to_dict(task)


def _run_comp_task(db: Session, body, fn, task_id: str, action: str):
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id, fn=fn)
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


def _comp_action_windows(body: CompensationWindowAction):
    return [{"starts_at": w.starts_at, "ends_at": w.ends_at}
            for w in body.windows]


@app.post("/api/admin/compensations/{task_id}/approvals", status_code=201)
def approve_compensation(task_id: str, body: CompensationApprovalAction,
                         db: Session = Depends(get_db)):
    """高风险补偿双人审批: 提交一名操作者的独立通过(审批人不能是创建者)。

    同一操作者同轮重复/并发提交幂等去重; 集齐两名不同操作者 -> APPROVED。
    审批依据(快照/质量门禁/计划状态)变化时已收集审批自动失效, 须重新收集。"""
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="comp.approve", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id,
            fn=lambda s, _t: auditreplay.approve_task(s, task_id, body.operator))
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/compensations/{task_id}/rejections", status_code=201)
def reject_compensation(task_id: str, body: CompensationRejectAction,
                        db: Session = Depends(get_db)):
    """高风险补偿审批拒绝(原因必填): 闸门关闭, 当轮已收集通过失效, 须重新收集。"""
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="comp.reject", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id,
            fn=lambda s, _t: auditreplay.reject_task(
                s, task_id, body.operator, body.reason))
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.put("/api/admin/compensations/{task_id}/windows")
def update_compensation_windows(task_id: str, body: CompensationWindowAction,
                                db: Session = Depends(get_db)):
    """为补偿任务预约/整体替换限定执行窗口(空列表清空)。

    窗口外禁止执行(显式执行 409; worker 在动作边界自动暂停并保留已完成动作,
    重新进入窗口自动继续)。与其他活动补偿任务时间重叠且批次相交 -> 409
    返回占用者与冲突时间段; 相同窗口重复预约幂等。"""
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="comp.window_update", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id,
            fn=lambda s, _t: auditreplay.update_task_windows(
                s, task_id, body.operator, _comp_action_windows(body)))
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayWindowConflict as e:
        db.rollback()
        raise HTTPException(status_code=409, detail={
            "error": "comp_window_conflict",
            "reason": "预约窗口与其他活动补偿任务冲突, 已返回占用者与冲突时间段",
            "conflicts": e.conflicts})
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/compensations/{task_id}/cancel")
def cancel_compensation(task_id: str, body: CompensationCancelAction,
                        db: Session = Depends(get_db)):
    """取消补偿任务(QUEUED/RUNNING/PARTIAL): 未执行动作终止, 已完成动作保留;
    未决审批随任务取消留痕(TASK_CANCELED), 审批历史仍可查询。"""
    try:
        result, replayed = auditreplay.run_comp_action(
            db, action="comp.cancel", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            task_id=task_id,
            fn=lambda s, _t: auditreplay.task_to_dict(
                auditreplay.cancel_task(s, task_id, body.operator, body.reason)))
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/compensations/{task_id}/execute")
def execute_compensation(task_id: str, body: CompensationAction,
                         db: Session = Depends(get_db)):
    """幂等执行任务全部待补偿动作(逐动作独立事务; 部分失败停 PARTIAL 不回滚)。

    每动作执行前实时复核质量门禁, 不通过则该动作 FAILED(gate_blocked);
    CANCELED 计划动作被拒绝; 快照过期拒绝执行。重复请求幂等, 不重复写入。"""
    return _run_comp_task(
        db, body,
        lambda s, _t: auditreplay.task_to_dict(
            auditreplay.execute_all(s, task_id, body.operator)),
        task_id, "comp.execute")


@app.post("/api/admin/compensations/{task_id}/retry")
def retry_compensation_action(task_id: str, body: CompensationRetry,
                              db: Session = Depends(get_db)):
    """逐动作失败重试(仅 FAILED): 重新过门禁, 成功不重复写入(确定性动作键)。

    不走通用幂等封装: 重试可能被多次调用直到成功, 幂等性由动作状态机
    (SUCCESS 直接返回 / FAILED 才可重试)与确定性 action_key 保证。"""
    try:
        action = auditreplay.retry_action(db, task_id, body.action_seq,
                                          body.operator)
        task = auditreplay.get_task(db, task_id)
        result = auditreplay.action_to_dict(action)
        result["task_id"] = task_id
        result["task_status"] = task.status
        db.commit()
    except auditreplay.AuditReplayNotFound as e:
        db.rollback()
        _audit_replay_not_found(e)
    except auditreplay.AuditReplayStateError as e:
        db.rollback()
        _audit_replay_conflict(e)
    return result


@app.post("/api/admin/compensations/{task_id}/undo")
def undo_compensation(task_id: str, body: CompensationAction,
                      db: Session = Depends(get_db)):
    """整体撤销: 对全部已成功动作按逆序用执行前镜像恢复现场(只追加 COMP_UNDONE,
    关联原执行事件)。撤销不被快照 TTL/门禁/审批/窗口卡死; 部分撤销失败停
    UNDO_PARTIAL; CANCELED 任务若已有成功动作同样允许撤销。"""
    return _run_comp_task(
        db, body,
        lambda s, _t: auditreplay.task_to_dict(
            auditreplay.undo_all(s, task_id, body.operator)),
        task_id, "comp.undo")


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
    批次外记录不受影响, 正常写入。

    写入提交后触发自动重扫编排: 若该批次属于执行中(RUNNING/HALTED)且绑定了
    质量规则的计划, 立即为受影响计划编排唯一重扫(数据指纹将漂移), 重复写入
    不产生重复任务(合并触发原因)。"""
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
    old = db.get(RecordOld, rec.id)
    changed = (old is None or old.name != rec.name or old.email != rec.email
               or old.tags_csv != rec.tags_csv)
    row = RecordOld(id=rec.id, name=rec.name, email=rec.email, tags_csv=rec.tags_csv)
    db.merge(row)
    # 批次范围内写入投影为 BATCH_WRITE 事件(仅活动计划流 + 批次流, 跨计划隔离)
    if batch is not None:
        from . import auditreplay
        auditreplay.emit_batch_write(
            db, batch=batch, record_id=rec.id, path="old",
            record=service.old_to_dict(row), operator="api", changed=changed)
    db.commit()
    rescans = []
    if changed and batch is not None:
        # 自动重扫编排(独立提交, 不影响写入主流程; DRAFT/PAUSED 计划不自动排队)
        try:
            rescans = quality.notify_batch_data_written(
                db, batch.id, record_id=rec.id, operator="api")
        except Exception:  # noqa: BLE001 - 编排失败不得阻断业务写入
            db.rollback()
    return {"ok": True, "structure": "old", "id": rec.id,
            "batch_id": batch.id if batch else None,
            "rescans": rescans}


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
    from . import auditreplay
    auditreplay.emit_batch_write(
        db, batch=batch, record_id=rec.id, path="new",
        record=service.new_to_dict(row), operator="api", changed=True)
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
