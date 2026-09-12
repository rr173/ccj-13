import os

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .db import Base, SessionLocal, engine
from .models import AuditLog, MigrationBatch, MigrationPlan, RecordNew, RecordOld
from .plans import PlanWorker
from .schemas import (
    AdminAction, BatchCreate, PlanAction, PlanCreate, RecordIn, RecoverAction,
)
from . import plans, service

APP_VERSION = service.APP_VERSION
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
# 测试可通过 PLAN_WORKER_ENABLED=0 关闭自动线程, 手动 run_plan_tick 做确定性验证。
# 在 startup 时读取(而非 import 时), 便于测试进程内切换。
def _worker_enabled() -> bool:
    return os.getenv("PLAN_WORKER_ENABLED", "1") not in ("0", "false", "False")


app = FastAPI(title="结构迁移切换服务(按业务分组批次)", version=APP_VERSION)
worker = PlanWorker(poll_interval=float(os.getenv("PLAN_WORKER_POLL_INTERVAL", "0.5")))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    db = SessionLocal()
    try:
        service.boot_check(db)
        db.commit()
        # 计划重启对账必须先于 worker: 把遗留 RUNNING 步骤复位, 计划才不会"假运行"
        plans.boot_recover_plans(db)
        db.commit()
    finally:
        db.close()
    if _worker_enabled():
        worker.start()


@app.on_event("shutdown")
def shutdown():
    worker.stop()


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
    return {
        "app_version": APP_VERSION,
        "batches": [service.batch_to_dict(db, b) for b in batches],
        "plans": [plans.plan_to_dict(db, p) for p in plan_rows],
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
    try:
        result, replayed = plans.run_plan_action(
            db, action="create", operator=body.operator,
            idempotency_key=body.idempotency_key, payload=body.model_dump(),
            plan_id=None,
            fn=lambda s, _p: plans.do_create_plan(
                s, _p, body.operator, body.name, steps, body.max_retries))
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
