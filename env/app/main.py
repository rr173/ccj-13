import os

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from .db import Base, SessionLocal, engine
from .models import AuditLog, MigrationState, RecordNew, RecordOld
from .schemas import AdminAction, RecordIn, RecoverAction
from . import service

APP_VERSION = service.APP_VERSION
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(title="结构迁移切换服务", version=APP_VERSION)


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
        service.get_state(db)
        service.boot_check(db)
        db.commit()
    finally:
        db.close()


# ---------- 错误映射 ----------

def _conflict(e: Exception, diffs=None):
    raise HTTPException(status_code=409, detail={"error": "conflict", "reason": str(e), "diffs": diffs or []})


# ---------- 状态与审计 ----------

@app.get("/api/status")
def status(db: Session = Depends(get_db)):
    st = service.get_state(db)
    return {
        "phase": st.phase,
        "epoch": st.epoch,
        "freeze_version": st.freeze_version,
        "watermark": st.watermark,
        "active_schema": st.active_schema,
        "app_version": APP_VERSION,
        "pending_diffs": len(service.compare_all(db)) if st.phase in ("FROZEN", "VALIDATING") else 0,
    }


@app.get("/api/admin/audit")
def audit_log(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.query(AuditLog).order_by(AuditLog.id.desc()).limit(limit).all()
    return [
        {
            "id": r.id, "ts": r.ts.isoformat() if r.ts else None,
            "operator": r.operator, "action": r.action,
            "from_phase": r.from_phase, "to_phase": r.to_phase, "epoch": r.epoch,
            "app_version": r.app_version, "freeze_version": r.freeze_version,
            "watermark": r.watermark, "reason": r.reason, "diffs": r.diffs,
        }
        for r in rows
    ]


# ---------- 迁移动作(幂等 + 栅栏) ----------

def _run(db: Session, action: str, body: AdminAction, fn):
    payload = body.model_dump()
    try:
        result, replayed = service.run_admin_action(
            db, action=action, operator=body.operator,
            idempotency_key=body.idempotency_key, payload=payload, fn=fn)
    except service.EpochConflict as e:
        db.rollback()
        _conflict(e)
    except service.PhaseError as e:
        db.rollback()
        _conflict(e, e.diffs)
    result["replayed"] = replayed
    return result


@app.post("/api/admin/freeze")
def freeze(body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "freeze", body,
                lambda s, st: service.do_freeze(s, st, body.operator))


@app.post("/api/admin/validate")
def validate(body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "validate", body,
                lambda s, st: service.do_validate(s, st, body.operator))


@app.post("/api/admin/cutover")
def cutover(body: AdminAction, db: Session = Depends(get_db)):
    return _run(db, "cutover", body,
                lambda s, st: service.do_cutover(s, st, body.operator, body.expected_epoch))


@app.post("/api/admin/recover")
def recover(body: RecoverAction, db: Session = Depends(get_db)):
    return _run(db, "recover", body,
                lambda s, st: service.do_recover(s, st, body.operator, body.reason))


# ---------- 记录读写(写入闸门 + 双读) ----------

def _reject_writes(st: MigrationState):
    raise HTTPException(status_code=423, detail={
        "error": "writes_rejected",
        "reason": f"迁移冻结中, 写入被拒绝 (freeze_version={st.freeze_version})",
        "phase": st.phase,
    })


@app.post("/api/records", status_code=201)
def create_old(rec: RecordIn, db: Session = Depends(get_db)):
    """旧结构写入路径。冻结期拒绝并说明原因; 切换后明确失败。"""
    st = service.get_state(db)
    if st.phase == "DONE":
        raise HTTPException(status_code=410, detail={
            "error": "old_path_retired",
            "reason": "结构已切换, 旧写入路径已关闭, 请使用 /api/v2/records",
        })
    if st.phase != "NORMAL":
        _reject_writes(st)
    row = RecordOld(id=rec.id, name=rec.name, email=rec.email, tags_csv=rec.tags_csv)
    db.merge(row)
    db.commit()
    return {"ok": True, "structure": "old", "id": rec.id}


@app.post("/api/v2/records", status_code=201)
def create_new(rec: RecordIn, db: Session = Depends(get_db)):
    """新结构写入路径。仅在切换完成后开放。"""
    st = service.get_state(db)
    if st.phase != "DONE":
        raise HTTPException(status_code=409, detail={
            "error": "new_path_inactive",
            "reason": f"新结构尚未生效(当前阶段 {st.phase})",
        })
    row = RecordNew(id=rec.id, name=rec.name, email=rec.email,
                    tags=rec.tags or [], schema_version=2)
    db.merge(row)
    db.commit()
    return {"ok": True, "structure": "new", "id": rec.id}


@app.get("/api/records/{record_id}")
def read_record(record_id: int, db: Session = Depends(get_db)):
    """NORMAL 读旧, DONE 读新; 冻结窗内同一编号同时返回新旧两份并给出差异。"""
    st = service.get_state(db)
    if st.phase == "DONE":
        new = db.get(RecordNew, record_id)
        if new is None:
            raise HTTPException(404, "记录不存在")
        return {"source": "new", "record": service.new_to_dict(new)}
    old = db.get(RecordOld, record_id)
    if old is None:
        raise HTTPException(404, "记录不存在")
    if st.phase == "NORMAL":
        return {"source": "old", "record": service.old_to_dict(old)}
    new = db.get(RecordNew, record_id)
    expected = service.transform(old)
    diffs = ([] if new is not None
             else [{"record_id": record_id, "field": "__missing__", "old": expected, "new": None}])
    if new is not None:
        diffs = service.diff_records(expected, service.new_to_dict(new), record_id)
    return {
        "source": "dual",
        "old": service.old_to_dict(old),
        "new": service.new_to_dict(new) if new else None,
        "diff": diffs,
        "consistent": not diffs,
    }


@app.get("/api/records/{record_id}/compare")
def compare_record(record_id: int, db: Session = Depends(get_db)):
    st = service.get_state(db)
    if st.phase in ("NORMAL", "DONE"):
        raise HTTPException(409, detail={"error": "not_in_freeze_window",
                                         "reason": "仅冻结窗内提供双读比对"})
    return read_record(record_id, db)


# ---------- 管理页面 ----------

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
