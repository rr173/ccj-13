"""按业务分组的迁移批次管理: 创建批次、冻结、双读比对、校验回填、原子切换、单独恢复。

不变量:
1. 每个批次独立状态机, 阶段只能沿状态机前进, 或通过 recover 明确回到 NORMAL(可写)。
2. 批次内每次迁移动作 epoch+1 且以 epoch 为条件更新 —— 两个管理员同时推进同一批次,
   只有一人成功, 其余收到 409。
3. cutover 是单事务: 崩溃即整体回滚, 不会留下"半切开"状态。
4. DONE 是终态, 不提供自动回退。
5. 回填/比对/清理都只作用于批次范围 [id_start, id_end]: 批次外记录正常读写,
   恢复一个失败批次不会清掉其他批次的数据。
"""
import hashlib
import json
import os
import uuid

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import AuditLog, IdempotencyKey, MigrationBatch, RecordNew, RecordOld

APP_VERSION = os.getenv("APP_VERSION", "1.0.0")


class PhaseError(Exception):
    """当前阶段不允许该操作 -> 409。"""

    def __init__(self, detail: str, diffs: list | None = None):
        super().__init__(detail)
        self.detail = detail
        self.diffs = diffs or []


class EpochConflict(Exception):
    """epoch 栅栏冲突: 别人已推进该批次状态 -> 409。"""


class BatchNotFound(Exception):
    """批次不存在 -> 404。"""


# ---------- 结构转换与比对 ----------

def transform(old: RecordOld) -> dict:
    """旧结构 -> 新结构。"""
    return {
        "id": old.id,
        "name": old.name,
        "email": old.email,
        "tags": [t for t in old.tags_csv.split(",") if t],
        "schema_version": 2,
    }


def new_to_dict(rec: RecordNew) -> dict:
    return {
        "id": rec.id,
        "name": rec.name,
        "email": rec.email,
        "tags": list(rec.tags or []),
        "schema_version": rec.schema_version,
    }


def old_to_dict(rec: RecordOld) -> dict:
    return {
        "id": rec.id,
        "name": rec.name,
        "email": rec.email,
        "tags_csv": rec.tags_csv,
    }


def diff_records(expected: dict, actual: dict, record_id: int) -> list[dict]:
    diffs = []
    for field in ("name", "email", "tags"):
        if expected.get(field) != actual.get(field):
            diffs.append({
                "record_id": record_id,
                "field": field,
                "old": expected.get(field),
                "new": actual.get(field),
            })
    return diffs


def _in_range(col, batch: MigrationBatch):
    return (col >= batch.id_start) & (col <= batch.id_end)


def compare_batch(session: Session, batch: MigrationBatch) -> list[dict]:
    """批次范围内双向比对旧表与按转换规则应得的新表内容。

    正向: 范围内每条旧记录在新表必须存在且字段一致;
    反向: 范围内新表不允许存在旧表没有的记录 —— 切换后它会静默生效, 必须报出并阻止。
    范围外的记录不参与比对, 不影响本批次切换。
    """
    diffs: list[dict] = []
    old_ids: set[int] = set()
    for old in (session.query(RecordOld)
                .filter(_in_range(RecordOld.id, batch))
                .order_by(RecordOld.id)):
        old_ids.add(old.id)
        new = session.get(RecordNew, old.id)
        if new is None:
            diffs.append({
                "record_id": old.id,
                "field": "__missing__",
                "old": old_to_dict(old),
                "new": None,
            })
            continue
        diffs.extend(diff_records(transform(old), new_to_dict(new), old.id))
    extra_q = (session.query(RecordNew)
               .filter(_in_range(RecordNew.id, batch)))
    if old_ids:
        extra_q = extra_q.filter(RecordNew.id.notin_(sorted(old_ids)))
    for new in extra_q.order_by(RecordNew.id):
        diffs.append({
            "record_id": new.id,
            "field": "__extra__",
            "old": None,
            "new": new_to_dict(new),
        })
    return diffs


# ---------- 批次行存取(带栅栏) ----------

def get_batch(session: Session, batch_id: str) -> MigrationBatch:
    batch = session.get(MigrationBatch, batch_id)
    if batch is None:
        raise BatchNotFound(f"批次 {batch_id} 不存在")
    return batch


def lock_batch(session: Session, batch_id: str) -> MigrationBatch:
    """Postgres 下行级锁串行化同一批次的管理员操作; SQLite 由库级写锁保证。"""
    if session.bind.dialect.name != "sqlite":
        batch = session.get(MigrationBatch, batch_id, with_for_update=True)
        if batch is not None:
            return batch
    return get_batch(session, batch_id)


def _lock_creation(session: Session) -> None:
    """串行化批次创建, 防止并发创建出重叠范围。

    Postgres 用事务级咨询锁; SQLite 写事务天然串行。
    """
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260912)"))


def bump(session: Session, batch: MigrationBatch, *, phase: str | None = None,
         operator: str, **fields) -> None:
    """以 epoch 为条件的栅栏更新: 更新行数不为 1 说明有人抢先推进, 直接冲突。"""
    new_epoch = batch.epoch + 1
    values = {"epoch": new_epoch, "updated_by": operator, **fields}
    if phase is not None:
        values["phase"] = phase
    rows = (
        session.query(MigrationBatch)
        .filter(MigrationBatch.id == batch.id, MigrationBatch.epoch == batch.epoch)
        .update(values, synchronize_session=False)
    )
    if rows != 1:
        session.rollback()
        raise EpochConflict(
            f"批次 {batch.id} epoch {batch.epoch} 已失效, 状态已被其他操作推进")
    session.expire_all()


def audit(session: Session, batch: MigrationBatch, *, operator: str, action: str,
          from_phase: str | None, reason: str | None = None,
          diffs: list | None = None) -> None:
    session.add(AuditLog(
        batch_id=batch.id, operator=operator, action=action,
        from_phase=from_phase, to_phase=batch.phase, epoch=batch.epoch,
        app_version=APP_VERSION, freeze_version=batch.freeze_version,
        watermark=batch.watermark, reason=reason, diffs=diffs or None,
    ))


# ---------- 幂等执行框架 ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def run_admin_action(session: Session, *, action: str, operator: str,
                     idempotency_key: str, payload: dict, fn,
                     batch_id: str | None = None) -> tuple[dict, bool]:
    """fn 在同一事务内执行; 结果与幂等键一起落库。重复执行返回首次结果。

    请求哈希包含动作与批次: 同一幂等键不能跨批次/跨动作复用。
    """
    req_hash = _hash({"action": action, "batch_id": batch_id, "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise PhaseError("幂等键被不同请求复用", [])
        return existing.response_json, True  # 重放, 无副作用

    batch = lock_batch(session, batch_id) if batch_id is not None else None
    if batch is None:
        _lock_creation(session)
    result = fn(session, batch)
    if batch_id is not None:
        fresh = get_batch(session, batch_id)
        result["batch_id"] = batch_id
        result["epoch"] = fresh.epoch
        result["phase"] = fresh.phase
    session.add(IdempotencyKey(key=idempotency_key, action=action,
                               request_hash=req_hash, response_json=result))
    try:
        session.commit()
    except IntegrityError:  # 并发下同键撞主键: 回滚后返回先提交者的结果
        session.rollback()
        winner = session.get(IdempotencyKey, idempotency_key)
        if winner is not None and winner.request_hash == req_hash:
            return winner.response_json, True
        raise
    return result, False


# ---------- 批次创建与各迁移动作 ----------

def do_create_batch(session: Session, _batch, operator: str, biz: str,
                    id_start: int, id_end: int) -> dict:
    if id_start > id_end:
        raise PhaseError(f"记录范围非法: id_start({id_start}) 必须 <= id_end({id_end})")
    overlap = (session.query(MigrationBatch)
               .filter(MigrationBatch.id_start <= id_end,
                       MigrationBatch.id_end >= id_start)
               .order_by(MigrationBatch.created_at, MigrationBatch.id)
               .first())
    if overlap is not None:
        raise PhaseError(
            f"记录范围 [{id_start},{id_end}] 与已有批次 {overlap.id}"
            f"({overlap.biz} [{overlap.id_start},{overlap.id_end}]) 重叠")
    batch_id = "B" + uuid.uuid4().hex[:10]
    batch = MigrationBatch(
        id=batch_id, biz=biz, id_start=id_start, id_end=id_end,
        phase="NORMAL", epoch=0, active_schema="old",
        created_by=operator, updated_by=operator)
    session.add(batch)
    session.flush()
    audit(session, batch, operator=operator, action="create", from_phase=None,
          reason=f"创建批次 {biz}, 记录范围 [{id_start},{id_end}]")
    return {"ok": True, "batch_id": batch_id,
            "detail": f"批次 {batch_id} 已创建: {biz} 记录范围 [{id_start},{id_end}]"}


def do_freeze(session: Session, batch: MigrationBatch, operator: str) -> dict:
    if batch.phase != "NORMAL":
        raise PhaseError(f"批次 {batch.id} 当前阶段 {batch.phase} 不允许冻结")
    from_phase = batch.phase
    freeze_version = uuid.uuid4().hex[:12]
    bump(session, batch, phase="FROZEN", operator=operator,
         freeze_version=freeze_version, watermark=batch.id_start - 1)
    st = get_batch(session, batch.id)
    audit(session, st, operator=operator, action="freeze", from_phase=from_phase,
          reason=f"开启冻结窗 {freeze_version}, 范围 [{st.id_start},{st.id_end}] 写入被拒绝")
    return {"ok": True, "freeze_version": freeze_version,
            "detail": f"批次已冻结, 范围 [{st.id_start},{st.id_end}] 内写入被拒绝, 范围外不受影响"}


def do_validate(session: Session, batch: MigrationBatch, operator: str) -> dict:
    # VALIDATED 也允许重新校验: 切换被复核阻止后, 修复数据需重新验证
    if batch.phase not in ("FROZEN", "VALIDATING", "VALIDATED"):
        raise PhaseError(f"批次 {batch.id} 当前阶段 {batch.phase} 不允许校验")
    from_phase = batch.phase
    bump(session, batch, phase="VALIDATING", operator=operator)
    st = get_batch(session, batch.id)
    audit(session, st, operator=operator, action="validate", from_phase=from_phase,
          reason=f"从水位 {st.watermark} 继续回填并比对(范围 [{st.id_start},{st.id_end}])")

    # 回填: 只处理本批次范围, 从水位续跑, upsert 保证重复执行幂等
    for old in (session.query(RecordOld)
                .filter(RecordOld.id > st.watermark,
                        _in_range(RecordOld.id, st))
                .order_by(RecordOld.id)):
        payload = transform(old)
        new = session.get(RecordNew, old.id)
        if new is None:
            session.add(RecordNew(**payload))
        else:
            new.name, new.email, new.tags = payload["name"], payload["email"], payload["tags"]
            new.schema_version = 2
        session.query(MigrationBatch).filter(MigrationBatch.id == st.id).update(
            {"watermark": old.id}, synchronize_session=False)
        session.flush()

    diffs = compare_batch(session, st)
    st = get_batch(session, batch.id)
    if diffs:
        # 校验未过: 停在 FROZEN(写入仍被拒绝), 差异落审计, 阻止切换
        bump(session, st, phase="FROZEN", operator=operator)
        st2 = get_batch(session, batch.id)
        audit(session, st2, operator=operator, action="validate",
              from_phase="VALIDATING", reason=f"校验失败: {len(diffs)} 处差异, 阻止切换",
              diffs=diffs)
        return {"ok": False, "diffs": diffs,
                "detail": f"校验发现 {len(diffs)} 处差异, 已阻止切换, 保持冻结"}
    bump(session, st, phase="VALIDATED", operator=operator)
    st2 = get_batch(session, batch.id)
    audit(session, st2, operator=operator, action="validate",
          from_phase="VALIDATING", reason="校验通过, 允许切换")
    return {"ok": True, "diffs": [], "detail": "校验通过, 可以切换"}


def do_cutover(session: Session, batch: MigrationBatch, operator: str,
               expected_epoch: int | None) -> dict:
    if batch.phase != "VALIDATED":
        raise PhaseError(f"批次 {batch.id} 当前阶段 {batch.phase} 不允许切换(需先通过校验)")
    if expected_epoch is not None and expected_epoch != batch.epoch:
        raise EpochConflict(
            f"批次 {batch.id} expected_epoch={expected_epoch} 与当前 epoch={batch.epoch} 不一致")
    # 切换前再全量比对一次本批次范围(防御性): 不一致则阻止, 审计随正常提交落库
    diffs = compare_batch(session, batch)
    if diffs:
        audit(session, batch, operator=operator, action="cutover",
              from_phase=batch.phase, reason=f"切换前复核发现 {len(diffs)} 处差异, 阻止切换",
              diffs=diffs)
        return {"ok": False, "diffs": diffs,
                "detail": f"复核发现 {len(diffs)} 处差异, 已阻止切换"}
    # 原子切换: 同事务内翻阶段+生效新结构+写审计, 崩溃则整体回滚
    from_phase = batch.phase
    bump(session, batch, phase="DONE", operator=operator, active_schema="new")
    st = get_batch(session, batch.id)
    audit(session, st, operator=operator, action="cutover", from_phase=from_phase,
          reason="一次性切换完成, 范围内新写入只进新结构")
    return {"ok": True, "detail": "切换完成, 本批次新结构已生效, 范围内旧写入路径已关闭"}


def do_recover(session: Session, batch: MigrationBatch, operator: str,
               reason: str) -> dict:
    if batch.phase == "NORMAL":
        raise PhaseError(f"批次 {batch.id} 已是可写状态, 无需恢复")
    if batch.phase == "DONE":
        raise PhaseError(f"批次 {batch.id} 切换已完成, 不支持自动回退(避免双状态)")
    from_phase = batch.phase
    # 只清理本批次范围内的半迁移数据, 与阶段回滚同一事务; 其他批次的数据不受影响
    removed = (session.query(RecordNew)
               .filter(_in_range(RecordNew.id, batch))
               .delete(synchronize_session=False))
    bump(session, batch, phase="NORMAL", operator=operator,
         freeze_version=None, watermark=None, active_schema="old")
    st = get_batch(session, batch.id)
    audit(session, st, operator=operator, action="recover", from_phase=from_phase,
          reason=f"{reason}(清理本批次半迁移数据 {removed} 行, 水位归零)")
    return {"ok": True,
            "detail": f"批次已恢复到冻结前可写状态, 清理 {removed} 行半迁移数据(仅本批次范围)"}


# ---------- 记录归属与批次视图 ----------

def batch_for_record(session: Session, record_id: int) -> MigrationBatch | None:
    """记录所属的批次(范围不重叠, 至多一个)。None 表示批次外, 正常读写。"""
    return (session.query(MigrationBatch)
            .filter(MigrationBatch.id_start <= record_id,
                    MigrationBatch.id_end >= record_id)
            .first())


def batch_progress(session: Session, batch: MigrationBatch) -> dict:
    """回填进度始终以批次范围 [id_start, id_end] 内的旧表记录为基准:
    total = 范围内旧表记录数; done = 其中在新表也存在的记录数。
    旧表不存在的"多余"新表记录(如旧记录删除后的残留、误写入)不算已迁移,
    否则会出现 done > total(例如 2/1); 这类记录由 compare_batch 作为差异报出并阻止切换。
    """
    total = (session.query(RecordOld)
             .filter(_in_range(RecordOld.id, batch)).count())
    done = (session.query(RecordOld.id)
            .join(RecordNew, RecordNew.id == RecordOld.id)
            .filter(_in_range(RecordOld.id, batch)).count())
    return {"done": done, "total": total}


def batch_to_dict(session: Session, b: MigrationBatch) -> dict:
    """批次视图: 阶段、epoch、水位、进度; 冻结窗内附带实时差异。"""
    diffs = compare_batch(session, b) if b.phase in ("FROZEN", "VALIDATING", "VALIDATED") else []
    return {
        "id": b.id,
        "biz": b.biz,
        "id_start": b.id_start,
        "id_end": b.id_end,
        "phase": b.phase,
        "epoch": b.epoch,
        "freeze_version": b.freeze_version,
        "watermark": b.watermark,
        "active_schema": b.active_schema,
        "progress": batch_progress(session, b),
        "pending_diffs": len(diffs),
        "diffs": diffs,
        "created_by": b.created_by,
        "updated_by": b.updated_by,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
    }


# ---------- 启动恢复检查 ----------

def boot_check(session: Session) -> None:
    """重启后每个非 NORMAL 批次各落一条审计。VALIDATING 是安全停止点:
    水位已持久化, 重跑校验即可续跑。"""
    batches = (session.query(MigrationBatch)
               .filter(MigrationBatch.phase != "NORMAL")
               .order_by(MigrationBatch.id)
               .all())
    for b in batches:
        audit(session, b, operator="system", action="boot", from_phase=b.phase,
              reason=f"服务重启, 批次 {b.id} 当前阶段 {b.phase}, 水位 {b.watermark}, 可安全续跑或恢复")
    if batches:
        session.commit()
