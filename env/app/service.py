"""迁移切换核心逻辑: 冻结、双读比对、校验回填、原子切换、恢复。

不变量:
1. 阶段只能沿状态机前进, 或通过 recover 明确回到 NORMAL(可写)。
2. 每次迁移动作 epoch+1 且以 epoch 为条件更新 —— 并发管理员只有一人成功。
3. cutover 是单事务: 崩溃即整体回滚, 不会留下"半切开"状态。
4. DONE 是终态, 不提供自动回退。
"""
import hashlib
import json
import os
import uuid

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import AuditLog, IdempotencyKey, MigrationState, RecordNew, RecordOld

APP_VERSION = os.getenv("APP_VERSION", "1.0.0")


class PhaseError(Exception):
    """当前阶段不允许该操作 -> 409。"""

    def __init__(self, detail: str, diffs: list | None = None):
        super().__init__(detail)
        self.detail = detail
        self.diffs = diffs or []


class EpochConflict(Exception):
    """epoch 栅栏冲突: 别人已推进状态 -> 409。"""


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


def compare_all(session: Session) -> list[dict]:
    """全量双向比对旧表与按转换规则应得的新表内容。

    正向: 每条旧记录在新表必须存在且字段一致;
    反向: 新表不允许存在旧表没有的记录 —— 切换后它会静默生效, 必须报出并阻止。
    """
    diffs: list[dict] = []
    old_ids: set[int] = set()
    for old in session.query(RecordOld).order_by(RecordOld.id):
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
    for new in (session.query(RecordNew)
                .filter(RecordNew.id.notin_(old_ids))
                .order_by(RecordNew.id)):
        diffs.append({
            "record_id": new.id,
            "field": "__extra__",
            "old": None,
            "new": new_to_dict(new),
        })
    return diffs


# ---------- 状态行存取(带栅栏) ----------

def get_state(session: Session) -> MigrationState:
    state = session.get(MigrationState, 1)
    if state is None:  # 首次启动初始化单行
        state = MigrationState(id=1, phase="NORMAL", epoch=0, active_schema="old")
        session.add(state)
        session.flush()
    return state


def lock_state(session: Session) -> MigrationState:
    """Postgres 下行级锁串行化管理员操作; SQLite 由库级写锁保证。"""
    if session.bind.dialect.name != "sqlite":
        state = session.get(MigrationState, 1, with_for_update=True)
        if state is not None:
            return state
    return get_state(session)


def bump(session: Session, state: MigrationState, *, phase: str | None = None,
         operator: str, **fields) -> None:
    """以 epoch 为条件的栅栏更新: 更新行数不为 1 说明有人抢先推进, 直接冲突。"""
    new_epoch = state.epoch + 1
    values = {"epoch": new_epoch, "updated_by": operator, **fields}
    if phase is not None:
        values["phase"] = phase
    rows = (
        session.query(MigrationState)
        .filter(MigrationState.id == 1, MigrationState.epoch == state.epoch)
        .update(values, synchronize_session=False)
    )
    if rows != 1:
        session.rollback()
        raise EpochConflict(f"epoch {state.epoch} 已失效, 状态已被其他操作推进")
    session.expire_all()


def audit(session: Session, state: MigrationState, *, operator: str, action: str,
          from_phase: str | None, reason: str | None = None,
          diffs: list | None = None) -> None:
    session.add(AuditLog(
        operator=operator, action=action,
        from_phase=from_phase, to_phase=state.phase, epoch=state.epoch,
        app_version=APP_VERSION, freeze_version=state.freeze_version,
        watermark=state.watermark, reason=reason, diffs=diffs or None,
    ))


# ---------- 幂等执行框架 ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def run_admin_action(session: Session, *, action: str, operator: str,
                     idempotency_key: str, payload: dict, fn) -> tuple[dict, bool]:
    """fn 在同一事务内执行; 结果与幂等键一起落库。重复执行返回首次结果。"""
    req_hash = _hash(payload)
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise PhaseError("幂等键被不同请求复用", [])
        return existing.response_json, True  # 重放, 无副作用

    state = lock_state(session)
    result = fn(session, state)
    result["epoch"] = get_state(session).epoch
    result["phase"] = get_state(session).phase
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


# ---------- 各迁移动作 ----------

def do_freeze(session: Session, state: MigrationState, operator: str) -> dict:
    if state.phase != "NORMAL":
        raise PhaseError(f"当前阶段 {state.phase} 不允许冻结")
    from_phase = state.phase
    freeze_version = uuid.uuid4().hex[:12]
    bump(session, state, phase="FROZEN", operator=operator,
         freeze_version=freeze_version, watermark=0)
    st = get_state(session)
    audit(session, st, operator=operator, action="freeze", from_phase=from_phase,
          reason=f"开启冻结窗 {freeze_version}")
    return {"ok": True, "freeze_version": freeze_version, "detail": "已冻结, 所有写入被拒绝"}


def do_validate(session: Session, state: MigrationState, operator: str) -> dict:
    # VALIDATED 也允许重新校验: 切换被复核阻止后, 修复数据需重新验证
    if state.phase not in ("FROZEN", "VALIDATING", "VALIDATED"):
        raise PhaseError(f"当前阶段 {state.phase} 不允许校验")
    from_phase = state.phase
    bump(session, state, phase="VALIDATING", operator=operator)
    st = get_state(session)
    audit(session, st, operator=operator, action="validate", from_phase=from_phase,
          reason=f"从水位 {st.watermark} 继续回填并比对")

    # 回填: 从水位续跑, upsert 保证重复执行幂等
    for old in (session.query(RecordOld)
                .filter(RecordOld.id > st.watermark)
                .order_by(RecordOld.id)):
        payload = transform(old)
        new = session.get(RecordNew, old.id)
        if new is None:
            session.add(RecordNew(**payload))
        else:
            new.name, new.email, new.tags = payload["name"], payload["email"], payload["tags"]
            new.schema_version = 2
        st_watermark = old.id
        session.query(MigrationState).filter(MigrationState.id == 1).update(
            {"watermark": st_watermark}, synchronize_session=False)
        session.flush()

    diffs = compare_all(session)
    st = get_state(session)
    if diffs:
        # 校验未过: 停在 FROZEN(写入仍被拒绝), 差异落审计, 阻止切换
        bump(session, st, phase="FROZEN", operator=operator)
        st2 = get_state(session)
        audit(session, st2, operator=operator, action="validate",
              from_phase="VALIDATING", reason=f"校验失败: {len(diffs)} 处差异, 阻止切换",
              diffs=diffs)
        return {"ok": False, "diffs": diffs,
                "detail": f"校验发现 {len(diffs)} 处差异, 已阻止切换, 保持冻结"}
    bump(session, st, phase="VALIDATED", operator=operator)
    st2 = get_state(session)
    audit(session, st2, operator=operator, action="validate",
          from_phase="VALIDATING", reason="校验通过, 允许切换")
    return {"ok": True, "diffs": [], "detail": "校验通过, 可以切换"}


def do_cutover(session: Session, state: MigrationState, operator: str,
               expected_epoch: int | None) -> dict:
    if state.phase != "VALIDATED":
        raise PhaseError(f"当前阶段 {state.phase} 不允许切换(需先通过校验)")
    if expected_epoch is not None and expected_epoch != state.epoch:
        raise EpochConflict(f"expected_epoch={expected_epoch} 与当前 epoch={state.epoch} 不一致")
    # 切换前再全量比对一次(防御性): 不一致则阻止, 审计随正常提交落库
    diffs = compare_all(session)
    if diffs:
        audit(session, state, operator=operator, action="cutover",
              from_phase=state.phase, reason=f"切换前复核发现 {len(diffs)} 处差异, 阻止切换",
              diffs=diffs)
        return {"ok": False, "diffs": diffs,
                "detail": f"复核发现 {len(diffs)} 处差异, 已阻止切换"}
    # 原子切换: 同事务内翻阶段+生效新结构+写审计, 崩溃则整体回滚
    from_phase = state.phase
    bump(session, state, phase="DONE", operator=operator, active_schema="new")
    st = get_state(session)
    audit(session, st, operator=operator, action="cutover", from_phase=from_phase,
          reason="一次性切换完成, 新写入只进新结构")
    return {"ok": True, "detail": "切换完成, 新结构已生效, 旧写入路径已关闭"}


def do_recover(session: Session, state: MigrationState, operator: str,
               reason: str) -> dict:
    if state.phase == "NORMAL":
        raise PhaseError("当前已是可写状态, 无需恢复")
    if state.phase == "DONE":
        raise PhaseError("切换已完成, 不支持自动回退(避免双状态)")
    from_phase = state.phase
    # 清理半迁移数据: 新表回填内容 + 水位归零, 与阶段回滚同一事务
    removed = session.query(RecordNew).delete(synchronize_session=False)
    bump(session, state, phase="NORMAL", operator=operator,
         freeze_version=None, watermark=0, active_schema="old")
    st = get_state(session)
    audit(session, st, operator=operator, action="recover", from_phase=from_phase,
          reason=f"{reason}(清理半迁移数据 {removed} 行, 水位归零)")
    return {"ok": True, "detail": f"已恢复到冻结前可写状态, 清理 {removed} 行半迁移数据"}


# ---------- 启动恢复检查 ----------

def boot_check(session: Session) -> None:
    """重启后审计落一条当前状态。VALIDATING 是安全停止点: 水位已持久化, 重跑校验即可续跑。"""
    state = get_state(session)
    if state.phase != "NORMAL":
        audit(session, state, operator="system", action="boot", from_phase=state.phase,
              reason=f"服务重启, 当前阶段 {state.phase}, 水位 {state.watermark}, 可安全续跑或恢复")
        session.commit()
