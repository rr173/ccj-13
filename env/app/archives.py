"""回放证据归档: 为已 COMPLETED 的回放按指定报告版本生成不可变归档包。

设计要点:
1. 归档任务状态机(与回放任务同构): QUEUED(排队等并发额度) -> RUNNING -> COMPLETED;
   pause 在归档单元边界停住(PAUSED), resume 重新排队(QUEUED, 再次受并发闸门约束);
   cancel 终止任务; 版本冲突/缺失步骤/数据被删/摘要不一致等致命错误 -> FAILED,
   FAILED/CANCELED 记录永久保留可查询。
2. 归档包(zip)必须包含:
   - metadata.json        归档元信息(归档 id、指定报告版本、操作者、时间)
   - replay/task.json     回放任务快照(报告版本、复核状态、确认人、差异汇总)
   - replay/steps.json    全部步骤报告(预期/实际状态、字段差异、批次状态差异)
   - review/conclusions.json 指定版本的逐步复核结论 + 全量历史版本结论
   - review/assignments.json  复核分派历史(只追加)
   - audit/summary.json   审计摘要(计划级/批次级审计、关键 freeze/cutover 证据)
   - manifest.json        逐文件大小/sha256 + 包内容摘要(本身不参与摘要)
   内容摘要 = sha256(逐文件 sha256 与文件名的有序拼接), 摘要随包与数据库双份保存,
   校验接口重算并比对, 不一致则归档明确置为 FAILED(digest_mismatch)并保留记录。
3. 归档严格只读: 绝不修改批次/计划/回放/复核/业务记录, 只写 replay_archive_* 表与
   归档包文件; 活动归档(QUEUED/RUNNING/PAUSED)期间, 对应回放的复核提交/确认/
   重新打开/分派一律被拒绝(归档期间不能修改原回放或复核数据)。
4. 归档单元边界即崩溃边界: 校验 -> 逐步报告 -> 复核结论 -> 分派历史 -> 审计摘要 ->
   打包, 每个单元产物先入 staging 并提交, 再进入下一单元; worker 每次 tick 至多
   推进一个单元, pause/cancel 在单元边界生效; 重启把遗留 RUNNING 任务复位到
   QUEUED, 从 staging 中第一个未完成单元续跑。
5. 并发: 同时 RUNNING 的归档任务不超过 ARCHIVE_MAX_CONCURRENCY(默认 2), 其余排队;
   Postgres 用咨询锁串行认领, SQLite 写事务天然串行。
6. 创建/暂停/恢复/取消全部走幂等框架(archive.* 命名空间); 同一(回放, 报告版本)
   的重复创建幂等返回已有归档(活动中或已完成), 控制请求对同态重复调用无副作用。
"""
import hashlib
import json
import os
import threading
import time
import uuid
import zipfile

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from .models import (
    AuditLog, IdempotencyKey, MigrationBatch, ReplayArchive, ReplayArchiveEvent,
    ReplayAssignment, ReplayCheckpoint, ReplayCheckpointStep, ReplayReview,
    ReplayTask, ReplayTaskStep,
)

# 归档单元(阶段)顺序: 校验 -> 逐步报告 -> 复核结论 -> 分派历史 -> 审计摘要 -> 打包
STAGE_VALIDATE = "validate"
STAGE_REVIEWS = "reviews"
STAGE_ASSIGNMENTS = "assignments"
STAGE_AUDIT_SUMMARY = "audit_summary"
STAGE_PACKAGE = "package"
TAIL_STAGES = (STAGE_REVIEWS, STAGE_ASSIGNMENTS, STAGE_AUDIT_SUMMARY, STAGE_PACKAGE)

ARCHIVE_ALLOWED_ACTIONS = {
    "pause": {"QUEUED", "RUNNING"},
    "resume": {"PAUSED"},
    "cancel": {"QUEUED", "RUNNING", "PAUSED"},
}

# 包内载荷文件(参与内容摘要); manifest.json 最后写入, 不参与摘要
PAYLOAD_FILES = (
    "metadata.json",
    "replay/task.json",
    "replay/steps.json",
    "review/conclusions.json",
    "review/assignments.json",
    "audit/summary.json",
)
MANIFEST_FILE = "manifest.json"
DIGEST_ALGORITHM = "sha256"


class ArchiveNotFound(Exception):
    """回放 / 归档不存在 -> 404。"""


class ArchiveStateError(Exception):
    """当前状态不允许该操作 / 归档活动锁 / 幂等键复用 -> 409。"""


class _ArchiveFatal(Exception):
    """归档执行中的致命错误: 任务必须 FAILED。code 为机器可读错误码。"""

    def __init__(self, code: str, reason: str):
        super().__init__(reason)
        self.code = code
        self.reason = reason


# ---------- 配置 ----------

def max_concurrency() -> int:
    """同时允许 RUNNING 的归档任务数, 环境变量 ARCHIVE_MAX_CONCURRENCY(默认 2, 至少 1)。"""
    try:
        return max(1, int(os.getenv("ARCHIVE_MAX_CONCURRENCY", "2")))
    except ValueError:
        return 2


def store_dir() -> str:
    """归档包落盘目录, 环境变量 ARCHIVE_STORE_DIR(默认 ./archive_store)。"""
    d = os.getenv("ARCHIVE_STORE_DIR", os.path.join(".", "archive_store"))
    return os.path.abspath(d)


def ensure_store_dir() -> None:
    os.makedirs(store_dir(), exist_ok=True)


def now_utc_naive():
    from .plans import now_utc_naive as _f
    return _f()


def _dt(v) -> str | None:
    return v.isoformat() if v else None


# ---------- 幂等框架(archive.* 命名空间) ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def run_archive_action(session: Session, *, action: str, operator: str,
                       idempotency_key: str, payload: dict, fn,
                       archive_id: str | None = None) -> tuple[dict, bool]:
    """与 replay.run_replay_action 同构: fn 在同事务执行, 结果与幂等键一起落库;
    重复提交(含崩溃重放)返回首次结果。请求哈希带归档 id, 同键跨归档/跨动作复用被拒。"""
    req_hash = _hash({"action": f"archive.{action}", "archive_id": archive_id,
                      "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise ArchiveStateError("幂等键被不同请求复用")
        return existing.response_json, True

    archive = lock_archive(session, archive_id) if archive_id is not None else None
    result = fn(session, archive)
    if archive_id is not None:
        fresh = get_archive(session, archive_id)
        result["archive_id"] = archive_id
        result["status"] = fresh.status
    session.add(IdempotencyKey(key=idempotency_key, action=f"archive.{action}",
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


# ---------- 行锁 / 串行锁 ----------

def get_archive(session: Session, archive_id: str) -> ReplayArchive:
    a = session.get(ReplayArchive, archive_id)
    if a is None:
        raise ArchiveNotFound(f"归档任务 {archive_id} 不存在")
    return a


def lock_archive(session: Session, archive_id: str) -> ReplayArchive:
    if session.bind.dialect.name != "sqlite":
        a = session.get(ReplayArchive, archive_id, with_for_update=True)
        if a is not None:
            return a
    return get_archive(session, archive_id)


def _lock_creation(session: Session) -> None:
    """串行化同一(回放, 版本)的重复归档创建竞争。SQLite 写事务天然串行。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260918)"))


def _lock_scheduling(session: Session) -> None:
    """串行化 worker 的并发额度认领。"""
    if session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT pg_advisory_xact_lock(20260917)"))


# ---------- 事件流水(只追加; 归档不写回放事件表与业务审计) ----------

def add_event(session: Session, *, archive_id: str, event: str, operator: str,
              stage: str | None = None, reason: str | None = None,
              detail: dict | None = None) -> None:
    session.add(ReplayArchiveEvent(
        archive_id=archive_id, stage=stage, event=event, operator=operator,
        reason=(reason[:500] if reason else None), detail=detail,
    ))


# ---------- 回放侧的归档活动锁(供 replay 模块在复核/分派入口调用) ----------

def active_archive_for_replay(session: Session,
                              replay_id: str) -> ReplayArchive | None:
    """该回放是否存在活动归档(QUEUED/RUNNING/PAUSED)。有则回放复核数据冻结不可改。"""
    from .models import ARCHIVE_ACTIVE_STATUSES
    return (session.query(ReplayArchive)
            .filter(ReplayArchive.replay_id == replay_id,
                    ReplayArchive.status.in_(ARCHIVE_ACTIVE_STATUSES))
            .order_by(ReplayArchive.created_at, ReplayArchive.id)
            .first())


def assert_replay_archive_lock(session: Session, replay_id: str,
                               action_desc: str) -> None:
    """归档活动期间拒绝修改原回放/复核数据: 复核提交/确认/重新打开/分派入口调用。"""
    a = active_archive_for_replay(session, replay_id)
    if a is not None:
        raise ArchiveStateError(
            f"回放 {replay_id} 的归档任务 {a.id} 正在进行(状态 {a.status}, "
            f"锁定报告版本 v{a.report_version}), {action_desc}被拒绝: "
            f"归档期间不能修改原回放或复核数据; 请等待归档完成或取消该归档任务")


# ---------- 创建归档 ----------

def _existing_for(session: Session, replay_id: str,
                  version: int) -> ReplayArchive | None:
    return (session.query(ReplayArchive)
            .filter(ReplayArchive.replay_id == replay_id,
                    ReplayArchive.report_version == version)
            .order_by(ReplayArchive.created_at.desc(), ReplayArchive.id.desc())
            .first())


def do_create_archive(session: Session, _archive, operator: str,
                      replay_id: str, report_version: int) -> dict:
    """为已 COMPLETED 回放按指定报告版本创建归档任务并排队。

    回放不存在 -> 404; 回放未完成/版本与当前报告版本不一致 -> 409(归档版本冲突)。
    同一(回放, 版本)已有活动或已完成归档时, 重复创建幂等返回已有归档;
    此前 FAILED/CANCELED 的归档保留失败记录, 允许发起新的归档尝试。
    """
    task = session.get(ReplayTask, replay_id)
    if task is None:
        raise ArchiveNotFound(f"回放任务 {replay_id} 不存在, 无法归档")
    if task.status != "COMPLETED":
        raise ArchiveStateError(
            f"回放任务 {replay_id} 当前状态 {task.status}, "
            f"只有 COMPLETED 回放才能生成证据归档")
    if report_version != task.report_version:
        raise ArchiveStateError(
            f"归档版本冲突: 请求归档报告版本 v{report_version}, 回放 {replay_id} "
            f"当前报告版本为 v{task.report_version}; 只能基于当前报告版本归档, "
            f"过期版本的数据不完整")

    _lock_creation(session)
    existing = _existing_for(session, replay_id, report_version)
    if existing is not None and existing.status in (
            "QUEUED", "RUNNING", "PAUSED", "COMPLETED"):
        add_event(session, archive_id=existing.id, event="create.idempotent",
                  operator=operator,
                  reason=f"重复归档请求幂等返回已有归档 {existing.id}({existing.status})")
        session.flush()
        label = {"QUEUED": "已排队", "RUNNING": "执行中", "PAUSED": "已暂停",
                 "COMPLETED": "已完成(归档包不可变)"}[existing.status]
        return {"ok": True, "archive_id": existing.id, "status": existing.status,
                "already_existing": True,
                "detail": f"回放 {replay_id} 报告版本 v{report_version} 已存在{label}归档 "
                          f"{existing.id}, 重复请求无副作用"}

    archive_id = "A" + uuid.uuid4().hex[:10]
    total_units = 1 + task.total_steps + len(TAIL_STAGES)
    a = ReplayArchive(
        id=archive_id, replay_id=replay_id, plan_id=task.plan_id,
        plan_name=task.plan_name, checkpoint_id=task.checkpoint_id,
        report_version=report_version, status="QUEUED",
        total_units=total_units, completed_units=0, current_stage=None,
        staging={"version": 1, "done": [], "data": {}},
        digest_algorithm=DIGEST_ALGORITHM,
        created_by=operator, updated_by=operator)
    session.add(a)
    session.flush()
    add_event(session, archive_id=archive_id, event="create", operator=operator,
              stage=None,
              reason=(f"创建证据归档: 回放 {replay_id}(计划 {task.plan_name}), "
                      f"锁定报告版本 v{report_version}, 共 {total_units} 个归档单元"),
              detail={"replay_id": replay_id, "plan_id": task.plan_id,
                      "checkpoint_id": task.checkpoint_id,
                      "report_version": report_version,
                      "total_units": total_units,
                      "max_concurrency": max_concurrency()})
    add_event(session, archive_id=archive_id, event="queue", operator=operator,
              reason=f"归档任务已排队, 等待并发额度(最多同时 {max_concurrency()} 个归档)")
    session.flush()
    return {"ok": True, "archive_id": archive_id, "status": "QUEUED",
            "already_existing": False,
            "detail": f"归档任务 {archive_id} 已创建并排队: 回放 {replay_id}, "
                      f"锁定报告版本 v{report_version}; 归档全程只读, "
                      f"活动期间该回放的复核与分派将被冻结"}


# ---------- 暂停 / 恢复 / 取消(均幂等) ----------

def _require_status(archive: ReplayArchive, action: str) -> None:
    allowed = ARCHIVE_ALLOWED_ACTIONS[action]
    if archive.status not in allowed:
        raise ArchiveStateError(
            f"归档任务 {archive.id} 当前状态 {archive.status} 不允许 {action}"
            f"(仅 {sorted(allowed)} 状态可执行该操作)")


def do_pause(session: Session, archive: ReplayArchive, operator: str) -> dict:
    if archive.status == "PAUSED":
        return {"ok": True, "already_in_state": True,
                "detail": "归档任务已处于暂停状态, 重复暂停无副作用"}
    _require_status(archive, "pause")
    archive.status = "PAUSED"
    archive.updated_by = operator
    archive.current_stage = None  # 暂停在单元边界, 不存在"当前单元"
    add_event(session, archive_id=archive.id, event="pause", operator=operator,
              reason=("暂停排队中的归档, 恢复后重新排队" if not archive.started_at
                      else "暂停请求已记录, 将在当前归档单元边界停住"))
    return {"ok": True, "detail": "归档任务已暂停, 将在当前归档单元边界停止"}


def do_resume(session: Session, archive: ReplayArchive, operator: str) -> dict:
    if archive.status in ("QUEUED", "RUNNING"):
        return {"ok": True, "already_in_state": True,
                "detail": f"归档任务当前为 {archive.status}, 恢复请求无副作用"}
    _require_status(archive, "resume")
    archive.status = "QUEUED"  # 重新排队, 再次受并发额度约束
    archive.updated_by = operator
    add_event(session, archive_id=archive.id, event="resume", operator=operator,
              reason="恢复归档: 重新排队, 获得并发额度后从首个未完成单元续跑")
    return {"ok": True, "detail": "归档任务已恢复, 已重新排队等待执行"}


def do_cancel(session: Session, archive: ReplayArchive, operator: str) -> dict:
    if archive.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "归档任务已取消, 重复取消无副作用"}
    _require_status(archive, "cancel")
    archive.status = "CANCELED"
    archive.updated_by = operator
    archive.current_stage = None
    archive.finished_at = now_utc_naive()
    add_event(session, archive_id=archive.id, event="cancel", operator=operator,
              reason="归档任务被取消, 已采集的单元产物保留在任务记录中, 不生成归档包")
    return {"ok": True, "detail": "归档任务已取消, 未生成归档包, 任务记录保留可查询"}


# ---------- 调度(并发闸门) ----------

def claim_due_archives(session: Session) -> list[str]:
    """按并发额度把 QUEUED 归档认领为 RUNNING, 返回认领的归档 id(已提交)。"""
    _lock_scheduling(session)
    try:
        running = (session.query(func.count(ReplayArchive.id))
                   .filter(ReplayArchive.status == "RUNNING").scalar()) or 0
        slots = max_concurrency() - running
        if slots <= 0:
            session.rollback()
            return []
        due = (session.query(ReplayArchive)
               .filter(ReplayArchive.status == "QUEUED")
               .order_by(ReplayArchive.created_at, ReplayArchive.id)
               .limit(slots).all())
        claimed: list[str] = []
        at = now_utc_naive()
        for a in due:
            a.status = "RUNNING"
            a.started_at = a.started_at or at
            a.updated_by = "system"
            if not isinstance(a.staging, dict):  # 防御: staging 损坏时重建
                a.staging = {"version": 1, "done": [], "data": {}}
            add_event(session, archive_id=a.id, event="claim", operator="system",
                      reason=f"获得并发额度, 开始执行(并发上限 {max_concurrency()})")
            claimed.append(a.id)
        session.commit()
        return claimed
    except Exception:
        session.rollback()
        raise


# ---------- 执行: 归档单元 ----------

def _stage_seq(seq: int) -> str:
    return f"step:{seq}"


def _planned_stages(archive: ReplayArchive) -> list[str]:
    n = archive.total_units - 1 - len(TAIL_STAGES)
    return [STAGE_VALIDATE] + [_stage_seq(i) for i in range(1, n + 1)] + list(TAIL_STAGES)


def _next_stage(archive: ReplayArchive) -> str | None:
    done = set((archive.staging or {}).get("done") or [])
    for stage in _planned_stages(archive):
        if stage not in done:
            return stage
    return None


def _mark_unit(session: Session, archive: ReplayArchive, stage: str,
               detail_msg: str, detail: dict | None = None) -> None:
    """单元产物已在 archive.staging["data"] 中: 登记完成、推进进度并提交(崩溃边界)。"""
    staging = archive.staging
    done = staging.setdefault("done", [])
    if stage not in done:
        done.append(stage)
    # JSON 列原地变更 SQLAlchemy 不自动跟踪, 显式标记后才会随事务持久化
    flag_modified(archive, "staging")
    archive.completed_units = len(done)
    archive.updated_by = "system"
    add_event(session, archive_id=archive.id, event="unit.done", operator="system",
              stage=stage, reason=detail_msg,
              detail={"completed_units": archive.completed_units,
                      "total_units": archive.total_units, **(detail or {})})
    session.commit()


def _task_snapshot(task: ReplayTask) -> dict:
    return {
        "id": task.id,
        "plan_id": task.plan_id,
        "plan_name": task.plan_name,
        "checkpoint_id": task.checkpoint_id,
        "status": task.status,
        "report_version": task.report_version,
        "review_status": task.review_status,
        "confirmed_by": task.confirmed_by,
        "confirmed_at": _dt(task.confirmed_at),
        "assignee": task.assignee,
        "total_steps": task.total_steps,
        "completed_steps": task.completed_steps,
        "diff_count": task.diff_count,
        "state_diff_count": task.state_diff_count,
        "created_by": task.created_by,
        "started_at": _dt(task.started_at),
        "finished_at": _dt(task.finished_at),
        "created_at": _dt(task.created_at),
    }


def _unit_validate(session: Session, archive: ReplayArchive) -> None:
    """校验单元: 回放仍在且 COMPLETED、版本未冲突、步骤报告齐全、检查点/批次/
    关键审计证据仍存在。任一不满足 -> 明确失败(版本冲突/缺失步骤/数据被删)。"""
    task = session.get(ReplayTask, archive.replay_id)
    if task is None:
        raise _ArchiveFatal(
            "data_deleted",
            f"回放任务 {archive.replay_id} 已被删除, 归档证据源不存在")
    if task.status != "COMPLETED":
        raise _ArchiveFatal(
            "data_deleted",
            f"回放任务 {task.id} 当前状态 {task.status}(需 COMPLETED), "
            f"归档证据源状态已变化")
    if task.report_version != archive.report_version:
        raise _ArchiveFatal(
            "version_conflict",
            f"归档版本冲突: 归档锁定报告版本 v{archive.report_version}, "
            f"回放当前报告版本已变为 v{task.report_version}(复核被重新打开过), "
            f"归档内容无法与指定版本保持一致, 明确失败; 可基于新版本重新发起归档")

    steps = (session.query(ReplayTaskStep)
             .filter(ReplayTaskStep.task_id == task.id)
             .order_by(ReplayTaskStep.seq).all())
    seqs = {s.seq for s in steps}
    missing = [seq for seq in range(1, task.total_steps + 1) if seq not in seqs]
    if missing:
        raise _ArchiveFatal(
            "missing_step",
            f"回放 {task.id} 缺失步骤报告 seq={missing}(任务应有 "
            f"{task.total_steps} 步, 实际 {len(steps)} 步), 归档必须包含全部步骤报告")
    no_report = [s.seq for s in steps
                 if s.status != "SUCCESS" or s.expected_state is None
                 or s.actual_state is None]
    if no_report:
        raise _ArchiveFatal(
            "missing_step",
            f"回放 {task.id} 步骤 seq={no_report} 没有已完成的报告"
            f"(状态/预期或实际状态缺失), 归档必须包含全部步骤报告")

    cp = session.get(ReplayCheckpoint, task.checkpoint_id)
    if cp is None:
        raise _ArchiveFatal(
            "data_deleted",
            f"检查点 {task.checkpoint_id} 已被删除, 归档证据链不完整")
    if cp.status != "COMPLETE":
        raise _ArchiveFatal(
            "data_deleted",
            f"检查点 {cp.id} 状态为 {cp.status}(需 COMPLETE), 归档证据链不完整")
    cp_steps = {cs.plan_step_id: cs for cs in cp.steps}
    missing_cp = []
    deleted_batches = []
    missing_audit: list[int] = []
    for ts in steps:
        cs = cp_steps.get(ts.plan_step_id)
        if cs is None:
            missing_cp.append(ts.seq)
            continue
        for aid in (cs.required_audit_ids or []):
            if session.get(AuditLog, aid) is None:
                missing_audit.append(aid)
        if session.get(MigrationBatch, ts.batch_id) is None:
            deleted_batches.append((ts.seq, ts.batch_id))
    if missing_cp:
        raise _ArchiveFatal(
            "missing_step",
            f"步骤 seq={missing_cp} 的检查点快照已不存在, 步骤报告证据不完整")
    if deleted_batches:
        detail = ", ".join(f"seq={s} 批次 {b}" for s, b in deleted_batches)
        raise _ArchiveFatal(
            "data_deleted",
            f"归档数据被删除: {detail}; 批次证据源不存在, 归档明确失败")
    if missing_audit:
        raise _ArchiveFatal(
            "audit_incomplete",
            f"归档数据被删除: 关键业务审计 {sorted(set(missing_audit))} 已不存在, "
            f"审计链不完整, 归档明确失败")

    archive.staging.setdefault("data", {})["task"] = _task_snapshot(task)
    _mark_unit(session, archive, STAGE_VALIDATE,
               f"校验通过: 回放 COMPLETED、报告版本 v{archive.report_version} 未冲突、"
               f"{len(steps)} 个步骤报告齐全、检查点/批次/关键审计证据完整",
               {"steps": len(steps)})


def _unit_step(session: Session, archive: ReplayArchive, seq: int) -> None:
    """逐步报告单元: 固化该步骤完整报告(预期/实际/差异)与检查点快照摘要。"""
    ts = (session.query(ReplayTaskStep)
          .filter(ReplayTaskStep.task_id == archive.replay_id,
                  ReplayTaskStep.seq == seq).first())
    if ts is None:
        raise _ArchiveFatal(
            "missing_step",
            f"归档执行期间步骤 seq={seq} 的报告被删除, 归档无法包含该步骤")
    cp = session.get(ReplayCheckpoint, archive.checkpoint_id)
    cs = (session.query(ReplayCheckpointStep)
          .filter(ReplayCheckpointStep.checkpoint_id == archive.checkpoint_id,
                  ReplayCheckpointStep.plan_step_id == ts.plan_step_id).first()
          if cp is not None else None)
    if cs is None:
        raise _ArchiveFatal(
            "missing_step",
            f"步骤 seq={seq} 的检查点快照已不存在, 步骤报告证据不完整")
    record = {
        "seq": ts.seq,
        "plan_step_id": ts.plan_step_id,
        "batch_id": ts.batch_id,
        "depends_on": ts.depends_on or [],
        "status": ts.status,
        "expected_state": ts.expected_state,
        "actual_state": ts.actual_state,
        "diffs": ts.diffs or [],
        "state_diffs": ts.state_diffs or [],
        "diff_count": ts.diff_count,
        "state_diff_count": ts.state_diff_count,
        "started_at": _dt(ts.started_at),
        "finished_at": _dt(ts.finished_at),
        "checkpoint_evidence": {
            "checkpoint_step_id": cs.id,
            "batch_snapshot": {
                "phase": cs.batch_phase, "epoch": cs.batch_epoch,
                "active_schema": cs.batch_active_schema,
                "watermark": cs.batch_watermark,
                "freeze_version": cs.batch_freeze_version,
            },
            "required_audit_ids": cs.required_audit_ids or [],
            "old_count": cs.old_count, "new_count": cs.new_count,
        },
    }
    data = archive.staging.setdefault("data", {})
    data.setdefault("steps", {})[str(seq)] = record
    _mark_unit(session, archive, _stage_seq(seq),
               f"步骤 seq={seq}(批次 {ts.batch_id})报告已固化: "
               f"{ts.diff_count} 处字段差异, {ts.state_diff_count} 处批次状态差异")


def _review_to_dict(rv: ReplayReview) -> dict:
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


def _unit_reviews(session: Session, archive: ReplayArchive) -> None:
    """复核结论单元: 指定(锁定)版本的逐步结论 + 全部历史版本结论, 并再次核验版本。"""
    task = session.get(ReplayTask, archive.replay_id)
    if task is None:
        raise _ArchiveFatal("data_deleted",
                            f"回放任务 {archive.replay_id} 在归档期间被删除")
    if task.report_version != archive.report_version:
        raise _ArchiveFatal(
            "version_conflict",
            f"归档版本冲突: 归档锁定 v{archive.report_version}, 回放当前为 "
            f"v{task.report_version}(复核在归档期间被重新打开), 归档明确失败")
    current = (session.query(ReplayReview)
               .filter(ReplayReview.task_id == task.id,
                       ReplayReview.report_version == archive.report_version)
               .order_by(ReplayReview.step_seq).all())
    history = (session.query(ReplayReview)
               .filter(ReplayReview.task_id == task.id)
               .order_by(ReplayReview.report_version, ReplayReview.step_seq).all())
    payload = {
        "archived_report_version": archive.report_version,
        "current_report_version": task.report_version,
        "review_status_at_archive": task.review_status,
        "confirmed_by": task.confirmed_by,
        "confirmed_at": _dt(task.confirmed_at),
        "assignee": task.assignee,
        "current_version_conclusions": [_review_to_dict(r) for r in current],
        "history": [_review_to_dict(r) for r in history],
        "current_version_counts": {
            "total": len(current),
            "passed": sum(1 for r in current if r.verdict == "PASS"),
            "failed": sum(1 for r in current if r.verdict == "FAIL"),
        },
    }
    archive.staging.setdefault("data", {})["reviews"] = payload
    _mark_unit(session, archive, STAGE_REVIEWS,
               f"复核结论已固化: v{archive.report_version} 共 {len(current)} 条"
               f"(PASS {payload['current_version_counts']['passed']}, "
               f"FAIL {payload['current_version_counts']['failed']}), "
               f"全量历史 {len(history)} 条")


def _assignment_to_dict(a: ReplayAssignment) -> dict:
    return {
        "id": a.id,
        "task_id": a.task_id,
        "assignee": a.assignee,
        "operator": a.operator,
        "report_version": a.report_version,
        "reason": a.reason,
        "created_at": _dt(a.created_at),
    }


def _unit_assignments(session: Session, archive: ReplayArchive) -> None:
    """分派历史单元: 全量只追加的复核分派/改派记录。"""
    rows = (session.query(ReplayAssignment)
            .filter(ReplayAssignment.task_id == archive.replay_id)
            .order_by(ReplayAssignment.id).all())
    archive.staging.setdefault("data", {})["assignments"] = {
        "replay_id": archive.replay_id,
        "history": [_assignment_to_dict(a) for a in rows],
        "total": len(rows),
    }
    _mark_unit(session, archive, STAGE_ASSIGNMENTS,
               f"复核分派历史已固化: {len(rows)} 条分派/改派记录")


def _audit_entry(r: AuditLog) -> dict:
    return {
        "id": r.id,
        "ts": _dt(r.ts),
        "batch_id": r.batch_id,
        "plan_id": r.plan_id,
        "step_id": r.step_id,
        "operator": r.operator,
        "action": r.action,
        "from_phase": r.from_phase,
        "to_phase": r.to_phase,
        "epoch": r.epoch,
        "app_version": r.app_version,
        "freeze_version": r.freeze_version,
        "watermark": r.watermark,
        "reason": r.reason,
    }


def _unit_audit_summary(session: Session, archive: ReplayArchive) -> None:
    """审计摘要单元: 计划级审计 + 每步批次关键 freeze/cutover 证据 + 计数;
    关键审计缺失(数据被删) -> audit_incomplete 明确失败。"""
    task = session.get(ReplayTask, archive.replay_id)
    if task is None:
        raise _ArchiveFatal("data_deleted",
                            f"回放任务 {archive.replay_id} 在归档期间被删除")
    cp = session.get(ReplayCheckpoint, task.checkpoint_id)
    if cp is None:
        raise _ArchiveFatal("data_deleted",
                            f"检查点 {task.checkpoint_id} 已被删除")
    plan_rows = (session.query(AuditLog)
                 .filter(AuditLog.plan_id == task.plan_id)
                 .order_by(AuditLog.id).all())
    plan_counts: dict[str, int] = {}
    for r in plan_rows:
        plan_counts[r.action] = plan_counts.get(r.action, 0) + 1

    steps = (session.query(ReplayTaskStep)
             .filter(ReplayTaskStep.task_id == task.id)
             .order_by(ReplayTaskStep.seq).all())
    cp_by_plan_step = {cs.plan_step_id: cs for cs in cp.steps}
    step_summaries = []
    missing_audit: list[int] = []
    total_batch_entries = 0
    for ts in steps:
        cs = cp_by_plan_step.get(ts.plan_step_id)
        required = list(cs.required_audit_ids) if cs else []
        rows = (session.query(AuditLog)
                .filter(AuditLog.batch_id == ts.batch_id)
                .order_by(AuditLog.id).all()) if session.get(
                    MigrationBatch, ts.batch_id) is not None else None
        if rows is None:
            raise _ArchiveFatal(
                "data_deleted",
                f"步骤 seq={ts.seq} 的批次 {ts.batch_id} 已被删除, 审计摘要无法生成")
        total_batch_entries += len(rows)
        by_id = {r.id: r for r in rows}
        for aid in required:
            if aid not in by_id:
                missing_audit.append(aid)
        freeze = next((r for r in rows if r.action == "freeze"), None)
        cutover = next((r for r in rows if r.action == "cutover"), None)

        def _light(r):
            return None if r is None else {
                "id": r.id, "ts": _dt(r.ts), "operator": r.operator,
                "epoch": r.epoch, "app_version": r.app_version,
                "freeze_version": r.freeze_version, "watermark": r.watermark}

        step_summaries.append({
            "seq": ts.seq,
            "batch_id": ts.batch_id,
            "required_audit_ids": required,
            "batch_audit_count": len(rows),
            "freeze": _light(freeze),
            "cutover": _light(cutover),
        })
    if missing_audit:
        raise _ArchiveFatal(
            "audit_incomplete",
            f"归档数据被删除: 关键业务审计 {sorted(set(missing_audit))} 已不存在, "
            f"审计链不完整, 归档明确失败")

    summary = {
        "replay_id": task.id,
        "plan_id": task.plan_id,
        "checkpoint_id": cp.id,
        "checkpoint_audit_cursor_id": cp.audit_cursor_id,
        "generated_at": _dt(now_utc_naive()),
        "total_audit_entries": len(plan_rows) + total_batch_entries,
        "plan_audit": {
            "count": len(plan_rows),
            "count_by_action": plan_counts,
            "entries": [_audit_entry(r) for r in plan_rows],
        },
        "steps": step_summaries,
        "issues": [],
    }
    archive.staging.setdefault("data", {})["audit_summary"] = summary
    _mark_unit(session, archive, STAGE_AUDIT_SUMMARY,
               f"审计摘要已固化: 计划级 {len(plan_rows)} 条, "
               f"批次级 {total_batch_entries} 条, 各步 freeze/cutover 证据齐全")


# ---------- 打包: 确定性 zip + 逐文件摘要 + 内容摘要 ----------

def _canonical_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2,
                       default=str) + "\n").encode("utf-8")


def _build_payloads(archive: ReplayArchive) -> dict[str, bytes]:
    data = archive.staging["data"]
    at = _dt(now_utc_naive())
    metadata = {
        "archive_id": archive.id,
        "replay_id": archive.replay_id,
        "plan_id": archive.plan_id,
        "plan_name": archive.plan_name,
        "checkpoint_id": archive.checkpoint_id,
        "report_version": archive.report_version,
        "digest_algorithm": DIGEST_ALGORITHM,
        "archived_by": archive.created_by,
        "archived_at": at,
        "app_version": _app_version(),
        "contents": list(PAYLOAD_FILES) + [MANIFEST_FILE],
        "immutable": True,
    }
    steps = data.get("steps") or {}
    steps_list = [steps[str(i)] for i in sorted(int(k) for k in steps)]
    return {
        "metadata.json": _canonical_bytes(metadata),
        "replay/task.json": _canonical_bytes(data["task"]),
        "replay/steps.json": _canonical_bytes({
            "replay_id": archive.replay_id,
            "report_version": archive.report_version,
            "total": len(steps_list),
            "steps": steps_list,
        }),
        "review/conclusions.json": _canonical_bytes(data["reviews"]),
        "review/assignments.json": _canonical_bytes(data["assignments"]),
        "audit/summary.json": _canonical_bytes(data["audit_summary"]),
    }


def _app_version() -> str:
    from .service import APP_VERSION
    return APP_VERSION


def _content_digest(files: dict[str, bytes]) -> tuple[str, dict[str, dict]]:
    """逐文件 sha256 + 内容摘要(文件名有序拼接, 仿 git 风格)。"""
    meta = {}
    h = hashlib.sha256()
    for name in sorted(files):
        blob = files[name]
        digest = hashlib.sha256(blob).hexdigest()
        meta[name] = {"size": len(blob), DIGEST_ALGORITHM: digest}
        h.update(f"{digest}  {name}\n".encode("utf-8"))
    return h.hexdigest(), meta


def _write_zip(path: str, files: dict[str, bytes], manifest: dict) -> None:
    """确定性写包: 固定 zip 时间戳、按文件名排序写入, 同载荷字节级一致。
    先写临时文件再原子替换, 避免崩溃留下半包。"""
    ensure_store_dir()
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in sorted(list(files) + [MANIFEST_FILE]):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                blob = files[name] if name in files else _canonical_bytes(manifest)
                zf.writestr(info, blob)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _unit_package(session: Session, archive: ReplayArchive) -> None:
    """打包单元: 组装载荷 -> 摘要 -> 写 zip -> 回读自检 -> 落库清单。"""
    try:
        files = _build_payloads(archive)
    except KeyError as e:
        raise _ArchiveFatal(
            "staging_incomplete",
            f"归档暂存数据缺少必要单元产物 {e}, 无法打包(请取消后重新发起归档)")
    content_digest, file_meta = _content_digest(files)
    manifest = {
        "archive_id": archive.id,
        "replay_id": archive.replay_id,
        "report_version": archive.report_version,
        "digest_algorithm": DIGEST_ALGORITHM,
        "created_at": _dt(now_utc_naive()),
        "files": file_meta,
        "content_digest": content_digest,
    }
    path = os.path.join(store_dir(), f"{archive.id}.zip")
    _write_zip(path, files, manifest)

    # 回读自检: 刚写出的包必须能通过摘要校验, 否则明确失败
    check = verify_package_bytes(path, expected_manifest=manifest,
                                 expected_content_digest=content_digest)
    if not check["valid"]:
        raise _ArchiveFatal(
            "digest_mismatch",
            f"归档包写出后自检失败(摘要不一致): {'; '.join(check['issues'])}")

    archive.manifest = manifest
    archive.content_digest = content_digest
    archive.package_path = path
    archive.package_size = os.path.getsize(path)
    archive.staging = None  # 包已不可变, 暂存清空
    archive.completed_units = archive.total_units
    archive.status = "COMPLETED"
    archive.current_stage = None
    archive.finished_at = now_utc_naive()
    archive.failure_code = None
    archive.failure_reason = None
    archive.updated_by = "system"
    add_event(session, archive_id=archive.id, event="complete", operator="system",
              stage=STAGE_PACKAGE,
              reason=(f"归档完成: 不可变归档包已生成({archive.package_size} 字节), "
                      f"内容摘要 {DIGEST_ALGORITHM}:{content_digest[:16]}…, "
                      f"可下载与重新校验"),
              detail={"package_path": path, "package_size": archive.package_size,
                      "content_digest": content_digest,
                      "files": sorted(file_meta)})
    session.commit()


def _fail_archive(session: Session, archive: ReplayArchive, code: str,
                  reason: str, stage: str | None = None,
                  operator: str = "system") -> None:
    """致命失败收尾: 任务 FAILED 并保留失败记录(含暂存产物与事件流水)。"""
    archive = lock_archive(session, archive.id)
    if archive.status in ("CANCELED", "COMPLETED", "FAILED"):
        session.rollback()
        return
    archive.status = "FAILED"
    archive.failure_code = code
    archive.failure_reason = f"[{code}] {reason}"[:500]
    archive.current_stage = None
    archive.finished_at = now_utc_naive()
    archive.updated_by = operator
    add_event(session, archive_id=archive.id, event="failed", operator=operator,
              stage=stage, reason=archive.failure_reason,
              detail={"code": code,
                      "completed_units": archive.completed_units,
                      "total_units": archive.total_units})
    session.commit()


def run_archive_tick(session: Session, archive_id: str) -> bool:
    """推进一个 RUNNING 归档任务至多一个归档单元, 返回是否执行了单元。
    暂停/取消在单元边界(下次 tick 入口)生效。"""
    archive = lock_archive(session, archive_id)
    if archive.status != "RUNNING":
        session.rollback()
        return False
    stage = _next_stage(archive)
    if stage is None:
        session.rollback()
        return False
    archive.current_stage = stage
    archive.updated_by = "system"
    add_event(session, archive_id=archive_id, event="unit.start", operator="system",
              stage=stage)
    session.commit()  # 单元边界: 当前单元先留痕, 崩溃重启后从同一未完成单元重跑

    try:
        if stage == STAGE_VALIDATE:
            _unit_validate(session, archive)
        elif stage.startswith("step:"):
            _unit_step(session, archive, int(stage.split(":", 1)[1]))
        elif stage == STAGE_REVIEWS:
            _unit_reviews(session, archive)
        elif stage == STAGE_ASSIGNMENTS:
            _unit_assignments(session, archive)
        elif stage == STAGE_AUDIT_SUMMARY:
            _unit_audit_summary(session, archive)
        elif stage == STAGE_PACKAGE:
            # 打包单元内部自行收尾为 COMPLETED(含暂存清理)
            _unit_package(session, archive)
            return True
    except _ArchiveFatal as e:
        session.rollback()
        _fail_archive(session, archive, e.code, e.reason, stage=stage)
        return True
    except Exception as e:  # 防御: 意外错误也明确失败留痕
        session.rollback()
        _fail_archive(session, archive, "internal_error",
                      f"归档执行时发生未预期错误: {e}", stage=stage)
        return True

    # 普通单元完成后尊重控制状态: pause 在边界停住(状态已由管理动作置位, 不覆盖);
    # cancel 已在管理动作中收尾为终态。重新取行, 只更新下一单元提示。
    archive = lock_archive(session, archive_id)
    if archive.status == "RUNNING":
        nxt = _next_stage(archive)
        archive.current_stage = nxt
        session.commit()
    else:
        session.rollback()
    return True


# ---------- 摘要校验 / 下载 ----------

def verify_package_bytes(path: str, *, expected_manifest: dict | None = None,
                         expected_content_digest: str | None = None) -> dict:
    """回读 zip 重算逐文件摘要与内容摘要并比对。返回校验结果(不抛异常)。"""
    issues: list[str] = []
    if not os.path.exists(path):
        return {"valid": False, "issues": ["归档包文件不存在(可能已被删除)"],
                "reason_code": "package_missing", "files": {}}
    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                issues.append(f"zip 压缩包损坏, 首个坏块: {bad}")
            names = set(zf.namelist())
            blobs: dict[str, bytes] = {}
            for name in names:
                blobs[name] = zf.read(name)
    except zipfile.BadZipFile:
        return {"valid": False, "issues": ["归档包不是合法的 zip 文件(已损坏或被篡改)"],
                "reason_code": "package_corrupt", "files": {}}

    expected_names = set(PAYLOAD_FILES) | {MANIFEST_FILE}
    extra = sorted(names - expected_names)
    missing = sorted(expected_names - names)
    if extra:
        issues.append(f"归档包出现清单外文件(疑似篡改): {extra}")
    if missing:
        issues.append(f"归档包缺失必要文件: {missing}")

    manifest = expected_manifest
    if manifest is None and MANIFEST_FILE in blobs:
        try:
            manifest = json.loads(blobs[MANIFEST_FILE].decode("utf-8"))
        except Exception as e:
            issues.append(f"manifest.json 无法解析: {e}")
    files_check: dict[str, dict] = {}
    recomputed_digest = None
    payload = {n: b for n, b in blobs.items() if n in PAYLOAD_FILES}
    if payload:
        recomputed_digest, _ = _content_digest(payload)

    for name in sorted(names & expected_names - {MANIFEST_FILE}):
        actual = hashlib.sha256(blobs[name]).hexdigest()
        expected = None
        if isinstance(manifest, dict):
            entry = (manifest.get("files") or {}).get(name) or {}
            expected = entry.get(DIGEST_ALGORITHM)
        ok = expected is None or actual == expected
        if not ok:
            issues.append(f"文件 {name} 摘要不一致(期望 {expected[:12]}…, 实际 {actual[:12]}…)")
        files_check[name] = {"ok": ok, "expected": expected, "actual": actual}

    want = expected_content_digest
    if want is None and isinstance(manifest, dict):
        want = manifest.get("content_digest")
    if want is not None and recomputed_digest != want:
        issues.append(f"内容摘要不一致(期望 {str(want)[:16]}…, 重算 {str(recomputed_digest)[:16]}…)")

    valid = not issues
    return {
        "valid": valid,
        "issues": issues,
        "reason_code": None if valid else (
            "package_missing" if not os.path.exists(path) else "digest_mismatch"),
        "content_digest": want,
        "recomputed_content_digest": recomputed_digest,
        "files": files_check,
        "extra_files": extra,
        "missing_files": missing,
    }


def verify_archive(session: Session, archive: ReplayArchive,
                   operator: str = "system") -> dict:
    """归档包摘要校验: 重算并比对; 不一致/包缺失则把 COMPLETED 归档明确置为
    FAILED(digest_mismatch/package_missing)并保留失败记录与事件。"""
    if archive.status not in ("COMPLETED", "FAILED") or not archive.package_path:
        raise ArchiveStateError(
            f"归档任务 {archive.id} 当前状态 {archive.status}, 尚无归档包可校验"
            f"(仅 COMPLETED 归档提供下载与校验)")
    result = verify_package_bytes(
        archive.package_path,
        expected_content_digest=archive.content_digest,
        expected_manifest=archive.manifest)
    result["archive_id"] = archive.id
    result["status"] = archive.status
    result["digest_algorithm"] = archive.digest_algorithm
    result["content_digest"] = archive.content_digest
    if result["valid"]:
        add_event(session, archive_id=archive.id, event="verify.ok",
                  operator=operator,
                  reason=f"摘要校验通过: {archive.digest_algorithm}:{archive.content_digest}")
        session.commit()
        return result
    # 摘要不一致/包被删: 明确失败并保留失败记录(包原样保留以便取证, 不删除)
    code = result.get("reason_code") or "digest_mismatch"
    reason = "; ".join(result["issues"])
    if archive.status == "COMPLETED":
        archive.status = "FAILED"
        archive.failure_code = code
        archive.failure_reason = f"[{code}] {reason}"[:500]
        archive.finished_at = archive.finished_at or now_utc_naive()
        archive.updated_by = operator
    add_event(session, archive_id=archive.id, event="verify.failed",
              operator=operator,
              reason=(f"摘要校验失败[{code}]: {reason}; 归档已标记 FAILED 并保留记录"
                      + ("(原归档包保留以便取证)" if os.path.exists(
                          archive.package_path or "") else "")))
    session.commit()
    result["status"] = archive.status
    return result


def package_path_for_download(session: Session, archive: ReplayArchive,
                              operator: str = "system") -> str:
    """下载前解析包路径并做存在性检查; COMPLETED 包缺失则明确失败并记录。"""
    if archive.status not in ("COMPLETED", "FAILED") or not archive.package_path:
        raise ArchiveStateError(
            f"归档任务 {archive.id} 当前状态 {archive.status}, 尚无归档包可下载")
    if not os.path.exists(archive.package_path):
        if archive.status == "COMPLETED":
            archive.status = "FAILED"
            archive.failure_code = "package_missing"
            archive.failure_reason = (
                "[package_missing] 归档包文件已不存在(可能被外部删除), "
                "下载被拒绝并记录失败")[:500]
            archive.updated_by = operator
            add_event(session, archive_id=archive.id, event="download.missing",
                      operator=operator,
                      reason=archive.failure_reason,
                      detail={"package_path": archive.package_path})
            session.commit()
        raise ArchiveNotFound(f"归档 {archive.id} 的归档包文件不存在")
    add_event(session, archive_id=archive.id, event="download", operator=operator,
              detail={"package_path": archive.package_path})
    session.commit()
    return archive.package_path


# ---------- 重启对账 ----------

def boot_recover_archives(session: Session) -> None:
    """服务重启时: 遗留 RUNNING 归档(执行线程已死)回到 QUEUED 重新参与排队;
    已完成单元在 staging 中保留, worker 从首个未完成单元续跑。
    QUEUED/PAUSED/终态任务保持不变(排队/暂停/失败记录都是持久化的用户态)。"""
    rows = session.query(ReplayArchive).order_by(ReplayArchive.id).all()
    for a in rows:
        if a.status != "RUNNING":
            continue
        a.status = "QUEUED"
        a.updated_by = "system"
        done = list((a.staging or {}).get("done") or [])
        a.completed_units = len(done)
        a.current_stage = None
        add_event(session, archive_id=a.id, event="boot", operator="system",
                  reason=(f"服务重启: RUNNING 归档回到排队位置, 已完成 "
                          f"{len(done)}/{a.total_units} 个单元, "
                          f"worker 将从未完成单元续跑, 失败记录与已完成归档不受影响"))
        session.commit()


# ---------- 后台 worker ----------

class ArchiveWorker:
    """单实例后台线程: 认领排队归档(受并发闸门)并各推进一个单元。

    与 ReplayWorker 相同的单副本假设: SQLite 写锁 / Postgres 咨询锁与行锁
    串行化 worker 与管理动作, 不会越过并发上限或重复执行单元。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("ARCHIVE_WORKER_POLL_INTERVAL", "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="archive-worker",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        """认领到期归档并给每个 RUNNING 归档(含刚认领的)推进一个单元。"""
        from .db import SessionLocal
        db = SessionLocal()
        try:
            claimed = claim_due_archives(db)
            run_ids = [r[0] for r in (db.query(ReplayArchive.id)
                                      .filter(ReplayArchive.status == "RUNNING")
                                      .order_by(ReplayArchive.id).all())]
        finally:
            db.close()
        ids = list(claimed) + [i for i in run_ids if i not in claimed]
        advanced = 0
        for aid in ids:
            db = SessionLocal()
            try:
                if run_archive_tick(db, aid):
                    advanced += 1
            except Exception:  # worker 绝不因单个归档异常退出; 失败已在任务上留痕
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

def archive_to_dict(session: Session, a: ReplayArchive, *,
                    with_events: bool = True) -> dict:
    out = {
        "id": a.id,
        "replay_id": a.replay_id,
        "plan_id": a.plan_id,
        "plan_name": a.plan_name,
        "checkpoint_id": a.checkpoint_id,
        "report_version": a.report_version,
        "status": a.status,
        "progress": {"done": a.completed_units, "total": a.total_units},
        "completed_units": a.completed_units,
        "total_units": a.total_units,
        "current_stage": a.current_stage,
        "failure_code": a.failure_code,
        "failure_reason": a.failure_reason,
        "digest_algorithm": a.digest_algorithm,
        "content_digest": a.content_digest,
        "package_size": a.package_size,
        "package_available": bool(a.package_path and os.path.exists(a.package_path)),
        "manifest": a.manifest,
        "stages_planned": _planned_stages(a),
        "stages_done": list((a.staging or {}).get("done") or []),
        "created_by": a.created_by,
        "updated_by": a.updated_by,
        "started_at": _dt(a.started_at),
        "finished_at": _dt(a.finished_at),
        "created_at": _dt(a.created_at),
        "updated_at": _dt(a.updated_at),
        "concurrency_limit": max_concurrency(),
    }
    if with_events:
        out["events"] = [
            {"id": e.id, "ts": _dt(e.ts), "stage": e.stage, "event": e.event,
             "operator": e.operator, "reason": e.reason, "detail": e.detail}
            for e in (session.query(ReplayArchiveEvent)
                      .filter(ReplayArchiveEvent.archive_id == a.id)
                      .order_by(ReplayArchiveEvent.id.desc()).limit(100).all())
        ]
    return out
