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
    AuditLog, ArchiveDigestMember, IdempotencyKey, MigrationBatch,
    MigrationPlan, ReplayArchive, ReplayArchiveEvent, ReplayAssignment,
    ReplayCheckpoint, ReplayCheckpointStep, ReplayReview, ReplayTask,
    ReplayTaskStep,
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
                    ReplayArchive.status.in_(ARCHIVE_ACTIVE_STATUSES),
                    ReplayArchive.cleaned_at.is_(None))
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
                    ReplayArchive.report_version == version,
                    ReplayArchive.cleaned_at.is_(None))
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
    # 归档目录检索用: 固化回放各步骤关联批次的业务分组集合(有序去重)
    biz_groups = _replay_biz_groups(session, task.id)
    a = ReplayArchive(
        id=archive_id, replay_id=replay_id, plan_id=task.plan_id,
        plan_name=task.plan_name, checkpoint_id=task.checkpoint_id,
        report_version=report_version, status="QUEUED",
        total_units=total_units, completed_units=0, current_stage=None,
        staging={"version": 1, "done": [], "data": {}},
        digest_algorithm=DIGEST_ALGORITHM,
        biz_groups=biz_groups,
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
    # 同摘要去重: 物理包只保留一份(canonical 成员的文件), 其余成员共享该路径;
    # 引用关系落 archive_digest_members, 原(canonical)归档删除前必须先移交文件,
    # 仍被引用的记录与文件不受影响。
    canonical_path = _register_digest_member(session, archive, content_digest, path)
    if canonical_path != path:
        # 已有同摘要成员: 删除本任务刚写出的副本, 共享 canonical 的物理文件
        try:
            os.remove(path)
        except OSError:
            pass
        archive.package_path = canonical_path
    else:
        archive.package_path = path
    archive.package_size = os.path.getsize(archive.package_path)
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
    FAILED(digest_mismatch/package_missing)并保留失败记录与事件。

    校验全程持有使用计数(active_verifies+1), 与清理并发协调: 归档正在校验时
    清理逐项跳过(in_use_verify), 物理文件绝不被删除。"""
    if archive.status not in ("COMPLETED", "FAILED") or not archive.package_path:
        raise ArchiveStateError(
            f"归档任务 {archive.id} 当前状态 {archive.status}, 尚无归档包可校验"
            f"(仅 COMPLETED 归档提供下载与校验)")
    begin_archive_use(session, archive, "verify")
    try:
        return _verify_archive_locked(session, archive, operator)
    finally:
        end_archive_use(session, archive.id, "verify")


def _verify_archive_locked(session: Session, archive: ReplayArchive,
                           operator: str) -> dict:
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
    """下载前解析包路径、登记在途下载并做存在性检查; COMPLETED 包缺失则明确
    失败并记录。使用计数(active_downloads+1)必须在响应发送完毕后由调用方
    end_archive_use 释放; 期间清理计划逐项跳过(in_use_download)。"""
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
    begin_archive_use(session, archive, "download")
    add_event(session, archive_id=archive.id, event="download", operator=operator,
              detail={"package_path": archive.package_path})
    session.commit()
    return archive.package_path


# ---------- 归档目录: 业务分组 / 同摘要去重与引用关系 ----------

def _replay_biz_groups(session: Session, replay_id: str) -> list[str]:
    """从回放步骤关联批次反查业务分组集合(有序去重), 归档时固化供目录检索。"""
    rows = (session.query(MigrationBatch.biz)
            .join(ReplayTaskStep,
                  ReplayTaskStep.batch_id == MigrationBatch.id)
            .filter(ReplayTaskStep.task_id == replay_id)
            .order_by(ReplayTaskStep.seq).all())
    seen: set[str] = set()
    groups: list[str] = []
    for (biz,) in rows:
        if biz and biz not in seen:
            seen.add(biz)
            groups.append(biz)
    return groups


def _register_digest_member(session: Session, archive: ReplayArchive,
                            content_digest: str, path: str) -> str:
    """归档完成时登记摘要成员关系(同事务)。同摘要已有存活 canonical 成员则复用
    其物理文件(返回 canonical 路径); 否则本归档成为 canonical(返回 path)。

    极端情况下原 canonical 已软清理(文件移交失败/手工干预): 由最早的存活成员
    接管 canonical, 其包文件必须存在且摘要匹配, 不匹配则本归档文件接管。"""
    members = (session.query(ArchiveDigestMember)
               .filter(ArchiveDigestMember.content_digest == content_digest)
               .order_by(ArchiveDigestMember.id).all())
    live = []
    for m in members:
        other = session.get(ReplayArchive, m.archive_id)
        if other is not None and other.cleaned_at is None and other.package_path:
            live.append((m, other))
    for m, other in live:
        if m.is_canonical and os.path.exists(other.package_path):
            if not session.get(ArchiveDigestMember,
                              _member_pk_for_archive(session, archive.id)):
                session.add(ArchiveDigestMember(
                    content_digest=content_digest, archive_id=archive.id,
                    is_canonical=False))
            add_event(session, archive_id=archive.id, event="digest.dedup",
                      operator="system",
                      reason=(f"内容摘要与归档 {other.id} 相同, 已去重并共享物理归档包: "
                              f"引用关系保留, 本归档引用数计入同摘要组"),
                      detail={"content_digest": content_digest,
                              "canonical_archive_id": other.id,
                              "shared_path": other.package_path})
            return other.package_path
    # 无可用 canonical: 本归档接管
    for m, _other in live:
        m.is_canonical = False
    session.add(ArchiveDigestMember(
        content_digest=content_digest, archive_id=archive.id,
        is_canonical=True))
    add_event(session, archive_id=archive.id, event="digest.register",
              operator="system",
              reason=(f"内容摘要登记: {content_digest[:16]}…, "
                      f"本归档为该摘要的规范(canonical)成员, 持有物理归档包"),
              detail={"content_digest": content_digest})
    return path


def _member_pk_for_archive(session: Session, archive_id: str) -> int | None:
    m = (session.query(ArchiveDigestMember.id)
         .filter(ArchiveDigestMember.archive_id == archive_id).first())
    return m[0] if m else None


def digest_group_view(session: Session, content_digest: str) -> dict:
    """同摘要归档组视图: canonical、全部成员(含引用关系与是否存活)、存活引用数。"""
    members = (session.query(ArchiveDigestMember)
               .filter(ArchiveDigestMember.content_digest == content_digest)
               .order_by(ArchiveDigestMember.id).all())
    refs = []
    live_refs = 0
    canonical_id = None
    for m in members:
        a = session.get(ReplayArchive, m.archive_id)
        alive = bool(a and a.cleaned_at is None)
        if m.is_canonical:
            canonical_id = m.archive_id
        if alive:
            live_refs += 1
        refs.append({
            "archive_id": m.archive_id,
            "is_canonical": m.is_canonical,
            "alive": alive,
            "cleaned_at": _dt(a.cleaned_at) if a else None,
            "replay_id": a.replay_id if a else None,
            "report_version": a.report_version if a else None,
            "created_at": _dt(a.created_at) if a else None,
        })
    return {"content_digest": content_digest,
            "canonical_archive_id": canonical_id,
            "reference_count": live_refs,
            "members": refs}


# ---------- 保留策略(到期保留 / 永久保留标记) ----------

RETENTION_MODES = ("NONE", "UNTIL", "PERMANENT")


def is_retained(archive: ReplayArchive, at=None) -> tuple[bool, str | None, str | None]:
    """归档当前是否受保留策略保护。返回(受保护, 原因码, 说明)。
    软清理/非 COMPLETED 的归档不参与保留判定。"""
    if archive.cleaned_at is not None or archive.status != "COMPLETED":
        return False, None, None
    if archive.retention_mode == "PERMANENT":
        return True, "retained_permanent", "归档带有永久保留标记"
    if archive.retention_mode == "UNTIL":
        until = archive.retain_until
        cur = at or now_utc_naive()
        if until is not None and until > cur:
            return True, "retained_until", (
                f"归档处于保留期内(保留至 {until.isoformat()} UTC)")
    return False, None, None


def retention_dict(archive: ReplayArchive) -> dict:
    retained, code, reason = is_retained(archive)
    return {
        "mode": archive.retention_mode or "NONE",
        "retain_until": _dt(archive.retain_until),
        "set_by": archive.retention_set_by,
        "set_at": _dt(archive.retention_set_at),
        "retained": retained,
        "retain_reason": code,
        "retain_detail": reason,
        "expired": (archive.retention_mode == "UNTIL" and not retained
                    and archive.cleaned_at is None and archive.status == "COMPLETED"),
    }


def set_retention(session: Session, archive: ReplayArchive, operator: str,
                  mode: str, retain_until=None) -> dict:
    """为已完成归档设置保留策略: UNTIL(必须带未来的到期时间)/PERMANENT/NONE(清除)。
    策略持久化, 服务重启后保留; 仍在保留期或永久保留的归档会被清理逐项跳过。"""
    if archive.cleaned_at is not None:
        raise ArchiveStateError(f"归档 {archive.id} 已被清理, 不能再设置保留策略")
    if archive.status != "COMPLETED":
        raise ArchiveStateError(
            f"归档任务 {archive.id} 当前状态 {archive.status}, "
            f"只有 COMPLETED 归档才能设置保留策略")
    if mode not in RETENTION_MODES:
        raise ArchiveStateError(
            f"保留模式必须是 {list(RETENTION_MODES)} 之一(收到 {mode!r})")
    until_dt = None
    if mode == "UNTIL":
        if retain_until is None:
            raise ArchiveStateError("保留模式 UNTIL 必须提供到期时间 retain_until")
        from datetime import datetime
        try:
            until_dt = (retain_until if isinstance(retain_until, datetime)
                        else datetime.fromisoformat(
                            str(retain_until).replace("Z", "+00:00")))
        except (ValueError, TypeError):
            raise ArchiveStateError(f"retain_until 不是合法的 ISO 8601 时间: {retain_until!r}")
        if until_dt.tzinfo is not None:
            from datetime import timezone
            until_dt = until_dt.astimezone(timezone.utc).replace(tzinfo=None)
        if until_dt <= now_utc_naive():
            raise ArchiveStateError(
                f"保留到期时间必须晚于当前时间(收到 {until_dt.isoformat()} UTC)")
    old_mode = archive.retention_mode
    old_until = archive.retain_until
    archive.retention_mode = mode
    archive.retain_until = until_dt
    archive.retention_set_by = operator
    archive.retention_set_at = now_utc_naive()
    archive.updated_by = operator
    if mode == "PERMANENT":
        detail = "已设置永久保留标记: 任何清理计划都将跳过该归档"
    elif mode == "UNTIL":
        detail = f"已设置保留策略: 保留至 {until_dt.isoformat()} UTC, 到期前不可清理"
    else:
        detail = "已清除保留策略, 归档可被清理计划处理"
    add_event(session, archive_id=archive.id, event="retention.set", operator=operator,
              reason=detail,
              detail={"from_mode": old_mode,
                      "from_retain_until": _dt(old_until),
                      "to_mode": mode, "to_retain_until": _dt(until_dt)})
    session.commit()
    return {"ok": True, "archive_id": archive.id, "retention": retention_dict(archive),
            "detail": detail}


# ---------- 下载 / 摘要校验并发协调(使用计数) ----------

def begin_archive_use(session: Session, archive: ReplayArchive, kind: str) -> None:
    """登记一次下载(download)/校验(verify)在途使用, 与行锁同事务提交。
    清理计划在逐项处理时看到计数 > 0 即跳过(in_use_download/in_use_verify),
    保证归档正在下载或校验时物理文件绝不被删除。"""
    a = lock_archive(session, archive.id)
    if kind == "download":
        a.active_downloads = (a.active_downloads or 0) + 1
    else:
        a.active_verifies = (a.active_verifies or 0) + 1
    a.updated_by = "system"
    session.commit()


def end_archive_use(session: Session, archive_id: str, kind: str) -> None:
    """在途下载/校验结束(响应已发送/校验完成), 释放使用计数。崩溃遗留计数由
    重启对账清零(进程死亡意味着在途 HTTP 请求已不存在)。"""
    try:
        a = lock_archive(session, archive_id)
        col = a.active_downloads if kind == "download" else a.active_verifies
        if (col or 0) > 0:
            if kind == "download":
                a.active_downloads = col - 1
            else:
                a.active_verifies = col - 1
        session.commit()
    except Exception:
        session.rollback()


def archive_in_use(archive: ReplayArchive) -> tuple[bool, str | None, str | None]:
    """归档是否正在下载/校验。返回(在使用, 原因码, 说明)。"""
    if (archive.active_downloads or 0) > 0:
        return True, "in_use_download", (
            f"归档正在下载中({archive.active_downloads} 个在途下载), 不能清理")
    if (archive.active_verifies or 0) > 0:
        return True, "in_use_verify", (
            f"归档正在摘要校验中({archive.active_verifies} 个在途校验), 不能清理")
    return False, None, None


# ---------- 归档目录检索 ----------

def search_archives(session: Session, *, replay_id: str | None = None,
                    biz: str | None = None, report_version: int | None = None,
                    content_digest: str | None = None,
                    status: str | None = None, retention: str | None = None,
                    include_cleaned: bool = False) -> list[ReplayArchive]:
    """归档目录多维检索: 按回放、业务分组、报告版本、内容摘要、归档状态、
    保留状态过滤。默认仅返回未清理(存活)的归档。"""
    q = session.query(ReplayArchive)
    if not include_cleaned:
        q = q.filter(ReplayArchive.cleaned_at.is_(None))
    if replay_id:
        q = q.filter(ReplayArchive.replay_id == replay_id)
    if report_version is not None:
        q = q.filter(ReplayArchive.report_version == report_version)
    if status:
        q = q.filter(ReplayArchive.status == status)
    if biz:
        if session.bind.dialect.name == "postgresql":
            q = q.filter(ReplayArchive.biz_groups.op("?")(biz))
        # SQLite 的 JSON 文本存储可能把中文转义成 \\uXXXX, 不能直接 LIKE 中文:
        # 取候选行后在结果侧按精确成员校验兜底
    rows = q.order_by(ReplayArchive.created_at, ReplayArchive.id).all()
    if biz and session.bind.dialect.name != "postgresql":
        rows = [a for a in rows if biz in (a.biz_groups or [])]
    if content_digest:
        d = content_digest.strip()
        rows = [a for a in rows if a.content_digest and (
            a.content_digest == d or
            (len(d) >= 12 and a.content_digest.startswith(d)))]
    if retention:
        rows = [a for a in rows if (a.retention_mode or "NONE") == retention]
    return rows


def find_by_digest(session: Session, content_digest: str) -> list[ReplayArchive]:
    """按内容摘要查询归档: 完整摘要精确匹配, 长度 >=12 的前缀也允许(消歧);
    仅返回未清理的存活归档。"""
    d = (content_digest or "").strip()
    if not d:
        return []
    q = (session.query(ReplayArchive)
         .filter(ReplayArchive.cleaned_at.is_(None),
                 ReplayArchive.content_digest.isnot(None)))
    if len(d) >= 64:
        return q.filter(ReplayArchive.content_digest == d).order_by(
            ReplayArchive.created_at, ReplayArchive.id).all()
    if len(d) >= 12:
        rows = q.order_by(ReplayArchive.created_at, ReplayArchive.id).all()
        return [a for a in rows if a.content_digest.startswith(d)]
    return []


# ---------- 重启对账 ----------

def boot_recover_archives(session: Session) -> None:
    """服务重启时: 遗留 RUNNING 归档(执行线程已死)回到 QUEUED 重新参与排队;
    已完成单元在 staging 中保留, worker 从首个未完成单元续跑。
    QUEUED/PAUSED/终态任务保持不变(排队/暂停/失败记录都是持久化的用户态)。
    下载/校验使用计数清零: 进程死亡意味着在途 HTTP 请求已不存在, 不得遗留
    "永久在使用"而阻止清理; 保留策略、摘要引用关系与清理进度均在库中, 不动。"""
    rows = session.query(ReplayArchive).order_by(ReplayArchive.id).all()
    for a in rows:
        if (a.active_downloads or 0) or (a.active_verifies or 0):
            d, v = a.active_downloads or 0, a.active_verifies or 0
            a.active_downloads = 0
            a.active_verifies = 0
            add_event(session, archive_id=a.id, event="boot.reset_use",
                      operator="system",
                      reason=(f"服务重启: 清零在途下载/校验计数"
                              f"(下载 {d}, 校验 {v}), 保留策略与引用关系不变"))
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
    member = (session.query(ArchiveDigestMember)
              .filter(ArchiveDigestMember.archive_id == a.id).first())
    same_digest = (session.query(ArchiveDigestMember)
                   .filter(ArchiveDigestMember.content_digest == a.content_digest)
                   .all()) if a.content_digest else []
    live_refs = 0
    for m in same_digest:
        other = session.get(ReplayArchive, m.archive_id)
        if other is not None and other.cleaned_at is None:
            live_refs += 1
    out = {
        "id": a.id,
        "replay_id": a.replay_id,
        "plan_id": a.plan_id,
        "plan_name": a.plan_name,
        "checkpoint_id": a.checkpoint_id,
        "report_version": a.report_version,
        "biz_groups": a.biz_groups or [],
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
        # 保留策略与保留状态
        "retention": retention_dict(a),
        # 同摘要去重与引用关系
        "digest_group": {
            "content_digest": a.content_digest,
            "is_canonical": bool(member and member.is_canonical),
            "reference_count": live_refs,
            "member_count": len(same_digest),
        },
        # 下载/校验并发状态(>0 时清理跳过)
        "active_downloads": a.active_downloads or 0,
        "active_verifies": a.active_verifies or 0,
        "in_use": bool((a.active_downloads or 0) or (a.active_verifies or 0)),
        # 清理(软删除)状态
        "cleaned": a.cleaned_at is not None,
        "cleaned_at": _dt(a.cleaned_at),
        "cleaned_by": a.cleaned_by,
        "cleanup_plan_id": a.cleanup_plan_id,
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
