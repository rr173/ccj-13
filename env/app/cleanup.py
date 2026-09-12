"""归档目录生命周期: 保留策略驱动的归档清理计划。

设计要点:
1. 清理计划状态机(与归档任务同构): QUEUED(排队) -> RUNNING -> COMPLETED;
   pause 在逐项边界停住(PAUSED), resume 重新排队(QUEUED); cancel 把未处理项
   置 SKIPPED_CANCELED; 执行遇未预期错误 -> FAILED(逐项进度保留, resume 可从未
   完成项继续)。逐项结果与跳过原因永久落库可查询, 服务重启后保留策略、引用关系
   与清理进度全部保留。
2. 逐项清理判定(按顺序, 第一条命中即落 SKIPPED + 机器可读原因码):
   not_found(归档不存在) / not_completed(归档活动中) / already_cleaned /
   retained_until(仍在保留期) / retained_permanent(永久保留) /
   in_use_download(正在下载) / in_use_verify(正在摘要校验);
   通过全部闸门才真正清理: 软删除归档记录(cleaned_at)并按同摘要引用关系处理
   物理文件 —— 同摘要组仍有其他存活成员时, canonical 先把物理文件移交给最早的
   存活成员(移交失败 -> digest_referenced 跳过, 绝不删除仍被引用的文件),
   非 canonical 成员只解除自己的引用; 最后一个引用解除后物理文件才被删除。
3. 并发协调: 下载/摘要校验在归档行上持有使用计数(active_downloads/
   active_verifies, 见 archives.begin_archive_use), 清理与下载/校验通过
   行锁 + 写事务串行化; 归档正在下载或校验时不能被删除。清理逐项独立事务提交,
   成功项不回滚, 失败/跳过项带原因。
4. 幂等: 创建/暂停/恢复/取消走 archive.cleanup.* 幂等命名空间, 同键重放返回
   首次结果(replayed=true), 不产生第二个计划、不二次推进。
5. 单 RUNNING worker 串行: 同时只有一个清理计划在执行(其余排队), 与归档 worker
   同生命周期的后台线程驱动; 测试用 PLAN_WORKER_ENABLED=0 关闭后手动驱动。
"""
import hashlib
import json
import os
import threading
import time
import uuid

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import (
    ArchiveCleanupItem, ArchiveCleanupPlan, ArchiveDigestMember, IdempotencyKey,
    ReplayArchive,
)
from . import archives

CLEANUP_ALLOWED_ACTIONS = {
    # FAILED 也可 resume: 逐项进度保留, 失败项重新排队继续
    "pause": {"QUEUED", "RUNNING"},
    "resume": {"PAUSED", "FAILED"},
    "cancel": {"QUEUED", "RUNNING", "PAUSED"},
}


class CleanupNotFound(Exception):
    """清理计划不存在 -> 404。"""


class CleanupStateError(Exception):
    """当前状态不允许该操作 / 幂等键复用 -> 409。"""


# ---------- 幂等框架(archive.cleanup.* 命名空间) ----------

def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def run_cleanup_action(session: Session, *, action: str, operator: str,
                       idempotency_key: str, payload: dict, fn,
                       plan_id: str | None = None) -> tuple[dict, bool]:
    """与 archives.run_archive_action 同构: 结果与幂等键同事务落库,
    重复提交(含崩溃重放)返回首次结果; 同键跨计划/跨动作/不同请求体 -> 409。"""
    req_hash = _hash({"action": f"archive.cleanup.{action}", "plan_id": plan_id,
                      "payload": payload})
    existing = session.get(IdempotencyKey, idempotency_key)
    if existing is not None:
        if existing.request_hash != req_hash:
            raise CleanupStateError("幂等键被不同请求复用")
        return existing.response_json, True

    plan = _lock_plan(session, plan_id) if plan_id is not None else None
    result = fn(session, plan)
    if plan_id is not None:
        fresh = get_plan(session, plan_id)
        result["plan_id"] = plan_id
        result["status"] = fresh.status
    session.add(IdempotencyKey(key=idempotency_key,
                               action=f"archive.cleanup.{action}",
                               request_hash=req_hash, response_json=result))
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        winner = session.get(IdempotencyKey, idempotency_key)
        if winner is not None and winner.request_hash == req_hash:
            return winner.response_json, True
        raise
    return result, False


# ---------- 行锁 ----------

def get_plan(session: Session, plan_id: str) -> ArchiveCleanupPlan:
    p = session.get(ArchiveCleanupPlan, plan_id)
    if p is None:
        raise CleanupNotFound(f"清理计划 {plan_id} 不存在")
    return p


def _lock_plan(session: Session, plan_id: str) -> ArchiveCleanupPlan:
    if session.bind.dialect.name != "sqlite":
        p = session.get(ArchiveCleanupPlan, plan_id, with_for_update=True)
        if p is not None:
            return p
    return get_plan(session, plan_id)


def _add_event(session: Session, plan: ArchiveCleanupPlan, event: str,
               operator: str, reason: str | None = None,
               detail: dict | None = None) -> None:
    evs = list(plan.events or [])
    evs.append({"ts": archives.now_utc_naive().isoformat(), "event": event,
                "operator": operator, "reason": reason, "detail": detail})
    plan.events = evs[-200:]
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(plan, "events")


# ---------- 创建清理计划 ----------

def do_create_plan(session: Session, _plan, operator: str,
                   archive_ids: list[str]) -> dict:
    """发起归档清理计划并排队。逐项在执行时判定: 不存在/活动中/已清理/保留期内/
    永久保留/下载校验占用都会逐项返回跳过原因, 不影响其他项。空列表拒绝。"""
    ids = list(dict.fromkeys(archive_ids))  # 保序去重
    if not ids:
        raise CleanupStateError("清理计划至少包含一个归档 id")
    plan_id = "CP" + uuid.uuid4().hex[:10]
    plan = ArchiveCleanupPlan(
        id=plan_id, operator=operator, status="QUEUED",
        total_items=len(ids), cleaned_items=0, skipped_items=0, failed_items=0,
        events=[])
    session.add(plan)
    session.flush()
    for pos, aid in enumerate(ids):
        session.add(ArchiveCleanupItem(
            plan_id=plan_id, archive_id=aid, position=pos, status="PENDING"))
    _add_event(session, plan, "create", operator,
               reason=f"创建归档清理计划: {len(ids)} 个归档项, 已排队",
               detail={"total": len(ids)})
    _add_event(session, plan, "queue", operator,
               reason="清理计划已排队, 等待执行(逐项判定保留策略/下载校验占用/引用关系)")
    session.flush()
    return {"ok": True, "plan_id": plan_id, "status": "QUEUED",
            "total_items": len(ids),
            "detail": f"清理计划 {plan_id} 已创建并排队: {len(ids)} 个归档项; "
                      f"保留期内/永久保留/正在下载或校验的归档将逐项跳过并记录原因"}


# ---------- 暂停 / 恢复 / 取消(均幂等) ----------

def _require_status(plan: ArchiveCleanupPlan, action: str) -> None:
    allowed = CLEANUP_ALLOWED_ACTIONS[action]
    if plan.status not in allowed:
        raise CleanupStateError(
            f"清理计划 {plan.id} 当前状态 {plan.status} 不允许 {action}"
            f"(仅 {sorted(allowed)} 状态可执行该操作)")


def do_pause(session: Session, plan: ArchiveCleanupPlan, operator: str) -> dict:
    if plan.status == "PAUSED":
        return {"ok": True, "already_in_state": True,
                "detail": "清理计划已处于暂停状态, 重复暂停无副作用"}
    _require_status(plan, "pause")
    plan.status = "PAUSED"
    plan.current_archive_id = None
    _add_event(session, plan, "pause", operator,
               reason="暂停请求已记录, 将在当前清理项边界停住")
    return {"ok": True, "detail": "清理计划已暂停, 将在当前清理项边界停止"}


def do_resume(session: Session, plan: ArchiveCleanupPlan, operator: str) -> dict:
    if plan.status in ("QUEUED", "RUNNING"):
        return {"ok": True, "already_in_state": True,
                "detail": f"清理计划当前为 {plan.status}, 恢复请求无副作用"}
    _require_status(plan, "resume")
    from_status = plan.status
    # 从 FAILED 恢复: 失败项重新排队(保留 CLEANED/SKIPPED 结果, 调整计数),
    # 已清理/已跳过项不二次处理; 从 PAUSED 恢复只需重新排队
    retried = 0
    if from_status == "FAILED":
        failed_items = (session.query(ArchiveCleanupItem)
                        .filter(ArchiveCleanupItem.plan_id == plan.id,
                                ArchiveCleanupItem.status == "FAILED").all())
        for item in failed_items:
            item.status = "PENDING"
            item.reason_code = None
            item.reason = None
            item.processed_by = None
            item.processed_at = None
            retried += 1
        plan.failed_items = 0
    plan.status = "QUEUED"
    plan.last_error = None
    plan.finished_at = None
    plan.current_archive_id = None
    _add_event(session, plan, "resume", operator,
               reason=(f"恢复失败的清理计划: 重新排队, {retried} 个失败项重试, "
                       f"已清理/已跳过项保留" if from_status == "FAILED"
                       else "恢复清理计划: 重新排队, 从首个未完成项续跑"),
               detail={"from_status": from_status, "retried_failed": retried})
    return {"ok": True,
            "detail": (f"清理计划已恢复: {retried} 个失败项重新排队"
                       if from_status == "FAILED"
                       else "清理计划已恢复, 已重新排队, 未完成项将继续处理")}


def do_cancel(session: Session, plan: ArchiveCleanupPlan, operator: str) -> dict:
    if plan.status == "CANCELED":
        return {"ok": True, "already_in_state": True,
                "detail": "清理计划已取消, 重复取消无副作用"}
    _require_status(plan, "cancel")
    pending = (session.query(ArchiveCleanupItem)
               .filter(ArchiveCleanupItem.plan_id == plan.id,
                       ArchiveCleanupItem.status == "PENDING").all())
    for item in pending:
        item.status = "SKIPPED_CANCELED"
        item.reason_code = "canceled"
        item.reason = "清理计划被取消, 该项未处理"
        item.processed_by = operator
        item.processed_at = archives.now_utc_naive()
        plan.skipped_items += 1
    plan.status = "CANCELED"
    plan.current_archive_id = None
    plan.finished_at = archives.now_utc_naive()
    _add_event(session, plan, "cancel", operator,
               reason=f"清理计划被取消, {len(pending)} 个未处理项置为跳过",
               detail={"canceled_pending": len(pending)})
    return {"ok": True, "detail": f"清理计划已取消, {len(pending)} 个未处理项已跳过"}


# ---------- 调度: 同时只允许一个 RUNNING 清理计划 ----------

def claim_due_plans(session: Session) -> list[str]:
    """把最前面的 QUEUED 清理计划认领为 RUNNING(仅当当前无 RUNNING 计划),
    返回认领的计划 id(已提交)。单 RUNNING 串行, 取消/暂停后下一个计划才能认领。"""
    if session.bind.dialect.name == "postgresql":
        from sqlalchemy import text
        session.execute(text("SELECT pg_advisory_xact_lock(20260919)"))
    running = (session.query(ArchiveCleanupPlan)
               .filter(ArchiveCleanupPlan.status == "RUNNING").count())
    if running > 0:
        session.rollback()
        return []
    due = (session.query(ArchiveCleanupPlan)
           .filter(ArchiveCleanupPlan.status == "QUEUED")
           .order_by(ArchiveCleanupPlan.created_at, ArchiveCleanupPlan.id)
           .first())
    if due is None:
        session.rollback()
        return []
    due.status = "RUNNING"
    due.started_at = due.started_at or archives.now_utc_naive()
    _add_event(session, due, "claim", "system",
               reason="清理计划获得执行权, 开始逐项处理")
    session.commit()
    return [due.id]


# ---------- 逐项清理 ----------

def _skip(item: ArchiveCleanupItem, code: str, reason: str,
          operator: str = "system") -> None:
    item.status = "SKIPPED"
    item.reason_code = code
    item.reason = reason[:500]
    item.processed_by = operator
    item.processed_at = archives.now_utc_naive()


def _fail_item(item: ArchiveCleanupItem, code: str, reason: str,
               operator: str = "system") -> None:
    item.status = "FAILED"
    item.reason_code = code
    item.reason = reason[:500]
    item.processed_by = operator
    item.processed_at = archives.now_utc_naive()


def _cleaned(item: ArchiveCleanupItem, operator: str, reason: str) -> None:
    item.status = "CLEANED"
    item.reason_code = None
    item.reason = reason[:500]
    item.processed_by = operator
    item.processed_at = archives.now_utc_naive()


def _process_item(session: Session, plan: ArchiveCleanupPlan,
                  item: ArchiveCleanupItem) -> str:
    """处理单个清理项, 返回结果类别: cleaned/skipped/failed。逐项判定与物理文件
    处理在同一事务提交; 物理文件删除在提交后执行(数据库为权威状态)。"""
    aid = item.archive_id
    archive = session.get(ReplayArchive, aid)
    if archive is None:
        _skip(item, "not_found", f"归档 {aid} 不存在, 无可清理内容")
        return "skipped"
    if archive.cleaned_at is not None:
        _skip(item, "already_cleaned",
              f"归档 {aid} 已在 {archive.cleaned_at.isoformat()} 被清理"
              f"(清理计划 {archive.cleanup_plan_id})")
        return "skipped"
    if archive.status != "COMPLETED":
        _skip(item, "not_completed",
              f"归档 {aid} 当前状态 {archive.status}(非 COMPLETED, 仅成功完成且包"
              f"可用的归档可清理; FAILED 归档作为失败记录永久保留), 活动中的归档不能清理")
        return "skipped"
    retained, code, reason = archives.is_retained(archive)
    if retained:
        _skip(item, code, f"归档 {aid} 被保留策略阻止清理: {reason}")
        return "skipped"
    in_use, code, reason = archives.archive_in_use(archive)
    if in_use:
        _skip(item, code, f"归档 {aid} {reason}, 本次清理跳过(可稍后重新发起清理)")
        return "skipped"
    # 归档包文件已不存在(被外部删除): 记录保留, 明确跳过并提示(与下载/校验的
    # package_missing 失败语义一致, 不静默清理成"成功")
    if not archive.package_path or not os.path.exists(archive.package_path):
        _skip(item, "package_missing",
              f"归档 {aid} 的物理归档包已不存在({archive.package_path}); "
              f"归档记录与失败原因保留, 请人工核查")
        return "skipped"

    # 全部闸门通过: 按同摘要引用关系处理物理文件, 再软删除归档记录
    pending_delete: list[str | None] = []
    try:
        file_action = _apply_cleanup(session, archive, plan, pending_delete)
    except _DigestReferenced as e:
        _skip(item, "digest_referenced",
              f"归档 {aid} 是同摘要组的规范(canonical)成员且仍被其他归档引用, "
              f"物理文件不能删除: {e}; 已跳过(可先解除引用后重试)")
        return "skipped"
    except Exception as e:  # 防御: 单项未预期错误记 FAILED, 不影响其他项
        session.rollback()
        return _mark_item_failed(session, plan, item, "internal_error",
                                 f"清理归档 {aid} 时发生未预期错误: {e}")

    archive.cleaned_at = archives.now_utc_naive()
    archive.cleaned_by = plan.operator
    archive.cleanup_plan_id = plan.id
    archive.updated_by = "system"
    archives.add_event(session, archive_id=aid, event="cleanup.cleaned",
                       operator=plan.operator,
                       reason=(f"归档由清理计划 {plan.id} 清理(软删除, 记录保留可查): "
                               f"{file_action}"),
                       detail={"plan_id": plan.id, "file_action": file_action})
    _cleaned(item, plan.operator, f"归档 {aid} 已清理: {file_action}")
    session.commit()
    # 数据库已提交为权威状态后删除物理文件; 删除失败不回滚清理结果, 但落事件留痕
    for fpath in pending_delete:
        if fpath:
            try:
                _delete_file(fpath)
            except OSError as e:
                _record_file_delete_failed(aid, fpath, e, plan)
    return "cleaned"


class _DigestReferenced(Exception):
    """canonical 移交失败但仍有存活引用, 不能删除物理文件。"""


def _apply_cleanup(session: Session, archive: ReplayArchive,
                   plan: ArchiveCleanupPlan,
                   pending_delete: list[str | None]) -> str:
    """按同摘要去重引用关系安排物理包文件处置, 返回人类可读的文件处置说明。
    需要删除的物理文件路径追加到 pending_delete(由调用方在事务提交后删除:
    数据库为权威状态, 提交前绝不删文件)。
    - 无摘要成员关系: 删除自己的物理文件(最后/唯一引用);
    - 非 canonical 成员: 共享文件由 canonical 持有, 只解除自己的成员引用;
    - canonical 成员且仍有其他存活成员: 把文件移交给最早的存活成员
      (其包文件须存在且摘要匹配), 移交后共享文件保留;
    - canonical 移交失败: 抛 _DigestReferenced, 调用方逐项跳过。"""
    member = (session.query(ArchiveDigestMember)
              .filter(ArchiveDigestMember.archive_id == archive.id).first())
    path = archive.package_path
    if member is None or not archive.content_digest:
        pending_delete.append(path)
        return "物理归档包已删除(无同摘要引用)"

    others = (session.query(ArchiveDigestMember)
              .filter(ArchiveDigestMember.content_digest == archive.content_digest,
                      ArchiveDigestMember.archive_id != archive.id)
              .order_by(ArchiveDigestMember.id).all())
    live_others = []
    for m in others:
        other = session.get(ReplayArchive, m.archive_id)
        if other is not None and other.cleaned_at is None:
            live_others.append((m, other))

    if not member.is_canonical:
        # 非 canonical: 物理文件由 canonical 持有, 解除自身引用即可, 不删文件
        session.delete(member)
        return (f"解除同摘要引用(引用数 {len(live_others) + 1} -> {len(live_others)}), "
                f"物理归档包由规范成员保留, 未删除")

    if not live_others:
        # canonical 且无其他存活引用: 物理文件可以安全删除, 成员关系保留为历史
        member.is_canonical = False
        pending_delete.append(path)
        return "同摘要组最后一个引用已解除, 物理归档包已删除"

    # canonical 仍被引用: 必须先把物理文件移交给其他存活成员
    successor_member, successor = live_others[0]
    succ_path = successor.package_path
    # 后继成员当前共享的就是 canonical 路径; 其文件必须可用且摘要匹配才能接管
    if not succ_path or not os.path.exists(succ_path):
        raise _DigestReferenced(
            f"后继归档 {successor.id} 的物理包不可用({succ_path}), 无法移交")
    check = archives.verify_package_bytes(
        succ_path, expected_content_digest=archive.content_digest,
        expected_manifest=successor.manifest)
    if not check["valid"]:
        raise _DigestReferenced(
            f"后继归档 {successor.id} 的物理包摘要校验不通过: "
            f"{'; '.join(check['issues'][:2])}")
    member.is_canonical = False
    successor_member.is_canonical = True
    archives.add_event(session, archive_id=successor.id, event="cleanup.transfer",
                       operator=plan.operator,
                       reason=(f"归档 {archive.id} 被清理, 同摘要物理包的规范持有权"
                               f"移交给本归档(仍被 {len(live_others)} 个存活归档引用, "
                               f"物理文件保留)"),
                       detail={"from_archive_id": archive.id,
                               "plan_id": plan.id,
                               "content_digest": archive.content_digest})
    return (f"物理归档包与规范持有权移交给同摘要归档 {successor.id}, "
            f"仍有 {len(live_others)} 个存活引用, 物理文件保留")


def _delete_file(path: str | None) -> None:
    """删除物理归档包; 文件本就不存在视为成功(幂等), 删除失败抛错由上层记原因。"""
    if not path:
        return
    if os.path.exists(path):
        os.remove(path)


def _record_file_delete_failed(archive_id: str, path: str, err: OSError,
                               plan: ArchiveCleanupPlan) -> None:
    """物理文件删除失败(权限/IO): 清理结果不回滚, 但落事件与 item 原因留痕。"""
    from .db import SessionLocal
    db = SessionLocal()
    try:
        archives.add_event(db, archive_id=archive_id, event="cleanup.file_delete_failed",
                           operator=plan.operator,
                           reason=(f"物理归档包删除失败: {err}(归档记录已软清理, "
                                   f"文件残留 {path}, 可人工核查)"),
                           detail={"plan_id": plan.id, "path": path})
        item = (db.query(ArchiveCleanupItem)
                .filter(ArchiveCleanupItem.plan_id == plan.id,
                        ArchiveCleanupItem.archive_id == archive_id).first())
        if item is not None:
            item.reason_code = "file_delete_failed"
            item.reason = (f"{item.reason or ''}; 物理文件删除失败: {err}")[:500]
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _mark_item_failed(session: Session, plan: ArchiveCleanupPlan,
                      item: ArchiveCleanupItem, code: str, reason: str) -> str:
    item = session.get(ArchiveCleanupItem, item.id)
    _fail_item(item, code, reason)
    plan.failed_items += 1
    plan.last_error = reason[:500]
    _add_event(session, plan, "item.failed", "system",
               reason=f"归档 {item.archive_id} 清理失败[{code}]: {reason}",
               detail={"archive_id": item.archive_id, "code": code})
    session.commit()
    return "failed"


# 每个 tick 至多处理的清理项数(逐项边界之间检查暂停/取消, 兼顾吞吐与响应)
def items_per_tick() -> int:
    try:
        return max(1, int(os.getenv("CLEANUP_ITEMS_PER_TICK", "5")))
    except ValueError:
        return 5


def run_cleanup_tick(session: Session, plan_id: str) -> bool:
    """推进一个 RUNNING 清理计划至多一个处理批次(_ITEMS_PER_TICK 项),
    返回是否处理了至少一项。暂停/取消在逐项边界生效。"""
    plan = _lock_plan(session, plan_id)
    if plan.status != "RUNNING":
        session.rollback()
        return False
    items = (session.query(ArchiveCleanupItem)
             .filter(ArchiveCleanupItem.plan_id == plan_id,
                     ArchiveCleanupItem.status == "PENDING")
             .order_by(ArchiveCleanupItem.position, ArchiveCleanupItem.id)
             .limit(items_per_tick()).all())
    if not items:
        session.rollback()
        return False
    processed = 0
    for item in items:
        # 逐项边界重新取控制状态: pause/cancel 在当前项处理前生效
        plan = _lock_plan(session, plan_id)
        if plan.status != "RUNNING":
            session.rollback()
            return processed > 0
        plan.current_archive_id = item.archive_id
        item.status = "RUNNING"
        session.commit()

        plan = _lock_plan(session, plan_id)
        item = session.get(ArchiveCleanupItem, item.id)
        try:
            outcome = _process_item(session, plan, item)
            if outcome == "cleaned":
                plan.cleaned_items += 1
            elif outcome == "skipped":
                plan.skipped_items += 1
            # failed 的计数已在 _mark_item_failed 内提交
            if outcome != "failed":
                archives.add_event(
                    session, archive_id=item.archive_id,
                    event=("cleanup.skipped" if outcome == "skipped"
                           else "cleanup.cleaned"),
                    operator=plan.operator,
                    reason=item.reason,
                    detail={"plan_id": plan_id, "outcome": outcome,
                            "reason_code": item.reason_code})
                session.commit()
        except Exception as e:  # 防御: 单项异常不拖垮整个计划
            session.rollback()
            _mark_item_failed(session, plan, item, "internal_error",
                              f"清理归档 {item.archive_id} 时发生未预期错误: {e}")
        processed += 1

    plan = _lock_plan(session, plan_id)
    plan.current_archive_id = None
    remaining = (session.query(ArchiveCleanupItem)
                 .filter(ArchiveCleanupItem.plan_id == plan_id,
                         ArchiveCleanupItem.status == "PENDING").count())
    if remaining == 0 and plan.status == "RUNNING":
        # 所有项都有终态结果(CLEANED/SKIPPED/FAILED): 计划 COMPLETED;
        # 逐项失败不改变计划终态(失败原因可逐项查询), 管理员修复后仍可 resume 重试
        plan.status = "COMPLETED"
        plan.finished_at = archives.now_utc_naive()
        _add_event(session, plan, "complete", "system",
                   reason=(f"清理计划完成: 已清理 {plan.cleaned_items}, "
                           f"跳过 {plan.skipped_items}"
                           + (f", 失败 {plan.failed_items}(可 resume 重试失败项)"
                              if plan.failed_items else "")),
                   detail={"cleaned": plan.cleaned_items,
                           "skipped": plan.skipped_items,
                           "failed": plan.failed_items})
    session.commit()
    return True


# ---------- 失败收尾(worker 级未预期错误) ----------

def fail_plan(session: Session, plan_id: str, reason: str) -> None:
    """清理计划执行发生未预期错误: 置 FAILED 并保留全部逐项进度与跳过原因,
    管理员修复原因后可 resume(失败项重新排队)。幂等: 终态计划不再变更。"""
    plan = _lock_plan(session, plan_id)
    if plan.status in ("COMPLETED", "CANCELED", "FAILED"):
        session.rollback()
        return
    plan.status = "FAILED"
    plan.last_error = reason[:500]
    plan.current_archive_id = None
    plan.finished_at = archives.now_utc_naive()
    _add_event(session, plan, "failed", "system",
               reason=f"清理计划执行失败: {reason}; 逐项进度保留, 可 resume 继续")
    session.commit()


# ---------- 重启对账 ----------

def boot_recover_cleanup_plans(session: Session) -> None:
    """服务重启时: 遗留 RUNNING 清理计划(执行线程已死)回到 QUEUED 重新排队,
    逐项进度(CLEANED/SKIPPED/FAILED)与跳过原因全部保留, worker 从首个 PENDING
    项续跑; RUNNING 中的单项(已提交为 RUNNING 但未完成)复位为 PENDING。
    QUEUED/PAUSED/终态计划保持不变(排队/暂停是持久化用户态)。"""
    plans = (session.query(ArchiveCleanupPlan)
             .order_by(ArchiveCleanupPlan.id).all())
    for plan in plans:
        if plan.status != "RUNNING":
            continue
        plan.status = "QUEUED"
        plan.current_archive_id = None
        running_items = (session.query(ArchiveCleanupItem)
                         .filter(ArchiveCleanupItem.plan_id == plan.id,
                                 ArchiveCleanupItem.status == "RUNNING").all())
        for item in running_items:
            item.status = "PENDING"
            item.reason_code = None
            item.reason = None
        # 按逐项终态重算汇总计数, 保证崩溃前部分提交与计数一致
        plan.cleaned_items = (session.query(ArchiveCleanupItem)
                              .filter(ArchiveCleanupItem.plan_id == plan.id,
                                      ArchiveCleanupItem.status == "CLEANED").count())
        plan.skipped_items = (session.query(ArchiveCleanupItem)
                              .filter(ArchiveCleanupItem.plan_id == plan.id,
                                      ArchiveCleanupItem.status.in_(
                                          ("SKIPPED", "SKIPPED_CANCELED"))).count())
        plan.failed_items = (session.query(ArchiveCleanupItem)
                             .filter(ArchiveCleanupItem.plan_id == plan.id,
                                     ArchiveCleanupItem.status == "FAILED").count())
        _add_event(session, plan, "boot", "system",
                   reason=(f"服务重启: RUNNING 清理计划回到排队位置, 已清理 "
                           f"{plan.cleaned_items}/{plan.total_items}, "
                           f"worker 将从首个未完成项续跑, 跳过原因与保留策略不变"))
        session.commit()


# ---------- 后台 worker ----------

class CleanupWorker:
    """单实例后台线程: 认领一个到期清理计划(单 RUNNING 串行)并推进一个批次。

    与 ArchiveWorker 相同的单副本假设与串行化保证; 测试用 PLAN_WORKER_ENABLED=0
    关闭后手动 claim_due_plans + run_cleanup_tick 做确定性验证。"""

    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (poll_interval if poll_interval is not None
                              else float(os.getenv("CLEANUP_WORKER_POLL_INTERVAL", "0.5")))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="cleanup-worker",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    def tick_once(self) -> int:
        from .db import SessionLocal
        db = SessionLocal()
        try:
            claimed = claim_due_plans(db)
            if not claimed:
                return 0
            plan_id = claimed[0]
        finally:
            db.close()
        db = SessionLocal()
        try:
            run_cleanup_tick(db, plan_id)
        except Exception as e:
            db.rollback()
            try:
                fail_plan(db, plan_id, f"清理执行未预期错误: {e}")
            except Exception:
                db.rollback()
        finally:
            db.close()
        return 1

    def _loop(self) -> None:
        while not self._stop.wait(self.poll_interval):
            try:
                self.tick_once()
            except Exception:
                time.sleep(self.poll_interval)


# ---------- 视图 ----------

def item_to_dict(item: ArchiveCleanupItem) -> dict:
    return {
        "archive_id": item.archive_id,
        "position": item.position,
        "status": item.status,
        "reason_code": item.reason_code,
        "reason": item.reason,
        "processed_by": item.processed_by,
        "processed_at": archives._dt(item.processed_at),
    }


def plan_to_dict(session: Session, plan: ArchiveCleanupPlan,
                 *, with_items: bool = True) -> dict:
    out = {
        "id": plan.id,
        "operator": plan.operator,
        "status": plan.status,
        "total_items": plan.total_items,
        "cleaned_items": plan.cleaned_items,
        "skipped_items": plan.skipped_items,
        "failed_items": plan.failed_items,
        "progress": {
            "total": plan.total_items,
            "cleaned": plan.cleaned_items,
            "skipped": plan.skipped_items,
            "failed": plan.failed_items,
            "processed": (plan.cleaned_items + plan.skipped_items
                          + plan.failed_items),
        },
        "current_archive_id": plan.current_archive_id,
        "last_error": plan.last_error,
        "created_at": archives._dt(plan.created_at),
        "started_at": archives._dt(plan.started_at),
        "finished_at": archives._dt(plan.finished_at),
        "updated_at": archives._dt(plan.updated_at),
        "events": list(plan.events or []),
    }
    if with_items:
        items = (session.query(ArchiveCleanupItem)
                 .filter(ArchiveCleanupItem.plan_id == plan.id)
                 .order_by(ArchiveCleanupItem.position, ArchiveCleanupItem.id).all())
        out["items"] = [item_to_dict(i) for i in items]
    return out
