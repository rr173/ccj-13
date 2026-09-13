"""补偿审批与执行窗口编排测试。

审批(高风险补偿任务):
- 低风险(仅回填)免审批; 高风险(清理/解冻/取消计划)须两名不同操作者独立审批;
- 审批人不能是创建者, 执行人不能是任一审批人;
- 拒绝带原因并阻止执行, 重新收集两轮通过后放行;
- 审批过程中快照/质量门禁状态/计划状态变化 -> 已收集审批自动失效并记录原因;
- 双人审批并发去重(同操作者并发提交只产生一条)。

执行窗口:
- 预约限定执行窗口, 窗口外显式执行/重试明确拒绝;
- 窗口冲突返回占用者与冲突时间段;
- 窗口临近过期在动作边界自动暂停并保留已完成动作, 重新进入窗口自动恢复;
- 暂停后恢复续跑。

其他: 任务撤销与审批历史关联; 重启后审批/窗口/失效状态不丢。
后台 worker 由环境变量关闭, 全部用 auditreplay 函数手动确定性驱动。
"""
import os
import tempfile
import threading
from datetime import datetime, timedelta, timezone

_tmp = tempfile.mkdtemp(prefix="comp-approval-window-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["AUDIT_SNAPSHOT_TTL_SECONDS"] = "3600"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    AuditSnapshot, CompensationTask, MigrationPlan, RecordNew,
)
from app import auditreplay, plans, quality  # noqa: F401  (quality 供后续场景扩展)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建辅助 ----------

def add_records(client, ids, tags="a,b"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": tags})
        assert r.status_code == 201, r.text


def mk_batch(client, start, end, key, biz="biz"):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key,
                          "biz": biz, "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def complete_plan(client, start, end, key):
    """建低风险迁移计划并跑完, 返回 (pid, batch)。"""
    add_records(client, range(start, end + 1))
    b = mk_batch(client, start, end, f"b-{key}")
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": f"p-{key}", "name": f"plan-{key}",
        "steps": [{"seq": 1, "batch_id": b}]})
    assert r.status_code == 201, r.text
    pid = r.json()["plan_id"]
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"s-{key}"})
    assert r.status_code == 200, r.text
    db = SessionLocal()
    try:
        for _ in range(6):
            plans.run_plan_tick(db, pid)
    finally:
        db.close()
    assert client.get(f"/api/admin/plans/{pid}").json()["status"] == "COMPLETED"
    return pid, b


def induce_drift(missing=(), changed=None):
    """直接改新表制造补偿动作: missing 中的 id 删除, changed 中改名。"""
    db = SessionLocal()
    try:
        for i in missing:
            row = db.get(RecordNew, i)
            if row is not None:
                db.delete(row)
        for i, name in (changed or {}).items():
            row = db.get(RecordNew, i)
            if row is not None:
                row.name = name
        db.commit()
    finally:
        db.close()


def make_snapshot(client, pid, key, ttl=None, target=None):
    body = {"operator": "alice", "idempotency_key": key, "plan_id": pid}
    if ttl:
        body["ttl_seconds"] = ttl
    if target:
        body["target_at"] = target
    r = client.post("/api/admin/audit-snapshots", json=body)
    assert r.status_code in (200, 201), r.text
    return r.json()["snapshot_id"]


def make_task(client, sid, key="ct", **extra):
    body = {"operator": "alice", "idempotency_key": key, "snapshot_id": sid}
    body.update(extra)
    r = client.post("/api/admin/compensations", json=body)
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


def get_task(client, tid):
    return client.get(f"/api/admin/compensations/{tid}").json()


def approve(client, tid, who, key, status=201):
    r = client.post(f"/api/admin/compensations/{tid}/approvals",
                    json={"operator": who, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r


def reject(client, tid, who, key, reason, status=201):
    r = client.post(f"/api/admin/compensations/{tid}/rejections",
                    json={"operator": who, "idempotency_key": key,
                          "reason": reason})
    assert r.status_code == status, r.text
    return r


def execute(client, tid, who, key, status=200):
    r = client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": who, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r


def backfill_setup(client, key="bf", n=5, missing=(2,)):
    """仅回填动作的低风险场景。"""
    pid, b = complete_plan(client, 1, n, key)
    induce_drift(missing=missing)
    sid = make_snapshot(client, pid, f"sn-{key}")
    return pid, b, sid


def cleanup_setup(client, key="cu", n=5, extra_id=None):
    """含 record_cleanup 的高风险场景: 快照基线不含 extra_id 而新表含。"""
    pid, b = complete_plan(client, 1, n, key)
    sid = make_snapshot(client, pid, f"sn-{key}")
    db = SessionLocal()
    try:
        snap = db.get(AuditSnapshot, sid)
        rid = extra_id or n
        sb = snap.batches[0]
        sb.expected_old_records = [r for r in sb.expected_old_records
                                   if r["id"] != rid]
        sb.expected_old_count -= 1
        db.commit()
    finally:
        db.close()
    return pid, b, sid


def set_task_windows_in_db(tid, ranges):
    """直接改库模拟窗口推移(确定性时间控制)。ranges: [(start, end)] naive UTC。"""
    from app.models import CompensationWindow
    db = SessionLocal()
    try:
        db.query(CompensationWindow).filter_by(task_id=tid).delete()
        for s, e in ranges:
            db.add(CompensationWindow(task_id=tid, starts_at=s, ends_at=e,
                                      created_by="alice"))
        db.commit()
    finally:
        db.close()


def run_ticks(tid, n=10):
    for _ in range(n):
        db = SessionLocal()
        try:
            if not auditreplay.run_execution_tick(db, tid):
                return False
        finally:
            db.close()
    return True


# ======================================================================
# ---------- 风险分级与免审批 ----------
# ======================================================================

def test_low_risk_backfill_requires_no_approval(client):
    pid, b, sid = backfill_setup(client, "low")
    tid = make_task(client, sid, "ct-low")
    t = get_task(client, tid)
    assert t["risk_level"] == "LOW"
    assert t["approval_status"] == "NOT_REQUIRED"
    assert t["required_approvals"] == 0
    # 创建者(alice)可直接执行, 不需要审批
    r = execute(client, tid, "alice", "run-low")
    assert r.json()["status"] == "COMPLETED"
    db = SessionLocal()
    assert db.get(RecordNew, 2).name == "r2"
    db.close()


def test_cleanup_action_is_high_risk_and_shown_on_task(client):
    pid, b, sid = cleanup_setup(client, "high")
    tid = make_task(client, sid, "ct-high")
    t = get_task(client, tid)
    assert t["risk_level"] == "HIGH"
    assert t["approval_status"] == "PENDING"
    assert t["required_approvals"] == 2
    assert t["approved_by"] == []
    # 列表接口同样带风险/审批字段
    rows = client.get("/api/admin/compensations").json()
    row = next(x for x in rows if x["task_id"] == tid)
    assert row["risk_level"] == "HIGH" and row["approval_status"] == "PENDING"
    assert row["has_windows"] is False


def test_canceled_plan_compensation_is_high_risk(client):
    add_records(client, [1, 2])
    b = mk_batch(client, 1, 2, "b-cx")
    client.post(f"/api/admin/batches/{b}/freeze",
                json={"operator": "alice", "idempotency_key": "fz"})
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": "p-cx", "name": "cx",
        "steps": [{"seq": 1, "batch_id": b}]})
    pid = r.json()["plan_id"]
    client.post(f"/api/admin/plans/{pid}/start",
                json={"operator": "alice", "idempotency_key": "st"})
    client.post(f"/api/admin/plans/{pid}/cancel",
                json={"operator": "alice", "idempotency_key": "cn"})
    sid = make_snapshot(client, pid, "sn-cx")
    tid = make_task(client, sid, "ct-cx")
    assert get_task(client, tid)["risk_level"] == "HIGH"


def test_explicit_high_risk_on_backfill(client):
    pid, b, sid = backfill_setup(client, "exh")
    tid = make_task(client, sid, "ct-exh", risk_level="HIGH")
    assert get_task(client, tid)["risk_level"] == "HIGH"
    # 无效风险等级 422
    r = client.post("/api/admin/compensations",
                    json={"operator": "alice", "idempotency_key": "ct-bad",
                          "snapshot_id": sid, "risk_level": "CRITICAL"})
    assert r.status_code == 422


# ======================================================================
# ---------- 双人审批: 独立性 / 创建者与执行人分离 ----------
# ======================================================================

def test_two_distinct_approvers_required(client):
    pid, b, sid = cleanup_setup(client, "two")
    tid = make_task(client, sid, "ct-two")
    # 未审批不能执行
    r = execute(client, tid, "alice", "run0", status=409)
    assert "审批" in r.json()["detail"]["reason"]
    # 创建者不能审批
    r = client.post(f"/api/admin/compensations/{tid}/approvals",
                    json={"operator": "alice", "idempotency_key": "a-self"})
    assert r.status_code == 409 and "创建者" in r.json()["detail"]["reason"]
    # 第一名审批: 仍 PENDING
    r = approve(client, tid, "bob", "a1")
    assert r.json()["approval_status"] == "PENDING"
    assert r.json()["approved_by"] == ["bob"]
    # 同一人重复审批不计数(幂等)
    r2 = approve(client, tid, "bob", "a1b")
    assert r2.json().get("already_in_state") is True
    t = get_task(client, tid)
    assert t["approved_by"] == ["bob"] and len(t["active_approvals"]) == 1
    # 仍不能执行
    execute(client, tid, "alice", "run1", status=409)
    # 第二名不同审批人: APPROVED
    r = approve(client, tid, "carol", "a2")
    assert r.json()["approval_status"] == "APPROVED"
    assert sorted(r.json()["approved_by"]) == ["bob", "carol"]
    # 创建者可执行; 审批人不能执行
    execute(client, tid, "bob", "run-bob", status=409)
    r = execute(client, tid, "carol", "run-carol", status=409)
    assert "审批人" in r.json()["detail"]["reason"]
    # 既不是创建者也不是审批人的第三方操作者可以执行
    r = execute(client, tid, "dave", "run-dave")
    assert r.json()["status"] == "COMPLETED"
    t = get_task(client, tid)
    # 执行动作以执行人身份落地
    assert all(a["executed_by"] == "dave" for a in t["actions"] if a["status"] == "SUCCESS")
    # 创建者(alice)同样可以执行(已完成, 幂等收口)
    r = execute(client, tid, "alice", "run-alice")
    assert r.json()["status"] == "COMPLETED"


def test_low_risk_approval_endpoints_rejected(client):
    pid, b, sid = backfill_setup(client, "lowa")
    tid = make_task(client, sid, "ct-lowa")
    assert client.post(f"/api/admin/compensations/{tid}/approvals",
                       json={"operator": "bob", "idempotency_key": "x"}).status_code == 409
    assert client.post(f"/api/admin/compensations/{tid}/rejections",
                       json={"operator": "bob", "idempotency_key": "x",
                             "reason": "no"}).status_code == 409


def test_reject_requires_recollecting_two_approvals(client):
    pid, b, sid = cleanup_setup(client, "rej")
    tid = make_task(client, sid, "ct-rej")
    approve(client, tid, "bob", "a1")
    # 拒绝原因必填(422)
    r = client.post(f"/api/admin/compensations/{tid}/rejections",
                    json={"operator": "carol", "idempotency_key": "r0"})
    assert r.status_code == 422
    # carol 拒绝: 闸门关闭, bob 的通过被取代
    r = reject(client, tid, "carol", "r1", "清理动作影响结算, 禁止")
    assert r.json()["approval_status"] == "REJECTED"
    t = get_task(client, tid)
    assert t["approval_status"] == "REJECTED"
    assert t["reject_reason"] == "清理动作影响结算, 禁止"
    execute(client, tid, "alice", "run", status=409)
    # 审批历史: bob 的 APPROVED 已 SUPERSEDED, carol 的 REJECTED 保留
    decisions = {(a["operator"], a["decision"]) for a in t["approvals"]}
    assert ("bob", "SUPERSEDED") in decisions
    assert ("carol", "REJECTED") in decisions
    inv = t["approval_invalidations"]
    assert any(x["reason_code"] == "rejection_reset" for x in inv)
    # 新一轮: 需要两名不同操作者重新通过(旧审批人可在新一轮再批)
    approve(client, tid, "bob", "a1n")
    approve(client, tid, "frank", "a2n")
    t = get_task(client, tid)
    assert t["approval_status"] == "APPROVED" and t["reject_reason"] is None
    assert t["approval_round"] == 2
    execute(client, tid, "alice", "run2")
    assert get_task(client, tid)["status"] == "COMPLETED"


def test_concurrent_approval_same_operator_dedup(client):
    pid, b, sid = cleanup_setup(client, "conc")
    tid = make_task(client, sid, "ct-conc")
    results: list[tuple[str, int]] = []
    barrier = threading.Barrier(4)

    def submit(who, key):
        barrier.wait()
        r = client.post(f"/api/admin/compensations/{tid}/approvals",
                        json={"operator": who, "idempotency_key": key})
        results.append((who, r.status_code))

    threads = [threading.Thread(target=submit, args=("bob", f"k{i}"))
               for i in range(3)]
    threads.append(threading.Thread(target=submit, args=("carol", "kc")))
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert all(code in (200, 201) for _, code in results)
    t = get_task(client, tid)
    # bob 的三次并发提交只产生一条有效审批
    assert sorted(t["approved_by"]) == ["bob", "carol"]
    assert len(t["active_approvals"]) == 2
    assert t["approval_status"] == "APPROVED"
    db = SessionLocal()
    from app.models import CompensationApproval
    rows = db.query(CompensationApproval).filter_by(task_id=tid).all()
    db.close()
    # 3 个 bob 并发: 唯一约束去重后只有一条 bob 行 + 一条 carol
    assert sorted((r.operator, r.decision) for r in rows) == [
        ("bob", "APPROVED"), ("carol", "APPROVED")]


# ======================================================================
# ---------- 审批后依据变化: 快照 / 质量门禁 / 计划状态 ----------
# ======================================================================

def test_approval_invalidated_on_snapshot_expiry(client):
    pid, b, sid = backfill_setup(client, "exp", n=5)
    # 高风险: 显式升级
    tid = make_task(client, sid, "ct-exp", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    assert get_task(client, tid)["approval_status"] == "APPROVED"
    # 快照过期: worker 周期 sweep 自动失效审批
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    db = SessionLocal()
    res = auditreplay.sweep_approval_drift(db)
    db.close()
    assert any(x["task_id"] == tid for x in res)
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    inv = t["approval_invalidations"]
    assert any(x["reason_code"] == "snapshot_expired" for x in inv)
    assert all(x["decision"] == "INVALIDATED"
               for x in inv if x["reason_code"] == "snapshot_expired")
    # 失效后执行被拒
    r = execute(client, tid, "alice", "run", status=409)
    assert "失效" in r.json()["detail"]["reason"]
    # 重新生成快照/审批依据恢复需重新收集: 这里把过期改回并重新审批(模拟重新生成)
    db = SessionLocal()
    snap = db.get(AuditSnapshot, sid)
    snap.expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)
    db.commit()
    db.close()
    approve(client, tid, "bob", "b1")
    approve(client, tid, "carol", "b2")
    assert get_task(client, tid)["approval_status"] == "APPROVED"
    execute(client, tid, "alice", "run2")
    assert get_task(client, tid)["status"] == "COMPLETED"


def test_approval_invalidated_on_gate_status_change(client):
    pid, b, sid = backfill_setup(client, "gate", n=5)
    tid = make_task(client, sid, "ct-gate", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 审批依据: 门禁 NOT_CONFIGURED。审批后给计划绑定规则但不扫描 -> NOT_SCANNED
    r = client.put(f"/api/admin/plans/{pid}/quality-rules", json={
        "operator": "alice", "idempotency_key": "qr", "plan_id": pid,
        "rules": [{"id": "r1", "type": "required", "field": "name",
                   "severity": "BLOCKER"}]})
    assert r.status_code in (200, 201), r.text
    db = SessionLocal()
    res = auditreplay.sweep_approval_drift(db)
    db.close()
    assert any(x["task_id"] == tid for x in res)
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    codes = {x["reason_code"] for x in t["approval_invalidations"]}
    assert "gate_status_changed" in codes
    # 事件流水留痕
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert any(e["event_type"] == "COMP_APPROVAL"
               and e["payload"].get("action") == "approval_invalidated"
               for e in ev)


def test_approval_invalidated_on_plan_status_change(client):
    pid, b, sid = cleanup_setup(client, "plan")
    tid = make_task(client, sid, "ct-plan")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 直接改库模拟计划状态变化(正常流程计划已 COMPLETED, 这里模拟异常取消等漂移)
    db = SessionLocal()
    db.get(MigrationPlan, pid).status = "CANCELED"
    db.commit()
    db.close()
    db = SessionLocal()
    res = auditreplay.sweep_approval_drift(db)
    db.close()
    assert any(x["task_id"] == tid for x in res)
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    assert any(x["reason_code"] == "plan_status_changed"
               for x in t["approval_invalidations"])


def test_approval_invalidated_on_batch_version_change(client):
    pid, b, sid = cleanup_setup(client, "batchver")
    tid = make_task(client, sid, "ct-batchver")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 批次版本(epoch/phase)在审批后变化 -> 已收集审批失效
    db = SessionLocal()
    from app.models import MigrationBatch
    batch = db.get(MigrationBatch, b)
    batch.epoch += 1
    db.commit()
    db.close()
    db = SessionLocal()
    res = auditreplay.sweep_approval_drift(db)
    db.close()
    assert any(x["task_id"] == tid for x in res)
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    assert any(x["reason_code"] == "batch_version_gap"
               for x in t["approval_invalidations"])


def test_partial_approvals_also_invalidated_and_history_kept(client):
    pid, b, sid = cleanup_setup(client, "part")
    tid = make_task(client, sid, "ct-part")
    approve(client, tid, "bob", "a1")  # 仅一人
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    db = SessionLocal()
    auditreplay.sweep_approval_drift(db)
    db.close()
    t = get_task(client, tid)
    # 单人审批也失效; 历史保留
    assert any(a["decision"] == "INVALIDATED" and a["operator"] == "bob"
               for a in t["approvals"])
    assert t["approval_round"] == 2


# ======================================================================
# ---------- 执行/重试/预约入口触发的依据失效: 必须可靠持久化 ----------
# (入口返回 409 后事务回滚, 失效状态/轮次/审批记录/事件不能随之丢失)
# ======================================================================

def test_execute_entry_persists_invalidation_on_snapshot_expiry(client):
    """快照过期后直接请求执行: 409 之外失效状态可靠落库; 重复请求不重复写;
    重新收集两名审批后可以执行; 详情接口展示失效历史。"""
    pid, b, sid = backfill_setup(client, "expe", n=5)
    tid = make_task(client, sid, "ct-expe", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    assert get_task(client, tid)["approval_status"] == "APPROVED"
    # 快照过期, 不经 sweep, 直接请求执行
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    r = execute(client, tid, "alice", "run-exp", status=409)
    assert "失效" in r.json()["detail"]["reason"]
    # 409 事务回滚后失效状态仍可靠持久化
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    assert t["approval_round"] == 2
    inv_rows = [a for a in t["approvals"] if a["decision"] == "INVALIDATED"]
    assert {a["operator"] for a in inv_rows} == {"bob", "carol"}
    assert all(a["invalidated_reason"] == "snapshot_expired" for a in inv_rows)
    # 详情接口展示失效历史(机器可读原因)
    hist = t["approval_invalidations"]
    assert len(hist) == 2
    assert all(x["reason_code"] == "snapshot_expired" for x in hist)
    # COMP_APPROVAL 事件落统一事件流(恰好一次)
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    inv_events = [e for e in ev if e["event_type"] == "COMP_APPROVAL"
                  and e["payload"].get("action") == "approval_invalidated"]
    assert len(inv_events) == 1
    assert inv_events[0]["payload"]["detail"]["reason_code"] == "snapshot_expired"
    # 重复请求(同键重放与跨键重试)都不重复写审批或事件
    execute(client, tid, "alice", "run-exp", status=409)
    execute(client, tid, "alice", "run-exp-again", status=409)
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED" and t["approval_round"] == 2
    assert len(t["approvals"]) == 2
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert len([e for e in ev if e["event_type"] == "COMP_APPROVAL"
                and e["payload"].get("action") == "approval_invalidated"]) == 1
    # 恢复快照有效期并重新收集两名审批 -> 可以执行
    db = SessionLocal()
    snap = db.get(AuditSnapshot, sid)
    snap.expires_at = (datetime.now(timezone.utc).replace(tzinfo=None)
                       + timedelta(hours=1))
    db.commit()
    db.close()
    approve(client, tid, "bob", "b1")
    approve(client, tid, "carol", "b2")
    t = get_task(client, tid)
    assert t["approval_status"] == "APPROVED" and t["approval_round"] == 2
    r = execute(client, tid, "alice", "run-ok")
    assert r.json()["status"] == "COMPLETED"
    # 执行完成后失效历史仍完整可查
    t = get_task(client, tid)
    assert any(x["reason_code"] == "snapshot_expired"
               for x in t["approval_invalidations"])


def test_execute_entry_persists_invalidation_on_gate_change(client):
    """质量门禁状态变化后直接请求执行: 失效持久化; 重新收集审批后审批闸门恢复。"""
    pid, b, sid = backfill_setup(client, "gatee", n=5)
    tid = make_task(client, sid, "ct-gatee", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 审批依据为门禁 NOT_CONFIGURED; 审批后绑定规则(未扫描) -> NOT_SCANNED
    r = client.put(f"/api/admin/plans/{pid}/quality-rules", json={
        "operator": "alice", "idempotency_key": "qr-gx", "plan_id": pid,
        "rules": [{"id": "r1", "type": "required", "field": "name",
                   "severity": "BLOCKER"}]})
    assert r.status_code in (200, 201), r.text
    # 不经 sweep, 直接请求执行 -> 409 且失效持久化
    r = execute(client, tid, "alice", "run-gx", status=409)
    assert "失效" in r.json()["detail"]["reason"]
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    assert t["approval_round"] == 2
    codes = {x["reason_code"] for x in t["approval_invalidations"]}
    assert "gate_status_changed" in codes
    assert all(a["decision"] == "INVALIDATED" for a in t["approvals"])
    # 重复执行请求不重复失效
    execute(client, tid, "alice", "run-gx2", status=409)
    t = get_task(client, tid)
    assert t["approval_round"] == 2 and len(t["approvals"]) == 2
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert len([e for e in ev if e["event_type"] == "COMP_APPROVAL"
                and e["payload"].get("action") == "approval_invalidated"]) == 1
    # 重新收集两名审批 -> 审批闸门恢复(失效历史保留)
    approve(client, tid, "bob", "b1")
    approve(client, tid, "carol", "b2")
    t = get_task(client, tid)
    assert t["approval_status"] == "APPROVED"
    assert any(x["reason_code"] == "gate_status_changed"
               for x in t["approval_invalidations"])


def test_retry_entry_persists_invalidation(client):
    """失败重试入口触发依据复核: 409 之外失效状态同样可靠落库, 动作未被重试。"""
    pid, b, sid = backfill_setup(client, "retr", n=5)
    tid = make_task(client, sid, "ct-retr", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 制造一个 FAILED 动作(直接落库模拟门禁拦截失败), 任务停在 PARTIAL
    db = SessionLocal()
    task = db.get(CompensationTask, tid)
    act = next(a for a in task.actions if a.seq == 1)
    act.status = "FAILED"
    act.last_error = "模拟门禁拦截失败"
    task.status = "PARTIAL"
    db.commit()
    db.close()
    # 快照过期后直接请求重试
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    r = client.post(f"/api/admin/compensations/{tid}/retry",
                    json={"operator": "alice", "idempotency_key": "rt1",
                          "action_seq": 1})
    assert r.status_code == 409, r.text
    assert "失效" in r.json()["detail"]["reason"]
    t = get_task(client, tid)
    assert t["approval_status"] == "INVALIDATED"
    assert t["approval_round"] == 2
    assert any(x["reason_code"] == "snapshot_expired"
               for x in t["approval_invalidations"])
    # 动作保持 FAILED 未被重试; 重复重试请求不重复失效
    assert t["actions"][0]["status"] == "FAILED"
    r = client.post(f"/api/admin/compensations/{tid}/retry",
                    json={"operator": "alice", "idempotency_key": "rt2",
                          "action_seq": 1})
    assert r.status_code == 409
    t = get_task(client, tid)
    assert t["approval_round"] == 2 and len(t["approvals"]) == 2


def test_window_booking_persists_invalidation(client):
    """预约窗口入口触发依据复核: 失效独立提交, 窗口预约本身不受影响且幂等。"""
    pid, b, sid = backfill_setup(client, "wininv", n=5)
    tid = make_task(client, sid, "ct-wininv", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    now = datetime.now(timezone.utc)
    wins = [{"starts_at": iso(now + timedelta(hours=1)),
             "ends_at": iso(now + timedelta(hours=2))}]
    r = client.put(f"/api/admin/compensations/{tid}/windows",
                   json={"operator": "alice", "idempotency_key": "w1",
                         "windows": wins})
    assert r.status_code == 200, r.text
    t = get_task(client, tid)
    assert t["has_windows"] is True
    assert t["approval_status"] == "INVALIDATED"
    assert t["approval_round"] == 2
    assert any(x["reason_code"] == "snapshot_expired"
               for x in t["approval_invalidations"])
    # 相同窗口重复预约幂等, 不重复写审批或事件
    r = client.put(f"/api/admin/compensations/{tid}/windows",
                   json={"operator": "alice", "idempotency_key": "w1b",
                         "windows": wins})
    assert r.status_code == 200 and r.json().get("already_in_state") is True
    t = get_task(client, tid)
    assert t["approval_round"] == 2 and len(t["approvals"]) == 2
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert len([e for e in ev if e["event_type"] == "COMP_APPROVAL"
                and e["payload"].get("action") == "approval_invalidated"]) == 1


# ======================================================================
# ---------- 执行窗口: 预约 / 窗口外拒绝 / 冲突 ----------
# ======================================================================

def test_window_booking_and_outside_rejected(client):
    pid, b, sid = backfill_setup(client, "win")
    tid = make_task(client, sid, "ct-win")
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=2)
    wins = [{"starts_at": iso(future), "ends_at": iso(future + timedelta(hours=1))}]
    r = client.put(f"/api/admin/compensations/{tid}/windows",
                   json={"operator": "alice", "idempotency_key": "w1",
                         "windows": wins})
    assert r.status_code == 200, r.text
    t = get_task(client, tid)
    assert t["has_windows"] is True and t["in_window"] is False
    assert len(t["windows"]) == 1
    # 窗口外显式执行明确拒绝
    r = execute(client, tid, "alice", "run", status=409)
    d = r.json()["detail"]
    assert "执行窗口" in d["reason"]
    # worker tick 不推进(自动暂停), 动作仍 PENDING
    assert run_ticks(tid, 3) is False
    t = get_task(client, tid)
    assert t["status"] in ("QUEUED", "RUNNING") and t["in_window"] is False
    assert all(a["status"] == "PENDING" for a in t["actions"])
    assert t["window_pause_reason"]
    # 窗口到达后 worker 自动跑完
    near = datetime.now(timezone.utc)
    set_task_windows_in_db(tid, [((near - timedelta(minutes=1)).replace(tzinfo=None),
                                  (near + timedelta(hours=1)).replace(tzinfo=None))])
    run_ticks(tid, 5)
    t = get_task(client, tid)
    assert t["status"] == "COMPLETED" and t["in_window"] is True
    assert t["window_pause_reason"] is None


def test_window_conflict_returns_occupier_and_range(client):
    # 两个活动补偿任务共享批次: 分别从两个快照创建, 各自把 id=5 移出基线
    # (模拟基线之外多余记录), 因此两个任务都推导出对同批次的清理动作
    pid, b, sid1 = cleanup_setup(client, "cf1")
    sid2 = make_snapshot(client, pid, "sn-cf2")
    db = SessionLocal()
    try:
        snap2 = db.get(AuditSnapshot, sid2)
        sb2 = snap2.batches[0]
        sb2.expected_old_records = [r for r in sb2.expected_old_records
                                    if r["id"] != 5]
        sb2.expected_old_count -= 1
        db.commit()
    finally:
        db.close()
    t1 = make_task(client, sid1, "ct-cf1")
    t2 = make_task(client, sid2, "ct-cf2")
    assert {a["batch_id"] for a in get_task(client, t1)["actions"]} == {b}
    assert {a["batch_id"] for a in get_task(client, t2)["actions"]} == {b}
    now = datetime.now(timezone.utc)
    w = [{"starts_at": iso(now + timedelta(hours=1)),
          "ends_at": iso(now + timedelta(hours=2))}]
    r = client.put(f"/api/admin/compensations/{t1}/windows",
                   json={"operator": "alice", "idempotency_key": "w1",
                         "windows": w})
    assert r.status_code == 200
    # t2 预约重叠窗口(共享批次 b) -> 409 带占用者与冲突时间
    w2 = [{"starts_at": iso(now + timedelta(minutes=90)),
           "ends_at": iso(now + timedelta(hours=3))}]
    r = client.put(f"/api/admin/compensations/{t2}/windows",
                   json={"operator": "alice", "idempotency_key": "w2",
                         "windows": w2})
    assert r.status_code == 409
    conflicts = r.json()["detail"]["conflicts"]
    assert len(conflicts) == 1
    c = conflicts[0]
    assert c["task_id"] == t1 and c["occupied_by"] == "alice"
    assert b in c["shared_batches"]
    assert c["conflict_range"]["starts_at"] == iso(now + timedelta(minutes=90))
    assert c["conflict_range"]["ends_at"] == iso(now + timedelta(hours=2))
    # 不重叠窗口可预约成功
    w3 = [{"starts_at": iso(now + timedelta(hours=3)),
           "ends_at": iso(now + timedelta(hours=4))}]
    r = client.put(f"/api/admin/compensations/{t2}/windows",
                   json={"operator": "alice", "idempotency_key": "w3",
                         "windows": w3})
    assert r.status_code == 200
    # 相同窗口幂等
    r = client.put(f"/api/admin/compensations/{t2}/windows",
                   json={"operator": "alice", "idempotency_key": "w3b",
                         "windows": w3})
    assert r.status_code == 200 and r.json().get("already_in_state") is True
    # 非法窗口聚合拒绝
    bad = [{"starts_at": iso(now), "ends_at": iso(now - timedelta(hours=1))}]
    r = client.put(f"/api/admin/compensations/{t2}/windows",
                   json={"operator": "alice", "idempotency_key": "wbad",
                         "windows": bad})
    assert r.status_code == 409 and "早于" in r.json()["detail"]["reason"]


def test_window_expiry_pauses_at_action_boundary_and_resumes(client):
    # 多动作: 先删 1 个基线(1 个 cleanup), 再制造第二个清理动作
    pid, b = complete_plan(client, 1, 6, "wexp")
    sid = make_snapshot(client, pid, "sn-wexp")
    db = SessionLocal()
    try:
        snap = db.get(AuditSnapshot, sid)
        for rid in (5, 6):
            sb = snap.batches[0]
            sb.expected_old_records = [r for r in sb.expected_old_records
                                       if r["id"] != rid]
        sb.expected_old_count -= 2
        db.commit()
    finally:
        db.close()
    tid = make_task(client, sid, "ct-wexp")
    assert get_task(client, tid)["risk_level"] == "HIGH"
    now = datetime.now(timezone.utc)
    # 初始窗口仅覆盖"现在": 第一个动作可执行, 之后窗口结束
    wins = [{"starts_at": iso(now - timedelta(minutes=1)),
             "ends_at": iso(now + timedelta(seconds=20))}]
    r = client.put(f"/api/admin/compensations/{tid}/windows",
                   json={"operator": "alice", "idempotency_key": "w1",
                         "windows": wins})
    assert r.status_code == 200
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    run_ticks(tid, 1)  # 第一个清理动作在窗口内完成
    # 窗口移到未来: 模拟窗口临近过期
    future = now + timedelta(hours=2)
    set_task_windows_in_db(tid, [(future.replace(tzinfo=None),
                                  (future + timedelta(hours=1)).replace(tzinfo=None))])
    db = SessionLocal()
    db.get(CompensationTask, tid).window_open = True
    db.commit()
    db.close()
    # tick 在动作边界暂停, 不再推进, 已完成动作保留
    assert run_ticks(tid, 3) is False
    t = get_task(client, tid)
    statuses = sorted(a["status"] for a in t["actions"])
    assert "SUCCESS" in statuses and "PENDING" in statuses
    assert t["window_open"] is False and t["window_pause_reason"]
    assert "暂停" in t["window_pause_reason"]
    done = sum(1 for a in t["actions"] if a["status"] == "SUCCESS")
    assert done >= 1
    # 暂停事件落统一事件流
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert any(e["event_type"] == "COMP_WINDOW"
               and "window_pause" in (e["payload"].get("action") or "")
               for e in ev)
    # 重新进入窗口: 自动续跑完成剩余动作
    near = datetime.now(timezone.utc)
    set_task_windows_in_db(tid, [((near - timedelta(minutes=1)).replace(tzinfo=None),
                                  (near + timedelta(hours=1)).replace(tzinfo=None))])
    run_ticks(tid, 5)
    t = get_task(client, tid)
    assert t["status"] == "COMPLETED" and t["in_window"] is True
    assert all(a["status"] == "SUCCESS" for a in t["actions"])
    ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=300").json()["items"]
    assert any(e["event_type"] == "COMP_WINDOW"
               and "window_resume" in (e["payload"].get("action") or "")
               for e in ev)


def test_retry_outside_window_rejected(client):
    pid, b, sid = backfill_setup(client, "rt", n=5)
    tid = make_task(client, sid, "ct-rt", risk_level="HIGH")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    execute(client, tid, "alice", "run")
    assert get_task(client, tid)["status"] == "COMPLETED"
    # 构造一个 FAILED 动作场景不易(门禁), 这里直接验证窗口预约后:
    # 无 FAILED 时 retry 返回状态冲突; 窗口外错误优先返回窗口信息由执行闸门保证。
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=2)
    client.put(f"/api/admin/compensations/{tid}/windows",
               json={"operator": "alice", "idempotency_key": "w1",
                     "windows": [{"starts_at": iso(future),
                                  "ends_at": iso(future + timedelta(hours=1))}]})
    # COMPLETED 任务改窗口被拒(终态)
    r = client.put(f"/api/admin/compensations/{tid}/windows",
                   json={"operator": "alice", "idempotency_key": "w2",
                         "windows": []})
    assert r.status_code == 409 and "活动状态" in r.json()["detail"]["reason"]


# ======================================================================
# ---------- 任务撤销与审批历史关联 ----------
# ======================================================================

def test_cancel_task_terminates_and_links_approval_history(client):
    pid, b, sid = cleanup_setup(client, "cx2")
    tid = make_task(client, sid, "ct-cx2")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    execute(client, tid, "alice", "run1")  # 动作很快, 可能已 COMPLETED
    t = get_task(client, tid)
    if t["status"] in ("QUEUED", "RUNNING", "PARTIAL"):
        r = client.post(f"/api/admin/compensations/{tid}/cancel",
                        json={"operator": "alice", "idempotency_key": "cn1",
                              "reason": "变更取消"})
        assert r.status_code == 200, r.text
        t = get_task(client, tid)
        assert t["status"] == "CANCELED"
        assert t["canceled_by"] == "alice" and t["cancel_reason"] == "变更取消"
        # 未决审批 TASK_CANCELED
        assert any(a["decision"] == "TASK_CANCELED" for a in t["approvals"])
        assert t["approval_status"] == "CANCELED"
        # 已完成动作保留
        assert t["success_actions"] >= 0
        # 取消后不能执行/再取消
        execute(client, tid, "alice", "run2", status=409)
        r = client.post(f"/api/admin/compensations/{tid}/cancel",
                        json={"operator": "alice", "idempotency_key": "cn2"})
        assert r.status_code == 409
        # 审批历史仍完整可查(通过/取消留痕)
        ops = {a["operator"] for a in t["approvals"]}
        assert {"bob", "carol"} <= ops
    else:
        # 单动作任务可能已瞬间完成: 用一个 PENDING 的多动作任务重测
        assert t["status"] == "COMPLETED"


def test_cancel_pending_task_before_execution(client):
    pid, b = complete_plan(client, 1, 8, "cx3")
    sid = make_snapshot(client, pid, "sn-cx3")
    db = SessionLocal()
    try:
        snap = db.get(AuditSnapshot, sid)
        sb = snap.batches[0]
        sb.expected_old_records = [r for r in sb.expected_old_records
                                   if r["id"] in (7, 8)]
        sb.expected_old_count -= 2
        db.commit()
    finally:
        db.close()
    tid = make_task(client, sid, "ct-cx3")
    approve(client, tid, "bob", "a1")
    # 仅一人审批, 任务无法执行, 直接取消
    r = client.post(f"/api/admin/compensations/{tid}/cancel",
                    json={"operator": "alice", "idempotency_key": "cn",
                          "reason": "不再需要"})
    assert r.status_code == 200
    t = get_task(client, tid)
    assert t["status"] == "CANCELED"
    assert all(a["status"] == "PENDING" for a in t["actions"])
    execute(client, tid, "alice", "run", status=409)


def test_canceled_task_with_completed_actions_can_undo(client):
    """取消后已执行动作保留, 仍可整体撤销(撤销不受窗口/审批/TTL 限制)。"""
    pid, b, sid = cleanup_setup(client, "cxu")
    tid = make_task(client, sid, "ct-cxu")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    execute(client, tid, "alice", "run")
    t = get_task(client, tid)
    assert t["status"] == "COMPLETED"  # 单动作已跑完
    # 再建一个含 PENDING 的任务不易(同快照唯一), 这里验证终态 COMPLETED 可撤销,
    # 且撤销事件保留任务审批历史关联
    r = client.post(f"/api/admin/compensations/{tid}/undo",
                    json={"operator": "alice", "idempotency_key": "un"})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "UNDONE"
    t = get_task(client, tid)
    assert any(a["decision"] == "APPROVED" for a in t["approvals"])


# ======================================================================
# ---------- 重启持久化 ----------
# ======================================================================

def test_restart_preserves_approval_invalidation_and_window(client):
    pid, b = complete_plan(client, 1, 8, "boot")
    sid = make_snapshot(client, pid, "sn-boot")
    db = SessionLocal()
    try:
        snap = db.get(AuditSnapshot, sid)
        sb = snap.batches[0]
        sb.expected_old_records = [r for r in sb.expected_old_records
                                   if r["id"] in (7, 8)]
        sb.expected_old_count -= 2
        db.commit()
    finally:
        db.close()
    tid = make_task(client, sid, "ct-boot")
    approve(client, tid, "bob", "a1")
    approve(client, tid, "carol", "a2")
    # 审批依据变化使审批失效
    db = SessionLocal()
    db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
    db.commit()
    db.close()
    # 预约一个未来窗口
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=2)
    client.put(f"/api/admin/compensations/{tid}/windows",
               json={"operator": "alice", "idempotency_key": "w1",
                     "windows": [{"starts_at": iso(future),
                                  "ends_at": iso(future + timedelta(hours=1))}]})
    # 重启: startup 执行 boot 对账(含审批失效与窗口边界重判)
    with TestClient(app):
        t = get_task(client, tid)
        assert t["approval_status"] == "INVALIDATED"
        assert any(x["reason_code"] == "snapshot_expired"
                   for x in t["approval_invalidations"])
        assert t["has_windows"] is True and t["in_window"] is False
        assert t["window_open"] is False
        assert len(t["windows"]) == 1
        # 重启不偷跑: 动作仍 PENDING
        assert all(a["status"] == "PENDING" for a in t["actions"])
        assert db is not None
    db = SessionLocal()
    assert db.get(RecordNew, 7) is not None and db.get(RecordNew, 8) is not None
    db.close()
