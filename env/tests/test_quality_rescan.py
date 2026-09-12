"""质量结果失效后的自动重扫编排测试。

覆盖:
- 三种失效来源(批次数据写入 / 规则版本变化 / 扫描过期)自动为受影响的执行态
  计划创建唯一重扫任务, 重复触发只合并触发原因不产生重复任务;
- 计划执行器在步骤边界遇到门禁失效即暂停(quality hold), 停在原步骤不复位;
  重扫完成且阻断问题处理后从原步骤自动继续(可恢复正常推进/批次后续可继续);
- 重扫失败恢复(resume 后自愈)、计划取消后不再自动恢复;
- 跨批次影响范围、并发触发去重、重启后待处理重扫与暂停状态保留;
- 页面/接口展示触发来源、重扫关联、暂停原因与恢复历史。

后台 worker 关闭: 用 plans.run_plan_tick / quality.claim_due_scans /
quality.run_scan_tick / quality.sweep_due_rescans 做确定性驱动。
"""
import os
import tempfile
import uuid
from datetime import datetime, timedelta

_tmp = tempfile.mkdtemp(prefix="migration-rescan-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "2"
os.environ["QUALITY_SCAN_TTL_SECONDS"] = "86400"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    MigrationBatch, MigrationPlan, QualityGateHold, QualityScan, RecordOld,
)
from app import plans, quality


def _key(prefix="k"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


EMAIL_RULE = {
    "id": "email_format", "name": "邮箱必须合法", "type": "format",
    "field": "email", "severity": "BLOCKER",
    "params": {"pattern": r"[^@\s]+@[^@\s]+\.[^@\s]+"},
}
NAME_REQUIRED_RULE = {
    "id": "name_required", "name": "名称必填", "type": "required",
    "field": "name", "severity": "BLOCKER",
}


@pytest.fixture(autouse=True)
def env_conf():
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "2"
    os.environ["QUALITY_SCAN_TTL_SECONDS"] = "86400"
    yield
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "2"
    os.environ["QUALITY_SCAN_TTL_SECONDS"] = "86400"


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 辅助 ----------

def mk_records(client, ids, *, email="@x.com"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}",
                              "email": f"r{i}{email}", "tags_csv": "a"})
        assert r.status_code == 201, r.text


def mk_bad_email(client, rid):
    r = client.post("/api/records",
                    json={"id": rid, "name": f"r{rid}",
                          "email": "bad", "tags_csv": "a"})
    assert r.status_code == 201, r.text


def write_record(client, rid, *, name=None, email=None, tags="a"):
    return client.post("/api/records",
                       json={"id": rid, "name": name or f"r{rid}",
                             "email": email if email is not None else f"r{rid}@x.com",
                             "tags_csv": tags})


def mk_batch(client, start, end, key=None, biz=None):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key or _key("kb"),
                          "biz": biz or f"biz-{start}", "id_start": start,
                          "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def mk_plan(client, batches, key=None, max_retries=0, deps=True):
    if isinstance(batches, str):
        batches = [batches]
    steps = [{"seq": 1, "batch_id": batches[0], "max_retries": max_retries}]
    for i, b in enumerate(batches[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "max_retries": max_retries,
                      **({"depends_on": [i - 1]} if deps else {})})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key or _key("kp"),
                          "name": "重扫编排测试", "max_retries": max_retries,
                          "steps": steps})
    assert r.status_code == 201, r.text
    return r.json()["plan_id"]


def save_rules(client, pid, rules, key=None, note=None):
    body = {"operator": "alice", "idempotency_key": key or _key("rules"),
            "plan_id": pid, "rules": rules}
    if note:
        body["note"] = note
    r = client.put(f"/api/admin/plans/{pid}/quality-rules", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def create_scan(client, pid, key=None):
    r = client.post(f"/api/admin/plans/{pid}/quality-scans",
                    json={"operator": "alice", "idempotency_key": key or _key("sc"),
                          "plan_id": pid})
    assert r.status_code == 201, r.text
    return r.json()


def claim():
    db = SessionLocal()
    try:
        return quality.claim_due_scans(db)
    finally:
        db.close()


def tick_scan(sid):
    db = SessionLocal()
    try:
        return quality.run_scan_tick(db, sid)
    finally:
        db.close()


def run_scan(sid, n=30):
    claim()
    for _ in range(n):
        if not tick_scan(sid):
            break


def run_rescan_to_end(sid, n=30):
    """自动重扫排队后驱动到终态(认领+逐批次)。"""
    run_scan(sid, n)
    d = scan_detail(sid)
    assert d["status"] in ("COMPLETED", "FAILED", "CANCELED"), d["status"]
    return d


def scan_detail(client, sid):
    return client.get(f"/api/admin/quality-scans/{sid}").json()


def gate(client, pid):
    return client.get(f"/api/admin/plans/{pid}/quality-gate").json()


def holds(client, pid):
    return client.get(f"/api/admin/plans/{pid}/quality-holds").json()


def plan_detail(client, pid):
    return client.get(f"/api/admin/plans/{pid}").json()


def start_plan(client, pid, key=None):
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": key or _key("start")})
    assert r.status_code == 200, r.text
    return r.json()


def cancel_plan(client, pid, key=None, status=200):
    r = client.post(f"/api/admin/plans/{pid}/cancel",
                    json={"operator": "alice", "idempotency_key": key or _key("cancel")})
    assert r.status_code == status, r.text
    return r.json()


def plan_tick(pid):
    db = SessionLocal()
    try:
        return plans.run_plan_tick(db, pid)
    finally:
        db.close()


def run_plan(pid, n=30):
    for _ in range(n):
        if not plan_tick(pid):
            break


def sweep():
    db = SessionLocal()
    try:
        return quality.sweep_due_rescans(db)
    finally:
        db.close()


def running_plan_with_pass_scan(client, batches, **kw):
    """创建计划(可多批线性依赖) -> 绑定邮箱规则 -> 扫描 PASS -> 启动。
    返回 (pid, initial_scan_id, batches)。"""
    if isinstance(batches, str):
        batches = [batches]
    pid = mk_plan(client, batches, **kw)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    assert gate(client, pid)["status"] == "PASS"
    start_plan(client, pid)
    return pid, sid, batches


def latest_rescan(client, pid):
    rows = client.get("/api/admin/quality-scans",
                      params={"plan_id": pid}).json()
    auto = [r for r in rows if r["is_auto_rescan"]]
    return auto[-1] if auto else None


# ---------- 1. 批次数据写入触发自动重扫 ----------

def test_batch_write_to_running_plan_enqueues_unique_rescan(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid, sid, _ = running_plan_with_pass_scan(client, [b1, b2])
    # 执行第一步: 批次 b1 完成切换(其数据不再可变); b2 仍 NORMAL 待执行
    assert plan_tick(pid) is True
    p = plan_detail(client, pid)
    assert p["status"] == "RUNNING" and p["steps"][0]["batch_phase"] == "DONE"
    # 向尚未执行的批次 b2 写入(内容变化) -> 唯一自动重扫
    r = write_record(client, 15, email="r15-new@x.com")
    j = r.json()
    assert j["rescans"] and j["rescans"][0]["created"] is True
    rescan_id = j["rescans"][0]["scan_id"]
    row = scan_detail(client, rescan_id)
    assert row["is_auto_rescan"] is True
    assert row["trigger_source"] == "batch_data_write"
    assert row["supersedes_scan_id"] == sid
    assert row["affected_batch_ids"] == [b2]
    assert row["progress"]["total"] == 2  # 重扫覆盖计划全部批次
    assert row["triggers"][0]["source"] == "batch_data_write"
    # 重复写入(数据变化) -> 合并到同一任务, 不产生第二个重扫
    write_record(client, 16, email="r16-new@x.com")
    rows = [x for x in client.get("/api/admin/quality-scans",
                                  params={"plan_id": pid}).json()
            if x["is_auto_rescan"]]
    assert [x["id"] for x in rows] == [rescan_id]
    merged = scan_detail(client, rescan_id)
    sources = [t["source"] for t in merged["triggers"]]
    assert sources.count("batch_data_write") >= 1


def test_write_outside_or_irrelevant_does_not_enqueue(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    # DRAFT 计划: 不自动排队(沿用手动扫描流程)
    pid = mk_plan(client, b1)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    j = write_record(client, 5, email="r5-new@x.com").json()
    assert j["rescans"] == []
    # 批次外记录写入也不触发
    j = write_record(client, 999, email="r999-new@x.com").json()
    assert j["rescans"] == []


def test_same_value_write_does_not_trigger_rescan(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    # 幂等写入(内容完全相同)不触发
    j = write_record(client, 3, email="r3@x.com", name="r3", tags="a").json()
    assert j["rescans"] == []


# ---------- 2. 规则版本变化触发 ----------

def test_rule_version_change_enqueues_rescan_for_running_plan(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, sid, _ = running_plan_with_pass_scan(client, b1)
    # 发布新规则版本(执行中) -> 保存事务内即时编排基于 v2 的自动重扫
    j = save_rules(client, pid, [EMAIL_RULE, NAME_REQUIRED_RULE], note="加必填")
    assert j["version"] == 2
    rescan = latest_rescan(client, pid)
    assert rescan is not None
    assert rescan["rule_version"] == 2
    assert rescan["trigger_source"] == "rule_version_change"
    assert rescan["supersedes_scan_id"] == sid
    # sweep 发现重扫已在途, 不重复创建
    assert all(x["plan_id"] != pid for x in sweep())


def test_rule_version_change_draft_plan_no_auto_rescan(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid = mk_plan(client, b1)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    save_rules(client, pid, [EMAIL_RULE, NAME_REQUIRED_RULE])
    assert sweep() == []
    assert latest_rescan(client, pid) is None


# ---------- 3. 扫描过期触发(worker 兜底) ----------

def test_expired_scan_enqueued_by_sweep(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, sid, _ = running_plan_with_pass_scan(client, b1)
    db = SessionLocal()
    db.get(QualityScan, sid).expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit()
    db.close()
    assert gate(client, pid)["status"] == "STALE"
    res = sweep()
    assert any(x["plan_id"] == pid and x["trigger"] == "scan_expired" for x in res)
    rescan = latest_rescan(client, pid)
    assert rescan["trigger_source"] == "scan_expired"
    # 第二次 sweep: 活动重扫在途, 不重复创建(merged)
    res2 = sweep()
    assert all(x["plan_id"] != pid for x in res2)
    rows = [x for x in client.get("/api/admin/quality-scans",
                                  params={"plan_id": pid}).json()
            if x["is_auto_rescan"]]
    assert len(rows) == 1


# ---------- 4. 执行器门禁暂停 + 自动恢复(干净数据) ----------

def test_executor_pauses_and_resumes_from_same_step_clean_rescan(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    # 步骤 seq=1 尚未执行: 写入同范围但合法的数据(指纹漂移, 重扫将 PASS)
    write_record(client, 4, email="r4-new@x.com")
    # worker tick: 执行器在步骤边界发现 STALE -> 暂停, 编排重扫, 不推进步骤
    assert plan_tick(pid) is False
    p = plan_detail(client, pid)
    assert p["status"] == "RUNNING" and p["quality_hold_flag"] is True
    hold = p["quality_hold"]
    assert hold["paused_at_seq"] == 1
    # 重扫已在途(QUEUED), 门禁状态为 RUNNING; 首次触发来源仍是数据写入
    assert hold["reason_code"] == "RUNNING"
    assert hold["trigger_source"] == "batch_data_write"
    assert hold["rescan_scan_id"]
    step = p["steps"][0]
    assert step["status"] == "PENDING" and step["attempts"] == 0  # 原步骤不复位
    # 待处理重扫完成(数据合法 -> PASS) -> 暂停自动解除
    run_scan(hold["rescan_scan_id"])
    p = plan_detail(client, pid)
    assert p["quality_hold_flag"] is False
    assert p["quality_hold"] is None
    h = holds(client, pid)
    assert h["active"] is None and h["history"][0]["status"] == "RESUMED"
    assert h["history"][0]["resume_mode"] == "auto_gate_pass"
    assert h["history"][0]["resume_scan_id"]
    # 从原步骤继续, 计划正常跑完
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"
    audits = client.get("/api/admin/audit", params={"plan_id": pid}).json()
    actions = [a["action"] for a in audits]
    assert "plan.quality_hold" in actions
    assert "plan.quality_resume" in actions


def test_executor_pauses_when_rescan_finds_blocker(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    # 写入违规数据(邮箱不合法)
    write_record(client, 5, email="broken")
    assert plan_tick(pid) is False  # 编排重扫并暂停
    p = plan_detail(client, pid)
    hold1 = p["quality_hold"]
    # 重扫刚排队: 门禁视图为 RUNNING(等待扫描完成), 首次触发来源是数据写入
    assert hold1["reason_code"] == "RUNNING"
    assert hold1["trigger_source"] == "batch_data_write"
    run_scan(hold1["rescan_scan_id"])
    # 重扫发现阻断问题: 门禁 BLOCKED, 暂停保持, 原因更新
    p = plan_detail(client, pid)
    assert p["quality_hold_flag"] is True
    assert p["quality_hold"]["reason_code"] == "BLOCKED"
    assert gate(client, pid)["status"] == "BLOCKED"
    # 修复数据 -> 指纹又漂移 -> 修复接口本身为执行态计划编排重扫
    blockers = client.get(f"/api/admin/plans/{pid}/quality-issues",
                          params={"severity": "BLOCKER",
                                  "status_filter": "OPEN"}).json()
    assert len(blockers) == 1
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 5).update({"email": "r5@x.com"})
    db.commit()
    db.close()
    # sweep 兜底: 数据已修 -> 编排新重扫(无活动扫描)
    res = sweep()
    assert any(x["plan_id"] == pid for x in res)
    rescan2 = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    run_scan(rescan2)
    # 门禁 PASS -> 自动恢复, 跑完
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"
    h = holds(client, pid)["history"][0]
    assert h["status"] == "RESUMED"


def test_exemption_unblocks_hold_without_new_scan(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 5, email="broken")
    plan_tick(pid)
    hold_id = plan_detail(client, pid)["quality_hold"]["id"]
    run_scan(plan_detail(client, pid)["quality_hold"]["rescan_scan_id"])
    assert plan_detail(client, pid)["quality_hold"]["reason_code"] == "BLOCKED"
    # 豁免阻断问题(不改数据、不重扫) -> 门禁凭当前有效扫描 PASS -> 自动恢复
    issue = client.get(f"/api/admin/plans/{pid}/quality-issues",
                       params={"severity": "BLOCKER",
                               "status_filter": "OPEN"}).json()[0]
    r = client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                    json={"operator": "carol", "idempotency_key": _key("ex"),
                          "plan_id": pid, "issue_id": issue["id"],
                          "reason": "接受该历史数据"})
    assert r.status_code == 201, r.text
    p = plan_detail(client, pid)
    assert p["quality_hold_flag"] is False
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"


# ---------- 5. 重复触发不产生重复任务(并发去重) ----------

def test_repeated_triggers_merge_into_single_rescan(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    db = SessionLocal()
    r1 = quality.enqueue_rescan(db, pid, trigger="batch_data_write",
                                reason="写入1", affected_batch_ids=[b1])
    r2 = quality.enqueue_rescan(db, pid, trigger="batch_data_write",
                                reason="写入2(同源合并)", affected_batch_ids=[b1])
    r3 = quality.enqueue_rescan(db, pid, trigger="scan_expired", reason="恰好又过期")
    db.close()
    assert r1["created"] and not r2["created"] and r2["merged"]
    assert r3["scan_id"] == r1["scan_id"]
    sc = scan_detail(client, r1["scan_id"])
    sources = sorted(t["source"] for t in sc["triggers"])
    assert sources == ["batch_data_write", "scan_expired"]
    # 全计划范围内只有这一个自动重扫
    db = SessionLocal()
    n = db.query(QualityScan).filter_by(
        plan_id=pid, scan_source="auto_rescan").count()
    db.close()
    assert n == 1


def test_concurrent_enqueue_calls_single_task(client):
    """两个独立会话同时为同一计划编排重扫(模拟并发触发): 咨询锁/写锁串行化,
    重复触发只合并, 不落第二个任务。"""
    import threading
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    out = []

    def fire(trigger):
        db = SessionLocal()
        try:
            out.append(quality.enqueue_rescan(
                db, pid, trigger=trigger, reason="并发触发",
                affected_batch_ids=[b1]))
        finally:
            db.close()

    t1 = threading.Thread(target=fire, args=("batch_data_write",))
    t2 = threading.Thread(target=fire, args=("rule_version_change",))
    t1.start(); t2.start(); t1.join(); t2.join()
    db = SessionLocal()
    rows = db.query(QualityScan).filter_by(
        plan_id=pid, scan_source="auto_rescan").all()
    db.close()
    assert len(rows) == 1
    assert {o["scan_id"] for o in out} == {rows[0].id}


# ---------- 6. 重扫失败恢复 ----------

def test_failed_rescan_keeps_hold_and_resume_self_heals(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 3, email="r3-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    claim()
    # 重扫执行中批次被删 -> 任务 FAILED
    db = SessionLocal()
    db.query(MigrationBatch).filter(MigrationBatch.id == b1).delete()
    db.commit()
    db.close()
    tick_scan(rescan_id)
    assert scan_detail(client, rescan_id)["status"] == "FAILED"
    # 计划仍暂停(不偷跑), 原因反映扫描失败
    p = plan_detail(client, pid)
    assert p["quality_hold_flag"] is True
    g = gate(client, pid)
    assert g["status"] == "FAILED" and g["quality_hold"]["rescan_scan_id"] == rescan_id
    # 恢复批次 -> resume 重扫 -> 成功 -> 自动恢复计划
    db = SessionLocal()
    db.add(MigrationBatch(id=b1, biz="biz-1", id_start=1, id_end=5,
                          phase="NORMAL", epoch=0, active_schema="old",
                          created_by="alice", updated_by="alice"))
    db.commit()
    db.close()
    r = client.post(f"/api/admin/quality-scans/{rescan_id}/resume",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200, r.text
    run_scan(rescan_id)
    assert plan_detail(client, pid)["quality_hold_flag"] is False
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"


# ---------- 7. 计划取消后不再自动恢复 ----------

def test_cancel_plan_cancels_rescan_and_never_resumes(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 4, email="r4-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    # 在暂停/重扫在途时取消计划
    j = cancel_plan(client, pid)
    assert j["quality"]["scans_canceled"] == [rescan_id]
    p = plan_detail(client, pid)
    assert p["status"] == "CANCELED" and p["quality_hold_flag"] is False
    assert scan_detail(client, rescan_id)["status"] == "CANCELED"
    h = holds(client, pid)
    assert h["history"][0]["status"] == "CANCELED"
    assert h["history"][0]["canceled_by"] == "alice"
    # 即使把重扫重新跑完成并门禁 PASS, 计划也不会复活
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "CANCELED"
    # sweep 不会为已取消计划再编排
    write_record(client, 5, email="r5-new@x.com")
    assert sweep() == []
    db = SessionLocal()
    assert db.query(QualityScan).filter_by(
        plan_id=pid, scan_source="auto_rescan").count() == 1
    db.close()


def test_cancel_completed_plan_does_not_resurrect(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"
    # 完成后批次已 DONE: 旧路径写入 410 拒绝, 不编排; sweep 也不会复活终态计划
    r = write_record(client, 3, email="r3-new@x.com")
    assert r.status_code == 410
    assert sweep() == []


# ---------- 8. 跨批次影响范围 ----------

def test_rescan_covers_all_plan_batches_with_affected_scope(client):
    mk_records(client, range(1, 31))
    b1, b2, b3 = (mk_batch(client, 1, 10, key="k1"),
                  mk_batch(client, 11, 20, key="k2"),
                  mk_batch(client, 21, 30, key="k3"))
    pid, _, _ = running_plan_with_pass_scan(client, [b1, b2, b3])
    # 第一步完成: b1 DONE, b2/b3 待执行
    plan_tick(pid)
    # 写入 b3(第三步批次): 重扫仍覆盖全部 3 个批次, 受影响范围记录为 [b3]
    write_record(client, 25, email="r25-new@x.com")
    plan_tick(pid)  # 暂停
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    d = scan_detail(client, rescan_id)
    assert d["progress"]["total"] == 3
    assert d["affected_batch_ids"] == [b3]
    assert {x["batch_id"] for x in d["batches"]} == {b1, b2, b3}
    run_scan(rescan_id)
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"


def test_batch_in_terminal_plan_does_not_enqueue_when_done(client):
    # 已完成计划的批次处于 DONE 终态: 旧路径写入被 410 拒绝, 不会编排重扫;
    # 同一批次被其他执行态计划占用在创建期就被拒绝(批次不可复用),
    # 因此用两个独立批次验证: 已完成计划的批次不再受影响。
    mk_records(client, range(1, 11))
    b1, b2 = mk_batch(client, 1, 5, key="kb1"), mk_batch(client, 6, 10, key="kb2")
    p_done, _, _ = running_plan_with_pass_scan(client, b1, key="kpd")
    run_plan(p_done)
    assert plan_detail(client, p_done)["status"] == "COMPLETED"
    # DONE 批次旧路径写入明确失败(410), 不触发任何重扫
    r = write_record(client, 3, email="r3-new@x.com")
    assert r.status_code == 410
    assert sweep() == []
    db = SessionLocal()
    n = db.query(QualityScan).filter_by(
        plan_id=p_done, scan_source="auto_rescan").count()
    db.close()
    assert n == 0
    # 另一个执行态计划的批次写入仍正常触发(互不影响)
    p2, _, _ = running_plan_with_pass_scan(client, b2, key="kp2")
    j = write_record(client, 7, email="r7-new@x.com").json()
    assert {x["plan_id"] for x in j["rescans"]} == {p2}


# ---------- 9. 重启持久化 ----------

def test_restart_preserves_pending_rescan_and_hold(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid, _, _ = running_plan_with_pass_scan(client, [b1, b2])
    write_record(client, 15, email="r15-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    # 重扫跑到一半(一个批次 SUCCESS, 任务 RUNNING)后"重启"
    claim()
    tick_scan(rescan_id)
    db = SessionLocal()
    quality.boot_recover_scans(db)
    db.close()
    d = scan_detail(client, rescan_id)
    assert d["status"] == "QUEUED" and d["progress"]["done"] == 1
    p = plan_detail(client, pid)
    assert p["status"] == "RUNNING" and p["quality_hold_flag"] is True
    assert p["quality_hold"]["rescan_scan_id"] == rescan_id
    # 暂停步骤仍是原步骤(PENDING, 计数不丢)
    assert p["steps"][0]["status"] == "PENDING"
    # worker 续跑重扫 -> 完成 -> 暂停解除 -> 计划继续
    run_scan(rescan_id)
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"
    # hold 历史完整(暂停 -> 恢复)
    hist = holds(client, pid)["history"]
    assert [x["status"] for x in hist] == ["RESUMED"]


def test_restart_sweep_enqueues_expired_rescan(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, sid, _ = running_plan_with_pass_scan(client, b1)
    db = SessionLocal()
    db.get(QualityScan, sid).expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit()
    db.close()
    # 重启后先做 boot 对账, worker 首个 tick 的 sweep 负责补编排
    db = SessionLocal()
    quality.boot_recover_scans(db)
    db.close()
    res = sweep()
    assert any(x["plan_id"] == pid and x["trigger"] == "scan_expired" for x in res)


# ---------- 10. 接口与页面数据展示 ----------

def test_gate_and_views_expose_trigger_and_hold(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, sid, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 3, email="r3-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    g = gate(client, pid)
    assert g["active_rescan_id"] == rescan_id
    assert g["quality_hold"]["trigger_source"] == "batch_data_write"
    # 最新扫描已是自动重扫; 它通过 supersedes 关联取代的旧 PASS 扫描
    assert g["latest_scan_id"] == rescan_id
    assert g["latest_scan_source"] == "auto_rescan"
    # 扫描列表项带来源/触发/取代关联
    rows = client.get("/api/admin/quality-scans", params={"plan_id": pid}).json()
    auto = [r for r in rows if r["id"] == rescan_id][0]
    assert auto["scan_source"] == "auto_rescan"
    assert auto["supersedes_scan_id"] == sid
    assert auto["triggers"][0]["batch_ids"] == [b1]
    # 扫描详情事件流含编排事件
    events = [e["event"] for e in scan_detail(client, rescan_id)["events"]]
    assert "rescan.enqueue" in events
    # 计划质量总览带门禁暂停
    ov = client.get(f"/api/admin/plans/{pid}/quality-overview").json()
    assert ov["gate"]["quality_hold"]["id"]
    # 全局 status 计划卡片数据含 hold 标记
    s = client.get("/api/status").json()
    p = [x for x in s["plans"] if x["id"] == pid][0]
    assert p["quality_hold_flag"] is True
    assert p["quality_hold"]["rescan_scan_id"] == rescan_id


def test_hold_history_records_multiple_episodes(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    # 第一轮: 写入合法数据 -> 暂停 -> 重扫 PASS -> 恢复
    write_record(client, 3, email="r3-a@x.com")
    plan_tick(pid)
    h1 = plan_detail(client, pid)["quality_hold"]["id"]
    run_scan(plan_detail(client, pid)["quality_hold"]["rescan_scan_id"])
    assert holds(client, pid)["active"] is None
    # 计划尚未继续推进时再次写入 -> 新一轮暂停(新 hold 行)
    write_record(client, 4, email="r4-a@x.com")
    plan_tick(pid)
    h2 = plan_detail(client, pid)["quality_hold"]["id"]
    assert h2 != h1
    run_scan(plan_detail(client, pid)["quality_hold"]["rescan_scan_id"])
    hist = holds(client, pid)["history"]
    assert [h["status"] for h in hist] == ["RESUMED", "RESUMED"]
    # 第一轮记录了首个触发来源与暂停步骤
    first = next(h for h in hist if h["id"] == h1)
    assert first["trigger_source"] == "batch_data_write"
    assert first["paused_at_seq"] == 1


# ---------- 11. 手动扫描在执行期暂停中的行为 ----------

def test_manual_scan_allowed_during_hold_and_releases_it(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 3, email="r3-new@x.com")
    plan_tick(pid)
    auto_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    # 已有活动(自动)扫描时手动发起: 幂等返回活动任务, 不产生第二个
    j = create_scan(client, pid, key=_key("manual"))
    assert j["already_active"] is True and j["scan_id"] == auto_id
    # 取消自动重扫(模拟放弃), 再手动扫描: 门禁暂停状态允许手动发起
    client.post(f"/api/admin/quality-scans/{auto_id}/cancel",
                json={"operator": "alice", "idempotency_key": _key()})
    j2 = create_scan(client, pid, key=_key("manual2"))
    assert j2["already_active"] is False and j2["scan_id"] != auto_id
    run_scan(j2["scan_id"])
    # 手动扫描通过门禁 -> 暂停同样自动解除
    assert plan_detail(client, pid)["quality_hold_flag"] is False
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"


def test_manual_scan_rejected_for_plain_running_plan(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    # RUNNING 且无质量暂停: 不允许手动扫描(只有 DRAFT/hold 中允许)
    r = client.post(f"/api/admin/plans/{pid}/quality-scans",
                    json={"operator": "alice", "idempotency_key": _key(),
                          "plan_id": pid})
    assert r.status_code == 409


# ---------- 12. 边界: 暂停/停住计划与质量暂停的叠加 ----------

def test_gate_checked_at_boundary_after_a_step_completes(client):
    """两步计划: 第一步完成后、推进第二步前的步骤边界也复评门禁 ——
    执行期间扫描过期时, 下一步批次不会在门禁失效时被切换。"""
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid, sid, _ = running_plan_with_pass_scan(client, [b1, b2])
    # 第一步完成: b1 DONE
    assert plan_tick(pid) is True
    p = plan_detail(client, pid)
    assert p["steps"][0]["batch_phase"] == "DONE"
    # 此时扫描结果过期(TTL)
    db = SessionLocal()
    db.get(QualityScan, sid).expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit(); db.close()
    # 下一个 tick: 在步骤边界暂停, b2 不被冻结/切换, 并自动编排重扫
    assert plan_tick(pid) is False
    p = plan_detail(client, pid)
    assert p["quality_hold_flag"] is True
    assert p["quality_hold"]["paused_at_seq"] == 2
    assert p["steps"][1]["status"] == "PENDING"
    db = SessionLocal()
    b2phase = db.get(MigrationBatch, b2).phase
    db.close()
    assert b2phase == "NORMAL"  # 第二步批次未被动到
    # 重扫完成(数据未变 -> 指纹漂移判定只比对 SUCCESS 批次 b1, 其旧表仍在, PASS)
    run_scan(p["quality_hold"]["rescan_scan_id"])
    assert plan_detail(client, pid)["quality_hold_flag"] is False
    run_plan(pid)
    assert plan_detail(client, pid)["status"] == "COMPLETED"


def test_user_pause_then_cancel_during_hold(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 3, email="r3-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    # 用户手动暂停: 计划进入 PAUSED(质量暂停标记保留)
    r = client.post(f"/api/admin/plans/{pid}/pause",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200, r.text
    assert plan_detail(client, pid)["status"] == "PAUSED"
    # 取消: hold 终止、重扫取消
    cancel_plan(client, pid)
    assert scan_detail(client, rescan_id)["status"] == "CANCELED"
    assert plan_detail(client, pid)["quality_hold_flag"] is False


def test_rescan_paused_scan_does_not_release_hold(client):
    mk_records(client, range(1, 6))
    b1 = mk_batch(client, 1, 5, key="kb1")
    pid, _, _ = running_plan_with_pass_scan(client, b1)
    write_record(client, 3, email="r3-new@x.com")
    plan_tick(pid)
    rescan_id = plan_detail(client, pid)["quality_hold"]["rescan_scan_id"]
    # 管理员暂停重扫: 计划保持暂停, 不恢复
    client.post(f"/api/admin/quality-scans/{rescan_id}/pause",
                json={"operator": "alice", "idempotency_key": _key()})
    plan_tick(pid)
    assert plan_detail(client, pid)["quality_hold_flag"] is True
    # 恢复重扫并完成 -> 自动恢复
    client.post(f"/api/admin/quality-scans/{rescan_id}/resume",
                json={"operator": "alice", "idempotency_key": _key()})
    run_scan(rescan_id)
    assert plan_detail(client, pid)["quality_hold_flag"] is False
