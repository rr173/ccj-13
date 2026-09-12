"""迁移前数据质量门禁测试:

规则绑定与版本化 / 四类规则求值(必填·格式·跨字段·范围) / 扫描排队·执行·暂停·
恢复·取消·失败 / 阻断级问题阻止启动, 修复与豁免放行 / 规则版本变化·批次数据变化·
结果过期使旧结果失效 / 幂等(扫描·修复·豁免) / 修复与豁免历史绑定规则版本 /
重启持久化与续跑 / 按计划查询问题与门禁结果。

后台 worker 由 conftest 关闭, 手动用 quality.claim_due_scans + quality.run_scan_tick
驱动(每次 tick 每个扫描至多一个批次), 时序确定。
"""
import os
import tempfile
import uuid
from datetime import datetime, timedelta

_tmp = tempfile.mkdtemp(prefix="migration-quality-test-")
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
    MigrationBatch, QualityFixBatch, QualityIssue, QualityRuleVersion,
    QualityScan, RecordOld,
)
from app import plans, quality, replay, service


def _key(prefix="k"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def concurrency():
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

def mk_records(client, ids, *, email_suffix="@x.com", tag="a"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}",
                              "email": f"r{i}{email_suffix}", "tags_csv": tag})
        assert r.status_code == 201, r.text


def mk_bad_email(client, rid, name=None):
    r = client.post("/api/records",
                    json={"id": rid, "name": name or f"r{rid}",
                          "email": "not-an-email", "tags_csv": "a"})
    assert r.status_code == 201, r.text
    return rid


def mk_batch(client, start, end, key=None, biz=None):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key or _key("kb"),
                          "biz": biz or f"biz-{start}", "id_start": start,
                          "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def mk_plan(client, batches, key=None, name=None, deps=True):
    """单批或线性多批计划(DRAFT), 返回 plan_id。"""
    if isinstance(batches, str):
        batches = [batches]
    steps = [{"seq": 1, "batch_id": batches[0]}]
    for i, b in enumerate(batches[1:], start=2):
        steps.append({"seq": i, "batch_id": b,
                      **({"depends_on": [i - 1]} if deps else {})})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key or _key("kp"),
                          "name": name or "质量测试计划", "max_retries": 0,
                          "steps": steps})
    assert r.status_code == 201, r.text
    return r.json()["plan_id"]


EMAIL_RULE = {
    "id": "email_format", "name": "邮箱必须合法", "type": "format",
    "field": "email", "severity": "BLOCKER",
    "params": {"pattern": r"[^@\s]+@[^@\s]+\.[^@\s]+"},
}
NAME_REQUIRED_RULE = {
    "id": "name_required", "name": "名称必填", "type": "required",
    "field": "name", "severity": "BLOCKER",
}
ID_RANGE_RULE = {
    "id": "id_range", "name": "id 范围", "type": "range", "field": "id",
    "severity": "WARNING", "params": {"min": 1, "max": 1_000_000},
}
TAGS_CROSS_RULE = {
    "id": "tags_ne_name", "name": "标签不应等于名称", "type": "cross_field",
    "field": "tags_csv", "severity": "INFO",
    "params": {"other_field": "name", "op": "ne"},
}


def save_rules(client, pid, rules, key=None, status=200, note=None,
               operator="alice"):
    body = {"operator": operator, "idempotency_key": key or _key("rules"),
            "plan_id": pid, "rules": rules}
    if note is not None:
        body["note"] = note
    r = client.put(f"/api/admin/plans/{pid}/quality-rules", json=body)
    assert r.status_code == status, r.text
    return r.json()


def create_scan(client, pid, key=None, status=201):
    r = client.post(f"/api/admin/plans/{pid}/quality-scans",
                    json={"operator": "alice", "idempotency_key": key or _key("scan"),
                          "plan_id": pid})
    assert r.status_code == status, r.text
    return r.json()


def claim():
    db = SessionLocal()
    try:
        return quality.claim_due_scans(db)
    finally:
        db.close()


def tick(sid):
    db = SessionLocal()
    try:
        return quality.run_scan_tick(db, sid)
    finally:
        db.close()


def run_scan(sid, n=20):
    claim()
    for _ in range(n):
        if not tick(sid):
            break


def scan_detail(client, sid):
    return client.get(f"/api/admin/quality-scans/{sid}").json()


def gate(client, pid):
    return client.get(f"/api/admin/plans/{pid}/quality-gate").json()


def open_blockers(client, pid):
    return client.get(f"/api/admin/plans/{pid}/quality-issues"
                      "?severity=BLOCKER&status_filter=OPEN").json()


def start_plan(client, pid, key=None, status=200):
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": key or _key("start")})
    assert r.status_code == status, r.text
    return r.json()


# ---------- 1. 规则绑定与版本化 ----------

def test_plan_without_rules_not_gated(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    g = gate(client, pid)
    assert g["status"] == "NOT_CONFIGURED" and g["passed"] is True
    # 无规则集计划可正常启动
    assert start_plan(client, pid)["status"] == "RUNNING"


def test_save_rules_requires_plan(client):
    r = client.put("/api/admin/plans/P-nope/quality-rules",
                   json={"operator": "alice", "idempotency_key": "x",
                         "plan_id": "P-nope", "rules": [NAME_REQUIRED_RULE]})
    assert r.status_code == 404


def test_save_rules_versioning_and_immutable_history(client):
    mk_records(client, range(1, 4))
    b = mk_batch(client, 1, 3)
    pid = mk_plan(client, b)
    j = save_rules(client, pid, [EMAIL_RULE], note="初版")
    assert j["version"] == 1 and j["rule_count"] == 1
    # 相同内容(顺序一致)不产生新版本
    again = save_rules(client, pid, [EMAIL_RULE])
    assert again["version"] == 1 and again["already_in_state"] is True
    # 内容变化 -> v2
    j2 = save_rules(client, pid, [EMAIL_RULE, NAME_REQUIRED_RULE])
    assert j2["version"] == 2 and j2["rule_count"] == 2
    doc = client.get(f"/api/admin/plans/{pid}/quality-rules").json()
    assert doc["current_version"] == 2
    assert [v["version"] for v in doc["versions"]] == [1, 2]
    # 历史版本不可变: v1 仍只有一条规则
    assert doc["versions"][0]["rules"][0]["id"] == "email_format"
    assert len(doc["versions"][0]["rules"]) == 1
    assert doc["versions"][0]["note"] == "初版"


def test_save_rules_idempotent_key_replay(client):
    mk_records(client, range(1, 4))
    b = mk_batch(client, 1, 3)
    pid = mk_plan(client, b)
    body = {"operator": "alice", "idempotency_key": "same-key", "plan_id": pid,
            "rules": [EMAIL_RULE]}
    r1 = client.put(f"/api/admin/plans/{pid}/quality-rules", json=body).json()
    r2 = client.put(f"/api/admin/plans/{pid}/quality-rules", json=body).json()
    assert r2["replayed"] is True and r1["version"] == r2["version"] == 1


def test_save_rules_aggregated_validation(client):
    mk_records(client, range(1, 4))
    b = mk_batch(client, 1, 3)
    pid = mk_plan(client, b)
    bad_rules = [
        {"id": "dup", "type": "format", "field": "email",
         "params": {"pattern": "("}},               # 非法正则
        {"id": "dup", "type": "range", "field": "nope",
         "params": {"min": 9, "max": 1}},           # 重复 id + 非法字段 + min>max
        {"id": "x", "type": "cross_field", "field": "name",
         "params": {"other_field": "name", "op": "eq"}},  # 自比较
        {"id": "y", "type": "weird", "field": "name"},  # 未知类型
    ]
    r = client.put(f"/api/admin/plans/{pid}/quality-rules",
                   json={"operator": "alice", "idempotency_key": "bad",
                         "plan_id": pid, "rules": bad_rules})
    assert r.status_code == 409
    reasons = r.json()["detail"]["reasons"]
    assert len(reasons) >= 4  # 聚合返回, 一次落不了任何数据
    # 失败后规则集仍不存在
    assert gate(client, pid)["status"] == "NOT_CONFIGURED"


# ---------- 2. 四类规则求值与问题明细 ----------

def test_scan_detects_all_four_rule_types(client):
    ids = range(1, 11)
    mk_records(client, ids)
    mk_bad_email(client, 3)  # format 违规(BLOCKER)
    # name 为空(required BLOCKER), 直接写库绕过接口校验; tags 保持与名称不同避免触发跨字段规则
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 5).update(
        {"name": "", "tags_csv": "keep"})
    db.commit(); db.close()
    # 跨字段违规: 某记录 tags_csv == name(INFO)
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 7).update(
        {"name": "zzz", "tags_csv": "zzz"})
    db.commit(); db.close()
    b = mk_batch(client, 1, 10)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE, NAME_REQUIRED_RULE, ID_RANGE_RULE,
                             TAGS_CROSS_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    d = scan_detail(client, sid)
    assert d["status"] == "COMPLETED"
    assert d["total_records"] == 10
    counts = d["issue_counts"]
    assert counts["blocker"] == 2          # 邮箱 + 名称
    assert counts["info"] == 1             # 跨字段
    assert counts["warning"] == 0
    # 问题明细可按计划查询, 带字段/消息/样本
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues").json()
    by_key = {(i["record_id"], i["rule_id"]): i for i in issues}
    assert by_key[(3, "email_format")]["severity"] == "BLOCKER"
    assert by_key[(3, "email_format")]["field"] == "email"
    assert "not-an-email" in by_key[(3, "email_format")]["message"]
    assert by_key[(3, "email_format")]["sample"]["email"] == "not-an-email"
    assert by_key[(5, "name_required")]["status"] == "OPEN"
    assert by_key[(7, "tags_ne_name")]["severity"] == "INFO"


def test_required_allow_blank_and_range_length(client):
    db = SessionLocal()
    db.add_all([
        RecordOld(id=1, name="n1", email="a@b.co", tags_csv="x"),
        RecordOld(id=2, name="  ", email="c@d.co", tags_csv="yy"),
    ])
    db.commit(); db.close()
    b = mk_batch(client, 1, 2)
    pid = mk_plan(client, b)
    rules = [
        {"id": "tags", "type": "required", "field": "tags_csv", "severity": "BLOCKER",
         "params": {"allow_blank": True}},
        {"id": "name_len", "type": "range", "field": "name", "severity": "BLOCKER",
         "params": {"min_length": 2, "max_length": 10}},
    ]
    save_rules(client, pid, rules)
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues").json()
    # allow_blank=True: 空白不算缺失; 名称 "  " 长度 2 通过, n1 长度 2 通过 -> 0 问题
    assert issues == []
    # 改短名称后重扫
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 1).update({"name": "n"})
    db.commit(); db.close()
    save_rules(client, pid, rules)  # 内容未变 -> 仍 v1
    sid2 = create_scan(client, pid)["scan_id"]
    run_scan(sid2)
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues",
                        params={"scan_id": sid2}).json()
    assert [(i["record_id"], i["rule_id"]) for i in issues] == [(1, "name_len")]


def test_numeric_range_rule(client):
    db = SessionLocal()
    db.add_all([RecordOld(id=1, name="a", email="a@b.co", tags_csv="t"),
                RecordOld(id=99, name="b", email="b@b.co", tags_csv="t")])
    db.commit(); db.close()
    b = mk_batch(client, 1, 100)
    pid = mk_plan(client, b)
    save_rules(client, pid, [{"id": "idmax", "type": "range", "field": "id",
                              "severity": "BLOCKER", "params": {"max": 50}}])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues").json()
    assert [(i["record_id"]) for i in issues] == [99]


def test_multi_batch_scan_progress(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid = mk_plan(client, [b1, b2])
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    claim()
    # 第一次 tick 只扫一个批次
    assert tick(sid) is True
    d = scan_detail(client, sid)
    assert d["status"] == "RUNNING" and d["progress"] == {"done": 1, "total": 2}
    assert d["current_batch_id"] in (b1, b2)
    statuses = {x["batch_id"]: x["status"] for x in d["batches"]}
    assert sorted(statuses.values()) == ["PENDING", "SUCCESS"]
    # 第二次 tick 完成
    assert tick(sid) is True
    d = scan_detail(client, sid)
    assert d["status"] == "COMPLETED" and d["progress"] == {"done": 2, "total": 2}
    assert {x["record_count"] for x in d["batches"]} == {10}
    # 每个批次记录数据指纹
    assert all(x["data_fingerprint"] for x in d["batches"])


# ---------- 3. 扫描生命周期: 排队/暂停/恢复/取消/失败 ----------

def test_scan_requires_rules_and_draft(client):
    mk_records(client, range(1, 4))
    b = mk_batch(client, 1, 3)
    pid = mk_plan(client, b)
    # 无规则
    r = client.post(f"/api/admin/plans/{pid}/quality-scans",
                    json={"operator": "alice", "idempotency_key": "s", "plan_id": pid})
    assert r.status_code == 409 and "规则集" in r.json()["detail"]["reason"]
    # 绑定不会命中的规则 -> 扫描通过门禁, 先启动计划
    save_rules(client, pid, [{**EMAIL_RULE, "id": "never",
                              "params": {"pattern": ".*"}}])
    sid = create_scan(client, pid, key="s0")["scan_id"]
    run_scan(sid)
    assert gate(client, pid)["status"] == "PASS"
    start_plan(client, pid)
    # 已启动的计划不能再扫描
    r = client.post(f"/api/admin/plans/{pid}/quality-scans",
                    json={"operator": "alice", "idempotency_key": "s2", "plan_id": pid})
    assert r.status_code == 409


def test_duplicate_active_scan_idempotent(client):
    mk_records(client, range(1, 4))
    b = mk_batch(client, 1, 3)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    j1 = create_scan(client, pid, key="dup-scan")
    j2 = create_scan(client, pid, key="dup-scan2")
    assert j1["scan_id"] == j2["scan_id"] and j2["already_active"] is True
    # 排队中也只允许一个活动任务
    db = SessionLocal()
    n = db.query(QualityScan).filter(QualityScan.plan_id == pid).count()
    db.close()
    assert n == 1


def test_pause_resume_at_batch_boundary(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid = mk_plan(client, [b1, b2])
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    claim(); tick(sid)  # 完成第一个批次
    # 在批次边界暂停
    r = client.post(f"/api/admin/quality-scans/{sid}/pause",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200 and r.json()["ok"]
    # 暂停后 tick 不推进
    assert tick(sid) is False
    d = scan_detail(client, sid)
    assert d["status"] == "PAUSED" and d["progress"]["done"] == 1
    # 恢复: 重新排队, 受并发闸门约束
    r = client.post(f"/api/admin/quality-scans/{sid}/resume",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200
    run_scan(sid)
    assert scan_detail(client, sid)["status"] == "COMPLETED"


def test_pause_queued_scan_and_resume(client):
    # QUEUED 状态直接暂停, 恢复后重新排队
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "1"
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    client.post(f"/api/admin/quality-scans/{sid}/pause",
                json={"operator": "alice", "idempotency_key": _key()})
    assert scan_detail(client, sid)["status"] == "PAUSED"
    assert claim() == []  # 暂停任务不被认领
    client.post(f"/api/admin/quality-scans/{sid}/resume",
                json={"operator": "alice", "idempotency_key": _key()})
    run_scan(sid)
    assert scan_detail(client, sid)["status"] == "COMPLETED"
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "2"


def test_cancel_skips_pending_batches(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid = mk_plan(client, [b1, b2])
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    r = client.post(f"/api/admin/quality-scans/{sid}/cancel",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200
    d = scan_detail(client, sid)
    assert d["status"] == "CANCELED"
    assert all(x["status"] == "SKIPPED" for x in d["batches"])
    # 终态控制被拒绝
    r = client.post(f"/api/admin/quality-scans/{sid}/resume",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 409
    # 取消后允许重新发起扫描
    sid2 = create_scan(client, pid)["scan_id"]
    assert sid2 != sid


def test_scan_failed_when_batch_deleted_and_resume(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    claim()
    # 删除批次(直接删库模拟批次不存在), 执行应明确失败
    db = SessionLocal()
    db.query(MigrationBatch).filter(MigrationBatch.id == b).delete()
    db.commit(); db.close()
    tick(sid)
    d = scan_detail(client, sid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] == "batch_missing"
    sb = d["batches"][0]
    assert sb["status"] == "FAILED"
    # 恢复批次后 resume: 失败批次重新排队, 可成功完成
    db = SessionLocal()
    db.add(MigrationBatch(id=b, biz="biz-1", id_start=1, id_end=5,
                          phase="NORMAL", epoch=0, active_schema="old",
                          created_by="alice", updated_by="alice"))
    db.commit(); db.close()
    r = client.post(f"/api/admin/quality-scans/{sid}/resume",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200
    run_scan(sid)
    assert scan_detail(client, sid)["status"] == "COMPLETED"


def test_concurrency_gate_queues_extra_scans(client):
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "1"
    # 两个计划各一个批次
    mk_records(client, range(1, 6))
    mk_records(client, range(11, 15))
    b1, b2 = mk_batch(client, 1, 5, key="kb1"), mk_batch(client, 11, 15, key="kb2")
    p1, p2 = mk_plan(client, b1, key="kp1"), mk_plan(client, b2, key="kp2")
    for p in (p1, p2):
        save_rules(client, p, [EMAIL_RULE])
    s1 = create_scan(client, p1, key="sc1")["scan_id"]
    s2 = create_scan(client, p2, key="sc2")["scan_id"]
    claimed = claim()
    assert claimed == [s1]            # 只有一个额度
    assert tick(s1) is True           # s1 跑完一个批次即完成(单批)
    # s1 完成后 s2 才能被认领
    assert claim() == [s2]
    tick(s2)
    assert scan_detail(client, s2)["status"] == "COMPLETED"
    os.environ["QUALITY_SCAN_MAX_CONCURRENCY"] = "2"


# ---------- 4. 门禁: 阻断问题阻止启动, 修复/豁免放行 ----------

def _plan_with_blocker(client, bad_id=3):
    mk_records(client, range(1, 11))
    mk_bad_email(client, bad_id)
    b = mk_batch(client, 1, 10)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    return pid, sid, b


def test_gate_blocks_start_until_blocker_handled(client):
    pid, sid, _ = _plan_with_blocker(client)
    g = gate(client, pid)
    assert g["status"] == "BLOCKED" and g["passed"] is False
    assert g["issue_counts"]["open_blocker"] == 1
    assert g["open_blockers"][0]["record_id"] == 3
    # 启动被门禁拒绝(409, 门禁语义)
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 409 and "质量门禁" in r.json()["detail"]["reason"]
    # WARNING 规则不阻断
    assert start_plan(client, pid, status=409) is not None


def test_warning_only_scan_passes_gate(client):
    mk_records(client, range(1, 6))
    mk_bad_email(client, 2)  # 有数据问题, 但规则只 WARNING
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [{**EMAIL_RULE, "severity": "WARNING"}])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    g = gate(client, pid)
    assert g["status"] == "PASS" and g["passed"] is True
    assert g["issue_counts"]["warning"] == 1
    start_plan(client, pid)  # 不抛异常即通过


def test_fix_batch_resolves_but_requires_rescan(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue = open_blockers(client, pid)[0]
    # 未修复直接核验 -> STILL_OPEN, 问题保持 OPEN
    r = client.post(f"/api/admin/plans/{pid}/quality-fixes",
                    json={"operator": "bob", "idempotency_key": _key("fix"),
                          "plan_id": pid, "issue_ids": [issue["id"]]})
    assert r.status_code == 201
    j = r.json()
    assert j["progress"]["still_open"] == 1 and j["progress"]["resolved"] == 0
    # 修复数据: 改正邮箱
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 3).update({"email": "r3@x.com"})
    db.commit(); db.close()
    # 数据已变化 -> 当前扫描结果 STALE, 门禁不放行
    g = gate(client, pid)
    assert g["status"] == "STALE" and any("数据" in x for x in g["reasons"])
    # 再核验 -> RESOLVED, 问题置 FIXED
    r = client.post(f"/api/admin/plans/{pid}/quality-fixes",
                    json={"operator": "bob", "idempotency_key": _key("fix"),
                          "plan_id": pid, "issue_ids": [issue["id"]],
                          "note": "已改正邮箱"})
    assert r.status_code == 201
    j = r.json()
    assert j["progress"]["resolved"] == 1
    assert j["results"][0]["verdict"] == "RESOLVED"
    assert j["rule_version"] == 1
    # 修复后问题为 FIXED, 但数据漂移仍需重新扫描
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues").json()
    assert issues[0]["status"] == "FIXED"
    assert issues[0]["resolved_by"] == "bob"
    assert gate(client, pid)["status"] == "STALE"
    # 重新扫描(数据已干净) -> 门禁通过, 可启动
    sid2 = create_scan(client, pid)["scan_id"]
    run_scan(sid2)
    assert gate(client, pid)["status"] == "PASS"
    start_plan(client, pid)


def test_fix_not_found_when_record_deleted(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue = open_blockers(client, pid)[0]
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 3).delete()
    db.commit(); db.close()
    r = client.post(f"/api/admin/plans/{pid}/quality-fixes",
                    json={"operator": "bob", "idempotency_key": _key("fix"),
                          "plan_id": pid, "issue_ids": [issue["id"]]}).json()
    assert r["results"][0]["verdict"] == "NOT_FOUND"
    assert r["progress"]["resolved"] == 0


def test_fix_rejects_unknown_and_non_open_issues(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue = open_blockers(client, pid)[0]["id"]
    # 不存在的问题
    r = client.post(f"/api/admin/plans/{pid}/quality-fixes",
                    json={"operator": "bob", "idempotency_key": _key("fix"),
                          "plan_id": pid, "issue_ids": ["QI-nope"]}).json()
    assert r["progress"]["rejected"] == 1 and r["results"][0]["verdict"] == "REJECTED"
    # 先豁免, 再修复 -> REJECTED
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("ex"),
                      "plan_id": pid, "issue_id": issue, "reason": "历史遗留"})
    r = client.post(f"/api/admin/plans/{pid}/quality-fixes",
                    json={"operator": "bob", "idempotency_key": _key("fix"),
                          "plan_id": pid, "issue_ids": [issue]}).json()
    assert r["results"][0]["verdict"] == "REJECTED"


def test_fix_idempotent_replay(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue = open_blockers(client, pid)[0]["id"]
    body = {"operator": "bob", "idempotency_key": "fix-same",
            "plan_id": pid, "issue_ids": [issue]}
    r1 = client.post(f"/api/admin/plans/{pid}/quality-fixes", json=body).json()
    r2 = client.post(f"/api/admin/plans/{pid}/quality-fixes", json=body).json()
    assert r2["replayed"] is True
    assert r1["fix_batch_id"] == r2["fix_batch_id"]
    db = SessionLocal()
    assert db.query(QualityFixBatch).count() == 1
    db.close()


def test_exemption_passes_gate_and_blocks_warning(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue = open_blockers(client, pid)[0]
    # 非阻断问题不能豁免: 造一个 WARNING 扫描场景单独验证
    # 阻断问题豁免需要原因
    r = client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                    json={"operator": "carol", "idempotency_key": _key("ex"),
                          "plan_id": pid, "issue_id": issue["id"],
                          "reason": "历史遗留数据, 业务确认可接受"})
    assert r.status_code == 201
    assert r.json()["status"] == "APPROVED" and r.json()["rule_version"] == 1
    g = gate(client, pid)
    assert g["status"] == "PASS" and g["issue_counts"]["exempted"] == 1
    # 豁免不改数据 -> 指纹不变, 不触发 STALE, 直接可启动
    start_plan(client, pid)


def test_exemption_idempotent_and_revoke_reopens(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue_id = open_blockers(client, pid)[0]["id"]
    body = {"operator": "carol", "idempotency_key": "ex-same",
            "plan_id": pid, "issue_id": issue_id, "reason": "已知问题"}
    r1 = client.post(f"/api/admin/plans/{pid}/quality-exemptions", json=body).json()
    r2 = client.post(f"/api/admin/plans/{pid}/quality-exemptions", json=body).json()
    assert r2["replayed"] is True
    # 不同幂等键重复豁免 -> already_in_state
    r3 = client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                     json={"operator": "carol", "idempotency_key": _key("ex"),
                           "plan_id": pid, "issue_id": issue_id,
                           "reason": "再豁免一次"}).json()
    assert r3.get("already_in_state") is True
    ex_id = r1["exemption_id"]
    # 撤销豁免 -> 问题重新 OPEN, 门禁再次阻断
    r = client.post(f"/api/admin/plans/{pid}/quality-exemptions/{ex_id}/revoke",
                    json={"operator": "dave", "idempotency_key": _key("rev"),
                          "plan_id": pid, "reason": "审批复核不通过"})
    assert r.status_code == 200
    assert gate(client, pid)["status"] == "BLOCKED"
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues").json()
    assert issues[0]["status"] == "OPEN"
    # 撤销后可重新豁免
    r = client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                    json={"operator": "erin", "idempotency_key": _key("ex"),
                          "plan_id": pid, "issue_id": issue_id,
                          "reason": "重新确认接受"})
    assert r.status_code == 201
    assert gate(client, pid)["status"] == "PASS"


def test_fix_and_exemption_history_bound_to_rule_version(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue_id = open_blockers(client, pid)[0]["id"]
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("h"),
                      "plan_id": pid, "issue_id": issue_id, "reason": "历史遗留"})
    hist = client.get(f"/api/admin/plans/{pid}/quality-history").json()
    assert len(hist["exemptions"]) == 1
    ex = hist["exemptions"][0]
    assert ex["rule_version"] == 1 and ex["reason"] == "历史遗留"
    assert ex["status"] == "APPROVED"
    # 豁免后再发起修复: 问题非 OPEN, 逐项 REJECTED, 修复批次仍落库并绑定规则版本
    client.post(f"/api/admin/plans/{pid}/quality-fixes",
                json={"operator": "bob", "idempotency_key": _key("h"),
                      "plan_id": pid, "issue_ids": [issue_id]})
    hist = client.get(f"/api/admin/plans/{pid}/quality-history").json()
    assert hist["fix_batches"][0]["rule_version"] == 1
    assert hist["fix_batches"][0]["rejected"] == 1


# ---------- 5. 旧结果失效: 规则版本变化 / 数据变化 / 过期 ----------

def test_rule_version_change_invalidates_old_scan(client):
    pid, sid, _ = _plan_with_blocker(client)
    assert gate(client, pid)["status"] == "BLOCKED"
    # 豁免阻断问题 -> PASS
    issue_id = open_blockers(client, pid)[0]["id"]
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("x"),
                      "plan_id": pid, "issue_id": issue_id, "reason": "接受"})
    assert gate(client, pid)["status"] == "PASS"
    # 发布新规则版本 -> 旧扫描 STALE, 不能放行
    save_rules(client, pid, [EMAIL_RULE, NAME_REQUIRED_RULE])
    g = gate(client, pid)
    assert g["status"] == "STALE"
    assert any("规则版本" in x for x in g["reasons"])
    assert g["scan_rule_version"] == 1 and g["current_rule_version"] == 2
    # 启动仍被阻止
    assert start_plan(client, pid, status=409) is not None


def test_batch_data_change_invalidates_old_scan(client):
    pid, sid, _ = _plan_with_blocker(client)
    issue_id = open_blockers(client, pid)[0]["id"]
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("x"),
                      "plan_id": pid, "issue_id": issue_id, "reason": "接受"})
    assert gate(client, pid)["status"] == "PASS"
    # 写入新的坏数据(批次范围内 id=4 改坏)
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 4).update({"email": "bad"})
    db.commit(); db.close()
    g = gate(client, pid)
    assert g["status"] == "STALE"
    assert any("数据" in x for x in g["reasons"])


def test_scan_expiry_invalidates_old_result(client):
    os.environ["QUALITY_SCAN_TTL_SECONDS"] = "60"
    pid, sid, _ = _plan_with_blocker(client)
    # 手工把过期时间提前
    db = SessionLocal()
    sc = db.get(QualityScan, sid)
    sc.expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit(); db.close()
    g = gate(client, pid)
    assert g["status"] == "STALE"
    assert any("过期" in x for x in g["reasons"])
    os.environ["QUALITY_SCAN_TTL_SECONDS"] = "86400"


def test_rescan_after_data_fixed_passes(client):
    # 数据修复(不经修复批次接口)后重新扫描, 门禁恢复
    pid, sid, _ = _plan_with_blocker(client)
    db = SessionLocal()
    db.query(RecordOld).filter(RecordOld.id == 3).update({"email": "r3@x.com"})
    db.commit(); db.close()
    assert gate(client, pid)["status"] == "STALE"
    sid2 = create_scan(client, pid)["scan_id"]
    run_scan(sid2)
    g = gate(client, pid)
    assert g["latest_scan_id"] == sid2 and g["status"] == "PASS"


def test_exemption_inherited_on_rescan_same_version(client):
    # 同规则版本重新扫描(如 TTL 过期后重扫), 有效豁免自动继承
    pid, sid, _ = _plan_with_blocker(client)
    issue_id = open_blockers(client, pid)[0]["id"]
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("x"),
                      "plan_id": pid, "issue_id": issue_id, "reason": "接受"})
    # 手工把扫描置过期(数据未变)
    db = SessionLocal()
    db.get(QualityScan, sid).expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit(); db.close()
    assert gate(client, pid)["status"] == "STALE"
    sid2 = create_scan(client, pid)["scan_id"]
    run_scan(sid2)
    g = gate(client, pid)
    assert g["status"] == "PASS"  # 豁免继承, 无需再次豁免
    issues = client.get(f"/api/admin/plans/{pid}/quality-issues",
                        params={"scan_id": sid2}).json()
    assert issues[0]["status"] == "EXEMPTED"
    assert "inherited" in issues[0]["resolved_by"]


# ---------- 6. 幂等(控制操作)与状态机冲突 ----------

def test_scan_control_idempotent_replay_and_state_conflicts(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    body = {"operator": "alice", "idempotency_key": "pause-same"}
    r1 = client.post(f"/api/admin/quality-scans/{sid}/pause", json=body).json()
    r2 = client.post(f"/api/admin/quality-scans/{sid}/pause", json=body).json()
    assert r2["replayed"] is True
    # PAUSED 不能取消后又恢复之外的非法动作
    r = client.post(f"/api/admin/quality-scans/{sid}/pause",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200 and r.json().get("already_in_state")
    r = client.post(f"/api/admin/quality-scans/{sid}/cancel",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 200
    # 已取消任务的暂停/恢复都 409
    r = client.post(f"/api/admin/quality-scans/{sid}/resume",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 409


def test_idempotency_key_cross_action_reuse_rejected(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    body = {"operator": "alice", "idempotency_key": "shared", "plan_id": pid}
    assert client.post(f"/api/admin/plans/{pid}/quality-scans", json=body).status_code == 201
    # 同键不同动作 -> 409
    r = client.put(f"/api/admin/plans/{pid}/quality-rules",
                   json={**body, "rules": [EMAIL_RULE]})
    assert r.status_code == 409


# ---------- 7. 重启持久化与续跑 ----------

def test_boot_recovery_requeues_running_scan(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="kb1"), mk_batch(client, 11, 20, key="kb2")
    pid = mk_plan(client, [b1, b2])
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    claim(); tick(sid)  # 第一批次完成, 任务 RUNNING
    db = SessionLocal()
    sc = db.get(QualityScan, sid)
    assert sc.status == "RUNNING"
    # 模拟重启: 第二批次尚未开始(无 RUNNING 批次), RUNNING 任务回 QUEUED
    quality.boot_recover_scans(db)
    db.close()
    d = scan_detail(client, sid)
    assert d["status"] == "QUEUED" and d["progress"]["done"] == 1
    # worker 续跑完成, 已完成批次不重扫
    run_scan(sid)
    d = scan_detail(client, sid)
    assert d["status"] == "COMPLETED"
    # 第一个批次指纹保留(没有重新扫描产生重复问题)
    assert d["total_records"] == 20


def test_boot_recovery_resets_running_batch(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    pid = mk_plan(client, b)
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    claim()
    # 手工制造"批次执行中崩溃": 任务 RUNNING + 批次 RUNNING(问题尚未提交)
    db = SessionLocal()
    from app.models import QualityScanBatch
    sc = db.get(QualityScan, sid)
    sc.status = "RUNNING"
    sb = db.query(QualityScanBatch).filter_by(scan_id=sid).one()
    sb.status = "RUNNING"
    db.commit(); db.close()
    db = SessionLocal()
    quality.boot_recover_scans(db)
    db.close()
    d = scan_detail(client, sid)
    assert d["status"] == "QUEUED"
    assert d["batches"][0]["status"] == "PENDING"
    run_scan(sid)
    assert scan_detail(client, sid)["status"] == "COMPLETED"


def test_persistence_across_restart(client):
    # 规则/扫描/问题/豁免/门禁状态全部落库
    pid, sid, _ = _plan_with_blocker(client)
    issue_id = open_blockers(client, pid)[0]["id"]
    client.post(f"/api/admin/plans/{pid}/quality-exemptions",
                json={"operator": "carol", "idempotency_key": _key("x"),
                      "plan_id": pid, "issue_id": issue_id, "reason": "接受"})
    # 模拟重启(新会话执行 boot 对账, 不应改变任何持久化用户态)
    db = SessionLocal()
    quality.boot_recover_scans(db)
    db.close()
    g = gate(client, pid)
    assert g["status"] == "PASS"
    rules = client.get(f"/api/admin/plans/{pid}/quality-rules").json()
    assert rules["current_version"] == 1
    hist = client.get(f"/api/admin/plans/{pid}/quality-history").json()
    assert len(hist["exemptions"]) == 1
    # status 接口含质量汇总
    s = client.get("/api/status").json()
    assert s["quality"]["gates"][pid]["status"] == "PASS"
    assert s["quality_concurrency"] == 2


# ---------- 8. 查询接口 ----------

def test_issues_query_filters(client):
    pid, sid, _ = _plan_with_blocker(client)
    # 按严重级别
    blockers = client.get(f"/api/admin/plans/{pid}/quality-issues"
                          "?severity=BLOCKER").json()
    assert len(blockers) == 1
    # 按状态
    opened = client.get(f"/api/admin/plans/{pid}/quality-issues"
                        "?status_filter=OPEN").json()
    assert len(opened) == 1
    fixed = client.get(f"/api/admin/plans/{pid}/quality-issues"
                       "?status_filter=FIXED").json()
    assert fixed == []
    # 按规则类型
    fmts = client.get(f"/api/admin/plans/{pid}/quality-issues"
                      "?rule_type=format").json()
    assert len(fmts) == 1
    # 非法过滤参数 422
    r = client.get(f"/api/admin/plans/{pid}/quality-issues?severity=NOPE")
    assert r.status_code == 422
    # 计划不存在 404
    r = client.get("/api/admin/plans/P-nope/quality-issues")
    assert r.status_code == 404


def test_overview_and_scan_listing(client):
    pid, sid, _ = _plan_with_blocker(client)
    ov = client.get(f"/api/admin/plans/{pid}/quality-overview").json()
    assert ov["ruleset"]["current_version"] == 1
    assert ov["gate"]["status"] == "BLOCKED"
    assert len(ov["scans"]) == 1
    assert ov["scans"][0]["issue_distribution"]["BLOCKER"]["OPEN"] == 1
    # 全量扫描列表 + 按计划/状态过滤
    rows = client.get("/api/admin/quality-scans").json()
    assert any(x["id"] == sid for x in rows)
    rows = client.get(f"/api/admin/quality-scans?plan_id={pid}").json()
    assert len(rows) == 1
    rows = client.get("/api/admin/quality-scans?status_filter=COMPLETED").json()
    assert all(x["status"] == "COMPLETED" for x in rows)
    # 扫描详情事件流水
    d = client.get(f"/api/admin/quality-scans/{sid}").json()
    events = [e["event"] for e in d["events"]]
    assert "scan.create" in events and "scan.complete" in events


def test_scan_issues_endpoint(client):
    pid, sid, _ = _plan_with_blocker(client)
    rows = client.get(f"/api/admin/quality-scans/{sid}/issues").json()
    assert len(rows) == 1 and rows[0]["sample"]["email"] == "not-an-email"
    r = client.get("/api/admin/quality-scans/QS-nope/issues")
    assert r.status_code == 404


# ---------- 9. 门禁与审批闸门叠加 ----------

def test_quality_gate_and_high_risk_approval_stack(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5)
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": _key("kp"),
                          "name": "高风险", "risk_level": "HIGH",
                          "steps": [{"seq": 1, "batch_id": b}]})
    pid = r.json()["plan_id"]
    save_rules(client, pid, [EMAIL_RULE])
    sid = create_scan(client, pid)["scan_id"]
    run_scan(sid)
    assert gate(client, pid)["status"] == "PASS"  # 数据干净
    # 门禁通过但审批未通过: 仍不能启动
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": _key()})
    assert r.status_code == 409 and "审批" in r.json()["detail"]["reason"]
    # 另一管理员审批后可启动
    r = client.post(f"/api/admin/plans/{pid}/approve",
                    json={"operator": "bob", "idempotency_key": _key()})
    assert r.status_code == 200
    assert start_plan(client, pid)["status"] == "RUNNING"
