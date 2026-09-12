"""批量复核与分派测试: 按复核状态/业务分组/报告版本筛选待处理任务、批量分派与历史、
分派权限(复核人只能提交自己被分派任务的结论)、批量复核逐项失败(版本变化/已确认/
无权限)且成功项不回滚、重复分派与批量提交幂等、批量结果查询接口、
重启后分派关系/批量结果/待处理队列保留。

后台 worker 由环境变量关闭, 手动用 replay.claim_due_tasks + replay.run_replay_tick
驱动回放执行, 时序确定。"""
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="migration-review-batch-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import ReplayAssignment, ReplayBatchOp, ReplayReview, ReplayTask
from app import plans, replay


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 辅助: 搭一个已完成回放的计划 ----------

def mk_records(client, ids, tags="a,b"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": tags})
        assert r.status_code == 201, r.text


def mk_batch(client, start, end, key, biz=None):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key,
                          "biz": biz or f"biz-{start}", "id_start": start,
                          "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def completed_replay(client, ranges=((1, 5), (6, 10)), key="kp", biz=None):
    """创建线性计划跑完 -> 固化检查点 -> 创建回放并跑完, 返回 (plan_id, replay_id)。"""
    ids = [i for lo, hi in ranges for i in range(lo, hi + 1)]
    mk_records(client, ids)
    bids = [mk_batch(client, lo, hi, f"kb-{key}-{lo}", biz=biz) for lo, hi in ranges]
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": 0}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1], "max_retries": 0})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key,
                          "name": "批量复核测试计划", "max_retries": 0, "steps": steps})
    assert r.status_code == 201, r.text
    pid = r.json()["plan_id"]
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"ks-{key}"})
    assert r.status_code == 200, r.text
    for _ in range(10):
        db = SessionLocal()
        try:
            moved = plans.run_plan_tick(db, pid)
        finally:
            db.close()
        if not moved:
            break
    assert client.get(f"/api/admin/plans/{pid}").json()["status"] == "COMPLETED"
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": f"kc-{key}",
                          "plan_id": pid})
    assert r.status_code == 201, r.text
    cp_id = r.json()["checkpoint_id"]
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": f"kr-{key}",
                          "plan_id": pid, "checkpoint_id": cp_id})
    assert r.status_code == 201, r.text
    rid = r.json()["replay_id"]
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "COMPLETED"
    return pid, rid


def run_replay(rid, n=20):
    db = SessionLocal()
    try:
        replay.claim_due_tasks(db)
    finally:
        db.close()
    for _ in range(n):
        db = SessionLocal()
        try:
            moved = replay.run_replay_tick(db, rid)
        finally:
            db.close()
        if not moved:
            break


def submit(client, rid, seq, verdict, key, version=1, issue=None, tags=None,
           status=201, operator="carol"):
    r = client.post(f"/api/admin/replays/{rid}/reviews",
                    json={"operator": operator, "idempotency_key": key,
                          "step_seq": seq, "report_version": version,
                          "verdict": verdict, "issue": issue,
                          "fix_tags": tags or []})
    assert r.status_code == status, r.text
    return r.json()


def confirm(client, rid, key, version=1, status=200, operator="carol"):
    r = client.post(f"/api/admin/replays/{rid}/review/confirm",
                    json={"operator": operator, "idempotency_key": key,
                          "report_version": version})
    assert r.status_code == status, r.text
    return r.json()


def reopen(client, rid, key, reason=None, status=200):
    r = client.post(f"/api/admin/replays/{rid}/review/reopen",
                    json={"operator": "carol", "idempotency_key": key,
                          "reason": reason})
    assert r.status_code == status, r.text
    return r.json()


def batch_assign(client, ids, assignee, key, operator="admin", reason=None,
                 status=201):
    r = client.post("/api/admin/review-batch/assign",
                    json={"operator": operator, "idempotency_key": key,
                          "assignee": assignee, "replay_ids": ids,
                          "reason": reason})
    assert r.status_code == status, r.text
    return r.json()


def batch_review(client, items, version, key, operator="dave", status=201):
    r = client.post("/api/admin/review-batch/reviews",
                    json={"operator": operator, "idempotency_key": key,
                          "report_version": version, "items": items})
    assert r.status_code == status, r.text
    return r.json()


# ---------- 筛选: 复核状态 / 业务分组 / 报告版本 ----------

def test_filter_review_tasks_by_status_biz_and_version(client):
    _, rid1 = completed_replay(client, ranges=((1, 5),), key="kp1", biz="订单")
    _, rid2 = completed_replay(client, ranges=((6, 10),), key="kp2", biz="库存")
    submit(client, rid2, 1, "FAIL", "kr-f", issue="问题")

    q = client.get("/api/admin/review-tasks?review_status=PENDING").json()
    assert [t["id"] for t in q] == [rid2]
    q = client.get("/api/admin/review-tasks?review_status=UNREVIEWED").json()
    assert [t["id"] for t in q] == [rid1]
    # 业务分组筛选(经步骤批次关联)
    q = client.get("/api/admin/review-tasks?biz=订单").json()
    assert [t["id"] for t in q] == [rid1]
    assert q[0]["bizs"] == ["订单"]
    q = client.get("/api/admin/review-tasks?biz=库存&review_status=PENDING").json()
    assert [t["id"] for t in q] == [rid2]
    # 报告版本筛选: rid2 重新打开后版本为 2
    reopen(client, rid2, "kro1")
    q = client.get("/api/admin/review-tasks?report_version=2").json()
    assert [t["id"] for t in q] == [rid2]
    q = client.get("/api/admin/review-tasks?report_version=1").json()
    assert [t["id"] for t in q] == [rid1]
    # 组合筛选与空结果
    q = client.get("/api/admin/review-tasks?biz=订单&report_version=2").json()
    assert q == []
    # 非法状态 -> 422
    r = client.get("/api/admin/review-tasks?review_status=BOGUS")
    assert r.status_code == 422


# ---------- 批量分派: 成功 / 历史 / 改派 / 幂等 ----------

def test_batch_assign_and_assignment_history(client):
    _, rid1 = completed_replay(client, ranges=((1, 5),), key="kp1")
    _, rid2 = completed_replay(client, ranges=((6, 10),), key="kp2")
    j = batch_assign(client, [rid1, rid2], "dave", "ba1", reason="本轮复核负责人")
    assert j["progress"] == {"total": 2, "succeeded": 2, "failed": 0}
    assert j["batch_op_id"].startswith("BO")
    assert all(r["ok"] and not r["already_assigned"] for r in j["results"])
    # 任务详情显示当前分派人与分派历史
    d = client.get(f"/api/admin/replays/{rid1}").json()
    assert d["assignee"] == "dave"
    assert len(d["assignments"]) == 1
    a = d["assignments"][0]
    assert a["assignee"] == "dave" and a["operator"] == "admin"
    assert a["report_version"] == 1 and a["reason"] == "本轮复核负责人"
    # 分派事件进任务事件流水
    evs = [e["event"] for e in d["events"]]
    assert "review.assign" in evs
    # 改派: 历史只追加, 当前分派人更新
    j = batch_assign(client, [rid1], "erin", "ba2")
    assert j["results"][0]["ok"] and "改派" in j["results"][0]["reason"]
    d = client.get(f"/api/admin/replays/{rid1}").json()
    assert d["assignee"] == "erin"
    assert [a["assignee"] for a in d["assignments"]] == ["erin", "dave"]
    # 重复分派(同人)幂等: 成功但无副作用, 历史不增加
    j = batch_assign(client, [rid1], "erin", "ba3")
    assert j["results"][0]["ok"] and j["results"][0]["already_assigned"] is True
    d = client.get(f"/api/admin/replays/{rid1}").json()
    assert len(d["assignments"]) == 2
    db = SessionLocal()
    try:
        assert db.query(ReplayAssignment).filter_by(task_id=rid1).count() == 2
        assert db.query(ReplayAssignment).filter_by(task_id=rid2).count() == 1
    finally:
        db.close()


def test_batch_assign_item_failures(client):
    _, rid1 = completed_replay(client, ranges=((1, 5),), key="kp1")
    # 已确认任务不能分派
    submit(client, rid1, 1, "PASS", "kr1")
    confirm(client, rid1, "kc1")
    j = batch_assign(client, [rid1, "R-nope"], "dave", "ba1")
    assert j["ok"] is False
    assert j["progress"] == {"total": 2, "succeeded": 0, "failed": 2}
    by_id = {r["replay_id"]: r for r in j["results"]}
    assert "CONFIRMED" in by_id[rid1]["reason"]
    assert "不存在" in by_id["R-nope"]["reason"]
    # 失败不产生分派历史
    db = SessionLocal()
    try:
        assert db.query(ReplayAssignment).count() == 0
    finally:
        db.close()


# ---------- 分派权限: 复核人只能提交自己被分派任务的结论 ----------

def test_assignee_permission_on_single_submit(client):
    _, rid = completed_replay(client)
    batch_assign(client, [rid], "dave", "ba1")
    # 未分派前任何人可提交; 分派后其他人被拒
    j = submit(client, rid, 1, "PASS", "kr-x", operator="carol", status=409)
    assert "无权" in j["detail"]["reason"]
    # 被分派人可以提交
    j = submit(client, rid, 1, "PASS", "kr-ok", operator="dave")
    assert j["review_status"] == "REVIEWING"
    # 未分派的任务不受限
    _, rid2 = completed_replay(client, ranges=((11, 15),), key="kp2")
    submit(client, rid2, 1, "PASS", "kr-free", operator="carol")


# ---------- 批量复核: 逐项失败原因, 成功项不回滚 ----------

def test_batch_review_partial_failures_keep_successes(client):
    _, rid_ok = completed_replay(client, ranges=((1, 5),), key="kp1")
    _, rid_stale = completed_replay(client, ranges=((6, 10),), key="kp2")
    _, rid_confirmed = completed_replay(client, ranges=((11, 15),), key="kp3")
    _, rid_other = completed_replay(client, ranges=((16, 20),), key="kp4")
    # 版本变化: rid_stale 重新打开到 v2, 批量按 v1 提交
    submit(client, rid_stale, 1, "FAIL", "kr-f", issue="问题")
    reopen(client, rid_stale, "kro1")
    # 已确认: rid_confirmed 全部 PASS 后确认
    submit(client, rid_confirmed, 1, "PASS", "kr-c")
    confirm(client, rid_confirmed, "kc-c")
    # 无权限: rid_other 分派给 carol, 批量操作者是 dave
    batch_assign(client, [rid_other], "carol", "ba-x")
    # 正常任务分派给 dave(批量操作者)
    batch_assign(client, [rid_ok], "dave", "ba-ok")

    items = [{"replay_id": rid, "step_seq": 1, "verdict": "PASS"}
             for rid in (rid_ok, rid_stale, rid_confirmed, rid_other)]
    j = batch_review(client, items, version=1, key="br1", operator="dave")
    assert j["ok"] is False
    assert j["progress"] == {"total": 4, "succeeded": 1, "failed": 3}
    by_id = {r["replay_id"]: r for r in j["results"]}
    assert by_id[rid_ok]["ok"] is True
    assert "过期" in by_id[rid_stale]["reason"]          # 版本变化
    assert "CONFIRMED" in by_id[rid_confirmed]["reason"]  # 已确认
    assert "无权" in by_id[rid_other]["reason"]           # 无权限
    # 成功项不被回滚: rid_ok 的结论已落库
    rv = client.get(f"/api/admin/replays/{rid_ok}/reviews").json()
    assert rv["steps"][0]["review"]["verdict"] == "PASS"
    assert rv["steps"][0]["review"]["operator"] == "dave"
    db = SessionLocal()
    try:
        assert db.query(ReplayReview).filter_by(task_id=rid_ok).count() == 1
        assert db.query(ReplayReview).filter_by(task_id=rid_stale).count() == 1  # 仅 v1 FAIL
    finally:
        db.close()
    # 逐项失败原因持久化, 可通过批量结果查询接口获取
    op = client.get(f"/api/admin/review-batch/{j['batch_op_id']}").json()
    assert op["action"] == "review"
    assert op["progress"] == {"total": 4, "succeeded": 1, "failed": 3}
    assert len(op["results"]) == 4


def test_batch_review_same_version_enforced_and_unknown_task(client):
    _, rid = completed_replay(client, ranges=((1, 5), (6, 10)), key="kp1")
    items = [{"replay_id": rid, "step_seq": 1, "verdict": "PASS"},
             {"replay_id": "R-nope", "step_seq": 1, "verdict": "PASS"},
             {"replay_id": rid, "step_seq": 1, "verdict": "MAYBE"}]
    j = batch_review(client, items, version=1, key="br1")
    assert j["progress"] == {"total": 3, "succeeded": 1, "failed": 2}
    assert j["results"][1]["ok"] is False and "不存在" in j["results"][1]["reason"]
    assert j["results"][2]["ok"] is False and "PASS 或 FAIL" in j["results"][2]["reason"]
    # 批量版本与任务当前版本不一致 -> 该项失败(同一报告版本约束)
    submit(client, rid, 2, "FAIL", "kr-f", issue="x")
    reopen(client, rid, "kro1")
    j = batch_review(client, [{"replay_id": rid, "step_seq": 2, "verdict": "PASS"}],
                     version=1, key="br2")
    assert j["results"][0]["ok"] is False and "过期" in j["results"][0]["reason"]
    j = batch_review(client, [{"replay_id": rid, "step_seq": 2, "verdict": "PASS"}],
                     version=2, key="br3")
    assert j["results"][0]["ok"] is True


# ---------- 幂等: 重复分派与批量提交 ----------

def test_batch_assign_idempotent_same_key(client):
    _, rid = completed_replay(client, ranges=((1, 5),), key="kp1")
    j1 = batch_assign(client, [rid], "dave", "ba1")
    assert j1["replayed"] is False
    j2 = batch_assign(client, [rid], "dave", "ba1")
    assert j2["replayed"] is True
    assert j2["batch_op_id"] == j1["batch_op_id"]
    db = SessionLocal()
    try:
        assert db.query(ReplayAssignment).count() == 1  # 重放不产生新历史
        assert db.query(ReplayBatchOp).count() == 1
    finally:
        db.close()
    # 同键不同请求体 -> 409
    r = client.post("/api/admin/review-batch/assign",
                    json={"operator": "admin", "idempotency_key": "ba1",
                          "assignee": "erin", "replay_ids": [rid]})
    assert r.status_code == 409


def test_batch_review_idempotent_same_key(client):
    _, rid = completed_replay(client, ranges=((1, 5), (6, 10)), key="kp1")
    items = [{"replay_id": rid, "step_seq": 1, "verdict": "PASS"},
             {"replay_id": rid, "step_seq": 2, "verdict": "PASS"}]
    j1 = batch_review(client, items, version=1, key="br1")
    assert j1["replayed"] is False and j1["progress"]["succeeded"] == 2
    j2 = batch_review(client, items, version=1, key="br1")
    assert j2["replayed"] is True
    assert j2["batch_op_id"] == j1["batch_op_id"]
    assert j2["results"] == j1["results"]
    db = SessionLocal()
    try:
        assert db.query(ReplayReview).count() == 2  # 重放不产生重复结论
        assert db.query(ReplayBatchOp).count() == 1
    finally:
        db.close()
    # 同键不同请求体 -> 409
    r = client.post("/api/admin/review-batch/reviews",
                    json={"operator": "dave", "idempotency_key": "br1",
                          "report_version": 1,
                          "items": [{"replay_id": rid, "step_seq": 1,
                                     "verdict": "FAIL", "issue": "x"}]})
    assert r.status_code == 409


# ---------- 批量结果查询接口 ----------

def test_batch_op_query_endpoints(client):
    _, rid = completed_replay(client, ranges=((1, 5),), key="kp1")
    j = batch_assign(client, [rid], "dave", "ba1")
    op_id = j["batch_op_id"]
    op = client.get(f"/api/admin/review-batch/{op_id}").json()
    assert op["action"] == "assign" and op["assignee"] == "dave"
    assert op["progress"] == {"total": 1, "succeeded": 1, "failed": 0}
    assert op["results"][0]["replay_id"] == rid
    # 列表接口(不含逐项结果)
    rows = client.get("/api/admin/review-batch").json()
    assert [o["id"] for o in rows] == [op_id]
    assert "results" not in rows[0]
    assert rows[0]["progress"]["succeeded"] == 1
    # 不存在 -> 404
    r = client.get("/api/admin/review-batch/BO-nope")
    assert r.status_code == 404


# ---------- 重启保留: 分派关系 / 批量结果 / 待处理队列 ----------

def test_restart_preserves_assignments_batch_results_and_queue(client):
    _, rid = completed_replay(client, ranges=((1, 5), (6, 10)), key="kp1")
    batch_assign(client, [rid], "dave", "ba1", reason="负责人")
    j = batch_review(client, [{"replay_id": rid, "step_seq": 1, "verdict": "FAIL",
                               "issue": "数据漂移", "fix_tags": ["修复"]}],
                     version=1, key="br1", operator="dave")
    op_id = j["batch_op_id"]
    # 模拟服务重启: 新 TestClient 触发 startup 对账
    with TestClient(app) as c2:
        d = c2.get(f"/api/admin/replays/{rid}").json()
        assert d["assignee"] == "dave"                     # 分派关系保留
        assert len(d["assignments"]) == 1
        assert d["review"]["status"] == "PENDING"
        q = c2.get("/api/admin/review-queue?status=PENDING").json()
        assert [t["id"] for t in q] == [rid]               # 待处理队列保留
        op = c2.get(f"/api/admin/review-batch/{op_id}").json()  # 批量结果保留
        assert op["progress"] == {"total": 1, "succeeded": 1, "failed": 0}
        assert op["results"][0]["verdict"] == "FAIL"
        rows = c2.get("/api/admin/review-batch").json()
        assert {o["id"] for o in rows} >= {op_id}
        db = SessionLocal()
        try:
            task = db.get(ReplayTask, rid)
            assert task.assignee == "dave"
            assert db.query(ReplayAssignment).filter_by(task_id=rid).count() == 1
            assert db.get(ReplayBatchOp, op_id) is not None
        finally:
            db.close()
    # 重启后权限仍然生效: 非被分派人提交被拒
    submit(client, rid, 2, "PASS", "kr-after", operator="carol", status=409)
    submit(client, rid, 2, "PASS", "kr-after2", operator="dave")


# ---------- 页面视图: 详情/队列带分派人 ----------

def test_views_include_assignee(client):
    _, rid = completed_replay(client, ranges=((1, 5),), key="kp1")
    batch_assign(client, [rid], "dave", "ba1")
    s = client.get("/api/status").json()
    r0 = next(t for t in s["replays"] if t["id"] == rid)
    assert r0["assignee"] == "dave"
    assert r0["assignments"][0]["assignee"] == "dave"
    rows = client.get("/api/admin/replays").json()
    assert rows[0]["assignee"] == "dave"
    # 筛选接口同样带分派人
    q = client.get("/api/admin/review-tasks").json()
    assert q[0]["assignee"] == "dave"
