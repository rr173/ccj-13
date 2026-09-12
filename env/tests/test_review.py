"""回放报告复核工作流测试: 逐步复核结论/问题说明/修复标签、结论与报告版本绑定、
全部通过才可确认、发现问题进入待处理并可重新打开(版本+1, 历史保留)、
过期版本冲突不覆盖新结论、同请求幂等、重启后复核状态/历史/待处理队列保留、
按状态查询待处理回放。

后台 worker 由环境变量关闭, 手动用 replay.claim_due_tasks + replay.run_replay_tick
驱动回放执行, 时序确定。"""
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="migration-review-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import ReplayReview, ReplayTask, ReplayTaskStep
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


def mk_batch(client, start, end, key):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key,
                          "biz": f"biz-{start}", "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def completed_replay(client, ranges=((1, 5), (6, 10)), key="kp"):
    """创建线性计划跑完 -> 固化检查点 -> 创建回放并跑完, 返回 (plan_id, replay_id)。"""
    ids = [i for lo, hi in ranges for i in range(lo, hi + 1)]
    mk_records(client, ids)
    bids = [mk_batch(client, lo, hi, f"kb-{key}-{lo}") for lo, hi in ranges]
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": 0}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1], "max_retries": 0})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key,
                          "name": "复核测试计划", "max_retries": 0, "steps": steps})
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


def confirm(client, rid, key, version=1, status=200):
    r = client.post(f"/api/admin/replays/{rid}/review/confirm",
                    json={"operator": "carol", "idempotency_key": key,
                          "report_version": version})
    assert r.status_code == status, r.text
    return r.json()


def reopen(client, rid, key, reason=None, status=200):
    r = client.post(f"/api/admin/replays/{rid}/review/reopen",
                    json={"operator": "carol", "idempotency_key": key,
                          "reason": reason})
    assert r.status_code == status, r.text
    return r.json()


def reviews(client, rid):
    r = client.get(f"/api/admin/replays/{rid}/reviews")
    assert r.status_code == 200, r.text
    return r.json()


# ---------- 基本流程: 逐步复核 -> 全部通过 -> 确认 ----------

def test_review_progress_and_confirm_flow(client):
    _, rid = completed_replay(client)
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "UNREVIEWED"
    assert rv["review"]["report_version"] == 1
    assert rv["review"]["total"] == 2 and rv["review"]["reviewed"] == 0
    assert all(s["review"] is None for s in rv["steps"])

    j = submit(client, rid, 1, "PASS", "kr1")
    assert j["review_status"] == "REVIEWING"
    assert j["review_progress"] == {"reviewed": 1, "total": 2}
    # 未全部通过前不能确认
    j = confirm(client, rid, "kc-early", status=409)
    assert "尚未通过复核" in j["detail"]["reason"]

    j = submit(client, rid, 2, "PASS", "kr2", tags=["抽检通过"])
    assert j["review_progress"] == {"reviewed": 2, "total": 2}
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "REVIEWING"
    assert rv["review"]["passed"] == 2 and rv["review"]["failed"] == 0
    assert rv["steps"][1]["review"]["fix_tags"] == ["抽检通过"]

    j = confirm(client, rid, "kc1")
    assert j["review_status"] == "CONFIRMED"
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "CONFIRMED"
    assert rv["review"]["confirmed_by"] == "carol"
    assert rv["review"]["confirmed_at"] is not None
    # 确认后复核锁定: 再提交/再打开都被拒
    submit(client, rid, 1, "FAIL", "kr-after", issue="x", status=409)
    reopen(client, rid, "kro-after", status=409)
    # 重复确认幂等
    j = confirm(client, rid, "kc2")
    assert j["already_in_state"] is True
    # 事件流水留痕
    evs = [e["event"] for e in client.get(f"/api/admin/replays/{rid}").json()["events"]]
    assert "review.submit" in evs and "review.confirm" in evs


# ---------- 发现问题 -> 待处理 -> 重新打开(版本+1, 历史保留) ----------

def test_fail_review_enters_pending_then_reopen_new_version(client):
    _, rid = completed_replay(client)
    j = submit(client, rid, 1, "FAIL", "kr1",
               issue="记录 3 的 name 与快照不一致", tags=["数据修复", "复跑"])
    assert j["review_status"] == "PENDING"
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "PENDING"
    assert rv["review"]["failed"] == 1
    cur = rv["steps"][0]["review"]
    assert cur["verdict"] == "FAIL"
    assert cur["issue"] == "记录 3 的 name 与快照不一致"
    assert cur["fix_tags"] == ["数据修复", "复跑"]
    # 待处理时不能确认
    j = confirm(client, rid, "kc-blocked", status=409)
    assert "FAIL" in j["detail"]["reason"]
    # 待处理队列能查到
    q = client.get("/api/admin/review-queue?status=PENDING").json()
    assert [t["id"] for t in q] == [rid]
    assert q[0]["review"]["failed"] == 1

    # 重新打开: 版本 +1, 回到未复核, 历史结论保留
    j = reopen(client, rid, "kro1", reason="数据已修复, 重新复核")
    assert j["report_version"] == 2
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "UNREVIEWED"
    assert rv["review"]["report_version"] == 2
    assert rv["review"]["reviewed"] == 0
    assert all(s["review"] is None for s in rv["steps"])
    assert len(rv["history"]) == 1 and rv["history"][0]["report_version"] == 1
    assert rv["history"][0]["verdict"] == "FAIL"
    assert any(e["event"] == "review.reopen" for e in rv["events"])
    # 待处理队列清空(已重新打开)
    assert client.get("/api/admin/review-queue?status=PENDING").json() == []

    # 新版本重新复核并确认
    submit(client, rid, 1, "PASS", "kr2", version=2)
    submit(client, rid, 2, "PASS", "kr3", version=2)
    confirm(client, rid, "kc1", version=2)
    rv = reviews(client, rid)
    assert rv["review"]["status"] == "CONFIRMED"
    # 历史: v1 FAIL + v2 两条 PASS, 全部保留
    assert len(rv["history"]) == 3
    assert {h["report_version"] for h in rv["history"]} == {1, 2}


# ---------- 版本栅栏: 过期版本冲突, 不覆盖新结论 ----------

def test_stale_report_version_conflict_does_not_overwrite(client):
    _, rid = completed_replay(client)
    submit(client, rid, 1, "FAIL", "kr1", issue="数据漂移", tags=["数据修复"])
    reopen(client, rid, "kro1")
    # 基于过期版本 v1 的写入 -> 409, 不产生新结论
    j = submit(client, rid, 1, "PASS", "kr-stale", version=1, status=409)
    assert "过期" in j["detail"]["reason"]
    rv = reviews(client, rid)
    assert rv["review"]["report_version"] == 2
    assert len(rv["history"]) == 1  # 仍只有 v1 的 FAIL
    # 新版本写入成功
    submit(client, rid, 1, "PASS", "kr2", version=2)
    # 再用过期版本写同一步骤 -> 409, v2 的新结论不被覆盖
    submit(client, rid, 1, "FAIL", "kr-stale2", version=1, issue="x", status=409)
    rv = reviews(client, rid)
    assert rv["steps"][0]["review"]["verdict"] == "PASS"
    assert rv["steps"][0]["review"]["report_version"] == 2
    # 确认也校验版本: 过期版本确认 -> 409
    j = confirm(client, rid, "kc-stale", version=1, status=409)
    assert "过期" in j["detail"]["reason"]


def test_duplicate_step_review_same_version_rejected(client):
    _, rid = completed_replay(client)
    submit(client, rid, 1, "PASS", "kr1")
    # 同版本同步骤第二条结论(不同幂等键) -> 409, 不覆盖
    j = submit(client, rid, 1, "FAIL", "kr2", issue="想改判", status=409)
    assert "不能覆盖" in j["detail"]["reason"]
    j = submit(client, rid, 1, "PASS", "kr3", status=409)
    assert "不能覆盖" in j["detail"]["reason"]
    rv = reviews(client, rid)
    assert len(rv["history"]) == 1
    assert rv["steps"][0]["review"]["verdict"] == "PASS"


# ---------- 幂等 ----------

def test_review_submit_idempotent_same_key(client):
    _, rid = completed_replay(client)
    j1 = submit(client, rid, 1, "PASS", "kr1")
    assert j1["replayed"] is False
    # 同键同请求体重放 -> 返回首次结果, 不产生第二条结论
    j2 = submit(client, rid, 1, "PASS", "kr1")
    assert j2["replayed"] is True
    assert j2["step_seq"] == j1["step_seq"]
    rv = reviews(client, rid)
    assert len(rv["history"]) == 1
    # 同键不同请求体 -> 409(幂等键复用)
    submit(client, rid, 2, "PASS", "kr1", status=409)
    # 确认/重新打开同键幂等
    submit(client, rid, 2, "FAIL", "kr-fail", issue="问题", tags=["修复"])
    j = reopen(client, rid, "kro1")
    assert j["replayed"] is False
    j = reopen(client, rid, "kro1")
    assert j["replayed"] is True and j["report_version"] == 2
    db = SessionLocal()
    try:
        assert db.query(ReplayReview).count() == 2  # 重放没有产生新行
    finally:
        db.close()


# ---------- 校验: 状态机与步骤状态 ----------

def test_confirm_requires_all_steps_passed(client):
    _, rid = completed_replay(client)
    submit(client, rid, 1, "PASS", "kr1")
    j = confirm(client, rid, "kc1", status=409)
    assert "[2]" in j["detail"]["reason"]
    submit(client, rid, 2, "FAIL", "kr2", issue="字段漂移")
    j = confirm(client, rid, "kc2", status=409)
    assert "FAIL" in j["detail"]["reason"]


def test_review_only_on_completed_replay(client):
    pid, _ = completed_replay(client)
    # 同一计划再固化检查点并创建回放, 保持 QUEUED: 未完成回放不能复核
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": "kc-q",
                          "plan_id": pid})
    cp_id = r.json()["checkpoint_id"]
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": "kr-q",
                          "plan_id": pid, "checkpoint_id": cp_id})
    rid2 = r.json()["replay_id"]
    j = submit(client, rid2, 1, "PASS", "kr-q1", status=409)
    assert "COMPLETED" in j["detail"]["reason"]
    # 取消后(终态非 COMPLETED)同样拒绝
    client.post(f"/api/admin/replays/{rid2}/cancel",
                json={"operator": "bob", "idempotency_key": "kr-q-cancel"})
    submit(client, rid2, 1, "PASS", "kr-q2", status=409)
    # 不存在的回放 -> 404
    r = client.post("/api/admin/replays/R-nope/reviews",
                    json={"operator": "carol", "idempotency_key": "kr-404",
                          "step_seq": 1, "report_version": 1, "verdict": "PASS"})
    assert r.status_code == 404


def test_review_requires_success_step_and_known_seq(client):
    _, rid = completed_replay(client)
    # 未知步骤 -> 404
    r = client.post(f"/api/admin/replays/{rid}/reviews",
                    json={"operator": "carol", "idempotency_key": "kr-x",
                          "step_seq": 99, "report_version": 1, "verdict": "PASS"})
    assert r.status_code == 404
    # 步骤非 SUCCESS(模拟报告被跳过) -> 409
    db = SessionLocal()
    try:
        ts = db.query(ReplayTaskStep).filter_by(task_id=rid, seq=2).one()
        ts.status = "SKIPPED"
        db.commit()
    finally:
        db.close()
    j = submit(client, rid, 2, "PASS", "kr-skip", status=409)
    assert "SUCCESS" in j["detail"]["reason"]
    # FAIL 必须带问题说明
    j = submit(client, rid, 1, "FAIL", "kr-noissue", status=409)
    assert "问题说明" in j["detail"]["reason"]
    # 非法结论 -> 409
    j = submit(client, rid, 1, "MAYBE", "kr-bad", status=409)
    assert "PASS 或 FAIL" in j["detail"]["reason"]


# ---------- 重启保留: 复核状态 / 历史 / 待处理队列 ----------

def test_restart_preserves_review_state_history_and_queue(client):
    _, rid = completed_replay(client)
    submit(client, rid, 1, "FAIL", "kr1", issue="数据漂移", tags=["数据修复"])
    reopen(client, rid, "kro1", reason="修复后重开")
    submit(client, rid, 1, "PASS", "kr2", version=2)
    # 模拟服务重启: 新 TestClient 触发 startup 对账(boot_check/boot_recover)
    with TestClient(app) as c2:
        rv = c2.get(f"/api/admin/replays/{rid}/reviews").json()
        assert rv["review"]["status"] == "REVIEWING"
        assert rv["review"]["report_version"] == 2
        assert rv["review"]["reviewed"] == 1
        assert len(rv["history"]) == 2  # v1 FAIL + v2 PASS 都在
        assert {h["report_version"] for h in rv["history"]} == {1, 2}
        # 复核数据在库中, 重启对账不影响
        db = SessionLocal()
        try:
            task = db.get(ReplayTask, rid)
            assert task.review_status == "REVIEWING"
            assert task.report_version == 2
            assert db.query(ReplayReview).filter_by(task_id=rid).count() == 2
        finally:
            db.close()
    # 待处理队列同样持久: 再制造一个 PENDING 后重启仍在队列里
    submit(client, rid, 2, "FAIL", "kr3", version=2, issue="又发现问题")
    with TestClient(app) as c3:
        q = c3.get("/api/admin/review-queue?status=PENDING").json()
        assert [t["id"] for t in q] == [rid]
        rv = c3.get(f"/api/admin/replays/{rid}/reviews").json()
        assert rv["review"]["status"] == "PENDING"


# ---------- 待处理队列: 按状态查询 ----------

def test_review_queue_filter_by_status(client):
    _, rid1 = completed_replay(client, ranges=((1, 5),), key="kp1")
    _, rid2 = completed_replay(client, ranges=((6, 10),), key="kp2")
    submit(client, rid2, 1, "FAIL", "kr-f", issue="问题")
    q = client.get("/api/admin/review-queue?status=PENDING").json()
    assert [t["id"] for t in q] == [rid2]
    q = client.get("/api/admin/review-queue?status=UNREVIEWED").json()
    assert [t["id"] for t in q] == [rid1]
    assert client.get("/api/admin/review-queue?status=CONFIRMED").json() == []
    # 默认就是 PENDING
    q = client.get("/api/admin/review-queue").json()
    assert [t["id"] for t in q] == [rid2]
    # 非法状态 -> 422
    r = client.get("/api/admin/review-queue?status=BOGUS")
    assert r.status_code == 422


# ---------- 视图: 状态/详情/报告都带复核进度 ----------

def test_status_and_detail_include_review_summary(client):
    _, rid = completed_replay(client)
    submit(client, rid, 1, "PASS", "kr1")
    s = client.get("/api/status").json()
    r0 = next(t for t in s["replays"] if t["id"] == rid)
    assert r0["review"]["status"] == "REVIEWING"
    assert r0["review"]["report_version"] == 1
    assert r0["review"]["reviewed"] == 1 and r0["review"]["total"] == 2
    d = client.get(f"/api/admin/replays/{rid}").json()
    assert d["review"]["passed"] == 1
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["review"]["status"] == "REVIEWING"
    # 列表接口也带
    rows = client.get("/api/admin/replays").json()
    assert rows[0]["review"]["reviewed"] == 1
