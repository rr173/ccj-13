"""迁移回放与报告测试: 检查点固化/不完整拒绝、回放报告与漂移检测、
暂停恢复取消、幂等、并发闸门、重启续跑、致命失败保留已完成报告、全程只读。

后台 worker 由 conftest 关闭, 手动用 replay.claim_due_tasks + replay.run_replay_tick
驱动(每次 tick 每个任务至多一步), 时序确定。"""
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="migration-replay-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["REPLAY_MAX_CONCURRENCY"] = "2"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    AuditLog, MigrationBatch, ReplayCheckpointStep, ReplayTask, ReplayTaskStep,
    RecordNew, RecordOld,
)
from app import plans, replay, service


@pytest.fixture(autouse=True)
def concurrency():
    os.environ["REPLAY_MAX_CONCURRENCY"] = "2"
    yield
    os.environ["REPLAY_MAX_CONCURRENCY"] = "2"


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 辅助: 搭一个已完成的 2 步线性计划 ----------

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


def create_and_complete_plan(client, ranges=((1, 10), (11, 20)), key="kp"):
    """创建 2 步线性计划并跑完, 返回 (plan_id, [batch_id...], checkpoint)。"""
    ids = [i for lo, hi in ranges for i in range(lo, hi + 1)]
    mk_records(client, ids)
    bids = [mk_batch(client, lo, hi, f"kb-{key}-{lo}") for lo, hi in ranges]
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": 0}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1], "max_retries": 0})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key,
                          "name": "回放测试计划", "max_retries": 0, "steps": steps})
    assert r.status_code == 201, r.text
    pid = r.json()["plan_id"]
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"k-start-{key}"})
    assert r.status_code == 200, r.text
    for _ in range(10):
        db = SessionLocal()
        try:
            moved = plans.run_plan_tick(db, pid)
        finally:
            db.close()
        if not moved:
            break
    p = client.get(f"/api/admin/plans/{pid}").json()
    assert p["status"] == "COMPLETED"
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": f"kc-{key}",
                          "plan_id": pid})
    assert r.status_code == 201, r.text
    cp = r.json()
    assert cp["checkpoint_status"] == "COMPLETE"
    return pid, bids, cp


def create_replay(client, pid, cp_id, key="kr", status=201):
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": key,
                          "plan_id": pid, "checkpoint_id": cp_id})
    assert r.status_code == status, r.text
    return r.json()


def replay_action(client, rid, action, key, status=200, operator="bob"):
    r = client.post(f"/api/admin/replays/{rid}/{action}",
                    json={"operator": operator, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r.json()


def claim():
    db = SessionLocal()
    try:
        return replay.claim_due_tasks(db)
    finally:
        db.close()


def tick(rid):
    db = SessionLocal()
    try:
        return replay.run_replay_tick(db, rid)
    finally:
        db.close()


def run_replay(rid, n=20):
    claim()
    for _ in range(n):
        if not tick(rid):
            break


# ---------- 检查点 ----------

def test_checkpoint_requires_plan(client):
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": "kc",
                          "plan_id": "P-nope"})
    assert r.status_code == 404
    assert "不存在" in r.json()["detail"]["reason"]


def test_checkpoint_incomplete_for_running_plan(client):
    # 单批次 + 未完成计划: 检查点持久化但 INCOMPLETE, 原因逐条列出
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5, "kb")
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": "kp",
                          "name": "未完成", "steps": [{"seq": 1, "batch_id": b}]})
    pid = r.json()["plan_id"]
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": "kc", "plan_id": pid})
    assert r.status_code == 201, r.text
    cp = r.json()
    assert cp["checkpoint_status"] == "INCOMPLETE"
    assert any("COMPLETED" in x for x in cp["issues"])
    detail = client.get(f"/api/admin/checkpoints/{cp['checkpoint_id']}").json()
    assert detail["status"] == "INCOMPLETE"
    assert any("freeze" in x or "SUCCESS" in x
               for x in detail["steps"][0]["audit_issues"])


def test_checkpoint_create_idempotent_same_key(client):
    pid, _, _ = create_and_complete_plan(client)
    r1 = client.post("/api/admin/checkpoints",
                     json={"operator": "bob", "idempotency_key": "kc", "plan_id": pid})
    r2 = client.post("/api/admin/checkpoints",
                     json={"operator": "bob", "idempotency_key": "kc", "plan_id": pid})
    assert r1.json()["checkpoint_id"] == r2.json()["checkpoint_id"]
    assert r2.json()["replayed"] is True


# ---------- 创建回放: 明确失败 ----------

def test_replay_create_failures(client):
    pid, _, cp = create_and_complete_plan(client)
    # 计划不存在
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": "k1",
                          "plan_id": "P-nope", "checkpoint_id": cp["checkpoint_id"]})
    assert r.status_code == 404
    # 检查点不存在
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": "k2",
                          "plan_id": pid, "checkpoint_id": "C-nope"})
    assert r.status_code == 404
    assert "检查点" in r.json()["detail"]["reason"]


def test_replay_rejects_checkpoint_of_other_plan(client):
    pid, _, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)), key="kp1")
    pid2, _, _ = create_and_complete_plan(client, ranges=((11, 15), (16, 20)), key="kp2")
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": "kx",
                          "plan_id": pid2, "checkpoint_id": cp["checkpoint_id"]})
    assert r.status_code == 409
    assert "不属于计划" in r.json()["detail"]["reason"]


def test_replay_rejects_incomplete_checkpoint(client):
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 5, "kb")
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": "kp",
                          "name": "未完成", "steps": [{"seq": 1, "batch_id": b}]})
    pid = r.json()["plan_id"]
    cp = client.post("/api/admin/checkpoints",
                     json={"operator": "bob", "idempotency_key": "kc",
                           "plan_id": pid}).json()
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": "kr",
                          "plan_id": pid, "checkpoint_id": cp["checkpoint_id"]})
    assert r.status_code == 409
    assert "审计不完整" in r.json()["detail"]["reason"]


def test_duplicate_replay_creation_is_idempotent(client):
    pid, _, cp = create_and_complete_plan(client)
    r1 = create_replay(client, pid, cp["checkpoint_id"], key="kr")
    assert r1["already_active"] is False
    # 同键 -> 幂等重放
    r2 = client.post("/api/admin/replays",
                     json={"operator": "bob", "idempotency_key": "kr",
                           "plan_id": pid, "checkpoint_id": cp["checkpoint_id"]}).json()
    assert r2["replayed"] is True and r2["replay_id"] == r1["replay_id"]
    # 不同键同一(计划,检查点) -> 回显已有任务
    r3 = create_replay(client, pid, cp["checkpoint_id"], key="kr2")
    assert r3["already_active"] is True and r3["replay_id"] == r1["replay_id"]
    rows = client.get("/api/admin/replays").json()
    assert len(rows) == 1


# ---------- 回放报告: 无漂移 / 字段漂移 / 缺失 / 多余 / 批次状态 ----------

def test_replay_completed_clean_report(client):
    pid, bids, cp = create_and_complete_plan(client)
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"
    assert rep["progress"] == {"done": 2, "total": 2}
    assert rep["current_seq"] is None
    assert rep["diff_count"] == 0 and rep["state_diff_count"] == 0
    assert rep["failure_reason"] is None and rep["last_error"] is None
    for st in rep["steps"]:
        assert st["status"] == "SUCCESS"
        assert st["expected_state"]["record_count"] == 10
        assert st["actual_state"]["record_count"] == 10
        assert st["diffs"] == [] and st["state_diffs"] == []
        assert st["expected_state"]["batch"]["phase"] == "DONE"
        assert st["expected_state"]["batch"]["active_schema"] == "new"


def test_replay_detects_field_drift_and_missing_and_extra(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    # 切换后业务漂移(直接改库模拟事后篡改; 回放只读检测, 不会修复也不会失败)
    db = SessionLocal()
    try:
        db.get(RecordNew, 2).name = "drifted-name"   # 字段不一致
        db.delete(db.get(RecordNew, 3))              # 新表缺行 -> __missing__
        db.add(RecordNew(id=999, name="ghost", email="g@x.com",
                         tags=[], schema_version=2))  # 所有批次范围外, 不计入
        db.commit()
    finally:
        db.close()
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"  # 有差异是发现项, 不是回放失败
    # 第一步: name 漂移 + 缺失一条
    s1 = next(s for s in rep["steps"] if s["seq"] == 1)
    fields = {(d["record_id"], d["field"]): d for d in s1["diffs"]}
    assert fields[(2, "name")]["actual"] == "drifted-name"
    assert fields[(3, "__missing__")]["actual"] is None
    assert s1["diff_count"] == 2
    # 范围外幽灵记录不参与(999 不在任何批次范围)
    assert all(d["record_id"] != 999 for s in rep["steps"] for d in s["diffs"])
    assert rep["diff_count"] == 2


def test_replay_detects_extra_record_and_phase_drift(client):
    # 批次范围 1..10 但旧记录只有 1..5(范围内空洞); 计划跑完后经 v2 路径写 id=6,
    # 该记录在新表存在但不在检查点预期集合里 -> __extra__
    mk_records(client, range(1, 6))
    b = mk_batch(client, 1, 10, "kb-gap")
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": "kp-gap",
                          "name": "空洞计划", "steps": [{"seq": 1, "batch_id": b}]})
    pid = r.json()["plan_id"]
    client.post(f"/api/admin/plans/{pid}/start",
                json={"operator": "alice", "idempotency_key": "ks-gap"})
    for _ in range(5):
        db = SessionLocal()
        try:
            if not plans.run_plan_tick(db, pid):
                break
        finally:
            db.close()
    cp = client.post("/api/admin/checkpoints",
                     json={"operator": "bob", "idempotency_key": "kc-gap",
                           "plan_id": pid}).json()
    rid = create_replay(client, pid, cp["checkpoint_id"], key="kr-gap")["replay_id"]
    # 切换后通过新结构写入路径新增一条范围内记录(检查点预期里没有)
    r = client.post("/api/v2/records",
                    json={"id": 6, "name": "late", "email": "l@x.com", "tags": ["x"]})
    assert r.status_code == 201, r.text
    # 批次阶段被回拨(状态漂移)
    db = SessionLocal()
    try:
        db.query(MigrationBatch).filter(MigrationBatch.id == b).update(
            {"phase": "FROZEN", "active_schema": "old"})
        db.commit()
    finally:
        db.close()
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"
    s1 = rep["steps"][0]
    extras = [d for d in s1["diffs"] if d["field"] == "__extra__"]
    assert [d["record_id"] for d in extras] == [6]
    assert s1["expected_state"]["record_count"] == 5
    state_fields = {d["field"]: d for d in s1["state_diffs"]}
    assert state_fields["phase"] == {"field": "phase", "expected": "DONE",
                                    "actual": "FROZEN"}
    assert state_fields["active_schema"]["actual"] == "old"
    assert rep["state_diff_count"] >= 2


# ---------- 致命失败: 检查点缺失 / 审计不完整 / 快照无法还原 ----------

def _run_steps_manually(rid, stop_after_seq):
    """手动认领并跑到指定步骤完成后停下。"""
    claim()
    for _ in range(stop_after_seq):
        assert tick(rid)


def test_failure_when_audit_deleted_keeps_prior_reports(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    _run_steps_manually(rid, stop_after_seq=1)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["steps"][0]["status"] == "SUCCESS"
    # 删掉第二步批次的 cutover 审计(检查点记录的关键证据)
    db = SessionLocal()
    try:
        cps = db.query(ReplayCheckpointStep).filter_by(checkpoint_id=cp["checkpoint_id"]).all()
        second = next(c for c in cps if c.seq == 2)
        for aid in second.required_audit_ids:
            db.delete(db.get(AuditLog, aid))
        db.commit()
    finally:
        db.close()
    assert tick(rid) is True  # 第二步骤跑 -> 任务 FAILED
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "FAILED"
    assert "audit_incomplete" in rep["failure_reason"]
    # 已完成步骤的报告保留; 失败步 FAILED, 无报告; 后续无步骤
    statuses = {s["seq"]: s["status"] for s in rep["steps"]}
    assert statuses == {1: "SUCCESS", 2: "FAILED"}
    assert rep["steps"][0]["expected_state"] is not None
    assert rep["steps"][1]["expected_state"] is None
    # 终态控制被拒
    r = client.post(f"/api/admin/replays/{rid}/pause",
                    json={"operator": "bob", "idempotency_key": "kp-x"})
    assert r.status_code == 409


def test_failure_when_snapshot_unrecoverable(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    db = SessionLocal()
    try:
        cs = (db.query(ReplayCheckpointStep)
              .filter_by(checkpoint_id=cp["checkpoint_id"], seq=1).one())
        cs.old_records = None  # 模拟快照损坏
        db.commit()
    finally:
        db.close()
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "FAILED"
    assert "snapshot_unrecoverable" in rep["failure_reason"]
    assert rep["steps"][0]["status"] == "FAILED"


def test_failure_when_checkpoint_deleted_mid_run(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    _run_steps_manually(rid, stop_after_seq=1)
    db = SessionLocal()
    try:
        from app.models import ReplayCheckpoint
        db.delete(db.get(ReplayCheckpoint, cp["checkpoint_id"]))
        db.commit()
    finally:
        db.close()
    tick(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "FAILED"
    assert "checkpoint_missing" in rep["failure_reason"]
    assert rep["steps"][0]["status"] == "SUCCESS"  # 首步报告保留


def test_failure_when_batch_gone(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5),))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    db = SessionLocal()
    try:
        db.delete(db.get(MigrationBatch, bids[0]))
        db.commit()
    finally:
        db.close()
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "FAILED"
    assert "batch_missing" in rep["failure_reason"]


# ---------- 暂停 / 恢复 / 取消 ----------

def test_pause_resume_boundary_and_progress(client):
    pid, _, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10), (11, 15)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    claim()
    assert tick(rid)       # seq1 执行
    replay_action(client, rid, "pause", "kpause")
    # 暂停在步骤边界: tick 不再推进(任务 PAUSED)
    assert tick(rid) is False
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "PAUSED"
    assert rep["current_seq"] is None
    assert rep["progress"]["done"] == 1
    # 重复暂停幂等
    again = replay_action(client, rid, "pause", "kpause")
    assert again["replayed"] is True
    replay_action(client, rid, "resume", "kresume")
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "QUEUED"  # 恢复后重新排队
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"
    assert rep["progress"]["done"] == 3
    # 已完成报告都在
    assert all(s["expected_state"] for s in rep["steps"])
    # 对终态恢复被拒
    r = client.post(f"/api/admin/replays/{rid}/resume",
                    json={"operator": "bob", "idempotency_key": "k2"})
    assert r.status_code == 409


def test_cancel_queued_skips_all(client):
    pid, _, cp = create_and_complete_plan(client)
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    replay_action(client, rid, "cancel", "kcancel")
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "CANCELED"
    assert all(s["status"] == "SKIPPED" for s in rep["steps"])
    # worker 不会再认领
    assert claim() == []
    # 重复取消幂等
    again = replay_action(client, rid, "cancel", "kcancel")
    assert again["replayed"] is True


def test_cancel_running_preserves_finished_reports(client):
    pid, _, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    _run_steps_manually(rid, stop_after_seq=1)
    replay_action(client, rid, "cancel", "kcancel")
    tick(rid)  # 第二步骤跑 -> 收尾为 SKIPPED
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "CANCELED"
    statuses = {s["seq"]: s["status"] for s in rep["steps"]}
    assert statuses == {1: "SUCCESS", 2: "SKIPPED"}
    assert rep["steps"][0]["expected_state"] is not None


def test_pause_queued_before_claim(client):
    pid, _, cp = create_and_complete_plan(client)
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    # 未认领时暂停: 仍是 PAUSED, worker 不认领
    replay_action(client, rid, "pause", "kpause")
    assert claim() == []
    replay_action(client, rid, "resume", "kresume")
    assert claim() == [rid]


# ---------- 并发闸门 ----------

def test_concurrency_limit_queues_extra_tasks(client):
    os.environ["REPLAY_MAX_CONCURRENCY"] = "1"
    p1, _, c1 = create_and_complete_plan(client, ranges=((1, 3),), key="kp1")
    p2, _, c2 = create_and_complete_plan(client, ranges=((4, 6),), key="kp2")
    r1 = create_replay(client, p1, c1["checkpoint_id"], key="r1")
    r2 = create_replay(client, p2, c2["checkpoint_id"], key="r2")
    assert claim() == [r1["replay_id"]]
    # 额度已满: 第二个仍排队, 不被重复认领
    assert claim() == []
    both = client.get("/api/admin/replays").json()
    st = {t["id"]: t["status"] for t in both}
    assert st[r1["replay_id"]] == "RUNNING"
    assert st[r2["replay_id"]] == "QUEUED"
    # 第一个跑完后第二个拿到额度
    run_replay(r1["replay_id"])
    assert claim() == [r2["replay_id"]]


# ---------- 重启安全 ----------

def test_boot_recover_returns_running_to_queue(client):
    pid, _, cp = create_and_complete_plan(client, ranges=((1, 5), (6, 10)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    _run_steps_manually(rid, stop_after_seq=1)
    # 模拟崩溃: 直接把任务/步骤改成执行中(RUNNING 边界已提交但报告未生成)
    db = SessionLocal()
    try:
        task = db.get(ReplayTask, rid)
        task.status = "RUNNING"
        task.current_seq = 2
        ts = db.query(ReplayTaskStep).filter_by(task_id=rid, seq=2).one()
        ts.status = "RUNNING"
        db.commit()
    finally:
        db.close()
    db = SessionLocal()
    try:
        replay.boot_recover_replays(db)
    finally:
        db.close()
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "QUEUED"
    assert rep["current_seq"] is None
    statuses = {s["seq"]: s["status"] for s in rep["steps"]}
    assert statuses == {1: "SUCCESS", 2: "PENDING"}  # 已完成报告不丢
    # worker 从安全位置续跑完成
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"
    assert all(s["status"] == "SUCCESS" for s in rep["steps"])


# ---------- 只读保证 / 依赖顺序 / 控制动作幂等 ----------

def test_replay_is_read_only_on_business_data(client):
    pid, bids, cp = create_and_complete_plan(client)
    db = SessionLocal()
    try:
        before = {
            "batches": [(b.id, b.phase, b.epoch) for b in db.query(MigrationBatch)
                        .order_by(MigrationBatch.id).all()],
            "old": db.query(RecordOld).count(),
            "new": db.query(RecordNew).count(),
            "audit": db.query(AuditLog).count(),
        }
    finally:
        db.close()
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    run_replay(rid)
    db = SessionLocal()
    try:
        after = {
            "batches": [(b.id, b.phase, b.epoch) for b in db.query(MigrationBatch)
                        .order_by(MigrationBatch.id).all()],
            "old": db.query(RecordOld).count(),
            "new": db.query(RecordNew).count(),
            "audit": db.query(AuditLog).count(),
        }
    finally:
        db.close()
    assert before == after  # 批次/业务记录/业务审计均未被回放改动


def test_replay_follows_plan_dependency_order(client):
    # 3 步且 seq2 依赖 seq1、seq3 依赖 seq2: 报告完成顺序必须是 1->2->3
    pid, _, cp = create_and_complete_plan(client, ranges=((1, 3), (4, 6), (7, 9)))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    claim()
    order = []
    for _ in range(10):
        moved = tick(rid)
        if not moved:
            break
        # 最近刚跑的步骤: 看事件流水里最新的 step.start(每个 tick 恰好一步)
        db = SessionLocal()
        try:
            task = db.get(ReplayTask, rid)
            started = [e for e in reversed(task.events) if e.event == "step.start"]
            order.append(started[-1].step_seq)
        finally:
            db.close()
    assert order == [1, 2, 3]
    rep = client.get(f"/api/admin/replays/{rid}/report").json()
    assert rep["status"] == "COMPLETED"
    assert [s["seq"] for s in rep["steps"] if s["status"] == "SUCCESS"] == [1, 2, 3]


def test_control_requests_idempotent_key_replay(client):
    pid, _, cp = create_and_complete_plan(client)
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    r1 = client.post(f"/api/admin/replays/{rid}/pause",
                     json={"operator": "bob", "idempotency_key": "kctrl-pause"})
    r2 = client.post(f"/api/admin/replays/{rid}/pause",
                     json={"operator": "bob", "idempotency_key": "kctrl-pause"})
    assert r1.status_code == 200 and r2.json()["replayed"] is True
    # 恢复后再用同一暂停键: 同键重放返回首次(暂停)结果, 不会再次改变状态
    client.post(f"/api/admin/replays/{rid}/resume",
                json={"operator": "bob", "idempotency_key": "kctrl-resume"})
    r3 = client.post(f"/api/admin/replays/{rid}/pause",
                     json={"operator": "bob", "idempotency_key": "kctrl-pause"})
    assert r3.json()["replayed"] is True
    rep = client.get(f"/api/admin/replays/{rid}").json()
    # r3 重放的是首次暂停结果, 实际任务仍处于恢复后的排队状态
    assert rep["status"] == "QUEUED"


def test_failed_task_is_terminal_and_tick_noop(client):
    pid, bids, cp = create_and_complete_plan(client, ranges=((1, 5),))
    rid = create_replay(client, pid, cp["checkpoint_id"])["replay_id"]
    db = SessionLocal()
    try:
        db.delete(db.get(MigrationBatch, bids[0]))
        db.commit()
    finally:
        db.close()
    run_replay(rid)
    # 终态: worker 再认领/再 tick 都是空操作, 不会产生新事件循环
    assert claim() == []
    assert tick(rid) is False
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "FAILED"
    assert rep["finished_at"] is not None


def test_status_includes_replay_summary(client):
    pid, _, cp = create_and_complete_plan(client)
    create_replay(client, pid, cp["checkpoint_id"])
    s = client.get("/api/status").json()
    assert s["replay_concurrency"] == 2
    assert len(s["replays"]) == 1
    assert s["replays"][0]["status"] == "QUEUED"
    assert len(s["checkpoints"]) == 1
