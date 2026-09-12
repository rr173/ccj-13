"""回放证据归档测试: 归档包内容与摘要、排队/暂停/恢复/取消/失败状态机、
版本冲突/缺失步骤/数据被删/摘要不一致的明确失败与失败记录保留、
重复创建与控制请求幂等、并发闸门、归档冻结原回放复核数据、重启续跑。

后台 worker 由 conftest 关闭, 手动用 archives.claim_due_archives +
archives.run_archive_tick 驱动(每次 tick 每个归档至多一个单元), 时序确定。"""
import io
import json
import os
import tempfile
import zipfile

_tmp = tempfile.mkdtemp(prefix="migration-archive-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["REPLAY_MAX_CONCURRENCY"] = "2"
os.environ["ARCHIVE_MAX_CONCURRENCY"] = "2"
os.environ["ARCHIVE_STORE_DIR"] = f"{_tmp}/store"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    AuditLog, IdempotencyKey, MigrationBatch, ReplayArchive, ReplayReview,
    ReplayTask, ReplayTaskStep,
)
from app import archives, plans, replay


@pytest.fixture(autouse=True)
def concurrency():
    os.environ["ARCHIVE_MAX_CONCURRENCY"] = "2"
    yield
    os.environ["ARCHIVE_MAX_CONCURRENCY"] = "2"


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(archives.store_dir(), exist_ok=True)
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _isolate_idem_keys(client):
    """幂等键表在批次/计划/回放各模块间共享; 用例间清空避免短键碰撞。"""
    db = SessionLocal()
    try:
        db.query(IdempotencyKey).delete()
        db.commit()
    finally:
        db.close()
    yield


# ---------- 辅助: 复用回放测试的搭建方式 ----------

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


def completed_replay(client, ranges=((1, 10), (11, 20)), key="kp",
                     review=False, confirm=False):
    ids = [i for lo, hi in ranges for i in range(lo, hi + 1)]
    mk_records(client, ids)
    bids = [mk_batch(client, lo, hi, f"kb-{key}-{lo}") for lo, hi in ranges]
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": 0}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1], "max_retries": 0})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key,
                          "name": "归档测试计划", "max_retries": 0, "steps": steps})
    pid = r.json()["plan_id"]
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"k-start-{key}"})
    assert r.status_code == 200, r.text
    for _ in range(12):
        db = SessionLocal()
        try:
            if not plans.run_plan_tick(db, pid):
                break
        finally:
            db.close()
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": f"kc-{key}",
                          "plan_id": pid})
    cp = r.json()
    assert cp["checkpoint_status"] == "COMPLETE", r.text
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": f"kr-{key}",
                          "plan_id": pid, "checkpoint_id": cp["checkpoint_id"]})
    rid = r.json()["replay_id"]
    run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "COMPLETED"
    if review:
        n = len(ranges)
        for seq in range(1, n + 1):
            r = client.post(f"/api/admin/replays/{rid}/reviews",
                            json={"operator": "bob", "idempotency_key": f"kv-{key}-{seq}",
                                  "step_seq": seq, "report_version": 1,
                                  "verdict": "PASS", "fix_tags": ["ok"]})
            assert r.status_code in (200, 201), r.text
        if confirm:
            r = client.post(f"/api/admin/replays/{rid}/review/confirm",
                            json={"operator": "bob", "idempotency_key": f"kcf-{key}",
                                  "report_version": 1})
            assert r.status_code == 200, r.text
    return rid, pid, cp["checkpoint_id"]


def run_replay(rid, n=20):
    db = SessionLocal()
    try:
        replay.claim_due_tasks(db)
    finally:
        db.close()
    for _ in range(n):
        db = SessionLocal()
        try:
            if not replay.run_replay_tick(db, rid):
                break
        finally:
            db.close()


def create_archive(client, rid, version=1, key="ka", status=201):
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": key,
                          "replay_id": rid, "report_version": version})
    assert r.status_code == status, r.text
    return r.json()


def archive_action(client, aid, action, key, status=200, operator="carol"):
    r = client.post(f"/api/admin/archives/{aid}/{action}",
                    json={"operator": operator, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r.json()


def claim_archives():
    db = SessionLocal()
    try:
        return archives.claim_due_archives(db)
    finally:
        db.close()


def tick_archive(aid):
    db = SessionLocal()
    try:
        return archives.run_archive_tick(db, aid)
    finally:
        db.close()


def run_archive(aid, n=50):
    """驱动一个归档到终态(每次 tick 前重新 claim, 模拟 pause 后 resume 的排队)。"""
    claim_archives()
    for _ in range(n):
        moved = tick_archive(aid)
        if not moved:
            break
    db = SessionLocal()
    try:
        return db.get(ReplayArchive, aid)
    finally:
        db.close()


def expected_units(total_steps):
    return 1 + total_steps + 4


def get_a(aid):
    db = SessionLocal()
    try:
        a = db.get(ReplayArchive, aid)
        return archives.archive_to_dict(db, a)
    finally:
        db.close()


def read_zip(path):
    with zipfile.ZipFile(path) as zf:
        return {n: zf.read(n) for n in zf.namelist()}


# ---------- 创建: 校验 ----------

def test_archive_requires_completed_replay(client):
    # 回放不存在 -> 404
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka1",
                          "replay_id": "R-nope", "report_version": 1})
    assert r.status_code == 404
    # 未完成回放 -> 409
    rid, pid, cp_id = completed_replay(client)
    db = SessionLocal()
    try:
        t = db.get(ReplayTask, rid)
        t.status = "RUNNING"  # 构造非终态
        db.commit()
    finally:
        db.close()
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka2",
                          "replay_id": rid, "report_version": 1})
    assert r.status_code == 409
    assert "COMPLETED" in r.json()["detail"]["reason"]


def test_archive_version_conflict_at_creation(client):
    rid, _, _ = completed_replay(client)
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka",
                          "replay_id": rid, "report_version": 2})
    assert r.status_code == 409
    assert "归档版本冲突" in r.json()["detail"]["reason"]


def test_archive_create_idempotent_same_key(client):
    rid, _, _ = completed_replay(client)
    a1 = create_archive(client, rid, key="ka")
    a2 = create_archive(client, rid, key="ka")
    assert a1["archive_id"] == a2["archive_id"]
    assert a2["replayed"] is True


# ---------- 完成归档: 包内容 / 摘要 / 确定性 ----------

def test_archive_happy_path_package_and_digest(client):
    rid, pid, cp_id = completed_replay(client, review=True, confirm=True)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    assert a["status"] == "QUEUED"
    done = run_archive(aid)
    assert done.status == "COMPLETED", done.failure_reason
    assert done.completed_units == done.total_units
    assert done.content_digest and len(done.content_digest) == 64
    assert os.path.exists(done.package_path)

    files = read_zip(done.package_path)
    required = {
        "metadata.json", "replay/task.json", "replay/steps.json",
        "review/conclusions.json", "review/assignments.json",
        "audit/summary.json", "manifest.json",
    }
    assert set(files) == required
    task = json.loads(files["replay/task.json"])
    assert task["id"] == rid and task["report_version"] == 1
    assert task["review_status"] == "CONFIRMED"
    steps_doc = json.loads(files["replay/steps.json"])
    assert steps_doc["total"] == 2
    assert [s["seq"] for s in steps_doc["steps"]] == [1, 2]
    for s in steps_doc["steps"]:
        assert s["expected_state"] and s["actual_state"]
        assert "checkpoint_evidence" in s
    reviews_doc = json.loads(files["review/conclusions.json"])
    assert reviews_doc["archived_report_version"] == 1
    assert reviews_doc["current_version_counts"]["passed"] == 2
    summary = json.loads(files["audit/summary.json"])
    assert summary["checkpoint_id"] == cp_id
    assert {s["seq"] for s in summary["steps"]} == {1, 2}
    assert all(s["freeze"] and s["cutover"] for s in summary["steps"])
    manifest = json.loads(files["manifest.json"])
    assert manifest["content_digest"] == done.content_digest
    # 摘要校验接口
    r = client.get(f"/api/admin/archives/{aid}/verify")
    assert r.status_code == 200, r.text
    v = r.json()
    assert v["valid"] is True
    assert v["recomputed_content_digest"] == done.content_digest
    # 下载
    r = client.get(f"/api/admin/archives/{aid}/download")
    assert r.status_code == 200
    assert "application/zip" in r.headers["content-type"]
    assert r.content[:2] == b"PK"
    assert f"{aid}" in r.headers["content-disposition"]


def test_archive_package_is_deterministic(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    done = run_archive(a["archive_id"])
    raw1 = open(done.package_path, "rb").read()
    db = SessionLocal()
    try:
        fresh = db.get(ReplayArchive, a["archive_id"])
        manifest = fresh.manifest
    finally:
        db.close()
    with zipfile.ZipFile(done.package_path) as zf:
        blobs = {n: zf.read(n) for n in zf.namelist() if n != "manifest.json"}
    digest, _ = archives._content_digest(blobs)
    assert digest == manifest["content_digest"]
    assert len(raw1) > 0


# ---------- 幂等: 重复归档与控制请求 ----------

def test_duplicate_archive_same_replay_version_returns_existing(client):
    rid, _, _ = completed_replay(client)
    a1 = create_archive(client, rid, key="ka1")
    # 不同幂等键的重复请求(归档仍 QUEUED): 回显同一归档
    a2 = create_archive(client, rid, key="ka2")
    assert a2["archive_id"] == a1["archive_id"]
    assert a2["already_existing"] is True
    # 完成后再请求: 仍回显已完成(不可变)归档
    done = run_archive(a1["archive_id"])
    assert done.status == "COMPLETED"
    a3 = create_archive(client, rid, key="ka3")
    assert a3["archive_id"] == a1["archive_id"]
    assert a3["status"] == "COMPLETED"


def test_control_requests_idempotent(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    # 暂停排队中的任务
    archive_action(client, aid, "pause", "kp1")
    r = archive_action(client, aid, "pause", "kp2")  # 同态重复, 新键
    assert r["already_in_state"] is True
    assert r["replayed"] is False
    # 同键重放
    r = archive_action(client, aid, "pause", "kp1")
    assert r["replayed"] is True
    # 非法状态动作 -> 409
    r = client.post(f"/api/admin/archives/{aid}/resume",
                    json={"operator": "carol", "idempotency_key": "kx"})
    assert r.status_code == 200  # PAUSED -> resume 正常
    r = client.post(f"/api/admin/archives/{aid}/resume",
                    json={"operator": "carol", "idempotency_key": "kx"})
    assert r.json()["replayed"] is True
    # 恢复后跑完成, 重复取消/暂停 -> 409
    run_archive(aid)
    assert get_a(aid)["status"] == "COMPLETED"
    for action in ("pause", "resume", "cancel"):
        r = client.post(f"/api/admin/archives/{aid}/{action}",
                        json={"operator": "carol", "idempotency_key": f"ke-{action}"})
        assert r.status_code == 409, (action, r.text)


def test_cancel_keeps_record_no_package(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    archive_action(client, aid, "cancel", "kc")
    # 重复取消幂等
    r = archive_action(client, aid, "cancel", "kc2")
    assert r["already_in_state"] is True
    detail = get_a(aid)
    assert detail["status"] == "CANCELED"
    assert detail["package_available"] is False
    # 重启后仍可查询
    assert client.get(f"/api/admin/archives/{aid}").json()["status"] == "CANCELED"
    # 取消后允许对同(回放,版本)重新发起归档
    a2 = create_archive(client, rid, key="ka2")
    assert a2["archive_id"] != aid
    run_archive(a2["archive_id"])
    assert get_a(a2["archive_id"])["status"] == "COMPLETED"


# ---------- 暂停 / 恢复: 单元边界与进度 ----------

def test_pause_resume_at_unit_boundary(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    claim_archives()
    # 跑两步: validate + step:1
    assert tick_archive(aid) is True
    assert tick_archive(aid) is True
    d = get_a(aid)
    assert d["progress"]["done"] == 2
    # 暂停在边界生效: 下一 tick 不再推进
    archive_action(client, aid, "pause", "arc-kp")
    assert tick_archive(aid) is False
    d = get_a(aid)
    assert d["status"] == "PAUSED" and d["progress"]["done"] == 2
    assert d["current_stage"] is None
    # 恢复后从下一单元(step:2)续跑并完成
    archive_action(client, aid, "resume", "arc-kr")
    claim_archives()
    for _ in range(10):
        if not tick_archive(aid):
            break
    assert get_a(aid)["status"] == "COMPLETED"
    # 已采集单元没有丢
    assert get_a(aid)["progress"]["done"] == get_a(aid)["progress"]["total"]


# ---------- 并发闸门 ----------

def test_archive_concurrency_limit(client):
    r1, _, _ = completed_replay(client, ranges=((1, 5), (6, 10)), key="kp1")
    r2, _, _ = completed_replay(client, ranges=((11, 15), (16, 20)), key="kp2")
    r3, _, _ = completed_replay(client, ranges=((21, 25), (26, 30)), key="kp3")
    ids = [create_archive(client, r, key=f"ka{i}")["archive_id"]
           for i, r in enumerate((r1, r2, r3))]
    claimed = claim_archives()
    assert set(claimed) == set(ids[:2])
    d3 = get_a(ids[2])
    assert d3["status"] == "QUEUED"
    # 先跑掉一个归档, 排队中的下一个才能被认领
    run_archive(ids[0])
    claimed2 = claim_archives()
    assert ids[2] in claimed2


# ---------- 失败: 版本冲突(执行中版本变化) / 缺失步骤 / 数据被删 / 摘要不一致 ----------

def test_fail_version_conflict_during_execution(client):
    rid, _, _ = completed_replay(client)
    # 先制造一个 FAIL 复核结论 -> PENDING, 归档执行中 reopen 使版本 +1
    r = client.post(f"/api/admin/replays/{rid}/reviews",
                    json={"operator": "bob", "idempotency_key": "kv1",
                          "step_seq": 1, "report_version": 1,
                          "verdict": "FAIL", "issue": "数据有疑问"})
    assert r.status_code in (200, 201)
    a = create_archive(client, rid, version=1, key="ka")
    aid = a["archive_id"]
    claim_archives()
    tick_archive(aid)  # validate 成功
    # validate 已过, 归档活动中 reopen 会被冻结; 直接在 DB 模拟"版本已变化"
    db = SessionLocal()
    try:
        t = db.get(ReplayTask, rid)
        t.report_version = 2
        db.commit()
    finally:
        db.close()
    # 先暂停保护? 不需要: 活动归档会冻结 HTTP reopen, 此处直接改库模拟绕过场景
    while tick_archive(aid):
        d = get_a(aid)
        if d["status"] in ("FAILED", "COMPLETED"):
            break
    d = get_a(aid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] == "version_conflict"
    assert "归档版本冲突" in d["failure_reason"]
    # 失败记录保留, 可查询, 且可按新版本重新归档
    assert client.get(f"/api/admin/archives/{aid}").json()["status"] == "FAILED"
    a2 = create_archive(client, rid, version=2, key="ka2")
    run_archive(a2["archive_id"])
    assert get_a(a2["archive_id"])["status"] == "COMPLETED"


def test_fail_missing_step_report(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    # 删除一个步骤报告行
    db = SessionLocal()
    try:
        row = (db.query(ReplayTaskStep)
               .filter(ReplayTaskStep.task_id == rid,
                       ReplayTaskStep.seq == 2).first())
        db.delete(row)
        db.commit()
    finally:
        db.close()
    run_archive(aid)
    d = get_a(aid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] == "missing_step"
    assert "seq=[2]" in d["failure_reason"]
    assert d["package_available"] is False


def test_fail_data_deleted_batch(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    db = SessionLocal()
    try:
        # 删除 seq=2 对应批次(及其审计), 模拟证据数据被删除
        ts = (db.query(ReplayTaskStep)
              .filter(ReplayTaskStep.task_id == rid, ReplayTaskStep.seq == 2).one())
        bid = ts.batch_id
        db.query(AuditLog).filter(AuditLog.batch_id == bid).delete()
        db.query(MigrationBatch).filter(MigrationBatch.id == bid).delete()
        db.commit()
    finally:
        db.close()
    run_archive(aid)
    d = get_a(aid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] in ("data_deleted", "audit_incomplete")
    assert d["failure_reason"]
    # 失败归档不出现在完成列表, 但列表接口仍保留
    listed = {x["id"] for x in client.get("/api/admin/archives").json()}
    assert aid in listed


def test_fail_data_deleted_replay(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    # 排队中回放被删 -> 执行时明确失败
    db = SessionLocal()
    try:
        db.query(ReplayTaskStep).filter(ReplayTaskStep.task_id == rid).delete()
        db.query(ReplayReview).filter(ReplayReview.task_id == rid).delete()
        db.query(ReplayTask).filter(ReplayTask.id == rid).delete()
        db.commit()
    finally:
        db.close()
    run_archive(aid)
    d = get_a(aid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] == "data_deleted"


def test_digest_mismatch_on_verify_marks_failed(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    done = run_archive(aid)
    assert done.status == "COMPLETED"
    # 外部篡改包内一个文件的内容
    path = done.package_path
    with zipfile.ZipFile(path) as zf:
        items = {n: zf.read(n) for n in zf.namelist()}
    items["metadata.json"] = items["metadata.json"].replace(b'"1.0.0-test"',
                                                            b'"9.9.9-evil"')
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, blob in items.items():
            zf.writestr(n, blob)
    r = client.get(f"/api/admin/archives/{aid}/verify")
    assert r.status_code == 200
    v = r.json()
    assert v["valid"] is False
    assert any("metadata.json" in x for x in v["issues"])
    d = get_a(aid)
    assert d["status"] == "FAILED"
    assert d["failure_code"] == "digest_mismatch"
    # 失败记录与事件保留
    assert any(e["event"] == "verify.failed" for e in d["events"])
    # 原包仍可下载(保留取证), 但列表不再标记可用摘要? 包路径仍在
    assert client.get(f"/api/admin/archives/{aid}/download").status_code == 200


def test_package_deleted_externally_marks_failed(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    done = run_archive(aid)
    os.remove(done.package_path)
    r = client.get(f"/api/admin/archives/{aid}/verify")
    assert r.json()["valid"] is False
    assert get_a(aid)["status"] == "FAILED"
    assert get_a(aid)["failure_code"] == "package_missing"
    r = client.get(f"/api/admin/archives/{aid}/download")
    assert r.status_code == 404


# ---------- 归档冻结原回放/复核数据 ----------

def test_replay_mutations_blocked_while_archive_active(client):
    rid, _, _ = completed_replay(client, review=False)
    # 先正常提交 seq1 结论, seq2 留到归档期间尝试
    r = client.post(f"/api/admin/replays/{rid}/reviews",
                    json={"operator": "bob", "idempotency_key": "kv1",
                          "step_seq": 1, "report_version": 1, "verdict": "PASS"})
    assert r.status_code in (200, 201), r.text
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    # QUEUED 即冻结
    body = {"operator": "bob", "idempotency_key": "kv2", "step_seq": 2,
            "report_version": 1, "verdict": "PASS"}
    r = client.post(f"/api/admin/replays/{rid}/reviews", json=body)
    assert r.status_code == 409
    assert "归档" in r.json()["detail"]["reason"]
    # 分派也被拒(批量分派逐项失败)
    r = client.post("/api/admin/review-batch/assign",
                    json={"operator": "admin", "idempotency_key": "kba",
                          "assignee": "dave", "replay_ids": [rid]})
    assert r.status_code == 201
    item = r.json()["results"][0]
    assert item["ok"] is False and "归档" in item["reason"]
    # 归档完成后复核可继续(只读归档不改变任何复核数据)
    run_archive(aid)
    r = client.post(f"/api/admin/replays/{rid}/reviews", json=body)
    assert r.status_code in (200, 201), r.text


def test_archive_is_read_only_on_source_data(client):
    rid, pid, cp_id = completed_replay(client, review=True)
    before = client.get(f"/api/admin/replays/{rid}/report").json()
    a = create_archive(client, rid, key="ka")
    run_archive(a["archive_id"])
    after = client.get(f"/api/admin/replays/{rid}/report").json()
    assert before["review"]["report_version"] == after["review"]["report_version"]
    assert before["steps"] == after["steps"]
    assert before["review"]["reviewed"] == after["review"]["reviewed"]


# ---------- 重启续跑 / 持久化 ----------

def test_boot_recovery_resumes_running_archive(client):
    rid, _, _ = completed_replay(client)
    a = create_archive(client, rid, key="ka")
    aid = a["archive_id"]
    claim_archives()
    tick_archive(aid)  # validate
    tick_archive(aid)  # step:1
    # 模拟崩溃: 直接把状态置 RUNNING 重启对账(当前已 RUNNING)
    db = SessionLocal()
    try:
        archives.boot_recover_archives(db)
    finally:
        db.close()
    d = get_a(aid)
    assert d["status"] == "QUEUED"
    assert d["progress"]["done"] == 2  # 已完成单元保留
    # 续跑到完成, 不重做 validate(失败计数无变化)
    claim_archives()
    for _ in range(20):
        if not tick_archive(aid):
            break
    assert get_a(aid)["status"] == "COMPLETED"
    assert get_a(aid)["progress"]["done"] == expected_units(2)


def test_archives_listed_in_status(client):
    rid, _, _ = completed_replay(client)
    create_archive(client, rid, key="ka")
    s = client.get("/api/status").json()
    assert len(s["archives"]) == 1
    assert s["archive_concurrency"] == 2
    assert s["archives"][0]["report_version"] == 1
