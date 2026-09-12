import os
import tempfile

# 必须在导入 app 前指向独立测试库
_tmp = tempfile.mkdtemp(prefix="migration-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
# 计划后台 worker 由 conftest 统一关闭, 测试中手动驱动 tick
os.environ["PLAN_WORKER_ENABLED"] = "0"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


def mk_records(client, ids):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": "a,b"})
        assert r.status_code == 201, r.text


def mk_batch(client, biz="订单", start=1, end=3, key="kb", operator="alice"):
    r = client.post("/api/admin/batches",
                    json={"operator": operator, "idempotency_key": key,
                          "biz": biz, "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def act(client, bid, action, key, operator="alice", **extra):
    body = {"operator": operator, "idempotency_key": key, **extra}
    return client.post(f"/api/admin/batches/{bid}/{action}", json=body)


def batch_view(client, bid):
    for b in client.get("/api/status").json()["batches"]:
        if b["id"] == bid:
            return b
    raise AssertionError(f"批次 {bid} 不在状态里")


# ---------- 批次创建 ----------

def test_create_batch_and_overlap_rejected(client):
    bid = mk_batch(client, biz="订单", start=1, end=100, key="kb1", operator="op1")
    b = batch_view(client, bid)
    assert b["phase"] == "NORMAL" and b["epoch"] == 0
    assert b["biz"] == "订单" and b["id_start"] == 1 and b["id_end"] == 100
    # 范围重叠(无论是否同一业务组)被拒绝
    r = client.post("/api/admin/batches",
                    json={"operator": "bob", "idempotency_key": "kb2",
                          "biz": "用户", "id_start": 50, "id_end": 200})
    assert r.status_code == 409 and "重叠" in r.json()["detail"]["reason"]
    # 非法范围被拒绝
    r = client.post("/api/admin/batches",
                    json={"operator": "bob", "idempotency_key": "kb3",
                          "biz": "用户", "id_start": 500, "id_end": 200})
    assert r.status_code == 409
    # 不重叠的第二批次可以创建
    bid2 = mk_batch(client, biz="用户", start=101, end=200, key="kb4")
    assert batch_view(client, bid2)["phase"] == "NORMAL"
    # 创建落审计, 带操作者
    audits = client.get("/api/admin/audit").json()
    creates = [a for a in audits if a["action"] == "create"]
    assert len(creates) == 2
    assert creates[0]["operator"] and creates[0]["batch_id"] in (bid, bid2)


def test_batch_not_found(client):
    r = act(client, "B-missing", "freeze", "k1")
    assert r.status_code == 404
    assert client.get("/api/admin/batches/B-missing").status_code == 404


# ---------- 批次外记录不受影响 ----------

def test_records_outside_batch_unaffected_during_freeze(client):
    mk_records(client, range(1, 6))      # 记录 1..5
    bid = mk_batch(client, start=1, end=3)  # 批次只覆盖 1..3
    act(client, bid, "freeze", "k1")
    # 批次内: 写入拒绝, 带批次与冻结窗信息
    r = client.post("/api/records", json={"id": 2, "name": "x"})
    assert r.status_code == 423
    d = r.json()["detail"]
    assert d["error"] == "writes_rejected" and d["batch_id"] == bid
    assert "freeze_version" in d["reason"]
    # 批次外: 正常读写
    assert client.post("/api/records", json={"id": 5, "name": "y"}).status_code == 201
    j = client.get("/api/records/5").json()
    assert j["source"] == "old" and j["batch_id"] is None
    # 批次外不做双读
    assert client.get("/api/records/5/compare").status_code == 409


def test_dual_read_and_diff_during_freeze(client):
    mk_records(client, range(1, 6))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    j = client.get("/api/records/1").json()
    assert j["source"] == "dual" and j["batch_id"] == bid
    assert j["old"]["tags_csv"] == "a,b"
    assert j["new"] is None                      # 尚未回填
    assert j["consistent"] is False
    assert j["diff"][0]["field"] == "__missing__"


# ---------- 单批次完整流程 ----------

def test_validate_cutover_happy_path(client):
    mk_records(client, range(1, 6))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    v = act(client, bid, "validate", "k2").json()
    assert v["ok"] and v["diffs"] == []
    b = batch_view(client, bid)
    assert b["phase"] == "VALIDATED" and b["watermark"] == 3
    assert b["progress"] == {"done": 3, "total": 3}
    # 校验后批次内双读一致
    j = client.get("/api/records/2").json()
    assert j["consistent"] and j["new"]["tags"] == ["a", "b"]
    # 冻结窗内批次内写入仍被拒绝
    assert client.post("/api/records", json={"id": 1, "name": "x"}).status_code == 423
    c = act(client, bid, "cutover", "k3").json()
    assert c["ok"]
    b = batch_view(client, bid)
    assert b["phase"] == "DONE" and b["active_schema"] == "new"
    # 批次内: 旧路径明确失败, 新路径可写, 读取走新结构
    r = client.post("/api/records", json={"id": 2, "name": "x"})
    assert r.status_code == 410 and r.json()["detail"]["error"] == "old_path_retired"
    assert client.post("/api/v2/records",
                       json={"id": 2, "name": "x", "tags": ["z"]}).status_code == 201
    j = client.get("/api/records/2").json()
    assert j["source"] == "new" and j["record"]["schema_version"] == 2
    # 批次外: 旧路径依旧可写, 新路径不开放
    assert client.post("/api/records", json={"id": 9, "name": "x"}).status_code == 201
    assert client.post("/api/v2/records",
                       json={"id": 9, "name": "x", "tags": ["z"]}).status_code == 409


def test_diff_blocks_cutover(client):
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    assert act(client, bid, "validate", "k2").json()["ok"]
    # 校验通过后新表数据被篡改 -> 切换前复核列出差异并阻止
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    db.query(RecordNew).filter_by(id=1).update({"name": "被篡改"})
    db.commit(); db.close()
    c = act(client, bid, "cutover", "k3").json()
    assert c["ok"] is False and len(c["diffs"]) == 1
    assert c["diffs"][0]["record_id"] == 1 and c["diffs"][0]["field"] == "name"
    assert batch_view(client, bid)["phase"] == "VALIDATED"  # 未切开
    # 修正数据后重新校验、切换可通过
    db = SessionLocal()
    db.query(RecordNew).filter_by(id=1).update({"name": "r1"})
    db.commit(); db.close()
    assert act(client, bid, "validate", "k4").json()["ok"]
    assert act(client, bid, "cutover", "k5").json()["ok"]


def test_extra_new_record_does_not_inflate_progress(client):
    """范围内多出一条旧表没有的新表记录时, 进度不得显示 done > total(例如 2/1):
    done 只统计范围内旧记录中已回填的部分; 多余记录由差异机制报出并阻止切换。"""
    mk_records(client, [1])                 # 旧表只有 id=1
    bid = mk_batch(client, start=1, end=2)  # 批次范围 1..2
    act(client, bid, "freeze", "k1")
    assert act(client, bid, "validate", "k2").json()["ok"]
    assert batch_view(client, bid)["progress"] == {"done": 1, "total": 1}
    # 范围内多出一条记录(误写入/残留): 新表 id=2 在旧表没有对应行
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    db.add(RecordNew(id=2, name="多余", email="x@x.com", tags=[], schema_version=2))
    db.commit(); db.close()
    # 修正前: done=2, total=1 -> 页面显示 2/1; 修正后多余记录不计入已完成
    assert batch_view(client, bid)["progress"] == {"done": 1, "total": 1}
    # 多余记录仍作为范围内差异阻止切换, 不会被静默生效
    c = act(client, bid, "cutover", "k3").json()
    assert c["ok"] is False
    assert any(d["field"] == "__extra__" and d["record_id"] == 2 for d in c["diffs"])

    # 回填后旧记录被删除的残留行同样不能抬高进度
    mk_records(client, [10, 11, 12])
    bid2 = mk_batch(client, biz="用户", start=10, end=12, key="kb2")
    act(client, bid2, "freeze", "k4")
    assert act(client, bid2, "validate", "k5").json()["ok"]
    from app.models import RecordOld
    db = SessionLocal()
    db.query(RecordOld).filter_by(id=12).delete()
    db.commit(); db.close()
    assert batch_view(client, bid2)["progress"] == {"done": 2, "total": 2}


def test_extra_record_scoping(client):
    """新表多余记录: 在批次范围内阻止切换; 在批次范围外不影响本批次。"""
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    assert act(client, bid, "validate", "k2").json()["ok"]
    from app.db import SessionLocal
    from app.models import RecordNew
    # 范围外的多余记录(99)不阻止本批次切换
    db = SessionLocal()
    db.add(RecordNew(id=99, name="范围外", email="x@x.com", tags=["a"], schema_version=2))
    db.commit(); db.close()
    assert act(client, bid, "cutover", "k3").json()["ok"]


def test_extra_record_in_range_blocks_cutover(client):
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    assert act(client, bid, "validate", "k2").json()["ok"]
    # 造一条范围内多余记录: 旧表删掉 id=3 但新表回填的 id=3 还在 -> 切换后它会静默生效
    from app.db import SessionLocal
    from app.models import RecordOld
    db = SessionLocal()
    db.query(RecordOld).filter_by(id=3).delete()
    db.commit(); db.close()
    c = act(client, bid, "cutover", "k3").json()
    assert c["ok"] is False
    assert any(d["field"] == "__extra__" and d["record_id"] == 3 for d in c["diffs"])
    assert batch_view(client, bid)["phase"] == "VALIDATED"  # 未切开
    # 重新校验同样失败: 差异落审计, 回到冻结态
    v = act(client, bid, "validate", "k4").json()
    assert v["ok"] is False and any(d["field"] == "__extra__" for d in v["diffs"])
    assert batch_view(client, bid)["phase"] == "FROZEN"
    audits = client.get(f"/api/admin/audit?batch_id={bid}").json()
    failed = [a for a in audits if a["action"] == "validate" and a["diffs"]]
    assert failed and failed[0]["diffs"][0]["record_id"] == 3


# ---------- 单独恢复, 不波及其他批次 ----------

def test_recover_only_cleans_own_batch(client):
    mk_records(client, list(range(1, 4)) + list(range(10, 13)))
    bidA = mk_batch(client, biz="订单", start=1, end=3, key="kbA")
    bidB = mk_batch(client, biz="用户", start=10, end=12, key="kbB")
    for bid in (bidA, bidB):
        act(client, bid, "freeze", f"k1-{bid}")
        assert act(client, bid, "validate", f"k2-{bid}").json()["ok"]
    # 两个批次都已回填(新表共 6 行)
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    assert db.query(RecordNew).count() == 6
    db.close()
    # 恢复批次 A(失败场景): 只清 A 的范围
    r = act(client, bidA, "recover", "k3", reason="订单批次校验复核不通过")
    assert r.status_code == 200 and r.json()["ok"]
    bA, bB = batch_view(client, bidA), batch_view(client, bidB)
    assert bA["phase"] == "NORMAL" and bA["watermark"] is None
    assert bB["phase"] == "VALIDATED"          # 批次 B 不受影响
    db = SessionLocal()
    remaining = {r.id for r in db.query(RecordNew).all()}
    db.close()
    assert remaining == {10, 11, 12}           # B 的回填数据完好, A 的已清理
    # A 恢复可写, B 仍冻结
    assert client.post("/api/records", json={"id": 1, "name": "x"}).status_code == 201
    assert client.post("/api/records", json={"id": 10, "name": "x"}).status_code == 423
    # B 可以继续走完切换
    assert act(client, bidB, "cutover", "k4").json()["ok"]
    # 审计里能查到 A 的恢复原因与操作者
    audits = client.get(f"/api/admin/audit?batch_id={bidA}").json()
    rec = [a for a in audits if a["action"] == "recover"]
    assert rec and "校验复核不通过" in rec[0]["reason"] and rec[0]["operator"] == "alice"
    assert rec[0]["app_version"] == "1.0.0-test"


def test_recover_requires_reason_and_not_from_done(client):
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    r = client.post(f"/api/admin/batches/{bid}/recover",
                    json={"operator": "a", "idempotency_key": "k9", "reason": ""})
    assert r.status_code == 422  # reason 必填
    act(client, bid, "validate", "k2")
    act(client, bid, "cutover", "k3")
    r = act(client, bid, "recover", "k4", reason="试图回退")
    assert r.status_code == 409  # DONE 是终态


# ---------- 幂等与并发栅栏 ----------

def test_idempotent_replay(client):
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    r1 = act(client, bid, "freeze", "same-key").json()
    r2 = act(client, bid, "freeze", "same-key").json()
    assert r2["replayed"] is True
    assert r1["freeze_version"] == r2["freeze_version"]
    assert batch_view(client, bid)["epoch"] == 1  # 只推进一次
    # 同键不同请求体 -> 409
    r = client.post(f"/api/admin/batches/{bid}/freeze",
                    json={"operator": "bob", "idempotency_key": "same-key"})
    assert r.status_code == 409
    # 同键跨批次复用 -> 409
    bid2 = mk_batch(client, biz="用户", start=10, end=12, key="kb2")
    r = act(client, bid2, "freeze", "same-key")
    assert r.status_code == 409
    # 创建动作同样幂等: 重放不产生第二个批次
    c1 = client.post("/api/admin/batches",
                     json={"operator": "a", "idempotency_key": "ck",
                           "biz": "物流", "id_start": 20, "id_end": 30})
    c2 = client.post("/api/admin/batches",
                     json={"operator": "a", "idempotency_key": "ck",
                           "biz": "物流", "id_start": 20, "id_end": 30})
    assert c2.json()["replayed"] is True
    assert c1.json()["batch_id"] == c2.json()["batch_id"]
    assert len(client.get("/api/admin/batches").json()) == 3


def test_epoch_fencing_two_admins_one_batch(client):
    """两个管理员同时推进同一批次: 只有一个成功。"""
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    act(client, bid, "freeze", "k1")
    act(client, bid, "validate", "k2")
    epoch = batch_view(client, bid)["epoch"]
    # 管理员 A 基于当前 epoch 切换成功
    assert act(client, bid, "cutover", "kA", operator="adminA",
               expected_epoch=epoch).json()["ok"]
    # 管理员 B 拿着过期 epoch 的并发请求不可能再"切开"一次
    r = act(client, bid, "cutover", "kB", operator="adminB", expected_epoch=epoch)
    assert r.status_code == 409
    audits = client.get(f"/api/admin/audit?batch_id={bid}").json()
    assert len([a for a in audits if a["action"] == "cutover"
                and a["to_phase"] == "DONE"]) == 1  # 该批次只有一次切开


def test_concurrent_freeze_only_one_wins(client):
    """两个管理员(不同幂等键)同时冻结同一批次: 第二个被阶段/栅栏拒绝。"""
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3)
    r1 = act(client, bid, "freeze", "kA", operator="adminA")
    r2 = act(client, bid, "freeze", "kB", operator="adminB")
    assert r1.status_code == 200 and r1.json()["ok"]
    assert r2.status_code == 409
    assert batch_view(client, bid)["epoch"] == 1  # 只推进一次
    # 另一个批次不受影响, 可以独立冻结
    bid2 = mk_batch(client, biz="用户", start=10, end=12, key="kb2")
    assert act(client, bid2, "freeze", "kC").json()["ok"]


# ---------- 重启保持 ----------

def test_restart_preserves_each_batch(client):
    mk_records(client, list(range(1, 4)) + list(range(10, 13)))
    bidA = mk_batch(client, biz="订单", start=1, end=3, key="kbA")
    bidB = mk_batch(client, biz="用户", start=10, end=12, key="kbB")
    act(client, bidA, "freeze", "k1", operator="op1")
    act(client, bidA, "validate", "k2", operator="op1")
    act(client, bidB, "freeze", "k3", operator="op2")
    assert batch_view(client, bidA)["phase"] == "VALIDATED"
    assert batch_view(client, bidB)["phase"] == "FROZEN"
    # 模拟重启: 重新进入 startup(同一库文件)
    with TestClient(app) as c2:
        bA, bB = batch_view(c2, bidA), batch_view(c2, bidB)
        assert bA["phase"] == "VALIDATED" and bA["watermark"] == 3
        assert bB["phase"] == "FROZEN" and bB["epoch"] == 1
        # 闸门仍然生效: A 范围内拒写, 批次外可写
        assert c2.post("/api/records", json={"id": 1, "name": "x"}).status_code == 423
        assert c2.post("/api/records", json={"id": 99, "name": "x"}).status_code == 201
        # 重启后幂等重放仍返回首次结果, 不二次推进
        r = act(c2, bidA, "validate", "k2", operator="op1").json()
        assert r["replayed"] is True
        assert batch_view(c2, bidA)["epoch"] == bA["epoch"]
        # 各批次可独立续跑
        assert act(c2, bidA, "cutover", "k4", operator="op1").json()["ok"]
        assert act(c2, bidB, "validate", "k5", operator="op2").json()["ok"]
        audits = c2.get("/api/admin/audit").json()
        boot = [a for a in audits if a["action"] == "boot"]
        assert {a["batch_id"] for a in boot} == {bidA, bidB}  # 每个在途批次都有重启审计


# ---------- 审计完整性 ----------

def test_audit_trail_complete(client):
    mk_records(client, range(1, 4))
    bid = mk_batch(client, start=1, end=3, key="kb", operator="op1")
    act(client, bid, "freeze", "k1", operator="op1")
    act(client, bid, "validate", "k2", operator="op1")
    act(client, bid, "cutover", "k3", operator="op2")
    audits = client.get(f"/api/admin/audit?batch_id={bid}").json()
    actions = [a["action"] for a in audits]
    assert actions[0] == "cutover" and actions[-1] == "create"  # 倒序, 首尾完整
    assert actions.count("validate") >= 1 and "freeze" in actions
    for a in audits:
        assert a["batch_id"] == bid
        assert a["operator"] and a["app_version"] and a["epoch"] is not None
    # 操作者审计正确
    by_action = {a["action"]: a["operator"] for a in audits}
    assert by_action["freeze"] == "op1" and by_action["cutover"] == "op2"
