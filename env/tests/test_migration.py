import os
import tempfile

# 必须在导入 app 前指向独立测试库
_tmp = tempfile.mkdtemp(prefix="migration-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"

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


def mk_records(client, n=3):
    for i in range(1, n + 1):
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": "a,b"})
        assert r.status_code == 201, r.text


def act(client, action, key, operator="alice", **extra):
    body = {"operator": operator, "idempotency_key": key, **extra}
    return client.post(f"/api/admin/{action}", json=body)


def test_normal_writable(client):
    r = client.post("/api/records", json={"id": 1, "name": "a", "tags_csv": "x"})
    assert r.status_code == 201
    j = client.get("/api/records/1").json()
    assert j["source"] == "old" and j["record"]["tags_csv"] == "x"


def test_freeze_rejects_writes_with_reason(client):
    act(client, "freeze", "k1")
    r = client.post("/api/records", json={"id": 1, "name": "a"})
    assert r.status_code == 423
    d = r.json()["detail"]
    assert d["error"] == "writes_rejected"
    assert "freeze_version" in d["reason"] and d["phase"] == "FROZEN"


def test_dual_read_and_diff_during_freeze(client):
    mk_records(client)
    act(client, "freeze", "k1")
    j = client.get("/api/records/1").json()
    assert j["source"] == "dual"
    assert j["old"]["tags_csv"] == "a,b"
    assert j["new"] is None                      # 尚未回填
    assert j["consistent"] is False
    assert j["diff"][0]["field"] == "__missing__"


def test_validate_cutover_happy_path(client):
    mk_records(client)
    act(client, "freeze", "k1")
    v = act(client, "validate", "k2").json()
    assert v["ok"] and v["diffs"] == []
    # 校验后双读一致
    j = client.get("/api/records/2").json()
    assert j["consistent"] and j["new"]["tags"] == ["a", "b"]
    # 冻结窗内写入仍被拒绝
    assert client.post("/api/records", json={"id": 9, "name": "x"}).status_code == 423
    c = act(client, "cutover", "k3").json()
    assert c["ok"]
    st = client.get("/api/status").json()
    assert st["phase"] == "DONE" and st["active_schema"] == "new"
    # 切换后: 旧路径明确失败, 新路径可写, 读取走新结构
    r = client.post("/api/records", json={"id": 9, "name": "x"})
    assert r.status_code == 410 and r.json()["detail"]["error"] == "old_path_retired"
    assert client.post("/api/v2/records",
                       json={"id": 9, "name": "x", "tags": ["z"]}).status_code == 201
    j = client.get("/api/records/9").json()
    assert j["source"] == "new" and j["record"]["schema_version"] == 2


def test_diff_blocks_cutover(client):
    mk_records(client)
    act(client, "freeze", "k1")
    assert act(client, "validate", "k2").json()["ok"]
    # 校验通过后新表数据被篡改 -> 切换前复核列出差异并阻止
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    db.query(RecordNew).filter_by(id=1).update({"name": "被篡改"})
    db.commit(); db.close()
    c = act(client, "cutover", "k3").json()
    assert c["ok"] is False and len(c["diffs"]) == 1
    assert c["diffs"][0]["record_id"] == 1 and c["diffs"][0]["field"] == "name"
    # 仍停在 VALIDATED, 未切开
    assert client.get("/api/status").json()["phase"] == "VALIDATED"
    # 修正数据后重新校验、切换可通过
    db = SessionLocal()
    db.query(RecordNew).filter_by(id=1).update({"name": "r1"})
    db.commit(); db.close()
    assert act(client, "validate", "k4").json()["ok"]
    assert act(client, "cutover", "k5").json()["ok"]


def test_extra_record_in_new_blocks_cutover(client):
    mk_records(client)
    act(client, "freeze", "k1")
    assert act(client, "validate", "k2").json()["ok"]
    # 校验通过后新表多出一条旧表没有的记录 -> 切换前复核必须报出并阻止
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    db.add(RecordNew(id=99, name="多出来的", email="x@x.com", tags=["a"], schema_version=2))
    db.commit(); db.close()
    c = act(client, "cutover", "k3").json()
    assert c["ok"] is False and len(c["diffs"]) == 1
    d = c["diffs"][0]
    assert d["record_id"] == 99 and d["field"] == "__extra__"
    assert d["old"] is None and d["new"]["name"] == "多出来的"
    assert client.get("/api/status").json()["phase"] == "VALIDATED"  # 未切开
    # 重新校验同样失败: 差异落审计, 回到冻结态
    v = act(client, "validate", "k4").json()
    assert v["ok"] is False and v["diffs"][0]["field"] == "__extra__"
    assert client.get("/api/status").json()["phase"] == "FROZEN"
    audits = client.get("/api/admin/audit").json()
    failed = [a for a in audits if a["action"] == "validate" and a["diffs"]]
    assert failed and failed[0]["diffs"][0]["record_id"] == 99
    # 清掉多余记录后校验、切换恢复可用
    db = SessionLocal()
    db.query(RecordNew).filter_by(id=99).delete()
    db.commit(); db.close()
    assert act(client, "validate", "k5").json()["ok"]
    assert act(client, "cutover", "k6").json()["ok"]


def test_recover_restores_writable_and_cleans_partial(client):
    mk_records(client)
    act(client, "freeze", "k1")
    act(client, "validate", "k2")  # 已回填 3 行到新表
    r = act(client, "recover", "k3", reason="演练回滚")
    assert r.status_code == 200 and r.json()["ok"]
    st = client.get("/api/status").json()
    assert st["phase"] == "NORMAL" and st["watermark"] == 0
    # 无半迁移数据
    from app.db import SessionLocal
    from app.models import RecordNew
    db = SessionLocal()
    assert db.query(RecordNew).count() == 0
    db.close()
    # 恢复可写
    assert client.post("/api/records", json={"id": 9, "name": "x"}).status_code == 201
    # 审计里能查到恢复原因
    audits = client.get("/api/admin/audit").json()
    rec = [a for a in audits if a["action"] == "recover"]
    assert rec and "演练回滚" in rec[0]["reason"] and rec[0]["operator"] == "alice"
    assert rec[0]["app_version"] == "1.0.0-test"


def test_recover_requires_reason_and_not_from_done(client):
    mk_records(client)
    act(client, "freeze", "k1")
    r = client.post("/api/admin/recover",
                    json={"operator": "a", "idempotency_key": "k9", "reason": ""})
    assert r.status_code == 422  # reason 必填
    act(client, "validate", "k2")
    act(client, "cutover", "k3")
    r = act(client, "recover", "k4", reason="试图回退")
    assert r.status_code == 409  # DONE 是终态


def test_idempotent_replay(client):
    mk_records(client)
    r1 = act(client, "freeze", "same-key").json()
    r2 = act(client, "freeze", "same-key").json()
    assert r2["replayed"] is True
    assert r1["freeze_version"] == r2["freeze_version"]
    assert client.get("/api/status").json()["epoch"] == 1  # 只推进一次
    # 同键不同请求 -> 409
    r = client.post("/api/admin/freeze",
                    json={"operator": "bob", "idempotency_key": "same-key"})
    assert r.status_code == 409


def test_epoch_fencing_blocks_stale_admin(client):
    mk_records(client)
    act(client, "freeze", "k1")
    act(client, "validate", "k2")
    epoch = client.get("/api/status").json()["epoch"]
    # 管理员 A 基于当前 epoch 切换成功
    assert act(client, "cutover", "kA", expected_epoch=epoch).json()["ok"]
    # 管理员 B 拿着过期 epoch 的并发请求不可能再"切开"一次
    r = act(client, "cutover", "kB", expected_epoch=epoch)
    assert r.status_code == 409
    audits = client.get("/api/admin/audit").json()
    assert len([a for a in audits if a["action"] == "cutover"
                and a["to_phase"] == "DONE"]) == 1  # 全库只有一次切开


def test_restart_preserves_state_and_resumes(client):
    mk_records(client)
    act(client, "freeze", "k1")
    act(client, "validate", "k2")
    st = client.get("/api/status").json()
    assert st["phase"] == "VALIDATED" and st["watermark"] == 3
    # 模拟重启: 重新进入 startup(同一库文件)
    with TestClient(app) as c2:
        st2 = c2.get("/api/status").json()
        assert st2["phase"] == "VALIDATED" and st2["watermark"] == 3
        assert c2.post("/api/records", json={"id": 9, "name": "x"}).status_code == 423
        assert act(c2, "cutover", "k3").json()["ok"]
        audits = c2.get("/api/admin/audit").json()
        assert any(a["action"] == "boot" for a in audits)  # 重启有审计


def test_audit_trail_complete(client):
    mk_records(client)
    act(client, "freeze", "k1", operator="op1")
    act(client, "validate", "k2", operator="op1")
    act(client, "cutover", "k3", operator="op2")
    audits = client.get("/api/admin/audit").json()
    actions = [a["action"] for a in audits]
    assert actions[0] == "cutover" and actions[-1] == "freeze"  # 倒序, 首尾完整
    assert actions.count("validate") >= 1
    for a in audits:
        assert a["operator"] and a["app_version"] and a["epoch"] is not None
        assert a["watermark"] is not None
