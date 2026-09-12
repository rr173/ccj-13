"""风险审批 + 执行窗口测试。

审批: 高风险计划须由不同于创建者的另一名管理员审批通过才能启动;
拒绝必须带原因并阻止启动; 启动前可撤销审批; 重复审批/拒绝/撤销幂等。
窗口: 计划只在允许窗口内推进, 窗口外暂停、重新进入窗口自动继续;
启动前可修改窗口; 窗口控制幂等; 重启后审批状态与窗口边界不丢失。
后台 worker 由 conftest 关闭, 这里手动用 plans.run_plan_tick 驱动。
"""
import os
import tempfile
from datetime import datetime, timedelta, timezone

_tmp = tempfile.mkdtemp(prefix="migration-approval-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import MigrationPlan, PlanWindow
from app import plans


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 辅助 ----------

def mk_records(client, ids):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": "a,b"})
        assert r.status_code == 201, r.text


def mk_batch(client, start, end, key=None):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key or f"kb-{start}",
                          "biz": f"biz-{start}", "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def create_plan(client, steps, *, key="kp", risk="LOW", windows=None,
                operator="alice", name="测试计划", status=201):
    body = {"operator": operator, "idempotency_key": key, "name": name,
            "risk_level": risk, "max_retries": 0, "steps": steps}
    if windows is not None:
        body["windows"] = windows
    r = client.post("/api/admin/plans", json=body)
    assert r.status_code == status, r.text
    return r.json()


def get_plan(client, pid):
    return client.get(f"/api/admin/plans/{pid}").json()


def plan_post(client, pid, action, key, *, operator="bob", reason=Ellipsis):
    body = {"operator": operator, "idempotency_key": key}
    if reason is not Ellipsis:
        body["reason"] = reason
    return client.post(f"/api/admin/plans/{pid}/{action}", json=body)


def run_ticks(pid, n=20):
    for _ in range(n):
        db = SessionLocal()
        try:
            if not plans.run_plan_tick(db, pid):
                return
        finally:
            db.close()


def set_windows_in_db(pid, ranges):
    """直接改库模拟时间推移到某个窗口集合(测试确定性控制)。ranges: [(start, end)] naive UTC。"""
    db = SessionLocal()
    db.query(PlanWindow).filter_by(plan_id=pid).delete()
    for s, e in ranges:
        db.add(PlanWindow(plan_id=pid, starts_at=s, ends_at=e, created_by="alice"))
    db.commit()
    db.close()


# ---------- 审批 ----------

def test_low_risk_starts_without_approval(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="LOW")["plan_id"]
    p = get_plan(client, pid)
    assert p["risk_level"] == "LOW" and p["approval_status"] == "NOT_REQUIRED"
    r = plan_post(client, pid, "start", "ks", operator="alice")
    assert r.status_code == 200 and r.json()["status"] == "RUNNING"
    run_ticks(pid)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_high_risk_requires_approval_by_another_admin(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="HIGH")["plan_id"]
    p = get_plan(client, pid)
    assert p["approval_status"] == "PENDING" and p["approved_by"] is None
    # 未审批不能启动
    r = plan_post(client, pid, "start", "ks", operator="alice")
    assert r.status_code == 409 and "审批" in r.json()["detail"]["reason"]
    # 创建者不能审批自己的高风险计划
    r = plan_post(client, pid, "approve", "ka-self", operator="alice")
    assert r.status_code == 409 and "另一名管理员" in r.json()["detail"]["reason"]
    # 另一名管理员审批通过
    r = plan_post(client, pid, "approve", "ka", operator="bob")
    assert r.status_code == 200 and r.json()["approval_status"] == "APPROVED"
    p = get_plan(client, pid)
    assert p["approved_by"] == "bob" and p["approved_at"]
    # 审批通过后可启动并跑完
    r = plan_post(client, pid, "start", "ks", operator="alice")
    assert r.status_code == 200
    run_ticks(pid)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_reject_records_reason_and_blocks_start(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="HIGH")["plan_id"]
    # 拒绝原因必填
    r = plan_post(client, pid, "reject", "kr0", operator="bob", reason=None)
    assert r.status_code == 422
    # 拒绝
    r = plan_post(client, pid, "reject", "kr", operator="bob", reason="变更窗口与结算冲突")
    assert r.status_code == 200 and r.json()["approval_status"] == "REJECTED"
    p = get_plan(client, pid)
    assert p["reject_reason"] == "变更窗口与结算冲突"
    # 被拒绝不能启动, 错误带回原因
    r = plan_post(client, pid, "start", "ks", operator="alice")
    assert r.status_code == 409
    assert "拒绝" in r.json()["detail"]["reason"]
    assert "结算冲突" in r.json()["detail"]["reason"]
    # 审批状态仍是 REJECTED, 计划仍 DRAFT
    p = get_plan(client, pid)
    assert p["status"] == "DRAFT" and p["approval_status"] == "REJECTED"
    # 拒绝原因落审计
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    rej = [a for a in audits if a["action"] == "plan.reject"]
    assert len(rej) == 1 and "结算冲突" in rej[0]["reason"] and rej[0]["operator"] == "bob"
    # 重新审批通过后可以启动
    r = plan_post(client, pid, "approve", "ka2", operator="carol")
    assert r.status_code == 200
    assert get_plan(client, pid)["reject_reason"] is None
    r = plan_post(client, pid, "start", "ks2", operator="alice")
    assert r.status_code == 200
    run_ticks(pid)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_revoke_approval_before_start_closes_gate(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="HIGH")["plan_id"]
    plan_post(client, pid, "approve", "ka", operator="bob")
    assert get_plan(client, pid)["approval_status"] == "APPROVED"
    # 启动前撤销审批
    r = plan_post(client, pid, "revoke-approval", "kv", operator="bob")
    assert r.status_code == 200 and r.json()["approval_status"] == "PENDING"
    assert get_plan(client, pid)["approved_by"] is None
    # 闸门重新关闭
    r = plan_post(client, pid, "start", "ks", operator="alice")
    assert r.status_code == 409 and "尚未" in r.json()["detail"]["reason"]
    # 再次审批 -> 可启动
    plan_post(client, pid, "approve", "ka2", operator="carol")
    assert plan_post(client, pid, "start", "ks2", operator="alice").status_code == 200


def test_approval_ops_idempotent(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="HIGH")["plan_id"]
    # 重复审批: 同键重放 + 新键重复都无副作用, 只有一条审批审计
    a1 = plan_post(client, pid, "approve", "ka", operator="bob").json()
    assert a1["approval_status"] == "APPROVED"
    a2 = plan_post(client, pid, "approve", "ka", operator="bob").json()
    a3 = plan_post(client, pid, "approve", "ka-new", operator="carol").json()
    assert a2["replayed"] is True
    assert a3.get("already_in_state") is True
    assert get_plan(client, pid)["approved_by"] == "bob"  # 第二人不覆盖
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    assert [a["action"] for a in audits].count("plan.approve") == 1

    # 撤销幂等: 撤销后再撤销(不同键)无副作用
    plan_post(client, pid, "revoke-approval", "kv1", operator="bob")
    r2 = plan_post(client, pid, "revoke-approval", "kv2", operator="bob")
    assert r2.status_code == 200 and r2.json().get("already_in_state") is True
    assert get_plan(client, pid)["approval_status"] == "PENDING"
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    assert [a["action"] for a in audits].count("plan.revoke_approval") == 1

    # 拒绝幂等
    plan_post(client, pid, "reject", "kr1", operator="bob", reason="原因一")
    r3 = plan_post(client, pid, "reject", "kr2", operator="bob", reason="另一个原因")
    assert r3.status_code == 200 and r3.json().get("already_in_state") is True
    assert get_plan(client, pid)["reject_reason"] == "原因一"
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    assert [a["action"] for a in audits].count("plan.reject") == 1

    # 幂等键跨动作复用 -> 409(沿用框架既有约定)
    r = plan_post(client, pid, "approve", "kr1", operator="carol")
    assert r.status_code == 409


def test_low_risk_plan_approval_ops_rejected(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="LOW")["plan_id"]
    assert plan_post(client, pid, "approve", "ka", operator="bob").status_code == 409
    assert plan_post(client, pid, "reject", "kr", operator="bob",
                     reason="x").status_code == 409
    assert plan_post(client, pid, "revoke-approval", "kv",
                     operator="bob").status_code == 409


def test_approval_ops_locked_after_start(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, "k1"), mk_batch(client, 11, 20, "k2")
    pid = create_plan(client, [{"seq": 1, "batch_id": b1},
                               {"seq": 2, "batch_id": b2, "depends_on": [1]}],
                      risk="HIGH")["plan_id"]
    plan_post(client, pid, "approve", "ka", operator="bob")
    plan_post(client, pid, "start", "ks", operator="alice")
    # 启动后审批/撤销/拒绝都被锁定
    assert plan_post(client, pid, "revoke-approval", "kv",
                     operator="bob").status_code == 409
    assert plan_post(client, pid, "reject", "kr", operator="bob",
                     reason="x").status_code == 409
    assert plan_post(client, pid, "approve", "ka2",
                     operator="carol").status_code == 409


# ---------- 执行窗口 ----------

def test_plan_pauses_outside_window_and_resumes_inside(client):
    mk_records(client, range(1, 31))
    b1, b2, b3 = (mk_batch(client, s, e, f"k{s}")
                  for s, e in ((1, 10), (11, 20), (21, 30)))
    now = datetime.now(timezone.utc)
    # 初始窗口只覆盖"现在": 启动后第一步可跑, 之后窗口结束
    windows = [{"starts_at": iso(now - timedelta(minutes=1)),
                "ends_at": iso(now + timedelta(seconds=30))}]
    pid = create_plan(client, [
        {"seq": 1, "batch_id": b1},
        {"seq": 2, "batch_id": b2, "depends_on": [1]},
        {"seq": 3, "batch_id": b3, "depends_on": [2]},
    ], windows=windows)["plan_id"]
    p = get_plan(client, pid)
    assert p["has_windows"] is True and p["in_window"] is True
    plan_post(client, pid, "start", "ks", operator="alice")
    run_ticks(pid, 1)  # s1 在窗口内完成
    # 把窗口整体移到未来: 模拟窗口已结束
    future = now + timedelta(hours=1)
    set_windows_in_db(pid, [(future.replace(tzinfo=None),
                             (future + timedelta(hours=1)).replace(tzinfo=None))])
    db = SessionLocal()
    db.query(MigrationPlan).filter_by(id=pid).update({"window_open": True})
    db.commit(); db.close()
    # tick 发现窗口外: 不推进, 计划保持 RUNNING 但 window_open=False
    moved = False
    for _ in range(3):
        db = SessionLocal()
        try:
            moved = plans.run_plan_tick(db, pid) or moved
        finally:
            db.close()
    assert moved is False
    p = get_plan(client, pid)
    assert p["status"] == "RUNNING" and p["in_window"] is False
    # 步骤停在 s1 SUCCESS, s2 PENDING, 批次 b2 未被触碰
    sm = {s["seq"]: s for s in p["steps"]}
    assert sm[1]["status"] == "SUCCESS" and sm[2]["status"] == "PENDING"
    assert client.get(f"/api/admin/batches/{b2}").json()["phase"] == "NORMAL"
    # 窗口暂停落审计
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    assert any(a["action"] == "plan.window_pause" for a in audits)
    # 重新进入窗口: 单 tick 完成状态转换(自动继续), 再跑剩余步骤
    near = datetime.now(timezone.utc)
    set_windows_in_db(pid, [((near - timedelta(minutes=1)).replace(tzinfo=None),
                             (near + timedelta(hours=1)).replace(tzinfo=None))])
    run_ticks(pid, 5)
    p = get_plan(client, pid)
    assert p["status"] == "COMPLETED" and p["in_window"] is True
    assert {s["seq"]: s["status"] for s in p["steps"]} == {1: "SUCCESS", 2: "SUCCESS", 3: "SUCCESS"}
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    assert any(a["action"] == "plan.window_resume" for a in audits)


def test_start_outside_window_waits_without_running_steps(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=2)
    windows = [{"starts_at": iso(future), "ends_at": iso(future + timedelta(hours=1))}]
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}],
                      windows=windows)["plan_id"]
    assert get_plan(client, pid)["in_window"] is False
    r = plan_post(client, pid, "start", "ks", operator="alice").json()
    assert "不在执行窗口" in r["detail"]
    # worker tick 不推进
    run_ticks(pid, 3)
    p = get_plan(client, pid)
    assert p["status"] == "RUNNING" and p["in_window"] is False
    assert {s["seq"]: s["status"] for s in p["steps"]} == {1: "PENDING"}
    assert client.get(f"/api/admin/batches/{b1}").json()["phase"] == "NORMAL"
    # 窗口到来后自动跑完
    near = datetime.now(timezone.utc)
    set_windows_in_db(pid, [((near - timedelta(minutes=1)).replace(tzinfo=None),
                             (near + timedelta(hours=1)).replace(tzinfo=None))])
    run_ticks(pid, 3)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_window_update_only_before_start_and_idempotent(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, "k1"), mk_batch(client, 11, 20, "k2")
    now = datetime.now(timezone.utc)
    w1 = [{"starts_at": iso(now - timedelta(hours=1)),
           "ends_at": iso(now + timedelta(hours=1))}]
    pid = create_plan(client, [{"seq": 1, "batch_id": b1},
                               {"seq": 2, "batch_id": b2, "depends_on": [1]}],
                      windows=w1)["plan_id"]
    # 幂等: 同样的窗口再存一次 -> 无副作用, 只有 create 审计(无 window_update)
    r = client.put(f"/api/admin/plans/{pid}/window",
                   json={"operator": "alice", "idempotency_key": "kw1",
                         "windows": w1})
    assert r.status_code == 200 and r.json().get("already_in_state") is True
    # 替换为新窗口
    w2 = [{"starts_at": iso(now + timedelta(hours=5)),
           "ends_at": iso(now + timedelta(hours=6))}]
    r = client.put(f"/api/admin/plans/{pid}/window",
                   json={"operator": "bob", "idempotency_key": "kw2", "windows": w2})
    assert r.status_code == 200
    p = get_plan(client, pid)
    assert len(p["windows"]) == 1 and p["in_window"] is False
    # 清空窗口限制(空列表)
    r = client.put(f"/api/admin/plans/{pid}/window",
                   json={"operator": "bob", "idempotency_key": "kw3", "windows": []})
    assert r.status_code == 200
    assert get_plan(client, pid)["has_windows"] is False
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    upd = [a for a in audits if a["action"] == "plan.window_update"]
    assert len(upd) == 2  # 幂等那次不产生审计
    # 非法窗口聚合拒绝: 开始 >= 结束
    r = client.put(f"/api/admin/plans/{pid}/window",
                   json={"operator": "bob", "idempotency_key": "kw4",
                         "windows": [{"starts_at": iso(now),
                                      "ends_at": iso(now - timedelta(hours=1))}]})
    assert r.status_code == 409 and any("早于" in x for x in r.json()["detail"]["reasons"])
    # 启动后不能再改窗口
    plan_post(client, pid, "start", "ks", operator="alice")
    run_ticks(pid, 5)
    r = client.put(f"/api/admin/plans/{pid}/window",
                   json={"operator": "alice", "idempotency_key": "kw5", "windows": w1})
    assert r.status_code == 409 and "启动前" in r.json()["detail"]["reason"]


def test_create_plan_with_invalid_window_rejected_atomically(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    r = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp", status=409,
                    windows=[{"starts_at": "2026-09-12T10:00:00Z",
                              "ends_at": "not-a-time"}])
    assert any("ISO 8601" in x for x in r["detail"]["reasons"])
    assert client.get("/api/admin/plans").json() == []


# ---------- 高风险 + 窗口组合 ----------

def test_high_risk_and_window_gates_compose(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10)
    future = datetime.now(timezone.utc) + timedelta(hours=3)
    windows = [{"starts_at": iso(future), "ends_at": iso(future + timedelta(hours=1))}]
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], risk="HIGH",
                      windows=windows)["plan_id"]
    # 审批通过但窗口外: 能启动(RUNNING)却不推进
    plan_post(client, pid, "approve", "ka", operator="bob")
    plan_post(client, pid, "start", "ks", operator="alice")
    run_ticks(pid, 3)
    p = get_plan(client, pid)
    assert p["status"] == "RUNNING" and p["in_window"] is False
    assert client.get(f"/api/admin/batches/{b1}").json()["phase"] == "NORMAL"
    # 窗口到达 -> 自动完成
    near = datetime.now(timezone.utc)
    set_windows_in_db(pid, [((near - timedelta(minutes=1)).replace(tzinfo=None),
                             (near + timedelta(hours=1)).replace(tzinfo=None))])
    run_ticks(pid, 3)
    assert get_plan(client, pid)["status"] == "COMPLETED"


# ---------- 重启持久化 ----------

def test_restart_preserves_approval_and_window(client):
    mk_records(client, range(1, 100))
    groups = [(mk_batch(client, s, e, f"k{s}"), s, e)
              for s, e in ((1, 10), (11, 20), (21, 30),
                           (31, 40), (41, 50), (51, 60),
                           (61, 70), (71, 80), (81, 90))]

    def steps3(g):
        return [{"seq": 1, "batch_id": g[0][0]},
                {"seq": 2, "batch_id": g[1][0], "depends_on": [1]},
                {"seq": 3, "batch_id": g[2][0], "depends_on": [2]}]

    g1, g2, g3 = groups[0:3], groups[3:6], groups[6:9]
    b2 = g3[1][0]
    # 高风险计划: 审批通过(不启动), 重启后审批状态不丢
    pid = create_plan(client, steps3(g1), risk="HIGH")["plan_id"]
    plan_post(client, pid, "approve", "ka", operator="bob")
    rejected_pid = create_plan(client, steps3(g2), key="kp2", risk="HIGH",
                               name="被拒计划")["plan_id"]
    plan_post(client, rejected_pid, "reject", "kr", operator="carol",
              reason="资料不全")
    # 带窗口的高风险计划, 启动第一步后窗口结束
    near = datetime.now(timezone.utc)
    win_pid = create_plan(client, steps3(g3), key="kp3", risk="HIGH", name="窗口计划",
                          windows=[{"starts_at": iso(near - timedelta(hours=1)),
                                    "ends_at": iso(near + timedelta(hours=1))}])["plan_id"]
    plan_post(client, win_pid, "approve", "ka3", operator="bob")
    plan_post(client, win_pid, "start", "ks3", operator="alice")
    run_ticks(win_pid, 1)
    future = near + timedelta(hours=5)
    set_windows_in_db(win_pid, [(future.replace(tzinfo=None),
                                 (future + timedelta(hours=1)).replace(tzinfo=None))])
    db = SessionLocal()
    db.query(MigrationPlan).filter_by(id=win_pid).update({"window_open": True})
    db.commit(); db.close()

    # 重启: startup 执行 boot 对账
    with TestClient(app):
        p1 = get_plan(client, pid)
        assert p1["status"] == "DRAFT" and p1["approval_status"] == "APPROVED"
        assert p1["approved_by"] == "bob" and p1["approved_at"]
        p2 = get_plan(client, rejected_pid)
        assert p2["approval_status"] == "REJECTED" and p2["reject_reason"] == "资料不全"
        # 窗口边界持久化, 重启对账后 window_open=False
        p3 = get_plan(client, win_pid)
        assert p3["status"] == "RUNNING" and p3["in_window"] is False
        assert len(p3["windows"]) == 1
        sm = {s["seq"]: s for s in p3["steps"]}
        assert sm[1]["status"] == "SUCCESS" and sm[2]["status"] == "PENDING"
        # 重启不会偷跑(对账 tick 之外没有 worker)
        assert client.get(f"/api/admin/batches/{b2}").json()["phase"] == "NORMAL"
        audits = client.get(f"/api/admin/audit?plan_id={win_pid}").json()
        assert any(a["action"] == "plan.window_pause" for a in audits)

    # 窗口重新到来后重启 + worker 自动续跑完成
    near2 = datetime.now(timezone.utc)
    set_windows_in_db(win_pid, [((near2 - timedelta(minutes=1)).replace(tzinfo=None),
                                 (near2 + timedelta(hours=1)).replace(tzinfo=None))])
    os.environ["PLAN_WORKER_ENABLED"] = "1"
    try:
        with TestClient(app):
            import time
            for _ in range(40):
                if get_plan(client, win_pid)["status"] in ("COMPLETED", "CANCELED"):
                    break
                time.sleep(0.1)
            assert get_plan(client, win_pid)["status"] == "COMPLETED"
            assert get_plan(client, win_pid)["in_window"] is True
    finally:
        os.environ["PLAN_WORKER_ENABLED"] = "0"
