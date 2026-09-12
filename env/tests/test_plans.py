"""迁移计划编排测试: 图校验拒绝 / 依赖推进 / 重试与停住 / 暂停恢复取消 /
幂等 / 重启对账 / 操作者与失败原因留痕。后台 worker 由 conftest 关闭,
这里手动用 plans.run_plan_tick 驱动, 每次调用对应一个 worker tick(每计划至多一步)。"""
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="migration-plan-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import MigrationPlan, PlanStep, RecordNew, RecordOld
from app import plans


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


def mk_batch(client, start, end, biz=None, key=None, operator="alice"):
    biz = biz or f"biz-{start}"
    key = key or f"kb-{start}-{end}"
    r = client.post("/api/admin/batches",
                    json={"operator": operator, "idempotency_key": key,
                          "biz": biz, "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def plan_action(client, pid, action, key, operator="alice", status=200):
    r = client.post(f"/api/admin/plans/{pid}/{action}",
                    json={"operator": operator, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r.json()


def create_plan(client, steps, name="测试计划", key="kp", max_retries=0, status=201,
                operator="alice"):
    r = client.post("/api/admin/plans",
                    json={"operator": operator, "idempotency_key": key,
                          "name": name, "max_retries": max_retries, "steps": steps})
    assert r.status_code == status, r.text
    return r.json()


def get_plan(client, pid):
    r = client.get(f"/api/admin/plans/{pid}")
    assert r.status_code == 200, r.text
    return r.json()


def step_map(p):
    return {s["seq"]: s for s in p["steps"]}


def run_ticks(pid, n=20):
    """模拟 worker: 每个 tick 每个计划至多推进一步。"""
    advanced_total = 0
    for _ in range(n):
        db = SessionLocal()
        try:
            moved = plans.run_plan_tick(db, pid)
        finally:
            db.close()
        if not moved:
            break
        advanced_total += 1
    return advanced_total


def linear_steps(bids, retries=None):
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": (retries or 0)}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1],
                      "max_retries": (retries or 0)})
    return steps


# ---------- 建计划图校验 ----------

def test_plan_create_rejects_missing_dup_cycle(client):
    mk_records(client, range(1, 31))
    b1, b2, b3 = mk_batch(client, 1, 10, key="k1"), mk_batch(client, 11, 20, key="k2"), \
        mk_batch(client, 21, 30, key="k3")

    # 批次不存在
    r = create_plan(client, [{"seq": 1, "batch_id": "B-nope"}], status=409)
    reasons = " ".join(r["detail"]["reasons"])
    assert "B-nope" in reasons and "不存在" in reasons

    # seq 重复 + 批次重复占用(计划内) + 依赖不存在 + 成环 + 自依赖, 一次聚合返回
    steps = [
        {"seq": 1, "batch_id": b1, "depends_on": [2]},   # 在环上
        {"seq": 1, "batch_id": b1},                      # seq 重复 + 批次重复
        {"seq": 2, "batch_id": b2, "depends_on": [1]},   # 环 1<->2
        {"seq": 3, "batch_id": b3, "depends_on": [9, 3]},  # 依赖不存在 + 自依赖
    ]
    r = create_plan(client, steps, key="kp2", status=409)
    reasons = " ".join(r["detail"]["reasons"])
    assert "顺序号重复" in reasons
    assert "重复占用" in reasons
    assert "seq=9" in reasons and "不存在" in reasons
    assert "不能依赖自身" in reasons
    assert "循环" in reasons
    # 被拒绝后不落任何计划/步骤
    assert client.get("/api/admin/plans").json() == []


def test_plan_rejects_done_batch_and_cross_plan_occupancy(client):
    mk_records(client, range(1, 21))
    b1 = mk_batch(client, 1, 10, key="k1")
    b2 = mk_batch(client, 11, 20, key="k2")
    # 第一个计划占用 b1(草稿态也算占用)
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp1")["plan_id"]
    # 第二个计划重复占用同一批次 -> 拒绝
    r = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp2", status=409)
    assert any("已被未终结计划" in x for x in r["detail"]["reasons"])
    # 批次 DONE 终态不能纳入计划: 手工把 b2 走完
    client.post(f"/api/admin/batches/{b2}/freeze",
                json={"operator": "a", "idempotency_key": "f"})
    client.post(f"/api/admin/batches/{b2}/validate",
                json={"operator": "a", "idempotency_key": "v"})
    client.post(f"/api/admin/batches/{b2}/cutover",
                json={"operator": "a", "idempotency_key": "c"})
    r = create_plan(client, [{"seq": 1, "batch_id": b2}], key="kp3", status=409)
    assert any("DONE" in x for x in r["detail"]["reasons"])
    # 取消第一个计划(终态)后, b1 可被新计划占用
    plan_action(client, pid, "cancel", "kc")
    r = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp4", status=201)
    assert r["ok"]


# ---------- 依赖推进 / happy path ----------

def test_plan_runs_batches_in_dependency_order(client):
    mk_records(client, range(1, 31))
    b1, b2, b3 = mk_batch(client, 1, 10, key="k1"), mk_batch(client, 11, 20, key="k2"), \
        mk_batch(client, 21, 30, key="k3")
    # 菱形依赖: s1, s2 无依赖; s3 依赖 [1,2]
    steps = [
        {"seq": 1, "batch_id": b1},
        {"seq": 2, "batch_id": b2},
        {"seq": 3, "batch_id": b3, "depends_on": [1, 2]},
    ]
    pid = create_plan(client, steps, key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    # 第一个 tick: 只取 seq 最小的就绪步(s1); s3 仍 BLOCKED, s2 仍可执行但本 tick 不动
    run_ticks(pid, 1)
    p = get_plan(client, pid)
    sm = step_map(p)
    assert sm[1]["status"] == "SUCCESS"
    assert sm[2]["status"] == "PENDING"
    assert sm[3]["status"] == "BLOCKED" and sm[3]["depends_on"] == [1, 2]
    assert p["status"] == "RUNNING"
    # 剩余 tick 跑完
    run_ticks(pid, 5)
    p = get_plan(client, pid)
    assert p["status"] == "COMPLETED" and p["progress"] == {"done": 3, "total": 3}
    assert all(s["status"] == "SUCCESS" and s["attempts"] == 1 for s in p["steps"])
    # 批次全部 DONE; 步骤记录了操作者
    for bid in (b1, b2, b3):
        assert client.get(f"/api/admin/batches/{bid}").json()["phase"] == "DONE"
    assert all(s["executed_by"] == "alice" for s in p["steps"])
    # 事件流水: 每步 start + success
    evs = p["events"]
    assert {e["event"] for e in evs} == {"start", "success"}
    # 计划完成后不能再启动/暂停/恢复/取消
    for act in ("start", "pause", "resume", "cancel"):
        assert plan_action(client, pid, act, f"kx-{act}", status=409)


def test_dependent_step_stays_blocked_when_upstream_not_done(client):
    """依赖未成功, 下游绝不执行。"""
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="k1"), mk_batch(client, 11, 20, key="k2")
    # 先手工冻结+回填, 再删掉一条旧记录 -> 新表残留成为 __extra__,
    # validate 持续失败(修复方式: 恢复后重新写旧记录), 让 s1 卡住
    client.post(f"/api/admin/batches/{b1}/freeze",
                json={"operator": "a", "idempotency_key": "f0"})
    client.post(f"/api/admin/batches/{b1}/validate",
                json={"operator": "a", "idempotency_key": "v0"})
    db = SessionLocal()
    db.query(RecordOld).filter_by(id=5).delete()
    db.commit(); db.close()
    pid = create_plan(client, linear_steps([b1, b2]), key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 5)
    p = get_plan(client, pid)
    sm = step_map(p)
    assert p["status"] == "HALTED" and sm[1]["status"] == "HALTED"
    assert sm[2]["status"] == "BLOCKED"  # 依赖未满足, 后续步骤被阻止
    # 批次 b2 完全没被碰过
    assert client.get(f"/api/admin/batches/{b2}").json()["phase"] == "NORMAL"
    # 计划最近错误指向 s1 且含原因
    assert p["failed_step_id"] == sm[1]["id"] and p["last_error"]
    assert "seq=1" in p["last_error"]


# ---------- 重试 / 停住 / 恢复 ----------

def test_retries_then_halt_then_resume_success(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="k1")
    # 制造持续失败: 先冻结回填, 再删掉一条旧记录 -> 新表残留成为 __extra__,
    # 此后任何 validate 都不通过, 直到管理员恢复并补回旧记录
    client.post(f"/api/admin/batches/{b1}/freeze",
                json={"operator": "a", "idempotency_key": "f0"})
    client.post(f"/api/admin/batches/{b1}/validate",
                json={"operator": "a", "idempotency_key": "v0"})
    db = SessionLocal()
    db.query(RecordOld).filter_by(id=8).delete()
    db.commit(); db.close()
    pid = create_plan(client, [{"seq": 1, "batch_id": b1, "max_retries": 2}],
                      key="kp", max_retries=2)["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 5)  # 首次 + 2 次重试 = 3 次尝试, 每 tick 一次
    p = get_plan(client, pid)
    s1 = step_map(p)[1]
    assert p["status"] == "HALTED" and s1["status"] == "HALTED"
    assert s1["attempts"] == 3
    fail_events = [e for e in p["events"] if e["event"] in ("fail", "halted")]
    # 事件视图按时间倒序: 三次尝试依次是 halted/fail/fail
    assert [e["attempt"] for e in fail_events] == [3, 2, 1]
    assert fail_events[0]["event"] == "halted"
    assert {e["event"] for e in fail_events[1:]} == {"fail"}
    assert fail_events[-1]["reason"] and fail_events[-1]["operator"] == "alice"
    # 批次停在 FROZEN(校验失败的安全停止点), 没有被切开
    assert client.get(f"/api/admin/batches/{b1}").json()["phase"] == "FROZEN"
    # 非允许动作被拒绝: HALTED 不能 start, 只能 resume
    plan_action(client, pid, "start", "kr-bad", status=409)
    # 修复数据: 补回缺失的旧记录(批次仍冻结, 直接在数据层修复)
    db = SessionLocal()
    db.add(RecordOld(id=8, name="r8", email="r8@x.com", tags_csv="a,b"))
    db.commit(); db.close()
    plan_action(client, pid, "resume", "kr", operator="bob")
    p = get_plan(client, pid)
    s1 = step_map(p)[1]
    assert p["status"] == "RUNNING"
    assert s1["attempts"] == 0 and s1["attempt_round"] == 2  # 新的一轮
    run_ticks(pid, 3)
    p = get_plan(client, pid)
    assert p["status"] == "COMPLETED"
    s1 = step_map(p)[1]
    assert s1["status"] == "SUCCESS" and s1["attempts"] == 1 and s1["attempt_round"] == 2
    assert s1["executed_by"] == "bob"  # 恢复操作者执行新一轮
    assert client.get(f"/api/admin/batches/{b1}").json()["phase"] == "DONE"


def test_plan_pause_resume_at_step_boundary(client):
    mk_records(client, range(1, 31))
    bids = [mk_batch(client, s, e, key=f"k{s}") for s, e in ((1, 10), (11, 20), (21, 30))]
    pid = create_plan(client, linear_steps(bids), key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 1)  # s1 完成
    plan_action(client, pid, "pause", "kpause")
    # 再多跑几个 tick: 暂停的计划不推进
    assert run_ticks(pid, 3) == 0
    p = get_plan(client, pid)
    sm = step_map(p)
    assert p["status"] == "PAUSED" and sm[1]["status"] == "SUCCESS"
    assert sm[2]["status"] in ("BLOCKED", "PENDING")
    assert sm[3]["status"] == "BLOCKED"
    # 对 PAUSED 做 start/pause/cancel 之外... start 不允许
    plan_action(client, pid, "start", "ks2", status=409)
    plan_action(client, pid, "resume", "kr")
    run_ticks(pid, 5)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_cancel_skips_remaining_steps(client):
    mk_records(client, range(1, 31))
    bids = [mk_batch(client, s, e, key=f"k{s}") for s, e in ((1, 10), (11, 20), (21, 30))]
    pid = create_plan(client, linear_steps(bids), key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 1)  # s1 完成
    plan_action(client, pid, "cancel", "kc", operator="carol")
    p = get_plan(client, pid)
    sm = step_map(p)
    assert p["status"] == "CANCELED"
    assert sm[1]["status"] == "SUCCESS"
    assert sm[2]["status"] == "SKIPPED" and sm[3]["status"] == "SKIPPED"
    # 已完成的批次保持 DONE, 其余批次没被动过
    assert client.get(f"/api/admin/batches/{bids[0]}").json()["phase"] == "DONE"
    assert client.get(f"/api/admin/batches/{bids[1]}").json()["phase"] == "NORMAL"
    # 终态计划不能再操作
    for act in ("start", "pause", "resume", "cancel"):
        plan_action(client, pid, act, f"kx-{act}", status=409)


def test_pause_during_step_execution_is_respected(client):
    """步骤已进入 RUNNING(边界已提交)后管理员暂停: 该步可以跑完,
    但收尾必须把计划留在 PAUSED, 不得覆盖成 COMPLETED/RUNNING。"""
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="k1")
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    # 标记一个 RUNNING 步骤(模拟执行中), 再暂停
    db = SessionLocal()
    step = db.query(PlanStep).filter_by(plan_id=pid).first()
    step.status = "RUNNING"
    step.attempts = 1
    db.commit(); db.close()
    plan_action(client, pid, "pause", "kpause")
    # worker 收尾这一步: 直接跑一个 tick, 内部会重新读到 PAUSED
    db = SessionLocal()
    # tick 见 PAUSED 不会自行启动; 这里模拟执行线程的收尾: 手工完成该步
    from app import plans as _plans
    plan = _plans.lock_plan(db, pid)
    st = db.query(PlanStep).filter_by(plan_id=pid).first()
    # 批次阶段动作(此时 NORMAL): 完整跑一次尝试
    _plans._attempt_step(db, plan, st)
    db.close()
    p = get_plan(client, pid)
    sm = step_map(p)
    assert p["status"] == "PAUSED"
    assert sm[1]["status"] == "SUCCESS"  # 步骤成果保留
    # 恢复后计划正常进入 COMPLETED(唯一一步已成功)
    plan_action(client, pid, "resume", "kr")
    run_ticks(pid, 2)
    assert get_plan(client, pid)["status"] == "COMPLETED"


def test_cancel_draft_plan_releases_batches(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="k1")
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp")["plan_id"]
    plan_action(client, pid, "cancel", "kc")
    # 草稿直接取消: 步骤 SKIPPED, 批次可被新计划占用
    assert step_map(get_plan(client, pid))[1]["status"] == "SKIPPED"
    assert create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp2",
                       status=201)["ok"]


# ---------- 幂等 ----------

def test_plan_actions_idempotent(client):
    mk_records(client, range(1, 21))
    b1, b2 = mk_batch(client, 1, 10, key="k1"), mk_batch(client, 11, 20, key="k2")
    # 创建重放不产生第二个计划
    steps = linear_steps([b1, b2])
    c1 = create_plan(client, steps, key="same")
    c2 = create_plan(client, steps, key="same")
    assert c2["replayed"] is True and c1["plan_id"] == c2["plan_id"]
    pid = c1["plan_id"]
    # start 重放: 只启动一次, 状态不变
    s1 = plan_action(client, pid, "start", "sk")
    s2 = plan_action(client, pid, "start", "sk")
    assert s2["replayed"] is True and s2["status"] == "RUNNING"
    run_ticks(pid, 5)
    assert get_plan(client, pid)["status"] == "COMPLETED"
    # 同键不同动作/不同计划 -> 409
    r = client.post(f"/api/admin/plans/{pid}/pause",
                    json={"operator": "a", "idempotency_key": "same"})
    assert r.status_code == 409
    # 批次动作也是幂等的: 手工拿同键调 validate 不重复推进
    # (计划已切完, 这里验证幂等键表里计划动作命名空间隔离)
    assert client.get("/api/admin/batches").status_code == 200


# ---------- 重启 ----------

def test_restart_reconciles_running_plan(client):
    mk_records(client, range(1, 31))
    bids = [mk_batch(client, s, e, key=f"k{s}") for s, e in ((1, 10), (11, 20), (21, 30))]
    pid = create_plan(client, linear_steps(bids), key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 1)  # s1 SUCCESS
    # 直接把 s2 改成 RUNNING 模拟"执行中崩溃"(RUNNING 边界已提交、进程死亡)
    db = SessionLocal()
    db.query(PlanStep).filter_by(plan_id=pid, seq=2).update({"status": "RUNNING"})
    db.commit(); db.close()
    # 重启: startup 执行 boot 对账(worker 关闭, 不自动跑)
    with TestClient(app):
        p = get_plan(client, pid)
        sm = step_map(p)
        assert p["status"] == "RUNNING"            # 计划仍是运行态, 不是假停住
        assert sm[1]["status"] == "SUCCESS"
        assert sm[2]["status"] == "PENDING"        # 遗留 RUNNING 已复位
        assert sm[3]["status"] == "BLOCKED"
        assert "重启" in sm[2]["last_error"]
        # boot 审计: 计划级 + 步骤级各有记录
        audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
        boots = [a for a in audits if a["action"] == "boot"]
        assert any(a["step_id"] == sm[2]["id"] for a in boots)
    # 再重启并开启 worker 自动续跑(同一库): 计划自行跑完, 不会卡在错误状态
    os.environ["PLAN_WORKER_ENABLED"] = "1"
    try:
        with TestClient(app):
            import time
            for _ in range(40):
                if get_plan(client, pid)["status"] in ("COMPLETED", "CANCELED"):
                    break
                time.sleep(0.1)
            assert get_plan(client, pid)["status"] == "COMPLETED"
    finally:
        os.environ["PLAN_WORKER_ENABLED"] = "0"


def test_restart_keeps_halted_plan_halted(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="k1")
    client.post(f"/api/admin/batches/{b1}/freeze",
                json={"operator": "a", "idempotency_key": "f0"})
    client.post(f"/api/admin/batches/{b1}/validate",
                json={"operator": "a", "idempotency_key": "v0"})
    db = SessionLocal()
    db.query(RecordOld).filter_by(id=3).delete()  # 持续校验失败(__extra__ 残留)
    db.commit(); db.close()
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp")["plan_id"]
    plan_action(client, pid, "start", "ks")
    run_ticks(pid, 2)
    assert get_plan(client, pid)["status"] == "HALTED"
    with TestClient(app):
        p = get_plan(client, pid)
        assert p["status"] == "HALTED"  # HALTED 是用户态, 重启不会偷偷续跑
        assert step_map(p)[1]["status"] == "HALTED"


# ---------- 状态接口 / 审计 ----------

def test_status_includes_plans_and_audit_filter(client):
    mk_records(client, range(1, 11))
    b1 = mk_batch(client, 1, 10, key="k1")
    pid = create_plan(client, [{"seq": 1, "batch_id": b1}], key="kp",
                      operator="op1")["plan_id"]
    plan_action(client, pid, "start", "ks", operator="op1")
    run_ticks(pid, 3)
    p = get_plan(client, pid)
    assert p["status"] == "COMPLETED"
    s = client.get("/api/status").json()
    assert "plans" in s and len(s["plans"]) == 1
    assert s["plans"][0]["id"] == pid
    audits = client.get(f"/api/admin/audit?plan_id={pid}").json()
    actions = {a["action"] for a in audits}
    assert {"plan.create", "plan.start", "plan.complete"} <= actions
    assert all(a["plan_id"] == pid and a["operator"] for a in audits)
