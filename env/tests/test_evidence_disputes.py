"""异常回执争议处理工作流测试。

覆盖:
- 正常打开(PARTIAL/REJECTED 回执)、当场指定处理人/先 OPEN 再指派、
  处理人提交结论、管理员确认关闭的完整状态机;
- 未授权处理(非创建管理员打开/关闭、非当前处理人提交结论、
  未指派时提交、处理人=打开管理员)一律拒绝;
- 重复提交(已 RESOLVED 再提交、CLOSED 后任何动作、重复关闭)拒绝;
- 状态冲突(SIGNED 回执不能开争议、OPEN 不能关闭/提交、
  未 RESOLVED 不能关闭、重复 assign 同管理员);
- 重复创建同一回执的争议单幂等(同键/异键/关闭后重开);
- 过期(EXPIRED)/待处理(PENDING_PROCESS)/撤销(REVOKED)的分发包不能新开争议
  (撤销前已打开的争议撤销后仍可处理/关闭);
- 原始回执、逐事件结果与分发包摘要只读(争议流程不改写, 快照固化);
- 待处理争议列表/当前处理人/结论/事件流水展示; 退回处理(reopen/改派);
- 服务重启(新进程/新会话, 复用同一数据库文件)后争议状态/处理人/结论/流水保留。
"""
import os
import tempfile

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp(prefix="evidence-dispute-test-")
_DB_PATH = f"{_tmp}/test.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["EVIDENCE_STORE_DIR"] = f"{_tmp}/store"
os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"] = f"{_tmp}/dist-store"

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app import distribution, evidence  # noqa: E402


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(os.environ["EVIDENCE_STORE_DIR"], exist_ok=True)
    os.makedirs(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"], exist_ok=True)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建(复用回执链路) ----------

def add_records(client, ids, tags="a,b"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": tags})
        assert r.status_code == 201, r.text


def mk_batch(client, start, end, key, biz="biz"):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key,
                          "biz": biz, "id_start": start, "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def mk_plan(client, batch_id, key):
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": key, "name": "plan",
        "risk_level": "LOW",
        "steps": [{"seq": 1, "batch_id": batch_id}]})
    assert r.status_code == 201, r.text
    return r.json()["plan_id"]


def complete_plan(client, pid, start_key, ticks=6):
    from app import plans
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": start_key})
    assert r.status_code == 200, r.text
    db = SessionLocal()
    try:
        for _ in range(ticks):
            plans.run_plan_tick(db, pid)
    finally:
        db.close()
    assert client.get(f"/api/admin/plans/{pid}").json()["status"] == "COMPLETED"


def make_archived_review(client, key="k", start=1, end=3):
    add_records(client, range(start, end + 1))
    b = mk_batch(client, start, end, f"b-{key}")
    pid = mk_plan(client, b, f"p-{key}")
    complete_plan(client, pid, f"s-{key}")
    r = client.post("/api/admin/evidence/sessions",
                    json={"operator": "alice", "idempotency_key": f"es-{key}",
                          "plan_id": pid})
    assert r.status_code == 201, r.text
    sid = r.json()["session_id"]
    cursor, items = None, []
    while True:
        r = client.post(f"/api/admin/evidence/sessions/{sid}/pages",
                        json={"operator": "alice", "cursor": cursor,
                              "limit": 50})
        j = r.json()
        items.extend(j["items"])
        cursor = j["cursor"]["next_cursor"]
        if not j["cursor"]["has_more"]:
            break
    r = client.post("/api/admin/evidence/exports",
                    json={"operator": "alice", "idempotency_key": f"ee-{key}",
                          "session_id": sid, "segment_size": 2})
    eid = r.json()["export_id"]
    w = evidence.EvidenceExportWorker()
    for _ in range(100):
        if client.get(f"/api/admin/evidence/exports/{eid}").json()[
                "status"] in ("COMPLETED", "FAILED"):
            break
        w.tick_once()
    r = client.post("/api/admin/evidence/reviews",
                    json={"operator": "alice", "idempotency_key": f"rv-{key}",
                          "session_id": sid})
    rid = r.json()["review_id"]
    gseqs = sorted(i["global_seq"] for i in items)
    n = len(gseqs)
    for i, gs in enumerate(gseqs):
        rr = client.post(f"/api/admin/evidence/reviews/{rid}/conclusions",
                         json={"operator": "bob", "idempotency_key": f"b-{key}-{gs}",
                               "global_seq": gs, "verdict": "CONFIRMED"},
                         headers={"If-Match": str(i)})
        assert rr.status_code == 200, rr.text
    for i, gs in enumerate(gseqs):
        rr = client.post(f"/api/admin/evidence/reviews/{rid}/conclusions",
                         json={"operator": "carol", "idempotency_key": f"c-{key}-{gs}",
                               "global_seq": gs, "verdict": "CONFIRMED"},
                         headers={"If-Match": str(n + i)})
        assert rr.status_code == 200, rr.text
    r = client.post(f"/api/admin/evidence/reviews/{rid}/archive",
                    json={"operator": "dave", "idempotency_key": f"ar-{key}"})
    assert r.status_code == 200, r.text
    return client.get(f"/api/admin/evidence/reviews/{rid}").json(), gseqs


def register(client, recipient, key=None, operator="admin", **extra):
    body = {"operator": operator,
            "idempotency_key": key or f"rcp-{recipient}",
            "recipient": recipient, **extra}
    r = client.post("/api/admin/evidence/recipients", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def create_dist(client, rid, recipient="auditor1", key="dist", ttl=3600):
    r = client.post("/api/admin/evidence/distributions", json={
        "operator": "admin", "idempotency_key": key, "review_id": rid,
        "recipient": recipient, "redaction_policy": "STANDARD",
        "ttl_seconds": ttl})
    assert r.status_code == 201, r.text
    return r.json()


def issue_and_redeem(client, pid, recipient, key=None):
    body = {"operator": recipient, "idempotency_key": key or f"tok-{recipient}"}
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/download-token", json=body)
    assert r.status_code == 201, r.text
    j = r.json()
    rd = client.get(
        f"/api/admin/evidence/distribution-downloads/{j['token']}",
        params={"operator": recipient})
    assert rd.status_code == 200, rd.text
    return j["download_id"], rd.content


def signed_events(seqs):
    return [{"global_seq": gs, "result": "CONFIRMED"} for gs in seqs]


def submit_partial(client, pid, recipient, download_id, d, seqs, key,
                   anomaly_idx=0, note="一个事件存疑"):
    evs = signed_events(seqs)
    evs[anomaly_idx] = {"global_seq": seqs[anomaly_idx], "result": "ANOMALY",
                        "note": "该事件载荷与现场记录不符"}
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/receipts", json={
            "operator": recipient, "idempotency_key": key,
            "download_id": download_id, "manifest_hash": d["manifest_hash"],
            "content_digest": d["content_digest"], "receipt_type": "PARTIAL",
            "note": note, "events": evs})


def submit_rejected(client, pid, recipient, download_id, d, key,
                    note="整包内容来源不可信, 拒收"):
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/receipts", json={
            "operator": recipient, "idempotency_key": key,
            "download_id": download_id, "manifest_hash": d["manifest_hash"],
            "content_digest": d["content_digest"], "receipt_type": "REJECTED",
            "note": note})


def set_valid_until_past(client, pid):
    from datetime import timedelta
    db = SessionLocal()
    try:
        db.get(distribution.EvidenceDistribution, pid).valid_until = \
            evidence.now_utc_naive() - timedelta(seconds=10)
        db.commit()
    finally:
        db.close()


def sweep(client, key="sweep"):
    return client.post(
        "/api/admin/evidence/distributions/lifecycle/sweep",
        json={"operator": "admin", "idempotency_key": key})


def revoke(client, pid, key="rev", reason="测试撤销"):
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/revoke",
        json={"operator": "admin", "idempotency_key": key, "reason": reason})


def get_receipt_id(client, pid, recipient="auditor1"):
    rep = client.get(
        f"/api/admin/evidence/distributions/{pid}/receipts",
        params={"operator": "admin"}).json()
    a = next(x for x in rep["assignments"] if x["recipient"] == recipient)
    return a["receipt"]["receipt_id"]


def open_dispute(client, receipt_id, *, operator="admin", key="do1", **kw):
    body = {"operator": operator, "idempotency_key": key, **kw}
    return client.post(
        f"/api/admin/evidence/receipts/{receipt_id}/dispute", json=body)


def assign_dispute(client, did, *, operator="admin", assignee="handler1",
                   key="da1", opinion="请核对异常事件",
                   supp="补充现场记录摘要", reason=None):
    body = {"operator": operator, "idempotency_key": key, "assignee": assignee,
            "handling_opinion": opinion,
            "supplementary_evidence": supp}
    if reason:
        body["reason"] = reason
    return client.post(
        f"/api/admin/evidence/disputes/{did}/assign", json=body)


def resolve_dispute(client, did, *, operator="handler1", key="dr1",
                    resolution="核查属实, 已更正并补充证据"):
    return client.post(
        f"/api/admin/evidence/disputes/{did}/resolve", json={
            "operator": operator, "idempotency_key": key,
            "resolution": resolution})


def close_dispute(client, did, *, operator="admin", key="dc1", note=None):
    body = {"operator": operator, "idempotency_key": key}
    if note:
        body["note"] = note
    return client.post(
        f"/api/admin/evidence/disputes/{did}/close", json=body)


def get_dispute(client, did, operator="admin"):
    return client.get(
        f"/api/admin/evidence/disputes/{did}", params={"operator": operator})


@pytest.fixture()
def partial_package(client):
    """含一张 PARTIAL 回执的分发包(auditor1)。返回 (dist, gseqs, receipt_id)。"""
    rv, gseqs = make_archived_review(client, key="base")
    register(client, "auditor1")
    register(client, "handler1", key="rcp-h1")
    d = create_dist(client, rv["review_id"], ttl=3600, key="d1")
    dl_id, _ = issue_and_redeem(client, d["package_id"], "auditor1", key="tk1")
    r = submit_partial(client, d["package_id"], "auditor1", dl_id, d,
                       sorted(gseqs), key="rp1")
    assert r.status_code == 201, r.text
    rid = get_receipt_id(client, d["package_id"])
    return d, sorted(gseqs), rid


# ======================================================================
# ---------- 正常打开 -> 指派 -> 处理结论 -> 管理员确认关闭 ----------
# ======================================================================

class TestHappyPath:
    def test_open_then_assign_resolve_close(self, client, partial_package):
        d, gseqs, rid = partial_package
        # 1) 管理员打开(不指派) -> OPEN
        r = open_dispute(client, rid, key="o1", reason="核查异常回执")
        assert r.status_code == 201, r.text
        j = r.json()
        did = j["dispute_id"]
        assert j["status"] == "OPEN" and j["assignee"] is None
        assert j["receipt_type"] == "PARTIAL" and j["deduped"] is False
        # 2) OPEN 时处理人不能提交结论
        rr = resolve_dispute(client, did, operator="handler1", key="x")
        assert rr.status_code == 403
        assert rr.json()["detail"]["code"] == "dispute_not_assigned"
        # 3) 管理员指派处理人(处理意见/补充证据摘要必填) -> ASSIGNED
        rr = assign_dispute(client, did, assignee="handler1", key="a1")
        assert rr.status_code == 200, rr.text
        assert rr.json()["status"] == "ASSIGNED"
        assert rr.json()["assignee"] == "handler1"
        # 4) 指派参数缺失 -> 422
        rr = client.post(f"/api/admin/evidence/disputes/{did}/assign", json={
            "operator": "admin", "idempotency_key": "a2",
            "assignee": "handler2"})
        assert rr.status_code == 422
        # 5) 非处理人提交结论 -> 403
        rr = resolve_dispute(client, did, operator="someone", key="r0")
        assert rr.status_code == 403
        assert rr.json()["detail"]["code"] == "not_current_assignee"
        # 6) 当前处理人提交结论 -> RESOLVED
        rr = resolve_dispute(client, did, operator="handler1", key="r1")
        assert rr.status_code == 200, rr.text
        rj = rr.json()
        assert rj["status"] == "RESOLVED"
        assert rj["resolved_by"] == "handler1" and rj["resolution"]
        # 7) RESOLVED 时非管理员不能关闭
        rr = close_dispute(client, did, operator="handler1", key="c0")
        assert rr.status_code == 403
        assert rr.json()["detail"]["code"] == "not_distribution_admin"
        # 8) 管理员确认关闭 -> CLOSED
        rr = close_dispute(client, did, operator="admin", key="c1",
                           note="结论属实, 同意关闭")
        assert rr.status_code == 200, rr.text
        assert rr.json()["status"] == "CLOSED"
        assert rr.json()["closed_by"] == "admin"
        # 详情: 事件流水含四步, 操作者/时间/原因齐备
        detail = get_dispute(client, did).json()
        assert [e["event"] for e in detail["events"]] == [
            "dispute.open", "dispute.assign", "dispute.resolve",
            "dispute.close"]
        assert all(e["operator"] and e["ts"] and e["to_status"]
                   for e in detail["events"])
        assert detail["handling_opinion"] and detail["supplementary_evidence"]

    def test_open_with_assignee_requires_handling_detail(self, client,
                                                          partial_package):
        d, gseqs, rid = partial_package
        r = open_dispute(client, rid, key="o1", assignee="handler1")
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "handling_detail_required"
        r = open_dispute(client, rid, key="o2", assignee="handler1",
                         handling_opinion="有意见但无补充证据")
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "handling_detail_required"
        # 不指派处理人则可以先打开 OPEN
        r = open_dispute(client, rid, key="o3")
        assert r.status_code == 201 and r.json()["status"] == "OPEN"

    def test_open_with_assignee_goes_direct_to_assigned(self, client,
                                                        partial_package):
        d, gseqs, rid = partial_package
        r = open_dispute(client, rid, key="o1", assignee="handler1",
                         handling_opinion="请处理",
                         supplementary_evidence="证据摘要",
                         reason="直接指派")
        assert r.status_code == 201, r.text
        j = r.json()
        assert j["status"] == "ASSIGNED" and j["assignee"] == "handler1"
        # 处理人可立即提交结论
        rr = resolve_dispute(client, j["dispute_id"], key="r1")
        assert rr.status_code == 200
        rr = close_dispute(client, j["dispute_id"], key="c1")
        assert rr.status_code == 200
        assert rr.json()["status"] == "CLOSED"

    def test_rejected_receipt_open_dispute(self, client):
        rv, gseqs = make_archived_review(client, key="rj")
        register(client, "auditor1")
        register(client, "handler1", key="rcp-h1")
        d = create_dist(client, rv["review_id"], key="d1")
        dl_id, _ = issue_and_redeem(client, d["package_id"], "auditor1",
                                   key="tk1")
        assert submit_rejected(client, d["package_id"], "auditor1", dl_id, d,
                               key="rj1").status_code == 201
        rid = get_receipt_id(client, d["package_id"])
        r = open_dispute(client, rid, key="o1", assignee="handler1",
                         handling_opinion="核实拒收依据",
                         supplementary_evidence="补充")
        assert r.status_code == 201
        assert r.json()["receipt_type"] == "REJECTED"


# ======================================================================
# ---------- 未授权处理 ----------
# ======================================================================

class TestAuthorization:
    def test_only_creator_admin_can_open(self, client, partial_package):
        d, gseqs, rid = partial_package
        # 其他人(接收方/处理人)不能打开
        r = open_dispute(client, rid, operator="auditor1", key="o1")
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "not_distribution_admin"
        r = open_dispute(client, rid, operator="handler1", key="o2")
        assert r.status_code == 403

    def test_assignee_cannot_be_opener(self, client, partial_package):
        d, gseqs, rid = partial_package
        r = open_dispute(client, rid, key="o1", assignee="admin",
                         handling_opinion="x", supplementary_evidence="y")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "assignee_is_opener"
        # 先 OPEN 再指派自己同样拒绝
        r = open_dispute(client, rid, key="o2")
        did = r.json()["dispute_id"]
        rr = assign_dispute(client, did, assignee="admin", key="a1")
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "assignee_is_opener"

    def test_only_opener_admin_can_assign(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1").json()["dispute_id"]
        # 处理人本人不能指派
        rr = assign_dispute(client, did, operator="handler1",
                            assignee="handler1", key="a1")
        assert rr.status_code == 403
        assert rr.json()["detail"]["code"] == "not_dispute_admin"

    def test_handler_can_only_resolve_after_assignment(self, client,
                                                       partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1").json()["dispute_id"]
        # 未指派 -> 403(即使将来要指派给他)
        rr = resolve_dispute(client, did, operator="handler1", key="r1")
        assert rr.status_code == 403
        assert rr.json()["detail"]["code"] == "dispute_not_assigned"
        assign_dispute(client, did, assignee="handler1", key="a1")
        # 被指派后可以提交
        assert resolve_dispute(client, did, key="r2").status_code == 200

    def test_view_access_control(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="x",
                           supplementary_evidence="y").json()["dispute_id"]
        # 无关第三方不能查看
        r = get_dispute(client, did, operator="mallory")
        assert r.status_code == 403
        # 创建管理员 / 处理人 / 接收方可查看
        assert get_dispute(client, did, "admin").status_code == 200
        assert get_dispute(client, did, "handler1").status_code == 200
        assert get_dispute(client, did, "auditor1").status_code == 200


# ======================================================================
# ---------- 重复提交 / 状态冲突 ----------
# ======================================================================

class TestDuplicatesAndStateConflicts:
    def test_duplicate_open_same_receipt_idempotent(self, client,
                                                    partial_package):
        d, gseqs, rid = partial_package
        r1 = open_dispute(client, rid, key="same", reason="首次")
        r2 = open_dispute(client, rid, key="same", reason="首次")
        assert r1.status_code == 201 and r2.status_code == 201
        assert r2.json()["replayed"] is True
        assert r2.json()["dispute_id"] == r1.json()["dispute_id"]
        # 不同幂等键重复打开同一回执 -> 自然幂等, 返回同一争议单
        r3 = open_dispute(client, rid, key="other-key",
                          assignee="handler1", handling_opinion="x",
                          supplementary_evidence="y")
        assert r3.status_code == 201
        assert r3.json()["deduped"] is True
        assert r3.json()["dispute_id"] == r1.json()["dispute_id"]
        # 首次为 OPEN, 幂等回显不改变状态
        assert r3.json()["status"] == "OPEN"
        # 库里只有一张争议单
        rows = client.get("/api/admin/evidence/disputes").json()
        assert [x["dispute_id"] for x in rows].count(r1.json()["dispute_id"]) == 1

    def test_cannot_reopen_after_close(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="x",
                           supplementary_evidence="y").json()["dispute_id"]
        resolve_dispute(client, did, key="r1")
        close_dispute(client, did, key="c1")
        # 关闭后再就同一回执新开 -> 幂等回显已关闭争议(不新建)
        r = open_dispute(client, rid, key="o2", assignee="handlerX",
                         handling_opinion="x", supplementary_evidence="y")
        assert r.json()["deduped"] is True
        assert r.json()["status"] == "CLOSED"
        # CLOSED 后指派/提交/关闭全部拒绝
        assert assign_dispute(client, did, assignee="handler2",
                              key="a2").status_code == 409
        assert resolve_dispute(client, did, key="r2").status_code == 409
        rr = close_dispute(client, did, key="c2")
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "dispute_closed"

    def test_duplicate_resolution_rejected(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="x",
                           supplementary_evidence="y").json()["dispute_id"]
        r1 = resolve_dispute(client, did, key="r1")
        assert r1.status_code == 200
        # 不同键重复提交结论 -> 拒绝(等待管理员确认/退回)
        r2 = resolve_dispute(client, did, key="r2", resolution="再交一次")
        assert r2.status_code == 409
        assert r2.json()["detail"]["code"] == "dispute_already_resolved"
        # 同键重放 -> 幂等
        r3 = resolve_dispute(client, did, key="r1")
        assert r3.json()["replayed"] is True

    def test_close_only_from_resolved(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1").json()["dispute_id"]
        # OPEN 不能关闭
        rr = close_dispute(client, did, key="c1")
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "dispute_not_resolved"
        assign_dispute(client, did, assignee="handler1", key="a1")
        # ASSIGNED 也不能关闭
        rr = close_dispute(client, did, key="c2")
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "dispute_not_resolved"

    def test_signed_receipt_not_disputable(self, client):
        rv, gseqs = make_archived_review(client, key="sg")
        register(client, "auditor1")
        d = create_dist(client, rv["review_id"], key="d1")
        dl_id, _ = issue_and_redeem(client, d["package_id"], "auditor1",
                                   key="tk1")
        r = client.post(
            f"/api/admin/evidence/distributions/{d['package_id']}/receipts",
            json={"operator": "auditor1", "idempotency_key": "rs1",
                  "download_id": dl_id, "manifest_hash": d["manifest_hash"],
                  "content_digest": d["content_digest"], "receipt_type": "SIGNED",
                  "events": signed_events(sorted(gseqs))})
        assert r.status_code == 201
        rid = get_receipt_id(client, d["package_id"])
        rr = open_dispute(client, rid, key="o1")
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "receipt_not_disputable"

    def test_admin_return_resolved_for_rework(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="初核",
                           supplementary_evidence="证据1").json()["dispute_id"]
        resolve_dispute(client, did, key="r1", resolution="初核结论")
        # 退回处理(可改派 handler2)
        rr = client.post(
            f"/api/admin/evidence/disputes/{did}/reopen", json={
                "operator": "admin", "idempotency_key": "ro1",
                "reason": "结论依据不足, 请补充", "new_assignee": "handler2"})
        assert rr.status_code == 200, rr.text
        j = rr.json()
        assert j["status"] == "ASSIGNED" and j["assignee"] == "handler2"
        assert j["reopen_count"] == 1
        assert j["resolution"] is None  # 旧结论清空(事件流水保留)
        # 旧处理人不再能提交
        assert resolve_dispute(client, did, operator="handler1",
                               key="rx").status_code == 403
        # 新处理人可提交, 之后关闭
        assert resolve_dispute(client, did, operator="handler2",
                               key="r2").status_code == 200
        assert close_dispute(client, did, key="c1").status_code == 200


# ======================================================================
# ---------- 撤销/过期: 不能新开; 已存在争议不受影响 ----------
# ======================================================================

class TestRevokedAndExpired:
    def test_revoked_package_blocks_new_dispute(self, client, partial_package):
        d, gseqs, rid = partial_package
        pid = d["package_id"]
        # 先为第一张 PARTIAL 回执打开争议(撤销前)
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="x",
                           supplementary_evidence="y").json()["dispute_id"]
        # 第二接收方在撤销前完成 REJECTED 回执(撤销后将不能就它新开争议)
        register(client, "auditor2", key="rcp2")
        assert client.post(
            f"/api/admin/evidence/distributions/{pid}/recipients", json={
                "operator": "admin", "idempotency_key": "as2",
                "recipient": "auditor2"}).status_code == 201
        dl2, _ = issue_and_redeem(client, pid, "auditor2", key="tk2")
        assert submit_rejected(client, pid, "auditor2", dl2, d,
                               key="rj2").status_code == 201
        rid2 = get_receipt_id(client, pid, "auditor2")
        revoke(client, pid, key="rv1")
        # 撤销后就第二张回执新开争议 -> 拒绝(幂等回显不适用, 这是全新回执)
        r = open_dispute(client, rid2, key="o2", assignee="handler1",
                         handling_opinion="x", supplementary_evidence="y")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_revoked"
        # 撤销前已存在的争议不受影响, 仍可走完流程
        assert resolve_dispute(client, did, key="r1").status_code == 200
        rr = close_dispute(client, did, key="c1")
        assert rr.status_code == 200 and rr.json()["status"] == "CLOSED"

    def test_expired_and_pending_package_block_open(self, client,
                                                    partial_package):
        d, gseqs, rid = partial_package
        pid = d["package_id"]
        # 过期(未 sweep): effective_status=EXPIRED -> 拒绝
        set_valid_until_past(client, pid)
        r = open_dispute(client, rid, key="o1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_expired"
        # 全部接收方都已回执时 sweep 不进待处理(派生为 EXPIRED), 仍是过期拒绝
        sweep(client, key="sw1")
        r = open_dispute(client, rid, key="o2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_expired"
        revoke(client, pid, key="rv1")
        r = open_dispute(client, rid, key="o3")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_revoked"

    def test_pending_process_package_blocks_open(self, client,
                                                  partial_package):
        """存在未完成回执接收方 -> sweep 进入 PENDING_PROCESS, 不能新开争议。"""
        d, gseqs, rid = partial_package
        pid = d["package_id"]
        register(client, "auditor2", key="rcp2")
        assert client.post(
            f"/api/admin/evidence/distributions/{pid}/recipients", json={
                "operator": "admin", "idempotency_key": "as2",
                "recipient": "auditor2"}).status_code == 201
        set_valid_until_past(client, pid)
        assert sweep(client, key="sw1").json()["swept"] == 1
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["status"] == "PENDING_PROCESS"
        r = open_dispute(client, rid, key="o1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_pending_process"

    def test_existing_dispute_opened_before_revoke_listed(self, client,
                                                          partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", reason="撤销前打开"
                           ).json()["dispute_id"]
        revoke(client, d["package_id"], key="rv1")
        # 撤销后争议单仍可查/指派(打开时刻是唯一闸门)
        assert get_dispute(client, did).status_code == 200
        rr = assign_dispute(client, did, assignee="handler1", key="a1")
        assert rr.status_code == 200


# ======================================================================
# ---------- 只读不变量: 原始回执/逐事件结果/分发包摘要不被争议改写 ----------
# ======================================================================

class TestReadonlySnapshots:
    def test_dispute_flow_does_not_mutate_receipt_or_package(self, client,
                                                              partial_package):
        d, gseqs, rid = partial_package
        pid = d["package_id"]
        before = client.get(
            f"/api/admin/evidence/distributions/{pid}/receipts",
            params={"operator": "admin"}).json()
        before_receipt = next(x for x in before["receipts"]
                              if x["receipt_id"] == rid)
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="处理意见",
                           supplementary_evidence="补充证据摘要",
                           reason="开单").json()["dispute_id"]
        resolve_dispute(client, did, key="r1", resolution="结论")
        close_dispute(client, did, key="c1", note="关闭")
        after = client.get(
            f"/api/admin/evidence/distributions/{pid}/receipts",
            params={"operator": "admin"}).json()
        after_receipt = next(x for x in after["receipts"]
                             if x["receipt_id"] == rid)
        # 原始回执与逐事件结果字段不变
        for k in ("receipt_type", "manifest_hash", "content_digest",
                  "confirmed_count", "anomaly_count", "rejected_event_count",
                  "submitted_by", "note"):
            assert after_receipt[k] == before_receipt[k], k
        assert after_receipt["events"] == before_receipt["events"]
        # 分发包固定摘要不变
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["manifest_hash"] == d["manifest_hash"]
        assert detail["content_digest"] == d["content_digest"]
        assert detail["signature_digest"] == d["signature_digest"]
        # 回执上挂有争议只读摘要
        assert after_receipt["dispute"]["dispute_id"] == did
        assert after_receipt["dispute"]["status"] == "CLOSED"

    def test_snapshots_frozen_at_open(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1", assignee="handler1",
                           handling_opinion="x",
                           supplementary_evidence="y").json()["dispute_id"]
        detail = get_dispute(client, did).json()
        # 固化的原始回执(含逐事件结果)
        snap = detail["receipt_snapshot"]
        assert snap["receipt_id"] == rid
        assert snap["receipt_type"] == "PARTIAL"
        assert {e["global_seq"] for e in snap["events"]} == set(gseqs)
        anomaly = next(e for e in snap["events"] if e["result"] == "ANOMALY")
        assert anomaly["note"] == "该事件载荷与现场记录不符"
        # 固化的分发包摘要
        ps = detail["package_snapshot"]
        assert ps["package_id"] == d["package_id"]
        assert ps["manifest_hash"] == d["manifest_hash"]
        assert ps["status"] == "ACTIVE"
        # 争议流程向分发包事件流追加 dispute.* 留痕(打开即指派, 含处理人信息),
        # 但不改固定摘要
        dist = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}",
            params={"operator": "admin"}).json()
        names = [e["event"] for e in dist["events"]]
        assert "dispute.open" in names
        open_ev = next(e for e in dist["events"]
                       if e["event"] == "dispute.open")
        assert open_ev["detail"]["assignee"] == "handler1"
        # 显式改派再产生 dispute.assign
        assert assign_dispute(client, did, assignee="handler2",
                              key="a9").status_code == 200
        dist = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}",
            params={"operator": "admin"}).json()
        assert "dispute.assign" in [e["event"] for e in dist["events"]]


# ======================================================================
# ---------- 列表/待处理展示 ----------
# ======================================================================

class TestListing:
    def test_pending_disputes_list_and_filters(self, client, partial_package):
        d, gseqs, rid = partial_package
        did = open_dispute(client, rid, key="o1").json()["dispute_id"]
        # 待处理列表
        rows = client.get(
            "/api/admin/evidence/disputes",
            params={"pending_only": "true"}).json()
        assert [x["dispute_id"] for x in rows] == [did]
        # 按状态过滤
        assert client.get("/api/admin/evidence/disputes",
                          params={"status_filter": "OPEN"}).json()
        bad = client.get("/api/admin/evidence/disputes",
                         params={"status_filter": "NOPE"})
        assert bad.status_code == 422
        assign_dispute(client, did, assignee="handler1", key="a1")
        rows = client.get("/api/admin/evidence/disputes",
                          params={"status_filter": "ASSIGNED"}).json()
        assert len(rows) == 1 and rows[0]["assignee"] == "handler1"
        # 按处理人过滤
        rows = client.get("/api/admin/evidence/disputes",
                          params={"assignee": "handler1"}).json()
        assert len(rows) == 1
        rows = client.get("/api/admin/evidence/disputes",
                          params={"assignee": "nobody"}).json()
        assert rows == []
        # 关闭后不再出现在 pending_only
        resolve_dispute(client, did, key="r1")
        close_dispute(client, did, key="c1")
        assert client.get("/api/admin/evidence/disputes",
                          params={"pending_only": "true"}).json() == []
        closed = client.get("/api/admin/evidence/disputes",
                            params={"status_filter": "CLOSED"}).json()
        assert len(closed) == 1
        # 包详情携带争议统计
        detail = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}",
            params={"operator": "admin"}).json()
        assert detail["dispute_count"] == 1
        assert detail["open_dispute_count"] == 0
        # 状态概览
        st = client.get("/api/status").json()
        assert "evidence_disputes" in st

    def test_dispute_not_found(self, client, partial_package):
        r = get_dispute(client, "EDCnonexistent")
        assert r.status_code == 409 or r.status_code == 404
        # get_dispute 定义为 404
        assert r.status_code == 404
        assert r.json()["detail"]["code"] == "dispute_not_found"


# ======================================================================
# ---------- 服务重启后状态保留 ----------
# ======================================================================

class TestRestartPersistence:
    def test_state_survives_restart_real(self, client, partial_package):
        d, gseqs, rid = partial_package
        r = open_dispute(client, rid, key="o1", reason="重启前打开",
                         handling_opinion=None)
        # OPEN 状态先指派
        did = r.json()["dispute_id"]
        assign_dispute(client, did, assignee="handler1", key="a1",
                       opinion="重启核对", supp="补充摘要")
        resolve_dispute(client, did, key="r1", resolution="重启前提交的结论")
        # 争议处于 RESOLVED(未关闭)时模拟服务重启: 全新 TestClient 触发 startup,
        # 复用同一磁盘数据库文件
        with TestClient(app) as client2:
            # 1) 争议单仍为 RESOLVED, 处理人/结论/意见保留
            detail = client2.get(
                f"/api/admin/evidence/disputes/{did}",
                params={"operator": "admin"}).json()
            assert detail["status"] == "RESOLVED"
            assert detail["assignee"] == "handler1"
            assert detail["resolution"] == "重启前提交的结论"
            assert detail["resolved_by"] == "handler1"
            assert detail["handling_opinion"] == "重启核对"
            assert detail["supplementary_evidence"] == "补充摘要"
            # 2) 事件流水完整保留
            assert [e["event"] for e in detail["events"]] == [
                "dispute.open", "dispute.assign", "dispute.resolve"]
            # 3) 待处理列表仍包含它
            pending = client2.get("/api/admin/evidence/disputes",
                                  params={"pending_only": "true"}).json()
            assert any(x["dispute_id"] == did for x in pending)
            # 4) 重启后可继续走完: 管理员确认关闭
            rr = close_dispute(client2, did, key="c-after-restart",
                               note="重启后确认")
            assert rr.status_code == 200, rr.text
            assert rr.json()["status"] == "CLOSED"
            # 5) 只读快照仍在
            detail2 = client2.get(
                f"/api/admin/evidence/disputes/{did}",
                params={"operator": "admin"}).json()
            assert detail2["receipt_snapshot"]["receipt_id"] == rid
            assert len(detail2["receipt_snapshot"]["events"]) == len(gseqs)
            assert detail2["package_snapshot"]["manifest_hash"] == \
                d["manifest_hash"]
