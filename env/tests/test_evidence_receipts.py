"""接收回执与分发包生命周期管理测试。

覆盖:
- 多接收方分配(主接收方自动分派 + 追加接收方/自定义回执截止与事件范围)与进度展示;
- 正常逐事件签收(SIGNED, 必须逐事件 CONFIRMED 且覆盖必须范围);
- 部分异常(PARTIAL, 带异常说明)/整包拒收(REJECTED, 必填原因);
- 摘要不匹配(manifest_hash/content_digest)、非授权接收方、包外事件/范围外事件拒绝;
- 回执一次性(重复拒绝)与同一幂等键重放只得到同一回执;
- 延期双人审批: 申请人不可自审、同人重复去重、第二名不同操作者通过后原子顺延;
- 审批期间包状态变化(撤销/进入待处理)或摘要变化 -> 审批自动失效;
- 到期未完成回执 -> 自动待处理并禁止下载/回执; 已提交回执与归档摘要只读;
- 待处理包恢复必须重新校验通过, 恢复后续期并重开回执窗口;
- 撤销后回执只读; 待处理/撤销后重新签发产生新 package_id 与新回执周期;
- 非回归: 分发包创建/一次性令牌/离线校验/篡改定位/证据分页导出/复核归档。
"""
import io
import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp(prefix="evidence-receipt-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["EVIDENCE_STORE_DIR"] = f"{_tmp}/store"
os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"] = f"{_tmp}/dist-store"

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app import distribution, evidence, plans  # noqa: E402


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(os.environ["EVIDENCE_STORE_DIR"], exist_ok=True)
    os.makedirs(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"], exist_ok=True)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建(复用现有测试的完整链路) ----------

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
        assert r.status_code == 200, r.text
        j = r.json()
        items.extend(j["items"])
        cursor = j["cursor"]["next_cursor"]
        if not j["cursor"]["has_more"]:
            break
    r = client.post("/api/admin/evidence/exports",
                    json={"operator": "alice", "idempotency_key": f"ee-{key}",
                          "session_id": sid, "segment_size": 2})
    assert r.status_code in (200, 201), r.text
    eid = r.json()["export_id"]
    w = evidence.EvidenceExportWorker()
    for _ in range(100):
        st = client.get(f"/api/admin/evidence/exports/{eid}").json()["status"]
        if st in ("COMPLETED", "FAILED", "PAUSED", "CANCELED"):
            break
        w.tick_once()
    assert client.get(f"/api/admin/evidence/exports/{eid}").json()[
               "status"] == "COMPLETED"
    r = client.post("/api/admin/evidence/reviews",
                    json={"operator": "alice", "idempotency_key": f"rv-{key}",
                          "session_id": sid})
    assert r.status_code == 201, r.text
    rid = r.json()["review_id"]
    gseqs = [i["global_seq"] for i in items]
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


def create_dist(client, rid, recipient="auditor1", key="dist",
                policy="STANDARD", ttl=3600, valid_until=None):
    body = {"operator": "admin", "idempotency_key": key,
            "review_id": rid, "recipient": recipient,
            "redaction_policy": policy}
    if valid_until is not None:
        body["valid_until"] = valid_until
    else:
        body["ttl_seconds"] = ttl
    r = client.post("/api/admin/evidence/distributions", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def add_recipient_assignment(client, pid, recipient, *, operator="admin",
                             key=None, due=None, seqs=None, note=None):
    body = {"operator": operator, "idempotency_key": key or f"asg-{recipient}",
            "recipient": recipient}
    if due is not None:
        body["receipt_due_at"] = due
    if seqs is not None:
        body["required_global_seqs"] = seqs
    if note is not None:
        body["note"] = note
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/recipients", json=body)


def issue_and_redeem(client, pid, recipient, key=None, ttl=None):
    """签发并兑换一次性令牌, 返回 (download_id, raw zip)。"""
    body = {"operator": recipient, "idempotency_key": key or f"tok-{recipient}"}
    if ttl is not None:
        body["ttl_seconds"] = ttl
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/download-token", json=body)
    assert r.status_code == 201, r.text
    j = r.json()
    rd = client.get(
        f"/api/admin/evidence/distribution-downloads/{j['token']}",
        params={"operator": recipient})
    assert rd.status_code == 200, rd.text
    return j["download_id"], rd.content


def submit_receipt(client, pid, recipient, download_id, d, *,
                   rtype="SIGNED", events=None, note=None, key=None,
                   mh=None, cd=None):
    body = {"operator": recipient, "idempotency_key": key or f"rcpt-{recipient}",
            "download_id": download_id,
            "manifest_hash": mh or d["manifest_hash"],
            "content_digest": cd or d["content_digest"],
            "receipt_type": rtype}
    if events is not None:
        body["events"] = events
    if note is not None:
        body["note"] = note
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/receipts", json=body)


def signed_events(seqs):
    return [{"global_seq": gs, "result": "CONFIRMED"} for gs in seqs]


def progress(client, pid, operator="admin"):
    return client.get(
        f"/api/admin/evidence/distributions/{pid}/receipts",
        params={"operator": operator}).json()


def set_valid_until_past(client, pid):
    db = SessionLocal()
    try:
        row = db.get(distribution.EvidenceDistribution, pid)
        row.valid_until = evidence.now_utc_naive() - timedelta(seconds=10)
        db.commit()
    finally:
        db.close()


def revoke(client, pid, operator="admin", key="rev", reason="测试撤销"):
    return client.post(
        f"/api/admin/evidence/distributions/{pid}/revoke",
        json={"operator": operator, "idempotency_key": key, "reason": reason})


def sweep(client, operator="admin", key="sweep"):
    return client.post(
        "/api/admin/evidence/distributions/lifecycle/sweep",
        json={"operator": operator, "idempotency_key": key})


@pytest.fixture()
def package(client):
    """已归档复核单 + 两个在册接收方 + 含 3+ 事件的分发包。"""
    rv, gseqs = make_archived_review(client, key="base")
    register(client, "auditor1")
    register(client, "auditor2")
    d = create_dist(client, rv["review_id"], ttl=3600, key="d1")
    return rv, gseqs, d


# ======================================================================
# ---------- 多接收方分配与进度展示 ----------
# ======================================================================

class TestAssignmentAndProgress:
    def test_primary_assignment_auto_created(self, client, package):
        rv, gseqs, d = package
        rep = progress(client, d["package_id"])
        assert rep["receipt_progress"]["recipient_count"] == 1
        a = rep["assignments"][0]
        assert a["recipient"] == "auditor1" and a["status"] == "PENDING"
        assert a["required_scope_all"] is True
        assert a["receipt_due_at"] == d["valid_until"]
        # 详情接口也带分派与进度
        detail = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}",
            params={"operator": "auditor1"}).json()
        assert detail["receipt_progress"]["unfinished"] == 1
        assert detail["recipient_count"] == 1

    def test_add_multiple_recipients_with_scope_and_due(self, client, package):
        rv, gseqs, d = package
        sub = sorted(gseqs)[:2]
        due = (datetime.utcnow() + timedelta(minutes=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = add_recipient_assignment(
            client, d["package_id"], "auditor2", due=due, seqs=sub)
        assert r.status_code == 201, r.text
        a = r.json()
        assert a["recipient"] == "auditor2"
        assert a["required_global_seqs"] == sub
        assert a["required_scope_all"] is False
        rep = progress(client, d["package_id"])
        p = rep["receipt_progress"]
        assert p["recipient_count"] == 2 and p["unfinished"] == 2
        assert {x["recipient"] for x in p["unfinished_recipients"]} == {
            "auditor1", "auditor2"}
        # 追加后 auditor2 可查看/下载该包
        tok = client.post(
            f"/api/admin/evidence/distributions/{d['package_id']}/download-token",
            json={"operator": "auditor2", "idempotency_key": "t-a2"})
        assert tok.status_code == 201, tok.text

    def test_add_recipient_guards(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        # 非创建管理员不能加
        r = add_recipient_assignment(client, pid, "auditor2",
                                     operator="auditor1", key="x1")
        assert r.status_code == 403
        # 重复分派同一接收方
        r = add_recipient_assignment(client, pid, "auditor1", key="x2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "recipient_already_assigned"
        # 包外事件范围
        r = add_recipient_assignment(client, pid, "auditor2",
                                    seqs=[999999], key="x3")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "event_not_in_package"
        # 晚于包有效期
        late = (datetime.utcnow() + timedelta(days=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = add_recipient_assignment(client, pid, "auditor2",
                                    due=late, key="x4")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "receipt_due_after_validity"
        # 未登记接收方
        r = add_recipient_assignment(client, pid, "ghost", key="x5")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "recipient_not_registered"
        # 分派接口幂等重放
        due = (datetime.utcnow() + timedelta(minutes=20)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r1 = add_recipient_assignment(client, pid, "auditor2", due=due,
                                     key="idem-a2")
        r2 = add_recipient_assignment(client, pid, "auditor2", due=due,
                                     key="idem-a2")
        assert r1.status_code == 201 and r2.json()["replayed"] is True
        assert r2.json()["assignment_id"] == r1.json()["assignment_id"]


# ======================================================================
# ---------- 正常逐事件签收 ----------
# ======================================================================

class TestSignedReceipt:
    def test_event_by_event_signoff_full_scope(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _raw = issue_and_redeem(client, pid, "auditor1", key="tk1")
        # 逐事件签收(两次调用合并, 覆盖全部必须事件)
        seqs = sorted(gseqs)
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           key="rcpt1")
        assert r.status_code == 201, r.text
        j = r.json()
        assert j["receipt_type"] == "SIGNED"
        assert j["confirmed_count"] == len(seqs)
        assert j["total_required"] == len(seqs)
        assert j["progress"]["signed"] == 1
        assert j["progress"]["all_completed"] is True
        # 进度/回执详情: 每事件结果/说明/操作者/时间
        rep = progress(client, pid)
        a = next(x for x in rep["assignments"] if x["recipient"] == "auditor1")
        assert a["status"] == "SIGNED" and a["completed_at"]
        receipt = rep["receipts"][0]
        assert {e["global_seq"] for e in receipt["events"]} == set(seqs)
        assert all(e["result"] == "CONFIRMED" and e["operator"] == "auditor1"
                   and e["created_at"] for e in receipt["events"])

    def test_signed_with_partial_coverage_rejected(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        # 缺一个必须事件
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED",
                           events=signed_events(seqs[:-1]), key="r1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "required_events_missing"
        assert r.json()["detail"]["missing"] == [seqs[-1]]
        # SIGNED 但夹带 ANOMALY -> 拒绝
        evs = signed_events(seqs)
        evs[0]["result"] = "ANOMALY"
        evs[0]["note"] = "有疑问"
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=evs, key="r2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == \
            "signed_requires_all_confirmed"

    def test_scoped_recipient_signs_only_scope(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        sub = sorted(gseqs)[:1]
        add_recipient_assignment(client, pid, "auditor2", seqs=sub,
                                 key="a2")
        dl2, _ = issue_and_redeem(client, pid, "auditor2", key="tk2")
        # 只签范围内 1 个事件即可完成
        r = submit_receipt(client, pid, "auditor2", dl2, d,
                           rtype="SIGNED", events=signed_events(sub),
                           key="r2")
        assert r.status_code == 201, r.text
        # 但多报范围外包内事件 -> 拒绝
        register(client, "auditor3", key="rcp3")
        add_recipient_assignment(client, pid, "auditor3", seqs=sub,
                                 key="a3")
        dl3, _ = issue_and_redeem(client, pid, "auditor3", key="tk3")
        extra = sub + [g for g in sorted(gseqs) if g not in sub][:1]
        r = submit_receipt(client, pid, "auditor3", dl3, d,
                           rtype="SIGNED", events=signed_events(extra),
                           key="r3")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "event_outside_required_scope"


# ======================================================================
# ---------- 部分异常 / 拒收 ----------
# ======================================================================

class TestPartialAndReject:
    def test_partial_anomaly_records_reasons(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        evs = signed_events(seqs)
        evs[1] = {"global_seq": seqs[1], "result": "ANOMALY",
                  "note": "该事件载荷与现场记录不符"}
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="PARTIAL", events=evs,
                           note="一个事件存疑", key="rp")
        assert r.status_code == 201, r.text
        j = r.json()
        assert j["receipt_type"] == "PARTIAL"
        assert j["anomaly_count"] == 1 and j["confirmed_count"] == len(seqs) - 1
        # 整体进度展示异常原因与未完成接收方
        rep = progress(client, pid)
        p = rep["receipt_progress"]
        assert p["partial"] == 1 and p["anomaly_count"] == 1
        assert p["anomalies"][0]["global_seq"] == seqs[1]
        assert "载荷与现场记录不符" in p["anomalies"][0]["note"]
        # PARTIAL 也是该接收方的终态(全部必须事件已给结论)
        assert p["completed"] == 1 and p["all_completed"] is True

    def test_partial_requires_anomaly_and_notes(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        # 全部 CONFIRMED 却报 PARTIAL -> 拒绝
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="PARTIAL", events=signed_events(seqs),
                           key="p1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "partial_requires_anomaly"
        # ANOMALY 不带说明 -> 422 业务校验
        evs = signed_events(seqs)
        evs[0] = {"global_seq": seqs[0], "result": "ANOMALY"}
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="PARTIAL", events=evs, key="p2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "event_note_required"

    def test_reject_package_with_reason(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        # 无原因拒收 -> 拒绝
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="REJECTED", key="j1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "reject_note_required"
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="REJECTED", note="整包内容来源不可信, 拒收",
                           key="j2")
        assert r.status_code == 201, r.text
        rep = progress(client, pid)
        assert rep["receipt_progress"]["rejected"] == 1
        receipt = rep["receipts"][0]
        assert receipt["receipt_type"] == "REJECTED"
        assert "来源不可信" in receipt["note"]


# ======================================================================
# ---------- 身份/令牌/摘要/范围拒绝 ----------
# ======================================================================

class TestReceiptGuards:
    def test_non_assigned_recipient_forbidden(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        register(client, "mallory", key="rcp-mal")
        # mallory 未分派: 不能查看, 也不能走回执
        r = client.get(f"/api/admin/evidence/distributions/{pid}",
                       params={"operator": "mallory"})
        assert r.status_code == 403
        r = submit_receipt(client, pid, "mallory", "EDDx", d,
                           rtype="SIGNED", events=[], key="m1")
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "recipient_forbidden"

    def test_download_redemption_required_and_bound(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        seqs = sorted(gseqs)
        # 未兑换任何令牌 -> 拒绝
        r = submit_receipt(client, pid, "auditor1", "EDD-fake", d,
                           rtype="SIGNED", events=signed_events(seqs),
                           key="g1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "download_not_redeemed"
        # auditor2 拿自己的令牌, auditor1 不能拿来回执
        add_recipient_assignment(client, pid, "auditor2", key="a2")
        dl2, _ = issue_and_redeem(client, pid, "auditor2", key="tk2")
        r = submit_receipt(client, pid, "auditor1", dl2, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           key="g2")
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "download_redemption_mismatch"

    def test_stale_or_tampered_digests_rejected(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        bad = "0" * 64
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           mh=bad, key="h1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "manifest_hash_mismatch"
        assert r.json()["detail"]["expected"] == d["manifest_hash"]
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           cd=bad, key="h2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "content_digest_mismatch"

    def test_event_not_in_package_rejected(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        evs = signed_events(sorted(gseqs))
        evs.append({"global_seq": 424242, "result": "CONFIRMED"})
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=evs, key="e1")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "event_not_in_package"

    def test_invalid_type_and_result(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="WAIT", events=[], key="t1")
        assert r.status_code == 422
        evs = [{"global_seq": sorted(gseqs)[0], "result": "MAYBE"}]
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="PARTIAL", events=evs, key="t2")
        assert r.status_code == 422


# ======================================================================
# ---------- 幂等: 同键重放同一回执, 一次性提交 ----------
# ======================================================================

class TestReceiptIdempotency:
    def test_same_key_returns_same_receipt(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        a = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           key="same-key")
        assert a.status_code == 201
        b = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED", events=signed_events(seqs),
                           key="same-key")
        assert b.status_code == 201 and b.json()["replayed"] is True
        assert b.json()["receipt_id"] == a.json()["receipt_id"]
        # 回执落库只有一条
        rep = progress(client, pid)
        assert len(rep["receipts"]) == 1

    def test_different_key_duplicate_submission_rejected(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1")
        seqs = sorted(gseqs)
        r1 = submit_receipt(client, pid, "auditor1", dl_id, d,
                            rtype="SIGNED", events=signed_events(seqs),
                            key="key-1")
        assert r1.status_code == 201
        r2 = submit_receipt(client, pid, "auditor1", dl_id, d,
                            rtype="REJECTED", note="再拒一次", key="key-2")
        assert r2.status_code == 409
        assert r2.json()["detail"]["code"] == "receipt_already_submitted"


# ======================================================================
# ---------- 延期双人审批 ----------
# ======================================================================

class TestExtensionApproval:
    def _request(self, client, d, key="ext1", days=30):
        new_until = (datetime.utcnow() + timedelta(days=days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = client.post(
            f"/api/admin/evidence/distributions/{d['package_id']}/extensions",
            json={"operator": "admin", "idempotency_key": key,
                  "new_valid_until": new_until, "reason": "对方流程延误"})
        return r

    def test_two_distinct_approvers_apply_extension(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        # auditor1 也有自己的较早回执截止(默认=valid_until), 先给它单独设更早 due
        early = (datetime.utcnow() + timedelta(minutes=30)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = self._request(client, d)
        assert r.status_code == 201, r.text
        ext = r.json()
        eid = ext["extension_id"]
        assert ext["status"] == "PENDING" and ext["required"] == 2
        # 申请人不能自审
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "admin", "idempotency_key": "ap0"})
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "approver_is_requester"
        # 第一名审批
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "bob", "idempotency_key": "ap1"})
        assert rr.status_code == 200, rr.text
        assert rr.json()["applied"] is False and rr.json()["approvals"] == 1
        # 同一人重复审批(不同键) -> 并发去重拒绝
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "bob", "idempotency_key": "ap1b"})
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "approver_already_decided"
        # 同一键重放 -> 幂等回显
        rr2 = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                          json={"operator": "bob", "idempotency_key": "ap1"})
        assert rr2.json()["replayed"] is True
        # 第二名不同操作者 -> 原子应用
        old_until = d["valid_until"]
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "carol", "idempotency_key": "ap2"})
        assert rr.status_code == 200, rr.text
        j = rr.json()
        assert j["applied"] is True and j["extension_status"] == "APPLIED"
        assert j["valid_until"] != old_until
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["valid_until"] != old_until
        # 已应用的申请不再接受审批
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "dave", "idempotency_key": "ap3"})
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "extension_not_pending"
        # 未完成分派的回执截止随包顺延
        a = next(x for x in detail["assignments"]
                 if x["recipient"] == "auditor1")
        assert a["receipt_due_at"] == detail["valid_until"]

    def test_reject_extension_closes_request(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        r = self._request(client, d, key="ext-r")
        eid = r.json()["extension_id"]
        # 先一票赞成, 再被另一人拒绝 -> 赞成票终态化
        client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                    json={"operator": "bob", "idempotency_key": "ap1"})
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/reject",
                         json={"operator": "carol", "idempotency_key": "rj1",
                               "reason": "理由不充分"})
        assert rr.status_code == 200, rr.text
        assert rr.json()["status"] == "REJECTED"
        ext = client.get(
            f"/api/admin/evidence/extensions/{eid}").json()
        assert ext["status"] == "REJECTED"
        approvals = {a["operator"]: a["decision"] for a in ext["approvals"]}
        assert approvals == {"bob": "SUPERSEDED", "carol": "REJECTED"}
        # 包有效期未变
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["valid_until"] == d["valid_until"]

    def test_only_one_pending_extension(self, client, package):
        rv, gseqs, d = package
        self._request(client, d, key="ext-a")
        r = self._request(client, d, key="ext-b", days=40)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "extension_already_pending"

    def test_extension_must_be_before_due_and_longer(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        # 新有效期不晚于当前 -> 拒绝
        soon = (datetime.utcnow() + timedelta(seconds=60)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = client.post(
            f"/api/admin/evidence/distributions/{pid}/extensions",
            json={"operator": "admin", "idempotency_key": "ex1",
                  "new_valid_until": soon})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "invalid_new_valid_until"
        # 到期后不能申请延期(只能恢复)
        set_valid_until_past(client, pid)
        r = self._request(client, d, key="ex2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "extension_window_closed"

    def test_approval_invalidated_on_revoke(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        r = self._request(client, d, key="ext-iv")
        eid = r.json()["extension_id"]
        client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                    json={"operator": "bob", "idempotency_key": "ap1"})
        # 审批期间包被撤销: 审批中的延期申请自动失效
        rv_revoke = revoke(client, pid, key="rv1")
        assert rv_revoke.json()["invalidated_extensions"] == 1
        ext = client.get(
            f"/api/admin/evidence/extensions/{eid}").json()
        assert ext["status"] == "INVALIDATED"
        assert ext["invalidated_reason"] == "package_status_changed"
        assert next(a for a in ext["approvals"]
                    if a["operator"] == "bob")["decision"] == "INVALIDATED"
        # 失效后的申请不再接受审批
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "carol", "idempotency_key": "ap2"})
        assert rr.status_code == 409
        assert rr.json()["detail"]["code"] == "extension_not_pending"

    def test_approval_invalidated_on_digest_change(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        r = self._request(client, d, key="ext-dg")
        eid = r.json()["extension_id"]
        client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                    json={"operator": "bob", "idempotency_key": "ap1"})
        # 审批期间库内摘要被篡改(模拟"摘要变化")
        db = SessionLocal()
        try:
            row = db.get(distribution.EvidenceDistribution, pid)
            row.manifest_hash = "f" * 64
            db.commit()
        finally:
            db.close()
        rr = client.post(f"/api/admin/evidence/extensions/{eid}/approve",
                         json={"operator": "carol", "idempotency_key": "ap2"})
        assert rr.status_code == 409
        assert rr.json()["detail"]["reason"] == "digest_changed"
        assert client.get(
            f"/api/admin/evidence/extensions/{eid}").json()["status"] \
            == "INVALIDATED"


# ======================================================================
# ---------- 到期待处理 / 禁止下载 / 恢复 / 撤销只读 / 重新签发 ----------
# ======================================================================

class TestLifecycle:
    def test_due_package_moves_to_pending_and_blocks(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        add_recipient_assignment(client, pid, "auditor2", key="a2")
        # auditor2 未回执; auditor1 完成签收
        dl1, _ = issue_and_redeem(client, pid, "auditor1", key="tk1")
        r = submit_receipt(client, pid, "auditor1", dl1, d,
                           rtype="SIGNED",
                           events=signed_events(sorted(gseqs)), key="r1")
        assert r.status_code == 201
        set_valid_until_past(client, pid)
        r = sweep(client, key="sw1")
        assert r.status_code == 200, r.text
        assert r.json()["swept"] == 1 and pid in r.json()["transitioned"]
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["status"] == "PENDING_PROCESS"
        assert detail["download_allowed"] is False
        assert detail["receipt_progress"]["overdue"] == 1
        overdue = next(x for x in detail["assignments"]
                       if x["recipient"] == "auditor2")
        assert overdue["status"] == "OVERDUE"
        # 禁止继续下载(签发与兑换都拒绝)
        tok = client.post(
            f"/api/admin/evidence/distributions/{pid}/download-token",
            json={"operator": "auditor2", "idempotency_key": "tk2"})
        assert tok.status_code == 409
        assert tok.json()["detail"]["code"] == "package_pending_process"
        # 手工塞一个未用令牌, 兑换同样被拒
        db = SessionLocal()
        try:
            import hashlib
            import uuid
            raw = uuid.uuid4().hex
            db.add(distribution.EvidenceDistributionDownload(
                id="EDDx" + uuid.uuid4().hex[:8],
                token_hash=hashlib.sha256(raw.encode()).hexdigest(),
                distribution_id=pid, bound_recipient="auditor2",
                issued_by="admin",
                expires_at=evidence.now_utc_naive() + timedelta(hours=1)))
            db.commit()
            old_token = raw
        finally:
            db.close()
        rd = client.get(
            f"/api/admin/evidence/distribution-downloads/{old_token}",
            params={"operator": "auditor2"})
        assert rd.status_code == 409
        assert rd.json()["detail"]["code"] == "package_pending_process"
        # 待处理期间不能再提交回执
        r = submit_receipt(client, pid, "auditor2",
                           dl1, d, rtype="REJECTED", note="迟交", key="late")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_pending_process"
        # 已提交回执保持只读可查
        rep = progress(client, pid)
        signed = next(x for x in rep["receipts"]
                      if x["recipient"] == "auditor1")
        assert signed["receipt_type"] == "SIGNED"
        # 原始归档摘要不变
        rv_after = client.get(
            f"/api/admin/evidence/reviews/{rv['review_id']}").json()
        assert rv_after["signature_hash"] == rv["signature_hash"]

    def test_all_signed_due_package_does_not_become_pending(self, client,
                                                            package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl1, _ = issue_and_redeem(client, pid, "auditor1", key="tk1")
        submit_receipt(client, pid, "auditor1", dl1, d,
                       rtype="SIGNED",
                       events=signed_events(sorted(gseqs)), key="r1")
        set_valid_until_past(client, pid)
        r = sweep(client, key="sw1")
        assert r.json()["swept"] == 0
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        # 全部按时回执: 不进待处理(派生为已过期)
        assert detail["status"] == "EXPIRED"

    def test_recover_requires_verification_then_reopens(self, client, package):
        rv, gseqs, d = package
        pid = d["package_id"]
        set_valid_until_past(client, pid)
        sweep(client, key="sw1")
        # 非待处理状态不能恢复(先确认当前确为待处理)
        # 恢复: 重新校验通过(留存包完好) + 续期
        r = client.post(
            f"/api/admin/evidence/distributions/{pid}/recover",
            json={"operator": "admin", "idempotency_key": "rc1",
                  "extend_seconds": 3600, "reason": "已联系接收方"})
        assert r.status_code == 200, r.text
        j = r.json()
        assert j["status"] == "RECOVERED" and j["valid_until"]
        assert "auditor1" in j["reopened_recipients"]
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["status"] == "RECOVERED"
        assert detail["download_allowed"] is True
        # 重开的接收方可继续下载与回执
        dl1, _ = issue_and_redeem(client, pid, "auditor1", key="tk2")
        r = submit_receipt(client, pid, "auditor1", dl1, d,
                           rtype="SIGNED",
                           events=signed_events(sorted(gseqs)), key="r2")
        assert r.status_code == 201, r.text

    def test_recover_blocked_when_verification_fails(self, client, package):
        import glob
        rv, gseqs, d = package
        pid = d["package_id"]
        set_valid_until_past(client, pid)
        sweep(client, key="sw1")
        # 篡改磁盘留存包 -> 恢复必须被拒绝(确定性: 截断尾部必然破坏 zip/中央目录)
        path = os.path.join(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"],
                            pid, f"distribution-{pid}.zip")
        assert glob.glob(path)
        with open(path, "rb") as f:
            blob = f.read()
        with open(path, "wb") as f:
            f.write(blob[:-200])
        r = client.post(
            f"/api/admin/evidence/distributions/{pid}/recover",
            json={"operator": "admin", "idempotency_key": "rc1",
                  "extend_seconds": 3600})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "recovery_verification_failed"
        assert r.json()["detail"]["tampering"]
        # 仍停留在待处理, 下载继续禁止
        detail = client.get(
            f"/api/admin/evidence/distributions/{pid}",
            params={"operator": "admin"}).json()
        assert detail["status"] == "PENDING_PROCESS"

    def test_revoke_keeps_receipts_readonly_and_blocks_new(self, client,
                                                           package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl1, _ = issue_and_redeem(client, pid, "auditor1", key="tk1")
        submit_receipt(client, pid, "auditor1", dl1, d,
                       rtype="SIGNED",
                       events=signed_events(sorted(gseqs)), key="r1")
        revoke(client, pid, key="rv1")
        # 已提交回执只读可查
        rep = progress(client, pid)
        assert rep["status"] == "REVOKED"
        assert rep["receipts"][0]["receipt_type"] == "SIGNED"
        # 撤销后不能再回执/下载
        r = submit_receipt(client, pid, "auditor1", dl1, d,
                           rtype="REJECTED", note="x", key="r2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_revoked"
        tok = client.post(
            f"/api/admin/evidence/distributions/{pid}/download-token",
            json={"operator": "auditor1", "idempotency_key": "tkx"})
        assert tok.status_code == 409
        # 撤销不影响离线密码学校验入口
        assert client.post(
            "/api/admin/evidence/distributions/verify",
            content=_read_stored_zip(pid),
            headers={"Content-Type": "application/zip"}).json()[
            "valid"] is True

    def test_reissue_after_pending_creates_new_cycle(self, client, package):
        rv, gseqs, d = package
        old_pid = d["package_id"]
        # 第二个接收方 auditor2 始终不回执(到期后进入 OVERDUE)
        add_recipient_assignment(client, old_pid, "auditor2", key="a2")
        dl1, _ = issue_and_redeem(client, old_pid, "auditor1", key="tk1")
        submit_receipt(client, old_pid, "auditor1", dl1, d,
                       rtype="PARTIAL",
                       events=[{**{"global_seq": gs, "result": "CONFIRMED"}}
                               for gs in sorted(gseqs)[:-1]]
                        + [{"global_seq": sorted(gseqs)[-1],
                            "result": "ANOMALY", "note": "异常"}],
                       key="r1")
        set_valid_until_past(client, old_pid)
        sweep(client, key="sw1")
        assert client.get(
            f"/api/admin/evidence/distributions/{old_pid}",
            params={"operator": "admin"}).json()["status"] == "PENDING_PROCESS"
        # 重新签发: 新 package_id + 新回执周期, 旧包保留只读
        d2 = create_dist(client, rv["review_id"], key="d2")
        assert d2["package_id"] != old_pid
        assert d2["issue_no"] == 2 and d2["reissued"] is True
        rep = progress(client, d2["package_id"])
        assert rep["receipt_progress"]["recipient_count"] == 1
        assert rep["receipt_progress"]["unfinished"] == 1
        # 旧包仍是待处理且回执保留
        old = client.get(
            f"/api/admin/evidence/distributions/{old_pid}",
            params={"operator": "admin"}).json()
        assert old["status"] == "PENDING_PROCESS"
        assert old["receipt_progress"]["partial"] == 1

    def test_reissue_after_revoke_new_cycle(self, client, package):
        rv, gseqs, d = package
        old_pid = d["package_id"]
        revoke(client, old_pid, key="rv1")
        d2 = create_dist(client, rv["review_id"], key="d2")
        assert d2["package_id"] != old_pid and d2["issue_no"] == 2
        rep = progress(client, d2["package_id"])
        assert rep["assignments"][0]["status"] == "PENDING"


def _read_stored_zip(pid):
    path = os.path.join(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"],
                        pid, f"distribution-{pid}.zip")
    with open(path, "rb") as f:
        return f.read()


# ======================================================================
# ---------- 非回归: 离线校验/篡改定位/证据分页导出/复核归档 ----------
# ======================================================================

class TestNoRegression:
    def test_offline_verify_and_tamper_location_still_works(self, client,
                                                            package):
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, raw = issue_and_redeem(client, pid, "auditor1", key="tk1")
        rep = client.post(
            "/api/admin/evidence/distributions/verify", content=raw,
            headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is True and rep["matches_server_record"] is True
        # 篡改一行 -> 定位到 seq
        src = zipfile.ZipFile(io.BytesIO(raw))
        files = {n: src.read(n) for n in src.namelist()}
        lines = files["events.jsonl"].decode().splitlines()
        ev = json.loads(lines[0])
        ev["event_type"] = "EVIL"
        lines[0] = json.dumps(ev, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"))
        files["events.jsonl"] = ("\n".join(lines) + "\n").encode()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in sorted(files):
                zf.writestr(name, files[name])
        evil = buf.getvalue()
        rep = client.post(
            "/api/admin/evidence/distributions/verify", content=evil,
            headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is False
        codes = {t["code"] for t in rep["tampering"]}
        assert {"line_digest_mismatch", "content_digest_mismatch",
                "signature_digest_mismatch"} <= codes

    def test_full_evidence_chain_alongside_receipts(self, client, package):
        # 已有完整 计划->会话->导出->复核归档->分发 链路; 回执后状态汇总仍齐全
        rv, gseqs, d = package
        pid = d["package_id"]
        dl_id, _ = issue_and_redeem(client, pid, "auditor1", key="tk1")
        r = submit_receipt(client, pid, "auditor1", dl_id, d,
                           rtype="SIGNED",
                           events=signed_events(sorted(gseqs)), key="r1")
        assert r.status_code == 201
        st = client.get("/api/status").json()
        for k in ("evidence_sessions", "evidence_exports", "evidence_reviews",
                  "evidence_distributions", "evidence_recipients",
                  "compensations"):
            assert k in st
        row = st["evidence_distributions"][0]
        assert row["receipt_progress"]["signed"] == 1
        # 复核归档接口仍可查
        assert client.get(
            f"/api/admin/evidence/reviews/{rv['review_id']}").json()[
            "status"] == "ARCHIVED"
