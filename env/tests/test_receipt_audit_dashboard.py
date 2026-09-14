"""回执审计看板模块测试(从零实现, 独立于既有争议工作流测试)。

覆盖:
- 创建固定时点查询: 按分发包/接收方/状态/时间范围/条目类型过滤; 包汇总
  (完成率/异常数量/待处理争议/最近事件); 空结果显式响应;
- 固定查询时点: 查询创建后新增回执/争议事件不插入已开始的分页结果,
  争议状态流转也不改变快照条目;
- 严格顺序游标: 正常逐页、稳定顺序、游标跳页/重复使用/无游标重取/跨查询/
  条件变化(伪造)/损坏签名全部拒绝; 末页关闭;
- CSV 导出: 与同查询分页结果行序/内容一致(逐行比对)、空结果只含表头、
  幂等(同键重放返回同一文件/digest)、异键重复导出字节一致、下载计数与文件;
- 非法时间范围 / 未知接收方 / 未知分发包 / 非法状态类型 的明确响应;
- 操作日志: 查询创建/翻页/被拒翻页/导出/下载全部留痕。
"""
import csv
import io
import os
import tempfile

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp(prefix="receipt-audit-test-")
_DB_PATH = f"{_tmp}/test.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["EVIDENCE_STORE_DIR"] = f"{_tmp}/store"
os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"] = f"{_tmp}/dist-store"
os.environ["RECEIPT_AUDIT_STORE_DIR"] = f"{_tmp}/audit-store"

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app import distribution, evidence, receiptaudit  # noqa: E402


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    for d in (os.environ["EVIDENCE_STORE_DIR"],
              os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"],
              os.environ["RECEIPT_AUDIT_STORE_DIR"]):
        os.makedirs(d, exist_ok=True)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建(完整证据 -> 归档复核 -> 分发 -> 回执链路) ----------

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


def complete_plan(client, pid, ticks=6):
    from app import plans
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"s-{pid}"})
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
    complete_plan(client, pid)
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


def register(client, recipient, key=None, **extra):
    body = {"operator": "admin",
            "idempotency_key": key or f"rcp-{recipient}",
            "recipient": recipient, **extra}
    r = client.post("/api/admin/evidence/recipients", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def create_dist(client, rid, recipient, key, ttl=3600):
    r = client.post("/api/admin/evidence/distributions", json={
        "operator": "admin", "idempotency_key": key, "review_id": rid,
        "recipient": recipient, "redaction_policy": "STANDARD",
        "ttl_seconds": ttl})
    assert r.status_code == 201, r.text
    return r.json()


def issue_and_redeem(client, pid, recipient, key):
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/download-token",
        json={"operator": recipient, "idempotency_key": key})
    assert r.status_code == 201, r.text
    j = r.json()
    rd = client.get(
        f"/api/admin/evidence/distribution-downloads/{j['token']}",
        params={"operator": recipient})
    assert rd.status_code == 200, rd.text
    return j["download_id"]


def submit_receipt(client, pid, recipient, download_id, d, seqs, key,
                   receipt_type="SIGNED", anomaly_idx=None):
    evs = [{"global_seq": gs, "result": "CONFIRMED"} for gs in seqs]
    if anomaly_idx is not None:
        evs[anomaly_idx] = {"global_seq": seqs[anomaly_idx],
                            "result": "ANOMALY",
                            "note": "该事件载荷与现场记录不符"}
    body = {"operator": recipient, "idempotency_key": key,
            "download_id": download_id, "manifest_hash": d["manifest_hash"],
            "content_digest": d["content_digest"],
            "receipt_type": receipt_type, "events": evs}
    if receipt_type == "REJECTED":
        body["note"] = "整包来源不可信, 拒收"
        body.pop("events")
    elif anomaly_idx is not None:
        body["note"] = "一个事件存疑"
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/receipts", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def get_receipt_id(client, pid, recipient):
    rep = client.get(
        f"/api/admin/evidence/distributions/{pid}/receipts",
        params={"operator": "admin"}).json()
    a = next(x for x in rep["assignments"] if x["recipient"] == recipient)
    return a["receipt"]["receipt_id"]


def assign_recipient(client, pid, recipient, key, due_seconds=3600):
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/recipients", json={
            "operator": "admin", "idempotency_key": key,
            "recipient": recipient})
    assert r.status_code == 201, r.text
    return r.json()


def open_dispute(client, receipt_id, key, *, operator="admin", **kw):
    return client.post(
        f"/api/admin/evidence/receipts/{receipt_id}/dispute",
        json={"operator": operator, "idempotency_key": key, **kw})


def assign_dispute(client, did, key, *, operator="admin", assignee="handler1",
                   opinion="请核对异常事件", supp="补充现场记录摘要"):
    return client.post(f"/api/admin/evidence/disputes/{did}/assign", json={
        "operator": operator, "idempotency_key": key, "assignee": assignee,
        "handling_opinion": opinion, "supplementary_evidence": supp})


def resolve_dispute(client, did, key, *, operator="handler1",
                    resolution="核查属实, 已更正"):
    return client.post(f"/api/admin/evidence/disputes/{did}/resolve", json={
        "operator": operator, "idempotency_key": key,
        "resolution": resolution})


def close_dispute(client, did, key, *, operator="admin"):
    return client.post(f"/api/admin/evidence/disputes/{did}/close", json={
        "operator": operator, "idempotency_key": key})


# ---------- 看板查询封装 ----------

BASE = "/api/admin/evidence/receipt-audit"


def create_query(client, key_op="admin", **params):
    body = {"operator": params.pop("operator", key_op), **params}
    return client.post(f"{BASE}/queries", json=body)


def next_page(client, qid, cursor=None, limit=None, operator="admin"):
    body = {"operator": operator}
    if cursor is not None:
        body["cursor"] = cursor
    if limit is not None:
        body["limit"] = limit
    return client.post(f"{BASE}/queries/{qid}/pages", json=body)


def fetch_all(client, qid, limit=2):
    """逐页取完整个查询, 返回 (全部 items, 每页响应)。"""
    cursor, items, pages = None, [], []
    while True:
        r = next_page(client, qid, cursor=cursor, limit=limit)
        assert r.status_code == 200, r.text
        j = r.json()
        pages.append(j)
        items.extend(j["items"])
        if not j["cursor"]["has_more"]:
            return items, pages
        cursor = j["cursor"]["next_cursor"]


def make_export(client, qid, key, operator="admin"):
    return client.post(f"{BASE}/queries/{qid}/exports",
                       json={"operator": operator, "idempotency_key": key})


def csv_rows(content: bytes):
    text = content.decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


# ---------- 多接收方/多包夹具 ----------

@pytest.fixture()
def world(client):
    """两个分发包:
      pkg1: auditor1 签收 SIGNED; auditor2 提交 PARTIAL(1 个异常)并打开争议
            -> ASSIGNED -> RESOLVED -> CLOSED; auditor3 未回执(PENDING)
      pkg2: auditor4 拒收 REJECTED
    返回各 id 与 gseqs。
    """
    rv1, gseqs1 = make_archived_review(client, key="base1", start=1, end=3)
    register(client, "auditor1", key="rcp-a1")
    register(client, "auditor2", key="rcp-a2")
    register(client, "auditor3", key="rcp-a3")
    register(client, "handler1", key="rcp-h1")
    d1 = create_dist(client, rv1["review_id"], "auditor1", key="dist1")
    pkg1 = d1["package_id"]
    dl1 = issue_and_redeem(client, pkg1, "auditor1", key="tk1")
    submit_receipt(client, pkg1, "auditor1", dl1, d1, sorted(gseqs1),
                   key="rcpt1")
    assign_recipient(client, pkg1, "auditor2", key="asg2")
    dl2 = issue_and_redeem(client, pkg1, "auditor2", key="tk2")
    submit_receipt(client, pkg1, "auditor2", dl2, d1, sorted(gseqs1),
                   key="rcpt2", receipt_type="PARTIAL", anomaly_idx=0)
    assign_recipient(client, pkg1, "auditor3", key="asg3")  # 不回执
    rid2 = get_receipt_id(client, pkg1, "auditor2")
    op = open_dispute(client, rid2, key="do1")
    assert op.status_code == 201, op.text
    did = op.json()["dispute_id"]
    assert assign_dispute(client, did, key="da1").status_code == 200
    assert resolve_dispute(client, did, key="dr1").status_code == 200
    assert close_dispute(client, did, key="dc1").status_code == 200

    rv2, gseqs2 = make_archived_review(client, key="base2", start=11, end=13)
    d2 = create_dist(client, rv2["review_id"], "auditor1", key="dist2",
                     ttl=3600)
    pkg2 = d2["package_id"]
    # pkg2 主接收方 auditor1 拒收; 另注册 auditor4 无数据
    register(client, "auditor4", key="rcp-a4")
    assign_recipient(client, pkg2, "auditor4", key="asg4")
    dl4 = issue_and_redeem(client, pkg2, "auditor4", key="tk4")
    submit_receipt(client, pkg2, "auditor4", dl4, d2, sorted(gseqs2),
                   key="rcpt4", receipt_type="REJECTED")
    return {"pkg1": pkg1, "pkg2": pkg2, "dispute_id": did,
            "partial_receipt_id": rid2,
            "gseqs1": sorted(gseqs1), "gseqs2": sorted(gseqs2)}


# ======================================================================
# ---------- 创建查询 / 过滤 / 包汇总 / 空结果 ----------
# ======================================================================

class TestQueryCreate:
    def test_unfiltered_query_lists_all_events_sorted(self, client, world):
        r = create_query(client)
        assert r.status_code == 201, r.text
        j = r.json()
        # 3 回执 + 4 争议生命周期事件(open/assign/resolve/close) = 7
        assert j["total_items"] == 7
        assert j["empty"] is False
        items, _ = fetch_all(client, j["query_id"], limit=3)
        kinds_ts = [(x["event_ts"], x["kind"], x.get("receipt_id")
                     or x.get("dispute_event")) for x in items]
        assert kinds_ts == sorted(kinds_ts, key=lambda x: x[0])
        assert {x["kind"] for x in items} == {"RECEIPT", "DISPUTE_EVENT"}

    def test_filter_by_package(self, client, world):
        r = create_query(client, package_id=world["pkg2"])
        assert r.status_code == 201, r.text
        j = r.json()
        assert j["total_items"] == 1  # 只有 REJECTED 回执, 无争议
        items, _ = fetch_all(client, j["query_id"])
        assert len(items) == 1
        assert items[0]["kind"] == "RECEIPT"
        assert items[0]["receipt_type"] == "REJECTED"
        assert all(x["package_id"] == world["pkg2"] for x in items)

    def test_filter_by_recipient(self, client, world):
        r = create_query(client, recipient="auditor4")
        j = r.json()
        assert j["total_items"] == 1
        items, _ = fetch_all(client, j["query_id"])
        assert items[0]["recipient"] == "auditor4"

    def test_filter_status_receipt_type(self, client, world):
        r = create_query(client, status=["PARTIAL"])
        items, _ = fetch_all(client, r.json()["query_id"])
        assert len(items) == 1 and items[0]["status"] == "PARTIAL"

        r = create_query(client, status=["SIGNED"])
        items, _ = fetch_all(client, r.json()["query_id"])
        assert len(items) == 1 and items[0]["status"] == "SIGNED"

        r = create_query(client, status=["SIGNED", "REJECTED"])
        items, _ = fetch_all(client, r.json()["query_id"])
        assert {x["status"] for x in items} == {"SIGNED", "REJECTED"}

    def test_filter_status_dispute(self, client, world):
        # CLOSED 过滤: 关闭事件(to=CLOSED)命中
        r = create_query(client, status=["CLOSED"])
        items, _ = fetch_all(client, r.json()["query_id"])
        events = [(x["dispute_event"], x["to_status"]) for x in items]
        assert ("dispute.close", "CLOSED") in events
        assert all(x["kind"] == "DISPUTE_EVENT" for x in items)

    def test_filter_kind(self, client, world):
        r = create_query(client, kinds=["RECEIPT"])
        items, _ = fetch_all(client, r.json()["query_id"])
        assert len(items) == 3
        assert all(x["kind"] == "RECEIPT" for x in items)

        r = create_query(client, kinds=["DISPUTE_EVENT"])
        items, _ = fetch_all(client, r.json()["query_id"])
        assert len(items) == 4
        assert all(x["kind"] == "DISPUTE_EVENT" for x in items)

    def test_time_range_filters(self, client, world):
        full = create_query(client).json()
        items, _ = fetch_all(client, full["query_id"], limit=100)
        first_ts, last_ts = items[0]["event_ts"], items[-1]["event_ts"]
        # 远未来起点 -> 明确空结果
        r2 = create_query(client, start_ts="2030-01-01T00:00:00Z")
        assert r2.status_code == 201
        assert r2.json()["total_items"] == 0
        # 覆盖全部时刻的闭区间包含全部条目
        r3 = create_query(client, start_ts=first_ts, end_ts=last_ts)
        assert r3.json()["total_items"] == full["total_items"]
        # 只覆盖最后一条时刻 -> 1 条
        r4 = create_query(client, start_ts=last_ts, end_ts=last_ts)
        assert r4.json()["total_items"] == 1

    def test_empty_result_explicit_response(self, client, world):
        # auditor4 与 pkg1 无任何关系: 事件流为空(显式指定的包卡片仍按
        # 接收方范围聚合, 接收方数为 0, 另见 card_present_... 用例)
        r = create_query(client, package_id=world["pkg1"],
                         recipient="auditor4")
        assert r.status_code == 201
        j = r.json()
        assert j["empty"] is True and j["total_items"] == 0
        pr = next_page(client, j["query_id"])
        assert pr.status_code == 200
        pj = pr.json()
        assert pj["items"] == [] and pj["empty"] is True
        assert pj["cursor"]["has_more"] is False
        assert pj["cursor"]["next_cursor"] is None
        assert pj["query_status"] == "CLOSED"

    def test_empty_feed_but_card_shown_within_scope(self, client, world):
        # 远未来时间窗口内没有事件, 但卡片仍展示范围内每个包(完成率/待处理争议)
        r = create_query(client, start_ts="2030-01-01T00:00:00Z")
        j = r.json()
        assert j["total_items"] == 0
        pids = {p["package_id"] for p in j["packages"]}
        assert {world["pkg1"], world["pkg2"]} <= pids

    def test_card_present_for_explicit_package_without_recipient_events(
            self, client, world):
        # pkg1 + auditor4: 该接收方在 pkg1 无分派 -> 事件为空, 但显式指定的
        # 包仍有卡片(接收方范围内 0 接收方)
        r = create_query(client, package_id=world["pkg1"],
                         recipient="auditor4")
        j = r.json()
        assert j["total_items"] == 0
        assert len(j["packages"]) == 1
        card = j["packages"][0]
        assert card["package_id"] == world["pkg1"]
        assert card["total_recipients"] == 0

    def test_invalid_time_range_rejected(self, client, world):
        r = create_query(client, start_ts="2026-09-10T10:00:00Z",
                         end_ts="2026-09-01T10:00:00Z")
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "invalid_time_range"

    def test_unknown_recipient_rejected(self, client, world):
        r = create_query(client, recipient="nobody")
        assert r.status_code == 404
        assert r.json()["detail"]["code"] == "recipient_unknown"

    def test_malformed_recipient_rejected(self, client, world):
        r = create_query(client, recipient="bad recipient!")
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "invalid_recipient"

    def test_unknown_package_rejected(self, client, world):
        r = create_query(client, package_id="EDP0000000000000000000000")
        assert r.status_code == 404
        assert r.json()["detail"]["code"] == "package_not_found"

    def test_invalid_status_and_kind(self, client, world):
        r = create_query(client, status=["BOGUS"])
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "invalid_status"
        r = create_query(client, kinds=["WAT"])
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "invalid_kind"

    def test_package_summary_cards(self, client, world):
        j = create_query(client).json()
        cards = {p["package_id"]: p for p in j["packages"]}
        c1 = cards[world["pkg1"]]
        # 3 个接收方: SIGNED + PARTIAL + PENDING -> 完成率 2/3
        assert c1["total_recipients"] == 3
        assert c1["completed_receipts"] == 2
        assert c1["completion_rate"] == round(2 / 3, 4)
        assert c1["signed"] == 1 and c1["partial"] == 1
        assert c1["pending"] == 1
        # 异常: PARTIAL 回执 1 张, 逐事件异常 1 条
        assert c1["anomaly_receipt_count"] == 1
        assert c1["anomaly_event_count"] == 1
        # 争议已关闭 -> 待处理 0, 总数 1
        assert c1["dispute_count"] == 1
        assert c1["open_dispute_count"] == 0
        assert len(c1["recent_events"]) <= 5
        assert c1["recent_events"]  # 非空
        c2 = cards[world["pkg2"]]
        # pkg2: 主接收方 auditor1 未回执(PENDING) + auditor4 拒收 -> 完成率 1/2
        assert c2["rejected"] == 1 and c2["pending"] == 1
        assert c2["completion_rate"] == 0.5
        assert c2["anomaly_receipt_count"] == 1
        assert c2["dispute_count"] == 0

    def test_summary_scoped_by_recipient_filter(self, client, world):
        # auditor1 视角: pkg1 只有 SIGNED(完成率 100%), pkg2 无其回执
        j = create_query(client, recipient="auditor1").json()
        cards = {p["package_id"]: p for p in j["packages"]}
        assert world["pkg1"] in cards
        c1 = cards[world["pkg1"]]
        assert c1["total_recipients"] == 1
        assert c1["completed_receipts"] == 1
        assert c1["completion_rate"] == 1.0
        assert c1["anomaly_receipt_count"] == 0

    def test_open_dispute_count(self, client, world):
        # 打开第二张争议(pkg2 的 REJECTED)不指派 -> OPEN, 待处理 +1
        rep = client.get(
            f"/api/admin/evidence/distributions/{world['pkg2']}/receipts",
            params={"operator": "admin"}).json()
        rid = next(x for x in rep["assignments"]
                   if x["recipient"] == "auditor4")["receipt"]["receipt_id"]
        r = open_dispute(client, rid, key="do2")
        assert r.status_code == 201, r.text
        j = create_query(client, package_id=world["pkg2"]).json()
        c = j["packages"][0]
        assert c["open_dispute_count"] == 1
        assert c["dispute_count"] == 1


# ======================================================================
# ---------- 固定查询时点(新事件不插入已开始结果; 状态快照不变) ----------
# ======================================================================

class TestFixedPointInTime:
    def test_new_receipt_after_create_not_inserted(self, client, world):
        q = create_query(client, package_id=world["pkg1"]).json()
        assert q["total_items"] == 6  # 2 回执 + 4 争议事件
        # 之后给 pkg1 的 auditor3 补一张 SIGNED 回执
        dl3 = issue_and_redeem(client, world["pkg1"], "auditor3", key="tk3")
        d = client.get(
            f"/api/admin/evidence/distributions/{world['pkg1']}",
            params={"operator": "admin"}).json()
        submit_receipt(client, world["pkg1"], "auditor3", dl3, d,
                       world["gseqs1"], key="rcpt3")
        items, pages = fetch_all(client, q["query_id"], limit=2)
        assert len(items) == 6  # 新回执没有插入
        assert all(x["recipient"] != "auditor3" for x in items)
        # 包汇总同样停留在固定时点(pending 仍为 1, 完成率仍 2/3)
        detail = client.get(
            f"{BASE}/queries/{q['query_id']}").json()
        card = next(p for p in detail["packages"]
                    if p["package_id"] == world["pkg1"])
        assert card["pending"] == 1
        assert card["completed_receipts"] == 2
        # 新查询能看到新回执
        q2 = create_query(client, package_id=world["pkg1"]).json()
        assert q2["total_items"] == 7

    def test_new_dispute_event_not_inserted_and_status_frozen(self, client,
                                                              world):
        # pkg2 REJECTED 打开争议但停留 OPEN, 建查询
        rep = client.get(
            f"/api/admin/evidence/distributions/{world['pkg2']}/receipts",
            params={"operator": "admin"}).json()
        rid = next(x for x in rep["assignments"]
                   if x["recipient"] == "auditor4")["receipt"]["receipt_id"]
        assert open_dispute(client, rid, key="do2").status_code == 201
        q = create_query(client, package_id=world["pkg2"]).json()
        # 1 回执 + 1 打开事件
        assert q["total_items"] == 2
        # 之后指派/处理/关闭
        did = open_dispute(client, rid, key="do2").json()["dispute_id"]
        assert assign_dispute(client, did, key="da2").status_code == 200
        assert resolve_dispute(client, did, key="dr2").status_code == 200
        assert close_dispute(client, did, key="dc2",
                             operator="admin").status_code == 200
        items, _ = fetch_all(client, q["query_id"], limit=5)
        # 快照里仍只有 open 一个争议事件, 且打开事件的状态副本不变
        dep = [x for x in items if x["kind"] == "DISPUTE_EVENT"]
        assert len(dep) == 1
        assert dep[0]["dispute_event"] == "dispute.open"
        assert dep[0]["status"] == "OPEN"
        # 固定时点包汇总: open_dispute_count=1
        detail = client.get(f"{BASE}/queries/{q['query_id']}").json()
        assert detail["packages"][0]["open_dispute_count"] == 1

    def test_card_dispute_status_frozen_after_close(self, client, world):
        # pkg2 REJECTED -> 打开争议停留 OPEN, 建查询(卡片 open=1),
        # 之后走完指派/结论/关闭; 固定时点卡片的待处理争议仍为 1
        rep = client.get(
            f"/api/admin/evidence/distributions/{world['pkg2']}/receipts",
            params={"operator": "admin"}).json()
        rid = next(x for x in rep["assignments"]
                   if x["recipient"] == "auditor4")["receipt"]["receipt_id"]
        r = open_dispute(client, rid, key="do2")
        assert r.status_code == 201
        did = r.json()["dispute_id"]
        q = create_query(client, package_id=world["pkg2"]).json()
        assert q["packages"][0]["open_dispute_count"] == 1
        assert assign_dispute(client, did, key="da2").status_code == 200
        assert resolve_dispute(client, did, key="dr2").status_code == 200
        assert close_dispute(client, did, key="dc2").status_code == 200
        detail = client.get(f"{BASE}/queries/{q['query_id']}").json()
        assert detail["packages"][0]["open_dispute_count"] == 1
        assert detail["packages"][0]["dispute_count"] == 1
        # 新查询看到争议已关闭
        q2 = create_query(client, package_id=world["pkg2"]).json()
        assert q2["packages"][0]["open_dispute_count"] == 0

    def test_card_assignment_state_frozen_before_receipt(self, client, world):
        # pkg1 的 auditor3 未回执时建查询(pending=1, 完成率 2/3),
        # 之后补 SIGNED: 固定时点卡片仍为 pending
        q = create_query(client, package_id=world["pkg1"]).json()
        card0 = next(p for p in q["packages"]
                     if p["package_id"] == world["pkg1"])
        assert card0["pending"] == 1 and card0["signed"] == 1
        dl3 = issue_and_redeem(client, world["pkg1"], "auditor3", key="tk3")
        d = client.get(
            f"/api/admin/evidence/distributions/{world['pkg1']}",
            params={"operator": "admin"}).json()
        submit_receipt(client, world["pkg1"], "auditor3", dl3, d,
                       world["gseqs1"], key="rcpt3")
        detail = client.get(f"{BASE}/queries/{q['query_id']}").json()
        card = next(p for p in detail["packages"]
                    if p["package_id"] == world["pkg1"])
        assert card["pending"] == 1
        assert card["completed_receipts"] == 2
        assert card["completion_rate"] == round(2 / 3, 4)

    def test_pagination_consistent_when_new_events_arrive(self, client, world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=3)
        first = r1.json()
        assert len(first["items"]) == 3
        cursor = first["cursor"]["next_cursor"]
        # 翻页途中新增数据
        rv3, g3 = make_archived_review(client, key="base3", start=21, end=22)
        register(client, "auditor9", key="rcp-a9")
        d3 = create_dist(client, rv3["review_id"], "auditor9", key="dist3")
        dl = issue_and_redeem(client, d3["package_id"], "auditor9", key="tk9")
        submit_receipt(client, d3["package_id"], "auditor9", dl, d3,
                       sorted(g3), key="rcpt9")
        r2 = next_page(client, q["query_id"], cursor=cursor, limit=3)
        assert r2.status_code == 200, r2.text
        # 第二页紧接着第一页位置, 没有重复/缺口
        assert r2.json()["items"][0]["position"] == 3
        items = first["items"] + r2.json()["items"]
        rest, _ = fetch_all_from(client, q["query_id"],
                                 r2.json()["cursor"]["next_cursor"], limit=3)
        all_items = items + rest
        assert len(all_items) == q["total_items"]
        positions = [x["position"] for x in all_items]
        assert positions == list(range(q["total_items"]))


def fetch_all_from(client, qid, cursor, limit=3):
    items, pages = [], []
    while True:
        r = next_page(client, qid, cursor=cursor, limit=limit)
        assert r.status_code == 200, r.text
        j = r.json()
        pages.append(j)
        items.extend(j["items"])
        if not j["cursor"]["has_more"]:
            return items, pages
        cursor = j["cursor"]["next_cursor"]


# ======================================================================
# ---------- 游标: 稳定顺序 / 跳页 / 重复 / 跨查询 / 损坏 / 末页 ----------
# ======================================================================

class TestCursorRules:
    def test_stable_order_across_pages(self, client, world):
        q = create_query(client).json()
        items, _ = fetch_all(client, q["query_id"], limit=2)
        keys = [(x["event_ts"], 0 if x["kind"] == "RECEIPT" else 1,
                 x["row_id"]) for x in items]
        assert keys == sorted(keys)

    def test_cursor_jump_rejected(self, client, world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=2).json()
        fp = receiptaudit._hash_obj(
            client.get(f"{BASE}/queries/{q['query_id']}")
            .json()["filters"])[:16]
        # 伪造一个"未来页码"(跳到第 5 页, 实际才交付 1 页)的游标 -> 拒绝
        future = receiptaudit.encode_cursor(q["query_id"], 8, 5, fp)
        r = next_page(client, q["query_id"], cursor=future)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "cursor_invalid"
        # 正常游标(第 2 页)仍可使用, 查询未被污染
        ok = next_page(client, q["query_id"],
                       cursor=r1["cursor"]["next_cursor"], limit=2)
        assert ok.status_code == 200

    def test_cursor_backward_page_rejected(self, client, world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=2).json()
        next_page(client, q["query_id"],
                  cursor=r1["cursor"]["next_cursor"], limit=2)
        # 已交付 2 页, 再拿第 1 页签发位置的游标(第 2 页页码) -> 重复使用
        fp = receiptaudit._hash_obj(
            client.get(f"{BASE}/queries/{q['query_id']}")
            .json()["filters"])[:16]
        stale = receiptaudit.encode_cursor(q["query_id"], 2, 2, fp)
        r = next_page(client, q["query_id"], cursor=stale)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "cursor_reused"

    def test_cursor_reuse_rejected(self, client, world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=2).json()
        cur = r1["cursor"]["next_cursor"]
        ok = next_page(client, q["query_id"], cursor=cur, limit=2)
        assert ok.status_code == 200
        again = next_page(client, q["query_id"], cursor=cur, limit=2)
        assert again.status_code == 409
        assert again.json()["detail"]["code"] == "cursor_reused"

    def test_no_cursor_restart_rejected(self, client, world):
        q = create_query(client).json()
        next_page(client, q["query_id"], limit=2)
        r = next_page(client, q["query_id"], limit=2)  # 不带 cursor
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "cursor_required"

    def test_cursor_other_query_rejected(self, client, world):
        q1 = create_query(client, package_id=world["pkg1"]).json()
        q2 = create_query(client, package_id=world["pkg2"]).json()
        r1 = next_page(client, q1["query_id"], limit=1).json()
        cur = r1["cursor"]["next_cursor"]
        r = next_page(client, q2["query_id"], cursor=cur, limit=1)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "cursor_other_query"

    def test_cursor_garbage_rejected(self, client, world):
        q = create_query(client, package_id=world["pkg1"]).json()
        next_page(client, q["query_id"], limit=1)
        r = next_page(client, q["query_id"], cursor="not-a-cursor")
        assert r.status_code == 422
        assert r.json()["detail"]["code"] == "cursor_invalid"

    def test_filters_change_rejected(self, client, world):
        # 游标内嵌条件指纹; 即便用同 query_id 伪造不同指纹的游标也被拒绝
        q = create_query(client, package_id=world["pkg1"]).json()
        next_page(client, q["query_id"], limit=1)
        forged = receiptaudit.encode_cursor(
            q["query_id"], 1, 2, "f" * 16)
        r = next_page(client, q["query_id"], cursor=forged)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "cursor_filters_mismatch"

    def test_last_page_closes_query(self, client, world):
        q = create_query(client, package_id=world["pkg2"]).json()
        r1 = next_page(client, q["query_id"], limit=1)
        j1 = r1.json()
        assert j1["cursor"]["has_more"] is False
        assert j1["cursor"]["next_cursor"] is None
        assert j1["query_status"] == "CLOSED"
        detail = client.get(f"{BASE}/queries/{q['query_id']}").json()
        assert detail["status"] == "CLOSED" and detail["closed_at"]

    def test_past_end_idempotent_empty_page(self, client, world):
        # 多页查询取完后再次取页(无游标)幂等返回空页, 不报错
        q = create_query(client).json()
        items, pages = fetch_all(client, q["query_id"], limit=2)
        assert len(items) == q["total_items"]
        again = next_page(client, q["query_id"])
        assert again.status_code == 200
        j = again.json()
        assert j["items"] == [] and j["cursor"]["has_more"] is False
        assert j["query_status"] == "CLOSED"

    def test_unknown_query_404(self, client, world):
        r = next_page(client, "RAQ00000000000000")
        assert r.status_code == 404
        assert r.json()["detail"]["code"] == "query_not_found"

    def test_order_stable_across_queries_and_page_size(self, client, world):
        def all_rows(limit):
            q = create_query(client).json()
            items, _ = fetch_all(client, q["query_id"], limit=limit)
            return [x["row_id"] for x in items]
        seq_small = all_rows(1)
        seq_large = all_rows(500)
        assert seq_small == seq_large  # 页大小不改变顺序
        assert len(seq_small) == len(set(seq_small))  # 无重复
        assert seq_small  # 非空
        # 第三查询(中间无新数据)结果完全一致
        seq_again = all_rows(10)
        assert seq_again == seq_small


# ======================================================================
# ---------- CSV 导出: 一致性 / 空结果 / 幂等 / 下载 ----------
# ======================================================================

class TestCsvExport:
    def _all_pages_items(self, client, qid, limit=2):
        items, _ = fetch_all(client, qid, limit=limit)
        return items

    def test_export_rows_match_pagination(self, client, world):
        q = create_query(client).json()
        page_items = self._all_pages_items(client, q["query_id"], limit=2)
        r = make_export(client, q["query_id"], key="ex1")
        assert r.status_code == 201, r.text
        ej = r.json()
        assert ej["row_count"] == q["total_items"]
        assert ej["feed_digest"] == q["feed_digest"]
        assert len(ej["file_digest"]) == 64
        dl = client.get(f"{BASE}/exports/{ej['export_id']}/download",
                        params={"operator": "admin"})
        assert dl.status_code == 200
        assert dl.headers["content-type"].startswith("text/csv")
        rows = csv_rows(dl.content)
        assert len(rows) == len(page_items)
        for row, item in zip(rows, page_items):
            assert row["kind"] == item["kind"]
            assert row["event_ts"] == (item["event_ts"] or "")
            assert row["package_id"] == (item.get("package_id") or "")
            assert row["recipient"] == item.get("recipient")
            assert row["status"] == item.get("status")
            assert int(row["position"]) == item["position"]
            assert row["receipt_id"] == (item.get("receipt_id") or "")
            assert row["dispute_event"] == (item.get("dispute_event") or "")
            assert row["operator"] == (item.get("operator") or "")
        # position 列严格连续
        assert [int(x["position"]) for x in rows] == list(range(len(rows)))

    def test_export_header_present(self, client, world):
        q = create_query(client).json()
        ej = make_export(client, q["query_id"], key="exh").json()
        dl = client.get(f"{BASE}/exports/{ej['export_id']}/download")
        text = dl.content.decode("utf-8-sig")
        header = text.splitlines()[0].split(",")
        assert header == receiptaudit.CSV_COLUMNS

    def test_empty_export_header_only(self, client, world):
        q = create_query(client, package_id=world["pkg1"],
                         recipient="auditor4").json()
        assert q["total_items"] == 0
        ej = make_export(client, q["query_id"], key="ex-empty").json()
        assert ej["row_count"] == 0 and ej["empty"] is True
        dl = client.get(f"{BASE}/exports/{ej['export_id']}/download")
        rows = csv_rows(dl.content)
        assert rows == []
        # 仍有且仅有表头行
        assert len(dl.content.decode("utf-8-sig").splitlines()) == 1

    def test_export_idempotent_same_key(self, client, world):
        q = create_query(client).json()
        e1 = make_export(client, q["query_id"], key="idem1").json()
        e2 = make_export(client, q["query_id"], key="idem1").json()
        assert e1["export_id"] == e2["export_id"]
        assert e1["file_digest"] == e2["file_digest"]

    def test_export_different_key_same_bytes(self, client, world):
        q = create_query(client).json()
        e1 = make_export(client, q["query_id"], key="idem-a").json()
        e2 = make_export(client, q["query_id"], key="idem-b").json()
        assert e1["export_id"] != e2["export_id"]
        assert e1["file_digest"] == e2["file_digest"]
        c1 = client.get(f"{BASE}/exports/{e1['export_id']}/download").content
        c2 = client.get(f"{BASE}/exports/{e2['export_id']}/download").content
        assert c1 == c2

    def test_export_key_cross_query_rejected(self, client, world):
        q1 = create_query(client, package_id=world["pkg1"]).json()
        q2 = create_query(client, package_id=world["pkg2"]).json()
        assert make_export(client, q1["query_id"], key="shared").status_code == 201
        r = make_export(client, q2["query_id"], key="shared")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "idempotency_reuse"
        # 拒绝也写导出日志且在错误回滚后仍保留
        logs = client.get(f"{BASE}/operation-logs",
                          params={"query_id": q2["query_id"]}).json()
        rej = [x for x in logs if x["operation"] == "query.export"
               and not x["ok"]]
        assert rej and rej[0]["reason_code"] == "idempotency_reuse"

    def test_export_independent_of_pagination(self, client, world):
        # 未翻页 / 翻到一半 / 翻完, 导出内容都与完整分页一致
        q = create_query(client).json()
        ej = make_export(client, q["query_id"], key="ex-nopage").json()
        page_items, _ = fetch_all(client, q["query_id"], limit=2)
        dl = client.get(f"{BASE}/exports/{ej['export_id']}/download")
        rows = csv_rows(dl.content)
        assert [int(x["position"]) for x in rows] == \
            [x["position"] for x in page_items]
        assert [x["receipt_id"] for x in rows
                if x["kind"] == "RECEIPT"] == \
            [x["receipt_id"] for x in page_items if x["kind"] == "RECEIPT"]

    def test_export_of_unknown_query(self, client, world):
        r = client.post(
            f"{BASE}/queries/RAQ00000000000000/exports",
            json={"operator": "admin", "idempotency_key": "x"})
        assert r.status_code == 404

    def test_download_increments_count_and_listed(self, client, world):
        q = create_query(client).json()
        ej = make_export(client, q["query_id"], key="exdl").json()
        assert ej["download_count"] == 0
        for _ in range(2):
            assert client.get(
                f"{BASE}/exports/{ej['export_id']}/download").status_code == 200
        got = client.get(f"{BASE}/exports/{ej['export_id']}").json()
        assert got["download_count"] == 2
        listing = client.get(f"{BASE}/exports",
                             params={"query_id": q["query_id"]}).json()
        assert any(x["export_id"] == ej["export_id"] for x in listing)


# ======================================================================
# ---------- 操作日志 ----------
# ======================================================================

class TestOperationLogs:
    def test_create_page_export_download_logged(self, client, world):
        q = create_query(client).json()
        next_page(client, q["query_id"], limit=2)
        ej = make_export(client, q["query_id"], key="logex").json()
        client.get(f"{BASE}/exports/{ej['export_id']}/download")
        logs = client.get(f"{BASE}/operation-logs",
                          params={"query_id": q["query_id"]}).json()
        ops = [x["operation"] for x in logs]
        assert "query.create" in ops
        assert "query.page" in ops
        assert "query.export" in ops
        assert "query.export_download" in ops
        # 倒序
        assert logs == sorted(logs, key=lambda x: x["id"], reverse=True)
        # 创建日志记录边界与空标志
        create_log = next(x for x in logs if x["operation"] == "query.create")
        assert create_log["ok"] is True
        assert create_log["detail"]["total_items"] == q["total_items"]
        assert "upper_dispute_event_id" in create_log["detail"]

    def test_rejected_page_logged(self, client, world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=2).json()
        cur = r1["cursor"]["next_cursor"]
        next_page(client, q["query_id"], cursor=cur, limit=2)
        again = next_page(client, q["query_id"], cursor=cur, limit=2)
        assert again.status_code == 409
        logs = client.get(f"{BASE}/operation-logs",
                          params={"query_id": q["query_id"],
                                  "operation": "query.page"}).json()
        rejected = [x for x in logs if not x["ok"]]
        assert rejected and rejected[0]["reason_code"] == "cursor_reused"

    def test_invalid_operation_filter(self, client, world):
        r = client.get(f"{BASE}/operation-logs",
                       params={"operation": "nope"})
        assert r.status_code == 422

    def test_rejected_create_logged(self, client, world):
        r = create_query(client, recipient="nobody")
        assert r.status_code == 404
        logs = client.get(f"{BASE}/operation-logs",
                          params={"operation": "query.create"}).json()
        rejected = [x for x in logs if not x["ok"]]
        assert rejected
        assert rejected[0]["reason_code"] == "recipient_unknown"


# ======================================================================
# ---------- 服务重启(复用同一数据库文件)后查询/导出仍可继续 ----------
# ======================================================================

class TestPersistenceAcrossSessions:
    def test_query_progress_and_export_survive_new_session(self, client,
                                                           world):
        q = create_query(client).json()
        r1 = next_page(client, q["query_id"], limit=2).json()
        ej = make_export(client, q["query_id"], key="persist-ex").json()
        # 用全新 ORM 会话访问同一数据库文件, 模拟服务重启后状态仍在
        import app.receiptaudit as ra
        db = SessionLocal()
        try:
            qrow = ra.get_query(db, q["query_id"])
            assert qrow.pages_delivered == 1
            assert qrow.last_cursor == r1["cursor"]["next_cursor"]
            assert qrow.status == "ACTIVE"
            exp = ra.get_export(db, ej["export_id"])
            assert exp.file_digest == ej["file_digest"]
            assert os.path.exists(exp.file_path)
        finally:
            db.close()
        # 游标仍是上一页签发的, 新请求可继续
        r2 = next_page(client, q["query_id"],
                       cursor=r1["cursor"]["next_cursor"], limit=2)
        assert r2.status_code == 200
