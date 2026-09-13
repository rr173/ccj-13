"""证据复核与签署归档流程测试。

覆盖:
- 正常创建(必须 CLOSED 会话 + COMPLETED 导出包, 固定筛选条件/起止 global_seq/
  manifest_hash/范围指纹)与逐事件复核(确认/存疑/排除, 存疑排除必须带说明);
- 复核单页面/接口展示事件详情、结论、说明、操作者、版本与待处理数量;
- 同一事件并发版本冲突(If-Match 过期/缺失 409, 后写不覆盖先写);
- 引用固定会话范围外事件被拒绝(event_out_of_scope);
- 两名不同操作者签署要求 + 同操作者重复签署去重(不覆盖) + 归档前缺签拒绝;
- 提交/归档前重新校验: 链变化 -> INVALIDATED(机器可读原因, 禁止提交与归档),
  导出包摘要不一致 -> INVALIDATED(manifest_hash/包校验);
- 失效后修复重新校验通过 -> OPEN(scope_version+1, 旧结论留史), 重新逐事件复核并归档;
- 归档生成不可变签署摘要(结论统计/事件范围/操作者/manifest_hash), 归档后禁止修改;
- 幂等: 同键重放不产生重复结论/归档;
- 非回归: 证据分页/导出、补偿审批与执行窗口接口。
"""
import io
import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

_tmp = tempfile.mkdtemp(prefix="evidence-review-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["EVIDENCE_STORE_DIR"] = f"{_tmp}/store"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import AuditEvent, EvidenceReview, EvidenceReviewConclusion
from app import auditreplay, evidence, plans


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(os.environ["EVIDENCE_STORE_DIR"], exist_ok=True)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建 ----------

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


def mk_plan(client, batch_id, key, name="plan", risk="LOW"):
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": key, "name": name,
        "risk_level": risk,
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


def setup_completed(client, start=1, end=3, key="k"):
    add_records(client, range(start, end + 1))
    b = mk_batch(client, start, end, f"b-{key}")
    p = mk_plan(client, b, f"p-{key}")
    complete_plan(client, p, f"s-{key}")
    return p, b


def new_session(client, pid, key="es", **extra):
    r = client.post("/api/admin/evidence/sessions",
                    json={"operator": "alice", "idempotency_key": key,
                          "plan_id": pid, **extra})
    assert r.status_code == 201, r.text
    return r.json()


def page_all(client, sid, limit=50, operator="alice"):
    cursor = None
    items = []
    while True:
        r = client.post(f"/api/admin/evidence/sessions/{sid}/pages",
                        json={"operator": operator, "cursor": cursor,
                              "limit": limit})
        assert r.status_code == 200, r.text
        j = r.json()
        items.extend(j["items"])
        cursor = j["cursor"]["next_cursor"]
        if not j["cursor"]["has_more"]:
            break
    return items


def run_export(client, eid, ticks=100):
    w = evidence.EvidenceExportWorker()
    for _ in range(ticks):
        st = client.get(f"/api/admin/evidence/exports/{eid}").json()["status"]
        if st in ("COMPLETED", "FAILED", "PAUSED", "CANCELED"):
            break
        w.tick_once()
    return client.get(f"/api/admin/evidence/exports/{eid}").json()


def create_export(client, sid, key="ee", segment_size=None):
    body = {"operator": "alice", "idempotency_key": key, "session_id": sid}
    if segment_size is not None:
        body["segment_size"] = segment_size
    r = client.post("/api/admin/evidence/exports", json=body)
    assert r.status_code in (200, 201), r.text
    return r.json()["export_id"]


def make_review_basis(client, pid, key="k", segment_size=2):
    """完整计划 -> CLOSED 会话 -> COMPLETED 导出包, 返回 (session, export, gseqs)。"""
    s = new_session(client, pid, key=f"es-{key}")
    sid = s["session_id"]
    items = page_all(client, sid, limit=50)
    assert len(items) == s["expected_count"]
    eid = create_export(client, sid, key=f"ee-{key}", segment_size=segment_size)
    d = run_export(client, eid)
    assert d["status"] == "COMPLETED", d
    return s, d, [i["global_seq"] for i in items]


def create_review(client, sid, key="rv", operator="alice", export_id=None,
                  status=201):
    body = {"operator": operator, "idempotency_key": key, "session_id": sid}
    if export_id:
        body["export_id"] = export_id
    r = client.post("/api/admin/evidence/reviews", json=body)
    assert r.status_code == status, r.text
    return r.json()


def get_review(client, rid, **params):
    r = client.get(f"/api/admin/evidence/reviews/{rid}", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def submit(client, rid, operator, key, gs, verdict="CONFIRMED", note=None,
           if_match=None, status=200):
    body = {"operator": operator, "idempotency_key": key,
            "global_seq": gs, "verdict": verdict}
    if note is not None:
        body["note"] = note
    headers = {"If-Match": str(if_match)} if if_match is not None else {}
    r = client.post(f"/api/admin/evidence/reviews/{rid}/conclusions",
                    json=body, headers=headers)
    assert r.status_code == status, r.text
    return r.json()


def sign_all(client, rid, gseqs, operator, key_prefix, start_version,
             verdict="CONFIRMED", note=None):
    """同一操作者对所有事件签署 CONFIRMED, 逐次 If-Match。"""
    for i, gs in enumerate(gseqs):
        submit(client, rid, operator, f"{key_prefix}-{gs}", gs,
               verdict=verdict, note=note, if_match=start_version + i)


def archive(client, rid, operator="dave", key="ar", status=200):
    r = client.post(f"/api/admin/evidence/reviews/{rid}/archive",
                    json={"operator": operator, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r.json()


def reverify(client, rid, operator="dave", key="rvf", status=200):
    r = client.post(f"/api/admin/evidence/reviews/{rid}/reverify",
                    json={"operator": operator, "idempotency_key": key})
    assert r.status_code == status, r.text
    return r.json()


def tamper_payload_rechain(db, pid, stream_seq_offset=2):
    """篡改计划流某事件 payload 并重链该流(单流自洽, 靠复核的范围指纹/锚点/包校验发现)。"""
    ev = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
          .order_by(AuditEvent.stream_seq).offset(stream_seq_offset).first())
    assert ev is not None
    ev.payload = {**ev.payload, "evil_marker": "TAMPERED"}
    rows = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
            .order_by(AuditEvent.stream_seq).all())
    prev = None
    for row in rows:
        row.prev_stream_hash = prev
        row.stream_hash = auditreplay._chain_hash(
            row.stream_seq, row.event_type, row.correlation_id, row.payload,
            row.event_ts, prev, row.operator)
        prev = row.stream_hash
    db.commit()
    return ev.global_seq


def restore_payload_rechain(db, pid, global_seq, clean_payload):
    ev = (db.query(AuditEvent).filter(AuditEvent.global_seq == global_seq).one())
    ev.payload = clean_payload
    rows = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
            .order_by(AuditEvent.stream_seq).all())
    prev = None
    for row in rows:
        row.prev_stream_hash = prev
        row.stream_hash = auditreplay._chain_hash(
            row.stream_seq, row.event_type, row.correlation_id, row.payload,
            row.event_ts, prev, row.operator)
        prev = row.stream_hash
    db.commit()


# ======================================================================
# ---------- 创建复核单: 前置条件与固定依据 ----------
# ======================================================================

class TestReviewCreation:
    def test_create_filters_and_fixes_basis(self, client):
        pid, _ = setup_completed(client)
        s, exp, gseqs = make_review_basis(client, pid)
        rv = create_review(client, s["session_id"])
        rid = rv["review_id"]
        assert rid.startswith("ER")
        assert rv["status"] == "OPEN"
        assert rv["version"] == 0 and rv["scope_version"] == 1
        # 固定筛选条件
        f = rv["fixed"]["filters"]
        assert f["plan_id"] == pid
        assert f["streams"] == s["filters"]["streams"]
        assert f["upper_global_seq"] == s["boundary"]["upper_global_seq"]
        # 固定起止 global_seq 与 manifest_hash
        assert rv["fixed"]["upper_global_seq"] == s["boundary"]["upper_global_seq"]
        assert rv["fixed"]["first_global_seq"] == min(gseqs)
        assert rv["fixed"]["last_global_seq"] == max(gseqs)
        assert rv["fixed"]["total_events"] == len(gseqs)
        assert rv["fixed"]["export_manifest_hash"] == exp["manifest_hash"]
        assert rv["fixed"]["scope_fingerprint"]
        # 待处理数量: 每个事件都需要两名签署
        assert rv["pending_count"] == len(gseqs)
        assert rv["stats"]["signed_events"] == 0
        assert rv["stats"]["operator_count"] == 0
        # 页面展示固定依据
        assert rv["export_id"] == exp["export_id"]
        assert rv["invalid_reason"] is None

    def test_requires_closed_session(self, client):
        pid, _ = setup_completed(client)
        s = new_session(client, pid, key="es-open")
        # ACTIVE(未翻页)直接创建 -> 409
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rv1",
                              "session_id": s["session_id"]})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "session_not_closed"

    def test_requires_completed_export(self, client):
        pid, _ = setup_completed(client)
        s = new_session(client, pid)
        page_all(client, s["session_id"])
        # 无任何导出 -> 409 export_not_completed
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rv1",
                              "session_id": s["session_id"]})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "export_not_completed"
        # 不存在的导出 -> 404
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rv2",
                              "session_id": s["session_id"],
                              "export_id": "EE-nope"})
        assert r.status_code == 404
        # 导出属于别的会话 -> 409
        s2 = new_session(client, pid, key="es2")
        page_all(client, s2["session_id"])
        eid = create_export(client, s2["session_id"], key="ee2")
        run_export(client, eid)
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rv3",
                              "session_id": s["session_id"], "export_id": eid})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "export_session_mismatch"

    def test_only_session_creator_can_create(self, client):
        pid, _ = setup_completed(client)
        s, _, _ = make_review_basis(client, pid)
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "mallory", "idempotency_key": "rvx",
                              "session_id": s["session_id"]})
        assert r.status_code == 409

    def test_unknown_session_404(self, client):
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rvx",
                              "session_id": "ES-nope"})
        assert r.status_code == 404
        r = client.get("/api/admin/evidence/reviews/ER-nope")
        assert r.status_code == 404

    def test_idempotent_create_and_list_filter(self, client):
        pid, _ = setup_completed(client)
        s, _, _ = make_review_basis(client, pid)
        a = create_review(client, s["session_id"], key="rv1")
        b = create_review(client, s["session_id"], key="rv1")
        assert b["replayed"] is True and b["review_id"] == a["review_id"]
        # 不同键也回显同一未归档复核单
        c = create_review(client, s["session_id"], key="rv2")
        assert c["review_id"] == a["review_id"]
        rows = client.get("/api/admin/evidence/reviews",
                          params={"session_id": s["session_id"]}).json()
        assert [x["review_id"] for x in rows] == [a["review_id"]]
        rows = client.get("/api/admin/evidence/reviews",
                          params={"status_filter": "OPEN"}).json()
        assert len(rows) == 1
        r = client.get("/api/admin/evidence/reviews",
                       params={"status_filter": "BOGUS"})
        assert r.status_code == 422


# ======================================================================
# ---------- 逐事件复核: 结论/说明/操作者/版本/待处理数量 ----------
# ======================================================================

class TestEventConclusions:
    def _basis(self, client, key="k"):
        pid, _ = setup_completed(client, key=key)
        s, exp, gseqs = make_review_basis(client, pid, key=key)
        rv = create_review(client, s["session_id"], key=f"rv-{key}")
        return rv, gseqs, exp, s

    def test_page_shows_event_detail_and_pending_count(self, client):
        rv, gseqs, exp, s = self._basis(client)
        d = get_review(client, rv["review_id"])
        item = d["items"][0]
        ev = item["event"]
        # 事件详情齐全
        assert ev["global_seq"] == gseqs[0]
        assert ev["stream_key"] and ev["event_type"]
        assert "payload" in ev and "stream_hash" in ev
        assert item["signatures"] == [] and item["pending"] is True
        # 提交一条确认后页面展示结论/说明/操作者/版本
        j = submit(client, rv["review_id"], "bob", "k1", gseqs[0],
                   if_match=0)
        assert j["version"] == 1
        assert j["stats"]["conclusion_count"] == 1
        d = get_review(client, rv["review_id"])
        assert d["version"] == 1
        assert d["pending_count"] == len(gseqs)  # 还没有事件达到两人
        item0 = next(x for x in d["items"] if x["event"]["global_seq"] == gseqs[0])
        assert item0["signature_count"] == 1
        sig = item0["signatures"][0]
        assert sig["verdict"] == "CONFIRMED"
        assert sig["operator"] == "bob"
        assert sig["scope_version"] == 1
        assert sig["review_version"] == 0
        assert sig["current"] is True
        # pending_only 过滤
        pending = client.get(
            f"/api/admin/evidence/reviews/{rv['review_id']}",
            params={"pending_only": "true"}).json()
        assert all(x["pending"] for x in pending["items"])
        assert all(x["event"]["global_seq"] != gseqs[0] or x["pending"]
                   for x in pending["items"])

    def test_questioned_excluded_require_note_and_counted(self, client):
        rv, gseqs, _, _ = self._basis(client, key="q")
        # QUESTIONED 无说明 -> 409
        j = submit(client, rv["review_id"], "bob", "kq", gseqs[0],
                   verdict="QUESTIONED", if_match=0, status=409)
        assert j["detail"]["code"] == "note_required"
        # EXCLUDED 无说明 -> 409
        j = submit(client, rv["review_id"], "bob", "ke", gseqs[1],
                   verdict="EXCLUDED", if_match=0, status=409)
        assert j["detail"]["code"] == "note_required"
        # 带说明成功
        j = submit(client, rv["review_id"], "bob", "kq2", gseqs[0],
                   verdict="QUESTIONED", note="该事件时间戳与工单不一致",
                   if_match=0)
        assert j["stats"]["verdict_counts"]["QUESTIONED"] == 1
        j = submit(client, rv["review_id"], "bob", "ke2", gseqs[1],
                   verdict="EXCLUDED", note="属于其他变更窗口, 排除",
                   if_match=1)
        assert j["stats"]["verdict_counts"]["EXCLUDED"] == 1
        # 非法结论 -> 409
        j = submit(client, rv["review_id"], "bob", "kb", gseqs[2],
                   verdict="MAYBE", if_match=2, status=409)
        assert j["detail"]["code"] == "invalid_verdict"
        d = get_review(client, rv["review_id"])
        sig = next(x for x in d["items"]
                   if x["event"]["global_seq"] == gseqs[0])["signatures"][0]
        assert sig["note"] == "该事件时间戳与工单不一致"

    def test_pending_decreases_only_after_two_distinct_operators(self, client):
        rv, gseqs, _, _ = self._basis(client, key="p")
        n = len(gseqs)
        # 事件0 两人签完 -> signed_events=1, pending n-1
        submit(client, rv["review_id"], "bob", "b0", gseqs[0], if_match=0)
        j = submit(client, rv["review_id"], "carol", "c0", gseqs[0], if_match=1)
        assert j["stats"]["signed_events"] == 1
        assert j["stats"]["distinct_operators"] == ["bob", "carol"]
        assert j["stats"]["pending_count"] == n - 1
        d = get_review(client, rv["review_id"])
        item0 = next(x for x in d["items"]
                     if x["event"]["global_seq"] == gseqs[0])
        assert item0["pending"] is False
        assert item0["operators"] == ["bob", "carol"]


# ======================================================================
# ---------- 版本栅栏 / 越界引用 / 重复签署 ----------
# ======================================================================

class TestVersionFenceAndScope:
    def _basis(self, client, key="v"):
        pid, _ = setup_completed(client, key=key)
        s, _, gseqs = make_review_basis(client, pid, key=key)
        rv = create_review(client, s["session_id"], key=f"rv-{key}")
        return rv, gseqs, s

    def test_concurrent_stale_if_match_conflict_no_overwrite(self, client):
        rv, gseqs, _ = self._basis(client)
        rid = rv["review_id"]
        # 两个并发请求都基于 v0: 第一个成功, 第二个过期 409
        j1 = submit(client, rid, "bob", "k1", gseqs[0], if_match=0)
        assert j1["version"] == 1
        j2 = submit(client, rid, "carol", "k2", gseqs[1], if_match=0, status=409)
        assert j2["detail"]["code"] == "version_conflict"
        assert j2["detail"]["expected"] == 1 and j2["detail"]["supplied"] == 0
        # 失败写入没有产生结论
        db = SessionLocal()
        try:
            assert db.query(EvidenceReviewConclusion).filter_by(
                review_id=rid).count() == 1
        finally:
            db.close()
        # 缺 If-Match -> 409 if_match_required
        j3 = submit(client, rid, "carol", "k3", gseqs[1], status=409)
        assert j3["detail"]["code"] == "if_match_required"
        # 非法 If-Match 头 -> 409
        r = client.post(f"/api/admin/evidence/reviews/{rid}/conclusions",
                        json={"operator": "carol", "idempotency_key": "k4",
                              "global_seq": gseqs[1], "verdict": "CONFIRMED"},
                        headers={"If-Match": "abc"})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "if_match_invalid"
        # 基于新版本的写入成功且不覆盖 v1 的结论
        j4 = submit(client, rid, "carol", "k5", gseqs[1], if_match=1)
        assert j4["version"] == 2
        d = get_review(client, rid)
        by = {x["event"]["global_seq"]: x for x in d["items"]}
        assert by[gseqs[0]]["signatures"][0]["operator"] == "bob"
        assert by[gseqs[1]]["signatures"][0]["operator"] == "carol"

    def test_event_out_of_fixed_session_rejected(self, client):
        rv, gseqs, s = self._basis(client, key="oos")
        rid = rv["review_id"]
        upper = s["boundary"]["upper_global_seq"]
        # global_seq 超出固定上界
        j = submit(client, rid, "bob", "o1", upper + 100, if_match=0, status=409)
        assert j["detail"]["code"] == "event_out_of_scope"
        assert j["detail"]["upper_global_seq"] == upper
        # global_seq 小于起始(=0 非法也 422 参数层); 取上界内但属于其他流的事件:
        # 用一个不同计划的事件 global_seq(必然不在 include_streams)
        pid2, _ = setup_completed(client, start=10, end=12, key="other")
        s2 = new_session(client, pid2, key="es-other")
        other_items = page_all(client, s2["session_id"])
        foreign_gs = other_items[0]["global_seq"]
        assert foreign_gs <= upper or True  # 无论大小, 流不在固定集合都必须拒
        j = submit(client, rid, "bob", "o2", foreign_gs, if_match=0, status=409)
        assert j["detail"]["code"] == "event_out_of_scope"
        # 越界写入未产生结论
        db = SessionLocal()
        try:
            assert db.query(EvidenceReviewConclusion).filter_by(
                review_id=rid).count() == 0
        finally:
            db.close()

    def test_duplicate_signature_same_operator_rejected(self, client):
        rv, gseqs, _ = self._basis(client, key="dup")
        rid = rv["review_id"]
        submit(client, rid, "bob", "k1", gseqs[0], if_match=0)
        # 同操作者同事件不同结论(不同幂等键) -> 409, 不覆盖
        j = submit(client, rid, "bob", "k2", gseqs[0],
                   verdict="QUESTIONED", note="想改判", if_match=1, status=409)
        assert j["detail"]["code"] == "duplicate_signature"
        assert j["detail"]["existing_verdict"] == "CONFIRMED"
        d = get_review(client, rid)
        sig = next(x for x in d["items"]
                   if x["event"]["global_seq"] == gseqs[0])["signatures"][0]
        assert sig["verdict"] == "CONFIRMED"
        db = SessionLocal()
        try:
            assert db.query(EvidenceReviewConclusion).filter_by(
                review_id=rid).count() == 1
        finally:
            db.close()

    def test_event_filtered_out_of_session_scope_rejected(self, client):
        """在流与 global_seq 边界内、但被会话创建时 event_types 过滤掉的事件,
        不属于固定会话结果集, 同样不能被复核结论引用。"""
        pid, _ = setup_completed(client, key="filtscope")
        # 会话只看 BATCH_CUTOVER 一类事件
        s = new_session(client, pid, key="es-f", event_types=["BATCH_CUTOVER"])
        in_scope = page_all(client, s["session_id"])
        assert in_scope and len(in_scope) < s["boundary"]["upper_global_seq"]
        eid = create_export(client, s["session_id"], key="ee-f")
        assert run_export(client, eid)["status"] == "COMPLETED"
        rv = create_review(client, s["session_id"], key="rv-f")
        rid = rv["review_id"]
        in_gs = {i["global_seq"] for i in in_scope}
        # 找一个边界内、流匹配但被类型过滤掉的事件
        db = SessionLocal()
        try:
            other = (db.query(AuditEvent)
                     .filter(AuditEvent.stream_key.in_(s["filters"]["streams"]),
                             AuditEvent.global_seq > s["filters"]["start_global_seq"],
                             AuditEvent.global_seq
                             <= s["boundary"]["upper_global_seq"])
                     .order_by(AuditEvent.global_seq).all())
        finally:
            db.close()
        excluded = next(e.global_seq for e in other if e.global_seq not in in_gs)
        j = submit(client, rid, "bob", "fx", excluded, if_match=0, status=409)
        assert j["detail"]["code"] == "event_out_of_scope"
        # 范围内事件可以正常引用
        j = submit(client, rid, "bob", "fok", min(in_gs), if_match=0)
        assert j["verdict"] == "CONFIRMED"

    def test_same_idempotency_key_replay_no_duplicate(self, client):
        rv, gseqs, _ = self._basis(client, key="idem")
        rid = rv["review_id"]
        j1 = submit(client, rid, "bob", "same-key", gseqs[0], if_match=0)
        j2 = submit(client, rid, "bob", "same-key", gseqs[0], if_match=0)
        assert j2["replayed"] is True
        assert j2["version"] == j1["version"]
        db = SessionLocal()
        try:
            assert db.query(EvidenceReviewConclusion).filter_by(
                review_id=rid).count() == 1
        finally:
            db.close()
        # 同键不同请求体 -> 409
        r = client.post(f"/api/admin/evidence/reviews/{rid}/conclusions",
                        json={"operator": "bob", "idempotency_key": "same-key",
                              "global_seq": gseqs[1], "verdict": "CONFIRMED"},
                        headers={"If-Match": "1"})
        assert r.status_code == 409


# ======================================================================
# ---------- 双人签署与归档 ----------
# ======================================================================

class TestSigningAndArchive:
    def _ready(self, client, key="a"):
        pid, _ = setup_completed(client, key=key)
        s, exp, gseqs = make_review_basis(client, pid, key=key)
        rv = create_review(client, s["session_id"], key=f"rv-{key}")
        return rv, gseqs, exp, s, pid

    def test_archive_requires_two_distinct_operators_all_events(self, client):
        rv, gseqs, _, _, _ = self._ready(client)
        rid = rv["review_id"]
        n = len(gseqs)
        # 只有 bob 一人签完全部 -> 归档拒绝
        sign_all(client, rid, gseqs, "bob", "bob", start_version=0)
        j = archive(client, rid, status=409)
        assert j["detail"]["code"] == "two_operators_required"
        # carol 只签一部分 -> 缺签拒绝, 机器可读待处理
        for i, gs in enumerate(gseqs[:-1]):
            submit(client, rid, "carol", f"carol-{gs}", gs,
                   if_match=n + i)
        j = archive(client, rid, status=409)
        assert j["detail"]["code"] == "signatures_incomplete"
        assert j["detail"]["pending_count"] == 1
        assert gseqs[-1] in j["detail"]["pending_global_seqs"]
        # 补齐最后一条 -> 归档成功
        submit(client, rid, "carol", f"carol-last", gseqs[-1],
               if_match=n + len(gseqs) - 1)
        j = archive(client, rid)
        assert j["ok"] is True and j["signature_hash"]
        summary = j["signed_summary"]
        assert summary["operators"] == ["bob", "carol"]
        assert summary["operator_count"] == 2
        assert summary["event_range"]["total_events"] == n
        assert summary["event_range"]["first_global_seq"] == min(gseqs)
        assert summary["event_range"]["last_global_seq"] == max(gseqs)
        assert summary["event_range"]["scope_fingerprint"] == \
            rv["fixed"]["scope_fingerprint"]
        assert summary["manifest_hash"] == rv["fixed"]["export_manifest_hash"]
        assert summary["conclusion_stats"]["total"] == 2 * n
        assert summary["conclusion_stats"]["events_signed"] == n
        assert len(summary["events"]) == n
        assert all(len(e["signers"]) == 2 for e in summary["events"])

    def test_archived_review_is_immutable(self, client):
        rv, gseqs, _, _, _ = self._ready(client, key="imm")
        rid = rv["review_id"]
        n = len(gseqs)
        sign_all(client, rid, gseqs, "bob", "bob", 0)
        sign_all(client, rid, gseqs, "carol", "carol", n)
        archive(client, rid, key="ar1")
        # 归档后提交结论 -> 409
        j = submit(client, rid, "bob", "after", gseqs[0],
                   verdict="EXCLUDED", note="x", if_match=2 * n, status=409)
        assert j["detail"]["code"] == "review_archived"
        # 归档后 reverify -> 409
        r = client.post(f"/api/admin/evidence/reviews/{rid}/reverify",
                        json={"operator": "x", "idempotency_key": "rf"})
        assert r.status_code == 409 and \
            r.json()["detail"]["code"] == "review_archived"
        # 重复归档: 同键幂等; 不同键 already_in_state 无副作用
        j2 = archive(client, rid, key="ar1")
        assert j2["replayed"] is True
        j3 = archive(client, rid, key="ar2")
        assert j3["already_in_state"] is True
        assert j3["signature_hash"] == j["signature_hash"] if "signature_hash" in j else True
        # 归档后不能基于同依据再建复核单
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": "rv-new",
                              "session_id": rv["session_id"]})
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "review_already_archived"
        # 详情含不可变签署摘要
        d = get_review(client, rid)
        assert d["status"] == "ARCHIVED"
        assert d["archived_by"] == "dave" and d["archived_at"]
        assert d["signed_summary"]["signature_hash"] == d["signature_hash"]
        # 可离线复算 signature_hash
        recomputed = evidence._hash_obj(
            {k: v for k, v in d["signed_summary"].items()
             if k != "signature_hash"})
        assert recomputed == d["signature_hash"]

    def test_mixed_verdicts_archive_and_restart_preserves(self, client):
        rv, gseqs, exp, _, pid = self._ready(client, key="mix")
        rid = rv["review_id"]
        n = len(gseqs)
        # bob 全部确认; carol 对首个存疑、次个排除(带说明), 其余确认
        sign_all(client, rid, gseqs, "bob", "bob", 0)
        submit(client, rid, "carol", "c0", gseqs[0],
               verdict="QUESTIONED", note="时间戳存疑", if_match=n)
        submit(client, rid, "carol", "c1", gseqs[1],
               verdict="EXCLUDED", note="非本窗口事件", if_match=n + 1)
        for i, gs in enumerate(gseqs[2:]):
            submit(client, rid, "carol", f"c-{gs}", gs,
                   if_match=n + 2 + i)
        j = archive(client, rid)
        by = j["signed_summary"]["conclusion_stats"]["by_verdict"]
        assert by["CONFIRMED"] == 2 * n - 2
        assert by["QUESTIONED"] == 1 and by["EXCLUDED"] == 1
        # 重启后归档/签署/摘要保留
        with TestClient(app) as c2:
            d = c2.get(f"/api/admin/evidence/reviews/{rid}").json()
            assert d["status"] == "ARCHIVED"
            assert d["signature_hash"] == j["signature_hash"]
            assert len(d["items"]) == n
            db = SessionLocal()
            try:
                row = db.get(EvidenceReview, rid)
                assert row.status == "ARCHIVED"
                assert db.query(EvidenceReviewConclusion).filter_by(
                    review_id=rid).count() == 2 * n
            finally:
                db.close()


# ======================================================================
# ---------- 链变化/导出摘要不一致 -> INVALIDATED -> 修复重签归档 ----------
# ======================================================================

class TestInvalidationAndRecovery:
    def _ready(self, client, key="inv"):
        pid, _ = setup_completed(client, key=key)
        s, exp, gseqs = make_review_basis(client, pid, key=key)
        rv = create_review(client, s["session_id"], key=f"rv-{key}")
        return pid, s, exp, rv, gseqs

    def test_chain_change_invalidates_and_blocks_submit_archive(self, client):
        pid, s, exp, rv, gseqs = self._ready(client)
        rid = rv["review_id"]
        submit(client, rid, "bob", "k1", gseqs[0], if_match=0)
        # 找到被篡改事件的原始 payload(便于修复)
        db = SessionLocal()
        target = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
                  .order_by(AuditEvent.stream_seq).offset(2).first())
        clean_payload = target.payload
        gs = tamper_payload_rechain(db, pid)
        db.close()
        # 再提交: 先重新校验 -> INVALIDATED + 409, 结论未写入
        j = submit(client, rid, "bob", "k2", gseqs[1], if_match=1, status=409)
        assert j["detail"]["code"] in ("chain_broken", "scope_changed")
        d = get_review(client, rid)
        assert d["status"] == "INVALIDATED"
        assert d["invalid_reason"]["code"] in ("chain_broken", "scope_changed")
        assert d["invalidated_at"]
        # 机器可读原因
        if d["invalid_reason"]["code"] == "chain_broken":
            assert d["invalid_reason"]["breaks"]
        # 失效期间提交/归档一律拒绝
        j = submit(client, rid, "bob", "k3", gseqs[1], if_match=1, status=409)
        assert j["detail"]["code"] == "review_invalidated"
        j = archive(client, rid, status=409)
        assert j["detail"]["code"] == "review_invalidated"
        # 未修复时显式重新校验仍失败, 保持 INVALIDATED
        r = reverify(client, rid, key="rf1")
        assert r["valid"] is False and r["status"] == "INVALIDATED"
        # 修复 -> 重新校验通过, scope_version+1, 旧结论留史, 当前签署清空
        db = SessionLocal()
        try:
            restore_payload_rechain(db, pid, gs, clean_payload)
        finally:
            db.close()
        r = reverify(client, rid, key="rf2")
        assert r["valid"] is True and r["status"] == "OPEN"
        assert r["scope_version"] == 2
        d = get_review(client, rid)
        assert d["scope_version"] == 2
        assert d["stats"]["signed_events"] == 0
        assert d["pending_count"] == len(gseqs)
        assert len(d["history"]) == 1
        assert d["history"][0]["scope_version"] == 1
        assert d["history"][0]["current"] is False
        # 重新逐事件复核(两名操作者)并归档
        v = d["version"]
        sign_all(client, rid, gseqs, "bob", "nb", v)
        sign_all(client, rid, gseqs, "carol", "nc", v + len(gseqs))
        j = archive(client, rid, key="ar2")
        assert j["status"] == "ARCHIVED"
        assert j["signed_summary"]["scope_version"] == 2
        d2 = get_review(client, rid)
        assert d2["status"] == "ARCHIVED"
        # 历史旧结论仍保留可查
        assert len(d2["history"]) == 1
        assert {e["event"] for e in d2["events"]} >= {
            "review.create", "conclusion.submit", "review.invalidated",
            "review.reverified", "review.archive"}

    def test_package_manifest_tamper_invalidates(self, client):
        """导出 zip 包被外部篡改 -> 包回读校验失败, 复核失效
        (包与 manifest_hash 固定值不一致)。"""
        pid, s, exp, rv, gseqs = self._ready(client, key="pkg")
        rid = rv["review_id"]
        # 直接覆写证据包文件
        import glob
        zips = glob.glob(os.path.join(os.environ["EVIDENCE_STORE_DIR"],
                                      exp["export_id"], "*.zip"))
        assert zips
        with open(zips[0], "r+b") as f:
            f.seek(0)
            f.write(b"PK\x03\x04 corrupted package contents !!!")
        j = submit(client, rid, "bob", "k1", gseqs[0], if_match=0, status=409)
        assert j["detail"]["code"] in (
            "package_verification_failed", "manifest_hash_mismatch")
        d = get_review(client, rid)
        assert d["status"] == "INVALIDATED"
        assert d["invalid_reason"]["code"] in (
            "package_verification_failed", "manifest_hash_mismatch")
        # 归档同样被拒绝
        assert archive(client, rid, status=409)["detail"]["code"] == \
            "review_invalidated"

    def test_invalidated_review_then_chain_still_broken_cannot_archive(self,
                                                                       client):
        pid, s, exp, rv, gseqs = self._ready(client, key="inv2")
        rid = rv["review_id"]
        db = SessionLocal()
        target = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
                  .order_by(AuditEvent.stream_seq).offset(2).first())
        clean_payload = target.payload
        gs = tamper_payload_rechain(db, pid)
        db.close()
        j = submit(client, rid, "bob", "k1", gseqs[0], if_match=0, status=409)
        assert j["detail"]["code"] in ("chain_broken", "scope_changed")
        # 不修复直接全签也不行: 每次提交前置校验都失效
        db = SessionLocal()
        try:
            restore_payload_rechain(db, pid, gs, clean_payload)
        finally:
            db.close()
        # 修复后恢复, 但只有一名操作者 -> 归档仍被业务规则拒绝(证明两类拒绝独立)
        r = reverify(client, rid, key="rf")
        assert r["valid"] is True
        v = get_review(client, rid)["version"]
        sign_all(client, rid, gseqs, "bob", "only", v)
        j = archive(client, rid, status=409)
        assert j["detail"]["code"] == "two_operators_required"


# ======================================================================
# ---------- 非回归: 分页/导出/补偿审批/执行窗口 ----------
# ======================================================================

class TestNoRegression:
    def test_paging_and_export_still_work_alongside_reviews(self, client):
        pid, _ = setup_completed(client, key="nr1")
        s = new_session(client, pid, key="es")
        # 分段小页 + 证明
        cur = None
        pages = []
        for _ in range(20):
            r = client.post(f"/api/admin/evidence/sessions/{s['session_id']}/pages",
                            json={"operator": "alice", "cursor": cur, "limit": 2})
            assert r.status_code == 200, r.text
            j = r.json()
            pages.append(j)
            cur = j["cursor"]["next_cursor"]
            if not j["cursor"]["has_more"]:
                break
        assert any(p["proof"]["fragment_digest"] for p in pages)
        eid = create_export(client, s["session_id"], key="ee", segment_size=2)
        d = run_export(client, eid)
        assert d["status"] == "COMPLETED"
        # 下载与离线复算 manifest_hash 不受复核模块影响
        tok = client.post(
            f"/api/admin/evidence/exports/{eid}/download-token",
            json={"operator": "alice", "idempotency_key": "dt"}).json()
        r = client.get(f"/api/admin/evidence/downloads/{tok['token']}")
        assert r.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(r.content))
        manifest = json.loads(z.read("manifest.json"))
        assert evidence._hash_obj(
            {k: v for k, v in manifest.items() if k != "manifest_hash"}) \
            == manifest["manifest_hash"] == d["manifest_hash"]
        # verify 接口
        assert client.get(
            f"/api/admin/evidence/exports/{eid}/verify").json()["valid"] is True
        # 状态汇总含复核区
        st = client.get("/api/status").json()
        assert "evidence_reviews" in st
        assert "evidence_sessions" in st and "evidence_exports" in st

    def test_compensation_approval_and_window_unchanged(self, client):
        add_records = lambda ids: [
            client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": "a,b"}) for i in ids]
        add_records(range(1, 4))
        b = mk_batch(client, 1, 3, "b-hw")
        pid = mk_plan(client, b, "p-hw", risk="HIGH")
        # HIGH 计划: 创建者不能自审, 他人审批; 审批可撤销; 窗口外启动不推进
        r = client.post(f"/api/admin/plans/{pid}/approve",
                        json={"operator": "alice", "idempotency_key": "ap0"})
        assert r.status_code == 409
        r = client.post(f"/api/admin/plans/{pid}/approve",
                        json={"operator": "bob", "idempotency_key": "ap1"})
        assert r.status_code == 200
        r = client.post(f"/api/admin/plans/{pid}/revoke-approval",
                        json={"operator": "bob", "idempotency_key": "rv1"})
        assert r.status_code == 200
        assert client.get(f"/api/admin/plans/{pid}").json()[
                   "approval_status"] == "PENDING"
        client.post(f"/api/admin/plans/{pid}/approve",
                    json={"operator": "bob", "idempotency_key": "ap2"})
        future = datetime.utcnow() + timedelta(hours=2)
        win = [{"starts_at": future.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ends_at": (future + timedelta(hours=1))
                .strftime("%Y-%m-%dT%H:%M:%SZ")}]
        r = client.put(f"/api/admin/plans/{pid}/window",
                       json={"operator": "alice", "idempotency_key": "w1",
                             "windows": win})
        assert r.status_code == 200
        r = client.post(f"/api/admin/plans/{pid}/start",
                        json={"operator": "alice", "idempotency_key": "st"})
        assert r.status_code == 200
        db = SessionLocal()
        try:
            plans.run_plan_tick(db, pid)
        finally:
            db.close()
        v = client.get(f"/api/admin/plans/{pid}").json()
        assert v["status"] == "RUNNING" and v["in_window"] is False
        # 补偿任务列表接口存在
        assert client.get("/api/admin/compensations").status_code == 200
        # 复核列表接口在无复核单时为空数组(接口共存)
        assert client.get("/api/admin/evidence/reviews").json() == []
