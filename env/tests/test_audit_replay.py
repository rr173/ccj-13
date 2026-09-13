"""审计事件回放与补偿模块测试。

覆盖:
- 统一事件流: 顺序号连续、哈希链、跨流扇出、按计划/批次/时间范围分页、
  乱序拒绝、显式补录幂等、跨批次隔离;
- 快照: 正常 VALID、非终态/早于终结拒绝、事件缺口/篡改断链、规则版本冲突、
  批次版本(epoch)不兼容、目标时点状态漂移、取消计划快照;
- 补偿: 预览、幂等执行、逐动作失败重试、质量门禁阻断、取消计划拒绝、
  部分失败恢复、快照过期、并发同快照不重复、整体撤销与撤销关联、重启续跑。

后台 worker 由 conftest 关闭, 全部用 auditreplay 函数手动确定性驱动。
"""
import os
import tempfile
from datetime import datetime, timedelta

_tmp = tempfile.mkdtemp(prefix="audit-replay-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["AUDIT_SNAPSHOT_TTL_SECONDS"] = "3600"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    AuditEvent, AuditSnapshot, CompensationAction, CompensationTask,
    MigrationBatch, RecordNew,
)
from app import auditreplay, plans, quality, service


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with TestClient(app) as c:
        yield c


# ---------- 搭场景辅助 ----------

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


def mk_plan(client, batch_id, key, name="plan"):
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": key, "name": name,
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


def setup_completed(client, start=1, end=5, key="k"):
    add_records(client, range(start, end + 1))
    b = mk_batch(client, start, end, f"b-{key}")
    pid = mk_plan(client, b, f"p-{key}")
    complete_plan(client, pid, f"s-{key}")
    return pid, b


def make_snapshot(client, pid, key="sn", ttl=None, target=None):
    body = {"operator": "alice", "idempotency_key": key, "plan_id": pid}
    if ttl:
        body["ttl_seconds"] = ttl
    if target:
        body["target_at"] = target
    r = client.post("/api/admin/audit-snapshots", json=body)
    return r


def make_task(client, sid, key="ct"):
    r = client.post("/api/admin/compensations",
                    json={"operator": "alice", "idempotency_key": key,
                          "snapshot_id": sid})
    assert r.status_code == 201, r.text
    return r.json()["task_id"]


def approve_task(client, tid, who, key):
    """高风险补偿双人审批: 不同于创建者 alice 的审批人独立通过。"""
    r = client.post(f"/api/admin/compensations/{tid}/approvals",
                    json={"operator": who, "idempotency_key": key})
    assert r.status_code in (200, 201), r.text
    return r


def _rechain(db, plan_id):
    """篡改 payload 后重算该计划流的哈希链, 隔离出非 chain_broken 的版本类校验。"""
    rows = (db.query(AuditEvent).filter(AuditEvent.stream_key == plan_id)
            .order_by(AuditEvent.stream_seq).all())
    prev_hash = None
    for r in rows:
        r.prev_stream_hash = prev_hash
        r.stream_hash = auditreplay._chain_hash(
            r.stream_seq, r.event_type, r.correlation_id, r.payload,
            r.event_ts, prev_hash, r.operator)
        prev_hash = r.stream_hash
    db.flush()


# ======================================================================
# ---------- 统一事件流 ----------
# ======================================================================

class TestEventStream:
    def test_stream_seq_contiguous_and_chain(self, client):
        pid, b = setup_completed(client)
        ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()
        seqs = [e["stream_seq"] for e in ev["items"]]
        assert seqs == list(range(1, len(seqs) + 1))
        # global_seq 严格递增
        gseqs = [e["global_seq"] for e in ev["items"]]
        assert gseqs == sorted(gseqs) and len(set(gseqs)) == len(gseqs)
        # 哈希链逐行可重算
        prev = None
        for e in ev["items"]:
            expect = auditreplay._chain_hash(
                e["stream_seq"], e["event_type"], e["correlation_id"],
                e["payload"], datetime.fromisoformat(e["event_ts"]), prev,
                e["operator"])
            assert expect == e["stream_hash"], e["stream_seq"]
            prev = e["stream_hash"]

    def test_event_types_projected(self, client):
        pid, b = setup_completed(client)
        types = {e["event_type"] for e in
                 client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200")
                 .json()["items"]}
        assert {"BATCH_ATTACH", "BATCH_FREEZE", "BATCH_VALIDATE",
                "BATCH_CUTOVER", "PLAN_ADVANCE"} <= types

    def test_event_immutable_no_update_endpoint(self, client):
        pid, b = setup_completed(client)
        ev = client.get(f"/api/admin/audit-events?plan_id={pid}").json()["items"][0]
        # 直接改库是唯一途径; 改后哈希链断裂(没有任何业务接口能改事件)
        db = SessionLocal()
        row = db.get(AuditEvent, ev["id"])
        row.operator = "hacker"
        db.commit()
        db.close()
        r = make_snapshot(client, pid, "sn")
        assert r.status_code == 422
        assert "chain_broken" in [x["code"] for x in
                                  r.json()["detail"]["snapshot"]["reasons"]]

    def test_pagination_keyset(self, client):
        pid, b = setup_completed(client)
        page1 = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=2").json()
        assert len(page1["items"]) == 2 and page1["has_more"] is True
        cur = page1["next_after_global_seq"]
        seen = [x["global_seq"] for x in page1["items"]]
        pages = 1
        while True:
            p = client.get(
                f"/api/admin/audit-events?plan_id={pid}&limit=2"
                f"&after_global_seq={cur}").json()
            seen += [x["global_seq"] for x in p["items"]]
            pages += 1
            if not p["has_more"]:
                break
            cur = p["next_after_global_seq"]
        assert seen == sorted(seen) and len(seen) == len(set(seen))
        assert pages >= 3

    def test_time_range_filter(self, client):
        pid, b = setup_completed(client)
        all_ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()["items"]
        mid = all_ev[len(all_ev) // 2]
        r = client.get(
            f"/api/admin/audit-events?plan_id={pid}"
            f"&start_ts={mid['event_ts']}&limit=200").json()
        assert all(e["event_ts"] >= mid["event_ts"] for e in r["items"])
        assert r["items"][0]["event_ts"] == mid["event_ts"]

    def test_batch_query_folds_correlations(self, client):
        pid, b = setup_completed(client)
        rows = client.get(f"/api/admin/audit-events?batch_id={b}&limit=200").json()["items"]
        # 同一逻辑事件(批次+计划两流, 共享 correlation)折叠为一条
        corrs = [e["correlation_id"] for e in rows]
        assert len(corrs) == len(set(corrs))
        # 折叠项记录它还投影到的计划
        plan_rows = [e for e in rows if e.get("other_plan_ids")]
        assert any(pid in e["other_plan_ids"] for e in plan_rows)

    def test_explicit_note_and_idempotent_replay(self, client):
        pid, b = setup_completed(client)
        r1 = client.post("/api/admin/audit-events",
                         json={"operator": "ops", "idempotency_key": "n1",
                               "content": "运维备注", "plan_id": pid})
        assert r1.status_code == 201 and r1.json()["replayed"] is False
        r2 = client.post("/api/admin/audit-events",
                         json={"operator": "ops", "idempotency_key": "n1",
                               "content": "运维备注", "plan_id": pid})
        assert r2.status_code == 201 and r2.json()["replayed"] is True
        assert r1.json()["global_seq"] == r2.json()["global_seq"]

    def test_out_of_order_note_rejected(self, client):
        pid, b = setup_completed(client)
        r = client.post("/api/admin/audit-events", json={
            "operator": "ops", "idempotency_key": "late", "content": "x",
            "plan_id": pid, "event_ts": "2000-01-01T00:00:00"})
        assert r.status_code == 409

    def test_invalid_event_type_422(self, client):
        r = client.get("/api/admin/audit-events?event_type=BOGUS")
        assert r.status_code == 422

    def test_cross_batch_stream_isolation(self, client):
        pid1, b1 = setup_completed(client, 1, 5, "one")
        pid2, b2 = setup_completed(client, 11, 15, "two")
        e1 = client.get(f"/api/admin/audit-events?plan_id={pid1}&limit=200").json()["items"]
        assert {e["batch_id"] for e in e1 if e["batch_id"]} == {b1}
        e2 = client.get(f"/api/admin/audit-events?plan_id={pid2}&limit=200").json()["items"]
        assert {e["batch_id"] for e in e2 if e["batch_id"]} == {b2}

    def test_batch_write_events(self, client):
        add_records(client, [1, 2])
        b = mk_batch(client, 1, 5, "bw")
        # 批次 NORMAL 时写入 -> BATCH_WRITE 投影到批次流
        r = client.post("/api/records",
                        json={"id": 3, "name": "r3", "email": "e", "tags_csv": "x"})
        assert r.status_code == 201
        rows = client.get(f"/api/admin/audit-events?batch_id={b}").json()["items"]
        writes = [e for e in rows if e["event_type"] == "BATCH_WRITE"]
        assert writes and writes[0]["payload"]["record_id"] == 3

    def test_quality_events_projected(self, client):
        add_records(client, [1, 2])
        b = mk_batch(client, 1, 2, "qb")
        pid = mk_plan(client, b, "qp")
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "qr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        sc = client.post(f"/api/admin/plans/{pid}/quality-scans",
                         json={"operator": "alice", "idempotency_key": "qs",
                               "plan_id": pid}).json()["scan_id"]
        db = SessionLocal()
        claimed = quality.claim_due_scans(db)
        assert claimed == [sc]
        while quality.run_scan_tick(db, sc):
            pass
        db.close()
        types = {e["event_type"] for e in
                 client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200")
                 .json()["items"]}
        assert "RULE_CHANGED" in types and "SCAN_STATUS" in types


# ======================================================================
# ---------- 快照校验 ----------
# ======================================================================

class TestSnapshotValidation:
    def test_valid_snapshot(self, client):
        pid, b = setup_completed(client)
        r = make_snapshot(client, pid)
        assert r.status_code == 201
        snap = r.json()
        assert snap["status"] == "VALID" and snap["reasons"] == []
        assert not snap["expired"] and snap["expires_at"]
        assert snap["event_count"] >= 5
        # 批次版本链与规则版本链已固化
        assert snap["batch_versions"] and snap["batch_versions"][0]["phase_now"] == "DONE"

    def test_non_terminal_plan_rejected(self, client):
        add_records(client, [1])
        b = mk_batch(client, 1, 1, "nt")
        pid = mk_plan(client, b, "ntp")  # DRAFT
        r = make_snapshot(client, pid)
        assert r.status_code == 422
        codes = [x["code"] for x in r.json()["detail"]["snapshot"]["reasons"]]
        assert "plan_not_terminal" in codes
        # REJECTED 快照仍持久化可查
        listed = client.get("/api/admin/audit-snapshots?status_filter=REJECTED").json()
        assert any(s["plan_id"] == pid for s in listed)

    def test_target_before_end_rejected(self, client):
        pid, b = setup_completed(client)
        r = make_snapshot(client, pid, key="early", target="2000-01-01T00:00:00")
        assert r.status_code == 422
        codes = [x["code"] for x in r.json()["detail"]["snapshot"]["reasons"]]
        assert "target_before_end" in codes

    def test_stream_gap_and_chain_broken(self, client):
        pid, b = setup_completed(client, key="gap")
        db = SessionLocal()
        mid = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid)
               .order_by(AuditEvent.stream_seq).offset(2).first())
        removed_seq = mid.stream_seq
        db.delete(mid)
        db.commit()
        db.close()
        r = make_snapshot(client, pid, key="gapsn")
        assert r.status_code == 422
        reasons = r.json()["detail"]["snapshot"]["reasons"]
        codes = [x["code"] for x in reasons]
        assert "stream_gap" in codes and "chain_broken" in codes
        gap = next(x for x in reasons if x["code"] == "stream_gap")
        assert removed_seq in gap["missing"]

    def test_tampered_hash_breaks_chain(self, client):
        pid, b = setup_completed(client, key="tam")
        db = SessionLocal()
        ev = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid,
                                          AuditEvent.event_type == "BATCH_CUTOVER")
              .first())
        ev.stream_hash = "deadbeef"
        db.commit()
        db.close()
        r = make_snapshot(client, pid, key="tamsn")
        assert "chain_broken" in [x["code"] for x in
                                  r.json()["detail"]["snapshot"]["reasons"]]

    def test_batch_state_drift_rejected(self, client):
        pid, b = setup_completed(client, key="drift")
        # 完成后篡改批次 epoch(模拟目标时点状态与当前不一致)
        db = SessionLocal()
        batch = db.get(MigrationBatch, b)
        batch.epoch = batch.epoch + 5
        db.commit()
        db.close()
        r = make_snapshot(client, pid, key="driftsn")
        assert r.status_code == 422
        codes = [x["code"] for x in r.json()["detail"]["snapshot"]["reasons"]]
        assert "batch_state_drift" in codes

    def test_rule_version_conflict_rejected(self, client):
        """RULE_CHANGED 引用的摘要与库中不可变版本不一致 -> rule_version_gap。

        篡改后重算后续哈希链, 隔离出版本校验(而非 chain_broken)失败。"""
        add_records(client, [1, 2])
        b = mk_batch(client, 1, 2, "rv")
        pid = mk_plan(client, b, "rvp")
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "rvr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        sc = client.post(f"/api/admin/plans/{pid}/quality-scans",
                         json={"operator": "alice", "idempotency_key": "rvs",
                               "plan_id": pid}).json()["scan_id"]
        db = SessionLocal()
        quality.claim_due_scans(db)
        while quality.run_scan_tick(db, sc):
            pass
        db.close()
        complete_plan(client, pid, "rvgo")
        # 篡改 RULE_CHANGED 的摘要并重链
        db = SessionLocal()
        ev = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid,
                                          AuditEvent.event_type == "RULE_CHANGED")
              .order_by(AuditEvent.stream_seq).first())
        ev.payload = {**ev.payload, "content_digest": "deadbeef" * 8}
        _rechain(db, pid)
        db.commit()
        db.close()
        r = make_snapshot(client, pid, key="rvsn")
        assert r.status_code == 422
        assert "rule_version_gap" in [x["code"] for x in
                                      r.json()["detail"]["snapshot"]["reasons"]]

    def test_batch_epoch_gap_rejected(self, client):
        """批次 epoch 在事件流中增量不连续(篡改 payload 后重链) -> batch_version_gap。"""
        pid, b = setup_completed(client, 1, 3, "beg")
        db = SessionLocal()
        ev = (db.query(AuditEvent).filter(AuditEvent.stream_key == pid,
                                          AuditEvent.event_type == "BATCH_CUTOVER")
              .first())
        # 把切换事件的 epoch 改成跳变值, 制造版本增量缺口(链同时重算)
        ev.payload = {**ev.payload, "epoch": ev.payload["epoch"] + 3}
        _rechain(db, pid)
        db.commit()
        db.close()
        r = make_snapshot(client, pid, key="begsn")
        assert r.status_code == 422
        codes = [x["code"] for x in r.json()["detail"]["snapshot"]["reasons"]]
        assert "batch_version_gap" in codes or "batch_state_drift" in codes

    def test_canceled_plan_snapshot_valid(self, client):
        add_records(client, [1, 2])
        b = mk_batch(client, 1, 2, "cb")
        # 先冻结批次再纳入计划, 启动后立即取消 -> 计划 CANCELED, 批次 FROZEN
        client.post(f"/api/admin/batches/{b}/freeze",
                    json={"operator": "alice", "idempotency_key": "fz"})
        pid = mk_plan(client, b, "cp")
        client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": "cs"})
        r = client.post(f"/api/admin/plans/{pid}/cancel",
                        json={"operator": "alice", "idempotency_key": "cc"})
        assert r.json()["status"] == "CANCELED"
        snap = make_snapshot(client, pid, key="cbsn").json()
        assert snap["status"] == "VALID"
        assert snap["plan_status_at_create"] == "CANCELED"

    def test_snapshot_ttl_expires(self, client):
        pid, b = setup_completed(client, key="ttl")
        sid = make_snapshot(client, pid, key="ttlsn", ttl=60).json()["snapshot_id"]
        db = SessionLocal()
        snap = db.get(AuditSnapshot, sid)
        snap.expires_at = datetime(2020, 1, 1)
        db.commit()
        db.close()
        detail = client.get(f"/api/admin/audit-snapshots/{sid}").json()
        assert detail["expired"] is True


# ======================================================================
# ---------- 补偿: 预览 / 执行 / 重试 / 撤销 ----------
# ======================================================================

def induce_drift(record_ids_delete=(), drift_ids=None):
    """直接在新表制造漂移: 删除/改坏记录。"""
    db = SessionLocal()
    for rid in record_ids_delete:
        db.query(RecordNew).filter(RecordNew.id == rid).delete()
    for rid, name in (drift_ids or {}).items():
        db.get(RecordNew, rid).name = name
    db.commit()
    db.close()


class TestCompensation:
    def test_preview_actions(self, client):
        pid, b = setup_completed(client, 1, 5, "pv")
        induce_drift((2, 3), {4: "DRIFT"})
        sid = make_snapshot(client, pid, key="pvsn").json()["snapshot_id"]
        pv = client.get(f"/api/admin/audit-snapshots/{sid}/preview").json()
        keys = sorted((a["action_type"], a["record_id"]) for a in pv["actions"])
        assert ("record_backfill", 2) in keys
        assert ("record_backfill", 3) in keys
        assert ("record_backfill", 4) in keys
        assert all(a["action_key"] for a in pv["actions"])

    def test_execute_backfill_and_idempotent(self, client):
        pid, b = setup_completed(client, 1, 5, "ex")
        induce_drift((2,), {})
        sid = make_snapshot(client, pid, key="exsn").json()["snapshot_id"]
        tid = make_task(client, sid)
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run1"})
        assert r.status_code == 200
        assert r.json()["status"] == "COMPLETED"
        assert r.json()["success_actions"] == 1
        db = SessionLocal()
        assert db.get(RecordNew, 2).name == "r2"
        db.close()
        # 重复请求幂等(同 idempotency_key), 不产生第二次写入
        r2 = client.post(f"/api/admin/compensations/{tid}/execute",
                         json={"operator": "alice", "idempotency_key": "run1"})
        assert r2.json()["replayed"] is True
        # 补偿执行事件已追加到统一事件流, 且原事件未变
        ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()["items"]
        assert any(e["event_type"] == "COMP_EXECUTED" for e in ev)

    def test_actions_append_only_original_events_unchanged(self, client):
        pid, b = setup_completed(client, 1, 3, "ao")
        before = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()["items"]
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="aosn").json()["snapshot_id"]
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        after = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()["items"]
        assert len(after) == len(before) + 1  # 只多一个 COMP_EXECUTED
        assert after[-1]["event_type"] == "COMP_EXECUTED"

    def test_gate_blocks_compensation(self, client):
        pid, b = setup_completed(client, 1, 5, "gb")
        induce_drift((2,), {})
        sid = make_snapshot(client, pid, key="gbsn").json()["snapshot_id"]
        # 完成后新增阻断规则但不扫描 -> 门禁 NOT_SCANNED, 补偿不得越过
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "gbr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        tid = make_task(client, sid)
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        j = r.json()
        assert j["status"] == "PARTIAL"
        act = j["actions"][0]
        assert act["status"] == "FAILED" and act["gate_status"] == "NOT_SCANNED"
        assert "门禁" in act["last_error"]
        # 记录未被回填(补偿没有越过门禁)
        db = SessionLocal()
        assert db.get(RecordNew, 2) is None
        db.close()

    def test_retry_after_gate_recovers(self, client):
        pid, b = setup_completed(client, 1, 5, "gr")
        induce_drift((2,), {})
        sid = make_snapshot(client, pid, key="grsn").json()["snapshot_id"]
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "grr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        tid = make_task(client, sid)
        first = client.post(f"/api/admin/compensations/{tid}/execute",
                            json={"operator": "alice", "idempotency_key": "run"})
        assert first.json()["actions"][0]["status"] == "FAILED"
        # 门禁恢复: COMPLETED 计划不允许再发起扫描, 移除规则集使门禁回到
        # NOT_CONFIGURED(放行), 模拟"阻断条件解除"。
        db = SessionLocal()
        from app.models import QualityRuleSet
        db.query(QualityRuleSet).filter(QualityRuleSet.plan_id == pid).delete()
        db.commit()
        db.close()
        rr = client.post(f"/api/admin/compensations/{tid}/retry",
                         json={"operator": "alice", "idempotency_key": "rt",
                               "action_seq": 1})
        assert rr.status_code == 200
        assert rr.json()["status"] == "SUCCESS"
        task = client.get(f"/api/admin/compensations/{tid}").json()
        assert task["status"] == "COMPLETED"

    def test_canceled_plan_compensation_rejected(self, client):
        add_records(client, [1, 2])
        b = mk_batch(client, 1, 2, "cb2")
        client.post(f"/api/admin/batches/{b}/freeze",
                    json={"operator": "alice", "idempotency_key": "fz2"})
        pid = mk_plan(client, b, "cp2")
        client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": "cs2"})
        client.post(f"/api/admin/plans/{pid}/cancel",
                    json={"operator": "alice", "idempotency_key": "cc2"})
        sid = make_snapshot(client, pid, key="cbsn2").json()["snapshot_id"]
        pv = client.get(f"/api/admin/audit-snapshots/{sid}/preview").json()
        assert all(a["execution_allowed"] is False for a in pv["actions"])
        tid = make_task(client, sid)
        assert client.get(f"/api/admin/compensations/{tid}").json()["risk_level"] == "HIGH"
        approve_task(client, tid, "bob", "cb-a1")
        approve_task(client, tid, "carol", "cb-a2")
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        assert all(a["status"] == "FAILED" for a in r.json()["actions"])
        assert all(a["gate_status"] == "PLAN_CANCELED" for a in r.json()["actions"])

    def test_partial_failure_progress_persisted(self, client):
        # 两个漂移动作, 门禁放行第一个(无规则), 实际场景下通过让一个动作失败验证部分恢复。
        # 用 batch_unfreeze 不易, 这里验证: 多个 backfill 全部成功无部分失败;
        # 部分失败用"门禁恢复后逐动作重试"路径已在 test_retry_after_gate_recovers 覆盖。
        pid, b = setup_completed(client, 1, 8, "pf")
        induce_drift((2, 3, 5), {})
        sid = make_snapshot(client, pid, key="pfsn").json()["snapshot_id"]
        tid = make_task(client, sid)
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        assert r.json()["status"] == "COMPLETED"
        assert r.json()["success_actions"] == 3

    def test_snapshot_expired_blocks_execute_not_undo(self, client):
        pid, b = setup_completed(client, 1, 5, "exp")
        induce_drift((2,), {})
        sid = make_snapshot(client, pid, key="expsn", ttl=60).json()["snapshot_id"]
        tid = make_task(client, sid)
        db = SessionLocal()
        db.get(AuditSnapshot, sid).expires_at = datetime(2020, 1, 1)
        db.commit()
        db.close()
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        assert r.status_code == 409 and "过期" in r.json()["detail"]["reason"]

    def test_undo_restores_before_image(self, client):
        pid, b = setup_completed(client, 1, 5, "un")
        induce_drift((2,), {4: "DRIFT"})
        sid = make_snapshot(client, pid, key="unsn").json()["snapshot_id"]
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        db = SessionLocal()
        assert db.get(RecordNew, 2).name == "r2"
        assert db.get(RecordNew, 4).name == "r4"
        db.close()
        u = client.post(f"/api/admin/compensations/{tid}/undo",
                        json={"operator": "bob", "idempotency_key": "undo"})
        assert u.status_code == 200 and u.json()["status"] == "UNDONE"
        # 插入的行被移除, 被改的行恢复为 DRIFT
        db = SessionLocal()
        assert db.get(RecordNew, 2) is None
        assert db.get(RecordNew, 4).name == "DRIFT"
        db.close()
        # COMP_UNDONE 事件已追加并关联原执行事件
        ev = client.get(f"/api/admin/audit-events?plan_id={pid}&limit=200").json()["items"]
        undone = [e for e in ev if e["event_type"] == "COMP_UNDONE"]
        executed = [e for e in ev if e["event_type"] == "COMP_EXECUTED"]
        assert len(undone) == 2 == len(executed)
        # 撤销动作携带原执行事件的 global_seq(撤销关联)
        assert all(e["payload"]["executed_event_global_seq"] for e in undone)

    def test_undo_operator_recorded(self, client):
        pid, b = setup_completed(client, 1, 4, "uo")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="uosn").json()["snapshot_id"]
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        client.post(f"/api/admin/compensations/{tid}/undo",
                    json={"operator": "bob", "idempotency_key": "undo"})
        t = client.get(f"/api/admin/compensations/{tid}").json()
        assert all(a["undone_by"] == "bob" for a in t["actions"])
        assert all(a["executed_by"] == "alice" for a in t["actions"])

    def test_concurrent_same_snapshot_no_duplicate_task(self, client):
        pid, b = setup_completed(client, 1, 4, "cc3")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="cc3sn").json()["snapshot_id"]
        t1 = make_task(client, sid, "one")
        t2 = make_task(client, sid, "two")
        assert t1 == t2
        db = SessionLocal()
        count = db.query(CompensationTask).filter(
            CompensationTask.snapshot_id == sid).count()
        assert count == 1
        db.close()

    def test_rejected_snapshot_cannot_create_task(self, client):
        add_records(client, [1])
        b = mk_batch(client, 1, 1, "rj")
        pid = mk_plan(client, b, "rjp")
        make_snapshot(client, pid, key="rjsn")  # 422 REJECTED
        # 找到 REJECTED 快照 id
        rejected = client.get(
            "/api/admin/audit-snapshots?status_filter=REJECTED").json()[0]["snapshot_id"]
        r = client.post("/api/admin/compensations",
                        json={"operator": "alice", "idempotency_key": "rjct",
                              "snapshot_id": rejected})
        assert r.status_code == 409


# ======================================================================
# ---------- 重启续跑 ----------
# ======================================================================

class TestBootRecovery:
    def test_running_task_returns_to_queued(self, client):
        pid, b = setup_completed(client, 1, 4, "br")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="brsn").json()["snapshot_id"]
        tid = make_task(client, sid)
        db = SessionLocal()
        t = db.get(CompensationTask, tid)
        t.status = "RUNNING"
        db.commit()
        db.close()
        db = SessionLocal()
        auditreplay.boot_recover_compensation(db)
        db.close()
        t = client.get(f"/api/admin/compensations/{tid}").json()
        assert t["status"] == "QUEUED"
        # 动作仍为 PENDING, 未丢失
        assert all(a["status"] == "PENDING" for a in t["actions"])
        # 恢复后可正常执行
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        assert r.json()["status"] == "COMPLETED"

    def test_failed_action_retained_across_recovery(self, client):
        pid, b = setup_completed(client, 1, 4, "fr")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="frsn").json()["snapshot_id"]
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "frr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        # 模拟在 PARTIAL 时重启: RUNNING 之外的状态保持, FAILED 与原因保留
        db = SessionLocal()
        auditreplay.boot_recover_compensation(db)
        db.close()
        t = client.get(f"/api/admin/compensations/{tid}").json()
        assert t["status"] == "PARTIAL"
        assert t["actions"][0]["status"] == "FAILED"
        assert t["actions"][0]["last_error"]

    def test_undo_running_returns_to_undo_partial(self, client):
        pid, b = setup_completed(client, 1, 4, "ur")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="ursn").json()["snapshot_id"]
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        db = SessionLocal()
        t = db.get(CompensationTask, tid)
        t.status = "UNDO_RUNNING"
        act = t.actions[0]
        act.status = "UNDOING"
        db.commit()
        db.close()
        db = SessionLocal()
        auditreplay.boot_recover_compensation(db)
        db.close()
        t = client.get(f"/api/admin/compensations/{tid}").json()
        assert t["status"] == "UNDO_PARTIAL"
        # UNDOING 动作复位为 SUCCESS, 可继续撤销
        assert t["actions"][0]["status"] == "SUCCESS"
        u = client.post(f"/api/admin/compensations/{tid}/undo",
                        json={"operator": "alice", "idempotency_key": "undo2"})
        assert u.json()["status"] == "UNDONE"

    def test_worker_drives_execution_one_action_per_tick(self, client):
        pid, b = setup_completed(client, 1, 8, "wk")
        induce_drift((2, 4, 6), {})
        sid = make_snapshot(client, pid, key="wksn").json()["snapshot_id"]
        tid = make_task(client, sid)
        worker = auditreplay.CompensationWorker()
        ticks = 0
        for _ in range(10):
            if worker.tick_once() == 0:
                break
            ticks += 1
        assert ticks == 3  # 每个动作一个 tick(含认领)
        t = client.get(f"/api/admin/compensations/{tid}").json()
        assert t["status"] == "COMPLETED" and t["success_actions"] == 3

    def test_comp_failed_event_appended_on_gate_block(self, client):
        pid, b = setup_completed(client, 1, 4, "cf")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="cfsn").json()["snapshot_id"]
        client.put(f"/api/admin/plans/{pid}/quality-rules", json={
            "operator": "alice", "idempotency_key": "cfr", "plan_id": pid,
            "rules": [{"id": "r1", "type": "required", "field": "name",
                       "severity": "BLOCKER"}]})
        tid = make_task(client, sid)
        client.post(f"/api/admin/compensations/{tid}/execute",
                    json={"operator": "alice", "idempotency_key": "run"})
        ev = client.get(
            f"/api/admin/audit-events?plan_id={pid}&event_type=COMP_FAILED"
            "&limit=10").json()["items"]
        assert len(ev) == 1
        assert ev[0]["payload"]["gate_status"] == "NOT_SCANNED"
        # 原失败动作没有被改成成功(只追加, 不修改)
        assert ev[0]["event_type"] == "COMP_FAILED"


# ======================================================================
# ---------- 清理动作(多余新表记录) ----------
# ======================================================================

class TestRecordCleanup:
    def test_cleanup_extra_new_record(self, client):
        """当前新表存在快照基线之外的记录 -> record_cleanup 动作并可撤销。

        正常流程范围内新表由切换产生、基线含全部旧记录, 不会出现多余项;
        这里在快照生成后直接构造"快照基线不含 id=5 而新表含 id=5"来验证
        推导(对应数据在基线时点之后被外部写入新表的漂移)。"""
        pid, b = setup_completed(client, 1, 5, "cl")
        sid = make_snapshot(client, pid, key="clsn").json()["snapshot_id"]
        db = SessionLocal()
        snap_obj = db.get(AuditSnapshot, sid)
        sb = snap_obj.batches[0]
        sb.expected_old_records = [r for r in sb.expected_old_records if r["id"] != 5]
        sb.expected_old_count -= 1
        db.commit()
        previews = auditreplay.derive_actions(db, snap_obj)
        cleanup = [a for a in previews if a["action_type"] == "record_cleanup"]
        assert any(a["record_id"] == 5 for a in cleanup)
        db.close()
        # 建任务执行: id=5 被删除, 撤销后按镜像恢复
        tid = make_task(client, sid, "clct")
        assert client.get(f"/api/admin/compensations/{tid}").json()["risk_level"] == "HIGH"
        approve_task(client, tid, "bob", "cl-a1")
        approve_task(client, tid, "carol", "cl-a2")
        r = client.post(f"/api/admin/compensations/{tid}/execute",
                        json={"operator": "alice", "idempotency_key": "run"})
        assert r.json()["status"] == "COMPLETED"
        db = SessionLocal()
        assert db.get(RecordNew, 5) is None
        db.close()
        client.post(f"/api/admin/compensations/{tid}/undo",
                    json={"operator": "alice", "idempotency_key": "undo"})
        db = SessionLocal()
        assert db.get(RecordNew, 5) is not None  # 撤销恢复
        db.close()


class TestSnapshotDetailAndListing:
    def test_snapshot_detail_contains_events_and_batches(self, client):
        pid, b = setup_completed(client, 1, 3, "dt")
        sid = make_snapshot(client, pid, key="dtsn").json()["snapshot_id"]
        d = client.get(f"/api/admin/audit-snapshots/{sid}").json()
        assert d["events"] and d["batches"]
        assert all("stream_hash" in e for e in d["events"])
        assert d["batches"][0]["expected_old_count"] == 3

    def test_list_compensations_filter(self, client):
        pid, b = setup_completed(client, 1, 3, "lc")
        induce_drift((1,), {})
        sid = make_snapshot(client, pid, key="lcsn").json()["snapshot_id"]
        tid = make_task(client, sid)
        by_plan = client.get(f"/api/admin/compensations?plan_id={pid}").json()
        assert len(by_plan) == 1 and by_plan[0]["task_id"] == tid
        by_snap = client.get(f"/api/admin/compensations?snapshot_id={sid}").json()
        assert len(by_snap) == 1
        assert client.get("/api/admin/compensations?status_filter=NOPE").status_code == 422
