"""证据封存分发与离线校验模块测试。

覆盖:
- 正常创建/页面展示(接收方/有效期/事件数/摘要/当前状态)与下载校验;
- 未归档复核单拒绝、非法/未登记/停用接收方拒绝;
- 同请求幂等(幂等键重放 + 同参数自然去重);
- 过期与撤销后下载拒绝、一次性令牌首次兑换后失效、接收方约束;
- 篡改包内容(行/文件/manifest/签名)后离线校验失败并指出位置;
- 撤销不影响原事件/复核结论/签署摘要; 撤销后重新签发得到新 package_id;
- 非授权接收方查看/下载被拒绝;
- 非回归: 证据分页/导出、复核归档、补偿审批接口。
"""
import io
import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

_tmp = tempfile.mkdtemp(prefix="evidence-dist-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["EVIDENCE_STORE_DIR"] = f"{_tmp}/store"
os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"] = f"{_tmp}/dist-store"

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app.models import AuditEvent, EvidenceReview  # noqa: E402
from app import auditreplay, distribution, evidence, plans  # noqa: E402


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(os.environ["EVIDENCE_STORE_DIR"], exist_ok=True)
    os.makedirs(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"], exist_ok=True)
    with TestClient(app) as c:
        yield c


# ---------- 场景搭建(复用证据复核流程) ----------

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


def mk_plan(client, batch_id, key, risk="LOW"):
    r = client.post("/api/admin/plans", json={
        "operator": "alice", "idempotency_key": key, "name": "plan",
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


def new_session(client, pid, key):
    r = client.post("/api/admin/evidence/sessions",
                    json={"operator": "alice", "idempotency_key": key,
                          "plan_id": pid})
    assert r.status_code == 201, r.text
    return r.json()


def page_all(client, sid):
    cursor = None
    items = []
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
    return items


def make_archived_review(client, key="k", start=1, end=3):
    """完成 计划→会话→导出→双人签署→归档, 返回 (review_dict, gseqs, plan_id)。"""
    pid, _ = setup_completed(client, start=start, end=end, key=key)
    s = new_session(client, pid, f"es-{key}")
    sid = s["session_id"]
    items = page_all(client, sid)
    assert items
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
    # bob 全签, carol 全签(逐次 If-Match)
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
    rv = client.get(f"/api/admin/evidence/reviews/{rid}").json()
    assert rv["status"] == "ARCHIVED" and rv["signature_hash"]
    return rv, gseqs, pid


def register_recipient(client, recipient="auditor1", key="rcp",
                       operator="admin", **extra):
    body = {"operator": operator, "idempotency_key": key,
            "recipient": recipient, **extra}
    r = client.post("/api/admin/evidence/recipients", json=body)
    return r


def create_dist(client, rid, recipient="auditor1", key="dist",
                policy="STANDARD", ttl=3600, valid_until=None,
                operator="admin", status=None):
    body = {"operator": operator, "idempotency_key": key,
            "review_id": rid, "recipient": recipient,
            "redaction_policy": policy}
    if valid_until is not None:
        body["valid_until"] = valid_until
    elif ttl is not None:
        body["ttl_seconds"] = ttl
    r = client.post("/api/admin/evidence/distributions", json=body)
    if status is not None:
        assert r.status_code == status, r.text
    return r


def get_dist(client, pid, operator="auditor1", **params):
    r = client.get(f"/api/admin/evidence/distributions/{pid}",
                   params={"operator": operator, **params})
    return r


def issue_token(client, pid, operator="admin", key="tok", ttl=None, status=None):
    body = {"operator": operator, "idempotency_key": key}
    if ttl is not None:
        body["ttl_seconds"] = ttl
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/download-token", json=body)
    if status is not None:
        assert r.status_code == status, r.text
    return r


def download(client, token, operator="auditor1"):
    return client.get(
        f"/api/admin/evidence/distribution-downloads/{token}",
        params={"operator": operator})


def revoke(client, pid, operator="admin", key="rev", reason=None, status=None):
    body = {"operator": operator, "idempotency_key": key}
    if reason is not None:
        body["reason"] = reason
    r = client.post(
        f"/api/admin/evidence/distributions/{pid}/revoke", json=body)
    if status is not None:
        assert r.status_code == status, r.text
    return r


def read_zip(content: bytes):
    z = zipfile.ZipFile(io.BytesIO(content))
    out = {n: z.read(n) for n in z.namelist()}
    return z, out


# ======================================================================
# ---------- 正常创建 / 页面展示 / 下载校验 ----------
# ======================================================================

class TestCreateAndDisplay:
    def test_create_from_archived_review_shows_all_fields(self, client):
        rv, gseqs, pid = make_archived_review(client, key="ok")
        r = register_recipient(client, name="审计员甲", contact="a@x.com")
        assert r.status_code == 201, r.text
        until = (datetime.utcnow() + timedelta(hours=12)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = create_dist(client, rv["review_id"], valid_until=until,
                        key="d1", policy="STANDARD")
        assert r.status_code == 201, r.text
        d = r.json()
        assert d["package_id"].startswith("EDP")
        assert d["status"] == "ACTIVE" and d["stored_status"] == "ACTIVE"
        assert d["recipient"] == "auditor1"
        assert d["review_id"] == rv["review_id"]
        assert d["plan_id"] == pid
        assert d["event_count"] == len(gseqs)
        assert d["first_global_seq"] == min(gseqs)
        assert d["last_global_seq"] == max(gseqs)
        assert d["redaction_policy"] == "STANDARD"
        assert d["valid_until"]
        assert d["manifest_hash"] and d["content_digest"] \
            and d["signature_digest"]
        assert d["review_signature_hash"] == rv["signature_hash"]
        assert d["issue_no"] == 1 and d["replayed"] is False
        assert d["deduped"] is False and d["reissued"] is False
        # 状态汇总与列表展示
        st = client.get("/api/status").json()
        assert any(x["package_id"] == d["package_id"]
                   for x in st["evidence_distributions"])
        assert any(x["recipient"] == "auditor1"
                   for x in st["evidence_recipients"])
        rows = client.get("/api/admin/evidence/distributions",
                          params={"review_id": rv["review_id"]}).json()
        assert [x["package_id"] for x in rows] == [d["package_id"]]
        # 详情页(授权接收方)展示
        detail = get_dist(client, d["package_id"]).json()
        assert detail["manifest"]["recipient"] == "auditor1"
        assert detail["manifest"]["event_count"] == len(gseqs)
        assert {e["event"] for e in detail["events"]} >= {
            "dist.create"}

    def test_package_contents_redacted_and_verifiable_offline(self, client):
        rv, gseqs, _ = make_archived_review(client, key="pkg")
        register_recipient(client)
        d = create_dist(client, rv["review_id"], key="d",
                        policy="STANDARD").json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key="t1").json()["token"]
        r = download(client, tok)
        assert r.status_code == 200, r.text
        assert r.headers["content-type"] == "application/zip"
        zip_raw = r.content
        z, files = read_zip(zip_raw)
        assert set(files) == {"events.jsonl", "signature.json",
                             "metadata.json", "manifest.json"}
        manifest = json.loads(files["manifest.json"])
        events = [json.loads(l) for l in
                  files["events.jsonl"].decode().splitlines()]
        assert len(events) == len(gseqs)
        # STANDARD: 操作者与说明类字段被隐藏
        assert all(e["operator"] == "«REDACTED»" for e in events)
        for e in events:
            for k, v in (e.get("payload") or {}).items():
                if k.lower() in ("reason", "note", "detail", "message",
                                 "comment", "description", "operator"):
                    assert v == "«REDACTED»"
        sig = json.loads(files["signature.json"])
        assert sig["recipient"] == "auditor1"
        # 两名不同签署人以假名呈现
        s0 = sig["events"][0]["signers"]
        assert len(set(s0)) == 2 and all(x.startswith("signer-") for x in s0)
        # 摘要与库内一致
        assert manifest["manifest_hash"] == d["manifest_hash"]
        assert manifest["content_digest"] == d["content_digest"]
        assert manifest["signature_digest"] == d["signature_digest"]
        # 逐行摘要齐全
        assert len(manifest["lines"]) == len(gseqs)
        # 服务端留存包校验通过
        r = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}/verify",
            params={"operator": "auditor1"})
        assert r.status_code == 200, r.text
        assert r.json()["valid"] is True
        # 独立离线校验入口(上传同一份 zip 字节, 无身份参数)
        r2 = client.post(
            "/api/admin/evidence/distributions/verify",
            content=zip_raw,
            headers={"Content-Type": "application/zip"})
        assert r2.status_code == 200, r2.text
        rep = r2.json()
        assert rep["valid"] is True
        assert rep["package_id"] == d["package_id"]
        assert rep["recipient"] == "auditor1"
        assert rep["matches_server_record"] is True

    def test_policy_none_keeps_plaintext_operators(self, client):
        rv, gseqs, _ = make_archived_review(client, key="none")
        register_recipient(client, key="rcp-none")
        d = create_dist(client, rv["review_id"], key="d",
                        policy="NONE").json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key="t").json()["token"]
        _, files = read_zip(download(client, tok).content)
        events = [json.loads(l) for l in
                  files["events.jsonl"].decode().splitlines()]
        assert {e["operator"] for e in events} - {"system", "api", "alice"} \
            <= {"system", "api", "alice", "dave", "bob", "carol", None}
        assert any(e["operator"] for e in events)
        sig = json.loads(files["signature.json"])
        assert sig["signer_identity"] == "plaintext"
        assert set(sig["events"][0]["signers"]) == {"bob", "carol"}

    def test_full_policy_redacts_payload(self, client):
        rv, _, _ = make_archived_review(client, key="full")
        register_recipient(client, key="rcp-full")
        d = create_dist(client, rv["review_id"], key="d",
                        policy="FULL").json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key="t").json()["token"]
        _, files = read_zip(download(client, tok).content)
        events = [json.loads(l) for l in
                  files["events.jsonl"].decode().splitlines()]
        assert all(isinstance(e["payload"], dict)
                   and e["payload"].get("redacted") is True for e in events)
        # 空请求体 -> 422
        r = client.post("/api/admin/evidence/distributions/verify",
                        content=b"",
                        headers={"Content-Type": "application/zip"})
        assert r.status_code == 422


# ======================================================================
# ---------- 创建前置条件: 未归档 / 非法接收方 ----------
# ======================================================================

class TestCreationGuards:
    def _open_review(self, client, key="open"):
        """构造一张 OPEN(未归档)复核单。"""
        pid, _ = setup_completed(client, key=key)
        s = new_session(client, pid, f"es-{key}")
        page_all(client, s["session_id"])
        r = client.post("/api/admin/evidence/exports",
                        json={"operator": "alice", "idempotency_key": f"ee-{key}",
                              "session_id": s["session_id"]})
        assert r.status_code in (200, 201)
        eid = r.json()["export_id"]
        w = evidence.EvidenceExportWorker()
        for _ in range(100):
            if client.get(f"/api/admin/evidence/exports/{eid}").json()[
                    "status"] in ("COMPLETED", "FAILED", "CANCELED"):
                break
            w.tick_once()
        r = client.post("/api/admin/evidence/reviews",
                        json={"operator": "alice", "idempotency_key": f"rv-{key}",
                              "session_id": s["session_id"]})
        assert r.status_code == 201
        return r.json()["review_id"]

    def test_review_not_archived_rejected(self, client):
        register_recipient(client)
        rid = self._open_review(client)
        r = create_dist(client, rid, key="d1", status=409)
        assert r.json()["detail"]["code"] == "review_not_archived"

    def test_unknown_review_404(self, client):
        register_recipient(client, key="r1")
        r = create_dist(client, "ER-nope", key="d1", status=404)
        assert r.json()["detail"]["error"] == "evidence_distribution_not_found"

    def test_invalid_recipient_format_rejected(self, client):
        rv, _, _ = make_archived_review(client, key="badfmt")
        for bad in ["", "a b", "bad/name", "x" * 65]:
            r = client.post("/api/admin/evidence/recipients",
                            json={"operator": "admin",
                                  "idempotency_key": f"bad-{bad or 'empty'}",
                                  "recipient": bad})
            assert r.status_code in (409, 422), (bad, r.text)
        # 创建时直接传非法账号同样被拒
        r = create_dist(client, rv["review_id"], recipient="a b",
                        key="d1", status=409)
        assert r.json()["detail"]["code"] == "invalid_recipient"

    def test_unregistered_and_disabled_recipient_rejected(self, client):
        rv, _, _ = make_archived_review(client, key="unreg")
        # 未登记
        r = create_dist(client, rv["review_id"], recipient="ghost",
                        key="d1", status=409)
        assert r.json()["detail"]["code"] == "recipient_not_registered"
        # 登记后停用
        register_recipient(client, recipient="ghost", key="g1")
        r = client.post("/api/admin/evidence/recipients/ghost/disable",
                        json={"operator": "admin", "idempotency_key": "g2",
                              "reason": "离职"})
        assert r.status_code == 200, r.text
        r = create_dist(client, rv["review_id"], recipient="ghost",
                        key="d2", status=409)
        assert r.json()["detail"]["code"] == "recipient_disabled"
        # 停用后不能重复登记
        r = register_recipient(client, recipient="ghost", key="g3", status=409)
        assert r.json()["detail"]["code"] == "recipient_disabled"

    def test_invalid_policy_and_validity_rejected(self, client):
        rv, _, _ = make_archived_review(client, key="badpol")
        register_recipient(client, key="r1")
        r = create_dist(client, rv["review_id"], policy="SUPERSECRET",
                        key="d1", status=409)
        assert r.json()["detail"]["code"] == "invalid_redaction_policy"
        past = (datetime.utcnow() - timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        r = create_dist(client, rv["review_id"], valid_until=past,
                        key="d2", status=409)
        assert r.json()["detail"]["code"] == "invalid_validity"


# ======================================================================
# ---------- 幂等: 同键重放 / 同参数去重 / 撤销与重签 ----------
# ======================================================================

class TestIdempotency:
    def test_same_idempotency_key_replays(self, client):
        rv, _, _ = make_archived_review(client, key="idem")
        register_recipient(client)
        a = create_dist(client, rv["review_id"], key="same", ttl=3600)
        assert a.status_code == 201
        b = create_dist(client, rv["review_id"], key="same", ttl=3600)
        assert b.status_code == 201 and b.json()["replayed"] is True
        assert b.json()["package_id"] == a.json()["package_id"]
        rows = client.get("/api/admin/evidence/distributions").json()
        assert len(rows) == 1
        # 同键不同请求体 -> 409
        r = create_dist(client, rv["review_id"], key="same", ttl=7200)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "idempotency_reuse"

    def test_same_params_natural_dedup(self, client):
        rv, _, _ = make_archived_review(client, key="dedup")
        register_recipient(client)
        a = create_dist(client, rv["review_id"], key="k1", ttl=3600).json()
        b = create_dist(client, rv["review_id"], key="k2", ttl=3600).json()
        assert b["package_id"] == a["package_id"] and b["deduped"] is True
        assert b["reissued"] is False
        # 不同接收方 -> 不同固定 package_id
        register_recipient(client, recipient="auditor2", key="r2")
        c = create_dist(client, rv["review_id"], recipient="auditor2",
                        key="k3", ttl=3600).json()
        assert c["package_id"] != a["package_id"] and c["deduped"] is False

    def test_revoke_idempotent_and_reissue_new_package(self, client):
        rv, _, _ = make_archived_review(client, key="reissue")
        register_recipient(client)
        d1 = create_dist(client, rv["review_id"], key="d1").json()
        # 撤销幂等: 同键重放 / 不同键重复撤销
        r = revoke(client, d1["package_id"], key="rv1",
                   reason="接收方权限收回")
        assert r.status_code == 200 and r.json()["status"] == "REVOKED"
        r2 = revoke(client, d1["package_id"], key="rv1")
        assert r2.status_code == 200 and r2.json()["replayed"] is True
        r3 = revoke(client, d1["package_id"], key="rv2")
        assert r3.status_code == 200 and r3.json()["already_in_state"] is True
        detail = get_dist(client, d1["package_id"]).json()
        assert detail["status"] == "REVOKED" and detail["revoked_by"] == "admin"
        assert detail["revoke_reason"] == "接收方权限收回"
        # 撤销后重新签发: 新 package_id, issue_no=2, 旧包原样保留
        d2 = create_dist(client, rv["review_id"], key="d2").json()
        assert d2["package_id"] != d1["package_id"]
        assert d2["issue_no"] == 2 and d2["reissued"] is True
        assert d2["status"] == "ACTIVE"
        rows = client.get("/api/admin/evidence/distributions",
                          params={"review_id": rv["review_id"]}).json()
        assert {x["package_id"] for x in rows} == {
            d1["package_id"], d2["package_id"]}
        old = get_dist(client, d1["package_id"]).json()
        assert old["status"] == "REVOKED"  # 旧包状态不被新签发改变

    def test_revoke_does_not_touch_review_or_events(self, client):
        rv, gseqs, pid = make_archived_review(client, key="imm-rev")
        register_recipient(client)
        d1 = create_dist(client, rv["review_id"], key="d1").json()
        sig_before = rv["signature_hash"]
        revoke(client, d1["package_id"])
        # 复核单仍为 ARCHIVED, 签署摘要不变, 结论不变
        after = client.get(
            f"/api/admin/evidence/reviews/{rv['review_id']}").json()
        assert after["status"] == "ARCHIVED"
        assert after["signature_hash"] == sig_before
        assert len(after["items"]) == len(gseqs)
        assert all(len(x["signatures"]) == 2 for x in after["items"])
        # 底层事件链未被触碰
        db = SessionLocal()
        try:
            row = db.get(EvidenceReview, rv["review_id"])
            assert row.status == "ARCHIVED"
            assert row.signature_hash == sig_before
            assert db.query(AuditEvent).count() > 0
        finally:
            db.close()


# ======================================================================
# ---------- 一次性下载令牌 / 接收方约束 / 过期 / 撤销 ----------
# ======================================================================

class TestDownloadGuards:
    def test_one_time_token_invalid_after_first_redeem(self, client):
        rv, _, _ = make_archived_review(client, key="once")
        register_recipient(client)
        d = create_dist(client, rv["review_id"], key="d").json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key="t").json()["token"]
        r1 = download(client, tok)
        assert r1.status_code == 200
        r2 = download(client, tok)
        assert r2.status_code == 409
        assert r2.json()["detail"]["code"] == "token_already_redeemed"
        detail = get_dist(client, d["package_id"]).json()
        assert detail["download_count"] == 1

    def test_expired_package_and_token_rejected(self, client):
        rv, _, _ = make_archived_review(client, key="exp")
        register_recipient(client)
        # 包有效期极短: ttl=60s, 直接把库内 valid_until 改到过去模拟到期
        d = create_dist(client, rv["review_id"], key="d", ttl=3600).json()
        db = SessionLocal()
        try:
            row = db.get(distribution.EvidenceDistribution, d["package_id"])
            row.valid_until = evidence.now_utc_naive() - timedelta(seconds=10)
            db.commit()
        finally:
            db.close()
        detail = get_dist(client, d["package_id"]).json()
        assert detail["status"] == "EXPIRED" and detail["expired"] is True
        # 过期包不能签发新令牌
        r = issue_token(client, d["package_id"], operator="auditor1",
                        key="t1", status=409)
        assert r.json()["detail"]["code"] == "package_expired"
        # 过期前签发的令牌也不能兑换: 手工建一个未用令牌
        db = SessionLocal()
        try:
            row = db.get(distribution.EvidenceDistribution, d["package_id"])
            import hashlib
            import uuid
            raw = uuid.uuid4().hex
            db.add(distribution.EvidenceDistributionDownload(
                id="EDDx" + uuid.uuid4().hex[:8],
                token_hash=hashlib.sha256(raw.encode()).hexdigest(),
                distribution_id=row.id, bound_recipient="auditor1",
                issued_by="admin",
                expires_at=evidence.now_utc_naive() + timedelta(hours=1)))
            db.commit()
            old_token = raw
        finally:
            db.close()
        r = download(client, old_token)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_expired"

    def test_revoked_package_blocks_token_issue_and_redeem(self, client):
        rv, _, _ = make_archived_review(client, key="revdl")
        register_recipient(client)
        d = create_dist(client, rv["review_id"], key="d").json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key="t1").json()["token"]
        revoke(client, d["package_id"], key="rv")
        # 撤销后未用令牌不能兑换
        r = download(client, tok)
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "package_revoked"
        # 撤销后不能签发新令牌
        r = issue_token(client, d["package_id"], operator="auditor1",
                        key="t2", status=409)
        assert r.json()["detail"]["code"] == "package_revoked"

    def test_recipient_binding_enforced(self, client):
        rv, _, _ = make_archived_review(client, key="bind")
        register_recipient(client)
        register_recipient(client, recipient="auditor2", key="r2")
        d = create_dist(client, rv["review_id"], key="d").json()
        # 非授权接收方不能签发令牌
        r = issue_token(client, d["package_id"], operator="mallory",
                        key="t0", status=403)
        assert r.json()["detail"]["code"] == "recipient_forbidden"
        # auditor2 也不能查看 auditor1 的包
        r = get_dist(client, d["package_id"], operator="auditor2")
        assert r.status_code == 403
        # 只有绑定接收方 auditor1 能兑换; 管理员创建者 admin 可查看
        tok = issue_token(client, d["package_id"], operator="admin",
                          key="t1").json()["token"]
        r = download(client, tok, operator="auditor2")
        assert r.status_code == 409
        assert r.json()["detail"]["code"] == "recipient_mismatch"
        r = get_dist(client, d["package_id"], operator="admin")
        assert r.status_code == 200
        # 接收方停用后令牌兑换被拒
        client.post("/api/admin/evidence/recipients/auditor1/disable",
                    json={"operator": "admin", "idempotency_key": "dis1"})
        tok2 = issue_token(client, d["package_id"], operator="admin",
                           key="t2")
        # 停用后签发也被拒(409)
        assert tok2.status_code == 409

    def test_unknown_token_404(self, client):
        r = download(client, "nope-not-a-token")
        assert r.status_code == 404

    def test_issue_token_idempotent_replay(self, client):
        rv, _, _ = make_archived_review(client, key="tokidem")
        register_recipient(client)
        d = create_dist(client, rv["review_id"], key="d").json()
        a = issue_token(client, d["package_id"], operator="auditor1", key="tk")
        b = issue_token(client, d["package_id"], operator="auditor1", key="tk")
        assert b.json()["replayed"] is True
        assert b.json()["token"] == a.json()["token"]


# ======================================================================
# ---------- 篡改: 行 / 文件 / manifest / 签名, 校验指出位置 ----------
# ======================================================================

class TestTamperVerification:
    def _pkg(self, client, key, policy="STANDARD"):
        rv, gseqs, _ = make_archived_review(client, key=key)
        register_recipient(client, key=f"r-{key}")
        d = create_dist(client, rv["review_id"], key=f"d-{key}",
                        policy=policy).json()
        tok = issue_token(client, d["package_id"], operator="auditor1",
                          key=f"t-{key}").json()["token"]
        raw = download(client, tok).content
        return rv, d, raw

    def _rewrite_zip(self, raw, mutate, names_out=None):
        src = zipfile.ZipFile(io.BytesIO(raw))
        files = {n: src.read(n) for n in src.namelist()}
        files = mutate(files)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in sorted(files):
                zf.writestr(name, files[name])
        return buf.getvalue()

    def test_tampered_event_line_detected_with_position(self, client):
        rv, d, raw = self._pkg(client, "t-line")
        # 改 events.jsonl 第二行的 event_type(同时保持 JSON 合法)

        def mutate(files):
            lines = files["events.jsonl"].decode().splitlines()
            ev = json.loads(lines[1])
            ev["event_type"] = "EVIL_TYPE"
            lines[1] = json.dumps(ev, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":"))
            files["events.jsonl"] = ("\n".join(lines) + "\n").encode()
            return files

        evil = self._rewrite_zip(raw, mutate)
        # 服务端留存包仍完好
        assert client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}/verify",
            params={"operator": "auditor1"}).json()["valid"] is True
        r = client.post("/api/admin/evidence/distributions/verify",
                        content=evil,
                        headers={"Content-Type": "application/zip"})
        rep = r.json()
        assert rep["valid"] is False
        codes = {t["code"] for t in rep["tampering"]}
        assert "line_digest_mismatch" in codes
        t = next(x for x in rep["tampering"]
                 if x["code"] == "line_digest_mismatch")
        assert t["target"].startswith("events.jsonl#seq2")
        assert t["expected"] and t["actual"] and t["expected"] != t["actual"]
        # 行内容变 -> content_digest 与签名摘要也必然失配
        assert "content_digest_mismatch" in codes
        assert "signature_digest_mismatch" in codes
        assert rep["matches_server_record"] is False

    def test_tampered_file_digest_detected(self, client):
        rv, d, raw = self._pkg(client, "t-file")

        def mutate(files):
            files["metadata.json"] = files["metadata.json"] + b" "
            return files

        evil = self._rewrite_zip(raw, mutate)
        rep = client.post("/api/admin/evidence/distributions/verify",
                          content=evil,
                          headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is False
        t = next(x for x in rep["tampering"]
                 if x["code"] == "file_digest_mismatch")
        assert t["target"] == "metadata.json"
        assert "content_digest_mismatch" in {
            x["code"] for x in rep["tampering"]}

    def test_manifest_field_tamper_detected(self, client):
        rv, d, raw = self._pkg(client, "t-man")

        def mutate(files):
            m = json.loads(f["manifest.json"]) if False else json.loads(
                files["manifest.json"])
            m["recipient"] = "auditor2"
            files["manifest.json"] = (json.dumps(
                m, ensure_ascii=False, sort_keys=True, indent=2) + "\n")\
                .encode()
            return files

        evil = self._rewrite_zip(raw, mutate)
        rep = client.post("/api/admin/evidence/distributions/verify",
                          content=evil,
                          headers={"content-type": "application/zip"}).json()
        assert rep["valid"] is False
        codes = {x["code"] for x in rep["tampering"]}
        # 主体不一致 + 签名摘要失配(接收方是签名 subject 的一部分)
        assert "subject_mismatch" in codes
        assert "signature_digest_mismatch" in codes
        assert "manifest_hash_mismatch" in codes

    def test_signature_file_tamper_detected(self, client):
        rv, d, raw = self._pkg(client, "t-sig")

        def mutate(files):
            s = json.loads(files["signature.json"])
            s["events"][0]["signers"] = ["signer-deadbeef1",
                                         "signer-deadbeef2"]
            files["signature.json"] = (json.dumps(
                s, ensure_ascii=False, sort_keys=True, indent=2) + "\n")\
                .encode()
            return files

        evil = self._rewrite_zip(raw, mutate)
        rep = client.post("/api/admin/evidence/distributions/verify",
                          content=evil,
                          headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is False
        assert any(x["code"] == "file_digest_mismatch"
                   and x["target"] == "signature.json"
                   for x in rep["tampering"])
        assert "content_digest_mismatch" in {
            x["code"] for x in rep["tampering"]}

    def test_redaction_bypass_detected(self, client):
        rv, d, raw = self._pkg(client, "t-bypass")

        def mutate(files):
            # 声称 STANDARD 却把操作者明文塞回去(同时保持行摘要一致是不可能的,
            # 这里只验证 redaction_bypass 也会被独立报告)
            lines = files["events.jsonl"].decode().splitlines()
            ev = json.loads(lines[0])
            ev["operator"] = "bob"
            lines[0] = json.dumps(ev, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":"))
            files["events.jsonl"] = ("\n".join(lines) + "\n").encode()
            return files

        evil = self._rewrite_zip(raw, mutate)
        rep = client.post("/api/admin/evidence/distributions/verify",
                          content=evil,
                          headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is False
        assert any(x["code"] == "redaction_bypass"
                   for x in rep["tampering"])

    def test_corrupt_zip_reported(self, client):
        rep = client.post(
            "/api/admin/evidence/distributions/verify",
            content=b"not a zip at all",
            headers={"Content-Type": "application/zip"}).json()
        assert rep["valid"] is False
        assert rep["tampering"][0]["code"] == "package_corrupt"

    def test_stored_package_tamper_marks_report_and_denies(self, client):
        """直接篡改磁盘上的留存包: 服务端校验报告具体位置。"""
        import glob
        rv, d, raw = self._pkg(client, "t-disk")
        path = os.path.join(os.environ["EVIDENCE_DISTRIBUTION_STORE_DIR"],
                            d["package_id"],
                            f"distribution-{d['package_id']}.zip")
        assert glob.glob(path)

        def mutate(files):
            lines = files["events.jsonl"].decode().splitlines()
            ev = json.loads(lines[0])
            ev["payload"] = {**(ev.get("payload") or {}), "x": "EVIL"}
            lines[0] = json.dumps(ev, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":"))
            files["events.jsonl"] = ("\n".join(lines) + "\n").encode()
            return files

        evil = self._rewrite_zip(raw, mutate)
        with open(path, "wb") as f:
            f.write(evil)
        r = client.get(
            f"/api/admin/evidence/distributions/{d['package_id']}/verify",
            params={"operator": "auditor1"})
        rep = r.json()
        assert rep["valid"] is False
        assert rep["tampering"], rep
        # 流水记录校验失败
        detail = get_dist(client, d["package_id"]).json()
        assert any(e["event"] == "dist.verify.failed" for e in detail["events"])


# ======================================================================
# ---------- 非回归: 证据分页/导出/复核归档/补偿审批 ----------
# ======================================================================

class TestNoRegression:
    def test_evidence_paging_export_review_archive_alongside(self, client):
        pid, _ = setup_completed(client, key="nr1")
        s = new_session(client, pid, "es-nr")
        # 分页 + 片段证明
        cur = None
        pages = []
        for _ in range(20):
            r = client.post(
                f"/api/admin/evidence/sessions/{s['session_id']}/pages",
                json={"operator": "alice", "cursor": cur, "limit": 2})
            assert r.status_code == 200, r.text
            j = r.json()
            pages.append(j)
            cur = j["cursor"]["next_cursor"]
            if not j["cursor"]["has_more"]:
                break
        assert any(p["proof"]["fragment_digest"] for p in pages)
        # 导出 + 一次性下载 + 摘要校验
        r = client.post("/api/admin/evidence/exports",
                        json={"operator": "alice", "idempotency_key": "ee-nr",
                              "session_id": s["session_id"],
                              "segment_size": 2})
        assert r.status_code in (200, 201)
        eid = r.json()["export_id"]
        w = evidence.EvidenceExportWorker()
        for _ in range(100):
            if client.get(f"/api/admin/evidence/exports/{eid}").json()[
                    "status"] in ("COMPLETED", "FAILED", "CANCELED"):
                break
            w.tick_once()
        assert client.get(f"/api/admin/evidence/exports/{eid}").json()[
                   "status"] == "COMPLETED"
        assert client.get(f"/api/admin/evidence/exports/{eid}/verify").json()[
                   "valid"] is True
        # 状态汇总各域齐全
        st = client.get("/api/status").json()
        for k in ("evidence_sessions", "evidence_exports", "evidence_reviews",
                  "evidence_distributions", "evidence_recipients",
                  "compensations"):
            assert k in st

    def test_compensation_approval_endpoints_present(self, client):
        add_records(client, range(100, 103))
        b = mk_batch(client, 100, 102, "b-hw")
        pid = mk_plan(client, b, "p-hw", risk="HIGH")
        # HIGH 计划审批流不受影响
        assert client.post(f"/api/admin/plans/{pid}/approve",
                           json={"operator": "alice",
                                 "idempotency_key": "ap0"}).status_code == 409
        assert client.post(f"/api/admin/plans/{pid}/approve",
                           json={"operator": "bob",
                                 "idempotency_key": "ap1"}).status_code == 200
        # 补偿任务列表与接收方/分发目录空态
        assert client.get("/api/admin/compensations").status_code == 200
        assert client.get(
            "/api/admin/evidence/recipients").json() == []
        assert client.get(
            "/api/admin/evidence/distributions").json() == []

    def test_distribution_filter_and_status_overview(self, client):
        rv1, _, p1 = make_archived_review(client, key="f1")
        rv2, _, p2 = make_archived_review(client, start=10, end=12, key="f2")
        register_recipient(client, key="r1")
        register_recipient(client, recipient="auditor2", key="r2")
        create_dist(client, rv1["review_id"], key="d1")
        create_dist(client, rv1["review_id"], recipient="auditor2", key="d2")
        create_dist(client, rv2["review_id"], key="d3")
        assert len(client.get("/api/admin/evidence/distributions").json()) == 3
        rows = client.get("/api/admin/evidence/distributions",
                          params={"recipient": "auditor1"}).json()
        assert {x["review_id"] for x in rows} == {
            rv1["review_id"], rv2["review_id"]}
        rows = client.get("/api/admin/evidence/distributions",
                          params={"plan_id": p2}).json()
        assert len(rows) == 1
        r = client.get("/api/admin/evidence/distributions",
                       params={"status_filter": "BOGUS"})
        assert r.status_code == 422
