"""归档目录与生命周期管理测试:

- 目录检索: 按回放/业务分组/报告版本/内容摘要/保留状态多维检索, 按摘要查询接口
- 保留策略: 到期保留与永久保留阻止清理, 到期后可清理, 非法参数拒绝, 重启保留
- 清理计划: 排队/执行/暂停/恢复/取消状态机, 逐项跳过原因查询, 幂等控制
- 并发协调: 正在下载/校验的归档不能被删除(计数闸门)
- 同摘要去重与引用: 非 canonical 解除引用不删文件; canonical 移交物理文件;
  移交失败(digest_referenced)跳过; 最后一个引用解除才删物理包; 原归档删除
  不影响仍被引用的记录
- 重启: 保留策略、引用关系、清理进度(逐项结果)、RUNNING 复位全部保留
"""
import datetime as dt
import json
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="migration-archive-life-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["APP_VERSION"] = "1.0.0-test"
os.environ["PLAN_WORKER_ENABLED"] = "0"
os.environ["REPLAY_MAX_CONCURRENCY"] = "2"
os.environ["ARCHIVE_MAX_CONCURRENCY"] = "2"
os.environ["ARCHIVE_STORE_DIR"] = f"{_tmp}/store"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, SessionLocal, engine
from app.main import app
from app.models import (
    ArchiveCleanupPlan, ArchiveDigestMember, IdempotencyKey, ReplayArchive,
)
from app import archives, cleanup, plans, replay


@pytest.fixture()
def client():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    os.makedirs(archives.store_dir(), exist_ok=True)
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def one_item_per_tick():
    """逐项边界测试需要确定性: 每个清理 tick 至多处理一项。"""
    os.environ["CLEANUP_ITEMS_PER_TICK"] = "1"
    yield
    os.environ.pop("CLEANUP_ITEMS_PER_TICK", None)


@pytest.fixture(autouse=True)
def _isolate_idem_keys(client):
    db = SessionLocal()
    try:
        db.query(IdempotencyKey).delete()
        db.commit()
    finally:
        db.close()
    yield


# ---------- 复用归档测试的搭建方式 ----------

def mk_records(client, ids, tags="a,b"):
    for i in ids:
        r = client.post("/api/records",
                        json={"id": i, "name": f"r{i}", "email": f"r{i}@x.com",
                              "tags_csv": tags})
        assert r.status_code == 201, r.text


def mk_batch(client, start, end, key, biz=None):
    r = client.post("/api/admin/batches",
                    json={"operator": "alice", "idempotency_key": key,
                          "biz": biz or f"biz-{start}", "id_start": start,
                          "id_end": end})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def completed_replay(client, ranges=((1, 10), (11, 20)), key="kp", biz=None):
    if ranges and isinstance(ranges[0], int):  # 单个 (lo, hi) 容错
        ranges = (ranges,)
    ids = []
    for lo, hi in ranges:
        ids.extend(range(lo, hi + 1))
    mk_records(client, ids)
    bids = []
    for lo, hi in ranges:
        b = biz(lo) if callable(biz) else (biz or f"biz-{lo}")
        bids.append(mk_batch(client, lo, hi, f"kb-{key}-{lo}", biz=b))
    steps = [{"seq": 1, "batch_id": bids[0], "max_retries": 0}]
    for i, b in enumerate(bids[1:], start=2):
        steps.append({"seq": i, "batch_id": b, "depends_on": [i - 1], "max_retries": 0})
    r = client.post("/api/admin/plans",
                    json={"operator": "alice", "idempotency_key": key,
                          "name": "归档生命周期测试计划", "max_retries": 0,
                          "steps": steps})
    pid = r.json()["plan_id"]
    r = client.post(f"/api/admin/plans/{pid}/start",
                    json={"operator": "alice", "idempotency_key": f"k-start-{key}"})
    assert r.status_code == 200, r.text
    for _ in range(12):
        db = SessionLocal()
        try:
            if not plans.run_plan_tick(db, pid):
                break
        finally:
            db.close()
    r = client.post("/api/admin/checkpoints",
                    json={"operator": "bob", "idempotency_key": f"kc-{key}",
                          "plan_id": pid})
    cp = r.json()
    assert cp["checkpoint_status"] == "COMPLETE", r.text
    r = client.post("/api/admin/replays",
                    json={"operator": "bob", "idempotency_key": f"kr-{key}",
                          "plan_id": pid, "checkpoint_id": cp["checkpoint_id"]})
    rid = r.json()["replay_id"]
    _run_replay(rid)
    rep = client.get(f"/api/admin/replays/{rid}").json()
    assert rep["status"] == "COMPLETED"
    return rid, pid, cp["checkpoint_id"], bids


def _run_replay(rid, n=20):
    db = SessionLocal()
    try:
        replay.claim_due_tasks(db)
    finally:
        db.close()
    for _ in range(n):
        db = SessionLocal()
        try:
            if not replay.run_replay_tick(db, rid):
                break
        finally:
            db.close()


def create_completed_archive(client, rid, key="ka"):
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": key,
                          "replay_id": rid, "report_version": 1})
    assert r.status_code == 201, r.text
    aid = r.json()["archive_id"]
    _run_archive(aid)
    a = client.get(f"/api/admin/archives/{aid}").json()
    assert a["status"] == "COMPLETED", a
    return a


def _claim():
    db = SessionLocal()
    try:
        return archives.claim_due_archives(db)
    finally:
        db.close()


def _tick(aid):
    db = SessionLocal()
    try:
        return archives.run_archive_tick(db, aid)
    finally:
        db.close()


def _run_archive(aid, n=50):
    _claim()
    for _ in range(n):
        if not _tick(aid):
            break


def _claim_cleanup():
    db = SessionLocal()
    try:
        return cleanup.claim_due_plans(db)
    finally:
        db.close()


def _tick_cleanup(pid):
    db = SessionLocal()
    try:
        return cleanup.run_cleanup_tick(db, pid)
    finally:
        db.close()


def run_cleanup_plan(pid, n=20):
    _claim_cleanup()
    for _ in range(n):
        if not _tick_cleanup(pid):
            break


def create_cleanup(client, aids, key="kcl"):
    r = client.post("/api/admin/archive-cleanups",
                    json={"operator": "dave", "idempotency_key": key,
                          "archive_ids": aids})
    assert r.status_code == 201, r.text
    return r.json()


def cleanup_action(client, pid, action, key):
    r = client.post(f"/api/admin/archive-cleanups/{pid}/{action}",
                    json={"operator": "dave", "idempotency_key": key})
    assert r.status_code == 200, r.text
    return r.json()


def get_cleanup(client, pid):
    return client.get(f"/api/admin/archive-cleanups/{pid}").json()


def item_map(view):
    return {i["archive_id"]: i for i in view["items"]}


def set_retention(client, aid, mode, key, retain_until=None):
    return client.put(f"/api/admin/archives/{aid}/retention",
                      json={"operator": "erin", "idempotency_key": key,
                            "mode": mode, "retain_until": retain_until})


# ---------- 归档目录检索 ----------

def test_catalog_filters_by_replay_biz_version(client):
    rid1, pid1, _, bids1 = completed_replay(
        client, ranges=((1, 5), (6, 10)), key="kp1",
        biz=lambda lo: "订单" if lo == 1 else "支付")
    rid2, _, _, _ = completed_replay(
        client, ranges=((11, 15), (16, 20)), key="kp2", biz="物流")
    a1 = create_completed_archive(client, rid1, key="ka1")
    a2 = create_completed_archive(client, rid2, key="ka2")
    # biz 过滤: 归档 biz_groups 固化了步骤批次的业务分组
    r = client.get("/api/admin/archives", params={"biz": "订单"})
    assert {x["id"] for x in r.json()} == {a1["id"]}
    assert "订单" in r.json()[0]["biz_groups"]
    r = client.get("/api/admin/archives", params={"biz": "物流"})
    assert {x["id"] for x in r.json()} == {a2["id"]}
    # 不匹配业务分组
    assert client.get("/api/admin/archives", params={"biz": "不存在"}).json() == []
    # 回放过滤
    r = client.get("/api/admin/archives", params={"replay_id": rid2})
    assert [x["id"] for x in r.json()] == [a2["id"]]
    # 报告版本过滤
    assert client.get("/api/admin/archives",
                      params={"report_version": 99}).json() == []
    r = client.get("/api/admin/archives", params={"report_version": 1})
    assert len(r.json()) == 2
    # status 过滤
    assert all(x["status"] == "COMPLETED"
               for x in client.get("/api/admin/archives",
                                   params={"status": "COMPLETED"}).json())


def test_catalog_by_digest_endpoint(client):
    rid, _, _, _ = completed_replay(client, key="kp1")
    a = create_completed_archive(client, rid, key="ka1")
    digest = a["content_digest"]
    # 完整摘要精确
    r = client.get(f"/api/admin/archives/by-digest/{digest}")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1 and body["archives"][0]["id"] == a["id"]
    assert body["digest_groups"][0]["reference_count"] == 1
    assert body["digest_groups"][0]["canonical_archive_id"] == a["id"]
    # 12 位前缀消歧
    r = client.get(f"/api/admin/archives/by-digest/{digest[:12]}")
    assert [x["id"] for x in r.json()["archives"]] == [a["id"]]
    # 过短前缀不做模糊匹配
    r = client.get(f"/api/admin/archives/by-digest/{digest[:6]}")
    assert r.json()["count"] == 0
    # 完整摘要不匹配
    assert client.get("/api/admin/archives/by-digest/" + ("0" * 64)).json()["count"] == 0
    # 列表接口也支持 content_digest 参数
    r = client.get("/api/admin/archives", params={"content_digest": digest[:12]})
    assert [x["id"] for x in r.json()] == [a["id"]]


# ---------- 保留策略 ----------

def test_retention_lifecycle_and_validation(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    aid = a["id"]
    assert a["retention"]["mode"] == "NONE" and a["retention"]["retained"] is False
    # 永久保留
    future = (dt.datetime.utcnow() + dt.timedelta(days=2)).isoformat()
    r = set_retention(client, aid, "PERMANENT", "kr1")
    assert r.status_code == 200, r.text
    v = client.get(f"/api/admin/archives/{aid}").json()
    assert v["retention"]["mode"] == "PERMANENT" and v["retention"]["retained"] is True
    # 到期保留
    r = set_retention(client, aid, "UNTIL", "kr2", retain_until=future)
    v = client.get(f"/api/admin/archives/{aid}").json()
    assert v["retention"]["mode"] == "UNTIL" and v["retention"]["retained"] is True
    assert v["retention"]["retain_reason"] == "retained_until"
    # 过去时间拒绝
    past = (dt.datetime.utcnow() - dt.timedelta(hours=1)).isoformat()
    r = set_retention(client, aid, "UNTIL", "kr3", retain_until=past)
    assert r.status_code == 409 and "晚于当前时间" in r.json()["detail"]["reason"]
    # UNTIL 缺时间拒绝 / 非法 mode 拒绝 / 非法时间格式拒绝
    r = set_retention(client, aid, "UNTIL", "kr4")
    assert r.status_code == 409
    r = set_retention(client, aid, "BOGUS", "kr5")
    assert r.status_code == 409
    r = set_retention(client, aid, "UNTIL", "kr6", retain_until="not-a-date")
    assert r.status_code == 409
    # 清除策略
    r = set_retention(client, aid, "NONE", "kr7")
    assert r.status_code == 200
    assert client.get(f"/api/admin/archives/{aid}").json()["retention"]["retained"] is False
    # 幂等: 同键重放无副作用
    r1 = set_retention(client, aid, "PERMANENT", "same-k")
    r2 = set_retention(client, aid, "PERMANENT", "same-k")
    assert r2.json()["replayed"] is True
    # retention 过滤
    r = client.get("/api/admin/archives", params={"retention": "PERMANENT"})
    assert [x["id"] for x in r.json()] == [aid]
    # 非 COMPLETED 归档不能设置保留策略
    rid2, _, _, _ = completed_replay(client, ranges=((21, 25), (26, 30)),
                                     key="kp2")
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka9",
                          "replay_id": rid2, "report_version": 1})
    active_id = r.json()["archive_id"]  # QUEUED
    r = set_retention(client, active_id, "PERMANENT", "kr8")
    assert r.status_code == 409 and "COMPLETED" in r.json()["detail"]["reason"]


def test_retention_blocks_cleanup_until_expired(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    aid, path = a["id"], None
    db = SessionLocal()
    try:
        path = db.get(ReplayArchive, aid).package_path
    finally:
        db.close()
    # 永久保留: 清理逐项跳过
    set_retention(client, aid, "PERMANENT", "k1")
    p = create_cleanup(client, [aid], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[aid]["status"] == "SKIPPED"
    assert items[aid]["reason_code"] == "retained_permanent"
    assert os.path.exists(path)  # 物理文件保留
    assert client.get(f"/api/admin/archives/{aid}").json()["cleaned"] is False
    # 改为到期保留(未来): 仍跳过
    future = (dt.datetime.utcnow() + dt.timedelta(days=1)).isoformat()
    set_retention(client, aid, "UNTIL", "k2", retain_until=future)
    p = create_cleanup(client, [aid], key="kc2")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[aid]["reason_code"] == "retained_until"
    assert os.path.exists(path)
    # 到期后(直接把到期时间改到过去): 清理成功并删除文件
    db = SessionLocal()
    try:
        row = db.get(ReplayArchive, aid)
        row.retain_until = dt.datetime.utcnow() - dt.timedelta(hours=1)
        db.commit()
    finally:
        db.close()
    v = client.get(f"/api/admin/archives/{aid}").json()
    assert v["retention"]["retained"] is False and v["retention"]["expired"] is True
    p = create_cleanup(client, [aid], key="kc3")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[aid]["status"] == "CLEANED" and items[aid]["reason_code"] is None
    assert not os.path.exists(path)  # 唯一引用, 物理文件删除
    # 默认目录检索不再返回, include_cleaned 可查
    assert client.get("/api/admin/archives").json() == []
    got = client.get(f"/api/admin/archives/{aid}").json()
    assert got["cleaned"] is True and got["cleaned_by"] == "dave"
    assert client.get("/api/admin/archives",
                      params={"include_cleaned": True}).json()[0]["id"] == aid


# ---------- 清理计划逐项跳过原因 ----------

def test_cleanup_skips_with_per_item_reasons(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    aid = a["id"]
    # 活动中归档(QUEUED) + 不存在 + 已清理(同一已清理归档放两次会被去重, 用两次计划)
    rid2, _, _, _ = completed_replay(client, ranges=((21, 25), (26, 30)),
                                     key="kp2")
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka2",
                          "replay_id": rid2, "report_version": 1})
    active_id = r.json()["archive_id"]
    p = create_cleanup(client, [aid, active_id, "A-nope"], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    view = get_cleanup(client, p)
    assert view["status"] == "COMPLETED"
    assert view["progress"] == {"total": 3, "cleaned": 1, "skipped": 2,
                                "failed": 0, "processed": 3}
    items = item_map(view)
    assert items[aid]["status"] == "CLEANED"
    assert items[active_id]["status"] == "SKIPPED"
    assert items[active_id]["reason_code"] == "not_completed"
    assert items["A-nope"]["status"] == "SKIPPED"
    assert items["A-nope"]["reason_code"] == "not_found"
    # 再次清理已清理归档 -> already_cleaned
    p2 = create_cleanup(client, [aid], key="kc2")["plan_id"]
    run_cleanup_plan(p2)
    assert item_map(get_cleanup(client, p2))[aid]["reason_code"] == "already_cleaned"
    # 包文件被外部删除的归档: package_missing 跳过, 记录保留
    rid3, _, _, _ = completed_replay(client, ranges=((31, 35), (36, 40)),
                                     key="kp3")
    a3 = create_completed_archive(client, rid3, key="ka3")
    db = SessionLocal()
    try:
        missing_path = db.get(ReplayArchive, a3["id"]).package_path
    finally:
        db.close()
    os.remove(missing_path)
    p3 = create_cleanup(client, [a3["id"]], key="kc3")["plan_id"]
    run_cleanup_plan(p3)
    assert item_map(get_cleanup(client, p3))[a3["id"]]["reason_code"] == "package_missing"
    assert client.get(f"/api/admin/archives/{a3['id']}").json()["cleaned"] is False
    # 清理计划列表
    listed = client.get("/api/admin/archive-cleanups").json()
    assert {x["id"] for x in listed} == {p, p2, p3}


def test_cleanup_plan_pause_resume_cancel(client):
    rids = []
    for i, ranges in enumerate(((1, 4), (5, 8), (9, 12), (13, 16))):
        rid, _, _, _ = completed_replay(client, ranges=ranges, key=f"kp{i}")
        rids.append(rid)
    aids = [create_completed_archive(client, rid, key=f"ka{i}")["id"]
            for i, rid in enumerate(rids)]
    p = create_cleanup(client, aids, key="kc1")["plan_id"]
    _claim_cleanup()
    assert _tick_cleanup(p) is True
    # 暂停在逐项边界: 已处理项保留, 后续不处理
    cleanup_action(client, p, "pause", "kp")
    assert _tick_cleanup(p) is False
    view = get_cleanup(client, p)
    assert view["status"] == "PAUSED"
    processed = view["progress"]["processed"]
    assert 1 <= processed < 4
    # 恢复排队并跑完
    cleanup_action(client, p, "resume", "kr")
    run_cleanup_plan(p)
    view = get_cleanup(client, p)
    assert view["status"] == "COMPLETED"
    assert view["progress"]["cleaned"] == 4

    # 取消: QUEUED 计划的未处理项全部置 SKIPPED_CANCELED
    aids2 = []
    for i, ranges in enumerate(((21, 24), (25, 28))):
        rid, _, _, _ = completed_replay(client, ranges=ranges, key=f"kq{i}")
        aids2.append(create_completed_archive(client, rid, key=f"kb{i}")["id"])
    p2 = create_cleanup(client, aids2, key="kc2")["plan_id"]
    cleanup_action(client, p2, "cancel", "kx")
    view = get_cleanup(client, p2)
    assert view["status"] == "CANCELED"
    assert all(i["status"] == "SKIPPED_CANCELED" for i in view["items"])
    assert client.get(f"/api/admin/archives/{aids2[0]}").json()["cleaned"] is False
    # 终态控制动作 409; 同键幂等重放
    r = client.post(f"/api/admin/archive-cleanups/{p2}/pause",
                    json={"operator": "dave", "idempotency_key": "kz"})
    assert r.status_code == 409
    r = cleanup_action(client, p2, "cancel", "kx")
    assert r["replayed"] is True


def test_cleanup_plan_idempotent_create_and_controls(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    body = {"operator": "dave", "idempotency_key": "same",
            "archive_ids": [a["id"]]}
    r1 = client.post("/api/admin/archive-cleanups", json=body)
    r2 = client.post("/api/admin/archive-cleanups", json=body)
    assert r1.json()["plan_id"] == r2.json()["plan_id"]
    assert r2.json()["replayed"] is True
    # 空列表拒绝
    r = client.post("/api/admin/archive-cleanups",
                    json={"operator": "dave", "idempotency_key": "empty",
                          "archive_ids": []})
    assert r.status_code == 422
    # 不存在计划 404
    r = client.post("/api/admin/archive-cleanups/CP-nope/pause",
                    json={"operator": "dave", "idempotency_key": "x"})
    assert r.status_code == 404


# ---------- 并发协调: 下载 / 摘要校验 ----------

def test_cleanup_skips_archive_being_downloaded_or_verified(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    aid = a["id"]
    db = SessionLocal()
    try:
        row = db.get(ReplayArchive, aid)
        assert os.path.exists(row.package_path)
        real_path = row.package_path
        # 模拟在途下载: 计数 +1
        archives.begin_archive_use(db, row, "download")
    finally:
        db.close()
    p = create_cleanup(client, [aid], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[aid]["status"] == "SKIPPED"
    assert items[aid]["reason_code"] == "in_use_download"
    assert os.path.exists(real_path)  # 下载中的文件绝不删除
    # 下载结束后可清理
    db = SessionLocal()
    try:
        archives.end_archive_use(db, aid, "download")
    finally:
        db.close()
    # 模拟在途校验
    db = SessionLocal()
    try:
        archives.begin_archive_use(db, db.get(ReplayArchive, aid), "verify")
    finally:
        db.close()
    p = create_cleanup(client, [aid], key="kc2")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[aid]["reason_code"] == "in_use_verify"
    assert os.path.exists(real_path)
    db = SessionLocal()
    try:
        archives.end_archive_use(db, aid, "verify")
    finally:
        db.close()
    p = create_cleanup(client, [aid], key="kc3")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[aid]["status"] == "CLEANED"
    assert not os.path.exists(real_path)


def test_download_endpoint_holds_use_counter(client):
    """HTTP 下载在响应期间持有计数; TestClient 后台任务在响应返回后释放计数,
    但开始下载的瞬间计数已 +1, 清理串行下不会与下载并发删文件。"""
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    aid = a["id"]
    r = client.get(f"/api/admin/archives/{aid}/download")
    assert r.status_code == 200
    db = SessionLocal()
    try:
        # 响应已完整发送, 后台任务释放计数
        assert db.get(ReplayArchive, aid).active_downloads == 0
    finally:
        db.close()
    # 手动持有计数时 HTTP 清理(直接走服务层)跳过 —— 与 worker 并发同一保证
    db = SessionLocal()
    try:
        archives.begin_archive_use(db, db.get(ReplayArchive, aid), "download")
    finally:
        db.close()
    p = create_cleanup(client, [aid], key="k")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[aid]["reason_code"] == "in_use_download"


# ---------- 同摘要去重与引用关系 ----------

def _force_same_digest(src_aid: str, dst_aid: str, *, canonical_src: bool = True):
    """把 dst 归档的内容摘要/包清单/路径伪造成与 src 相同, 重建成员关系,
    模拟同摘要去重组(归档包为确定性产物, 自然重复极罕见)。"""
    db = SessionLocal()
    try:
        src = db.get(ReplayArchive, src_aid)
        dst = db.get(ReplayArchive, dst_aid)
        # 删除 dst 自己刚写出的物理副本
        if dst.package_path and dst.package_path != src.package_path \
                and os.path.exists(dst.package_path):
            os.remove(dst.package_path)
        dst.content_digest = src.content_digest
        dst.manifest = src.manifest
        dst.package_path = src.package_path
        db.query(ArchiveDigestMember).filter(
            ArchiveDigestMember.archive_id == dst_aid).delete()
        db.add(ArchiveDigestMember(content_digest=src.content_digest,
                                   archive_id=dst_aid, is_canonical=False))
        db.commit()
    finally:
        db.close()


def test_digest_group_dedup_and_noncanonical_cleanup(client):
    rid1, _, _, _ = completed_replay(client, ranges=((1, 5), (6, 10)),
                                     key="kp1")
    rid2, _, _, _ = completed_replay(client, ranges=((11, 15), (16, 20)),
                                     key="kp2")
    a1 = create_completed_archive(client, rid1, key="ka1")
    a2 = create_completed_archive(client, rid2, key="ka2")
    _force_same_digest(a1["id"], a2["id"])
    # 按摘要查询返回两个归档, 引用数 = 2
    r = client.get(f"/api/admin/archives/by-digest/{a1['content_digest']}")
    assert r.json()["count"] == 2
    assert r.json()["digest_groups"][0]["reference_count"] == 2
    # 清理非 canonical 成员 a2: 解除引用, 物理文件保留(仍被 a1 引用)
    db = SessionLocal()
    try:
        path = db.get(ReplayArchive, a1["id"]).package_path
    finally:
        db.close()
    p = create_cleanup(client, [a2["id"]], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[a2["id"]]["status"] == "CLEANED"
    assert "规范成员保留" in items[a2["id"]]["reason"]
    assert os.path.exists(path)
    assert client.get(f"/api/admin/archives/{a1['id']}").json()["package_available"]
    # a1 引用数变为 1; a2 成员关系已解除
    db = SessionLocal()
    try:
        members = db.query(ArchiveDigestMember).filter(
            ArchiveDigestMember.content_digest == a1["content_digest"]).all()
        alive = [m for m in members
                 if db.get(ReplayArchive, m.archive_id).cleaned_at is None]
        assert len(alive) == 1 and alive[0].archive_id == a1["id"]
    finally:
        db.close()
    # 再清理 a1(最后引用): 物理文件删除
    p = create_cleanup(client, [a1["id"]], key="kc2")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[a1["id"]]["status"] == "CLEANED"
    assert not os.path.exists(path)


def test_cleanup_canonical_transfers_file_to_referencing_archive(client):
    rid1, _, _, _ = completed_replay(client, ranges=((1, 5), (6, 10)),
                                     key="kp1")
    rid2, _, _, _ = completed_replay(client, ranges=((11, 15), (16, 20)),
                                     key="kp2")
    a1 = create_completed_archive(client, rid1, key="ka1")
    a2 = create_completed_archive(client, rid2, key="ka2")
    _force_same_digest(a1["id"], a2["id"])
    db = SessionLocal()
    try:
        real_path = db.get(ReplayArchive, a1["id"]).package_path
    finally:
        db.close()
    # 清理 canonical a1: 文件移交给 a2, a2 记录不受影响
    p = create_cleanup(client, [a1["id"]], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[a1["id"]]["status"] == "CLEANED"
    assert a2["id"] in items[a1["id"]]["reason"] and "移交" in items[a1["id"]]["reason"]
    assert os.path.exists(real_path)  # 仍被 a2 引用, 文件保留
    # a2 升级为 canonical, 引用数 = 1, 可下载/校验
    v2 = client.get(f"/api/admin/archives/{a2['id']}").json()
    assert v2["digest_group"]["is_canonical"] is True
    assert v2["digest_group"]["reference_count"] == 1
    assert v2["package_available"] is True
    r = client.get(f"/api/admin/archives/{a2['id']}/verify")
    assert r.json()["valid"] is True
    assert any(e["event"] == "cleanup.transfer" for e in v2["events"])
    # a2 最后清理: 文件删除
    p = create_cleanup(client, [a2["id"]], key="kc2")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[a2["id"]]["status"] == "CLEANED"
    assert not os.path.exists(real_path)


def test_cleanup_canonical_transfer_failure_skips(client):
    """canonical 的物理包损坏且后继成员不可接管时: digest_referenced 跳过,
    原归档不被删除, 仍被引用的记录与文件不受影响。"""
    rid1, _, _, _ = completed_replay(client, ranges=((1, 5), (6, 10)),
                                     key="kp1")
    rid2, _, _, _ = completed_replay(client, ranges=((11, 15), (16, 20)),
                                     key="kp2")
    a1 = create_completed_archive(client, rid1, key="ka1")
    a2 = create_completed_archive(client, rid2, key="ka2")
    _force_same_digest(a1["id"], a2["id"])
    # 破坏共享物理包: 改写 zip 中载荷使内容摘要不再匹配(zip 末尾追加字节会被
    # 解析器忽略, 这里直接重写 metadata.json 条目)
    import zipfile
    db = SessionLocal()
    try:
        path = db.get(ReplayArchive, a1["id"]).package_path
    finally:
        db.close()
    with zipfile.ZipFile(path) as zf:
        blobs = {n: zf.read(n) for n in zf.namelist()}
    blobs["metadata.json"] = blobs["metadata.json"].replace(
        b'"immutable": true', b'"immutable": false')
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, blob in blobs.items():
            zf.writestr(n, blob)
    p = create_cleanup(client, [a1["id"]], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    items = item_map(get_cleanup(client, p))
    assert items[a1["id"]]["status"] == "SKIPPED"
    assert items[a1["id"]]["reason_code"] == "digest_referenced"
    # 原归档未被清理, 引用记录完好
    v = client.get(f"/api/admin/archives/{a1['id']}").json()
    assert v["cleaned"] is False
    r = client.get(f"/api/admin/archives/by-digest/{a1['content_digest']}")
    assert r.json()["digest_groups"][0]["reference_count"] == 2


# ---------- 重启持久化 ----------

def test_restart_preserves_retention_references_and_cleanup_progress(client):
    rid1, _, _, _ = completed_replay(client, ranges=((1, 5), (6, 10)),
                                     key="kp1")
    rid2, _, _, _ = completed_replay(client, ranges=((11, 15), (16, 20)),
                                     key="kp2")
    rid3, _, _, _ = completed_replay(client, ranges=((21, 25), (26, 30)),
                                     key="kp3")
    a1 = create_completed_archive(client, rid1, key="ka1")
    a2 = create_completed_archive(client, rid2, key="ka2")
    a3 = create_completed_archive(client, rid3, key="ka3")
    future = (dt.datetime.utcnow() + dt.timedelta(days=30)).isoformat()
    set_retention(client, a1["id"], "PERMANENT", "kr1")
    set_retention(client, a2["id"], "UNTIL", "kr2", retain_until=future)
    _force_same_digest(a2["id"], a3["id"])
    # 清理计划处理一部分后暂停
    p = create_cleanup(client, [a1["id"], a2["id"], a3["id"]], key="kc1")["plan_id"]
    _claim_cleanup()
    _tick_cleanup(p)
    cleanup_action(client, p, "pause", "kpause")
    before = get_cleanup(client, p)
    # 模拟重启: 重新 startup, 再跑对账(RUNNING 计划由 TestClient startup 处理,
    # 这里显式调用以覆盖遗留 RUNNING 场景)
    db = SessionLocal()
    try:
        row = db.get(ArchiveCleanupPlan, p)
        row.status = "RUNNING"
        db.commit()
    finally:
        db.close()
    with TestClient(app):
        db = SessionLocal()
        try:
            cleanup.boot_recover_cleanup_plans(db)
        finally:
            db.close()
        # 保留策略保留
        v1 = client.get(f"/api/admin/archives/{a1['id']}").json()
        v2 = client.get(f"/api/admin/archives/{a2['id']}").json()
        assert v1["retention"]["mode"] == "PERMANENT"
        assert v2["retention"]["mode"] == "UNTIL" and v2["retention"]["retained"]
        # 引用关系保留
        r = client.get(f"/api/admin/archives/by-digest/{a2['content_digest']}")
        assert r.json()["digest_groups"][0]["reference_count"] == 2
        # 清理进度与逐项结果保留, RUNNING 复位 QUEUED
        view = get_cleanup(client, p)
        assert view["status"] == "QUEUED"
        assert view["progress"] == before["progress"]
        assert len([i for i in view["items"]
                    if i["status"] in ("CLEANED", "SKIPPED")]) == \
            before["progress"]["processed"]
        # 恢复跑完: 永久保留的 a1 仍跳过
        cleanup_action(client, p, "resume", "kresume")
        run_cleanup_plan(p)
        view = get_cleanup(client, p)
        assert view["status"] == "COMPLETED"
        items = item_map(view)
        assert items[a1["id"]]["reason_code"] == "retained_permanent"
        assert client.get(f"/api/admin/archives/{a1['id']}").json()["cleaned"] is False


def test_boot_clears_stale_use_counters(client):
    """崩溃遗留的下载/校验计数在重启时清零, 不会永久阻止清理。"""
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    db = SessionLocal()
    try:
        row = db.get(ReplayArchive, a["id"])
        row.active_downloads = 2
        row.active_verifies = 1
        db.commit()
    finally:
        db.close()
    with TestClient(app):
        db = SessionLocal()
        try:
            archives.boot_recover_archives(db)
            row = db.get(ReplayArchive, a["id"])
            assert row.active_downloads == 0 and row.active_verifies == 0
        finally:
            db.close()
    p = create_cleanup(client, [a["id"]], key="kc")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[a["id"]]["status"] == "CLEANED"


def test_failed_cleanup_plan_resume_retries_failed_items(client):
    """计划执行中单项未预期错误 -> 计划 FAILED 留痕; resume 后失败项重新排队,
    已 CLEANED/SKIPPED 项不二次处理。"""
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    p = create_cleanup(client, [a["id"]], key="kc1")["plan_id"]
    # 直接把计划置 FAILED 并把项置 FAILED(模拟执行期未预期错误)
    db = SessionLocal()
    try:
        from app.models import ArchiveCleanupItem
        cleanup.fail_plan(db, p, "构造失败状态")
        item = db.query(ArchiveCleanupItem).filter(
            ArchiveCleanupItem.plan_id == p).one()
        item.status = "FAILED"
        item.reason_code = "internal_error"
        item.reason = "构造的失败项"
        row = db.get(ArchiveCleanupPlan, p)
        row.failed_items = 1
        db.commit()
    finally:
        db.close()
    assert get_cleanup(client, p)["status"] == "FAILED"
    cleanup_action(client, p, "resume", "kr")
    run_cleanup_plan(p)
    view = get_cleanup(client, p)
    assert view["status"] == "COMPLETED"
    assert item_map(view)[a["id"]]["status"] == "CLEANED"


def test_status_includes_cleanup_plans(client):
    rid, _, _, _ = completed_replay(client)
    a = create_completed_archive(client, rid)
    create_cleanup(client, [a["id"]], key="kc1")
    s = client.get("/api/status").json()
    assert len(s["cleanup_plans"]) == 1
    assert s["cleanup_plans"][0]["total_items"] == 1
    # 已清理归档不进 status 归档区
    run_cleanup_plan(s["cleanup_plans"][0]["id"])
    s = client.get("/api/status").json()
    assert s["archives"] == []


def test_rearchive_after_cleanup_creates_fresh_archive(client):
    """归档被清理(软删除)后允许对同一(回放,版本)重新归档; 新归档重新登记引用。"""
    rid, _, _, _ = completed_replay(client)
    a1 = create_completed_archive(client, rid, key="ka1")
    p = create_cleanup(client, [a1["id"]], key="kc1")["plan_id"]
    run_cleanup_plan(p)
    assert item_map(get_cleanup(client, p))[a1["id"]]["status"] == "CLEANED"
    r = client.post("/api/admin/archives",
                    json={"operator": "carol", "idempotency_key": "ka2",
                          "replay_id": rid, "report_version": 1})
    assert r.status_code == 201, r.text
    aid2 = r.json()["archive_id"]
    assert aid2 != a1["id"]
    _run_archive(aid2)
    v2 = client.get(f"/api/admin/archives/{aid2}").json()
    assert v2["status"] == "COMPLETED" and v2["digest_group"]["is_canonical"]
    # 旧记录仍可查(软删除), 新记录是目录中唯一的存活归档
    assert client.get(f"/api/admin/archives/{a1['id']}").json()["cleaned"] is True
    listed = client.get("/api/admin/archives").json()
    assert [x["id"] for x in listed] == [aid2]
