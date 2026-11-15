"""核心语义测试：上传校验、游标快照、合并切换、回收安全、崩溃恢复。"""
from __future__ import annotations

import io
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.merger import manager


def _parquet(rows: int, tag: int, dup_from: pa.Table | None = None) -> bytes:
    base = tag * 10_000
    stations = [f"ST-{(i + tag) % 4 + 1:03d}" for i in range(rows)]
    ts = [pa.scalar(base + i, type=pa.timestamp("us", tz="UTC")).as_py()
          for i in range(rows)]
    temps = [20.0 + (i % 7) + tag for i in range(rows)]
    table = pa.table({
        "station_id": pa.array(stations, pa.string()),
        "observed_at": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "temperature": pa.array(temps, pa.float64()),
    })
    if dup_from is not None:
        head = dup_from.slice(dup_from.num_rows - 2, 2)
        table = pa.concat_tables([head, table.slice(2, rows - 2)])
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue(), table


client = TestClient(app)


def _wait_merge(state, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = client.get("/merges/status").json()["active_merge"]
        if m and m["state"] == state:
            return m
        time.sleep(0.1)
    raise AssertionError("merge never reached %s" % state)


def _wait_terminal(timeout=30):
    """等所有后台合并任务走到终态，消除测试间的线程竞争。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        m = client.get("/merges/status").json()["active_merge"]
        last = m
        if m is None or m["state"] in ("completed", "failed", "cancelled"):
            return m
        time.sleep(0.1)
    raise AssertionError("merge did not settle, last=%s" % last)


def _start_merge_retry():
    """启动合并；若残留活动任务则等其终态后重试。"""
    for _ in range(3):
        r = client.post("/merges")
        if r.status_code == 202:
            return r
        assert r.status_code == 409, r.json()
        _wait_terminal()
    status = client.get("/merges/status").json()
    files = client.get("/files").json()["files"]
    raise AssertionError(
        f"无法启动新的合并任务 status={status} "
        f"active_files={[f['file_id'] for f in files if f['status']=='active']}")


def _active_files():
    return [f for f in client.get("/files").json()["files"]
            if f["status"] == "active"]


def _close_all_cursors():
    """关闭全部遗留游标，消除测试间的回收引用耦合。"""
    for c in client.get("/cursors").json()["cursors"]:
        if not c["closed"]:
            client.post(f"/cursors/{c['cursor_id']}/close")


def test_health_and_empty_listing():
    r = client.get("/health")
    assert r.status_code == 200
    assert client.get("/files").json()["files"] == []


def test_upload_validation_errors():
    # 非 parquet
    r = client.post("/uploads", content=b"not parquet")
    assert r.status_code == 422
    # 空 body
    assert client.post("/uploads", content=b"").status_code == 422
    # 缺列
    bad = pa.table({"station_id": pa.array(["a"]),
                    "observed_at": pa.array(
                        [pa.scalar(1, type=pa.timestamp("us", tz="UTC")).as_py()],
                        pa.timestamp("us", tz="UTC"))})
    buf = io.BytesIO()
    pq.write_table(bad, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422
    # 多余列
    bad2 = pa.table({
        "station_id": pa.array(["a"]),
        "observed_at": pa.array(
            [pa.scalar(1, type=pa.timestamp("us", tz="UTC")).as_py()],
            pa.timestamp("us", tz="UTC")),
        "temperature": pa.array([1.0]),
        "extra": pa.array([1]),
    })
    buf = io.BytesIO()
    pq.write_table(bad2, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422
    # 空表
    empty = bad2.schema.empty_table()
    buf = io.BytesIO()
    pq.write_table(empty, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422


def test_size_limit_413(monkeypatch):
    monkeypatch.setattr(config, "MAX_UPLOAD_BYTES", 64)
    data, _ = _parquet(50, 1)
    r = client.post("/uploads", content=data)
    assert r.status_code == 413
    monkeypatch.setattr(config, "MAX_UPLOAD_BYTES", 8 * 1024 * 1024)


def test_full_flow_cursor_merge_reclaim():
    # 清理前序测试遗留的游标/obsolete 文件，保证本测试的计数断言精确
    _close_all_cursors()
    client.post("/merges/reclaim")
    # 上传 4 个文件，相邻文件含 2 行重复
    total = 0
    prev = None
    for tag in range(4):
        data, table = _parquet(50, tag, dup_from=prev)
        prev = table
        r = client.post("/uploads", content=data)
        assert r.status_code == 200
        total += r.json()["row_count"]
    assert total == 200

    # 游标快照固定
    cur = client.post("/cursors").json()
    cid = cur["cursor_id"]
    assert cur["snapshot_rows"] == total

    # 分页 + 重复行保留
    rows = []
    while True:
        page = client.get(f"/cursors/{cid}/pages?limit=37").json()
        rows.extend(page["rows"])
        if page["exhausted"]:
            break
    assert len(rows) == total
    keys = [(r["station_id"], r["observed_at"], r["temperature"]) for r in rows]
    assert len(keys) - len(set(keys)) >= 6  # 3 个接缝 x 2 行重复

    # 第二个游标，合并期间上传的文件不进旧快照
    cur2 = client.post("/cursors").json()
    cid2 = cur2["cursor_id"]

    # 启动合并（同期只能一个）
    assert client.post("/merges").status_code == 202
    assert client.post("/merges").status_code == 409
    m = _wait_merge("completed")
    assert m["output_file_id"]

    # 合并期间/之后上传新文件
    data, extra_table = _parquet(30, 99)
    extra = client.post("/uploads", content=data).json()
    assert extra["row_count"] == 30

    files = client.get("/files").json()["files"]
    active = [f for f in files if f["status"] == "active"]
    obsolete = [f for f in files if f["status"] == "obsolete"]
    merged = [f for f in active if f["kind"] == "merged"]
    assert len(merged) == 1 and merged[0]["row_count"] == total
    assert len(obsolete) == 4

    # 旧游标仍然完整可读（obsolete 文件未回收），内容不变
    old_rows = []
    while True:
        p = client.get(f"/cursors/{cid2}/pages?limit=64").json()
        old_rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert old_rows == rows

    # 新游标看到 merged + 新文件，且合并段行序与旧游标一致
    cur3 = client.post("/cursors").json()
    assert cur3["snapshot_rows"] == total + 30
    cid3 = cur3["cursor_id"]
    new_rows = []
    while True:
        p = client.get(f"/cursors/{cid3}/pages?limit=128").json()
        new_rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert new_rows[:total] == rows

    # 旧游标未关闭时 reclaim 不能删 obsolete 文件
    rec = client.post("/merges/reclaim").json()
    assert rec["reclaimed"] == []
    disk = set(os.listdir(config.FILES_DIR))
    assert len(disk) == 6  # 4 obsolete + 1 merged + 1 新上传

    section_step = None
    # 关闭全部旧游标后回收，active 文件不受影响
    r1 = client.post(f"/cursors/{cid}/close").json()
    r2 = client.post(f"/cursors/{cid2}/close").json()
    r3 = client.post(f"/cursors/{cid3}/close").json()
    rec = client.post("/merges/reclaim").json()
    reclaimed = set(r1["reclaimed"] + r2["reclaimed"] + r3["reclaimed"]
                    + rec["reclaimed"])
    assert len(reclaimed) == 4
    remaining = client.get("/files").json()["files"]
    assert all(f["status"] == "active" for f in remaining)
    disk_after = set(os.listdir(config.FILES_DIR))
    assert disk_after == {f["file_name"] for f in remaining}


def test_pagination_across_file_boundaries():
    """页大小恰好等于文件行数时，必须跨文件继续读完，不能在边界提前耗尽。"""
    _wait_terminal()
    _close_all_cursors()
    client.post("/merges/reclaim")
    per_file = 40
    n_files = 3
    for tag in range(n_files):
        data, _ = _parquet(per_file, tag + 200)
        client.post("/uploads", content=data)
    created_rows = per_file * n_files
    snap = client.post("/cursors").json()
    cid = snap["cursor_id"]
    total = snap["snapshot_rows"]
    assert total >= created_rows

    # 每页正好一个文件的行数，验证跨文件边界不会提前耗尽或多出空页
    rows = []
    while True:
        p = client.get(f"/cursors/{cid}/pages?limit={per_file}").json()
        rows.extend(p["rows"])
        if p["exhausted"]:
            # 最后一页之外不允许出现空页
            assert p["returned"] > 0
            break
        assert p["returned"] == per_file
    assert len(rows) == total

    # limit=1 逐行翻页也必须完整且行序一致
    cid2 = client.post("/cursors").json()["cursor_id"]
    rows2 = []
    while True:
        p = client.get(f"/cursors/{cid2}/pages?limit=1").json()
        rows2.extend(p["rows"])
        if p["exhausted"]:
            break
    assert len(rows2) == total
    assert rows2 == rows
    _close_all_cursors()


def test_pause_resume_and_concurrent_upload():
    _wait_terminal()
    # 当前 active：merged + 额外上传，再来一轮合并并暂停
    client.post("/merges/pause")  # 没有任务时 409 可接受
    r = _start_merge_retry()
    assert r.status_code == 202
    pr = client.post("/merges/pause")
    assert pr.status_code == 200
    time.sleep(0.3)
    # 暂停期间上传仍可用
    data, _ = _parquet(10, 33)
    assert client.post("/uploads", content=data).status_code == 200
    client.post("/merges/resume")
    _wait_merge("completed")


def test_fail_injection_keeps_original_data():
    _wait_terminal()
    _close_all_cursors()
    before = client.get("/files").json()["files"]
    before_merged = [f["file_id"] for f in before if f["kind"] == "merged"]
    client.post("/merges/fail-inject/switch")
    r = _start_merge_retry()
    assert r.status_code == 202
    m = _wait_merge("failed")
    assert m["fail_at_step"] == "switch"
    after = client.get("/files").json()["files"]
    # 失败不改变文件集合，也不产生新的 merged 产物
    assert [f["file_id"] for f in after] == [f["file_id"] for f in before]
    assert all(f["status"] == "active" for f in after)
    assert [f["file_id"] for f in after if f["kind"] == "merged"] == before_merged
    # tmp 被清理
    assert not [n for n in os.listdir(config.TMP_DIR) if n.endswith(".tmp")]

    # 再注入 stream 故障：同样不得改变文件集合
    _wait_terminal()
    client.post("/merges/fail-inject/stream")
    _start_merge_retry()
    _wait_merge("failed")
    after2 = client.get("/files").json()["files"]
    assert [f["file_id"] for f in after2] == [f["file_id"] for f in before]
    assert all(f["status"] == "active" for f in after2)

    # 清除后合并成功
    _wait_terminal()
    _start_merge_retry()
    _wait_merge("completed")
    _wait_terminal()


def test_restart_recovery_open_cursor_and_reclaim():
    """模拟进程重启：重新执行 startup 恢复钩子，校验游标与回收。"""
    _wait_terminal()
    # 保证至少有一个 active 文件可供新游标快照
    if not _active_files():
        data, _ = _parquet(25, 77)
        client.post("/uploads", content=data)
    # 先建一个引用当前 active 文件的游标并读一部分，保持打开
    cur = client.post("/cursors").json()
    cid = cur["cursor_id"]
    total = cur["snapshot_rows"]
    assert total > 0
    page = client.get(f"/cursors/{cid}/pages?limit=11").json()
    assert page["returned"] == min(11, total)

    # 重启恢复
    result = manager.recover_on_startup(auto_merge=False)
    assert isinstance(result["reclaimed"], list)

    # 游标仍可续读且总量不变
    got = page["rows_read"]
    while True:
        p = client.get(f"/cursors/{cid}/pages?limit=97").json()
        got += p["returned"]
        if p["exhausted"]:
            break
    assert got == total

    # 关闭后回收，磁盘与元数据一致，无 active 被误删
    client.post(f"/cursors/{cid}/close")
    client.post("/merges/reclaim")
    files = client.get("/files").json()["files"]
    disk = {f["file_name"] for f in files}
    assert set(os.listdir(config.FILES_DIR)) == disk
    assert all(f["status"] == "active" for f in files)


def test_restart_during_switching_marks_failed_keeps_inputs():
    # 手工制造一个 switching 状态的合并，再走恢复逻辑
    _wait_terminal()
    from app.db import get_conn
    conn = get_conn()
    active_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM files WHERE status='active'").fetchall()]
    cur = conn.execute(
        "INSERT INTO merges(state) VALUES ('switching')")
    mid = cur.lastrowid
    for pos, fid in enumerate(active_ids):
        conn.execute(
            "INSERT INTO merge_files(merge_id,file_id,position) VALUES (?,?,?)",
            (mid, fid, pos))
    conn.commit()
    # 放一个伪 tmp，恢复时应被清理
    tmp = os.path.join(config.TMP_DIR, f"merge-{mid}.parquet.tmp")
    with open(tmp, "wb") as f:
        f.write(b"x")

    manager.recover_on_startup(auto_merge=False)
    state = conn.execute("SELECT state FROM merges WHERE id=?", (mid,)).fetchone()
    assert state["state"] == "failed"
    assert all(r["status"] == "active" for r in conn.execute(
        "SELECT status FROM files WHERE id IN (%s)"
        % ",".join("?" * len(active_ids)), active_ids).fetchall())
    assert not os.path.exists(tmp)




# ======================= 筛选游标 + 站点时间范围索引 =======================

def _filtered_parquet(spec: list[tuple[str, str, float]]) -> tuple[bytes, pa.Table]:
    """按 (station_id, ISO8601, temperature) 列表构造 Parquet。"""
    from datetime import datetime

    stations = [s for s, _, _ in spec]
    ts = [datetime.fromisoformat(t.replace("Z", "+00:00")) for _, t, _ in spec]
    temps = [x for _, _, x in spec]
    table = pa.table({
        "station_id": pa.array(stations, pa.string()),
        "observed_at": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "temperature": pa.array(temps, pa.float64()),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue(), table


def _drain(cid: str, limit: int = 7) -> tuple[list[dict], list[dict]]:
    rows: list[dict] = []
    pages: list[dict] = []
    while True:
        p = client.get(f"/cursors/{cid}/pages?limit={limit}").json()
        pages.append(p)
        rows.extend(p["rows"])
        if p["exhausted"]:
            return rows, pages
        assert p["returned"] <= limit


def _create_filtered(station_id, start, end, expect=200):
    r = client.post("/cursors", json={
        "station_id": station_id, "start": start, "end": end})
    assert r.status_code == expect, r.text
    return r.json()


def _reset_lake():
    """关闭遗留游标并回收，避免前序用例的引用干扰。"""
    _wait_terminal()
    _close_all_cursors()
    client.post("/merges/reclaim")


def _file_pk(file_id: str) -> int:
    from app.db import get_conn
    return get_conn().execute(
        "SELECT id FROM files WHERE file_id=?", (file_id,)
    ).fetchone()["id"]


def _snapshot_file_ids(cursor_id: str) -> set[str]:
    from app.db import get_conn
    rows = get_conn().execute(
        """SELECT f.file_id FROM cursor_files cf JOIN files f ON f.id=cf.file_id
           WHERE cf.cursor_id=?""", (cursor_id,)).fetchall()
    return {r["file_id"] for r in rows}


def test_filtered_cursor_only_matches_and_preserves_order_and_duplicates():
    _reset_lake()
    # 三个文件，覆盖两个站点，窗口内/窗口外/边界值/重复行齐全
    f1 = _filtered_parquet([
        ("ST-F1", "2026-11-13T00:00:00Z", 1.0),
        ("ST-F2", "2026-11-13T00:05:00Z", 2.0),
        ("ST-F1", "2026-11-13T00:10:00Z", 3.0),
        ("ST-F1", "2026-11-13T00:10:00Z", 3.0),   # 重复行必须保留
    ])
    f2 = _filtered_parquet([
        ("ST-F1", "2026-11-13T00:59:59.999999Z", 4.0),  # 右端点前一行
        ("ST-F1", "2026-11-13T01:00:00Z", 5.0),         # 左闭右开：排除
        ("ST-F2", "2026-11-13T00:30:00Z", 6.0),
        ("ST-F1", "2026-11-13T00:20:00Z", 7.0),
        ("ST-F3", "2026-11-13T00:40:00Z", 8.0),
    ])
    f3 = _filtered_parquet([
        ("ST-F1", "2026-11-13T02:00:00Z", 9.0),   # 整个文件都在窗口之后
        ("ST-F1", "2026-11-13T03:00:00Z", 10.0),
    ])
    ids = []
    for data, _table in (f1, f2, f3):
        info = client.post("/uploads", content=data).json()
        ids.append(info["file_id"])
        assert info["indexed_stations"] >= 1

    # 文件级/站点级索引可查
    idx = client.get(f"/files/{ids[0]}/ranges").json()
    assert idx["indexed"] is True
    by_station = {r["station_id"]: r for r in idx["ranges"]}
    assert by_station["ST-F1"]["row_count"] == 3
    assert by_station["ST-F1"]["observed_min"] == "2026-11-13T00:00:00+00:00"
    assert by_station["ST-F1"]["observed_max"] == "2026-11-13T00:10:00+00:00"

    start, end = "2026-11-13T00:00:00Z", "2026-11-13T01:00:00Z"
    created = _create_filtered("ST-F1", start, end)
    cid = created["cursor_id"]
    snap = _snapshot_file_ids(cid)
    assert ids[0] in snap and ids[1] in snap
    assert ids[2] not in snap, "整文件确定不命中，必须被索引跳过"
    # 快照只含命中候选文件：f3 及其它前序用例的无关文件都被跳过
    assert created["snapshot_files"] == len(snap)

    rows, pages = _drain(cid, limit=2)
    got = [(r["station_id"], r["observed_at"], r["temperature"]) for r in rows]
    assert got == [
        ("ST-F1", "2026-11-13T00:00:00+00:00", 1.0),
        ("ST-F1", "2026-11-13T00:10:00+00:00", 3.0),
        ("ST-F1", "2026-11-13T00:10:00+00:00", 3.0),
        ("ST-F1", "2026-11-13T00:59:59.999999+00:00", 4.0),
        ("ST-F1", "2026-11-13T00:20:00+00:00", 7.0),
    ]
    assert pages[-1]["total_rows"] == 5
    assert pages[-1]["matched_rows"] == 5
    assert all(p["rows"] for p in pages), "分页只返回命中行"

    # 另一个站点 + 更窄时段：只有 f1 的 ST-F2 行命中
    w = _create_filtered("ST-F2", "2026-11-13T00:04:00Z",
                         "2026-11-13T00:06:00Z")
    wrows, _ = _drain(w["cursor_id"], limit=10)
    assert [(r["station_id"], r["temperature"]) for r in wrows] == [("ST-F2", 2.0)]
    assert ids[1] not in _snapshot_file_ids(w["cursor_id"])

    # 无命中的游标：立即耗尽且返回 0 行
    none = _create_filtered("ST-F404", start, end)
    nrows, npages = _drain(none["cursor_id"], limit=10)
    assert nrows == []
    assert npages[-1]["exhausted"] is True
    assert npages[-1]["total_rows"] == 0
    _close_all_cursors()


def test_filtered_cursor_snapshot_ignores_later_uploads_and_merges():
    _reset_lake()
    data1, _ = _filtered_parquet([
        ("ST-G9", "2026-11-13T03:00:00Z", 1.0),
        ("ST-G9", "2026-11-13T03:10:00Z", 2.0),
    ])
    client.post("/uploads", content=data1)
    created = _create_filtered("ST-G9", "2026-11-13T03:00:00Z",
                               "2026-11-13T04:00:00Z")
    cid = created["cursor_id"]
    first = client.get(f"/cursors/{cid}/pages?limit=1").json()
    assert first["returned"] == 1

    # 创建后补传：不得混入旧筛选游标
    data2, _ = _filtered_parquet([("ST-G9", "2026-11-13T03:30:00Z", 3.0)])
    client.post("/uploads", content=data2)

    # 合并把创建时快照输入变 obsolete；游标仍可继续翻页
    _start_merge_retry()
    _wait_merge("completed")

    rest, _pages = _drain(cid, limit=5)
    rows = first["rows"] + rest
    assert [(r["station_id"], r["temperature"]) for r in rows] == [
        ("ST-G9", 1.0), ("ST-G9", 2.0)
    ]
    # 新筛选游标看到合并产物 + 补传文件（补传文件随后也被并入了产物）
    new = _create_filtered("ST-G9", "2026-11-13T03:00:00Z",
                           "2026-11-13T04:00:00Z")
    nrows, _ = _drain(new["cursor_id"], limit=10)
    assert [r["temperature"] for r in nrows] == [1.0, 2.0, 3.0]
    _close_all_cursors()


def test_filtered_cursor_resumes_by_raw_line_position_after_restart():
    _reset_lake()
    # 前 50 行是别的站点，之后目标站点分散出现：进度必须按原始行位置保存
    spec = [("ST-H7", f"2026-11-13T05:00:{i % 60:02d}Z", float(i))
            for i in range(50)]
    spec += [("ST-H8", "2026-11-13T05:30:00Z", 1.0),
             ("ST-H7", "2026-11-13T05:30:01Z", 2.0),
             ("ST-H8", "2026-11-13T05:30:02Z", 3.0),
             ("ST-H8", "2026-11-13T05:30:03Z", 4.0)]
    data, _ = _filtered_parquet(spec)
    info = client.post("/uploads", content=data).json()
    fpk = _file_pk(info["file_id"])
    created = _create_filtered("ST-H8", "2026-11-13T05:00:00Z",
                               "2026-11-13T06:00:00Z")
    cid = created["cursor_id"]
    p1 = client.get(f"/cursors/{cid}/pages?limit=2").json()
    assert [r["temperature"] for r in p1["rows"]] == [1.0, 3.0]
    assert p1["exhausted"] is False

    # 持久化的是原始行位置：第 3 个命中行之前共扫描 53 行（50+2+1）
    from app.db import get_conn
    pos = get_conn().execute(
        "SELECT rows_read FROM cursor_positions WHERE cursor_id=? AND file_id=?",
        (cid, fpk)).fetchone()["rows_read"]
    assert pos == 53, pos

    # 重启恢复后按同一原始位置续读，不重不漏
    manager.recover_on_startup(auto_merge=False)
    rest, pages = _drain(cid, limit=10)
    assert [r["temperature"] for r in rest] == [4.0]
    assert pages[-1]["total_rows"] == 3
    _close_all_cursors()


def test_filtered_cursor_scans_unindexed_legacy_file():
    _reset_lake()
    data, _ = _filtered_parquet([
        ("ST-J80", "2026-11-13T07:00:00Z", 1.0),
        ("ST-J81", "2026-11-13T07:10:00Z", 2.0),
    ])
    info = client.post("/uploads", content=data).json()
    assert info["indexed_stations"] >= 1

    # 模拟“旧的无索引文件”：清空索引行并把 indexed 置 0
    from app.db import get_conn
    conn = get_conn()
    fpk = _file_pk(info["file_id"])
    conn.execute(
        """UPDATE files SET indexed=0, min_observed_us=NULL,
           max_observed_us=NULL WHERE id=?""", (fpk,))
    conn.execute("DELETE FROM file_station_ranges WHERE file_id=?", (fpk,))
    conn.commit()

    created = _create_filtered("ST-J81", "2026-11-13T07:00:00Z",
                               "2026-11-13T08:00:00Z")
    # 无索引文件不得被跳过，且仍能命中
    assert info["file_id"] in _snapshot_file_ids(created["cursor_id"])
    rows, _ = _drain(created["cursor_id"], limit=10)
    assert [(r["station_id"], r["temperature"]) for r in rows] == [("ST-J81", 2.0)]
    _close_all_cursors()


def test_filtered_index_switch_is_atomic_on_failure():
    _reset_lake()
    data, _ = _filtered_parquet([("ST-K90", "2026-11-13T09:00:00Z", 1.0)])
    client.post("/uploads", content=data)
    before = client.get("/files").json()["files"]
    before_idx = {
        f["file_id"]: client.get(f"/files/{f['file_id']}/ranges").json()
        for f in before
    }

    # switch 阶段失败：不得留下新产物或任何索引残留
    client.post("/merges/fail-inject/switch")
    _start_merge_retry()
    _wait_merge("failed")
    after = client.get("/files").json()["files"]
    assert {f["file_id"] for f in after} == set(before_idx)
    for f in after:
        assert client.get(f"/files/{f['file_id']}/ranges").json() == \
            before_idx[f["file_id"]]
    from app.db import get_conn
    conn = get_conn()
    orphan = conn.execute(
        """SELECT COUNT(*) AS n FROM file_station_ranges r
           LEFT JOIN files f ON f.id = r.file_id WHERE f.id IS NULL"""
    ).fetchone()["n"]
    assert orphan == 0

    # 成功合并后产物索引覆盖全部输入站点（含旧无索引文件的站点），
    # 与直接从产物 Parquet 重新计算的结果完全一致
    _start_merge_retry()
    _wait_merge("completed")
    merged = [f for f in client.get("/files").json()["files"]
              if f["kind"] == "merged" and f["status"] == "active"]
    assert len(merged) == 1
    idx = client.get(f"/files/{merged[0]['file_id']}/ranges").json()
    from app import config, storage
    import pyarrow.parquet as pq
    path = os.path.join(config.FILES_DIR, merged[0]["file_name"])
    physical, _lo, _hi = storage.station_ranges_from_table(pq.read_table(path))
    expected = {
        sid: {
            "row_count": cnt,
            "min": storage.us_to_iso(lo),
            "max": storage.us_to_iso(hi),
        }
        for sid, cnt, lo, hi in physical
    }
    got = {
        r["station_id"]: {
            "row_count": r["row_count"],
            "min": r["observed_min"],
            "max": r["observed_max"],
        }
        for r in idx["ranges"]
    }
    assert got == expected
    _close_all_cursors()


def test_filter_parameter_validation():
    bad = [
        {"station_id": "ST-001"},
        {"station_id": "ST-001", "start": "2026-11-13T00:00:00Z"},
        {"start": "2026-11-13T00:00:00Z", "end": "2026-11-13T01:00:00Z"},
        {"station_id": "ST-001", "start": "2026-11-13T00:00:00",
         "end": "2026-11-13T01:00:00Z"},  # naive
        {"station_id": "ST-001", "start": "not-a-time",
         "end": "2026-11-13T01:00:00Z"},
        {"station_id": "ST-001", "start": "2026-11-13T01:00:00Z",
         "end": "2026-11-13T01:00:00Z"},  # 空区间
        {"station_id": "ST-001", "start": "2026-11-13T02:00:00Z",
         "end": "2026-11-13T01:00:00Z"},  # 反向
        {"station_id": "", "start": "2026-11-13T00:00:00Z",
         "end": "2026-11-13T01:00:00Z"},
        {"station_id": "ST-001", "start": "2026-11-13T00:00:00Z",
         "end": "2026-11-13T01:00:00Z", "extra": 1},
    ]
    for body in bad:
        assert client.post("/cursors", json=body).status_code == 422, body
    # body 为空 / 无 body 仍然是原有无筛选游标
    assert client.post("/cursors").status_code == 200
    assert client.post("/cursors", json={}).status_code == 200
    # 时区偏移被正确换算成 UTC
    r = client.post("/cursors", json={
        "station_id": "ST-NOBODY",
        "start": "2026-11-13T08:00:00+08:00",
        "end": "2026-11-13T09:00:00+08:00",
    })
    assert r.status_code == 200
    assert r.json()["filter"]["start"] == "2026-11-13T00:00:00+00:00"
    assert r.json()["filter"]["start_us"] == 1794528000000000
    _close_all_cursors()


def test_filtered_scan_across_multiple_row_groups():
    """文件含多个 row group 时，分页的原始行位置必须跨组准确推进。"""
    _reset_lake()
    from datetime import datetime, timedelta, timezone

    n = 500
    base = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)
    stations = ["ST-R1" if i % 4 == 0 else "ST-R2" for i in range(n)]
    ts = [base + timedelta(seconds=i) for i in range(n)]
    table = pa.table({
        "station_id": pa.array(stations, pa.string()),
        "observed_at": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "temperature": pa.array([float(i) for i in range(n)], pa.float64()),
    })
    buf = io.BytesIO()
    # 故意切成很多 row group
    pq.write_table(table, buf, row_group_size=37)
    assert pq.ParquetFile(io.BytesIO(buf.getvalue())).metadata.num_row_groups > 10
    client.post("/uploads", content=buf.getvalue())

    start = base.isoformat()
    end = (base + timedelta(seconds=300)).isoformat()
    created = _create_filtered("ST-R1", start, end)
    expected = [float(i) for i in range(300) if i % 4 == 0]
    for limit in (1, 7, 50):
        cid = _create_filtered("ST-R1", start, end)["cursor_id"]
        got: list[float] = []
        pages = 0
        while True:
            page = client.get(f"/cursors/{cid}/pages?limit={limit}").json()
            pages += 1
            got.extend(r["temperature"] for r in page["rows"])
            if page["exhausted"]:
                break
            assert page["returned"] > 0, "非耗尽页不应为空"
        assert got == expected, (limit, len(got), len(expected))
        assert pages >= 1
        client.post(f"/cursors/{cid}/close")
    assert created["snapshot_files"] >= 1
    _close_all_cursors()


def test_upload_registration_and_index_are_atomic():
    """索引写入失败时，文件登记与落盘文件都必须一起回滚。"""
    from app.db import get_conn

    conn = get_conn()
    conn.execute(
        """CREATE TRIGGER IF NOT EXISTS fail_range_insert
           BEFORE INSERT ON file_station_ranges
           BEGIN SELECT RAISE(ABORT, 'injected index failure'); END"""
    )
    conn.commit()
    before_files = client.get("/files").json()["files"]
    before_disk = set(os.listdir(config.FILES_DIR))
    before_ranges = conn.execute(
        "SELECT COUNT(*) AS n FROM file_station_ranges"
    ).fetchone()["n"]
    before_seq = conn.execute(
        "SELECT value FROM settings WHERE key='file_seq'"
    ).fetchone()["value"]
    data, _ = _filtered_parquet([
        ("ST-Z1", "2026-11-13T11:00:00Z", 1.0),
        ("ST-Z2", "2026-11-13T11:05:00Z", 2.0),
    ])
    # 用不抛出服务端异常的客户端观察 500 响应
    tolerant = TestClient(app, raise_server_exceptions=False)
    try:
        r = tolerant.post("/uploads", content=data)
        assert r.status_code == 500, r.status_code
        after_files = client.get("/files").json()["files"]
        assert after_files == before_files, "失败的登记不得留下文件行"
        assert set(os.listdir(config.FILES_DIR)) == before_disk, \
            "失败的登记不得留下落盘文件"
        orphan = conn.execute(
            "SELECT COUNT(*) AS n FROM file_station_ranges"
        ).fetchone()["n"]
        assert orphan == before_ranges, "失败的登记不得留下索引行"
        # 序号也不应因失败被消耗（序号更新在同一事务里回滚）
        seq = conn.execute(
            "SELECT value FROM settings WHERE key='file_seq'"
        ).fetchone()["value"]
        assert seq == before_seq, (seq, before_seq)
    finally:
        conn.execute("DROP TRIGGER IF EXISTS fail_range_insert")
        conn.commit()
    # 恢复后同一文件可以正常上传
    r = client.post("/uploads", content=data)
    assert r.status_code == 200, r.text
    assert r.json()["indexed_stations"] == 2
    _close_all_cursors()


def test_restart_during_switch_cleans_orphan_output_file():
    """切换阶段 os.replace 已落位但事务未提交时退出：再次启动应清理孤儿产物。"""
    _reset_lake()
    data, _ = _filtered_parquet([("ST-ORP", "2026-11-13T12:00:00Z", 1.0)])
    client.post("/uploads", content=data)

    from app.db import get_conn
    conn = get_conn()
    active_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM files WHERE status='active'").fetchall()]
    cur = conn.execute("INSERT INTO merges(state) VALUES ('switching')")
    mid = cur.lastrowid
    for pos, fid in enumerate(active_ids):
        conn.execute(
            "INSERT INTO merge_files(merge_id,file_id,position) VALUES (?,?,?)",
            (mid, fid, pos))
    conn.commit()

    # 模拟已落位但未登记的产物
    orphan = os.path.join(config.FILES_DIR, f"999999999999-deadbeef-merged-{mid}.parquet")
    with open(orphan, "wb") as f:
        f.write(b"orphan")
    # tmp 半成品也应被清理
    tmp = os.path.join(config.TMP_DIR, f"merge-{mid}.parquet.tmp")
    with open(tmp, "wb") as f:
        f.write(b"x")

    manager.recover_on_startup(auto_merge=False)
    assert not os.path.exists(orphan), "未登记到元数据的孤儿产物必须清理"
    assert not os.path.exists(tmp)
    files = client.get("/files").json()["files"]
    assert all(f["status"] == "active" for f in files)
    disk = set(os.listdir(config.FILES_DIR))
    assert disk == {f["file_name"] for f in files}
    _close_all_cursors()
