"""游标：创建时固定当时已提交的行，之后按上传顺序 + 文件内行序分页。

- 快照只包含创建时 status=active 的文件；合并切换产生的新文件不会
  追加进旧游标，旧游标引用的输入文件转为 obsolete 但在游标读完/关闭前不回收。
- 保留重复行（不做任何去重）。
- 读取位置在每页返回时持久化，进程重启后未关闭游标可继续翻页。
- 可选筛选（station_id + UTC 左闭右开时段）：创建时用每个文件按站点划分的
  时间范围索引跳过“确定不命中”的文件；无索引的旧文件保守整文件扫描。
  筛选游标只返回命中行，但内部进度始终记录原始行位置（文件内偏移）。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from . import storage
from .db import LOCK, get_conn


class CursorError(RuntimeError):
    """游标状态错误，API 层映射为 404/409。"""


class CursorFilterError(ValueError):
    """筛选参数非法，API 层映射为 422。"""


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_utc(value: str, name: str) -> int:
    """把 ISO8601 字符串解析成 UTC 微秒时间戳。

    只接受带时区偏移或 Z 后缀的写法，naive 时间有歧义直接拒绝。
    用整数运算换算，避免浮点丢失微秒。
    """
    if not isinstance(value, str) or not value.strip():
        raise CursorFilterError(f"{name} 必须是非空 ISO8601 字符串")
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise CursorFilterError(f"{name} 不是合法的 ISO8601 时间: {value}") from exc
    if dt.tzinfo is None:
        raise CursorFilterError(
            f"{name} 必须带时区（例如 Z 或 +00:00），naive 时间不被接受"
        )
    delta = dt.astimezone(timezone.utc) - _EPOCH
    return (
        (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    )


def _normalize_filter(
    station_id: str | None, start: str | None, end: str | None
) -> tuple[str, int, int] | None:
    provided = [x is not None for x in (station_id, start, end)]
    if not any(provided):
        return None
    if not all(provided):
        raise CursorFilterError(
            "筛选游标必须同时提供 station_id、start、end 三个参数"
        )
    # 数字站点号也接受（与上传时 station_id 统一转字符串保持一致）
    if isinstance(station_id, int) and not isinstance(station_id, bool):
        station_id = str(station_id)
    if not isinstance(station_id, str) or not station_id.strip():
        raise CursorFilterError("station_id 不能为空")
    start_us = parse_utc(start, "start")
    end_us = parse_utc(end, "end")
    if start_us >= end_us:
        raise CursorFilterError("时段必须满足 start < end（UTC 左闭右开）")
    return station_id, start_us, end_us


def create_cursor(
    station_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    filter_spec = _normalize_filter(station_id, start, end)
    conn = get_conn()
    cursor_id = uuid.uuid4().hex
    with LOCK:
        files = conn.execute(
            """SELECT id, row_count, read_order, seq_order, indexed FROM files
               WHERE status='active' ORDER BY read_order, seq_order"""
        ).fetchall()

        included: list[tuple[object, int]] = []  # (file row, candidate_rows)
        skipped = 0
        if filter_spec is None:
            included = [(f, f["row_count"]) for f in files]
        else:
            sid, start_us, end_us = filter_spec
            for f in files:
                if not f["indexed"]:
                    # 旧的无索引文件：无法排除，必须扫描整文件
                    included.append((f, f["row_count"]))
                    continue
                hit = conn.execute(
                    """SELECT r.row_count AS station_rows
                       FROM file_station_ranges r
                       WHERE r.file_id=? AND r.station_id=?
                         AND r.max_observed_us >= ? AND r.min_observed_us < ?""",
                    (f["id"], sid, start_us, end_us),
                ).fetchone()
                if hit is None:
                    skipped += 1
                    continue
                # 该站在文件内的总行数（含时间窗口外的行）是命中行数的保守上界
                included.append((f, int(hit["station_rows"])))

        candidate_rows = sum(c for _, c in included)
        conn.execute(
            """INSERT INTO cursors
               (id, filter_station, filter_start_us, filter_end_us,
                candidate_rows, files_skipped)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                cursor_id,
                filter_spec[0] if filter_spec else None,
                filter_spec[1] if filter_spec else None,
                filter_spec[2] if filter_spec else None,
                candidate_rows,
                skipped,
            ),
        )
        for pos, (f, _cand) in enumerate(included):
            conn.execute(
                """INSERT INTO cursor_files
                   (cursor_id, file_id, position, row_count)
                   VALUES (?, ?, ?, ?)""",
                (cursor_id, f["id"], pos, f["row_count"]),
            )
            conn.execute(
                "INSERT INTO cursor_positions(cursor_id, file_id, rows_read) "
                "VALUES (?, ?, 0)",
                (cursor_id, f["id"]),
            )
        conn.commit()

    result = {
        "cursor_id": cursor_id,
        "snapshot_files": len(included),
        # 无筛选时 snapshot_rows 即快照总行数（与原实现一致）；
        # 筛选时它是命中行数的保守上界，精确命中数在游标耗尽后给出
        "snapshot_rows": candidate_rows,
        "candidate_rows": candidate_rows,
        "files_skipped": skipped,
        "closed": False,
    }
    if filter_spec is not None:
        result["filter"] = {
            "station_id": filter_spec[0],
            "start": storage.us_to_iso(filter_spec[1]),
            "end": storage.us_to_iso(filter_spec[2]),
            "start_us": filter_spec[1],
            "end_us": filter_spec[2],
            "window": "[start, end)",
        }
    return result


def _get_cursor(conn, cursor_id: str):
    row = conn.execute(
        "SELECT * FROM cursors WHERE id=?", (cursor_id,)
    ).fetchone()
    if row is None:
        raise CursorError(f"游标 {cursor_id} 不存在")
    return row


def next_page(cursor_id: str, limit: int) -> dict:
    conn = get_conn()
    with LOCK:
        cur = _get_cursor(conn, cursor_id)
        if cur["closed"]:
            raise CursorError(f"游标 {cursor_id} 已关闭")

        snapshot = conn.execute(
            """SELECT cf.file_id, cf.position, cf.row_count, f.path,
                      COALESCE(cp.rows_read, 0) AS rows_read
               FROM cursor_files cf
               LEFT JOIN cursor_positions cp
                 ON cp.cursor_id = cf.cursor_id AND cp.file_id = cf.file_id
               JOIN files f ON f.id = cf.file_id
               WHERE cf.cursor_id=?
               ORDER BY cf.position""",
            (cursor_id,),
        ).fetchall()

        filtered = cur["filter_station"] is not None
        remaining = limit
        rows: list[dict] = []
        touched: list[tuple[int, int]] = []
        truncated = False

        for entry in snapshot:
            if remaining <= 0:
                break
            if filtered:
                unread = entry["row_count"] - entry["rows_read"]
                if unread <= 0:
                    continue
                # 从原始行位置继续扫描，命中多少返回多少，未命中行同样推进位置
                batch, next_pos, stopped, _total = storage.scan_filtered(
                    entry["path"],
                    entry["rows_read"],
                    cur["filter_station"],
                    int(cur["filter_start_us"]),
                    int(cur["filter_end_us"]),
                    remaining,
                )
                rows.extend(batch)
                remaining -= len(batch)
                touched.append((entry["file_id"], next_pos))
                if stopped:
                    # 凑满本页且文件内还有未扫描的行，交给下一页继续
                    truncated = True
                    break
                continue

            unread = entry["row_count"] - entry["rows_read"]
            if unread <= 0:
                continue
            take = min(remaining, unread)
            # 关键：文件读取在 LOCK 内完成，reclaim 不可能同时删除该文件
            batch = storage.read_rows(entry["path"], entry["rows_read"], take)
            rows.extend(batch)
            new_read = entry["rows_read"] + len(batch)
            touched.append((entry["file_id"], new_read))
            remaining -= len(batch)
            if len(batch) < take:
                # 物理文件行数与元数据不符，保护性中断，避免误推进位置
                truncated = True
                break

        for file_id, rows_read in touched:
            conn.execute(
                "UPDATE cursor_positions SET rows_read=? "
                "WHERE cursor_id=? AND file_id=?",
                (rows_read, cursor_id, file_id),
            )

        if filtered:
            matched_before = int(cur["matched_rows"])
            matched_now = matched_before + len(rows)
            conn.execute(
                "UPDATE cursors SET matched_rows=? WHERE id=?",
                (matched_now, cursor_id),
            )
            # 是否耗尽取决于每个文件是否都已扫描到末尾（原始位置），
            # 而不是本页是否凑满命中行
            position_by_file = {
                e["file_id"]: e["rows_read"] for e in snapshot
            }
            position_by_file.update(dict(touched))
            exhausted = all(
                position_by_file[e["file_id"]] >= e["row_count"]
                for e in snapshot
            )
            if exhausted:
                conn.execute(
                    "UPDATE cursors SET exhausted=1 WHERE id=?", (cursor_id,)
                )
            conn.commit()
            return {
                "cursor_id": cursor_id,
                "limit": limit,
                "returned": len(rows),
                "rows": rows,
                "exhausted": bool(exhausted),
                # 筛选游标下 total_rows 为“命中行数”的已知计数；
                # 未耗尽时后续可能还有命中，remaining 为 null
                "total_rows": matched_now if exhausted else None,
                "matched_rows": matched_now,
                "rows_read": sum(position_by_file.values()),
                "remaining": 0 if exhausted else None,
                "filter": {
                    "station_id": cur["filter_station"],
                    "start_us": cur["filter_start_us"],
                    "end_us": cur["filter_end_us"],
                },
            }

        # 是否耗尽取决于读取位置而不是本页是否拿满：
        # 最后一页恰好拿满（remaining=0）也可能已经读到快照末尾
        updated = dict(touched)
        read_total = sum(
            updated.get(e["file_id"], e["rows_read"]) for e in snapshot
        )
        total_rows = sum(e["row_count"] for e in snapshot)
        exhausted = (not truncated) and read_total >= total_rows

        if exhausted:
            conn.execute(
                "UPDATE cursors SET exhausted=1 WHERE id=?", (cursor_id,)
            )
        conn.execute(
            "UPDATE cursors SET matched_rows=matched_rows+? WHERE id=?",
            (len(rows), cursor_id),
        )
        conn.commit()

        total_rows = sum(e["row_count"] for e in snapshot)
        # 精确统计持久化后的已读行数
        pos_rows = conn.execute(
            "SELECT COALESCE(SUM(rows_read),0) AS n FROM cursor_positions "
            "WHERE cursor_id=?",
            (cursor_id,),
        ).fetchone()["n"]
        return {
            "cursor_id": cursor_id,
            "limit": limit,
            "returned": len(rows),
            "rows": rows,
            "exhausted": bool(exhausted),
            "total_rows": total_rows,
            "rows_read": pos_rows,
            "remaining": total_rows - pos_rows,
        }


def close_cursor(cursor_id: str) -> dict:
    conn = get_conn()
    with LOCK:
        cur = _get_cursor(conn, cursor_id)
        already = bool(cur["closed"])
        if not already:
            conn.execute(
                "UPDATE cursors SET closed=1 WHERE id=?", (cursor_id,)
            )
            conn.commit()
        # 关闭后立即尝试回收因此失去最后引用的 obsolete 文件
        from .merger import manager

        rec = manager.reclaim()
    return {
        "cursor_id": cursor_id,
        "closed": True,
        "already_closed": already,
        "reclaimed": rec["reclaimed"],
    }


def list_cursors() -> dict:
    conn = get_conn()
    with LOCK:
        rows = conn.execute(
            """SELECT c.id, c.created_at, c.closed, c.exhausted,
                      c.filter_station, c.filter_start_us, c.filter_end_us,
                      c.candidate_rows, c.matched_rows, c.files_skipped,
                      (SELECT COALESCE(SUM(cf.row_count),0) FROM cursor_files cf
                       WHERE cf.cursor_id=c.id) AS total_rows,
                      (SELECT COALESCE(SUM(cp.rows_read),0)
                       FROM cursor_positions cp WHERE cp.cursor_id=c.id) AS rows_read
               FROM cursors c ORDER BY c.created_at"""
        ).fetchall()
    out = []
    for r in rows:
        filtered = r["filter_station"] is not None
        item = {
            "cursor_id": r["id"],
            "created_at": r["created_at"],
            "closed": bool(r["closed"]),
            "exhausted": bool(r["exhausted"]),
            "rows_read": r["rows_read"],
            "filtered": filtered,
            "matched_rows": r["matched_rows"],
        }
        if filtered:
            item.update(
                {
                    "filter": {
                        "station_id": r["filter_station"],
                        "start": storage.us_to_iso(r["filter_start_us"]),
                        "end": storage.us_to_iso(r["filter_end_us"]),
                        "window": "[start, end)",
                    },
                    "candidate_rows": r["candidate_rows"],
                    "files_skipped": r["files_skipped"],
                    "snapshot_files": conn.execute(
                        "SELECT COUNT(*) AS n FROM cursor_files WHERE cursor_id=?",
                        (r["id"],),
                    ).fetchone()["n"],
                    "total_rows": r["matched_rows"] if r["exhausted"] else None,
                }
            )
        else:
            item["total_rows"] = r["total_rows"]
        out.append(item)
    return {"cursors": out}
