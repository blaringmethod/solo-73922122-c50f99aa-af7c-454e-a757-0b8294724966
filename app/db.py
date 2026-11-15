"""SQLite 元数据库：文件清单、合并任务、游标快照。

所有写操作都在进程内的一把可重入锁保护下进行（单进程模型），
配合 WAL 模式保证后台合并线程与 API 线程之间的一致性。
"""
from __future__ import annotations

import os
import sqlite3
import threading

from . import config

# 进程级锁：保护所有元数据变更以及“读 parquet + 删除文件”的临界区
LOCK = threading.RLock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    os.makedirs(config.FILES_DIR, exist_ok=True)
    os.makedirs(config.TMP_DIR, exist_ok=True)
    conn = _connect()
    with LOCK:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );

            -- Parquet 文件清单（上传文件与合并产物共用一张表）
            CREATE TABLE IF NOT EXISTS files (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id      TEXT NOT NULL UNIQUE,
                kind         TEXT NOT NULL CHECK (kind IN ('upload', 'merged')),
                path         TEXT NOT NULL,
                size_bytes   INTEGER NOT NULL,
                row_count    INTEGER NOT NULL,
                seq_order    INTEGER NOT NULL UNIQUE,
                -- 全局读取顺序：合并产物继承其输入的最早位置，
                -- 切换后新上传的文件排在其后；seq_order 仅用于同位置内的次序
                read_order   INTEGER NOT NULL,
                status       TEXT NOT NULL CHECK (status IN ('active', 'obsolete')),
                -- 站点时间范围索引的登记状态：1=已建索引，0=旧文件/未建索引，
                -- 筛选查询对 0 的文件必须保守扫描，不能凭“没有索引行”跳过
                indexed      INTEGER NOT NULL DEFAULT 0,
                min_observed_us INTEGER,
                max_observed_us INTEGER,
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                obsolete_at  TEXT
            );

            -- 每个文件按站点划分的观测时间范围（微秒 UTC，半开区间语义由查询方处理）
            -- 与 files 的登记在同一个事务里写入；旧文件没有行且 indexed=0。
            CREATE TABLE IF NOT EXISTS file_station_ranges (
                file_id          INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                station_id       TEXT NOT NULL,
                row_count        INTEGER NOT NULL,
                -- 该站该文件内所有非空 observed_at 的最小/最大值；全为空时 NULL
                min_observed_us  INTEGER,
                max_observed_us  INTEGER,
                PRIMARY KEY (file_id, station_id)
            );

            -- 后台合并任务
            CREATE TABLE IF NOT EXISTS merges (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                output_file_id TEXT,
                state         TEXT NOT NULL CHECK (state IN (
                                  'running', 'paused', 'switching',
                                  'completed', 'failed', 'cancelled')),
                fail_at_step  TEXT,
                error         TEXT,
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            -- 某次合并在“开始时”捕获的输入文件
            CREATE TABLE IF NOT EXISTS merge_files (
                merge_id        INTEGER NOT NULL REFERENCES merges(id) ON DELETE CASCADE,
                file_id         INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                position        INTEGER NOT NULL,
                rows_written    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (merge_id, file_id)
            );

            -- 游标（创建时固定已提交行的快照）
            -- filter_* 三个字段同时非空表示“按站点 + UTC 左闭右开时段筛选”的游标；
            -- 筛选游标的 cursor_positions.rows_read 始终是“原始行位置”（文件内偏移），
            -- 而不是命中行数，因此跳过未命中行后仍能准确续读。
            CREATE TABLE IF NOT EXISTS cursors (
                id          TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                closed      INTEGER NOT NULL DEFAULT 0,
                exhausted   INTEGER NOT NULL DEFAULT 0,
                filter_station  TEXT,
                filter_start_us INTEGER,
                filter_end_us   INTEGER,
                -- 快照候选行的保守上界（未索引文件按整文件行数计）
                candidate_rows  INTEGER NOT NULL DEFAULT 0,
                -- 已返回的命中行数（筛选游标；无筛选游标也同步维护）
                matched_rows    INTEGER NOT NULL DEFAULT 0,
                -- 创建时因索引确定不命中而跳过的文件数
                files_skipped   INTEGER NOT NULL DEFAULT 0
            );

            -- 游标快照：游标能看到哪些文件、各自的行数
            CREATE TABLE IF NOT EXISTS cursor_files (
                cursor_id   TEXT NOT NULL REFERENCES cursors(id) ON DELETE CASCADE,
                file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                position    INTEGER NOT NULL,
                row_count   INTEGER NOT NULL,
                PRIMARY KEY (cursor_id, file_id)
            );

            -- 游标读取位置：在每个快照文件里已经读过多少行。
            -- 无筛选游标即已返回行数；筛选游标是“原始行位置”（文件内偏移），
            -- 跳过的未命中行同样计入，保证重启后精确续读。
            CREATE TABLE IF NOT EXISTS cursor_positions (
                cursor_id   TEXT NOT NULL REFERENCES cursors(id) ON DELETE CASCADE,
                file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                rows_read   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (cursor_id, file_id)
            );

            CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
            CREATE INDEX IF NOT EXISTS idx_files_read_order
                ON files(read_order, seq_order);
            CREATE INDEX IF NOT EXISTS idx_cursors_closed ON cursors(closed);
            CREATE INDEX IF NOT EXISTS idx_range_station_time
                ON file_station_ranges(station_id, min_observed_us, max_observed_us);
            """
        )
        _migrate(conn)
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES ('file_seq', 0)"
        )
        conn.commit()
    return conn


_conn: sqlite3.Connection | None = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = init()
    return _conn


def next_file_seq(conn: sqlite3.Connection) -> int:
    """自增并返回下一个文件序号（调用方需持有 LOCK 与事务）。"""
    cur = conn.execute(
        "UPDATE settings SET value = value + 1 WHERE key = 'file_seq'"
    )
    assert cur.rowcount == 1
    row = conn.execute(
        "SELECT value FROM settings WHERE key = 'file_seq'"
    ).fetchone()
    return int(row["value"])


def _migrate(conn: sqlite3.Connection) -> None:
    """为既有数据库补列（幂等）。

    老库里的 files 行没有站点时间范围索引，保持 indexed=0，筛选查询会
    保守地整文件扫描，不会因为“索引表里没有行”而误跳过。
    """
    file_cols = {r["name"] for r in conn.execute("PRAGMA table_info(files)")}
    for name, ddl in (
        ("indexed", "INTEGER NOT NULL DEFAULT 0"),
        ("min_observed_us", "INTEGER"),
        ("max_observed_us", "INTEGER"),
    ):
        if name not in file_cols:
            conn.execute(f"ALTER TABLE files ADD COLUMN {name} {ddl}")

    cur_cols = {r["name"] for r in conn.execute("PRAGMA table_info(cursors)")}
    for name, ddl in (
        ("filter_station", "TEXT"),
        ("filter_start_us", "INTEGER"),
        ("filter_end_us", "INTEGER"),
        ("candidate_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("matched_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("files_skipped", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in cur_cols:
            conn.execute(f"ALTER TABLE cursors ADD COLUMN {name} {ddl}")


def register_file(
    conn: sqlite3.Connection,
    *,
    file_id: str,
    kind: str,
    path: str,
    size_bytes: int,
    row_count: int,
    seq_order: int,
    read_order: int,
    ranges: list[tuple[str, int, int | None, int | None]],
    min_observed_us: int | None,
    max_observed_us: int | None,
) -> int:
    """登记文件并写入按站点的时间范围索引（调用方持有 LOCK 与事务）。

    文件行与索引行必须在同一事务提交：任何一步失败都回滚，绝不会留下
    “已登记但没有索引”或“有索引却未登记”的中间状态。
    """
    cur = conn.execute(
        """INSERT INTO files
           (file_id, kind, path, size_bytes, row_count, seq_order,
            read_order, status, indexed, min_observed_us, max_observed_us)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?)""",
        (file_id, kind, path, size_bytes, row_count, seq_order,
         read_order, min_observed_us, max_observed_us),
    )
    file_pk = int(cur.lastrowid)
    conn.executemany(
        """INSERT INTO file_station_ranges
           (file_id, station_id, row_count, min_observed_us, max_observed_us)
           VALUES (?, ?, ?, ?, ?)""",
        [(file_pk, sid, cnt, lo, hi) for sid, cnt, lo, hi in ranges],
    )
    return file_pk
