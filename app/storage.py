"""Parquet 读写：上传校验/规范化、索引聚合、分页读取、流式合并。"""
from __future__ import annotations

import os
import uuid
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import config

REQUIRED_COLUMNS = ("station_id", "observed_at", "temperature")


class ParquetValidationError(ValueError):
    """上传文件不满足契约时抛出，API 层映射为 422。"""


def _canonical_table(table: pa.Table) -> pa.Table:
    """把输入表强制规范为固定三列：station_id(str)/observed_at(UTC ts[us])/temperature(float64)。

    - 缺失列、出现多余列 => 422
    - observed_at 只接受时间戳类型，统一转 UTC；naive 时间戳按 UTC 处理
    - station_id 统一转 UTF-8 字符串（数字站点号也接受）
    - temperature 转 float64（布尔值不接受，避免 True/False 被当成 1/0）
    """
    names = table.column_names
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise ParquetValidationError(f"缺少必需列: {', '.join(missing)}")
    extra = [c for c in names if c not in REQUIRED_COLUMNS]
    if extra:
        raise ParquetValidationError(f"存在未声明的额外列: {', '.join(extra)}")
    if table.num_rows == 0:
        raise ParquetValidationError("文件不包含任何数据行")

    # station_id
    sid = table.column("station_id")
    if pa.types.is_string(sid.type) or pa.types.is_large_string(sid.type):
        sid = sid.cast(pa.string())
    elif pa.types.is_integer(sid.type):
        sid = sid.cast(pa.string())
    else:
        raise ParquetValidationError(
            f"station_id 列类型 {sid.type} 不受支持，需要 string 或整数"
        )

    # observed_at
    ts = table.column("observed_at")
    if not pa.types.is_timestamp(ts.type):
        raise ParquetValidationError(
            f"observed_at 列类型 {ts.type} 不受支持，需要 timestamp"
        )
    if ts.type.tz is None:
        ts = ts.cast(pa.timestamp("us", tz="UTC"))
    else:
        ts = ts.cast(pa.timestamp("us", tz="UTC"))

    # temperature
    temp = table.column("temperature")
    if pa.types.is_boolean(temp.type):
        raise ParquetValidationError("temperature 列不能是布尔类型")
    if not (pa.types.is_floating(temp.type) or pa.types.is_integer(temp.type)):
        raise ParquetValidationError(
            f"temperature 列类型 {temp.type} 不受支持，需要数值类型"
        )
    temp = temp.cast(pa.float64())

    return pa.table(
        {
            "station_id": sid,
            "observed_at": ts,
            "temperature": temp,
        }
    )


def validate_and_stage(data: bytes) -> tuple[pa.Table, int, str]:
    """校验上传字节流并规范化后落到 tmp，返回(规范化表, 行数, tmp路径)。"""
    try:
        table = pq.read_table(pa.BufferReader(data))
    except Exception as exc:  # noqa: BLE001 - pyarrow 的异常类型很多
        raise ParquetValidationError(f"无法解析为 Parquet: {exc}") from exc
    table = _canonical_table(table)

    tmp_name = f"upload-{uuid.uuid4().hex}.parquet.tmp"
    os.makedirs(config.TMP_DIR, exist_ok=True)
    tmp_path = os.path.join(config.TMP_DIR, tmp_name)
    pq.write_table(table, tmp_path, compression="snappy")
    return table, table.num_rows, tmp_path


def read_rows(path: str, offset: int, limit: int) -> list[dict]:
    """读取单个 parquet 文件 [offset, offset+limit) 的行，转成 JSON 友好字典。"""
    table = pq.read_table(path)
    if offset:
        table = table.slice(offset, limit)
    else:
        table = table.slice(0, limit)
    return table_to_rows(table)


# ------------------------- 按站点的时间范围索引 -------------------------

StationRange = tuple[str, int, int | None, int | None]


def us_to_iso(us: int | None) -> str | None:
    """微秒 UTC 时间戳转 ISO8601（用于 API 展示）。"""
    if us is None:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc).isoformat()


def station_ranges_from_table(
    table: pa.Table,
) -> tuple[list[StationRange], int | None, int | None]:
    """从一个规范化表计算 (station_id, 行数, 最小us, 最大us) 列表与文件级范围。

    - observed_at 为 NULL 的行计入该站行数，但不参与 min/max
    - station_id 为 NULL 的行不可能命中任何站点筛选，直接排除出索引
    """
    n = table.num_rows
    sids = table.column("station_id")
    ts_us = table.column("observed_at").cast(pa.int64())
    work = pa.table(
        {
            "station_id": sids,
            "ts": ts_us,
            "one": pa.array([1] * n, pa.int64()),
        }
    )
    work = work.filter(pc.is_valid(work.column("station_id")))
    if work.num_rows:
        grouped = work.group_by("station_id").aggregate(
            [("one", "sum"), ("ts", "min"), ("ts", "max")]
        )
        g = grouped.to_pydict()
        ranges: list[StationRange] = [
            (
                sid,
                int(cnt),
                None if lo is None else int(lo),
                None if hi is None else int(hi),
            )
            for sid, cnt, lo, hi in zip(
                g["station_id"], g["one_sum"], g["ts_min"], g["ts_max"]
            )
        ]
    else:
        ranges = []

    valid_ts = pc.drop_null(ts_us)
    file_min = int(pc.min(valid_ts).as_py()) if len(valid_ts) else None
    file_max = int(pc.max(valid_ts).as_py()) if len(valid_ts) else None
    return ranges, file_min, file_max


def merge_station_ranges(
    acc: dict[str, list[int | None]],
    ranges: list[StationRange],
) -> None:
    """把一份文件的站点范围并入累加器（合并产物发布时用）。"""
    for sid, cnt, lo, hi in ranges:
        slot = acc.get(sid)
        if slot is None:
            acc[sid] = [cnt, lo, hi]
            continue
        slot[0] = int(slot[0]) + cnt
        if lo is not None:
            slot[1] = lo if slot[1] is None else min(int(slot[1]), lo)
        if hi is not None:
            slot[2] = hi if slot[2] is None else max(int(slot[2]), hi)


def accumulated_ranges(
    acc: dict[str, list[int | None]],
) -> tuple[list[StationRange], int | None, int | None]:
    ranges: list[StationRange] = [
        (sid, int(v[0]), v[1], v[2]) for sid, v in sorted(acc.items())
    ]
    mins = [v[1] for v in acc.values() if v[1] is not None]
    maxs = [v[2] for v in acc.values() if v[2] is not None]
    return ranges, (min(mins) if mins else None), (max(maxs) if maxs else None)


def scan_filtered(
    path: str,
    start_offset: int,
    station_id: str,
    start_us: int,
    end_us: int,
    limit: int,
) -> tuple[list[dict], int, bool, int]:
    """从原始行位置 start_offset 起扫描，返回命中 [start_us, end_us) 的行。

    返回 (rows, 下一个原始行位置, 是否因凑满 limit 提前停止, 文件总行数)。
    分块读取控制内存；已读完的 row group 直接按元数据跳过，不重复解压。
    "原始行位置" 即文件内偏移，跳过的未命中行同样推进位置，保证续读准确。
    """
    pf = pq.ParquetFile(path)
    total = pf.metadata.num_rows
    if start_offset >= total:
        return [], total, False, total

    # 定位 start_offset 所在的 row group，之前的组整组跳过
    group_starts: list[int] = []
    offset = 0
    for i in range(pf.metadata.num_row_groups):
        group_starts.append(offset)
        offset += pf.metadata.row_group(i).num_rows
    first_group = 0
    for i, begin in enumerate(group_starts):
        n_rows = pf.metadata.row_group(i).num_rows
        if begin + n_rows > start_offset:
            first_group = i
            break
    base = group_starts[first_group] if group_starts else 0
    local_start = max(0, start_offset - base)

    rows: list[dict] = []
    remaining = limit
    target = pa.scalar(station_id, pa.string())
    batch_size = config.FILTER_BATCH_ROWS
    skip = local_start  # 首个参与批内需要跳过的行数

    for batch in pf.iter_batches(
        batch_size=batch_size,
        row_groups=list(range(first_group, pf.metadata.num_row_groups)),
        columns=list(REQUIRED_COLUMNS),
    ):
        n = batch.num_rows
        if skip >= n:
            # 整个批次都在 start_offset 之前
            skip -= n
            base += n
            continue
        tbl = pa.Table.from_batches([batch]).slice(skip)
        batch_base = base + skip  # 本批切片首行在文件中的绝对位置
        skip = 0
        ts_us = tbl.column("observed_at").cast(pa.int64())
        mask = pc.and_(
            pc.equal(tbl.column("station_id"), target),
            pc.and_(
                pc.greater_equal(ts_us, start_us),
                pc.less(ts_us, end_us),
            ),
        )
        mask = pc.fill_null(mask, False)
        hits = pc.indices_nonzero(mask).to_pylist()
        if hits:
            take = hits[:remaining]
            picked = tbl.take(pa.array(take, pa.int64()))
            rows.extend(table_to_rows(picked))
            remaining -= len(take)
            if remaining <= 0:
                return rows, batch_base + take[-1] + 1, True, total
        base += n

    return rows, total, False, total


def table_to_rows(table: pa.Table) -> list[dict]:
    sids = table.column("station_id").to_pylist()
    # UTC 时间戳转成带偏移的 ISO8601 字符串
    ts = table.column("observed_at").to_pylist()
    temps = table.column("temperature").to_pylist()
    rows: list[dict] = []
    for sid, t, temp in zip(sids, ts, temps):
        if t is not None and t.tzinfo is not None:
            observed = t.isoformat()
        elif t is not None:
            observed = t.isoformat() + "+00:00"
        else:
            observed = None
        rows.append(
            {
                "station_id": sid,
                "observed_at": observed,
                "temperature": temp,
            }
        )
    return rows


class MergeStreamWriter:
    """把多个 parquet 按顺序流式写入一个输出文件。

    使用 ParquetWriter 逐文件 append，内存占用不随输入总大小增长。
    写入的同时按站点累计时间范围，产物的索引因此不依赖输入是否已建索引
    （旧的无索引文件合并后产物依然有正确索引）。
    """

    def __init__(self, output_path: str):
        self.output_path = output_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self._writer: pq.ParquetWriter | None = None
        self.rows_written = 0
        self.station_acc: dict[str, list[int | None]] = {}

    def append_file(self, path: str) -> int:
        table = pq.read_table(path)
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.output_path, table.schema, compression="snappy"
            )
        self._writer.write_table(table)
        ranges, _lo, _hi = station_ranges_from_table(table)
        merge_station_ranges(self.station_acc, ranges)
        self.rows_written += table.num_rows
        return table.num_rows

    def accumulated(self) -> tuple[list[StationRange], int | None, int | None]:
        return accumulated_ranges(self.station_acc)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> "MergeStreamWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def verify_output(path: str, expected_rows: int) -> int:
    """合并产物落地后的完整性校验，返回实际行数；不匹配则抛错。"""
    if not os.path.exists(path):
        raise RuntimeError("合并产物不存在")
    pf = pq.ParquetFile(path)
    actual = pf.metadata.num_rows
    if actual != expected_rows:
        raise RuntimeError(
            f"合并产物行数校验失败: 期望 {expected_rows}, 实际 {actual}"
        )
    # 再完整读一遍，确保文件尾部/footer 没有损坏
    table = pq.read_table(path)
    if table.num_rows != expected_rows:
        raise RuntimeError("合并产物重新读取行数不一致")
    if table.column_names != list(REQUIRED_COLUMNS):
        raise RuntimeError("合并产物列结构不符合契约")
    return actual


def iter_merge_inputs(paths: list[str]) -> Iterator[tuple[str, pa.Table]]:
    for p in paths:
        yield p, pq.read_table(p)
