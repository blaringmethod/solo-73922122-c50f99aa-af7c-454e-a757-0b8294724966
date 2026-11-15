# 气象站小文件入湖与后台合并服务

单进程湖仓维护 API（FastAPI + PyArrow + SQLite），把气象站持续入湖的零散
Parquet 小文件合并为单个 Parquet，降低下游分析的文件打开开销。

支持**按站点 + UTC 左闭右开时段的筛选分页**：分析员排查单站短时异常时，
直接拿到该站该时段的原始读数，不必逐页遍历全库。

## 核心契约与保证

| 需求 | 实现方式 |
| --- | --- |
| 追加上传，每份 ≤ 8MiB，固定三列 | `POST /uploads` 按字节流式接收，超限立即 413；校验并规范化为 `station_id(string)` / `observed_at(timestamp[us, UTC])` / `temperature(float64)`，缺列、多列、空表、非 Parquet 一律 422 |
| 按上传顺序 + 文件内行序分页，保留重复行 | 全局 `read_order` 决定文件次序，游标在文件内按偏移顺序读，全程不去重 |
| 游标固定创建时已提交的行 | 创建游标时把当时所有 `active` 文件快照写入 `cursor_files`，之后新上传/合并产物都不会进入旧游标 |
| **按站点 + 时段筛选，只返回命中行** | `POST /cursors` 传 `station_id`/`start`/`end`（ISO8601，必须带时区）创建筛选游标；行级匹配 `station_id == ? AND start <= observed_at < end`，重复行照常保留 |
| **跳过大片不命中的文件** | 每个文件在 `file_station_ranges` 里登记按站点划分的观测时间范围（微秒 UTC）；创建游标时用 `max_observed_us >= start AND min_observed_us < end` 排除确定不命中的文件，`files_skipped` 给出数量 |
| **旧的无索引文件仍须扫描** | `files.indexed=0` 的文件不参与跳过，按整文件候选保守扫描；因此老库升级后结果依然正确 |
| **进度按原始行位置保存** | 筛选游标内部 `cursor_positions.rows_read` 始终是文件内偏移（跳过的未命中行同样推进），每页提交，重启后精确续读；`rows_read` 在响应中即扫描的原始行数 |
| **索引与文件登记同一事务** | 上传与合并发布都调用 `db.register_file()`，文件行 + 站点范围索引在同一 SQLite 事务提交；任一步失败整体回滚且删除落位文件，绝不会留下“有文件无索引”或“有索引无文件” |
| **合并产物索引随切换发布** | 合并写入时顺带按站点累计范围（不依赖输入是否已建索引），切换事务里随产物一起登记；switch 失败则产物与索引都不存在，原文件索引不变 |
| 后台把“开始时”的文件合成一个 Parquet，同期只合并一次 | 合并启动瞬间捕获输入清单（`merge_files`），进行中的合并不吞新文件；存在活动任务时再次启动返回 409 |
| 合并期间仍可上传、翻页 | 合并在独立后台线程流式 `ParquetWriter` 写入 tmp，API 与分页不阻塞；筛选游标引用旧文件期间旧文件不回收，可持续翻页 |
| 完整结果才可切换，失败保留原数据 | 先做行数 + 完整重读 + 列结构校验，通过后在单个 SQLite 事务里原子切换；任何失败输入文件始终 `active`，tmp 自动清理 |
| 旧文件等游标读完或关闭后才回收 | 旧文件转 `obsolete`；回收 SQL 用 `NOT EXISTS` 校验没有任何未关闭游标引用，读取在同一把进程锁内完成，不会与删除竞争 |
| 切换/回收中退出，重启后可恢复 | 启动恢复：清理 tmp 半成品；`switching` 任务（事务未提交）标记 failed 且原数据完好；`running/paused` 任务重起后台线程续跑；继续回收无引用旧文件；active 文件永不删除 |

数据与元数据全部落在挂载卷 `/data`：

```
/data
├── files/      # 正式 Parquet（上传文件与合并产物）
├── tmp/        # 上传/合并的临时文件（*.tmp，崩溃后启动清理）
└── meta/lakehouse.db   # SQLite（WAL 模式）：文件清单、站点时间范围索引、
                        # 合并任务、游标快照、原始行位置进度
```

## 快速开始（Docker Compose）

```bash
docker compose up -d --build
# 交互式文档（可直接在页面上演示全部接口）
open http://localhost:8000/docs
```

健康检查：`GET /health` 返回启动恢复结果。

### 在 /docs 演示筛选分页

1. `POST /uploads`：点 **Try it out**，请求体选文件，上传一份 Parquet
   （`scripts/generate_data.py` 可生成样例）。
2. `POST /cursors`：请求体填

   ```json
   {"station_id": "ST-001",
    "start": "2026-11-13T00:00:00Z",
    "end": "2026-11-13T02:00:00Z"}
   ```

   响应里的 `files_skipped` 是索引跳过的不命中文件数，`candidate_rows`
   是候选行上界；不传 body 即为原来的无筛选游标。
3. `GET /cursors/{cursor_id}/pages?limit=10`：反复调用即可翻页，`rows`
   只含 `ST-001` 且落在 `[start, end)` 内的原始读数（重复行照常返回）；
   每页都会推进并持久化原始行位置，`rows_read` 为已扫描的原始行数。
   `exhausted=true` 时 `total_rows` 为命中总数。
4. `GET /files/{file_id}/ranges`：查看该文件按站点划分的时间范围索引。

## API 一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /uploads` | 原始 Parquet 字节流上传（**二进制 body，不是 multipart**），≤8MiB；同一事务登记文件与站点时间范围索引 |
| `GET  /files` | 文件清单（active/obsolete、行数、读取次序、是否已建索引、时间范围） |
| `GET  /files/{file_id}/ranges` | 某文件按站点划分的时间范围索引 |
| `POST /cursors` | 创建游标固定当前已提交行；body 可选 `{station_id, start, end}` 启用筛选 |
| `GET  /cursors` | 全部游标及读取进度（筛选游标额外给出 filter/matched_rows/files_skipped） |
| `GET  /cursors/{id}/pages?limit=N` | 顺序分页；筛选游标只返回命中行，并在响应中带 `filter` |
| `POST /cursors/{id}/close` | 显式关闭，可能立即触发旧文件回收 |
| `POST /merges` | 启动后台合并（202；已有活动任务时 409） |
| `GET  /merges/status` | 最近合并任务状态与每个输入文件的写入进度 |
| `POST /merges/pause` / `/resume` | 暂停/恢复合并（演示用） |
| `POST /merges/reclaim` | 立即回收无游标引用的 obsolete 文件 |
| `POST /merges/fail-inject/{stream|switch}` | 演示用故障注入，验证失败保留原数据（含索引） |

## 一键端到端演示

```bash
./scripts/run_demo.sh
```

脚本依次演示：

1. 构建启动服务，生成 6 份合成 Parquet（含故意构造的跨文件重复行）；
2. 上传 → 建游标 → 校验快照行数、重复行保留、跨文件边界分页；
3. 启动合并并**暂停**，暂停期间继续上传和翻页，再恢复完成切换；
4. 校验旧游标的行序/内容不变、新游标看到合并产物 + 新文件；
5. **筛选游标**：按 `ST-001` + 2 小时时段翻页，校验只返回命中行、
   结果与手工过滤逐行一致、数据外时段被索引全部跳过；
6. 关闭游标触发回收，确认 active 文件不被误删；
7. **切换前退出**：合并进行中 `kill -9` 容器，重启后原数据完好、任务续跑；
8. **切换后退出**：保留未关闭游标和 obsolete 文件后 `kill -9`，重启后游标继续读、
   继续回收、现用文件不误删；
9. 故障注入：stream / switch 两阶段失败均保留原数据，清除后合并成功。

### 不用 Docker 时本地运行

```bash
pip install -r requirements.txt
export DATA_DIR=/tmp/lakehouse
python scripts/generate_data.py --out "$DATA_DIR/synthetic" --files 6 --rows 200
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
# 另开终端
DATA_DIR=$DATA_DIR python scripts/demo.py demo "$DATA_DIR"/synthetic/weather-*.parquet
DATA_DIR=$DATA_DIR python scripts/demo.py fail-demo
```

## 测试

```bash
pip install -r requirements.txt pytest
pytest            # 18 个用例：校验/分页边界/快照/合并/回收/故障/重启恢复
                  #             + 筛选命中与重复行/索引跳过/无索引旧文件/
                  #               原始行位置续读/跨 row group/切换与上传事务原子性/
                  #               切换崩溃孤儿产物清理/参数校验
```

## 关键设计说明

- **单进程 + 一把可重入锁**：所有元数据写事务、Parquet 读取与旧文件删除都在
  `app.db.LOCK` 保护下，配合 SQLite WAL，杜绝“游标正在读的文件被回收”。
- **索引与数据同源**：站点范围直接由即将落位的数据计算（上传时由规范化表，
  合并时由写入流的累加器），并在登记文件的同一事务提交；不受半途失败影响，
  也不会因输入是旧的无索引文件而缺失。
- **索引只做“保守排除”**：范围是 `[min, max]` 的闭区间上界，只用于证明某文件
  对某站点+时段**一定没有命中**；任何可能命中的文件（含无索引旧文件）都整文件
  扫描，正确性不依赖索引精度。
- **筛选游标与普通游标同构**：都固定创建时的文件清单，都在 `cursor_positions`
  里按文件记录进度；区别只是筛选游标按原始行位置扫描并只返回命中行，因此合并
  切换、旧件回收、重启续读的行为与原来完全一致。
- **原子切换**：合并产物先写 `tmp/` 并通过完整性校验，再 `os.replace` 落位，
  最后在一个 SQLite 事务里插入新产物 + 其站点索引 + 旧文件置 obsolete；
  事务失败自动回滚并退回产物，原索引不被触碰。
- **崩溃安全**：所有进度（合并输入、游标原始行位置、已返回命中数、文件状态）
  在每页/每输入文件处理后立即提交；重启时按状态机恢复，不依赖进程内存。
- **合并产物继承最早输入的读取位置**，新上传文件 `read_order` 恒在其后，
  保证切换前后全局行序对任何游标都稳定。
