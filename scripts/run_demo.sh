#!/usr/bin/env bash
# 完整演示编排：
#   A. 合成数据 + /docs 可用
#   B. 上传、游标快照/重复行、合并暂停、切换、旧游标读完回收（demo.py demo）
#   C. 切换前退出（合并中 kill -9 -> 重启 -> 任务恢复/失败后原数据完好）
#   D. 切换后退出（制造 obsolete 旧文件与未关闭游标 -> kill -9 ->
#      重启后游标继续读、继续回收、不误删现用文件）
#   E. 故障注入：stream/switch 阶段失败保留原数据
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  COMPOSE="docker-compose"
fi
EXEC="$COMPOSE exec -T lakehouse"
STATE=/data/demo-state.json

wait_health() {
  for _ in $(seq 1 60); do
    if $EXEC python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/health')" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "服务未在预期时间内就绪" >&2
  exit 1
}

section() { printf '\n########## %s ##########\n' "$1"; }

section "A. 构建并启动服务，生成合成 Parquet"
$COMPOSE up -d --build
wait_health
$EXEC python scripts/generate_data.py --out /data/synthetic --files 6 --rows 200
echo "交互式文档: http://localhost:8000/docs （上传用二进制 body，非 multipart）"

section "B. 主流程演示：上传/分页/暂停合并/切换/回收"
$EXEC python scripts/demo.py --state "$STATE" demo /data/synthetic/weather-*.parquet

section "C. 切换前退出：合并进行中 kill -9，重启后原数据完好、任务可继续"
$EXEC python scripts/generate_data.py --out /data/synthetic2 --files 3 --rows 100 >/dev/null
for f in /data/synthetic2/weather-*.parquet; do
  $EXEC python -c "
import urllib.request
urllib.request.urlopen(urllib.request.Request(
  'http://127.0.0.1:8000/uploads',
  data=open('$f','rb').read(), method='POST',
  headers={'Content-Type':'application/vnd.apache.parquet'}))
"
done
$EXEC python -c "
import json,urllib.request,time
req=urllib.request.Request('http://127.0.0.1:8000/merges',method='POST',data=b'{}')
urllib.request.urlopen(req)
for _ in range(100):
  s=json.load(urllib.request.urlopen('http://127.0.0.1:8000/merges/status'))
  m=s['active_merge']
  if m['state'] in ('running','paused','switching'): break
  time.sleep(0.05)
print('kill 前合并状态:', m['state'])
"
$COMPOSE kill -s KILL lakehouse
echo "进程已被 SIGKILL（模拟切换前断电）"
$COMPOSE up -d
wait_health
$EXEC python -c "
import json,urllib.request
h=json.load(urllib.request.urlopen('http://127.0.0.1:8000/health'))
print('重启恢复信息:', json.dumps(h['recovery'], ensure_ascii=False))
s=json.load(urllib.request.urlopen('http://127.0.0.1:8000/merges/status'))
print('最近合并状态:', s['active_merge']['state'])
files=json.load(urllib.request.urlopen('http://127.0.0.1:8000/files'))['files']
assert all(f['status']=='active' for f in files), '崩溃后不应出现 obsolete'
print('全部', len(files), '个文件仍为 active，原数据完好')
"
# 等恢复的合并任务走完（若崩溃点在 switching，则任务标记 failed，手动再合并一次）
$EXEC python -c "
import json,time,urllib.request
for _ in range(120):
  s=json.load(urllib.request.urlopen('http://127.0.0.1:8000/merges/status'))['active_merge']
  if s['state'] in ('completed','failed'): break
  time.sleep(0.25)
print('恢复后合并终态:', s['state'])
if s['state']=='failed':
  urllib.request.urlopen(urllib.request.Request(
    'http://127.0.0.1:8000/merges',method='POST',data=b'{}'))
  for _ in range(120):
    s=json.load(urllib.request.urlopen('http://127.0.0.1:8000/merges/status'))['active_merge']
    if s['state']=='completed': break
    time.sleep(0.25)
assert s['state']=='completed'
print('再次合并完成')
"

section "D. 切换后退出：保留未关闭游标与 obsolete 文件，重启后继续回收"
$EXEC python -c "
import json,urllib.request
cur=json.load(urllib.request.urlopen(urllib.request.Request(
  'http://127.0.0.1:8000/cursors',method='POST',data=b'{}')))
open('/data/hold-cursor.txt','w').write(cur['cursor_id'])
print('创建持锁游标:', cur['cursor_id'], '快照行数:', cur['snapshot_rows'])
page=json.load(urllib.request.urlopen(
  'http://127.0.0.1:8000/cursors/%s/pages?limit=10'%%cur['cursor_id']))
print('先读 10 行，remaining =', page['remaining'])
# 再建一个筛选游标（ST-001，2026-11-13 全天），只读一页后重启
f=json.load(urllib.request.urlopen(urllib.request.Request(
  'http://127.0.0.1:8000/cursors',method='POST',
  data=json.dumps({'station_id':'ST-001',
                   'start':'2026-11-13T00:00:00Z',
                   'end':'2026-11-14T00:00:00Z'}).encode(),
  headers={'Content-Type':'application/json'})))
open('/data/hold-filter-cursor.txt','w').write(f['cursor_id'])
fp=json.load(urllib.request.urlopen(
  'http://127.0.0.1:8000/cursors/%s/pages?limit=3'%%f['cursor_id']))
print('筛选游标:', f['cursor_id'], '候选文件:', f['snapshot_files'],
      '索引跳过:', f['files_skipped'], '重启前已读:', fp['returned'])
"
$COMPOSE kill -s KILL lakehouse
echo "进程已被 SIGKILL（模拟切换/回收中退出）"
$COMPOSE up -d
wait_health
$EXEC python scripts/demo.py --state /data/restart-state.json restart-check
$EXEC python -c "
import json,urllib.request
cid=open('/data/hold-cursor.txt').read().strip()
# kill 前已读过 10 行，重启后从持久化位置续读，总数含那 10 行
rows=[]
total=None
while True:
  p=json.load(urllib.request.urlopen(
    'http://127.0.0.1:8000/cursors/%s/pages?limit=500'%%cid))
  rows += p['rows']
  total=p['total_rows']
  if p['exhausted']: break
# 重启前预读的 10 行 + 重启后续读
assert len(rows)==total-10, (len(rows), total)
print('重启后续读', len(rows), '行，连同重启前 10 行共', total, '行，游标耗尽')
r=json.load(urllib.request.urlopen(urllib.request.Request(
  'http://127.0.0.1:8000/cursors/%s/close'%%cid,method='POST',data=b'{}')))
print('关闭后回收文件:', r['reclaimed'])
# 筛选游标：重启后按原始行位置续读，仍然只返回 ST-001 且行序连续
fcid=open('/data/hold-filter-cursor.txt').read().strip()
frows=[]
ftotal=None
while True:
  p=json.load(urllib.request.urlopen(
    'http://127.0.0.1:8000/cursors/%s/pages?limit=4'%%fcid))
  frows += p['rows']
  ftotal=p['total_rows']
  if p['exhausted']: break
assert all(x['station_id']=='ST-001' for x in frows)
assert all('2026-11-13T00:00:00+00:00' <= x['observed_at'] < '2026-11-14T00:00:00+00:00'
           for x in frows)
print('筛选游标重启后续读', len(frows), '行命中行，连同重启前 3 行共', ftotal, '行')
json.load(urllib.request.urlopen(urllib.request.Request(
  'http://127.0.0.1:8000/cursors/%s/close'%%fcid,method='POST',data=b'{}')))
files=json.load(urllib.request.urlopen('http://127.0.0.1:8000/files'))['files']
assert all(f['status']=='active' for f in files)
print('现用文件均未被误删')
"

section "E. 故障注入演示（stream / switch 失败保留原数据）"
$EXEC python scripts/demo.py fail-demo

section "全部演示完成 ✔  Swagger 文档: http://localhost:8000/docs"
