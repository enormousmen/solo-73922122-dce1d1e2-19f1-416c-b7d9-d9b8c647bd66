# TCP 故障代理 / 采样包上传卡顿复现平台

设备接入测试场景：复现 32 MiB 采样包上传卡顿，并判断超时发生在**上传阶段**还是
**回执阶段**。本项目用 **FastAPI + asyncio 单进程**实现一个 TCP 故障代理，固定转发到
本地接收端，可按连接、按方向在累计转发 N 字节后**暂停**，并支持查询、恢复、强断。

## 架构

```
 sender (FastAPI)  ──TCP:9000──▶  proxy (asyncio 转发 + FastAPI 控制面:8080)  ──▶  receiver (asyncio:9100)
   /docs 发起上传                    /docs 预设暂停/查询/恢复/强断                    收 EOF 才出回执(字节数+SHA-256)
   32MiB 后 write_eof                每方向独立 pump 任务                            回执 JSON 后反向半关闭
   读回执直到 EOF                    回执查询 HTTP:9101
```

三个组件各一个容器（`docker compose up`），也可在本机直接用脚本起三个进程。

| 组件 | 端口 | 说明 |
| --- | --- | --- |
| receiver | TCP 9100 / HTTP 9101 | 收满到 EOF 才计算并返回 `{bytes, sha256, ...}` 回执；HTTP 查最近回执 |
| proxy | TCP 9000 / HTTP 8080 | 故障转发入口；8080 为控制 API 与 `/docs` |
| sender | HTTP 8081 | `/docs` 发起上传、查看 run 与回执 |

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 控制文档
open http://127.0.0.1:8080/docs    # 故障代理：预设/连接/恢复/强断
open http://127.0.0.1:8081/docs    # 发送器：发起上传、查看回执
# receiver 回执
curl http://127.0.0.1:9101/receipts
```

无 Docker 时（本机已有 Python 3.11）：

```bash
scripts/run-local.sh        # 建 venv、装依赖、起三个进程（日志在 /tmp/fp-logs）
scripts/stop-local.sh       # 停止
python -I tests/smoke_test.py   # 端到端冒烟（37 项检查）
```

## 复现卡顿并定位超时阶段（标准操作流）

1. 在代理 `/docs` 调 `PUT /preset`，设 `c2r_pause_after=1048576`（上传 1 MiB 后暂停）。
2. 在发送器 `/docs` 调 `POST /send`（默认 32 MiB），拿到 `run_id`。
3. `GET /connections` 看到连接 `c2r` 方向 `paused=true`、`forwarded_bytes=1048576`、
   `fin` 未送达；发送器 run 停在 `connected`（drain 被 TCP 背压阻塞）——**说明超时在上传阶段**。
4. `POST /connections/{id}/resume`（`direction=c2r`），积压与 FIN 继续，接收端出回执，
   run 变 `done`，`hash_match=true`。
5. 要定位**回执阶段**超时：`PUT /preset` 设 `r2c_pause_after=16`，此时上传正常完成
   （`c2r.half_closed_delivered=true`），run 停在 `waiting-receipt`，恢复 `r2c` 后才收到回执。
6. `POST /connections/{id}/force-close` 可随时强断，立即打断暂停与阻塞写。

## 控制 API（proxy:8080）

| 方法/路径 | 作用 |
| --- | --- |
| `GET /health` | 健康状态、转发目标、每方向用户态缓冲上限（256 KiB） |
| `GET /preset` / `PUT /preset` | 查看/预设**新连接**每方向暂停阈值（`null` 取消） |
| `GET /connections` | 全部连接（活动在前、已关闭在后），含每方向转发字节、积压、暂停/FIN 状态 |
| `GET /connections/{id}` | 单连接详情 |
| `POST /connections/{id}/pause` | 手动暂停某方向（阈值之外的测试手段） |
| `POST /connections/{id}/resume` | 恢复指定方向；不传 direction 则恢复全部暂停方向 |
| `POST /connections/{id}/force-close` | 强断：abort 两端、取消 pump，任务退出且两端关闭后才返回 |

连接状态字段（每方向）：`forwarded_bytes`（已转发）、`pending_bytes`（暂停时暂扣的积压）、
`paused`、`fin_pending`、`half_closed_delivered`、`eof_seen`、`peer_closed` 等。

## 关键正确性设计与保证

- **恰好停在 N 字节（跨读取块）**：每次 `read(64KiB)` 后按阈值切分，`head` 转发、
  `tail` 作为该方向唯一的用户态积压（`pending`）；暂停期间不再读 socket。
- **积压与 FIN 不越过暂停**：暂停时不做任何读和 FIN 投递；读到 EOF 也只标记
  `eof_seen/fin_pending`，等恢复后先把积压发完再 `write_eof()`。
- **恢复后不丢、不重、不乱序**：恢复后先 flush `pending` 再继续读，字节顺序唯一；
  测试以双端 SHA-256 与字节数比对验证。
- **背压、无整包缓存、不落盘**：暂停即停读 → 内核接收缓冲填满 → TCP 流控反压到发送端，
  发送端阻塞在 `drain()`；每方向用户态数据 = StreamReader 缓冲（上限 256 KiB）+
  至多一个切分尾块，全程不写磁盘。
- **FIN 半关闭语义**：FIN 等本方向积压清空后才向对端半关闭；反向 pump 是独立任务，
  回执方向不受影响，可一直读到 EOF。
- **强断语义**：`transport.abort()` 让阻塞的 `drain()`/`read()` 立即抛错，两个 pump
  被取消并退出，supervisor 关闭两端 socket、连接从活动列表移除后 API 才报完成；
  其他连接与控制 API 不受影响（冒烟测试覆盖）。

## 测试

`tests/smoke_test.py`（纯标准库）覆盖：基线哈希一致、自动暂停精确停 N、积压/FIN 暂扣、
奇数 N 跨块、手动暂停/恢复、回执方向暂停、强断与隔离性、背压下用户态积压 ≤ 256 KiB、
32 MiB 级载荷恢复后哈希一致。可用环境变量 `PROXY_API/SENDER_API/RECEIVER_API/PROXY_TCP`
指向 Compose 暴露的端口直接对容器跑。

```bash
python -I tests/smoke_test.py
# 37/37 checks passed
```

## 实测结果（本环境，Python 3.11.2 / loopback）

环境无 Docker 守护进程，故用等价的三进程本地部署验证（Compose 文件已就绪，逻辑与
容器部署一致，仅主机名改为 127.0.0.1）。连续多轮结果：

- 冒烟套件 **37/37 通过**，并连跑 8 轮无回归；覆盖下列断言。
- 精确停点：阈值 N=300000、100007（奇数，跨 64KiB 读块）、1048576、4194304、0
  均实测 `forwarded_bytes == N`，暂停期间计数冻结、FIN 不送达。
- 背压：16 MiB 载荷在 N=300000 暂停后，发送器实际卡在 `drain()`（仅发出约 3.75 MiB），
  代理 `pending_bytes` 峰值约 62 KiB，全程 **≤ 256 KiB**，接收端无回执。
- 恢复完整性：恢复后字节数与双端 SHA-256 全部一致（含 32 MiB 完整采样包，
  实测 33554432 字节、约 3–7 秒、`hash_match=true`、接收端 `eof_seen=true`）。
- 回执阶段：`r2c_pause_after=16` 时上传照常半关闭完成，run 停在 `waiting-receipt`，
  恢复 r2c 后才收到回执——可据此区分超时在上传还是回执阶段。
- 强断：暂停中的连接 `force-close` 约 20–40 ms 返回，发送端收到
  `ConnectionResetError`，连接从活动列表移除，控制 API 与后续新连接不受影响。
- N=0 边界：立即暂停、暂扣全部字节与 FIN，恢复后 8 字节测试载荷与回执完整。

> 说明：发送器对回执读取带 `SENDER_RECEIPT_TIMEOUT`（默认 120s）超时保护，
> 回执阶段长期无 EOF 时 run 置 `failed` 并注明 receipt-phase stall，而非永久挂起。

## 目录

```
proxy/app.py       故障代理（asyncio 双向 pump + FastAPI 控制面）
sender/app.py      发送器（确定性 32MiB 采样流、半关闭、读回执，FastAPI /docs）
receiver/app.py    接收端（读到 EOF 算字节数/SHA-256，回执 + 极简 HTTP 查询）
tests/smoke_test.py 端到端冒烟
scripts/           本机启停脚本
Dockerfile docker-compose.yml requirements.txt
```
