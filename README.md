# MinerU PDF API

基于 MinerU 4.0.12 的 PDF 转 Markdown 服务，支持 flash/basic/standard/advanced。SQLite WAL 保存全局任务、幂等记录和阶段指标；CPU API、GPU worker 和 vLLM 分开运行。

## 部署

需要 Docker Compose 2.24.4+、NVIDIA Container Toolkit、兼容 CUDA 13.0 的驱动，以及本地持久磁盘。API 镜像自行编译并校验 SQLite 3.51.3，避免依赖系统中未修复并发 WAL 问题的旧版本。

```bash
python3 scripts/init_env.py
# 修改本地 .env 中的 GPU_ID、API_PORT 等参数。
# 初始化脚本自动生成 worker 凭据，不打印密钥，已有配置会保留。
docker compose build

docker compose run --rm --no-deps --entrypoint mineru-kit mineru-worker \
  models download --tier standard --small-backend torch --vlm-engine vllm -s modelscope

docker compose up -d --scale mineru-worker=2
curl http://127.0.0.1:3888/health
```

默认 GPU 基础镜像为 `vllm/vllm-openai:v0.29.0`，可通过 `VLLM_IMAGE` 替换兼容镜像源。API 是 CPU 镜像，不加载 Torch 或 GPU 模型。首次模型下载和预热需要时间；健康接口的 `model_ready` 表示存在近期活跃 worker。

## 异步任务

```bash
curl -F 'file=@example.pdf' -H 'Idempotency-Key: example-request-1' \
  'http://127.0.0.1:3888/jobs?tier=standard&encode_img=true'
# 返回 HTTP 202、任务 id、status_url 和 result_url。
curl http://127.0.0.1:3888/jobs/REPLACE_WITH_TASK_ID
curl http://127.0.0.1:3888/jobs/REPLACE_WITH_TASK_ID/result
```

状态为 queued/running/succeeded/failed。结果未就绪时返回 202 和 Retry-After；成功时返回 `success/message/markdown/pages/duration`。任务 ID 是随机标识，不包含文件名。

相同内容、档位、图片选项、解析器及模型版本默认共享解析和缓存。`Idempotency-Key` 用于重试同一个逻辑请求，同一键用于不同输入返回 409；不同键可以共享同一次解析。`disable_cache=true` 跳过内容缓存和内容去重，显式幂等键仍然有效。

`encode_img=false` 省略图片资源，保留识别文字、表格和公式。扫描件在 flash 下仍可能调用 OCR；不同档位的质量需单独评估。

## 同步兼容接口

```bash
curl --max-time 6000 -F 'file=@example.pdf' \
  'http://127.0.0.1:3888/v2/parse/file?tier=standard&encode_img=true'
```

同步接口通过同一持久队列等待结果。HTTP 等待超时返回 504，任务继续执行。文件无效返回 400，大小/页数或单文件工作量超限返回 413，上传/传输/任务容量满返回 503（含 Retry-After）。长文档或高负载场景宜使用异步接口。

## 扩容

同一张卡增加独立 worker 进程：

```bash
docker compose up -d --scale mineru-worker=2
```

增加第二张卡及其两个 worker：

```bash
GPU_ID=0 GPU_ID_B=1 docker compose -f compose.yaml -f compose.scale.yaml up -d \
  --scale mineru-worker=2 --scale mineru-worker-b=2
```

覆盖配置明确替换 GPU 设备列表。所有 worker 从同一全局队列领取任务，不拥有本地待执行队列。推理网关在独立 vLLM 副本之间按活跃请求数分配调用，并限制活跃和等待数量。

远端机器也可以运行 `python -m app.worker`，只需配置 `WORKER_API_URL`、`WORKER_API_TOKEN`、模型和 GPU；worker 不读取 SQLite 文件，也不需要共享对象目录。SQLite 文件保留在调度端本地磁盘，不支持放在 NFS 上供多个主机打开；调度端当前不是跨节点主动主动部署。详见[架构与运维](docs/scaling.md)。

## 主要配置

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `API_PORT` | 3888 | API 端口 |
| `GPU_ID` / `GPU_ID_B` | 0 / 1 | 两个 GPU 解析单元的设备编号 |
| `API_PROCESSES` | 1 | 同一调度主机的 API 进程数，共享同一个 SQLite 文件 |
| `PARSE_WORKERS` | 2 | 每个 GPU worker 进程的执行线程数 |
| `MAX_PENDING` | 64（Compose） | 全局运行及等待任务总数 |
| `QUEUE_MAX_WORK` | 4096 | 全局加权工作量，按页数和档位估算 |
| `QUEUE_MAX_UPLOADS` | 8 | 全局上传与校验并发 |
| `QUEUE_MAX_SYNC_WAITERS` | 8 | 全局同步 HTTP 等待数 |
| `QUEUE_MAX_TRANSFERS` | 8 | 结果下载及 worker 文件传输并发 |
| `QUEUE_LEASE_SECONDS` | 60 | 任务租约秒数 |
| `QUEUE_HEARTBEAT_SECONDS` | 10 | worker 续租间隔，必须小于租约的一半 |
| `QUEUE_MAX_QUEUE_SECONDS` | 300 | 单次排队最长秒数 |
| `QUEUE_MAX_ATTEMPTS` | 3 | 最大执行尝试次数 |
| `QUEUE_RETENTION_SECONDS` | 86400 | 已结束任务和结果保留秒数 |
| `QUEUE_MAX_RESULT_BYTES` | 67108864 | 单份 Markdown 结果最大 64 MiB |
| `MAX_UPLOAD_BYTES` / `MAX_PAGES` | 52428800 / 1000 | 单文件大小和页数限制 |
| `REQUEST_TIMEOUT` | 6000 | 同步接口等待上限 |
| `WORKER_MAX_RUN_SECONDS` | 6000 | worker 执行硬期限，超时退出进程释放 GPU |
| `CACHE_BYTES` / `CACHE_TTL` | 134217728 / 3600 | 全局内容缓存字节预算和有效秒数 |
| `OBJECT_STORAGE` | local | local 或 s3 |
| `OBJECT_DIR` | /data/objects（Compose） | 调度端的文件存储目录 |
| `MODEL_REVISION` | MinerU2.5-Pro-2605-1.2B | 模型版本标识，参与缓存键和 worker 校验 |

初始队列容量写入数据库，多 API 进程必须使用一致配置。修改现有容量可通过管理命令完成，已有任务不会删除；随后同步更新部署环境配置：

```bash
docker compose exec mineru-api python -m app.queue_admin limits \
  --max-pending 64 --max-work 4096 --max-uploads 8
```

## 监控、测试和备份

`/metrics` 提供全局队列、任务事件及分阶段直方图。网关的内部 `/metrics` 提供各 vLLM 副本请求耗时、网关等待和活跃数；vLLM 本身的指标另行采集。指标只使用固定阶段、档位和副本序号，不记录文件名、正文、主机或部署地址。

```bash
python -m venv .venv
.venv/bin/pip install -e '.[test,s3]'
.venv/bin/python scripts/build_sqlite.py
LD_PRELOAD="$PWD/artifacts/sqlite/lib/libsqlite3.so.0" .venv/bin/python -m pytest -q
.venv/bin/ruff check app tests scripts
.venv/bin/ruff format --check app tests scripts
.venv/bin/python scripts/smoke_test.py http://127.0.0.1:3888

.venv/bin/python scripts/load_test.py http://127.0.0.1:3888 samples/*.pdf \
  --rate 0.2 --duration 60 --output artifacts/load-test.md

docker compose exec mineru-api python -m app.queue_admin backup /data/backup.sqlite3
```

定速压测同时报告提交窗口和包含排空时间的吞吐，以及拒绝、失败、未完成和压测端丢弃数量。原始文档、数据库、备份、密钥、日志和测试产物均不纳入版本控制。

参见[验证说明](docs/validation.md)、[性能汇总](docs/benchmark.md)和[实施计划](docs/implementation-plan.md)。上游：[MinerU](https://github.com/opendatalab/MinerU)、[SQLite WAL](https://sqlite.org/wal.html)。
