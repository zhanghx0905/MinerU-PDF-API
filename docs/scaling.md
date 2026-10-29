# 调度运维与扩容

调度器的任务流程、状态机、领取顺序、租约及异常处理见[调度器实现与异常处理](scheduling.md)。本文说明部署和运维操作，容量实测见[性能汇总](benchmark.md)。

## 部署边界

CPU API 协调任务，SQLite WAL 保存元数据，GPU worker 通过认证 HTTP 领取、续租和提交结果。SQLite 必须位于调度主机本地持久磁盘，不能让不同机器通过 NFS 直接打开同一个 WAL 文件。worker 可以跨机器扩容，不需要共享数据库文件或对象目录。

同一调度主机可以启动多个 API 进程，共享同一个 SQLite 文件。WAL 仍然只有一个写入者，增加 API 进程不会增加 SQLite 写入并行度。当前调度端有单节点故障边界，没有跨节点多活或自动故障转移。

API 镜像编译并校验 SQLite 3.51.3，运行时版本检查见 [sqlite_runtime.py](../app/sqlite_runtime.py)。不要用未通过此检查的系统 SQLite 代替部署镜像的运行库。

## 扩容执行资源

同一张 GPU 增加 worker 进程：

```bash
docker compose up -d --scale mineru-worker=2
```

增加第二张 GPU，分别启动两个 worker 进程：

```bash
GPU_ID=0 GPU_ID_B=1 docker compose -f compose.yaml -f compose.scale.yaml up -d \
  --scale mineru-worker=2 --scale mineru-worker-b=2
```

`GPU_ID` 在部署时指定设备，第二个解析单元通过覆盖配置替换 GPU 设备列表。`PARSE_WORKERS` 控制每个 worker 进程内的执行线程数，默认 2；它与容器副本数是两个维度。每张卡上的 worker 与 vLLM 共用资源，增加进程也会增加模型内存和争用，应依据吞吐、P95 和显存测量调整。

远端 worker 需要配置 `WORKER_API_URL`、`WORKER_API_TOKEN`、解析档位、模型位置和 VLM 服务地址；MinerU 版本与 `MODEL_REVISION` 必须与协调端一致。worker 不直接访问 SQLite，文件仍经过协调端中转。

当前没有自动扩缩容控制器。队列长度、加权工作量和最老任务等待时间持续增长时，先判断执行资源、本地 SDK 阶段、推理副本还是控制平面受限。增加队列只允许更多任务等待，不会增加处理能力。

## 调整准入预算

`MAX_PENDING`、`QUEUE_MAX_WORK`、`QUEUE_MAX_UPLOADS` 初值写入数据库，所有 API 进程启动时检查它们与配置一致。修改现有容量使用管理命令：

```bash
docker compose exec mineru-api python -m app.queue_admin limits \
  --max-pending 64 --max-work 4096 --max-uploads 8
```

命令在事务中更新上限，不删除已有任务。降低上限后，已有负载可以暂时高于新上限，新建任务会继续受准入限制，直到负载降低。随后同步更新本地部署配置，否则 API 下次启动会因配置与数据库不一致而失败。

同步等待数、传输数、超时和其他限制来自 API 进程配置，也应保持所有进程一致。优先根据实际文档分布和延迟目标设置预算，具体计费和短任务规则见[调度实现](scheduling.md)。

## 停止、重启与升级

worker 收到 SIGTERM/SIGINT 后设置停止标志，各线程在当前任务结束后停止继续领取；信号与领取可能有并发窗口，不是协调端的全局排空屏障。Compose 默认给 worker 120 秒停止宽限，超过后终止容器。尚未完成的任务依靠租约过期恢复。

API 重启不删除已提交任务，但长时间无法续租会使正在运行的 worker 退出，并触发后续重试。计划维护时应先暂停上游新提交，再等待 `/health` 的 `queued` 和 `running` 均为 0，再停止或更新服务。当前没有内置的全局 drain 接口。

升级模型或解析器时，先排空旧任务，统一更新模型文件、协调端和 worker 版本及 `MODEL_REVISION`，再启动预热和恢复流量。领取接口检查解析器版本与模型标识，但不验证实际权重的内容摘要，也不按旧版本任务自动选择不同 worker。

## 文件存储与清理

`OBJECT_STORAGE=local` 时，文件保留在协调端持久卷；`s3` 时，配置 `S3_BUCKET`、`S3_ENDPOINT_URL` 和标准 AWS 凭据。只有协调端访问这些对象，worker 经认证 API 下载 PDF 和上传 Markdown。

缓存预算只约束可复用的成功结果，任务保留期内的其他结果和待清理文件仍占空间。需要另外监控文件系统或对象存储容量。后台清理失败会保留垃圾键重试，但当前没有全量对象核对或总存储配额；崩溃窗口可能遗留孤儿文件，详见[文件清理与故障边界](scheduling.md#6-文件清理与故障边界)。

文件中转仍消耗协调端带宽和内存。若它成为进一步扩容的瓶颈，可在后续实现预签名直传和下载；当前版本没有这条数据路径。

## 备份与恢复

数据库备份使用 SQLite 在线 backup API：

```bash
docker compose exec mineru-api python -m app.queue_admin backup /data/backup.sqlite3
```

这个命令只备份数据库，不包含 PDF、Markdown、模型或部署凭据。不要单独复制正在变化的数据库主文件代替在线备份；也不要把“在线数据库备份 + 任意时刻的对象复制”当成跨存储一致快照。

当前没有一键联合快照功能。需要可恢复的一组数据时，采用以下维护流程：

1. 暂停上游新提交，等待在途任务结束，并停止 worker。
2. 停止 API，避免任务保留清理和对象删除继续改变备份范围。
3. 在维护容器中对同一持久卷运行备份命令，再备份关联对象。也可在服务全部停止后使用存储系统提供的一致快照；使用 S3 时需保留相应对象版本或采取等效的数据保护方式。
4. 恢复时将数据库与这组对象一起恢复到持久存储，使用匹配的配置、模型与凭据启动 API，再启动 worker 并做冒烟检查。

单解析单元在暂停流量、确认任务排空后的维护命令示例：

```bash
docker compose stop mineru-worker mineru-api
docker compose run --rm --no-deps --entrypoint python mineru-api \
  -m app.queue_admin backup /data/backup.sqlite3
```

多解析单元要停止全部 worker；对象备份完成前保持 API 停止。维护容器复用同一持久卷，只运行备份命令，不启动 API 和清理循环。

停止服务并不自动修复已经丢失的对象。主机掉电、磁盘损坏和真实 S3 故障恢复尚未完成实测，已验证范围见[验证说明](validation.md)。

## 监控与排障入口

API `/metrics` 是同一个数据库的全局视图。多 API 进程不会产生各自独立的任务计数，采集多个入口时应选择单一入口，或对重复的全局视图取 max，不能直接相加。网关指标属于进程内指标，当前部署保持一个网关进程。

阶段指标只使用固定阶段、档位和副本序号，不记录文件名、正文或部署地址。SDK 阶段是嵌套墙钟计时，可能重叠，不能直接相加。网关的 HTTP 时间和准入等待也不能替代 vLLM 内部排队、prefill/decode 和 KV 缓存指标。

排障顺序为：任务状态及错误码 → 全局排队和重试 → worker 与存储日志 → SDK 阶段 → 网关及 vLLM 原生指标。逐项异常的自动处理和人工操作见[异常处理表](scheduling.md#5-异常情况如何处理)。
