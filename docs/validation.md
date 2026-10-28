# 验证说明

## 自动化检查

服务 1.1.0 使用 MinerU 4.0.12，43 项测试通过。队列测试使用 SQLite 3.51.3，真实 SDK 测试使用生成的图文 PDF 与 flash 原生文本路径，不需要预先下载模型。

覆盖内容包括：

- PDF 格式、加密、大小、页数和工作量限制；同步与异步接口的状态码。
- multipart 解析前的全局上传准入、分块请求体限制、同步等待和文件传输预算。
- 跨进程领取与全局容量、内容去重、独立幂等键、缓存总预算及结果保留。
- 失效租约、旧 worker 结果隔离、重复完成、有限重试、排队超时和垃圾清理。
- HTTP 等待超时后保留任务、在线备份和容量调整、拒绝未修复 WAL 问题的 SQLite。
- worker HTTP 协议、租约失效后退出，以及推理网关分配和故障后的名额回收。
- 本地存储与 S3 客户端契约、真实 SDK 阶段计时、图片和图表的回退渲染。

```bash
.venv/bin/python scripts/build_sqlite.py
LD_PRELOAD="$PWD/artifacts/sqlite/lib/libsqlite3.so.0" .venv/bin/python -m pytest -q
.venv/bin/ruff check app tests scripts
.venv/bin/ruff format --check app tests scripts
.venv/bin/python -m pip check
# 先运行 scripts/init_env.py，再校验部署配置。
docker compose config --quiet
docker compose -f compose.yaml -f compose.scale.yaml config --quiet
```

测试输出有两条 TestClient 超时参数弃用提示，不影响测试结果。S3 使用模拟客户端验证调用和读取关闭行为，尚未验证真实对象存储服务。

## 容器与真实 SDK

CPU API 镜像完成构建，确认未安装 Torch，SQLite 为 3.51.3。它与独立 worker 通过 HTTP 完成合成 PDF 的带图、缓存命中和无图解析；页数、关键文字和图片内嵌检查通过。GPU 环境同样完成同步兼容接口的这些检查；最终镜像的 flash、basic、standard、advanced 四档关闭缓存后，页数、关键文字与无图输出检查均通过。

真实 GPU 环境使用 L40、MinerU 4.0.12、Torch 2.13.0+cu130、vLLM 0.29.0 和 Pro-2605-1.2B 模型。API 不分配 GPU，worker 与独立 vLLM 共享每张 GPU。单卡与双卡分别运行独立 worker 池，通过同一个 SQLite 队列领取任务。定速到达测量见[性能汇总](benchmark.md)。

## 故障恢复

真实 GPU 测试强制终止正在执行的 worker。租约到期后，另一个 worker 用新租约重新执行；13 页、非空结果和关键文字检查通过。从终止到重新完成约 49.32 秒，包含新 worker 启动及租约等待，不等价于所有部署的恢复上限。

另一个测试在任务处于 queued 时重启 API；原任务 ID 和状态保留，随后以一次执行尝试完成 6 页解析。自动化测试还覆盖了旧租约不能提交、失败重试上限以及同步请求超时不取消持久任务。

这些验证覆盖进程故障和 API 重启；未执行主机掉电、磁盘损坏、真实 S3 故障或跨节点控制平面切换演练。

## 历史验证与精度边界

初始同步版本的 GPU 计时测试累计 144 次请求、1,002 页，失败数为 0，预热排除在统计之外。四种解析档位的无图检查均通过；无效文件和档位检查通过。

以上内容检查为 HTTP 状态、页数、非空 Markdown 和部分关键文字，并非逐字 OCR、公式或表格的标注集精度评估。分阶段计时存在嵌套与重叠，不能简单求和。

## 数据与复现

仓库只保留通用方法和匿名汇总，不包含部署地址、主机或用户信息、原始请求记录、环境快照、输入 PDF、逐次采样或中间 JSON。复现时使用自备样本和部署环境，原始结果写入忽略的 artifacts/ 目录。
