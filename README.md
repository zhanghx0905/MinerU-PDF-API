# MinerU PDF API

基于 MinerU 4.0.12 的 PDF 转 Markdown 服务，支持 `flash`、`basic`、`standard`、`advanced` 四种解析档位。提供文件校验、有界任务队列、结果缓存和独立 vLLM 推理服务。

## 部署

需要 Docker Compose、NVIDIA Container Toolkit 和兼容 CUDA 13.0 的显卡驱动。默认基础镜像为 `vllm/vllm-openai:v0.29.0`；可通过 `VLLM_IMAGE` 指定兼容的镜像源。

```bash
cp .env.example .env
# 在 .env 中设置 GPU_ID、API_PORT 等部署参数。
docker compose build mineru-api

# 首次部署下载模型；standard 资源集合也供 advanced 使用。
docker compose run --rm --no-deps --entrypoint mineru-kit mineru-api \
  models download --tier standard --small-backend torch --vlm-engine vllm -s modelscope

docker compose up -d
curl http://127.0.0.1:3888/health
```

模型下载和首次初始化需要时间。vLLM 健康后 API 启动，完成预热后接受请求。接口文档位于 `/docs`。

GPU 编号对应宿主机设备，API 小模型和 vLLM 默认共用所选显卡，容器内通常显示为 `cuda:0`。可在 `.env` 中配置，或通过环境变量启动：

```bash
GPU_ID=0 API_PORT=3888 API_PROCESSES=2 PARSE_WORKERS=2 \
  docker compose up -d --no-build
```

API 通过 HTTP 连接 vLLM，复用基础镜像内的 Torch/CUDA。`requirements-gpu.txt` 为该基础镜像提供依赖约束；替换镜像版本时需要重新验证兼容性。

## 调用

```bash
curl --max-time 6000 -F 'file=@example.pdf' \
  'http://127.0.0.1:3888/v2/parse/file?tier=standard&encode_img=true'
```

成功响应：

```json
{"success": true, "message": "", "markdown": "...", "pages": 1, "duration": 1.23}
```

- `tier`：默认 standard；flash 适合原生文本快速预览，advanced 用于更复杂的解析任务。扫描件在 flash 下仍可能调用 OCR，不同档位的内容质量需要单独评估。
- `encode_img`：默认 true，内嵌图片；false 省略图片资源，保留识别文字、表格和公式。
- `disable_cache`：默认 false；true 跳过缓存读取、写入和相同请求合并。

缓存键包含 PDF 内容、图片选项和档位。无效文件返回 400，文件大小或页数超限返回 413，队列满返回 503（含 Retry-After），等待超时返回 504，解析异常返回 500。

## 调度与配置

```text
上传 PDF → 校验 → 缓存 / 相同请求合并 → 有界任务队列
        → MinerU SDK：版面、公式、OCR → HTTP vLLM 推理
        → Markdown 渲染 → 响应
```

整份 PDF 交给 SDK 处理，以保留跨页结构。`API_PROCESSES` 控制独立 API 进程，`PARSE_WORKERS` 控制每进程解析线程；客户端请求并发由调用方控制。独立进程分别加载小模型，共用同一个 vLLM。

多进程下，队列、缓存、相同请求合并和健康计数分别属于各进程。总任务容量上限为 `API_PROCESSES × MAX_PENDING`，缓存预算为 `API_PROCESSES × CACHE_BYTES`，小模型显存占用也随进程数增加。并发配置应根据实际文档和延迟目标压测。

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `API_PORT` | 3888 | 宿主机 API 端口 |
| `GPU_ID` | 0 | API 与 vLLM 共用的宿主机 GPU |
| `VLLM_IMAGE` | vllm/vllm-openai:v0.29.0 | GPU 基础镜像 |
| `SMALL_BACKEND` | torch | GPU 小模型后端；onnx 用于 CPU 对照 |
| `MODEL_DIR` | Docker 数据卷 | 可选的模型目录 |
| `GPU_MEMORY_UTILIZATION` | 0.35 | vLLM 显存比例 |
| `PARSE_TIER` | standard | 默认解析档位 |
| `PRELOAD_MODELS` | true | 启动预热 |
| `API_PROCESSES` | 1 | 独立 API 进程数 |
| `PARSE_WORKERS` | 2 | 每进程解析线程数 |
| `MAX_PENDING` | 8 | 每进程运行与等待任务总数 |
| `MAX_UPLOAD_BYTES` | 52428800 | 单文件 50 MiB |
| `MAX_PAGES` | 1000 | 单文件页数上限 |
| `REQUEST_TIMEOUT` | 6000 | HTTP 等待秒数，包含排队 |
| `CACHE_BYTES` | 134217728 | 每进程缓存 128 MiB，0 为关闭 |
| `CACHE_TTL` | 3600 | 缓存有效秒数 |

HTTP 等待超时后，已经提交的解析仍占用队列名额，直到任务完成并清理临时文件。`/health` 报告版本、预热状态、队列及错误类型，不触发 GPU 推理；多进程下返回接收该请求的进程状态。

## 开发与测试

```bash
python -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest -q
.venv/bin/ruff check app tests scripts
.venv/bin/ruff format --check app tests scripts
docker compose config --quiet
.venv/bin/python scripts/smoke_test.py http://127.0.0.1:3888
```

基准脚本关闭 API 结果缓存，先预热每个输入，再测量响应耗时和吞吐：

```bash
.venv/bin/python scripts/benchmark.py http://127.0.0.1:3888 samples/*.pdf \
  --tier standard --concurrency 1 2 4 8 --requests 8 \
  --output artifacts/benchmark.json
```

输入 PDF、自定义关键文字检查文件和原始测试输出由调用方自行管理。`artifacts/`、JSON/CSV 报告、日志、PDF 样本、模型、密钥和本地环境文件均不纳入版本控制。

代码分为 `app/`（接口、调度和 SDK）、`scripts/`（构建、冒烟和压测）、`tests/`（接口与 SDK 回归）。参见[验证说明](docs/validation.md)和[匿名性能汇总](docs/benchmark.md)。

上游：[MinerU](https://github.com/opendatalab/MinerU)、[MinerU 4.0.12](https://pypi.org/project/mineru/4.0.12/)。
