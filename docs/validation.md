# 验证说明

## 自动化检查

使用 Python 3.11 和 MinerU 4.0.12，15 项测试通过。真实 SDK 测试使用生成的图文 PDF 与 flash 原生文本路径，不需要预先下载模型。

覆盖内容包括：

- API 响应、PDF 格式、加密文件、大小和页数限制。
- 缓存按图片选项和解析档位隔离，相同请求合并，大结果跳过缓存。
- 并行任务、队列容量、超时后保留任务名额，以及解析失败后的恢复。
- 图片、空表格和图表的回退渲染；无图输出保留文字、表格、公式，且不修改原始 SDK 结果。

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check app tests scripts
.venv/bin/ruff format --check app tests scripts
.venv/bin/python -m pip check
docker compose config --quiet
```

## GPU 集成验证

验证环境使用 MinerU 4.0.12、Torch 2.13.0+cu130、vLLM 0.29.0 和 Pro-2605-1.2B 模型。API 小模型与 vLLM 共用一张 L40，Torch GPU 运算、模型加载、启动预热及容器健康检查均通过。

GPU 计时测试累计 144 次请求、1,002 页，失败数为 0，预热排除在统计之外。检查内容为 HTTP 状态、页数、非空 Markdown 和部分关键文字，不代表逐字 OCR、公式或表格的标注集精度评估。汇总见[性能测试](benchmark.md)。

合成 PDF 冒烟测试验证带图输出、缓存命中及无图输出；四种解析档位的无图检查均通过。错误扩展名、空文件、损坏文件、密码加密文件返回 400，无效档位返回 422。GPU 镜像的依赖检查通过。

## 数据与复现

仓库只保留通用方法和汇总结果，不包含部署地址、主机或用户信息、原始请求记录、环境快照、输入 PDF、逐次采样或中间 JSON。复现时使用自备样本和部署环境，原始结果写入忽略的 `artifacts/` 目录。
