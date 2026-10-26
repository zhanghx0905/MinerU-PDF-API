# Isolate API dependencies while reusing the GPU base runtime.
ARG VLLM_IMAGE=vllm/vllm-openai:v0.29.0
FROM ${VLLM_IMAGE}
ARG SMALL_BACKEND=torch
# Reuse Torch/CUDA from the base image. MinerU and its HTTP client live in
# the venv; vLLM itself remains exclusively in the inference container.
RUN if [ "$SMALL_BACKEND" = "torch" ]; then \
      /usr/bin/python3 -m pip uninstall -y vllm \
      && /usr/bin/python3 -m venv --system-site-packages /opt/mineru-venv; \
    else /usr/bin/python3 -m venv /opt/mineru-venv; fi
ENV PATH="/opt/mineru-venv/bin:${PATH}"
WORKDIR /app
COPY pyproject.toml ./
COPY requirements-gpu.txt ./
COPY scripts/install_dependencies.py /tmp/install_dependencies.py
# Cache dependencies independently from application code.
ARG PIP_FIND_LINKS=""
RUN python3 /tmp/install_dependencies.py
COPY app ./app
RUN python3 -m pip install --no-cache-dir --no-deps --no-build-isolation .
ENV MINERU_MODEL_SMALL_BACKEND=${SMALL_BACKEND} \
    MINERU_MODEL_SOURCE=modelscope \
    PARSE_TIER=standard \
    PYTHONUNBUFFERED=1
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/health', timeout=3)"
ENTRYPOINT ["python3", "-m", "uvicorn"]
CMD ["app.main:app", "--host", "0.0.0.0", "--port", "3000", "--workers", "1"]
