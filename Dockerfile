FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    CSMAR_MCP_STATE_DIR=/var/lib/csmar-mcp \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000 \
    MCP_TRANSPORT=streamable-http

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY csmar_mcp ./csmar_mcp
COPY csmarapi ./csmarapi

RUN uv sync --frozen --no-dev \
 && mkdir -p "$CSMAR_MCP_STATE_DIR"

EXPOSE 8000

ENTRYPOINT ["uv", "run", "--no-sync", "csmar-mcp"]
