FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY relay ./relay
COPY LICENSE README.md ./
RUN uv sync --frozen --no-dev

FROM python:3.12-slim
ARG POLARIS_HOST
ENV POLARIS_HOST=${POLARIS_HOST} DATA_DIR=/data PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN useradd --system --uid 10001 --home /app relay && mkdir /data && chown relay /data
COPY --from=build --chown=relay /app /app
WORKDIR /app
USER relay
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=60s --timeout=5s CMD python -c "import urllib.request,os; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=4)" || exit 1
CMD ["polaris-relay", "serve", "--port", "8080"]
