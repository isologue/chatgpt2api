# BuildKit 注入的构建参数
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

# ============================================
# 前端构建阶段
# 仅在最终目标为 app-with-web 时使用
# ============================================
FROM node:22-alpine AS web-build

# 国内环境可传入镜像源
ARG NPM_REGISTRY=https://registry.npmmirror.com

ENV HOME=/tmp \
    NEXT_TELEMETRY_DISABLED=1 \
    npm_config_cache=/tmp/.npm \
    BUN_INSTALL_CACHE_DIR=/tmp/.bun

WORKDIR /app/web

# 先复制依赖清单，便于利用 Docker 层缓存
COPY web/package.json web/bun.lock ./

# web 使用 bun.lock，但 Next 16 在容器内用 Node 执行构建更稳
RUN npm install -g bun \
    && if [ -n "$NPM_REGISTRY" ]; then export NPM_CONFIG_REGISTRY="$NPM_REGISTRY"; fi \
    && bun install --frozen-lockfile

COPY VERSION /app/VERSION
COPY CHANGELOG.md /app/CHANGELOG.md
COPY web ./

RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" node node_modules/next/dist/bin/next build


# ============================================
# 后端基础运行阶段
# 既可作为纯 API 镜像，也可作为带前端镜像的基础层
# ============================================
FROM python:3.13-slim AS app-base

ARG TARGETOS
ARG TARGETARCH

# 是否启用国内镜像优化
ARG USE_CN_MIRROR=1
ARG APT_MIRROR=https://mirrors.aliyun.com/debian
ARG APT_SECURITY_MIRROR=https://mirrors.aliyun.com/debian-security
ARG PYPI_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
ARG PIP_TRUSTED_HOST=mirrors.aliyun.com

# 是否安装可选存储后端依赖
ARG ENABLE_GIT_STORAGE=0
ARG ENABLE_POSTGRES_STORAGE=0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_INDEX_URL=${PYPI_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

WORKDIR /app

RUN set -eux; \
    if [ "$USE_CN_MIRROR" = "1" ]; then \
        for file in /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources; do \
            [ -f "$file" ] || continue; \
            sed -i "s|http://deb.debian.org/debian|$APT_MIRROR|g; s|https://deb.debian.org/debian|$APT_MIRROR|g; s|http://security.debian.org/debian-security|$APT_SECURITY_MIRROR|g; s|https://security.debian.org/debian-security|$APT_SECURITY_MIRROR|g" "$file"; \
        done; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates openssl; \
    if [ "$ENABLE_GIT_STORAGE" = "1" ]; then \
        apt-get install -y --no-install-recommends git; \
    fi; \
    if [ "$ENABLE_POSTGRES_STORAGE" = "1" ]; then \
        apt-get install -y --no-install-recommends gcc libpq-dev; \
    fi; \
    rm -rf /var/lib/apt/lists/*

COPY requirements ./requirements

RUN set -eux; \
    pip install -r requirements/docker-core.txt; \
    if [ "$ENABLE_GIT_STORAGE" = "1" ]; then \
        pip install -r requirements/docker-storage-git.txt; \
    fi; \
    if [ "$ENABLE_POSTGRES_STORAGE" = "1" ]; then \
        pip install -r requirements/docker-storage-postgres.txt; \
    fi

COPY main.py ./
COPY config.json ./
COPY VERSION ./
COPY api ./api
COPY services ./services
COPY utils ./utils
COPY scripts ./scripts

EXPOSE 80

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--access-log"]


# ============================================
# 纯后端目标
# ============================================
FROM app-base AS app


# ============================================
# 完整目标
# 复制前端构建产物到运行镜像
# ============================================
FROM app-base AS app-with-web

COPY --from=web-build /app/web/out ./web_dist
