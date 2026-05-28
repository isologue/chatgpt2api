# 构建平台，由 Docker BuildKit 注入。
ARG BUILDPLATFORM

# 目标运行平台，由 Docker BuildKit 注入。
ARG TARGETPLATFORM

# 目标 CPU 架构，由 Docker BuildKit 注入。
ARG TARGETARCH

# ============================================
# 前端构建阶段
# 用于编译 web 管理页面静态资源。
# 只有最终目标为 app-with-web 时，产物才会被复制进运行镜像。
# ============================================
FROM --platform=$BUILDPLATFORM node:22-alpine AS web-build

# npm 镜像源地址。
# 国内环境可使用 npmmirror 提升依赖下载速度。
ARG NPM_REGISTRY=https://registry.npmmirror.com

# 前端项目工作目录。
WORKDIR /app/web

# 先复制依赖清单，便于利用 Docker 层缓存。
COPY web/package.json web/bun.lock ./

# 配置 npm 镜像源并安装前端依赖。
RUN if [ -n "$NPM_REGISTRY" ]; then npm config set registry "$NPM_REGISTRY"; fi \
    && npm install

# 复制版本号文件，用于前端构建时注入版本信息。
COPY VERSION /app/VERSION

# 复制前端源码。
COPY web ./

# 构建前端静态站点，并注入版本号环境变量。
RUN NEXT_PUBLIC_APP_VERSION="$(cat /app/VERSION)" npm run build


# ============================================
# 后端基础运行阶段
# 安装 Python 运行环境和按需依赖。
# 这个阶段既可作为纯 API 镜像，也可作为带前端镜像的基础层。
# ============================================
FROM --platform=$TARGETPLATFORM python:3.13-slim AS app-base

# 目标运行平台参数。
ARG TARGETPLATFORM

# 目标 CPU 架构参数。
ARG TARGETARCH

# 是否启用国内镜像源优化。
# 1 表示启用，0 表示关闭。
ARG USE_CN_MIRROR=1

# Debian 软件源地址。
ARG APT_MIRROR=https://mirrors.aliyun.com/debian

# Debian 安全更新源地址。
ARG APT_SECURITY_MIRROR=https://mirrors.aliyun.com/debian-security

# Python 包索引地址。
ARG PYPI_INDEX_URL=https://mirrors.aliyun.com/pypi/simple

# pip 信任的镜像主机。
ARG PIP_TRUSTED_HOST=mirrors.aliyun.com

# 是否安装 Git 存储后端依赖。
# 1 安装，0 不安装。
ARG ENABLE_GIT_STORAGE=0

# 是否安装 PostgreSQL 存储后端依赖。
# 1 安装，0 不安装。
ARG ENABLE_POSTGRES_STORAGE=0

# Python / pip 运行环境变量。
# 关闭 pyc，打开实时日志，关闭 pip 版本检查，并配置镜像源。
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_INDEX_URL=${PYPI_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

# 后端应用工作目录。
WORKDIR /app

# 配置 apt 镜像源并安装系统依赖。
# 默认只安装基础运行所需组件；
# 启用 Git / PostgreSQL 存储时，再按需追加安装相关依赖。
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

# 复制 Python 依赖清单。
COPY requirements ./requirements

# 先安装核心依赖，再按构建开关安装可选存储后端依赖。
RUN set -eux; \
    pip install -r requirements/docker-core.txt; \
    if [ "$ENABLE_GIT_STORAGE" = "1" ]; then \
        pip install -r requirements/docker-storage-git.txt; \
    fi; \
    if [ "$ENABLE_POSTGRES_STORAGE" = "1" ]; then \
        pip install -r requirements/docker-storage-postgres.txt; \
    fi

# 复制后端运行所需文件。
COPY main.py ./
COPY config.json ./
COPY VERSION ./
COPY api ./api
COPY services ./services
COPY utils ./utils
COPY scripts ./scripts

# 暴露容器服务端口。
EXPOSE 80

# 容器启动命令。
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--access-log"]


# ============================================
# 纯后端目标
# 不包含前端静态资源，适合只跑 API 的场景。
# ============================================
FROM app-base AS app


# ============================================
# 完整目标
# 在后端基础镜像上复制前端构建产物。
# 适合需要管理页面和静态资源访问的场景。
# ============================================
FROM app-base AS app-with-web

# 将前端构建产物复制到后端服务目录中。
COPY --from=web-build /app/web/out ./web_dist
