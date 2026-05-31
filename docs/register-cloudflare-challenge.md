# 注册流程 Cloudflare Challenge 排查记录

## 背景

在本地通过 `docker compose -f docker-compose.postgres.yml up -d --build` 启动项目后，注册任务在 `platform authorize` 阶段偶发或连续失败，日志类似：

```text
任务1 注册失败，原因: 被 Cloudflare 拦截，请更换 IP 重试
```

后续增强日志后，确认上游真实返回的是 Cloudflare challenge 页面：

```text
content_type=text/html; charset=UTF-8
body=...<title>Just a moment...</title>...
```

这说明问题不是前端展示错误，也不是简单的状态码误判，而是注册链路中的程序化 HTTP 请求确实被 Cloudflare 风控拦截。

## 现象判断

- 浏览器手动访问 `https://auth.openai.com` 和 `https://platform.openai.com` 可以正常打开。
- 但注册流程里的 `curl_cffi` 请求在 `platform authorize` 阶段可能拿到 challenge 页面。
- 同一台机器、同一出口 IP 下，浏览器可访问并不代表脚本请求一定能通过 Cloudflare。

## 先修复的问题

### 1. 纠正 Cloudflare 误判

原来的判断条件过宽，只要响应头里有 `server: cloudflare` 且状态非 `200`，就可能被当成 challenge。

现在已收紧为：

- 状态码必须是 `403` / `429` / `503`
- 响应内容必须像 HTML 页面
- 页面中还需要出现 Cloudflare challenge 特征，例如：
  - `Just a moment`
  - `/cdn-cgi/challenge-platform/`
  - `challenges.cloudflare.com`

相关代码：

- [services/register/openai_register.py](D:\test\chatgpt2api\services\register\openai_register.py:187)

### 2. 修复注册代理残留问题

此前注册配置存在顶层 `proxy` 已清空，但 `mail.proxy` 仍残留旧值的问题，导致任务仍然尝试连接历史代理地址。

现在已改为始终同步：

- 顶层 `proxy` 为空时，`mail.proxy` 也强制清空

相关代码：

- [services/register_service.py](D:\test\chatgpt2api\services\register_service.py:27)
- [services/register_service.py](D:\test\chatgpt2api\services\register_service.py:71)

## 最终生效方案

只靠一次 `platform authorize` 请求成功率不稳定，因此在 `PlatformRegistrar` 中增加了两层增强：

### 1. Challenge 自动重试

在 `platform authorize` 遇到 Cloudflare challenge 时：

- 最多重试 3 次
- 每次重试前重建 `session`
- 重新生成新的 `device_id`
- 清空上一次授权相关状态
- 进行短暂退避后再试

### 2. 会话预热

在真正请求 `platform authorize` 之前，先按当前 `device_id` 访问：

1. `https://platform.openai.com`
2. `https://auth.openai.com`
3. 带 `login_hint` 的 auth 首页

这样做的目的是先拿一轮更接近真实浏览器访问路径的 cookie 和会话状态，再发起授权请求。

相关代码：

- [services/register/openai_register.py](D:\test\chatgpt2api\services\register\openai_register.py:397)
- [services/register/openai_register.py](D:\test\chatgpt2api\services\register\openai_register.py:408)
- [services/register/openai_register.py](D:\test\chatgpt2api\services\register\openai_register.py:431)
- [services/register/openai_register.py](D:\test\chatgpt2api\services\register\openai_register.py:458)

## 结果

这套“误判修正 + 自动重试 + 会话预热”的组合方案已在当前本地环境验证成功，注册流程可以继续通过 `platform authorize` 阶段。

## 如果后续再次失败

优先看日志是否属于哪一类：

- `platform 会话预热遇到 Cloudflare challenge`
- `platform authorize 遇到 Cloudflare challenge`
- `platform_authorize_http_...`

如果三次重试后仍然持续出现 `Just a moment...`，通常说明：

- 当前出口 IP 被风控较严
- 单纯 HTTP 模拟浏览器的方案已接近上限

这时可以考虑：

- 更换出口 IP 或代理
- 调整请求指纹策略
- 如需更高稳定性，改为浏览器自动化方案
