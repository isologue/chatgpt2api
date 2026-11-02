# 图片线程内部等待调查（2026-11-01）

本轮按用户要求先处理应用内部等待；下载并发保持 64，不调整超时、切号或线上 Compose，不额外下载、解码返回图片。保留现有账号内存调度索引。

## 证据和边界

- 旧版本为 sha256:42a164be91daf3b0edd774ececb3cfbaa339d2d16737cf6c1bb8fb1b7dbee23a（实际发布标识以 output/internal_wait_deploy_precheck_20261101.json 为准）。
- 200 请求期间对图片事件循环线程各做约 1.5 秒 futex 原生栈采样。下载阶段看到 pthread_cond_timedwait -> PyEval_RestoreThread -> curl_cffi，单次约 4.26ms；另有 CFFI 正文回调的执行权交接栈。采样结束 TracerPid 为 0，临时跟踪文件已删除。
- 这证明存在重新获取 Python 执行权的等待，不能单凭短时样本解释全部历史停顿，也不能以带探针请求延迟作无探针对照。
- 后续一波 500 请求附带 60 秒、49Hz、仅记录持有 GIL 的应用 Python 函数栈采样，不采集 locals、请求正文或其他项目。采样器使用临时目录，结束删除；没有安装到服务器系统环境。
- 图片线程在采样中占主要份额，并未观察到某一条后台线程持续独占执行权。因此修复针对频繁交接的放大效应，不宣称找到一个独占数十秒的业务锁。

## 可复现的本地对照

scripts/benchmark_image_gil.py 使用独立进程、本机回环 TLS/HTTP2 服务端，每轮 100 请求、每请求 512KiB 合成字节，校验证书并完整读取正文。Docker 使用 --network none；不访问真实上游，不生成图片。两个模式交替执行各两轮，只改变图片 socket_action 调用边界。

| 场景 | 原 CFFI 边界 | 保留执行权的调用边界 |
| --- | --- | --- |
| 两个后台 Python 竞争线程，请求 P95 | 16.699 / 15.330 秒 | 4.826 / 5.277 秒 |
| 同场景 curl socket 累计墙钟时间 | 12.811 / 11.885 秒 | 0.225 / 0.238 秒 |
| 同场景单次 curl socket 最大值 | 225.55 / 137.27ms | 10.18 / 5.72ms |
| 无竞争线程，请求 P95 | 375.47 / 323.71ms | 353.43 / 351.58ms |
| 每轮成功请求 | 100/100 | 100/100 |

竞争线程仅用于复现机制，不代表生产中的线程工作内容。无竞争场景未证明明显加速；竞争场景仍有事件循环延迟，不能宣称修复所有调度等待。

结果：output/internal_wait_gil_benchmark_contended.json、output/internal_wait_gil_benchmark_idle.json。

## 实现

services/image_curl.py 从已加载的 curl_cffi 获取 curl_multi_socket_action 的真实函数指针，检查完整参数/返回类型，再通过 ctypes.PYFUNCTYPE 调用。仅用于 Linux、启用 GIL 的 CPython；不满足运行时或 ABI 条件时沿用原库。没有另载 libcurl，没有全局 monkey patch 或修改 Python 线程时间片。

仅非阻塞 multi socket 驱动在一次调用期间保留 Python 执行权，避免原生函数和 Python 回调之间反复交接。现有回调队列的 2ms 批次预算、让出、取消、watchdog、错误传播及账号私有 multi/连接/Cookie/认证保持原行为。单次原生调用仍不可中断，批次预算不是抢占保证；若某次底层调用本身变慢，仍会影响其他 Python 线程。

## 验证和发布

- Windows 原有相关回归 58 项通过；新增 ABI 回退用例后，边界专项 5 项通过。
- Linux 扩展回归 153 项通过；最终镜像边界专项 5 项通过。首次离线容器缺少 tokenizer 缓存导致的 10 项失败，在补齐测试缓存后全部通过。
- 本地 docker compose -f .\docker-compose.warp.yml up -d --build 完成。
- 镜像 sha256:2e32b8cdda30a65b7dec03bbf8ab6e4ad4964c83d2aa5e0d771c62cb2477e0ab，推送 milodk/chatgpt2api:hot。线上按 pull/down/up 原流程部署，Compose SHA256 未改变；保留 rollback-before-internal-wait-20261101 标签。
- 线上运行文件、原生边界启用状态及下载并发 64 由发布前检查核验。线上复测结果见后续记录。

本轮不合并分支或推送 Git，工作区继续保留现有未提交改动。


## 线上无探针复测

入口均为服务器本机发布端口 127.0.0.1:3005，不包含公网客户端/CDN。同步 gpt-image-2、n=1、url，多种安全提示词；每波结束等待在途任务清零，再收集结构化日志。均未额外下载验图。

| 指标 | 新版 200 请求 | 新版 500 请求 |
| --- | --- | --- |
| 北京时间 | 15:15:59—15:17:41 | 15:19:09—15:21:04 |
| 成功数 | 196/200 | 495/500 |
| 502 原因 | 4 个 no_image_generated | 5 个 no_image_generated |
| 504 / 空体 502 | 0 / 0 | 0 / 0 |
| 请求 P50 / P95 | 40.819 / 48.695 秒 | 86.638 / 113.316 秒 |
| 单次下载正文 P95 | 8.449 秒 | 12.532 秒 |
| 等下载名额 P95 | 7.884 秒 | 40.932 秒 |
| curl socket 单次最大值 | 29.844ms | 31.712ms |
| 采样中 >=50ms 慢 socket 回调 | 0 | 0 |
| 回调队列最大等待 | 1.336 秒 | 4.456 秒 |
| API / 图片循环峰值 | 443.622 / 527.730ms | 716.493 / 1105.310ms |

500 请求创建跨度约 16.319 秒，不能声称 500 个同时进入上游。该轮 495 次下载最终均 EOF，9 次 curl28 下载尝试错误经原地址重试恢复，没有下载重试耗尽或下载失败切号。3 个请求曾切号一次，需与下载错误分开；全部客户端最终成功或明确返回 no_image_generated。

500 轮图片循环峰值发生于 15:19:28.022，约 2.427 秒后首次监控样本 last_ms 低于 10ms；API 峰值于 15:19:25.081，约 3.161 秒后首次观察到低于 10ms。为低频采样恢复上界观察，不能精确定位恢复瞬间。

对比之前 12:56 无探针旧版 500 波次：curl socket 累计墙钟 40.677 秒 / 线程 CPU 27.145 秒，新版为 28.422 / 28.126 秒；差值从约 13.532 秒缩至 0.296 秒。单次最大值从 401.546ms 降到 31.712ms。这与本地竞争对照一致，支持修复了频繁执行权交接造成的等待放大。

但该次旧版请求 P95 为 93.553 秒，新版为 113.316 秒，整体没有证明提速。正文读取 5 秒窗口峰值从 27.460 降到 24.313MiB/s；下载队列仍然存在。上游、时段及账号状态不相同，不能把变化全部归因于代码，也不能把带采样的 15:00 旧版 500 波次当无探针基线。

新版 500 波次启动集中期回调队列最多 975 项，最大排队发生在尚无下载正文流量的阶段。单次调用已经较短，但单图片循环仍需串行消化大量 TLS/元数据就绪事件；回调队列和循环停顿尚未全部解决。后续应继续区分初始化集中负载与下载吞吐，而不是宣称此次修复根治高并发尾延迟。

完整证据：

- output/loadtest_detailed_20261101T151548/transfer_summary.json、report.md。
- output/loadtest_detailed_20261101T151858/transfer_summary.json、report.md。
- output/internal_wait_native_stacks_20261101.jsonl、output/internal_wait_gil_profile_20261101.json。
- output/internal_wait_release_20261101.json、output/internal_wait_remote_deploy_20261101.jsonl、output/internal_wait_final_health_20261101.json。

最后健康核验：新镜像运行，重启 0、无 OOM；图片任务、下载中和下载排队均为 0；图片线程 TracerPid=0。线上 Compose SHA256 与部署前一致。
