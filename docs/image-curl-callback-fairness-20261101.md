# curl 回调公平调度研究与优化（2026-11-01）

## 范围和基线

- 分支：codex/image-curl-callback-fairness；稳定基线为 main / origin/main 的 4c0ebd7959c3f1344f8361c25dea3739a4444dcb。
- 继续调查上一轮线上 200 并发中的图片循环短时拥堵；本轮不访问线上、不发真实生图请求、不调整账号或并发配置，不引入 Nginx。
- 账号仍独享 curl multi、连接、Cookie 和认证；紧凑账号索引方案继续暂缓。

## 已复现的定时器边界

当前锁定的 curl_cffi 依赖中，timer_function 收到 timeout_ms=-1 后取消旧 timer，但仍通过 call_later(-.001, process_data, -1, 0) 创建一次立即执行的 timer。libcurl CURLMOPT_TIMERFUNCTION 的约定是：-1 删除 timer，0 尽快异步处理，正数设置指定延迟。

本地对真实 ImageAsyncCurl 实例先通知 50 毫秒，再通知 -1；等待 10 毫秒。基线结果：cancel_notification_scheduled_timer=true，callbacks_after_cancel=1。证据为 output/curl_timer_cancel_baseline_20261101.json。

这证明存在多余驱动，不证明该边界单独造成上一轮 2.21 秒停顿，也没有证据称其会形成无限循环。

## 实现

### 定时器契约

仅为项目 ImageAsyncCurl 设置独立 TIMERFUNCTION 回调，不全局修改 curl_cffi：

- 负值删除已安排 timer，以及尚未执行的旧 timer 通知；0 和正值仍通过事件循环安排。
- 多次 timer 更新只保留当前有效 timer。
- 同一排队项若同时包含 watchdog 通知，删除 timer 不会删除 watchdog 兜底。
- 保留 libcurl 的 socket 读写需求、curl 超时和现有业务预算；socket 注册由项目回调按最新需求管理。

### 按循环调度回调

services/image_curl.py 新增 _CallbackQueue：

- 原生通知先入当前循环的队列，同一账号驱动和同一 fd 的未执行读写通知合并，事件位按 OR 保留；不同账号不合并。
- socket 通知排队时暂时撤下对应 fd 的读写监听，防止同一就绪状态在等待期间不断唤醒循环；处理后按 libcurl 最新要求恢复监听。REMOVE 会移除排队旧事件，重复 REMOVE 也可安全处理。
- 每批处理最多 16 项或累计约 2 毫秒，再安排下一轮处理，使业务任务、超时和取消有机会执行。这是回调调度预算，不是生图并发上限；单次底层调用不可被中途抢占。
- FIFO 处理，不将刚入队的通知插到其他账号前面；已关闭驱动、无活动请求、已移除 socket 的旧通知不再驱动 curl。
- 关闭驱动时清除其待处理通知；空队列取消自己的调度 handle，不保留一个循环空转任务。
- watchdog 不为已有排队通知的驱动重复补偿，仍保留 1 秒无进展后兜底和原有批次让出。
- 队列回调使用空 Context，不继承创建队列时的某个账号请求上下文；没有额外逐请求线程。

### 有界监控

services/image_loop_work.py 沿用每 0.5 秒发布、近 300 秒、最多 600 快照的限制：

- 保留 curl_socket、curl_completion、business_slice 原指标。
- 新增按 socket / timer / watchdog 触发来源统计的完整执行次数、墙钟耗时和 CPU 耗时；来源耗时与原有网络/通知耗时重叠，不能相加。
- 新增通知触发次数、合并次数、处理批次/让出次数、待执行通知数峰值、最长排队时间及删除 timer 的次数。
- 次数按窗口求和，队列峰值和最长等待按窗口取最大；只记录固定类别和数值，不记录 fd、账号、URL、请求正文或认证。
- 监控页“图片循环工作耗时”下显示这些字段。这里的排队是回调通知排队，不是图片任务队列。

## 对照验证

### 固定成本回调风暴（Windows，3 轮交错中位数）

每项回调固定执行约 200 微秒 CPU 工作，200 个驱动；不访问网络，不模拟真实上游。

| 场景 | 基线循环峰值 | 当前循环峰值 | 基线总耗时 | 当前总耗时 |
| --- | ---: | ---: | ---: | ---: |
| 每驱动 1 次通知，共 200 次 | 40.177 毫秒 | 11.587 毫秒 | 41.148 毫秒 | 42.105 毫秒 |
| 每驱动连续 3 次读写通知，共 600 次 | 122.528 毫秒 | 3.285 毫秒 | 123.501 毫秒 | 43.033 毫秒 |

第一种场景工作量没有减少，公平性改善但总耗时增加约 2.3%。第二种场景执行次数由 600 合并成 200，验证全部读写事件位仍保留，因减少重复工作而缩短总耗时。这不意味着线上实际存在同样比例的可合并通知。

### 真实本机 TLS/HTTP（200 并发，3 轮交错中位数）

每账号独立会话：HTML、两次 metadata、512 KiB 流式下载；验证响应字节，以及每个账号 Cookie、Authorization 和 User-Agent 隔离。两侧均为账号独享驱动，并采用 1 秒 watchdog 宽限期。基线取自 Git 4c0ebd7，不与更早的原生 AsyncCurl 混比。

| 平台 / 指标 | 稳定基线 | 当前 |
| --- | ---: | ---: |
| Windows 循环峰值 | 50.358 毫秒 | 25.540 毫秒 |
| Windows 请求 P95 | 2155.779 毫秒 | 2348.405 毫秒 |
| Linux 循环峰值 | 105.635 毫秒 | 20.463 毫秒 |
| Linux 请求 P95 | 1743.631 毫秒 | 1716.806 毫秒 |
| Linux 整波耗时 | 1788.725 毫秒 | 1749.287 毫秒 |
| Linux CPU 耗时 | 1428.127 毫秒 | 1590.281 毫秒 |
| Linux 超时类驱动次数（timer/watchdog 合计） | 1600 | 612 |
| Linux 全部 socket_action 次数 | 6800 | 4779 |

Linux 使用一次性 Docker 容器、--network none，仅连接容器内 127.0.0.1 的临时 TLS 服务，证书为临时文件，退出后删除；研究源码通过只读目录加载，现有应用容器和镜像不替换。两种平台每轮均通过 400 次身份隔离检查及完整响应校验。

Linux 的调度峰值改善约 81%，请求 P95 缩短约 1.5%，整波耗时缩短约 2.2%，CPU 耗时增加约 11%。Windows 请求 P95 增加约 9%，说明不能将公平性改善描述为所有平台吞吐或延迟都会改善。这是本机负载下的收益和代价；不能预测线上生图 P95、成功率或带宽表现。单次底层长调用仍需线上观察。

最终证据：output/curl_fairness_synthetic_final_20261101.json、output/curl_fairness_tls_final_20261101.json、output/curl_fairness_linux_paused_20261101.json 及 stdout/stderr。早期对照还保存在 output/curl_fairness_linux_20261101.json 和 output/curl_fairness_linux_dispatch_20261101.json；一次单轮检查中，若排队期间保留监听，会收到 31120 次 socket 通知、其中 26180 次合并；暂停监听后的最终 Linux 三轮 socket 通知为 4238 / 4322 / 4201 次，未观察到重复合并通知。不是相同轮次逐项配对，不能将两个数字差值直接当作线上收益。早期对照工具把 legacy 模式的 watchdog_grace_secs 显示为 null；本轮实际基线是带 1 秒兜底的 4c0ebd7，已修正工具的显示逻辑，计时值不变。

## 回归和状态

- 23 个相关测试文件的 294 项回归通过：真实本机 HTTP、SSE、下载完整性、预算、取消、同步/异步任务、多图、补查切号、热池及评分。
- 新增公平调度测试覆盖：读写事件位合并、timer 删除不误删 watchdog、回调重入、其他任务可及时执行、关闭/取消/移除 socket、单账号异常隔离、Context 隔离、原生 timer 的 -1/0/正数、单驱动多请求取消/超时隔离、暂停监听和恢复最新读写要求，以及监控聚合不可变性。
- TypeScript 独立检查、git diff --check 通过。
- 已按用户要求完成下方本地构建部署；尚未部署线上、发起线上压测、合入 main 或推送研究改动。

## 重放

```powershell
uv run python scripts/benchmark_image_curl_fairness.py --baseline-ref 4c0ebd7 --requests 200 --repeats 3 --output output/curl_fairness_synthetic_20261101.json
uv run python scripts/benchmark_image_curl.py --baseline-ref 4c0ebd7 --requests 200 --repeats 3 --output output/curl_fairness_tls_20261101.json
```

下一轮部署后，按相同设置做一次 200 并发，重点对齐循环峰值窗口的 socket/timer/watchdog 执行量、合并率、最长排队及让出次数，并同时比较首页/PoW 恢复等待、下载和长尾，确认公平调度有没有引入明显耗时回退。


## 本地构建部署（2026-11-01）

- 已执行 docker compose -f .\docker-compose.warp.yml up -d --build，前端静态构建、镜像构建、主容器重建均成功。
- 新容器 ID：4b448370582dc80027fdc453d23ee01b9d3ae1620f65d88eb8baf91a6a079256；运行镜像与 chatgpt2api:warp 一致，7 个关键源文件与工作区 SHA256 一致。
- 本地入口 http://localhost:3005；首页、设置、监控页面及认证 /api/monitoring 均 HTTP 200。新回调来源类别及前端展示已验证。
- 新镜像内部 8 个并发本机 HTTP 请求全部成功，响应字节一致；关闭后驱动、排队通知及 watchdog 清理正常。未发真实上游生图请求。
- 启动日志无 traceback 或 curl 回调异常；容器重启 0 次，无 OOM；执行中 / 排队均为 0。PostgreSQL、WARP、Privoxy、FlareSolverr 均 healthy。
- 验证结果保存在 output/curl_fairness_local_deploy_verification_20261101.json。


## 线上对照后第二轮：下载诊断与积压调度（2026-11-01）

用户授权本地构建、推送 milodk/chatgpt2api:hot、按现有服务器 Compose 执行 pull/down/up，并自行复测。线上 Compose 不修改，并保留原镜像回退标签。

- 01:10 波次：198/200 成功，P50 48.951s、P95 58.618s，图片循环峰值455.689ms。13次下载正文首次失败、重试均恢复，下载P95 19.834s，首页P95 9.154s；最长回调等待2740.533ms。上一轮原始诊断未保留curl错误码，不能认定13次都是同一原因。
- 第二轮保留2ms时间预算，只在待处理至少32项且最老事件等待超过50ms时逐级增加短回调数量，最多64项。FIFO、账号隔离、取消、原生timer契约、watchdog保持。单个原生调用仍不能抢占。试验过扩大时间片/使用call_soon，但离线停顿或耗时恶化，最终未采用。
- 每次HTTP传输记录回调累计及最长排队；累计值可能有多个fd等待重叠，不能当作端到端耗时的可直接相减分量。全局监控保留按来源最长等待、均值所需的count/total、积压加速批次。
- 下载传输摘要记录bytes、expected_bytes、chunks、速率、间隔、curl错误码和回调等待；新增已读取字节计数，包括失败传输已读部分。禁止保存原始异常消息、账号和签名URL。日志中文展示。
- 下载已缓冲块最多合并256KiB，不为凑块等待后续数据，减少greenlet边界；原有正文上限、超时、重试和共享总预算保持。
- 修正离线benchmark的跨轮队列复用：每轮验证旧队列空后重建，否则基线先创建的dispatcher会被当前版继续沿用。支持直接对照保存的工作区源码。
- 191项相关回归通过；TypeScript独立检查、diff检查通过。本机Linux TLS最终3轮交错中位数：旧版P95 1410.176ms、当前1482.679ms；循环峰值18.597ms/17.127ms；CPU1279.387ms/1321.520ms。离线未证明整体提速，必须以线上结果评估，不能以循环峰值单项宣称成功。
- 本轮本地镜像/推送manifest：sha256:8971ea792d4e1b891980e54ad5b559abb07498644276ae499ec17e80da214a0e；本地页面和认证监控200、6个关键文件hash匹配、无重启/OOM。线上结果追加在后续记录。

本轮忽略证据：output/curl_fair_adaptive_linux_20261101.json、output/curl_adaptive_local_verification_20261101.json、output/curl_adaptive_deploy_precheck_20261101.json、output/curl_adaptive_remote_deploy_20261101.jsonl。


## 第二轮线上结果（01:50）

- 线上按用户命令完成pull/down/up；Compose内容SHA256未变，16个运行文件hash匹配，运行镜像为上述hot digest。
- 200并发：197成功、2个no_image_generated、1个image_result_uncertain。P50 43.877s，P95 49.345s，最慢107.126s；197张成功图片均完整解码。
- 对比01:10：P50 48.951→43.877s，P95 58.618→49.345s，首页P95 9.154→6.716s，下载P95 19.834→12.254s，下载重试13→0，最长回调等待2.741→2.132s。图片循环峰值455.689→580.428ms，成功率99%→98.5%，不能称为全面改善。
- 504为80秒生成硬预算耗尽后补查9.79秒无结果；剩余总预算不足20秒再生成窗口，未切号，不是切号次数配置失效。
- 下载阶段最长回调等待分布P95 328.666ms，首页P95 2088.935ms；下载仍慢，网络饱和尚未证实。下一步研究首页队列拥堵与长尾恢复预算，避免直接将所有耗时归为带宽。
- 完成后执行/排队0、重启0、无OOM、curl回调异常0；未新增下载图片文件。代码仍在codex/image-curl-callback-fairness研究分支，未合并或Git推送。
- 完整报告和原始证据：output/loadtest_detailed_20261101T015002/report.md及同目录JSON/JSONL。
