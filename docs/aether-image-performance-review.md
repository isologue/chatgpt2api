# Aether 生图链路性能借鉴评估

评估日期：2026-10-30。范围是 `G:\Demo\Aether` 当前工作区源码与 `G:\Demo\chatgpt2api` 当前工作区对照，仅研究性能实现。本次没有调用 Aether 或线上生图接口，没有搬迁旧协议，没有修改业务代码或部署。

## 结论

最值得借鉴的是异步网络等待、后台工作隔离以及细粒度传输诊断。下一步应验证当前项目的图片专用异步 I/O 链路，而不是只扩大提交线程数。Aether 通用 HTTP 客户端缓存不能直接当作其 ChatGPT-Web 生图链路的优势：实际 Web 生图默认走 browser_wreq，该分支仍逐次建客户端；公网图片下载也逐次建客户端。

当前项目的 17.22 秒提交至执行 P95 尚未定位根因。固定 4 个提交线程只是候选因素，本地简化对照未复现 17 秒等待；需要补齐分段计时并在真实负载下定位。

## 1. 追踪到的实际调用链

Aether：

```text
同步图片执行入口（async fn）
  -> maybe_execute_chatgpt_web_image_sync
  -> execute_chatgpt_web_image_with_progress
  -> 本地账号状态复核、bootstrap、requirements / PoW、prepare
  -> web_start_conversation
     -> DirectSyncExecutionRuntime.execute_sync_with_body_diagnostics
     -> send_via_browser_wreq_transport
     -> await 网络响应、await bytes_stream
     -> 收完整段 SSE，再 parse_web_image_sse
  -> resolve_and_download_images
  -> 异步下载图片、轻量格式检查、Base64中间结果
  -> 同步响应转换、部分状态写入、后台成功报告
```

这里的 Sync 表示调用方等待完整结果，不代表使用阻塞 HTTP 客户端。网络请求及正文接收通过 Rust async/await 执行；但 PoW 等函数仍同步调用，不能把整条链路描述成完全没有 CPU 阻塞。

当前项目：

```text
ASGI / FastAPI
  -> 图片队列准入
  -> 固定4线程提交分发器
  -> 图片执行线程（整个生成任务占用）
  -> 同步 Session HTTP
  -> SSE / 下载时另用共享传输线程
  -> 增量SSE解析、补查、图片校验与保存
  -> 日志持久化确认、响应
```

当前结构在网络等待期间仍占住图片执行线程，流式传输另需工作线程。保留线程复用有帮助，但线程数量仍随实际网络并发增长。

## 2. 可借鉴内容与边界

### A. 优先：把网络等待改为协程等待

Aether 的 `web_start_conversation`、`web_poll_conversation`、图片下载均为 async fn。其收益方向是减少挂起请求占用的工作线程、线程栈和唤醒调度，不代表服务器可以无限并发。

Python 侧已有 curl_cffi.AsyncSession，可继续使用现有 libcurl 和浏览器指纹能力，无需照搬旧上游端点。实施时要把生图编排、SSE消费、补查、下载的调用链一起接成 async；仅在同步线程中嵌套事件循环，仍会保留父线程占用。

选号和现有校验通道先保持不变，阻塞账号操作在短工作任务中完成；PoW继续使用现有CPU进程池，图片校验、同步文件I/O等放到适合的工作执行器。保留同步兼容路径，先验证图片接口。

注意当前安装版本 AsyncSession 的 max_clients 默认只有10；接入必须按现有并发设置配置和监控，避免出现隐藏的10连接瓶颈。Session池、Cookie、鉴权头、代理出口和浏览器指纹必须保持账号隔离；不能把一个可变鉴权Session直接给不同账号共用。缓存必须有界并可回收，不为全部账号长期持有客户端，不涉及已暂缓的账号紧凑索引重构。

### B. 优先：增加首块、末块、最长间隔和排队分段

Aether 的 InitialResponseDiagnostics 记录 headers_ms、first_chunk_ms、last_chunk_ms、max_chunk_gap_ms、idle_after_last_chunk_ms、chunk_count、received_bytes、EOF/中断/未收到响应头等状态。相比只知道SSE耗时80秒，这些字段能区分没有响应、持续有数据但没图片、已静默很久、传输中断。

建议当前项目保留现有阶段汇总，增加固定容量的传输摘要，以及：提交分发等待、submit执行、工作线程启动等待、传输连接/句柄等待、完成后事件循环恢复。只保存数字与状态；不保存每块正文、完整图片或凭据，不逐块打印日志。

### C. 后续：成功结果与后台统计分开，但保持日志可靠性

Aether 在若干同步成功路径使用 spawn_sync_report，usage另有独立后台运行时和阻塞任务上限。值得借鉴的是资源隔离及避免非必要后台工作占用接入能力。

当前项目已经有独立日志批量写入器：JSONL正文、SQLite索引、fsync及批量确认；并非每个请求直接同步写PostgreSQL日志。不能将现有批处理重复实现，也不能把1.28秒日志确认直接认定为PostgreSQL连接池瓶颈。

先测清日志确认中的排队、实际写入、fsync、索引提交和确认恢复耗时。若要允许响应先返回，需先确保可靠接纳、失败可追踪、关闭可排空，再异步更新统计/展示索引；不采用仅创建后台任务即认为日志安全的方案。

### D. 后续：内存按字节约束

Aether 对输入、原始下载、SSE包装、保留URL/标识和文本摘要有独立有界处理。当前 download_image_bytes 会收集chunk再join，未在该读取循环中看到明确的图片字节上限。高并发下可以借鉴累计字节计数、可配置单图限制及在途字节监控，减少异常大正文导致的内存放大。

不要照搬Aether的Base64中间层；当前URL返回路径已经能从原始bytes直接保存，不必再绕一轮Base64。

## 3. 不直接采用的实现

1. **完整SSE收集后解析**：Aether此分支先collect，再parse；当前增量解析更适合保留，不回退。
2. **轻量图片校验替代完整性检查**：Aether主要做文件签名、声明MIME及头部尺寸检查，不能等同于完整解码。当前已有裂图事故，不通过减少校验换取速度；可把校验移出关键调度路径。
3. **逐次创建HTTP客户端**：实际browser_wreq和公网图片下载仍逐次构建客户端，不能宣称它们已跨请求复用连接。通用Reqwest缓存是另一分支的实现思路，可作为后续独立研究。
4. **每次生成后远程刷新额度**：Aether会发起延迟后台额度刷新，当前项目已有热池与可用性缓存，不退回逐请求额外刷新。
5. **旧协议端点、PoW参数、补查固定次数和默认长超时**：不迁移。当前协议、账号评分、切号预算与80/10秒超时规则保持独立。
6. **把1k/10k mock流压测当成真实生图证明**：仓库相关脚本默认测 `/v1/chat/completions`，含渐进启动，不能直接与200突发生图比较。

## 4. 本地200并发I/O对照

脚本：`G:\Demo\chatgpt2api\output\_aether_io_comparison.py`。
数据：`G:\Demo\chatgpt2api\output\aether_io_comparison.json`。

仅请求本机HTTP模拟服务，200请求，每个服务端等待0.5秒再结束正文。当前链路使用实际 run_image_in_threadpool 与 ImageBackendSession；异步原型使用 AsyncSession(max_clients=200) 和异步正文读取。

| 实现 | 总耗时 | P95 | Python线程峰值 | 服务端同时在途峰值 |
| --- | --- | --- | --- | --- |
| 当前同步链路，首次启动 | 0.8149秒 | 799.863毫秒 | 405 | 200 |
| 当前同步链路，复用线程 | 0.6923秒 | 669.960毫秒 | 405 | 200 |
| AsyncSession原型 | 0.6805秒 | 625.137毫秒 | 7 | 200 |

线程峰值包括该测试进程已有线程；异步样本基线6、峰值7，当前冷样本基线1、峰值405。异步样本不是运行Aether二进制。测试不包含TLS、代理、Cookie/鉴权隔离、PoW、真实SSE大正文、图片下载解码与业务日志，也不是线上端到端性能估计。

单次样例的结论限于：异步I/O显著减少线程，线程复用后总耗时差异较小；异步原型仍有约57.9毫秒事件循环尖峰（同步复用样本约54.2毫秒），不能据此宣布事件循环问题已解决。线上17秒等待未复现，不能归咎于4线程分发器或承诺改async后消失。

## 5. 推荐实施顺序

1. 增加分段计时与SSE传输摘要，同时做图片专用异步I/O链路；复用当前协议构造和结果解析。
2. 本地模拟校验取消、总预算、SSE静默、补查、下载重试、账号隔离及队列满429；比较线程数、内存、开始上游请求的时间跨度和事件循环延迟。
3. 完成协议回归及构建后再部署，按相同200并发测试冷运行和热运行。先证明资源与启动长尾改善，再考虑更高并发。
4. 最后处理日志确认和大图内存，硬超时恢复作为单独的成功率策略验证，避免把两类改动混在一起归因。

## 关键源码位置

Aether：

- `G:\Demo\Aether\apps\aether-gateway\src\execution_runtime\chatgpt_web_image.rs:664`：主生图编排。
- 同文件 `:1348`：生成请求与完整SSE接收；`:1474`：图片解析下载；`:1960`：公网下载逐次创建客户端；`:2652`：后台额度刷新；`:3376`：默认browser_wreq；`:3535`：完整SSE解析。
- `G:\Demo\Aether\apps\aether-gateway\src\execution_runtime\transport.rs:2265`：异步正文收集；`:3006`：browser_wreq提交；`:4767`：逐次创建客户端；`:136`及`:369`：通用Reqwest缓存分支。
- `G:\Demo\Aether\apps\aether-gateway\src\execution_runtime\chatgpt_web_image\diagnostics.rs:785`：首次响应诊断。
- `G:\Demo\Aether\apps\aether-gateway\src\usage\reporting\mod.rs:151`：后台成功报告。
- `G:\Demo\Aether\crates\aether-usage\runtime\src\executor.rs:32`：独立usage运行时。

当前项目：

- `G:\Demo\chatgpt2api\services\async_completion.py:90`：固定提交线程。
- `G:\Demo\chatgpt2api\services\thread_pool.py:187`：同步图片任务入口。
- `G:\Demo\chatgpt2api\services\image_transport.py:11`：同步传输Session。
- `G:\Demo\chatgpt2api\services\openai_backend_api.py:2568`：图片正文收集和校验。
- `G:\Demo\chatgpt2api\services\protocol\conversation.py:366`：原始bytes保存与响应格式。
- `G:\Demo\chatgpt2api\services\log_service.py:916`、`G:\Demo\chatgpt2api\services\storage\batch_writer.py:35`：日志确认和既有批量写入。

源码哈希、Aether HEAD与原型数据已保存至 `G:\Demo\chatgpt2api\output\aether_image_performance_evidence.json`。这里只把源码用于解释结构，不把本地检出代码等同于Aether实际运行的部署版本。

## 2026-10-31 实施记录

已将异步网络等待、独立图片 I/O 运行时、SSE 传输摘要和按字节限制正文接入当前协议链路，同时增加原对话优先补查、受共享预算约束的流超时切号恢复。当前行为、兼容范围、本地对照及复测方法见 `docs/image-runtime-performance.md`；上文的对比保留为实施前的结构评估。
