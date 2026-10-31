# 2026-10-31 完成通知与已缓冲正文读取优化

## 线上依据

最近一次已完成的线上压测为北京时间 16:16:57–16:18:16，200 个同步请求单波，n=1、response_format=url，走服务器本机发布端口。198 成功，2 个 no_image_generated；无 429、504 或空体 502。198 张返回图片全部完整下载并解码正常。本轮代码修改发生在该次压测之后。

证据目录：`output/loadtest_detailed_20261031T161645/`，包括 `report.md`、`summary_runtime.json`、`contention_analysis.json` 和 `probe.jsonl`。

- 全请求 P50 52.587s、P95 55.311s、最大 78.462s。
- PoW 阶段 P95 982ms，子进程实际计算 P95 2.889ms，CPU 结果回到主进程后恢复等待 P95 871.684ms。
- 首页解析 CPU 恢复等待 P95 4914.762ms，实际子进程计算 P95 约 55ms。
- 下载阶段 P95 13.923s，正文读取 P95 10.989s，下载工作结束后恢复等待 P95 2.023s。
- API loop 峰值约 607ms，图片 I/O loop 峰值约 2110ms。16:17:45 附近还有一次约 640ms 的 other 同步片段，具体函数尚未定位。

上述阶段有包含关系，不能将各项 P95 相加。正文下载阶段包含调度等待，不能只凭该耗时认定网络带宽不足。

## 本轮实现

1. 后台提交执行器后立即连接工作 Future 的结果，省去业务事件循环先接收提交结果、恢复后才登记工作结果的中间往返。CPU 进程任务和图片阻塞短任务复用此路径；提交仍离开事件循环，原执行器容量不变。
2. 完成通知待交付队列改用 deque，按原先每次最多 64 项取出；积压时不再复制整个剩余列表，并继续分批让出事件循环。
3. HTTP 传输已结束且队列有缓冲数据时，直接读缓冲正文，减少逐块创建和取消预算计时器。传输尚未结束时保留计时器，因为流结束或错误标记可能继续等待 aclose。读前后仍检查总预算/取消，保留正文上限及每 256KiB 主动让出。
4. 提交前、提交中、工作运行中取消都接管工作 Future；选号、落盘等需要资源结果的操作继续 shield 等待收尾。业务 RuntimeError 原样传播，不能误判进程池损坏后重跑。

## 本地验证与性能边界

本地对照命令，无上游访问、无凭据、无图片落盘：

```powershell
uv run python scripts/benchmark_image_completion.py --output output/completion_buffered_comparison.json
uv run python scripts/benchmark_image_runtime_contention.py --handoff staged --output output/handoff_staged.json
uv run python scripts/benchmark_image_runtime_contention.py --handoff direct --output output/handoff_direct.json
```

局部基准默认 200 并发、5 次交换执行顺序，以下为各次结果的中位数：

| 场景 | 对照 | 本轮 |
| --- | ---: | ---: |
| 2000 个短任务完成通知数 | 4000 | 2000 |
| 2000 个短任务墙钟 | 41.488ms | 43.534ms |
| 200 个已缓冲 512KiB 正文的读取墙钟 | 127.338ms | 70.669ms |
| 上述正文预算计时器注册次数 | 25800 | 0 |

正文基准使用 curl_cffi 原生异步正文迭代器及已结束的传输任务，逐块校验内容、字节数和 EOF。只测内存缓冲的读取，不能外推为线上下载时间缩短约 45%。短任务通知数确实减半，但墙钟没有改善，不能声称该项已经提速。

同机 200 请求混合 HTTP/HTML/PoW/2MiB 正文基准，staged 单次 P95 3256.355ms，direct 单次 P95 3419.920ms；API loop 峰值分别 22.057ms/31.481ms，图片 loop 峰值 284.506ms/350.825ms。该对照只改变完成通知方式，未显示整体收益；本轮不能宣称端到端性能提升。

相关回归 208 项通过，覆盖完成通知取消竞态、2000 混合结果、账号槽位回收、CPU 业务异常不重跑、真实进程执行、正文完整性/上限/公平性、流结束及错误清理超时、下载失败切号、补查预算、日志指标和同步/异步响应等。仅有已有 SQLAlchemy 弃用、Windows curl selector 适配和旧同步下载 chunk_size 提示。

本轮未构建部署、未重启本地服务、未发起新一轮线上压测。部署后仍需按相同 200 并发口径检查成功率、两条 loop 时间线、cpu_resume、offload_resume、下载完整性，并定位上述尚未归因的长同步片段。
