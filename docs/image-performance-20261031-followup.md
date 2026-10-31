# 2026-10-31 图片链路后续优化

## 线上依据

13:20:05–13:21:27（Asia/Shanghai），服务器本机发布端口，200 个同步请求单波发起，n=1、response_format=url。198 成功、2 个 no_image_generated，198 张成功图片全部完整解码。无 429/504/空体 502。该轮发生在本次后续优化之前。

原始数据：`output/loadtest_detailed_20261031T131946/probe.jsonl`。

| 指标 | 08:55 前一轮 | 13:20 本轮 |
| --- | ---: | ---: |
| 全请求 P95 | 61.355s | 61.178s |
| API loop 延迟峰值 | 1258.601ms | 801.418ms |
| 图片 I/O loop 延迟峰值 | 4089.678ms | 3066.996ms |
| 单次 PoW 阶段 P95 | 3334.628ms | 3265.985ms |
| PoW 子进程实际计算墙钟 P95 | 2.240ms | 1.937ms |
| PoW 工作线程完成后恢复等待 P95 | 3024.913ms | 3113.101ms |
| 图片下载阶段 P95 | 8963.431ms | 12725.914ms |
| 结果解析阶段 P95 | 5451.354ms | 2389.538ms |

08:55 某请求有两次 PoW：表中阶段单次分布来自日志诊断步骤，子阶段分布来自各请求内的累计值；本轮均各一次，可直接用于单次 P95。历史 `request_stage_ms` 则是每请求聚合，不一概视为单次计时。

当前证据说明小型 JSON 和元数据路径的线程切换已减少，但 PoW 完成后恢复仍慢。图片正文耗时包含事件循环调度，不能仅据该耗时判定带宽瓶颈。`generation_work`、上游汇总、SSE、下载等指标有包含关系，不能直接相加。

## 实现

- 保留 PoW 进程池，图片协作路径直接 await 进程 Future，取消和总预算可停止等待，不再为每次进程计算占一个 image-part 线程。
- 首页 HTML 解析与验证脚本纯计算也进入同一 CPU 进程池，减轻大量 Python 解析线程争抢主进程 GIL。只改变执行位置，仍用原解析算法，首页预热不跳过、不跨账号缓存。
- 极短的 requirements token 构造直接执行；PoW 求解继续隔离在子进程。
- 图片正文合并已就绪的小块，避免每个小块都往返协议 greenlet；不等待凑满指定长度，SSE 默认逐块处理不变。已缓冲正文每读 256KiB 主动让出，保留正文上限和取消检查。
- 新增固定数值诊断 `cpu_resume`，分开记录 IPC 回传与协程恢复等待；前端性能项使用对应中文说明。
- 同步兼容路径、账号评分、切号策略、缓存有效期和超时配置沿用原行为。未重新实现已暂缓的紧凑账号调度索引。

## 可重放的本地对照

命令在独立进程执行，避免前一个模式留下的线程影响基线：

```powershell
uv run python scripts/benchmark_image_runtime_contention.py --mode threaded --output output/contention_threaded_control.json
uv run python scripts/benchmark_image_runtime_contention.py --mode native --output output/contention_native_control.json
```

两组使用相同当前代码、相同正文读取逻辑；threaded 在工作线程解析 HTML 并阻塞等待进程结果，native 使用新 CPU 路径。每组 200 请求、每请求约 267KB 合成 HTML、2MiB 合成图片正文、4 个预热 CPU worker、小型元数据查询。使用真实回环 HTTP，检查所有返回正文 CRC32，不访问上游、不使用凭据、不落盘图片。

| 指标 | threaded 对照 | native 新路径 |
| --- | ---: | ---: |
| 请求数 | 200 | 200 |
| P95 | 7901.796ms | 3342.531ms |
| API loop 延迟峰值 | 187.295ms | 37.505ms |
| 图片 I/O loop 延迟峰值 | 585.642ms | 313.667ms |
| 线程峰值（含主线程） | 183 | 31 |
| 服务端在途连接峰值 | 200 | 200 |

这是本机单次合成负载对照，说明减少主进程计算争用和等待线程有效，不能直接外推成线上生图耗时提升比例。网络、代理、TLS、真实生成、图片完整解码和持久化日志不在此基准范围。

## 验证

`test/test_image_runtime_contention.py` 验证真实进程执行、无 image-part 等待线程、业务异常、超时和取消、进程池故障回退只执行一次且离开循环、进程结果未就绪时总预算仍释放图片槽位、真实 PoW 结果验证、同步兼容行为、原 HTML 解析结果一致性，以及正文合并的字节完整性、不等待凑块、错误/超限传播、缓冲流主动让出和取消。

部署后以相同 200 并发配置复测，重点对比成功率、两个 loop 的时间线与恢复、单次 PoW 及其 cpu_queue/cpu_work/cpu_return/cpu_resume、下载耗时和完整解码。当前没有本次代码的线上性能结论。

验证结果：扩展相关回归先通过 197 项；补充总预算保护与真实 PoW 验证后，82 项核心回归（含新增的总预算测试）再次全部通过。中文性能项标签检查、Python 编译检查和 git diff --check 通过。测试仅有已知 SQLAlchemy 弃用、Windows curl selector 适配及旧同步下载 chunk_size 提示。
