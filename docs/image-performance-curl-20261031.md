# 2026-10-31 HTTP 传输兜底调度与首页解码优化

## 问题和边界

本轮根据北京时间 20:07 的线上 200 并发采样实施。图片 I/O loop 的 2.7715 秒峰值附近，Python 栈主要停在 curl_cffi 的 socket_action，线程同期有约 2.48 秒 CPU。未获得可靠 native 栈，因此这只能定位底层传输热点，不能认定其内部一定是 TLS、证书解析、解压或 GIL 竞争。

保留每个账号尝试独立的 Session、easy/multi handle、连接缓存、Cookie、headers、代理和指纹。没有采用共享 multi/跨账号连接池，避免引入 HTTP/2 连接复用、指纹与生命周期相互影响的风险。继续保留当前内存选号索引及已有切号/评分规则。

## 最终修改

1. services/image_curl.py：基于锁定版本 curl_cffi 0.15.0 的 AsyncCurl 适配，合并原先每个账号各自每100毫秒唤醒的兜底轮询。每个事件循环只有一个公共扫描任务，各账号的原生socket/timer回调仍各自执行。
2. 正常传输不额外重复驱动。仅当传输活跃、且连续1秒没有执行过socket_action时才补偿一次。首次通知还未发生时允许下一次扫描补偿，避免丢失启动通知后先等满1秒。每16个条目或约2毫秒让出一次；这里是让出调度的批次，不是并发数量限制，不能中断一个已经进入C层的长调用。
3. 补偿使用process_data，同时处理完成/错误队列；最后一个会话关闭时注销并回收公共任务，关闭一个会话不关闭其他账号的multi。
4. 大于等于50毫秒的socket_action记入固定数值快照image_curl_callback_slow，可通过监控接口phases读取；不保存URL、请求头、认证、正文或调用参数。它只统计长回调，不是全部curl调用分布，当前未新增前端展示行。
5. utils/pow.py、services/openai_backend_api.py：首页成功响应的bytes和已确定编码直接送入原CPU进程池，解码与HTML解析一起执行，避免先在图片loop计算response.text。解码保留curl Response.text原有的errors=replace及无效编码回退规则；同步路径仍直接解析response.text。

正常curl定时器、请求总预算、阶段超时及取消机制仍独立生效；1秒是异常情况下的无回调兜底宽限期，不是生图超时、补查超时、重试等待或新的并发配置。若正常通知全部丢失，底层curl的补偿处理会受到该宽限期影响，业务包装器仍用原预算中断等待，相关故障测试覆盖此边界。

该适配使用AsyncCurl的内部生命周期，后续升级curl_cffi时必须重跑相应回归，不能假设所有未来版本兼容。

## 对照与验证

全部对照仅本地运行；Linux使用现有app镜像启动独立、自动删除的临时容器，网络设为none，客户端和合成服务均在容器回环内。没有访问线上、上游或修改现有容器、配置及数据库。TLS证书放临时目录，结束自动删除，不写系统信任库。基准每轮都核对返回正文完整性。

TLS基准200并发，每请求读取约260KB HTML、两次元数据和512KiB正文；逐账号核对Authorization、User-Agent和Cookie隔离。5轮交换执行顺序，每种模式合计1000请求、2000次账号隔离核对，以下是各轮指标的中位数：

| 指标 | 原兜底逻辑 | 最终逻辑 |
| --- | ---: | ---: |
| 请求P95 | 1567.853ms | 1510.483ms |
| 整波墙钟 | 1585.222ms | 1532.062ms |
| 进程CPU | 1104.579ms | 1092.392ms |
| curl超时驱动回调（含正常timer） | 4020 | 1600 |
| 所有curl驱动回调 | 9220 | 6800 |
| 同一loop延迟峰值 | 56.299ms | 60.300ms |

证据：output/image_curl_linux_tls_comparison.json。P95约降低3.7%，超时驱动回调约减少60%；CPU仅有小幅差异，loop峰值没有改善，不能把回调数量减少等同于整体CPU或端到端同幅提速。

混合HTTP/HTML/PoW/2MiB正文基准，每种模式3轮、各轮200请求，交替执行，每轮独立进程和CPU池。对照为原兜底+loop解码，最终为新兜底+worker解码；这是组合对照，不能据此区分两项改动分别贡献多少。

| 指标 | 原路径 | 最终路径 |
| --- | ---: | ---: |
| 请求P95 | 3981.750ms | 3866.262ms |
| 整波墙钟 | 4.010s | 3.907s |
| 图片loop峰值 | 286.048ms | 263.932ms |
| API loop峰值 | 164.967ms | 216.302ms |

证据：output/image_curl_mixed_grace1_linux_comparison.json。合成混合负载P95约降低2.9%，API峰值仍有波动；不能宣称已解决线上首页/建联长停顿或取得线上生图提速。

早期仍按100毫秒补偿的版本只减少回调、未显示稳定收益，未采用该阈值。Windows初步TLS对照也显示平台和轮次波动，因此以最后Linux对照为本轮有限证据，部署后仍需线上同口径验证。

## 回归结果和测试条件

最终190项相关回归通过，包含新增12项curl/首页解析测试，以及真实200任务HTTP、同步/异步接口、生成/编辑、取消、SSE/下载流、完整正文、超时补查、失败切号、评分、完成通知、真实CPU进程执行和预算释放。

新增覆盖：独立Cookie与认证；关闭一个账号时其他请求继续；idle/近期已驱动会话不重复补偿；多个会话关闭时公共任务回收；注册前关闭；最后一个关闭后新建会话；故意丢失全部正常通知时兜底返回curl超时；业务50毫秒总预算不等待1秒兜底；UTF-8、中文编码、无效编码/字节及空正文一致性；bootstrap提交进程前不读取Response.text；真实spawn进程解析bytes。

最初扩展收集还运行了test_v1_images_generations.py和test_v1_images_edits.py共4个手工集成用例，分别因localhost:8000未运行或assets中的图片素材缺失失败。这4项不是最终190项通过集的一部分；没有为了让手工测试通过而启动真实上游生成或伪造素材。早期故障测试另抓到首次通知丢失后延迟启动的问题，已修正并在最终回归中通过。

只剩原有SQLAlchemy弃用、Windows curl selector适配和同步下载chunk_size提示。Python编译与本轮文件的空白检查通过。

## 重放

本机TLS基准需要cryptography生成临时证书；也可以用--cert和--key传入专用本机测试证书，避免添加运行时依赖。--http可运行纯HTTP对照。

```powershell
uv run python scripts/benchmark_image_curl.py --requests 200 --repeats 5 --output output/image_curl_comparison.json
uv run python scripts/benchmark_image_runtime_contention.py --curl-watchdog legacy --decode loop --output output/curl_legacy.json
uv run python scripts/benchmark_image_runtime_contention.py --curl-watchdog tuned --decode worker --output output/curl_tuned.json
```

本轮仅修改代码并验证，未构建/部署服务、未推送、未追加线上生图压测。部署后建议按同一波200并发重点检查成功率、首页预热、curl长回调、两条loop时间线、cpu_resume和图片完整性。
