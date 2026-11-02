# 首页脚本解析开销研究（2026-11-01）

## 问题与边界

上一轮最差波次中，PoW阶段 P95 约3.5秒，但 CPU 计算 P95 只有5.33毫秒，进程池排队 P95 为3415.58毫秒。首页解析同样使用该池。此次仅减少首页解析对普通 HTML 元素的无用属性解码，不改请求、Session、Cookie、账号身份隔离和超时。

## 变更

`utils/pow.py` 的 ScriptSrcParser 对普通标签继续使用 HTMLParser 的完整标签边界检查，跳过属性构造；script、原始文本、RCDATA、plaintext、noscript 保留父类完整解析。没有共享或缓存跨账号正文。

不同 Python 版本对 plaintext / noscript 的状态处理不同。Linux差分测试首先抓到 plaintext 回归；修正后 Windows Python 3.14 专项105项、Linux Python 3.13专项110项通过，包括分段输入、畸形标签、实体、注释和scripting开关。

## 本地基准

合成2.945MB HTML，16进程、500请求在1秒发出，首页解析后接PoW。预热后3轮交替运行。Linux解析累计CPU开销从33.2～34.8秒降至19.5～19.6秒；整段墙钟5.61～5.71秒降至5.41～5.49秒。此基准仍受IPC与队列影响，不能将CPU收益直接当作线上请求提速。Windows收益更大，不能代替生产Linux验证。

证据：`output/parser_windows_regression.txt`、`output/parser_linux_regression.txt`、`output/parser_mixed_linux.json`、`output/parser_mixed_windows.json`。

## 线上验证

已部署 parser 候选 `sha256:f25e8424f6a2cda1df9b052fa7ba83f1a4bded619459072a0d6acc4efecf1204`，在同一容器执行两轮固定500请求、1秒发出的测试。请求从服务器本机发布端口进入，不包含公网客户端/CDN耗时，不代表500张图同时进入上游。没有额外下载验图。下载并发64、回调时间片2ms、SSE80秒、补查10秒、总预算120秒未改。

| 2026-11-01 波次 | 成功 | 失败 | P50 / P95 |
| --- | --- | --- | --- |
| 23:26 刚部署 | 491 | 9 no_image_generated | 87.621 / 105.445秒 |
| 23:31 同容器重复 | 488 | 10 no_image_generated、1 image_result_uncertain、1 image_timeout | 72.646 / 88.417秒 |

两轮500条请求日志和timing均收齐，空体502均为0。第一轮 PoW P95 67.835ms，第二轮69.848ms；第一轮首页解析排队/计算 P95 252.803/39.898ms，第二轮约64.8/30.8ms。下载等待 P95 27.878/18.148秒，下载正文约7.5秒。首页HTTP P95 18.356/13.384秒，curl回调最长等待6.111/4.525秒。API循环峰值1242.318/936.765ms，图片循环1736.559/1484.762ms；app CPU峰值549.35/514.42%。

这些波次不是相邻原版/候选交替对照，不能把第二轮变快归功于 parser。当前仅确认局部解析CPU收益，尚未证实端到端稳定改善。

两个504均未切号：生成前 bootstrap + requirements + submit 已耗约27～29秒；之后 SSE约77秒硬超时，补查再耗约8秒，到达120秒总预算边界。它们不是PoW计算或下载队列导致。另有117秒成功请求在第一账号补查失败后切号成功，说明切号机制工作，但总预算是否允许重试取决于前面实际耗时。

证据：`output/loadtest_detailed_20261101T232556/`、`output/loadtest_detailed_20261101T233052/`、`output/parser_longtail_diagnostics_20261101.json`。

## 回退

原始源码保存于 `output/parser_baseline_pow.py`。前一候选（元数据BytesIO缓冲）已拒绝，源码与本地/线上/Hub均先回退至16dbd8c8a356。它的专项测试移到 `output/rejected_metadata_sink_test.py`，作为实验记录保留，不作为现行实现的测试。
