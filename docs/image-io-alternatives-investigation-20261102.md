# I/O替代方案隔离实验（2026-11-02）

生产镜像Python、独立本机HTTP/2 TLS测试服务、500请求、每请求4个新TLS连接、每次64KiB响应。开启CA验证，每请求独立Session。CPU实验串行，3轮交替，不改生产。新连接通过FRESH_CONNECT与FORBID_REUSE保证。所有有效轮次0错误。

## 多I/O循环线程：拒绝

native单循环墙钟4.161/2.812/2.661秒，native四循环8.416/7.168/7.279秒；cffi单循环3.044/2.534/2.631秒，cffi四循环7.537/7.803/7.506秒。多线程还增加CPU开销，不能用分片替代当前循环。

旧文件tls_shards_experiment_20261101.json因服务端缺h2导致请求全失败，完全无效；最终validated文件修正服务端就绪、连接语义，并在错误数不为0时断言失败。h2依赖仅放在output隔离目录。

## 同步线程池：拒绝

同一测试服务和请求量，普通32/64/128线程池约4.18～4.30秒、CPU约8.2～8.6秒；当前异步单循环热轮约2.45～2.49秒、CPU约2.43～2.48秒。它不能改善当前路径。

## 匿名首页压缩格式：没有收益依据

线上容器匿名首页24次交替，12次br、12次gzip，全部HTTP200、6个script，正文约446KB。br的请求CPU/解析CPU/总CPU中位数14.884/20.136/34.491ms；gzip为16.633/22.278/38.787ms。墙钟中位数140.285/164.606ms。这组结果不支持更改Accept-Encoding；正文轻微变化，不能将它当严格压缩算法基准。

## 证据与边界

- output/_benchmark_tls_shards.py、output/tls_shards_validated_20261101.json
- output/_benchmark_tls_threads.py、output/tls_threads_comparison_20261101.json
- output/_probe_bootstrap_encoding_repeat.py、output/bootstrap_encoding_repeat_20261101.json

本地合成请求不能代替上游生图链路。上述方案均未改生产代码，也不构成端到端优化成果。
