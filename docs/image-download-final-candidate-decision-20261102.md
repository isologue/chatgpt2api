# 最后一轮下载候选验证与收尾决策（2026-11-02）

## 决策

最后一个候选没有出现可重复改善，停止本轮继续试探，保留当前已发布的较优实现并合入 `main`：

- 独立常驻下载进程、认证回环 IPC、全局 FIFO 下载名额。
- 正文批量异步消费，完成正文后及时交还下载名额。
- 保留完整 certifi 信任集合的 CA 哈希目录，以及已有解析、调度和诊断优化。
- 保留 **4 个下载进程 / 全局下载并发 64 / IPC 正文帧上限 256 KiB**。全局 64 不是每个进程 64。
- **不合入**本次 1 MiB 正文帧候选，不提高生产下载并发。

这表示在本轮已完成的对照中选择较优版本，不表示已经解决全部高并发长尾，也不证明服务器带宽已饱和。完整生图成功率不能由本轮下载微基准推断。

## 最后一个候选

只把下载 IPC 正文帧上限由 256 KiB 增大到 1 MiB，尝试减少帧数、读写唤醒与调度次数。候选通过临时只读挂载进入隔离容器，未修改产品源码、生产容器或线上设置。

有效测试条件：

- 使用线上当前镜像 `sha256:acdb6ea107a8a0b483e9766c938858a1ef66be1b96179860fc312f6d1a1d5912`，临时容器 `network=none`，不访问外网。
- 两组均为 4 个 worker、全局并发 128。这里的 128 仅用于隔离压力实验，生产仍为 64。
- 固定每个源响应为 12 个已缓冲的 256 KiB 块，共 3 MiB；两组源块大小、总正文、任务数不变，仅 IPC 帧上限不同。候选包含真实合并复制开销。
- 每轮 512 个响应，1.5 GiB 正文，6 轮顺序为 `256 / 1024 / 1024 / 256 / 256 / 1024 KiB`。
- 各轮独立初始化并预热下载池，正式吞吐不含导入和预热；轮间留间隔，每轮前确认生产空闲。
- **3072/3072 成功**，每个响应消费长度、worker 正文统计和传输正文统计一致，声明长度检查通过。总正文 9 GiB，仅内存流转。
- 新建图片 0、图片副本落盘 0、额外图片解码 0；没有重跑完整 500 并发生图。

| 指标 | 原版 256 KiB | 候选 1 MiB |
| --- | ---: | ---: |
| 三轮吞吐 MiB/s | 368.059 / 313.838 / 354.760 | 330.235 / 284.588 / 222.149 |
| 吞吐中位数 MiB/s | 354.760 | 284.588 |
| 各轮含排队 P95 的中位数 | 4.225 秒 | 5.292 秒 |
| 每轮主进程与 worker 合计 CPU 时间中位数 | 6.629 秒 | 8.896 秒 |
| 各轮 worker 合计 RSS 峰值的中位数 | 142.898 MiB | 274.492 MiB |
| 各轮主进程 RSS 峰值的中位数 | 442.672 MiB | 458.684 MiB |
| 各轮 event-loop lag P95 的中位数 | 22.580 ms | 16.263 ms |
| 六轮中各组最高 event-loop lag | 86.954 ms | 74.013 ms |

候选吞吐中位数下降 **19.8%**，含排队 P95、CPU 和 worker 内存开销均恶化。虽然部分 lag 指标下降，但不足以构成整体收益。测试存在机器调度波动，不把该百分比当作所有线上负载的固定差距；它足以否定本次候选值得继续发布的假设，因此不再为它追加真实图片下载测试。

### 排除的预试验

最初一组模拟源错误地把源块大小也绑定到 IPC 帧上限，同时改变了源迭代次数和传输帧大小，存在混杂变量。该组结果单独保存，**不纳入上表和决策**。随后固定源块为 256 KiB，重新完整运行六轮。

## 回归与构建

- 图片异步运行时、curl、CA、进程池、下载名额、正文消费、诊断、切号、超时预算、配置、响应格式、账号扣分、监控和脚本解析相关测试：**303 passed, 1 skipped**。
- `docker compose -f .\docker-compose.warp.yml build` 成功；存在既有 `RedundantTargetPlatform` 提示，无构建错误。
- `git diff --check` 通过。仅有既有 CRLF 提示，不批量转换文件换行。
- 扫描准备提交文件的私钥、常见 API key、签名 URL、凭据 URL 和敏感赋值模式，未发现新增真实凭据；临时控制器、服务器访问资料、原始实验数据均不进入 Git。

## 收尾运行态核验

- 生产容器 running，OOM=false，restart_count=0，镜像 ID 未改变。
- 将 16 个相关后端源码/构建脚本与线上容器逐一做归一化换行后的 SHA256 对比，全部一致。
- Compose SHA256 仍为 `a6ee21a42bc8762457468e8421da2dee62c1e5da8a1d601a38df670a77fda45d`，未修改线上 Compose。
- 生产配置仍为 4/64，SSE 80 秒、补查 10 秒、总预算 120 秒、下载 15 秒、下载重试 1。
- 执行器 inflight/queued=0，下载 active/waiting=0，没有遗留 probe 容器。生产重启后未生成新图，协作运行时尚未惰性初始化，不能把空 worker 列表误判成进程池没有生效；隔离六轮均实际运行 4 个 worker。
- 本轮不重新推送 Docker 镜像、不重启生产容器；保留已经发布的产品实现，仅完成代码归档合并与远程 Git 推送。

## 本地证据（output 已忽略，不提交凭据与原始数据）

- 有效六轮：`output/download_final_frame_fixed_source_20261102.json`
- 有效统计：`output/download_final_frame_fixed_source_summary_20261102.json`
- 固定源 fixture：`output/_download_final_frame_fixed_source_worker.py`
- 控制器：`output/_download_final_frame_fixed_source_control.py`
- 隔离执行：`output/_download_final_frame_synthetic_remote.py`
- 候选 IPC：`output/_download_final_frame_ipc.py`
- 排除的预试验：`output/download_final_frame_synthetic_20261102.json`
- 收尾核验：`output/download_final_frame_final_health_20261102.json`

此前真实同源下载并发 64/128 的六轮证据及保持 64 的理由见 `docs/image-download-bulk-concurrency-investigation-20261102.md`。
