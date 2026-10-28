"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Activity, HardDrive, LoaderCircle, MemoryStick, RefreshCw, Server, Users } from "lucide-react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { fetchMonitoring, type MonitoringSnapshot } from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth-guard";
import { RefreshFailuresDialog, type RefreshFailureSelection } from "./refresh-failures-dialog";

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatDuration(milliseconds: number | null | undefined) {
  return typeof milliseconds === "number" && Number.isFinite(milliseconds)
    ? `${(milliseconds / 1000).toFixed(2)} 秒` : "—";
}

function formatTime(timestamp: number) {
  if (!timestamp) return "-";
  return new Date(timestamp * 1000).toLocaleString();
}

function Metric({ label, value, hint, tone = "text-stone-950" }: { label: string; value: string | number; hint?: string; tone?: string }) {
  return (
    <div className="rounded-xl border border-stone-200 bg-white px-4 py-3">
      <div className="text-xs text-stone-500">{label}</div>
      <div className={`mt-2 text-2xl font-semibold tracking-tight ${tone}`}>{value}</div>
      {hint ? <div className="mt-1 text-xs text-stone-400">{hint}</div> : null}
    </div>
  );
}

function localDay(offset = 0, base?: string) {
  const day = base ? new Date(`${base}T12:00:00`) : new Date();
  day.setDate(day.getDate() + offset);
  return `${day.getFullYear()}-${String(day.getMonth() + 1).padStart(2, "0")}-${String(day.getDate()).padStart(2, "0")}`;
}

function MonitoringContent() {
  const [snapshot, setSnapshot] = useState<MonitoringSnapshot | null>(null);
  const [failureSelection, setFailureSelection] = useState<RefreshFailureSelection | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState("");

  const [range, setRange] = useState({ start: "", end: "", preset: "今天" });
  const [draftStart, setDraftStart] = useState(localDay());
  const [draftEnd, setDraftEnd] = useState(localDay());
  const requestId = useRef(0);
  const pending = useRef(false);

  const load = useCallback(async (silent = false) => {
    if (silent && pending.current) return;
    const id = ++requestId.current;
    pending.current = true;
    if (silent) setIsRefreshing(true);
    else setIsLoading(true);
    try {
      const next = await fetchMonitoring(range.start, range.end);
      if (id !== requestId.current) return;
      setSnapshot(next);
      setError("");
    } catch (cause) {
      if (id !== requestId.current) return;
      const message = cause instanceof Error ? cause.message : "加载监控数据失败";
      setError(message);
      if (!silent) toast.error(message);
    } finally {
      if (id === requestId.current) {
        pending.current = false;
        setIsRefreshing(false);
        setIsLoading(false);
      }
    }
  }, [range.start, range.end]);

  useEffect(() => {
    void load();
    return () => { requestId.current += 1; pending.current = false; };
  }, [load]);

  useEffect(() => {
    const interval = Math.max(1, Number(snapshot?.refresh_interval_secs || 3)) * 1000;
    const timer = window.setInterval(() => {
      if (!document.hidden) void load(true);
    }, interval);
    return () => window.clearInterval(timer);
  }, [load, snapshot?.refresh_interval_secs]);

  useEffect(() => {
    if (!range.start && snapshot) {
      setDraftStart(snapshot.images.start_date);
      setDraftEnd(snapshot.images.end_date);
    }
  }, [range.start, snapshot?.images.start_date, snapshot?.images.end_date]);

  const selectPreset = (label: string, days: number, yesterday = false) => {
    const start = localDay(yesterday ? -1 : -(days - 1), snapshot?.server_date);
    const end = localDay(yesterday ? -1 : 0, snapshot?.server_date);
    setDraftStart(start);
    setDraftEnd(end);
    setRange({ start, end, preset: label });
  };

  if (isLoading && !snapshot) {
    return <div className="flex min-h-[40vh] items-center justify-center"><LoaderCircle className="size-5 animate-spin text-stone-400" /></div>;
  }

  const data = snapshot;
  if (!data) {
    return <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error || "暂无监控数据"}</div>;
  }

  const memory = data.system.memory;
  const disk = data.system.disk;
  const executor = data.image_executor;
  const accounts = data.accounts;
  const images = data.images;
  const refresh = data.account_refresh;
  const refreshStatus = refresh ? ({ idle: "尚未开始", running: "刷新中", completed: "已完成",
    completed_with_errors: "已完成（有失败）", failed: "刷新中断" }[refresh.status]) : "暂无刷新监控";
  const refreshPhase = refresh ? ({ idle: "等待启动", preparing: "准备账号列表", refresh: "更新账号信息",
    keepalive: "Token 保活", waiting: "等待下一轮" }[refresh.phase]) : "";
  const refreshPercent = refresh?.refresh.total ? Math.min(100, refresh.refresh.completed / refresh.refresh.total * 100) : 0;

  return (
    <section className="space-y-5">
      {Object.entries(data.snapshot_state ?? {}).some(([, state]) => state.stale) && (
        <div role="status" className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">
          部分指标正在刷新或采集失败，当前展示最近成功快照：
          {Object.entries(data.snapshot_state ?? {}).filter(([, state]) => state.stale).map(([key, state]) =>
            `${key === "runtime" ? "运行指标" : "图片统计"}距今 ${(state.age_ms / 1000).toFixed(1)} 秒${state.refresh_failed ? "（刷新失败）" : ""}`
          ).join("；")}
        </div>
      )}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div className="space-y-1">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-stone-500">Monitoring</div>
          <h1 className="text-2xl font-semibold tracking-tight">运行监控</h1>
          <p className="text-sm text-stone-500">更新时间：{formatTime(data.timestamp)} · 自动刷新：{data.refresh_interval_secs} 秒</p>
        </div>
        <Button variant="outline" className="h-9 rounded-xl border-stone-200 bg-white px-3 text-stone-700" onClick={() => void load(true)} disabled={isRefreshing}>
          <RefreshCw className={`size-4 ${isRefreshing ? "animate-spin" : ""}`} />
          刷新
        </Button>
      </div>

      {error ? <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">本次刷新失败，当前仍展示上一次数据：{error}</div> : null}

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric label="已接纳未结束" value={executor.inflight} hint={`图片执行线程上限 ${executor.workers}，包含账号等待`} tone="text-sky-700" />
        <Metric label="可用执行槽位" value={executor.available_slots} hint={`总容量 ${executor.capacity}`} tone="text-emerald-700" />
        <Metric label="剩余排队槽位" value={executor.remaining_queue_slots} hint={`已排队 ${executor.queued} / ${executor.queue_size}`} tone="text-amber-700" />
        <Metric label="执行池状态" value={executor.full ? "已满" : "有余量"} hint={executor.full ? "新请求将返回 429" : "可以继续接收任务"} tone={executor.full ? "text-rose-700" : "text-emerald-700"} />
      </div>

      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="space-y-4 p-5">
          <div>
            <h2 className="font-semibold">生图阶段与校验</h2>
            <p className="mt-1 text-xs leading-5 text-stone-500">阶段可能包含其他阶段，数量不能相加。上游调用中包含提交和轮询，不代表上游服务器已开始绘制。耗时为本进程最近最多 256 次阶段完成样本的 P95。</p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="选号与校验中" value={data.phases?.account_lookup?.active ?? 0} />
            <Metric label="上游调用中" value={data.phases?.upstream_generation?.active ?? 0} tone="text-sky-700" />
            <Metric label="图片下载中" value={data.phases?.image_download?.active ?? 0} />
            <Metric label="图片保存中" value={data.phases?.image_save?.active ?? 0} />
            <Metric label="正在远程校验" value={data.account_verification?.active ?? 0} hint={`等待 ${data.account_verification?.waiting ?? 0}，后台待完成 ${data.account_verification?.background_pending ?? 0}`} />
            <Metric label="校验缓存命中率" value={`${(data.account_verification?.cache_hit_rate ?? 0).toFixed(1)}%`} hint="进程累计，包含允许后台刷新的缓存命中" tone="text-emerald-700" />
            <Metric label="日志确认等待" value={data.phases?.log_confirmation?.active ?? 0} hint={`日志队列待确认 ${data.persistence?.logs.pending ?? 0}`} />
            <Metric label="结果处理队列" value={data.completion_executor?.queued ?? 0} hint={`正在处理 ${data.completion_executor?.active ?? 0}`} />
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead><tr className="border-b border-stone-200 text-stone-500"><th className="py-2 font-medium">阶段</th><th className="py-2 font-medium">进行中</th><th className="py-2 font-medium">近期 P95</th><th className="py-2 font-medium">累计完成</th></tr></thead>
              <tbody>
                {([
                  ["request_auth", "接入鉴权"], ["request_filter", "接入内容检查"],
                  ["event_loop_lag", "事件循环调度延迟"],
                  ["account_lookup", "账号获取与校验"], ["account_verification_queue", "校验并发 / 速率等待"],
                  ["account_verification", "远程校验"], ["upstream_generation", "上游提交与轮询"],
                  ["image_download", "图片下载"], ["image_save", "图片保存"],
                  ["log_prepare", "日志详情构造"], ["log_confirmation", "日志持久化确认"],
                  ["log_write", "日志批次写入"], ["response_serialization", "响应编码与调度"],
                ] as const).map(([key, label]) => {
                  const phase = data.phases?.[key];
                  return <tr key={key} className="border-b border-stone-100 last:border-0"><td className="py-2 text-stone-700">{label}</td><td className="py-2">{phase?.active ?? 0}</td><td className="py-2">{((phase?.p95_ms ?? 0) / 1000).toFixed(3)} 秒</td><td className="py-2">{phase?.completed ?? 0}</td></tr>;
                })}
              </tbody>
            </table>
          </div>
          <p className="text-xs leading-5 text-stone-500">
            校验累计失败 {data.account_verification?.failed ?? 0} 次，后台刷新容量不足跳过 {data.account_verification?.background_rejected ?? 0} 次。
            日志待确认 {data.persistence?.logs.pending ?? 0} 条，最近一批最长排队 {((data.persistence?.logs.last_queue_wait_ms ?? 0) / 1000).toFixed(3)} 秒，实际写入 {((data.persistence?.logs.last_write_ms ?? 0) / 1000).toFixed(3)} 秒。
          </p>
          <details className="text-xs text-stone-600">
            <summary className="cursor-pointer font-medium">查看锁等待与日志慢批次</summary>
            <p className="mt-2 leading-5">
              当前写入 {data.persistence?.logs.active_items ?? 0} 条，已执行 {((data.persistence?.logs.active_for_ms ?? 0) / 1000).toFixed(3)} 秒。
              {data.persistence?.logs.slowest_batch && <>
                本进程最慢一批 {data.persistence.logs.slowest_batch.items} 条 / {(data.persistence.logs.slowest_batch.bytes / 1024).toFixed(1)} KB，
                总耗时 {(data.persistence.logs.slowest_batch.wall_ms / 1000).toFixed(3)} 秒，写线程 CPU {(data.persistence.logs.slowest_batch.thread_cpu_ms / 1000).toFixed(3)} 秒，
                {data.persistence.logs.slowest_batch.succeeded ? "提交成功" : "提交失败"}。
              </>}
              CPU 时间较低而总耗时较高时，仍需结合锁、磁盘和调度判断原因。
            </p>
            <div className="mt-2 grid gap-x-6 gap-y-2 sm:grid-cols-2">
              {([
                ["account_selection_lock_wait", "选号锁等待"], ["account_cache_lock_wait", "账号缓存锁等待"],
                ["account_selection_lock_hold", "选号锁持有"], ["account_selection_lock_hold_cpu", "选号持锁 CPU"],
                ["account_update_account_lock_hold", "账号更新持锁"],
                ["account_record_image_result_lock_hold", "结果结算持锁"],
                ["image_completion_queue", "结果处理排队"], ["image_completion_work", "结果处理执行"],
                ["image_completion_resume", "结果处理恢复"], ["image_worker_resume", "生图线程完成后恢复"],
                ["request_filter_queue", "审核通用池排队"],
                ["account_read_lock_wait", "账号读取锁等待"], ["account_token_refresh", "选号时令牌续期"],
                ["account_slot_wait", "单账号槽位等待"], ["account_foreground_verification", "前台校验"],
                ["image_download_http", "图片下载 HTTP"], ["log_index_update", "日志批量索引更新"],
                ["log_index_sync", "日志历史索引同步"], ["log_body_append", "日志正文追加"],
                ["log_body_fsync", "日志正文落盘"], ["log_index_commit", "日志索引提交"],
                ["log_transaction_wait", "日志写事务等待"], ["log_append_lock_wait", "日志追加锁等待"],
              ] as const).map(([key, label]) => <div key={key} className="flex justify-between gap-3">
                <span>{label}</span><span>P95 {((data.phases?.[key]?.p95_ms ?? 0) / 1000).toFixed(3)} 秒 / 最大 {((data.phases?.[key]?.max_ms ?? 0) / 1000).toFixed(3)} 秒</span>
              </div>)}
            </div>
            {data.persistence?.logs.slowest_batch && <p className="mt-2 break-words leading-5">
              最慢批次分解：{Object.entries(data.persistence.logs.slowest_batch.stages).map(([key, value]) => `${key}: ${(value / 1000).toFixed(3)} 秒`).join("；")}
            </p>}
          </details>
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
          <CardContent className="space-y-4 p-5">
            <div className="flex items-center gap-2"><Users className="size-4 text-stone-500" /><h2 className="font-semibold">账号池</h2></div>
            <div className="grid gap-3 sm:grid-cols-3">
              <Metric label="总账号" value={accounts.total} />
              <Metric label="可用账号" value={accounts.available} tone="text-emerald-700" />
              <Metric label="不可用账号" value={accounts.unavailable} tone="text-rose-700" />
            </div>
            <div className="flex flex-wrap gap-2 text-xs text-stone-500">
              <Badge variant="secondary" className="rounded-md">限流 {accounts.limited}</Badge>
              <Badge variant="secondary" className="rounded-md">异常 {accounts.abnormal}</Badge>
              <Badge variant="secondary" className="rounded-md">禁用 {accounts.disabled}</Badge>
            </div>
            <div className="space-y-3 border-t border-stone-100 pt-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <h3 className="text-sm font-medium">后台账号刷新</h3>
                <Badge variant="secondary" className={`rounded-md ${refresh?.status === "running" ? "text-sky-700" : refresh?.status === "failed" || refresh?.status === "completed_with_errors" ? "text-amber-700" : "text-emerald-700"}`}>{refreshStatus}</Badge>
              </div>
              {refresh ? <>
                <p className="text-xs text-stone-500">{refresh.round ? `第 ${refresh.round} 轮 · ` : ""}{refreshPhase} · {refresh.finished_at ? "本轮耗时" : "已运行"} {formatDuration(refresh.elapsed_seconds * 1000)}</p>
                <div className="grid gap-3 sm:grid-cols-3">
                  <Metric label="成功刷新" value={refresh.refresh.succeeded} tone="text-emerald-700" />
                  <button type="button" aria-label={`查看刷新失败账号（${refresh.refresh.failed} 个）`} disabled={refresh.refresh.failed === 0}
                    onClick={() => setFailureSelection({ round: refresh.round, kind: "refresh" })}
                    className="rounded-xl text-left outline-none transition-shadow enabled:hover:ring-2 enabled:hover:ring-rose-200 focus-visible:ring-2 focus-visible:ring-rose-400 disabled:cursor-default">
                    <Metric label="刷新失败" value={refresh.refresh.failed} hint={refresh.refresh.failed ? "点击查看账号和失败原因" : "暂无失败账号"} tone="text-rose-700" />
                  </button>
                  <Metric label="正在处理" value={refresh.refresh.active} hint="含校验并发、速率等待" tone="text-sky-700" />
                  <Metric label="本轮计划刷新" value={refresh.refresh.total} />
                  <Metric label="已处理" value={refresh.refresh.completed} />
                  <Metric label="待处理" value={refresh.refresh.pending} />
                </div>
                {refresh.refresh.total > 0 ? <div className="space-y-1.5">
                  <div className="flex justify-between text-xs text-stone-500"><span>账号信息刷新进度</span><span>{refresh.refresh.completed} / {refresh.refresh.total} · {refreshPercent.toFixed(1)}%</span></div>
                  <div role="progressbar" aria-label="账号信息刷新进度" aria-valuemin={0} aria-valuemax={refresh.refresh.total} aria-valuenow={refresh.refresh.completed} className="h-1.5 overflow-hidden rounded-full bg-stone-100"><div className="h-full rounded-full bg-sky-500 transition-[width]" style={{ width: `${refreshPercent}%` }} /></div>
                </div> : <p className="text-xs text-stone-500">{refresh.status === "running" && refresh.phase === "preparing" ? "正在收集本轮待刷新账号…" : refresh.status === "idle" ? "尚无后台刷新记录" : "本轮没有需要更新信息的账号"}</p>}
                <dl className="grid gap-x-4 gap-y-3 text-xs sm:grid-cols-2">
                  <div><dt className="text-stone-400">最近一轮开始</dt><dd className="mt-1 text-stone-700">{formatTime(refresh.started_at ?? 0)}</dd></div>
                  <div><dt className="text-stone-400">最近一轮结束</dt><dd className="mt-1 text-stone-700">{refresh.status === "running" ? "尚未结束" : formatTime(refresh.finished_at ?? 0)}</dd></div>
                  <div><dt className="text-stone-400">最近成功刷新 / 保活</dt><dd className="mt-1 text-stone-700">{formatTime(refresh.last_success_at ?? 0)}</dd></div>
                  <div><dt className="text-stone-400">最近完整刷新结束</dt><dd className="mt-1 text-stone-700">{formatTime(refresh.last_full_refresh_finished_at ?? 0)}</dd></div>
                  <div><dt className="text-stone-400">预计下次完整刷新</dt><dd className="mt-1 text-stone-700">{formatTime(refresh.next_full_refresh_at ?? refresh.next_run_at ?? 0)}</dd></div>
                </dl>
                <p className="text-xs leading-5 text-stone-500">Token 保活：已处理 {refresh.keepalive.completed} / {refresh.keepalive.total}，成功 {refresh.keepalive.succeeded}，<button type="button" disabled={refresh.keepalive.failed === 0} onClick={() => setFailureSelection({ round: refresh.round, kind: "keepalive" })} className="rounded text-rose-700 enabled:underline enabled:underline-offset-2 disabled:text-inherit">失败 {refresh.keepalive.failed}{refresh.keepalive.failed ? "（查看原因）" : ""}</button>，正在处理 {refresh.keepalive.active}，待处理 {refresh.keepalive.pending}。</p>
                {refresh.error ? <p role="alert" className="text-xs text-amber-700">{refresh.error}</p> : null}
                <p className="text-xs leading-5 text-stone-400">仅统计后台自动巡检，不含手动刷新或生图触发的校验；失败包含网络及校验异常，不等于账号已失效。信息刷新和 Token 保活均结束才算整轮完成，自动重新登录不在本进度内。当前轮次明细会在应用重启后重置，但完整刷新时间会持久化，下一轮按上次完整刷新结束时间计算{refresh.interval_seconds > 0 ? `，间隔 ${refresh.interval_seconds / 60} 分钟` : ""}。</p>
              </> : <p className="text-xs text-stone-500">当前后端未提供后台刷新状态，请更新后端后查看。</p>}
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
          <CardContent className="space-y-4 p-5">
            <div className="flex items-center gap-2"><Activity className="size-4 text-stone-500" /><h2 className="font-semibold">图片结果</h2></div>
            <div className="flex flex-wrap items-center gap-2">
              {[["今天", 1], ["昨天", 1], ["近7天", 7], ["近30天", 30]].map(([label, days]) => (
                <Button key={String(label)} size="sm" variant={range.preset === label ? "default" : "outline"} onClick={() => selectPreset(String(label), Number(days), label === "昨天")}>{label}</Button>
              ))}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Input aria-label="开始日期" type="date" className="w-auto" value={draftStart} onChange={(event) => setDraftStart(event.target.value)} />
              <span className="text-xs text-stone-500">至</span>
              <Input aria-label="结束日期" type="date" className="w-auto" value={draftEnd} onChange={(event) => setDraftEnd(event.target.value)} />
              <Button size="sm" variant="outline" disabled={!draftStart || !draftEnd || draftStart > draftEnd} onClick={() => setRange({ start: draftStart, end: draftEnd, preset: "自定义" })}>查询</Button>
            </div>
            <p className="text-xs text-stone-500">{images.start_date} 至 {images.end_date} · 按服务器完成日期统计（{data.timezone}，基于保留的调用日志）{isLoading ? " · 正在更新…" : ""}</p>
            <div className="grid gap-3 sm:grid-cols-3">
              <Metric label="成功出图" value={images.success_images} hint={`成功 ${images.success} 次请求`} tone="text-emerald-700" />
              <Metric label="出图失败" value={images.failed} hint="技术失败请求数" tone="text-rose-700" />
              <Metric label="成功率" value={`${images.success_rate.toFixed(2)}%`} hint={`按请求统计，共 ${images.total} 次`} tone="text-sky-700" />
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <Metric label="P50 耗时" value={formatDuration(images.duration_p50_ms)} hint="50% 的有效样本耗时不超过此值" />
              <Metric label="P95 耗时" value={formatDuration(images.duration_p95_ms)} hint="95% 的有效样本耗时不超过此值" />
            </div>
            <p className="text-xs leading-5 text-stone-500">耗时样本：{images.duration_samples ?? 0} 次。统计所选日期内已完成的图片请求，包含成功和技术失败，排除安全审核拒绝及缺失或无效耗时，每个请求计一次。使用调用日志耗时，异步为任务执行耗时，不是创建或轮询接口响应耗时；并非客户端端到端耗时。</p>
            <p className="text-xs leading-5 text-stone-500">内容审查排除：{images.excluded} 次。敏感词、安全政策等明确拒绝不计入失败数和成功率分母；审核服务异常、超时等仍计入失败。</p>
          </CardContent>
        </Card>
      </div>

      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="space-y-4 p-5">
          <div className="flex items-center gap-2"><Server className="size-4 text-stone-500" /><h2 className="font-semibold">服务器资源</h2></div>
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-xl border border-stone-200 bg-white px-4 py-3">
              <div className="flex items-center gap-2 text-xs text-stone-500"><Activity className="size-3.5" />CPU</div>
              <div className="mt-2 text-2xl font-semibold text-stone-950">{data.system.cpu_percent === null ? "-" : `${data.system.cpu_percent.toFixed(2)}%`}</div>
              <div className="mt-1 text-xs text-stone-400">负载 {data.system.load_average.length ? data.system.load_average.map((item) => item.toFixed(2)).join(" / ") : "-"}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white px-4 py-3">
              <div className="flex items-center gap-2 text-xs text-stone-500"><MemoryStick className="size-3.5" />内存</div>
              <div className="mt-2 text-2xl font-semibold text-stone-950">{memory.percent.toFixed(2)}%</div>
              <div className="mt-1 text-xs text-stone-400">{formatBytes(memory.used_bytes)} / {formatBytes(memory.total_bytes)}</div>
            </div>
            <div className="rounded-xl border border-stone-200 bg-white px-4 py-3">
              <div className="flex items-center gap-2 text-xs text-stone-500"><HardDrive className="size-3.5" />磁盘</div>
              <div className="mt-2 text-2xl font-semibold text-stone-950">{disk.percent.toFixed(2)}%</div>
              <div className="mt-1 text-xs text-stone-400">已用 {formatBytes(disk.used_bytes)} · 剩余 {formatBytes(disk.free_bytes)}</div>
            </div>
          </div>
        </CardContent>
      </Card>
      <RefreshFailuresDialog selection={failureSelection} onClose={() => setFailureSelection(null)} />
    </section>
  );
}

export default function MonitoringPage() {
  const { isCheckingAuth, session } = useAuthGuard(["admin"]);
  if (isCheckingAuth || !session || session.role !== "admin") {
    return <div className="flex min-h-[40vh] items-center justify-center"><LoaderCircle className="size-5 animate-spin text-stone-400" /></div>;
  }
  return <MonitoringContent />;
}
