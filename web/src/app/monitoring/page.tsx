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

  return (
    <section className="space-y-5">
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
        <Metric label="实时并发数" value={executor.inflight} hint={`执行中 ${executor.workers} 个 worker`} tone="text-sky-700" />
        <Metric label="可用执行槽位" value={executor.available_slots} hint={`总容量 ${executor.capacity}`} tone="text-emerald-700" />
        <Metric label="剩余排队槽位" value={executor.remaining_queue_slots} hint={`已排队 ${executor.queued} / ${executor.queue_size}`} tone="text-amber-700" />
        <Metric label="执行池状态" value={executor.full ? "已满" : "有余量"} hint={executor.full ? "新请求将返回 429" : "可以继续接收任务"} tone={executor.full ? "text-rose-700" : "text-emerald-700"} />
      </div>

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
