"use client";

import { useEffect, useRef, useState } from "react";
import { Clock3, LoaderCircle, Save, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { fetchRetention, runRetention, saveRetention, type RetentionKind, type RetentionStatus } from "@/lib/api";

function displayTime(value: string | null) {
  return value ? value.replace("T", " ") : "—";
}

export function DataRetentionCard({ kind, onComplete }: { kind: RetentionKind; onComplete: () => void }) {
  const [status, setStatus] = useState<RetentionStatus | null>(null);
  const [hours, setHours] = useState("");
  const [interval, setIntervalValue] = useState("60");
  const [enabled, setEnabled] = useState(false);
  const [saving, setSaving] = useState(false);
  const [starting, setStarting] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState("");
  const lastStatus = useRef<RetentionStatus | null>(null);
  const onCompleteRef = useRef(onComplete);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  const label = kind === "images" ? "图片" : "日志";
  const warning = kind === "images"
    ? "清理本地及已记录的 WebDAV 原图、缩略图和标签，删除后原图片链接将失效。"
    : "清理全部调用日志和账号管理日志，包括请求体、响应体；监控页的历史统计也会随之减少。";
  const validHours = Number.isInteger(Number(hours)) && Number(hours) >= 1 && Number(hours) <= 876000;
  const validInterval = Number.isInteger(Number(interval)) && Number(interval) >= 1 && Number(interval) <= 43200;

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    let initialized = false;
    async function poll() {
      try {
        if (document.hidden && initialized) return;
        const data = await fetchRetention(kind, controller.signal);
        if (controller.signal.aborted) return;
        if (!initialized) {
          setHours(String(data.policy.retention_hours));
          setIntervalValue(String(data.policy.interval_minutes));
          setEnabled(data.policy.enabled);
          initialized = true;
        }
        if (lastStatus.current?.running && !data.running) {
          onCompleteRef.current();
          if (data.last_error) toast.error("清理失败：" + data.last_error);
          else if (data.last_result?.failed) toast.warning("清理已完成，部分图片删除失败，请查看执行结果");
          else toast.success("清理完成，共删除 " + (data.last_result?.removed ?? 0) + " 条");
        }
        lastStatus.current = data;
        setStatus(data);
        setError("");
      } catch (err) {
        if (!controller.signal.aborted) setError(err instanceof Error ? err.message : "读取清理状态失败");
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(poll, lastStatus.current?.running ? 2000 : 15000);
      }
    }
    void poll();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [kind]);

  async function save() {
    if (!validHours || !validInterval) return;
    setSaving(true);
    try {
      const data = await saveRetention(kind, { enabled, retention_hours: Number(hours), interval_minutes: Number(interval) });
      setStatus(data);
      toast.success("清理策略已保存" + (enabled ? "，将在下次执行时间自动清理" : "，自动清理已关闭"));
    } catch (err) { toast.error(err instanceof Error ? err.message : "保存失败"); }
    finally { setSaving(false); }
  }

  async function run() {
    setStarting(true);
    try {
      const data = await runRetention(kind, Number(hours));
      lastStatus.current = data;
      setStatus(data);
      setConfirming(false);
      toast.success("已开始后台清理，关闭页面不影响执行");
      // 下次轮询会读取任务结果，无需持续占用请求连接。
    } catch (err) { toast.error(err instanceof Error ? err.message : "启动清理失败"); }
    finally { setStarting(false); }
  }

  return (
    <Card className="rounded-2xl border-stone-200 bg-white/90 shadow-sm dark:border-white/10 dark:bg-white/5">
      <CardContent className="space-y-4 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-sm font-medium"><Clock3 className="size-4" />{label}定时清理</div>
          <label className="flex items-center gap-2 text-sm text-stone-600 dark:text-stone-300">
            <Checkbox checked={enabled} onCheckedChange={(value) => setEnabled(value === true)} disabled={!status} />自动清理（保存后生效）
          </label>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <label className="space-y-1.5 text-sm"><span>保留最近（小时）</span>
            <Input type="number" min={1} max={876000} step={1} value={hours} onChange={(event) => setHours(event.target.value)} disabled={!status} className="w-40" />
          </label>
          <label className="space-y-1.5 text-sm"><span>自动执行间隔（分钟）</span>
            <Input type="number" min={1} max={43200} step={1} value={interval} onChange={(event) => setIntervalValue(event.target.value)} disabled={!status} className="w-40" />
          </label>
          <Button variant="outline" onClick={() => void save()} disabled={!status || saving || !validHours || !validInterval}>
            {saving ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}保存策略
          </Button>
          <Button variant="outline" className="border-rose-200 text-rose-600 hover:bg-rose-50" onClick={() => setConfirming(true)} disabled={!status || status.running || starting || !validHours}>
            {status?.running ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}{status?.running ? "后台清理中" : "立即清理"}
          </Button>
        </div>
        <div className="space-y-1 text-xs leading-relaxed text-stone-500 dark:text-stone-400">
          <p>删除超过保留时长的{label}，不受当前列表筛选条件影响。{warning}删除不可恢复。</p>
          <p>自动清理默认关闭；开启或重启服务后，等待一个执行间隔再开始。手动清理使用上方填写的保留时长。</p>
          {status ? <p>自动清理：{status.policy.enabled ? "已开启" : "已关闭"} · 下次执行：{displayTime(status.next_run_at)} · 最近开始：{displayTime(status.last_started_at)} · 最近结束：{displayTime(status.last_finished_at)}</p> : null}
          {status?.running ? <p>正在清理 {status.retention_hours} 小时以前的数据（截止 {displayTime(status.cutoff ?? null)}），可关闭页面。</p> : null}
          {status?.last_result ? <p>最近结果：删除 {status.last_result.removed} 条{status.last_result.failed ? "，失败 " + status.last_result.failed + " 条" : ""}{status.last_result.reclaimed_bytes !== undefined ? "，释放 " + (status.last_result.reclaimed_bytes / 1024 / 1024).toFixed(2) + " MB 日志正文" : ""}</p> : null}
          {status?.last_result?.errors?.map((message) => <p key={message} className="break-all text-rose-600">{message}</p>)}
          {status?.last_error || error ? <p className="break-all text-rose-600">{status?.last_error || error}</p> : null}
          {status && (!validHours || !validInterval) ? <p className="text-rose-600">保留时长需为 1～876000 小时的整数，执行间隔需为 1～43200 分钟的整数。</p> : null}
        </div>
      </CardContent>
      <Dialog open={confirming} onOpenChange={(open) => { if (!starting) setConfirming(open); }}>
        <DialogContent>
          <DialogHeader><DialogTitle>确认清理{label}</DialogTitle><DialogDescription>将删除 {hours} 小时以前的全部{label}，保留最近 {hours} 小时。{warning}此操作不可恢复。</DialogDescription></DialogHeader>
          <DialogFooter><Button variant="outline" onClick={() => setConfirming(false)} disabled={starting}>取消</Button><Button onClick={() => void run()} disabled={starting} className="bg-rose-600 text-white hover:bg-rose-700">{starting ? "正在启动…" : "确认清理"}</Button></DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
