"use client";

import { useEffect, useState } from "react";
import { LoaderCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { fetchAccountRefreshFailures, type AccountRefreshFailures } from "@/lib/api";

export type RefreshFailureSelection = { round: number; kind: "refresh" | "keepalive" };

export function RefreshFailuresDialog({ selection, onClose }: { selection: RefreshFailureSelection | null; onClose: () => void }) {
  const [offset, setOffset] = useState(0);
  const [reload, setReload] = useState(0);
  const [data, setData] = useState<AccountRefreshFailures | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const round = selection?.round;
  const kind = selection?.kind;
  const limit = 20;

  useEffect(() => { setOffset(0); setData(null); setError(""); }, [round, kind]);
  useEffect(() => {
    if (round === undefined || !kind) return;
    let cancelled = false;
    setLoading(true);
    setError("");
    fetchAccountRefreshFailures(round, kind, offset, limit).then((result) => {
      if (!cancelled) setData(result);
    }).catch((cause) => {
      if (!cancelled) { setData(null); setError(cause instanceof Error ? cause.message : "加载失败，请重试"); }
    }).finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [round, kind, offset, reload]);

  return <Dialog open={selection !== null} onOpenChange={(open) => { if (!open) onClose(); }}>
    <DialogContent className="flex max-h-[85vh] w-[min(94vw,800px)] flex-col overflow-hidden rounded-2xl p-0">
      <DialogHeader className="shrink-0 border-b border-stone-100 px-5 py-4 pr-12">
        <DialogTitle>{kind === "keepalive" ? "Token 保活失败账号" : "刷新失败账号"}</DialogTitle>
        <DialogDescription>第 {round} 轮后台自动巡检 · 每页 {limit} 条。仅显示账号标识、失败时间及脱敏原因，不展示 Token 或密码。</DialogDescription>
      </DialogHeader>
      <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto px-5 py-4" aria-busy={loading}>
        {loading ? <div role="status" className="flex items-center justify-center gap-2 py-8 text-sm text-stone-500"><LoaderCircle className="size-4 animate-spin" />正在加载失败账号…</div> : error ? <p role="alert" className="py-4 text-sm text-rose-700">{error}</p> : data ? <>
          <p className="text-xs text-stone-500">本轮失败 {data.total} 个，已记录明细 {data.retained} 条。刷新进行中时可点击下方刷新按钮更新。</p>
          {data.total > data.retained ? <p className="text-xs text-amber-700">明细最多保留本轮前 {data.max_retained} 条（账号信息刷新与保活合计），超出部分仍计入失败总数；处理中的统计也可能短暂延迟。</p> : null}
          {data.items.length ? data.items.map((item, index) => <div key={`${offset}-${index}`} className="rounded-xl border border-stone-200 p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span className="min-w-0 break-all text-sm font-medium text-stone-900">{item.account}</span>
              <time className="text-xs text-stone-400">{new Date(item.time * 1000).toLocaleString()}</time>
            </div>
            <p className="mt-2 whitespace-pre-wrap break-all text-sm leading-6 text-rose-700">{item.error}</p>
          </div>) : <p className="py-6 text-center text-sm text-stone-500">暂无失败明细</p>}
        </> : null}
      </div>
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-t border-stone-100 px-5 py-3">
        <Button size="sm" variant="outline" disabled={loading} onClick={() => setReload((value) => value + 1)}>刷新明细</Button>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="outline" disabled={loading || offset === 0} onClick={() => setOffset((value) => Math.max(0, value - limit))}>上一页</Button>
          <span className="text-xs text-stone-500">第 {Math.floor(offset / limit) + 1} 页</span>
          <Button size="sm" variant="outline" disabled={loading || !data || offset + limit >= data.retained} onClick={() => setOffset((value) => value + limit)}>下一页</Button>
        </div>
      </div>
    </DialogContent>
  </Dialog>;
}
