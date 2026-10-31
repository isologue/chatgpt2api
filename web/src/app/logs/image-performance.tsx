import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { diagnosticPhaseLabel, performanceComponentLabel } from "@/lib/image-diagnostic-labels";

function records(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> =>
    item !== null && typeof item === "object" && !Array.isArray(item)) : [];
}
function milliseconds(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(3)} 毫秒` : "-";
}
function timestamp(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? new Date(value * 1000).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) : "-";
}

export function ImagePerformance({ diagnostics }: { diagnostics: unknown }) {
  if (!diagnostics || typeof diagnostics !== "object") return null;
  const data = diagnostics as Record<string, unknown>;
  const summaries = records(data.performance);
  const slices = records(data.slow_slices);
  if (!summaries.length && !slices.length) return null;
  return (
    <section className="space-y-3 rounded-xl border border-stone-200 bg-white p-4">
      <h3 className="text-sm font-semibold text-stone-700">图片请求性能细分</h3>
      <p className="text-xs leading-5 text-stone-500">按阶段汇总，不同指标可能包含或重叠，不能直接相加。连接计时均从本次连接开始累计，经过代理时可能包含代理连接。工作线程执行可能包含等待子进程；CPU 时间用于区分实际计算与等待。</p>
      {slices.length ? (
        <div className="space-y-2">
          <h4 className="text-xs font-medium text-stone-700">图片循环慢同步片段（超过 50 毫秒，每阶段保留最慢一次）</h4>
          <p className="text-xs text-stone-500">墙钟时间可能包含线程被抢占或等待 GIL，不等同于一直在计算；这里只覆盖协议编排片段，不包含全部底层网络回调。</p>
          <div className="overflow-x-auto rounded-lg border border-stone-200">
            <Table>
              <TableHeader><TableRow><TableHead>阶段</TableHead><TableHead>最慢片段开始时间</TableHead><TableHead>慢片段次数</TableHead><TableHead>最慢墙钟耗时</TableHead><TableHead>该次 CPU 耗时</TableHead></TableRow></TableHeader>
              <TableBody>{slices.map((row, i) => <TableRow key={i}>
                <TableCell>{diagnosticPhaseLabel(row.stage)}</TableCell><TableCell className="whitespace-nowrap">{timestamp(row.started_at)}</TableCell>
                <TableCell>{String(row.count ?? "-")}</TableCell><TableCell className="whitespace-nowrap">{milliseconds(row.duration_ms)}</TableCell>
                <TableCell className="whitespace-nowrap">{milliseconds(row.cpu_ms)}</TableCell>
              </TableRow>)}</TableBody>
            </Table>
          </div>
        </div>
      ) : null}
      {summaries.length ? <details className="group">
        <summary className="cursor-pointer text-xs font-medium text-stone-700">展开分阶段计时（{summaries.length} 项）</summary>
        <div className="mt-3 max-h-96 overflow-auto rounded-lg border border-stone-200">
          <Table>
            <TableHeader><TableRow><TableHead>阶段</TableHead><TableHead>性能项</TableHead><TableHead>次数</TableHead><TableHead>累计耗时</TableHead><TableHead>单次最大耗时</TableHead></TableRow></TableHeader>
            <TableBody>{summaries.map((row, i) => <TableRow key={i}>
              <TableCell>{diagnosticPhaseLabel(row.stage)}</TableCell><TableCell>{performanceComponentLabel(row.component)}</TableCell>
              <TableCell>{String(row.count ?? "-")}</TableCell><TableCell className="whitespace-nowrap">{milliseconds(row.total_ms)}</TableCell>
              <TableCell className="whitespace-nowrap">{milliseconds(row.max_ms)}</TableCell>
            </TableRow>)}</TableBody>
          </Table>
        </div>
      </details> : null}
    </section>
  );
}
