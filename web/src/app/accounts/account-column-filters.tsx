"use client";

import { useState, type ReactNode } from "react";
import { Check, Filter } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

export type AccountTableFilters = {
  source: string; rtStatus: string; pool: string; scoreMin: string; scoreMax: string;
  createdStart: string; createdEnd: string;
};
export const emptyAccountFilters: AccountTableFilters = {
  source: "all", rtStatus: "all", pool: "all", scoreMin: "", scoreMax: "", createdStart: "", createdEnd: "",
};

function FilterShell({ label, active, summary, children, onOpen }: {
  label: string; active: boolean; summary?: string; onOpen?: () => void;
  children: (close: () => void) => ReactNode;
}) {
  const [open, setOpen] = useState(false);
  return <span className="inline-flex items-center gap-1 whitespace-nowrap">
    {label}
    <Popover open={open} onOpenChange={(next) => { if (next) onOpen?.(); setOpen(next); }}>
      <PopoverTrigger asChild>
        <button type="button" aria-label={`筛选${label}${active && summary ? `：${summary}` : ""}`}
          title={active ? summary : `筛选${label}`}
          className={cn("inline-flex size-6 shrink-0 items-center justify-center rounded-md transition-colors hover:bg-stone-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500", active ? "bg-sky-50 text-sky-700" : "text-stone-400")}>
          <Filter className="size-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-64 p-3 text-sm font-normal tracking-normal normal-case">
        <p className="mb-2 text-xs font-medium text-stone-500">筛选{label}</p>
        {children(() => setOpen(false))}
      </PopoverContent>
    </Popover>
  </span>;
}

export function AccountColumnFilter({ label, value, options, onChange }: {
  label: string; value: string; options: { value: string; label: string }[]; onChange: (value: string) => void;
}) {
  return <FilterShell label={label} active={value !== "all"} summary={options.find((o) => o.value === value)?.label ?? value}>
    {(close) => <div className="max-h-64 overflow-y-auto">{options.map((option) =>
      <button key={option.value} type="button" aria-pressed={value === option.value}
        onClick={() => { onChange(option.value); close(); }}
        className="flex w-full items-center gap-2 rounded-lg px-2 py-2 text-left text-sm text-stone-700 transition-colors hover:bg-stone-100 focus-visible:outline-sky-500">
        <Check className={cn("size-3.5 shrink-0", value === option.value ? "opacity-100" : "opacity-0")} />
        <span className="truncate">{option.label}</span>
      </button>)}</div>}
  </FilterShell>;
}

export function PoolScoreColumnFilter({ value, onChange }: {
  value: AccountTableFilters; onChange: (patch: Partial<AccountTableFilters>) => void;
}) {
  const [pool, setPool] = useState(value.pool);
  const [min, setMin] = useState(value.scoreMin);
  const [max, setMax] = useState(value.scoreMax);
  const active = value.pool !== "all" || value.scoreMin !== "" || value.scoreMax !== "";
  return <FilterShell label="热池 / 评分" active={active}
    summary={`${value.pool === "hot" ? "热池" : value.pool === "cold" ? "冷池" : "全部池"} · ${value.scoreMin || "0"}–${value.scoreMax || "100"}分`}
    onOpen={() => { setPool(value.pool); setMin(value.scoreMin); setMax(value.scoreMax); }}>
    {(close) => <form className="space-y-3" onSubmit={(event) => {
      event.preventDefault();
      if (min !== "" && max !== "" && Number(min) > Number(max)) { toast.error("最低评分不能高于最高评分"); return; }
      onChange({ pool, scoreMin: min, scoreMax: max }); close();
    }}>
      <div className="grid grid-cols-3 gap-1 rounded-lg bg-stone-100 p-1">
        {[["all", "全部"], ["hot", "热池"], ["cold", "冷池"]].map(([key, label]) =>
          <button key={key} type="button" aria-pressed={pool === key} onClick={() => setPool(key)}
            className={cn("rounded-md py-1.5 transition-colors", pool === key ? "bg-white text-stone-900 shadow-sm" : "text-stone-500 hover:text-stone-900")}>{label}</button>)}
      </div>
      <div className="grid grid-cols-2 gap-2">
        <label className="space-y-1 text-xs text-stone-500">最低评分<Input aria-label="最低评分" type="number" min={0} max={100} step={1} value={min} onChange={(e) => setMin(e.target.value)} placeholder="0" /></label>
        <label className="space-y-1 text-xs text-stone-500">最高评分<Input aria-label="最高评分" type="number" min={0} max={100} step={1} value={max} onChange={(e) => setMax(e.target.value)} placeholder="100" /></label>
      </div>
      <div className="flex justify-end gap-2 border-t border-stone-100 pt-3">
        <Button type="button" size="sm" variant="ghost" onClick={() => { onChange({ pool: "all", scoreMin: "", scoreMax: "" }); close(); }}>重置</Button>
        <Button type="submit" size="sm">应用</Button>
      </div>
    </form>}
  </FilterShell>;
}

function localDay(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

// Send actual UTC boundaries of the displayed local calendar days; end is exclusive.
export function accountDateBoundary(day: string, end = false): number | undefined {
  if (!day) return undefined;
  const date = new Date(`${day}T00:00:00`);
  if (end) date.setDate(date.getDate() + 1);
  return date.getTime() / 1000;
}

export function CreatedColumnFilter({ value, onChange }: {
  value: AccountTableFilters; onChange: (patch: Partial<AccountTableFilters>) => void;
}) {
  const [start, setStart] = useState(value.createdStart);
  const [end, setEnd] = useState(value.createdEnd);
  return <FilterShell label="创建时间" active={!!(value.createdStart || value.createdEnd)}
    summary={`${value.createdStart || "不限"} 至 ${value.createdEnd || "不限"}`}
    onOpen={() => { setStart(value.createdStart); setEnd(value.createdEnd); }}>
    {(close) => <form className="space-y-3" onSubmit={(event) => {
      event.preventDefault();
      if (start && end && start > end) { toast.error("开始日期不能晚于结束日期"); return; }
      onChange({ createdStart: start, createdEnd: end }); close();
    }}>
      <div className="grid grid-cols-4 gap-1">{["今天", "昨天", "近7天", "近30天"].map((label, index) =>
        <button type="button" key={label} className="rounded-md bg-stone-100 py-1.5 text-xs text-stone-600 transition-colors hover:bg-stone-200"
          onClick={() => { const to = new Date(); if (index === 1) to.setDate(to.getDate() - 1); const from = new Date(to); if (index >= 2) from.setDate(from.getDate() - (index === 2 ? 6 : 29)); setStart(localDay(from)); setEnd(localDay(to)); }}>{label}</button>)}</div>
      <label className="block space-y-1 text-xs text-stone-500">开始日期<Input aria-label="创建开始日期" type="date" value={start} onChange={(e) => setStart(e.target.value)} /></label>
      <label className="block space-y-1 text-xs text-stone-500">结束日期<Input aria-label="创建结束日期" type="date" value={end} onChange={(e) => setEnd(e.target.value)} /></label>
      <p className="text-xs text-stone-400">按浏览器时区，包含所选结束日。</p>
      <div className="flex justify-end gap-2 border-t border-stone-100 pt-3">
        <Button type="button" size="sm" variant="ghost" onClick={() => { onChange({ createdStart: "", createdEnd: "" }); close(); }}>重置</Button>
        <Button type="submit" size="sm">应用</Button>
      </div>
    </form>}
  </FilterShell>;
}
