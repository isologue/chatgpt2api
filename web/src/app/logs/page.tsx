"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronLeft, ChevronRight, Filter, ImageIcon, LoaderCircle, RefreshCw, Search, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { DataRetentionCard } from "@/components/data-retention-card";
import { DateRangeFilter } from "@/components/date-range-filter";
import { ImageLightbox } from "@/components/image-lightbox";
import { ImageThumbnail, getImageThumbnailUrl, getImagePreviewUrl } from "@/components/image-thumbnail";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { deleteSystemLogs, fetchLogKeyNames, fetchLogStorage, fetchSystemLog, fetchSystemLogs, type LogStorageStats, type SystemLog } from "@/lib/api";
import { diagnosticPhaseLabel, diagnosticReasonLabel, diagnosticStatusLabel } from "@/lib/image-diagnostic-labels";
import { useAuthGuard } from "@/lib/use-auth-guard";

import { ImagePerformance } from "./image-performance";

const LogType = {
  Call: "call",
  Account: "account",
} as const;

const typeLabels: Record<string, string> = {
  [LogType.Call]: "调用日志",
  [LogType.Account]: "账号管理日志",
};

function getDetailText(item: SystemLog, key: string) {
  const value = item.detail?.[key];
  return typeof value === "string" || typeof value === "number" ? String(value) : "-";
}

function getCallTime(item: SystemLog, key: "started_at" | "ended_at") {
  const value = item.detail?.[key];
  if (typeof value === "string" && value) return value;
  return key === "ended_at" ? item.time : "-";
}

function formatDuration(item: SystemLog) {
  const value = item.detail?.duration_ms;
  return typeof value === "number" ? `${(value / 1000).toFixed(2)} s` : "-";
}

function getAsyncLabel(item: SystemLog) {
  return typeof item.detail?.is_async === "boolean" ? (item.detail.is_async ? "是" : "否") : "-";
}

function getResponseFormat(item: SystemLog) {
  const value = item.detail?.response_format;
  if (value === "b64_json") return "b64_json";
  if (value === "url") return "URL";
  return "-";
}

function getSwitchCount(item: SystemLog) {
  const value = item.detail?.switch_count;
  return typeof value === "number" ? String(Math.max(0, value)) : "0";
}

function getUrls(item: SystemLog | null) {
  const urls = item?.detail?.urls;
  return Array.isArray(urls) ? urls.filter((url): url is string => typeof url === "string").map(getImagePreviewUrl) : [];
}

function getStatus(item: SystemLog) {
  const status = item.detail?.status;
  if (status === "success") return "成功";
  if (status === "failed") return "失败";
  if (status === "safety_rejected") return "安全审核拒绝";
  return "-";
}

function formatDiagnosticTime(value: unknown) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return new Date(value * 1000).toLocaleString(undefined, {
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function ColumnFilter({ label, value, options, onChange, searchable = false, loadOptions }: {
  label: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
  searchable?: boolean;
  loadOptions?: (query: string, signal: AbortSignal) => Promise<{ value: string; label: string }[]>;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [loadedOptions, setLoadedOptions] = useState<{ value: string; label: string }[]>([]);
  const [loadingOptions, setLoadingOptions] = useState(false);
  const selected = options.find((option) => option.value === value);
  const combined = [...options, ...loadedOptions.filter((option) => !options.some((base) => base.value === option.value))];
  const visibleOptions = combined.filter((option) => option.value === "all" || option.label.toLowerCase().includes(query.toLowerCase()));

  useEffect(() => {
    if (!open || !loadOptions) return;
    const controller = new AbortController();
    setLoadingOptions(true);
    setLoadedOptions([]);
    const timer = window.setTimeout(() => {
      void loadOptions(query, controller.signal).then((result) => {
        if (!controller.signal.aborted) setLoadedOptions(result);
      }).catch((error) => {
        if (!controller.signal.aborted) toast.error(error instanceof Error ? error.message : "加载令牌名称失败");
      }).finally(() => {
        if (!controller.signal.aborted) setLoadingOptions(false);
      });
    }, 250);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [open, query, loadOptions]);
  return (
    <span className="inline-flex items-center gap-1 whitespace-nowrap">
      {label}
      <Popover open={open} onOpenChange={(next) => { setOpen(next); if (!next) setQuery(""); }}>
        <PopoverTrigger asChild>
          <button type="button" aria-label={`筛选${label}${value !== "all" ? `：${selected?.label || value}` : ""}`} className={`inline-flex size-6 items-center justify-center rounded-md transition-colors hover:bg-stone-100 ${value !== "all" ? "bg-sky-50 text-sky-700" : "text-stone-400"}`}>
            <Filter className="size-3.5" />
          </button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-60 p-2">
          <p className="px-2 py-1.5 text-xs font-medium text-stone-500">筛选{label}</p>
          {searchable ? <Input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索令牌名称" className="mb-2 h-8" aria-label="搜索令牌名称" /> : null}
          {loadingOptions ? <p className="px-2 py-1 text-xs text-stone-400">正在加载…</p> : null}
          <div className="max-h-64 overflow-y-auto">
            {visibleOptions.map((option) => (
              <button key={option.value} type="button" aria-pressed={value === option.value} onClick={() => { onChange(option.value); setOpen(false); setQuery(""); }} className="flex w-full items-center gap-2 rounded-lg px-2 py-2 text-left text-sm text-stone-700 hover:bg-stone-100">
                <Check className={`size-3.5 shrink-0 ${value === option.value ? "opacity-100" : "opacity-0"}`} />
                <span className="break-all">{option.label}</span>
              </button>
            ))}
            {!loadingOptions && visibleOptions.length === 1 && query ? <p className="px-2 py-3 text-xs text-stone-400">没有匹配的令牌名称</p> : null}
          </div>
        </PopoverContent>
      </Popover>
    </span>
  );
}

function JsonValue({ value, path, expanded, onToggle }: {
  value: unknown;
  path: string;
  expanded: Set<string>;
  onToggle: (path: string) => void;
}) {
  if (typeof value === "string") {
    const isLong = value.length > 240 || (/^[A-Za-z0-9+/=]+$/.test(value) && value.length > 100);
    if (!isLong) return <span className="whitespace-pre-wrap break-all text-emerald-800">{JSON.stringify(value)}</span>;
    const isExpanded = expanded.has(path);
    const preview = `${value.slice(0, 96)}... (${value.length.toLocaleString()} 字符)`;
    return (
      <button type="button" className="text-left whitespace-pre-wrap break-all text-emerald-800 underline decoration-dotted underline-offset-2 hover:text-emerald-950" aria-expanded={isExpanded} onClick={() => onToggle(path)}>
        {JSON.stringify(isExpanded ? value : preview)}
      </button>
    );
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return <span>[]</span>;
    return <span>[<span className="block border-l border-stone-200 pl-4">{value.map((item, index) => <span key={`${path}.${index}`} className="block"><JsonValue value={item} path={`${path}.${index}`} expanded={expanded} onToggle={onToggle} />{index < value.length - 1 ? "," : ""}</span>)}</span>]</span>;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return <span>{"{}"}</span>;
    return <span>{"{"}<span className="block border-l border-stone-200 pl-4">{entries.map(([key, item], index) => <span key={`${path}.${key}`} className="block"><span className="text-sky-800">{JSON.stringify(key)}</span>: <JsonValue value={item} path={`${path}.${key}`} expanded={expanded} onToggle={onToggle} />{index < entries.length - 1 ? "," : ""}</span>)}</span>{"}"}</span>;
  }
  return <span className="text-amber-800">{JSON.stringify(value) ?? "null"}</span>;
}

function formatStorageSize(bytes: number) {
  if (bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  return (bytes / 1024 ** index).toFixed(index === 0 ? 0 : 1) + " " + units[index];
}

function LogsContent() {
  const [items, setItems] = useState<SystemLog[]>([]);
  const [storage, setStorage] = useState<LogStorageStats | null>(null);
  const [storageLoading, setStorageLoading] = useState(true);
  const [storageError, setStorageError] = useState("");
  const storageRequestId = useRef(0);

  const loadStorage = useCallback(async (signal?: AbortSignal) => {
    const id = ++storageRequestId.current;
    setStorageLoading(true);
    try {
      const data = await fetchLogStorage(signal);
      if (signal?.aborted || id !== storageRequestId.current) return;
      setStorage(data);
      setStorageError("");
    } catch (error) {
      if (!signal?.aborted && id === storageRequestId.current) setStorageError(error instanceof Error ? error.message : "加载日志存储信息失败");
    } finally {
      if (!signal?.aborted && id === storageRequestId.current) setStorageLoading(false);
    }
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void loadStorage(controller.signal);
    return () => controller.abort();
  }, [loadStorage]);
  const [type, setType] = useState<string>(LogType.Call);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [keyNameFilter, setKeyNameFilter] = useState("all");
  const [asyncFilter, setAsyncFilter] = useState("all");
  const [formatFilter, setFormatFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [total, setTotal] = useState(0);
  const [pageSize, setPageSize] = useState(20);
  const [jumpPage, setJumpPage] = useState("");
  const requestId = useRef(0);
  const [detailLog, setDetailLog] = useState<SystemLog | null>(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [expandedJsonValues, setExpandedJsonValues] = useState<Set<string>>(() => new Set());
  const [lightboxIndex, setLightboxIndex] = useState(0);
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const [page, setPage] = useState(1);
  const [isLoading, setIsLoading] = useState(true);
  const [isDeleting, setIsDeleting] = useState(false);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [deletingItems, setDeletingItems] = useState<SystemLog[]>([]);
  const detailUrls = getUrls(detailLog);
  const detailImages = detailUrls.map((url, index) => ({ id: `${index}`, src: url }));
  const detailData = detailLog?.detail || {};
  const hasRequestBody = Object.prototype.hasOwnProperty.call(detailData, "request_body");
  const hasResponseBody = Object.prototype.hasOwnProperty.call(detailData, "response_body");
  const requestBody = detailData.request_body;
  const responseBody = detailData.response_body;
  const switchCount = typeof detailData.switch_count === "number" ? Math.max(0, detailData.switch_count) : 0;
  const accountTimeMs = typeof detailData.account_time_ms === "number" ? Math.max(0, detailData.account_time_ms) : 0;
  const totalDurationMs = typeof detailData.duration_ms === "number" ? Math.max(0, detailData.duration_ms) : 0;
  const accountTimeShare = totalDurationMs > 0 ? Math.min(100, accountTimeMs * 100 / totalDurationMs) : 0;
  const accountSwitchTrace = Array.isArray(detailData.account_switch_trace)
    ? detailData.account_switch_trace.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
    : [];
  const diagnosticSteps = detailData.diagnostics && typeof detailData.diagnostics === "object"
    && Array.isArray((detailData.diagnostics as Record<string, unknown>).steps)
    ? ((detailData.diagnostics as Record<string, unknown>).steps as unknown[]).filter(
      (item): item is Record<string, unknown> => Boolean(item && typeof item === "object"),
    )
    : [];
  const isCallLog = type === LogType.Call;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(page, pageCount);
  const currentRows = isLoading ? [] : items;
  const selectedSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const currentPageSelected = currentRows.length > 0 && currentRows.every((item) => selectedSet.has(item.id));

  const loadLogs = useCallback(async (signal?: AbortSignal) => {
    const id = ++requestId.current;
    setIsLoading(true);
    try {
      const data = await fetchSystemLogs({
        type, start_date: startDate, end_date: endDate, page, page_size: pageSize,
        ...(type === LogType.Call ? {
          key_name: keyNameFilter === "all" ? undefined : keyNameFilter.slice(5),
          is_async: asyncFilter === "all" ? undefined : asyncFilter === "true",
          response_format: formatFilter === "all" ? undefined : formatFilter,
          status: statusFilter === "all" ? undefined : statusFilter,
        } : {}),
      }, signal);
      if (signal?.aborted || id !== requestId.current) return;
      setItems(data.items);
      setTotal(data.total);
      setPage(data.page);
      setSelectedIds((current) => current.filter((selected) => data.items.some((item) => item.id === selected)));
    } catch (error) {
      if (!signal?.aborted && id === requestId.current) toast.error(error instanceof Error ? error.message : "加载日志失败");
    } finally {
      if (!signal?.aborted && id === requestId.current) setIsLoading(false);
    }
  }, [type, startDate, endDate, page, pageSize, keyNameFilter, asyncFilter, formatFilter, statusFilter]);

  const loadKeyNames = useCallback(async (query: string, signal: AbortSignal) => {
    const result = await fetchLogKeyNames({ type, start_date: startDate, end_date: endDate, query }, signal);
    return result.key_names.map((name) => ({ value: `name:${name}`, label: name || "未记录令牌名称" }));
  }, [type, startDate, endDate]);
  const keyNameOptions = [
    { value: "all", label: "全部令牌" },
    { value: "name:", label: "未记录令牌名称" },
    ...(keyNameFilter !== "all" && keyNameFilter !== "name:" ? [{ value: keyNameFilter, label: keyNameFilter.slice(5) }] : []),
  ];

  const clearFilters = () => {
    setStartDate("");
    setEndDate("");
    setKeyNameFilter("all");
    setAsyncFilter("all");
    setFormatFilter("all");
    setStatusFilter("all");
    setPage(1);
  };

  const openDetail = async (item: SystemLog) => {
    setDetailLog(item);
    setExpandedJsonValues(new Set());
    setDetailOpen(true);
    setIsLoadingDetail(true);
    try {
      const response = await fetchSystemLog(item.id);
      setDetailLog(response.item);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "加载日志详情失败");
    } finally {
      setIsLoadingDetail(false);
    }
  };

  const toggleJsonValue = (path: string) => {
    setExpandedJsonValues((current) => {
      const next = new Set(current);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  const openLogImage = (item: SystemLog, index: number) => {
    setDetailLog(item);
    setLightboxIndex(index);
    setLightboxOpen(true);
  };

  const toggleIds = (ids: string[], checked: boolean) => {
    setSelectedIds((current) => checked ? Array.from(new Set([...current, ...ids])) : current.filter((id) => !ids.includes(id)));
  };

  const confirmDelete = async () => {
    const ids = deletingItems.map((item) => item.id);
    if (ids.length === 0) return;
    setIsDeleting(true);
    try {
      const data = await deleteSystemLogs(ids);
      toast.success(`已删除 ${data.removed} 条日志`);
      setDeletingItems([]);
      setSelectedIds((current) => current.filter((id) => !ids.includes(id)));
      if (detailLog && ids.includes(detailLog.id)) {
        setDetailOpen(false);
        setDetailLog(null);
      }
      await Promise.all([loadLogs(), loadStorage()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除日志失败");
    } finally {
      setIsDeleting(false);
    }
  };

  useEffect(() => {
    const controller = new AbortController();
    void loadLogs(controller.signal);
    return () => controller.abort();
  }, [loadLogs]);

  return (
    <section className="space-y-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div className="space-y-1">
          <div className="text-xs font-semibold tracking-[0.18em] text-stone-500 uppercase">Logs</div>
          <h1 className="text-2xl font-semibold tracking-tight">日志管理</h1>
        </div>
        <div className="flex flex-wrap gap-2">
          <Select value={type} onValueChange={(value) => { setType(value); setPage(1); }}>
            <SelectTrigger className="h-10 w-[150px] rounded-xl border-stone-200 bg-white"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value={LogType.Call}>调用日志</SelectItem>
              <SelectItem value={LogType.Account}>账号管理日志</SelectItem>
            </SelectContent>
          </Select>
          <DateRangeFilter startDate={startDate} endDate={endDate} onChange={(start, end) => { setStartDate(start); setEndDate(end); setPage(1); }} />
          <Button variant="outline" onClick={clearFilters} className="h-10 rounded-xl border-stone-200 bg-white px-4 text-stone-700">
            清除筛选条件
          </Button>
          <Button onClick={() => { void loadLogs(); void loadStorage(); }} disabled={isLoading} className="h-10 rounded-xl bg-stone-950 px-4 text-white hover:bg-stone-800">
            {isLoading ? <LoaderCircle className="size-4 animate-spin" /> : <Search className="size-4" />}
            查询
          </Button>
        </div>
      </div>

      <div className="space-y-3">
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-4" aria-busy={storageLoading}>
          <div className="min-h-28 rounded-xl border border-stone-200 bg-white/80 p-4 dark:border-white/10 dark:bg-white/5">
            <div className="text-xs text-stone-500">磁盘总量</div>
            <div className="mt-1 text-lg font-bold text-stone-800 dark:text-stone-100">{storage ? formatStorageSize(storage.disk_total_mb * 1024 * 1024) : "—"}</div>
          </div>
          <div className="min-h-28 rounded-xl border border-stone-200 bg-white/80 p-4 dark:border-white/10 dark:bg-white/5">
            <div className="text-xs text-stone-500">剩余空间</div>
            <div className={"mt-1 text-lg font-bold " + (!storage ? "text-stone-400" : storage.disk_free_mb < 200 ? "text-red-500" : storage.disk_free_mb < 500 ? "text-yellow-500" : "text-green-600")}>
              {storage ? formatStorageSize(storage.disk_free_mb * 1024 * 1024) : "—"}
            </div>
          </div>
          <div className="min-h-28 rounded-xl border border-stone-200 bg-white/80 p-4 dark:border-white/10 dark:bg-white/5">
            <div className="text-xs text-stone-500">日志数量</div>
            <div className="mt-1 text-lg font-bold text-stone-800 dark:text-stone-100">{storage ? storage.log_count.toLocaleString() : "—"}</div>
          </div>
          <div className="min-h-28 rounded-xl border border-stone-200 bg-white/80 p-4 dark:border-white/10 dark:bg-white/5">
            <div className="text-xs text-stone-500">日志占用</div>
            <div className="mt-1 text-lg font-bold text-stone-800 dark:text-stone-100">{storage ? formatStorageSize(storage.log_size_bytes) : "—"}</div>
            {storage ? <div className="mt-1 text-xs text-stone-500">正文 {formatStorageSize(storage.log_file_size_bytes)} · 索引 {formatStorageSize(storage.log_index_size_bytes)}</div> : null}
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-stone-500">
          <span>统计全部调用日志和账号管理日志，不受列表筛选影响；占用包含日志正文、索引及 SQLite 辅助文件。</span>
          <Button variant="ghost" size="sm" className="h-7 px-2 text-xs" onClick={() => void loadStorage()} disabled={storageLoading}>
            <RefreshCw className={"size-3 " + (storageLoading ? "animate-spin" : "")} />{storageLoading ? "加载存储信息…" : "刷新存储信息"}
          </Button>
        </div>
        {storageError ? <p role="alert" className="text-xs text-rose-600">{storageError}{storage ? "（当前展示上次成功读取的数据）" : ""}</p> : null}
      </div>

      <DataRetentionCard kind="logs" onComplete={() => { void loadLogs(); void loadStorage(); }} />

      <Card className="overflow-hidden rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="p-0">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-stone-100 px-5 py-4">
            <div className="flex flex-wrap items-center gap-3 text-sm text-stone-600">
              <span>共 {total.toLocaleString()} 条</span>
              <label className="flex items-center gap-2">
                <Checkbox checked={currentPageSelected} onCheckedChange={(checked) => toggleIds(currentRows.map((item) => item.id), Boolean(checked))} />
                本页全选
              </label>
              {selectedIds.length > 0 ? <span>已选 {selectedIds.length} 条</span> : null}
            </div>
            <div className="flex items-center gap-2">
              <Button variant="ghost" className="h-8 rounded-lg px-3 text-stone-500" onClick={() => { void loadLogs(); void loadStorage(); }} disabled={isLoading}>
                <RefreshCw className={`size-4 ${isLoading ? "animate-spin" : ""}`} />
                刷新
              </Button>
              <button type="button" className="text-sm text-stone-500 hover:text-stone-900 disabled:text-stone-300" onClick={() => setSelectedIds([])} disabled={selectedIds.length === 0 || isDeleting || isLoading}>
                取消选择
              </button>
              <Button variant="outline" className="h-8 rounded-lg border-rose-200 bg-white px-3 text-rose-600 hover:bg-rose-50" onClick={() => setDeletingItems(items.filter((item) => selectedSet.has(item.id)))} disabled={selectedIds.length === 0 || isDeleting || isLoading}>
                <Trash2 className="size-4" />
                删除所选
              </Button>
            </div>
          </div>
          <div className="overflow-x-auto">
            <Table className="min-w-[900px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12"></TableHead>
                  {isCallLog ? (
                    <>
                      <TableHead>开始时间</TableHead>
                      <TableHead>结束时间</TableHead>
                    </>
                  ) : <TableHead>时间</TableHead>}
                  <TableHead>类型</TableHead>
                  {isCallLog ? <TableHead><ColumnFilter label="令牌名称" value={keyNameFilter} options={keyNameOptions} onChange={(value) => { setKeyNameFilter(value); setPage(1); }} searchable loadOptions={loadKeyNames} /></TableHead> : null}
                  {isCallLog ? <TableHead>调用耗时</TableHead> : null}
                  {isCallLog ? <TableHead>切号次数</TableHead> : null}
                  {isCallLog ? <TableHead><ColumnFilter label="异步" value={asyncFilter} options={[{ value: "all", label: "全部" }, { value: "true", label: "是" }, { value: "false", label: "否" }]} onChange={(value) => { setAsyncFilter(value); setPage(1); }} /></TableHead> : null}
                  {isCallLog ? <TableHead><ColumnFilter label="返回格式" value={formatFilter} options={[{ value: "all", label: "全部" }, { value: "url", label: "URL" }, { value: "b64_json", label: "Base64（b64_json）" }]} onChange={(value) => { setFormatFilter(value); setPage(1); }} /></TableHead> : null}
                  {isCallLog ? <TableHead><ColumnFilter label="状态" value={statusFilter} options={[{ value: "all", label: "全部" }, { value: "success", label: "成功" }, { value: "failed", label: "失败" }, { value: "safety_rejected", label: "安全审核拒绝" }]} onChange={(value) => { setStatusFilter(value); setPage(1); }} /></TableHead> : null}
                  {isCallLog ? <TableHead className="w-36">图片</TableHead> : null}
                  <TableHead>简述</TableHead>
                  <TableHead className="w-40">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {currentRows.map((item) => {
                  const urls = getUrls(item);
                  return (
                    <TableRow key={item.id} className="text-stone-600">
                      <TableCell>
                        <Checkbox checked={selectedSet.has(item.id)} onCheckedChange={(checked) => toggleIds([item.id], Boolean(checked))} />
                      </TableCell>
                      {isCallLog ? (
                        <>
                          <TableCell className="whitespace-nowrap">{getCallTime(item, "started_at")}</TableCell>
                          <TableCell className="whitespace-nowrap">{getCallTime(item, "ended_at")}</TableCell>
                        </>
                      ) : <TableCell className="whitespace-nowrap">{item.time}</TableCell>}
                      <TableCell><Badge variant="secondary" className="rounded-md">{typeLabels[item.type] || item.type}</Badge></TableCell>
                      {isCallLog ? <TableCell>{getDetailText(item, "key_name")}</TableCell> : null}
                      {isCallLog ? <TableCell>{formatDuration(item)}</TableCell> : null}
                      {isCallLog ? <TableCell>{getSwitchCount(item)}</TableCell> : null}
                      {isCallLog ? <TableCell>{getAsyncLabel(item)}</TableCell> : null}
                      {isCallLog ? <TableCell>{getResponseFormat(item)}</TableCell> : null}
                      {isCallLog ? (
                        <TableCell>
                          <Badge variant={item.detail?.status === "safety_rejected" ? "warning" : item.detail?.status === "failed" ? "danger" : item.detail?.status === "success" ? "success" : "secondary"} className="rounded-md">
                            {getStatus(item)}
                          </Badge>
                        </TableCell>
                      ) : null}
                      {isCallLog ? (
                        <TableCell>
                          {urls.length ? (
                            <div className="flex items-center gap-1.5">
                              {urls.slice(0, 3).map((url, imageIndex) => (
                                <button
                                  key={`${url}-${imageIndex}`}
                                  type="button"
                                  className="relative size-9 overflow-hidden rounded-lg border border-stone-200 bg-stone-100"
                                  onClick={() => openLogImage(item, imageIndex)}
                                  title="预览图片"
                                >
                                  <ImageThumbnail src={url} thumbnailSrc={getImageThumbnailUrl(url)} className="h-full w-full" />
                                </button>
                              ))}
                              {urls.length > 3 ? <span className="text-xs text-stone-400">+{urls.length - 3}</span> : null}
                            </div>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-xs text-stone-400">
                              <ImageIcon className="size-3.5" />
                              -
                            </span>
                          )}
                        </TableCell>
                      ) : null}
                      <TableCell className="max-w-[420px] truncate text-stone-500">{item.summary || "-"}</TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Button variant="ghost" className="h-8 rounded-lg px-3 text-stone-600" onClick={() => openDetail(item)}>
                            查看详情
                          </Button>
                          <Button variant="ghost" className="h-8 rounded-lg px-3 text-rose-600 hover:bg-rose-50 hover:text-rose-700" onClick={() => setDeletingItems([item])}>
                            删除
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-stone-100 px-4 py-3 text-sm text-stone-500">
            <span>第 {safePage.toLocaleString()} / {pageCount.toLocaleString()} 页，共 {total.toLocaleString()} 条</span>
            <Select value={String(pageSize)} onValueChange={(value) => { setPageSize(Number(value)); setPage(1); }}>
              <SelectTrigger className="h-9 w-28 rounded-lg" aria-label="每页条数"><SelectValue /></SelectTrigger>
              <SelectContent>{[10, 20, 50, 100].map((size) => <SelectItem key={size} value={String(size)}>{size} 条/页</SelectItem>)}</SelectContent>
            </Select>
            <form className="flex items-center gap-1" onSubmit={(event) => { event.preventDefault(); const target = Number(jumpPage); if (Number.isInteger(target) && target >= 1 && target <= pageCount) { setPage(target); setJumpPage(""); } }}>
              <Input type="number" min={1} max={pageCount} value={jumpPage} onChange={(event) => setJumpPage(event.target.value)} placeholder="页码" aria-label="跳转页码" className="h-9 w-20" />
              <Button type="submit" variant="outline" className="h-9 rounded-lg" disabled={isLoading || !jumpPage || !Number.isInteger(Number(jumpPage)) || Number(jumpPage) < 1 || Number(jumpPage) > pageCount}>跳转</Button>
            </form>
            <Button variant="outline" size="icon" className="size-9 rounded-lg border-stone-200 bg-white" disabled={isLoading || safePage <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>
              <ChevronLeft className="size-4" />
            </Button>
            <Button variant="outline" size="icon" className="size-9 rounded-lg border-stone-200 bg-white" disabled={isLoading || safePage >= pageCount} onClick={() => setPage((value) => Math.min(pageCount, value + 1))}>
              <ChevronRight className="size-4" />
            </Button>
          </div>
          {isLoading ? <div className="flex items-center justify-center gap-2 px-6 py-14 text-sm text-stone-400"><LoaderCircle className="size-4 animate-spin" />正在加载日志…</div> : null}
          {!isLoading && items.length === 0 ? <div className="px-6 py-14 text-center text-sm text-stone-500">没有找到日志</div> : null}
        </CardContent>
      </Card>
      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="flex h-[min(88vh,860px)] w-[min(92vw,920px)] flex-col overflow-hidden rounded-2xl p-0">
          <DialogHeader className="shrink-0 border-b border-stone-100 px-6 py-5">
            <DialogTitle>日志详情</DialogTitle>
          </DialogHeader>
          <div className="flex-1 overflow-y-auto px-6 py-5">
            <div className="space-y-4">
              {isLoadingDetail ? <div className="text-sm text-stone-400">正在加载日志详情...</div> : null}
              <div className="grid gap-3 rounded-xl border border-stone-200 bg-white p-4 text-sm text-stone-600 md:grid-cols-2">
                {Object.entries(detailLog?.detail || {})
                  .filter(([key, value]) => !["urls", "request_text", "duration_ms", "request_body", "response_body"].includes(key) && typeof value !== "object")
                  .map(([key, value]) => (
                    <div key={key} className="flex items-start justify-between gap-4">
                      <span className="text-stone-400">{key}</span>
                      <span className="text-right font-medium break-all text-stone-700">{key === "status" && detailLog ? getStatus(detailLog) : String(value)}</span>
                    </div>
                  ))}
              </div>
              <ImagePerformance diagnostics={detailData.diagnostics} />
              {diagnosticSteps.length > 0 ? (
                <section className="space-y-3 rounded-xl border border-stone-200 bg-white p-4">
                  <div>
                    <h3 className="text-sm font-semibold text-stone-700">图片请求诊断时间线</h3>
                    <p className="mt-1 text-xs text-stone-500">展示阶段、相对开始时间和阶段耗时；阶段可能重叠，账号、请求体、下载地址和图片正文不会写入这里。</p>
                  </div>
                  <div className="overflow-x-auto rounded-lg border border-stone-200">
                    <Table className="min-w-[900px]">
                      <TableHeader>
                        <TableRow>
                          <TableHead>时间</TableHead>
                          <TableHead>阶段</TableHead>
                          <TableHead>图片 / 尝试</TableHead>
                          <TableHead>账号</TableHead>
                          <TableHead>开始偏移</TableHead>
                          <TableHead>耗时</TableHead>
                          <TableHead>状态</TableHead>
                          <TableHead>原因</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {diagnosticSteps.map((step, index) => {
                          const offset = Number(step.offset_ms);
                          const duration = Number(step.duration_ms);
                          const imageIndex = Number(step.image_index);
                          const attempt = Number(step.attempt);
                          const reason = diagnosticReasonLabel(step);
                          return (
                            <TableRow key={`${String(step.id ?? index)}-${index}`}>
                              <TableCell className="whitespace-nowrap text-xs">{formatDiagnosticTime(step.started_at)}</TableCell>
                              <TableCell className="font-medium text-stone-800">{diagnosticPhaseLabel(step.phase)}</TableCell>
                              <TableCell className="whitespace-nowrap">{Number.isFinite(imageIndex) ? imageIndex + 1 : "-"} / {Number.isFinite(attempt) ? attempt : "-"}</TableCell>
                              <TableCell className="max-w-[180px] break-all text-xs">{String(step.account || "-")}</TableCell>
                              <TableCell className="whitespace-nowrap">{Number.isFinite(offset) ? `${(offset / 1000).toFixed(3)} 秒` : "-"}</TableCell>
                              <TableCell className="whitespace-nowrap">{Number.isFinite(duration) ? `${(duration / 1000).toFixed(3)} 秒` : "-"}</TableCell>
                              <TableCell>{diagnosticStatusLabel(step)}</TableCell>
                              <TableCell className="max-w-[260px] break-all text-xs text-stone-500">{reason || "-"}</TableCell>
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </div>
                  {typeof (detailData.diagnostics as Record<string, unknown> | undefined)?.dropped_steps === "number"
                    && Number((detailData.diagnostics as Record<string, unknown>).dropped_steps) > 0 ? (
                    <p className="text-xs text-amber-700">时间线因达到上限而省略 {String((detailData.diagnostics as Record<string, unknown>).dropped_steps)} 个步骤。</p>
                  ) : null}
                </section>
              ) : null}
              {detailLog?.type === LogType.Call ? (
                <section className="space-y-3 rounded-xl border border-stone-200 bg-white p-4">
                  <div className="flex flex-wrap items-baseline justify-between gap-2">
                    <div>
                      <h3 className="text-sm font-semibold text-stone-700">切号分析</h3>
                      <p className="mt-1 text-xs text-stone-500">用于判断总耗时是否主要消耗在账号切换和各账号尝试上。</p>
                    </div>
                    <div className="flex flex-wrap gap-2 text-xs">
                      <Badge variant={switchCount > 0 ? "warning" : "secondary"} className="rounded-md">切号 {switchCount} 次</Badge>
                      <Badge variant="secondary" className="rounded-md">账号耗时 {(accountTimeMs / 1000).toFixed(2)} s</Badge>
                      <Badge variant="secondary" className="rounded-md">总耗时 {(totalDurationMs / 1000).toFixed(2)} s</Badge>
                      <Badge variant="secondary" className="rounded-md">账号耗时占比 {accountTimeShare.toFixed(1)}%</Badge>
                    </div>
                  </div>
                  {accountSwitchTrace.length > 0 ? (
                    <div className="overflow-x-auto rounded-lg border border-stone-200">
                      <Table className="min-w-[760px]">
                        <TableHeader>
                          <TableRow>
                            <TableHead>图片</TableHead>
                            <TableHead>账号</TableHead>
                            <TableHead>开始时间</TableHead>
                            <TableHead>结束时间</TableHead>
                            <TableHead>使用时长</TableHead>
                            <TableHead>后续账号</TableHead>
                            <TableHead>结束原因</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {accountSwitchTrace.map((attempt, index) => {
                            const next = accountSwitchTrace[index + 1];
                            const duration = Number(attempt.duration_ms || 0);
                            return (
                              <TableRow key={`${String(attempt.image_index)}-${index}`}>
                                <TableCell>{Number(attempt.image_index || 0) + 1}</TableCell>
                                <TableCell className="font-medium text-stone-800">{String(attempt.account_name || "未知账号")}</TableCell>
                                <TableCell className="whitespace-nowrap text-xs">{String(attempt.started_at || "-")}</TableCell>
                                <TableCell className="whitespace-nowrap text-xs">{String(attempt.ended_at || "-")}</TableCell>
                                <TableCell>{(Number.isFinite(duration) ? duration / 1000 : 0).toFixed(2)} s</TableCell>
                                <TableCell className="font-medium text-sky-700">{next ? `-> ${String(next.account_name || "未知账号")}` : "生成完成"}</TableCell>
                                <TableCell className="max-w-[260px] text-xs text-stone-500">{String(attempt.reason || "-")}</TableCell>
                              </TableRow>
                            );
                          })}
                        </TableBody>
                      </Table>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-stone-200 px-3 py-4 text-sm text-stone-400">没有记录到账号尝试轨迹。</div>
                  )}
                </section>
              ) : null}
              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-stone-700">请求体</h3>
                <div className="max-h-[34vh] overflow-auto rounded-md border border-stone-200 bg-stone-50 p-4 font-mono text-xs leading-6 text-stone-700">
                  {!hasRequestBody ? <span className="text-stone-400">该日志没有保存请求体</span> : <JsonValue value={requestBody} path="request" expanded={expandedJsonValues} onToggle={toggleJsonValue} />}
                </div>
              </section>
              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-stone-700">响应体</h3>
                <div className="max-h-[34vh] overflow-auto rounded-md border border-stone-200 bg-stone-50 p-4 font-mono text-xs leading-6 text-stone-700">
                  {!hasResponseBody ? <span className="text-stone-400">该日志没有保存响应体</span> : <JsonValue value={responseBody} path="response" expanded={expandedJsonValues} onToggle={toggleJsonValue} />}
                </div>
              </section>
            </div>
          </div>
        </DialogContent>
      </Dialog>
      <ImageLightbox
        images={detailImages}
        currentIndex={lightboxIndex}
        open={lightboxOpen}
        onOpenChange={setLightboxOpen}
        onIndexChange={setLightboxIndex}
      />
      <Dialog open={deletingItems.length > 0} onOpenChange={(open) => (!open ? setDeletingItems([]) : null)}>
        <DialogContent showCloseButton={false} className="rounded-2xl p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>{deletingItems.length === 1 ? "删除日志" : "删除所选日志"}</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              确认删除 {deletingItems.length} 条日志吗？删除后无法恢复。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" className="rounded-xl" onClick={() => setDeletingItems([])} disabled={isDeleting}>
              取消
            </Button>
            <Button className="rounded-xl bg-rose-600 text-white hover:bg-rose-700" onClick={() => void confirmDelete()} disabled={isDeleting || deletingItems.length === 0}>
              {isDeleting ? <LoaderCircle className="size-4 animate-spin" /> : null}
              确认删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  );
}

export default function LogsPage() {
  const { isCheckingAuth, session } = useAuthGuard(["admin"]);
  if (isCheckingAuth || !session || session.role !== "admin") {
    return <div className="flex min-h-[40vh] items-center justify-center"><LoaderCircle className="size-5 animate-spin text-stone-400" /></div>;
  }
  return <LogsContent />;
}
