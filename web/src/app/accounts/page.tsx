"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ComponentProps } from "react";
import {
  Ban,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  CircleOff,
  Copy,
  Download,
  Link2,
  LoaderCircle,
  LogIn,
  Pencil,
  RefreshCw,
  Search,
  Trash2,
  UserRound,
} from "lucide-react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  deleteAccounts,
  fetchAccountsPage,
  fetchAccountTokens,
  type AccountSummary,
  fetchModels,
  fetchRefreshProgress,
  fetchReLoginProgress,
  reLoginAccounts,
  refreshAccounts,
  testProxy,
  updateAccount,
  type Account,
  type AccountRefreshResponse,
  type AccountStatus,
  type Model,
  type RefreshProgressResponse,
} from "@/lib/api";
import { useAuthGuard } from "@/lib/use-auth-guard";
import { cn } from "@/lib/utils";

import { AccountImportDialog } from "./components/account-import-dialog";

const accountStatusOptions: { label: string; value: AccountStatus | "all" }[] = [
  { label: "全部状态", value: "all" },
  { label: "正常", value: "正常" },
  { label: "限流", value: "限流" },
  { label: "异常", value: "异常" },
  { label: "禁用", value: "禁用" },
];

const statusMeta: Record<
  AccountStatus,
  {
    icon: typeof CheckCircle2;
    badge: ComponentProps<typeof Badge>["variant"];
  }
> = {
  正常: { icon: CheckCircle2, badge: "success" },
  限流: { icon: CircleAlert, badge: "warning" },
  异常: { icon: CircleOff, badge: "danger" },
  禁用: { icon: Ban, badge: "secondary" },
};

const metricCards = [
  { key: "total", label: "账户总数", color: "text-stone-900", icon: UserRound },
  { key: "active", label: "正常账户", color: "text-emerald-600", icon: CheckCircle2 },
  { key: "limited", label: "限流账户", color: "text-orange-500", icon: CircleAlert },
  { key: "abnormal", label: "异常账户", color: "text-rose-500", icon: CircleOff },
  { key: "disabled", label: "禁用账户", color: "text-stone-500", icon: Ban },
  { key: "quota", label: "剩余额度", color: "text-blue-500", icon: RefreshCw },
] as const;

function formatCompact(value: number) {
  if (value >= 1000) {
    return `${(value / 1000).toFixed(1)}k`;
  }
  return String(value);
}

function formatQuota(account: Account) {
  return String(Math.max(0, account.quota));
}

function formatAccountTimestamp(value?: string | null) {
  if (!value) return "";
  const normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(value)
    ? value.replace(" ", "T") + "Z"
    : value;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString("zh-CN", { hour12: false });
}

function getRefreshTokenMeta(account: Account) {
  const status = account.refresh_token_status ?? (account.has_refresh_token ? "unverified" : "missing");
  const refreshedAt = formatAccountTimestamp(account.last_token_refresh_at);
  const errorAt = formatAccountTimestamp(account.last_token_refresh_error_at);
  const error = account.last_token_refresh_error?.trim();
  const errorDetail = [errorAt ? "最近刷新：" + errorAt : "", error || "暂无刷新错误详情"]
    .filter(Boolean)
    .join("\n");

  switch (status) {
    case "valid":
      return {
        label: "RT有效",
        className: "bg-emerald-50 text-emerald-700",
        title: refreshedAt ? "最近一次 RT 刷新成功：" + refreshedAt : "最近一次 RT 刷新成功",
      };
    case "invalid":
      return {
        label: "RT失效",
        className: "bg-rose-50 text-rose-700",
        title: "最近一次刷新确认 RT 失效\n" + errorDetail,
      };
    case "pending":
      return {
        label: "RT待确认",
        className: "bg-amber-50 text-amber-700",
        title: "最近一次刷新失败，但错误不足以确认 RT 失效\n" + errorDetail,
      };
    case "unverified":
      return {
        label: "RT未验证",
        className: "bg-stone-100 text-stone-600",
        title: "账号包含 RT，但还没有可用于判断有效性的刷新记录",
      };
    default:
      return {
        label: "无 RT",
        className: "bg-stone-100 text-stone-500",
        title: "账号未包含 refresh_token",
      };
  }
}

function formatRestoreAt(value?: string | null) {
  if (!value) {
    return { absolute: "—", relative: "" };
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return { absolute: value, relative: "" };
  }

  const diffMs = Math.max(0, date.getTime() - Date.now());
  const totalHours = Math.ceil(diffMs / (1000 * 60 * 60));
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  const relative = diffMs > 0 ? `剩余 ${days}d ${hours}h` : "已到恢复时间";

  const pad = (num: number) => String(num).padStart(2, "0");
  const absolute = `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(
    date.getHours(),
  )}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;

  return { absolute, relative };
}

function maskToken(token?: string) {
  if (!token) return "—";
  if (token.length <= 18) return token;
  return `${token.slice(0, 16)}...${token.slice(-8)}`;
}

function downloadTokens(tokens: string[]) {
  const content = `${tokens.join("\n")}\n`;
  const blob = new Blob([content], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `accounts-${Date.now()}.txt`;
  link.click();
  URL.revokeObjectURL(url);
}

function displayAccountType(account: Account) {
  return account.type || "Free";
}

function displayAccountSource(account: Account) {
  const source = String(account.source_type || "").trim().toLowerCase();
  if (!source) {
    return "web";
  }
  if (source === "web") {
    return "web";
  }
  return source;
}

function AccountsPageContent() {
  const requestIdRef = useRef(0);
  const mountedRef = useRef(true);
  const selectedAccountsRef = useRef(new Map<string, Account>());
  const [total, setTotal] = useState(0);
  const [types, setTypes] = useState<string[]>([]);
  const [poolSummary, setPoolSummary] = useState<AccountSummary>({ total: 0, active: 0, limited: 0, abnormal: 0, disabled: 0, quota: 0 });
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [availableModels, setAvailableModels] = useState<Model[]>([]);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState<AccountStatus | "all">("all");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState("10");
  const [editingAccount, setEditingAccount] = useState<Account | null>(null);
  const [editStatus, setEditStatus] = useState<AccountStatus>("正常");
  const [editProxy, setEditProxy] = useState("");
  const [isTestingProxy, setIsTestingProxy] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingModels, setIsLoadingModels] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isUpdating, setIsUpdating] = useState(false);
  const [isRelogining, setIsRelogining] = useState(false);
  const [progress, setProgress] = useState<{
    visible: boolean;
    current: number;
    total: number;
    message: string;
    email: string;
  }>({
    visible: false,
    current: 0,
    total: 0,
    message: "",
    email: "",
  });

  const loadAccounts = useCallback(async (silent = false, signal?: AbortSignal) => {
    const requestId = ++requestIdRef.current;
    if (!silent) setIsLoading(true);
    try {
      const data = await fetchAccountsPage({ page, pageSize: Number(pageSize), query: debouncedQuery, accountType: typeFilter, status: statusFilter }, signal);
      if (signal?.aborted || requestId !== requestIdRef.current) return;
      setAccounts(data.items);
      for (const item of data.items) {
        if (selectedAccountsRef.current.has(item.access_token)) selectedAccountsRef.current.set(item.access_token, item);
      }
      setTotal(data.total);
      setTypes(data.types);
      setPoolSummary(data.summary);
      if (page !== data.page) setPage(data.page);
    } catch (error) {
      if (signal?.aborted || requestId !== requestIdRef.current) return;
      toast.error(error instanceof Error ? error.message : "加载账户失败");
    } finally {
      if (!signal?.aborted && requestId === requestIdRef.current) setIsLoading(false);
    }
  }, [page, pageSize, debouncedQuery, typeFilter, statusFilter]);

  const latestLoadAccountsRef = useRef(loadAccounts);
  useEffect(() => { latestLoadAccountsRef.current = loadAccounts; }, [loadAccounts]);

  useEffect(() => {
    const timer = setTimeout(() => { setDebouncedQuery(query.trim()); setPage(1); }, 300);
    return () => clearTimeout(timer);
  }, [query]);
  useEffect(() => {
    const controller = new AbortController();
    void loadAccounts(false, controller.signal);
    return () => controller.abort();
  }, [loadAccounts]);

  const loadModels = async () => {
    setIsLoadingModels(true);
    try {
      const data = await fetchModels();
      setAvailableModels(Array.isArray(data.data) ? data.data : []);
    } catch (error) {
      const message = error instanceof Error ? error.message : "加载模型列表失败";
      toast.error(message);
    } finally {
      setIsLoadingModels(false);
    }
  };

  useEffect(() => {
    mountedRef.current = true;
    void loadModels();
    return () => { mountedRef.current = false; requestIdRef.current++; };
  }, []);

  const pageCount = Math.max(1, Math.ceil(total / Number(pageSize)));
  const safePage = Math.min(page, pageCount);
  const startIndex = (safePage - 1) * Number(pageSize);
  const currentRows = accounts;
  const allCurrentSelected = currentRows.length > 0 && currentRows.every((row) => selectedIds.includes(row.access_token));
  const summary = { ...poolSummary, quota: formatCompact(poolSummary.quota) };
  const accountTypeOptions = [{ label: "全部类型", value: "all" }, ...types.map((type) => ({ label: type, value: type }))];

  const selectedTokens = selectedIds;

  const paginationItems = useMemo(() => {
    const items: (number | "...")[] = [];
    const start = Math.max(1, safePage - 1);
    const end = Math.min(pageCount, safePage + 1);

    if (start > 1) items.push(1);
    if (start > 2) items.push("...");
    for (let current = start; current <= end; current += 1) items.push(current);
    if (end < pageCount - 1) items.push("...");
    if (end < pageCount) items.push(pageCount);

    return items;
  }, [pageCount, safePage]);

  const handleDeleteTokens = async (tokens: string[]) => {
    if (tokens.length === 0) {
      toast.error("请先选择要删除的账户");
      return;
    }

    setIsDeleting(true);
    try {
      const data = await deleteAccounts(tokens, false);
      selectedAccountsRef.current.clear();
      setSelectedIds([]);
      await latestLoadAccountsRef.current(true);
      toast.success(`删除 ${data.removed ?? 0} 个账户`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "删除账户失败";
      toast.error(message);
    } finally {
      setIsDeleting(false);
    }
  };

  const handleRefreshAccounts = async (accessTokens: string[] | null) => {
    const all = accessTokens === null;
    if (!all && !accessTokens.length) { toast.error("没有需要刷新的账户"); return; }
    setIsRefreshing(true);
    const count = all ? poolSummary.total : accessTokens.length;
    setProgress({ visible: true, current: 0, total: count, message: "正在刷新账号信息...", email: "" });
    try {
      const { progress_id } = await refreshAccounts(accessTokens ?? []);
      let result: AccountRefreshResponse | null | undefined;
      let lastSummaryRefresh = Date.now();
      for (;;) {
        if (!mountedRef.current) return;
        const p = await fetchRefreshProgress(progress_id, false);
        setProgress((prev) => ({ ...prev, current: p.processed, total: p.total }));
        if (p.error) throw new Error(p.error);
        if (p.done) { result = p.result; break; }
        if (Date.now() - lastSummaryRefresh > 4000) { await latestLoadAccountsRef.current(true); lastSummaryRefresh = Date.now(); }
        await new Promise((resolve) => setTimeout(resolve, 800));
      }
      if (!result) throw new Error("刷新结果为空");
      await latestLoadAccountsRef.current(true);
      const errors = result.errors ?? [];
      if (errors.length) toast.error(`刷新成功 ${result.refreshed} 个，失败 ${errors.length} 个，首个错误：${errors[0].error}`);
      else toast.success(`刷新成功 ${result.refreshed} 个账户`);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "刷新账户失败");
    } finally {
      setIsRefreshing(false);
      setProgress((prev) => ({ ...prev, visible: false }));
    }
  };

  const handleReLogin = async (accessTokens: string[]) => {
    const targets = accessTokens.filter((token) => selectedAccountsRef.current.get(token)?.status === "异常");
    if (!targets.length) { toast.error("选中账号中没有异常账号"); return; }
    setIsRelogining(true);
    setProgress({ visible: true, current: 0, total: targets.length, message: "正在尝试恢复异常账号...", email: "" });
    try {
      const { progress_id } = await reLoginAccounts(targets);
      let lastSummaryRefresh = Date.now();
      for (;;) {
        if (!mountedRef.current) return;
        const p = await fetchReLoginProgress(progress_id);
        setProgress((prev) => ({ ...prev, current: p.processed, total: p.total }));
        if (p.error) throw new Error(p.error);
        if (p.done) break;
        if (Date.now() - lastSummaryRefresh > 4000) { await latestLoadAccountsRef.current(true); lastSummaryRefresh = Date.now(); }
        await new Promise((resolve) => setTimeout(resolve, 800));
      }
      await latestLoadAccountsRef.current(true);
      toast.success("恢复流程已完成");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "恢复账户失败");
    } finally {
      setIsRelogining(false);
      setProgress((prev) => ({ ...prev, visible: false }));
    }
  };

  const handleDeleteAbnormal = async () => {
    try { await handleDeleteTokens((await fetchAccountTokens("异常")).tokens); }
    catch (error) { toast.error(error instanceof Error ? error.message : "加载异常账号失败"); }
  };
  const handleExportAllTokens = async () => {
    try { downloadTokens((await fetchAccountTokens()).tokens); }
    catch (error) { toast.error(error instanceof Error ? error.message : "导出失败"); }
  };

  const openEditDialog = (account: Account) => {
    setEditingAccount(account);
    setEditStatus(account.status);
    setEditProxy(account.proxy ?? "");
  };

  const handleTestAccountProxy = async () => {
    const candidate = editProxy.trim();
    if (!candidate) {
      toast.error("请先填写代理地址");
      return;
    }
    setIsTestingProxy(true);
    try {
      const data = await testProxy(candidate);
      data.result.ok
        ? toast.success(`代理可用（${data.result.latency_ms} ms，HTTP ${data.result.status}）`)
        : toast.error(`代理不可用：${data.result.error ?? "未知错误"}`);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "测试代理失败");
    } finally {
      setIsTestingProxy(false);
    }
  };

  const handleUpdateAccount = async () => {
    if (!editingAccount) {
      return;
    }

    setIsUpdating(true);
    try {
      await updateAccount(editingAccount.access_token, {
        status: editStatus,
        proxy: editProxy.trim(),
      });
      selectedAccountsRef.current.clear();
      setSelectedIds([]);
      await latestLoadAccountsRef.current(true);
      setEditingAccount(null);
      toast.success("账号信息已更新");
    } catch (error) {
      const message = error instanceof Error ? error.message : "更新账号失败";
      toast.error(message);
    } finally {
      setIsUpdating(false);
    }
  };

  const toggleSelectAll = (checked: boolean) => {
    if (checked) {
      currentRows.forEach((item) => selectedAccountsRef.current.set(item.access_token, item));
      setSelectedIds((prev) => Array.from(new Set([...prev, ...currentRows.map((item) => item.access_token)])));
      return;
    }
    currentRows.forEach((item) => selectedAccountsRef.current.delete(item.access_token));
    setSelectedIds((prev) => prev.filter((id) => !currentRows.some((row) => row.access_token === id)));
  };

  return (
    <>
      <section className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-1">
          <div className="text-xs font-semibold tracking-[0.18em] text-stone-500 uppercase">
            Account Pool
          </div>
          <h1 className="text-2xl font-semibold tracking-tight">号池管理</h1>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            className="h-10 rounded-xl border-stone-200 bg-white/80 px-4 text-stone-700 hover:bg-white"
            onClick={() => void loadAccounts()}
            disabled={isLoading || isRefreshing || isDeleting}
          >
            <RefreshCw className={cn("size-4", isLoading ? "animate-spin" : "")} />
            刷新
          </Button>
          <Button
            variant="outline"
            className="h-10 rounded-xl border-stone-200 bg-white/80 px-4 text-stone-700 hover:bg-white"
            onClick={() => void handleRefreshAccounts(null)}
            disabled={isLoading || isRefreshing || isDeleting || accounts.length === 0}
          >
            <RefreshCw className={cn("size-4", isRefreshing ? "animate-spin" : "")} />
            一键刷新所有账号信息和额度
          </Button>
          <AccountImportDialog
            disabled={isLoading || isRefreshing || isDeleting}
            onImported={() => {
              selectedAccountsRef.current.clear();
              setSelectedIds([]);
              if (page !== 1) setPage(1);
              else void loadAccounts(true);
            }}
          />
          <Button
            variant="outline"
            className="h-10 rounded-xl border-stone-200 bg-white/80 px-4 text-stone-700 hover:bg-white"
            onClick={() => void handleExportAllTokens()}
            disabled={accounts.length === 0}
          >
            <Download className="size-4" />
            导出全部 Token
          </Button>
        </div>
      </section>

      {/* 进度条 */}
      {progress.visible && (
        <div className="overflow-hidden rounded-2xl border border-stone-200 bg-white/90 shadow-sm">
          <div className="px-4 py-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-stone-600">
                {progress.message}
                {progress.email && <span className="ml-1 font-medium text-stone-700">{progress.email}</span>}
              </span>
              <span className="font-medium text-stone-700">
                {progress.current}/{progress.total}
              </span>
            </div>
            <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-stone-100">
              <div
                className="h-full rounded-full bg-gradient-to-r from-amber-400 to-orange-500 transition-all duration-300 ease-out"
                style={{ width: `${progress.total > 0 ? (progress.current / progress.total) * 100 : 0}%` }}
              />
            </div>
          </div>
        </div>
      )}

      <Dialog open={Boolean(editingAccount)} onOpenChange={(open) => (!open ? setEditingAccount(null) : null)}>
        <DialogContent showCloseButton={false} className="rounded-2xl p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>编辑账户</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              手动修改账号状态和专属代理。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700">状态</label>
              <Select value={editStatus} onValueChange={(value) => setEditStatus(value as AccountStatus)}>
                <SelectTrigger className="h-11 rounded-xl border-stone-200 bg-white">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {accountStatusOptions
                    .filter((option) => option.value !== "all")
                    .map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700">账号代理</label>
              <div className="flex flex-col gap-2 sm:flex-row">
                <Input
                  value={editProxy}
                  onChange={(event) => setEditProxy(event.target.value)}
                  placeholder="留空走全局代理，例如 http://127.0.0.1:7890"
                  className="h-11 rounded-xl border-stone-200 bg-white"
                />
                <Button
                  variant="outline"
                  className="h-11 rounded-xl border-stone-200 bg-white px-4 text-stone-700 sm:w-24"
                  onClick={() => void handleTestAccountProxy()}
                  disabled={isTestingProxy}
                >
                  {isTestingProxy ? <LoaderCircle className="size-4 animate-spin" /> : <Link2 className="size-4" />}
                  测试
                </Button>
              </div>
            </div>
          </div>
          <DialogFooter className="pt-2">
            <Button
              variant="secondary"
              className="h-10 rounded-xl bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => setEditingAccount(null)}
              disabled={isUpdating}
            >
              取消
            </Button>
            <Button
              className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
              onClick={() => void handleUpdateAccount()}
              disabled={isUpdating}
            >
              {isUpdating ? <LoaderCircle className="size-4 animate-spin" /> : null}
              保存修改
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <section className="space-y-3">
        <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
          {metricCards.map((item) => {
            const Icon = item.icon;
            const value = summary[item.key];
            return (
              <Card key={item.key} className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
                <CardContent className="p-4">
                  <div className="mb-4 flex items-start justify-between">
                    <span className="text-xs font-medium text-stone-400">{item.label}</span>
                    <Icon className="size-4 text-stone-400" />
                  </div>
                  <div className={cn("text-[1.75rem] font-semibold tracking-tight", item.color)}>
                    <span className={typeof value === "number" ? "" : "text-[1.1rem]"}>
                      {typeof value === "number" ? formatCompact(value) : value}
                    </span>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
        <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
          <CardContent className="p-4">
            <div className="mb-3 text-sm font-medium text-stone-700">
              系统可用模型
              <span className="ml-1 text-stone-400">({availableModels.length})</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {availableModels.length > 0 ? (
                availableModels.map((model) => (
                  <button
                    key={model.id}
                    type="button"
                    className="inline-flex cursor-pointer items-center rounded-full border border-stone-200 bg-white px-2.5 py-1 text-xs font-medium text-stone-700 transition hover:border-stone-300 hover:bg-stone-50"
                    onClick={() => {
                      void navigator.clipboard.writeText(model.id);
                      toast.success("模型名已复制");
                    }}
                    title={`点击复制 ${model.id}`}
                  >
                    <img
                      src="/openai.svg"
                      alt=""
                      aria-hidden="true"
                      className="mr-1.5 size-3.5 shrink-0"
                    />
                    {model.id}
                  </button>
                ))
              ) : isLoadingModels ? (
                <span className="text-sm text-stone-400">正在加载模型列表...</span>
              ) : (
                <span className="text-sm text-stone-400">当前暂无可用模型</span>
              )}
            </div>
          </CardContent>
        </Card>
      </section>

      <section className="space-y-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <h2 className="text-lg font-semibold tracking-tight">账户列表</h2>
            <Badge variant="secondary" className="rounded-lg bg-stone-200 px-2 py-0.5 text-stone-700">
              {total}
            </Badge>
          </div>

          <div className="flex flex-col gap-2 lg:flex-row lg:items-center">
            <div className="relative min-w-[260px]">
              <Search className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-stone-400" />
              <Input
                value={query}
                onChange={(event) => {
                  setQuery(event.target.value);
                  setPage(1);
                }}
                placeholder="搜索邮箱"
                className="h-10 rounded-xl border-stone-200 bg-white/85 pl-10"
              />
            </div>
            <Select
              value={typeFilter}
              onValueChange={(value) => {
                setTypeFilter(value);
                setPage(1);
              }}
            >
              <SelectTrigger className="h-10 w-full rounded-xl border-stone-200 bg-white/85 lg:w-[150px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {accountTypeOptions.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select
              value={statusFilter}
              onValueChange={(value) => {
                setStatusFilter(value as AccountStatus | "all");
                setPage(1);
              }}
            >
              <SelectTrigger className="h-10 w-full rounded-xl border-stone-200 bg-white/85 lg:w-[150px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {accountStatusOptions.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {isLoading && accounts.length === 0 ? (
          <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
            <CardContent className="flex flex-col items-center justify-center gap-3 px-6 py-14 text-center">
              <div className="rounded-xl bg-stone-100 p-3 text-stone-500">
                <LoaderCircle className="size-5 animate-spin" />
              </div>
              <div className="space-y-1">
                <p className="text-sm font-medium text-stone-700">正在加载账户</p>
                <p className="text-sm text-stone-500">从后端同步账号列表和状态。</p>
              </div>
            </CardContent>
          </Card>
        ) : null}

        <Card
          className={cn(
            "overflow-hidden rounded-2xl border-white/80 bg-white/90 shadow-sm",
            isLoading && accounts.length === 0 ? "hidden" : "",
          )}
        >
          <CardContent className="space-y-0 p-0">
            <div className="flex flex-col gap-3 border-b border-stone-100 px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex flex-wrap items-center gap-2 text-sm text-stone-500">
                <Button
                  variant="ghost"
                  className="h-8 rounded-lg px-3 text-stone-500 hover:bg-stone-100"
                  onClick={() => void handleRefreshAccounts(selectedTokens)}
                  disabled={selectedTokens.length === 0 || isRefreshing}
                >
                  {isRefreshing ? <LoaderCircle className="size-4 animate-spin" /> : <RefreshCw className="size-4" />}
                  刷新选中账号信息和额度
                </Button>
                <Button
                  variant="ghost"
                  className="h-8 rounded-lg px-3 text-amber-600 hover:bg-amber-50 hover:text-amber-700"
                  onClick={() => void handleReLogin(selectedTokens)}
                  disabled={selectedTokens.length === 0 || isRelogining}
                  title="尝试密码登录恢复账号"
                >
                  {isRelogining ? <LoaderCircle className="size-4 animate-spin" /> : <LogIn className="size-4" />}
                  尝试恢复异常账号
                </Button>
                <Button
                  variant="ghost"
                  className="h-8 rounded-lg px-3 text-rose-500 hover:bg-rose-50 hover:text-rose-600"
                  onClick={() => void handleDeleteAbnormal()}
                  disabled={poolSummary.abnormal === 0 || isDeleting}
                >
                  {isDeleting ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                  移除异常账号
                </Button>
                <Button
                  variant="ghost"
                  className="h-8 rounded-lg px-3 text-rose-500 hover:bg-rose-50 hover:text-rose-600"
                  onClick={() => void handleDeleteTokens(selectedTokens)}
                  disabled={selectedTokens.length === 0 || isDeleting}
                >
                  {isDeleting ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                  删除所选
                </Button>
                {selectedIds.length > 0 ? (
                  <span className="rounded-lg bg-stone-100 px-2.5 py-1 text-xs font-medium text-stone-600">
                    已选择 {selectedIds.length} 项
                  </span>
                ) : null}
              </div>
            </div>

            <div aria-busy={isLoading} className={cn("overflow-x-auto", isLoading && "pointer-events-none opacity-60")}>
              <table className="w-full min-w-[1000px] text-left">
                <thead className="border-b border-stone-100 text-[11px] text-stone-400 uppercase tracking-[0.18em]">
                  <tr>
                    <th className="w-12 px-4 py-3">
                      <Checkbox
                        checked={allCurrentSelected}
                        onCheckedChange={(checked) => toggleSelectAll(Boolean(checked))}
                      />
                    </th>
                    <th className="w-56 px-4 py-3">token</th>
                    <th className="w-28 px-4 py-3">类型</th>
                    <th className="w-24 px-4 py-3">来源</th>
                    <th className="w-24 px-4 py-3">状态</th>
                    <th className="w-28 px-4 py-3">RT状态</th>
                    <th className="w-56 px-4 py-3">账号信息</th>
                    <th className="w-32 px-4 py-3">创建时间</th>
                    <th className="w-24 px-4 py-3">额度</th>
                    <th className="w-40 px-4 py-3">恢复时间</th>
                    <th className="w-18 px-4 py-3">在途</th>
                    <th className="w-18 px-4 py-3">成功</th>
                    <th className="w-18 px-4 py-3">失败</th>
                    <th className="w-24 px-4 py-3">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {currentRows.map((account) => {
                    const status = statusMeta[account.status];
                    const StatusIcon = status.icon;

                    return (
                      <tr
                        key={account.access_token}
                        className="border-b border-stone-100/80 text-sm text-stone-600 transition-colors hover:bg-stone-50/70"
                      >
                        <td className="px-4 py-3">
                          <Checkbox
                            checked={selectedIds.includes(account.access_token)}
                            onCheckedChange={(checked) => {
                              if (checked) selectedAccountsRef.current.set(account.access_token, account);
                              else selectedAccountsRef.current.delete(account.access_token);
                              setSelectedIds((prev) =>
                                checked
                                  ? Array.from(new Set([...prev, account.access_token]))
                                  : prev.filter((item) => item !== account.access_token),
                              );
                            }}
                          />
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-2">
                            <span className="font-medium tracking-tight text-stone-700">
                              {maskToken(account.access_token)}
                            </span>
                            <button
                              type="button"
                              className="rounded-lg p-1 text-stone-400 transition hover:bg-stone-100 hover:text-stone-700"
                              onClick={() => {
                                void navigator.clipboard.writeText(account.access_token);
                                toast.success("token 已复制");
                              }}
                            >
                              <Copy className="size-4" />
                            </button>
                          </div>
                        </td>
                        <td className="px-4 py-3">
                          <Badge variant="secondary" className="rounded-md bg-stone-100 text-stone-700">
                            {displayAccountType(account)}
                          </Badge>
                        </td>
                        <td className="px-4 py-3">
                          <Badge variant="outline" className="rounded-md border-stone-200 text-stone-600">
                            {displayAccountSource(account)}
                          </Badge>
                        </td>
                        <td className="px-4 py-3">
                          <Badge
                            variant={status.badge}
                            className="inline-flex items-center gap-1 rounded-md px-2 py-1"
                          >
                            <StatusIcon className="size-3.5" />
                            {account.status}
                          </Badge>
                        </td>
                        <td className="px-4 py-3">
                          {(() => {
                            const refreshToken = getRefreshTokenMeta(account);
                            return (
                              <span
                                className={cn("inline-flex rounded-md px-2 py-1 text-xs font-medium", refreshToken.className)}
                                title={refreshToken.title}
                                aria-label={refreshToken.title}
                              >
                                {refreshToken.label}
                              </span>
                            );
                          })()}
                        </td>
                        <td className="px-4 py-3">
                          <div className="text-xs leading-5 text-stone-500">{account.email ?? "—"}</div>
                        </td>
                        <td className="px-4 py-3 text-xs leading-5 text-stone-500">
                          {(() => {
                            const raw = (account as any).created_at;
                            if (!raw) return "—";
                            try {
                              const d = new Date(raw + "Z");
                              if (isNaN(d.getTime())) return String(raw).slice(0, 10);
                              return d.toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
                            } catch { return String(raw).slice(0, 10); }
                          })()}
                        </td>
                        <td className="px-4 py-3">
                          <Badge variant="info" className="rounded-md">
                            {formatQuota(account)}
                          </Badge>
                        </td>
                        <td className="px-4 py-3 text-xs leading-5 text-stone-500">
                          {(() => {
                            const restore = formatRestoreAt(account.restore_at);
                            return (
                              <div className="space-y-0.5">
                                {restore.relative ? <div className="font-medium text-stone-700">{restore.relative}</div> : null}
                                <div>{restore.absolute}</div>
                              </div>
                            );
                          })()}
                        </td>
                        <td className="px-4 py-3">
                          {(() => {
                            const inflight = account.image_inflight ?? 0;
                            return (
                              <span
                                className={
                                  inflight > 0
                                    ? "font-semibold text-amber-600"
                                    : "text-stone-400"
                                }
                                title={
                                  inflight > 0
                                    ? "当前正在生成的图片数。号池空闲时此值持续 > 0，说明并发槽位泄漏、该账号已被静默排除出调度"
                                    : "当前无在途生图任务"
                                }
                              >
                                {inflight}
                              </span>
                            );
                          })()}
                        </td>
                        <td className="px-4 py-3 text-stone-500">{account.success}</td>
                        <td className="px-4 py-3 text-stone-500">{account.fail}</td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-1 text-stone-400">
                            <button
                              type="button"
                              className="rounded-lg p-2 transition hover:bg-stone-100 hover:text-stone-700"
                              onClick={() => openEditDialog(account)}
                              disabled={isUpdating}
                            >
                              <Pencil className="size-4" />
                            </button>
                            <button
                              type="button"
                              className="rounded-lg p-2 transition hover:bg-stone-100 hover:text-stone-700"
                              onClick={() => void handleRefreshAccounts([account.access_token])}
                              disabled={isRefreshing}
                            >
                              <RefreshCw className={cn("size-4", isRefreshing ? "animate-spin" : "")} />
                            </button>
                            <button
                              type="button"
                              className="rounded-lg p-2 transition hover:bg-rose-50 hover:text-rose-500"
                              onClick={() => void handleDeleteTokens([account.access_token])}
                              disabled={isDeleting}
                            >
                              <Trash2 className="size-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              {!isLoading && currentRows.length === 0 ? (
                <div className="flex flex-col items-center justify-center gap-3 px-6 py-14 text-center">
                  <div className="rounded-xl bg-stone-100 p-3 text-stone-500">
                    <Search className="size-5" />
                  </div>
                  <div className="space-y-1">
                    <p className="text-sm font-medium text-stone-700">没有匹配的账户</p>
                    <p className="text-sm text-stone-500">调整筛选条件或搜索关键字后重试。</p>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="border-t border-stone-100 px-4 py-4">
              <div className="flex items-center justify-center gap-3 overflow-x-auto whitespace-nowrap">
                <div className="shrink-0 text-sm text-stone-500">
                显示第 {total === 0 ? 0 : startIndex + 1} -{" "}
                {Math.min(startIndex + Number(pageSize), total)} 条，共{" "}
                {total} 条
                </div>

                <span className="shrink-0 text-sm leading-none text-stone-500">
                  {safePage} / {pageCount} 页
                </span>
                <Select
                  value={pageSize}
                  onValueChange={(value) => {
                    setPageSize(value);
                    setPage(1);
                  }}
                >
                  <SelectTrigger className="h-10 w-[108px] shrink-0 rounded-lg border-stone-200 bg-white text-sm leading-none">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="10">10 / 页</SelectItem>
                    <SelectItem value="20">20 / 页</SelectItem>
                    <SelectItem value="50">50 / 页</SelectItem>
                    <SelectItem value="100">100 / 页</SelectItem>
                  </SelectContent>
                </Select>
                <Button
                  variant="outline"
                  size="icon"
                  className="size-10 shrink-0 rounded-lg border-stone-200 bg-white"
                  disabled={isLoading || safePage <= 1}
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                >
                  <ChevronLeft className="size-4" />
                </Button>
                {paginationItems.map((item, index) =>
                  item === "..." ? (
                    <span key={`ellipsis-${index}`} className="px-1 text-sm text-stone-400">
                      ...
                    </span>
                  ) : (
                    <Button
                      key={item}
                      variant={item === safePage ? "default" : "outline"}
                      className={cn(
                        "h-10 min-w-10 shrink-0 rounded-lg px-3",
                        item === safePage
                          ? "bg-stone-950 text-white hover:bg-stone-800"
                          : "border-stone-200 bg-white text-stone-700",
                      )}
                      onClick={() => setPage(item)}
                    >
                      {item}
                    </Button>
                  ),
                )}
                <Button
                  variant="outline"
                  size="icon"
                  className="size-10 shrink-0 rounded-lg border-stone-200 bg-white"
                  disabled={isLoading || safePage >= pageCount}
                  onClick={() => setPage((prev) => Math.min(pageCount, prev + 1))}
                >
                  <ChevronRight className="size-4" />
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </section>
    </>
  );
}

export default function AccountsPage() {
  const { isCheckingAuth, session } = useAuthGuard(["admin"]);

  if (isCheckingAuth || !session || session.role !== "admin") {
    return (
      <div className="flex min-h-[40vh] items-center justify-center">
        <LoaderCircle className="size-5 animate-spin text-stone-400" />
      </div>
    );
  }

  return <AccountsPageContent />;
}
