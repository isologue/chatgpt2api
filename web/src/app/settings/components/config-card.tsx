"use client";

import { Cloud, LoaderCircle, PlugZap, RefreshCw, Save } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type { ImageStorageMode } from "@/lib/api";
import { testProxy, type ProxyTestResult } from "@/lib/api";

import { useSettingsStore } from "../store";

const verificationFields = [
  { key: "cache_ttl_secs", label: "可用性确认有效期 X（秒）", min: 0, fallback: 900, hint: "最近校验成功、有效出图或上游安全拒绝任一时间在 X 内，跳过信息校验；0 表示每次先校验。凭据检查始终保留。" },
  { key: "workers", label: "账号刷新并发 C（每条通道）", min: 1, fallback: 50, hint: "前台按需、热池补充、热池维护、冷池维护、手动刷新各自拥有 C；C=50 时最多同时刷新 250 个不同账号。同账号跨通道去重，不代表生图并发。" },
  { key: "hot_pool_size", label: "基础热池账号数", min: 0, fallback: 500, hint: "健康热池的补位目标，不是上限。热池无空闲槽位时按实际需求扩张；0 表示不自动保底补位。" },
  { key: "hot_idle_secs", label: "热池闲置退出 Y（秒）", min: 1, fallback: 86400, hint: "从最后实际承接生图开始计时；后台刷新不算使用。在途账号不退出，正常缩容不低于基础目标。" },
  { key: "hot_refresh_interval_secs", label: "热池维护间隔（秒）", min: 1, fallback: 1800, hint: "按账号最近可用性确认时间计算到期，近期成功出图/安全拒绝者无需重复刷新。重启不重置已有确认时间。" },
  { key: "cold_refresh_interval_secs", label: "冷池维护间隔（秒）", min: 1, fallback: 86400, hint: "非热池账号低频错峰维护。没有历史确认的冷账号分散到一个周期内，避免启动刷新风暴。" },
  { key: "failure_backoff_secs", label: "校验失败退避基数（秒）", min: 1, fallback: 30, hint: "同一账号连续校验失败指数退避，最长一小时，不限制其他账号的正常并发。" },
  { key: "wait_timeout_secs", label: "校验/槽位等待上限（秒）", min: 1, fallback: 60, hint: "限制前台排队和等待结果的时间；超时不会取消或重复启动正在进行的同账号校验。" },
] as const;

export function ConfigCard() {
  const [isTestingProxy, setIsTestingProxy] = useState(false);
  const [proxyTestResult, setProxyTestResult] = useState<ProxyTestResult | null>(null);
  const logLevelOptions = ["debug", "info", "warning", "error"];
  const config = useSettingsStore((state) => state.config);
  const isLoadingConfig = useSettingsStore((state) => state.isLoadingConfig);
  const isSavingConfig = useSettingsStore((state) => state.isSavingConfig);
  const setRefreshAccountIntervalMinute = useSettingsStore((state) => state.setRefreshAccountIntervalMinute);
  const setAnyioThreadLimit = useSettingsStore((state) => state.setAnyioThreadLimit);
  const setImageResponseFormat = useSettingsStore((state) => state.setImageResponseFormat);
  const setImageAllowResponseFormatOverride = useSettingsStore((state) => state.setImageAllowResponseFormatOverride);
  const setImageResponseWorkers = useSettingsStore((state) => state.setImageResponseWorkers);
  const setImageAccountVerificationField = useSettingsStore((state) => state.setImageAccountVerificationField);
  const setImageWorkers = useSettingsStore((state) => state.setImageWorkers);
  const setImageQueueSize = useSettingsStore((state) => state.setImageQueueSize);
  const setImageAccountSelectionMaxAttempts = useSettingsStore((state) => state.setImageAccountSelectionMaxAttempts);
  const setImagePollTimeoutAccountSwitchRetries = useSettingsStore((state) => state.setImagePollTimeoutAccountSwitchRetries);
  const setImageTextReplyAccountSwitchRetries = useSettingsStore((state) => state.setImageTextReplyAccountSwitchRetries);
  const setMonitoringRefreshIntervalSecs = useSettingsStore((state) => state.setMonitoringRefreshIntervalSecs);
  const setImageSseTimeoutSecs = useSettingsStore((state) => state.setImageSseTimeoutSecs);
  const setImagePollTimeoutSecs = useSettingsStore((state) => state.setImagePollTimeoutSecs);
  const setImageTotalTimeoutSecs = useSettingsStore((state) => state.setImageTotalTimeoutSecs);
  const setImageDownloadTimeoutSecs = useSettingsStore((state) => state.setImageDownloadTimeoutSecs);
  const setImageDownloadRetries = useSettingsStore((state) => state.setImageDownloadRetries);
  const setImageAccountConcurrency = useSettingsStore((state) => state.setImageAccountConcurrency);
  const setImageParallelGeneration = useSettingsStore((state) => state.setImageParallelGeneration);
  const setImageSettleEnabled = useSettingsStore((state) => state.setImageSettleEnabled);
  const setImageRemoveConversationAfterResult = useSettingsStore((state) => state.setImageRemoveConversationAfterResult);
  const setImageRemoveConversationAlways = useSettingsStore((state) => state.setImageRemoveConversationAlways);
  const setImageSettleSecs = useSettingsStore((state) => state.setImageSettleSecs);
  const setImageTimeoutRetrySecs = useSettingsStore((state) => state.setImageTimeoutRetrySecs);
  const setAutoRemoveInvalidAccounts = useSettingsStore((state) => state.setAutoRemoveInvalidAccounts);
  const setAutoRemoveRateLimitedAccounts = useSettingsStore((state) => state.setAutoRemoveRateLimitedAccounts);
  const setAutoReloginAfterRefresh = useSettingsStore((state) => state.setAutoReloginAfterRefresh);
  const setLogLevel = useSettingsStore((state) => state.setLogLevel);
  const setProxy = useSettingsStore((state) => state.setProxy);
  const setBaseUrl = useSettingsStore((state) => state.setBaseUrl);
  const setGlobalSystemPrompt = useSettingsStore((state) => state.setGlobalSystemPrompt);
  const setDefaultUpstreamModelName = useSettingsStore((state) => state.setDefaultUpstreamModelName);
  const setDefaultThinkingEffort = useSettingsStore((state) => state.setDefaultThinkingEffort);
  const setSensitiveWordsText = useSettingsStore((state) => state.setSensitiveWordsText);
  const setAIReviewField = useSettingsStore((state) => state.setAIReviewField);
  const setImageStorageField = useSettingsStore((state) => state.setImageStorageField);
  const testImageStorage = useSettingsStore((state) => state.testImageStorage);
  const syncImagesToWebDAV = useSettingsStore((state) => state.syncImagesToWebDAV);
  const isTestingImageStorage = useSettingsStore((state) => state.isTestingImageStorage);
  const isSyncingImageStorage = useSettingsStore((state) => state.isSyncingImageStorage);
  const saveConfig = useSettingsStore((state) => state.saveConfig);

  const handleTestProxy = async () => {
    const candidate = String(config?.proxy || "").trim();
    if (!candidate) {
      toast.error("请先填写代理地址");
      return;
    }
    setIsTestingProxy(true);
    setProxyTestResult(null);
    try {
      const data = await testProxy(candidate);
      setProxyTestResult(data.result);
      if (data.result.ok) {
        toast.success(`代理可用（${data.result.latency_ms} ms，HTTP ${data.result.status}）`);
      } else {
        toast.error(`代理不可用：${data.result.error ?? "未知错误"}`);
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "测试代理失败");
    } finally {
      setIsTestingProxy(false);
    }
  };

  if (isLoadingConfig) {
    return (
      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="flex items-center justify-center p-10">
          <LoaderCircle className="size-5 animate-spin text-stone-400" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
      <CardContent className="space-y-4 p-6">
        <div className="rounded-xl border border-stone-200 bg-stone-50 px-4 py-3 text-sm leading-6 text-stone-600">
          管理员登录密钥继续从部署配置读取，不再在此页面展示；如需分发给其他人，请在下方创建普通用户密钥。
        </div>
        <div className="space-y-4 rounded-xl border border-stone-200 bg-stone-50/60 p-4">
          <h3 className="text-sm font-semibold text-stone-800">图片返回格式</h3>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">默认 response_format</label>
            <Select value={config?.image_response_format || "url"} onValueChange={(value) => setImageResponseFormat(value as "url" | "b64_json")}>
              <SelectTrigger className="w-full sm:w-72"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="url">URL 图片链接</SelectItem>
                <SelectItem value="b64_json">Base64（b64_json）</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <label className="flex items-start gap-3 text-sm text-stone-700">
            <Checkbox checked={config?.image_allow_response_format_override !== false} onCheckedChange={(checked) => setImageAllowResponseFormatOverride(checked === true)} />
            <span>允许调用方指定 response_format<span className="mt-1 block text-xs leading-5 text-stone-500">勾选后优先使用请求参数，兼容 base64、b64_json、b64 等写法；未传参数使用默认格式。取消勾选后统一使用系统默认格式。同步和异步均生效，已创建的异步任务保持创建时的格式。</span></span>
          </label>
        </div>
        <div className="rounded-xl border border-sky-200 bg-sky-50/60 p-4">
          <div className="mb-3">
            <h3 className="text-sm font-semibold text-stone-800">图片生成并发</h3>
            <p className="mt-1 text-xs leading-5 text-stone-600">
              控制图片任务同时执行和排队的数量。当前最多同时执行 {Number(config?.image_workers || 32)} 个任务，额外排队 {Number(config?.image_queue_size || 0)} 个；队列满后返回 429。保存后立即生效。
            </p>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <label className="text-sm text-stone-700">同时执行的图片任务</label>
              <Input
                type="number"
                min={1}
                max={10000}
                step={1}
                value={String(config?.image_workers ?? 32)}
                onChange={(event) => setImageWorkers(event.target.value)}
                placeholder="32"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs leading-5 text-stone-500">例如设置为 1000，表示最多允许 1000 个图片任务同时进入执行阶段。受服务器、账号数量和上游限制影响。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">图片任务等待队列</label>
              <Input
                type="number"
                min={0}
                max={10000}
                step={1}
                value={String(config?.image_queue_size ?? 128)}
                onChange={(event) => setImageQueueSize(event.target.value)}
                placeholder="128"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs leading-5 text-stone-500">图片任务执行名额全部占用后允许等待的任务数。设置为 0 表示没有等待队列，满载时直接返回 429。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">单账号图片并发</label>
              <Input
                type="number"
                min={1}
                max={10000}
                step={1}
                value={String(config?.image_account_concurrency ?? 1)}
                onChange={(event) => setImageAccountConcurrency(event.target.value)}
                placeholder="1"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs leading-5 text-stone-500">每个账号允许同时处理的图片请求数。账号数量少或设置为 1 时，会限制实际生图速度。</p>
            </div>
            <div className="space-y-2">
              <label className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3 text-sm text-stone-700">
                <Checkbox
                  checked={Boolean(config?.image_parallel_generation !== false)}
                  onCheckedChange={(checked) => setImageParallelGeneration(Boolean(checked))}
                />
                单次请求的多张图片并行生成
              </label>
              <p className="text-xs leading-5 text-stone-500">开启后，同一个请求中的多张图片会并行请求上游；关闭后会逐张生成。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">图片请求接入线程并发</label>
              <Input
                type="number"
                min={1}
                max={1000}
                step={1}
                value={String(config?.anyio_thread_limit ?? 40)}
                onChange={(event) => setAnyioThreadLimit(event.target.value)}
                placeholder="40"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs leading-5 text-stone-500">用于鉴权、参数处理、任务提交和日志等同步环节；不决定同时执行的生图任务数，默认 40，最大 1000。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">监控页刷新间隔（秒）</label>
              <Input
                type="number"
                min={1}
                max={60}
                step={1}
                value={String(config?.monitoring_refresh_interval_secs ?? 3)}
                onChange={(event) => setMonitoringRefreshIntervalSecs(event.target.value)}
                placeholder="3"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs leading-5 text-stone-500">监控页自动拉取数据的间隔，范围 1 到 60 秒，保存后生效。</p>
            </div>
          </div>
          <div className="mt-4 space-y-4 rounded-xl border border-stone-200 bg-white/70 p-4">
            <div>
              <h4 className="text-sm font-semibold text-stone-800">账号校验与结果返回</h4>
              <p className="mt-1 text-xs leading-5 text-stone-600">动态热池与按需刷新。确认时间、热池身份和评分随账号持久化；过期必须先校验再生成，不再放行过期缓存。各通道采用同一 C、独立槽位，保存后生效。</p>
            </div>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {verificationFields.slice(0, 6).map((field) => (
                <div className="space-y-2" key={field.key}>
                  <label className="text-sm text-stone-700" htmlFor={`verification-${field.key}`}>{field.label}</label>
                  <Input id={`verification-${field.key}`} type="number" min={field.min} step={1}
                    value={String(config?.image_account_verification?.[field.key] ?? field.fallback)}
                    onChange={(event) => setImageAccountVerificationField(field.key, event.target.value)}
                    className="h-10 rounded-xl border-stone-200 bg-white" />
                  <p className="text-xs leading-5 text-stone-500">{field.hint}</p>
                </div>
              ))}
              <div className="space-y-2">
                <label className="text-sm text-stone-700" htmlFor="image-response-workers">结果处理线程并发</label>
                <Input id="image-response-workers" type="number" min={1} max={128} step={1}
                  value={String(config?.image_response_workers ?? 16)} onChange={(event) => setImageResponseWorkers(event.target.value)}
                  className="h-10 rounded-xl border-stone-200 bg-white" />
                <p className="text-xs leading-5 text-stone-500">独立处理日志详情构造和响应编码，避免与图片接入、管理接口争抢通用线程。</p>
              </div>
            </div>
            <details className="text-sm text-stone-600">
              <summary className="cursor-pointer font-medium">高级：失败退避与等待保护</summary>
              <div className="mt-3 grid gap-4 md:grid-cols-2">
                {verificationFields.slice(6).map((field) => <div className="space-y-2" key={field.key}>
                  <label className="text-sm text-stone-700" htmlFor={`verification-${field.key}`}>{field.label}</label>
                  <Input id={`verification-${field.key}`} type="number" min={field.min} step={1}
                    value={String(config?.image_account_verification?.[field.key] ?? field.fallback)}
                    onChange={(event) => setImageAccountVerificationField(field.key, event.target.value)}
                    className="h-10 rounded-xl border-stone-200 bg-white" />
                  <p className="text-xs leading-5 text-stone-500">{field.hint}</p>
                </div>)}
              </div>
            </details>
          </div>
          <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50/70 p-4">
            <div className="mb-3">
              <h4 className="text-sm font-semibold text-stone-800">图片账号故障切换</h4>
              <p className="mt-1 text-xs leading-5 text-stone-600">
                统一控制生图遇到账号不可用、上游未返回图片或图片下载重试耗尽时的账号切换上限。各类错误触发的换号均受总尝试预算约束，分项上限不会相乘；保存后立即生效。
              </p>
            </div>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-2">
                <label className="text-sm text-stone-700">单张图账号总尝试上限</label>
                <Input
                  type="number"
                  min={1}
                  max={1000}
                  step={1}
                  value={String(config?.image_account_selection_max_attempts ?? 20)}
                  onChange={(event) => setImageAccountSelectionMaxAttempts(event.target.value)}
                  placeholder="20"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                />
                <p className="text-xs leading-5 text-stone-500">预检与生成共用的总预算：N 表示最多使用 N 个不同账号（最多切 N−1 次），不因生成重试重新计数。至少为 1，最大 1000。</p>
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">补查失败切号次数</label>
                <Input
                  type="number"
                  min={0}
                  max={100}
                  step={1}
                  value={String(config?.image_poll_timeout_account_switch_retries ?? 4)}
                  onChange={(event) => setImagePollTimeoutAccountSwitchRetries(event.target.value)}
                  placeholder="4"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                />
                <p className="text-xs leading-5 text-stone-500">图片结果补查超时或失败后，最多切换多少次账号重新生成；0 表示不切号。仍受账号尝试上限和请求总预算约束。</p>
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">未生成图片切号次数</label>
                <Input
                  type="number"
                  min={0}
                  max={100}
                  step={1}
                  value={String(config?.image_text_reply_account_switch_retries ?? 3)}
                  onChange={(event) => setImageTextReplyAccountSwitchRetries(event.target.value)}
                  placeholder="3"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                />
                <p className="text-xs leading-5 text-stone-500">上游返回文本但没有图片时最多切换多少次账号，设置为 0 表示不因该原因切号。</p>
              </div>
            </div>
          </div>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <label className="text-sm text-stone-700">AT/RT 凭据维护检查间隔</label>
            <Input
              value={String(config?.refresh_account_interval_minute || "")}
              onChange={(event) => setRefreshAccountIntervalMinute(event.target.value)}
              placeholder="分钟"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">单位分钟，仅检查临期 AT 和 RT 保活；账号信息刷新使用上方热池/冷池维护间隔，不再全量刷新。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">全局代理</label>
            <Input
              value={String(config?.proxy || "")}
              onChange={(event) => {
                setProxy(event.target.value);
                setProxyTestResult(null);
              }}
              placeholder="http://127.0.0.1:7890"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs leading-5 text-stone-500">
              留空表示不使用代理。支持协议://账号:密码@主机:端口，也可直接粘贴代理商的 主机:端口:账号:密码；示例 http://user:pass@127.0.0.1:7890、127.0.0.1:7890:user:pass。账号密码含 @/: 等特殊字符时需 URL 编码。
            </p>
            {proxyTestResult ? (
              <div
                className={`rounded-xl border px-3 py-2 text-xs leading-6 ${
                  proxyTestResult.ok
                    ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                    : "border-rose-200 bg-rose-50 text-rose-800"
                }`}
              >
                {proxyTestResult.ok
                  ? `代理可用：HTTP ${proxyTestResult.status}，用时 ${proxyTestResult.latency_ms} ms`
                  : `代理不可用：${proxyTestResult.error ?? "未知错误"}（用时 ${proxyTestResult.latency_ms} ms）`}
              </div>
            ) : null}
            <div className="flex justify-end">
              <Button
                type="button"
                variant="outline"
                className="h-9 rounded-xl border-stone-200 bg-white px-4 text-stone-700"
                onClick={() => void handleTestProxy()}
                disabled={isTestingProxy}
              >
                {isTestingProxy ? <LoaderCircle className="size-4 animate-spin" /> : <PlugZap className="size-4" />}
                测试代理
              </Button>
            </div>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">图片访问地址</label>
            <Input
              value={String(config?.base_url || "")}
              onChange={(event) => setBaseUrl(event.target.value)}
              placeholder="https://example.com"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">用于生成图片结果的访问前缀地址。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">默认请求上游模型名称</label>
            <Input
              value={String(config?.default_upstream_model_name || "")}
              onChange={(event) => setDefaultUpstreamModelName(event.target.value)}
              placeholder="gpt-5-5"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">gpt-image-2 发起图片请求时使用的上游模型名称，默认 gpt-5-5。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">默认思考强度</label>
            <Select
              value={String(config?.default_thinking_effort || "auto")}
              onValueChange={(value) => setDefaultThinkingEffort(value as "auto" | "standard" | "extended" | "max")}
            >
              <SelectTrigger className="h-10 rounded-xl border-stone-200 bg-white">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="auto">Auto（不传递）</SelectItem>
                <SelectItem value="standard">Standard</SelectItem>
                <SelectItem value="extended">Extended</SelectItem>
                <SelectItem value="max">Max</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-stone-500">模型名称以 -standard、-extended 或 -max 结尾时，模型后缀优先。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">上游生成流超时（秒）</label>
            <Input
              value={String(config?.image_sse_timeout_secs || "")}
              onChange={(event) => setImageSseTimeoutSecs(event.target.value)}
              placeholder="80"
              inputMode="numeric"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">从提交上游生成请求计时，包含等待响应头和接收生成流；超时关闭连接并返回结果未确定，不自动切号。与结果补查分别计时，仍受请求总预算限制。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">图片结果补查超时（秒）</label>
            <Input
              value={String(config?.image_poll_timeout_secs || "")}
              onChange={(event) => setImagePollTimeoutSecs(event.target.value)}
              placeholder="10"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">从生成流结束后开始计时，任务检查、结果查询、等待和补查重试共用此上限；不会自动延长到 300 秒。获取下载地址后结束计时，图片下载单独计时。补查失败按「补查失败切号次数」重新选号生成，仍受请求总预算限制。</p>
          </div>
          <div className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2">
              <label className="text-sm text-stone-700">图片请求总预算（秒）</label>
              <Input
                value={String(config?.image_total_timeout_secs || "240")}
                onChange={(event) => setImageTotalTimeoutSecs(event.target.value)}
                inputMode="numeric"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs text-stone-500">包含排队、校验、切号、生成、轮询和下载；切号不会重新计时。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">单次图片下载超时（秒）</label>
              <Input
                value={String(config?.image_download_timeout_secs || "30")}
                onChange={(event) => setImageDownloadTimeoutSecs(event.target.value)}
                inputMode="numeric"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs text-stone-500">限制每次结果下载的时长；下载重试耗尽后可切号重新生图，仍受请求总预算约束。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">图片下载重试次数</label>
              <Input
                value={String(config?.image_download_retries ?? "1")}
                onChange={(event) => setImageDownloadRetries(event.target.value)}
                inputMode="numeric"
                className="h-10 rounded-xl border-stone-200 bg-white"
              />
              <p className="text-xs text-stone-500">先重试同一个结果地址；耗尽后按「单张图账号总尝试上限」切号重新生图，可能额外消耗额度。设为 0 仅关闭同地址重试。</p>
            </div>
          </div>
          <div className="space-y-2">
            <label className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3 text-sm text-stone-700">
              <Checkbox
                checked={Boolean(config?.auto_remove_invalid_accounts)}
                onCheckedChange={(checked) => setAutoRemoveInvalidAccounts(Boolean(checked))}
              />
              自动移除异常账号
            </label>
            <p className="text-xs text-stone-500">刷新时检测并移除</p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
              <Checkbox
                checked={Boolean(config?.image_settle_enabled !== false)}
                onCheckedChange={(checked) => setImageSettleEnabled(Boolean(checked))}
              />
              <span className="text-sm text-stone-700">图片二次确认机制</span>
            </div>
            <p className="text-xs text-stone-500">打开后能稍微提升获取图片的成功率。</p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
              <Checkbox
                checked={Boolean(config?.image_remove_conversation_after_result)}
                onCheckedChange={(checked) => setImageRemoveConversationAfterResult(Boolean(checked))}
              />
              <span className="text-sm text-stone-700">出图后移除本地对话</span>
            </div>
            <p className="text-xs text-stone-500">成功拿到图片后，异步隐藏 ChatGPT 侧对应的本地对话记录。</p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
              <Checkbox
                checked={Boolean(config?.image_remove_conversation_always)}
                onCheckedChange={(checked) => setImageRemoveConversationAlways(Boolean(checked))}
              />
              <span className="text-sm text-stone-700">没出图也移除本地对话</span>
            </div>
            <p className="text-xs text-stone-500">失败、超时或只返回文本时也一并隐藏对话记录（打开后包含出图成功的情况）。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">图片超时继续等待时间</label>
            <Input
              value={String(config?.image_timeout_retry_secs || "30")}
              onChange={(event) => setImageTimeoutRetrySecs(event.target.value)}
              placeholder="30"
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">单位秒，超时后点击"继续等待"额外等待的时间。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">图片二次确认等待时间</label>
            <Input
              value={String(config?.image_settle_secs || "2.0")}
              onChange={(event) => setImageSettleSecs(event.target.value)}
              placeholder="2.0"
              className="h-10 rounded-xl border-stone-200 bg-white disabled:cursor-not-allowed disabled:opacity-50"
              disabled={!config?.image_settle_enabled}
            />
            <p className="text-xs text-stone-500">单位秒，找到图片后等待多久再次确认。需配合图片二次确认机制使用。</p>
          </div>
          <div className="flex gap-4 md:col-span-2">
            <div className="flex-1 space-y-2">
              <label className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3 text-sm text-stone-700">
                <Checkbox
                  checked={Boolean(config?.auto_relogin_after_refresh)}
                  onCheckedChange={(checked) => setAutoReloginAfterRefresh(Boolean(checked))}
                />
                刷新后自动尝试移除异常状态
              </label>
              <p className="text-xs text-stone-500">开启后刷新时自动尝试密码登录恢复账号。</p>
            </div>
            <div className="flex-1" aria-hidden="true" />
          </div>
          <label className="flex items-center gap-3 rounded-xl border border-stone-200 bg-white px-4 py-3 text-sm text-stone-700">
            <Checkbox
              checked={Boolean(config?.auto_remove_rate_limited_accounts)}
              onCheckedChange={(checked) => setAutoRemoveRateLimitedAccounts(Boolean(checked))}
            />
            自动移除限流账号
          </label>
          <div className="space-y-3 rounded-xl border border-stone-200 bg-white px-4 py-3">
            <div>
              <label className="text-sm text-stone-700">控制台日志级别</label>
              <p className="mt-1 text-xs text-stone-500">不选择时使用默认 info / warning / error。</p>
            </div>
            <div className="grid grid-cols-2 gap-2">
              {logLevelOptions.map((level) => (
                <label key={level} className="flex items-center gap-2 text-sm capitalize text-stone-700">
                  <Checkbox
                    checked={Boolean(config?.log_levels?.includes(level))}
                    onCheckedChange={(checked) => setLogLevel(level, Boolean(checked))}
                  />
                  {level}
                </label>
              ))}
            </div>
          </div>
          <div className="space-y-2 md:col-span-2">
            <label className="text-sm text-stone-700">全局附加指令</label>
            <Textarea
              value={String(config?.global_system_prompt || "")}
              onChange={(event) => setGlobalSystemPrompt(event.target.value)}
              placeholder="例如：先判断用户提示词是否合规；遇到违法、色情、暴力、仇恨等请求时拒绝回答。"
              className="min-h-28 rounded-xl border-stone-200 bg-white font-mono text-xs shadow-none"
            />
            <p className="text-xs text-stone-500">每次请求都会作为 system 消息注入，可用于审核用户提示词、避免违规内容、统一约束模型行为或固定角色设定。</p>
          </div>
          <div className="space-y-2 md:col-span-2">
            <label className="text-sm text-stone-700">敏感词</label>
            <Textarea
              value={(config?.sensitive_words || []).join("\n")}
              onChange={(event) => setSensitiveWordsText(event.target.value)}
              placeholder="一行一个，命中即拒绝"
              className="min-h-28 rounded-xl border-stone-200 bg-white font-mono text-xs shadow-none"
            />
            <p className="text-xs text-stone-500">只要用户请求包含任意敏感词，就直接返回拒绝。</p>
          </div>
          <div className="space-y-4 rounded-xl border border-stone-200 bg-white px-4 py-3 md:col-span-2">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <label className="flex items-center gap-3 text-sm text-stone-700">
                <Checkbox
                  checked={Boolean(config?.image_storage?.enabled)}
                  onCheckedChange={(checked) => setImageStorageField("enabled", Boolean(checked))}
                />
                启用 WebDAV 图片存储
              </label>
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  variant="outline"
                  className="h-9 rounded-xl border-stone-200 bg-white px-4 text-stone-700"
                  onClick={() => void testImageStorage()}
                  disabled={isTestingImageStorage || !config?.image_storage?.enabled}
                >
                  {isTestingImageStorage ? <LoaderCircle className="size-4 animate-spin" /> : <Cloud className="size-4" />}
                  测试 WebDAV
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  className="h-9 rounded-xl border-stone-200 bg-white px-4 text-stone-700"
                  onClick={() => void syncImagesToWebDAV()}
                  disabled={isSyncingImageStorage || !config?.image_storage?.enabled || config?.image_storage?.mode === "local"}
                >
                  {isSyncingImageStorage ? <LoaderCircle className="size-4 animate-spin" /> : <RefreshCw className="size-4" />}
                  全量同步
                </Button>
              </div>
            </div>
            <p className="text-xs leading-6 text-stone-500">
              生成时只处理本次新图片；全量同步用于把已有本地图片补传到 WebDAV。
            </p>
            <div className="rounded-lg border border-stone-100 bg-stone-50 px-3 py-2 text-xs text-stone-600">
              当前待保存模式：
              <span className="ml-1 font-medium text-stone-900">
                {config?.image_storage?.enabled
                  ? config.image_storage.mode === "both"
                    ? "本机 + WebDAV"
                    : config.image_storage.mode === "webdav"
                      ? "仅 WebDAV"
                      : "仅本机"
                  : "仅本机"}
              </span>
              <span className="ml-2 text-stone-400">修改后需要点保存，或通过测试/同步按钮自动保存。</span>
            </div>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-2">
                <label className="text-sm text-stone-700">保存模式</label>
                <Select
                  value={String(config?.image_storage?.mode || "local")}
                  onValueChange={(value) => setImageStorageField("mode", value as ImageStorageMode)}
                  disabled={!config?.image_storage?.enabled}
                >
                  <SelectTrigger className="h-10 rounded-xl border-stone-200 bg-white shadow-none">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="local">仅本机</SelectItem>
                    <SelectItem value="webdav">仅 WebDAV</SelectItem>
                    <SelectItem value="both">本机 + WebDAV</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2 md:col-span-2">
                <label className="text-sm text-stone-700">WebDAV URL</label>
                <Input
                  value={String(config?.image_storage?.webdav_url || "")}
                  onChange={(event) => setImageStorageField("webdav_url", event.target.value)}
                  placeholder="https://example.com/dav"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                  disabled={!config?.image_storage?.enabled}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">用户名</label>
                <Input
                  value={String(config?.image_storage?.webdav_username || "")}
                  onChange={(event) => setImageStorageField("webdav_username", event.target.value)}
                  className="h-10 rounded-xl border-stone-200 bg-white"
                  disabled={!config?.image_storage?.enabled}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">密码</label>
                <Input
                  type="password"
                  value={String(config?.image_storage?.webdav_password || "")}
                  onChange={(event) => setImageStorageField("webdav_password", event.target.value)}
                  className="h-10 rounded-xl border-stone-200 bg-white"
                  disabled={!config?.image_storage?.enabled}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">远端目录</label>
                <Input
                  value={String(config?.image_storage?.webdav_root_path || "")}
                  onChange={(event) => setImageStorageField("webdav_root_path", event.target.value)}
                  placeholder="chatgpt2api/images"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                  disabled={!config?.image_storage?.enabled}
                />
              </div>
              <div className="space-y-2 md:col-span-3">
                <label className="text-sm text-stone-700">公开访问前缀</label>
                <Input
                  value={String(config?.image_storage?.public_base_url || "")}
                  onChange={(event) => setImageStorageField("public_base_url", event.target.value)}
                  placeholder="https://cdn.example.com/chatgpt2api/images"
                  className="h-10 rounded-xl border-stone-200 bg-white"
                  disabled={!config?.image_storage?.enabled}
                />
                <p className="text-xs text-stone-500">留空时返回本应用 /images/... 代理地址；填入后直接返回公开图片地址。</p>
              </div>
            </div>
          </div>
          <div className="space-y-4 rounded-xl border border-stone-200 bg-white px-4 py-3 md:col-span-2">
            <label className="flex items-center gap-3 text-sm text-stone-700">
              <Checkbox
                checked={Boolean(config?.ai_review?.enabled)}
                onCheckedChange={(checked) => setAIReviewField("enabled", Boolean(checked))}
              />
              启用 AI 审核
            </label>
            <p className="text-xs leading-6 text-stone-500">
              开启后会在请求进入生图账号前先调用审核模型，审核不通过会直接拒绝，减少违规提示词触达账号造成风控或封号的风险。
            </p>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="space-y-2">
                <label className="text-sm text-stone-700">Base URL</label>
                <Input value={String(config?.ai_review?.base_url || "")} onChange={(event) => setAIReviewField("base_url", event.target.value)} placeholder="https://api.openai.com" className="h-10 rounded-xl border-stone-200 bg-white" />
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">API Key</label>
                <Input value={String(config?.ai_review?.api_key || "")} onChange={(event) => setAIReviewField("api_key", event.target.value)} placeholder="sk-..." className="h-10 rounded-xl border-stone-200 bg-white" />
              </div>
              <div className="space-y-2">
                <label className="text-sm text-stone-700">Model</label>
                <Input value={String(config?.ai_review?.model || "")} onChange={(event) => setAIReviewField("model", event.target.value)} placeholder="gpt-5.4-mini" className="h-10 rounded-xl border-stone-200 bg-white" />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-stone-700">审核提示词</label>
              <Textarea value={String(config?.ai_review?.prompt || "")} onChange={(event) => setAIReviewField("prompt", event.target.value)} placeholder="判断用户请求是否允许。只回答 ALLOW 或 REJECT。" className="min-h-24 rounded-xl border-stone-200 bg-white text-xs shadow-none" />
            </div>
          </div>
        </div>

        <div className="flex justify-end">
          <Button
            className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
            onClick={() => void saveConfig()}
            disabled={isSavingConfig}
          >
            {isSavingConfig ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}
            保存
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
