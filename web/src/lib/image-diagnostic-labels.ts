const phaseLabels: Record<string, string> = {
  image_queue_wait: "图片任务排队", image_admission_wait: "等待图片执行名额",
  image_dispatch_wait: "分发图片任务", sse_transport: "生成流传输摘要", account_slot_wait: "等待账号槽位",
  account_foreground_verification: "按需校验账号", account_verification_wait: "等待账号校验",
  account_token_refresh: "刷新账号令牌", request_auth: "请求鉴权", request_filter: "请求内容检查",
  account_lookup: "获取可用账号", account_verification_queue: "账号校验排队",
  account_verification: "校验账号信息", access_token_refresh: "刷新访问令牌",
  reference_upload: "上传参考图片", upstream_bootstrap: "初始化上游会话",
  requirements_token: "获取生成凭证", pow_solve: "计算工作量证明",
  conversation_prepare: "准备生成对话", generation_submit: "提交生成请求",
  upstream_generation: "上游图片生成", sse_stream: "读取上游生成流",
  result_poll: "轮询图片结果", result_query: "查询图片结果", result_resolve: "补查并解析图片结果",
  retry_wait: "等待后续查询或重试", image_download: "下载图片",
  image_download_http: "建立图片下载请求", image_download_body: "接收图片数据",
  image_validate: "校验图片完整性", image_save: "保存图片", log_confirmation: "确认日志写入",
  other: "其他处理", response_serialization: "组织返回结果", decision: "重试与切号决策",
};

const statusLabels: Record<string, string> = {
  succeeded: "成功", success: "成功", failed: "失败", running: "进行中",
  recorded: "已记录", stopped: "已停止读取", cancelled: "已取消", skipped: "已跳过",
};
const actionLabels: Record<string, string> = {
  recover_original_result: "补查原对话图片", result_poll_failed: "图片结果补查失败",
  result_download_failed: "图片下载失败", account_switch: "切换账号",
  generation_result_uncertain: "生成结果尚未确定",
};
const reasonLabels: Record<string, string> = {
  retry_same_result_exhausted: "同一图片下载重试已用尽", image_download_failed: "图片下载失败",
  result_resolve_failed: "图片结果补查或解析失败",
  sse_hard_timeout: "上游生成流读取超时", result_poll_timeout: "图片结果补查超时",
};
const errorLabels: Record<string, string> = {
  GeneratorExit: "已结束读取并关闭生成流", ImageDeadlineExceeded: "图片请求总预算耗尽",
  ImagePollTimeoutError: "图片结果补查超时", ImageStreamHardTimeoutError: "上游生成流读取超时",
  ImageRequestCancelled: "调用方已取消请求", ImageContentPolicyError: "安全审核拒绝",
  ImageResultResolveError: "图片结果补查或解析失败",
  ImageDownloadError: "图片下载失败", ImageGenerationError: "图片生成失败",
  ImageStorageError: "图片保存失败", InvalidAccessTokenError: "账号访问令牌无效",
  UpstreamHTTPError: "上游请求返回异常", Timeout: "网络请求超时", TimeoutError: "操作超时",
  ConnectionError: "网络连接失败", RequestException: "网络请求异常",
  HTTPError: "接口请求异常", CurlError: "网络传输异常", RuntimeError: "执行异常",
  ValueError: "数据格式异常", OSError: "系统读写异常", CancelledError: "任务已取消",
};

export function diagnosticPhaseLabel(value: unknown): string {
  return typeof value === "string" && value ? phaseLabels[value] || "其他阶段" : "-";
}

export function diagnosticStatusLabel(step: Record<string, unknown>): string {
  // 兼容旧日志：生成器被关闭不代表上游生成失败。
  if (step.phase === "sse_stream" && step.error_type === "GeneratorExit") return "已停止读取";
  return typeof step.status === "string" ? statusLabels[step.status] || "未知状态" : "-";
}

export function diagnosticReasonLabel(step: Record<string, unknown>): string {
  if (step.phase === "sse_transport" && step.transport && typeof step.transport === "object") {
    const data = step.transport as Record<string, unknown>;
    const ms = (key: string) => `${Math.round(Number(data[key]) || 0)}ms`;
    const endings: Record<string, string> = { eof: "流结束", closed: "主动结束读取", error: "传输异常", body_limit: "正文超过上限" };
    return `响应头 ${ms("headers_ms")} · 首段 ${data.first_chunk_received ? ms("first_chunk_ms") : "未收到"} · 最后数据 ${ms("last_chunk_ms")} · 最长间隔 ${ms("max_gap_ms")} · 末段后等待 ${ms("idle_after_last_ms")} · ${Number(data.bytes) || 0}字节/${Number(data.chunks) || 0}段 · ${endings[String(data.termination)] || "未知结束"}`;
  }
  return ([['action', actionLabels], ['reason', reasonLabels], ['error_type', errorLabels]] as const)
    .flatMap(([key, labels]) => {
      const value = step[key];
      if (typeof value !== "string" || !value) return [];
      return [labels[value] || (/[\u3400-\u9fff]/.test(value) ? value : key === "error_type" ? "其他异常" : "其他原因")];
    }).join(" / ");
}

const performanceLabels: Record<string, string> = {
  http_headers: "等待并接收响应头", http_body: "响应头后至读取收尾", http_total: "HTTP 调用总耗时",
  curl_dns: "连接计时：域名解析完成", curl_connect: "连接计时：TCP 连接完成",
  curl_tls: "连接计时：TLS 握手完成", curl_first_byte: "连接计时：收到首字节",
  offload_queue: "工作线程提交及排队", offload_work: "工作线程执行",
  offload_resume: "工作线程完成后恢复等待", cpu_submit: "CPU 进程池提交",
  cpu_queue: "CPU 任务提交至子进程开始", cpu_work: "子进程计算墙钟时间",
  cpu_work_cpu: "子进程实际 CPU 时间", cpu_return: "子进程结果回传",
  cpu_resume: "子进程结果就绪后协程恢复等待",
  cpu_fallback: "CPU 任务回退本进程执行", sync_slice: "图片循环同步片段",
  sync_slice_cpu: "图片循环同步片段 CPU 时间",
};

export function performanceComponentLabel(value: unknown): string {
  return typeof value === "string" ? performanceLabels[value] || "其他性能项" : "其他性能项";
}
