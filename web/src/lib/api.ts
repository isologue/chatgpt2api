import { httpRequest, request } from "@/lib/request";

export type AccountType = string;
export type AccountStatus = "正常" | "限流" | "异常" | "禁用";
export type RefreshTokenStatus = "missing" | "unverified" | "pending" | "valid" | "invalid";
export type ImageModel = string;
export type AuthRole = "admin" | "user";
export type ImageStorageMode = "local" | "webdav" | "both";

export type ImageStorageSettings = {
  enabled: boolean;
  mode: ImageStorageMode;
  webdav_url: string;
  webdav_username: string;
  webdav_password: string;
  webdav_root_path: string;
  public_base_url: string;
};

export type Account = {
  access_token: string;
  type: AccountType;
  source_type?: string | null;
  status: AccountStatus;
  quota: number;
  email?: string | null;
  user_id?: string | null;
  limits_progress?: Array<{
    feature_name?: string;
    remaining?: number;
    reset_after?: string;
  }>;
  default_model_slug?: string | null;
  restore_at?: string | null;
  success: number;
  fail: number;
  /** 当前图片在途数(正在生成、尚未结束的图片数)。号池空闲时持续 > 0 表示并发槽位泄漏。 */
  image_inflight?: number;
  last_used_at?: string | null;
  has_refresh_token?: boolean;
  refresh_token_status?: RefreshTokenStatus;
  last_token_refresh_at?: string | null;
  last_token_refresh_error?: string | null;
  last_token_refresh_error_at?: string | null;
  proxy?: string | null;
};

export type AccountImportPayload = {
  access_token: string;
  accessToken?: string;
  type?: string;
  export_type?: string;
  source_type?: string;
  [key: string]: unknown;
};

export type Model = {
  id: string;
  object: string;
  created: number;
  owned_by: string;
  permission: unknown[];
  root: string;
  parent: string | null;
};

type AccountListResponse = {
  items: Account[];
};

type ModelListResponse = {
  object: string;
  data: Model[];
};

type AccountMutationResponse = {
  items?: Account[];
  added?: number;
  skipped?: number;
  removed?: number;
  refreshed?: number;
  relogined?: number;
  errors?: Array<{ access_token: string; error: string }>;
};

export type AccountRefreshResponse = {
  added?: number;
  skipped?: number;
  items?: Account[];
  refreshed: number;
  relogined?: number;
  errors: Array<{ access_token: string; error: string }>;
};

export type RefreshProgressResponse = {
  phase?: "saving" | "refreshing";
  total: number;
  processed: number;
  done: boolean;
  error: string | null;
  status_counts?: Record<string, number>;
  total_quota?: number;
  result?: AccountRefreshResponse | null;
  results?: Array<{ token: string; status: string; error?: string | null }>;
};

type AccountUpdateResponse = {
  item: Account;
  items?: Account[];
};

export type ProxyRuntimeEgressMode = "direct" | "single_proxy";
export type ProxyRuntimeClearanceMode = "none" | "manual" | "flaresolverr";

export type ProxyRuntimeClearanceSettings = {
  enabled: boolean;
  mode: ProxyRuntimeClearanceMode;
  cf_cookies: string;
  cf_clearance: string;
  user_agent: string;
  browser: string;
  flaresolverr_url: string;
  timeout_sec: number | string;
  refresh_interval: number | string;
  warm_up_on_start: boolean;
  has_cf_cookies?: boolean;
  has_cf_clearance?: boolean;
};

export type ProxyRuntimeSettings = {
  enabled: boolean;
  egress_mode: ProxyRuntimeEgressMode;
  proxy_url: string;
  resource_proxy_url: string;
  skip_ssl_verify: boolean;
  reset_session_status_codes: number[];
  clearance: ProxyRuntimeClearanceSettings;
};

export type ProxyRuntimeStatus = {
  enabled: boolean;
  egress_mode: ProxyRuntimeEgressMode | string;
  proxy_source: string;
  has_proxy: boolean;
  clearance_enabled: boolean;
  clearance_mode: ProxyRuntimeClearanceMode | string;
  has_clearance_bundle: boolean;
  cached_clearance_hosts: string[];
};

export type ProxyRuntimeResponse = {
  runtime: ProxyRuntimeSettings;
  status: ProxyRuntimeStatus;
};

export type ThirdPartyAppsSettings = {
  infinite_canvas: {
    enabled: boolean;
    url: string;
  };
};

export type ImageAccountVerificationSettings = {
  cache_ttl_secs: number | string;
  max_stale_secs: number | string;
  background_refresh: boolean;
  workers: number | string;
  queue_size: number | string;
  starts_per_second: number | string;
  failure_backoff_secs: number | string;
  query_workers: number | string;
};

export type SettingsConfig = {
  proxy: string;
  base_url?: string;
  global_system_prompt?: string;
  default_upstream_model_name?: string;
  default_thinking_effort?: "auto" | "standard" | "extended" | "max";
  sensitive_words?: string[];
  ai_review?: {
    enabled?: boolean;
    base_url?: string;
    api_key?: string;
    model?: string;
    prompt?: string;
  };
  refresh_account_interval_minute?: number | string;
  account_refresh_startup_mode?: "deferred" | "full";
  anyio_thread_limit?: number | string;
  image_response_format?: "url" | "b64_json";
  image_allow_response_format_override?: boolean;
  image_workers?: number | string;
  image_queue_size?: number | string;
  image_response_workers?: number | string;
  image_account_verification?: ImageAccountVerificationSettings;
  image_account_selection_max_attempts?: number | string;
  image_poll_timeout_account_switch_retries?: number | string;
  image_text_reply_account_switch_retries?: number | string;
  monitoring_refresh_interval_secs?: number | string;
  image_retention_days?: number | string;
  image_poll_timeout_secs?: number | string;
  image_account_concurrency?: number | string;
  image_parallel_generation?: boolean;
  image_settle_enabled?: boolean;
  image_check_before_hit_enabled?: boolean;
  image_remove_conversation_after_result?: boolean;
  image_remove_conversation_always?: boolean;
  image_settle_secs?: number | string;
  image_timeout_retry_secs?: number | string;
  auto_remove_invalid_accounts?: boolean;
  auto_remove_rate_limited_accounts?: boolean;
  auto_relogin_after_refresh?: boolean;
  log_levels?: string[];
  image_storage?: ImageStorageSettings;
  proxy_runtime?: ProxyRuntimeSettings;
  third_party_apps?: ThirdPartyAppsSettings;
  backup?: BackupSettings;
  backup_state?: BackupState;
  [key: string]: unknown;
};

export type BackupInclude = {
  config: boolean;
  cpa: boolean;
  sub2api: boolean;
  logs: boolean;
  image_tasks: boolean;
  accounts_snapshot: boolean;
  auth_keys_snapshot: boolean;
  images: boolean;
};

export type BackupSettings = {
  enabled: boolean;
  provider: "cloudflare_r2" | string;
  account_id: string;
  access_key_id: string;
  secret_access_key: string;
  bucket: string;
  prefix: string;
  interval_minutes: number | string;
  rotation_keep: number | string;
  encrypt: boolean;
  passphrase: string;
  include: BackupInclude;
};

export type BackupState = {
  running: boolean;
  last_started_at?: string | null;
  last_finished_at?: string | null;
  last_status?: string;
  last_error?: string | null;
  last_object_key?: string | null;
};

export type BackupItem = {
  key: string;
  name: string;
  size: number;
  updated_at?: string | null;
  encrypted: boolean;
};

export type BackupDetail = {
  key: string;
  name: string;
  encrypted: boolean;
  created_at?: string | null;
  trigger?: string | null;
  app_version?: string | null;
  storage_backend?: Record<string, unknown> | null;
  files: Array<{
    name: string;
    exists: boolean;
    content_type?: string;
    size: number;
    sha256?: string;
  }>;
  snapshots: Array<{
    name: string;
    count: number;
  }>;
};

export type ManagedImage = {
  rel: string;
  path?: string;
  name: string;
  date: string;
  size: number;
  url: string;
  thumbnail_url?: string;
  created_at: string;
  width?: number;
  height?: number;
  tags?: string[];
};

export type SystemLog = {
  id: string;
  time: string;
  type: "call" | "account" | string;
  summary?: string;
  detail?: Record<string, unknown>;
  [key: string]: unknown;
};

export type RuntimePhase = { active: number; completed: number; sample_count: number; p95_ms: number; last_ms: number; max_ms?: number };
export type PersistenceBatch = { items: number; bytes: number; succeeded: boolean; wall_ms: number; thread_cpu_ms: number; stages: Record<string, number> };
export type PersistenceQueue = {
  capacity: number; queued: number; pending: number; failed: number; last_queue_wait_ms: number; last_write_ms: number;
  active_items?: number; active_for_ms?: number; last_batch_items?: number; max_batch_items?: number;
  last_batch?: PersistenceBatch | null; slowest_batch?: PersistenceBatch | null;
};

export type AccountRefreshCounts = {
  total: number; completed: number; succeeded: number; failed: number; active: number; pending: number;
};
export type AccountRefreshFailures = {
  round: number; kind: "refresh" | "keepalive"; total: number; retained: number;
  max_retained: number; offset: number; limit: number;
  items: { account: string; error: string; time: number }[];
};
export type AccountRefreshSnapshot = {
  round: number;
  status: "idle" | "running" | "completed" | "completed_with_errors" | "failed";
  phase: "idle" | "preparing" | "refresh" | "keepalive" | "waiting";
  started_at: number | null; finished_at: number | null; last_success_at: number | null;
  last_full_refresh_finished_at: number | null; next_full_refresh_at: number | null;
  next_run_at: number | null; interval_seconds: number; elapsed_seconds: number;
  error: string | null;
  refresh: AccountRefreshCounts; keepalive: AccountRefreshCounts;
};

export type MonitoringSnapshot = {
  snapshot_state?: Record<string, { collected_at: number | null; age_ms: number; stale: boolean; refreshing: boolean; refresh_failed: boolean }>;
  account_refresh?: AccountRefreshSnapshot;
  phases?: Record<string, RuntimePhase>;
  account_verification?: { active: number; waiting: number; background_pending: number; background_capacity: number; cache_hit_rate: number; completed: number; failed: number; background_rejected: number };
  persistence?: { logs: PersistenceQueue; image_index: PersistenceQueue };
  completion_executor?: { active: number; pending: number; queued: number };
  timestamp: number;
  refresh_interval_secs: number;
  server_date: string;
  timezone: string;
  accounts: {
    total: number;
    available: number;
    unavailable: number;
    limited: number;
    abnormal: number;
    disabled: number;
  };
  image_executor: {
    workers: number;
    queue_size: number;
    capacity: number;
    inflight: number;
    queued: number;
    available: number;
    available_slots: number;
    remaining_queue_slots: number;
    full: boolean;
  };
  images: {
    success: number;
    failed: number;
    total: number;
    success_rate: number;
    success_images: number;
    excluded: number;
    duration_samples?: number;
    duration_p50_ms?: number | null;
    duration_p95_ms?: number | null;
    start_date: string;
    end_date: string;
  };
  system: {
    cpu_percent: number | null;
    memory: { used_bytes: number; total_bytes: number; percent: number };
    disk: { used_bytes: number; total_bytes: number; free_bytes: number; percent: number };
    load_average: number[];
  };
};

export type ImageResponse = {
  created: number;
  data: Array<{ b64_json?: string; url?: string; revised_prompt?: string }>;
};

export type ImageTask = {
  id: string;
  status: "queued" | "running" | "success" | "error";
  mode: "generate" | "edit";
  model?: ImageModel;
  size?: string;
  quality?: string;
  created_at: string;
  updated_at: string;
  conversation_id?: string;
  data?: Array<{ b64_json?: string; url?: string; revised_prompt?: string }>;
  error?: string;
  progress?: string;
  elapsed_secs?: number;
  duration_ms?: number;
};

type ImageTaskResponse =
  | { created_at: number; model: string; status: "queued"; task_id: string }
  | { task_id: string; status: "queued" | "processing"; progress: string }
  | {
      created_at: number;
      data: ImageResponse;
      model: string;
      progress: "100%";
      status: "succeeded";
      task_id: string;
    }
  | { task_id: string; status: "failed"; error: { message: string } };

function fromImageTaskResponse(task: ImageTaskResponse): ImageTask {
  const base = {
    id: task.task_id,
    mode: "generate" as const,
    model: "model" in task ? task.model as ImageModel : undefined,
    created_at: new Date(("created_at" in task ? task.created_at : Date.now() / 1000) * 1000).toISOString(),
    updated_at: new Date().toISOString(),
  };
  if (task.status === "failed") {
    return { ...base, status: "error", error: task.error.message };
  }
  if (task.status === "succeeded") {
    return { ...base, status: "success", data: task.data.data, progress: task.progress };
  }
  return {
    ...base,
    status: task.status === "processing" ? "running" : "queued",
    progress: "progress" in task ? task.progress : undefined,
  };
}

type ImageTaskListResponse = {
  items: ImageTask[];
  missing_ids: string[];
};

export type LoginResponse = {
  ok: boolean;
  version: string;
  role: AuthRole;
  subject_id: string;
  name: string;
};

export type UserKey = {
  id: string;
  name: string;
  role: "user";
  enabled: boolean;
  created_at: string | null;
  last_used_at: string | null;
};

export async function login(authKey: string) {
  const normalizedAuthKey = String(authKey || "").trim();
  return httpRequest<LoginResponse>("/auth/login", {
    method: "POST",
    body: {},
    headers: {
      Authorization: `Bearer ${normalizedAuthKey}`,
    },
    redirectOnUnauthorized: false,
  });
}

export type AccountSummary = {
  total: number; active: number; limited: number; abnormal: number; disabled: number; quota: number;
};
export type AccountPage = {
  items: Account[]; total: number; page: number; page_size: number;
  summary: AccountSummary; types: string[];
};
export async function fetchAccounts() {
  return httpRequest<AccountListResponse>("/api/accounts");
}
export async function fetchAccountsPage(options: {
  page: number; pageSize: number; query?: string; accountType?: string; status?: string;
}, signal?: AbortSignal) {
  const params = new URLSearchParams({
    page: String(options.page), page_size: String(options.pageSize),
    query: options.query ?? "", account_type: options.accountType ?? "all", status: options.status ?? "all",
  });
  return (await request.get<AccountPage>(`/api/accounts?${params}`, { signal, timeout: 30000 })).data;
}
export async function fetchAccountTokens(status = "all") {
  return httpRequest<{ tokens: string[] }>(`/api/accounts/tokens?status=${encodeURIComponent(status)}`);
}

export async function fetchModels() {
  return httpRequest<ModelListResponse>("/v1/models");
}

export async function createAccounts(tokens: string[], accounts: AccountImportPayload[] = []) {
  return httpRequest<AccountMutationResponse>("/api/accounts", {
    method: "POST",
    body: { tokens, accounts },
  });
}

export async function importAccounts(
  tokens: string[],
  accounts: AccountImportPayload[] = [],
  refreshAfterImport = true,
) {
  return httpRequest<{ progress_id: string }>("/api/accounts/import?include_items=false", {
    method: "POST",
    body: { tokens, accounts, refresh_after_import: refreshAfterImport },
  });
}

export type OAuthLoginStartResponse = {
  session_id: string;
  authorize_url: string;
  expires_in: string;
  redirect_uri_prefix: string;
};

export async function startOAuthLogin(emailHint?: string) {
  return httpRequest<OAuthLoginStartResponse>("/api/accounts/oauth/start", {
    method: "POST",
    body: { email_hint: emailHint ?? "" },
  });
}

export async function finishOAuthLogin(sessionId: string, callback: string) {
  return httpRequest<AccountMutationResponse>("/api/accounts/oauth/finish?include_items=false", {
    method: "POST",
    body: { session_id: sessionId, callback },
  });
}

export async function deleteAccounts(tokens: string[], includeItems = true) {
  return httpRequest<AccountMutationResponse>(`/api/accounts?include_items=${includeItems}`, {
    method: "DELETE",
    body: { tokens },
  });
}

export async function refreshAccounts(accessTokens: string[]) {
  return httpRequest<{ progress_id: string }>("/api/accounts/refresh?include_items=false", {
    method: "POST",
    body: { access_tokens: accessTokens },
  });
}

export async function fetchRefreshProgress(progressId: string, includeItems = true) {
  return httpRequest<RefreshProgressResponse>(`/api/accounts/refresh/progress/${progressId}?include_items=${includeItems}`);
}

export async function reLoginAccounts(accessTokens: string[]) {
  return httpRequest<{ progress_id: string }>("/api/accounts/re-login?include_items=false", {
    method: "POST",
    body: { access_tokens: accessTokens },
  });
}

export async function fetchReLoginProgress(progressId: string) {
  return httpRequest<RefreshProgressResponse>(`/api/accounts/re-login/progress/${progressId}?include_items=false`);
}

export async function updateAccount(
  accessToken: string,
  updates: {
    type?: AccountType;
    status?: AccountStatus;
    quota?: number;
    proxy?: string;
  },
) {
  return httpRequest<AccountUpdateResponse>("/api/accounts/update?include_items=false", {
    method: "POST",
    body: {
      access_token: accessToken,
      ...updates,
    },
  });
}

export async function generateImage(prompt: string, model?: ImageModel, size?: string, quality = "auto") {
  return httpRequest<ImageResponse>(
    "/v1/images/generations",
    {
      method: "POST",
      body: {
        prompt,
        ...(model ? { model } : {}),
        ...(size ? { size } : {}),
        quality,
        n: 1,
        response_format: "b64_json",
      },
    },
  );
}

export async function editImage(files: File | File[], prompt: string, model?: ImageModel, size?: string, quality = "auto") {
  const formData = new FormData();
  const uploadFiles = Array.isArray(files) ? files : [files];

  uploadFiles.forEach((file) => {
    formData.append("image", file);
  });
  formData.append("prompt", prompt);
  if (model) {
    formData.append("model", model);
  }
  if (size) {
    formData.append("size", size);
  }
  formData.append("quality", quality);
  formData.append("n", "1");

  return httpRequest<ImageResponse>(
    "/v1/images/edits",
    {
      method: "POST",
      body: formData,
    },
  );
}

export async function createImageGenerationTask(clientTaskId: string, prompt: string, model?: ImageModel, size?: string, quality = "auto") {
  const task = await httpRequest<ImageTaskResponse>("/v1/images/generations", {
    method: "POST",
    body: {
      async: true,
      client_task_id: clientTaskId,
      prompt,
      ...(model ? { model } : {}),
      ...(size ? { size } : {}),
      quality,
    },
  });
  return fromImageTaskResponse(task);
}

export async function createImageEditTask(
  clientTaskId: string,
  files: File | File[],
  prompt: string,
  model?: ImageModel,
  size?: string,
  quality = "auto",
) {
  const formData = new FormData();
  const uploadFiles = Array.isArray(files) ? files : [files];

  uploadFiles.forEach((file) => {
    formData.append("image", file);
  });
  formData.append("client_task_id", clientTaskId);
  formData.append("prompt", prompt);
  if (model) {
    formData.append("model", model);
  }
  if (size) {
    formData.append("size", size);
  }
  formData.append("quality", quality);
  formData.append("async", "true");

  const task = await httpRequest<ImageTaskResponse>("/v1/images/edits", {
    method: "POST",
    body: formData,
  });
  return fromImageTaskResponse(task);
}

export async function fetchImageTasks(ids: string[]) {
  const tasks = await Promise.all(ids.map(async (id) => {
    try {
      const task = await httpRequest<ImageTaskResponse>(`/v1/images/tasks/${encodeURIComponent(id)}?_t=${Date.now()}`);
      return fromImageTaskResponse(task);
    } catch (error) {
      if (error instanceof Error && error.message === "task not found") return null;
      throw error;
    }
  }));
  return { items: tasks.filter((task): task is ImageTask => task !== null), missing_ids: ids.filter((_, index) => tasks[index] === null) };
}

export async function resumeImagePoll(taskId: string, extraTimeoutSecs = 30) {
  return httpRequest<ImageTask>(`/api/image-tasks/${encodeURIComponent(taskId)}/resume-poll`, {
    method: "POST",
    body: { extra_timeout_secs: extraTimeoutSecs },
  });
}

export async function fetchSettingsConfig() {
  return httpRequest<{ config: SettingsConfig }>("/api/settings");
}

export async function updateSettingsConfig(settings: SettingsConfig) {
  return httpRequest<{ config: SettingsConfig }>("/api/settings", {
    method: "POST",
    body: settings,
  });
}

export async function fetchThirdPartyApps() {
  return httpRequest<{ third_party_apps: ThirdPartyAppsSettings }>("/api/third-party-apps");
}

export async function testBackupConnection() {
  return httpRequest<{ result: { ok: boolean; status: number } }>("/api/backup/test", {
    method: "POST",
    body: {},
  });
}

export async function testImageStorageConnection() {
  return httpRequest<{ result: { ok: boolean; status: number; error?: string } }>("/api/image-storage/test", {
    method: "POST",
    body: {},
  });
}

export async function syncImageStorage() {
  return httpRequest<{ result: { uploaded: number; skipped: number; failed: number } }>("/api/image-storage/sync", {
    method: "POST",
    body: {},
  });
}

export async function fetchBackups() {
  return httpRequest<{ items: BackupItem[]; state: BackupState; settings: BackupSettings }>("/api/backups");
}

export async function runBackupNow() {
  return httpRequest<{ result: { key: string; size: number; encrypted: boolean } }>("/api/backups/run", {
    method: "POST",
    body: {},
  });
}

export async function deleteBackup(key: string) {
  return httpRequest<{ ok: boolean }>("/api/backups/delete", {
    method: "POST",
    body: { key },
  });
}

export async function fetchBackupDetail(key: string) {
  const params = new URLSearchParams();
  params.set("key", key);
  return httpRequest<{ item: BackupDetail }>(`/api/backups/detail?${params.toString()}`);
}

export function getBackupDownloadUrl(key: string) {
  const params = new URLSearchParams();
  params.set("key", key);
  return `/api/backups/download?${params.toString()}`;
}

export async function fetchManagedImages(filters: { start_date?: string; end_date?: string }) {
  const params = new URLSearchParams();
  if (filters.start_date) params.set("start_date", filters.start_date);
  if (filters.end_date) params.set("end_date", filters.end_date);
  return httpRequest<{ items: ManagedImage[]; groups: Array<{ date: string; items: ManagedImage[] }> }>(
    `/api/images${params.toString() ? `?${params.toString()}` : ""}`,
  );
}

export async function deleteManagedImages(body: { paths?: string[]; start_date?: string; end_date?: string; all_matching?: boolean }) {
  return httpRequest<{ removed: number }>("/api/images/delete", { method: "POST", body });
}

export async function downloadImages(paths: string[]) {
  const response = await request.post("/api/images/download", { paths }, { responseType: "blob" });
  const blob = response.data as Blob;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "images.zip";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export async function downloadSingleImage(path: string) {
  const response = await request.get(`/api/images/download/${path}`, { responseType: "blob" });
  const blob = response.data as Blob;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = path.split("/").pop() || "image.png";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export async function fetchImageTags() {
  return httpRequest<{ tags: string[] }>("/api/images/tags");
}

export async function setImageTags(path: string, tags: string[]) {
  return httpRequest<{ ok: boolean; tags: string[] }>("/api/images/tags", {
    method: "POST",
    body: { path, tags },
  });
}

export async function deleteImageTag(tag: string) {
  return httpRequest<{ ok: boolean; removed_from: number }>(`/api/images/tags/${encodeURIComponent(tag)}`, {
    method: "DELETE",
  });
}

export type ImageStorageStats = {
  disk_total_mb: number; disk_used_mb: number; disk_free_mb: number;
  image_count: number; image_size_mb: number; image_size_bytes: number;
};

export async function fetchImageStorage() {
  return httpRequest<ImageStorageStats>("/api/images/storage");
}

export async function compressAllImages() {
  return httpRequest<{ compressed: number; saved_bytes: number; saved_mb: number }>("/api/images/storage/compress", { method: "POST" });
}

export async function deleteToTarget(targetFreeMb: number) {
  return httpRequest<{ removed: number; freed_mb: number; done: boolean }>(
    `/api/images/storage/cleanup-to-target?target_free_mb=${targetFreeMb}&dry_run=false`,
    { method: "POST" },
  );
}

export async function fetchSystemLogs(filters: {
  type?: string; start_date?: string; end_date?: string; key_name?: string;
  is_async?: boolean; response_format?: string; status?: string; page?: number; page_size?: number;
}, signal?: AbortSignal) {
  const params = new URLSearchParams();
  if (filters.type) params.set("type", filters.type);
  if (filters.start_date) params.set("start_date", filters.start_date);
  if (filters.end_date) params.set("end_date", filters.end_date);
  if (filters.key_name !== undefined) params.set("key_name", filters.key_name);
  if (filters.is_async !== undefined) params.set("is_async", String(filters.is_async));
  if (filters.response_format) params.set("response_format", filters.response_format);
  if (filters.status) params.set("status", filters.status);
  params.set("page", String(filters.page || 1));
  params.set("page_size", String(filters.page_size || 20));
  return (await request.get<{ items: SystemLog[]; total: number; page: number; page_size: number }>(
    `/api/logs?${params.toString()}`, { signal },
  )).data;
}

export async function fetchLogKeyNames(filters: { type: string; start_date: string; end_date: string; query: string }, signal?: AbortSignal) {
  const params = new URLSearchParams(filters);
  return (await request.get<{ key_names: string[] }>(`/api/logs/filter-options?${params.toString()}`, { signal })).data;
}

export async function fetchSystemLog(id: string) {
  return httpRequest<{ item: SystemLog }>(`/api/logs/${encodeURIComponent(id)}`);
}

export async function fetchAccountRefreshFailures(round: number, kind: "refresh" | "keepalive", offset = 0, limit = 20) {
  const query = new URLSearchParams({ round: String(round), kind, offset: String(offset), limit: String(limit) });
  return httpRequest<AccountRefreshFailures>(`/api/monitoring/account-refresh/failures?${query.toString()}`);
}

export async function fetchMonitoring(startDate?: string, endDate?: string) {
  const query = new URLSearchParams();
  if (startDate) query.set("start_date", startDate);
  if (endDate) query.set("end_date", endDate);
  return httpRequest<MonitoringSnapshot>(`/api/monitoring?${query.toString()}`);
}

export async function deleteSystemLogs(ids: string[]) {
  return httpRequest<{ removed: number }>("/api/logs/delete", {
    method: "POST",
    body: { ids },
  });
}

export async function fetchUserKeys() {
  return httpRequest<{ items: UserKey[] }>("/api/auth/users");
}

export async function createUserKey(name: string) {
  return httpRequest<{ item: UserKey; key: string; items: UserKey[] }>("/api/auth/users", {
    method: "POST",
    body: { name },
  });
}

export async function updateUserKey(keyId: string, updates: { enabled?: boolean; name?: string; key?: string }) {
  return httpRequest<{ item: UserKey; items: UserKey[] }>(`/api/auth/users/${keyId}`, {
    method: "POST",
    body: updates,
  });
}

export async function deleteUserKey(keyId: string) {
  return httpRequest<{ items: UserKey[] }>(`/api/auth/users/${keyId}`, {
    method: "DELETE",
  });
}

// ── CPA (CLIProxyAPI) ──────────────────────────────────────────────

export type CPAPool = {
  id: string;
  name: string;
  base_url: string;
  import_job?: CPAImportJob | null;
};

export type CPARemoteFile = {
  name: string;
  email: string;
};

export type CPAImportJob = {
  job_id: string;
  status: "pending" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  total: number;
  completed: number;
  added: number;
  skipped: number;
  refreshed: number;
  failed: number;
  errors: Array<{ name: string; error: string }>;
};

export async function fetchCPAPools() {
  return httpRequest<{ pools: CPAPool[] }>("/api/cpa/pools");
}

export async function createCPAPool(pool: { name: string; base_url: string; secret_key: string }) {
  return httpRequest<{ pool: CPAPool; pools: CPAPool[] }>("/api/cpa/pools", {
    method: "POST",
    body: pool,
  });
}

export async function updateCPAPool(
  poolId: string,
  updates: { name?: string; base_url?: string; secret_key?: string },
) {
  return httpRequest<{ pool: CPAPool; pools: CPAPool[] }>(`/api/cpa/pools/${poolId}`, {
    method: "POST",
    body: updates,
  });
}

export async function deleteCPAPool(poolId: string) {
  return httpRequest<{ pools: CPAPool[] }>(`/api/cpa/pools/${poolId}`, {
    method: "DELETE",
  });
}

export async function fetchCPAPoolFiles(poolId: string) {
  return httpRequest<{ pool_id: string; files: CPARemoteFile[] }>(`/api/cpa/pools/${poolId}/files`);
}

export async function startCPAImport(poolId: string, names: string[]) {
  return httpRequest<{ import_job: CPAImportJob | null }>(`/api/cpa/pools/${poolId}/import`, {
    method: "POST",
    body: { names },
  });
}

export async function fetchCPAPoolImportJob(poolId: string) {
  return httpRequest<{ import_job: CPAImportJob | null }>(`/api/cpa/pools/${poolId}/import`);
}

// ── Sub2API ────────────────────────────────────────────────────────

export type Sub2APIServer = {
  id: string;
  name: string;
  base_url: string;
  email: string;
  has_api_key: boolean;
  group_id: string;
  import_job?: CPAImportJob | null;
};

export type Sub2APIRemoteAccount = {
  id: string;
  name: string;
  email: string;
  plan_type: string;
  status: string;
  expires_at: string;
  has_refresh_token: boolean;
};

export type Sub2APIRemoteGroup = {
  id: string;
  name: string;
  description: string;
  platform: string;
  status: string;
  account_count: number;
  active_account_count: number;
};

export async function fetchSub2APIServers() {
  return httpRequest<{ servers: Sub2APIServer[] }>("/api/sub2api/servers");
}

export async function createSub2APIServer(server: {
  name: string;
  base_url: string;
  email: string;
  password: string;
  api_key: string;
  group_id: string;
}) {
  return httpRequest<{ server: Sub2APIServer; servers: Sub2APIServer[] }>("/api/sub2api/servers", {
    method: "POST",
    body: server,
  });
}

export async function updateSub2APIServer(
  serverId: string,
  updates: {
    name?: string;
    base_url?: string;
    email?: string;
    password?: string;
    api_key?: string;
    group_id?: string;
  },
) {
  return httpRequest<{ server: Sub2APIServer; servers: Sub2APIServer[] }>(`/api/sub2api/servers/${serverId}`, {
    method: "POST",
    body: updates,
  });
}

export async function fetchSub2APIServerGroups(serverId: string) {
  return httpRequest<{ server_id: string; groups: Sub2APIRemoteGroup[] }>(
    `/api/sub2api/servers/${serverId}/groups`,
  );
}

export async function deleteSub2APIServer(serverId: string) {
  return httpRequest<{ servers: Sub2APIServer[] }>(`/api/sub2api/servers/${serverId}`, {
    method: "DELETE",
  });
}

export async function fetchSub2APIServerAccounts(serverId: string) {
  return httpRequest<{ server_id: string; accounts: Sub2APIRemoteAccount[] }>(
    `/api/sub2api/servers/${serverId}/accounts`,
  );
}

export async function startSub2APIImport(serverId: string, accountIds: string[]) {
  return httpRequest<{ import_job: CPAImportJob | null }>(`/api/sub2api/servers/${serverId}/import`, {
    method: "POST",
    body: { account_ids: accountIds },
  });
}

export async function fetchSub2APIImportJob(serverId: string) {
  return httpRequest<{ import_job: CPAImportJob | null }>(`/api/sub2api/servers/${serverId}/import`);
}

// ── Upstream proxy ────────────────────────────────────────────────

export type ProxySettings = {
  enabled: boolean;
  url: string;
};

export type ProxyTestResult = {
  ok: boolean;
  status: number;
  latency_ms: number;
  error: string | null;
  proxy_source?: string;
  has_proxy?: boolean;
};

export type ClearanceTestResult = {
  ok: boolean;
  status: string;
  latency_ms: number;
  has_cookies: boolean;
  user_agent: string;
  error: string | null;
  runtime: ProxyRuntimeStatus;
};

export async function fetchProxy() {
  return httpRequest<{ proxy: ProxySettings }>("/api/proxy");
}

export async function updateProxy(updates: { enabled?: boolean; url?: string }) {
  return httpRequest<{ proxy: ProxySettings }>("/api/proxy", {
    method: "POST",
    body: updates,
  });
}

export async function testProxy(url?: string) {
  return httpRequest<{ result: ProxyTestResult }>("/api/proxy/test", {
    method: "POST",
    body: { url: url ?? "" },
  });
}

export async function fetchProxyRuntime() {
  return httpRequest<ProxyRuntimeResponse>("/api/proxy/runtime");
}

export async function updateProxyRuntime(runtime: ProxyRuntimeSettings) {
  return httpRequest<ProxyRuntimeResponse>("/api/proxy/runtime", {
    method: "POST",
    body: runtime,
  });
}

export async function testProxyClearance(targetUrl?: string) {
  return httpRequest<{ result: ClearanceTestResult }>("/api/proxy/clearance/test", {
    method: "POST",
    body: { target_url: targetUrl ?? "https://chatgpt.com" },
  });
}


export type RetentionKind = "images" | "logs";
export type RetentionPolicy = { enabled: boolean; retention_hours: number; interval_minutes: number };
export type RetentionStatus = {
  policy: RetentionPolicy;
  running: boolean;
  last_started_at: string | null;
  last_finished_at: string | null;
  next_run_at: string | null;
  cutoff?: string;
  retention_hours?: number;
  source?: "manual" | "automatic";
  last_result: { removed: number; failed?: number; reclaimed_bytes?: number; errors?: string[] } | null;
  last_error: string | null;
};

export async function fetchRetention(kind: RetentionKind, signal?: AbortSignal) {
  return (await request.get<RetentionStatus>(`/api/retention/${kind}`, { signal })).data;
}
export async function saveRetention(kind: RetentionKind, policy: RetentionPolicy) {
  return (await request.post<RetentionStatus>(`/api/retention/${kind}`, policy)).data;
}
export async function runRetention(kind: RetentionKind, retentionHours: number) {
  return (await request.post<RetentionStatus>(`/api/retention/${kind}/run`, { retention_hours: retentionHours })).data;
}


export type LogStorageStats = {
  disk_total_mb: number;
  disk_used_mb: number;
  disk_free_mb: number;
  log_count: number;
  log_size_bytes: number;
  log_file_size_bytes: number;
  log_index_size_bytes: number;
};

export async function fetchLogStorage(signal?: AbortSignal) {
  return (await request.get<LogStorageStats>("/api/logs/storage", { signal })).data;
}
