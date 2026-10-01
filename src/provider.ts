import { createHash, randomUUID } from 'node:crypto';

export type ExecStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'timed_out' | 'interrupted';

/** 服务端返回的环境选择结果：选了什么、为什么、是否降级；不暴露镜像/运行时/宿主细节。 */
export interface EffectiveEnvironment {
  profile_name?: string;
  /** 服务端生成的不透明 Profile revision。 */
  profile_revision?: string;
  selection_mode?: 'profile' | 'auto';
  /** 可追溯性线索，例如默认回退原因或未知/未满足的能力。 */
  selection_reason?: string[];
  capabilities?: string[];
  /** 仅当已批准的策略/运行时回退改变了请求的执行保证时为 true。 */
  degraded?: boolean;
}

export interface ExecSessionResult {
  exec_id?: string;
  status: ExecStatus;
  exit_code: number;
  stdout: string;
  stderr: string;
  error_code?: string;
  stdout_truncated?: boolean;
  stderr_truncated?: boolean;
  effective_environment?: EffectiveEnvironment;
  environment?: 'sandbox';
  session_id?: string;
  workspace_id?: string;
  sandbox_id?: string;
  cwd?: string;
}

export interface ExecutionRequest {
  operation_id: string;
  command?: string[];
  code?: string;
  language?: 'python' | 'javascript';
  working_dir?: string;
  env?: Record<string, string>;
  timeout_seconds?: number;
}

export interface ExecRecord {
  exec_id: string;
  operation_id: string;
  session_id: string;
  tenant_id: string;
  status: ExecStatus;
  exit_code: number;
  stdout?: string;
  stderr?: string;
  error_code?: string;
  stdout_truncated?: boolean;
  stderr_truncated?: boolean;
  logs_url?: string;
}

/** 可安全写入 Claw task/run 持久化存储的远端 Session 快照，不包含认证凭据。 */
export interface Session {
  session_id: string;
  workspace_id: string;
  tenant_id: string;
  user_id: string;
  /** 逻辑状态为 active 不代表 runtime 已存在；仅该绑定字段表示当前有 runtime。 */
  active_sandbox_id?: string;
  status?: string;
  expires_at: string;
}

export interface ExecutionProvider {
  execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecRecord>;
}

/** 返回当前可用的 Bearer 凭据；每个 HTTP 请求都会重新调用，便于短期凭据轮转。 */
export type AccessTokenProvider = (signal?: AbortSignal) => string | Promise<string>;

export interface RemoteOptions {
  endpoint: string;
  getAccessToken: AccessTokenProvider;
  tenantId: string;
  userId: string;
  fetch?: typeof fetch;
}

export interface ExecLogEvent {
  cursor: number;
  event: string;
  time?: string;
  message: string;
}

export interface StreamExecLogsOptions {
  /** 从该游标之后续读；值也会写入 Last-Event-ID。 */
  cursor?: number;
  signal?: AbortSignal;
  /** 网络中断后的最大自动重连次数，默认 5，最大 20。 */
  maxReconnects?: number;
}

export interface JobArtifact {
  artifact_id: string;
  tenant_id: string;
  workspace_id: string;
  job_id: string;
  name: string;
  path: string;
  size: number;
  sha256: string;
  mime?: string;
  created_at: string;
}

export interface ArtifactPage {
  items: JobArtifact[];
  total: number;
  nextOffset?: number;
}

export interface ArtifactDownload {
  content: Uint8Array;
  contentType: string;
  sha256: string;
}

export class SandboxApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'SandboxApiError';
    this.status = status;
  }
}

export class RemoteExecutionError extends Error {
  readonly session_id: string;
  readonly operation_id: string;
  readonly exec_id?: string;

  constructor(message: string, sessionId: string, operationId: string, execId?: string) {
    super(message);
    this.name = 'RemoteExecutionError';
    this.session_id = sessionId;
    this.operation_id = operationId;
    this.exec_id = execId;
  }
}

export class RemoteLogStreamError extends Error {
  readonly session_id: string;
  readonly exec_id: string;
  readonly cursor: number;

  constructor(sessionId: string, execId: string, cursor: number, message: string) {
    super(message);
    this.name = 'RemoteLogStreamError';
    this.session_id = sessionId;
    this.exec_id = execId;
    this.cursor = cursor;
  }
}

export class RemoteLogGapError extends Error {
  readonly session_id: string;
  readonly exec_id: string;
  readonly previous_cursor: number;
  readonly next_cursor: number;

  constructor(sessionId: string, execId: string, previousCursor: number, nextCursor: number) {
    super(`Sandbox log cursor gap after ${previousCursor}; next event is ${nextCursor}`);
    this.name = 'RemoteLogGapError';
    this.session_id = sessionId;
    this.exec_id = execId;
    this.previous_cursor = previousCursor;
    this.next_cursor = nextCursor;
  }
}

export class SandboxProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SandboxProtocolError';
  }
}

const JSON_RESPONSE_LIMIT = 4 * 1024 * 1024;
const MAX_ARTIFACT_RESPONSE_BYTES = 64 * 1024 * 1024;
const MAX_SSE_FRAME_CHARS = 1024 * 1024;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export class RemoteExecutionProvider implements ExecutionProvider {
  private readonly endpoint: string;
  private readonly getAccessToken: AccessTokenProvider;
  private readonly tenantId: string;
  private readonly userId: string;
  private readonly fetcher: typeof fetch;
  private session: Session | undefined;
  private initializing: Promise<Session> | undefined;

  constructor(options: RemoteOptions) {
    const url = new URL(options.endpoint);
    if (url.username || url.password || url.search || url.hash || url.pathname !== '/') {
      throw new Error('Invalid sandbox endpoint');
    }
    const localHttp = url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
    if (url.protocol !== 'https:' && !localHttp) throw new Error('Remote sandbox requires HTTPS');
    if (typeof options.getAccessToken !== 'function' || !options.tenantId || !options.userId) {
      throw new Error('Authenticated sandbox identity required');
    }
    this.endpoint = url.origin;
    this.getAccessToken = options.getAccessToken;
    this.tenantId = requiredIdentity(options.tenantId, 'tenant_id');
    this.userId = requiredIdentity(options.userId, 'user_id');
    this.fetcher = options.fetch ?? fetch;
  }

  // remote 模式只访问 Sandbox；任何远端失败都不会调用本地 provider。
  async initialize(profile = 'skill-polyglot-basic', runId: string = randomUUID()): Promise<Session> {
    if (this.session) return this.exportSession();
    if (this.initializing) return this.initializing;
    this.initializing = this.createSession(profile, runId);
    try { return await this.initializing; } finally { this.initializing = undefined; }
  }

  /** 取出不含 token 的快照，供调用方与其 Task/Run 一起持久化。 */
  exportSession(): Session {
    return { ...this.requireSession() };
  }

  /**
   * 从持久化快照恢复。先向服务端读取当前 Session，再校验 tenant/user/workspace，
   * 不以本地快照作为授权依据。
   */
  async restore(snapshot: Session, signal?: AbortSignal): Promise<Session> {
    if (this.initializing) await this.initializing;
    this.session = undefined;
    const saved = validateSessionSnapshot(snapshot);
    if (saved.tenant_id !== this.tenantId || saved.user_id !== this.userId) {
      throw new Error('Persisted sandbox session identity mismatch');
    }
    const current = await this.json<Session>('GET', this.sessionPath(saved.session_id), undefined, signal);
    const session = validateRemoteSession(current, this.tenantId, this.userId);
    if (session.session_id !== saved.session_id || session.workspace_id !== saved.workspace_id) {
      throw new Error('Sandbox session ownership mismatch');
    }
    this.session = session;
    return this.exportSession();
  }

  /** 单次提交；遇到传输错误不会自动重发，调用方应按 operation_id 查询结果。 */
  async submit(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecRecord> {
    validateOperationId(request.operation_id);
    validateExecutionRequest(request);
    const session = this.requireSession();
    const record = await this.json<ExecRecord>('POST', `${this.sessionPath(session.session_id)}/exec:async`, request, signal);
    return validateExecRecord(record, session.session_id, this.tenantId, request.operation_id);
  }

  async queryExec(execId: string, signal?: AbortSignal): Promise<ExecRecord> {
    const session = this.requireSession();
    const path = `${this.sessionPath(session.session_id)}/execs/${pathSegment(execId, 'exec_id')}`;
    const record = await this.json<ExecRecord>('GET', path, undefined, signal);
    return validateExecRecord(record, session.session_id, this.tenantId, undefined, execId);
  }

  async queryExecByOperationId(operationId: string, signal?: AbortSignal): Promise<ExecRecord> {
    validateOperationId(operationId);
    const session = this.requireSession();
    const record = await this.json<ExecRecord>(
      'GET',
      `${this.sessionPath(session.session_id)}/execs:lookup?operation_id=${encodeURIComponent(operationId)}`,
      undefined,
      signal,
    );
    return validateExecRecord(record, session.session_id, this.tenantId, operationId);
  }

  async cancelExec(execId: string, signal?: AbortSignal): Promise<ExecRecord> {
    const session = this.requireSession();
    const record = await this.json<ExecRecord>(
      'POST', `${this.sessionPath(session.session_id)}/execs/${pathSegment(execId, 'exec_id')}:cancel`, undefined, signal,
    );
    return validateExecRecord(record, session.session_id, this.tenantId, undefined, execId);
  }

  async cancelExecByOperationId(operationId: string, signal?: AbortSignal): Promise<ExecRecord> {
    const current = await this.queryExecByOperationId(operationId, signal);
    if (current.status !== 'queued' && current.status !== 'running') return current;
    return this.cancelExec(current.exec_id, signal);
  }

  /** 便利阻塞接口。提交只发生一次；传输结果不明时只按 operation_id 查询，不重放命令。 */
  async execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecRecord> {
    validateOperationId(request.operation_id);
    validateExecutionRequest(request);
    const session = this.requireSession();
    const timeoutMs = (request.timeout_seconds ?? 60) * 1000 + 30_000;
    const deadlineSignal = AbortSignal.timeout(timeoutMs);
    const boundedSignal = signal ? AbortSignal.any([signal, deadlineSignal]) : deadlineSignal;
    let record: ExecRecord;
    try {
      record = await this.submit(request, boundedSignal);
    } catch (submitError) {
      if (isDefinitiveRequestRejection(submitError)) throw submitError;
      try {
        record = await this.queryExecByOperationId(request.operation_id, AbortSignal.timeout(5_000));
      } catch {
        throw new RemoteExecutionError(errorMessage(submitError), session.session_id, request.operation_id);
      }
      if (boundedSignal.aborted) {
        await this.cancelIfActive(record);
        throw new RemoteExecutionError(
          errorMessage(boundedSignal.reason ?? submitError), session.session_id, request.operation_id, record.exec_id,
        );
      }
    }

    while (record.status === 'queued' || record.status === 'running') {
      if (boundedSignal.aborted) {
        await this.cancelIfActive(record);
        throw new RemoteExecutionError(
          errorMessage(boundedSignal.reason), session.session_id, request.operation_id, record.exec_id,
        );
      }
      try {
        await delay(250, boundedSignal);
        record = await this.queryExec(record.exec_id, boundedSignal);
      } catch (error) {
        if (boundedSignal.aborted) {
          await this.cancelIfActive(record);
          throw new RemoteExecutionError(
            errorMessage(boundedSignal.reason ?? error), session.session_id, request.operation_id, record.exec_id,
          );
        }
        // 查询是无副作用操作，可以有限重试；提交 POST 永远不会在这里重放。
        try { record = await this.queryExec(record.exec_id, AbortSignal.timeout(5_000)); }
        catch { throw new RemoteExecutionError(errorMessage(error), session.session_id, request.operation_id, record.exec_id); }
      }
    }
    return record;
  }

  /** SSE 游标可持久化；中断自动以 Last-Event-ID 重连，不会重新提交执行命令。 */
  async *streamExecLogs(execId: string, options: StreamExecLogsOptions = {}): AsyncGenerator<ExecLogEvent> {
    const session = this.requireSession();
    const safeExecId = pathId(execId, 'exec_id');
    let cursor = validateCursor(options.cursor ?? 0);
    const maximumReconnects = options.maxReconnects ?? 5;
    if (!Number.isSafeInteger(maximumReconnects) || maximumReconnects < 0 || maximumReconnects > 20) {
      throw new Error('Invalid maximum reconnect count');
    }
    let reconnects = 0;

    while (true) {
      options.signal?.throwIfAborted();
      try {
        const response = await this.send(
          'GET',
          `${this.sessionPath(session.session_id)}/execs/${pathSegment(safeExecId, 'exec_id')}/logs`,
          undefined,
          options.signal,
          { Accept: 'text/event-stream', 'Last-Event-ID': String(cursor) }, null,
        );
        const contentType = response.headers.get('content-type') ?? '';
        if (!contentType.toLowerCase().startsWith('text/event-stream')) {
          await response.body?.cancel();
          throw new SandboxApiError(502, 'Sandbox returned an invalid log stream content type');
        }
        if (!response.body) return;
        for await (const frame of readSseFrames(response.body, options.signal)) {
          const event = decodeLogEvent(frame);
          if (!event || event.cursor <= cursor) continue;
          if (event.cursor !== cursor + 1) throw new RemoteLogGapError(session.session_id, safeExecId, cursor, event.cursor);
          cursor = event.cursor;
          reconnects = 0;
          yield event;
        }
        return;
      } catch (error) {
        if (options.signal?.aborted) throw options.signal.reason ?? error;
        if (error instanceof SandboxProtocolError || error instanceof RemoteLogGapError) throw error;
        if (error instanceof SandboxApiError && error.status < 500) throw error;
        if (reconnects >= maximumReconnects) {
          throw new RemoteLogStreamError(session.session_id, safeExecId, cursor, errorMessage(error));
        }
        reconnects++;
        await delay(Math.min(250 * (2 ** (reconnects - 1)), 4_000), options.signal);
      }
    }
  }

  async listJobArtifacts(jobId: string, offset = 0, limit = 100, signal?: AbortSignal): Promise<ArtifactPage> {
    const safeJobId = pathId(jobId, 'job_id');
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error('Invalid artifact pagination');
    }
    const response = await this.send(
      'GET', `/v1/jobs/${pathSegment(safeJobId, 'job_id')}/artifacts?offset=${offset}&limit=${limit}`, undefined, signal,
    );
    const data = await readBytes(response, JSON_RESPONSE_LIMIT);
    const items = parseJson<JobArtifact[]>(data);
    if (!Array.isArray(items)) throw new Error('Invalid artifact list response');
    for (const item of items) validateArtifact(item, jobId, this.tenantId);
    const totalHeader = response.headers.get('x-total-count');
    const nextHeader = response.headers.get('x-next-offset');
    const total = totalHeader === null ? offset + items.length : parseNonNegativeInteger(totalHeader, 'X-Total-Count');
    const nextOffset = nextHeader === null ? undefined : parseNonNegativeInteger(nextHeader, 'X-Next-Offset');
    return { items, total, ...(nextOffset === undefined ? {} : { nextOffset }) };
  }

  async downloadArtifact(artifactId: string, maximumBytes = 16 * 1024 * 1024, signal?: AbortSignal): Promise<ArtifactDownload> {
    const safeArtifactId = pathId(artifactId, 'artifact_id');
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || maximumBytes > MAX_ARTIFACT_RESPONSE_BYTES) {
      throw new Error(`maximumBytes must be between 1 and ${MAX_ARTIFACT_RESPONSE_BYTES}`);
    }
    const response = await this.send('GET', `/v1/artifacts/${pathSegment(safeArtifactId, 'artifact_id')}`, undefined, signal);
    const content = await readBytes(response, maximumBytes);
    const expectedHash = response.headers.get('x-artifact-sha256')?.toLowerCase();
    if (!expectedHash || !/^[a-f0-9]{64}$/.test(expectedHash)) {
      throw new SandboxProtocolError('Sandbox artifact SHA-256 header is missing or invalid');
    }
    const actualHash = createHash('sha256').update(content).digest('hex');
    if (actualHash !== expectedHash) throw new SandboxProtocolError('Sandbox artifact SHA-256 mismatch');
    return {
      content,
      contentType: response.headers.get('content-type') ?? 'application/octet-stream',
      sha256: actualHash,
    };
  }

  async renew(ttlSeconds = 3600): Promise<Session> {
    if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 1 || ttlSeconds > 7 * 24 * 60 * 60) {
      throw new Error('Invalid session renewal TTL');
    }
    const previous = this.requireSession();
    const session = await this.json<Session>(
      'POST', `${this.sessionPath(previous.session_id)}:renew`, { extend_seconds: ttlSeconds },
    );
    const validated = validateRemoteSession(session, this.tenantId, this.userId);
    if (validated.session_id !== previous.session_id || validated.workspace_id !== previous.workspace_id) {
      throw new Error('Sandbox session ownership mismatch');
    }
    this.session = validated;
    return this.exportSession();
  }

  /** 显式恢复 Session 的短生命周期 runtime；服务端会校验并返回当前真实绑定。 */
  async resume(signal?: AbortSignal): Promise<Session> {
    const previous = this.requireSession();
    const resumed = await this.json<Session>(
      'POST', `${this.sessionPath(previous.session_id)}:resume`, undefined, signal,
    );
    const validated = validateRemoteSession(resumed, this.tenantId, this.userId);
    if (validated.session_id !== previous.session_id || validated.workspace_id !== previous.workspace_id) {
      throw new Error('Sandbox session ownership mismatch');
    }
    if (validated.status !== 'active') throw new SandboxProtocolError('Sandbox resume response is not logically active');
    if (!validated.active_sandbox_id) throw new SandboxProtocolError('Sandbox resume response has no active runtime binding');
    this.session = validated;
    return this.exportSession();
  }

  async upload(path: string, content: Uint8Array, revision?: string): Promise<{ sha256: string }> {
    validateWorkspacePath(path);
    const extra: Record<string, string> = {};
    if (revision !== undefined) {
      if (!revision || /[\r\n]/.test(revision)) throw new Error('Invalid file revision');
      extra['If-Match'] = revision;
    }
    const url = `${this.sessionPath(this.requireSession().session_id)}/files?path=${encodeURIComponent(path)}`;
    return this.json('PUT', url, content, undefined, extra);
  }

  async download(path: string, maximumBytes = 16 * 1024 * 1024): Promise<Uint8Array> {
    validateWorkspacePath(path);
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || maximumBytes > MAX_ARTIFACT_RESPONSE_BYTES) {
      throw new Error(`maximumBytes must be between 1 and ${MAX_ARTIFACT_RESPONSE_BYTES}`);
    }
    const url = `${this.sessionPath(this.requireSession().session_id)}/files?path=${encodeURIComponent(path)}`;
    const response = await this.send('GET', url);
    return readBytes(response, maximumBytes);
  }

  async list(path = '.', offset = 0, limit = 100): Promise<unknown> {
    validateWorkspacePath(path, true);
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error('Invalid list pagination');
    }
    const sessionPath = this.sessionPath(this.requireSession().session_id);
    const url = `${sessionPath}/files:list?path=${encodeURIComponent(path)}&offset=${offset}&limit=${limit}`;
    return this.json('GET', url);
  }

  async dispose(deleteWorkspace = false): Promise<void> {
    const session = this.session;
    if (!session) return;
    // 与其他语言 SDK 的清理约定一致：DELETE 404 视为目标已被 TTL 或并发清理回收，
    // 不把它抛进 finally 去掩盖业务主结果；其余失败仍显式上抛。
    await this.deleteIgnoringNotFound(this.sessionPath(session.session_id));
    if (deleteWorkspace) {
      await this.deleteIgnoringNotFound(`/v1/workspaces/${pathSegment(session.workspace_id, 'workspace_id')}`);
    }
    this.session = undefined;
  }

  private async createSession(profile: string, runId: string): Promise<Session> {
    if (!profile || profile.length > 128 || /[\r\n]/.test(profile)) throw new Error('Invalid sandbox profile');
    validateOperationId(runId);
    const created = await this.json<Session>('POST', '/v1/sessions', {
      environment: { profile: { name: profile } }, state_policy: 'session', ttl_seconds: 3600,
      workspace_retention: 'ttl', workspace_ttl_seconds: 86400,
      idempotency_key: runId, metadata: { run_id: runId },
    });
    this.session = validateRemoteSession(created, this.tenantId, this.userId);
    return this.exportSession();
  }

  private async cancelIfActive(record: ExecRecord): Promise<void> {
    if (record.status === 'queued' || record.status === 'running') {
      await this.cancelExec(record.exec_id, AbortSignal.timeout(5_000));
    }
  }

  /** 幂等清理：404 视为已删除，其余错误原样上抛。 */
  private async deleteIgnoringNotFound(path: string): Promise<void> {
    try {
      await this.json('DELETE', path);
    } catch (error) {
      if (!(error instanceof SandboxApiError && error.status === 404)) throw error;
    }
  }

  private requireSession(): Session {
    if (!this.session) throw new Error('Initialize or restore remote session first');
    return this.session;
  }

  private sessionPath(id: string): string { return `/v1/sessions/${pathSegment(id, 'session_id')}`; }

  private async send(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
    extra: Record<string, string> = {},
    timeoutMs: number | null = DEFAULT_REQUEST_TIMEOUT_MS,
  ): Promise<Response> {
    const requestSignal = signal ?? (timeoutMs === null ? undefined : AbortSignal.timeout(timeoutMs));
    const credentialSignal = requestSignal ?? AbortSignal.timeout(DEFAULT_REQUEST_TIMEOUT_MS);
    const token = await awaitWithSignal(Promise.resolve().then(() => this.getAccessToken(credentialSignal)), credentialSignal);
    if (typeof token !== 'string' || !token || /[\r\n]/.test(token)) {
      throw new Error('Access token provider returned an invalid credential');
    }
    const binary = body instanceof Uint8Array;
    const response = await this.fetcher(this.endpoint + path, {
      method,
      headers: { Authorization: `Bearer ${token}`, ...(binary ? {} : { 'Content-Type': 'application/json' }), ...extra },
      body: body === undefined ? undefined : binary ? body as Uint8Array : JSON.stringify(body),
      ...(requestSignal ? { signal: requestSignal } : {}),
      redirect: 'error',
    });
    if (!response.ok) {
      let message = `Sandbox HTTP ${response.status}`;
      try {
        const bytes = await readBytes(response, 64 * 1024);
        const parsed = parseJson<unknown>(bytes);
        if (parsed && typeof parsed === 'object' && 'message' in parsed && typeof parsed.message === 'string') {
          message = parsed.message;
        }
      } catch { await response.body?.cancel().catch(() => undefined); }
      throw new SandboxApiError(response.status, message);
    }
    return response;
  }

  private async json<T>(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
    extra?: Record<string, string>,
  ): Promise<T> {
    const response = await this.send(method, path, body, signal, extra);
    return parseJson<T>(await readBytes(response, JSON_RESPONSE_LIMIT));
  }
}

export function chooseExecutionProvider(
  mode: 'local' | 'remote',
  providers: { local: ExecutionProvider; remote?: ExecutionProvider },
): ExecutionProvider {
  if (mode === 'local') return providers.local;
  if (!providers.remote) throw new Error('Remote execution is not configured');
  return providers.remote;
}

function validateSessionSnapshot(value: Session): Session {
  if (!value || typeof value !== 'object') throw new Error('Invalid persisted sandbox session');
  return {
    session_id: validateIdentifier(value.session_id, 'session_id'),
    workspace_id: validateIdentifier(value.workspace_id, 'workspace_id'),
    tenant_id: requiredIdentity(value.tenant_id, 'tenant_id'),
    user_id: requiredIdentity(value.user_id, 'user_id'),
    ...(value.active_sandbox_id ? { active_sandbox_id: validateIdentifier(value.active_sandbox_id, 'active_sandbox_id') } : {}),
    ...(typeof value.status === 'string' ? { status: value.status } : {}),
    expires_at: validTimestamp(value.expires_at),
  };
}

function validateRemoteSession(value: Session, tenantId: string, userId: string): Session {
  const session = validateSessionSnapshot(value);
  if (session.tenant_id !== tenantId || session.user_id !== userId) throw new Error('Sandbox session identity mismatch');
  return session;
}

function validateExecRecord(
  value: ExecRecord,
  sessionId: string,
  tenantId: string,
  operationId?: string,
  execId?: string,
): ExecRecord {
  if (!value || typeof value !== 'object') throw new Error('Invalid execution record');
  const record = value;
  validateIdentifier(record.exec_id, 'exec_id');
  validateIdentifier(record.session_id, 'session_id');
  validateOperationId(record.operation_id);
  const wrongScope = record.session_id !== sessionId;
  const wrongOperation = operationId !== undefined && record.operation_id !== operationId;
  const wrongExec = execId !== undefined && record.exec_id !== execId;
  if (wrongScope || wrongOperation || wrongExec) {
    throw new Error('Sandbox execution ownership mismatch');
  }
  if (requiredIdentity(record.tenant_id, 'tenant_id') !== tenantId) {
    throw new Error('Sandbox execution tenant identity mismatch');
  }
  if (!Number.isSafeInteger(record.exit_code)) throw new SandboxProtocolError('Invalid execution exit code');
  if (record.stdout !== undefined && typeof record.stdout !== 'string') throw new SandboxProtocolError('Invalid execution stdout');
  if (record.stderr !== undefined && typeof record.stderr !== 'string') throw new SandboxProtocolError('Invalid execution stderr');
  if (!['queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted'].includes(record.status)) {
    throw new Error('Invalid execution status');
  }
  return record;
}

function validateExecutionRequest(request: ExecutionRequest): void {
  if (!request || typeof request !== 'object') throw new Error('Execution request is required');
  const invalidCommand = request.command !== undefined && (
    !Array.isArray(request.command)
    || request.command.length === 0
    || !request.command[0]
    || request.command.some((part) => typeof part !== 'string' || part.includes('\0'))
  );
  if (invalidCommand) throw new Error('Invalid command');
  if (request.code !== undefined && typeof request.code !== 'string') throw new Error('Invalid code');
  if ((request.command === undefined) === (request.code === undefined)) throw new Error('Provide exactly one of command or code');
  if (request.timeout_seconds !== undefined && (
    !Number.isSafeInteger(request.timeout_seconds)
    || request.timeout_seconds < 1
    || request.timeout_seconds > 86_400
  )) {
    throw new Error('Invalid execution timeout');
  }
  if (request.working_dir !== undefined) validateWorkingDirectory(request.working_dir);
  if (request.env !== undefined) {
    if (!request.env || typeof request.env !== 'object' || Array.isArray(request.env)) throw new Error('Invalid environment');
    for (const [key, value] of Object.entries(request.env)) {
      if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key) || typeof value !== 'string' || /[\0\r\n]/.test(value)) {
        throw new Error('Invalid environment entry');
      }
    }
  }
}

function validateArtifact(value: JobArtifact, jobId: string, tenantId: string): void {
  const invalidArtifact = !value || typeof value !== 'object'
    || value.job_id !== jobId
    || value.tenant_id !== tenantId
    || !value.artifact_id
    || typeof value.name !== 'string'
    || !value.name
    || !Number.isSafeInteger(value.size)
    || value.size < 0
    || !/^[a-f0-9]{64}$/i.test(value.sha256);
  if (invalidArtifact) {
    throw new Error('Invalid artifact metadata');
  }
  validateIdentifier(value.artifact_id, 'artifact_id');
  validateIdentifier(value.workspace_id, 'workspace_id');
}

function validateWorkspacePath(value: string, allowCurrentDirectory = false): void {
  if (
    typeof value !== 'string'
    || !value
    || value.startsWith('/')
    || value.includes('\\')
    || /[\0\r\n]/.test(value)
    || /^[A-Za-z]:/.test(value)
  ) {
    throw new Error('Relative workspace path required');
  }
  if (allowCurrentDirectory && value === '.') return;
  const segments = value.split('/');
  if (segments.some((part) => !part || part === '.' || part === '..')) throw new Error('Relative workspace path required');
}

function validateWorkingDirectory(value: string): void {
  if (value === '/workspace') return;
  if (value.startsWith('/workspace/')) {
    validateWorkspacePath(value.slice('/workspace/'.length));
    return;
  }
  validateWorkspacePath(value, true);
}

function pathId(value: string, label: string): string { return validateIdentifier(value, label); }

function pathSegment(value: string, label: string): string { return encodeURIComponent(validateIdentifier(value, label)); }

function validateIdentifier(value: string, label: string): string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value)) throw new Error(`Invalid ${label}`);
  return value;
}

function validateOperationId(value: string): void { pathId(value, 'operation_id'); }

function requiredIdentity(value: string, label: string): string {
  if (typeof value !== 'string' || !value || value.length > 256 || /[\0\r\n]/.test(value)) throw new Error(`Invalid ${label}`);
  return value;
}

function validTimestamp(value: string): string {
  if (typeof value !== 'string' || !Number.isFinite(Date.parse(value))) throw new Error('Invalid sandbox session expiry');
  return value;
}

function validateCursor(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error('Invalid log cursor');
  return value;
}

function parseNonNegativeInteger(value: string, label: string): number {
  if (!/^\d+$/.test(value)) throw new Error(`Invalid ${label}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`Invalid ${label}`);
  return parsed;
}

function parseJson<T>(bytes: Uint8Array): T {
  if (!bytes.byteLength) return undefined as T;
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as T;
}

async function readBytes(response: Response, limit: number): Promise<Uint8Array> {
  if (!Number.isSafeInteger(limit) || limit < 0) throw new Error('Invalid response limit');
  const contentLength = response.headers.get('content-length');
  if (contentLength && /^\d+$/.test(contentLength) && Number(contentLength) > limit) {
    await response.body?.cancel();
    throw new Error('Sandbox response limit exceeded');
  }
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) throw new Error('Sandbox response limit exceeded');
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  const result = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; }
  return result;
}

interface SseFrame { id?: string; event?: string; data: string; }

async function* readSseFrames(body: ReadableStream<Uint8Array>, signal?: AbortSignal): AsyncGenerator<SseFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let buffer = '';
  let frame: SseFrame = { data: '' };
  const consumeLine = (line: string): SseFrame | undefined => {
    if (!line) {
      const completed = frame.data ? frame : undefined;
      frame = { data: '' };
      return completed;
    }
    if (line.startsWith(':')) return undefined;
    const separator = line.indexOf(':');
    const field = separator < 0 ? line : line.slice(0, separator);
    const raw = separator < 0 ? '' : line.slice(separator + 1);
    const value = raw.startsWith(' ') ? raw.slice(1) : raw;
    if (field === 'id' && !value.includes('\0')) frame.id = value;
    else if (field === 'event') frame.event = value;
    else if (field === 'data') frame.data = frame.data ? `${frame.data}\n${value}` : value;
    return undefined;
  };
  try {
    while (true) {
      signal?.throwIfAborted();
      const { done, value } = await reader.read();
      try { buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done }); }
      catch { throw new SandboxProtocolError('Sandbox log stream is not valid UTF-8'); }
      if (buffer.length > MAX_SSE_FRAME_CHARS) throw new SandboxProtocolError('Sandbox log event exceeds size limit');
      while (true) {
        const newline = buffer.search(/[\r\n]/);
        if (newline < 0 || (buffer[newline] === '\r' && newline === buffer.length - 1 && !done)) break;
        const line = buffer.slice(0, newline);
        const delimiterLength = buffer[newline] === '\r' && buffer[newline + 1] === '\n' ? 2 : 1;
        buffer = buffer.slice(newline + delimiterLength);
        const completed = consumeLine(line);
        if (completed) yield completed;
      }
      if (done) {
        if (buffer) {
          const completed = consumeLine(buffer);
          if (completed) yield completed;
        }
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function decodeLogEvent(frame: SseFrame): ExecLogEvent | undefined {
  if (!frame.data) return undefined;
  if (!frame.id || !/^\d+$/.test(frame.id)) throw new SandboxProtocolError('Invalid Sandbox log cursor');
  let cursor: number;
  let payload: { cursor?: unknown; time?: unknown; message?: unknown };
  try {
    cursor = parseNonNegativeInteger(frame.id, 'log cursor');
    payload = JSON.parse(frame.data) as { cursor?: unknown; time?: unknown; message?: unknown };
  } catch { throw new SandboxProtocolError('Invalid Sandbox log event payload'); }
  if (payload.cursor !== cursor || typeof payload.message !== 'string') {
    throw new SandboxProtocolError('Invalid Sandbox log event');
  }
  if (payload.time !== undefined && typeof payload.time !== 'string') {
    throw new SandboxProtocolError('Invalid Sandbox log event time');
  }
  const event = frame.event ?? 'message';
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(event)) throw new SandboxProtocolError('Invalid Sandbox log event type');
  return { cursor, event, message: payload.message, ...(payload.time === undefined ? {} : { time: payload.time }) };
}

async function delay(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  await new Promise<void>((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(signal?.reason); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, ms);
    signal?.addEventListener('abort', abort, { once: true });
  });
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }

function isDefinitiveRequestRejection(error: unknown): boolean {
  return error instanceof SandboxApiError
    && error.status >= 400
    && error.status < 500
    && error.status !== 408
    && error.status !== 425;
}

function awaitWithSignal<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', abort);
    const abort = () => { cleanup(); reject(signal.reason); };
    signal.addEventListener('abort', abort, { once: true });
    promise.then((value) => { cleanup(); resolve(value); }, (error) => { cleanup(); reject(error); });
  });
}
