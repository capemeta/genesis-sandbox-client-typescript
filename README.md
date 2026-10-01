# Genesis Sandbox Client for TypeScript

面向 Node 22+ 受信服务端 Runtime（如 Claw Daemon）的 Genesis Sandbox 客户端 SDK。它只提供 Sandbox HTTP API 适配，不接管 Agent Run、审批、本地能力或用户文件 staging。显式选择 remote 后，任何请求错误都不会调用本地 Provider。

- 零运行时依赖（仅使用 Node 内置模块与标准 `fetch`）
- ESM 发布，附完整 `.d.ts` 类型声明
- 语义与 `genesis-sandbox-client-go` / `-python` / `-java` 保持一致

## 安装

```bash
npm install genesis-sandbox-client-typescript
```

要求 Node.js 22 或更高版本。

## 快速开始

```ts
import { RemoteExecutionProvider } from 'genesis-sandbox-client-typescript';

const remote = new RemoteExecutionProvider({
  endpoint: process.env.SANDBOX_ENDPOINT!,
  // 该回调在每个 HTTP 请求前调用，可从受信凭据代理取得当前短期凭据。
  getAccessToken: (signal) => credentialBroker.getSandboxCredential({ signal }),
  tenantId: trustedRun.tenantId,
  userId: trustedRun.userId,
});

const session = await remote.initialize('skill-polyglot-basic', trustedRun.runId);
await runStore.saveSandboxSession(trustedRun.runId, remote.exportSession());

const accepted = await remote.submit({ operation_id: trustedRun.operationId, command: ['python', '-c', 'print(1)'] });
const result = await remote.queryExec(accepted.exec_id);
await remote.dispose(true);
```

`getAccessToken` 必须由受信的 daemon/service 代码提供，不能交给 Renderer、工作流参数或不可信 Skill。SDK 不缓存 token，每次 HTTP 请求都会重新获取，并把该请求的 `AbortSignal` 传给回调；回调应支持及时取消。不要把 token 写入 Session 快照、Task 事件、日志或持久化业务记录。`tenantId` 和 `userId` 只用于校验远端响应与本地 Run 的归属，**它们本身不是身份凭据，也不能由不可信请求决定授权**。

## Session 持久化与恢复

在调用 `initialize(profile, runId)` 前先持久化稳定的 Run ID；若创建响应丢失，应以同一 ID 处理，而不是生成新 Run 并重复副作用。创建成功后，在 Claw Task/Run 的持久化事务中保存 `exportSession()` 返回值；Runtime 重启后创建新的 Provider，再调用 `restore(snapshot)`。恢复会用当前凭据请求 `GET /v1/sessions/{id}`，核对服务端返回的 tenant、user、session 和 workspace；本地快照不会成为授权依据。快照里不包含访问凭据。

`restore()` 恢复的是逻辑 Session，不会自动创建容器。若需要在执行前明确唤醒或校验 runtime，调用 `resume()`；它访问 `POST /v1/sessions/{id}:resume` 并要求服务端返回 `active_sandbox_id`。逻辑 `status: active` 并不表示 runtime 已存在，必须检查 `active_sandbox_id`，也可以直接调用 `resume()` 让服务端进行权威对账。Run 完成后调用 `dispose(true)` 释放 Session 与 Workspace；daemon 异常退出时由服务端 TTL 回收。

## Exec、取消与幂等

- `submit()` 每次只发送一次创建请求。若响应丢失，按稳定 `operation_id` 调用 `queryExecByOperationId()`；SDK 不会自动重发命令。
- 使用 `queryExec(execId)` 或 `queryExecByOperationId(operationId)` 恢复执行观察；使用 `cancelExec(execId)` 或 `cancelExecByOperationId(operationId)` 显式取消。
- `execute()` 是阻塞便捷接口：只提交一次并轮询只读状态；传入的 `AbortSignal` 会尝试取消已知 Exec。观察连接失败会携带 session/operation/exec 身份抛错，调用方可重连查询。
- `timed_out` 与 `interrupted` 都是终态。前者表示执行超过 deadline；后者表示 worker 重启后执行结果无法确认。SDK 不自动重试，业务层应先对账副作用再决定下一步。
- 同一操作的重试必须复用 `operation_id` 和完全相同的请求内容。不要为了解决超时创建新 ID 并再次执行非幂等操作。

## SSE 日志与断点续读

```ts
let cursor = savedRun.sandboxLogCursor ?? 0;
try {
  for await (const event of remote.streamExecLogs(execId, { cursor, signal })) {
    await runStore.appendSandboxLogAndCursor(runId, event, event.cursor);
    cursor = event.cursor;
  }
} catch (error) {
  // RemoteLogStreamError.cursor 可与 Run 状态一起保存，稍后继续。
}
```

日志流携带 `Last-Event-ID`，网络错误时会有限退避并从最后已交付游标续连；已交付游标不会重复 yield。若服务端日志保留窗口导致游标出现缺口，SDK 抛出 `RemoteLogGapError`，调用方应将日志标记为不完整后再决定如何恢复。进程退出后，调用方应持久化已处理游标，再把它传回 `streamExecLogs`。取消日志订阅不会隐式取消正在运行的命令；执行取消走独立的 cancel API。

## Job 产物

`listJobArtifacts(jobId, offset, limit)` 返回带总数和下一页偏移的页面；`downloadArtifact(artifactId, maximumBytes)` 返回二进制和 Content-Type，并校验下载内容与服务端 SHA-256 一致。下载会在读取期间执行字节上限，单次上限最多 64 MiB；超限或哈希不符时关闭响应流并报错。调用方仍需核对 artifact 与当前 Run 的权限和预期内容类型，再决定是否写入本地文件。

## 认证实现边界

当前 Sandbox HTTP 服务只把 `Authorization: Bearer <credential>` 当作配置中的 API key 校验，没有实现 Genesis Auth/OIDC 令牌验证、委托签发、audience/scope 交换或短期 token introspection。SDK 的异步 `getAccessToken` 仅支持凭据轮转接口；它**不会自行完成真实 Auth 委托**。生产接入前必须由受信 Gateway/凭据服务签发或映射当前 Sandbox 已支持的 API key，并依据可信身份绑定 tenant/user；不能把浏览器会话或用户可控字段直接当成 Sandbox 身份。

本 SDK 提供可供后续 Runtime adapter 使用的客户端边界，不代表 Claw 产品已经接线，也不提供本地/远端自动回退。

## 开发

```bash
npm install        # 安装开发依赖（TypeScript、ESLint）
npm run lint       # ESLint 检查
npm run typecheck  # tsc --noEmit（src + tests + examples）
npm test           # node:test 直接运行 TypeScript 源码测试
npm run build      # 编译到 dist/（ESM + .d.ts + sourcemap）
```

测试使用 Node 内置 HTTP/SSE 协议伪端，不依赖或启动浏览器，也不需要真实 Sandbox 服务。CI 在 Node 22 与 24 上运行完整检查（见 `.github/workflows/ci.yml`）。

## License

Apache-2.0
