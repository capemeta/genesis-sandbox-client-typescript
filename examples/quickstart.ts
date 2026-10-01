/**
 * 最小可用示例：创建 Session、阻塞执行一条命令、读取日志、释放资源。
 *
 * 运行前设置环境变量（见 docs/sdks/README.md 中的服务端配置说明）：
 *   SANDBOX_ENDPOINT    例如 http://127.0.0.1:18010
 *   SANDBOX_API_KEY     服务端配置的 API key，SDK 发送为 Authorization: Bearer <key>
 *   SANDBOX_TENANT_ID   与该 key 绑定的租户身份，SDK 用它校验响应归属
 *   SANDBOX_USER_ID     与该 key 绑定的用户身份
 *
 * 运行：node --experimental-strip-types examples/quickstart.ts
 */
import { RemoteExecutionProvider } from '../src/index.ts';

const endpoint = process.env.SANDBOX_ENDPOINT;
const apiKey = process.env.SANDBOX_API_KEY;
const tenantId = process.env.SANDBOX_TENANT_ID;
const userId = process.env.SANDBOX_USER_ID;
if (!endpoint || !apiKey || !tenantId || !userId) {
  console.error('Set SANDBOX_ENDPOINT, SANDBOX_API_KEY, SANDBOX_TENANT_ID and SANDBOX_USER_ID first.');
  process.exit(1);
}

// 受信进程内可以直接返回静态 key；多租户服务应从凭据代理按请求轮转获取。
const remote = new RemoteExecutionProvider({
  endpoint,
  getAccessToken: () => apiKey,
  tenantId,
  userId,
});

try {
  const session = await remote.initialize('skill-polyglot-basic');
  console.log('session ready:', session.session_id, 'workspace:', session.workspace_id);

  // operation_id 必须在业务侧稳定且唯一；重试同一操作时复用同一个 ID。
  const result = await remote.execute({
    operation_id: `demo-${Date.now()}`,
    command: ['python', '-c', 'print("hello from sandbox")'],
  });
  console.log('exec finished:', result.status, 'exit code:', result.exit_code);

  for await (const event of remote.streamExecLogs(result.exec_id)) {
    console.log(`[${event.cursor}] ${event.message}`);
  }
} finally {
  await remote.dispose(true);
}
