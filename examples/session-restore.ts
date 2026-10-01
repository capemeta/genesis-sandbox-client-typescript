/**
 * Session 快照持久化与恢复示例：exportSession 不含凭据，可随业务记录一起落盘；
 * restore 会向服务端重新核对身份与归属，本地快照不构成授权依据。
 *
 * 运行：node --experimental-strip-types examples/session-restore.ts
 */
import { randomUUID } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { RemoteExecutionProvider, type Session } from '../src/index.ts';

const endpoint = process.env.SANDBOX_ENDPOINT;
const apiKey = process.env.SANDBOX_API_KEY;
const tenantId = process.env.SANDBOX_TENANT_ID;
const userId = process.env.SANDBOX_USER_ID;
const snapshotPath = process.env.SESSION_SNAPSHOT ?? 'session-snapshot.json';
if (!endpoint || !apiKey || !tenantId || !userId) {
  console.error('Set SANDBOX_ENDPOINT, SANDBOX_API_KEY, SANDBOX_TENANT_ID and SANDBOX_USER_ID first.');
  process.exit(1);
}

// 实际业务中 runId 应来自持久化的 Run 记录，而不是每次随机生成。
const runId = process.env.SANDBOX_RUN_ID ?? randomUUID();
const remote = new RemoteExecutionProvider({
  endpoint,
  getAccessToken: () => apiKey,
  tenantId,
  userId,
});

try {
  let snapshot: Session | undefined;
  try {
    snapshot = JSON.parse(readFileSync(snapshotPath, 'utf8')) as Session;
  } catch {
    snapshot = undefined;
  }

  if (snapshot) {
    const restored = await remote.restore(snapshot);
    console.log('session restored:', restored.session_id);
    // restore 只恢复逻辑 Session；执行前可用 resume() 显式唤醒并校验 runtime 绑定。
    const resumed = await remote.resume();
    console.log('runtime bound:', resumed.active_sandbox_id);
  } else {
    const created = await remote.initialize('skill-polyglot-basic', runId);
    writeFileSync(snapshotPath, JSON.stringify(remote.exportSession()));
    console.log('session created and persisted:', created.session_id);
  }

  const result = await remote.execute({
    operation_id: `demo-${runId}`,
    command: ['python', '-c', 'print("resumed run")'],
  });
  console.log('exec finished:', result.status);
} finally {
  await remote.dispose(true);
}
