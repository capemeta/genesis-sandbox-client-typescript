import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  RemoteExecutionError,
  RemoteExecutionProvider,
  RemoteLogGapError,
  RemoteLogStreamError,
  SandboxApiError,
  SandboxProtocolError,
  chooseExecutionProvider,
  type ExecRecord,
  type JobArtifact,
  type Session,
} from '../src/index.ts';

const identity = { endpoint: 'http://127.0.0.1:8090', getAccessToken: async () => 'api-key-1', tenantId: 'tenant-a', userId: 'user-a' };
const session: Session = {
  session_id: 'session-1', workspace_id: 'work-1', tenant_id: 'tenant-a', user_id: 'user-a',
  status: 'active', expires_at: '2099-01-01T00:00:00Z',
};
const execRecord = (status: ExecRecord['status'] = 'succeeded'): ExecRecord => ({
  exec_id: 'exec-1', operation_id: 'op-1', session_id: session.session_id,
  tenant_id: session.tenant_id, status, exit_code: 0,
});
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {
  status,
  headers: { 'Content-Type': 'application/json' },
});
const sessionJSON = () => json(session);
const requestUrl = (input: Parameters<typeof fetch>[0]) => new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
const requestHeaders = (init?: RequestInit) => new Headers(init?.headers);

function provider(fetcher: typeof fetch, getAccessToken: () => string | Promise<string> = identity.getAccessToken) {
  return new RemoteExecutionProvider({ ...identity, getAccessToken, fetch: fetcher });
}

test('explicit remote has no local fallback and requires a credential provider', () => {
  const local = { execute: async () => { throw new Error('must not execute'); } };
  assert.throws(() => chooseExecutionProvider('remote', { local }), /not configured/);
  assert.throws(() => new RemoteExecutionProvider({ ...identity, endpoint: 'http://remote.example' }), /HTTPS/);
  assert.throws(() => new RemoteExecutionProvider({
    endpoint: identity.endpoint,
    getAccessToken: undefined as never,
    tenantId: 't',
    userId: 'u',
  }), /identity/);
});

test('credentials are resolved afresh for each request and never appear in URLs', async () => {
  let tokenNumber = 0;
  const headers: string[] = [];
  const p = provider((async (input, init) => {
    const url = requestUrl(input);
    headers.push(requestHeaders(init).get('Authorization') ?? '');
    assert.equal(url.search, '');
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) return json(execRecord());
    if (url.pathname === '/v1/sessions/session-1') return json({ status: 'deleted' });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch, async () => `rotating-key-${++tokenNumber}`);

  await p.initialize();
  await p.submit({ operation_id: 'op-1', code: 'print(1)' });
  await p.dispose();
  assert.deepEqual(headers, ['Bearer rotating-key-1', 'Bearer rotating-key-2', 'Bearer rotating-key-3']);
});

test('abort also interrupts a stalled asynchronous credential provider', async () => {
  let tokenCalls = 0;
  const p = provider((async (input) => {
    if (requestUrl(input).pathname === '/v1/sessions') return sessionJSON();
    throw new Error('fetch must not run after the stalled token request');
  }) as typeof fetch, async () => {
    tokenCalls++;
    if (tokenCalls === 1) return 'api-key';
    return new Promise<string>(() => undefined);
  });
  await p.initialize();
  const controller = new AbortController();
  const submission = p.submit({ operation_id: 'op-1', code: 'neverSubmitted()' }, controller.signal);
  setTimeout(() => controller.abort(new Error('caller cancelled')), 10);
  await assert.rejects(submission, /caller cancelled/);
  assert.equal(tokenCalls, 2);
});

test('session snapshot can be restored only after the server confirms its tenant, user, and workspace', async () => {
  const requests: string[] = [];
  const p = provider((async (input) => {
    const url = requestUrl(input);
    requests.push(url.pathname);
    if (url.pathname === '/v1/sessions/session-1') return sessionJSON();
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  const restored = await p.restore(session);
  assert.deepEqual(restored, session);
  assert.deepEqual(p.exportSession(), session);
  assert.deepEqual(requests, ['/v1/sessions/session-1']);
});

test('restore rejects forged local identity and mismatched server ownership before allowing execution', async () => {
  let requests = 0;
  const wrongServerSession = { ...session, workspace_id: 'other-work' };
  const p = provider((async () => { requests++; return json(wrongServerSession); }) as typeof fetch);

  await assert.rejects(p.restore(session), /ownership mismatch/);
  await assert.rejects(p.queryExec('exec-1'), /Initialize or restore/);
  await assert.rejects(p.restore({ ...session, tenant_id: 'tenant-b' }), /identity mismatch/);
  assert.equal(requests, 1);
});

test('resume consults the server even when logical status is active but no runtime is bound', async () => {
  const requests: Array<{ path: string; method: string; body: unknown }> = [];
  const p = provider((async (input, init) => {
    const url = requestUrl(input);
    requests.push({ path: url.pathname, method: init?.method ?? 'GET', body: init?.body });
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/sessions/session-1:resume') return json({ ...session, active_sandbox_id: 'sandbox-1' });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  await p.initialize();
  assert.equal(p.exportSession().status, 'active');
  assert.equal(p.exportSession().active_sandbox_id, undefined);
  const resumed = await p.resume();
  assert.equal(resumed.active_sandbox_id, 'sandbox-1');
  assert.deepEqual(requests[1], { path: '/v1/sessions/session-1:resume', method: 'POST', body: undefined });
});

test('restored suspended snapshot stays dormant until resume returns a verified runtime binding', async () => {
  const suspended = { ...session, status: 'suspended' };
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions/session-1') return json(suspended);
    if (url.pathname === '/v1/sessions/session-1:resume') return json({ ...session, active_sandbox_id: 'sandbox-restored' });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  const restored = await p.restore(suspended);
  assert.equal(restored.status, 'suspended');
  assert.equal(restored.active_sandbox_id, undefined);
  const resumed = await p.resume();
  assert.equal(resumed.status, 'active');
  assert.equal(resumed.active_sandbox_id, 'sandbox-restored');
});

test('resume rejects missing or mismatched runtime bindings', async () => {
  let response: Session = session;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/sessions/session-1:resume') return json(response);
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.resume(), /no active runtime binding/);
  response = { ...session, active_sandbox_id: 'sandbox-1', workspace_id: 'other-work' };
  await assert.rejects(p.resume(), /ownership mismatch/);
  response = { ...session, active_sandbox_id: 'sandbox-1', status: 'suspended' };
  await assert.rejects(p.resume(), /not logically active/);
  assert.equal(p.exportSession().active_sandbox_id, undefined);
});

test('submit, query by either identifier, and cancel use scoped routes and validate returned records', async () => {
  const requests: Array<{ path: string; method: string }> = [];
  const p = provider((async (input, init) => {
    const url = requestUrl(input);
    requests.push({ path: url.pathname, method: init?.method ?? 'GET' });
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) return json(execRecord('running'));
    if (url.pathname.endsWith('/execs:lookup')) return json(execRecord('running'));
    if (url.pathname.endsWith('/execs/exec-1:cancel')) return json(execRecord('cancelled'));
    if (url.pathname.endsWith('/execs/exec-1')) return json(execRecord('running'));
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  await p.initialize();
  assert.equal((await p.submit({ operation_id: 'op-1', command: ['python', '-c', 'pass'] })).status, 'running');
  assert.equal((await p.queryExec('exec-1')).exec_id, 'exec-1');
  assert.equal((await p.queryExecByOperationId('op-1')).operation_id, 'op-1');
  assert.equal((await p.cancelExecByOperationId('op-1')).status, 'cancelled');
  assert.ok(requests.some((item) => item.path.endsWith('/execs:lookup')));
  assert.ok(requests.some((item) => item.path.endsWith('/execs/exec-1:cancel') && item.method === 'POST'));
});

test('ambiguous submit result is resolved by operation ID without replaying the side effect', async () => {
  let posts = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) { posts++; throw new TypeError('connection lost after request write'); }
    if (url.pathname.endsWith('/execs:lookup')) return json(execRecord('succeeded'));
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  await p.initialize();
  const result = await p.execute({ operation_id: 'op-1', code: 'sideEffect()' });
  assert.equal(result.status, 'succeeded');
  assert.equal(posts, 1);
});

test('ambiguous submit with no query result fails with operation identity and never posts twice', async () => {
  let submissions = 0;
  let lookups = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) { submissions++; throw new TypeError('connection lost'); }
    if (url.pathname.endsWith('/execs:lookup')) { lookups++; return json({ message: 'not found' }, 404); }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.execute({ operation_id: 'op-1', code: 'sideEffect()' }), (error: unknown) => {
    assert.ok(error instanceof RemoteExecutionError);
    assert.equal(error.operation_id, 'op-1');
    assert.equal(error.session_id, 'session-1');
    return true;
  });
  assert.equal(submissions, 1);
  assert.equal(lookups, 1);
});

test('definitive submit rejection preserves the API error without operation lookup', async () => {
  let lookups = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) return json({ message: 'policy denied' }, 403);
    if (url.pathname.endsWith('/execs:lookup')) { lookups++; return json(execRecord('running')); }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.execute({ operation_id: 'op-1', code: 'sideEffect()' }), (error: unknown) => {
    assert.ok(error instanceof SandboxApiError);
    assert.equal(error.status, 403);
    assert.match(error.message, /policy denied/);
    return true;
  });
  assert.equal(lookups, 0);
});

test('interrupted is a terminal execution state and is never retried', async () => {
  let submissions = 0;
  let reads = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) { submissions++; return json(execRecord('running')); }
    if (url.pathname.endsWith('/execs/exec-1')) { reads++; return json(execRecord('interrupted')); }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  const result = await p.execute({ operation_id: 'op-1', code: 'longTask()' });
  assert.equal(result.status, 'interrupted');
  assert.equal(submissions, 1);
  assert.equal(reads, 1);
});

test('timed_out is a terminal execution state and is never retried', async () => {
  let submissions = 0;
  let reads = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) { submissions++; return json(execRecord('running')); }
    if (url.pathname.endsWith('/execs/exec-1')) { reads++; return json(execRecord('timed_out')); }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  const result = await p.execute({ operation_id: 'op-1', code: 'longTask()' });
  assert.equal(result.status, 'timed_out');
  assert.equal(submissions, 1);
  assert.equal(reads, 1);
});

test('temporary status read failure preserves reconnect identity without implicit cancellation', async () => {
  let submissions = 0;
  let statusReads = 0;
  let cancellations = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) { submissions++; return json(execRecord('running')); }
    if (url.pathname.endsWith('/execs/exec-1:cancel')) { cancellations++; return json(execRecord('cancelled')); }
    if (url.pathname.endsWith('/execs/exec-1')) { statusReads++; return json({ message: 'temporarily unavailable' }, 503); }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.execute({ operation_id: 'op-1', code: 'longTask()' }), (error: unknown) => {
    assert.ok(error instanceof RemoteExecutionError);
    assert.equal(error.session_id, 'session-1');
    assert.equal(error.operation_id, 'op-1');
    assert.equal(error.exec_id, 'exec-1');
    return true;
  });
  assert.equal(submissions, 1);
  assert.equal(statusReads, 2);
  assert.equal(cancellations, 0);
});

test('caller cancellation targets the known exec and never invokes a local fallback', async () => {
  const requests: string[] = [];
  const p = provider((async (input) => {
    const url = requestUrl(input);
    requests.push(url.pathname);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) return json(execRecord('running'));
    if (url.pathname.endsWith('/execs/exec-1:cancel')) return json(execRecord('cancelled'));
    if (url.pathname.endsWith('/execs/exec-1')) return json(execRecord('running'));
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  const controller = new AbortController();
  const execution = p.execute({ operation_id: 'op-1', code: 'longTask()' }, controller.signal);
  setTimeout(() => controller.abort(), 20);
  await assert.rejects(execution, (error: unknown) => error instanceof RemoteExecutionError && error.exec_id === 'exec-1');
  assert.ok(requests.some((path) => path.endsWith('/execs/exec-1:cancel')));
  assert.equal(requests.filter((path) => path.endsWith('/exec:async')).length, 1);
});

test('SSE reconnect resumes from the last yielded event using Last-Event-ID', async () => {
  const requests: Array<{ path: string; lastEventId: string | null }> = [];
  let streams = 0;
  const frame = (cursor: number, message: string) => [
    `id: ${cursor}`,
    'event: stdout',
    `data: ${JSON.stringify({ cursor, message, time: '2026-09-30T00:00:00Z' })}`,
    '',
    '',
  ].join('\n');
  const firstStream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      controller.enqueue(encoder.encode('id: 1\r'));
      controller.enqueue(encoder.encode(`\nevent: stdout\r\ndata: ${JSON.stringify({ cursor: 1, message: 'one' })}\r\n\r\n`));
    },
    pull(controller) {
      controller.error(new TypeError('connection reset'));
    },
  });
  const p = provider((async (input, init) => {
    const url = requestUrl(input);
    requests.push({ path: url.pathname, lastEventId: requestHeaders(init).get('Last-Event-ID') });
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/logs')) {
      streams++;
      if (streams === 1) return new Response(firstStream, { headers: { 'Content-Type': 'text/event-stream' } });
      return new Response(frame(2, 'two'), { headers: { 'Content-Type': 'text/event-stream' } });
    }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);

  await p.initialize();
  const events = [];
  for await (const event of p.streamExecLogs('exec-1')) events.push(event);
  assert.deepEqual(events.map((event) => [event.cursor, event.message]), [[1, 'one'], [2, 'two']]);
  assert.deepEqual(requests.filter((request) => request.path.endsWith('/logs')).map((request) => request.lastEventId), ['0', '1']);
});

test('SSE cursor validation and bounded reconnect failures are explicit', async () => {
  const online = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions/session-1') return sessionJSON();
    throw new TypeError('offline');
  }) as typeof fetch);
  await online.restore(session);
  await assert.rejects(online.streamExecLogs('exec-1', { cursor: -1 }).next(), /cursor/);
  await assert.rejects(
    online.streamExecLogs('exec-1', { maxReconnects: 0 }).next(),
    (error: unknown) => error instanceof RemoteLogStreamError && error.cursor === 0,
  );
});

test('malformed SSE payload is rejected without retrying or replaying execution', async () => {
  let streamRequests = 0;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/logs')) {
      streamRequests++;
      return new Response('id: 1\nevent: stdout\ndata: not-json\n\n', { headers: { 'Content-Type': 'text/event-stream' } });
    }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(async () => { for await (const _event of p.streamExecLogs('exec-1')) { /* 消费事件以触发协议校验 */ } }, SandboxProtocolError);
  assert.equal(streamRequests, 1);
});

test('log retention gaps are reported instead of silently dropping events', async () => {
  const event = `id: 4\nevent: stdout\ndata: ${JSON.stringify({ cursor: 4, message: 'gap' })}\n\n`;
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/logs')) return new Response(event, { headers: { 'Content-Type': 'text/event-stream' } });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.streamExecLogs('exec-1').next(), (error: unknown) => {
    assert.ok(error instanceof RemoteLogGapError);
    assert.equal(error.previous_cursor, 0);
    assert.equal(error.next_cursor, 4);
    return true;
  });
});

test('job artifact listing is tenant scoped and download enforces a hard response ceiling', async () => {
  const artifactHash = createHash('sha256').update('csv').digest('hex');
  const artifact: JobArtifact = {
    artifact_id: 'artifact-1', tenant_id: 'tenant-a', workspace_id: 'work-1', job_id: 'job-1',
    name: 'result.csv', path: 'result.csv', size: 3, sha256: artifactHash, mime: 'text/csv', created_at: '2026-09-30T00:00:00Z',
  };
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/jobs/job-1/artifacts') {
      return new Response(JSON.stringify([artifact]), { headers: { 'X-Total-Count': '2', 'X-Next-Offset': '1' } });
    }
    if (url.pathname === '/v1/artifacts/artifact-1') {
      return new Response(new TextEncoder().encode('csv'), {
        headers: { 'Content-Type': 'text/csv', 'X-Artifact-SHA256': artifact.sha256 },
      });
    }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  const page = await p.listJobArtifacts('job-1', 0, 1);
  assert.equal(page.items[0].name, 'result.csv');
  assert.equal(page.total, 2);
  assert.equal(page.nextOffset, 1);
  const downloaded = await p.downloadArtifact('artifact-1', 3);
  assert.equal(new TextDecoder().decode(downloaded.content), 'csv');
  assert.equal(downloaded.contentType, 'text/csv');
  assert.equal(downloaded.sha256, artifact.sha256);
  await assert.rejects(p.downloadArtifact('artifact-1', 2), /response limit/);
  await assert.rejects(p.downloadArtifact('artifact-1', 0), /maximumBytes/);
  await assert.rejects(p.downloadArtifact('artifact-1', 65 * 1024 * 1024), /maximumBytes/);
});

test('artifact payload hash mismatch is rejected', async () => {
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/artifacts/artifact-1') return new Response('payload', { headers: { 'X-Artifact-SHA256': '0'.repeat(64) } });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.downloadArtifact('artifact-1'), /SHA-256 mismatch/);
});

test('malicious IDs, file paths, artifact metadata, and cross-session records are rejected', async () => {
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname.endsWith('/exec:async')) return json({ ...execRecord(), session_id: 'session-other' });
    if (url.pathname === '/v1/jobs/job-1/artifacts') {
      return json([{
        artifact_id: '../secret', tenant_id: 'tenant-a', workspace_id: 'work-1', job_id: 'job-1',
        name: 'x', path: 'x', size: 1, sha256: 'a'.repeat(64), created_at: 'now',
      }]);
    }
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.queryExec('../other'), /exec_id/);
  await assert.rejects(p.upload('../secret', new Uint8Array()), /Relative workspace path/);
  await assert.rejects(p.upload('C:/secret', new Uint8Array()), /Relative workspace path/);
  await assert.rejects(p.submit({ operation_id: 'op-1', code: 'run()' }), /ownership mismatch/);
  await assert.rejects(p.listJobArtifacts('job-1'), /artifact_id/);
});

test('dispose treats 404 as already deleted instead of masking the run outcome', async () => {
  const requests: string[] = [];
  const p = provider((async (input) => {
    const url = requestUrl(input);
    requests.push(url.pathname);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/sessions/session-1') return json({ message: 'not found' }, 404);
    if (url.pathname === '/v1/workspaces/work-1') return json({ message: 'not found' }, 404);
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await p.dispose(true);
  assert.deepEqual(requests, ['/v1/sessions', '/v1/sessions/session-1', '/v1/workspaces/work-1']);
  await p.dispose(true); // 清理后再次 dispose 是幂等 no-op，不会再发请求。
  assert.equal(requests.length, 3);
  await assert.rejects(p.queryExec('exec-1'), /Initialize or restore/);
});

test('dispose still surfaces cleanup failures other than 404', async () => {
  const p = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions') return sessionJSON();
    if (url.pathname === '/v1/sessions/session-1') return json({ message: 'not found' }, 404);
    if (url.pathname === '/v1/workspaces/work-1') return json({ message: 'denied' }, 403);
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await p.initialize();
  await assert.rejects(p.dispose(true), (error: unknown) => error instanceof SandboxApiError && error.status === 403);
});

test('API errors retain status and oversized payloads fail closed before buffering', async () => {
  const unauthorized = provider((async () => json({ message: 'denied' }, 401)) as typeof fetch);
  await assert.rejects(unauthorized.initialize(), (error: unknown) => error instanceof SandboxApiError && error.status === 401);

  const oversized = provider((async (input) => {
    const url = requestUrl(input);
    if (url.pathname === '/v1/sessions/session-1') return sessionJSON();
    if (url.pathname === '/v1/artifacts/artifact-1') return new Response('12345', { headers: { 'Content-Length': '5' } });
    throw new Error(`unexpected request ${url}`);
  }) as typeof fetch);
  await oversized.restore(session);
  await assert.rejects(oversized.downloadArtifact('artifact-1', 4), /response limit/);
});

