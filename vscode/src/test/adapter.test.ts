import { spawn, ChildProcess } from 'node:child_process';
import * as path from 'node:path';
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { DebugClient } from '@vscode/debugadapter-testsupport';
import type { DebugProtocol } from '@vscode/debugprotocol';

// The binary under test. `task test:ext` passes the freshly built one;
// relative paths resolve against the extension folder (npm's cwd).
const binary = path.resolve(process.env.RPCN_DEBUG_BIN ?? '../bin/rpcn-debug');

// The tracer sample, relative to the extension folder.
const tracerPipeline = path.resolve('../testdata/tracer/pipeline.yaml');
// The line of the sample's mapping, where the session stops.
const mappingLine = 13;

// DebugClient.start() spawns `runtime executable`, which would pass a stray
// positional argument to rpcn-debug. The test spawns the binary itself and
// connects the client to its stdio instead.
class StdioDebugClient extends DebugClient {
  constructor() {
    super('unused', 'unused', 'redpanda-connect');
  }

  attach(proc: ChildProcess): void {
    if (!proc.stdout || !proc.stdin) {
      throw new Error('rpcn-debug was spawned without piped stdio');
    }
    this.connect(proc.stdout, proc.stdin);
  }
}

function exitCode(proc: ChildProcess): Promise<number | null> {
  return new Promise((resolve, reject) => {
    if (proc.exitCode !== null) {
      resolve(proc.exitCode);
      return;
    }
    proc.once('error', reject);
    proc.once('exit', (code) => resolve(code));
  });
}

test('rpcn-debug answers initialize and exits 0 on disconnect', async () => {
  const proc = spawn(binary, [], { stdio: ['pipe', 'pipe', 'inherit'] });
  const exited = exitCode(proc);
  try {
    const dc = new StdioDebugClient();
    dc.attach(proc);

    const init = await dc.initializeRequest();
    assert.equal(init.success, true);
    assert.equal(init.command, 'initialize');
    assert.deepEqual(
      init.body ?? {},
      { supportsConfigurationDoneRequest: true },
      'initialize must advertise only configurationDone',
    );

    const disc = await dc.disconnectRequest();
    assert.equal(disc.success, true);

    assert.equal(await exited, 0);
  } finally {
    if (proc.exitCode === null) {
      proc.kill();
    }
  }
});

test('F5 path: captures, stops before the mapping, steps over and terminates', async () => {
  const proc = spawn(binary, [], { stdio: ['pipe', 'pipe', 'inherit'] });
  const exited = exitCode(proc);
  try {
    const dc = new StdioDebugClient();
    dc.attach(proc);
    await dc.initializeRequest();

    // What the extension's DebugConfigurationProvider sends for the active
    // editor's file.
    const initialized = dc.waitForEvent('initialized');
    const launch = await dc.launchRequest({ pipeline: tracerPipeline } as DebugProtocol.LaunchRequestArguments);
    assert.equal(launch.success, true);
    await initialized;

    const stopped = dc.waitForEvent('stopped');
    await dc.configurationDoneRequest();
    const stop = await stopped;
    assert.equal(stop.body.reason, 'entry');

    const threads = await dc.threadsRequest();
    assert.equal(threads.body.threads.length, 1);
    const threadId = threads.body.threads[0].id;

    const stack = await dc.stackTraceRequest({ threadId });
    assert.equal(stack.body.stackFrames.length, 1);
    const frame = stack.body.stackFrames[0];
    assert.equal(frame.line, mappingLine);
    assert.equal(frame.source?.path, tracerPipeline);

    const scopes = await dc.scopesRequest({ frameId: frame.id });
    assert.equal(scopes.body.scopes.length, 1);
    const vars = await dc.variablesRequest({
      variablesReference: scopes.body.scopes[0].variablesReference,
    });
    assert.equal(vars.body.variables.length, 1);
    assert.deepEqual(JSON.parse(vars.body.variables[0].value), { greeting: 'hello', n: 1 });

    const terminated = dc.waitForEvent('terminated');
    await dc.nextRequest({ threadId });
    await terminated;

    const disc = await dc.disconnectRequest();
    assert.equal(disc.success, true);
    assert.equal(await exited, 0);
  } finally {
    if (proc.exitCode === null) {
      proc.kill();
    }
  }
});
