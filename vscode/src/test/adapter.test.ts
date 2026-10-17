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
const mappingLine = 16;

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

// child returns the variable called name under variablesReference.
async function child(dc: DebugClient, variablesReference: number, name: string): Promise<DebugProtocol.Variable> {
  const vars = await dc.variablesRequest({ variablesReference });
  const v = vars.body.variables.find((x) => x.name === name);
  assert.ok(v, `no variable ${name} in ${JSON.stringify(vars.body.variables)}`);
  return v;
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

    // A YAML breakpoint is answered unverified (hollow), without an error.
    const bps = await dc.setBreakpointsRequest({
      source: { path: tracerPipeline },
      breakpoints: [{ line: mappingLine }],
    });
    assert.equal(bps.success, true);
    assert.equal(bps.body.breakpoints.length, 1);
    assert.equal(bps.body.breakpoints[0].verified, false);
    assert.ok(bps.body.breakpoints[0].message, 'an unverified breakpoint says why');
    const exc = await dc.setExceptionBreakpointsRequest({ filters: [] });
    assert.equal(exc.success, true);

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

    // Batches › batch 0 › message 0 › content, metadata › internal.
    const scopes = await dc.scopesRequest({ frameId: frame.id });
    assert.equal(scopes.body.scopes.length, 1);
    assert.equal(scopes.body.scopes[0].name, 'Batches');
    const batch = await child(dc, scopes.body.scopes[0].variablesReference, 'batch 0');
    const message = await child(dc, batch.variablesReference, 'message 0');
    const content = await child(dc, message.variablesReference, 'content');
    assert.deepEqual(JSON.parse(content.value), { greeting: 'hello', n: 1 });
    const metadata = await child(dc, message.variablesReference, 'metadata');
    assert.equal((await child(dc, metadata.variablesReference, 'source')).value, 'tracer');
    assert.equal((await child(dc, metadata.variablesReference, 'kafka_key')).value, 'k1');
    const internal = await child(dc, metadata.variablesReference, 'internal');
    assert.equal((await child(dc, internal.variablesReference, '__rpcn_internal')).value, 'x');

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
