import { spawn, ChildProcess } from 'node:child_process';
import * as path from 'node:path';
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { DebugClient } from '@vscode/debugadapter-testsupport';

// The binary under test. `task test:ext` passes the freshly built one;
// relative paths resolve against the extension folder (npm's cwd).
const binary = path.resolve(process.env.RPCN_DEBUG_BIN ?? '../bin/rpcn-debug');

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
    assert.deepEqual(init.body ?? {}, {}, 'initialize must advertise no capabilities');

    const disc = await dc.disconnectRequest();
    assert.equal(disc.success, true);

    assert.equal(await exited, 0);
  } finally {
    if (proc.exitCode === null) {
      proc.kill();
    }
  }
});
