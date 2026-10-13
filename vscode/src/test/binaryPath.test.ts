import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { resolveBinaryPath } from '../binaryPath';

test('an unset or blank rpcnDebug.binaryPath names the setting', () => {
  for (const value of [undefined, '', '   ']) {
    assert.throws(() => resolveBinaryPath(value), /rpcnDebug\.binaryPath/);
  }
});

test('a set rpcnDebug.binaryPath is used, trimmed', () => {
  assert.equal(resolveBinaryPath(' /opt/rpcn-debug '), '/opt/rpcn-debug');
});
