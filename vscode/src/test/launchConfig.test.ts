import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { fillLaunchConfig } from '../launchConfig';

const entry = { type: 'redpanda-connect', request: 'launch', name: 'Debug pipeline' };

test('fills a missing pipeline from the active editor and cwd from the workspace folder', () => {
  assert.deepEqual(fillLaunchConfig(entry, 'redpanda-connect', '/w/a.yaml', '/w'), {
    ...entry,
    pipeline: '/w/a.yaml',
    cwd: '/w',
  });
});

test('keeps a pipeline and a cwd that are set', () => {
  const config = { ...entry, pipeline: 'other.yaml', cwd: '/elsewhere', maxMessages: 3 };
  assert.deepEqual(fillLaunchConfig(config, 'redpanda-connect', '/w/a.yaml', '/w'), config);
});

test('fills only the missing one of pipeline and cwd', () => {
  assert.deepEqual(fillLaunchConfig({ ...entry, pipeline: 'p.yaml' }, 'redpanda-connect', '/w/a.yaml', '/w'), {
    ...entry,
    pipeline: 'p.yaml',
    cwd: '/w',
  });
  assert.deepEqual(fillLaunchConfig({ ...entry, cwd: '/c' }, 'redpanda-connect', '/w/a.yaml', '/w'), {
    ...entry,
    cwd: '/c',
    pipeline: '/w/a.yaml',
  });
});

test('F5 with no launch.json gets a launch entry for the active file', () => {
  assert.deepEqual(fillLaunchConfig({}, 'redpanda-connect', '/w/p.yaml', '/w'), {
    ...entry,
    pipeline: '/w/p.yaml',
    cwd: '/w',
  });
});

test('adds nothing without an active file or a workspace folder', () => {
  assert.deepEqual(fillLaunchConfig(entry, 'redpanda-connect', undefined, undefined), entry);
});

test('does not touch other keys, valid or not', () => {
  const config = { ...entry, pipeline: 'p.yaml', cwd: '/c', maxMesages: 5 };
  assert.deepEqual(fillLaunchConfig(config, 'redpanda-connect', '/w/a.yaml', '/w'), config);
});
